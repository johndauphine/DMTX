package migrate

import (
	"encoding/json"
	"regexp"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// The per-fact decision: what each mode does with one piece of observed drift,
// and the discard bookkeeping that follows from it.

func decideSchemaDriftFact(
	fact schema.SchemaDriftFact,
	targetMode string,
	options SchemaContractOptions,
	context schemaContractContext,
) (SchemaContractDecision, *SchemaContractError) {
	mode := schemaContractModeForFact(options.Contract, fact.Entity)
	decision := SchemaContractDecision{
		Entity:     fact.Entity,
		Mode:       mode,
		ChangeKind: fact.ChangeKind,
		Object:     fact.Object,
		Previous:   append(json.RawMessage(nil), fact.Previous...),
		Current:    append(json.RawMessage(nil), fact.Current...),
	}

	if options.Contract == nil {
		if options.FailOnSchemaDrift {
			return abortSchemaContractDecision(
				decision,
				SchemaContractDriftBlocked,
				"schema contract is omitted and fail_on_schema_drift requires a hard gate",
			)
		}
		decision.Action = SchemaContractReport
		decision.Reason = "schema contract is omitted; drift is report-only"
		return decision, nil
	}
	if mode == config.SchemaContractEvolve &&
		fact.Entity == schema.SchemaContractTables &&
		schemaContractFactDependsOnDiscardedColumn(fact, context) {
		decision.Action = SchemaContractDiscardValue
		decision.Reason = "dependent object is pruned with its discard_value column"
		return decision, nil
	}
	if mode == config.SchemaContractEvolve {
		key := schemaContractTableKey{
			schema: fact.Object.Schema,
			table:  fact.Object.Table,
		}
		if _, discarded := context.discardTables[key]; discarded {
			decision.Action = SchemaContractDiscardRow
			decision.Reason = "the table is excluded by another entity's discard_row decision"
			return decision, nil
		}
	}

	switch mode {
	case config.SchemaContractReport:
		decision.Action = SchemaContractReport
		decision.Reason = "report mode records drift without changing target schema"
		return decision, nil
	case config.SchemaContractFreeze:
		return abortSchemaContractDecision(
			decision,
			SchemaContractDriftBlocked,
			"freeze mode rejects drift before transfer",
		)
	case config.SchemaContractDiscardRow:
		if isSourceDrop(fact.ChangeKind) {
			decision.Action = SchemaContractRetainTarget
			decision.Reason = "source drops are reported and retained on the target"
			return decision, nil
		}
		if fact.ChangeKind == schema.SchemaDriftColumnOrderChanged &&
			schemaContractOrderOnlySourceDrops(fact, context) {
			decision.Action = SchemaContractRetainTarget
			decision.Reason = "column order drift is fully explained by source drops retained on the target"
			return decision, nil
		}
		decision.Action = SchemaContractDiscardRow
		decision.Reason = "discard_row excludes the affected table from this run"
		return decision, nil
	case config.SchemaContractDiscardValue:
		return decideDiscardValue(fact, decision, context)
	case config.SchemaContractEvolve:
		return decideEvolve(fact, decision, targetMode, context)
	default:
		return abortSchemaContractDecision(
			decision,
			SchemaContractInvalidPolicy,
			"schema contract mode is unsupported",
		)
	}
}

func decideDiscardValue(
	fact schema.SchemaDriftFact,
	decision SchemaContractDecision,
	context schemaContractContext,
) (SchemaContractDecision, *SchemaContractError) {
	switch fact.Entity {
	case schema.SchemaContractTables:
		return abortSchemaContractDecision(
			decision,
			SchemaContractInvalidPolicy,
			"tables cannot use discard_value",
		)
	case schema.SchemaContractColumns:
		switch fact.ChangeKind {
		case schema.SchemaDriftColumnAdded:
			if reason := protectedDiscardReason(fact, context); reason != "" {
				return abortSchemaContractDecision(
					decision,
					SchemaContractProtectedDiscard,
					reason,
				)
			}
			decision.Action = SchemaContractDiscardValue
			decision.Reason = "discard_value omits the eligible added column and dependent objects"
			return decision, nil
		case schema.SchemaDriftColumnDropped:
			decision.Action = SchemaContractRetainTarget
			decision.Reason = "source column drops are reported and retained on the target"
			return decision, nil
		case schema.SchemaDriftColumnOrderChanged:
			if schemaContractOrderOnlySourceDrops(fact, context) {
				decision.Action = SchemaContractRetainTarget
				decision.Reason = "column order drift is fully explained by source drops retained on the target"
				return decision, nil
			}
			if schemaContractOrderOnlyAddsOrDrops(fact, context) {
				decision.Action = SchemaContractDiscardValue
				decision.Reason = "column order drift is fully explained by independently discarded columns"
				return decision, nil
			}
		}
	case schema.SchemaContractDataType:
		if fact.ChangeKind == schema.SchemaDriftDataTypeChanged {
			if reason := protectedDiscardReason(fact, context); reason != "" {
				return abortSchemaContractDecision(
					decision,
					SchemaContractProtectedDiscard,
					reason,
				)
			}
			decision.Action = SchemaContractDiscardValue
			decision.Reason = "discard_value omits the affected column and retains its prior type evidence"
			return decision, nil
		}
	}
	return abortSchemaContractDecision(
		decision,
		SchemaContractUnsafeEvolution,
		"discard_value is not defined safely for this drift kind",
	)
}

func decideEvolve(
	fact schema.SchemaDriftFact,
	decision SchemaContractDecision,
	targetMode string,
	context schemaContractContext,
) (SchemaContractDecision, *SchemaContractError) {
	if isSourceDrop(fact.ChangeKind) {
		decision.Action = SchemaContractRetainTarget
		decision.Reason = "source drops are reported and retained on the target"
		return decision, nil
	}
	if targetMode == "drop_recreate" {
		decision.Action = SchemaContractRebuildCurrentShape
		decision.Reason = "rebuild mode deterministically recreates the current source shape"
		return decision, nil
	}

	switch fact.ChangeKind {
	case schema.SchemaDriftTableAdded:
		decision.Action = SchemaContractCreateTable
		decision.Reason = "evolve creates an eligible newly discovered table"
		return decision, nil
	case schema.SchemaDriftColumnAdded:
		var current schema.SnapshotColumn
		if err := decodeSchemaContractEvidence(fact.Current, &current); err != nil {
			return abortSchemaContractDecision(
				decision,
				SchemaContractUnsafeEvolution,
				"added-column evidence is incomplete",
			)
		}
		if current.PrimaryKey ||
			schemaContractIdentityColumn(fact, context, current.Name) {
			return abortSchemaContractDecision(
				decision,
				SchemaContractUnsafeEvolution,
				"upsert cannot add a primary-key or identity column",
			)
		}
		if !current.Nullable {
			return abortSchemaContractDecision(
				decision,
				SchemaContractUnsafeEvolution,
				"upsert can add only nullable columns",
			)
		}
		if current.Default != nil && !safeSchemaContractDefault(*current.Default) {
			return abortSchemaContractDecision(
				decision,
				SchemaContractUnsafeEvolution,
				"added column has a non-literal or otherwise unsafe default",
			)
		}
		decision.Action = SchemaContractAddColumn
		decision.Reason = "evolve adds a nullable column with no default or a deterministic literal default"
		return decision, nil
	case schema.SchemaDriftNullabilityChanged:
		var previous, current schema.SnapshotColumn
		if decodeSchemaContractEvidence(fact.Previous, &previous) == nil &&
			decodeSchemaContractEvidence(fact.Current, &current) == nil &&
			!previous.Nullable && current.Nullable {
			decision.Action = SchemaContractRelaxNullability
			decision.Reason = "evolve safely relaxes nullability"
			return decision, nil
		}
		return abortSchemaContractDecision(
			decision,
			SchemaContractUnsafeEvolution,
			"nullability tightening is not a compatible evolution",
		)
	case schema.SchemaDriftDataTypeChanged:
		var previous, current schema.SnapshotColumn
		if decodeSchemaContractEvidence(fact.Previous, &previous) == nil &&
			decodeSchemaContractEvidence(fact.Current, &current) == nil &&
			safeSchemaContractTypeWidening(previous, current) &&
			!schemaContractColumnHasCoupledUnsafeDrift(fact, context) {
			decision.Action = SchemaContractWidenType
			decision.Reason = "evolve admits the proven lossless type widening"
			return decision, nil
		}
		return abortSchemaContractDecision(
			decision,
			SchemaContractUnsafeEvolution,
			"type change is narrowing, lossy, ambiguous, or coupled to unsafe drift",
		)
	case schema.SchemaDriftColumnOrderChanged:
		if schemaContractOrderOnlyAddsOrDrops(fact, context) {
			decision.Action = SchemaContractRetainTarget
			decision.Reason = "existing target column order is retained while column add/drop decisions apply independently"
			return decision, nil
		}
	}
	return abortSchemaContractDecision(
		decision,
		SchemaContractUnsafeEvolution,
		"upsert evolution has no proven deterministic compatible operation for this drift",
	)
}

func abortSchemaContractDecision(
	decision SchemaContractDecision,
	kind SchemaContractErrorKind,
	reason string,
) (SchemaContractDecision, *SchemaContractError) {
	decision.Action = SchemaContractAbort
	decision.Reason = reason
	copied := decision
	return decision, &SchemaContractError{
		Kind:     kind,
		Decision: &copied,
		Reason:   reason,
	}
}

func schemaContractModeForFact(
	contract *config.SchemaContract,
	entity schema.SchemaContractEntity,
) config.SchemaContractMode {
	if contract == nil {
		return config.SchemaContractReport
	}
	switch entity {
	case schema.SchemaContractTables:
		return contract.Tables
	case schema.SchemaContractColumns:
		return contract.Columns
	case schema.SchemaContractDataType:
		return contract.DataType
	default:
		return ""
	}
}

func isSourceDrop(kind schema.SchemaDriftChangeKind) bool {
	return kind == schema.SchemaDriftTableDropped ||
		kind == schema.SchemaDriftColumnDropped
}

func protectedDiscardReason(
	fact schema.SchemaDriftFact,
	context schemaContractContext,
) string {
	key := schemaContractTableKey{
		schema: fact.Object.Schema,
		table:  fact.Object.Table,
	}
	columnName := fact.Object.Column
	previous, previousOK := context.previous[key]
	current, currentOK := context.current[key]
	for _, table := range []schema.SnapshotTable{previous, current} {
		for _, column := range table.Columns {
			if column.Name == columnName && column.PrimaryKey {
				return "primary-key columns cannot be discarded"
			}
		}
		if table.Identity != nil && table.Identity.Column == columnName {
			return "identity columns cannot be discarded"
		}
		for _, orderingColumn := range table.ClickHouseOrderBy {
			if orderingColumn == columnName {
				return "physical ordering columns cannot be discarded"
			}
		}
	}
	if !previousOK && !currentOK {
		return "column protection cannot be proven from snapshot evidence"
	}
	if _, protected := context.dateColumns[columnName]; protected {
		return "selected date-tracking columns cannot be discarded"
	}
	return ""
}

func schemaContractIdentityColumn(
	fact schema.SchemaDriftFact,
	context schemaContractContext,
	column string,
) bool {
	key := schemaContractTableKey{
		schema: fact.Object.Schema,
		table:  fact.Object.Table,
	}
	table, ok := context.current[key]
	return ok && table.Identity != nil && table.Identity.Column == column
}

func schemaContractColumnHasCoupledUnsafeDrift(
	fact schema.SchemaDriftFact,
	context schemaContractContext,
) bool {
	key := schemaContractTableKey{
		schema: fact.Object.Schema,
		table:  fact.Object.Table,
	}
	for _, other := range context.factsByTable[key] {
		if other.Object.Column != fact.Object.Column {
			continue
		}
		switch other.ChangeKind {
		case schema.SchemaDriftDataTypeChanged,
			schema.SchemaDriftNullabilityChanged:
			continue
		default:
			return true
		}
	}
	return false
}

func schemaContractFactDependsOnDiscardedColumn(
	fact schema.SchemaDriftFact,
	context schemaContractContext,
) bool {
	key := schemaContractTableKey{
		schema: fact.Object.Schema,
		table:  fact.Object.Table,
	}
	depends := func(raw json.RawMessage) bool {
		if string(raw) == "null" {
			return false
		}
		switch fact.Object.Kind {
		case schema.SchemaDriftObjectIndex:
			var index schema.SnapshotIndex
			return decodeSchemaContractEvidence(raw, &index) == nil &&
				schemaContractIndexUsesAny(
					index,
					context.discardColumns[key],
				)
		case schema.SchemaDriftObjectCheck:
			var check schema.SnapshotCheckConstraint
			return decodeSchemaContractEvidence(raw, &check) == nil &&
				schemaContractCheckUsesAny(
					check.Expression,
					context.discardColumns[key],
				)
		case schema.SchemaDriftObjectForeignKey:
			var foreignKey schema.SnapshotForeignKey
			if decodeSchemaContractEvidence(raw, &foreignKey) != nil {
				return false
			}
			if schemaContractStringsUseAny(
				foreignKey.Columns,
				context.discardColumns[key],
			) {
				return true
			}
			owner := schema.SnapshotTable{
				Schema: fact.Object.Schema,
				Name:   fact.Object.Table,
			}
			for referencedKey, columns := range context.discardColumns {
				if schemaContractReferencedTableMatches(
					owner,
					foreignKey,
					referencedKey,
				) && schemaContractStringsUseAny(
					foreignKey.ReferencedColumns,
					columns,
				) {
					return true
				}
			}
		}
		return false
	}
	return depends(fact.Previous) || depends(fact.Current)
}

func schemaContractOrderOnlyAddsOrDrops(
	fact schema.SchemaDriftFact,
	context schemaContractContext,
) bool {
	var previousOrder, currentOrder []string
	if decodeSchemaContractEvidence(fact.Previous, &previousOrder) != nil ||
		decodeSchemaContractEvidence(fact.Current, &currentOrder) != nil {
		return false
	}
	previousSet := schemaContractStringSet(previousOrder)
	currentSet := schemaContractStringSet(currentOrder)
	previousCommon := make([]string, 0, len(previousOrder))
	currentCommon := make([]string, 0, len(currentOrder))
	for _, column := range previousOrder {
		if _, exists := currentSet[column]; exists {
			previousCommon = append(previousCommon, column)
		}
	}
	for _, column := range currentOrder {
		if _, exists := previousSet[column]; exists {
			currentCommon = append(currentCommon, column)
		}
	}
	if !equalSchemaContractStrings(previousCommon, currentCommon) {
		return false
	}
	key := schemaContractTableKey{
		schema: fact.Object.Schema,
		table:  fact.Object.Table,
	}
	changed := make(map[string]struct{})
	for _, other := range context.factsByTable[key] {
		if other.ChangeKind == schema.SchemaDriftColumnAdded ||
			other.ChangeKind == schema.SchemaDriftColumnDropped {
			changed[other.Object.Column] = struct{}{}
		}
	}
	for column := range previousSet {
		if _, exists := currentSet[column]; exists {
			continue
		}
		if _, explained := changed[column]; !explained {
			return false
		}
	}
	for column := range currentSet {
		if _, exists := previousSet[column]; exists {
			continue
		}
		if _, explained := changed[column]; !explained {
			return false
		}
	}
	return true
}

func schemaContractOrderOnlySourceDrops(
	fact schema.SchemaDriftFact,
	context schemaContractContext,
) bool {
	if !schemaContractOrderOnlyAddsOrDrops(fact, context) {
		return false
	}
	key := schemaContractTableKey{
		schema: fact.Object.Schema,
		table:  fact.Object.Table,
	}
	sawDrop := false
	for _, other := range context.factsByTable[key] {
		switch other.ChangeKind {
		case schema.SchemaDriftColumnAdded:
			return false
		case schema.SchemaDriftColumnDropped:
			sawDrop = true
		}
	}
	return sawDrop
}

var schemaContractNumberDefault = regexp.MustCompile(
	`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`,
)
