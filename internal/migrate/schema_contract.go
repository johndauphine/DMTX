package migrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// SchemaContractAction is the complete, machine-readable result of applying
// one schema-contract mode to one drift fact. Adapters must not infer a
// mutation from a report or retain_target action.
type SchemaContractAction string

const (
	SchemaContractReport              SchemaContractAction = "report"
	SchemaContractAbort               SchemaContractAction = "abort"
	SchemaContractRetainTarget        SchemaContractAction = "retain_target"
	SchemaContractCreateTable         SchemaContractAction = "create_table"
	SchemaContractAddColumn           SchemaContractAction = "add_column"
	SchemaContractRelaxNullability    SchemaContractAction = "relax_nullability"
	SchemaContractWidenType           SchemaContractAction = "widen_type"
	SchemaContractRebuildCurrentShape SchemaContractAction = "rebuild_current_shape"
	SchemaContractDiscardRow          SchemaContractAction = "discard_row"
	SchemaContractDiscardValue        SchemaContractAction = "discard_value"
)

// SchemaContractDecision is audit-ready and contains no implicit fields. The
// evidence is copied from the deterministic schema drift fact.
type SchemaContractDecision struct {
	Entity     schema.SchemaContractEntity  `json:"entity"`
	Mode       config.SchemaContractMode    `json:"mode"`
	ChangeKind schema.SchemaDriftChangeKind `json:"change_kind"`
	Object     schema.SchemaDriftObject     `json:"object"`
	Previous   json.RawMessage              `json:"previous"`
	Current    json.RawMessage              `json:"current"`
	Action     SchemaContractAction         `json:"action"`
	Reason     string                       `json:"reason"`
}

// SchemaContractPlan keeps row/value projections separate from target-shape
// projections. In particular, report-only policy never changes UpsertSnapshot,
// while rebuild mode can independently recreate RebuildSnapshot.
type SchemaContractPlan struct {
	Decisions          []SchemaContractDecision `json:"decisions"`
	TransferSnapshot   schema.SchemaSnapshot    `json:"transfer_snapshot"`
	ValidationSnapshot schema.SchemaSnapshot    `json:"validation_snapshot"`
	SuccessfulSnapshot schema.SchemaSnapshot    `json:"successful_snapshot"`
	UpsertSnapshot     schema.SchemaSnapshot    `json:"upsert_snapshot"`
	RebuildSnapshot    schema.SchemaSnapshot    `json:"rebuild_snapshot"`
}

type SchemaContractErrorKind string

const (
	SchemaContractInvalidPolicy    SchemaContractErrorKind = "invalid_policy"
	SchemaContractDriftBlocked     SchemaContractErrorKind = "drift_blocked"
	SchemaContractUnsafeEvolution  SchemaContractErrorKind = "unsafe_evolution"
	SchemaContractProtectedDiscard SchemaContractErrorKind = "protected_discard"
)

// SchemaContractError is stable and classifiable with errors.As. When a drift
// fact caused the failure, Decision contains the exact abort decision.
type SchemaContractError struct {
	Kind     SchemaContractErrorKind
	Decision *SchemaContractDecision
	Reason   string
}

func (err *SchemaContractError) Error() string {
	if err.Decision == nil {
		return fmt.Sprintf("schema contract %s: %s", err.Kind, err.Reason)
	}
	object := err.Decision.Object.Table
	if err.Decision.Object.Schema != "" {
		object = err.Decision.Object.Schema + "." + object
	}
	if err.Decision.Object.Column != "" {
		object += "." + err.Decision.Object.Column
	} else if err.Decision.Object.Name != "" {
		object += "." + err.Decision.Object.Name
	}
	return fmt.Sprintf(
		"schema contract %s: %s %s for %s: %s",
		err.Kind,
		err.Decision.Mode,
		err.Decision.ChangeKind,
		object,
		err.Reason,
	)
}

type SchemaContractOptions struct {
	Contract           *config.SchemaContract
	FailOnSchemaDrift  bool
	TargetMode         string
	DateUpdatedColumns []string
}

// BuildSchemaContractPlan applies policy to deterministic schema snapshots
// without database I/O or adapter lifecycle effects. A non-nil error makes all
// projections non-executable, but the returned plan still contains every
// deterministic decision for diagnostics and audit.
func BuildSchemaContractPlan(
	previous schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	options SchemaContractOptions,
) (SchemaContractPlan, error) {
	targetMode := options.TargetMode
	if targetMode == "" {
		targetMode = "drop_recreate"
	}
	if targetMode != "drop_recreate" && targetMode != "upsert" {
		return SchemaContractPlan{}, &SchemaContractError{
			Kind:   SchemaContractInvalidPolicy,
			Reason: fmt.Sprintf("unsupported target mode %q", targetMode),
		}
	}
	if err := validateSchemaContractOptions(options); err != nil {
		return SchemaContractPlan{}, err
	}

	facts, err := schema.CompareSchemaSnapshots(previous, current)
	if err != nil {
		return SchemaContractPlan{}, fmt.Errorf("compare schema contract snapshots: %w", err)
	}
	previous, err = canonicalSchemaSnapshot(previous)
	if err != nil {
		return SchemaContractPlan{}, fmt.Errorf("normalize previous schema contract snapshot: %w", err)
	}
	current, err = canonicalSchemaSnapshot(current)
	if err != nil {
		return SchemaContractPlan{}, fmt.Errorf("normalize current schema contract snapshot: %w", err)
	}

	plan := SchemaContractPlan{
		Decisions:          make([]SchemaContractDecision, 0, len(facts)),
		TransferSnapshot:   cloneSchemaSnapshot(current),
		ValidationSnapshot: cloneSchemaSnapshot(current),
		SuccessfulSnapshot: cloneSchemaSnapshot(current),
		UpsertSnapshot:     cloneSchemaSnapshot(previous),
		RebuildSnapshot:    cloneSchemaSnapshot(current),
	}
	if len(facts) == 0 {
		return plan, nil
	}

	context := newSchemaContractContext(previous, current, facts, options)
	var firstError *SchemaContractError
	for _, fact := range facts {
		decision, decisionError := decideSchemaDriftFact(
			fact,
			targetMode,
			options,
			context,
		)
		plan.Decisions = append(plan.Decisions, decision)
		if firstError == nil && decisionError != nil {
			firstError = decisionError
		}
	}

	discardedTables := make(map[schemaContractTableKey]struct{})
	discardedColumns := make(map[schemaContractTableKey]map[string]struct{})
	typeDiscardedColumns := make(map[schemaContractTableKey]map[string]struct{})
	for _, decision := range plan.Decisions {
		key := schemaContractTableKey{
			schema: decision.Object.Schema,
			table:  decision.Object.Table,
		}
		switch decision.Action {
		case SchemaContractDiscardRow:
			discardedTables[key] = struct{}{}
		case SchemaContractDiscardValue:
			if decision.Object.Column == "" {
				continue
			}
			addSchemaContractColumn(discardedColumns, key, decision.Object.Column)
			if decision.ChangeKind == schema.SchemaDriftDataTypeChanged {
				addSchemaContractColumn(
					typeDiscardedColumns,
					key,
					decision.Object.Column,
				)
			}
		}
	}

	applySafeUpsertDecisions(&plan.UpsertSnapshot, current, plan.Decisions)
	for _, target := range []*schema.SchemaSnapshot{
		&plan.TransferSnapshot,
		&plan.ValidationSnapshot,
		&plan.RebuildSnapshot,
	} {
		projectSchemaContractSnapshot(
			target,
			discardedTables,
			discardedColumns,
			nil,
		)
	}
	if err := restoreRebuildSourceDrops(
		&plan.RebuildSnapshot,
		previous,
		plan.Decisions,
		discardedTables,
		discardedColumns,
	); err != nil {
		return plan, &SchemaContractError{
			Kind:   SchemaContractUnsafeEvolution,
			Reason: "cannot retain source-drop target shape during rebuild: " + err.Error(),
		}
	}
	projectSchemaContractSnapshot(
		&plan.SuccessfulSnapshot,
		discardedTables,
		discardedColumns,
		typeDiscardedColumns,
	)
	restoreDiscardedTypeEvidence(
		&plan.SuccessfulSnapshot,
		previous,
		typeDiscardedColumns,
	)
	if err := normalizeSchemaContractPlan(&plan); err != nil {
		return plan, &SchemaContractError{
			Kind:   SchemaContractInvalidPolicy,
			Reason: "schema projection is not internally valid: " + err.Error(),
		}
	}
	if firstError != nil {
		return plan, firstError
	}
	return plan, nil
}

func validateSchemaContractOptions(options SchemaContractOptions) error {
	if options.Contract == nil {
		return nil
	}
	modes := []struct {
		entity string
		mode   config.SchemaContractMode
	}{
		{entity: "tables", mode: options.Contract.Tables},
		{entity: "columns", mode: options.Contract.Columns},
		{entity: "data_type", mode: options.Contract.DataType},
	}
	for _, item := range modes {
		switch item.mode {
		case config.SchemaContractEvolve,
			config.SchemaContractFreeze,
			config.SchemaContractDiscardRow,
			config.SchemaContractDiscardValue,
			config.SchemaContractReport:
		default:
			return &SchemaContractError{
				Kind: SchemaContractInvalidPolicy,
				Reason: fmt.Sprintf(
					"entity %s has unsupported mode %q",
					item.entity,
					item.mode,
				),
			}
		}
	}
	if options.Contract.Tables == config.SchemaContractDiscardValue {
		return &SchemaContractError{
			Kind:   SchemaContractInvalidPolicy,
			Reason: "tables cannot use discard_value",
		}
	}
	return nil
}

type schemaContractTableKey struct {
	schema string
	table  string
}

type schemaContractContext struct {
	previous       map[schemaContractTableKey]schema.SnapshotTable
	current        map[schemaContractTableKey]schema.SnapshotTable
	factsByTable   map[schemaContractTableKey][]schema.SchemaDriftFact
	dateColumns    map[string]struct{}
	discardColumns map[schemaContractTableKey]map[string]struct{}
	discardTables  map[schemaContractTableKey]struct{}
}

func newSchemaContractContext(
	previous,
	current schema.SchemaSnapshot,
	facts []schema.SchemaDriftFact,
	options SchemaContractOptions,
) schemaContractContext {
	result := schemaContractContext{
		previous:     indexSchemaContractTables(previous),
		current:      indexSchemaContractTables(current),
		factsByTable: make(map[schemaContractTableKey][]schema.SchemaDriftFact),
		dateColumns:  make(map[string]struct{}, len(options.DateUpdatedColumns)),
		discardColumns: make(
			map[schemaContractTableKey]map[string]struct{},
		),
		discardTables: make(map[schemaContractTableKey]struct{}),
	}
	for _, fact := range facts {
		key := schemaContractTableKey{
			schema: fact.Object.Schema,
			table:  fact.Object.Table,
		}
		result.factsByTable[key] = append(result.factsByTable[key], fact)
		if options.Contract != nil &&
			(fact.ChangeKind == schema.SchemaDriftColumnAdded &&
				options.Contract.Columns == config.SchemaContractDiscardValue ||
				fact.ChangeKind == schema.SchemaDriftDataTypeChanged &&
					options.Contract.DataType == config.SchemaContractDiscardValue) {
			addSchemaContractColumn(
				result.discardColumns,
				key,
				fact.Object.Column,
			)
		}
	}
	for _, column := range options.DateUpdatedColumns {
		result.dateColumns[column] = struct{}{}
	}
	if options.Contract != nil {
		for _, fact := range facts {
			if schemaContractModeForFact(
				options.Contract,
				fact.Entity,
			) != config.SchemaContractDiscardRow ||
				isSourceDrop(fact.ChangeKind) ||
				fact.ChangeKind == schema.SchemaDriftColumnOrderChanged &&
					schemaContractOrderOnlySourceDrops(fact, result) {
				continue
			}
			result.discardTables[schemaContractTableKey{
				schema: fact.Object.Schema,
				table:  fact.Object.Table,
			}] = struct{}{}
		}
	}
	return result
}

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

func safeSchemaContractDefault(value string) bool {
	value = strings.TrimSpace(value)
	switch strings.ToUpper(value) {
	case "NULL", "TRUE", "FALSE":
		return true
	}
	if schemaContractNumberDefault.MatchString(value) {
		return true
	}
	if validSchemaContractQuotedLiteral(value, '\'') {
		return true
	}
	if len(value) >= 3 &&
		(value[0] == 'X' || value[0] == 'x') &&
		validSchemaContractQuotedLiteral(value[1:], '\'') {
		for _, digit := range value[2 : len(value)-1] {
			if !strings.ContainsRune("0123456789abcdefABCDEF", digit) {
				return false
			}
		}
		return (len(value)-3)%2 == 0
	}
	return false
}

func validSchemaContractQuotedLiteral(value string, quote byte) bool {
	if len(value) < 2 || value[0] != quote || value[len(value)-1] != quote {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		if value[index] != quote {
			continue
		}
		if index+1 >= len(value)-1 || value[index+1] != quote {
			return false
		}
		index++
	}
	return true
}

func safeSchemaContractTypeWidening(
	previous,
	current schema.SnapshotColumn,
) bool {
	if !schemaContractTypeEvidenceConsistent(previous) ||
		!schemaContractTypeEvidenceConsistent(current) {
		return false
	}
	generic := schemaContractGenericTypeRelation(
		previous.Type,
		current.Type,
	)
	if generic == schemaContractTypeInvalid {
		return false
	}
	if previous.DeclaredType == nil || current.DeclaredType == nil {
		return previous.DeclaredType == nil &&
			current.DeclaredType == nil &&
			generic == schemaContractTypeWidening
	}
	declared := schemaContractDeclaredTypeRelation(
		*previous.DeclaredType,
		*current.DeclaredType,
	)
	if declared == schemaContractTypeInvalid {
		return false
	}
	switch generic {
	case schemaContractTypeEqual:
		return declared == schemaContractTypeWidening
	case schemaContractTypeWidening:
		// When both evidence channels are present they must independently
		// prove the same widening. A stale, unchanged declaration is not
		// sufficient corroboration for a changed generic type.
		return declared == schemaContractTypeWidening &&
			schemaContractDeclaredWideningMatchesGeneric(
				previous.Type,
				current.Type,
				*previous.DeclaredType,
				*current.DeclaredType,
			)
	default:
		return false
	}
}

func schemaContractDeclaredWideningMatchesGeneric(
	previousType,
	currentType string,
	previous,
	current schema.SnapshotDeclaredType,
) bool {
	previousType = canonicalSchemaContractGenericType(previousType)
	currentType = canonicalSchemaContractGenericType(currentType)
	previousBase := normalizeSchemaContractType(previous.Base)
	currentBase := normalizeSchemaContractType(current.Base)
	switch {
	case previousType == "integer" && currentType == "bigint":
		previousRank, previousOK := schemaContractIntegerRank(previousBase)
		currentRank, currentOK := schemaContractIntegerRank(currentBase)
		return previousOK && currentOK &&
			previousRank < 5 && currentRank == 5
	case previousType == "real" && currentType == "double":
		return (previousBase == "real" || previousBase == "float4") &&
			(currentBase == "double" ||
				currentBase == "double precision" ||
				currentBase == "float8")
	case previousType == "varchar" && currentType == "text":
		return schemaContractVariableTextBase(previousBase) &&
			schemaContractTextRank(currentBase) >=
				schemaContractTextRank("text")
	case previousType == "varbinary" && currentType == "blob":
		return previousBase == "varbinary" &&
			schemaContractBlobRank(currentBase) >=
				schemaContractBlobRank("blob")
	default:
		return false
	}
}

func normalizeSchemaContractType(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

type schemaContractTypeRelation uint8

const (
	schemaContractTypeInvalid schemaContractTypeRelation = iota
	schemaContractTypeEqual
	schemaContractTypeWidening
)

func schemaContractGenericTypeRelation(
	previous,
	current string,
) schemaContractTypeRelation {
	previous = canonicalSchemaContractGenericType(previous)
	current = canonicalSchemaContractGenericType(current)
	if previous == "" || current == "" {
		return schemaContractTypeInvalid
	}
	if previous == current {
		return schemaContractTypeEqual
	}
	switch {
	case previous == "integer" && current == "bigint",
		previous == "real" && current == "double",
		previous == "varchar" && current == "text",
		previous == "varbinary" && current == "blob":
		return schemaContractTypeWidening
	default:
		return schemaContractTypeInvalid
	}
}

func canonicalSchemaContractGenericType(value string) string {
	switch normalizeSchemaContractType(value) {
	case "int", "integer", "int4":
		return "integer"
	case "bigint", "int8":
		return "bigint"
	case "real", "float4":
		return "real"
	case "double", "double precision", "float8":
		return "double"
	case "decimal", "numeric":
		return "numeric"
	case "varchar", "character varying", "varying character":
		return "varchar"
	case "char", "character":
		return "char"
	case "bool", "boolean":
		return "boolean"
	default:
		return normalizeSchemaContractType(value)
	}
}

func schemaContractDeclaredTypeRelation(
	previous,
	current schema.SnapshotDeclaredType,
) schemaContractTypeRelation {
	previousBase := normalizeSchemaContractType(previous.Base)
	currentBase := normalizeSchemaContractType(current.Base)
	if schemaContractEquivalentDeclaredBase(previousBase, currentBase) &&
		reflect.DeepEqual(previous.Arguments, current.Arguments) {
		return schemaContractTypeEqual
	}
	if rank, ok := schemaContractIntegerRank(previousBase); ok {
		currentRank, currentOK := schemaContractIntegerRank(currentBase)
		if currentOK && currentRank > rank {
			return schemaContractTypeWidening
		}
		return schemaContractTypeInvalid
	}
	if schemaContractNumericBase(previousBase) &&
		schemaContractNumericBase(currentBase) {
		previousPrecision, previousScale, previousOK :=
			schemaContractNumericModifiers(previous.Arguments)
		currentPrecision, currentScale, currentOK :=
			schemaContractNumericModifiers(current.Arguments)
		if previousOK && currentOK &&
			currentScale >= previousScale &&
			currentPrecision-currentScale >=
				previousPrecision-previousScale &&
			(currentPrecision > previousPrecision ||
				currentScale > previousScale) {
			return schemaContractTypeWidening
		}
		return schemaContractTypeInvalid
	}
	if schemaContractEquivalentDeclaredBase(previousBase, currentBase) {
		switch canonicalSchemaContractDeclaredBase(previousBase) {
		case "varchar", "nvarchar", "varbinary":
			if len(previous.Arguments) == 1 &&
				len(current.Arguments) == 1 &&
				current.Arguments[0] > previous.Arguments[0] {
				return schemaContractTypeWidening
			}
		case "time", "datetime", "timestamp", "timestamptz":
			if len(previous.Arguments) == 1 &&
				len(current.Arguments) == 1 &&
				current.Arguments[0] > previous.Arguments[0] {
				return schemaContractTypeWidening
			}
		}
		return schemaContractTypeInvalid
	}
	if schemaContractTextRank(previousBase) >= 0 &&
		schemaContractTextRank(currentBase) >
			schemaContractTextRank(previousBase) {
		return schemaContractTypeWidening
	}
	if schemaContractBlobRank(previousBase) >= 0 &&
		schemaContractBlobRank(currentBase) >
			schemaContractBlobRank(previousBase) {
		return schemaContractTypeWidening
	}
	if schemaContractVariableTextBase(previousBase) &&
		schemaContractTextRank(currentBase) >=
			schemaContractTextRank("text") {
		return schemaContractTypeWidening
	}
	if previousBase == "varbinary" &&
		schemaContractBlobRank(currentBase) >=
			schemaContractBlobRank("blob") {
		return schemaContractTypeWidening
	}
	if (previousBase == "real" || previousBase == "float4") &&
		(currentBase == "double" ||
			currentBase == "double precision" ||
			currentBase == "float8") {
		return schemaContractTypeWidening
	}
	return schemaContractTypeInvalid
}

func schemaContractTypeEvidenceConsistent(
	column schema.SnapshotColumn,
) bool {
	if column.DeclaredType == nil {
		return true
	}
	generic := canonicalSchemaContractGenericType(column.Type)
	base := normalizeSchemaContractType(column.DeclaredType.Base)
	switch generic {
	case "integer":
		rank, ok := schemaContractIntegerRank(base)
		return ok && rank < 5
	case "bigint":
		rank, ok := schemaContractIntegerRank(base)
		return ok && rank == 5
	case "numeric":
		return schemaContractNumericBase(base)
	case "real":
		return base == "real" || base == "float4"
	case "double":
		return base == "double" ||
			base == "double precision" ||
			base == "float8" ||
			base == "float"
	case "varchar":
		return schemaContractVariableTextBase(base)
	case "char":
		return base == "char" ||
			base == "character" ||
			base == "nchar"
	case "text":
		return schemaContractVariableTextBase(base) ||
			base == "char" ||
			base == "character" ||
			base == "nchar" ||
			schemaContractTextRank(base) >= 0
	case "binary":
		return base == "binary"
	case "varbinary":
		return base == "varbinary"
	case "blob":
		return base == "binary" ||
			base == "varbinary" ||
			base == "bytea" ||
			schemaContractBlobRank(base) >= 0
	case "time":
		return base == "time"
	case "date":
		return base == "date"
	case "timestamp":
		return base == "timestamp" || base == "datetime"
	case "datetime":
		return base == "datetime" ||
			base == "datetime2" ||
			base == "timestamp" ||
			base == "smalldatetime"
	case "timestamptz":
		return base == "timestamptz"
	case "boolean":
		return base == "bool" ||
			base == "boolean" ||
			base == "bit"
	case "uuid":
		return base == "uuid" || base == "uniqueidentifier"
	case "json":
		return base == "json"
	case "jsonb":
		return base == "jsonb"
	default:
		return false
	}
}

func schemaContractEquivalentDeclaredBase(left, right string) bool {
	return canonicalSchemaContractDeclaredBase(left) ==
		canonicalSchemaContractDeclaredBase(right)
}

func canonicalSchemaContractDeclaredBase(value string) string {
	switch normalizeSchemaContractType(value) {
	case "decimal", "numeric":
		return "numeric"
	case "character varying", "varying character", "varchar":
		return "varchar"
	case "native character", "nvarchar":
		return "nvarchar"
	case "double", "double precision", "float8":
		return "double"
	default:
		return normalizeSchemaContractType(value)
	}
}

func schemaContractVariableTextBase(value string) bool {
	switch canonicalSchemaContractDeclaredBase(value) {
	case "varchar", "nvarchar":
		return true
	default:
		return false
	}
}

func schemaContractNumericModifiers(arguments []int) (int, int, bool) {
	switch len(arguments) {
	case 1:
		return arguments[0], 0, arguments[0] > 0
	case 2:
		return arguments[0], arguments[1],
			arguments[0] > 0 &&
				arguments[1] >= 0 &&
				arguments[1] <= arguments[0]
	default:
		return 0, 0, false
	}
}

func schemaContractIntegerRank(value string) (int, bool) {
	switch value {
	case "tinyint":
		return 1, true
	case "smallint", "int2":
		return 2, true
	case "mediumint":
		return 3, true
	case "int", "integer", "int4":
		return 4, true
	case "bigint", "int8":
		return 5, true
	default:
		return 0, false
	}
}

func schemaContractNumericBase(value string) bool {
	return value == "numeric" || value == "decimal"
}

func schemaContractTextRank(value string) int {
	switch value {
	case "tinytext":
		return 0
	case "text":
		return 1
	case "mediumtext":
		return 2
	case "longtext":
		return 3
	default:
		return -1
	}
}

func schemaContractBlobRank(value string) int {
	switch value {
	case "tinyblob":
		return 0
	case "blob":
		return 1
	case "mediumblob":
		return 2
	case "longblob":
		return 3
	default:
		return -1
	}
}

func applySafeUpsertDecisions(
	upsert *schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	decisions []SchemaContractDecision,
) {
	for _, decision := range decisions {
		key := schemaContractTableKey{
			schema: decision.Object.Schema,
			table:  decision.Object.Table,
		}
		switch decision.Action {
		case SchemaContractCreateTable:
			if table, ok := findSchemaContractTable(current, key); ok {
				setSchemaContractTable(upsert, table)
			}
		case SchemaContractAddColumn:
			applySchemaContractColumnAdd(
				upsert,
				current,
				key,
				decision.Object.Column,
			)
		case SchemaContractRelaxNullability:
			updateSchemaContractColumn(
				upsert,
				current,
				key,
				decision.Object.Column,
				func(target *schema.SnapshotColumn, source schema.SnapshotColumn) {
					target.Nullable = source.Nullable
				},
			)
		case SchemaContractWidenType:
			updateSchemaContractColumn(
				upsert,
				current,
				key,
				decision.Object.Column,
				func(target *schema.SnapshotColumn, source schema.SnapshotColumn) {
					target.Type = source.Type
					target.DeclaredType = cloneSnapshotDeclaredType(source.DeclaredType)
				},
			)
		}
	}
}

func applySchemaContractColumnAdd(
	target *schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	key schemaContractTableKey,
	columnName string,
) {
	currentTable, ok := findSchemaContractTable(current, key)
	if !ok {
		return
	}
	var currentColumn schema.SnapshotColumn
	currentPosition := -1
	for index, column := range currentTable.Columns {
		if column.Name == columnName {
			currentColumn = cloneSnapshotColumn(column)
			currentPosition = index
			break
		}
	}
	if currentPosition < 0 {
		return
	}
	for tableIndex := range target.Tables {
		if schemaContractKey(target.Tables[tableIndex]) != key {
			continue
		}
		columns := target.Tables[tableIndex].Columns
		insertAt := len(columns)
		for index := currentPosition + 1; index < len(currentTable.Columns); index++ {
			next := currentTable.Columns[index].Name
			for targetIndex, targetColumn := range columns {
				if targetColumn.Name == next {
					insertAt = targetIndex
					index = len(currentTable.Columns)
					break
				}
			}
		}
		columns = append(columns, schema.SnapshotColumn{})
		copy(columns[insertAt+1:], columns[insertAt:])
		columns[insertAt] = currentColumn
		target.Tables[tableIndex].Columns = columns
		return
	}
}

func updateSchemaContractColumn(
	target *schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	key schemaContractTableKey,
	columnName string,
	update func(*schema.SnapshotColumn, schema.SnapshotColumn),
) {
	currentTable, ok := findSchemaContractTable(current, key)
	if !ok {
		return
	}
	var currentColumn schema.SnapshotColumn
	found := false
	for _, column := range currentTable.Columns {
		if column.Name == columnName {
			currentColumn = column
			found = true
			break
		}
	}
	if !found {
		return
	}
	for tableIndex := range target.Tables {
		if schemaContractKey(target.Tables[tableIndex]) != key {
			continue
		}
		for columnIndex := range target.Tables[tableIndex].Columns {
			if target.Tables[tableIndex].Columns[columnIndex].Name == columnName {
				update(
					&target.Tables[tableIndex].Columns[columnIndex],
					currentColumn,
				)
				return
			}
		}
	}
}

func projectSchemaContractSnapshot(
	target *schema.SchemaSnapshot,
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
	retainedTypeColumns map[schemaContractTableKey]map[string]struct{},
) {
	tables := make([]schema.SnapshotTable, 0, len(target.Tables))
	for _, table := range target.Tables {
		key := schemaContractKey(table)
		if _, discarded := discardedTables[key]; discarded {
			continue
		}
		columns := discardedColumns[key]
		retained := retainedTypeColumns[key]
		if len(columns) > 0 {
			filtered := make([]schema.SnapshotColumn, 0, len(table.Columns))
			for _, column := range table.Columns {
				if _, discard := columns[column.Name]; discard {
					if _, retain := retained[column.Name]; !retain {
						continue
					}
				}
				filtered = append(filtered, column)
			}
			table.Columns = filtered
		}
		table = pruneSchemaContractDependencies(
			table,
			columns,
			discardedTables,
			discardedColumns,
		)
		tables = append(tables, table)
	}
	target.Tables = tables
}

func restoreDiscardedTypeEvidence(
	target *schema.SchemaSnapshot,
	previous schema.SchemaSnapshot,
	columns map[schemaContractTableKey]map[string]struct{},
) {
	for tableIndex := range target.Tables {
		key := schemaContractKey(target.Tables[tableIndex])
		previousTable, ok := findSchemaContractTable(previous, key)
		if !ok {
			continue
		}
		for columnIndex := range target.Tables[tableIndex].Columns {
			name := target.Tables[tableIndex].Columns[columnIndex].Name
			if _, restore := columns[key][name]; !restore {
				continue
			}
			for _, previousColumn := range previousTable.Columns {
				if previousColumn.Name == name {
					target.Tables[tableIndex].Columns[columnIndex] =
						cloneSnapshotColumn(previousColumn)
					break
				}
			}
		}
	}
}

func restoreRebuildSourceDrops(
	target *schema.SchemaSnapshot,
	previous schema.SchemaSnapshot,
	decisions []SchemaContractDecision,
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) error {
	droppedTables := make(map[schemaContractTableKey]struct{})
	droppedColumns := make(
		map[schemaContractTableKey]map[string]struct{},
	)
	for _, decision := range decisions {
		if decision.Action != SchemaContractRetainTarget &&
			decision.Action != SchemaContractReport {
			continue
		}
		key := schemaContractTableKey{
			schema: decision.Object.Schema,
			table:  decision.Object.Table,
		}
		switch decision.ChangeKind {
		case schema.SchemaDriftTableDropped:
			droppedTables[key] = struct{}{}
		case schema.SchemaDriftColumnDropped:
			addSchemaContractColumn(
				droppedColumns,
				key,
				decision.Object.Column,
			)
		}
	}
	if len(droppedTables) == 0 && len(droppedColumns) == 0 {
		return nil
	}

	for _, key := range sortedSchemaContractTableKeys(droppedTables) {
		if _, excluded := discardedTables[key]; excluded {
			continue
		}
		previousTable, ok := findSchemaContractTable(previous, key)
		if !ok {
			return fmt.Errorf(
				"dropped table %s has no previous evidence",
				schemaContractQualifiedKey(key),
			)
		}
		if _, exists := findSchemaContractTable(*target, key); exists {
			return fmt.Errorf(
				"dropped table %s conflicts with current evidence",
				schemaContractQualifiedKey(key),
			)
		}
		setSchemaContractTable(target, previousTable)
	}

	for _, key := range sortedSchemaContractColumnKeys(droppedColumns) {
		if _, excluded := discardedTables[key]; excluded {
			continue
		}
		previousTable, ok := findSchemaContractTable(previous, key)
		if !ok {
			return fmt.Errorf(
				"dropped columns for %s have no previous table evidence",
				schemaContractQualifiedKey(key),
			)
		}
		tableIndex := schemaContractTableIndex(*target, key)
		if tableIndex < 0 {
			return fmt.Errorf(
				"dropped columns for %s have no current rebuild table",
				schemaContractQualifiedKey(key),
			)
		}
		if err := restoreRebuildTableColumns(
			&target.Tables[tableIndex],
			previousTable,
			droppedColumns[key],
			discardedTables,
			discardedColumns,
		); err != nil {
			return err
		}
	}

	// Restore inbound FKs after every retained table/column exists. This
	// prevents rebuilding another selected table from destructively dropping
	// a relationship whose referenced source object disappeared.
	for _, previousOwner := range previous.Tables {
		ownerKey := schemaContractKey(previousOwner)
		if _, excluded := discardedTables[ownerKey]; excluded {
			continue
		}
		ownerIndex := schemaContractTableIndex(*target, ownerKey)
		if ownerIndex < 0 {
			continue
		}
		for _, foreignKey := range previousOwner.ForeignKeys {
			if !schemaContractForeignKeyDependsOnSourceDrop(
				previousOwner,
				foreignKey,
				droppedTables,
				droppedColumns,
			) {
				continue
			}
			if schemaContractRestoredForeignKeyExcluded(
				previousOwner,
				foreignKey,
				discardedTables,
				discardedColumns,
			) {
				continue
			}
			if err := mergeSchemaContractForeignKey(
				&target.Tables[ownerIndex].ForeignKeys,
				foreignKey,
			); err != nil {
				return fmt.Errorf(
					"retain source-drop foreign key on %s: %w",
					schemaContractQualifiedKey(ownerKey),
					err,
				)
			}
		}
	}
	// Restoration is deliberately last, so reapply every active exclusion as
	// a final invariant. This covers complete restored tables and any
	// multi-column dependency spanning a retained drop and a discarded object.
	projectSchemaContractSnapshot(
		target,
		discardedTables,
		discardedColumns,
		nil,
	)
	return nil
}

func restoreRebuildTableColumns(
	target *schema.SnapshotTable,
	previous schema.SnapshotTable,
	dropped map[string]struct{},
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) error {
	existing := make(map[string]struct{}, len(target.Columns))
	for _, column := range target.Columns {
		existing[column.Name] = struct{}{}
	}
	for previousIndex, column := range previous.Columns {
		if _, restore := dropped[column.Name]; !restore {
			continue
		}
		if _, collision := existing[column.Name]; collision {
			return fmt.Errorf(
				"retained column %s.%s conflicts with current evidence",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				column.Name,
			)
		}
		insertAt := len(target.Columns)
		for next := previousIndex + 1; next < len(previous.Columns); next++ {
			for targetIndex, targetColumn := range target.Columns {
				if targetColumn.Name == previous.Columns[next].Name {
					insertAt = targetIndex
					next = len(previous.Columns)
					break
				}
			}
		}
		target.Columns = append(target.Columns, schema.SnapshotColumn{})
		copy(target.Columns[insertAt+1:], target.Columns[insertAt:])
		target.Columns[insertAt] = cloneSnapshotColumn(column)
		existing[column.Name] = struct{}{}
	}

	if previous.Identity != nil {
		if _, restore := dropped[previous.Identity.Column]; restore {
			switch {
			case target.Identity == nil:
				identity := *previous.Identity
				target.Identity = &identity
			case !reflect.DeepEqual(target.Identity, previous.Identity):
				return fmt.Errorf(
					"retained identity on %s conflicts with current identity",
					schemaContractQualifiedKey(schemaContractKey(previous)),
				)
			}
		}
	}
	for _, index := range previous.Indexes {
		if !schemaContractIndexUsesAny(index, dropped) {
			continue
		}
		if schemaContractIndexUsesAny(
			index,
			discardedColumns[schemaContractKey(previous)],
		) {
			continue
		}
		if err := mergeSchemaContractIndex(&target.Indexes, index); err != nil {
			return fmt.Errorf(
				"retain source-drop index on %s: %w",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				err,
			)
		}
	}
	for _, check := range previous.Checks {
		if !schemaContractCheckUsesAny(check.Expression, dropped) {
			continue
		}
		if schemaContractCheckUsesAny(
			check.Expression,
			discardedColumns[schemaContractKey(previous)],
		) {
			continue
		}
		if err := mergeSchemaContractCheck(&target.Checks, check); err != nil {
			return fmt.Errorf(
				"retain source-drop check on %s: %w",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				err,
			)
		}
	}
	for _, foreignKey := range previous.ForeignKeys {
		if !schemaContractStringsUseAny(foreignKey.Columns, dropped) {
			continue
		}
		if schemaContractRestoredForeignKeyExcluded(
			previous,
			foreignKey,
			discardedTables,
			discardedColumns,
		) {
			continue
		}
		if err := mergeSchemaContractForeignKey(
			&target.ForeignKeys,
			foreignKey,
		); err != nil {
			return fmt.Errorf(
				"retain source-drop foreign key on %s: %w",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				err,
			)
		}
	}
	return nil
}

func schemaContractForeignKeyDependsOnSourceDrop(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	droppedTables map[schemaContractTableKey]struct{},
	droppedColumns map[schemaContractTableKey]map[string]struct{},
) bool {
	for key := range droppedTables {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) {
			return true
		}
	}
	for key, columns := range droppedColumns {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) &&
			schemaContractStringsUseAny(
				foreignKey.ReferencedColumns,
				columns,
			) {
			return true
		}
	}
	return false
}

func schemaContractRestoredForeignKeyExcluded(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) bool {
	if schemaContractStringsUseAny(
		foreignKey.Columns,
		discardedColumns[schemaContractKey(owner)],
	) {
		return true
	}
	if schemaContractFKReferencesDiscarded(
		owner,
		foreignKey,
		discardedTables,
	) {
		return true
	}
	return schemaContractFKReferencesDiscardedColumns(
		owner,
		foreignKey,
		discardedColumns,
	)
}

func mergeSchemaContractIndex(
	target *[]schema.SnapshotIndex,
	value schema.SnapshotIndex,
) error {
	for _, existing := range *target {
		if schemaContractSideObjectMatches(
			existing.Name,
			value.Name,
			existing,
			value,
		) {
			if reflect.DeepEqual(existing, value) {
				return nil
			}
			return fmt.Errorf("index %q has conflicting current metadata", value.Name)
		}
	}
	*target = append(*target, value)
	return nil
}

func mergeSchemaContractForeignKey(
	target *[]schema.SnapshotForeignKey,
	value schema.SnapshotForeignKey,
) error {
	for _, existing := range *target {
		if schemaContractSideObjectMatches(
			existing.Name,
			value.Name,
			existing,
			value,
		) {
			if reflect.DeepEqual(existing, value) {
				return nil
			}
			return fmt.Errorf(
				"foreign key %q has conflicting current metadata",
				value.Name,
			)
		}
	}
	*target = append(*target, value)
	return nil
}

func mergeSchemaContractCheck(
	target *[]schema.SnapshotCheckConstraint,
	value schema.SnapshotCheckConstraint,
) error {
	for _, existing := range *target {
		if schemaContractSideObjectMatches(
			existing.Name,
			value.Name,
			existing,
			value,
		) {
			if reflect.DeepEqual(existing, value) {
				return nil
			}
			return fmt.Errorf("check %q has conflicting current metadata", value.Name)
		}
	}
	*target = append(*target, value)
	return nil
}

func schemaContractSideObjectMatches(
	leftName,
	rightName string,
	left,
	right any,
) bool {
	if leftName != "" || rightName != "" {
		return leftName == rightName
	}
	return reflect.DeepEqual(left, right)
}

func sortedSchemaContractTableKeys(
	values map[schemaContractTableKey]struct{},
) []schemaContractTableKey {
	result := make([]schemaContractTableKey, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sortSchemaContractTableKeys(result)
	return result
}

func sortedSchemaContractColumnKeys(
	values map[schemaContractTableKey]map[string]struct{},
) []schemaContractTableKey {
	result := make([]schemaContractTableKey, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sortSchemaContractTableKeys(result)
	return result
}

func sortSchemaContractTableKeys(values []schemaContractTableKey) {
	sort.Slice(values, func(left, right int) bool {
		if values[left].schema != values[right].schema {
			return values[left].schema < values[right].schema
		}
		return values[left].table < values[right].table
	})
}

func schemaContractQualifiedKey(key schemaContractTableKey) string {
	if key.schema == "" {
		return key.table
	}
	return key.schema + "." + key.table
}

func schemaContractTableIndex(
	snapshot schema.SchemaSnapshot,
	key schemaContractTableKey,
) int {
	for index := range snapshot.Tables {
		if schemaContractKey(snapshot.Tables[index]) == key {
			return index
		}
	}
	return -1
}

func pruneSchemaContractDependencies(
	table schema.SnapshotTable,
	columns map[string]struct{},
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) schema.SnapshotTable {
	if len(columns) > 0 {
		indexes := table.Indexes[:0]
		for _, index := range table.Indexes {
			if !schemaContractIndexUsesAny(index, columns) {
				indexes = append(indexes, index)
			}
		}
		table.Indexes = indexes

		checks := table.Checks[:0]
		for _, check := range table.Checks {
			if !schemaContractCheckUsesAny(check.Expression, columns) {
				checks = append(checks, check)
			}
		}
		table.Checks = checks
	}

	foreignKeys := table.ForeignKeys[:0]
	for _, foreignKey := range table.ForeignKeys {
		if schemaContractStringsUseAny(foreignKey.Columns, columns) ||
			schemaContractFKReferencesDiscarded(
				table,
				foreignKey,
				discardedTables,
			) {
			continue
		}
		if schemaContractFKReferencesDiscardedColumns(
			table,
			foreignKey,
			discardedColumns,
		) {
			continue
		}
		foreignKeys = append(foreignKeys, foreignKey)
	}
	table.ForeignKeys = foreignKeys
	return table
}

func schemaContractIndexUsesAny(
	index schema.SnapshotIndex,
	columns map[string]struct{},
) bool {
	for _, column := range index.Columns {
		if _, exists := columns[column.Name]; exists {
			return true
		}
	}
	return false
}

func schemaContractStringsUseAny(
	values []string,
	columns map[string]struct{},
) bool {
	for _, value := range values {
		if _, exists := columns[value]; exists {
			return true
		}
	}
	return false
}

func schemaContractFKReferencesDiscarded(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	discarded map[schemaContractTableKey]struct{},
) bool {
	for key := range discarded {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) {
			return true
		}
	}
	return false
}

func schemaContractFKReferencesDiscardedColumns(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	discarded map[schemaContractTableKey]map[string]struct{},
) bool {
	for key, columns := range discarded {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) &&
			schemaContractStringsUseAny(
				foreignKey.ReferencedColumns,
				columns,
			) {
			return true
		}
	}
	return false
}

func schemaContractReferencedTableMatches(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	key schemaContractTableKey,
) bool {
	reference := foreignKey.ReferencedTable
	if reference == key.schema+"."+key.table && key.schema != "" {
		return true
	}
	if reference != key.table {
		return false
	}
	// An unqualified reference is conservatively treated as matching every
	// same-named discarded table. Over-pruning is safe; retaining a dangling FK
	// is not.
	return owner.Schema == key.schema || key.schema == ""
}

func schemaContractCheckUsesAny(
	expression string,
	columns map[string]struct{},
) bool {
	identifiers, complete := schemaContractSQLIdentifiers(expression)
	if !complete {
		return true
	}
	for _, identifier := range identifiers {
		if _, exists := columns[identifier]; exists {
			return true
		}
	}
	return false
}

func schemaContractSQLIdentifiers(value string) ([]string, bool) {
	result := make([]string, 0)
	for index := 0; index < len(value); {
		switch value[index] {
		case '\'':
			index++
			closed := false
			for index < len(value) {
				if value[index] != '\'' {
					index++
					continue
				}
				if index+1 < len(value) && value[index+1] == '\'' {
					index += 2
					continue
				}
				index++
				closed = true
				break
			}
			if !closed {
				return nil, false
			}
		case '"':
			index++
			var identifier strings.Builder
			closed := false
			for index < len(value) {
				if value[index] != '"' {
					identifier.WriteByte(value[index])
					index++
					continue
				}
				if index+1 < len(value) && value[index+1] == '"' {
					identifier.WriteByte('"')
					index += 2
					continue
				}
				index++
				closed = true
				break
			}
			if !closed {
				return nil, false
			}
			result = append(result, identifier.String())
		default:
			if value[index] == '_' ||
				unicode.IsLetter(rune(value[index])) {
				start := index
				index++
				for index < len(value) &&
					(value[index] == '_' ||
						value[index] == '$' ||
						unicode.IsLetter(rune(value[index])) ||
						unicode.IsDigit(rune(value[index]))) {
					index++
				}
				result = append(result, value[start:index])
				continue
			}
			index++
		}
	}
	return result, true
}

func normalizeSchemaContractPlan(plan *SchemaContractPlan) error {
	for _, item := range []struct {
		name   string
		target *schema.SchemaSnapshot
	}{
		{name: "transfer", target: &plan.TransferSnapshot},
		{name: "validation", target: &plan.ValidationSnapshot},
		{name: "successful", target: &plan.SuccessfulSnapshot},
		{name: "upsert", target: &plan.UpsertSnapshot},
		{name: "rebuild", target: &plan.RebuildSnapshot},
	} {
		normalized, err := canonicalSchemaSnapshot(*item.target)
		if err != nil {
			return fmt.Errorf("%s snapshot: %w", item.name, err)
		}
		if _, err := schema.CompareSchemaSnapshots(normalized, normalized); err != nil {
			return fmt.Errorf("%s snapshot: %w", item.name, err)
		}
		*item.target = normalized
	}
	sort.SliceStable(plan.Decisions, func(left, right int) bool {
		leftJSON, _ := json.Marshal(plan.Decisions[left])
		rightJSON, _ := json.Marshal(plan.Decisions[right])
		return bytes.Compare(leftJSON, rightJSON) < 0
	})
	return nil
}

func canonicalSchemaSnapshot(
	value schema.SchemaSnapshot,
) (schema.SchemaSnapshot, error) {
	encoded, err := value.CanonicalJSON()
	if err != nil {
		return schema.SchemaSnapshot{}, err
	}
	return schema.ParseSchemaSnapshot(encoded)
}

func cloneSchemaSnapshot(value schema.SchemaSnapshot) schema.SchemaSnapshot {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("clone schema contract snapshot: %v", err))
	}
	var result schema.SchemaSnapshot
	if err := json.Unmarshal(encoded, &result); err != nil {
		panic(fmt.Sprintf("clone schema contract snapshot: %v", err))
	}
	return result
}

func cloneSnapshotColumn(value schema.SnapshotColumn) schema.SnapshotColumn {
	result := value
	result.DeclaredType = cloneSnapshotDeclaredType(value.DeclaredType)
	if value.Default != nil {
		defaultValue := *value.Default
		result.Default = &defaultValue
	}
	return result
}

func cloneSnapshotDeclaredType(
	value *schema.SnapshotDeclaredType,
) *schema.SnapshotDeclaredType {
	if value == nil {
		return nil
	}
	result := *value
	result.Arguments = append([]int(nil), value.Arguments...)
	return &result
}

func indexSchemaContractTables(
	snapshot schema.SchemaSnapshot,
) map[schemaContractTableKey]schema.SnapshotTable {
	result := make(
		map[schemaContractTableKey]schema.SnapshotTable,
		len(snapshot.Tables),
	)
	for _, table := range snapshot.Tables {
		result[schemaContractKey(table)] = table
	}
	return result
}

func schemaContractKey(table schema.SnapshotTable) schemaContractTableKey {
	return schemaContractTableKey{schema: table.Schema, table: table.Name}
}

func findSchemaContractTable(
	snapshot schema.SchemaSnapshot,
	key schemaContractTableKey,
) (schema.SnapshotTable, bool) {
	for _, table := range snapshot.Tables {
		if schemaContractKey(table) == key {
			return table, true
		}
	}
	return schema.SnapshotTable{}, false
}

func setSchemaContractTable(
	snapshot *schema.SchemaSnapshot,
	table schema.SnapshotTable,
) {
	key := schemaContractKey(table)
	for index := range snapshot.Tables {
		if schemaContractKey(snapshot.Tables[index]) == key {
			snapshot.Tables[index] = cloneSchemaSnapshot(
				schema.SchemaSnapshot{
					Version: schema.SchemaSnapshotVersion,
					Tables:  []schema.SnapshotTable{table},
				},
			).Tables[0]
			return
		}
	}
	snapshot.Tables = append(snapshot.Tables, cloneSchemaSnapshot(
		schema.SchemaSnapshot{
			Version: schema.SchemaSnapshotVersion,
			Tables:  []schema.SnapshotTable{table},
		},
	).Tables[0])
}

func decodeSchemaContractEvidence(
	raw json.RawMessage,
	target any,
) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing schema contract evidence")
		}
		return err
	}
	return nil
}

func addSchemaContractColumn(
	target map[schemaContractTableKey]map[string]struct{},
	key schemaContractTableKey,
	column string,
) {
	if target[key] == nil {
		target[key] = make(map[string]struct{})
	}
	target[key][column] = struct{}{}
}

func schemaContractStringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func equalSchemaContractStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
