package migrate

import (
	"encoding/json"
	"fmt"

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
