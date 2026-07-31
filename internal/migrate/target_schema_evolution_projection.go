package migrate

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Stage4TargetSchemaPlanner is the pure target projection seam used to turn
// source-engine metadata into the exact logical shape expected in the target.
// It deliberately excludes every target I/O and mutation capability.
type Stage4TargetSchemaPlanner interface {
	Engine() string
	PlanTables(string, []schema.Table, string) ([]schema.Table, error)
}

// Stage4PriorAwareTargetSchemaPlanner is an optional pure projection seam for
// targets whose engine-wide object-name scopes require retained target names
// to be reserved before names are allocated to newly added objects.
//
// priorSourceTables and priorTargetTables are the exact inputs and result from
// the already-validated prior projection. Implementations must not mutate any
// caller-owned metadata.
type Stage4PriorAwareTargetSchemaPlanner interface {
	PlanTablesAfterPrior(
		sourceEngine string,
		priorSourceTables []schema.Table,
		priorTargetTables []schema.Table,
		currentSourceTables []schema.Table,
		mode string,
	) ([]schema.Table, error)
}

// Stage4SchemaObjectIdentity names either a table (Column is empty) or one of
// its columns. Values are copied into and out of projection evidence.
type Stage4SchemaObjectIdentity struct {
	Schema string
	Table  string
	Column string
}

// Stage4TargetSchemaObjectMapping explicitly relates source contract identity
// to target executable identity. Schema-contract decisions remain unchanged;
// lifecycle integration must opt into this mapping when building operations.
type Stage4TargetSchemaObjectMapping struct {
	Source Stage4SchemaObjectIdentity
	Target Stage4SchemaObjectIdentity
}

// Stage4TargetSchemaEvolutionProjection is immutable target-ready evidence for
// one authorized schema transition. Accessors return deep copies so callers
// cannot change the evidence after its deterministic digests are established.
type Stage4TargetSchemaEvolutionProjection struct {
	sourceEngine        string
	targetEngine        string
	targetMode          string
	sourcePriorDigest   string
	sourceCurrentDigest string
	priorDigest         string
	currentDigest       string
	decisions           []SchemaContractDecision
	priorTables         []schema.Table
	currentTables       []schema.Table
	objectMappings      []Stage4TargetSchemaObjectMapping
}

func (projection Stage4TargetSchemaEvolutionProjection) SourceEngine() string {
	return projection.sourceEngine
}

func (projection Stage4TargetSchemaEvolutionProjection) TargetEngine() string {
	return projection.targetEngine
}

func (projection Stage4TargetSchemaEvolutionProjection) TargetMode() string {
	return projection.targetMode
}

func (projection Stage4TargetSchemaEvolutionProjection) SourcePriorDigest() string {
	return projection.sourcePriorDigest
}

func (projection Stage4TargetSchemaEvolutionProjection) SourceCurrentDigest() string {
	return projection.sourceCurrentDigest
}

func (projection Stage4TargetSchemaEvolutionProjection) PriorDigest() string {
	return projection.priorDigest
}

func (projection Stage4TargetSchemaEvolutionProjection) CurrentDigest() string {
	return projection.currentDigest
}

func (projection Stage4TargetSchemaEvolutionProjection) PriorTables() []schema.Table {
	return cloneStage4TargetSchemaProjectionTables(projection.priorTables)
}

func (projection Stage4TargetSchemaEvolutionProjection) CurrentTables() []schema.Table {
	return cloneStage4TargetSchemaProjectionTables(projection.currentTables)
}

func (projection Stage4TargetSchemaEvolutionProjection) Decisions() []SchemaContractDecision {
	result := make(
		[]SchemaContractDecision,
		len(projection.decisions),
	)
	for index, decision := range projection.decisions {
		result[index] = cloneTargetSchemaEvolutionContractDecision(decision)
	}
	return result
}

func (projection Stage4TargetSchemaEvolutionProjection) ObjectMappings() []Stage4TargetSchemaObjectMapping {
	return append(
		[]Stage4TargetSchemaObjectMapping(nil),
		projection.objectMappings...,
	)
}

// TargetObject returns the explicit target identity for one source table or
// column identity. It does not rewrite a schema-contract decision.
func (projection Stage4TargetSchemaEvolutionProjection) TargetObject(
	source Stage4SchemaObjectIdentity,
) (Stage4SchemaObjectIdentity, bool) {
	index := sort.Search(
		len(projection.objectMappings),
		func(index int) bool {
			return stage4TargetSchemaObjectIdentityKey(
				projection.objectMappings[index].Source,
			) >= stage4TargetSchemaObjectIdentityKey(source)
		},
	)
	if index == len(projection.objectMappings) ||
		projection.objectMappings[index].Source != source {
		return Stage4SchemaObjectIdentity{}, false
	}
	return projection.objectMappings[index].Target, true
}

// BuildStage4TargetSchemaEvolutionProjection reconstructs both endpoints of a
// schema transition from durable snapshot evidence and sends both through the
// same target PlanTables path. It performs no adapter I/O or mutation.
func BuildStage4TargetSchemaEvolutionProjection(
	gate Stage4SchemaGateResult,
	sourceEngine string,
	target Stage4TargetSchemaPlanner,
	targetMode string,
) (Stage4TargetSchemaEvolutionProjection, error) {
	var result Stage4TargetSchemaEvolutionProjection
	if strings.TrimSpace(sourceEngine) == "" ||
		sourceEngine != strings.TrimSpace(sourceEngine) {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: canonical source engine is required",
		)
	}
	if stage4TargetSchemaProjectionNil(target) {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner is required",
		)
	}
	if targetMode == "" {
		targetMode = "drop_recreate"
	}
	if targetMode != "upsert" && targetMode != "drop_recreate" {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: unsupported target mode %q",
			targetMode,
		)
	}
	targetEngine := target.Engine()
	if strings.TrimSpace(targetEngine) == "" ||
		targetEngine != strings.TrimSpace(targetEngine) {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: canonical target engine is required",
		)
	}

	previous, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"previous",
		gate.PreviousSnapshot,
	)
	if err != nil {
		return result, err
	}
	current, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"current",
		gate.CurrentSnapshot,
	)
	if err != nil {
		return result, err
	}
	desired := gate.Plan.RebuildSnapshot
	if targetMode == "upsert" {
		desired = gate.Plan.UpsertSnapshot
	}
	desired, err = canonicalStage4TargetSchemaProjectionSnapshot(
		"desired "+targetMode,
		desired,
	)
	if err != nil {
		return result, err
	}
	if err := validateStage4TargetSchemaProjectionGate(
		gate,
		targetMode,
		previous,
		current,
		desired,
	); err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: %w",
			err,
		)
	}
	if err := validateStage4TargetSchemaProjectionDecisionEvidence(
		previous,
		current,
		gate.Plan.Decisions,
	); err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: %w",
			err,
		)
	}
	if err := validateStage4TargetSchemaProjectionProvenance(
		previous,
		current,
		desired,
	); err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: %w",
			err,
		)
	}
	sourcePriorDigest, err := previous.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: digest durable source prior snapshot: %w",
			err,
		)
	}
	sourceCurrentDigest, err := current.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: digest current source snapshot: %w",
			err,
		)
	}

	priorSource, err := schema.MaterializeSchemaSnapshot(previous)
	if err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: materialize durable prior snapshot: %w",
			err,
		)
	}
	currentSource, err := schema.MaterializeSchemaSnapshot(desired)
	if err != nil {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: materialize desired %s snapshot: %w",
			targetMode,
			err,
		)
	}

	priorTables, priorDigest, priorMappings, err :=
		planStage4TargetSchemaProjectionEndpoint(
			"prior",
			sourceEngine,
			target,
			targetEngine,
			targetMode,
			priorSource,
			nil,
			nil,
		)
	if err != nil {
		return result, err
	}
	currentTables, currentDigest, currentMappings, err :=
		planStage4TargetSchemaProjectionEndpoint(
			"current",
			sourceEngine,
			target,
			targetEngine,
			targetMode,
			currentSource,
			priorSource,
			priorTables,
		)
	if err != nil {
		return result, err
	}
	if target.Engine() != targetEngine {
		return result, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner engine changed from %q to %q",
			targetEngine,
			target.Engine(),
		)
	}
	for sourceIdentity, priorTargetIdentity := range priorMappings {
		currentTargetIdentity, found := currentMappings[sourceIdentity]
		if !found {
			continue
		}
		if priorTargetIdentity != currentTargetIdentity {
			return result, fmt.Errorf(
				"project Stage 4 target schema evolution: source object %s maps to target object %s in prior projection and %s in current projection",
				stage4TargetSchemaObjectIdentityString(sourceIdentity),
				stage4TargetSchemaObjectIdentityString(priorTargetIdentity),
				stage4TargetSchemaObjectIdentityString(currentTargetIdentity),
			)
		}
	}
	objectMappings, err := mergeStage4TargetSchemaProjectionMappings(
		priorMappings,
		currentMappings,
	)
	if err != nil {
		return result, err
	}

	result = Stage4TargetSchemaEvolutionProjection{
		sourceEngine:        sourceEngine,
		targetEngine:        targetEngine,
		targetMode:          targetMode,
		sourcePriorDigest:   sourcePriorDigest,
		sourceCurrentDigest: sourceCurrentDigest,
		priorDigest:         priorDigest,
		currentDigest:       currentDigest,
		decisions:           cloneStage4TargetSchemaProjectionDecisions(gate.Plan.Decisions),
		priorTables:         cloneStage4TargetSchemaProjectionTables(priorTables),
		currentTables:       cloneStage4TargetSchemaProjectionTables(currentTables),
		objectMappings: append(
			[]Stage4TargetSchemaObjectMapping(nil),
			objectMappings...,
		),
	}
	return result, nil
}

func validateStage4TargetSchemaProjectionDecisionEvidence(
	previous schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	decisions []SchemaContractDecision,
) error {
	facts, err := schema.CompareSchemaSnapshots(previous, current)
	if err != nil {
		return fmt.Errorf(
			"recompute schema drift facts for decision authority: %w",
			err,
		)
	}
	if len(decisions) != len(facts) {
		return fmt.Errorf(
			"schema decision evidence count %d does not match %d exact source drift facts",
			len(decisions),
			len(facts),
		)
	}
	used := make([]bool, len(facts))
	for decisionIndex, decision := range decisions {
		match := -1
		for factIndex, fact := range facts {
			if used[factIndex] ||
				decision.Entity != fact.Entity ||
				decision.ChangeKind != fact.ChangeKind ||
				!reflect.DeepEqual(decision.Object, fact.Object) ||
				!bytes.Equal(decision.Previous, fact.Previous) ||
				!bytes.Equal(decision.Current, fact.Current) {
				continue
			}
			match = factIndex
			break
		}
		if match < 0 {
			return fmt.Errorf(
				"schema decision %d does not contain exact evidence from any current source drift fact",
				decisionIndex,
			)
		}
		used[match] = true
	}
	return nil
}

func cloneStage4TargetSchemaProjectionDecisions(
	decisions []SchemaContractDecision,
) []SchemaContractDecision {
	result := make([]SchemaContractDecision, len(decisions))
	for index, decision := range decisions {
		result[index] = cloneTargetSchemaEvolutionContractDecision(decision)
	}
	return result
}

func stage4TargetSchemaProjectionNil(target Stage4TargetSchemaPlanner) bool {
	if target == nil {
		return true
	}
	value := reflect.ValueOf(target)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func canonicalStage4TargetSchemaProjectionSnapshot(
	name string,
	snapshot schema.SchemaSnapshot,
) (schema.SchemaSnapshot, error) {
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		return schema.SchemaSnapshot{}, fmt.Errorf(
			"project Stage 4 target schema evolution: normalize %s snapshot: %w",
			name,
			err,
		)
	}
	normalized, err := schema.ParseSchemaSnapshot(encoded)
	if err != nil {
		return schema.SchemaSnapshot{}, fmt.Errorf(
			"project Stage 4 target schema evolution: parse normalized %s snapshot: %w",
			name,
			err,
		)
	}
	return normalized, nil
}

func validateStage4TargetSchemaProjectionGate(
	gate Stage4SchemaGateResult,
	targetMode string,
	previous schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	desired schema.SchemaSnapshot,
) error {
	previousEqualsCurrent, err := schema.SchemaSnapshotsEqual(previous, current)
	if err != nil {
		return fmt.Errorf("compare previous and current gate snapshots: %w", err)
	}
	previousEqualsDesired, err := schema.SchemaSnapshotsEqual(previous, desired)
	if err != nil {
		return fmt.Errorf("compare previous and desired gate snapshots: %w", err)
	}
	currentEqualsDesired, err := schema.SchemaSnapshotsEqual(current, desired)
	if err != nil {
		return fmt.Errorf("compare current and desired gate snapshots: %w", err)
	}
	successful, normalizeErr := canonicalStage4TargetSchemaProjectionSnapshot(
		"successful",
		gate.Plan.SuccessfulSnapshot,
	)
	if normalizeErr != nil {
		return normalizeErr
	}
	if err := rejectStage4RetainedSubobjectProjection(
		successful,
		desired,
	); err != nil {
		return err
	}
	if gate.Baseline {
		if gate.RebuildRequiresTargetCatalog {
			return fmt.Errorf(
				"baseline gate unexpectedly requires retained prior-only reconstruction",
			)
		}
		switch {
		case previousEqualsCurrent:
			if !previousEqualsDesired {
				return fmt.Errorf(
					"unchanged baseline gate has a different desired snapshot",
				)
			}
			if len(gate.Plan.Decisions) != 0 {
				return fmt.Errorf(
					"unchanged baseline gate contains %d schema actions",
					len(gate.Plan.Decisions),
				)
			}
		case len(previous.Tables) != 0:
			return fmt.Errorf(
				"changed baseline gate has nonempty durable prior evidence",
			)
		default:
			if !currentEqualsDesired {
				return fmt.Errorf(
					"first-run baseline desired snapshot does not equal current source evidence",
				)
			}
			if err := validateStage4TargetSchemaProjectionBaselineCreates(
				gate.Plan.Decisions,
				current,
				targetMode,
			); err != nil {
				return err
			}
		}
	} else {
		switch {
		case previousEqualsCurrent && len(gate.Plan.Decisions) != 0:
			return fmt.Errorf(
				"unchanged non-baseline gate contains %d schema actions",
				len(gate.Plan.Decisions),
			)
		case !previousEqualsCurrent && len(gate.Plan.Decisions) == 0:
			return fmt.Errorf(
				"changed non-baseline gate contains no schema actions",
			)
		}
	}

	for index, decision := range gate.Plan.Decisions {
		if !stage4TargetSchemaProjectionActionAllowed(
			targetMode,
			decision.Action,
		) {
			return fmt.Errorf(
				"schema action %d is %q and is inconsistent with target mode %q",
				index,
				decision.Action,
				targetMode,
			)
		}
	}

	rebuildCurrent, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"current-backed rebuild",
		gate.RebuildCurrentSnapshot,
	)
	if err != nil {
		return err
	}
	transfer, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"transfer",
		gate.Plan.TransferSnapshot,
	)
	if err != nil {
		return err
	}
	equal, err := schema.SchemaSnapshotsEqual(rebuildCurrent, transfer)
	if err != nil {
		return fmt.Errorf(
			"compare current-backed rebuild and transfer snapshots: %w",
			err,
		)
	}
	if !equal {
		return fmt.Errorf(
			"current-backed rebuild snapshot is inconsistent with transfer snapshot",
		)
	}
	rebuild, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"required rebuild",
		gate.Plan.RebuildSnapshot,
	)
	if err != nil {
		return err
	}
	equal, err = schema.SchemaSnapshotsEqual(rebuildCurrent, rebuild)
	if err != nil {
		return fmt.Errorf(
			"compare current-backed and required rebuild snapshots: %w",
			err,
		)
	}
	if gate.RebuildRequiresTargetCatalog == equal {
		return fmt.Errorf(
			"retained prior-only reconstruction flag is inconsistent with rebuild snapshots",
		)
	}
	return nil
}

// rejectStage4RetainedSubobjectProjection prevents one successful source
// snapshot from being mistaken for durable target-shape authority. Retaining a
// whole prior-only table is independently representable as a target-only
// catalog object on later runs. Retaining columns, indexes, CHECKs, or foreign
// keys inside a still-managed table is not: the successful source baseline
// forgets that subobject, so a later exact target projection cannot reconstruct
// it without separate immutable target-shape evidence.
func rejectStage4RetainedSubobjectProjection(
	successful schema.SchemaSnapshot,
	desired schema.SchemaSnapshot,
) error {
	successfulTables := stage4TargetSchemaProjectionSnapshotTables(successful)
	for _, desiredTable := range desired.Tables {
		identity := stage4TargetSchemaProjectionTableIdentity{
			schema: desiredTable.Schema,
			table:  desiredTable.Name,
		}
		successfulTable, managed := successfulTables[identity]
		if !managed || reflect.DeepEqual(successfulTable, desiredTable) {
			continue
		}
		return fmt.Errorf(
			"target table %s retains prior-only subobjects but the schema gate has no separate immutable target-shape evidence; retained columns, indexes, CHECKs, and foreign keys must fail closed before target evolution",
			stage4TargetSchemaProjectionIdentityString(identity),
		)
	}
	return nil
}

func validateStage4TargetSchemaProjectionBaselineCreates(
	decisions []SchemaContractDecision,
	current schema.SchemaSnapshot,
	targetMode string,
) error {
	if len(decisions) != len(current.Tables) {
		return fmt.Errorf(
			"first-run baseline has %d schema actions for %d current tables",
			len(decisions),
			len(current.Tables),
		)
	}
	requiredAction := SchemaContractCreateTable
	if targetMode == "drop_recreate" {
		requiredAction = SchemaContractRebuildCurrentShape
	}
	currentTables := stage4TargetSchemaProjectionSnapshotTables(current)
	seen := make(map[stage4TargetSchemaProjectionTableIdentity]struct{}, len(decisions))
	for index, decision := range decisions {
		identity := stage4TargetSchemaProjectionTableIdentity{
			schema: decision.Object.Schema,
			table:  decision.Object.Table,
		}
		if decision.Entity != schema.SchemaContractTables ||
			decision.ChangeKind != schema.SchemaDriftTableAdded ||
			decision.Object.Column != "" ||
			decision.Object.Name != "" ||
			decision.Action != requiredAction {
			return fmt.Errorf(
				"first-run baseline schema action %d is not an exact %q table-add decision",
				index,
				requiredAction,
			)
		}
		if _, found := currentTables[identity]; !found {
			return fmt.Errorf(
				"first-run baseline schema action %d references absent current table %s",
				index,
				stage4TargetSchemaProjectionIdentityString(identity),
			)
		}
		if _, duplicate := seen[identity]; duplicate {
			return fmt.Errorf(
				"first-run baseline repeats table-add action for %s",
				stage4TargetSchemaProjectionIdentityString(identity),
			)
		}
		seen[identity] = struct{}{}
	}
	return nil
}

func stage4TargetSchemaProjectionActionAllowed(
	targetMode string,
	action SchemaContractAction,
) bool {
	switch action {
	case SchemaContractReport,
		SchemaContractRetainTarget,
		SchemaContractDiscardRow,
		SchemaContractDiscardValue:
		return true
	case SchemaContractCreateTable,
		SchemaContractAddColumn,
		SchemaContractRelaxNullability,
		SchemaContractWidenType:
		return targetMode == "upsert"
	case SchemaContractRebuildCurrentShape:
		return targetMode == "drop_recreate"
	default:
		return false
	}
}

type stage4TargetSchemaProjectionTableIdentity struct {
	schema string
	table  string
}

func validateStage4TargetSchemaProjectionProvenance(
	previous schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	desired schema.SchemaSnapshot,
) error {
	previousByIdentity := stage4TargetSchemaProjectionSnapshotTables(previous)
	currentByIdentity := stage4TargetSchemaProjectionSnapshotTables(current)
	for _, desiredTable := range desired.Tables {
		identity := stage4TargetSchemaProjectionTableIdentity{
			schema: desiredTable.Schema,
			table:  desiredTable.Name,
		}
		previousTable, inPrevious := previousByIdentity[identity]
		currentTable, inCurrent := currentByIdentity[identity]
		switch {
		case !inPrevious && !inCurrent:
			return fmt.Errorf(
				"desired table %s has neither current nor durable prior evidence; retained prior-only reconstruction is unsupported",
				stage4TargetSchemaProjectionIdentityString(identity),
			)
		case !inCurrent:
			if !reflect.DeepEqual(desiredTable, previousTable) {
				return fmt.Errorf(
					"desired prior-only table %s is not exact durable prior evidence; retained prior-only reconstruction is unsupported",
					stage4TargetSchemaProjectionIdentityString(identity),
				)
			}
		case !inPrevious:
			if !reflect.DeepEqual(desiredTable, currentTable) {
				return fmt.Errorf(
					"desired new table %s is not exact current evidence",
					stage4TargetSchemaProjectionIdentityString(identity),
				)
			}
		default:
			if err := validateStage4TargetSchemaProjectionTableProvenance(
				identity,
				previousTable,
				currentTable,
				desiredTable,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func stage4TargetSchemaProjectionSnapshotTables(
	snapshot schema.SchemaSnapshot,
) map[stage4TargetSchemaProjectionTableIdentity]schema.SnapshotTable {
	result := make(
		map[stage4TargetSchemaProjectionTableIdentity]schema.SnapshotTable,
		len(snapshot.Tables),
	)
	for _, table := range snapshot.Tables {
		result[stage4TargetSchemaProjectionTableIdentity{
			schema: table.Schema,
			table:  table.Name,
		}] = table
	}
	return result
}

func validateStage4TargetSchemaProjectionTableProvenance(
	identity stage4TargetSchemaProjectionTableIdentity,
	previous schema.SnapshotTable,
	current schema.SnapshotTable,
	desired schema.SnapshotTable,
) error {
	if desired.MySQLCollation != previous.MySQLCollation &&
		desired.MySQLCollation != current.MySQLCollation {
		return stage4TargetSchemaProjectionUnsupportedPriorObject(
			identity,
			"MySQL collation",
		)
	}
	if !reflect.DeepEqual(desired.ClickHouseOrderBy, previous.ClickHouseOrderBy) &&
		!reflect.DeepEqual(desired.ClickHouseOrderBy, current.ClickHouseOrderBy) {
		return stage4TargetSchemaProjectionUnsupportedPriorObject(
			identity,
			"ClickHouse order key",
		)
	}
	if !reflect.DeepEqual(desired.Identity, previous.Identity) &&
		!reflect.DeepEqual(desired.Identity, current.Identity) {
		return stage4TargetSchemaProjectionUnsupportedPriorObject(
			identity,
			"identity",
		)
	}
	if desired.SQLiteWithoutRowID != previous.SQLiteWithoutRowID &&
		desired.SQLiteWithoutRowID != current.SQLiteWithoutRowID {
		return stage4TargetSchemaProjectionUnsupportedPriorObject(
			identity,
			"SQLite WITHOUT ROWID property",
		)
	}
	if desired.SQLiteStrict != previous.SQLiteStrict &&
		desired.SQLiteStrict != current.SQLiteStrict {
		return stage4TargetSchemaProjectionUnsupportedPriorObject(
			identity,
			"SQLite STRICT property",
		)
	}

	for _, item := range desired.Columns {
		if !stage4TargetSchemaProjectionContains(previous.Columns, item) &&
			!stage4TargetSchemaProjectionContains(current.Columns, item) {
			return stage4TargetSchemaProjectionUnsupportedPriorObject(
				identity,
				"column "+item.Name,
			)
		}
	}
	for _, item := range desired.Indexes {
		if !stage4TargetSchemaProjectionContains(previous.Indexes, item) &&
			!stage4TargetSchemaProjectionContains(current.Indexes, item) {
			return stage4TargetSchemaProjectionUnsupportedPriorObject(
				identity,
				"index "+item.Name,
			)
		}
	}
	for _, item := range desired.ForeignKeys {
		if !stage4TargetSchemaProjectionContains(previous.ForeignKeys, item) &&
			!stage4TargetSchemaProjectionContains(current.ForeignKeys, item) {
			return stage4TargetSchemaProjectionUnsupportedPriorObject(
				identity,
				"foreign key "+item.Name,
			)
		}
	}
	for _, item := range desired.Checks {
		if !stage4TargetSchemaProjectionContains(previous.Checks, item) &&
			!stage4TargetSchemaProjectionContains(current.Checks, item) {
			return stage4TargetSchemaProjectionUnsupportedPriorObject(
				identity,
				"CHECK "+item.Name,
			)
		}
	}
	return nil
}

func stage4TargetSchemaProjectionContains[T any](
	values []T,
	requested T,
) bool {
	for _, value := range values {
		if reflect.DeepEqual(value, requested) {
			return true
		}
	}
	return false
}

func stage4TargetSchemaProjectionUnsupportedPriorObject(
	identity stage4TargetSchemaProjectionTableIdentity,
	object string,
) error {
	return fmt.Errorf(
		"desired %s for table %s has neither exact current nor durable prior evidence; retained prior-only reconstruction is unsupported",
		object,
		stage4TargetSchemaProjectionIdentityString(identity),
	)
}

func planStage4TargetSchemaProjectionEndpoint(
	label string,
	sourceEngine string,
	target Stage4TargetSchemaPlanner,
	targetEngine string,
	targetMode string,
	sourceTables []schema.Table,
	priorSourceTables []schema.Table,
	priorTargetTables []schema.Table,
) (
	[]schema.Table,
	string,
	map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity,
	error,
) {
	var zeroMappings map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity
	baseline := cloneStage4TargetSchemaProjectionTables(sourceTables)
	plan := func(input []schema.Table) ([]schema.Table, error) {
		priorAware, ok := target.(Stage4PriorAwareTargetSchemaPlanner)
		if label != "current" || !ok {
			return target.PlanTables(sourceEngine, input, targetMode)
		}
		priorSourceInput := cloneStage4TargetSchemaProjectionTables(
			priorSourceTables,
		)
		priorTargetInput := cloneStage4TargetSchemaProjectionTables(
			priorTargetTables,
		)
		priorSourceBefore := cloneStage4TargetSchemaProjectionTables(
			priorSourceInput,
		)
		priorTargetBefore := cloneStage4TargetSchemaProjectionTables(
			priorTargetInput,
		)
		planned, err := priorAware.PlanTablesAfterPrior(
			sourceEngine,
			priorSourceInput,
			priorTargetInput,
			input,
			targetMode,
		)
		if !reflect.DeepEqual(priorSourceInput, priorSourceBefore) {
			return nil, fmt.Errorf(
				"target planner %s mutated prior source projection evidence while planning %s snapshot",
				targetEngine,
				label,
			)
		}
		if !reflect.DeepEqual(priorTargetInput, priorTargetBefore) {
			return nil, fmt.Errorf(
				"target planner %s mutated prior target projection evidence while planning %s snapshot",
				targetEngine,
				label,
			)
		}
		return planned, err
	}
	firstInput := cloneStage4TargetSchemaProjectionTables(sourceTables)
	first, firstErr := plan(firstInput)
	if !reflect.DeepEqual(firstInput, baseline) {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner %s mutated %s source metadata",
			targetEngine,
			label,
		)
	}
	if firstErr != nil {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner %s failed for %s snapshot: %w",
			targetEngine,
			label,
			firstErr,
		)
	}
	first = cloneStage4TargetSchemaProjectionTables(first)

	secondInput := cloneStage4TargetSchemaProjectionTables(sourceTables)
	second, secondErr := plan(secondInput)
	if !reflect.DeepEqual(secondInput, baseline) {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner %s mutated repeated %s source metadata",
			targetEngine,
			label,
		)
	}
	if secondErr != nil {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner %s failed for repeated %s snapshot: %w",
			targetEngine,
			label,
			secondErr,
		)
	}
	second = cloneStage4TargetSchemaProjectionTables(second)
	if !reflect.DeepEqual(first, second) {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner %s is nondeterministic for %s snapshot",
			targetEngine,
			label,
		)
	}
	if target.Engine() != targetEngine {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner engine changed while planning %s snapshot",
			label,
		)
	}
	mappings, err := validateStage4TargetSchemaProjectionIdentities(
		label,
		sourceTables,
		first,
	)
	if err != nil {
		return nil, "", zeroMappings, err
	}
	for _, table := range first {
		if table.Identity != nil && table.Identity.Frontier != nil {
			return nil, "", zeroMappings, fmt.Errorf(
				"project Stage 4 target schema evolution: target planner %s invented dynamic identity frontier for %s table %s",
				targetEngine,
				label,
				stage4TargetSchemaProjectionIdentityString(
					stage4TargetSchemaProjectionTableIdentity{
						schema: table.Schema,
						table:  table.Name,
					},
				),
			)
		}
	}
	snapshot, err := schema.NewSchemaSnapshot(first)
	if err != nil {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: snapshot target-ready %s tables: %w",
			label,
			err,
		)
	}
	digest, err := snapshot.Digest()
	if err != nil {
		return nil, "", zeroMappings, fmt.Errorf(
			"project Stage 4 target schema evolution: digest target-ready %s tables: %w",
			label,
			err,
		)
	}
	return cloneStage4TargetSchemaProjectionTables(first), digest, mappings, nil
}

func validateStage4TargetSchemaProjectionIdentities(
	label string,
	sourceTables []schema.Table,
	targetTables []schema.Table,
) (
	map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity,
	error,
) {
	if len(targetTables) != len(sourceTables) {
		return nil, fmt.Errorf(
			"project Stage 4 target schema evolution: target planner returned %d %s tables for %d source tables",
			len(targetTables),
			label,
			len(sourceTables),
		)
	}
	result := make(
		map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity,
		len(sourceTables)*2,
	)
	targetOwners := make(
		map[stage4TargetSchemaProjectionTableIdentity]stage4TargetSchemaProjectionTableIdentity,
		len(targetTables),
	)
	for index, sourceTable := range sourceTables {
		targetTable := targetTables[index]
		sourceIdentity := stage4TargetSchemaProjectionTableIdentity{
			schema: sourceTable.Schema,
			table:  sourceTable.Name,
		}
		targetIdentity := stage4TargetSchemaProjectionTableIdentity{
			schema: targetTable.Schema,
			table:  targetTable.Name,
		}
		if targetTable.Name != sourceTable.Name {
			return nil, fmt.Errorf(
				"project Stage 4 target schema evolution: target planner changed %s source table %s to %s",
				label,
				stage4TargetSchemaProjectionIdentityString(sourceIdentity),
				stage4TargetSchemaProjectionIdentityString(targetIdentity),
			)
		}
		if owner, duplicate := targetOwners[targetIdentity]; duplicate {
			return nil, fmt.Errorf(
				"project Stage 4 target schema evolution: source tables %s and %s collide at target table %s",
				stage4TargetSchemaProjectionIdentityString(owner),
				stage4TargetSchemaProjectionIdentityString(sourceIdentity),
				stage4TargetSchemaProjectionIdentityString(targetIdentity),
			)
		}
		sourceObject := Stage4SchemaObjectIdentity{
			Schema: sourceIdentity.schema,
			Table:  sourceIdentity.table,
		}
		targetObject := Stage4SchemaObjectIdentity{
			Schema: targetIdentity.schema,
			Table:  targetIdentity.table,
		}
		result[sourceObject] = targetObject
		targetOwners[targetIdentity] = sourceIdentity
		if len(targetTable.Columns) != len(sourceTable.Columns) {
			return nil, fmt.Errorf(
				"project Stage 4 target schema evolution: target planner returned %d columns for %s table %s with %d source columns",
				len(targetTable.Columns),
				label,
				stage4TargetSchemaProjectionIdentityString(targetIdentity),
				len(sourceTable.Columns),
			)
		}
		targetColumnOwners := make(map[string]string, len(targetTable.Columns))
		for columnIndex, sourceColumn := range sourceTable.Columns {
			targetColumn := targetTable.Columns[columnIndex]
			sourceColumnObject := Stage4SchemaObjectIdentity{
				Schema: sourceIdentity.schema,
				Table:  sourceIdentity.table,
				Column: sourceColumn.Name,
			}
			targetColumnObject := Stage4SchemaObjectIdentity{
				Schema: targetIdentity.schema,
				Table:  targetIdentity.table,
				Column: targetColumn.Name,
			}
			if targetColumn.Name != sourceColumn.Name {
				return nil, fmt.Errorf(
					"project Stage 4 target schema evolution: target planner changed %s source column %s to %s",
					label,
					stage4TargetSchemaObjectIdentityString(sourceColumnObject),
					stage4TargetSchemaObjectIdentityString(targetColumnObject),
				)
			}
			aliasKey := strings.ToLower(targetColumn.Name)
			if existingSource, duplicate := targetColumnOwners[aliasKey]; duplicate {
				return nil, fmt.Errorf(
					"project Stage 4 target schema evolution: source columns %s and %s collapse at target column %s",
					existingSource,
					stage4TargetSchemaObjectIdentityString(sourceColumnObject),
					stage4TargetSchemaObjectIdentityString(targetColumnObject),
				)
			}
			result[sourceColumnObject] = targetColumnObject
			targetColumnOwners[aliasKey] =
				stage4TargetSchemaObjectIdentityString(sourceColumnObject)
		}
	}
	return result, nil
}

func mergeStage4TargetSchemaProjectionMappings(
	prior map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity,
	current map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity,
) ([]Stage4TargetSchemaObjectMapping, error) {
	merged := make(
		map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity,
		len(prior)+len(current),
	)
	for source, target := range prior {
		merged[source] = target
	}
	for source, target := range current {
		if previous, found := merged[source]; found && previous != target {
			return nil, fmt.Errorf(
				"project Stage 4 target schema evolution: source object %s has conflicting target mappings %s and %s",
				stage4TargetSchemaObjectIdentityString(source),
				stage4TargetSchemaObjectIdentityString(previous),
				stage4TargetSchemaObjectIdentityString(target),
			)
		}
		merged[source] = target
	}

	sources := make([]Stage4SchemaObjectIdentity, 0, len(merged))
	for source := range merged {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(left, right int) bool {
		return stage4TargetSchemaObjectIdentityKey(sources[left]) <
			stage4TargetSchemaObjectIdentityKey(sources[right])
	})
	sourceAliases := make(map[string]Stage4SchemaObjectIdentity, len(sources))
	targetAliases := make(map[string]Stage4SchemaObjectIdentity, len(sources))
	for _, source := range sources {
		sourceAlias := stage4TargetSchemaObjectAliasKey(source)
		if previous, duplicate := sourceAliases[sourceAlias]; duplicate &&
			previous != source {
			return nil, fmt.Errorf(
				"project Stage 4 target schema evolution: source objects %s and %s are ambiguous aliases",
				stage4TargetSchemaObjectIdentityString(previous),
				stage4TargetSchemaObjectIdentityString(source),
			)
		}
		sourceAliases[sourceAlias] = source

		target := merged[source]
		targetAlias := stage4TargetSchemaObjectAliasKey(target)
		if previousSource, duplicate := targetAliases[targetAlias]; duplicate &&
			previousSource != source {
			return nil, fmt.Errorf(
				"project Stage 4 target schema evolution: source objects %s and %s collapse at target objects %s and %s",
				stage4TargetSchemaObjectIdentityString(previousSource),
				stage4TargetSchemaObjectIdentityString(source),
				stage4TargetSchemaObjectIdentityString(merged[previousSource]),
				stage4TargetSchemaObjectIdentityString(target),
			)
		}
		targetAliases[targetAlias] = source
	}
	result := make([]Stage4TargetSchemaObjectMapping, len(sources))
	for index, source := range sources {
		result[index] = Stage4TargetSchemaObjectMapping{
			Source: source,
			Target: merged[source],
		}
	}
	return result, nil
}

func stage4TargetSchemaObjectAliasKey(
	identity Stage4SchemaObjectIdentity,
) string {
	kind := "table"
	if identity.Column != "" {
		kind = "column"
	}
	return kind + "\x00" +
		strings.ToLower(identity.Schema) + "\x00" +
		strings.ToLower(identity.Table) + "\x00" +
		strings.ToLower(identity.Column)
}

func stage4TargetSchemaObjectIdentityKey(
	identity Stage4SchemaObjectIdentity,
) string {
	return identity.Schema + "\x00" + identity.Table + "\x00" + identity.Column
}

func stage4TargetSchemaObjectIdentityString(
	identity Stage4SchemaObjectIdentity,
) string {
	table := identity.Table
	if identity.Schema != "" {
		table = identity.Schema + "." + table
	}
	if identity.Column != "" {
		table += "." + identity.Column
	}
	return table
}

func stage4TargetSchemaProjectionIdentityString(
	identity stage4TargetSchemaProjectionTableIdentity,
) string {
	if identity.schema == "" {
		return identity.table
	}
	return identity.schema + "." + identity.table
}

func cloneStage4TargetSchemaProjectionTables(
	tables []schema.Table,
) []schema.Table {
	if tables == nil {
		return nil
	}
	result := make([]schema.Table, len(tables))
	for index, table := range tables {
		result[index] = cloneStage4RichTable(table)
	}
	return result
}
