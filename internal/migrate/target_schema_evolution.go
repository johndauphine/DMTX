package migrate

import (
	"context"
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

// TargetSchemaEvolutionErrorKind is a stable classification for the
// adapter-neutral schema-evolution lifecycle.
type TargetSchemaEvolutionErrorKind string

const (
	TargetSchemaEvolutionInvalidPlan  TargetSchemaEvolutionErrorKind = "invalid_plan"
	TargetSchemaEvolutionCatalogDrift TargetSchemaEvolutionErrorKind = "catalog_drift"
	TargetSchemaEvolutionReadFailed   TargetSchemaEvolutionErrorKind = "catalog_read_failed"
	TargetSchemaEvolutionApplyFailed  TargetSchemaEvolutionErrorKind = "apply_failed"
	TargetSchemaEvolutionVerifyFailed TargetSchemaEvolutionErrorKind = "verification_failed"
)

const targetSchemaEvolutionVerificationTimeout = 10 * time.Second

// sqliteTargetEvolutionCopySwapStatement is an immutable-operation marker, not
// executable SQL.  SQLite cannot perform the proved relax/widen transitions
// in-place.  The SQLite target adapter recognizes this marker only after it
// has independently revalidated the exact before/after catalog states and
// executes its retained-row copy/swap bundle under BEGIN IMMEDIATE.
const sqliteTargetEvolutionCopySwapStatement = "dmtx:sqlite:retained-row-copy-swap:v1"

// TargetSchemaEvolutionError preserves a classifiable failure without
// converting an uncertain or partially applied target into success.
type TargetSchemaEvolutionError struct {
	Kind   TargetSchemaEvolutionErrorKind
	Phase  string
	Reason string
	Cause  error
}

func (err *TargetSchemaEvolutionError) Error() string {
	if err == nil {
		return ""
	}
	message := "target schema evolution " + string(err.Kind)
	if err.Phase != "" {
		message += " during " + err.Phase
	}
	if err.Reason != "" {
		message += ": " + err.Reason
	}
	if err.Cause != nil {
		message += ": " + err.Cause.Error()
	}
	return message
}

func (err *TargetSchemaEvolutionError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// TargetSchemaEvolutionNameReservation records one engine-wide name which is
// not otherwise represented by a table catalog entry. Scope is an
// adapter-owned canonical collision domain such as "relation" or
// "foreign_key"; Namespace is the engine namespace in which Name is reserved.
// Reservations let a create planner reject collisions with sequences, views,
// or other objects which cannot safely be hidden behind table metadata.
type TargetSchemaEvolutionNameReservation struct {
	Scope     string
	Namespace string
	Name      string
}

// TargetSchemaEvolutionCatalog is an exhaustive logical target snapshot.
// Tables contains every in-scope and target-only table. Reservations contains
// every collision-relevant name not already represented by Tables. The fields
// are private so adapters must use NewTargetSchemaEvolutionCatalog, which
// canonicalizes and validates the evidence before it can reach planning.
type TargetSchemaEvolutionCatalog struct {
	tables       []schema.Table
	reservations []TargetSchemaEvolutionNameReservation
}

func NewTargetSchemaEvolutionCatalog(
	tables []schema.Table,
	reservations []TargetSchemaEvolutionNameReservation,
) (TargetSchemaEvolutionCatalog, error) {
	result := TargetSchemaEvolutionCatalog{
		tables:       cloneTargetSchemaEvolutionTables(tables),
		reservations: cloneTargetSchemaEvolutionReservations(reservations),
	}
	if err := validateTargetSchemaEvolutionCatalog(result); err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	sortTargetSchemaEvolutionReservations(result.reservations)
	return result, nil
}

func (catalog TargetSchemaEvolutionCatalog) Tables() []schema.Table {
	return cloneTargetSchemaEvolutionTables(catalog.tables)
}

func (catalog TargetSchemaEvolutionCatalog) Reservations() []TargetSchemaEvolutionNameReservation {
	return cloneTargetSchemaEvolutionReservations(catalog.reservations)
}

// TargetSchemaEvolutionCatalogReader must return an exhaustive logical catalog
// and every unmodeled collision-relevant name in the target namespace.
// Adapters remain responsible for taking the engine-specific lock/snapshot
// required to make one read exact.
type TargetSchemaEvolutionCatalogReader interface {
	ReadTargetSchemaEvolutionCatalog(context.Context) (TargetSchemaEvolutionCatalog, error)
}

// TargetSchemaEvolutionMutationSession owns one engine-specific lock,
// transaction, or pinned connection from the pre-mutation catalog read through
// execution and the post-mutation catalog read. Combining both methods on one
// session prevents callers from accidentally verifying on a different
// connection or outside the lock that guards DDL.
type TargetSchemaEvolutionMutationSession interface {
	TargetSchemaEvolutionCatalogReader
	ExecuteTargetSchemaEvolution(
		context.Context,
		[]TargetSchemaEvolutionOperation,
	) error
}

// TargetSchemaEvolutionCreatePlanner returns a complete, globally ordered
// create plan. Its bundle must cover every requested table including indexes,
// CHECKs, and foreign keys; the core deliberately never falls back to a bare
// CREATE TABLE renderer.
type TargetSchemaEvolutionCreatePlanner interface {
	PlanCompleteTargetSchemaCreates(
		target schema.Dialect,
		createTables []schema.Table,
		completeDesiredTables []schema.Table,
		actualCatalog TargetSchemaEvolutionCatalog,
	) (CompleteTargetSchemaCreateBundle, error)
}

// TargetSchemaCreateStep is constructor input for one target statement and the
// exact cumulative shape of all newly created tables after that statement.
// Keeping one statement per step makes implicit-commit engines resumable only
// at a catalog shape that the target planner explicitly declared.
type TargetSchemaCreateStep struct {
	Statement    schema.DDLStatement
	ResultTables []schema.Table
}

// CompleteTargetSchemaCreateBundle is an opaque completeness assertion for a
// target-provided create plan. NewCompleteTargetSchemaCreateBundle is the only
// constructor so every statement boundary, cumulative catalog shape, and the
// final complete object set are owned immutably.
type CompleteTargetSchemaCreateBundle struct {
	target   schema.Dialect
	tables   []schema.Table
	snapshot schema.SchemaSnapshot
	steps    []targetSchemaCreateStep
}

type targetSchemaCreateStep struct {
	statement string
	tables    []schema.Table
	snapshot  schema.SchemaSnapshot
}

func NewCompleteTargetSchemaCreateBundle(
	target schema.Dialect,
	tables []schema.Table,
	steps []TargetSchemaCreateStep,
) (CompleteTargetSchemaCreateBundle, error) {
	if len(tables) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"complete target create bundle has no tables",
		)
	}
	if len(steps) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"complete target create bundle has no statement steps",
		)
	}
	requestedSnapshot, err := schema.NewSchemaSnapshot(tables)
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"snapshot complete target create bundle: %w",
			err,
		)
	}
	result := CompleteTargetSchemaCreateBundle{
		target:   target,
		tables:   cloneTargetSchemaEvolutionTables(tables),
		snapshot: requestedSnapshot,
		steps:    make([]targetSchemaCreateStep, len(steps)),
	}
	var previous schema.SchemaSnapshot
	for index, step := range steps {
		statement, statementErr := schema.RenderDDLStatement(
			step.Statement,
			target,
		)
		if statementErr != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"complete target create bundle statement %d is not an opaque renderer-owned %s boundary: %w",
				index,
				target,
				statementErr,
			)
		}
		snapshot, snapshotErr := schema.NewSchemaSnapshot(step.ResultTables)
		if snapshotErr != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"snapshot complete target create bundle step %d: %w",
				index,
				snapshotErr,
			)
		}
		if len(snapshot.Tables) == 0 {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"complete target create bundle step %d has no resulting tables",
				index,
			)
		}
		if subsetErr := validateTargetSchemaCreateStepSubset(
			previous,
			snapshot,
			requestedSnapshot,
		); subsetErr != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"complete target create bundle step %d: %w",
				index,
				subsetErr,
			)
		}
		result.steps[index] = targetSchemaCreateStep{
			statement: statement,
			tables:    cloneTargetSchemaEvolutionTables(step.ResultTables),
			snapshot:  snapshot,
		}
		previous = snapshot
	}
	equal, err := schema.SchemaSnapshotsEqual(previous, requestedSnapshot)
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"compare final complete target create bundle step: %w",
			err,
		)
	}
	if !equal {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"final complete target create bundle step does not contain every requested table and dependent object",
		)
	}
	return result, nil
}

// boundTargetSchemaEvolutionDecision binds immutable source/audit evidence to the
// explicitly projected target object which it authorizes. The audit decision
// is never rewritten.
type boundTargetSchemaEvolutionDecision struct {
	contract     SchemaContractDecision
	targetObject schema.SchemaDriftObject
}

// TargetSchemaEvolutionRequest is opaque authority for one projected,
// audit-backed transition. Callers cannot substitute the live target catalog
// for durable prior intent or hand-build target identities.
type TargetSchemaEvolutionRequest struct {
	target                      schema.Dialect
	sourceEngine                string
	targetEngine                string
	targetMode                  string
	sourcePrior                 string
	sourceCurrent               string
	projectionPrior             string
	projectionNext              string
	targetAuthorityTopology     string
	targetAuthorityCatalog      string
	targetAuthorityReservations []TargetSchemaEvolutionNameReservation
	mappings                    []Stage4TargetSchemaObjectMapping
	decisions                   []boundTargetSchemaEvolutionDecision
	priorTables                 []schema.Table
	currentTables               []schema.Table
	createPlanner               TargetSchemaEvolutionCreatePlanner
	authorityDigest             string
}

// NewTargetSchemaEvolutionRequest is the only production constructor for an
// executable schema transition. It binds the durable source snapshots, the
// deterministic target projection, the original audit decisions, and their
// explicit source-to-target identities into one tamper-evident value.
func NewTargetSchemaEvolutionRequest(
	target schema.Dialect,
	projection Stage4TargetSchemaEvolutionProjection,
	createPlanner TargetSchemaEvolutionCreatePlanner,
) (TargetSchemaEvolutionRequest, error) {
	request := TargetSchemaEvolutionRequest{
		target:                      target,
		sourceEngine:                projection.SourceEngine(),
		targetEngine:                projection.TargetEngine(),
		targetMode:                  projection.TargetMode(),
		sourcePrior:                 projection.SourcePriorDigest(),
		sourceCurrent:               projection.SourceCurrentDigest(),
		projectionPrior:             projection.PriorDigest(),
		projectionNext:              projection.CurrentDigest(),
		targetAuthorityTopology:     projection.TargetAuthorityTopologyHash(),
		targetAuthorityCatalog:      projection.TargetAuthorityCatalogDigest(),
		targetAuthorityReservations: projection.TargetAuthorityReservations(),
		mappings:                    projection.ObjectMappings(),
		priorTables:                 projection.PriorTables(),
		currentTables:               projection.CurrentTables(),
		createPlanner:               createPlanner,
	}
	if err := validateTargetSchemaEvolutionProjectionAuthority(
		request,
		projection,
	); err != nil {
		return TargetSchemaEvolutionRequest{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"authority",
			"target schema projection is not executable authority",
			err,
		)
	}
	decisions := projection.Decisions()
	request.decisions = make(
		[]boundTargetSchemaEvolutionDecision,
		0,
		len(decisions),
	)
	for _, decision := range decisions {
		copied := cloneTargetSchemaEvolutionContractDecision(decision)
		targetObject := copied.Object
		switch copied.Action {
		case SchemaContractCreateTable,
			SchemaContractAddColumn,
			SchemaContractRelaxNullability,
			SchemaContractWidenType:
			source := Stage4SchemaObjectIdentity{
				Schema: copied.Object.Schema,
				Table:  copied.Object.Table,
				Column: copied.Object.Column,
			}
			mapped, found := projection.TargetObject(source)
			if !found {
				return TargetSchemaEvolutionRequest{}, targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"authority",
					"schema-contract decision has no exact target projection for "+
						targetSchemaEvolutionObjectName(copied.Object),
					nil,
				)
			}
			targetObject.Schema = mapped.Schema
			targetObject.Table = mapped.Table
			targetObject.Column = mapped.Column
		}
		request.decisions = append(
			request.decisions,
			boundTargetSchemaEvolutionDecision{
				contract:     copied,
				targetObject: targetObject,
			},
		)
	}
	sortTargetSchemaEvolutionDecisions(request.decisions)
	digest, err := digestTargetSchemaEvolutionAuthority(request)
	if err != nil {
		return TargetSchemaEvolutionRequest{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"authority",
			"encode target schema evolution authority",
			err,
		)
	}
	request.authorityDigest = digest
	return request, nil
}

// TargetSchemaEvolutionOperation is an immutable executable batch. A create
// batch may cover multiple tables so its target-specific statements can order
// base tables and dependent objects globally.
type TargetSchemaEvolutionOperation struct {
	action       SchemaContractAction
	objects      []schema.SchemaDriftObject
	statements   []string
	beforeDigest string
	afterDigest  string
}

func (operation TargetSchemaEvolutionOperation) Action() SchemaContractAction {
	return operation.action
}

func (operation TargetSchemaEvolutionOperation) Objects() []schema.SchemaDriftObject {
	return append([]schema.SchemaDriftObject(nil), operation.objects...)
}

func (operation TargetSchemaEvolutionOperation) Statements() []string {
	return append([]string(nil), operation.statements...)
}

func (operation TargetSchemaEvolutionOperation) BeforeCatalogDigest() string {
	return operation.beforeDigest
}

func (operation TargetSchemaEvolutionOperation) AfterCatalogDigest() string {
	return operation.afterDigest
}

// TargetSchemaEvolutionPlan owns a complete deterministic state machine from
// exact prior catalog through exact desired catalog. The zero value is not
// executable.
type TargetSchemaEvolutionPlan struct {
	target          schema.Dialect
	operations      []TargetSchemaEvolutionOperation
	states          [][]schema.Table
	reservations    []TargetSchemaEvolutionNameReservation
	observedPrefix  int
	authorityDigest string
	digest          string
}

func (plan TargetSchemaEvolutionPlan) Target() schema.Dialect {
	return plan.target
}

func (plan TargetSchemaEvolutionPlan) Digest() string {
	return plan.digest
}

func (plan TargetSchemaEvolutionPlan) OperationCount() int {
	return len(plan.operations)
}

func (plan TargetSchemaEvolutionPlan) AppliedPrefix() int {
	return plan.observedPrefix
}

func (plan TargetSchemaEvolutionPlan) Complete() bool {
	return plan.valid() && plan.observedPrefix == len(plan.operations)
}

func (plan TargetSchemaEvolutionPlan) PendingOperations() []TargetSchemaEvolutionOperation {
	if !plan.valid() {
		return nil
	}
	return cloneTargetSchemaEvolutionOperations(
		plan.operations[plan.observedPrefix:],
	)
}

func (plan TargetSchemaEvolutionPlan) valid() bool {
	return plan.digest != "" &&
		plan.authorityDigest != "" &&
		len(plan.states) == len(plan.operations)+1 &&
		plan.observedPrefix >= 0 &&
		plan.observedPrefix <= len(plan.operations)
}

// PreflightTargetSchemaEvolution performs catalog discovery and planning only.
// It never receives an executor and therefore cannot mutate the target.
