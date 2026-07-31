package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
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
	target          schema.Dialect
	sourceEngine    string
	targetEngine    string
	targetMode      string
	sourcePrior     string
	sourceCurrent   string
	projectionPrior string
	projectionNext  string
	mappings        []Stage4TargetSchemaObjectMapping
	decisions       []boundTargetSchemaEvolutionDecision
	priorTables     []schema.Table
	currentTables   []schema.Table
	createPlanner   TargetSchemaEvolutionCreatePlanner
	authorityDigest string
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
		target:          target,
		sourceEngine:    projection.SourceEngine(),
		targetEngine:    projection.TargetEngine(),
		targetMode:      projection.TargetMode(),
		sourcePrior:     projection.SourcePriorDigest(),
		sourceCurrent:   projection.SourceCurrentDigest(),
		projectionPrior: projection.PriorDigest(),
		projectionNext:  projection.CurrentDigest(),
		mappings:        projection.ObjectMappings(),
		priorTables:     projection.PriorTables(),
		currentTables:   projection.CurrentTables(),
		createPlanner:   createPlanner,
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
func PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
	reader TargetSchemaEvolutionCatalogReader,
) (TargetSchemaEvolutionPlan, error) {
	if ctx == nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target schema evolution context is required",
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if isNilInterface(reader) {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target catalog reader is required",
			nil,
		)
	}
	catalog, err := reader.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionReadFailed,
			"preflight",
			"read complete target catalog",
			err,
		)
	}
	return BuildTargetSchemaEvolutionPlan(request, catalog)
}

// BuildTargetSchemaEvolutionPlan is pure: it consumes an exact catalog and
// returns an immutable plan without database I/O or target mutation.
func BuildTargetSchemaEvolutionPlan(
	request TargetSchemaEvolutionRequest,
	catalog TargetSchemaEvolutionCatalog,
) (TargetSchemaEvolutionPlan, error) {
	definition, err := prepareTargetSchemaEvolutionDefinition(request)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := validateTargetSchemaEvolutionCatalog(catalog); err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"preflight",
			"target catalog is not complete and structurally valid",
			err,
		)
	}
	if err := prepareTargetSchemaEvolutionCreateBundle(
		&definition,
		request.createPlanner,
		catalog,
	); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}

	states, operations, err := buildTargetSchemaEvolutionStates(
		definition,
		catalog.tables,
	)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	prefix, err := matchTargetSchemaEvolutionState(
		states,
		catalog.reservations,
		catalog,
	)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	digest, err := digestTargetSchemaEvolutionPlan(
		definition.target,
		request.authorityDigest,
		catalog.reservations,
		operations,
		states,
	)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"encode immutable evolution plan",
			err,
		)
	}
	return TargetSchemaEvolutionPlan{
		target:          definition.target,
		operations:      cloneTargetSchemaEvolutionOperations(operations),
		states:          cloneTargetSchemaEvolutionStates(states),
		reservations:    cloneTargetSchemaEvolutionReservations(catalog.reservations),
		observedPrefix:  prefix,
		authorityDigest: request.authorityDigest,
		digest:          digest,
	}, nil
}

// ApplyTargetSchemaEvolution re-verifies the exact preflight state, executes
// only the pending suffix, and then verifies the exact desired catalog. An
// executor error is never returned without a best-effort catalog read that
// classifies a verified prefix when possible.
func ApplyTargetSchemaEvolution(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	session TargetSchemaEvolutionMutationSession,
) error {
	if ctx == nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"target schema evolution context is required",
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !plan.valid() {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"schema evolution plan is zero or internally incomplete",
			nil,
		)
	}
	if isNilInterface(session) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"same-session target catalog reader and executor are required",
			nil,
		)
	}
	before, err := session.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionReadFailed,
			"pre-apply verification",
			"read complete target catalog",
			err,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(before); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"pre-apply verification",
			"target catalog is not complete and structurally valid",
			err,
		)
	}
	prefix, err := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		before,
	)
	if err != nil {
		return err
	}
	if prefix != plan.observedPrefix {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"pre-apply verification",
			fmt.Sprintf(
				"catalog moved from verified prefix %d to prefix %d; rerun preflight",
				plan.observedPrefix,
				prefix,
			),
			nil,
		)
	}
	if prefix == len(plan.operations) {
		return nil
	}
	pending := cloneTargetSchemaEvolutionOperations(plan.operations[prefix:])
	executeErr := session.ExecuteTargetSchemaEvolution(ctx, pending)
	verificationCtx, cancelVerification := context.WithTimeout(
		context.WithoutCancel(ctx),
		targetSchemaEvolutionVerificationTimeout,
	)
	defer cancelVerification()
	after, readErr := session.ReadTargetSchemaEvolutionCatalog(
		verificationCtx,
	)
	if executeErr != nil {
		return classifyTargetSchemaEvolutionExecutionFailure(
			plan,
			prefix,
			after,
			readErr,
			executeErr,
		)
	}
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"execution returned success but the complete target catalog could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(after); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"execution returned success but the target catalog is structurally invalid",
			),
			err,
		)
	}
	afterPrefix, matchErr := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		after,
	)
	if matchErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"execution returned success but the catalog has unexpected or mixed drift",
			),
			matchErr,
		)
	}
	if afterPrefix != len(plan.operations) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
				"execution returned success after only prefix %d of %d",
				afterPrefix,
				len(plan.operations),
			)),
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

type targetSchemaEvolutionDefinition struct {
	target         schema.Dialect
	prior          []schema.Table
	current        []schema.Table
	priorIndex     map[targetSchemaEvolutionTableKey]schema.Table
	currentIndex   map[targetSchemaEvolutionTableKey]schema.Table
	specifications []targetSchemaEvolutionSpecification
	createObjects  []schema.SchemaDriftObject
	createTables   []schema.Table
	createBundle   CompleteTargetSchemaCreateBundle
}

type targetSchemaEvolutionSpecification struct {
	action SchemaContractAction
	object schema.SchemaDriftObject
	order  int
}

type targetSchemaEvolutionTableKey struct {
	schema string
	table  string
}

func prepareTargetSchemaEvolutionDefinition(
	request TargetSchemaEvolutionRequest,
) (targetSchemaEvolutionDefinition, error) {
	if request.authorityDigest == "" {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target schema evolution request was not constructed from projection authority",
			nil,
		)
	}
	recomputed, digestErr := digestTargetSchemaEvolutionAuthority(request)
	if digestErr != nil || recomputed != request.authorityDigest {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target schema evolution request authority changed after construction",
			digestErr,
		)
	}
	switch request.target {
	case schema.Postgres, schema.SQLServer, schema.MySQL:
	default:
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			fmt.Sprintf(
				"target dialect %q has no in-place evolution renderer",
				request.target,
			),
			nil,
		)
	}
	priorSnapshot, err := schema.NewSchemaSnapshot(request.priorTables)
	if err != nil {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target-ready prior projection is invalid",
			err,
		)
	}
	currentSnapshot, err := schema.NewSchemaSnapshot(request.currentTables)
	if err != nil {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target-ready current projection is invalid",
			err,
		)
	}
	_ = priorSnapshot
	_ = currentSnapshot

	definition := targetSchemaEvolutionDefinition{
		target:       request.target,
		prior:        cloneTargetSchemaEvolutionTables(request.priorTables),
		current:      cloneTargetSchemaEvolutionTables(request.currentTables),
		priorIndex:   indexTargetSchemaEvolutionTables(request.priorTables),
		currentIndex: indexTargetSchemaEvolutionTables(request.currentTables),
	}
	if err := collectTargetSchemaEvolutionSpecifications(
		request.decisions,
		&definition,
	); err != nil {
		return targetSchemaEvolutionDefinition{}, err
	}
	if err := validateTargetSchemaEvolutionColumnOrder(definition); err != nil {
		return targetSchemaEvolutionDefinition{}, err
	}
	if err := validateTargetSchemaEvolutionManagedSets(definition); err != nil {
		return targetSchemaEvolutionDefinition{}, err
	}
	return definition, nil
}

func prepareTargetSchemaEvolutionCreateBundle(
	definition *targetSchemaEvolutionDefinition,
	planner TargetSchemaEvolutionCreatePlanner,
	actual TargetSchemaEvolutionCatalog,
) error {
	if len(definition.createTables) == 0 {
		return nil
	}
	if isNilInterface(planner) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create planner is required for create_table decisions",
			nil,
		)
	}
	createBaseline := cloneTargetSchemaEvolutionTables(definition.createTables)
	desiredBaseline := cloneTargetSchemaEvolutionTables(definition.current)
	actualBaseline := cloneTargetSchemaEvolutionCatalog(actual)

	firstCreate := cloneTargetSchemaEvolutionTables(createBaseline)
	firstDesired := cloneTargetSchemaEvolutionTables(desiredBaseline)
	firstActual := cloneTargetSchemaEvolutionCatalog(actualBaseline)
	first, err := planner.PlanCompleteTargetSchemaCreates(
		definition.target,
		firstCreate,
		firstDesired,
		firstActual,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"plan complete target create bundle",
			err,
		)
	}
	if !reflect.DeepEqual(firstCreate, createBaseline) ||
		!reflect.DeepEqual(firstDesired, desiredBaseline) ||
		!reflect.DeepEqual(firstActual, actualBaseline) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create planner mutated immutable planning evidence",
			nil,
		)
	}

	secondCreate := cloneTargetSchemaEvolutionTables(createBaseline)
	secondDesired := cloneTargetSchemaEvolutionTables(desiredBaseline)
	secondActual := cloneTargetSchemaEvolutionCatalog(actualBaseline)
	second, err := planner.PlanCompleteTargetSchemaCreates(
		definition.target,
		secondCreate,
		secondDesired,
		secondActual,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"repeat complete target create planning",
			err,
		)
	}
	if !reflect.DeepEqual(secondCreate, createBaseline) ||
		!reflect.DeepEqual(secondDesired, desiredBaseline) ||
		!reflect.DeepEqual(secondActual, actualBaseline) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"repeated complete target create planner mutated immutable planning evidence",
			nil,
		)
	}
	if !reflect.DeepEqual(first, second) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create planner returned nondeterministic statement boundaries or catalog shapes",
			nil,
		)
	}
	if first.target != definition.target {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create bundle is bound to a different dialect",
			nil,
		)
	}
	requestedSnapshot, err := schema.NewSchemaSnapshot(
		definition.createTables,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"snapshot requested complete target create tables",
			err,
		)
	}
	equal, err := schema.SchemaSnapshotsEqual(
		first.snapshot,
		requestedSnapshot,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"compare complete target create bundle coverage",
			err,
		)
	}
	if !equal {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create bundle does not cover the exact requested tables and dependent objects",
			nil,
		)
	}
	definition.createBundle = first
	return nil
}

// validateTargetSchemaEvolutionColumnOrder proves that the durable source
// snapshot can reconstruct the exact physical target column order on every
// later run. In-place PostgreSQL ADD COLUMN can only append, so a source that
// inserted a new column before any retained column must fail closed instead of
// creating a target order that the next source snapshot cannot reproduce.
func validateTargetSchemaEvolutionColumnOrder(
	definition targetSchemaEvolutionDefinition,
) error {
	added := make(
		map[targetSchemaEvolutionTableKey]map[string]struct{},
	)
	for _, specification := range definition.specifications {
		if specification.action != SchemaContractAddColumn {
			continue
		}
		key := targetSchemaEvolutionTableKey{
			schema: specification.object.Schema,
			table:  specification.object.Table,
		}
		if added[key] == nil {
			added[key] = make(map[string]struct{})
		}
		added[key][specification.object.Column] = struct{}{}
	}
	for tableIndex := range definition.current {
		table := definition.current[tableIndex]
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		prior, existed := definition.priorIndex[key]
		if !existed {
			continue
		}
		priorIndex := 0
		sawAdded := false
		for _, currentColumn := range table.Columns {
			if _, isAdded := added[key][currentColumn.Name]; isAdded {
				sawAdded = true
				continue
			}
			if sawAdded ||
				priorIndex >= len(prior.Columns) ||
				prior.Columns[priorIndex].Name != currentColumn.Name {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"independent proof",
					fmt.Sprintf(
						"add_column cannot preserve exact durable target column order for %s: newly admitted columns must follow every retained column",
						targetSchemaEvolutionTableName(key),
					),
					nil,
				)
			}
			priorIndex++
		}
		if priorIndex != len(prior.Columns) {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"independent proof",
				fmt.Sprintf(
					"target column order for %s omits or reorders durable prior columns",
					targetSchemaEvolutionTableName(key),
				),
				nil,
			)
		}
	}
	return nil
}

func collectTargetSchemaEvolutionSpecifications(
	decisions []boundTargetSchemaEvolutionDecision,
	definition *targetSchemaEvolutionDefinition,
) error {
	seen := make(map[string]struct{})
	for _, bound := range decisions {
		decision := bound.contract
		if decision.Action == SchemaContractAbort {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"schema contract contains an abort decision",
				nil,
			)
		}
		switch decision.Action {
		case SchemaContractCreateTable,
			SchemaContractAddColumn,
			SchemaContractRelaxNullability,
			SchemaContractWidenType:
		default:
			continue
		}
		if err := validateTargetSchemaEvolutionDecision(decision); err != nil {
			return err
		}
		if bound.targetObject.Kind != decision.Object.Kind ||
			bound.targetObject.Name != decision.Object.Name ||
			bound.targetObject.Table == "" ||
			(decision.Object.Column != "" &&
				bound.targetObject.Column == "") {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"executable decision has invalid target object authority for "+
					targetSchemaEvolutionObjectName(decision.Object),
				nil,
			)
		}
		key := string(decision.Action) + "\x00" +
			bound.targetObject.Schema + "\x00" +
			bound.targetObject.Table + "\x00" +
			bound.targetObject.Column
		if _, duplicate := seen[key]; duplicate {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"schema contract contains a duplicate executable decision for "+
					targetSchemaEvolutionObjectName(bound.targetObject),
				nil,
			)
		}
		seen[key] = struct{}{}
		specification := targetSchemaEvolutionSpecification{
			action: decision.Action,
			object: bound.targetObject,
			order:  targetSchemaEvolutionActionOrder(decision.Action),
		}
		if decision.Action == SchemaContractCreateTable {
			definition.createObjects = append(
				definition.createObjects,
				bound.targetObject,
			)
			continue
		}
		definition.specifications = append(
			definition.specifications,
			specification,
		)
	}
	sort.Slice(definition.createObjects, func(left, right int) bool {
		return targetSchemaEvolutionObjectName(
			definition.createObjects[left],
		) < targetSchemaEvolutionObjectName(definition.createObjects[right])
	})
	for _, object := range definition.createObjects {
		table, exists := definition.currentIndex[targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}]
		if !exists {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"create_table decision has no target-ready current table "+
					targetSchemaEvolutionObjectName(object),
				nil,
			)
		}
		definition.createTables = append(
			definition.createTables,
			cloneStage4RichTable(table),
		)
	}
	sort.Slice(definition.specifications, func(left, right int) bool {
		leftSpec := definition.specifications[left]
		rightSpec := definition.specifications[right]
		leftTable := leftSpec.object.Schema + "\x00" + leftSpec.object.Table
		rightTable := rightSpec.object.Schema + "\x00" + rightSpec.object.Table
		if leftTable != rightTable {
			return leftTable < rightTable
		}
		if leftSpec.order != rightSpec.order {
			return leftSpec.order < rightSpec.order
		}
		if leftSpec.action == SchemaContractAddColumn {
			leftPosition := targetSchemaEvolutionColumnPosition(
				definition.currentIndex,
				leftSpec.object,
			)
			rightPosition := targetSchemaEvolutionColumnPosition(
				definition.currentIndex,
				rightSpec.object,
			)
			if leftPosition != rightPosition {
				return leftPosition < rightPosition
			}
		}
		if leftSpec.object.Column != rightSpec.object.Column {
			return leftSpec.object.Column < rightSpec.object.Column
		}
		return leftSpec.action < rightSpec.action
	})
	return nil
}

func validateTargetSchemaEvolutionDecision(
	decision SchemaContractDecision,
) error {
	if strings.TrimSpace(decision.Reason) == "" ||
		decision.Reason != strings.TrimSpace(decision.Reason) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"executable decision has no canonical reason",
			nil,
		)
	}
	if len(decision.Previous) == 0 || !json.Valid(decision.Previous) ||
		len(decision.Current) == 0 || !json.Valid(decision.Current) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"executable decision has invalid previous/current evidence",
			nil,
		)
	}
	if decision.Mode != config.SchemaContractEvolve {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			fmt.Sprintf(
				"executable decision %s has non-evolve mode %q",
				decision.Action,
				decision.Mode,
			),
			nil,
		)
	}
	if decision.Object.Table == "" {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"executable decision has no table identity",
			nil,
		)
	}
	var (
		expectedEntity schema.SchemaContractEntity
		expectedChange schema.SchemaDriftChangeKind
		expectedKind   schema.SchemaDriftObjectKind
	)
	switch decision.Action {
	case SchemaContractCreateTable:
		expectedEntity = schema.SchemaContractTables
		expectedChange = schema.SchemaDriftTableAdded
		expectedKind = schema.SchemaDriftObjectTable
		if decision.Object.Column != "" {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"create_table decision unexpectedly names a column",
				nil,
			)
		}
	case SchemaContractAddColumn,
		SchemaContractRelaxNullability:
		expectedEntity = schema.SchemaContractColumns
		if decision.Action == SchemaContractAddColumn {
			expectedChange = schema.SchemaDriftColumnAdded
			expectedKind = schema.SchemaDriftObjectColumn
		} else {
			expectedChange = schema.SchemaDriftNullabilityChanged
			expectedKind = schema.SchemaDriftObjectNullability
		}
		if decision.Object.Column == "" {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(decision.Action)+" decision has no column identity",
				nil,
			)
		}
	case SchemaContractWidenType:
		expectedEntity = schema.SchemaContractDataType
		expectedChange = schema.SchemaDriftDataTypeChanged
		expectedKind = schema.SchemaDriftObjectDataType
		if decision.Object.Column == "" {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(decision.Action)+" decision has no column identity",
				nil,
			)
		}
	}
	if decision.Entity != expectedEntity ||
		decision.ChangeKind != expectedChange ||
		decision.Object.Kind != expectedKind {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			fmt.Sprintf(
				"%s decision metadata does not match its executable action for %s",
				decision.Action,
				targetSchemaEvolutionObjectName(decision.Object),
			),
			nil,
		)
	}
	return nil
}

func validateTargetSchemaEvolutionManagedSets(
	definition targetSchemaEvolutionDefinition,
) error {
	create := make(map[targetSchemaEvolutionTableKey]struct{})
	for _, object := range definition.createObjects {
		key := targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}
		if _, exists := definition.priorIndex[key]; exists {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"create_table prior projection already contains "+
					targetSchemaEvolutionObjectName(object),
				nil,
			)
		}
		create[key] = struct{}{}
	}
	for _, specification := range definition.specifications {
		key := targetSchemaEvolutionTableKey{
			schema: specification.object.Schema,
			table:  specification.object.Table,
		}
		if _, prior := definition.priorIndex[key]; !prior {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(specification.action)+
					" requires a target-ready prior projection for "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		if _, current := definition.currentIndex[key]; !current {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(specification.action)+
					" requires a target-ready current projection for "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
	}
	for key := range definition.currentIndex {
		if _, existed := definition.priorIndex[key]; existed {
			continue
		}
		if _, authorized := create[key]; !authorized {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"target-ready current projection adds table "+
					targetSchemaEvolutionTableName(key)+
					" without a create_table decision",
				nil,
			)
		}
	}
	for key := range definition.priorIndex {
		if _, retained := definition.currentIndex[key]; !retained {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"target-ready current projection drops prior table "+
					targetSchemaEvolutionTableName(key),
				nil,
			)
		}
	}
	return nil
}

func buildTargetSchemaEvolutionStates(
	definition targetSchemaEvolutionDefinition,
	actual []schema.Table,
) ([][]schema.Table, []TargetSchemaEvolutionOperation, error) {
	managed := make(map[targetSchemaEvolutionTableKey]struct{})
	for key := range definition.priorIndex {
		managed[key] = struct{}{}
	}
	for key := range definition.currentIndex {
		managed[key] = struct{}{}
	}
	baseline := make([]schema.Table, 0, len(actual)+len(definition.prior))
	for _, table := range actual {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, isManaged := managed[key]; isManaged {
			continue
		}
		baseline = append(baseline, cloneStage4RichTable(table))
	}
	baseline = append(
		baseline,
		cloneTargetSchemaEvolutionTables(definition.prior)...,
	)
	sortTargetSchemaEvolutionTables(baseline)

	states := [][]schema.Table{
		cloneTargetSchemaEvolutionTables(baseline),
	}
	operations := make(
		[]TargetSchemaEvolutionOperation,
		0,
		len(definition.createBundle.steps)+len(definition.specifications),
	)
	currentState := cloneTargetSchemaEvolutionTables(baseline)
	if len(definition.createTables) > 0 {
		var previousCreated []schema.Table
		for _, step := range definition.createBundle.steps {
			beforeDigest, err := digestTargetSchemaEvolutionCatalog(currentState)
			if err != nil {
				return nil, nil, err
			}
			currentState = replaceTargetSchemaEvolutionCreatedTables(
				currentState,
				definition.createObjects,
				step.tables,
			)
			if err := validateTargetSchemaEvolutionCatalog(
				TargetSchemaEvolutionCatalog{tables: currentState},
			); err != nil {
				return nil, nil, targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"target create step does not produce a complete valid catalog",
					err,
				)
			}
			afterDigest, err := digestTargetSchemaEvolutionCatalog(currentState)
			if err != nil {
				return nil, nil, err
			}
			objects := changedTargetSchemaCreateObjects(
				previousCreated,
				step.tables,
			)
			operations = append(operations, TargetSchemaEvolutionOperation{
				action:       SchemaContractCreateTable,
				objects:      objects,
				statements:   []string{step.statement},
				beforeDigest: beforeDigest,
				afterDigest:  afterDigest,
			})
			states = append(
				states,
				cloneTargetSchemaEvolutionTables(currentState),
			)
			previousCreated = step.tables
		}
	}
	for _, specification := range definition.specifications {
		before := cloneTargetSchemaEvolutionTables(currentState)
		after, err := applyTargetSchemaEvolutionSpecification(
			before,
			definition.currentIndex,
			specification,
		)
		if err != nil {
			return nil, nil, err
		}
		statement, err := proveAndRenderTargetSchemaEvolution(
			definition.target,
			before,
			after,
			specification,
		)
		if err != nil {
			return nil, nil, err
		}
		beforeDigest, err := digestTargetSchemaEvolutionCatalog(before)
		if err != nil {
			return nil, nil, err
		}
		afterDigest, err := digestTargetSchemaEvolutionCatalog(after)
		if err != nil {
			return nil, nil, err
		}
		operations = append(operations, TargetSchemaEvolutionOperation{
			action:       specification.action,
			objects:      []schema.SchemaDriftObject{specification.object},
			statements:   []string{statement},
			beforeDigest: beforeDigest,
			afterDigest:  afterDigest,
		})
		currentState = after
		states = append(
			states,
			cloneTargetSchemaEvolutionTables(currentState),
		)
	}

	expectedFinal := make([]schema.Table, 0, len(currentState))
	for _, table := range baseline {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, isManaged := managed[key]; isManaged {
			continue
		}
		expectedFinal = append(expectedFinal, cloneStage4RichTable(table))
	}
	expectedFinal = append(
		expectedFinal,
		cloneTargetSchemaEvolutionTables(definition.current)...,
	)
	sortTargetSchemaEvolutionTables(expectedFinal)
	equal, err := equalCanonicalTargetSchemaEvolutionCatalog(
		expectedFinal,
		currentState,
	)
	if err != nil {
		return nil, nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"compare simulated and requested target-ready current projections",
			err,
		)
	}
	if !equal {
		return nil, nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target-ready prior and current projections contain drift not represented by executable schema-contract decisions",
			nil,
		)
	}
	return states, operations, nil
}

func applyTargetSchemaEvolutionSpecification(
	tables []schema.Table,
	current map[targetSchemaEvolutionTableKey]schema.Table,
	specification targetSchemaEvolutionSpecification,
) ([]schema.Table, error) {
	result := cloneTargetSchemaEvolutionTables(tables)
	key := targetSchemaEvolutionTableKey{
		schema: specification.object.Schema,
		table:  specification.object.Table,
	}
	tableIndex := findTargetSchemaEvolutionTable(result, key)
	if tableIndex < 0 {
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"prior target projection is missing "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	desiredTable := current[key]
	desiredColumn, desired := findTargetSchemaEvolutionColumn(
		desiredTable,
		specification.object.Column,
	)
	if !desired {
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"current target projection is missing "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	columnIndex := findTargetSchemaEvolutionColumnIndex(
		result[tableIndex],
		specification.object.Column,
	)
	switch specification.action {
	case SchemaContractAddColumn:
		if columnIndex >= 0 {
			return nil, targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"add_column prior target projection already contains "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		result[tableIndex].Columns = append(
			result[tableIndex].Columns,
			cloneStage4RichColumn(desiredColumn),
		)
	case SchemaContractRelaxNullability:
		if columnIndex < 0 {
			return nil, targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"relax_nullability prior target projection is missing "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		result[tableIndex].Columns[columnIndex].Nullable =
			desiredColumn.Nullable
	case SchemaContractWidenType:
		if columnIndex < 0 {
			return nil, targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"widen_type prior target projection is missing "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		result[tableIndex].Columns[columnIndex].Type = desiredColumn.Type
		result[tableIndex].Columns[columnIndex].DeclaredType =
			cloneStage4RichColumn(desiredColumn).DeclaredType
	default:
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"unsupported executable action "+string(specification.action),
			nil,
		)
	}
	return result, nil
}

func proveAndRenderTargetSchemaEvolution(
	target schema.Dialect,
	before []schema.Table,
	after []schema.Table,
	specification targetSchemaEvolutionSpecification,
) (string, error) {
	key := targetSchemaEvolutionTableKey{
		schema: specification.object.Schema,
		table:  specification.object.Table,
	}
	beforeIndex := findTargetSchemaEvolutionTable(before, key)
	afterIndex := findTargetSchemaEvolutionTable(after, key)
	if beforeIndex < 0 || afterIndex < 0 {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"evolution proof table is missing for "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	beforeColumn, beforeExists := findTargetSchemaEvolutionColumn(
		before[beforeIndex],
		specification.object.Column,
	)
	afterColumn, afterExists := findTargetSchemaEvolutionColumn(
		after[afterIndex],
		specification.object.Column,
	)
	if !afterExists {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"evolution proof current column is missing for "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	var (
		proof schema.ColumnEvolution
		err   error
	)
	switch specification.action {
	case SchemaContractAddColumn:
		if beforeExists {
			return "", targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"nullable-column proof found the column in prior projection",
				nil,
			)
		}
		beforeSnapshot, snapshotErr := schema.NewSchemaSnapshot(
			[]schema.Table{before[beforeIndex]},
		)
		if snapshotErr != nil {
			err = snapshotErr
			break
		}
		proof, err = schema.PlanAddNullableColumn(
			beforeSnapshot.Tables[0],
			after[afterIndex],
			afterColumn,
		)
	case SchemaContractRelaxNullability:
		if !beforeExists {
			err = fmt.Errorf("prior column is missing")
			break
		}
		proof, err = schema.PlanRelaxNullability(
			after[afterIndex],
			beforeColumn,
			afterColumn,
		)
	case SchemaContractWidenType:
		if !beforeExists {
			err = fmt.Errorf("prior column is missing")
			break
		}
		complete, catalogErr := schema.NewCompleteEvolutionCatalog(after)
		if catalogErr != nil {
			err = catalogErr
			break
		}
		proof, err = schema.PlanSafeTypeWidening(
			complete,
			after[afterIndex],
			beforeColumn,
			afterColumn,
		)
	}
	if err != nil {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"independent evolution proof rejected "+
				string(specification.action)+" for "+
				targetSchemaEvolutionObjectName(specification.object),
			err,
		)
	}
	statement, err := schema.RenderColumnEvolution(target, proof)
	if err != nil {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"render proved "+string(specification.action)+" for "+
				targetSchemaEvolutionObjectName(specification.object),
			err,
		)
	}
	return statement, nil
}

func matchTargetSchemaEvolutionState(
	states [][]schema.Table,
	reservations []TargetSchemaEvolutionNameReservation,
	actual TargetSchemaEvolutionCatalog,
) (int, error) {
	if !reflect.DeepEqual(
		canonicalTargetSchemaEvolutionReservations(reservations),
		canonicalTargetSchemaEvolutionReservations(actual.reservations),
	) {
		return 0, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"catalog comparison",
			"target namespace name reservations changed outside the deterministic evolution plan",
			nil,
		)
	}
	matches := make([]int, 0, 1)
	for index, expected := range states {
		equal, err := equalCanonicalTargetSchemaEvolutionCatalog(
			expected,
			actual.tables,
		)
		if err != nil {
			return 0, targetSchemaEvolutionError(
				TargetSchemaEvolutionCatalogDrift,
				"catalog comparison",
				fmt.Sprintf("compare catalog with evolution prefix %d", index),
				err,
			)
		}
		if equal {
			matches = append(matches, index)
		}
	}
	if len(matches) == 0 {
		return 0, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"catalog comparison",
			"target catalog is neither the exact prior shape, the exact desired shape, nor a deterministic applied prefix",
			nil,
		)
	}
	if len(matches) != 1 {
		return 0, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"catalog comparison",
			"multiple evolution prefixes have indistinguishable catalog shapes",
			nil,
		)
	}
	return matches[0], nil
}

func classifyTargetSchemaEvolutionExecutionFailure(
	plan TargetSchemaEvolutionPlan,
	startPrefix int,
	after TargetSchemaEvolutionCatalog,
	readErr error,
	executeErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(
				"execution failed and the complete target catalog could not be read",
			),
			fmt.Errorf("%w; catalog read: %v", executeErr, readErr),
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(after); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(
				"execution failed and left structurally invalid catalog evidence",
			),
			fmt.Errorf("%w; catalog validation: %v", executeErr, err),
		)
	}
	prefix, matchErr := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		after,
	)
	if matchErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(
				"execution failed and left unexpected or mixed catalog drift",
			),
			fmt.Errorf("%w; catalog classification: %v", executeErr, matchErr),
		)
	}
	if prefix < startPrefix {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
				"execution failed and catalog regressed from prefix %d to %d",
				startPrefix,
				prefix,
			)),
			executeErr,
		)
	}
	return targetSchemaEvolutionError(
		TargetSchemaEvolutionApplyFailed,
		"execution",
		targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
			"execution failed after verified prefix %d of %d",
			prefix,
			len(plan.operations),
		)),
		executeErr,
	)
}

func targetSchemaEvolutionRecoveryWording(detail string) string {
	return detail +
		"; rerun the same migration or resume so DMTX can re-read the complete target catalog and continue only from an exact verified prefix; if preflight reports catalog drift, repair or restore the target, or rebuild the affected target shape, before retrying"
}

func validateTargetSchemaEvolutionCatalog(
	catalog TargetSchemaEvolutionCatalog,
) error {
	if len(catalog.tables) != 0 {
		if _, err := schema.NewCompleteEvolutionCatalog(catalog.tables); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(catalog.reservations))
	for index, reservation := range catalog.reservations {
		if strings.TrimSpace(reservation.Scope) == "" ||
			reservation.Scope != strings.TrimSpace(reservation.Scope) ||
			strings.TrimSpace(reservation.Namespace) == "" ||
			reservation.Namespace != strings.TrimSpace(reservation.Namespace) ||
			strings.TrimSpace(reservation.Name) == "" ||
			reservation.Name != strings.TrimSpace(reservation.Name) {
			return fmt.Errorf(
				"target name reservation %d has a non-canonical scope, namespace, or name",
				index,
			)
		}
		key := reservation.Scope + "\x00" +
			reservation.Namespace + "\x00" +
			reservation.Name
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf(
				"duplicate target name reservation %s/%s/%s",
				reservation.Scope,
				reservation.Namespace,
				reservation.Name,
			)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateTargetSchemaCreateStepSubset(
	previous schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	requested schema.SchemaSnapshot,
) error {
	previousTables := indexTargetSchemaCreateSnapshotTables(previous)
	currentTables := indexTargetSchemaCreateSnapshotTables(current)
	requestedTables := indexTargetSchemaCreateSnapshotTables(requested)
	for key, table := range currentTables {
		finalTable, exists := requestedTables[key]
		if !exists {
			return fmt.Errorf(
				"statement introduces unrequested table %s",
				targetSchemaEvolutionTableName(key),
			)
		}
		if !equalTargetSchemaCreateTableCore(table, finalTable) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Indexes,
				finalTable.Indexes,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.ForeignKeys,
				finalTable.ForeignKeys,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Checks,
				finalTable.Checks,
			) {
			return fmt.Errorf(
				"statement result for %s is not a structural subset of the requested complete shape",
				targetSchemaEvolutionTableName(key),
			)
		}
	}
	for key, table := range previousTables {
		next, exists := currentTables[key]
		if !exists {
			return fmt.Errorf(
				"statement removes previously created table %s",
				targetSchemaEvolutionTableName(key),
			)
		}
		if !equalTargetSchemaCreateTableCore(table, next) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Indexes,
				next.Indexes,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.ForeignKeys,
				next.ForeignKeys,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Checks,
				next.Checks,
			) {
			return fmt.Errorf(
				"statement removes or changes previously created shape for %s",
				targetSchemaEvolutionTableName(key),
			)
		}
	}
	if previous.Version != 0 {
		equal, err := schema.SchemaSnapshotsEqual(previous, current)
		if err != nil {
			return err
		}
		if equal {
			return fmt.Errorf("statement does not advance the declared catalog shape")
		}
	}
	return nil
}

func indexTargetSchemaCreateSnapshotTables(
	snapshot schema.SchemaSnapshot,
) map[targetSchemaEvolutionTableKey]schema.SnapshotTable {
	result := make(
		map[targetSchemaEvolutionTableKey]schema.SnapshotTable,
		len(snapshot.Tables),
	)
	for _, table := range snapshot.Tables {
		result[targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = table
	}
	return result
}

func equalTargetSchemaCreateTableCore(
	left schema.SnapshotTable,
	right schema.SnapshotTable,
) bool {
	left.Indexes = nil
	left.ForeignKeys = nil
	left.Checks = nil
	right.Indexes = nil
	right.ForeignKeys = nil
	right.Checks = nil
	return reflect.DeepEqual(left, right)
}

func targetSchemaEvolutionSnapshotSubset[T any](
	subset []T,
	superset []T,
) bool {
	counts := make(map[string]int, len(superset))
	for _, value := range superset {
		encoded, err := json.Marshal(value)
		if err != nil {
			return false
		}
		counts[string(encoded)]++
	}
	for _, value := range subset {
		encoded, err := json.Marshal(value)
		if err != nil {
			return false
		}
		key := string(encoded)
		if counts[key] == 0 {
			return false
		}
		counts[key]--
	}
	return true
}

func replaceTargetSchemaEvolutionCreatedTables(
	current []schema.Table,
	objects []schema.SchemaDriftObject,
	created []schema.Table,
) []schema.Table {
	keys := make(map[targetSchemaEvolutionTableKey]struct{}, len(objects))
	for _, object := range objects {
		keys[targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}] = struct{}{}
	}
	result := make([]schema.Table, 0, len(current)+len(created))
	for _, table := range current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, isCreated := keys[key]; isCreated {
			continue
		}
		result = append(result, cloneStage4RichTable(table))
	}
	result = append(result, cloneTargetSchemaEvolutionTables(created)...)
	sortTargetSchemaEvolutionTables(result)
	return result
}

func changedTargetSchemaCreateObjects(
	previous []schema.Table,
	current []schema.Table,
) []schema.SchemaDriftObject {
	previousSnapshots := make(map[targetSchemaEvolutionTableKey]schema.SchemaSnapshot)
	for _, table := range previous {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		snapshot, _ := schema.NewSchemaSnapshot([]schema.Table{table})
		previousSnapshots[key] = snapshot
	}
	result := make([]schema.SchemaDriftObject, 0, len(current))
	for _, table := range current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		currentSnapshot, _ := schema.NewSchemaSnapshot([]schema.Table{table})
		previousSnapshot, existed := previousSnapshots[key]
		equal := false
		if existed {
			equal, _ = schema.SchemaSnapshotsEqual(
				previousSnapshot,
				currentSnapshot,
			)
		}
		if !equal {
			result = append(result, schema.SchemaDriftObject{
				Kind:   schema.SchemaDriftObjectTable,
				Schema: table.Schema,
				Table:  table.Name,
			})
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return targetSchemaEvolutionObjectName(result[left]) <
			targetSchemaEvolutionObjectName(result[right])
	})
	return result
}

func equalCanonicalTargetSchemaEvolutionCatalog(
	expected []schema.Table,
	actual []schema.Table,
) (bool, error) {
	expectedSnapshot, err := schema.NewSchemaSnapshot(expected)
	if err != nil {
		return false, err
	}
	actualSnapshot, err := schema.NewSchemaSnapshot(actual)
	if err != nil {
		return false, err
	}
	return schema.SchemaSnapshotsEqual(expectedSnapshot, actualSnapshot)
}

func digestTargetSchemaEvolutionPlan(
	target schema.Dialect,
	authorityDigest string,
	reservations []TargetSchemaEvolutionNameReservation,
	operations []TargetSchemaEvolutionOperation,
	states [][]schema.Table,
) (string, error) {
	type digestOperation struct {
		Action       SchemaContractAction       `json:"action"`
		Objects      []schema.SchemaDriftObject `json:"objects"`
		Statements   []string                   `json:"statements"`
		BeforeDigest string                     `json:"before_digest"`
		AfterDigest  string                     `json:"after_digest"`
	}
	value := struct {
		Target          schema.Dialect                         `json:"target"`
		AuthorityDigest string                                 `json:"authority_digest"`
		Reservations    []TargetSchemaEvolutionNameReservation `json:"reservations"`
		Operations      []digestOperation                      `json:"operations"`
		States          []string                               `json:"states"`
	}{
		Target:          target,
		AuthorityDigest: authorityDigest,
		Reservations: canonicalTargetSchemaEvolutionReservations(
			reservations,
		),
		Operations: make([]digestOperation, len(operations)),
		States:     make([]string, len(states)),
	}
	for index, operation := range operations {
		value.Operations[index] = digestOperation{
			Action:       operation.action,
			Objects:      append([]schema.SchemaDriftObject(nil), operation.objects...),
			Statements:   append([]string(nil), operation.statements...),
			BeforeDigest: operation.beforeDigest,
			AfterDigest:  operation.afterDigest,
		}
	}
	for index, state := range states {
		digest, err := digestTargetSchemaEvolutionCatalog(state)
		if err != nil {
			return "", err
		}
		value.States[index] = digest
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func digestTargetSchemaEvolutionCatalog(tables []schema.Table) (string, error) {
	snapshot, err := schema.NewSchemaSnapshot(tables)
	if err != nil {
		return "", err
	}
	return snapshot.Digest()
}

func indexTargetSchemaEvolutionTables(
	tables []schema.Table,
) map[targetSchemaEvolutionTableKey]schema.Table {
	result := make(map[targetSchemaEvolutionTableKey]schema.Table, len(tables))
	for _, table := range tables {
		result[targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = cloneStage4RichTable(table)
	}
	return result
}

func findTargetSchemaEvolutionTable(
	tables []schema.Table,
	key targetSchemaEvolutionTableKey,
) int {
	for index, table := range tables {
		if table.Schema == key.schema && table.Name == key.table {
			return index
		}
	}
	return -1
}

func findTargetSchemaEvolutionColumn(
	table schema.Table,
	name string,
) (schema.Column, bool) {
	index := findTargetSchemaEvolutionColumnIndex(table, name)
	if index < 0 {
		return schema.Column{}, false
	}
	return cloneStage4RichColumn(table.Columns[index]), true
}

func findTargetSchemaEvolutionColumnIndex(
	table schema.Table,
	name string,
) int {
	for index, column := range table.Columns {
		if column.Name == name {
			return index
		}
	}
	return -1
}

func targetSchemaEvolutionColumnPosition(
	tables map[targetSchemaEvolutionTableKey]schema.Table,
	object schema.SchemaDriftObject,
) int {
	table := tables[targetSchemaEvolutionTableKey{
		schema: object.Schema,
		table:  object.Table,
	}]
	return findTargetSchemaEvolutionColumnIndex(table, object.Column)
}

func targetSchemaEvolutionActionOrder(action SchemaContractAction) int {
	switch action {
	case SchemaContractCreateTable:
		return 0
	case SchemaContractAddColumn:
		return 1
	case SchemaContractRelaxNullability:
		return 2
	case SchemaContractWidenType:
		return 3
	default:
		return 100
	}
}

func targetSchemaEvolutionObjectName(object schema.SchemaDriftObject) string {
	name := object.Table
	if object.Schema != "" {
		name = object.Schema + "." + name
	}
	if object.Column != "" {
		name += "." + object.Column
	}
	return name
}

func targetSchemaEvolutionTableName(key targetSchemaEvolutionTableKey) string {
	if key.schema == "" {
		return key.table
	}
	return key.schema + "." + key.table
}

func sortTargetSchemaEvolutionTables(tables []schema.Table) {
	sort.Slice(tables, func(left, right int) bool {
		leftKey := tables[left].Schema + "\x00" + tables[left].Name
		rightKey := tables[right].Schema + "\x00" + tables[right].Name
		return leftKey < rightKey
	})
}

func cloneTargetSchemaEvolutionTables(
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

func cloneTargetSchemaEvolutionStates(
	states [][]schema.Table,
) [][]schema.Table {
	result := make([][]schema.Table, len(states))
	for index, state := range states {
		result[index] = cloneTargetSchemaEvolutionTables(state)
	}
	return result
}

func cloneTargetSchemaEvolutionCatalog(
	catalog TargetSchemaEvolutionCatalog,
) TargetSchemaEvolutionCatalog {
	return TargetSchemaEvolutionCatalog{
		tables: cloneTargetSchemaEvolutionTables(catalog.tables),
		reservations: cloneTargetSchemaEvolutionReservations(
			catalog.reservations,
		),
	}
}

func cloneTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) []TargetSchemaEvolutionNameReservation {
	return append(
		[]TargetSchemaEvolutionNameReservation(nil),
		reservations...,
	)
}

func sortTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) {
	sort.Slice(reservations, func(left, right int) bool {
		leftKey := reservations[left].Scope + "\x00" +
			reservations[left].Namespace + "\x00" +
			reservations[left].Name
		rightKey := reservations[right].Scope + "\x00" +
			reservations[right].Namespace + "\x00" +
			reservations[right].Name
		return leftKey < rightKey
	})
}

func canonicalTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) []TargetSchemaEvolutionNameReservation {
	result := cloneTargetSchemaEvolutionReservations(reservations)
	sortTargetSchemaEvolutionReservations(result)
	return result
}

func cloneTargetSchemaEvolutionContractDecision(
	decision SchemaContractDecision,
) SchemaContractDecision {
	decision.Previous = append(json.RawMessage(nil), decision.Previous...)
	decision.Current = append(json.RawMessage(nil), decision.Current...)
	return decision
}

func sortTargetSchemaEvolutionDecisions(
	decisions []boundTargetSchemaEvolutionDecision,
) {
	sort.Slice(decisions, func(left, right int) bool {
		leftJSON, _ := json.Marshal(struct {
			Contract SchemaContractDecision   `json:"contract"`
			Target   schema.SchemaDriftObject `json:"target"`
		}{
			Contract: decisions[left].contract,
			Target:   decisions[left].targetObject,
		})
		rightJSON, _ := json.Marshal(struct {
			Contract SchemaContractDecision   `json:"contract"`
			Target   schema.SchemaDriftObject `json:"target"`
		}{
			Contract: decisions[right].contract,
			Target:   decisions[right].targetObject,
		})
		return string(leftJSON) < string(rightJSON)
	})
}

func validateTargetSchemaEvolutionProjectionAuthority(
	request TargetSchemaEvolutionRequest,
	projection Stage4TargetSchemaEvolutionProjection,
) error {
	if request.targetMode != "upsert" {
		return fmt.Errorf(
			"in-place schema evolution requires target mode upsert, got %q",
			request.targetMode,
		)
	}
	if request.sourceEngine == "" ||
		request.targetEngine == "" ||
		request.targetEngine != string(request.target) {
		return fmt.Errorf(
			"projection route %q-to-%q does not match target dialect %q",
			request.sourceEngine,
			request.targetEngine,
			request.target,
		)
	}
	if request.sourcePrior == "" ||
		request.sourceCurrent == "" ||
		request.sourcePrior != projection.SourcePriorDigest() ||
		request.sourceCurrent != projection.SourceCurrentDigest() {
		return fmt.Errorf(
			"projection has missing or unstable durable source endpoint digests",
		)
	}
	priorDigest, err := digestTargetSchemaEvolutionCatalog(
		request.priorTables,
	)
	if err != nil {
		return fmt.Errorf("digest target-ready prior projection: %w", err)
	}
	currentDigest, err := digestTargetSchemaEvolutionCatalog(
		request.currentTables,
	)
	if err != nil {
		return fmt.Errorf("digest target-ready current projection: %w", err)
	}
	if request.projectionPrior == "" ||
		request.projectionNext == "" ||
		request.projectionPrior != priorDigest ||
		request.projectionNext != currentDigest {
		return fmt.Errorf(
			"projection digests do not match its target-ready endpoint tables",
		)
	}
	if !reflect.DeepEqual(
		request.mappings,
		projection.ObjectMappings(),
	) {
		return fmt.Errorf("projection object mappings changed during construction")
	}
	seenSource := make(map[Stage4SchemaObjectIdentity]struct{}, len(request.mappings))
	seenTarget := make(map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity, len(request.mappings))
	for index, mapping := range request.mappings {
		if mapping.Source.Table == "" || mapping.Target.Table == "" {
			return fmt.Errorf("projection object mapping %d has no table identity", index)
		}
		if (mapping.Source.Column == "") != (mapping.Target.Column == "") {
			return fmt.Errorf(
				"projection object mapping %d changes table/column identity kind",
				index,
			)
		}
		if _, duplicate := seenSource[mapping.Source]; duplicate {
			return fmt.Errorf(
				"projection repeats source object %s",
				stage4TargetSchemaObjectIdentityString(mapping.Source),
			)
		}
		if priorSource, collision := seenTarget[mapping.Target]; collision &&
			priorSource != mapping.Source {
			return fmt.Errorf(
				"projection aliases source objects %s and %s to target object %s",
				stage4TargetSchemaObjectIdentityString(priorSource),
				stage4TargetSchemaObjectIdentityString(mapping.Source),
				stage4TargetSchemaObjectIdentityString(mapping.Target),
			)
		}
		seenSource[mapping.Source] = struct{}{}
		seenTarget[mapping.Target] = mapping.Source
	}
	requiredTargets := make(map[Stage4SchemaObjectIdentity]struct{})
	for _, tables := range [][]schema.Table{
		request.priorTables,
		request.currentTables,
	} {
		for _, table := range tables {
			requiredTargets[Stage4SchemaObjectIdentity{
				Schema: table.Schema,
				Table:  table.Name,
			}] = struct{}{}
			for _, column := range table.Columns {
				requiredTargets[Stage4SchemaObjectIdentity{
					Schema: table.Schema,
					Table:  table.Name,
					Column: column.Name,
				}] = struct{}{}
			}
		}
	}
	for target := range requiredTargets {
		if _, found := seenTarget[target]; !found {
			return fmt.Errorf(
				"projection has no source authority for target object %s",
				stage4TargetSchemaObjectIdentityString(target),
			)
		}
	}
	for target := range seenTarget {
		if _, found := requiredTargets[target]; !found {
			return fmt.Errorf(
				"projection mapping invents absent target object %s",
				stage4TargetSchemaObjectIdentityString(target),
			)
		}
	}
	return nil
}

func digestTargetSchemaEvolutionAuthority(
	request TargetSchemaEvolutionRequest,
) (string, error) {
	priorTablesDigest, err := digestTargetSchemaEvolutionCatalog(
		request.priorTables,
	)
	if err != nil {
		return "", err
	}
	currentTablesDigest, err := digestTargetSchemaEvolutionCatalog(
		request.currentTables,
	)
	if err != nil {
		return "", err
	}
	decisions := append(
		[]boundTargetSchemaEvolutionDecision(nil),
		request.decisions...,
	)
	for index := range decisions {
		decisions[index].contract = cloneTargetSchemaEvolutionContractDecision(
			decisions[index].contract,
		)
	}
	sortTargetSchemaEvolutionDecisions(decisions)
	mappings := append(
		[]Stage4TargetSchemaObjectMapping(nil),
		request.mappings...,
	)
	sort.Slice(mappings, func(left, right int) bool {
		leftKey := stage4TargetSchemaObjectIdentityKey(mappings[left].Source) +
			"\x00" +
			stage4TargetSchemaObjectIdentityKey(mappings[left].Target)
		rightKey := stage4TargetSchemaObjectIdentityKey(mappings[right].Source) +
			"\x00" +
			stage4TargetSchemaObjectIdentityKey(mappings[right].Target)
		return leftKey < rightKey
	})
	type digestDecision struct {
		Contract SchemaContractDecision   `json:"contract"`
		Target   schema.SchemaDriftObject `json:"target"`
	}
	value := struct {
		Target              schema.Dialect                    `json:"target"`
		SourceEngine        string                            `json:"source_engine"`
		TargetEngine        string                            `json:"target_engine"`
		TargetMode          string                            `json:"target_mode"`
		SourcePrior         string                            `json:"source_prior"`
		SourceCurrent       string                            `json:"source_current"`
		ProjectionPrior     string                            `json:"projection_prior"`
		ProjectionNext      string                            `json:"projection_next"`
		Mappings            []Stage4TargetSchemaObjectMapping `json:"mappings"`
		Decisions           []digestDecision                  `json:"decisions"`
		PriorTablesDigest   string                            `json:"prior_tables_digest"`
		CurrentTablesDigest string                            `json:"current_tables_digest"`
	}{
		Target:              request.target,
		SourceEngine:        request.sourceEngine,
		TargetEngine:        request.targetEngine,
		TargetMode:          request.targetMode,
		SourcePrior:         request.sourcePrior,
		SourceCurrent:       request.sourceCurrent,
		ProjectionPrior:     request.projectionPrior,
		ProjectionNext:      request.projectionNext,
		Mappings:            mappings,
		Decisions:           make([]digestDecision, len(decisions)),
		PriorTablesDigest:   priorTablesDigest,
		CurrentTablesDigest: currentTablesDigest,
	}
	for index, decision := range decisions {
		value.Decisions[index] = digestDecision{
			Contract: decision.contract,
			Target:   decision.targetObject,
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func cloneTargetSchemaEvolutionOperations(
	operations []TargetSchemaEvolutionOperation,
) []TargetSchemaEvolutionOperation {
	if operations == nil {
		return nil
	}
	result := make([]TargetSchemaEvolutionOperation, len(operations))
	for index, operation := range operations {
		result[index] = TargetSchemaEvolutionOperation{
			action:       operation.action,
			objects:      append([]schema.SchemaDriftObject(nil), operation.objects...),
			statements:   append([]string(nil), operation.statements...),
			beforeDigest: operation.beforeDigest,
			afterDigest:  operation.afterDigest,
		}
	}
	return result
}

func targetSchemaEvolutionError(
	kind TargetSchemaEvolutionErrorKind,
	phase string,
	reason string,
	cause error,
) error {
	return &TargetSchemaEvolutionError{
		Kind:   kind,
		Phase:  phase,
		Reason: reason,
		Cause:  cause,
	}
}
