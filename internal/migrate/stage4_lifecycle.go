package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4SchemaGateStrategy    = "stage4_aggregate_schema_contract_v1"
	stage4SchemaGateRangeID     = "aggregate-schema"
	stage4TargetShapeStrategy   = "stage4_target_shape_authority_v1"
	stage4TargetShapeRangeID    = "aggregate-target-shape"
	stage4TargetShapeEvidenceV1 = 1
)

var stage4SchemaGateTask = state.TaskKey{
	Type:  "schema-contract",
	Table: "aggregate-source-schema",
}

var stage4TargetShapeTask = state.TaskKey{
	Type:  "target-schema-shape",
	Table: "aggregate-target-schema",
}

// ErrStage4TargetShapeSeedRequired tells production composition to perform one
// exact read-only target catalog read and retry authority preparation. It is
// returned only after the distinct target-shape work plan exists, and never
// for a same-run staged authority.
var ErrStage4TargetShapeSeedRequired = errors.New(
	"stage 4 target-shape catalog seed required",
)

// Stage4StateBackend is the complete durable-state surface required by the
// Stage 4 migration lifecycle. Production observers must expose an already
// lease-fenced implementation; helpers never fence a raw backend themselves.
type Stage4StateBackend interface {
	state.RangeBackend
	state.Stage4Backend
}

// Stage4RunContext binds Stage 4 lifecycle work to one durable run. The spool
// directory is an absolute, stable, run-private location; resolving a context
// never creates that directory or writes state.
type Stage4RunContext struct {
	RunID          string
	Backend        Stage4StateBackend
	Resume         bool
	SpoolDirectory string
}

// Validate rejects contexts that could accidentally use an unfixed working
// directory, a shared spool root, or an incomplete durable backend.
func (run Stage4RunContext) Validate() error {
	if strings.TrimSpace(run.RunID) == "" || run.RunID != strings.TrimSpace(run.RunID) {
		return fmt.Errorf("stage 4 run ID must be non-empty and canonical")
	}
	if stage4BackendIsNil(run.Backend) {
		return fmt.Errorf("stage 4 run %q is missing the range and evidence backend", run.RunID)
	}
	if run.SpoolDirectory == "" || !filepath.IsAbs(run.SpoolDirectory) {
		return fmt.Errorf("stage 4 spool directory must be absolute")
	}
	if filepath.Clean(run.SpoolDirectory) != run.SpoolDirectory {
		return fmt.Errorf("stage 4 spool directory must be a clean stable path")
	}
	volume := filepath.VolumeName(run.SpoolDirectory)
	if run.SpoolDirectory == string(filepath.Separator) ||
		volume != "" && run.SpoolDirectory == volume+string(filepath.Separator) {
		return fmt.Errorf("stage 4 spool directory must be run-private, not a filesystem root")
	}
	runDigest := sha256.Sum256([]byte(run.RunID))
	if filepath.Base(run.SpoolDirectory) != hex.EncodeToString(runDigest[:]) {
		return fmt.Errorf("stage 4 spool directory is not bound to run %q", run.RunID)
	}
	if err := validateStage4PrivateDirectory(
		run.SpoolDirectory,
		"stage 4 spool directory",
	); err != nil {
		return err
	}
	if err := validateStage4PrivateDirectory(
		filepath.Dir(run.SpoolDirectory),
		"stage 4 spool parent",
	); err != nil {
		return err
	}
	return nil
}

func validateStage4PrivateDirectory(path, label string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", label, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%s must be a real directory, not a symlink or file", label)
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf(
			"%s permissions must be 0700 (owner-only and owner-accessible)",
			label,
		)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", label, err)
	}
	if resolved != path {
		return fmt.Errorf("%s must not traverse symlinks", label)
	}
	return nil
}

func stage4BackendIsNil(backend Stage4StateBackend) bool {
	if backend == nil {
		return true
	}
	value := reflect.ValueOf(backend)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// Stage4RunObserver is optional. Legacy/test observers can omit it; production
// lifecycle composition invokes it only when Stage 4 state is required.
type Stage4RunObserver interface {
	Stage4RunContext() (Stage4RunContext, error)
}

// Stage4SchemaDecisionReport is the complete deterministic fact published
// after the durable schema gate and its sentinel evidence have been verified.
// It contains route identity and schema digests, but never endpoint
// credentials or row data.
type Stage4SchemaDecisionReport struct {
	RunID                  string                   `json:"run_id"`
	Resume                 bool                     `json:"resume"`
	Baseline               bool                     `json:"baseline"`
	SourceEngine           string                   `json:"source_engine"`
	TargetEngine           string                   `json:"target_engine"`
	TargetMode             string                   `json:"target_mode"`
	GateTopologyHash       string                   `json:"gate_topology_hash"`
	PreviousSchemaDigest   string                   `json:"previous_schema_digest"`
	CurrentSchemaDigest    string                   `json:"current_schema_digest"`
	SuccessfulSchemaDigest string                   `json:"successful_schema_digest"`
	Decisions              []SchemaContractDecision `json:"decisions"`
}

// Stage4SchemaDecisionObserver is optional for legacy/test observers.
// Production observers implement it to durably publish every baseline,
// report, retain, evolution, and discard decision before target planning.
type Stage4SchemaDecisionObserver interface {
	ObserveStage4SchemaDecisions(
		context.Context,
		Stage4SchemaDecisionReport,
	) error
}

// ResolveStage4RunContext resolves and validates an optional observer context
// without target mutation, state writes, or spool-directory creation.
func ResolveStage4RunContext(
	observer TableObserver,
) (Stage4RunContext, bool, error) {
	stage4Observer, ok := observer.(Stage4RunObserver)
	if !ok {
		return Stage4RunContext{}, false, nil
	}
	run, err := stage4Observer.Stage4RunContext()
	if err != nil {
		return Stage4RunContext{}, true, fmt.Errorf(
			"resolve Stage 4 run context: %w",
			err,
		)
	}
	if err := run.Validate(); err != nil {
		return Stage4RunContext{}, true, fmt.Errorf(
			"resolve Stage 4 run context: %w",
			err,
		)
	}
	return run, true, nil
}

// Stage4SchemaGateOptions contains only configuration identity, never current
// discovered schema. Consequently its topology remains stable when legitimate
// source drift is the very condition the gate must evaluate.
type Stage4SchemaGateOptions struct {
	SourceEngine       string
	TargetEngine       string
	TargetMode         string
	IncludeTables      []string
	ExcludeTables      []string
	ConfigIdentity     string
	Contract           *config.SchemaContract
	FailOnSchemaDrift  bool
	DateUpdatedColumns []string
	CapturedAt         time.Time
}

// Stage4SchemaGateResult contains durable/canonical evidence and executable
// rich metadata projections. SuccessfulSnapshot and UpsertSnapshot remain in
// Plan only: both may intentionally retain prior evidence or target objects
// for which the current source has no rich AST metadata.
type Stage4SchemaGateResult struct {
	Task                         state.TaskKey
	TopologyHash                 string
	Baseline                     bool
	PreviousSnapshot             schema.SchemaSnapshot
	CurrentSnapshot              schema.SchemaSnapshot
	PendingSnapshot              state.SchemaSnapshot
	Plan                         SchemaContractPlan
	TransferTables               []schema.Table
	ValidationTables             []schema.Table
	RebuildCurrentSnapshot       schema.SchemaSnapshot
	RebuildCurrentTables         []schema.Table
	RebuildRequiresTargetCatalog bool
}

// Stage4TargetShapeSeed is explicit, canonical target-catalog evidence used
// only when a route has no successful durable target-shape authority yet. An
// empty seed is still explicit: callers may construct one only after their
// destructive planning and preflight semantics prove that an empty target
// prior is correct. Upsert callers must seed the exact read-only target
// catalog, including target-only objects that must be retained.
type Stage4TargetShapeSeed struct {
	snapshot      schema.SchemaSnapshot
	reservations  []TargetSchemaEvolutionNameReservation
	catalogDigest string
	present       bool
}

// NewStage4TargetShapeSeed freezes an exact read-only target catalog for
// first-authority admission. It performs no target I/O and never aliases the
// caller's metadata.
func NewStage4TargetShapeSeed(
	catalog TargetSchemaEvolutionCatalog,
) (Stage4TargetShapeSeed, error) {
	snapshot, err := schema.NewSchemaSnapshot(
		catalog.Tables(),
	)
	if err != nil {
		return Stage4TargetShapeSeed{}, fmt.Errorf(
			"build explicit Stage 4 target-shape seed: %w",
			err,
		)
	}
	reservations := canonicalTargetSchemaEvolutionReservations(
		catalog.Reservations(),
	)
	catalogDigest, err := stage4TargetShapeCatalogDigest(
		snapshot,
		reservations,
	)
	if err != nil {
		return Stage4TargetShapeSeed{}, fmt.Errorf(
			"digest explicit Stage 4 target-shape seed catalog: %w",
			err,
		)
	}
	return Stage4TargetShapeSeed{
		snapshot:      snapshot,
		reservations:  reservations,
		catalogDigest: catalogDigest,
		present:       true,
	}, nil
}

// Stage4TargetShapeAuthority is immutable, topology-bound evidence used to
// reconstruct a target projection independently from the successful filtered
// source baseline. Its fields are deliberately private; accessors and binders
// return canonical deep copies.
type Stage4TargetShapeAuthority struct {
	runID               string
	task                state.TaskKey
	topologyHash        string
	sourceEngine        string
	targetEngine        string
	targetMode          string
	sourcePriorDigest   string
	sourceCurrentDigest string
	sourceSuccessDigest string
	decisionDigest      string
	priorEvidenceDigest string
	priorCatalogDigest  string
	priorSnapshot       schema.SchemaSnapshot
	priorReservations   []TargetSchemaEvolutionNameReservation
	staged              state.SchemaSnapshot
	stagedFound         bool
	capturedAt          time.Time
}

func (authority Stage4TargetShapeAuthority) Task() state.TaskKey {
	return authority.task
}

func (authority Stage4TargetShapeAuthority) TopologyHash() string {
	return authority.topologyHash
}

func (authority Stage4TargetShapeAuthority) PriorDigest() string {
	return authority.priorEvidenceDigest
}

func (authority Stage4TargetShapeAuthority) PriorSnapshot() schema.SchemaSnapshot {
	return cloneSchemaSnapshot(authority.priorSnapshot)
}

func (authority Stage4TargetShapeAuthority) PriorReservations() []TargetSchemaEvolutionNameReservation {
	return cloneTargetSchemaEvolutionReservations(
		authority.priorReservations,
	)
}

func (authority Stage4TargetShapeAuthority) PriorCatalogDigest() string {
	return authority.priorCatalogDigest
}

// stage4TargetShapeEvidence is the versioned canonical payload stored under
// the distinct target-shape task. It binds the target shape to the exact
// source decision endpoint and topology that produced it, so a same-run resume
// cannot reinterpret an equal-looking target shape under different evidence.
type stage4TargetShapeEvidence struct {
	Version                int                                    `json:"version"`
	TopologyHash           string                                 `json:"topology_hash"`
	SourceEngine           string                                 `json:"source_engine"`
	TargetEngine           string                                 `json:"target_engine"`
	TargetMode             string                                 `json:"target_mode"`
	SourcePriorDigest      string                                 `json:"source_prior_digest"`
	SourceCurrentDigest    string                                 `json:"source_current_digest"`
	SourceSuccessDigest    string                                 `json:"source_success_digest"`
	DecisionDigest         string                                 `json:"decision_digest"`
	PriorTargetShapeDigest string                                 `json:"prior_target_shape_digest"`
	PriorTargetShape       schema.SchemaSnapshot                  `json:"prior_target_shape"`
	PriorReservations      []TargetSchemaEvolutionNameReservation `json:"prior_reservations"`
	PriorCatalogDigest     string                                 `json:"prior_catalog_digest"`
	TargetShapeDigest      string                                 `json:"target_shape_digest"`
	TargetShape            schema.SchemaSnapshot                  `json:"target_shape"`
	TargetReservations     []TargetSchemaEvolutionNameReservation `json:"target_reservations"`
	TargetCatalogDigest    string                                 `json:"target_catalog_digest"`
}

// PrepareStage4SchemaGate establishes the aggregate structured work sentinel,
// verifies same-run staged evidence, selects the latest successful applicable
// baseline, evaluates schema policy, and reconstructs executable rich metadata
// projections. It deliberately does not save or complete PendingSnapshot and
// is not yet wired into the production adapter runner: target schema seams and
// atomic run finalization must be composed first.
func PrepareStage4SchemaGate(
	run Stage4RunContext,
	tables []schema.Table,
	options Stage4SchemaGateOptions,
) (Stage4SchemaGateResult, error) {
	result := Stage4SchemaGateResult{Task: stage4SchemaGateTask}
	if err := run.Validate(); err != nil {
		return result, err
	}
	topologyHash, normalizedMode, err := stage4SchemaGateTopology(options)
	if err != nil {
		return result, err
	}
	result.TopologyHash = topologyHash

	startedAt := options.CapturedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	} else {
		startedAt = startedAt.UTC()
	}
	task := state.WorkTask{
		RunID:        run.RunID,
		Key:          stage4SchemaGateTask,
		Strategy:     stage4SchemaGateStrategy,
		TopologyHash: topologyHash,
		StartedAt:    startedAt,
	}
	ranges := []state.RangeState{{
		ID:           stage4SchemaGateRangeID,
		Strategy:     stage4SchemaGateStrategy,
		TopologyHash: topologyHash,
	}}
	if _, err := run.Backend.EnsureWorkPlan(task, ranges); err != nil {
		return result, fmt.Errorf(
			"establish aggregate schema gate before reading schema evidence: %w",
			err,
		)
	}

	current, err := schema.NewSchemaSnapshot(tables)
	if err != nil {
		return result, fmt.Errorf("build current aggregate schema snapshot: %w", err)
	}
	result.CurrentSnapshot = current

	staged, stagedFound, err := run.Backend.LoadSchemaSnapshot(
		run.RunID,
		stage4SchemaGateTask,
	)
	if err != nil {
		return result, fmt.Errorf("load same-run aggregate schema snapshot: %w", err)
	}
	var stagedSchema schema.SchemaSnapshot
	if stagedFound {
		stagedSchema, err = parseStage4SchemaEvidence(
			staged,
			stage4SchemaGateTask,
			run.RunID,
		)
		if err != nil {
			return result, fmt.Errorf("verify same-run aggregate schema snapshot: %w", err)
		}
	}

	previousRecord, previousFound, err :=
		run.Backend.LoadLatestApplicableSchemaSnapshot(
			run.RunID,
			stage4SchemaGateTask,
		)
	if err != nil {
		return result, fmt.Errorf("load latest successful aggregate schema snapshot: %w", err)
	}
	previous := current
	result.Baseline = !previousFound
	if previousFound {
		if previousRecord.RunID == run.RunID {
			return result, fmt.Errorf(
				"latest successful schema baseline unexpectedly belongs to active run %q",
				run.RunID,
			)
		}
		previous, err = parseStage4SchemaEvidence(
			previousRecord,
			stage4SchemaGateTask,
			"",
		)
		if err != nil {
			return result, fmt.Errorf("verify latest successful aggregate schema snapshot: %w", err)
		}
	} else if normalizedMode == "upsert" &&
		options.Contract != nil &&
		options.Contract.Tables == config.SchemaContractEvolve {
		// An explicit tables:evolve policy is the authority to create tables
		// that are absent from a first-run upsert target. Compare the first
		// successful source discovery with an empty durable prior so every
		// source table receives an auditable create_table decision. All other
		// policies retain the historical current=current baseline and therefore
		// cannot silently authorize target mutation.
		previous, err = schema.NewSchemaSnapshot(nil)
		if err != nil {
			return result, fmt.Errorf(
				"build empty first-run upsert schema baseline: %w",
				err,
			)
		}
	}
	result.PreviousSnapshot = previous

	plan, planErr := BuildSchemaContractPlan(
		previous,
		current,
		SchemaContractOptions{
			Contract:           options.Contract,
			FailOnSchemaDrift:  options.FailOnSchemaDrift,
			TargetMode:         normalizedMode,
			DateUpdatedColumns: append([]string(nil), options.DateUpdatedColumns...),
		},
	)
	result.Plan = plan
	if plan.SuccessfulSnapshot.Version == 0 {
		if planErr != nil {
			return result, planErr
		}
		return result, fmt.Errorf(
			"schema contract planner returned no successful projection",
		)
	}

	successfulJSON, err := plan.SuccessfulSnapshot.CanonicalJSON()
	if err != nil {
		return result, fmt.Errorf("encode successful aggregate schema projection: %w", err)
	}
	successfulDigest, err := plan.SuccessfulSnapshot.Digest()
	if err != nil {
		return result, fmt.Errorf("digest successful aggregate schema projection: %w", err)
	}
	result.PendingSnapshot = state.SchemaSnapshot{
		RunID:         run.RunID,
		Task:          stage4SchemaGateTask,
		CanonicalJSON: string(successfulJSON),
		Digest:        successfulDigest,
		CapturedAt:    startedAt,
	}
	if stagedFound {
		equal, compareErr := schema.SchemaSnapshotsEqual(
			stagedSchema,
			plan.SuccessfulSnapshot,
		)
		if compareErr != nil {
			return result, fmt.Errorf(
				"compare same-run successful schema projection: %w",
				compareErr,
			)
		}
		if !equal {
			return result, fmt.Errorf(
				"same-run successful schema projection differs from current policy result; " +
					"resume cannot reinterpret already-staged schema evidence",
			)
		}
		result.PendingSnapshot = staged
	}
	if planErr != nil {
		return result, planErr
	}

	projections := []struct {
		name     string
		snapshot schema.SchemaSnapshot
		assign   func([]schema.Table)
	}{
		{
			name: "transfer", snapshot: plan.TransferSnapshot,
			assign: func(value []schema.Table) { result.TransferTables = value },
		},
		{
			name: "validation", snapshot: plan.ValidationSnapshot,
			assign: func(value []schema.Table) { result.ValidationTables = value },
		},
	}
	for _, projection := range projections {
		projected, err := projectStage4RichTables(tables, current, projection.snapshot)
		if err != nil {
			return result, fmt.Errorf(
				"prove %s schema-contract projection from current rich metadata: %w",
				projection.name,
				err,
			)
		}
		projection.assign(projected)
	}

	// RebuildSnapshot may retain prior-only dropped tables, columns, and
	// dependent objects. Current source metadata proves only this current
	// executable portion. A later target-catalog seam must merge and verify the
	// full Plan.RebuildSnapshot before mutation whenever the flag is true.
	result.RebuildCurrentSnapshot = plan.TransferSnapshot
	result.RebuildCurrentTables, err = projectStage4RichTables(
		tables,
		current,
		result.RebuildCurrentSnapshot,
	)
	if err != nil {
		return result, fmt.Errorf(
			"prove current-backed rebuild projection from rich metadata: %w",
			err,
		)
	}
	rebuildEqual, err := schema.SchemaSnapshotsEqual(
		result.RebuildCurrentSnapshot,
		plan.RebuildSnapshot,
	)
	if err != nil {
		return result, fmt.Errorf(
			"compare current-backed and required rebuild projections: %w",
			err,
		)
	}
	result.RebuildRequiresTargetCatalog = !rebuildEqual
	return result, nil
}

// PrepareStage4TargetShapeAuthority establishes and reads the distinct
// target-shape sentinel. It never derives target shape from the successful
// source baseline. On the first authority run, seed must contain an explicit
// exact read-only target catalog. Once target authority exists, its latest
// successful shape is the only prior accepted for subsequent projections.
func PrepareStage4TargetShapeAuthority(
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	options Stage4SchemaGateOptions,
	seed Stage4TargetShapeSeed,
) (Stage4TargetShapeAuthority, error) {
	var result Stage4TargetShapeAuthority
	if err := run.Validate(); err != nil {
		return result, err
	}
	topologyHash, targetMode, err := stage4SchemaGateTopology(options)
	if err != nil {
		return result, err
	}
	if gate.Task != stage4SchemaGateTask ||
		gate.TopologyHash == "" ||
		gate.TopologyHash != topologyHash {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: source gate topology or task is inconsistent",
		)
	}
	if gate.PendingSnapshot.RunID != run.RunID ||
		gate.PendingSnapshot.Task != stage4SchemaGateTask {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: source-success evidence is not bound to active run %q",
			run.RunID,
		)
	}
	if _, err := parseStage4SchemaEvidence(
		gate.PendingSnapshot,
		stage4SchemaGateTask,
		run.RunID,
	); err != nil {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: verify pending source-success evidence: %w",
			err,
		)
	}

	sourcePrior, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"target-authority source prior",
		gate.PreviousSnapshot,
	)
	if err != nil {
		return result, err
	}
	sourceCurrent, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"target-authority source current",
		gate.CurrentSnapshot,
	)
	if err != nil {
		return result, err
	}
	sourceSuccess, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"target-authority source success",
		gate.Plan.SuccessfulSnapshot,
	)
	if err != nil {
		return result, err
	}
	sourcePriorDigest, err := sourcePrior.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: digest source prior: %w",
			err,
		)
	}
	sourceCurrentDigest, err := sourceCurrent.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: digest source current: %w",
			err,
		)
	}
	sourceSuccessDigest, err := sourceSuccess.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: digest source success: %w",
			err,
		)
	}
	decisionDigest, err := stage4TargetShapeDecisionDigest(gate.Plan.Decisions)
	if err != nil {
		return result, fmt.Errorf(
			"prepare Stage 4 target-shape authority: %w",
			err,
		)
	}

	startedAt := gate.PendingSnapshot.CapturedAt.UTC()
	task := state.WorkTask{
		RunID:        run.RunID,
		Key:          stage4TargetShapeTask,
		Strategy:     stage4TargetShapeStrategy,
		TopologyHash: topologyHash,
		StartedAt:    startedAt,
	}
	ranges := []state.RangeState{{
		ID:           stage4TargetShapeRangeID,
		Strategy:     stage4TargetShapeStrategy,
		TopologyHash: topologyHash,
	}}
	if _, err := run.Backend.EnsureWorkPlan(task, ranges); err != nil {
		return result, fmt.Errorf(
			"establish target-shape authority before reading target evidence: %w",
			err,
		)
	}

	stagedRecord, stagedFound, err := run.Backend.LoadSchemaSnapshot(
		run.RunID,
		stage4TargetShapeTask,
	)
	if err != nil {
		return result, fmt.Errorf(
			"load same-run target-shape evidence: %w",
			err,
		)
	}
	var stagedEvidence stage4TargetShapeEvidence
	if stagedFound {
		stagedEvidence, err = parseStage4TargetShapeEvidence(
			stagedRecord,
			run.RunID,
		)
		if err != nil {
			return result, fmt.Errorf(
				"verify same-run target-shape evidence: %w",
				err,
			)
		}
		if err := validateStage4TargetShapeEvidenceBinding(
			stagedEvidence,
			topologyHash,
			options.SourceEngine,
			options.TargetEngine,
			targetMode,
			sourcePriorDigest,
			sourceCurrentDigest,
			sourceSuccessDigest,
			decisionDigest,
		); err != nil {
			return result, fmt.Errorf(
				"same-run target-shape evidence cannot be reinterpreted: %w",
				err,
			)
		}
	}

	previousRecord, previousFound, err :=
		run.Backend.LoadLatestApplicableSchemaSnapshot(
			run.RunID,
			stage4TargetShapeTask,
		)
	if err != nil {
		return result, fmt.Errorf(
			"load latest successful target-shape evidence: %w",
			err,
		)
	}
	var previousEvidence stage4TargetShapeEvidence
	previousSameTopology := false
	if previousFound {
		if previousRecord.RunID == run.RunID {
			return result, fmt.Errorf(
				"latest successful target-shape authority unexpectedly belongs to active run %q",
				run.RunID,
			)
		}
		previousEvidence, err = parseStage4TargetShapeEvidence(
			previousRecord,
			"",
		)
		if err != nil {
			return result, fmt.Errorf(
				"verify latest successful target-shape evidence: %w",
				err,
			)
		}
		previousSameTopology =
			previousEvidence.TopologyHash == topologyHash &&
				previousEvidence.SourceEngine == options.SourceEngine &&
				previousEvidence.TargetEngine == options.TargetEngine &&
				previousEvidence.TargetMode == targetMode
		if previousSameTopology &&
			previousEvidence.SourceSuccessDigest != sourcePriorDigest {
			return result, fmt.Errorf(
				"latest successful source baseline and target-shape authority do not describe the same completed source projection",
			)
		}
	}

	var prior schema.SchemaSnapshot
	var priorReservations []TargetSchemaEvolutionNameReservation
	switch {
	case stagedFound:
		prior = stagedEvidence.PriorTargetShape
		priorReservations = cloneTargetSchemaEvolutionReservations(
			stagedEvidence.PriorReservations,
		)
		if previousSameTopology {
			equal, compareErr := stage4TargetShapeCatalogEqual(
				prior,
				priorReservations,
				previousEvidence.TargetShape,
				previousEvidence.TargetReservations,
			)
			if compareErr != nil {
				return result, fmt.Errorf(
					"compare same-run and successful target priors: %w",
					compareErr,
				)
			}
			if !equal {
				return result, fmt.Errorf(
					"same-run target-shape evidence replaced the latest successful target prior",
				)
			}
		}
		if seed.present {
			equal, compareErr := stage4TargetShapeCatalogEqual(
				prior,
				priorReservations,
				seed.snapshot,
				seed.reservations,
			)
			if compareErr != nil {
				return result, fmt.Errorf(
					"compare explicit target seed with staged prior: %w",
					compareErr,
				)
			}
			if !equal {
				return result, fmt.Errorf(
					"explicit target seed differs from the immutable same-run target prior",
				)
			}
		}
	case previousSameTopology:
		if gate.Baseline {
			return result, fmt.Errorf(
				"target-shape history exists but the source gate unexpectedly selected a baseline",
			)
		}
		if seed.present {
			return result, fmt.Errorf(
				"explicit target seed cannot replace same-topology successful target-shape authority",
			)
		}
		prior = previousEvidence.TargetShape
		priorReservations = cloneTargetSchemaEvolutionReservations(
			previousEvidence.TargetReservations,
		)
	default:
		if !seed.present {
			reason := "first target-shape authority"
			if previousFound {
				reason = "new target-shape topology epoch"
			}
			return result, fmt.Errorf(
				"%w: %s requires an explicit exact read-only target catalog seed",
				ErrStage4TargetShapeSeedRequired,
				reason,
			)
		}
		prior = seed.snapshot
		priorReservations = cloneTargetSchemaEvolutionReservations(
			seed.reservations,
		)
	}
	prior, err = canonicalStage4TargetSchemaProjectionSnapshot(
		"durable target prior",
		prior,
	)
	if err != nil {
		return result, err
	}
	priorDigest, err := prior.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"digest durable target-shape prior: %w",
			err,
		)
	}
	priorCatalogDigest, err := stage4TargetShapeCatalogDigest(
		prior,
		priorReservations,
	)
	if err != nil {
		return result, fmt.Errorf(
			"digest durable target-shape catalog authority: %w",
			err,
		)
	}
	if stagedFound &&
		priorCatalogDigest != stagedEvidence.PriorCatalogDigest {
		return result, fmt.Errorf(
			"same-run target-shape catalog digest changed",
		)
	}
	if !stagedFound && !previousSameTopology && seed.present &&
		priorCatalogDigest != seed.catalogDigest {
		return result, fmt.Errorf(
			"explicit target-shape seed catalog digest changed",
		)
	}

	result = Stage4TargetShapeAuthority{
		runID:               run.RunID,
		task:                stage4TargetShapeTask,
		topologyHash:        topologyHash,
		sourceEngine:        options.SourceEngine,
		targetEngine:        options.TargetEngine,
		targetMode:          targetMode,
		sourcePriorDigest:   sourcePriorDigest,
		sourceCurrentDigest: sourceCurrentDigest,
		sourceSuccessDigest: sourceSuccessDigest,
		decisionDigest:      decisionDigest,
		priorEvidenceDigest: priorDigest,
		priorCatalogDigest:  priorCatalogDigest,
		priorSnapshot:       cloneSchemaSnapshot(prior),
		priorReservations: cloneTargetSchemaEvolutionReservations(
			priorReservations,
		),
		staged:      stagedRecord,
		stagedFound: stagedFound,
		capturedAt:  startedAt,
	}
	if stagedFound {
		result.capturedAt = stagedRecord.CapturedAt.UTC()
	}
	return result, nil
}

// BindStage4TargetShapeProjection freezes the exact target projection and its
// source-decision authority into a separate immutable pending snapshot. It
// deliberately does not save or complete that snapshot; production lifecycle
// composition must stage it before target DDL and publish success only after
// validation and final run completion.
func BindStage4TargetShapeProjection(
	authority Stage4TargetShapeAuthority,
	projection Stage4TargetSchemaEvolutionProjection,
) (state.SchemaSnapshot, error) {
	if authority.task != stage4TargetShapeTask ||
		authority.runID == "" ||
		authority.topologyHash == "" ||
		authority.priorEvidenceDigest == "" ||
		authority.priorCatalogDigest == "" {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: complete prepared authority is required",
		)
	}
	if projection.targetAuthorityTopologyHash != authority.topologyHash ||
		projection.targetAuthorityPriorDigest != authority.priorEvidenceDigest ||
		projection.targetAuthorityCatalogDigest != authority.priorCatalogDigest ||
		projection.SourceEngine() != authority.sourceEngine ||
		projection.TargetEngine() != authority.targetEngine ||
		projection.TargetMode() != authority.targetMode ||
		projection.SourcePriorDigest() != authority.sourcePriorDigest ||
		projection.SourceCurrentDigest() != authority.sourceCurrentDigest {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: projection authority differs from prepared durable evidence",
		)
	}
	if got, err := stage4TargetShapeDecisionDigest(projection.Decisions()); err != nil {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: %w",
			err,
		)
	} else if got != authority.decisionDigest {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: projection decision evidence changed",
		)
	}
	current, err := schema.NewSchemaSnapshot(projection.CurrentTables())
	if err != nil {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: snapshot current target projection: %w",
			err,
		)
	}
	currentDigest, err := current.Digest()
	if err != nil {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: digest current target projection: %w",
			err,
		)
	}
	if currentDigest != projection.CurrentDigest() {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: projection current digest changed",
		)
	}
	currentCatalogDigest, err := stage4TargetShapeCatalogDigest(
		current,
		authority.priorReservations,
	)
	if err != nil {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: digest current target catalog: %w",
			err,
		)
	}
	evidence := stage4TargetShapeEvidence{
		Version:                stage4TargetShapeEvidenceV1,
		TopologyHash:           authority.topologyHash,
		SourceEngine:           authority.sourceEngine,
		TargetEngine:           authority.targetEngine,
		TargetMode:             authority.targetMode,
		SourcePriorDigest:      authority.sourcePriorDigest,
		SourceCurrentDigest:    authority.sourceCurrentDigest,
		SourceSuccessDigest:    authority.sourceSuccessDigest,
		DecisionDigest:         authority.decisionDigest,
		PriorTargetShapeDigest: authority.priorEvidenceDigest,
		PriorTargetShape:       cloneSchemaSnapshot(authority.priorSnapshot),
		PriorReservations: cloneTargetSchemaEvolutionReservations(
			authority.priorReservations,
		),
		PriorCatalogDigest: authority.priorCatalogDigest,
		TargetShapeDigest:  currentDigest,
		TargetShape:        cloneSchemaSnapshot(current),
		TargetReservations: cloneTargetSchemaEvolutionReservations(
			authority.priorReservations,
		),
		TargetCatalogDigest: currentCatalogDigest,
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return state.SchemaSnapshot{}, fmt.Errorf(
			"bind Stage 4 target shape: encode canonical evidence: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	record := state.SchemaSnapshot{
		RunID:         authority.runID,
		Task:          stage4TargetShapeTask,
		CanonicalJSON: string(encoded),
		Digest:        hex.EncodeToString(digest[:]),
		CapturedAt:    authority.capturedAt,
	}
	if authority.stagedFound {
		if !reflect.DeepEqual(record, authority.staged) {
			return state.SchemaSnapshot{}, fmt.Errorf(
				"same-run target-shape projection differs from immutable staged evidence",
			)
		}
		return authority.staged, nil
	}
	return record, nil
}

func stage4TargetShapeDecisionDigest(
	decisions []SchemaContractDecision,
) (string, error) {
	encoded, err := json.Marshal(
		cloneStage4TargetSchemaProjectionDecisions(decisions),
	)
	if err != nil {
		return "", fmt.Errorf("encode schema decisions: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func parseStage4TargetShapeEvidence(
	record state.SchemaSnapshot,
	requiredRunID string,
) (stage4TargetShapeEvidence, error) {
	var result stage4TargetShapeEvidence
	if record.Task != stage4TargetShapeTask {
		return result, fmt.Errorf(
			"target-shape evidence task is %#v, want %#v",
			record.Task,
			stage4TargetShapeTask,
		)
	}
	if strings.TrimSpace(record.RunID) == "" ||
		requiredRunID != "" && record.RunID != requiredRunID {
		return result, fmt.Errorf(
			"target-shape evidence run is %q, want %q",
			record.RunID,
			requiredRunID,
		)
	}
	if record.CapturedAt.IsZero() {
		return result, fmt.Errorf(
			"target-shape evidence capture time is required",
		)
	}
	if err := json.Unmarshal([]byte(record.CanonicalJSON), &result); err != nil {
		return result, fmt.Errorf("parse target-shape evidence: %w", err)
	}
	if result.Version != stage4TargetShapeEvidenceV1 {
		return result, fmt.Errorf(
			"target-shape evidence version is %d, want %d",
			result.Version,
			stage4TargetShapeEvidenceV1,
		)
	}
	if strings.TrimSpace(result.TopologyHash) == "" ||
		result.TopologyHash != strings.TrimSpace(result.TopologyHash) ||
		strings.TrimSpace(result.SourceEngine) == "" ||
		result.SourceEngine != strings.TrimSpace(result.SourceEngine) ||
		strings.TrimSpace(result.TargetEngine) == "" ||
		result.TargetEngine != strings.TrimSpace(result.TargetEngine) ||
		(result.TargetMode != "upsert" &&
			result.TargetMode != "drop_recreate") {
		return result, fmt.Errorf(
			"target-shape evidence route, mode, or topology is not canonical",
		)
	}
	for label, digest := range map[string]string{
		"source prior":         result.SourcePriorDigest,
		"source current":       result.SourceCurrentDigest,
		"source success":       result.SourceSuccessDigest,
		"decisions":            result.DecisionDigest,
		"prior target shape":   result.PriorTargetShapeDigest,
		"prior target catalog": result.PriorCatalogDigest,
		"target shape":         result.TargetShapeDigest,
		"target catalog":       result.TargetCatalogDigest,
	} {
		decoded, decodeErr := hex.DecodeString(digest)
		if decodeErr != nil || len(decoded) != sha256.Size ||
			digest != strings.ToLower(digest) {
			return result, fmt.Errorf(
				"target-shape evidence %s digest is not a canonical SHA-256 digest",
				label,
			)
		}
	}
	prior, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"target-shape evidence prior",
		result.PriorTargetShape,
	)
	if err != nil {
		return result, err
	}
	current, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"target-shape evidence current",
		result.TargetShape,
	)
	if err != nil {
		return result, err
	}
	priorDigest, err := prior.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"digest target-shape evidence prior: %w",
			err,
		)
	}
	currentDigest, err := current.Digest()
	if err != nil {
		return result, fmt.Errorf(
			"digest target-shape evidence current: %w",
			err,
		)
	}
	if result.PriorTargetShapeDigest != priorDigest ||
		result.TargetShapeDigest != currentDigest {
		return result, fmt.Errorf(
			"target-shape evidence digest does not match canonical target payload",
		)
	}
	priorReservations := canonicalTargetSchemaEvolutionReservations(
		result.PriorReservations,
	)
	targetReservations := canonicalTargetSchemaEvolutionReservations(
		result.TargetReservations,
	)
	if !reflect.DeepEqual(priorReservations, result.PriorReservations) ||
		!reflect.DeepEqual(targetReservations, result.TargetReservations) {
		return result, fmt.Errorf(
			"target-shape reservation evidence is not canonical",
		)
	}
	priorTables, err := schema.MaterializeSchemaSnapshot(prior)
	if err != nil {
		return result, fmt.Errorf(
			"materialize target-shape evidence prior catalog: %w",
			err,
		)
	}
	if _, err := NewTargetSchemaEvolutionCatalog(
		priorTables,
		priorReservations,
	); err != nil {
		return result, fmt.Errorf(
			"validate target-shape evidence prior catalog: %w",
			err,
		)
	}
	targetTables, err := schema.MaterializeSchemaSnapshot(current)
	if err != nil {
		return result, fmt.Errorf(
			"materialize target-shape evidence current catalog: %w",
			err,
		)
	}
	if _, err := NewTargetSchemaEvolutionCatalog(
		targetTables,
		targetReservations,
	); err != nil {
		return result, fmt.Errorf(
			"validate target-shape evidence current catalog: %w",
			err,
		)
	}
	priorCatalogDigest, err := stage4TargetShapeCatalogDigest(
		prior,
		priorReservations,
	)
	if err != nil {
		return result, fmt.Errorf(
			"digest target-shape evidence prior catalog: %w",
			err,
		)
	}
	targetCatalogDigest, err := stage4TargetShapeCatalogDigest(
		current,
		targetReservations,
	)
	if err != nil {
		return result, fmt.Errorf(
			"digest target-shape evidence current catalog: %w",
			err,
		)
	}
	if result.PriorCatalogDigest != priorCatalogDigest ||
		result.TargetCatalogDigest != targetCatalogDigest {
		return result, fmt.Errorf(
			"target-shape catalog digest does not match tables and reservations",
		)
	}
	result.PriorTargetShape = prior
	result.TargetShape = current
	result.PriorReservations = priorReservations
	result.TargetReservations = targetReservations
	canonical, err := json.Marshal(result)
	if err != nil {
		return result, fmt.Errorf(
			"re-encode canonical target-shape evidence: %w",
			err,
		)
	}
	if record.CanonicalJSON != string(canonical) {
		return result, fmt.Errorf(
			"target-shape evidence payload is not canonical",
		)
	}
	recordDigest := sha256.Sum256(canonical)
	if record.Digest != hex.EncodeToString(recordDigest[:]) {
		return result, fmt.Errorf(
			"target-shape evidence record digest does not match canonical payload",
		)
	}
	return result, nil
}

func validateStage4TargetShapeEvidenceBinding(
	evidence stage4TargetShapeEvidence,
	topologyHash string,
	sourceEngine string,
	targetEngine string,
	targetMode string,
	sourcePriorDigest string,
	sourceCurrentDigest string,
	sourceSuccessDigest string,
	decisionDigest string,
) error {
	if evidence.TopologyHash != topologyHash ||
		evidence.SourceEngine != sourceEngine ||
		evidence.TargetEngine != targetEngine ||
		evidence.TargetMode != targetMode ||
		evidence.SourcePriorDigest != sourcePriorDigest ||
		evidence.SourceCurrentDigest != sourceCurrentDigest ||
		evidence.SourceSuccessDigest != sourceSuccessDigest ||
		evidence.DecisionDigest != decisionDigest {
		return fmt.Errorf(
			"topology, source endpoints, or exact decision digest differs",
		)
	}
	return nil
}

func stage4TargetShapeCatalogDigest(
	snapshot schema.SchemaSnapshot,
	reservations []TargetSchemaEvolutionNameReservation,
) (string, error) {
	normalized, err := canonicalStage4TargetSchemaProjectionSnapshot(
		"target-shape catalog",
		snapshot,
	)
	if err != nil {
		return "", err
	}
	wire := struct {
		Shape        schema.SchemaSnapshot                  `json:"shape"`
		Reservations []TargetSchemaEvolutionNameReservation `json:"reservations"`
	}{
		Shape: normalized,
		Reservations: canonicalTargetSchemaEvolutionReservations(
			reservations,
		),
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode target-shape catalog: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func stage4TargetShapeCatalogEqual(
	first schema.SchemaSnapshot,
	firstReservations []TargetSchemaEvolutionNameReservation,
	second schema.SchemaSnapshot,
	secondReservations []TargetSchemaEvolutionNameReservation,
) (bool, error) {
	firstDigest, err := stage4TargetShapeCatalogDigest(
		first,
		firstReservations,
	)
	if err != nil {
		return false, err
	}
	secondDigest, err := stage4TargetShapeCatalogDigest(
		second,
		secondReservations,
	)
	if err != nil {
		return false, err
	}
	return firstDigest == secondDigest, nil
}

func stage4SchemaGateTopology(
	options Stage4SchemaGateOptions,
) (string, string, error) {
	source := strings.TrimSpace(options.SourceEngine)
	target := strings.TrimSpace(options.TargetEngine)
	if source == "" || source != options.SourceEngine ||
		target == "" || target != options.TargetEngine {
		return "", "", fmt.Errorf(
			"aggregate schema gate requires canonical source and target route engines",
		)
	}
	mode := options.TargetMode
	if mode == "" {
		mode = "drop_recreate"
	}
	if mode != "drop_recreate" && mode != "upsert" {
		return "", "", fmt.Errorf("unsupported aggregate schema gate target mode %q", mode)
	}
	configIdentity := strings.TrimSpace(options.ConfigIdentity)
	if configIdentity == "" || configIdentity != options.ConfigIdentity {
		return "", "", fmt.Errorf("aggregate schema gate configuration identity is required")
	}
	include := append([]string(nil), options.IncludeTables...)
	exclude := append([]string(nil), options.ExcludeTables...)
	sort.Strings(include)
	sort.Strings(exclude)
	dateUpdatedColumns := append([]string(nil), options.DateUpdatedColumns...)
	var contract *config.SchemaContract
	if options.Contract != nil {
		copied := *options.Contract
		contract = &copied
	}
	wire := struct {
		Version            int                    `json:"version"`
		SourceEngine       string                 `json:"source_engine"`
		TargetEngine       string                 `json:"target_engine"`
		TargetMode         string                 `json:"target_mode"`
		IncludeTables      []string               `json:"include_tables"`
		ExcludeTables      []string               `json:"exclude_tables"`
		ConfigIdentity     string                 `json:"config_identity"`
		Contract           *config.SchemaContract `json:"schema_contract"`
		FailOnSchemaDrift  bool                   `json:"fail_on_schema_drift"`
		DateUpdatedColumns []string               `json:"date_updated_columns"`
	}{
		Version:            1,
		SourceEngine:       source,
		TargetEngine:       target,
		TargetMode:         mode,
		IncludeTables:      include,
		ExcludeTables:      exclude,
		ConfigIdentity:     configIdentity,
		Contract:           contract,
		FailOnSchemaDrift:  options.FailOnSchemaDrift,
		DateUpdatedColumns: dateUpdatedColumns,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", "", fmt.Errorf("encode aggregate schema gate topology: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), mode, nil
}

func parseStage4SchemaEvidence(
	record state.SchemaSnapshot,
	task state.TaskKey,
	requiredRunID string,
) (schema.SchemaSnapshot, error) {
	if record.Task != task {
		return schema.SchemaSnapshot{}, fmt.Errorf(
			"schema evidence task is %#v, want %#v",
			record.Task,
			task,
		)
	}
	if strings.TrimSpace(record.RunID) == "" ||
		requiredRunID != "" && record.RunID != requiredRunID {
		return schema.SchemaSnapshot{}, fmt.Errorf(
			"schema evidence run is %q, want %q",
			record.RunID,
			requiredRunID,
		)
	}
	if record.CapturedAt.IsZero() {
		return schema.SchemaSnapshot{}, fmt.Errorf("schema evidence capture time is required")
	}
	parsed, err := schema.ParseSchemaSnapshot([]byte(record.CanonicalJSON))
	if err != nil {
		return schema.SchemaSnapshot{}, fmt.Errorf("parse canonical schema evidence: %w", err)
	}
	digest, err := parsed.Digest()
	if err != nil {
		return schema.SchemaSnapshot{}, fmt.Errorf("digest canonical schema evidence: %w", err)
	}
	if record.Digest != digest {
		return schema.SchemaSnapshot{}, fmt.Errorf(
			"schema evidence digest %q does not match canonical payload",
			record.Digest,
		)
	}
	return parsed, nil
}

type stage4RichTableKey struct {
	schema string
	table  string
}

func projectStage4RichTables(
	rich []schema.Table,
	current schema.SchemaSnapshot,
	projection schema.SchemaSnapshot,
) ([]schema.Table, error) {
	currentJSON, err := current.CanonicalJSON()
	if err != nil {
		return nil, fmt.Errorf("normalize current schema: %w", err)
	}
	current, err = schema.ParseSchemaSnapshot(currentJSON)
	if err != nil {
		return nil, fmt.Errorf("parse normalized current schema: %w", err)
	}
	projectionJSON, err := projection.CanonicalJSON()
	if err != nil {
		return nil, fmt.Errorf("normalize projected schema: %w", err)
	}
	projection, err = schema.ParseSchemaSnapshot(projectionJSON)
	if err != nil {
		return nil, fmt.Errorf("parse normalized projected schema: %w", err)
	}
	if len(rich) != len(current.Tables) {
		return nil, fmt.Errorf("current rich and canonical table counts differ")
	}

	richByKey := make(map[stage4RichTableKey]schema.Table, len(rich))
	currentByKey := make(
		map[stage4RichTableKey]schema.SnapshotTable,
		len(current.Tables),
	)
	for _, table := range rich {
		key := stage4RichTableKey{schema: table.Schema, table: table.Name}
		if _, exists := richByKey[key]; exists {
			return nil, fmt.Errorf(
				"current rich metadata contains duplicate table (%q, %q)",
				key.schema,
				key.table,
			)
		}
		richByKey[key] = cloneStage4RichTable(table)
	}
	for _, table := range current.Tables {
		key := stage4RichTableKey{schema: table.Schema, table: table.Name}
		currentByKey[key] = table
	}

	result := make([]schema.Table, 0, len(projection.Tables))
	for _, requested := range projection.Tables {
		key := stage4RichTableKey{
			schema: requested.Schema,
			table:  requested.Name,
		}
		sourceRich, richFound := richByKey[key]
		sourceSnapshot, snapshotFound := currentByKey[key]
		if !richFound || !snapshotFound {
			return nil, fmt.Errorf(
				"projected table (%q, %q) has no current rich metadata",
				key.schema,
				key.table,
			)
		}
		projected, err := projectStage4RichTable(
			sourceRich,
			sourceSnapshot,
			requested,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"table (%q, %q): %w",
				key.schema,
				key.table,
				err,
			)
		}
		result = append(result, projected)
	}
	return result, nil
}

func projectStage4RichTable(
	rich schema.Table,
	current schema.SnapshotTable,
	requested schema.SnapshotTable,
) (schema.Table, error) {
	projected := cloneStage4RichTable(rich)
	projected.Columns = nil
	projected.Indexes = nil
	projected.ForeignKeys = nil
	projected.Checks = nil
	projected.Identity = nil

	columns := make(map[string]struct {
		rich     schema.Column
		snapshot schema.SnapshotColumn
	}, len(current.Columns))
	if len(rich.Columns) != len(current.Columns) {
		return schema.Table{}, fmt.Errorf("column metadata count differs")
	}
	for index, column := range current.Columns {
		if rich.Columns[index].Name != column.Name {
			return schema.Table{}, fmt.Errorf(
				"rich column %d is %q, canonical column is %q",
				index,
				rich.Columns[index].Name,
				column.Name,
			)
		}
		columns[column.Name] = struct {
			rich     schema.Column
			snapshot schema.SnapshotColumn
		}{
			rich:     cloneStage4RichColumn(rich.Columns[index]),
			snapshot: column,
		}
	}
	for _, column := range requested.Columns {
		candidate, found := columns[column.Name]
		if !found || !reflect.DeepEqual(candidate.snapshot, column) {
			return schema.Table{}, fmt.Errorf(
				"projected column %q is not exact current metadata",
				column.Name,
			)
		}
		projected.Columns = append(projected.Columns, candidate.rich)
	}

	switch {
	case requested.Identity == nil:
	case current.Identity == nil || rich.Identity == nil ||
		!reflect.DeepEqual(current.Identity, requested.Identity):
		return schema.Table{}, fmt.Errorf(
			"projected identity is not exact current metadata",
		)
	default:
		projected.Identity = cloneStage4RichIdentity(rich.Identity)
	}

	var err error
	projected.Indexes, err = selectStage4Indexes(rich.Indexes, requested.Indexes)
	if err != nil {
		return schema.Table{}, err
	}
	projected.ForeignKeys, err = selectStage4ForeignKeys(
		rich.ForeignKeys,
		requested.ForeignKeys,
	)
	if err != nil {
		return schema.Table{}, err
	}
	projected.Checks, err = selectStage4Checks(rich.Checks, requested.Checks)
	if err != nil {
		return schema.Table{}, err
	}

	candidateSnapshot, err := schema.NewSchemaSnapshot([]schema.Table{projected})
	if err != nil {
		return schema.Table{}, fmt.Errorf("snapshot projected rich metadata: %w", err)
	}
	requestedSnapshot := schema.SchemaSnapshot{
		Version: schema.SchemaSnapshotVersion,
		Tables:  []schema.SnapshotTable{requested},
	}
	equal, err := schema.SchemaSnapshotsEqual(candidateSnapshot, requestedSnapshot)
	if err != nil {
		return schema.Table{}, fmt.Errorf("verify projected rich metadata: %w", err)
	}
	if !equal {
		return schema.Table{}, fmt.Errorf(
			"projected canonical shape cannot be reconstructed exactly from current rich metadata",
		)
	}
	return projected, nil
}

func selectStage4Indexes(
	rich []schema.Index,
	requested []schema.SnapshotIndex,
) ([]schema.Index, error) {
	required := make(map[string]int, len(requested))
	for _, value := range requested {
		key, err := stage4CanonicalObjectKey(value)
		if err != nil {
			return nil, fmt.Errorf("encode projected index: %w", err)
		}
		required[key]++
	}
	result := make([]schema.Index, 0, len(requested))
	for _, value := range rich {
		snapshot := schema.SnapshotIndex{
			Name:    value.Name,
			Unique:  value.Unique,
			Inline:  value.Inline,
			Columns: make([]schema.SnapshotIndexColumn, len(value.Columns)),
		}
		for index, column := range value.Columns {
			snapshot.Columns[index] = schema.SnapshotIndexColumn{
				Name:       column.Name,
				Descending: column.Descending,
				Collation:  column.Collation,
			}
		}
		key, _ := stage4CanonicalObjectKey(snapshot)
		if required[key] == 0 {
			continue
		}
		required[key]--
		result = append(result, cloneStage4RichIndex(value))
	}
	if missingStage4Object(required) {
		return nil, fmt.Errorf("projected index is not exact current rich metadata")
	}
	return result, nil
}

func selectStage4ForeignKeys(
	rich []schema.ForeignKey,
	requested []schema.SnapshotForeignKey,
) ([]schema.ForeignKey, error) {
	required := make(map[string]int, len(requested))
	for _, value := range requested {
		key, err := stage4CanonicalObjectKey(value)
		if err != nil {
			return nil, fmt.Errorf("encode projected foreign key: %w", err)
		}
		required[key]++
	}
	result := make([]schema.ForeignKey, 0, len(requested))
	for _, value := range rich {
		snapshot := schema.SnapshotForeignKey{
			Name:              value.Name,
			Columns:           append([]string(nil), value.Columns...),
			ReferencedSchema:  value.ReferencedSchema,
			ReferencedTable:   value.ReferencedTable,
			ReferencedColumns: append([]string(nil), value.ReferencedColumns...),
			OnUpdate:          value.OnUpdate,
			OnDelete:          value.OnDelete,
			Match:             value.Match,
		}
		key, _ := stage4CanonicalObjectKey(snapshot)
		if required[key] == 0 {
			continue
		}
		required[key]--
		result = append(result, cloneStage4RichForeignKey(value))
	}
	if missingStage4Object(required) {
		return nil, fmt.Errorf("projected foreign key is not exact current rich metadata")
	}
	return result, nil
}

func selectStage4Checks(
	rich []schema.CheckConstraint,
	requested []schema.SnapshotCheckConstraint,
) ([]schema.CheckConstraint, error) {
	required := make(map[string]int, len(requested))
	for _, value := range requested {
		key, err := stage4CanonicalObjectKey(value)
		if err != nil {
			return nil, fmt.Errorf("encode projected check: %w", err)
		}
		required[key]++
	}
	result := make([]schema.CheckConstraint, 0, len(requested))
	for _, value := range rich {
		snapshot := schema.SnapshotCheckConstraint{
			Name:       value.Name,
			Expression: value.Expression.CanonicalSQL(),
		}
		key, _ := stage4CanonicalObjectKey(snapshot)
		if required[key] == 0 {
			continue
		}
		required[key]--
		result = append(result, value)
	}
	if missingStage4Object(required) {
		return nil, fmt.Errorf("projected check is not exact current rich metadata")
	}
	return result, nil
}

func stage4CanonicalObjectKey(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func missingStage4Object(required map[string]int) bool {
	for _, count := range required {
		if count != 0 {
			return true
		}
	}
	return false
}

func cloneStage4RichTable(value schema.Table) schema.Table {
	result := value
	result.ClickHouseOrderBy = append([]string(nil), value.ClickHouseOrderBy...)
	result.Identity = cloneStage4RichIdentity(value.Identity)
	if value.Columns != nil {
		result.Columns = make([]schema.Column, len(value.Columns))
		for index, column := range value.Columns {
			result.Columns[index] = cloneStage4RichColumn(column)
		}
	}
	if value.Indexes != nil {
		result.Indexes = make([]schema.Index, len(value.Indexes))
		for index, item := range value.Indexes {
			result.Indexes[index] = cloneStage4RichIndex(item)
		}
	}
	if value.ForeignKeys != nil {
		result.ForeignKeys = make([]schema.ForeignKey, len(value.ForeignKeys))
		for index, item := range value.ForeignKeys {
			result.ForeignKeys[index] = cloneStage4RichForeignKey(item)
		}
	}
	result.Checks = append([]schema.CheckConstraint(nil), value.Checks...)
	return result
}

func cloneStage4RichIdentity(value *schema.Identity) *schema.Identity {
	if value == nil {
		return nil
	}
	result := *value
	if value.Frontier != nil {
		frontier := *value.Frontier
		result.Frontier = &frontier
	}
	return &result
}

func cloneStage4RichColumn(value schema.Column) schema.Column {
	result := value
	if value.DeclaredType != nil {
		declared := *value.DeclaredType
		declared.Arguments = cloneStage4Ints(value.DeclaredType.Arguments)
		declared.Length = cloneStage4Int64(value.DeclaredType.Length)
		declared.Precision = cloneStage4Int64(value.DeclaredType.Precision)
		declared.Scale = cloneStage4Int64(value.DeclaredType.Scale)
		declared.FractionalSecondPrecision = cloneStage4Int64(
			value.DeclaredType.FractionalSecondPrecision,
		)
		if value.DeclaredType.Spatial != nil {
			spatial := *value.DeclaredType.Spatial
			if value.DeclaredType.Spatial.SRID != nil {
				srid := *value.DeclaredType.Spatial.SRID
				spatial.SRID = &srid
			}
			declared.Spatial = &spatial
		}
		if value.DeclaredType.MySQL != nil {
			mysql := *value.DeclaredType.MySQL
			mysql.BitWidth = cloneStage4Int64(
				value.DeclaredType.MySQL.BitWidth,
			)
			mysql.EnumMembers = cloneStage4Strings(
				value.DeclaredType.MySQL.EnumMembers,
			)
			mysql.SetMembers = cloneStage4Strings(
				value.DeclaredType.MySQL.SetMembers,
			)
			declared.MySQL = &mysql
		}
		result.DeclaredType = &declared
	}
	if value.Default != nil {
		expression := *value.Default
		result.Default = &expression
	}
	return result
}

func cloneStage4Int64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneStage4Ints(value []int) []int {
	if value == nil {
		return nil
	}
	return append([]int{}, value...)
}

func cloneStage4Strings(value []string) []string {
	if value == nil {
		return nil
	}
	return append([]string{}, value...)
}

func cloneStage4RichIndex(value schema.Index) schema.Index {
	result := value
	result.Columns = append([]schema.IndexColumn(nil), value.Columns...)
	return result
}

func cloneStage4RichForeignKey(value schema.ForeignKey) schema.ForeignKey {
	result := value
	result.Columns = append([]string(nil), value.Columns...)
	result.ReferencedColumns = append([]string(nil), value.ReferencedColumns...)
	return result
}
