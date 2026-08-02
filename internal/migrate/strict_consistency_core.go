package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// StrictConsistencyEngine names a source capability rather than a driver
// package. MariaDB remains distinct at the opener boundary even though its
// durable source-engine identity is the MySQL family.
type StrictConsistencyEngine string

const (
	StrictConsistencyPostgres   StrictConsistencyEngine = "postgres"
	StrictConsistencyMSSQL      StrictConsistencyEngine = "mssql"
	StrictConsistencyMySQL      StrictConsistencyEngine = "mysql"
	StrictConsistencyMariaDB    StrictConsistencyEngine = "mariadb"
	StrictConsistencySQLite     StrictConsistencyEngine = "sqlite"
	StrictConsistencyClickHouse StrictConsistencyEngine = "clickhouse"
)

// StrictConsistencyState is the durable protocol surface required before a
// stable source view can authorize target work.
type StrictConsistencyState interface {
	state.Stage4Backend
	state.RangeBackend
}

// StrictConsistencyTable binds one stable-view count to the exact durable work
// identity that will consume it.
type StrictConsistencyTable struct {
	Task                state.TaskKey
	AttemptID           string
	WorkTopologyHash    string
	DurableWorkAttempts int
}

// BuildStrictConsistencyAttemptID derives the opaque evidence attempt ID from
// the durable work identity. The run ID is already a separate state key.
func BuildStrictConsistencyAttemptID(
	task state.TaskKey,
	workTopologyHash string,
	durableWorkAttempts int,
) (string, error) {
	if err := task.Validate(); err != nil {
		return "", fmt.Errorf("strict attempt task identity: %w", err)
	}
	if workTopologyHash == "" {
		return "", errors.New("strict attempt work topology hash is required")
	}
	if err := validateCredentialFreeIdentifier(
		"durable work topology hash",
		workTopologyHash,
	); err != nil {
		return "", err
	}
	if durableWorkAttempts < 0 {
		return "", errors.New(
			"strict attempt durable work attempts must not be negative",
		)
	}
	payload, err := json.Marshal(struct {
		Task         state.TaskKey `json:"task"`
		TopologyHash string        `json:"topology_hash"`
		Attempts     int           `json:"attempts"`
	}{
		Task:         task,
		TopologyHash: workTopologyHash,
		Attempts:     durableWorkAttempts,
	})
	if err != nil {
		return "", fmt.Errorf("encode strict attempt identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return "strict-" + hex.EncodeToString(digest[:]), nil
}

// StrictConsistencyRequest is deliberately independent from config parsing.
// ProcessEpoch must be a fresh, credential-free identifier for this process.
type StrictConsistencyRequest struct {
	RunID        string
	SourceEngine StrictConsistencyEngine
	Scope        state.StrictSnapshotScope
	Resume       bool
	ProcessEpoch string
	State        StrictConsistencyState
	Tables       []StrictConsistencyTable
}

// StrictConsistencyOpenRequest is the complete engine-owned session request.
// RequiredMigrationSnapshot is set only when SQL Server resume must reopen the
// single durable database snapshot rather than create a new source instant.
type StrictConsistencyOpenRequest struct {
	RunID                     string
	SourceEngine              StrictConsistencyEngine
	Scope                     state.StrictSnapshotScope
	Resume                    bool
	ProcessEpoch              string
	Tables                    []StrictConsistencyTable
	RequiredMigrationSnapshot *state.StrictMigrationSnapshot
	// ReopenUnownedMigrationSnapshot is a narrowly-scoped crash recovery
	// allowance for a SQL Server planned migration snapshot created before
	// the core could persist its owner record. It authorizes reopening only
	// the deterministic source-bound snapshot; it never permits creating a
	// replacement source instant during resume.
	ReopenUnownedMigrationSnapshot bool
}

// PlannedStrictConsistencyRequest is the two-phase stable-view request used
// when the exact durable work topology can only be derived from the stable
// source view itself. PostgreSQL network pagination is the first production
// user: its range boundaries must be planned after the exported snapshot
// exists, while target mutation remains forbidden until the resulting exact
// work and same-view count evidence are both durable.
type PlannedStrictConsistencyRequest struct {
	RunID        string
	SourceEngine StrictConsistencyEngine
	Scope        state.StrictSnapshotScope
	Resume       bool
	ProcessEpoch string
	State        StrictConsistencyState
	Tasks        []state.TaskKey
}

// PlannedStrictConsistencyOpenRequest contains only structural table
// identities. No durable attempt is claimed until Plan has checkpointed the
// exact topology observed through the opened stable view.
type PlannedStrictConsistencyOpenRequest struct {
	RunID        string
	SourceEngine StrictConsistencyEngine
	Scope        state.StrictSnapshotScope
	Resume       bool
	ProcessEpoch string
	Tasks        []state.TaskKey
	// RequiredMigrationSnapshot is set only for SQL Server migration resume.
	// The opener must reopen this exact durable database snapshot rather than
	// create a new source instant.
	RequiredMigrationSnapshot *state.StrictMigrationSnapshot
	// ReopenUnownedMigrationSnapshot is set only for SQL Server migration
	// resume when no owner record exists. It covers the narrow crash window
	// after CREATE DATABASE SNAPSHOT and before owner persistence; the opener
	// must reopen the deterministic source-bound snapshot or fail closed.
	ReopenUnownedMigrationSnapshot bool
}

// PlannedStrictConsistencyOpener opens an engine-owned stable view without
// requiring a topology that cannot honestly exist before that view. The
// returned capture must contain every requested task exactly once; attempt IDs
// remain empty until the coordinator binds the finalized durable work.
type PlannedStrictConsistencyOpener interface {
	OpenPlannedStrictConsistency(
		context.Context,
		PlannedStrictConsistencyOpenRequest,
	) (StrictConsistencySession, error)
}

// PlannedStrictConsistencyPlanner derives and durably checkpoints exact work
// through the still-open stable session. It must return the complete finalized
// attempt identity for every selected task. The coordinator re-reads durable
// work and persists same-view evidence before returning an executable handle.
type PlannedStrictConsistencyPlanner func(
	context.Context,
	StrictConsistencySession,
	StrictConsistencyCapture,
) ([]StrictConsistencyTable, error)

// StrictConsistencyTableCapture contains no rows or credentials. The source
// session must obtain ExactSourceRowCount through the exact same stable view
// represented by SnapshotReference.
type StrictConsistencyTableCapture struct {
	Task                state.TaskKey
	AttemptID           string
	SnapshotReference   string
	ExactSourceRowCount int64
	CapturedAt          time.Time
}

// StrictConsistencyCapture is returned by an already-open engine session.
// Migration fields must be empty for table scope. At migration scope every
// table reference must equal MigrationSnapshotReference.
type StrictConsistencyCapture struct {
	MigrationEpochID           string
	MigrationSnapshotReference string
	MigrationCapturedAt        time.Time
	Tables                     []StrictConsistencyTableCapture
}

// StrictConsistencyOpener is implemented by a source adapter. The core never
// synthesizes transactions, locks, exported snapshots, or database snapshots.
// The opener must prove the engine contract: PostgreSQL exported MVCC views,
// SQL Server locks or a supported durable database snapshot, MySQL/MariaDB
// InnoDB plus LOCK TABLES privilege, or a serial SQLite read transaction.
type StrictConsistencyOpener interface {
	OpenStrictConsistency(
		context.Context,
		StrictConsistencyOpenRequest,
	) (StrictConsistencySession, error)
}

// StrictConsistencySession owns the engine stable view. CaptureSameViewEvidence
// must count through that view; adapter-specific read methods may be exposed by
// the concrete session and used inside StrictConsistencyExecution.Run. Close
// must honor its context deadline; the core invokes it once and bounds how long
// the coordinator waits even if an adapter violates that contract.
type StrictConsistencySession interface {
	CaptureSameViewEvidence(context.Context) (StrictConsistencyCapture, error)
	Close(context.Context) error
}

// strictMigrationSnapshotResumeReporter is implemented only by the SQL Server
// migration-snapshot session. It distinguishes a graceful failure that has
// released its source authority from the narrow preserved/cleanup-receipt
// paths that remain safe to resume.
type strictMigrationSnapshotResumeReporter interface {
	StrictMigrationSnapshotResumeAvailable() bool
}

// StrictConsistencyExecution is returned only after every selected table has
// immutable durable evidence. Run keeps the engine session alive for source
// reads and always makes cleanup failure observable.
type StrictConsistencyExecution struct {
	runID             string
	sourceEngine      StrictConsistencyEngine
	scope             state.StrictSnapshotScope
	processEpoch      string
	state             StrictConsistencyState
	tables            []StrictConsistencyTable
	evidence          []state.StrictSnapshotEvidence
	migrationSnapshot *state.StrictMigrationSnapshot
	session           StrictConsistencySession

	runMu     sync.Mutex
	ran       bool
	runActive bool
	closed    bool

	closeOnce sync.Once
	closeErr  error
}

const strictConsistencyCleanupTimeout = 15 * time.Second

// Evidence returns a defensive copy of the durable per-table evidence.
func (execution *StrictConsistencyExecution) Evidence() []state.StrictSnapshotEvidence {
	if execution == nil {
		return nil
	}
	return append([]state.StrictSnapshotEvidence(nil), execution.evidence...)
}

// MigrationSnapshot returns the durable owner for a migration-scoped view.
func (execution *StrictConsistencyExecution) MigrationSnapshot() (state.StrictMigrationSnapshot, bool) {
	if execution == nil || execution.migrationSnapshot == nil {
		return state.StrictMigrationSnapshot{}, false
	}
	return *execution.migrationSnapshot, true
}

// ProcessEpoch is the fresh coordinator epoch for this process. On SQL Server
// resume the surviving snapshot evidence retains its original owner epoch.
func (execution *StrictConsistencyExecution) ProcessEpoch() string {
	if execution == nil {
		return ""
	}
	return execution.processEpoch
}

// Run invokes work against the still-open engine session exactly once and
// closes the session afterward. A callback and cleanup error are joined.
func (execution *StrictConsistencyExecution) Run(
	ctx context.Context,
	work func(context.Context, StrictConsistencySession) error,
) (resultErr error) {
	if execution == nil {
		return errors.New("strict consistency execution is nil")
	}
	execution.runMu.Lock()
	if execution.ran {
		execution.runMu.Unlock()
		return errors.New("strict consistency execution has already run")
	}
	if execution.closed {
		execution.runMu.Unlock()
		return errors.New("strict consistency execution is already closed")
	}
	execution.ran = true
	execution.runActive = true
	execution.runMu.Unlock()
	defer func() {
		execution.markRunInactive()
		resultErr = joinStrictConsistencyCleanup(
			resultErr,
			execution.Close(ctx),
		)
		resultErr = markSQLServerMigrationSnapshotNotResumable(
			execution.session,
			resultErr,
		)
	}()

	if isNilInterface(work) {
		return errors.New("strict consistency work callback is required")
	}
	if ctx == nil {
		return errors.New("strict consistency execution context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNilInterface(execution.state) {
		return NewTransferError(
			ErrorClassState,
			errors.New(
				"strict consistency durable state is unavailable at execution",
			),
		)
	}
	if err := requireStrictConsistencyWorkTasks(
		StrictConsistencyRequest{
			RunID: execution.runID,
			State: execution.state,
			Tables: append(
				[]StrictConsistencyTable(nil),
				execution.tables...,
			),
		},
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"revalidate durable strict work before source execution: %w",
				err,
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	resultErr = work(ctx, execution.session)
	if resultErr == nil {
		resultErr = ctx.Err()
	}
	return resultErr
}

// Close releases the engine stable view once. Its result is stable across
// repeated calls so an operational cleanup failure cannot disappear.
func (execution *StrictConsistencyExecution) Close(ctx context.Context) error {
	if execution == nil {
		return errors.New("strict consistency execution is nil")
	}
	execution.runMu.Lock()
	if execution.runActive {
		execution.runMu.Unlock()
		return errors.New(
			"strict consistency execution cannot close while source work is active",
		)
	}
	execution.closed = true
	execution.runMu.Unlock()
	execution.closeOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		if execution.session == nil {
			execution.closeErr = errors.New("strict consistency session is unavailable")
			return
		}
		if err := closeStrictConsistencySession(ctx, execution.session); err != nil {
			execution.closeErr = fmt.Errorf(
				"release strict source snapshot: %w",
				err,
			)
		}
	})
	return execution.closeErr
}
