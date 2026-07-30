package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
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
}

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

func (execution *StrictConsistencyExecution) markRunInactive() {
	execution.runMu.Lock()
	execution.runActive = false
	execution.runMu.Unlock()
}

// BeginStrictConsistency establishes an engine-owned stable view, captures and
// persists exact same-view evidence, and only then returns executable state.
// Unsupported capability combinations fail before the opener is called.
func BeginStrictConsistency(
	ctx context.Context,
	request StrictConsistencyRequest,
	opener StrictConsistencyOpener,
) (*StrictConsistencyExecution, error) {
	normalized, err := normalizeStrictConsistencyRequest(request)
	if err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	if err := validateStrictConsistencyCapability(
		normalized.SourceEngine,
		normalized.Scope,
	); err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("strict consistency context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNilInterface(normalized.State) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("strict consistency state backend is required"),
		)
	}
	if isNilInterface(opener) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("strict consistency opener is required"),
		)
	}
	if err := requireStrictConsistencyWorkTasks(normalized); err != nil {
		return nil, NewTransferError(ErrorClassState, err)
	}
	existingEvidence, err := loadStrictConsistencyAttemptEvidence(normalized)
	if err != nil {
		return nil, NewTransferError(ErrorClassState, err)
	}

	var latest state.StrictMigrationSnapshot
	var latestFound bool
	if normalized.Scope == state.StrictSnapshotMigration &&
		(normalized.SourceEngine == StrictConsistencyPostgres ||
			normalized.SourceEngine == StrictConsistencyMSSQL) {
		latest, latestFound, err = normalized.State.
			LoadLatestStrictMigrationSnapshot(normalized.RunID)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("load durable strict migration snapshot: %w", err),
			)
		}
		if latestFound {
			if err := validateDurableMigrationSnapshot(
				normalized,
				latest,
			); err != nil {
				return nil, NewTransferError(ErrorClassState, err)
			}
			if !normalized.Resume {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New(
						"a durable strict migration snapshot already exists; continuing this run requires explicit resume",
					),
				)
			}
		}
	}

	openRequest := StrictConsistencyOpenRequest{
		RunID:        normalized.RunID,
		SourceEngine: normalized.SourceEngine,
		Scope:        normalized.Scope,
		Resume:       normalized.Resume,
		ProcessEpoch: normalized.ProcessEpoch,
		Tables: append(
			[]StrictConsistencyTable(nil),
			normalized.Tables...,
		),
	}
	if normalized.SourceEngine == StrictConsistencyMSSQL &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume {
		if !latestFound {
			return nil, NewTransferError(
				ErrorClassState,
				errors.New(
					"SQL Server migration resume requires the surviving durable database snapshot; it is missing and a replacement source instant is forbidden",
				),
			)
		}
		if normalized.ProcessEpoch == latest.ProcessEpoch {
			return nil, NewTransferError(
				ErrorClassPolicy,
				errors.New(
					"SQL Server migration resume requires a fresh coordinator process epoch",
				),
			)
		}
		required := latest
		openRequest.RequiredMigrationSnapshot = &required
	}
	if normalized.SourceEngine == StrictConsistencyPostgres &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume && latestFound &&
		normalized.ProcessEpoch == latest.ProcessEpoch {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"PostgreSQL migration resume requires a fresh process epoch and a new exported snapshot",
			),
		)
	}

	session, err := opener.OpenStrictConsistency(ctx, openRequest)
	if err != nil {
		if normalized.SourceEngine == StrictConsistencyMSSQL &&
			normalized.Scope == state.StrictSnapshotMigration &&
			normalized.Resume {
			err = fmt.Errorf(
				"reopen surviving SQL Server database snapshot; resume fails closed because a replacement source instant is forbidden: %w",
				err,
			)
		}
		primary := NewTransferError(ErrorClassPermanent, err)
		if !isNilInterface(session) {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				primary,
			)
		}
		return nil, primary
	}
	if isNilInterface(session) {
		return nil, NewTransferError(
			ErrorClassPermanent,
			errors.New("strict consistency opener returned a nil session"),
		)
	}

	capture, err := session.CaptureSameViewEvidence(ctx)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				fmt.Errorf("capture strict same-view evidence: %w", err),
			),
		)
	}
	if normalized.SourceEngine == StrictConsistencyPostgres &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume && latestFound &&
		(capture.MigrationEpochID == latest.EpochID ||
			capture.MigrationSnapshotReference == latest.SnapshotReference) {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				errors.New(
					"PostgreSQL migration resume must open a new exported snapshot epoch and reference",
				),
			),
		)
	}
	evidence, owner, err := buildStrictConsistencyEvidence(
		normalized,
		capture,
		openRequest.RequiredMigrationSnapshot,
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassPermanent, err),
		)
	}
	if err := reconcileStrictConsistencyAttemptEvidence(
		normalized,
		evidence,
		owner,
		existingEvidence,
	); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassState, err),
		)
	}
	for index := range evidence {
		if prior, found := existingEvidence[evidence[index].Task]; found {
			evidence[index] = prior
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
	}
	if owner != nil {
		if err := normalized.State.SaveStrictMigrationSnapshot(*owner); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"persist strict migration snapshot before target mutation: %w",
						err,
					),
				),
			)
		}
	}
	for _, record := range evidence {
		if err := ctx.Err(); err != nil {
			return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
		}
		if err := normalized.State.SaveStrictSnapshotEvidence(record); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"persist strict evidence for %s.%s before target mutation: %w",
						record.Task.Schema,
						record.Task.Table,
						err,
					),
				),
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
	}
	if err := requireStrictConsistencyWorkTasks(normalized); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"revalidate durable strict work immediately before authorization: %w",
					err,
				),
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
	}

	execution := &StrictConsistencyExecution{
		runID:        normalized.RunID,
		sourceEngine: normalized.SourceEngine,
		scope:        normalized.Scope,
		processEpoch: normalized.ProcessEpoch,
		state:        normalized.State,
		tables: append(
			[]StrictConsistencyTable(nil),
			normalized.Tables...,
		),
		evidence: append(
			[]state.StrictSnapshotEvidence(nil),
			evidence...,
		),
		session: session,
	}
	if owner != nil {
		copyOwner := *owner
		execution.migrationSnapshot = &copyOwner
	}
	return execution, nil
}

func normalizeStrictConsistencyRequest(
	request StrictConsistencyRequest,
) (StrictConsistencyRequest, error) {
	if request.RunID == "" || strings.TrimSpace(request.RunID) != request.RunID {
		return StrictConsistencyRequest{}, errors.New(
			"strict consistency run ID is required and must not have surrounding whitespace",
		)
	}
	engine, err := normalizeStrictConsistencyEngine(request.SourceEngine)
	if err != nil {
		return StrictConsistencyRequest{}, err
	}
	if request.ProcessEpoch == "" ||
		strings.TrimSpace(request.ProcessEpoch) != request.ProcessEpoch {
		return StrictConsistencyRequest{}, errors.New(
			"strict consistency process epoch is required and must not have surrounding whitespace",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyRequest{}, errors.New(
			"strict consistency requires at least one selected table",
		)
	}
	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, table := range tables {
		if err := table.Task.Validate(); err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d: %w",
				index,
				err,
			)
		}
		if table.AttemptID == "" ||
			strings.TrimSpace(table.AttemptID) != table.AttemptID {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d attempt ID is required and must not have surrounding whitespace",
				index,
			)
		}
		if err := validateCredentialFreeIdentifier(
			"attempt ID",
			table.AttemptID,
		); err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d: %w",
				index,
				err,
			)
		}
		if table.WorkTopologyHash == "" {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d durable work topology hash is required",
				index,
			)
		}
		if err := validateCredentialFreeIdentifier(
			"durable work topology hash",
			table.WorkTopologyHash,
		); err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d: %w",
				index,
				err,
			)
		}
		if table.DurableWorkAttempts < 0 {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d durable work attempts must not be negative",
				index,
			)
		}
		expectedAttemptID, err := BuildStrictConsistencyAttemptID(
			table.Task,
			table.WorkTopologyHash,
			table.DurableWorkAttempts,
		)
		if err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d durable attempt identity: %w",
				index,
				err,
			)
		}
		if table.AttemptID != expectedAttemptID {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d attempt ID does not match its durable task, topology, and attempt counter; expected %q",
				index,
				expectedAttemptID,
			)
		}
		if _, duplicate := seen[table.Task]; duplicate {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency selected task is duplicated: type=%q schema=%q table=%q partition=%q",
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
			)
		}
		seen[table.Task] = struct{}{}
	}
	sort.Slice(tables, func(left, right int) bool {
		return strictConsistencyTableLess(tables[left], tables[right])
	})
	request.SourceEngine = engine
	request.Tables = tables
	return request, nil
}

func normalizeStrictConsistencyEngine(
	engine StrictConsistencyEngine,
) (StrictConsistencyEngine, error) {
	switch strings.ToLower(strings.TrimSpace(string(engine))) {
	case "postgres", "postgresql", "pg":
		return StrictConsistencyPostgres, nil
	case "mssql", "sqlserver", "sql-server":
		return StrictConsistencyMSSQL, nil
	case "mysql":
		return StrictConsistencyMySQL, nil
	case "mariadb", "maria":
		return StrictConsistencyMariaDB, nil
	case "sqlite", "sqlite3", "sqlitedb":
		return StrictConsistencySQLite, nil
	case "clickhouse":
		return StrictConsistencyClickHouse, nil
	default:
		return "", fmt.Errorf(
			"unknown strict consistency source engine %q",
			engine,
		)
	}
}

func strictConsistencyStateEngine(engine StrictConsistencyEngine) string {
	if engine == StrictConsistencyMariaDB {
		return "mysql"
	}
	return string(engine)
}

func validateStrictConsistencyCapability(
	engine StrictConsistencyEngine,
	scope state.StrictSnapshotScope,
) error {
	switch scope {
	case state.StrictSnapshotTable:
		switch engine {
		case StrictConsistencyPostgres,
			StrictConsistencyMSSQL,
			StrictConsistencyMySQL,
			StrictConsistencyMariaDB,
			StrictConsistencySQLite:
			return nil
		case StrictConsistencyClickHouse:
			return errors.New(
				"ClickHouse does not support strict consistency",
			)
		}
	case state.StrictSnapshotMigration:
		switch engine {
		case StrictConsistencyPostgres, StrictConsistencyMSSQL:
			return nil
		case StrictConsistencyMySQL, StrictConsistencyMariaDB:
			return errors.New(
				"MySQL and MariaDB do not support migration-scoped strict consistency",
			)
		case StrictConsistencySQLite:
			return errors.New(
				"SQLite does not support migration-scoped strict consistency",
			)
		case StrictConsistencyClickHouse:
			return errors.New(
				"ClickHouse does not support strict consistency",
			)
		}
	default:
		return fmt.Errorf("unknown strict consistency scope %q", scope)
	}
	return fmt.Errorf(
		"source engine %q does not support strict consistency scope %q",
		engine,
		scope,
	)
}

func requireStrictConsistencyWorkTasks(
	request StrictConsistencyRequest,
) error {
	tasks, _, err := request.State.ListWork(request.RunID)
	if err != nil {
		return fmt.Errorf("list durable strict work tasks: %w", err)
	}
	counts := make(map[state.TaskKey]int, len(tasks))
	for index, task := range tasks {
		if task.RunID != request.RunID {
			return fmt.Errorf(
				"durable work task %d belongs to run %q, not %q",
				index,
				task.RunID,
				request.RunID,
			)
		}
		if err := task.Key.Validate(); err != nil {
			return fmt.Errorf(
				"durable work task %d has invalid structured identity: %w",
				index,
				err,
			)
		}
		counts[task.Key]++
	}
	for _, selected := range request.Tables {
		switch counts[selected.Task] {
		case 0:
			return fmt.Errorf(
				"strict consistency work task does not exist before source snapshot creation: type=%q schema=%q table=%q partition=%q",
				selected.Task.Type,
				selected.Task.Schema,
				selected.Task.Table,
				selected.Task.Partition,
			)
		case 1:
			var durable state.WorkTask
			for _, candidate := range tasks {
				if candidate.Key == selected.Task {
					durable = candidate
					break
				}
			}
			if durable.Status != "running" {
				return fmt.Errorf(
					"strict consistency work task is %q, not running: type=%q schema=%q table=%q partition=%q",
					durable.Status,
					selected.Task.Type,
					selected.Task.Schema,
					selected.Task.Table,
					selected.Task.Partition,
				)
			}
			if durable.TopologyHash != selected.WorkTopologyHash ||
				durable.Attempts != selected.DurableWorkAttempts {
				return fmt.Errorf(
					"strict consistency attempt %q does not match durable work identity: topology=%q attempts=%d, expected topology=%q attempts=%d",
					selected.AttemptID,
					durable.TopologyHash,
					durable.Attempts,
					selected.WorkTopologyHash,
					selected.DurableWorkAttempts,
				)
			}
		default:
			return fmt.Errorf(
				"strict consistency work task is duplicated in durable state: type=%q schema=%q table=%q partition=%q",
				selected.Task.Type,
				selected.Task.Schema,
				selected.Task.Table,
				selected.Task.Partition,
			)
		}
	}
	return nil
}

func loadStrictConsistencyAttemptEvidence(
	request StrictConsistencyRequest,
) (map[state.TaskKey]state.StrictSnapshotEvidence, error) {
	existing := make(
		map[state.TaskKey]state.StrictSnapshotEvidence,
		len(request.Tables),
	)
	for _, selected := range request.Tables {
		record, found, err := request.State.LoadStrictSnapshotEvidence(
			request.RunID,
			selected.Task,
			selected.AttemptID,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"load strict attempt evidence for %s.%s: %w",
				selected.Task.Schema,
				selected.Task.Table,
				err,
			)
		}
		if !found {
			continue
		}
		if record.RunID != request.RunID ||
			record.Task != selected.Task ||
			record.AttemptID != selected.AttemptID {
			return nil, fmt.Errorf(
				"strict attempt evidence lookup returned a mismatched structural identity for %s.%s",
				selected.Task.Schema,
				selected.Task.Table,
			)
		}
		if request.SourceEngine != StrictConsistencyMSSQL ||
			request.Scope != state.StrictSnapshotMigration ||
			!request.Resume {
			return nil, fmt.Errorf(
				"strict attempt %q for %s.%s already has immutable evidence; its prior stable view is not reusable, so resume with a fresh process epoch, advance the durable work attempt, and use its derived new strict attempt ID",
				selected.AttemptID,
				selected.Task.Schema,
				selected.Task.Table,
			)
		}
		existing[selected.Task] = record
	}
	return existing, nil
}

func reconcileStrictConsistencyAttemptEvidence(
	request StrictConsistencyRequest,
	captured []state.StrictSnapshotEvidence,
	owner *state.StrictMigrationSnapshot,
	existing map[state.TaskKey]state.StrictSnapshotEvidence,
) error {
	if len(existing) == 0 {
		return nil
	}
	if request.SourceEngine != StrictConsistencyMSSQL ||
		request.Scope != state.StrictSnapshotMigration ||
		!request.Resume ||
		owner == nil {
		return errors.New(
			"existing strict evidence may only be reused with its surviving SQL Server migration snapshot",
		)
	}
	for _, candidate := range captured {
		prior, found := existing[candidate.Task]
		if !found {
			continue
		}
		if prior.SourceEngine != "mssql" ||
			prior.Scope != state.StrictSnapshotMigration ||
			prior.MigrationEpochID != owner.EpochID ||
			prior.SnapshotReference != owner.SnapshotReference ||
			prior.ProcessEpoch != owner.ProcessEpoch ||
			prior.ExactSourceRowCount < 0 ||
			prior.CapturedAt.IsZero() {
			return fmt.Errorf(
				"existing SQL Server strict evidence for %s.%s does not belong to the one surviving durable snapshot",
				candidate.Task.Schema,
				candidate.Task.Table,
			)
		}
		if prior.ExactSourceRowCount != candidate.ExactSourceRowCount {
			return fmt.Errorf(
				"existing SQL Server strict count for %s.%s is %d but the surviving snapshot now reports %d",
				candidate.Task.Schema,
				candidate.Task.Table,
				prior.ExactSourceRowCount,
				candidate.ExactSourceRowCount,
			)
		}
	}
	return nil
}

func validateDurableMigrationSnapshot(
	request StrictConsistencyRequest,
	snapshot state.StrictMigrationSnapshot,
) error {
	if snapshot.RunID != request.RunID {
		return fmt.Errorf(
			"durable strict migration snapshot belongs to run %q, not %q",
			snapshot.RunID,
			request.RunID,
		)
	}
	expectedEngine := strictConsistencyStateEngine(request.SourceEngine)
	if snapshot.SourceEngine != expectedEngine {
		return fmt.Errorf(
			"durable strict migration snapshot engine %q differs from source engine %q",
			snapshot.SourceEngine,
			expectedEngine,
		)
	}
	if err := validateCredentialFreeIdentifier(
		"migration epoch",
		snapshot.EpochID,
	); err != nil {
		return fmt.Errorf("invalid durable strict migration snapshot: %w", err)
	}
	if err := validateSnapshotReference(snapshot.SnapshotReference); err != nil {
		return fmt.Errorf("invalid durable strict migration snapshot: %w", err)
	}
	if err := validateCredentialFreeIdentifier(
		"owner process epoch",
		snapshot.ProcessEpoch,
	); err != nil {
		return fmt.Errorf("invalid durable strict migration snapshot: %w", err)
	}
	if snapshot.CapturedAt.IsZero() {
		return errors.New(
			"durable strict migration snapshot capture time is missing",
		)
	}
	return nil
}

func buildStrictConsistencyEvidence(
	request StrictConsistencyRequest,
	capture StrictConsistencyCapture,
	requiredOwner *state.StrictMigrationSnapshot,
) (
	[]state.StrictSnapshotEvidence,
	*state.StrictMigrationSnapshot,
	error,
) {
	migrationScoped := request.Scope == state.StrictSnapshotMigration
	if migrationScoped {
		if err := validateCredentialFreeIdentifier(
			"migration epoch",
			capture.MigrationEpochID,
		); err != nil {
			return nil, nil, err
		}
		if err := validateSnapshotReference(
			capture.MigrationSnapshotReference,
		); err != nil {
			return nil, nil, err
		}
		if capture.MigrationCapturedAt.IsZero() {
			return nil, nil, errors.New(
				"strict migration snapshot capture time is required",
			)
		}
	} else if capture.MigrationEpochID != "" ||
		capture.MigrationSnapshotReference != "" ||
		!capture.MigrationCapturedAt.IsZero() {
		return nil, nil, errors.New(
			"table-scoped strict evidence cannot claim a migration epoch or snapshot",
		)
	}

	type captureIdentity struct {
		task      state.TaskKey
		attemptID string
	}
	expected := make(map[captureIdentity]StrictConsistencyTable, len(request.Tables))
	for _, table := range request.Tables {
		expected[captureIdentity{task: table.Task, attemptID: table.AttemptID}] = table
	}
	captured := make(map[captureIdentity]StrictConsistencyTableCapture, len(capture.Tables))
	for index, table := range capture.Tables {
		identity := captureIdentity{task: table.Task, attemptID: table.AttemptID}
		if _, exists := expected[identity]; !exists {
			return nil, nil, fmt.Errorf(
				"strict session returned unexpected or mismatched table evidence at index %d: type=%q schema=%q table=%q partition=%q attempt=%q",
				index,
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
				table.AttemptID,
			)
		}
		if _, duplicate := captured[identity]; duplicate {
			return nil, nil, fmt.Errorf(
				"strict session returned duplicate evidence for type=%q schema=%q table=%q partition=%q attempt=%q",
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
				table.AttemptID,
			)
		}
		if table.ExactSourceRowCount < 0 {
			return nil, nil, fmt.Errorf(
				"strict session returned a negative exact row count for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if table.CapturedAt.IsZero() {
			return nil, nil, fmt.Errorf(
				"strict session omitted the capture time for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if err := validateSnapshotReference(table.SnapshotReference); err != nil {
			return nil, nil, fmt.Errorf(
				"strict session reference for %s.%s: %w",
				table.Task.Schema,
				table.Task.Table,
				err,
			)
		}
		if migrationScoped &&
			table.SnapshotReference != capture.MigrationSnapshotReference {
			return nil, nil, fmt.Errorf(
				"strict session table %s.%s reference %q differs from migration snapshot reference %q",
				table.Task.Schema,
				table.Task.Table,
				table.SnapshotReference,
				capture.MigrationSnapshotReference,
			)
		}
		if migrationScoped &&
			table.CapturedAt.Before(capture.MigrationCapturedAt) {
			return nil, nil, fmt.Errorf(
				"strict session table %s.%s capture time precedes its migration snapshot",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		captured[identity] = table
	}
	if len(captured) != len(expected) {
		for _, selected := range request.Tables {
			identity := captureIdentity{
				task: selected.Task, attemptID: selected.AttemptID,
			}
			if _, exists := captured[identity]; !exists {
				return nil, nil, fmt.Errorf(
					"strict session omitted evidence for type=%q schema=%q table=%q partition=%q attempt=%q",
					selected.Task.Type,
					selected.Task.Schema,
					selected.Task.Table,
					selected.Task.Partition,
					selected.AttemptID,
				)
			}
		}
		return nil, nil, errors.New(
			"strict session returned an invalid evidence cardinality",
		)
	}

	stateEngine := strictConsistencyStateEngine(request.SourceEngine)
	processEpoch := request.ProcessEpoch
	migrationEpoch := ""
	var owner *state.StrictMigrationSnapshot
	if migrationScoped {
		migrationEpoch = capture.MigrationEpochID
		candidate := state.StrictMigrationSnapshot{
			RunID:             request.RunID,
			EpochID:           capture.MigrationEpochID,
			SourceEngine:      stateEngine,
			SnapshotReference: capture.MigrationSnapshotReference,
			ProcessEpoch:      request.ProcessEpoch,
			CapturedAt:        capture.MigrationCapturedAt.UTC(),
		}
		if requiredOwner != nil {
			if request.SourceEngine != StrictConsistencyMSSQL ||
				!request.Resume {
				return nil, nil, errors.New(
					"a durable migration owner may only be required for SQL Server resume",
				)
			}
			if capture.MigrationEpochID != requiredOwner.EpochID ||
				capture.MigrationSnapshotReference !=
					requiredOwner.SnapshotReference {
				return nil, nil, errors.New(
					"SQL Server resume did not reuse the one surviving durable database snapshot; replacement is forbidden",
				)
			}
			candidate = *requiredOwner
			migrationEpoch = requiredOwner.EpochID
			processEpoch = requiredOwner.ProcessEpoch
		}
		owner = &candidate
	}

	evidence := make([]state.StrictSnapshotEvidence, 0, len(request.Tables))
	for _, selected := range request.Tables {
		table := captured[captureIdentity{
			task: selected.Task, attemptID: selected.AttemptID,
		}]
		evidence = append(evidence, state.StrictSnapshotEvidence{
			RunID:               request.RunID,
			Task:                selected.Task,
			AttemptID:           selected.AttemptID,
			SourceEngine:        stateEngine,
			Scope:               request.Scope,
			MigrationEpochID:    migrationEpoch,
			SnapshotReference:   table.SnapshotReference,
			ProcessEpoch:        processEpoch,
			ExactSourceRowCount: table.ExactSourceRowCount,
			CapturedAt:          table.CapturedAt.UTC(),
		})
	}
	return evidence, owner, nil
}

func validateSnapshotReference(reference string) error {
	return validateCredentialFreeIdentifier(
		"snapshot reference",
		reference,
	)
}

// validateCredentialFreeIdentifier accepts only a short opaque token. Engine
// adapters must encode or hash a snapshot handle into this grammar and must
// never place credentials in it. The core deliberately does not guess whether
// otherwise-valid token text has secret meaning.
func validateCredentialFreeIdentifier(label, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	const maximumOpaqueTokenBytes = 256
	if len(value) > maximumOpaqueTokenBytes {
		return fmt.Errorf(
			"%s exceeds %d bytes",
			label,
			maximumOpaqueTokenBytes,
		)
	}
	isASCIIAlphanumeric := func(character byte) bool {
		return character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9'
	}
	if !isASCIIAlphanumeric(value[0]) ||
		!isASCIIAlphanumeric(value[len(value)-1]) {
		return fmt.Errorf(
			"%s must begin and end with an ASCII letter or digit",
			label,
		)
	}
	for index := 1; index < len(value)-1; index++ {
		character := value[index]
		if !isASCIIAlphanumeric(character) &&
			character != '.' &&
			character != '_' &&
			character != '-' {
			return fmt.Errorf(
				"%s must contain only ASCII letters, digits, dot, underscore, or hyphen",
				label,
			)
		}
	}
	return nil
}

func strictConsistencyTableLess(
	left StrictConsistencyTable,
	right StrictConsistencyTable,
) bool {
	if left.Task.Type != right.Task.Type {
		return left.Task.Type < right.Task.Type
	}
	if left.Task.Schema != right.Task.Schema {
		return left.Task.Schema < right.Task.Schema
	}
	if left.Task.Table != right.Task.Table {
		return left.Task.Table < right.Task.Table
	}
	if left.Task.Partition != right.Task.Partition {
		return left.Task.Partition < right.Task.Partition
	}
	return left.AttemptID < right.AttemptID
}

func closeStrictConsistencyAfterFailure(
	ctx context.Context,
	session StrictConsistencySession,
	primary error,
) error {
	cleanupErr := closeStrictConsistencySession(ctx, session)
	if cleanupErr == nil {
		return primary
	}
	return joinStrictConsistencyCleanup(
		primary,
		fmt.Errorf("release strict source snapshot after failure: %w", cleanupErr),
	)
}

// joinStrictConsistencyCleanup keeps a cleanup deadline operationally visible
// without letting its context identity override a primary state, policy, or
// source failure in ClassifyTransferError. A cancellation primary remains
// cancellation, and a standalone Close still exposes its context cause.
func joinStrictConsistencyCleanup(primary error, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	if primary != nil &&
		ClassifyTransferError(primary) != ErrorClassCanceled &&
		(errors.Is(cleanup, context.Canceled) ||
			errors.Is(cleanup, context.DeadlineExceeded)) {
		cleanup = errors.New(cleanup.Error())
	}
	return errors.Join(primary, cleanup)
}

func closeStrictConsistencySession(
	caller context.Context,
	session StrictConsistencySession,
) error {
	if isNilInterface(session) {
		return errors.New("strict consistency session is unavailable")
	}
	base := context.Background()
	if caller != nil {
		base = context.WithoutCancel(caller)
	}
	now := time.Now()
	deadline := now.Add(strictConsistencyCleanupTimeout)
	if caller != nil {
		if callerDeadline, found := caller.Deadline(); found &&
			callerDeadline.After(now) &&
			callerDeadline.Before(deadline) {
			deadline = callerDeadline
		}
	}
	cleanupContext, cancel := context.WithDeadline(base, deadline)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- session.Close(cleanupContext)
	}()
	select {
	case err := <-result:
		return err
	case <-cleanupContext.Done():
		return fmt.Errorf(
			"strict source snapshot cleanup exceeded its bounded deadline: %w",
			cleanupContext.Err(),
		)
	}
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
