package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type plannedStrictTestOpener struct {
	session StrictConsistencySession
	err     error
	request PlannedStrictConsistencyOpenRequest
	events  *[]string
}

type stage4PostgresStrictParallelTestSource struct {
	adapterStableNetworkSource

	active  *atomic.Int32
	maximum *atomic.Int32
	entered chan<- struct{}
	release <-chan struct{}
}

func (
	source *stage4PostgresStrictParallelTestSource,
) ReadNetworkRangePage(
	ctx context.Context,
	_ schema.Table,
	_ []string,
	_ PaginationPlan,
	_ PaginationRange,
	_ NetworkReadRequest,
) (NetworkReadPage, error) {
	active := source.active.Add(1)
	defer source.active.Add(-1)
	for {
		maximum := source.maximum.Load()
		if active <= maximum ||
			source.maximum.CompareAndSwap(maximum, active) {
			break
		}
	}
	select {
	case <-ctx.Done():
		return NetworkReadPage{}, ctx.Err()
	case source.entered <- struct{}{}:
	}
	select {
	case <-ctx.Done():
		return NetworkReadPage{}, ctx.Err()
	case <-source.release:
		return NetworkReadPage{}, nil
	}
}

func (opener *plannedStrictTestOpener) OpenPlannedStrictConsistency(
	_ context.Context,
	request PlannedStrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	opener.request = request
	if opener.events != nil {
		*opener.events = append(*opener.events, "open-unbound")
	}
	return opener.session, opener.err
}

func TestBeginPlannedStrictConsistencyBindsWorkInsideOpenEpoch(
	t *testing.T,
) {
	at := strictCoreTime()
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	events := []string{}
	backend := &strictCoreFakeState{events: &events}
	session := &strictCoreFakeSession{
		events: &events,
		capture: StrictConsistencyCapture{
			Tables: []StrictConsistencyTableCapture{{
				Task:                task,
				SnapshotReference:   "snapshot-planned-1",
				ExactSourceRowCount: 17,
				CapturedAt:          at,
			}},
		},
	}
	opener := &plannedStrictTestOpener{
		session: session,
		events:  &events,
	}
	const topology = "epoch-bound-topology"
	attemptID, err := BuildStrictConsistencyAttemptID(
		task,
		topology,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := BeginPlannedStrictConsistency(
		context.Background(),
		PlannedStrictConsistencyRequest{
			RunID:        "planned-run",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-planned-1",
			State:        backend,
			Tasks:        []state.TaskKey{task},
		},
		opener,
		func(
			_ context.Context,
			got StrictConsistencySession,
			capture StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			events = append(events, "plan")
			if got != session ||
				len(capture.Tables) != 1 ||
				capture.Tables[0].AttemptID != "" {
				t.Fatalf(
					"planner session/capture = %T %#v",
					got,
					capture,
				)
			}
			backend.tasks = []state.WorkTask{{
				RunID:        "planned-run",
				Key:          task,
				Status:       "running",
				TopologyHash: topology,
			}}
			return []StrictConsistencyTable{{
				Task:             task,
				AttemptID:        attemptID,
				WorkTopologyHash: topology,
			}}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"open-unbound",
		"capture",
		"plan",
		"list-work",
		"load-evidence:public/items",
		"save-evidence:public/items",
		"list-work",
	}
	if !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %#v, want %#v", events, want)
	}
	evidence := execution.Evidence()
	if len(evidence) != 1 ||
		evidence[0].AttemptID != attemptID ||
		evidence[0].SnapshotReference != "snapshot-planned-1" ||
		evidence[0].ExactSourceRowCount != 17 {
		t.Fatalf("planned evidence = %#v", evidence)
	}
	if err := execution.Run(
		context.Background(),
		func(
			context.Context,
			StrictConsistencySession,
		) error {
			events = append(events, "mutate")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if got := events[len(events)-3:]; !reflect.DeepEqual(
		got,
		[]string{"list-work", "mutate", "close"},
	) {
		t.Fatalf("authorization/work/cleanup = %#v", got)
	}
}

func TestBeginPlannedStrictConsistencyPlannerFailureClosesEpoch(
	t *testing.T,
) {
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	session := &strictCoreFakeSession{
		capture: StrictConsistencyCapture{
			Tables: []StrictConsistencyTableCapture{{
				Task:                task,
				SnapshotReference:   "snapshot-planned-failure",
				ExactSourceRowCount: 1,
				CapturedAt:          strictCoreTime(),
			}},
		},
	}
	backend := &strictCoreFakeState{}
	_, err := BeginPlannedStrictConsistency(
		context.Background(),
		PlannedStrictConsistencyRequest{
			RunID:        "planned-failure",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-planned-failure",
			State:        backend,
			Tasks:        []state.TaskKey{task},
		},
		&plannedStrictTestOpener{session: session},
		func(
			context.Context,
			StrictConsistencySession,
			StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			return nil, errors.New("pagination failed")
		},
	)
	if err == nil || !strings.Contains(err.Error(), "pagination failed") {
		t.Fatalf("error = %v", err)
	}
	closeCalls, _, _ := session.closeSnapshot()
	if closeCalls != 1 {
		t.Fatalf("snapshot close calls = %d, want 1", closeCalls)
	}
	if len(backend.evidence) != 0 {
		t.Fatalf(
			"planner failure persisted evidence: %#v",
			backend.evidence,
		)
	}
}

// TestBeginPlannedSQLServerMigrationRecoversSnapshotCreatedBeforeOwner proves
// the narrow crash recovery path: a deterministic SQL Server database snapshot
// may survive CREATE DATABASE SNAPSHOT before the core can Save its owner. A
// resume is allowed to reopen that one source instant, bind the same finalized
// topology/attempt, and then persist the missing owner; it cannot request a
// replacement snapshot.
func TestBeginPlannedSQLServerMigrationRecoversSnapshotCreatedBeforeOwner(
	t *testing.T,
) {
	const (
		runID    = "mssql-pre-owner-crash"
		topology = "surviving-snapshot-topology"
	)
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
	}
	reference := sqlServerMigrationSnapshotName(runID, "source_db")
	epoch := sqlServerMigrationSnapshotEpoch(reference)
	at := strictCoreTime()
	backend := &strictCoreFakeState{}
	session := &strictCoreFakeSession{capture: StrictConsistencyCapture{
		MigrationEpochID:           epoch,
		MigrationSnapshotReference: reference,
		MigrationCapturedAt:        at,
		Tables: []StrictConsistencyTableCapture{{
			Task:                task,
			SnapshotReference:   reference,
			ExactSourceRowCount: 9,
			CapturedAt:          at,
		}},
	}}
	opener := &plannedStrictTestOpener{session: session}
	attemptID, err := BuildStrictConsistencyAttemptID(task, topology, 0)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := BeginPlannedStrictConsistency(
		context.Background(),
		PlannedStrictConsistencyRequest{
			RunID:        runID,
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			Resume:       true,
			ProcessEpoch: "recovery-process",
			State:        backend,
			Tasks:        []state.TaskKey{task},
		},
		opener,
		func(
			_ context.Context,
			_ StrictConsistencySession,
			capture StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			if capture.MigrationSnapshotReference != reference ||
				capture.MigrationEpochID != epoch {
				t.Fatalf("recovered snapshot capture = %#v", capture)
			}
			backend.tasks = []state.WorkTask{{
				RunID: runID, Key: task, Status: "running", TopologyHash: topology,
			}}
			return []StrictConsistencyTable{{
				Task: task, AttemptID: attemptID, WorkTopologyHash: topology,
			}}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if opener.request.RequiredMigrationSnapshot != nil ||
		!opener.request.ReopenUnownedMigrationSnapshot {
		t.Fatalf("pre-owner recovery request = %#v", opener.request)
	}
	owner, found := execution.MigrationSnapshot()
	if !found || owner.RunID != runID || owner.EpochID != epoch ||
		owner.SnapshotReference != reference ||
		owner.ProcessEpoch != "recovery-process" {
		t.Fatalf("recovered durable owner = %#v found=%v", owner, found)
	}
	evidence := execution.Evidence()
	if len(evidence) != 1 || evidence[0].AttemptID != attemptID ||
		evidence[0].SnapshotReference != reference {
		t.Fatalf("recovered strict evidence = %#v", evidence)
	}
	if err := execution.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPlannedSQLServerMigrationSnapshotResumabilityOutcomes(t *testing.T) {
	const (
		runID    = "mssql-graceful-outcome"
		topology = "mssql-graceful-topology"
	)
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
	}
	reference := sqlServerMigrationSnapshotName(runID, "source_db")
	capture := StrictConsistencyCapture{
		MigrationEpochID:           sqlServerMigrationSnapshotEpoch(reference),
		MigrationSnapshotReference: reference,
		MigrationCapturedAt:        strictCoreTime(),
		Tables: []StrictConsistencyTableCapture{{
			Task: task, SnapshotReference: reference, ExactSourceRowCount: 3,
			CapturedAt: strictCoreTime(),
		}},
	}
	attemptID, err := BuildStrictConsistencyAttemptID(task, topology, 0)
	if err != nil {
		t.Fatal(err)
	}
	plan := func(backend *strictCoreFakeState) func(
		context.Context,
		StrictConsistencySession,
		StrictConsistencyCapture,
	) ([]StrictConsistencyTable, error) {
		return func(
			_ context.Context,
			_ StrictConsistencySession,
			_ StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			backend.tasks = []state.WorkTask{{
				RunID: runID, Key: task, Status: "running", TopologyHash: topology,
			}}
			return []StrictConsistencyTable{{
				Task: task, AttemptID: attemptID, WorkTopologyHash: topology,
			}}, nil
		}
	}

	t.Run("graceful-transfer-failure-releases-and-forbids-resume", func(t *testing.T) {
		backend := &strictCoreFakeState{}
		session := &strictMigrationSnapshotResumeTestSession{
			strictCoreFakeSession: strictCoreFakeSession{capture: capture},
		}
		execution, err := BeginPlannedStrictConsistency(
			context.Background(),
			PlannedStrictConsistencyRequest{
				RunID: runID, SourceEngine: StrictConsistencyMSSQL,
				Scope: state.StrictSnapshotMigration, ProcessEpoch: "initial-process",
				State: backend, Tasks: []state.TaskKey{task},
			},
			&plannedStrictTestOpener{session: session},
			plan(backend),
		)
		if err != nil {
			t.Fatal(err)
		}
		transferErr := errors.New("target writer failed")
		err = execution.Run(context.Background(), func(context.Context, StrictConsistencySession) error {
			return transferErr
		})
		if !errors.Is(err, transferErr) ||
			!errors.Is(err, ErrSQLServerMigrationSnapshotNotResumable) {
			t.Fatalf("graceful SQL Server migration failure = %v", err)
		}
		closeCalls, _, _ := session.closeSnapshot()
		if closeCalls != 1 || !session.released {
			t.Fatalf("graceful SQL Server migration close calls=%d released=%t", closeCalls, session.released)
		}
	})

	t.Run("graceful-cancellation-releases-and-forbids-resume", func(t *testing.T) {
		backend := &strictCoreFakeState{}
		session := &strictMigrationSnapshotResumeTestSession{
			strictCoreFakeSession: strictCoreFakeSession{capture: capture},
		}
		execution, err := BeginPlannedStrictConsistency(
			context.Background(),
			PlannedStrictConsistencyRequest{
				RunID: runID + "-cancel", SourceEngine: StrictConsistencyMSSQL,
				Scope: state.StrictSnapshotMigration, ProcessEpoch: "initial-process",
				State: backend, Tasks: []state.TaskKey{task},
			},
			&plannedStrictTestOpener{session: session},
			func(
				_ context.Context,
				_ StrictConsistencySession,
				_ StrictConsistencyCapture,
			) ([]StrictConsistencyTable, error) {
				backend.tasks = []state.WorkTask{{
					RunID: runID + "-cancel", Key: task, Status: "running", TopologyHash: topology,
				}}
				return []StrictConsistencyTable{{
					Task: task, AttemptID: attemptID, WorkTopologyHash: topology,
				}}, nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		err = execution.Run(context.Background(), func(context.Context, StrictConsistencySession) error {
			return context.Canceled
		})
		if !errors.Is(err, context.Canceled) ||
			!errors.Is(err, ErrSQLServerMigrationSnapshotNotResumable) ||
			!session.released {
			t.Fatalf("graceful SQL Server migration cancellation = %v released=%t", err, session.released)
		}
	})

	t.Run("hard-stop-owner-remains-resumable-and-reopens-exact-snapshot", func(t *testing.T) {
		backend := &strictCoreFakeState{}
		firstSession := &strictMigrationSnapshotResumeTestSession{
			strictCoreFakeSession: strictCoreFakeSession{capture: capture},
			resumeAvailable:       true,
		}
		execution, err := BeginPlannedStrictConsistency(
			context.Background(),
			PlannedStrictConsistencyRequest{
				RunID: runID, SourceEngine: StrictConsistencyMSSQL,
				Scope: state.StrictSnapshotMigration, ProcessEpoch: "original-process",
				State: backend, Tasks: []state.TaskKey{task},
			},
			&plannedStrictTestOpener{session: firstSession},
			plan(backend),
		)
		if err != nil {
			t.Fatal(err)
		}
		owner, found := execution.MigrationSnapshot()
		if !found {
			t.Fatal("initial SQL Server migration has no durable owner")
		}
		backend.latest, backend.latestFound = owner, true
		resumedCapture := capture
		resumedSession := &strictMigrationSnapshotResumeTestSession{
			strictCoreFakeSession: strictCoreFakeSession{capture: resumedCapture},
			resumeAvailable:       true,
		}
		resumed := &plannedStrictTestOpener{session: resumedSession}
		resumedExecution, err := BeginPlannedStrictConsistency(
			context.Background(),
			PlannedStrictConsistencyRequest{
				RunID: runID, SourceEngine: StrictConsistencyMSSQL,
				Scope: state.StrictSnapshotMigration, Resume: true,
				ProcessEpoch: "resume-process", State: backend, Tasks: []state.TaskKey{task},
			},
			resumed,
			plan(backend),
		)
		if err != nil {
			t.Fatal(err)
		}
		if resumed.request.RequiredMigrationSnapshot == nil ||
			*resumed.request.RequiredMigrationSnapshot != owner {
			t.Fatalf("hard-stop resume owner = %#v, want %#v", resumed.request.RequiredMigrationSnapshot, owner)
		}
		if err := resumedExecution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("missing-required-snapshot-forbids-further-resume", func(t *testing.T) {
		backend := &strictCoreFakeState{}
		owner := state.StrictMigrationSnapshot{
			RunID: runID, EpochID: capture.MigrationEpochID, SourceEngine: "mssql",
			SnapshotReference: capture.MigrationSnapshotReference,
			ProcessEpoch:      "original-process", CapturedAt: capture.MigrationCapturedAt,
		}
		backend.latest, backend.latestFound = owner, true
		opener := &plannedStrictTestOpener{err: fmt.Errorf("%w: test fault", errSQLServerMigrationSnapshotMissing)}
		_, err := BeginPlannedStrictConsistency(
			context.Background(),
			PlannedStrictConsistencyRequest{
				RunID: runID, SourceEngine: StrictConsistencyMSSQL,
				Scope: state.StrictSnapshotMigration, Resume: true,
				ProcessEpoch: "resume-process", State: backend, Tasks: []state.TaskKey{task},
			},
			opener,
			plan(backend),
		)
		if !errors.Is(err, ErrSQLServerMigrationSnapshotNotResumable) {
			t.Fatalf("missing SQL Server migration snapshot resume error = %v", err)
		}
		if opener.request.RequiredMigrationSnapshot == nil ||
			*opener.request.RequiredMigrationSnapshot != owner {
			t.Fatalf("missing snapshot resume owner = %#v, want %#v", opener.request.RequiredMigrationSnapshot, owner)
		}
	})
}

type strictMigrationSnapshotResumeTestSession struct {
	strictCoreFakeSession
	resumeAvailable bool
	released        bool
}

func (session *strictMigrationSnapshotResumeTestSession) Close(ctx context.Context) error {
	session.released = true
	return session.strictCoreFakeSession.Close(ctx)
}

func (session *strictMigrationSnapshotResumeTestSession) StrictMigrationSnapshotResumeAvailable() bool {
	return session.resumeAvailable
}

func TestBeginPlannedStrictConsistencyEvidenceFailureClosesEpoch(
	t *testing.T,
) {
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	const topology = "planned-state-failure-topology"
	attemptID, err := BuildStrictConsistencyAttemptID(
		task,
		topology,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	session := &strictCoreFakeSession{
		capture: StrictConsistencyCapture{
			Tables: []StrictConsistencyTableCapture{{
				Task:                task,
				SnapshotReference:   "snapshot-planned-state-failure",
				ExactSourceRowCount: 1,
				CapturedAt:          strictCoreTime(),
			}},
		},
	}
	backend := &strictCoreFakeState{
		saveEvidenceAt:  1,
		saveEvidenceErr: errors.New("state unavailable"),
	}
	_, err = BeginPlannedStrictConsistency(
		context.Background(),
		PlannedStrictConsistencyRequest{
			RunID:        "planned-state-failure",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-planned-state-failure",
			State:        backend,
			Tasks:        []state.TaskKey{task},
		},
		&plannedStrictTestOpener{session: session},
		func(
			context.Context,
			StrictConsistencySession,
			StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			backend.tasks = []state.WorkTask{{
				RunID:        "planned-state-failure",
				Key:          task,
				Status:       "running",
				TopologyHash: topology,
			}}
			return []StrictConsistencyTable{{
				Task:             task,
				AttemptID:        attemptID,
				WorkTopologyHash: topology,
			}}, nil
		},
	)
	if err == nil ||
		ClassifyTransferError(err) != ErrorClassState ||
		!strings.Contains(err.Error(), "state unavailable") {
		t.Fatalf("error = %v", err)
	}
	closeCalls, _, _ := session.closeSnapshot()
	if closeCalls != 1 {
		t.Fatalf("snapshot close calls = %d, want 1", closeCalls)
	}
	if len(backend.evidence) != 0 {
		t.Fatalf(
			"failed evidence write appeared durable: %#v",
			backend.evidence,
		)
	}
}

func TestBeginPlannedStrictConsistencyRejectsWorkOutsideEpochTaskSet(
	t *testing.T,
) {
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	other := task
	other.Table = "other"
	session := &strictCoreFakeSession{
		capture: StrictConsistencyCapture{
			Tables: []StrictConsistencyTableCapture{{
				Task:                task,
				SnapshotReference:   "snapshot-planned-task-set",
				ExactSourceRowCount: 1,
				CapturedAt:          strictCoreTime(),
			}},
		},
	}
	topology := "unexpected-work"
	attemptID, err := BuildStrictConsistencyAttemptID(
		other,
		topology,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	backend := &strictCoreFakeState{
		tasks: []state.WorkTask{{
			RunID:        "planned-task-set",
			Key:          other,
			Status:       "running",
			TopologyHash: topology,
		}},
	}
	_, err = BeginPlannedStrictConsistency(
		context.Background(),
		PlannedStrictConsistencyRequest{
			RunID:        "planned-task-set",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-planned-task-set",
			State:        backend,
			Tasks:        []state.TaskKey{task},
		},
		&plannedStrictTestOpener{session: session},
		func(
			context.Context,
			StrictConsistencySession,
			StrictConsistencyCapture,
		) ([]StrictConsistencyTable, error) {
			return []StrictConsistencyTable{{
				Task:             other,
				AttemptID:        attemptID,
				WorkTopologyHash: topology,
			}}, nil
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "unexpected task") {
		t.Fatalf("error = %v", err)
	}
	closeCalls, _, _ := session.closeSnapshot()
	if closeCalls != 1 {
		t.Fatalf("snapshot close calls = %d, want 1", closeCalls)
	}
}

func TestStage4PostgresStrictWorkIdentityBindsEpochAndSnapshot(
	t *testing.T,
) {
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	base := stage4AdapterWork{
		task:     task,
		strategy: stage4AdapterCopyStrategy,
		topology: "base-pagination-topology",
		ranges: []state.RangeState{
			{ID: "range/0", TopologyHash: "base-pagination-topology"},
			{ID: "range/1", TopologyHash: "base-pagination-topology"},
		},
	}
	first := stage4PostgresStrictEpochBinding{
		tables: map[state.TaskKey]stage4PostgresStrictTableBinding{
			task: {
				scope:             state.StrictSnapshotMigration,
				processEpoch:      "process-1",
				snapshotReference: "snapshot-1",
			},
		},
	}
	left, err := first.finalizeWork(
		cloneStage4AdapterNetworkWork(base),
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := first.finalizeWork(
		cloneStage4AdapterNetworkWork(base),
	)
	if err != nil {
		t.Fatal(err)
	}
	if left.topology != right.topology ||
		left.topology == base.topology {
		t.Fatalf(
			"strict topology left=%q right=%q base=%q",
			left.topology,
			right.topology,
			base.topology,
		)
	}
	for _, workRange := range left.ranges {
		if workRange.TopologyHash != left.topology {
			t.Fatalf(
				"range topology = %q, want %q",
				workRange.TopologyHash,
				left.topology,
			)
		}
	}
	nextProcess := first
	nextProcess.tables = map[state.TaskKey]stage4PostgresStrictTableBinding{
		task: {
			scope:             state.StrictSnapshotMigration,
			processEpoch:      "process-2",
			snapshotReference: "snapshot-1",
		},
	}
	next, err := nextProcess.finalizeWork(
		cloneStage4AdapterNetworkWork(base),
	)
	if err != nil {
		t.Fatal(err)
	}
	if next.topology == left.topology {
		t.Fatal("fresh process epoch reused the prior work topology")
	}
	nextSnapshot := first
	nextSnapshot.tables = map[state.TaskKey]stage4PostgresStrictTableBinding{
		task: {
			scope:             state.StrictSnapshotMigration,
			processEpoch:      "process-1",
			snapshotReference: "snapshot-2",
		},
	}
	next, err = nextSnapshot.finalizeWork(
		cloneStage4AdapterNetworkWork(base),
	)
	if err != nil {
		t.Fatal(err)
	}
	if next.topology == left.topology {
		t.Fatal("different exported snapshot reused the prior work topology")
	}
}

func TestStage4AdapterValidationSpecsRequireStrictSnapshotCount(
	t *testing.T,
) {
	table := stage4AdapterTestTable()
	key := stage4RichTableKey{
		schema: table.Schema,
		table:  table.Name,
	}
	prepared := stage4AdapterPrepared{
		mode: "upsert",
		plans: []adapterTablePlan{{
			source: table,
			target: table,
			columns: []string{
				"id",
			},
		}},
		gate: Stage4SchemaGateResult{
			ValidationTables: []schema.Table{table},
		},
		strictSourceRows: map[stage4RichTableKey]int64{
			key: 41,
		},
	}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 ||
		specs[0].StrictSourceRows == nil ||
		*specs[0].StrictSourceRows != 41 {
		t.Fatalf("strict validation specs = %#v", specs)
	}
	prepared.strictSourceRows = map[stage4RichTableKey]int64{}
	if _, err := stage4AdapterValidationTableSpecs(
		prepared,
	); err == nil ||
		!strings.Contains(err.Error(), "missing or invalid") {
		t.Fatalf("missing strict count error = %v", err)
	}
}

func TestStage4PostgresStrictParallelSourceOverlapsWithinBound(
	t *testing.T,
) {
	var active atomic.Int32
	var maximum atomic.Int32
	var secondary atomic.Int32
	entered := make(chan struct{}, 3)
	release := make(chan struct{})
	newSource := func() *stage4PostgresStrictParallelTestSource {
		return &stage4PostgresStrictParallelTestSource{
			active:  &active,
			maximum: &maximum,
			entered: entered,
			release: release,
		}
	}
	parallel, err := newStage4PostgresStrictParallelSource(
		newSource(),
		2,
		func(
			_ context.Context,
			work func(adapterStableNetworkSource) error,
		) error {
			secondary.Add(1)
			return work(newSource())
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	var group sync.WaitGroup
	errs := make(chan error, 3)
	for index := 0; index < 3; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, err := parallel.ReadNetworkRangePage(
				ctx,
				schema.Table{Name: "items"},
				[]string{"id"},
				PaginationPlan{},
				PaginationRange{},
				NetworkReadRequest{},
			)
			errs <- err
		}()
	}
	for index := 0; index < 2; index++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("parallel strict readers did not overlap")
		}
	}
	select {
	case <-entered:
		t.Fatal("parallel strict source exceeded its admitted reader bound")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() != 2 || secondary.Load() == 0 {
		t.Fatalf(
			"parallel strict reader maximum=%d secondary=%d, want 2 and at least 1",
			maximum.Load(),
			secondary.Load(),
		)
	}
}

func TestStage4PostgresStrictSnapshotOwnerStaysWithinConnectionBound(
	t *testing.T,
) {
	effective := func(
		connectionLimit int,
		readers int,
		writers int,
	) config.EffectiveTransferPlan {
		return config.EffectiveTransferPlan{
			ConnectionLimit: config.EffectiveInt{
				Value: connectionLimit,
			},
			Readers: config.EffectiveInt{Value: readers},
			Writers: config.EffectiveInt{Value: writers},
		}
	}
	for _, test := range []struct {
		name        string
		resources   config.EffectiveTransferPlan
		wantReaders int
		wantWriters int
	}{
		{
			name:        "reserve_default_saturated_plan",
			resources:   effective(4, 2, 2),
			wantReaders: 2,
			wantWriters: 1,
		},
		{
			name:        "retain_existing_owner_capacity",
			resources:   effective(4, 2, 1),
			wantReaders: 2,
			wantWriters: 1,
		},
		{
			name:        "minimum_viable_plan",
			resources:   effective(3, 1, 1),
			wantReaders: 1,
			wantWriters: 1,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got, err := reserveStage4AdapterStrictSnapshotOwner(
				test.resources,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got.Readers.Value != test.wantReaders ||
				got.Writers.Value != test.wantWriters ||
				got.Readers.Value+got.Writers.Value+1 >
					got.ConnectionLimit.Value {
				t.Fatalf(
					"strict resources = %#v, want readers=%d writers=%d with one owner",
					got,
					test.wantReaders,
					test.wantWriters,
				)
			}
		})
	}
	if _, err := reserveStage4AdapterStrictSnapshotOwner(
		effective(2, 1, 1),
	); err == nil ||
		ClassifyTransferError(err) != ErrorClassPolicy ||
		!strings.Contains(err.Error(), "at least 3") {
		t.Fatalf("impossible strict connection plan error = %v", err)
	}
}
