package migrate

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func TestStrictConsistencyCapabilityMatrixFailsBeforeOpen(t *testing.T) {
	tests := []struct {
		engine    StrictConsistencyEngine
		scope     state.StrictSnapshotScope
		supported bool
	}{
		{StrictConsistencyPostgres, state.StrictSnapshotTable, true},
		{StrictConsistencyPostgres, state.StrictSnapshotMigration, true},
		{StrictConsistencyMSSQL, state.StrictSnapshotTable, true},
		{StrictConsistencyMSSQL, state.StrictSnapshotMigration, true},
		{StrictConsistencyMySQL, state.StrictSnapshotTable, true},
		{StrictConsistencyMySQL, state.StrictSnapshotMigration, false},
		{StrictConsistencyMariaDB, state.StrictSnapshotTable, true},
		{StrictConsistencyMariaDB, state.StrictSnapshotMigration, false},
		{StrictConsistencySQLite, state.StrictSnapshotTable, true},
		{StrictConsistencySQLite, state.StrictSnapshotMigration, false},
		{StrictConsistencyClickHouse, state.StrictSnapshotTable, false},
		{StrictConsistencyClickHouse, state.StrictSnapshotMigration, false},
		{"future-engine", state.StrictSnapshotTable, false},
		{StrictConsistencyPostgres, "future-scope", false},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%s/%s", test.engine, test.scope)
		t.Run(name, func(t *testing.T) {
			at := strictCoreTime()
			table := strictCoreTables()[0]
			backend := &strictCoreFakeState{
				tasks: []state.WorkTask{{
					RunID: "run-1", Key: table.Task, Status: "running",
				}},
			}
			opener := &strictCoreFakeOpener{}
			request := StrictConsistencyRequest{
				RunID: "run-1", SourceEngine: test.engine,
				Scope: test.scope, ProcessEpoch: "process-1",
				State: backend, Tables: []StrictConsistencyTable{table},
			}
			opener.session = &strictCoreFakeSession{
				capture: strictCoreCapture(request, at),
			}
			execution, err := BeginStrictConsistency(
				context.Background(),
				request,
				opener,
			)
			if !test.supported {
				if err == nil {
					if execution != nil {
						_ = execution.Close(context.Background())
					}
					t.Fatal("unsupported strict capability succeeded")
				}
				if opener.calls != 0 {
					t.Fatalf("unsupported capability called opener %d times", opener.calls)
				}
				if ClassifyTransferError(err) != ErrorClassPolicy {
					t.Fatalf("unsupported error class = %q: %v", ClassifyTransferError(err), err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if opener.calls != 1 {
				t.Fatalf("supported capability opener calls = %d", opener.calls)
			}
			if err := execution.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBuildStrictConsistencyAttemptIDBindsDurableIdentity(t *testing.T) {
	table := strictCoreTables()[0]
	first := strictCoreAttemptID(table)
	if first == "" || first != strictCoreAttemptID(table) {
		t.Fatalf("nondeterministic strict attempt ID %q", first)
	}
	changedTask := table
	changedTask.Task.Schema = table.Task.Schema + ".other"
	changedTopology := table
	changedTopology.WorkTopologyHash += "-other"
	changedAttempt := table
	changedAttempt.DurableWorkAttempts++
	for name, candidate := range map[string]StrictConsistencyTable{
		"task":     changedTask,
		"topology": changedTopology,
		"attempt":  changedAttempt,
	} {
		if got := strictCoreAttemptID(candidate); got == first {
			t.Fatalf("%s change retained attempt ID %q", name, got)
		}
	}
	if _, err := BuildStrictConsistencyAttemptID(
		state.TaskKey{},
		"topology",
		0,
	); err == nil {
		t.Fatal("invalid task produced a strict attempt ID")
	}
	if _, err := BuildStrictConsistencyAttemptID(
		table.Task,
		table.WorkTopologyHash,
		-1,
	); err == nil {
		t.Fatal("negative durable attempt produced a strict attempt ID")
	}
}

func TestBeginStrictConsistencyOrdersEvidenceBeforeExecutableState(t *testing.T) {
	at := strictCoreTime()
	tables := strictCoreTables()
	events := []string{}
	backend := &strictCoreFakeState{
		tasks: []state.WorkTask{
			{RunID: "run-1", Key: tables[0].Task, Status: "running"},
			{RunID: "run-1", Key: tables[1].Task, Status: "running"},
		},
		events: &events,
	}
	request := StrictConsistencyRequest{
		RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
		Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
		State: backend,
		Tables: []StrictConsistencyTable{
			tables[1],
			tables[0],
		},
	}
	capture := strictCoreCapture(request, at)
	capture.Tables[0], capture.Tables[1] = capture.Tables[1], capture.Tables[0]
	session := &strictCoreFakeSession{capture: capture, events: &events}
	opener := &strictCoreFakeOpener{session: session, events: &events}

	execution, err := BeginStrictConsistency(
		context.Background(),
		request,
		opener,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantBeforeReturn := []string{
		"list-work",
		"load-evidence:a.b/c:d",
		"load-evidence:z.schema/items",
		"open",
		"capture",
		"save-evidence:a.b/c:d",
		"save-evidence:z.schema/items",
		"list-work",
	}
	if !reflect.DeepEqual(events, wantBeforeReturn) {
		t.Fatalf("events before executable state = %#v, want %#v", events, wantBeforeReturn)
	}
	evidence := execution.Evidence()
	if len(evidence) != 2 ||
		evidence[0].Task != tables[0].Task ||
		evidence[1].Task != tables[1].Task {
		t.Fatalf("ordered structural evidence = %#v", evidence)
	}
	if evidence[0].MigrationEpochID != "" ||
		evidence[0].Scope != state.StrictSnapshotTable ||
		evidence[0].ProcessEpoch != request.ProcessEpoch {
		t.Fatalf("table evidence claimed migration state: %#v", evidence[0])
	}
	evidence[0].SnapshotReference = "caller-mutated"
	if execution.Evidence()[0].SnapshotReference == "caller-mutated" {
		t.Fatal("Evidence returned aliased storage")
	}
	if err := execution.Run(
		context.Background(),
		func(_ context.Context, got StrictConsistencySession) error {
			if got != session {
				t.Fatal("Run received a different engine session")
			}
			events = append(events, "work")
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if got := events[len(events)-2:]; !reflect.DeepEqual(got, []string{"work", "close"}) {
		t.Fatalf("work/cleanup order = %#v", got)
	}
	if err := execution.Run(context.Background(), func(context.Context, StrictConsistencySession) error {
		return nil
	}); err == nil || !strings.Contains(err.Error(), "already run") {
		t.Fatalf("second Run error = %v", err)
	}
}

func TestBeginStrictConsistencyRevalidatesBeforeAuthorization(t *testing.T) {
	at := strictCoreTime()
	table := strictCoreTables()[0]

	t.Run("cancellation-after-final-evidence", func(t *testing.T) {
		beginContext, cancel := context.WithCancel(context.Background())
		backend := &strictCoreFakeState{
			tasks: []state.WorkTask{{
				RunID: "run-1", Key: table.Task, Status: "running",
			}},
			saveEvidenceHook: func(int) {
				cancel()
			},
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
			Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
			State: backend, Tables: []StrictConsistencyTable{table},
		}
		session := &strictCoreFakeSession{
			capture: strictCoreCapture(request, at),
		}
		execution, err := BeginStrictConsistency(
			beginContext,
			request,
			&strictCoreFakeOpener{session: session},
		)
		if execution != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"post-evidence cancellation execution=%#v error=%v",
				execution,
				err,
			)
		}
		if len(backend.evidence) != 1 || backend.listCalls != 1 {
			t.Fatalf(
				"post-evidence durable state evidence=%d list=%d",
				len(backend.evidence),
				backend.listCalls,
			)
		}
		if calls, _, _ := session.closeSnapshot(); calls != 1 {
			t.Fatalf("post-evidence cancellation close calls = %d", calls)
		}
	})

	t.Run("durable-work-changed-during-capture", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks: []state.WorkTask{{
				RunID: "run-1", Key: table.Task, Status: "running",
			}},
		}
		backend.listHook = func(call int) {
			if call == 2 {
				backend.tasks[0].Attempts++
			}
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
			Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
			State: backend, Tables: []StrictConsistencyTable{table},
		}
		session := &strictCoreFakeSession{
			capture: strictCoreCapture(request, at),
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{session: session},
		)
		if execution != nil ||
			ClassifyTransferError(err) != ErrorClassState ||
			!strings.Contains(
				err.Error(),
				"immediately before authorization",
			) {
			t.Fatalf(
				"stale-work authorization execution=%#v error=%v class=%q",
				execution,
				err,
				ClassifyTransferError(err),
			)
		}
		if len(backend.evidence) != 1 || backend.listCalls != 2 {
			t.Fatalf(
				"stale-work durable state evidence=%d list=%d",
				len(backend.evidence),
				backend.listCalls,
			)
		}
		if calls, _, _ := session.closeSnapshot(); calls != 1 {
			t.Fatalf("stale-work close calls = %d", calls)
		}
	})
}

func TestStrictConsistencyRunRevalidatesWorkAndCancellation(t *testing.T) {
	at := strictCoreTime()
	table := strictCoreTables()[0]
	newExecution := func(
		t *testing.T,
	) (
		*StrictConsistencyExecution,
		*strictCoreFakeState,
		*strictCoreFakeSession,
	) {
		t.Helper()
		backend := &strictCoreFakeState{
			tasks: []state.WorkTask{{
				RunID: "run-1", Key: table.Task, Status: "running",
			}},
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencySQLite,
			Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
			State: backend, Tables: []StrictConsistencyTable{table},
		}
		session := &strictCoreFakeSession{
			capture: strictCoreCapture(request, at),
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{session: session},
		)
		if err != nil {
			t.Fatal(err)
		}
		return execution, backend, session
	}

	for name, mutate := range map[string]func(*strictCoreFakeState){
		"missing-task": func(backend *strictCoreFakeState) {
			backend.tasks = nil
		},
		"changed-topology": func(backend *strictCoreFakeState) {
			backend.tasks[0].TopologyHash = "different-topology"
		},
		"changed-attempt": func(backend *strictCoreFakeState) {
			backend.tasks[0].Attempts++
		},
	} {
		t.Run(name, func(t *testing.T) {
			execution, backend, session := newExecution(t)
			mutate(backend)
			called := false
			err := execution.Run(
				context.Background(),
				func(context.Context, StrictConsistencySession) error {
					called = true
					return nil
				},
			)
			if err == nil ||
				ClassifyTransferError(err) != ErrorClassState ||
				called ||
				!strings.Contains(
					err.Error(),
					"before source execution",
				) {
				t.Fatalf(
					"stale delayed execution called=%v error=%v class=%q",
					called,
					err,
					ClassifyTransferError(err),
				)
			}
			if calls, _, _ := session.closeSnapshot(); calls != 1 {
				t.Fatalf("stale delayed execution close calls = %d", calls)
			}
		})
	}

	t.Run("already-cancelled", func(t *testing.T) {
		execution, _, session := newExecution(t)
		runContext, cancel := context.WithCancel(context.Background())
		cancel()
		called := false
		err := execution.Run(
			runContext,
			func(context.Context, StrictConsistencySession) error {
				called = true
				return nil
			},
		)
		if !errors.Is(err, context.Canceled) || called {
			t.Fatalf(
				"pre-cancelled execution called=%v error=%v",
				called,
				err,
			)
		}
		if calls, sawCanceled, hadDeadline := session.closeSnapshot(); calls != 1 || sawCanceled || !hadDeadline {
			t.Fatalf(
				"pre-cancel cleanup calls=%d cancelled=%v deadline=%v",
				calls,
				sawCanceled,
				hadDeadline,
			)
		}
	})

	t.Run("cancelled-by-callback", func(t *testing.T) {
		execution, _, session := newExecution(t)
		runContext, cancel := context.WithCancel(context.Background())
		err := execution.Run(
			runContext,
			func(context.Context, StrictConsistencySession) error {
				cancel()
				return nil
			},
		)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("callback cancellation error = %v", err)
		}
		if calls, sawCanceled, hadDeadline := session.closeSnapshot(); calls != 1 || sawCanceled || !hadDeadline {
			t.Fatalf(
				"callback-cancel cleanup calls=%d cancelled=%v deadline=%v",
				calls,
				sawCanceled,
				hadDeadline,
			)
		}
	})

	t.Run("cleanup-deadline-does-not-mask-state", func(t *testing.T) {
		execution, backend, session := newExecution(t)
		backend.tasks[0].TopologyHash = "different-topology"
		session.closeErr = context.DeadlineExceeded
		err := execution.Run(
			context.Background(),
			func(context.Context, StrictConsistencySession) error {
				t.Fatal("stale work callback was invoked")
				return nil
			},
		)
		if err == nil ||
			ClassifyTransferError(err) != ErrorClassState ||
			!strings.Contains(err.Error(), context.DeadlineExceeded.Error()) {
			t.Fatalf(
				"state/cleanup classification error=%v class=%q",
				err,
				ClassifyTransferError(err),
			)
		}
		if calls, _, _ := session.closeSnapshot(); calls != 1 {
			t.Fatalf("state/cleanup close calls = %d", calls)
		}
	})
}

func TestBeginStrictConsistencyRequiresExistingUniqueStructuredWork(t *testing.T) {
	table := strictCoreTables()[0]
	listErr := errors.New("state unavailable")
	tests := []struct {
		name      string
		tasks     []state.WorkTask
		listErr   error
		wantClass TransferErrorClass
	}{
		{name: "missing", tasks: nil, wantClass: ErrorClassState},
		{
			name: "duplicate",
			tasks: []state.WorkTask{
				{RunID: "run-1", Key: table.Task},
				{RunID: "run-1", Key: table.Task},
			},
			wantClass: ErrorClassState,
		},
		{
			name: "wrong-run",
			tasks: []state.WorkTask{{
				RunID: "other-run", Key: table.Task,
			}},
			wantClass: ErrorClassState,
		},
		{
			name: "completed",
			tasks: []state.WorkTask{{
				RunID: "run-1", Key: table.Task, Status: "completed",
			}},
			wantClass: ErrorClassState,
		},
		{
			name: "topology-mismatch",
			tasks: []state.WorkTask{{
				RunID: "run-1", Key: table.Task,
				TopologyHash: "other-topology",
			}},
			wantClass: ErrorClassState,
		},
		{
			name: "attempt-mismatch",
			tasks: []state.WorkTask{{
				RunID: "run-1", Key: table.Task, Attempts: 1,
			}},
			wantClass: ErrorClassState,
		},
		{
			name: "list-error", listErr: listErr,
			wantClass: ErrorClassState,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &strictCoreFakeState{
				tasks: test.tasks, listErr: test.listErr,
			}
			opener := &strictCoreFakeOpener{}
			_, err := BeginStrictConsistency(
				context.Background(),
				StrictConsistencyRequest{
					RunID:        "run-1",
					SourceEngine: StrictConsistencyPostgres,
					Scope:        state.StrictSnapshotTable,
					ProcessEpoch: "process-1",
					State:        backend,
					Tables:       []StrictConsistencyTable{table},
				},
				opener,
			)
			if err == nil || ClassifyTransferError(err) != test.wantClass {
				t.Fatalf("error = %v, class=%q", err, ClassifyTransferError(err))
			}
			if opener.calls != 0 {
				t.Fatalf("opener called %d times without durable work", opener.calls)
			}
			if test.listErr != nil && !errors.Is(err, listErr) {
				t.Fatalf("error %v does not preserve %v", err, listErr)
			}
		})
	}
}

func TestBeginStrictConsistencyRejectsAmbiguousCoordinatorRequests(t *testing.T) {
	table := strictCoreTables()[0]
	tests := []struct {
		name   string
		mutate func(*StrictConsistencyRequest)
	}{
		{
			name: "empty-run",
			mutate: func(request *StrictConsistencyRequest) {
				request.RunID = ""
			},
		},
		{
			name: "empty-process",
			mutate: func(request *StrictConsistencyRequest) {
				request.ProcessEpoch = ""
			},
		},
		{
			name: "process-control-character",
			mutate: func(request *StrictConsistencyRequest) {
				request.ProcessEpoch = "process\nsecret"
			},
		},
		{
			name: "missing-table",
			mutate: func(request *StrictConsistencyRequest) {
				request.Tables = nil
			},
		},
		{
			name: "duplicate-structured-table",
			mutate: func(request *StrictConsistencyRequest) {
				request.Tables = append(request.Tables, request.Tables[0])
				request.Tables[1].AttemptID = "other-attempt"
			},
		},
		{
			name: "missing-attempt",
			mutate: func(request *StrictConsistencyRequest) {
				request.Tables[0].AttemptID = ""
			},
		},
		{
			name: "nil-state",
			mutate: func(request *StrictConsistencyRequest) {
				request.State = nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &strictCoreFakeState{
				tasks: []state.WorkTask{{RunID: "run-1", Key: table.Task}},
			}
			request := StrictConsistencyRequest{
				RunID:        "run-1",
				SourceEngine: StrictConsistencyPostgres,
				Scope:        state.StrictSnapshotTable,
				ProcessEpoch: "process-1",
				State:        backend,
				Tables:       []StrictConsistencyTable{table},
			}
			test.mutate(&request)
			opener := &strictCoreFakeOpener{}
			_, err := BeginStrictConsistency(
				context.Background(),
				request,
				opener,
			)
			if err == nil || ClassifyTransferError(err) != ErrorClassPolicy {
				t.Fatalf("error = %v, class=%q", err, ClassifyTransferError(err))
			}
			if opener.calls != 0 {
				t.Fatalf("invalid request called opener %d times", opener.calls)
			}
		})
	}

	backend := &strictCoreFakeState{
		tasks: []state.WorkTask{{RunID: "run-1", Key: table.Task}},
	}
	request := StrictConsistencyRequest{
		RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
		Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
		State: backend, Tables: []StrictConsistencyTable{table},
	}
	_, err := BeginStrictConsistency(context.Background(), request, nil)
	if err == nil || ClassifyTransferError(err) != ErrorClassPolicy {
		t.Fatalf("nil opener error = %v", err)
	}
}

func TestPostgresMigrationResumeCreatesVisibleFreshEpoch(t *testing.T) {
	at := strictCoreTime()
	table := strictCoreTables()[0]
	old := state.StrictMigrationSnapshot{
		RunID: "run-1", EpochID: "pg-epoch-old",
		SourceEngine:      "postgres",
		SnapshotReference: "pg-snapshot-old",
		ProcessEpoch:      "process-old", CapturedAt: at.Add(-time.Hour),
	}
	backend := &strictCoreFakeState{
		tasks:  []state.WorkTask{{RunID: "run-1", Key: table.Task}},
		latest: old, latestFound: true,
	}
	request := StrictConsistencyRequest{
		RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
		Scope: state.StrictSnapshotMigration, Resume: true,
		ProcessEpoch: "process-new", State: backend,
		Tables: []StrictConsistencyTable{table},
	}
	capture := strictCoreCapture(request, at)
	capture.MigrationEpochID = "pg-epoch-new"
	capture.MigrationSnapshotReference = "pg-snapshot-new"
	capture.Tables[0].SnapshotReference = "pg-snapshot-new"
	session := &strictCoreFakeSession{capture: capture}
	opener := &strictCoreFakeOpener{session: session}
	execution, err := BeginStrictConsistency(
		context.Background(),
		request,
		opener,
	)
	if err != nil {
		t.Fatal(err)
	}
	owner, found := execution.MigrationSnapshot()
	if !found || owner.EpochID != "pg-epoch-new" ||
		owner.SnapshotReference != "pg-snapshot-new" ||
		owner.ProcessEpoch != "process-new" {
		t.Fatalf("new PostgreSQL owner = %#v found=%v", owner, found)
	}
	evidence := execution.Evidence()
	if len(evidence) != 1 ||
		evidence[0].MigrationEpochID != owner.EpochID ||
		evidence[0].ProcessEpoch != "process-new" ||
		evidence[0].SnapshotReference != "pg-snapshot-new" {
		t.Fatalf("new PostgreSQL epoch evidence = %#v", evidence)
	}
	if opener.request.RequiredMigrationSnapshot != nil {
		t.Fatal("PostgreSQL resume was asked to reuse the old snapshot")
	}
	if err := execution.Close(context.Background()); err != nil {
		t.Fatal(err)
	}

	request.ProcessEpoch = old.ProcessEpoch
	secondOpener := &strictCoreFakeOpener{}
	_, err = BeginStrictConsistency(
		context.Background(),
		request,
		secondOpener,
	)
	if err == nil || !strings.Contains(err.Error(), "fresh process epoch") {
		t.Fatalf("same-process PostgreSQL resume error = %v", err)
	}
	if secondOpener.calls != 0 {
		t.Fatal("same-process PostgreSQL resume reached opener")
	}
}

func TestSQLServerMigrationResumeRequiresAndReusesSurvivingSnapshot(t *testing.T) {
	at := strictCoreTime()
	table := strictCoreTables()[0]
	owner := state.StrictMigrationSnapshot{
		RunID: "run-1", EpochID: "mssql-epoch-1",
		SourceEngine:      "mssql",
		SnapshotReference: "database_snapshot_1",
		ProcessEpoch:      "process-original", CapturedAt: at.Add(-time.Hour),
	}
	request := StrictConsistencyRequest{
		RunID: "run-1", SourceEngine: StrictConsistencyMSSQL,
		Scope: state.StrictSnapshotMigration, Resume: true,
		ProcessEpoch: "process-resume",
		Tables:       []StrictConsistencyTable{table},
	}
	t.Run("missing", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks: []state.WorkTask{{RunID: "run-1", Key: table.Task}},
		}
		request.State = backend
		opener := &strictCoreFakeOpener{}
		_, err := BeginStrictConsistency(
			context.Background(),
			request,
			opener,
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassState ||
			!strings.Contains(err.Error(), "replacement source instant is forbidden") {
			t.Fatalf("missing SQL Server owner error = %v", err)
		}
		if opener.calls != 0 {
			t.Fatal("missing SQL Server owner reached opener")
		}
	})
	t.Run("unavailable", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks:  []state.WorkTask{{RunID: "run-1", Key: table.Task}},
			latest: owner, latestFound: true,
		}
		request.State = backend
		unavailable := errors.New("snapshot was dropped")
		opener := &strictCoreFakeOpener{openErr: unavailable}
		_, err := BeginStrictConsistency(
			context.Background(),
			request,
			opener,
		)
		if err == nil || !errors.Is(err, unavailable) ||
			!strings.Contains(err.Error(), "fails closed") {
			t.Fatalf("unavailable SQL Server owner error = %v", err)
		}
		if opener.calls != 1 ||
			opener.request.RequiredMigrationSnapshot == nil ||
			*opener.request.RequiredMigrationSnapshot != owner {
			t.Fatalf("required owner request = %#v", opener.request)
		}
	})
	t.Run("reuse", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks:  []state.WorkTask{{RunID: "run-1", Key: table.Task}},
			latest: owner, latestFound: true,
		}
		request.State = backend
		capture := strictCoreCapture(request, at)
		capture.MigrationEpochID = owner.EpochID
		capture.MigrationSnapshotReference = owner.SnapshotReference
		capture.Tables[0].SnapshotReference = owner.SnapshotReference
		opener := &strictCoreFakeOpener{
			session: &strictCoreFakeSession{capture: capture},
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			opener,
		)
		if err != nil {
			t.Fatal(err)
		}
		gotOwner, found := execution.MigrationSnapshot()
		if !found || gotOwner != owner {
			t.Fatalf("reused owner = %#v found=%v", gotOwner, found)
		}
		gotEvidence := execution.Evidence()
		if len(gotEvidence) != 1 ||
			gotEvidence[0].ProcessEpoch != owner.ProcessEpoch ||
			gotEvidence[0].MigrationEpochID != owner.EpochID {
			t.Fatalf("SQL Server resume evidence = %#v", gotEvidence)
		}
		if execution.ProcessEpoch() != request.ProcessEpoch {
			t.Fatalf("current coordinator process epoch = %q", execution.ProcessEpoch())
		}
		if err := execution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("replacement-rejected", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks:  []state.WorkTask{{RunID: "run-1", Key: table.Task}},
			latest: owner, latestFound: true,
		}
		request.State = backend
		capture := strictCoreCapture(request, at)
		capture.MigrationEpochID = "replacement-epoch"
		capture.MigrationSnapshotReference = "replacement-snapshot"
		capture.Tables[0].SnapshotReference = "replacement-snapshot"
		session := &strictCoreFakeSession{capture: capture}
		opener := &strictCoreFakeOpener{session: session}
		_, err := BeginStrictConsistency(
			context.Background(),
			request,
			opener,
		)
		if err == nil || !strings.Contains(err.Error(), "replacement is forbidden") {
			t.Fatalf("SQL Server replacement error = %v", err)
		}
		if calls, _, _ := session.closeSnapshot(); calls != 1 {
			t.Fatalf("replacement session close calls = %d", calls)
		}
	})
}

func TestStrictConsistencyCaptureFailsClosedOnMalformedEvidence(t *testing.T) {
	at := strictCoreTime()
	tables := strictCoreTables()
	baseRequest := StrictConsistencyRequest{
		RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
		Scope: state.StrictSnapshotMigration, ProcessEpoch: "process-1",
		Tables: tables,
	}
	tests := []struct {
		name   string
		mutate func(*StrictConsistencyRequest, *StrictConsistencyCapture)
	}{
		{
			name: "missing",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables = capture.Tables[:1]
			},
		},
		{
			name: "duplicate",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables[1] = capture.Tables[0]
			},
		},
		{
			name: "mismatched-attempt",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables[0].AttemptID = "wrong-attempt"
			},
		},
		{
			name: "unexpected-extra",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				extra := capture.Tables[0]
				extra.Task.Table = "unexpected"
				capture.Tables = append(capture.Tables, extra)
			},
		},
		{
			name: "negative-count",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables[0].ExactSourceRowCount = -1
			},
		},
		{
			name: "missing-table-time",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables[0].CapturedAt = time.Time{}
			},
		},
		{
			name: "mismatched-migration-reference",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables[0].SnapshotReference = "other-view"
			},
		},
		{
			name: "table-capture-precedes-migration-owner",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.Tables[0].CapturedAt =
					capture.MigrationCapturedAt.Add(-time.Nanosecond)
			},
		},
		{
			name: "credential-bearing-reference",
			mutate: func(_ *StrictConsistencyRequest, capture *StrictConsistencyCapture) {
				capture.MigrationSnapshotReference = "postgres://user:password@host/db"
				for index := range capture.Tables {
					capture.Tables[index].SnapshotReference =
						capture.MigrationSnapshotReference
				}
			},
		},
		{
			name: "table-claims-migration",
			mutate: func(request *StrictConsistencyRequest, _ *StrictConsistencyCapture) {
				request.Scope = state.StrictSnapshotTable
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := baseRequest
			request.Tables = append(
				[]StrictConsistencyTable(nil),
				baseRequest.Tables...,
			)
			capture := strictCoreCapture(request, at)
			test.mutate(&request, &capture)
			backend := &strictCoreFakeState{
				tasks: []state.WorkTask{
					{RunID: "run-1", Key: tables[0].Task},
					{RunID: "run-1", Key: tables[1].Task},
				},
			}
			request.State = backend
			session := &strictCoreFakeSession{capture: capture}
			_, err := BeginStrictConsistency(
				context.Background(),
				request,
				&strictCoreFakeOpener{session: session},
			)
			if err == nil {
				t.Fatal("malformed engine evidence succeeded")
			}
			if calls, _, _ := session.closeSnapshot(); calls != 1 {
				t.Fatalf("failed capture close calls = %d", calls)
			}
			if len(backend.evidence) != 0 || len(backend.owners) != 0 {
				t.Fatalf(
					"malformed evidence was persisted: owners=%#v evidence=%#v",
					backend.owners,
					backend.evidence,
				)
			}
		})
	}
}

func TestStrictConsistencyStateFailuresCloseBeforeSuccess(t *testing.T) {
	at := strictCoreTime()
	tables := strictCoreTables()
	saveOwnerErr := errors.New("owner disk full")
	saveEvidenceErr := errors.New("evidence disk full")
	tests := []struct {
		name            string
		saveOwnerErr    error
		saveEvidenceAt  int
		saveEvidenceErr error
		wantEvidence    int
	}{
		{
			name: "owner", saveOwnerErr: saveOwnerErr,
			wantEvidence: 0,
		},
		{
			name: "second-evidence", saveEvidenceAt: 2,
			saveEvidenceErr: saveEvidenceErr, wantEvidence: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &strictCoreFakeState{
				tasks: []state.WorkTask{
					{RunID: "run-1", Key: tables[0].Task},
					{RunID: "run-1", Key: tables[1].Task},
				},
				saveOwnerErr:    test.saveOwnerErr,
				saveEvidenceAt:  test.saveEvidenceAt,
				saveEvidenceErr: test.saveEvidenceErr,
			}
			request := StrictConsistencyRequest{
				RunID:        "run-1",
				SourceEngine: StrictConsistencyPostgres,
				Scope:        state.StrictSnapshotMigration,
				ProcessEpoch: "process-1",
				State:        backend, Tables: tables,
			}
			session := &strictCoreFakeSession{
				capture: strictCoreCapture(request, at),
			}
			execution, err := BeginStrictConsistency(
				context.Background(),
				request,
				&strictCoreFakeOpener{session: session},
			)
			if execution != nil || err == nil ||
				ClassifyTransferError(err) != ErrorClassState {
				t.Fatalf(
					"execution=%#v error=%v class=%q",
					execution,
					err,
					ClassifyTransferError(err),
				)
			}
			if calls, _, _ := session.closeSnapshot(); calls != 1 {
				t.Fatalf("state failure close calls = %d", calls)
			}
			if len(backend.evidence) != test.wantEvidence {
				t.Fatalf("persisted evidence = %d, want %d", len(backend.evidence), test.wantEvidence)
			}
			if test.saveOwnerErr != nil && !errors.Is(err, test.saveOwnerErr) {
				t.Fatalf("owner state cause lost: %v", err)
			}
			if test.saveEvidenceErr != nil && !errors.Is(err, test.saveEvidenceErr) {
				t.Fatalf("evidence state cause lost: %v", err)
			}
		})
	}
}

func TestStrictConsistencyOpaqueTokenGrammar(t *testing.T) {
	valid := []string{
		"a",
		"snapshot.handle_1-2",
		strings.Repeat("a", 256),
	}
	for _, value := range valid {
		if err := validateCredentialFreeIdentifier(
			"test token",
			value,
		); err != nil {
			t.Fatalf("valid opaque token %q: %v", value, err)
		}
	}
	invalid := map[string]string{
		"empty":         "",
		"too-long":      strings.Repeat("a", 257),
		"leading-mark":  "-snapshot",
		"trailing-mark": "snapshot.",
		"uri":           "postgres://user-pass-host",
		"query":         "snapshot?password-secret",
		"userinfo":      "user@host",
		"key-value":     "password=secret",
		"separator":     "snapshot/handle",
		"whitespace":    "snapshot handle",
		"control":       "snapshot\nhandle",
		"non-ascii":     "snápshot",
		"encoded-query": "password%3Dsecret",
		"semicolon-dsn": "user-alice;pwd-secret",
	}
	for name, value := range invalid {
		t.Run(name, func(t *testing.T) {
			if err := validateCredentialFreeIdentifier(
				"test token",
				value,
			); err == nil {
				t.Fatalf("unsafe/non-opaque token %q was accepted", value)
			}
			if err := validateSnapshotReference(value); err == nil {
				t.Fatalf(
					"unsafe/non-opaque snapshot reference %q was accepted",
					value,
				)
			}
		})
	}
}

func TestStrictConsistencyCleanupFailuresRemainVisible(t *testing.T) {
	at := strictCoreTime()
	table := strictCoreTables()[0]
	backend := &strictCoreFakeState{
		tasks: []state.WorkTask{{RunID: "run-1", Key: table.Task}},
	}
	request := StrictConsistencyRequest{
		RunID: "run-1", SourceEngine: StrictConsistencySQLite,
		Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
		State: backend, Tables: []StrictConsistencyTable{table},
	}
	cleanupErr := errors.New("release failed")
	workErr := errors.New("copy failed")
	session := &strictCoreFakeSession{
		capture:  strictCoreCapture(request, at),
		closeErr: cleanupErr,
	}
	execution, err := BeginStrictConsistency(
		context.Background(),
		request,
		&strictCoreFakeOpener{session: session},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = execution.Run(
		context.Background(),
		func(context.Context, StrictConsistencySession) error {
			return workErr
		},
	)
	if !errors.Is(err, workErr) || !errors.Is(err, cleanupErr) ||
		!strings.Contains(err.Error(), "release strict source snapshot") {
		t.Fatalf("joined work/cleanup error = %v", err)
	}
	if second := execution.Close(context.Background()); !errors.Is(second, cleanupErr) {
		t.Fatalf("repeated cleanup error = %v", second)
	}
	if calls, _, _ := session.closeSnapshot(); calls != 1 {
		t.Fatalf("cleanup calls = %d", calls)
	}

	captureErr := errors.New("count failed")
	captureSession := &strictCoreFakeSession{
		captureErr: captureErr, closeErr: cleanupErr,
	}
	request.Tables = append(
		[]StrictConsistencyTable(nil),
		request.Tables...,
	)
	request.State = &strictCoreFakeState{
		tasks: []state.WorkTask{{RunID: "run-1", Key: table.Task}},
	}
	openErr := errors.New("open failed after allocating a session")
	openSession := &strictCoreFakeSession{closeErr: cleanupErr}
	_, err = BeginStrictConsistency(
		context.Background(),
		request,
		&strictCoreFakeOpener{session: openSession, openErr: openErr},
	)
	openCalls, _, _ := openSession.closeSnapshot()
	if !errors.Is(err, openErr) || !errors.Is(err, cleanupErr) ||
		openCalls != 1 {
		t.Fatalf(
			"partial-open/cleanup error=%v close calls=%d",
			err,
			openCalls,
		)
	}
	_, err = BeginStrictConsistency(
		context.Background(),
		request,
		&strictCoreFakeOpener{session: captureSession},
	)
	if !errors.Is(err, captureErr) || !errors.Is(err, cleanupErr) {
		t.Fatalf("capture/cleanup error = %v", err)
	}
}

func TestStrictConsistencyCleanupUsesFreshBoundedContext(t *testing.T) {
	at := strictCoreTime()
	table := strictCoreTables()[0]
	newRequest := func() StrictConsistencyRequest {
		return StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencySQLite,
			Scope: state.StrictSnapshotTable, ProcessEpoch: "process-1",
			State: &strictCoreFakeState{
				tasks: []state.WorkTask{{
					RunID: "run-1", Key: table.Task,
				}},
			},
			Tables: []StrictConsistencyTable{table},
		}
	}

	t.Run("cancelled-work-context", func(t *testing.T) {
		request := newRequest()
		session := &strictCoreFakeSession{
			capture:             strictCoreCapture(request, at),
			rejectCanceledClose: true,
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{session: session},
		)
		if err != nil {
			t.Fatal(err)
		}
		workContext, cancel := context.WithCancel(context.Background())
		cancel()
		if err := execution.Run(
			workContext,
			func(context.Context, StrictConsistencySession) error {
				return nil
			},
		); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled work context error: %v", err)
		}
		calls, sawCanceled, hadDeadline := session.closeSnapshot()
		if calls != 1 || sawCanceled || !hadDeadline {
			t.Fatalf(
				"cleanup context calls=%d cancelled=%v deadline=%v",
				calls,
				sawCanceled,
				hadDeadline,
			)
		}
	})

	t.Run("cancelled-capture-context", func(t *testing.T) {
		request := newRequest()
		captureContext, cancel := context.WithCancel(context.Background())
		session := &strictCoreFakeSession{
			capture:             strictCoreCapture(request, at),
			captureErr:          context.Canceled,
			captureHook:         cancel,
			rejectCanceledClose: true,
		}
		execution, err := BeginStrictConsistency(
			captureContext,
			request,
			&strictCoreFakeOpener{session: session},
		)
		if execution != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled capture execution=%#v error=%v", execution, err)
		}
		calls, sawCanceled, hadDeadline := session.closeSnapshot()
		if calls != 1 || sawCanceled || !hadDeadline {
			t.Fatalf(
				"capture cleanup context calls=%d cancelled=%v deadline=%v",
				calls,
				sawCanceled,
				hadDeadline,
			)
		}
	})

	t.Run("useful-caller-deadline-bounds-cleanup", func(t *testing.T) {
		request := newRequest()
		blockClose := make(chan struct{})
		session := &strictCoreFakeSession{
			capture:    strictCoreCapture(request, at),
			closeBlock: blockClose,
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{session: session},
		)
		if err != nil {
			t.Fatal(err)
		}
		cleanupContext, cancel := context.WithTimeout(
			context.Background(),
			200*time.Millisecond,
		)
		cancel()
		started := time.Now()
		first := execution.Close(cleanupContext)
		elapsed := time.Since(started)
		if !errors.Is(first, context.DeadlineExceeded) {
			t.Fatalf("bounded cleanup error = %v", first)
		}
		if elapsed > 2*time.Second {
			t.Fatalf("bounded cleanup took %s", elapsed)
		}
		close(blockClose)
		second := execution.Close(context.Background())
		if second != first {
			t.Fatalf(
				"cleanup error was not stably cached: first=%v second=%v",
				first,
				second,
			)
		}
		calls, sawCanceled, hadDeadline := session.closeSnapshot()
		if calls != 1 || sawCanceled || !hadDeadline {
			t.Fatalf(
				"bounded cleanup context calls=%d cancelled=%v deadline=%v",
				calls,
				sawCanceled,
				hadDeadline,
			)
		}
	})
}

func TestStrictConsistencyPartialPersistenceHasExplicitResumePaths(t *testing.T) {
	at := strictCoreTime()
	tables := strictCoreTables()
	work := []state.WorkTask{
		{RunID: "run-1", Key: tables[0].Task},
		{RunID: "run-1", Key: tables[1].Task},
	}
	persistErr := errors.New("second evidence write failed")

	t.Run("postgres-owner-only-requires-fresh-epoch", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks:          append([]state.WorkTask(nil), work[:1]...),
			saveEvidenceAt: 1, saveEvidenceErr: persistErr,
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "pg-owner-process-old",
			State:        backend, Tables: tables[:1],
		}
		firstSession := &strictCoreFakeSession{
			capture: strictCoreCapture(request, at),
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{session: firstSession},
		)
		if execution != nil || !errors.Is(err, persistErr) ||
			len(backend.owners) != 1 || len(backend.evidence) != 0 {
			t.Fatalf(
				"PG owner-only execution=%#v error=%v owners=%d evidence=%d",
				execution,
				err,
				len(backend.owners),
				len(backend.evidence),
			)
		}
		backend.latest = backend.owners[0]
		backend.latestFound = true
		backend.saveEvidenceAt = 0
		backend.saveEvidenceCalls = 0
		request.Resume = true
		sameEpochOpener := &strictCoreFakeOpener{}
		_, err = BeginStrictConsistency(
			context.Background(),
			request,
			sameEpochOpener,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "fresh process epoch") ||
			sameEpochOpener.calls != 0 ||
			len(backend.owners) != 1 {
			t.Fatalf(
				"same-epoch PG owner retry error=%v calls=%d owners=%d",
				err,
				sameEpochOpener.calls,
				len(backend.owners),
			)
		}
		request.ProcessEpoch = "pg-owner-process-new"
		capture := strictCoreCapture(request, at.Add(time.Hour))
		capture.MigrationEpochID = "pg-owner-epoch-new"
		capture.MigrationSnapshotReference = "pg-owner-snapshot-new"
		capture.Tables[0].SnapshotReference = "pg-owner-snapshot-new"
		execution, err = BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{
				session: &strictCoreFakeSession{capture: capture},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(backend.owners) != 2 || len(backend.evidence) != 1 {
			t.Fatalf(
				"fresh PG owner retry owners=%d evidence=%d",
				len(backend.owners),
				len(backend.evidence),
			)
		}
		if err := execution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("mssql-owner-only-reuses-owner", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks:          append([]state.WorkTask(nil), work[:1]...),
			saveEvidenceAt: 1, saveEvidenceErr: persistErr,
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "mssql-owner-process-old",
			State:        backend, Tables: tables[:1],
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{
				session: &strictCoreFakeSession{
					capture: strictCoreCapture(request, at),
				},
			},
		)
		if execution != nil || !errors.Is(err, persistErr) ||
			len(backend.owners) != 1 || len(backend.evidence) != 0 {
			t.Fatalf(
				"MSSQL owner-only execution=%#v error=%v owners=%d evidence=%d",
				execution,
				err,
				len(backend.owners),
				len(backend.evidence),
			)
		}
		backend.latest = backend.owners[0]
		backend.latestFound = true
		backend.saveEvidenceAt = 0
		backend.saveEvidenceCalls = 0
		request.Resume = true
		request.ProcessEpoch = "mssql-owner-process-new"
		capture := strictCoreCapture(request, at.Add(time.Hour))
		capture.MigrationEpochID = backend.latest.EpochID
		capture.MigrationSnapshotReference =
			backend.latest.SnapshotReference
		capture.Tables[0].SnapshotReference =
			backend.latest.SnapshotReference
		opener := &strictCoreFakeOpener{
			session: &strictCoreFakeSession{capture: capture},
		}
		execution, err = BeginStrictConsistency(
			context.Background(),
			request,
			opener,
		)
		if err != nil {
			t.Fatal(err)
		}
		if opener.request.RequiredMigrationSnapshot == nil ||
			*opener.request.RequiredMigrationSnapshot != backend.latest ||
			len(backend.owners) != 1 ||
			len(backend.evidence) != 1 {
			t.Fatalf(
				"MSSQL owner retry request=%#v owners=%d evidence=%d",
				opener.request.RequiredMigrationSnapshot,
				len(backend.owners),
				len(backend.evidence),
			)
		}
		if err := execution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("mssql-reuses-surviving-owner", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks: append([]state.WorkTask(nil), work...), saveEvidenceAt: 2,
			saveEvidenceErr: persistErr,
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "process-original",
			State:        backend, Tables: tables,
		}
		firstSession := &strictCoreFakeSession{
			capture: strictCoreCapture(request, at),
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{session: firstSession},
		)
		if execution != nil || !errors.Is(err, persistErr) {
			t.Fatalf("partial MSSQL execution=%#v error=%v", execution, err)
		}
		firstCloseCalls, _, _ := firstSession.closeSnapshot()
		if firstCloseCalls != 1 ||
			len(backend.owners) != 1 ||
			len(backend.evidence) != 1 {
			t.Fatalf(
				"partial MSSQL state owners=%d evidence=%d closes=%d",
				len(backend.owners),
				len(backend.evidence),
				firstCloseCalls,
			)
		}
		prior := backend.evidence[0]
		backend.latest = backend.owners[0]
		backend.latestFound = true
		backend.saveEvidenceAt = 0
		backend.saveEvidenceCalls = 0
		request.Resume = true
		request.ProcessEpoch = "process-resume"
		resumeCapture := strictCoreCapture(request, at.Add(time.Hour))
		resumeCapture.MigrationEpochID = backend.latest.EpochID
		resumeCapture.MigrationSnapshotReference =
			backend.latest.SnapshotReference
		for index := range resumeCapture.Tables {
			resumeCapture.Tables[index].SnapshotReference =
				backend.latest.SnapshotReference
		}
		resumeOpener := &strictCoreFakeOpener{
			session: &strictCoreFakeSession{capture: resumeCapture},
		}
		execution, err = BeginStrictConsistency(
			context.Background(),
			request,
			resumeOpener,
		)
		if err != nil {
			t.Fatal(err)
		}
		if resumeOpener.request.RequiredMigrationSnapshot == nil ||
			*resumeOpener.request.RequiredMigrationSnapshot != backend.latest {
			t.Fatalf(
				"MSSQL partial retry owner = %#v",
				resumeOpener.request.RequiredMigrationSnapshot,
			)
		}
		got := execution.Evidence()
		if len(got) != 2 || got[0] != prior ||
			got[1].SnapshotReference != backend.latest.SnapshotReference {
			t.Fatalf("MSSQL partial retry evidence = %#v", got)
		}
		if len(backend.owners) != 1 || len(backend.evidence) != 2 {
			t.Fatalf(
				"MSSQL retry duplicated structural state owners=%d evidence=%d",
				len(backend.owners),
				len(backend.evidence),
			)
		}
		if err := execution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("postgres-requires-new-attempt-and-epoch", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks: append([]state.WorkTask(nil), work...), saveEvidenceAt: 2,
			saveEvidenceErr: persistErr,
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "pg-process-old",
			State:        backend, Tables: tables,
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{
				session: &strictCoreFakeSession{
					capture: strictCoreCapture(request, at),
				},
			},
		)
		if execution != nil || !errors.Is(err, persistErr) {
			t.Fatalf("partial PG execution=%#v error=%v", execution, err)
		}
		backend.latest = backend.owners[0]
		backend.latestFound = true
		backend.saveEvidenceAt = 0
		backend.saveEvidenceCalls = 0
		request.Resume = true
		request.ProcessEpoch = "pg-process-new"
		sameAttemptOpener := &strictCoreFakeOpener{}
		_, err = BeginStrictConsistency(
			context.Background(),
			request,
			sameAttemptOpener,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "new strict attempt ID") ||
			sameAttemptOpener.calls != 0 {
			t.Fatalf(
				"same-attempt PG resume error=%v opener calls=%d",
				err,
				sameAttemptOpener.calls,
			)
		}
		request.Tables = append(
			[]StrictConsistencyTable(nil),
			request.Tables...,
		)
		for index := range request.Tables {
			request.Tables[index].DurableWorkAttempts = 1
			request.Tables[index].AttemptID = strictCoreAttemptID(
				request.Tables[index],
			)
			backend.tasks[index].Attempts = 1
		}
		capture := strictCoreCapture(request, at.Add(time.Hour))
		capture.MigrationEpochID = "pg-epoch-new"
		capture.MigrationSnapshotReference = "pg-snapshot-new"
		for index := range capture.Tables {
			capture.Tables[index].SnapshotReference = "pg-snapshot-new"
		}
		execution, err = BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{
				session: &strictCoreFakeSession{capture: capture},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		owner, found := execution.MigrationSnapshot()
		if !found || owner.ProcessEpoch != "pg-process-new" ||
			owner.EpochID != "pg-epoch-new" ||
			len(backend.owners) != 2 {
			t.Fatalf(
				"fresh PG resume owner=%#v found=%v all=%#v",
				owner,
				found,
				backend.owners,
			)
		}
		if err := execution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("table-scope-retry-uses-new-attempt", func(t *testing.T) {
		backend := &strictCoreFakeState{
			tasks: append([]state.WorkTask(nil), work...), saveEvidenceAt: 2,
			saveEvidenceErr: persistErr,
		}
		request := StrictConsistencyRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "table-process-old",
			State:        backend, Tables: tables,
		}
		execution, err := BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{
				session: &strictCoreFakeSession{
					capture: strictCoreCapture(request, at),
				},
			},
		)
		if execution != nil || !errors.Is(err, persistErr) {
			t.Fatalf("partial table execution=%#v error=%v", execution, err)
		}
		backend.saveEvidenceAt = 0
		backend.saveEvidenceCalls = 0
		request.Resume = true
		request.ProcessEpoch = "table-process-new"
		blockedOpener := &strictCoreFakeOpener{}
		_, err = BeginStrictConsistency(
			context.Background(),
			request,
			blockedOpener,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "prior stable view is not reusable") ||
			blockedOpener.calls != 0 {
			t.Fatalf(
				"same table attempt retry error=%v opener calls=%d",
				err,
				blockedOpener.calls,
			)
		}
		request.Tables = append(
			[]StrictConsistencyTable(nil),
			request.Tables...,
		)
		for index := range request.Tables {
			request.Tables[index].DurableWorkAttempts = 1
			request.Tables[index].AttemptID = strictCoreAttemptID(
				request.Tables[index],
			)
			backend.tasks[index].Attempts = 1
		}
		capture := strictCoreCapture(request, at.Add(time.Hour))
		capture.Tables[0].SnapshotReference = "table-new-view-1"
		capture.Tables[1].SnapshotReference = "table-new-view-2"
		execution, err = BeginStrictConsistency(
			context.Background(),
			request,
			&strictCoreFakeOpener{
				session: &strictCoreFakeSession{capture: capture},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(execution.Evidence()) != 2 ||
			len(backend.evidence) != 3 {
			t.Fatalf(
				"fresh table retry evidence execution=%#v durable=%#v",
				execution.Evidence(),
				backend.evidence,
			)
		}
		if err := execution.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStrictConsistencyDurableBackendConformance(t *testing.T) {
	type durableStrictBackend interface {
		state.Backend
		StrictConsistencyState
	}
	factories := []struct {
		name string
		new  func(*testing.T) durableStrictBackend
	}{
		{
			name: "yaml",
			new: func(t *testing.T) durableStrictBackend {
				return state.YAMLStore{
					Path: filepath.Join(t.TempDir(), "strict.yaml"),
				}
			},
		},
		{
			name: "sqlite",
			new: func(t *testing.T) durableStrictBackend {
				return state.SQLiteStore{
					Path: filepath.Join(t.TempDir(), "strict.db"),
				}
			},
		},
	}
	for _, factory := range factories {
		t.Run(factory.name, func(t *testing.T) {
			for _, test := range []struct {
				name   string
				engine StrictConsistencyEngine
				scope  state.StrictSnapshotScope
			}{
				{
					name: "table", engine: StrictConsistencySQLite,
					scope: state.StrictSnapshotTable,
				},
				{
					name: "migration", engine: StrictConsistencyPostgres,
					scope: state.StrictSnapshotMigration,
				},
			} {
				t.Run(test.name, func(t *testing.T) {
					backend := factory.new(t)
					at := strictCoreTime()
					table := strictCoreTables()[0]
					runID := "durable-" + test.name
					sourceEngine := strictConsistencyStateEngine(test.engine)
					if err := backend.InitializeRun(state.Run{
						ID: runID, Source: "source", Target: "target",
						SourceEngine:   sourceEngine,
						SourceIdentity: sourceEngine + ":source.example/database",
						TargetIdentity: "postgres:target.example/database",
						Outcome:        state.Running, Resumable: true,
						Reason: "running", StartedAt: at,
					}, "config-hash"); err != nil {
						t.Fatal(err)
					}
					if created, err := backend.EnsureWorkPlan(
						state.WorkTask{
							RunID: runID, Key: table.Task,
							Strategy:     "strict-full-table",
							TopologyHash: table.WorkTopologyHash,
							StartedAt:    at,
						},
						[]state.RangeState{{ID: "0"}},
					); err != nil || !created {
						t.Fatalf("ensure work plan created=%v err=%v", created, err)
					}
					request := StrictConsistencyRequest{
						RunID: runID, SourceEngine: test.engine,
						Scope: test.scope, ProcessEpoch: "process-1",
						State:  backend,
						Tables: []StrictConsistencyTable{table},
					}
					session := &strictCoreFakeSession{
						capture: strictCoreCapture(request, at.Add(time.Minute)),
					}
					execution, err := BeginStrictConsistency(
						context.Background(),
						request,
						&strictCoreFakeOpener{session: session},
					)
					if err != nil {
						t.Fatal(err)
					}
					want := execution.Evidence()[0]
					got, found, err := backend.LoadStrictSnapshotEvidence(
						runID,
						table.Task,
						table.AttemptID,
					)
					if err != nil || !found || got != want {
						t.Fatalf(
							"durable strict evidence = %#v found=%v err=%v, want %#v",
							got,
							found,
							err,
							want,
						)
					}
					if test.scope == state.StrictSnapshotMigration {
						owner, found, err := backend.
							LoadLatestStrictMigrationSnapshot(runID)
						if err != nil || !found ||
							owner.EpochID != want.MigrationEpochID ||
							owner.SnapshotReference != want.SnapshotReference {
							t.Fatalf(
								"durable migration owner = %#v found=%v err=%v",
								owner,
								found,
								err,
							)
						}
					}
					if err := execution.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
				})
			}
		})
	}
}

func TestStrictConsistencyConcurrentCoordinatorsHaveNoSharedState(t *testing.T) {
	const workers = 32
	var group sync.WaitGroup
	errs := make(chan error, workers)
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			table := strictCoreTables()[index%2]
			runID := fmt.Sprintf("run-%d", index)
			backend := &strictCoreFakeState{
				tasks: []state.WorkTask{{RunID: runID, Key: table.Task}},
			}
			request := StrictConsistencyRequest{
				RunID: runID, SourceEngine: StrictConsistencySQLite,
				Scope:        state.StrictSnapshotTable,
				ProcessEpoch: fmt.Sprintf("process-%d", index),
				State:        backend, Tables: []StrictConsistencyTable{table},
			}
			session := &strictCoreFakeSession{
				capture: strictCoreCapture(
					request,
					strictCoreTime().Add(time.Duration(index)*time.Second),
				),
			}
			execution, err := BeginStrictConsistency(
				context.Background(),
				request,
				&strictCoreFakeOpener{session: session},
			)
			if err == nil {
				err = execution.Run(
					context.Background(),
					func(context.Context, StrictConsistencySession) error {
						return nil
					},
				)
			}
			errs <- err
		}(index)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type strictCoreFakeState struct {
	state.Stage4Backend
	state.RangeBackend

	tasks   []state.WorkTask
	listErr error

	latest      state.StrictMigrationSnapshot
	latestFound bool
	latestErr   error

	saveOwnerErr      error
	saveEvidenceAt    int
	saveEvidenceErr   error
	saveEvidenceCalls int
	saveEvidenceHook  func(int)
	listCalls         int
	listHook          func(int)

	owners   []state.StrictMigrationSnapshot
	evidence []state.StrictSnapshotEvidence
	events   *[]string
}

func (backend *strictCoreFakeState) ListWork(
	string,
) ([]state.WorkTask, []state.RangeState, error) {
	backend.event("list-work")
	backend.listCalls++
	if backend.listHook != nil {
		backend.listHook(backend.listCalls)
	}
	tasks := append([]state.WorkTask(nil), backend.tasks...)
	for index := range tasks {
		if tasks[index].Status == "" {
			tasks[index].Status = "running"
		}
		if tasks[index].TopologyHash == "" {
			tasks[index].TopologyHash = strictCoreTopology(tasks[index].Key)
		}
	}
	return tasks, nil, backend.listErr
}

func (backend *strictCoreFakeState) LoadLatestStrictMigrationSnapshot(
	string,
) (state.StrictMigrationSnapshot, bool, error) {
	backend.event("load-owner")
	return backend.latest, backend.latestFound, backend.latestErr
}

func (backend *strictCoreFakeState) SaveStrictMigrationSnapshot(
	snapshot state.StrictMigrationSnapshot,
) error {
	backend.event("save-owner")
	if backend.saveOwnerErr != nil {
		return backend.saveOwnerErr
	}
	for _, existing := range backend.owners {
		if existing.RunID == snapshot.RunID &&
			existing.EpochID == snapshot.EpochID {
			if existing != snapshot {
				return state.ErrImmutableEvidence
			}
			return nil
		}
	}
	backend.owners = append(backend.owners, snapshot)
	return nil
}

func (backend *strictCoreFakeState) SaveStrictSnapshotEvidence(
	evidence state.StrictSnapshotEvidence,
) error {
	backend.event("save-evidence:" + evidence.Task.Schema + "/" + evidence.Task.Table)
	backend.saveEvidenceCalls++
	if backend.saveEvidenceAt == backend.saveEvidenceCalls {
		return backend.saveEvidenceErr
	}
	for _, existing := range backend.evidence {
		if existing.RunID == evidence.RunID &&
			existing.Task == evidence.Task &&
			existing.AttemptID == evidence.AttemptID {
			if existing != evidence {
				return state.ErrImmutableEvidence
			}
			return nil
		}
	}
	backend.evidence = append(backend.evidence, evidence)
	if backend.saveEvidenceHook != nil {
		backend.saveEvidenceHook(backend.saveEvidenceCalls)
	}
	return nil
}

func (backend *strictCoreFakeState) LoadStrictSnapshotEvidence(
	runID string,
	task state.TaskKey,
	attemptID string,
) (state.StrictSnapshotEvidence, bool, error) {
	backend.event("load-evidence:" + task.Schema + "/" + task.Table)
	for _, record := range backend.evidence {
		if record.RunID == runID &&
			record.Task == task &&
			record.AttemptID == attemptID {
			return record, true, nil
		}
	}
	return state.StrictSnapshotEvidence{}, false, nil
}

func (backend *strictCoreFakeState) event(event string) {
	if backend.events != nil {
		*backend.events = append(*backend.events, event)
	}
}

type strictCoreFakeOpener struct {
	session StrictConsistencySession
	openErr error
	calls   int
	request StrictConsistencyOpenRequest
	events  *[]string
}

func (opener *strictCoreFakeOpener) OpenStrictConsistency(
	_ context.Context,
	request StrictConsistencyOpenRequest,
) (StrictConsistencySession, error) {
	opener.calls++
	opener.request = request
	if opener.events != nil {
		*opener.events = append(*opener.events, "open")
	}
	return opener.session, opener.openErr
}

type strictCoreFakeSession struct {
	capture             StrictConsistencyCapture
	captureErr          error
	captureHook         func()
	closeErr            error
	closeMu             sync.Mutex
	closeCalls          int
	closeSawCanceled    bool
	closeHadDeadline    bool
	closeBlock          <-chan struct{}
	rejectCanceledClose bool
	events              *[]string
}

func (session *strictCoreFakeSession) CaptureSameViewEvidence(
	context.Context,
) (StrictConsistencyCapture, error) {
	if session.events != nil {
		*session.events = append(*session.events, "capture")
	}
	if session.captureHook != nil {
		session.captureHook()
	}
	return session.capture, session.captureErr
}

func (session *strictCoreFakeSession) Close(ctx context.Context) error {
	session.closeMu.Lock()
	session.closeCalls++
	session.closeSawCanceled = ctx.Err() != nil
	_, session.closeHadDeadline = ctx.Deadline()
	session.closeMu.Unlock()
	if session.events != nil {
		*session.events = append(*session.events, "close")
	}
	if session.rejectCanceledClose && ctx.Err() != nil {
		return errors.New("fake session refused canceled cleanup")
	}
	if session.closeBlock != nil {
		<-session.closeBlock
		return session.closeErr
	}
	return session.closeErr
}

func (session *strictCoreFakeSession) closeSnapshot() (int, bool, bool) {
	session.closeMu.Lock()
	defer session.closeMu.Unlock()
	return session.closeCalls, session.closeSawCanceled, session.closeHadDeadline
}

func strictCoreTables() []StrictConsistencyTable {
	tables := []StrictConsistencyTable{
		{
			Task: state.TaskKey{
				Type: "table:copy", Schema: "a.b",
				Table: "c:d", Partition: `p/1\2`,
			},
			WorkTopologyHash: "topology-c-d",
		},
		{
			Task: state.TaskKey{
				Type: "table:copy", Schema: "z.schema",
				Table: "items", Partition: "p:2",
			},
			WorkTopologyHash: "topology-items",
		},
	}
	for index := range tables {
		tables[index].AttemptID = strictCoreAttemptID(tables[index])
	}
	return tables
}

func strictCoreAttemptID(table StrictConsistencyTable) string {
	attemptID, err := BuildStrictConsistencyAttemptID(
		table.Task,
		table.WorkTopologyHash,
		table.DurableWorkAttempts,
	)
	if err != nil {
		panic(err)
	}
	return attemptID
}

func strictCoreTopology(task state.TaskKey) string {
	return "topology-" + strings.NewReplacer(
		":", "-",
		"/", "-",
		`\\`, "-",
		".", "-",
	).Replace(task.Table)
}

func strictCoreTime() time.Time {
	return time.Date(2026, 7, 30, 18, 0, 0, 0, time.UTC)
}

func strictCoreCapture(
	request StrictConsistencyRequest,
	at time.Time,
) StrictConsistencyCapture {
	reference := ""
	capture := StrictConsistencyCapture{}
	if request.Scope == state.StrictSnapshotMigration {
		reference = "migration-snapshot-1"
		capture.MigrationEpochID = "migration-epoch-1"
		capture.MigrationSnapshotReference = reference
		capture.MigrationCapturedAt = at
	}
	for index, table := range request.Tables {
		tableReference := reference
		if request.Scope == state.StrictSnapshotTable {
			tableReference = fmt.Sprintf("table-snapshot-%d", index+1)
		}
		capture.Tables = append(
			capture.Tables,
			StrictConsistencyTableCapture{
				Task: table.Task, AttemptID: table.AttemptID,
				SnapshotReference:   tableReference,
				ExactSourceRowCount: int64(index + 10),
				CapturedAt:          at.Add(time.Duration(index) * time.Second),
			},
		)
	}
	return capture
}
