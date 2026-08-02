package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func TestPostgresStrictTableSnapshotsAndParallelReaderPrimitive(
	t *testing.T,
) {
	t.Parallel()
	tables := postgresStrictTestTables()
	transactions := make([]*postgresStrictFakeTransaction, 0, 4)
	var transactionMu sync.Mutex
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			transactionMu.Lock()
			defer transactionMu.Unlock()
			transaction := &postgresStrictFakeTransaction{
				id: len(transactions),
			}
			transactions = append(transactions, transaction)
			return transaction, nil
		},
	)
	references := []string{
		"00000003-0000000A-1",
		"00000003-0000000B-1",
	}
	opener.exportSnapshot = func(
		_ context.Context,
		transaction postgresStrictTransaction,
	) (string, time.Time, error) {
		fake := transaction.(*postgresStrictFakeTransaction)
		return references[fake.id],
			time.Date(2026, 7, 30, 10, fake.id, 0, 0, time.FixedZone("test", 3600)),
			nil
	}
	opener.countTable = func(
		_ context.Context,
		_ postgresStrictTransaction,
		task state.TaskKey,
	) (int64, error) {
		if task == tables[0].Task {
			return 11, nil
		}
		return 22, nil
	}
	var imported []string
	var importMu sync.Mutex
	opener.importSnapshot = func(
		_ context.Context,
		_ postgresStrictTransaction,
		reference string,
	) error {
		importMu.Lock()
		imported = append(imported, reference)
		importMu.Unlock()
		return nil
	}

	rawSession, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-table",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-table",
			Tables: []StrictConsistencyTable{
				tables[1],
				tables[0],
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := rawSession.(*PostgresStrictConsistencySession)
	capture, err := session.CaptureSameViewEvidence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if capture.MigrationEpochID != "" ||
		capture.MigrationSnapshotReference != "" ||
		!capture.MigrationCapturedAt.IsZero() {
		t.Fatalf("table capture claimed migration evidence: %#v", capture)
	}
	if len(capture.Tables) != 2 ||
		capture.Tables[0].Task != tables[0].Task ||
		capture.Tables[1].Task != tables[1].Task ||
		capture.Tables[0].ExactSourceRowCount != 11 ||
		capture.Tables[1].ExactSourceRowCount != 22 ||
		capture.Tables[0].SnapshotReference ==
			capture.Tables[1].SnapshotReference {
		t.Fatalf("table capture = %#v", capture)
	}
	if capture.Tables[0].CapturedAt.Location() != time.UTC {
		t.Fatalf(
			"capture time location = %v, want UTC",
			capture.Tables[0].CapturedAt.Location(),
		)
	}
	capture.Tables[0].SnapshotReference = "caller-mutation"
	fresh, err := session.CaptureSameViewEvidence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Tables[0].SnapshotReference == "caller-mutation" {
		t.Fatal("capture returned aliased table evidence")
	}

	var group sync.WaitGroup
	errs := make(chan error, 2)
	for _, selected := range tables {
		selected := selected
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- session.RunReader(
				context.Background(),
				selected.Task,
				func(
					_ context.Context,
					queryer PostgresStrictSnapshotQueryer,
				) error {
					if _, ok := queryer.(*postgresStrictFakeTransaction); !ok {
						t.Errorf("reader queryer type = %T", queryer)
					}
					return nil
				},
			)
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(imported) != 2 {
		t.Fatalf("import calls = %#v", imported)
	}
	for _, reference := range imported {
		if reference != references[0] && reference != references[1] {
			t.Fatalf("imported unknown snapshot %q", reference)
		}
	}
	if len(transactions) != 4 ||
		transactions[2].rollbackCalls != 1 ||
		transactions[3].rollbackCalls != 1 {
		t.Fatalf("reader transaction cleanup = %#v", transactions)
	}
	if err := session.RunReader(
		context.Background(),
		state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: "public",
			Table:  "unselected",
		},
		func(context.Context, PostgresStrictSnapshotQueryer) error {
			return nil
		},
	); err == nil || !strings.Contains(err.Error(), "not selected") {
		t.Fatalf("unselected reader error = %v", err)
	}

	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transactions[0].rollbackCalls != 1 ||
		transactions[1].rollbackCalls != 1 {
		t.Fatalf("snapshot owner cleanup = %#v", transactions[:2])
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transactions[0].rollbackCalls != 1 ||
		transactions[1].rollbackCalls != 1 {
		t.Fatal("repeated Close rolled owners back again")
	}
	if _, err := session.CaptureSameViewEvidence(
		context.Background(),
	); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("capture after close error = %v", err)
	}
}

func TestPostgresStrictMigrationSnapshotUsesOneOwnerAndFreshEpoch(
	t *testing.T,
) {
	t.Parallel()
	tables := postgresStrictTestTables()
	var transactions []*postgresStrictFakeTransaction
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			transaction := &postgresStrictFakeTransaction{
				id: len(transactions),
			}
			transactions = append(transactions, transaction)
			return transaction, nil
		},
	)
	const reference = "00000004-0000000C-2"
	capturedAt := time.Date(
		2026,
		7,
		30,
		11,
		0,
		0,
		0,
		time.UTC,
	)
	opener.exportSnapshot = func(
		context.Context,
		postgresStrictTransaction,
	) (string, time.Time, error) {
		return reference, capturedAt, nil
	}
	opener.countTable = func(
		_ context.Context,
		_ postgresStrictTransaction,
		task state.TaskKey,
	) (int64, error) {
		if task == tables[0].Task {
			return 7, nil
		}
		return 9, nil
	}

	rawSession, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-migration",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotMigration,
			Resume:       true,
			ProcessEpoch: "process-new",
			Tables:       tables,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := rawSession.(*PostgresStrictConsistencySession)
	capture, err := session.CaptureSameViewEvidence(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(transactions) != 1 {
		t.Fatalf("migration owner transactions = %d, want 1", len(transactions))
	}
	if capture.MigrationSnapshotReference != reference ||
		capture.MigrationEpochID !=
			postgresStrictEpochID("process-new", reference) ||
		!capture.MigrationCapturedAt.Equal(capturedAt) {
		t.Fatalf("migration capture = %#v", capture)
	}
	for _, table := range capture.Tables {
		if table.SnapshotReference != reference ||
			!table.CapturedAt.Equal(capturedAt) {
			t.Fatalf("table did not share migration snapshot: %#v", table)
		}
	}
	if postgresStrictEpochID("process-old", reference) ==
		capture.MigrationEpochID {
		t.Fatal("migration epoch did not bind the fresh process epoch")
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStrictOpenerComposesWithCore(t *testing.T) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	var transactions []*postgresStrictFakeTransaction
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			transaction := &postgresStrictFakeTransaction{
				id: len(transactions),
			}
			transactions = append(transactions, transaction)
			return transaction, nil
		},
	)
	const reference = "00000007-00000010-1"
	capturedAt := time.Date(
		2026,
		7,
		30,
		13,
		0,
		0,
		0,
		time.UTC,
	)
	opener.exportSnapshot = func(
		context.Context,
		postgresStrictTransaction,
	) (string, time.Time, error) {
		return reference, capturedAt, nil
	}
	opener.countTable = func(
		context.Context,
		postgresStrictTransaction,
		state.TaskKey,
	) (int64, error) {
		return 41, nil
	}
	backend := &strictCoreFakeState{
		tasks: []state.WorkTask{{
			RunID:        "run-core",
			Key:          table.Task,
			Status:       "running",
			TopologyHash: table.WorkTopologyHash,
			Attempts:     table.DurableWorkAttempts,
		}},
	}
	execution, err := BeginStrictConsistency(
		context.Background(),
		StrictConsistencyRequest{
			RunID:        "run-core",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotMigration,
			ProcessEpoch: "process-core",
			State:        backend,
			Tables:       []StrictConsistencyTable{table},
		},
		opener,
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence := execution.Evidence()
	if len(evidence) != 1 ||
		evidence[0].ExactSourceRowCount != 41 ||
		evidence[0].SnapshotReference != reference ||
		evidence[0].MigrationEpochID !=
			postgresStrictEpochID("process-core", reference) ||
		len(backend.evidence) != 1 ||
		len(backend.owners) != 1 {
		t.Fatalf(
			"core PostgreSQL evidence=%#v durable=%#v owners=%#v",
			evidence,
			backend.evidence,
			backend.owners,
		)
	}
	err = execution.Run(
		context.Background(),
		func(
			ctx context.Context,
			raw StrictConsistencySession,
		) error {
			session, ok := raw.(*PostgresStrictConsistencySession)
			if !ok {
				return fmt.Errorf(
					"strict session type = %T",
					raw,
				)
			}
			return session.RunReader(
				ctx,
				table.Task,
				func(
					context.Context,
					PostgresStrictSnapshotQueryer,
				) error {
					return nil
				},
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transactions) != 2 ||
		transactions[0].rollbackCalls != 1 ||
		transactions[1].rollbackCalls != 1 {
		t.Fatalf("core cleanup transactions = %#v", transactions)
	}
}

func TestPostgresStrictOpenFailureReleasesEverySnapshotOwner(
	t *testing.T,
) {
	t.Parallel()
	tables := postgresStrictTestTables()
	transactions := make([]*postgresStrictFakeTransaction, 0, 2)
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			transaction := &postgresStrictFakeTransaction{
				id: len(transactions),
			}
			if transaction.id == 0 {
				transaction.rollbackErr = errors.New("rollback sentinel")
			}
			transactions = append(transactions, transaction)
			return transaction, nil
		},
	)
	opener.exportSnapshot = func(
		_ context.Context,
		transaction postgresStrictTransaction,
	) (string, time.Time, error) {
		id := transaction.(*postgresStrictFakeTransaction).id
		return []string{
				"00000005-0000000D-1",
				"00000005-0000000E-1",
			}[id],
			time.Date(2026, 7, 30, 12, id, 0, 0, time.UTC),
			nil
	}
	opener.countTable = func(
		_ context.Context,
		_ postgresStrictTransaction,
		task state.TaskKey,
	) (int64, error) {
		if task == tables[1].Task {
			return 0, errors.New("count sentinel")
		}
		return 1, nil
	}

	session, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-failure",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-failure",
			Tables:       tables,
		},
	)
	if session != nil ||
		err == nil ||
		!strings.Contains(err.Error(), "count sentinel") ||
		!strings.Contains(err.Error(), "rollback sentinel") {
		t.Fatalf("open failure session=%#v error=%v", session, err)
	}
	if len(transactions) != 2 ||
		transactions[0].rollbackCalls != 1 ||
		transactions[1].rollbackCalls != 1 {
		t.Fatalf("open failure cleanup = %#v", transactions)
	}
}

func TestPostgresStrictCloseFailureRemainsOperationallyVisible(
	t *testing.T,
) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	transaction := &postgresStrictFakeTransaction{
		rollbackErr: errors.New("close sentinel"),
	}
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			return transaction, nil
		},
	)
	raw, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-close-error",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-close-error",
			Tables:       []StrictConsistencyTable{table},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*PostgresStrictConsistencySession)
	// The nil context is the subject of this assertion: Close must refuse it.
	// Static analysis flags nil contexts generically, so do not "fix" this by
	// passing context.TODO — that would delete the coverage.
	if nilContext := session.Close(nil); nilContext == nil ||
		!strings.Contains(nilContext.Error(), "cleanup context is required") {
		t.Fatalf("nil cleanup context error = %v", nilContext)
	}
	if transaction.rollbackCalls != 0 {
		t.Fatal("nil cleanup context consumed the one cleanup attempt")
	}
	first := session.Close(context.Background())
	second := session.Close(context.Background())
	if first == nil ||
		!strings.Contains(first.Error(), "close sentinel") ||
		second == nil ||
		second.Error() != first.Error() {
		t.Fatalf(
			"stable cleanup errors first=%v second=%v",
			first,
			second,
		)
	}
	if transaction.rollbackCalls != 1 {
		t.Fatalf(
			"cleanup failure rollback calls = %d",
			transaction.rollbackCalls,
		)
	}
}

func TestPostgresStrictCloseHonorsDeadlineWhenRollbackBlocks(
	t *testing.T,
) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	transaction := newPostgresStrictBlockingTransaction()
	cleanupErr := errors.New("blocked rollback sentinel")
	transaction.rollbackErr = cleanupErr
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			return transaction, nil
		},
	)
	raw, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-close-deadline",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-close-deadline",
			Tables:       []StrictConsistencyTable{table},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*PostgresStrictConsistencySession)

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- session.Close(context.Background())
	}()
	select {
	case <-transaction.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background cleanup did not start rollback")
	}

	deadline, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()
	startedAt := time.Now()
	second := session.Close(deadline)
	elapsed := time.Since(startedAt)
	if !errors.Is(second, context.DeadlineExceeded) ||
		!strings.Contains(second.Error(), "rollback may still be in progress") {
		t.Fatalf("concurrent deadline cleanup error = %v", second)
	}
	if elapsed > time.Second {
		t.Fatalf("concurrent deadline cleanup took %s", elapsed)
	}
	if calls := transaction.rollbackCallCount(); calls != 1 {
		t.Fatalf("deadline cleanup rollback calls = %d", calls)
	}

	close(transaction.release)
	select {
	case <-transaction.finished:
	case <-time.After(2 * time.Second):
		t.Fatal("background rollback did not finish after release")
	}
	first := <-firstResult
	if !errors.Is(first, cleanupErr) {
		t.Fatalf("eventual cleanup error = %v", first)
	}
	third := session.Close(context.Background())
	if !errors.Is(third, cleanupErr) ||
		third.Error() != first.Error() {
		t.Fatalf(
			"eventual cleanup result was not stable: first=%v third=%v",
			first,
			third,
		)
	}
	if calls := transaction.rollbackCallCount(); calls != 1 {
		t.Fatalf("completed cleanup rollback calls = %d", calls)
	}
}

func TestPostgresStrictCloseDeadlineDoesNotWaitForReaderSetupIO(
	t *testing.T,
) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	owner := &postgresStrictFakeTransaction{id: 0}
	reader := newPostgresStrictBlockingTransaction()
	reader.id = 1
	var (
		beginCalls int
		orderMu    sync.Mutex
		order      []string
	)
	owner.rollbackHook = func() {
		orderMu.Lock()
		order = append(order, "owner")
		orderMu.Unlock()
	}
	reader.rollbackHook = func() {
		orderMu.Lock()
		order = append(order, "reader")
		orderMu.Unlock()
	}
	importStarted := make(chan struct{})
	var importStartedOnce sync.Once
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			beginCalls++
			if beginCalls == 1 {
				return owner, nil
			}
			if beginCalls == 2 {
				return reader, nil
			}
			return nil, errors.New("unexpected extra reader transaction")
		},
	)
	opener.importSnapshot = func(
		context.Context,
		postgresStrictTransaction,
		string,
	) error {
		importStartedOnce.Do(func() {
			close(importStarted)
		})
		<-reader.release
		return nil
	}
	raw, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-close-reader-setup",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-close-reader-setup",
			Tables:       []StrictConsistencyTable{table},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*PostgresStrictConsistencySession)
	callbackCalled := make(chan struct{}, 1)
	readerResult := make(chan error, 1)
	go func() {
		readerResult <- session.RunReader(
			context.Background(),
			table.Task,
			func(context.Context, PostgresStrictSnapshotQueryer) error {
				callbackCalled <- struct{}{}
				return nil
			},
		)
	}()
	select {
	case <-importStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("reader snapshot import did not block")
	}

	deadline, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()
	closeResult := make(chan error, 1)
	startedAt := time.Now()
	go func() {
		closeResult <- session.Close(deadline)
	}()
	var closeErr error
	blockedOnReaderIO := false
	select {
	case closeErr = <-closeResult:
	case <-time.After(time.Second):
		blockedOnReaderIO = true
		close(reader.release)
		closeErr = <-closeResult
	}
	elapsed := time.Since(startedAt)
	if !blockedOnReaderIO {
		close(reader.release)
	}

	if finalErr := session.Close(context.Background()); finalErr != nil {
		t.Fatalf("eventual reader setup cleanup error = %v", finalErr)
	}
	runErr := <-readerResult
	if blockedOnReaderIO {
		t.Fatalf(
			"Close blocked %s acquiring the session lock during reader I/O",
			elapsed,
		)
	}
	if !errors.Is(closeErr, context.DeadlineExceeded) ||
		elapsed > time.Second {
		t.Fatalf(
			"reader setup deadline cleanup error=%v elapsed=%s",
			closeErr,
			elapsed,
		)
	}
	if runErr == nil ||
		!strings.Contains(runErr.Error(), "closed during reader setup") {
		t.Fatalf("reader setup close error = %v", runErr)
	}
	select {
	case <-callbackCalled:
		t.Fatal("reader callback ran after session cleanup started")
	default:
	}
	if owner.rollbackCalls != 1 ||
		reader.rollbackCallCount() != 1 {
		t.Fatalf(
			"reader/owner cleanup calls reader=%d owner=%d",
			reader.rollbackCallCount(),
			owner.rollbackCalls,
		)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if !reflect.DeepEqual(order, []string{"reader", "owner"}) {
		t.Fatalf("reader/owner cleanup order = %v", order)
	}
}

func TestPostgresStrictCloseWaitsForAdmittedReaderBeforeRollback(
	t *testing.T,
) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	owner := &postgresStrictFakeTransaction{id: 0}
	reader := &postgresStrictFakeTransaction{id: 1}
	var (
		beginCalls int
		orderMu    sync.Mutex
		order      []string
	)
	owner.rollbackHook = func() {
		orderMu.Lock()
		order = append(order, "owner")
		orderMu.Unlock()
	}
	reader.rollbackHook = func() {
		orderMu.Lock()
		order = append(order, "reader")
		orderMu.Unlock()
	}
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			beginCalls++
			if beginCalls == 1 {
				return owner, nil
			}
			if beginCalls == 2 {
				return reader, nil
			}
			return nil, errors.New("unexpected extra reader transaction")
		},
	)
	raw, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-close-admitted-reader",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-close-admitted-reader",
			Tables:       []StrictConsistencyTable{table},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*PostgresStrictConsistencySession)
	admitted := make(chan struct{})
	release := make(chan struct{})
	session.beforeWork = func() {
		close(admitted)
		<-release
	}
	callbackCalled := make(chan struct{}, 1)
	readerResult := make(chan error, 1)
	go func() {
		readerResult <- session.RunReader(
			context.Background(),
			table.Task,
			func(context.Context, PostgresStrictSnapshotQueryer) error {
				callbackCalled <- struct{}{}
				return nil
			},
		)
	}()
	select {
	case <-admitted:
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not pause after callback admission")
	}

	deadline, cancel := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancel()
	closeErr := session.Close(deadline)
	readerCallsBeforeRelease := reader.rollbackCalls
	ownerCallsBeforeRelease := owner.rollbackCalls
	callbackRanBeforeRelease := false
	select {
	case <-callbackCalled:
		callbackRanBeforeRelease = true
	default:
	}

	close(release)
	runErr := <-readerResult
	finalErr := session.Close(context.Background())

	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("admitted reader deadline cleanup error = %v", closeErr)
	}
	if readerCallsBeforeRelease != 0 ||
		ownerCallsBeforeRelease != 0 {
		t.Fatalf(
			"cleanup finished before admitted callback: reader=%d owner=%d",
			readerCallsBeforeRelease,
			ownerCallsBeforeRelease,
		)
	}
	if callbackRanBeforeRelease {
		t.Fatal("reader callback ran while the admission hook was paused")
	}
	if runErr != nil {
		t.Fatalf("admitted reader callback error = %v", runErr)
	}
	if finalErr != nil {
		t.Fatalf("eventual admitted reader cleanup error = %v", finalErr)
	}
	if reader.rollbackCalls != 1 ||
		owner.rollbackCalls != 1 {
		t.Fatalf(
			"admitted reader cleanup calls reader=%d owner=%d",
			reader.rollbackCalls,
			owner.rollbackCalls,
		)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if !reflect.DeepEqual(order, []string{"reader", "owner"}) {
		t.Fatalf("admitted reader cleanup order = %v", order)
	}
}

func TestPostgresStrictRequestAndSnapshotReferencesFailClosed(
	t *testing.T,
) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	base := StrictConsistencyOpenRequest{
		RunID:        "run-policy",
		SourceEngine: StrictConsistencyPostgres,
		Scope:        state.StrictSnapshotTable,
		ProcessEpoch: "process-policy",
		Tables:       []StrictConsistencyTable{table},
	}
	durable := state.StrictMigrationSnapshot{
		RunID:             "run-policy",
		EpochID:           "old",
		SourceEngine:      "postgres",
		SnapshotReference: "00000001-00000001-1",
		ProcessEpoch:      "process-old",
		CapturedAt:        time.Now(),
	}
	tests := map[string]func(*StrictConsistencyOpenRequest){
		"wrong engine": func(request *StrictConsistencyOpenRequest) {
			request.SourceEngine = StrictConsistencyMySQL
		},
		"unknown scope": func(request *StrictConsistencyOpenRequest) {
			request.Scope = "future"
		},
		"surviving snapshot reuse": func(request *StrictConsistencyOpenRequest) {
			request.Scope = state.StrictSnapshotMigration
			request.RequiredMigrationSnapshot = &durable
		},
		"partitioned task": func(request *StrictConsistencyOpenRequest) {
			request.Tables[0].Task.Partition = "range-1"
			request.Tables[0] = postgresStrictRepairAttempt(
				request.Tables[0],
			)
		},
		"wrong task type": func(request *StrictConsistencyOpenRequest) {
			request.Tables[0].Task.Type = "validate"
			request.Tables[0] = postgresStrictRepairAttempt(
				request.Tables[0],
			)
		},
		"implicit schema": func(request *StrictConsistencyOpenRequest) {
			request.Tables[0].Task.Schema = ""
			request.Tables[0] = postgresStrictRepairAttempt(
				request.Tables[0],
			)
		},
		"mismatched attempt": func(request *StrictConsistencyOpenRequest) {
			request.Tables[0].AttemptID = "strict-mismatch"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			request := base
			request.Tables = append(
				[]StrictConsistencyTable(nil),
				base.Tables...,
			)
			mutate(&request)
			if _, err := normalizePostgresStrictOpenRequest(
				request,
			); err == nil {
				t.Fatal("invalid request succeeded")
			}
		})
	}

	valid := []string{
		"00000003-0000000A-1",
		"a-B-0",
	}
	for _, reference := range valid {
		if err := validatePostgresSnapshotReference(reference); err != nil {
			t.Fatalf("valid reference %q: %v", reference, err)
		}
	}
	invalid := []string{
		"",
		"one-two",
		"one-two-three",
		"1-2-3'",
		"1--3",
		"1-2-3-4",
	}
	for _, reference := range invalid {
		if err := validatePostgresSnapshotReference(reference); err == nil {
			t.Fatalf("invalid reference %q succeeded", reference)
		}
	}
}

func TestPostgresStrictReaderImportFailureRollsBack(t *testing.T) {
	t.Parallel()
	table := postgresStrictTestTables()[0]
	var transactions []*postgresStrictFakeTransaction
	opener := postgresStrictFakeOpener(
		func(context.Context) (postgresStrictTransaction, error) {
			transaction := &postgresStrictFakeTransaction{
				id: len(transactions),
			}
			transactions = append(transactions, transaction)
			return transaction, nil
		},
	)
	opener.exportSnapshot = func(
		context.Context,
		postgresStrictTransaction,
	) (string, time.Time, error) {
		return "00000006-0000000F-1", time.Now(), nil
	}
	opener.importSnapshot = func(
		context.Context,
		postgresStrictTransaction,
		string,
	) error {
		return errors.New("import sentinel")
	}
	raw, err := opener.OpenStrictConsistency(
		context.Background(),
		StrictConsistencyOpenRequest{
			RunID:        "run-import",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-import",
			Tables:       []StrictConsistencyTable{table},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	session := raw.(*PostgresStrictConsistencySession)
	err = session.RunReader(
		context.Background(),
		table.Task,
		func(context.Context, PostgresStrictSnapshotQueryer) error {
			t.Fatal("callback ran after failed snapshot import")
			return nil
		},
	)
	if err == nil || !strings.Contains(err.Error(), "import sentinel") {
		t.Fatalf("reader import error = %v", err)
	}
	if len(transactions) != 2 || transactions[1].rollbackCalls != 1 {
		t.Fatalf("failed reader cleanup = %#v", transactions)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStrictCancellationReleasesOwnersAndReaders(
	t *testing.T,
) {
	t.Parallel()
	table := postgresStrictTestTables()[0]

	t.Run("open", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var transactions []*postgresStrictFakeTransaction
		opener := postgresStrictFakeOpener(
			func(context.Context) (postgresStrictTransaction, error) {
				transaction := &postgresStrictFakeTransaction{
					id: len(transactions),
				}
				transactions = append(transactions, transaction)
				return transaction, nil
			},
		)
		opener.countTable = func(
			context.Context,
			postgresStrictTransaction,
			state.TaskKey,
		) (int64, error) {
			cancel()
			return 0, context.Canceled
		}
		session, err := opener.OpenStrictConsistency(
			ctx,
			StrictConsistencyOpenRequest{
				RunID:        "run-canceled-open",
				SourceEngine: StrictConsistencyPostgres,
				Scope:        state.StrictSnapshotTable,
				ProcessEpoch: "process-canceled-open",
				Tables:       []StrictConsistencyTable{table},
			},
		)
		if session != nil || !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled open session=%#v error=%v", session, err)
		}
		if len(transactions) != 1 ||
			transactions[0].rollbackCalls != 1 {
			t.Fatalf(
				"canceled open owner cleanup = %#v",
				transactions,
			)
		}
	})

	t.Run("reader", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var transactions []*postgresStrictFakeTransaction
		opener := postgresStrictFakeOpener(
			func(context.Context) (postgresStrictTransaction, error) {
				transaction := &postgresStrictFakeTransaction{
					id: len(transactions),
				}
				transactions = append(transactions, transaction)
				return transaction, nil
			},
		)
		raw, err := opener.OpenStrictConsistency(
			context.Background(),
			StrictConsistencyOpenRequest{
				RunID:        "run-canceled-reader",
				SourceEngine: StrictConsistencyPostgres,
				Scope:        state.StrictSnapshotTable,
				ProcessEpoch: "process-canceled-reader",
				Tables:       []StrictConsistencyTable{table},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		session := raw.(*PostgresStrictConsistencySession)
		err = session.RunReader(
			ctx,
			table.Task,
			func(context.Context, PostgresStrictSnapshotQueryer) error {
				cancel()
				return nil
			},
		)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled reader error = %v", err)
		}
		if len(transactions) != 2 ||
			transactions[1].rollbackCalls != 1 {
			t.Fatalf(
				"canceled reader cleanup = %#v",
				transactions,
			)
		}
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if transactions[0].rollbackCalls != 1 {
			t.Fatal("canceled reader session leaked snapshot owner")
		}
	})
}

type postgresStrictFakeTransaction struct {
	id            int
	rollbackCalls int
	rollbackErr   error
	rollbackHook  func()
	execQueries   []string
}

func (*postgresStrictFakeTransaction) QueryContext(
	context.Context,
	string,
	...any,
) (*sql.Rows, error) {
	return nil, errors.New("unexpected fake QueryContext")
}

func (*postgresStrictFakeTransaction) QueryRowContext(
	context.Context,
	string,
	...any,
) *sql.Row {
	return &sql.Row{}
}

func (transaction *postgresStrictFakeTransaction) ExecContext(
	_ context.Context,
	query string,
	_ ...any,
) (sql.Result, error) {
	transaction.execQueries = append(transaction.execQueries, query)
	return nil, nil
}

func (transaction *postgresStrictFakeTransaction) Rollback() error {
	transaction.rollbackCalls++
	if transaction.rollbackHook != nil {
		transaction.rollbackHook()
	}
	return transaction.rollbackErr
}

type postgresStrictBlockingTransaction struct {
	postgresStrictFakeTransaction

	mu       sync.Mutex
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func newPostgresStrictBlockingTransaction() *postgresStrictBlockingTransaction {
	return &postgresStrictBlockingTransaction{
		started:  make(chan struct{}),
		release:  make(chan struct{}),
		finished: make(chan struct{}),
	}
}

func (transaction *postgresStrictBlockingTransaction) Rollback() error {
	transaction.mu.Lock()
	transaction.rollbackCalls++
	transaction.mu.Unlock()
	close(transaction.started)
	<-transaction.release
	if transaction.rollbackHook != nil {
		transaction.rollbackHook()
	}
	close(transaction.finished)
	return transaction.rollbackErr
}

func (transaction *postgresStrictBlockingTransaction) rollbackCallCount() int {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	return transaction.rollbackCalls
}

func postgresStrictFakeOpener(
	begin func(context.Context) (postgresStrictTransaction, error),
) *PostgresStrictConsistencyOpener {
	return &PostgresStrictConsistencyOpener{
		database:         &sql.DB{},
		beginTransaction: begin,
		admitTransaction: func(
			context.Context,
			postgresStrictTransaction,
		) error {
			return nil
		},
		exportSnapshot: func(
			context.Context,
			postgresStrictTransaction,
		) (string, time.Time, error) {
			return "00000001-00000001-1", time.Now(), nil
		},
		countTable: func(
			context.Context,
			postgresStrictTransaction,
			state.TaskKey,
		) (int64, error) {
			return 1, nil
		},
		importSnapshot: func(
			context.Context,
			postgresStrictTransaction,
			string,
		) error {
			return nil
		},
	}
}

func postgresStrictTestTables() []StrictConsistencyTable {
	tables := []StrictConsistencyTable{
		{
			Task: state.TaskKey{
				Type:   stage4AdapterNetworkTaskType,
				Schema: "a",
				Table:  "alpha",
			},
			WorkTopologyHash: "topology-alpha",
		},
		{
			Task: state.TaskKey{
				Type:   stage4AdapterNetworkTaskType,
				Schema: "z",
				Table:  "beta",
			},
			WorkTopologyHash: "topology-beta",
		},
	}
	for index := range tables {
		tables[index] = postgresStrictRepairAttempt(tables[index])
	}
	return tables
}

func postgresStrictRepairAttempt(
	table StrictConsistencyTable,
) StrictConsistencyTable {
	attempt, err := BuildStrictConsistencyAttemptID(
		table.Task,
		table.WorkTopologyHash,
		table.DurableWorkAttempts,
	)
	if err != nil {
		panic(err)
	}
	table.AttemptID = attempt
	return table
}

func TestPostgresStrictEpochIDIsDeterministicAndBounded(t *testing.T) {
	t.Parallel()
	first := postgresStrictEpochID(
		"process-1",
		"00000001-00000001-1",
	)
	if first != postgresStrictEpochID(
		"process-1",
		"00000001-00000001-1",
	) {
		t.Fatal("epoch ID is nondeterministic")
	}
	if first == postgresStrictEpochID(
		"process-2",
		"00000001-00000001-1",
	) ||
		first == postgresStrictEpochID(
			"process-1",
			"00000001-00000002-1",
		) {
		t.Fatal("epoch ID did not bind process and snapshot")
	}
	if err := validateCredentialFreeIdentifier(
		"epoch",
		first,
	); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresStrictRequestOrderingIsDeterministic(t *testing.T) {
	t.Parallel()
	tables := postgresStrictTestTables()
	request, err := normalizePostgresStrictOpenRequest(
		StrictConsistencyOpenRequest{
			RunID:        "run-order",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "process-order",
			Tables: []StrictConsistencyTable{
				tables[1],
				tables[0],
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(request.Tables, tables) {
		t.Fatalf("normalized tables = %#v, want %#v", request.Tables, tables)
	}
}
