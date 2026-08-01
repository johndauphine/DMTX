package migrate

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestSQLServerMigrationSnapshotReopenFailurePreservesDurableOwner(t *testing.T) {
	opener, closeDatabases := newSQLServerMigrationSnapshotFaultOpener(t)
	defer closeDatabases()
	plan := sqlServerMigrationSnapshotPlan{
		sourceDatabase: "source_db", sourceDatabaseID: 42,
		snapshot: sqlServerMigrationSnapshotName("run-1", "source_db"),
	}
	captured := strictCoreTime()
	record := sqlServerMigrationSnapshotRecord{
		sourceDatabaseID: plan.sourceDatabaseID,
		state:            "ONLINE",
		readOnly:         true,
		capturedAt:       captured,
	}
	opener.preflightFn = func(context.Context, string) (sqlServerMigrationSnapshotPlan, error) {
		return plan, nil
	}
	opener.lookupFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error) {
		return record, nil
	}
	drops := 0
	opener.dropFn = func(context.Context, string) error {
		drops++
		return nil
	}
	transient := errors.New("temporary verified TLS connection failure")
	opener.openSnapshot = func(context.Context, config.Endpoint) (*sql.DB, error) {
		return nil, transient
	}

	_, err := opener.OpenPlannedStrictConsistency(context.Background(),
		PlannedStrictConsistencyOpenRequest{
			RunID:        "run-1",
			SourceEngine: StrictConsistencyMSSQL,
			Scope:        state.StrictSnapshotMigration,
			Resume:       true,
			ProcessEpoch: "resume-process",
			Tasks: []state.TaskKey{{
				Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
			}},
			RequiredMigrationSnapshot: &state.StrictMigrationSnapshot{
				RunID:             "run-1",
				EpochID:           sqlServerMigrationSnapshotEpoch(plan.snapshot),
				SourceEngine:      "mssql",
				SnapshotReference: plan.snapshot,
				ProcessEpoch:      "original-process",
				CapturedAt:        captured,
			},
		},
	)
	if !errors.Is(err, transient) {
		t.Fatalf("reopen error = %v, want transient reader-open failure", err)
	}
	if drops != 0 {
		t.Fatalf("transient durable reopen dropped %d snapshots, want 0", drops)
	}
}

func TestSQLServerMigrationSnapshotReaderOpenFailureJoinsBoundedCleanupFailure(t *testing.T) {
	opener, closeDatabases := newSQLServerMigrationSnapshotFaultOpener(t)
	defer closeDatabases()
	plan := sqlServerMigrationSnapshotPlan{
		sourceDatabase: "source_db", sourceDatabaseID: 42,
		snapshot: sqlServerMigrationSnapshotName("run-1", "source_db"),
	}
	record := sqlServerMigrationSnapshotRecord{
		sourceDatabaseID: plan.sourceDatabaseID,
		state:            "ONLINE",
		readOnly:         true,
		capturedAt:       strictCoreTime(),
	}
	opener.preflightFn = func(context.Context, string) (sqlServerMigrationSnapshotPlan, error) {
		return plan, nil
	}
	opener.lookupIfPresentFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, bool, error) {
		return sqlServerMigrationSnapshotRecord{}, false, nil
	}
	opener.lookupFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error) {
		return record, nil
	}
	opener.createFn = func(context.Context, sqlServerMigrationSnapshotPlan) error {
		return nil
	}
	readerOpenErr := errors.New("verified TLS reader open failed")
	opener.openSnapshot = func(context.Context, config.Endpoint) (*sql.DB, error) {
		return nil, readerOpenErr
	}
	dropErr := errors.New("drop SQL Server snapshot failed")
	var deadline time.Time
	opener.dropFn = func(ctx context.Context, reference string) error {
		if reference != plan.snapshot {
			t.Fatalf("cleanup reference = %q, want %q", reference, plan.snapshot)
		}
		var ok bool
		deadline, ok = ctx.Deadline()
		if !ok {
			t.Fatal("created snapshot cleanup context has no deadline")
		}
		return dropErr
	}

	_, err := opener.OpenPlannedStrictConsistency(context.Background(),
		PlannedStrictConsistencyOpenRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyMSSQL,
			Scope: state.StrictSnapshotMigration, ProcessEpoch: "process-1",
			Tasks: []state.TaskKey{{
				Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
			}},
		},
	)
	if !errors.Is(err, readerOpenErr) || !errors.Is(err, dropErr) {
		t.Fatalf("reader-open/cleanup error = %v", err)
	}
	if deadline.IsZero() || time.Until(deadline) > strictConsistencyCleanupTimeout {
		t.Fatalf("created snapshot cleanup deadline = %v", deadline)
	}
}

func TestSQLServerMigrationSnapshotAmbiguousCreateAcknowledgementCleansVerifiedSnapshot(t *testing.T) {
	opener, closeDatabases := newSQLServerMigrationSnapshotFaultOpener(t)
	defer closeDatabases()
	plan := sqlServerMigrationSnapshotPlan{
		sourceDatabase: "source_db", sourceDatabaseID: 42,
		snapshot: sqlServerMigrationSnapshotName("run-1", "source_db"),
	}
	record := sqlServerMigrationSnapshotRecord{
		sourceDatabaseID: plan.sourceDatabaseID,
		state:            "ONLINE",
		readOnly:         true,
		capturedAt:       strictCoreTime(),
	}
	opener.preflightFn = func(context.Context, string) (sqlServerMigrationSnapshotPlan, error) {
		return plan, nil
	}
	lookups := 0
	opener.lookupIfPresentFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, bool, error) {
		lookups++
		if lookups == 1 {
			return sqlServerMigrationSnapshotRecord{}, false, nil
		}
		return record, true, nil
	}
	createErr := errors.New("create acknowledgement lost after server commit")
	opener.createFn = func(context.Context, sqlServerMigrationSnapshotPlan) error {
		return createErr
	}
	opener.lookupFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error) {
		return record, nil
	}
	drops := 0
	opener.dropFn = func(ctx context.Context, reference string) error {
		drops++
		if reference != plan.snapshot {
			t.Fatalf("ambiguous CREATE cleanup reference = %q, want %q", reference, plan.snapshot)
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("ambiguous CREATE cleanup context has no deadline")
		}
		return nil
	}

	_, err := opener.OpenPlannedStrictConsistency(context.Background(),
		PlannedStrictConsistencyOpenRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyMSSQL,
			Scope: state.StrictSnapshotMigration, ProcessEpoch: "process-1",
			Tasks: []state.TaskKey{{
				Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
			}},
		},
	)
	if !errors.Is(err, createErr) {
		t.Fatalf("ambiguous CREATE error = %v", err)
	}
	if lookups != 2 || drops != 1 {
		t.Fatalf("ambiguous CREATE reconciliation lookups=%d drops=%d", lookups, drops)
	}
}

func TestSQLServerMigrationSnapshotSetupCleanupRefusesReplacementBeforeDrop(t *testing.T) {
	opener, closeDatabases := newSQLServerMigrationSnapshotFaultOpener(t)
	defer closeDatabases()
	plan := sqlServerMigrationSnapshotPlan{
		sourceDatabase: "source_db", sourceDatabaseID: 42,
		snapshot: sqlServerMigrationSnapshotName("run-1", "source_db"),
	}
	original := sqlServerMigrationSnapshotRecord{
		sourceDatabaseID: plan.sourceDatabaseID,
		state:            "ONLINE",
		readOnly:         true,
		capturedAt:       strictCoreTime(),
	}
	replacement := original
	replacement.capturedAt = original.capturedAt.Add(time.Second)
	opener.preflightFn = func(context.Context, string) (sqlServerMigrationSnapshotPlan, error) {
		return plan, nil
	}
	opener.lookupIfPresentFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, bool, error) {
		return sqlServerMigrationSnapshotRecord{}, false, nil
	}
	lookupCalls := 0
	opener.lookupFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error) {
		lookupCalls++
		if lookupCalls == 1 {
			return original, nil
		}
		return replacement, nil
	}
	opener.createFn = func(context.Context, sqlServerMigrationSnapshotPlan) error {
		return nil
	}
	readerOpenErr := errors.New("verified TLS reader open failed")
	opener.openSnapshot = func(context.Context, config.Endpoint) (*sql.DB, error) {
		return nil, readerOpenErr
	}
	drops := 0
	opener.dropFn = func(context.Context, string) error {
		drops++
		return nil
	}

	_, err := opener.OpenPlannedStrictConsistency(context.Background(),
		PlannedStrictConsistencyOpenRequest{
			RunID: "run-1", SourceEngine: StrictConsistencyMSSQL,
			Scope: state.StrictSnapshotMigration, ProcessEpoch: "process-1",
			Tasks: []state.TaskKey{{
				Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
			}},
		},
	)
	if !errors.Is(err, readerOpenErr) ||
		!strings.Contains(err.Error(), "creation time changed") {
		t.Fatalf("replacement setup cleanup error = %v", err)
	}
	if lookupCalls != 2 || drops != 0 {
		t.Fatalf("replacement setup cleanup lookups=%d drops=%d", lookupCalls, drops)
	}
}

func TestSQLServerMigrationSnapshotResumeWithoutOwnerOnlyReopensDeterministicSnapshot(t *testing.T) {
	t.Run("existing", func(t *testing.T) {
		opener, closeDatabases := newSQLServerMigrationSnapshotFaultOpener(t)
		defer closeDatabases()
		plan := sqlServerMigrationSnapshotPlan{
			sourceDatabase: "source_db", sourceDatabaseID: 42,
			snapshot: sqlServerMigrationSnapshotName("run-1", "source_db"),
		}
		record := sqlServerMigrationSnapshotRecord{
			sourceDatabaseID: plan.sourceDatabaseID,
			state:            "ONLINE",
			readOnly:         true,
			capturedAt:       strictCoreTime(),
		}
		opener.preflightFn = func(context.Context, string) (sqlServerMigrationSnapshotPlan, error) {
			return plan, nil
		}
		opener.lookupFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error) {
			return record, nil
		}
		created := 0
		opener.createFn = func(context.Context, sqlServerMigrationSnapshotPlan) error {
			created++
			return nil
		}

		session, err := opener.OpenPlannedStrictConsistency(context.Background(),
			unownedSQLServerMigrationResumeRequest(),
		)
		if err != nil {
			t.Fatal(err)
		}
		if created != 0 {
			t.Fatalf("unowned resume created %d replacement snapshots", created)
		}
		got, ok := session.(*SQLServerMigrationSnapshotSession)
		if !ok || got.reference != plan.snapshot ||
			got.epoch != sqlServerMigrationSnapshotEpoch(plan.snapshot) {
			t.Fatalf("unowned recovered session = %#v", session)
		}
		// The helper databases are not SQL Server; closing their handles directly
		// avoids exercising DROP DATABASE in this decision-path test.
		if err := got.snapshot.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("absent", func(t *testing.T) {
		opener, closeDatabases := newSQLServerMigrationSnapshotFaultOpener(t)
		defer closeDatabases()
		plan := sqlServerMigrationSnapshotPlan{
			sourceDatabase: "source_db", sourceDatabaseID: 42,
			snapshot: sqlServerMigrationSnapshotName("run-1", "source_db"),
		}
		opener.preflightFn = func(context.Context, string) (sqlServerMigrationSnapshotPlan, error) {
			return plan, nil
		}
		opener.lookupFn = func(context.Context, sqlServerMigrationSnapshotPlan) (sqlServerMigrationSnapshotRecord, error) {
			return sqlServerMigrationSnapshotRecord{}, errors.New("snapshot is missing")
		}
		created := 0
		opener.createFn = func(context.Context, sqlServerMigrationSnapshotPlan) error {
			created++
			return nil
		}

		_, err := opener.OpenPlannedStrictConsistency(context.Background(),
			unownedSQLServerMigrationResumeRequest(),
		)
		if err == nil || !strings.Contains(err.Error(), "pre-state crash") {
			t.Fatalf("missing unowned snapshot error = %v", err)
		}
		if created != 0 {
			t.Fatalf("missing unowned resume created %d replacement snapshots", created)
		}
	})
}

func TestSQLServerMigrationSnapshotCleanupRefusesReplacementAtSameIdentity(t *testing.T) {
	captured := strictCoreTime()
	session := &SQLServerMigrationSnapshotSession{
		sourceDatabaseID: 42,
		captured:         captured,
	}
	err := session.requireVerifiedCleanupRecord(sqlServerMigrationSnapshotRecord{
		sourceDatabaseID: 42,
		state:            "ONLINE",
		readOnly:         true,
		capturedAt:       captured.Add(time.Second),
	})
	if err == nil || !strings.Contains(err.Error(), "creation time differs") {
		t.Fatalf("replacement snapshot cleanup error = %v", err)
	}
}

func newSQLServerMigrationSnapshotFaultOpener(
	t *testing.T,
) (*SQLServerMigrationSnapshotOpener, func()) {
	t.Helper()
	source, err := sql.Open("sqlite", "file:strict-source?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := sql.Open("sqlite", "file:strict-snapshot?mode=memory&cache=shared")
	if err != nil {
		_ = source.Close()
		t.Fatal(err)
	}
	opener := &SQLServerMigrationSnapshotOpener{
		source: source,
		endpoint: config.Endpoint{
			Database: "source_db",
		},
		namespace:    "dbo",
		verifySource: func(context.Context, *sql.DB) error { return nil },
		openSnapshot: func(context.Context, config.Endpoint) (*sql.DB, error) {
			return snapshot, nil
		},
		now: func() time.Time { return strictCoreTime() },
	}
	return opener, func() {
		_ = snapshot.Close()
		_ = source.Close()
	}
}

func unownedSQLServerMigrationResumeRequest() PlannedStrictConsistencyOpenRequest {
	return PlannedStrictConsistencyOpenRequest{
		RunID:        "run-1",
		SourceEngine: StrictConsistencyMSSQL,
		Scope:        state.StrictSnapshotMigration,
		Resume:       true,
		ProcessEpoch: "resume-process",
		Tasks: []state.TaskKey{{
			Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
		}},
		ReopenUnownedMigrationSnapshot: true,
	}
}
