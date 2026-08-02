package migrate

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4DeferredRuntimeTuningPersistsFencedSQLiteHistoryBeforePrepare(
	t *testing.T,
) {
	events := make([]string, 0)
	rows := make([]string, 24)
	for index := range rows {
		rows[index] = fmt.Sprintf("payload-%02d", index+1)
	}
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
		rows:   rows,
	}
	raw := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")}
	runID := "stage4-runtime-tuning-sqlite-history"
	initializeStage4LifecycleRun(
		t,
		raw,
		runID,
		time.Now().UTC().Add(-time.Minute),
	)
	leaseStore := state.SQLiteStore{Path: filepath.Join(t.TempDir(), "leases.db")}
	lease, err := leaseStore.AcquireLease(
		"postgres:target.example:5432/app",
		runID,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.BindRunLease(runID, lease); err != nil {
		t.Fatal(err)
	}
	fenced, ok := state.FenceBackend(
		raw,
		state.NewLeaseGuard(leaseStore, lease),
	).(Stage4StateBackend)
	if !ok {
		t.Fatalf("fenced SQLite backend does not implement Stage4StateBackend")
	}

	protected := false
	rawTarget := &recordingAdapterTarget{
		events:    &events,
		protected: &protected,
	}
	baseTarget := &stage4RuntimeTuningProtocolLimitTarget{
		stage4NetworkAdmissionTarget: &stage4NetworkAdmissionTarget{
			recordingAdapterTarget: rawTarget,
			backend:                fenced,
			runID:                  runID,
		},
		protocolFailures: 1,
	}
	target := &stage4RuntimeTuningHistoryPrepareTarget{
		stage4RuntimeTuningProtocolLimitTarget: baseTarget,
		raw:                                    raw,
		runID:                                  runID,
	}
	observer := &stage4NetworkAdmissionObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run:                    stage4LifecycleRunContext(t, fenced, runID, false),
		},
		protected: &protected,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Partitions = 2
	cfg.Migration.ChunkSize = 8
	cfg.Migration.ReadAhead = 2
	cfg.Migration.MaxRetries = 1
	cfg.Migration.RuntimeTuning = true
	cfg.Migration.RuntimeTuningInterval = time.Hour

	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if target.sessionsBeforePrepare != 1 || result.RuntimeTuning == nil ||
		len(result.RuntimeTuning.Tables) != 1 {
		t.Fatalf(
			"history-before-prepare=%d result=%#v",
			target.sessionsBeforePrepare,
			result,
		)
	}

	history, ok := any(raw).(state.Stage4RuntimeTuningHistoryBackend)
	if !ok {
		t.Fatalf("raw SQLite state does not expose history reads")
	}
	sessionIDs := target.runtimeTuningHistorySessionIDs(t)
	if len(sessionIDs) != 1 {
		t.Fatalf("persisted runtime-tuning session IDs = %v", sessionIDs)
	}
	session, found, err := history.LoadStage4RuntimeTuningSession(
		runID,
		sessionIDs[0],
	)
	if err != nil || !found || session.Session.Resume ||
		session.Session.Task != (state.TaskKey{
			Type: stage4AdapterNetworkTaskType, Schema: "public", Table: "items",
		}) ||
		session.Session.DecisionLimit != 128 {
		t.Fatalf("persisted runtime-tuning session=%#v found=%v err=%v", session, found, err)
	}
	decisions, err := history.LoadStage4RuntimeTuningDecisions(
		runID,
		sessionIDs[0],
	)
	if err != nil || len(decisions) == 0 ||
		len(decisions) != len(result.RuntimeTuning.Tables[0].Adjustments) {
		t.Fatalf(
			"persisted decisions=%#v report=%#v err=%v",
			decisions,
			result.RuntimeTuning.Tables[0],
			err,
		)
	}
	if !runtimeTuningHistoryDecisionsContainReason(
		decisions,
		string(RuntimeReasonProtocolWriteError),
	) {
		t.Fatalf("persisted decisions omit protocol safety reduction: %#v", decisions)
	}
}

// stage4RuntimeTuningHistoryPrepareTarget proves the session receipt exists
// before the target adapter reaches its first mutating lifecycle call. The
// production adapter's normal durable-work sentinel remains underneath it.
type stage4RuntimeTuningHistoryPrepareTarget struct {
	*stage4RuntimeTuningProtocolLimitTarget
	raw                   state.SQLiteStore
	runID                 string
	sessionsBeforePrepare int
}

func (target *stage4RuntimeTuningHistoryPrepareTarget) PrepareTables(
	ctx context.Context,
	tables []schema.Table,
	mode string,
) error {
	sessions, err := target.runtimeTuningHistorySessionIDsFromStore()
	if err != nil {
		return err
	}
	target.sessionsBeforePrepare = len(sessions)
	if len(sessions) != 1 {
		return fmt.Errorf(
			"target preparation reached before one durable runtime-tuning session: %v",
			sessions,
		)
	}
	return target.stage4RuntimeTuningProtocolLimitTarget.PrepareTables(
		ctx,
		tables,
		mode,
	)
}

func (target *stage4RuntimeTuningHistoryPrepareTarget) runtimeTuningHistorySessionIDs(
	t *testing.T,
) []string {
	t.Helper()
	ids, err := target.runtimeTuningHistorySessionIDsFromStore()
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func (target *stage4RuntimeTuningHistoryPrepareTarget) runtimeTuningHistorySessionIDsFromStore() ([]string, error) {
	database, err := target.raw.Open()
	if err != nil {
		return nil, err
	}
	defer database.Close()
	rows, err := database.Query(`
		SELECT record_id FROM stage4_records
		WHERE kind = ? AND run_id = ? AND task_key = ?
		ORDER BY record_id
	`, "runtime_tuning_session", target.runID, "migration")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]string, 0, 1)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

func runtimeTuningHistoryDecisionsContainReason(
	decisions []state.Stage4RuntimeTuningDecisionReceipt,
	want string,
) bool {
	for _, receipt := range decisions {
		for _, reason := range receipt.Decision.Reasons {
			if reason == want {
				return true
			}
		}
	}
	return false
}

var _ targetAdapter = (*stage4RuntimeTuningHistoryPrepareTarget)(nil)
