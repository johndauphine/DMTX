package state

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func TestStage4RuntimeTuningHistorySQLiteConformance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	fixture := newStage4AggregateFixture(t, func(t *testing.T) (Backend, func() Backend) {
		return SQLiteStore{Path: path}, func() Backend { return SQLiteStore{Path: path} }
	}, false)
	history, ok := fixture.backend.(Stage4RuntimeTuningHistoryBackend)
	if !ok {
		t.Fatalf("%T does not implement runtime-tuning history", fixture.backend)
	}
	session := stage4RuntimeTuningHistorySession(fixture, 1, false, 3)
	stored, created, err := history.EnsureStage4RuntimeTuningSession(session)
	if err != nil || !created || !stored.Session.Equal(session) ||
		stored.Validate() != nil {
		t.Fatalf("first session=%#v created=%v err=%v", stored, created, err)
	}
	if replay, created, err := history.EnsureStage4RuntimeTuningSession(session); err != nil ||
		created || !replay.Equal(stored) {
		t.Fatalf("session replay=%#v created=%v err=%v", replay, created, err)
	}
	reopened := fixture.reopen().(Stage4RuntimeTuningHistoryBackend)
	loaded, found, err := reopened.LoadStage4RuntimeTuningSession(
		fixture.runID,
		session.SessionID,
	)
	if err != nil || !found || !loaded.Equal(stored) {
		t.Fatalf("loaded session=%#v found=%v err=%v", loaded, found, err)
	}

	first := stage4RuntimeTuningHistoryDecision(session, 1, "")
	firstReceipt, created, err := history.EnsureStage4RuntimeTuningDecision(first)
	if err != nil || !created || !firstReceipt.Decision.Equal(first) ||
		firstReceipt.Validate() != nil {
		t.Fatalf("first decision=%#v created=%v err=%v", firstReceipt, created, err)
	}
	if replay, created, err := history.EnsureStage4RuntimeTuningDecision(first); err != nil ||
		created || !replay.Equal(firstReceipt) {
		t.Fatalf("decision replay=%#v created=%v err=%v", replay, created, err)
	}

	for label, changed := range map[string]Stage4RuntimeTuningDecision{
		"skipped ordinal": stage4RuntimeTuningHistoryDecision(
			session, 3, firstReceipt.Digest,
		),
		"changed replay": func() Stage4RuntimeTuningDecision {
			value := first.Clone()
			value.After.ChunkRows.Value++
			return value
		}(),
	} {
		if _, _, err := history.EnsureStage4RuntimeTuningDecision(changed); !errors.Is(err, ErrState) || !errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("%s decision error = %v", label, err)
		}
	}
	second := stage4RuntimeTuningHistoryDecision(session, 2, firstReceipt.Digest)
	secondReceipt, created, err := history.EnsureStage4RuntimeTuningDecision(second)
	if err != nil || !created || !secondReceipt.Decision.Equal(second) {
		t.Fatalf("second decision=%#v created=%v err=%v", secondReceipt, created, err)
	}
	decisions, err := reopened.LoadStage4RuntimeTuningDecisions(
		fixture.runID,
		session.SessionID,
	)
	if err != nil || len(decisions) != 2 || !decisions[0].Equal(firstReceipt) ||
		!decisions[1].Equal(secondReceipt) {
		t.Fatalf("loaded decisions=%#v err=%v", decisions, err)
	}

	changedSession := session.Clone()
	changedSession.IntentDigest = stage4RuntimeTuningDigest("different")
	if _, _, err := history.EnsureStage4RuntimeTuningSession(changedSession); !errors.Is(err, ErrState) ||
		!errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("changed session error = %v", err)
	}
	resume := session.Clone()
	resume.SessionID = stage4RuntimeTuningHistorySessionID(2)
	resume.Resume = true
	resume.StartedAt = resume.StartedAt.Add(time.Minute)
	if _, created, err := history.EnsureStage4RuntimeTuningSession(resume); err != nil || !created {
		t.Fatalf("resume session created=%v err=%v", created, err)
	}
	// A resume owns a fresh controller/session. It starts a new receipt chain
	// at ordinal one and may choose a different initial safety state; previous
	// adaptive values are historical evidence, never resume input.
	resumeDecision := stage4RuntimeTuningHistoryDecision(resume, 1, "")
	resumeDecision.After.ChunkRows.Value = 4
	resumeDecision.After.ChunkRows.LiveProvenance = "safety_reduction"
	resumeDecision.Reasons = []string{"write_error"}
	if receipt, created, err := history.EnsureStage4RuntimeTuningDecision(
		resumeDecision,
	); err != nil || !created ||
		receipt.Decision.Boundary.Ordinal != 1 ||
		receipt.Decision.PreviousDigest != "" {
		t.Fatalf("resume first decision=%#v created=%v err=%v", receipt, created, err)
	}
}

func TestStage4RuntimeTuningHistorySQLiteRetentionAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	fixture := newStage4AggregateFixture(t, func(t *testing.T) (Backend, func() Backend) {
		return SQLiteStore{Path: path}, func() Backend { return SQLiteStore{Path: path} }
	}, false)
	history := fixture.backend.(Stage4RuntimeTuningHistoryBackend)
	session := stage4RuntimeTuningHistorySession(fixture, 1, false, 2)
	if _, created, err := history.EnsureStage4RuntimeTuningSession(session); err != nil || !created {
		t.Fatalf("ensure session created=%v err=%v", created, err)
	}
	previous := ""
	for ordinal := uint64(1); ordinal <= 3; ordinal++ {
		receipt, created, err := history.EnsureStage4RuntimeTuningDecision(
			stage4RuntimeTuningHistoryDecision(session, ordinal, previous),
		)
		if err != nil || !created {
			t.Fatalf("decision %d created=%v err=%v", ordinal, created, err)
		}
		previous = receipt.Digest
	}
	decisions, err := fixture.reopen().(Stage4RuntimeTuningHistoryBackend).
		LoadStage4RuntimeTuningDecisions(fixture.runID, session.SessionID)
	if err != nil || len(decisions) != 2 ||
		decisions[0].Decision.Boundary.Ordinal != 2 ||
		decisions[1].Decision.Boundary.Ordinal != 3 ||
		decisions[1].Decision.PreviousDigest != decisions[0].Digest {
		t.Fatalf("retained decisions=%#v err=%v", decisions, err)
	}

	for index := 2; index <= Stage4RuntimeTuningSessionRetention+1; index++ {
		candidate := stage4RuntimeTuningHistorySession(fixture, index, true, 2)
		candidate.StartedAt = session.StartedAt.Add(time.Duration(index) * time.Minute)
		if _, created, err := history.EnsureStage4RuntimeTuningSession(candidate); err != nil || !created {
			t.Fatalf("session %d created=%v err=%v", index, created, err)
		}
	}
	if _, found, err := history.LoadStage4RuntimeTuningSession(
		fixture.runID,
		session.SessionID,
	); err != nil || found {
		t.Fatalf("pruned first session found=%v err=%v", found, err)
	}
	newestID := stage4RuntimeTuningHistorySessionID(
		Stage4RuntimeTuningSessionRetention + 1,
	)
	if _, found, err := history.LoadStage4RuntimeTuningSession(
		fixture.runID,
		newestID,
	); err != nil || !found {
		t.Fatalf("newest session found=%v err=%v", found, err)
	}

	store := fixture.backend.(SQLiteStore)
	database, err := store.openStage4()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		UPDATE stage4_records SET payload = ?
		WHERE kind = ? AND run_id = ? AND task_key = ? AND record_id = ?
	`, `{"corrupt":true}`, stage4RuntimeTuningSessionRecord,
		fixture.runID, stage4MigrationTaskKey, newestID); err != nil {
		database.Close()
		t.Fatal(err)
	}
	database.Close()
	if _, _, err := history.LoadStage4RuntimeTuningSession(
		fixture.runID,
		newestID,
	); !errors.Is(err, ErrState) {
		t.Fatalf("corrupt session error = %v", err)
	}
}

func TestStage4RuntimeTuningHistorySQLiteRequiresAuthorityAndFencesWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	raw := SQLiteStore{Path: path}
	started := time.Date(2026, time.August, 1, 12, 0, 0, 0, time.UTC)
	if err := raw.InitializeRun(Run{
		ID: "no-history-authority", Source: "source", Target: "target",
		SourceEngine: "postgres", SourceIdentity: "postgres:source/database",
		TargetIdentity: "postgres:target/database", Outcome: Running,
		Resumable: true, Reason: "running", StartedAt: started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}
	noAuthority := Stage4RuntimeTuningSession{
		Version: Stage4RuntimeTuningHistoryVersion, RunID: "no-history-authority",
		SessionID:    stage4RuntimeTuningHistorySessionID(1),
		Task:         TaskKey{Type: "table-copy", Schema: "public", Table: "items"},
		TopologyHash: "table-topology", SourceEngine: "postgres", TargetEngine: "postgres",
		IntentDigest: stage4RuntimeTuningDigest("intent"), IntervalNanos: int64(time.Second),
		DecisionLimit: 2, StartedAt: started.Add(time.Second),
	}
	if _, _, err := raw.EnsureStage4RuntimeTuningSession(noAuthority); !errors.Is(err, ErrState) ||
		!errors.Is(err, ErrUnknownWork) {
		t.Fatalf("missing inventory/work authority error = %v", err)
	}

	fixture := newStage4AggregateFixture(t, func(t *testing.T) (Backend, func() Backend) {
		return raw, func() Backend { return SQLiteStore{Path: path} }
	}, false)
	leaseStore := SQLiteStore{Path: filepath.Join(t.TempDir(), "lease.db")}
	lease, err := leaseStore.AcquireLease("sqlite:target", fixture.runID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.BindRunLease(fixture.runID, lease); err != nil {
		t.Fatal(err)
	}
	fenced := FenceBackend(raw, NewLeaseGuard(leaseStore, lease))
	history, ok := ResolveFencedStage4RuntimeTuningHistory(fenced)
	if !ok {
		t.Fatalf("%T did not resolve fenced runtime-tuning history", fenced)
	}
	session := stage4RuntimeTuningHistorySession(fixture, 2, false, 2)
	if _, created, err := history.EnsureStage4RuntimeTuningSession(session); err != nil || !created {
		t.Fatalf("fenced session created=%v err=%v", created, err)
	}
	database, err := leaseStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(
		`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
		time.Unix(0, 0).UTC(), lease.Target,
	); err != nil {
		database.Close()
		t.Fatal(err)
	}
	database.Close()
	if _, err := leaseStore.AcquireLease(lease.Target, "replacement", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, _, err := history.EnsureStage4RuntimeTuningDecision(
		stage4RuntimeTuningHistoryDecision(session, 1, ""),
	); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale lease decision error = %v", err)
	}

	yaml := YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	if _, ok := any(yaml).(Stage4RuntimeTuningHistoryBackend); ok {
		t.Fatal("YAML unexpectedly advertises full runtime-tuning history")
	}
	yamlFenced := FenceBackend(yaml, NewLeaseGuard(leaseStore, lease))
	if _, ok := ResolveFencedStage4RuntimeTuningHistory(yamlFenced); ok {
		t.Fatal("fenced YAML unexpectedly resolves full runtime-tuning history")
	}
}

func stage4RuntimeTuningHistorySession(
	fixture stage4AggregateFixture,
	index int,
	resume bool,
	limit int,
) Stage4RuntimeTuningSession {
	return Stage4RuntimeTuningSession{
		Version:       Stage4RuntimeTuningHistoryVersion,
		RunID:         fixture.runID,
		SessionID:     stage4RuntimeTuningHistorySessionID(index),
		Resume:        resume,
		Task:          fixture.task,
		TopologyHash:  "table-topology",
		SourceEngine:  "postgres",
		TargetEngine:  "postgres",
		IntentDigest:  stage4RuntimeTuningDigest("runtime-intent-v1"),
		IntervalNanos: int64(time.Second),
		DecisionLimit: limit,
		StartedAt:     fixture.started.Add(time.Minute),
	}
}

func stage4RuntimeTuningHistorySessionID(index int) string {
	return fmt.Sprintf("%032x", index)
}

func stage4RuntimeTuningDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func stage4RuntimeTuningHistoryDecision(
	session Stage4RuntimeTuningSession,
	ordinal uint64,
	previous string,
) Stage4RuntimeTuningDecision {
	initial := Stage4RuntimeTuningValues{
		ChunkRows: Stage4RuntimeTuningValue{
			Value: 8, IntentValue: 8, IntentProvenance: "derived",
			LiveProvenance: "initial",
		},
		Writers: Stage4RuntimeTuningValue{
			Value: 1, IntentValue: 1, IntentProvenance: "derived",
			LiveProvenance: "initial",
		},
		BufferDepth: Stage4RuntimeTuningValue{
			Value: 2, IntentValue: 2, IntentProvenance: "derived",
			LiveProvenance: "initial",
		},
	}
	return Stage4RuntimeTuningDecision{
		Version: Stage4RuntimeTuningHistoryVersion,
		RunID:   session.RunID, SessionID: session.SessionID,
		Boundary: Stage4RuntimeTuningBoundary{
			Ordinal: ordinal, TableSchema: session.Task.Schema,
			TableName: session.Task.Table, RangeIndex: 0,
			ChunkSequence: ordinal - 1,
		},
		Before: initial, After: initial,
		Reasons: []string{"healthy_observation"}, PreviousDigest: previous,
	}
}
