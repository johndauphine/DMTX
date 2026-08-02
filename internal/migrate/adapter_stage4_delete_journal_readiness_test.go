package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4DeleteJournalReadinessStaysDormantWithoutReconcile(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	runID := "delete-journal-readiness-dormant"
	initializeStage4LifecycleRun(t, backend, runID, time.Now().UTC().Add(-time.Minute))
	run := stage4LifecycleRunContext(t, backend, runID, false)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.Deletes.Mode = config.DeleteModeOff

	prepared, err := prepareStage4AdapterRun(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		"upsert",
		run,
	)
	if err != nil {
		t.Fatalf("prepare deletes-off adapter run: %v", err)
	}
	if prepared.deleteJournalReadiness != nil {
		t.Fatal("deletes-off route admitted delete-journal readiness")
	}
	if target.nativeCalls != 0 {
		t.Fatalf("deletes-off native journal preparation calls=%d", target.nativeCalls)
	}
	for _, event := range events {
		if event == "delete_journal_preflight" ||
			event == "delete_journal_native_reread" {
			t.Fatalf("deletes-off route crossed readiness seam: %v", events)
		}
	}
}

func TestStage4DeleteJournalReadinessPrecheckpointCapabilitiesFailClosed(
	t *testing.T,
) {
	tests := []struct {
		name     string
		backend  func(state.YAMLStore) Stage4StateBackend
		observer func(*[]string, Stage4RunContext) TableObserver
		want     string
	}{
		{
			name: "missing readiness backend",
			backend: func(raw state.YAMLStore) Stage4StateBackend {
				return stage4DeleteJournalReadinessMissingBackend{
					Backend:                raw,
					RangeBackend:           raw,
					Stage4Backend:          raw,
					Stage4AggregateBackend: raw,
				}
			},
			observer: func(events *[]string, run Stage4RunContext) TableObserver {
				return stage4AdapterObserver{
					recordingTableObserver: recordingTableObserver{events: events},
					run:                    run,
				}
			},
			want: "backend is unavailable",
		},
		{
			name: "missing mutation protector",
			backend: func(raw state.YAMLStore) Stage4StateBackend {
				return raw
			},
			observer: func(events *[]string, run Stage4RunContext) TableObserver {
				return stage4AdapterUnprotectedObserver{
					recordingTableObserver: recordingTableObserver{events: events},
					run:                    run,
				}
			},
			want: "lease-fenced target mutation protector",
		},
		{
			name: "typed nil mutation protector",
			backend: func(raw state.YAMLStore) Stage4StateBackend {
				return raw
			},
			observer: func(*[]string, Stage4RunContext) TableObserver {
				var observer *stage4AdapterObserver
				return observer
			},
			want: "lease-fenced target mutation protector",
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			events := make([]string, 0)
			raw := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
			runID := "delete-journal-precheckpoint-" +
				strings.ReplaceAll(testCase.name, " ", "-")
			initializeStage4LifecycleRun(
				t,
				raw,
				runID,
				time.Now().UTC().Add(-time.Minute),
			)
			run := stage4LifecycleRunContext(
				t,
				testCase.backend(raw),
				runID,
				false,
			)
			target := newStage4DeleteJournalReadinessTestTarget(&events)
			capability, err := admitStage4AdapterDeleteJournalReadinessForRun(
				context.Background(),
				testCase.observer(&events, run),
				run,
				target,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("precheckpoint capability error = %v, want %q", err, testCase.want)
			}
			if capability != nil {
				t.Fatalf("failed precheckpoint admission retained capability=%#v", capability)
			}
			assertStage4DeleteJournalReadinessPrecheckpointAdmissionUntouched(
				t,
				raw,
				runID,
				target,
				events,
			)
		})
	}
}

func TestStage4DeleteJournalReadinessOrdersPreflightAndDurableBoundary(
	t *testing.T,
) {
	backend := &stage4DeleteJournalCommitThenErrorBackend{
		YAMLStore: state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
	events := make([]string, 0)
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	capability, err := admitStage4DeleteJournalReadiness(
		context.Background(),
		target,
	)
	if err != nil {
		t.Fatalf("admit readiness preflight: %v", err)
	}
	if capability == nil || len(events) != 1 ||
		events[0] != "delete_journal_preflight" {
		t.Fatalf("read-only readiness preflight events=%v capability=%#v", events, capability)
	}

	fixture := newStage4DeleteJournalReadinessFixture(t, backend, target)
	fixture.prepared.deleteJournalReadiness = capability
	target.beforeNative = func(request Stage4DeleteJournalReadinessRequest) error {
		return assertStage4DeleteJournalReadinessNativePrerequisites(
			fixture,
			request,
		)
	}
	if err := runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		fixture.prepared,
		target,
	); err != nil {
		t.Fatalf("run durable readiness boundary: %v", err)
	}

	for _, pair := range [][2]string{
		{"delete_journal_preflight", "inventory_published"},
		{"inventory_published", "ordinary_tasks_bound"},
		{"ordinary_tasks_bound", "structured_work_bound"},
		{"structured_work_bound", "schema_evolution"},
		{"schema_evolution", "delete_journal_protector"},
		{"delete_journal_protector", "delete_journal_native_reread"},
		{"schema_evolution", "delete_journal_native_reread"},
		{"delete_journal_native_reread", "target_prepare"},
		{"delete_journal_native_reread", "target_write"},
		{"target_prepare", "target_write"},
	} {
		assertStage4AdapterEventBefore(t, events, pair[0], pair[1])
	}
	if target.protectorCalls != 1 || backend.boundaryCalls != 1 ||
		backend.ensureCalls != 1 {
		t.Fatalf(
			"fresh readiness checks protector=%d boundary=%d ensure=%d",
			target.protectorCalls,
			backend.boundaryCalls,
			backend.ensureCalls,
		)
	}
}

func TestStage4DeleteJournalReadinessRejectsUnsafeBoundaryBeforeNativePreparation(
	t *testing.T,
) {
	backend := &stage4DeleteJournalCommitThenErrorBackend{
		YAMLStore: state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
	events := make([]string, 0)
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	fixture := newStage4DeleteJournalReadinessFixture(t, backend, target)
	if err := backend.CompleteTask(
		fixture.run.RunID,
		fixture.table.Task.Table,
		1,
		time.Now().UTC(),
	); err != nil {
		t.Fatal(err)
	}
	if err := runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		fixture.prepared,
		target,
	); err == nil {
		t.Fatal("unsafe pre-receipt boundary unexpectedly reached target lifecycle")
	}
	if target.nativeCalls != 0 {
		t.Fatalf("native journal preparation calls=%d, want 0", target.nativeCalls)
	}
	if backend.boundaryCalls != 1 || backend.ensureCalls != 0 {
		t.Fatalf(
			"unsafe readiness state checks boundary=%d ensure=%d",
			backend.boundaryCalls,
			backend.ensureCalls,
		)
	}
	assertStage4DeleteJournalReadinessTargetUntouched(t, events)
}

func TestStage4DeleteJournalReadinessRejectsIncrementalAttemptBeforeNativePreparation(
	t *testing.T,
) {
	backend := &stage4DeleteJournalCommitThenErrorBackend{
		YAMLStore: state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
	events := make([]string, 0)
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	fixture := newStage4DeleteJournalReadinessFixture(t, backend, target)
	if _, created, err := backend.BeginIncrementalAttempt(state.IncrementalAttempt{
		RunID:     fixture.run.RunID,
		Task:      fixture.table.Task,
		AttemptID: "attempt-before-readiness",
		Mode:      state.IncrementalBaseline,
		StartedAt: fixture.started.Add(time.Second),
	}); err != nil || !created {
		t.Fatalf("begin incremental attempt created=%v err=%v", created, err)
	}
	if err := runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		fixture.prepared,
		target,
	); err == nil {
		t.Fatal("incremental attempt unexpectedly reached native readiness preparation")
	}
	if target.protectorCalls != 0 || target.nativeCalls != 0 ||
		backend.boundaryCalls != 1 || backend.ensureCalls != 0 {
		t.Fatalf(
			"incremental evidence crossed readiness boundary protector=%d native=%d boundary=%d ensure=%d",
			target.protectorCalls,
			target.nativeCalls,
			backend.boundaryCalls,
			backend.ensureCalls,
		)
	}
	assertStage4DeleteJournalReadinessTargetUntouched(t, events)
}

func TestStage4DeleteJournalReadinessRechecksLeaseBeforeNativePreparation(
	t *testing.T,
) {
	directory := t.TempDir()
	backend := &stage4DeleteJournalCommitThenErrorBackend{
		YAMLStore: state.YAMLStore{Path: filepath.Join(directory, "state.yaml")},
	}
	events := make([]string, 0)
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	fixture := newStage4DeleteJournalReadinessFixture(t, backend, target)
	leaseStore := state.SQLiteStore{Path: filepath.Join(directory, "leases.db")}
	lease, err := leaseStore.AcquireLease(
		"postgres:target.example:5432/app",
		fixture.run.RunID,
		time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	guard := state.NewLeaseGuard(leaseStore, lease)
	target.mutationProtect = func(
		ctx context.Context,
		mutation func() error,
	) error {
		*target.events = append(*target.events, "delete_journal_lease_takeover")
		if err := leaseStore.ReleaseLease(lease); err != nil {
			return err
		}
		if _, err := leaseStore.AcquireLease(
			lease.Target,
			"takeover-run",
			time.Minute,
		); err != nil {
			return err
		}
		return guard.Protect(ctx, mutation)
	}

	err = runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		fixture.prepared,
		target,
	)
	if err == nil || !errors.Is(err, state.ErrLeaseLost) {
		t.Fatalf("lease takeover readiness error = %v", err)
	}
	if target.protectorCalls != 1 || target.nativeCalls != 0 ||
		backend.boundaryCalls != 1 || backend.ensureCalls != 0 {
		t.Fatalf(
			"lease takeover crossed readiness boundary protector=%d native=%d boundary=%d ensure=%d",
			target.protectorCalls,
			target.nativeCalls,
			backend.boundaryCalls,
			backend.ensureCalls,
		)
	}
	if _, found, loadErr := backend.YAMLStore.LoadStage4DeleteJournalReadiness(
		fixture.run.RunID,
	); loadErr != nil || found {
		t.Fatalf("lease takeover persisted readiness found=%v err=%v", found, loadErr)
	}
	assertStage4AdapterEventBefore(
		t,
		events,
		"structured_work_bound",
		"delete_journal_protector",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"delete_journal_protector",
		"delete_journal_lease_takeover",
	)
	assertStage4DeleteJournalReadinessTargetUntouched(t, events)
}

func TestStage4DeleteJournalReadinessSentinelProgressStopsNativePreparation(
	t *testing.T,
) {
	tests := []struct {
		name        string
		targetShape bool
		advance     bool
	}{
		{name: "advanced schema sentinel", advance: true},
		{name: "completed schema sentinel"},
		{name: "advanced target-shape sentinel", targetShape: true, advance: true},
		{name: "completed target-shape sentinel", targetShape: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			backend := &stage4DeleteJournalCommitThenErrorBackend{
				YAMLStore: state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
			}
			events := make([]string, 0)
			target := newStage4DeleteJournalReadinessTestTarget(&events)
			fixture := newStage4DeleteJournalReadinessFixture(t, backend, target)
			task := state.WorkTask{
				RunID:        fixture.run.RunID,
				Key:          state.TaskKey{Type: "schema-contract", Table: "aggregate-source-schema"},
				Strategy:     "stage4_aggregate_schema_contract_v1",
				TopologyHash: "schema-topology",
				StartedAt:    fixture.started,
			}
			rangeID := "aggregate-schema"
			if test.targetShape {
				task = state.WorkTask{
					RunID: fixture.run.RunID,
					Key: state.TaskKey{
						Type: "target-schema-shape", Table: "aggregate-target-schema",
					},
					Strategy:     "stage4_target_shape_authority_v1",
					TopologyHash: "schema-topology",
					StartedAt:    fixture.started,
				}
				rangeID = "aggregate-target-shape"
				if _, err := backend.EnsureWorkPlan(
					task,
					[]state.RangeState{{ID: rangeID}},
				); err != nil {
					t.Fatal(err)
				}
			}
			if test.advance {
				stage4DeleteJournalReadinessAdvanceTestSentinel(
					t,
					backend,
					fixture.run.RunID,
					task,
					rangeID,
					fixture.started,
				)
			} else {
				stage4DeleteJournalReadinessCompleteTestSentinel(
					t,
					backend,
					fixture.run.RunID,
					task,
					rangeID,
					fixture.started,
				)
			}
			if err := runStage4DeleteJournalReadinessTestLifecycle(
				context.Background(),
				fixture.prepared,
				target,
			); err == nil {
				t.Fatal("progressed sentinel unexpectedly reached native readiness preparation")
			}
			if target.nativeCalls != 0 || backend.boundaryCalls != 1 ||
				backend.ensureCalls != 0 {
				t.Fatalf(
					"progressed sentinel crossed readiness boundary native=%d boundary=%d ensure=%d",
					target.nativeCalls,
					backend.boundaryCalls,
					backend.ensureCalls,
				)
			}
			assertStage4DeleteJournalReadinessTargetUntouched(t, events)
		})
	}
}

func stage4DeleteJournalReadinessAdvanceTestSentinel(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
	task state.WorkTask,
	rangeID string,
	started time.Time,
) {
	t.Helper()
	if err := backend.BeginRangeChunk(state.RangeChunkIntent{
		RunID:        runID,
		Task:         task.Key,
		RangeID:      rangeID,
		TopologyHash: task.TopologyHash,
		Sequence:     0,
		ChunkRows:    1,
		Exhausted:    true,
		At:           started.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.RecordRangeAttempt(state.RangeAttempt{
		RunID:        runID,
		Task:         task.Key,
		RangeID:      rangeID,
		TopologyHash: task.TopologyHash,
		Sequence:     0,
		At:           started.Add(2 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
}

func stage4DeleteJournalReadinessCompleteTestSentinel(
	t *testing.T,
	backend state.RangeBackend,
	runID string,
	task state.WorkTask,
	rangeID string,
	started time.Time,
) {
	t.Helper()
	completedAt := started.Add(time.Second)
	if err := backend.CompleteRange(
		runID,
		task.Key,
		rangeID,
		task.TopologyHash,
		0,
		completedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := backend.CompleteWorkTask(
		runID,
		task.Key,
		task.TopologyHash,
		completedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func TestStage4DeleteJournalReadinessCommitThenErrorResumeUsesStoredAuthority(
	t *testing.T,
) {
	backend := &stage4DeleteJournalCommitThenErrorBackend{
		YAMLStore:   state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
		returnError: true,
	}
	firstEvents := make([]string, 0)
	firstTarget := newStage4DeleteJournalReadinessTestTarget(&firstEvents)
	fixture := newStage4DeleteJournalReadinessFixture(t, backend, firstTarget)
	firstTarget.readyAt = fixture.started.Add(10 * time.Second)
	if err := runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		fixture.prepared,
		firstTarget,
	); err == nil {
		t.Fatal("commit-then-error readiness write unexpectedly succeeded")
	}
	if firstTarget.nativeCalls != 1 {
		t.Fatalf("first native journal preparation calls=%d", firstTarget.nativeCalls)
	}
	assertStage4DeleteJournalReadinessTargetUntouched(t, firstEvents)
	stored, found, err := backend.YAMLStore.LoadStage4DeleteJournalReadiness(
		fixture.run.RunID,
	)
	if err != nil || !found {
		t.Fatalf("committed readiness found=%v err=%v", found, err)
	}

	backend.returnError = false
	resumeEvents := make([]string, 0)
	resumeTarget := newStage4DeleteJournalReadinessTestTarget(&resumeEvents)
	resumeTarget.readyAt = stored.Readiness.ReadyAt.Add(time.Hour)
	prepared := fixture.prepared
	prepared.run.Resume = true
	prepared.deleteJournalReadiness =
		stage4DeleteJournalReadinessCapabilityForTarget(resumeTarget)
	if err := runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		prepared,
		resumeTarget,
	); err != nil {
		t.Fatalf("resume exact readiness: %v", err)
	}
	if len(resumeTarget.requests) != 1 ||
		resumeTarget.requests[0].Existing == nil ||
		!resumeTarget.requests[0].Existing.Equal(stored) {
		t.Fatalf("resume native reread request = %#v, stored=%#v", resumeTarget.requests, stored)
	}
	if len(resumeTarget.observed) != 1 ||
		!resumeTarget.observed[0].ReadyAt.Equal(resumeTarget.readyAt) ||
		resumeTarget.observed[0].ReadyAt.Equal(stored.Readiness.ReadyAt) {
		t.Fatalf(
			"resume native observation did not use a distinct reread time: %#v stored=%#v",
			resumeTarget.observed,
			stored,
		)
	}
	restored, found, err := backend.YAMLStore.LoadStage4DeleteJournalReadiness(
		fixture.run.RunID,
	)
	if err != nil || !found || !restored.Equal(stored) {
		t.Fatalf("resume receipt=%#v found=%v err=%v", restored, found, err)
	}
	assertStage4AdapterEventBefore(
		t,
		resumeEvents,
		"delete_journal_native_reread",
		"target_prepare",
	)
	assertStage4AdapterEventBefore(
		t,
		resumeEvents,
		"target_prepare",
		"target_write",
	)

	for name, mutate := range map[string]func(*stage4DeleteJournalReadinessTestTarget){
		"target": func(target *stage4DeleteJournalReadinessTestTarget) {
			target.targetIdentity = "postgres:other.example:5432/app"
		},
		"journal": func(target *stage4DeleteJournalReadinessTestTarget) {
			target.journalCatalog = "journal-catalog-v2"
		},
		"flavor": func(target *stage4DeleteJournalReadinessTestTarget) {
			target.targetFlavor = "libsql"
		},
		"version": func(target *stage4DeleteJournalReadinessTestTarget) {
			target.targetVersion = "3.46.0"
		},
		"inventory": func(target *stage4DeleteJournalReadinessTestTarget) {
			target.inventoryDigest = stateDigest("changed-inventory")
		},
	} {
		t.Run(name, func(t *testing.T) {
			events := make([]string, 0)
			target := newStage4DeleteJournalReadinessTestTarget(&events)
			mutate(target)
			candidate := prepared
			candidate.deleteJournalReadiness =
				stage4DeleteJournalReadinessCapabilityForTarget(target)
			if err := runStage4DeleteJournalReadinessTestLifecycle(
				context.Background(),
				candidate,
				target,
			); err == nil {
				t.Fatalf("changed %s authority unexpectedly reached target lifecycle", name)
			}
			if target.nativeCalls != 1 {
				t.Fatalf("changed %s native reread calls=%d", name, target.nativeCalls)
			}
			assertStage4DeleteJournalReadinessTargetUntouched(t, events)
		})
	}
	if backend.boundaryCalls != 1 || backend.ensureCalls != 2 {
		t.Fatalf(
			"commit-then-error state checks boundary=%d ensure=%d",
			backend.boundaryCalls,
			backend.ensureCalls,
		)
	}
}

func TestStage4DeleteJournalReadinessPostEvolutionCommitThenErrorResumes(
	t *testing.T,
) {
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			raw := newBackend(t)
			aggregate, ok := raw.(state.Stage4AggregateBackend)
			if !ok {
				t.Fatalf("%T lacks aggregate readiness state", raw)
			}
			readiness, ok := raw.(state.Stage4DeleteJournalReadinessBackend)
			if !ok {
				t.Fatalf("%T lacks delete-journal readiness state", raw)
			}
			backend := &stage4DeleteJournalEnsureFailureBackend{
				Backend:                             raw,
				Stage4StateBackend:                  raw,
				Stage4AggregateBackend:              aggregate,
				Stage4DeleteJournalReadinessBackend: readiness,
				returnError:                         true,
			}
			firstEvents := make([]string, 0)
			firstTarget := newStage4DeleteJournalReadinessTestTarget(
				&firstEvents,
			)
			fixture := newStage4DeleteJournalReadinessFixture(
				t,
				backend,
				firstTarget,
			)
			firstTarget.readyAt = fixture.started.Add(10 * time.Second)
			err := runStage4DeleteJournalReadinessTestLifecycle(
				context.Background(),
				fixture.prepared,
				firstTarget,
			)
			if err == nil || !strings.Contains(err.Error(), "commit-then-error") {
				t.Fatalf("post-evolution readiness receipt failure = %v", err)
			}
			assertStage4AdapterEventBefore(
				t,
				firstEvents,
				"schema_evolution",
				"delete_journal_native_reread",
			)
			assertStage4DeleteJournalReadinessTargetUntouched(t, firstEvents)
			stored, found, err := readiness.LoadStage4DeleteJournalReadiness(
				fixture.run.RunID,
			)
			if err != nil || !found {
				t.Fatalf("post-evolution committed readiness found=%v err=%v", found, err)
			}

			backend.returnError = false
			resumeEvents := make([]string, 0)
			resumeTarget := newStage4DeleteJournalReadinessTestTarget(
				&resumeEvents,
			)
			resumeTarget.readyAt = stored.Readiness.ReadyAt.Add(time.Hour)
			prepared := fixture.prepared
			prepared.run.Resume = true
			prepared.deleteJournalReadiness =
				stage4DeleteJournalReadinessCapabilityForTarget(resumeTarget)
			if err := runStage4DeleteJournalReadinessTestLifecycle(
				context.Background(),
				prepared,
				resumeTarget,
			); err != nil {
				t.Fatalf("resume post-evolution readiness receipt: %v", err)
			}
			if len(resumeTarget.requests) != 1 ||
				resumeTarget.requests[0].Existing == nil ||
				!resumeTarget.requests[0].Existing.Equal(stored) {
				t.Fatalf(
					"resume post-evolution native request=%#v stored=%#v",
					resumeTarget.requests,
					stored,
				)
			}
			resumed, found, err := readiness.LoadStage4DeleteJournalReadiness(
				fixture.run.RunID,
			)
			if err != nil || !found || !resumed.Equal(stored) {
				t.Fatalf("resume post-evolution receipt=%#v found=%v err=%v", resumed, found, err)
			}
			for _, pair := range [][2]string{
				{"schema_evolution", "delete_journal_native_reread"},
				{"delete_journal_native_reread", "target_prepare"},
				{"target_prepare", "target_write"},
			} {
				assertStage4AdapterEventBefore(
					t,
					resumeEvents,
					pair[0],
					pair[1],
				)
			}
		})
	}
}

func TestStage4DeleteJournalReadinessMissingBackendCapabilityFailsBeforeNativePreparation(
	t *testing.T,
) {
	raw := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	events := make([]string, 0)
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	fixture := newStage4DeleteJournalReadinessFixture(t, raw, target)
	missing := stage4DeleteJournalReadinessMissingBackend{
		Backend:                raw,
		RangeBackend:           raw,
		Stage4Backend:          raw,
		Stage4AggregateBackend: raw,
	}
	prepared := fixture.prepared
	prepared.run.Backend = missing
	if err := runStage4DeleteJournalReadinessTestLifecycle(
		context.Background(),
		prepared,
		target,
	); err == nil {
		t.Fatal("missing readiness backend capability unexpectedly reached target lifecycle")
	}
	if target.nativeCalls != 0 {
		t.Fatalf("missing capability native journal preparation calls=%d", target.nativeCalls)
	}
	assertStage4DeleteJournalReadinessTargetUntouched(t, events)
}

func TestStage4DeleteJournalReadinessMissingMutationProtectorFailsBeforeNativePreparation(
	t *testing.T,
) {
	backend := &stage4DeleteJournalCommitThenErrorBackend{
		YAMLStore: state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
	events := make([]string, 0)
	target := newStage4DeleteJournalReadinessTestTarget(&events)
	fixture := newStage4DeleteJournalReadinessFixture(t, backend, target)
	err := ensureStage4AdapterDeleteJournalReadiness(
		context.Background(),
		recordingTableObserver{events: &events},
		fixture.prepared,
	)
	if err == nil {
		t.Fatal("missing mutation protector unexpectedly reached native readiness preparation")
	}
	if target.nativeCalls != 0 || backend.boundaryCalls != 0 ||
		backend.ensureCalls != 0 {
		t.Fatalf(
			"missing protector crossed readiness boundary native=%d boundary=%d ensure=%d",
			target.nativeCalls,
			backend.boundaryCalls,
			backend.ensureCalls,
		)
	}
	assertStage4DeleteJournalReadinessTargetUntouched(t, events)
}

func assertStage4DeleteJournalReadinessPrecheckpointAdmissionUntouched(
	t *testing.T,
	backend state.YAMLStore,
	runID string,
	target *stage4DeleteJournalReadinessTestTarget,
	events []string,
) {
	t.Helper()
	if len(events) != 1 || events[0] != "delete_journal_preflight" {
		t.Fatalf("precheckpoint admission events=%v", events)
	}
	if inventory, found, err := backend.LoadStage4TableInventory(runID); err != nil || found {
		t.Fatalf(
			"precheckpoint admission inventory=%#v found=%v err=%v",
			inventory,
			found,
			err,
		)
	}
	if tasks, err := backend.ListTasks(runID); err != nil || len(tasks) != 0 {
		t.Fatalf("precheckpoint admission ordinary tasks=%#v err=%v", tasks, err)
	}
	if tasks, ranges, err := backend.ListWork(runID); err != nil ||
		len(tasks) != 0 || len(ranges) != 0 {
		t.Fatalf(
			"precheckpoint admission structured work=%#v ranges=%#v err=%v",
			tasks,
			ranges,
			err,
		)
	}
	if target.nativeCalls != 0 || len(target.prepared) != 0 ||
		len(target.written) != 0 {
		t.Fatalf(
			"precheckpoint admission reached target mutation native=%d prepare=%d write=%d",
			target.nativeCalls,
			len(target.prepared),
			len(target.written),
		)
	}
}

type stage4DeleteJournalReadinessFixture struct {
	run      Stage4RunContext
	prepared stage4AdapterPrepared
	table    state.Stage4TableInventoryEntry
	started  time.Time
}

func newStage4DeleteJournalReadinessFixture(
	t *testing.T,
	backend Stage4StateBackend,
	target *stage4DeleteJournalReadinessTestTarget,
) stage4DeleteJournalReadinessFixture {
	t.Helper()
	durable, ok := backend.(state.Backend)
	if !ok {
		t.Fatalf("%T cannot initialize a Stage 4 run", backend)
	}
	aggregate, ok := backend.(state.Stage4AggregateBackend)
	if !ok {
		t.Fatalf("%T lacks aggregate Stage 4 inventory", backend)
	}
	runID := "delete-journal-readiness"
	started := time.Now().UTC().Add(-time.Minute)
	initializeStage4LifecycleRun(t, durable, runID, started)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	schemaTask := state.TaskKey{
		Type: "schema-contract", Table: "aggregate-source-schema",
	}
	if _, err := backend.EnsureWorkPlan(state.WorkTask{
		RunID:        runID,
		Key:          schemaTask,
		Strategy:     "stage4_aggregate_schema_contract_v1",
		TopologyHash: "schema-topology",
		StartedAt:    started,
	}, []state.RangeState{{ID: "aggregate-schema"}}); err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(state.SchemaSnapshot{
		RunID:         runID,
		Task:          schemaTask,
		CanonicalJSON: `{"version":1}`,
		CapturedAt:    started.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, found, err := backend.LoadSchemaSnapshot(runID, schemaTask)
	if err != nil || !found {
		t.Fatalf("schema snapshot found=%v err=%v", found, err)
	}
	table := state.Stage4TableInventoryEntry{
		Table: "items",
		Task: state.TaskKey{
			Type: "network-table-copy", Schema: "public", Table: "items",
		},
		Strategy:     "network",
		TopologyHash: "table-topology",
		Ranges:       []state.Stage4InventoryRange{{ID: "0"}},
	}
	if err := aggregate.EnsureStage4TableInventory(state.Stage4TableInventory{
		RunID:                runID,
		SchemaTask:           schemaTask,
		SchemaTopologyHash:   "schema-topology",
		SchemaSnapshotDigest: snapshot.Digest,
		Tables:               []state.Stage4TableInventoryEntry{table},
	}); err != nil {
		t.Fatal(err)
	}
	*target.events = append(*target.events, "inventory_published")
	if err := durable.CreateTask(state.Task{
		RunID: runID, Table: table.Table, StartedAt: started,
	}); err != nil {
		t.Fatal(err)
	}
	*target.events = append(*target.events, "ordinary_tasks_bound")
	if _, err := backend.EnsureWorkPlan(state.WorkTask{
		RunID:        runID,
		Key:          table.Task,
		Strategy:     table.Strategy,
		TopologyHash: table.TopologyHash,
		StartedAt:    started,
	}, []state.RangeState{{ID: "0"}}); err != nil {
		t.Fatal(err)
	}
	*target.events = append(*target.events, "structured_work_bound")
	return stage4DeleteJournalReadinessFixture{
		run:     run,
		table:   table,
		started: started,
		prepared: stage4AdapterPrepared{
			run: run,
			deleteJournalReadiness: stage4DeleteJournalReadinessCapabilityForTarget(
				target,
			),
		},
	}
}

type stage4DeleteJournalReadinessTestTarget struct {
	*recordingAdapterTarget

	targetIdentity  string
	targetFlavor    string
	targetVersion   string
	journalCatalog  string
	inventoryDigest string
	preflightErr    error
	prepareErr      error
	readyAt         time.Time
	beforeNative    func(Stage4DeleteJournalReadinessRequest) error
	mutationProtect func(context.Context, func() error) error
	protectorCalls  int
	nativeCalls     int
	requests        []Stage4DeleteJournalReadinessRequest
	observed        []state.Stage4DeleteJournalReadiness
}

func newStage4DeleteJournalReadinessTestTarget(
	events *[]string,
) *stage4DeleteJournalReadinessTestTarget {
	return &stage4DeleteJournalReadinessTestTarget{
		recordingAdapterTarget: &recordingAdapterTarget{events: events},
		targetIdentity:         "postgres:target.example:5432/app",
		targetFlavor:           "sqlite",
		targetVersion:          "3.45.0",
		journalCatalog:         "journal-catalog-v1",
	}
}

func (target *stage4DeleteJournalReadinessTestTarget) PreflightStage4DeleteJournalReadiness(
	context.Context,
) error {
	*target.events = append(*target.events, "delete_journal_preflight")
	return target.preflightErr
}

func (target *stage4DeleteJournalReadinessTestTarget) PrepareStage4DeleteJournalReadiness(
	_ context.Context,
	request Stage4DeleteJournalReadinessRequest,
) (state.Stage4DeleteJournalReadiness, error) {
	target.nativeCalls++
	copyRequest := request
	if request.Existing != nil {
		existing := request.Existing.Clone()
		copyRequest.Existing = &existing
	}
	target.requests = append(target.requests, copyRequest)
	*target.events = append(*target.events, "delete_journal_native_reread")
	if target.prepareErr != nil {
		return state.Stage4DeleteJournalReadiness{}, target.prepareErr
	}
	if target.beforeNative != nil {
		if err := target.beforeNative(request); err != nil {
			return state.Stage4DeleteJournalReadiness{}, err
		}
	}
	inventoryDigest := request.InventoryDigest
	if target.inventoryDigest != "" {
		inventoryDigest = target.inventoryDigest
	}
	readyAt := target.readyAt
	if readyAt.IsZero() {
		readyAt = time.Now().UTC()
	}
	observed, err := state.NewStage4DeleteJournalReadiness(
		request.RunID,
		inventoryDigest,
		target.targetIdentity,
		target.Engine(),
		target.targetFlavor,
		target.targetVersion,
		stateDigest(target.journalCatalog),
		1,
		readyAt,
	)
	if err != nil {
		return state.Stage4DeleteJournalReadiness{}, err
	}
	target.observed = append(target.observed, observed.Clone())
	return observed, nil
}

func stage4DeleteJournalReadinessCapabilityForTarget(
	target *stage4DeleteJournalReadinessTestTarget,
) *stage4AdapterDeleteJournalReadinessCapability {
	return &stage4AdapterDeleteJournalReadinessCapability{
		targetEngine: target.Engine(),
		preparer:     target,
	}
}

func runStage4DeleteJournalReadinessTestLifecycle(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	target *stage4DeleteJournalReadinessTestTarget,
) error {
	observer := &stage4DeleteJournalReadinessTestObserver{target: target}
	// Production applies and exactly re-verifies table schema evolution after
	// immutable inventory/work binding, before it materializes the private
	// journal. The test harness records that completed schema boundary before it
	// calls readiness, then still proves ordinary table preparation/writes remain
	// unreachable until the durable receipt exists.
	*target.events = append(*target.events, "schema_evolution")
	if err := ensureStage4AdapterDeleteJournalReadiness(
		ctx,
		observer,
		prepared,
	); err != nil {
		return err
	}
	table := stage4AdapterTestTable()
	table.Schema = ""
	if err := target.PrepareTables(ctx, []schema.Table{table}, "upsert"); err != nil {
		return err
	}
	_, err := target.WriteBatch(
		ctx,
		table,
		[]string{"id", "payload"},
		"upsert",
		[][]any{{int64(1), "payload"}},
	)
	return err
}

type stage4DeleteJournalReadinessTestObserver struct {
	target *stage4DeleteJournalReadinessTestTarget
}

func (*stage4DeleteJournalReadinessTestObserver) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (*stage4DeleteJournalReadinessTestObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return nil
}

func (observer *stage4DeleteJournalReadinessTestObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	observer.target.protectorCalls++
	*observer.target.events = append(*observer.target.events, "delete_journal_protector")
	if observer.target.mutationProtect != nil {
		return observer.target.mutationProtect(ctx, mutation)
	}
	return mutation()
}

func assertStage4DeleteJournalReadinessNativePrerequisites(
	fixture stage4DeleteJournalReadinessFixture,
	request Stage4DeleteJournalReadinessRequest,
) error {
	aggregate, ok := fixture.run.Backend.(state.Stage4AggregateBackend)
	if !ok {
		return fmt.Errorf("missing aggregate backend")
	}
	inventory, found, err := aggregate.LoadStage4TableInventory(fixture.run.RunID)
	if err != nil || !found {
		return fmt.Errorf("load durable table inventory found=%v err=%v", found, err)
	}
	if request.RunID != fixture.run.RunID ||
		request.InventoryDigest != inventory.Digest {
		return fmt.Errorf("native readiness request is not bound to durable inventory")
	}
	durable, ok := fixture.run.Backend.(state.Backend)
	if !ok {
		return fmt.Errorf("missing ordinary task backend")
	}
	ordinary, err := durable.ListTasks(fixture.run.RunID)
	if err != nil {
		return fmt.Errorf("list durable ordinary tasks: %w", err)
	}
	if len(ordinary) != 1 || ordinary[0].Table != fixture.table.Table ||
		ordinary[0].Status != "running" || ordinary[0].RowsDone != 0 ||
		!ordinary[0].CompletedAt.IsZero() {
		return fmt.Errorf("ordinary tasks are not durably pristine: %#v", ordinary)
	}
	workTasks, ranges, err := fixture.run.Backend.ListWork(fixture.run.RunID)
	if err != nil {
		return fmt.Errorf("list durable structured work: %w", err)
	}
	var workFound, rangeFound bool
	for _, workTask := range workTasks {
		if workTask.Key != fixture.table.Task {
			continue
		}
		workFound = workTask.Status == "running" &&
			workTask.Strategy == fixture.table.Strategy &&
			workTask.TopologyHash == fixture.table.TopologyHash &&
			workTask.Attempts == 0 && workTask.Retries == 0 &&
			workTask.Error == "" && workTask.CompletedAt.IsZero()
	}
	for _, workRange := range ranges {
		if workRange.Task != fixture.table.Task || workRange.ID != "0" {
			continue
		}
		rangeFound = workRange.Status == "running" &&
			workRange.Strategy == fixture.table.Strategy &&
			workRange.TopologyHash == fixture.table.TopologyHash &&
			workRange.NextSequence == 0 && workRange.RowsDone == 0 &&
			workRange.Attempts == 0 && workRange.Retries == 0 &&
			len(workRange.Frontier) == 0 && !workRange.FrontierValid &&
			len(workRange.Pending) == 0 && workRange.CompletedAt.IsZero()
	}
	if !workFound || !rangeFound {
		return fmt.Errorf(
			"structured table work is not durably bound and pristine: tasks=%#v ranges=%#v",
			workTasks,
			ranges,
		)
	}
	if _, active, err := fixture.run.Backend.LoadActiveIncrementalAttempt(
		fixture.run.RunID,
		fixture.table.Task,
	); err != nil || active {
		return fmt.Errorf("incremental attempt exists before native readiness active=%v err=%v", active, err)
	}
	return nil
}

func assertStage4DeleteJournalReadinessTargetUntouched(
	t *testing.T,
	events []string,
) {
	t.Helper()
	for _, event := range events {
		if event == "target_prepare" || event == "target_write" {
			t.Fatalf("target mutation reached without durable matching readiness: %v", events)
		}
	}
}

type stage4DeleteJournalCommitThenErrorBackend struct {
	state.YAMLStore
	returnError   bool
	boundaryCalls int
	ensureCalls   int
}

func (backend *stage4DeleteJournalCommitThenErrorBackend) ValidateStage4DeleteJournalReadinessBoundary(
	boundary state.Stage4DeleteJournalReadinessBoundary,
) error {
	backend.boundaryCalls++
	return backend.YAMLStore.ValidateStage4DeleteJournalReadinessBoundary(boundary)
}

func (backend *stage4DeleteJournalCommitThenErrorBackend) EnsureStage4DeleteJournalReadiness(
	ready state.Stage4DeleteJournalReadiness,
) (state.Stage4DeleteJournalReadinessReceipt, bool, error) {
	backend.ensureCalls++
	receipt, created, err := backend.YAMLStore.EnsureStage4DeleteJournalReadiness(ready)
	if err != nil || !backend.returnError {
		return receipt, created, err
	}
	return receipt, created, errors.New("injected commit-then-error")
}

// stage4DeleteJournalEnsureFailureBackend keeps the built-in YAML and SQLite
// state implementations intact while deterministically simulating a receipt
// write that commits before reporting an error.
type stage4DeleteJournalEnsureFailureBackend struct {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
	state.Stage4DeleteJournalReadinessBackend
	returnError bool
}

func (backend *stage4DeleteJournalEnsureFailureBackend) EnsureStage4DeleteJournalReadiness(
	ready state.Stage4DeleteJournalReadiness,
) (state.Stage4DeleteJournalReadinessReceipt, bool, error) {
	receipt, created, err := backend.Stage4DeleteJournalReadinessBackend.
		EnsureStage4DeleteJournalReadiness(ready)
	if err != nil || !backend.returnError {
		return receipt, created, err
	}
	return receipt, created, errors.New("injected commit-then-error")
}

type stage4DeleteJournalReadinessMissingBackend struct {
	state.Backend
	state.RangeBackend
	state.Stage4Backend
	state.Stage4AggregateBackend
}

func stateDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
