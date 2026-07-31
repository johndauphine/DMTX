package migrate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func stage4RunCompletionConfig() config.Config {
	return config.Config{
		Source: config.Endpoint{
			Type:     "postgres",
			Host:     "source.example",
			Port:     5432,
			Database: "source",
		},
		Target: config.Endpoint{
			Type:     "postgres",
			Host:     "target.example",
			Port:     5432,
			Database: "target",
		},
		Migration: config.Migration{
			TargetMode:         "upsert",
			DateUpdatedColumns: []string{"updated_at"},
			Validation: config.ValidationPolicy{
				Mode:           config.ValidationCountOnly,
				FailOnMismatch: true,
				FailOnTimeout:  true,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
}

func stage4RunCompletionTable() schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
}

// runStage4RunCompletionIncremental drives the real date-based incremental route
// to a validated result, which is the only route that composes the aggregate
// evidence PublishStage4RunCompletion requires.
func runStage4RunCompletionIncremental(
	t *testing.T,
	backend state.YAMLStore,
	runID string,
) Stage4RunContext {
	t.Helper()
	events := make([]string, 0)
	first := time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	second := first.Add(time.Hour)
	source := &stage4IncrementalTestSource{
		events: &events,
		table:  stage4RunCompletionTable(),
		rows: []stage4IncrementalTestRow{
			{id: 1, payload: "first", updated: &first},
			{id: 2, payload: "second", updated: &second},
		},
	}
	target := &stage4IncrementalTestTarget{events: &events}
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	observer := stage4IncrementalTestObserver{
		events:  &events,
		backend: backend,
		run:     run,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		stage4RunCompletionConfig(),
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if !result.Validated {
		t.Fatalf("result = %#v", result)
	}
	return run
}

func stage4RunCompletionSentinelStatus(
	t *testing.T,
	backend state.YAMLStore,
	runID string,
) map[state.TaskKey]string {
	t.Helper()
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	status := make(map[state.TaskKey]string)
	for _, task := range tasks {
		if _, sentinel := stage4SentinelRangeID(task.Key); !sentinel {
			continue
		}
		status[task.Key] = task.Status
	}
	if len(status) == 0 {
		t.Fatalf("run %q established no schema sentinel", runID)
	}
	return status
}

func TestPublishStage4RunCompletionComposesIncrementalRoute(t *testing.T) {
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-run-completion"
	run := runStage4RunCompletionIncremental(t, backend, runID)

	// The route must leave the sentinels running so the aggregate publication
	// can complete them together with the run outcome.
	for task, status := range stage4RunCompletionSentinelStatus(
		t,
		backend,
		runID,
	) {
		if status != "running" {
			t.Fatalf(
				"sentinel %#v was completed before publication: %q",
				task,
				status,
			)
		}
	}
	latest, found, err := backend.Latest()
	if err != nil || !found {
		t.Fatalf("pre-publication run found=%v err=%v", found, err)
	}
	if latest.Outcome != state.Running || !latest.Resumable {
		t.Fatalf("pre-publication run = %#v", latest)
	}

	completedAt := time.Now().UTC()
	published, err := PublishStage4RunCompletion(
		context.Background(),
		run,
		"migration completed",
		completedAt,
	)
	if err != nil || !published {
		t.Fatalf("publish published=%v err=%v", published, err)
	}

	for task, status := range stage4RunCompletionSentinelStatus(
		t,
		backend,
		runID,
	) {
		if status != "completed" {
			t.Fatalf("sentinel %#v = %q after publication", task, status)
		}
	}
	latest, found, err = backend.Latest()
	if err != nil || !found {
		t.Fatalf("published run found=%v err=%v", found, err)
	}
	if latest.Outcome != state.Success ||
		latest.Resumable ||
		latest.Reason != "migration completed" ||
		!latest.EndedAt.Equal(completedAt) {
		t.Fatalf("published run = %#v", latest)
	}

	// Republishing the identical completion is the crash-replay path between the
	// durable success and the caller's terminal audit.
	published, err = PublishStage4RunCompletion(
		context.Background(),
		run,
		"migration completed",
		completedAt,
	)
	if err != nil || !published {
		t.Fatalf("replay published=%v err=%v", published, err)
	}
	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	successes := 0
	for _, record := range runs {
		if record.ID == runID && record.Outcome == state.Success {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("replay recorded %d successful outcomes", successes)
	}
}

func TestPublishStage4RunCompletionRejectsDivergentReplay(t *testing.T) {
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-run-completion-divergent"
	run := runStage4RunCompletionIncremental(t, backend, runID)
	completedAt := time.Now().UTC()
	if published, err := PublishStage4RunCompletion(
		context.Background(),
		run,
		"migration completed",
		completedAt,
	); err != nil || !published {
		t.Fatalf("publish published=%v err=%v", published, err)
	}
	if _, err := PublishStage4RunCompletion(
		context.Background(),
		run,
		"migration resumed and completed",
		completedAt,
	); err == nil {
		t.Fatal("divergent success reason was accepted")
	}
	if _, err := PublishStage4RunCompletion(
		context.Background(),
		run,
		"migration completed",
		completedAt.Add(time.Second),
	); err == nil {
		t.Fatal("divergent completion time was accepted")
	}
}

// TestPublishStage4RunCompletionSkipsUncomposedRoute proves the marker for an
// aggregate-composed route is the durable table inventory, so routes that never
// publish one keep recording success by their ordinary path.
func TestPublishStage4RunCompletionSkipsUncomposedRoute(t *testing.T) {
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-run-completion-uncomposed"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	published, err := PublishStage4RunCompletion(
		context.Background(),
		run,
		"migration completed",
		time.Now().UTC(),
	)
	if err != nil || published {
		t.Fatalf("uncomposed publish published=%v err=%v", published, err)
	}
	latest, found, err := backend.Latest()
	if err != nil || !found {
		t.Fatalf("run found=%v err=%v", found, err)
	}
	if latest.Outcome != state.Running || !latest.Resumable {
		t.Fatalf("uncomposed publish mutated the run: %#v", latest)
	}
}

func TestPublishStage4RunCompletionRequiresCompleteEvidence(t *testing.T) {
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-run-completion-evidence"
	run := runStage4RunCompletionIncremental(t, backend, runID)
	for _, test := range []struct {
		name        string
		reason      string
		completedAt time.Time
	}{
		{
			name:        "blank reason",
			completedAt: time.Now().UTC(),
		},
		{
			name:   "zero completion time",
			reason: "migration completed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PublishStage4RunCompletion(
				context.Background(),
				run,
				test.reason,
				test.completedAt,
			); err == nil {
				t.Fatal("incomplete lifecycle evidence was accepted")
			}
		})
	}
	latest, found, err := backend.Latest()
	if err != nil || !found {
		t.Fatalf("run found=%v err=%v", found, err)
	}
	if latest.Outcome != state.Running {
		t.Fatalf("rejected publication mutated the run: %#v", latest)
	}
}
