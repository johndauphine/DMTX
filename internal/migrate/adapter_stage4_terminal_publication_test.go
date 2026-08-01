package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func assertStage4TerminalSchemaSentinels(
	t *testing.T,
	backend Stage4StateBackend,
	runID string,
	wantCompleted bool,
) {
	t.Helper()
	tasks, _, err := backend.ListWork(runID)
	if err != nil {
		t.Fatalf("list Stage 4 terminal schema sentinels: %v", err)
	}
	found := 0
	for _, task := range tasks {
		if _, sentinel := stage4SentinelRangeID(task.Key); !sentinel {
			continue
		}
		found++
		completed := task.Status == "completed" && !task.CompletedAt.IsZero()
		if completed != wantCompleted {
			t.Fatalf(
				"Stage 4 terminal schema sentinel %#v completed=%t, want %t",
				task.Key,
				completed,
				wantCompleted,
			)
		}
	}
	if found == 0 {
		t.Fatalf("run %q established no Stage 4 terminal schema sentinels", runID)
	}
}

func TestStage4AdapterTerminalSentinelsCompleteWithoutAggregateInventory(
	t *testing.T,
) {
	for name, newBackend := range stage4LifecycleBackendFactories() {
		name, newBackend := name, newBackend
		t.Run(name, func(t *testing.T) {
			backend := newBackend(t)
			runID := "stage4-terminal-sentinel-legacy-" + name
			started := time.Now().UTC().Add(-time.Minute)
			initializeStage4LifecycleRun(t, backend, runID, started)
			run := stage4LifecycleRunContext(t, backend, runID, false)
			const topology = "terminal-sentinel-legacy-topology"
			if _, err := backend.EnsureWorkPlan(state.WorkTask{
				RunID:        runID,
				Key:          stage4SchemaGateTask,
				Strategy:     stage4SchemaGateStrategy,
				TopologyHash: topology,
				StartedAt:    started,
			}, []state.RangeState{{
				ID:           stage4SchemaGateRangeID,
				Strategy:     stage4SchemaGateStrategy,
				TopologyHash: topology,
			}}); err != nil {
				t.Fatalf("seed legacy schema sentinel: %v", err)
			}
			if err := completeStage4AdapterTerminalSchemaGateSentinels(
				context.Background(),
				stage4AdapterPrepared{
					run:  run,
					gate: Stage4SchemaGateResult{Task: stage4SchemaGateTask, TopologyHash: topology},
				},
			); err != nil {
				t.Fatalf("complete legacy schema sentinel: %v", err)
			}
			assertStage4TerminalSchemaSentinels(t, backend, runID, true)
			published, err := PublishStage4RunCompletion(
				context.Background(),
				run,
				"legacy direct completion",
				time.Now().UTC(),
			)
			if err != nil || published {
				t.Fatalf(
					"legacy direct completion publication published=%t err=%v",
					published,
					err,
				)
			}
		})
	}
}
