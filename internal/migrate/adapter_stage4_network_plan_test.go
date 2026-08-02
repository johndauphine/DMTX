package migrate

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4AdapterNetworkPlanTableWritesNoDurableWork pins the seam the stable
// network route needs in order to publish a Stage 4 table inventory. The
// inventory may only be established while no table work exists, so planning a
// table must stay free of durable writes until the caller commits it.
func TestStage4AdapterNetworkPlanTableWritesNoDurableWork(t *testing.T) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table:  stage4AdapterTestTable(),
	}
	target := &recordingAdapterTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-network-plan-only"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    run,
	}
	cfg := stage4AdapterTestConfig(
		t,
		"source-password",
		"target-password",
	)
	cfg.Migration.TargetMode = "upsert"
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
		t.Fatal(err)
	}
	resources := stage4NetworkRunnerResources()
	execution, err := admitStage4AdapterNetworkTransfer(
		context.Background(),
		cfg,
		observer,
		source,
		target,
		prepared,
		&resources,
	)
	if err != nil {
		t.Fatal(err)
	}

	planned, err := execution.planTable(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.ranges) == 0 {
		t.Fatal("planned table produced no range inventory")
	}
	plannedTask := planned.work.task
	plannedRanges := len(planned.ranges)
	if err := planned.session.Close(); err != nil {
		t.Fatal(err)
	}

	// Planning must leave the durable work plan untouched, and must not have
	// advanced the shared global range offset that only a committed table owns.
	tasks, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if _, sentinel := stage4SentinelRangeID(task.Key); sentinel {
			continue
		}
		t.Fatalf("planning wrote durable table work %#v", task.Key)
	}
	for _, workRange := range ranges {
		if _, sentinel := stage4SentinelRangeID(workRange.Task); sentinel {
			continue
		}
		t.Fatalf("planning wrote durable table range %q", workRange.ID)
	}
	execution.mu.Lock()
	offset := execution.nextGlobalRange
	execution.mu.Unlock()
	if offset != 0 {
		t.Fatalf("planning advanced the global range offset to %d", offset)
	}

	// Committing the same table through openTable publishes the identical work
	// identity, so the inventory a caller derives from planning stays exact.
	tableExecution, err := execution.openTable(context.Background(), 0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tableExecution.Close(); err != nil {
			t.Error(err)
		}
	}()
	if tableExecution.work.task != plannedTask ||
		len(tableExecution.ranges) != plannedRanges {
		t.Fatalf(
			"committed work %#v/%d differs from planned %#v/%d",
			tableExecution.work.task,
			len(tableExecution.ranges),
			plannedTask,
			plannedRanges,
		)
	}
	tasks, _, err = backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, task := range tasks {
		if task.Key == plannedTask {
			found = true
		}
	}
	if !found {
		t.Fatalf("committed table work %#v is not durable", plannedTask)
	}
}
