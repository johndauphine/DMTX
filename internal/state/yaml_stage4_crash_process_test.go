package state

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	yamlStage4CrashModeEnv  = "DMTX_YAML_STAGE4_CRASH_MODE"
	yamlStage4CrashStateEnv = "DMTX_YAML_STAGE4_CRASH_STATE"
	yamlStage4CrashEventEnv = "DMTX_YAML_STAGE4_CRASH_EVENT"
)

// seedStage4CrashDocument builds a YAML document carrying the full Stage 4
// evidence surface — run, ordinary task, structured work and ranges, schema
// sentinel and snapshot, and a table inventory — so the replacement under test
// moves a realistically sized document rather than the two-record one the
// original crash fixture used.
func seedStage4CrashDocument(t *testing.T, store YAMLStore) TaskKey {
	t.Helper()
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	runID := "yaml-stage4-crash"
	task := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
	if err := store.InitializeRun(Run{
		ID: runID, Source: "source", Target: "target",
		SourceEngine: "postgres", SourceIdentity: "postgres:source/database",
		TargetIdentity: "postgres:target/database",
		Outcome:        Running, Resumable: true, Reason: "running",
		StartedAt: started,
	}, "stage4-hash"); err != nil {
		t.Fatal(err)
	}
	snapshot := installStage4AggregateSchemaAuthority(
		t,
		store,
		store,
		runID,
		started,
	)
	if err := store.EnsureStage4TableInventory(Stage4TableInventory{
		RunID:                runID,
		SchemaTask:           stage4SchemaContractSentinelTask,
		SchemaTopologyHash:   "schema-topology",
		SchemaSnapshotDigest: snapshot.Digest,
		Tables: []Stage4TableInventoryEntry{{
			Table: task.Table, Task: task, Strategy: "tuple_keyset",
			TopologyHash: "table-topology",
			Ranges:       []Stage4InventoryRange{{ID: "0"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTask(Task{
		RunID: runID, Table: task.Table, StartedAt: started,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnsureWorkPlan(WorkTask{
		RunID: runID, Key: task, Strategy: "tuple_keyset",
		TopologyHash: "table-topology", StartedAt: started,
	}, []RangeState{{ID: "0"}}); err != nil {
		t.Fatal(err)
	}
	return task
}

// TestStage4YAMLAtomicReplacementCrashMatrix reruns the atomic-replacement
// guarantee against the expanded Stage 4 document. A larger document is written
// in more filesystem operations, so a partial write is likelier to be observable
// here than in the original two-record fixture; the invariant is unchanged, but
// the exposure is not.
func TestStage4YAMLAtomicReplacementCrashMatrix(t *testing.T) {
	for _, test := range []struct {
		name           string
		mode           string
		wantCompletion bool
	}{
		{
			name: "before replace keeps whole prior document",
			mode: "before-replace",
		},
		{
			name:           "after replace exposes whole new document",
			mode:           "after-replace",
			wantCompletion: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			statePath := filepath.Join(directory, "state.yaml")
			eventPath := filepath.Join(directory, "stage4-boundary")
			store := YAMLStore{Path: statePath}
			task := seedStage4CrashDocument(t, store)

			command := exec.Command(
				os.Args[0],
				"-test.run=^TestStage4YAMLCrashHelperProcess$",
			)
			command.Env = append(os.Environ(),
				yamlStage4CrashModeEnv+"="+test.mode,
				yamlStage4CrashStateEnv+"="+statePath,
				yamlStage4CrashEventEnv+"="+eventPath,
			)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			wait := make(chan error, 1)
			go func() { wait <- command.Wait() }()
			reaped := false
			t.Cleanup(func() {
				if reaped {
					return
				}
				_ = command.Process.Kill()
				<-wait
			})
			waitForYAMLCrashBoundary(t, eventPath, wait, &reaped, &output)
			if err := command.Process.Kill(); err != nil {
				waitErr := <-wait
				reaped = true
				t.Fatalf("kill writer: %v (wait %v)\n%s", err, waitErr, output.String())
			}
			if err := <-wait; err == nil {
				t.Fatalf("writer exited instead of being killed\n%s", output.String())
			}
			reaped = true

			// Whichever side of the replacement the kill landed on, every
			// Stage 4 record must still be readable and self-consistent. A torn
			// document typically fails to decode, or decodes with a receipt
			// whose digest no longer matches.
			restored := YAMLStore{Path: statePath}
			runs, err := restored.List()
			if err != nil {
				t.Fatalf("read runs after hard kill: %v", err)
			}
			if len(runs) != 1 || runs[0].Outcome != Running {
				t.Fatalf("runs after %s = %#v", test.mode, runs)
			}
			inventory, found, err := restored.LoadStage4TableInventory(
				"yaml-stage4-crash",
			)
			if err != nil || !found {
				t.Fatalf("inventory after %s: found=%v err=%v", test.mode, found, err)
			}
			if len(inventory.Inventory.Tables) != 1 ||
				inventory.Inventory.Tables[0].Task != task {
				t.Fatalf("inventory after %s = %#v", test.mode, inventory.Inventory)
			}
			snapshot, found, err := restored.LoadSchemaSnapshot(
				"yaml-stage4-crash",
				stage4SchemaContractSentinelTask,
			)
			if err != nil || !found || snapshot.Digest == "" {
				t.Fatalf("snapshot after %s: found=%v err=%v", test.mode, found, err)
			}
			receipts, err := restored.LoadStage4TableCompletions(
				"yaml-stage4-crash",
			)
			if err != nil {
				t.Fatalf("receipts after %s: %v", test.mode, err)
			}
			want := 0
			if test.wantCompletion {
				want = 1
			}
			if len(receipts) != want {
				t.Fatalf(
					"receipts after %s = %d, want %d",
					test.mode,
					len(receipts),
					want,
				)
			}
			if test.wantCompletion &&
				receipts[0].Completion.Task != task {
				t.Fatalf("receipt after %s = %#v", test.mode, receipts[0])
			}
		})
	}
}

func TestStage4YAMLCrashHelperProcess(t *testing.T) {
	mode := os.Getenv(yamlStage4CrashModeEnv)
	if mode == "" {
		return
	}
	eventPath := os.Getenv(yamlStage4CrashEventEnv)
	block := func() error {
		if err := os.WriteFile(eventPath, []byte("ready"), 0o600); err != nil {
			return err
		}
		for {
			time.Sleep(time.Hour)
		}
	}
	switch mode {
	case "before-replace":
		yamlStateBeforeReplace = func(string, string) error { return block() }
	case "after-replace":
		yamlStateAfterReplace = func(string) error { return block() }
	default:
		t.Fatalf("unknown Stage 4 YAML crash mode %q", mode)
	}
	store := YAMLStore{Path: os.Getenv(yamlStage4CrashStateEnv)}
	task := TaskKey{Type: "table-copy", Schema: "public", Table: "items"}
	if err := store.CompleteStage4Table(Stage4TableCompletion{
		RunID: "yaml-stage4-crash", Table: task.Table, Task: task,
		TopologyHash: "table-topology",
		Ranges:       []Stage4RangeCompletion{{ID: "0"}},
		CompletedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	t.Fatal("Stage 4 YAML replacement completed before hard kill")
}
