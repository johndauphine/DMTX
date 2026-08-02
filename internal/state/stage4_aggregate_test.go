package state

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

type stage4AggregateFactory func(*testing.T) (Backend, func() Backend)

type stage4AggregateFixture struct {
	backend    Backend
	reopen     func() Backend
	aggregate  Stage4AggregateBackend
	stage4     Stage4Backend
	runID      string
	task       TaskKey
	started    time.Time
	completion Stage4TableCompletion
}

func TestStage4AggregateCompletionConformance(t *testing.T) {
	factories := map[string]stage4AggregateFactory{
		"sqlite": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.db")
			return SQLiteStore{Path: path}, func() Backend {
				return SQLiteStore{Path: path}
			}
		},
		"yaml": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.yaml")
			return YAMLStore{Path: path}, func() Backend {
				return YAMLStore{Path: path}
			}
		},
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Run("table incremental completion and replay", func(t *testing.T) {
				testStage4AggregateTableCompletion(t, factory)
			})
			t.Run("table mismatch and partial evidence", func(t *testing.T) {
				testStage4AggregateTableRejections(t, factory)
			})
			t.Run("table failure is atomic", func(t *testing.T) {
				testStage4AggregateTableFailureAtomicity(t, factory)
			})
			t.Run("run completion and replay", func(t *testing.T) {
				testStage4AggregateRunCompletion(t, factory)
			})
			t.Run("run explicit empty inventory", func(t *testing.T) {
				testStage4AggregateEmptyRunCompletion(t, factory)
			})
			t.Run("run mismatch is atomic", func(t *testing.T) {
				testStage4AggregateRunMismatch(t, factory)
			})
			t.Run("run rejects malformed delete evidence", func(t *testing.T) {
				testStage4AggregateRunDeleteEvidence(t, factory)
			})
			t.Run("run requires table-work bijection", func(t *testing.T) {
				testStage4AggregateRunTableBijection(t, factory)
			})
			t.Run("causal timestamps", func(t *testing.T) {
				testStage4AggregateCausalTimestamps(t, factory)
			})
			t.Run("run failure is atomic", func(t *testing.T) {
				testStage4AggregateRunFailureAtomicity(t, factory)
			})
		})
	}
}

func testStage4AggregateTableCompletion(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, true)
	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatal(err)
	}
	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatalf("identical table replay: %v", err)
	}
	restored := fixture.reopen()
	tasks, err := restored.ListTasks(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 ||
		tasks[0].Status != "completed" ||
		tasks[0].RowsDone != 0 ||
		!tasks[0].CompletedAt.Equal(fixture.completion.CompletedAt) {
		t.Fatalf("ordinary table completion = %#v", tasks)
	}
	workTasks, ranges, err := restored.(RangeBackend).ListWork(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	var tableWork WorkTask
	var tableRange RangeState
	for _, task := range workTasks {
		if task.Key == fixture.task {
			tableWork = task
		}
	}
	for _, workRange := range ranges {
		if workRange.Task == fixture.task {
			tableRange = workRange
		}
	}
	if tableWork.Status != "completed" ||
		tableRange.Status != "completed" {
		t.Fatalf("structured table completion = %#v %#v", workTasks, ranges)
	}
	incremental, found, err := restored.(Stage4Backend).
		LoadIncrementalAttempt(fixture.runID, fixture.task, "attempt-1")
	if err != nil || !found ||
		incremental.Status != IncrementalCompleted ||
		!incremental.TableSucceeded ||
		!incremental.CompletedAt.Equal(fixture.completion.CompletedAt) {
		t.Fatalf(
			"incremental completion = %#v found=%v err=%v",
			incremental,
			found,
			err,
		)
	}
	different := fixture.completion
	different.CompletedAt = different.CompletedAt.Add(time.Second)
	different.Incremental = &IncrementalCommit{
		RunID: different.RunID, Task: different.Task,
		AttemptID: "attempt-1", TopologyHash: different.TopologyHash,
		CompletedAt: different.CompletedAt,
	}
	err = fixture.aggregate.CompleteStage4Table(different)
	if !errors.Is(err, ErrState) ||
		!errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("different table replay error = %v", err)
	}
}

func testStage4AggregateTableRejections(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	t.Run("unknown", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		unknown := fixture.completion
		unknown.Table = "missing"
		unknown.Task.Table = "missing"
		err := fixture.aggregate.CompleteStage4Table(unknown)
		if !errors.Is(err, ErrState) || !errors.Is(err, ErrUnknownWork) {
			t.Fatalf("unknown table error = %v", err)
		}
		assertStage4AggregateTableRunning(t, fixture)
	})
	t.Run("topology", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		changed := fixture.completion
		changed.TopologyHash = "other-topology"
		err := fixture.aggregate.CompleteStage4Table(changed)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrTopologyChanged) {
			t.Fatalf("topology error = %v", err)
		}
		assertStage4AggregateTableRunning(t, fixture)
	})
	t.Run("partial ordinary completion", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		if err := fixture.backend.CompleteTask(
			fixture.runID,
			fixture.task.Table,
			0,
			fixture.completion.CompletedAt,
		); err != nil {
			t.Fatal(err)
		}
		err := fixture.aggregate.CompleteStage4Table(fixture.completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("partial table evidence error = %v", err)
		}
		workTasks, ranges, listErr :=
			fixture.backend.(RangeBackend).ListWork(fixture.runID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		if workTasks[0].Status != "running" ||
			ranges[0].Status != "running" {
			t.Fatalf("partial rejection changed structured state: %#v %#v", workTasks, ranges)
		}
	})
	t.Run("legacy terminal fields lack receipt", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		if err := fixture.backend.(RangeBackend).CompleteRange(
			fixture.runID,
			fixture.task,
			"0",
			fixture.completion.TopologyHash,
			0,
			fixture.completion.CompletedAt,
		); err != nil {
			t.Fatal(err)
		}
		if err := fixture.backend.(RangeBackend).CompleteWorkTask(
			fixture.runID,
			fixture.task,
			fixture.completion.TopologyHash,
			fixture.completion.CompletedAt,
		); err != nil {
			t.Fatal(err)
		}
		if err := fixture.backend.CompleteTask(
			fixture.runID,
			fixture.task.Table,
			0,
			fixture.completion.CompletedAt,
		); err != nil {
			t.Fatal(err)
		}
		err := fixture.aggregate.CompleteStage4Table(fixture.completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("missing aggregate receipt error = %v", err)
		}
	})
	t.Run("tampered receipt", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		if err := fixture.aggregate.CompleteStage4Table(
			fixture.completion,
		); err != nil {
			t.Fatal(err)
		}
		tamperStage4AggregateReceipt(
			t,
			fixture.backend,
			fixture.runID,
			fixture.task,
		)
		err := fixture.aggregate.CompleteStage4Table(fixture.completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("tampered aggregate receipt error = %v", err)
		}
	})
}

func testStage4AggregateTableFailureAtomicity(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, true)
	injected := errors.New("aggregate table commit failure")
	previous := stage4BeforeAggregateTableCommit
	stage4BeforeAggregateTableCommit = func() error { return injected }
	t.Cleanup(func() { stage4BeforeAggregateTableCommit = previous })
	err := fixture.aggregate.CompleteStage4Table(fixture.completion)
	stage4BeforeAggregateTableCommit = previous
	if !errors.Is(err, injected) || !errors.Is(err, ErrState) {
		t.Fatalf("injected table error = %v", err)
	}
	assertStage4AggregateTableRunning(t, fixture)
	attempt, found, err := fixture.reopen().(Stage4Backend).
		LoadIncrementalAttempt(fixture.runID, fixture.task, "attempt-1")
	if err != nil || !found ||
		attempt.Status != IncrementalRunning ||
		attempt.TableSucceeded {
		t.Fatalf(
			"failed table commit changed incremental = %#v found=%v err=%v",
			attempt,
			found,
			err,
		)
	}
}

func testStage4AggregateRunCompletion(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture, completion := newStage4AggregateRunFixture(t, factory)
	if err := fixture.aggregate.CompleteStage4Run(completion); err != nil {
		t.Fatal(err)
	}
	if err := fixture.aggregate.CompleteStage4Run(completion); err != nil {
		t.Fatalf("identical run replay: %v", err)
	}
	runs, err := fixture.reopen().List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 ||
		runs[1].Outcome != Success ||
		runs[1].Resumable ||
		runs[1].Reason != completion.Reason ||
		!runs[1].EndedAt.Equal(completion.CompletedAt) {
		t.Fatalf("published run outcomes = %#v", runs)
	}
	workTasks, ranges, err := fixture.reopen().(RangeBackend).
		ListWork(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, sentinel := range completion.Sentinels {
		var sentinelTask WorkTask
		var sentinelRange RangeState
		for _, task := range workTasks {
			if task.Key == sentinel.Task {
				sentinelTask = task
			}
		}
		for _, workRange := range ranges {
			if workRange.Task == sentinel.Task {
				sentinelRange = workRange
			}
		}
		if sentinelTask.Status != "completed" ||
			sentinelRange.Status != "completed" ||
			!sentinelTask.CompletedAt.Equal(completion.CompletedAt) ||
			!sentinelRange.CompletedAt.Equal(completion.CompletedAt) {
			t.Fatalf(
				"published schema sentinel = %#v %#v",
				sentinelTask,
				sentinelRange,
			)
		}
	}
	different := completion
	different.Reason = "different success"
	err = fixture.aggregate.CompleteStage4Run(different)
	if !errors.Is(err, ErrState) ||
		!errors.Is(err, ErrImmutableEvidence) {
		t.Fatalf("different run replay error = %v", err)
	}
}

func testStage4AggregateEmptyRunCompletion(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	backend, reopen := factory(t)
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	runID := "empty-aggregate-run"
	if err := backend.InitializeRun(Run{
		ID: runID, Source: "source", Target: "target",
		SourceEngine:   "postgres",
		SourceIdentity: "postgres:source/empty",
		TargetIdentity: "postgres:target/empty",
		Outcome:        Running, Resumable: true, Reason: "running",
		StartedAt: started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}
	fixture := stage4AggregateFixture{
		backend: backend, reopen: reopen,
		aggregate: backend.(Stage4AggregateBackend),
		stage4:    backend.(Stage4Backend),
		runID:     runID, started: started,
	}
	snapshot, completion := addStage4AggregateSentinel(t, fixture)
	if err := fixture.aggregate.CompleteStage4Run(
		completion,
	); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("implicit empty inventory error = %v", err)
	}
	if err := fixture.aggregate.EnsureStage4TableInventory(
		Stage4TableInventory{
			RunID:                runID,
			SchemaTask:           stage4SchemaContractSentinelTask,
			SchemaTopologyHash:   "schema-topology",
			SchemaSnapshotDigest: snapshot.Digest,
		},
	); err != nil {
		t.Fatalf("record explicit empty inventory: %v", err)
	}
	if err := fixture.aggregate.CompleteStage4Run(completion); err != nil {
		t.Fatalf("explicit empty run completion: %v", err)
	}
	runs, err := reopen().List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[1].Outcome != Success {
		t.Fatalf("empty run outcomes = %#v", runs)
	}
}

func testStage4AggregateRunMismatch(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	t.Run("snapshot evidence", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		completion.Sentinels[0].Snapshot.CapturedAt =
			completion.Sentinels[0].Snapshot.CapturedAt.Add(time.Second)
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("snapshot mismatch error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("sentinel inventory", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		extraTask := TaskKey{
			Type: "unexpected-shape", Table: "__unexpected_shape__",
		}
		if _, err := fixture.backend.(RangeBackend).EnsureWorkPlan(WorkTask{
			RunID: fixture.runID, Key: extraTask,
			Strategy: "target-sentinel", TopologyHash: "target-topology",
			StartedAt: fixture.started,
		}, []RangeState{{ID: "target"}}); err != nil {
			t.Fatal(err)
		}
		extra, err := normalizeSchemaSnapshot(SchemaSnapshot{
			RunID: fixture.runID, Task: extraTask,
			CanonicalJSON: `{"target":1}`,
			CapturedAt:    fixture.started.Add(91 * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := fixture.stage4.SaveSchemaSnapshot(extra); err != nil {
			t.Fatal(err)
		}
		err = fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("sentinel inventory error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("unknown completed work without snapshot", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		unknown := TaskKey{
			Type: "unexpected-lifecycle", Table: "__unexpected_lifecycle__",
		}
		if _, err := fixture.backend.(RangeBackend).EnsureWorkPlan(WorkTask{
			RunID: fixture.runID, Key: unknown,
			Strategy: "unexpected", TopologyHash: "unexpected-topology",
			StartedAt: fixture.started,
		}, []RangeState{{ID: "unexpected"}}); err != nil {
			t.Fatal(err)
		}
		completedAt := fixture.started.Add(70 * time.Second)
		if err := fixture.backend.(RangeBackend).CompleteRange(
			fixture.runID,
			unknown,
			"unexpected",
			"unexpected-topology",
			0,
			completedAt,
		); err != nil {
			t.Fatal(err)
		}
		if err := fixture.backend.(RangeBackend).CompleteWorkTask(
			fixture.runID,
			unknown,
			"unexpected-topology",
			completedAt,
		); err != nil {
			t.Fatal(err)
		}
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("unknown completed work error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("omitted precompleted target sentinel", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		target := completion.Sentinels[1]
		if err := fixture.backend.(RangeBackend).CompleteRange(
			fixture.runID,
			target.Task,
			target.RangeID,
			target.TopologyHash,
			target.NextSequence,
			fixture.started.Add(90*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		if err := fixture.backend.(RangeBackend).CompleteWorkTask(
			fixture.runID,
			target.Task,
			target.TopologyHash,
			fixture.started.Add(90*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		completion.Sentinels = completion.Sentinels[:1]
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("omitted sentinel error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("wrong sentinel type", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		completion.Sentinels[0].Task = TaskKey{
			Type: "other-schema", Table: "aggregate-source-schema",
		}
		completion.Sentinels[0].Snapshot.Task =
			completion.Sentinels[0].Task
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("wrong sentinel type error = %v", err)
		}
	})
	t.Run("malformed durable sentinel", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		schemaSentinel := completion.Sentinels[0]
		if err := fixture.backend.(RangeBackend).ResetWorkPlan(WorkTask{
			RunID: fixture.runID, Key: schemaSentinel.Task,
			Strategy:     "unexpected-sentinel-strategy",
			TopologyHash: schemaSentinel.TopologyHash,
			StartedAt:    fixture.started,
		}, []RangeState{{ID: schemaSentinel.RangeID}}); err != nil {
			t.Fatal(err)
		}
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("malformed durable sentinel error = %v", err)
		}
	})
}

func testStage4AggregateRunDeleteEvidence(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	tests := []struct {
		name   string
		record func(stage4AggregateFixture) DeleteReconciliation
	}{
		{
			name: "unknown status",
			record: func(fixture stage4AggregateFixture) DeleteReconciliation {
				return DeleteReconciliation{
					RunID: fixture.runID, Task: fixture.task,
					AttemptID: "unknown-status", Due: true,
					Status:    DeleteReconciliationStatus("unknown"),
					StartedAt: fixture.started,
				}
			},
		},
		{
			name: "missing completion",
			record: func(fixture stage4AggregateFixture) DeleteReconciliation {
				return DeleteReconciliation{
					RunID: fixture.runID, Task: fixture.task,
					AttemptID: "missing-completion", Due: true,
					Status:    DeleteReconciliationCompleted,
					StartedAt: fixture.started,
				}
			},
		},
		{
			name: "invalid counts",
			record: func(fixture stage4AggregateFixture) DeleteReconciliation {
				return DeleteReconciliation{
					RunID: fixture.runID, Task: fixture.task,
					AttemptID: "invalid-counts", Due: true,
					Status:     DeleteReconciliationCompleted,
					Candidates: 1, DeletedRows: 2,
					StartedAt:   fixture.started,
					CompletedAt: fixture.started.Add(time.Minute),
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture, completion := newStage4AggregateRunFixture(t, factory)
			injectStage4AggregateDelete(
				t,
				fixture.backend,
				test.record(fixture),
			)
			err := fixture.aggregate.CompleteStage4Run(completion)
			if !errors.Is(err, ErrState) ||
				!errors.Is(err, ErrStateTransition) {
				t.Fatalf("malformed delete error = %v", err)
			}
			assertStage4AggregateRunRunning(
				t,
				fixture,
				completion.Sentinels[0].Task,
			)
		})
	}
}

func testStage4AggregateRunTableBijection(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	t.Run("missing structured table", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		removeStage4AggregateWork(
			t,
			fixture.backend,
			fixture.runID,
			fixture.task,
		)
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("missing structured table error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("orphan structured table", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		orphan := TaskKey{
			Type: "network-table-copy", Schema: "public", Table: "orphan",
		}
		if _, err := fixture.backend.(RangeBackend).EnsureWorkPlan(WorkTask{
			RunID: fixture.runID, Key: orphan, Strategy: "tuple_keyset",
			TopologyHash: "orphan-topology", StartedAt: fixture.started,
		}, []RangeState{{ID: "0"}}); err != nil {
			t.Fatal(err)
		}
		if err := fixture.backend.(RangeBackend).CompleteRange(
			fixture.runID,
			orphan,
			"0",
			"orphan-topology",
			0,
			fixture.started.Add(70*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		if err := fixture.backend.(RangeBackend).CompleteWorkTask(
			fixture.runID,
			orphan,
			"orphan-topology",
			fixture.started.Add(70*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("orphan structured table error = %v", err)
		}
	})
	t.Run("legacy partial completion", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		if err := fixture.backend.CompleteTask(
			fixture.runID,
			fixture.task.Table,
			0,
			fixture.completion.CompletedAt,
		); err != nil {
			t.Fatal(err)
		}
		_, completion := addStage4AggregateSentinel(t, fixture)
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("partial run evidence error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("empty acknowledgement with table evidence", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		completion.Tables = nil
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrImmutableEvidence) {
			t.Fatalf("false empty inventory error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
}

func testStage4AggregateCausalTimestamps(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	t.Run("table precedes start", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		completion := fixture.completion
		completion.CompletedAt = fixture.started.Add(-time.Second)
		err := fixture.aggregate.CompleteStage4Table(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("early table completion error = %v", err)
		}
		assertStage4AggregateTableRunning(t, fixture)
	})
	t.Run("table precedes range", func(t *testing.T) {
		fixture := newStage4AggregateFixture(t, factory, false)
		if err := fixture.backend.(RangeBackend).CompleteRange(
			fixture.runID,
			fixture.task,
			"0",
			fixture.completion.TopologyHash,
			0,
			fixture.started.Add(2*time.Minute),
		); err != nil {
			t.Fatal(err)
		}
		err := fixture.aggregate.CompleteStage4Table(fixture.completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("early table frontier error = %v", err)
		}
		tasks, listErr := fixture.reopen().ListTasks(fixture.runID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		workTasks, ranges, listErr := fixture.reopen().(RangeBackend).
			ListWork(fixture.runID)
		if listErr != nil {
			t.Fatal(listErr)
		}
		var tableWork WorkTask
		var tableRange RangeState
		for _, task := range workTasks {
			if task.Key == fixture.task {
				tableWork = task
			}
		}
		for _, workRange := range ranges {
			if workRange.Task == fixture.task {
				tableRange = workRange
			}
		}
		if tasks[0].Status != "running" ||
			tableWork.Status != "running" ||
			tableRange.Status != "completed" {
			t.Fatalf("causal rejection changed state: %#v %#v %#v", tasks, workTasks, ranges)
		}
	})
	t.Run("run precedes table", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		completion.CompletedAt = fixture.started.Add(30 * time.Second)
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("early run completion error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
	t.Run("run precedes delete completion", func(t *testing.T) {
		fixture, completion := newStage4AggregateRunFixture(t, factory)
		injectStage4AggregateDelete(t, fixture.backend, DeleteReconciliation{
			RunID: fixture.runID, Task: fixture.task,
			AttemptID: "late-delete", Due: false,
			Status: DeleteReconciliationNotDue,
			Reason: "not due", StartedAt: fixture.started,
			CompletedAt: fixture.started.Add(3 * time.Minute),
		})
		err := fixture.aggregate.CompleteStage4Run(completion)
		if !errors.Is(err, ErrState) ||
			!errors.Is(err, ErrStateTransition) {
			t.Fatalf("early run delete error = %v", err)
		}
		assertStage4AggregateRunRunning(
			t,
			fixture,
			completion.Sentinels[0].Task,
		)
	})
}

func testStage4AggregateRunFailureAtomicity(
	t *testing.T,
	factory stage4AggregateFactory,
) {
	t.Helper()
	fixture, completion := newStage4AggregateRunFixture(t, factory)
	injected := errors.New("aggregate run commit failure")
	previous := stage4BeforeAggregateRunCommit
	stage4BeforeAggregateRunCommit = func() error { return injected }
	t.Cleanup(func() { stage4BeforeAggregateRunCommit = previous })
	err := fixture.aggregate.CompleteStage4Run(completion)
	stage4BeforeAggregateRunCommit = previous
	if !errors.Is(err, injected) || !errors.Is(err, ErrState) {
		t.Fatalf("injected run error = %v", err)
	}
	assertStage4AggregateRunRunning(t, fixture, completion.Sentinels[0].Task)
}

func TestStage4AggregateCompletionRejectsStaleLease(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			directory := t.TempDir()
			var raw Backend
			if stateKind == "sqlite" {
				raw = SQLiteStore{Path: filepath.Join(directory, "state.db")}
			} else {
				raw = YAMLStore{Path: filepath.Join(directory, "state.yaml")}
			}
			fixture := initializeStage4AggregateFixture(t, raw, func() Backend {
				if stateKind == "sqlite" {
					return SQLiteStore{Path: filepath.Join(directory, "state.db")}
				}
				return YAMLStore{Path: filepath.Join(directory, "state.yaml")}
			}, false)
			leaseStore := SQLiteStore{Path: filepath.Join(directory, "lease.db")}
			lease, err := leaseStore.AcquireLease(
				"postgres:target",
				fixture.runID,
				time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			if err := raw.BindRunLease(fixture.runID, lease); err != nil {
				t.Fatal(err)
			}
			fenced, ok := FenceBackend(
				raw,
				NewLeaseGuard(leaseStore, lease),
			).(Stage4AggregateBackend)
			if !ok {
				t.Fatalf("%T lacks fenced aggregate capability", raw)
			}
			_, runCompletion := addStage4AggregateSentinel(t, fixture)

			database, err := leaseStore.Open()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := database.Exec(
				`UPDATE leases SET heartbeat_at = ? WHERE target = ?`,
				time.Unix(0, 0).UTC(),
				lease.Target,
			); err != nil {
				database.Close()
				t.Fatal(err)
			}
			database.Close()
			if _, err := leaseStore.AcquireLease(
				lease.Target,
				"replacement-run",
				time.Second,
			); err != nil {
				t.Fatal(err)
			}
			if err := fenced.CompleteStage4Table(
				fixture.completion,
			); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale table completion error = %v", err)
			}
			if err := fenced.CompleteStage4Run(
				runCompletion,
			); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale run completion error = %v", err)
			}
			assertStage4AggregateTableRunning(t, fixture)
			runs, err := raw.List()
			if err != nil {
				t.Fatal(err)
			}
			if len(runs) != 1 || runs[0].Outcome != Running {
				t.Fatalf("stale completion changed run = %#v", runs)
			}
		})
	}
}

func newStage4AggregateFixture(
	t *testing.T,
	factory stage4AggregateFactory,
	incremental bool,
) stage4AggregateFixture {
	t.Helper()
	backend, reopen := factory(t)
	return initializeStage4AggregateFixture(t, backend, reopen, incremental)
}

func initializeStage4AggregateFixture(
	t *testing.T,
	backend Backend,
	reopen func() Backend,
	incremental bool,
) stage4AggregateFixture {
	t.Helper()
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	runID := "aggregate-run"
	task := TaskKey{
		Type: "table-copy", Schema: "public", Table: "items",
	}
	if err := backend.InitializeRun(Run{
		ID: runID, Source: "source", Target: "target",
		SourceEngine: "postgres", SourceIdentity: "postgres:source/database",
		TargetIdentity: "postgres:target/database",
		Outcome:        Running, Resumable: true, Reason: "running",
		StartedAt: started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}
	stage4 := backend.(Stage4Backend)
	aggregate, ok := backend.(Stage4AggregateBackend)
	if !ok {
		t.Fatalf("%T does not implement Stage4AggregateBackend", backend)
	}
	schemaSnapshot := installStage4AggregateSchemaAuthority(
		t,
		backend,
		stage4,
		runID,
		started,
	)
	if err := aggregate.EnsureStage4TableInventory(Stage4TableInventory{
		RunID:                runID,
		SchemaTask:           stage4SchemaContractSentinelTask,
		SchemaTopologyHash:   "schema-topology",
		SchemaSnapshotDigest: schemaSnapshot.Digest,
		Tables: []Stage4TableInventoryEntry{{
			Table: task.Table, Task: task, Strategy: "tuple_keyset",
			TopologyHash: "table-topology",
			Ranges:       []Stage4InventoryRange{{ID: "0"}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.CreateTask(Task{
		RunID: runID, Table: task.Table, StartedAt: started,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.(RangeBackend).EnsureWorkPlan(WorkTask{
		RunID: runID, Key: task, Strategy: "tuple_keyset",
		TopologyHash: "table-topology", StartedAt: started,
	}, []RangeState{{ID: "0"}}); err != nil {
		t.Fatal(err)
	}
	completedAt := started.Add(time.Minute)
	completion := Stage4TableCompletion{
		RunID: runID, Table: task.Table, Task: task,
		TopologyHash: "table-topology",
		Ranges:       []Stage4RangeCompletion{{ID: "0"}},
		RowsDone:     0, CompletedAt: completedAt,
	}
	if incremental {
		attempt := IncrementalAttempt{
			RunID: runID, Task: task, AttemptID: "attempt-1",
			Mode: IncrementalBaseline, StartedAt: started,
		}
		if _, created, err := stage4.BeginIncrementalAttempt(
			attempt,
		); err != nil || !created {
			t.Fatalf("begin incremental created=%v err=%v", created, err)
		}
		completion.Incremental = &IncrementalCommit{
			RunID: runID, Task: task, AttemptID: attempt.AttemptID,
			TopologyHash: completion.TopologyHash,
			CompletedAt:  completion.CompletedAt,
		}
	}
	return stage4AggregateFixture{
		backend: backend, reopen: reopen, aggregate: aggregate,
		stage4: stage4, runID: runID, task: task,
		started: started, completion: completion,
	}
}

func newStage4AggregateRunFixture(
	t *testing.T,
	factory stage4AggregateFactory,
) (stage4AggregateFixture, Stage4RunCompletion) {
	t.Helper()
	fixture := newStage4AggregateFixture(t, factory, false)
	if err := fixture.aggregate.CompleteStage4Table(
		fixture.completion,
	); err != nil {
		t.Fatal(err)
	}
	_, completion := addStage4AggregateSentinel(t, fixture)
	return fixture, completion
}

func addStage4AggregateSentinel(
	t *testing.T,
	fixture stage4AggregateFixture,
) (SchemaSnapshot, Stage4RunCompletion) {
	t.Helper()
	sentinelTask := stage4SchemaContractSentinelTask
	snapshot := installStage4AggregateSchemaAuthority(
		t,
		fixture.backend,
		fixture.stage4,
		fixture.runID,
		fixture.started,
	)
	targetTask := stage4TargetShapeSentinelTask
	if _, err := fixture.backend.(RangeBackend).EnsureWorkPlan(WorkTask{
		RunID: fixture.runID, Key: targetTask,
		Strategy:     "stage4_target_shape_authority_v1",
		TopologyHash: "target-topology",
		StartedAt:    fixture.started,
	}, []RangeState{{ID: "aggregate-target-shape"}}); err != nil {
		t.Fatal(err)
	}
	targetSnapshot, err := normalizeSchemaSnapshot(SchemaSnapshot{
		RunID: fixture.runID, Task: targetTask,
		CanonicalJSON: `{"target":1}`,
		CapturedAt:    fixture.started.Add(11 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.stage4.SaveSchemaSnapshot(targetSnapshot); err != nil {
		t.Fatal(err)
	}
	completion := Stage4RunCompletion{
		RunID: fixture.runID,
		Sentinels: []Stage4SentinelCompletion{
			{
				Task: sentinelTask, RangeID: "aggregate-schema",
				TopologyHash: "schema-topology", Snapshot: snapshot,
			},
			{
				Task: targetTask, RangeID: "aggregate-target-shape",
				TopologyHash: "target-topology", Snapshot: targetSnapshot,
			},
		},
		Reason:      "migration completed",
		CompletedAt: fixture.started.Add(2 * time.Minute),
	}
	if fixture.completion.RunID != "" {
		completion.Tables = []Stage4TableCompletion{fixture.completion}
	}
	return snapshot, completion
}

func installStage4AggregateSchemaAuthority(
	t *testing.T,
	backend Backend,
	stage4 Stage4Backend,
	runID string,
	started time.Time,
) SchemaSnapshot {
	t.Helper()
	if _, err := backend.(RangeBackend).EnsureWorkPlan(WorkTask{
		RunID: runID, Key: stage4SchemaContractSentinelTask,
		Strategy:     "stage4_aggregate_schema_contract_v1",
		TopologyHash: "schema-topology",
		StartedAt:    started,
	}, []RangeState{{ID: "aggregate-schema"}}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := normalizeSchemaSnapshot(SchemaSnapshot{
		RunID: runID, Task: stage4SchemaContractSentinelTask,
		CanonicalJSON: `{"version":1}`,
		CapturedAt:    started.Add(10 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stage4.SaveSchemaSnapshot(snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertStage4AggregateTableRunning(
	t *testing.T,
	fixture stage4AggregateFixture,
) {
	t.Helper()
	tasks, err := fixture.reopen().ListTasks(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	workTasks, ranges, err := fixture.reopen().(RangeBackend).
		ListWork(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	var tableWork WorkTask
	var tableRange RangeState
	for _, task := range workTasks {
		if task.Key == fixture.task {
			tableWork = task
		}
	}
	for _, workRange := range ranges {
		if workRange.Task == fixture.task {
			tableRange = workRange
		}
	}
	if len(tasks) != 1 ||
		tasks[0].Status != "running" ||
		tasks[0].RowsDone != 0 ||
		!tasks[0].CompletedAt.IsZero() ||
		tableWork.Status != "running" ||
		tableRange.Status != "running" {
		t.Fatalf(
			"aggregate table state is not running: %#v %#v %#v",
			tasks,
			workTasks,
			ranges,
		)
	}
}

func assertStage4AggregateRunRunning(
	t *testing.T,
	fixture stage4AggregateFixture,
	sentinel TaskKey,
) {
	t.Helper()
	runs, err := fixture.reopen().List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].Outcome != Running {
		t.Fatalf("aggregate run changed = %#v", runs)
	}
	workTasks, ranges, err := fixture.reopen().(RangeBackend).
		ListWork(fixture.runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range workTasks {
		if task.Key == sentinel && task.Status != "running" {
			t.Fatalf("aggregate sentinel task changed = %#v", task)
		}
	}
	for _, workRange := range ranges {
		if workRange.Task == sentinel && workRange.Status != "running" {
			t.Fatalf("aggregate sentinel range changed = %#v", workRange)
		}
	}
}

func injectStage4AggregateDelete(
	t *testing.T,
	backend Backend,
	record DeleteReconciliation,
) {
	t.Helper()
	switch store := backend.(type) {
	case SQLiteStore:
		database, err := store.openStage4()
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		taskKey, err := record.Task.canonical()
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(`
			INSERT INTO stage4_records(
				kind, run_id, task_key, record_id, payload
			) VALUES (?, ?, ?, ?, ?)
		`, stage4DeleteRecord, record.RunID, taskKey,
			record.AttemptID, string(payload)); err != nil {
			t.Fatal(err)
		}
	case YAMLStore:
		if err := store.update(func(document *yamlStateDocument) error {
			document.DeleteReconciliations = append(
				document.DeleteReconciliations,
				record,
			)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported aggregate test backend %T", backend)
	}
}

func removeStage4AggregateWork(
	t *testing.T,
	backend Backend,
	runID string,
	task TaskKey,
) {
	t.Helper()
	switch store := backend.(type) {
	case SQLiteStore:
		database, err := store.openStage4()
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		taskKey, err := task.canonical()
		if err != nil {
			t.Fatal(err)
		}
		transaction, err := database.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer transaction.Rollback()
		if _, err := transaction.Exec(
			`DELETE FROM work_ranges WHERE run_id = ? AND task_key = ?`,
			runID,
			taskKey,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := transaction.Exec(
			`DELETE FROM work_tasks WHERE run_id = ? AND task_key = ?`,
			runID,
			taskKey,
		); err != nil {
			t.Fatal(err)
		}
		if err := transaction.Commit(); err != nil {
			t.Fatal(err)
		}
	case YAMLStore:
		if err := store.update(func(document *yamlStateDocument) error {
			keptTasks := document.WorkTasks[:0]
			for _, workTask := range document.WorkTasks {
				if workTask.RunID != runID || workTask.Key != task {
					keptTasks = append(keptTasks, workTask)
				}
			}
			document.WorkTasks = keptTasks
			keptRanges := document.WorkRanges[:0]
			for _, workRange := range document.WorkRanges {
				if workRange.RunID != runID || workRange.Task != task {
					keptRanges = append(keptRanges, workRange)
				}
			}
			document.WorkRanges = keptRanges
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported aggregate test backend %T", backend)
	}
}

func tamperStage4AggregateReceipt(
	t *testing.T,
	backend Backend,
	runID string,
	task TaskKey,
) {
	t.Helper()
	switch store := backend.(type) {
	case SQLiteStore:
		database, err := store.openStage4()
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		taskKey, err := task.canonical()
		if err != nil {
			t.Fatal(err)
		}
		var payload string
		if err := database.QueryRow(`
			SELECT payload FROM stage4_records
			WHERE kind = ? AND run_id = ? AND task_key = ?
		`, stage4AggregateTableRecord, runID, taskKey).Scan(&payload); err != nil {
			t.Fatal(err)
		}
		var receipt Stage4TableCompletionReceipt
		if err := json.Unmarshal([]byte(payload), &receipt); err != nil {
			t.Fatal(err)
		}
		receipt.Digest = "tampered"
		encoded, err := json.Marshal(receipt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(`
			UPDATE stage4_records SET payload = ?
			WHERE kind = ? AND run_id = ? AND task_key = ?
		`, string(encoded), stage4AggregateTableRecord, runID, taskKey); err != nil {
			t.Fatal(err)
		}
	case YAMLStore:
		if err := store.update(func(document *yamlStateDocument) error {
			for index := range document.Stage4TableCompletions {
				receipt := &document.Stage4TableCompletions[index]
				if receipt.Completion.RunID == runID &&
					receipt.Completion.Task == task {
					receipt.Digest = "tampered"
					return nil
				}
			}
			return errors.New("aggregate receipt not found")
		}); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported aggregate test backend %T", backend)
	}
}
