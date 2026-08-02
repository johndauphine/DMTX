package state

import (
	"errors"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRangeBackendConformance(t *testing.T) {
	backends := map[string]RangeBackend{
		"sqlite": SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")},
		"yaml":   YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
	for name, backend := range backends {
		t.Run(name, func(t *testing.T) {
			testRangeBackend(t, backend)
		})
	}
}

func TestTaskKeyCanonicalizationHasNoDelimiterCollisions(
	t *testing.T,
) {
	keys := []TaskKey{
		{
			Type:      `table:"copy"`,
			Schema:    `sales.eu`,
			Table:     `"订单"|items`,
			Partition: `part:/one`,
		},
		{
			Type:      `table`,
			Schema:    `"copy":sales`,
			Table:     `eu."订单"`,
			Partition: `part:/one`,
		},
	}
	left, err := keys[0].canonical()
	if err != nil {
		t.Fatal(err)
	}
	right, err := keys[1].canonical()
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatalf("distinct structured task keys collided: %q", left)
	}
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			var backend RangeBackend
			switch stateKind {
			case "sqlite":
				backend = SQLiteStore{
					Path: filepath.Join(t.TempDir(), "state.db"),
				}
			case "yaml":
				backend = YAMLStore{
					Path: filepath.Join(t.TempDir(), "state.yaml"),
				}
			}
			for index, key := range keys {
				created, err := backend.EnsureWorkPlan(
					WorkTask{
						RunID:    "quoted-identity",
						Key:      key,
						Strategy: "tuple_keyset",
						TopologyHash: "topology-" +
							string(rune('a'+index)),
					},
					[]RangeState{{ID: "range"}},
				)
				if err != nil || !created {
					t.Fatalf(
						"key %d created=%v error=%v",
						index,
						created,
						err,
					)
				}
			}
			tasks, ranges, err := backend.ListWork(
				"quoted-identity",
			)
			if err != nil {
				t.Fatal(err)
			}
			seen := make(map[TaskKey]bool, len(tasks))
			for _, task := range tasks {
				seen[task.Key] = true
			}
			if len(tasks) != 2 || len(ranges) != 2 ||
				!seen[keys[0]] || !seen[keys[1]] {
				t.Fatalf(
					"tasks=%#v ranges=%#v",
					tasks,
					ranges,
				)
			}
		})
	}
}

func TestTaskKeyRejectsLossyOrUnboundedIdentity(
	t *testing.T,
) {
	invalidUTF8A := string([]byte{0xff})
	invalidUTF8B := string([]byte{0xfe})
	tests := []struct {
		name string
		key  TaskKey
	}{
		{
			name: "invalid UTF-8 A",
			key: TaskKey{
				Type:  "table-copy",
				Table: invalidUTF8A,
			},
		},
		{
			name: "invalid UTF-8 B",
			key: TaskKey{
				Type:  "table-copy",
				Table: invalidUTF8B,
			},
		},
		{
			name: "NUL",
			key: TaskKey{
				Type:   "table-copy",
				Schema: "main\x00shadow",
				Table:  "items",
			},
		},
		{
			name: "unbounded",
			key: TaskKey{
				Type:  "table-copy",
				Table: "items",
				Partition: strings.Repeat(
					"p",
					maximumTaskKeyFieldBytes+1,
				),
			},
		},
	}
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			var backend RangeBackend
			switch stateKind {
			case "sqlite":
				backend = SQLiteStore{
					Path: filepath.Join(t.TempDir(), "state.db"),
				}
			case "yaml":
				backend = YAMLStore{
					Path: filepath.Join(t.TempDir(), "state.yaml"),
				}
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					if _, err := backend.EnsureWorkPlan(
						WorkTask{
							RunID:        "invalid-identity",
							Key:          test.key,
							Strategy:     "tuple_keyset",
							TopologyHash: "topology-a",
						},
						[]RangeState{{ID: "range"}},
					); err == nil {
						t.Fatal("invalid task key was accepted")
					}
				})
			}
			tasks, ranges, err := backend.ListWork(
				"invalid-identity",
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 0 || len(ranges) != 0 {
				t.Fatalf(
					"invalid identities mutated state: tasks=%#v ranges=%#v",
					tasks,
					ranges,
				)
			}
		})
	}

	multiInvalid := TaskKey{
		Type:  invalidUTF8A,
		Table: "items\x00shadow",
	}
	for attempt := 0; attempt < 100; attempt++ {
		if err := multiInvalid.Validate(); err == nil ||
			err.Error() != "task type contains invalid UTF-8" {
			t.Fatalf(
				"attempt %d deterministic validation error = %v",
				attempt,
				err,
			)
		}
	}
}

func TestRangeBackendRejectsMutableInitialProgress(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(*WorkTask, *RangeState)
	}{
		{
			name: "task progress",
			mutate: func(task *WorkTask, _ *RangeState) {
				task.Attempts = 1
				task.UpdatedAt = time.Now().UTC()
			},
		},
		{
			name: "range progress",
			mutate: func(_ *WorkTask, workRange *RangeState) {
				workRange.RowsDone = 1
				workRange.NextSequence = 1
			},
		},
		{
			name: "pending progress",
			mutate: func(_ *WorkTask, workRange *RangeState) {
				workRange.Pending = []PendingAcknowledgement{{
					Sequence:  0,
					ChunkRows: 1,
				}}
			},
		},
		{
			name: "negative planned rows",
			mutate: func(_ *WorkTask, workRange *RangeState) {
				workRange.RowsTotal = -1
			},
		},
	}
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					var backend RangeBackend
					switch stateKind {
					case "sqlite":
						backend = SQLiteStore{
							Path: filepath.Join(
								t.TempDir(),
								"state.db",
							),
						}
					case "yaml":
						backend = YAMLStore{
							Path: filepath.Join(
								t.TempDir(),
								"state.yaml",
							),
						}
					}
					task := WorkTask{
						RunID: "mutable-plan",
						Key: TaskKey{
							Type:  "table-copy",
							Table: "items",
						},
						Strategy:     "integer_keyset",
						TopologyHash: "topology-a",
					}
					workRange := RangeState{ID: "range"}
					test.mutate(&task, &workRange)
					if _, err := backend.EnsureWorkPlan(
						task,
						[]RangeState{workRange},
					); err == nil {
						t.Fatal(
							"mutable initial progress was accepted",
						)
					}
					tasks, ranges, err := backend.ListWork(
						"mutable-plan",
					)
					if err != nil {
						t.Fatal(err)
					}
					if len(tasks) != 0 || len(ranges) != 0 {
						t.Fatalf(
							"rejected plan mutated state: tasks=%#v ranges=%#v",
							tasks,
							ranges,
						)
					}
				})
			}
		})
	}
}

func TestApplyRangeAttemptRejectsNegativePersistedCounters(
	t *testing.T,
) {
	tests := []struct {
		name   string
		mutate func(*WorkTask, *RangeState)
	}{
		{
			name: "task attempts",
			mutate: func(task *WorkTask, _ *RangeState) {
				task.Attempts = -1
			},
		},
		{
			name: "task retries",
			mutate: func(task *WorkTask, _ *RangeState) {
				task.Retries = -1
			},
		},
		{
			name: "range attempts",
			mutate: func(_ *WorkTask, workRange *RangeState) {
				workRange.Attempts = -1
			},
		},
		{
			name: "range retries",
			mutate: func(_ *WorkTask, workRange *RangeState) {
				workRange.Retries = -1
			},
		},
		{
			name: "pending attempts",
			mutate: func(_ *WorkTask, workRange *RangeState) {
				workRange.Pending[0].Attempts = -1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			key := TaskKey{
				Type:  "table-copy",
				Table: "items",
			}
			task := WorkTask{
				RunID:        "negative-counter",
				Key:          key,
				Status:       "running",
				TopologyHash: "topology-a",
			}
			workRange := RangeState{
				RunID:        task.RunID,
				Task:         key,
				ID:           "range",
				Status:       "running",
				TopologyHash: task.TopologyHash,
				Pending: []PendingAcknowledgement{{
					Sequence:  0,
					ChunkRows: 1,
				}},
			}
			test.mutate(&task, &workRange)
			_, _, err := applyRangeAttempt(
				task,
				workRange,
				RangeAttempt{
					RunID:        task.RunID,
					Task:         key,
					RangeID:      workRange.ID,
					TopologyHash: task.TopologyHash,
					Sequence:     0,
				},
			)
			if !errors.Is(err, ErrRangeOrder) {
				t.Fatalf("error = %v, want %v", err, ErrRangeOrder)
			}
		})
	}
}

func testRangeBackend(t *testing.T, backend RangeBackend) {
	t.Helper()
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	key := TaskKey{Type: "table-copy", Schema: "main", Table: "items"}
	task := WorkTask{RunID: "run-1", Key: key, Strategy: "tuple_keyset", TopologyHash: "topology-a", StartedAt: now}
	ranges := []RangeState{
		{ID: "0", Lower: TypedTuple{Int64Value(-1)}, Upper: TypedTuple{Int64Value(9_007_199_254_740_993)}, UpperInclusive: true},
		{ID: "1", Lower: TypedTuple{Int64Value(9_007_199_254_740_993)}, Upper: TypedTuple{TextValue("z")}, UpperInclusive: true},
	}
	created, err := backend.EnsureWorkPlan(task, ranges)
	if err != nil || !created {
		t.Fatalf("EnsureWorkPlan created=%v err=%v", created, err)
	}
	if created, err := backend.EnsureWorkPlan(task, ranges); err != nil || created {
		t.Fatalf("idempotent ensure created=%v err=%v", created, err)
	}
	changed := task
	changed.TopologyHash = "topology-b"
	if _, err := backend.EnsureWorkPlan(changed, ranges); !errors.Is(err, ErrTopologyChanged) {
		t.Fatalf("topology error = %v", err)
	}
	if _, err := backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "missing", TopologyHash: "topology-a",
		ChunkRows: 1, DurableRows: 1,
	}); !errors.Is(err, ErrUnknownWork) {
		t.Fatalf("unknown range error = %v", err)
	}

	firstIntent := RangeChunkIntent{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5,
		StartFrontier: TypedTuple{Int64Value(-1)}, StartFrontierValid: true,
		EndFrontier: TypedTuple{Int64Value(9_007_199_254_740_992)}, FrontierValid: true,
		Fingerprint: "page-fingerprint-0", At: now,
	}
	if err := backend.BeginRangeChunk(firstIntent); err != nil {
		t.Fatal(err)
	}
	_, issued, err := backend.ListWork("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(issued[0].Pending) != 1 ||
		issued[0].Pending[0].DurableRows != 0 ||
		!typedTupleEqual(
			issued[0].Pending[0].StartFrontier,
			firstIntent.StartFrontier,
		) ||
		!issued[0].Pending[0].StartFrontierValid ||
		!typedTupleEqual(
			issued[0].Pending[0].IssuedEndFrontier,
			firstIntent.EndFrontier,
		) ||
		!issued[0].Pending[0].IssuedEndValid ||
		issued[0].Pending[0].Fingerprint != firstIntent.Fingerprint ||
		issued[0].Pending[0].Exhausted {
		t.Fatalf("issued intent was not persisted: %#v", issued[0])
	}
	if err := backend.BeginRangeChunk(firstIntent); err != nil {
		t.Fatalf("idempotent rich issued intent: %v", err)
	}
	for name, mutate := range map[string]func(*RangeChunkIntent){
		"start frontier": func(intent *RangeChunkIntent) {
			intent.StartFrontier = TypedTuple{Int64Value(-2)}
		},
		"start frontier validity": func(intent *RangeChunkIntent) {
			intent.StartFrontierValid = false
		},
		"fingerprint": func(intent *RangeChunkIntent) {
			intent.Fingerprint = "changed"
		},
		"exhausted": func(intent *RangeChunkIntent) {
			intent.Exhausted = true
		},
	} {
		t.Run("issued mismatch "+name, func(t *testing.T) {
			changed := firstIntent
			mutate(&changed)
			if err := backend.BeginRangeChunk(changed); !errors.Is(
				err,
				ErrRangeOrder,
			) {
				t.Fatalf("error = %v, want %v", err, ErrRangeOrder)
			}
		})
	}
	if err := backend.RecordRangeAttempt(RangeAttempt{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, At: now,
	}); err != nil {
		t.Fatal(err)
	}

	// A later durable sequence is retained but cannot advance over sequence 0.
	if err := backend.BeginRangeChunk(RangeChunkIntent{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 1, ChunkRows: 3,
		StartFrontier: TypedTuple{
			Int64Value(9_007_199_254_740_992),
		},
		StartFrontierValid: true,
		EndFrontier:        TypedTuple{Int64Value(9_007_199_254_740_993)}, FrontierValid: true, At: now.Add(time.Second),
		Fingerprint: "page-fingerprint-1", Exhausted: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := backend.RecordRangeAttempt(RangeAttempt{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 1, At: now.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	state, err := backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 1, ChunkRows: 3, DurableRows: 3,
		Frontier: TypedTuple{Int64Value(9_007_199_254_740_993)}, FrontierValid: true, At: now.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 0 || state.RowsDone != 0 {
		t.Fatalf("out-of-order acknowledgement advanced state: %#v", state)
	}
	state, err = backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5, DurableRows: 2,
		Frontier: TypedTuple{Int64Value(9_007_199_254_740_991)}, FrontierValid: true, At: now.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 0 || state.SequenceOffset != 2 || state.RowsDone != 2 {
		t.Fatalf("partial state = %#v", state)
	}
	if len(state.Pending) != 2 ||
		!typedTupleEqual(
			state.Pending[0].IssuedEndFrontier,
			firstIntent.EndFrontier,
		) ||
		!state.Pending[0].IssuedEndValid ||
		typedTupleEqual(
			state.Pending[0].Frontier,
			state.Pending[0].IssuedEndFrontier,
		) {
		t.Fatalf(
			"partial acknowledgement lost immutable issued frontier: %#v",
			state.Pending,
		)
	}
	if err := backend.RecordRangeAttempt(RangeAttempt{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, At: now.Add(3 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	state, err = backend.AcknowledgeRange(RangeAcknowledgement{
		RunID: "run-1", Task: key, RangeID: "0", TopologyHash: "topology-a",
		Sequence: 0, ChunkRows: 5, AttemptOffset: 2, DurableRows: 3,
		Frontier: TypedTuple{Int64Value(9_007_199_254_740_992)}, FrontierValid: true, At: now.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.NextSequence != 2 || state.SequenceOffset != 0 || state.RowsDone != 8 {
		t.Fatalf("contiguous state = %#v", state)
	}
	if err := backend.CompleteRange("run-1", key, "0", "topology-a", 2, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := backend.CompleteRange("run-1", key, "1", "topology-a", 0, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := backend.CompleteWorkTask("run-1", key, "topology-a", now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	tasks, restored, err := backend.ListWork("run-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Status != "completed" || len(restored) != 2 {
		t.Fatalf("restored tasks=%#v ranges=%#v", tasks, restored)
	}
	value, err := restored[0].Upper[0].SQLValue()
	if err != nil || value != int64(9_007_199_254_740_993) {
		t.Fatalf("typed large integer = %#v, err=%v", value, err)
	}
}

func TestRangeBackendRejectsMalformedRichIssuedEvidence(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			var backend RangeBackend
			switch stateKind {
			case "sqlite":
				backend = SQLiteStore{
					Path: filepath.Join(t.TempDir(), "state.db"),
				}
			case "yaml":
				backend = YAMLStore{
					Path: filepath.Join(t.TempDir(), "state.yaml"),
				}
			}
			key := TaskKey{Type: "table-copy", Table: "items"}
			_, err := backend.EnsureWorkPlan(
				WorkTask{
					RunID:        "rich-invalid",
					Key:          key,
					Strategy:     "integer_keyset",
					TopologyHash: "topology-a",
				},
				[]RangeState{{
					ID: "0",
				}},
			)
			if err != nil {
				t.Fatal(err)
			}
			base := RangeChunkIntent{
				RunID:         "rich-invalid",
				Task:          key,
				RangeID:       "0",
				TopologyHash:  "topology-a",
				ChunkRows:     1,
				EndFrontier:   TypedTuple{Int64Value(1)},
				FrontierValid: true,
				Fingerprint:   "page-0",
			}
			for name, mutate := range map[string]func(*RangeChunkIntent){
				"unmarked start frontier": func(
					intent *RangeChunkIntent,
				) {
					intent.StartFrontier =
						TypedTuple{Int64Value(0)}
				},
				"unmarked end frontier": func(
					intent *RangeChunkIntent,
				) {
					intent.FrontierValid = false
				},
				"secret-shaped fingerprint": func(
					intent *RangeChunkIntent,
				) {
					intent.Fingerprint = "user/password"
				},
				"unbounded fingerprint": func(
					intent *RangeChunkIntent,
				) {
					intent.Fingerprint = strings.Repeat("a", 129)
				},
			} {
				t.Run(name, func(t *testing.T) {
					intent := base
					mutate(&intent)
					if err := backend.BeginRangeChunk(
						intent,
					); !errors.Is(err, ErrRangeOrder) {
						t.Fatalf(
							"error = %v, want %v",
							err,
							ErrRangeOrder,
						)
					}
				})
			}
			_, ranges, err := backend.ListWork("rich-invalid")
			if err != nil {
				t.Fatal(err)
			}
			if len(ranges) != 1 || len(ranges[0].Pending) != 0 {
				t.Fatalf(
					"malformed intent mutated state: %#v",
					ranges,
				)
			}
		})
	}
}

func TestRangeBackendRejectsCounterOverflowWithoutMutation(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			var backend RangeBackend
			switch stateKind {
			case "sqlite":
				backend = SQLiteStore{
					Path: filepath.Join(t.TempDir(), "state.db"),
				}
			case "yaml":
				backend = YAMLStore{
					Path: filepath.Join(t.TempDir(), "state.yaml"),
				}
			}
			key := TaskKey{Type: "table-copy", Table: "items"}
			task := WorkTask{
				RunID:        "overflow-run",
				Key:          key,
				Strategy:     "integer_keyset",
				TopologyHash: "topology-a",
			}
			if _, err := backend.EnsureWorkPlan(
				task,
				[]RangeState{{ID: "0"}, {ID: "1"}},
			); err != nil {
				t.Fatal(err)
			}
			issue := func(
				rangeID string,
				sequence uint64,
				rows int64,
			) {
				t.Helper()
				if err := backend.BeginRangeChunk(
					RangeChunkIntent{
						RunID:        task.RunID,
						Task:         key,
						RangeID:      rangeID,
						TopologyHash: task.TopologyHash,
						Sequence:     sequence,
						ChunkRows:    rows,
					},
				); err != nil {
					t.Fatal(err)
				}
				if err := backend.RecordRangeAttempt(
					RangeAttempt{
						RunID:        task.RunID,
						Task:         key,
						RangeID:      rangeID,
						TopologyHash: task.TopologyHash,
						Sequence:     sequence,
					},
				); err != nil {
					t.Fatal(err)
				}
			}
			ack := func(
				rangeID string,
				sequence uint64,
				rows int64,
				offset int64,
				durable int64,
			) error {
				t.Helper()
				_, err := backend.AcknowledgeRange(
					RangeAcknowledgement{
						RunID:         task.RunID,
						Task:          key,
						RangeID:       rangeID,
						TopologyHash:  task.TopologyHash,
						Sequence:      sequence,
						ChunkRows:     rows,
						AttemptOffset: offset,
						DurableRows:   durable,
					},
				)
				return err
			}
			snapshot := func() (
				[]WorkTask,
				[]RangeState,
			) {
				t.Helper()
				tasks, ranges, err := backend.ListWork(task.RunID)
				if err != nil {
					t.Fatal(err)
				}
				return tasks, ranges
			}

			issue("0", 1, math.MaxInt64)
			if err := ack(
				"0",
				1,
				math.MaxInt64,
				0,
				math.MaxInt64,
			); err != nil {
				t.Fatal(err)
			}
			issue("0", 0, 1)
			beforeTasks, beforeRanges := snapshot()
			if err := ack("0", 0, 1, 0, 1); !errors.Is(
				err,
				ErrRangeOrder,
			) {
				t.Fatalf("fold overflow error = %v", err)
			}
			afterTasks, afterRanges := snapshot()
			if !reflect.DeepEqual(beforeTasks, afterTasks) ||
				!reflect.DeepEqual(beforeRanges, afterRanges) {
				t.Fatal("fold overflow mutated durable state")
			}

			issue("1", 0, math.MaxInt64)
			if err := ack(
				"1",
				0,
				math.MaxInt64,
				0,
				math.MaxInt64-1,
			); err != nil {
				t.Fatal(err)
			}
			issueAttempt := RangeAttempt{
				RunID:        task.RunID,
				Task:         key,
				RangeID:      "1",
				TopologyHash: task.TopologyHash,
				Sequence:     0,
			}
			if err := backend.RecordRangeAttempt(
				issueAttempt,
			); err != nil {
				t.Fatal(err)
			}
			beforeTasks, beforeRanges = snapshot()
			if err := ack(
				"1",
				0,
				math.MaxInt64,
				math.MaxInt64-1,
				2,
			); !errors.Is(err, ErrRangeOrder) {
				t.Fatalf("offset overflow error = %v", err)
			}
			afterTasks, afterRanges = snapshot()
			if !reflect.DeepEqual(beforeTasks, afterTasks) ||
				!reflect.DeepEqual(beforeRanges, afterRanges) {
				t.Fatal("offset overflow mutated durable state")
			}

			if err := backend.BeginRangeChunk(
				RangeChunkIntent{
					RunID:        task.RunID,
					Task:         key,
					RangeID:      "1",
					TopologyHash: task.TopologyHash,
					Sequence:     math.MaxUint64,
					ChunkRows:    1,
				},
			); !errors.Is(err, ErrRangeOrder) {
				t.Fatalf("sequence overflow error = %v", err)
			}
		})
	}
}
