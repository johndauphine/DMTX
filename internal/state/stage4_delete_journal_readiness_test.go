package state

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStage4DeleteJournalReadinessConformance(t *testing.T) {
	for name, factory := range map[string]stage4AggregateFactory{
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
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newStage4AggregateFixture(t, factory, false)
			readiness, ok := fixture.backend.(Stage4DeleteJournalReadinessBackend)
			if !ok {
				t.Fatalf("%T does not implement delete-journal readiness", fixture.backend)
			}
			inventory, found, err := fixture.aggregate.LoadStage4TableInventory(fixture.runID)
			if err != nil || !found {
				t.Fatalf("inventory found=%v err=%v", found, err)
			}
			ready := stage4DeleteJournalTestReadiness(t, fixture, inventory.Digest)
			receipt, created, err := readiness.EnsureStage4DeleteJournalReadiness(ready)
			if err != nil || !created || !receipt.Readiness.Equal(ready) ||
				!receipt.Equal(receipt.Clone()) || receipt.Validate() != nil {
				t.Fatalf("first receipt=%#v created=%v err=%v", receipt, created, err)
			}
			if _, created, err := readiness.EnsureStage4DeleteJournalReadiness(ready); err != nil || created {
				t.Fatalf("identical ensure created=%v err=%v", created, err)
			}
			if err := readiness.SaveStage4DeleteJournalReadiness(ready); err != nil {
				t.Fatalf("identical save: %v", err)
			}
			reopened, ok := fixture.reopen().(Stage4DeleteJournalReadinessBackend)
			if !ok {
				t.Fatalf("reopened backend lost readiness capability")
			}
			restored, found, err := reopened.LoadStage4DeleteJournalReadiness(fixture.runID)
			if err != nil || !found || !restored.Equal(receipt) {
				t.Fatalf("restored=%#v found=%v err=%v", restored, found, err)
			}

			for label, changed := range map[string]Stage4DeleteJournalReadiness{
				"target": mustStage4DeleteJournalReadiness(t, fixture.runID, inventory.Digest,
					"postgres:other/database", "postgres", "postgresql", "16.4", "journal-catalog-v1", 1, ready.ReadyAt),
				"flavor": mustStage4DeleteJournalReadiness(t, fixture.runID, inventory.Digest,
					"postgres:target/database", "postgres", "timescaledb", "16.4", "journal-catalog-v1", 1, ready.ReadyAt),
				"journal": mustStage4DeleteJournalReadiness(t, fixture.runID, inventory.Digest,
					"postgres:target/database", "postgres", "postgresql", "16.4", "journal-catalog-v2", 1, ready.ReadyAt),
				"inventory": mustStage4DeleteJournalReadiness(t, fixture.runID,
					stage4DeleteJournalDigestString("other-inventory"), "postgres:target/database", "postgres", "postgresql", "16.4", "journal-catalog-v1", 1, ready.ReadyAt),
			} {
				if _, _, err := readiness.EnsureStage4DeleteJournalReadiness(changed); !errors.Is(err, ErrState) ||
					!errors.Is(err, ErrImmutableEvidence) {
					t.Fatalf("%s mismatch error = %v", label, err)
				}
			}
		})
	}
}

func TestStage4DeleteJournalReadinessRejectsPreexistingMutationEvidence(t *testing.T) {
	for name, factory := range stage4ReadinessFactories() {
		t.Run(name, func(t *testing.T) {
			fixture := newStage4AggregateFixture(t, factory, true)
			readiness := fixture.backend.(Stage4DeleteJournalReadinessBackend)
			inventory, found, err := fixture.aggregate.LoadStage4TableInventory(fixture.runID)
			if err != nil || !found {
				t.Fatalf("inventory found=%v err=%v", found, err)
			}
			err = readiness.ValidateStage4DeleteJournalReadinessBoundary(
				Stage4DeleteJournalReadinessBoundary{
					RunID:           fixture.runID,
					InventoryDigest: inventory.Digest,
				},
			)
			if !errors.Is(err, ErrState) || !errors.Is(err, ErrStateTransition) {
				t.Fatalf("read-only incremental boundary error = %v", err)
			}
			_, _, err = readiness.EnsureStage4DeleteJournalReadiness(
				stage4DeleteJournalTestReadiness(t, fixture, inventory.Digest),
			)
			if !errors.Is(err, ErrState) || !errors.Is(err, ErrStateTransition) {
				t.Fatalf("incremental mutation evidence error = %v", err)
			}
		})
	}
}

func TestStage4DeleteJournalReadinessRejectsSentinelProgress(t *testing.T) {
	tests := map[string]func(*testing.T, stage4AggregateFixture, WorkTask, string){
		"advanced schema sentinel": func(
			t *testing.T,
			fixture stage4AggregateFixture,
			task WorkTask,
			rangeID string,
		) {
			t.Helper()
			stage4DeleteJournalAdvanceSentinel(t, fixture, task, rangeID)
		},
		"completed schema sentinel": func(
			t *testing.T,
			fixture stage4AggregateFixture,
			task WorkTask,
			rangeID string,
		) {
			t.Helper()
			stage4DeleteJournalCompleteSentinel(t, fixture, task, rangeID)
		},
		"advanced target-shape sentinel": func(
			t *testing.T,
			fixture stage4AggregateFixture,
			task WorkTask,
			rangeID string,
		) {
			t.Helper()
			stage4DeleteJournalAdvanceSentinel(t, fixture, task, rangeID)
		},
		"completed target-shape sentinel": func(
			t *testing.T,
			fixture stage4AggregateFixture,
			task WorkTask,
			rangeID string,
		) {
			t.Helper()
			stage4DeleteJournalCompleteSentinel(t, fixture, task, rangeID)
		},
	}
	for backendName, factory := range stage4ReadinessFactories() {
		for name, mutate := range tests {
			t.Run(backendName+"/"+name, func(t *testing.T) {
				fixture := newStage4AggregateFixture(t, factory, false)
				readiness := fixture.backend.(Stage4DeleteJournalReadinessBackend)
				inventory, found, err := fixture.aggregate.LoadStage4TableInventory(
					fixture.runID,
				)
				if err != nil || !found {
					t.Fatalf("inventory found=%v err=%v", found, err)
				}
				task := WorkTask{
					RunID:        fixture.runID,
					Key:          stage4SchemaContractSentinelTask,
					Strategy:     "stage4_aggregate_schema_contract_v1",
					TopologyHash: inventory.Inventory.SchemaTopologyHash,
					StartedAt:    fixture.started,
				}
				rangeID := "aggregate-schema"
				if strings.Contains(name, "target-shape") {
					task = WorkTask{
						RunID:        fixture.runID,
						Key:          stage4TargetShapeSentinelTask,
						Strategy:     "stage4_target_shape_authority_v1",
						TopologyHash: inventory.Inventory.SchemaTopologyHash,
						StartedAt:    fixture.started,
					}
					rangeID = "aggregate-target-shape"
					if _, err := fixture.backend.(RangeBackend).EnsureWorkPlan(
						task,
						[]RangeState{{ID: rangeID}},
					); err != nil {
						t.Fatal(err)
					}
				}
				mutate(t, fixture, task, rangeID)
				boundary := Stage4DeleteJournalReadinessBoundary{
					RunID:           fixture.runID,
					InventoryDigest: inventory.Digest,
				}
				if err := readiness.ValidateStage4DeleteJournalReadinessBoundary(boundary); !errors.Is(err, ErrState) ||
					!errors.Is(err, ErrStateTransition) {
					t.Fatalf("read-only sentinel boundary error = %v", err)
				}
				if _, _, err := readiness.EnsureStage4DeleteJournalReadiness(
					stage4DeleteJournalTestReadiness(t, fixture, inventory.Digest),
				); !errors.Is(err, ErrState) || !errors.Is(err, ErrStateTransition) {
					t.Fatalf("atomic sentinel readiness error = %v", err)
				}
			})
		}
	}
}

func TestStage4DeleteJournalReadinessRejectsTargetShapeSentinelTopologyMismatch(
	t *testing.T,
) {
	for backendName, factory := range stage4ReadinessFactories() {
		t.Run(backendName, func(t *testing.T) {
			fixture := newStage4AggregateFixture(t, factory, false)
			readiness := fixture.backend.(Stage4DeleteJournalReadinessBackend)
			inventory, found, err := fixture.aggregate.LoadStage4TableInventory(
				fixture.runID,
			)
			if err != nil || !found {
				t.Fatalf("inventory found=%v err=%v", found, err)
			}
			if _, err := fixture.backend.(RangeBackend).EnsureWorkPlan(WorkTask{
				RunID:        fixture.runID,
				Key:          stage4TargetShapeSentinelTask,
				Strategy:     "stage4_target_shape_authority_v1",
				TopologyHash: "different-target-shape-topology",
				StartedAt:    fixture.started,
			}, []RangeState{{ID: "aggregate-target-shape"}}); err != nil {
				t.Fatal(err)
			}
			err = readiness.ValidateStage4DeleteJournalReadinessBoundary(
				Stage4DeleteJournalReadinessBoundary{
					RunID:           fixture.runID,
					InventoryDigest: inventory.Digest,
				},
			)
			if !errors.Is(err, ErrState) || !errors.Is(err, ErrStateTransition) {
				t.Fatalf("target-shape topology readiness error = %v", err)
			}
		})
	}
}

func stage4DeleteJournalAdvanceSentinel(
	t *testing.T,
	fixture stage4AggregateFixture,
	task WorkTask,
	rangeID string,
) {
	t.Helper()
	rangeBackend := fixture.backend.(RangeBackend)
	at := fixture.started.Add(time.Second)
	if err := rangeBackend.BeginRangeChunk(RangeChunkIntent{
		RunID:        fixture.runID,
		Task:         task.Key,
		RangeID:      rangeID,
		TopologyHash: task.TopologyHash,
		Sequence:     0,
		ChunkRows:    1,
		Exhausted:    true,
		At:           at,
	}); err != nil {
		t.Fatal(err)
	}
	if err := rangeBackend.RecordRangeAttempt(RangeAttempt{
		RunID:        fixture.runID,
		Task:         task.Key,
		RangeID:      rangeID,
		TopologyHash: task.TopologyHash,
		Sequence:     0,
		At:           at.Add(time.Second),
	}); err != nil {
		t.Fatal(err)
	}
}

func stage4DeleteJournalCompleteSentinel(
	t *testing.T,
	fixture stage4AggregateFixture,
	task WorkTask,
	rangeID string,
) {
	t.Helper()
	rangeBackend := fixture.backend.(RangeBackend)
	completedAt := fixture.started.Add(time.Second)
	if err := rangeBackend.CompleteRange(
		fixture.runID,
		task.Key,
		rangeID,
		task.TopologyHash,
		0,
		completedAt,
	); err != nil {
		t.Fatal(err)
	}
	if err := rangeBackend.CompleteWorkTask(
		fixture.runID,
		task.Key,
		task.TopologyHash,
		completedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func TestStage4DeleteJournalReadinessDetectsCorruption(t *testing.T) {
	for name, factory := range stage4ReadinessFactories() {
		t.Run(name, func(t *testing.T) {
			fixture := newStage4AggregateFixture(t, factory, false)
			readiness := fixture.backend.(Stage4DeleteJournalReadinessBackend)
			inventory, found, err := fixture.aggregate.LoadStage4TableInventory(fixture.runID)
			if err != nil || !found {
				t.Fatalf("inventory found=%v err=%v", found, err)
			}
			receipt, _, err := readiness.EnsureStage4DeleteJournalReadiness(
				stage4DeleteJournalTestReadiness(t, fixture, inventory.Digest),
			)
			if err != nil {
				t.Fatal(err)
			}
			switch store := fixture.backend.(type) {
			case YAMLStore:
				data, readErr := os.ReadFile(store.Path)
				if readErr != nil {
					t.Fatal(readErr)
				}
				corrupt := strings.Replace(string(data), receipt.Digest, "deadbeef", 1)
				if corrupt == string(data) {
					t.Fatal("readiness receipt digest was not persisted in YAML")
				}
				if writeErr := os.WriteFile(store.Path, []byte(corrupt), 0o600); writeErr != nil {
					t.Fatal(writeErr)
				}
			case SQLiteStore:
				database, openErr := store.Open()
				if openErr != nil {
					t.Fatal(openErr)
				}
				_, execErr := database.Exec(`
					UPDATE stage4_records SET payload = ?
					WHERE kind = ? AND run_id = ?
				`, "{", stage4DeleteJournalReadinessRecord, fixture.runID)
				closeErr := database.Close()
				if execErr != nil {
					t.Fatal(execErr)
				}
				if closeErr != nil {
					t.Fatal(closeErr)
				}
			default:
				t.Fatalf("unexpected readiness store %T", store)
			}
			if _, _, err := readiness.LoadStage4DeleteJournalReadiness(fixture.runID); !errors.Is(err, ErrState) {
				t.Fatalf("corrupt readiness load error = %v", err)
			}
		})
	}
}

func TestStage4DeleteJournalReadinessSQLiteMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	store := SQLiteStore{Path: path}
	fixture := initializeStage4AggregateFixture(
		t,
		store,
		func() Backend { return SQLiteStore{Path: path} },
		false,
	)
	database, err := store.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		UPDATE state_schema_versions SET version = 1
		WHERE component = 'stage4_state'
	`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	readiness := fixture.backend.(Stage4DeleteJournalReadinessBackend)
	if _, found, err := readiness.LoadStage4DeleteJournalReadiness(fixture.runID); err != nil || found {
		t.Fatalf("migration load found=%v err=%v", found, err)
	}
	database, err = store.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var version int
	if err := database.QueryRow(`
		SELECT version FROM state_schema_versions WHERE component = 'stage4_state'
	`).Scan(&version); err != nil || version != sqliteStage4SchemaVersion {
		t.Fatalf("migrated version=%d err=%v", version, err)
	}
}

func TestFencedDeleteJournalReadinessRejectsStaleLease(t *testing.T) {
	directory := t.TempDir()
	leaseStore := SQLiteStore{Path: filepath.Join(directory, "leases.db")}
	first, err := leaseStore.AcquireLease("postgres:target/database", "run-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	raw := YAMLStore{Path: filepath.Join(directory, "state.yaml")}
	fenced := FenceBackend(raw, NewLeaseGuard(leaseStore, first))
	started := time.Now().UTC()
	if err := fenced.InitializeRun(Run{
		ID: "run-1", Source: "source", Target: "target",
		SourceEngine: "postgres", SourceIdentity: "postgres:source/database",
		TargetIdentity: "postgres:target/database", Outcome: Running,
		Resumable: true, Reason: "running", StartedAt: started,
	}, "hash"); err != nil {
		t.Fatal(err)
	}
	readiness, ok := fenced.(Stage4DeleteJournalReadinessBackend)
	if !ok {
		t.Fatal("fenced readiness capability is missing")
	}
	database, err := leaseStore.Open()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		UPDATE leases SET heartbeat_at = ? WHERE target = ?
	`, time.Unix(0, 0).UTC(), first.Target); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := leaseStore.AcquireLease(first.Target, "run-2", time.Second); err != nil {
		t.Fatal(err)
	}
	ready := mustStage4DeleteJournalReadiness(t, "run-1",
		stage4DeleteJournalDigestString("inventory"), "postgres:target/database",
		"postgres", "postgresql", "16.4", "journal", 1, started.Add(time.Second))
	if err := readiness.SaveStage4DeleteJournalReadiness(ready); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale readiness save error = %v", err)
	}
	if _, _, err := readiness.EnsureStage4DeleteJournalReadiness(ready); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale readiness ensure error = %v", err)
	}
	if _, found, err := raw.LoadStage4DeleteJournalReadiness("run-1"); err != nil || found {
		t.Fatalf("stale readiness write persisted receipt found=%v err=%v", found, err)
	}
}

func stage4ReadinessFactories() map[string]stage4AggregateFactory {
	return map[string]stage4AggregateFactory{
		"sqlite": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.db")
			return SQLiteStore{Path: path}, func() Backend { return SQLiteStore{Path: path} }
		},
		"yaml": func(t *testing.T) (Backend, func() Backend) {
			path := filepath.Join(t.TempDir(), "state.yaml")
			return YAMLStore{Path: path}, func() Backend { return YAMLStore{Path: path} }
		},
	}
}

func stage4DeleteJournalTestReadiness(
	t *testing.T,
	fixture stage4AggregateFixture,
	inventoryDigest string,
) Stage4DeleteJournalReadiness {
	t.Helper()
	return mustStage4DeleteJournalReadiness(
		t,
		fixture.runID,
		inventoryDigest,
		"postgres:target/database",
		"postgres",
		"postgresql",
		"16.4",
		"journal-catalog-v1",
		1,
		fixture.started.Add(time.Second),
	)
}

func mustStage4DeleteJournalReadiness(
	t *testing.T,
	runID string,
	inventoryDigest string,
	targetIdentity string,
	targetEngine string,
	targetFlavor string,
	targetVersion string,
	journalCatalog string,
	journalVersion int,
	readyAt time.Time,
) Stage4DeleteJournalReadiness {
	t.Helper()
	ready, err := NewStage4DeleteJournalReadiness(
		runID,
		inventoryDigest,
		targetIdentity,
		targetEngine,
		targetFlavor,
		targetVersion,
		stage4DeleteJournalDigestString(journalCatalog),
		journalVersion,
		readyAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return ready
}
