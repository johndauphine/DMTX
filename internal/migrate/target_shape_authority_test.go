package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4TargetShapeAuthorityRejectsProjectionAndEvidenceTamper(
	t *testing.T,
) {
	t.Parallel()

	tables := []schema.Table{
		stage4LifecycleSimpleTable("source", "items"),
	}
	gate := stage4TargetSchemaProjectionGate(
		t,
		nil,
		tables,
		"upsert",
		true,
	)
	target := &stage4TargetSchemaProjectionTestTarget{
		engine: "postgres", targetSchema: "target",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t,
		gate,
		"postgres",
		target,
		"upsert",
	)

	substituted := authority
	substituted.topologyHash = "substituted"
	if _, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		substituted,
		"postgres",
		target,
		"upsert",
	); err == nil || !strings.Contains(err.Error(), "topology differ") {
		t.Fatalf("substituted topology error = %v", err)
	}

	reservationTamper := authority
	reservationTamper.priorReservations = append(
		reservationTamper.priorReservations,
		TargetSchemaEvolutionNameReservation{
			Scope: "relation", Namespace: "target", Name: "injected",
		},
	)
	if _, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		reservationTamper,
		"postgres",
		target,
		"upsert",
	); err == nil ||
		!strings.Contains(err.Error(), "catalog authority was mutated") {
		t.Fatalf("reservation tamper error = %v", err)
	}

	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		target,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	projection.targetAuthorityCatalogDigest = "substituted"
	if _, err := BindStage4TargetShapeProjection(
		authority,
		projection,
	); err == nil ||
		!strings.Contains(err.Error(), "projection authority differs") {
		t.Fatalf("projection authority tamper error = %v", err)
	}

	projection.targetAuthorityCatalogDigest = authority.priorCatalogDigest
	record, err := BindStage4TargetShapeProjection(authority, projection)
	if err != nil {
		t.Fatal(err)
	}
	var evidence stage4TargetShapeEvidence
	if err := json.Unmarshal(
		[]byte(record.CanonicalJSON),
		&evidence,
	); err != nil {
		t.Fatal(err)
	}
	evidence.PriorReservations = append(
		evidence.PriorReservations,
		TargetSchemaEvolutionNameReservation{
			Scope: "relation", Namespace: "target", Name: "injected",
		},
	)
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	record.CanonicalJSON = string(encoded)
	record.Digest = hex.EncodeToString(digest[:])
	if _, err := parseStage4TargetShapeEvidence(
		record,
		authority.runID,
	); err == nil ||
		!strings.Contains(err.Error(), "catalog digest") {
		t.Fatalf("stored evidence tamper error = %v", err)
	}
}

func TestStage4TargetShapeAuthorityRetainsSubobjectsAcrossThreeRuns(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 31, 13, 0, 0, 0, time.UTC)
			options := stage4TargetShapeTestOptions(started)
			options.Contract.Tables = config.SchemaContractReport
			planner := &stage4TargetSchemaProjectionTestTarget{
				engine:       "postgres",
				targetSchema: "target",
			}
			original := stage4TargetShapeRichTable(t)

			firstGate, firstAuthority, firstProjection, firstTarget :=
				stage4TargetShapeTestRun(
					t,
					backend,
					"target-shape-first",
					started,
					[]schema.Table{original},
					options,
					planner,
					stage4TargetShapeTestCatalog(
						t,
						projectionTargetTables(
							t,
							[]schema.Table{original},
							planner,
						),
						nil,
					),
				)
			stage4TargetShapeTestPublishSuccess(
				t,
				backend,
				"target-shape-first",
				started,
				firstGate,
				firstTarget,
			)
			if len(firstProjection.CurrentTables()) != 1 ||
				len(firstAuthority.PriorSnapshot().Tables) != 1 {
				t.Fatalf(
					"first target projection/prior = %#v/%#v",
					firstProjection.CurrentTables(),
					firstAuthority.PriorSnapshot(),
				)
			}

			dropped := cloneStage4RichTable(original)
			dropped.Columns = dropped.Columns[:1]
			dropped.Indexes = nil
			dropped.Checks = nil
			dropped.ForeignKeys = nil
			secondOptions := options
			secondOptions.CapturedAt = started.Add(time.Hour)
			secondGate, _, secondProjection, secondTarget :=
				stage4TargetShapeTestRun(
					t,
					backend,
					"target-shape-second",
					started.Add(time.Hour),
					[]schema.Table{dropped},
					secondOptions,
					planner,
					TargetSchemaEvolutionCatalog{},
				)
			stage4TargetShapeTestAssertRetained(t, secondProjection.CurrentTables())
			stage4TargetShapeTestPublishSuccess(
				t,
				backend,
				"target-shape-second",
				started.Add(time.Hour),
				secondGate,
				secondTarget,
			)

			added := cloneStage4RichTable(dropped)
			added.Columns = append(added.Columns, schema.Column{
				Name: "new_value", Type: "text", Nullable: true,
			})
			thirdOptions := options
			thirdOptions.CapturedAt = started.Add(2 * time.Hour)
			thirdGate, _, thirdProjection, _ :=
				stage4TargetShapeTestRun(
					t,
					backend,
					"target-shape-third",
					started.Add(2*time.Hour),
					[]schema.Table{added},
					thirdOptions,
					planner,
					TargetSchemaEvolutionCatalog{},
				)
			if len(thirdGate.PreviousSnapshot.Tables) != 1 ||
				len(thirdGate.PreviousSnapshot.Tables[0].Columns) != 1 {
				t.Fatalf(
					"third source baseline was contaminated by target shape: %#v",
					thirdGate.PreviousSnapshot,
				)
			}
			stage4TargetShapeTestAssertRetained(t, thirdProjection.CurrentTables())
			table := stage4TargetSchemaProjectionFindTable(
				t,
				thirdProjection.CurrentTables(),
				"items",
			)
			stage4TargetSchemaProjectionFindColumn(t, table, "new_value")

			frozenOptions := options
			frozenOptions.CapturedAt = started.Add(3 * time.Hour)
			frozenOptions.Contract = &config.SchemaContract{
				Tables:   config.SchemaContractFreeze,
				Columns:  config.SchemaContractDiscardRow,
				DataType: config.SchemaContractEvolve,
			}
			initializeStage4LifecycleRun(
				t,
				backend,
				"target-shape-freeze",
				started.Add(3*time.Hour),
			)
			mixed := cloneStage4LifecycleTables([]schema.Table{added})
			mixed[0].Columns = append(mixed[0].Columns, schema.Column{
				Name: "discarded_row_change", Type: "text", Nullable: true,
			})
			mixed = append(
				mixed,
				stage4LifecycleSimpleTable("source", "new_table"),
			)
			result, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(
					t,
					backend,
					"target-shape-freeze",
					false,
				),
				mixed,
				frozenOptions,
			)
			if err == nil ||
				!strings.Contains(err.Error(), "freeze mode rejects drift") {
				t.Fatalf("mixed discard_row/freeze error = %v", err)
			}
			if len(result.PreviousSnapshot.Tables) != 1 ||
				len(result.PreviousSnapshot.Tables[0].Columns) != 1 {
				t.Fatalf(
					"mixed policy did not evaluate the successful filtered source baseline: %#v",
					result.PreviousSnapshot,
				)
			}
		})
	}
}

func TestStage4TargetShapeAuthoritySameRunPartialStagingIsImmutable(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			t.Run("source staged first", func(t *testing.T) {
				backend := factory(t)
				started := time.Date(2026, 7, 31, 15, 0, 0, 0, time.UTC)
				options := stage4TargetShapeTestOptions(started)
				tables := []schema.Table{
					stage4LifecycleSimpleTable("source", "items"),
				}
				planner := &stage4TargetSchemaProjectionTestTarget{
					engine: "postgres", targetSchema: "target",
				}
				gate, authority, projection, pending :=
					stage4TargetShapeTestRun(
						t,
						backend,
						"source-first",
						started,
						tables,
						options,
						planner,
						stage4TargetShapeTestCatalog(t, nil, nil),
					)
				if err := backend.SaveSchemaSnapshot(gate.PendingSnapshot); err != nil {
					t.Fatal(err)
				}
				resumedGate, err := PrepareStage4SchemaGate(
					stage4LifecycleRunContext(
						t, backend, "source-first", true,
					),
					tables,
					options,
				)
				if err != nil {
					t.Fatal(err)
				}
				seed, err := NewStage4TargetShapeSeed(
					stage4TargetShapeTestCatalog(t, nil, nil),
				)
				if err != nil {
					t.Fatal(err)
				}
				resumedAuthority, err := PrepareStage4TargetShapeAuthority(
					stage4LifecycleRunContext(
						t, backend, "source-first", true,
					),
					resumedGate,
					options,
					seed,
				)
				if err != nil {
					t.Fatal(err)
				}
				resumedProjection, err :=
					BuildStage4TargetSchemaEvolutionProjection(
						resumedGate,
						resumedAuthority,
						"postgres",
						planner,
						"upsert",
					)
				if err != nil {
					t.Fatal(err)
				}
				resumedPending, err := BindStage4TargetShapeProjection(
					resumedAuthority,
					resumedProjection,
				)
				if err != nil {
					t.Fatal(err)
				}
				if pending.CanonicalJSON != resumedPending.CanonicalJSON ||
					authority.PriorCatalogDigest() !=
						resumedAuthority.PriorCatalogDigest() ||
					projection.CurrentDigest() !=
						resumedProjection.CurrentDigest() {
					t.Fatal("source-first partial resume reinterpreted evidence")
				}
			})

			t.Run("target staged first", func(t *testing.T) {
				backend := factory(t)
				started := time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)
				options := stage4TargetShapeTestOptions(started)
				tables := []schema.Table{
					stage4LifecycleSimpleTable("source", "items"),
				}
				planner := &stage4TargetSchemaProjectionTestTarget{
					engine: "postgres", targetSchema: "target",
				}
				_, _, _, pending := stage4TargetShapeTestRun(
					t,
					backend,
					"target-first",
					started,
					tables,
					options,
					planner,
					stage4TargetShapeTestCatalog(t, nil, nil),
				)
				if err := backend.SaveSchemaSnapshot(pending); err != nil {
					t.Fatal(err)
				}
				resumedGate, err := PrepareStage4SchemaGate(
					stage4LifecycleRunContext(
						t, backend, "target-first", true,
					),
					tables,
					options,
				)
				if err != nil {
					t.Fatal(err)
				}
				resumedAuthority, err := PrepareStage4TargetShapeAuthority(
					stage4LifecycleRunContext(
						t, backend, "target-first", true,
					),
					resumedGate,
					options,
					Stage4TargetShapeSeed{},
				)
				if err != nil {
					t.Fatal(err)
				}
				resumedProjection, err :=
					BuildStage4TargetSchemaEvolutionProjection(
						resumedGate,
						resumedAuthority,
						"postgres",
						planner,
						"upsert",
					)
				if err != nil {
					t.Fatal(err)
				}
				resumedPending, err := BindStage4TargetShapeProjection(
					resumedAuthority,
					resumedProjection,
				)
				if err != nil || resumedPending.CanonicalJSON != pending.CanonicalJSON {
					t.Fatalf(
						"target-first partial resume pending equal=%t err=%v",
						resumedPending.CanonicalJSON == pending.CanonicalJSON,
						err,
					)
				}

				changed := cloneStage4LifecycleTables(tables)
				changed[0].Columns = append(
					changed[0].Columns,
					schema.Column{
						Name: "changed", Type: "text", Nullable: true,
					},
				)
				changedGate, err := PrepareStage4SchemaGate(
					stage4LifecycleRunContext(
						t, backend, "target-first", true,
					),
					changed,
					options,
				)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := PrepareStage4TargetShapeAuthority(
					stage4LifecycleRunContext(
						t, backend, "target-first", true,
					),
					changedGate,
					options,
					Stage4TargetShapeSeed{},
				); err == nil ||
					!strings.Contains(
						err.Error(),
						"cannot be reinterpreted",
					) {
					t.Fatalf("target-first changed resume error = %v", err)
				}
			})
		})
	}
}

func TestStage4TargetShapeAuthorityTopologyEpochRequiresExactSeed(
	t *testing.T,
) {
	for name, factory := range stage4LifecycleBackendFactories() {
		t.Run(name, func(t *testing.T) {
			backend := factory(t)
			started := time.Date(2026, 7, 31, 17, 0, 0, 0, time.UTC)
			report := stage4TargetShapeTestOptions(started)
			report.Contract = &config.SchemaContract{
				Tables:   config.SchemaContractReport,
				Columns:  config.SchemaContractReport,
				DataType: config.SchemaContractReport,
			}
			tables := []schema.Table{
				stage4LifecycleSimpleTable("source", "items"),
			}
			planner := &stage4TargetSchemaProjectionTestTarget{
				engine: "postgres", targetSchema: "target",
			}
			gate, _, projection, pending := stage4TargetShapeTestRun(
				t,
				backend,
				"topology-report",
				started,
				tables,
				report,
				planner,
				stage4TargetShapeTestCatalog(
					t,
					projectionTargetTables(t, tables, planner),
					[]TargetSchemaEvolutionNameReservation{{
						Scope: "relation", Namespace: "target", Name: "view_a",
					}},
				),
			)
			stage4TargetShapeTestPublishSuccess(
				t,
				backend,
				"topology-report",
				started,
				gate,
				pending,
			)

			evolve := stage4TargetShapeTestOptions(started.Add(time.Hour))
			initializeStage4LifecycleRun(
				t, backend, "topology-evolve", started.Add(time.Hour),
			)
			evolveGate, err := PrepareStage4SchemaGate(
				stage4LifecycleRunContext(
					t, backend, "topology-evolve", false,
				),
				tables,
				evolve,
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = PrepareStage4TargetShapeAuthority(
				stage4LifecycleRunContext(
					t, backend, "topology-evolve", false,
				),
				evolveGate,
				evolve,
				Stage4TargetShapeSeed{},
			)
			if !errors.Is(err, ErrStage4TargetShapeSeedRequired) {
				t.Fatalf("new topology seed error = %v", err)
			}
			catalog := stage4TargetShapeTestCatalog(
				t,
				projection.CurrentTables(),
				[]TargetSchemaEvolutionNameReservation{{
					Scope: "relation", Namespace: "target", Name: "view_a",
				}},
			)
			seed, err := NewStage4TargetShapeSeed(catalog)
			if err != nil {
				t.Fatal(err)
			}
			authority, err := PrepareStage4TargetShapeAuthority(
				stage4LifecycleRunContext(
					t, backend, "topology-evolve", false,
				),
				evolveGate,
				evolve,
				seed,
			)
			if err != nil {
				t.Fatal(err)
			}
			if authority.TopologyHash() == gate.TopologyHash {
				t.Fatal("new policy reused old target authority topology")
			}
		})
	}
}

func stage4TargetShapeTestRun(
	t *testing.T,
	backend stage4LifecycleTestBackend,
	runID string,
	started time.Time,
	tables []schema.Table,
	options Stage4SchemaGateOptions,
	planner Stage4TargetSchemaPlanner,
	seedCatalog TargetSchemaEvolutionCatalog,
) (
	Stage4SchemaGateResult,
	Stage4TargetShapeAuthority,
	Stage4TargetSchemaEvolutionProjection,
	state.SchemaSnapshot,
) {
	t.Helper()
	initializeStage4LifecycleRun(t, backend, runID, started)
	run := stage4LifecycleRunContext(t, backend, runID, false)
	gate, err := PrepareStage4SchemaGate(run, tables, options)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := PrepareStage4TargetShapeAuthority(
		run,
		gate,
		options,
		Stage4TargetShapeSeed{},
	)
	if errors.Is(err, ErrStage4TargetShapeSeedRequired) {
		seed, seedErr := NewStage4TargetShapeSeed(seedCatalog)
		if seedErr != nil {
			t.Fatal(seedErr)
		}
		authority, err = PrepareStage4TargetShapeAuthority(
			run,
			gate,
			options,
			seed,
		)
	}
	if err != nil {
		t.Fatal(err)
	}
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		options.SourceEngine,
		planner,
		options.TargetMode,
	)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := BindStage4TargetShapeProjection(authority, projection)
	if err != nil {
		t.Fatal(err)
	}
	return gate, authority, projection, pending
}

func stage4TargetShapeTestPublishSuccess(
	t *testing.T,
	backend stage4LifecycleTestBackend,
	runID string,
	started time.Time,
	gate Stage4SchemaGateResult,
	target state.SchemaSnapshot,
) {
	t.Helper()
	if err := backend.SaveSchemaSnapshot(gate.PendingSnapshot); err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveSchemaSnapshot(target); err != nil {
		t.Fatal(err)
	}
	if err := backend.Append(state.Run{
		ID: runID, Source: "source", Target: "target",
		Outcome: state.Success, Resumable: false, Reason: "complete",
		StartedAt: started, EndedAt: started.Add(time.Minute),
	}); err != nil {
		t.Fatal(err)
	}
}

func stage4TargetShapeTestOptions(
	capturedAt time.Time,
) Stage4SchemaGateOptions {
	return Stage4SchemaGateOptions{
		SourceEngine:   "postgres",
		TargetEngine:   "postgres",
		TargetMode:     "upsert",
		ConfigIdentity: "target-shape-configuration",
		Contract: &config.SchemaContract{
			Tables:   config.SchemaContractEvolve,
			Columns:  config.SchemaContractEvolve,
			DataType: config.SchemaContractEvolve,
		},
		CapturedAt: capturedAt,
	}
}

func stage4TargetShapeTestCatalog(
	t *testing.T,
	tables []schema.Table,
	reservations []TargetSchemaEvolutionNameReservation,
) TargetSchemaEvolutionCatalog {
	t.Helper()
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, reservations)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func projectionTargetTables(
	t *testing.T,
	tables []schema.Table,
	planner Stage4TargetSchemaPlanner,
) []schema.Table {
	t.Helper()
	result, err := planner.PlanTables("postgres", tables, "upsert")
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func stage4TargetShapeRichTable(t *testing.T) schema.Table {
	t.Helper()
	check, err := schema.ParsePostgresCatalogCheck(
		`legacy_parent > 0`,
		[]schema.Column{{Name: "legacy_parent", Type: "bigint"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return schema.Table{
		Schema: "source",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{Name: "legacy_parent", Type: "bigint", Nullable: true},
			{Name: "legacy_code", Type: "text", Nullable: true},
		},
		Indexes: []schema.Index{{
			Name:    "items_legacy_code_idx",
			Columns: []schema.IndexColumn{{Name: "legacy_code"}},
		}},
		Checks: []schema.CheckConstraint{{
			Name: "items_legacy_code_check", Expression: check,
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:             "items_legacy_parent_fk",
			Columns:          []string{"legacy_parent"},
			ReferencedSchema: "source", ReferencedTable: "items",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION", OnDelete: "SET NULL", Match: "SIMPLE",
		}},
	}
}

func stage4TargetShapeTestAssertRetained(
	t *testing.T,
	tables []schema.Table,
) {
	t.Helper()
	table := stage4TargetSchemaProjectionFindTable(t, tables, "items")
	stage4TargetSchemaProjectionFindColumn(t, table, "legacy_parent")
	stage4TargetSchemaProjectionFindColumn(t, table, "legacy_code")
	if len(table.Indexes) != 1 ||
		len(table.Checks) != 1 ||
		len(table.ForeignKeys) != 1 {
		t.Fatalf(
			"retained index/CHECK/FK counts = %d/%d/%d",
			len(table.Indexes),
			len(table.Checks),
			len(table.ForeignKeys),
		)
	}
}
