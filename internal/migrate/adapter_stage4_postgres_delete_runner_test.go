package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestStage4PostgresDeleteCompositionAdmissionIsExact(t *testing.T) {
	tests := map[string]struct {
		mutate func(*config.Config, *stage4AdapterPrepared, *string, *string)
		want   string
	}{
		"mysql source": {
			mutate: func(_ *config.Config, _ *stage4AdapterPrepared, source, _ *string) {
				*source = "mysql"
			},
			want: "only postgres-to-postgres",
		},
		"mysql target": {
			mutate: func(_ *config.Config, _ *stage4AdapterPrepared, _, target *string) {
				*target = "mysql"
			},
			want: "only postgres-to-postgres",
		},
		"drop recreate": {
			mutate: func(cfg *config.Config, _ *stage4AdapterPrepared, _, _ *string) {
				cfg.Migration.TargetMode = "drop_recreate"
			},
			want: "requires target mode upsert",
		},
		"prepared mode differs": {
			mutate: func(_ *config.Config, prepared *stage4AdapterPrepared, _, _ *string) {
				prepared.mode = "drop_recreate"
			},
			want: "requires target mode upsert",
		},
		"strict": {
			mutate: func(cfg *config.Config, _ *stage4AdapterPrepared, _, _ *string) {
				cfg.Migration.StrictConsistency = true
			},
			want: "not yet composed with strict consistency",
		},
		"incremental config": {
			mutate: func(cfg *config.Config, _ *stage4AdapterPrepared, _, _ *string) {
				cfg.Migration.DateUpdatedColumns = []string{"updated_at"}
			},
			want: "full-table, non-incremental",
		},
		"incremental prepared state": {
			mutate: func(_ *config.Config, prepared *stage4AdapterPrepared, _, _ *string) {
				prepared.incremental = &stage4AdapterIncrementalPrepared{}
			},
			want: "full-table, non-incremental",
		},
		"pending evolution": {
			mutate: func(_ *config.Config, prepared *stage4AdapterPrepared, _, _ *string) {
				prepared.evolution = &stage4AdapterTargetSchemaEvolution{
					plan: TargetSchemaEvolutionPlan{
						target:          schema.Postgres,
						operations:      []TargetSchemaEvolutionOperation{{}},
						states:          [][]schema.Table{{}, {}},
						observedPrefix:  0,
						authorityDigest: "authority",
						digest:          "plan",
					},
				}
			},
			want: "evolution to be complete",
		},
		"delete mode off": {
			mutate: func(cfg *config.Config, _ *stage4AdapterPrepared, _, _ *string) {
				cfg.Migration.Deletes.Mode = config.DeleteModeOff
			},
			want: "requires reconcile/hard/interval",
		},
		"partitioned work": {
			mutate: func(_ *config.Config, prepared *stage4AdapterPrepared, _, _ *string) {
				prepared.work[0].task.Partition = "partition/0"
			},
			want: "exact unpartitioned",
		},
		"work inventory differs": {
			mutate: func(_ *config.Config, prepared *stage4AdapterPrepared, _, _ *string) {
				prepared.work = prepared.work[:1]
			},
			want: "one network work identity",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, prepared := stage4PostgresDeleteRunnerFixture()
			source, target := "postgres", "postgres"
			test.mutate(&cfg, &prepared, &source, &target)
			err := requireStage4AdapterPostgresDeleteComposition(
				cfg,
				source,
				target,
				prepared,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("admission error = %v; want %q", err, test.want)
			}
		})
	}

	cfg, prepared := stage4PostgresDeleteRunnerFixture()
	if err := requireStage4AdapterPostgresDeleteComposition(
		cfg,
		"postgres",
		"postgres",
		prepared,
	); err != nil {
		t.Fatalf("valid PostgreSQL delete composition: %v", err)
	}
}

func TestStage4PostgresDeleteAttemptIDBindsOnlyStableWorkIdentity(t *testing.T) {
	_, prepared := stage4PostgresDeleteRunnerFixture()
	work := prepared.work[0]
	first, err := stage4AdapterPostgresDeleteAttemptID("run-1", work)
	if err != nil {
		t.Fatal(err)
	}
	second, err := stage4AdapterPostgresDeleteAttemptID("run-1", work)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) != 64 {
		t.Fatalf("attempt IDs = %q and %q", first, second)
	}

	transient := work
	transient.ranges = []state.RangeState{{
		ID: "range/0", Strategy: work.strategy,
		TopologyHash: work.topology, RowsDone: 42,
	}}
	transient.pagination = PaginationPlan{Strategy: PaginationIntegerKeyset}
	transientID, err := stage4AdapterPostgresDeleteAttemptID(
		"run-1",
		transient,
	)
	if err != nil {
		t.Fatal(err)
	}
	if transientID != first {
		t.Fatalf("transient progress changed attempt ID: %s != %s", transientID, first)
	}

	changes := map[string]func(*string, *stage4AdapterWork){
		"run": func(runID *string, _ *stage4AdapterWork) {
			*runID = "run-2"
		},
		"task": func(_ *string, work *stage4AdapterWork) {
			work.task.Table = "child"
		},
		"strategy": func(_ *string, work *stage4AdapterWork) {
			work.strategy = "stage4_adapter_network_ranges_v2"
		},
		"topology": func(_ *string, work *stage4AdapterWork) {
			work.topology = "parent-final-v2"
		},
	}
	for name, mutate := range changes {
		t.Run(name, func(t *testing.T) {
			runID := "run-1"
			changed := work
			mutate(&runID, &changed)
			changedID, changedErr :=
				stage4AdapterPostgresDeleteAttemptID(runID, changed)
			if changedErr != nil {
				t.Fatal(changedErr)
			}
			if changedID == first {
				t.Fatalf("%s did not change attempt ID", name)
			}
		})
	}

	invalid := work
	invalid.task.Partition = "partition/0"
	if _, err := stage4AdapterPostgresDeleteAttemptID(
		"run-1",
		invalid,
	); err == nil {
		t.Fatal("partitioned work was admitted")
	}
}

func TestStage4PostgresDeleteReversePlanOrder(t *testing.T) {
	order, err := stage4AdapterPostgresDeleteReversePlanIndexes(4)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []int{3, 2, 1, 0}) {
		t.Fatalf("delete order = %v", order)
	}
	if _, err := stage4AdapterPostgresDeleteReversePlanIndexes(0); err == nil {
		t.Fatal("empty delete plan was admitted")
	}
}

func TestStage4PostgresDeleteTerminalStrictnessIsAuthenticated(t *testing.T) {
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: "source", Table: "items",
	}
	base := state.DeleteReconciliation{
		RunID: "run-1", Task: task, AttemptID: "attempt-1",
		Due: true, StartedAt: started, CompletedAt: started.Add(time.Minute),
	}
	tests := map[string]struct {
		outcome deleteReconcileOutcome
		want    bool
		wantErr string
	}{
		"completed zero candidates is strict": {
			outcome: deleteReconcileOutcome{
				Record: func() state.DeleteReconciliation {
					record := base
					record.Status = state.DeleteReconciliationCompleted
					return record
				}(),
				StrictCountValidation: true,
			},
			want: true,
		},
		"not due is permissive": {
			outcome: deleteReconcileOutcome{
				Record: func() state.DeleteReconciliation {
					record := base
					record.Due = false
					record.Status = state.DeleteReconciliationNotDue
					record.Reason = "interval has not elapsed"
					return record
				}(),
			},
		},
		"dry run cannot authenticate production": {
			outcome: deleteReconcileOutcome{
				Record: func() state.DeleteReconciliation {
					record := base
					record.DryRun = true
					record.Status = state.DeleteReconciliationDryRun
					return record
				}(),
			},
			wantErr: "dry-run evidence",
		},
		"running is not terminal": {
			outcome: deleteReconcileOutcome{
				Record: func() state.DeleteReconciliation {
					record := base
					record.Status = state.DeleteReconciliationRunning
					record.CompletedAt = time.Time{}
					return record
				}(),
			},
			wantErr: "remains running",
		},
		"incomplete fails closed": {
			outcome: deleteReconcileOutcome{
				Record: func() state.DeleteReconciliation {
					record := base
					record.Status = state.DeleteReconciliationIncomplete
					record.Reason = state.DeleteReconciliationReasonCancelled
					return record
				}(),
			},
			wantErr: "is incomplete",
		},
		"strict flag contradiction": {
			outcome: deleteReconcileOutcome{
				Record: func() state.DeleteReconciliation {
					record := base
					record.Status = state.DeleteReconciliationCompleted
					return record
				}(),
			},
			wantErr: "contradicts",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			strict, err := stage4AdapterPostgresDeleteTerminalStrictness(
				test.outcome,
			)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("strictness error = %v; want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || strict != test.want {
				t.Fatalf("strictness = %v, %v; want %v", strict, err, test.want)
			}
		})
	}
}

func TestStage4PostgresDeleteRequestUsesExactRunAuthorities(t *testing.T) {
	cfg, prepared := stage4PostgresDeleteRunnerFixture()
	fixedNow := time.Date(2026, 7, 31, 14, 30, 0, 0, time.UTC)
	backend := state.SQLiteStore{Path: t.TempDir() + "/state.db"}
	protector := &stage4PostgresDeleteRunnerProtector{}
	source := &stage4PostgresDeleteRunnerSource{}
	target := &stage4PostgresDeleteRunnerTarget{}
	canonicalizer := &stage4PostgresDeleteRunnerCanonicalizer{}
	entry := stage4AdapterPostgresDeleteEntry{
		planIndex: 0,
		source:    prepared.plans[0].source,
		target:    prepared.plans[0].target,
		capabilities: postgresDeleteReconciliationCapabilities{
			source: source, target: target, canonicalizer: canonicalizer,
		},
	}
	composition := &stage4AdapterPostgresDeletePrepared{
		run: Stage4RunContext{
			RunID:          "run-1",
			Backend:        backend,
			SpoolDirectory: t.TempDir(),
		},
		policy:        cfg.Migration.Deletes,
		maxBatchBytes: 8 << 20,
		protector:     protector,
		entries:       []stage4AdapterPostgresDeleteEntry{entry},
		now:           func() time.Time { return fixedNow },
	}
	reconciler, request, err := composition.requestFor(
		entry,
		prepared.work[0],
	)
	if err != nil {
		t.Fatal(err)
	}
	wantAttempt, err := stage4AdapterPostgresDeleteAttemptID(
		"run-1",
		prepared.work[0],
	)
	if err != nil {
		t.Fatal(err)
	}
	if request.RunID != "run-1" ||
		request.AttemptID != wantAttempt ||
		request.Task != prepared.work[0].task ||
		request.TargetMode != "upsert" || request.DryRun ||
		request.Policy != cfg.Migration.Deletes ||
		!request.Now.Equal(fixedNow) ||
		request.SpoolDirectory != composition.run.SpoolDirectory ||
		request.MaxBatchBytes != composition.maxBatchBytes ||
		!reflect.DeepEqual(request.SourceTable, entry.source) ||
		!reflect.DeepEqual(request.TargetTable, entry.target) {
		t.Fatalf("delete request is not exactly wired: %#v", request)
	}
	if reconciler.state != backend ||
		reconciler.source != source ||
		reconciler.target != target ||
		reconciler.canonicalizer != canonicalizer {
		t.Fatalf("delete reconciler authorities differ: %#v", reconciler)
	}
	called := false
	if err := reconciler.protector.ProtectDeleteMutation(
		context.Background(),
		func() error {
			called = true
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if !called || protector.calls != 1 {
		t.Fatalf("protector calls = %d, mutation called = %v", protector.calls, called)
	}
}

func TestStage4PostgresDeleteTerminalAuthorityIsAuthenticatedBeforeReuse(
	t *testing.T,
) {
	tests := map[string]struct {
		currentAuthorityID string
		terminal           bool
		wantErr            bool
		wantErrContains    string
		seed               func(
			*testing.T,
			state.SQLiteStore,
			deleteReconcileRequest,
			deleteKeyPlan,
		)
	}{
		"completed attempt": {
			currentAuthorityID: "postgres-test-authority-recreated",
			terminal:           true,
			wantErr:            true,
			wantErrContains:    "current PostgreSQL catalog",
			seed: func(
				t *testing.T,
				backend state.SQLiteStore,
				request deleteReconcileRequest,
				keyPlan deleteKeyPlan,
			) {
				t.Helper()
				stage4PostgresDeleteRunnerSeedCompletedAuthority(
					t,
					backend,
					request,
					request.AttemptID,
					keyPlan,
					request.Now.Add(-time.Minute),
				)
			},
		},
		"not-due attempt": {
			currentAuthorityID: "postgres-test-authority-recreated",
			terminal:           true,
			wantErr:            true,
			wantErrContains:    "current PostgreSQL catalog",
			seed: func(
				t *testing.T,
				backend state.SQLiteStore,
				request deleteReconcileRequest,
				keyPlan deleteKeyPlan,
			) {
				t.Helper()
				stage4PostgresDeleteRunnerSeedCompletedAuthority(
					t,
					backend,
					request,
					"prior-success",
					keyPlan,
					request.Now.Add(-time.Hour),
				)
				stored, created, err := backend.BeginDeleteReconciliation(
					state.DeleteReconciliation{
						RunID:     request.RunID,
						Task:      request.Task,
						AttemptID: request.AttemptID,
						Due:       false,
						Reason:    "reconciliation interval has not elapsed",
						StartedAt: request.Now,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if !created ||
					stored.Status != state.DeleteReconciliationNotDue {
					t.Fatalf(
						"seed not-due evidence = %#v, created=%v",
						stored,
						created,
					)
				}
			},
		},
		"not-due preserves safe batch setting changes": {
			currentAuthorityID: "postgres-test-authority-original",
			terminal:           true,
			seed: func(
				t *testing.T,
				backend state.SQLiteStore,
				request deleteReconcileRequest,
				keyPlan deleteKeyPlan,
			) {
				t.Helper()
				priorRequest := request
				priorRequest.Policy.Reconcile.BatchSize = 17
				priorRequest.MaxBatchBytes = 4 << 20
				stage4PostgresDeleteRunnerSeedCompletedAuthority(
					t,
					backend,
					priorRequest,
					"prior-success",
					keyPlan,
					request.Now.Add(-time.Hour),
				)
				stored, created, err := backend.BeginDeleteReconciliation(
					state.DeleteReconciliation{
						RunID:     request.RunID,
						Task:      request.Task,
						AttemptID: request.AttemptID,
						Due:       false,
						Reason:    "reconciliation interval has not elapsed",
						StartedAt: request.Now,
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				if !created ||
					stored.Status != state.DeleteReconciliationNotDue {
					t.Fatalf(
						"seed not-due evidence = %#v, created=%v",
						stored,
						created,
					)
				}
			},
		},
		"running crash plan": {
			currentAuthorityID: "postgres-test-authority-recreated",
			wantErr:            true,
			wantErrContains:    "no longer matches route proof",
			seed: func(
				t *testing.T,
				backend state.SQLiteStore,
				request deleteReconcileRequest,
				keyPlan deleteKeyPlan,
			) {
				t.Helper()
				stage4PostgresDeleteRunnerSeedRunningAuthority(
					t,
					backend,
					request,
					request.AttemptID,
					keyPlan,
					request.Now.Add(-time.Minute),
				)
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, fixture := stage4PostgresDeleteRunnerFixture()
			work := fixture.work[0]
			work.pagination = PaginationPlan{
				Strategy: PaginationIntegerKeyset,
				Keys: []KeySpec{{
					Name: "id", Kind: KeyInteger,
				}},
				Ranges: []PaginationRange{{ID: 0, Empty: true}},
			}
			workRange, err := stage4AdapterStateRange(
				work.pagination.Ranges[0],
				work.topology,
			)
			if err != nil {
				t.Fatal(err)
			}
			work.ranges = []state.RangeState{workRange}

			backend := state.SQLiteStore{
				Path: t.TempDir() + "/state.db",
			}
			startedAt := time.Date(
				2026, 7, 31, 13, 0, 0, 0, time.UTC,
			)
			if err := backend.InitializeRun(state.Run{
				ID:             "run-1",
				Source:         "source",
				Target:         "target",
				SourceEngine:   "postgres",
				SourceIdentity: "postgres:source.example:5432/app",
				TargetIdentity: "postgres:target.example:5432/app",
				Outcome:        state.Running,
				Resumable:      true,
				Reason:         "running",
				StartedAt:      startedAt,
			}, "config-run-1"); err != nil {
				t.Fatal(err)
			}
			created, err := backend.EnsureWorkPlan(
				state.WorkTask{
					RunID:        "run-1",
					Key:          work.task,
					Strategy:     work.strategy,
					TopologyHash: work.topology,
					StartedAt:    startedAt,
				},
				work.ranges,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !created {
				t.Fatal("durable final work was not created")
			}
			if err := backend.CompleteRange(
				"run-1",
				work.task,
				work.ranges[0].ID,
				work.topology,
				0,
				startedAt.Add(time.Minute),
			); err != nil {
				t.Fatal(err)
			}

			protector := &stage4PostgresDeleteRunnerProtector{}
			originalAuthority :=
				&stage4PostgresDeleteRunnerAuthorityCanonicalizer{
					id: "postgres-test-authority-original",
				}
			currentAuthority :=
				&stage4PostgresDeleteRunnerAuthorityCanonicalizer{
					id: test.currentAuthorityID,
				}
			entry := stage4AdapterPostgresDeleteEntry{
				planIndex: 0,
				source:    fixture.plans[0].source,
				target:    fixture.plans[0].target,
				capabilities: postgresDeleteReconciliationCapabilities{
					source:        &stage4PostgresDeleteRunnerSource{},
					target:        &stage4PostgresDeleteRunnerTarget{},
					canonicalizer: originalAuthority,
				},
				currentAuthority: func(
					context.Context,
				) (deleteKeyCanonicalizer, error) {
					return currentAuthority, nil
				},
			}
			composition := &stage4AdapterPostgresDeletePrepared{
				run: Stage4RunContext{
					RunID:          "run-1",
					Backend:        backend,
					SpoolDirectory: t.TempDir(),
				},
				policy:        cfg.Migration.Deletes,
				maxBatchBytes: 8 << 20,
				protector:     protector,
				entries:       []stage4AdapterPostgresDeleteEntry{entry},
				now: func() time.Time {
					return startedAt.Add(2 * time.Hour)
				},
			}
			_, request, err := composition.requestFor(entry, work)
			if err != nil {
				t.Fatal(err)
			}
			originalPlan, err := validateDeleteReconcileRequest(
				request,
				originalAuthority,
			)
			if err != nil {
				t.Fatal(err)
			}
			test.seed(t, backend, request, originalPlan)

			if test.terminal {
				strict, directErr :=
					authenticateStage4AdapterPostgresDeleteTerminal(
						context.Background(),
						composition,
						0,
						work,
					)
				if test.wantErr &&
					(directErr == nil || strict ||
						ClassifyTransferError(directErr) != ErrorClassState ||
						!strings.Contains(
							directErr.Error(),
							test.wantErrContains,
						)) {
					t.Fatalf(
						"direct terminal authority drift = strict %v, error %v",
						strict,
						directErr,
					)
				} else if !test.wantErr && (directErr != nil || strict) {
					t.Fatalf(
						"safe not-due authority = strict %v, error %v",
						strict,
						directErr,
					)
				}
			}

			result, err := composition.reconcile(
				context.Background(),
				[]stage4AdapterWork{work},
			)
			if test.wantErr {
				if err == nil || !strings.Contains(
					err.Error(),
					test.wantErrContains,
				) {
					t.Fatalf(
						"terminal authority drift error = %v; result=%#v",
						err,
						result,
					)
				}
				if test.terminal &&
					ClassifyTransferError(err) != ErrorClassState {
					t.Fatalf(
						"terminal authority drift class = %q: %v",
						ClassifyTransferError(err),
						err,
					)
				}
				if len(result.tables) != 0 ||
					len(result.strictByTable) != 0 {
					t.Fatalf(
						"stale terminal authority became reusable output: %#v",
						result,
					)
				}
			} else {
				if err != nil {
					t.Fatalf("safe not-due reuse: %v", err)
				}
				key := stage4RichTableKey{
					schema: entry.source.Schema,
					table:  entry.source.Name,
				}
				if len(result.tables) != 1 ||
					len(result.strictByTable) != 1 ||
					result.strictByTable[key] {
					t.Fatalf(
						"safe not-due result = %#v",
						result,
					)
				}
			}
			if protector.calls != 0 {
				t.Fatalf(
					"stale terminal authority reached %d target mutations",
					protector.calls,
				)
			}
		})
	}
}

func stage4PostgresDeleteRunnerSeedCompletedAuthority(
	t *testing.T,
	backend state.Stage4Backend,
	request deleteReconcileRequest,
	attemptID string,
	keyPlan deleteKeyPlan,
	completedAt time.Time,
) {
	t.Helper()
	stage4PostgresDeleteRunnerSeedRunningAuthority(
		t,
		backend,
		request,
		attemptID,
		keyPlan,
		completedAt.Add(-time.Minute),
	)
	if err := backend.FinishDeleteReconciliation(
		state.DeleteReconciliationResult{
			RunID: request.RunID, Task: request.Task,
			AttemptID:   attemptID,
			Status:      state.DeleteReconciliationCompleted,
			CompletedAt: completedAt,
		},
	); err != nil {
		t.Fatal(err)
	}
}

func stage4PostgresDeleteRunnerSeedRunningAuthority(
	t *testing.T,
	backend state.Stage4Backend,
	request deleteReconcileRequest,
	attemptID string,
	keyPlan deleteKeyPlan,
	startedAt time.Time,
) {
	t.Helper()
	stored, created, err := backend.BeginDeleteReconciliation(
		state.DeleteReconciliation{
			RunID: request.RunID, Task: request.Task,
			AttemptID: attemptID, Due: true,
			StartedAt: startedAt,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !created || stored.Status != state.DeleteReconciliationRunning {
		t.Fatalf(
			"seed running delete authority = %#v, created=%v",
			stored,
			created,
		)
	}
	batchSize, err := deleteBatchLimit(
		request.Policy.Reconcile.BatchSize,
		postgresDeleteMaximumParameters,
		len(keyPlan.targetColumns),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.SaveDeleteReconciliationPlan(
		state.DeleteReconciliationPlan{
			RunID: request.RunID, Task: request.Task,
			AttemptID:           attemptID,
			PlanID:              "plan-" + attemptID,
			SpoolPath:           request.SpoolDirectory + "/plan-" + attemptID,
			EqualityProofDigest: keyPlan.proofDigest,
			CandidateDigest:     strings.Repeat("a", 64),
			Candidates:          0,
			BatchSize:           batchSize,
			BatchByteLimit:      request.MaxBatchBytes,
			KeyWidth:            len(keyPlan.targetColumns),
			PlannedAt:           startedAt.Add(time.Second),
		},
	); err != nil {
		t.Fatal(err)
	}
}

func TestStage4PostgresDeleteFinalWorkBindingIsIdentityBased(t *testing.T) {
	_, prepared := stage4PostgresDeleteRunnerFixture()
	composition := &stage4AdapterPostgresDeletePrepared{
		entries: []stage4AdapterPostgresDeleteEntry{
			{planIndex: 0, source: prepared.plans[0].source, target: prepared.plans[0].target},
			{planIndex: 1, source: prepared.plans[1].source, target: prepared.plans[1].target},
		},
	}
	bound, err := composition.bindFinalWork([]stage4AdapterWork{
		prepared.work[1],
		prepared.work[0],
	})
	if err != nil {
		t.Fatal(err)
	}
	if bound[0].task.Table != "parent" || bound[1].task.Table != "child" {
		t.Fatalf("bound work = %#v", bound)
	}
	duplicate := []stage4AdapterWork{prepared.work[0], prepared.work[0]}
	if _, err := composition.bindFinalWork(duplicate); err == nil ||
		!strings.Contains(err.Error(), "duplicate final work") {
		t.Fatalf("duplicate final-work error = %v", err)
	}
}

func TestStage4PostgresDeleteRequiresDurablyCompletedFinalRanges(t *testing.T) {
	_, fixture := stage4PostgresDeleteRunnerFixture()
	work := fixture.work[:1]
	work[0].pagination = PaginationPlan{
		Strategy: PaginationIntegerKeyset,
		Keys:     []KeySpec{{Name: "id", Kind: KeyInteger}},
		Ranges:   []PaginationRange{{ID: 0, Empty: true}},
	}
	workRange, err := stage4AdapterStateRange(
		work[0].pagination.Ranges[0],
		work[0].topology,
	)
	if err != nil {
		t.Fatal(err)
	}
	work[0].ranges = []state.RangeState{workRange}
	composition := &stage4AdapterPostgresDeletePrepared{
		run: Stage4RunContext{RunID: "run-1"},
		entries: []stage4AdapterPostgresDeleteEntry{{
			planIndex: 0,
			source:    fixture.plans[0].source,
			target:    fixture.plans[0].target,
		}},
	}
	startedAt := time.Date(2026, 7, 31, 14, 59, 0, 0, time.UTC)
	completedAt := time.Date(2026, 7, 31, 15, 0, 0, 0, time.UTC)
	validInventory := func() stage4WorkInventory {
		return stage4WorkInventory{
			tasks: map[state.TaskKey]state.WorkTask{
				work[0].task: {
					RunID: "run-1", Key: work[0].task,
					Status: "running", Strategy: work[0].strategy,
					TopologyHash: work[0].topology,
					StartedAt:    startedAt,
					UpdatedAt:    completedAt,
				},
			},
			ranges: map[state.TaskKey][]state.RangeState{
				work[0].task: {
					{
						RunID: "run-1", Task: work[0].task,
						ID: "range/0", Status: "completed",
						Strategy:     work[0].strategy,
						TopologyHash: work[0].topology,
						UpdatedAt:    completedAt,
						CompletedAt:  completedAt,
					},
				},
			},
		}
	}
	if err := composition.authenticateFinalWork(
		work,
		validInventory(),
	); err != nil {
		t.Fatalf("valid final work: %v", err)
	}

	tests := map[string]struct {
		mutate func(stage4WorkInventory) stage4WorkInventory
		want   string
	}{
		"missing task": {
			mutate: func(inventory stage4WorkInventory) stage4WorkInventory {
				delete(inventory.tasks, work[0].task)
				return inventory
			},
			want: "orphaned Stage 4 network ranges",
		},
		"task topology": {
			mutate: func(inventory stage4WorkInventory) stage4WorkInventory {
				task := inventory.tasks[work[0].task]
				task.TopologyHash = "different"
				inventory.tasks[work[0].task] = task
				return inventory
			},
			want: "work topology changed",
		},
		"range running": {
			mutate: func(inventory stage4WorkInventory) stage4WorkInventory {
				ranges := append([]state.RangeState(nil), inventory.ranges[work[0].task]...)
				ranges[0].Status = "running"
				ranges[0].CompletedAt = time.Time{}
				inventory.ranges[work[0].task] = ranges
				return inventory
			},
			want: "is not durably complete",
		},
		"pending range receipt": {
			mutate: func(inventory stage4WorkInventory) stage4WorkInventory {
				ranges := append([]state.RangeState(nil), inventory.ranges[work[0].task]...)
				ranges[0].Pending = []state.PendingAcknowledgement{{Sequence: 1}}
				inventory.ranges[work[0].task] = ranges
				return inventory
			},
			want: "unsafe network evidence",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := composition.authenticateFinalWork(
				work,
				test.mutate(validInventory()),
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("authentication error = %v; want %q", err, test.want)
			}
		})
	}
}

func TestStage4PostgresDeleteBatchByteLimitIsBounded(t *testing.T) {
	if _, err := stage4AdapterPostgresDeleteBatchByteLimit(0); err == nil {
		t.Fatal("zero memory ceiling was admitted")
	}
	limit, err := stage4AdapterPostgresDeleteBatchByteLimit(64 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if limit != 16<<20 {
		t.Fatalf("64 MiB limit = %d", limit)
	}
	limit, err = stage4AdapterPostgresDeleteBatchByteLimit(1 << 40)
	if err != nil {
		t.Fatal(err)
	}
	if limit != postgresDeleteMaximumBatchBytes {
		t.Fatalf("large limit = %d", limit)
	}
}

func stage4PostgresDeleteRunnerFixture() (
	config.Config,
	stage4AdapterPrepared,
) {
	parentSource := stage4PostgresDeleteRunnerTable("source", "parent")
	parentTarget := stage4PostgresDeleteRunnerTable("target", "parent")
	childSource := stage4PostgresDeleteRunnerTable("source", "child")
	childTarget := stage4PostgresDeleteRunnerTable("target", "child")
	cfg := config.Config{Migration: config.Migration{
		TargetMode:         "upsert",
		MemoryCeilingBytes: 64 << 20,
		Deletes: config.DeletePolicy{
			Mode:           config.DeleteModeReconcile,
			TargetBehavior: config.DeleteTargetHard,
			Reconcile: config.DeleteReconcilePolicy{
				Schedule:          config.DeleteScheduleInterval,
				Interval:          24 * time.Hour,
				BatchSize:         100,
				RequirePrimaryKey: true,
			},
		},
	}}
	prepared := stage4AdapterPrepared{
		mode: "upsert",
		plans: []adapterTablePlan{
			{source: parentSource, target: parentTarget, columns: []string{"id"}},
			{source: childSource, target: childTarget, columns: []string{"id"}},
		},
		work: []stage4AdapterWork{
			{
				task: state.TaskKey{
					Type: stage4AdapterNetworkTaskType, Schema: "source", Table: "parent",
				},
				strategy: stage4AdapterCopyStrategy,
				topology: "parent-final-v1",
			},
			{
				task: state.TaskKey{
					Type: stage4AdapterNetworkTaskType, Schema: "source", Table: "child",
				},
				strategy: stage4AdapterCopyStrategy,
				topology: "child-final-v1",
			},
		},
	}
	return cfg, prepared
}

func stage4PostgresDeleteRunnerTable(namespace, name string) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
		},
	}
}

type stage4PostgresDeleteRunnerProtector struct {
	calls int
}

func (protector *stage4PostgresDeleteRunnerProtector) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	protector.calls++
	return mutation()
}

type stage4PostgresDeleteRunnerSource struct{}

func (*stage4PostgresDeleteRunnerSource) OpenDeletePrimaryKeys(
	context.Context,
	schema.Table,
	[]string,
) (deleteKeyRows, error) {
	return nil, errors.New("not used")
}

type stage4PostgresDeleteRunnerTarget struct{}

func (*stage4PostgresDeleteRunnerTarget) OpenDeletePrimaryKeys(
	context.Context,
	schema.Table,
	[]string,
) (deleteKeyRows, error) {
	return nil, errors.New("not used")
}

func (*stage4PostgresDeleteRunnerTarget) MaxDeleteParameters() int {
	return postgresDeleteMaximumParameters
}

func (*stage4PostgresDeleteRunnerTarget) ApplyDeleteBatch(
	context.Context,
	deleteTargetBatch,
) (deleteTargetBatchReceipt, error) {
	return deleteTargetBatchReceipt{}, errors.New("not used")
}

type stage4PostgresDeleteRunnerCanonicalizer struct{}

func (*stage4PostgresDeleteRunnerCanonicalizer) ProveDeleteKeyEquality(
	schema.Table,
	schema.Table,
	[]schema.Column,
	[]schema.Column,
) (deleteKeyEqualityProof, error) {
	return deleteKeyEqualityProof{}, errors.New("not used")
}

func (*stage4PostgresDeleteRunnerCanonicalizer) CanonicalizeDeleteKeyValue(
	deleteKeySide,
	deleteKeyEqualityProof,
	int,
	any,
) (deleteCanonicalValue, error) {
	return deleteCanonicalValue{}, errors.New("not used")
}

type stage4PostgresDeleteRunnerAuthorityCanonicalizer struct {
	id string
}

func (canonicalizer *stage4PostgresDeleteRunnerAuthorityCanonicalizer) ProveDeleteKeyEquality(
	source schema.Table,
	target schema.Table,
	sourcePrimaryKey []schema.Column,
	targetPrimaryKey []schema.Column,
) (deleteKeyEqualityProof, error) {
	if canonicalizer == nil || strings.TrimSpace(canonicalizer.id) == "" {
		return deleteKeyEqualityProof{}, errors.New("authority is unavailable")
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(
		source,
		sourcePrimaryKey,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(
		target,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	columns := make([]deleteKeyColumnProof, len(sourcePrimaryKey))
	for index := range columns {
		columns[index].Semantics = "integer"
	}
	return deleteKeyEqualityProof{
		CanonicalizerID:   canonicalizer.id,
		SourceFingerprint: sourceFingerprint,
		TargetFingerprint: targetFingerprint,
		Columns:           columns,
	}, nil
}

func (*stage4PostgresDeleteRunnerAuthorityCanonicalizer) CanonicalizeDeleteKeyValue(
	deleteKeySide,
	deleteKeyEqualityProof,
	int,
	any,
) (deleteCanonicalValue, error) {
	return deleteCanonicalValue{}, errors.New("not used")
}
