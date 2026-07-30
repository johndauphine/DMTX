package state

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestRunLeaseEvidenceRoundTripsAndIsImmutable(t *testing.T) {
	for name, backend := range runLeaseEvidenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			startedAt := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
			initial := boundRunLeaseFixture(startedAt)
			if err := backend.InitializeRun(initial, "compatibility-hash"); err != nil {
				t.Fatal(err)
			}

			transition := Run{
				ID:        initial.ID,
				Outcome:   Failed,
				Resumable: true,
				Reason:    "interrupted",
				StartedAt: initial.StartedAt,
				EndedAt:   startedAt.Add(time.Minute),
			}
			if err := backend.Append(transition); err != nil {
				t.Fatalf("append transition with inherited lease evidence: %v", err)
			}

			latest, found, err := backend.Latest()
			if err != nil {
				t.Fatal(err)
			}
			if !found {
				t.Fatal("bound run disappeared")
			}
			assertRunLeaseEvidence(t, latest, initial)
			lease, err := latest.BoundLease()
			if err != nil {
				t.Fatalf("read bound lease: %v", err)
			}
			wantLease := Lease{
				Target: initial.LeaseTarget, RunID: initial.ID,
				OwnerToken: initial.LeaseOwnerToken,
				Generation: initial.LeaseGeneration,
			}
			if !reflect.DeepEqual(lease, wantLease) {
				t.Fatalf("bound lease = %#v, want %#v", lease, wantLease)
			}

			mutated := transition
			mutated.LeaseTarget = initial.LeaseTarget
			mutated.LeaseOwnerToken = initial.LeaseOwnerToken
			mutated.LeaseGeneration = initial.LeaseGeneration + 1
			mutated.Outcome = Success
			mutated.Resumable = false
			mutated.Reason = "migration completed"
			if err := backend.Append(mutated); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("mutated lease evidence error = %v, want immutable evidence", err)
			}
		})
	}
}

func TestFencedRunInitialBindAndResumeRebind(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			directory := t.TempDir()
			leaseStore := SQLiteStore{
				Path: filepath.Join(directory, "leases.db"),
			}
			raw := runLeaseBackendForKind(t, stateKind, directory)
			startedAt := time.Date(2026, 7, 30, 12, 30, 0, 0, time.UTC)

			first, err := leaseStore.AcquireLease(
				"postgres:target.example:5432/app/public",
				"resumed-run",
				time.Minute,
			)
			if err != nil {
				t.Fatal(err)
			}
			firstFenced := FenceBackend(
				raw,
				NewLeaseGuard(leaseStore, first),
			)
			initial := boundRunLeaseFixture(startedAt)
			initial.ID = first.RunID
			initial.LeaseTarget = ""
			initial.LeaseOwnerToken = ""
			initial.LeaseGeneration = 0
			conflicting := initial
			conflicting.LeaseTarget = first.Target
			conflicting.LeaseOwnerToken = "different-owner"
			conflicting.LeaseGeneration = first.Generation
			if err := firstFenced.InitializeRun(
				conflicting,
				"compatibility-hash",
			); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("conflicting initial binding error = %v, want lease loss", err)
			}
			if err := firstFenced.InitializeRun(
				initial,
				"compatibility-hash",
			); err != nil {
				t.Fatalf("initialize fenced run: %v", err)
			}
			assertLatestRunLease(t, raw, first)
			if err := firstFenced.UpdateRecoverableOutcome(
				first.RunID,
				Failed,
				"interrupted",
				startedAt.Add(time.Minute),
			); err != nil {
				t.Fatalf("record first attempt: %v", err)
			}
			if err := leaseStore.ReleaseLease(first); err != nil {
				t.Fatalf("release first lease: %v", err)
			}

			second, err := leaseStore.AcquireLease(
				first.Target,
				first.RunID,
				time.Minute,
			)
			if err != nil {
				t.Fatalf("acquire resume lease: %v", err)
			}
			if second.Generation <= first.Generation ||
				second.OwnerToken == first.OwnerToken {
				t.Fatalf(
					"resume lease = generation %d token %q; first = %d %q",
					second.Generation,
					second.OwnerToken,
					first.Generation,
					first.OwnerToken,
				)
			}
			secondFenced := FenceBackend(
				raw,
				NewLeaseGuard(leaseStore, second),
			)

			if err := secondFenced.ReactivateRun(
				second.RunID,
				"resume before rebind must fail",
			); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("unbound resume error = %v, want lease loss", err)
			}
			if err := firstFenced.BindRunLease(
				first.RunID,
				first,
			); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale rebind error = %v, want lease loss", err)
			}
			if err := secondFenced.BindRunLease(
				second.RunID,
				second,
			); err != nil {
				t.Fatalf("bind resume lease: %v", err)
			}
			if err := secondFenced.ReactivateRun(
				second.RunID,
				"resume in progress",
			); err != nil {
				t.Fatalf("reactivate rebound run: %v", err)
			}
			assertAllRunLeaseEvidence(t, raw, second)

			mutated := initial
			mutated.Outcome = Success
			mutated.Resumable = false
			mutated.Reason = "ordinary transition cannot rebind"
			mutated.LeaseTarget = second.Target
			mutated.LeaseOwnerToken = "unauthorized-owner"
			mutated.LeaseGeneration = second.Generation + 1
			if err := raw.Append(mutated); !errors.Is(
				err,
				ErrImmutableEvidence,
			) {
				t.Fatalf("ordinary rebind error = %v, want immutable evidence", err)
			}
		})
	}
}

func TestFencedMutationRejectsLegacyUnboundRun(t *testing.T) {
	for _, stateKind := range []string{"sqlite", "yaml"} {
		t.Run(stateKind, func(t *testing.T) {
			directory := t.TempDir()
			raw := runLeaseBackendForKind(t, stateKind, directory)
			run := Run{
				ID: "legacy-run", Source: "source", Target: "target",
				Outcome: Running, Resumable: true, Reason: "legacy",
				StartedAt: time.Date(2026, 7, 30, 12, 45, 0, 0, time.UTC),
			}
			if err := raw.InitializeRun(run, "legacy-hash"); err != nil {
				t.Fatal(err)
			}
			leaseStore := SQLiteStore{
				Path: filepath.Join(directory, "leases.db"),
			}
			lease, err := leaseStore.AcquireLease(
				"postgres:target",
				run.ID,
				time.Minute,
			)
			if err != nil {
				t.Fatal(err)
			}
			fenced := FenceBackend(raw, NewLeaseGuard(leaseStore, lease))
			if err := fenced.Append(Run{
				ID: run.ID, Outcome: Failed, Resumable: true,
				Reason: "must bind first", StartedAt: run.StartedAt,
			}); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("legacy fenced mutation error = %v, want lease loss", err)
			}
		})
	}
}

func TestRunLifecycleTransitionsPreserveLeaseEvidence(t *testing.T) {
	for name, backend := range runLeaseEvidenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			startedAt := time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)
			initial := boundRunLeaseFixture(startedAt)
			if err := backend.InitializeRun(initial, "compatibility-hash"); err != nil {
				t.Fatal(err)
			}
			if err := backend.UpdateRecoverableOutcome(
				initial.ID,
				Failed,
				"target commit may precede checkpoint",
				startedAt.Add(time.Minute),
			); err != nil {
				t.Fatalf("record recoverable outcome: %v", err)
			}
			assertLatestRunLeaseEvidence(t, backend, initial)

			if err := backend.ReactivateRun(initial.ID, "resume after lease acquisition"); err != nil {
				t.Fatalf("reactivate run: %v", err)
			}
			assertLatestRunLeaseEvidence(t, backend, initial)

			if err := backend.AbandonRun(
				initial.ID,
				"operator abandoned migration",
				startedAt.Add(2*time.Minute),
			); err != nil {
				t.Fatalf("abandon run: %v", err)
			}
			assertLatestRunLeaseEvidence(t, backend, initial)
		})
	}
}

func TestRunLeaseEvidenceRequiresCompletePositiveTuple(t *testing.T) {
	valid := boundRunLeaseFixture(time.Now().UTC())
	tests := map[string]Run{
		"target only": func() Run {
			run := valid
			run.LeaseOwnerToken = ""
			run.LeaseGeneration = 0
			return run
		}(),
		"token only": func() Run {
			run := valid
			run.LeaseTarget = ""
			run.LeaseGeneration = 0
			return run
		}(),
		"generation only": func() Run {
			run := valid
			run.LeaseTarget = ""
			run.LeaseOwnerToken = ""
			return run
		}(),
		"zero generation": func() Run {
			run := valid
			run.LeaseGeneration = 0
			return run
		}(),
		"negative generation": func() Run {
			run := valid
			run.LeaseGeneration = -1
			return run
		}(),
		"blank target": func() Run {
			run := valid
			run.LeaseTarget = " \t "
			return run
		}(),
		"blank token": func() Run {
			run := valid
			run.LeaseOwnerToken = "\n"
			return run
		}(),
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			if err := validateRunLeaseEvidence(run); err == nil {
				t.Fatal("invalid lease evidence was accepted")
			}
			if _, err := run.BoundLease(); !errors.Is(
				err,
				ErrRunLeaseEvidenceUnavailable,
			) {
				t.Fatalf("ownership error = %v, want unavailable evidence", err)
			}
		})
	}
	if err := validateRunLeaseEvidence(Run{ID: "legacy"}); err != nil {
		t.Fatalf("legacy blank lease evidence rejected: %v", err)
	}
}

func TestLegacyRunWithoutLeaseEvidenceIsReadableButCannotProveOwnership(t *testing.T) {
	for name, backend := range runLeaseEvidenceBackends(t) {
		t.Run(name, func(t *testing.T) {
			legacy := Run{
				ID:        "legacy-run",
				Source:    "source",
				Target:    "target",
				Outcome:   Running,
				Resumable: true,
				Reason:    "legacy migration",
				StartedAt: time.Date(2026, 7, 30, 14, 0, 0, 0, time.UTC),
			}
			if err := backend.InitializeRun(legacy, "legacy-hash"); err != nil {
				t.Fatal(err)
			}
			latest, found, err := backend.Latest()
			if err != nil || !found {
				t.Fatalf("read legacy run: found=%v err=%v", found, err)
			}
			if _, err := latest.BoundLease(); !errors.Is(err, ErrRunLeaseEvidenceUnavailable) {
				t.Fatalf("legacy ownership error = %v, want unavailable evidence", err)
			}

			retrofit := legacy
			retrofit.Outcome = Failed
			retrofit.LeaseTarget = "postgres:target.example:5432/app/public"
			retrofit.LeaseOwnerToken = "new-owner-token"
			retrofit.LeaseGeneration = 1
			if err := backend.Append(retrofit); !errors.Is(err, ErrImmutableEvidence) {
				t.Fatalf("legacy lease retrofit error = %v, want immutable evidence", err)
			}
		})
	}
}

func TestSQLiteRunLeaseEvidenceAdditiveMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE runs (
			id TEXT NOT NULL, source TEXT NOT NULL, target TEXT NOT NULL,
			outcome TEXT NOT NULL, source_engine TEXT NOT NULL DEFAULT '',
			source_identity TEXT NOT NULL DEFAULT '',
			target_identity TEXT NOT NULL DEFAULT '',
			resumable INTEGER NOT NULL, reason TEXT NOT NULL,
			started_at DATETIME NOT NULL, ended_at DATETIME,
			PRIMARY KEY (id, outcome)
		);
		INSERT INTO runs (
			id, source, target, outcome, source_engine, source_identity,
			target_identity, resumable, reason, started_at, ended_at
		) VALUES (
			'legacy', 'source', 'target', 'success', '', '', '',
			0, 'completed', '2026-07-30T15:00:00Z', '2026-07-30T15:01:00Z'
		);
	`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	store := SQLiteStore{Path: path}
	runs, err := store.List()
	if err != nil {
		t.Fatalf("read migrated state: %v", err)
	}
	if len(runs) != 1 || runs[0].ID != "legacy" {
		t.Fatalf("migrated runs = %#v", runs)
	}
	if _, err := runs[0].BoundLease(); !errors.Is(err, ErrRunLeaseEvidenceUnavailable) {
		t.Fatalf("migrated legacy ownership error = %v", err)
	}

	database, err = store.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	rows, err := database.Query(`PRAGMA table_info(runs)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var ordinal int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(
			&ordinal,
			&name,
			&columnType,
			&notNull,
			&defaultValue,
			&primaryKey,
		); err != nil {
			t.Fatal(err)
		}
		columns[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"lease_target",
		"lease_owner_token",
		"lease_generation",
	} {
		if !columns[name] {
			t.Fatalf("additive migration omitted %q", name)
		}
	}
}

func TestYAMLRunLeaseEvidenceWireKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	store := YAMLStore{Path: path}
	run := boundRunLeaseFixture(time.Date(2026, 7, 30, 16, 0, 0, 0, time.UTC))
	if err := store.InitializeRun(run, "compatibility-hash"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, wire := range []string{
		"lease_target: " + run.LeaseTarget,
		"lease_owner_token: " + run.LeaseOwnerToken,
		"lease_generation: 7",
	} {
		if !strings.Contains(text, wire) {
			t.Fatalf("YAML state omitted %q:\n%s", wire, text)
		}
	}
}

func runLeaseEvidenceBackends(t *testing.T) map[string]Backend {
	t.Helper()
	return map[string]Backend{
		"sqlite": SQLiteStore{Path: filepath.Join(t.TempDir(), "state.db")},
		"yaml":   YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
	}
}

func runLeaseBackendForKind(
	t *testing.T,
	kind string,
	directory string,
) Backend {
	t.Helper()
	switch kind {
	case "sqlite":
		return SQLiteStore{Path: filepath.Join(directory, "state.db")}
	case "yaml":
		return YAMLStore{Path: filepath.Join(directory, "state.yaml")}
	default:
		t.Fatalf("unknown state backend %q", kind)
		return nil
	}
}

func boundRunLeaseFixture(startedAt time.Time) Run {
	return Run{
		ID:              "bound-run",
		Source:          "postgres://source",
		Target:          "postgres://target",
		SourceEngine:    "postgres",
		SourceIdentity:  "postgres:source.example:5432/app/public",
		TargetIdentity:  "postgres:target.example:5432/app/public",
		LeaseTarget:     "postgres:target.example:5432/app/public",
		LeaseOwnerToken: "owner-token-0123456789abcdef",
		LeaseGeneration: 7,
		Outcome:         Running,
		Resumable:       true,
		Reason:          "migration in progress",
		StartedAt:       startedAt,
	}
}

func assertLatestRunLeaseEvidence(t *testing.T, backend Backend, want Run) {
	t.Helper()
	latest, found, err := backend.Latest()
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("run disappeared")
	}
	assertRunLeaseEvidence(t, latest, want)
}

func assertRunLeaseEvidence(t *testing.T, got, want Run) {
	t.Helper()
	if got.LeaseTarget != want.LeaseTarget ||
		got.LeaseOwnerToken != want.LeaseOwnerToken ||
		got.LeaseGeneration != want.LeaseGeneration {
		t.Fatalf(
			"lease evidence = (%q, %q, %d), want (%q, %q, %d)",
			got.LeaseTarget,
			got.LeaseOwnerToken,
			got.LeaseGeneration,
			want.LeaseTarget,
			want.LeaseOwnerToken,
			want.LeaseGeneration,
		)
	}
}

func assertLatestRunLease(t *testing.T, backend Backend, want Lease) {
	t.Helper()
	latest, found, err := backend.Latest()
	if err != nil || !found {
		t.Fatalf("read latest bound run: found=%v err=%v", found, err)
	}
	got, err := latest.BoundLease()
	if err != nil {
		t.Fatalf("read latest bound lease: %v", err)
	}
	if !sameLease(got, want) {
		t.Fatalf("latest bound lease = %#v, want %#v", got, want)
	}
}

func assertAllRunLeaseEvidence(t *testing.T, backend Backend, want Lease) {
	t.Helper()
	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) == 0 {
		t.Fatal("run history is empty")
	}
	for _, run := range runs {
		if run.ID != want.RunID {
			continue
		}
		got, err := run.BoundLease()
		if err != nil {
			t.Fatalf("read bound lease for %q/%q: %v", run.ID, run.Outcome, err)
		}
		if !sameLease(got, want) {
			t.Fatalf(
				"bound lease for %q/%q = %#v, want %#v",
				run.ID,
				run.Outcome,
				got,
				want,
			)
		}
	}
}
