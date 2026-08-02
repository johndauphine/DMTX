package app

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestRunStoresCompletedTableCheckpoint(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	wantSourceIdentity, err := endpointWorkloadIdentity(
		config.Endpoint{Type: "sqlite", Database: sourcePath},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantTargetIdentity, err := endpointWorkloadIdentity(
		config.Endpoint{Type: "sqlite", Database: targetPath},
	)
	if err != nil {
		t.Fatal(err)
	}

	var output, errors bytes.Buffer
	if code := Run([]string{"run", "--config", configPath}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	latest, found, err := (state.SQLiteStore{Path: configPath + ".state.db"}).Latest()
	if err != nil || !found {
		t.Fatalf("latest = %#v, found = %v, error = %v", latest, found, err)
	}
	if latest.SourceIdentity != wantSourceIdentity ||
		latest.TargetIdentity != wantTargetIdentity {
		t.Fatalf(
			"run workload identities = (%q, %q), want (%q, %q)",
			latest.SourceIdentity,
			latest.TargetIdentity,
			wantSourceIdentity,
			wantTargetIdentity,
		)
	}
	if latest.SourceEngine != "sqlite" {
		t.Fatalf("source engine = %q, want sqlite", latest.SourceEngine)
	}
	boundLease, err := latest.BoundLease()
	if err != nil {
		t.Fatalf("bound run lease: %v", err)
	}
	if boundLease.RunID != latest.ID ||
		boundLease.Target == "" ||
		boundLease.OwnerToken == "" ||
		boundLease.Generation < 1 {
		t.Fatalf("bound run lease = %#v", boundLease)
	}
	var status bytes.Buffer
	if code := showState(
		[]string{"--state", configPath + ".state.db"},
		&status,
		true,
	); code != Success {
		t.Fatalf("status exit code = %d", code)
	}
	if strings.Contains(status.String(), boundLease.OwnerToken) ||
		strings.Contains(status.String(), "lease_owner_token") {
		t.Fatalf("public status leaked lease owner token: %s", status.String())
	}
	tasks, err := (state.SQLiteStore{Path: configPath + ".state.db"}).ListTasks(latest.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].Table != "users" || tasks[0].Status != "completed" {
		t.Fatalf("tasks = %#v", tasks)
	}
	auditStream, err := os.ReadFile(configPath + ".audit.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(auditStream), `"type":"run_started"`) || !strings.Contains(string(auditStream), `"type":"run_succeeded"`) {
		t.Fatalf("audit stream = %q", auditStream)
	}
}

func TestTableCheckpointObserverExposesFreshAndResumeStage4Contexts(t *testing.T) {
	directory := t.TempDir()
	statePath := filepath.Join(directory, "migration.state.db")
	raw := state.SQLiteStore{Path: statePath}

	for _, test := range []struct {
		name   string
		runID  string
		resume bool
	}{
		{name: "fresh", runID: "fresh-run", resume: false},
		{name: "resume", runID: "resume-run", resume: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			spool, err := stage4SpoolDirectory(statePath, test.runID)
			if err != nil {
				t.Fatal(err)
			}
			guard := state.NewLeaseGuard(
				state.SQLiteStore{Path: filepath.Join(directory, "leases.db")},
				state.Lease{
					Target:     "sqlite:target",
					RunID:      test.runID,
					OwnerToken: "owner-" + test.runID,
					Generation: 1,
				},
			)
			fenced, err := newStage4FencedStateBackend(raw, guard)
			if err != nil {
				t.Fatal(err)
			}
			observer := tableCheckpointObserver{
				store:          fenced,
				runID:          test.runID,
				guard:          guard,
				resume:         test.resume,
				spoolDirectory: spool,
			}
			run, found, err := migrate.ResolveStage4RunContext(observer)
			if err != nil || !found {
				t.Fatalf("context found=%v err=%v", found, err)
			}
			if run.RunID != test.runID || run.Resume != test.resume ||
				run.SpoolDirectory != spool || run.Backend == nil {
				t.Fatalf("context = %#v", run)
			}
			info, err := os.Lstat(spool)
			if err != nil {
				t.Fatal(err)
			}
			if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
				t.Fatalf("spool mode = %v", info.Mode())
			}
			repeated, found, err := migrate.ResolveStage4RunContext(observer)
			if err != nil || !found || repeated.SpoolDirectory != spool {
				t.Fatalf(
					"repeated context = %#v found=%v err=%v",
					repeated,
					found,
					err,
				)
			}
		})
	}
	if _, err := os.Stat(statePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("resolving Stage 4 context mutated state: %v", err)
	}
}

func TestTableCheckpointObserverStage4ContextFailsClosedOnCapabilities(
	t *testing.T,
) {
	directory := t.TempDir()
	statePath := filepath.Join(directory, "migration.state.db")
	raw := state.SQLiteStore{Path: statePath}
	guard := state.NewLeaseGuard(
		state.SQLiteStore{Path: filepath.Join(directory, "leases.db")},
		state.Lease{
			Target:     "sqlite:target",
			RunID:      "capability-run",
			OwnerToken: "owner",
			Generation: 1,
		},
	)
	spool, err := stage4SpoolDirectory(statePath, "capability-run")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("missing lease guard", func(t *testing.T) {
		observer := tableCheckpointObserver{
			store: raw, runID: "capability-run", spoolDirectory: spool,
		}
		if _, err := observer.Stage4RunContext(); err == nil ||
			!strings.Contains(err.Error(), "lease guard") {
			t.Fatalf("missing guard error = %v", err)
		}
	})
	t.Run("backend not explicitly fenced", func(t *testing.T) {
		observer := tableCheckpointObserver{
			store: raw, runID: "capability-run", guard: guard,
			spoolDirectory: spool,
		}
		if _, err := observer.Stage4RunContext(); err == nil ||
			!strings.Contains(err.Error(), "lease-fenced state backend") {
			t.Fatalf("missing fence error = %v", err)
		}
	})
	t.Run("missing Stage 4 backend", func(t *testing.T) {
		if _, err := newStage4FencedStateBackend(
			appBackendWithoutStage4{Backend: raw},
			guard,
		); err == nil ||
			!strings.Contains(err.Error(), "lacks required Stage 4") {
			t.Fatalf("missing capability error = %v", err)
		}
	})
	t.Run("lease owns a different run", func(t *testing.T) {
		fenced, err := newStage4FencedStateBackend(raw, guard)
		if err != nil {
			t.Fatal(err)
		}
		otherSpool, err := stage4SpoolDirectory(statePath, "other-run")
		if err != nil {
			t.Fatal(err)
		}
		observer := tableCheckpointObserver{
			store: fenced, runID: "other-run", guard: guard,
			spoolDirectory: otherSpool,
		}
		if _, err := observer.Stage4RunContext(); err == nil ||
			!strings.Contains(err.Error(), "does not own run") {
			t.Fatalf("wrong-run lease error = %v", err)
		}
	})
}

func TestStage4SpoolDirectoryIsStableAcrossStateSymlinkAlias(t *testing.T) {
	directory := t.TempDir()
	statePath := filepath.Join(directory, "migration.state.db")
	aliasPath := filepath.Join(directory, "state-alias.db")
	if err := os.WriteFile(statePath, []byte{}, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(statePath, aliasPath); err != nil {
		t.Fatal(err)
	}
	direct, err := stage4SpoolDirectory(statePath, "stable-run")
	if err != nil {
		t.Fatal(err)
	}
	alias, err := stage4SpoolDirectory(aliasPath, "stable-run")
	if err != nil {
		t.Fatal(err)
	}
	if direct != alias {
		t.Fatalf("spool alias changed identity: direct=%q alias=%q", direct, alias)
	}
}

func TestStage4SpoolDirectoryRejectsDanglingStateSymlinkAndInaccessibleDirectory(
	t *testing.T,
) {
	directory := t.TempDir()
	dangling := filepath.Join(directory, "dangling.state.db")
	if err := os.Symlink(
		filepath.Join(directory, "missing.state.db"),
		dangling,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := stage4SpoolDirectory(dangling, "dangling-run"); err == nil ||
		!strings.Contains(err.Error(), "dangling state symlink") {
		t.Fatalf("dangling state symlink error = %v", err)
	}

	statePath := filepath.Join(directory, "inaccessible.state.db")
	spool, err := stage4SpoolDirectory(statePath, "inaccessible-run")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(spool, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := stage4SpoolDirectory(
		statePath,
		"inaccessible-run",
	); err == nil || !strings.Contains(err.Error(), "owner-accessible") {
		t.Fatalf("inaccessible spool error = %v", err)
	}
}

func TestStage4SpoolFailureLeavesFreshRunDurablyRecoverable(t *testing.T) {
	for _, stateName := range []string{"migration.state.db", "migration.state.yaml"} {
		t.Run(filepath.Ext(stateName), func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := filepath.Join(directory, stateName)
			createStage2LifecycleSource(t, sourcePath)
			cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
			spoolRoot := filepath.Join(
				directory,
				"."+filepath.Base(statePath)+".stage4-spool",
			)
			if err := os.WriteFile(spoolRoot, []byte("blocked"), 0o600); err != nil {
				t.Fatal(err)
			}

			var stdout, stderr bytes.Buffer
			code := Run(
				[]string{
					"run", "--config", configPath, "--state", statePath,
				},
				&stdout,
				&stderr,
			)
			if code != StateError ||
				!strings.Contains(stderr.String(), "Stage 4 spool directory") {
				t.Fatalf(
					"fresh spool failure code=%d stdout=%q stderr=%q",
					code,
					stdout.String(),
					stderr.String(),
				)
			}
			store, err := state.NewBackend(statePath)
			if err != nil {
				t.Fatal(err)
			}
			latest, found, err := store.Latest()
			if err != nil || !found {
				t.Fatalf("latest run found=%v err=%v", found, err)
			}
			if latest.Outcome != state.Failed || !latest.Resumable ||
				!strings.Contains(
					latest.Reason,
					"Stage 4 spool preparation failed",
				) {
				t.Fatalf("fresh spool failure state = %#v", latest)
			}
			assertStage2LifecycleLeaseReleased(t, cfg, latest.ID)
		})
	}
}

func TestStage4SpoolFailureLeavesReactivatedRunDurablyRecoverable(
	t *testing.T,
) {
	for _, stateName := range []string{"migration.state.db", "migration.state.yaml"} {
		t.Run(filepath.Ext(stateName), func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := filepath.Join(directory, stateName)
			const runID = "stage4-spool-resume"
			createStage2LifecycleSource(t, sourcePath)
			cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
			hash, err := config.Hash(cfg)
			if err != nil {
				t.Fatal(err)
			}
			store, err := state.NewBackend(statePath)
			if err != nil {
				t.Fatal(err)
			}
			startedAt := time.Date(2026, 7, 30, 16, 0, 0, 0, time.UTC)
			if err := store.InitializeRun(state.Run{
				ID:           runID,
				Source:       sourcePath,
				Target:       targetPath,
				SourceEngine: "sqlite",
				Outcome:      state.Failed,
				Resumable:    true,
				Reason:       "recoverable fixture",
				StartedAt:    startedAt,
				EndedAt:      startedAt.Add(time.Minute),
			}, hash); err != nil {
				t.Fatal(err)
			}
			spoolRoot := filepath.Join(
				directory,
				"."+filepath.Base(statePath)+".stage4-spool",
			)
			if err := os.WriteFile(spoolRoot, []byte("blocked"), 0o600); err != nil {
				t.Fatal(err)
			}

			var stdout, stderr bytes.Buffer
			code := Run(
				[]string{
					"resume", "--config", configPath, "--state", statePath,
				},
				&stdout,
				&stderr,
			)
			if code != StateError ||
				!strings.Contains(stderr.String(), "Stage 4 spool directory") {
				t.Fatalf(
					"resume spool failure code=%d stdout=%q stderr=%q",
					code,
					stdout.String(),
					stderr.String(),
				)
			}
			latest, found, err := store.Latest()
			if err != nil || !found {
				t.Fatalf("latest run found=%v err=%v", found, err)
			}
			if latest.ID != runID ||
				latest.Outcome != state.Failed ||
				!latest.Resumable ||
				!strings.Contains(
					latest.Reason,
					"Stage 4 spool preparation failed",
				) {
				t.Fatalf("resume spool failure state = %#v", latest)
			}
			assertStage2LifecycleLeaseReleased(t, cfg, runID)
		})
	}
}

type appBackendWithoutStage4 struct {
	state.Backend
}
