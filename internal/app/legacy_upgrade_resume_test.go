package app

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

func TestResumeRejectsAmbiguousLegacyProgressForEveryStateBackend(t *testing.T) {
	for _, extension := range []string{".db", ".yaml"} {
		extension := extension
		t.Run(extension, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := filepath.Join(directory, "migration.state"+extension)

			source, err := sql.Open("sqlite", sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.Exec(`
				CREATE TABLE items (
					id REAL PRIMARY KEY NOT NULL,
					payload TEXT NOT NULL
				);
				INSERT INTO items VALUES (1.5, 'one'), (2.5, 'two');
			`); err != nil {
				t.Fatal(err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}

			target, err := sql.Open("sqlite", targetPath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := target.Exec(`
				CREATE TABLE sentinel (value TEXT NOT NULL);
				INSERT INTO sentinel VALUES ('unchanged');
			`); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}

			configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
				"\ntarget:\n  type: sqlite\n  database: " + targetPath +
				"\nmigration:\n  target_mode: upsert\n  include_tables: [items]\n"
			if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Parse([]byte(configuration))
			if err != nil {
				t.Fatal(err)
			}
			configHash, err := config.Hash(cfg)
			if err != nil {
				t.Fatal(err)
			}

			backend, err := state.NewBackend(statePath)
			if err != nil {
				t.Fatal(err)
			}
			started := time.Date(2026, 7, 28, 9, 0, 0, 0, time.UTC)
			if err := backend.InitializeRun(state.Run{
				ID:        "legacy-ambiguous",
				Source:    sourcePath,
				Target:    targetPath,
				Outcome:   state.Failed,
				Resumable: true,
				Reason:    "legacy interruption",
				StartedAt: started,
				EndedAt:   started.Add(time.Minute),
			}, configHash); err != nil {
				t.Fatal(err)
			}
			if err := backend.CreateTask(state.Task{
				RunID:     "legacy-ambiguous",
				Table:     "items",
				StartedAt: started,
			}); err != nil {
				t.Fatal(err)
			}
			if err := backend.AdvanceIntegerKeysetTask(
				"legacy-ambiguous",
				"items",
				1,
				1,
			); err != nil {
				t.Fatal(err)
			}

			var output, errorsOutput bytes.Buffer
			code := Run(
				[]string{
					"resume",
					"--config", configPath,
					"--state", statePath,
				},
				&output,
				&errorsOutput,
			)
			if code != StateError {
				t.Fatalf(
					"exit code = %d, stdout = %q, stderr = %q",
					code,
					output.String(),
					errorsOutput.String(),
				)
			}
			if !strings.Contains(
				errorsOutput.String(),
				state.ErrAmbiguousLegacy.Error(),
			) {
				t.Fatalf("stderr = %q", errorsOutput.String())
			}

			target, err = sql.Open("sqlite", targetPath)
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			var sentinel string
			if err := target.QueryRow(
				`SELECT value FROM sentinel`,
			).Scan(&sentinel); err != nil {
				t.Fatal(err)
			}
			if sentinel != "unchanged" {
				t.Fatalf("sentinel = %q, want unchanged", sentinel)
			}
			var selectedTableExists int
			if err := target.QueryRow(`
				SELECT COUNT(*) FROM sqlite_master
				WHERE type = 'table' AND name = 'items'
			`).Scan(&selectedTableExists); err != nil {
				t.Fatal(err)
			}
			if selectedTableExists != 0 {
				t.Fatal("ambiguous legacy resume mutated the selected target")
			}
		})
	}
}
