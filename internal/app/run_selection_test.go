package app

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// writeRunnableSQLiteConfig writes a config that migrates one row between two
// sqlite files, and returns its path.
func writeRunnableSQLiteConfig(t *testing.T, directory string) string {
	t.Helper()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	// Cleanup rather than a Close after the Exec below: that Exec can t.Fatal,
	// and a Close placed after it would be skipped on exactly the path where
	// the handle would otherwise be held for the rest of the run. The migration
	// opens its own connection, so leaving this one open until the test ends
	// costs nothing.
	t.Cleanup(func() { _ = source.Close() })
	if _, err := source.Exec(
		`CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);` +
			` INSERT INTO notes (body) VALUES ('first')`,
	); err != nil {
		t.Fatal(err)
	}

	configuration := "source:\n  type: sqlite\n  database: " + sourcePath +
		"\ntarget:\n  type: sqlite\n  database: " + targetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	return configPath
}

// TestRunRecordsIntoTheSameStateDatabaseFromEitherSurface is the §21.1 claim
// stated where it can fail.
//
// The derivation of an unnamed state path used to live in runArguments, which
// only the command line reaches, so `dmtx run --config m.yaml` recorded into
// m.yaml.state.db and the identical request through Execute recorded into
// nothing. Both surfaces are exercised here against equivalent configs, and the
// assertion is about the file on disk rather than about the Request - a test
// that only compared parsed Requests would still pass with the bug restored,
// because the command line's Request was never the wrong one.
func TestRunRecordsIntoTheSameStateDatabaseFromEitherSurface(t *testing.T) {
	commandLineConfig := writeRunnableSQLiteConfig(t, t.TempDir())
	seamConfig := writeRunnableSQLiteConfig(t, t.TempDir())

	var output, errors bytes.Buffer
	if code := Run(
		[]string{"run", "--config", commandLineConfig},
		&output,
		&errors,
	); code != Success {
		t.Fatalf("command line exit code = %d, stderr = %s", code, errors.String())
	}

	outcome := Execute(context.Background(), Request{
		Command:    "run",
		ConfigPath: seamConfig,
	})
	if outcome.ExitCode != Success {
		t.Fatalf("seam exit code = %d, messages = %v", outcome.ExitCode, outcome.Messages)
	}

	// Read back through status rather than stat'ing the file. An empty file at
	// the right path would satisfy os.Stat while proving nothing; a success
	// record in it proves the run actually recorded there.
	for _, configPath := range []string{commandLineConfig, seamConfig} {
		status := Execute(context.Background(), Request{
			Command:   "status",
			Latest:    true,
			StatePath: configPath + ".state.db",
		})
		if status.ExitCode != Success {
			t.Fatalf("status for %s: exit %d, %v", configPath, status.ExitCode, status.Messages)
		}
		if status.Payload == nil ||
			!bytes.Contains(status.Payload.Data, []byte(`"outcome":"success"`)) {
			t.Fatalf("no successful run recorded beside %s", configPath)
		}
	}

	// And nothing was recorded anywhere else. Every artifact a run leaves is
	// named after either the config or the state database derived from it, so an
	// artifact named after neither means some path was resolved from something
	// this test did not control.
	entries, err := os.ReadDir(filepath.Dir(seamConfig))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == "source.db" || name == "target.db" {
			continue
		}
		if !strings.Contains(name, "migration.yaml") {
			t.Fatalf("seam run left %q, which is derived from neither input", name)
		}
	}
}

// TestRunWithoutAConfigAnswersUsageFromEitherSurface pins the other half of the
// same divergence.
//
// executeRun had no empty-config guard, unlike validate, config, analyze and
// preflight, so a request with no config reached os.ReadFile and produced
// "read configuration: open : no such file or directory" - an error naming an
// empty path, where the command line answered with usage.
func TestRunWithoutAConfigAnswersUsageFromEitherSurface(t *testing.T) {
	var output, errors bytes.Buffer
	code := Run([]string{"run"}, &output, &errors)
	if code != ConfigurationError {
		t.Fatalf("command line exit code = %d", code)
	}
	if !bytes.Contains(errors.Bytes(), []byte("usage: dmtx run")) {
		t.Fatalf("command line said %q", errors.String())
	}

	outcome := Execute(context.Background(), Request{Command: "run"})
	if outcome.ExitCode != ConfigurationError {
		t.Fatalf("seam exit code = %d, messages = %v", outcome.ExitCode, outcome.Messages)
	}
	if len(outcome.Messages) != 1 || outcome.Messages[0].Text != runUsage {
		t.Fatalf("seam said %v", outcome.Messages)
	}
}

// TestRunUsageIsOneString guards the consolidation rather than the strings.
//
// The usage text was duplicated between the parser and the executor for both
// run and resume. Comparing the two surfaces' bytes is what makes a later edit
// to one copy fail here rather than ship a command line that offers flags the
// executor does not describe.
func TestRunUsageIsOneString(t *testing.T) {
	var output, parserErrors bytes.Buffer
	Run([]string{"run", "--nonsense"}, &output, &parserErrors)
	executorOutcome := Execute(context.Background(), Request{Command: "run"})

	if len(executorOutcome.Messages) != 1 {
		t.Fatalf("executor messages = %v", executorOutcome.Messages)
	}
	if parserErrors.String() != executorOutcome.Messages[0].Text+"\n" {
		t.Fatalf(
			"parser said %q, executor said %q",
			parserErrors.String(),
			executorOutcome.Messages[0].Text,
		)
	}
}
