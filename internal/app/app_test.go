package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	var output, errors bytes.Buffer
	if code := Run([]string{"--version"}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d", code)
	}
	if output.String() != Version+"\n" {
		t.Fatalf("version output = %q", output.String())
	}
}

func TestResumeRejectsNetworkPairBeforeStateOrLeaseAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	configuration := "source:\n  type: sqlite\n  database: source.db\ntarget:\n  type: postgres\n  host: db.example\n  database: target\n  user: dmtx\n"
	if err := os.WriteFile(path, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	if code := Run([]string{"resume", "--config", path}, &output, &errors); code != ConfigurationError {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if !strings.Contains(errors.String(), "SQLite-to-SQLite") {
		t.Fatalf("stderr = %q", errors.String())
	}
}

func TestRunRejectsUnsupportedCapabilityBeforeStateCreation(t *testing.T) {
	directory := t.TempDir()
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.db")
	configuration := "" +
		"source:\n" +
		"  type: sqlite\n" +
		"  database: source.db\n" +
		"target:\n" +
		"  type: clickhouse\n" +
		"  host: db.example\n" +
		"  database: target\n" +
		"  user: dmtx\n" +
		"migration:\n" +
		"  target_mode: upsert\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	var output, errors bytes.Buffer
	code := Run(
		[]string{
			"run",
			"--config", configPath,
			"--state", statePath,
		},
		&output,
		&errors,
	)
	if code != ConfigurationError {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if !strings.Contains(errors.String(), "does not support upsert") {
		t.Fatalf("stderr = %q", errors.String())
	}
	if _, err := os.Stat(statePath); !os.IsNotExist(err) {
		t.Fatalf("state path exists after capability rejection: %v", err)
	}
}

func TestUnknownCommandHasConfigurationExitCode(t *testing.T) {
	var output, errors bytes.Buffer
	if code := Run([]string{"unknown"}, &output, &errors); code != ConfigurationError {
		t.Fatalf("exit code = %d", code)
	}
}

func TestMigrationExitCodeClassifiesCancellation(t *testing.T) {
	if got := migrationExitCode(context.Canceled); got != Cancelled {
		t.Fatalf("cancelled exit code = %d", got)
	}
	if got := migrationExitCode(context.DeadlineExceeded); got != Cancelled {
		t.Fatalf("deadline exit code = %d", got)
	}
}
