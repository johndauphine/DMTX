package app

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestResumeRejectsChangedDataPlaneConfiguration(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	originalTargetPath := filepath.Join(directory, "target.db")
	changedTargetPath := filepath.Join(directory, "other-target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	configuration := "source:\n  type: sqlite\n  database: " + sourcePath + "\ntarget:\n  type: sqlite\n  database: " + changedTargetPath + "\n"
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}

	store := state.SQLiteStore{Path: configPath + ".state.db"}
	started := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	if err := store.Append(state.Run{ID: "run-1", Source: sourcePath, Target: changedTargetPath, Outcome: state.Failed, Resumable: true, Reason: "interrupted", StartedAt: started, EndedAt: started}); err != nil {
		t.Fatal(err)
	}
	originalHash, err := config.Hash(config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: originalTargetPath},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveConfigHash("run-1", originalHash); err != nil {
		t.Fatal(err)
	}

	var output, errors bytes.Buffer
	if code := Run([]string{"resume", "--config", configPath}, &output, &errors); code != ConfigurationError {
		t.Fatalf("exit code = %d, stderr = %s", code, errors.String())
	}
	if !strings.Contains(errors.String(), "does not match") {
		t.Fatalf("stderr = %q", errors.String())
	}
}
