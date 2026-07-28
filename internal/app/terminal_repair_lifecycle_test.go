package app

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/state"
)

func TestStage2TerminalRepairRejectsCandidateInsertedAfterPreselection(t *testing.T) {
	for _, backend := range stage2ResumeLifecycleBackends() {
		t.Run(backend.name, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := filepath.Join(directory, backend.stateName)
			const (
				successfulRunID = "stage2-terminal-success"
				supersedingID   = "stage2-newer-candidate"
			)

			createStage2LifecycleSource(t, sourcePath)
			cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
			store := openStage2ResumeLifecycleBackend(t, statePath)
			configHash, err := config.Hash(cfg)
			if err != nil {
				t.Fatal(err)
			}
			startedAt := time.Date(2026, 7, 28, 17, 0, 0, 0, time.UTC)
			if err := store.InitializeRun(state.Run{
				ID:        successfulRunID,
				Source:    sourcePath,
				Target:    targetPath,
				Outcome:   state.Success,
				Resumable: false,
				Reason:    runSuccessReason,
				StartedAt: startedAt,
				EndedAt:   startedAt.Add(time.Minute),
			}, configHash); err != nil {
				t.Fatal(err)
			}

			priorBoundary := appLifecycleBoundary
			t.Cleanup(func() { appLifecycleBoundary = priorBoundary })
			insertedCandidate := false
			appLifecycleBoundary = func(reached string) error {
				if reached != "resume_terminal_candidate_selected" ||
					insertedCandidate {
					return nil
				}
				insertedCandidate = true
				return store.InitializeRun(state.Run{
					ID:        supersedingID,
					Source:    sourcePath,
					Target:    targetPath,
					Outcome:   state.Failed,
					Resumable: true,
					Reason:    "newer retryable attempt",
					StartedAt: startedAt.Add(2 * time.Minute),
					EndedAt:   startedAt.Add(3 * time.Minute),
				}, configHash)
			}

			var stdout, stderr bytes.Buffer
			code := Run(
				[]string{"resume", "--config", configPath, "--state", statePath},
				&stdout,
				&stderr,
			)
			if code != StateError ||
				!strings.Contains(
					stderr.String(),
					"terminal repair candidate changed after target lease acquisition",
				) {
				t.Fatalf(
					"terminal candidate race: code=%d stdout=%q stderr=%q",
					code, stdout.String(), stderr.String(),
				)
			}
			if !insertedCandidate {
				t.Fatal("test did not insert a superseding terminal candidate")
			}
			latest, found, err := store.Latest()
			if err != nil || !found || latest.ID != supersedingID ||
				latest.Outcome != state.Failed {
				t.Fatalf(
					"latest superseding candidate: found=%t run=%#v err=%v",
					found, latest, err,
				)
			}
			if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("terminal candidate rejection mutated target: %v", err)
			}
			if _, err := os.Stat(configPath + ".audit.ndjson"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("terminal candidate rejection wrote audit: %v", err)
			}
			assertStage2LifecycleLeaseReleased(t, cfg, successfulRunID)
		})
	}
}
