package app

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage2ResumeSignalHelperEnv = "DMTX_STAGE2_RESUME_SIGNAL_HELPER"
	stage2ResumeSignalConfigEnv = "DMTX_STAGE2_RESUME_SIGNAL_CONFIG"
	stage2ResumeSignalStateEnv  = "DMTX_STAGE2_RESUME_SIGNAL_STATE"
	stage2ResumeSignalReadyEnv  = "DMTX_STAGE2_RESUME_SIGNAL_READY"
)

func TestStage2ResumeReactivatesRunBeforeAuditAndMigration(t *testing.T) {
	for _, backend := range stage2ResumeLifecycleBackends() {
		t.Run(backend.name, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := filepath.Join(directory, backend.stateName)
			const runID = "stage2-reactivation"

			createStage2LifecycleSource(t, sourcePath)
			cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
			store := openStage2ResumeLifecycleBackend(t, statePath)
			configHash, err := config.Hash(cfg)
			if err != nil {
				t.Fatal(err)
			}
			startedAt := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
			if err := store.InitializeRun(state.Run{
				ID:        runID,
				Source:    sourcePath,
				Target:    targetPath,
				Outcome:   state.Running,
				Resumable: true,
				Reason:    "initial migration",
				StartedAt: startedAt,
			}, configHash); err != nil {
				t.Fatal(err)
			}
			if err := store.UpdateRecoverableOutcome(
				runID,
				state.Failed,
				"recoverable transfer failure",
				startedAt.Add(time.Minute),
			); err != nil {
				t.Fatal(err)
			}

			priorBoundary := appLifecycleBoundary
			t.Cleanup(func() { appLifecycleBoundary = priorBoundary })
			sawReactivation := false
			appLifecycleBoundary = func(reached string) error {
				if reached != "resume_reactivated" {
					return nil
				}
				sawReactivation = true
				latest, found, err := store.Latest()
				if err != nil || !found {
					t.Fatalf("latest reactivated run: found=%t err=%v", found, err)
				}
				if latest.ID != runID || latest.Outcome != state.Running ||
					!latest.Resumable ||
					latest.Reason != "migration resume in progress" ||
					!latest.EndedAt.IsZero() {
					t.Fatalf("run at resume_reactivated = %#v", latest)
				}
				if _, err := os.Stat(configPath + ".audit.ndjson"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("resume audit existed before reactivation boundary: %v", err)
				}
				return nil
			}

			var stdout, stderr bytes.Buffer
			code := Run(
				[]string{"resume", "--config", configPath, "--state", statePath},
				&stdout,
				&stderr,
			)
			if code != Success {
				t.Fatalf(
					"resume exit code=%d stdout=%q stderr=%q",
					code, stdout.String(), stderr.String(),
				)
			}
			if !sawReactivation {
				t.Fatal("resume did not reach the reactivation boundary")
			}
			runs, err := store.List()
			if err != nil {
				t.Fatal(err)
			}
			var history []state.Outcome
			for _, run := range runs {
				if run.ID == runID {
					history = append(history, run.Outcome)
				}
			}
			want := []state.Outcome{state.Failed, state.Running, state.Success}
			if !equalStage2Outcomes(history, want) {
				t.Fatalf("resume lifecycle history=%v, want %v", history, want)
			}
			if got, want := readStage1AuditTypes(
				t, configPath+".audit.ndjson",
			), []string{
				"resume_started",
				"validation_completed",
				"resume_succeeded",
			}; !equalStage1Strings(got, want) {
				t.Fatalf("resume audit=%v, want %v", got, want)
			}
		})
	}
}

func TestStage2ResumeAndAbandonRejectSuccessInsertedAfterPreselection(t *testing.T) {
	operations := []struct {
		name string
		args func(configPath, statePath string) []string
	}{
		{
			name: "resume",
			args: func(configPath, statePath string) []string {
				return []string{
					"resume",
					"--config", configPath,
					"--state", statePath,
					"--force-resume",
				}
			},
		},
		{
			name: "abandon",
			args: func(configPath, statePath string) []string {
				return []string{
					"resume",
					"--config", configPath,
					"--state", statePath,
					"--abandon",
					"--abandon-reason", "superseded test",
				}
			},
		},
	}

	for _, backend := range stage2ResumeLifecycleBackends() {
		for _, operation := range operations {
			t.Run(backend.name+"/"+operation.name, func(t *testing.T) {
				directory := t.TempDir()
				sourcePath := filepath.Join(directory, "source.db")
				targetPath := filepath.Join(directory, "target.db")
				configPath := filepath.Join(directory, "migration.yaml")
				statePath := filepath.Join(directory, backend.stateName)
				const runID = "stage2-superseded"

				createStage2LifecycleSource(t, sourcePath)
				cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
				store := openStage2ResumeLifecycleBackend(t, statePath)
				oldConfig := cfg
				oldConfig.Migration.ChunkSize++
				oldHash, err := config.Hash(oldConfig)
				if err != nil {
					t.Fatal(err)
				}
				compatibilityHash, err := config.ResumeCompatibilityHash(cfg)
				if err != nil {
					t.Fatal(err)
				}
				startedAt := time.Date(2026, 7, 28, 14, 0, 0, 0, time.UTC)
				if err := store.InitializeRun(state.Run{
					ID:        runID,
					Source:    sourcePath,
					Target:    targetPath,
					Outcome:   state.Failed,
					Resumable: true,
					Reason:    "retryable failure",
					StartedAt: startedAt,
					EndedAt:   startedAt.Add(time.Minute),
				}, oldHash); err != nil {
					t.Fatal(err)
				}
				if err := store.SaveResumeCompatibilityHash(
					runID, compatibilityHash,
				); err != nil {
					t.Fatal(err)
				}

				priorBoundary := appLifecycleBoundary
				t.Cleanup(func() { appLifecycleBoundary = priorBoundary })
				insertedSuccess := false
				appLifecycleBoundary = func(reached string) error {
					if reached != "resume_candidate_selected" || insertedSuccess {
						return nil
					}
					insertedSuccess = true
					return store.Append(state.Run{
						ID:        runID,
						Source:    sourcePath,
						Target:    targetPath,
						Outcome:   state.Success,
						Resumable: false,
						Reason:    runSuccessReason,
						StartedAt: startedAt,
						EndedAt:   startedAt.Add(2 * time.Minute),
					})
				}

				var stdout, stderr bytes.Buffer
				code := Run(operation.args(configPath, statePath), &stdout, &stderr)
				if code != StateError ||
					!strings.Contains(
						stderr.String(),
						"superseded by a successful run after target lease acquisition",
					) {
					t.Fatalf(
						"superseded %s: code=%d stdout=%q stderr=%q",
						operation.name, code, stdout.String(), stderr.String(),
					)
				}
				if !insertedSuccess {
					t.Fatal("test did not insert the superseding success")
				}
				latest, found, err := store.Latest()
				if err != nil || !found || latest.Outcome != state.Success {
					t.Fatalf(
						"authoritative success: found=%t run=%#v err=%v",
						found, latest, err,
					)
				}
				storedHash, found, err := store.ConfigHash(runID)
				if err != nil || !found || storedHash != oldHash {
					t.Fatalf(
						"config evidence after rejection: found=%t hash=%q want=%q err=%v",
						found, storedHash, oldHash, err,
					)
				}
				if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("superseded %s mutated target: %v", operation.name, err)
				}
				if _, err := os.Stat(configPath + ".audit.ndjson"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("superseded %s wrote audit: %v", operation.name, err)
				}
				assertStage2LifecycleLeaseReleased(t, cfg, runID)
			})
		}
	}
}

func TestStage2ResumeRereadsConfigEvidenceAfterTargetLease(t *testing.T) {
	for _, backend := range stage2ResumeLifecycleBackends() {
		t.Run(backend.name, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			configPath := filepath.Join(directory, "migration.yaml")
			statePath := filepath.Join(directory, backend.stateName)
			const runID = "stage2-config-reread"

			createStage2LifecycleSource(t, sourcePath)
			cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
			store := openStage2ResumeLifecycleBackend(t, statePath)
			oldConfig := cfg
			oldConfig.Migration.ChunkSize++
			oldHash, err := config.Hash(oldConfig)
			if err != nil {
				t.Fatal(err)
			}
			currentCompatibility, err := config.ResumeCompatibilityHash(cfg)
			if err != nil {
				t.Fatal(err)
			}
			startedAt := time.Date(2026, 7, 28, 15, 0, 0, 0, time.UTC)
			if err := store.InitializeRun(state.Run{
				ID:        runID,
				Source:    sourcePath,
				Target:    targetPath,
				Outcome:   state.Failed,
				Resumable: true,
				Reason:    "retryable failure",
				StartedAt: startedAt,
				EndedAt:   startedAt.Add(time.Minute),
			}, oldHash); err != nil {
				t.Fatal(err)
			}
			if err := store.SaveResumeCompatibilityHash(
				runID, currentCompatibility,
			); err != nil {
				t.Fatal(err)
			}
			driftedConfig := cfg
			driftedConfig.Migration.TargetMode = "upsert"
			driftedHash, err := config.Hash(driftedConfig)
			if err != nil {
				t.Fatal(err)
			}
			driftedCompatibility, err := config.ResumeCompatibilityHash(driftedConfig)
			if err != nil {
				t.Fatal(err)
			}

			priorBoundary := appLifecycleBoundary
			t.Cleanup(func() { appLifecycleBoundary = priorBoundary })
			rewroteEvidence := false
			appLifecycleBoundary = func(reached string) error {
				if reached != "resume_candidate_selected" || rewroteEvidence {
					return nil
				}
				rewroteEvidence = true
				return store.AcknowledgeConfigOverride(
					runID, driftedHash, driftedCompatibility,
				)
			}

			var stdout, stderr bytes.Buffer
			code := Run([]string{
				"resume",
				"--config", configPath,
				"--state", statePath,
				"--force-resume",
			}, &stdout, &stderr)
			if code != ConfigurationError ||
				!strings.Contains(
					stderr.String(),
					"structurally incompatible data-plane change",
				) {
				t.Fatalf(
					"config evidence race: code=%d stdout=%q stderr=%q",
					code, stdout.String(), stderr.String(),
				)
			}
			if !rewroteEvidence {
				t.Fatal("test did not rewrite configuration evidence")
			}
			storedHash, found, err := store.ConfigHash(runID)
			if err != nil || !found || storedHash != driftedHash {
				t.Fatalf(
					"config hash after rejection: found=%t hash=%q want=%q err=%v",
					found, storedHash, driftedHash, err,
				)
			}
			storedCompatibility, found, err := store.ResumeCompatibilityHash(runID)
			if err != nil || !found || storedCompatibility != driftedCompatibility {
				t.Fatalf(
					"compatibility after rejection: found=%t hash=%q want=%q err=%v",
					found, storedCompatibility, driftedCompatibility, err,
				)
			}
			latest, found, err := store.Latest()
			if err != nil || !found || latest.Outcome != state.Failed {
				t.Fatalf(
					"run changed before config rejection: found=%t run=%#v err=%v",
					found, latest, err,
				)
			}
			if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("config evidence rejection mutated target: %v", err)
			}
			if _, err := os.Stat(configPath + ".audit.ndjson"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("config evidence rejection wrote audit: %v", err)
			}
			assertStage2LifecycleLeaseReleased(t, cfg, runID)
		})
	}
}

func TestStage2ResumeSIGTERMAfterReactivationPersistsCancelled(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM subprocess behavior is Unix-specific")
	}

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.db")
	readyPath := filepath.Join(directory, "resume-signal-ready")
	const runID = "stage2-resume-signal"

	createStage2LifecycleSource(t, sourcePath)
	cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
	store := state.SQLiteStore{Path: statePath}
	configHash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Date(2026, 7, 28, 16, 0, 0, 0, time.UTC)
	if err := store.InitializeRun(state.Run{
		ID:        runID,
		Source:    sourcePath,
		Target:    targetPath,
		Outcome:   state.Failed,
		Resumable: true,
		Reason:    "retryable failure",
		StartedAt: startedAt,
		EndedAt:   startedAt.Add(time.Minute),
	}, configHash); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestStage2ResumeSIGTERMHelperProcess$",
	)
	command.Env = append(os.Environ(),
		stage2ResumeSignalHelperEnv+"=1",
		stage2ResumeSignalConfigEnv+"="+configPath,
		stage2ResumeSignalStateEnv+"="+statePath,
		stage2ResumeSignalReadyEnv+"="+readyPath,
	)
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	reaped := false
	t.Cleanup(func() {
		if reaped {
			return
		}
		_ = command.Process.Kill()
		_ = command.Wait()
	})

	waitForStage2LifecycleFile(t, readyPath, command)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	waitErr := command.Wait()
	reaped = true
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) || exitErr.ExitCode() != Cancelled {
		t.Fatalf(
			"resume signal helper exit=%v stdout=%q stderr=%q",
			waitErr, stdout.String(), stderr.String(),
		)
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "context canceled") {
		t.Fatalf(
			"resume signal output: stdout=%q stderr=%q",
			stdout.String(), stderr.String(),
		)
	}
	latest, found, err := store.Latest()
	if err != nil || !found {
		t.Fatalf("latest cancelled resume: found=%t err=%v", found, err)
	}
	if latest.ID != runID || latest.Outcome != state.Cancelled ||
		!latest.Resumable ||
		!strings.Contains(latest.Reason, "context canceled") {
		t.Fatalf("cancelled resume=%#v", latest)
	}
	if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("early resume cancellation mutated target: %v", err)
	}
	if got, want := readStage1AuditTypes(
		t, configPath+".audit.ndjson",
	), []string{"resume_cancelled"}; !equalStage1Strings(got, want) {
		t.Fatalf("early resume cancellation audit=%v, want %v", got, want)
	}
	if succeeded, err := audit.HasEvent(
		configPath+".audit.ndjson", runID, "resume_succeeded",
	); err != nil || succeeded {
		t.Fatalf("resume_succeeded after SIGTERM: found=%t err=%v", succeeded, err)
	}
	assertStage2LifecycleLeaseReleased(t, cfg, runID)
}

func TestStage2ResumeSIGTERMHelperProcess(t *testing.T) {
	if os.Getenv(stage2ResumeSignalHelperEnv) == "" {
		return
	}
	received := make(chan os.Signal, 1)
	signal.Notify(received, syscall.SIGTERM)
	defer signal.Stop(received)
	appLifecycleBoundary = func(reached string) error {
		if reached != "resume_reactivated" {
			return nil
		}
		if err := os.WriteFile(
			os.Getenv(stage2ResumeSignalReadyEnv),
			[]byte(reached),
			0o600,
		); err != nil {
			return err
		}
		select {
		case <-received:
			// Resume installed its signal-backed context at entry. Allow its
			// cancellation goroutine to run before returning to the early check.
			time.Sleep(50 * time.Millisecond)
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for SIGTERM")
		}
	}
	code := Run([]string{
		"resume",
		"--config", os.Getenv(stage2ResumeSignalConfigEnv),
		"--state", os.Getenv(stage2ResumeSignalStateEnv),
	}, os.Stdout, os.Stderr)
	os.Exit(code)
}

type stage2ResumeLifecycleBackend struct {
	name      string
	stateName string
}

func stage2ResumeLifecycleBackends() []stage2ResumeLifecycleBackend {
	return []stage2ResumeLifecycleBackend{
		{name: "sqlite", stateName: "migration.state.db"},
		{name: "yaml", stateName: "migration.state.yaml"},
	}
}

func openStage2ResumeLifecycleBackend(
	t *testing.T,
	path string,
) state.Backend {
	t.Helper()
	store, err := state.NewBackend(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func equalStage2Outcomes(left, right []state.Outcome) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
