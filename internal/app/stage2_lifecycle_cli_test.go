package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
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
	stage2SignalHelperEnv = "DMTX_STAGE2_SIGNAL_HELPER"
	stage2SignalConfigEnv = "DMTX_STAGE2_SIGNAL_CONFIG"
	stage2SignalReadyEnv  = "DMTX_STAGE2_SIGNAL_READY"
)

func TestStage2ResumeAbandonLifecycle(t *testing.T) {
	backends := []struct {
		name      string
		stateName string
	}{
		{name: "sqlite", stateName: "migration.state.db"},
		{name: "yaml", stateName: "migration.state.yaml"},
	}
	outcomes := []struct {
		name string
		from state.Outcome
		want state.Outcome
	}{
		{name: "cancelled_becomes_failed", from: state.Cancelled, want: state.Failed},
		{name: "partial_remains_partial", from: state.Partial, want: state.Partial},
	}

	for _, backend := range backends {
		for _, outcome := range outcomes {
			t.Run(backend.name+"/"+outcome.name, func(t *testing.T) {
				directory := t.TempDir()
				sourcePath := filepath.Join(directory, "source.db")
				targetPath := filepath.Join(directory, "target.db")
				configPath := filepath.Join(directory, "migration.yaml")
				statePath := filepath.Join(directory, backend.stateName)
				const (
					runID  = "stage2-abandon-run"
					reason = "operator selected a clean restart"
				)

				createStage2LifecycleSource(t, sourcePath)
				cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
				store, err := state.NewBackend(statePath)
				if err != nil {
					t.Fatal(err)
				}
				startedAt := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
				if err := store.InitializeRun(state.Run{
					ID:        runID,
					Source:    sourcePath,
					Target:    targetPath,
					Outcome:   outcome.from,
					Resumable: true,
					Reason:    "recoverable interruption",
					StartedAt: startedAt,
					EndedAt:   startedAt.Add(time.Minute),
				}, "fixture-config-hash"); err != nil {
					t.Fatal(err)
				}
				if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("target exists before abandonment: %v", err)
				}

				var stdout, stderr bytes.Buffer
				code := Run([]string{
					"resume",
					"--config", configPath,
					"--state", statePath,
					"--abandon",
					"--abandon-reason", reason,
				}, &stdout, &stderr)
				if code != Success {
					t.Fatalf("abandon exit code = %d, stderr = %q", code, stderr.String())
				}
				if stderr.Len() != 0 {
					t.Fatalf("abandon stderr = %q", stderr.String())
				}

				var response struct {
					RunID     string        `json:"run_id"`
					Outcome   state.Outcome `json:"outcome"`
					Resumable bool          `json:"resumable"`
				}
				if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
					t.Fatalf("decode abandon response %q: %v", stdout.String(), err)
				}
				if response.RunID != runID || response.Outcome != outcome.want || response.Resumable {
					t.Fatalf("abandon response = %#v", response)
				}

				latest, found, err := store.Latest()
				if err != nil || !found {
					t.Fatalf("latest abandoned run: found=%t err=%v", found, err)
				}
				if latest.ID != runID || latest.Outcome != outcome.want || latest.Resumable ||
					latest.Reason != reason || latest.EndedAt.IsZero() {
					t.Fatalf("abandoned run = %#v", latest)
				}
				if _, found, err := store.LatestResumableForTarget(targetPath); err != nil || found {
					t.Fatalf("resumable run after abandonment: found=%t err=%v", found, err)
				}

				foundAudit, err := audit.HasEvent(
					configPath+".audit.ndjson", runID, "run_abandoned",
				)
				if err != nil || !foundAudit {
					t.Fatalf("run_abandoned audit: found=%t err=%v", foundAudit, err)
				}
				assertStage2LifecycleLeaseReleased(t, cfg, runID)
				if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("abandonment mutated target: %v", err)
				}

				stdout.Reset()
				stderr.Reset()
				code = Run([]string{
					"resume", "--config", configPath, "--state", statePath,
				}, &stdout, &stderr)
				if code != StateError ||
					!strings.Contains(stderr.String(), "no resumable run exists") {
					t.Fatalf(
						"resume after abandonment: code=%d stdout=%q stderr=%q",
						code, stdout.String(), stderr.String(),
					)
				}
				if _, err := os.Stat(targetPath); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("resume rejection mutated target: %v", err)
				}
			})
		}
	}
}

func TestStage2RunSIGTERMPersistsCancelledOutcome(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM subprocess behavior is Unix-specific")
	}

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	configPath := filepath.Join(directory, "migration.yaml")
	readyPath := filepath.Join(directory, "signal-ready")
	createStage2LifecycleSource(t, sourcePath)
	cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)

	var stdout, stderr bytes.Buffer
	command := exec.Command(os.Args[0], "-test.run=^TestStage2RunSIGTERMHelperProcess$")
	command.Env = append(os.Environ(),
		stage2SignalHelperEnv+"=1",
		stage2SignalConfigEnv+"="+configPath,
		stage2SignalReadyEnv+"="+readyPath,
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
			"signal helper exit = %v, stdout=%q stderr=%q",
			waitErr, stdout.String(), stderr.String(),
		)
	}
	if stdout.Len() != 0 {
		t.Fatalf("cancelled run emitted success output %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "context canceled") {
		t.Fatalf("cancelled run stderr = %q", stderr.String())
	}

	store := state.SQLiteStore{Path: configPath + ".state.db"}
	latest, found, err := store.Latest()
	if err != nil || !found {
		t.Fatalf("latest cancelled run: found=%t err=%v", found, err)
	}
	if latest.Outcome != state.Cancelled || !latest.Resumable ||
		!strings.Contains(latest.Reason, "context canceled") {
		t.Fatalf("cancelled run = %#v", latest)
	}
	runs, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, run := range runs {
		if run.Outcome == state.Success {
			t.Fatalf("SIGTERM produced false success history: %#v", runs)
		}
	}
	cancelledAudit, err := audit.HasEvent(
		configPath+".audit.ndjson", latest.ID, "run_cancelled",
	)
	if err != nil || !cancelledAudit {
		t.Fatalf("run_cancelled audit: found=%t err=%v", cancelledAudit, err)
	}
	if succeeded, err := audit.HasEvent(
		configPath+".audit.ndjson", latest.ID, "run_succeeded",
	); err != nil || succeeded {
		t.Fatalf("run_succeeded audit after SIGTERM: found=%t err=%v", succeeded, err)
	}
	assertStage2LifecycleLeaseReleased(t, cfg, latest.ID)
}

func TestStage2RunSIGTERMHelperProcess(t *testing.T) {
	if os.Getenv(stage2SignalHelperEnv) == "" {
		return
	}
	readyPath := os.Getenv(stage2SignalReadyEnv)
	appLifecycleBoundary = func(reached string) error {
		if reached != "run_initialized" {
			return nil
		}
		received := make(chan os.Signal, 1)
		signal.Notify(received, syscall.SIGTERM)
		defer signal.Stop(received)
		if err := os.WriteFile(readyPath, []byte(reached), 0o600); err != nil {
			return err
		}
		select {
		case <-received:
			// Run installed its signal-backed context before this boundary.
			// Give that goroutine a scheduling turn before migration starts.
			time.Sleep(50 * time.Millisecond)
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("timed out waiting for SIGTERM")
		}
	}
	code := Run(
		[]string{"run", "--config", os.Getenv(stage2SignalConfigEnv)},
		os.Stdout,
		os.Stderr,
	)
	os.Exit(code)
}

func createStage2LifecycleSource(t *testing.T, path string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO users (id, payload) VALUES (1, 'one');
	`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertStage2LifecycleLeaseReleased(
	t *testing.T,
	cfg config.Config,
	runID string,
) {
	t.Helper()
	identity, leasePath, err := targetLeaseLocation(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	database, err := (state.SQLiteStore{Path: leasePath}).Open()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var ownerToken, ownerRun string
	var generation int64
	var heartbeat time.Time
	if err := database.QueryRow(`
		SELECT owner_token, run_id, generation, heartbeat_at
		FROM leases WHERE target = ?
	`, identity).Scan(&ownerToken, &ownerRun, &generation, &heartbeat); err != nil {
		t.Fatal(err)
	}
	if ownerToken != "" || ownerRun != "" || generation != 1 || heartbeat.Unix() != 0 {
		t.Fatalf(
			"released lease for %q = owner %q run %q generation %d heartbeat %s",
			runID, ownerToken, ownerRun, generation, heartbeat,
		)
	}
}

func waitForStage2LifecycleFile(t *testing.T, path string, command *exec.Cmd) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if command.ProcessState != nil {
			t.Fatalf("signal helper exited before readiness")
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for signal helper readiness")
}
