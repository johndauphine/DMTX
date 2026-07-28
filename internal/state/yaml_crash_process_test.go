package state

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	yamlCrashModeEnv  = "DMTX_YAML_CRASH_MODE"
	yamlCrashStateEnv = "DMTX_YAML_CRASH_STATE"
	yamlCrashEventEnv = "DMTX_YAML_CRASH_EVENT"
)

func TestYAMLReplacementIsValidAcrossMidReplacementHardKills(t *testing.T) {
	tests := []struct {
		name     string
		mode     string
		wantRuns int
	}{
		{name: "before replace keeps old state", mode: "before-replace", wantRuns: 1},
		{name: "after replace exposes new state", mode: "after-replace", wantRuns: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			statePath := filepath.Join(directory, "state.yaml")
			eventPath := filepath.Join(directory, "replacement-boundary")
			store := YAMLStore{Path: statePath}
			started := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
			if err := store.InitializeRun(Run{
				ID:        "yaml-crash",
				Source:    "source.db",
				Target:    "target.db",
				Outcome:   Running,
				Resumable: true,
				Reason:    "migration in progress",
				StartedAt: started,
			}, "hash"); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(os.Args[0], "-test.run=^TestYAMLReplacementCrashHelperProcess$")
			command.Env = append(os.Environ(),
				yamlCrashModeEnv+"="+test.mode,
				yamlCrashStateEnv+"="+statePath,
				yamlCrashEventEnv+"="+eventPath,
			)
			var output bytes.Buffer
			command.Stdout = &output
			command.Stderr = &output
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			wait := make(chan error, 1)
			go func() { wait <- command.Wait() }()
			reaped := false
			t.Cleanup(func() {
				if reaped {
					return
				}
				_ = command.Process.Kill()
				<-wait
			})
			waitForYAMLCrashBoundary(t, eventPath, wait, &reaped, &output)
			if err := command.Process.Kill(); err != nil {
				t.Fatalf("kill YAML writer: %v\n%s", err, output.String())
			}
			if err := <-wait; err == nil {
				t.Fatalf("YAML writer exited successfully instead of being killed\n%s", output.String())
			}
			reaped = true

			runs, err := store.List()
			if err != nil {
				t.Fatalf("read state after hard kill: %v", err)
			}
			if len(runs) != test.wantRuns || runs[0].Outcome != Running {
				t.Fatalf("runs after %s = %#v", test.mode, runs)
			}
			if test.wantRuns == 2 && runs[1].Outcome != Success {
				t.Fatalf("new replacement state = %#v", runs)
			}
			hash, found, err := store.ConfigHash("yaml-crash")
			if err != nil || !found || hash != "hash" {
				t.Fatalf("config hash = %q, found = %v, error = %v", hash, found, err)
			}
		})
	}
}

func TestYAMLReplacementCrashHelperProcess(t *testing.T) {
	mode := os.Getenv(yamlCrashModeEnv)
	if mode == "" {
		return
	}
	eventPath := os.Getenv(yamlCrashEventEnv)
	block := func() error {
		if err := os.WriteFile(eventPath, []byte("ready"), 0o600); err != nil {
			return err
		}
		select {}
	}
	switch mode {
	case "before-replace":
		yamlStateBeforeReplace = func(string, string) error { return block() }
	case "after-replace":
		yamlStateAfterReplace = func(string) error { return block() }
	default:
		t.Fatalf("unknown YAML crash mode %q", mode)
	}
	store := YAMLStore{Path: os.Getenv(yamlCrashStateEnv)}
	run, found, err := store.Latest()
	if err != nil || !found {
		t.Fatalf("read initial state: found = %v, error = %v", found, err)
	}
	if err := store.Append(Run{
		ID:        run.ID,
		Source:    run.Source,
		Target:    run.Target,
		Outcome:   Success,
		Resumable: false,
		Reason:    "migration completed",
		StartedAt: run.StartedAt,
		EndedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	t.Fatal("YAML replacement completed before hard kill")
}

func waitForYAMLCrashBoundary(t *testing.T, eventPath string, exited <-chan error, reaped *bool, output *bytes.Buffer) {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-exited:
			*reaped = true
			t.Fatalf("YAML writer exited before replacement boundary: %v\n%s", err, output.String())
		case <-deadline.C:
			t.Fatalf("timed out waiting for YAML replacement boundary\n%s", output.String())
		case <-ticker.C:
			if _, err := os.Stat(eventPath); err == nil {
				return
			} else if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
		}
	}
}
