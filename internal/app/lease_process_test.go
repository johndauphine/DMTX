package app

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/state"
)

const (
	leaseProcessConfigEnv = "DMTX_TEST_LEASE_PROCESS_CONFIG"
	leaseProcessStateEnv  = "DMTX_TEST_LEASE_PROCESS_STATE"
)

// TestLeaseProcessHelper is the child entry point. It runs one real migration
// through the ordinary command surface so the lease is acquired exactly as
// production acquires it, and reports the command's exit code.
func TestLeaseProcessHelper(t *testing.T) {
	configPath := os.Getenv(leaseProcessConfigEnv)
	statePath := os.Getenv(leaseProcessStateEnv)
	if configPath == "" || statePath == "" {
		t.Skip("child-only helper")
	}
	var stdout, stderr bytes.Buffer
	code := Run(
		[]string{"run", "--config", configPath, "--state", statePath},
		&stdout,
		&stderr,
	)
	// The parent reads these from combined output; the exit code travels
	// through the sentinel line rather than the test binary's own status.
	os.Stdout.WriteString(
		"DMTX_CHILD_EXIT=" + strconv.Itoa(code) + "\n" + stderr.String(),
	)
	if code != Success && code != StateError {
		t.Fatalf("unexpected child exit %d: %s", code, stderr.String())
	}
}

func leaseProcessCommand(configPath, statePath string) *exec.Cmd {
	command := exec.Command(
		os.Args[0],
		"-test.run=^TestLeaseProcessHelper$",
	)
	command.Env = append(os.Environ(),
		leaseProcessConfigEnv+"="+configPath,
		leaseProcessStateEnv+"="+statePath,
	)
	return command
}

// runLeaseProcess returns an error rather than calling t.Fatalf because callers
// invoke it from goroutines, where the testing package forbids Fatalf.
func runLeaseProcess(configPath, statePath string) (int, string, error) {
	command := leaseProcessCommand(configPath, statePath)
	var combined bytes.Buffer
	command.Stdout = &combined
	command.Stderr = &combined
	_ = command.Run()
	output := combined.String()
	marker := "DMTX_CHILD_EXIT="
	index := strings.Index(output, marker)
	if index < 0 {
		return 0, output, fmt.Errorf(
			"child did not report an exit code: %s",
			output,
		)
	}
	rest := output[index+len(marker):]
	end := strings.IndexAny(rest, "\r\n")
	if end < 0 {
		end = len(rest)
	}
	code, err := strconv.Atoi(rest[:end])
	if err != nil {
		return 0, output, fmt.Errorf(
			"malformed child exit code %q",
			rest[:end],
		)
	}
	return code, output, nil
}

func mustRunLeaseProcess(
	t *testing.T,
	configPath, statePath string,
) (int, string) {
	t.Helper()
	code, output, err := runLeaseProcess(configPath, statePath)
	if err != nil {
		t.Fatal(err)
	}
	return code, output
}

// TestTargetLeaseTwoProcessRace proves the exclusive-ownership rule across a
// real process boundary rather than between goroutines.
//
// Contention is made deterministic rather than raced. Two simultaneously
// launched processes would usually overlap, but process startup jitter on a
// loaded machine can serialize them into two legitimate successes, which would
// make the fixture flaky in exactly the direction that hides a regression.
// Instead this test holds the lease in-process and proves a second real process
// cannot take it, then proves the target becomes available once released.
func TestTargetLeaseTwoProcessRace(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	createStage2LifecycleSource(t, sourcePath)

	configPath := filepath.Join(directory, "migration.yaml")
	cfg := writeSQLiteStateConfig(t, configPath, sourcePath, targetPath)
	statePath := filepath.Join(directory, "migration.state.db")

	leaseStore, lease, err := acquireTargetLease(cfg.Target, "holder-run")
	if err != nil {
		t.Fatal(err)
	}
	guard := state.NewLeaseGuard(leaseStore, lease)

	code, output := mustRunLeaseProcess(t, configPath, statePath)
	if code != StateError {
		t.Fatalf(
			"second process took an owned target: exit=%d output=%s",
			code,
			output,
		)
	}
	if !strings.Contains(output, "lease") &&
		!strings.Contains(output, "target") {
		t.Fatalf("second process failed for a non-ownership reason: %s", output)
	}

	if err := guard.Release(); err != nil {
		t.Fatal(err)
	}
	code, output = mustRunLeaseProcess(t, configPath, statePath)
	if code != Success {
		t.Fatalf(
			"released target stayed locked: exit=%d output=%s",
			code,
			output,
		)
	}
}

// TestDifferentCanonicalTargetsRunConcurrently is the converse rule: the lease
// must scope to a canonical target, not serialize the tool globally.
func TestDifferentCanonicalTargetsRunConcurrently(t *testing.T) {
	if testing.Short() {
		t.Skip("process concurrency test")
	}
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	createStage2LifecycleSource(t, sourcePath)

	firstConfig := filepath.Join(directory, "first.yaml")
	secondConfig := filepath.Join(directory, "second.yaml")
	writeSQLiteStateConfig(
		t,
		firstConfig,
		sourcePath,
		filepath.Join(directory, "target-one.db"),
	)
	writeSQLiteStateConfig(
		t,
		secondConfig,
		sourcePath,
		filepath.Join(directory, "target-two.db"),
	)
	firstState := filepath.Join(directory, "first.state.db")
	secondState := filepath.Join(directory, "second.state.db")

	type outcome struct {
		code   int
		output string
		err    error
	}
	results := make(chan outcome, 2)
	for _, pair := range [][2]string{
		{firstConfig, firstState},
		{secondConfig, secondState},
	} {
		go func(configPath, statePath string) {
			code, output, err := runLeaseProcess(configPath, statePath)
			results <- outcome{code: code, output: output, err: err}
		}(pair[0], pair[1])
	}
	for index := 0; index < 2; index++ {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.code != Success {
			t.Fatalf(
				"distinct canonical targets did not run concurrently: exit=%d output=%s",
				result.code,
				result.output,
			)
		}
	}
}
