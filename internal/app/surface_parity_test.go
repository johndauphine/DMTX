package app

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/contract"
	"github.com/johndauphine/dmtx/internal/secrets"
)

// TestNoSurfaceCallsARegisteredCommandUnknown is the parity criterion applied
// to the whole registry rather than a chosen few.
//
// dmtx shipped with the command line reporting nine commands as "planned in
// this stage" while Execute reported the same nine as "unknown command". An
// operator asking the console what dmtx could do got a different answer from
// the one the terminal gave, which is exactly what §21.1 forbids.
//
// It went unnoticed because the parity test that existed compared Execute
// against the API - both sides of the same function - over a hand-written list
// of four commands that happened to be the implemented ones. This iterates
// contract.Commands, so a command added to the registry is covered the day it
// is added rather than the day someone remembers to list it.
func TestNoSurfaceCallsARegisteredCommandUnknown(t *testing.T) {
	runEveryCommandSomewhereDisposable(t)
	if len(contract.Commands) < 10 {
		t.Fatalf(
			"only %d registered commands; the registry is not being read",
			len(contract.Commands),
		)
	}
	for _, registered := range contract.Commands {
		t.Run(registered.Name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			Run([]string{registered.Name}, &out, &errOut)
			commandLine := out.String() + errOut.String()

			outcome := Execute(context.Background(), Request{Command: registered.Name})
			seam := saidBy(outcome)

			for surface, said := range map[string]string{
				"the command line": commandLine,
				"Execute":          seam,
			} {
				if strings.Contains(said, "unknown command") {
					t.Errorf(
						"%s calls the registered command %q unknown: %q",
						surface, registered.Name, strings.TrimSpace(said),
					)
				}
			}
		})
	}
}

// TestUnimplementedCommandsAreCalledPlannedByBothSurfaces pins the agreement
// itself, not merely the absence of "unknown".
func TestUnimplementedCommandsAreCalledPlannedByBothSurfaces(t *testing.T) {
	runEveryCommandSomewhereDisposable(t)
	planned := 0
	for _, registered := range contract.Commands {
		if registered.TUI == contract.Omitted && registered.WebUI == contract.Omitted {
			continue
		}
		var out, errOut bytes.Buffer
		Run([]string{registered.Name}, &out, &errOut)
		commandLine := out.String() + errOut.String()
		if !strings.Contains(commandLine, "is planned in this stage") {
			continue // implemented; its answer depends on its arguments
		}
		planned++

		outcome := Execute(context.Background(), Request{Command: registered.Name})
		seam := saidBy(outcome)
		if !strings.Contains(seam, "is planned in this stage") {
			t.Errorf(
				"the command line calls %q planned but Execute says %q",
				registered.Name, strings.TrimSpace(seam),
			)
		}
		if outcome.ExitCode != Success {
			t.Errorf(
				"Execute failed a planned command %q with exit %d; the command "+
					"line exits 0",
				registered.Name, outcome.ExitCode,
			)
		}
	}
	if planned == 0 {
		t.Fatal("no command reported as planned; this test proved nothing")
	}
}

// TestAnUnregisteredCommandIsStillUnknown pins that the fix did not turn the
// registry check into a blanket acceptance: something dmtx genuinely does not
// have must still say so.
func TestAnUnregisteredCommandIsStillUnknown(t *testing.T) {
	outcome := Execute(context.Background(), Request{Command: "teleport"})
	if outcome.ExitCode == Success {
		t.Error("an invented command succeeded")
	}
	found := false
	for _, message := range outcome.Messages {
		if strings.Contains(message.Text, "unknown command") {
			found = true
		}
	}
	if !found {
		t.Errorf("an invented command was not called unknown: %+v", outcome.Messages)
	}
}

// TestAnAliasAnswersLikeTheCommandItNames pins that a registered alias is not a
// second-class spelling.
//
// "health-check" is an alias of preflight. The command line accepted it and the
// seam called it unknown, so the two surfaces disagreed about what dmtx can do -
// the thing the parity criterion forbids, and the thing the parity test above
// was written to catch. It did not, because it iterates contract.Commands by
// Name, and aliases are registry entries the name loop never visits.
//
// A test that walks a collection by one field cannot hold a property of the
// other fields, however thorough the walk looks.
func TestAnAliasAnswersLikeTheCommandItNames(t *testing.T) {
	runEveryCommandSomewhereDisposable(t)
	aliases := 0
	for _, registered := range contract.Commands {
		for _, alias := range registered.Aliases {
			aliases++
			t.Run(alias, func(t *testing.T) {
				byAlias := Execute(context.Background(), Request{Command: alias})
				byName := Execute(context.Background(), Request{Command: registered.Name})

				if strings.Contains(saidBy(byAlias), "unknown command") {
					t.Fatalf(
						"the registered alias %q of %q is refused as unknown: %q",
						alias, registered.Name, saidBy(byAlias),
					)
				}
				if byAlias.ExitCode != byName.ExitCode {
					t.Errorf(
						"%q exits %d but %q exits %d",
						alias, byAlias.ExitCode, registered.Name, byName.ExitCode,
					)
				}
				// The Outcome names the canonical command, so nothing
				// downstream has to know the alias exists - which is the rule
				// parseRequest already follows for argv.
				if byAlias.Command != registered.Name {
					t.Errorf(
						"an outcome for %q names the command %q; it should be canonicalised to %q",
						alias, byAlias.Command, registered.Name,
					)
				}
			})
		}
	}
	if aliases == 0 {
		t.Fatal("no command has an alias, so this test proved nothing")
	}
}

// TestTheRegistryIsValid pins that the runtime guard actually holds the
// invariants, rather than leaving them to tests that production never runs.
func TestTheRegistryIsValid(t *testing.T) {
	if !contract.Valid() {
		t.Fatal("the command registry is invalid")
	}
}

// TestCacheIsOmittedBecauseThereIsNothingToClear pins the decision itself.
//
// The test below iterates the Omitted commands, so it cannot hold membership of
// that set: flipping cache back to Planned drops it out of the loop and passes,
// which is exactly what happened until this test existed. A decision that stops
// being checked the moment it is reversed is not being checked.
//
// DMT's cache command clears a type-mapping cache. dmtx has none, and the only
// thing it keeps under the user cache directory is lease coordination state,
// which is durable and would be harmful to clear during a run. If dmtx gains a
// cache worth clearing, change this deliberately.
func TestCacheIsOmittedBecauseThereIsNothingToClear(t *testing.T) {
	for _, registered := range contract.Commands {
		if registered.Name != "cache" {
			continue
		}
		if registered.TUI != contract.Omitted || registered.WebUI != contract.Omitted {
			t.Fatalf(
				"cache is offered as %s/%s, but dmtx has no cache to clear. "+
					"A command that clears nothing tells an operator something "+
					"happened. If dmtx now has a cache, say so here.",
				registered.TUI, registered.WebUI,
			)
		}
		return
	}
	t.Fatal("cache is no longer registered at all, which is a different decision again")
}

// TestAnOmittedCommandIsRefusedWithItsOwnReason pins that a command dmtx will
// not run says why, in its own words.
//
// Omitted covers two different situations - serve exists but is not a front
// end's to run, cache does not apply to dmtx at all - and one sentence for both
// sends an operator looking for a flag that will never exist. The reason lives
// in the registry beside the disposition, so a command marked Omitted without
// one fails here rather than shipping a refusal that explains nothing.
func TestAnOmittedCommandIsRefusedWithItsOwnReason(t *testing.T) {
	omitted := 0
	for _, registered := range contract.Commands {
		if registered.TUI != contract.Omitted || registered.WebUI != contract.Omitted {
			continue
		}
		omitted++
		t.Run(registered.Name, func(t *testing.T) {
			if registered.Note == "" {
				t.Fatalf(
					"%s is Omitted with no Note, so its refusal cannot say why",
					registered.Name,
				)
			}
			outcome := Execute(context.Background(), Request{Command: registered.Name})
			if outcome.ExitCode == Success {
				t.Fatalf("%s ran through the seam", registered.Name)
			}
			said := saidBy(outcome)
			if strings.Contains(said, "planned") {
				t.Errorf("%s is reported as planned, but it is omitted: %q", registered.Name, said)
			}
			if !strings.Contains(said, registered.Note) {
				t.Errorf(
					"the refusal for %s does not carry its reason %q: %q",
					registered.Name, registered.Note, said,
				)
			}
		})
	}
	if omitted == 0 {
		t.Fatal("no command is Omitted, so this test proved nothing")
	}
}

// saidBy joins an Outcome's messages for substring checks.
//
// Newline-separated rather than run together: concatenation can manufacture a
// match across a message boundary - two messages ending and beginning mid-word
// - and it makes a failure unreadable, which is when the text matters most.
func saidBy(outcome Outcome) string {
	lines := make([]string, 0, len(outcome.Messages))
	for _, message := range outcome.Messages {
		lines = append(lines, message.Text)
	}
	return strings.Join(lines, "\n")
}

// runEveryCommandSomewhereDisposable isolates a test from the real machine.
//
// These tests invoke every registered command, and not every command is a
// reader. Two have caught me out, in the same way and a day apart:
//
//   - init writes a configuration relative to the working directory, so
//     enumerating commands from the package directory left a migration.yaml in
//     the source tree. That reached a commit.
//   - init-secrets writes to ~/.secrets, which the working directory does not
//     govern at all. That reached the author's actual home directory, alongside
//     their real credentials for two other tools.
//
// The second is the lesson: redirecting the working directory looked like
// isolation and was not, because the next command derived its path from
// somewhere else entirely. A test that enumerates commands has to assume the
// next one added will reach somewhere new, so both the directory and the home
// are moved rather than only the one that has bitten so far.
//
// t.Chdir and t.Setenv rather than the os equivalents: they restore themselves
// and refuse to run in a parallel test, which is the failure mode that makes
// hand-rolled process-state changes a hazard to whatever runs beside them.
func runEveryCommandSomewhereDisposable(t *testing.T) {
	t.Helper()
	elsewhere := t.TempDir()
	t.Chdir(elsewhere)
	// os.UserHomeDir reads HOME on unix and USERPROFILE on Windows.
	t.Setenv("HOME", elsewhere)
	t.Setenv("USERPROFILE", elsewhere)
}

// TestTheIsolationActuallyIsolates holds the helper above to its claim.
//
// Redirecting the working directory looked like isolation until a command
// derived its path from the home directory instead, and the file landed beside
// the author's real credentials. So the isolation is checked rather than
// assumed, and checked against a path a command actually computes rather than
// only against the environment it reads.
//
// This cannot catch a future command that derives a path from somewhere neither
// of these covers. Nothing here can. What it can do is fail the moment the
// redirection stops working, instead of the moment somebody notices a file in
// their home directory.
func TestTheIsolationActuallyIsolates(t *testing.T) {
	realHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	realWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	runEveryCommandSomewhereDisposable(t)

	isolatedHome, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	if isolatedHome == realHome {
		t.Errorf("the home directory is not redirected; it is still %s", realHome)
	}
	isolatedWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if isolatedWorkingDirectory == realWorkingDirectory {
		t.Errorf("the working directory is not redirected; it is still %s", realWorkingDirectory)
	}

	// The path a command computes, not merely the environment it reads.
	secretsPath, err := secrets.Path()
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(secretsPath, realHome+string(filepath.Separator)) {
		t.Errorf(
			"init-secrets would write to %s, inside the real home directory",
			secretsPath,
		)
	}
}
