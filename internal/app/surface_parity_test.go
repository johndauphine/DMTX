package app

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/contract"
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
			seam := ""
			for _, message := range outcome.Messages {
				seam += message.Text
			}

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
		seam := ""
		for _, message := range outcome.Messages {
			seam += message.Text
		}
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

// TestServeIsNotOfferedThroughTheSeam pins the one registered command a front
// end must not be able to run: it is what starts a front end.
func TestServeIsNotOfferedThroughTheSeam(t *testing.T) {
	outcome := Execute(context.Background(), Request{Command: "serve"})
	if outcome.ExitCode == Success {
		t.Fatal("serve ran through the seam")
	}
	said := ""
	for _, message := range outcome.Messages {
		said += message.Text
	}
	if strings.Contains(said, "planned") {
		t.Errorf("serve is reported as planned, but it exists: %q", said)
	}
	if !strings.Contains(said, "not available through this interface") {
		t.Errorf("serve's refusal does not say why: %q", said)
	}
}
