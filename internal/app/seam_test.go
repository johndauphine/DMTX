package app

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// TestRequestAndOutcomeSurviveJSONRoundTrip pins the property the seam exists
// for: a surface that is not a terminal must be able to send a Request and
// receive an Outcome over the wire without a translation layer.
//
// A translation layer is how two front ends start disagreeing. If this test
// ever needs an adapter to pass, the types have drifted away from being
// transport-ready and the WebUI will pay for it.
func TestRequestAndOutcomeSurviveJSONRoundTrip(t *testing.T) {
	request := Request{
		Command:                "run",
		ConfigPath:             "migration.yaml",
		StatePath:              "migration.state.db",
		DryRun:                 true,
		AcknowledgeDestructive: true,
		Latest:                 true,
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	var decoded Request
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	if decoded != request {
		t.Fatalf("request round trip = %#v, want %#v", decoded, request)
	}

	outcome := Outcome{
		Command:  "status",
		ExitCode: Success,
		Messages: []Message{{Stream: StreamStderr, Text: "something happened"}},
		Payload:  &Payload{Kind: PayloadRun, Data: json.RawMessage(`{"id":"r1"}`)},
	}
	encoded, err = json.Marshal(outcome)
	if err != nil {
		t.Fatalf("marshal outcome: %v", err)
	}
	var decodedOutcome Outcome
	if err := json.Unmarshal(encoded, &decodedOutcome); err != nil {
		t.Fatalf("unmarshal outcome: %v", err)
	}
	if !reflect.DeepEqual(decodedOutcome, outcome) {
		t.Fatalf("outcome round trip = %#v, want %#v", decodedOutcome, outcome)
	}
}

// TestRenderTextReproducesTheOriginalByteStream pins the guarantee that made
// this refactor safe to make: rendering an Outcome as text produces exactly
// what the command wrote directly before the seam existed - messages on their
// original streams, then the payload as one JSON line on stdout.
func TestRenderTextReproducesTheOriginalByteStream(t *testing.T) {
	outcome := Outcome{
		Command:  "validate",
		ExitCode: Success,
		Messages: []Message{
			{Stream: StreamStdout, Text: "first"},
			{Stream: StreamStderr, Text: "second"},
		},
		Payload: &Payload{
			Kind: PayloadResult,
			Data: json.RawMessage(`{"passed":true}`),
		},
	}
	var stdout, stderr bytes.Buffer
	if err := RenderText(&stdout, &stderr, outcome); err != nil {
		t.Fatalf("render: %v", err)
	}
	if stdout.String() != "first\n{\"passed\":true}\n" {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if stderr.String() != "second\n" {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

// TestRenderJSONCarriesMessagesNotJustTheExitCode pins that the non-terminal
// surface receives the same words an operator would read at a terminal. An API
// that returned only an exit code would force its client to invent explanatory
// text, which is precisely the divergence the parity criterion forbids.
func TestRenderJSONCarriesMessagesNotJustTheExitCode(t *testing.T) {
	outcome := Outcome{
		Command:  "preflight",
		ExitCode: ConfigurationError,
		Messages: []Message{{Stream: StreamStderr, Text: "configuration: bad"}},
	}
	var stdout bytes.Buffer
	if err := RenderJSON(&stdout, outcome); err != nil {
		t.Fatalf("render: %v", err)
	}
	var decoded Outcome
	if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Messages) != 1 ||
		decoded.Messages[0].Text != "configuration: bad" ||
		decoded.ExitCode != ConfigurationError {
		t.Fatalf("json outcome = %#v", decoded)
	}
}

// TestExecuteRefusesCommandsNotBehindTheSeam pins that Execute says so plainly
// rather than returning a plausible-looking empty Outcome.
//
// run and resume still write as they work and are handled by the command line
// directly. A surface built against Execute must fail loudly on them, not
// silently receive an Outcome describing nothing.
func TestExecuteRefusesCommandsNotBehindTheSeam(t *testing.T) {
	for _, command := range []string{"run", "resume"} {
		outcome := Execute(t.Context(), Request{Command: command})
		if outcome.ExitCode == Success {
			t.Fatalf("%s reported success through a seam it is not behind", command)
		}
		if len(outcome.Messages) == 0 {
			t.Fatalf("%s refusal carried no explanation", command)
		}
	}
}

// TestNoSurfacePackageImportsMigrateDirectly enforces the boundary Stage 5
// depends on.
//
// internal/app is the facade: it uses 89 exported symbols from internal/migrate
// so that surfaces do not have to. A WebUI or TUI reaching past it into the
// data plane would be able to re-derive facts the engine already decided, which
// is the failure the "present, do not re-decide" rule exists to prevent. The
// check runs over the surface packages that exist plus the ones Stage 5 will
// add, so it starts guarding them the moment they appear.
func TestNoSurfacePackageImportsMigrateDirectly(t *testing.T) {
	surfaces := []string{"tui", "webui", "cli", "api", "notify", "metrics"}
	const dataPlane = "internal/migrate"

	for _, surface := range surfaces {
		directory := filepath.Join("..", surface)
		fileSet := token.NewFileSet()
		packages, err := parser.ParseDir(fileSet, directory, nil, parser.ImportsOnly)
		if err != nil {
			// The package does not exist yet. That is the expected state for
			// most of these; the test exists so the rule is already in force
			// when one is added.
			continue
		}
		for _, pkg := range packages {
			ast.Inspect(pkg, func(node ast.Node) bool {
				spec, ok := node.(*ast.ImportSpec)
				if !ok {
					return true
				}
				path := strings.Trim(spec.Path.Value, `"`)
				if strings.HasSuffix(path, dataPlane) ||
					strings.Contains(path, dataPlane+"/") {
					t.Errorf(
						"internal/%s imports %s directly; surfaces must consume internal/app so they present engine facts rather than re-deriving them",
						surface,
						path,
					)
				}
				return true
			})
		}
	}
}
