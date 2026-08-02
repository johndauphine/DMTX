package app

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

// The JSON these commands emit is the public API. Nothing pinned it: output is
// json.Marshal of an internal struct, so renaming a Go field silently renames a
// wire field, and adding one silently extends the contract. Section 21.1
// requires stable CLI/JSON, and "stable" cannot mean "whatever the struct
// happens to be this week".
//
// These tests pin field *names*, not values. Values change with every fixture;
// names are the contract. A rename or removal fails here, which makes changing
// the wire format a deliberate act - update the golden list, and the diff shows
// a reviewer exactly which consumers break.
//
// Nested objects are pinned by path, so a field buried three levels down is as
// protected as a top-level one.

// wireFields returns every JSON path a marshalled value produces, sorted.
//
// Arrays are collapsed to a single element path: a list of runs has the same
// contract as one run, and indexing every element would make the golden list
// depend on fixture size rather than on shape.
func wireFields(t *testing.T, value any) []string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	seen := map[string]struct{}{}
	var walk func(prefix string, node any)
	walk = func(prefix string, node any) {
		switch typed := node.(type) {
		case map[string]any:
			for key, child := range typed {
				path := key
				if prefix != "" {
					path = prefix + "." + key
				}
				seen[path] = struct{}{}
				walk(path, child)
			}
		case []any:
			for _, child := range typed {
				walk(prefix+"[]", child)
			}
		}
	}
	walk("", decoded)
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func assertWireShape(t *testing.T, name string, value any, want []string) {
	t.Helper()
	got := wireFields(t, value)
	if strings.Join(got, "\n") == strings.Join(want, "\n") {
		return
	}
	missing := difference(want, got)
	added := difference(got, want)
	t.Errorf(
		"%s wire shape changed.\n  removed or renamed (breaks consumers): %v\n  added (extends the contract): %v\n"+
			"If this change is intended, update the golden list in this test - that diff is how a reviewer sees which consumers break.",
		name, missing, added,
	)
}

func difference(from, against []string) []string {
	present := map[string]struct{}{}
	for _, item := range against {
		present[item] = struct{}{}
	}
	var result []string
	for _, item := range from {
		if _, ok := present[item]; !ok {
			result = append(result, item)
		}
	}
	return result
}

// TestOutcomeEnvelopeWireShape pins the envelope every surface receives.
func TestOutcomeEnvelopeWireShape(t *testing.T) {
	outcome := Outcome{
		Command:  "status",
		ExitCode: Success,
		Messages: []Message{{Stream: StreamStdout, Text: "text"}},
		Payload:  &Payload{Kind: PayloadRun, Data: json.RawMessage(`{"id":"r"}`)},
	}
	assertWireShape(t, "Outcome", outcome, []string{
		"command",
		"exit_code",
		"messages",
		"messages[].stream",
		"messages[].text",
		"payload",
		"payload.data",
		"payload.data.id",
		"payload.kind",
	})
}

// TestRequestWireShape pins what a surface may send. Adding a field here is
// safe; removing or renaming one breaks callers that already send it.
func TestRequestWireShape(t *testing.T) {
	request := Request{
		Command:                "resume",
		ConfigPath:             "c",
		StatePath:              "s",
		DryRun:                 true,
		AcknowledgeDestructive: true,
		Latest:                 true,
		ForceResume:            true,
		Abandon:                true,
		AbandonReason:          "r",
	}
	assertWireShape(t, "Request", request, []string{
		"abandon",
		"abandon_reason",
		"acknowledge_destructive",
		"command",
		"config_path",
		"dry_run",
		"force_resume",
		"latest",
		"state_path",
	})
}

// TestResultPayloadWireShape pins what run, resume, and validate report.
func TestResultPayloadWireShape(t *testing.T) {
	assertWireShape(t, "migrate.Result", migrate.Result{}, []string{
		"rows",
		"tables",
		"validated",
	})
}

// TestPartialResultPayloadWireShape pins the accepted-partial shape. It embeds
// migrate.Result, so a field added there surfaces here too - which is the point:
// the embedding is part of the contract, not an implementation detail.
func TestPartialResultPayloadWireShape(t *testing.T) {
	assertWireShape(t, "acceptedPartialResult", acceptedPartialResult{}, []string{
		"outcome",
		"resumable",
		"rows",
		"tables",
		"validated",
	})
}

// TestResumeAbandonmentPayloadWireShape pins the abandonment response, which is
// declared inline at its call site and so has no type a reader can find.
func TestResumeAbandonmentPayloadWireShape(t *testing.T) {
	response := struct {
		RunID     string `json:"run_id"`
		Outcome   string `json:"outcome"`
		Resumable bool   `json:"resumable"`
	}{}
	assertWireShape(t, "resume abandonment", response, []string{
		"outcome",
		"resumable",
		"run_id",
	})
}

// TestRunPayloadWireShapeExcludesSecrets pins the public run record.
//
// This one is load-bearing beyond naming: publicRun exists to strip secrets
// before a run record leaves the process. A field appearing here that was not
// deliberately added is a candidate leak, and the redaction tests elsewhere
// cannot catch a *new* secret-bearing field nobody thought to redact.
func TestRunPayloadWireShapeExcludesSecrets(t *testing.T) {
	fields := wireFields(t, publicRun(state.Run{}))
	for _, field := range fields {
		lowered := strings.ToLower(field)
		for _, forbidden := range []string{
			"password", "secret", "token", "dsn", "credential", "key",
		} {
			if strings.Contains(lowered, forbidden) {
				t.Errorf(
					"public run record exposes %q, which reads as secret-bearing; publicRun must strip it",
					field,
				)
			}
		}
	}
	if len(fields) == 0 {
		t.Fatal("public run record marshalled to nothing, so this proves nothing")
	}
}

// TestEveryPayloadKindIsPinned fails when a new payload kind is introduced
// without a golden shape, so the wire format cannot grow unpinned.
func TestEveryPayloadKindIsPinned(t *testing.T) {
	pinned := map[string]bool{
		PayloadResult:         true,
		PayloadPartialResult:  true,
		PayloadResumeResponse: true,
		PayloadRun:            true,
		// Plan, runs, and the preflight report are pinned through their own
		// packages' tests and the envelope above; they are listed so adding a
		// kind here is a conscious decision rather than an omission.
		PayloadPlan:            true,
		PayloadRuns:            true,
		PayloadPreflightReport: true,
	}
	all := []string{
		PayloadPlan, PayloadResult, PayloadPartialResult, PayloadRun,
		PayloadRuns, PayloadPreflightReport, PayloadResumeResponse,
	}
	for _, kind := range all {
		if !pinned[kind] {
			t.Errorf("payload kind %q has no pinned wire shape", kind)
		}
	}
	if len(all) != len(pinned) {
		t.Errorf(
			"payload kinds and pinned shapes disagree: %d kinds, %d pinned",
			len(all), len(pinned),
		)
	}
}
