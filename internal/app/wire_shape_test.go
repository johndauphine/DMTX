package app

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
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
// Arrays contribute one element path: every element shares the shape, so only
// the first is walked. Walking all of them would make these tests scale with
// fixture size while proving nothing extra.
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
			// One element is enough: every element shares the shape.
			if len(typed) > 0 {
				walk(prefix+"[]", typed[0])
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
		// An empty object: the envelope's contract is kind and data, not
		// whatever a particular payload happens to contain. Pinning a field
		// from a sample payload would make this fail when the sample changed.
		Payload: &Payload{Kind: PayloadRun, Data: json.RawMessage(`{}`)},
	}
	assertWireShape(t, "Outcome", outcome, []string{
		"command",
		"exit_code",
		"messages",
		"messages[].stream",
		"messages[].text",
		"payload",
		"payload.data",
		"payload.kind",
	})
}

// TestRequestWireShape pins what a surface may send.
//
// Additions fail this test as well as removals. That is deliberate even though
// an added field is backward-compatible for callers: the golden list is where a
// reviewer sees the request contract grow, and a growth nobody reviewed is how
// two surfaces end up accepting different things.
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
//
// RuntimeTuning is populated deliberately. It carries omitempty, so a zero
// Result omits it entirely and a golden list built from one would never pin it -
// leaving the optional half of the contract free to be renamed silently. That is
// exactly the blind spot these tests exist to close, so every optional field
// must be present in the fixture.
func TestResultPayloadWireShape(t *testing.T) {
	assertWireShape(t, "migrate.Result", resultFixture(), resultWireFields)
}

// resultFixture returns a Result with every optional field populated, including
// one element in each nested slice. An empty slice pins the key but nothing
// inside it, which would leave the deeper half of the contract unguarded.
func resultFixture() migrate.Result {
	return migrate.Result{
		RuntimeTuning: &migrate.RuntimeTuningReport{
			Reason: "populated so omitempty does not hide the field",
			Tables: []migrate.RuntimeTuningTableReport{{
				Adjustments: []migrate.RuntimeTuningAdjustmentReport{{}},
			}},
		},
	}
}

// TestPartialResultPayloadWireShape pins the accepted-partial shape. It embeds
// migrate.Result, so a field added there surfaces here too - which is the point:
// the embedding is part of the contract, not an implementation detail.
func TestPartialResultPayloadWireShape(t *testing.T) {
	assertWireShape(
		t,
		"acceptedPartialResult",
		acceptedPartialResult{Result: resultFixture()},
		append([]string{"outcome", "resumable"}, resultWireFields...),
	)
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

// TestEveryPayloadKindIsPinned fails when a payload kind is declared without a
// pinned wire shape.
//
// It reads the constants out of the source rather than comparing against a
// hand-written list. A list would have to be updated by the same person adding
// the constant, which is exactly the step that gets forgotten - and the test
// would then pass while the new shape went unpinned, proving nothing.
func TestEveryPayloadKindIsPinned(t *testing.T) {
	pinned := map[string]string{
		PayloadResult:          "TestResultPayloadWireShape",
		PayloadPartialResult:   "TestPartialResultPayloadWireShape",
		PayloadResumeResponse:  "TestResumeAbandonmentPayloadWireShape",
		PayloadRun:             "TestRunPayloadWireShapeExcludesSecrets",
		PayloadRuns:            "TestRunPayloadWireShapeExcludesSecrets (same element shape)",
		PayloadPlan:            "migrate.Plan, pinned by its own package",
		PayloadPreflightReport: "productionPreflightReport, pinned by preflight tests",
		PayloadConfig:          "TestConfigPayloadWireShapeExcludesSecrets",
	}

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "request.go", nil, 0)
	if err != nil {
		t.Fatalf("parse request.go: %v", err)
	}
	declared := map[string]string{}
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.ValueSpec)
		if !ok || len(spec.Names) == 0 || len(spec.Values) == 0 {
			return true
		}
		name := spec.Names[0].Name
		if !strings.HasPrefix(name, "Payload") {
			return true
		}
		literal, ok := spec.Values[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		declared[name] = strings.Trim(literal.Value, `"`)
		return true
	})
	if len(declared) == 0 {
		t.Fatal("found no Payload constants in request.go, so this proves nothing")
	}
	for name, value := range declared {
		if _, ok := pinned[value]; !ok {
			t.Errorf(
				"%s = %q is declared but has no pinned wire shape; add one and record it here",
				name, value,
			)
		}
	}
}

var resultWireFields = []string{
	"rows",
	"runtime_tuning",
	"runtime_tuning.enabled",
	"runtime_tuning.reason",
	"runtime_tuning.tables",
	"runtime_tuning.tables[].adjustments",
	"runtime_tuning.tables[].adjustments[].after",
	"runtime_tuning.tables[].adjustments[].after.buffer_depth",
	"runtime_tuning.tables[].adjustments[].after.buffer_depth.intent_provenance",
	"runtime_tuning.tables[].adjustments[].after.buffer_depth.intent_value",
	"runtime_tuning.tables[].adjustments[].after.buffer_depth.live_provenance",
	"runtime_tuning.tables[].adjustments[].after.buffer_depth.performance_pinned",
	"runtime_tuning.tables[].adjustments[].after.buffer_depth.value",
	"runtime_tuning.tables[].adjustments[].after.chunk_rows",
	"runtime_tuning.tables[].adjustments[].after.chunk_rows.intent_provenance",
	"runtime_tuning.tables[].adjustments[].after.chunk_rows.intent_value",
	"runtime_tuning.tables[].adjustments[].after.chunk_rows.live_provenance",
	"runtime_tuning.tables[].adjustments[].after.chunk_rows.performance_pinned",
	"runtime_tuning.tables[].adjustments[].after.chunk_rows.value",
	"runtime_tuning.tables[].adjustments[].after.writers",
	"runtime_tuning.tables[].adjustments[].after.writers.intent_provenance",
	"runtime_tuning.tables[].adjustments[].after.writers.intent_value",
	"runtime_tuning.tables[].adjustments[].after.writers.live_provenance",
	"runtime_tuning.tables[].adjustments[].after.writers.performance_pinned",
	"runtime_tuning.tables[].adjustments[].after.writers.value",
	"runtime_tuning.tables[].adjustments[].before",
	"runtime_tuning.tables[].adjustments[].before.buffer_depth",
	"runtime_tuning.tables[].adjustments[].before.buffer_depth.intent_provenance",
	"runtime_tuning.tables[].adjustments[].before.buffer_depth.intent_value",
	"runtime_tuning.tables[].adjustments[].before.buffer_depth.live_provenance",
	"runtime_tuning.tables[].adjustments[].before.buffer_depth.performance_pinned",
	"runtime_tuning.tables[].adjustments[].before.buffer_depth.value",
	"runtime_tuning.tables[].adjustments[].before.chunk_rows",
	"runtime_tuning.tables[].adjustments[].before.chunk_rows.intent_provenance",
	"runtime_tuning.tables[].adjustments[].before.chunk_rows.intent_value",
	"runtime_tuning.tables[].adjustments[].before.chunk_rows.live_provenance",
	"runtime_tuning.tables[].adjustments[].before.chunk_rows.performance_pinned",
	"runtime_tuning.tables[].adjustments[].before.chunk_rows.value",
	"runtime_tuning.tables[].adjustments[].before.writers",
	"runtime_tuning.tables[].adjustments[].before.writers.intent_provenance",
	"runtime_tuning.tables[].adjustments[].before.writers.intent_value",
	"runtime_tuning.tables[].adjustments[].before.writers.live_provenance",
	"runtime_tuning.tables[].adjustments[].before.writers.performance_pinned",
	"runtime_tuning.tables[].adjustments[].before.writers.value",
	"runtime_tuning.tables[].adjustments[].boundary",
	"runtime_tuning.tables[].adjustments[].boundary.attempt",
	"runtime_tuning.tables[].adjustments[].boundary.chunk_sequence",
	"runtime_tuning.tables[].adjustments[].boundary.ordinal",
	"runtime_tuning.tables[].adjustments[].boundary.range_index",
	"runtime_tuning.tables[].adjustments[].boundary.table_name",
	"runtime_tuning.tables[].adjustments[].boundary.table_schema",
	"runtime_tuning.tables[].adjustments[].reasons",
	"runtime_tuning.tables[].schema",
	"runtime_tuning.tables[].snapshot",
	"runtime_tuning.tables[].snapshot.applied_boundaries",
	"runtime_tuning.tables[].snapshot.effective",
	"runtime_tuning.tables[].snapshot.effective.buffer_depth",
	"runtime_tuning.tables[].snapshot.effective.buffer_depth.intent_provenance",
	"runtime_tuning.tables[].snapshot.effective.buffer_depth.intent_value",
	"runtime_tuning.tables[].snapshot.effective.buffer_depth.live_provenance",
	"runtime_tuning.tables[].snapshot.effective.buffer_depth.performance_pinned",
	"runtime_tuning.tables[].snapshot.effective.buffer_depth.value",
	"runtime_tuning.tables[].snapshot.effective.chunk_rows",
	"runtime_tuning.tables[].snapshot.effective.chunk_rows.intent_provenance",
	"runtime_tuning.tables[].snapshot.effective.chunk_rows.intent_value",
	"runtime_tuning.tables[].snapshot.effective.chunk_rows.live_provenance",
	"runtime_tuning.tables[].snapshot.effective.chunk_rows.performance_pinned",
	"runtime_tuning.tables[].snapshot.effective.chunk_rows.value",
	"runtime_tuning.tables[].snapshot.effective.writers",
	"runtime_tuning.tables[].snapshot.effective.writers.intent_provenance",
	"runtime_tuning.tables[].snapshot.effective.writers.intent_value",
	"runtime_tuning.tables[].snapshot.effective.writers.live_provenance",
	"runtime_tuning.tables[].snapshot.effective.writers.performance_pinned",
	"runtime_tuning.tables[].snapshot.effective.writers.value",
	"runtime_tuning.tables[].snapshot.has_boundary",
	"runtime_tuning.tables[].snapshot.healthy_boundaries",
	"runtime_tuning.tables[].snapshot.initialization_reasons",
	"runtime_tuning.tables[].snapshot.intent",
	"runtime_tuning.tables[].snapshot.intent.buffer_depth",
	"runtime_tuning.tables[].snapshot.intent.buffer_depth.provenance",
	"runtime_tuning.tables[].snapshot.intent.buffer_depth.value",
	"runtime_tuning.tables[].snapshot.intent.chunk_rows",
	"runtime_tuning.tables[].snapshot.intent.chunk_rows.provenance",
	"runtime_tuning.tables[].snapshot.intent.chunk_rows.value",
	"runtime_tuning.tables[].snapshot.intent.connection_limit",
	"runtime_tuning.tables[].snapshot.intent.connection_limit.provenance",
	"runtime_tuning.tables[].snapshot.intent.connection_limit.value",
	"runtime_tuning.tables[].snapshot.intent.detected_memory_limit_bytes",
	"runtime_tuning.tables[].snapshot.intent.detected_memory_limit_bytes.provenance",
	"runtime_tuning.tables[].snapshot.intent.detected_memory_limit_bytes.value",
	"runtime_tuning.tables[].snapshot.intent.memory_budget_bytes",
	"runtime_tuning.tables[].snapshot.intent.memory_budget_bytes.provenance",
	"runtime_tuning.tables[].snapshot.intent.memory_budget_bytes.value",
	"runtime_tuning.tables[].snapshot.intent.readers",
	"runtime_tuning.tables[].snapshot.intent.readers.provenance",
	"runtime_tuning.tables[].snapshot.intent.readers.value",
	"runtime_tuning.tables[].snapshot.intent.target_mode",
	"runtime_tuning.tables[].snapshot.intent.workers",
	"runtime_tuning.tables[].snapshot.intent.workers.provenance",
	"runtime_tuning.tables[].snapshot.intent.workers.value",
	"runtime_tuning.tables[].snapshot.intent.writers",
	"runtime_tuning.tables[].snapshot.intent.writers.provenance",
	"runtime_tuning.tables[].snapshot.intent.writers.value",
	"runtime_tuning.tables[].snapshot.interval",
	"runtime_tuning.tables[].snapshot.retained_decisions",
	"runtime_tuning.tables[].snapshot.total_decisions",
	"runtime_tuning.tables[].snapshot.trusted_row_width_bytes",
	"runtime_tuning.tables[].table",
	"tables",
	"validated",
}
