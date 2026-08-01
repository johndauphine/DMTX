package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestAppendAttemptTerminalAuditWritesRedactedRuntimeTuningBeforeOutcome(
	t *testing.T,
) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		invocation string
		result     migrate.Result
	}{
		{
			name:       "fresh partial",
			invocation: "run",
			result:     migrate.Result{Rows: 2},
		},
		{
			name:       "resume failed",
			invocation: "resume",
			result:     migrate.Result{},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			configPath := filepath.Join(t.TempDir(), "migration.yaml")
			report := runtimeTuningAuditReportForTest()
			test.result.RuntimeTuning = &report
			cause := migrate.NewTransferError(
				migrate.ErrorClassTransient,
				errors.New("driver failure password=must-not-reach-audit"),
			)
			disposition := migrationAttemptDisposition(
				test.result,
				cause,
				config.Migration{},
			)
			if err := appendAttemptTerminalAudit(
				configPath,
				"runtime-tuning-"+test.invocation,
				test.invocation,
				test.result,
				disposition,
				cause,
			); err != nil {
				t.Fatalf("append terminal audit: %v", err)
			}

			events := readRuntimeTuningAuditEvents(
				t,
				configPath+".audit.ndjson",
			)
			if len(events) != 2 {
				t.Fatalf("audit event count = %d, want 2", len(events))
			}
			if events[0].Type != runtimeTuningAuditEvent ||
				events[1].Type != test.invocation+"_"+disposition.auditSuffix {
				t.Fatalf(
					"audit order = (%q, %q), want (%q, %q)",
					events[0].Type,
					events[1].Type,
					runtimeTuningAuditEvent,
					test.invocation+"_"+disposition.auditSuffix,
				)
			}
			if len(events[1].Payload) != 0 {
				t.Fatalf("terminal outcome unexpectedly carries payload: %s", events[1].Payload)
			}

			var payload runtimeTuningAuditPayload
			if err := json.Unmarshal(events[0].Payload, &payload); err != nil {
				t.Fatalf("decode runtime tuning payload: %v", err)
			}
			if payload.Invocation != test.invocation ||
				payload.Disposition != disposition.auditSuffix ||
				payload.Outcome != disposition.outcome ||
				payload.Resumable != disposition.resumable ||
				payload.ExitCode != disposition.exitCode ||
				payload.ExitClass != "transfer" ||
				payload.ErrorClass != migrate.ErrorClassTransient {
				t.Fatalf("runtime tuning audit payload = %#v", payload)
			}
			if !reflect.DeepEqual(payload.RuntimeTuning, report) {
				t.Fatalf(
					"runtime tuning payload = %#v, want %#v",
					payload.RuntimeTuning,
					report,
				)
			}

			data, err := os.ReadFile(configPath + ".audit.ndjson")
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(data, []byte("password=must-not-reach-audit")) ||
				bytes.Contains(data, []byte("RuntimeTuning")) {
				t.Fatalf("runtime tuning audit leaked an internal or secret value: %s", data)
			}
			if !bytes.Contains(events[0].Payload, []byte(`"runtime_tuning"`)) ||
				!bytes.Contains(events[0].Payload, []byte(`"exit_class":"transfer"`)) ||
				!bytes.Contains(events[0].Payload, []byte(`"error_class":"transient"`)) {
				t.Fatalf("runtime tuning payload lost public snake_case fields: %s", events[0].Payload)
			}
		})
	}
}

func TestAppendAttemptTerminalAuditPreservesAcceptedPartialResultWireFormat(
	t *testing.T,
) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "migration.yaml")
	report := runtimeTuningAuditReportForTest()
	result := migrate.Result{
		Tables:        1,
		Rows:          2,
		RuntimeTuning: &report,
	}
	cause := errors.New("writer stopped after partial progress")
	disposition := migrationAttemptDisposition(
		result,
		cause,
		config.Migration{AllowPartial: true},
	)
	if !disposition.acceptedPartial || disposition.outcome != state.Partial ||
		disposition.resumable || disposition.exitCode != Success {
		t.Fatalf("accepted partial disposition = %#v", disposition)
	}

	before, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendAttemptTerminalAudit(
		configPath,
		"accepted-partial",
		"run",
		result,
		disposition,
		cause,
	); err != nil {
		t.Fatalf("append terminal audit: %v", err)
	}
	after, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("runtime audit mutated accepted partial result:\n before %s\n  after %s", before, after)
	}

	result.Validated = false
	encoded, err := json.Marshal(acceptedPartialResult{
		Result: result, Outcome: state.Partial, Resumable: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, auditOnlyField := range []string{
		`"invocation"`, `"disposition"`, `"exit_class"`, `"exit_code"`,
	} {
		if bytes.Contains(encoded, []byte(auditOnlyField)) {
			t.Fatalf("accepted partial stdout gained audit field %s: %s", auditOnlyField, encoded)
		}
	}

	events := readRuntimeTuningAuditEvents(t, configPath+".audit.ndjson")
	if len(events) != 2 || events[0].Type != runtimeTuningAuditEvent ||
		events[1].Type != "run_partial_accepted" {
		t.Fatalf("accepted partial audit events = %#v", events)
	}
}

func TestAppendAttemptTerminalAuditFailsBeforeTerminalOutcomeWhenRuntimeAuditWriteFails(
	t *testing.T,
) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "migration.yaml")
	if err := os.Mkdir(configPath+".audit.ndjson", 0o700); err != nil {
		t.Fatal(err)
	}
	report := runtimeTuningAuditReportForTest()
	result := migrate.Result{RuntimeTuning: &report}
	cause := errors.New("ordinary transfer failure")
	disposition := migrationAttemptDisposition(result, cause, config.Migration{})
	err := appendAttemptTerminalAudit(
		configPath,
		"audit-write-failure",
		"resume",
		result,
		disposition,
		cause,
	)
	if err == nil || !strings.Contains(err.Error(), "record runtime tuning audit") {
		t.Fatalf("runtime audit write error = %v", err)
	}
	entries, readErr := os.ReadDir(configPath + ".audit.ndjson")
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("terminal outcome was written after failed runtime audit: %#v", entries)
	}
}

func TestAppendAttemptTerminalAuditLeavesOrdinaryOutcomeAuditUnchangedWithoutReport(
	t *testing.T,
) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "migration.yaml")
	result := migrate.Result{Rows: 1}
	cause := errors.New("ordinary transfer failure")
	disposition := migrationAttemptDisposition(result, cause, config.Migration{})
	if err := appendAttemptTerminalAudit(
		configPath,
		"no-runtime-report",
		"run",
		result,
		disposition,
		cause,
	); err != nil {
		t.Fatalf("append terminal audit: %v", err)
	}
	events := readRuntimeTuningAuditEvents(t, configPath+".audit.ndjson")
	if len(events) != 1 || events[0].Type != "run_partial" ||
		len(events[0].Payload) != 0 {
		t.Fatalf("ordinary terminal audit changed without report: %#v", events)
	}
}

func TestMigrationExitClass(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		code int
		want string
	}{
		{code: Success, want: "success"},
		{code: ConfigurationError, want: "configuration"},
		{code: ConnectionError, want: "connection"},
		{code: TransferError, want: "transfer"},
		{code: ValidationError, want: "validation"},
		{code: Cancelled, want: "cancelled"},
		{code: StateError, want: "state"},
		{code: FileError, want: "file"},
		{code: -1, want: "unknown"},
	} {
		if got := migrationExitClass(test.code); got != test.want {
			t.Fatalf("exit class for %d = %q, want %q", test.code, got, test.want)
		}
	}
}

func runtimeTuningAuditReportForTest() migrate.RuntimeTuningReport {
	return migrate.RuntimeTuningReport{
		Enabled: true,
		Reason:  "deferred stable table controller",
		Tables: []migrate.RuntimeTuningTableReport{{
			Schema: "public",
			Table:  "items",
			Snapshot: migrate.RuntimeTuningStatusReport{
				Interval:          "5s",
				AppliedBoundaries: 2,
				TotalDecisions:    2,
				RetainedDecisions: 1,
			},
			Adjustments: []migrate.RuntimeTuningAdjustmentReport{{
				Reasons: []string{"write_error"},
			}},
		}},
	}
}

func readRuntimeTuningAuditEvents(t *testing.T, path string) []audit.Event {
	t.Helper()
	if _, err := audit.HasEvent(path, "", ""); err != nil {
		t.Fatalf("verify audit stream: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	events := make([]audit.Event, len(lines))
	for index, line := range lines {
		if err := json.Unmarshal(line, &events[index]); err != nil {
			t.Fatalf("decode audit event %d: %v", index, err)
		}
	}
	return events
}
