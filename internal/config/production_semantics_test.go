package config

import (
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestParseAppliesProductionSemanticsDefaults(t *testing.T) {
	got, err := Parse([]byte("source: {}\ntarget: {}\nmigration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	migration := got.Migration
	if migration.SchemaContract != nil {
		t.Fatalf("omitted schema contract = %#v, want report-only nil policy", migration.SchemaContract)
	}
	if migration.Validation.Mode != ValidationCountOnly ||
		!migration.Validation.FailOnMismatch ||
		!migration.Validation.FailOnTimeout ||
		!migration.Validation.FailOnEstimateMismatch {
		t.Fatalf("validation defaults = %#v", migration.Validation)
	}
	if migration.Deletes.Mode != DeleteModeOff {
		t.Fatalf("delete mode = %q, want off", migration.Deletes.Mode)
	}
	if migration.HistoryRetentionDays != DefaultHistoryRetentionDays {
		t.Fatalf("history retention = %d", migration.HistoryRetentionDays)
	}
	if migration.Tuning != TuningAuto ||
		!migration.RuntimeTuning ||
		migration.RuntimeTuningInterval != DefaultRuntimeTuningInterval {
		t.Fatalf(
			"tuning defaults = mode %q runtime %t interval %s",
			migration.Tuning,
			migration.RuntimeTuning,
			migration.RuntimeTuningInterval,
		)
	}
}

func TestParseProductionSemanticsSurface(t *testing.T) {
	got, err := Parse([]byte(`
source: {}
target: {}
migration:
  target_mode: upsert
  date_updated_columns: [modified_at, updated_at]
  fail_on_schema_drift: true
  schema_contract:
    tables: freeze
    columns: discard_value
  validation:
    mode: sample
    fail_on_mismatch: false
    fail_on_timeout: false
    fail_on_estimate_mismatch: false
  deletes:
    mode: reconcile
  history_retention_days: 0
  tuning: off
  runtime_tuning: false
  runtime_tuning_interval: 9s
`))
	if err != nil {
		t.Fatal(err)
	}
	migration := got.Migration
	if got, want := strings.Join(migration.DateUpdatedColumns, ","), "modified_at,updated_at"; got != want {
		t.Fatalf("date candidates = %q, want %q", got, want)
	}
	if !migration.FailOnSchemaDrift {
		t.Fatal("fail_on_schema_drift was not parsed")
	}
	if migration.SchemaContract == nil ||
		migration.SchemaContract.Tables != SchemaContractFreeze ||
		migration.SchemaContract.Columns != SchemaContractDiscardValue ||
		migration.SchemaContract.DataType != SchemaContractEvolve {
		t.Fatalf("schema contract = %#v", migration.SchemaContract)
	}
	if migration.Validation.Mode != ValidationSample ||
		migration.Validation.FailOnMismatch ||
		migration.Validation.FailOnTimeout ||
		migration.Validation.FailOnEstimateMismatch {
		t.Fatalf("validation policy = %#v", migration.Validation)
	}
	if migration.Deletes.Mode != DeleteModeReconcile ||
		migration.Deletes.TargetBehavior != DeleteTargetHard ||
		migration.Deletes.Reconcile.Schedule != DeleteScheduleInterval ||
		migration.Deletes.Reconcile.Interval != DefaultDeleteInterval ||
		migration.Deletes.Reconcile.BatchSize != DefaultDeleteBatchSize ||
		!migration.Deletes.Reconcile.RequirePrimaryKey {
		t.Fatalf("delete policy = %#v", migration.Deletes)
	}
	if migration.HistoryRetentionDays != 0 {
		t.Fatalf("explicit zero history retention defaulted to %d", migration.HistoryRetentionDays)
	}
	if migration.Tuning != TuningOff ||
		migration.RuntimeTuning ||
		migration.RuntimeTuningInterval != 9*time.Second {
		t.Fatalf(
			"tuning = mode %q runtime %t interval %s",
			migration.Tuning,
			migration.RuntimeTuning,
			migration.RuntimeTuningInterval,
		)
	}
}

func TestParseExpandsScalarAndEmptySchemaContracts(t *testing.T) {
	scalar, err := Parse([]byte("migration:\n  schema_contract: report\n"))
	if err != nil {
		t.Fatal(err)
	}
	if scalar.Migration.SchemaContract == nil ||
		*scalar.Migration.SchemaContract != (SchemaContract{
			Tables:   SchemaContractReport,
			Columns:  SchemaContractReport,
			DataType: SchemaContractReport,
		}) {
		t.Fatalf("scalar schema contract = %#v", scalar.Migration.SchemaContract)
	}

	empty, err := Parse([]byte("migration:\n  schema_contract: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if empty.Migration.SchemaContract == nil ||
		*empty.Migration.SchemaContract != (SchemaContract{
			Tables:   SchemaContractEvolve,
			Columns:  SchemaContractEvolve,
			DataType: SchemaContractEvolve,
		}) {
		t.Fatalf("empty schema contract = %#v", empty.Migration.SchemaContract)
	}
}

func TestParseCanonicalizesDeprecatedProductionSettings(t *testing.T) {
	legacy, err := Parse([]byte(`
migration:
  schema_evolution: report
  ai_adjust: false
  ai_adjust_interval: 11s
`))
	if err != nil {
		t.Fatal(err)
	}
	legacyDiagnostics := legacy.Diagnostics()
	if len(legacyDiagnostics) != 3 ||
		legacyDiagnostics[0].Field != "migration.schema_evolution" ||
		legacyDiagnostics[0].Replacement != "migration.schema_contract" ||
		legacyDiagnostics[0].RemovalVersion != "6" ||
		!strings.Contains(legacyDiagnostics[0].Message, "version 6") {
		t.Fatalf(
			"schema evolution diagnostic = %#v",
			legacyDiagnostics,
		)
	}
	if legacy.Migration.SchemaContract == nil ||
		legacy.Migration.SchemaContract.Tables != SchemaContractReport {
		t.Fatalf("legacy schema policy was not canonicalized: %#v", legacy.Migration)
	}
	if legacy.Migration.RuntimeTuning ||
		legacy.Migration.RuntimeTuningInterval != 11*time.Second {
		t.Fatalf(
			"legacy tuning aliases were not canonicalized: runtime=%t interval=%s",
			legacy.Migration.RuntimeTuning,
			legacy.Migration.RuntimeTuningInterval,
		)
	}
	canonicalYAML, err := yaml.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(canonicalYAML), "schema_evolution") ||
		strings.Contains(string(canonicalYAML), "ai_adjust") {
		t.Fatalf("canonical YAML retained deprecated names:\n%s", canonicalYAML)
	}
	if _, err := Parse(canonicalYAML); err != nil {
		t.Fatalf("canonicalized legacy config does not round trip: %v", err)
	}

	preferred, err := Parse([]byte(`
migration:
  runtime_tuning: true
  runtime_tuning_interval: 7s
  ai_adjust_interval: 11s
  ai_adjust: false
`))
	if err != nil {
		t.Fatal(err)
	}
	if !preferred.Migration.RuntimeTuning ||
		preferred.Migration.RuntimeTuningInterval != 7*time.Second {
		t.Fatalf(
			"preferred tuning did not take precedence: runtime=%t interval=%s",
			preferred.Migration.RuntimeTuning,
			preferred.Migration.RuntimeTuningInterval,
		)
	}
	diagnostics := preferred.Diagnostics()
	if len(diagnostics) != 2 {
		t.Fatalf("deprecated diagnostics = %#v", diagnostics)
	}
	for index, want := range []struct {
		field       string
		replacement string
	}{
		{
			field:       "migration.ai_adjust",
			replacement: "migration.runtime_tuning",
		},
		{
			field:       "migration.ai_adjust_interval",
			replacement: "migration.runtime_tuning_interval",
		},
	} {
		got := diagnostics[index]
		if got.Severity != ConfigDiagnosticWarning ||
			got.Code != ConfigDiagnosticDeprecatedField ||
			got.Field != want.field ||
			got.Replacement != want.replacement ||
			got.RemovalVersion != "6" ||
			!strings.Contains(got.Message, "version 6") {
			t.Fatalf("diagnostic[%d] = %#v, want %#v", index, got, want)
		}
	}
	diagnostics[0].Message = "mutated"
	if preferred.Diagnostics()[0].Message == "mutated" {
		t.Fatal("Diagnostics exposed mutable configuration diagnostic state")
	}

	preferredHash, err := Hash(preferred)
	if err != nil {
		t.Fatal(err)
	}
	legacyEquivalent, err := Parse([]byte(`
migration:
  ai_adjust: true
  ai_adjust_interval: 7s
`))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"runtime_tuning",
		"runtime_tuning_interval",
	} {
		provenance, found := legacyEquivalent.Migration.SettingProvenance(field)
		if !found || provenance != ProvenanceRequested {
			t.Fatalf(
				"legacy %s provenance = %q, found=%t",
				field,
				provenance,
				found,
			)
		}
	}
	legacyHash, err := Hash(legacyEquivalent)
	if err != nil {
		t.Fatal(err)
	}
	if preferredHash != legacyHash {
		t.Fatalf(
			"deprecated tuning names changed canonical hash: %s != %s",
			preferredHash,
			legacyHash,
		)
	}
}

func TestCanonicalYAMLRoundTripPreservesRequestedAndDerivedIntent(t *testing.T) {
	original, err := Parse([]byte(`
migration:
  include_tables: []
  max_retries: 0
  validation:
    fail_on_timeout: false
  runtime_tuning: false
`))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := yaml.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, requested := range []string{
		"include_tables: []",
		"max_retries: 0",
		"fail_on_timeout: false",
		"runtime_tuning: false",
	} {
		if !strings.Contains(text, requested) {
			t.Fatalf("canonical YAML omitted %q:\n%s", requested, text)
		}
	}
	for _, derived := range []string{
		"workers:",
		"chunk_size:",
		"runtime_tuning_interval:",
	} {
		if strings.Contains(text, derived) {
			t.Fatalf("canonical YAML pinned derived %q:\n%s", derived, text)
		}
	}

	roundTrip, err := Parse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		"include_tables",
		"max_retries",
		"runtime_tuning",
	} {
		got, found := roundTrip.Migration.SettingProvenance(field)
		if !found || got != ProvenanceRequested {
			t.Fatalf(
				"round-trip %s provenance = %q, found=%t",
				field,
				got,
				found,
			)
		}
	}
	for _, field := range []string{
		"workers",
		"chunk_size",
		"runtime_tuning_interval",
	} {
		got, found := roundTrip.Migration.SettingProvenance(field)
		if !found || got != ProvenanceDerived {
			t.Fatalf(
				"round-trip %s provenance = %q, found=%t",
				field,
				got,
				found,
			)
		}
	}
	originalHash, err := Hash(original)
	if err != nil {
		t.Fatal(err)
	}
	roundTripHash, err := Hash(roundTrip)
	if err != nil {
		t.Fatal(err)
	}
	if originalHash != roundTripHash {
		t.Fatalf(
			"canonical YAML changed intent hash: %s != %s",
			originalHash,
			roundTripHash,
		)
	}
}

func TestParseRejectsUnknownProductionSemanticsFields(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "migration typo",
			yaml: "migration:\n  workres: 4",
			want: "migration.workres is unsupported",
		},
		{
			name: "documented but unavailable no-op",
			yaml: "migration:\n  create_indexes: true",
			want: "migration.create_indexes is unsupported",
		},
		{
			name: "validation typo",
			yaml: "migration:\n  validation:\n    mod: sample",
			want: "migration.validation.mod is unsupported",
		},
		{
			name: "delete typo",
			yaml: "migration:\n  deletes:\n    target_behaviour: hard",
			want: "migration.deletes.target_behaviour is unsupported",
		},
		{
			name: "reconcile typo",
			yaml: "migration:\n  deletes:\n    reconcile:\n      batchsize: 10",
			want: "migration.deletes.reconcile.batchsize is unsupported",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse([]byte(test.yaml + "\n"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse error = %v, want token %q", err, test.want)
			}
		})
	}

	if _, err := Parse([]byte(`
profile:
  future_compatible: true
ai:
  future_compatible: true
slack:
  future_compatible: true
migration: {}
`)); err != nil {
		t.Fatalf("optional future top-level sections were rejected: %v", err)
	}
}

func TestParseFailsClosedForRootEndpointAndDocumentShape(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "unknown root section",
			yaml: "soruce: {}",
			want: `configuration section "soruce" is unsupported`,
		},
		{
			name: "null source",
			yaml: "source: null",
			want: "source must be a mapping, not null",
		},
		{
			name: "scalar target",
			yaml: "target: postgres",
			want: "target must be a mapping",
		},
		{
			name: "unknown endpoint field",
			yaml: "source:\n  sslmode: require",
			want: "source.sslmode is unsupported",
		},
		{
			name: "null endpoint field",
			yaml: "target:\n  host: null",
			want: "target.host must not be null",
		},
		{
			name: "mapping endpoint scalar",
			yaml: "source:\n  host: {}",
			want: "source.host must be a scalar",
		},
		{
			name: "blank explicit engine",
			yaml: "source:\n  type: ''",
			want: "source.type must not be blank",
		},
		{
			name: "null optional section",
			yaml: "ai: null",
			want: "ai must be a mapping, not null",
		},
		{
			name: "scalar optional section",
			yaml: "profile: default",
			want: "profile must be a mapping",
		},
		{
			name: "multiple documents",
			yaml: "migration: {}\n---\nmigration: {}",
			want: "exactly one YAML document",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse([]byte(test.yaml + "\n"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse error = %v, want token %q", err, test.want)
			}
		})
	}
}

func TestParseRejectsExplicitBlankNullAndZeroProductionValues(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "null migration",
			yaml: "migration: null",
			want: "migration must be a mapping, not null",
		},
		{
			name: "blank target mode",
			yaml: "migration:\n  target_mode: ''",
			want: "migration.target_mode must not be blank",
		},
		{
			name: "null worker count",
			yaml: "migration:\n  workers: null",
			want: "migration.workers must not be null",
		},
		{
			name: "blank worker count",
			yaml: "migration:\n  workers: ''",
			want: "migration.workers must not be blank",
		},
		{
			name: "zero worker count",
			yaml: "migration:\n  workers: 0",
			want: "migration.workers must be positive",
		},
		{
			name: "null include list",
			yaml: "migration:\n  include_tables: null",
			want: "migration.include_tables must not be null",
		},
		{
			name: "blank include item",
			yaml: "migration:\n  include_tables: ['']",
			want: "migration.include_tables[0] must not be blank or null",
		},
		{
			name: "blank schema scalar",
			yaml: "migration:\n  schema_contract: ''",
			want: "migration.schema_contract must not be blank",
		},
		{
			name: "null schema entity",
			yaml: "migration:\n  schema_contract:\n    tables: null",
			want: "migration.schema_contract.tables must not be blank or null",
		},
		{
			name: "blank schema entity",
			yaml: "migration:\n  schema_contract:\n    columns: ''",
			want: "migration.schema_contract.columns must not be blank or null",
		},
		{
			name: "null validation",
			yaml: "migration:\n  validation: null",
			want: "migration.validation must not be null",
		},
		{
			name: "blank validation mode",
			yaml: "migration:\n  validation:\n    mode: ''",
			want: "migration.validation.mode must not be blank",
		},
		{
			name: "null validation boolean",
			yaml: "migration:\n  validation:\n    fail_on_timeout: null",
			want: "migration.validation.fail_on_timeout must not be null",
		},
		{
			name: "blank delete mode",
			yaml: "migration:\n  deletes:\n    mode: ''",
			want: "migration.deletes.mode must not be blank",
		},
		{
			name: "null reconcile settings",
			yaml: "migration:\n  deletes:\n    reconcile: null",
			want: "migration.deletes.reconcile must not be null",
		},
		{
			name: "null delete boolean",
			yaml: "migration:\n  deletes:\n    reconcile:\n      require_primary_key: null",
			want: "migration.deletes.reconcile.require_primary_key must not be null",
		},
		{
			name: "null history retention",
			yaml: "migration:\n  history_retention_days: null",
			want: "migration.history_retention_days must not be null",
		},
		{
			name: "blank tuning mode",
			yaml: "migration:\n  tuning: ''",
			want: "migration.tuning must not be blank",
		},
		{
			name: "null runtime tuning",
			yaml: "migration:\n  runtime_tuning: null",
			want: "migration.runtime_tuning must not be null",
		},
		{
			name: "blank runtime tuning",
			yaml: "migration:\n  runtime_tuning: ''",
			want: "migration.runtime_tuning must not be blank",
		},
		{
			name: "null runtime interval",
			yaml: "migration:\n  runtime_tuning_interval: null",
			want: "migration.runtime_tuning_interval must not be null",
		},
		{
			name: "blank runtime interval",
			yaml: "migration:\n  runtime_tuning_interval: ''",
			want: "migration.runtime_tuning_interval must not be blank",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse([]byte(test.yaml + "\n"))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse error = %v, want token %q", err, test.want)
			}
		})
	}
}

func TestParseRejectsInvalidProductionSemantics(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "null schema contract",
			yaml: "schema_contract: null",
			want: "schema_contract must be a mode or mapping",
		},
		{
			name: "unknown schema entity",
			yaml: "schema_contract:\n    indexes: freeze",
			want: "unknown schema contract entity",
		},
		{
			name: "unknown schema mode",
			yaml: "schema_contract: replace",
			want: "invalid mode",
		},
		{
			name: "discard table value",
			yaml: "schema_contract:\n    tables: discard_value",
			want: "tables cannot use discard_value",
		},
		{
			name: "conflicting schema names",
			yaml: "schema_contract: report\n  schema_evolution: report",
			want: "cannot be combined",
		},
		{
			name: "reserved full validation",
			yaml: "validation:\n    mode: full",
			want: "full is reserved and unsupported",
		},
		{
			name: "unknown validation",
			yaml: "validation:\n    mode: shallow",
			want: "invalid value",
		},
		{
			name: "reconcile in rebuild",
			yaml: "deletes:\n    mode: reconcile",
			want: "requires target_mode upsert",
		},
		{
			name: "settings while deletes off",
			yaml: "deletes:\n    mode: off\n    target_behavior: hard",
			want: "settings require mode reconcile",
		},
		{
			name: "soft delete",
			yaml: "target_mode: upsert\n  deletes:\n    mode: reconcile\n    target_behavior: soft",
			want: "only hard is supported",
		},
		{
			name: "unsupported schedule",
			yaml: "target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      schedule: daily",
			want: "only interval is supported",
		},
		{
			name: "zero interval",
			yaml: "target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      interval: 0s",
			want: "interval must be positive",
		},
		{
			name: "zero batch",
			yaml: "target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      batch_size: 0",
			want: "batch_size must be positive",
		},
		{
			name: "primary key disabled",
			yaml: "target_mode: upsert\n  deletes:\n    mode: reconcile\n    reconcile:\n      require_primary_key: false",
			want: "require_primary_key must be true",
		},
		{
			name: "empty date candidate",
			yaml: "date_updated_columns: [updated_at, '']",
			want: "must not be empty",
		},
		{
			name: "duplicate date candidate",
			yaml: "date_updated_columns: [updated_at, updated_at]",
			want: "contains duplicate",
		},
		{
			name: "unknown tuning",
			yaml: "tuning: random",
			want: "tuning has invalid value",
		},
		{
			name: "zero runtime interval",
			yaml: "runtime_tuning_interval: 0s",
			want: "runtime_tuning_interval must be positive",
		},
		{
			name: "negative history",
			yaml: "history_retention_days: -1",
			want: "history_retention_days must not be negative",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			data := []byte("source: {}\ntarget: {}\nmigration:\n  " + test.yaml + "\n")
			_, err := Parse(data)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse error = %v, want token %q", err, test.want)
			}
		})
	}
}
