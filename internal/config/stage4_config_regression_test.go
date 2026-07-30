package config

import (
	"strings"
	"testing"
)

func TestParseAdaptsOnlyDerivedConcurrencyToConnectionLimit(t *testing.T) {
	derived, err := Parse([]byte(`
migration:
  connection_limit: 3
`))
	if err != nil {
		t.Fatal(err)
	}
	if derived.Migration.ReaderParallelism+
		derived.Migration.WriterParallelism > 3 ||
		derived.Migration.Workers > 3 {
		t.Fatalf("derived concurrency exceeds limit: %#v", derived.Migration)
	}
	for _, field := range []string{
		"workers",
		"reader_parallelism",
		"writer_parallelism",
	} {
		provenance, found := derived.Migration.SettingProvenance(field)
		if !found || provenance != ProvenanceDerived {
			t.Fatalf(
				"%s provenance = %q found=%t; want derived",
				field,
				provenance,
				found,
			)
		}
	}

	onePinned, err := Parse([]byte(`
migration:
  connection_limit: 3
  reader_parallelism: 2
`))
	if err != nil {
		t.Fatal(err)
	}
	if onePinned.Migration.ReaderParallelism != 2 ||
		onePinned.Migration.WriterParallelism != 1 {
		t.Fatalf(
			"single pin was not preserved safely: %#v",
			onePinned.Migration,
		)
	}

	if _, err := Parse([]byte(`
migration:
  connection_limit: 3
  reader_parallelism: 2
  writer_parallelism: 2
`)); err == nil || !strings.Contains(
		err.Error(),
		"exceeds connection_limit",
	) {
		t.Fatalf("conflicting explicit pins error = %v", err)
	}
}

func TestParseRequiresExactYAMLScalarTypes(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "endpoint string",
			yaml: "source:\n  database: 123\n",
			want: "source.database must be a string",
		},
		{
			name: "endpoint port",
			yaml: "source:\n  port: \"5432\"\n",
			want: "source.port must be a integer",
		},
		{
			name: "string list item",
			yaml: "migration:\n  include_tables: [123]\n",
			want: "migration.include_tables[0] must be a string",
		},
		{
			name: "integer",
			yaml: "migration:\n  workers: \"4\"\n",
			want: "migration.workers must be a integer",
		},
		{
			name: "boolean",
			yaml: "migration:\n  strict_consistency: \"false\"\n",
			want: "migration.strict_consistency must be a boolean",
		},
		{
			name: "duration",
			yaml: "migration:\n  runtime_tuning_interval: 5\n",
			want: "migration.runtime_tuning_interval must be a string",
		},
		{
			name: "nested boolean",
			yaml: "migration:\n  validation:\n    fail_on_timeout: \"true\"\n",
			want: "migration.validation.fail_on_timeout must be a boolean",
		},
		{
			name: "schema mode",
			yaml: "migration:\n  schema_contract:\n    columns: true\n",
			want: "migration.schema_contract.columns must not be blank or null",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Parse([]byte(test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse error = %v, want token %q", err, test.want)
			}
		})
	}
}
