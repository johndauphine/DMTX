package config

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestParsePreflightSkipChecksPreservesExactIntent(t *testing.T) {
	t.Parallel()

	cfg, err := Parse([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  preflight:
    skip_checks:
      - connection.authentication
      - target.disk
      - all
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []string{
		"connection.authentication",
		"target.disk",
		"all",
	}
	if len(cfg.Migration.Preflight.SkipChecks) != len(want) {
		t.Fatalf(
			"skip checks = %#v",
			cfg.Migration.Preflight.SkipChecks,
		)
	}
	for index := range want {
		if cfg.Migration.Preflight.SkipChecks[index] != want[index] {
			t.Fatalf(
				"skip checks = %#v, want %#v",
				cfg.Migration.Preflight.SkipChecks,
				want,
			)
		}
	}
	cfg.Migration.Preflight.SkipChecks[0] = "schema.usage"
	encoded, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	text := string(encoded)
	if !strings.Contains(text, "preflight:") ||
		!strings.Contains(text, "- schema.usage") ||
		!strings.Contains(text, "- target.disk") ||
		!strings.Contains(text, "- all") {
		t.Fatalf("canonical YAML = %s", text)
	}
}

func TestParsePreflightSkipChecksRejectsInvalidShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "not mapping",
			yaml: "  preflight: []\n",
			want: "migration.preflight must be a mapping",
		},
		{
			name: "unknown field",
			yaml: "  preflight:\n    skips: []\n",
			want: "migration.preflight.skips is unsupported",
		},
		{
			name: "not list",
			yaml: "  preflight:\n    skip_checks: all\n",
			want: "migration.preflight.skip_checks must be a list",
		},
		{
			name: "single segment",
			yaml: "  preflight:\n    skip_checks: [connection]\n",
			want: "must contain at least two dotted identifiers",
		},
		{
			name: "uppercase",
			yaml: "  preflight:\n    skip_checks: [Connection.authentication]\n",
			want: "must start with a lowercase ASCII letter",
		},
		{
			name: "wildcard",
			yaml: "  preflight:\n    skip_checks: [connection.*]\n",
			want: "must start with a lowercase ASCII letter",
		},
		{
			name: "duplicate",
			yaml: "  preflight:\n    skip_checks: [all, all]\n",
			want: "contains duplicate",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := Parse([]byte(
				"source:\n  type: sqlite\n  database: source.db\n" +
					"target:\n  type: sqlite\n  database: target.db\n" +
					"migration:\n" + test.yaml,
			))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestPreflightSkipChecksAffectConfigHashButNotResumeCompatibility(
	t *testing.T,
) {
	t.Parallel()

	base, err := Parse([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
`))
	if err != nil {
		t.Fatal(err)
	}
	withSkip := base
	withSkip.Migration.Preflight.SkipChecks = []string{
		"connection.authentication",
	}
	baseHash, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	skipHash, err := Hash(withSkip)
	if err != nil {
		t.Fatal(err)
	}
	if baseHash == skipHash {
		t.Fatal("operator preflight exception did not affect config hash")
	}
	baseResume, err := ResumeCompatibilityHash(base)
	if err != nil {
		t.Fatal(err)
	}
	skipResume, err := ResumeCompatibilityHash(withSkip)
	if err != nil {
		t.Fatal(err)
	}
	if baseResume != skipResume {
		t.Fatal("preflight exception changed row/schema resume compatibility")
	}
}
