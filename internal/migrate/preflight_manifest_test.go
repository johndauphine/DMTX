package migrate

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildProductionPreflightManifestIsStableAndConditional(
	t *testing.T,
) {
	t.Parallel()

	scope := ProductionPreflightScope{
		TargetMode:         "drop_recreate",
		StrictConsistency:  true,
		SourceSizeEvidence: true,
		Additional: []PreflightCheckRequirement{
			{
				Check: "engine.postgres.snapshot_export",
				Side:  PreflightSource,
			},
			{
				Check: "engine.postgres.transactional_ddl",
				Side:  PreflightTarget,
			},
		},
	}
	first, err := BuildProductionPreflightManifest(scope)
	if err != nil {
		t.Fatalf("build manifest: %v", err)
	}
	scope.Additional[0], scope.Additional[1] =
		scope.Additional[1], scope.Additional[0]
	second, err := BuildProductionPreflightManifest(scope)
	if err != nil {
		t.Fatalf("build reordered manifest: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf(
			"manifest depends on input order:\nfirst=%#v\nsecond=%#v",
			first,
			second,
		)
	}
	for _, required := range []PreflightCheckRequirement{
		{
			Check: "connection.authentication",
			Side:  PreflightSource,
		},
		{
			Check: "connection.authentication",
			Side:  PreflightTarget,
		},
		{
			Check: "privileges.read",
			Side:  PreflightSource,
		},
		{
			Check: "privileges.write",
			Side:  PreflightTarget,
		},
		{
			Check: "consistency.strict_prerequisites",
			Side:  PreflightSource,
		},
		{
			Check: "target.disk_capacity",
			Side:  PreflightTarget,
		},
		{
			Check: "target.destructive_acknowledgement",
			Side:  PreflightTarget,
		},
		{
			Check: "engine.postgres.snapshot_export",
			Side:  PreflightSource,
		},
	} {
		if !manifestContainsRequirement(first, required) {
			t.Fatalf("manifest is missing %#v: %#v", required, first)
		}
	}
}

func TestBuildProductionPreflightManifestSelectsUpsertChecks(t *testing.T) {
	t.Parallel()

	manifest, err := BuildProductionPreflightManifest(
		ProductionPreflightScope{TargetMode: "upsert"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !manifestContainsRequirement(
		manifest,
		PreflightCheckRequirement{
			Check: "target.upsert_capability",
			Side:  PreflightTarget,
		},
	) {
		t.Fatalf("upsert capability is missing: %#v", manifest)
	}
	for _, absent := range []PreflightCheckRequirement{
		{
			Check: "target.destructive_acknowledgement",
			Side:  PreflightTarget,
		},
		{
			Check: "schema.create_access",
			Side:  PreflightTarget,
		},
		{
			Check: "consistency.strict_prerequisites",
			Side:  PreflightSource,
		},
		{
			Check: "target.disk_capacity",
			Side:  PreflightTarget,
		},
	} {
		if manifestContainsRequirement(manifest, absent) {
			t.Fatalf("inapplicable check is present: %#v", absent)
		}
	}
}

func TestPreflightManifestEvaluateFailsClosedOnMissingEvidence(
	t *testing.T,
) {
	t.Parallel()

	manifest, err := NewPreflightManifest(
		[]PreflightCheckRequirement{
			{
				Check: "connection.authentication",
				Side:  PreflightSource,
			},
			{
				Check: "connection.authentication",
				Side:  PreflightTarget,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := manifest.Evaluate(
		[]PreflightFinding{
			preflightTestFinding(
				"connection.authentication",
				PreflightSource,
				PreflightSeverityInfo,
			),
		},
		[]string{"all"},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"connection.authentication/target",
	) {
		t.Fatalf("missing evidence error = %v", err)
	}
	if !reflect.DeepEqual(decision, PreflightDecision{}) {
		t.Fatalf("missing evidence returned a decision: %#v", decision)
	}
}

func TestPreflightManifestEvaluatePreservesExtraFindingsAndVisibleSkip(
	t *testing.T,
) {
	t.Parallel()

	manifest, err := NewPreflightManifest(
		[]PreflightCheckRequirement{
			{
				Check: "connection.authentication",
				Side:  PreflightSource,
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	required := preflightTestFinding(
		"connection.authentication",
		PreflightSource,
		PreflightSeverityError,
	)
	extra := preflightTestFinding(
		"engine.postgres.snapshot_export",
		PreflightSource,
		PreflightSeverityInfo,
	)
	decision, err := manifest.Evaluate(
		[]PreflightFinding{extra, required},
		[]string{"connection.authentication"},
	)
	if err != nil {
		t.Fatalf("evaluate manifest: %v", err)
	}
	if !decision.Proceed || len(decision.Findings) != 2 {
		t.Fatalf("decision = %#v", decision)
	}
	if decision.Findings[0].Check != "connection.authentication" ||
		!decision.Findings[0].Skipped ||
		decision.Findings[0].Severity != PreflightSeverityWarning ||
		decision.Findings[1].Check !=
			"engine.postgres.snapshot_export" {
		t.Fatalf("findings = %#v", decision.Findings)
	}
}

func TestNewPreflightManifestRejectsInvalidAndDuplicateRequirements(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name         string
		requirements []PreflightCheckRequirement
		want         string
	}{
		{
			name: "invalid check",
			requirements: []PreflightCheckRequirement{
				{Check: "Connection", Side: PreflightSource},
			},
			want: "invalid preflight manifest",
		},
		{
			name: "invalid side",
			requirements: []PreflightCheckRequirement{
				{Check: "connection.authentication", Side: "both"},
			},
			want: "invalid side",
		},
		{
			name: "duplicate",
			requirements: []PreflightCheckRequirement{
				{
					Check: "connection.authentication",
					Side:  PreflightSource,
				},
				{
					Check: "connection.authentication",
					Side:  PreflightSource,
				},
			},
			want: "duplicate preflight manifest requirement",
		},
		{
			name: "bad target mode",
			want: "unsupported target mode",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			var err error
			if test.name == "bad target mode" {
				_, err = BuildProductionPreflightManifest(
					ProductionPreflightScope{TargetMode: "replace"},
				)
			} else {
				_, err = NewPreflightManifest(test.requirements)
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func manifestContainsRequirement(
	manifest PreflightManifest,
	requirement PreflightCheckRequirement,
) bool {
	for _, candidate := range manifest.Required {
		if candidate == requirement {
			return true
		}
	}
	return false
}
