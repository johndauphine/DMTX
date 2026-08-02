package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

// TestValidateAdapterCountReportsRowCountDisagreement covers the failure paths
// of validateAdapterCount, which had no coverage at all.
//
// Found by mutation testing on 2026-08-01: replacing both comparisons with a
// constant false — so the adapter route could never report a row-count
// disagreement — left the entire armed internal/migrate suite passing. The
// function is live production code on both the run path (adapter_runner.go) and
// the resume path (adapter_resume.go), so a target that silently lost rows
// would have been reported as validated.
//
// The mode split is the substance and is easy to get wrong in both directions.
// Rebuild requires exact equality because the target was recreated from the
// source. Upsert requires only that the target not hold fewer rows than the
// source, because retained target-only rows are a supported outcome there — a
// "fix" that made upsert demand equality would break every upsert that keeps
// target-only data, so that case is pinned here too.
func TestValidateAdapterCountReportsRowCountDisagreement(t *testing.T) {
	const table = "items"
	sourceTable := schema.Table{Name: table}

	for name, test := range map[string]struct {
		mode        string
		sourceRows  int
		targetRows  int
		wantFailure bool
		wantMessage string
	}{
		"rebuild target lost rows": {
			mode: "drop_recreate", sourceRows: 3, targetRows: 2,
			wantFailure: true, wantMessage: "source has 3 rows, target has 2",
		},
		"rebuild target gained rows": {
			mode: "drop_recreate", sourceRows: 3, targetRows: 4,
			wantFailure: true, wantMessage: "source has 3 rows, target has 4",
		},
		"rebuild counts agree": {
			mode: "drop_recreate", sourceRows: 3, targetRows: 3,
		},
		"upsert target lost rows": {
			mode: "upsert", sourceRows: 3, targetRows: 2,
			wantFailure: true, wantMessage: "target has only 2",
		},
		"upsert retains target-only rows": {
			// Deliberately not a failure: upsert merges, so a target holding
			// rows the source never had is correct behaviour.
			mode: "upsert", sourceRows: 3, targetRows: 5,
		},
		"upsert counts agree": {
			mode: "upsert", sourceRows: 3, targetRows: 3,
		},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			events := make([]string, 0)
			source := &stage4IncrementalTestSource{
				events: &events,
				table:  sourceTable,
				rows:   make([]stage4IncrementalTestRow, test.sourceRows),
			}
			target := &recordingAdapterTarget{
				events:      &events,
				rowsByTable: map[string]int{table: test.targetRows},
			}

			err := validateAdapterCount(
				context.Background(),
				source,
				target,
				sourceTable,
				sourceTable,
				test.mode,
			)
			if !test.wantFailure {
				if err != nil {
					t.Fatalf("%s counts were rejected: %v", test.mode, err)
				}
				return
			}
			if err == nil {
				t.Fatalf(
					"%s accepted source=%d target=%d without reporting a disagreement",
					test.mode,
					test.sourceRows,
					test.targetRows,
				)
			}
			// Assert the reported numbers, not merely that something failed.
			// An error naming the wrong counts would be as misleading to an
			// operator as no error at all.
			if !strings.Contains(err.Error(), test.wantMessage) {
				t.Fatalf(
					"%s disagreement = %q, want it to report %q",
					test.mode,
					err.Error(),
					test.wantMessage,
				)
			}
			if !strings.Contains(err.Error(), table) {
				t.Fatalf("disagreement does not name the table: %q", err.Error())
			}
		})
	}
}
