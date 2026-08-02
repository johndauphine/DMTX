package migrate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

const validationCoreTestEqualityProof = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestBuildValidationPlanIsInclusiveAndRejectsFull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mode config.ValidationMode
		want []ValidationPass
	}{
		{
			name: "omitted",
			want: []ValidationPass{ValidationPassCount},
		},
		{
			name: "count",
			mode: config.ValidationCountOnly,
			want: []ValidationPass{ValidationPassCount},
		},
		{
			name: "null parity",
			mode: config.ValidationNullParity,
			want: []ValidationPass{
				ValidationPassCount,
				ValidationPassNullParity,
			},
		},
		{
			name: "sample",
			mode: config.ValidationSample,
			want: []ValidationPass{
				ValidationPassCount,
				ValidationPassNullParity,
				ValidationPassSample,
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan, err := BuildValidationPlan(test.mode)
			if err != nil {
				t.Fatalf("build plan: %v", err)
			}
			if !reflect.DeepEqual(plan.Passes, test.want) {
				t.Fatalf("passes = %v, want %v", plan.Passes, test.want)
			}
		})
	}

	if _, err := BuildValidationPlan(config.ValidationFull); err == nil ||
		!strings.Contains(err.Error(), "reserved and unsupported") {
		t.Fatalf("full mode error = %v", err)
	}
	if _, err := BuildValidationPlan("shallow"); err == nil ||
		!strings.Contains(err.Error(), "invalid validation mode") {
		t.Fatalf("unknown mode error = %v", err)
	}
}

func TestValidationCoreCountTargetPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		targetMode           string
		reconciliationStrict bool
		failOnMismatch       bool
		source               int64
		target               int64
		wantPassed           bool
		wantSeverity         ValidationSeverity
	}{
		{
			name:       "rebuild equality",
			targetMode: "drop_recreate", failOnMismatch: true,
			source: 3, target: 3, wantPassed: true,
			wantSeverity: ValidationSeverityInfo,
		},
		{
			name:       "rebuild mismatch",
			targetMode: "drop_recreate", failOnMismatch: true,
			source: 3, target: 4, wantPassed: false,
			wantSeverity: ValidationSeverityError,
		},
		{
			name:       "upsert permits target superset",
			targetMode: "upsert", failOnMismatch: true,
			source: 3, target: 4, wantPassed: true,
			wantSeverity: ValidationSeverityInfo,
		},
		{
			name:       "upsert rejects missing target row",
			targetMode: "upsert", failOnMismatch: true,
			source: 3, target: 2, wantPassed: false,
			wantSeverity: ValidationSeverityError,
		},
		{
			name:       "completed reconciliation makes upsert strict",
			targetMode: "upsert", reconciliationStrict: true,
			failOnMismatch: true,
			source:         3, target: 4, wantPassed: false,
			wantSeverity: ValidationSeverityError,
		},
		{
			name:       "exact mismatch can be log only",
			targetMode: "drop_recreate", failOnMismatch: false,
			source: 3, target: 4, wantPassed: true,
			wantSeverity: ValidationSeverityWarning,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			probe := &validationCoreProbeStub{
				exact: func(
					_ context.Context,
					side ValidationSide,
					_ schema.Table,
				) (int64, error) {
					if side == ValidationSource {
						return test.source, nil
					}
					return test.target, nil
				},
			}
			options := validationCoreTestOptions(config.ValidationCountOnly)
			options.TargetMode = test.targetMode
			options.FailOnMismatch = test.failOnMismatch
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{
					Table:                schema.Table{Name: "items"},
					ReconciliationStrict: test.reconciliationStrict,
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed != test.wantPassed {
				t.Fatalf("passed = %t, want %t: %#v", report.Passed, test.wantPassed, report.Findings)
			}
			comparison := findCoreValidationFinding(
				t,
				report,
				"items",
				"validation.count.compare",
				"",
				0,
			)
			if comparison.Severity != test.wantSeverity {
				t.Fatalf("comparison severity = %s, want %s", comparison.Severity, test.wantSeverity)
			}
		})
	}
}

func TestValidationCoreUsesPerTableReconciliationStrictness(t *testing.T) {
	t.Parallel()

	probe := &validationCoreProbeStub{
		exact: func(
			_ context.Context,
			side ValidationSide,
			_ schema.Table,
		) (int64, error) {
			if side == ValidationSource {
				return 2, nil
			}
			return 3, nil
		},
	}
	options := validationCoreTestOptions(config.ValidationCountOnly)
	options.TargetMode = "upsert"
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{
			{
				Table: schema.Table{Name: "not_due"},
			},
			{
				Table:                schema.Table{Name: "reconciled"},
				ReconciliationStrict: true,
			},
		},
		probe,
	)
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed {
		t.Fatalf("mixed reconciliation validation unexpectedly passed: %#v", report.Findings)
	}
	notDue := findCoreValidationFinding(
		t,
		report,
		"not_due",
		"validation.count.compare",
		"",
		0,
	)
	if notDue.Outcome != ValidationOutcomePass {
		t.Fatalf("not-due comparison = %#v", notDue)
	}
	reconciled := findCoreValidationFinding(
		t,
		report,
		"reconciled",
		"validation.count.compare",
		"",
		0,
	)
	if reconciled.Outcome != ValidationOutcomeMismatch ||
		reconciled.Severity != ValidationSeverityError {
		t.Fatalf("reconciled comparison = %#v", reconciled)
	}
}

func TestValidationCoreExactTimeoutAndEstimatePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		failOnTimeout          bool
		failOnEstimateMismatch bool
		estimateSource         int64
		target                 int64
		wantPassed             bool
		wantCompareSeverity    ValidationSeverity
	}{
		{
			name:          "matching estimate still fails on timeout by default",
			failOnTimeout: true, failOnEstimateMismatch: true,
			estimateSource: 8, target: 8, wantPassed: false,
			wantCompareSeverity: ValidationSeverityInfo,
		},
		{
			name:          "matching estimate with timeout log only",
			failOnTimeout: false, failOnEstimateMismatch: true,
			estimateSource: 8, target: 8, wantPassed: true,
			wantCompareSeverity: ValidationSeverityInfo,
		},
		{
			name:          "estimate mismatch fails independently",
			failOnTimeout: false, failOnEstimateMismatch: true,
			estimateSource: 7, target: 8, wantPassed: false,
			wantCompareSeverity: ValidationSeverityError,
		},
		{
			name:          "estimate mismatch can be log only",
			failOnTimeout: false, failOnEstimateMismatch: false,
			estimateSource: 7, target: 8, wantPassed: true,
			wantCompareSeverity: ValidationSeverityWarning,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			probe := &validationCoreProbeStub{
				exact: func(
					_ context.Context,
					side ValidationSide,
					_ schema.Table,
				) (int64, error) {
					if side == ValidationSource {
						return 0, context.DeadlineExceeded
					}
					return test.target, nil
				},
				estimate: func(
					_ context.Context,
					side ValidationSide,
					_ schema.Table,
				) (int64, error) {
					if side != ValidationSource {
						t.Fatalf("estimate requested for exact target")
					}
					return test.estimateSource, nil
				},
			}
			options := validationCoreTestOptions(config.ValidationCountOnly)
			options.FailOnTimeout = test.failOnTimeout
			options.FailOnEstimateMismatch = test.failOnEstimateMismatch
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{Table: schema.Table{Name: "items"}}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed != test.wantPassed {
				t.Fatalf("passed = %t, want %t: %#v", report.Passed, test.wantPassed, report.Findings)
			}
			comparison := findCoreValidationFinding(
				t,
				report,
				"items",
				"validation.count.compare",
				"",
				0,
			)
			if comparison.Severity != test.wantCompareSeverity {
				t.Fatalf("comparison severity = %s, want %s", comparison.Severity, test.wantCompareSeverity)
			}
			if comparison.SourceEvidence != ValidationEvidenceEstimate ||
				comparison.TargetEvidence != ValidationEvidenceExact {
				t.Fatalf("comparison evidence = %s/%s", comparison.SourceEvidence, comparison.TargetEvidence)
			}
			if got := probe.callCount("estimate:source:items"); got != 1 {
				t.Fatalf("source estimate calls = %d, want 1", got)
			}
			if got := probe.callCount("estimate:target:items"); got != 0 {
				t.Fatalf("target estimate calls = %d, want 0", got)
			}
		})
	}
}

func TestValidationCoreRecognizesDriverErrorAfterExactDeadline(
	t *testing.T,
) {
	t.Parallel()

	probe := &validationCoreProbeStub{
		exact: func(
			ctx context.Context,
			side ValidationSide,
			_ schema.Table,
		) (int64, error) {
			if side == ValidationSource {
				<-ctx.Done()
				return 0, errors.New("driver returned a custom cancellation error")
			}
			return 4, nil
		},
		estimate: func(
			_ context.Context,
			side ValidationSide,
			_ schema.Table,
		) (int64, error) {
			if side != ValidationSource {
				return 0, fmt.Errorf("unexpected target estimate")
			}
			return 4, nil
		},
	}
	options := validationCoreTestOptions(config.ValidationCountOnly)
	options.ExactCountTimeout = 15 * time.Millisecond
	options.TableTimeout = 250 * time.Millisecond
	options.FailOnTimeout = false
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{{Table: schema.Table{Name: "items"}}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf("log-only timeout validation failed: %#v", report.Findings)
	}
	timeout := findCoreValidationFinding(
		t,
		report,
		"items",
		"validation.count.exact",
		"",
		0,
	)
	if timeout.Side != ValidationSource ||
		timeout.Outcome != ValidationOutcomeTimeout ||
		timeout.Severity != ValidationSeverityWarning {
		t.Fatalf("exact timeout finding = %#v", timeout)
	}
	if got := probe.callCount("estimate:source:items"); got != 1 {
		t.Fatalf("source estimate calls = %d, want 1", got)
	}
}

func TestValidationCoreDoesNotEstimateAfterNonTimeoutFailure(t *testing.T) {
	t.Parallel()

	probe := &validationCoreProbeStub{
		exact: func(
			_ context.Context,
			side ValidationSide,
			_ schema.Table,
		) (int64, error) {
			if side == ValidationSource {
				return 0, errors.New("permission denied")
			}
			return 2, nil
		},
		estimate: func(
			context.Context,
			ValidationSide,
			schema.Table,
		) (int64, error) {
			return 2, nil
		},
	}
	report, err := RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationCountOnly),
		[]ValidationTableSpec{{Table: schema.Table{Name: "items"}}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if report.Passed {
		t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
	}
	if got := probe.callCount("estimate:source:items"); got != 0 {
		t.Fatalf("source estimate calls = %d, want 0", got)
	}
}

func TestValidationCoreStrictSnapshotCountIsAuthoritative(t *testing.T) {
	t.Parallel()

	strictCount := int64(5)
	probe := &validationCoreProbeStub{
		exact: func(
			_ context.Context,
			side ValidationSide,
			_ schema.Table,
		) (int64, error) {
			if side == ValidationSource {
				t.Fatal("live source count called despite strict-snapshot evidence")
			}
			return 5, nil
		},
	}
	report, err := RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationCountOnly),
		[]ValidationTableSpec{{
			Table: schema.Table{Name: "items"}, StrictSourceRows: &strictCount,
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf("validation failed: %#v", report.Findings)
	}
	comparison := findCoreValidationFinding(
		t,
		report,
		"items",
		"validation.count.compare",
		"",
		0,
	)
	if comparison.SourceEvidence != ValidationEvidenceStrictSnapshot {
		t.Fatalf("source evidence = %s", comparison.SourceEvidence)
	}
}

func TestValidationCoreNullParityDetectsSystematicConversion(t *testing.T) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	probe := validationCoreMatchingProbe(table)
	probe.nulls = func(
		_ context.Context,
		side ValidationSide,
		_ schema.Table,
		projection []string,
		scope ValidationNullScope,
	) (ValidationNullCountEvidence, error) {
		result := zeroNullCounts(projection)
		if side == ValidationSource {
			result["note"] = 2
		}
		return ValidationNullCountEvidence{
			Scope: cloneValidationNullScope(scope),
			Rows:  2, Counts: result,
		}, nil
	}
	report, err := RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationNullParity),
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if report.Passed {
		t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
	}
	finding := findCoreValidationFinding(
		t,
		report,
		"items",
		"validation.null_parity.compare",
		"note",
		0,
	)
	if finding.Outcome != ValidationOutcomeMismatch ||
		finding.Severity != ValidationSeverityError {
		t.Fatalf("NULL finding = %#v", finding)
	}
}

func TestValidationCoreRejectsNullCountsAboveAuthoritativeRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		strictCount *int64
		side        ValidationSide
	}{
		{name: "exact source count", side: ValidationSource},
		{name: "exact target count", side: ValidationTarget},
		{
			name:        "strict snapshot source count",
			strictCount: int64Pointer(1),
			side:        ValidationSource,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := validationCoreSampleTable("items")
			probe := validationCoreMatchingProbe(table)
			probe.exact = func(
				_ context.Context,
				_ ValidationSide,
				_ schema.Table,
			) (int64, error) {
				return 1, nil
			}
			probe.nulls = func(
				_ context.Context,
				side ValidationSide,
				_ schema.Table,
				projection []string,
				scope ValidationNullScope,
			) (ValidationNullCountEvidence, error) {
				result := zeroNullCounts(projection)
				if side == test.side {
					result["note"] = 2
				}
				return ValidationNullCountEvidence{
					Scope: cloneValidationNullScope(scope),
					Rows:  1, Counts: result,
				}, nil
			}
			report, err := RunValidationCore(
				context.Background(),
				validationCoreTestOptions(config.ValidationNullParity),
				[]ValidationTableSpec{{
					Table: table, Projection: validationCoreProjection(),
					StrictSourceRows: test.strictCount,
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed {
				t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
			}
			var finding CoreValidationFinding
			found := false
			for _, candidate := range report.Findings {
				if candidate.Check == "validation.null_parity.collect" &&
					candidate.Side == test.side {
					finding = candidate
					found = true
					break
				}
			}
			if !found ||
				finding.Severity != ValidationSeverityError ||
				!strings.Contains(finding.Message, "authoritative row count") {
				t.Fatalf("%s NULL evidence finding = %#v", test.side, finding)
			}
		})
	}
}

func TestValidationCoreUpsertNullParityUsesSourceOwnedTargetScope(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name              string
		targetScopedNulls int64
		targetWholeNulls  int64
		wantPassed        bool
	}{
		{
			name:              "target-only NULL row is outside comparison scope",
			targetScopedNulls: 1,
			targetWholeNulls:  2,
			wantPassed:        true,
		},
		{
			name:              "source-row conversion is not masked by target-only NULL",
			targetScopedNulls: 0,
			targetWholeNulls:  1,
			wantPassed:        false,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := validationCoreSampleTable("items")
			probe := validationCoreMatchingProbe(table)
			probe.exact = func(
				_ context.Context,
				side ValidationSide,
				_ schema.Table,
			) (int64, error) {
				if side == ValidationSource {
					return 2, nil
				}
				return 3, nil
			}
			var (
				scopeMu     sync.Mutex
				targetScope ValidationNullScope
			)
			probe.nulls = func(
				_ context.Context,
				side ValidationSide,
				_ schema.Table,
				projection []string,
				scope ValidationNullScope,
			) (ValidationNullCountEvidence, error) {
				counts := zeroNullCounts(projection)
				rows := int64(2)
				if side == ValidationSource {
					counts["note"] = 1
				} else {
					scopeMu.Lock()
					targetScope = cloneValidationNullScope(scope)
					scopeMu.Unlock()
					switch scope.Kind {
					case ValidationNullScopeTargetSourcePrimaryKeys:
						counts["note"] = test.targetScopedNulls
					case ValidationNullScopeWholeTarget:
						rows = 3
						counts["note"] = test.targetWholeNulls
					}
				}
				return ValidationNullCountEvidence{
					Scope: cloneValidationNullScope(scope),
					Rows:  rows, Counts: counts,
				}, nil
			}
			options := validationCoreTestOptions(
				config.ValidationNullParity,
			)
			options.TargetMode = "upsert"
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{
					Table: table, Projection: validationCoreProjection(),
					PrimaryKeyEqualityProof: validationCoreTestEqualityProof,
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed != test.wantPassed {
				t.Fatalf(
					"passed = %t, want %t: %#v",
					report.Passed,
					test.wantPassed,
					report.Findings,
				)
			}
			scopeMu.Lock()
			gotScope := cloneValidationNullScope(targetScope)
			scopeMu.Unlock()
			wantScope := ValidationNullScope{
				Kind:                ValidationNullScopeTargetSourcePrimaryKeys,
				PrimaryKeyColumns:   []string{"id", "code"},
				EqualityProofDigest: validationCoreTestEqualityProof,
			}
			if !sameValidationNullScope(gotScope, wantScope) {
				t.Fatalf("target NULL scope = %#v, want %#v", gotScope, wantScope)
			}
		})
	}
}

func TestValidationCoreUpsertNullParityRequiresRouteEqualityProof(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name  string
		proof string
	}{
		{name: "missing"},
		{name: "too short", proof: "abc123"},
		{
			name:  "uppercase is not canonical",
			proof: strings.Repeat("A", 64),
		},
		{
			name:  "non hexadecimal",
			proof: strings.Repeat("g", 64),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := validationCoreSampleTable("items")
			probe := validationCoreMatchingProbe(table)
			options := validationCoreTestOptions(
				config.ValidationNullParity,
			)
			options.TargetMode = "upsert"
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{
					Table: table, Projection: validationCoreProjection(),
					PrimaryKeyEqualityProof: test.proof,
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed {
				t.Fatalf(
					"validation unexpectedly passed: %#v",
					report.Findings,
				)
			}
			finding := findCoreValidationFinding(
				t,
				report,
				"items",
				"validation.null_parity.scope",
				"",
				0,
			)
			if finding.Outcome != ValidationOutcomeUnavailable ||
				finding.Severity != ValidationSeverityError ||
				!strings.Contains(
					finding.Remedy,
					"route-bound primary-key equality proof",
				) {
				t.Fatalf("scope finding = %#v", finding)
			}
			if got := probe.callCount("nulls:source:items") +
				probe.callCount("nulls:target:items"); got != 0 {
				t.Fatalf(
					"NULL probes ran before proof rejection: %d",
					got,
				)
			}
		})
	}
}

func TestValidationCoreUpsertNullParityRejectsMismatchedProofEcho(
	t *testing.T,
) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	probe := validationCoreMatchingProbe(table)
	probe.exact = func(
		_ context.Context,
		side ValidationSide,
		_ schema.Table,
	) (int64, error) {
		if side == ValidationSource {
			return 2, nil
		}
		return 3, nil
	}
	probe.nulls = func(
		_ context.Context,
		side ValidationSide,
		_ schema.Table,
		projection []string,
		scope ValidationNullScope,
	) (ValidationNullCountEvidence, error) {
		evidenceScope := cloneValidationNullScope(scope)
		if side == ValidationTarget {
			evidenceScope.EqualityProofDigest = strings.Repeat("b", 64)
		}
		return ValidationNullCountEvidence{
			Scope: evidenceScope,
			Rows:  2, Counts: zeroNullCounts(projection),
		}, nil
	}
	options := validationCoreTestOptions(config.ValidationNullParity)
	options.TargetMode = "upsert"
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
			PrimaryKeyEqualityProof: validationCoreTestEqualityProof,
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if report.Passed {
		t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
	}
	found := false
	for _, finding := range report.Findings {
		if finding.Check == "validation.null_parity.scope" &&
			finding.Side == ValidationTarget &&
			finding.Severity == ValidationSeverityError {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing target scope error: %#v", report.Findings)
	}
}

func TestValidationCoreUpsertNullParityRejectsNullPrimaryKeyEvidence(
	t *testing.T,
) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	probe := validationCoreMatchingProbe(table)
	probe.exact = func(
		_ context.Context,
		side ValidationSide,
		_ schema.Table,
	) (int64, error) {
		if side == ValidationSource {
			return 2, nil
		}
		return 3, nil
	}
	probe.nulls = func(
		_ context.Context,
		side ValidationSide,
		_ schema.Table,
		projection []string,
		scope ValidationNullScope,
	) (ValidationNullCountEvidence, error) {
		counts := zeroNullCounts(projection)
		if side == ValidationSource {
			counts["id"] = 1
		}
		return ValidationNullCountEvidence{
			Scope: cloneValidationNullScope(scope),
			Rows:  2, Counts: counts,
		}, nil
	}
	options := validationCoreTestOptions(config.ValidationNullParity)
	options.TargetMode = "upsert"
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
			PrimaryKeyEqualityProof: validationCoreTestEqualityProof,
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if report.Passed {
		t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
	}
	found := false
	for _, finding := range report.Findings {
		if finding.Check == "validation.null_parity.scope" &&
			finding.Side == ValidationSource &&
			finding.Column == "id" &&
			finding.Severity == ValidationSeverityError {
			found = true
		}
	}
	if !found {
		t.Fatalf("missing NULL primary-key scope error: %#v", report.Findings)
	}
}

func TestValidationCoreUpsertNullParityRejectsUnsafePrimaryKey(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*schema.Table)
	}{
		{
			name: "missing",
			mutate: func(table *schema.Table) {
				for index := range table.Columns {
					table.Columns[index].PrimaryKey = false
					table.Columns[index].PrimaryKeyPosition = 0
				}
			},
		},
		{
			name: "nullable",
			mutate: func(table *schema.Table) {
				table.Columns[0].Nullable = true
			},
		},
		{
			name: "unsupported floating point",
			mutate: func(table *schema.Table) {
				table.Columns[0].Type = "double precision"
			},
		},
		{
			name: "incomplete position sequence",
			mutate: func(table *schema.Table) {
				table.Columns[1].PrimaryKeyPosition = 3
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := validationCoreSampleTable("items")
			test.mutate(&table)
			probe := validationCoreMatchingProbe(table)
			options := validationCoreTestOptions(
				config.ValidationNullParity,
			)
			options.TargetMode = "upsert"
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{
					Table: table, Projection: validationCoreProjection(),
					PrimaryKeyEqualityProof: validationCoreTestEqualityProof,
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed {
				t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
			}
			found := false
			for _, finding := range report.Findings {
				if finding.Check == "validation.null_parity.scope" &&
					finding.Severity == ValidationSeverityError {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing unsafe-PK scope error: %#v", report.Findings)
			}
			if got := probe.callCount("nulls:source:items") +
				probe.callCount("nulls:target:items"); got != 0 {
				t.Fatalf("NULL probes ran before scope rejection: %d", got)
			}
		})
	}
}

func TestValidationCoreDeepTimeoutsHonorLogOnlyPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      config.ValidationMode
		mutate    func(*validationCoreProbeStub)
		wantCheck string
	}{
		{
			name: "NULL parity driver error after table deadline",
			mode: config.ValidationNullParity,
			mutate: func(probe *validationCoreProbeStub) {
				probe.nulls = func(
					ctx context.Context,
					_ ValidationSide,
					_ schema.Table,
					_ []string,
					_ ValidationNullScope,
				) (ValidationNullCountEvidence, error) {
					<-ctx.Done()
					return ValidationNullCountEvidence{},
						errors.New("driver returned custom NULL timeout")
				}
			},
			wantCheck: "validation.null_parity.collect",
		},
		{
			name: "sample driver error after table deadline",
			mode: config.ValidationSample,
			mutate: func(probe *validationCoreProbeStub) {
				probe.sourceSamples = func(
					ctx context.Context,
					_ schema.Table,
					_ []string,
					_ int,
				) ([]ValidationSampleRow, error) {
					<-ctx.Done()
					return nil, errors.New("driver returned custom sample timeout")
				}
			},
			wantCheck: "validation.sample.collect",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := validationCoreSampleTable("items")
			probe := validationCoreMatchingProbe(table)
			test.mutate(probe)
			options := validationCoreTestOptions(test.mode)
			// What this test asserts is the ordering of two deadlines, not
			// their absolute size: the exact count must finish well inside the
			// table budget so the deep phase is actually reached and can time
			// out. The original 5ms/25ms was too tight to survive a loaded
			// machine — under -race in the full suite, scheduler jitter alone
			// exceeded the count budget, the run short-circuited before
			// sampling, and the expected finding never appeared. These values
			// keep the same 1:5 ratio with enough headroom that ordinary jitter
			// cannot invert it, and still bound the test to well under a second.
			options.ExactCountTimeout = 50 * time.Millisecond
			options.TableTimeout = 250 * time.Millisecond
			options.FailOnTimeout = false
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{
					Table: table, Projection: validationCoreProjection(),
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if !report.Passed {
				t.Fatalf("log-only deep timeout failed: %#v", report.Findings)
			}
			foundTimeout := false
			for _, finding := range report.Findings {
				if finding.Check == test.wantCheck &&
					finding.Outcome == ValidationOutcomeTimeout {
					foundTimeout = true
					if finding.Severity != ValidationSeverityWarning {
						t.Fatalf("timeout severity = %s", finding.Severity)
					}
				}
			}
			if !foundTimeout {
				t.Fatalf("missing timeout finding: %#v", report.Findings)
			}
		})
	}
}

func TestValidationCoreSampleUsesCompletePKAndCanonicalValues(t *testing.T) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	probe := validationCoreMatchingProbe(table)
	probe.sourceSamples = func(
		context.Context,
		schema.Table,
		[]string,
		int,
	) ([]ValidationSampleRow, error) {
		return []ValidationSampleRow{
			{Values: []any{
				[]byte("second"), int16(2),
				time.Date(2026, 7, 30, 12, 30, 0, 123, time.FixedZone("CST", -6*60*60)),
				[]byte("B"), []byte{0, 1, 2},
			}},
			{Values: []any{
				"first", int32(10),
				"2026-07-30T18:30:00.000000123Z",
				"A", []byte{3, 4},
			}},
		}, nil
	}
	probe.targetSamples = func(
		_ context.Context,
		_ schema.Table,
		_ []string,
		keys []ValidationPrimaryKey,
	) ([]ValidationSampleRow, error) {
		if len(keys) != 2 {
			t.Fatalf("target key count = %d", len(keys))
		}
		for _, key := range keys {
			if len(key.Values) != 2 {
				t.Fatalf("incomplete target key = %#v", key.Values)
			}
		}
		if !reflect.DeepEqual(keys[0].Values, []any{int16(2), []byte("B")}) ||
			!reflect.DeepEqual(keys[1].Values, []any{int32(10), "A"}) {
			t.Fatalf("source primary-key order was changed: %#v", keys)
		}
		return []ValidationSampleRow{
			{Values: []any{
				[]byte("first"), "10",
				time.Date(2026, 7, 30, 18, 30, 0, 123, time.UTC),
				[]byte("A"), []byte{3, 4},
			}},
			{Values: []any{
				"second", int64(2),
				"2026-07-30 18:30:00.000000123+00:00",
				"B", []byte{0, 1, 2},
			}},
		}, nil
	}
	options := validationCoreTestOptions(config.ValidationSample)
	options.SampleLimit = 10
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf("validation failed: %#v", report.Findings)
	}
	samples := 0
	for _, finding := range report.Findings {
		if finding.Check == "validation.sample.compare" {
			samples++
			if finding.Outcome != ValidationOutcomePass {
				t.Fatalf("sample finding = %#v", finding)
			}
		}
	}
	if samples != 2 {
		t.Fatalf("sample comparisons = %d, want 2", samples)
	}
}

func TestCompareValidationPrimaryKeyValuesUsesTypedCompositeOrder(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		descriptor validationSampleDescriptor
		left       []any
		right      []any
		want       int
	}{
		{
			name: "integer mathematical order",
			descriptor: validationSampleDescriptor{
				Columns: []validationColumnDescriptor{{
					Name: "id", Kind: validationInteger,
				}},
			},
			left: []any{2}, right: []any{10}, want: -1,
		},
		{
			name: "decimal mathematical order",
			descriptor: validationSampleDescriptor{
				Columns: []validationColumnDescriptor{{
					Name: "id", Kind: validationDecimal,
				}},
			},
			left: []any{"2.5"}, right: []any{"10.25"}, want: -1,
		},
		{
			name: "later composite component",
			descriptor: validationSampleDescriptor{
				Columns: []validationColumnDescriptor{
					{Name: "tenant", Kind: validationInteger},
					{Name: "code", Kind: validationText},
				},
			},
			left: []any{2, "A"}, right: []any{2, "B"}, want: -1,
		},
		{
			name: "earlier composite component",
			descriptor: validationSampleDescriptor{
				Columns: []validationColumnDescriptor{
					{Name: "tenant", Kind: validationInteger},
					{Name: "code", Kind: validationText},
				},
			},
			left: []any{2, "Z"}, right: []any{10, "A"}, want: -1,
		},
		{
			name: "equal complete key",
			descriptor: validationSampleDescriptor{
				Columns: []validationColumnDescriptor{
					{Name: "tenant", Kind: validationInteger},
					{Name: "code", Kind: validationText},
				},
			},
			left: []any{2, "A"}, right: []any{"2", []byte("A")},
			want: 0,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := compareValidationPrimaryKeyValues(
				test.descriptor,
				test.left,
				test.right,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("comparison = %d, want %d", got, test.want)
			}
			reverse, err := compareValidationPrimaryKeyValues(
				test.descriptor,
				test.right,
				test.left,
			)
			if err != nil {
				t.Fatal(err)
			}
			if reverse != -test.want {
				t.Fatalf(
					"reverse comparison = %d, want %d",
					reverse,
					-test.want,
				)
			}
		})
	}
}

func TestValidationCoreRejectsNonIncreasingSourcePrimaryKeysBeforeTarget(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name string
		rows []ValidationSampleRow
	}{
		{
			name: "descending numeric component",
			rows: validationCoreOrderedRows(
				10, "A",
				2, "B",
			),
		},
		{
			name: "descending composite component",
			rows: validationCoreOrderedRows(
				2, "B",
				2, "A",
			),
		},
		{
			name: "equal complete key",
			rows: validationCoreOrderedRows(
				2, "A",
				2, "A",
			),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := validationCoreSampleTable("items")
			probe := validationCoreMatchingProbe(table)
			probe.sourceSamples = func(
				context.Context,
				schema.Table,
				[]string,
				int,
			) ([]ValidationSampleRow, error) {
				return cloneValidationSampleRows(test.rows), nil
			}
			probe.targetSamples = func(
				context.Context,
				schema.Table,
				[]string,
				[]ValidationPrimaryKey,
			) ([]ValidationSampleRow, error) {
				return nil, errors.New(
					"target sampler must not run for unordered source keys",
				)
			}
			report, err := RunValidationCore(
				context.Background(),
				validationCoreTestOptions(config.ValidationSample),
				[]ValidationTableSpec{{
					Table: table, Projection: validationCoreProjection(),
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed {
				t.Fatalf(
					"unordered source sample unexpectedly passed: %#v",
					report.Findings,
				)
			}
			finding := findCoreValidationFinding(
				t,
				report,
				"items",
				"validation.sample.order",
				"",
				0,
			)
			if finding.Side != ValidationSource ||
				finding.Outcome != ValidationOutcomeUnavailable ||
				finding.Severity != ValidationSeverityError {
				t.Fatalf("order finding = %#v", finding)
			}
			if got := probe.callCount(
				"sample_target:target:items",
			); got != 0 {
				t.Fatalf(
					"target sampler ran after source order failure: %d",
					got,
				)
			}
		})
	}
}

func TestValidationCoreSampleRejectsNullablePrimaryKeyBeforeDeepProbes(
	t *testing.T,
) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	table.Columns[0].Nullable = true
	probe := validationCoreMatchingProbe(table)
	report, err := RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationSample),
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if report.Passed {
		t.Fatalf(
			"nullable primary key unexpectedly passed: %#v",
			report.Findings,
		)
	}
	finding := findCoreValidationFinding(
		t,
		report,
		"items",
		"validation.projection",
		"",
		0,
	)
	if finding.Outcome != ValidationOutcomeUnavailable ||
		finding.Severity != ValidationSeverityError {
		t.Fatalf("projection finding = %#v", finding)
	}
	for _, call := range []string{
		"nulls:source:items",
		"nulls:target:items",
		"sample_source:source:items",
		"sample_target:target:items",
	} {
		if got := probe.callCount(call); got != 0 {
			t.Fatalf("deep probe %q ran before nullable-PK rejection", call)
		}
	}
}

func TestValidationCoreAcceptsEffectiveDiscardValueProjection(t *testing.T) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	table.Columns = table.Columns[:len(table.Columns)-1]
	projection := []string{"note", "id", "occurred_at", "code"}
	rows := []ValidationSampleRow{
		{Values: []any{
			"first", int64(1), "2026-07-30T00:00:00Z", "A",
		}},
		{Values: []any{
			"second", int64(2), "2026-07-30T01:00:00Z", "B",
		}},
	}
	probe := validationCoreMatchingProbe(table)
	probe.sourceSamples = func(
		context.Context,
		schema.Table,
		[]string,
		int,
	) ([]ValidationSampleRow, error) {
		return cloneValidationSampleRows(rows), nil
	}
	probe.targetSamples = func(
		context.Context,
		schema.Table,
		[]string,
		[]ValidationPrimaryKey,
	) ([]ValidationSampleRow, error) {
		return cloneValidationSampleRows(rows), nil
	}
	report, err := RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationSample),
		[]ValidationTableSpec{{
			Table: table, Projection: projection,
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf(
			"effective discard_value projection failed: %#v",
			report.Findings,
		)
	}
}

func TestValidationCoreSampleMismatchFactsDoNotLeakValues(t *testing.T) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	probe := validationCoreMatchingProbe(table)
	const sourceSecret = "sensitive-source-value"
	const targetSecret = "sensitive-target-value"
	probe.sourceSamples = func(
		context.Context,
		schema.Table,
		[]string,
		int,
	) ([]ValidationSampleRow, error) {
		return []ValidationSampleRow{{Values: []any{
			sourceSecret, int64(1), "2026-07-30T00:00:00Z",
			"A", []byte("source-secret-bytes"),
		}}}, nil
	}
	probe.targetSamples = func(
		context.Context,
		schema.Table,
		[]string,
		[]ValidationPrimaryKey,
	) ([]ValidationSampleRow, error) {
		return []ValidationSampleRow{{Values: []any{
			targetSecret, int64(1), "2026-07-30T00:00:00Z",
			"A", []byte("target-secret-bytes"),
		}}}, nil
	}
	probe.exact = func(
		_ context.Context,
		_ ValidationSide,
		_ schema.Table,
	) (int64, error) {
		return 1, nil
	}
	options := validationCoreTestOptions(config.ValidationSample)
	options.SampleLimit = 1
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if report.Passed {
		t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal report: %v", err)
	}
	for _, secret := range []string{
		sourceSecret,
		targetSecret,
		"source-secret-bytes",
		"target-secret-bytes",
	} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("report leaked %q: %s", secret, encoded)
		}
	}
}

func TestValidationCoreFailsClosedOnIncompleteEvidence(t *testing.T) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	tests := []struct {
		name       string
		mode       config.ValidationMode
		projection []string
		mutate     func(*validationCoreProbeStub)
		wantCheck  string
	}{
		{
			name:       "projection omits transferred column",
			mode:       config.ValidationNullParity,
			projection: []string{"note", "id", "occurred_at", "code"},
			wantCheck:  "validation.projection",
		},
		{
			name:       "NULL evidence omits column",
			mode:       config.ValidationNullParity,
			projection: validationCoreProjection(),
			mutate: func(probe *validationCoreProbeStub) {
				probe.nulls = func(
					_ context.Context,
					_ ValidationSide,
					_ schema.Table,
					projection []string,
					scope ValidationNullScope,
				) (ValidationNullCountEvidence, error) {
					result := zeroNullCounts(projection)
					delete(result, "note")
					return ValidationNullCountEvidence{
						Scope: cloneValidationNullScope(scope),
						Rows:  2, Counts: result,
					}, nil
				}
			},
			wantCheck: "validation.null_parity.collect",
		},
		{
			name:       "sample shorter than exact bounded set",
			mode:       config.ValidationSample,
			projection: validationCoreProjection(),
			mutate: func(probe *validationCoreProbeStub) {
				probe.sourceSamples = func(
					context.Context,
					schema.Table,
					[]string,
					int,
				) ([]ValidationSampleRow, error) {
					return []ValidationSampleRow{{
						Values: validationCoreSourceRows()[0].Values,
					}}, nil
				}
			},
			wantCheck: "validation.sample.collect",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			probe := validationCoreMatchingProbe(table)
			if test.mutate != nil {
				test.mutate(probe)
			}
			options := validationCoreTestOptions(test.mode)
			options.SampleLimit = 10
			report, err := RunValidationCore(
				context.Background(),
				options,
				[]ValidationTableSpec{{
					Table: table, Projection: test.projection,
				}},
				probe,
			)
			if err != nil {
				t.Fatalf("run validation: %v", err)
			}
			if report.Passed {
				t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
			}
			found := false
			for _, finding := range report.Findings {
				if finding.Check == test.wantCheck &&
					finding.Severity == ValidationSeverityError {
					found = true
				}
			}
			if !found {
				t.Fatalf("missing error check %s: %#v", test.wantCheck, report.Findings)
			}
		})
	}
}

func TestValidationCoreFindingsHaveStableTableOrder(t *testing.T) {
	t.Parallel()

	probe := &validationCoreProbeStub{
		exact: func(
			_ context.Context,
			_ ValidationSide,
			table schema.Table,
		) (int64, error) {
			switch table.Name {
			case "alpha":
				time.Sleep(12 * time.Millisecond)
			case "middle":
				time.Sleep(6 * time.Millisecond)
			}
			return 1, nil
		},
	}
	options := validationCoreTestOptions(config.ValidationCountOnly)
	options.TableConcurrency = 3
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{
			{Table: schema.Table{Name: "zulu"}},
			{Table: schema.Table{Name: "alpha"}},
			{Table: schema.Table{Name: "middle"}},
		},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	var tables []string
	for _, finding := range report.Findings {
		if len(tables) == 0 || tables[len(tables)-1] != finding.Table {
			tables = append(tables, finding.Table)
		}
	}
	if want := []string{"alpha", "middle", "zulu"}; !reflect.DeepEqual(tables, want) {
		t.Fatalf("table order = %v, want %v", tables, want)
	}
}

func TestValidationCoreUsesStructuralTableIdentity(t *testing.T) {
	t.Parallel()

	probe := &validationCoreProbeStub{
		exact: func(
			_ context.Context,
			_ ValidationSide,
			_ schema.Table,
		) (int64, error) {
			return 1, nil
		},
	}
	report, err := RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationCountOnly),
		[]ValidationTableSpec{
			{Table: schema.Table{Schema: "a.b", Name: "c"}},
			{Table: schema.Table{Schema: "a", Name: "b.c"}},
		},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf("structural identities failed: %#v", report.Findings)
	}
	var identities [][3]string
	for _, finding := range report.Findings {
		identity := [3]string{
			finding.Schema,
			finding.TableName,
			finding.Table,
		}
		if len(identities) == 0 ||
			identities[len(identities)-1] != identity {
			identities = append(identities, identity)
		}
	}
	want := [][3]string{
		{"a", "b.c", `a."b.c"`},
		{"a.b", "c", `"a.b".c`},
	}
	if !reflect.DeepEqual(identities, want) {
		t.Fatalf("table identities = %#v, want %#v", identities, want)
	}

	_, err = RunValidationCore(
		context.Background(),
		validationCoreTestOptions(config.ValidationCountOnly),
		[]ValidationTableSpec{
			{Table: schema.Table{Schema: "a.b", Name: "c"}},
			{Table: schema.Table{Schema: "a.b", Name: "c"}},
		},
		probe,
	)
	if err == nil || !strings.Contains(err.Error(), "duplicate validation table") {
		t.Fatalf("duplicate structural identity error = %v", err)
	}
}

func TestValidationCoreBoundsDeepTableTime(t *testing.T) {
	t.Parallel()

	table := validationCoreSampleTable("items")
	probe := validationCoreMatchingProbe(table)
	probe.nulls = func(
		ctx context.Context,
		_ ValidationSide,
		_ schema.Table,
		_ []string,
		_ ValidationNullScope,
	) (ValidationNullCountEvidence, error) {
		<-ctx.Done()
		return ValidationNullCountEvidence{}, ctx.Err()
	}
	options := validationCoreTestOptions(config.ValidationNullParity)
	options.TableTimeout = 20 * time.Millisecond
	started := time.Now()
	report, err := RunValidationCore(
		context.Background(),
		options,
		[]ValidationTableSpec{{
			Table: table, Projection: validationCoreProjection(),
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run validation: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("validation exceeded bound: %s", elapsed)
	}
	if report.Passed {
		t.Fatalf("validation unexpectedly passed: %#v", report.Findings)
	}
}

func TestValidationCoreRejectsUnboundedInvocation(t *testing.T) {
	t.Parallel()

	probe := &validationCoreProbeStub{}
	tests := []struct {
		name   string
		mutate func(*ValidationCoreOptions)
		want   string
	}{
		{
			name: "exact timeout",
			mutate: func(options *ValidationCoreOptions) {
				options.ExactCountTimeout = 0
			},
			want: "exact count timeout",
		},
		{
			name: "table timeout",
			mutate: func(options *ValidationCoreOptions) {
				options.TableTimeout = 0
			},
			want: "table timeout",
		},
		{
			name: "concurrency",
			mutate: func(options *ValidationCoreOptions) {
				options.TableConcurrency = 0
			},
			want: "table concurrency",
		},
		{
			name: "sample limit",
			mutate: func(options *ValidationCoreOptions) {
				options.SampleLimit = maxValidationSampleRows + 1
			},
			want: "sample limit",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			options := validationCoreTestOptions(config.ValidationSample)
			test.mutate(&options)
			_, err := RunValidationCore(
				context.Background(),
				options,
				nil,
				probe,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func validationCoreTestOptions(
	mode config.ValidationMode,
) ValidationCoreOptions {
	return ValidationCoreOptions{
		Mode: mode, TargetMode: "drop_recreate",
		FailOnMismatch: true, FailOnTimeout: true,
		FailOnEstimateMismatch: true,
		ExactCountTimeout:      100 * time.Millisecond,
		TableTimeout:           time.Second,
		TableConcurrency:       2,
		SampleLimit:            10,
	}
}

func validationCoreProjection() []string {
	return []string{"note", "id", "occurred_at", "code", "payload"}
}

func validationCoreSampleTable(name string) schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   name,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "code", Type: "varchar", PrimaryKey: true, PrimaryKeyPosition: 2},
			{Name: "note", Type: "text", Nullable: true},
			{Name: "occurred_at", Type: "timestamp"},
			{Name: "payload", Type: "blob", Nullable: true},
		},
	}
}

func validationCoreSourceRows() []ValidationSampleRow {
	return []ValidationSampleRow{
		{Values: []any{
			"first", int64(1), "2026-07-30T00:00:00Z",
			"A", []byte{1},
		}},
		{Values: []any{
			"second", int64(2), "2026-07-30T01:00:00Z",
			"B", []byte{2},
		}},
	}
}

func validationCoreOrderedRows(
	firstID int64,
	firstCode string,
	secondID int64,
	secondCode string,
) []ValidationSampleRow {
	return []ValidationSampleRow{
		{Values: []any{
			"first", firstID, "2026-07-30T00:00:00Z",
			firstCode, []byte{1},
		}},
		{Values: []any{
			"second", secondID, "2026-07-30T01:00:00Z",
			secondCode, []byte{2},
		}},
	}
}

func validationCoreMatchingProbe(
	table schema.Table,
) *validationCoreProbeStub {
	rows := validationCoreSourceRows()
	return &validationCoreProbeStub{
		exact: func(
			context.Context,
			ValidationSide,
			schema.Table,
		) (int64, error) {
			return int64(len(rows)), nil
		},
		nulls: func(
			_ context.Context,
			_ ValidationSide,
			_ schema.Table,
			projection []string,
			scope ValidationNullScope,
		) (ValidationNullCountEvidence, error) {
			return ValidationNullCountEvidence{
				Scope:  cloneValidationNullScope(scope),
				Rows:   int64(len(rows)),
				Counts: zeroNullCounts(projection),
			}, nil
		},
		sourceSamples: func(
			context.Context,
			schema.Table,
			[]string,
			int,
		) ([]ValidationSampleRow, error) {
			return cloneValidationSampleRows(rows), nil
		},
		targetSamples: func(
			context.Context,
			schema.Table,
			[]string,
			[]ValidationPrimaryKey,
		) ([]ValidationSampleRow, error) {
			return cloneValidationSampleRows(rows), nil
		},
	}
}

func zeroNullCounts(projection []string) map[string]int64 {
	result := make(map[string]int64, len(projection))
	for _, column := range projection {
		result[column] = 0
	}
	return result
}

func cloneValidationSampleRows(
	rows []ValidationSampleRow,
) []ValidationSampleRow {
	cloned := make([]ValidationSampleRow, len(rows))
	for index, row := range rows {
		cloned[index] = ValidationSampleRow{
			Values: append([]any(nil), row.Values...),
		}
	}
	return cloned
}

func findCoreValidationFinding(
	t *testing.T,
	report CoreValidationReport,
	table string,
	check string,
	column string,
	sample int,
) CoreValidationFinding {
	t.Helper()
	for _, finding := range report.Findings {
		if (finding.TableName == table || finding.Table == table) &&
			finding.Check == check &&
			finding.Column == column && finding.Sample == sample {
			return finding
		}
	}
	t.Fatalf(
		"missing finding table=%q check=%q column=%q sample=%d: %#v",
		table,
		check,
		column,
		sample,
		report.Findings,
	)
	return CoreValidationFinding{}
}

type validationCoreProbeStub struct {
	mu sync.Mutex

	calls []string

	exact func(
		context.Context,
		ValidationSide,
		schema.Table,
	) (int64, error)
	estimate func(
		context.Context,
		ValidationSide,
		schema.Table,
	) (int64, error)
	nulls func(
		context.Context,
		ValidationSide,
		schema.Table,
		[]string,
		ValidationNullScope,
	) (ValidationNullCountEvidence, error)
	sourceSamples func(
		context.Context,
		schema.Table,
		[]string,
		int,
	) ([]ValidationSampleRow, error)
	targetSamples func(
		context.Context,
		schema.Table,
		[]string,
		[]ValidationPrimaryKey,
	) ([]ValidationSampleRow, error)
}

func (probe *validationCoreProbeStub) ExactCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	probe.record("exact", side, table)
	if probe.exact == nil {
		return 0, fmt.Errorf("unexpected exact count")
	}
	return probe.exact(ctx, side, table)
}

func (probe *validationCoreProbeStub) EstimateCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	probe.record("estimate", side, table)
	if probe.estimate == nil {
		return 0, fmt.Errorf("unexpected estimate count")
	}
	return probe.estimate(ctx, side, table)
}

func (probe *validationCoreProbeStub) NullCounts(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
	projection []string,
	scope ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	probe.record("nulls", side, table)
	if probe.nulls == nil {
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"unexpected NULL counts",
		)
	}
	return probe.nulls(ctx, side, table, projection, scope)
}

func (probe *validationCoreProbeStub) SampleSourceRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	limit int,
) ([]ValidationSampleRow, error) {
	probe.record("sample_source", ValidationSource, table)
	if probe.sourceSamples == nil {
		return nil, fmt.Errorf("unexpected source sample")
	}
	return probe.sourceSamples(ctx, table, projection, limit)
}

func (probe *validationCoreProbeStub) SampleTargetRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	probe.record("sample_target", ValidationTarget, table)
	if probe.targetSamples == nil {
		return nil, fmt.Errorf("unexpected target sample")
	}
	return probe.targetSamples(ctx, table, projection, keys)
}

func (probe *validationCoreProbeStub) record(
	operation string,
	side ValidationSide,
	table schema.Table,
) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.calls = append(
		probe.calls,
		fmt.Sprintf("%s:%s:%s", operation, side, table.Name),
	)
}

func (probe *validationCoreProbeStub) callCount(call string) int {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	count := 0
	for _, candidate := range probe.calls {
		if candidate == call {
			count++
		}
	}
	return count
}
