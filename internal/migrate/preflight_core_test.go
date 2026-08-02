package migrate

import (
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestEvaluatePreflightOrdersFindings(t *testing.T) {
	t.Parallel()

	sourceAuth := preflightTestFinding(
		"connection.authentication",
		PreflightSource,
		PreflightSeverityError,
	)
	targetAuth := preflightTestFinding(
		"connection.authentication",
		PreflightTarget,
		PreflightSeverityInfo,
	)
	disk := preflightTestFinding(
		"target.disk.capacity",
		PreflightTarget,
		PreflightSeverityWarning,
	)
	first, err := EvaluatePreflight(
		[]PreflightFinding{
			disk,
			targetAuth,
			sourceAuth,
		},
		nil,
	)
	if err != nil {
		t.Fatalf("evaluate preflight: %v", err)
	}
	second, err := EvaluatePreflight(
		[]PreflightFinding{
			sourceAuth,
			disk,
			targetAuth,
		},
		nil,
	)
	if err != nil {
		t.Fatalf("evaluate reordered preflight: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("nondeterministic decisions:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.Proceed {
		t.Fatalf("unskipped error permitted proceed: %#v", first)
	}
	if len(first.Findings) != 3 {
		t.Fatalf("findings = %d, want 3: %#v", len(first.Findings), first.Findings)
	}
	gotOrder := []string{
		preflightFindingIdentity(first.Findings[0]),
		preflightFindingIdentity(first.Findings[1]),
		preflightFindingIdentity(first.Findings[2]),
	}
	wantOrder := []string{
		"connection.authentication/source",
		"connection.authentication/target",
		"target.disk.capacity/target",
	}
	if !reflect.DeepEqual(gotOrder, wantOrder) {
		t.Fatalf("finding order = %v, want %v", gotOrder, wantOrder)
	}
}

func TestEvaluatePreflightRejectsDuplicateFindings(t *testing.T) {
	t.Parallel()

	duplicate := preflightTestFinding(
		"connection.authentication",
		PreflightSource,
		PreflightSeverityError,
	)
	other := preflightTestFinding(
		"target.disk.capacity",
		PreflightTarget,
		PreflightSeverityInfo,
	)
	firstDecision, firstErr := EvaluatePreflight(
		[]PreflightFinding{duplicate, other, duplicate},
		nil,
	)
	secondDecision, secondErr := EvaluatePreflight(
		[]PreflightFinding{other, duplicate, duplicate},
		nil,
	)
	if firstErr == nil || !strings.Contains(
		firstErr.Error(),
		"duplicate preflight findings",
	) {
		t.Fatalf("first error = %v", firstErr)
	}
	if secondErr == nil {
		t.Fatal("reordered duplicate findings were accepted")
	}
	if firstErr.Error() != secondErr.Error() {
		t.Fatalf(
			"duplicate errors depend on input order:\nfirst=%q\nsecond=%q",
			firstErr,
			secondErr,
		)
	}
	if !reflect.DeepEqual(firstDecision, PreflightDecision{}) ||
		!reflect.DeepEqual(secondDecision, PreflightDecision{}) {
		t.Fatalf(
			"duplicate evidence returned actionable decisions:\nfirst=%#v\nsecond=%#v",
			firstDecision,
			secondDecision,
		)
	}
}

func TestEvaluatePreflightRejectsConflictingFindings(t *testing.T) {
	t.Parallel()

	base := preflightTestFinding(
		"target.permissions.create",
		PreflightTarget,
		PreflightSeverityError,
	)
	tests := []struct {
		name   string
		mutate func(*PreflightFinding)
	}{
		{
			name: "severity",
			mutate: func(finding *PreflightFinding) {
				finding.Severity = PreflightSeverityWarning
			},
		},
		{
			name: "message",
			mutate: func(finding *PreflightFinding) {
				finding.Message = "different evidence"
			},
		},
		{
			name: "remedy",
			mutate: func(finding *PreflightFinding) {
				finding.Remedy = "different remedy"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			conflict := base
			test.mutate(&conflict)
			decision, err := EvaluatePreflight(
				[]PreflightFinding{base, conflict},
				nil,
			)
			if err == nil || !strings.Contains(
				err.Error(),
				"conflicting preflight findings",
			) {
				t.Fatalf("error = %v", err)
			}
			if decision.Proceed {
				t.Fatalf("invalid evidence permitted proceed: %#v", decision)
			}
			reorderedDecision, reorderedErr := EvaluatePreflight(
				[]PreflightFinding{conflict, base},
				nil,
			)
			if reorderedErr == nil || err.Error() != reorderedErr.Error() {
				t.Fatalf(
					"conflict error depends on input order:\nfirst=%v\nsecond=%v",
					err,
					reorderedErr,
				)
			}
			if !reflect.DeepEqual(decision, PreflightDecision{}) ||
				!reflect.DeepEqual(reorderedDecision, PreflightDecision{}) {
				t.Fatalf(
					"conflicting evidence returned actionable decisions:\nfirst=%#v\nsecond=%#v",
					decision,
					reorderedDecision,
				)
			}
		})
	}
}

func TestEvaluatePreflightValidatesFindingStructure(t *testing.T) {
	t.Parallel()

	valid := preflightTestFinding(
		"connection.auth_v2",
		PreflightSource,
		PreflightSeverityInfo,
	)
	if _, err := EvaluatePreflight([]PreflightFinding{valid}, nil); err != nil {
		t.Fatalf("valid finding rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*PreflightFinding)
	}{
		{
			name: "empty check",
			mutate: func(finding *PreflightFinding) {
				finding.Check = ""
			},
		},
		{
			name: "single identifier",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection"
			},
		},
		{
			name: "leading dot",
			mutate: func(finding *PreflightFinding) {
				finding.Check = ".connection"
			},
		},
		{
			name: "trailing dot",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection."
			},
		},
		{
			name: "empty middle identifier",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection..authentication"
			},
		},
		{
			name: "uppercase",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "Connection.authentication"
			},
		},
		{
			name: "hyphen",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection.auth-check"
			},
		},
		{
			name: "leading number",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection.2auth"
			},
		},
		{
			name: "wildcard",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection.*"
			},
		},
		{
			name: "non ASCII",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection.åuth"
			},
		},
		{
			name: "long identifier",
			mutate: func(finding *PreflightFinding) {
				finding.Check = "connection." +
					strings.Repeat("a", maxPreflightSegmentLength+1)
			},
		},
		{
			name: "invalid severity",
			mutate: func(finding *PreflightFinding) {
				finding.Severity = "fatal"
			},
		},
		{
			name: "invalid side",
			mutate: func(finding *PreflightFinding) {
				finding.Side = "both"
			},
		},
		{
			name: "blank message",
			mutate: func(finding *PreflightFinding) {
				finding.Message = " "
			},
		},
		{
			name: "padded message",
			mutate: func(finding *PreflightFinding) {
				finding.Message = " evidence "
			},
		},
		{
			name: "message control",
			mutate: func(finding *PreflightFinding) {
				finding.Message = "evidence\ncontinued"
			},
		},
		{
			name: "blank remedy",
			mutate: func(finding *PreflightFinding) {
				finding.Remedy = ""
			},
		},
		{
			name: "remedy control",
			mutate: func(finding *PreflightFinding) {
				finding.Remedy = "retry\tcarefully"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			finding := valid
			test.mutate(&finding)
			decision, err := EvaluatePreflight(
				[]PreflightFinding{finding},
				nil,
			)
			if err == nil {
				t.Fatalf("invalid finding accepted: %#v", finding)
			}
			if decision.Proceed {
				t.Fatalf("invalid finding permitted proceed: %#v", decision)
			}
		})
	}
}

func TestEvaluatePreflightValidatesAndNormalizesSkipSelectors(t *testing.T) {
	t.Parallel()

	finding := preflightTestFinding(
		"target.permissions.create",
		PreflightTarget,
		PreflightSeverityError,
	)
	decision, err := EvaluatePreflight(
		[]PreflightFinding{finding},
		[]string{
			"target.permissions",
			"all",
			"target.permissions",
			"source.authentication",
		},
	)
	if err != nil {
		t.Fatalf("evaluate selectors: %v", err)
	}
	want := []string{
		"all",
		"source.authentication",
		"target.permissions",
	}
	if !reflect.DeepEqual(decision.SkipSelectors, want) {
		t.Fatalf("selectors = %v, want %v", decision.SkipSelectors, want)
	}

	invalid := []string{
		"",
		" ",
		"target",
		".target",
		"target.",
		"target..permissions",
		"Target.permissions",
		"target-permissions.create",
		"target.permissions.*",
		"target/permissions.create",
		"target.pérmissions",
	}
	for _, selector := range invalid {
		selector := selector
		t.Run(selector, func(t *testing.T) {
			t.Parallel()
			invalidDecision, err := EvaluatePreflight(
				[]PreflightFinding{finding},
				[]string{selector},
			)
			if err == nil {
				t.Fatalf("invalid selector %q accepted", selector)
			}
			if invalidDecision.Proceed {
				t.Fatalf(
					"invalid selector %q permitted proceed: %#v",
					selector,
					invalidDecision,
				)
			}
		})
	}
}

func TestEvaluatePreflightAppliesVisibleExactPrefixAndAllSkips(
	t *testing.T,
) {
	t.Parallel()

	findings := []PreflightFinding{
		preflightTestFinding(
			"target.permissions.create",
			PreflightTarget,
			PreflightSeverityError,
		),
		preflightTestFinding(
			"target.permissions.drop",
			PreflightTarget,
			PreflightSeverityError,
		),
		preflightTestFinding(
			"target.permissions.inspect",
			PreflightTarget,
			PreflightSeverityWarning,
		),
		preflightTestFinding(
			"target.permissions.create.table",
			PreflightTarget,
			PreflightSeverityError,
		),
		preflightTestFinding(
			"target.disk.capacity",
			PreflightTarget,
			PreflightSeverityError,
		),
	}
	partial, err := EvaluatePreflight(
		findings,
		[]string{
			"target.permissions",
			"target.permissions.create",
		},
	)
	if err != nil {
		t.Fatalf("evaluate partial skips: %v", err)
	}
	if partial.Proceed {
		t.Fatalf("unskipped disk error permitted proceed: %#v", partial)
	}
	assertPreflightSkip(
		t,
		partial,
		"target.permissions.create",
		PreflightSeverityError,
		PreflightSeverityWarning,
		"target.permissions.create",
		PreflightSkipExact,
	)
	assertPreflightSkip(
		t,
		partial,
		"target.permissions.drop",
		PreflightSeverityError,
		PreflightSeverityWarning,
		"target.permissions",
		PreflightSkipPrefix,
	)
	assertPreflightSkip(
		t,
		partial,
		"target.permissions.inspect",
		PreflightSeverityWarning,
		PreflightSeverityInfo,
		"target.permissions",
		PreflightSkipPrefix,
	)
	assertPreflightSkip(
		t,
		partial,
		"target.permissions.create.table",
		PreflightSeverityError,
		PreflightSeverityWarning,
		"target.permissions.create",
		PreflightSkipPrefix,
	)
	disk := findEvaluatedPreflightFinding(
		t,
		partial,
		"target.disk.capacity",
		PreflightTarget,
	)
	if disk.Skipped || disk.Skip != nil ||
		disk.Severity != PreflightSeverityError ||
		disk.OriginalSeverity != "" {
		t.Fatalf("unskipped disk finding = %#v", disk)
	}

	all, err := EvaluatePreflight(
		findings,
		[]string{
			"all",
			"target.permissions",
			"target.permissions.create",
		},
	)
	if err != nil {
		t.Fatalf("evaluate all skips: %v", err)
	}
	if !all.Proceed {
		t.Fatalf("all errors were skipped but proceed=false: %#v", all)
	}
	assertPreflightSkip(
		t,
		all,
		"target.permissions.create",
		PreflightSeverityError,
		PreflightSeverityWarning,
		"target.permissions.create",
		PreflightSkipExact,
	)
	assertPreflightSkip(
		t,
		all,
		"target.permissions.drop",
		PreflightSeverityError,
		PreflightSeverityWarning,
		"target.permissions",
		PreflightSkipPrefix,
	)
	assertPreflightSkip(
		t,
		all,
		"target.disk.capacity",
		PreflightSeverityError,
		PreflightSeverityWarning,
		"all",
		PreflightSkipAll,
	)
}

func TestEvaluatePreflightPrefixMatchingUsesDottedBoundary(t *testing.T) {
	t.Parallel()

	finding := preflightTestFinding(
		"target.permissions.create",
		PreflightTarget,
		PreflightSeverityError,
	)
	decision, err := EvaluatePreflight(
		[]PreflightFinding{finding},
		[]string{"target.permission"},
	)
	if err != nil {
		t.Fatalf("evaluate boundary selector: %v", err)
	}
	if decision.Proceed {
		t.Fatalf("partial identifier prefix skipped error: %#v", decision)
	}
	got := decision.Findings[0]
	if got.Skipped || got.Skip != nil {
		t.Fatalf("partial identifier prefix matched: %#v", got)
	}

	exact, err := EvaluatePreflight(
		[]PreflightFinding{finding},
		[]string{"target.permissions.create"},
	)
	if err != nil {
		t.Fatalf("evaluate exact selector: %v", err)
	}
	if !exact.Proceed {
		t.Fatalf("exact selector did not skip error: %#v", exact)
	}
}

func TestEvaluatePreflightPreservesSkippedEvidence(t *testing.T) {
	t.Parallel()

	raw := preflightTestFinding(
		"source.version.supported",
		PreflightSource,
		PreflightSeverityError,
	)
	raw.Message = "source version is below the supported floor"
	raw.Remedy = "upgrade the source before migration"
	decision, err := EvaluatePreflight(
		[]PreflightFinding{raw},
		[]string{"source.version"},
	)
	if err != nil {
		t.Fatalf("evaluate skipped evidence: %v", err)
	}
	if !decision.Proceed || len(decision.Findings) != 1 {
		t.Fatalf("decision = %#v", decision)
	}
	evaluated := decision.Findings[0]
	if evaluated.Check != raw.Check ||
		evaluated.Side != raw.Side ||
		evaluated.Message != raw.Message ||
		evaluated.Remedy != raw.Remedy ||
		evaluated.OriginalSeverity != raw.Severity ||
		evaluated.Severity != PreflightSeverityWarning ||
		!evaluated.Skipped ||
		evaluated.Skip == nil {
		t.Fatalf("skipped evidence changed or disappeared: %#v", evaluated)
	}
}

func TestEvaluatePreflightIsRaceSafeAndDoesNotMutateInputs(t *testing.T) {
	t.Parallel()

	findings := []PreflightFinding{
		preflightTestFinding(
			"connection.authentication",
			PreflightSource,
			PreflightSeverityError,
		),
		preflightTestFinding(
			"target.permissions.create",
			PreflightTarget,
			PreflightSeverityWarning,
		),
	}
	selectors := []string{"connection.authentication", "target.permissions"}
	findingsBefore := append([]PreflightFinding(nil), findings...)
	selectorsBefore := append([]string(nil), selectors...)
	want, err := EvaluatePreflight(findings, selectors)
	if err != nil {
		t.Fatalf("evaluate baseline: %v", err)
	}

	const workers = 32
	var group sync.WaitGroup
	errors := make(chan string, workers)
	group.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 50; iteration++ {
				got, err := EvaluatePreflight(findings, selectors)
				if err != nil {
					errors <- err.Error()
					return
				}
				if !reflect.DeepEqual(got, want) {
					errors <- "nondeterministic decision"
					return
				}
			}
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if !reflect.DeepEqual(findings, findingsBefore) {
		t.Fatalf("input findings mutated: %#v", findings)
	}
	if !reflect.DeepEqual(selectors, selectorsBefore) {
		t.Fatalf("input selectors mutated: %#v", selectors)
	}
}

func TestEvaluatePreflightEmptyEvidenceIsNonBlocking(t *testing.T) {
	t.Parallel()

	decision, err := EvaluatePreflight(
		nil,
		[]string{"source.authentication"},
	)
	if err != nil {
		t.Fatalf("evaluate empty evidence: %v", err)
	}
	if !decision.Proceed || len(decision.Findings) != 0 {
		t.Fatalf("empty decision = %#v", decision)
	}
	if want := []string{"source.authentication"}; !reflect.DeepEqual(
		decision.SkipSelectors,
		want,
	) {
		t.Fatalf("selectors = %v, want %v", decision.SkipSelectors, want)
	}
}

func preflightTestFinding(
	check string,
	side PreflightSide,
	severity PreflightSeverity,
) PreflightFinding {
	return PreflightFinding{
		Severity: severity,
		Check:    check,
		Side:     side,
		Message:  "deterministic preflight evidence",
		Remedy:   "apply the documented remediation and retry",
	}
}

func preflightFindingIdentity(finding EvaluatedPreflightFinding) string {
	return finding.Check + "/" + string(finding.Side)
}

func findEvaluatedPreflightFinding(
	t *testing.T,
	decision PreflightDecision,
	check string,
	side PreflightSide,
) EvaluatedPreflightFinding {
	t.Helper()
	for _, finding := range decision.Findings {
		if finding.Check == check && finding.Side == side {
			return finding
		}
	}
	t.Fatalf(
		"missing preflight finding %s/%s: %#v",
		check,
		side,
		decision.Findings,
	)
	return EvaluatedPreflightFinding{}
}

func assertPreflightSkip(
	t *testing.T,
	decision PreflightDecision,
	check string,
	original PreflightSeverity,
	effective PreflightSeverity,
	selector string,
	match PreflightSkipMatch,
) {
	t.Helper()
	finding := findEvaluatedPreflightFinding(
		t,
		decision,
		check,
		PreflightTarget,
	)
	if !finding.Skipped ||
		finding.OriginalSeverity != original ||
		finding.Severity != effective ||
		finding.Skip == nil ||
		finding.Skip.Selector != selector ||
		finding.Skip.Match != match {
		t.Fatalf("skip finding %s = %#v", check, finding)
	}
}
