package migrate

import (
	"fmt"
	"sort"
	"strings"
	"unicode"
)

// PreflightSeverity is the operator-facing severity of one preflight fact.
type PreflightSeverity string

const (
	PreflightSeverityInfo    PreflightSeverity = "info"
	PreflightSeverityWarning PreflightSeverity = "warning"
	PreflightSeverityError   PreflightSeverity = "error"
)

// PreflightSide identifies the endpoint which produced or owns a finding.
type PreflightSide string

const (
	PreflightSource PreflightSide = "source"
	PreflightTarget PreflightSide = "target"
)

// PreflightFinding is immutable evidence produced by an adapter probe.
type PreflightFinding struct {
	Severity PreflightSeverity `json:"severity"`
	Check    string            `json:"check"`
	Side     PreflightSide     `json:"side"`
	Message  string            `json:"message"`
	Remedy   string            `json:"remedy"`
}

type PreflightSkipMatch string

const (
	PreflightSkipExact  PreflightSkipMatch = "exact"
	PreflightSkipPrefix PreflightSkipMatch = "prefix"
	PreflightSkipAll    PreflightSkipMatch = "all"
)

// PreflightSkipProvenance records the exact operator selector which changed a
// finding's effective severity.
type PreflightSkipProvenance struct {
	Selector string             `json:"selector"`
	Match    PreflightSkipMatch `json:"match"`
}

// EvaluatedPreflightFinding preserves the original fact and visibly records
// any operator skip. Message and Remedy are never rewritten by skip policy.
type EvaluatedPreflightFinding struct {
	Severity         PreflightSeverity        `json:"severity"`
	OriginalSeverity PreflightSeverity        `json:"original_severity,omitempty"`
	Check            string                   `json:"check"`
	Side             PreflightSide            `json:"side"`
	Message          string                   `json:"message"`
	Remedy           string                   `json:"remedy"`
	Skipped          bool                     `json:"skipped"`
	Skip             *PreflightSkipProvenance `json:"skip,omitempty"`
}

// PreflightDecision is safe to act on only when Proceed is true. Findings are
// structurally ordered; repeated check/side evidence is rejected fail closed.
type PreflightDecision struct {
	Proceed       bool                        `json:"proceed"`
	SkipSelectors []string                    `json:"skip_selectors"`
	Findings      []EvaluatedPreflightFinding `json:"findings"`
}

const (
	maxPreflightCheckLength   = 256
	maxPreflightSegmentLength = 64
)

// EvaluatePreflight validates, orders, and applies skip policy to findings.
// Invalid, duplicate, or conflicting evidence returns a zero, fail-closed
// decision. An unskipped error always leaves Proceed false.
func EvaluatePreflight(
	findings []PreflightFinding,
	skipSelectors []string,
) (PreflightDecision, error) {
	selectors, err := normalizePreflightSkipSelectors(skipSelectors)
	if err != nil {
		return PreflightDecision{}, err
	}
	normalized, err := normalizePreflightFindings(findings)
	if err != nil {
		return PreflightDecision{}, err
	}

	decision := PreflightDecision{
		Proceed:       true,
		SkipSelectors: selectors,
		Findings: make(
			[]EvaluatedPreflightFinding,
			0,
			len(normalized),
		),
	}
	for _, finding := range normalized {
		evaluated := EvaluatedPreflightFinding{
			Severity: finding.Severity,
			Check:    finding.Check,
			Side:     finding.Side,
			Message:  finding.Message,
			Remedy:   finding.Remedy,
		}
		if skip, matched := selectPreflightSkip(
			finding.Check,
			selectors,
		); matched {
			evaluated.OriginalSeverity = finding.Severity
			evaluated.Severity = downgradedPreflightSeverity(
				finding.Severity,
			)
			evaluated.Skipped = true
			evaluated.Skip = &skip
		}
		if evaluated.Severity == PreflightSeverityError {
			decision.Proceed = false
		}
		decision.Findings = append(decision.Findings, evaluated)
	}
	return decision, nil
}

func normalizePreflightFindings(
	findings []PreflightFinding,
) ([]PreflightFinding, error) {
	candidates := append([]PreflightFinding(nil), findings...)
	validationErrors := make(map[string]struct{})
	for _, finding := range candidates {
		if err := validatePreflightFinding(finding); err != nil {
			message := fmt.Sprintf(
				"check=%q side=%q: %v",
				finding.Check,
				finding.Side,
				err,
			)
			validationErrors[message] = struct{}{}
		}
	}
	if len(validationErrors) != 0 {
		messages := make([]string, 0, len(validationErrors))
		for message := range validationErrors {
			messages = append(messages, message)
		}
		sort.Strings(messages)
		return nil, fmt.Errorf(
			"invalid preflight findings: %s",
			strings.Join(messages, "; "),
		)
	}

	sort.Slice(candidates, func(left, right int) bool {
		return preflightFindingLess(candidates[left], candidates[right])
	})
	result := make([]PreflightFinding, 0, len(candidates))
	for start := 0; start < len(candidates); {
		end := start + 1
		for end < len(candidates) &&
			candidates[end].Check == candidates[start].Check &&
			candidates[end].Side == candidates[start].Side {
			end++
		}
		if end-start > 1 {
			conflicting := false
			for index := start + 1; index < end; index++ {
				if candidates[index] != candidates[start] {
					conflicting = true
					break
				}
			}
			if conflicting {
				return nil, fmt.Errorf(
					"conflicting preflight findings for check %q on %s side",
					candidates[start].Check,
					candidates[start].Side,
				)
			}
			return nil, fmt.Errorf(
				"duplicate preflight findings for check %q on %s side",
				candidates[start].Check,
				candidates[start].Side,
			)
		}
		result = append(result, candidates[start])
		start = end
	}
	return result, nil
}

func preflightFindingLess(
	left PreflightFinding,
	right PreflightFinding,
) bool {
	if left.Check != right.Check {
		return left.Check < right.Check
	}
	if left.Side != right.Side {
		return preflightSideOrder(left.Side) <
			preflightSideOrder(right.Side)
	}
	if left.Severity != right.Severity {
		return left.Severity < right.Severity
	}
	if left.Message != right.Message {
		return left.Message < right.Message
	}
	return left.Remedy < right.Remedy
}

func validatePreflightFinding(finding PreflightFinding) error {
	switch finding.Severity {
	case PreflightSeverityInfo,
		PreflightSeverityWarning,
		PreflightSeverityError:
	default:
		return fmt.Errorf(
			"invalid severity %q",
			finding.Severity,
		)
	}
	switch finding.Side {
	case PreflightSource, PreflightTarget:
	default:
		return fmt.Errorf("invalid side %q", finding.Side)
	}
	if err := validatePreflightDottedName(finding.Check, 2); err != nil {
		return fmt.Errorf("invalid check name %q: %w", finding.Check, err)
	}
	if err := validatePreflightText("message", finding.Message); err != nil {
		return err
	}
	if err := validatePreflightText("remedy", finding.Remedy); err != nil {
		return err
	}
	return nil
}

func validatePreflightText(field string, value string) error {
	if value == "" || strings.TrimSpace(value) != value {
		return fmt.Errorf(
			"%s must be non-empty without surrounding whitespace",
			field,
		)
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s must not contain control characters", field)
	}
	return nil
}

func normalizePreflightSkipSelectors(
	selectors []string,
) ([]string, error) {
	unique := make(map[string]struct{}, len(selectors))
	validationErrors := make(map[string]struct{})
	for _, selector := range selectors {
		if selector == "all" {
			unique[selector] = struct{}{}
			continue
		}
		if err := validatePreflightDottedName(selector, 2); err != nil {
			validationErrors[fmt.Sprintf(
				"%q: %v",
				selector,
				err,
			)] = struct{}{}
			continue
		}
		unique[selector] = struct{}{}
	}
	if len(validationErrors) != 0 {
		messages := make([]string, 0, len(validationErrors))
		for message := range validationErrors {
			messages = append(messages, message)
		}
		sort.Strings(messages)
		return nil, fmt.Errorf(
			"invalid preflight skip selectors: %s",
			strings.Join(messages, "; "),
		)
	}
	result := make([]string, 0, len(unique))
	for selector := range unique {
		result = append(result, selector)
	}
	sort.Strings(result)
	return result, nil
}

func validatePreflightDottedName(
	value string,
	minimumSegments int,
) error {
	if value == "" {
		return fmt.Errorf("must not be empty")
	}
	if len(value) > maxPreflightCheckLength {
		return fmt.Errorf(
			"must not exceed %d bytes",
			maxPreflightCheckLength,
		)
	}
	segments := strings.Split(value, ".")
	if len(segments) < minimumSegments {
		return fmt.Errorf(
			"must contain at least %d dotted identifiers",
			minimumSegments,
		)
	}
	for _, segment := range segments {
		if err := validatePreflightIdentifier(segment); err != nil {
			return err
		}
	}
	return nil
}

func validatePreflightIdentifier(identifier string) error {
	if identifier == "" {
		return fmt.Errorf("contains an empty identifier")
	}
	if len(identifier) > maxPreflightSegmentLength {
		return fmt.Errorf(
			"identifier must not exceed %d bytes",
			maxPreflightSegmentLength,
		)
	}
	for index := 0; index < len(identifier); index++ {
		character := identifier[index]
		if index == 0 {
			if character < 'a' || character > 'z' {
				return fmt.Errorf(
					"identifier %q must start with a lowercase ASCII letter",
					identifier,
				)
			}
			continue
		}
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '_' {
			continue
		}
		return fmt.Errorf(
			"identifier %q contains an unsupported character",
			identifier,
		)
	}
	return nil
}

func preflightSideOrder(side PreflightSide) int {
	if side == PreflightSource {
		return 0
	}
	return 1
}

func downgradedPreflightSeverity(
	severity PreflightSeverity,
) PreflightSeverity {
	switch severity {
	case PreflightSeverityError:
		return PreflightSeverityWarning
	case PreflightSeverityWarning:
		return PreflightSeverityInfo
	default:
		return PreflightSeverityInfo
	}
}

func selectPreflightSkip(
	check string,
	selectors []string,
) (PreflightSkipProvenance, bool) {
	var (
		selected PreflightSkipProvenance
		priority int
	)
	for _, selector := range selectors {
		candidate := PreflightSkipProvenance{Selector: selector}
		candidatePriority := 0
		switch {
		case selector == check:
			candidate.Match = PreflightSkipExact
			candidatePriority = 3
		case selector != "all" &&
			strings.HasPrefix(check, selector+"."):
			candidate.Match = PreflightSkipPrefix
			candidatePriority = 2
		case selector == "all":
			candidate.Match = PreflightSkipAll
			candidatePriority = 1
		default:
			continue
		}
		if candidatePriority > priority ||
			candidatePriority == priority &&
				len(candidate.Selector) > len(selected.Selector) {
			selected = candidate
			priority = candidatePriority
		}
	}
	return selected, priority != 0
}
