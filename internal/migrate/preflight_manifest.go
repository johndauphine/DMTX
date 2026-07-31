package migrate

import (
	"fmt"
	"sort"
	"strings"
)

// PreflightCheckRequirement is one mandatory piece of preflight evidence.
// Check names deliberately remain independent from their source/target side so
// the same semantic check can be compared across both endpoints.
type PreflightCheckRequirement struct {
	Check string        `json:"check"`
	Side  PreflightSide `json:"side"`
}

// ProductionPreflightScope selects the checks which are conditionally
// applicable to one migration. Additional requirements let version-pinned
// adapters add engine-specific probes without weakening the common matrix.
type ProductionPreflightScope struct {
	TargetMode         string
	StrictConsistency  bool
	SourceSizeEvidence bool
	Additional         []PreflightCheckRequirement
}

// PreflightManifest is an immutable, sorted inventory of evidence which must
// exist before a decision can be actionable.
type PreflightManifest struct {
	Required []PreflightCheckRequirement `json:"required"`
}

// BuildProductionPreflightManifest returns the common Section 13 readiness
// matrix plus exact adapter-supplied checks. The manifest describes evidence;
// it performs no I/O and cannot mutate either endpoint.
func BuildProductionPreflightManifest(
	scope ProductionPreflightScope,
) (PreflightManifest, error) {
	switch scope.TargetMode {
	case "drop_recreate", "upsert":
	default:
		return PreflightManifest{}, fmt.Errorf(
			"build production preflight manifest: unsupported target mode %q",
			scope.TargetMode,
		)
	}

	requirements := make([]PreflightCheckRequirement, 0, 32)
	for _, side := range []PreflightSide{
		PreflightSource,
		PreflightTarget,
	} {
		for _, check := range []string{
			"connection.reachability",
			"connection.authentication",
			"server.version",
			"database.exists",
			"schema.exists",
			"schema.usage",
			"encoding.compatibility",
			"connection.pool_headroom",
			"engine.capability",
		} {
			requirements = append(
				requirements,
				PreflightCheckRequirement{
					Check: check,
					Side:  side,
				},
			)
		}
	}
	requirements = append(
		requirements,
		PreflightCheckRequirement{
			Check: "privileges.read",
			Side:  PreflightSource,
		},
		PreflightCheckRequirement{
			Check: "privileges.write",
			Side:  PreflightTarget,
		},
		PreflightCheckRequirement{
			Check: "source.size_evidence",
			Side:  PreflightSource,
		},
	)
	if scope.TargetMode == "drop_recreate" {
		requirements = append(
			requirements,
			PreflightCheckRequirement{
				Check: "schema.create_access",
				Side:  PreflightTarget,
			},
			PreflightCheckRequirement{
				Check: "target.destructive_acknowledgement",
				Side:  PreflightTarget,
			},
		)
	} else {
		requirements = append(
			requirements,
			PreflightCheckRequirement{
				Check: "target.upsert_capability",
				Side:  PreflightTarget,
			},
		)
	}
	if scope.StrictConsistency {
		requirements = append(
			requirements,
			PreflightCheckRequirement{
				Check: "consistency.strict_prerequisites",
				Side:  PreflightSource,
			},
		)
	}
	if scope.SourceSizeEvidence {
		requirements = append(
			requirements,
			PreflightCheckRequirement{
				Check: "target.disk_capacity",
				Side:  PreflightTarget,
			},
		)
	}
	requirements = append(requirements, scope.Additional...)
	return NewPreflightManifest(requirements)
}

// NewPreflightManifest validates, sorts, and copies an exact evidence
// inventory. Duplicate identities are rejected because they would make a
// missing-check audit ambiguous.
func NewPreflightManifest(
	requirements []PreflightCheckRequirement,
) (PreflightManifest, error) {
	normalized := append(
		[]PreflightCheckRequirement(nil),
		requirements...,
	)
	validationErrors := make(map[string]struct{})
	for _, requirement := range normalized {
		if err := validatePreflightDottedName(
			requirement.Check,
			2,
		); err != nil {
			validationErrors[fmt.Sprintf(
				"check=%q side=%q: %v",
				requirement.Check,
				requirement.Side,
				err,
			)] = struct{}{}
		}
		switch requirement.Side {
		case PreflightSource, PreflightTarget:
		default:
			validationErrors[fmt.Sprintf(
				"check=%q side=%q: invalid side",
				requirement.Check,
				requirement.Side,
			)] = struct{}{}
		}
	}
	if len(validationErrors) != 0 {
		messages := make([]string, 0, len(validationErrors))
		for message := range validationErrors {
			messages = append(messages, message)
		}
		sort.Strings(messages)
		return PreflightManifest{}, fmt.Errorf(
			"invalid preflight manifest: %s",
			strings.Join(messages, "; "),
		)
	}
	sort.Slice(normalized, func(left, right int) bool {
		if normalized[left].Check != normalized[right].Check {
			return normalized[left].Check < normalized[right].Check
		}
		return preflightSideOrder(normalized[left].Side) <
			preflightSideOrder(normalized[right].Side)
	})
	for index := 1; index < len(normalized); index++ {
		if normalized[index] == normalized[index-1] {
			return PreflightManifest{}, fmt.Errorf(
				"duplicate preflight manifest requirement for check %q on %s side",
				normalized[index].Check,
				normalized[index].Side,
			)
		}
	}
	return PreflightManifest{Required: normalized}, nil
}

// Evaluate requires one and only one valid finding for every manifest entry
// before applying skip policy. Extra adapter findings remain visible, but a
// missing common or engine-specific probe yields no actionable decision.
func (manifest PreflightManifest) Evaluate(
	findings []PreflightFinding,
	skipSelectors []string,
) (PreflightDecision, error) {
	validatedManifest, err := NewPreflightManifest(manifest.Required)
	if err != nil {
		return PreflightDecision{}, err
	}
	normalized, err := normalizePreflightFindings(findings)
	if err != nil {
		return PreflightDecision{}, err
	}
	observed := make(
		map[PreflightCheckRequirement]struct{},
		len(normalized),
	)
	for _, finding := range normalized {
		observed[PreflightCheckRequirement{
			Check: finding.Check,
			Side:  finding.Side,
		}] = struct{}{}
	}
	missing := make([]string, 0)
	for _, requirement := range validatedManifest.Required {
		if _, ok := observed[requirement]; ok {
			continue
		}
		missing = append(
			missing,
			requirement.Check+"/"+string(requirement.Side),
		)
	}
	if len(missing) != 0 {
		return PreflightDecision{}, fmt.Errorf(
			"incomplete preflight evidence: missing %s",
			strings.Join(missing, ", "),
		)
	}
	return EvaluatePreflight(normalized, skipSelectors)
}
