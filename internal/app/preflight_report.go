package app

import (
	"fmt"
	"sort"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

type preflightFindingClass string

const (
	preflightClassPassed     preflightFindingClass = "passed"
	preflightClassFailed     preflightFindingClass = "failed"
	preflightClassUnverified preflightFindingClass = "unverified"
)

// productionPreflightFact keeps operator evidence next to the normalized
// finding consumed by migrate.PreflightManifest. Class and Evidence are
// presentation facts; they never participate in skip policy.
type productionPreflightFact struct {
	Finding  migrate.PreflightFinding
	Class    preflightFindingClass
	Evidence string
}

type productionPreflightFinding struct {
	Severity         migrate.PreflightSeverity        `json:"severity"`
	OriginalSeverity migrate.PreflightSeverity        `json:"original_severity,omitempty"`
	Check            string                           `json:"check"`
	Side             migrate.PreflightSide            `json:"side"`
	Class            preflightFindingClass            `json:"class"`
	Message          string                           `json:"message"`
	Remedy           string                           `json:"remedy"`
	Evidence         string                           `json:"evidence"`
	Skipped          bool                             `json:"skipped"`
	Skip             *migrate.PreflightSkipProvenance `json:"skip,omitempty"`
}

type productionPreflightReport struct {
	Proceed       bool                         `json:"proceed"`
	SkipSelectors []string                     `json:"skip_selectors"`
	Findings      []productionPreflightFinding `json:"findings"`
}

func composeProductionPreflightReport(
	cfg config.Config,
	facts []productionPreflightFact,
	sourceSizeEvidence bool,
) (productionPreflightReport, error) {
	manifest, err := migrate.BuildProductionPreflightManifest(
		migrate.ProductionPreflightScope{
			TargetMode:         cfg.Migration.TargetMode,
			StrictConsistency:  cfg.Migration.StrictConsistency,
			SourceSizeEvidence: sourceSizeEvidence,
			Additional:         productionPreflightRequirements(cfg),
		},
	)
	if err != nil {
		return productionPreflightReport{}, err
	}

	findings := make([]migrate.PreflightFinding, 0, len(facts))
	presentation := make(
		map[migrate.PreflightCheckRequirement]productionPreflightFact,
		len(facts),
	)
	for _, fact := range facts {
		if err := validateProductionPreflightFact(fact); err != nil {
			return productionPreflightReport{}, err
		}
		key := migrate.PreflightCheckRequirement{
			Check: fact.Finding.Check,
			Side:  fact.Finding.Side,
		}
		if _, exists := presentation[key]; exists {
			return productionPreflightReport{}, fmt.Errorf(
				"duplicate production preflight evidence for check %q on %s side",
				key.Check,
				key.Side,
			)
		}
		presentation[key] = fact
		findings = append(findings, fact.Finding)
	}

	decision, err := manifest.Evaluate(
		findings,
		cfg.Migration.Preflight.SkipChecks,
	)
	if err != nil {
		return productionPreflightReport{}, err
	}
	report := productionPreflightReport{
		Proceed: decision.Proceed,
		SkipSelectors: append(
			make([]string, 0, len(decision.SkipSelectors)),
			decision.SkipSelectors...,
		),
		Findings: make(
			[]productionPreflightFinding,
			0,
			len(decision.Findings),
		),
	}
	for _, evaluated := range decision.Findings {
		key := migrate.PreflightCheckRequirement{
			Check: evaluated.Check,
			Side:  evaluated.Side,
		}
		fact, exists := presentation[key]
		if !exists {
			return productionPreflightReport{}, fmt.Errorf(
				"evaluated preflight finding %q on %s side has no presentation evidence",
				key.Check,
				key.Side,
			)
		}
		report.Findings = append(
			report.Findings,
			productionPreflightFinding{
				Severity:         evaluated.Severity,
				OriginalSeverity: evaluated.OriginalSeverity,
				Check:            evaluated.Check,
				Side:             evaluated.Side,
				Class:            fact.Class,
				Message:          evaluated.Message,
				Remedy:           evaluated.Remedy,
				Evidence:         fact.Evidence,
				Skipped:          evaluated.Skipped,
				Skip:             evaluated.Skip,
			},
		)
	}
	return report, nil
}

func productionPreflightRequirements(
	cfg config.Config,
) []migrate.PreflightCheckRequirement {
	requirements := make([]migrate.PreflightCheckRequirement, 0, 5)
	if cfg.Source.Type == "mysql" {
		requirements = append(
			requirements,
			migrate.PreflightCheckRequirement{
				Check: "engine.mysql.max_allowed_packet",
				Side:  migrate.PreflightSource,
			},
		)
	}
	if cfg.Target.Type == "mysql" {
		requirements = append(
			requirements,
			migrate.PreflightCheckRequirement{
				Check: "engine.mysql.bulk_path",
				Side:  migrate.PreflightTarget,
			},
			migrate.PreflightCheckRequirement{
				Check: "engine.mysql.max_allowed_packet",
				Side:  migrate.PreflightTarget,
			},
		)
	}
	if cfg.Migration.StrictConsistency &&
		cfg.Migration.StrictConsistencyScope ==
			config.StrictConsistencyMigration &&
		cfg.Source.Type == "mssql" {
		requirements = append(
			requirements,
			migrate.PreflightCheckRequirement{
				Check: "engine.mssql.snapshot_isolation",
				Side:  migrate.PreflightSource,
			},
		)
	}
	if cfg.Migration.Deletes.Mode == config.DeleteModeReconcile {
		requirements = append(
			requirements,
			migrate.PreflightCheckRequirement{
				Check: "privileges.delete_reconcile",
				Side:  migrate.PreflightTarget,
			},
		)
	}
	return requirements
}

func validateProductionPreflightFact(fact productionPreflightFact) error {
	switch fact.Class {
	case preflightClassPassed,
		preflightClassFailed,
		preflightClassUnverified:
	default:
		return fmt.Errorf(
			"preflight check %q on %s side has invalid class %q",
			fact.Finding.Check,
			fact.Finding.Side,
			fact.Class,
		)
	}
	if fact.Evidence == "" {
		return fmt.Errorf(
			"preflight check %q on %s side has no evidence",
			fact.Finding.Check,
			fact.Finding.Side,
		)
	}
	return nil
}

func sortedProductionPreflightFacts(
	facts []productionPreflightFact,
) []productionPreflightFact {
	result := append([]productionPreflightFact(nil), facts...)
	sort.Slice(result, func(left, right int) bool {
		if result[left].Finding.Check != result[right].Finding.Check {
			return result[left].Finding.Check <
				result[right].Finding.Check
		}
		return result[left].Finding.Side <
			result[right].Finding.Side
	})
	return result
}
