package app

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

func TestComposeProductionPreflightReportRequiresCompleteManifest(
	t *testing.T,
) {
	t.Parallel()

	cfg := productionPreflightTestConfig()
	facts := completeProductionPreflightTestFacts(t, cfg)
	facts = facts[1:]
	report, err := composeProductionPreflightReport(cfg, facts, false)
	if err == nil || !strings.Contains(
		err.Error(),
		"incomplete preflight evidence",
	) {
		t.Fatalf("missing manifest evidence error = %v", err)
	}
	if !reflect.DeepEqual(report, productionPreflightReport{}) {
		t.Fatalf("missing evidence returned actionable report: %#v", report)
	}
}

func TestComposeProductionPreflightReportPreservesVisibleExactSkip(
	t *testing.T,
) {
	t.Parallel()

	cfg := productionPreflightTestConfig()
	cfg.Migration.Preflight.SkipChecks = []string{
		"connection.authentication",
	}
	facts := completeProductionPreflightTestFacts(t, cfg)
	for index := range facts {
		if facts[index].Finding.Check != "connection.authentication" ||
			facts[index].Finding.Side != migrate.PreflightTarget {
			continue
		}
		facts[index] = productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityError,
				Check:    "connection.authentication",
				Side:     migrate.PreflightTarget,
				Message:  "target credentials were rejected",
				Remedy:   "provide credentials accepted by the target",
			},
			Class:    preflightClassFailed,
			Evidence: "authentication_rejected",
		}
	}

	report, err := composeProductionPreflightReport(cfg, facts, false)
	if err != nil {
		t.Fatalf("compose report: %v", err)
	}
	if !report.Proceed {
		t.Fatalf("explicit skip did not admit report: %#v", report)
	}
	if !reflect.DeepEqual(
		report.SkipSelectors,
		[]string{"connection.authentication"},
	) {
		t.Fatalf("skip selectors = %#v", report.SkipSelectors)
	}
	var target productionPreflightFinding
	for _, finding := range report.Findings {
		if finding.Check == "connection.authentication" &&
			finding.Side == migrate.PreflightTarget {
			target = finding
			break
		}
	}
	if !target.Skipped ||
		target.Severity != migrate.PreflightSeverityWarning ||
		target.OriginalSeverity != migrate.PreflightSeverityError ||
		target.Class != preflightClassFailed ||
		target.Evidence != "authentication_rejected" ||
		target.Skip == nil ||
		target.Skip.Selector != "connection.authentication" ||
		target.Skip.Match != migrate.PreflightSkipExact {
		t.Fatalf("visible skipped finding = %#v", target)
	}
}

func TestComposeProductionPreflightReportBlocksUnknownWriteAuthority(
	t *testing.T,
) {
	t.Parallel()

	cfg := productionPreflightTestConfig()
	facts := completeProductionPreflightTestFacts(t, cfg)
	unknown := targetWriteFact(productionEndpointProbe{
		endpoint: config.Endpoint{Type: "sqlite", Database: "target.db"},
		side:     migrate.PreflightTarget,
	})
	for index := range facts {
		if facts[index].Finding.Check == "privileges.write" &&
			facts[index].Finding.Side == migrate.PreflightTarget {
			facts[index] = unknown
		}
	}

	report, err := composeProductionPreflightReport(cfg, facts, false)
	if err != nil {
		t.Fatalf("compose report: %v", err)
	}
	if report.Proceed {
		t.Fatalf("unknown target write authority admitted report: %#v", report)
	}
	finding := findPreflightFinding(
		t,
		report,
		"privileges.write",
		migrate.PreflightTarget,
	)
	if finding.Severity != migrate.PreflightSeverityError ||
		finding.Class != preflightClassUnverified ||
		finding.Skipped {
		t.Fatalf("unknown target write finding = %#v", finding)
	}

	cfg.Migration.Preflight.SkipChecks = []string{"privileges.write"}
	report, err = composeProductionPreflightReport(cfg, facts, false)
	if err != nil {
		t.Fatalf("compose explicitly skipped report: %v", err)
	}
	finding = findPreflightFinding(
		t,
		report,
		"privileges.write",
		migrate.PreflightTarget,
	)
	if !report.Proceed || !finding.Skipped ||
		finding.Severity != migrate.PreflightSeverityWarning ||
		finding.OriginalSeverity != migrate.PreflightSeverityError ||
		finding.Class != preflightClassUnverified {
		t.Fatalf("skipped unknown target write finding = %#v", finding)
	}
}

func TestComposeProductionPreflightReportIsStructurallyOrdered(
	t *testing.T,
) {
	t.Parallel()

	cfg := productionPreflightTestConfig()
	facts := completeProductionPreflightTestFacts(t, cfg)
	for left, right := 0, len(facts)-1; left < right; left, right =
		left+1, right-1 {
		facts[left], facts[right] = facts[right], facts[left]
	}
	report, err := composeProductionPreflightReport(cfg, facts, false)
	if err != nil {
		t.Fatalf("compose report: %v", err)
	}
	if !report.Proceed {
		t.Fatalf("all-passing report rejected: %#v", report)
	}
	for index := 1; index < len(report.Findings); index++ {
		previous := report.Findings[index-1]
		current := report.Findings[index]
		if previous.Check > current.Check ||
			previous.Check == current.Check &&
				previous.Side > current.Side {
			t.Fatalf(
				"findings are not structurally ordered at %d: %#v then %#v",
				index,
				previous,
				current,
			)
		}
	}
}

func TestComposeProductionPreflightReportRequiresApplicableEngineFacts(
	t *testing.T,
) {
	t.Parallel()

	cfg := productionPreflightTestConfig()
	cfg.Source.Type = "mysql"
	cfg.Target.Type = "mysql"
	cfg.Migration.Deletes = config.DeletePolicy{
		Mode:           config.DeleteModeReconcile,
		TargetBehavior: config.DeleteTargetHard,
		Reconcile: config.DeleteReconcilePolicy{
			Schedule:          config.DeleteScheduleInterval,
			Interval:          config.DefaultDeleteInterval,
			BatchSize:         config.DefaultDeleteBatchSize,
			RequirePrimaryKey: true,
		},
	}
	facts := completeProductionPreflightTestFacts(t, cfg)
	var withoutDelete []productionPreflightFact
	for _, fact := range facts {
		if fact.Finding.Check != "privileges.delete_reconcile" {
			withoutDelete = append(withoutDelete, fact)
		}
	}
	report, err := composeProductionPreflightReport(
		cfg,
		withoutDelete,
		false,
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"privileges.delete_reconcile/target",
	) {
		t.Fatalf("missing delete privilege error = %v", err)
	}
	if !reflect.DeepEqual(report, productionPreflightReport{}) {
		t.Fatalf("missing engine evidence returned report: %#v", report)
	}
}

func TestProductionPreflightRequirementsCoverApplicableSafetyFacts(
	t *testing.T,
) {
	t.Parallel()

	cfg := productionPreflightTestConfig()
	cfg.Source.Type = "mysql"
	cfg.Target.Type = "mysql"
	cfg.Migration.Deletes.Mode = config.DeleteModeReconcile
	requirements := productionPreflightRequirements(cfg)
	for _, required := range []migrate.PreflightCheckRequirement{
		{
			Check: "engine.mysql.max_allowed_packet",
			Side:  migrate.PreflightSource,
		},
		{
			Check: "engine.mysql.max_allowed_packet",
			Side:  migrate.PreflightTarget,
		},
		{
			Check: "engine.mysql.bulk_path",
			Side:  migrate.PreflightTarget,
		},
		{
			Check: "privileges.delete_reconcile",
			Side:  migrate.PreflightTarget,
		},
	} {
		if !containsProductionPreflightRequirement(
			requirements,
			required,
		) {
			t.Fatalf(
				"applicable requirements %#v omit %#v",
				requirements,
				required,
			)
		}
	}

	cfg = productionPreflightTestConfig()
	cfg.Source.Type = "mssql"
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope =
		config.StrictConsistencyMigration
	requirements = productionPreflightRequirements(cfg)
	if !containsProductionPreflightRequirement(
		requirements,
		migrate.PreflightCheckRequirement{
			Check: "engine.mssql.snapshot_isolation",
			Side:  migrate.PreflightSource,
		},
	) {
		t.Fatalf(
			"SQL Server strict requirements = %#v",
			requirements,
		)
	}
}

func productionPreflightTestConfig() config.Config {
	return config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: "source.db"},
		Target: config.Endpoint{Type: "sqlite", Database: "target.db"},
		Migration: config.Migration{
			TargetMode: "upsert",
		},
	}
}

func completeProductionPreflightTestFacts(
	t *testing.T,
	cfg config.Config,
) []productionPreflightFact {
	t.Helper()
	manifest, err := migrate.BuildProductionPreflightManifest(
		migrate.ProductionPreflightScope{
			TargetMode:        cfg.Migration.TargetMode,
			StrictConsistency: cfg.Migration.StrictConsistency,
			Additional:        productionPreflightRequirements(cfg),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	facts := make(
		[]productionPreflightFact,
		0,
		len(manifest.Required),
	)
	for _, requirement := range manifest.Required {
		facts = append(facts, productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityInfo,
				Check:    requirement.Check,
				Side:     requirement.Side,
				Message:  "required evidence is present",
				Remedy:   "no operator action is required",
			},
			Class:    preflightClassPassed,
			Evidence: "verified",
		})
	}
	return facts
}

func containsProductionPreflightRequirement(
	requirements []migrate.PreflightCheckRequirement,
	required migrate.PreflightCheckRequirement,
) bool {
	for _, candidate := range requirements {
		if candidate == required {
			return true
		}
	}
	return false
}
