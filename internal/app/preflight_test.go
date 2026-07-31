package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/migrate"
	_ "modernc.org/sqlite"
)

func TestPreflightAcceptsDistinctSQLitePaths(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	createPreflightSQLiteTable(t, sourcePath, false)
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: sqlite\n"+
			"  database: "+sourcePath+"\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+filepath.Join(directory, "target.db")+"\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != Success {
		t.Fatalf(
			"code = %d, stderr = %s, report = %#v",
			code,
			stderr,
			report,
		)
	}
	if !report.Proceed || len(report.Findings) == 0 {
		t.Fatalf("report = %#v", report)
	}
	assertPreflightFinding(
		t,
		report,
		"connection.authentication",
		migrate.PreflightSource,
		preflightClassPassed,
		migrate.PreflightSeverityInfo,
	)
	assertPreflightFinding(
		t,
		report,
		"database.exists",
		migrate.PreflightTarget,
		preflightClassUnverified,
		migrate.PreflightSeverityWarning,
	)
	assertPreflightFinding(
		t,
		report,
		"target.disk_capacity",
		migrate.PreflightTarget,
		preflightClassPassed,
		migrate.PreflightSeverityInfo,
	)
}

func TestPreflightRejectsStrictConsistencyAsUnsupportedCapability(
	t *testing.T,
) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	createPreflightSQLiteTable(t, sourcePath, false)
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: sqlite\n"+
			"  database: "+sourcePath+"\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+filepath.Join(directory, "target.db")+"\n"+
			"migration:\n"+
			"  target_mode: upsert\n"+
			"  strict_consistency: true\n"+
			"  strict_consistency_scope: migration\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != ConfigurationError || stderr != "" || report.Proceed {
		t.Fatalf(
			"code = %d, stderr = %q, report = %#v",
			code,
			stderr,
			report,
		)
	}
	assertPreflightFinding(
		t,
		report,
		"engine.capability",
		migrate.PreflightSource,
		preflightClassFailed,
		migrate.PreflightSeverityError,
	)
	assertPreflightFinding(
		t,
		report,
		"consistency.strict_prerequisites",
		migrate.PreflightSource,
		preflightClassFailed,
		migrate.PreflightSeverityError,
	)
}

func TestPreflightRejectsMissingSourceWithoutCreatingIt(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "missing-source.db")
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: sqlite\n"+
			"  database: "+sourcePath+"\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+filepath.Join(directory, "target.db")+"\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != ConfigurationError || stderr != "" || report.Proceed {
		t.Fatalf(
			"code = %d, stderr = %q, report = %#v",
			code,
			stderr,
			report,
		)
	}
	assertPreflightFinding(
		t,
		report,
		"connection.reachability",
		migrate.PreflightSource,
		preflightClassFailed,
		migrate.PreflightSeverityError,
	)
	if _, err := os.Stat(sourcePath); !os.IsNotExist(err) {
		t.Fatalf("missing source was created: %v", err)
	}
}

func TestPreflightRejectsSameDatabase(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "source.db")
	createPreflightSQLiteTable(t, databasePath, false)
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: sqlite\n"+
			"  database: "+databasePath+"\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+databasePath+"\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != ConfigurationError || stderr != "" || report.Proceed {
		t.Fatalf(
			"code = %d, stderr = %q, report = %#v",
			code,
			stderr,
			report,
		)
	}
	assertPreflightFinding(
		t,
		report,
		"engine.capability",
		migrate.PreflightTarget,
		preflightClassFailed,
		migrate.PreflightSeverityError,
	)
}

func TestPreflightRejectsIncompleteNetworkSource(t *testing.T) {
	directory := t.TempDir()
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: postgres\n"+
			"  database: source\n"+
			"  user: dmtx\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+filepath.Join(directory, "target.db")+"\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != ConfigurationError || stderr != "" || report.Proceed {
		t.Fatalf(
			"code = %d, stderr = %q, report = %#v",
			code,
			stderr,
			report,
		)
	}
	assertPreflightFinding(
		t,
		report,
		"connection.reachability",
		migrate.PreflightSource,
		preflightClassFailed,
		migrate.PreflightSeverityError,
	)
}

func TestPreflightRejectsIncompleteNetworkTarget(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	createPreflightSQLiteTable(t, sourcePath, false)
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: sqlite\n"+
			"  database: "+sourcePath+"\n"+
			"target:\n"+
			"  type: postgres\n"+
			"  database: target\n"+
			"  user: dmtx\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != ConfigurationError || stderr != "" || report.Proceed {
		t.Fatalf(
			"code = %d, stderr = %q, report = %#v",
			code,
			stderr,
			report,
		)
	}
	assertPreflightFinding(
		t,
		report,
		"connection.authentication",
		migrate.PreflightTarget,
		preflightClassFailed,
		migrate.PreflightSeverityError,
	)
}

func TestPreflightExactSkipKeepsPopulatedTargetEvidenceVisible(
	t *testing.T,
) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	createPreflightSQLiteTable(t, sourcePath, true)
	createPreflightSQLiteTable(t, targetPath, true)
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: sqlite\n"+
			"  database: "+sourcePath+"\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+targetPath+"\n"+
			"migration:\n"+
			"  preflight:\n"+
			"    skip_checks: [target.destructive_acknowledgement]\n",
	)

	report, code, stderr := runPreflightCommand(t, configPath)
	if code != Success || stderr != "" || !report.Proceed {
		t.Fatalf(
			"code = %d, stderr = %q, report = %#v",
			code,
			stderr,
			report,
		)
	}
	finding := findPreflightFinding(
		t,
		report,
		"target.destructive_acknowledgement",
		migrate.PreflightTarget,
	)
	if !finding.Skipped ||
		finding.Severity != migrate.PreflightSeverityWarning ||
		finding.OriginalSeverity != migrate.PreflightSeverityError ||
		finding.Class != preflightClassFailed ||
		finding.Evidence != "populated_target_unacknowledged" ||
		finding.Skip == nil ||
		finding.Skip.Match != migrate.PreflightSkipExact {
		t.Fatalf("skipped destructive evidence = %#v", finding)
	}
}

func TestPreflightOutputNeverContainsResolvedOrConfiguredPassword(
	t *testing.T,
) {
	const secret = "preflight-super-secret"
	directory := t.TempDir()
	configPath := writePreflightConfig(
		t,
		directory,
		"source:\n"+
			"  type: postgres\n"+
			"  database: source\n"+
			"  user: dmtx\n"+
			"  password: "+secret+"\n"+
			"target:\n"+
			"  type: sqlite\n"+
			"  database: "+filepath.Join(directory, "target.db")+"\n",
	)

	var stdout, stderr bytes.Buffer
	code := Run(
		[]string{"preflight", "--config", configPath},
		&stdout,
		&stderr,
	)
	if code != ConfigurationError {
		t.Fatalf("code = %d, stdout = %s", code, stdout.String())
	}
	if bytes.Contains(stdout.Bytes(), []byte(secret)) ||
		bytes.Contains(stderr.Bytes(), []byte(secret)) {
		t.Fatalf(
			"secret leaked: stdout=%q stderr=%q",
			stdout.String(),
			stderr.String(),
		)
	}
}

func TestPreflightDoesNotMutateSQLiteFilesOrSchema(t *testing.T) {
	t.Run("existing source and target", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "source.db")
		targetPath := filepath.Join(directory, "target.db")
		createPreflightSQLiteTable(t, sourcePath, true)
		createPreflightSQLiteTable(t, targetPath, true)
		sourceBefore, err := os.ReadFile(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		targetBefore, err := os.ReadFile(targetPath)
		if err != nil {
			t.Fatal(err)
		}
		sourceSchemaBefore := preflightSQLiteSchema(t, sourcePath)
		targetSchemaBefore := preflightSQLiteSchema(t, targetPath)
		configPath := writePreflightConfig(
			t,
			directory,
			"source:\n"+
				"  type: sqlite\n"+
				"  database: "+sourcePath+"\n"+
				"target:\n"+
				"  type: sqlite\n"+
				"  database: "+targetPath+"\n",
		)

		_, code, stderr := runPreflightCommand(t, configPath)
		if code != ConfigurationError || stderr != "" {
			t.Fatalf("code = %d, stderr = %q", code, stderr)
		}
		sourceAfter, err := os.ReadFile(sourcePath)
		if err != nil {
			t.Fatal(err)
		}
		targetAfter, err := os.ReadFile(targetPath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(sourceBefore, sourceAfter) ||
			!bytes.Equal(targetBefore, targetAfter) {
			t.Fatal("preflight changed SQLite database bytes")
		}
		if sourceSchemaBefore != preflightSQLiteSchema(t, sourcePath) ||
			targetSchemaBefore != preflightSQLiteSchema(t, targetPath) {
			t.Fatal("preflight changed SQLite schema")
		}
	})

	t.Run("missing source and target", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath := filepath.Join(directory, "missing-source.db")
		targetPath := filepath.Join(directory, "missing-target.db")
		configPath := writePreflightConfig(
			t,
			directory,
			"source:\n"+
				"  type: sqlite\n"+
				"  database: "+sourcePath+"\n"+
				"target:\n"+
				"  type: sqlite\n"+
				"  database: "+targetPath+"\n",
		)

		_, code, stderr := runPreflightCommand(t, configPath)
		if code != ConfigurationError || stderr != "" {
			t.Fatalf("code = %d, stderr = %q", code, stderr)
		}
		for _, path := range []string{sourcePath, targetPath} {
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("preflight created missing SQLite file %s: %v", path, err)
			}
		}
	})
}

func createPreflightSQLiteTable(
	t *testing.T,
	path string,
	withRow bool,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(
		`CREATE TABLE source_probe (id INTEGER PRIMARY KEY)`,
	); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if withRow {
		if _, err := database.Exec(
			`INSERT INTO source_probe (id) VALUES (1)`,
		); err != nil {
			_ = database.Close()
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func preflightSQLiteSchema(t *testing.T, path string) string {
	t.Helper()
	database, err := sql.Open("sqlite", sqlitePreflightReadOnlyURI(path))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var schemaSQL string
	if err := database.QueryRow(`
		SELECT COALESCE(group_concat(sql, ';'), '')
		FROM (
			SELECT sql
			FROM sqlite_schema
			WHERE sql IS NOT NULL
			ORDER BY type, name
		)
	`).Scan(&schemaSQL); err != nil {
		t.Fatal(err)
	}
	return schemaSQL
}

func writePreflightConfig(
	t *testing.T,
	directory string,
	contents string,
) string {
	t.Helper()
	path := filepath.Join(directory, "migration.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runPreflightCommand(
	t *testing.T,
	configPath string,
) (productionPreflightReport, int, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := Run(
		[]string{"preflight", "--config", configPath},
		&stdout,
		&stderr,
	)
	var report productionPreflightReport
	if stdout.Len() != 0 {
		if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
			t.Fatalf(
				"decode preflight report: %v: %q",
				err,
				stdout.String(),
			)
		}
	}
	return report, code, stderr.String()
}

func assertPreflightFinding(
	t *testing.T,
	report productionPreflightReport,
	check string,
	side migrate.PreflightSide,
	class preflightFindingClass,
	severity migrate.PreflightSeverity,
) {
	t.Helper()
	finding := findPreflightFinding(t, report, check, side)
	if finding.Class != class || finding.Severity != severity ||
		finding.Message == "" || finding.Remedy == "" ||
		finding.Evidence == "" {
		t.Fatalf("finding = %#v", finding)
	}
}

func findPreflightFinding(
	t *testing.T,
	report productionPreflightReport,
	check string,
	side migrate.PreflightSide,
) productionPreflightFinding {
	t.Helper()
	for _, finding := range report.Findings {
		if finding.Check == check && finding.Side == side {
			return finding
		}
	}
	t.Fatalf(
		"missing preflight finding %s/%s in %#v",
		check,
		side,
		report,
	)
	return productionPreflightFinding{}
}
