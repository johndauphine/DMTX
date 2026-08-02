package app

import (
	"bytes"
	"context"
	"database/sql"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4RebuildAggregateKillHelperEnv = "DMTX_STAGE4_REBUILD_AGGREGATE_KILL_HELPER"
	stage4RebuildAggregateKillConfigEnv = "DMTX_STAGE4_REBUILD_AGGREGATE_KILL_CONFIG"
	stage4RebuildAggregateKillStateEnv  = "DMTX_STAGE4_REBUILD_AGGREGATE_KILL_STATE"
	stage4RebuildAggregateKillEventEnv  = "DMTX_STAGE4_REBUILD_AGGREGATE_KILL_EVENT"
)

// TestStage4RebuildAggregatePublicationProcessKillLive proves the app-side
// aggregate publication boundary separately from the adapter recovery matrix.
// The child has already run the real composed drop/recreate route, published
// the original successful aggregate state, and then blocks before its terminal
// audit. A hard-killed process must be repairable without reopening the source
// or re-dropping the rebuilt PostgreSQL target. Run it against both durable
// local-state implementations because the app is the owner of the aggregate
// publication and terminal audit lifecycle.
func TestStage4RebuildAggregatePublicationProcessKillLive(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("DMTX_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN for Stage 4 rebuild aggregate-publication coverage")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL rebuild aggregate fixture: %T", err)
	}
	stage4AppRequireVerifiedPostgresTLS(t, parsed)
	caFile := stage4AppPostgresTLSCAFile(t, dsn)
	for _, stateKind := range []string{"yaml", "sqlite"} {
		stateKind := stateKind
		t.Run(stateKind, func(t *testing.T) {
			stage4RunRebuildAggregatePublicationProcessKill(t, dsn, parsed, caFile, stateKind)
		})
	}
}

// TestStage4RebuildAggregatePublicationProcessKillHelperProcess is a
// child-only helper. It deliberately uses the public app command path so the
// test covers aggregate publication, durable success, and the app lifecycle
// boundary rather than a direct migration helper.
func TestStage4RebuildAggregatePublicationProcessKillHelperProcess(t *testing.T) {
	if os.Getenv(stage4RebuildAggregateKillHelperEnv) != "1" {
		return
	}
	eventPath := os.Getenv(stage4RebuildAggregateKillEventEnv)
	appLifecycleBoundary = func(reached string) error {
		if reached != "run_success_persisted" {
			return nil
		}
		if err := os.WriteFile(eventPath, []byte(reached+"\n"), 0o600); err != nil {
			return err
		}
		return waitForParentHardKill(context.Background())
	}
	os.Exit(Run([]string{
		"run",
		"--config", os.Getenv(stage4RebuildAggregateKillConfigEnv),
		"--state", os.Getenv(stage4RebuildAggregateKillStateEnv),
		"--acknowledge-destructive",
	}, os.Stdout, os.Stderr))
}

func stage4RunRebuildAggregatePublicationProcessKill(
	t *testing.T,
	dsn string,
	parsed *pgx.ConnConfig,
	caFile string,
	stateKind string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	targetDatabase, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL rebuild aggregate target: %T", err)
	}
	t.Cleanup(func() { _ = targetDatabase.Close() })
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL rebuild aggregate target: %T", err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_rebuild_aggregate_" + suffix
	parent, child := "parent_"+suffix, "child_"+suffix
	if _, err := targetDatabase.ExecContext(ctx, "CREATE SCHEMA "+stage4AppPostgresIdentifier(namespace)); err != nil {
		t.Fatalf("create PostgreSQL rebuild aggregate schema: %T", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := targetDatabase.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+stage4AppPostgresIdentifier(namespace)+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL rebuild aggregate schema: %T", err)
		}
	})
	for _, statement := range []string{
		"CREATE TABLE " + stage4AppPostgresQualified(namespace, parent) + " (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL)",
		"CREATE TABLE " + stage4AppPostgresQualified(namespace, child) + " (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL, CONSTRAINT " + stage4AppPostgresIdentifier(child+"_parent_fk") + " FOREIGN KEY (parent_id) REFERENCES " + stage4AppPostgresQualified(namespace, parent) + "(id))",
		"INSERT INTO " + stage4AppPostgresQualified(namespace, parent) + " (id, payload) VALUES (1, 'stale-parent')",
		"INSERT INTO " + stage4AppPostgresQualified(namespace, child) + " (id, parent_id, payload) VALUES (10, 1, 'stale-child')",
	} {
		if _, err := targetDatabase.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed PostgreSQL rebuild aggregate target: %T", err)
		}
	}

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	configPath := filepath.Join(directory, "rebuild.yaml")
	statePath := filepath.Join(directory, "rebuild.state."+stage4StateSuffix(stateKind))
	eventPath := filepath.Join(directory, "success-persisted")
	stage4CreateRebuildAggregateSource(t, ctx, sourcePath, parent, child)
	target := config.Endpoint{
		Type: "postgres", Host: parsed.Host, Port: int(parsed.Port), Database: parsed.Database,
		User: parsed.User, Password: parsed.Password, Schema: namespace,
		SSLMode: "verify-full", TLSCAFile: caFile,
	}
	stage4WriteRebuildAggregateConfig(t, configPath, sourcePath, target, parent, child)

	command := exec.Command(os.Args[0], "-test.run=^TestStage4RebuildAggregatePublicationProcessKillHelperProcess$")
	command.Env = append(os.Environ(),
		stage4RebuildAggregateKillHelperEnv+"=1",
		stage4RebuildAggregateKillConfigEnv+"="+configPath,
		stage4RebuildAggregateKillStateEnv+"="+statePath,
		stage4RebuildAggregateKillEventEnv+"="+eventPath,
	)
	childProcess := startStage1Child(t, command)
	childProcess.waitForFile(t, eventPath, "published Stage 4 rebuild aggregate success")
	childProcess.kill(t)

	store, err := state.NewBackend(statePath)
	if err != nil {
		t.Fatalf("open %s rebuild aggregate state: %T", stateKind, err)
	}
	run, found, err := store.Latest()
	if err != nil || !found || run.Outcome != state.Success || run.Resumable {
		t.Fatalf("aggregate-published rebuild state found=%t run=%#v err=%T", found, run, err)
	}
	stage4AssertRebuildAggregateGraph(t, ctx, targetDatabase, namespace, parent, child, "parent-one")
	stage4ExpireRebuildAggregateLease(t, target)
	stage4MutateRebuildAggregateSource(t, ctx, sourcePath, parent)

	var stdout, stderr bytes.Buffer
	if code := Run([]string{
		"resume", "--config", configPath, "--state", statePath,
		"--acknowledge-destructive",
	}, &stdout, &stderr); code != Success {
		t.Fatalf("terminal repair after aggregate publication exit=%d", code)
	}
	stage4AssertRebuildAggregateGraph(t, ctx, targetDatabase, namespace, parent, child, "parent-one")
	repaired, found, err := store.Latest()
	if err != nil || !found || repaired.ID != run.ID || repaired.Outcome != state.Success || repaired.Resumable {
		t.Fatalf("terminal repair changed rebuild aggregate state found=%t run=%#v err=%T", found, repaired, err)
	}
	if !strings.Contains(stdout.String(), `"tables":2`) || !strings.Contains(stdout.String(), `"validated":true`) {
		t.Fatalf("terminal repair did not report the original rebuilt aggregate")
	}
	if strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("terminal repair wrote unexpected diagnostics")
	}
	stage4AssertRebuildAggregateAudit(t, configPath, run.ID)
}

func stage4StateSuffix(kind string) string {
	if kind == "sqlite" {
		return "db"
	}
	return "yaml"
}

func stage4CreateRebuildAggregateSource(
	t *testing.T,
	ctx context.Context,
	path, parent, child string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite rebuild aggregate source: %T", err)
	}
	defer database.Close()
	for _, statement := range []string{
		"CREATE TABLE " + stage4AppSQLiteIdentifier(parent) + " (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL)",
		"CREATE TABLE " + stage4AppSQLiteIdentifier(child) + " (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL, FOREIGN KEY (parent_id) REFERENCES " + stage4AppSQLiteIdentifier(parent) + "(id))",
		"INSERT INTO " + stage4AppSQLiteIdentifier(parent) + " (id, payload) VALUES (1, 'parent-one'), (2, 'parent-two')",
		"INSERT INTO " + stage4AppSQLiteIdentifier(child) + " (id, parent_id, payload) VALUES (10, 1, 'child-one'), (20, 2, 'child-two')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed SQLite rebuild aggregate source: %T", err)
		}
	}
}

func stage4WriteRebuildAggregateConfig(
	t *testing.T,
	path, sourcePath string,
	target config.Endpoint,
	parent, child string,
) {
	t.Helper()
	quote := strconv.Quote
	configuration := strings.Join([]string{
		"source:",
		"  type: sqlite",
		"  database: " + quote(sourcePath),
		"target:",
		"  type: postgres",
		"  host: " + quote(target.Host),
		"  port: " + strconv.Itoa(target.Port),
		"  database: " + quote(target.Database),
		"  user: " + quote(target.User),
		"  password: " + quote(target.Password),
		"  schema: " + quote(target.Schema),
		"  ssl_mode: verify-full",
		"  tls_ca_file: " + quote(target.TLSCAFile),
		"migration:",
		"  target_mode: drop_recreate",
		"  include_tables:",
		"    - " + quote(parent),
		"    - " + quote(child),
		"  partitions: 1",
		"  connection_limit: 4",
		"  reader_parallelism: 1",
		"  writer_parallelism: 1",
		"  memory_ceiling_bytes: 67108864",
		"  runtime_tuning: false",
		"  validation:",
		"    mode: count_only",
		"    fail_on_mismatch: true",
		"    fail_on_timeout: true",
		"    fail_on_estimate_mismatch: true",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(configuration), 0o600); err != nil {
		t.Fatalf("write rebuild aggregate config: %T", err)
	}
}

func stage4ExpireRebuildAggregateLease(t *testing.T, target config.Endpoint) {
	t.Helper()
	identity, leasePath, err := targetLeaseLocation(target)
	if err != nil {
		t.Fatalf("resolve rebuild aggregate lease: %T", err)
	}
	database, err := (state.SQLiteStore{Path: leasePath}).Open()
	if err != nil {
		t.Fatalf("open rebuild aggregate lease: %T", err)
	}
	defer database.Close()
	result, err := database.Exec(
		"UPDATE leases SET heartbeat_at = ? WHERE target = ?",
		time.Now().UTC().Add(-time.Hour), identity,
	)
	if err != nil {
		t.Fatalf("expire rebuild aggregate lease: %T", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		t.Fatalf("expire rebuild aggregate lease affected=%d err=%T", affected, err)
	}
}

func stage4MutateRebuildAggregateSource(
	t *testing.T,
	ctx context.Context,
	path, parent string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open rebuild aggregate source for mutation: %T", err)
	}
	defer database.Close()
	if _, err := database.ExecContext(ctx, "UPDATE "+stage4AppSQLiteIdentifier(parent)+" SET payload = 'changed-after-publication' WHERE id = 1"); err != nil {
		t.Fatalf("mutate rebuild aggregate source after publish: %T", err)
	}
}

func stage4AssertRebuildAggregateGraph(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace, parent, child, expectedPayload string,
) {
	t.Helper()
	var parents, children int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+stage4AppPostgresQualified(namespace, parent)).Scan(&parents); err != nil {
		t.Fatalf("count aggregate rebuilt parents: %T", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+stage4AppPostgresQualified(namespace, child)).Scan(&children); err != nil {
		t.Fatalf("count aggregate rebuilt children: %T", err)
	}
	if parents != 2 || children != 2 {
		t.Fatalf("aggregate rebuilt rows = parents:%d children:%d", parents, children)
	}
	var payload string
	if err := database.QueryRowContext(ctx, "SELECT payload FROM "+stage4AppPostgresQualified(namespace, parent)+" WHERE id = 1").Scan(&payload); err != nil {
		t.Fatalf("read aggregate rebuilt parent: %T", err)
	}
	if payload != expectedPayload {
		t.Fatalf("aggregate rebuilt parent payload = %q", payload)
	}
	var foreignKeys int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM information_schema.table_constraints
		 WHERE table_schema = $1 AND table_name = $2
		   AND constraint_type = 'FOREIGN KEY'
	`, namespace, child).Scan(&foreignKeys); err != nil {
		t.Fatalf("read aggregate rebuilt FK: %T", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("aggregate rebuilt FK count = %d", foreignKeys)
	}
}

func stage4AssertRebuildAggregateAudit(t *testing.T, configPath, runID string) {
	t.Helper()
	contents, err := os.ReadFile(configPath + ".audit.ndjson")
	if err != nil {
		t.Fatalf("read rebuild aggregate audit: %T", err)
	}
	lines := bytes.Split(bytes.TrimSpace(contents), []byte{'\n'})
	expected := []string{"run_started", "validation_completed", "resume_finalization_started", "run_succeeded"}
	matched := 0
	for index, line := range lines {
		if !bytes.Contains(line, []byte(`"run_id":"`+runID+`"`)) {
			t.Fatalf("rebuild aggregate audit event %d belongs to another run", index)
		}
		if matched < len(expected) && bytes.Contains(
			line,
			[]byte(`"type":"`+expected[matched]+`"`),
		) {
			matched++
		}
	}
	if matched != len(expected) || len(lines) == 0 || !bytes.Contains(
		lines[len(lines)-1],
		[]byte(`"type":"run_succeeded"`),
	) {
		t.Fatalf("rebuild aggregate audit does not preserve terminal publication ordering")
	}
}

func stage4AppRequireVerifiedPostgresTLS(t *testing.T, parsed *pgx.ConnConfig) {
	t.Helper()
	if parsed == nil || parsed.TLSConfig == nil || parsed.TLSConfig.InsecureSkipVerify ||
		parsed.TLSConfig.RootCAs == nil || strings.TrimSpace(parsed.TLSConfig.ServerName) == "" {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must verify PostgreSQL TLS certificate and hostname")
	}
	for _, fallback := range parsed.Fallbacks {
		if fallback.TLSConfig == nil || fallback.TLSConfig.InsecureSkipVerify ||
			fallback.TLSConfig.RootCAs == nil || strings.TrimSpace(fallback.TLSConfig.ServerName) == "" {
			t.Fatal("DMTX_TEST_POSTGRES_DSN fallback must verify PostgreSQL TLS certificate and hostname")
		}
	}
}

func stage4AppPostgresTLSCAFile(t *testing.T, dsn string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv("PGSSLROOTCERT"))
	if value == "" && (strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")) {
		if parsed, err := url.Parse(dsn); err == nil {
			value = strings.TrimSpace(parsed.Query().Get("sslrootcert"))
		}
	}
	if value == "" {
		for _, field := range strings.Fields(dsn) {
			key, candidate, found := strings.Cut(field, "=")
			if found && strings.EqualFold(key, "sslrootcert") {
				value = strings.Trim(strings.TrimSpace(candidate), "'\"")
				break
			}
		}
	}
	if value == "" || value == "system" {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must expose an explicit verified TLS CA file")
	}
	absolute, err := filepath.Abs(value)
	if err != nil {
		t.Fatalf("resolve PostgreSQL rebuild aggregate CA file: %T", err)
	}
	info, err := os.Stat(absolute)
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("inspect PostgreSQL rebuild aggregate CA file: %T", err)
	}
	return absolute
}

func stage4AppPostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func stage4AppPostgresQualified(namespace, table string) string {
	return stage4AppPostgresIdentifier(namespace) + "." + stage4AppPostgresIdentifier(table)
}

func stage4AppSQLiteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
