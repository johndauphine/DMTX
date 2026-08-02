package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4SQLiteStrictComposedPostgresLiveTLS is the representative
// cross-engine Section 10 sentinel. A WAL source commit occurs only after
// SQLite's same-view evidence is durable; the production aggregate route must
// transfer and validate the retained three-row view, not the later four-row
// source. The public endpoint passed to production retains verify-full and an
// explicit CA rather than merely checking the setup DSN.
func TestStage4SQLiteStrictComposedPostgresLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN to run the SQLite-to-PostgreSQL strict TLS sentinel")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL TLS DSN: %v", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must use verified TLS")
	}
	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	target, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = target.Close() })
	if err := target.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	namespace := "dmtx_sqlite_strict_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := target.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(namespace)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
		defer done()
		if _, err := target.ExecContext(cleanup, "DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE"); err != nil {
			t.Errorf("drop SQLite strict PostgreSQL schema: %v", err)
		}
	})
	if _, err := target.ExecContext(ctx, "CREATE TABLE "+postgresQualified(namespace, "items")+" (id bigint PRIMARY KEY, payload text NOT NULL)"); err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	sourceSetup, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceSetup.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE items (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`); err != nil {
		sourceSetup.Close()
		t.Fatal(err)
	}
	if err := sourceSetup.Close(); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", sqliteSourceTestURI(sourcePath, "rw"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = writer.Close() })

	raw := state.YAMLStore{Path: filepath.Join(directory, "state.yaml")}
	runID := "stage4-sqlite-strict-postgres-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if err := raw.InitializeRun(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "sqlite",
		SourceIdentity: "sqlite:" + sourcePath,
		TargetIdentity: "postgres:strict-live",
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      time.Now().UTC().Add(-time.Minute),
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
	backend := &stage4PostgresStrictMutationBackend{
		Stage4StateBackend: raw,
		mutate: func() error {
			_, err := writer.ExecContext(ctx, `INSERT INTO items VALUES (4, 'after-view')`)
			return err
		},
	}
	events := []string{}
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run:                    stage4LifecycleRunContext(t, backend, runID, false),
	}
	cfg := stage4AdapterTestConfig(t, "", parsed.Password)
	cfg.Source = config.Endpoint{Type: "sqlite", Database: sourcePath}
	cfg.Target = config.Endpoint{
		Type:      "postgres",
		Host:      parsed.Host,
		Port:      int(parsed.Port),
		Database:  parsed.Database,
		User:      parsed.User,
		Password:  parsed.Password,
		Schema:    namespace,
		SSLMode:   "verify-full",
		TLSCAFile: caFile,
	}
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.IncludeTables = []string{"items"}
	cfg.Migration.ConnectionLimit = 3
	cfg.Migration.Workers = 2
	cfg.Migration.ChunkSize = 1
	cfg.Migration.Partitions = 1
	cfg.Migration.ReaderParallelism = 4
	cfg.Migration.WriterParallelism = 1
	cfg.Migration.ReadAhead = 1
	cfg.Migration.MaxRetries = 0
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope = config.StrictConsistencyTable
	cfg.Migration.Validation.Mode = config.ValidationCountOnly
	if cfg.Target.SSLMode != "verify-full" || cfg.Target.TLSCAFile != caFile || cfg.Target.Password != parsed.Password {
		t.Fatal("production PostgreSQL endpoint lost verified TLS authority")
	}
	result, err := Execute(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("run SQLite strict PostgreSQL route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	if backend.err != nil {
		t.Fatalf("concurrent SQLite WAL mutation: %v", backend.err)
	}
	var sourceCount, targetCount int
	if err := writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM items`).Scan(&sourceCount); err != nil || sourceCount != 4 {
		t.Fatalf("source count after mutation = %d err=%v", sourceCount, err)
	}
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(namespace, "items")).Scan(&targetCount); err != nil || targetCount != 3 {
		t.Fatalf("target count = %d err=%v", targetCount, err)
	}
	work, _, err := raw.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var network state.WorkTask
	for _, candidate := range work {
		if candidate.Key.Type == stage4AdapterNetworkTaskType {
			network = candidate
		}
	}
	attempt, err := BuildStrictConsistencyAttemptID(network.Key, network.TopologyHash, 0)
	if err != nil {
		t.Fatal(err)
	}
	evidence, found, err := raw.LoadStrictSnapshotEvidence(runID, network.Key, attempt)
	if err != nil || !found || evidence.ExactSourceRowCount != 3 || evidence.SourceEngine != "sqlite" || evidence.Scope != state.StrictSnapshotTable {
		t.Fatalf("strict evidence found=%t evidence=%#v err=%v", found, evidence, err)
	}
}
