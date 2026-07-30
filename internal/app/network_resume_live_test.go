package app

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestResumeSQLiteToPostgresUpsertLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the app network-resume fixture",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if parsed.TLSConfig == nil {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	for _, fallback := range parsed.Fallbacks {
		if fallback.TLSConfig == nil {
			t.Fatal("every PostgreSQL fallback must require TLS")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open live PostgreSQL connection: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live PostgreSQL connection: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify live PostgreSQL connection: %T", err)
	}
	var tlsActive bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect live PostgreSQL TLS: %v", err)
	}
	if !tlsActive {
		t.Fatal("live PostgreSQL fixture is not encrypted")
	}

	namespace := fmt.Sprintf(
		"dmtx_app_resume_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := database.ExecContext(
		ctx,
		`CREATE SCHEMA `+quotePostgresTestIdentifier(namespace),
	); err != nil {
		t.Fatalf("create live PostgreSQL schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			`DROP SCHEMA IF EXISTS `+
				quotePostgresTestIdentifier(namespace)+` CASCADE`,
		); err != nil {
			t.Errorf("drop live PostgreSQL schema: %v", err)
		}
	})

	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	configPath := filepath.Join(directory, "migration.yaml")
	statePath := filepath.Join(directory, "migration.state.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `
		CREATE TABLE items (
			id BIGINT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO items (id, payload) VALUES (1, 'before');
	`); err != nil {
		source.Close()
		t.Fatalf("create live-resume SQLite source: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	endpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
		SSLMode:  "require",
	}
	bootstrap := config.Config{
		Source: config.Endpoint{
			Type: "sqlite", Database: sourcePath,
		},
		Target: endpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{"items"},
		},
	}
	bootstrapResult, err := migrate.Execute(ctx, bootstrap, nil)
	if err != nil {
		t.Fatalf("bootstrap exact PostgreSQL target: %v", err)
	}
	if bootstrapResult.Tables != 1 ||
		bootstrapResult.Rows != 1 ||
		!bootstrapResult.Validated {
		t.Fatalf("bootstrap result = %+v", bootstrapResult)
	}

	source, err = sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `
		UPDATE items SET payload = 'after' WHERE id = 1;
		INSERT INTO items (id, payload) VALUES (2, 'second');
	`); err != nil {
		source.Close()
		t.Fatalf("update live-resume SQLite source: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	configuration := fmt.Sprintf(`source:
  type: sqlite
  database: %q
target:
  type: postgres
  host: %q
  port: %d
  database: %q
  user: %q
  password: %q
  schema: %q
  ssl_mode: require
migration:
  target_mode: upsert
  include_tables: [items]
`, sourcePath, endpoint.Host, endpoint.Port, endpoint.Database,
		endpoint.User, endpoint.Password, endpoint.Schema)
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse([]byte(configuration))
	if err != nil {
		t.Fatalf("parse live-resume config: %v", err)
	}
	configHash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	compatibilityHash, err := config.ResumeCompatibilityHash(cfg)
	if err != nil {
		t.Fatal(err)
	}
	sourceIdentity, err := endpointWorkloadIdentity(cfg.Source)
	if err != nil {
		t.Fatal(err)
	}
	targetIdentity, err := endpointWorkloadIdentity(cfg.Target)
	if err != nil {
		t.Fatal(err)
	}
	store := state.SQLiteStore{Path: statePath}
	started := time.Now().Add(-time.Minute).UTC()
	const runID = "network-upsert-resume"
	if err := store.InitializeRun(state.Run{
		ID: runID, Source: cfg.Source.Database, Target: cfg.Target.Database,
		SourceEngine:   cfg.Source.Type,
		SourceIdentity: sourceIdentity, TargetIdentity: targetIdentity,
		Outcome: state.Failed, Resumable: true,
		Reason:    "interrupted after a committed upsert page",
		StartedAt: started, EndedAt: started.Add(time.Second),
	}, configHash); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveResumeCompatibilityHash(
		runID,
		compatibilityHash,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTask(state.Task{
		RunID: runID, Table: "items", StartedAt: started,
	}); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	code := Run(
		[]string{"resume", "--config", configPath, "--state", statePath},
		&stdout,
		&stderr,
	)
	if code != Success {
		t.Fatalf(
			"network resume exit=%d stdout=%q stderr=%q",
			code,
			stdout.String(),
			stderr.String(),
		)
	}
	rows, err := database.QueryContext(
		ctx,
		`SELECT id, payload FROM `+
			quotePostgresTestIdentifier(namespace)+
			`.items ORDER BY id`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type row struct {
		id      int64
		payload string
	}
	var got []row
	for rows.Next() {
		var value row
		if err := rows.Scan(&value.id, &value.payload); err != nil {
			t.Fatal(err)
		}
		got = append(got, value)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	want := []row{{id: 1, payload: "after"}, {id: 2, payload: "second"}}
	if len(got) != len(want) {
		t.Fatalf("resumed target rows = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("resumed target rows = %#v, want %#v", got, want)
		}
	}
	latest, found, err := store.Latest()
	if err != nil || !found ||
		latest.ID != runID ||
		latest.Outcome != state.Success ||
		latest.Resumable {
		t.Fatalf(
			"latest network resume = %#v, found=%t err=%v",
			latest,
			found,
			err,
		)
	}
	tasks, err := store.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 ||
		tasks[0].Table != "items" ||
		tasks[0].Status != "completed" ||
		tasks[0].RowsDone != 2 {
		t.Fatalf("network resume tasks = %#v", tasks)
	}
}

func quotePostgresTestIdentifier(value string) string {
	return `"` + string(bytes.ReplaceAll(
		[]byte(value),
		[]byte(`"`),
		[]byte(`""`),
	)) + `"`
}
