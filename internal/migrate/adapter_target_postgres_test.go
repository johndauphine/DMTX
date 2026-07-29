package migrate

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestPostgresTargetEndpointValidationDoesNotResolveSecrets(t *testing.T) {
	endpoint := config.Endpoint{
		Host:     "database.example",
		Database: "target",
		User:     "migrator",
		Password: "${file:/path/that/does/not/exist}",
	}
	if err := validatePostgresTargetEndpoint(endpoint); err != nil {
		t.Fatalf("validatePostgresTargetEndpoint: %v", err)
	}

	endpoint.Host = ""
	err := validatePostgresTargetEndpoint(endpoint)
	if err == nil ||
		err.Error() != "PostgreSQL host, database, and user are required" {
		t.Fatalf("missing-host error = %v", err)
	}
}

func TestPostgresTargetSchemaMappingDoesNotMutateSource(t *testing.T) {
	if got := postgresTargetNamespace(config.Endpoint{}); got != "public" {
		t.Fatalf("default namespace = %q, want public", got)
	}
	if got := postgresTargetNamespace(config.Endpoint{Schema: "archive"}); got != "archive" {
		t.Fatalf("explicit namespace = %q, want archive", got)
	}

	source := schema.Table{
		Schema: "source_schema",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true},
		},
	}
	target := postgresTargetTable(source, "target_schema")
	if source.Schema != "source_schema" {
		t.Fatalf("source schema mutated to %q", source.Schema)
	}
	if target.Schema != "target_schema" || target.Name != source.Name {
		t.Fatalf("target table = %#v", target)
	}

	adapter := &postgresTargetAdapter{namespace: "target_schema"}
	if adapter.Engine() != "postgres" {
		t.Fatalf("Engine() = %q, want postgres", adapter.Engine())
	}
}

func TestPostgresTargetWriteBatchReturnsDurableReceipt(t *testing.T) {
	database := newPostgresTargetTestDatabase(t)
	adapter := &postgresTargetAdapter{
		database:  database,
		namespace: "main",
	}
	table := postgresTargetTestTable()
	rows := [][]any{{int64(1), "first"}, {int64(2), "second"}}

	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	wantRows := int64(len(rows))
	if receipt.Certainty != CommitDurable ||
		receipt.AttemptedRows != wantRows ||
		receipt.CommittedRows != wantRows {
		t.Fatalf("receipt = %#v", receipt)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("receipt.Validate: %v", err)
	}
	count, err := adapter.CountRows(context.Background(), table)
	if err != nil {
		t.Fatalf("CountRows: %v", err)
	}
	if count != len(rows) {
		t.Fatalf("count = %d, want %d", count, len(rows))
	}
}

func TestPostgresTargetWriteBatchRollsBackFailedBatch(t *testing.T) {
	database := newPostgresTargetTestDatabase(t)
	adapter := &postgresTargetAdapter{
		database:  database,
		namespace: "main",
	}
	table := postgresTargetTestTable()
	rows := [][]any{{int64(1), "first"}, {int64(1), "duplicate"}}

	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		rows,
	)
	if err == nil || !strings.Contains(err.Error(), "write PostgreSQL table events") {
		t.Fatalf("error = %v", err)
	}
	if receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != int64(len(rows)) ||
		receipt.CommittedRows != 0 {
		t.Fatalf("failure receipt = %#v", receipt)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("receipt.Validate: %v", err)
	}
	count, countErr := adapter.CountRows(context.Background(), table)
	if countErr != nil {
		t.Fatalf("CountRows: %v", countErr)
	}
	if count != 0 {
		t.Fatalf("rolled-back count = %d, want 0", count)
	}
}

func TestPostgresPairWrappersRejectWrongTypes(t *testing.T) {
	tests := []struct {
		name string
		run  migrationRunner
		cfg  config.Config
		want string
	}{
		{
			name: "postgres source",
			run:  PostgresToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "mysql"},
				Target: config.Endpoint{Type: "postgres"},
			},
			want: "PostgreSQL-to-PostgreSQL requires source.type and target.type postgres",
		},
		{
			name: "postgres target",
			run:  PostgresToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "postgres"},
				Target: config.Endpoint{Type: "mysql"},
			},
			want: "PostgreSQL-to-PostgreSQL requires source.type and target.type postgres",
		},
		{
			name: "mysql source",
			run:  MySQLToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "postgres"},
				Target: config.Endpoint{Type: "postgres"},
			},
			want: "MySQL-to-PostgreSQL requires source.type mysql and target.type postgres",
		},
		{
			name: "mysql target",
			run:  MySQLToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "mysql"},
				Target: config.Endpoint{Type: "sqlite"},
			},
			want: "MySQL-to-PostgreSQL requires source.type mysql and target.type postgres",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.run(context.Background(), test.cfg, nil)
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func newPostgresTargetTestDatabase(t *testing.T) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})
	if _, err := database.Exec(
		`CREATE TABLE "events" ("id" INTEGER PRIMARY KEY, "payload" TEXT)`,
	); err != nil {
		t.Fatalf("create test table: %v", err)
	}
	return database
}

func postgresTargetTestTable() schema.Table {
	return schema.Table{
		Schema: "main",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true},
			{Name: "payload"},
		},
	}
}
