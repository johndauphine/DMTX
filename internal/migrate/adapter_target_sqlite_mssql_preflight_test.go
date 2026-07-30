package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerSQLitePreflightRejectsObjectCollisionWithoutMutation(
	t *testing.T,
) {
	database, err := sql.Open(
		"sqlite",
		sqliteTargetURI(filepath.Join(t.TempDir(), "target.db")),
	)
	if err != nil {
		t.Fatalf("open SQLite target: %v", err)
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE "retained" ("id" INTEGER PRIMARY KEY, "value" TEXT);
		INSERT INTO "retained" VALUES (1, 'keep');
		CREATE INDEX "planned_idx" ON "retained" ("value");
	`); err != nil {
		t.Fatalf("seed SQLite target: %v", err)
	}

	adapter := &sqliteTargetAdapter{
		database:       database,
		sqlServerRoute: true,
	}
	table := schema.Table{
		Name: "incoming",
		Columns: []schema.Column{{
			Name: "id", Type: "bigint", PrimaryKey: true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
		Indexes: []schema.Index{{
			Name:    "planned_idx",
			Columns: []schema.IndexColumn{{Name: "id"}},
		}},
	}
	err = adapter.PreflightTables(
		context.Background(),
		[]schema.Table{table},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "object name") {
		t.Fatalf("preflight error = %v", err)
	}
	var value string
	if err := database.QueryRow(
		`SELECT "value" FROM "retained" WHERE "id" = 1`,
	).Scan(&value); err != nil {
		t.Fatalf("read retained target after preflight: %v", err)
	}
	if value != "keep" {
		t.Fatalf("retained target value = %q", value)
	}
	var incoming int
	if err := database.QueryRow(
		`SELECT COUNT(*) FROM sqlite_schema
		  WHERE type = 'table' AND name = 'incoming'`,
	).Scan(&incoming); err != nil {
		t.Fatalf("inspect incoming target table: %v", err)
	}
	if incoming != 0 {
		t.Fatal("preflight mutated the incoming target table")
	}
}
