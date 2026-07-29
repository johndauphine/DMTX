package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func openNeutralIdentitySQLite(t *testing.T) *sql.DB {
	t.Helper()
	database, err := sql.Open(
		"sqlite",
		filepath.Join(t.TempDir(), "identity.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func neutralIdentityTable(frontier int64) schema.Table {
	return schema.Table{
		Name: "events",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "note", Type: "text", Nullable: true},
		},
	}
}

func TestInspectSQLiteSchemaDiscoversNeutralIdentityFrontier(t *testing.T) {
	ctx := context.Background()
	database := openNeutralIdentitySQLite(t)
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			note TEXT
		);
		INSERT INTO events(id, note) VALUES (50, 'retired');
		DELETE FROM events WHERE id = 50;
	`); err != nil {
		t.Fatal(err)
	}
	table, _, err := inspectSQLiteSchema(ctx, database, "events")
	if err != nil {
		t.Fatal(err)
	}
	if table.Identity == nil ||
		table.Identity.Column != "id" ||
		table.Identity.Generation != schema.IdentityByDefault ||
		table.Identity.Frontier == nil ||
		*table.Identity.Frontier != 50 {
		t.Fatalf("discovered identity = %#v", table.Identity)
	}
}

func TestSQLiteTargetUpsertPreflightComparesNeutralIdentity(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		targetDDL string
		planned   schema.Table
		wantErr   bool
	}{
		{
			name: "matching identity",
			targetDDL: `CREATE TABLE events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				note TEXT
			)`,
			planned: neutralIdentityTable(50),
		},
		{
			name: "planned identity missing on target",
			targetDDL: `CREATE TABLE events (
				id INTEGER PRIMARY KEY,
				note TEXT
			)`,
			planned: neutralIdentityTable(50),
			wantErr: true,
		},
		{
			name: "unexpected target identity",
			targetDDL: `CREATE TABLE events (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				note TEXT
			)`,
			planned: schema.Table{
				Name: "events",
				Columns: []schema.Column{
					{
						Name:               "id",
						Type:               "bigint",
						PrimaryKey:         true,
						PrimaryKeyPosition: 1,
					},
					{Name: "note", Type: "text", Nullable: true},
				},
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database := openNeutralIdentitySQLite(t)
			if _, err := database.ExecContext(ctx, test.targetDDL); err != nil {
				t.Fatal(err)
			}
			adapter := &sqliteTargetAdapter{database: database}
			err := adapter.PreflightTables(
				ctx,
				[]schema.Table{test.planned},
				"upsert",
			)
			if test.wantErr && err == nil {
				t.Fatal("identity mismatch was accepted")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("matching identity rejected: %v", err)
			}
			if err != nil && !strings.Contains(err.Error(), "identity") {
				t.Fatalf("identity error = %v", err)
			}
		})
	}
}

func TestSQLiteTargetUpsertFinalizeRaisesNeutralIdentityFrontier(t *testing.T) {
	ctx := context.Background()
	database := openNeutralIdentitySQLite(t)
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			note TEXT
		);
		INSERT INTO events(id, note) VALUES (7, 'existing');
	`); err != nil {
		t.Fatal(err)
	}
	adapter := &sqliteTargetAdapter{database: database}
	table := neutralIdentityTable(50)
	if err := adapter.PreflightTables(
		ctx, []schema.Table{table}, "upsert",
	); err != nil {
		t.Fatal(err)
	}
	if err := adapter.FinalizeTables(
		ctx, []schema.Table{table}, "upsert",
	); err != nil {
		t.Fatal(err)
	}
	var sequence int64
	if err := database.QueryRowContext(
		ctx,
		`SELECT seq FROM sqlite_sequence WHERE name = 'events'`,
	).Scan(&sequence); err != nil {
		t.Fatal(err)
	}
	if sequence != 50 {
		t.Fatalf("sequence = %d, want 50", sequence)
	}
	result, err := database.ExecContext(
		ctx,
		`INSERT INTO events(note) VALUES ('next')`,
	)
	if err != nil {
		t.Fatal(err)
	}
	next, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if next != 51 {
		t.Fatalf("next identity = %d, want 51", next)
	}
}
