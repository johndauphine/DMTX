package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/schema"
	_ "modernc.org/sqlite"
)

func TestSQLiteToSQLitePreservesRichSemanticSchemaAndExactRows(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLiteSchemaTestDatabase(t, sourcePath)
	if _, err := source.Exec(`
		PRAGMA foreign_keys = ON;
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			external_id VARCHAR(40) NOT NULL,
			balance DECIMAL(12,2) NOT NULL DEFAULT (0.00),
			created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
			payload BLOB DEFAULT X'00',
			status TEXT NOT NULL DEFAULT 'active',
			label VARCHAR(40) NOT NULL DEFAULT 'unknown',
			CHECK (balance >= 0),
			CHECK (status IN ('active', 'paused'))
		);
		CREATE UNIQUE INDEX accounts_external_id_uq ON accounts(external_id);
		CREATE INDEX accounts_status_balance_idx ON accounts(status, balance DESC);
		INSERT INTO accounts(id, external_id, balance, created_at, payload, status, label)
		VALUES
			(1, 'alpha-?', 12.50, '2026-07-27T12:00:00.123Z', X'00FF', 'active', 'primary'),
			(2, 'beta-?', 0.00, '2026-07-27T12:00:01.000Z', NULL, 'paused', '');
		INSERT INTO accounts(id, external_id, balance, created_at, status, label)
		VALUES (50, 'deleted-high-water', 1.00, '2026-07-27T12:00:02.000Z', 'active', 'deleted');
		DELETE FROM accounts WHERE id = 50;

		CREATE TABLE account_events (
			account_id INTEGER NOT NULL,
			sequence_no INTEGER NOT NULL,
			note TEXT NOT NULL DEFAULT 'none',
			raw BLOB,
			PRIMARY KEY (sequence_no, account_id),
			UNIQUE (account_id, note),
			FOREIGN KEY (account_id) REFERENCES accounts(id) ON UPDATE CASCADE ON DELETE RESTRICT,
			CHECK (sequence_no > 0)
		);
		INSERT INTO account_events(account_id, sequence_no, note, raw) VALUES
			(1, 2, 'second', X'1020'),
			(1, 1, 'first', NULL),
			(2, 1, 'unicode-?', X'');
	`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	result, err := SQLiteToSQLite(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 2 || result.Rows != 5 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}

	source = openSQLiteSchemaTestDatabase(t, sourcePath)
	defer source.Close()
	target := openSQLiteSchemaTestDatabase(t, targetPath)
	defer target.Close()
	for _, tableName := range []string{"account_events", "accounts"} {
		sourceSchema, _, err := inspectTable(context.Background(), source, tableName)
		if err != nil {
			t.Fatal(err)
		}
		targetSchema, _, err := inspectTable(context.Background(), target, tableName)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(sourceSchema, targetSchema) {
			t.Fatalf("semantic schema mismatch for %s:\nsource: %#v\ntarget: %#v", tableName, sourceSchema, targetSchema)
		}
	}

	assertSQLiteRowsEqual(t, source, target, "accounts", `id`)
	assertSQLiteRowsEqual(t, source, target, "account_events", `sequence_no, account_id`)
	var sourceSequence, targetSequence int64
	if err := source.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'accounts'`).Scan(&sourceSequence); err != nil {
		t.Fatal(err)
	}
	if err := target.QueryRow(`SELECT seq FROM sqlite_sequence WHERE name = 'accounts'`).Scan(&targetSequence); err != nil {
		t.Fatal(err)
	}
	if sourceSequence != 50 || targetSequence != sourceSequence {
		t.Fatalf("AUTOINCREMENT sequence source=%d target=%d", sourceSequence, targetSequence)
	}
	resultInsert, err := target.Exec(`INSERT INTO accounts(external_id, balance, status) VALUES ('next', 1.25, 'active')`)
	if err != nil {
		t.Fatal(err)
	}
	nextID, err := resultInsert.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	var label string
	if err := target.QueryRow(`SELECT label FROM accounts WHERE id = ?`, nextID).Scan(&label); err != nil {
		t.Fatal(err)
	}
	if nextID != 51 || label != "unknown" {
		t.Fatalf("next AUTOINCREMENT/default = (%d, %q), want (51, unknown)", nextID, label)
	}
}

func TestSQLiteUpsertResumeCompletesInterruptedFinalization(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLiteSchemaTestDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
		CREATE INDEX users_payload_idx ON users(payload);
		CREATE UNIQUE INDEX users_payload_id_uq ON users(payload, id DESC);
		INSERT INTO users(id, payload) VALUES (1, 'one'), (50, 'high-water');
		DELETE FROM users WHERE id = 50;
	`); err != nil {
		t.Fatal(err)
	}
	sourceTable, _, err := inspectTable(context.Background(), source, "users")
	if err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	target := openSQLiteSchemaTestDatabase(t, targetPath)
	ddl, err := schema.CreateTable(schema.SQLite, sourceTable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(ddl); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`INSERT INTO users(id, payload) VALUES (1, 'one')`); err != nil {
		t.Fatal(err)
	}
	indexPlan, err := schema.CreateIndexes(schema.SQLite, sourceTable)
	if err != nil || len(indexPlan) != 2 {
		t.Fatalf("index plan = %#v, %v", indexPlan, err)
	}
	if _, err := target.Exec(indexPlan[0]); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	watermark := int64(1)
	result, err := SQLiteToSQLiteResumeWithProgress(context.Background(), config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "upsert"},
	}, nil, map[string]TableProgress{"users": {RowsDone: 1, IntegerWatermark: &watermark}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 1 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	source = openSQLiteSchemaTestDatabase(t, sourcePath)
	defer source.Close()
	target = openSQLiteSchemaTestDatabase(t, targetPath)
	defer target.Close()
	want, _, err := inspectTable(context.Background(), source, "users")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := inspectTable(context.Background(), target, "users")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resumed schema mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func TestSQLiteRejectsCanonicalSameDatabaseBeforeMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	createDatabase(t, sourcePath, `CREATE TABLE users (id INTEGER PRIMARY KEY); INSERT INTO users VALUES (1)`)
	aliasPath := directory + string(filepath.Separator) + "." + string(filepath.Separator) + "source.db"
	_, err := SQLiteToSQLite(context.Background(), config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: aliasPath},
		Migration: config.Migration{TargetMode: "drop_recreate"},
	})
	if err == nil {
		t.Fatal("expected canonical same-database rejection")
	}
	database := openSQLiteSchemaTestDatabase(t, sourcePath)
	defer database.Close()
	var count int
	if err := database.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("source mutated through aliased target path: %d rows", count)
	}
}

func TestSQLiteSchemaDiscoveryFailsClosedBeforeAnyTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLiteSchemaTestDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE a_valid (id INTEGER PRIMARY KEY, value TEXT);
		INSERT INTO a_valid VALUES (1, 'source');
		CREATE TABLE z_unsupported (id INTEGER PRIMARY KEY, value TEXT);
		CREATE INDEX z_expression_idx ON z_unsupported(lower(value));
	`); err != nil {
		t.Fatal(err)
	}
	source.Close()
	target := openSQLiteSchemaTestDatabase(t, targetPath)
	if _, err := target.Exec(`CREATE TABLE a_valid (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO a_valid VALUES (99, 'sentinel')`); err != nil {
		t.Fatal(err)
	}
	target.Close()

	_, err := SQLiteToSQLite(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
	})
	var policyError *schema.PolicyError
	if !errors.As(err, &policyError) {
		t.Fatalf("expected typed schema policy error, got %v", err)
	}
	target = openSQLiteSchemaTestDatabase(t, targetPath)
	defer target.Close()
	var id int
	var value string
	if err := target.QueryRow(`SELECT id, value FROM a_valid`).Scan(&id, &value); err != nil {
		t.Fatal(err)
	}
	if id != 99 || value != "sentinel" {
		t.Fatalf("target mutated before full schema validation: (%d, %q)", id, value)
	}
}

func TestSQLiteSchemaDiscoveryRejectsTriggersBeforeAnyTargetMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLiteSchemaTestDatabase(t, sourcePath)
	if _, err := source.Exec(`
		CREATE TABLE a_valid (id INTEGER PRIMARY KEY, value TEXT);
		INSERT INTO a_valid VALUES (1, 'source');
		CREATE TABLE z_triggered (id INTEGER PRIMARY KEY, value TEXT);
		CREATE TRIGGER z_second_trigger AFTER INSERT ON z_triggered
		BEGIN
			UPDATE z_triggered SET value = NEW.value WHERE id = NEW.id;
		END;
		CREATE TRIGGER z_first_trigger AFTER INSERT ON z_triggered
		BEGIN
			UPDATE z_triggered SET value = NEW.value WHERE id = NEW.id;
		END;
	`); err != nil {
		t.Fatal(err)
	}
	source.Close()
	target := openSQLiteSchemaTestDatabase(t, targetPath)
	if _, err := target.Exec(`CREATE TABLE a_valid (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO a_valid VALUES (99, 'sentinel')`); err != nil {
		t.Fatal(err)
	}
	target.Close()
	cfg := config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "upsert"},
	}

	_, firstErr := SQLiteToSQLite(context.Background(), cfg)
	var policyError *schema.PolicyError
	if !errors.As(firstErr, &policyError) {
		t.Fatalf("expected typed schema policy error, got %v", firstErr)
	}
	if policyError.Operation != "discover SQLite table trigger" || policyError.Type != "z_triggered.z_first_trigger" {
		t.Fatalf("unexpected trigger policy error: %#v", policyError)
	}
	const wantError = `schema policy: discover SQLite table trigger type "z_triggered.z_first_trigger" is unsupported for sqlite`
	if firstErr.Error() != wantError {
		t.Fatalf("unexpected trigger policy error text:\n got: %q\nwant: %q", firstErr, wantError)
	}
	_, secondErr := SQLiteToSQLite(context.Background(), cfg)
	if secondErr == nil || secondErr.Error() != firstErr.Error() {
		t.Fatalf("trigger policy error was not deterministic:\nfirst: %v\nsecond: %v", firstErr, secondErr)
	}

	target = openSQLiteSchemaTestDatabase(t, targetPath)
	defer target.Close()
	var id int
	var value string
	if err := target.QueryRow(`SELECT id, value FROM a_valid`).Scan(&id, &value); err != nil {
		t.Fatal(err)
	}
	if id != 99 || value != "sentinel" {
		t.Fatalf("target mutated before trigger validation: (%d, %q)", id, value)
	}
	var triggeredTableCount int
	if err := target.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name = 'z_triggered'`).Scan(&triggeredTableCount); err != nil {
		t.Fatal(err)
	}
	if triggeredTableCount != 0 {
		t.Fatal("triggered table was created before trigger validation failed")
	}
}

func TestSQLiteSchemaDiscoveryRejectsUnsupportedFeatures(t *testing.T) {
	tests := []struct {
		name      string
		statement string
	}{
		{name: "generated column", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT, normalized TEXT GENERATED ALWAYS AS (lower(value)) STORED)`},
		{name: "partial index", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX items_partial ON items(value) WHERE value IS NOT NULL`},
		{name: "expression index", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX items_expression ON items(lower(value))`},
		{name: "unsupported check expression", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT, CHECK (instr(value, 'x') > 0))`},
		{name: "typeless column", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value)`},
		{name: "column collation", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT COLLATE NOCASE)`},
		{name: "conflict algorithm", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT UNIQUE ON CONFLICT IGNORE)`},
		{name: "foreign key match", statement: `CREATE TABLE parents (id INTEGER PRIMARY KEY); CREATE TABLE items (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parents(id) MATCH FULL)`},
		{name: "named constraint", statement: `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT, CONSTRAINT value_check CHECK (length(value) > 0))`},
		{name: "descending primary key", statement: `CREATE TABLE items (id INTEGER, value TEXT, PRIMARY KEY (id DESC))`},
		{name: "collated primary key", statement: `CREATE TABLE items (id TEXT, value TEXT, PRIMARY KEY (id COLLATE NOCASE))`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.db")
			database := openSQLiteSchemaTestDatabase(t, path)
			defer database.Close()
			if _, err := database.Exec(test.statement); err != nil {
				t.Fatal(err)
			}
			_, _, err := inspectTable(context.Background(), database, "items")
			var policyError *schema.PolicyError
			if !errors.As(err, &policyError) {
				t.Fatalf("expected typed policy error, got %v", err)
			}
		})
	}
}

func TestSQLiteUpsertPreservesTargetSupersetAndRejectsIncompatibleShape(t *testing.T) {
	t.Run("target row superset", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath, targetPath := filepath.Join(directory, "source.db"), filepath.Join(directory, "target.db")
		createDatabase(t, sourcePath, `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL); INSERT INTO items VALUES (1, 'new')`)
		createDatabase(t, targetPath, `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL); INSERT INTO items VALUES (1, 'old'), (2, 'target-only')`)
		result, err := SQLiteToSQLite(context.Background(), config.Config{
			Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
			Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
			Migration: config.Migration{TargetMode: "upsert"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if result.Rows != 1 || countRowsForTest(t, targetPath, "items") != 2 {
			t.Fatalf("result=%+v target rows=%d", result, countRowsForTest(t, targetPath, "items"))
		}
	})

	t.Run("incompatible target", func(t *testing.T) {
		directory := t.TempDir()
		sourcePath, targetPath := filepath.Join(directory, "source.db"), filepath.Join(directory, "target.db")
		createDatabase(t, sourcePath, `CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL); INSERT INTO items VALUES (1, 'new')`)
		createDatabase(t, targetPath, `CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB NOT NULL); INSERT INTO items VALUES (1, X'6F6C64')`)
		_, err := SQLiteToSQLite(context.Background(), config.Config{
			Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
			Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
			Migration: config.Migration{TargetMode: "upsert"},
		})
		var policyError *schema.PolicyError
		if !errors.As(err, &policyError) {
			t.Fatalf("expected typed compatibility error, got %v", err)
		}
		if countRowsForTest(t, targetPath, "items") != 1 {
			t.Fatal("incompatible upsert mutated target")
		}
	})
}

func openSQLiteSchemaTestDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func assertSQLiteRowsEqual(t *testing.T, source, target *sql.DB, table, order string) {
	t.Helper()
	read := func(database *sql.DB) [][]any {
		rows, err := database.Query(`SELECT * FROM ` + quote(table) + ` ORDER BY ` + order)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		var result [][]any
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for index := range values {
				pointers[index] = &values[index]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			for index, value := range values {
				if bytes, ok := value.([]byte); ok {
					cloned := make([]byte, len(bytes))
					copy(cloned, bytes)
					values[index] = cloned
				}
			}
			result = append(result, values)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return result
	}
	sourceRows, targetRows := read(source), read(target)
	if !reflect.DeepEqual(sourceRows, targetRows) {
		t.Fatalf("row mismatch for %s:\nsource: %#v\ntarget: %#v", table, sourceRows, targetRows)
	}
}
