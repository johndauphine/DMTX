package migrate

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLiteTargetOpenEnablesForeignKeyEnforcement(t *testing.T) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	var enabled int
	if err := adapter.database.QueryRow(
		"PRAGMA foreign_keys",
	).Scan(&enabled); err != nil {
		t.Fatalf("inspect foreign_keys: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("PRAGMA foreign_keys = %d, want 1", enabled)
	}
}

func TestSQLiteTargetClosesDestructivePreflightRace(t *testing.T) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	table := sqliteLifecycleTable("events")
	execSQLiteLifecycleTest(
		t,
		adapter.database,
		`CREATE TABLE "events" (
			"id" INTEGER NOT NULL,
			PRIMARY KEY ("id")
		)`,
	)
	if err := adapter.PreflightTables(
		context.Background(),
		[]schema.Table{table},
		"drop_recreate",
	); err != nil {
		t.Fatalf("PreflightTables: %v", err)
	}
	if err := adapter.PreflightDestructive(
		context.Background(),
		[]schema.Table{table},
		config.Migration{TargetMode: "drop_recreate"},
	); err != nil {
		t.Fatalf("PreflightDestructive: %v", err)
	}

	execSQLiteLifecycleTest(
		t,
		adapter.database,
		`INSERT INTO "events" ("id") VALUES (41)`,
	)
	err := adapter.PrepareTables(
		context.Background(),
		[]schema.Table{table},
		"drop_recreate",
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"PrepareTables error = %v, want destructive acknowledgement",
			err,
		)
	}
	if !strings.Contains(err.Error(), "rolled back without target changes") {
		t.Fatalf("PrepareTables error lacks rollback proof: %v", err)
	}
	var id int
	if err := adapter.database.QueryRow(
		`SELECT "id" FROM "events"`,
	).Scan(&id); err != nil {
		t.Fatalf("read preserved row: %v", err)
	}
	if id != 41 {
		t.Fatalf("preserved id = %d, want 41", id)
	}
}

func TestSQLiteTargetPreparationRollbackIsAtomic(t *testing.T) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	for _, name := range []string{"alpha", "oversized"} {
		execSQLiteLifecycleTest(
			t,
			adapter.database,
			`CREATE TABLE `+quote(name)+
				` ("id" INTEGER NOT NULL, PRIMARY KEY ("id"))`,
		)
		execSQLiteLifecycleTest(
			t,
			adapter.database,
			`INSERT INTO `+quote(name)+` ("id") VALUES (7)`,
		)
	}

	oversized := schema.Table{Name: "oversized"}
	for index := 0; index < 2001; index++ {
		oversized.Columns = append(
			oversized.Columns,
			schema.Column{
				Name: fmt.Sprintf("column_%04d", index),
				Type: "integer",
				DeclaredType: &schema.DeclaredType{
					Base: "integer",
				},
			},
		)
	}
	tables := []schema.Table{
		oversized,
		sqliteLifecycleTable("alpha"),
	}
	if err := adapter.PreflightTables(
		context.Background(),
		tables,
		"drop_recreate",
	); err != nil {
		t.Fatalf("PreflightTables: %v", err)
	}
	if err := adapter.PreflightDestructive(
		context.Background(),
		tables,
		config.Migration{
			TargetMode:              "drop_recreate",
			DestructiveAcknowledged: true,
		},
	); err != nil {
		t.Fatalf("PreflightDestructive: %v", err)
	}
	err := adapter.PrepareTables(
		context.Background(),
		tables,
		"drop_recreate",
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"rolled back without target changes",
	) {
		t.Fatalf("PrepareTables error = %v, want atomic rollback", err)
	}
	for _, name := range []string{"alpha", "oversized"} {
		var id int
		if err := adapter.database.QueryRow(
			`SELECT "id" FROM ` + quote(name),
		).Scan(&id); err != nil {
			t.Fatalf("read preserved %s: %v", name, err)
		}
		if id != 7 {
			t.Fatalf("%s preserved id = %d, want 7", name, id)
		}
	}
}

func TestSQLiteTargetPreparationDropsAllTablesBeforeAnyCreate(
	t *testing.T,
) {
	connection := &sqliteLifecycleRecordingConnection{}
	database := sql.OpenDB(&sqliteLifecycleRecordingConnector{
		connection: connection,
	})
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close recording database: %v", err)
		}
	})
	adapter := &sqliteTargetAdapter{
		database:                database,
		destructiveAcknowledged: true,
	}
	if err := adapter.PrepareTables(
		context.Background(),
		[]schema.Table{
			sqliteLifecycleTable("later"),
			sqliteLifecycleTable("first"),
		},
		"drop_recreate",
	); err != nil {
		t.Fatalf("PrepareTables: %v", err)
	}
	ddl := make([]string, 0, 4)
	for _, statement := range connection.statements {
		if strings.HasPrefix(statement, "DROP TABLE") ||
			strings.HasPrefix(statement, "CREATE TABLE") {
			ddl = append(ddl, statement)
		}
	}
	if len(ddl) != 4 {
		t.Fatalf("DDL statements = %#v", ddl)
	}
	if !strings.HasPrefix(ddl[0], "DROP TABLE") ||
		!strings.Contains(ddl[0], `"first"`) ||
		!strings.HasPrefix(ddl[1], "DROP TABLE") ||
		!strings.Contains(ddl[1], `"later"`) ||
		!strings.HasPrefix(ddl[2], "CREATE TABLE") ||
		!strings.Contains(ddl[2], `"first"`) ||
		!strings.HasPrefix(ddl[3], "CREATE TABLE") ||
		!strings.Contains(ddl[3], `"later"`) {
		t.Fatalf(
			"preparation did not run deterministic two-phase DDL: %#v",
			ddl,
		)
	}
}

func TestSQLiteTargetPreparationPreservesForeignKeyEnforcement(
	t *testing.T,
) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	execSQLiteLifecycleTest(
		t,
		adapter.database,
		`CREATE TABLE "parents" (
			"id" INTEGER NOT NULL,
			PRIMARY KEY ("id")
		);
		CREATE TABLE "children" (
			"id" INTEGER NOT NULL,
			"parent_id" INTEGER NOT NULL,
			PRIMARY KEY ("id"),
			FOREIGN KEY ("parent_id") REFERENCES "parents" ("id")
		);
		INSERT INTO "parents" VALUES (1);
		INSERT INTO "children" VALUES (1, 1);`,
	)
	parents := sqliteLifecycleTable("parents")
	children := sqliteLifecycleTable("children")
	children.Columns = append(
		children.Columns,
		schema.Column{
			Name:     "parent_id",
			Type:     "integer",
			Nullable: false,
			DeclaredType: &schema.DeclaredType{
				Base: "integer",
			},
		},
	)
	children.ForeignKeys = []schema.ForeignKey{{
		Columns:           []string{"parent_id"},
		ReferencedTable:   "parents",
		ReferencedColumns: []string{"id"},
	}}
	tables := []schema.Table{parents, children}
	if err := adapter.PreflightTables(
		context.Background(),
		tables,
		"drop_recreate",
	); err != nil {
		t.Fatalf("PreflightTables: %v", err)
	}
	if err := adapter.PreflightDestructive(
		context.Background(),
		tables,
		config.Migration{
			TargetMode:              "drop_recreate",
			DestructiveAcknowledged: true,
		},
	); err != nil {
		t.Fatalf("PreflightDestructive: %v", err)
	}
	if err := adapter.PrepareTables(
		context.Background(),
		tables,
		"drop_recreate",
	); err != nil {
		t.Fatalf("PrepareTables: %v", err)
	}
	if err := preflightSQLiteForeignKeyIntegrity(
		context.Background(),
		adapter.database,
		"",
	); err != nil {
		t.Fatalf("foreign_key_check: %v", err)
	}
	var enabled int
	if err := adapter.database.QueryRow(
		"PRAGMA foreign_keys",
	).Scan(&enabled); err != nil {
		t.Fatalf("inspect foreign_keys: %v", err)
	}
	if enabled != 1 {
		t.Fatalf("PRAGMA foreign_keys = %d, want 1", enabled)
	}
}

func TestSQLiteTargetRejectsExternalForeignKeyDependencyBeforeMutation(
	t *testing.T,
) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	execSQLiteLifecycleTest(
		t,
		adapter.database,
		`CREATE TABLE "parents" (
			"id" INTEGER NOT NULL,
			PRIMARY KEY ("id")
		);
		CREATE TABLE "unselected_children" (
			"id" INTEGER NOT NULL,
			"parent_id" INTEGER NOT NULL,
			PRIMARY KEY ("id"),
			FOREIGN KEY ("parent_id") REFERENCES "parents" ("id")
		);
		INSERT INTO "parents" VALUES (5);
		INSERT INTO "unselected_children" VALUES (8, 5);`,
	)
	err := adapter.PreflightTables(
		context.Background(),
		[]schema.Table{sqliteLifecycleTable("parents")},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"external foreign-key dependency",
	) {
		t.Fatalf("PreflightTables error = %v", err)
	}
	var id int
	if err := adapter.database.QueryRow(
		`SELECT "id" FROM "parents"`,
	).Scan(&id); err != nil {
		t.Fatalf("read preserved parent: %v", err)
	}
	if id != 5 {
		t.Fatalf("preserved parent id = %d, want 5", id)
	}
}

func TestSQLiteTargetUpsertRequiresFullRetainedSchemaCompatibility(
	t *testing.T,
) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	check, err := schema.ParseSQLiteCheckExpression(`"parent_id" > 0`)
	if err != nil {
		t.Fatalf("parse CHECK: %v", err)
	}
	defaultNote, err := schema.ParseSQLiteDefault(`'kept'`)
	if err != nil {
		t.Fatalf("parse default: %v", err)
	}
	parents := sqliteLifecycleTable("parents")
	children := sqliteLifecycleTable("children")
	children.Columns = append(
		children.Columns,
		schema.Column{
			Name:     "parent_id",
			Type:     "integer",
			Nullable: false,
			DeclaredType: &schema.DeclaredType{
				Base: "integer",
			},
		},
		schema.Column{
			Name:     "note",
			Type:     "text",
			Nullable: false,
			DeclaredType: &schema.DeclaredType{
				Base: "text",
			},
			Default: defaultNote,
		},
	)
	children.Indexes = []schema.Index{{
		Name: "children_parent_id",
		Columns: []schema.IndexColumn{{
			Name: "parent_id",
		}},
	}}
	children.ForeignKeys = []schema.ForeignKey{{
		Columns:           []string{"parent_id"},
		ReferencedTable:   "parents",
		ReferencedColumns: []string{"id"},
	}}
	children.Checks = []schema.CheckConstraint{{
		Expression: check,
	}}
	for _, table := range []schema.Table{parents, children} {
		statement, err := schema.CreateTable(schema.SQLite, table)
		if err != nil {
			t.Fatalf("plan %s: %v", table.Name, err)
		}
		execSQLiteLifecycleTest(t, adapter.database, statement)
		indexes, err := schema.CreateIndexes(schema.SQLite, table)
		if err != nil {
			t.Fatalf("plan %s indexes: %v", table.Name, err)
		}
		for _, statement := range indexes {
			execSQLiteLifecycleTest(t, adapter.database, statement)
		}
	}
	if err := adapter.PreflightTables(
		context.Background(),
		[]schema.Table{parents, children},
		"upsert",
	); err != nil {
		t.Fatalf("compatible retained preflight: %v", err)
	}

	incompatible := children
	incompatible.Indexes = nil
	err = adapter.PreflightTables(
		context.Background(),
		[]schema.Table{parents, incompatible},
		"upsert",
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"retained SQLite indexes",
	) {
		t.Fatalf("incompatible retained preflight error = %v", err)
	}

	execSQLiteLifecycleTest(
		t,
		adapter.database,
		`CREATE TRIGGER "children_side_effect"
		AFTER INSERT ON "children"
		BEGIN
			UPDATE "children" SET "note" = 'changed'
			WHERE "id" = NEW."id";
		END`,
	)
	err = adapter.PreflightTables(
		context.Background(),
		[]schema.Table{parents, children},
		"upsert",
	)
	if err == nil || !strings.Contains(err.Error(), "trigger") {
		t.Fatalf("trigger retained preflight error = %v", err)
	}
}

func TestSQLiteTargetPreparationPlansAllStatementsBeforeMutation(
	t *testing.T,
) {
	adapter := openSQLiteLifecycleTestAdapter(t)
	execSQLiteLifecycleTest(
		t,
		adapter.database,
		`CREATE TABLE "valid" (
			"id" INTEGER NOT NULL,
			PRIMARY KEY ("id")
		);
		INSERT INTO "valid" VALUES (9);`,
	)
	invalid := sqliteLifecycleTable("invalid")
	invalid.Columns[0].Type = "unsupported"
	invalid.Columns[0].DeclaredType = nil
	err := adapter.PrepareTables(
		context.Background(),
		[]schema.Table{
			sqliteLifecycleTable("valid"),
			invalid,
		},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "create") {
		t.Fatalf("PrepareTables error = %v", err)
	}
	var id int
	if err := adapter.database.QueryRow(
		`SELECT "id" FROM "valid"`,
	).Scan(&id); err != nil {
		t.Fatalf("read preserved row: %v", err)
	}
	if id != 9 {
		t.Fatalf("preserved id = %d, want 9", id)
	}
}

func openSQLiteLifecycleTestAdapter(
	t *testing.T,
) *sqliteTargetAdapter {
	t.Helper()
	target, err := openSQLiteTargetAdapter(
		context.Background(),
		config.Endpoint{
			Type:     "sqlite",
			Database: filepath.Join(t.TempDir(), "target.db"),
		},
	)
	if err != nil {
		t.Fatalf("open SQLite target adapter: %v", err)
	}
	adapter, ok := target.(*sqliteTargetAdapter)
	if !ok {
		t.Fatalf("target adapter type = %T", target)
	}
	t.Cleanup(func() {
		if err := adapter.Close(); err != nil {
			t.Errorf("close SQLite target adapter: %v", err)
		}
	})
	return adapter
}

func sqliteLifecycleTable(name string) schema.Table {
	return schema.Table{
		Name: name,
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "integer",
			Nullable:           false,
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType: &schema.DeclaredType{
				Base: "integer",
			},
		}},
	}
}

func execSQLiteLifecycleTest(
	t *testing.T,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	if _, err := database.Exec(statement); err != nil {
		t.Fatalf("execute SQLite fixture: %v\n%s", err, statement)
	}
}

type sqliteLifecycleRecordingConnector struct {
	connection *sqliteLifecycleRecordingConnection
}

func (connector *sqliteLifecycleRecordingConnector) Connect(
	context.Context,
) (sqldriver.Conn, error) {
	return connector.connection, nil
}

func (*sqliteLifecycleRecordingConnector) Driver() sqldriver.Driver {
	return sqliteLifecycleRecordingDriver{}
}

type sqliteLifecycleRecordingDriver struct{}

func (sqliteLifecycleRecordingDriver) Open(
	string,
) (sqldriver.Conn, error) {
	return nil, fmt.Errorf("direct recording driver open is unsupported")
}

type sqliteLifecycleRecordingConnection struct {
	statements []string
}

func (*sqliteLifecycleRecordingConnection) Prepare(
	string,
) (sqldriver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are unsupported")
}

func (*sqliteLifecycleRecordingConnection) Close() error {
	return nil
}

func (*sqliteLifecycleRecordingConnection) Begin() (sqldriver.Tx, error) {
	return nil, fmt.Errorf("driver transactions are unsupported")
}

func (connection *sqliteLifecycleRecordingConnection) ExecContext(
	_ context.Context,
	query string,
	_ []sqldriver.NamedValue,
) (sqldriver.Result, error) {
	connection.statements = append(connection.statements, query)
	return sqldriver.RowsAffected(0), nil
}

func (*sqliteLifecycleRecordingConnection) QueryContext(
	_ context.Context,
	query string,
	_ []sqldriver.NamedValue,
) (sqldriver.Rows, error) {
	if strings.TrimSpace(query) == "PRAGMA foreign_keys" {
		return &sqliteLifecycleRecordingRows{
			columns: []string{"foreign_keys"},
			values:  [][]sqldriver.Value{{int64(1)}},
		}, nil
	}
	switch {
	case strings.Contains(query, "FROM sqlite_schema"),
		strings.HasPrefix(
			strings.TrimSpace(query),
			"PRAGMA foreign_key_check",
		):
		return &sqliteLifecycleRecordingRows{
			columns: []string{"empty"},
		}, nil
	default:
		return nil, fmt.Errorf(
			"unexpected recording query %q",
			query,
		)
	}
}

type sqliteLifecycleRecordingRows struct {
	columns []string
	values  [][]sqldriver.Value
	index   int
}

func (rows *sqliteLifecycleRecordingRows) Columns() []string {
	return rows.columns
}

func (*sqliteLifecycleRecordingRows) Close() error {
	return nil
}

func (rows *sqliteLifecycleRecordingRows) Next(
	dest []sqldriver.Value,
) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(dest, rows.values[rows.index])
	rows.index++
	return nil
}
