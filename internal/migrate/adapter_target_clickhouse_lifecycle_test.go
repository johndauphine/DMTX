package migrate

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestClickHousePrepareDropsEveryTableBeforeCreatingAny(t *testing.T) {
	connection := &clickHouseLifecycleTestConnection{}
	database := sql.OpenDB(&clickHouseLifecycleTestConnector{
		connection: connection,
	})
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lifecycle test database: %v", err)
		}
	})
	adapter := &clickHouseTargetAdapter{
		database:                database,
		destructiveAcknowledged: true,
	}
	tables := []schema.Table{
		clickHouseLifecycleTable("later"),
		clickHouseLifecycleTable("first"),
	}
	if err := adapter.PrepareTables(
		context.Background(),
		tables,
		"drop_recreate",
	); err != nil {
		t.Fatal(err)
	}
	if len(connection.statements) != 4 {
		t.Fatalf("statements = %#v", connection.statements)
	}
	for index := 0; index < 2; index++ {
		if !strings.HasPrefix(
			connection.statements[index],
			"DROP TABLE",
		) {
			t.Fatalf(
				"statement %d ran before every drop: %s",
				index,
				connection.statements[index],
			)
		}
	}
	for index := 2; index < 4; index++ {
		if !strings.HasPrefix(
			connection.statements[index],
			"CREATE TABLE",
		) {
			t.Fatalf(
				"statement %d is not a create: %s",
				index,
				connection.statements[index],
			)
		}
	}
	if !strings.Contains(connection.statements[0], `"first"`) ||
		!strings.Contains(connection.statements[1], `"later"`) ||
		!strings.Contains(connection.statements[2], `"first"`) ||
		!strings.Contains(connection.statements[3], `"later"`) {
		t.Fatalf(
			"preparation order is not deterministic: %#v",
			connection.statements,
		)
	}
}

func TestClickHouseDestructivePreflightRequiresAcknowledgementForAnyExistingTarget(
	t *testing.T,
) {
	connection := &clickHouseLifecycleTestConnection{
		existing: map[string]uint64{
			"analytics.existing": 1,
		},
	}
	database := sql.OpenDB(&clickHouseLifecycleTestConnector{
		connection: connection,
	})
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lifecycle test database: %v", err)
		}
	})
	adapter := &clickHouseTargetAdapter{database: database}
	tables := []schema.Table{clickHouseLifecycleTable("existing")}
	err := adapter.PreflightDestructive(
		context.Background(),
		tables,
		config.Migration{TargetMode: "drop_recreate"},
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) ||
		!strings.Contains(err.Error(), "--acknowledge-destructive") {
		t.Fatalf("unacknowledged existing target error = %v", err)
	}
	if adapter.destructiveAcknowledged {
		t.Fatal("failed preflight retained destructive acknowledgement")
	}
	if len(connection.statements) != 0 {
		t.Fatalf("destructive preflight mutated target: %#v", connection.statements)
	}

	if err := adapter.PreflightDestructive(
		context.Background(),
		tables,
		config.Migration{
			TargetMode:              "drop_recreate",
			DestructiveAcknowledged: true,
		},
	); err != nil {
		t.Fatal(err)
	}
	if !adapter.destructiveAcknowledged {
		t.Fatal("successful acknowledged preflight did not arm preparation")
	}
}

func TestClickHousePrepareRechecksUnacknowledgedNamesAfterCheckpoint(
	t *testing.T,
) {
	connection := &clickHouseLifecycleTestConnection{
		existing: make(map[string]uint64),
	}
	database := sql.OpenDB(&clickHouseLifecycleTestConnector{
		connection: connection,
	})
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lifecycle test database: %v", err)
		}
	})
	adapter := &clickHouseTargetAdapter{database: database}
	tables := []schema.Table{
		clickHouseLifecycleTable("later"),
		clickHouseLifecycleTable("first"),
	}
	if err := adapter.PreflightDestructive(
		context.Background(),
		tables,
		config.Migration{TargetMode: "drop_recreate"},
	); err != nil {
		t.Fatal(err)
	}

	// Simulate a target created and populated by the table-set checkpoint
	// after read-only preflight.
	connection.existing["analytics.first"] = 1
	err := adapter.PrepareTables(
		context.Background(),
		tables,
		"drop_recreate",
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf("post-checkpoint existing target error = %v", err)
	}
	if len(connection.statements) != 0 {
		t.Fatalf("unacknowledged preparation executed DDL: %#v", connection.statements)
	}
}

func TestClickHouseUnacknowledgedPrepareCreatesWithoutDrop(t *testing.T) {
	connection := &clickHouseLifecycleTestConnection{
		existing: make(map[string]uint64),
	}
	database := sql.OpenDB(&clickHouseLifecycleTestConnector{
		connection: connection,
	})
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lifecycle test database: %v", err)
		}
	})
	adapter := &clickHouseTargetAdapter{database: database}
	tables := []schema.Table{
		clickHouseLifecycleTable("later"),
		clickHouseLifecycleTable("first"),
	}
	if err := adapter.PreflightDestructive(
		context.Background(),
		tables,
		config.Migration{TargetMode: "drop_recreate"},
	); err != nil {
		t.Fatal(err)
	}
	if err := adapter.PrepareTables(
		context.Background(),
		tables,
		"drop_recreate",
	); err != nil {
		t.Fatal(err)
	}
	if len(connection.statements) != 2 {
		t.Fatalf("statements = %#v", connection.statements)
	}
	for _, statement := range connection.statements {
		if !strings.HasPrefix(statement, "CREATE TABLE") {
			t.Fatalf("unacknowledged preparation executed %q", statement)
		}
	}
}

func TestClickHousePrepareFailureNamesRebuildRecoveryPath(t *testing.T) {
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run(fmt.Sprintf("statement_%d", failAt), func(t *testing.T) {
			forced := errors.New("forced DDL failure")
			connection := &clickHouseLifecycleTestConnection{
				failAt: failAt,
				fail:   forced,
			}
			database := sql.OpenDB(&clickHouseLifecycleTestConnector{
				connection: connection,
			})
			t.Cleanup(func() {
				if err := database.Close(); err != nil {
					t.Errorf("close lifecycle test database: %v", err)
				}
			})
			adapter := &clickHouseTargetAdapter{
				database:                database,
				destructiveAcknowledged: true,
			}
			err := adapter.PrepareTables(
				context.Background(),
				[]schema.Table{
					clickHouseLifecycleTable("later"),
					clickHouseLifecycleTable("first"),
				},
				"drop_recreate",
			)
			if !errors.Is(err, forced) ||
				!strings.Contains(
					err.Error(),
					"target preparation may be partial",
				) ||
				!strings.Contains(
					err.Error(),
					"rerun the full migration in drop_recreate mode",
				) ||
				!strings.Contains(
					err.Error(),
					"rebuild all selected targets",
				) {
				t.Fatalf("partial preparation error = %v", err)
			}
			if len(connection.statements) != failAt {
				t.Fatalf(
					"partial preparation statements = %#v",
					connection.statements,
				)
			}
			for index, statement := range connection.statements {
				if index < 2 &&
					!strings.HasPrefix(statement, "DROP TABLE") {
					t.Fatalf(
						"statement %d ran before every drop: %s",
						index,
						statement,
					)
				}
				if index >= 2 &&
					!strings.HasPrefix(statement, "CREATE TABLE") {
					t.Fatalf(
						"statement %d is not a create: %s",
						index,
						statement,
					)
				}
			}
		})
	}
}

func TestClickHousePreparePlansEveryStatementBeforeMutation(t *testing.T) {
	connection := &clickHouseLifecycleTestConnection{}
	database := sql.OpenDB(&clickHouseLifecycleTestConnector{
		connection: connection,
	})
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lifecycle test database: %v", err)
		}
	})
	invalid := clickHouseLifecycleTable("invalid")
	invalid.Columns[0].Type = "unsupported"
	adapter := &clickHouseTargetAdapter{
		database:                database,
		destructiveAcknowledged: true,
	}
	err := adapter.PrepareTables(
		context.Background(),
		[]schema.Table{
			clickHouseLifecycleTable("valid"),
			invalid,
		},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "create") {
		t.Fatalf("planning error = %v", err)
	}
	if len(connection.statements) != 0 {
		t.Fatalf(
			"planning failure executed statements: %#v",
			connection.statements,
		)
	}
}

func clickHouseLifecycleTable(name string) schema.Table {
	return schema.Table{
		Schema:            "analytics",
		Name:              name,
		ClickHouseOrderBy: []string{"id"},
		Columns: []schema.Column{{
			Name: "id",
			Type: "bigint",
		}},
	}
}

type clickHouseLifecycleTestConnector struct {
	connection *clickHouseLifecycleTestConnection
}

func (connector *clickHouseLifecycleTestConnector) Connect(
	context.Context,
) (sqldriver.Conn, error) {
	return connector.connection, nil
}

func (*clickHouseLifecycleTestConnector) Driver() sqldriver.Driver {
	return clickHouseLifecycleTestDriver{}
}

type clickHouseLifecycleTestDriver struct{}

func (clickHouseLifecycleTestDriver) Open(
	string,
) (sqldriver.Conn, error) {
	return nil, fmt.Errorf("direct lifecycle driver open is unsupported")
}

type clickHouseLifecycleTestConnection struct {
	statements []string
	existing   map[string]uint64
	failAt     int
	fail       error
}

func (*clickHouseLifecycleTestConnection) Prepare(
	string,
) (sqldriver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are unsupported")
}

func (*clickHouseLifecycleTestConnection) Close() error {
	return nil
}

func (*clickHouseLifecycleTestConnection) Begin() (sqldriver.Tx, error) {
	return nil, fmt.Errorf("transactions are unsupported")
}

func (connection *clickHouseLifecycleTestConnection) ExecContext(
	_ context.Context,
	query string,
	_ []sqldriver.NamedValue,
) (sqldriver.Result, error) {
	connection.statements = append(connection.statements, query)
	if connection.failAt != 0 &&
		len(connection.statements) == connection.failAt {
		return nil, connection.fail
	}
	return sqldriver.RowsAffected(0), nil
}

func (connection *clickHouseLifecycleTestConnection) QueryContext(
	_ context.Context,
	query string,
	arguments []sqldriver.NamedValue,
) (sqldriver.Rows, error) {
	if !strings.Contains(query, "FROM system.tables") ||
		len(arguments) != 2 {
		return nil, fmt.Errorf("unexpected lifecycle query %q", query)
	}
	database, databaseOK := arguments[0].Value.(string)
	table, tableOK := arguments[1].Value.(string)
	if !databaseOK || !tableOK {
		return nil, fmt.Errorf(
			"unexpected lifecycle query arguments %#v",
			arguments,
		)
	}
	return &clickHouseLifecycleTestRows{
		value: connection.existing[database+"."+table],
	}, nil
}

type clickHouseLifecycleTestRows struct {
	value uint64
	read  bool
}

func (*clickHouseLifecycleTestRows) Columns() []string {
	return []string{"count()"}
}

func (*clickHouseLifecycleTestRows) Close() error {
	return nil
}

func (rows *clickHouseLifecycleTestRows) Next(
	values []sqldriver.Value,
) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	values[0] = int64(rows.value)
	return nil
}
