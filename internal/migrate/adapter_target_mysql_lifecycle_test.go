package migrate

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLTargetDestructivePreflightRequiresAcknowledgement(
	t *testing.T,
) {
	connection := &mysqlTargetLifecycleTestConnection{
		relations: map[string]string{
			"target\x00events": "BASE TABLE",
		},
		populated: map[string]bool{
			"target\x00events": true,
		},
	}
	adapter := openMySQLTargetLifecycleTestAdapter(t, connection)
	table := mysqlTargetLifecycleTestTable("events")
	err := adapter.PreflightDestructive(
		context.Background(),
		[]schema.Table{table},
		config.Migration{TargetMode: "drop_recreate"},
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) ||
		!strings.Contains(err.Error(), "--acknowledge-destructive") {
		t.Fatalf("destructive preflight error = %v", err)
	}
	if adapter.destructiveAcknowledged {
		t.Fatal("unacknowledged preflight retained acknowledgement state")
	}

	if err := adapter.PreflightDestructive(
		context.Background(),
		[]schema.Table{table},
		config.Migration{
			TargetMode:              "drop_recreate",
			DestructiveAcknowledged: true,
		},
	); err != nil {
		t.Fatalf("acknowledged destructive preflight: %v", err)
	}
	if !adapter.destructiveAcknowledged {
		t.Fatal("acknowledged preflight did not retain acknowledgement state")
	}
}

func TestMySQLTargetPreparationClosesPopulatedTableRace(t *testing.T) {
	connection := &mysqlTargetLifecycleTestConnection{
		relations: map[string]string{
			"target\x00events": "BASE TABLE",
		},
		populated:      map[string]bool{},
		populateOnLock: "target\x00events",
	}
	adapter := openMySQLTargetLifecycleTestAdapter(t, connection)
	err := prepareMySQLTargets(
		context.Background(),
		adapter.database,
		[]schema.Table{mysqlTargetLifecycleTestTable("events")},
		engine.MySQLServerFlavorOracle80,
		false,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"preparation race error = %v, want destructive acknowledgement",
			err,
		)
	}
	if !connection.sawLock {
		t.Fatal("preparation did not acquire a WRITE lock")
	}
	for _, statement := range connection.statements {
		if strings.HasPrefix(statement, "DROP TABLE") ||
			strings.HasPrefix(statement, "CREATE TABLE") {
			t.Fatalf(
				"populated-table race reached target DDL: %s",
				statement,
			)
		}
	}
	if connection.locked {
		t.Fatal("preparation error left test connection locked")
	}
}

func TestMySQLTargetPreparationUsesOneLockedDropBeforeCreates(
	t *testing.T,
) {
	connection := &mysqlTargetLifecycleTestConnection{
		relations: map[string]string{
			"target\x00first": "BASE TABLE",
			"target\x00later": "BASE TABLE",
		},
		populated: map[string]bool{},
	}
	adapter := openMySQLTargetLifecycleTestAdapter(t, connection)
	if err := prepareMySQLTargets(
		context.Background(),
		adapter.database,
		[]schema.Table{
			mysqlTargetLifecycleTestTable("later"),
			mysqlTargetLifecycleTestTable("first"),
		},
		engine.MySQLServerFlavorOracle80,
		true,
	); err != nil {
		t.Fatalf("prepare targets: %v", err)
	}
	var ddl []string
	for _, statement := range connection.statements {
		if strings.HasPrefix(statement, "DROP TABLE") ||
			strings.HasPrefix(statement, "CREATE TABLE") {
			ddl = append(ddl, statement)
		}
	}
	if len(ddl) != 3 {
		t.Fatalf("target DDL = %#v", ddl)
	}
	if !strings.HasPrefix(ddl[0], "DROP TABLE") ||
		!strings.Contains(ddl[0], "`target`.`first`") ||
		!strings.Contains(ddl[0], "`target`.`later`") {
		t.Fatalf("first DDL is not one deterministic multi-table drop: %s", ddl[0])
	}
	if !strings.HasPrefix(ddl[1], "CREATE TABLE") ||
		!strings.Contains(ddl[1], "`target`.`first`") ||
		!strings.HasPrefix(ddl[2], "CREATE TABLE") ||
		!strings.Contains(ddl[2], "`target`.`later`") {
		t.Fatalf("creates are not deterministically ordered after drop: %#v", ddl)
	}
	if !connection.dropWhileLocked {
		t.Fatal("multi-table DROP did not execute while WRITE locks were held")
	}
}

func TestMySQLTargetPreparationFailureNamesRebuildRecovery(t *testing.T) {
	forced := errors.New("forced create failure")
	connection := &mysqlTargetLifecycleTestConnection{
		relations: map[string]string{
			"target\x00first": "BASE TABLE",
			"target\x00later": "BASE TABLE",
		},
		populated:       map[string]bool{},
		failCreateCause: forced,
	}
	adapter := openMySQLTargetLifecycleTestAdapter(t, connection)
	err := prepareMySQLTargets(
		context.Background(),
		adapter.database,
		[]schema.Table{
			mysqlTargetLifecycleTestTable("later"),
			mysqlTargetLifecycleTestTable("first"),
		},
		engine.MySQLServerFlavorOracle80,
		true,
	)
	if !errors.Is(err, forced) ||
		!strings.Contains(
			err.Error(),
			"rerunning drop_recreate mode is the recovery path",
		) {
		t.Fatalf("partial preparation error = %v", err)
	}
	var dropCount, createCount int
	for _, statement := range connection.statements {
		if strings.HasPrefix(statement, "DROP TABLE") {
			dropCount++
		}
		if strings.HasPrefix(statement, "CREATE TABLE") {
			createCount++
		}
	}
	if dropCount != 1 || createCount != 1 {
		t.Fatalf(
			"partial preparation DDL counts = drop %d create %d",
			dropCount,
			createCount,
		)
	}
}

func TestMySQLTargetUpsertPrepareRepeatsRetainedPreflight(t *testing.T) {
	connection := &mysqlTargetLifecycleTestConnection{
		relations: map[string]string{},
		populated: map[string]bool{},
	}
	adapter := openMySQLTargetLifecycleTestAdapter(t, connection)
	err := adapter.PrepareTables(
		context.Background(),
		[]schema.Table{mysqlTargetLifecycleTestTable("events")},
		"upsert",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "requires an existing target table") {
		t.Fatalf("retained recheck error = %v", err)
	}
}

func TestMySQLTargetPreflightRejectsSelectedTrigger(t *testing.T) {
	connection := &mysqlTargetLifecycleTestConnection{
		relations: map[string]string{
			"target\x00events": "BASE TABLE",
		},
		populated: map[string]bool{},
		triggers: []mysqlTargetLifecycleTestTrigger{{
			schema: "target",
			table:  "events",
			name:   "events_after_insert",
		}},
	}
	adapter := openMySQLTargetLifecycleTestAdapter(t, connection)
	err := preflightMySQLDropRecreate(
		context.Background(),
		adapter.database,
		[]schema.Table{mysqlTargetLifecycleTestTable("events")},
		engine.MySQLServerFlavorOracle80,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "target trigger events_after_insert") {
		t.Fatalf("trigger preflight error = %v", err)
	}
}

func TestOracleMySQLViewVisibilityFailsClosed(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		showView       bool
		partialRevokes int
		want           string
	}{
		{
			name: "missing global privilege",
			want: "global SHOW VIEW",
		},
		{
			name:           "partial revokes",
			showView:       true,
			partialRevokes: 1,
			want:           "partial_revokes",
		},
		{
			name:     "complete visibility",
			showView: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateOracleMySQLGlobalViewVisibility(
				testCase.showView,
				testCase.partialRevokes,
			)
			if testCase.want == "" {
				if err != nil {
					t.Fatalf("visibility rejected: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("visibility error = %v, want %q", err, testCase.want)
			}
		})
	}
}

func mysqlTargetLifecycleTestTable(name string) schema.Table {
	return schema.Table{
		Schema: "target",
		Name:   name,
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			Nullable:           false,
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType: &schema.DeclaredType{
				Base: "bigint",
			},
		}},
	}
}

func openMySQLTargetLifecycleTestAdapter(
	t *testing.T,
	connection *mysqlTargetLifecycleTestConnection,
) *mysqlTargetAdapter {
	t.Helper()
	database := sql.OpenDB(&mysqlTargetLifecycleTestConnector{
		connection: connection,
	})
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close lifecycle test database: %v", err)
		}
	})
	return &mysqlTargetAdapter{
		database:  database,
		flavor:    engine.MySQLServerFlavorOracle80,
		namespace: "target",
	}
}

type mysqlTargetLifecycleTestConnector struct {
	connection *mysqlTargetLifecycleTestConnection
}

func (connector *mysqlTargetLifecycleTestConnector) Connect(
	context.Context,
) (sqldriver.Conn, error) {
	return connector.connection, nil
}

func (*mysqlTargetLifecycleTestConnector) Driver() sqldriver.Driver {
	return mysqlTargetLifecycleTestDriver{}
}

type mysqlTargetLifecycleTestDriver struct{}

func (mysqlTargetLifecycleTestDriver) Open(
	string,
) (sqldriver.Conn, error) {
	return nil, fmt.Errorf("direct lifecycle driver open is unsupported")
}

type mysqlTargetLifecycleTestTrigger struct {
	schema string
	table  string
	name   string
}

type mysqlTargetLifecycleTestConnection struct {
	relations        map[string]string
	populated        map[string]bool
	triggers         []mysqlTargetLifecycleTestTrigger
	populateOnLock   string
	failCreateCause  error
	statements       []string
	locked           bool
	sawLock          bool
	dropWhileLocked  bool
	foreignKeyChecks int
}

func (*mysqlTargetLifecycleTestConnection) Prepare(
	string,
) (sqldriver.Stmt, error) {
	return nil, fmt.Errorf("prepared statements are unsupported")
}

func (*mysqlTargetLifecycleTestConnection) Close() error {
	return nil
}

func (*mysqlTargetLifecycleTestConnection) Begin() (sqldriver.Tx, error) {
	return nil, fmt.Errorf("transactions are unsupported")
}

func (connection *mysqlTargetLifecycleTestConnection) ExecContext(
	_ context.Context,
	query string,
	_ []sqldriver.NamedValue,
) (sqldriver.Result, error) {
	query = strings.TrimSpace(query)
	connection.statements = append(connection.statements, query)
	switch {
	case strings.HasPrefix(query, "LOCK TABLES"):
		connection.locked = true
		connection.sawLock = true
		if connection.populateOnLock != "" {
			connection.populated[connection.populateOnLock] = true
		}
	case query == "UNLOCK TABLES":
		connection.locked = false
	case query == "SET SESSION FOREIGN_KEY_CHECKS = 0":
		connection.foreignKeyChecks = 0
	case query == "SET SESSION FOREIGN_KEY_CHECKS = 1":
		connection.foreignKeyChecks = 1
	case strings.HasPrefix(query, "DROP TABLE"):
		connection.dropWhileLocked = connection.locked
	case strings.HasPrefix(query, "CREATE TABLE"):
		if connection.failCreateCause != nil {
			return nil, connection.failCreateCause
		}
	}
	return sqldriver.RowsAffected(0), nil
}

func (connection *mysqlTargetLifecycleTestConnection) QueryContext(
	_ context.Context,
	query string,
	arguments []sqldriver.NamedValue,
) (sqldriver.Rows, error) {
	switch {
	case strings.Contains(query, "VERSION()") &&
		strings.Contains(query, "@@version_comment"):
		return newMySQLTargetLifecycleRows(
			[]string{"version", "version_comment"},
			[]any{
				"8.0.46",
				"MySQL Community Server - GPL",
			},
		), nil
	case strings.Contains(
		query,
		"information_schema.SCHEMA_PRIVILEGES",
	) && strings.Contains(query, "PRIVILEGE_TYPE = 'TRIGGER'"):
		return newMySQLTargetLifecycleRows(
			[]string{
				"global_visibility",
				"schema_visibility",
				"table_visibility",
				"partial_revokes",
			},
			[]any{false, true, false, 0},
		), nil
	case strings.Contains(query, "@@SESSION.FOREIGN_KEY_CHECKS"):
		value := connection.foreignKeyChecks
		if value == 0 && !containsMySQLLifecycleStatement(
			connection.statements,
			"SET SESSION FOREIGN_KEY_CHECKS = 0",
		) {
			value = 1
		}
		return newMySQLTargetLifecycleRows([]string{"enabled"}, []any{value}), nil
	case strings.Contains(query, "SELECT TABLE_TYPE") &&
		strings.Contains(query, "information_schema.TABLES"):
		key := mysqlTargetLifecycleArgumentKey(arguments)
		kind, exists := connection.relations[key]
		if !exists {
			return mysqlTargetLifecycleEmptyRows("TABLE_TYPE"), nil
		}
		return newMySQLTargetLifecycleRows(
			[]string{"TABLE_TYPE"},
			[]any{kind},
		), nil
	case strings.Contains(query, "SELECT EXISTS (") &&
		strings.Contains(query, "information_schema.TABLES"):
		key := mysqlTargetLifecycleArgumentKey(arguments)
		_, exists := connection.relations[key]
		return newMySQLTargetLifecycleRows(
			[]string{"exists"},
			[]any{exists},
		), nil
	case strings.Contains(query, "information_schema.TRIGGERS"):
		values := make([][]any, len(connection.triggers))
		for index, trigger := range connection.triggers {
			values[index] = []any{
				trigger.schema,
				trigger.table,
				trigger.name,
			}
		}
		return &mysqlTargetLifecycleRows{
			columns: []string{
				"EVENT_OBJECT_SCHEMA",
				"EVENT_OBJECT_TABLE",
				"TRIGGER_NAME",
			},
			values: values,
		}, nil
	case strings.Contains(query, "SELECT TABLE_NAME, TABLE_TYPE") &&
		strings.Contains(query, "information_schema.TABLES"):
		namespace := mysqlTargetLifecycleStringArgument(arguments, 0)
		var keys []string
		for key := range connection.relations {
			if strings.HasPrefix(key, namespace+"\x00") {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		values := make([][]any, len(keys))
		for index, key := range keys {
			values[index] = []any{
				strings.TrimPrefix(key, namespace+"\x00"),
				connection.relations[key],
			}
		}
		return &mysqlTargetLifecycleRows{
			columns: []string{"TABLE_NAME", "TABLE_TYPE"},
			values:  values,
		}, nil
	case strings.Contains(query, "information_schema.USER_PRIVILEGES") &&
		strings.Contains(query, "partial_revokes"):
		return newMySQLTargetLifecycleRows(
			[]string{"show_view", "partial_revokes"},
			[]any{true, int64(0)},
		), nil
	case strings.Contains(query, "information_schema.USER_PRIVILEGES"):
		return newMySQLTargetLifecycleRows(
			[]string{"show_view"},
			[]any{true},
		), nil
	case strings.Contains(query, "information_schema.VIEW_TABLE_USAGE"):
		return mysqlTargetLifecycleEmptyRows(
			"VIEW_SCHEMA",
			"VIEW_NAME",
			"TABLE_SCHEMA",
			"TABLE_NAME",
		), nil
	case strings.Contains(query, "information_schema.VIEWS"):
		return mysqlTargetLifecycleEmptyRows(
			"TABLE_SCHEMA",
			"TABLE_NAME",
			"VIEW_DEFINITION",
			"DEFINER",
		), nil
	case strings.Contains(query, "information_schema.KEY_COLUMN_USAGE"):
		return mysqlTargetLifecycleEmptyRows(
			"TABLE_SCHEMA",
			"TABLE_NAME",
			"CONSTRAINT_NAME",
			"REFERENCED_TABLE_SCHEMA",
			"REFERENCED_TABLE_NAME",
		), nil
	case strings.Contains(query, "SELECT EXISTS (SELECT 1 FROM"):
		key := mysqlTargetLifecycleQualifiedQueryKey(query)
		return newMySQLTargetLifecycleRows(
			[]string{"populated"},
			[]any{connection.populated[key]},
		), nil
	default:
		return nil, fmt.Errorf(
			"unsupported lifecycle test query: %s",
			strings.Join(strings.Fields(query), " "),
		)
	}
}

func mysqlTargetLifecycleArgumentKey(
	arguments []sqldriver.NamedValue,
) string {
	return mysqlTargetLifecycleStringArgument(arguments, 0) + "\x00" +
		mysqlTargetLifecycleStringArgument(arguments, 1)
}

func mysqlTargetLifecycleStringArgument(
	arguments []sqldriver.NamedValue,
	index int,
) string {
	if index >= len(arguments) {
		return ""
	}
	value, _ := arguments[index].Value.(string)
	return value
}

func mysqlTargetLifecycleQualifiedQueryKey(query string) string {
	start := strings.Index(query, "`")
	if start < 0 {
		return ""
	}
	parts := strings.Split(query[start:], "`")
	if len(parts) < 4 {
		return ""
	}
	return parts[1] + "\x00" + parts[3]
}

func containsMySQLLifecycleStatement(
	statements []string,
	want string,
) bool {
	for _, statement := range statements {
		if statement == want {
			return true
		}
	}
	return false
}

type mysqlTargetLifecycleRows struct {
	columns []string
	values  [][]any
	index   int
}

func newMySQLTargetLifecycleRows(
	columns []string,
	values ...[]any,
) *mysqlTargetLifecycleRows {
	return &mysqlTargetLifecycleRows{
		columns: columns,
		values:  values,
	}
}

func mysqlTargetLifecycleEmptyRows(
	columns ...string,
) *mysqlTargetLifecycleRows {
	return &mysqlTargetLifecycleRows{columns: columns}
}

func (rows *mysqlTargetLifecycleRows) Columns() []string {
	return rows.columns
}

func (*mysqlTargetLifecycleRows) Close() error {
	return nil
}

func (rows *mysqlTargetLifecycleRows) Next(
	destination []sqldriver.Value,
) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	for index, value := range rows.values[rows.index] {
		destination[index] = value
	}
	rows.index++
	return nil
}
