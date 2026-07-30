package migrate

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerTargetLifecycleOrdersObjectsBeforeIdentityEffects(
	t *testing.T,
) {
	frontier := int64(7)
	table := sqlServerTargetLifecycleIdentityTable(frontier)
	state := &sqlServerLifecycleDriverState{}
	database := openSQLServerLifecycleTestDatabase(t, state)

	if err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"drop_recreate",
	); err != nil {
		t.Fatalf("finalizeSQLServerTargets: %v", err)
	}

	indexPosition := sqlServerLifecycleEventPosition(
		state.events,
		"exec CREATE ",
	)
	reseedPosition := sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	)
	if indexPosition < 0 || reseedPosition < 0 ||
		indexPosition >= reseedPosition {
		t.Fatalf(
			"finalization events = %#v; post-load DDL must precede identity reseed",
			state.events,
		)
	}
	for index := reseedPosition + 1; index < len(state.events); index++ {
		event := state.events[index]
		if strings.HasPrefix(event, "exec CREATE ") ||
			strings.HasPrefix(event, "exec ALTER ") {
			t.Fatalf(
				"fallible post-load DDL %q followed identity effect: %#v",
				event,
				state.events,
			)
		}
	}
}

func TestSQLServerTargetLifecyclePlanSortsIdentityAfterEveryObject(
	t *testing.T,
) {
	frontier := int64(3)
	first := sqlServerTargetLifecycleIdentityTable(frontier)
	first.Name = "z_identity"
	second := first
	second.Name = "a_identity"
	plain := first
	plain.Name = "plain"
	plain.Identity = nil

	objects := []schema.SQLServerObjectStatement{
		{Table: "first", SQL: "first object"},
		{Table: "second", SQL: "second object"},
	}
	steps := sqlServerFinalizationSteps(
		objects,
		[]schema.Table{first, plain, second},
	)
	if len(steps) != 4 {
		t.Fatalf("steps = %#v, want 4", steps)
	}
	for index := range objects {
		if steps[index].identity ||
			steps[index].object.SQL != objects[index].SQL {
			t.Fatalf("object step %d = %#v", index, steps[index])
		}
	}
	if !steps[2].identity || steps[2].table.Name != "a_identity" ||
		!steps[3].identity || steps[3].table.Name != "z_identity" {
		t.Fatalf("identity steps are not ordered last: %#v", steps)
	}
}

func TestSQLServerTargetLifecycleKeepsNextIdentityAtOneAfterNegativeRows(
	t *testing.T,
) {
	table := sqlServerTargetLifecycleIdentityTable(0)
	table.Identity.Frontier = nil
	state := &sqlServerLifecycleDriverState{
		maximumValue: int64(-4),
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	if err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"drop_recreate",
	); err != nil {
		t.Fatalf("finalizeSQLServerTargets: %v", err)
	}
	reseedPosition := sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	)
	if reseedPosition < 0 ||
		!strings.Contains(state.events[reseedPosition], ", RESEED, 0)") {
		t.Fatalf(
			"negative-only identity was not reseeded to frontier zero: %#v",
			state.events,
		)
	}
}

func TestSQLServerTargetLifecyclePreservesSourceFrontierBelowRowMaximum(
	t *testing.T,
) {
	table := sqlServerTargetLifecycleIdentityTable(7)
	state := &sqlServerLifecycleDriverState{
		maximumValue: int64(100),
		currentValue: int64(100),
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	if err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"drop_recreate",
	); err != nil {
		t.Fatalf("finalizeSQLServerTargets: %v", err)
	}
	reseedPosition := sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	)
	if reseedPosition < 0 ||
		!strings.Contains(state.events[reseedPosition], ", RESEED, 7)") {
		t.Fatalf(
			"source identity frontier was raised to explicit row MAX: %#v",
			state.events,
		)
	}
}

func TestSQLServerTargetLifecyclePreservesUncalledSourceWithExplicitRows(
	t *testing.T,
) {
	table := sqlServerTargetLifecycleIdentityTable(0)
	table.Identity.Frontier = nil
	state := &sqlServerLifecycleDriverState{
		maximumValue: int64(100),
		currentValue: int64(100),
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	if err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"drop_recreate",
	); err != nil {
		t.Fatalf("finalizeSQLServerTargets: %v", err)
	}
	reseedPosition := sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	)
	if reseedPosition < 0 ||
		!strings.Contains(state.events[reseedPosition], ", RESEED, 0)") {
		t.Fatalf(
			"uncalled source identity did not preserve next value 1: %#v",
			state.events,
		)
	}
}

func TestSQLServerTargetLifecycleUpsertRetainsHighestIdentityState(
	t *testing.T,
) {
	table := sqlServerTargetLifecycleIdentityTable(7)
	state := &sqlServerLifecycleDriverState{
		maximumValue: int64(100),
		currentValue: int64(90),
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	if err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"upsert",
	); err != nil {
		t.Fatalf("finalizeSQLServerTargets: %v", err)
	}
	reseedPosition := sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	)
	if reseedPosition < 0 ||
		!strings.Contains(state.events[reseedPosition], ", RESEED, 100)") {
		t.Fatalf(
			"upsert did not retain the highest identity state: %#v",
			state.events,
		)
	}
}

func TestSQLServerTargetLifecycleUpsertRetainsExhaustedIdentityState(
	t *testing.T,
) {
	table := sqlServerTargetLifecycleIdentityTable(7)
	state := &sqlServerLifecycleDriverState{
		currentValue: int64(math.MaxInt64),
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	if err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"upsert",
	); err != nil {
		t.Fatalf("finalizeSQLServerTargets: %v", err)
	}
	if sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	) >= 0 {
		t.Fatalf(
			"exhausted retained identity was unnecessarily reseeded: %#v",
			state.events,
		)
	}
}

func TestSQLServerTargetLifecycleRejectsUnsafeEmptyPrimerBeforeDBCC(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression("id < 0")
	if err != nil {
		t.Fatal(err)
	}
	table := sqlServerTargetLifecycleIdentityTable(7)
	table.Indexes = nil
	table.Checks = []schema.CheckConstraint{{
		Name:       "ck_identity_target_id",
		Expression: check,
	}}
	state := &sqlServerLifecycleDriverState{}
	database := openSQLServerLifecycleTestDatabase(t, state)

	err = finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "synthetic row") {
		t.Fatalf("unsafe empty identity primer error = %v", err)
	}
	if sqlServerLifecycleEventPosition(
		state.events,
		"exec DBCC CHECKIDENT ",
	) >= 0 {
		t.Fatalf("DBCC ran before primer safety rejection: %#v", state.events)
	}
}

func TestSQLServerTargetLifecyclePrimerOverridesNotNullDefaultNull(
	t *testing.T,
) {
	table := sqlServerTargetLifecycleIdentityTable(7)
	table.Indexes = nil
	table.Columns[1].Nullable = false
	definition := "((NULL))"
	expression, err := schema.ParseSQLServerCatalogDefault(
		table.Columns[1],
		&definition,
	)
	if err != nil {
		t.Fatalf("ParseSQLServerCatalogDefault: %v", err)
	}
	table.Columns[1].Default = expression

	statement, arguments, err := sqlServerIdentityPrimerInsert(table)
	if err != nil {
		t.Fatalf("sqlServerIdentityPrimerInsert: %v", err)
	}
	if !strings.Contains(statement, "([payload]) VALUES (@p1)") {
		t.Fatalf(
			"DEFAULT NULL NOT NULL column was omitted from primer: %q",
			statement,
		)
	}
	if len(arguments) != 1 || arguments[0] != "" {
		t.Fatalf("primer arguments = %#v, want explicit empty string", arguments)
	}
}

func TestSQLServerTargetLifecycleCommitAmbiguityIsStateAndRedacted(
	t *testing.T,
) {
	commitFailure := errors.New("driver exposed secret target DSN")
	state := &sqlServerLifecycleDriverState{
		commitErr: commitFailure,
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	err := finalizeSQLServerTargets(
		context.Background(),
		database,
		nil,
		"upsert",
	)
	if !errors.Is(err, commitFailure) {
		t.Fatalf("error = %v, want commit failure", err)
	}
	if ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf(
			"error class = %s, want state: %v",
			ClassifyTransferError(err),
			err,
		)
	}
	if strings.Contains(err.Error(), "secret target DSN") ||
		!strings.Contains(err.Error(), "outcome is unknown") {
		t.Fatalf("unsafe or unclear commit error: %v", err)
	}
	if !state.closed {
		t.Fatal("ambiguous commit connection was not discarded")
	}
}

func TestSQLServerTargetLifecycleRollbackFailureDiscardsAndMarksState(
	t *testing.T,
) {
	execFailure := errors.New("driver exposed secret object")
	rollbackFailure := errors.New("driver exposed secret rollback")
	state := &sqlServerLifecycleDriverState{
		execErr:     execFailure,
		rollbackErr: rollbackFailure,
	}
	database := openSQLServerLifecycleTestDatabase(t, state)
	table := sqlServerTargetLifecyclePlainIndexedTable()

	err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{table},
		"drop_recreate",
	)
	if !errors.Is(err, execFailure) ||
		!errors.Is(err, rollbackFailure) {
		t.Fatalf("error = %v, want object and rollback failures", err)
	}
	if ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf(
			"error class = %s, want state: %v",
			ClassifyTransferError(err),
			err,
		)
	}
	if strings.Contains(err.Error(), "secret object") ||
		strings.Contains(err.Error(), "secret rollback") {
		t.Fatalf("safe lifecycle error exposed driver text: %v", err)
	}
	if !state.closed {
		t.Fatal("connection with failed rollback was not discarded")
	}
}

func TestSQLServerTargetLifecycleSuppressesStructuredXACTAbortRollback(
	t *testing.T,
) {
	execFailure := errors.New("driver exposed object failure")
	state := &sqlServerLifecycleDriverState{
		execErr: execFailure,
		rollbackErr: mssql.Error{
			Number:  sqlServerRollbackWithoutBeginErrorNumber,
			Message: "ROLLBACK TRANSACTION has no corresponding BEGIN",
		},
	}
	database := openSQLServerLifecycleTestDatabase(t, state)

	err := finalizeSQLServerTargets(
		context.Background(),
		database,
		[]schema.Table{sqlServerTargetLifecyclePlainIndexedTable()},
		"drop_recreate",
	)
	if !errors.Is(err, execFailure) {
		t.Fatalf("error = %v, want object failure", err)
	}
	if ClassifyTransferError(err) == ErrorClassState {
		t.Fatalf("known XACT_ABORT completion was marked unknown: %v", err)
	}
	if strings.Contains(err.Error(), "roll back SQL Server") {
		t.Fatalf("known XACT_ABORT rollback error was joined: %v", err)
	}
}

func sqlServerTargetLifecycleIdentityTable(frontier int64) schema.Table {
	return schema.Table{
		Schema: "dbo",
		Name:   "identity_target",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name: "payload",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{64},
				},
				Nullable: true,
			},
		},
		Indexes: []schema.Index{{
			Name:    "ix_identity_payload",
			Columns: []schema.IndexColumn{{Name: "payload"}},
		}},
	}
}

func sqlServerTargetLifecyclePlainIndexedTable() schema.Table {
	return schema.Table{
		Schema: "dbo",
		Name:   "plain_target",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
		Indexes: []schema.Index{{
			Name:    "ix_plain_id",
			Columns: []schema.IndexColumn{{Name: "id"}},
		}},
	}
}

func sqlServerLifecycleEventPosition(
	events []string,
	prefix string,
) int {
	for index, event := range events {
		if strings.HasPrefix(event, prefix) {
			return index
		}
	}
	return -1
}

var sqlServerLifecycleDriverSequence atomic.Uint64

func openSQLServerLifecycleTestDatabase(
	t *testing.T,
	state *sqlServerLifecycleDriverState,
) *sql.DB {
	t.Helper()
	name := fmt.Sprintf(
		"dmtx_mssql_lifecycle_%d",
		sqlServerLifecycleDriverSequence.Add(1),
	)
	sql.Register(name, sqlServerLifecycleTestDriver{state: state})
	database, err := sql.Open(name, "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	database.SetMaxOpenConns(1)
	t.Cleanup(func() {
		_ = database.Close()
	})
	return database
}

type sqlServerLifecycleDriverState struct {
	events       []string
	execErr      error
	commitErr    error
	rollbackErr  error
	closed       bool
	maximumValue sqldriver.Value
	currentValue sqldriver.Value
}

type sqlServerLifecycleTestDriver struct {
	state *sqlServerLifecycleDriverState
}

func (testDriver sqlServerLifecycleTestDriver) Open(
	string,
) (sqldriver.Conn, error) {
	return &sqlServerLifecycleTestConnection{
		state: testDriver.state,
	}, nil
}

type sqlServerLifecycleTestConnection struct {
	state *sqlServerLifecycleDriverState
}

func (connection *sqlServerLifecycleTestConnection) Prepare(
	string,
) (sqldriver.Stmt, error) {
	return nil, sqldriver.ErrSkip
}

func (connection *sqlServerLifecycleTestConnection) Close() error {
	connection.state.closed = true
	connection.state.events = append(connection.state.events, "close")
	return nil
}

func (connection *sqlServerLifecycleTestConnection) Begin() (
	sqldriver.Tx,
	error,
) {
	return connection.BeginTx(context.Background(), sqldriver.TxOptions{})
}

func (connection *sqlServerLifecycleTestConnection) BeginTx(
	_ context.Context,
	_ sqldriver.TxOptions,
) (sqldriver.Tx, error) {
	connection.state.events = append(connection.state.events, "begin")
	return &sqlServerLifecycleTestTransaction{
		state: connection.state,
	}, nil
}

func (connection *sqlServerLifecycleTestConnection) ExecContext(
	_ context.Context,
	statement string,
	_ []sqldriver.NamedValue,
) (sqldriver.Result, error) {
	connection.state.events = append(
		connection.state.events,
		"exec "+statement,
	)
	if connection.state.execErr != nil {
		return nil, connection.state.execErr
	}
	return sqldriver.RowsAffected(1), nil
}

func (connection *sqlServerLifecycleTestConnection) QueryContext(
	_ context.Context,
	statement string,
	_ []sqldriver.NamedValue,
) (sqldriver.Rows, error) {
	connection.state.events = append(
		connection.state.events,
		"query "+statement,
	)
	switch {
	case strings.HasPrefix(statement, "SELECT MAX("):
		return &sqlServerLifecycleTestRows{
			values: [][]sqldriver.Value{{connection.state.maximumValue}},
		}, nil
	case strings.Contains(statement, "FROM sys.identity_columns"):
		return &sqlServerLifecycleTestRows{
			values: [][]sqldriver.Value{{connection.state.currentValue}},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected test query")
	}
}

type sqlServerLifecycleTestTransaction struct {
	state *sqlServerLifecycleDriverState
}

func (transaction *sqlServerLifecycleTestTransaction) Commit() error {
	transaction.state.events = append(transaction.state.events, "commit")
	return transaction.state.commitErr
}

func (transaction *sqlServerLifecycleTestTransaction) Rollback() error {
	transaction.state.events = append(transaction.state.events, "rollback")
	return transaction.state.rollbackErr
}

type sqlServerLifecycleTestRows struct {
	values [][]sqldriver.Value
	index  int
}

func (rows *sqlServerLifecycleTestRows) Columns() []string {
	return []string{"value"}
}

func (rows *sqlServerLifecycleTestRows) Close() error {
	return nil
}

func (rows *sqlServerLifecycleTestRows) Next(
	destination []sqldriver.Value,
) error {
	if rows.index >= len(rows.values) {
		return io.EOF
	}
	copy(destination, rows.values[rows.index])
	rows.index++
	return nil
}
