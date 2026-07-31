package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestStableRelationalViewForwardsOnlyImmutableScopedMetadata(
	t *testing.T,
) {
	ctx := context.Background()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			payload TEXT NOT NULL
		);
		INSERT INTO events VALUES (1, 'one'), (2, 'two');
	`); err != nil {
		t.Fatal(err)
	}
	transaction, err := database.BeginTx(
		ctx,
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback() }()

	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine:      "fixture",
			displayName: "fixture",
			qualifiedTable: func(_, table string) string {
				return `"` + table + `"`
			},
		},
		database: database,
	}
	view, err := newAdapterRetainedStableRelationalView(
		source,
		&adapterSQLTransactionStableView{
			transaction: transaction,
			engine:      "fixture",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	table := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
		},
	}
	if err := view.bindTableScope(table); err != nil {
		t.Fatal(err)
	}
	var sourceContract sourceAdapter = view
	if _, ok := any(sourceContract).(adapterNetworkStableRangePageSource); !ok {
		t.Fatal("stable relational view lacks range admission marker")
	}
	names, err := sourceContract.ListTables(ctx)
	if err != nil || !reflect.DeepEqual(names, []string{"events"}) {
		t.Fatalf("stable table list = %#v, %v", names, err)
	}
	inspected, err := sourceContract.InspectTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	inspected.Columns[0].Name = "mutated"
	again, err := sourceContract.InspectTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if again.Columns[0].Name != "id" {
		t.Fatal("stable catalog alias escaped its deep clone")
	}
	if _, err := sourceContract.InspectTable(ctx, "other"); err == nil {
		t.Fatal("out-of-scope stable table metadata was exposed")
	}
	count, err := sourceContract.CountRows(ctx, table)
	if err != nil || count != 2 {
		t.Fatalf("stable row count = %d, %v", count, err)
	}
	other := table
	other.Name = "other"
	if _, err := sourceContract.CountRows(ctx, other); err == nil {
		t.Fatal("out-of-scope stable table count succeeded")
	}
	if err := sourceContract.Close(); err != nil {
		t.Fatal(err)
	}
	if count, err := sourceContract.CountRows(ctx, table); err != nil ||
		count != 2 {
		t.Fatalf(
			"wrapper Close ended owner transaction: count=%d error=%v",
			count,
			err,
		)
	}
}

func TestStableRelationalViewRejectsCrossViewKeysetPlans(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "source",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name:               "tenant",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
		},
	}
	columns := adapterColumnNames(table)
	const retainedBytes = int64(128)
	scope := adapterStableTableIdentity{
		schema: table.Schema,
		table:  table.Name,
	}
	for _, strategy := range []PaginationStrategy{
		PaginationIntegerKeyset,
		PaginationTupleKeyset,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			plan := PaginationPlan{
				Strategy:     strategy,
				TopologyHash: strings.Repeat("a", 64),
			}
			identity := adapterStablePaginationIdentity(
				table,
				plan.TopologyHash,
			)
			foreignView := &adapterRetainedStableRelationalView{
				paginationPlans: map[string]PaginationPlan{
					identity: clonePaginationPlan(plan),
				},
			}
			if _, ok := foreignView.paginationPlans[identity]; !ok {
				t.Fatal("foreign stable view did not record its plan")
			}
			readerView := &adapterRetainedStableRelationalView{
				retainedRowBounds: map[string]int64{
					adapterStableRetainedIdentity(
						table,
						columns,
					): retainedBytes,
				},
				paginationPlans: make(map[string]PaginationPlan),
				tableScope:      &scope,
			}
			err := readerView.admitNetworkRangeRead(
				table,
				columns,
				plan,
				retainedBytes,
			)
			if err == nil ||
				!strings.Contains(
					err.Error(),
					"exact same-view pagination plan",
				) {
				t.Fatalf(
					"cross-view %s plan error = %v",
					strategy,
					err,
				)
			}
			readerView.paginationPlans[identity] =
				clonePaginationPlan(plan)
			if err := readerView.admitNetworkRangeRead(
				table,
				columns,
				plan,
				retainedBytes,
			); err != nil {
				t.Fatalf(
					"same-view %s plan rejected: %v",
					strategy,
					err,
				)
			}
		})
	}
}

func TestStableRelationalViewRejectsMutatedRecordedPlan(
	t *testing.T,
) {
	lower := KeyTuple{IntegerKey(1), BytesKey([]byte{0x01})}
	upper := KeyTuple{IntegerKey(9), BytesKey([]byte{0xff})}
	plan := PaginationPlan{
		Strategy: PaginationTupleKeyset,
		Keys: []KeySpec{
			{Name: "tenant", Kind: KeyInteger},
			{Name: "digest", Kind: KeyBytes},
		},
		Ranges: []PaginationRange{{
			ID:    0,
			Lower: &lower,
			Upper: &upper,
		}},
		TopologyHash: strings.Repeat("a", 64),
	}
	table := schema.Table{
		Schema: "source",
		Name:   "events",
	}
	columns := []string{"tenant", "digest"}
	const retainedBytes = int64(128)
	scope := adapterStableTableIdentity{
		schema: table.Schema,
		table:  table.Name,
	}
	view := &adapterRetainedStableRelationalView{
		retainedRowBounds: map[string]int64{
			adapterStableRetainedIdentity(
				table,
				columns,
			): retainedBytes,
		},
		paginationPlans: make(map[string]PaginationPlan),
		tableScope:      &scope,
	}
	identity := adapterStablePaginationIdentity(
		table,
		plan.TopologyHash,
	)
	view.paginationPlans[identity] = clonePaginationPlan(plan)
	returned := clonePaginationPlan(plan)
	if err := view.admitNetworkRangeRead(
		table,
		columns,
		returned,
		retainedBytes,
	); err != nil {
		t.Fatalf("exact recorded pagination plan rejected: %v", err)
	}

	mutations := []struct {
		name   string
		mutate func(*PaginationPlan)
	}{
		{
			name: "strategy",
			mutate: func(value *PaginationPlan) {
				value.Strategy = PaginationRowNumber
			},
		},
		{
			name: "key",
			mutate: func(value *PaginationPlan) {
				value.Keys[1].Kind = KeyText
			},
		},
		{
			name: "range identity",
			mutate: func(value *PaginationPlan) {
				value.Ranges[0].ID = 1
			},
		},
		{
			name: "nested lower bound",
			mutate: func(value *PaginationPlan) {
				(*value.Ranges[0].Lower)[0] = IntegerKey(2)
			},
		},
		{
			name: "nested upper kind",
			mutate: func(value *PaginationPlan) {
				(*value.Ranges[0].Upper)[1] = TextKey("mutated")
			},
		},
		{
			name: "row envelope",
			mutate: func(value *PaginationPlan) {
				value.Ranges[0].LastRow = 9
			},
		},
		{
			name: "range inventory",
			mutate: func(value *PaginationPlan) {
				value.Ranges = append(
					value.Ranges,
					PaginationRange{ID: 1, Empty: true},
				)
			},
		},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			mutated := clonePaginationPlan(returned)
			mutation.mutate(&mutated)
			if mutated.TopologyHash != plan.TopologyHash {
				t.Fatal("mutation changed the recorded topology hash")
			}
			err := view.admitNetworkRangeRead(
				table,
				columns,
				mutated,
				retainedBytes,
			)
			if err == nil ||
				!strings.Contains(
					err.Error(),
					"exact same-view pagination plan",
				) {
				t.Fatalf(
					"mutated recorded plan admission error = %v",
					err,
				)
			}
		})
	}
	if !equalAdapterStablePaginationPlan(
		view.paginationPlans[identity],
		plan,
	) {
		t.Fatal("caller mutation aliased the recorded pagination plan")
	}
}

func TestStableNetworkCustomOpenerReportsValidationAndCloseFailures(
	t *testing.T,
) {
	closeFailure := errors.New("forced invalid-session close failure")
	table := schema.Table{Name: "events"}
	validSource := &recordingAdapterSource{}
	for _, test := range []struct {
		name    string
		session *adapterStableNetworkTableSession
		want    string
	}{
		{
			name: "missing source",
			session: &adapterStableNetworkTableSession{
				readerLimit: 1,
			},
			want: "stable network table session is unavailable",
		},
		{
			name: "invalid reader limit",
			session: &adapterStableNetworkTableSession{
				source: validSource,
			},
			want: "stable network source reader limit is invalid",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			closed := 0
			test.session.closeFn = func() error {
				closed++
				return closeFailure
			}
			source := &stableNetworkInvalidSessionOpener{
				session: test.session,
			}
			opened, err := OpenAdapterStableNetworkTableSource(
				context.Background(),
				source,
				table,
			)
			if opened != nil ||
				err == nil ||
				!strings.Contains(err.Error(), test.want) ||
				!errors.Is(err, closeFailure) {
				t.Fatalf(
					"invalid custom session = %#v, %v",
					opened,
					err,
				)
			}
			if closed != 1 {
				t.Fatalf("invalid custom session close calls = %d", closed)
			}
		})
	}
}

type stableNetworkInvalidSessionOpener struct {
	sourceAdapter
	session *adapterStableNetworkTableSession
}

func (source *stableNetworkInvalidSessionOpener) openStableNetworkTableSource(
	context.Context,
	schema.Table,
) (*adapterStableNetworkTableSession, error) {
	return source.session, nil
}

func TestStableNetworkRelationalSessionDiscardsRollbackFailure(
	t *testing.T,
) {
	pinFailure := errors.New("forced stable pin failure")
	rollbackFailure := errors.New("forced stable rollback failure")
	for _, test := range []struct {
		name       string
		pinFailure error
	}{
		{name: "normal close"},
		{name: "open failure", pinFailure: pinFailure},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := &stableRollbackTestState{
				queryErr:    test.pinFailure,
				rollbackErr: rollbackFailure,
			}
			driverName := fmt.Sprintf(
				"dmtx_stable_rollback_%d",
				stableRollbackDriverSequence.Add(1),
			)
			sql.Register(
				driverName,
				stableRollbackTestDriver{state: state},
			)
			database, err := sql.Open(driverName, "")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = database.Close() }()
			source := &relationalSourceAdapter{
				spec: relationalSourceSpec{
					engine:      "postgres",
					displayName: "rollback fixture",
					qualifiedTable: func(schemaName, tableName string) string {
						return `"` + schemaName + `"."` +
							tableName + `"`
					},
				},
				database:  database,
				namespace: "public",
			}
			table := schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				}},
			}
			session, openErr := OpenAdapterStableNetworkTableSource(
				context.Background(),
				source,
				table,
			)
			if test.pinFailure == nil {
				if openErr != nil || session == nil {
					t.Fatalf(
						"open rollback fixture session = %#v, %v",
						session,
						openErr,
					)
				}
				openErr = session.Close()
			} else if session != nil || !errors.Is(openErr, pinFailure) {
				t.Fatalf(
					"failed rollback fixture open = %#v, %v",
					session,
					openErr,
				)
			}
			if !errors.Is(openErr, rollbackFailure) {
				t.Fatalf("rollback failure was not preserved: %v", openErr)
			}
			opened, closed := state.counts()
			if opened != 1 || closed != 1 {
				t.Fatalf(
					"rollback-failed connection counts = open:%d close:%d",
					opened,
					closed,
				)
			}
			if err := database.PingContext(context.Background()); err != nil {
				t.Fatalf("ping after discarded rollback connection: %v", err)
			}
			opened, closed = state.counts()
			if opened != 2 || closed != 1 {
				t.Fatalf(
					"post-discard connection counts = open:%d close:%d",
					opened,
					closed,
				)
			}
		})
	}
}

var stableRollbackDriverSequence atomic.Uint64

type stableRollbackTestState struct {
	mu          sync.Mutex
	queryErr    error
	rollbackErr error
	opened      int
	closed      int
}

func (state *stableRollbackTestState) counts() (int, int) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.opened, state.closed
}

type stableRollbackTestDriver struct {
	state *stableRollbackTestState
}

func (testDriver stableRollbackTestDriver) Open(
	string,
) (driver.Conn, error) {
	testDriver.state.mu.Lock()
	testDriver.state.opened++
	testDriver.state.mu.Unlock()
	return &stableRollbackTestConnection{state: testDriver.state}, nil
}

type stableRollbackTestConnection struct {
	state *stableRollbackTestState
}

func (*stableRollbackTestConnection) Prepare(
	string,
) (driver.Stmt, error) {
	return nil, errors.New("stable rollback fixture does not prepare")
}

func (connection *stableRollbackTestConnection) Close() error {
	connection.state.mu.Lock()
	connection.state.closed++
	connection.state.mu.Unlock()
	return nil
}

func (connection *stableRollbackTestConnection) Begin() (
	driver.Tx,
	error,
) {
	return connection.BeginTx(
		context.Background(),
		driver.TxOptions{},
	)
}

func (connection *stableRollbackTestConnection) BeginTx(
	context.Context,
	driver.TxOptions,
) (driver.Tx, error) {
	return &stableRollbackTestTransaction{state: connection.state}, nil
}

func (connection *stableRollbackTestConnection) QueryContext(
	context.Context,
	string,
	[]driver.NamedValue,
) (driver.Rows, error) {
	if connection.state.queryErr != nil {
		return nil, connection.state.queryErr
	}
	return &stableRollbackTestRows{}, nil
}

func (*stableRollbackTestConnection) Ping(context.Context) error {
	return nil
}

type stableRollbackTestTransaction struct {
	state *stableRollbackTestState
}

func (*stableRollbackTestTransaction) Commit() error {
	return nil
}

func (transaction *stableRollbackTestTransaction) Rollback() error {
	return transaction.state.rollbackErr
}

type stableRollbackTestRows struct {
	read bool
}

func (*stableRollbackTestRows) Columns() []string {
	return []string{"count"}
}

func (*stableRollbackTestRows) Close() error {
	return nil
}

func (rows *stableRollbackTestRows) Next(
	values []driver.Value,
) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	values[0] = int64(1)
	return nil
}
