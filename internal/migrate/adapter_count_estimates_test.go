package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestAdapterRowCountEstimatorsUseCatalogStatistics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		value      driver.Value
		estimate   func(*sql.DB) (int64, error)
		queryParts []string
		wantArgs   []any
		want       int64
	}{
		{
			name:  "postgres source",
			value: float64(12.6),
			estimate: func(database *sql.DB) (int64, error) {
				adapter := &relationalSourceAdapter{
					spec:     relationalSourceSpec{engine: "postgres"},
					database: database, namespace: "public",
				}
				return adapter.EstimateRows(
					context.Background(),
					schema.Table{Name: "items"},
				)
			},
			queryParts: []string{
				"relation.reltuples",
				"pg_catalog.pg_class",
				"relation.relkind IN ('r', 'p')",
			},
			wantArgs: []any{"public", "items"},
			want:     13,
		},
		{
			name:  "mysql target",
			value: int64(41),
			estimate: func(database *sql.DB) (int64, error) {
				adapter := &mysqlTargetAdapter{
					database: database, namespace: "app",
				}
				return adapter.EstimateRows(
					context.Background(),
					schema.Table{Name: "items"},
				)
			},
			queryParts: []string{
				"information_schema.TABLES",
				"TABLE_TYPE = 'BASE TABLE'",
			},
			wantArgs: []any{"app", "items"},
			want:     41,
		},
		{
			name:  "sql server target",
			value: int64(52),
			estimate: func(database *sql.DB) (int64, error) {
				adapter := &sqlServerTargetAdapter{
					database: database, namespace: "dbo",
				}
				return adapter.EstimateRows(
					context.Background(),
					schema.Table{Name: "items"},
				)
			},
			queryParts: []string{
				"sys.partitions",
				"index_id IN (0, 1)",
			},
			wantArgs: []any{"dbo", "items"},
			want:     52,
		},
		{
			name:  "clickhouse source",
			value: int64(63),
			estimate: func(database *sql.DB) (int64, error) {
				adapter := &clickHouseSourceAdapter{
					database: database, namespace: "app",
				}
				return adapter.EstimateRows(
					context.Background(),
					schema.Table{Name: "items"},
				)
			},
			queryParts: []string{
				"SELECT total_rows",
				"FROM system.tables",
				"is_temporary = 0",
			},
			wantArgs: []any{"app", "items"},
			want:     63,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connector := &adapterEstimateConnector{value: test.value}
			database := sql.OpenDB(connector)
			t.Cleanup(func() { _ = database.Close() })

			got, err := test.estimate(database)
			if err != nil {
				t.Fatalf("EstimateRows: %v", err)
			}
			if got != test.want {
				t.Fatalf("estimate = %d, want %d", got, test.want)
			}
			query, arguments := connector.observation()
			for _, part := range test.queryParts {
				if !strings.Contains(query, part) {
					t.Fatalf("query %q does not contain %q", query, part)
				}
			}
			if !reflect.DeepEqual(arguments, test.wantArgs) {
				t.Fatalf(
					"arguments = %#v, want %#v",
					arguments,
					test.wantArgs,
				)
			}
			if strings.Contains(query, "items") ||
				strings.Contains(query, "public") ||
				strings.Contains(query, "app") {
				t.Fatalf(
					"catalog identity was interpolated into query %q",
					query,
				)
			}
			if strings.Contains(strings.ToUpper(query), "COUNT(") {
				t.Fatalf("estimate query executed COUNT(*): %q", query)
			}
			if strings.Contains(query, "sys.dm_db_partition_stats") {
				t.Fatalf(
					"estimate query requires an unadmitted SQL Server DMV privilege: %q",
					query,
				)
			}
		})
	}
}

func TestAdapterRowCountEstimatorsRejectUnavailableOrMalformedEvidence(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name     string
		value    driver.Value
		estimate func(*sql.DB) (int64, error)
	}{
		{
			name:  "postgres unknown statistics",
			value: float64(-1),
			estimate: func(database *sql.DB) (int64, error) {
				return estimatePostgresRows(
					context.Background(),
					database,
					"public",
					"items",
				)
			},
		},
		{
			name:  "postgres non-finite statistics",
			value: "NaN",
			estimate: func(database *sql.DB) (int64, error) {
				return estimatePostgresRows(
					context.Background(),
					database,
					"public",
					"items",
				)
			},
		},
		{
			name:  "postgres integer overflow boundary",
			value: float64(math.MaxInt64),
			estimate: func(database *sql.DB) (int64, error) {
				return estimatePostgresRows(
					context.Background(),
					database,
					"public",
					"items",
				)
			},
		},
		{
			name:  "mysql null statistics",
			value: nil,
			estimate: func(database *sql.DB) (int64, error) {
				return estimateMySQLRows(
					context.Background(),
					database,
					"app",
					"items",
				)
			},
		},
		{
			name:  "sql server missing object",
			value: nil,
			estimate: func(database *sql.DB) (int64, error) {
				return estimateSQLServerRows(
					context.Background(),
					database,
					"dbo",
					"items",
				)
			},
		},
		{
			name:  "clickhouse unknown statistics",
			value: nil,
			estimate: func(database *sql.DB) (int64, error) {
				return estimateClickHouseRows(
					context.Background(),
					database,
					"app",
					"items",
				)
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			connector := &adapterEstimateConnector{value: test.value}
			database := sql.OpenDB(connector)
			t.Cleanup(func() { _ = database.Close() })
			if _, err := test.estimate(database); err == nil {
				t.Fatal("expected unavailable estimate to fail")
			}
		})
	}
}

func TestSQLiteRowCountEstimateUsesAnalyzeWithoutExactCount(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "statistics.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Exec(`
		CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT);
		INSERT INTO items (id, payload)
		VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');
		CREATE INDEX a_partial ON items(id) WHERE id = 1;
		CREATE INDEX y_full ON items(id);
		CREATE INDEX z_full ON items(payload);
		ANALYZE;
	`); err != nil {
		t.Fatalf("create SQLite statistics: %v", err)
	}
	adapter := &sqliteTargetAdapter{database: database}
	got, err := adapter.EstimateRows(
		context.Background(),
		schema.Table{Name: "items"},
	)
	if err != nil {
		t.Fatalf("EstimateRows: %v", err)
	}
	if got != 4 {
		t.Fatalf("estimate = %d, want 4", got)
	}
	if _, err := database.Exec(
		`UPDATE sqlite_stat1 SET stat = '3 1' WHERE idx = 'y_full'`,
	); err != nil {
		t.Fatalf("corrupt one SQLite statistic: %v", err)
	}
	if _, err := adapter.EstimateRows(
		context.Background(),
		schema.Table{Name: "items"},
	); err == nil {
		t.Fatal("disagreeing full-table statistics produced an estimate")
	}
	if _, err := database.Exec(`DELETE FROM sqlite_stat1`); err != nil {
		t.Fatalf("clear SQLite statistics: %v", err)
	}
	if _, err := adapter.EstimateRows(
		context.Background(),
		schema.Table{Name: "items"},
	); err == nil {
		t.Fatal("missing ANALYZE evidence unexpectedly produced an estimate")
	}
}

func TestStage4AdapterCountProbeUsesOnlyExplicitEstimateSeam(t *testing.T) {
	t.Parallel()

	table := stage4AdapterTestTable()
	source := &stage4EstimateSource{
		recordingAdapterSource: recordingAdapterSource{table: table},
		estimate:               71,
	}
	target := &stage4EstimateTarget{
		recordingAdapterTarget: recordingAdapterTarget{},
		estimate:               72,
	}
	probe := &stage4AdapterCountProbe{
		source: source,
		target: target,
		plans: stage4AdapterPlansBySource([]adapterTablePlan{{
			source: table,
			target: table,
		}}),
	}
	for side, want := range map[ValidationSide]int64{
		ValidationSource: 71,
		ValidationTarget: 72,
	} {
		got, err := probe.EstimateCount(
			context.Background(),
			side,
			table,
		)
		if err != nil {
			t.Fatalf("EstimateCount(%s): %v", side, err)
		}
		if got != want {
			t.Fatalf("EstimateCount(%s) = %d, want %d", side, got, want)
		}
	}
	if source.countCalls != 0 || target.countCalls != 0 {
		t.Fatalf(
			"exact count was called: source=%d target=%d",
			source.countCalls,
			target.countCalls,
		)
	}
	if source.estimateCalls != 1 || target.estimateCalls != 1 {
		t.Fatalf(
			"estimate calls: source=%d target=%d",
			source.estimateCalls,
			target.estimateCalls,
		)
	}
}

func TestStage4AdapterCountProbeCanceledEstimateDoesNotWaitForAdapterGate(
	t *testing.T,
) {
	t.Parallel()

	table := stage4AdapterTestTable()
	source := &blockingStage4EstimateSource{
		recordingAdapterSource: recordingAdapterSource{table: table},
		called:                 make(chan struct{}, 2),
		release:                make(chan struct{}),
	}
	probe := &stage4AdapterCountProbe{
		source: source,
		target: &stage4EstimateTarget{
			recordingAdapterTarget: recordingAdapterTarget{},
			estimate:               72,
		},
		plans: stage4AdapterPlansBySource([]adapterTablePlan{{
			source: table,
			target: table,
		}}),
	}
	first := make(chan error, 1)
	go func() {
		_, err := probe.EstimateCount(
			context.Background(),
			ValidationSource,
			table,
		)
		first <- err
	}()
	select {
	case <-source.called:
	case <-time.After(time.Second):
		t.Fatal("first estimate did not enter the source adapter")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	second := make(chan error, 1)
	go func() {
		_, err := probe.EstimateCount(
			canceled,
			ValidationSource,
			table,
		)
		second <- err
	}()
	select {
	case err := <-second:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled estimate error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled estimate waited behind the blocked adapter call")
	}
	select {
	case <-source.called:
		t.Fatal("canceled estimate reached the source adapter")
	default:
	}

	close(source.release)
	select {
	case err := <-first:
		if err != nil {
			t.Fatalf("first estimate: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first estimate did not finish after release")
	}
}

type stage4EstimateSource struct {
	recordingAdapterSource
	estimate      int64
	estimateCalls int
	countCalls    int
}

func (source *stage4EstimateSource) EstimateRows(
	context.Context,
	schema.Table,
) (int64, error) {
	source.estimateCalls++
	return source.estimate, nil
}

func (source *stage4EstimateSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	source.countCalls++
	return 0, errors.New("exact count must not be called")
}

type blockingStage4EstimateSource struct {
	recordingAdapterSource
	called  chan struct{}
	release chan struct{}
}

func (source *blockingStage4EstimateSource) EstimateRows(
	ctx context.Context,
	_ schema.Table,
) (int64, error) {
	source.called <- struct{}{}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-source.release:
		return 71, nil
	}
}

type stage4EstimateTarget struct {
	recordingAdapterTarget
	estimate      int64
	estimateCalls int
	countCalls    int
}

func (target *stage4EstimateTarget) EstimateRows(
	context.Context,
	schema.Table,
) (int64, error) {
	target.estimateCalls++
	return target.estimate, nil
}

func (target *stage4EstimateTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	target.countCalls++
	return 0, errors.New("exact count must not be called")
}

type adapterEstimateConnector struct {
	mu        sync.Mutex
	value     driver.Value
	query     string
	arguments []any
}

func (connector *adapterEstimateConnector) Connect(
	context.Context,
) (driver.Conn, error) {
	return &adapterEstimateConnection{connector: connector}, nil
}

func (*adapterEstimateConnector) Driver() driver.Driver {
	return adapterEstimateDriver{}
}

func (connector *adapterEstimateConnector) observation() (
	string,
	[]any,
) {
	connector.mu.Lock()
	defer connector.mu.Unlock()
	return connector.query, append([]any(nil), connector.arguments...)
}

type adapterEstimateDriver struct{}

func (adapterEstimateDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("adapter estimate test driver requires a connector")
}

type adapterEstimateConnection struct {
	connector *adapterEstimateConnector
}

func (*adapterEstimateConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare is unsupported")
}

func (*adapterEstimateConnection) Close() error {
	return nil
}

func (*adapterEstimateConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("transactions are unsupported")
}

func (connection *adapterEstimateConnection) QueryContext(
	_ context.Context,
	query string,
	arguments []driver.NamedValue,
) (driver.Rows, error) {
	connection.connector.mu.Lock()
	connection.connector.query = query
	connection.connector.arguments = make([]any, len(arguments))
	for index, argument := range arguments {
		connection.connector.arguments[index] = argument.Value
	}
	value := connection.connector.value
	connection.connector.mu.Unlock()
	return &adapterEstimateRows{value: value}, nil
}

type adapterEstimateRows struct {
	value driver.Value
	read  bool
}

func (*adapterEstimateRows) Columns() []string {
	return []string{"estimate"}
}

func (*adapterEstimateRows) Close() error {
	return nil
}

func (rows *adapterEstimateRows) Next(values []driver.Value) error {
	if rows.read {
		return io.EOF
	}
	rows.read = true
	values[0] = rows.value
	return nil
}

var _ driver.QueryerContext = (*adapterEstimateConnection)(nil)
