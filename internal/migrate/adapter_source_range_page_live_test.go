package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

func TestPostgresNetworkRangePageLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL range-page sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL range-page DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL range-page setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close PostgreSQL range-page setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL range-page setup: %T", err)
	}
	namespace := "dmtx_range_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "tuple_events"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL range-page schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL range-page schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			tenant BIGINT NOT NULL,
			id BIGINT NOT NULL,
			payload VARCHAR(16) NOT NULL,
			label VARCHAR(32) NOT NULL,
			PRIMARY KEY (tenant, id)
		)`,
		`INSERT INTO ` + qualified + ` VALUES
			(-9, 9007199254740993, '01', 'negative'),
			(1, -9223372036854775807, '0203', 'minimum'),
			(1, 4, '04', 'middle'),
			(3, 1, '050607', 'later'),
			(3, 9007199254741000, '08', 'maximum')`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create PostgreSQL range-page fixture: %v", err)
		}
	}
	source, err := openPostgresSourceAdapter(ctx, config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	})
	if err != nil {
		t.Fatalf("open PostgreSQL range-page source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close PostgreSQL range-page source: %v", err)
		}
	})
	assertAdapterNetworkRangePageLive(
		t,
		ctx,
		source,
		tableName,
		[][]int64{
			{-9, 9_007_199_254_740_993},
			{1, -9_223_372_036_854_775_807},
			{1, 4},
			{3, 1},
			{3, 9_007_199_254_741_000},
		},
	)
}

func TestMySQLNetworkRangePageLiveTLS(t *testing.T) {
	testMySQLFamilyNetworkRangePageLiveTLS(
		t,
		mysqlRangePageLiveFixture{
			name:       "MySQL",
			dsnEnv:     "DMTX_TEST_MYSQL_DSN",
			caEnv:      "DMTX_TEST_MYSQL_CA",
			tlsConfig:  "dmtx_test",
			collation:  "utf8mb4_0900_bin",
			wantFlavor: engine.MySQLServerFlavorOracle80,
		},
	)
}

func TestMariaDBNetworkRangePageLiveTLS(t *testing.T) {
	testMySQLFamilyNetworkRangePageLiveTLS(
		t,
		mysqlRangePageLiveFixture{
			name:       "MariaDB",
			dsnEnv:     "DMTX_TEST_MARIADB_DSN",
			caEnv:      "DMTX_TEST_MARIADB_CA",
			tlsConfig:  "dmtx_mariadb_test",
			collation:  "utf8mb4_nopad_bin",
			wantFlavor: engine.MySQLServerFlavorMariaDB1011,
		},
	)
}

type mysqlRangePageLiveFixture struct {
	name       string
	dsnEnv     string
	caEnv      string
	tlsConfig  string
	collation  string
	wantFlavor engine.MySQLServerFlavor
}

func testMySQLFamilyNetworkRangePageLiveTLS(
	t *testing.T,
	fixture mysqlRangePageLiveFixture,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || caPath == "" {
		t.Skip(
			"set " + fixture.dsnEnv + " and " + fixture.caEnv +
				" to run the " + fixture.name +
				" range-page sentinel",
		)
	}
	registerMySQLCommonFixtureTLSNamed(
		t,
		caPath,
		fixture.tlsConfig,
	)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"range-page source",
		dsn,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s range-page setup: %T", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close %s range-page setup: %v", fixture.name, err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping %s range-page setup: %T", fixture.name, err)
	}
	flavor, err := engine.DetectMySQLServerFlavor(ctx, setup)
	if err != nil {
		t.Fatalf("detect %s range-page flavor: %v", fixture.name, err)
	}
	if flavor != fixture.wantFlavor {
		t.Fatalf(
			"%s live flavor = %v, want %v",
			fixture.name,
			flavor,
			fixture.wantFlavor,
		)
	}
	tableName := "dmtx_range_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	qualified := mySQLQualified(parsed.DBName, tableName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+qualified,
		); err != nil {
			t.Errorf(
				"drop %s range-page table: %v",
				fixture.name,
				err,
			)
		}
	})
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			tenant BIGINT NOT NULL,
			id BIGINT NOT NULL,
			payload VARBINARY(16) NOT NULL,
			label VARCHAR(32) NOT NULL,
			observed_at DATETIME(6) NOT NULL,
			event_time TIME(6) NOT NULL,
			PRIMARY KEY (tenant, id)
		) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
			fixture.collation,
		`INSERT INTO ` + qualified + ` VALUES
			(-9, 9007199254740993, X'01', 'negative',
			 '2024-01-01 00:00:00.000001', '00:00:00.000001'),
			(1, -9223372036854775807, X'0203', 'minimum',
			 '2024-02-03 04:05:06.000007', '04:05:06.000007'),
			(1, 4, X'04', 'middle',
			 '2024-03-04 05:06:07.123456', '05:06:07.123456'),
			(3, 1, X'050607', 'later',
			 '2024-04-05 06:07:08.654321', '06:07:08.654321'),
			(3, 9007199254741000, X'08', 'maximum',
			 '2024-05-06 07:08:09.999999', '23:59:59.999999')`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"create %s range-page fixture: %v",
				fixture.name,
				err,
			)
		}
	}
	source, err := openMySQLSourceAdapter(
		ctx,
		mysqlNativeTargetEndpoint(t, parsed, caPath),
	)
	if err != nil {
		t.Fatalf("open %s range-page source: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close %s range-page source: %v", fixture.name, err)
		}
	})
	assertAdapterNetworkRangePageLive(
		t,
		ctx,
		source,
		tableName,
		[][]int64{
			{-9, 9_007_199_254_740_993},
			{1, -9_223_372_036_854_775_807},
			{1, 4},
			{3, 1},
			{3, 9_007_199_254_741_000},
		},
	)
}

func TestSQLServerNetworkRangePageLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA " +
				"to run the SQL Server range-page sentinel",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("sqlserver", dsn)
	if err != nil {
		t.Fatalf("open SQL Server range-page setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close SQL Server range-page setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server range-page setup: %T", err)
	}
	tableName := "dmtx_range_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	primaryKey := tableName + "_pk"
	qualified := sqlServerQualified("dbo", tableName)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+qualified,
		); err != nil {
			t.Errorf("drop SQL Server range-page table: %v", err)
		}
	})
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			[id] BIGINT NOT NULL,
			[payload] VARBINARY(16) NOT NULL,
			[label] VARCHAR(32)
				COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			[amount] DECIMAL(18,4) NOT NULL,
			[event_id] UNIQUEIDENTIFIER NOT NULL,
			[event_time] TIME(6) NOT NULL,
			CONSTRAINT ` + sqlServerIdentifier(primaryKey) +
			` PRIMARY KEY CLUSTERED ([id])
		)`,
		`INSERT INTO ` + qualified + ` VALUES
			(-9, 0x01, 'negative', -9.0001,
			 '00112233-4455-6677-8899-aabbccddeeff',
			 '00:00:00.000001'),
			(1, 0x0203, 'one', 1.2500,
			 '11112233-4455-6677-8899-aabbccddeeff',
			 '04:05:06.000007'),
			(9007199254740993, 0x04, 'large', 42.1234,
			 '22112233-4455-6677-8899-aabbccddeeff',
			 '12:13:14.123456'),
			(9007199254741000, 0x050607, 'maximum', 99999999999999.9999,
			 '33112233-4455-6677-8899-aabbccddeeff',
			 '23:59:59.999999')`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create SQL Server range-page fixture: %v", err)
		}
	}
	source, err := openSQLServerSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server range-page source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQL Server range-page source: %v", err)
		}
	})
	assertAdapterNetworkRangePageLive(
		t,
		ctx,
		source,
		tableName,
		[][]int64{
			{-9},
			{1},
			{9_007_199_254_740_993},
			{9_007_199_254_741_000},
		},
	)
}

func assertAdapterNetworkRangePageLive(
	t *testing.T,
	ctx context.Context,
	source sourceAdapter,
	tableName string,
	wantKeys [][]int64,
) {
	t.Helper()
	table, err := source.InspectTable(ctx, tableName)
	if err != nil {
		t.Fatalf("inspect network range-page table: %v", err)
	}
	planner, err := requirePaginationSourceAdapter(source)
	if err != nil {
		t.Fatal(err)
	}
	pagination, err := planner.PlanPagination(ctx, table, 1)
	if err != nil {
		t.Fatalf("plan network range-page table: %v", err)
	}
	if pagination.Strategy != PaginationIntegerKeyset &&
		pagination.Strategy != PaginationTupleKeyset ||
		len(pagination.Ranges) != 1 ||
		pagination.Ranges[0].Empty {
		t.Fatalf("network range-page plan = %#v", pagination)
	}
	columns := adapterColumnNames(table)
	evidence, err := planAdapterSourceRetainedRowWidth(
		ctx,
		source,
		table,
		columns,
	)
	if err != nil {
		t.Fatalf("plan network range retained width: %v", err)
	}
	capability, err := requireAdapterNetworkRangePageSource(source)
	if err != nil {
		t.Fatal(err)
	}
	request := NetworkReadRequest{
		Range: NetworkRangePlan{
			RangeIndex:   0,
			TableSchema:  table.Schema,
			TableName:    table.Name,
			TopologyHash: "live-network-topology",
			Pagination:   pagination.Strategy,
			MaxRowBytes:  evidence.UpperBoundBytes,
		},
		MaxRows: 1,
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := capability.ReadNetworkRangePage(
		canceled,
		table,
		columns,
		pagination,
		pagination.Ranges[0],
		request,
	); err == nil {
		t.Fatal("canceled network range read succeeded")
	}
	var gotKeys [][]int64
	var firstRequest NetworkReadRequest
	var firstPage NetworkReadPage
	for pageIndex := 0; pageIndex < 16; pageIndex++ {
		page, err := capability.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			pagination,
			pagination.Ranges[0],
			request,
		)
		if err != nil {
			t.Fatalf("read network range page %d: %v", pageIndex, err)
		}
		if len(page.Rows) == 0 {
			t.Fatalf("network range page %d was unexpectedly empty", pageIndex)
		}
		if len(page.Fingerprint) != 64 ||
			len(page.RowBytes) != len(page.Rows) {
			t.Fatalf("network range page %d facts = %#v", pageIndex, page)
		}
		var exactTotal int64
		for rowIndex, row := range page.Rows {
			assertAdapterRangePageLiveValueShapes(
				t,
				source.Engine(),
				columns,
				row,
			)
			exact, err := measureAdapterRetainedRowBytes(row)
			if err != nil {
				t.Fatal(err)
			}
			if page.RowBytes[rowIndex] != exact ||
				exact > evidence.UpperBoundBytes {
				t.Fatalf(
					"network page %d row %d retained=%d exact=%d bound=%d",
					pageIndex,
					rowIndex,
					page.RowBytes[rowIndex],
					exact,
					evidence.UpperBoundBytes,
				)
			}
			exactTotal += exact
			key, err := adapterRangePageRowFrontier(
				row,
				adapterRangePageAdmission{
					table: table,
					keys:  pagination.Keys,
					keyIndexes: adapterRangePageLiveKeyIndexes(
						columns,
						pagination.Keys,
					),
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			gotKeys = append(gotKeys, key)
		}
		if exactTotal != page.RetainedBytes {
			t.Fatalf(
				"network page %d retained total=%d, want %d",
				pageIndex,
				page.RetainedBytes,
				exactTotal,
			)
		}
		if pageIndex == 0 {
			firstRequest = request
			firstPage = page
		}
		if page.Exhausted {
			break
		}
		assertAdapterRangePageSourceCursorReleased(t, source)
		request.Sequence++
		request.StartFrontier = page.EndFrontier
	}
	assertAdapterRangePageSourceCursorReleased(t, source)
	if !reflect.DeepEqual(gotKeys, wantKeys) {
		t.Fatalf("network range keys = %#v, want %#v", gotKeys, wantKeys)
	}
	replay := firstRequest
	replay.ReplayExpected = &NetworkIssuedChunk{
		RangeIndex:    replay.Range.RangeIndex,
		Sequence:      replay.Sequence,
		Rows:          len(firstPage.Rows),
		StartFrontier: cloneNetworkBytes(replay.StartFrontier),
		EndFrontier:   cloneNetworkBytes(firstPage.EndFrontier),
		Fingerprint:   firstPage.Fingerprint,
		Exhausted:     firstPage.Exhausted,
	}
	replay.MaxRows = len(firstPage.Rows)
	replayed, err := capability.ReadNetworkRangePage(
		ctx,
		table,
		columns,
		pagination,
		pagination.Ranges[0],
		replay,
	)
	if err != nil {
		t.Fatalf("replay first network range page: %v", err)
	}
	if replayed.Fingerprint != firstPage.Fingerprint ||
		!bytes.Equal(replayed.EndFrontier, firstPage.EndFrontier) ||
		replayed.Exhausted != firstPage.Exhausted {
		t.Fatalf(
			"network range replay changed: %#v / %#v",
			firstPage,
			replayed,
		)
	}
}

func assertAdapterRangePageLiveValueShapes(
	t *testing.T,
	engineName string,
	columns []string,
	row []any,
) {
	t.Helper()
	value := func(name string) any {
		index := columnIndex(columns, name)
		if index < 0 || index >= len(row) {
			t.Fatalf("live range-page row omits column %s", name)
		}
		return row[index]
	}
	switch engineName {
	case "postgres":
		for _, name := range []string{"payload", "label"} {
			if _, ok := value(name).(string); !ok {
				t.Fatalf(
					"PostgreSQL text %s value has type %T",
					name,
					value(name),
				)
			}
		}
	case "mysql":
		for _, name := range []string{"payload", "label"} {
			if _, ok := value(name).([]byte); !ok {
				t.Fatalf(
					"MySQL-family byte-backed %s value has type %T",
					name,
					value(name),
				)
			}
		}
		if _, ok := value("observed_at").(time.Time); !ok {
			t.Fatalf(
				"MySQL-family datetime value has type %T",
				value("observed_at"),
			)
		}
		if _, ok := value("event_time").(string); !ok {
			t.Fatalf(
				"MySQL-family time value has type %T",
				value("event_time"),
			)
		}
	case "mssql":
		if _, ok := value("payload").([]byte); !ok {
			t.Fatalf(
				"SQL Server binary value has type %T",
				value("payload"),
			)
		}
		if _, ok := value("label").(string); !ok {
			t.Fatalf(
				"SQL Server text value has type %T",
				value("label"),
			)
		}
		for _, name := range []string{
			"amount",
			"event_id",
			"event_time",
		} {
			if _, ok := value(name).(string); !ok {
				t.Fatalf(
					"SQL Server normalized %s value has type %T",
					name,
					value(name),
				)
			}
		}
	}
}

func assertAdapterRangePageSourceCursorReleased(
	t *testing.T,
	source sourceAdapter,
) {
	t.Helper()
	switch typed := source.(type) {
	case *relationalSourceAdapter:
		if inUse := typed.database.Stats().InUse; inUse != 0 {
			t.Fatalf("range-page source retains %d database connection(s)", inUse)
		}
	case *sqliteSourceAdapter:
		if inUse := typed.database.Stats().InUse; inUse > 1 {
			t.Fatalf("SQLite range-page source retains extra connections: %d", inUse)
		}
	}
}

func adapterRangePageLiveKeyIndexes(
	columns []string,
	keys []KeySpec,
) []int {
	result := make([]int, len(keys))
	for keyIndex, key := range keys {
		result[keyIndex] = columnIndex(columns, key.Name)
	}
	return result
}
