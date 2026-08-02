package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestPostgresRetainedRowWidthLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL retained-row sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL retained-row DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL retained-row setup: %T", err)
	}
	t.Cleanup(func() { _ = setup.Close() })
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL retained-row setup: %T", err)
	}
	namespace := "dmtx_width_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	name := "payloads"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL retained-row schema: %v", err)
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
			t.Errorf("drop PostgreSQL retained-row schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, name)
	if _, err := setup.ExecContext(ctx, `
		CREATE TABLE `+qualified+` (
			id BIGINT PRIMARY KEY,
			label VARCHAR(8) NOT NULL,
			amount NUMERIC(8,2) NOT NULL,
			ratio REAL NOT NULL,
			enabled BOOLEAN NOT NULL,
			external_id UUID NOT NULL,
			note TEXT,
			payload BYTEA,
			document JSONB,
			created_on DATE NOT NULL,
			local_time TIME(6) NOT NULL,
			updated_at TIMESTAMP(6) NOT NULL
		);
		INSERT INTO `+qualified+` VALUES
			(
				1,
				'界',
				-1234.50,
				-12.5,
				true,
				'6f9619ff-8b86-d011-b42d-00c04fc964ff',
				'naïve café 東京',
				decode('00ff1020', 'hex'),
				'{"source":"postgres","items":[1,2,3]}'::jsonb,
				'2026-07-30',
				'23:59:59.123456',
				'2026-07-30 12:34:56.123456'
			),
			(
				2,
				'short',
				0.00,
				0.5,
				false,
				'ff19966f-868b-11d0-b42d-00c04fc964ff',
				NULL,
				NULL,
				NULL,
				'2026-07-31',
				'00:00:00',
				'2026-07-31 00:00:00'
			)
	`); err != nil {
		t.Fatalf("create PostgreSQL retained-row fixture: %v", err)
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
		t.Fatalf("open PostgreSQL retained-row source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	table, err := source.InspectTable(ctx, name)
	if err != nil {
		t.Fatalf("inspect PostgreSQL retained-row fixture: %v", err)
	}
	allColumns := adapterColumnNames(table)
	assertMutableLiveSourceRejectsDynamicRetainedBound(
		t,
		ctx,
		source,
		table,
		allColumns,
	)
	bounded := []string{
		"id",
		"label",
		"amount",
		"ratio",
		"enabled",
		"external_id",
		"created_on",
		"local_time",
		"updated_at",
	}
	boundedRows := assertLiveSourceRowsWithinRetainedBound(
		t,
		ctx,
		source,
		table,
		bounded,
	)
	assertPostgresRetainedDriverShapes(t, bounded, boundedRows)
	assertPostgresStableRetainedRowWidthConcurrentGrowth(
		t,
		ctx,
		setup,
		source,
		table,
		allColumns,
		qualified,
	)
}

func TestMySQLRetainedRowWidthLiveTLS(t *testing.T) {
	testMySQLFamilyRetainedRowWidthLiveTLS(t, mysqlRetainedLiveFixture{
		name:                "MySQL",
		dsnEnv:              "DMTX_TEST_MYSQL_DSN",
		caEnv:               "DMTX_TEST_MYSQL_CA",
		tlsConfig:           "dmtx_test",
		collation:           "utf8mb4_0900_bin",
		maximumNegativeTime: "-838:59:59.000000",
	})
}

func TestMariaDBRetainedRowWidthLiveTLS(t *testing.T) {
	testMySQLFamilyRetainedRowWidthLiveTLS(t, mysqlRetainedLiveFixture{
		name:                "MariaDB",
		dsnEnv:              "DMTX_TEST_MARIADB_DSN",
		caEnv:               "DMTX_TEST_MARIADB_CA",
		tlsConfig:           "dmtx_mariadb_test",
		collation:           "utf8mb4_nopad_bin",
		maximumNegativeTime: "-838:59:59.999999",
	})
}

type mysqlRetainedLiveFixture struct {
	name                string
	dsnEnv              string
	caEnv               string
	tlsConfig           string
	collation           string
	maximumNegativeTime string
}

func testMySQLFamilyRetainedRowWidthLiveTLS(
	t *testing.T,
	fixture mysqlRetainedLiveFixture,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || caPath == "" {
		t.Skip(
			"set " + fixture.dsnEnv + " and " + fixture.caEnv +
				" to run the " + fixture.name +
				" retained-row sentinel",
		)
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"retained-row source",
		dsn,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s retained-row setup: %T", fixture.name, err)
	}
	t.Cleanup(func() { _ = setup.Close() })
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping %s retained-row setup: %T", fixture.name, err)
	}
	name := "dmtx_width_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	qualified := mySQLQualified(parsed.DBName, name)
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
			t.Errorf("drop %s retained-row table: %v", fixture.name, err)
		}
	})
	statements := []string{
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			label VARCHAR(8) NOT NULL,
			amount DECIMAL(8,2) NOT NULL,
			fixed_payload VARBINARY(8) NOT NULL,
			note LONGTEXT NULL,
			payload LONGBLOB NULL,
			document JSON NULL,
			created_on DATE NOT NULL,
			local_time TIME(6) NOT NULL,
			updated_at DATETIME(6) NOT NULL
		) ENGINE=InnoDB
		  DEFAULT CHARACTER SET utf8mb4
		  COLLATE ` + fixture.collation + `
		  ROW_FORMAT=DYNAMIC`,
		`INSERT INTO ` + qualified + ` VALUES
			(
				1,
				'界',
				-1234.50,
				UNHEX('00ff1020'),
				'naïve café 東京',
				UNHEX('00ff1020'),
				JSON_OBJECT('source', '` + fixture.name + `', 'items', JSON_ARRAY(1,2,3)),
				'2026-07-30',
				'23:59:59.123456',
				'2026-07-30 12:34:56.123456'
			),
			(
				2,
				'short',
				0.00,
				UNHEX('10'),
				NULL,
				NULL,
				NULL,
				'2026-07-31',
				'00:00:00',
				'2026-07-31 00:00:00'
			)`,
	}
	for _, statement := range statements {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"create %s retained-row fixture: %v",
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
		t.Fatalf("open %s retained-row source: %v", fixture.name, err)
	}
	t.Cleanup(func() { _ = source.Close() })
	table, err := source.InspectTable(ctx, name)
	if err != nil {
		t.Fatalf("inspect %s retained-row fixture: %v", fixture.name, err)
	}
	assertMutableLiveSourceRejectsDynamicRetainedBound(
		t,
		ctx,
		source,
		table,
		adapterColumnNames(table),
	)
	assertTableStableLiveSourceRowsWithinRetainedBound(
		t,
		ctx,
		source,
		table,
		adapterColumnNames(table),
	)
	assertLiveSourceRowsWithinRetainedBound(
		t,
		ctx,
		source,
		table,
		[]string{
			"id",
			"label",
			"amount",
			"fixed_payload",
			"created_on",
			"local_time",
			"updated_at",
		},
	)
	assertMySQLFamilyMaximumNegativeTimeRetainedBound(
		t,
		ctx,
		setup,
		source,
		table,
		qualified,
		fixture.maximumNegativeTime,
	)
}

func assertMySQLFamilyMaximumNegativeTimeRetainedBound(
	t *testing.T,
	ctx context.Context,
	setup *sql.DB,
	source sourceAdapter,
	table schema.Table,
	qualified string,
	value string,
) {
	t.Helper()
	if value == "" {
		t.Fatal("maximum negative MySQL-family TIME fixture is required")
	}
	if _, err := setup.ExecContext(
		ctx,
		"UPDATE "+qualified+" SET local_time = ? WHERE id = 1",
		value,
	); err != nil {
		t.Fatalf("write maximum negative MySQL-family TIME: %v", err)
	}
	var (
		raw    string
		length int
	)
	if err := setup.QueryRowContext(
		ctx,
		"SELECT CAST(local_time AS CHAR), "+
			"OCTET_LENGTH(CAST(local_time AS CHAR)) FROM "+
			qualified+" WHERE id = 1",
	).Scan(&raw, &length); err != nil {
		t.Fatalf("read maximum negative MySQL-family TIME: %v", err)
	}
	if raw != value || length != 17 || len(raw) != 17 {
		t.Fatalf(
			"maximum negative MySQL-family TIME = %q bytes=%d, want %q/17",
			raw,
			length,
			value,
		)
	}
	var timeColumn *schema.Column
	for index := range table.Columns {
		if table.Columns[index].Name == "local_time" {
			timeColumn = &table.Columns[index]
			break
		}
	}
	if timeColumn == nil {
		t.Fatal("MySQL-family retained fixture omits local_time")
	}
	bound, err := mySQLRetainedColumnBound(*timeColumn)
	if err != nil {
		t.Fatalf("plan maximum negative MySQL-family TIME: %v", err)
	}
	want := int64(unsafe.Sizeof([]byte(nil))) + int64(length)
	if bound.fixedBytes != want {
		t.Fatalf(
			"MySQL-family TIME retained bytes = %d, want %d",
			bound.fixedBytes,
			want,
		)
	}
	rows, err := source.OpenRows(
		ctx,
		table,
		[]string{"local_time"},
	)
	if err != nil {
		t.Fatalf("open maximum negative MySQL-family TIME row: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf(
			"read maximum negative MySQL-family TIME row: %v",
			rows.Err(),
		)
	}
	var scanned any
	err = rows.Scan(&scanned)
	if err == nil || !strings.Contains(err.Error(), "invalid time value") {
		t.Fatalf(
			"maximum negative MySQL-family TIME scan error = %v, want fail-closed conversion",
			err,
		)
	}
}

func TestSQLServerRetainedRowWidthLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA " +
				"to run the SQL Server retained-row sentinel",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	setup, err := engine.OpenSQLServer2022Source(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server retained-row setup: %v", err)
	}
	t.Cleanup(func() { _ = setup.Close() })
	name := "dmtx_width_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	qualified := sqlServerQualified("dbo", name)
	primaryKey := sqlServerIdentifier(name + "_pk")
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
			t.Errorf("drop SQL Server retained-row table: %v", err)
		}
	})
	if _, err := setup.ExecContext(ctx, `
		CREATE TABLE `+qualified+` (
			[id] BIGINT NOT NULL,
			[label] VARCHAR(8) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			[amount] DECIMAL(8,2) NOT NULL,
			[fixed_payload] VARBINARY(8) NOT NULL,
			[note] VARCHAR(MAX) COLLATE Latin1_General_100_BIN2_UTF8 NULL,
			[payload] VARBINARY(MAX) NULL,
			[created_on] DATE NOT NULL,
			[local_time] TIME(6) NOT NULL,
			[updated_at] DATETIME2(6) NOT NULL,
			[external_id] UNIQUEIDENTIFIER NOT NULL,
			CONSTRAINT `+primaryKey+` PRIMARY KEY CLUSTERED ([id])
		);
		INSERT INTO `+qualified+` VALUES
			(
				1,
				'wide',
				-1234.50,
				0x00FF1020,
				'retained source payload',
				0x00FF1020,
				'2026-07-30',
				'23:59:59.123456',
				'2026-07-30T12:34:56.123456',
				CONVERT(uniqueidentifier, '6F9619FF-8B86-D011-B42D-00C04FC964FF')
			),
			(
				2,
				'short',
				0.00,
				0x10,
				NULL,
				NULL,
				'2026-07-31',
				'00:00:00',
				'2026-07-31T00:00:00',
				CONVERT(uniqueidentifier, 'FF19966F-868B-11D0-B42D-00C04FC964FF')
			)
	`); err != nil {
		t.Fatalf("create SQL Server retained-row fixture: %v", err)
	}
	source, err := openSQLServerSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server retained-row source: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	table, err := source.InspectTable(ctx, name)
	if err != nil {
		t.Fatalf("inspect SQL Server retained-row fixture: %v", err)
	}
	assertMutableLiveSourceRejectsDynamicRetainedBound(
		t,
		ctx,
		source,
		table,
		adapterColumnNames(table),
	)
	assertTableStableLiveSourceRowsWithinRetainedBound(
		t,
		ctx,
		source,
		table,
		adapterColumnNames(table),
	)
	assertLiveSourceRowsWithinRetainedBound(
		t,
		ctx,
		source,
		table,
		[]string{
			"id",
			"label",
			"amount",
			"fixed_payload",
			"created_on",
			"local_time",
			"updated_at",
			"external_id",
		},
	)
}

func assertMutableLiveSourceRejectsDynamicRetainedBound(
	t *testing.T,
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
	columns []string,
) {
	t.Helper()
	if _, err := planAdapterSourceRetainedRowWidth(
		ctx,
		source,
		table,
		columns,
	); err == nil ||
		!strings.Contains(err.Error(), "active stable source view") {
		t.Fatalf(
			"%s mutable dynamic retained-row error = %v",
			source.DisplayName(),
			err,
		)
	}
}

func assertTableStableLiveSourceRowsWithinRetainedBound(
	t *testing.T,
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
	columns []string,
) {
	t.Helper()
	session, err := OpenAdapterStableNetworkTableSource(
		ctx,
		source,
		table,
	)
	if err != nil {
		t.Fatalf("open stable retained-row table source: %v", err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			t.Errorf("close stable retained-row table source: %v", err)
		}
	}()
	stable, err := session.Source()
	if err != nil {
		t.Fatal(err)
	}
	pagination, err := stable.PlanPagination(ctx, table, 1)
	if err != nil {
		t.Fatalf("plan stable retained-row pagination: %v", err)
	}
	evidence, err := stable.PlanRetainedRowWidth(ctx, table, columns)
	if err != nil {
		t.Fatalf("plan stable retained-row width: %v", err)
	}
	request := NetworkReadRequest{
		Range: NetworkRangePlan{
			RangeIndex:   0,
			TableSchema:  table.Schema,
			TableName:    table.Name,
			TopologyHash: "stable-retained-live",
			Pagination:   pagination.Strategy,
			MaxRowBytes:  evidence.UpperBoundBytes,
		},
		MaxRows: 1,
	}
	rows := 0
	for pageIndex := 0; pageIndex < 16; pageIndex++ {
		page, err := stable.ReadNetworkRangePage(
			ctx,
			table,
			columns,
			pagination,
			pagination.Ranges[0],
			request,
		)
		if err != nil {
			t.Fatalf(
				"read stable retained-row page %d: %v",
				pageIndex,
				err,
			)
		}
		if len(page.Rows) != 1 ||
			len(page.RowBytes) != 1 ||
			page.RowBytes[0] > evidence.UpperBoundBytes {
			t.Fatalf(
				"stable retained-row page %d = %#v, bound=%d",
				pageIndex,
				page,
				evidence.UpperBoundBytes,
			)
		}
		rows++
		if page.Exhausted {
			break
		}
		request.Sequence++
		request.StartFrontier = cloneNetworkBytes(page.EndFrontier)
	}
	if rows != 2 {
		t.Fatalf("stable retained-row pages = %d, want 2", rows)
	}
}

func assertPostgresRetainedDriverShapes(
	t *testing.T,
	columns []string,
	rows [][]any,
) {
	t.Helper()
	if err := postgresRetainedDriverShapeError(columns, rows); err != nil {
		t.Fatal(err)
	}
}

func postgresRetainedDriverShapeError(
	columns []string,
	rows [][]any,
) error {
	if len(rows) != 2 {
		return fmt.Errorf("PostgreSQL retained driver rows = %d, want 2", len(rows))
	}
	indexes := make(map[string]int, len(columns))
	for index, column := range columns {
		indexes[column] = index
	}
	for _, required := range []string{"ratio", "enabled", "external_id"} {
		if _, ok := indexes[required]; !ok {
			return fmt.Errorf(
				"PostgreSQL retained driver fixture omits %s",
				required,
			)
		}
	}
	if _, ok := rows[0][indexes["ratio"]].(float64); !ok {
		return fmt.Errorf(
			"PostgreSQL REAL driver shape = %T, want float64",
			rows[0][indexes["ratio"]],
		)
	}
	if _, ok := rows[0][indexes["enabled"]].(bool); !ok {
		return fmt.Errorf(
			"PostgreSQL BOOLEAN driver shape = %T, want bool",
			rows[0][indexes["enabled"]],
		)
	}
	uuid, ok := rows[0][indexes["external_id"]].(string)
	if !ok || len(uuid) != 36 {
		return fmt.Errorf(
			"PostgreSQL UUID driver shape = %T(%v), want 36-byte string",
			rows[0][indexes["external_id"]],
			rows[0][indexes["external_id"]],
		)
	}
	return nil
}

func assertPostgresStableRetainedRowWidthConcurrentGrowth(
	t *testing.T,
	ctx context.Context,
	setup *sql.DB,
	source sourceAdapter,
	table schema.Table,
	columns []string,
	qualified string,
) {
	t.Helper()
	relational, ok := source.(*relationalSourceAdapter)
	if !ok || relational.database == nil {
		t.Fatal("PostgreSQL retained-row source is not relational")
	}
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: table.Schema,
		Table:  table.Name,
	}
	const topology = "retained-width-live"
	attemptID, err := BuildStrictConsistencyAttemptID(task, topology, 0)
	if err != nil {
		t.Fatalf("build PostgreSQL retained strict attempt: %v", err)
	}
	opener, err := NewPostgresStrictConsistencyOpener(relational.database)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := opener.OpenStrictConsistency(
		ctx,
		StrictConsistencyOpenRequest{
			RunID:        "retained-width-live",
			SourceEngine: StrictConsistencyPostgres,
			Scope:        state.StrictSnapshotTable,
			ProcessEpoch: "retained-width-process-1",
			Tables: []StrictConsistencyTable{{
				Task:             task,
				AttemptID:        attemptID,
				WorkTopologyHash: topology,
			}},
		},
	)
	if err != nil {
		t.Fatalf("open PostgreSQL retained-row exported snapshot: %v", err)
	}
	session := raw.(*PostgresStrictConsistencySession)
	defer func() {
		closeCtx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		if err := session.Close(closeCtx); err != nil {
			t.Errorf("close PostgreSQL retained-row snapshot: %v", err)
		}
	}()

	err = session.RunReader(
		ctx,
		task,
		func(
			readerCtx context.Context,
			queryer PostgresStrictSnapshotQueryer,
		) error {
			stable, err := newPostgresAdapterRetainedStableRelationalView(
				source,
				queryer,
			)
			if err != nil {
				return err
			}
			evidence, err := stable.PlanRetainedRowWidth(
				readerCtx,
				table,
				columns,
			)
			if err != nil {
				return err
			}

			writeCtx, cancel := context.WithTimeout(readerCtx, 5*time.Second)
			defer cancel()
			writeDone := make(chan error, 1)
			go func() {
				_, updateErr := setup.ExecContext(
					writeCtx,
					"UPDATE "+qualified+
						" SET note = repeat('x', 131072) WHERE id = 1",
				)
				writeDone <- updateErr
			}()
			select {
			case updateErr := <-writeDone:
				if updateErr != nil {
					return fmt.Errorf(
						"grow PostgreSQL retained source concurrently: %w",
						updateErr,
					)
				}
			case <-writeCtx.Done():
				return fmt.Errorf(
					"concurrent PostgreSQL retained source growth blocked: %w",
					writeCtx.Err(),
				)
			}

			if err := assertPostgresStableRetainedRangePages(
				readerCtx,
				stable,
				table,
				columns,
				evidence.UpperBoundBytes,
			); err != nil {
				return err
			}
			stream, err := stable.OpenRows(readerCtx, table, columns)
			if err != nil {
				return err
			}
			rows, err := collectRetainedRowsWithinBound(
				stream,
				len(columns),
				evidence.UpperBoundBytes,
			)
			if err != nil {
				return err
			}
			if err := postgresRetainedDriverShapeError(
				columns,
				rows,
			); err != nil {
				return err
			}
			noteIndex := -1
			for index, column := range columns {
				if column == "note" {
					noteIndex = index
					break
				}
			}
			if noteIndex < 0 {
				return errors.New("PostgreSQL retained fixture omits note")
			}
			note, ok := rows[0][noteIndex].(string)
			if !ok || len(note) >= 131072 {
				return fmt.Errorf(
					"exported snapshot observed concurrent growth: %T len=%d",
					rows[0][noteIndex],
					len(note),
				)
			}
			t.Logf(
				"PostgreSQL stable retained-row bound: columns=%d peak=%d rows=%d",
				len(columns),
				evidence.UpperBoundBytes,
				len(rows),
			)
			return nil
		},
	)
	if err != nil {
		t.Fatalf("read PostgreSQL retained-row exported snapshot: %v", err)
	}
	var freshLength int
	if err := setup.QueryRowContext(
		ctx,
		"SELECT octet_length(note) FROM "+qualified+" WHERE id = 1",
	).Scan(&freshLength); err != nil {
		t.Fatalf("read fresh PostgreSQL retained source length: %v", err)
	}
	if freshLength != 131072 {
		t.Fatalf(
			"fresh PostgreSQL retained source length = %d, want 131072",
			freshLength,
		)
	}
}

func assertPostgresStableRetainedRangePages(
	ctx context.Context,
	stable *adapterRetainedStableRelationalView,
	table schema.Table,
	columns []string,
	upperBound int64,
) error {
	pagination, err := stable.PlanPagination(ctx, table, 1)
	if err != nil {
		return fmt.Errorf(
			"plan PostgreSQL retained snapshot pagination: %w",
			err,
		)
	}
	if pagination.Strategy != PaginationIntegerKeyset ||
		len(pagination.Ranges) != 1 {
		return fmt.Errorf(
			"PostgreSQL retained snapshot pagination is malformed",
		)
	}
	request := NetworkReadRequest{
		Range: NetworkRangePlan{
			RangeIndex:   0,
			TableSchema:  table.Schema,
			TableName:    table.Name,
			TopologyHash: strings.Repeat("b", 64),
			Pagination:   pagination.Strategy,
			MaxRowBytes:  upperBound,
		},
		MaxRows: 1,
	}
	first, err := stable.ReadNetworkRangePage(
		ctx,
		table,
		columns,
		pagination,
		pagination.Ranges[0],
		request,
	)
	if err != nil {
		return fmt.Errorf(
			"read first PostgreSQL retained snapshot range page: %w",
			err,
		)
	}
	if len(first.Rows) != 1 ||
		len(first.RowBytes) != 1 ||
		first.RowBytes[0] > upperBound ||
		first.Exhausted ||
		first.Fingerprint == "" {
		return fmt.Errorf(
			"first PostgreSQL retained snapshot range page is malformed",
		)
	}
	noteIndex := -1
	for index, column := range columns {
		if column == "note" {
			noteIndex = index
			break
		}
	}
	if noteIndex < 0 {
		return errors.New("PostgreSQL retained fixture omits note")
	}
	note, ok := first.Rows[0][noteIndex].(string)
	if !ok || len(note) >= 131072 {
		return fmt.Errorf(
			"snapshot range page observed concurrent growth: %T len=%d",
			first.Rows[0][noteIndex],
			len(note),
		)
	}

	request.Sequence = 1
	request.StartFrontier = first.EndFrontier
	second, err := stable.ReadNetworkRangePage(
		ctx,
		table,
		columns,
		pagination,
		pagination.Ranges[0],
		request,
	)
	if err != nil {
		return fmt.Errorf(
			"read second PostgreSQL retained snapshot range page: %w",
			err,
		)
	}
	if len(second.Rows) != 1 ||
		len(second.RowBytes) != 1 ||
		second.RowBytes[0] > upperBound ||
		!second.Exhausted ||
		second.Fingerprint == "" {
		return fmt.Errorf(
			"second PostgreSQL retained snapshot range page is malformed",
		)
	}
	return nil
}

func collectRetainedRowsWithinBound(
	rows adapterRows,
	columnCount int,
	upperBound int64,
) ([][]any, error) {
	defer rows.Close()
	values := make([]any, columnCount)
	destinations := make([]any, columnCount)
	for index := range values {
		destinations[index] = &values[index]
	}
	owned := make([][]any, 0, 2)
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		row := cloneAdapterRow(values)
		retained, err := measureAdapterRetainedRowBytes(row)
		if err != nil {
			return nil, err
		}
		if retained <= 0 || retained > upperBound {
			return nil, fmt.Errorf(
				"stable retained row bytes = %d, bound = %d",
				retained,
				upperBound,
			)
		}
		owned = append(owned, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(owned) != 2 {
		return nil, fmt.Errorf("stable retained rows = %d, want 2", len(owned))
	}
	return owned, nil
}

func assertLiveSourceRowsWithinRetainedBound(
	t *testing.T,
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
	columns []string,
) [][]any {
	t.Helper()
	evidence, err := planAdapterSourceRetainedRowWidth(
		ctx,
		source,
		table,
		columns,
	)
	if err != nil {
		t.Fatalf("plan retained-row fixture %s: %v", table.Name, err)
	}
	if evidence.UpperBoundBytes <= 0 {
		t.Fatalf(
			"retained-row fixture %s returned invalid evidence: %#v",
			table.Name,
			evidence,
		)
	}
	rows, err := source.OpenRows(ctx, table, columns)
	if err != nil {
		t.Fatalf("open retained-row fixture %s: %v", table.Name, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close retained-row fixture %s: %v", table.Name, err)
		}
	}()
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	count := 0
	ownedRows := make([][]any, 0, 2)
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			t.Fatalf("scan retained-row fixture %s: %v", table.Name, err)
		}
		row := cloneAdapterRow(values)
		retained, err := measureAdapterRetainedRowBytes(row)
		if err != nil {
			t.Fatalf("measure retained-row fixture %s: %v", table.Name, err)
		}
		if retained > evidence.UpperBoundBytes {
			t.Fatalf(
				"%s fixture row %d retained bytes = %d, admitted bound = %d",
				source.DisplayName(),
				count,
				retained,
				evidence.UpperBoundBytes,
			)
		}
		ownedRows = append(ownedRows, row)
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate retained-row fixture %s: %v", table.Name, err)
	}
	if count != 2 {
		t.Fatalf(
			"%s retained-row fixture rows = %d, want 2",
			source.DisplayName(),
			count,
		)
	}
	t.Logf(
		"%s retained-row bound: table=%s columns=%d max=%d rows=%d",
		source.DisplayName(),
		fmt.Sprintf("%s.%s", table.Schema, table.Name),
		len(columns),
		evidence.UpperBoundBytes,
		count,
	)
	return ownedRows
}
