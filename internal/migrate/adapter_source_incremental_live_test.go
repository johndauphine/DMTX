package migrate

import (
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
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresIncrementalWindowLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL incremental source sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL incremental DSN: %T", err)
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
		t.Fatalf("open PostgreSQL incremental setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close PostgreSQL incremental setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL incremental setup: %T", err)
	}
	namespace := "dmtx_inc_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "events"
	emptyName := "empty_events"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL incremental schema: %v", err)
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
			t.Errorf("drop PostgreSQL incremental schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	emptyQualified := postgresQualified(namespace, emptyName)
	if _, err := setup.ExecContext(ctx, `
		CREATE TABLE `+qualified+` (
			tenant_id bigint NOT NULL,
			id bigint NOT NULL,
			updated_at timestamp(3),
			payload text NOT NULL,
			PRIMARY KEY (tenant_id, id)
		);
		INSERT INTO `+qualified+` VALUES
			(1, 1, NULL, 'null'),
			(1, 2, timestamp '2026-07-30 12:00:00.000', 'equal lower'),
			(1, 3, timestamp '2026-07-30 12:00:01.000', 'inside'),
			(1, 4, timestamp '2026-07-30 12:00:02.000', 'equal upper');
		CREATE TABLE `+emptyQualified+` (
			id bigint NOT NULL PRIMARY KEY,
			updated_at timestamp(3),
			payload text NOT NULL
		);
		INSERT INTO `+emptyQualified+` VALUES (1, NULL, 'null only');
	`); err != nil {
		t.Fatalf("create PostgreSQL incremental fixture: %v", err)
	}
	endpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	source, err := openPostgresSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open PostgreSQL incremental source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close PostgreSQL incremental source: %v", err)
		}
	})
	assertAdapterIncrementalWindowLive(
		t,
		ctx,
		source,
		tableName,
		emptyName,
		func() error {
			_, err := setup.ExecContext(
				ctx,
				"INSERT INTO "+qualified+
					" VALUES (1, 5, timestamp '2026-07-30 12:00:03.000', 'after fence')",
			)
			return err
		},
	)
}

func TestMySQLIncrementalWindowLive(t *testing.T) {
	testMySQLFamilyIncrementalWindowLive(
		t,
		mysqlIncrementalLiveFixture{
			name:       "MySQL",
			dsnEnv:     "DMTX_TEST_MYSQL_DSN",
			caEnv:      "DMTX_TEST_MYSQL_CA",
			tlsConfig:  "dmtx_test",
			collation:  "utf8mb4_0900_bin",
			wantFlavor: engine.MySQLServerFlavorOracle80,
		},
	)
}

func TestMariaDBIncrementalWindowLive(t *testing.T) {
	testMySQLFamilyIncrementalWindowLive(
		t,
		mysqlIncrementalLiveFixture{
			name:       "MariaDB",
			dsnEnv:     "DMTX_TEST_MARIADB_DSN",
			caEnv:      "DMTX_TEST_MARIADB_CA",
			tlsConfig:  "dmtx_mariadb_test",
			collation:  "utf8mb4_nopad_bin",
			wantFlavor: engine.MySQLServerFlavorMariaDB1011,
		},
	)
}

type mysqlIncrementalLiveFixture struct {
	name       string
	dsnEnv     string
	caEnv      string
	tlsConfig  string
	collation  string
	wantFlavor engine.MySQLServerFlavor
}

func testMySQLFamilyIncrementalWindowLive(
	t *testing.T,
	fixture mysqlIncrementalLiveFixture,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || caPath == "" {
		t.Skip(
			"set " + fixture.dsnEnv + " and " + fixture.caEnv +
				" to run the " + fixture.name +
				" incremental source sentinel",
		)
	}
	registerMySQLCommonFixtureTLSNamed(
		t,
		caPath,
		fixture.tlsConfig,
	)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"incremental source",
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
		t.Fatalf("open %s incremental setup: %T", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close %s incremental setup: %v", fixture.name, err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping %s incremental setup: %T", fixture.name, err)
	}
	flavor, err := engine.DetectMySQLServerFlavor(ctx, setup)
	if err != nil {
		t.Fatalf("detect %s incremental flavor: %v", fixture.name, err)
	}
	if flavor != fixture.wantFlavor {
		t.Fatalf(
			"%s live flavor = %v, want %v",
			fixture.name,
			flavor,
			fixture.wantFlavor,
		)
	}
	prefix := "dmtx_inc_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := prefix + "_events"
	emptyName := prefix + "_empty"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, name := range []string{emptyName, tableName} {
			if _, err := setup.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+mySQLIdentifier(name),
			); err != nil {
				t.Errorf(
					"drop %s incremental table %s: %v",
					fixture.name,
					name,
					err,
				)
			}
		}
	})
	qualified := mySQLQualified(parsed.DBName, tableName)
	emptyQualified := mySQLQualified(parsed.DBName, emptyName)
	setupStatements := []struct {
		name string
		sql  string
	}{
		{
			name: "source table",
			sql: `
				CREATE TABLE ` + qualified + ` (
					tenant_id BIGINT NOT NULL,
					id BIGINT NOT NULL,
					updated_at DATETIME(3) NULL,
					payload VARCHAR(32) NOT NULL,
					PRIMARY KEY (tenant_id, id)
				) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
				fixture.collation,
		},
		{
			name: "source rows",
			sql: `
				INSERT INTO ` + qualified + ` VALUES
					(1, 1, NULL, 'null'),
					(1, 2, '2026-07-30 12:00:00.000', 'equal lower'),
					(1, 3, '2026-07-30 12:00:01.000', 'inside'),
					(1, 4, '2026-07-30 12:00:02.000', 'equal upper')`,
		},
		{
			name: "empty table",
			sql: `
				CREATE TABLE ` + emptyQualified + ` (
					id BIGINT NOT NULL PRIMARY KEY,
					updated_at DATETIME(3) NULL,
					payload VARCHAR(32) NOT NULL
				) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
				fixture.collation,
		},
		{
			name: "empty rows",
			sql: `
				INSERT INTO ` + emptyQualified +
				` VALUES (1, NULL, 'null only')`,
		},
	}
	for _, statement := range setupStatements {
		if _, err := setup.ExecContext(ctx, statement.sql); err != nil {
			t.Fatalf(
				"create %s incremental fixture %s: %v",
				fixture.name,
				statement.name,
				err,
			)
		}
	}
	endpoint := mysqlNativeTargetEndpoint(t, parsed, caPath)
	source, err := openMySQLSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open %s incremental source: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close %s incremental source: %v", fixture.name, err)
		}
	})
	assertAdapterIncrementalWindowLive(
		t,
		ctx,
		source,
		tableName,
		emptyName,
		func() error {
			_, err := setup.ExecContext(
				ctx,
				"INSERT INTO "+qualified+
					" VALUES (1, 5, '2026-07-30 12:00:03.000', 'after fence')",
			)
			return err
		},
	)
}

func TestSQLServerIncrementalWindowLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA " +
				"to run the SQL Server incremental source sentinel",
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
		t.Fatalf("open SQL Server incremental setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close SQL Server incremental setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server incremental setup: %T", err)
	}
	prefix := "dmtx_inc_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := prefix + "_events"
	emptyName := prefix + "_empty"
	pkName := prefix + "_pk"
	emptyPKName := prefix + "_empty_pk"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, name := range []string{emptyName, tableName} {
			if _, err := setup.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+
					sqlServerQualified("dbo", name),
			); err != nil {
				t.Errorf(
					"drop SQL Server incremental table %s: %v",
					name,
					err,
				)
			}
		}
	})
	qualified := sqlServerQualified("dbo", tableName)
	emptyQualified := sqlServerQualified("dbo", emptyName)
	if _, err := setup.ExecContext(ctx, `
		CREATE TABLE `+qualified+` (
			[tenant_id] BIGINT NOT NULL,
			[id] BIGINT NOT NULL,
			[updated_at] DATETIME2(3) NULL,
			[payload] VARCHAR(32)
				COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			CONSTRAINT `+sqlServerIdentifier(pkName)+`
				PRIMARY KEY CLUSTERED ([tenant_id], [id])
		);
		INSERT INTO `+qualified+` VALUES
			(1, 1, NULL, 'null'),
			(1, 2, CONVERT(datetime2(3), '2026-07-30T12:00:00.000'), 'equal lower'),
			(1, 3, CONVERT(datetime2(3), '2026-07-30T12:00:01.000'), 'inside'),
			(1, 4, CONVERT(datetime2(3), '2026-07-30T12:00:02.000'), 'equal upper');
		CREATE TABLE `+emptyQualified+` (
			[id] BIGINT NOT NULL,
			[updated_at] DATETIME2(3) NULL,
			[payload] VARCHAR(32)
				COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			CONSTRAINT `+sqlServerIdentifier(emptyPKName)+`
				PRIMARY KEY CLUSTERED ([id])
		);
		INSERT INTO `+emptyQualified+` VALUES (1, NULL, 'null only');
	`); err != nil {
		t.Fatalf("create SQL Server incremental fixture: %v", err)
	}
	source, err := openSQLServerSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server incremental source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQL Server incremental source: %v", err)
		}
	})
	assertAdapterIncrementalWindowLive(
		t,
		ctx,
		source,
		tableName,
		emptyName,
		func() error {
			_, err := setup.ExecContext(
				ctx,
				"INSERT INTO "+qualified+
					" VALUES (1, 5, CONVERT(datetime2(3), '2026-07-30T12:00:03.000'), 'after fence')",
			)
			return err
		},
	)
}

func assertAdapterIncrementalWindowLive(
	t *testing.T,
	ctx context.Context,
	source sourceAdapter,
	tableName string,
	emptyName string,
	insertAfterFence func() error,
) {
	t.Helper()
	incremental, err := requireIncrementalSourceAdapter(source)
	if err != nil {
		t.Fatal(err)
	}
	table, err := source.InspectTable(ctx, tableName)
	if err != nil {
		t.Fatalf("inspect incremental source table: %v", err)
	}
	mapped, err := incremental.IncrementalTable(table)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildIncrementalTablePlan(
		mapped,
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.DateColumn == nil ||
		plan.DateColumn.TemporalKind != IncrementalTemporalTimestamp {
		t.Fatalf("incremental date column = %#v", plan.DateColumn)
	}
	upper, err := incremental.SampleIncrementalUpperFence(
		ctx,
		table,
		*plan.DateColumn,
	)
	if err != nil {
		t.Fatalf("sample incremental upper fence: %v", err)
	}
	wantUpper := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		2,
		0,
		time.UTC,
	)
	if upper == nil || !upper.Equal(wantUpper) {
		t.Fatalf("upper fence = %v, want %v", upper, wantUpper)
	}
	if err := insertAfterFence(); err != nil {
		t.Fatalf("insert row after immutable upper fence: %v", err)
	}
	lower := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		0,
		time.UTC,
	)
	read := IncrementalReadPlan{
		Table:    mapped,
		Scope:    IncrementalReadWindow,
		Ordering: windowIncrementalOrdering(plan.Ordering),
		Window: &IncrementalWindow{
			Column:         "updated_at",
			Lower:          &lower,
			Upper:          upper,
			LowerExclusive: true,
			UpperInclusive: true,
			ExcludeNull:    true,
		},
		Resumed:                  true,
		ReplayFromLowerWatermark: true,
	}
	assertAdapterIncrementalLiveIDs(
		t,
		ctx,
		incremental,
		table,
		read,
		[]int64{3, 4},
	)
	// A resumed attempt deliberately replays the complete window.
	assertAdapterIncrementalLiveIDs(
		t,
		ctx,
		incremental,
		table,
		read,
		[]int64{3, 4},
	)

	emptyRead := read
	emptyRead.Window = &IncrementalWindow{
		Column:         "updated_at",
		Lower:          upper,
		Upper:          upper,
		LowerExclusive: true,
		UpperInclusive: true,
		ExcludeNull:    true,
		Empty:          true,
	}
	assertAdapterIncrementalLiveIDs(
		t,
		ctx,
		incremental,
		table,
		emptyRead,
		nil,
	)

	emptyTable, err := source.InspectTable(ctx, emptyName)
	if err != nil {
		t.Fatalf("inspect empty incremental source table: %v", err)
	}
	emptyMapped, err := incremental.IncrementalTable(emptyTable)
	if err != nil {
		t.Fatal(err)
	}
	emptyPlan, err := BuildIncrementalTablePlan(
		emptyMapped,
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	emptyUpper, err := incremental.SampleIncrementalUpperFence(
		ctx,
		emptyTable,
		*emptyPlan.DateColumn,
	)
	if err != nil {
		t.Fatal(err)
	}
	if emptyUpper != nil {
		t.Fatalf("all-NULL upper fence = %v, want nil", emptyUpper)
	}
}

func assertAdapterIncrementalLiveIDs(
	t *testing.T,
	ctx context.Context,
	source incrementalSourceAdapter,
	table schema.Table,
	read IncrementalReadPlan,
	want []int64,
) {
	t.Helper()
	rows, err := source.OpenIncrementalRows(
		ctx,
		table,
		[]string{"tenant_id", "id", "updated_at"},
		read,
	)
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for rows.Next() {
		var tenantID, id, updatedAt any
		if err := rows.Scan(&tenantID, &id, &updatedAt); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		rowID, ok := id.(int64)
		if !ok {
			_ = rows.Close()
			t.Fatalf("incremental id has driver type %T", id)
		}
		if _, ok := updatedAt.(time.Time); !ok {
			_ = rows.Close()
			t.Fatalf(
				"incremental timestamp wrapper returned %T",
				updatedAt,
			)
		}
		got = append(got, rowID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incremental IDs = %v, want %v", got, want)
	}
}
