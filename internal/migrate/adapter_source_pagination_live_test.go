package migrate

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

func TestPostgresPaginationPlanningLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL pagination planner sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL pagination DSN: %T", err)
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
		t.Fatalf("open PostgreSQL pagination setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close PostgreSQL pagination setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL pagination setup: %T", err)
	}

	namespace := "dmtx_page_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	integerName := "integer_ids"
	tupleName := "tuple_ids"
	textName := "text_ids"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL pagination schema: %v", err)
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
			t.Errorf("drop PostgreSQL pagination schema: %v", err)
		}
	})
	statements := []string{
		`CREATE TABLE ` + postgresQualified(namespace, integerName) + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		)`,
		`INSERT INTO ` + postgresQualified(namespace, integerName) + ` VALUES
			(9007199254740993, 'one'),
			(9007199254740994, 'two'),
			(9007199254741000, 'three')`,
		`CREATE TABLE ` + postgresQualified(namespace, tupleName) + ` (
			tenant BIGINT NOT NULL,
			id BIGINT NOT NULL,
			payload TEXT NOT NULL,
			PRIMARY KEY (tenant, id)
		)`,
		`INSERT INTO ` + postgresQualified(namespace, tupleName) + ` VALUES
			(1, 1, 'one'),
			(1, 2, 'two'),
			(2, 1, 'three'),
			(2, 2, 'four'),
			(3, 1, 'five')`,
		`CREATE TABLE ` + postgresQualified(namespace, textName) + ` (
			code TEXT NOT NULL PRIMARY KEY,
			payload TEXT NOT NULL
		)`,
		`INSERT INTO ` + postgresQualified(namespace, textName) + ` VALUES
			('a', 'one'),
			('b', 'two'),
			('c', 'three')`,
	}
	for _, statement := range statements {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create PostgreSQL pagination fixture: %v", err)
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
		t.Fatalf("open PostgreSQL pagination source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close PostgreSQL pagination source: %v", err)
		}
	})
	assertAdapterPaginationPlanningLive(
		t,
		ctx,
		source,
		integerName,
		tupleName,
		textName,
		PaginationTupleKeyset,
	)
}

func TestMySQLPaginationPlanningLive(t *testing.T) {
	testMySQLFamilyPaginationPlanningLive(
		t,
		mysqlPaginationLiveFixture{
			name:       "MySQL",
			dsnEnv:     "DMTX_TEST_MYSQL_DSN",
			caEnv:      "DMTX_TEST_MYSQL_CA",
			tlsConfig:  "dmtx_test",
			collation:  "utf8mb4_0900_bin",
			wantFlavor: engine.MySQLServerFlavorOracle80,
		},
	)
}

func TestMariaDBPaginationPlanningLive(t *testing.T) {
	testMySQLFamilyPaginationPlanningLive(
		t,
		mysqlPaginationLiveFixture{
			name:       "MariaDB",
			dsnEnv:     "DMTX_TEST_MARIADB_DSN",
			caEnv:      "DMTX_TEST_MARIADB_CA",
			tlsConfig:  "dmtx_mariadb_test",
			collation:  "utf8mb4_nopad_bin",
			wantFlavor: engine.MySQLServerFlavorMariaDB1011,
		},
	)
}

type mysqlPaginationLiveFixture struct {
	name       string
	dsnEnv     string
	caEnv      string
	tlsConfig  string
	collation  string
	wantFlavor engine.MySQLServerFlavor
}

func testMySQLFamilyPaginationPlanningLive(
	t *testing.T,
	fixture mysqlPaginationLiveFixture,
) {
	t.Helper()
	dsn := os.Getenv(fixture.dsnEnv)
	caPath := os.Getenv(fixture.caEnv)
	if dsn == "" || caPath == "" {
		t.Skip(
			"set " + fixture.dsnEnv + " and " + fixture.caEnv +
				" to run the " + fixture.name +
				" pagination planner sentinel",
		)
	}
	registerMySQLCommonFixtureTLSNamed(
		t,
		caPath,
		fixture.tlsConfig,
	)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		"pagination source",
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
		t.Fatalf("open %s pagination setup: %T", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close %s pagination setup: %v", fixture.name, err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping %s pagination setup: %T", fixture.name, err)
	}
	flavor, err := engine.DetectMySQLServerFlavor(ctx, setup)
	if err != nil {
		t.Fatalf("detect %s pagination flavor: %v", fixture.name, err)
	}
	if flavor != fixture.wantFlavor {
		t.Fatalf(
			"%s live flavor = %v, want %v",
			fixture.name,
			flavor,
			fixture.wantFlavor,
		)
	}

	prefix := "dmtx_page_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	integerName := prefix + "_integer"
	tupleName := prefix + "_tuple"
	textName := prefix + "_text"
	names := []string{integerName, tupleName, textName}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, name := range names {
			if _, err := setup.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+
					mySQLQualified(parsed.DBName, name),
			); err != nil {
				t.Errorf(
					"drop %s pagination table %s: %v",
					fixture.name,
					name,
					err,
				)
			}
		}
	})
	integerQualified := mySQLQualified(parsed.DBName, integerName)
	tupleQualified := mySQLQualified(parsed.DBName, tupleName)
	textQualified := mySQLQualified(parsed.DBName, textName)
	statements := []string{
		`CREATE TABLE ` + integerQualified + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			payload VARCHAR(16) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
			fixture.collation,
		`INSERT INTO ` + integerQualified + ` VALUES
			(9007199254740993, 'one'),
			(9007199254740994, 'two'),
			(9007199254741000, 'three')`,
		`CREATE TABLE ` + tupleQualified + ` (
			tenant BIGINT NOT NULL,
			id BIGINT NOT NULL,
			payload VARCHAR(16) NOT NULL,
			PRIMARY KEY (tenant, id)
		) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
			fixture.collation,
		`INSERT INTO ` + tupleQualified + ` VALUES
			(1, 1, 'one'),
			(1, 2, 'two'),
			(2, 1, 'three'),
			(2, 2, 'four'),
			(3, 1, 'five')`,
		`CREATE TABLE ` + textQualified + ` (
			code VARCHAR(16) NOT NULL PRIMARY KEY,
			payload VARCHAR(16) NOT NULL
		) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE ` +
			fixture.collation,
		`INSERT INTO ` + textQualified + ` VALUES
			('a', 'one'),
			('b', 'two'),
			('c', 'three')`,
	}
	for _, statement := range statements {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"create %s pagination fixture: %v",
				fixture.name,
				err,
			)
		}
	}

	endpoint := mysqlNativeTargetEndpoint(t, parsed, caPath)
	source, err := openMySQLSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open %s pagination source: %v", fixture.name, err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close %s pagination source: %v", fixture.name, err)
		}
	})
	assertAdapterPaginationPlanningLive(
		t,
		ctx,
		source,
		integerName,
		tupleName,
		textName,
		PaginationTupleKeyset,
	)
}

func TestSQLServerPaginationPlanningLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA " +
				"to run the SQL Server pagination planner sentinel",
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
		t.Fatalf("open SQL Server pagination setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close SQL Server pagination setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("ping SQL Server pagination setup: %T", err)
	}

	prefix := "dmtx_page_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	integerName := prefix + "_integer"
	tupleName := prefix + "_tuple"
	integerPK := prefix + "_integer_pk"
	tuplePK := prefix + "_tuple_pk"
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, name := range []string{tupleName, integerName} {
			if _, err := setup.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+
					sqlServerQualified("dbo", name),
			); err != nil {
				t.Errorf(
					"drop SQL Server pagination table %s: %v",
					name,
					err,
				)
			}
		}
	})
	integerQualified := sqlServerQualified("dbo", integerName)
	tupleQualified := sqlServerQualified("dbo", tupleName)
	statements := []string{
		`CREATE TABLE ` + integerQualified + ` (
			[id] BIGINT NOT NULL,
			[payload] VARCHAR(16)
				COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			CONSTRAINT ` + sqlServerIdentifier(integerPK) +
			` PRIMARY KEY CLUSTERED ([id])
		)`,
		`INSERT INTO ` + integerQualified + ` VALUES
			(9007199254740993, 'one'),
			(9007199254740994, 'two'),
			(9007199254741000, 'three')`,
		`CREATE TABLE ` + tupleQualified + ` (
			[tenant] BIGINT NOT NULL,
			[id] BIGINT NOT NULL,
			[payload] VARCHAR(16)
				COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL,
			CONSTRAINT ` + sqlServerIdentifier(tuplePK) +
			` PRIMARY KEY CLUSTERED ([tenant], [id])
		)`,
		`INSERT INTO ` + tupleQualified + ` VALUES
			(1, 1, 'one'),
			(1, 2, 'two'),
			(2, 1, 'three'),
			(2, 2, 'four'),
			(3, 1, 'five')`,
	}
	for _, statement := range statements {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"create SQL Server pagination fixture: %v",
				err,
			)
		}
	}

	source, err := openSQLServerSourceAdapter(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server pagination source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQL Server pagination source: %v", err)
		}
	})
	assertAdapterPaginationPlanningLive(
		t,
		ctx,
		source,
		integerName,
		tupleName,
		"",
		PaginationRowNumber,
	)
}

func assertAdapterPaginationPlanningLive(
	t *testing.T,
	ctx context.Context,
	source sourceAdapter,
	integerName string,
	tupleName string,
	textName string,
	tupleStrategy PaginationStrategy,
) {
	t.Helper()
	planner, err := requirePaginationSourceAdapter(source)
	if err != nil {
		t.Fatal(err)
	}

	integerTable, err := source.InspectTable(ctx, integerName)
	if err != nil {
		t.Fatalf("inspect integer pagination table: %v", err)
	}
	firstInteger, err := planner.PlanPagination(
		ctx,
		integerTable,
		3,
	)
	if err != nil {
		t.Fatalf("plan integer pagination: %v", err)
	}
	secondInteger, err := planner.PlanPagination(
		ctx,
		integerTable,
		3,
	)
	if err != nil {
		t.Fatalf("replan integer pagination: %v", err)
	}
	if firstInteger.Strategy != PaginationIntegerKeyset ||
		len(firstInteger.Ranges) != 3 ||
		firstInteger.TopologyHash == "" ||
		firstInteger.TopologyHash != secondInteger.TopologyHash {
		t.Fatalf(
			"integer pagination plans = %#v / %#v",
			firstInteger,
			secondInteger,
		)
	}
	lastInteger, err := (*firstInteger.Ranges[2].Upper)[0].SQLValue()
	if err != nil ||
		lastInteger != int64(9_007_199_254_741_000) {
		t.Fatalf(
			"integer upper bound = %#v, %v",
			lastInteger,
			err,
		)
	}

	tupleTable, err := source.InspectTable(ctx, tupleName)
	if err != nil {
		t.Fatalf("inspect tuple pagination table: %v", err)
	}
	tuplePlan, err := planner.PlanPagination(
		ctx,
		tupleTable,
		3,
	)
	if err != nil {
		t.Fatalf("plan tuple pagination: %v", err)
	}
	if tuplePlan.Strategy != tupleStrategy ||
		len(tuplePlan.Keys) != 2 ||
		tuplePlan.Keys[0].Name != "tenant" ||
		tuplePlan.Keys[1].Name != "id" ||
		len(tuplePlan.Ranges) != 3 {
		t.Fatalf("tuple pagination plan = %#v", tuplePlan)
	}
	if tupleStrategy == PaginationTupleKeyset {
		wantBounds := [][]int64{{1, 2}, {2, 2}, {3, 1}}
		for rangeIndex, want := range wantBounds {
			upper := *tuplePlan.Ranges[rangeIndex].Upper
			for keyIndex, wantValue := range want {
				got, err := upper[keyIndex].SQLValue()
				if err != nil || got != wantValue {
					t.Fatalf(
						"tuple range %d key %d = %#v, %v; want %d",
						rangeIndex,
						keyIndex,
						got,
						err,
						wantValue,
					)
				}
			}
		}
	} else if tuplePlan.Ranges[0].FirstRow != 1 ||
		tuplePlan.Ranges[0].LastRow != 2 ||
		tuplePlan.Ranges[2].FirstRow != 5 ||
		tuplePlan.Ranges[2].LastRow != 5 {
		t.Fatalf("row-number tuple ranges = %#v", tuplePlan.Ranges)
	}

	if textName == "" {
		return
	}
	textTable, err := source.InspectTable(ctx, textName)
	if err != nil {
		t.Fatalf("inspect text pagination table: %v", err)
	}
	textPlan, err := planner.PlanPagination(
		ctx,
		textTable,
		2,
	)
	if err != nil {
		t.Fatalf("plan text pagination: %v", err)
	}
	if textPlan.Strategy != PaginationRowNumber ||
		len(textPlan.Keys) != 1 ||
		textPlan.Keys[0].Name != "code" ||
		len(textPlan.Ranges) != 2 ||
		textPlan.Ranges[0].FirstRow != 1 ||
		textPlan.Ranges[0].LastRow != 2 ||
		textPlan.Ranges[1].FirstRow != 3 ||
		textPlan.Ranges[1].LastRow != 3 {
		t.Fatalf("text pagination plan = %#v", textPlan)
	}
}
