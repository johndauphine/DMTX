package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerToSQLServerCommonFixtureLive(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if sourceDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_TARGET_DSN, and DMTX_TEST_MSSQL_CA to run the SQL Server-to-SQL Server common fixture",
		)
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sourceDSN,
		caPath,
	)
	targetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		targetDSN,
		caPath,
	)
	if strings.EqualFold(
		sourceEndpoint.Database,
		targetEndpoint.Database,
	) {
		t.Fatal(
			"SQL Server common fixture requires distinct source and target databases",
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Second,
	)
	defer cancel()
	sourceDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"source",
		sourceEndpoint,
	)
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetEndpoint,
	)

	prefix := "dmtx_ss_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	cleanupSQLServerNativeTables(
		t,
		sourceDatabase,
		eventsName,
		accountsName,
	)
	cleanupSQLServerNativeTables(
		t,
		targetDatabase,
		eventsName,
		accountsName,
	)
	seedSQLServerNativeReplacementTargets(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	createSQLServerCommonFixture(
		t,
		ctx,
		sourceDatabase,
		prefix,
		accountsName,
		eventsName,
	)
	insertSQLServerCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)

	sourceMetadata := inspectSQLServerCommonFixture(
		t,
		ctx,
		sourceDatabase,
		accountsName,
		eventsName,
	)
	migrationConfig := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	result, err := SQLServerToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"unacknowledged SQL Server rebuild result = %+v, error = %v",
			result,
			err,
		)
	}
	for _, name := range []string{accountsName, eventsName} {
		var retained int
		if err := targetDatabase.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM "+
				sqlServerQualified("dbo", name)+
				" WHERE [stale_id] = 99 AND "+
				"[stale_marker] = 'must disappear'",
		).Scan(&retained); err != nil {
			t.Fatalf(
				"inspect unacknowledged SQL Server target %s: %v",
				name,
				err,
			)
		}
		if retained != 1 {
			t.Fatalf(
				"unacknowledged SQL Server rebuild mutated %s: stale rows = %d",
				name,
				retained,
			)
		}
	}
	migrationConfig.Migration.DestructiveAcknowledged = true
	result, err = SQLServerToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("migrate SQL Server common fixture into SQL Server: %v", err)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"SQL Server-to-SQL Server result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetMetadata := inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	if !reflect.DeepEqual(targetMetadata, sourceMetadata) {
		t.Fatalf(
			"SQL Server common-fixture metadata differs:\nsource: %#v\ntarget: %#v",
			sourceMetadata,
			targetMetadata,
		)
	}
	assertSQLServerNativeCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertSQLServerNativeStaleTargetsWereReplaced(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)

	insertSQLServerNativeTargetOnlyAccount(
		t,
		ctx,
		targetDatabase,
		accountsName,
		29,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	upsertResult, err := SQLServerToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf("upsert SQL Server common fixture: %v", err)
	}
	if upsertResult.Tables != 2 ||
		upsertResult.Rows != 4 ||
		!upsertResult.Validated {
		t.Fatalf(
			"SQL Server retained-upsert result = %+v, want 2 tables, 4 rows, validated",
			upsertResult,
		)
	}
	var retained int
	if err := targetDatabase.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", accountsName)+
			" WHERE [id] = 29 AND [code] = 'target-only'",
	).Scan(&retained); err != nil {
		t.Fatalf("read retained SQL Server target row: %v", err)
	}
	if retained != 1 {
		t.Fatalf("retained SQL Server target rows = %d, want 1", retained)
	}
	targetMetadata = inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	if !reflect.DeepEqual(targetMetadata, sourceMetadata) {
		t.Fatalf(
			"retained SQL Server metadata differs:\nsource: %#v\ntarget: %#v",
			sourceMetadata,
			targetMetadata,
		)
	}
	assertSQLServerNativeDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		accountsName,
	)
}

func TestPostgresToSQLServerCommonFixtureLive(t *testing.T) {
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if postgresDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN, DMTX_TEST_MSSQL_TARGET_DSN, and DMTX_TEST_MSSQL_CA to run the PostgreSQL-to-SQL Server common fixture",
		)
	}
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL common-fixture DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	targetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		targetDSN,
		caPath,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL common-fixture source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL common-fixture source: %v", err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL common-fixture source: %T", err)
	}
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetEndpoint,
	)

	prefix := "dmtx_ps_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	accountsName := prefix + "_accounts"
	eventsName := prefix + "_events"
	namespace := prefix
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL-to-SQL Server source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL-to-SQL Server source schema: %v",
				err,
			)
		}
	})
	cleanupSQLServerNativeTables(
		t,
		targetDatabase,
		eventsName,
		accountsName,
	)
	fixture := postgresSQLServerCommonFixtureTables(
		t,
		namespace,
		accountsName,
		eventsName,
	)
	createPostgresCommonFixture(t, ctx, sourceDatabase, fixture)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		`SELECT pg_catalog.setval(
			pg_catalog.pg_get_serial_sequence($1, $2),
			41,
			true
		)`,
		namespace+"."+accountsName,
		"id",
	); err != nil {
		t.Fatalf("set PostgreSQL identity frontier: %v", err)
	}
	insertPostgresSQLServerCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		namespace,
		accountsName,
		eventsName,
	)

	migrationConfig := config.Config{
		Source: config.Endpoint{
			Type:     "postgres",
			Host:     postgresConfig.Host,
			Port:     int(postgresConfig.Port),
			Database: postgresConfig.Database,
			User:     postgresConfig.User,
			Password: postgresConfig.Password,
			Schema:   namespace,
			SSLMode:  "require",
		},
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{accountsName, eventsName},
		},
	}
	result, err := PostgresToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"migrate PostgreSQL common fixture into SQL Server: %v",
			err,
		)
	}
	if result.Tables != 2 || result.Rows != 4 || !result.Validated {
		t.Fatalf(
			"PostgreSQL-to-SQL Server result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}
	assertPostgresSQLServerCommonRows(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	targetTables := inspectSQLServerCommonFixture(
		t,
		ctx,
		targetDatabase,
		accountsName,
		eventsName,
	)
	assertPostgresToSQLServerCommonMetadata(
		t,
		targetTables,
		accountsName,
		eventsName,
	)

	insertPostgresSQLServerNativeTargetOnlyAccount(
		t,
		ctx,
		targetDatabase,
		accountsName,
		29,
	)
	migrationConfig.Migration.TargetMode = "upsert"
	upsertResult, err := PostgresToSQLServerWithObserver(
		ctx,
		migrationConfig,
		nil,
	)
	if err != nil {
		t.Fatalf(
			"retained upsert PostgreSQL common fixture into SQL Server: %v",
			err,
		)
	}
	if upsertResult.Tables != 2 ||
		upsertResult.Rows != 4 ||
		!upsertResult.Validated {
		t.Fatalf(
			"PostgreSQL-to-SQL Server retained result = %+v, want 2 tables, 4 rows, validated",
			upsertResult,
		)
	}
}

func TestSQLServerNativeNonIdentityUUIDLive(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if sourceDSN == "" || postgresDSN == "" ||
		targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN, DMTX_TEST_POSTGRES_DSN, DMTX_TEST_MSSQL_TARGET_DSN, and DMTX_TEST_MSSQL_CA to run non-identity UUID routes",
		)
	}
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL UUID-fixture DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sourceDSN,
		caPath,
	)
	targetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		targetDSN,
		caPath,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Second,
	)
	defer cancel()
	sourceDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"UUID source",
		sourceEndpoint,
	)
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"UUID target",
		targetEndpoint,
	)
	postgresDatabase, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL UUID source: %T", err)
	}
	t.Cleanup(func() {
		if err := postgresDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL UUID source: %v", err)
		}
	})
	if err := postgresDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL UUID source: %T", err)
	}

	prefix := "dmtx_uuid_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	const sourceUUID = "6f9619ff-8b86-d011-b42d-00c04fc964ff"
	const postgresUUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	t.Run("SQL Server to SQL Server", func(t *testing.T) {
		tableName := prefix + "_mssql"
		cleanupSQLServerNativeTables(t, sourceDatabase, tableName)
		cleanupSQLServerNativeTables(t, targetDatabase, tableName)
		if _, err := sourceDatabase.ExecContext(
			ctx,
			"CREATE TABLE "+sqlServerQualified("dbo", tableName)+
				" ([id] BIGINT NOT NULL, "+
				"[external_id] UNIQUEIDENTIFIER NOT NULL, "+
				"CONSTRAINT "+
				sqlServerIdentifier(tableName+"_pk")+
				" PRIMARY KEY CLUSTERED ([id] ASC))",
		); err != nil {
			t.Fatalf("create SQL Server non-identity UUID source: %v", err)
		}
		if _, err := sourceDatabase.ExecContext(
			ctx,
			"INSERT INTO "+sqlServerQualified("dbo", tableName)+
				" ([id], [external_id]) VALUES "+
				"(1, CONVERT(uniqueidentifier, @p1))",
			sourceUUID,
		); err != nil {
			t.Fatalf("insert SQL Server non-identity UUID source: %v", err)
		}
		result, err := SQLServerToSQLServerWithObserver(
			ctx,
			config.Config{
				Source: sourceEndpoint,
				Target: targetEndpoint,
				Migration: config.Migration{
					TargetMode:    "drop_recreate",
					IncludeTables: []string{tableName},
				},
			},
			nil,
		)
		if err != nil {
			t.Fatalf("migrate SQL Server non-identity UUID: %v", err)
		}
		if result.Tables != 1 || result.Rows != 1 ||
			!result.Validated {
			t.Fatalf("SQL Server non-identity UUID result = %+v", result)
		}
		assertSQLServerNativeUUIDRow(
			t,
			ctx,
			targetDatabase,
			tableName,
			sourceUUID,
		)
	})

	t.Run("PostgreSQL to SQL Server", func(t *testing.T) {
		namespace := prefix + "_pg"
		tableName := prefix + "_postgres"
		if _, err := postgresDatabase.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(namespace),
		); err != nil {
			t.Fatalf("create PostgreSQL UUID source schema: %v", err)
		}
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(),
				15*time.Second,
			)
			defer cleanupCancel()
			if _, err := postgresDatabase.ExecContext(
				cleanupCtx,
				"DROP SCHEMA IF EXISTS "+
					postgresIdentifier(namespace)+" CASCADE",
			); err != nil {
				t.Errorf("drop PostgreSQL UUID source schema: %v", err)
			}
		})
		cleanupSQLServerNativeTables(t, targetDatabase, tableName)
		if _, err := postgresDatabase.ExecContext(
			ctx,
			"CREATE TABLE "+
				postgresQualified(namespace, tableName)+
				` ("id" BIGINT NOT NULL,
				    "external_id" UUID NOT NULL,
				    CONSTRAINT `+
				postgresIdentifier(tableName+"_pk")+
				` PRIMARY KEY ("id"))`,
		); err != nil {
			t.Fatalf("create PostgreSQL non-identity UUID source: %v", err)
		}
		if _, err := postgresDatabase.ExecContext(
			ctx,
			"INSERT INTO "+
				postgresQualified(namespace, tableName)+
				` ("id", "external_id") VALUES (1, $1::uuid)`,
			postgresUUID,
		); err != nil {
			t.Fatalf("insert PostgreSQL non-identity UUID source: %v", err)
		}
		result, err := PostgresToSQLServerWithObserver(
			ctx,
			config.Config{
				Source: config.Endpoint{
					Type:     "postgres",
					Host:     postgresConfig.Host,
					Port:     int(postgresConfig.Port),
					Database: postgresConfig.Database,
					User:     postgresConfig.User,
					Password: postgresConfig.Password,
					Schema:   namespace,
					SSLMode:  "require",
				},
				Target: targetEndpoint,
				Migration: config.Migration{
					TargetMode:    "drop_recreate",
					IncludeTables: []string{tableName},
				},
			},
			nil,
		)
		if err != nil {
			t.Fatalf("migrate PostgreSQL non-identity UUID: %v", err)
		}
		if result.Tables != 1 || result.Rows != 1 ||
			!result.Validated {
			t.Fatalf("PostgreSQL non-identity UUID result = %+v", result)
		}
		assertSQLServerNativeUUIDRow(
			t,
			ctx,
			targetDatabase,
			tableName,
			postgresUUID,
		)
	})
}

func assertSQLServerNativeUUIDRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	tableName string,
	expected string,
) {
	t.Helper()
	var actual string
	if err := database.QueryRowContext(
		ctx,
		"SELECT CONVERT(varchar(36), [external_id]) FROM "+
			sqlServerQualified("dbo", tableName)+
			" WHERE [id] = 1",
	).Scan(&actual); err != nil {
		t.Fatalf("read SQL Server non-identity UUID target: %v", err)
	}
	if !strings.EqualFold(actual, expected) {
		t.Fatalf(
			"SQL Server non-identity UUID = %q, want %q",
			actual,
			expected,
		)
	}
}

func TestPostgresToSQLServerRejectsEndOfDayTimeBeforeMutationLive(
	t *testing.T,
) {
	postgresDSN := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if postgresDSN == "" || targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN, DMTX_TEST_MSSQL_TARGET_DSN, and DMTX_TEST_MSSQL_CA to run the PostgreSQL TIME-domain preflight fixture",
		)
	}
	postgresConfig, err := pgx.ParseConfig(postgresDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL TIME-domain DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(postgresConfig) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	targetEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		targetDSN,
		caPath,
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL TIME-domain source: %T", err)
	}
	t.Cleanup(func() {
		if err := sourceDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL TIME-domain source: %v", err)
		}
	})
	if err := sourceDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL TIME-domain source: %T", err)
	}
	targetDatabase := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"target",
		targetEndpoint,
	)

	prefix := "dmtx_ps_time_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := prefix
	tableName := prefix + "_clocks"
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL TIME-domain schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL TIME-domain schema: %v", err)
		}
	})
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(namespace, tableName)+
			` ("id" bigint NOT NULL PRIMARY KEY,
			    "clock" time(6) NOT NULL)`,
	); err != nil {
		t.Fatalf("create PostgreSQL TIME-domain table: %v", err)
	}
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, tableName)+
			` ("id", "clock") VALUES (1, '24:00:00')`,
	); err != nil {
		t.Fatalf("insert PostgreSQL end-of-day TIME: %v", err)
	}

	var driverValue any
	if err := sourceDatabase.QueryRowContext(
		ctx,
		"SELECT \"clock\" FROM "+
			postgresQualified(namespace, tableName),
	).Scan(&driverValue); err != nil {
		t.Fatalf("read PostgreSQL TIME driver value: %v", err)
	}
	text, ok := driverValue.(string)
	if !ok || !strings.HasPrefix(text, "24:00:00") {
		t.Fatalf(
			"pgx database/sql TIME value = %T, want end-of-day string",
			driverValue,
		)
	}

	cleanupSQLServerNativeTables(t, targetDatabase, tableName)
	if _, err := targetDatabase.ExecContext(
		ctx,
		"CREATE TABLE "+sqlServerQualified("dbo", tableName)+
			" ([sentinel] BIGINT NOT NULL PRIMARY KEY)",
	); err != nil {
		t.Fatalf("create SQL Server TIME-domain sentinel: %v", err)
	}
	if _, err := targetDatabase.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified("dbo", tableName)+
			" ([sentinel]) VALUES (73)",
	); err != nil {
		t.Fatalf("insert SQL Server TIME-domain sentinel: %v", err)
	}
	var beforeObjectID int64
	if err := targetDatabase.QueryRowContext(
		ctx,
		"SELECT CONVERT(bigint, OBJECT_ID(@p1, 'U'))",
		"dbo."+tableName,
	).Scan(&beforeObjectID); err != nil {
		t.Fatalf("read SQL Server sentinel object ID: %v", err)
	}

	observer := &sqlServerNativePreflightObserver{}
	result, err := PostgresToSQLServerWithObserver(
		ctx,
		config.Config{
			Source: config.Endpoint{
				Type:     "postgres",
				Host:     postgresConfig.Host,
				Port:     int(postgresConfig.Port),
				Database: postgresConfig.Database,
				User:     postgresConfig.User,
				Password: postgresConfig.Password,
				Schema:   namespace,
				SSLMode:  "require",
			},
			Target: targetEndpoint,
			Migration: config.Migration{
				TargetMode:              "drop_recreate",
				IncludeTables:           []string{tableName},
				DestructiveAcknowledged: true,
			},
		},
		observer,
	)
	if err == nil || !strings.Contains(err.Error(), "TIME") {
		t.Fatalf(
			"PostgreSQL end-of-day TIME result = %+v, error = %v",
			result,
			err,
		)
	}
	if result != (Result{}) ||
		observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 ||
		observer.mutations != 0 {
		t.Fatalf(
			"PostgreSQL TIME preflight reached mutation: result=%+v observer=%+v",
			result,
			observer,
		)
	}
	var (
		afterObjectID int64
		sentinel      int64
	)
	if err := targetDatabase.QueryRowContext(
		ctx,
		"SELECT CONVERT(bigint, OBJECT_ID(@p1, 'U'))",
		"dbo."+tableName,
	).Scan(&afterObjectID); err != nil {
		t.Fatalf("read SQL Server sentinel object ID after rejection: %v", err)
	}
	if err := targetDatabase.QueryRowContext(
		ctx,
		"SELECT [sentinel] FROM "+
			sqlServerQualified("dbo", tableName),
	).Scan(&sentinel); err != nil {
		t.Fatalf("read SQL Server sentinel after rejection: %v", err)
	}
	if afterObjectID != beforeObjectID || sentinel != 73 {
		t.Fatalf(
			"SQL Server target mutated: object ID %d -> %d, sentinel=%d",
			beforeObjectID,
			afterObjectID,
			sentinel,
		)
	}
}

func TestSQLServerToSQLServerRejectsLiveSameDatabaseAlias(t *testing.T) {
	sourceDSN := os.Getenv("DMTX_TEST_MSSQL_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if sourceDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server same-database alias guard",
		)
	}
	sourceEndpoint := sqlServerCommonFixtureEndpoint(
		t,
		sourceDSN,
		caPath,
	)
	targetAlias := sourceEndpoint
	if strings.EqualFold(sourceEndpoint.Host, "localhost") {
		targetAlias.Host = "127.0.0.1"
	} else {
		targetAlias.Host = "localhost"
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"source",
		sourceEndpoint,
	)
	tableName := "dmtx_ss_alias_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupSQLServerNativeTables(t, database, tableName)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+sqlServerQualified("dbo", tableName)+
			" ([id] BIGINT NOT NULL, [payload] VARCHAR(24) "+
			"COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL, "+
			"PRIMARY KEY ([id]))",
	); err != nil {
		t.Fatalf("create SQL Server alias-guard source: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified("dbo", tableName)+
			" ([id], [payload]) VALUES (1, 'must remain')",
	); err != nil {
		t.Fatalf("insert SQL Server alias-guard sentinel: %v", err)
	}

	observer := &sqlServerNativePreflightObserver{}
	result, err := SQLServerToSQLServerWithObserver(
		ctx,
		config.Config{
			Source: sourceEndpoint,
			Target: targetAlias,
			Migration: config.Migration{
				TargetMode:    "drop_recreate",
				IncludeTables: []string{tableName},
			},
		},
		observer,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "distinct live source and target") {
		t.Fatalf(
			"SQL Server same-database alias result = %+v, error = %v",
			result,
			err,
		)
	}
	if result != (Result{}) ||
		observer.beforeSets != 0 ||
		observer.before != 0 ||
		observer.after != 0 ||
		observer.mutations != 0 {
		t.Fatalf(
			"SQL Server same-database guard reached mutation: result=%+v observer=%+v",
			result,
			observer,
		)
	}
	var payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT [payload] FROM "+
			sqlServerQualified("dbo", tableName)+" WHERE [id] = 1",
	).Scan(&payload); err != nil {
		t.Fatalf("read SQL Server alias-guard sentinel: %v", err)
	}
	if payload != "must remain" {
		t.Fatalf("SQL Server alias-guard sentinel = %q", payload)
	}
}

func openSQLServerNativeLiveDatabase(
	t *testing.T,
	ctx context.Context,
	role string,
	endpoint config.Endpoint,
) *sql.DB {
	t.Helper()
	database, err := engine.OpenSQLServer2022Source(ctx, endpoint)
	if err != nil {
		t.Fatalf("open SQL Server native %s database: %v", role, err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQL Server native %s database: %v", role, err)
		}
	})
	return database
}

func cleanupSQLServerNativeTables(
	t *testing.T,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cancel()
		for _, name := range names {
			if _, err := database.ExecContext(
				ctx,
				"DROP TABLE IF EXISTS "+
					sqlServerQualified("dbo", name),
			); err != nil {
				t.Errorf(
					"drop SQL Server native-target table %s: %v",
					name,
					err,
				)
			}
		}
	})
}

func seedSQLServerNativeReplacementTargets(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	for _, name := range names {
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+sqlServerQualified("dbo", name)+
				" ([stale_id] BIGINT NOT NULL, "+
				"[stale_marker] VARCHAR(24) NOT NULL, "+
				"PRIMARY KEY ([stale_id]))",
		); err != nil {
			t.Fatalf(
				"create stale SQL Server replacement target %s: %v",
				name,
				err,
			)
		}
		if _, err := database.ExecContext(
			ctx,
			"INSERT INTO "+sqlServerQualified("dbo", name)+
				" ([stale_id], [stale_marker]) "+
				"VALUES (99, 'must disappear')",
		); err != nil {
			t.Fatalf(
				"seed stale SQL Server replacement target %s: %v",
				name,
				err,
			)
		}
	}
}

func assertSQLServerNativeStaleTargetsWereReplaced(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	names ...string,
) {
	t.Helper()
	for _, name := range names {
		var staleColumns int
		if err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			 FROM sys.columns AS target_column
			 JOIN sys.tables AS target_table
			   ON target_table.object_id = target_column.object_id
			 JOIN sys.schemas AS target_schema
			   ON target_schema.schema_id = target_table.schema_id
			 WHERE target_schema.name = @p1
			   AND target_table.name = @p2
			   AND target_column.name IN ('stale_id', 'stale_marker')`,
			"dbo",
			name,
		).Scan(&staleColumns); err != nil {
			t.Fatalf("inspect replaced SQL Server target %s: %v", name, err)
		}
		if staleColumns != 0 {
			t.Fatalf(
				"SQL Server drop/recreate retained %d stale columns on %s",
				staleColumns,
				name,
			)
		}
	}
}

func assertSQLServerNativeCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 2 || eventCount != 2 {
		t.Fatalf(
			"SQL Server common row counts = (%d, %d), want (2, 2)",
			accountCount,
			eventCount,
		)
	}
	var (
		code        string
		balance     string
		payload     []byte
		description string
		externalID  string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [code],
		        CONVERT(varchar(64), [balance]),
		        [payload],
		        [description],
		        CONVERT(varchar(36), [external_id])
		   FROM `+sqlServerQualified("dbo", accountsName)+
			` WHERE [id] = 7`,
	).Scan(
		&code,
		&balance,
		&payload,
		&description,
		&externalID,
	); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		!reflect.DeepEqual(payload, []byte{0x00, 0xff}) ||
		description != "Zażółć gęślą jaźń — 東京" ||
		!strings.EqualFold(
			externalID,
			"6F9619FF-8B86-D011-B42D-00C04FC964FF",
		) {
		t.Fatalf(
			"SQL Server common account = (%q, %q, %x, %q, %q)",
			code,
			balance,
			payload,
			description,
			externalID,
		)
	}
	var nullPayload, nullDescription any
	if err := database.QueryRowContext(
		ctx,
		"SELECT [payload], [description] FROM "+
			sqlServerQualified("dbo", accountsName)+" WHERE [id] = 11",
	).Scan(&nullPayload, &nullDescription); err != nil {
		t.Fatal(err)
	}
	if nullPayload != nil || nullDescription != nil {
		t.Fatalf(
			"SQL Server source NULLs became defaults: payload=%#v description=%#v",
			nullPayload,
			nullDescription,
		)
	}
	var note string
	if err := database.QueryRowContext(
		ctx,
		"SELECT [note] FROM "+
			sqlServerQualified("dbo", eventsName)+
			" WHERE [tenant_id] = 1 AND [event_id] = 9007199254740993",
	).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf("SQL Server common note = %q", note)
	}
}

func insertSQLServerNativeTargetOnlyAccount(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	id int64,
) {
	t.Helper()
	table := sqlServerQualified("dbo", accountsName)
	batch := fmt.Sprintf(`
		SET IDENTITY_INSERT %s ON;
		INSERT INTO %s
			([id], [code], [balance], [ratio], [enabled], [payload],
			 [created_at], [description], [external_id])
		VALUES
			(%d, 'target-only', 1.00, CONVERT(real, 1), 1, NULL,
			 CONVERT(datetime2(3), '2026-07-30T00:00:00.000'),
			 'retained',
			 CONVERT(uniqueidentifier,
			         '11111111-2222-3333-4444-555555555555'));
		SET IDENTITY_INSERT %s OFF;
	`, table, table, id, table)
	if _, err := database.ExecContext(ctx, batch); err != nil {
		_, _ = database.ExecContext(
			context.Background(),
			"SET IDENTITY_INSERT "+table+" OFF",
		)
		t.Fatalf("insert retained SQL Server target account: %v", err)
	}
}

func assertSQLServerNativeDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
) {
	t.Helper()
	var (
		id      int64
		code    string
		balance string
		enabled bool
	)
	err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+sqlServerQualified("dbo", accountsName)+
			` ([ratio], [created_at], [external_id])
			 OUTPUT INSERTED.[id],
			        INSERTED.[code],
			        CONVERT(varchar(64), INSERTED.[balance]),
			        INSERTED.[enabled]
			 VALUES (
			   CONVERT(real, 1),
			   CONVERT(datetime2(3), '2026-07-30T12:00:00.000'),
			   CONVERT(uniqueidentifier,
			           'AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE')
			 )`,
	).Scan(&id, &code, &balance, &enabled)
	if err != nil {
		t.Fatalf("insert SQL Server target defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		balance != "0.00" ||
		!enabled {
		t.Fatalf(
			"SQL Server target defaults row = (%d, %q, %q, %v)",
			id,
			code,
			balance,
			enabled,
		)
	}
}

func postgresSQLServerCommonFixtureTables(
	t *testing.T,
	namespace string,
	accountsName string,
	eventsName string,
) []schema.Table {
	t.Helper()
	fixture := postgresCommonFixtureTables(t, namespace)
	numericCheck, err := schema.ParseSQLiteCheckExpression("balance >= 0")
	if err != nil {
		t.Fatal(err)
	}
	for index := range fixture {
		switch fixture[index].Name {
		case "accounts":
			fixture[index].Name = accountsName
			columns := fixture[index].Columns[:0]
			for _, column := range fixture[index].Columns {
				if column.Name != "document" {
					if column.Name == "created_at" {
						column.Default = nil
					}
					columns = append(columns, column)
				}
			}
			fixture[index].Columns = columns
			fixture[index].Indexes = []schema.Index{{
				Name:   accountsName + "_id_uq",
				Unique: true,
				Columns: []schema.IndexColumn{{
					Name: "id",
				}},
			}}
			fixture[index].Checks = []schema.CheckConstraint{{
				Name:       accountsName + "_balance_ck",
				Expression: numericCheck,
			}}
		case "account_events":
			fixture[index].Name = eventsName
			fixture[index].ForeignKeys[0].Name =
				eventsName + "_account_fk"
			fixture[index].ForeignKeys[0].ReferencedTable =
				accountsName
			// SQL Server has NO ACTION but not PostgreSQL's distinct
			// RESTRICT timing contract. Keep this cross-engine fixture
			// inside the exact common subset.
			fixture[index].ForeignKeys[0].OnDelete = "NO ACTION"
		}
	}
	return fixture
}

func insertPostgresSQLServerCommonFixtureRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, accountsName)+
			` ("id", "code", "balance", "enabled", "payload", "created_at")
			 VALUES
			 (7, '東京', 12.34, true, decode('00ff', 'hex'),
			  '2026-07-29 12:34:56.123'),
			 (11, 'emoji 😀', 0.00, false, NULL,
			  '2026-07-29 23:59:59.999')`,
	); err != nil {
		t.Fatalf("insert PostgreSQL-to-SQL Server accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, eventsName)+
			` ("tenant_id", "event_id", "account_id", "note")
			 VALUES
			 (1, 9007199254740993, 7,
			  'Zażółć gęślą jaźń — 東京'),
			 (1, 9007199254740995, 11, 'emoji 😀')`,
	); err != nil {
		t.Fatalf("insert PostgreSQL-to-SQL Server events: %v", err)
	}
}

func assertPostgresSQLServerCommonRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	var accountCount, eventCount int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", accountsName),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", eventsName),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 2 || eventCount != 2 {
		t.Fatalf(
			"PostgreSQL-to-SQL Server row counts = (%d, %d), want (2, 2)",
			accountCount,
			eventCount,
		)
	}
	var (
		code    string
		balance string
		payload []byte
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT [code], CONVERT(varchar(64), [balance]), [payload]
		   FROM `+sqlServerQualified("dbo", accountsName)+
			` WHERE [id] = 7`,
	).Scan(&code, &balance, &payload); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		!reflect.DeepEqual(payload, []byte{0x00, 0xff}) {
		t.Fatalf(
			"PostgreSQL-to-SQL Server account = (%q, %q, %x)",
			code,
			balance,
			payload,
		)
	}
	var nullPayload any
	if err := database.QueryRowContext(
		ctx,
		"SELECT [payload] FROM "+
			sqlServerQualified("dbo", accountsName)+" WHERE [id] = 11",
	).Scan(&nullPayload); err != nil {
		t.Fatal(err)
	}
	if nullPayload != nil {
		t.Fatalf(
			"PostgreSQL NULL payload became %#v",
			nullPayload,
		)
	}
	var note string
	if err := database.QueryRowContext(
		ctx,
		"SELECT [note] FROM "+
			sqlServerQualified("dbo", eventsName)+
			" WHERE [tenant_id] = 1 AND [event_id] = 9007199254740993",
	).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf("PostgreSQL-to-SQL Server note = %q", note)
	}
}

func insertPostgresSQLServerNativeTargetOnlyAccount(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	accountsName string,
	id int64,
) {
	t.Helper()
	table := sqlServerQualified("dbo", accountsName)
	batch := fmt.Sprintf(`
		SET IDENTITY_INSERT %s ON;
		INSERT INTO %s
			([id], [code], [balance], [enabled], [payload], [created_at])
		VALUES
			(%d, 'target-only', 1.00, 1, NULL,
			 CONVERT(datetime2(3), '2026-07-30T00:00:00.000'));
		SET IDENTITY_INSERT %s OFF;
	`, table, table, id, table)
	if _, err := database.ExecContext(ctx, batch); err != nil {
		_, _ = database.ExecContext(
			context.Background(),
			"SET IDENTITY_INSERT "+table+" OFF",
		)
		t.Fatalf(
			"insert retained PostgreSQL-to-SQL Server target account: %v",
			err,
		)
	}
}

func assertPostgresToSQLServerCommonMetadata(
	t *testing.T,
	tables map[string]schema.Table,
	accountsName string,
	eventsName string,
) {
	t.Helper()
	accounts, accountsOK := tables[accountsName]
	events, eventsOK := tables[eventsName]
	if !accountsOK || !eventsOK ||
		accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 {
		t.Fatalf("PostgreSQL-to-SQL Server metadata = %#v", tables)
	}
	if len(accounts.Columns) != 6 ||
		len(accounts.Indexes) != 1 ||
		len(accounts.Checks) != 1 ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf(
			"PostgreSQL-to-SQL Server object metadata = %#v",
			tables,
		)
	}
}

type sqlServerNativePreflightObserver struct {
	beforeSets int
	before     int
	after      int
	mutations  int
}

func (observer *sqlServerNativePreflightObserver) BeforeTables(
	context.Context,
	[]string,
) error {
	observer.beforeSets++
	return nil
}

func (observer *sqlServerNativePreflightObserver) BeforeTable(
	context.Context,
	string,
) error {
	observer.before++
	return nil
}

func (observer *sqlServerNativePreflightObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.after++
	return nil
}

func (observer *sqlServerNativePreflightObserver) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	observer.mutations++
	return mutation()
}
