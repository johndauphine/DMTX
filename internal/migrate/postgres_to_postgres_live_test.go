package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresToPostgresCommonFixtureLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL-to-PostgreSQL common fixture",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL common-fixture DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()
	sourceDatabase, err := sql.Open("pgx", dsn)
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

	targetDatabaseName := fmt.Sprintf(
		"dmtx_pg_target_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE DATABASE "+postgresIdentifier(targetDatabaseName),
	); err != nil {
		t.Fatalf("create PostgreSQL common-fixture target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+
				postgresIdentifier(targetDatabaseName)+
				" WITH (FORCE)",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL common-fixture target database: %v",
				err,
			)
		}
	})

	namespace := fmt.Sprintf(
		"dmtx_pg_common_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL common-fixture source schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := sourceDatabase.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL common-fixture source schema: %v",
				err,
			)
		}
	})
	sourceEndpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetDatabaseName
	targetSetupDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	targetSetup, err := sql.Open("pgx", targetSetupDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL target schema setup: %v", err)
	}
	if _, err := targetSetup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		_ = targetSetup.Close()
		t.Fatalf("create PostgreSQL common-fixture target schema: %v", err)
	}
	if err := targetSetup.Close(); err != nil {
		t.Fatalf("close PostgreSQL target schema setup: %v", err)
	}

	fixture := postgresCommonFixtureTables(t, namespace)
	createPostgresCommonFixture(
		t,
		ctx,
		sourceDatabase,
		fixture,
	)
	if _, err := sourceDatabase.ExecContext(
		ctx,
		`SELECT pg_catalog.setval(
			pg_catalog.pg_get_serial_sequence($1, $2),
			41,
			true
		)`,
		namespace+".accounts",
		"id",
	); err != nil {
		t.Fatalf("set common-fixture source identity frontier: %v", err)
	}
	insertPostgresCommonFixtureRows(
		t,
		ctx,
		sourceDatabase,
		namespace,
	)

	sourceMetadata := inspectPostgresCommonFixture(
		t,
		ctx,
		sourceDatabase,
		namespace,
	)
	result, err := PostgresToPostgresWithObserver(
		ctx,
		config.Config{
			Source: sourceEndpoint,
			Target: targetEndpoint,
			Migration: config.Migration{
				TargetMode: "drop_recreate",
			},
		},
		nil,
	)
	if err != nil {
		t.Fatalf("migrate PostgreSQL common fixture: %v", err)
	}
	if result.Tables != 2 ||
		result.Rows != 4 ||
		!result.Validated {
		t.Fatalf(
			"PostgreSQL common-fixture result = %+v, want 2 tables, 4 rows, validated",
			result,
		)
	}

	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	targetDatabase, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open PostgreSQL common-fixture target: %v", err)
	}
	t.Cleanup(func() {
		if err := targetDatabase.Close(); err != nil {
			t.Errorf("close PostgreSQL common-fixture target: %v", err)
		}
	})
	if err := targetDatabase.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL common-fixture target: %v", err)
	}
	targetMetadata := inspectPostgresCommonFixture(
		t,
		ctx,
		targetDatabase,
		namespace,
	)
	if !reflect.DeepEqual(targetMetadata, sourceMetadata) {
		t.Fatalf(
			"PostgreSQL common-fixture metadata differs:\nsource: %#v\ntarget: %#v",
			sourceMetadata,
			targetMetadata,
		)
	}
	assertPostgresCommonFixtureRows(
		t,
		ctx,
		targetDatabase,
		namespace,
	)
	assertPostgresCommonFixtureDefaultsAndIdentity(
		t,
		ctx,
		targetDatabase,
		namespace,
	)
}

func postgresCommonFixtureTables(
	t *testing.T,
	namespace string,
) []schema.Table {
	t.Helper()
	parseDefault := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatalf("parse common-fixture default %q: %v", value, err)
		}
		return expression
	}
	check, err := schema.ParseSQLiteCheckExpression(
		`balance >= 0 AND code <> ''`,
	)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	return []schema.Table{
		{
			Schema: namespace,
			Name:   "account_events",
			Columns: []schema.Column{
				{
					Name:               "tenant_id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
				{
					Name:               "event_id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 2,
				},
				{Name: "account_id", Type: "bigint"},
				{
					Name: "note",
					Type: "varchar",
					DeclaredType: &schema.DeclaredType{
						Base:      "varchar",
						Arguments: []int{80},
					},
					Default: parseDefault("'created'"),
				},
			},
			ForeignKeys: []schema.ForeignKey{{
				Name:              "account_events_account_fkey",
				Columns:           []string{"account_id"},
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
				OnDelete:          "RESTRICT",
				Match:             "SIMPLE",
			}},
		},
		{
			Schema: namespace,
			Name:   "accounts",
			Identity: &schema.Identity{
				Column:     "id",
				Generation: schema.IdentityByDefault,
				Frontier:   &frontier,
			},
			Columns: []schema.Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
				{
					Name: "code",
					Type: "varchar",
					DeclaredType: &schema.DeclaredType{
						Base:      "varchar",
						Arguments: []int{24},
					},
					Default: parseDefault("'guest'"),
				},
				{
					Name: "balance",
					Type: "numeric",
					DeclaredType: &schema.DeclaredType{
						Base:      "numeric",
						Arguments: []int{12, 2},
					},
					Default: parseDefault("0.00"),
				},
				{
					Name:    "enabled",
					Type:    "boolean",
					Default: parseDefault("TRUE"),
				},
				{
					Name:     "payload",
					Type:     "bytea",
					Nullable: true,
					Default:  parseDefault("X'00FF'"),
				},
				{
					Name: "created_at",
					Type: "timestamp",
					DeclaredType: &schema.DeclaredType{
						Base:      "timestamp",
						Arguments: []int{3},
					},
					Default: parseDefault("CURRENT_TIMESTAMP"),
				},
				{
					Name:     "document",
					Type:     "jsonb",
					Nullable: true,
				},
			},
			Indexes: []schema.Index{{
				Name:   "accounts_code_uq",
				Unique: true,
				Columns: []schema.IndexColumn{{
					Name:      "code",
					Collation: "BINARY",
				}},
			}},
			Checks: []schema.CheckConstraint{{
				Name:       "accounts_balance_check",
				Expression: check,
			}},
		},
	}
}

func createPostgresCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) {
	t.Helper()
	for _, table := range tables {
		statement, err := schema.CreateTable(schema.Postgres, table)
		if err != nil {
			t.Fatalf("render common-fixture table %s: %v", table.Name, err)
		}
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create common-fixture table %s: %v", table.Name, err)
		}
	}
	objects, err := schema.PlanPostgresDropRecreateObjects(
		tables,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatalf("plan common-fixture objects: %v", err)
	}
	for _, object := range objects {
		if _, err := database.ExecContext(ctx, object.SQL); err != nil {
			t.Fatalf(
				"create common-fixture object %s: %v",
				object.Name,
				err,
			)
		}
	}
}

func insertPostgresCommonFixtureRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, "accounts")+
			` ("id", "code", "balance", "enabled", "payload", "created_at", "document")
			 VALUES
			 (7, '東京', 12.34, true, decode('00ff', 'hex'), '2026-07-29 12:34:56.123', '{"active":true}'),
			 (11, 'emoji 😀', 0.00, false, NULL, '2026-07-29 23:59:59.999', NULL)`,
	); err != nil {
		t.Fatalf("insert common-fixture accounts: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, "account_events")+
			` ("tenant_id", "event_id", "account_id", "note")
			 VALUES
			 (1, 9007199254740993, 7, 'Zażółć gęślą jaźń — 東京'),
			 (1, 9007199254740995, 11, 'emoji 😀')`,
	); err != nil {
		t.Fatalf("insert common-fixture events: %v", err)
	}
}

func inspectPostgresCommonFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) []schema.Table {
	t.Helper()
	names, err := engine.ListPostgresTables(ctx, database, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"account_events", "accounts"}) {
		t.Fatalf("common-fixture tables = %#v", names)
	}
	tables := make([]schema.Table, 0, len(names))
	for _, name := range names {
		table, err := engine.InspectPostgresTable(
			ctx,
			database,
			namespace,
			name,
		)
		if err != nil {
			t.Fatalf("inspect common-fixture table %s: %v", name, err)
		}
		tables = append(tables, table)
	}
	return tables
}

func assertPostgresCommonFixtureRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var (
		accountCount int
		eventCount   int
	)
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+postgresQualified(namespace, "accounts"),
	).Scan(&accountCount); err != nil {
		t.Fatal(err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(namespace, "account_events"),
	).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if accountCount != 2 || eventCount != 2 {
		t.Fatalf(
			"common-fixture row counts = (%d, %d)",
			accountCount,
			eventCount,
		)
	}
	var (
		code     string
		balance  string
		payload  string
		document string
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT "code", "balance"::text, encode("payload", 'hex'),
		        "document"::text
		   FROM `+postgresQualified(namespace, "accounts")+
			` WHERE "id" = 7`,
	).Scan(&code, &balance, &payload, &document); err != nil {
		t.Fatal(err)
	}
	if code != "東京" ||
		balance != "12.34" ||
		payload != "00ff" ||
		document != `{"active": true}` {
		t.Fatalf(
			"common-fixture account = (%q, %q, %q, %q)",
			code,
			balance,
			payload,
			document,
		)
	}
	var note string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "note" FROM `+
			postgresQualified(namespace, "account_events")+
			` WHERE "tenant_id" = 1 AND "event_id" = 9007199254740993`,
	).Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "Zażółć gęślą jaźń — 東京" {
		t.Fatalf("common-fixture note = %q", note)
	}
}

func assertPostgresCommonFixtureDefaultsAndIdentity(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
) {
	t.Helper()
	var (
		id      int64
		code    string
		balance string
		enabled bool
		payload string
	)
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+postgresQualified(namespace, "accounts")+
			` DEFAULT VALUES
			 RETURNING "id", "code", "balance"::text, "enabled",
			           encode("payload", 'hex')`,
	).Scan(&id, &code, &balance, &enabled, &payload); err != nil {
		t.Fatalf("insert target defaults row: %v", err)
	}
	if id != 42 ||
		code != "guest" ||
		balance != "0.00" ||
		!enabled ||
		payload != "00ff" {
		t.Fatalf(
			"target defaults row = (%d, %q, %q, %v, %q)",
			id,
			code,
			balance,
			enabled,
			payload,
		)
	}
}
