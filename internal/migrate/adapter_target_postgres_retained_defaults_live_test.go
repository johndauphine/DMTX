package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresRetainedDefaultPreflightLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run retained PostgreSQL default preflight tests",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	runPostgresRetainedObjectPreflightLiveCase(
		t,
		dsn,
		parsed,
		"default",
		func(
			ctx context.Context,
			database *sql.DB,
			namespace string,
		) error {
			_, err := database.ExecContext(
				ctx,
				"ALTER TABLE "+
					postgresQualified(namespace, "accounts")+
					" ALTER COLUMN "+
					postgresIdentifier("label")+
					" SET DEFAULT 'drift'",
			)
			return err
		},
	)
}

func TestPostgresRetainedDefaultsMatchPG16CatalogLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run PostgreSQL default catalog tests",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open live PostgreSQL connection: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live PostgreSQL connection: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify live PostgreSQL connection: %T", err)
	}

	namespace := fmt.Sprintf(
		"dmtx_retained_defaults_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create retained-default catalog schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop retained-default catalog schema: %v", err)
		}
	})

	parseDefault := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatalf("parse SQLite default %q: %v", value, err)
		}
		return expression
	}
	table := schema.Table{
		Schema: namespace,
		Name:   "defaults",
		Columns: []schema.Column{
			{Name: "bool_true", Type: "boolean", Nullable: true, Default: parseDefault("TRUE")},
			{Name: "bool_false", Type: "boolean", Nullable: true, Default: parseDefault("FALSE")},
			{Name: "int_positive", Type: "integer", Nullable: true, Default: parseDefault("7")},
			{Name: "int_negative", Type: "integer", Nullable: true, Default: parseDefault("-7")},
			{Name: "bigint_small", Type: "bigint", Nullable: true, Default: parseDefault("7")},
			{
				Name:     "bigint_large",
				Type:     "bigint",
				Nullable: true,
				Default:  parseDefault("9223372036854775807"),
			},
			{Name: "double_plain", Type: "double", Nullable: true, Default: parseDefault("1.25")},
			{Name: "double_exponent", Type: "double", Nullable: true, Default: parseDefault("1e2")},
			{
				Name:     "double_negative_zero",
				Type:     "double",
				Nullable: true,
				Default:  parseDefault("-0"),
			},
			{
				Name:     "numeric_leading",
				Type:     "numeric",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
				Default: parseDefault("00.50"),
			},
			{
				Name:     "numeric_plus",
				Type:     "numeric",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
				Default: parseDefault("+7"),
			},
			{Name: "text_plain", Type: "text", Nullable: true, Default: parseDefault("'active'")},
			{
				Name:     "char_plain",
				Type:     "text",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
				Default: parseDefault("'AB'"),
			},
			{
				Name:     "varchar_plain",
				Type:     "text",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{40},
				},
				Default: parseDefault("'unknown'"),
			},
			{Name: "bytea_plain", Type: "blob", Nullable: true, Default: parseDefault("X'00FF'")},
			{Name: "date_value", Type: "date", Nullable: true, Default: parseDefault("CURRENT_DATE")},
			{
				Name:     "timestamp_value",
				Type:     "timestamp",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
				Default: parseDefault("CURRENT_TIMESTAMP"),
			},
			{Name: "explicit_null", Type: "text", Nullable: true, Default: parseDefault("NULL")},
			{Name: "absent", Type: "text", Nullable: true},
		},
	}
	statement, err := schema.CreateTable(schema.Postgres, table)
	if err != nil {
		t.Fatalf("render retained-default catalog table: %v", err)
	}
	if _, err := database.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create retained-default catalog table: %v", err)
	}
	if err := preflightPostgresRetainedDefaults(
		ctx,
		database,
		[]schema.Table{table},
	); err != nil {
		t.Fatalf("preflight PostgreSQL 16 rendered defaults: %v", err)
	}
}
