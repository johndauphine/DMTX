package migrate

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestPostgresRetainedCheckPreflightRejectsLogicalDriftLive(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run retained PostgreSQL CHECK preflight tests",
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
		"CHECK",
		func(
			ctx context.Context,
			database *sql.DB,
			namespace string,
		) error {
			name, err := readPostgresRichBalanceCheckName(
				ctx,
				database,
				namespace,
			)
			if err != nil {
				return err
			}
			if _, err := database.ExecContext(
				ctx,
				"ALTER TABLE "+
					postgresQualified(namespace, "accounts")+
					" DROP CONSTRAINT "+postgresIdentifier(name),
			); err != nil {
				return err
			}
			_, err = database.ExecContext(
				ctx,
				"ALTER TABLE "+
					postgresQualified(namespace, "accounts")+
					" ADD CONSTRAINT "+postgresIdentifier(name)+
					" CHECK ("+postgresIdentifier("balance")+
					" >= -1)",
			)
			return err
		},
	)
}

func readPostgresRichBalanceCheckName(
	ctx context.Context,
	database *sql.DB,
	namespace string,
) (string, error) {
	var name string
	err := database.QueryRowContext(
		ctx,
		`SELECT constraint_object.conname
		   FROM pg_catalog.pg_constraint AS constraint_object
		   JOIN pg_catalog.pg_class AS table_relation
		     ON table_relation.oid = constraint_object.conrelid
		   JOIN pg_catalog.pg_namespace AS table_namespace
		     ON table_namespace.oid = table_relation.relnamespace
		  WHERE table_namespace.nspname = $1
		    AND table_relation.relname = 'accounts'
		    AND constraint_object.contype = 'c'
		    AND pg_catalog.pg_get_expr(
		          constraint_object.conbin,
		          constraint_object.conrelid,
		          true
		        ) LIKE '%balance%'`,
		namespace,
	).Scan(&name)
	return name, err
}
