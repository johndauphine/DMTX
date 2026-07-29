package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
)

type postgresRetainedObjectLiveMutation func(
	context.Context,
	*sql.DB,
	string,
) error

func TestPostgresRetainedIndexAndForeignKeyPreflightLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run retained PostgreSQL object preflight tests",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	tests := []struct {
		name   string
		object string
		mutate postgresRetainedObjectLiveMutation
		accept bool
	}{
		{
			name:   "ordinary unique index",
			accept: true,
		},
		{
			name:   "missing secondary index",
			object: "secondary index",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				_, err := database.ExecContext(
					ctx,
					"DROP INDEX "+
						postgresQualified(
							namespace,
							"accounts_status_balance_idx",
						),
				)
				return err
			},
		},
		{
			name:   "extra secondary index",
			object: "secondary index",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				_, err := database.ExecContext(
					ctx,
					"CREATE INDEX "+
						postgresIdentifier("dmtx_unexpected_accounts_idx")+
						" ON "+
						postgresQualified(namespace, "accounts")+
						" ("+postgresIdentifier("balance")+")",
				)
				return err
			},
		},
		{
			name:   "changed secondary index",
			object: "secondary index",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				if _, err := database.ExecContext(
					ctx,
					"DROP INDEX "+
						postgresQualified(
							namespace,
							"accounts_status_balance_idx",
						),
				); err != nil {
					return err
				}
				_, err := database.ExecContext(
					ctx,
					"CREATE INDEX "+
						postgresIdentifier("accounts_status_balance_idx")+
						" ON "+
						postgresQualified(namespace, "accounts")+
						" ("+postgresIdentifier("balance")+", "+
						postgresIdentifier("status")+")",
				)
				return err
			},
		},
		{
			name:   "changed unique null semantics",
			object: "secondary index",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				if _, err := database.ExecContext(
					ctx,
					"DROP INDEX "+
						postgresQualified(
							namespace,
							"accounts_external_id_uq",
						),
				); err != nil {
					return err
				}
				_, err := database.ExecContext(
					ctx,
					"CREATE UNIQUE INDEX "+
						postgresIdentifier("accounts_external_id_uq")+
						" ON "+postgresQualified(namespace, "accounts")+
						" ("+postgresIdentifier("external_id")+
						` COLLATE "pg_catalog"."C" ASC NULLS FIRST)`+
						" NULLS NOT DISTINCT",
				)
				return err
			},
		},
		{
			name:   "missing foreign key",
			object: "foreign key",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				name, err := readPostgresRichForeignKeyName(
					ctx,
					database,
					namespace,
				)
				if err != nil {
					return err
				}
				_, err = database.ExecContext(
					ctx,
					"ALTER TABLE "+
						postgresQualified(namespace, "account_events")+
						" DROP CONSTRAINT "+postgresIdentifier(name),
				)
				return err
			},
		},
		{
			name:   "extra foreign key",
			object: "foreign key",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				_, err := database.ExecContext(
					ctx,
					"ALTER TABLE "+
						postgresQualified(namespace, "account_events")+
						" ADD CONSTRAINT "+
						postgresIdentifier("dmtx_unexpected_account_fkey")+
						" FOREIGN KEY ("+
						postgresIdentifier("account_id")+
						") REFERENCES "+
						postgresQualified(namespace, "accounts")+
						" ("+postgresIdentifier("id")+")"+
						" ON UPDATE CASCADE ON DELETE RESTRICT",
				)
				return err
			},
		},
		{
			name:   "changed foreign key",
			object: "foreign key",
			mutate: func(
				ctx context.Context,
				database *sql.DB,
				namespace string,
			) error {
				name, err := readPostgresRichForeignKeyName(
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
						postgresQualified(namespace, "account_events")+
						" DROP CONSTRAINT "+postgresIdentifier(name),
				); err != nil {
					return err
				}
				_, err = database.ExecContext(
					ctx,
					"ALTER TABLE "+
						postgresQualified(namespace, "account_events")+
						" ADD CONSTRAINT "+postgresIdentifier(name)+
						" FOREIGN KEY ("+
						postgresIdentifier("account_id")+
						") REFERENCES "+
						postgresQualified(namespace, "accounts")+
						" ("+postgresIdentifier("id")+")"+
						" ON UPDATE CASCADE ON DELETE CASCADE",
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runPostgresRetainedObjectPreflightLiveCaseWithExpectation(
				t,
				dsn,
				parsed,
				test.object,
				test.mutate,
				test.accept,
			)
		})
	}
}

func runPostgresRetainedObjectPreflightLiveCase(
	t *testing.T,
	dsn string,
	parsed *pgx.ConnConfig,
	object string,
	mutate postgresRetainedObjectLiveMutation,
) {
	t.Helper()
	runPostgresRetainedObjectPreflightLiveCaseWithExpectation(
		t,
		dsn,
		parsed,
		object,
		mutate,
		false,
	)
}

func runPostgresRetainedObjectPreflightLiveCaseWithExpectation(
	t *testing.T,
	dsn string,
	parsed *pgx.ConnConfig,
	object string,
	mutate postgresRetainedObjectLiveMutation,
	accept bool,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
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
		"dmtx_retained_objects_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create retained-object test schema: %v", err)
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
			t.Errorf("drop retained-object test schema: %v", err)
		}
	})

	sourcePath := createSQLitePostgresRichSource(t, ctx)
	endpoint := config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	}
	if _, err := SQLiteToPostgresWithObserver(
		ctx,
		sqlitePostgresRichConfig(sourcePath, endpoint, "drop_recreate"),
		nil,
	); err != nil {
		t.Fatalf("prepare retained-object target: %v", err)
	}
	if mutate != nil {
		if err := mutate(ctx, database, namespace); err != nil {
			t.Fatalf("mutate retained PostgreSQL %s fixture: %v", object, err)
		}
	}
	updateSQLitePostgresRichSource(t, ctx, sourcePath)

	observer := &sqlitePostgresLiveObserver{}
	result, err := SQLiteToPostgresWithObserver(
		ctx,
		sqlitePostgresRichConfig(sourcePath, endpoint, "upsert"),
		observer,
	)
	if accept {
		if err != nil {
			t.Fatalf("ordinary retained unique index preflight: %v", err)
		}
		if result.Tables != 2 ||
			result.Rows != 7 ||
			!result.Validated {
			t.Fatalf(
				"ordinary retained unique index result = %+v",
				result,
			)
		}
		return
	}
	if err == nil || !strings.Contains(err.Error(), object) {
		t.Fatalf("retained %s preflight error = %v", object, err)
	}
	if len(observer.tableSets) != 0 ||
		len(observer.before) != 0 ||
		len(observer.after) != 0 {
		t.Fatalf(
			"retained %s mismatch reached callbacks: %#v",
			object,
			observer,
		)
	}

	var accountLabel string
	if err := database.QueryRowContext(
		ctx,
		"SELECT label FROM "+
			postgresQualified(namespace, "accounts")+" WHERE id = 2",
	).Scan(&accountLabel); err != nil {
		t.Fatalf("read account after rejected preflight: %v", err)
	}
	if accountLabel != "" {
		t.Fatalf(
			"account changed before rejected preflight: label = %q",
			accountLabel,
		)
	}
	var eventNote string
	if err := database.QueryRowContext(
		ctx,
		"SELECT note FROM "+
			postgresQualified(namespace, "account_events")+
			" WHERE account_id = 1 AND sequence_no = 2",
	).Scan(&eventNote); err != nil {
		t.Fatalf("read event after rejected preflight: %v", err)
	}
	if eventNote != "second" {
		t.Fatalf(
			"event changed before rejected preflight: note = %q",
			eventNote,
		)
	}
}

func readPostgresRichForeignKeyName(
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
		    AND table_relation.relname = 'account_events'
		    AND constraint_object.contype = 'f'`,
		namespace,
	).Scan(&name)
	if err != nil {
		return "", err
	}
	return name, nil
}
