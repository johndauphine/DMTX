package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLiteToPostgresDropRecreateRelationCollisionLive(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the live PostgreSQL relation-collision test",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must require TLS for the public route test",
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	sourcePath := createSQLitePostgresRelationCollisionSource(t, ctx)
	tests := []struct {
		name         string
		externalName string
		wantPlanKind string
	}{
		{
			name:         "planned post-load index",
			externalName: "accounts_external_id_uq",
			wantPlanKind: "post-load index",
		},
		{
			name:         "planned identity sequence",
			externalName: "accounts_id_seq",
			wantPlanKind: "identity sequence",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			namespace := fmt.Sprintf(
				"dmtx_pg_collision_%d_%d",
				os.Getpid(),
				time.Now().UnixNano(),
			)
			if _, err := database.ExecContext(
				ctx,
				"CREATE SCHEMA "+postgresIdentifier(namespace),
			); err != nil {
				t.Fatalf("create collision schema: %v", err)
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
					t.Errorf("drop collision schema: %v", err)
				}
			})

			if _, err := database.ExecContext(
				ctx,
				"CREATE TABLE "+
					postgresQualified(namespace, "accounts")+
					` (id BIGINT PRIMARY KEY, payload TEXT NOT NULL);
				 INSERT INTO `+
					postgresQualified(namespace, "accounts")+
					` (id, payload) VALUES (99, 'sentinel');
				 CREATE SEQUENCE `+
					postgresQualified(namespace, test.externalName),
			); err != nil {
				t.Fatalf("create collision target fixture: %v", err)
			}

			observer := &sqlitePostgresLiveObserver{}
			result, err := SQLiteToPostgresWithObserver(
				ctx,
				config.Config{
					Source: config.Endpoint{
						Type:     "sqlite",
						Database: sourcePath,
					},
					Target: config.Endpoint{
						Type:     "postgres",
						Host:     parsed.Host,
						Port:     int(parsed.Port),
						Database: parsed.Database,
						User:     parsed.User,
						Password: parsed.Password,
						Schema:   namespace,
					},
					Migration: config.Migration{
						TargetMode:    "drop_recreate",
						IncludeTables: []string{"accounts"},
					},
				},
				observer,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.externalName) ||
				!strings.Contains(err.Error(), test.wantPlanKind) ||
				!strings.Contains(
					err.Error(),
					"outside selected target tables",
				) {
				t.Fatalf("relation collision error = %v", err)
			}
			if result != (Result{}) {
				t.Fatalf("collision result = %+v, want zero result", result)
			}
			if len(observer.tableSets) != 0 ||
				len(observer.before) != 0 ||
				len(observer.after) != 0 {
				t.Fatalf(
					"callbacks occurred before relation collision rejection: table_sets=%#v before=%#v after=%#v",
					observer.tableSets,
					observer.before,
					observer.after,
				)
			}

			var sentinel string
			if err := database.QueryRowContext(
				ctx,
				"SELECT payload FROM "+
					postgresQualified(namespace, "accounts")+
					" WHERE id = 99",
			).Scan(&sentinel); err != nil {
				t.Fatalf("read sentinel after collision rejection: %v", err)
			}
			if sentinel != "sentinel" {
				t.Fatalf(
					"sentinel after collision rejection = %q",
					sentinel,
				)
			}
			var relationKind string
			if err := database.QueryRowContext(ctx, `
				SELECT relation.relkind::text
				FROM pg_catalog.pg_class AS relation
				JOIN pg_catalog.pg_namespace AS namespace
				  ON namespace.oid = relation.relnamespace
				WHERE namespace.nspname = $1
				  AND relation.relname = $2
			`, namespace, test.externalName).Scan(
				&relationKind,
			); err != nil {
				t.Fatalf("read external collision relation: %v", err)
			}
			if relationKind != "S" {
				t.Fatalf(
					"external collision relation kind = %q, want sequence",
					relationKind,
				)
			}
		})
	}
}

func createSQLitePostgresRelationCollisionSource(
	t *testing.T,
	ctx context.Context,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "relation-collision.sqlite")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open collision SQLite source: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE accounts (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			external_id TEXT NOT NULL
		);
		CREATE UNIQUE INDEX accounts_external_id_uq
			ON accounts(external_id);
		INSERT INTO accounts(id, external_id)
		VALUES (1, 'source');
	`); err != nil {
		_ = database.Close()
		t.Fatalf("create collision SQLite source: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close collision SQLite source: %v", err)
	}
	return path
}
