package app

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/johndauphine/dmtx/internal/config"
)

func TestPostgresPreflightPrivilegesLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL preflight privilege sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL preflight DSN: %T", err)
	}
	requirePostgresPreflightTLS(t, parsed)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	administrator, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL preflight administrator: %T", err)
	}
	t.Cleanup(func() {
		if err := administrator.Close(); err != nil {
			t.Errorf("close PostgreSQL preflight administrator: %v", err)
		}
	})
	if err := administrator.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL preflight administrator: %T", err)
	}
	requirePostgresPreflightSessionTLS(t, ctx, administrator)

	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	namespace := "dmtx_preflight_" + suffix
	principal := "dmtx_preflight_role_" + suffix
	password := "DmtxPreflight-" + suffix
	quotedNamespace := quotePostgresTestIdentifier(namespace)
	quotedPrincipal := quotePostgresTestIdentifier(principal)
	if _, err := administrator.ExecContext(
		ctx,
		"CREATE ROLE "+quotedPrincipal+" LOGIN PASSWORD "+
			quotePostgresTestLiteral(password),
	); err != nil {
		t.Fatalf("create PostgreSQL preflight role: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		_, _ = administrator.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+quotedNamespace+" CASCADE",
		)
		_, _ = administrator.ExecContext(
			cleanupCtx,
			"DROP OWNED BY "+quotedPrincipal,
		)
		if _, err := administrator.ExecContext(
			cleanupCtx,
			"DROP ROLE IF EXISTS "+quotedPrincipal,
		); err != nil {
			t.Errorf("drop PostgreSQL preflight role: %v", err)
		}
	})
	if _, err := administrator.ExecContext(
		ctx,
		"CREATE SCHEMA "+quotedNamespace+"; "+
			"CREATE TABLE "+quotedNamespace+
			".items (id bigint PRIMARY KEY, value text NOT NULL); "+
			"GRANT USAGE ON SCHEMA "+quotedNamespace+" TO "+
			quotedPrincipal+"; "+
			"GRANT SELECT, INSERT ON "+quotedNamespace+
			".items TO "+quotedPrincipal,
	); err != nil {
		t.Fatalf("prepare PostgreSQL preflight privilege fixture: %v", err)
	}

	limitedConfig := parsed.Copy()
	limitedConfig.User = principal
	limitedConfig.Password = password
	limited := stdlib.OpenDB(*limitedConfig)
	t.Cleanup(func() {
		if err := limited.Close(); err != nil {
			t.Errorf("close PostgreSQL preflight role: %v", err)
		}
	})
	if err := limited.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL preflight role: %T", err)
	}
	requirePostgresPreflightSessionTLS(t, ctx, limited)
	endpoint := config.Endpoint{
		Type:     "postgres",
		Schema:   namespace,
		Database: parsed.Database,
	}

	write, create := probeTargetPrivileges(
		ctx,
		limited,
		endpoint,
		"upsert",
	)
	if write || create {
		t.Fatalf(
			"SELECT+INSERT-only PostgreSQL privileges = write:%v create:%v",
			write,
			create,
		)
	}
	if probeTargetDeletePrivilege(ctx, limited, endpoint) {
		t.Fatal("PostgreSQL delete preflight accepted a role without DELETE")
	}

	if _, err := administrator.ExecContext(
		ctx,
		"GRANT UPDATE, DELETE ON "+quotedNamespace+
			".items TO "+quotedPrincipal,
	); err != nil {
		t.Fatalf("grant exact PostgreSQL upsert/delete privileges: %v", err)
	}
	write, create = probeTargetPrivileges(
		ctx,
		limited,
		endpoint,
		"upsert",
	)
	if !write || create {
		t.Fatalf(
			"complete PostgreSQL upsert privileges = write:%v create:%v",
			write,
			create,
		)
	}
	if !probeTargetDeletePrivilege(ctx, limited, endpoint) {
		t.Fatal("PostgreSQL delete preflight rejected an exact DELETE grant")
	}

	if _, err := administrator.ExecContext(
		ctx,
		"GRANT CREATE ON SCHEMA "+quotedNamespace+" TO "+
			quotedPrincipal+"; GRANT TRUNCATE ON "+quotedNamespace+
			".items TO "+quotedPrincipal,
	); err != nil {
		t.Fatalf("grant PostgreSQL destructive privileges: %v", err)
	}
	write, create = probeTargetPrivileges(
		ctx,
		limited,
		endpoint,
		"drop_recreate",
	)
	if write || !create {
		t.Fatalf(
			"non-owner PostgreSQL destructive privileges = write:%v create:%v",
			write,
			create,
		)
	}
	if _, err := administrator.ExecContext(
		ctx,
		"ALTER TABLE "+quotedNamespace+".items OWNER TO "+quotedPrincipal,
	); err != nil {
		t.Fatalf("transfer PostgreSQL fixture ownership: %v", err)
	}
	write, create = probeTargetPrivileges(
		ctx,
		limited,
		endpoint,
		"drop_recreate",
	)
	if !write || !create {
		t.Fatalf(
			"owner PostgreSQL destructive privileges = write:%v create:%v",
			write,
			create,
		)
	}
}

func requirePostgresPreflightTLS(t *testing.T, parsed *pgx.ConnConfig) {
	t.Helper()
	requireVerified := func(label string, tlsConfig *tls.Config) {
		t.Helper()
		if tlsConfig == nil || tlsConfig.InsecureSkipVerify ||
			tlsConfig.RootCAs == nil ||
			strings.TrimSpace(tlsConfig.ServerName) == "" {
			t.Fatalf(
				"%s DMTX_TEST_POSTGRES_DSN must require verified TLS",
				label,
			)
		}
	}
	requireVerified("primary", parsed.TLSConfig)
	for _, fallback := range parsed.Fallbacks {
		requireVerified("fallback", fallback.TLSConfig)
	}
}

func requirePostgresPreflightSessionTLS(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
) {
	t.Helper()
	var active bool
	if err := database.QueryRowContext(
		ctx,
		`SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()`,
	).Scan(&active); err != nil {
		t.Fatalf("inspect PostgreSQL preflight TLS: %v", err)
	}
	if !active {
		t.Fatal("PostgreSQL preflight session is not encrypted")
	}
}

func quotePostgresTestLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
