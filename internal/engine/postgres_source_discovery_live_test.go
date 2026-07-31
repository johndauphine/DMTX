package engine

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestInspectPostgres16SourceSchemaLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run PostgreSQL source discovery tests",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if parsed.TLSConfig == nil {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	if !postgresTLSConfigVerifiesServer(parsed.TLSConfig) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate",
		)
	}
	for _, fallback := range parsed.Fallbacks {
		if fallback.TLSConfig == nil {
			t.Fatal("DMTX_TEST_POSTGRES_DSN fallback must require TLS")
		}
		if !postgresTLSConfigVerifiesServer(fallback.TLSConfig) {
			t.Fatal(
				"DMTX_TEST_POSTGRES_DSN fallback must verify the PostgreSQL server certificate",
			)
		}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL discovery connection: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL discovery connection: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL discovery connection: %T", err)
	}
	if err := VerifyPostgres16Source(ctx, database); err != nil {
		t.Fatal(err)
	}

	namespace := fmt.Sprintf(
		"dmtx_pg_source_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresTestIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL discovery schema: %v", err)
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
				postgresTestIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL discovery schema: %v", err)
		}
	})

	fixture := postgresSourceDiscoveryFixture(t, namespace)
	for _, table := range fixture {
		statement, err := schema.CreateTable(schema.Postgres, table)
		if err != nil {
			t.Fatalf("render source table %s: %v", table.Name, err)
		}
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create source table %s: %v", table.Name, err)
		}
	}
	objects, err := schema.PlanPostgresDropRecreateObjects(
		fixture,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatalf("plan source objects: %v", err)
	}
	for _, object := range objects {
		if _, err := database.ExecContext(ctx, object.SQL()); err != nil {
			t.Fatalf(
				"create source object %s on %s: %v",
				object.Name(),
				object.Table(),
				err,
			)
		}
	}
	if _, err := database.ExecContext(
		ctx,
		`SELECT pg_catalog.setval(
			pg_catalog.pg_get_serial_sequence($1, $2),
			41,
			true
		)`,
		namespace+".accounts",
		"id",
	); err != nil {
		t.Fatalf("set source identity frontier: %v", err)
	}

	names, err := ListPostgresTables(ctx, database, namespace)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"account_events", "accounts"}) {
		t.Fatalf("source table names = %#v", names)
	}

	discovered := make([]schema.Table, 0, len(names))
	for _, name := range names {
		table, err := InspectPostgresTable(
			ctx,
			database,
			namespace,
			name,
		)
		if err != nil {
			t.Fatalf("inspect source table %s: %v", name, err)
		}
		discovered = append(discovered, table)
	}
	assertPostgresSourceDiscoveryFixture(t, discovered)
	for _, table := range discovered {
		if _, err := schema.CreateTable(
			schema.Postgres,
			table,
		); err != nil {
			t.Fatalf("render discovered table %s: %v", table.Name, err)
		}
	}
	if _, err := schema.PlanPostgresDropRecreateObjects(
		discovered,
		schema.PostgresObjectPlanOptions{},
	); err != nil {
		t.Fatalf("plan discovered PostgreSQL objects: %v", err)
	}

	t.Run("unbounded varchar fails closed", func(t *testing.T) {
		name := "unsupported_varchar"
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+
				postgresTestQualified(namespace, name)+
				` ("id" bigint PRIMARY KEY, "value" varchar)`,
		); err != nil {
			t.Fatal(err)
		}
		_, err := InspectPostgresTable(
			ctx,
			database,
			namespace,
			name,
		)
		assertPostgresSourcePolicyError(t, err, "type modifier")
	})
	t.Run("partial index fails closed", func(t *testing.T) {
		name := "unsupported_partial"
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+
				postgresTestQualified(namespace, name)+
				` ("id" bigint PRIMARY KEY, "value" bigint NOT NULL); `+
				"CREATE INDEX "+
				postgresTestIdentifier(name+"_idx")+" ON "+
				postgresTestQualified(namespace, name)+
				` ("value") WHERE "value" > 0`,
		); err != nil {
			t.Fatal(err)
		}
		_, err := InspectPostgresTable(
			ctx,
			database,
			namespace,
			name,
		)
		assertPostgresSourcePolicyError(t, err, "index catalog shape")
	})
	t.Run("identity always fails closed", func(t *testing.T) {
		name := "unsupported_identity"
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+
				postgresTestQualified(namespace, name)+
				` ("id" bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)`,
		); err != nil {
			t.Fatal(err)
		}
		_, err := InspectPostgresTable(
			ctx,
			database,
			namespace,
			name,
		)
		assertPostgresSourcePolicyError(t, err, "identity generation")
	})
}

func postgresTLSConfigVerifiesServer(config *tls.Config) bool {
	return config != nil &&
		config.RootCAs != nil &&
		(!config.InsecureSkipVerify || config.VerifyPeerCertificate != nil)
}

func postgresSourceDiscoveryFixture(
	t *testing.T,
	namespace string,
) []schema.Table {
	t.Helper()
	parseDefault := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatalf("parse fixture default %q: %v", value, err)
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
						Arguments: []int{40},
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
						Arguments: []int{12},
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
				{
					Name: "rounded_bucket",
					Type: "numeric",
					DeclaredType: &schema.DeclaredType{
						Base:      "numeric",
						Arguments: []int{2, -3},
					},
				},
				{
					Name: "fractional_ratio",
					Type: "numeric",
					DeclaredType: &schema.DeclaredType{
						Base:      "numeric",
						Arguments: []int{3, 5},
					},
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

func assertPostgresSourceDiscoveryFixture(
	t *testing.T,
	tables []schema.Table,
) {
	t.Helper()
	if len(tables) != 2 ||
		tables[0].Name != "account_events" ||
		tables[1].Name != "accounts" {
		t.Fatalf("discovered tables = %#v", tables)
	}
	events := tables[0]
	if events.Columns[0].PrimaryKeyPosition != 1 ||
		events.Columns[1].PrimaryKeyPosition != 2 ||
		len(events.ForeignKeys) != 1 {
		t.Fatalf("event metadata = %#v", events)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name != "account_events_account_fkey" ||
		!reflect.DeepEqual(foreignKey.Columns, []string{"account_id"}) ||
		foreignKey.ReferencedTable != "accounts" ||
		!reflect.DeepEqual(
			foreignKey.ReferencedColumns,
			[]string{"id"},
		) ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "RESTRICT" ||
		foreignKey.Match != "SIMPLE" {
		t.Fatalf("foreign key = %#v", foreignKey)
	}

	accounts := tables[1]
	if accounts.Identity == nil ||
		accounts.Identity.Column != "id" ||
		accounts.Identity.Generation != schema.IdentityByDefault ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 ||
		accounts.Columns[0].PrimaryKeyPosition != 1 {
		t.Fatalf("account identity = %#v", accounts.Identity)
	}
	if accounts.Columns[1].DeclaredType == nil ||
		!reflect.DeepEqual(
			accounts.Columns[1].DeclaredType.Arguments,
			[]int{12},
		) ||
		accounts.Columns[2].DeclaredType == nil ||
		!reflect.DeepEqual(
			accounts.Columns[2].DeclaredType.Arguments,
			[]int{12, 2},
		) ||
		accounts.Columns[5].DeclaredType == nil ||
		!reflect.DeepEqual(
			accounts.Columns[5].DeclaredType.Arguments,
			[]int{3},
		) ||
		accounts.Columns[7].DeclaredType == nil ||
		!reflect.DeepEqual(
			accounts.Columns[7].DeclaredType.Arguments,
			[]int{2, -3},
		) ||
		accounts.Columns[8].DeclaredType == nil ||
		!reflect.DeepEqual(
			accounts.Columns[8].DeclaredType.Arguments,
			[]int{3, 5},
		) {
		t.Fatalf("account modifiers = %#v", accounts.Columns)
	}
	for _, position := range []int{1, 2, 3, 4, 5} {
		if accounts.Columns[position].Default == nil {
			t.Fatalf(
				"account column %s lost its default",
				accounts.Columns[position].Name,
			)
		}
	}
	if len(accounts.Indexes) != 1 ||
		accounts.Indexes[0].Name != "accounts_code_uq" ||
		!accounts.Indexes[0].Unique ||
		!reflect.DeepEqual(
			accounts.Indexes[0].Columns,
			[]schema.IndexColumn{{
				Name:      "code",
				Collation: "BINARY",
			}},
		) ||
		len(accounts.Checks) != 1 ||
		accounts.Checks[0].Name != "accounts_balance_check" {
		t.Fatalf("account objects = %#v", accounts)
	}
}

func assertPostgresSourcePolicyError(
	t *testing.T,
	err error,
	operation string,
) {
	t.Helper()
	var policy *schema.PolicyError
	if !errors.As(err, &policy) ||
		policy.Operation !=
			"discover PostgreSQL source "+operation {
		t.Fatalf(
			"error = %v, want PostgreSQL source policy operation %q",
			err,
			operation,
		)
	}
}

func postgresTestIdentifier(value string) string {
	result := `"`
	for _, character := range value {
		if character == '"' {
			result += `""`
		} else {
			result += string(character)
		}
	}
	return result + `"`
}

func postgresTestQualified(namespace, name string) string {
	return postgresTestIdentifier(namespace) + "." +
		postgresTestIdentifier(name)
}
