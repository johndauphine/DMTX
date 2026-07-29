package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresNativeWriterLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN to run live PostgreSQL COPY tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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

	namespace := fmt.Sprintf("dmtx_native_%d", time.Now().UnixNano())
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create live PostgreSQL schema: %v", err)
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
			t.Errorf("drop live PostgreSQL schema: %v", err)
		}
	})

	writer := newPostgresNativeWriter(database)
	t.Run("copy and staged upsert", func(t *testing.T) {
		table := schema.Table{
			Schema: namespace,
			Name:   "events",
			Columns: []schema.Column{
				{
					Name:               "id",
					Type:               "integer",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
				{Name: "payload", Type: "text"},
				{Name: "enabled", Type: "boolean"},
				{Name: "amount", Type: "numeric"},
				{Name: "document", Type: "jsonb"},
				{Name: "raw", Type: "bytea"},
			},
		}
		columns := []string{
			"id",
			"payload",
			"enabled",
			"amount",
			"document",
			"raw",
		}
		createPostgresNativeLiveTable(t, ctx, database, table)
		rows := [][]any{
			{
				int64(1),
				"first",
				true,
				"12345678901234567890.1234567890",
				`{"row":1}`,
				[]byte{0, 1, 2},
			},
			{
				int64(2),
				"second",
				false,
				"-0.0000000001",
				`{"row":2}`,
				[]byte{3, 4, 5},
			},
		}
		receipt, err := writer.WriteBatch(
			ctx,
			table,
			columns,
			"drop_recreate",
			rows,
		)
		if err != nil {
			t.Fatalf("native COPY: %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)
		if count := postgresNativeLiveCount(
			t,
			ctx,
			database,
			table,
		); count != 2 {
			t.Fatalf("COPY count = %d, want 2", count)
		}

		upsertRows := [][]any{
			{
				int64(1),
				"updated",
				false,
				"99999999999999999999.9999999999",
				`{"row":"updated"}`,
				[]byte{9},
			},
			{
				int64(3),
				"inserted",
				true,
				"3.0000000000",
				`{"row":3}`,
				[]byte{8},
			},
		}
		receipt, err = writer.WriteBatch(
			ctx,
			table,
			columns,
			"upsert",
			upsertRows,
		)
		if err != nil {
			t.Fatalf("staged upsert: %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)
		if count := postgresNativeLiveCount(
			t,
			ctx,
			database,
			table,
		); count != 3 {
			t.Fatalf("upsert count = %d, want 3", count)
		}
		var payload string
		if err := database.QueryRowContext(
			ctx,
			"SELECT payload FROM "+
				postgresQualified(namespace, table.Name)+
				" WHERE id = 1",
		).Scan(&payload); err != nil {
			t.Fatalf("read updated row: %v", err)
		}
		if payload != "updated" {
			t.Fatalf("updated payload = %q, want updated", payload)
		}

		assertPostgresNativeStageAbsent(
			t,
			ctx,
			database,
			table,
			columns,
		)
	})

	t.Run("atomic COPY rollback", func(t *testing.T) {
		table := schema.Table{
			Schema: namespace,
			Name:   "checked_events",
			Columns: []schema.Column{
				{
					Name:               "id",
					Type:               "integer",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
			},
		}
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+
				postgresQualified(namespace, table.Name)+
				` ("id" INTEGER PRIMARY KEY CHECK ("id" > 0))`,
		); err != nil {
			t.Fatalf("create checked live table: %v", err)
		}
		receipt, err := writer.WriteBatch(
			ctx,
			table,
			[]string{"id"},
			"drop_recreate",
			[][]any{{int64(1)}, {int64(-1)}},
		)
		if err == nil {
			t.Fatal("constraint-violating COPY unexpectedly succeeded")
		}
		assertPostgresReceipt(t, receipt, CommitNotCommitted, 2, 0)
		if count := postgresNativeLiveCount(
			t,
			ctx,
			database,
			table,
		); count != 0 {
			t.Fatalf("rolled-back COPY count = %d, want 0", count)
		}
	})

	t.Run("staged upsert rollback", func(t *testing.T) {
		testPostgresNativeStagedUpsertRollback(
			t,
			ctx,
			database,
			writer,
			namespace,
		)
	})

	t.Run("quoted identifiers", func(t *testing.T) {
		testPostgresNativeQuotedIdentifiers(
			t,
			ctx,
			database,
			writer,
			namespace,
		)
	})
	t.Run("PostgreSQL special values", func(t *testing.T) {
		testPostgresNativeSpecialValues(
			t,
			ctx,
			database,
			writer,
			namespace,
		)
	})

	t.Run("key-only conflict no-op", func(t *testing.T) {
		table := schema.Table{
			Schema: namespace,
			Name:   "keys",
			Columns: []schema.Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
			},
		}
		createPostgresNativeLiveTable(t, ctx, database, table)
		if _, err := database.ExecContext(
			ctx,
			"INSERT INTO "+
				postgresQualified(namespace, table.Name)+
				` ("id") VALUES (1)`,
		); err != nil {
			t.Fatalf("seed key-only live table: %v", err)
		}
		receipt, err := writer.WriteBatch(
			ctx,
			table,
			[]string{"id"},
			"upsert",
			[][]any{{int64(1)}, {int64(2)}},
		)
		if err != nil {
			t.Fatalf("key-only staged upsert: %v", err)
		}
		assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)
		if count := postgresNativeLiveCount(
			t,
			ctx,
			database,
			table,
		); count != 2 {
			t.Fatalf("key-only count = %d, want 2", count)
		}
	})
}

func testPostgresNativeStagedUpsertRollback(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	writer *postgresNativeWriter,
	namespace string,
) {
	t.Helper()
	table := schema.Table{
		Schema: namespace,
		Name:   "guarded_events",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
		},
	}
	columns := []string{"id", "payload"}
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(namespace, table.Name)+
			` ("id" INTEGER PRIMARY KEY, `+
			`"payload" TEXT NOT NULL CHECK ("payload" <> 'forbidden'))`,
	); err != nil {
		t.Fatalf("create guarded live table: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+
			postgresQualified(namespace, table.Name)+
			` ("id", "payload") VALUES ($1, $2)`,
		1,
		"original",
	); err != nil {
		t.Fatalf("seed guarded live table: %v", err)
	}

	receipt, err := writer.WriteBatch(
		ctx,
		table,
		columns,
		"upsert",
		[][]any{
			{int64(1), "updated"},
			{int64(2), "forbidden"},
		},
	)
	if err == nil {
		t.Fatal("constraint-violating staged upsert unexpectedly succeeded")
	}
	assertPostgresReceipt(t, receipt, CommitNotCommitted, 2, 0)
	if count := postgresNativeLiveCount(
		t,
		ctx,
		database,
		table,
	); count != 1 {
		t.Fatalf("rolled-back staged upsert count = %d, want 1", count)
	}
	var payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT payload FROM "+
			postgresQualified(namespace, table.Name)+
			" WHERE id = 1",
	).Scan(&payload); err != nil {
		t.Fatalf("read row after staged rollback: %v", err)
	}
	if payload != "original" {
		t.Fatalf(
			"payload after staged rollback = %q, want original",
			payload,
		)
	}
	assertPostgresNativeStageAbsent(
		t,
		ctx,
		database,
		table,
		columns,
	)
}

func testPostgresNativeQuotedIdentifiers(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	writer *postgresNativeWriter,
	namespace string,
) {
	t.Helper()
	table := schema.Table{
		Schema: namespace,
		Name:   `quoted"events`,
		Columns: []schema.Column{
			{
				Name:               `id"key`,
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: `pay"load`, Type: "text"},
		},
	}
	columns := []string{`id"key`, `pay"load`}
	createPostgresNativeLiveTable(t, ctx, database, table)
	receipt, err := writer.WriteBatch(
		ctx,
		table,
		columns,
		"drop_recreate",
		[][]any{{int64(7), "quoted payload"}},
	)
	if err != nil {
		t.Fatalf("COPY with quoted identifiers: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 1, 1)

	var payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT "+postgresIdentifier(`pay"load`)+
			" FROM "+postgresQualified(table.Schema, table.Name)+
			" WHERE "+postgresIdentifier(`id"key`)+" = $1",
		7,
	).Scan(&payload); err != nil {
		t.Fatalf("read quoted COPY row: %v", err)
	}
	if payload != "quoted payload" {
		t.Fatalf("quoted COPY payload = %q", payload)
	}
}

func testPostgresNativeSpecialValues(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	writer *postgresNativeWriter,
	namespace string,
) {
	t.Helper()
	table := schema.Table{
		Schema: namespace,
		Name:   "special_values",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "float_nan", Type: "float8"},
			{Name: "float_pos", Type: "float8"},
			{Name: "float_neg", Type: "float8"},
			{Name: "numeric_nan", Type: "numeric"},
			{Name: "date_pos", Type: "date"},
			{Name: "date_neg", Type: "date"},
			{Name: "timestamp_pos", Type: "timestamp"},
			{Name: "timestamp_neg", Type: "timestamp"},
			{Name: "timestamptz_pos", Type: "timestamptz"},
			{Name: "timestamptz_neg", Type: "timestamptz"},
		},
	}
	columns := []string{
		"id",
		"float_nan",
		"float_pos",
		"float_neg",
		"numeric_nan",
		"date_pos",
		"date_neg",
		"timestamp_pos",
		"timestamp_neg",
		"timestamptz_pos",
		"timestamptz_neg",
	}
	createPostgresNativeLiveTable(t, ctx, database, table)
	receipt, err := writer.WriteBatch(
		ctx,
		table,
		columns,
		"drop_recreate",
		[][]any{{
			int64(1),
			math.NaN(),
			math.Inf(1),
			math.Inf(-1),
			"NaN",
			"infinity",
			"-infinity",
			"infinity",
			"-infinity",
			"infinity",
			"-infinity",
		}},
	)
	if err != nil {
		t.Fatalf("COPY PostgreSQL special values: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 1, 1)
	if count := postgresNativeLiveCount(
		t,
		ctx,
		database,
		table,
	); count != 1 {
		t.Fatalf("special-value COPY count = %d, want 1", count)
	}

	var got [10]string
	if err := database.QueryRowContext(
		ctx,
		`SELECT "float_nan"::text,
		        "float_pos"::text,
		        "float_neg"::text,
		        "numeric_nan"::text,
		        "date_pos"::text,
		        "date_neg"::text,
		        "timestamp_pos"::text,
		        "timestamp_neg"::text,
		        "timestamptz_pos"::text,
		        "timestamptz_neg"::text
		   FROM `+postgresQualified(table.Schema, table.Name)+
			` WHERE "id" = 1`,
	).Scan(
		&got[0],
		&got[1],
		&got[2],
		&got[3],
		&got[4],
		&got[5],
		&got[6],
		&got[7],
		&got[8],
		&got[9],
	); err != nil {
		t.Fatalf("read PostgreSQL special values: %v", err)
	}
	want := [10]string{
		"NaN",
		"Infinity",
		"-Infinity",
		"NaN",
		"infinity",
		"-infinity",
		"infinity",
		"-infinity",
		"infinity",
		"-infinity",
	}
	if got != want {
		t.Fatalf("special-value readback = %#v, want %#v", got, want)
	}
}

func createPostgresNativeLiveTable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) {
	t.Helper()
	statement, err := schema.CreateTable(schema.Postgres, table)
	if err != nil {
		t.Fatalf("plan live PostgreSQL table: %v", err)
	}
	if _, err := database.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create live PostgreSQL table: %v", err)
	}
}

func postgresNativeLiveCount(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) int {
	t.Helper()
	var count int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(table.Schema, table.Name),
	).Scan(&count); err != nil {
		t.Fatalf("count live PostgreSQL table: %v", err)
	}
	return count
}

func assertPostgresNativeStageAbsent(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
	columns []string,
) {
	t.Helper()
	stage := postgresStageTableName(table, columns)
	var stages int
	if err := database.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		   FROM pg_catalog.pg_class
		   JOIN pg_catalog.pg_namespace
		     ON pg_namespace.oid = pg_class.relnamespace
		  WHERE pg_class.relname = $1
		    AND pg_namespace.nspname LIKE 'pg_temp_%'`,
		stage,
	).Scan(&stages); err != nil {
		t.Fatalf("inspect temporary staging cleanup: %v", err)
	}
	if stages != 0 {
		t.Fatalf("temporary staging tables remaining = %d", stages)
	}
}
