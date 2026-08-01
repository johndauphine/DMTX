package migrate

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresStage4RebuildFreshReplayAndConflictsLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN to run PostgreSQL rebuild replay live tests")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL rebuild DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL rebuild target: %T", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL rebuild target: %T", err)
	}
	namespace := "dmtx_s4_rebuild_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL rebuild schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		_, _ = database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE",
		)
	})

	table := schema.Table{
		Schema: namespace,
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "code", Type: "text"},
			{Name: "payload", Type: "text"},
		},
	}
	qualified := postgresQualified(namespace, table.Name)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+qualified+" ("+
			`"id" integer NOT NULL PRIMARY KEY, `+
			`"code" text NOT NULL UNIQUE, `+
			`"payload" text NOT NULL)`,
	); err != nil {
		t.Fatalf("create PostgreSQL rebuild table: %v", err)
	}
	writer := newPostgresNativeWriter(database)
	columns := []string{"id", "code", "payload"}
	freshPage := [][]any{
		{int64(1), "alpha", "original"},
		{int64(2), "beta", "second"},
	}
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteFreshInsert,
		freshPage,
	)
	if err != nil {
		t.Fatalf("PostgreSQL fresh rebuild page: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)

	// Model a lost acknowledgement by replaying the committed page. A changed
	// source payload is deliberately supplied to prove replay never updates.
	receipt, err = writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteDuplicateSafeInsertOnly,
		[][]any{
			{int64(1), "alpha", "must-not-update"},
			{int64(2), "beta", "must-not-update"},
		},
	)
	if err != nil {
		t.Fatalf("PostgreSQL issued-page replay: %v", err)
	}
	assertPostgresReceipt(t, receipt, CommitDurable, 2, 2)
	stage4AssertPostgresRebuildRow(
		t,
		ctx,
		database,
		qualified,
		1,
		"alpha",
		"original",
	)

	if _, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteFreshInsert,
		[][]any{{int64(1), "alpha", "fresh-conflict"}},
	); err == nil {
		t.Fatal("PostgreSQL fresh rebuild conflict succeeded")
	}
	if _, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteDuplicateSafeInsertOnly,
		[][]any{{int64(3), "alpha", "secondary-conflict"}},
	); err == nil {
		t.Fatal("PostgreSQL replay ignored a secondary UNIQUE conflict")
	}
	stage4AssertPostgresRebuildRow(
		t,
		ctx,
		database,
		qualified,
		1,
		"alpha",
		"original",
	)
}

func TestMySQLStage4RebuildFreshReplayAndConflictsLiveTLS(t *testing.T) {
	testMySQLFamilyStage4RebuildFreshReplayAndConflictsLive(
		t,
		stage4MySQLNetworkLiveFixture{
			name:       "MySQL",
			dsnEnv:     "DMTX_TEST_MYSQL_TARGET_DSN",
			caEnv:      "DMTX_TEST_MYSQL_CA",
			required:   "MYSQL_REQUIRED",
			tlsConfig:  "dmtx_test",
			flavor:     engine.MySQLServerFlavorOracle80,
			namePrefix: "dmtx_s4_mysql_rebuild_",
		},
	)
}

func TestMariaDBStage4RebuildFreshReplayAndConflictsLiveTLS(t *testing.T) {
	testMySQLFamilyStage4RebuildFreshReplayAndConflictsLive(
		t,
		stage4MySQLNetworkLiveFixture{
			name:       "MariaDB",
			dsnEnv:     "DMTX_TEST_MARIADB_TARGET_DSN",
			caEnv:      "DMTX_TEST_MARIADB_CA",
			required:   "MARIADB_REQUIRED",
			tlsConfig:  "dmtx_mariadb_test",
			flavor:     engine.MySQLServerFlavorMariaDB1011,
			namePrefix: "dmtx_s4_maria_rebuild_",
		},
	)
}

func testMySQLFamilyStage4RebuildFreshReplayAndConflictsLive(
	t *testing.T,
	fixture stage4MySQLNetworkLiveFixture,
) {
	t.Helper()
	values := stage4NetworkLiveEnvironment(
		t,
		fixture.required,
		fixture.dsnEnv,
		fixture.caEnv,
	)
	dsn, caPath := values[0], values[1]
	registerMySQLCommonFixtureTLSNamed(t, caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		fixture.name+" Stage 4 rebuild target",
		dsn,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		fixture.name+" Stage 4 rebuild target",
		dsn,
		fixture.flavor == engine.MySQLServerFlavorOracle80,
	)
	stage4ConfigureMySQLFamilyTargetSession(t, ctx, database, fixture)
	tableName := fixture.namePrefix +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, database, tableName)
	tableCollation := "utf8mb4_0900_bin"
	if fixture.flavor == engine.MySQLServerFlavorMariaDB1011 {
		tableCollation = "utf8mb4_nopad_bin"
	}
	table := schema.Table{
		Schema:         parsed.DBName,
		Name:           tableName,
		MySQLCollation: tableCollation,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name:         "code",
				Type:         "varchar",
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{64}},
			},
			{
				Name:         "payload",
				Type:         "varchar",
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{64}},
			},
		},
	}
	qualified := mySQLQualified(parsed.DBName, tableName)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+qualified+" ("+
			"`id` BIGINT NOT NULL, "+
			"`code` VARCHAR(64) NOT NULL, "+
			"`payload` VARCHAR(64) NOT NULL, "+
			"PRIMARY KEY (`id`)) "+
			"ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE="+
			tableCollation+" ROW_FORMAT=DYNAMIC",
	); err != nil {
		t.Fatalf("create %s rebuild table: %v", fixture.name, err)
	}
	writer := newMySQLNativeWriterForFlavor(database, fixture.flavor)
	columns := []string{"id", "code", "payload"}
	freshPage := [][]any{
		{int64(1), "alpha", "original"},
		{int64(2), "beta", "second"},
	}
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteFreshInsert,
		freshPage,
	)
	if err != nil {
		t.Fatalf("%s fresh rebuild page: %v", fixture.name, err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 2, 2)
	receipt, err = writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteDuplicateSafeInsertOnly,
		[][]any{
			{int64(1), "alpha", "must-not-update"},
			{int64(2), "beta", "must-not-update"},
		},
	)
	if err != nil {
		t.Fatalf("%s issued-page replay: %v", fixture.name, err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitDurable, 2, 2)
	stage4AssertMySQLRebuildRow(
		t,
		ctx,
		database,
		qualified,
		1,
		"alpha",
		"original",
	)
	if _, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteFreshInsert,
		[][]any{{int64(1), "alpha", "fresh-conflict"}},
	); err == nil {
		t.Fatalf("%s fresh rebuild conflict succeeded", fixture.name)
	}
	// No secondary-UNIQUE conflict case here, deliberately. A rebuild load
	// page runs before the set-wide finalizer creates secondary objects, so
	// stage4RebuildNetworkGuard proves the load-time shape with those objects
	// excluded. Seeding a UNIQUE key up front and then asserting it fires would
	// be asserting a state a real rebuild never occupies — and it is what made
	// this test fail against correct product behaviour. Secondary-object
	// conflicts belong to post-finalize coverage.
	stage4AssertMySQLRebuildRow(
		t,
		ctx,
		database,
		qualified,
		1,
		"alpha",
		"original",
	)
}

func TestSQLServerStage4RebuildCompositePKReplayAndConflictsLiveTLS(
	t *testing.T,
) {
	values := stage4NetworkLiveEnvironment(
		t,
		"MSSQL_REQUIRED",
		"DMTX_TEST_MSSQL_TARGET_DSN",
		"DMTX_TEST_MSSQL_CA",
	)
	endpoint := sqlServerCommonFixtureEndpoint(t, values[0], values[1])
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"Stage 4 composite-key rebuild target",
		endpoint,
	)
	tableName := "dmtx_s4_mssql_rebuild_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupSQLServerNativeTables(t, database, tableName)
	uniqueName := "ux_" + tableName + "_code"
	table := schema.Table{
		Schema: "dbo",
		Name:   tableName,
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name:         "code",
				Type:         "text",
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{64}},
			},
			{
				Name:         "payload",
				Type:         "text",
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{64}},
			},
		},
		Indexes: []schema.Index{{
			Name:    uniqueName,
			Unique:  true,
			Columns: []schema.IndexColumn{{Name: "code"}},
		}},
	}
	create, err := schema.CreateSQLServerTable(table)
	if err != nil {
		t.Fatalf("plan SQL Server rebuild table: %v", err)
	}
	if _, err := database.ExecContext(ctx, create); err != nil {
		t.Fatalf("create SQL Server rebuild table: %v", err)
	}
	// The planned table keeps its secondary UNIQUE index so the plan stays
	// realistic, but that object is deliberately not created yet: a rebuild
	// creates secondary objects in the set-wide finalizer, after the data
	// pages land. stage4RebuildNetworkGuard proves the load-time shape with
	// secondary objects excluded, so materializing them up front describes a
	// state a real rebuild never occupies.
	if _, err := schema.PlanSQLServerDropRecreateObjects(
		[]schema.Table{table},
	); err != nil {
		t.Fatalf("plan SQL Server rebuild objects: %v", err)
	}
	qualified := sqlServerQualified("dbo", tableName)
	writer := newSQLServerNativeWriter(database)
	columns := []string{"tenant_id", "id", "code", "payload"}
	freshPage := [][]any{
		{int64(7), int64(1), "alpha", "original"},
		{int64(7), int64(2), "beta", "second"},
	}
	receipt, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteFreshInsert,
		freshPage,
	)
	if err != nil {
		t.Fatalf("SQL Server fresh rebuild page: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 2, 2)
	receipt, err = writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteDuplicateSafeInsertOnly,
		[][]any{
			{int64(7), int64(1), "alpha", "must-not-update"},
			{int64(7), int64(2), "beta", "must-not-update"},
		},
	)
	if err != nil {
		t.Fatalf("SQL Server composite-key issued-page replay: %v", err)
	}
	assertSQLServerNativeReceipt(t, receipt, CommitDurable, 2, 2)
	stage4AssertSQLServerRebuildRow(
		t,
		ctx,
		database,
		qualified,
		7,
		1,
		"alpha",
		"original",
	)
	if _, err := writer.WriteStage4NetworkRebuildBatch(
		ctx,
		table,
		columns,
		NetworkWriteFreshInsert,
		[][]any{{int64(7), int64(1), "alpha", "fresh-conflict"}},
	); err == nil {
		t.Fatal("SQL Server fresh rebuild conflict succeeded")
	}
	// The secondary-UNIQUE conflict case is deliberately absent for the same
	// reason the index is not created above; it belongs to post-finalize
	// coverage, not to a load page.
	stage4AssertSQLServerRebuildRow(
		t,
		ctx,
		database,
		qualified,
		7,
		1,
		"alpha",
		"original",
	)
}

func stage4AssertPostgresRebuildRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	qualified string,
	id int64,
	wantCode string,
	wantPayload string,
) {
	t.Helper()
	var code, payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT code, payload FROM "+qualified+" WHERE id = $1",
		id,
	).Scan(&code, &payload); err != nil {
		t.Fatalf("read PostgreSQL rebuild row: %v", err)
	}
	if code != wantCode || payload != wantPayload {
		t.Fatalf("PostgreSQL rebuild row = (%q, %q)", code, payload)
	}
}

func stage4AssertMySQLRebuildRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	qualified string,
	id int64,
	wantCode string,
	wantPayload string,
) {
	t.Helper()
	var code, payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT `code`, `payload` FROM "+qualified+" WHERE `id` = ?",
		id,
	).Scan(&code, &payload); err != nil {
		t.Fatalf("read MySQL-family rebuild row: %v", err)
	}
	if code != wantCode || payload != wantPayload {
		t.Fatalf("MySQL-family rebuild row = (%q, %q)", code, payload)
	}
}

func stage4AssertSQLServerRebuildRow(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	qualified string,
	tenantID int64,
	id int64,
	wantCode string,
	wantPayload string,
) {
	t.Helper()
	var code, payload string
	if err := database.QueryRowContext(
		ctx,
		"SELECT [code], [payload] FROM "+qualified+
			" WHERE [tenant_id] = @p1 AND [id] = @p2",
		tenantID,
		id,
	).Scan(&code, &payload); err != nil {
		t.Fatalf("read SQL Server rebuild row: %v", err)
	}
	if code != wantCode || payload != wantPayload {
		t.Fatalf("SQL Server rebuild row = (%q, %q)", code, payload)
	}
}
