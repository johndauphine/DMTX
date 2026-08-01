package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresDatabaseValidationProbeStableTLSLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL deep-validation sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL validation DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL validation database: %T", err)
	}
	database.SetMaxOpenConns(6)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL validation database: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL validation database: %T", err)
	}

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceNamespace := "dmtx_validation_source_" + suffix
	targetNamespace := "dmtx_validation_target_" + suffix
	for _, namespace := range []string{
		sourceNamespace,
		targetNamespace,
	} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(namespace),
		); err != nil {
			t.Fatalf("create PostgreSQL validation schema: %v", err)
		}
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, namespace := range []string{
			sourceNamespace,
			targetNamespace,
		} {
			if _, err := database.ExecContext(
				cleanupCtx,
				"DROP SCHEMA IF EXISTS "+
					postgresIdentifier(namespace)+" CASCADE",
			); err != nil {
				t.Errorf(
					"drop PostgreSQL validation schema: %v",
					err,
				)
			}
		}
	})
	sourceQualified := postgresQualified(
		sourceNamespace,
		"items",
	)
	targetQualified := postgresQualified(
		targetNamespace,
		"items",
	)
	for _, qualified := range []string{
		sourceQualified,
		targetQualified,
	} {
		if _, err := database.ExecContext(ctx, `
			CREATE TABLE `+qualified+` (
				id bigint PRIMARY KEY,
				payload text,
				marker bytea NOT NULL
			)`); err != nil {
			t.Fatalf("create PostgreSQL validation table: %v", err)
		}
	}
	if _, err := database.ExecContext(ctx, `
		INSERT INTO `+sourceQualified+` (id, payload, marker)
		VALUES
			(1, 'alpha', decode('01', 'hex')),
			(2, NULL,    decode('02', 'hex')),
			(3, 'gamma', decode('03', 'hex'));
		INSERT INTO `+targetQualified+` (id, payload, marker)
		VALUES
			(1,  'alpha', decode('01', 'hex')),
			(2,  NULL,    decode('02', 'hex')),
			(3,  'gamma', decode('03', 'hex')),
			(99, NULL,    decode('63', 'hex'));
		ANALYZE `+sourceQualified+`;
		ANALYZE `+targetQualified); err != nil {
		t.Fatalf("load PostgreSQL validation fixture: %v", err)
	}

	transaction, err := database.BeginTx(
		context.WithoutCancel(ctx),
		&sql.TxOptions{
			Isolation: sql.LevelRepeatableRead,
			ReadOnly:  true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := transaction.Rollback(); err != nil &&
			!errors.Is(err, sql.ErrTxDone) {
			t.Errorf(
				"rollback PostgreSQL validation snapshot: %v",
				err,
			)
		}
	})
	var pinned int64
	if err := transaction.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+sourceQualified,
	).Scan(&pinned); err != nil || pinned != 3 {
		t.Fatalf(
			"pin PostgreSQL validation snapshot = %d, %v",
			pinned,
			err,
		)
	}

	sourceTable := adapterValidationPostgresLiveTable(
		sourceNamespace,
	)
	targetTable := adapterValidationPostgresLiveTable(
		targetNamespace,
	)
	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine:         adapterValidationPostgres,
			displayName:    "PostgreSQL",
			qualifiedTable: postgresQualified,
		},
		database:  database,
		namespace: sourceNamespace,
	}
	stable, err := newAdapterRetainedStableRelationalView(
		source,
		&adapterSQLTransactionStableView{
			transaction: transaction,
			engine:      adapterValidationPostgres,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := stable.bindTableScope(sourceTable); err != nil {
		t.Fatal(err)
	}
	target := &postgresTargetAdapter{
		database:  database,
		namespace: targetNamespace,
	}
	contract, err := stable.Stage4ValidationProbe(
		source,
		target,
		[]adapterTablePlan{{
			source: sourceTable,
			target: targetTable,
			columns: []string{
				"id",
				"payload",
				"marker",
			},
		}},
	)
	if err != nil {
		t.Fatalf("construct stable PostgreSQL validation probe: %v", err)
	}
	probe := contract.(*adapterDatabaseValidationProbe)
	proof, err := probe.Stage4ValidationPrimaryKeyEqualityProof(
		sourceTable,
	)
	if err != nil {
		t.Fatalf("certify PostgreSQL validation key: %v", err)
	}

	// This row is deliberately committed after the source snapshot was
	// pinned. Every source validation pass must continue to observe three
	// rows through the supplied stable view.
	if _, err := database.ExecContext(
		ctx,
		`INSERT INTO `+sourceQualified+
			` (id, payload, marker)
			 VALUES (4, 'later', decode('04', 'hex'))`,
	); err != nil {
		t.Fatalf("mutate live PostgreSQL source: %v", err)
	}
	sourceCount, err := probe.ExactCount(
		ctx,
		ValidationSource,
		sourceTable,
	)
	if err != nil || sourceCount != 3 {
		t.Fatalf(
			"stable PostgreSQL source count = %d, %v",
			sourceCount,
			err,
		)
	}
	targetCount, err := probe.ExactCount(
		ctx,
		ValidationTarget,
		sourceTable,
	)
	if err != nil || targetCount != 4 {
		t.Fatalf(
			"PostgreSQL target count = %d, %v",
			targetCount,
			err,
		)
	}
	for _, side := range []ValidationSide{
		ValidationSource,
		ValidationTarget,
	} {
		estimate, err := probe.EstimateCount(
			ctx,
			side,
			sourceTable,
		)
		if err != nil || estimate < 0 {
			t.Fatalf(
				"PostgreSQL %s estimate = %d, %v",
				side,
				estimate,
				err,
			)
		}
	}

	report, err := RunValidationCore(
		ctx,
		ValidationCoreOptions{
			Mode:              config.ValidationSample,
			TargetMode:        "upsert",
			FailOnMismatch:    true,
			FailOnTimeout:     true,
			ExactCountTimeout: 5 * time.Second,
			TableTimeout:      20 * time.Second,
			TableConcurrency:  1,
			SampleLimit:       3,
		},
		[]ValidationTableSpec{{
			Table: sourceTable,
			Projection: []string{
				"id",
				"payload",
				"marker",
			},
			PrimaryKeyEqualityProof: proof,
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run PostgreSQL deep validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf(
			"PostgreSQL deep validation report = %#v",
			report,
		)
	}
}

func adapterValidationPostgresLiveTable(
	namespace string,
) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   "items",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint",
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{
				Name: "payload", Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base: "text",
				},
				Nullable: true,
			},
			{
				Name: "marker", Type: "bytea",
				DeclaredType: &schema.DeclaredType{
					Base: "bytea",
				},
			},
		},
	}
}

// TestSQLServerToPostgresDatabaseValidationProbeLiveTLS exercises the real
// cross-driver probe. It is deliberately gated because it mutates isolated
// temporary objects in both TLS test containers.
func TestSQLServerToPostgresDatabaseValidationProbeLiveTLS(t *testing.T) {
	mssqlDSN, ca, pgDSN := os.Getenv("DMTX_TEST_MSSQL_DSN"), os.Getenv("DMTX_TEST_MSSQL_CA"), os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if mssqlDSN == "" || ca == "" || pgDSN == "" {
		t.Skip("set DMTX_TEST_MSSQL_DSN, DMTX_TEST_MSSQL_CA, and DMTX_TEST_POSTGRES_DSN")
	}
	pg, err := pgx.ParseConfig(pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	if !postgresRouteLiveRequiresTLS(pg) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	endpoint := sqlServerCommonFixtureEndpoint(t, mssqlDSN, ca)
	sourceDB, err := engine.OpenSQLServer2022Source(ctx, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sourceDB.Close() })
	targetDB, err := sql.Open("pgx", pgDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = targetDB.Close() })
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceName, ns := "dmtx_val_mssql_"+suffix, "dmtx_val_pg_"+suffix
	srcQ := sqlServerQualified("dbo", sourceName)
	t.Cleanup(func() {
		_, _ = sourceDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS "+srcQ)
		_, _ = targetDB.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+postgresIdentifier(ns)+" CASCADE")
	})
	if _, err = sourceDB.ExecContext(ctx, `CREATE TABLE `+srcQ+` ([id] BIGINT NOT NULL PRIMARY KEY,[payload] NVARCHAR(40) NULL,[marker] VARBINARY(8) NULL); INSERT INTO `+srcQ+` VALUES (1,N'alpha',0x01),(2,NULL,0x02)`); err != nil {
		t.Fatal(err)
	}
	if _, err = targetDB.ExecContext(ctx, `CREATE SCHEMA `+postgresIdentifier(ns)+`; CREATE TABLE `+postgresQualified(ns, sourceName)+` (id bigint PRIMARY KEY,payload text,marker bytea); INSERT INTO `+postgresQualified(ns, sourceName)+` VALUES (1,'alpha',decode('01','hex')),(2,NULL,decode('02','hex'))`); err != nil {
		t.Fatal(err)
	}
	table := schema.Table{Schema: "dbo", Name: sourceName, Columns: []schema.Column{{Name: "id", Type: "bigint", DeclaredType: &schema.DeclaredType{Base: "bigint"}, PrimaryKey: true, PrimaryKeyPosition: 1}, {Name: "payload", Type: "nvarchar", DeclaredType: &schema.DeclaredType{Base: "nvarchar"}, Nullable: true}, {Name: "marker", Type: "varbinary", DeclaredType: &schema.DeclaredType{Base: "varbinary"}, Nullable: true}}}
	targetTable := table
	targetTable.Schema = ns
	source := &relationalSourceAdapter{spec: relationalSourceSpec{engine: adapterValidationSQLServer}, database: sourceDB, namespace: "dbo"}
	target := &postgresTargetAdapter{database: targetDB, namespace: ns}
	contract, err := source.Stage4ValidationProbe(source, target, []adapterTablePlan{{source: table, target: targetTable, columns: []string{"id", "payload", "marker"}}})
	if err != nil {
		t.Fatal(err)
	}
	probe := contract.(*adapterDatabaseValidationProbe)
	proof, err := probe.Stage4ValidationPrimaryKeyEqualityProof(table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newValidationSampleDescriptor(table, []string{"id", "payload", "marker"}); err != nil {
		t.Fatalf("source sample descriptor: %v", err)
	}
	if count, err := probe.ExactCount(ctx, ValidationSource, table); err != nil || count != 2 {
		t.Fatalf("source exact count=%d err=%v", count, err)
	}
	if count, err := probe.ExactCount(ctx, ValidationTarget, table); err != nil || count != 2 {
		t.Fatalf("target exact count=%d err=%v", count, err)
	}
	if _, err := probe.NullCounts(ctx, ValidationSource, table, []string{"id", "payload", "marker"}, ValidationNullScope{Kind: ValidationNullScopeTransferredSource}); err != nil {
		t.Fatalf("source null parity: %v", err)
	}
	if rows, err := probe.SampleSourceRows(ctx, table, []string{"id", "payload", "marker"}, 2); err != nil || len(rows) != 2 {
		t.Fatalf("source sample=%#v err=%v", rows, err)
	}
	report, err := RunValidationCore(ctx, ValidationCoreOptions{Mode: config.ValidationSample, TargetMode: "upsert", FailOnMismatch: true, FailOnTimeout: true, ExactCountTimeout: 10 * time.Second, TableTimeout: 30 * time.Second, TableConcurrency: 1, SampleLimit: 2}, []ValidationTableSpec{{Table: table, Projection: []string{"id", "payload", "marker"}, PrimaryKeyEqualityProof: proof}}, probe)
	if err != nil || !report.Passed {
		t.Fatalf("cross-driver report=%#v err=%v", report, err)
	}
}

func TestSQLiteToMySQLDatabaseValidationProbeLiveTLS(t *testing.T) {
	dsn, ca := os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN"), os.Getenv("DMTX_TEST_MYSQL_CA")
	if dsn == "" || ca == "" || os.Getenv("MYSQL_REQUIRED") == "" {
		t.Skip("set MYSQL_REQUIRED, DMTX_TEST_MYSQL_TARGET_DSN, and DMTX_TEST_MYSQL_CA")
	}
	registerMySQLCommonFixtureTLSNamed(t, ca, "dmtx_test")
	parsed := parseMySQLNativeTargetDSNForTLS(t, "validation target", dsn, "dmtx_test")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	targetDB := openMySQLNativeLiveDatabaseForFlavor(t, ctx, "SQLite validation target", dsn, true)
	name := "dmtx_val_sqlite_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	t.Cleanup(func() { cleanupMySQLNativeTables(t, targetDB, name) })
	if _, err := targetDB.ExecContext(ctx, "CREATE TABLE `"+name+"` (id BIGINT PRIMARY KEY,payload TEXT NULL,marker BLOB NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := targetDB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES (1,'alpha',X'01'),(2,NULL,X'02')"); err != nil {
		t.Fatal(err)
	}
	sourceDB := openAdapterValidationSQLiteTestDatabase(t, filepath.Join(t.TempDir(), "source.db"))
	if _, err := sourceDB.Exec(`CREATE TABLE "` + name + `" (id INTEGER PRIMARY KEY,payload TEXT,marker BLOB); INSERT INTO "` + name + `" VALUES (1,'alpha',X'01'),(2,NULL,X'02')`); err != nil {
		t.Fatal(err)
	}
	tx, err := sourceDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	table := adapterValidationSQLiteTestTable(name)
	targetTable := table
	targetTable.Schema = parsed.DBName
	source := &sqliteSourceAdapter{database: sourceDB, snapshot: tx}
	target := &mysqlTargetAdapter{database: targetDB, namespace: parsed.DBName}
	contract, err := source.Stage4ValidationProbe(source, target, []adapterTablePlan{{source: table, target: targetTable, columns: []string{"id", "payload", "marker"}}})
	if err != nil {
		t.Fatal(err)
	}
	probe := contract.(*adapterDatabaseValidationProbe)
	proof, err := probe.Stage4ValidationPrimaryKeyEqualityProof(table)
	if err != nil {
		t.Fatal(err)
	}
	report, err := RunValidationCore(ctx, ValidationCoreOptions{Mode: config.ValidationSample, TargetMode: "upsert", FailOnMismatch: true, FailOnTimeout: true, ExactCountTimeout: 10 * time.Second, TableTimeout: 30 * time.Second, TableConcurrency: 1, SampleLimit: 2}, []ValidationTableSpec{{Table: table, Projection: []string{"id", "payload", "marker"}, PrimaryKeyEqualityProof: proof}}, probe)
	if err != nil || !report.Passed {
		t.Fatalf("report=%#v err=%v", report, err)
	}
}

// TestMySQLToSQLServerDatabaseValidationProbeLiveTLS exercises the remaining
// MySQL source and SQL Server target query paths through the production
// database-backed validation probe. It is deliberately gated because it
// creates isolated objects in both verified-TLS test containers.
func TestMySQLToSQLServerDatabaseValidationProbeLiveTLS(t *testing.T) {
	sourceDSN, sourceCA := os.Getenv("DMTX_TEST_MYSQL_DSN"), os.Getenv("DMTX_TEST_MYSQL_CA")
	targetDSN, targetCA := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN"), os.Getenv("DMTX_TEST_MSSQL_CA")
	if os.Getenv("MYSQL_REQUIRED") == "" || os.Getenv("MSSQL_REQUIRED") == "" ||
		sourceDSN == "" || sourceCA == "" || targetDSN == "" || targetCA == "" {
		t.Skip("set MYSQL_REQUIRED, MSSQL_REQUIRED, DMTX_TEST_MYSQL_DSN, DMTX_TEST_MYSQL_CA, DMTX_TEST_MSSQL_TARGET_DSN, and DMTX_TEST_MSSQL_CA")
	}
	registerMySQLCommonFixtureTLSNamed(t, sourceCA, "dmtx_test")
	sourceConfig := parseMySQLNativeTargetDSNForTLS(t, "validation source", sourceDSN, "dmtx_test")
	targetEndpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, targetCA)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sourceDB := openMySQLNativeLiveDatabaseForFlavor(t, ctx, "validation source", sourceDSN, false)
	var tlsVariable, tlsCipher string
	if err := sourceDB.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Ssl_cipher'").Scan(&tlsVariable, &tlsCipher); err != nil {
		t.Fatalf("inspect MySQL validation-source TLS status: %v", err)
	}
	if tlsVariable != "Ssl_cipher" || tlsCipher == "" {
		t.Fatal("MySQL validation source is not using TLS")
	}
	targetDB := openSQLServerNativeLiveDatabase(t, ctx, "validation target", targetEndpoint)

	name := "dmtx_val_mysql_mssql_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, sourceDB, name)
	cleanupSQLServerNativeTables(t, targetDB, name)
	if _, err := sourceDB.ExecContext(ctx, "CREATE TABLE "+mySQLIdentifier(name)+" (id BIGINT NOT NULL PRIMARY KEY,payload TEXT NULL,marker BLOB NULL)"); err != nil {
		t.Fatalf("create MySQL validation source table: %v", err)
	}
	if _, err := sourceDB.ExecContext(ctx, "INSERT INTO "+mySQLIdentifier(name)+" VALUES (1,'alpha',X'01'),(2,NULL,X'02')"); err != nil {
		t.Fatalf("load MySQL validation source table: %v", err)
	}
	targetQualified := sqlServerQualified("dbo", name)
	if _, err := targetDB.ExecContext(ctx, "CREATE TABLE "+targetQualified+" ([id] BIGINT NOT NULL PRIMARY KEY,[payload] NVARCHAR(40) NULL,[marker] VARBINARY(8) NULL); INSERT INTO "+targetQualified+" VALUES (1,N'alpha',0x01),(2,NULL,0x02)"); err != nil {
		t.Fatalf("create/load SQL Server validation target table: %v", err)
	}

	sourceTable := schema.Table{
		Schema: sourceConfig.DBName,
		Name:   name,
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", DeclaredType: &schema.DeclaredType{Base: "bigint"}, PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "payload", Type: "text", DeclaredType: &schema.DeclaredType{Base: "text"}, Nullable: true},
			{Name: "marker", Type: "blob", DeclaredType: &schema.DeclaredType{Base: "blob"}, Nullable: true},
		},
	}
	targetTable := sourceTable
	targetTable.Schema = "dbo"
	targetTable.Columns = []schema.Column{
		{Name: "id", Type: "bigint", DeclaredType: &schema.DeclaredType{Base: "bigint"}, PrimaryKey: true, PrimaryKeyPosition: 1},
		{Name: "payload", Type: "nvarchar", DeclaredType: &schema.DeclaredType{Base: "nvarchar"}, Nullable: true},
		{Name: "marker", Type: "varbinary", DeclaredType: &schema.DeclaredType{Base: "varbinary"}, Nullable: true},
	}
	source := &relationalSourceAdapter{
		spec:      relationalSourceSpec{engine: adapterValidationMySQL},
		database:  sourceDB,
		namespace: sourceConfig.DBName,
	}
	target := &sqlServerTargetAdapter{database: targetDB, namespace: "dbo"}
	contract, err := source.Stage4ValidationProbe(source, target, []adapterTablePlan{{
		source:  sourceTable,
		target:  targetTable,
		columns: []string{"id", "payload", "marker"},
	}})
	if err != nil {
		t.Fatalf("construct MySQL-to-SQL Server validation probe: %v", err)
	}
	probe := contract.(*adapterDatabaseValidationProbe)
	proof, err := probe.Stage4ValidationPrimaryKeyEqualityProof(sourceTable)
	if err != nil {
		t.Fatalf("certify mixed-engine integer key: %v", err)
	}
	if proof == "" {
		t.Fatal("mixed-engine integer key proof is empty")
	}
	if count, err := probe.ExactCount(ctx, ValidationSource, sourceTable); err != nil || count != 2 {
		t.Fatalf("MySQL source exact count=%d err=%v", count, err)
	}
	if count, err := probe.ExactCount(ctx, ValidationTarget, sourceTable); err != nil || count != 2 {
		t.Fatalf("SQL Server target exact count=%d err=%v", count, err)
	}
	sourceNulls, err := probe.NullCounts(ctx, ValidationSource, sourceTable, []string{"id", "payload", "marker"}, ValidationNullScope{Kind: ValidationNullScopeTransferredSource})
	if err != nil || sourceNulls.Rows != 2 || sourceNulls.Counts["payload"] != 1 || sourceNulls.Counts["marker"] != 0 {
		t.Fatalf("MySQL source NULL parity=%#v err=%v", sourceNulls, err)
	}
	targetNulls, err := probe.NullCounts(ctx, ValidationTarget, sourceTable, []string{"id", "payload", "marker"}, ValidationNullScope{Kind: ValidationNullScopeTargetSourcePrimaryKeys, PrimaryKeyColumns: []string{"id"}, EqualityProofDigest: proof})
	if err != nil || targetNulls.Rows != 2 || targetNulls.Counts["payload"] != 1 || targetNulls.Counts["marker"] != 0 {
		t.Fatalf("SQL Server target NULL parity=%#v err=%v", targetNulls, err)
	}
	sourceRows, err := probe.SampleSourceRows(ctx, sourceTable, []string{"id", "payload", "marker"}, 2)
	if err != nil || len(sourceRows) != 2 {
		t.Fatalf("ordered MySQL source sample=%#v err=%v", sourceRows, err)
	}
	for index, want := range []int64{1, 2} {
		got, ok := sourceRows[index].Values[0].(int64)
		if !ok || got != want {
			t.Fatalf("ordered MySQL source sample key %d = %#v, want %d", index, sourceRows[index].Values[0], want)
		}
	}
	targetRows, err := probe.SampleTargetRows(ctx, sourceTable, []string{"id", "payload", "marker"}, []ValidationPrimaryKey{{Values: []any{int64(1)}}, {Values: []any{int64(2)}}})
	if err != nil || len(targetRows) != 2 {
		t.Fatalf("keyed SQL Server target sample=%#v err=%v", targetRows, err)
	}
	targetKeys := make(map[int64]bool, len(targetRows))
	for _, row := range targetRows {
		key, ok := row.Values[0].(int64)
		if !ok {
			t.Fatalf("keyed SQL Server target sample key = %#v, want bigint", row.Values[0])
		}
		targetKeys[key] = true
	}
	if !targetKeys[1] || !targetKeys[2] {
		t.Fatalf("keyed SQL Server target sample keys = %#v, want 1 and 2", targetKeys)
	}
	report, err := RunValidationCore(ctx, ValidationCoreOptions{
		Mode: config.ValidationSample, TargetMode: "upsert", FailOnMismatch: true, FailOnTimeout: true,
		ExactCountTimeout: 10 * time.Second, TableTimeout: 30 * time.Second, TableConcurrency: 1, SampleLimit: 2,
	}, []ValidationTableSpec{{
		Table: sourceTable, Projection: []string{"id", "payload", "marker"}, PrimaryKeyEqualityProof: proof,
	}}, probe)
	if err != nil || !report.Passed {
		t.Fatalf("MySQL-to-SQL Server validation report=%#v err=%v", report, err)
	}
}
