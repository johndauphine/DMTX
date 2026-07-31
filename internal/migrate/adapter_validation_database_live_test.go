package migrate

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
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
