package migrate

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// TestStage4ValidationRouteMatrixLive is the armed real-driver deep-validation
// matrix for every certified relational/SQLite source and target family.  Each
// route uses the production adapter probe construction followed by the Stage 4
// validation runner in inclusive sample mode.  The fixture includes typed
// integer keys, NULL text, binary bytes, and timestamps so the query paths and
// canonical row representation are exercised rather than only the SQL-shape
// helpers.
func TestStage4ValidationRouteMatrixLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	environment := newStage4IncrementalLiveMatrixEnvironment(t, ctx)
	engines := []string{
		adapterValidationPostgres,
		adapterValidationSQLServer,
		adapterValidationMySQL,
		adapterValidationSQLite,
	}
	for _, sourceEngine := range engines {
		for _, targetEngine := range engines {
			sourceEngine, targetEngine := sourceEngine, targetEngine
			t.Run(sourceEngine+"-to-"+targetEngine, func(t *testing.T) {
				fixture := newStage4ValidationLiveRoute(
					t,
					ctx,
					environment,
					sourceEngine,
					targetEngine,
				)
				fixture.validateSampleComposition(t, ctx)
			})
		}
	}
}

// TestStage4ValidationMariaDBFlavorLive verifies that the canonical mysql
// validation route is also exercised against the distinct MariaDB 10.11
// driver/catalog flavor.  MariaDB is a configuration alias, not an additional
// capability cell, so this deliberately does not promote an unsupported route.
func TestStage4ValidationMariaDBFlavorLive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	environment := newStage4IncrementalMariaDBFamilyEnvironment(t, ctx)
	fixture := newStage4ValidationLiveRoute(
		t,
		ctx,
		environment,
		adapterValidationMySQL,
		adapterValidationMySQL,
	)
	fixture.validateSampleComposition(t, ctx)
}

// TestStage4ValidationRouteMatrixRefusesUnprovenCrossEngineTextKeyBeforeMutation
// pins the pre-mutation admission boundary.  Cross-engine text equality is not
// certified without a portable collation proof, so the route must be refused
// while producing the route-bound proof, before any validation query or target
// schema/data mutation can happen.
func TestStage4ValidationRouteMatrixRefusesUnprovenCrossEngineTextKeyBeforeMutation(
	t *testing.T,
) {
	sourceDatabase := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "source.db"),
	)
	targetDatabase := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "target.db"),
	)
	var before int
	if err := targetDatabase.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'view', 'index', 'trigger')",
	).Scan(&before); err != nil {
		t.Fatal(err)
	}

	sourceTable := schema.Table{
		Schema: "source_scope",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name: "id", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
				PrimaryKey:   true, PrimaryKeyPosition: 1,
			},
			{
				Name: "payload", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
				Nullable:     true,
			},
		},
	}
	targetTable := cloneStage4RichTable(sourceTable)
	targetTable.Schema = "target_scope"
	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine: adapterValidationMySQL,
		},
		database:  sourceDatabase,
		namespace: "source_scope",
	}
	target := &postgresTargetAdapter{
		database:  targetDatabase,
		namespace: "target_scope",
	}
	cfg := stage4ValidationLiveConfig()
	probe, err := stage4AdapterValidationProbe(
		cfg,
		nil,
		source,
		target,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id", "payload"},
		}},
	)
	if err != nil {
		t.Fatalf("construct unproven-key validation probe: %v", err)
	}
	_, err = prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
		cfg.Migration.Validation.Mode,
		"upsert",
		probe,
		[]schema.Table{sourceTable},
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
		!strings.Contains(err.Error(), "text/collation") {
		t.Fatalf("unproven cross-engine text-key admission error = %v", err)
	}
	_, err = stage4AdapterValidationProbe(
		cfg,
		nil,
		source,
		&clickHouseTargetAdapter{
			database:  targetDatabase,
			namespace: "target_scope",
		},
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id", "payload"},
		}},
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
		!strings.Contains(err.Error(), "target engine \"clickhouse\" is not certified") {
		t.Fatalf("unproven ClickHouse validation target admission error = %v", err)
	}
	var after int
	if err := targetDatabase.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'view', 'index', 'trigger')",
	).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf(
			"unproven cross-engine validation mutated target catalog: before=%d after=%d",
			before,
			after,
		)
	}
}

// TestStage4ValidationRouteMatrixTimeoutPolicyLiveTLS uses a real PostgreSQL
// ACCESS EXCLUSIVE lock on an isolated source table.  The production exact
// count query crosses an actual driver/network cancellation boundary, while
// the catalog estimate remains available.  It pins default failure and the
// explicit log-only timeout policy without manufacturing a probe error.
func TestStage4ValidationRouteMatrixTimeoutPolicyLiveTLS(t *testing.T) {
	if os.Getenv("DMTX_STAGE4_LIVE_REQUIRED") != "1" {
		t.Skip("set DMTX_STAGE4_LIVE_REQUIRED=1 and DMTX_TEST_POSTGRES_DSN to run the validation timeout policy route")
	}
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN to run the validation timeout policy route")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL validation timeout DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require verified TLS")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	database := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"pgx",
		dsn,
		"PostgreSQL validation timeout route",
	)
	locker := stage4IncrementalLiveOpenDatabase(
		t,
		ctx,
		"pgx",
		dsn,
		"PostgreSQL validation timeout locker",
	)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceNamespace := "dmtx_val_timeout_src_" + suffix
	targetNamespace := "dmtx_val_timeout_tgt_" + suffix
	for _, namespace := range []string{sourceNamespace, targetNamespace} {
		if _, err := database.ExecContext(
			ctx,
			"CREATE SCHEMA "+postgresIdentifier(namespace),
		); err != nil {
			t.Fatalf("create validation-timeout schema: %v", err)
		}
	}
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		for _, namespace := range []string{sourceNamespace, targetNamespace} {
			if _, err := database.ExecContext(
				cleanup,
				"DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE",
			); err != nil {
				t.Errorf("drop validation-timeout schema: %v", err)
			}
		}
	})
	sourceQualified := postgresQualified(sourceNamespace, "items")
	targetQualified := postgresQualified(targetNamespace, "items")
	for _, qualified := range []string{sourceQualified, targetQualified} {
		if _, err := database.ExecContext(ctx, `
			CREATE TABLE `+qualified+` (
				id bigint PRIMARY KEY,
				payload text NULL
			)`); err != nil {
			t.Fatalf("create validation-timeout table: %v", err)
		}
		if _, err := database.ExecContext(ctx, `
			INSERT INTO `+qualified+` (id, payload)
			VALUES (1, 'one'), (2, NULL);
			ANALYZE `+qualified); err != nil {
			t.Fatalf("seed validation-timeout table: %v", err)
		}
	}

	sourceTable := schema.Table{
		Schema: sourceNamespace,
		Name:   "items",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
				PrimaryKey:   true, PrimaryKeyPosition: 1,
			},
			{
				Name: "payload", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
				Nullable:     true,
			},
		},
	}
	targetTable := cloneStage4RichTable(sourceTable)
	targetTable.Schema = targetNamespace
	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine:         adapterValidationPostgres,
			displayName:    "PostgreSQL",
			qualifiedTable: postgresQualified,
		},
		database:  database,
		namespace: sourceNamespace,
	}
	target := &postgresTargetAdapter{
		database:  database,
		namespace: targetNamespace,
	}
	cfg := stage4ValidationLiveConfig()
	cfg.Migration.Validation.Mode = config.ValidationNullParity
	plan := adapterTablePlan{
		source:  sourceTable,
		target:  targetTable,
		columns: []string{"id", "payload"},
	}
	probe, err := stage4AdapterValidationProbe(
		cfg,
		nil,
		source,
		target,
		[]adapterTablePlan{plan},
	)
	if err != nil {
		t.Fatalf("construct validation-timeout route probe: %v", err)
	}
	proofs, err := prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
		cfg.Migration.Validation.Mode,
		"upsert",
		probe,
		[]schema.Table{sourceTable},
	)
	if err != nil {
		t.Fatalf("prepare validation-timeout equality proof: %v", err)
	}
	prepared := stage4AdapterPrepared{
		mode:                               "upsert",
		plans:                              []adapterTablePlan{plan},
		gate:                               Stage4SchemaGateResult{ValidationTables: []schema.Table{sourceTable}},
		validation:                         probe,
		validationPrimaryKeyEqualityProofs: proofs,
	}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		t.Fatalf("construct validation-timeout table specs: %v", err)
	}

	lockCtx, lockCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	// Register cancellation first so the LIFO cleanups roll the transaction
	// back while its setup context is still live.
	t.Cleanup(lockCancel)
	lock, err := locker.BeginTx(lockCtx, nil)
	if err != nil {
		t.Fatalf("begin validation-timeout lock transaction: %v", err)
	}
	t.Cleanup(func() {
		if err := lock.Rollback(); err != nil {
			t.Errorf("release validation-timeout lock: %v", err)
		}
	})
	if _, err := lock.ExecContext(
		lockCtx,
		"LOCK TABLE "+sourceQualified+" IN ACCESS EXCLUSIVE MODE",
	); err != nil {
		t.Fatalf("lock validation-timeout source table: %v", err)
	}

	for _, test := range []struct {
		name          string
		failOnTimeout bool
		wantPassed    bool
		wantSeverity  ValidationSeverity
	}{
		{
			name:          "default-fail",
			failOnTimeout: true,
			wantPassed:    false,
			wantSeverity:  ValidationSeverityError,
		},
		{
			name:          "explicit-log-only",
			failOnTimeout: false,
			wantPassed:    true,
			wantSeverity:  ValidationSeverityWarning,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			report, err := RunValidationCore(
				ctx,
				ValidationCoreOptions{
					Mode:                   config.ValidationNullParity,
					TargetMode:             "upsert",
					FailOnMismatch:         true,
					FailOnTimeout:          test.failOnTimeout,
					FailOnEstimateMismatch: true,
					ExactCountTimeout:      30 * time.Millisecond,
					TableTimeout:           250 * time.Millisecond,
					TableConcurrency:       1,
					SampleLimit:            2,
				},
				specs,
				probe,
			)
			if err != nil {
				t.Fatalf("run validation-timeout policy: %v", err)
			}
			if report.Passed != test.wantPassed {
				t.Fatalf(
					"validation-timeout passed=%t, want %t: %#v",
					report.Passed,
					test.wantPassed,
					report.Findings,
				)
			}
			finding := stage4ValidationLiveFinding(
				t,
				report,
				"validation.count.exact",
				ValidationSource,
			)
			if finding.Outcome != ValidationOutcomeTimeout ||
				finding.Severity != test.wantSeverity {
				t.Fatalf("exact timeout finding = %#v", finding)
			}
		})
	}
}

func newStage4ValidationLiveRoute(
	t *testing.T,
	ctx context.Context,
	environment *stage4IncrementalLiveMatrixEnvironment,
	sourceEngine string,
	targetEngine string,
) *stage4IncrementalLiveRouteFixture {
	t.Helper()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	fixture := &stage4IncrementalLiveRouteFixture{
		sourceEngine:        sourceEngine,
		targetEngine:        targetEngine,
		mySQLTableCollation: environment.mySQLTableCollation,
		sourceTable: schema.Table{
			Name: "dmtx_validation_" + sourceEngine + "_" + targetEngine + "_" + suffix,
		},
	}
	environment.configureSource(t, ctx, fixture, suffix)
	environment.configureTarget(t, ctx, fixture, suffix)
	if err := fixture.createSourceTable(ctx); err != nil {
		t.Fatalf("create validation matrix source table: %v", err)
	}
	if err := stage4ValidationLiveAddBinaryColumn(ctx, fixture); err != nil {
		t.Fatalf("add validation matrix binary column: %v", err)
	}
	if err := fixture.precreateTarget(ctx); err != nil {
		t.Fatalf("precreate validation matrix target: %v", err)
	}
	if err := stage4ValidationLiveSeedTarget(ctx, fixture); err != nil {
		t.Fatalf("seed validation matrix target: %v", err)
	}
	return fixture
}

func stage4ValidationLiveAddBinaryColumn(
	ctx context.Context,
	fixture *stage4IncrementalLiveRouteFixture,
) error {
	var statements []string
	switch fixture.sourceEngine {
	case adapterValidationPostgres:
		statements = []string{
			"ALTER TABLE " + fixture.sourceQualified() + " ADD COLUMN marker BYTEA",
			"UPDATE " + fixture.sourceQualified() + " SET marker = CASE id WHEN 1 THEN decode('00ff', 'hex') WHEN 2 THEN decode('0102', 'hex') END",
			"ALTER TABLE " + fixture.sourceQualified() + " ALTER COLUMN marker SET NOT NULL",
		}
	case adapterValidationMySQL:
		statements = []string{
			"ALTER TABLE " + fixture.sourceQualified() + " ADD COLUMN marker BLOB NULL",
			"UPDATE " + fixture.sourceQualified() + " SET marker = CASE id WHEN 1 THEN X'00FF' WHEN 2 THEN X'0102' END",
			"ALTER TABLE " + fixture.sourceQualified() + " MODIFY COLUMN marker BLOB NOT NULL",
		}
	case adapterValidationSQLServer:
		statements = []string{
			"ALTER TABLE " + fixture.sourceQualified() + " ADD [marker] VARBINARY(16) NULL",
			"UPDATE " + fixture.sourceQualified() + " SET [marker] = CASE [id] WHEN 1 THEN 0x00FF WHEN 2 THEN 0x0102 END",
			"ALTER TABLE " + fixture.sourceQualified() + " ALTER COLUMN [marker] VARBINARY(16) NOT NULL",
		}
	case adapterValidationSQLite:
		statements = []string{
			"ALTER TABLE " + fixture.sourceQualified() + " ADD COLUMN \"marker\" BLOB NOT NULL DEFAULT X'00'",
			"UPDATE " + fixture.sourceQualified() + " SET \"marker\" = CASE \"id\" WHEN 1 THEN X'00FF' WHEN 2 THEN X'0102' ELSE X'00' END",
		}
	default:
		return fmt.Errorf("unknown validation source engine %q", fixture.sourceEngine)
	}
	for _, statement := range statements {
		if _, err := fixture.sourceDatabase.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func stage4ValidationLiveSeedTarget(
	ctx context.Context,
	fixture *stage4IncrementalLiveRouteFixture,
) error {
	endpoint := adapterValidationSQLEndpoint{engine: fixture.targetEngine}
	columns := []string{"id", "payload", "note", "updated_at", "marker"}
	quotedColumns := adapterValidationQuotedColumns(endpoint, columns)
	placeholders := make([]string, len(columns))
	for index := range placeholders {
		placeholders[index] = endpoint.placeholder(index + 1)
	}
	statement := "INSERT INTO " + fixture.targetQualified() + " (" +
		quotedColumns + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	timestamps := []time.Time{
		time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC),
	}
	rows := [][]any{
		{
			int64(1), "baseline-one", nil,
			stage4ValidationLiveTimestampArgument(fixture.targetEngine, timestamps[0]),
			[]byte{0x00, 0xff},
		},
		{
			int64(2), "baseline-two", "baseline-note",
			stage4ValidationLiveTimestampArgument(fixture.targetEngine, timestamps[1]),
			[]byte{0x01, 0x02},
		},
	}
	for _, row := range rows {
		if _, err := fixture.targetDatabase.ExecContext(ctx, statement, row...); err != nil {
			return err
		}
	}
	return nil
}

func stage4ValidationLiveTimestampArgument(
	engine string,
	value time.Time,
) any {
	if engine == adapterValidationSQLite {
		return value.UTC().Format("2006-01-02 15:04:05.000")
	}
	return value.UTC()
}

func (fixture *stage4IncrementalLiveRouteFixture) validateSampleComposition(
	t *testing.T,
	ctx context.Context,
) {
	t.Helper()
	source, err := builtInAdapters.sources[fixture.sourceEngine].open(
		ctx,
		fixture.sourceEndpoint,
	)
	if err != nil {
		t.Fatalf("open validation matrix source adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close validation matrix source adapter: %v", err)
		}
	})
	target, err := builtInAdapters.targets[fixture.targetEngine].open(
		ctx,
		fixture.targetEndpoint,
	)
	if err != nil {
		t.Fatalf("open validation matrix target adapter: %v", err)
	}
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Errorf("close validation matrix target adapter: %v", err)
		}
	})
	sourceTable, err := source.InspectTable(ctx, fixture.sourceTable.Name)
	if err != nil {
		t.Fatalf("inspect validation matrix source table: %v", err)
	}
	if fixture.sourceEngine == adapterValidationMySQL &&
		sourceTable.MySQLCollation != fixture.mySQLTableCollation {
		t.Fatalf(
			"validation matrix MySQL source collation = %q, want %q",
			sourceTable.MySQLCollation,
			fixture.mySQLTableCollation,
		)
	}
	targetTables, err := target.PlanTables(
		fixture.sourceEngine,
		[]schema.Table{sourceTable},
		"upsert",
	)
	if err != nil {
		t.Fatalf("plan validation matrix target table: %v", err)
	}
	if len(targetTables) != 1 {
		t.Fatalf("validation matrix target plan count = %d, want 1", len(targetTables))
	}
	projection := adapterColumnNames(sourceTable)
	plan := adapterTablePlan{
		source:  sourceTable,
		target:  targetTables[0],
		columns: projection,
	}
	cfg := stage4ValidationLiveConfig()
	probe, err := stage4AdapterValidationProbe(
		cfg,
		nil,
		source,
		target,
		[]adapterTablePlan{plan},
	)
	if err != nil {
		t.Fatalf("construct validation matrix composed probe: %v", err)
	}
	databaseProbe, ok := probe.(*adapterDatabaseValidationProbe)
	if !ok {
		t.Fatalf("validation matrix probe type = %T, want database probe", probe)
	}
	proofs, err := prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
		cfg.Migration.Validation.Mode,
		"upsert",
		probe,
		[]schema.Table{sourceTable},
	)
	if err != nil {
		t.Fatalf("prepare validation matrix equality proof: %v", err)
	}
	key := stage4RichTableKey{schema: sourceTable.Schema, table: sourceTable.Name}
	proof := proofs[key]
	if !validValidationEqualityProofDigest(proof) {
		t.Fatalf("validation matrix primary-key proof = %q", proof)
	}
	if count, err := databaseProbe.ExactCount(
		ctx,
		ValidationSource,
		sourceTable,
	); err != nil || count != 2 {
		t.Fatalf("validation matrix source count=%d err=%v", count, err)
	}
	if count, err := databaseProbe.ExactCount(
		ctx,
		ValidationTarget,
		sourceTable,
	); err != nil || count != 2 {
		t.Fatalf("validation matrix target count=%d err=%v", count, err)
	}
	sourceNulls, err := databaseProbe.NullCounts(
		ctx,
		ValidationSource,
		sourceTable,
		projection,
		ValidationNullScope{Kind: ValidationNullScopeTransferredSource},
	)
	if err != nil || sourceNulls.Rows != 2 ||
		sourceNulls.Counts["note"] != 1 ||
		sourceNulls.Counts["marker"] != 0 {
		t.Fatalf("validation matrix source NULL evidence=%#v err=%v", sourceNulls, err)
	}
	primaryKey, err := adapterValidationPrimaryKey(sourceTable)
	if err != nil {
		t.Fatalf("read validation matrix primary key: %v", err)
	}
	targetScope := ValidationNullScope{
		Kind:                ValidationNullScopeTargetSourcePrimaryKeys,
		PrimaryKeyColumns:   adapterValidationColumnNames(primaryKey),
		EqualityProofDigest: proof,
	}
	targetNulls, err := databaseProbe.NullCounts(
		ctx,
		ValidationTarget,
		sourceTable,
		projection,
		targetScope,
	)
	if err != nil || targetNulls.Rows != 2 ||
		targetNulls.Counts["note"] != 1 ||
		targetNulls.Counts["marker"] != 0 ||
		!sameValidationNullScope(targetNulls.Scope, targetScope) {
		t.Fatalf("validation matrix target NULL evidence=%#v err=%v", targetNulls, err)
	}
	sourceRows, err := databaseProbe.SampleSourceRows(
		ctx,
		sourceTable,
		projection,
		2,
	)
	if err != nil || len(sourceRows) != 2 {
		t.Fatalf("validation matrix source samples=%#v err=%v", sourceRows, err)
	}
	if err := stage4ValidationLiveAssertCanonicalSample(
		ctx,
		databaseProbe,
		sourceTable,
		projection,
		sourceRows,
	); err != nil {
		t.Fatalf("validation matrix canonical sample: %v", err)
	}
	prepared := stage4AdapterPrepared{
		mode:                               "upsert",
		plans:                              []adapterTablePlan{plan},
		gate:                               Stage4SchemaGateResult{ValidationTables: []schema.Table{sourceTable}},
		validation:                         probe,
		validationPrimaryKeyEqualityProofs: proofs,
	}
	if err := validateStage4AdapterRun(ctx, cfg, source, target, prepared); err != nil {
		t.Fatalf("run validation matrix Stage 4 composition: %v", err)
	}
}

func stage4ValidationLiveAssertCanonicalSample(
	ctx context.Context,
	probe *adapterDatabaseValidationProbe,
	table schema.Table,
	projection []string,
	sourceRows []ValidationSampleRow,
) error {
	descriptor, err := newValidationSampleDescriptor(table, projection)
	if err != nil {
		return err
	}
	primaryKeyIndexes, primaryKeyDescriptor, err :=
		validationPrimaryKeyDescriptor(table, descriptor)
	if err != nil {
		return err
	}
	canonicalSource, err := canonicalizeValidationSamples(
		descriptor,
		primaryKeyDescriptor,
		primaryKeyIndexes,
		sourceRows,
	)
	if err != nil {
		return err
	}
	if err := validateIncreasingValidationPrimaryKeys(
		primaryKeyDescriptor,
		canonicalSource,
	); err != nil {
		return err
	}
	keys := make([]ValidationPrimaryKey, len(canonicalSource))
	for index, row := range canonicalSource {
		keys[index] = ValidationPrimaryKey{
			Values: append([]any(nil), row.keyValues...),
		}
	}
	targetRows, err := probe.SampleTargetRows(
		ctx,
		table,
		projection,
		keys,
	)
	if err != nil {
		return err
	}
	if len(targetRows) != len(sourceRows) {
		return fmt.Errorf(
			"target returned %d sampled rows, want %d",
			len(targetRows),
			len(sourceRows),
		)
	}
	canonicalTarget, err := canonicalizeValidationSamples(
		descriptor,
		primaryKeyDescriptor,
		primaryKeyIndexes,
		targetRows,
	)
	if err != nil {
		return err
	}
	targetByKey := make(map[string][]byte, len(canonicalTarget))
	for _, row := range canonicalTarget {
		targetByKey[string(row.key)] = row.row
	}
	for _, row := range canonicalSource {
		target, found := targetByKey[string(row.key)]
		if !found {
			return fmt.Errorf("target omitted sampled complete primary key")
		}
		if !bytes.Equal(row.row, target) {
			return fmt.Errorf("target sampled row differs after typed canonicalization")
		}
	}
	return nil
}

func stage4ValidationLiveConfig() config.Config {
	return config.Config{
		Migration: config.Migration{
			TargetMode: "upsert",
			Validation: config.ValidationPolicy{
				Mode:                   config.ValidationSample,
				FailOnMismatch:         true,
				FailOnTimeout:          true,
				FailOnEstimateMismatch: true,
			},
		},
	}
}

func stage4ValidationLiveFinding(
	t *testing.T,
	report CoreValidationReport,
	check string,
	side ValidationSide,
) CoreValidationFinding {
	t.Helper()
	for _, finding := range report.Findings {
		if finding.Check == check && finding.Side == side {
			return finding
		}
	}
	t.Fatalf("missing validation finding %s/%s: %#v", check, side, report.Findings)
	return CoreValidationFinding{}
}
