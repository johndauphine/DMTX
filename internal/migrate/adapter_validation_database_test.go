package migrate

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	_ "modernc.org/sqlite"
)

func TestSQLiteDatabaseValidationProbeDeepSemantics(t *testing.T) {
	ctx := context.Background()
	sourceDatabase := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "source.db"),
	)
	targetDatabase := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "target.db"),
	)
	for _, fixture := range []struct {
		database *sql.DB
		rows     string
	}{
		{
			database: sourceDatabase,
			rows: `(1, 'alpha', X'01'),
			       (2, NULL,    X'02'),
			       (3, 'gamma', X'03')`,
		},
		{
			database: targetDatabase,
			rows: `(1,  'alpha', X'01'),
			       (2,  NULL,    X'02'),
			       (3,  'gamma', X'03'),
			       (99, NULL,    X'63')`,
		},
	} {
		if _, err := fixture.database.ExecContext(ctx, `
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				payload TEXT,
				marker BLOB NOT NULL
			);
			INSERT INTO items (id, payload, marker) VALUES `+
			fixture.rows); err != nil {
			t.Fatalf("create SQLite validation fixture: %v", err)
		}
	}
	sourceSnapshot, err := sourceDatabase.BeginTx(
		ctx,
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sourceSnapshot.Rollback(); err != nil &&
			!errors.Is(err, sql.ErrTxDone) {
			t.Errorf("rollback SQLite validation snapshot: %v", err)
		}
	})
	var pinned int
	if err := sourceSnapshot.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM items`,
	).Scan(&pinned); err != nil || pinned != 3 {
		t.Fatalf("pin SQLite validation snapshot = %d, %v", pinned, err)
	}

	table := adapterValidationSQLiteTestTable("items")
	plan := adapterTablePlan{
		source: table,
		target: table,
		columns: []string{
			"id",
			"payload",
			"marker",
		},
	}
	source := &sqliteSourceAdapter{
		database: sourceDatabase,
		snapshot: sourceSnapshot,
	}
	target := &sqliteTargetAdapter{database: targetDatabase}
	contract, err := source.Stage4ValidationProbe(
		source,
		target,
		[]adapterTablePlan{plan},
	)
	if err != nil {
		t.Fatalf("construct SQLite validation probe: %v", err)
	}
	probe, ok := contract.(*adapterDatabaseValidationProbe)
	if !ok {
		t.Fatalf("SQLite validation probe type = %T", contract)
	}
	proof, err := probe.Stage4ValidationPrimaryKeyEqualityProof(
		table,
	)
	if err != nil {
		t.Fatalf("certify SQLite validation key: %v", err)
	}
	if !validValidationEqualityProofDigest(proof) {
		t.Fatalf("SQLite validation proof = %q", proof)
	}

	sourceCount, err := probe.ExactCount(
		ctx,
		ValidationSource,
		table,
	)
	if err != nil || sourceCount != 3 {
		t.Fatalf("SQLite source count = %d, %v", sourceCount, err)
	}
	targetCount, err := probe.ExactCount(
		ctx,
		ValidationTarget,
		table,
	)
	if err != nil || targetCount != 4 {
		t.Fatalf("SQLite target count = %d, %v", targetCount, err)
	}
	if _, err := probe.EstimateCount(
		ctx,
		ValidationSource,
		table,
	); err == nil ||
		err.Error() !=
			"source validation estimates are unavailable for engine sqlite" {
		t.Fatalf("SQLite estimate error = %v", err)
	}

	projection := append([]string(nil), plan.columns...)
	sourceNulls, err := probe.NullCounts(
		ctx,
		ValidationSource,
		table,
		projection,
		ValidationNullScope{
			Kind: ValidationNullScopeTransferredSource,
		},
	)
	if err != nil {
		t.Fatalf("SQLite source NULL counts: %v", err)
	}
	if sourceNulls.Rows != 3 ||
		sourceNulls.Counts["payload"] != 1 ||
		sourceNulls.Counts["id"] != 0 ||
		sourceNulls.Counts["marker"] != 0 {
		t.Fatalf("SQLite source NULL evidence = %#v", sourceNulls)
	}
	targetScope := ValidationNullScope{
		Kind: ValidationNullScopeTargetSourcePrimaryKeys,
		PrimaryKeyColumns: []string{
			"id",
		},
		EqualityProofDigest: proof,
	}
	targetNulls, err := probe.NullCounts(
		ctx,
		ValidationTarget,
		table,
		projection,
		targetScope,
	)
	if err != nil {
		t.Fatalf("SQLite scoped target NULL counts: %v", err)
	}
	if targetNulls.Rows != 3 ||
		targetNulls.Counts["payload"] != 1 ||
		!sameValidationNullScope(
			targetNulls.Scope,
			targetScope,
		) {
		t.Fatalf("SQLite target NULL evidence = %#v", targetNulls)
	}
	duplicatePredicate, duplicateArguments, err :=
		adapterValidationKeyPredicate(
			probe.target,
			[]schema.Column{table.Columns[0]},
			[]ValidationPrimaryKey{
				{Values: []any{int64(2)}},
				{Values: []any{int64(2)}},
			},
		)
	if err != nil {
		t.Fatalf("build duplicate-key subset predicate: %v", err)
	}
	duplicateEvidence, err := queryAdapterValidationNullCounts(
		ctx,
		probe.target,
		plan.target,
		projection,
		targetScope,
		duplicatePredicate,
		duplicateArguments,
	)
	if err != nil {
		t.Fatalf("query duplicate-key target subset: %v", err)
	}
	if duplicateEvidence.Rows != 1 ||
		duplicateEvidence.Counts["payload"] != 1 {
		t.Fatalf(
			"duplicate-key target subset counted a row more than once: %#v",
			duplicateEvidence,
		)
	}

	samples, err := probe.SampleSourceRows(
		ctx,
		table,
		projection,
		2,
	)
	if err != nil {
		t.Fatalf("SQLite source sample: %v", err)
	}
	if len(samples) != 2 ||
		samples[0].Values[0] != int64(1) ||
		samples[1].Values[0] != int64(2) {
		t.Fatalf("SQLite source samples = %#v", samples)
	}
	fetched, err := probe.SampleTargetRows(
		ctx,
		table,
		projection,
		[]ValidationPrimaryKey{
			{Values: []any{int64(3)}},
			{Values: []any{int64(1)}},
		},
	)
	if err != nil {
		t.Fatalf("SQLite target sample: %v", err)
	}
	if len(fetched) != 2 {
		t.Fatalf("SQLite target samples = %#v", fetched)
	}
	if _, err := probe.SampleTargetRows(
		ctx,
		table,
		projection,
		[]ValidationPrimaryKey{
			{Values: []any{int64(1)}},
			{Values: []any{int(1)}},
		},
	); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("duplicate requested target key error = %v", err)
	}

	report, err := RunValidationCore(
		ctx,
		ValidationCoreOptions{
			Mode:              config.ValidationSample,
			TargetMode:        "upsert",
			FailOnMismatch:    true,
			FailOnTimeout:     true,
			ExactCountTimeout: time.Second,
			TableTimeout:      5 * time.Second,
			TableConcurrency:  1,
			SampleLimit:       3,
		},
		[]ValidationTableSpec{{
			Table: table, Projection: projection,
			PrimaryKeyEqualityProof: proof,
		}},
		probe,
	)
	if err != nil {
		t.Fatalf("run SQLite deep validation: %v", err)
	}
	if !report.Passed {
		t.Fatalf("SQLite deep validation report = %#v", report)
	}
}

func TestDatabaseValidationProbeFailsClosed(t *testing.T) {
	t.Run("typed nil providers", func(t *testing.T) {
		var source *sqliteSourceAdapter
		if _, err := source.Stage4ValidationProbe(
			source,
			&sqliteTargetAdapter{},
			nil,
		); err == nil ||
			!strings.Contains(err.Error(), "open SQLite source") {
			t.Fatalf("typed-nil source error = %v", err)
		}
		var target *sqliteTargetAdapter
		if _, err := adapterValidationTargetEndpoint(
			target,
		); err == nil ||
			!strings.Contains(err.Error(), "open SQLite target") {
			t.Fatalf("typed-nil target error = %v", err)
		}
		var queryer *sql.DB
		if err := validateAdapterValidationEndpoint(
			adapterValidationSQLEndpoint{
				engine:         adapterValidationSQLite,
				queryer:        queryer,
				parameterLimit: 1,
			},
		); err == nil ||
			!strings.Contains(err.Error(), "unavailable") {
			t.Fatalf("typed-nil queryer error = %v", err)
		}
	})

	t.Run("same live relation", func(t *testing.T) {
		database := openAdapterValidationSQLiteTestDatabase(
			t,
			filepath.Join(t.TempDir(), "same.db"),
		)
		table := adapterValidationSQLiteTestTable("items")
		_, err := newAdapterDatabaseValidationProbe(
			adapterValidationSQLEndpoint{
				engine:  adapterValidationSQLite,
				queryer: database, database: database,
				parameterLimit: adapterValidationSQLiteParameterLimit,
			},
			&sqliteTargetAdapter{database: database},
			[]adapterTablePlan{{
				source: table,
				target: table,
				columns: []string{
					"id",
					"payload",
					"marker",
				},
			}},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "same live relation") {
			t.Fatalf("same-relation validation error = %v", err)
		}
	})

	t.Run("cross engine", func(t *testing.T) {
		database := openAdapterValidationSQLiteTestDatabase(
			t,
			filepath.Join(t.TempDir(), "cross.db"),
		)
		table := adapterValidationSQLiteTestTable("items")
		source := &sqliteSourceAdapter{database: database}
		transaction, err := database.Begin()
		if err != nil {
			t.Fatal(err)
		}
		source.snapshot = transaction
		t.Cleanup(func() {
			_ = transaction.Rollback()
		})
		_, err = source.Stage4ValidationProbe(
			source,
			&postgresTargetAdapter{
				database:  database,
				namespace: "public",
			},
			[]adapterTablePlan{{
				source: table,
				target: schema.Table{
					Schema: "public", Name: table.Name,
					Columns: table.Columns,
				},
				columns: []string{"id", "payload", "marker"},
			}},
		)
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"lacks a certified cross-engine",
			) {
			t.Fatalf("cross-engine validation error = %v", err)
		}
	})

	t.Run("uncertified text key", func(t *testing.T) {
		table := adapterValidationSQLiteTestTable("items")
		table.Columns[0].Type = "text"
		table.Columns[0].DeclaredType = &schema.DeclaredType{
			Base: "text",
		}
		_, _, _, err := adapterValidationEqualityProof(
			adapterValidationSQLite,
			adapterTablePlan{
				source: table,
				target: table,
				columns: []string{
					"id",
					"payload",
					"marker",
				},
			},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "lacks a certified") {
			t.Fatalf("text-key validation error = %v", err)
		}
	})

	t.Run("different key types", func(t *testing.T) {
		sourceTable := adapterValidationSQLiteTestTable("items")
		targetTable := adapterValidationSQLiteTestTable("items")
		targetTable.Columns[0].Type = "bigint"
		targetTable.Columns[0].DeclaredType = &schema.DeclaredType{
			Base: "bigint",
		}
		_, _, _, err := adapterValidationEqualityProof(
			adapterValidationSQLite,
			adapterTablePlan{
				source: sourceTable,
				target: targetTable,
				columns: []string{
					"id",
					"payload",
					"marker",
				},
			},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "types differ") {
			t.Fatalf("different-key-type validation error = %v", err)
		}
	})

	t.Run("foreign equality proof", func(t *testing.T) {
		probe, table := adapterValidationSQLiteProbeFixture(t)
		_, err := probe.NullCounts(
			context.Background(),
			ValidationTarget,
			table,
			[]string{"id", "payload", "marker"},
			ValidationNullScope{
				Kind: ValidationNullScopeTargetSourcePrimaryKeys,
				PrimaryKeyColumns: []string{
					"id",
				},
				EqualityProofDigest: strings.Repeat("a", 64),
			},
		)
		if err == nil ||
			!strings.Contains(err.Error(), "certified primary-key") {
			t.Fatalf("foreign-proof validation error = %v", err)
		}
	})

	t.Run("malformed projections", func(t *testing.T) {
		database := openAdapterValidationSQLiteTestDatabase(
			t,
			filepath.Join(t.TempDir(), "projection.db"),
		)
		table := adapterValidationSQLiteTestTable("items")
		for _, projection := range [][]string{
			{"id", "payload"},
			{"id", "payload", "payload"},
			{"id", "payload", "missing"},
		} {
			_, err := newAdapterDatabaseValidationProbe(
				adapterValidationSQLEndpoint{
					engine:         adapterValidationSQLite,
					queryer:        database,
					parameterLimit: adapterValidationSQLiteParameterLimit,
				},
				&sqliteTargetAdapter{
					database: openAdapterValidationSQLiteTestDatabase(
						t,
						filepath.Join(
							t.TempDir(),
							"projection-target.db",
						),
					),
				},
				[]adapterTablePlan{{
					source:  table,
					target:  table,
					columns: projection,
				}},
			)
			if err == nil {
				t.Fatalf(
					"malformed projection %#v was accepted",
					projection,
				)
			}
		}
	})
}

func TestAdapterValidationSQLShapeAndBounds(t *testing.T) {
	endpoint := adapterValidationSQLEndpoint{
		engine:         adapterValidationPostgres,
		parameterLimit: adapterValidationPostgresParameterLimit,
	}
	primaryKey := []schema.Column{
		{Name: `tenant"id`, PrimaryKey: true, PrimaryKeyPosition: 1},
		{Name: "item_id", PrimaryKey: true, PrimaryKeyPosition: 2},
	}
	predicate, arguments, err := adapterValidationKeyPredicate(
		endpoint,
		primaryKey,
		[]ValidationPrimaryKey{
			{Values: []any{int64(7), int64(11)}},
			{Values: []any{int64(8), int64(12)}},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	wantPredicate := `(("tenant""id" = $1 AND "item_id" = $2) OR ` +
		`("tenant""id" = $3 AND "item_id" = $4))`
	if predicate != wantPredicate {
		t.Fatalf("PostgreSQL key predicate = %s", predicate)
	}
	if !reflect.DeepEqual(
		arguments,
		[]any{int64(7), int64(11), int64(8), int64(12)},
	) {
		t.Fatalf("PostgreSQL key arguments = %#v", arguments)
	}
	if got := endpoint.qualified(schema.Table{
		Schema: `tenant"schema`,
		Name:   `item"table`,
	}); got != `"tenant""schema"."item""table"` {
		t.Fatalf("PostgreSQL qualified table = %s", got)
	}
	injectedIdentifier := `safe"; DROP TABLE private.secrets; --`
	if got := endpoint.qualified(schema.Table{
		Schema: injectedIdentifier,
		Name:   injectedIdentifier,
	}); got != `"safe""; DROP TABLE private.secrets; --".`+
		`"safe""; DROP TABLE private.secrets; --"` {
		t.Fatalf("PostgreSQL injected identifier quoting = %s", got)
	}
	const secretValue = `key'); DROP TABLE private.secrets; --`
	valuePredicate, valueArguments, err := adapterValidationKeyPredicate(
		endpoint,
		primaryKey[:1],
		[]ValidationPrimaryKey{{
			Values: []any{secretValue},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(valuePredicate, secretValue) ||
		valuePredicate != `(("tenant""id" = $1))` {
		t.Fatalf(
			"PostgreSQL value predicate interpolated row data: %s",
			valuePredicate,
		)
	}
	if !reflect.DeepEqual(valueArguments, []any{secretValue}) {
		t.Fatalf("PostgreSQL value arguments = %#v", valueArguments)
	}
	if _, _, err := adapterValidationKeyPredicate(
		adapterValidationSQLEndpoint{
			engine:         adapterValidationSQLite,
			parameterLimit: 1,
		},
		primaryKey,
		[]ValidationPrimaryKey{{
			Values: []any{int64(1), int64(2)},
		}},
	); err == nil || !strings.Contains(err.Error(), "parameter limit") {
		t.Fatalf("parameter-limit error = %v", err)
	}
	if batch, err := adapterValidationKeyBatchSize(
		5,
		2,
	); err != nil || batch != 2 {
		t.Fatalf("bounded key batch = %d, %v", batch, err)
	}

	descriptorPrimaryKey := []schema.Column{{
		Name:               "id",
		Type:               "integer",
		PrimaryKey:         true,
		PrimaryKeyPosition: 1,
	}}
	if _, err := adapterValidationCanonicalKeySet(
		descriptorPrimaryKey,
		[]ValidationPrimaryKey{
			{Values: []any{int64(7)}},
			{Values: []any{int(7)}},
		},
	); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("canonical duplicate key error = %v", err)
	}
	const sensitiveKey = "customer-secret-primary-key"
	if _, err := adapterValidationCanonicalKeySet(
		descriptorPrimaryKey,
		[]ValidationPrimaryKey{{
			Values: []any{sensitiveKey},
		}},
	); err == nil || strings.Contains(err.Error(), sensitiveKey) {
		t.Fatalf("sensitive key error disclosure = %v", err)
	}
	requested, err := adapterValidationCanonicalKeySet(
		descriptorPrimaryKey,
		[]ValidationPrimaryKey{{Values: []any{int64(7)}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, rows := range map[string][]ValidationSampleRow{
		"duplicate result": {
			{Values: []any{int64(7)}},
			{Values: []any{int(7)}},
		},
		"unrequested result": {
			{Values: []any{int64(8)}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateAdapterValidationTargetRows(
				schema.Table{
					Name:    "items",
					Columns: descriptorPrimaryKey,
				},
				[]string{"id"},
				descriptorPrimaryKey,
				requested,
				rows,
			)
			if err == nil {
				t.Fatal("invalid target sample result was accepted")
			}
		})
	}
}

func adapterValidationSQLiteProbeFixture(
	t *testing.T,
) (*adapterDatabaseValidationProbe, schema.Table) {
	t.Helper()
	ctx := context.Background()
	sourceDatabase := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "proof-source.db"),
	)
	targetDatabase := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "proof-target.db"),
	)
	for _, database := range []*sql.DB{sourceDatabase, targetDatabase} {
		if _, err := database.ExecContext(ctx, `
			CREATE TABLE items (
				id INTEGER PRIMARY KEY,
				payload TEXT,
				marker BLOB NOT NULL
			)`); err != nil {
			t.Fatal(err)
		}
	}
	transaction, err := sourceDatabase.BeginTx(
		ctx,
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = transaction.Rollback()
	})
	table := adapterValidationSQLiteTestTable("items")
	source := &sqliteSourceAdapter{
		database: sourceDatabase,
		snapshot: transaction,
	}
	contract, err := source.Stage4ValidationProbe(
		source,
		&sqliteTargetAdapter{database: targetDatabase},
		[]adapterTablePlan{{
			source: table,
			target: table,
			columns: []string{
				"id",
				"payload",
				"marker",
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	return contract.(*adapterDatabaseValidationProbe), table
}

func openAdapterValidationSQLiteTestDatabase(
	t *testing.T,
	path string,
) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close SQLite validation database: %v", err)
		}
	})
	if err := database.Ping(); err != nil {
		t.Fatal(err)
	}
	return database
}

func adapterValidationSQLiteTestTable(
	name string,
) schema.Table {
	return schema.Table{
		Name: name,
		Columns: []schema.Column{
			{
				Name: "id", Type: "integer",
				DeclaredType: &schema.DeclaredType{
					Base: "integer",
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
				Name: "marker", Type: "blob",
				DeclaredType: &schema.DeclaredType{
					Base: "blob",
				},
			},
		},
	}
}
