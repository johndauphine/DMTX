package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresRetainedRowWidthUsesCatalogAndLiveEvidence(
	t *testing.T,
) {
	t.Parallel()
	table := schema.Table{
		Schema: "source",
		Name:   "payloads",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{
				Name: "label",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{3},
				},
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{5, 2},
				},
			},
			{Name: "note", Type: "text"},
			{Name: "payload", Type: "bytea"},
			{Name: "document", Type: "jsonb"},
			{Name: "created", Type: "date"},
			{Name: "clock", Type: "time"},
		},
	}
	columns := adapterColumnNames(table)
	connector := &adapterRetainedBoundTestConnector{
		columns: []string{
			"dmtx_retained_3",
			"dmtx_retained_4",
			"dmtx_retained_5",
		},
		rows: [][]driver.Value{{
			int64(5),
			int64(4),
			int64(7),
		}},
	}
	database := sql.OpenDB(connector)
	t.Cleanup(func() { _ = database.Close() })
	source := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine: "postgres",
		},
		database:  database,
		namespace: "source",
	}

	stable, err := newAdapterRetainedStableRelationalView(
		source,
		&adapterRetainedBoundTestStableView{
			queryer: database,
			engine:  "postgres",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := stable.PlanRetainedRowWidth(
		context.Background(),
		table,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = int64(866)
	if !evidence.Trustworthy ||
		evidence.CompleteColumnCount != len(columns) ||
		evidence.ExpectedColumnCount != len(columns) ||
		evidence.UpperBoundBytes != want {
		t.Fatalf("PostgreSQL retained evidence = %#v, want %d", evidence, want)
	}
	query := connector.observedQuery()
	for _, fragment := range []string{
		`octet_length("note")::bigint`,
		`octet_length("payload")::bigint`,
		`octet_length(("document")::text)::bigint`,
		`FROM "source"."payloads"`,
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("PostgreSQL retained query %q lacks %q", query, fragment)
		}
	}

	actual, err := measureAdapterRetainedRowBytes([]any{
		int64(9),
		"界",
		"-12.34",
		"hello",
		[]byte{1, 2, 3, 4},
		[]byte(`{"a":1}`),
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
		"23:59:59.999999",
	})
	if err != nil {
		t.Fatal(err)
	}
	if actual > evidence.UpperBoundBytes {
		t.Fatalf(
			"measured PostgreSQL retained row = %d, bound = %d",
			actual,
			evidence.UpperBoundBytes,
		)
	}
}

func TestPostgresRetainedRowWidthFailsClosed(t *testing.T) {
	t.Parallel()
	valid := schema.Table{
		Schema: "source",
		Name:   "items",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "body", Type: "text"},
		},
	}
	tests := map[string]struct {
		table         schema.Table
		columns       []string
		resultColumns []string
		resultRows    [][]driver.Value
		closeErr      error
	}{
		"unknown selected column": {
			table: valid, columns: []string{"missing"},
		},
		"duplicate selected column": {
			table: valid, columns: []string{"id", "id"},
		},
		"corrupt catalog type": {
			table: func() schema.Table {
				value := valid
				value.Columns = append([]schema.Column(nil), valid.Columns...)
				value.Columns[0].DeclaredType = &schema.DeclaredType{
					Base: "bigint",
				}
				return value
			}(),
			columns: []string{"id"},
		},
		"wrong aggregate shape": {
			table: valid, columns: []string{"body"},
			resultColumns: []string{"unexpected"},
			resultRows:    [][]driver.Value{{int64(1)}},
		},
		"multiple aggregate rows": {
			table: valid, columns: []string{"body"},
			resultColumns: []string{"dmtx_retained_0"},
			resultRows:    [][]driver.Value{{int64(1)}, {int64(2)}},
		},
		"negative live length": {
			table: valid, columns: []string{"body"},
			resultColumns: []string{"dmtx_retained_0"},
			resultRows:    [][]driver.Value{{int64(-1)}},
		},
		"overflowing live length": {
			table: valid, columns: []string{"body"},
			resultColumns: []string{"dmtx_retained_0"},
			resultRows:    [][]driver.Value{{int64(math.MaxInt64)}},
		},
		"aggregate close failure": {
			table: valid, columns: []string{"body"},
			resultColumns: []string{"dmtx_retained_0"},
			resultRows:    [][]driver.Value{{int64(1)}},
			closeErr:      errors.New("injected retained result close failure"),
		},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			connector := &adapterRetainedBoundTestConnector{
				columns:  test.resultColumns,
				rows:     test.resultRows,
				closeErr: test.closeErr,
			}
			database := sql.OpenDB(connector)
			t.Cleanup(func() { _ = database.Close() })
			source := &relationalSourceAdapter{
				spec:      relationalSourceSpec{engine: "postgres"},
				database:  database,
				namespace: "source",
			}
			stable, err := newAdapterRetainedStableRelationalView(
				source,
				&adapterRetainedBoundTestStableView{
					queryer: database,
					engine:  "postgres",
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := stable.PlanRetainedRowWidth(
				context.Background(),
				test.table,
				test.columns,
			); err == nil {
				t.Fatal("invalid retained row proof succeeded")
			}
		})
	}
}

func TestRelationalRetainedRowWidthRequiresDiscoveredSchema(
	t *testing.T,
) {
	t.Parallel()
	tests := []struct {
		name string
		plan func(schema.Table) error
	}{
		{
			name: "PostgreSQL",
			plan: func(table schema.Table) error {
				_, err := planPostgresRetainedRowBound(
					"source",
					table,
					table.Columns,
				)
				return err
			},
		},
		{
			name: "MySQL family",
			plan: func(table schema.Table) error {
				table.Columns[0] = retainedTestDeclaredColumn(
					"id",
					"bigint",
					"bigint",
				)
				_, err := planMySQLRetainedRowBound(
					"source",
					table,
					table.Columns,
				)
				return err
			},
		},
		{
			name: "SQL Server",
			plan: func(table schema.Table) error {
				table.Columns[0] = retainedTestDeclaredColumn(
					"id",
					"bigint",
					"bigint",
				)
				_, err := planSQLServerRetainedRowBound(
					"source",
					table,
					table.Columns,
				)
				return err
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := test.plan(schema.Table{
				Name: "items",
				Columns: []schema.Column{{
					Name: "id",
					Type: "bigint",
				}},
			})
			if err == nil ||
				!strings.Contains(err.Error(), "discovered source schema") {
				t.Fatalf("missing source schema error = %v", err)
			}
		})
	}
}

func TestMutableRelationalRetainedRowWidthRejectsDynamicEvidenceBeforeQuery(
	t *testing.T,
) {
	t.Parallel()
	tests := map[string]struct {
		engine string
		table  schema.Table
	}{
		"PostgreSQL": {
			engine: "postgres",
			table: schema.Table{
				Schema: "source",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "body", Type: "text"},
				},
			},
		},
		"MySQL family": {
			engine: "mysql",
			table: schema.Table{
				Schema: "source",
				Name:   "items",
				Columns: []schema.Column{
					retainedTestDeclaredColumn(
						"body",
						"text",
						"longtext",
					),
				},
			},
		},
		"SQL Server": {
			engine: "mssql",
			table: schema.Table{
				Schema: "source",
				Name:   "items",
				Columns: []schema.Column{
					retainedTestDeclaredColumn(
						"body",
						"text",
						"text",
					),
				},
			},
		},
	}
	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			connector := &adapterRetainedBoundTestConnector{
				columns: []string{"dmtx_retained_0"},
				rows:    [][]driver.Value{{int64(1)}},
			}
			database := sql.OpenDB(connector)
			t.Cleanup(func() { _ = database.Close() })
			source := &relationalSourceAdapter{
				spec: relationalSourceSpec{
					engine:      test.engine,
					displayName: name,
				},
				database:  database,
				namespace: "source",
			}
			if _, err := planAdapterSourceRetainedRowWidth(
				context.Background(),
				source,
				test.table,
				[]string{"body"},
			); err == nil ||
				!strings.Contains(err.Error(), "active stable source view") {
				t.Fatalf("mutable dynamic retained evidence error = %v", err)
			}
			if query := connector.observedQuery(); query != "" {
				t.Fatalf(
					"mutable dynamic retained evidence emitted query %q",
					query,
				)
			}
		})
	}
}

func TestRetainedIdentifierAdmissionMatchesEngineCatalogs(t *testing.T) {
	t.Parallel()
	invalidUTF8 := string([]byte{0xff})
	tests := []struct {
		name   string
		engine string
		value  string
		valid  bool
	}{
		{"PostgreSQL 63 bytes", "postgres", strings.Repeat("é", 31) + "a", true},
		{"PostgreSQL 64 bytes", "postgres", strings.Repeat("é", 32), false},
		{"PostgreSQL NUL", "postgres", "bad\x00name", false},
		{"PostgreSQL invalid UTF-8", "postgres", invalidUTF8, false},
		{"MySQL 64 BMP characters", "mysql", strings.Repeat("界", 64), true},
		{"MySQL 65 BMP characters", "mysql", strings.Repeat("界", 65), false},
		{"MySQL supplementary rune", "mysql", "emoji😀", false},
		{"MySQL trailing space", "mysql", "items ", false},
		{"MySQL NUL", "mysql", "bad\x00name", false},
		{"MySQL invalid UTF-8", "mysql", invalidUTF8, false},
		{"SQL Server 128 UTF-16 units", "mssql", strings.Repeat("😀", 64), true},
		{"SQL Server 130 UTF-16 units", "mssql", strings.Repeat("😀", 65), false},
		{"SQL Server replacement rune", "mssql", "bad\uFFFDname", false},
		{"SQL Server trailing space", "mssql", "items ", false},
		{"SQL Server NUL", "mssql", "bad\x00name", false},
		{"SQL Server invalid UTF-8", "mssql", invalidUTF8, false},
		{"SQLite supplementary rune", "sqlite", "emoji😀", true},
		{"SQLite NUL", "sqlite", "bad\x00name", false},
		{"SQLite invalid UTF-8", "sqlite", invalidUTF8, false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateAdapterRetainedIdentifier(
				test.engine,
				"column",
				test.value,
			)
			if (err == nil) != test.valid {
				t.Fatalf(
					"identifier admission error = %v, valid = %t",
					err,
					test.valid,
				)
			}
		})
	}
}

func TestRetainedIdentifierValidationCoversEveryQueryComponent(
	t *testing.T,
) {
	t.Parallel()
	type engineFixture struct {
		engine           string
		invalid          string
		table            schema.Table
		planBadNamespace func(schema.Table, []schema.Column) error
	}
	fixtures := []engineFixture{
		{
			engine:  "postgres",
			invalid: strings.Repeat("é", 32),
			table: schema.Table{
				Schema: "source",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", Type: "bigint"},
				},
			},
			planBadNamespace: func(
				table schema.Table,
				columns []schema.Column,
			) error {
				_, err := planPostgresRetainedRowBound(
					strings.Repeat("é", 32),
					table,
					columns,
				)
				return err
			},
		},
		{
			engine:  "mysql",
			invalid: "emoji😀",
			table: schema.Table{
				Schema: "source",
				Name:   "items",
				Columns: []schema.Column{
					retainedTestDeclaredColumn(
						"id",
						"bigint",
						"bigint",
					),
				},
			},
			planBadNamespace: func(
				table schema.Table,
				columns []schema.Column,
			) error {
				_, err := planMySQLRetainedRowBound(
					"emoji😀",
					table,
					columns,
				)
				return err
			},
		},
		{
			engine:  "mssql",
			invalid: "bad\uFFFDname",
			table: schema.Table{
				Schema: "source",
				Name:   "items",
				Columns: []schema.Column{
					retainedTestDeclaredColumn(
						"id",
						"bigint",
						"bigint",
					),
				},
			},
			planBadNamespace: func(
				table schema.Table,
				columns []schema.Column,
			) error {
				_, err := planSQLServerRetainedRowBound(
					"bad\uFFFDname",
					table,
					columns,
				)
				return err
			},
		},
		{
			engine:  "sqlite",
			invalid: "bad\x00name",
			table: schema.Table{
				Name: "items",
				Columns: []schema.Column{
					retainedTestDeclaredColumn(
						"id",
						"integer",
						"integer",
					),
				},
			},
			planBadNamespace: func(
				table schema.Table,
				columns []schema.Column,
			) error {
				table.Schema = "main"
				_, err := planSQLiteRetainedRowBound(table, columns)
				return err
			},
		},
	}
	for _, fixture := range fixtures {
		fixture := fixture
		t.Run(fixture.engine, func(t *testing.T) {
			t.Parallel()
			if err := fixture.planBadNamespace(
				fixture.table,
				fixture.table.Columns,
			); err == nil {
				t.Fatal("invalid namespace entered retained query planning")
			}

			invalidTable := fixture.table
			invalidTable.Name = fixture.invalid
			if _, err := exactAdapterRetainedColumns(
				fixture.engine,
				invalidTable,
				[]string{"id"},
			); err == nil {
				t.Fatal("invalid table entered retained query planning")
			}

			invalidCatalog := fixture.table
			invalidCatalog.Columns = append(
				[]schema.Column(nil),
				fixture.table.Columns...,
			)
			invalidCatalog.Columns[0].Name = fixture.invalid
			if _, err := exactAdapterRetainedColumns(
				fixture.engine,
				invalidCatalog,
				[]string{"id"},
			); err == nil {
				t.Fatal("invalid catalog column entered retained query planning")
			}

			if _, err := exactAdapterRetainedColumns(
				fixture.engine,
				fixture.table,
				[]string{fixture.invalid},
			); err == nil {
				t.Fatal("invalid selected column entered retained query planning")
			}
		})
	}
}

func TestMySQLFamilyRetainedRowWidthUsesCatalogAndLiveEvidence(
	t *testing.T,
) {
	t.Parallel()
	table := schema.Table{
		Schema: "source",
		Name:   "payloads",
		Columns: []schema.Column{
			retainedTestDeclaredColumn("id", "bigint", "bigint"),
			retainedTestDeclaredColumn(
				"label",
				"varchar",
				"varchar",
				3,
			),
			retainedTestDeclaredColumn(
				"amount",
				"numeric",
				"decimal",
				5,
				2,
			),
			retainedTestDeclaredColumn("note", "text", "longtext"),
			retainedTestDeclaredColumn("payload", "blob", "longblob"),
			retainedTestDeclaredColumn("document", "json", "json"),
			retainedTestDeclaredColumn("created", "date", "date"),
			retainedTestDeclaredColumn("clock", "time", "time", 6),
			retainedTestDeclaredColumn(
				"updated",
				"datetime",
				"datetime",
				6,
			),
		},
	}
	connector := &adapterRetainedBoundTestConnector{
		columns: []string{
			"dmtx_retained_3",
			"dmtx_retained_4",
			"dmtx_retained_5",
		},
		rows: [][]driver.Value{{
			int64(5),
			int64(4),
			int64(7),
		}},
	}
	database := sql.OpenDB(connector)
	t.Cleanup(func() { _ = database.Close() })
	source := &relationalSourceAdapter{
		spec:      relationalSourceSpec{engine: "mysql"},
		database:  database,
		namespace: "source",
	}
	columns := adapterColumnNames(table)
	stable, err := newAdapterRetainedStableRelationalView(
		source,
		&adapterRetainedBoundTestStableView{
			queryer: database,
			engine:  "mysql",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := stable.PlanRetainedRowWidth(
		context.Background(),
		table,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = int64(1080)
	if evidence.UpperBoundBytes != want {
		t.Fatalf("MySQL retained bound = %d, want %d", evidence.UpperBoundBytes, want)
	}
	query := connector.observedQuery()
	for _, fragment := range []string{
		"CAST(OCTET_LENGTH(`note`) AS SIGNED)",
		"CAST(OCTET_LENGTH(`payload`) AS SIGNED)",
		"CAST(OCTET_LENGTH(`document`) AS SIGNED)",
		"FROM `source`.`payloads`",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("MySQL retained query %q lacks %q", query, fragment)
		}
	}
	actual, err := measureAdapterRetainedRowBytes([]any{
		int64(9),
		[]byte("界"),
		[]byte("-12.34"),
		[]byte("hello"),
		[]byte{1, 2, 3, 4},
		[]byte(`{"a":1}`),
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
		"-838:59:59.999999",
		time.Date(2026, 7, 30, 10, 11, 12, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	if actual > evidence.UpperBoundBytes {
		t.Fatalf(
			"measured MySQL retained row = %d, bound = %d",
			actual,
			evidence.UpperBoundBytes,
		)
	}
	transient, err := retainedTestTransientRowBytes(
		[]any{
			int64(9),
			[]byte("界"),
			[]byte("-12.34"),
			[]byte("hello"),
			[]byte{1, 2, 3, 4},
			[]byte(`{"a":1}`),
			[]byte("2026-07-30"),
			[]byte("-838:59:59.999999"),
			[]byte("2026-07-30 10:11:12.000000"),
		},
		[]any{
			int64(9),
			[]byte("界"),
			[]byte("-12.34"),
			[]byte("hello"),
			[]byte{1, 2, 3, 4},
			[]byte(`{"a":1}`),
			time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
			"-838:59:59.999999",
			time.Date(2026, 7, 30, 10, 11, 12, 0, time.UTC),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if transient > evidence.UpperBoundBytes {
		t.Fatalf(
			"maxRows=1 MySQL scan/copy peak = %d, bound = %d",
			transient,
			evidence.UpperBoundBytes,
		)
	}
}

func TestMySQLSpatialRetainedRowWidthUsesExactLivePayloadEvidence(
	t *testing.T,
) {
	t.Parallel()
	srid := uint32(4326)
	table := schema.Table{
		Schema: "source",
		Name:   "places",
		Columns: []schema.Column{
			retainedTestDeclaredColumn("id", "bigint", "bigint"),
			{
				Name: "position",
				Type: "point",
				DeclaredType: &schema.DeclaredType{
					Base: "point",
					Spatial: &schema.SpatialTypeMetadata{
						Subtype: schema.SpatialSubtypePoint,
						SRID:    &srid,
					},
				},
			},
		},
	}
	connector := &adapterRetainedBoundTestConnector{
		columns: []string{"dmtx_retained_1"},
		rows:    [][]driver.Value{{int64(25)}},
	}
	database := sql.OpenDB(connector)
	t.Cleanup(func() { _ = database.Close() })
	source := &relationalSourceAdapter{
		spec:      relationalSourceSpec{engine: "mysql"},
		database:  database,
		namespace: "source",
	}
	stable, err := newAdapterRetainedStableRelationalView(
		source,
		&adapterRetainedBoundTestStableView{
			queryer: database,
			engine:  "mysql",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := stable.PlanRetainedRowWidth(
		context.Background(),
		table,
		adapterColumnNames(table),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !evidence.Trustworthy ||
		evidence.CompleteColumnCount != 2 ||
		evidence.UpperBoundBytes < 25 {
		t.Fatalf("spatial retained-row evidence = %+v", evidence)
	}
	if query := connector.observedQuery(); !strings.Contains(
		query,
		"CAST(OCTET_LENGTH(`position`) AS SIGNED)",
	) {
		t.Fatalf("spatial retained query = %q", query)
	}

	invalid := table
	invalid.Columns = append([]schema.Column(nil), table.Columns...)
	declaration := *invalid.Columns[1].DeclaredType
	spatial := *declaration.Spatial
	spatial.SRID = nil
	spatial.Subtype = schema.SpatialSubtypeGeometry
	declaration.Spatial = &spatial
	invalid.Columns[1].DeclaredType = &declaration
	if _, err := planMySQLRetainedRowBound(
		invalid.Schema,
		invalid,
		invalid.Columns,
	); err == nil {
		t.Fatal("mismatched spatial subtype entered retained-row planning")
	}
}

func TestSQLServerRetainedRowWidthUsesCatalogAndLiveEvidence(
	t *testing.T,
) {
	t.Parallel()
	table := schema.Table{
		Schema: "source",
		Name:   "payloads",
		Columns: []schema.Column{
			retainedTestDeclaredColumn("id", "bigint", "bigint"),
			retainedTestDeclaredColumn("label", "text", "varchar", 3),
			retainedTestDeclaredColumn(
				"amount",
				"numeric",
				"decimal",
				5,
				2,
			),
			retainedTestDeclaredColumn("note", "text", "text"),
			retainedTestDeclaredColumn("payload", "blob", "blob"),
			retainedTestDeclaredColumn("created", "date", "date"),
			retainedTestDeclaredColumn("clock", "time", "time", 6),
			retainedTestDeclaredColumn(
				"updated",
				"datetime",
				"timestamp",
				6,
			),
			retainedTestDeclaredColumn("external_id", "uuid", "uuid"),
		},
	}
	connector := &adapterRetainedBoundTestConnector{
		columns: []string{
			"dmtx_retained_3",
			"dmtx_retained_4",
		},
		rows: [][]driver.Value{{int64(5), int64(4)}},
	}
	database := sql.OpenDB(connector)
	t.Cleanup(func() { _ = database.Close() })
	source := &relationalSourceAdapter{
		spec:      relationalSourceSpec{engine: "mssql"},
		database:  database,
		namespace: "source",
	}
	columns := adapterColumnNames(table)
	stable, err := newAdapterRetainedStableRelationalView(
		source,
		&adapterRetainedBoundTestStableView{
			queryer: database,
			engine:  "mssql",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := stable.PlanRetainedRowWidth(
		context.Background(),
		table,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = int64(980)
	if evidence.UpperBoundBytes != want {
		t.Fatalf(
			"SQL Server retained bound = %d, want %d",
			evidence.UpperBoundBytes,
			want,
		)
	}
	query := connector.observedQuery()
	for _, fragment := range []string{
		"CONVERT(bigint, DATALENGTH([note]))",
		"CONVERT(bigint, DATALENGTH([payload]))",
		"FROM [source].[payloads]",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf(
				"SQL Server retained query %q lacks %q",
				query,
				fragment,
			)
		}
	}
	actual, err := measureAdapterRetainedRowBytes([]any{
		int64(9),
		"abc",
		"-12.34",
		"hello",
		[]byte{1, 2, 3, 4},
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
		"23:59:59.999999",
		time.Date(2026, 7, 30, 10, 11, 12, 0, time.UTC),
		"ff19966f-868b-11d0-b42d-00c04fc964ff",
	})
	if err != nil {
		t.Fatal(err)
	}
	if actual > evidence.UpperBoundBytes {
		t.Fatalf(
			"measured SQL Server retained row = %d, bound = %d",
			actual,
			evidence.UpperBoundBytes,
		)
	}
	transient, err := retainedTestTransientRowBytes(
		[]any{
			int64(9),
			"abc",
			[]byte("-12.34"),
			"hello",
			[]byte{1, 2, 3, 4},
			time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
			time.Date(1, 1, 1, 23, 59, 59, 999999000, time.UTC),
			time.Date(2026, 7, 30, 10, 11, 12, 0, time.UTC),
			[]byte{
				0xff, 0x19, 0x96, 0x6f,
				0x86, 0x8b,
				0x11, 0xd0,
				0xb4, 0x2d,
				0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff,
			},
		},
		[]any{
			int64(9),
			"abc",
			"-12.34",
			"hello",
			[]byte{1, 2, 3, 4},
			time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
			"23:59:59.999999",
			time.Date(2026, 7, 30, 10, 11, 12, 0, time.UTC),
			"ff19966f-868b-11d0-b42d-00c04fc964ff",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if transient > evidence.UpperBoundBytes {
		t.Fatalf(
			"maxRows=1 SQL Server scan/conversion/copy peak = %d, bound = %d",
			transient,
			evidence.UpperBoundBytes,
		)
	}
}

func TestSQLiteRetainedRowWidthUsesSnapshotLengthEvidence(
	t *testing.T,
) {
	t.Parallel()
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.Exec(`
		CREATE TABLE payloads (
			id INTEGER PRIMARY KEY,
			note TEXT NOT NULL,
			payload BLOB NOT NULL
		);
		INSERT INTO payloads VALUES (1, 'hello', x'01020304')
	`); err != nil {
		t.Fatal(err)
	}
	snapshot, err := database.BeginTx(
		context.Background(),
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	source := &sqliteSourceAdapter{
		database: database,
		snapshot: snapshot,
	}
	t.Cleanup(func() { _ = source.Close() })
	table := schema.Table{
		Name: "payloads",
		Columns: []schema.Column{
			func() schema.Column {
				column := retainedTestDeclaredColumn(
					"id",
					"integer",
					"integer",
				)
				column.PrimaryKey = true
				column.PrimaryKeyPosition = 1
				return column
			}(),
			retainedTestDeclaredColumn("note", "text", "text"),
			retainedTestDeclaredColumn("payload", "blob", "blob"),
		},
	}
	columns := adapterColumnNames(table)
	evidence, err := planAdapterSourceRetainedRowWidth(
		context.Background(),
		source,
		table,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.UpperBoundBytes != 640 {
		t.Fatalf(
			"SQLite retained bound = %d, want 640",
			evidence.UpperBoundBytes,
		)
	}
	rows, err := source.OpenRows(context.Background(), table, columns)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("SQLite retained fixture row is missing")
	}
	values := make([]any, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := rows.Scan(destinations...); err != nil {
		t.Fatal(err)
	}
	actual, err := measureAdapterRetainedRowBytes(cloneAdapterRow(values))
	if err != nil {
		t.Fatal(err)
	}
	if actual > evidence.UpperBoundBytes {
		t.Fatalf(
			"measured SQLite retained row = %d, bound = %d",
			actual,
			evidence.UpperBoundBytes,
		)
	}
}

func TestMeasureAdapterRetainedRowBytesRejectsUnknownValue(t *testing.T) {
	t.Parallel()
	if _, err := measureAdapterRetainedRowBytes(
		[]any{complex(1, 2)},
	); err == nil || !strings.Contains(err.Error(), "complex128") {
		t.Fatalf("unsupported retained value error = %v", err)
	}
}

func retainedTestDeclaredColumn(
	name string,
	typ string,
	base string,
	arguments ...int,
) schema.Column {
	return schema.Column{
		Name: name,
		Type: typ,
		DeclaredType: &schema.DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		},
	}
}

func retainedTestTransientRowBytes(
	raw []any,
	owned []any,
) (int64, error) {
	if len(raw) == 0 || len(raw) != len(owned) {
		return 0, errors.New("transient retained-row fixture is malformed")
	}
	rawBytes, err := measureAdapterRetainedRowBytes(raw)
	if err != nil {
		return 0, err
	}
	ownedBytes, err := measureAdapterRetainedRowBytes(owned)
	if err != nil {
		return 0, err
	}
	destinations, err := newAdapterRetainedRowBoundPlan(
		"test",
		"row",
		len(raw),
	)
	if err != nil {
		return 0, err
	}
	total, err := addAdapterRetainedBytes(rawBytes, ownedBytes)
	if err != nil {
		return 0, err
	}
	return addAdapterRetainedBytes(total, destinations.baseBytes)
}

type adapterRetainedBoundTestConnector struct {
	columns  []string
	rows     [][]driver.Value
	query    string
	closeErr error
}

type adapterRetainedBoundTestStableView struct {
	queryer adapterPaginationQueryer
	engine  string
}

func (view *adapterRetainedBoundTestStableView) QueryContext(
	ctx context.Context,
	query string,
	arguments ...any,
) (*sql.Rows, error) {
	return view.queryer.QueryContext(ctx, query, arguments...)
}

func (view *adapterRetainedBoundTestStableView) QueryRowContext(
	ctx context.Context,
	query string,
	arguments ...any,
) *sql.Row {
	return view.queryer.QueryRowContext(ctx, query, arguments...)
}

func (view *adapterRetainedBoundTestStableView) retainedStableViewEngine() string {
	return view.engine
}

func (connector *adapterRetainedBoundTestConnector) Connect(
	context.Context,
) (driver.Conn, error) {
	return &adapterRetainedBoundTestConnection{connector: connector}, nil
}

func (*adapterRetainedBoundTestConnector) Driver() driver.Driver {
	return adapterRetainedBoundTestDriver{}
}

func (connector *adapterRetainedBoundTestConnector) observedQuery() string {
	return connector.query
}

type adapterRetainedBoundTestDriver struct{}

func (adapterRetainedBoundTestDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected retained bound driver Open")
}

type adapterRetainedBoundTestConnection struct {
	connector *adapterRetainedBoundTestConnector
}

func (*adapterRetainedBoundTestConnection) Prepare(
	string,
) (driver.Stmt, error) {
	return nil, errors.New("unexpected retained bound Prepare")
}

func (*adapterRetainedBoundTestConnection) Close() error { return nil }

func (*adapterRetainedBoundTestConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected retained bound Begin")
}

func (connection *adapterRetainedBoundTestConnection) QueryContext(
	_ context.Context,
	query string,
	_ []driver.NamedValue,
) (driver.Rows, error) {
	connection.connector.query = query
	rows := make([][]driver.Value, len(connection.connector.rows))
	for index := range rows {
		rows[index] = append(
			[]driver.Value(nil),
			connection.connector.rows[index]...,
		)
	}
	return &adapterRetainedBoundTestRows{
		columns: append(
			[]string(nil),
			connection.connector.columns...,
		),
		rows:     rows,
		closeErr: connection.connector.closeErr,
	}, nil
}

type adapterRetainedBoundTestRows struct {
	columns  []string
	rows     [][]driver.Value
	index    int
	closeErr error
}

func (rows *adapterRetainedBoundTestRows) Columns() []string {
	return rows.columns
}

func (rows *adapterRetainedBoundTestRows) Close() error {
	return rows.closeErr
}

func (rows *adapterRetainedBoundTestRows) Next(
	destinations []driver.Value,
) error {
	if rows.index >= len(rows.rows) {
		return io.EOF
	}
	values := rows.rows[rows.index]
	rows.index++
	if len(destinations) != len(values) {
		return errors.New("retained bound destination count mismatch")
	}
	copy(destinations, values)
	return nil
}

// TestSQLServerRetainedTextLimitsMatchTheEngine keeps this bound's idea of a
// legal declaration from drifting from discovery's.
//
// Both places encode SQL Server's limits - 4000 characters for the national
// types, 8000 for the others - and they are in different packages, so nothing
// but a test stops one from being widened alone. The consequence of drift is
// quiet: an nvarchar declared past 4000 cannot exist, but if one reached here
// the bound would accept it and under-count the column by half, and this bound
// is what decides how many rows fit inside the memory ceiling.
func TestSQLServerRetainedTextLimitsMatchTheEngine(t *testing.T) {
	t.Parallel()
	for _, expected := range []struct {
		base  string
		limit int64
	}{
		{base: "nvarchar", limit: 4_000},
		{base: "nchar", limit: 4_000},
		{base: "varchar", limit: 8_000},
		{base: "char", limit: 8_000},
	} {
		if got := sqlServerRetainedTextLengthLimit(expected.base); got != expected.limit {
			t.Errorf(
				"%s limit = %d characters, want %d",
				expected.base,
				got,
				expected.limit,
			)
		}
	}
}

// TestSQLServerRetainedNationalTextCountsUTF8Bytes pins the expansion factor.
//
// The declared length is characters by the time it reaches here - discovery
// converts nchar/nvarchar from sys.columns.max_length, which is bytes - and the
// driver returns Go strings, which are UTF-8. A BMP character costs up to three
// bytes there. A surrogate pair costs four across two units, which is less per
// unit, so three is the worst case and four would be a looser bound than the
// engine can produce.
func TestSQLServerRetainedNationalTextCountsUTF8Bytes(t *testing.T) {
	t.Parallel()
	if got := sqlServerRetainedUTF8Expansion("nvarchar"); got != 3 {
		t.Errorf("nvarchar expansion = %d, want 3", got)
	}
	if got := sqlServerRetainedUTF8Expansion("nchar"); got != 3 {
		t.Errorf("nchar expansion = %d, want 3", got)
	}
	// char and varchar are deliberately 1 and that is a tracked gap, not a
	// correct answer - see the comment on sqlServerRetainedUTF8Expansion. This
	// asserts the current state so the gap is visible rather than assumed.
	if got := sqlServerRetainedUTF8Expansion("varchar"); got != 1 {
		t.Errorf("varchar expansion = %d, want 1 (the tracked gap)", got)
	}
	if got := sqlServerRetainedUTF8Expansion("bigint"); got != 1 {
		t.Errorf("non-text expansion = %d, want 1", got)
	}
}
