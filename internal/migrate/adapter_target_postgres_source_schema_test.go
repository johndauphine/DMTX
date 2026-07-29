package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func sqliteProjectionColumn(
	name string,
	columnType string,
) schema.Column {
	return schema.Column{
		Name: name,
		Type: columnType,
		DeclaredType: &schema.DeclaredType{
			Base: columnType,
		},
	}
}

func basicSQLiteProjectionTable() schema.Table {
	column := sqliteProjectionColumn("id", "INTEGER")
	column.PrimaryKey = true
	column.PrimaryKeyPosition = 1
	tenant := sqliteProjectionColumn("tenant", "TEXT")
	tenant.PrimaryKey = true
	tenant.PrimaryKeyPosition = 2
	return schema.Table{
		Name: "events",
		Columns: []schema.Column{
			column,
			tenant,
		},
	}
}

func TestPostgresTargetPlansSQLiteTypesWithoutMutatingSource(t *testing.T) {
	source := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			sqliteProjectionColumn("id", "INTEGER"),
			sqliteProjectionColumn("reading", "DOUBLE PRECISION"),
			sqliteProjectionColumn("amount", "DECIMAL"),
			sqliteProjectionColumn("note", "VARCHAR"),
			sqliteProjectionColumn("payload", "BLOB"),
			sqliteProjectionColumn("enabled", "BOOLEAN"),
			sqliteProjectionColumn("event_day", "DATE"),
			sqliteProjectionColumn("created_at", "DATETIME"),
			sqliteProjectionColumn("document", "JSON"),
			sqliteProjectionColumn("external_id", "UUID"),
		},
	}
	source.Columns[0].PrimaryKey = true
	source.Columns[0].PrimaryKeyPosition = 1
	source.Columns[9].PrimaryKey = true
	source.Columns[9].PrimaryKeyPosition = 2
	sourceTypes := make([]string, len(source.Columns))
	sourceDeclarations := make([]*schema.DeclaredType, len(source.Columns))
	for index, column := range source.Columns {
		sourceTypes[index] = column.Type
		sourceDeclarations[index] = column.DeclaredType
	}

	adapter := &postgresTargetAdapter{namespace: "archive"}
	planned, err := planSingleTargetTable(adapter, "sqlite", source, "")
	if err != nil {
		t.Fatalf("PlanTable: %v", err)
	}
	if planned.Schema != "archive" || planned.Name != source.Name {
		t.Fatalf("planned table = %#v", planned)
	}
	wantTypes := []string{
		"bigint",
		"double precision",
		"numeric",
		"text",
		"bytea",
		"boolean",
		"date",
		"timestamp",
		"json",
		"uuid",
	}
	for index, column := range planned.Columns {
		if column.Type != wantTypes[index] {
			t.Fatalf(
				"planned column %s type = %q, want %q",
				column.Name,
				column.Type,
				wantTypes[index],
			)
		}
		if column.DeclaredType != nil {
			t.Fatalf(
				"planned column %s retained SQLite declaration",
				column.Name,
			)
		}
		if source.Columns[index].Type != sourceTypes[index] ||
			source.Columns[index].DeclaredType != sourceDeclarations[index] {
			t.Fatalf("source column %s was mutated", column.Name)
		}
	}
	if source.Schema != "" {
		t.Fatalf("source schema mutated to %q", source.Schema)
	}
}

func TestPostgresTargetRejectsSQLiteImplicitRowIDIdentityWithoutMutation(
	t *testing.T,
) {
	source := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "integer",
				},
			},
			{
				Name: "payload",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base: "text",
				},
			},
		},
	}
	before := source
	before.Columns = append([]schema.Column(nil), source.Columns...)
	for index, column := range source.Columns {
		if column.DeclaredType == nil {
			continue
		}
		declaration := *column.DeclaredType
		declaration.Arguments = append(
			[]int(nil),
			column.DeclaredType.Arguments...,
		)
		before.Columns[index].DeclaredType = &declaration
	}

	adapter := &postgresTargetAdapter{namespace: "public"}
	_, err := planSingleTargetTable(adapter, "sqlite", source, "drop_recreate")
	var policy *schema.PolicyError
	if !errors.As(err, &policy) ||
		policy.Operation != "map SQLite implicit rowid identity" ||
		policy.Type != "events.id" {
		t.Fatalf("rowid identity error = %v", err)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source table mutated: got %#v, want %#v", source, before)
	}
}

func TestPostgresTargetPlansNetworkSourceWithoutMutation(t *testing.T) {
	source := schema.Table{
		Schema: "source_namespace",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
			{Name: "note", Type: "text", Nullable: true},
		},
	}
	before := source
	before.Columns = append([]schema.Column(nil), source.Columns...)

	adapter := &postgresTargetAdapter{namespace: "target_namespace"}
	planned, err := planSingleTargetTable(adapter, "postgres", source, "upsert")
	if err != nil {
		t.Fatalf("PlanTable: %v", err)
	}
	if planned.Schema != "target_namespace" ||
		!reflect.DeepEqual(planned.Columns, source.Columns) {
		t.Fatalf("planned table = %#v", planned)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source table mutated: got %#v, want %#v", source, before)
	}
}

func TestPostgresTargetRejectsUnmappedSQLiteSchemaSemantics(t *testing.T) {
	negative := int64(-1)
	tests := []struct {
		name      string
		operation string
		mutate    func(*schema.Table)
	}{
		{
			name:      "schema namespace",
			operation: "map SQLite schema namespace",
			mutate: func(table *schema.Table) {
				table.Schema = "main"
			},
		},
		{
			name:      "invalid identity generation",
			operation: "render PostgreSQL identity",
			mutate: func(table *schema.Table) {
				table.Identity = &schema.Identity{
					Column:     "id",
					Generation: "always",
				}
			},
		},
		{
			name:      "negative identity frontier",
			operation: "render PostgreSQL identity",
			mutate: func(table *schema.Table) {
				table.Identity = &schema.Identity{
					Column:     "id",
					Generation: schema.IdentityByDefault,
					Frontier:   &negative,
				}
			},
		},
		{
			name:      "strict",
			operation: "map SQLite STRICT table",
			mutate: func(table *schema.Table) {
				table.SQLiteStrict = true
			},
		},
		{
			name:      "without rowid",
			operation: "map SQLite WITHOUT ROWID table",
			mutate: func(table *schema.Table) {
				table.SQLiteWithoutRowID = true
			},
		},
		{
			name:      "missing declared type",
			operation: "map SQLite declared type",
			mutate: func(table *schema.Table) {
				table.Columns[0].DeclaredType = nil
			},
		},
		{
			name:      "mismatched declared type",
			operation: "map SQLite declared type",
			mutate: func(table *schema.Table) {
				table.Columns[0].DeclaredType.Base = "TEXT"
			},
		},
		{
			name:      "type modifier",
			operation: "map SQLite type modifier",
			mutate: func(table *schema.Table) {
				table.Columns[0].DeclaredType.Arguments = []int{8}
			},
		},
		{
			name:      "unsupported time",
			operation: "map SQLite type",
			mutate: func(table *schema.Table) {
				table.Columns[0].Type = "TIME"
				table.Columns[0].DeclaredType.Base = "TIME"
			},
		},
	}

	adapter := &postgresTargetAdapter{namespace: "public"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := basicSQLiteProjectionTable()
			test.mutate(&source)
			_, err := planSingleTargetTable(adapter,
				"sqlite",
				source,
				"drop_recreate",
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Operation != test.operation {
				t.Fatalf(
					"error = %v, want policy operation %q",
					err,
					test.operation,
				)
			}
		})
	}
}

func TestPostgresTargetPlansSQLiteScalarModifiersAndDefaultsWithoutMutation(
	t *testing.T,
) {
	parseDefault := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatalf("ParseSQLiteDefault(%q): %v", value, err)
		}
		return expression
	}
	column := func(
		name string,
		columnType string,
		arguments []int,
		defaultSQL string,
	) schema.Column {
		value := sqliteProjectionColumn(name, columnType)
		value.DeclaredType.Arguments = append([]int(nil), arguments...)
		value.Default = parseDefault(defaultSQL)
		return value
	}
	source := schema.Table{
		Name: "scalar_defaults",
		Columns: []schema.Column{
			column("code", "CHAR", []int{4}, "'AB'"),
			column("name", "VARCHAR", []int{12}, "'guest'"),
			column("amount", "DECIMAL", []int{7, 2}, "(0.00)"),
			column("enabled", "BOOLEAN", nil, "TRUE"),
			column("payload", "BLOB", nil, "X'00FF'"),
			column("event_day", "DATE", nil, "CURRENT_DATE"),
			column(
				"created_at",
				"DATETIME",
				nil,
				"CURRENT_TIMESTAMP",
			),
		},
	}
	before := source
	before.Columns = append([]schema.Column(nil), source.Columns...)
	for index, sourceColumn := range source.Columns {
		declaration := *sourceColumn.DeclaredType
		declaration.Arguments = append(
			[]int(nil),
			sourceColumn.DeclaredType.Arguments...,
		)
		before.Columns[index].DeclaredType = &declaration
	}

	adapter := &postgresTargetAdapter{namespace: "archive"}
	planned, err := planSingleTargetTable(adapter, "sqlite", source, "drop_recreate")
	if err != nil {
		t.Fatalf("PlanTable: %v", err)
	}
	if planned.Schema != "archive" {
		t.Fatalf("planned schema = %q", planned.Schema)
	}
	wantTypes := []string{
		"text",
		"text",
		"numeric",
		"boolean",
		"bytea",
		"date",
		"timestamp",
	}
	wantDeclarations := []*schema.DeclaredType{
		{Base: "varchar", Arguments: []int{4}},
		{Base: "varchar", Arguments: []int{12}},
		{Base: "numeric", Arguments: []int{7, 2}},
		nil,
		nil,
		nil,
		nil,
	}
	for index, column := range planned.Columns {
		if column.Type != wantTypes[index] ||
			!reflect.DeepEqual(
				column.DeclaredType,
				wantDeclarations[index],
			) {
			t.Fatalf("planned column %s = %#v", column.Name, column)
		}
		if column.Default == nil {
			t.Fatalf("planned column %s lost its default", column.Name)
		}
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source table mutated: got %#v, want %#v", source, before)
	}
	planned.Columns[0].DeclaredType.Arguments[0] = 99
	if source.Columns[0].DeclaredType.Arguments[0] != 4 {
		t.Fatal("planned character modifiers alias the source declaration")
	}
}

func TestPostgresTargetRejectsUnsafeSQLiteScalarMappings(t *testing.T) {
	parseDefault := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatalf("ParseSQLiteDefault(%q): %v", value, err)
		}
		return expression
	}
	tests := []struct {
		name       string
		columnType string
		arguments  []int
		defaultSQL string
		operation  string
	}{
		{
			name:       "zero varchar length",
			columnType: "VARCHAR",
			arguments:  []int{0},
			defaultSQL: "'x'",
			operation:  "map SQLite type modifier",
		},
		{
			name:       "numeric scale beyond precision",
			columnType: "DECIMAL",
			arguments:  []int{4, 5},
			defaultSQL: "0",
			operation:  "map SQLite type modifier",
		},
		{
			name:       "overlong varchar default",
			columnType: "VARCHAR",
			arguments:  []int{2},
			defaultSQL: "'abc'",
			operation:  "render PostgreSQL default",
		},
		{
			name:       "numeric default would round",
			columnType: "DECIMAL",
			arguments:  []int{4, 2},
			defaultSQL: "1.234",
			operation:  "render PostgreSQL default",
		},
		{
			name:       "current timestamp on text",
			columnType: "TEXT",
			defaultSQL: "CURRENT_TIMESTAMP",
			operation:  "render PostgreSQL default",
		},
	}

	adapter := &postgresTargetAdapter{namespace: "public"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			column := sqliteProjectionColumn("value", test.columnType)
			column.DeclaredType.Arguments = append(
				[]int(nil),
				test.arguments...,
			)
			column.Default = parseDefault(test.defaultSQL)
			_, err := planSingleTargetTable(adapter,
				"sqlite",
				schema.Table{
					Name:    "unsafe_scalar",
					Columns: []schema.Column{column},
				},
				"drop_recreate",
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Operation != test.operation {
				t.Fatalf(
					"error = %v, want policy operation %q",
					err,
					test.operation,
				)
			}
		})
	}
}

func TestPostgresTargetPlanRejectsUnsupportedSourceAndNativeShape(
	t *testing.T,
) {
	adapter := &postgresTargetAdapter{namespace: "public"}
	table := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
		},
	}
	if _, err := planSingleTargetTable(adapter,
		"oracle",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "source engine") {
		t.Fatalf("unsupported source error = %v", err)
	}

	table.Columns = append(
		table.Columns,
		schema.Column{Name: "id", Type: "text"},
	)
	if _, err := planSingleTargetTable(adapter,
		"postgres",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "duplicate column") {
		t.Fatalf("native shape error = %v", err)
	}
}

func TestPostgresTargetPlansSQLiteObjectsAndIdentityWithoutMutation(
	t *testing.T,
) {
	fixture := func() []schema.Table {
		check, err := schema.ParseSQLiteCheckExpression("balance >= 0")
		if err != nil {
			t.Fatal(err)
		}
		sequence := int64(50)
		return []schema.Table{
			{
				Name: "account_events",
				Columns: []schema.Column{
					{
						Name:               "event_id",
						Type:               "bigint",
						PrimaryKey:         true,
						PrimaryKeyPosition: 1,
						DeclaredType: &schema.DeclaredType{
							Base: "bigint",
						},
					},
					{
						Name: "account_id",
						Type: "integer",
						DeclaredType: &schema.DeclaredType{
							Base: "integer",
						},
					},
				},
				ForeignKeys: []schema.ForeignKey{{
					Columns:           []string{"account_id"},
					ReferencedTable:   "accounts",
					ReferencedColumns: []string{"id"},
					OnUpdate:          "CASCADE",
					OnDelete:          "RESTRICT",
					Match:             "NONE",
				}},
			},
			{
				Name: "accounts",
				Identity: &schema.Identity{
					Column:     "id",
					Generation: schema.IdentityByDefault,
					Frontier:   &sequence,
				},
				Columns: []schema.Column{
					{
						Name:               "id",
						Type:               "integer",
						PrimaryKey:         true,
						PrimaryKeyPosition: 1,
						DeclaredType: &schema.DeclaredType{
							Base: "integer",
						},
					},
					{
						Name: "external_id",
						Type: "text",
						DeclaredType: &schema.DeclaredType{
							Base: "text",
						},
					},
					{
						Name: "balance",
						Type: "numeric",
						DeclaredType: &schema.DeclaredType{
							Base:      "numeric",
							Arguments: []int{12, 2},
						},
					},
				},
				Indexes: []schema.Index{{
					Name:   "accounts_external_id_uq",
					Unique: true,
					Columns: []schema.IndexColumn{{
						Name:      "external_id",
						Collation: "BINARY",
					}},
				}},
				Checks: []schema.CheckConstraint{{
					Expression: check,
				}},
			},
		}
	}
	source := fixture()
	before := fixture()
	adapter := &postgresTargetAdapter{namespace: "archive"}
	planned, err := adapter.PlanTables(
		"sqlite",
		source,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source metadata mutated:\n got: %#v\nwant: %#v", source, before)
	}
	if len(planned) != 2 ||
		planned[0].Schema != "archive" ||
		planned[1].Schema != "archive" ||
		planned[1].Identity == nil ||
		planned[1].Identity.Column != "id" ||
		planned[1].Identity.Generation != schema.IdentityByDefault ||
		planned[1].Identity.Frontier == nil ||
		*planned[1].Identity.Frontier != 50 ||
		planned[1].Columns[0].Type != "bigint" ||
		len(planned[1].Indexes) != 1 ||
		len(planned[1].Checks) != 1 ||
		len(planned[0].ForeignKeys) != 1 {
		t.Fatalf("planned object metadata = %#v", planned)
	}

	planned[1].Indexes[0].Columns[0].Name = "changed"
	planned[0].ForeignKeys[0].Columns[0] = "changed"
	if source[1].Indexes[0].Columns[0].Name != "external_id" ||
		source[0].ForeignKeys[0].Columns[0] != "account_id" {
		t.Fatal("planned object metadata aliases source slices")
	}
}
