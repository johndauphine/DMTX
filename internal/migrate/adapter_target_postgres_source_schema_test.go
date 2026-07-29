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
	planned, err := adapter.PlanTable("sqlite", source, "")
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
	_, err := adapter.PlanTable("sqlite", source, "drop_recreate")
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
	planned, err := adapter.PlanTable("postgres", source, "upsert")
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
	literalDefault, err := schema.ParseSQLiteDefault("0")
	if err != nil {
		t.Fatalf("ParseSQLiteDefault: %v", err)
	}
	sequence := int64(7)
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
			name:      "autoincrement",
			operation: "map SQLite AUTOINCREMENT",
			mutate: func(table *schema.Table) {
				table.AutoIncrementColumn = "id"
			},
		},
		{
			name:      "sequence",
			operation: "map SQLite AUTOINCREMENT",
			mutate: func(table *schema.Table) {
				table.SQLiteSequence = &sequence
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
			name:      "index",
			operation: "map SQLite indexes",
			mutate: func(table *schema.Table) {
				table.Indexes = []schema.Index{{Name: "events_id"}}
			},
		},
		{
			name:      "foreign key",
			operation: "map SQLite foreign keys",
			mutate: func(table *schema.Table) {
				table.ForeignKeys = []schema.ForeignKey{{}}
			},
		},
		{
			name:      "check",
			operation: "map SQLite checks",
			mutate: func(table *schema.Table) {
				table.Checks = []schema.CheckConstraint{{Name: "positive_id"}}
			},
		},
		{
			name:      "default",
			operation: "map SQLite default",
			mutate: func(table *schema.Table) {
				table.Columns[0].Default = literalDefault
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
			_, err := adapter.PlanTable(
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
	if _, err := adapter.PlanTable(
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
	if _, err := adapter.PlanTable(
		"postgres",
		table,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "duplicate column") {
		t.Fatalf("native shape error = %v", err)
	}
}
