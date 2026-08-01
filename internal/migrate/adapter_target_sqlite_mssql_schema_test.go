package migrate

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectSQLServerTableForSQLitePreservesSafeShape(t *testing.T) {
	frontier := int64(41)
	enabled := sqlServerSQLiteTestDefault(
		t,
		schema.Column{
			Name:         "enabled",
			Type:         "boolean",
			DeclaredType: &schema.DeclaredType{Base: "bool"},
		},
		"(1)",
	)
	countDefault := sqlServerSQLiteTestDefault(
		t,
		schema.Column{
			Name: "exact_count",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{18, 0},
			},
		},
		"((0))",
	)
	source := schema.Table{
		Schema: "dbo",
		Name:   "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "enabled", Type: "boolean",
				DeclaredType: &schema.DeclaredType{Base: "bool"},
				Default:      enabled,
			},
			{
				Name: "exact_count", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{18, 0},
				},
				Default: countDefault,
			},
			{
				Name: "ratio", Type: "real",
				DeclaredType: &schema.DeclaredType{Base: "real"},
			},
			{
				Name: "code", Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{24},
				},
			},
			{
				Name: "payload", Type: "blob", Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "varbinary", Arguments: []int{16},
				},
			},
			{
				Name: "occurred_at", Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
			{
				Name: "external_id", Type: "uuid",
				DeclaredType: &schema.DeclaredType{Base: "uuid"},
			},
		},
		Indexes: []schema.Index{
			{
				Name: "accounts_occurred_idx",
				Columns: []schema.IndexColumn{
					{Name: "occurred_at", Descending: true},
				},
			},
		},
	}
	check, err := schema.ParseSQLServerCatalogCheck(
		"([exact_count]>=(0))",
		source.Columns,
	)
	if err != nil {
		t.Fatalf("parse safe SQL Server CHECK: %v", err)
	}
	source.Checks = []schema.CheckConstraint{
		{Name: "accounts_count_ck", Expression: check},
	}
	before := cloneSQLiteTargetTable(source)

	projected, err := projectSQLServerTableForSQLite(source)
	if err != nil {
		t.Fatalf("project SQL Server table: %v", err)
	}
	if projected.Schema != "" ||
		projected.Columns[2].Type != "bigint" ||
		projected.Columns[2].DeclaredType == nil ||
		projected.Columns[2].DeclaredType.Base != "bigint" ||
		projected.Columns[5].DeclaredType == nil ||
		projected.Columns[5].DeclaredType.Base != "blob" ||
		len(projected.Checks) != 2 {
		t.Fatalf("projected table = %#v", projected)
	}
	if projected.Identity == source.Identity ||
		projected.Columns[2].DeclaredType == source.Columns[2].DeclaredType {
		t.Fatal("projection retained mutable source metadata aliases")
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source table mutated: got %#v, want %#v", source, before)
	}
	if _, err := schema.CreateTable(schema.SQLite, projected); err != nil {
		t.Fatalf("render projected SQLite table: %v", err)
	}
	if _, err := schema.CreateIndexes(schema.SQLite, projected); err != nil {
		t.Fatalf("render projected SQLite indexes: %v", err)
	}
	if _, err := schema.SQLiteSequencePlan(projected); err != nil {
		t.Fatalf("render projected SQLite identity: %v", err)
	}
}

func TestProjectSQLServerTableForSQLiteFailsClosed(t *testing.T) {
	base := func() schema.Table {
		return schema.Table{
			Schema: "dbo",
			Name:   "events",
			Columns: []schema.Column{
				{
					Name: "id", Type: "bigint", PrimaryKey: true,
					PrimaryKeyPosition: 1,
					DeclaredType:       &schema.DeclaredType{Base: "bigint"},
				},
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*schema.Table)
		want   string
	}{
		{
			name: "fractional exact numeric",
			mutate: func(table *schema.Table) {
				table.Columns = append(table.Columns, schema.Column{
					Name: "amount", Type: "numeric",
					DeclaredType: &schema.DeclaredType{
						Base: "decimal", Arguments: []int{12, 2},
					},
				})
			},
			want: "exact numeric",
		},
		{
			name: "wide integer numeric",
			mutate: func(table *schema.Table) {
				table.Columns = append(table.Columns, schema.Column{
					Name: "amount", Type: "numeric",
					DeclaredType: &schema.DeclaredType{
						Base: "numeric", Arguments: []int{19, 0},
					},
				})
			},
			want: "exact numeric",
		},
		{
			name: "text primary key",
			mutate: func(table *schema.Table) {
				table.Columns[0] = schema.Column{
					Name: "id", Type: "text", PrimaryKey: true,
					PrimaryKeyPosition: 1,
					DeclaredType: &schema.DeclaredType{
						Base: "varchar", Arguments: []int{20},
					},
				}
			},
			want: "primary-key comparison",
		},
		{
			name: "text index",
			mutate: func(table *schema.Table) {
				table.Columns = append(table.Columns, schema.Column{
					Name: "note", Type: "text",
					DeclaredType: &schema.DeclaredType{
						Base: "varchar", Arguments: []int{20},
					},
				})
				table.Indexes = []schema.Index{{
					Name:    "events_note_idx",
					Columns: []schema.IndexColumn{{Name: "note"}},
				}}
			},
			want: "index comparison",
		},
		{
			name: "nullable unique index",
			mutate: func(table *schema.Table) {
				table.Columns = append(table.Columns, schema.Column{
					Name: "number", Type: "bigint", Nullable: true,
					DeclaredType: &schema.DeclaredType{Base: "bigint"},
				})
				table.Indexes = []schema.Index{{
					Name: "events_number_uq", Unique: true,
					Columns: []schema.IndexColumn{{Name: "number"}},
				}}
			},
			want: "nullable unique index",
		},
		{
			name: "floating default",
			mutate: func(table *schema.Table) {
				column := schema.Column{
					Name: "ratio", Type: "real",
					DeclaredType: &schema.DeclaredType{Base: "real"},
				}
				column.Default = sqlServerSQLiteTestDefault(
					t,
					schema.Column{
						Name: "seed", Type: "bigint",
						DeclaredType: &schema.DeclaredType{Base: "bigint"},
					},
					"(0)",
				)
				table.Columns = append(table.Columns, column)
			},
			want: "floating-point type",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := base()
			test.mutate(&table)
			if _, err := projectSQLServerTableForSQLite(table); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("projection error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSQLiteSQLServerPlanRejectsUnsafeSetAndUpsert(t *testing.T) {
	parent := schema.Table{
		Schema: "dbo",
		Name:   "parent",
		Columns: []schema.Column{{
			Name: "id", Type: "bigint", PrimaryKey: true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
	}
	child := schema.Table{
		Schema: "dbo",
		Name:   "child",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint", PrimaryKey: true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name:         "parent_id",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
		},
		ForeignKeys: []schema.ForeignKey{{
			Name: "child_parent_fk", Columns: []string{"parent_id"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "NO ACTION",
			Match:             "SIMPLE",
		}},
	}
	adapter := &sqliteTargetAdapter{}
	planned, err := adapter.PlanTables(
		"mssql",
		[]schema.Table{parent, child},
		"drop_recreate",
	)
	if err != nil || len(planned) != 2 || !adapter.sqlServerRoute {
		t.Fatalf("safe SQL Server plan = %#v, %v", planned, err)
	}
	upsert, err := adapter.PlanTables(
		"mssql",
		[]schema.Table{parent},
		"upsert",
	)
	if err != nil || len(upsert) != 1 || !adapter.sqlServerRoute {
		t.Fatalf("safe SQL Server upsert plan = %#v, %v", upsert, err)
	}
	if _, err := adapter.PlanTables(
		"mssql",
		[]schema.Table{child},
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "unselected table") {
		t.Fatalf("unselected FK error = %v", err)
	}

	parent.Indexes = []schema.Index{{
		Name:    "child",
		Columns: []schema.IndexColumn{{Name: "id"}},
	}}
	if _, err := adapter.PlanTables(
		"mssql",
		[]schema.Table{parent, child},
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "object names") {
		t.Fatalf("SQLite object collision error = %v", err)
	}
}

func TestValidateSQLServerSQLiteBatchFailsBeforeWrite(t *testing.T) {
	table := schema.Table{
		Name: "values",
		Columns: []schema.Column{
			{
				Name:         "id",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "note", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			{
				Name: "created_at", Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
		},
	}
	columns := []string{"id", "note", "created_at"}
	if err := validateSQLServerSQLiteBatch(
		table,
		columns,
		[][]any{{"9007199254740993", "東京", time.Now()}},
	); err != nil {
		t.Fatalf("validate exact batch: %v", err)
	}
	normalized, err := normalizeSQLServerSQLiteBatch(
		table,
		columns,
		[][]any{{
			"9007199254740993",
			"東京",
			time.Date(
				2026, 7, 29, 12, 34, 56, 123456000, time.UTC,
			),
		}},
	)
	if err != nil {
		t.Fatalf("normalize exact batch: %v", err)
	}
	if got, want := normalized[0][2],
		"2026-07-29 12:34:56.123456"; got != want {
		t.Fatalf("normalized timestamp = %#v, want %q", got, want)
	}
	for _, row := range [][]any{
		{"9223372036854775808", "safe", time.Now()},
		{int64(1), "contains\x00nul", time.Now()},
		{int64(1), "safe", "not a time"},
	} {
		if err := validateSQLServerSQLiteBatch(
			table,
			columns,
			[][]any{row},
		); err == nil {
			t.Fatalf("unsafe row accepted: %#v", row)
		}
	}
}

func sqlServerSQLiteTestDefault(
	t *testing.T,
	column schema.Column,
	definition string,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParseSQLServerCatalogDefault(
		column,
		&definition,
	)
	if err != nil {
		t.Fatalf("parse SQL Server default %q: %v", definition, err)
	}
	return expression
}
