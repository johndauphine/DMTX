package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectPostgresTableForMySQLPreservesCommonShape(t *testing.T) {
	defaultValue := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatal(err)
		}
		return expression
	}
	check, err := schema.ParseSQLiteCheckExpression(`balance >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	source := schema.Table{
		Schema: "source",
		Name:   "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:         "code",
				Type:         "varchar",
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{24}},
				Default:      defaultValue("'guest'"),
			},
			{
				Name:         "balance",
				Type:         "numeric",
				DeclaredType: &schema.DeclaredType{Base: "numeric", Arguments: []int{12, 2}},
				Default:      defaultValue("0.00"),
			},
			{Name: "enabled", Type: "boolean", Default: defaultValue("TRUE")},
			{
				Name:     "payload",
				Type:     "bytea",
				Nullable: true,
				Default:  defaultValue("X'00FF'"),
			},
			{
				Name:         "created_at",
				Type:         "timestamp",
				DeclaredType: &schema.DeclaredType{Base: "timestamp", Arguments: []int{3}},
				Default:      defaultValue("CURRENT_TIMESTAMP"),
			},
			{Name: "document", Type: "jsonb", Nullable: true},
		},
		Indexes: []schema.Index{{
			Name:   "accounts_code_uq",
			Unique: true,
			Columns: []schema.IndexColumn{{
				Name:      "code",
				Collation: "BINARY",
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "accounts_balance_check",
			Expression: check,
		}},
	}

	got, err := projectMySQLTargetTable("postgres", source)
	if err != nil {
		t.Fatal(err)
	}
	if got.Schema != source.Schema ||
		got.MySQLCollation != "utf8mb4_0900_bin" ||
		got.Identity == nil ||
		got.Identity.Frontier == source.Identity.Frontier ||
		*got.Identity.Frontier != 41 {
		t.Fatalf("projected identity = %#v", got.Identity)
	}
	expectedTypes := []struct {
		column    int
		typ       string
		base      string
		arguments []int
	}{
		{0, "bigint", "bigint", nil},
		{1, "varchar", "varchar", []int{24}},
		{2, "numeric", "decimal", []int{12, 2}},
		{3, "integer", "tinyint", []int{1}},
		{4, "blob", "longblob", nil},
		{5, "datetime", "datetime", []int{3}},
		{6, "text", "longtext", nil},
	}
	for _, expected := range expectedTypes {
		column := got.Columns[expected.column]
		if column.Type != expected.typ ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != expected.base ||
			!reflect.DeepEqual(
				column.DeclaredType.Arguments,
				expected.arguments,
			) {
			t.Fatalf("projected column %s = %#v", column.Name, column)
		}
	}
	if got.Columns[3].Default == nil ||
		got.Columns[3].Default.CanonicalSQL() != "1" {
		t.Fatalf(
			"projected boolean default = %#v",
			got.Columns[3].Default,
		)
	}
	if got.Columns[4].Default == nil ||
		got.Columns[4].Default.CanonicalSQL() != "X'00ff'" {
		t.Fatalf(
			"projected blob default = %#v",
			got.Columns[4].Default,
		)
	}
	if len(got.Checks) != 2 ||
		got.Checks[1].Name == "" ||
		!strings.Contains(got.Checks[1].Expression.CanonicalSQL(), "IN") {
		t.Fatalf("projected checks = %#v", got.Checks)
	}
}

func TestProjectPostgresTableForMySQLNormalizesForeignKeyMatch(t *testing.T) {
	source := schema.Table{
		Name: "child",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "child_parent_fkey",
			Columns:           []string{"id"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
			Match:             "SIMPLE",
		}},
	}
	got, err := projectMySQLTargetTable("postgres", source)
	if err != nil {
		t.Fatal(err)
	}
	if got.ForeignKeys[0].Match != "NONE" {
		t.Fatalf("foreign-key match = %q", got.ForeignKeys[0].Match)
	}
}

func TestProjectPostgresTableForMySQLFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		column schema.Column
	}{
		{
			name:   "timezone-aware timestamp",
			column: schema.Column{Name: "value", Type: "timestamptz"},
		},
		{
			name: "numeric precision",
			column: schema.Column{
				Name:         "value",
				Type:         "numeric",
				DeclaredType: &schema.DeclaredType{Base: "numeric", Arguments: []int{66, 2}},
			},
		},
		{
			name: "numeric scale",
			column: schema.Column{
				Name:         "value",
				Type:         "numeric",
				DeclaredType: &schema.DeclaredType{Base: "numeric", Arguments: []int{65, 31}},
			},
		},
		{
			name: "varchar octet bound",
			column: schema.Column{
				Name:         "value",
				Type:         "varchar",
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16_384}},
			},
		},
		{
			name: "uuid semantic mismatch",
			column: schema.Column{
				Name: "value",
				Type: "uuid",
			},
		},
		{
			name: "text-preserving json",
			column: schema.Column{
				Name: "value",
				Type: "json",
			},
		},
		{
			name: "fixed-width character semantics",
			column: schema.Column{
				Name:         "value",
				Type:         "char",
				DeclaredType: &schema.DeclaredType{Base: "char", Arguments: []int{8}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := projectMySQLTargetTable("postgres", schema.Table{
				Name:    "items",
				Columns: []schema.Column{test.column},
			})
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Target != string(schema.MySQL) {
				t.Fatalf("error = %v, want MySQL policy error", err)
			}
		})
	}
}

func TestProjectMySQLTargetTableClonesSourceMetadata(t *testing.T) {
	frontier := int64(9)
	source := schema.Table{
		Name: "items",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{{
			Name:         "id",
			Type:         "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		}},
		Indexes: []schema.Index{{
			Name:    "items_id",
			Columns: []schema.IndexColumn{{Name: "id"}},
		}},
	}
	got, err := projectMySQLTargetTable("mysql", source)
	if err != nil {
		t.Fatal(err)
	}
	got.Columns[0].DeclaredType.Base = "int"
	got.Indexes[0].Columns[0].Name = "changed"
	*got.Identity.Frontier = 10
	if source.Columns[0].DeclaredType.Base != "bigint" ||
		source.Indexes[0].Columns[0].Name != "id" ||
		*source.Identity.Frontier != 9 {
		t.Fatalf("projection mutated source metadata: %#v", source)
	}
}
