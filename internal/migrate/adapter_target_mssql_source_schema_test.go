package migrate

import (
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectPostgresTableForSQLServerRejectsUUIDComparisonRoles(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression("token IS NULL")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*schema.Table)
		role   string
	}{
		{
			name: "primary key",
			mutate: func(table *schema.Table) {
				table.Columns[0].PrimaryKey = false
				table.Columns[0].PrimaryKeyPosition = 0
				table.Columns[1].PrimaryKey = true
				table.Columns[1].PrimaryKeyPosition = 1
			},
			role: "UUID primary-key comparison",
		},
		{
			name: "index",
			mutate: func(table *schema.Table) {
				table.Indexes = []schema.Index{{
					Name: "records_token_idx",
					Columns: []schema.IndexColumn{{
						Name: "token",
					}},
				}}
			},
			role: "UUID index comparison",
		},
		{
			name: "foreign key",
			mutate: func(table *schema.Table) {
				table.ForeignKeys = []schema.ForeignKey{{
					Name:              "records_token_fk",
					Columns:           []string{"token"},
					ReferencedTable:   "parents",
					ReferencedColumns: []string{"token"},
					Match:             "SIMPLE",
				}}
			},
			role: "UUID foreign-key comparison",
		},
		{
			name: "CHECK",
			mutate: func(table *schema.Table) {
				table.Checks = []schema.CheckConstraint{{
					Name:       "records_token_ck",
					Expression: check,
				}}
			},
			role: "UUID CHECK comparison",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := postgresSQLServerUUIDProjectionFixture()
			test.mutate(&table)
			_, err := projectPostgresTableForSQLServer(table)
			var policy *schema.PolicyError
			if err == nil ||
				!errors.As(err, &policy) ||
				!strings.Contains(err.Error(), test.role) {
				t.Fatalf("projection error = %v, want %q policy", err, test.role)
			}
		})
	}
}

func TestProjectPostgresTableForSQLServerAllowsScalarUUID(t *testing.T) {
	table := postgresSQLServerUUIDProjectionFixture()
	projected, err := projectPostgresTableForSQLServer(table)
	if err != nil {
		t.Fatalf("project scalar UUID: %v", err)
	}
	if projected.Columns[1].Type != "uuid" ||
		projected.Columns[1].DeclaredType == nil ||
		projected.Columns[1].DeclaredType.Base != "uuid" {
		t.Fatalf("projected UUID = %#v", projected.Columns[1])
	}
}

func TestProjectPostgresTableForSQLServerRejectsTextIndexes(t *testing.T) {
	for _, unique := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonunique", true: "unique"}[unique], func(t *testing.T) {
			table := schema.Table{
				Schema: "public",
				Name:   "records",
				Columns: []schema.Column{
					{
						Name:               "id",
						Type:               "bigint",
						DeclaredType:       &schema.DeclaredType{Base: "bigint"},
						PrimaryKey:         true,
						PrimaryKeyPosition: 1,
					},
					{
						Name: "label",
						Type: "text",
						DeclaredType: &schema.DeclaredType{
							Base: "text",
						},
					},
				},
				Indexes: []schema.Index{{
					Name:   "records_label_idx",
					Unique: unique,
					Columns: []schema.IndexColumn{{
						Name:      "label",
						Collation: "BINARY",
					}},
				}},
			}
			_, err := projectPostgresTableForSQLServer(table)
			var policy *schema.PolicyError
			if err == nil ||
				!errors.As(err, &policy) ||
				!strings.Contains(
					err.Error(),
					"text index comparison",
				) {
				t.Fatalf("text-index projection error = %v", err)
			}
		})
	}
}

func postgresSQLServerUUIDProjectionFixture() schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   "records",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:         "token",
				Type:         "uuid",
				DeclaredType: &schema.DeclaredType{Base: "uuid"},
				Nullable:     true,
			},
		},
	}
}
