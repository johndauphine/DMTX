package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectMySQLTableForSQLServerPreservesAdmittedShape(
	t *testing.T,
) {
	frontier := int64(41)
	source := schema.Table{
		Schema:         "source_db",
		Name:           "events",
		MySQLCollation: "utf8mb4_0900_bin",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name: "code",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 2},
				},
			},
			{
				Name: "enabled",
				Type: "integer",
				DeclaredType: &schema.DeclaredType{
					Base:      "tinyint",
					Arguments: []int{1},
				},
			},
			{
				Name:     "parent_id",
				Type:     "bigint",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name:     "payload",
				Type:     "varbinary",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base:      "varbinary",
					Arguments: []int{16},
				},
			},
			{
				Name: "occurred_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "datetime",
					Arguments: []int{6},
				},
			},
			{
				Name:     "description",
				Type:     "text",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "mediumtext",
				},
			},
		},
		Indexes: []schema.Index{
			{
				Name: "events_amount_idx",
				Columns: []schema.IndexColumn{{
					Name:       "amount",
					Descending: true,
				}},
			},
			{
				Name: "events_parent_idx",
				Columns: []schema.IndexColumn{{
					Name: "parent_id",
				}},
			},
		},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_parent_fk",
			Columns:           []string{"parent_id"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "SET NULL",
			Match:             "NONE",
		}},
	}
	source.Columns[1].Default = mustMySQLSQLServerDefault(
		t,
		source.Columns[1],
		"guest",
		false,
	)
	source.Columns[2].Default = mustMySQLSQLServerDefault(
		t,
		source.Columns[2],
		"0.00",
		false,
	)
	source.Columns[3].Default = mustMySQLSQLServerDefault(
		t,
		source.Columns[3],
		"1",
		false,
	)
	check, err := schema.ParseMySQLCatalogCheck(
		"`amount` >= 0 AND `enabled` IN (0, 1)",
		source.Columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	source.Checks = []schema.CheckConstraint{{
		Name:       "events_values_ck",
		Expression: check,
	}}

	projected, err := projectSQLServerTargetTable("mysql", source)
	if err != nil {
		t.Fatalf("project MySQL table for SQL Server: %v", err)
	}
	if projected.MySQLCollation != "" ||
		projected.Identity == nil ||
		projected.Identity == source.Identity ||
		projected.Identity.Frontier == source.Identity.Frontier ||
		*projected.Identity.Frontier != 41 {
		t.Fatalf("projected table metadata = %#v", projected)
	}
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[0],
		"bigint",
	)
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[1],
		"varchar",
		96,
	)
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[2],
		"decimal",
		12,
		2,
	)
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[3],
		"smallint",
	)
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[5],
		"varbinary",
		16,
	)
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[6],
		"timestamp",
		6,
	)
	assertMySQLSQLServerDeclaration(
		t,
		projected.Columns[7],
		"text",
	)
	for index, want := range []string{"'guest'", "0", "1"} {
		column := projected.Columns[index+1]
		if column.Default == nil ||
			column.Default == source.Columns[index+1].Default ||
			column.Default.CanonicalSQL() != want {
			t.Fatalf(
				"projected default for %s = %#v",
				column.Name,
				column.Default,
			)
		}
	}
	if len(projected.ForeignKeys) != 1 ||
		projected.ForeignKeys[0].Match != "SIMPLE" ||
		projected.ForeignKeys[0].OnUpdate != "CASCADE" ||
		projected.ForeignKeys[0].OnDelete != "SET NULL" {
		t.Fatalf(
			"projected foreign key = %#v",
			projected.ForeignKeys,
		)
	}
	if !reflect.DeepEqual(projected.Indexes, source.Indexes) ||
		!reflect.DeepEqual(projected.Checks, source.Checks) {
		t.Fatalf(
			"projected objects = indexes %#v checks %#v",
			projected.Indexes,
			projected.Checks,
		)
	}

	projected.Schema = "dbo"
	if _, err := schema.CreateSQLServerTable(projected); err != nil {
		t.Fatalf("render projected SQL Server table: %v", err)
	}
	if _, err := schema.RenderPortableCheckForSQLServer(
		projected.Checks[0].Expression,
		projected.Columns,
	); err != nil {
		t.Fatalf("render projected SQL Server CHECK: %v", err)
	}

	projected.Columns[1].DeclaredType.Arguments[0] = 1
	projected.Indexes[0].Columns[0].Name = "id"
	projected.ForeignKeys[0].Columns[0] = "id"
	*projected.Identity.Frontier = 99
	if source.Columns[1].DeclaredType.Arguments[0] != 24 ||
		source.Indexes[0].Columns[0].Name != "amount" ||
		source.ForeignKeys[0].Columns[0] != "parent_id" ||
		*source.Identity.Frontier != 41 {
		t.Fatal("projected MySQL table aliases source metadata")
	}
}

func TestProjectMySQLTableForSQLServerPinsSourceCollation(
	t *testing.T,
) {
	for _, collation := range []string{
		"utf8mb4_0900_bin",
		"utf8mb4_nopad_bin",
	} {
		t.Run(collation, func(t *testing.T) {
			source := mySQLSQLServerProjectionFixture(t)
			source.MySQLCollation = collation
			if _, err := projectMySQLTableForSQLServer(source); err != nil {
				t.Fatalf("project %s source: %v", collation, err)
			}
		})
	}
	for _, collation := range []string{
		"",
		"utf8mb4_bin",
		"utf8mb4_0900_ai_ci",
	} {
		t.Run("reject_"+collation, func(t *testing.T) {
			source := mySQLSQLServerProjectionFixture(t)
			source.MySQLCollation = collation
			assertMySQLSQLServerPolicy(
				t,
				projectMySQLTableForSQLServerError(source),
				"map MySQL collation",
			)
		})
	}
}

func TestProjectMySQLColumnForSQLServerFailsClosed(
	t *testing.T,
) {
	clockColumn := schema.Column{
		Name: "created_at",
		Type: "timestamp",
		DeclaredType: &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{3},
		},
	}
	clockColumn.Default = mustMySQLSQLServerDefault(
		t,
		clockColumn,
		"CURRENT_TIMESTAMP(3)",
		true,
	)
	tests := []struct {
		name      string
		column    schema.Column
		operation string
	}{
		{
			name: "missing declaration",
			column: schema.Column{
				Name: "value",
				Type: "bigint",
			},
			operation: "map MySQL declared type",
		},
		{
			name: "noncanonical type",
			column: schema.Column{
				Name:         "value",
				Type:         "integer",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			operation: "map MySQL type modifier",
		},
		{
			name: "tinyint modifier",
			column: schema.Column{
				Name: "value",
				Type: "integer",
				DeclaredType: &schema.DeclaredType{
					Base:      "tinyint",
					Arguments: []int{2},
				},
			},
			operation: "map MySQL type modifier",
		},
		{
			name: "decimal precision",
			column: schema.Column{
				Name: "value",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{39, 2},
				},
			},
			operation: "map MySQL type modifier",
		},
		{
			name: "varchar byte limit",
			column: schema.Column{
				Name: "value",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{2_001},
				},
			},
			operation: "map MySQL type modifier",
		},
		{
			name: "varbinary byte limit",
			column: schema.Column{
				Name: "value",
				Type: "varbinary",
				DeclaredType: &schema.DeclaredType{
					Base:      "varbinary",
					Arguments: []int{8_001},
				},
			},
			operation: "map MySQL type modifier",
		},
		{
			name: "fixed character",
			column: schema.Column{
				Name: "value",
				Type: "char",
				DeclaredType: &schema.DeclaredType{
					Base:      "char",
					Arguments: []int{8},
				},
			},
			operation: "map MySQL character type",
		},
		{
			name: "signed duration",
			column: schema.Column{
				Name: "value",
				Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base:      "time",
					Arguments: []int{6},
				},
			},
			operation: "map MySQL type",
		},
		{
			name: "JSON",
			column: schema.Column{
				Name:         "value",
				Type:         "json",
				DeclaredType: &schema.DeclaredType{Base: "json"},
			},
			operation: "map MySQL type",
		},
		{
			name: "LONGTEXT capacity",
			column: schema.Column{
				Name:         "value",
				Type:         "text",
				DeclaredType: &schema.DeclaredType{Base: "longtext"},
			},
			operation: "map MySQL type",
		},
		{
			name: "LONGBLOB capacity",
			column: schema.Column{
				Name:         "value",
				Type:         "blob",
				DeclaredType: &schema.DeclaredType{Base: "longblob"},
			},
			operation: "map MySQL type",
		},
		{
			name:      "clock default",
			column:    clockColumn,
			operation: "map MySQL default",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := projectMySQLColumnForSQLServer(test.column)
			assertMySQLSQLServerPolicy(t, err, test.operation)
		})
	}
}

func TestProjectMySQLTableForSQLServerRejectsNonportableObjects(
	t *testing.T,
) {
	tests := []struct {
		name      string
		operation string
		mutate    func(*testing.T, *schema.Table)
	}{
		{
			name:      "text primary key",
			operation: "map MySQL text primary-key collation",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[0].PrimaryKey = false
				table.Columns[0].PrimaryKeyPosition = 0
				table.Columns[1].PrimaryKey = true
				table.Columns[1].PrimaryKeyPosition = 1
			},
		},
		{
			name:      "text index",
			operation: "map MySQL text index comparison",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Indexes = []schema.Index{{
					Name: "records_label_idx",
					Columns: []schema.IndexColumn{{
						Name:      "label",
						Collation: "BINARY",
					}},
				}}
			},
		},
		{
			name:      "unexpected index collation",
			operation: "map MySQL index collation",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Indexes[0].Columns[0].Collation = "BINARY"
			},
		},
		{
			name:      "nullable unique index",
			operation: "map MySQL nullable unique index",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Indexes[0].Unique = true
				table.Columns[2].Nullable = true
			},
		},
		{
			name:      "text foreign key",
			operation: "map MySQL text foreign-key comparison",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].Columns = []string{"label"}
				table.ForeignKeys[0].ReferencedColumns =
					[]string{"label"}
			},
		},
		{
			name:      "unexpected foreign key match",
			operation: "map MySQL foreign-key match",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].Match = "SIMPLE"
			},
		},
		{
			name:      "RESTRICT action",
			operation: "map MySQL foreign-key action",
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].OnDelete = "RESTRICT"
			},
		},
		{
			name:      "text CHECK",
			operation: "map MySQL text CHECK comparison",
			mutate: func(t *testing.T, table *schema.Table) {
				expression, err := schema.ParseMySQLCatalogCheck(
					"`label` <> ''",
					table.Columns,
				)
				if err != nil {
					t.Fatal(err)
				}
				table.Checks = []schema.CheckConstraint{{
					Name:       "records_label_ck",
					Expression: expression,
				}}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := mySQLSQLServerProjectionFixture(t)
			test.mutate(t, &table)
			assertMySQLSQLServerPolicy(
				t,
				projectMySQLTableForSQLServerError(table),
				test.operation,
			)
		})
	}
}

func mySQLSQLServerProjectionFixture(t *testing.T) schema.Table {
	t.Helper()
	table := schema.Table{
		Schema:         "source_db",
		Name:           "records",
		MySQLCollation: "utf8mb4_0900_bin",
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
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 2},
				},
			},
		},
		Indexes: []schema.Index{{
			Name: "records_amount_idx",
			Columns: []schema.IndexColumn{{
				Name: "amount",
			}},
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "records_parent_fk",
			Columns:           []string{"id"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "CASCADE",
			Match:             "NONE",
		}},
	}
	expression, err := schema.ParseMySQLCatalogCheck(
		"`amount` >= 0",
		table.Columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	table.Checks = []schema.CheckConstraint{{
		Name:       "records_amount_ck",
		Expression: expression,
	}}
	return table
}

func mustMySQLSQLServerDefault(
	t *testing.T,
	column schema.Column,
	value string,
	generated bool,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParseMySQLCatalogDefault(
		column,
		&value,
		generated,
	)
	if err != nil {
		t.Fatalf("parse MySQL default %q: %v", value, err)
	}
	return expression
}

func assertMySQLSQLServerDeclaration(
	t *testing.T,
	column schema.Column,
	base string,
	arguments ...int,
) {
	t.Helper()
	if column.DeclaredType == nil ||
		column.DeclaredType.Base != base ||
		!reflect.DeepEqual(column.DeclaredType.Arguments, arguments) {
		t.Fatalf(
			"column %s declaration = %#v, want %s%v",
			column.Name,
			column.DeclaredType,
			base,
			arguments,
		)
	}
}

func projectMySQLTableForSQLServerError(table schema.Table) error {
	_, err := projectMySQLTableForSQLServer(table)
	return err
}

func assertMySQLSQLServerPolicy(
	t *testing.T,
	err error,
	operation string,
) {
	t.Helper()
	var policy *schema.PolicyError
	if err == nil ||
		!errors.As(err, &policy) ||
		policy.Target != string(schema.SQLServer) ||
		!strings.Contains(policy.Operation, operation) {
		t.Fatalf(
			"projection error = %v, want SQL Server %q policy",
			err,
			operation,
		)
	}
}
