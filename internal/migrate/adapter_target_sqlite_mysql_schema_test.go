package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectMySQLColumnForSQLiteExactMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		sourceType      string
		sourceBase      string
		sourceArguments []int
		targetType      string
		targetBase      string
		targetArguments []int
	}{
		{
			name:       "tinyint",
			sourceType: "integer", sourceBase: "tinyint",
			targetType: "integer", targetBase: "tinyint",
		},
		{
			name:       "tinyint one is integer",
			sourceType: "integer", sourceBase: "tinyint",
			sourceArguments: []int{1},
			targetType:      "integer", targetBase: "tinyint",
		},
		{
			name:       "smallint",
			sourceType: "integer", sourceBase: "smallint",
			targetType: "integer", targetBase: "smallint",
		},
		{
			name:       "mediumint",
			sourceType: "integer", sourceBase: "mediumint",
			targetType: "integer", targetBase: "mediumint",
		},
		{
			name:       "int",
			sourceType: "integer", sourceBase: "int",
			targetType: "integer", targetBase: "int",
		},
		{
			name:       "bigint",
			sourceType: "bigint", sourceBase: "bigint",
			targetType: "bigint", targetBase: "bigint",
		},
		{
			name:       "exact decimal",
			sourceType: "numeric", sourceBase: "decimal",
			sourceArguments: []int{18, 0},
			targetType:      "numeric", targetBase: "bigint",
		},
		{
			name:       "varchar",
			sourceType: "varchar", sourceBase: "varchar",
			sourceArguments: []int{40},
			targetType:      "varchar", targetBase: "varchar",
			targetArguments: []int{40},
		},
		{
			name:       "tinytext",
			sourceType: "text", sourceBase: "tinytext",
			targetType: "text", targetBase: "text",
		},
		{
			name:       "text",
			sourceType: "text", sourceBase: "text",
			targetType: "text", targetBase: "text",
		},
		{
			name:       "mediumtext",
			sourceType: "text", sourceBase: "mediumtext",
			targetType: "text", targetBase: "text",
		},
		{
			name:       "longtext",
			sourceType: "text", sourceBase: "longtext",
			targetType: "text", targetBase: "text",
		},
		{
			name:       "binary",
			sourceType: "binary", sourceBase: "binary",
			sourceArguments: []int{16},
			targetType:      "binary", targetBase: "binary",
			targetArguments: []int{16},
		},
		{
			name:       "varbinary",
			sourceType: "varbinary", sourceBase: "varbinary",
			sourceArguments: []int{255},
			targetType:      "varbinary", targetBase: "varbinary",
			targetArguments: []int{255},
		},
		{
			name:       "blob families collapse",
			sourceType: "blob", sourceBase: "mediumblob",
			targetType: "blob", targetBase: "blob",
		},
		{
			name:       "date",
			sourceType: "date", sourceBase: "date",
			targetType: "date", targetBase: "date",
		},
		{
			name:       "time",
			sourceType: "time", sourceBase: "time",
			sourceArguments: []int{6},
			targetType:      "time", targetBase: "time",
			targetArguments: []int{6},
		},
		{
			name:       "datetime",
			sourceType: "datetime", sourceBase: "datetime",
			sourceArguments: []int{3},
			targetType:      "datetime", targetBase: "datetime",
			targetArguments: []int{3},
		},
		{
			name:       "timestamp",
			sourceType: "timestamp", sourceBase: "timestamp",
			sourceArguments: []int{0},
			targetType:      "timestamp", targetBase: "timestamp",
			targetArguments: []int{0},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := schema.Column{
				Name: "value",
				Type: test.sourceType,
				DeclaredType: &schema.DeclaredType{
					Base: test.sourceBase,
					Arguments: append(
						[]int(nil),
						test.sourceArguments...,
					),
				},
			}
			projected, err := projectMySQLColumnForSQLite(source)
			if err != nil {
				t.Fatalf("project column: %v", err)
			}
			if projected.Type != test.targetType ||
				projected.DeclaredType == nil ||
				projected.DeclaredType.Base != test.targetBase ||
				!reflect.DeepEqual(
					projected.DeclaredType.Arguments,
					test.targetArguments,
				) {
				t.Fatalf(
					"projected column = %#v, want type %q declaration %q%v",
					projected,
					test.targetType,
					test.targetBase,
					test.targetArguments,
				)
			}
			if source.DeclaredType.Base != test.sourceBase ||
				!reflect.DeepEqual(
					source.DeclaredType.Arguments,
					test.sourceArguments,
				) {
				t.Fatalf("projection mutated source: %#v", source)
			}
		})
	}
}

func TestProjectMySQLColumnForSQLiteRejectsLossyShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		column schema.Column
		want   string
	}{
		{
			name:   "missing declaration",
			column: schema.Column{Name: "value", Type: "integer"},
			want:   "declared type",
		},
		{
			name: "canonical mismatch",
			column: mysqlSQLiteColumn(
				"value", "bigint", "int",
			),
			want: "integer type",
		},
		{
			name: "tinyint display width",
			column: mysqlSQLiteColumnWithArguments(
				"value", "integer", "tinyint", 4,
			),
			want: "integer type",
		},
		{
			name: "fractional decimal",
			column: mysqlSQLiteColumnWithArguments(
				"value", "numeric", "decimal", 18, 1,
			),
			want: "exact decimal",
		},
		{
			name: "wide decimal",
			column: mysqlSQLiteColumnWithArguments(
				"value", "numeric", "decimal", 19, 0,
			),
			want: "exact decimal",
		},
		{
			name: "fixed char",
			column: mysqlSQLiteColumnWithArguments(
				"value", "char", "char", 20,
			),
			want: "blank-padding",
		},
		{
			name: "double",
			column: mysqlSQLiteColumn(
				"value", "double precision", "double",
			),
			want: "floating-point",
		},
		{
			name:   "json",
			column: mysqlSQLiteColumn("value", "json", "json"),
			want:   "JSON",
		},
		{
			name: "varchar without length",
			column: mysqlSQLiteColumn(
				"value", "varchar", "varchar",
			),
			want: "varying text",
		},
		{
			name: "datetime unspecified precision",
			column: mysqlSQLiteColumn(
				"value", "datetime", "datetime",
			),
			want: "temporal",
		},
		{
			name: "binary default",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumnWithArguments(
					"value", "binary", "binary", 4,
				),
				"X'00000000'",
			),
			want: "binary default",
		},
		{
			name: "fractional current timestamp default",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumnWithArguments(
					"value", "timestamp", "timestamp", 3,
				),
				"CURRENT_TIMESTAMP",
			),
			want: "fractional temporal default",
		},
		{
			name: "noncanonical integral decimal default",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumnWithArguments(
					"value", "numeric", "decimal", 18, 0,
				),
				"9007199254740993.000",
			),
			want: "exact decimal default",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := projectMySQLColumnForSQLite(test.column)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Target != string(schema.SQLite) {
				t.Fatalf("error = %T %v, want SQLite policy", err, err)
			}
		})
	}
}

func TestProjectMySQLDefaultForSQLiteIsStructural(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		column    schema.Column
		canonical string
	}{
		{
			name: "integer",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumn("value", "integer", "int"),
				"-12",
			),
			canonical: "-12",
		},
		{
			name: "integral decimal",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumnWithArguments(
					"value", "numeric", "decimal", 18, 0,
				),
				"12",
			),
			canonical: "12",
		},
		{
			name: "unicode text",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumnWithArguments(
					"value", "varchar", "varchar", 40,
				),
				"'東京 😀'",
			),
			canonical: "'東京 😀'",
		},
		{
			name: "blob",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumn("value", "blob", "blob"),
				"X'00ff'",
			),
			canonical: "X'00ff'",
		},
		{
			name: "whole-second current timestamp",
			column: mysqlSQLiteColumnWithDefault(
				t,
				mysqlSQLiteColumnWithArguments(
					"value", "timestamp", "timestamp", 0,
				),
				"CURRENT_TIMESTAMP",
			),
			canonical: "CURRENT_TIMESTAMP",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			sourceDefault := test.column.Default
			projected, err := projectMySQLColumnForSQLite(test.column)
			if err != nil {
				t.Fatalf("project column: %v", err)
			}
			if projected.Default == nil ||
				projected.Default.CanonicalSQL() != test.canonical {
				t.Fatalf(
					"projected default = %#v, want %q",
					projected.Default,
					test.canonical,
				)
			}
			if projected.Default == sourceDefault {
				t.Fatal("projected default aliases source expression")
			}
		})
	}
}

func TestProjectMySQLTableForSQLitePreservesExactObjectsAndIsPure(
	t *testing.T,
) {
	t.Parallel()

	for _, collation := range []string{
		"utf8mb4_0900_bin",
		"utf8mb4_nopad_bin",
	} {
		collation := collation
		t.Run(collation, func(t *testing.T) {
			t.Parallel()
			source := mysqlSQLiteParentFixture(t, collation)
			before := cloneSQLiteTargetTable(source)

			projected, err := projectMySQLTableForSQLite(source)
			if err != nil {
				t.Fatalf("project table: %v", err)
			}
			if projected.Schema != "" ||
				projected.MySQLCollation != "" ||
				len(projected.ClickHouseOrderBy) != 0 {
				t.Fatalf(
					"projected table retained source-only metadata: %#v",
					projected,
				)
			}
			if projected.Identity == nil ||
				projected.Identity.Column != "id" ||
				projected.Identity.Frontier == nil ||
				*projected.Identity.Frontier != 41 {
				t.Fatalf("identity = %#v", projected.Identity)
			}
			if len(projected.Indexes) != 2 ||
				projected.Indexes[0].Name != "uq_accounts_name" ||
				!projected.Indexes[0].Unique ||
				projected.Indexes[0].Columns[0].Collation !=
					"BINARY" ||
				projected.Indexes[1].Name !=
					"ix_accounts_exact_count" ||
				!projected.Indexes[1].Columns[0].Descending {
				t.Fatalf("indexes = %#v", projected.Indexes)
			}
			if len(projected.Checks) != 1 ||
				projected.Checks[0].Name != "" ||
				projected.Checks[0].Expression.CanonicalSQL() !=
					`"exact_count" >= 0` {
				t.Fatalf("checks = %#v", projected.Checks)
			}
			if len(projected.Columns) != 10 ||
				projected.Columns[1].DeclaredType.Base !=
					"tinyint" ||
				len(projected.Columns[1].DeclaredType.Arguments) !=
					0 {
				t.Fatalf(
					"TINYINT(1) projection = %#v",
					projected.Columns[1],
				)
			}
			create, err := schema.CreateTable(schema.SQLite, projected)
			if err != nil {
				t.Fatalf("render table: %v", err)
			}
			if !strings.Contains(
				create,
				`"id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT`,
			) ||
				strings.Contains(create, `"enabled" BOOLEAN`) {
				t.Fatalf("SQLite DDL = %s", create)
			}
			if !reflect.DeepEqual(source, before) {
				t.Fatalf(
					"projection mutated source\n got: %#v\nwant: %#v",
					source,
					before,
				)
			}

			projected.Columns[2].DeclaredType.Base = "text"
			projected.Indexes[0].Columns[0].Name = "changed"
			projected.Identity.Column = "changed"
			if source.Columns[2].DeclaredType.Base != "decimal" ||
				source.Indexes[0].Columns[0].Name != "name" ||
				source.Identity.Column != "id" {
				t.Fatal("projected nested metadata aliases source")
			}
		})
	}
}

func TestProjectMySQLTableForSQLiteRejectsUnexpectedShapes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*schema.Table)
		want   string
	}{
		{
			name: "unknown collation",
			mutate: func(table *schema.Table) {
				table.MySQLCollation = "utf8mb4_0900_ai_ci"
			},
			want: "collation",
		},
		{
			name: "reserved table",
			mutate: func(table *schema.Table) {
				table.Name = "sqlite_shadow"
			},
			want: "reserved",
		},
		{
			name: "case-folded columns",
			mutate: func(table *schema.Table) {
				table.Columns[1].Name = "ID"
			},
			want: "case-insensitive",
		},
		{
			name: "primary gap",
			mutate: func(table *schema.Table) {
				table.Identity = nil
				table.Columns[0].PrimaryKeyPosition = 2
			},
			want: "primary-key order",
		},
		{
			name: "inline source index",
			mutate: func(table *schema.Table) {
				table.Indexes[0].Inline = true
			},
			want: "index shape",
		},
		{
			name: "text index without binary collation",
			mutate: func(table *schema.Table) {
				table.Indexes[0].Columns[0].Collation = ""
			},
			want: "text index collation",
		},
		{
			name: "reserved index name",
			mutate: func(table *schema.Table) {
				table.Indexes[0].Name = "sqlite_autoindex_x"
			},
			want: "reserved",
		},
		{
			name: "identity without frontier",
			mutate: func(table *schema.Table) {
				table.Identity.Frontier = nil
			},
			want: "identity",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			table := mysqlSQLiteParentFixture(
				t,
				"utf8mb4_0900_bin",
			)
			test.mutate(&table)
			_, err := projectMySQLTableForSQLite(table)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestProjectMySQLTableForSQLiteRejectsLossyProjectedCheck(
	t *testing.T,
) {
	t.Parallel()

	table := mysqlSQLiteParentFixture(t, "utf8mb4_0900_bin")
	table.Checks[0].Expression = mysqlSQLiteCheck(
		t,
		`"exact_count" < 9007199254740992.1`,
	)
	_, err := projectMySQLTableForSQLite(table)
	if err == nil ||
		!strings.Contains(err.Error(), "projected comparison") {
		t.Fatalf(
			"lossy projected CHECK error = %v, want projected comparison policy",
			err,
		)
	}
	var policy *schema.PolicyError
	if !errors.As(err, &policy) ||
		policy.Target != string(schema.SQLite) {
		t.Fatalf("error = %T %v, want SQLite policy", err, err)
	}
}

func TestValidateMySQLSQLiteTablesPreservesForeignKeyGraph(t *testing.T) {
	t.Parallel()

	parent := mysqlSQLiteParentFixture(t, "utf8mb4_0900_bin")
	child := mysqlSQLiteChildFixture(t, "utf8mb4_0900_bin")
	projectedParent, err := projectMySQLTableForSQLite(parent)
	if err != nil {
		t.Fatalf("project parent: %v", err)
	}
	projectedChild, err := projectMySQLTableForSQLite(child)
	if err != nil {
		t.Fatalf("project child: %v", err)
	}
	if err := validateMySQLSQLiteTables(
		[]schema.Table{parent, child},
		[]schema.Table{projectedParent, projectedChild},
	); err != nil {
		t.Fatalf("validate table set: %v", err)
	}
	if len(projectedChild.ForeignKeys) != 1 ||
		projectedChild.ForeignKeys[0].Name != "" ||
		projectedChild.ForeignKeys[0].Match != "NONE" ||
		projectedChild.ForeignKeys[0].OnDelete != "CASCADE" ||
		projectedChild.ForeignKeys[0].OnUpdate != "RESTRICT" {
		t.Fatalf(
			"projected foreign keys = %#v",
			projectedChild.ForeignKeys,
		)
	}
}

func TestSQLiteTargetPlanTablesUsesMySQLProjection(t *testing.T) {
	t.Parallel()

	source := []schema.Table{
		mysqlSQLiteParentFixture(t, "utf8mb4_0900_bin"),
		mysqlSQLiteChildFixture(t, "utf8mb4_0900_bin"),
	}
	adapter := &sqliteTargetAdapter{}
	planned, err := adapter.PlanTables(
		"mysql",
		source,
		"drop_recreate",
	)
	if err != nil {
		t.Fatalf("plan MySQL-to-SQLite tables: %v", err)
	}
	if adapter.sourceEngine != "mysql" ||
		adapter.sqlServerRoute ||
		len(planned) != len(source) ||
		planned[0].Schema != "" ||
		planned[0].MySQLCollation != "" ||
		planned[1].ForeignKeys[0].Name != "" {
		t.Fatalf(
			"adapter/planned tables = engine %q SQLServer %t %#v",
			adapter.sourceEngine,
			adapter.sqlServerRoute,
			planned,
		)
	}
	if reflect.DeepEqual(planned, source) {
		t.Fatal("MySQL tables passed through without target projection")
	}
}

func TestValidateMySQLSQLiteTablesFailsClosed(t *testing.T) {
	t.Parallel()

	type fixture struct {
		source []schema.Table
		target []schema.Table
	}
	newFixture := func(t *testing.T) fixture {
		parent := mysqlSQLiteParentFixture(
			t,
			"utf8mb4_0900_bin",
		)
		child := mysqlSQLiteChildFixture(
			t,
			"utf8mb4_0900_bin",
		)
		projectedParent, err :=
			projectMySQLTableForSQLite(parent)
		if err != nil {
			t.Fatalf("project parent: %v", err)
		}
		projectedChild, err :=
			projectMySQLTableForSQLite(child)
		if err != nil {
			t.Fatalf("project child: %v", err)
		}
		return fixture{
			source: []schema.Table{parent, child},
			target: []schema.Table{
				projectedParent,
				projectedChild,
			},
		}
	}

	tests := []struct {
		name   string
		mutate func(*fixture)
		want   string
	}{
		{
			name: "parent unselected",
			mutate: func(value *fixture) {
				value.source = value.source[1:]
				value.target = value.target[1:]
			},
			want: "unselected",
		},
		{
			name: "child before parent",
			mutate: func(value *fixture) {
				value.source[0], value.source[1] =
					value.source[1], value.source[0]
				value.target[0], value.target[1] =
					value.target[1], value.target[0]
			},
			want: "plan order",
		},
		{
			name: "parent key not unique",
			mutate: func(value *fixture) {
				value.source[0].Columns[0].PrimaryKey = false
				value.source[0].Columns[0].PrimaryKeyPosition = 0
				value.source[0].Identity = nil
			},
			want: "parent key",
		},
		{
			name: "foreign key type mismatch",
			mutate: func(value *fixture) {
				value.source[1].Columns[1] =
					mysqlSQLiteColumn(
						"account_id",
						"integer",
						"int",
					)
			},
			want: "comparison",
		},
		{
			name: "different source namespace",
			mutate: func(value *fixture) {
				value.source[1].Schema = "other"
			},
			want: "table set order",
		},
		{
			name: "table index global collision",
			mutate: func(value *fixture) {
				value.target[1].Indexes[0].Name = "ACCOUNTS"
			},
			want: "global object names",
		},
		{
			name: "cross-table index collision",
			mutate: func(value *fixture) {
				value.target[1].Indexes[0].Name =
					"UQ_ACCOUNTS_NAME"
			},
			want: "global object names",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := newFixture(t)
			test.mutate(&value)
			err := validateMySQLSQLiteTables(
				value.source,
				value.target,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func mysqlSQLiteParentFixture(
	t *testing.T,
	collation string,
) schema.Table {
	t.Helper()
	frontier := int64(41)
	columns := []schema.Column{
		mysqlSQLiteColumn("id", "bigint", "bigint"),
		mysqlSQLiteColumnWithArguments(
			"enabled", "integer", "tinyint", 1,
		),
		mysqlSQLiteColumnWithDefault(
			t,
			mysqlSQLiteColumnWithArguments(
				"exact_count", "numeric", "decimal", 18, 0,
			),
			"0",
		),
		mysqlSQLiteColumnWithDefault(
			t,
			mysqlSQLiteColumnWithArguments(
				"name", "varchar", "varchar", 80,
			),
			"'guest'",
		),
		mysqlSQLiteColumn("notes", "text", "longtext"),
		mysqlSQLiteColumnWithArguments(
			"fixed_key", "binary", "binary", 4,
		),
		mysqlSQLiteColumn("payload", "blob", "mediumblob"),
		mysqlSQLiteColumnWithArguments(
			"created_at", "datetime", "datetime", 3,
		),
		mysqlSQLiteColumnWithArguments(
			"local_time", "time", "time", 6,
		),
		mysqlSQLiteColumn("created_date", "date", "date"),
	}
	columns[0].PrimaryKey = true
	columns[0].PrimaryKeyPosition = 1
	check := mysqlSQLiteCheck(
		t,
		`"exact_count" >= 0`,
	)
	return schema.Table{
		Schema:         "dmtx",
		Name:           "accounts",
		MySQLCollation: collation,
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: columns,
		Indexes: []schema.Index{
			{
				Name:   "uq_accounts_name",
				Unique: true,
				Columns: []schema.IndexColumn{{
					Name:      "name",
					Collation: "BINARY",
				}},
			},
			{
				Name: "ix_accounts_exact_count",
				Columns: []schema.IndexColumn{{
					Name:       "exact_count",
					Descending: true,
				}},
			},
		},
		Checks: []schema.CheckConstraint{{
			Name:       "ck_accounts_exact_count",
			Expression: check,
		}},
	}
}

func mysqlSQLiteChildFixture(
	t *testing.T,
	collation string,
) schema.Table {
	t.Helper()
	columns := []schema.Column{
		mysqlSQLiteColumn("event_id", "bigint", "bigint"),
		mysqlSQLiteColumn("account_id", "bigint", "bigint"),
		mysqlSQLiteColumnWithArguments(
			"label", "varchar", "varchar", 80,
		),
	}
	columns[0].PrimaryKey = true
	columns[0].PrimaryKeyPosition = 1
	return schema.Table{
		Schema:         "dmtx",
		Name:           "account_events",
		MySQLCollation: collation,
		Columns:        columns,
		Indexes: []schema.Index{{
			Name: "ix_account_events_account",
			Columns: []schema.IndexColumn{{
				Name: "account_id",
			}},
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "fk_account_events_account",
			Columns:           []string{"account_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "RESTRICT",
			OnDelete:          "CASCADE",
			Match:             "NONE",
		}},
	}
}

func mysqlSQLiteColumn(
	name string,
	semantic string,
	base string,
) schema.Column {
	return schema.Column{
		Name: name,
		Type: semantic,
		DeclaredType: &schema.DeclaredType{
			Base: base,
		},
	}
}

func mysqlSQLiteColumnWithArguments(
	name string,
	semantic string,
	base string,
	arguments ...int,
) schema.Column {
	column := mysqlSQLiteColumn(name, semantic, base)
	column.DeclaredType.Arguments = append([]int(nil), arguments...)
	return column
}

func mysqlSQLiteColumnWithDefault(
	t *testing.T,
	column schema.Column,
	value string,
) schema.Column {
	t.Helper()
	expression, err := schema.ParseSQLiteDefault(value)
	if err != nil {
		t.Fatalf("parse fixture default %q: %v", value, err)
	}
	column.Default = expression
	return column
}

func mysqlSQLiteCheck(
	t *testing.T,
	value string,
) schema.Expression {
	t.Helper()
	expression, err := schema.ParseSQLiteCheckExpression(value)
	if err != nil {
		t.Fatalf("parse fixture CHECK %q: %v", value, err)
	}
	return expression
}
