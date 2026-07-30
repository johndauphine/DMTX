package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectSQLiteTableForSQLServerPreservesAdmittedShape(
	t *testing.T,
) {
	frontier := int64(41)
	source := schema.Table{
		Name: "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			sqliteSQLServerTestColumn(
				"id",
				"integer",
				nil,
				true,
				1,
			),
			sqliteSQLServerTestColumn(
				"code",
				"varchar",
				[]int{24},
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"amount",
				"decimal",
				[]int{18, 0},
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"enabled",
				"boolean",
				nil,
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"payload",
				"varbinary",
				[]int{16},
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"occurred_at",
				"datetime",
				[]int{3},
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"event_day",
				"date",
				nil,
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"external_id",
				"uuid",
				nil,
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"note",
				"text",
				nil,
				false,
				0,
			),
			sqliteSQLServerTestColumn(
				"reading",
				"double precision",
				nil,
				false,
				0,
			),
		},
		Indexes: []schema.Index{{
			Name: "accounts_amount_idx",
			Columns: []schema.IndexColumn{{
				Name:       "amount",
				Descending: true,
				Collation:  "BINARY",
			}},
		}},
	}
	source.Columns[0].Nullable = true
	source.Columns[1].Default =
		sqliteSQLServerTestDefault(t, "'guest'")
	source.Columns[2].Default =
		sqliteSQLServerTestDefault(t, "0")
	source.Columns[3].Default =
		sqliteSQLServerTestDefault(t, "TRUE")
	source.Columns[4].Default =
		sqliteSQLServerTestDefault(t, "X'00ff'")
	check, err := schema.ParseSQLiteCheckExpression(
		"amount >= 0 AND enabled IN (TRUE, FALSE)",
	)
	if err != nil {
		t.Fatal(err)
	}
	source.Checks = []schema.CheckConstraint{{Expression: check}}
	before := cloneSQLServerTargetTable(source)

	projected, err := projectSQLiteTableForSQLServer(source)
	if err != nil {
		t.Fatalf("project SQLite table for SQL Server: %v", err)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source metadata mutated:\n got %#v\nwant %#v", source, before)
	}
	if projected.Identity == nil ||
		projected.Identity == source.Identity ||
		projected.Identity.Frontier == source.Identity.Frontier ||
		*projected.Identity.Frontier != 41 ||
		projected.Columns[0].Nullable {
		t.Fatalf("projected identity = %#v / %#v", projected.Identity, projected.Columns[0])
	}
	wantDeclarations := []*schema.DeclaredType{
		{Base: "bigint"},
		{Base: "varchar", Arguments: []int{96}},
		{Base: "decimal", Arguments: []int{18, 0}},
		{Base: "bool"},
		{Base: "varbinary", Arguments: []int{16}},
		{Base: "timestamp", Arguments: []int{3}},
		{Base: "date"},
		{Base: "uuid"},
		{Base: "text"},
		{Base: "double precision"},
	}
	wantTypes := []string{
		"bigint",
		"text",
		"numeric",
		"boolean",
		"blob",
		"datetime",
		"date",
		"uuid",
		"text",
		"double precision",
	}
	for index := range projected.Columns {
		if projected.Columns[index].Type != wantTypes[index] ||
			!reflect.DeepEqual(
				projected.Columns[index].DeclaredType,
				wantDeclarations[index],
			) {
			t.Fatalf(
				"projected column %s = %#v",
				projected.Columns[index].Name,
				projected.Columns[index],
			)
		}
	}
	if len(projected.Indexes) != 1 ||
		projected.Indexes[0].Columns[0].Collation != "" ||
		len(projected.Checks) != 1 {
		t.Fatalf("projected objects = %#v / %#v", projected.Indexes, projected.Checks)
	}
	for _, index := range []int{1, 2, 3, 4} {
		if projected.Columns[index].Default == nil ||
			projected.Columns[index].Default ==
				source.Columns[index].Default {
			t.Fatalf(
				"projected default for %s aliases source",
				projected.Columns[index].Name,
			)
		}
	}

	projected.Schema = "dbo"
	if _, err := schema.CreateSQLServerTable(projected); err != nil {
		t.Fatalf("render projected table: %v", err)
	}
	if _, err := schema.PlanSQLServerDropRecreateObjects(
		[]schema.Table{projected},
	); err != nil {
		t.Fatalf("render projected objects: %v", err)
	}

	projected.Columns[1].DeclaredType.Arguments[0] = 1
	projected.Indexes[0].Columns[0].Name = "id"
	*projected.Identity.Frontier = 99
	if source.Columns[1].DeclaredType.Arguments[0] != 24 ||
		source.Indexes[0].Columns[0].Name != "amount" ||
		*source.Identity.Frontier != 41 {
		t.Fatal("projected SQLite table aliases source metadata")
	}
}

func TestProjectSQLiteColumnForSQLServerAdmittedMatrix(t *testing.T) {
	tests := []struct {
		name      string
		base      string
		arguments []int
		wantType  string
		wantDecl  schema.DeclaredType
	}{
		{
			name:     "integer runtime domain",
			base:     "tinyint",
			wantType: "bigint",
			wantDecl: schema.DeclaredType{Base: "bigint"},
		},
		{
			name:     "float64",
			base:     "float",
			wantType: "double precision",
			wantDecl: schema.DeclaredType{Base: "double precision"},
		},
		{
			name:      "numeric precision defaults scale",
			base:      "numeric",
			arguments: []int{18},
			wantType:  "numeric",
			wantDecl: schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{18, 0},
			},
		},
		{
			name:      "character expands UTF8 bytes",
			base:      "char",
			arguments: []int{20},
			wantType:  "text",
			wantDecl: schema.DeclaredType{
				Base:      "varchar",
				Arguments: []int{80},
			},
		},
		{
			name:     "unbounded character",
			base:     "nvarchar",
			wantType: "text",
			wantDecl: schema.DeclaredType{Base: "text"},
		},
		{
			name:      "fixed binary remains unpadded",
			base:      "binary",
			arguments: []int{32},
			wantType:  "blob",
			wantDecl: schema.DeclaredType{
				Base:      "varbinary",
				Arguments: []int{32},
			},
		},
		{
			name:     "unbounded binary",
			base:     "varbinary",
			wantType: "blob",
			wantDecl: schema.DeclaredType{Base: "blob"},
		},
		{
			name:     "boolean",
			base:     "bool",
			wantType: "boolean",
			wantDecl: schema.DeclaredType{Base: "bool"},
		},
		{
			name:     "date",
			base:     "date",
			wantType: "date",
			wantDecl: schema.DeclaredType{Base: "date"},
		},
		{
			name:     "default timestamp precision",
			base:     "timestamp",
			wantType: "datetime",
			wantDecl: schema.DeclaredType{
				Base:      "timestamp",
				Arguments: []int{6},
			},
		},
		{
			name:      "explicit datetime precision",
			base:      "datetime",
			arguments: []int{0},
			wantType:  "datetime",
			wantDecl: schema.DeclaredType{
				Base:      "timestamp",
				Arguments: []int{0},
			},
		},
		{
			name:     "scalar UUID",
			base:     "uuid",
			wantType: "uuid",
			wantDecl: schema.DeclaredType{Base: "uuid"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := sqliteSQLServerTestColumn(
				"value",
				test.base,
				test.arguments,
				false,
				0,
			)
			target, err := projectSQLiteColumnForSQLServer(source)
			if err != nil {
				t.Fatalf("project %s: %v", test.base, err)
			}
			if target.Type != test.wantType ||
				target.DeclaredType == source.DeclaredType ||
				!reflect.DeepEqual(*target.DeclaredType, test.wantDecl) {
				t.Fatalf("projected column = %#v", target)
			}
		})
	}
}

func TestProjectSQLiteTableForSQLServerFailsClosed(t *testing.T) {
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
			name:      "STRICT",
			operation: "map SQLite STRICT table",
			mutate: func(table *schema.Table) {
				table.SQLiteStrict = true
			},
		},
		{
			name:      "WITHOUT ROWID",
			operation: "map SQLite WITHOUT ROWID table",
			mutate: func(table *schema.Table) {
				table.SQLiteWithoutRowID = true
			},
		},
		{
			name:      "implicit rowid identity",
			operation: "map SQLite implicit rowid identity",
			mutate: func(table *schema.Table) {
				table.Identity = nil
			},
		},
		{
			name:      "missing declaration",
			operation: "map SQLite declared type",
			mutate: func(table *schema.Table) {
				table.Columns[0].DeclaredType = nil
			},
		},
		{
			name:      "mismatched declaration",
			operation: "map SQLite declared type",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "blob"
			},
		},
		{
			name:      "unbounded numeric",
			operation: "map SQLite numeric modifier",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "numeric"
				table.Columns[1].DeclaredType =
					&schema.DeclaredType{Base: "numeric"}
			},
		},
		{
			name:      "fractional numeric",
			operation: "map SQLite numeric modifier",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "decimal"
				table.Columns[1].DeclaredType =
					&schema.DeclaredType{
						Base:      "decimal",
						Arguments: []int{18, 2},
					}
			},
		},
		{
			name:      "numeric beyond exact SQLite integer domain",
			operation: "map SQLite numeric modifier",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "decimal"
				table.Columns[1].DeclaredType =
					&schema.DeclaredType{
						Base:      "decimal",
						Arguments: []int{19, 0},
					}
			},
		},
		{
			name:      "overlong varchar",
			operation: "map SQLite character modifier",
			mutate: func(table *schema.Table) {
				table.Columns[1].DeclaredType.Arguments =
					[]int{2_001}
			},
		},
		{
			name:      "unsupported time",
			operation: "map SQLite type",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "time"
				table.Columns[1].DeclaredType =
					&schema.DeclaredType{Base: "time"}
			},
		},
		{
			name:      "clock default",
			operation: "map SQLite default",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "datetime"
				table.Columns[1].DeclaredType =
					&schema.DeclaredType{Base: "datetime"}
				table.Columns[1].Default =
					sqliteSQLServerTestDefault(
						t,
						"CURRENT_TIMESTAMP",
					)
			},
		},
		{
			name:      "negative frontier",
			operation: "map SQLite AUTOINCREMENT identity",
			mutate: func(table *schema.Table) {
				negative := int64(-1)
				table.Identity.Frontier = &negative
			},
		},
		{
			name:      "RESTRICT action",
			operation: "map SQLite foreign-key action",
			mutate: func(table *schema.Table) {
				table.ForeignKeys = []schema.ForeignKey{{
					Columns:           []string{"value"},
					ReferencedTable:   "parent",
					ReferencedColumns: []string{"value"},
					OnUpdate:          "NO ACTION",
					OnDelete:          "RESTRICT",
					Match:             "NONE",
				}}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := sqliteSQLServerIdentityFixture()
			test.mutate(&table)
			_, err := projectSQLiteTableForSQLServer(table)
			assertSQLiteSQLServerTestPolicy(
				t,
				err,
				test.operation,
			)
		})
	}
}

func TestProjectSQLiteTableForSQLServerRejectsComparisonHazards(
	t *testing.T,
) {
	for _, base := range []string{"text", "uuid", "blob"} {
		t.Run(base, func(t *testing.T) {
			tests := []struct {
				name      string
				operation string
				mutate    func(*schema.Table)
			}{
				{
					name:      "primary key",
					operation: "primary-key comparison",
					mutate: func(table *schema.Table) {
						table.Identity = nil
						table.Columns[0] =
							sqliteSQLServerTestColumn(
								"id",
								"bigint",
								nil,
								false,
								0,
							)
						table.Columns[1].PrimaryKey = true
						table.Columns[1].PrimaryKeyPosition = 1
					},
				},
				{
					name:      "index",
					operation: "index comparison",
					mutate: func(table *schema.Table) {
						table.Indexes = []schema.Index{{
							Name: "records_value_idx",
							Columns: []schema.IndexColumn{{
								Name:      "value",
								Collation: "BINARY",
							}},
						}}
					},
				},
				{
					name:      "foreign key",
					operation: "foreign-key comparison",
					mutate: func(table *schema.Table) {
						table.ForeignKeys =
							[]schema.ForeignKey{{
								Columns: []string{
									"value",
								},
								ReferencedTable: "parent",
								ReferencedColumns: []string{
									"value",
								},
								OnUpdate: "NO ACTION",
								OnDelete: "NO ACTION",
								Match:    "NONE",
							}}
					},
				},
				{
					name:      "CHECK",
					operation: "CHECK comparison",
					mutate: func(table *schema.Table) {
						expression := "value IS NULL"
						if base == "text" {
							expression = "value = 'x'"
						}
						check, err :=
							schema.ParseSQLiteCheckExpression(
								expression,
							)
						if err != nil {
							t.Fatal(err)
						}
						table.Checks =
							[]schema.CheckConstraint{{
								Expression: check,
							}}
					},
				},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					table := sqliteSQLServerIdentityFixture()
					table.Columns[1].Type = base
					table.Columns[1].DeclaredType =
						&schema.DeclaredType{Base: base}
					test.mutate(&table)
					_, err := projectSQLiteTableForSQLServer(
						table,
					)
					assertSQLiteSQLServerTestPolicy(
						t,
						err,
						test.operation,
					)
				})
			}
		})
	}
}

func TestProjectSQLiteTableForSQLServerNormalizesInlineUniqueIndex(
	t *testing.T,
) {
	table := sqliteSQLServerIdentityFixture()
	table.Columns[1].Type = "bigint"
	table.Columns[1].DeclaredType =
		&schema.DeclaredType{Base: "bigint"}
	table.Indexes = []schema.Index{{
		Unique: true,
		Inline: true,
		Columns: []schema.IndexColumn{{
			Name:      "value",
			Collation: "BINARY",
		}},
	}}
	projected, err := projectSQLiteTableForSQLServer(table)
	if err != nil {
		t.Fatalf("project inline unique index: %v", err)
	}
	if len(projected.Indexes) != 1 ||
		projected.Indexes[0].Inline ||
		projected.Indexes[0].Name != "" ||
		projected.Indexes[0].Columns[0].Collation != "" {
		t.Fatalf("projected inline index = %#v", projected.Indexes)
	}
	projected.Schema = "dbo"
	objects, err := schema.PlanSQLServerDropRecreateObjects(
		[]schema.Table{projected},
	)
	if err != nil {
		t.Fatalf("plan inline unique index: %v", err)
	}
	if len(objects) != 1 ||
		objects[0].Kind != schema.SQLServerIndexObject {
		t.Fatalf("planned objects = %#v", objects)
	}

	table.Columns[1].Nullable = true
	_, err = projectSQLiteTableForSQLServer(table)
	assertSQLiteSQLServerTestPolicy(
		t,
		err,
		"map SQLite nullable unique index",
	)
}

func TestProjectSQLiteTableForSQLServerRejectsLossyNumericCheck(
	t *testing.T,
) {
	table := sqliteSQLServerIdentityFixture()
	table.Columns[1].Type = "decimal"
	table.Columns[1].DeclaredType = &schema.DeclaredType{
		Base:      "decimal",
		Arguments: []int{18, 0},
	}
	expression, err := schema.ParseSQLiteCheckExpression(
		`value < 9007199254740992.1`,
	)
	if err != nil {
		t.Fatal(err)
	}
	table.Checks = []schema.CheckConstraint{{
		Expression: expression,
	}}
	_, err = projectSQLiteTableForSQLServer(table)
	assertSQLiteSQLServerTestPolicy(
		t,
		err,
		"CHECK source numeric semantics",
	)
}

func TestValidateSQLiteSQLServerTablesChecksCompleteRelationalSet(
	t *testing.T,
) {
	parent := schema.Table{
		Name: "accounts",
		Columns: []schema.Column{
			sqliteSQLServerTestColumn(
				"id",
				"bigint",
				nil,
				true,
				1,
			),
			sqliteSQLServerTestColumn(
				"balance",
				"decimal",
				[]int{12, 0},
				false,
				0,
			),
		},
	}
	child := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			sqliteSQLServerTestColumn(
				"account_id",
				"bigint",
				nil,
				true,
				2,
			),
			sqliteSQLServerTestColumn(
				"sequence_no",
				"bigint",
				nil,
				true,
				1,
			),
		},
		ForeignKeys: []schema.ForeignKey{{
			Columns:           []string{"account_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "NO ACTION",
			Match:             "NONE",
		}},
	}
	source := []schema.Table{parent, child}
	projected := make([]schema.Table, len(source))
	for index, table := range source {
		value, err := projectSQLiteTableForSQLServer(table)
		if err != nil {
			t.Fatalf("project table %s: %v", table.Name, err)
		}
		value.Schema = "dbo"
		projected[index] = value
	}
	if projected[1].ForeignKeys[0].Match != "SIMPLE" {
		t.Fatalf(
			"projected FK match = %q",
			projected[1].ForeignKeys[0].Match,
		)
	}
	if err := validateSQLiteSQLServerTables(
		source,
		projected,
	); err != nil {
		t.Fatalf("validate relational set: %v", err)
	}
	if projected[1].ForeignKeys[0].Name == "" {
		t.Fatal("whole-set validation did not materialize FK name")
	}

	err := validateSQLiteSQLServerTables(
		source[1:],
		projected[1:],
	)
	var policy *schema.PolicyError
	if err == nil ||
		!errors.As(err, &policy) ||
		!strings.Contains(
			err.Error(),
			"referenced table is not selected",
		) {
		t.Fatalf("missing-parent validation error = %v", err)
	}

	err = validateSQLiteSQLServerTables(
		source,
		projected[:1],
	)
	assertSQLiteSQLServerTestPolicy(
		t,
		err,
		"map SQLite table selection",
	)
}

func TestValidateSQLiteSQLServerTablesMaterializesForeignKeyShorthand(
	t *testing.T,
) {
	parent := sqliteSQLServerIdentityFixture()
	parent.Name = "accounts"
	child := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			sqliteSQLServerTestColumn(
				"id",
				"bigint",
				nil,
				true,
				1,
			),
			sqliteSQLServerTestColumn(
				"account_id",
				"bigint",
				nil,
				false,
				0,
			),
		},
		ForeignKeys: []schema.ForeignKey{{
			Columns:         []string{"account_id"},
			ReferencedTable: "accounts",
			OnUpdate:        "NO ACTION",
			OnDelete:        "NO ACTION",
			Match:           "NONE",
		}},
	}
	source := []schema.Table{parent, child}
	projected := make([]schema.Table, len(source))
	for index, table := range source {
		value, err := projectSQLiteTableForSQLServer(table)
		if err != nil {
			t.Fatalf("project table %s: %v", table.Name, err)
		}
		value.Schema = "dbo"
		projected[index] = value
	}

	if err := validateSQLiteSQLServerTables(
		source,
		projected,
	); err != nil {
		t.Fatalf("validate FK shorthand: %v", err)
	}
	foreignKey := projected[1].ForeignKeys[0]
	if !reflect.DeepEqual(
		foreignKey.ReferencedColumns,
		[]string{"id"},
	) || foreignKey.Name == "" {
		t.Fatalf("materialized FK shorthand = %#v", foreignKey)
	}
	if len(source[1].ForeignKeys[0].ReferencedColumns) != 0 {
		t.Fatalf(
			"source FK shorthand was mutated: %#v",
			source[1].ForeignKeys[0],
		)
	}
}

func TestSQLServerTargetPlanScopesSQLiteWriteBoundaryToSuccessfulPlan(
	t *testing.T,
) {
	adapter := &sqlServerTargetAdapter{namespace: "dbo"}
	planned, err := adapter.PlanTables(
		"sqlite",
		[]schema.Table{sqliteSQLServerIdentityFixture()},
		"drop_recreate",
	)
	if err != nil {
		t.Fatalf("plan SQLite target: %v", err)
	}
	if len(planned) != 1 || adapter.sourceEngine != "sqlite" {
		t.Fatalf(
			"planned tables/source engine = %#v / %q",
			planned,
			adapter.sourceEngine,
		)
	}

	if _, err := adapter.PlanTables(
		"unsupported",
		[]schema.Table{sqliteSQLServerIdentityFixture()},
		"drop_recreate",
	); err == nil {
		t.Fatal("unsupported source engine was admitted")
	}
	if adapter.sourceEngine != "" {
		t.Fatalf(
			"failed plan retained source engine %q",
			adapter.sourceEngine,
		)
	}
}

func sqliteSQLServerIdentityFixture() schema.Table {
	frontier := int64(9)
	return schema.Table{
		Name: "records",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			func() schema.Column {
				column := sqliteSQLServerTestColumn(
					"id",
					"integer",
					nil,
					true,
					1,
				)
				column.Nullable = true
				return column
			}(),
			sqliteSQLServerTestColumn(
				"value",
				"varchar",
				[]int{24},
				false,
				0,
			),
		},
	}
}

func sqliteSQLServerTestColumn(
	name string,
	base string,
	arguments []int,
	primaryKey bool,
	primaryKeyPosition int,
) schema.Column {
	return schema.Column{
		Name:               name,
		Type:               base,
		PrimaryKey:         primaryKey,
		PrimaryKeyPosition: primaryKeyPosition,
		DeclaredType: &schema.DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		},
	}
}

func sqliteSQLServerTestDefault(
	t *testing.T,
	value string,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParseSQLiteDefault(value)
	if err != nil {
		t.Fatalf("ParseSQLiteDefault(%q): %v", value, err)
	}
	return expression
}

func assertSQLiteSQLServerTestPolicy(
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
