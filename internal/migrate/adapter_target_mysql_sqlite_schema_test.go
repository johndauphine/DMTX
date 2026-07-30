package migrate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectSQLiteColumnForMySQLExactMatrix(t *testing.T) {
	tests := []struct {
		declaration string
		wantType    string
		wantBase    string
		wantArgs    []int
	}{
		{"INTEGER", "bigint", "bigint", nil},
		{"INT8", "bigint", "bigint", nil},
		{"DOUBLE PRECISION", "double precision", "double", nil},
		{"DECIMAL(18,0)", "numeric", "decimal", []int{18, 0}},
		{"NUMERIC(7)", "numeric", "decimal", []int{7, 0}},
		{"VARCHAR(24)", "varchar", "varchar", []int{24}},
		{"CHAR", "text", "longtext", nil},
		{"TEXT", "text", "longtext", nil},
		{"CLOB", "text", "longtext", nil},
		{"BLOB", "blob", "longblob", nil},
		{"VARBINARY(64)", "varbinary", "varbinary", []int{64}},
		{"BINARY", "blob", "longblob", nil},
		{"BOOLEAN", "integer", "tinyint", []int{1}},
		{"DATE", "date", "date", nil},
		{"DATETIME", "datetime", "datetime", []int{6}},
		{"TIMESTAMP(0)", "datetime", "datetime", []int{0}},
		{"UUID", "uuid", "varchar", []int{36}},
	}
	for _, test := range tests {
		t.Run(test.declaration, func(t *testing.T) {
			source := sqliteMySQLSchemaTestColumn(
				t,
				"value",
				test.declaration,
				true,
			)
			projected, err := projectSQLiteColumnForMySQL(source)
			if err != nil {
				t.Fatal(err)
			}
			if projected.Type != test.wantType ||
				projected.DeclaredType == nil ||
				projected.DeclaredType.Base != test.wantBase ||
				!reflect.DeepEqual(
					projected.DeclaredType.Arguments,
					test.wantArgs,
				) {
				t.Fatalf(
					"projected column = %#v, want type=%q declaration=%s%v",
					projected,
					test.wantType,
					test.wantBase,
					test.wantArgs,
				)
			}
		})
	}
}

func TestProjectSQLiteColumnForMySQLNormalizesDefaults(t *testing.T) {
	tests := []struct {
		declaration string
		source      string
		want        string
	}{
		{"BIGINT", "7", "7"},
		{"DECIMAL(18,0)", "9007199254740993", "9007199254740993"},
		{"VARCHAR(24)", "'guest'", "'guest'"},
		{"TEXT", "'profile'", "'profile'"},
		{"BLOB", "X'DEADBEEF'", "X'deadbeef'"},
		{"BOOLEAN", "TRUE", "1"},
		{"DATE", "'2026-07-30'", "'2026-07-30'"},
		{
			"DATETIME(6)",
			"'2026-07-30 12:34:56.123456'",
			"'2026-07-30 12:34:56.123456'",
		},
		{"DATE", "CURRENT_DATE", "CURRENT_DATE"},
		{"DATETIME(0)", "CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"},
		{"UUID", "'123e4567-e89b-12d3-a456-426614174000'", "'123e4567-e89b-12d3-a456-426614174000'"},
	}
	for _, test := range tests {
		t.Run(test.declaration+"_"+test.source, func(t *testing.T) {
			source := sqliteMySQLSchemaTestColumn(
				t,
				"value",
				test.declaration,
				false,
			)
			expression, err := schema.ParseSQLiteDefault(test.source)
			if err != nil {
				t.Fatal(err)
			}
			source.Default = expression
			projected, err := projectSQLiteColumnForMySQL(source)
			if err != nil {
				t.Fatal(err)
			}
			if projected.Default == nil ||
				projected.Default.CanonicalSQL() != test.want {
				t.Fatalf(
					"default = %#v, want %q",
					projected.Default,
					test.want,
				)
			}
		})
	}
}

func TestProjectSQLiteColumnForMySQLRejectsFractionalCurrentTimestamp(
	t *testing.T,
) {
	source := sqliteMySQLSchemaTestColumn(
		t,
		"created_at",
		"DATETIME(6)",
		false,
	)
	source.Default = sqliteMySQLSchemaDefault(t, "CURRENT_TIMESTAMP")
	if _, err := projectSQLiteColumnForMySQL(source); err == nil ||
		!strings.Contains(err.Error(), "fractional temporal default") {
		t.Fatalf("error = %v, want fractional temporal default policy", err)
	}
}

func TestMySQLTargetPlansSQLiteRelationalObjectsForBothFlavors(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression(
		"exact_count >= 0",
	)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	accounts := schema.Table{
		Name: "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			sqliteMySQLSchemaPrimaryColumn(t, "id", "INTEGER", true, 1),
			sqliteMySQLSchemaTestColumn(t, "code", "VARCHAR(24)", false),
			sqliteMySQLSchemaTestColumn(
				t,
				"exact_count",
				"DECIMAL(18,0)",
				false,
			),
			sqliteMySQLSchemaTestColumn(t, "enabled", "BOOLEAN", false),
		},
		Indexes: []schema.Index{{
			Unique: true,
			Inline: true,
			Columns: []schema.IndexColumn{{
				Name:      "code",
				Collation: "BINARY",
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Expression: check,
		}},
	}
	accounts.Columns[1].Default = sqliteMySQLSchemaDefault(t, "'guest'")
	accounts.Columns[2].Default = sqliteMySQLSchemaDefault(t, "0")
	accounts.Columns[3].Default = sqliteMySQLSchemaDefault(t, "TRUE")

	events := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			sqliteMySQLSchemaPrimaryColumn(t, "tenant_id", "INTEGER", false, 1),
			sqliteMySQLSchemaPrimaryColumn(t, "event_id", "INTEGER", false, 2),
			sqliteMySQLSchemaTestColumn(t, "account_id", "INTEGER", false),
		},
		Indexes: []schema.Index{{
			Name: "dmtx_events_account_id_fkey_idx",
			Columns: []schema.IndexColumn{{
				Name:      "event_id",
				Collation: "BINARY",
			}},
		}},
		ForeignKeys: []schema.ForeignKey{{
			Columns:         []string{"account_id"},
			ReferencedTable: "accounts",
			OnUpdate:        "CASCADE",
			OnDelete:        "NO ACTION",
			Match:           "NONE",
		}},
	}
	source := []schema.Table{accounts, events}
	before := sqliteMySQLSchemaNormalizedClone(source)

	for _, fixture := range []struct {
		name      string
		flavor    engine.MySQLServerFlavor
		collation string
	}{
		{
			name:      "Oracle MySQL 8",
			flavor:    engine.MySQLServerFlavorOracle80,
			collation: "utf8mb4_0900_bin",
		},
		{
			name:      "MariaDB 10.11",
			flavor:    engine.MySQLServerFlavorMariaDB1011,
			collation: "utf8mb4_nopad_bin",
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			adapter := &mysqlTargetAdapter{
				flavor:    fixture.flavor,
				namespace: "target_db",
			}
			planned, err := adapter.PlanTables(
				"sqlite",
				source,
				"drop_recreate",
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(planned) != 2 ||
				planned[0].MySQLCollation != fixture.collation ||
				planned[1].MySQLCollation != fixture.collation {
				t.Fatalf("planned tables = %#v", planned)
			}
			if planned[0].Identity == nil ||
				planned[0].Identity.Frontier == nil ||
				*planned[0].Identity.Frontier != 41 ||
				planned[0].Columns[0].Nullable {
				t.Fatalf(
					"planned identity = %#v column = %#v",
					planned[0].Identity,
					planned[0].Columns[0],
				)
			}
			if len(planned[0].Indexes) != 1 ||
				planned[0].Indexes[0].Name == "" ||
				planned[0].Indexes[0].Inline ||
				len(planned[0].Checks) != 2 {
				t.Fatalf(
					"planned accounts objects = indexes %#v checks %#v",
					planned[0].Indexes,
					planned[0].Checks,
				)
			}
			for _, object := range planned[0].Checks {
				if object.Name == "" {
					t.Fatalf("anonymous planned CHECK = %#v", object)
				}
			}
			if len(planned[1].ForeignKeys) != 1 {
				t.Fatalf(
					"planned foreign keys = %#v",
					planned[1].ForeignKeys,
				)
			}
			foreignKey := planned[1].ForeignKeys[0]
			if foreignKey.Name == "" ||
				!reflect.DeepEqual(
					foreignKey.ReferencedColumns,
					[]string{"id"},
				) {
				t.Fatalf("planned foreign key = %#v", foreignKey)
			}
			if len(planned[1].Indexes) != 2 {
				t.Fatalf(
					"planned FK indexes = %#v",
					planned[1].Indexes,
				)
			}
			names := make(map[string]bool)
			for _, index := range planned[1].Indexes {
				if index.Name == "" ||
					names[strings.ToLower(index.Name)] {
					t.Fatalf(
						"colliding planned index = %#v in %#v",
						index,
						planned[1].Indexes,
					)
				}
				names[strings.ToLower(index.Name)] = true
			}
			if !sqliteMySQLSchemaIndexSupportsColumns(
				planned[1],
				[]string{"account_id"},
			) {
				t.Fatalf(
					"foreign key has no explicit support index: %#v",
					planned[1].Indexes,
				)
			}
			if _, err := schema.PlanMySQLDropRecreateObjects(
				planned,
			); err != nil {
				t.Fatalf("replan materialized objects: %v", err)
			}
		})
	}
	normalizedSource := sqliteMySQLSchemaNormalizedClone(source)
	if !reflect.DeepEqual(normalizedSource, before) {
		for index := range normalizedSource {
			if reflect.DeepEqual(normalizedSource[index], before[index]) {
				continue
			}
			t.Fatalf(
				"SQLite source table %d was mutated:\ngot  %#v\nwant %#v",
				index,
				normalizedSource[index],
				before[index],
			)
		}
		t.Fatal("SQLite source metadata was mutated")
	}
}

func TestProjectSQLiteTableForMySQLFailsClosed(t *testing.T) {
	base := func() schema.Table {
		return schema.Table{
			Name: "events",
			Columns: []schema.Column{
				sqliteMySQLSchemaPrimaryColumn(
					t,
					"id",
					"BIGINT",
					false,
					1,
				),
			},
		}
	}
	tests := []struct {
		name string
		edit func(*schema.Table)
		want string
	}{
		{
			name: "strict",
			edit: func(table *schema.Table) {
				table.SQLiteStrict = true
			},
			want: "STRICT",
		},
		{
			name: "without rowid",
			edit: func(table *schema.Table) {
				table.SQLiteWithoutRowID = true
			},
			want: "WITHOUT ROWID",
		},
		{
			name: "implicit rowid alias",
			edit: func(table *schema.Table) {
				table.Columns[0] = sqliteMySQLSchemaPrimaryColumn(
					t,
					"id",
					"INTEGER",
					true,
					1,
				)
			},
			want: "implicit rowid",
		},
		{
			name: "JSON",
			edit: func(table *schema.Table) {
				table.Columns = append(
					table.Columns,
					sqliteMySQLSchemaTestColumn(t, "payload", "JSON", true),
				)
			},
			want: "json",
		},
		{
			name: "fractional decimal",
			edit: func(table *schema.Table) {
				table.Columns = append(
					table.Columns,
					sqliteMySQLSchemaTestColumn(
						t,
						"amount",
						"DECIMAL(18,2)",
						false,
					),
				)
			},
			want: "numeric modifier",
		},
		{
			name: "nullable primary key",
			edit: func(table *schema.Table) {
				table.Columns[0].Nullable = true
			},
			want: "nullable primary key",
		},
		{
			name: "set default action",
			edit: func(table *schema.Table) {
				table.ForeignKeys = []schema.ForeignKey{{
					Columns:           []string{"id"},
					ReferencedTable:   "events",
					ReferencedColumns: []string{"id"},
					OnDelete:          "SET DEFAULT",
					Match:             "NONE",
				}}
			},
			want: "foreign-key action",
		},
		{
			name: "nonbinary index",
			edit: func(table *schema.Table) {
				table.Indexes = []schema.Index{{
					Name: "events_id_idx",
					Columns: []schema.IndexColumn{{
						Name:      "id",
						Collation: "NOCASE",
					}},
				}}
			},
			want: "index collation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := base()
			test.edit(&table)
			_, err := projectSQLiteTableForMySQL(table)
			if err == nil ||
				!strings.Contains(
					strings.ToLower(err.Error()),
					strings.ToLower(test.want),
				) {
				t.Fatalf(
					"error = %v, want substring %q",
					err,
					test.want,
				)
			}
		})
	}
}

func TestValidateSQLiteMySQLTablesRejectsUnknownShorthandTarget(
	t *testing.T,
) {
	source := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			sqliteMySQLSchemaPrimaryColumn(t, "id", "BIGINT", false, 1),
			sqliteMySQLSchemaTestColumn(t, "account_id", "BIGINT", false),
		},
		ForeignKeys: []schema.ForeignKey{{
			Columns:         []string{"account_id"},
			ReferencedTable: "missing_accounts",
			OnUpdate:        "NO ACTION",
			OnDelete:        "NO ACTION",
			Match:           "NONE",
		}},
	}
	projected, err := projectSQLiteTableForMySQL(source)
	if err != nil {
		t.Fatal(err)
	}
	projected.Schema = "target_db"
	if _, err := validateSQLiteMySQLTables(
		[]schema.Table{source},
		[]schema.Table{projected},
	); err == nil ||
		!strings.Contains(err.Error(), "foreign-key target") {
		t.Fatalf("shorthand target error = %v", err)
	}
}

func sqliteMySQLSchemaTestColumn(
	t *testing.T,
	name string,
	declaration string,
	nullable bool,
) schema.Column {
	t.Helper()
	declared, err := schema.ParseSQLiteDeclaredType(declaration)
	if err != nil {
		t.Fatalf("parse SQLite type %q: %v", declaration, err)
	}
	return schema.Column{
		Name:         name,
		Type:         declared.Base,
		Nullable:     nullable,
		DeclaredType: declared,
	}
}

func sqliteMySQLSchemaPrimaryColumn(
	t *testing.T,
	name string,
	declaration string,
	nullable bool,
	position int,
) schema.Column {
	t.Helper()
	column := sqliteMySQLSchemaTestColumn(
		t,
		name,
		declaration,
		nullable,
	)
	column.PrimaryKey = true
	column.PrimaryKeyPosition = position
	return column
}

func sqliteMySQLSchemaDefault(
	t *testing.T,
	value string,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParseSQLiteDefault(value)
	if err != nil {
		t.Fatalf("parse SQLite default %q: %v", value, err)
	}
	return expression
}

func sqliteMySQLSchemaIndexSupportsColumns(
	table schema.Table,
	columns []string,
) bool {
	for _, index := range table.Indexes {
		if len(index.Columns) < len(columns) {
			continue
		}
		matches := true
		for position := range columns {
			if index.Columns[position].Name != columns[position] ||
				index.Columns[position].Descending {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func sqliteMySQLSchemaNormalizedClone(
	source []schema.Table,
) []schema.Table {
	cloned := make([]schema.Table, len(source))
	for index := range source {
		cloned[index] = cloneMySQLTargetTable(source[index])
		for columnIndex := range cloned[index].Columns {
			declaration := cloned[index].Columns[columnIndex].DeclaredType
			if declaration != nil && len(declaration.Arguments) == 0 {
				declaration.Arguments = nil
			}
		}
		for objectIndex := range cloned[index].Indexes {
			if len(cloned[index].Indexes[objectIndex].Columns) == 0 {
				cloned[index].Indexes[objectIndex].Columns = nil
			}
		}
		for objectIndex := range cloned[index].ForeignKeys {
			foreignKey := &cloned[index].ForeignKeys[objectIndex]
			if len(foreignKey.Columns) == 0 {
				foreignKey.Columns = nil
			}
			if len(foreignKey.ReferencedColumns) == 0 {
				foreignKey.ReferencedColumns = nil
			}
		}
	}
	return cloned
}
