package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/engine"
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

	got, err := projectMySQLTargetTable(
		"postgres",
		source,
		engine.MySQLServerFlavorOracle80,
	)
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
	renderedCheck, err := schema.RenderPortableCheckForMySQL(
		check,
		got.Columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	expectedCheck, err := schema.ParseMySQLCatalogCheck(
		renderedCheck,
		got.Columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Checks[0].Expression, expectedCheck) {
		t.Fatalf(
			"projected MySQL check = %q, want catalog canonical %q",
			got.Checks[0].Expression.CanonicalSQL(),
			expectedCheck.CanonicalSQL(),
		)
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
	got, err := projectMySQLTargetTable(
		"postgres",
		source,
		engine.MySQLServerFlavorOracle80,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got.ForeignKeys[0].Match != "NONE" {
		t.Fatalf("foreign-key match = %q", got.ForeignKeys[0].Match)
	}
}

func TestProjectMySQLTargetTableCanonicalizesPortableChecksForCatalogRecovery(
	t *testing.T,
) {
	portable, err := schema.ParseSQLiteCheckExpression(`code <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	source := schema.Table{
		Schema:         "app",
		Name:           "items",
		MySQLCollation: "utf8mb4_0900_bin",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name:         "code",
				Type:         "varchar",
				Nullable:     true,
				DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16}},
			},
		},
		Checks: []schema.CheckConstraint{{
			Name: "items_code_check", Expression: portable,
		}},
	}
	projected, err := projectMySQLTargetTable(
		"mysql",
		source,
		engine.MySQLServerFlavorOracle80,
	)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := schema.RenderPortableCheckForMySQL(
		portable,
		projected.Columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	want, err := schema.ParseMySQLCatalogCheck(rendered, projected.Columns)
	if err != nil {
		t.Fatal(err)
	}
	if len(projected.Checks) != 1 ||
		!reflect.DeepEqual(projected.Checks[0].Expression, want) {
		t.Fatalf(
			"projected check = %#v, want exact MySQL catalog AST %#v",
			projected.Checks,
			want,
		)
	}
	first := projected.Checks[0].Expression
	if err := canonicalizeMySQLTargetChecks(&projected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projected.Checks[0].Expression, first) {
		t.Fatalf(
			"first repeated MySQL check canonicalization changed the AST: before=%#v after=%#v",
			first,
			projected.Checks[0].Expression,
		)
	}
	if err := canonicalizeMySQLTargetChecks(&projected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projected.Checks[0].Expression, first) {
		t.Fatalf(
			"second repeated MySQL check canonicalization changed the AST: before=%#v after=%#v",
			first,
			projected.Checks[0].Expression,
		)
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
			_, err := projectMySQLTargetTable(
				"postgres",
				schema.Table{
					Name:    "items",
					Columns: []schema.Column{test.column},
				},
				engine.MySQLServerFlavorOracle80,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Target != string(schema.MySQL) {
				t.Fatalf("error = %v, want MySQL policy error", err)
			}
		})
	}
}

func TestProjectSQLServerTableForMySQLPreservesAdmittedShape(
	t *testing.T,
) {
	defaultValue := func(value string) *schema.Expression {
		t.Helper()
		expression, err := schema.ParseSQLiteDefault(value)
		if err != nil {
			t.Fatal(err)
		}
		return expression
	}
	check, err := schema.ParseSQLiteCheckExpression(`amount >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	source := schema.Table{
		Schema: "dbo",
		Name:   "events",
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
				Name:         "priority",
				Type:         "integer",
				DeclaredType: &schema.DeclaredType{Base: "tinyint"},
				Default:      defaultValue("255"),
			},
			{
				Name:         "attempts",
				Type:         "integer",
				DeclaredType: &schema.DeclaredType{Base: "int"},
			},
			{
				Name:         "enabled",
				Type:         "boolean",
				DeclaredType: &schema.DeclaredType{Base: "bool"},
				Default:      defaultValue("TRUE"),
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 3},
				},
				Default: defaultValue("0.000"),
			},
			{
				Name:         "ratio",
				Type:         "real",
				DeclaredType: &schema.DeclaredType{Base: "real"},
			},
			{
				Name: "code",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
				Default: defaultValue("'guest'"),
			},
			{
				Name:         "description",
				Type:         "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
				Nullable:     true,
			},
			{
				Name: "digest",
				Type: "blob",
				DeclaredType: &schema.DeclaredType{
					Base:      "varbinary",
					Arguments: []int{16},
				},
				Default: defaultValue("X'00FF'"),
			},
			{
				Name:         "payload",
				Type:         "blob",
				DeclaredType: &schema.DeclaredType{Base: "blob"},
				Nullable:     true,
			},
			{
				Name:         "observed_on",
				Type:         "date",
				DeclaredType: &schema.DeclaredType{Base: "date"},
			},
			{
				Name: "local_time",
				Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base:      "time",
					Arguments: []int{6},
				},
			},
			{
				Name: "occurred_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{6},
				},
			},
			{
				Name:         "rounded_at",
				Type:         "datetime",
				DeclaredType: &schema.DeclaredType{Base: "smalldatetime"},
			},
			{
				Name:         "external_id",
				Type:         "uuid",
				DeclaredType: &schema.DeclaredType{Base: "uuid"},
			},
			{
				Name:         "account_id",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
		},
		Indexes: []schema.Index{{
			Name: "events_occurred_idx",
			Columns: []schema.IndexColumn{{
				Name:       "occurred_at",
				Descending: true,
			}},
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_account_fk",
			Columns:           []string{"account_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "NO ACTION",
			Match:             "SIMPLE",
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "events_amount_ck",
			Expression: check,
		}},
	}

	for _, fixture := range []struct {
		name      string
		flavor    engine.MySQLServerFlavor
		collation string
	}{
		{
			name:      "Oracle MySQL",
			flavor:    engine.MySQLServerFlavorOracle80,
			collation: "utf8mb4_0900_bin",
		},
		{
			name:      "MariaDB",
			flavor:    engine.MySQLServerFlavorMariaDB1011,
			collation: "utf8mb4_nopad_bin",
		},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			got, err := projectMySQLTargetTable(
				"mssql",
				source,
				fixture.flavor,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got.MySQLCollation != fixture.collation {
				t.Fatalf(
					"target collation = %q, want %q",
					got.MySQLCollation,
					fixture.collation,
				)
			}
			if got.Identity == nil ||
				got.Identity.Frontier == source.Identity.Frontier ||
				*got.Identity.Frontier != 41 {
				t.Fatalf("projected identity = %#v", got.Identity)
			}
			expected := []struct {
				column    int
				typ       string
				base      string
				arguments []int
			}{
				{0, "bigint", "bigint", nil},
				{1, "integer", "smallint", nil},
				{2, "integer", "int", nil},
				{3, "integer", "tinyint", []int{1}},
				{4, "numeric", "decimal", []int{12, 3}},
				{5, "double precision", "double", nil},
				{6, "varchar", "varchar", []int{24}},
				{7, "text", "longtext", nil},
				{8, "varbinary", "varbinary", []int{16}},
				{9, "blob", "longblob", nil},
				{10, "date", "date", nil},
				{11, "time", "time", []int{6}},
				{12, "datetime", "datetime", []int{6}},
				{13, "datetime", "datetime", []int{0}},
				{14, "char", "char", []int{36}},
				{15, "bigint", "bigint", nil},
			}
			for _, want := range expected {
				column := got.Columns[want.column]
				if column.Type != want.typ ||
					column.DeclaredType == nil ||
					column.DeclaredType.Base != want.base ||
					!reflect.DeepEqual(
						column.DeclaredType.Arguments,
						want.arguments,
					) {
					t.Fatalf(
						"projected column %s = %#v",
						column.Name,
						column,
					)
				}
			}
			if got.Columns[1].Default == nil ||
				got.Columns[1].Default.CanonicalSQL() != "255" ||
				got.Columns[3].Default == nil ||
				got.Columns[3].Default.CanonicalSQL() != "1" ||
				got.Columns[8].Default == nil ||
				got.Columns[8].Default.CanonicalSQL() != "X'00ff'" {
				t.Fatalf("projected defaults = %#v", got.Columns)
			}
			if got.ForeignKeys[0].Match != "NONE" {
				t.Fatalf(
					"foreign-key match = %q",
					got.ForeignKeys[0].Match,
				)
			}
			if len(got.Checks) != 2 ||
				!strings.Contains(
					got.Checks[1].Expression.CanonicalSQL(),
					"IN",
				) {
				t.Fatalf("projected checks = %#v", got.Checks)
			}
			if _, err := schema.CreateTable(schema.MySQL, got); err != nil {
				t.Fatalf("render projected table: %v", err)
			}
		})
	}

	if source.MySQLCollation != "" ||
		source.Columns[1].DeclaredType.Base != "tinyint" ||
		source.ForeignKeys[0].Match != "SIMPLE" ||
		*source.Identity.Frontier != 41 {
		t.Fatalf("projection mutated source metadata: %#v", source)
	}
}

func TestProjectSQLServerTableForMySQLFailsClosedOnColumnShapes(
	t *testing.T,
) {
	defaultValue, err := schema.ParseSQLiteDefault("0.1")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		column schema.Column
	}{
		{
			name:   "missing declaration",
			column: schema.Column{Name: "value", Type: "bigint"},
		},
		{
			name: "tinyint neutral type mismatch",
			column: schema.Column{
				Name:         "value",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "tinyint"},
			},
		},
		{
			name: "numeric precision",
			column: schema.Column{
				Name: "value",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{39, 2},
				},
			},
		},
		{
			name: "numeric scale",
			column: schema.Column{
				Name: "value",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{38, 31},
				},
			},
		},
		{
			name: "varchar length",
			column: schema.Column{
				Name: "value",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{8_001},
				},
			},
		},
		{
			name: "temporal precision",
			column: schema.Column{
				Name: "value",
				Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base:      "time",
					Arguments: []int{7},
				},
			},
		},
		{
			name: "REAL default would be re-rounded as DOUBLE",
			column: schema.Column{
				Name:         "value",
				Type:         "real",
				DeclaredType: &schema.DeclaredType{Base: "real"},
				Default:      defaultValue,
			},
		},
		{
			name: "unsupported declaration",
			column: schema.Column{
				Name:         "value",
				Type:         "money",
				DeclaredType: &schema.DeclaredType{Base: "money"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := projectMySQLTargetTable(
				"mssql",
				schema.Table{
					Name:    "items",
					Columns: []schema.Column{test.column},
				},
				engine.MySQLServerFlavorOracle80,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Target != string(schema.MySQL) {
				t.Fatalf(
					"error = %v, want MySQL policy error",
					err,
				)
			}
		})
	}
}

func TestProjectSQLServerTableForMySQLRejectsNonportableObjects(
	t *testing.T,
) {
	textCheck, err := schema.ParseSQLiteCheckExpression(`code <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	baseColumns := func() []schema.Column {
		return []schema.Column{
			{
				Name:         "id",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "code",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
				Nullable: true,
			},
			{
				Name:         "token",
				Type:         "uuid",
				DeclaredType: &schema.DeclaredType{Base: "uuid"},
			},
			{
				Name: "payload",
				Type: "blob",
				DeclaredType: &schema.DeclaredType{
					Base:      "varbinary",
					Arguments: []int{16},
				},
			},
		}
	}
	tests := []struct {
		name  string
		table func() schema.Table
	}{
		{
			name: "text primary key",
			table: func() schema.Table {
				columns := baseColumns()
				columns[1].PrimaryKey = true
				columns[1].PrimaryKeyPosition = 1
				return schema.Table{Name: "items", Columns: columns}
			},
		},
		{
			name: "UUID index",
			table: func() schema.Table {
				return schema.Table{
					Name:    "items",
					Columns: baseColumns(),
					Indexes: []schema.Index{{
						Name: "items_token_idx",
						Columns: []schema.IndexColumn{{
							Name: "token",
						}},
					}},
				}
			},
		},
		{
			name: "nullable unique index",
			table: func() schema.Table {
				columns := baseColumns()
				columns[0].Nullable = true
				return schema.Table{
					Name:    "items",
					Columns: columns,
					Indexes: []schema.Index{{
						Name:   "items_id_uq",
						Unique: true,
						Columns: []schema.IndexColumn{{
							Name: "id",
						}},
					}},
				}
			},
		},
		{
			name: "binary foreign key",
			table: func() schema.Table {
				return schema.Table{
					Name:    "items",
					Columns: baseColumns(),
					ForeignKeys: []schema.ForeignKey{{
						Name:              "items_payload_fk",
						Columns:           []string{"payload"},
						ReferencedTable:   "parents",
						ReferencedColumns: []string{"payload"},
						Match:             "SIMPLE",
					}},
				}
			},
		},
		{
			name: "SET DEFAULT foreign key",
			table: func() schema.Table {
				return schema.Table{
					Name:    "items",
					Columns: baseColumns(),
					ForeignKeys: []schema.ForeignKey{{
						Name:              "items_id_fk",
						Columns:           []string{"id"},
						ReferencedTable:   "parents",
						ReferencedColumns: []string{"id"},
						OnDelete:          "SET DEFAULT",
						Match:             "SIMPLE",
					}},
				}
			},
		},
		{
			name: "text CHECK",
			table: func() schema.Table {
				return schema.Table{
					Name:    "items",
					Columns: baseColumns(),
					Checks: []schema.CheckConstraint{{
						Name:       "items_code_ck",
						Expression: textCheck,
					}},
				}
			},
		},
		{
			name: "foreign-key match",
			table: func() schema.Table {
				return schema.Table{
					Name:    "items",
					Columns: baseColumns(),
					ForeignKeys: []schema.ForeignKey{{
						Name:              "items_id_fk",
						Columns:           []string{"id"},
						ReferencedTable:   "parents",
						ReferencedColumns: []string{"id"},
						Match:             "FULL",
					}},
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := projectMySQLTargetTable(
				"mssql",
				test.table(),
				engine.MySQLServerFlavorOracle80,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				policy.Target != string(schema.MySQL) {
				t.Fatalf(
					"error = %v, want MySQL policy error",
					err,
				)
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
	source.MySQLCollation = "utf8mb4_0900_bin"
	got, err := projectMySQLTargetTable(
		"mysql",
		source,
		engine.MySQLServerFlavorOracle80,
	)
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

func TestProjectMySQLTargetTablePreservesOracleSpatialMetadata(
	t *testing.T,
) {
	srid := uint32(4326)
	source := schema.Table{
		Schema:         "source",
		Name:           "places",
		MySQLCollation: "utf8mb4_0900_bin",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
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
	projected, err := projectMySQLTargetTable(
		"mysql",
		source,
		engine.MySQLServerFlavorOracle80,
	)
	if err != nil {
		t.Fatal(err)
	}
	spatial := projected.Columns[1].DeclaredType.Spatial
	if spatial == nil ||
		spatial.Subtype != schema.SpatialSubtypePoint ||
		spatial.SRID == nil ||
		*spatial.SRID != 4326 {
		t.Fatalf("projected spatial metadata = %#v", spatial)
	}
	*spatial.SRID = 0
	if source.Columns[1].DeclaredType.Spatial.SRID == nil ||
		*source.Columns[1].DeclaredType.Spatial.SRID != 4326 {
		t.Fatal("MySQL spatial projection aliases source SRID metadata")
	}

	if _, err := projectMySQLTargetTable(
		"mysql",
		source,
		engine.MySQLServerFlavorMariaDB1011,
	); err == nil {
		t.Fatal("MariaDB target accepted Oracle MySQL spatial metadata")
	}

	indexed := source
	indexed.Indexes = []schema.Index{{
		Name:    "places_position_idx",
		Columns: []schema.IndexColumn{{Name: "position"}},
	}}
	if _, err := projectMySQLTargetTable(
		"mysql",
		indexed,
		engine.MySQLServerFlavorOracle80,
	); err == nil {
		t.Fatal("unmodeled MySQL spatial index was accepted")
	}
}

func TestProjectMySQLTargetTablePinsFlavorCollation(t *testing.T) {
	mysqlSource := schema.Table{
		Name:           "items",
		MySQLCollation: "utf8mb4_nopad_bin",
		Columns:        []schema.Column{{Name: "id", Type: "bigint"}},
	}
	maria, err := projectMySQLTargetTable(
		"mysql",
		mysqlSource,
		engine.MySQLServerFlavorMariaDB1011,
	)
	if err != nil {
		t.Fatal(err)
	}
	if maria.MySQLCollation != "utf8mb4_nopad_bin" {
		t.Fatalf("MariaDB collation = %q", maria.MySQLCollation)
	}

	postgresSource := schema.Table{
		Name:    "items",
		Columns: []schema.Column{{Name: "id", Type: "bigint"}},
	}
	maria, err = projectMySQLTargetTable(
		"postgres",
		postgresSource,
		engine.MySQLServerFlavorMariaDB1011,
	)
	if err != nil {
		t.Fatal(err)
	}
	if maria.MySQLCollation != "utf8mb4_nopad_bin" {
		t.Fatalf(
			"PostgreSQL-to-MariaDB collation = %q",
			maria.MySQLCollation,
		)
	}
}

func TestProjectMySQLTargetTableRejectsCrossFlavorCollation(t *testing.T) {
	tests := []struct {
		name      string
		collation string
		target    engine.MySQLServerFlavor
	}{
		{
			name:      "MariaDB source into Oracle target",
			collation: "utf8mb4_nopad_bin",
			target:    engine.MySQLServerFlavorOracle80,
		},
		{
			name:      "Oracle source into MariaDB target",
			collation: "utf8mb4_0900_bin",
			target:    engine.MySQLServerFlavorMariaDB1011,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := projectMySQLTargetTable(
				"mysql",
				schema.Table{
					Name:           "items",
					MySQLCollation: test.collation,
					Columns: []schema.Column{{
						Name: "id",
						Type: "bigint",
					}},
				},
				test.target,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want policy error", err)
			}
		})
	}
}

// TestProjectSQLServerNationalTextForMySQL covers the projection this branch
// added, which the live corpus proved and no unit test pinned.
//
// The lengths are the point. nchar and nvarchar declare UTF-16 code units,
// which discovery has already converted to characters, and MySQL's modifier is
// characters - so the number passes straight through. Multiplying it, as the
// SQL Server target correctly must going the other way, would declare four
// times what the source can hold. That is the defect this whole area was
// rewritten after, pointing in the opposite direction.
func TestProjectSQLServerNationalTextForMySQL(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		base     string
		argument []int
		wantType string
		wantBase string
		wantArgs []int
		refused  bool
	}{
		{
			name: "nvarchar keeps its character length",
			base: "nvarchar", argument: []int{40},
			wantType: "varchar", wantBase: "varchar", wantArgs: []int{40},
		},
		{
			name: "nchar likewise",
			base: "nchar", argument: []int{10},
			wantType: "varchar", wantBase: "varchar", wantArgs: []int{10},
		},
		{
			name: "nvarchar at its ceiling",
			base: "nvarchar", argument: []int{4_000},
			wantType: "varchar", wantBase: "varchar", wantArgs: []int{4_000},
		},
		{
			// The ceiling is the SOURCE family's, not one constant: SQL Server
			// cannot declare an nvarchar past 4000, so accepting one would be
			// accepting a column that cannot exist.
			name: "nvarchar past its ceiling is refused",
			base: "nvarchar", argument: []int{4_001},
			refused: true,
		},
		{
			// And the same number is legal for the narrow family, which is why
			// the ceiling cannot be one constant.
			name: "varchar at the same length is fine",
			base: "varchar", argument: []int{4_001},
			wantType: "varchar", wantBase: "varchar", wantArgs: []int{4_001},
		},
		{
			name: "varchar past its own ceiling is refused",
			base: "varchar", argument: []int{8_001},
			refused: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			column := schema.Column{
				Name: "c",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      testCase.base,
					Arguments: testCase.argument,
				},
			}
			projected, err := projectSQLServerColumnForMySQL(column)
			if testCase.refused {
				if err == nil {
					t.Fatalf("%s(%v) was accepted as %+v",
						testCase.base, testCase.argument, projected.DeclaredType)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s(%v) was refused: %v", testCase.base, testCase.argument, err)
			}
			if projected.Type != testCase.wantType {
				t.Errorf("type = %q, want %q", projected.Type, testCase.wantType)
			}
			if projected.DeclaredType == nil ||
				projected.DeclaredType.Base != testCase.wantBase {
				t.Fatalf("declared = %+v, want base %q",
					projected.DeclaredType, testCase.wantBase)
			}
			if len(projected.DeclaredType.Arguments) != len(testCase.wantArgs) {
				t.Fatalf("arguments = %v, want %v",
					projected.DeclaredType.Arguments, testCase.wantArgs)
			}
			for index, want := range testCase.wantArgs {
				if projected.DeclaredType.Arguments[index] != want {
					t.Errorf("arguments = %v, want %v - a byte count would give %d",
						projected.DeclaredType.Arguments, testCase.wantArgs, want*4)
				}
			}
		})
	}
}

// TestProjectSQLServerUnboundedNationalTextForMySQL pins nvarchar(max).
//
// Discovery records it as an unbounded text rather than a national one, so the
// projection never sees the national spelling - and the target is LONGTEXT,
// which is what let the corpus's AboutMe column arrive intact.
func TestProjectSQLServerUnboundedNationalTextForMySQL(t *testing.T) {
	column := schema.Column{
		Name:         "AboutMe",
		Type:         "text",
		DeclaredType: &schema.DeclaredType{Base: "text"},
	}
	projected, err := projectSQLServerColumnForMySQL(column)
	if err != nil {
		t.Fatalf("unbounded text was refused: %v", err)
	}
	if projected.DeclaredType == nil || projected.DeclaredType.Base != "longtext" {
		t.Fatalf("declared = %+v, want longtext", projected.DeclaredType)
	}
}
