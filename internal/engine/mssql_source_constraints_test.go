package engine

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func sqlServerSourceConstraintTestTable() schema.Table {
	return schema.Table{
		Schema: "dbo",
		Name:   "orders",
		Columns: []schema.Column{
			{
				Name: "tenant_id",
				Type: "bigint",
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name: "order_id",
				Type: "bigint",
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
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
				Name: "status",
				Type: "text",
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
				Name: "external_id",
				Type: "uuid",
				DeclaredType: &schema.DeclaredType{
					Base: "uuid",
				},
			},
			{
				Name: "digest",
				Type: "blob",
				DeclaredType: &schema.DeclaredType{
					Base:      "varbinary",
					Arguments: []int{16},
				},
			},
		},
	}
}

func validSQLServerSourcePrimaryKeyCatalog() sqlServerSourcePrimaryKeyCatalog {
	return sqlServerSourcePrimaryKeyCatalog{
		tableObjectID:        100,
		namespace:            "dbo",
		table:                "orders",
		objectID:             200,
		name:                 "PK_orders",
		typ:                  "PK",
		typeDescription:      "PRIMARY_KEY_CONSTRAINT",
		parentObjectID:       100,
		indexID:              1,
		indexName:            "PK_orders",
		indexType:            1,
		indexTypeDescription: "CLUSTERED",
		unique:               true,
		primary:              true,
		columns: []sqlServerSourcePrimaryKeyColumn{
			{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      1,
				name:          "tenant_id",
			},
			{
				indexColumnID: 2,
				keyOrdinal:    2,
				columnID:      2,
				name:          "order_id",
			},
		},
	}
}

func TestSQLServerSourcePrimaryKeyFromCatalogPreservesOrder(t *testing.T) {
	table := sqlServerSourceConstraintTestTable()
	positions, err := sqlServerSourcePrimaryKeyFromCatalog(
		table,
		validSQLServerSourcePrimaryKeyCatalog(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(
		positions,
		map[string]int{"tenant_id": 1, "order_id": 2},
	) {
		t.Fatalf("primary-key positions = %#v", positions)
	}
}

func TestSQLServerSourcePrimaryKeyFailsClosed(t *testing.T) {
	table := sqlServerSourceConstraintTestTable()
	tests := map[string]func(*sqlServerSourcePrimaryKeyCatalog){
		"descending key": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.columns[0].descending = true
		},
		"included column": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.columns[1].included = true
			value.columns[1].keyOrdinal = 0
		},
		"partition column": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.columns[0].partitionOrdinal = 1
		},
		"columnstore metadata": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.columns[0].columnStoreOrdinal = 1
		},
		"nullable column": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.columns[0].name = "parent_id"
		},
		"text key": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.columns = []sqlServerSourcePrimaryKeyColumn{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      4,
				name:          "status",
				collation: sql.NullString{
					String: "Latin1_General_100_BIN2_UTF8",
					Valid:  true,
				},
			}}
		},
		"UUID key ordering": func(
			value *sqlServerSourcePrimaryKeyCatalog,
		) {
			value.columns = []sqlServerSourcePrimaryKeyColumn{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      6,
				name:          "external_id",
			}}
		},
		"binary key comparison": func(
			value *sqlServerSourcePrimaryKeyCatalog,
		) {
			value.columns = []sqlServerSourcePrimaryKeyColumn{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      7,
				name:          "digest",
			}}
		},
		"non-rowstore": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.indexType = 5
			value.indexTypeDescription = "CLUSTERED COLUMNSTORE"
		},
		"filtered": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.filtered = true
			value.filterDefinition = sql.NullString{
				String: "[tenant_id]>(0)",
				Valid:  true,
			}
		},
		"ignore duplicate": func(value *sqlServerSourcePrimaryKeyCatalog) {
			value.ignoreDuplicateKey = true
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validSQLServerSourcePrimaryKeyCatalog()
			mutate(&value)
			assertSQLServerSourceConstraintPolicy(
				t,
				func() error {
					_, err := sqlServerSourcePrimaryKeyFromCatalog(
						table,
						value,
					)
					return err
				}(),
			)
		})
	}
}

func validSQLServerSourceIndexCatalog() sqlServerSourceIndexCatalog {
	return sqlServerSourceIndexCatalog{
		tableObjectID:   100,
		namespace:       "dbo",
		table:           "orders",
		indexID:         3,
		name:            "UX_orders_amount_tenant",
		typ:             2,
		typeDescription: "NONCLUSTERED",
		unique:          true,
		columns: []sqlServerSourceIndexColumnCatalog{
			{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      5,
				name:          "amount",
			},
			{
				indexColumnID: 2,
				keyOrdinal:    2,
				descending:    true,
				columnID:      1,
				name:          "tenant_id",
			},
		},
	}
}

func TestSQLServerSourceIndexFromCatalogPreservesPortableShape(
	t *testing.T,
) {
	index, err := sqlServerSourceIndexFromCatalog(
		sqlServerSourceConstraintTestTable(),
		validSQLServerSourceIndexCatalog(),
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := schema.Index{
		Name:   "UX_orders_amount_tenant",
		Unique: true,
		Columns: []schema.IndexColumn{
			{Name: "amount"},
			{Name: "tenant_id", Descending: true},
		},
	}
	if !reflect.DeepEqual(index, expected) {
		t.Fatalf("index = %#v, want %#v", index, expected)
	}
}

func TestSQLServerSourceIndexAllowsNullableNonuniqueKey(t *testing.T) {
	catalog := validSQLServerSourceIndexCatalog()
	catalog.unique = false
	catalog.columns = []sqlServerSourceIndexColumnCatalog{{
		indexColumnID: 1,
		keyOrdinal:    1,
		descending:    true,
		columnID:      3,
		name:          "parent_id",
	}}
	index, err := sqlServerSourceIndexFromCatalog(
		sqlServerSourceConstraintTestTable(),
		catalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.Columns) != 1 ||
		index.Columns[0].Name != "parent_id" ||
		!index.Columns[0].Descending {
		t.Fatalf("index = %#v", index)
	}
}

func TestSQLServerSourceIndexFailsClosedOnNonportableShape(t *testing.T) {
	table := sqlServerSourceConstraintTestTable()
	tests := map[string]func(*sqlServerSourceIndexCatalog){
		"filtered": func(value *sqlServerSourceIndexCatalog) {
			value.filtered = true
			value.filterDefinition = sql.NullString{
				String: "[amount]>(0)",
				Valid:  true,
			}
		},
		"included": func(value *sqlServerSourceIndexCatalog) {
			value.columns[1].included = true
			value.columns[1].keyOrdinal = 0
		},
		"disabled": func(value *sqlServerSourceIndexCatalog) {
			value.disabled = true
		},
		"hypothetical": func(value *sqlServerSourceIndexCatalog) {
			value.hypothetical = true
		},
		"columnstore": func(value *sqlServerSourceIndexCatalog) {
			value.typ = 6
			value.typeDescription = "NONCLUSTERED COLUMNSTORE"
		},
		"hash": func(value *sqlServerSourceIndexCatalog) {
			value.typ = 7
			value.typeDescription = "NONCLUSTERED HASH"
		},
		"collation": func(value *sqlServerSourceIndexCatalog) {
			value.columns[0].collation = sql.NullString{
				String: "SQL_Latin1_General_CP1_CI_AS",
				Valid:  true,
			}
		},
		"unexpected nontext collation": func(
			value *sqlServerSourceIndexCatalog,
		) {
			value.columns[0].collation = sql.NullString{
				String: "Latin1_General_100_BIN2",
				Valid:  true,
			}
		},
		"text key": func(value *sqlServerSourceIndexCatalog) {
			value.columns = []sqlServerSourceIndexColumnCatalog{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      4,
				name:          "status",
				collation: sql.NullString{
					String: "Latin1_General_100_BIN2_UTF8",
					Valid:  true,
				},
			}}
		},
		"nullable unique key": func(value *sqlServerSourceIndexCatalog) {
			value.columns = []sqlServerSourceIndexColumnCatalog{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      3,
				name:          "parent_id",
			}}
		},
		"UUID key ordering": func(value *sqlServerSourceIndexCatalog) {
			value.columns = []sqlServerSourceIndexColumnCatalog{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      6,
				name:          "external_id",
			}}
		},
		"binary key comparison": func(
			value *sqlServerSourceIndexCatalog,
		) {
			value.columns = []sqlServerSourceIndexColumnCatalog{{
				indexColumnID: 1,
				keyOrdinal:    1,
				columnID:      7,
				name:          "digest",
			}}
		},
		"duplicate column": func(value *sqlServerSourceIndexCatalog) {
			value.columns[1].name = "amount"
			value.columns[1].columnID = 5
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validSQLServerSourceIndexCatalog()
			mutate(&value)
			assertSQLServerSourceConstraintPolicy(
				t,
				func() error {
					_, err := sqlServerSourceIndexFromCatalog(
						table,
						value,
					)
					return err
				}(),
			)
		})
	}
}

func validSQLServerSourceCheckCatalog() sqlServerSourceCheckCatalog {
	return sqlServerSourceCheckCatalog{
		tableObjectID:   100,
		namespace:       "dbo",
		table:           "orders",
		objectID:        300,
		name:            "CK_orders_amount",
		typ:             "C",
		typeDescription: "CHECK_CONSTRAINT",
		parentObjectID:  100,
		// SQL Server 2022 reports this for ordinary identifier-bearing
		// constraints, including numeric-only expressions.
		usesDatabaseCollation: true,
		definition: sql.NullString{
			String: "([amount]>=(0))",
			Valid:  true,
		},
	}
}

func TestSQLServerSourceCheckFromCatalogParsesPortableDefinition(
	t *testing.T,
) {
	check, err := sqlServerSourceCheckFromCatalog(
		sqlServerSourceConstraintTestTable(),
		validSQLServerSourceCheckCatalog(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if check.Name != "CK_orders_amount" ||
		check.Expression.CanonicalSQL() != `"amount" >= 0` {
		t.Fatalf("CHECK = %#v", check)
	}
}

func TestSQLServerSourceCheckFailsClosedOnUnsafeCatalog(t *testing.T) {
	table := sqlServerSourceConstraintTestTable()
	tests := map[string]func(*sqlServerSourceCheckCatalog){
		"disabled": func(value *sqlServerSourceCheckCatalog) {
			value.disabled = true
		},
		"untrusted": func(value *sqlServerSourceCheckCatalog) {
			value.notTrusted = true
		},
		"not for replication": func(value *sqlServerSourceCheckCatalog) {
			value.notForReplication = true
		},
		"unknown parent column": func(value *sqlServerSourceCheckCatalog) {
			value.parentColumnID = 99
			value.parentColumn = sql.NullString{
				String: "missing",
				Valid:  true,
			}
		},
		"function expression": func(value *sqlServerSourceCheckCatalog) {
			value.definition.String = "([amount] < GETDATE())"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validSQLServerSourceCheckCatalog()
			mutate(&value)
			assertSQLServerSourceConstraintPolicy(
				t,
				func() error {
					_, err := sqlServerSourceCheckFromCatalog(
						table,
						value,
					)
					return err
				}(),
			)
		})
	}
}

func validSQLServerSourceForeignKeyCatalog() sqlServerSourceForeignKeyCatalog {
	return sqlServerSourceForeignKeyCatalog{
		databaseID:                     5,
		tableObjectID:                  100,
		namespace:                      "dbo",
		table:                          "orders",
		objectID:                       400,
		name:                           "FK_orders_parent",
		typ:                            "F",
		typeDescription:                "FOREIGN_KEY_CONSTRAINT",
		parentObjectID:                 100,
		referencedObjectID:             101,
		referencedNamespace:            "dbo",
		referencedTable:                "parents",
		keyIndexID:                     1,
		updateAction:                   0,
		updateActionDescription:        "NO_ACTION",
		deleteAction:                   2,
		deleteActionDescription:        "SET_NULL",
		referencedIndexType:            1,
		referencedIndexTypeDescription: "CLUSTERED",
		referencedIndexUnique:          true,
		columns: []sqlServerSourceForeignKeyColumn{
			{
				position:           1,
				parentObjectID:     100,
				parentColumnID:     1,
				parentColumn:       "tenant_id",
				referencedObjectID: 101,
				referencedColumnID: 1,
				referencedColumn:   "tenant_id",
			},
			{
				position:           2,
				parentObjectID:     100,
				parentColumnID:     3,
				parentColumn:       "parent_id",
				referencedObjectID: 101,
				referencedColumnID: 2,
				referencedColumn:   "parent_id",
			},
		},
	}
}

func TestSQLServerSourceForeignKeyFromCatalogPreservesOrderAndActions(
	t *testing.T,
) {
	foreignKey, err := sqlServerSourceForeignKeyFromCatalog(
		sqlServerSourceConstraintTestTable(),
		validSQLServerSourceForeignKeyCatalog(),
	)
	if err != nil {
		t.Fatal(err)
	}
	expected := schema.ForeignKey{
		Name:              "FK_orders_parent",
		ReferencedTable:   "parents",
		Columns:           []string{"tenant_id", "parent_id"},
		ReferencedColumns: []string{"tenant_id", "parent_id"},
		OnUpdate:          "NO ACTION",
		OnDelete:          "SET NULL",
		Match:             "SIMPLE",
	}
	if !reflect.DeepEqual(foreignKey, expected) {
		t.Fatalf("foreign key = %#v, want %#v", foreignKey, expected)
	}
}

func TestSQLServerSourceForeignKeyFailsClosed(t *testing.T) {
	table := sqlServerSourceConstraintTestTable()
	tests := map[string]func(*sqlServerSourceForeignKeyCatalog){
		"cross schema": func(value *sqlServerSourceForeignKeyCatalog) {
			value.referencedNamespace = "archive"
		},
		"untrusted": func(value *sqlServerSourceForeignKeyCatalog) {
			value.notTrusted = true
		},
		"disabled": func(value *sqlServerSourceForeignKeyCatalog) {
			value.disabled = true
		},
		"not for replication": func(value *sqlServerSourceForeignKeyCatalog) {
			value.notForReplication = true
		},
		"column order": func(value *sqlServerSourceForeignKeyCatalog) {
			value.columns[1].position = 3
		},
		"columnstore referenced key": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.referencedIndexType = 5
			value.referencedIndexTypeDescription =
				"CLUSTERED COLUMNSTORE"
		},
		"nonunique referenced key": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.referencedIndexUnique = false
		},
		"action description mismatch": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.deleteActionDescription = "CASCADE"
		},
		"text foreign key": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.columns = []sqlServerSourceForeignKeyColumn{{
				position:           1,
				parentObjectID:     100,
				parentColumnID:     4,
				parentColumn:       "status",
				referencedObjectID: 101,
				referencedColumnID: 4,
				referencedColumn:   "status",
			}}
		},
		"UUID foreign key": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.columns = []sqlServerSourceForeignKeyColumn{{
				position:           1,
				parentObjectID:     100,
				parentColumnID:     6,
				parentColumn:       "external_id",
				referencedObjectID: 101,
				referencedColumnID: 6,
				referencedColumn:   "external_id",
			}}
		},
		"binary foreign key": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.columns = []sqlServerSourceForeignKeyColumn{{
				position:           1,
				parentObjectID:     100,
				parentColumnID:     7,
				parentColumn:       "digest",
				referencedObjectID: 101,
				referencedColumnID: 7,
				referencedColumn:   "digest",
			}}
		},
		"duplicate local column": func(
			value *sqlServerSourceForeignKeyCatalog,
		) {
			value.columns[1].parentColumn = "tenant_id"
			value.columns[1].parentColumnID = 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validSQLServerSourceForeignKeyCatalog()
			mutate(&value)
			assertSQLServerSourceConstraintPolicy(
				t,
				func() error {
					_, err := sqlServerSourceForeignKeyFromCatalog(
						table,
						value,
					)
					return err
				}(),
			)
		})
	}
}

func TestValidateSQLServerSourceObjectNamesRejectsCaseCollisions(
	t *testing.T,
) {
	table := sqlServerSourceConstraintTestTable()
	table.Indexes = []schema.Index{{Name: "IX_orders_status"}}
	table.Checks = []schema.CheckConstraint{{Name: "ix_ORDERS_status"}}
	assertSQLServerSourceConstraintPolicy(
		t,
		validateSQLServerSourceObjectNames(table),
	)
}

func TestSQLServerSourceForeignKeyActionExactCatalogPairs(t *testing.T) {
	tests := []struct {
		value       int
		description string
		expected    string
	}{
		{0, "NO_ACTION", "NO ACTION"},
		{1, "CASCADE", "CASCADE"},
		{2, "SET_NULL", "SET NULL"},
		{3, "SET_DEFAULT", "SET DEFAULT"},
	}
	for _, test := range tests {
		actual, ok := sqlServerSourceForeignKeyAction(
			test.value,
			test.description,
		)
		if !ok || actual != test.expected {
			t.Fatalf(
				"action (%d, %q) = (%q, %t)",
				test.value,
				test.description,
				actual,
				ok,
			)
		}
	}
	if _, ok := sqlServerSourceForeignKeyAction(1, "NO_ACTION"); ok {
		t.Fatal("mismatched action code and description was accepted")
	}
}

func assertSQLServerSourceConstraintPolicy(t *testing.T, err error) {
	t.Helper()
	var policy *schema.PolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("error = %T %v, want PolicyError", err, err)
	}
	if policy.Target != string(schema.SQLServer) {
		t.Fatalf("policy target = %q", policy.Target)
	}
}
