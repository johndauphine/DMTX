package schema

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestCreateSQLServerTableRendersExactTypesDefaultsAndIdentity(
	t *testing.T,
) {
	codeDefault, err := ParseSQLiteDefault("'O''Hare'")
	if err != nil {
		t.Fatal(err)
	}
	amountDefault, err := ParseSQLiteDefault("1.25")
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	table := Table{
		Schema: "dbo",
		Name:   "accounts",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name:         "code",
				Type:         "text",
				DeclaredType: &DeclaredType{Base: "varchar", Arguments: []int{12}},
				Default:      codeDefault,
			},
			{
				Name:         "amount",
				Type:         "numeric",
				Nullable:     true,
				DeclaredType: &DeclaredType{Base: "decimal", Arguments: []int{12, 2}},
				Default:      amountDefault,
			},
		},
	}
	got, err := CreateSQLServerTable(table)
	if err != nil {
		t.Fatalf("CreateSQLServerTable: %v", err)
	}
	want := "CREATE TABLE [dbo].[accounts] (" +
		"[id] BIGINT IDENTITY(1,1) NOT NULL, " +
		"[code] VARCHAR(12) COLLATE Latin1_General_100_BIN2_UTF8 " +
		"NOT NULL DEFAULT N'O''Hare', " +
		"[amount] DECIMAL(12,2) NULL DEFAULT 1.25, " +
		"CONSTRAINT [dmtx_accounts_pk] PRIMARY KEY CLUSTERED ([id] ASC));"
	if got != want {
		t.Fatalf("SQL Server table DDL:\n%s\nwant:\n%s", got, want)
	}
	primaryKeyName, err := SQLServerPrimaryKeyConstraintName(table)
	if err != nil {
		t.Fatalf("SQLServerPrimaryKeyConstraintName: %v", err)
	}
	if primaryKeyName != "dmtx_accounts_pk" {
		t.Fatalf("primary-key name = %q", primaryKeyName)
	}
	withoutPrimaryKey := cloneSQLServerTestTable(table)
	withoutPrimaryKey.Identity = nil
	withoutPrimaryKey.Columns[0].PrimaryKey = false
	withoutPrimaryKey.Columns[0].PrimaryKeyPosition = 0
	name, err := SQLServerPrimaryKeyConstraintName(withoutPrimaryKey)
	if err != nil || name != "" {
		t.Fatalf("table without primary key name = %q, error = %v", name, err)
	}
	invalid := cloneSQLServerTestTable(table)
	invalid.Name += " "
	if _, err := SQLServerPrimaryKeyConstraintName(invalid); err == nil {
		t.Fatal("invalid table produced a primary-key constraint name")
	}

	drop, err := DropSQLServerTable(Table{
		Schema: "d]bo",
		Name:   "order]items",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := drop,
		"DROP TABLE IF EXISTS [d]]bo].[order]]items];"; got != want {
		t.Fatalf("drop = %q, want %q", got, want)
	}
}

func TestPlanSQLServerObjectsIsDeterministicAndDependencyOrdered(
	t *testing.T,
) {
	tables := sqlServerTargetFixture(t)
	first, err := PlanSQLServerDropRecreateObjects(tables)
	if err != nil {
		t.Fatalf("PlanSQLServerDropRecreateObjects: %v", err)
	}
	reversed := []Table{tables[1], tables[0]}
	second, err := PlanSQLServerDropRecreateObjects(reversed)
	if err != nil {
		t.Fatalf("reversed plan: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("input order changed plan:\n%#v\n%#v", first, second)
	}
	if len(first) != 4 {
		t.Fatalf("object plan = %#v", first)
	}
	kinds := []SQLServerObjectKind{
		first[0].Kind,
		first[1].Kind,
		first[2].Kind,
		first[3].Kind,
	}
	if !reflect.DeepEqual(kinds, []SQLServerObjectKind{
		SQLServerIndexObject,
		SQLServerIndexObject,
		SQLServerCheckObject,
		SQLServerForeignKeyObject,
	}) {
		t.Fatalf("object kind order = %#v", kinds)
	}
	for _, expected := range []string{
		"CREATE UNIQUE NONCLUSTERED INDEX [accounts_amount_uq] ON [dbo].[accounts] ([amount] ASC);",
		"CREATE NONCLUSTERED INDEX [events_amount_idx] ON [dbo].[events] ([amount] DESC);",
		"ALTER TABLE [dbo].[events] WITH CHECK ADD CONSTRAINT [events_amount_ck] CHECK ([amount] >= 0);",
		"ALTER TABLE [dbo].[events] WITH CHECK ADD CONSTRAINT [events_account_fk] FOREIGN KEY ([account_id]) REFERENCES [dbo].[accounts] ([id]) ON UPDATE NO ACTION ON DELETE CASCADE;",
	} {
		found := false
		for _, statement := range first {
			if statement.SQL == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("plan does not contain %q: %#v", expected, first)
		}
	}
}

func TestRenderPortableCheckForSQLServerUsesStructuralLiterals(
	t *testing.T,
) {
	expression, err := ParseSQLiteCheckExpression(
		`"status" IN ('new', 'O''Hare') AND "enabled" = TRUE`,
	)
	if err != nil {
		t.Fatal(err)
	}
	columns := []Column{
		{
			Name:         "status",
			Type:         "text",
			DeclaredType: &DeclaredType{Base: "varchar", Arguments: []int{20}},
		},
		{
			Name:         "enabled",
			Type:         "boolean",
			DeclaredType: &DeclaredType{Base: "bool"},
		},
	}
	got, err := RenderPortableCheckForSQLServer(expression, columns)
	if err != nil {
		t.Fatalf("RenderPortableCheckForSQLServer: %v", err)
	}
	want := "[status] IN (N'new', N'O''Hare') AND [enabled] = 1"
	if got != want {
		t.Fatalf("rendered CHECK = %q, want %q", got, want)
	}

	tooLarge, err := ParseSQLiteCheckExpression(
		`"amount" < 123456789012345678901234567890123456789`,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = RenderPortableCheckForSQLServer(tooLarge, []Column{{
		Name:         "amount",
		Type:         "numeric",
		DeclaredType: &DeclaredType{Base: "decimal", Arguments: []int{38, 0}},
	}})
	if err == nil ||
		!strings.Contains(err.Error(), "exceeds DECIMAL") {
		t.Fatalf("large numeric CHECK error = %v", err)
	}
}

func TestSQLServerTargetFailsClosedOnInvalidBaseShapes(t *testing.T) {
	valid := Table{
		Schema: "dbo",
		Name:   "items",
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &DeclaredType{Base: "bigint"},
		}},
	}
	tests := []struct {
		name   string
		mutate func(*Table)
	}{
		{
			name: "trailing-space identifier",
			mutate: func(table *Table) {
				table.Name += " "
			},
		},
		{
			name: "case-insensitive duplicate column",
			mutate: func(table *Table) {
				table.Columns = append(table.Columns, Column{
					Name:         "ID",
					Type:         "bigint",
					DeclaredType: &DeclaredType{Base: "bigint"},
				})
			},
		},
		{
			name: "missing declared type",
			mutate: func(table *Table) {
				table.Columns[0].DeclaredType = nil
			},
		},
		{
			name: "unsupported modifier",
			mutate: func(table *Table) {
				table.Columns[0] = Column{
					Name: "id",
					Type: "text",
					DeclaredType: &DeclaredType{
						Base:      "varchar",
						Arguments: []int{8_001},
					},
				}
			},
		},
		{
			name: "identity outside single bigint key",
			mutate: func(table *Table) {
				table.Identity = &Identity{
					Column:     "id",
					Generation: IdentityByDefault,
				}
				table.Columns = append(table.Columns, Column{
					Name:               "tenant_id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 2,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := cloneSQLServerTestTable(valid)
			test.mutate(&table)
			if _, err := CreateSQLServerTable(table); err == nil {
				t.Fatal("invalid table unexpectedly rendered")
			}
		})
	}
}

func TestSQLServerTargetRejectsUnsafeIdentityFrontiers(t *testing.T) {
	base := Table{
		Schema: "dbo",
		Name:   "identity_items",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
		},
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &DeclaredType{Base: "bigint"},
		}},
	}
	for _, frontier := range []int64{-1, math.MaxInt64} {
		table := cloneSQLServerTestTable(base)
		table.Identity.Frontier = &frontier
		if _, err := CreateSQLServerTable(table); err == nil ||
			!strings.Contains(err.Error(), "identity frontier") {
			t.Fatalf("unsafe frontier %d error = %v", frontier, err)
		}
	}
	supported := int64(math.MaxInt64 - 1)
	base.Identity.Frontier = &supported
	if _, err := CreateSQLServerTable(base); err != nil {
		t.Fatalf("supported identity frontier was rejected: %v", err)
	}
}

func TestSQLServerTargetUsesUTF16IdentifierLimitAndRowAdmission(
	t *testing.T,
) {
	astralBoundary := strings.Repeat("😀", 64)
	table := Table{
		Schema: "dbo",
		Name:   astralBoundary,
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &DeclaredType{Base: "bigint"},
		}},
	}
	if _, err := CreateSQLServerTable(table); err != nil {
		t.Fatalf("128-code-unit identifier was rejected: %v", err)
	}
	if units := sqlServerUTF16Length(
		sqlServerPrimaryKeyName(table),
	); units > sqlServerIdentifierMaximumCharacters {
		t.Fatalf("generated primary-key name has %d UTF-16 units", units)
	}
	table.Name += "😀"
	if _, err := CreateSQLServerTable(table); err == nil {
		t.Fatal("130-code-unit identifier was accepted")
	}

	wide := Table{
		Schema: "dbo",
		Name:   "wide_row",
		Columns: []Column{
			{
				Name:         "left_payload",
				Type:         "blob",
				DeclaredType: &DeclaredType{Base: "binary", Arguments: []int{4026}},
			},
			{
				Name:         "right_payload",
				Type:         "blob",
				DeclaredType: &DeclaredType{Base: "binary", Arguments: []int{4026}},
			},
		},
	}
	_, err := CreateSQLServerTable(wide)
	if err == nil || !strings.Contains(err.Error(), "8060-byte") {
		t.Fatalf("oversized row error = %v", err)
	}
}

func TestPlanSQLServerObjectsRejectsNameAndForeignKeyHazards(
	t *testing.T,
) {
	t.Run("descending unique index is a valid FK target", func(t *testing.T) {
		tables := sqlServerTargetFixture(t)
		tables[0].Indexes[0].Columns[0].Descending = true
		tables[1].Columns[1] = tables[1].Columns[2]
		tables[1].Columns[1].Name = "account_amount"
		tables[1].ForeignKeys[0].Columns = []string{"account_amount"}
		tables[1].ForeignKeys[0].ReferencedColumns = []string{"amount"}
		if _, err := PlanSQLServerDropRecreateObjects(tables); err != nil {
			t.Fatalf("descending unique FK target was rejected: %v", err)
		}
	})

	t.Run("schema constraint collision", func(t *testing.T) {
		tables := sqlServerTargetFixture(t)
		tables[0].Checks = []CheckConstraint{{
			Name:       "EVENTS_AMOUNT_CK",
			Expression: tables[1].Checks[0].Expression,
		}}
		_, err := PlanSQLServerDropRecreateObjects(tables)
		if err == nil || !strings.Contains(err.Error(), "collides") {
			t.Fatalf("constraint collision error = %v", err)
		}
	})

	t.Run("generated primary key collides with table", func(t *testing.T) {
		tables := sqlServerTargetFixture(t)
		name, err := SQLServerPrimaryKeyConstraintName(tables[0])
		if err != nil {
			t.Fatal(err)
		}
		tables = append(tables, sqlServerTargetKeyTable(name))
		_, err = PlanSQLServerDropRecreateObjects(tables)
		if err == nil || !strings.Contains(err.Error(), "collides") {
			t.Fatalf("primary-key/table collision error = %v", err)
		}
	})

	for _, objectKind := range []SQLServerObjectKind{
		SQLServerCheckObject,
		SQLServerForeignKeyObject,
	} {
		t.Run("generated object collides with table", func(t *testing.T) {
			tables := sqlServerTargetFixture(t)
			switch objectKind {
			case SQLServerCheckObject:
				tables[1].Checks[0].Name = ""
			case SQLServerForeignKeyObject:
				tables[1].ForeignKeys[0].Name = ""
			}
			objects, err := PlanSQLServerDropRecreateObjects(tables)
			if err != nil {
				t.Fatalf("generate object name: %v", err)
			}
			name := ""
			for _, object := range objects {
				if object.Kind == objectKind {
					name = object.Name
					break
				}
			}
			if name == "" {
				t.Fatalf("generated object kind %d is missing", objectKind)
			}
			tables = append(tables, sqlServerTargetKeyTable(name))
			_, err = PlanSQLServerDropRecreateObjects(tables)
			if err == nil || !strings.Contains(err.Error(), "collides") {
				t.Fatalf("generated object/table collision error = %v", err)
			}
		})
	}

	t.Run("table index collision", func(t *testing.T) {
		tables := sqlServerTargetFixture(t)
		tables[0].Indexes = append(tables[0].Indexes, Index{
			Name:    "ACCOUNTS_AMOUNT_UQ",
			Columns: []IndexColumn{{Name: "id"}},
		})
		_, err := PlanSQLServerDropRecreateObjects(tables)
		if err == nil || !strings.Contains(err.Error(), "collides") {
			t.Fatalf("index collision error = %v", err)
		}
	})

	tests := []struct {
		name   string
		mutate func(*ForeignKey, *[]Table)
	}{
		{
			name: "unsupported action",
			mutate: func(foreignKey *ForeignKey, _ *[]Table) {
				foreignKey.OnDelete = "RESTRICT"
			},
		},
		{
			name: "unsupported match",
			mutate: func(foreignKey *ForeignKey, _ *[]Table) {
				foreignKey.Match = "FULL"
			},
		},
		{
			name: "SET NULL required column",
			mutate: func(foreignKey *ForeignKey, _ *[]Table) {
				foreignKey.OnDelete = "SET NULL"
			},
		},
		{
			name: "SET DEFAULT without default",
			mutate: func(foreignKey *ForeignKey, _ *[]Table) {
				foreignKey.OnDelete = "SET DEFAULT"
			},
		},
		{
			name: "referenced columns not unique",
			mutate: func(foreignKey *ForeignKey, _ *[]Table) {
				foreignKey.ReferencedColumns = []string{"amount"}
			},
		},
		{
			name: "incompatible column types",
			mutate: func(_ *ForeignKey, tables *[]Table) {
				(*tables)[1].Columns[1].DeclaredType =
					&DeclaredType{Base: "int"}
				(*tables)[1].Columns[1].Type = "integer"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tables := sqlServerTargetFixture(t)
			test.mutate(&tables[1].ForeignKeys[0], &tables)
			if _, err := PlanSQLServerDropRecreateObjects(
				tables,
			); err == nil {
				t.Fatal("unsafe foreign key unexpectedly planned")
			}
		})
	}
}

func TestPlanSQLServerCascadeTopology(t *testing.T) {
	t.Run("single cascade tree", func(t *testing.T) {
		tables := []Table{
			sqlServerCascadeTable("root"),
			sqlServerCascadeTable("branch", "root"),
			sqlServerCascadeTable("leaf", "branch"),
		}
		if _, err := PlanSQLServerDropRecreateObjects(tables); err != nil {
			t.Fatalf("single cascade tree was rejected: %v", err)
		}
	})

	for _, event := range []string{"DELETE", "UPDATE"} {
		t.Run(strings.ToLower(event)+" diamond", func(t *testing.T) {
			tables := []Table{
				sqlServerCascadeTable("root"),
				sqlServerCascadeTable("left", "root"),
				sqlServerCascadeTable("right", "root"),
				sqlServerCascadeTable("leaf", "left", "right"),
			}
			if event == "UPDATE" {
				for tableIndex := range tables {
					for foreignKeyIndex := range tables[tableIndex].ForeignKeys {
						foreignKey := &tables[tableIndex].
							ForeignKeys[foreignKeyIndex]
						foreignKey.OnDelete = "NO ACTION"
						foreignKey.OnUpdate = "CASCADE"
					}
				}
			}
			_, err := PlanSQLServerDropRecreateObjects(tables)
			if err == nil ||
				!strings.Contains(err.Error(), "cascade topology") {
				t.Fatalf("%s cascade diamond error = %v", event, err)
			}
		})
	}

	t.Run("cycle", func(t *testing.T) {
		tables := []Table{
			sqlServerCascadeTable("left", "right"),
			sqlServerCascadeTable("right", "left"),
		}
		_, err := PlanSQLServerDropRecreateObjects(tables)
		if err == nil || !strings.Contains(err.Error(), "cascade cycle") {
			t.Fatalf("cascade cycle error = %v", err)
		}
	})
}

func sqlServerTargetFixture(t *testing.T) []Table {
	t.Helper()
	check, err := ParseSQLiteCheckExpression(`"amount" >= 0`)
	if err != nil {
		t.Fatal(err)
	}
	return []Table{
		{
			Schema: "dbo",
			Name:   "accounts",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				},
				{
					Name:         "amount",
					Type:         "numeric",
					DeclaredType: &DeclaredType{Base: "decimal", Arguments: []int{12, 2}},
				},
			},
			Indexes: []Index{{
				Name:    "accounts_amount_uq",
				Unique:  true,
				Columns: []IndexColumn{{Name: "amount"}},
			}},
		},
		{
			Schema: "dbo",
			Name:   "events",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				},
				{
					Name:         "account_id",
					Type:         "bigint",
					DeclaredType: &DeclaredType{Base: "bigint"},
				},
				{
					Name:         "amount",
					Type:         "numeric",
					DeclaredType: &DeclaredType{Base: "decimal", Arguments: []int{12, 2}},
				},
			},
			Indexes: []Index{{
				Name: "events_amount_idx",
				Columns: []IndexColumn{{
					Name:       "amount",
					Descending: true,
				}},
			}},
			Checks: []CheckConstraint{{
				Name:       "events_amount_ck",
				Expression: check,
			}},
			ForeignKeys: []ForeignKey{{
				Name:              "events_account_fk",
				Columns:           []string{"account_id"},
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "NO ACTION",
				OnDelete:          "CASCADE",
				Match:             "SIMPLE",
			}},
		},
	}
}

func sqlServerTargetKeyTable(name string) Table {
	return Table{
		Schema: "dbo",
		Name:   name,
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &DeclaredType{Base: "bigint"},
		}},
	}
}

func sqlServerCascadeTable(name string, parents ...string) Table {
	table := sqlServerTargetKeyTable(name)
	for _, parent := range parents {
		column := parent + "_id"
		table.Columns = append(table.Columns, Column{
			Name:         column,
			Type:         "bigint",
			DeclaredType: &DeclaredType{Base: "bigint"},
		})
		table.ForeignKeys = append(table.ForeignKeys, ForeignKey{
			Name:              name + "_" + parent + "_fk",
			Columns:           []string{column},
			ReferencedTable:   parent,
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "CASCADE",
			Match:             "SIMPLE",
		})
	}
	return table
}

func cloneSQLServerTestTable(source Table) Table {
	cloned := source
	cloned.Columns = append([]Column(nil), source.Columns...)
	for index := range cloned.Columns {
		if source.Columns[index].DeclaredType != nil {
			declaration := *source.Columns[index].DeclaredType
			declaration.Arguments = append(
				[]int(nil),
				declaration.Arguments...,
			)
			cloned.Columns[index].DeclaredType = &declaration
		}
	}
	if source.Identity != nil {
		identity := *source.Identity
		cloned.Identity = &identity
	}
	return cloned
}
