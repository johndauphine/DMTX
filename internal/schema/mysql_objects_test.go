package schema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestPlanMySQLDropRecreateObjectsExactDDLAndOrder(t *testing.T) {
	check, err := ParseSQLiteCheckExpression(
		`balance >= 0 AND code <> ''`,
	)
	if err != nil {
		t.Fatal(err)
	}
	tables := []Table{
		{
			Schema: "target",
			Name:   "events",
			Columns: []Column{
				{
					Name:               "tenant_id",
					Type:               "integer",
					PrimaryKeyPosition: 1,
					DeclaredType:       &DeclaredType{Base: "int"},
				},
				{
					Name:               "event_id",
					Type:               "bigint",
					PrimaryKeyPosition: 2,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				},
				{
					Name:         "account_id",
					Type:         "bigint",
					DeclaredType: &DeclaredType{Base: "bigint"},
				},
			},
			ForeignKeys: []ForeignKey{{
				Name:              "events_account_fk",
				Columns:           []string{"account_id"},
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
				OnDelete:          "RESTRICT",
				Match:             "SIMPLE",
			}},
		},
		{
			Schema: "target",
			Name:   "accounts",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKeyPosition: 1,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				},
				{
					Name: "code",
					Type: "varchar",
					DeclaredType: &DeclaredType{
						Base:      "varchar",
						Arguments: []int{24},
					},
				},
				{
					Name: "balance",
					Type: "numeric",
					DeclaredType: &DeclaredType{
						Base:      "decimal",
						Arguments: []int{12, 2},
					},
				},
			},
			Indexes: []Index{{
				Name:   "accounts_code_uq",
				Unique: true,
				Columns: []IndexColumn{{
					Name:      "code",
					Collation: "BINARY",
				}},
			}},
			Checks: []CheckConstraint{{
				Name:       "accounts_balance_check",
				Expression: check,
			}},
		},
	}

	got, err := PlanMySQLDropRecreateObjects(tables)
	if err != nil {
		t.Fatal(err)
	}
	want := []MySQLObjectStatement{
		{
			Kind:   MySQLIndexObject,
			Schema: "target",
			Table:  "accounts",
			Name:   "accounts_code_uq",
			SQL: "CREATE UNIQUE INDEX `accounts_code_uq` ON " +
				"`target`.`accounts` (`code` ASC);",
		},
		{
			Kind:   MySQLCheckObject,
			Schema: "target",
			Table:  "accounts",
			Name:   "accounts_balance_check",
			SQL: "ALTER TABLE `target`.`accounts` ADD CONSTRAINT " +
				"`accounts_balance_check` CHECK " +
				"(`balance` >= 0 AND `code` <> '');",
		},
		{
			Kind:   MySQLForeignKeyObject,
			Schema: "target",
			Table:  "events",
			Name:   "events_account_fk",
			SQL: "ALTER TABLE `target`.`events` ADD CONSTRAINT " +
				"`events_account_fk` FOREIGN KEY (`account_id`) " +
				"REFERENCES `target`.`accounts` (`id`) " +
				"ON UPDATE CASCADE ON DELETE RESTRICT;",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MySQL object plan:\n got: %#v\nwant: %#v", got, want)
	}

	reversed := []Table{tables[1], tables[0]}
	for attempt := 0; attempt < 20; attempt++ {
		again, err := PlanMySQLDropRecreateObjects(reversed)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(again, want) {
			t.Fatalf("attempt %d was not deterministic: %#v", attempt, again)
		}
	}
}

func TestPlanMySQLObjectsGeneratesBoundedCollisionSafeNames(t *testing.T) {
	check, err := ParseSQLiteCheckExpression(`enabled IN (0, 1)`)
	if err != nil {
		t.Fatal(err)
	}
	table := Table{
		Schema: "target",
		Name:   strings.Repeat("x", 64),
		Columns: []Column{{
			Name: "enabled",
			Type: "integer",
			DeclaredType: &DeclaredType{
				Base:      "tinyint",
				Arguments: []int{1},
			},
		}},
		Checks: []CheckConstraint{
			{Expression: check},
			{Expression: check},
		},
	}
	first, err := PlanMySQLDropRecreateObjects([]Table{table})
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanMySQLDropRecreateObjects([]Table{table})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || !reflect.DeepEqual(first, second) ||
		first[0].Name == first[1].Name {
		t.Fatalf("generated names are not deterministic: %#v", first)
	}
	for _, statement := range first {
		if len([]rune(statement.Name)) > mysqlIdentifierMaximumCharacters {
			t.Fatalf("generated name is too long: %q", statement.Name)
		}
	}
}

func TestAddMySQLForeignKeyIndexesMakesImplicitShapeExplicit(t *testing.T) {
	table := Table{
		Schema: "target",
		Name:   "events",
		Columns: []Column{
			{
				Name: "account_id",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
			},
			{
				Name:         "sequence_no",
				Type:         "integer",
				DeclaredType: &DeclaredType{Base: "int"},
			},
		},
		Indexes: []Index{{
			Name: "events_account_fk",
			Columns: []IndexColumn{{
				Name: "sequence_no",
			}},
		}},
		ForeignKeys: []ForeignKey{{
			Name:              "events_account_fk",
			Columns:           []string{"account_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
		}},
	}
	got, err := AddMySQLForeignKeyIndexes(table)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Indexes) != 2 {
		t.Fatalf("indexes = %#v, want explicit and FK support", got.Indexes)
	}
	support := got.Indexes[1]
	if support.Name == "" ||
		strings.EqualFold(support.Name, "events_account_fk") ||
		!reflect.DeepEqual(
			support.Columns,
			[]IndexColumn{{
				Name:      "account_id",
				Collation: "BINARY",
			}},
		) {
		t.Fatalf("supporting index = %#v", support)
	}

	parent := Table{
		Schema: "target",
		Name:   "accounts",
		Columns: []Column{{
			Name:               "id",
			Type:               "varchar",
			PrimaryKeyPosition: 1,
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{24},
			},
		}},
	}
	if _, err := PlanMySQLDropRecreateObjects([]Table{parent, got}); err != nil {
		t.Fatalf("explicit index/FK name collision was not resolved: %v", err)
	}
}

func TestPlanMySQLObjectsFailsClosed(t *testing.T) {
	base := func() []Table {
		return []Table{
			{
				Schema: "target",
				Name:   "parent",
				Columns: []Column{{
					Name:               "id",
					Type:               "bigint",
					PrimaryKeyPosition: 1,
					DeclaredType:       &DeclaredType{Base: "bigint"},
				}},
			},
			{
				Schema: "target",
				Name:   "child",
				Columns: []Column{{
					Name:         "parent_id",
					Type:         "bigint",
					DeclaredType: &DeclaredType{Base: "bigint"},
				}},
				ForeignKeys: []ForeignKey{{
					Name:              "child_parent_fk",
					Columns:           []string{"parent_id"},
					ReferencedTable:   "parent",
					ReferencedColumns: []string{"id"},
					OnDelete:          "RESTRICT",
				}},
			},
		}
	}
	tests := map[string]func([]Table){
		"unknown index column": func(tables []Table) {
			tables[1].Indexes = []Index{{
				Name:    "bad",
				Columns: []IndexColumn{{Name: "missing"}},
			}}
		},
		"nonbinary index collation": func(tables []Table) {
			tables[1].Indexes = []Index{{
				Name: "bad",
				Columns: []IndexColumn{{
					Name:      "parent_id",
					Collation: "NOCASE",
				}},
			}}
		},
		"incompatible foreign key": func(tables []Table) {
			tables[1].Columns[0].DeclaredType.Base = "int"
		},
		"unsupported action": func(tables []Table) {
			tables[1].ForeignKeys[0].OnDelete = "SET DEFAULT"
		},
		"SET NULL nonnullable": func(tables []Table) {
			tables[1].ForeignKeys[0].OnDelete = "SET NULL"
		},
		"ON UPDATE SET NULL nonnullable": func(tables []Table) {
			tables[1].ForeignKeys[0].OnUpdate = "SET NULL"
		},
		"unsupported match": func(tables []Table) {
			tables[1].ForeignKeys[0].Match = "FULL"
		},
		"unknown referenced table": func(tables []Table) {
			tables[1].ForeignKeys[0].ReferencedTable = "missing"
		},
		"trailing-space table identifier": func(tables []Table) {
			tables[1].Name += " "
		},
		"trailing-space column identifier": func(tables []Table) {
			tables[1].Columns[0].Name += " "
		},
		"supplementary-plane index identifier": func(tables []Table) {
			tables[1].Indexes = []Index{{
				Name:    "parent_\U0001F600",
				Columns: []IndexColumn{{Name: "parent_id"}},
			}}
		},
		"AUTO_INCREMENT CHECK": func(tables []Table) {
			tables[1].Columns[0].PrimaryKeyPosition = 1
			tables[1].Identity = &Identity{
				Column:     "parent_id",
				Generation: IdentityByDefault,
			}
			expression, err := ParseSQLiteCheckExpression("parent_id > 0")
			if err != nil {
				t.Fatal(err)
			}
			tables[1].Checks = []CheckConstraint{{
				Name:       "identity_check",
				Expression: expression,
			}}
		},
		"referential action CHECK conflict": func(tables []Table) {
			tables[1].ForeignKeys[0].OnUpdate = "CASCADE"
			expression, err := ParseSQLiteCheckExpression("parent_id > 0")
			if err != nil {
				t.Fatal(err)
			}
			tables[1].Checks = []CheckConstraint{{
				Name:       "parent_check",
				Expression: expression,
			}}
		},
		"same-column self reference": func(tables []Table) {
			tables[1].ForeignKeys[0].ReferencedTable = "child"
			tables[1].ForeignKeys[0].ReferencedColumns =
				[]string{"parent_id"}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			tables := base()
			mutate(tables)
			_, err := PlanMySQLDropRecreateObjects(tables)
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestRenderPortableCheckForMySQLRejectsLossyNumericLiterals(
	t *testing.T,
) {
	columns := []Column{{
		Name: "amount",
		Type: "numeric",
		DeclaredType: &DeclaredType{
			Base:      "decimal",
			Arguments: []int{65, 30},
		},
	}}
	tests := []struct {
		name       string
		literal    string
		wantPolicy bool
	}{
		{
			name:    "exact precision boundary",
			literal: strings.Repeat("9", 65),
		},
		{
			name:       "precision overflow",
			literal:    strings.Repeat("9", 66),
			wantPolicy: true,
		},
		{
			name:       "integer magnitude overflow",
			literal:    "1" + strings.Repeat("0", 99),
			wantPolicy: true,
		},
		{
			name:       "scale overflow",
			literal:    "0." + strings.Repeat("0", 30) + "1",
			wantPolicy: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expression, err := ParseSQLiteCheckExpression(
				"amount <= " + test.literal,
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = RenderPortableCheckForMySQL(expression, columns)
			var policy *PolicyError
			if test.wantPolicy {
				if !errors.As(err, &policy) {
					t.Fatalf("error = %v, want PolicyError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("boundary literal was rejected: %v", err)
			}
		})
	}
}

func TestPlanMySQLIndexesRejectsShapesThatNeedPrefixesOrExceedLimits(
	t *testing.T,
) {
	tests := []struct {
		name    string
		columns []Column
		index   Index
	}{
		{
			name: "large object",
			columns: []Column{{
				Name:         "payload",
				Type:         "blob",
				DeclaredType: &DeclaredType{Base: "longblob"},
			}},
			index: Index{
				Name:    "payload_idx",
				Columns: []IndexColumn{{Name: "payload"}},
			},
		},
		{
			name: "utf8mb4 key bytes",
			columns: []Column{{
				Name: "label",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{769},
				},
			}},
			index: Index{
				Name: "label_idx",
				Columns: []IndexColumn{{
					Name:      "label",
					Collation: "BINARY",
				}},
			},
		},
		{
			name: "fractional timestamp key bytes",
			columns: []Column{
				{
					Name: "label",
					Type: "varchar",
					DeclaredType: &DeclaredType{
						Base:      "varchar",
						Arguments: []int{767},
					},
				},
				{
					Name: "occurred_at",
					Type: "timestamp",
					DeclaredType: &DeclaredType{
						Base:      "timestamp",
						Arguments: []int{6},
					},
				},
			},
			index: Index{
				Name: "label_time_idx",
				Columns: []IndexColumn{
					{Name: "label", Collation: "BINARY"},
					{Name: "occurred_at"},
				},
			},
		},
		{
			name: "too many key parts",
			columns: func() []Column {
				result := make([]Column, 17)
				for index := range result {
					result[index] = Column{
						Name: "c" + string(rune('a'+index)),
						Type: "integer",
						DeclaredType: &DeclaredType{
							Base: "int",
						},
					}
				}
				return result
			}(),
			index: func() Index {
				result := Index{Name: "wide_idx"}
				for index := 0; index < 17; index++ {
					result.Columns = append(
						result.Columns,
						IndexColumn{
							Name: "c" + string(rune('a'+index)),
						},
					)
				}
				return result
			}(),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PlanMySQLDropRecreateObjects([]Table{{
				Name:    "items",
				Columns: test.columns,
				Indexes: []Index{test.index},
			}})
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestPlanMySQLIndexAllowsExactTimestampZeroKeyBoundary(t *testing.T) {
	table := Table{
		Name: "items",
		Columns: []Column{
			{
				Name: "label",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{767},
				},
			},
			{
				Name: "occurred_at",
				Type: "timestamp",
				DeclaredType: &DeclaredType{
					Base: "timestamp",
				},
			},
		},
		Indexes: []Index{{
			Name: "label_time_idx",
			Columns: []IndexColumn{
				{Name: "label", Collation: "BINARY"},
				{Name: "occurred_at"},
			},
		}},
	}
	if _, err := PlanMySQLDropRecreateObjects([]Table{table}); err != nil {
		t.Fatalf("exact 3072-byte key boundary: %v", err)
	}
}
