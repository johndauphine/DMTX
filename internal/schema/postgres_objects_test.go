package schema

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"unicode/utf8"
)

type postgresObjectStatementView struct {
	Kind   PostgresObjectKind
	Schema string
	Table  string
	Name   string
	SQL    string
}

func viewPostgresObjectStatements(
	statements []PostgresObjectStatement,
) []postgresObjectStatementView {
	views := make([]postgresObjectStatementView, len(statements))
	for index, statement := range statements {
		views[index] = postgresObjectStatementView{
			Kind:   statement.Kind(),
			Schema: statement.Schema(),
			Table:  statement.Table(),
			Name:   statement.Name(),
			SQL:    statement.SQL(),
		}
	}
	return views
}

func TestPlanPostgresDropRecreateObjectsExactDDLAndGlobalOrder(
	t *testing.T,
) {
	t.Parallel()
	check := mustPostgresObjectCheck(t, `seq >= 0`)
	tables := []Table{
		{
			Schema: "source",
			Name:   "events",
			Columns: []Column{
				{Name: "id", Type: "integer", PrimaryKey: true},
				{Name: "parent_tenant", Type: "integer"},
				{Name: "parent_id", Type: "integer"},
				{Name: "code", Type: "text"},
				{Name: "label", Type: "text", Nullable: true},
				{Name: "seq", Type: "integer"},
			},
			Indexes: []Index{
				{
					Name: "events_by_label",
					Columns: []IndexColumn{
						{
							Name:       "label",
							Descending: true,
							Collation:  "BINARY",
						},
						{Name: "seq", Collation: "BINARY"},
					},
				},
				{
					Unique: true,
					Inline: true,
					Columns: []IndexColumn{{
						Name:      "code",
						Collation: "BINARY",
					}},
				},
			},
			Checks: []CheckConstraint{{
				Name:       "positive",
				Expression: check,
			}},
			ForeignKeys: []ForeignKey{{
				Columns:         []string{"parent_tenant", "parent_id"},
				ReferencedTable: "parents",
				OnUpdate:        "cascade",
				OnDelete:        "set null",
				Match:           "full",
			}},
		},
		{
			Schema: "source",
			Name:   "parents",
			Columns: []Column{
				{
					Name:               "tenant_id",
					Type:               "integer",
					PrimaryKeyPosition: 1,
				},
				{
					Name:               "id",
					Type:               "integer",
					PrimaryKeyPosition: 2,
				},
				{Name: "code", Type: "text"},
			},
			Indexes: []Index{{
				Name:   "shared",
				Unique: true,
				Columns: []IndexColumn{{
					Name:      "code",
					Collation: "binary",
				}},
			}},
		},
	}
	options := PostgresObjectPlanOptions{
		MapNamespace: func(source string) (string, error) {
			if source != "source" {
				return "", fmt.Errorf("unexpected source namespace")
			}
			return `tenant "west"`, nil
		},
	}

	got, err := PlanPostgresDropRecreateObjects(tables, options)
	if err != nil {
		t.Fatal(err)
	}
	want := []postgresObjectStatementView{
		{
			Kind:   PostgresIndexObject,
			Schema: `tenant "west"`,
			Table:  "events",
			Name:   "dmtx_events_code_key",
			SQL: `CREATE UNIQUE INDEX "dmtx_events_code_key" ON ` +
				`"tenant ""west"""."events" ` +
				`("code" COLLATE "pg_catalog"."C" ASC NULLS FIRST);`,
		},
		{
			Kind:   PostgresIndexObject,
			Schema: `tenant "west"`,
			Table:  "events",
			Name:   "events_by_label",
			SQL: `CREATE INDEX "events_by_label" ON ` +
				`"tenant ""west"""."events" ` +
				`("label" COLLATE "pg_catalog"."C" DESC NULLS LAST, ` +
				`"seq" ASC NULLS FIRST);`,
		},
		{
			Kind:   PostgresIndexObject,
			Schema: `tenant "west"`,
			Table:  "parents",
			Name:   "shared",
			SQL: `CREATE UNIQUE INDEX "shared" ON ` +
				`"tenant ""west"""."parents" ` +
				`("code" COLLATE "pg_catalog"."C" ASC NULLS FIRST);`,
		},
		{
			Kind:   PostgresCheckObject,
			Schema: `tenant "west"`,
			Table:  "events",
			Name:   "positive",
			SQL: `ALTER TABLE "tenant ""west"""."events" ` +
				`ADD CONSTRAINT "positive" CHECK ("seq" >= 0);`,
		},
		{
			Kind:   PostgresForeignKeyObject,
			Schema: `tenant "west"`,
			Table:  "events",
			Name:   "dmtx_events_parent_tenant_parent_id_fkey",
			SQL: `ALTER TABLE "tenant ""west"""."events" ` +
				`ADD CONSTRAINT "dmtx_events_parent_tenant_parent_id_fkey" ` +
				`FOREIGN KEY ("parent_tenant", "parent_id") ` +
				`REFERENCES "tenant ""west"""."parents" ` +
				`("tenant_id", "id") MATCH FULL ` +
				`ON UPDATE CASCADE ON DELETE SET NULL;`,
		},
	}
	if !reflect.DeepEqual(viewPostgresObjectStatements(got), want) {
		t.Fatalf("plan mismatch:\n got: %#v\nwant: %#v", got, want)
	}

	reversed := []Table{tables[1], tables[0]}
	reversed[1].Indexes = []Index{
		reversed[1].Indexes[1],
		reversed[1].Indexes[0],
	}
	for attempt := 0; attempt < 25; attempt++ {
		again, err := PlanPostgresDropRecreateObjects(
			reversed,
			options,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(viewPostgresObjectStatements(again), want) {
			t.Fatalf("attempt %d was not deterministic: %#v", attempt, again)
		}
	}
}

func TestPlanPostgresObjectsQuotesEveryIdentifier(t *testing.T) {
	t.Parallel()
	table := Table{
		Name: `odd"table`,
		Columns: []Column{{
			Name: `odd"column`,
			Type: "text",
		}},
		Indexes: []Index{{
			Name: `odd"index`,
			Columns: []IndexColumn{{
				Name:      `odd"column`,
				Collation: "BINARY",
			}},
		}},
	}
	got, err := PlanPostgresDropRecreateObjects(
		[]Table{table},
		PostgresObjectPlanOptions{
			MapNamespace: func(string) (string, error) {
				return `odd"schema`, nil
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = `CREATE INDEX "odd""index" ON ` +
		`"odd""schema"."odd""table" ` +
		`("odd""column" COLLATE "pg_catalog"."C" ASC NULLS FIRST);`
	if len(got) != 1 || got[0].SQL() != want {
		t.Fatalf("got %#v, want %q", got, want)
	}
}

func TestPlanPostgresObjectsInfersReferencedPrimaryKeyOrder(
	t *testing.T,
) {
	t.Parallel()
	tables := []Table{
		{
			Name: "parent",
			Columns: []Column{
				{Name: "second", Type: "integer", PrimaryKeyPosition: 2},
				{Name: "first", Type: "integer", PrimaryKeyPosition: 1},
			},
		},
		{
			Name: "child",
			Columns: []Column{
				{Name: "local_first", Type: "integer"},
				{Name: "local_second", Type: "integer"},
			},
			ForeignKeys: []ForeignKey{{
				Columns: []string{
					"local_first",
					"local_second",
				},
				ReferencedTable: "parent",
			}},
		},
	}
	got, err := PlanPostgresDropRecreateObjects(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = `ALTER TABLE "public"."child" ` +
		`ADD CONSTRAINT "dmtx_child_local_first_local_second_fkey" ` +
		`FOREIGN KEY ("local_first", "local_second") ` +
		`REFERENCES "public"."parent" ("first", "second") ` +
		`MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION;`
	if len(got) != 1 || got[0].SQL() != want {
		t.Fatalf("got %#v, want %q", got, want)
	}
}

func TestPlanPostgresObjectsPreservesForeignKeyConstraintName(
	t *testing.T,
) {
	tables := []Table{
		{
			Schema: "archive",
			Name:   "parents",
			Columns: []Column{{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			}},
		},
		{
			Schema: "archive",
			Name:   "children",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
				{Name: "parent_id", Type: "bigint"},
			},
			ForeignKeys: []ForeignKey{{
				Name:              "children_parent_contract",
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parents",
				ReferencedColumns: []string{"id"},
			}},
		},
	}
	statements, err := PlanPostgresDropRecreateObjects(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, statement := range statements {
		if statement.Kind() != PostgresForeignKeyObject {
			continue
		}
		found = true
		if statement.Name() != "children_parent_contract" {
			t.Fatalf("foreign-key name = %q", statement.Name())
		}
	}
	if !found {
		t.Fatal("foreign-key statement was not planned")
	}
}

func TestPlanPostgresObjectsAcceptsKnownUniqueReferencedIndex(
	t *testing.T,
) {
	t.Parallel()
	tables := []Table{
		{
			Name: "parent",
			Columns: []Column{
				{Name: "a", Type: "integer"},
				{Name: "b", Type: "integer"},
			},
			Indexes: []Index{{
				Name:   "parent_a_b_key",
				Unique: true,
				Columns: []IndexColumn{
					{Name: "a", Collation: "BINARY"},
					{Name: "b", Collation: "BINARY"},
				},
			}},
		},
		{
			Name: "child",
			Columns: []Column{
				{Name: "a", Type: "integer"},
				{Name: "b", Type: "integer"},
			},
			ForeignKeys: []ForeignKey{{
				Columns:           []string{"a", "b"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"a", "b"},
			}},
		},
	}
	got, err := PlanPostgresDropRecreateObjects(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 ||
		got[0].Kind() != PostgresIndexObject ||
		got[1].Kind() != PostgresForeignKeyObject {
		t.Fatalf("unexpected plan: %#v", got)
	}
}

func TestPostgresObjectNamesAreBoundedAndCollisionSafe(t *testing.T) {
	t.Parallel()
	longName := strings.Repeat("é", 40)
	tables := []Table{
		{
			Name: "alpha",
			Columns: []Column{
				{Name: "id", Type: "integer", PrimaryKey: true},
				{Name: "value", Type: "integer"},
			},
			Indexes: []Index{
				{
					Name:    "shared",
					Columns: []IndexColumn{{Name: "value"}},
				},
				{
					Name:    longName,
					Columns: []IndexColumn{{Name: "id"}},
				},
			},
		},
		{
			Name:    "beta",
			Columns: []Column{{Name: "value", Type: "integer"}},
			Indexes: []Index{{
				Name:    "shared",
				Columns: []IndexColumn{{Name: "value"}},
			}},
		},
		{
			Name:    "shared",
			Columns: []Column{{Name: "id", Type: "integer"}},
		},
	}
	first, err := PlanPostgresDropRecreateObjects(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanPostgresDropRecreateObjects(
		[]Table{tables[2], tables[1], tables[0]},
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("collision allocation was not deterministic")
	}
	if len(first) != 3 {
		t.Fatalf("statements = %d, want 3", len(first))
	}
	names := make(map[string]bool, len(first))
	for _, statement := range first {
		if len(statement.Name()) > postgresIdentifierMaximumBytes {
			t.Fatalf(
				"name %q is %d bytes",
				statement.Name(),
				len(statement.Name()),
			)
		}
		if !utf8.ValidString(statement.Name()) {
			t.Fatalf("name is not UTF-8: %q", statement.Name())
		}
		if names[statement.Name()] {
			t.Fatalf("duplicate relation name %q", statement.Name())
		}
		if statement.Name() == "shared" {
			t.Fatalf("index name collided with table relation: %#v", first)
		}
		names[statement.Name()] = true
	}
}

func TestPostgresObjectNameCollisionsIncludeConstraintsAndPrimaryKeys(
	t *testing.T,
) {
	t.Parallel()
	check := mustPostgresObjectCheck(t, `parent_id > 0`)
	table := Table{
		Name: "child",
		Columns: []Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "parent_id", Type: "integer"},
		},
		Checks: []CheckConstraint{{
			Name:       "dmtx_child_parent_id_fkey",
			Expression: check,
		}},
		ForeignKeys: []ForeignKey{{
			Columns:           []string{"parent_id"},
			ReferencedTable:   "parent",
			ReferencedColumns: []string{"id"},
		}},
	}
	parent := Table{
		Name:    "parent",
		Columns: []Column{{Name: "id", Type: "integer", PrimaryKey: true}},
		Indexes: []Index{{
			Name:    "parent_pkey",
			Columns: []IndexColumn{{Name: "id"}},
		}},
	}
	got, err := PlanPostgresDropRecreateObjects(
		[]Table{table, parent},
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %#v", got)
	}
	for _, statement := range got {
		if statement.Kind() == PostgresIndexObject &&
			statement.Name() == "parent_pkey" {
			t.Fatalf("index collided with generated primary-key relation")
		}
	}
	if got[1].Name() == got[2].Name() {
		t.Fatalf("constraint names collided: %#v", got)
	}
}

func TestPlanPostgresObjectsRejectsInvalidMetadata(t *testing.T) {
	t.Parallel()
	validParent := Table{
		Name:    "parent",
		Columns: []Column{{Name: "id", Type: "integer", PrimaryKey: true}},
	}
	validChild := Table{
		Name: "child",
		Columns: []Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
			{Name: "parent_id", Type: "integer"},
		},
	}
	validIndex := Index{
		Name:    "child_parent",
		Columns: []IndexColumn{{Name: "parent_id", Collation: "BINARY"}},
	}
	validForeignKey := ForeignKey{
		Columns:           []string{"parent_id"},
		ReferencedTable:   "parent",
		ReferencedColumns: []string{"id"},
	}
	validCheck := mustPostgresObjectCheck(t, `parent_id > 0`)

	tests := []struct {
		name   string
		tables []Table
	}{
		{
			name:   "empty table",
			tables: []Table{{Columns: validChild.Columns}},
		},
		{
			name: "long table",
			tables: []Table{{
				Name:    strings.Repeat("t", 64),
				Columns: validChild.Columns,
			}},
		},
		{
			name: "long column",
			tables: []Table{{
				Name: "child",
				Columns: []Column{{
					Name: strings.Repeat("c", 64),
					Type: "integer",
				}},
			}},
		},
		{
			name: "duplicate columns",
			tables: []Table{{
				Name: "child",
				Columns: []Column{
					{Name: "id", Type: "integer"},
					{Name: "id", Type: "integer"},
				},
			}},
		},
		{
			name: "standalone index without name",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{Columns: validIndex.Columns},
			)},
		},
		{
			name: "nonunique inline index",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{
					Inline:  true,
					Columns: validIndex.Columns,
				},
			)},
		},
		{
			name: "index without columns",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{Name: "empty"},
			)},
		},
		{
			name: "expression index",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{
					Name:    "expression",
					Columns: []IndexColumn{{}},
				},
			)},
		},
		{
			name: "unknown indexed column",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{
					Name: "unknown",
					Columns: []IndexColumn{{
						Name: "missing",
					}},
				},
			)},
		},
		{
			name: "NOCASE collation",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{
					Name: "nocase",
					Columns: []IndexColumn{{
						Name:      "parent_id",
						Collation: "NOCASE",
					}},
				},
			)},
		},
		{
			name: "invalid check expression",
			tables: []Table{postgresObjectTableWithCheck(
				validChild,
				CheckConstraint{},
			)},
		},
		{
			name: "empty foreign key",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					ForeignKey{},
				),
			},
		},
		{
			name: "unknown referenced table",
			tables: []Table{postgresObjectTableWithForeignKey(
				validChild,
				validForeignKey,
			)},
		},
		{
			name: "mismatched referenced columns",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					ForeignKey{
						Columns:           []string{"id", "parent_id"},
						ReferencedTable:   "parent",
						ReferencedColumns: []string{"id"},
					},
				),
			},
		},
		{
			name: "unknown local column",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					ForeignKey{
						Columns:           []string{"missing"},
						ReferencedTable:   "parent",
						ReferencedColumns: []string{"id"},
					},
				),
			},
		},
		{
			name: "unknown referenced column",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					ForeignKey{
						Columns:           []string{"parent_id"},
						ReferencedTable:   "parent",
						ReferencedColumns: []string{"missing"},
					},
				),
			},
		},
		{
			name: "duplicate local column",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					ForeignKey{
						Columns: []string{
							"parent_id",
							"parent_id",
						},
						ReferencedTable: "parent",
						ReferencedColumns: []string{
							"id",
							"id",
						},
					},
				),
			},
		},
		{
			name: "referenced columns not unique",
			tables: []Table{
				{
					Name: "parent",
					Columns: []Column{{
						Name: "id",
						Type: "integer",
					}},
				},
				postgresObjectTableWithForeignKey(
					validChild,
					validForeignKey,
				),
			},
		},
		{
			name: "unsupported update action",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					postgresObjectForeignKeyWith(
						validForeignKey,
						"INVALID",
						"",
						"",
					),
				),
			},
		},
		{
			name: "unsupported delete action",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					postgresObjectForeignKeyWith(
						validForeignKey,
						"",
						"INVALID",
						"",
					),
				),
			},
		},
		{
			name: "MATCH PARTIAL",
			tables: []Table{
				validParent,
				postgresObjectTableWithForeignKey(
					validChild,
					postgresObjectForeignKeyWith(
						validForeignKey,
						"",
						"",
						"PARTIAL",
					),
				),
			},
		},
		{
			name: "cross namespace reference is not guessed",
			tables: []Table{
				{
					Schema:  "one",
					Name:    validParent.Name,
					Columns: validParent.Columns,
				},
				{
					Schema:      "two",
					Name:        validChild.Name,
					Columns:     validChild.Columns,
					ForeignKeys: []ForeignKey{validForeignKey},
				},
			},
		},
		{
			name: "invalid UTF-8 index name",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{
					Name:    string([]byte{0xff}),
					Columns: validIndex.Columns,
				},
			)},
		},
		{
			name: "NUL index name",
			tables: []Table{postgresObjectTableWithIndex(
				validChild,
				Index{
					Name:    "bad\x00name",
					Columns: validIndex.Columns,
				},
			)},
		},
	}
	_ = validCheck
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := PlanPostgresDropRecreateObjects(
				test.tables,
				PostgresObjectPlanOptions{},
			)
			if err == nil {
				t.Fatal("expected fail-closed error")
			}
			if _, ok := err.(*PolicyError); !ok {
				t.Fatalf("error type = %T, want *PolicyError: %v", err, err)
			}
		})
	}
}

func TestPlanPostgresObjectsRejectsNamespaceFailures(t *testing.T) {
	t.Parallel()
	table := Table{
		Schema:  "source",
		Name:    "items",
		Columns: []Column{{Name: "id", Type: "integer"}},
	}
	cases := []struct {
		name   string
		mapper PostgresNamespaceMapper
		policy bool
	}{
		{
			name: "empty target",
			mapper: func(string) (string, error) {
				return "", nil
			},
			policy: true,
		},
		{
			name: "long target",
			mapper: func(string) (string, error) {
				return strings.Repeat("s", 64), nil
			},
			policy: true,
		},
		{
			name: "mapper error",
			mapper: func(string) (string, error) {
				return "", errors.New("mapping failed")
			},
		},
	}
	for _, test := range cases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := PlanPostgresDropRecreateObjects(
				[]Table{table},
				PostgresObjectPlanOptions{
					MapNamespace: test.mapper,
				},
			)
			if err == nil {
				t.Fatal("expected error")
			}
			_, policy := err.(*PolicyError)
			if policy != test.policy {
				t.Fatalf("policy error = %v, want %v: %T %v",
					policy, test.policy, err, err)
			}
		})
	}

	_, err := PlanPostgresDropRecreateObjects(
		[]Table{
			table,
			{
				Schema:  "other",
				Name:    "items",
				Columns: table.Columns,
			},
		},
		PostgresObjectPlanOptions{
			MapNamespace: func(string) (string, error) {
				return "collapsed", nil
			},
		},
	)
	if err == nil {
		t.Fatal("expected namespace collision")
	}
	if _, ok := err.(*PolicyError); !ok {
		t.Fatalf("error type = %T, want *PolicyError", err)
	}
}

func TestPlanPostgresObjectsDefaultCheckTranslationFailsClosed(
	t *testing.T,
) {
	t.Parallel()
	expression := mustPostgresObjectCheck(t, `length(value) > 0`)
	table := Table{
		Name:    "items",
		Columns: []Column{{Name: "value", Type: "text"}},
		Checks:  []CheckConstraint{{Expression: expression}},
	}
	if _, err := PlanPostgresDropRecreateObjects(
		[]Table{table},
		PostgresObjectPlanOptions{},
	); err == nil {
		t.Fatal("unsupported portable CHECK unexpectedly planned")
	}
}

func TestPlanPostgresObjectsEmptyInput(t *testing.T) {
	t.Parallel()
	got, err := PlanPostgresDropRecreateObjects(
		nil,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %#v, want empty plan", got)
	}
}

func TestPlanPostgresObjectsRejectsIncompatibleForeignKeyTypes(
	t *testing.T,
) {
	t.Parallel()
	tables := []Table{
		{
			Name: "parent",
			Columns: []Column{{
				Name:       "id",
				Type:       "bigint",
				PrimaryKey: true,
			}},
		},
		{
			Name: "child",
			Columns: []Column{{
				Name: "parent_id",
				Type: "text",
			}},
			ForeignKeys: []ForeignKey{{
				Columns:           []string{"parent_id"},
				ReferencedTable:   "parent",
				ReferencedColumns: []string{"id"},
			}},
		},
	}
	_, err := PlanPostgresDropRecreateObjects(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err == nil {
		t.Fatal("TEXT child to BIGINT parent unexpectedly planned")
	}
	var policy *PolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("error type = %T, want *PolicyError: %v", err, err)
	}
	if policy.Operation != "create PostgreSQL foreign key" {
		t.Fatalf("policy operation = %q", policy.Operation)
	}
}

func TestPlanPostgresObjectsReservesIdentitySequenceName(
	t *testing.T,
) {
	t.Parallel()
	frontier := int64(9)
	table := Table{
		Name: "accounts",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
			{Name: "label", Type: "text"},
		},
		Indexes: []Index{{
			Name: "accounts_id_seq",
			Columns: []IndexColumn{{
				Name:      "label",
				Collation: "BINARY",
			}},
		}},
	}
	first, err := PlanPostgresDropRecreateObjects(
		[]Table{table},
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PlanPostgresDropRecreateObjects(
		[]Table{table},
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0].Name() == "accounts_id_seq" {
		t.Fatalf("identity sequence name was not reserved: %#v", first)
	}
	if len(first[0].Name()) > postgresIdentifierMaximumBytes ||
		!reflect.DeepEqual(first, second) {
		t.Fatalf("identity collision allocation is invalid: %#v", first)
	}
}

func TestPostgresGeneratedRelationNameRetainsLabelAndUTF8(
	t *testing.T,
) {
	t.Parallel()
	got := postgresGeneratedRelationName(
		strings.Repeat("é", 24),
		strings.Repeat("x", 40),
		"seq",
	)
	if len(got) > postgresIdentifierMaximumBytes {
		t.Fatalf("generated name is %d bytes: %q", len(got), got)
	}
	if !utf8.ValidString(got) || !strings.HasSuffix(got, "_seq") {
		t.Fatalf("generated name is invalid: %q", got)
	}
}

func mustPostgresObjectCheck(t *testing.T, value string) Expression {
	t.Helper()
	expression, err := ParseSQLiteCheckExpression(value)
	if err != nil {
		t.Fatal(err)
	}
	return expression
}

func postgresObjectTableWithIndex(table Table, index Index) Table {
	table.Indexes = []Index{index}
	return table
}

func postgresObjectTableWithCheck(
	table Table,
	check CheckConstraint,
) Table {
	table.Checks = []CheckConstraint{check}
	return table
}

func postgresObjectTableWithForeignKey(
	table Table,
	foreignKey ForeignKey,
) Table {
	table.ForeignKeys = []ForeignKey{foreignKey}
	return table
}

func postgresObjectForeignKeyWith(
	foreignKey ForeignKey,
	onUpdate string,
	onDelete string,
	match string,
) ForeignKey {
	foreignKey.OnUpdate = onUpdate
	foreignKey.OnDelete = onDelete
	foreignKey.Match = match
	return foreignKey
}
