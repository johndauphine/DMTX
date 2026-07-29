package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPlanPostgresRetainedIndexesAndForeignKeys(t *testing.T) {
	tables := postgresRetainedObjectTestTables()
	plan, err := planPostgresRetainedIndexesAndForeignKeys(tables)
	if err != nil {
		t.Fatal(err)
	}

	accountsKey := postgresRetainedTableKey("tenant", "accounts")
	accountIndexes := plan.indexes[accountsKey]
	if len(accountIndexes) != 1 {
		t.Fatalf("account indexes = %#v", accountIndexes)
	}
	index := accountIndexes[0]
	if index.name != "accounts_external_id_uq" ||
		!index.unique ||
		index.nullsNotDistinct ||
		index.method != "btree" ||
		index.keyColumns != 1 ||
		index.totalColumns != 1 ||
		len(index.columns) != 1 {
		t.Fatalf("planned retained index = %#v", index)
	}
	if index.columns[0] != (postgresRetainedIndexColumn{
		name:            "external_id",
		nullsFirst:      true,
		collationSchema: "pg_catalog",
		collationName:   "C",
		defaultOperator: true,
	}) {
		t.Fatalf("planned retained index column = %#v", index.columns[0])
	}

	eventsKey := postgresRetainedTableKey("tenant", "account_events")
	eventForeignKeys := plan.foreignKeys[eventsKey]
	if len(eventForeignKeys) != 1 {
		t.Fatalf("event foreign keys = %#v", eventForeignKeys)
	}
	foreignKey := eventForeignKeys[0]
	if foreignKey.name != "dmtx_account_events_account_id_fkey" ||
		!foreignKey.validated ||
		foreignKey.onUpdate != "c" ||
		foreignKey.onDelete != "r" ||
		foreignKey.match != "s" ||
		foreignKey.referencedSchema != "tenant" ||
		foreignKey.referencedTable != "accounts" ||
		len(foreignKey.columns) != 1 ||
		foreignKey.columns[0] != (postgresRetainedForeignKeyColumn{
			local:      "account_id",
			referenced: "id",
		}) {
		t.Fatalf("planned retained foreign key = %#v", foreignKey)
	}
}

func TestValidatePostgresRetainedIndexesRejectsMissingExtraAndChanged(
	t *testing.T,
) {
	table := schema.Table{Name: "accounts"}
	expected := exactPostgresRetainedIndex()
	tests := []struct {
		name   string
		actual []postgresRetainedIndex
		want   string
	}{
		{
			name: "missing",
			want: "is missing",
		},
		{
			name: "extra",
			actual: []postgresRetainedIndex{
				expected,
				postgresRetainedIndex{name: "unexpected"},
			},
			want: "unexpected secondary index",
		},
		{
			name: "changed uniqueness",
			actual: func() []postgresRetainedIndex {
				changed := clonePostgresRetainedIndex(expected)
				changed.unique = false
				return []postgresRetainedIndex{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "changed unique null semantics",
			actual: func() []postgresRetainedIndex {
				changed := clonePostgresRetainedIndex(expected)
				changed.nullsNotDistinct = true
				return []postgresRetainedIndex{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "changed order",
			actual: func() []postgresRetainedIndex {
				changed := clonePostgresRetainedIndex(expected)
				changed.columns[0].descending = true
				changed.columns[0].nullsFirst = false
				return []postgresRetainedIndex{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "changed collation",
			actual: func() []postgresRetainedIndex {
				changed := clonePostgresRetainedIndex(expected)
				changed.columns[0].collationName = "default"
				return []postgresRetainedIndex{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "nondefault operator class",
			actual: func() []postgresRetainedIndex {
				changed := clonePostgresRetainedIndex(expected)
				changed.columns[0].defaultOperator = false
				return []postgresRetainedIndex{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "constraint-backed replacement",
			actual: func() []postgresRetainedIndex {
				changed := clonePostgresRetainedIndex(expected)
				changed.constraintType = "u"
				return []postgresRetainedIndex{changed}
			}(),
			want: "differs from the planned shape",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostgresRetainedIndexes(
				table,
				[]postgresRetainedIndex{expected},
				test.actual,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("retained index error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidatePostgresRetainedForeignKeysRejectsMissingExtraAndChanged(
	t *testing.T,
) {
	table := schema.Table{Name: "account_events"}
	expected := exactPostgresRetainedForeignKey()
	tests := []struct {
		name   string
		actual []postgresRetainedForeignKey
		want   string
	}{
		{
			name: "missing",
			want: "is missing",
		},
		{
			name: "extra",
			actual: []postgresRetainedForeignKey{
				expected,
				postgresRetainedForeignKey{name: "unexpected"},
			},
			want: "unexpected foreign key",
		},
		{
			name: "changed update action",
			actual: func() []postgresRetainedForeignKey {
				changed := clonePostgresRetainedForeignKey(expected)
				changed.onUpdate = "a"
				return []postgresRetainedForeignKey{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "changed delete action",
			actual: func() []postgresRetainedForeignKey {
				changed := clonePostgresRetainedForeignKey(expected)
				changed.onDelete = "c"
				return []postgresRetainedForeignKey{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "changed referenced table",
			actual: func() []postgresRetainedForeignKey {
				changed := clonePostgresRetainedForeignKey(expected)
				changed.referencedTable = "other_accounts"
				return []postgresRetainedForeignKey{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "changed column order",
			actual: func() []postgresRetainedForeignKey {
				changed := clonePostgresRetainedForeignKey(expected)
				changed.columns[0].referenced = "other_id"
				return []postgresRetainedForeignKey{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "not validated",
			actual: func() []postgresRetainedForeignKey {
				changed := clonePostgresRetainedForeignKey(expected)
				changed.validated = false
				return []postgresRetainedForeignKey{changed}
			}(),
			want: "differs from the planned shape",
		},
		{
			name: "deferrable",
			actual: func() []postgresRetainedForeignKey {
				changed := clonePostgresRetainedForeignKey(expected)
				changed.deferrable = true
				return []postgresRetainedForeignKey{changed}
			}(),
			want: "differs from the planned shape",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validatePostgresRetainedForeignKeys(
				table,
				[]postgresRetainedForeignKey{expected},
				test.actual,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("retained foreign-key error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidatePostgresRetainedObjectsAcceptsExactShapes(t *testing.T) {
	index := exactPostgresRetainedIndex()
	if err := validatePostgresRetainedIndexes(
		schema.Table{Name: "accounts"},
		[]postgresRetainedIndex{index},
		[]postgresRetainedIndex{clonePostgresRetainedIndex(index)},
	); err != nil {
		t.Fatal(err)
	}
	foreignKey := exactPostgresRetainedForeignKey()
	if err := validatePostgresRetainedForeignKeys(
		schema.Table{Name: "account_events"},
		[]postgresRetainedForeignKey{foreignKey},
		[]postgresRetainedForeignKey{
			clonePostgresRetainedForeignKey(foreignKey),
		},
	); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresRetainedObjectQueriesUseStructuralCatalogFields(t *testing.T) {
	indexFragments := []string{
		"index_metadata.indisunique",
		"index_metadata.indnullsnotdistinct",
		"index_metadata.indoption",
		"index_metadata.indcollation",
		"index_metadata.indclass",
		"operator_class.opcdefault",
		"NOT index_metadata.indisprimary",
	}
	for _, fragment := range indexFragments {
		if !strings.Contains(postgresRetainedIndexesQuery, fragment) {
			t.Fatalf("index catalog query is missing %q", fragment)
		}
	}
	foreignKeyFragments := []string{
		"constraint_object.conkey",
		"constraint_object.confkey",
		"constraint_object.confupdtype",
		"constraint_object.confdeltype",
		"constraint_object.confmatchtype",
	}
	for _, fragment := range foreignKeyFragments {
		if !strings.Contains(postgresRetainedForeignKeysQuery, fragment) {
			t.Fatalf("foreign-key catalog query is missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"pg_get_indexdef",
		"pg_get_constraintdef",
	} {
		if strings.Contains(postgresRetainedIndexesQuery, forbidden) ||
			strings.Contains(postgresRetainedForeignKeysQuery, forbidden) {
			t.Fatalf("catalog query uses executable definition text %q", forbidden)
		}
	}
}

func postgresRetainedObjectTestTables() []schema.Table {
	accounts := schema.Table{
		Schema: "tenant",
		Name:   "accounts",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "external_id", Type: "text"},
		},
		Indexes: []schema.Index{
			{
				Name:   "accounts_external_id_uq",
				Unique: true,
				Columns: []schema.IndexColumn{
					{Name: "external_id", Collation: "BINARY"},
				},
			},
		},
	}
	events := schema.Table{
		Schema: "tenant",
		Name:   "account_events",
		Columns: []schema.Column{
			{
				Name:               "account_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{
				Name:               "sequence_no",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
		},
		ForeignKeys: []schema.ForeignKey{
			{
				Columns:           []string{"account_id"},
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "CASCADE",
				OnDelete:          "RESTRICT",
			},
		},
	}
	return []schema.Table{events, accounts}
}

func exactPostgresRetainedIndex() postgresRetainedIndex {
	return postgresRetainedIndex{
		name:         "accounts_external_id_uq",
		unique:       true,
		valid:        true,
		ready:        true,
		live:         true,
		method:       "btree",
		keyColumns:   1,
		totalColumns: 1,
		columns: []postgresRetainedIndexColumn{
			{
				name:            "external_id",
				nullsFirst:      true,
				collationSchema: "pg_catalog",
				collationName:   "C",
				defaultOperator: true,
			},
		},
	}
}

func exactPostgresRetainedForeignKey() postgresRetainedForeignKey {
	return postgresRetainedForeignKey{
		name:             "dmtx_account_events_account_id_fkey",
		validated:        true,
		noInherit:        true,
		local:            true,
		onUpdate:         "c",
		onDelete:         "r",
		match:            "s",
		referencedSchema: "tenant",
		referencedTable:  "accounts",
		columns: []postgresRetainedForeignKeyColumn{
			{local: "account_id", referenced: "id"},
		},
	}
}

func clonePostgresRetainedIndex(
	value postgresRetainedIndex,
) postgresRetainedIndex {
	value.columns = append([]postgresRetainedIndexColumn(nil), value.columns...)
	return value
}

func clonePostgresRetainedForeignKey(
	value postgresRetainedForeignKey,
) postgresRetainedForeignKey {
	value.columns = append(
		[]postgresRetainedForeignKeyColumn(nil),
		value.columns...,
	)
	return value
}
