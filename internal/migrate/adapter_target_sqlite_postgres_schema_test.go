package migrate

import (
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectPostgresTableForSQLitePreservesConservativeShape(
	t *testing.T,
) {
	sourceTables := postgresSQLiteProjectionFixture(t)
	targetTables := make([]schema.Table, len(sourceTables))
	for index, source := range sourceTables {
		projected, err := projectPostgresTableForSQLite(source)
		if err != nil {
			t.Fatalf("project %s: %v", source.Name, err)
		}
		targetTables[index] = projected
	}
	if err := validatePostgresSQLiteTables(
		sourceTables,
		targetTables,
	); err != nil {
		t.Fatalf("validate projected table set: %v", err)
	}

	accounts := targetTables[0]
	if accounts.Schema != "" ||
		accounts.Identity == nil ||
		accounts.Identity.Frontier == nil ||
		*accounts.Identity.Frontier != 41 {
		t.Fatalf("projected accounts identity = %#v", accounts.Identity)
	}
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[0], "bigint", "bigint", nil,
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[1], "varchar", "varchar", []int{24},
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[2], "numeric", "bigint", nil,
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[3], "boolean", "boolean", nil,
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[4], "bytea", "blob", nil,
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[5], "text", "text", nil,
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[6], "date", "date", nil,
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[7], "time", "time", []int{6},
	)
	assertPostgresSQLiteProjectedColumn(
		t, accounts.Columns[8], "timestamp", "timestamp", []int{3},
	)
	if got := accounts.Columns[1].Default.CanonicalSQL(); got != "'guest'" {
		t.Fatalf("SQLite string default = %q, want %q", got, "'guest'")
	}
	if got := accounts.Columns[4].Default.CanonicalSQL(); got != "X'00ff'" {
		t.Fatalf("SQLite blob default = %q, want X'00ff'", got)
	}
	if len(accounts.Checks) != 2 {
		t.Fatalf(
			"projected accounts checks = %#v, want source check and boolean domain",
			accounts.Checks,
		)
	}
	for _, check := range accounts.Checks {
		if check.Name != "" {
			t.Fatalf("SQLite CHECK retained source name %q", check.Name)
		}
	}
	if len(accounts.Indexes) != 2 ||
		accounts.Indexes[0].Columns[0].Collation != "BINARY" {
		t.Fatalf("projected indexes = %#v", accounts.Indexes)
	}

	events := targetTables[1]
	if len(events.ForeignKeys) != 1 {
		t.Fatalf("projected foreign keys = %#v", events.ForeignKeys)
	}
	foreignKey := events.ForeignKeys[0]
	if foreignKey.Name != "" ||
		foreignKey.Match != "NONE" ||
		foreignKey.OnUpdate != "CASCADE" ||
		foreignKey.OnDelete != "RESTRICT" {
		t.Fatalf("projected foreign key = %#v", foreignKey)
	}

	// Every nested mutable field is owned by the projection.
	if accounts.Identity == sourceTables[0].Identity ||
		accounts.Columns[1].DeclaredType ==
			sourceTables[0].Columns[1].DeclaredType ||
		accounts.Columns[1].Default ==
			sourceTables[0].Columns[1].Default {
		t.Fatal("projected identity, declaration, or default aliases source")
	}
	accounts.Identity.Frontier = postgresSQLiteInt64(99)
	accounts.Columns[1].DeclaredType.Arguments[0] = 1
	accounts.Indexes[0].Columns[0].Name = "changed"
	events.ForeignKeys[0].Columns[0] = "changed"
	if got := *sourceTables[0].Identity.Frontier; got != 41 {
		t.Fatalf("source identity frontier changed to %d", got)
	}
	if got := sourceTables[0].Columns[1].
		DeclaredType.Arguments[0]; got != 24 {
		t.Fatalf("source VARCHAR length changed to %d", got)
	}
	if got := sourceTables[0].Indexes[0].
		Columns[0].Name; got != "code" {
		t.Fatalf("source index column changed to %q", got)
	}
	if got := sourceTables[1].ForeignKeys[0].
		Columns[0]; got != "account_id" {
		t.Fatalf("source foreign-key column changed to %q", got)
	}
}

func TestProjectPostgresTableForSQLiteFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		table  int
		mutate func(*testing.T, *schema.Table)
		want   string
	}{
		{
			name:  "fractional numeric",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[2].DeclaredType.Arguments[1] = 2
			},
			want: "exact numeric",
		},
		{
			name:  "wide numeric",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[2].DeclaredType.Arguments[0] = 19
			},
			want: "exact numeric",
		},
		{
			name:  "fixed-width text",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[5].Type = "char"
				table.Columns[5].DeclaredType = &schema.DeclaredType{
					Base:      "char",
					Arguments: []int{10},
				}
				table.Columns[5].Default = nil
			},
			want: "blank-padding",
		},
		{
			name:  "floating point",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[5].Type = "double precision"
				table.Columns[5].DeclaredType = nil
				table.Columns[5].Default = nil
			},
			want: "floating-point",
		},
		{
			name:  "timezone timestamp",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[8].Type = "timestamptz"
				table.Columns[8].DeclaredType = nil
				table.Columns[8].Default = nil
			},
			want: "timezone-aware",
		},
		{
			name:  "json",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[5].Type = "jsonb"
				table.Columns[5].DeclaredType = nil
				table.Columns[5].Default = nil
			},
			want: "JSON",
		},
		{
			name:  "uuid",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[5].Type = "uuid"
				table.Columns[5].DeclaredType = nil
				table.Columns[5].Default = nil
			},
			want: "UUID",
		},
		{
			name:  "text primary key",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Identity = nil
				table.Columns[0].Type = "text"
				table.Columns[0].DeclaredType = nil
			},
			want: "primary-key comparison",
		},
		{
			name:  "text index without binary proof",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Indexes[0].Columns[0].Collation = ""
			},
			want: "index comparison",
		},
		{
			name:  "nullable indexed text",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[1].Nullable = true
			},
			want: "index comparison",
		},
		{
			name:  "text check",
			table: 0,
			mutate: func(t *testing.T, table *schema.Table) {
				table.Checks = append(
					table.Checks,
					schema.CheckConstraint{
						Name: "code_check",
						Expression: postgresSQLiteCheck(
							t,
							`"code" = 'active'`,
						),
					},
				)
			},
			want: "CHECK comparison",
		},
		{
			name:  "text foreign key",
			table: 1,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Columns[2].Type = "varchar"
				table.Columns[2].DeclaredType = &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				}
			},
			want: "foreign-key comparison",
		},
		{
			name:  "match full",
			table: 1,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].Match = "FULL"
			},
			want: "foreign-key match",
		},
		{
			name:  "duplicate foreign key columns",
			table: 1,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].Columns =
					[]string{"account_id", "account_id"}
				table.ForeignKeys[0].ReferencedColumns =
					[]string{"id", "id"}
			},
			want: "foreign-key columns",
		},
		{
			name:  "set default",
			table: 1,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].OnDelete = "SET DEFAULT"
			},
			want: "foreign-key action",
		},
		{
			name:  "set null into nonnullable column",
			table: 1,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.ForeignKeys[0].OnDelete = "SET NULL"
			},
			want: "SET NULL",
		},
		{
			name:  "inline source index",
			table: 0,
			mutate: func(_ *testing.T, table *schema.Table) {
				table.Indexes[0].Inline = true
			},
			want: "index shape",
		},
		{
			name:  "incompatible default",
			table: 1,
			mutate: func(t *testing.T, table *schema.Table) {
				table.Columns[0].Default =
					postgresSQLiteDefault(t, "'wrong'")
			},
			want: "default",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			tables := postgresSQLiteProjectionFixture(t)
			test.mutate(t, &tables[test.table])
			_, err := projectPostgresTableForSQLite(
				tables[test.table],
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"projection error = %v, want substring %q",
					err,
					test.want,
				)
			}
			var policy *schema.PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf(
					"projection error type = %T, want PolicyError: %v",
					err,
					err,
				)
			}
		})
	}
}

func TestValidatePostgresSQLiteTablesFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(
			*testing.T,
			[]schema.Table,
			[]schema.Table,
		) ([]schema.Table, []schema.Table)
		want string
	}{
		{
			name: "unselected parent",
			mutate: func(
				_ *testing.T,
				source []schema.Table,
				target []schema.Table,
			) ([]schema.Table, []schema.Table) {
				return source[1:], target[1:]
			},
			want: "unselected table",
		},
		{
			name: "parent follows child",
			mutate: func(
				_ *testing.T,
				source []schema.Table,
				target []schema.Table,
			) ([]schema.Table, []schema.Table) {
				return []schema.Table{source[1], source[0]},
					[]schema.Table{target[1], target[0]}
			},
			want: "plan order",
		},
		{
			name: "reference types differ",
			mutate: func(
				t *testing.T,
				source []schema.Table,
				_ []schema.Table,
			) ([]schema.Table, []schema.Table) {
				source[1].Columns[2].Type = "integer"
				projected := postgresSQLiteProjectTables(t, source)
				return source, projected
			},
			want: "foreign-key comparison",
		},
		{
			name: "parent columns are not primary key",
			mutate: func(
				t *testing.T,
				source []schema.Table,
				_ []schema.Table,
			) ([]schema.Table, []schema.Table) {
				source[0].Identity = nil
				source[0].Columns[0].PrimaryKey = false
				source[0].Columns[0].PrimaryKeyPosition = 0
				projected := postgresSQLiteProjectTables(t, source)
				return source, projected
			},
			want: "parent key",
		},
		{
			name: "global table names collide",
			mutate: func(
				t *testing.T,
				_ []schema.Table,
				_ []schema.Table,
			) ([]schema.Table, []schema.Table) {
				source := []schema.Table{
					postgresSQLiteMinimalTable("Alpha"),
					postgresSQLiteMinimalTable("alpha"),
				}
				return source,
					postgresSQLiteProjectTables(t, source)
			},
			want: "global object names",
		},
		{
			name: "index collides with table",
			mutate: func(
				t *testing.T,
				source []schema.Table,
				_ []schema.Table,
			) ([]schema.Table, []schema.Table) {
				source[0].Indexes[0].Name =
					strings.ToUpper(source[1].Name)
				return source,
					postgresSQLiteProjectTables(t, source)
			},
			want: "global object names",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			source := postgresSQLiteProjectionFixture(t)
			target := postgresSQLiteProjectTables(t, source)
			source, target = test.mutate(t, source, target)
			err := validatePostgresSQLiteTables(source, target)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"table-set error = %v, want substring %q",
					err,
					test.want,
				)
			}
			var policy *schema.PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf(
					"table-set error type = %T, want PolicyError: %v",
					err,
					err,
				)
			}
		})
	}
}

func postgresSQLiteProjectionFixture(
	t *testing.T,
) []schema.Table {
	t.Helper()
	frontier := int64(41)
	accounts := schema.Table{
		Schema: "tenant",
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
				Name: "code",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
			},
			{
				Name: "exact_count",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{18, 0},
				},
			},
			{Name: "enabled", Type: "boolean"},
			{Name: "payload", Type: "bytea", Nullable: true},
			{Name: "description", Type: "text", Nullable: true},
			{Name: "created_on", Type: "date"},
			{
				Name: "local_time",
				Type: "time",
				DeclaredType: &schema.DeclaredType{
					Base:      "time",
					Arguments: []int{6},
				},
			},
			{
				Name: "created_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
		},
		Indexes: []schema.Index{
			{
				Name:   "accounts_code_uq",
				Unique: true,
				Columns: []schema.IndexColumn{{
					Name:      "code",
					Collation: "BINARY",
				}},
			},
			{
				Name: "accounts_exact_count_idx",
				Columns: []schema.IndexColumn{{
					Name:       "exact_count",
					Descending: true,
				}},
			},
		},
		Checks: []schema.CheckConstraint{{
			Name: "accounts_exact_count_check",
			Expression: postgresSQLiteCheck(
				t,
				`"exact_count" >= 0`,
			),
		}},
	}
	accounts.Columns[1].Default = postgresSQLiteCatalogDefault(
		t,
		accounts.Columns[1],
		`'guest'::character varying`,
	)
	accounts.Columns[2].Default = postgresSQLiteCatalogDefault(
		t,
		accounts.Columns[2],
		`0`,
	)
	accounts.Columns[3].Default = postgresSQLiteCatalogDefault(
		t,
		accounts.Columns[3],
		`true`,
	)
	accounts.Columns[4].Default = postgresSQLiteCatalogDefault(
		t,
		accounts.Columns[4],
		`decode('00ff'::text, 'hex'::text)`,
	)
	accounts.Columns[5].Default = postgresSQLiteCatalogDefault(
		t,
		accounts.Columns[5],
		`'safe'::text`,
	)
	accounts.Columns[6].Default =
		postgresSQLiteDefault(t, "CURRENT_DATE")
	accounts.Columns[8].Default =
		postgresSQLiteDefault(t, "CURRENT_TIMESTAMP")

	events := schema.Table{
		Schema: "tenant",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "event_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{Name: "account_id", Type: "bigint"},
			{
				Name: "note",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{80},
				},
			},
		},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_account_fkey",
			Columns:           []string{"account_id"},
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "RESTRICT",
			Match:             "SIMPLE",
		}},
		Checks: []schema.CheckConstraint{{
			Name: "events_id_check",
			Expression: postgresSQLiteCheck(
				t,
				`"event_id" > 0`,
			),
		}},
	}
	events.Columns[3].Default = postgresSQLiteCatalogDefault(
		t,
		events.Columns[3],
		`'created'::character varying`,
	)
	return []schema.Table{accounts, events}
}

func postgresSQLiteMinimalTable(name string) schema.Table {
	return schema.Table{
		Schema: "tenant",
		Name:   name,
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "integer",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
}

func postgresSQLiteProjectTables(
	t *testing.T,
	source []schema.Table,
) []schema.Table {
	t.Helper()
	target := make([]schema.Table, len(source))
	for index, table := range source {
		projected, err := projectPostgresTableForSQLite(table)
		if err != nil {
			t.Fatalf("project %s: %v", table.Name, err)
		}
		target[index] = projected
	}
	return target
}

func postgresSQLiteCatalogDefault(
	t *testing.T,
	column schema.Column,
	catalog string,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParsePostgresCatalogDefault(
		column,
		&catalog,
	)
	if err != nil {
		t.Fatalf("parse PostgreSQL default %q: %v", catalog, err)
	}
	return expression
}

func postgresSQLiteDefault(
	t *testing.T,
	value string,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParseSQLiteDefault(value)
	if err != nil {
		t.Fatalf("parse structured default %q: %v", value, err)
	}
	return expression
}

func postgresSQLiteCheck(
	t *testing.T,
	value string,
) schema.Expression {
	t.Helper()
	expression, err := schema.ParseSQLiteCheckExpression(value)
	if err != nil {
		t.Fatalf("parse structured CHECK %q: %v", value, err)
	}
	return expression
}

func postgresSQLiteInt64(value int64) *int64 {
	return &value
}

func assertPostgresSQLiteProjectedColumn(
	t *testing.T,
	column schema.Column,
	sourceType string,
	declaredBase string,
	arguments []int,
) {
	t.Helper()
	if column.Type != sourceType ||
		column.DeclaredType == nil ||
		column.DeclaredType.Base != declaredBase ||
		!postgresSQLiteIntsEqual(
			column.DeclaredType.Arguments,
			arguments,
		) {
		t.Fatalf(
			"projected column %s = %#v, want Type %q declaration %s%v",
			column.Name,
			column,
			sourceType,
			declaredBase,
			arguments,
		)
	}
}

func postgresSQLiteIntsEqual(left []int, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
