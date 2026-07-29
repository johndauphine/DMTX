package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresWriteShapeRejectsInvalidRequestsBeforeConnection(t *testing.T) {
	valid := postgresNativeTestTable()
	tests := []struct {
		name    string
		table   schema.Table
		columns []string
		mode    string
		want    string
	}{
		{
			name:    "empty schema",
			table:   withPostgresTableSchema(valid, ""),
			columns: []string{"id"},
			mode:    "drop_recreate",
			want:    "target schema and table name are required",
		},
		{
			name:    "empty table",
			table:   withPostgresTableName(valid, ""),
			columns: []string{"id"},
			mode:    "drop_recreate",
			want:    "target schema and table name are required",
		},
		{
			name:    "mode",
			table:   valid,
			columns: []string{"id"},
			mode:    "append",
			want:    `unsupported target mode "append"`,
		},
		{
			name:  "no requested columns",
			table: valid,
			mode:  "drop_recreate",
			want:  "at least one column is required",
		},
		{
			name: "empty metadata name",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					{Type: "integer"},
				},
			},
			columns: []string{"id"},
			mode:    "drop_recreate",
			want:    "schema contains an empty column name",
		},
		{
			name: "duplicate metadata",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", Type: "integer"},
					{Name: "id", Type: "integer"},
				},
			},
			columns: []string{"id"},
			mode:    "drop_recreate",
			want:    "schema contains duplicate column id",
		},
		{
			name:    "empty requested name",
			table:   valid,
			columns: []string{""},
			mode:    "drop_recreate",
			want:    "requested column name is empty",
		},
		{
			name:    "duplicate requested",
			table:   valid,
			columns: []string{"id", "id"},
			mode:    "drop_recreate",
			want:    "requested column id is duplicated",
		},
		{
			name:    "missing requested",
			table:   valid,
			columns: []string{"missing"},
			mode:    "drop_recreate",
			want:    "requested column missing is not present",
		},
		{
			name: "upsert without primary key",
			table: schema.Table{
				Schema: "public",
				Name:   "events",
				Columns: []schema.Column{
					{Name: "id", Type: "integer"},
				},
			},
			columns: []string{"id"},
			mode:    "upsert",
			want:    "has no primary key",
		},
		{
			name:    "upsert omits primary key",
			table:   valid,
			columns: []string{`pay"load`},
			mode:    "upsert",
			want:    "primary-key column id is not included",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, provider, _ := newPostgresNativeTestWriter()
			receipt, err := writer.WriteBatch(
				context.Background(),
				test.table,
				test.columns,
				test.mode,
				nil,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
			assertPostgresReceipt(
				t,
				receipt,
				CommitNotCommitted,
				0,
				0,
			)
			if provider.calls != 0 {
				t.Fatalf("provider calls = %d, want 0", provider.calls)
			}
		})
	}
}

func TestPostgresNativePlanIsDeterministicAndQuotesIdentifiers(t *testing.T) {
	table := schema.Table{
		Schema: `tenant"schema`,
		Name:   `event"data`,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{
				Name:               `tenant"id`,
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: `pay"load`, Type: "text"},
		},
	}
	columns := []string{"id", `tenant"id`, `pay"load`}
	stage := postgresStageTableName(table, columns)
	if again := postgresStageTableName(table, columns); again != stage {
		t.Fatalf("stage name changed: %q then %q", stage, again)
	}
	if !strings.HasPrefix(stage, "dmtx_stage_") || len(stage) != 27 {
		t.Fatalf("stage name = %q", stage)
	}
	if changed := postgresStageTableName(
		table,
		[]string{`tenant"id`, "id", `pay"load`},
	); changed == stage {
		t.Fatalf("column-order change reused stage name %q", stage)
	}

	create := postgresCreateStageStatement(table, columns, stage)
	wantCreate := `CREATE TEMP TABLE ` + postgresIdentifier(stage) +
		` ON COMMIT DROP AS SELECT "id", "tenant""id", "pay""load"` +
		` FROM "tenant""schema"."event""data" WITH NO DATA`
	if create != wantCreate {
		t.Fatalf("create statement = %q, want %q", create, wantCreate)
	}

	merge, updates, err := postgresMergeStageStatement(
		table,
		columns,
		stage,
	)
	if err != nil {
		t.Fatalf("postgresMergeStageStatement: %v", err)
	}
	if !updates {
		t.Fatal("merge unexpectedly reported key-only behavior")
	}
	wantMerge := `INSERT INTO "tenant""schema"."event""data"` +
		` ("id", "tenant""id", "pay""load")` +
		` SELECT "id", "tenant""id", "pay""load"` +
		` FROM "pg_temp".` + postgresIdentifier(stage) +
		` WHERE true ON CONFLICT ("tenant""id", "id")` +
		` DO UPDATE SET "pay""load" = EXCLUDED."pay""load"`
	if merge != wantMerge {
		t.Fatalf("merge statement = %q, want %q", merge, wantMerge)
	}
}

func TestPostgresNativePlanKeyOnlyUsesDoNothing(t *testing.T) {
	table := schema.Table{
		Schema: "public",
		Name:   "keys",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", PrimaryKey: true},
		},
	}
	statement, updates, err := postgresMergeStageStatement(
		table,
		[]string{"id"},
		"dmtx_stage_keys",
	)
	if err != nil {
		t.Fatalf("postgresMergeStageStatement: %v", err)
	}
	if updates {
		t.Fatal("key-only plan reported update columns")
	}
	if !strings.HasSuffix(
		statement,
		`WHERE true ON CONFLICT ("id") DO NOTHING`,
	) {
		t.Fatalf("statement = %q", statement)
	}
}

func withPostgresTableSchema(
	table schema.Table,
	namespace string,
) schema.Table {
	table.Schema = namespace
	return table
}

func withPostgresTableName(
	table schema.Table,
	name string,
) schema.Table {
	table.Name = name
	return table
}
