package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestClickHouseTargetProjectsStrictSQLiteScalarContract(t *testing.T) {
	adapter := &clickHouseTargetAdapter{namespace: "analytics"}
	source := schema.Table{
		Name:         "events",
		SQLiteStrict: true,
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "integer",
				Nullable:           true,
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "integer",
				},
			},
			{
				Name:               "event_id",
				Type:               "int",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
				DeclaredType: &schema.DeclaredType{
					Base: "int",
				},
			},
			{
				Name:     "score",
				Type:     "real",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "real",
				},
			},
			{
				Name: "note",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base: "text",
				},
			},
			{
				Name:     "payload",
				Type:     "blob",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "blob",
				},
			},
		},
	}
	target, err := planSingleTargetTable(
		adapter,
		"sqlite",
		source,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if target.Schema != "analytics" ||
		target.SQLiteStrict ||
		target.SQLiteWithoutRowID ||
		target.Columns[0].Nullable {
		t.Fatalf("projected target = %+v", target)
	}
	gotTypes := make([]string, len(target.Columns))
	for index, column := range target.Columns {
		gotTypes[index] = column.Type
		if column.DeclaredType != nil || column.Default != nil {
			t.Fatalf("projected column retained SQLite metadata: %+v", column)
		}
	}
	if want := []string{
		"bigint",
		"bigint",
		"double",
		"text",
		"blob",
	}; !reflect.DeepEqual(gotTypes, want) {
		t.Fatalf("projected types = %v, want %v", gotTypes, want)
	}
	statement, err := schema.CreateTable(schema.ClickHouse, target)
	if err != nil {
		t.Fatal(err)
	}
	const want = `CREATE TABLE "analytics"."events" (` +
		`"tenant_id" Int64, ` +
		`"event_id" Int64, ` +
		`"score" Nullable(Float64), ` +
		`"note" String, ` +
		`"payload" Nullable(String)) ` +
		`ENGINE = MergeTree ORDER BY ("tenant_id", "event_id");`
	if statement != want {
		t.Fatalf("ClickHouse DDL:\n got: %s\nwant: %s", statement, want)
	}
}

func TestClickHouseTargetProjectionFailsClosed(t *testing.T) {
	defaultExpression, err := schema.ParseSQLiteDefault("1")
	if err != nil {
		t.Fatal(err)
	}
	checkExpression, err := schema.ParseSQLiteCheckExpression(`"id" > 0`)
	if err != nil {
		t.Fatal(err)
	}
	base := schema.Table{
		Name:         "events",
		SQLiteStrict: true,
		Columns: []schema.Column{{
			Name:       "id",
			Type:       "integer",
			PrimaryKey: true,
			DeclaredType: &schema.DeclaredType{
				Base: "integer",
			},
		}},
	}
	tests := []struct {
		name   string
		mutate func(*schema.Table)
		want   string
	}{
		{
			name: "non strict",
			mutate: func(table *schema.Table) {
				table.SQLiteStrict = false
			},
			want: "non-STRICT",
		},
		{
			name: "any",
			mutate: func(table *schema.Table) {
				table.Columns[0].DeclaredType.Base = "any"
			},
			want: "declared type",
		},
		{
			name: "modifier",
			mutate: func(table *schema.Table) {
				table.Columns[0].DeclaredType.Arguments = []int{8}
			},
			want: "declared type",
		},
		{
			name: "default",
			mutate: func(table *schema.Table) {
				table.Columns[0].Default = defaultExpression
			},
			want: "default",
		},
		{
			name: "identity",
			mutate: func(table *schema.Table) {
				table.Identity = &schema.Identity{
					Column:     "id",
					Generation: schema.IdentityByDefault,
				}
			},
			want: "identity",
		},
		{
			name: "index",
			mutate: func(table *schema.Table) {
				table.Indexes = []schema.Index{{Name: "events_id"}}
			},
			want: "indexes",
		},
		{
			name: "foreign key",
			mutate: func(table *schema.Table) {
				table.ForeignKeys = []schema.ForeignKey{{
					Columns:         []string{"id"},
					ReferencedTable: "parents",
				}}
			},
			want: "foreign keys",
		},
		{
			name: "check",
			mutate: func(table *schema.Table) {
				table.Checks = []schema.CheckConstraint{{
					Expression: checkExpression,
				}}
			},
			want: "CHECK",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := base
			table.Columns = append([]schema.Column(nil), base.Columns...)
			declared := *base.Columns[0].DeclaredType
			table.Columns[0].DeclaredType = &declared
			test.mutate(&table)
			_, err := projectSQLiteTableForClickHouse(table)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %T %v, want policy containing %q", err, err, test.want)
			}
		})
	}
}

func TestClickHouseTargetRejectsUnsupportedRouteAndMode(t *testing.T) {
	adapter := &clickHouseTargetAdapter{namespace: "analytics"}
	if _, err := adapter.PlanTables(
		"postgres",
		nil,
		"drop_recreate",
	); err == nil || !strings.Contains(err.Error(), "source engine") {
		t.Fatalf("source error = %v", err)
	}
	if _, err := adapter.PlanTables(
		"sqlite",
		nil,
		"upsert",
	); err == nil || !strings.Contains(err.Error(), "target mode") {
		t.Fatalf("mode error = %v", err)
	}
}

func TestValidateClickHouseTargetEndpoint(t *testing.T) {
	valid := config.Endpoint{
		Host:     "127.0.0.1",
		Database: "analytics",
		User:     "dmtx",
		Schema:   "analytics",
	}
	if err := validateClickHouseTargetEndpoint(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*config.Endpoint)
		want   string
	}{
		{
			name: "missing host",
			mutate: func(endpoint *config.Endpoint) {
				endpoint.Host = ""
			},
			want: "host, database, and user",
		},
		{
			name: "reserved database",
			mutate: func(endpoint *config.Endpoint) {
				endpoint.Database = "SYSTEM"
				endpoint.Schema = ""
			},
			want: "reserved system database",
		},
		{
			name: "schema mismatch",
			mutate: func(endpoint *config.Endpoint) {
				endpoint.Schema = "other"
			},
			want: "schema must be empty or match",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := valid
			test.mutate(&endpoint)
			if err := validateClickHouseTargetEndpoint(endpoint); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}
