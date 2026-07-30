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
	if want := []string{"tenant_id", "event_id"}; !reflect.DeepEqual(
		target.ClickHouseOrderBy,
		want,
	) {
		t.Fatalf(
			"ClickHouse order = %v, want %v",
			target.ClickHouseOrderBy,
			want,
		)
	}
	gotTypes := make([]string, len(target.Columns))
	for index, column := range target.Columns {
		gotTypes[index] = column.Type
		if column.DeclaredType != nil ||
			column.Default != nil ||
			column.PrimaryKey ||
			column.PrimaryKeyPosition != 0 {
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

func TestClickHouseTargetRebuildsNativeOrderingWithoutRelationalKey(
	t *testing.T,
) {
	adapter := &clickHouseTargetAdapter{namespace: "target"}
	source := schema.Table{
		Schema:            "source",
		Name:              "events",
		ClickHouseOrderBy: []string{"tenant_id", "event_id"},
		Columns: []schema.Column{
			{Name: "payload", Type: "text"},
			{Name: "tenant_id", Type: "bigint"},
			{Name: "event_id", Type: "bigint"},
			{Name: "score", Type: "double", Nullable: true},
			{Name: "note", Type: "text", Nullable: true},
		},
	}
	target, err := planSingleTargetTable(
		adapter,
		"clickhouse",
		source,
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	if target.Schema != "target" ||
		!reflect.DeepEqual(
			target.ClickHouseOrderBy,
			source.ClickHouseOrderBy,
		) {
		t.Fatalf("projected native target = %#v", target)
	}
	for _, column := range target.Columns {
		if column.PrimaryKey || column.PrimaryKeyPosition != 0 {
			t.Fatalf("native order became relational key: %#v", column)
		}
	}
	statement, err := schema.CreateTable(schema.ClickHouse, target)
	if err != nil {
		t.Fatal(err)
	}
	const want = `CREATE TABLE "target"."events" (` +
		`"payload" String, "tenant_id" Int64, "event_id" Int64, ` +
		`"score" Nullable(Float64), "note" Nullable(String)) ` +
		`ENGINE = MergeTree ORDER BY ("tenant_id", "event_id");`
	if statement != want {
		t.Fatalf("ClickHouse DDL:\n got: %s\nwant: %s", statement, want)
	}
}

func TestClickHouseNativeProjectionFailsClosed(t *testing.T) {
	base := schema.Table{
		Name:              "events",
		ClickHouseOrderBy: []string{"id"},
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "payload", Type: "text", Nullable: true},
		},
	}
	tests := []struct {
		name   string
		mutate func(*schema.Table)
		want   string
	}{
		{
			name: "missing order",
			mutate: func(table *schema.Table) {
				table.ClickHouseOrderBy = nil
			},
			want: "ordering key",
		},
		{
			name: "relational key",
			mutate: func(table *schema.Table) {
				table.Columns[0].PrimaryKey = true
			},
			want: "column metadata",
		},
		{
			name: "unsupported type",
			mutate: func(table *schema.Table) {
				table.Columns[1].Type = "decimal"
			},
			want: "column type",
		},
		{
			name: "nullable order",
			mutate: func(table *schema.Table) {
				table.Columns[0].Nullable = true
			},
			want: "nullable ordering",
		},
		{
			name: "floating order",
			mutate: func(table *schema.Table) {
				table.Columns[0].Type = "double"
			},
			want: "floating-point ordering",
		},
		{
			name: "unknown order",
			mutate: func(table *schema.Table) {
				table.ClickHouseOrderBy = []string{"missing"}
			},
			want: "ordering column",
		},
		{
			name: "source object",
			mutate: func(table *schema.Table) {
				table.Indexes = []schema.Index{{Name: "idx"}}
			},
			want: "source metadata",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := base
			table.Columns = append([]schema.Column(nil), base.Columns...)
			table.ClickHouseOrderBy = append(
				[]string(nil),
				base.ClickHouseOrderBy...,
			)
			test.mutate(&table)
			_, err := projectClickHouseTableForClickHouse(table)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"error = %T %v, want policy containing %q",
					err,
					err,
					test.want,
				)
			}
		})
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
