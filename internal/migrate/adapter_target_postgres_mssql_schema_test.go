package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestProjectSQLServerTableForPostgresPreservesSafeShape(t *testing.T) {
	frontier := int64(9)
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
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name: "note",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{80},
				},
			},
			{
				Name: "payload",
				Type: "blob",
				DeclaredType: &schema.DeclaredType{
					Base:      "varbinary",
					Arguments: []int{16},
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
				Name: "ratio",
				Type: "real",
				DeclaredType: &schema.DeclaredType{
					Base: "real",
				},
			},
			{
				Name: "minute_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "smalldatetime",
				},
			},
		},
		Indexes: []schema.Index{{
			Name: "events_occurred_idx",
			Columns: []schema.IndexColumn{{
				Name:       "occurred_at",
				Descending: true,
			}},
		}},
	}
	got, err := projectSQLServerTableForPostgres(source)
	if err != nil {
		t.Fatal(err)
	}
	wantTypes := []struct {
		typ         string
		declaration *schema.DeclaredType
	}{
		{typ: "bigint"},
		{
			typ: "text",
			declaration: &schema.DeclaredType{
				Base:      "varchar",
				Arguments: []int{80},
			},
		},
		{typ: "bytea"},
		{
			typ: "datetime",
			declaration: &schema.DeclaredType{
				Base:      "timestamp",
				Arguments: []int{6},
			},
		},
		{
			typ: "real",
			declaration: &schema.DeclaredType{
				Base: "real",
			},
		},
		{
			typ: "datetime",
			declaration: &schema.DeclaredType{
				Base:      "timestamp",
				Arguments: []int{0},
			},
		},
	}
	for index, want := range wantTypes {
		if got.Columns[index].Type != want.typ ||
			!reflect.DeepEqual(
				got.Columns[index].DeclaredType,
				want.declaration,
			) {
			t.Fatalf(
				"column %d = %#v, want type %s declaration %#v",
				index,
				got.Columns[index],
				want.typ,
				want.declaration,
			)
		}
	}
	if got.Identity == source.Identity ||
		got.Identity == nil ||
		got.Identity.Frontier == source.Identity.Frontier {
		t.Fatalf("identity was not deeply cloned: %#v", got.Identity)
	}
	ddl, err := schema.CreateTable(schema.Postgres, got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ddl, `"ratio" REAL NOT NULL`) {
		t.Fatalf("REAL projection DDL = %q", ddl)
	}
}

func TestProjectSQLServerTableForPostgresFailsClosed(t *testing.T) {
	tests := []schema.Column{
		{Name: "missing", Type: "text"},
		{
			Name: "legacy_image",
			Type: "blob",
			DeclaredType: &schema.DeclaredType{
				Base: "image",
			},
		},
		{
			Name: "nanosecond_time",
			Type: "datetime",
			DeclaredType: &schema.DeclaredType{
				Base:      "timestamp",
				Arguments: []int{7},
			},
		},
		{
			Name: "offset_timestamp",
			Type: "timestamptz",
			DeclaredType: &schema.DeclaredType{
				Base:      "timestamptz",
				Arguments: []int{6},
			},
		},
		{
			Name: "national_text",
			Type: "text",
			DeclaredType: &schema.DeclaredType{
				Base:      "nvarchar",
				Arguments: []int{80},
			},
		},
		{
			Name: "legacy_datetime",
			Type: "datetime",
			DeclaredType: &schema.DeclaredType{
				Base:      "datetime",
				Arguments: []int{3},
			},
		},
	}
	for _, column := range tests {
		t.Run(column.Name, func(t *testing.T) {
			_, err := projectSQLServerTableForPostgres(
				schema.Table{
					Name:    "items",
					Columns: []schema.Column{column},
				},
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}
