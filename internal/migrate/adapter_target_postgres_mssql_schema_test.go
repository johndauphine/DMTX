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
			typ: "varchar",
			declaration: &schema.DeclaredType{
				Base:      "varchar",
				Arguments: []int{80},
			},
		},
		{typ: "bytea"},
		{
			typ: "timestamp",
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
			typ: "timestamp",
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

// TestProjectSQLServerNationalTextForPostgres pins the certification that
// national text carries.
//
// nvarchar and nchar used to be refused here and in discovery. Both are now
// certified for transfer - the evidence is a live round trip of emoji, CJK and
// accented text through a real pair of engines - so the projection has to
// produce a target column that holds them.
//
// The length matters as much as the type. Discovery converts
// sys.columns.max_length from bytes to characters for the national spellings,
// so an nvarchar(40) arrives here as 40 and must leave as varchar(40). Passing
// the byte count through would declare varchar(80): a target that still loads,
// still validates, and is silently wrong.
func TestProjectSQLServerNationalTextForPostgres(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		declared *schema.DeclaredType
		wantType string
		wantBase string
		wantArgs []int
	}{
		{
			name:     "nvarchar keeps its character length",
			declared: &schema.DeclaredType{Base: "nvarchar", Arguments: []int{40}},
			wantType: "varchar",
			wantBase: "varchar",
			wantArgs: []int{40},
		},
		{
			name:     "nchar keeps its character length",
			declared: &schema.DeclaredType{Base: "nchar", Arguments: []int{10}},
			wantType: "varchar",
			wantBase: "varchar",
			wantArgs: []int{10},
		},
		{
			name:     "nvarchar(max) becomes text",
			declared: &schema.DeclaredType{Base: "text"},
			wantType: "text",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			projected, err := projectSQLServerTableForPostgres(schema.Table{
				Name: "items",
				Columns: []schema.Column{{
					Name:         "c",
					Type:         "text",
					DeclaredType: testCase.declared,
				}},
			})
			if err != nil {
				t.Fatalf("certified national text was refused: %v", err)
			}
			column := projected.Columns[0]
			if column.Type != testCase.wantType {
				t.Errorf("type = %q, want %q", column.Type, testCase.wantType)
			}
			if testCase.wantBase == "" {
				return
			}
			if column.DeclaredType == nil ||
				column.DeclaredType.Base != testCase.wantBase {
				t.Fatalf("declared = %+v, want base %q", column.DeclaredType, testCase.wantBase)
			}
			if len(column.DeclaredType.Arguments) != len(testCase.wantArgs) {
				t.Fatalf("arguments = %v, want %v", column.DeclaredType.Arguments, testCase.wantArgs)
			}
			for index, want := range testCase.wantArgs {
				if column.DeclaredType.Arguments[index] != want {
					t.Fatalf("arguments = %v, want %v", column.DeclaredType.Arguments, testCase.wantArgs)
				}
			}
		})
	}
}

// TestProjectedNationalTextRefusesAnImpossibleLength keeps this projection
// fail-closed on a declaration SQL Server cannot produce.
//
// nvarchar caps at 4000 UTF-16 code units. The projection accepted up to 8000
// for every family, so an nvarchar(8000) - which cannot exist - was refused by
// discovery and by the retained row bound and accepted here. Three encodings of
// one limit, and the odd one out was the one nothing tested.
func TestProjectedNationalTextRefusesAnImpossibleLength(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		base     string
		length   int
		accepted bool
	}{
		{name: "nvarchar at its limit", base: "nvarchar", length: 4_000, accepted: true},
		{name: "nvarchar past its limit", base: "nvarchar", length: 4_001},
		{name: "nchar past its limit", base: "nchar", length: 4_001},
		{name: "varchar at its limit", base: "varchar", length: 8_000, accepted: true},
		{name: "varchar past its limit", base: "varchar", length: 8_001},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := projectSQLServerTableForPostgres(schema.Table{
				Name: "items",
				Columns: []schema.Column{{
					Name: "c",
					Type: "text",
					DeclaredType: &schema.DeclaredType{
						Base:      testCase.base,
						Arguments: []int{testCase.length},
					},
				}},
			})
			if testCase.accepted && err != nil {
				t.Fatalf("%s(%d) was refused: %v", testCase.base, testCase.length, err)
			}
			if !testCase.accepted && err == nil {
				t.Fatalf(
					"%s(%d) was accepted; SQL Server cannot declare it",
					testCase.base,
					testCase.length,
				)
			}
		})
	}
}
