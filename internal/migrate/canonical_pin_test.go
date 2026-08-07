package migrate

import (
	"reflect"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

// What the swapped routes actually declare, written out.
//
// The two differential tests in canonical_swap_test.go compared the canonical
// path against the pairwise one. That was the right test to write BEFORE each
// swap and it caught real disagreements. It stopped testing anything the moment
// the swap landed: the projection function it calls IS the canonical path now,
// so both sides of the comparison are the same code and the assertion cannot
// fail.
//
// A double precision column then lost its declaration, and nothing objected -
// not the differential test, which was comparing canonical to itself, and not
// the armed live gate, because the SO2010 corpus has no floating-point column.
// The declaration is required by SQL Server's renderer, so the route produced
// tables that could not be created.
//
// So the expected values are written here instead of derived. A pin is worth
// less than a differential test at swap time and more than one afterwards,
// because it is the only kind that still fails when the single implementation
// changes.
func TestSQLServerToPostgresDeclarations(t *testing.T) {
	three := int64(3)
	precision, scale := int64(12), int64(2)

	for _, testCase := range []struct {
		name         string
		column       schema.Column
		wantType     string
		wantDeclared *schema.DeclaredType
	}{
		{
			name: "nvarchar(40), the DisplayName shape",
			column: schema.Column{
				Name: "DisplayName", Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base: "nvarchar", Arguments: []int{40},
				},
			},
			// Forty CHARACTERS, not eighty. nvarchar declares UTF-16 code units
			// which discovery has already turned into characters, and
			// PostgreSQL's varchar(n) is characters - so the number passes
			// through. Doubling it here is the original defect.
			wantType:     "varchar",
			wantDeclared: &schema.DeclaredType{Base: "varchar", Arguments: []int{40}},
		},
		{
			name: "nvarchar(max), which arrives as unbounded text",
			column: schema.Column{
				Name: "AboutMe", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			// Spelled text rather than varchar with no argument, because a
			// PostgreSQL catalog round trip reports those differently and target
			// authority is authenticated against that catalog.
			wantType: "text", wantDeclared: nil,
		},
		{
			name: "datetime, the CreationDate shape",
			column: schema.Column{
				Name: "CreationDate", Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{3},
					FractionalSecondPrecision: &three,
				},
			},
			wantType:     "timestamp",
			wantDeclared: &schema.DeclaredType{Base: "timestamp", Arguments: []int{3}},
		},
		{
			name: "smalldatetime, whose zero precision is not an absence",
			column: schema.Column{
				Name: "Stamp", Type: "datetime",
				DeclaredType: &schema.DeclaredType{Base: "smalldatetime"},
			},
			// timestamp(0), not a bare timestamp. Absent would let PostgreSQL
			// apply its own six digits, which this source can never fill.
			wantType:     "timestamp",
			wantDeclared: &schema.DeclaredType{Base: "timestamp", Arguments: []int{0}},
		},
		{
			name: "tinyint",
			column: schema.Column{
				Name: "Small", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "tinyint"},
			},
			// Widened to integer, and deliberately: the PostgreSQL target has
			// been writing integer for SQL Server's narrow integers since before
			// the canonical type existed, and target authority is authenticated
			// against the catalogs those runs created.
			wantType: "integer", wantDeclared: nil,
		},
		{
			name: "int",
			column: schema.Column{
				Name: "Id", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "int"},
			},
			wantType: "integer", wantDeclared: nil,
		},
		{
			name: "bigint",
			column: schema.Column{
				Name: "Big", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
			wantType: "bigint", wantDeclared: nil,
		},
		{
			// The type whose absence let the SQL Server side ship broken.
			name: "float, which SQL Server calls double precision",
			column: schema.Column{
				Name: "Score", Type: "double precision",
				DeclaredType: &schema.DeclaredType{Base: "double precision"},
			},
			wantType: "double precision", wantDeclared: nil,
		},
		{
			name: "real, which PostgreSQL does declare",
			column: schema.Column{
				Name: "Ratio", Type: "real",
				DeclaredType: &schema.DeclaredType{Base: "real"},
			},
			wantType: "real", wantDeclared: &schema.DeclaredType{Base: "real"},
		},
		{
			name: "decimal with precision and scale",
			column: schema.Column{
				Name: "Amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base: "decimal", Arguments: []int{12, 2},
					Precision: &precision, Scale: &scale,
				},
			},
			wantType: "numeric",
			wantDeclared: &schema.DeclaredType{
				Base: "numeric", Arguments: []int{12, 2},
				Precision: &precision, Scale: &scale,
			},
		},
		{
			name: "varbinary(16)",
			column: schema.Column{
				Name: "Hash", Type: "blob",
				DeclaredType: &schema.DeclaredType{
					Base: "varbinary", Arguments: []int{16},
				},
			},
			// bytea takes no width, so the bound is lost while the bytes are
			// not - the padding and the length are part of the stored value.
			wantType: "bytea", wantDeclared: nil,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotType, gotDeclared, err :=
				projectSQLServerColumnForPostgres(testCase.column)
			if err != nil {
				t.Fatalf("projection refused the column: %v", err)
			}
			assertProjected(t, gotType, gotDeclared, testCase.wantType, testCase.wantDeclared)
		})
	}
}

// TestPostgresToSQLServerDeclarations is the same pin for the other direction.
func TestPostgresToSQLServerDeclarations(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		column       schema.Column
		wantType     string
		wantDeclared *schema.DeclaredType
	}{
		{
			name:         "text, recorded with no declaration",
			column:       schema.Column{Name: "aboutme", Type: "text"},
			wantType:     "text",
			wantDeclared: &schema.DeclaredType{Base: "text"},
		},
		{
			name:         "uuid, likewise",
			column:       schema.Column{Name: "external_id", Type: "uuid"},
			wantType:     "uuid",
			wantDeclared: &schema.DeclaredType{Base: "uuid"},
		},
		{
			name:         "bytea, likewise",
			column:       schema.Column{Name: "payload", Type: "bytea"},
			wantType:     "blob",
			wantDeclared: &schema.DeclaredType{Base: "blob"},
		},
		{
			name:         "boolean, likewise",
			column:       schema.Column{Name: "enabled", Type: "boolean"},
			wantType:     "boolean",
			wantDeclared: &schema.DeclaredType{Base: "bool"},
		},
		{
			name:         "date, likewise",
			column:       schema.Column{Name: "born", Type: "date"},
			wantType:     "date",
			wantDeclared: &schema.DeclaredType{Base: "date"},
		},
		{
			// The one that shipped broken. PostgreSQL records a double with no
			// declaration and SQL Server's renderer requires one, so a nil here
			// is a table that cannot be created.
			name:         "double precision, recorded with no declaration",
			column:       schema.Column{Name: "score", Type: "double precision"},
			wantType:     "double precision",
			wantDeclared: &schema.DeclaredType{Base: "double precision"},
		},
		{
			name:         "real, which PostgreSQL does declare",
			column:       schema.Column{Name: "ratio", Type: "real"},
			wantType:     "real",
			wantDeclared: &schema.DeclaredType{Base: "real"},
		},
		{
			name: "integer",
			column: schema.Column{
				Name: "id", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "integer"},
			},
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "int"},
		},
		{
			// The character-to-byte widening: 40 characters becomes 160 bytes,
			// because SQL Server's varchar(n) under the UTF-8 collation dmtx
			// writes counts bytes and a character can spend four.
			name: "bounded varchar",
			column: schema.Column{
				Name: "displayname", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
			wantType: "text",
			wantDeclared: &schema.DeclaredType{
				Base: "varchar", Arguments: []int{160},
			},
		},
		{
			// Past what varchar can declare once multiplied, so it widens to
			// varchar(max) rather than truncating to the limit.
			name: "varchar wide enough that four times it will not fit",
			column: schema.Column{
				Name: "body", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{4_000},
				},
			},
			wantType:     "text",
			wantDeclared: &schema.DeclaredType{Base: "text"},
		},
		{
			name: "numeric with precision and scale",
			column: schema.Column{
				Name: "amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base: "numeric", Arguments: []int{12, 2},
				},
			},
			wantType: "numeric",
			wantDeclared: &schema.DeclaredType{
				Base: "decimal", Arguments: []int{12, 2},
			},
		},
		{
			name: "timestamp with fractional seconds",
			column: schema.Column{
				Name: "creationdate", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
			wantType: "datetime",
			wantDeclared: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{6},
			},
		},
		{
			// A bare PostgreSQL timestamp MEANS timestamp(6), and recording it
			// as an absence is what failed the armed gate after the differential
			// test had passed.
			name:     "bare timestamp, which means six digits",
			column:   schema.Column{Name: "seen", Type: "timestamp"},
			wantType: "datetime",
			wantDeclared: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{6},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := projectPostgresColumnForSQLServer(testCase.column)
			if err != nil {
				t.Fatalf("projection refused the column: %v", err)
			}
			assertProjected(
				t, got.Type, got.DeclaredType,
				testCase.wantType, testCase.wantDeclared,
			)
		})
	}
}

func assertProjected(
	t *testing.T,
	gotType string,
	gotDeclared *schema.DeclaredType,
	wantType string,
	wantDeclared *schema.DeclaredType,
) {
	t.Helper()
	if gotType != wantType {
		t.Errorf("portable type = %q, want %q", gotType, wantType)
	}
	if (gotDeclared == nil) != (wantDeclared == nil) {
		t.Fatalf("declaration = %+v, want %+v", gotDeclared, wantDeclared)
	}
	// The WHOLE declaration. Comparing Base and Arguments only is what let the
	// canonical path populate Length with a number the pairwise path left nil,
	// and target authority is authenticated against a catalog that never
	// mentioned it.
	if gotDeclared != nil && !reflect.DeepEqual(*gotDeclared, *wantDeclared) {
		// %#v rather than %+v. A nil Arguments and an empty one print
		// identically under %+v, and they are not the same value to
		// reflect.DeepEqual - which is a failure message that says the two
		// sides are equal while the test says they are not.
		t.Errorf("declaration:\n  got  %#v\n  want %#v", gotDeclared, wantDeclared)
	}
}
