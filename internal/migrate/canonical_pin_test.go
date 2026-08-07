package migrate

import (
	"errors"
	"reflect"
	"strings"
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
			// Arguments only. The source declaration carries the structured
			// Precision and Scale as well, and the projection deliberately does
			// not copy them: PostgreSQL's own discovery records a numeric as
			// Arguments and nothing else, so carrying them would record a shape
			// the target's catalog will never report.
			wantType: "numeric",
			wantDeclared: &schema.DeclaredType{
				Base: "numeric", Arguments: []int{12, 2},
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
		wantRefused  string
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
			// SQL Server's decimal stops at 38 digits. Both swaps dropped that
			// bound and it now lives in the target vocabulary, so the refusal
			// arrives at projection time naming the source column rather than at
			// CREATE TABLE time naming a declaration nobody asked for.
			name: "numeric past 38 digits",
			column: schema.Column{
				Name: "amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base: "numeric", Arguments: []int{39, 2},
				},
			},
			wantRefused: "exceeds what SQL Server can declare",
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
			if testCase.wantRefused != "" {
				if err == nil {
					t.Fatalf("accepted as %s/%+v, want a refusal naming %q",
						got.Type, got.DeclaredType, testCase.wantRefused)
				}
				if !strings.Contains(err.Error(), testCase.wantRefused) {
					t.Errorf("refusal does not name %q: %v",
						testCase.wantRefused, err)
				}
				return
			}
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

// TestMySQLToSQLServerDeclarations pins the third swapped route.
//
// Every accepted shape here was compared against the pairwise projection before
// the swap and agreed with it, whole declaration at a time. The numbers are the
// interesting part: a MySQL modifier is characters for text and bytes for
// binary, and SQL Server's varchar under the UTF-8 collation dmtx writes counts
// bytes while its varbinary already does - so one family multiplies by four and
// the other must not.
func TestMySQLToSQLServerDeclarations(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		column       schema.Column
		wantType     string
		wantDeclared *schema.DeclaredType
		wantRefused  string
	}{
		{
			// Not TINYINT. SQL Server's is unsigned 0-255 and MySQL's is signed,
			// so the narrowest type that holds the source's whole range is
			// SMALLINT.
			name:         "tinyint",
			column:       mySQLColumn("integer", "tinyint"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "smallint"},
		},
		{
			name:         "tinyint(1), which is MySQL's conventional boolean",
			column:       mySQLColumn("integer", "tinyint", 1),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "smallint"},
		},
		{
			name:         "smallint",
			column:       mySQLColumn("integer", "smallint"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "smallint"},
		},
		{
			// MEDIUMINT is 24 bits and nothing else has it, so INTEGER is the
			// narrowest honest home rather than SMALLINT.
			name:         "mediumint",
			column:       mySQLColumn("integer", "mediumint"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "int"},
		},
		{
			name:         "int",
			column:       mySQLColumn("integer", "int"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "int"},
		},
		{
			name:         "bigint",
			column:       mySQLColumn("bigint", "bigint"),
			wantType:     "bigint",
			wantDeclared: &schema.DeclaredType{Base: "bigint"},
		},
		{
			name:         "double",
			column:       mySQLColumn("double precision", "double"),
			wantType:     "double precision",
			wantDeclared: &schema.DeclaredType{Base: "double precision"},
		},
		{
			name:     "decimal, whose two arguments are positional",
			column:   mySQLColumn("numeric", "decimal", 12, 2),
			wantType: "numeric",
			wantDeclared: &schema.DeclaredType{
				Base: "decimal", Arguments: []int{12, 2},
			},
		},
		{
			// Forty characters becomes 160 bytes.
			name:     "varchar(40)",
			column:   mySQLColumn("varchar", "varchar", 40),
			wantType: "text",
			wantDeclared: &schema.DeclaredType{
				Base: "varchar", Arguments: []int{160},
			},
		},
		{
			name:     "varchar(2000), the widest that still fits multiplied",
			column:   mySQLColumn("varchar", "varchar", 2_000),
			wantType: "text",
			wantDeclared: &schema.DeclaredType{
				Base: "varchar", Arguments: []int{8_000},
			},
		},
		{
			// One character wider, and 8004 bytes is past what varchar can
			// declare - so it widens to the MAX form rather than being refused,
			// which is what the pairwise projection did and what made an
			// ordinary MySQL column unmovable.
			name:         "varchar(2001), one past that",
			column:       mySQLColumn("varchar", "varchar", 2_001),
			wantType:     "text",
			wantDeclared: &schema.DeclaredType{Base: "text"},
		},
		{
			name:         "text",
			column:       mySQLColumn("text", "text"),
			wantType:     "text",
			wantDeclared: &schema.DeclaredType{Base: "text"},
		},
		{
			// Bytes, so no multiplication. Multiplying here would declare four
			// times the width for no reason - the mirror of the mistake that
			// halving would be on the text path.
			name:     "binary(16), which stays fixed and stays sixteen",
			column:   mySQLColumn("binary", "binary", 16),
			wantType: "blob",
			wantDeclared: &schema.DeclaredType{
				Base: "binary", Arguments: []int{16},
			},
		},
		{
			name:     "varbinary(16), which stays varying",
			column:   mySQLColumn("varbinary", "varbinary", 16),
			wantType: "blob",
			wantDeclared: &schema.DeclaredType{
				Base: "varbinary", Arguments: []int{16},
			},
		},
		{
			name:         "varbinary(9000), past what SQL Server can declare",
			column:       mySQLColumn("varbinary", "varbinary", 9_000),
			wantType:     "blob",
			wantDeclared: &schema.DeclaredType{Base: "blob"},
		},
		{
			name:         "blob",
			column:       mySQLColumn("blob", "blob"),
			wantType:     "blob",
			wantDeclared: &schema.DeclaredType{Base: "blob"},
		},
		{
			name:         "date",
			column:       mySQLColumn("date", "date"),
			wantType:     "date",
			wantDeclared: &schema.DeclaredType{Base: "date"},
		},
		{
			name:     "datetime(6)",
			column:   mySQLColumn("datetime", "datetime", 6),
			wantType: "datetime",
			wantDeclared: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{6},
			},
		},
		{
			// Zero digits is a precision, not an absence.
			name:     "datetime(0)",
			column:   mySQLColumn("datetime", "datetime", 0),
			wantType: "datetime",
			wantDeclared: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{0},
			},
		},

		// The refusals, each for a reason about VALUES rather than about types.
		{
			name:        "char, which is blank-padded",
			column:      mySQLColumn("char", "char", 10),
			wantRefused: "blank-padding",
		},
		{
			name:        "longtext, which can hold more than VARCHAR(MAX)",
			column:      mySQLColumn("text", "longtext"),
			wantRefused: "LONGTEXT",
		},
		{
			name:        "longblob, likewise",
			column:      mySQLColumn("blob", "longblob"),
			wantRefused: "LONGBLOB",
		},
		{
			// MySQL's TIME runs from -838:59:59 to 838:59:59. SQL Server's is a
			// time of day, so the out-of-range values have nowhere to arrive.
			name:        "time, which is a signed duration",
			column:      mySQLColumn("time", "time", 0),
			wantRefused: "signed durations",
		},
		{
			name:        "json",
			column:      mySQLColumn("json", "json"),
			wantRefused: "JSON storage",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := projectMySQLColumnForSQLServer(testCase.column)
			if testCase.wantRefused != "" {
				if err == nil {
					t.Fatalf("accepted as %s/%+v, want a refusal naming %q",
						got.Type, got.DeclaredType, testCase.wantRefused)
				}
				if !strings.Contains(err.Error(), testCase.wantRefused) {
					t.Errorf("refusal does not name %q: %v",
						testCase.wantRefused, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("projection refused the column: %v", err)
			}
			assertProjected(
				t, got.Type, got.DeclaredType,
				testCase.wantType, testCase.wantDeclared,
			)
			// And the target must be able to create it, which is the check the
			// differential tests never made.
			got.Nullable = true
			if _, err := schema.CreateSQLServerTable(schema.Table{
				Schema: "dbo", Name: "t", Columns: []schema.Column{got},
			}); err != nil {
				t.Fatalf("the target cannot create %s/%+v: %v",
					got.Type, got.DeclaredType, err)
			}
		})
	}
}

func mySQLColumn(portable, base string, arguments ...int) schema.Column {
	declared := &schema.DeclaredType{Base: base}
	if len(arguments) > 0 {
		declared.Arguments = arguments
	}
	return schema.Column{Name: "c", Type: portable, DeclaredType: declared}
}

// TestCanonicalRefusalNamesTheColumn checks the thing an operator needs.
//
// The canonical layer knows the type it could not project and not the column it
// came from, so its refusals read "decimal(39,2) exceeds what SQL Server can
// declare" - true, and no help at all finding which of four hundred columns it
// was. The Operation is left alone: it says which rule was violated, and the
// fail-closed tests assert on it.
func TestCanonicalRefusalNamesTheColumn(t *testing.T) {
	wide := &schema.DeclaredType{Base: "numeric", Arguments: []int{39, 2}}

	t.Run("postgres source", func(t *testing.T) {
		_, err := projectPostgresColumnForSQLServer(schema.Column{
			Name: "Reputation", Type: "numeric", DeclaredType: wide,
		})
		assertRefusalNames(t, err, "Reputation")
	})
	t.Run("mysql source", func(t *testing.T) {
		_, err := projectMySQLColumnForSQLServer(schema.Column{
			Name: "Reputation", Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base: "decimal", Arguments: []int{39, 2},
			},
		})
		assertRefusalNames(t, err, "Reputation")
	})
}

func assertRefusalNames(t *testing.T, err error, columnName string) {
	t.Helper()
	if err == nil {
		t.Fatal("the column was accepted")
	}
	if !strings.Contains(err.Error(), columnName) {
		t.Errorf("refusal does not name %q: %v", columnName, err)
	}
	var policy *schema.PolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("refusal is not a PolicyError: %v", err)
	}
	// The Operation still says which rule was violated, rather than being
	// flattened into a generic wrapper by the renaming.
	if policy.Operation != "project canonical numeric precision" {
		t.Errorf("operation = %q, want the rule that was violated",
			policy.Operation)
	}
}

// TestMySQLToPostgresDeclarations pins the fourth swapped route.
//
// The interesting cases are the two where MySQL and PostgreSQL mean different
// things by the same word, and the family bounds that a first draft of this swap
// silently dropped.
func TestMySQLToPostgresDeclarations(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		column       schema.Column
		wantType     string
		wantDeclared *schema.DeclaredType
		wantRefused  string
	}{
		{
			// MySQL strips trailing spaces from a CHAR when the value is
			// RETRIEVED, so a reader gets back what a varchar would have held.
			// PostgreSQL's bpchar pads on storage and compares padded, and is
			// refused by CanonicalFromPostgres for exactly that reason - one
			// spelling, two engines, opposite answers.
			name:         "char, which MySQL returns unpadded",
			column:       mySQLColumn("char", "char", 10),
			wantType:     "varchar",
			wantDeclared: &schema.DeclaredType{Base: "varchar", Arguments: []int{10}},
		},
		{
			name:         "varchar(40)",
			column:       mySQLColumn("varchar", "varchar", 40),
			wantType:     "varchar",
			wantDeclared: &schema.DeclaredType{Base: "varchar", Arguments: []int{40}},
		},
		{
			name:         "text stays unbounded",
			column:       mySQLColumn("text", "text"),
			wantType:     "text",
			wantDeclared: nil,
		},
		{
			// LONGTEXT is carried here, unlike on the SQL Server target where
			// its capacity exceeds VARCHAR(MAX). PostgreSQL's text has no bound
			// to exceed.
			name:         "longtext, which this target can hold",
			column:       mySQLColumn("text", "longtext"),
			wantType:     "text",
			wantDeclared: nil,
		},
		{
			name:         "tinyint",
			column:       mySQLColumn("integer", "tinyint"),
			wantType:     "integer",
			wantDeclared: nil,
		},
		{
			name:         "bigint",
			column:       mySQLColumn("bigint", "bigint"),
			wantType:     "bigint",
			wantDeclared: nil,
		},
		{
			// Arguments only. PostgreSQL's own discovery records a numeric that
			// way and nothing else, so populating the structured pair would
			// record a shape the target's catalog never reports.
			name:     "decimal",
			column:   mySQLColumn("numeric", "decimal", 12, 2),
			wantType: "numeric",
			wantDeclared: &schema.DeclaredType{
				Base: "numeric", Arguments: []int{12, 2},
			},
		},
		{
			name:         "binary(16), which bytea holds without a width",
			column:       mySQLColumn("binary", "binary", 16),
			wantType:     "bytea",
			wantDeclared: nil,
		},
		{
			name:         "blob",
			column:       mySQLColumn("blob", "blob"),
			wantType:     "bytea",
			wantDeclared: nil,
		},
		{
			name:     "datetime(6)",
			column:   mySQLColumn("datetime", "datetime", 6),
			wantType: "timestamp",
			wantDeclared: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{6},
			},
		},
		{
			// A bare MySQL DATETIME means ZERO fractional digits, where a bare
			// PostgreSQL timestamp means six. Recording an absence would let the
			// target apply its own six, which this source can never fill.
			name:     "bare datetime means zero digits",
			column:   mySQLColumn("datetime", "datetime"),
			wantType: "timestamp",
			wantDeclared: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{0},
			},
		},
		{
			// Carried here, refused on the SQL Server target, which has no JSON
			// type at all.
			name:         "json",
			column:       mySQLColumn("json", "json"),
			wantType:     "json",
			wantDeclared: nil,
		},

		// The bounds a first draft of this swap dropped. Each produced a schema
		// that loads and is not the source's.
		{
			name:        "longtext with a length bounds an unbounded column",
			column:      mySQLColumn("text", "longtext", 100),
			wantRefused: "carries no length",
		},
		{
			name:        "char without a length unbounds a bounded one",
			column:      mySQLColumn("text", "char"),
			wantRefused: "carries a length",
		},
		{
			// One argument where MySQL always reports two. The branch that reads
			// a precision only fires on a pair, so this came out as an unbounded
			// NUMERIC.
			name:        "decimal with no scale",
			column:      mySQLColumn("numeric", "decimal", 12),
			wantRefused: "a precision and a scale",
		},
		{
			name:        "decimal whose scale exceeds its precision",
			column:      mySQLColumn("numeric", "decimal", 4, 5),
			wantRefused: "outside what MySQL can declare",
		},
		{
			// The same gap through the other spelling. A structured Precision
			// with no Scale used to default the scale to zero, which completed
			// a declaration rather than refusing it - the Arguments path had
			// just been fixed for exactly this.
			name: "decimal with a structured precision and no scale",
			column: func() schema.Column {
				precision := int64(12)
				return schema.Column{
					Name: "c", Type: "numeric",
					DeclaredType: &schema.DeclaredType{
						Base: "decimal", Precision: &precision,
					},
				}
			}(),
			wantRefused: "a precision and a scale",
		},
		{
			// MySQL's TIME runs from -838:59:59 to 838:59:59, and PostgreSQL's
			// is a time of day.
			name:        "time, which is a signed duration",
			column:      mySQLColumn("time", "time", 0),
			wantRefused: "signed durations",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gotType, gotDeclared, _, err :=
				projectMySQLColumnForPostgres(testCase.column)
			if testCase.wantRefused != "" {
				if err == nil {
					t.Fatalf("accepted as %s/%+v, want a refusal naming %q",
						gotType, gotDeclared, testCase.wantRefused)
				}
				if !strings.Contains(err.Error(), testCase.wantRefused) {
					t.Errorf("refusal does not name %q: %v",
						testCase.wantRefused, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("projection refused the column: %v", err)
			}
			assertProjected(
				t, gotType, gotDeclared,
				testCase.wantType, testCase.wantDeclared,
			)
			if _, err := schema.CreateTable(schema.Postgres, schema.Table{
				Schema: "public", Name: "t",
				Columns: []schema.Column{{
					Name: "c", Type: gotType,
					DeclaredType: gotDeclared, Nullable: true,
				}},
			}); err != nil {
				t.Fatalf("the target cannot create %s/%+v: %v",
					gotType, gotDeclared, err)
			}
		})
	}
}

// TestSQLServerToMySQLDeclarations pins the fifth swapped route, and the first
// onto a MySQL target.
//
// Every case here matched the pairwise projection before the swap except the
// one marked, which was a defect: SQL Server's BINARY runs to 8000 and MySQL's
// stops at 255, and the projection carried the width under the same keyword.
func TestSQLServerToMySQLDeclarations(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		column       schema.Column
		wantType     string
		wantDeclared *schema.DeclaredType
		wantRefused  string
	}{
		{
			name:         "tinyint keeps a narrow width here",
			column:       mySQLColumn("integer", "tinyint"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "smallint"},
		},
		{
			name:         "int",
			column:       mySQLColumn("integer", "int"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "int"},
		},
		{
			// MySQL has no boolean; BOOLEAN is a synonym for tinyint(1).
			name:         "bool",
			column:       mySQLColumn("boolean", "bool"),
			wantType:     "integer",
			wantDeclared: &schema.DeclaredType{Base: "tinyint", Arguments: []int{1}},
		},
		{
			// Every binary32 value is exactly representable in binary64, so the
			// collapse is lossless for the column. The DEFAULT is what the
			// projection refuses.
			name:         "real collapses into double",
			column:       mySQLColumn("real", "real"),
			wantType:     "double precision",
			wantDeclared: &schema.DeclaredType{Base: "double"},
		},
		{
			// Forty CHARACTERS. nvarchar declares UTF-16 code units, which
			// discovery has already converted to characters, and MySQL's
			// modifier is characters - so the number passes through. The SQL
			// Server TARGET multiplies by four going the other way; doing it
			// here would declare four times what the source can hold.
			name:         "nvarchar(40) passes its length through",
			column:       mySQLColumn("text", "nvarchar", 40),
			wantType:     "varchar",
			wantDeclared: &schema.DeclaredType{Base: "varchar", Arguments: []int{40}},
		},
		{
			name:         "varchar at SQL Server's own ceiling",
			column:       mySQLColumn("text", "varchar", 8_000),
			wantType:     "varchar",
			wantDeclared: &schema.DeclaredType{Base: "varchar", Arguments: []int{8_000}},
		},
		{
			name:         "varchar(max), which arrives as unbounded text",
			column:       mySQLColumn("text", "text"),
			wantType:     "text",
			wantDeclared: &schema.DeclaredType{Base: "longtext"},
		},
		{
			name:         "binary(16) stays fixed",
			column:       mySQLColumn("blob", "binary", 16),
			wantType:     "binary",
			wantDeclared: &schema.DeclaredType{Base: "binary", Arguments: []int{16}},
		},
		{
			// The defect. SQL Server can declare BINARY(300); MySQL's BINARY
			// stops at 255, and the pairwise projection wrote BINARY(300) - DDL
			// MySQL refuses, so the route produced a table nobody could create.
			name:         "binary(300) widens rather than failing at CREATE TABLE",
			column:       mySQLColumn("blob", "binary", 300),
			wantType:     "varbinary",
			wantDeclared: &schema.DeclaredType{Base: "varbinary", Arguments: []int{300}},
		},
		{
			name:         "varbinary(8000), which MySQL can declare",
			column:       mySQLColumn("blob", "varbinary", 8_000),
			wantType:     "varbinary",
			wantDeclared: &schema.DeclaredType{Base: "varbinary", Arguments: []int{8_000}},
		},
		{
			name:         "varbinary(max)",
			column:       mySQLColumn("blob", "blob"),
			wantType:     "blob",
			wantDeclared: &schema.DeclaredType{Base: "longblob"},
		},
		{
			name:     "datetime(3), the CreationDate shape",
			column:   mySQLColumn("datetime", "timestamp", 3),
			wantType: "datetime",
			wantDeclared: &schema.DeclaredType{
				Base: "datetime", Arguments: []int{3},
			},
		},
		{
			// smalldatetime resolves to the minute, and that zero is a
			// precision rather than an absence.
			name:     "smalldatetime",
			column:   mySQLColumn("datetime", "smalldatetime"),
			wantType: "datetime",
			wantDeclared: &schema.DeclaredType{
				Base: "datetime", Arguments: []int{0},
			},
		},
		{
			// MySQL stops at six fractional digits where datetime2 carries
			// seven, so this is not something to render smaller - rendering it
			// as six would truncate every value while producing a schema that
			// loads.
			name:        "datetime2(7), which MySQL cannot hold",
			column:      mySQLColumn("datetime", "timestamp", 7),
			wantRefused: "temporal precision",
		},
		{
			name:         "uniqueidentifier as the canonical text form",
			column:       mySQLColumn("uuid", "uuid"),
			wantType:     "char",
			wantDeclared: &schema.DeclaredType{Base: "char", Arguments: []int{36}},
		},
		{
			// SQL Server cannot declare it, so a source presenting one is a
			// declaration dmtx never read from a catalog. Refused by the
			// CONVERTER, because MySQL's own limit is 65 and a target check
			// alone let this through.
			name:        "decimal(39,2), which SQL Server cannot declare",
			column:      mySQLColumn("numeric", "decimal", 39, 2),
			wantRefused: "which SQL Server cannot declare",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := projectSQLServerColumnForMySQL(testCase.column)
			if testCase.wantRefused != "" {
				if err == nil {
					t.Fatalf("accepted as %s/%+v, want a refusal naming %q",
						got.Type, got.DeclaredType, testCase.wantRefused)
				}
				if !strings.Contains(err.Error(), testCase.wantRefused) {
					t.Errorf("refusal does not name %q: %v",
						testCase.wantRefused, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("projection refused the column: %v", err)
			}
			assertProjected(
				t, got.Type, got.DeclaredType,
				testCase.wantType, testCase.wantDeclared,
			)
			// And MySQL must be able to create it. This is what the pairwise
			// BINARY(300) failed, and no differential test would have asked.
			got.Nullable = true
			if _, err := schema.CreateTable(schema.MySQL, schema.Table{
				Schema: "dmtx", Name: "t", Columns: []schema.Column{got},
			}); err != nil {
				t.Fatalf("the target cannot create %s/%+v: %v",
					got.Type, got.DeclaredType, err)
			}
		})
	}
}
