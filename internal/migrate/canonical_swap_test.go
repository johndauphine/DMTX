package migrate

import (
	"reflect"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

// The mssql -> postgres route, both ways, over the same columns.
//
// This runs BEFORE the swap and must keep passing after it. The canonical path
// and the pairwise projection are given identical SQL Server columns and must
// produce identical target types and declarations - because the pairwise code
// is what has been writing production schemas, and a canonical path that
// differs from it is not a refactor, it is a change of behaviour wearing one.
//
// The declarations below use Arguments rather than the structured Length field,
// because that is what SQL Server discovery populates. The first draft of this
// test set Length instead and the pairwise projection refused every text column
// - which is the differential test doing its job on its own fixture: the two
// paths read different fields, and only one of them is the field production
// fills.
//
// The columns below are the SO2010 corpus's actual shapes plus the boundaries
// that have gone wrong on this branch: a bounded national string, an unbounded
// one, and a datetime whose fractional precision is present rather than absent.
func TestCanonicalMatchesPairwiseForSQLServerToPostgres(t *testing.T) {
	three := int64(3)
	precision := int64(12)
	scale := int64(2)

	for _, testCase := range []struct {
		name      string
		column    schema.Column
		isKey     bool
		collation string
	}{
		{
			name: "nvarchar(40), the DisplayName shape",
			column: schema.Column{
				Name: "DisplayName", Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "nvarchar",
					Arguments: []int{40},
				},
			},
			collation: "SQL_Latin1_General_CP1_CI_AS",
		},
		{
			name: "nvarchar at its limit",
			column: schema.Column{
				Name: "Body", Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "nvarchar",
					Arguments: []int{4_000},
				},
			},
			collation: "SQL_Latin1_General_CP1_CI_AS",
		},
		{
			name: "nvarchar(max), which arrives as unbounded text",
			column: schema.Column{
				Name: "AboutMe", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			collation: "SQL_Latin1_General_CP1_CI_AS",
		},
		{
			name: "datetime, the CreationDate shape",
			column: schema.Column{
				Name: "CreationDate", Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:                      "timestamp",
					Arguments:                 []int{3},
					FractionalSecondPrecision: &three,
				},
			},
		},
		{
			name: "int",
			column: schema.Column{
				Name: "Id", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "int"},
			},
		},
		{
			name: "bigint",
			column: schema.Column{
				Name: "Big", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
		},
		{
			name: "decimal with precision and scale",
			column: schema.Column{
				Name: "Amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 2},
					Precision: &precision,
					Scale:     &scale,
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			// The path that has been running.
			pairwiseType, pairwiseDeclared, pairwiseErr :=
				projectSQLServerColumnForPostgres(testCase.column)

			// The path that will replace it.
			canonical, err := schema.CanonicalFromSQLServer(
				testCase.column,
				testCase.collation,
				testCase.isKey,
			)
			if err != nil {
				if pairwiseErr == nil {
					t.Fatalf("canonical refused a column the projection accepted: %v", err)
				}
				return
			}
			canonicalType, canonicalDeclared, err :=
				schema.CanonicalToDeclared(canonical, schema.Postgres)
			if err != nil {
				if pairwiseErr == nil {
					t.Fatalf("canonical refused a column the projection accepted: %v", err)
				}
				return
			}
			if pairwiseErr != nil {
				t.Fatalf(
					"the projection refused this column but canonical accepted it as %s",
					canonicalType,
				)
			}

			if canonicalType != pairwiseType {
				t.Errorf(
					"portable type: canonical %q, pairwise %q",
					canonicalType,
					pairwiseType,
				)
			}
			assertSameDeclaration(t, canonicalDeclared, pairwiseDeclared)
		})
	}
}

// assertSameDeclaration compares the fields that reach a target's catalog.
//
// Arguments and the structured fields both carry the modifier, and different
// code paths populate different ones, so a mismatch in either is a mismatch -
// the length is the number that has already been wrong twice on this branch.
func assertSameDeclaration(t *testing.T, canonical, pairwise *schema.DeclaredType) {
	t.Helper()
	if (canonical == nil) != (pairwise == nil) {
		t.Fatalf("declaration presence differs: canonical %v, pairwise %v",
			canonical, pairwise)
	}
	if canonical == nil {
		return
	}
	// The WHOLE declaration, not the fields that seemed to matter.
	//
	// The first version of this compared Base and Arguments only, and missed
	// the canonical path populating Length with the same number the pairwise
	// path left nil. Target authority is authenticated against a catalog on
	// later incremental runs, so an extra populated field is a change to the
	// recorded shape - and a differential test that looks at a subset of the
	// output is a differential test that proves a subset of the equivalence.
	if !reflect.DeepEqual(canonical, pairwise) {
		t.Errorf("declaration differs:\n  canonical %+v\n  pairwise  %+v",
			canonical, pairwise)
	}
}

// The postgres -> mssql route, both ways, over the same columns.
//
// Written before that swap, and shaped by what the first one taught: it
// compares the whole projected column, because a subset comparison proves a
// subset of the equivalence.
//
// The fixtures include the case that has no analogue on the SQL Server side -
// a column recorded with NO DeclaredType at all, which is how PostgreSQL
// discovery records text, uuid, bytea, json, bool and date.
func TestCanonicalMatchesPairwiseForPostgresToSQLServer(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		column schema.Column
	}{
		{name: "text, recorded with no declaration",
			column: schema.Column{Name: "aboutme", Type: "text"}},
		{name: "uuid, likewise",
			column: schema.Column{Name: "external_id", Type: "uuid"}},
		{name: "bytea, likewise",
			column: schema.Column{Name: "payload", Type: "bytea"}},
		{name: "boolean, likewise",
			column: schema.Column{Name: "enabled", Type: "boolean"}},
		{name: "date, likewise",
			column: schema.Column{Name: "born", Type: "date"}},
		{
			name: "integer",
			column: schema.Column{
				Name: "id", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "integer"},
			},
		},
		{
			name: "bigint",
			column: schema.Column{
				Name: "big", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
		},
		{
			// The character-to-byte widening: 40 characters becomes 160 bytes.
			name: "bounded varchar",
			column: schema.Column{
				Name: "displayname", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "numeric with precision and scale",
			column: schema.Column{
				Name: "amount", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base: "numeric", Arguments: []int{12, 2},
				},
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
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pairwise, pairwiseErr := projectPostgresColumnForSQLServer(testCase.column)

			canonical, err := schema.CanonicalFromPostgres(testCase.column, false)
			if err != nil {
				if pairwiseErr == nil {
					t.Fatalf("canonical refused what the projection accepted: %v", err)
				}
				return
			}
			canonicalType, canonicalDeclared, err :=
				schema.CanonicalToDeclared(canonical, schema.SQLServer)
			if err != nil {
				if pairwiseErr == nil {
					t.Fatalf("canonical refused what the projection accepted: %v", err)
				}
				return
			}
			if pairwiseErr != nil {
				t.Fatalf("the projection refused this but canonical produced %s", canonicalType)
			}
			if canonicalType != pairwise.Type {
				t.Errorf("portable type: canonical %q, pairwise %q",
					canonicalType, pairwise.Type)
			}
			assertSameDeclaration(t, canonicalDeclared, pairwise.DeclaredType)
		})
	}
}
