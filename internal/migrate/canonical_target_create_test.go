package migrate

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

// TestCanonicalColumnsCanBeCreatedOnTheTarget asks the question the differential
// tests cannot: not "does the canonical path agree with the pairwise one" but
// "can the target actually create what came out".
//
// Both are needed and they fail differently. A differential test compares two
// implementations, so it says nothing when its fixtures omit a type - and its
// fixtures omitted double precision, which the canonical path was projecting
// with no declaration at all. SQL Server's renderer refuses a column with no
// declaration, so every floating-point column on that route produced a table
// that could not be created. The armed live gate missed it too: the SO2010
// corpus has no floating-point column, and a corpus proves what it contains.
//
// The types below are every kind a PostgreSQL source can present, so a kind
// added to the lattice without a target vocabulary entry fails here rather than
// at a customer's first float column.
func TestCanonicalColumnsCanBeCreatedOnTheTarget(t *testing.T) {
	six := int64(6)
	precision, scale := int64(12), int64(2)

	for _, column := range []schema.Column{
		{Name: "flag", Type: "boolean"},
		{Name: "n", Type: "integer"},
		{Name: "big", Type: "bigint"},
		{Name: "score", Type: "double precision"},
		{Name: "ratio", Type: "real"},
		{
			Name: "amount", Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base: "numeric", Arguments: []int{12, 2},
				Precision: &precision, Scale: &scale,
			},
		},
		{
			Name: "title", Type: "varchar",
			DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{40}},
		},
		{Name: "body", Type: "text"},
		{Name: "payload", Type: "bytea"},
		{Name: "day", Type: "date"},
		{
			Name: "created", Type: "timestamp",
			DeclaredType: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{6},
				FractionalSecondPrecision: &six,
			},
		},
		{Name: "id", Type: "uuid"},
	} {
		t.Run(column.Name+" "+column.Type, func(t *testing.T) {
			projected, err := projectPostgresColumnForSQLServer(column)
			if err != nil {
				t.Fatalf("projection refused an ordinary PostgreSQL column: %v", err)
			}
			projected.Nullable = true
			if _, err := schema.CreateSQLServerTable(schema.Table{
				Schema:  "dbo",
				Name:    "canonical_target_check",
				Columns: []schema.Column{projected},
			}); err != nil {
				t.Fatalf(
					"the target cannot create %s, projected as %s/%+v: %v",
					column.Type, projected.Type, projected.DeclaredType, err,
				)
			}
		})
	}
}
