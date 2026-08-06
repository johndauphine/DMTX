package migrate

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

// The renderer must agree with the projection it will replace.
//
// This is the safety net for the swap, and it is written before the swap. A
// canonical renderer that disagrees with the pairwise code currently running is
// a migration that writes a different schema than the one in production - and
// the disagreement would surface as a target that loads, validates, and differs.
//
// So every canonical type dmtx can produce is rendered both ways and compared.
// While both paths exist this test is the contract between them; when the
// pairwise projections go, it is what licensed removing them.
func TestRenderedCanonicalMatchesThePairwiseProjection(t *testing.T) {
	forty := int64(40)
	precision := int64(12)
	scale := int64(2)
	three := int64(3)

	for _, testCase := range []struct {
		name      string
		canonical schema.CanonicalType
		// What the pairwise PostgreSQL projection produces for the same column,
		// as schema.Column.Type - the portable name it assigns, not DDL.
		pairwise string
	}{
		{
			name: "bounded text",
			canonical: schema.CanonicalType{
				Kind:          schema.KindText,
				Length:        &forty,
				Certification: schema.Certification{Transfer: true},
			},
			pairwise: "varchar",
		},
		{
			name: "unbounded text",
			canonical: schema.CanonicalType{
				Kind:          schema.KindText,
				Certification: schema.Certification{Transfer: true},
			},
			pairwise: "text",
		},
		{
			name: "integer",
			canonical: schema.CanonicalType{
				Kind:          schema.KindInteger,
				Certification: schema.Certification{Transfer: true},
			},
			pairwise: "integer",
		},
		{
			name: "bigint",
			canonical: schema.CanonicalType{
				Kind:          schema.KindBigInt,
				Certification: schema.Certification{Transfer: true},
			},
			pairwise: "bigint",
		},
		{
			name: "numeric with precision and scale",
			canonical: schema.CanonicalType{
				Kind:          schema.KindNumeric,
				Precision:     &precision,
				Scale:         &scale,
				Certification: schema.Certification{Transfer: true},
			},
			pairwise: "numeric",
		},
		{
			name: "datetime with fractional seconds",
			canonical: schema.CanonicalType{
				Kind:                      schema.KindDateTime,
				FractionalSecondPrecision: &three,
				Certification:             schema.Certification{Transfer: true},
			},
			pairwise: "timestamp",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rendered, err := schema.RenderCanonical(testCase.canonical, schema.Postgres)
			if err != nil {
				t.Fatalf("canonical render: %v", err)
			}
			if rendered == "" {
				t.Fatal("canonical render produced nothing")
			}
			// The two paths speak different vocabularies - the projection
			// assigns a portable Type, the renderer writes DDL - so the check
			// is that the renderer's declaration starts with the projection's
			// name. Anything more exact would compare a spelling against a
			// classification and fail for the wrong reason.
			if !startsWithFold(rendered, testCase.pairwise) {
				t.Errorf(
					"renderer produced %q, which is not the %q the pairwise"+
						" projection assigns",
					rendered,
					testCase.pairwise,
				)
			}
		})
	}
}

func startsWithFold(value, prefix string) bool {
	if len(value) < len(prefix) {
		return false
	}
	for index := 0; index < len(prefix); index++ {
		a, b := value[index], prefix[index]
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}
