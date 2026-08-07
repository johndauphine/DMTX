package schema

import (
	"fmt"
	"strings"
)

// PostgreSQL's declarations, turned into canonical types.
//
// Two things differ from the SQL Server converter and both were checked against
// internal/engine/postgres_source_discovery.go rather than assumed - assuming
// is what refused every smalldatetime column on the previous route.
//
// A PostgreSQL column often carries NO DeclaredType at all. text, uuid, bytea,
// json, jsonb, bool and date are recorded as a portable type and nothing else,
// because the catalog adds nothing to the name. So a nil declaration is the
// ordinary case here, not a fault.
//
// And the portable name is the target's own vocabulary: a bounded string is
// varchar and an unbounded one is text, where SQL Server calls both text. The
// bound lives in Length rather than in the name.

// CanonicalFromPostgres turns a discovered PostgreSQL column into a canonical
// type.
//
// Ordering certification is granted for every certified column rather than
// asked about a collation, and that is a real difference from the other
// engines rather than an omission. dmtx admits a PostgreSQL source only under
// a byte-ordered collation - see the C-collation checks in
// internal/schema/postgres_check_signature.go - so a column that reached here
// has already been established to order by bytes. There is no per-column
// collation left to interrogate.
func CanonicalFromPostgres(column Column, isKey bool) (CanonicalType, error) {
	canonical := CanonicalType{
		Kind:          canonicalKindFromPortable(column.Type),
		Certification: Certification{Transfer: true},
	}
	if canonical.Kind == KindUnknown {
		return CanonicalType{}, fmt.Errorf(
			"PostgreSQL column %s has an unclassified type %q",
			column.Name,
			column.Type,
		)
	}

	// A nil declaration is ordinary - the portable name is the whole fact for
	// the types PostgreSQL records without a modifier - but it is not the whole
	// fact for a temporal one, so the defaults below are applied either way.
	//
	// Returning early here was a real defect and only the armed live gate found
	// it: a bare timestamp reached the SQL Server target with no precision, and
	// its renderer refuses that. The corpus column is badges.date, which is a
	// TIMESTAMP, so the refusal read "date timestamp" and looked like a type
	// confusion rather than a missing modifier.
	if column.DeclaredType == nil {
		applyPostgresTemporalDefault(&canonical)
		if isKey {
			canonical.Certification.Ordering = true
		}
		return canonical, nil
	}

	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	portable := strings.ToLower(strings.TrimSpace(column.Type))
	// The declared base must agree with the portable name. The pairwise
	// projection refused a column where they differed, on the reasoning that a
	// PostgreSQL catalog reports one type per column and a disagreement means
	// something upstream synthesised the pair rather than reading it.
	if base != portable {
		return CanonicalType{}, fmt.Errorf(
			"PostgreSQL column %s declares %q but is typed %q",
			column.Name,
			base,
			portable,
		)
	}

	canonical.Precision = column.DeclaredType.Precision
	canonical.Scale = column.DeclaredType.Scale
	canonical.FractionalSecondPrecision =
		column.DeclaredType.FractionalSecondPrecision

	switch canonical.Kind {
	case KindNumeric:
		if canonical.Precision == nil && len(column.DeclaredType.Arguments) == 2 {
			precision := int64(column.DeclaredType.Arguments[0])
			scale := int64(column.DeclaredType.Arguments[1])
			canonical.Precision = &precision
			canonical.Scale = &scale
		}
	case KindText, KindBlob:
		if length, ok := declaredModifier(column.DeclaredType); ok {
			bounded := length
			canonical.Length = &bounded
		}
	case KindTime, KindDateTime:
		if canonical.FractionalSecondPrecision == nil {
			if digits, ok := declaredModifier(column.DeclaredType); ok {
				precision := digits
				canonical.FractionalSecondPrecision = &precision
			}
		}
		applyPostgresTemporalDefault(&canonical)
	}

	if isKey {
		canonical.Certification.Ordering = true
	}
	return canonical, nil
}

// applyPostgresTemporalDefault records the precision a bare temporal means.
//
// A PostgreSQL timestamp with no modifier IS timestamp(6). Microseconds are the
// type's definition rather than a default a target may reinterpret, so the
// canonical type carries six rather than an absence - and a target that would
// have chosen differently, or refused, never gets the chance to.
//
// Applied on both paths through the converter, because PostgreSQL records some
// temporals with a declaration and some without, and the meaning is the same
// either way. Applying it on one path only is what let a bare timestamp reach
// the SQL Server renderer unmodified.
func applyPostgresTemporalDefault(canonical *CanonicalType) {
	switch canonical.Kind {
	case KindTime, KindDateTime:
		if canonical.FractionalSecondPrecision == nil {
			microseconds := int64(6)
			canonical.FractionalSecondPrecision = &microseconds
		}
	}
}
