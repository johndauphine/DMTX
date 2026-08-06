package schema

import "testing"

// TestCertificationSeparatesTransferFromOrdering pins the distinction the whole
// canonical type exists to hold.
//
// Conflating them is the defect this work follows. SQL Server discovery
// required a binary collation of every text column, not just of keys, so an
// ordinary StackOverflow table could not be read - and the property being
// protected, that a chunk boundary means the same thing on both engines, is
// meaningless for a column nothing is ordered by.
func TestCertificationSeparatesTransferFromOrdering(t *testing.T) {
	// A body of HTML under a case-insensitive collation. It transfers exactly;
	// it orders differently on the two engines; nothing orders by it.
	body := CanonicalType{
		Kind:          KindText,
		Certification: Certification{Transfer: true},
	}
	if !body.Certified() {
		t.Error("a transferable column was refused")
	}
	if body.CertifiedAsKey() {
		t.Error("a column with no ordering certification was accepted as a key")
	}

	// The same kind, on a column that orders a paged read.
	key := CanonicalType{
		Kind:          KindText,
		Certification: Certification{Transfer: true, Ordering: true},
	}
	if !key.CertifiedAsKey() {
		t.Error("a portable key was refused")
	}

	// Ordering without transfer is not a state that means anything: a value
	// that does not arrive cannot be ordered on arrival. Certified() gates on
	// transfer, so CertifiedAsKey cannot be reached without it.
	incoherent := CanonicalType{
		Kind:          KindText,
		Certification: Certification{Ordering: true},
	}
	if incoherent.Certified() || incoherent.CertifiedAsKey() {
		t.Error("a column that does not transfer was certified")
	}
}

// TestUnknownKindIsNeverCertified keeps the zero value fail-closed.
//
// A type reaching a renderer as Unknown is one discovery admitted without
// classifying. That is a bug rather than a shape to render, and it must not
// become a silently-empty column definition in somebody's target.
func TestUnknownKindIsNeverCertified(t *testing.T) {
	zero := CanonicalType{}
	if zero.Certified() || zero.CertifiedAsKey() {
		t.Fatal("the zero value certified itself")
	}
	// Even asserted certifications do not rescue it: the kind is the thing
	// missing, and no renderer can act on it.
	claimed := CanonicalType{
		Certification: Certification{Transfer: true, Ordering: true},
	}
	if claimed.Certified() || claimed.CertifiedAsKey() {
		t.Fatal("an unclassified type certified itself")
	}
}

// TestModifiersDistinguishAbsentFromZero is why they are pointers.
//
// A NUMERIC with no precision is not NUMERIC(0), and a timestamp with no
// fractional precision is not timestamp(0). Collapsing the two is how a
// datetime loses its milliseconds on the way to a target - a value that still
// loads, still validates, and is wrong.
func TestModifiersDistinguishAbsentFromZero(t *testing.T) {
	zero := int64(0)
	absent := CanonicalType{Kind: KindDateTime}
	explicit := CanonicalType{Kind: KindDateTime, FractionalSecondPrecision: &zero}

	if absent.FractionalSecondPrecision != nil {
		t.Error("an unspecified precision is not absent")
	}
	if explicit.FractionalSecondPrecision == nil {
		t.Fatal("an explicit zero precision was lost")
	}
	if *explicit.FractionalSecondPrecision != 0 {
		t.Error("an explicit zero precision changed value")
	}
}
