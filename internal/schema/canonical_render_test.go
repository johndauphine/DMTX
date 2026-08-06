package schema

import "testing"

// TestRenderedTextKeepsItsBound is the defect this renderer exists after.
//
// nvarchar(40) became varchar(80) in a target because a byte count was carried
// where a character count belonged. Length is CHARACTERS by the time a type is
// canonical - the converters do whatever byte arithmetic their engine needs -
// so a renderer that widens or doubles it is reintroducing that bug one layer
// further out.
func TestRenderedTextKeepsItsBound(t *testing.T) {
	forty := int64(40)
	bounded := CanonicalType{
		Kind:          KindText,
		Length:        &forty,
		Certification: Certification{Transfer: true},
	}
	for target, want := range map[Dialect]string{
		Postgres: "VARCHAR(40)",
		MySQL:    "VARCHAR(40)",
		SQLite:   "VARCHAR(40)",
	} {
		got, err := RenderCanonical(bounded, target)
		if err != nil {
			t.Fatalf("%s: %v", target, err)
		}
		if got != want {
			t.Errorf("%s rendered %q, want %q", target, got, want)
		}
	}

	// Unbounded stays unbounded. Narrowing it would refuse values the source
	// holds; that is the same error pointing the other way.
	unbounded := CanonicalType{
		Kind:          KindText,
		Certification: Certification{Transfer: true},
	}
	got, err := RenderCanonical(unbounded, Postgres)
	if err != nil {
		t.Fatal(err)
	}
	if got != "TEXT" {
		t.Errorf("unbounded text rendered %q, want TEXT", got)
	}
}

// TestRenderedTemporalKeepsItsPrecision pins the other unit defect.
//
// Absent is not zero. A timestamp declared with no fractional precision is not
// timestamp(0), and rendering it as one truncates every value it carries into
// a target that still loads and still validates.
func TestRenderedTemporalKeepsItsPrecision(t *testing.T) {
	three := int64(3)
	withPrecision := CanonicalType{
		Kind:                      KindDateTime,
		FractionalSecondPrecision: &three,
		Certification:             Certification{Transfer: true},
	}
	absent := CanonicalType{
		Kind:          KindDateTime,
		Certification: Certification{Transfer: true},
	}

	for _, testCase := range []struct {
		target Dialect
		with   string
		none   string
	}{
		{Postgres, "TIMESTAMP(3)", "TIMESTAMP"},
		{SQLServer, "DATETIME2(3)", "DATETIME2"},
		{MySQL, "DATETIME(3)", "DATETIME"},
	} {
		got, err := RenderCanonical(withPrecision, testCase.target)
		if err != nil {
			t.Fatal(err)
		}
		if got != testCase.with {
			t.Errorf("%s rendered %q, want %q", testCase.target, got, testCase.with)
		}
		got, err = RenderCanonical(absent, testCase.target)
		if err != nil {
			t.Fatal(err)
		}
		if got != testCase.none {
			t.Errorf("%s rendered %q for absent, want %q", testCase.target, got, testCase.none)
		}
	}
}

// TestUncertifiedTypesAreRefusedRatherThanGuessed keeps the posture at the one
// place it could leak.
//
// Refuse-unless-certified is the whole stance of this tool. A renderer that
// produced something plausible for a type nobody proved would be the single
// spot where an uncertified value reaches a target anyway.
func TestUncertifiedTypesAreRefusedRatherThanGuessed(t *testing.T) {
	for _, value := range []CanonicalType{
		// Never classified.
		{Kind: KindUnknown, Certification: Certification{Transfer: true}},
		// Classified, never certified.
		{Kind: KindText},
		// The zero value.
		{},
	} {
		if _, err := RenderCanonical(value, Postgres); err == nil {
			t.Errorf("rendered an uncertified type %+v", value)
		}
	}
}

// TestSQLServerTextRendersUnicodeCapable pins the choice that lets dmtx skip
// the national/non-national distinction a general schema tool needs.
//
// The SQL Server target always writes a UTF-8 collation, so varchar holds any
// Unicode character including non-BMP - verified against SQL Server 2022 with
// emoji, CJK and accents. One text type serves both roles, which is why there
// is no National flag on CanonicalType.
func TestSQLServerTextRendersUnicodeCapable(t *testing.T) {
	forty := int64(40)
	for _, value := range []CanonicalType{
		{Kind: KindText, Length: &forty, Certification: Certification{Transfer: true}},
		{Kind: KindText, Certification: Certification{Transfer: true}},
	} {
		got, err := RenderCanonical(value, SQLServer)
		if err != nil {
			t.Fatal(err)
		}
		if got == "" || !containsCollation(got) {
			t.Errorf("SQL Server text rendered %q without a UTF-8 collation", got)
		}
	}
}

func containsCollation(rendered string) bool {
	for index := 0; index+len(sqlServerPortableTextCollation) <= len(rendered); index++ {
		if rendered[index:index+len(sqlServerPortableTextCollation)] ==
			sqlServerPortableTextCollation {
			return true
		}
	}
	return false
}
