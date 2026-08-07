package schema

import (
	"strings"
	"testing"
)

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

// TestRenderBinaryRespectsEachFamilysOwnCap covers the two engines whose binary
// families do not share a limit.
//
// MySQL's BINARY stops at 255 and its VARBINARY runs to 65535. SQL Server's both
// stop at 8000. A single shared limit rendered BINARY(8000) - valid on SQL
// Server, rejected by MySQL - which is the same shape of mistake as declaring a
// character length in bytes: one number, two engines, and only the engine says
// which bound applies.
func TestRenderBinaryRespectsEachFamilysOwnCap(t *testing.T) {
	fixed := func(length int64) CanonicalType {
		return CanonicalType{
			Kind: KindBinary, Length: &length,
			Certification: Certification{Transfer: true},
		}
	}
	varying := func(length int64) CanonicalType {
		return CanonicalType{
			Kind: KindBlob, Length: &length,
			Certification: Certification{Transfer: true},
		}
	}

	for _, testCase := range []struct {
		name   string
		value  CanonicalType
		target Dialect
		want   string
	}{
		{"MySQL keeps a short fixed width", fixed(16), MySQL, "BINARY(16)"},
		{"MySQL at its fixed cap", fixed(255), MySQL, "BINARY(255)"},
		{
			// Past BINARY's cap it widens to VARBINARY at the same width rather
			// than being rendered as DDL MySQL would reject.
			"MySQL past its fixed cap widens", fixed(256), MySQL,
			"VARBINARY(256)",
		},
		{
			"MySQL at a width only SQL Server could declare fixed",
			fixed(8_000), MySQL, "VARBINARY(8000)",
		},
		{"MySQL varying at its cap", varying(65_535), MySQL, "VARBINARY(65535)"},
		{"MySQL past every cap", varying(65_536), MySQL, "LONGBLOB"},

		{"SQL Server keeps a fixed width", fixed(8_000), SQLServer, "BINARY(8000)"},
		{"SQL Server varying", varying(16), SQLServer, "VARBINARY(16)"},
		{"SQL Server past its cap", varying(8_001), SQLServer, "VARBINARY(MAX)"},

		// PostgreSQL's bytea takes no width at all, so both families collapse.
		{"PostgreSQL has one spelling", fixed(16), Postgres, "BYTEA"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := RenderCanonical(testCase.value, testCase.target)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if got != testCase.want {
				t.Errorf("rendered %q, want %q", got, testCase.want)
			}
		})
	}
}

// TestRenderedSmallIntAgreesWithTheProjection is the invariant, not an example.
//
// The DDL creates the catalog that CanonicalToDeclared's declaration is later
// authenticated against, so a target where the two disagree is a target that
// fails its own incremental run. The renderer said SMALLINT for PostgreSQL while
// the projection recorded integer.
func TestRenderedSmallIntAgreesWithTheProjection(t *testing.T) {
	value := CanonicalType{
		Kind:          KindSmallInt,
		Certification: Certification{Transfer: true},
	}
	for _, target := range []Dialect{Postgres, SQLServer, MySQL, SQLite} {
		t.Run(string(target), func(t *testing.T) {
			rendered, err := RenderCanonical(value, target)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			portable, declared, err := CanonicalToDeclared(value, target)
			if err != nil {
				t.Fatalf("project: %v", err)
			}
			narrow := strings.Contains(strings.ToUpper(rendered), "SMALLINT")
			recordedNarrow := portable == "smallint" ||
				declared != nil && strings.Contains(
					strings.ToLower(declared.Base), "smallint",
				)
			if narrow != recordedNarrow {
				t.Errorf(
					"DDL says %q but the recorded authority is %q/%+v -"+
						" the catalog created will not match what is checked",
					rendered, portable, declared,
				)
			}
		})
	}
}
