package schema

import "testing"

// TestSQLServerKeyCollationsMatchWhatWasMeasured pins the ordering rule at its
// new single home.
//
// Every case here was measured against SQL Server 2022, and two of the three
// refusals are near misses that read as safe - which is why they are asserted
// rather than trusted to a reader's intuition about collation names.
func TestSQLServerKeyCollationsMatchWhatWasMeasured(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		base      string
		collation string
		certified bool
	}{
		{
			name: "narrow BIN2_UTF8 is the one that works",
			base: "varchar", collation: "Latin1_General_100_BIN2_UTF8",
			certified: true,
		},
		{
			// BIN2 ignores the locale prefixing it, so every _BIN2_UTF8 is
			// equally safe and pinning one name would refuse the rest.
			name: "any locale, so long as it is BIN2_UTF8",
			base: "char", collation: "Japanese_XJIS_140_BIN2_UTF8",
			certified: true,
		},
		{
			// Binary, and still wrong: it orders by CP1252 bytes, so
			// [EUR, y-diaeresis] where UTF-8 gives the reverse.
			name: "narrow BIN2 orders by code page",
			base: "varchar", collation: "Latin1_General_100_BIN2",
		},
		{
			name: "case-insensitive",
			base: "varchar", collation: "SQL_Latin1_General_CP1_CI_AS",
		},
		{
			// UTF-16 code-unit order agrees with PostgreSQL across the BMP and
			// stops above it, where surrogates sort before what they encode.
			name: "national types have no safe spelling",
			base: "nvarchar", collation: "Latin1_General_100_BIN2",
		},
		{
			// _UTF8 re-encodes char and varchar only, so it does not rescue a
			// national key - and an operator who tried it deserves to be told.
			name: "and _UTF8 does not rescue them",
			base: "nvarchar", collation: "Latin1_General_100_BIN2_UTF8",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got := SQLServerTextKeyCollationCertified(testCase.base, testCase.collation)
			if got != testCase.certified {
				t.Errorf(
					"%s under %s: certified=%v, want %v",
					testCase.base, testCase.collation, got, testCase.certified,
				)
			}
		})
	}
}

// TestSQLServerTextLimitsAreOneFact is the point of moving them here.
//
// This limit lived in three packages at once - discovery, the retained row
// bound, and the PostgreSQL projection - and the third accepted an
// nvarchar(8000) that SQL Server cannot declare. The copies are gone; this is
// the fact.
func TestSQLServerTextLimitsAreOneFact(t *testing.T) {
	for base, want := range map[string]int64{
		"varchar":  8_000,
		"char":     8_000,
		"nvarchar": 4_000,
		"nchar":    4_000,
	} {
		if got := SQLServerTextLengthLimit(base); got != want {
			t.Errorf("%s limit = %d, want %d", base, got, want)
		}
	}
}

// TestCanonicalFromSQLServerAsksOrderingOnlyOfKeys is the separation the whole
// canonical type exists for.
func TestCanonicalFromSQLServerAsksOrderingOnlyOfKeys(t *testing.T) {
	forty := int64(40)
	body := Column{
		Name:         "AboutMe",
		Type:         "text",
		DeclaredType: &DeclaredType{Base: "nvarchar", Length: &forty},
	}

	// As data, under the collation SQL Server installs by default. This is the
	// column whose refusal made StackOverflow unmigratable.
	canonical, err := CanonicalFromSQLServer(body, "SQL_Latin1_General_CP1_CI_AS", false)
	if err != nil {
		t.Fatalf("an ordinary data column was refused: %v", err)
	}
	if !canonical.Certified() {
		t.Error("an ordinary data column was not certified for transfer")
	}
	if canonical.CertifiedAsKey() {
		t.Error("a data column was certified for ordering without being asked")
	}

	// The same column as a key is refused, with a remedy rather than a verdict.
	canonical, err = CanonicalFromSQLServer(body, "SQL_Latin1_General_CP1_CI_AS", true)
	if err != nil {
		t.Fatalf("a key should be refused by certification, not by error: %v", err)
	}
	if canonical.CertifiedAsKey() {
		t.Error("a case-insensitive key was certified")
	}
	if canonical.Certification.Reason == "" {
		t.Error("a refused key was given no remedy")
	}
	// It still transfers. A key that cannot order is not a value that cannot move.
	if !canonical.Certified() {
		t.Error("a refused key lost its transfer certification")
	}
}

// TestCanonicalFromSQLServerRefusesAnImpossibleLength keeps the converter
// fail-closed on a declaration the engine cannot produce.
func TestCanonicalFromSQLServerRefusesAnImpossibleLength(t *testing.T) {
	tooLong := int64(4_001)
	column := Column{
		Name:         "c",
		Type:         "text",
		DeclaredType: &DeclaredType{Base: "nvarchar", Length: &tooLong},
	}
	if _, err := CanonicalFromSQLServer(column, "Latin1_General_100_BIN2_UTF8", false); err == nil {
		t.Fatal("nvarchar(4001) was accepted; SQL Server cannot declare it")
	}

	// The same number is legal for the narrow family, which is the whole reason
	// the limit cannot be one constant.
	narrow := Column{
		Name:         "c",
		Type:         "text",
		DeclaredType: &DeclaredType{Base: "varchar", Length: &tooLong},
	}
	if _, err := CanonicalFromSQLServer(narrow, "Latin1_General_100_BIN2_UTF8", false); err != nil {
		t.Fatalf("varchar(4001) was refused: %v", err)
	}
}
