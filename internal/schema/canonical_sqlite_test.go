package schema

import "testing"

// TestCanonicalFromSQLiteReadsStorageNotSpelling is the fact that makes this
// converter different from the other three.
//
// SQLite's declared type is a hint. A column declared TINYINT holds an
// eight-byte integer, so producing a narrow kind would declare a range on the
// target that the source never enforced - and the first value past 127 would be
// refused by a target that had been told to expect one.
func TestCanonicalFromSQLiteReadsStorageNotSpelling(t *testing.T) {
	for _, base := range []string{
		"int", "integer", "tinyint", "smallint", "mediumint",
		"bigint", "int2", "int8",
	} {
		t.Run(base, func(t *testing.T) {
			canonical, err := CanonicalFromSQLite(Column{
				Name: "n", Type: base, DeclaredType: &DeclaredType{Base: base},
			}, false)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if canonical.Kind != KindBigInt {
				t.Errorf("kind = %s, want %s - SQLite stores eight bytes"+
					" whatever the column was declared as",
					canonical.Kind, KindBigInt)
			}
		})
	}

	// The same argument for floats: SQLite stores every one as an eight-byte
	// IEEE-754 double, so REAL is not binary32 here the way it is on SQL Server.
	for _, base := range []string{"real", "double", "double precision", "float"} {
		t.Run(base, func(t *testing.T) {
			canonical, err := CanonicalFromSQLite(Column{
				Name: "x", Type: base, DeclaredType: &DeclaredType{Base: base},
			}, false)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if canonical.Kind != KindDouble {
				t.Errorf("kind = %s, want %s", canonical.Kind, KindDouble)
			}
		})
	}
}

// TestCanonicalFromSQLiteCharIsCarried records the third distinct answer this
// package gives to the word "char".
//
// PostgreSQL's pads on storage and compares padded, so it is refused. MySQL's
// strips trailing spaces on retrieval, so it is carried to PostgreSQL and
// refused to SQL Server. SQLite's has TEXT affinity and pads nothing at all, so
// the stored value simply is the varchar one.
func TestCanonicalFromSQLiteCharIsCarried(t *testing.T) {
	for _, base := range []string{
		"char", "character", "nchar", "native character",
		"varchar", "varying character", "character varying", "nvarchar",
		"text", "clob",
	} {
		t.Run(base, func(t *testing.T) {
			canonical, err := CanonicalFromSQLite(Column{
				Name: "s", Type: base, DeclaredType: &DeclaredType{Base: base},
			}, false)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if canonical.Kind != KindText {
				t.Errorf("kind = %s, want %s", canonical.Kind, KindText)
			}
		})
	}
}

// TestCanonicalFromSQLiteBinaryIsNotFixed pins the other half of that argument.
//
// SQLite has no fixed-width byte string. BINARY has BLOB affinity and pads
// nothing, so declaring it fixed on the far side would invent padding the source
// never wrote.
func TestCanonicalFromSQLiteBinaryIsNotFixed(t *testing.T) {
	for _, base := range []string{"blob", "binary", "varbinary"} {
		t.Run(base, func(t *testing.T) {
			canonical, err := CanonicalFromSQLite(Column{
				Name: "b", Type: base, DeclaredType: &DeclaredType{Base: base},
			}, false)
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if canonical.Kind != KindBlob {
				t.Errorf("kind = %s, want %s - SQLite pads nothing",
					canonical.Kind, KindBlob)
			}
		})
	}
}

// TestCanonicalFromSQLiteRefusesWhatItCannotDescribe covers the two spellings
// ParseSQLiteDeclaredType admits and this converter will not.
func TestCanonicalFromSQLiteRefusesWhatItCannotDescribe(t *testing.T) {
	for _, testCase := range []struct{ base, why string }{
		{"any", "declares no affinity, so dmtx cannot say what it holds"},
		{"unsigned big int", "names a range SQLite does not have"},
	} {
		t.Run(testCase.base, func(t *testing.T) {
			_, err := CanonicalFromSQLite(Column{
				Name: "c", Type: testCase.base,
				DeclaredType: &DeclaredType{Base: testCase.base},
			}, false)
			if err == nil {
				t.Fatalf("%s was accepted: it %s", testCase.base, testCase.why)
			}
		})
	}
}

// TestCanonicalFromSQLiteModifiers pins the ranges the pairwise projections
// checked and a converter that trusted its input would have dropped.
func TestCanonicalFromSQLiteModifiers(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		base      string
		arguments []int
		refused   bool
		check     func(*testing.T, CanonicalType)
	}{
		{
			name: "varchar carries its length", base: "varchar",
			arguments: []int{40},
			check: func(t *testing.T, value CanonicalType) {
				if value.Length == nil || *value.Length != 40 {
					t.Errorf("length = %v, want 40", value.Length)
				}
			},
		},
		{
			name: "numeric with one argument means an implicit zero scale",
			base: "numeric", arguments: []int{10},
			check: func(t *testing.T, value CanonicalType) {
				if value.Precision == nil || *value.Precision != 10 {
					t.Fatalf("precision = %v, want 10", value.Precision)
				}
				if value.Scale == nil || *value.Scale != 0 {
					t.Errorf("scale = %v, want an explicit 0", value.Scale)
				}
			},
		},
		{
			name: "numeric scale past its precision", base: "numeric",
			arguments: []int{4, 5}, refused: true,
		},
		{
			name: "datetime carries fractional digits", base: "datetime",
			arguments: []int{3},
			check: func(t *testing.T, value CanonicalType) {
				if value.FractionalSecondPrecision == nil ||
					*value.FractionalSecondPrecision != 3 {
					t.Errorf("digits = %v, want 3",
						value.FractionalSecondPrecision)
				}
			},
		},
		{
			name: "datetime past six digits", base: "datetime",
			arguments: []int{7}, refused: true,
		},
		{
			name: "a zero length", base: "varchar",
			arguments: []int{0}, refused: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			canonical, err := CanonicalFromSQLite(Column{
				Name: "c", Type: testCase.base,
				DeclaredType: &DeclaredType{
					Base: testCase.base, Arguments: testCase.arguments,
				},
			}, false)
			if testCase.refused {
				if err == nil {
					t.Fatalf("%s%v was accepted as %+v",
						testCase.base, testCase.arguments, canonical)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			testCase.check(t, canonical)
		})
	}
}

// TestCanonicalFromSQLiteCertifiesOrderingForEveryKey states why this converter
// asks no collation question, unlike SQL Server's and MySQL's.
//
// SQLite has one text comparison - BINARY, bytewise over UTF-8 - unless a column
// names NOCASE or RTRIM, and dmtx does not admit a column that names either. A
// column that reached here already orders by bytes.
func TestCanonicalFromSQLiteCertifiesOrderingForEveryKey(t *testing.T) {
	key := Column{
		Name: "Title", Type: "text",
		DeclaredType: &DeclaredType{Base: "text"},
	}
	canonical, err := CanonicalFromSQLite(key, true)
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if !canonical.CertifiedAsKey() {
		t.Errorf("a text key was not certified to order: %q",
			canonical.Certification.Reason)
	}

	body, err := CanonicalFromSQLite(key, false)
	if err != nil {
		t.Fatalf("refused: %v", err)
	}
	if !body.Certified() {
		t.Error("an ordinary data column was not certified for transfer")
	}
	if body.CertifiedAsKey() {
		t.Error("ordering was granted to a column that was not asked about")
	}
}
