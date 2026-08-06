package schema

import (
	"fmt"
	"strings"
)

// SQL Server's declarations, turned into canonical types.
//
// This file is where the facts about SQL Server text live. They used to live in
// three places at once - a length limit in discovery, another in the retained
// row bound, a third in the PostgreSQL projection - and the third was wrong,
// accepting an nvarchar(8000) that SQL Server cannot declare. Three copies is
// what a pairwise projection costs; one home is what this is for.

// SQL Server's declarable text lengths.
//
// The unit differs by family and saying so plainly matters more than brevity,
// because unit confusion here has caused two separate defects - a doubled
// length in a target's DDL, and an under-counted memory bound.
//
//	char, varchar      n is BYTES, capped at 8000. Under a _UTF8 collation a
//	                   multi-byte character spends several of them.
//	nchar, nvarchar    n is UTF-16 CODE UNITS, capped at 4000, which is the
//	                   same 8000 bytes of storage.
//
// Beyond either cap SQL Server requires MAX, and the column arrives as
// unbounded text instead of a bounded one.
const (
	SQLServerNarrowTextLimit   = 8_000
	SQLServerNationalTextLimit = 4_000
)

// SQLServerTextLengthLimit is the largest length the named family may declare.
func SQLServerTextLengthLimit(base string) int64 {
	if SQLServerNationalText(base) {
		return SQLServerNationalTextLimit
	}
	return SQLServerNarrowTextLimit
}

// SQLServerNationalText reports whether a base is one of the UTF-16 spellings.
func SQLServerNationalText(base string) bool {
	switch strings.ToLower(strings.TrimSpace(base)) {
	case "nchar", "nvarchar":
		return true
	default:
		return false
	}
}

// SQLServerTextTransferBytes is the worst-case UTF-8 bytes per declared unit.
//
// A memory bound built on a declared length must not be optimistic: under-
// counting hands a reader more rows per chunk than the budget allows, and the
// ceiling stops meaning anything.
//
// A national unit is a UTF-16 code unit, and one inside the BMP costs up to
// three UTF-8 bytes. A surrogate pair costs four across two units, which is
// cheaper per unit, so three is the worst case rather than four.
//
// The narrow families declare bytes, and those bytes are UTF-8 only under a
// _UTF8 collation; under any other they are code-page bytes that widen on the
// way out. That is a known gap rather than a claim - see task #43 - and it is
// left at 1 here so this move does not silently change a bound whose aggregate
// nobody has yet explained.
func SQLServerTextTransferBytes(base string) int64 {
	if SQLServerNationalText(base) {
		return 3
	}
	return 1
}

// SQLServerTextKeyCollationCertified reports whether a text column under this
// collation may order a paged read.
//
// Measured against SQL Server 2022 rather than argued from the collation's
// name, because two spellings that read as safe are not:
//
//	varchar  COLLATE Latin1_General_100_BIN2       orders [EUR, y-diaeresis]
//	varchar  COLLATE Latin1_General_100_BIN2_UTF8  orders [y-diaeresis, EUR]
//	PostgreSQL                                     orders [y-diaeresis, EUR]
//
// A narrow _BIN2 is a binary *code page* collation, so it orders by CP1252
// bytes where EUR is 0x80 and y-diaeresis is 0xFF; transcoded to UTF-8 those
// are U+20AC and U+00FF, the other way round. Byte ordering is not portable
// because it is binary. It is portable when the bytes are the same bytes.
//
//	nvarchar COLLATE Latin1_General_100_BIN2  orders [U+1F389, U+FFFD]
//	PostgreSQL                                orders [U+FFFD, U+1F389]
//
// The national families have no safe spelling at all. They are UTF-16, and SQL
// Server's _UTF8 collations change the encoding of char and varchar only, so a
// national key always orders by UTF-16 code unit - which agrees with PostgreSQL
// across the BMP and stops agreeing above it, where surrogates occupy
// D800-DFFF while the characters they encode live at U+10000 and up. One emoji
// in a key column moves a chunk boundary.
//
// Matched by suffix rather than one full name because BIN2 ignores the locale
// prefixing it and _UTF8 fixes the encoding, so every _BIN2_UTF8 collation is
// equally safe and pinning one would refuse the others for no reason.
func SQLServerTextKeyCollationCertified(base, collation string) bool {
	if SQLServerNationalText(base) {
		return false
	}
	return strings.HasSuffix(
		strings.ToUpper(strings.TrimSpace(collation)),
		"_BIN2_UTF8",
	)
}

// SQLServerKeyCollationRemedy says what a refused text key would need.
//
// "A binary collation" was the old wording and it was actively misleading:
// Latin1_General_100_BIN2 is a binary collation and is refused, so an operator
// read the message, looked at their column, and had nowhere to go. The two ways
// a text key fails have different remedies and neither is guessable.
func SQLServerKeyCollationRemedy(base string) string {
	if SQLServerNationalText(base) {
		return "a national text key cannot order the same way on both engines" +
			" whatever its collation, because nchar and nvarchar are UTF-16;" +
			" declare the key as varchar with a _BIN2_UTF8 collation, or key" +
			" the table on a non-text column"
	}
	return "a text key must carry a _BIN2_UTF8 collation, which is the only" +
		" kind whose byte ordering matches the target's; a plain _BIN2 orders" +
		" by code page, so it sorts the same bytes differently once they are" +
		" UTF-8"
}

// CanonicalFromSQLServer turns a discovered SQL Server column into a canonical
// type, certified for what it can actually do.
//
// isKey decides which certification is asked, not which is granted. A column
// that cannot order portably is still perfectly transferable, and refusing it
// on that basis is precisely what made Users.AboutMe - a body of HTML that
// nothing sorts by - unreadable.
func CanonicalFromSQLServer(
	column Column,
	collation string,
	isKey bool,
) (CanonicalType, error) {
	if column.DeclaredType == nil {
		return CanonicalType{}, fmt.Errorf(
			"SQL Server column %s has no declared type",
			column.Name,
		)
	}
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))

	canonical := CanonicalType{
		Kind:          canonicalKindFromPortable(column.Type),
		Certification: Certification{Transfer: true},
	}
	if canonical.Kind == KindUnknown {
		return CanonicalType{}, fmt.Errorf(
			"SQL Server column %s has an unclassified type %q",
			column.Name,
			column.Type,
		)
	}
	// The declared base has to be one this converter recognises for that kind,
	// not merely present. Classifying from column.Type alone would accept a
	// blob declared as SQL Server's deprecated image, or a datetime declared as
	// datetime rather than the timestamp discovery emits - spellings that reach
	// here only when something upstream is already wrong, and that a permissive
	// converter would pass through into a target.
	if !sqlServerBaseKnown(base, canonical.Kind) {
		return CanonicalType{}, fmt.Errorf(
			"SQL Server column %s declares %q, which is not a base dmtx"+
				" certifies for %s",
			column.Name,
			base,
			canonical.Kind,
		)
	}

	// The single positional argument means different things by kind - a length
	// for text, a fractional-second precision for a temporal - so it is read
	// into the field that matches rather than into Length for everything. An
	// earlier draft did the latter, which quietly gave every timestamp a
	// character length of three.
	canonical.Precision = column.DeclaredType.Precision
	canonical.Scale = column.DeclaredType.Scale
	canonical.FractionalSecondPrecision =
		column.DeclaredType.FractionalSecondPrecision

	switch canonical.Kind {
	case KindNumeric:
		// decimal declares two positional arguments, and discovery writes them
		// there rather than into the structured Precision and Scale. Reading
		// only the structured pair left a numeric with no modifiers at all,
		// which the PostgreSQL renderer refuses - found by the armed live gate
		// rather than by a unit test, because the unit fixtures had been
		// written with the structured spelling.
		if canonical.Precision == nil && len(column.DeclaredType.Arguments) == 2 {
			precision := int64(column.DeclaredType.Arguments[0])
			scale := int64(column.DeclaredType.Arguments[1])
			canonical.Precision = &precision
			canonical.Scale = &scale
		}
	case KindText, KindBlob:
		if length, ok := declaredModifier(column.DeclaredType); ok {
			if canonical.Kind == KindText &&
				length > SQLServerTextLengthLimit(base) {
				return CanonicalType{}, fmt.Errorf(
					"SQL Server column %s declares %s(%d), beyond the %d this"+
						" family can declare",
					column.Name,
					base,
					length,
					SQLServerTextLengthLimit(base),
				)
			}
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
		// smalldatetime resolves to the minute, so it has no sub-second
		// component at all - and that is an explicit zero rather than an
		// absence. Absent would let a target choose its own default precision,
		// which for PostgreSQL is six digits the source can never fill.
		if base == "smalldatetime" && canonical.FractionalSecondPrecision == nil {
			zero := int64(0)
			canonical.FractionalSecondPrecision = &zero
		}
	}

	// Ordering is asked only where it changes an answer.
	if !isKey {
		return canonical, nil
	}
	if canonical.Kind != KindText {
		canonical.Certification.Ordering = true
		return canonical, nil
	}
	if SQLServerTextKeyCollationCertified(base, collation) {
		canonical.Certification.Ordering = true
		return canonical, nil
	}
	canonical.Certification.Reason = SQLServerKeyCollationRemedy(base)
	return canonical, nil
}

// canonicalKindFromPortable maps the portable type a source already assigns.
//
// dmtx's engines have set column.Type to a small vocabulary since before this
// package existed. Reusing it rather than reclassifying from the engine's own
// spelling keeps one classification rather than adding a second that can
// disagree with it.
func canonicalKindFromPortable(portable string) Kind {
	switch strings.ToLower(strings.TrimSpace(portable)) {
	case "boolean":
		return KindBoolean
	case "integer":
		return KindInteger
	case "bigint":
		return KindBigInt
	case "numeric":
		return KindNumeric
	case "real":
		return KindReal
	case "double precision":
		return KindDouble
	case "text":
		return KindText
	case "blob":
		return KindBlob
	case "date":
		return KindDate
	case "time":
		return KindTime
	case "datetime":
		return KindDateTime
	case "uuid":
		return KindUUID
	case "json":
		return KindJSON
	default:
		return KindUnknown
	}
}

// declaredModifier reads the single positional modifier a declaration carries.
//
// The structured fields are the newer spelling; Arguments carries the same
// number for types declared positionally, which is what SQL Server discovery
// populates. Reading both means the converter does not care which a discoverer
// chose - but the CALLER must decide what the number means, because Arguments
// says nothing about whether it is a length or a precision.
func declaredModifier(declared *DeclaredType) (int64, bool) {
	if declared.Length != nil {
		return *declared.Length, true
	}
	if len(declared.Arguments) == 1 {
		return int64(declared.Arguments[0]), true
	}
	return 0, false
}

// sqlServerBaseKnown reports whether a declared base is one discovery emits for
// this kind.
//
// Fail-closed by enumeration rather than by trusting the portable type. The
// bases here are exactly the ones internal/engine assigns; anything else is a
// declaration dmtx did not produce and has not certified.
func sqlServerBaseKnown(base string, kind Kind) bool {
	switch kind {
	case KindBoolean:
		return base == "bool"
	case KindInteger:
		return base == "tinyint" || base == "smallint" || base == "int" ||
			base == "integer"
	case KindBigInt:
		return base == "bigint"
	case KindNumeric:
		return base == "decimal" || base == "numeric"
	case KindReal:
		return base == "real"
	case KindDouble:
		return base == "double precision" || base == "float"
	case KindText:
		return base == "char" || base == "varchar" || base == "nchar" ||
			base == "nvarchar" || base == "text"
	case KindBlob:
		return base == "binary" || base == "varbinary" || base == "blob"
	case KindDate:
		return base == "date"
	case KindTime:
		return base == "time"
	case KindDateTime:
		// timestamp is what discovery emits for datetime and datetime2, and
		// smalldatetime keeps its own name because its resolution is a minute -
		// a fact the target authority needs and "timestamp" would lose.
		//
		// Enumerated rather than assumed. An earlier draft claimed all three
		// arrived as timestamp, which was a guess, and it refused every
		// smalldatetime column until a test said so.
		return base == "timestamp" || base == "smalldatetime"
	case KindUUID:
		return base == "uuid"
	case KindJSON:
		return base == "json"
	default:
		return false
	}
}
