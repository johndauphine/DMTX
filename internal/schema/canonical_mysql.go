package schema

import (
	"errors"
	"fmt"
	"strings"
)

// MySQL's declarations, turned into canonical types.
//
// The collation set here was written in five places and none of them agreed.
// Discovery certified utf8mb4_bin and utf8mb4_0900_bin; the PostgreSQL and SQL
// Server projections certified utf8mb4_0900_bin and utf8mb4_nopad_bin; the
// delete path certified by flavour; the target renderer wrote a sixth opinion.
// So utf8mb4_bin passed discovery and failed both projections, and a test
// asserted that inconsistency without anyone noticing, because no test ran a
// real MySQL table through the whole path.

// MySQLFlavor distinguishes the two servers, because their certified binary
// collations do not overlap at all.
//
// This is not a detail to flatten. MySQL 8.0 has utf8mb4_bin and
// utf8mb4_0900_bin; MariaDB has utf8mb4_nopad_bin and neither of the others.
// A single combined set would certify, on each server, a collation that server
// does not have - which is worse than either list alone, because it would pass
// a schema check and fail at the engine.
type MySQLFlavor string

const (
	MySQLFlavorUnknown MySQLFlavor = ""
	MySQLFlavorOracle  MySQLFlavor = "mysql"
	MySQLFlavorMariaDB MySQLFlavor = "mariadb"
)

// MySQLTextKeyCollationCertified reports whether a text column under this
// collation may order a paged read on this server.
//
// Measured against MySQL 8.0 rather than argued from the collation's name:
//
//	utf8mb4_unicode_ci  orders [EUR, y-diaeresis]  and matches 'Ü' to 'ü'
//	utf8mb4_bin         orders [y-diaeresis, EUR]  and matches neither
//	PostgreSQL          orders [y-diaeresis, EUR]
//
// A paged read orders by the key, so under a case- or accent-insensitive
// collation MySQL calls two different strings equal where the target does not.
// A chunk boundary computed on one engine then does not mean the same thing on
// the other, and rows are skipped or repeated at the seam - silent corruption
// proportional to table size.
//
// Transfer is unaffected and is not asked here. The same emoji, CJK and
// accented text round-trip byte for byte under either collation, which is why
// certifying only the binary ones for DATA made an ordinary MySQL database
// unreadable for no gain.
//
// An unknown flavour certifies nothing. dmtx has measured two servers; a third
// would need measuring rather than assuming its names mean what these do.
func MySQLTextKeyCollationCertified(flavor MySQLFlavor, collation string) bool {
	collation = strings.ToLower(strings.TrimSpace(collation))
	switch flavor {
	case MySQLFlavorOracle:
		return collation == "utf8mb4_bin" || collation == "utf8mb4_0900_bin"
	case MySQLFlavorMariaDB:
		// MariaDB has neither of MySQL's two. Its NO PAD binary collation is
		// the one whose ordering was established.
		return collation == "utf8mb4_nopad_bin"
	default:
		return false
	}
}

// MySQLKeyCollationRemedy says what a refused text key would need.
//
// Named per flavour because the answer differs by server, and an operator told
// to use utf8mb4_bin on MariaDB - which does not have it - has been sent
// somewhere that does not exist.
func MySQLKeyCollationRemedy(flavor MySQLFlavor) string {
	switch flavor {
	case MySQLFlavorOracle:
		return "a text key must carry utf8mb4_bin or utf8mb4_0900_bin;" +
			" a case- or accent-insensitive collation calls two different" +
			" strings equal where the target does not, which moves a chunk" +
			" boundary. The table may keep its own default - only the key" +
			" column needs the binary collation"
	case MySQLFlavorMariaDB:
		return "a text key must carry utf8mb4_nopad_bin, which is MariaDB's" +
			" binary collation; MySQL's utf8mb4_bin and utf8mb4_0900_bin do" +
			" not exist here. The table may keep its own default - only the" +
			" key column needs it"
	default:
		return "dmtx has certified text-key ordering for MySQL 8.0 and" +
			" MariaDB 10.11 only, and this server is neither"
	}
}

// Why a MySQL declaration was refused, in the three categories the pairwise
// projections reported.
//
// A caller that wants to name the category has to be able to ask, and asking by
// matching on the message text would break the first time a message is
// reworded. The message says which rule; these say which kind of rule.
var (
	// ErrMySQLUndeclared is a column with no declaration, or one whose declared
	// base disagrees with the portable type discovery assigned it.
	ErrMySQLUndeclared = errors.New("declaration is missing or inconsistent")

	// ErrMySQLUnsupportedType is a base dmtx does not certify for this kind.
	ErrMySQLUnsupportedType = errors.New("type is not certified")

	// ErrMySQLBadModifier is a certified base carrying modifiers it does not
	// take - a length on LONGTEXT, a decimal with one argument, a display width
	// on an integer.
	ErrMySQLBadModifier = errors.New("type modifiers are not valid")
)

// CanonicalFromMySQL turns a discovered MySQL column into a canonical type.
//
// isKey decides which certification is asked, not which is granted - the same
// separation the SQL Server converter makes, and for the same reason. A body of
// text under an ordinary collation transfers perfectly and orders differently;
// only one of those facts matters, and only for the columns a read is ordered
// by.
func CanonicalFromMySQL(
	column Column,
	flavor MySQLFlavor,
	collation string,
	isKey bool,
) (CanonicalType, error) {
	if column.DeclaredType == nil {
		return CanonicalType{}, fmt.Errorf(
			"MySQL column %s has no declared type: %w",
			column.Name,
			ErrMySQLUndeclared,
		)
	}

	canonical := CanonicalType{
		Kind:          canonicalKindFromPortable(column.Type),
		Certification: Certification{Transfer: true},
	}
	if canonical.Kind == KindUnknown {
		return CanonicalType{}, fmt.Errorf(
			"MySQL column %s has an unclassified type %q: %w",
			column.Name,
			column.Type,
			ErrMySQLUndeclared,
		)
	}

	// The declared base has to be one MySQL discovery emits for this kind.
	//
	// Fail closed by enumeration, the same posture the SQL Server converter
	// takes. Classifying from column.Type alone would let a column whose
	// declaration says something dmtx never wrote pass straight through into a
	// target - and the pairwise projection this replaces checked the pair on
	// every branch, so leaving the check out would be a loss of strictness
	// wearing a refactor's clothes.
	base := strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	if !mySQLBaseKnown(base, canonical.Kind) {
		// Two different failures wear one check, and the pairwise projection
		// reported them differently: a base dmtx has never heard of is an
		// unsupported TYPE, while a base it knows for some other kind is a
		// declaration that disagrees with the portable name discovery assigned.
		// An operator can act on the second - something upstream synthesised the
		// pair - and not on the first.
		category := ErrMySQLUnsupportedType
		if mySQLBaseKnownForSomeKind(base) {
			category = ErrMySQLUndeclared
		}
		return CanonicalType{}, fmt.Errorf(
			"MySQL column %s declares %q, which is not a base dmtx certifies"+
				" for %s: %w",
			column.Name,
			column.DeclaredType.Base,
			canonical.Kind,
			category,
		)
	}

	// The modifiers have to be the ones this base carries.
	//
	// Every one of these was a branch of the pairwise projection, and each was
	// dropped by the first draft of this swap. What that draft accepted is the
	// argument for putting them back: LONGTEXT(100) became varchar(100), silently
	// bounding an unbounded column; a bare CHAR became unbounded text; and
	// decimal(12) - one argument where MySQL always reports two - came out as an
	// unbounded NUMERIC, because the branch that reads a precision only fires on
	// a pair. All three produce a schema that loads and is not the source's.
	if err := mySQLModifiersValid(
		base,
		canonical.Kind,
		column.DeclaredType,
	); err != nil {
		return CanonicalType{}, fmt.Errorf(
			"MySQL column %s %s: %w",
			column.Name,
			err.Error(),
			ErrMySQLBadModifier,
		)
	}

	// The width MySQL declared, which the portable name lost.
	//
	// Discovery names tinyint, smallint, mediumint and int alike "integer", so
	// carrying an ordinary MySQL TINYINT to a target as INT would spend four
	// bytes a row where one was declared. MEDIUMINT is deliberately not
	// narrowed: it is 24 bits, no other engine has it, and INTEGER is the
	// narrowest home that holds all of it.
	//
	// SMALLINT rather than TINYINT on the far side is not an accident of this
	// mapping - see KindSmallInt. MySQL's TINYINT is signed and SQL Server's is
	// not, so they are different types wearing one name.
	if canonical.Kind == KindInteger && (base == "tinyint" || base == "smallint") {
		canonical.Kind = KindSmallInt
	}
	// And the fixed byte string, for the same reason the SQL Server converter
	// refines it: the portable name blob covers both families and the base is
	// where the padding is stated.
	if canonical.Kind == KindBlob && base == "binary" {
		canonical.Kind = KindBinary
	}

	canonical.Precision = column.DeclaredType.Precision
	canonical.Scale = column.DeclaredType.Scale
	canonical.FractionalSecondPrecision =
		column.DeclaredType.FractionalSecondPrecision

	// The positional modifier means a length for text and a fractional-second
	// precision for a temporal, so it is read into the field that matches its
	// kind rather than into Length for everything.
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
	case KindText, KindBinary, KindBlob:
		if length, ok := declaredModifier(column.DeclaredType); ok {
			if limit := MySQLDeclaredLengthLimit(base); length > limit {
				return CanonicalType{}, fmt.Errorf(
					"MySQL column %s declares %s(%d), beyond the %d this family"+
						" can declare: %w",
					column.Name,
					base,
					length,
					limit,
					ErrMySQLBadModifier,
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
		// A bare MySQL DATETIME means ZERO fractional digits, where a bare
		// PostgreSQL timestamp means six. Same shape of fact, opposite value,
		// which is exactly why each converter states its own rather than a
		// shared default guessing for both.
		//
		// Discovery always writes the argument, so this is defence rather than
		// the ordinary path - and an absence here would let the PostgreSQL
		// target apply six digits this source can never fill.
		if canonical.FractionalSecondPrecision == nil {
			whole := int64(0)
			canonical.FractionalSecondPrecision = &whole
		}
	}

	if !isKey {
		return canonical, nil
	}
	if canonical.Kind != KindText {
		canonical.Certification.Ordering = true
		return canonical, nil
	}
	if MySQLTextKeyCollationCertified(flavor, collation) {
		canonical.Certification.Ordering = true
		return canonical, nil
	}
	canonical.Certification.Reason = MySQLKeyCollationRemedy(flavor)
	return canonical, nil
}

// mySQLBaseKnown reports whether a declared base is one MySQL discovery emits
// for this kind.
//
// Taken from the type switch in mysql_source_discovery.go rather than from
// MySQL's manual: the point is not what MySQL can declare, it is what dmtx has
// admitted. Anything else reaching here is a declaration dmtx did not produce.
//
// The spelling pairs - character for char, character varying for varchar,
// double precision for double, numeric for decimal - are what a catalog can
// report for the same type, and the pairwise projection accepted both. They are
// kept so that a source describing itself in the standard's words is not
// refused for it.
func mySQLBaseKnown(base string, kind Kind) bool {
	switch kind {
	case KindBoolean:
		// MySQL has no boolean type; BOOLEAN is a synonym for tinyint(1). A
		// column typed boolean therefore declares any of the three spellings,
		// and the pairwise projection accepted all three.
		return base == "bool" || base == "boolean" || base == "tinyint"
	case KindSmallInt, KindInteger:
		// One case for both. This runs before the width refinement below, so a
		// MySQL source always arrives as KindInteger; KindSmallInt appears only
		// when a caller validates an already-refined type.
		return base == "tinyint" || base == "smallint" ||
			base == "mediumint" || base == "int" || base == "integer"
	case KindBigInt:
		return base == "bigint"
	case KindNumeric:
		return base == "decimal" || base == "numeric"
	case KindDouble:
		return base == "double" || base == "double precision"
	case KindText:
		return base == "char" || base == "character" ||
			base == "varchar" || base == "character varying" ||
			base == "tinytext" || base == "text" ||
			base == "mediumtext" || base == "longtext"
	case KindBinary:
		return base == "binary"
	case KindBlob:
		// binary is admitted here as well as under KindBinary. MySQL discovery
		// names a binary column "binary", but a caller presenting the portable
		// bytea or blob with a fixed base is what the pairwise projection
		// accepted, and refineMySQLKind narrows it below.
		return base == "binary" || base == "varbinary" || base == "tinyblob" ||
			base == "blob" || base == "mediumblob" || base == "longblob"
	case KindDate:
		return base == "date"
	case KindTime:
		return base == "time"
	case KindDateTime:
		return base == "datetime" || base == "timestamp"
	case KindJSON:
		return base == "json"
	default:
		// MySQL discovery emits no real and no uuid, so a column claiming one
		// of those kinds did not come from it.
		return false
	}
}

// MySQLDeclaredLengthLimit is the largest modifier the named family may carry.
//
// These are MySQL's own bounds on a SOURCE declaration, the counterpart to
// SQLServerTextLengthLimit, and they are not one number:
//
//	char, binary       255
//	varchar            65535 CHARACTERS, subject to the row limit besides
//	varbinary          65535 BYTES, likewise
//
// mysql_source_discovery.go enforces these when it reads a catalog, so a column
// that reached here from a live server already satisfies them. The check is for
// everything else - a synthesised column, a fixture, a future caller - because
// the pairwise projection this replaces checked on every branch, and a
// converter that trusts its input is a converter that will eventually be handed
// something that lied.
func MySQLDeclaredLengthLimit(base string) int64 {
	switch base {
	case "char", "character", "binary":
		return MySQLBinaryLengthLimit
	default:
		return MySQLVarBinaryLengthLimit
	}
}

// mySQLModifiersValid checks a declaration's modifiers against its base.
//
// Taken from mysql_source_discovery.go, which is the authority on what dmtx
// admits, rather than from MySQL's grammar - the question is not what MySQL can
// write but what dmtx has read and certified.
//
// It reads the declaration rather than the Arguments slice, because the same
// modifier can arrive in either the structured field or the positional one and
// declaredModifier already knows that. A first draft looked only at Arguments
// and refused a varchar whose length was in Length - the two paths reading
// different fields, which is the mistake the differential tests were written
// after.
func mySQLModifiersValid(base string, kind Kind, declared *DeclaredType) error {
	arguments := declared.Arguments
	modifier, hasModifier := declaredModifier(declared)

	switch kind {
	case KindBoolean:
		// MySQL's boolean IS tinyint(1), so a column typed boolean may declare
		// either spelling - and the tinyint one must carry its 1, which is what
		// makes it the boolean rather than an ordinary narrow integer.
		if base == "tinyint" {
			if hasModifier && modifier == 1 {
				return nil
			}
			return fmt.Errorf(
				"declares %s%v as a boolean, and MySQL's boolean is tinyint(1)",
				base, arguments,
			)
		}
		if hasModifier {
			return fmt.Errorf(
				"declares %s%v, and this type carries no modifier",
				base, arguments,
			)
		}
		return nil

	case KindSmallInt, KindInteger, KindBigInt:
		// MySQL 8.0 deprecated display widths, and discovery admits only a bare
		// integer or tinyint(1) - the conventional boolean.
		if !hasModifier || base == "tinyint" && modifier == 1 {
			return nil
		}
		return fmt.Errorf(
			"declares %s%v, and discovery emits only a bare integer or"+
				" tinyint(1)",
			base, arguments,
		)

	case KindNumeric:
		// Always a precision and a scale. MySQL reports both for every decimal,
		// so one argument is not "scale omitted" - it is a declaration dmtx did
		// not read from a catalog.
		precision, scale, ok := mySQLNumericModifiers(declared)
		if !ok {
			return fmt.Errorf(
				"declares %s%v, and a decimal carries a precision and a scale",
				base, arguments,
			)
		}
		if precision < 1 || precision > mySQLNumericPrecisionLimit ||
			scale < 0 || scale > mySQLNumericScaleLimit || scale > precision {
			return fmt.Errorf(
				"declares %s(%d,%d), outside what MySQL can declare",
				base, precision, scale,
			)
		}
		return nil

	case KindText, KindBinary, KindBlob:
		// The bounded families REQUIRE a length and the unbounded ones forbid
		// one. Conflating those is how LONGTEXT(100) became varchar(100).
		if mySQLBoundedTextBase(base) {
			if !hasModifier || modifier < 1 {
				return fmt.Errorf(
					"declares %s%v, and this family carries a length",
					base, arguments,
				)
			}
			return nil
		}
		if hasModifier {
			return fmt.Errorf(
				"declares %s%v, and this family carries no length",
				base, arguments,
			)
		}
		return nil

	case KindTime, KindDateTime:
		// Zero or one, and the one is fractional-second digits.
		if declared.FractionalSecondPrecision != nil {
			modifier, hasModifier = *declared.FractionalSecondPrecision, true
		}
		if !hasModifier {
			return nil
		}
		if modifier < 0 || modifier > 6 {
			return fmt.Errorf(
				"declares %s%v, and a temporal carries at most six fractional"+
					" digits",
				base, arguments,
			)
		}
		return nil

	default:
		if hasModifier {
			return fmt.Errorf(
				"declares %s%v, and this type carries no modifier",
				base, arguments,
			)
		}
		return nil
	}
}

// mySQLNumericModifiers reads a decimal's precision and scale from whichever
// pair of fields the caller populated.
func mySQLNumericModifiers(declared *DeclaredType) (int, int, bool) {
	if declared.Precision != nil {
		scale := 0
		if declared.Scale != nil {
			scale = int(*declared.Scale)
		}
		return int(*declared.Precision), scale, true
	}
	if len(declared.Arguments) == 2 {
		return declared.Arguments[0], declared.Arguments[1], true
	}
	return 0, 0, false
}

// mySQLBoundedTextBase reports whether a base declares its own width.
func mySQLBoundedTextBase(base string) bool {
	switch base {
	case "char", "character", "varchar", "character varying",
		"binary", "varbinary":
		return true
	default:
		return false
	}
}

// MySQL's decimal bounds, which discovery already enforces.
const (
	mySQLNumericPrecisionLimit = 65
	mySQLNumericScaleLimit     = 30
)

// mySQLBaseKnownForSomeKind reports whether dmtx recognises this base at all.
//
// Used only to tell an unsupported type apart from a declaration that
// contradicts its own portable name. It asks every kind rather than listing the
// bases again, so a base added above cannot be forgotten here.
func mySQLBaseKnownForSomeKind(base string) bool {
	for _, kind := range []Kind{
		KindSmallInt, KindInteger, KindBigInt, KindNumeric, KindDouble,
		KindText, KindBinary, KindBlob, KindDate, KindTime, KindDateTime,
		KindJSON,
	} {
		if mySQLBaseKnown(base, kind) {
			return true
		}
	}
	return false
}
