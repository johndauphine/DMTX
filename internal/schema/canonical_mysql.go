package schema

import (
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
			"MySQL column %s has no declared type",
			column.Name,
		)
	}

	canonical := CanonicalType{
		Kind:          canonicalKindFromPortable(column.Type),
		Certification: Certification{Transfer: true},
	}
	if canonical.Kind == KindUnknown {
		return CanonicalType{}, fmt.Errorf(
			"MySQL column %s has an unclassified type %q",
			column.Name,
			column.Type,
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
		return CanonicalType{}, fmt.Errorf(
			"MySQL column %s declares %q, which is not a base dmtx certifies"+
				" for %s",
			column.Name,
			column.DeclaredType.Base,
			canonical.Kind,
		)
	}

	// An integer carries no modifier, with one exception discovery emits.
	//
	// MySQL 8.0 deprecated display widths and mysql_source_discovery.go admits
	// only a bare integer or tinyint(1) - the conventional boolean. Anything
	// else is a declaration dmtx did not produce. The pairwise projection
	// checked this on every integer branch; the check is here now rather than
	// nowhere.
	switch canonical.Kind {
	case KindInteger, KindBigInt:
		arguments := column.DeclaredType.Arguments
		legal := len(arguments) == 0 ||
			base == "tinyint" && len(arguments) == 1 && arguments[0] == 1
		if !legal {
			return CanonicalType{}, fmt.Errorf(
				"MySQL column %s declares %s%v, and discovery emits only a bare"+
					" integer or tinyint(1)",
				column.Name,
				base,
				arguments,
			)
		}
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
		return base == "varbinary" || base == "tinyblob" ||
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
		// MySQL discovery emits no boolean, no real and no uuid, so a column
		// claiming one of those kinds did not come from it.
		return false
	}
}
