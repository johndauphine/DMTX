package schema

import (
	"fmt"
	"strings"
)

// SQLite's declarations, turned into canonical types.
//
// This converter differs from the other three in a way that is worth stating
// before any of the code: in SQLite the declared type is a HINT, not a
// constraint. A column declared TINYINT holds an eight-byte integer; a column
// declared VARCHAR(10) holds ten thousand characters. SQLite records what you
// wrote and applies an affinity, and nothing enforces the rest.
//
// So the converter reads a declaration for what the STORAGE can actually hold
// rather than for what the word says, which is why every integer spelling
// becomes KindBigInt and every float spelling becomes KindDouble. Carrying
// TINYINT across as a narrow integer would declare a bound on the far side that
// the source never had, and the first row past 127 would be refused by a target
// that was told to expect one.
//
// Discovery makes this converter's shape unusual too. sqlite_schema.go sets
// Column.Type to the declared base verbatim - "mediumint", "clob", "int2" - so
// there is no small portable vocabulary to classify from, and
// canonicalKindFromPortable would answer Unknown for most of a real database.
// The mapping below is SQLite's own, which is where it belongs.

// CanonicalFromSQLite turns a discovered SQLite column into a canonical type.
//
// Ordering certification is granted to every certified column, like PostgreSQL's
// and unlike SQL Server's and MySQL's. SQLite has one text comparison - BINARY,
// bytewise over UTF-8 - unless a column names NOCASE or RTRIM, and dmtx does not
// admit a column that names either. So a column that reached here already orders
// by bytes and there is no per-column collation left to interrogate.
func CanonicalFromSQLite(column Column, isKey bool) (CanonicalType, error) {
	if column.DeclaredType == nil {
		return CanonicalType{}, fmt.Errorf(
			"SQLite column %s has no declared type",
			column.Name,
		)
	}
	base := strings.ToLower(strings.Join(
		strings.Fields(column.DeclaredType.Base),
		" ",
	))

	kind, ok := canonicalKindFromSQLite(base)
	if !ok {
		return CanonicalType{}, fmt.Errorf(
			"SQLite column %s declares %q, which dmtx does not certify",
			column.Name,
			column.DeclaredType.Base,
		)
	}
	canonical := CanonicalType{
		Kind:          kind,
		Certification: Certification{Transfer: true},
	}

	arguments := column.DeclaredType.Arguments
	if err := sqliteModifiersValid(base, kind, arguments); err != nil {
		return CanonicalType{}, fmt.Errorf("SQLite column %s %w", column.Name, err)
	}

	switch kind {
	case KindNumeric:
		// One argument means a precision with an implicit zero scale, which is
		// how SQL writes NUMERIC(10) and how the pairwise projection read it.
		if len(arguments) >= 1 {
			precision := int64(arguments[0])
			scale := int64(0)
			if len(arguments) == 2 {
				scale = int64(arguments[1])
			}
			canonical.Precision = &precision
			canonical.Scale = &scale
		}
	case KindText, KindBlob:
		if len(arguments) == 1 {
			// The bound is carried, and it is worth saying what that means.
			//
			// SQLite does not enforce a declared length: a column declared
			// VARCHAR(10) accepts and returns a ten-thousand-character string.
			// PostgreSQL, MySQL and SQL Server all DO enforce theirs, so
			// carrying the number across can produce a target that refuses rows
			// the source held quite happily.
			//
			// It is carried anyway, because that is what every pairwise
			// projection has been writing and changing it is a data-fidelity
			// decision that needs evidence rather than a refactor. Task #46.
			length := int64(arguments[0])
			canonical.Length = &length
		}
	case KindTime, KindDateTime:
		if len(arguments) == 1 {
			digits := int64(arguments[0])
			canonical.FractionalSecondPrecision = &digits
		}
	}

	if isKey {
		canonical.Certification.Ordering = true
	}
	return canonical, nil
}

// canonicalKindFromSQLite maps SQLite's declared spellings onto kinds.
//
// The spellings come from sqliteTypeRules, which is what
// ParseSQLiteDeclaredType admits, MINUS two it deliberately will not classify -
// see the default case for which and why. So this is not a list to "fix" into
// agreement with the parser: doing that would re-admit them.
//
// TestEverySQLiteSpellingIsClassifiedOrDeliberatelyRefused holds both halves to
// the parser, so a spelling added there fails here rather than falling through
// as an unclassified type nobody notices.
func canonicalKindFromSQLite(base string) (Kind, bool) {
	switch base {
	case "int", "integer", "tinyint", "smallint", "mediumint",
		"bigint", "int2", "int8":
		// Every integer spelling, one kind, and the widest one.
		//
		// SQLite stores an integer in up to eight bytes whatever the column was
		// declared as, so TINYINT is not a narrow type here - it is a word.
		// Producing KindSmallInt would declare a range on the target that the
		// source never enforced, and the first value past 127 would be refused
		// by a target that had been told to expect one.
		//
		// This is the opposite of the MySQL converter, which narrows tinyint to
		// KindSmallInt precisely because MySQL DOES enforce it. Same word,
		// different engines, different answers - and neither is a property of
		// the word.
		return KindBigInt, true
	case "real", "double", "double precision", "float":
		// Likewise: SQLite stores every float as an eight-byte IEEE-754 double,
		// so REAL is not binary32 here the way it is on SQL Server.
		return KindDouble, true
	case "numeric", "decimal":
		return KindNumeric, true
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "native character", "nvarchar",
		"text", "clob":
		// CHAR is carried, which it is not from a PostgreSQL source. SQLite's
		// CHAR does not pad and does not compare padded - it has TEXT affinity
		// and nothing else - so the stored value is the varchar one. The third
		// distinct answer this word has produced in this package.
		return KindText, true
	case "blob", "binary", "varbinary":
		// KindBlob for all three, including BINARY. SQLite has no fixed-width
		// byte string: BINARY has BLOB affinity and pads nothing, so declaring
		// it fixed on the far side would invent padding the source never wrote.
		return KindBlob, true
	case "bool", "boolean":
		return KindBoolean, true
	case "date":
		return KindDate, true
	case "time":
		return KindTime, true
	case "datetime", "timestamp":
		return KindDateTime, true
	case "json":
		return KindJSON, true
	case "uuid":
		return KindUUID, true
	default:
		// "any" and "unsigned big int" are admitted by
		// ParseSQLiteDeclaredType and deliberately absent here.
		//
		// ANY declares no affinity at all, so dmtx cannot say what the column
		// holds - the one thing a canonical type must be able to say. UNSIGNED
		// BIG INT names a range SQLite does not have: it stores a signed eight-
		// byte integer, so the values that spelling promises above 2^63 cannot
		// be there, and a target told to expect them would be told something
		// false. The pairwise projections refused both.
		return KindUnknown, false
	}
}

// sqliteModifiersValid checks a declaration's modifiers against its base.
//
// SQLite's parser already bounds the ARITY - sqliteTypeRules gives each base a
// maxArgs - so what is left is the ranges, which the pairwise projections
// checked and which a converter that trusted its input would drop.
func sqliteModifiersValid(base string, kind Kind, arguments []int) error {
	switch kind {
	case KindNumeric:
		if len(arguments) == 0 {
			return nil
		}
		if arguments[0] < 1 || arguments[0] > sqliteNumericPrecisionLimit {
			return fmt.Errorf(
				"declares %s(%d), outside the precision dmtx certifies",
				base, arguments[0],
			)
		}
		if len(arguments) == 2 &&
			(arguments[1] < 0 || arguments[1] > arguments[0]) {
			return fmt.Errorf(
				"declares %s(%d,%d), whose scale is outside its precision",
				base, arguments[0], arguments[1],
			)
		}
		return nil

	case KindText, KindBlob:
		if len(arguments) == 0 {
			return nil
		}
		if arguments[0] < 1 || arguments[0] > sqliteTextLengthLimit {
			return fmt.Errorf(
				"declares %s(%d), outside the length dmtx certifies",
				base, arguments[0],
			)
		}
		return nil

	case KindTime, KindDateTime:
		if len(arguments) == 0 {
			return nil
		}
		if arguments[0] < 0 || arguments[0] > 6 {
			return fmt.Errorf(
				"declares %s(%d), and a temporal carries at most six"+
					" fractional digits",
				base, arguments[0],
			)
		}
		return nil

	default:
		if len(arguments) != 0 {
			return fmt.Errorf(
				"declares %s%v, and this type carries no modifier",
				base, arguments,
			)
		}
		return nil
	}
}

// What dmtx certifies for a SQLite declaration's modifiers.
//
// Neither is enforced by SQLite, so these bound what dmtx will CARRY rather
// than what the source can hold. They are the numbers the pairwise projections
// used.
const (
	sqliteNumericPrecisionLimit = 1_000
	sqliteTextLengthLimit       = 10_485_760
)
