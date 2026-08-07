package schema

import "fmt"

// The MySQL target's own vocabulary.
//
// A target records the names its catalog reports, and MySQL's are its own in
// almost every case: it says int where PostgreSQL says nothing, decimal where
// PostgreSQL says numeric, double where the standard says double precision, and
// longtext where both of the others say text. Target authority is authenticated
// against that catalog on later incremental runs, so the declaration has to be
// what MySQL will say rather than a neutral spelling.
//
// Two ceilings here are the TARGET's and have no counterpart on the source
// side, which is why they cannot be checked by a converter:
//
//	varchar    16383 CHARACTERS. MySQL's row limit is 65535 bytes and utf8mb4
//	           spends up to four per character, so this is where a bounded
//	           string stops being declarable and becomes LONGTEXT.
//	binary     255, where varbinary runs to 65535. Two families, two numbers.
//
// The second was wrong in the pairwise projection this replaces. A SQL Server
// BINARY(8000) - which that engine can declare - was projected as MySQL
// BINARY(8000), and MySQL's renderer refuses anything past 255, so the route
// produced a table that could not be created. The same mistake as the one the
// canonical DDL renderer had: one number where the engine has two.

// canonicalToMySQLDeclared gives the portable type and declaration a MySQL
// target should carry.
func canonicalToMySQLDeclared(
	value CanonicalType,
) (string, *DeclaredType, error) {
	declared := func(base string, arguments ...int) *DeclaredType {
		return &DeclaredType{
			Base:      base,
			Arguments: append([]int(nil), arguments...),
		}
	}
	switch value.Kind {
	case KindBoolean:
		// MySQL has no boolean type. BOOLEAN is a synonym for tinyint(1), and
		// the portable name is integer because that is what MySQL discovery
		// will report when this target is read back.
		return "integer", declared("tinyint", 1), nil
	case KindSmallInt:
		// MySQL keeps the narrow width, unlike PostgreSQL - see
		// smallIntKeepsItsWidth. The pairwise projection has been writing
		// SMALLINT for SQL Server's tinyint and smallint since before this
		// package existed, so keeping it is what agrees with the targets those
		// runs created.
		return "integer", declared("smallint"), nil
	case KindInteger:
		return "integer", declared("int"), nil
	case KindBigInt:
		return "bigint", declared("bigint"), nil
	case KindReal, KindDouble:
		// REAL collapses into DOUBLE, and that is exact rather than lossy:
		// every IEEE-754 binary32 value is representable in binary64. What is
		// NOT safe is carrying a REAL default across, because re-evaluating its
		// decimal token as a double would not reproduce the source's binary32
		// rounding - a property of the DEFAULT, refused by the projection.
		return "double precision", declared("double"), nil
	case KindNumeric:
		return canonicalToMySQLNumeric(value, declared)
	case KindText:
		return canonicalToMySQLText(value, declared)
	case KindBinary, KindBlob:
		return canonicalToMySQLBinary(value, declared)
	case KindDate:
		return "date", declared("date"), nil
	case KindTime:
		return canonicalToMySQLTemporal(value, "time", declared)
	case KindDateTime:
		return canonicalToMySQLTemporal(value, "datetime", declared)
	case KindUUID:
		// MySQL has no UUID type, so it arrives as the canonical 36-character
		// text form. The portable name is char because that is what MySQL
		// discovery reports for a CHAR column.
		return "char", declared("char", 36), nil
	case KindJSON:
		// LONGTEXT rather than MySQL's JSON. MySQL's JSON column normalises the
		// document on the way in - it reorders object keys and rewrites
		// whitespace - so the bytes that arrive are not the bytes that left.
		// Text holds the document as written, which is what fidelity means
		// here.
		return "text", declared("longtext"), nil
	default:
		return "", nil, &PolicyError{
			Operation: "project canonical type",
			Type:      string(value.Kind),
			Target:    string(MySQL),
		}
	}
}

// canonicalToMySQLNumeric applies MySQL's own decimal bounds.
//
// 65 digits and a scale of 30, where SQL Server stops at 38 and 38. A source
// can carry more than this target declares, so the check belongs here rather
// than in a converter - the same reason SQL Server's 38 does.
func canonicalToMySQLNumeric(
	value CanonicalType,
	declared func(string, ...int) *DeclaredType,
) (string, *DeclaredType, error) {
	if value.Precision == nil {
		return "", nil, &PolicyError{
			Operation: "project canonical type",
			Type:      "numeric without precision",
			Target:    string(MySQL),
		}
	}
	scale := int64(0)
	if value.Scale != nil {
		scale = *value.Scale
	}
	if *value.Precision < 1 || *value.Precision > mySQLNumericPrecisionLimit ||
		scale < 0 || scale > mySQLNumericScaleLimit || scale > *value.Precision {
		return "", nil, &PolicyError{
			Operation: "project canonical numeric precision",
			Type: fmt.Sprintf(
				"decimal(%d,%d) exceeds what MySQL can declare",
				*value.Precision, scale,
			),
			Target: string(MySQL),
		}
	}
	return "numeric", declared("decimal", int(*value.Precision), int(scale)), nil
}

// canonicalToMySQLText keeps a bound where MySQL can declare one.
//
// Length is CHARACTERS by the time it reaches here and MySQL's modifier is
// characters, so no arithmetic applies - unlike the SQL Server target, whose
// varchar counts bytes and must multiply by four. That difference is the whole
// reason each target writes its own text projection instead of sharing one.
func canonicalToMySQLText(
	value CanonicalType,
	declared func(string, ...int) *DeclaredType,
) (string, *DeclaredType, error) {
	if value.Length == nil || *value.Length > mysqlMaximumVarcharCharacters {
		// Past what a row can hold inline, so it becomes LONGTEXT rather than
		// being truncated to the limit - which would refuse values the source
		// holds.
		return "text", declared("longtext"), nil
	}
	return "varchar", declared("varchar", int(*value.Length)), nil
}

// canonicalToMySQLBinary respects the two families' different caps.
func canonicalToMySQLBinary(
	value CanonicalType,
	declared func(string, ...int) *DeclaredType,
) (string, *DeclaredType, error) {
	if value.Length == nil || *value.Length > MySQLVarBinaryLengthLimit {
		return "blob", declared("longblob"), nil
	}
	// Fixed stays fixed only while BINARY can declare the width. Past 255 it
	// widens to VARBINARY at the same width: the padding bytes are part of the
	// stored value and travel either way, and the alternative is DDL MySQL
	// rejects. The pairwise projection declared BINARY(8000) here and produced
	// a table that could not be created.
	if value.Kind == KindBinary && *value.Length <= MySQLBinaryLengthLimit {
		return "binary", declared("binary", int(*value.Length)), nil
	}
	return "varbinary", declared("varbinary", int(*value.Length)), nil
}

// canonicalToMySQLTemporal keeps fractional-second digits, zero included.
func canonicalToMySQLTemporal(
	value CanonicalType,
	base string,
	declared func(string, ...int) *DeclaredType,
) (string, *DeclaredType, error) {
	digits := int64(0)
	if value.FractionalSecondPrecision != nil {
		digits = *value.FractionalSecondPrecision
		if digits < 0 || digits > temporalDigitCeiling(MySQL) {
			return "", nil, &PolicyError{
				Operation: "project canonical temporal precision",
				Type:      base,
				Target:    string(MySQL),
			}
		}
	}
	// The argument is always written, including a zero. MySQL's catalog reports
	// datetime_precision for every temporal, so a declaration without it is a
	// shape the target will never report back.
	// MySQL's portable name and its declared base are the same word for both
	// temporals, unlike SQL Server's, where a datetime is declared "timestamp".
	return base, declared(base, int(digits)), nil
}
