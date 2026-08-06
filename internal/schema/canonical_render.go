package schema

import (
	"fmt"
	"strconv"
)

// Canonical types, rendered into a target's declaration.
//
// This is the second half of the lattice: N renderers out, where the pairwise
// projections under internal/migrate had N x N. MapType is the closest thing
// that existed, and it takes a bare type name - so it answers "TEXT" for
// everything text-shaped and cannot say varchar(40). The modifiers are the
// whole difficulty, and dropping them is how an nvarchar(40) became a
// varchar(80) and a datetime lost its milliseconds.
//
// Nothing routes through this yet. It is proved against the pairwise
// projections first, by TestRenderedCanonicalMatchesThePairwiseProjection,
// because a renderer that disagrees with the code it replaces is a migration
// that writes a different schema than the one that has been running.

// RenderCanonical writes a canonical type as one dialect's declaration.
//
// An uncertified type is refused rather than rendered. The whole posture of
// this tool is that an unknown type is a value nobody has proved survives the
// trip, and a renderer that guessed would be the one place that posture leaked.
func RenderCanonical(value CanonicalType, target Dialect) (string, error) {
	if !value.Certified() {
		return "", &PolicyError{
			Operation: "render canonical type",
			Type:      string(value.Kind),
			Target:    string(target),
		}
	}
	switch value.Kind {
	case KindBoolean:
		return renderBoolean(target), nil
	case KindInteger:
		return renderNamed(target, "INTEGER", "Int32"), nil
	case KindBigInt:
		return renderNamed(target, "BIGINT", "Int64"), nil
	case KindReal:
		return renderReal(target), nil
	case KindDouble:
		return renderDouble(target), nil
	case KindNumeric:
		return renderNumeric(value, target)
	case KindText:
		return renderText(value, target), nil
	case KindBlob:
		return renderBlob(target), nil
	case KindDate:
		return renderNamed(target, "DATE", "Date"), nil
	case KindTime:
		return renderTime(value, target), nil
	case KindDateTime:
		return renderDateTime(value, target), nil
	case KindUUID:
		return renderUUID(target), nil
	case KindJSON:
		return renderJSON(target), nil
	default:
		return "", &PolicyError{
			Operation: "render canonical type",
			Type:      string(value.Kind),
			Target:    string(target),
		}
	}
}

func renderNamed(target Dialect, standard, clickhouse string) string {
	if target == ClickHouse {
		return clickhouse
	}
	return standard
}

func renderBoolean(target Dialect) string {
	switch target {
	case SQLServer:
		return "BIT"
	case ClickHouse:
		return "Bool"
	default:
		return "BOOLEAN"
	}
}

func renderReal(target Dialect) string {
	switch target {
	case SQLServer:
		return "REAL"
	case ClickHouse:
		return "Float32"
	default:
		return "REAL"
	}
}

func renderDouble(target Dialect) string {
	switch target {
	case SQLServer:
		return "FLOAT"
	case MySQL:
		return "DOUBLE"
	case ClickHouse:
		return "Float64"
	case SQLite:
		return "REAL"
	default:
		return "DOUBLE PRECISION"
	}
}

// renderNumeric keeps precision and scale, which is the point of the type.
//
// A NUMERIC with no precision is not NUMERIC(0) - it is a different type, and
// the engines differ on what it means. Absent stays absent.
func renderNumeric(value CanonicalType, target Dialect) (string, error) {
	if value.Precision == nil {
		if target == ClickHouse {
			return "", &PolicyError{
				Operation: "render canonical type",
				Type:      "numeric without precision",
				Target:    string(target),
			}
		}
		return "NUMERIC", nil
	}
	scale := int64(0)
	if value.Scale != nil {
		scale = *value.Scale
	}
	if target == ClickHouse {
		return fmt.Sprintf("Decimal(%d, %d)", *value.Precision, scale), nil
	}
	return "NUMERIC(" + strconv.FormatInt(*value.Precision, 10) +
		"," + strconv.FormatInt(scale, 10) + ")", nil
}

// renderText keeps a bounded length bounded.
//
// Length is CHARACTERS by the time a type is canonical - the converters do the
// byte arithmetic their engines require - so it is written straight through.
// An unbounded text is unbounded on the other side too; widening a bounded
// column to unbounded would be a silent change to what the target accepts.
func renderText(value CanonicalType, target Dialect) string {
	if value.Length == nil {
		switch target {
		case SQLServer:
			return "VARCHAR(MAX) COLLATE " + sqlServerPortableTextCollation
		case MySQL:
			return "LONGTEXT"
		case ClickHouse:
			return "String"
		default:
			return "TEXT"
		}
	}
	length := strconv.FormatInt(*value.Length, 10)
	switch target {
	case SQLServer:
		return "VARCHAR(" + length + ") COLLATE " + sqlServerPortableTextCollation
	case ClickHouse:
		// ClickHouse has no bounded string, so the bound is dropped and the
		// value still fits. Recorded here rather than silently: a target that
		// accepts more than the source could is a widening, not a loss.
		return "String"
	default:
		return "VARCHAR(" + length + ")"
	}
}

func renderBlob(target Dialect) string {
	switch target {
	case Postgres:
		return "BYTEA"
	case SQLServer:
		return "VARBINARY(MAX)"
	case MySQL:
		return "LONGBLOB"
	case ClickHouse:
		return "String"
	default:
		return "BLOB"
	}
}

// renderTime and renderDateTime keep fractional-second precision.
//
// Absent is not zero. A timestamp declared without a precision is not
// timestamp(0), and rendering it as one truncates every value it carries -
// which loads, validates, and is wrong.
func renderTime(value CanonicalType, target Dialect) string {
	if value.FractionalSecondPrecision == nil {
		return "TIME"
	}
	return "TIME(" + strconv.FormatInt(*value.FractionalSecondPrecision, 10) + ")"
}

func renderDateTime(value CanonicalType, target Dialect) string {
	digits := value.FractionalSecondPrecision
	switch target {
	case SQLServer:
		if digits == nil {
			return "DATETIME2"
		}
		return "DATETIME2(" + strconv.FormatInt(*digits, 10) + ")"
	case ClickHouse:
		if digits == nil {
			return "DateTime64(6)"
		}
		return "DateTime64(" + strconv.FormatInt(*digits, 10) + ")"
	case MySQL:
		if digits == nil {
			return "DATETIME"
		}
		return "DATETIME(" + strconv.FormatInt(*digits, 10) + ")"
	default:
		if digits == nil {
			return "TIMESTAMP"
		}
		return "TIMESTAMP(" + strconv.FormatInt(*digits, 10) + ")"
	}
}

func renderUUID(target Dialect) string {
	switch target {
	case Postgres, ClickHouse:
		return "UUID"
	case SQLServer:
		return "UNIQUEIDENTIFIER"
	case MySQL:
		return "CHAR(36)"
	default:
		return "TEXT"
	}
}

func renderJSON(target Dialect) string {
	switch target {
	case Postgres, MySQL:
		return "JSON"
	case SQLServer:
		return "NVARCHAR(MAX)"
	case ClickHouse:
		return "String"
	default:
		return "TEXT"
	}
}
