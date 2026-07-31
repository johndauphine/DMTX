package schema

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ValidateCatalogType rejects ambiguous declarations and modifier
// combinations before they can enter a durable schema snapshot.
func ValidateCatalogType(value CatalogType) error {
	if err := validateCatalogBase(value.Base); err != nil {
		return err
	}

	structured := catalogTypeUsesStructuredModifiers(value)
	if structured && len(value.Arguments) != 0 {
		return fmt.Errorf(
			"catalog type %q mixes legacy positional and structured modifiers",
			value.Base,
		)
	}
	base := canonicalCatalogBase(value.Base)
	if !structured {
		if base == "enum" || base == "set" {
			return invalidCatalogModifiers(
				value.Base,
				strings.ToUpper(base)+" members are missing",
			)
		}
		return validateLegacyCatalogArguments(value)
	}

	if err := validateOrdinaryCatalogModifiers(base, value); err != nil {
		return err
	}
	if err := validateSpatialCatalogModifiers(base, value); err != nil {
		return err
	}
	if err := validateMySQLCatalogModifiers(base, value); err != nil {
		return err
	}
	return nil
}

// ValidateDeclaredType is retained as the Stage-3-compatible spelling.
func ValidateDeclaredType(value DeclaredType) error {
	return ValidateCatalogType(value)
}

func catalogTypeUsesStructuredModifiers(value DeclaredType) bool {
	return value.Length != nil ||
		value.Precision != nil ||
		value.Scale != nil ||
		value.FractionalSecondPrecision != nil ||
		value.Spatial != nil ||
		value.MySQL != nil
}

func validateCatalogBase(value string) error {
	if value == "" ||
		value != strings.TrimSpace(value) ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("catalog type has an empty or invalid base")
	}
	if strings.ContainsAny(value, "(),;") {
		return fmt.Errorf(
			"catalog type base %q contains declaration syntax",
			value,
		)
	}
	if strings.Join(strings.Fields(value), " ") != value {
		return fmt.Errorf(
			"catalog type base %q is not in canonical whitespace form",
			value,
		)
	}
	return nil
}

func validateLegacyCatalogArguments(value DeclaredType) error {
	if len(value.Arguments) > 2 {
		return invalidCatalogModifiers(value.Base, "too many positional modifiers")
	}
	if len(value.Arguments) == 0 {
		return nil
	}

	base := canonicalCatalogBase(value.Base)
	switch base {
	case "numeric", "decimal":
		precision := value.Arguments[0]
		if precision <= 0 {
			return invalidCatalogModifiers(value.Base, "invalid numeric precision or scale")
		}
		if len(value.Arguments) == 2 &&
			(value.Arguments[1] < -1000 ||
				value.Arguments[1] > 1000) {
			return invalidCatalogModifiers(
				value.Base,
				"invalid numeric precision or scale",
			)
		}
	case "char", "character", "character varying", "varchar",
		"varying character", "binary", "varbinary", "nchar",
		"native character", "nvarchar", "float":
		if len(value.Arguments) != 1 || value.Arguments[0] <= 0 {
			return invalidCatalogModifiers(value.Base, "invalid length or precision")
		}
	case "tinyint":
		if len(value.Arguments) != 1 || value.Arguments[0] != 1 {
			return invalidCatalogModifiers(value.Base, "invalid TINYINT display width")
		}
	case "time", "datetime2", "datetimeoffset":
		if len(value.Arguments) != 1 || value.Arguments[0] > 7 {
			return invalidCatalogModifiers(value.Base, "invalid fractional-second precision")
		}
	case "datetime", "timestamp", "timestamptz":
		if len(value.Arguments) != 1 || value.Arguments[0] > 6 {
			return invalidCatalogModifiers(value.Base, "invalid fractional-second precision")
		}
	case "datetime64":
		if len(value.Arguments) != 1 || value.Arguments[0] > 9 {
			return invalidCatalogModifiers(value.Base, "invalid fractional-second precision")
		}
	default:
		return invalidCatalogModifiers(value.Base, "unknown positional modifier shape")
	}
	for index, argument := range value.Arguments {
		postgresNumericScale := (base == "numeric" ||
			base == "decimal") &&
			index == 1
		if argument < 0 && !postgresNumericScale {
			return invalidCatalogModifiers(
				value.Base,
				"negative positional modifier",
			)
		}
	}
	return nil
}

func validateOrdinaryCatalogModifiers(
	base string,
	value DeclaredType,
) error {
	if value.Length != nil {
		if *value.Length <= 0 || !catalogLengthBase(base) {
			return invalidCatalogModifiers(value.Base, "invalid length")
		}
	}
	if value.Precision != nil {
		if *value.Precision <= 0 || !catalogPrecisionBase(base) {
			return invalidCatalogModifiers(value.Base, "invalid precision")
		}
	}
	if value.Scale != nil {
		if value.Precision == nil {
			return invalidCatalogModifiers(value.Base, "scale requires a compatible precision")
		}
		if base == "numeric" || base == "decimal" {
			if *value.Scale < -1000 || *value.Scale > 1000 {
				return invalidCatalogModifiers(
					value.Base,
					"scale requires a compatible precision",
				)
			}
		} else if *value.Scale < 0 ||
			*value.Scale > *value.Precision {
			return invalidCatalogModifiers(
				value.Base,
				"scale requires a compatible precision",
			)
		}
	}
	if value.FractionalSecondPrecision != nil {
		maximum, temporal := catalogFractionalSecondMaximum(base)
		if *value.FractionalSecondPrecision < 0 ||
			!temporal ||
			*value.FractionalSecondPrecision > maximum {
			return invalidCatalogModifiers(
				value.Base,
				"invalid fractional-second precision",
			)
		}
	}

	groups := 0
	if value.Length != nil {
		groups++
	}
	if value.Precision != nil || value.Scale != nil {
		groups++
	}
	if value.FractionalSecondPrecision != nil {
		groups++
	}
	if groups > 1 {
		return invalidCatalogModifiers(
			value.Base,
			"contradictory ordinary modifier groups",
		)
	}
	return nil
}

func validateSpatialCatalogModifiers(
	base string,
	value DeclaredType,
) error {
	if value.Spatial == nil {
		return nil
	}
	if value.Length != nil ||
		value.Precision != nil ||
		value.Scale != nil ||
		value.FractionalSecondPrecision != nil ||
		value.MySQL != nil {
		return invalidCatalogModifiers(
			value.Base,
			"spatial metadata is coupled to non-spatial modifiers",
		)
	}
	if !catalogSpatialBase(base) {
		return invalidCatalogModifiers(
			value.Base,
			"spatial metadata is attached to a non-spatial base",
		)
	}
	subtype := value.Spatial.Subtype
	if !validSpatialSubtype(subtype) {
		return invalidCatalogModifiers(value.Base, "unknown spatial subtype")
	}
	if base != "geometry" &&
		base != "geography" &&
		base != string(subtype) {
		return invalidCatalogModifiers(
			value.Base,
			"spatial base and subtype disagree",
		)
	}
	return nil
}

func validateMySQLCatalogModifiers(
	base string,
	value DeclaredType,
) error {
	mysql := value.MySQL
	if mysql == nil {
		return nil
	}
	if !mysql.Unsigned &&
		!mysql.Zerofill &&
		!mysql.TinyIntOne &&
		mysql.BitWidth == nil &&
		mysql.EnumMembers == nil &&
		mysql.SetMembers == nil {
		return invalidCatalogModifiers(value.Base, "empty MySQL metadata")
	}
	if mysql.Zerofill && !mysql.Unsigned {
		return invalidCatalogModifiers(
			value.Base,
			"MySQL ZEROFILL requires explicit unsigned evidence",
		)
	}
	if (mysql.Unsigned || mysql.Zerofill) && !catalogMySQLNumericBase(base) {
		return invalidCatalogModifiers(
			value.Base,
			"MySQL numeric flags are attached to a non-numeric base",
		)
	}
	if mysql.TinyIntOne {
		if base != "tinyint" ||
			value.Length != nil ||
			value.Precision != nil ||
			value.Scale != nil ||
			value.FractionalSecondPrecision != nil {
			return invalidCatalogModifiers(
				value.Base,
				"TINYINT(1) evidence has a contradictory base or modifier",
			)
		}
	}
	if mysql.BitWidth != nil {
		if base != "bit" ||
			*mysql.BitWidth < 1 ||
			*mysql.BitWidth > 64 ||
			value.Length != nil ||
			value.Precision != nil ||
			value.Scale != nil ||
			value.FractionalSecondPrecision != nil {
			return invalidCatalogModifiers(value.Base, "invalid BIT width")
		}
	}
	if err := validateMySQLMembers(
		value.Base,
		base,
		"ENUM",
		mysql.EnumMembers,
		mysql.SetMembers,
	); err != nil {
		return err
	}
	if err := validateMySQLMembers(
		value.Base,
		base,
		"SET",
		mysql.SetMembers,
		mysql.EnumMembers,
	); err != nil {
		return err
	}
	if base == "enum" && len(mysql.EnumMembers) == 0 {
		return invalidCatalogModifiers(value.Base, "ENUM members are missing")
	}
	if base == "set" && len(mysql.SetMembers) == 0 {
		return invalidCatalogModifiers(value.Base, "SET members are missing")
	}
	if (len(mysql.EnumMembers) > 0 || len(mysql.SetMembers) > 0) &&
		(value.Length != nil ||
			value.Precision != nil ||
			value.Scale != nil ||
			value.FractionalSecondPrecision != nil ||
			mysql.Unsigned ||
			mysql.Zerofill ||
			mysql.TinyIntOne ||
			mysql.BitWidth != nil) {
		return invalidCatalogModifiers(
			value.Base,
			"ENUM/SET members are coupled to incompatible modifiers",
		)
	}
	return nil
}

func validateMySQLMembers(
	originalBase string,
	base string,
	kind string,
	members []string,
	other []string,
) error {
	if members == nil {
		return nil
	}
	if base != strings.ToLower(kind) || len(members) == 0 || other != nil {
		return invalidCatalogModifiers(
			originalBase,
			"contradictory "+kind+" member shape",
		)
	}
	if kind == "SET" && len(members) > 64 {
		return invalidCatalogModifiers(originalBase, "SET has more than 64 members")
	}
	seen := make(map[string]struct{}, len(members))
	for _, member := range members {
		if !utf8.ValidString(member) {
			return invalidCatalogModifiers(
				originalBase,
				kind+" contains invalid UTF-8",
			)
		}
		if _, duplicate := seen[member]; duplicate {
			return invalidCatalogModifiers(
				originalBase,
				kind+" contains duplicate members",
			)
		}
		seen[member] = struct{}{}
	}
	return nil
}

func canonicalCatalogBase(value string) string {
	return strings.ToLower(value)
}

func catalogLengthBase(base string) bool {
	switch base {
	case "char", "character", "character varying", "varchar",
		"varying character", "nchar", "nvarchar",
		"national char", "national character",
		"national character varying", "binary", "varbinary",
		"binary varying", "bit varying", "varbit":
		return true
	default:
		return false
	}
}

func catalogPrecisionBase(base string) bool {
	switch base {
	case "numeric", "decimal", "dec", "number", "float":
		return true
	default:
		return false
	}
}

func catalogTemporalBase(base string) bool {
	_, ok := catalogFractionalSecondMaximum(base)
	return ok
}

func catalogFractionalSecondMaximum(base string) (int64, bool) {
	switch base {
	case "time", "datetime2", "datetimeoffset":
		return 7, true
	case "datetime64":
		return 9, true
	case "time with time zone", "time without time zone", "timetz",
		"timestamp", "timestamp with time zone",
		"timestamp without time zone", "timestamptz", "datetime":
		return 6, true
	default:
		return 0, false
	}
}

func catalogSpatialBase(base string) bool {
	switch base {
	case "geometry", "geography", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection":
		return true
	default:
		return false
	}
}

func validSpatialSubtype(value SpatialSubtype) bool {
	switch value {
	case SpatialSubtypeGeometry,
		SpatialSubtypePoint,
		SpatialSubtypeLineString,
		SpatialSubtypePolygon,
		SpatialSubtypeMultiPoint,
		SpatialSubtypeMultiLineString,
		SpatialSubtypeMultiPolygon,
		SpatialSubtypeGeometryCollection:
		return true
	default:
		return false
	}
}

func catalogMySQLNumericBase(base string) bool {
	switch base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"decimal", "numeric", "float", "double", "double precision", "real":
		return true
	default:
		return false
	}
}

func invalidCatalogModifiers(base, detail string) error {
	return fmt.Errorf("catalog type %q has invalid modifiers: %s", base, detail)
}
