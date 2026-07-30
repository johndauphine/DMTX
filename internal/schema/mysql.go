package schema

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

const (
	mysqlIdentifierMaximumCharacters = 64
	mysqlMaximumVarcharCharacters    = 16_383
	mysqlMaximumDeclaredRowBytes     = 65_535
	mysqlMaximumInnoDBColumns        = 1_017
	mysqlMaximumInnoDBLocalRowBytes  = 8_000
)

func mysqlTableCollation(table Table) (string, error) {
	value := strings.ToLower(strings.TrimSpace(table.MySQLCollation))
	if value == "" {
		return "utf8mb4_bin", nil
	}
	switch value {
	case "utf8mb4_bin", "utf8mb4_0900_bin", "utf8mb4_nopad_bin":
		return value, nil
	default:
		return "", &PolicyError{
			Operation: "render MySQL table collation",
			Type:      table.MySQLCollation,
			Target:    string(MySQL),
		}
	}
}

// renderMySQLDeclaredType renders only the constrained type declarations
// represented by DMTX's canonical metadata. MySQL attributes that are not
// represented structurally (for example unsigned and zerofill) never enter
// this renderer.
func renderMySQLDeclaredType(value DeclaredType) (string, error) {
	base := strings.ToLower(strings.Join(strings.Fields(value.Base), " "))
	noArguments := func() bool { return len(value.Arguments) == 0 }
	oneArgument := func(minimum, maximum int) (int, bool) {
		if len(value.Arguments) != 1 ||
			value.Arguments[0] < minimum ||
			value.Arguments[0] > maximum {
			return 0, false
		}
		return value.Arguments[0], true
	}
	switch base {
	case "tinyint":
		if noArguments() {
			return "TINYINT", nil
		}
		if width, ok := oneArgument(1, 1); ok {
			return fmt.Sprintf("TINYINT(%d)", width), nil
		}
	case "smallint":
		if noArguments() {
			return "SMALLINT", nil
		}
	case "mediumint":
		if noArguments() {
			return "MEDIUMINT", nil
		}
	case "int", "integer":
		if noArguments() {
			return "INT", nil
		}
	case "bigint":
		if noArguments() {
			return "BIGINT", nil
		}
	case "double", "double precision":
		if noArguments() {
			return "DOUBLE", nil
		}
	case "decimal", "numeric":
		if len(value.Arguments) == 2 {
			precision, scale := value.Arguments[0], value.Arguments[1]
			if precision >= 1 && precision <= 65 &&
				scale >= 0 && scale <= 30 && scale <= precision {
				return fmt.Sprintf("DECIMAL(%d,%d)", precision, scale), nil
			}
		}
	case "char", "character":
		if length, ok := oneArgument(1, 255); ok {
			return fmt.Sprintf("CHAR(%d)", length), nil
		}
	case "varchar", "character varying":
		if length, ok := oneArgument(1, mysqlMaximumVarcharCharacters); ok {
			return fmt.Sprintf("VARCHAR(%d)", length), nil
		}
	case "tinytext":
		if noArguments() {
			return "TINYTEXT", nil
		}
	case "text":
		if noArguments() {
			return "TEXT", nil
		}
	case "mediumtext":
		if noArguments() {
			return "MEDIUMTEXT", nil
		}
	case "longtext":
		if noArguments() {
			return "LONGTEXT", nil
		}
	case "binary":
		if length, ok := oneArgument(1, 255); ok {
			return fmt.Sprintf("BINARY(%d)", length), nil
		}
	case "varbinary":
		if length, ok := oneArgument(1, 65_535); ok {
			return fmt.Sprintf("VARBINARY(%d)", length), nil
		}
	case "tinyblob":
		if noArguments() {
			return "TINYBLOB", nil
		}
	case "blob":
		if noArguments() {
			return "BLOB", nil
		}
	case "mediumblob":
		if noArguments() {
			return "MEDIUMBLOB", nil
		}
	case "longblob":
		if noArguments() {
			return "LONGBLOB", nil
		}
	case "bool", "boolean":
		if noArguments() {
			return "TINYINT(1)", nil
		}
	case "date":
		if noArguments() {
			return "DATE", nil
		}
	case "time", "datetime", "timestamp":
		if noArguments() {
			return strings.ToUpper(base), nil
		}
		if precision, ok := oneArgument(0, 6); ok {
			if precision == 0 {
				return strings.ToUpper(base), nil
			}
			return fmt.Sprintf("%s(%d)", strings.ToUpper(base), precision), nil
		}
	case "json":
		if noArguments() {
			return "JSON", nil
		}
	}
	return "", mysqlDeclaredTypePolicy(value)
}

func mysqlDeclaredTypePolicy(value DeclaredType) error {
	return &PolicyError{
		Operation: "render MySQL declared type",
		Type:      declaredTypeDescription(value),
		Target:    string(MySQL),
	}
}

// mysqlIdentityColumn validates the portable identity shape that maps exactly
// to MySQL BIGINT AUTO_INCREMENT.
func mysqlIdentityColumn(table Table) (string, error) {
	if table.Identity == nil {
		return "", nil
	}
	identity := table.Identity
	if identity.Generation != IdentityByDefault ||
		identity.Column == "" ||
		identity.Frontier != nil && *identity.Frontier < 0 {
		return "", mysqlIdentityPolicy(table.Name)
	}

	var column *Column
	for index := range table.Columns {
		if table.Columns[index].Name == identity.Column {
			column = &table.Columns[index]
			break
		}
	}
	if column == nil || column.Nullable || column.Default != nil {
		return "", mysqlIdentityPolicy(table.Name)
	}
	keys := orderedPrimaryKeyColumns(table)
	if len(keys) != 1 || keys[0].Name != identity.Column {
		return "", mysqlIdentityPolicy(table.Name)
	}
	rendered, err := renderColumnType(*column, MySQL)
	if err != nil || !strings.EqualFold(rendered, "BIGINT") {
		return "", mysqlIdentityPolicy(table.Name)
	}
	return identity.Column, nil
}

func mysqlIdentityPolicy(table string) error {
	return &PolicyError{
		Operation: "render MySQL identity",
		Type:      table,
		Target:    string(MySQL),
	}
}

// validateMySQLDeclaredRowSize enforces MySQL's server-level 65,535-byte
// declared row limit before any DDL can reach a target. Variable-width
// character columns use utf8mb4's four-byte maximum because CreateTable fixes
// that character set. TEXT, BLOB, and JSON contribute only their conservative
// inline reference size; their payload is stored outside this server limit.
func validateMySQLDeclaredRowSize(table Table) error {
	if len(table.Columns) > mysqlMaximumInnoDBColumns {
		return &PolicyError{
			Operation: "render MySQL InnoDB column count",
			Type:      table.Name,
			Target:    string(MySQL),
		}
	}
	var total uint64
	// Leave a conservative margin beneath InnoDB's 16 KiB-page local-record
	// ceiling. The fixed allowance covers record/MVCC headers; externalizable
	// DYNAMIC fields contribute their pointer and length-directory footprint.
	local := uint64(18)
	nullableColumns := 0
	for _, column := range table.Columns {
		columnBytes, err := mysqlColumnMaximumRowBytes(column)
		if err != nil {
			return err
		}
		total += columnBytes
		localBytes, err := mysqlColumnMaximumInnoDBLocalBytes(column)
		if err != nil {
			return err
		}
		local += localBytes
		if column.Nullable {
			nullableColumns++
		}
	}
	// MySQL's server-level record calculation always reserves at least one
	// header/null byte. Its null bitmap grows by one byte per eight nullable
	// columns; include that catalog-independent overhead in the pure plan.
	overhead := (nullableColumns + 7) / 8
	if overhead < 1 {
		overhead = 1
	}
	total += uint64(overhead)
	local += uint64(overhead)
	if total > mysqlMaximumDeclaredRowBytes {
		return &PolicyError{
			Operation: "render MySQL declared row",
			Type:      table.Name,
			Target:    string(MySQL),
		}
	}
	if local > mysqlMaximumInnoDBLocalRowBytes {
		return &PolicyError{
			Operation: "render MySQL InnoDB local row",
			Type:      table.Name,
			Target:    string(MySQL),
		}
	}
	return nil
}

func mysqlColumnMaximumInnoDBLocalBytes(
	column Column,
) (uint64, error) {
	full, err := mysqlColumnMaximumRowBytes(column)
	if err != nil {
		return 0, err
	}
	base := mysqlColumnBase(column)
	switch base {
	case "tinytext", "text", "mediumtext", "longtext",
		"tinyblob", "blob", "mediumblob", "longblob", "json":
		// Account for the external pointer, variable-field directory, and
		// conservative record bookkeeping even though the server-level
		// declared-row calculation uses a smaller LOB reference. MySQL 8.0
		// rejects 198 such columns under DYNAMIC row format when a 40-byte
		// allowance still falls below the local-row budget, so retain the
		// additional per-field directory byte here.
		return 41, nil
	case "varchar", "character varying", "varbinary":
		// Short variable values remain inline. Only values large enough for
		// InnoDB's externalization threshold may be reduced to a pointer.
		if full > 768 {
			return 40, nil
		}
	case "char", "character":
		// InnoDB encodes fixed-width values of at least 768 bytes as
		// variable-width values that DYNAMIC row format may externalize.
		if full >= 768 {
			return 40, nil
		}
	}
	return full, nil
}

func mysqlColumnMaximumRowBytes(column Column) (uint64, error) {
	if column.DeclaredType == nil {
		return mysqlPortableColumnMaximumRowBytes(column)
	}
	value := *column.DeclaredType
	if _, err := renderMySQLDeclaredType(value); err != nil {
		return 0, err
	}
	base := strings.ToLower(strings.Join(strings.Fields(value.Base), " "))
	arguments := value.Arguments
	switch base {
	case "tinyint", "bool", "boolean":
		return 1, nil
	case "smallint":
		return 2, nil
	case "mediumint", "date":
		return 3, nil
	case "int", "integer":
		return 4, nil
	case "bigint", "double", "double precision":
		return 8, nil
	case "decimal", "numeric":
		if len(arguments) != 2 {
			return 0, mysqlDeclaredTypePolicy(value)
		}
		return uint64(mysqlDecimalStorageBytes(
			arguments[0],
			arguments[1],
		)), nil
	case "char", "character":
		if len(arguments) != 1 {
			return 0, mysqlDeclaredTypePolicy(value)
		}
		return uint64(arguments[0] * 4), nil
	case "varchar", "character varying":
		if len(arguments) != 1 {
			return 0, mysqlDeclaredTypePolicy(value)
		}
		payload := arguments[0] * 4
		return uint64(payload + mysqlLengthPrefixBytes(payload)), nil
	case "binary":
		if len(arguments) != 1 {
			return 0, mysqlDeclaredTypePolicy(value)
		}
		return uint64(arguments[0]), nil
	case "varbinary":
		if len(arguments) != 1 {
			return 0, mysqlDeclaredTypePolicy(value)
		}
		return uint64(
			arguments[0] + mysqlLengthPrefixBytes(arguments[0]),
		), nil
	case "time":
		precision, err := mysqlDeclaredTemporalPrecision(value)
		if err != nil {
			return 0, err
		}
		return uint64(3 + mysqlFractionalStorageBytes(precision)), nil
	case "datetime":
		precision, err := mysqlDeclaredTemporalPrecision(value)
		if err != nil {
			return 0, err
		}
		return uint64(5 + mysqlFractionalStorageBytes(precision)), nil
	case "timestamp":
		precision, err := mysqlDeclaredTemporalPrecision(value)
		if err != nil {
			return 0, err
		}
		return uint64(4 + mysqlFractionalStorageBytes(precision)), nil
	case "tinytext", "text", "mediumtext", "longtext",
		"tinyblob", "blob", "mediumblob", "longblob":
		// MySQL's server row-size calculation counts 9-12 bytes for each
		// off-page large-object value. Use the upper bound.
		return 12, nil
	case "json":
		// JSON uses binary large-object storage. Keep a wider conservative
		// inline allowance than ordinary BLOB/TEXT references.
		return 20, nil
	default:
		return 0, mysqlDeclaredTypePolicy(value)
	}
}

func mysqlPortableColumnMaximumRowBytes(column Column) (uint64, error) {
	switch strings.ToLower(strings.Join(strings.Fields(column.Type), " ")) {
	case "int", "integer", "int4":
		return 4, nil
	case "bigint", "int8":
		return 8, nil
	case "real", "float", "float4", "double", "double precision", "float8":
		return 8, nil
	case "decimal", "numeric":
		return uint64(mysqlDecimalStorageBytes(38, 10)), nil
	case "text", "varchar", "character varying":
		return 12, nil
	case "uuid":
		return 36 * 4, nil
	case "blob", "binary", "varbinary", "bytea":
		return 12, nil
	case "json", "jsonb":
		return 20, nil
	case "bool", "boolean":
		return 1, nil
	case "timestamp", "datetime":
		return 4, nil
	case "date":
		return 3, nil
	default:
		_, err := MapType(column.Type, MySQL)
		if err != nil {
			return 0, err
		}
		return 0, &PolicyError{
			Operation: "render MySQL declared row",
			Type:      column.Name,
			Target:    string(MySQL),
		}
	}
}

func mysqlDecimalStorageBytes(precision, scale int) int {
	integerDigits := precision - scale
	return mysqlDecimalDigitsStorageBytes(integerDigits) +
		mysqlDecimalDigitsStorageBytes(scale)
}

func mysqlDecimalDigitsStorageBytes(digits int) int {
	const fullGroupDigits = 9
	const fullGroupBytes = 4
	partialBytes := [...]int{0, 1, 1, 2, 2, 3, 3, 4, 4}
	return digits/fullGroupDigits*fullGroupBytes +
		partialBytes[digits%fullGroupDigits]
}

func mysqlLengthPrefixBytes(payloadBytes int) int {
	if payloadBytes <= 255 {
		return 1
	}
	return 2
}

func mysqlDeclaredTemporalPrecision(value DeclaredType) (int, error) {
	if len(value.Arguments) == 0 {
		return 0, nil
	}
	if len(value.Arguments) != 1 ||
		value.Arguments[0] < 0 ||
		value.Arguments[0] > 6 {
		return 0, mysqlDeclaredTypePolicy(value)
	}
	return value.Arguments[0], nil
}

func mysqlFractionalStorageBytes(precision int) int {
	return (precision + 1) / 2
}

func renderMySQLDefault(column Column) (string, error) {
	expression := column.Default
	if expression == nil {
		return "", mysqlDefaultPolicy(column)
	}
	base := mysqlColumnBase(column)
	switch expression.kind {
	case expressionNull:
		if !column.Nullable {
			return "", mysqlDefaultPolicy(column)
		}
		return "NULL", nil
	case expressionBoolean:
		if !mysqlColumnIsBoolean(column) {
			return "", mysqlDefaultPolicy(column)
		}
		if strings.EqualFold(expression.literal, "TRUE") {
			return "1", nil
		}
		if strings.EqualFold(expression.literal, "FALSE") {
			return "0", nil
		}
	case expressionNumber:
		if err := validateMySQLNumberDefault(column, expression.literal); err == nil {
			return expression.literal, nil
		}
	case expressionString:
		if target := mysqlCatalogDefaultTargetForColumn(column); target == mysqlCatalogDefaultDate ||
			target == mysqlCatalogDefaultTimestamp {
			literal, ok := canonicalMySQLStaticTemporalDefault(
				column,
				target,
				expression.literal,
			)
			if !ok {
				return "", mysqlDefaultPolicy(column)
			}
			return mysqlStringLiteral(literal), nil
		}
		if !mysqlTextBase(base) ||
			!utf8.ValidString(expression.literal) ||
			strings.ContainsRune(expression.literal, '\x00') {
			return "", mysqlDefaultPolicy(column)
		}
		if length, ok := mysqlCharacterLength(column); ok &&
			utf8.RuneCountInString(expression.literal) > length {
			return "", mysqlDefaultPolicy(column)
		}
		rendered := mysqlStringLiteral(expression.literal)
		if mysqlLargeObjectBase(base) {
			rendered = "(" + rendered + ")"
		}
		return rendered, nil
	case expressionBlob:
		if !mysqlBinaryBase(base) ||
			len(expression.literal)%2 != 0 ||
			!isLowerHex(strings.ToLower(expression.literal)) {
			return "", mysqlDefaultPolicy(column)
		}
		if length, ok := mysqlInlineBinaryLength(column); ok &&
			len(expression.literal)/2 > length {
			return "", mysqlDefaultPolicy(column)
		}
		rendered := "X'" + strings.ToLower(expression.literal) + "'"
		if mysqlLargeObjectBase(base) {
			rendered = "(" + rendered + ")"
		}
		return rendered, nil
	case expressionCurrentTime:
		if base == "time" {
			if precision := mysqlCatalogTemporalPrecision(column); precision > 0 {
				return fmt.Sprintf("(CURRENT_TIME(%d))", precision), nil
			}
			return "(CURRENT_TIME)", nil
		}
	case expressionCurrentDate:
		if base == "date" {
			return "(CURRENT_DATE)", nil
		}
	case expressionCurrentTimestamp:
		if base == "datetime" || base == "timestamp" {
			if precision := mysqlCatalogTemporalPrecision(column); precision > 0 {
				return fmt.Sprintf("CURRENT_TIMESTAMP(%d)", precision), nil
			}
			return "CURRENT_TIMESTAMP", nil
		}
	}
	return "", mysqlDefaultPolicy(column)
}

// NormalizeMySQLDefault validates a projected target default through the
// MySQL renderer and returns the canonical structured form produced by MySQL
// discovery. The returned expression never aliases the input. DEFAULT NULL is
// normalized to absence because information_schema cannot distinguish those
// equivalent target shapes.
func NormalizeMySQLDefault(column Column) (*Expression, error) {
	if column.Default == nil {
		return nil, nil
	}
	rendered, err := renderMySQLDefault(column)
	if err != nil {
		return nil, err
	}
	switch column.Default.kind {
	case expressionNull:
		return nil, nil
	case expressionString:
		target := mysqlCatalogDefaultTargetForColumn(column)
		if target == mysqlCatalogDefaultDate ||
			target == mysqlCatalogDefaultTimestamp {
			literal := column.Default.literal
			return ParseMySQLCatalogDefault(
				column,
				&literal,
				false,
			)
		}
		return &Expression{
			sql:     portableCheckStringLiteral(column.Default.literal),
			kind:    expressionString,
			literal: column.Default.literal,
		}, nil
	case expressionBlob:
		hexadecimal := strings.ToLower(column.Default.literal)
		return &Expression{
			sql:     "X'" + hexadecimal + "'",
			kind:    expressionBlob,
			literal: hexadecimal,
		}, nil
	case expressionCurrentTime,
		expressionCurrentDate,
		expressionCurrentTimestamp:
		return ParseMySQLCatalogDefault(column, &rendered, true)
	default:
		return ParseMySQLCatalogDefault(column, &rendered, false)
	}
}

func mysqlDefaultPolicy(column Column) error {
	return &PolicyError{
		Operation: "render MySQL default",
		Type:      column.Name,
		Target:    string(MySQL),
	}
}

func mysqlStringLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "'", "''")
	return "'" + value + "'"
}

func mysqlColumnBase(column Column) string {
	base := column.Type
	if column.DeclaredType != nil {
		base = column.DeclaredType.Base
	}
	return strings.ToLower(strings.Join(strings.Fields(base), " "))
}

func mysqlColumnIsBoolean(column Column) bool {
	base := mysqlColumnBase(column)
	if base == "bool" || base == "boolean" {
		return true
	}
	return base == "tinyint" &&
		column.DeclaredType != nil &&
		len(column.DeclaredType.Arguments) == 1 &&
		column.DeclaredType.Arguments[0] == 1
}

func mysqlTextBase(base string) bool {
	switch base {
	case "char", "character", "varchar", "character varying",
		"tinytext", "text", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

func mysqlBinaryBase(base string) bool {
	switch base {
	case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

func mysqlLargeObjectBase(base string) bool {
	switch base {
	case "tinytext", "text", "mediumtext", "longtext",
		"tinyblob", "blob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

func mysqlCharacterLength(column Column) (int, bool) {
	if column.DeclaredType == nil ||
		len(column.DeclaredType.Arguments) != 1 {
		return 0, false
	}
	switch mysqlColumnBase(column) {
	case "char", "character", "varchar", "character varying":
		return column.DeclaredType.Arguments[0], true
	default:
		return 0, false
	}
}

func mysqlInlineBinaryLength(column Column) (int, bool) {
	if column.DeclaredType == nil ||
		len(column.DeclaredType.Arguments) != 1 {
		return 0, false
	}
	switch mysqlColumnBase(column) {
	case "binary", "varbinary":
		return column.DeclaredType.Arguments[0], true
	default:
		return 0, false
	}
}

func validateMySQLNumberDefault(column Column, value string) error {
	base := mysqlColumnBase(column)
	bits := 0
	switch base {
	case "tinyint":
		bits = 8
	case "smallint":
		bits = 16
	case "mediumint":
		number, err := strconv.ParseInt(value, 10, 32)
		if err != nil || number < -8_388_608 || number > 8_388_607 {
			return fmt.Errorf("invalid MEDIUMINT")
		}
		return nil
	case "int", "integer":
		bits = 32
	case "bigint":
		bits = 64
	case "decimal", "numeric":
		if column.DeclaredType == nil ||
			len(column.DeclaredType.Arguments) != 2 {
			return fmt.Errorf("missing DECIMAL modifiers")
		}
		precision, scale := column.DeclaredType.Arguments[0],
			column.DeclaredType.Arguments[1]
		if precision < 1 || precision > 65 ||
			scale < 0 || scale > 30 || scale > precision {
			return fmt.Errorf("invalid DECIMAL modifiers")
		}
		return validatePostgresDecimalLiteral(value, precision, scale)
	case "double", "double precision", "float", "real":
		number, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
			return fmt.Errorf("invalid DOUBLE")
		}
		return nil
	default:
		return fmt.Errorf("numeric literal is incompatible with column")
	}
	_, err := strconv.ParseInt(value, 10, bits)
	return err
}

func isLowerHex(value string) bool {
	for _, character := range value {
		if character >= '0' && character <= '9' ||
			character >= 'a' && character <= 'f' {
			continue
		}
		return false
	}
	return true
}

// MySQLAutoIncrementPlan renders a deterministic identity reset. frontier is
// the greatest value already allocated; MySQL accepts the next value.
func MySQLAutoIncrementPlan(table Table, frontier int64) (Statement, error) {
	if _, err := mysqlIdentityColumn(table); err != nil {
		return Statement{}, err
	}
	if table.Identity == nil || frontier < 0 {
		return Statement{}, mysqlIdentityPolicy(table.Name)
	}
	if frontier == math.MaxInt64 {
		// BIGINT AUTO_INCREMENT is exhausted. MySQL retains this state
		// without a representable next value, so there is no reset DDL to
		// issue after the explicit maximum key is loaded.
		return Statement{}, nil
	}
	return Statement{
		SQL: "ALTER TABLE " + qualified(MySQL, table.Schema, table.Name) +
			" AUTO_INCREMENT = " + strconv.FormatInt(frontier+1, 10) + ";",
	}, nil
}
