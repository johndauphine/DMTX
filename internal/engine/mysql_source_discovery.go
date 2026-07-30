package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	mysql80MinimumPatch       = 16
	mysql80TargetMinimumPatch = 30
)

// VerifyMySQL80Source pins discovery to the Oracle MySQL 8.0 catalog shape
// covered by DMTX's live conformance tests. MariaDB and later MySQL catalog
// generations must opt in through their own verified implementation.
func VerifyMySQL80Source(
	ctx context.Context,
	database *sql.DB,
) error {
	catalog, err := readMySQL80ServerCatalog(ctx, database)
	if err != nil {
		return err
	}
	return validateMySQL80SourceServerCatalog(catalog)
}

// VerifyMySQL80Target adds the minimum patch-level and session contracts
// required by the native target's row-alias upserts and primary-key behavior.
func VerifyMySQL80Target(
	ctx context.Context,
	database *sql.DB,
) error {
	catalog, err := readMySQL80ServerCatalog(ctx, database)
	if err != nil {
		return err
	}
	if err := validateMySQL80SourceServerCatalog(catalog); err != nil {
		return err
	}
	if err := validateMySQL80TargetServerCatalog(catalog); err != nil {
		return err
	}
	var generateInvisiblePrimaryKey, requirePrimaryKey int
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			@@session.sql_generate_invisible_primary_key,
			@@session.sql_require_primary_key`,
	).Scan(
		&generateInvisiblePrimaryKey,
		&requirePrimaryKey,
	); err != nil {
		return fmt.Errorf(
			"read MySQL target primary-key generation contract: %w",
			err,
		)
	}
	if generateInvisiblePrimaryKey != 0 || requirePrimaryKey != 0 {
		return mysqlSourcePolicy(
			"target primary-key generation",
			fmt.Sprintf(
				"sql_generate_invisible_primary_key=%d sql_require_primary_key=%d; both must be disabled",
				generateInvisiblePrimaryKey,
				requirePrimaryKey,
			),
		)
	}
	return nil
}

func validateMySQL80TargetServerCatalog(
	catalog mysql80SourceServerCatalog,
) error {
	if err := validateMySQL80TargetVersion(catalog); err != nil {
		return err
	}
	modes := mysqlSQLModes(catalog.sqlMode)
	if !modes["NO_AUTO_VALUE_ON_ZERO"] {
		return mysqlSourcePolicy(
			"target SQL mode",
			"required mode NO_AUTO_VALUE_ON_ZERO is absent",
		)
	}
	if catalog.foreignKeyChecks != 1 || catalog.uniqueChecks != 1 {
		return mysqlSourcePolicy(
			"target constraint enforcement",
			fmt.Sprintf(
				"foreign_key_checks=%d unique_checks=%d; both must be enabled",
				catalog.foreignKeyChecks,
				catalog.uniqueChecks,
			),
		)
	}
	if catalog.innodbPageSize != 16_384 {
		return mysqlSourcePolicy(
			"target InnoDB page size",
			fmt.Sprintf(
				"innodb_page_size=%d; 16384 is required",
				catalog.innodbPageSize,
			),
		)
	}
	return nil
}

func validateMySQL80TargetVersion(
	catalog mysql80SourceServerCatalog,
) error {
	_, _, patch, _ := parseMySQLVersion(catalog.version)
	if patch < mysql80TargetMinimumPatch {
		return mysqlSourcePolicy(
			"target catalog version",
			fmt.Sprintf(
				"version=%q; Oracle MySQL 8.0.%d or later is required for the native target session contract",
				catalog.version,
				mysql80TargetMinimumPatch,
			),
		)
	}
	return nil
}

func readMySQL80ServerCatalog(
	ctx context.Context,
	database *sql.DB,
) (mysql80SourceServerCatalog, error) {
	var catalog mysql80SourceServerCatalog
	err := database.QueryRowContext(ctx, mysql80SourceServerCatalogQuery).Scan(
		&catalog.version,
		&catalog.versionComment,
		&catalog.sqlMode,
		&catalog.sessionTimeZone,
		&catalog.systemTimeZone,
		&catalog.autoIncrementIncrement,
		&catalog.autoIncrementOffset,
		&catalog.lowerCaseTableNames,
		&catalog.explicitTimestampDefaults,
		&catalog.foreignKeyChecks,
		&catalog.uniqueChecks,
		&catalog.innodbPageSize,
	)
	if err != nil {
		return mysql80SourceServerCatalog{}, fmt.Errorf(
			"read MySQL source version and session contract: %w",
			err,
		)
	}
	return catalog, nil
}

type mysql80SourceServerCatalog struct {
	version                   string
	versionComment            string
	sqlMode                   string
	sessionTimeZone           string
	systemTimeZone            string
	autoIncrementIncrement    int64
	autoIncrementOffset       int64
	lowerCaseTableNames       int
	explicitTimestampDefaults int
	foreignKeyChecks          int
	uniqueChecks              int
	innodbPageSize            int64
}

const mysql80SourceServerCatalogQuery = `
	SELECT
		VERSION(),
		@@version_comment,
		@@session.sql_mode,
		@@session.time_zone,
		@@system_time_zone,
		@@session.auto_increment_increment,
		@@session.auto_increment_offset,
		@@lower_case_table_names,
		@@explicit_defaults_for_timestamp,
		@@session.foreign_key_checks,
		@@session.unique_checks,
		@@innodb_page_size
`

func validateMySQL80SourceServerCatalog(
	value mysql80SourceServerCatalog,
) error {
	major, minor, patch, ok := parseMySQLVersion(value.version)
	comment := strings.ToLower(value.versionComment)
	if !ok ||
		major != 8 ||
		minor != 0 ||
		patch < mysql80MinimumPatch ||
		strings.Contains(strings.ToLower(value.version), "mariadb") ||
		!strings.Contains(comment, "mysql") {
		return mysqlSourcePolicy(
			"catalog version",
			fmt.Sprintf(
				"version=%q comment=%q; Oracle MySQL 8.0.%d or later is required",
				value.version,
				value.versionComment,
				mysql80MinimumPatch,
			),
		)
	}

	modes := mysqlSQLModes(value.sqlMode)
	requiredModes := []string{
		"STRICT_TRANS_TABLES",
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"ERROR_FOR_DIVISION_BY_ZERO",
		"NO_ENGINE_SUBSTITUTION",
	}
	for _, mode := range requiredModes {
		if !modes[mode] {
			return mysqlSourcePolicy(
				"SQL mode",
				"required mode "+mode+" is absent",
			)
		}
	}
	for _, mode := range []string{"ANSI_QUOTES", "NO_BACKSLASH_ESCAPES"} {
		if modes[mode] {
			return mysqlSourcePolicy(
				"SQL mode",
				"unsupported mode "+mode+" is enabled",
			)
		}
	}
	if value.sessionTimeZone != "+00:00" &&
		!(strings.EqualFold(value.sessionTimeZone, "SYSTEM") &&
			strings.EqualFold(value.systemTimeZone, "UTC")) {
		return mysqlSourcePolicy(
			"session time zone",
			fmt.Sprintf(
				"session=%q system=%q; UTC is required",
				value.sessionTimeZone,
				value.systemTimeZone,
			),
		)
	}
	if value.autoIncrementIncrement != 1 ||
		value.autoIncrementOffset != 1 {
		return mysqlSourcePolicy(
			"auto-increment allocation",
			fmt.Sprintf(
				"increment=%d offset=%d",
				value.autoIncrementIncrement,
				value.autoIncrementOffset,
			),
		)
	}
	if value.lowerCaseTableNames != 0 {
		return mysqlSourcePolicy(
			"identifier case",
			fmt.Sprintf(
				"lower_case_table_names=%d; value 0 is required",
				value.lowerCaseTableNames,
			),
		)
	}
	if value.explicitTimestampDefaults != 1 {
		return mysqlSourcePolicy(
			"timestamp defaults",
			"explicit_defaults_for_timestamp must be enabled",
		)
	}
	return nil
}

func mysqlSQLModes(value string) map[string]bool {
	modes := make(map[string]bool)
	for _, mode := range strings.Split(value, ",") {
		mode = strings.ToUpper(strings.TrimSpace(mode))
		if mode != "" {
			modes[mode] = true
		}
	}
	return modes
}

func parseMySQLVersion(value string) (int, int, int, bool) {
	core := value
	if suffix := strings.IndexByte(core, '-'); suffix >= 0 {
		core = core[:suffix]
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return 0, 0, 0, false
	}
	numbers := make([]int, 3)
	for index, part := range parts {
		number, err := strconv.Atoi(part)
		if err != nil || number < 0 {
			return 0, 0, 0, false
		}
		numbers[index] = number
	}
	return numbers[0], numbers[1], numbers[2], true
}

type mysql80SourceTableCatalog struct {
	tableType      string
	engine         sql.NullString
	version        sql.NullInt64
	rowFormat      sql.NullString
	tableCollation sql.NullString
	createOptions  string
	tableComment   string
	autoIncrement  sql.NullInt64
	columnCount    int
	partitionCount int
	triggerCount   int
}

const mysql80SourceTableCatalogQuery = `
	SELECT
		source_table.TABLE_TYPE,
		source_table.ENGINE,
		source_table.VERSION,
		source_table.ROW_FORMAT,
		source_table.TABLE_COLLATION,
		source_table.CREATE_OPTIONS,
		source_table.TABLE_COMMENT,
		source_table.AUTO_INCREMENT,
		(
			SELECT COUNT(*)
			FROM information_schema.COLUMNS AS source_column
			WHERE source_column.TABLE_SCHEMA = source_table.TABLE_SCHEMA
			  AND source_column.TABLE_NAME = source_table.TABLE_NAME
		),
		(
			SELECT COUNT(*)
			FROM information_schema.PARTITIONS AS source_partition
			WHERE source_partition.TABLE_SCHEMA = source_table.TABLE_SCHEMA
			  AND source_partition.TABLE_NAME = source_table.TABLE_NAME
			  AND source_partition.PARTITION_NAME IS NOT NULL
		),
		(
			SELECT COUNT(*)
			FROM information_schema.TRIGGERS AS source_trigger
			WHERE source_trigger.EVENT_OBJECT_SCHEMA = source_table.TABLE_SCHEMA
			  AND source_trigger.EVENT_OBJECT_TABLE = source_table.TABLE_NAME
		)
	FROM information_schema.TABLES AS source_table
	WHERE source_table.TABLE_SCHEMA = ?
	  AND source_table.TABLE_NAME = ?
`

func readMySQL80SourceTableCatalog(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (mysql80SourceTableCatalog, error) {
	var result mysql80SourceTableCatalog
	err := database.QueryRowContext(
		ctx,
		mysql80SourceTableCatalogQuery,
		namespace,
		name,
	).Scan(
		&result.tableType,
		&result.engine,
		&result.version,
		&result.rowFormat,
		&result.tableCollation,
		&result.createOptions,
		&result.tableComment,
		&result.autoIncrement,
		&result.columnCount,
		&result.partitionCount,
		&result.triggerCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return mysql80SourceTableCatalog{}, fmt.Errorf(
			"MySQL table %s.%s does not exist",
			namespace,
			name,
		)
	}
	if err != nil {
		return mysql80SourceTableCatalog{}, fmt.Errorf(
			"inspect MySQL table %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if err := validateMySQL80SourceTableCatalog(
		namespace,
		name,
		result,
	); err != nil {
		return mysql80SourceTableCatalog{}, err
	}
	return result, nil
}

func validateMySQL80SourceTableCatalog(
	namespace string,
	name string,
	value mysql80SourceTableCatalog,
) error {
	identity := namespace + "." + name
	switch {
	case !validMySQLSourceIdentifier(namespace) ||
		!validMySQLSourceIdentifier(name):
		return mysqlSourcePolicy("table identifier", identity)
	case value.tableType != "BASE TABLE":
		return mysqlSourcePolicy(
			"table",
			identity+" type "+quotedCatalogValue(value.tableType),
		)
	case !value.engine.Valid ||
		!strings.EqualFold(value.engine.String, "InnoDB"):
		return mysqlSourcePolicy(
			"storage engine",
			identity+" engine "+quotedNullString(value.engine),
		)
	case !value.version.Valid || value.version.Int64 <= 0:
		return mysqlSourcePolicy("table catalog version", identity)
	case !value.rowFormat.Valid ||
		!strings.EqualFold(value.rowFormat.String, "Dynamic"):
		return mysqlSourcePolicy(
			"row format",
			identity+" row format "+quotedNullString(value.rowFormat),
		)
	case !value.tableCollation.Valid ||
		!mySQLBinaryUTF8Collation(value.tableCollation.String):
		return mysqlSourcePolicy(
			"table collation",
			identity+" collation "+quotedNullString(value.tableCollation),
		)
	case strings.TrimSpace(value.createOptions) != "" &&
		!strings.EqualFold(
			strings.TrimSpace(value.createOptions),
			"row_format=DYNAMIC",
		):
		return mysqlSourcePolicy(
			"table options",
			identity+" options "+quotedCatalogValue(value.createOptions),
		)
	case value.tableComment != "":
		return mysqlSourcePolicy(
			"table comment",
			identity,
		)
	case value.columnCount <= 0:
		return mysqlSourcePolicy(
			"columns",
			identity+" has no columns",
		)
	case value.partitionCount != 0:
		return mysqlSourcePolicy(
			"partitioning",
			fmt.Sprintf("%s partitions=%d", identity, value.partitionCount),
		)
	case value.triggerCount != 0:
		return mysqlSourcePolicy(
			"triggers",
			fmt.Sprintf("%s triggers=%d", identity, value.triggerCount),
		)
	default:
		return nil
	}
}

type mysql80SourceColumnCatalog struct {
	position          int
	name              string
	dataType          string
	columnType        string
	nullable          string
	defaultValue      sql.NullString
	extra             string
	generation        string
	characterLength   sql.NullInt64
	octetLength       sql.NullInt64
	numericPrecision  sql.NullInt64
	numericScale      sql.NullInt64
	datetimePrecision sql.NullInt64
	characterSet      sql.NullString
	collation         sql.NullString
	columnKey         string
	comment           string
	srid              sql.NullInt64
}

type mysql80SourceColumnMetadata struct {
	autoIncrement    bool
	defaultGenerated bool
}

const mysql80SourceColumnsQuery = `
	SELECT
		ORDINAL_POSITION,
		COLUMN_NAME,
		DATA_TYPE,
		COLUMN_TYPE,
		IS_NULLABLE,
		COLUMN_DEFAULT,
		EXTRA,
		GENERATION_EXPRESSION,
		CHARACTER_MAXIMUM_LENGTH,
		CHARACTER_OCTET_LENGTH,
		NUMERIC_PRECISION,
		NUMERIC_SCALE,
		DATETIME_PRECISION,
		CHARACTER_SET_NAME,
		COLLATION_NAME,
		COLUMN_KEY,
		COLUMN_COMMENT,
		SRS_ID
	FROM information_schema.COLUMNS
	WHERE TABLE_SCHEMA = ?
	  AND TABLE_NAME = ?
	ORDER BY ORDINAL_POSITION
`

func readMySQL80SourceColumns(
	ctx context.Context,
	database *sql.DB,
	table mysql80SourceTableCatalog,
	namespace string,
	name string,
) ([]schema.Column, []string, error) {
	rows, err := database.QueryContext(
		ctx,
		mysql80SourceColumnsQuery,
		namespace,
		name,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"list MySQL columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	defer rows.Close()

	columns := make([]schema.Column, 0, table.columnCount)
	autoIncrementColumns := make([]string, 0, 1)
	for rows.Next() {
		var catalog mysql80SourceColumnCatalog
		if err := rows.Scan(
			&catalog.position,
			&catalog.name,
			&catalog.dataType,
			&catalog.columnType,
			&catalog.nullable,
			&catalog.defaultValue,
			&catalog.extra,
			&catalog.generation,
			&catalog.characterLength,
			&catalog.octetLength,
			&catalog.numericPrecision,
			&catalog.numericScale,
			&catalog.datetimePrecision,
			&catalog.characterSet,
			&catalog.collation,
			&catalog.columnKey,
			&catalog.comment,
			&catalog.srid,
		); err != nil {
			return nil, nil, fmt.Errorf(
				"read MySQL column for %s.%s: %w",
				namespace,
				name,
				err,
			)
		}
		if catalog.position != len(columns)+1 {
			return nil, nil, mysqlSourcePolicy(
				"column order",
				fmt.Sprintf(
					"%s.%s position=%d expected=%d",
					namespace,
					name,
					catalog.position,
					len(columns)+1,
				),
			)
		}
		column, metadata, err := mySQL80SourceColumnFromCatalog(catalog)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"discover MySQL column %s.%s.%s: %w",
				namespace,
				name,
				catalog.name,
				err,
			)
		}
		if catalog.characterSet.Valid &&
			(!catalog.collation.Valid ||
				!strings.EqualFold(
					catalog.collation.String,
					table.tableCollation.String,
				)) {
			return nil, nil, mysqlSourcePolicy(
				"column collation override",
				namespace+"."+name+"."+catalog.name,
			)
		}
		if catalog.defaultValue.Valid || metadata.defaultGenerated {
			var catalogDefault *string
			if catalog.defaultValue.Valid {
				value := catalog.defaultValue.String
				catalogDefault = &value
			}
			column.Default, err = schema.ParseMySQLCatalogDefault(
				column,
				catalogDefault,
				metadata.defaultGenerated,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"discover MySQL default for %s.%s.%s: %w",
					namespace,
					name,
					catalog.name,
					err,
				)
			}
		}
		if metadata.autoIncrement {
			autoIncrementColumns = append(
				autoIncrementColumns,
				catalog.name,
			)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf(
			"iterate MySQL columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if len(columns) != table.columnCount {
		return nil, nil, mysqlSourcePolicy(
			"column catalog shape",
			fmt.Sprintf(
				"%s.%s table columns=%d discovered=%d",
				namespace,
				name,
				table.columnCount,
				len(columns),
			),
		)
	}
	if len(autoIncrementColumns) > 1 {
		return nil, nil, mysqlSourcePolicy(
			"auto-increment",
			fmt.Sprintf(
				"%s.%s columns=%d",
				namespace,
				name,
				len(autoIncrementColumns),
			),
		)
	}
	return columns, autoIncrementColumns, nil
}

func mySQL80SourceColumnFromCatalog(
	catalog mysql80SourceColumnCatalog,
) (schema.Column, mysql80SourceColumnMetadata, error) {
	var metadata mysql80SourceColumnMetadata
	if !validMySQLSourceIdentifier(catalog.name) ||
		(catalog.nullable != "YES" && catalog.nullable != "NO") ||
		catalog.generation != "" ||
		catalog.comment != "" ||
		catalog.srid.Valid {
		return schema.Column{}, metadata, mysqlSourcePolicy(
			"column catalog shape",
			catalog.name+" "+catalog.columnType,
		)
	}
	switch catalog.columnKey {
	case "", "PRI", "UNI", "MUL":
	default:
		return schema.Column{}, metadata, mysqlSourcePolicy(
			"column key catalog shape",
			catalog.name+" key "+quotedCatalogValue(catalog.columnKey),
		)
	}
	extraTokens := strings.Fields(catalog.extra)
	for _, token := range extraTokens {
		switch token {
		case "auto_increment":
			if metadata.autoIncrement {
				return schema.Column{}, metadata, mysqlSourcePolicy(
					"column extra",
					catalog.name+" "+catalog.extra,
				)
			}
			metadata.autoIncrement = true
		case "DEFAULT_GENERATED":
			if metadata.defaultGenerated {
				return schema.Column{}, metadata, mysqlSourcePolicy(
					"column extra",
					catalog.name+" "+catalog.extra,
				)
			}
			metadata.defaultGenerated = true
		default:
			return schema.Column{}, metadata, mysqlSourcePolicy(
				"column extra",
				catalog.name+" "+catalog.extra,
			)
		}
	}
	if metadata.autoIncrement && metadata.defaultGenerated {
		return schema.Column{}, metadata, mysqlSourcePolicy(
			"column extra",
			catalog.name+" "+catalog.extra,
		)
	}

	column := schema.Column{
		Name:     catalog.name,
		Nullable: catalog.nullable == "YES",
	}
	if strings.Contains(catalog.columnType, " unsigned") ||
		strings.Contains(catalog.columnType, " zerofill") {
		return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
	}

	switch catalog.dataType {
	case "tinyint":
		if catalog.columnType != "tinyint" &&
			catalog.columnType != "tinyint(1)" {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		if err := validateMySQLNumericCatalog(
			catalog,
			3,
			0,
		); err != nil {
			return schema.Column{}, metadata, err
		}
		column.Type = "integer"
		column.DeclaredType = &schema.DeclaredType{Base: "tinyint"}
		if catalog.columnType == "tinyint(1)" {
			column.DeclaredType.Arguments = []int{1}
		}
	case "smallint":
		if catalog.columnType != "smallint" {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		if err := validateMySQLNumericCatalog(catalog, 5, 0); err != nil {
			return schema.Column{}, metadata, err
		}
		column.Type = "integer"
		column.DeclaredType = &schema.DeclaredType{Base: "smallint"}
	case "mediumint":
		if catalog.columnType != "mediumint" {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		if err := validateMySQLNumericCatalog(catalog, 7, 0); err != nil {
			return schema.Column{}, metadata, err
		}
		column.Type = "integer"
		column.DeclaredType = &schema.DeclaredType{Base: "mediumint"}
	case "int":
		if catalog.columnType != "int" {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		if err := validateMySQLNumericCatalog(catalog, 10, 0); err != nil {
			return schema.Column{}, metadata, err
		}
		column.Type = "integer"
		column.DeclaredType = &schema.DeclaredType{Base: "int"}
	case "bigint":
		if catalog.columnType != "bigint" {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		if err := validateMySQLNumericCatalog(catalog, 19, 0); err != nil {
			return schema.Column{}, metadata, err
		}
		column.Type = "bigint"
		column.DeclaredType = &schema.DeclaredType{Base: "bigint"}
	case "decimal":
		if !catalog.numericPrecision.Valid ||
			!catalog.numericScale.Valid ||
			catalog.numericPrecision.Int64 < 1 ||
			catalog.numericPrecision.Int64 > 65 ||
			catalog.numericScale.Int64 < 0 ||
			catalog.numericScale.Int64 > 30 ||
			catalog.numericScale.Int64 > catalog.numericPrecision.Int64 ||
			catalog.columnType != fmt.Sprintf(
				"decimal(%d,%d)",
				catalog.numericPrecision.Int64,
				catalog.numericScale.Int64,
			) ||
			!mySQLNonCharacterColumn(catalog) ||
			catalog.datetimePrecision.Valid {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = "numeric"
		column.DeclaredType = &schema.DeclaredType{
			Base: "decimal",
			Arguments: []int{
				int(catalog.numericPrecision.Int64),
				int(catalog.numericScale.Int64),
			},
		}
	case "double":
		if catalog.columnType != "double" ||
			!catalog.numericPrecision.Valid ||
			catalog.numericPrecision.Int64 != 22 ||
			catalog.numericScale.Valid ||
			!mySQLNonCharacterColumn(catalog) ||
			catalog.datetimePrecision.Valid {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = "double precision"
		column.DeclaredType = &schema.DeclaredType{Base: "double"}
	case "char", "varchar":
		if !catalog.characterLength.Valid ||
			catalog.characterLength.Int64 <= 0 ||
			catalog.characterLength.Int64 > 16_383 ||
			!catalog.octetLength.Valid ||
			catalog.octetLength.Int64 < catalog.characterLength.Int64 ||
			!catalog.characterSet.Valid ||
			catalog.characterSet.String != "utf8mb4" ||
			!catalog.collation.Valid ||
			!mySQLBinaryUTF8Collation(catalog.collation.String) ||
			catalog.numericPrecision.Valid ||
			catalog.numericScale.Valid ||
			catalog.datetimePrecision.Valid ||
			catalog.columnType != fmt.Sprintf(
				"%s(%d)",
				catalog.dataType,
				catalog.characterLength.Int64,
			) {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = catalog.dataType
		column.DeclaredType = &schema.DeclaredType{
			Base:      catalog.dataType,
			Arguments: []int{int(catalog.characterLength.Int64)},
		}
	case "tinytext", "text", "mediumtext", "longtext":
		if catalog.columnType != catalog.dataType ||
			!catalog.characterLength.Valid ||
			catalog.characterLength.Int64 <= 0 ||
			!catalog.octetLength.Valid ||
			catalog.octetLength.Int64 < catalog.characterLength.Int64 ||
			!catalog.characterSet.Valid ||
			catalog.characterSet.String != "utf8mb4" ||
			!catalog.collation.Valid ||
			!mySQLBinaryUTF8Collation(catalog.collation.String) ||
			catalog.numericPrecision.Valid ||
			catalog.numericScale.Valid ||
			catalog.datetimePrecision.Valid {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = "text"
		column.DeclaredType = &schema.DeclaredType{Base: catalog.dataType}
	case "binary", "varbinary":
		if !catalog.characterLength.Valid ||
			catalog.characterLength.Int64 <= 0 ||
			catalog.characterLength.Int64 > 65_535 ||
			!catalog.octetLength.Valid ||
			catalog.octetLength.Int64 != catalog.characterLength.Int64 ||
			catalog.characterSet.Valid ||
			catalog.collation.Valid ||
			catalog.numericPrecision.Valid ||
			catalog.numericScale.Valid ||
			catalog.datetimePrecision.Valid ||
			catalog.columnType != fmt.Sprintf(
				"%s(%d)",
				catalog.dataType,
				catalog.characterLength.Int64,
			) {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = catalog.dataType
		column.DeclaredType = &schema.DeclaredType{
			Base:      catalog.dataType,
			Arguments: []int{int(catalog.characterLength.Int64)},
		}
	case "tinyblob", "blob", "mediumblob", "longblob":
		if catalog.columnType != catalog.dataType ||
			!catalog.characterLength.Valid ||
			catalog.characterLength.Int64 <= 0 ||
			!catalog.octetLength.Valid ||
			catalog.octetLength.Int64 != catalog.characterLength.Int64 ||
			catalog.characterSet.Valid ||
			catalog.collation.Valid ||
			catalog.numericPrecision.Valid ||
			catalog.numericScale.Valid ||
			catalog.datetimePrecision.Valid {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = "blob"
		column.DeclaredType = &schema.DeclaredType{Base: catalog.dataType}
	case "date":
		if catalog.columnType != "date" ||
			!mySQLNonCharacterColumn(catalog) ||
			catalog.datetimePrecision.Valid {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = "date"
		column.DeclaredType = &schema.DeclaredType{Base: "date"}
	case "datetime", "timestamp":
		if !catalog.datetimePrecision.Valid ||
			catalog.datetimePrecision.Int64 < 0 ||
			catalog.datetimePrecision.Int64 > 6 ||
			!mySQLNonCharacterColumn(catalog) {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		expected := catalog.dataType
		if catalog.datetimePrecision.Int64 > 0 {
			expected = fmt.Sprintf(
				"%s(%d)",
				catalog.dataType,
				catalog.datetimePrecision.Int64,
			)
		}
		if catalog.columnType != expected {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = catalog.dataType
		column.DeclaredType = &schema.DeclaredType{
			Base:      catalog.dataType,
			Arguments: []int{int(catalog.datetimePrecision.Int64)},
		}
	case "json":
		if catalog.columnType != "json" ||
			catalog.characterLength.Valid ||
			catalog.octetLength.Valid ||
			catalog.numericPrecision.Valid ||
			catalog.numericScale.Valid ||
			catalog.datetimePrecision.Valid ||
			catalog.characterSet.Valid ||
			catalog.collation.Valid {
			return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
		}
		column.Type = "json"
		column.DeclaredType = &schema.DeclaredType{Base: "json"}
	default:
		return schema.Column{}, metadata, unsupportedMySQLSourceType(catalog)
	}

	if metadata.autoIncrement {
		if column.Nullable ||
			catalog.defaultValue.Valid ||
			column.Type != "bigint" ||
			column.DeclaredType == nil ||
			column.DeclaredType.Base != "bigint" {
			return schema.Column{}, metadata, mysqlSourcePolicy(
				"auto-increment column",
				catalog.name+" "+catalog.columnType,
			)
		}
	}
	return column, metadata, nil
}

func validateMySQLNumericCatalog(
	catalog mysql80SourceColumnCatalog,
	precision int64,
	scale int64,
) error {
	if !catalog.numericPrecision.Valid ||
		catalog.numericPrecision.Int64 != precision ||
		!catalog.numericScale.Valid ||
		catalog.numericScale.Int64 != scale ||
		!mySQLNonCharacterColumn(catalog) ||
		catalog.datetimePrecision.Valid {
		return unsupportedMySQLSourceType(catalog)
	}
	return nil
}

func mySQLNonCharacterColumn(
	catalog mysql80SourceColumnCatalog,
) bool {
	return !catalog.characterLength.Valid &&
		!catalog.octetLength.Valid &&
		!catalog.characterSet.Valid &&
		!catalog.collation.Valid
}

func unsupportedMySQLSourceType(
	catalog mysql80SourceColumnCatalog,
) error {
	return mysqlSourcePolicy(
		"type modifier",
		catalog.name+" "+catalog.columnType,
	)
}

func inspectMySQL80Table(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (schema.Table, error) {
	if err := VerifyMySQL80Source(ctx, database); err != nil {
		return schema.Table{}, err
	}
	tableCatalog, err := readMySQL80SourceTableCatalog(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	columns, autoIncrementColumns, err := readMySQL80SourceColumns(
		ctx,
		database,
		tableCatalog,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table := schema.Table{
		Schema:         namespace,
		Name:           name,
		MySQLCollation: tableCatalog.tableCollation.String,
		Columns:        columns,
	}
	if err := discoverMySQL80SourcePrimaryKey(
		ctx,
		database,
		&table,
	); err != nil {
		return schema.Table{}, err
	}
	if len(autoIncrementColumns) == 1 {
		table.Identity, err = discoverMySQL80SourceIdentity(
			tableCatalog,
			table,
			autoIncrementColumns[0],
		)
		if err != nil {
			return schema.Table{}, err
		}
	} else if tableCatalog.autoIncrement.Valid {
		return schema.Table{}, mysqlSourcePolicy(
			"auto-increment catalog shape",
			namespace+"."+name+" has a table frontier without a column",
		)
	}
	table.Indexes, err = discoverMySQL80SourceIndexes(
		ctx,
		database,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table.Checks, err = discoverMySQL80SourceChecks(
		ctx,
		database,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table.ForeignKeys, err = discoverMySQL80SourceForeignKeys(
		ctx,
		database,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	return table, nil
}

func discoverMySQL80SourceIdentity(
	catalog mysql80SourceTableCatalog,
	table schema.Table,
	columnName string,
) (*schema.Identity, error) {
	var column *schema.Column
	keyCount := 0
	for index := range table.Columns {
		candidate := &table.Columns[index]
		if candidate.PrimaryKey || candidate.PrimaryKeyPosition > 0 {
			keyCount++
		}
		if candidate.Name == columnName {
			column = candidate
		}
	}
	if column == nil ||
		column.Type != "bigint" ||
		column.Nullable ||
		column.Default != nil ||
		keyCount != 1 ||
		!column.PrimaryKey ||
		column.PrimaryKeyPosition != 1 {
		return nil, mysqlSourcePolicy(
			"auto-increment identity",
			table.Schema+"."+table.Name+"."+columnName,
		)
	}
	if !catalog.autoIncrement.Valid ||
		catalog.autoIncrement.Int64 < 1 {
		return nil, mysqlSourcePolicy(
			"auto-increment frontier",
			table.Schema+"."+table.Name,
		)
	}
	frontier := catalog.autoIncrement.Int64 - 1
	return &schema.Identity{
		Column:     columnName,
		Generation: schema.IdentityByDefault,
		Frontier:   &frontier,
	}, nil
}

func mysqlSourcePolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: "discover MySQL 8.0 source " + operation,
		Type:      value,
		Target:    string(schema.MySQL),
	}
}

func validMySQLSourceIdentifier(value string) bool {
	return value != "" &&
		utf8.ValidString(value) &&
		!strings.ContainsRune(value, '\x00') &&
		!strings.HasSuffix(value, " ")
}

func mySQLBinaryUTF8Collation(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "utf8mb4_bin", "utf8mb4_0900_bin":
		return true
	default:
		return false
	}
}

func quotedNullString(value sql.NullString) string {
	if !value.Valid {
		return "NULL"
	}
	return quotedCatalogValue(value.String)
}
