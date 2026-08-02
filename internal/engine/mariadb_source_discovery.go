package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

const mariaDB1011MinimumPatch = 8

type mariaDB1011SourceServerCatalog struct {
	version                   string
	versionComment            string
	sqlMode                   string
	sessionTimeZone           string
	systemTimeZone            string
	autoIncrementIncrement    int64
	autoIncrementOffset       int64
	lowerCaseTableNames       int
	explicitTimestampDefaults int
}

const mariaDB1011SourceServerCatalogQuery = `
	SELECT
		VERSION(),
		@@version_comment,
		@@session.sql_mode,
		@@session.time_zone,
		@@system_time_zone,
		@@session.auto_increment_increment,
		@@session.auto_increment_offset,
		@@lower_case_table_names,
		@@explicit_defaults_for_timestamp
`

// VerifyMariaDB1011Source pins discovery to the MariaDB 10.11 catalog shape
// covered by DMTX's source conformance contract.
func VerifyMariaDB1011Source(
	ctx context.Context,
	database *sql.DB,
) error {
	return verifyMariaDB1011Source(ctx, database)
}

func verifyMariaDB1011Source(
	ctx context.Context,
	queryer MySQLCatalogQueryer,
) error {
	catalog, err := readMariaDB1011SourceServerCatalog(ctx, queryer)
	if err != nil {
		return err
	}
	return validateMariaDB1011SourceServerCatalog(catalog)
}

func readMariaDB1011SourceServerCatalog(
	ctx context.Context,
	database MySQLCatalogQueryer,
) (mariaDB1011SourceServerCatalog, error) {
	var catalog mariaDB1011SourceServerCatalog
	if err := database.QueryRowContext(
		ctx,
		mariaDB1011SourceServerCatalogQuery,
	).Scan(
		&catalog.version,
		&catalog.versionComment,
		&catalog.sqlMode,
		&catalog.sessionTimeZone,
		&catalog.systemTimeZone,
		&catalog.autoIncrementIncrement,
		&catalog.autoIncrementOffset,
		&catalog.lowerCaseTableNames,
		&catalog.explicitTimestampDefaults,
	); err != nil {
		return mariaDB1011SourceServerCatalog{}, fmt.Errorf(
			"read MariaDB 10.11 source version and session contract: %w",
			err,
		)
	}
	return catalog, nil
}

func validateMariaDB1011SourceServerCatalog(
	value mariaDB1011SourceServerCatalog,
) error {
	major, minor, patch, ok := parseMySQLVersion(value.version)
	version := strings.ToLower(value.version)
	comment := strings.ToLower(value.versionComment)
	if !ok ||
		major != 10 ||
		minor != 11 ||
		patch < mariaDB1011MinimumPatch ||
		!strings.Contains(version, "mariadb") ||
		!strings.Contains(comment, "mariadb") {
		return mariaDB1011SourcePolicy(
			"catalog version",
			fmt.Sprintf(
				"version=%q comment=%q; MariaDB 10.11.%d or later in the 10.11 series is required",
				value.version,
				value.versionComment,
				mariaDB1011MinimumPatch,
			),
		)
	}

	modes := mysqlSQLModes(value.sqlMode)
	for _, mode := range []string{
		"STRICT_TRANS_TABLES",
		"NO_ZERO_IN_DATE",
		"NO_ZERO_DATE",
		"ERROR_FOR_DIVISION_BY_ZERO",
		"NO_ENGINE_SUBSTITUTION",
	} {
		if !modes[mode] {
			return mariaDB1011SourcePolicy(
				"SQL mode",
				"required mode "+mode+" is absent",
			)
		}
	}
	for _, mode := range []string{
		"ANSI_QUOTES",
		"NO_BACKSLASH_ESCAPES",
		"EMPTY_STRING_IS_NULL",
		"PAD_CHAR_TO_FULL_LENGTH",
	} {
		if modes[mode] {
			return mariaDB1011SourcePolicy(
				"SQL mode",
				"unsupported mode "+mode+" is enabled",
			)
		}
	}
	if value.sessionTimeZone != "+00:00" &&
		!(strings.EqualFold(value.sessionTimeZone, "SYSTEM") &&
			strings.EqualFold(value.systemTimeZone, "UTC")) {
		return mariaDB1011SourcePolicy(
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
		return mariaDB1011SourcePolicy(
			"auto-increment allocation",
			fmt.Sprintf(
				"increment=%d offset=%d",
				value.autoIncrementIncrement,
				value.autoIncrementOffset,
			),
		)
	}
	if value.lowerCaseTableNames != 0 {
		return mariaDB1011SourcePolicy(
			"identifier case",
			fmt.Sprintf(
				"lower_case_table_names=%d; value 0 is required",
				value.lowerCaseTableNames,
			),
		)
	}
	if value.explicitTimestampDefaults != 1 {
		return mariaDB1011SourcePolicy(
			"timestamp defaults",
			"explicit_defaults_for_timestamp must be enabled",
		)
	}
	return nil
}

func readMariaDB1011SourceTableCatalog(
	ctx context.Context,
	database MySQLCatalogQueryer,
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
			"MariaDB table %s.%s does not exist",
			namespace,
			name,
		)
	}
	if err != nil {
		return mysql80SourceTableCatalog{}, fmt.Errorf(
			"inspect MariaDB table %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if err := validateMariaDB1011SourceTableCatalog(
		namespace,
		name,
		result,
	); err != nil {
		return mysql80SourceTableCatalog{}, err
	}
	return result, nil
}

func validateMariaDB1011SourceTableCatalog(
	namespace string,
	name string,
	value mysql80SourceTableCatalog,
) error {
	if !value.tableCollation.Valid ||
		!strings.EqualFold(
			value.tableCollation.String,
			"utf8mb4_nopad_bin",
		) {
		return mariaDB1011SourcePolicy(
			"table collation",
			namespace+"."+name+" collation "+
				quotedNullString(value.tableCollation),
		)
	}
	shared := value
	shared.tableCollation.String = "utf8mb4_bin"
	if err := translateMariaDB1011Policy(
		validateMySQL80SourceTableCatalog(namespace, name, shared),
	); err != nil {
		return err
	}
	return nil
}

type mariaDB1011SourceColumnCatalog struct {
	position          int
	name              string
	dataType          string
	columnType        string
	nullable          string
	defaultValue      sql.NullString
	extra             string
	generation        sql.NullString
	isGenerated       string
	characterLength   sql.NullInt64
	octetLength       sql.NullInt64
	numericPrecision  sql.NullInt64
	numericScale      sql.NullInt64
	datetimePrecision sql.NullInt64
	characterSet      sql.NullString
	collation         sql.NullString
	columnKey         string
	comment           string
	columnCheckCount  int
}

const mariaDB1011SourceColumnsQuery = `
	SELECT
		source_column.ORDINAL_POSITION,
		source_column.COLUMN_NAME,
		source_column.DATA_TYPE,
		source_column.COLUMN_TYPE,
		source_column.IS_NULLABLE,
		source_column.COLUMN_DEFAULT,
		source_column.EXTRA,
		source_column.GENERATION_EXPRESSION,
		source_column.IS_GENERATED,
		source_column.CHARACTER_MAXIMUM_LENGTH,
		source_column.CHARACTER_OCTET_LENGTH,
		source_column.NUMERIC_PRECISION,
		source_column.NUMERIC_SCALE,
		source_column.DATETIME_PRECISION,
		source_column.CHARACTER_SET_NAME,
		source_column.COLLATION_NAME,
		source_column.COLUMN_KEY,
		source_column.COLUMN_COMMENT,
		(
			SELECT COUNT(*)
			FROM information_schema.TABLE_CONSTRAINTS AS source_constraint
			JOIN information_schema.CHECK_CONSTRAINTS AS source_check
			  ON source_check.CONSTRAINT_SCHEMA =
			     source_constraint.CONSTRAINT_SCHEMA
			 AND source_check.TABLE_NAME = source_constraint.TABLE_NAME
			 AND source_check.CONSTRAINT_NAME =
			     source_constraint.CONSTRAINT_NAME
			WHERE source_constraint.TABLE_SCHEMA =
			      source_column.TABLE_SCHEMA
			  AND source_constraint.TABLE_NAME =
			      source_column.TABLE_NAME
			  AND source_constraint.CONSTRAINT_TYPE = 'CHECK'
			  AND source_check.LEVEL = 'Column'
			  AND source_constraint.CONSTRAINT_NAME =
			      source_column.COLUMN_NAME
		)
	FROM information_schema.COLUMNS AS source_column
	WHERE source_column.TABLE_SCHEMA = ?
	  AND source_column.TABLE_NAME = ?
	ORDER BY source_column.ORDINAL_POSITION
`

func readMariaDB1011SourceColumns(
	ctx context.Context,
	database MySQLCatalogQueryer,
	table mysql80SourceTableCatalog,
	namespace string,
	name string,
) ([]schema.Column, []string, error) {
	rows, err := database.QueryContext(
		ctx,
		mariaDB1011SourceColumnsQuery,
		namespace,
		name,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"list MariaDB columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	defer rows.Close()

	columns := make([]schema.Column, 0, table.columnCount)
	autoIncrementColumns := make([]string, 0, 1)
	for rows.Next() {
		var catalog mariaDB1011SourceColumnCatalog
		if err := rows.Scan(
			&catalog.position,
			&catalog.name,
			&catalog.dataType,
			&catalog.columnType,
			&catalog.nullable,
			&catalog.defaultValue,
			&catalog.extra,
			&catalog.generation,
			&catalog.isGenerated,
			&catalog.characterLength,
			&catalog.octetLength,
			&catalog.numericPrecision,
			&catalog.numericScale,
			&catalog.datetimePrecision,
			&catalog.characterSet,
			&catalog.collation,
			&catalog.columnKey,
			&catalog.comment,
			&catalog.columnCheckCount,
		); err != nil {
			return nil, nil, fmt.Errorf(
				"read MariaDB column for %s.%s: %w",
				namespace,
				name,
				err,
			)
		}
		if catalog.position != len(columns)+1 {
			return nil, nil, mariaDB1011SourcePolicy(
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
		column, metadata, err := mariaDB1011SourceColumnFromCatalog(
			catalog,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"discover MariaDB column %s.%s.%s: %w",
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
				)) &&
			!mariaDB1011JSONAliasCandidate(catalog) {
			return nil, nil, mariaDB1011SourcePolicy(
				"column collation override",
				namespace+"."+name+"."+catalog.name,
			)
		}
		if catalog.defaultValue.Valid {
			value := catalog.defaultValue.String
			column.Default, err = schema.ParseMariaDBCatalogDefault(
				column,
				&value,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"discover MariaDB default for %s.%s.%s: %w",
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
			"iterate MariaDB columns for %s.%s: %w",
			namespace,
			name,
			err,
		)
	}
	if len(columns) != table.columnCount {
		return nil, nil, mariaDB1011SourcePolicy(
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
		return nil, nil, mariaDB1011SourcePolicy(
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

func mariaDB1011SourceColumnFromCatalog(
	catalog mariaDB1011SourceColumnCatalog,
) (schema.Column, mysql80SourceColumnMetadata, error) {
	var metadata mysql80SourceColumnMetadata
	if catalog.columnCheckCount < 0 ||
		catalog.columnCheckCount > 1 {
		return schema.Column{}, metadata, mariaDB1011SourcePolicy(
			"column CHECK catalog shape",
			catalog.name+" "+catalog.columnType,
		)
	}
	if catalog.isGenerated != "NEVER" ||
		catalog.generation.Valid &&
			strings.TrimSpace(catalog.generation.String) != "" {
		return schema.Column{}, metadata, mariaDB1011SourcePolicy(
			"generated column",
			catalog.name+" "+catalog.columnType,
		)
	}
	extra := strings.TrimSpace(catalog.extra)
	if extra != "" && extra != "auto_increment" {
		return schema.Column{}, metadata, mariaDB1011SourcePolicy(
			"column extra",
			catalog.name+" "+catalog.extra,
		)
	}
	if catalog.collation.Valid &&
		!strings.EqualFold(
			catalog.collation.String,
			"utf8mb4_nopad_bin",
		) &&
		!mariaDB1011JSONAliasCandidate(catalog) {
		return schema.Column{}, metadata, mariaDB1011SourcePolicy(
			"column collation",
			catalog.name+" "+catalog.collation.String,
		)
	}

	shared := mysql80SourceColumnCatalog{
		position:          catalog.position,
		name:              catalog.name,
		dataType:          catalog.dataType,
		columnType:        mariaDB1011SharedColumnType(catalog),
		nullable:          catalog.nullable,
		defaultValue:      catalog.defaultValue,
		extra:             extra,
		characterLength:   catalog.characterLength,
		octetLength:       catalog.octetLength,
		numericPrecision:  catalog.numericPrecision,
		numericScale:      catalog.numericScale,
		datetimePrecision: catalog.datetimePrecision,
		characterSet:      catalog.characterSet,
		collation:         catalog.collation,
		columnKey:         catalog.columnKey,
		comment:           catalog.comment,
	}
	if shared.collation.Valid {
		shared.collation.String = "utf8mb4_bin"
	}
	column, metadata, err := mySQL80SourceColumnFromCatalog(shared)
	if err != nil {
		return schema.Column{}, mysql80SourceColumnMetadata{},
			translateMariaDB1011Policy(err)
	}
	return column, metadata, nil
}

func mariaDB1011JSONAliasCandidate(
	catalog mariaDB1011SourceColumnCatalog,
) bool {
	return catalog.columnCheckCount == 1 &&
		catalog.dataType == "longtext" &&
		catalog.columnType == "longtext" &&
		catalog.characterSet.Valid &&
		catalog.characterSet.String == "utf8mb4" &&
		catalog.collation.Valid &&
		strings.EqualFold(catalog.collation.String, "utf8mb4_bin")
}

func mariaDB1011SharedColumnType(
	catalog mariaDB1011SourceColumnCatalog,
) string {
	switch catalog.dataType {
	case "tinyint":
		if catalog.columnType == "tinyint(4)" {
			return "tinyint"
		}
	case "smallint":
		if catalog.columnType == "smallint(6)" {
			return "smallint"
		}
	case "mediumint":
		if catalog.columnType == "mediumint(9)" {
			return "mediumint"
		}
	case "int":
		if catalog.columnType == "int(11)" {
			return "int"
		}
	case "bigint":
		if catalog.columnType == "bigint(20)" {
			return "bigint"
		}
	}
	return catalog.columnType
}

func inspectMariaDB1011Table(
	ctx context.Context,
	database MySQLCatalogQueryer,
	namespace string,
	name string,
) (schema.Table, error) {
	if err := verifyMariaDB1011Source(ctx, database); err != nil {
		return schema.Table{}, err
	}
	tableCatalog, err := readMariaDB1011SourceTableCatalog(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	columns, autoIncrementColumns, err :=
		readMariaDB1011SourceColumns(
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
	if err := discoverMariaDB1011SourcePrimaryKey(
		ctx,
		database,
		&table,
	); err != nil {
		return schema.Table{}, err
	}
	if len(autoIncrementColumns) == 1 {
		table.Identity, err = discoverMariaDB1011SourceIdentity(
			tableCatalog,
			table,
			autoIncrementColumns[0],
		)
		if err != nil {
			return schema.Table{}, err
		}
	} else if tableCatalog.autoIncrement.Valid {
		return schema.Table{}, mariaDB1011SourcePolicy(
			"auto-increment catalog shape",
			namespace+"."+name+" has a table frontier without a column",
		)
	}
	table.Indexes, err = discoverMariaDB1011SourceIndexes(
		ctx,
		database,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table.Checks, err = discoverMariaDB1011SourceChecks(
		ctx,
		database,
		&table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	table.ForeignKeys, err = discoverMariaDB1011SourceForeignKeys(
		ctx,
		database,
		table,
	)
	if err != nil {
		return schema.Table{}, err
	}
	return table, nil
}

func discoverMariaDB1011SourceIdentity(
	catalog mysql80SourceTableCatalog,
	table schema.Table,
	columnName string,
) (*schema.Identity, error) {
	identity, err := discoverMySQL80SourceIdentity(
		catalog,
		table,
		columnName,
	)
	if err != nil {
		return nil, translateMariaDB1011Policy(err)
	}
	return identity, nil
}

func mariaDB1011SourcePolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: "discover MariaDB 10.11 source " + operation,
		Type:      value,
		Target:    string(schema.MySQL),
	}
}

func translateMariaDB1011Policy(err error) error {
	if err == nil {
		return nil
	}
	var policy *schema.PolicyError
	if !errors.As(err, &policy) {
		return err
	}
	translated := *policy
	translated.Operation = strings.Replace(
		translated.Operation,
		"discover MySQL 8.0 source ",
		"discover MariaDB 10.11 source ",
		1,
	)
	return &translated
}
