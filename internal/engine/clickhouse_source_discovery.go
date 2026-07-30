package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type clickHouseSourceTableCatalog struct {
	engine               string
	engineFull           string
	partitionKey         string
	sortingKey           string
	primaryKey           string
	samplingKey          string
	storagePolicy        string
	comment              string
	dependenciesDatabase []string
	dependenciesTable    []string
	createTableQuery     string
}

type clickHouseSourceColumnCatalog struct {
	position          uint64
	name              string
	rawType           string
	defaultKind       string
	defaultExpression string
	comment           string
	inPartitionKey    uint8
	inSortingKey      uint8
	inPrimaryKey      uint8
	inSamplingKey     uint8
	compressionCodec  string
}

// InspectClickHouseTable discovers the narrow ClickHouse 24.8 rebuild shape.
// ClickHouse ordering and sparse-index metadata are deliberately stored apart
// from relational primary-key fields because neither guarantees uniqueness.
func InspectClickHouseTable(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (schema.Table, error) {
	if database == nil {
		return schema.Table{}, fmt.Errorf(
			"discover ClickHouse 24.8 source table: database is required",
		)
	}
	if namespace == "" || name == "" {
		return schema.Table{}, fmt.Errorf(
			"discover ClickHouse 24.8 source table: database and table names are required",
		)
	}
	tableCatalog, err := readClickHouseSourceTableCatalog(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	columnCatalogs, err := readClickHouseSourceColumnCatalogs(
		ctx,
		database,
		namespace,
		name,
	)
	if err != nil {
		return schema.Table{}, err
	}
	var skippingIndexes uint64
	if err := database.QueryRowContext(
		ctx,
		`SELECT count()
		   FROM system.data_skipping_indices
		  WHERE database = ? AND table = ?`,
		namespace,
		name,
	).Scan(&skippingIndexes); err != nil {
		return schema.Table{}, fmt.Errorf(
			"inspect ClickHouse source skipping indexes: %w",
			err,
		)
	}
	var recheckedCreateQuery string
	err = database.QueryRowContext(
		ctx,
		`SELECT create_table_query
		   FROM system.tables
		  WHERE database = ? AND name = ? AND is_temporary = 0`,
		namespace,
		name,
	).Scan(&recheckedCreateQuery)
	if errors.Is(err, sql.ErrNoRows) {
		return schema.Table{}, fmt.Errorf(
			"ClickHouse table %s.%s changed during discovery",
			namespace,
			name,
		)
	}
	if err != nil {
		return schema.Table{}, fmt.Errorf(
			"recheck ClickHouse source table: %w",
			err,
		)
	}
	if recheckedCreateQuery != tableCatalog.createTableQuery {
		return schema.Table{}, clickHouseSourcePolicy(
			"catalog changed during discovery",
			namespace+"."+name,
		)
	}
	return clickHouseSourceTableFromCatalog(
		namespace,
		name,
		tableCatalog,
		columnCatalogs,
		skippingIndexes,
	)
}

func readClickHouseSourceTableCatalog(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) (clickHouseSourceTableCatalog, error) {
	var catalog clickHouseSourceTableCatalog
	err := database.QueryRowContext(
		ctx,
		`SELECT
			engine,
			engine_full,
			partition_key,
			sorting_key,
			primary_key,
			sampling_key,
			storage_policy,
			comment,
			dependencies_database,
			dependencies_table,
			create_table_query
		   FROM system.tables
		  WHERE database = ? AND name = ? AND is_temporary = 0`,
		namespace,
		name,
	).Scan(
		&catalog.engine,
		&catalog.engineFull,
		&catalog.partitionKey,
		&catalog.sortingKey,
		&catalog.primaryKey,
		&catalog.samplingKey,
		&catalog.storagePolicy,
		&catalog.comment,
		&catalog.dependenciesDatabase,
		&catalog.dependenciesTable,
		&catalog.createTableQuery,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return clickHouseSourceTableCatalog{}, fmt.Errorf(
			"ClickHouse table %s.%s does not exist",
			namespace,
			name,
		)
	}
	if err != nil {
		return clickHouseSourceTableCatalog{}, fmt.Errorf(
			"inspect ClickHouse source table: %w",
			err,
		)
	}
	return catalog, nil
}

func readClickHouseSourceColumnCatalogs(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) ([]clickHouseSourceColumnCatalog, error) {
	rows, err := database.QueryContext(
		ctx,
		`SELECT
			position,
			name,
			type,
			default_kind,
			default_expression,
			comment,
			is_in_partition_key,
			is_in_sorting_key,
			is_in_primary_key,
			is_in_sampling_key,
			compression_codec
		   FROM system.columns
		  WHERE database = ? AND table = ?
		  ORDER BY position`,
		namespace,
		name,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"list ClickHouse source columns: %w",
			err,
		)
	}
	defer rows.Close()
	var catalogs []clickHouseSourceColumnCatalog
	for rows.Next() {
		var catalog clickHouseSourceColumnCatalog
		if err := rows.Scan(
			&catalog.position,
			&catalog.name,
			&catalog.rawType,
			&catalog.defaultKind,
			&catalog.defaultExpression,
			&catalog.comment,
			&catalog.inPartitionKey,
			&catalog.inSortingKey,
			&catalog.inPrimaryKey,
			&catalog.inSamplingKey,
			&catalog.compressionCodec,
		); err != nil {
			return nil, fmt.Errorf(
				"read ClickHouse source column: %w",
				err,
			)
		}
		catalogs = append(catalogs, catalog)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate ClickHouse source columns: %w",
			err,
		)
	}
	return catalogs, nil
}

func clickHouseSourceTableFromCatalog(
	namespace string,
	name string,
	tableCatalog clickHouseSourceTableCatalog,
	columnCatalogs []clickHouseSourceColumnCatalog,
	skippingIndexes uint64,
) (schema.Table, error) {
	if tableCatalog.engine != "MergeTree" {
		return schema.Table{}, clickHouseSourcePolicy(
			"table engine",
			name+" "+tableCatalog.engine,
		)
	}
	if tableCatalog.partitionKey != "" {
		return schema.Table{}, clickHouseSourcePolicy(
			"partition key",
			name+" "+tableCatalog.partitionKey,
		)
	}
	if tableCatalog.samplingKey != "" {
		return schema.Table{}, clickHouseSourcePolicy(
			"sampling key",
			name+" "+tableCatalog.samplingKey,
		)
	}
	if tableCatalog.sortingKey == "" {
		return schema.Table{}, clickHouseSourcePolicy(
			"empty ordering key",
			name,
		)
	}
	orderBy, err := parseClickHouseDirectColumnKey(
		tableCatalog.sortingKey,
	)
	if err != nil {
		return schema.Table{}, clickHouseSourcePolicy(
			"ordering key",
			name+" "+tableCatalog.sortingKey,
		)
	}
	if tableCatalog.primaryKey != tableCatalog.sortingKey {
		return schema.Table{}, clickHouseSourcePolicy(
			"non-default sparse primary key",
			name+" "+tableCatalog.primaryKey,
		)
	}
	expectedEngineFull := "MergeTree ORDER BY "
	if len(orderBy) == 1 {
		expectedEngineFull += tableCatalog.sortingKey
	} else {
		expectedEngineFull += "(" + tableCatalog.sortingKey + ")"
	}
	expectedEngineFull += " SETTINGS index_granularity = 8192"
	if tableCatalog.engineFull != expectedEngineFull {
		return schema.Table{}, clickHouseSourcePolicy(
			"MergeTree settings",
			name+" "+tableCatalog.engineFull,
		)
	}
	if tableCatalog.storagePolicy != "default" {
		return schema.Table{}, clickHouseSourcePolicy(
			"storage policy",
			name+" "+tableCatalog.storagePolicy,
		)
	}
	if tableCatalog.comment != "" {
		return schema.Table{}, clickHouseSourcePolicy(
			"table comment",
			name,
		)
	}
	if len(tableCatalog.dependenciesDatabase) != 0 ||
		len(tableCatalog.dependenciesTable) != 0 {
		return schema.Table{}, clickHouseSourcePolicy(
			"table dependencies",
			name,
		)
	}
	if skippingIndexes != 0 {
		return schema.Table{}, clickHouseSourcePolicy(
			"data-skipping indexes",
			name,
		)
	}
	if len(columnCatalogs) == 0 {
		return schema.Table{}, clickHouseSourcePolicy(
			"empty table",
			name,
		)
	}

	orderMembership := make(map[string]struct{}, len(orderBy))
	for _, column := range orderBy {
		if _, duplicate := orderMembership[column]; duplicate {
			return schema.Table{}, clickHouseSourcePolicy(
				"duplicate ordering column",
				name+"."+column,
			)
		}
		orderMembership[column] = struct{}{}
	}
	columns := make([]schema.Column, len(columnCatalogs))
	columnNames := make(map[string]struct{}, len(columnCatalogs))
	for index, catalog := range columnCatalogs {
		if catalog.position != uint64(index+1) {
			return schema.Table{}, clickHouseSourcePolicy(
				"column position",
				name+"."+catalog.name,
			)
		}
		if catalog.name == "" {
			return schema.Table{}, clickHouseSourcePolicy(
				"empty column name",
				name,
			)
		}
		if _, duplicate := columnNames[catalog.name]; duplicate {
			return schema.Table{}, clickHouseSourcePolicy(
				"duplicate column",
				name+"."+catalog.name,
			)
		}
		columnNames[catalog.name] = struct{}{}
		column, err := clickHouseSourceColumnFromCatalog(catalog)
		if err != nil {
			return schema.Table{}, err
		}
		_, inOrder := orderMembership[catalog.name]
		expectedMembership := uint8(0)
		if inOrder {
			expectedMembership = 1
			if column.Nullable {
				return schema.Table{}, clickHouseSourcePolicy(
					"nullable ordering column",
					name+"."+catalog.name,
				)
			}
			if column.Type == "double" {
				return schema.Table{}, clickHouseSourcePolicy(
					"floating-point ordering column",
					name+"."+catalog.name,
				)
			}
		}
		if catalog.inSortingKey != expectedMembership ||
			catalog.inPrimaryKey != expectedMembership ||
			catalog.inPartitionKey != 0 ||
			catalog.inSamplingKey != 0 {
			return schema.Table{}, clickHouseSourcePolicy(
				"column key membership",
				name+"."+catalog.name,
			)
		}
		columns[index] = column
	}
	for _, key := range orderBy {
		if _, exists := columnNames[key]; !exists {
			return schema.Table{}, clickHouseSourcePolicy(
				"missing ordering column",
				name+"."+key,
			)
		}
	}
	if err := validateClickHouseCreateTableQuery(
		tableCatalog.createTableQuery,
		tableCatalog.engineFull,
		columnCatalogs,
	); err != nil {
		return schema.Table{}, clickHouseSourcePolicy(
			"table definition",
			name,
		)
	}
	return schema.Table{
		Schema:            namespace,
		Name:              name,
		ClickHouseOrderBy: append([]string(nil), orderBy...),
		Columns:           columns,
	}, nil
}

func clickHouseSourceColumnFromCatalog(
	catalog clickHouseSourceColumnCatalog,
) (schema.Column, error) {
	if catalog.defaultKind != "" || catalog.defaultExpression != "" {
		return schema.Column{}, clickHouseSourcePolicy(
			"column default or generated expression",
			catalog.name,
		)
	}
	if catalog.comment != "" {
		return schema.Column{}, clickHouseSourcePolicy(
			"column comment",
			catalog.name,
		)
	}
	if catalog.compressionCodec != "" {
		return schema.Column{}, clickHouseSourcePolicy(
			"column compression codec",
			catalog.name+" "+catalog.compressionCodec,
		)
	}
	rawType := catalog.rawType
	nullable := false
	if strings.HasPrefix(rawType, "Nullable(") &&
		strings.HasSuffix(rawType, ")") {
		nullable = true
		rawType = rawType[len("Nullable(") : len(rawType)-1]
	}
	var canonical string
	switch rawType {
	case "Int64":
		canonical = "bigint"
	case "Float64":
		canonical = "double"
	case "String":
		canonical = "text"
	default:
		return schema.Column{}, clickHouseSourcePolicy(
			"column type",
			catalog.name+" "+catalog.rawType,
		)
	}
	return schema.Column{
		Name:     catalog.name,
		Type:     canonical,
		Nullable: nullable,
	}, nil
}

func parseClickHouseDirectColumnKey(value string) ([]string, error) {
	var tokens []string
	start := 0
	quoted := false
	for index := 0; index < len(value); index++ {
		switch value[index] {
		case '`':
			if quoted && index+1 < len(value) && value[index+1] == '`' {
				index++
				continue
			}
			quoted = !quoted
		case ',':
			if !quoted {
				tokens = append(tokens, value[start:index])
				start = index + 1
			}
		}
	}
	if quoted {
		return nil, fmt.Errorf("unterminated quoted identifier")
	}
	tokens = append(tokens, value[start:])
	result := make([]string, 0, len(tokens))
	for _, token := range tokens {
		identifier, err := parseClickHouseDirectColumn(token)
		if err != nil {
			return nil, err
		}
		result = append(result, identifier)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("ordering key is empty")
	}
	return result, nil
}

func parseClickHouseDirectColumn(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("ordering column is empty")
	}
	if value[0] == '`' {
		if len(value) < 2 || value[len(value)-1] != '`' {
			return "", fmt.Errorf("quoted ordering column is malformed")
		}
		identifier := strings.ReplaceAll(
			value[1:len(value)-1],
			"``",
			"`",
		)
		if identifier == "" {
			return "", fmt.Errorf("ordering column is empty")
		}
		return identifier, nil
	}
	for index, character := range value {
		if character == '_' ||
			character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return "", fmt.Errorf("ordering key is not a direct column list")
	}
	return value, nil
}

func validateClickHouseCreateTableQuery(
	query string,
	engineFull string,
	columns []clickHouseSourceColumnCatalog,
) error {
	if !strings.HasPrefix(query, "CREATE TABLE ") {
		return fmt.Errorf("unexpected CREATE TABLE prefix")
	}
	open := strings.IndexByte(query, '(')
	if open < 0 {
		return fmt.Errorf("CREATE TABLE has no column list")
	}
	closeIndex, err := clickHouseClosingParenthesis(query, open)
	if err != nil {
		return err
	}
	expectedDefinitions := make([]string, len(columns))
	for index, column := range columns {
		expectedDefinitions[index] = clickHouseCatalogIdentifier(column.name) +
			" " + column.rawType
	}
	if query[open+1:closeIndex] != strings.Join(
		expectedDefinitions,
		", ",
	) {
		return fmt.Errorf("unsupported ClickHouse column definition")
	}
	if query[closeIndex+1:] != " ENGINE = "+engineFull {
		return fmt.Errorf("unsupported ClickHouse table suffix")
	}
	return nil
}

func clickHouseClosingParenthesis(value string, open int) (int, error) {
	depth := 0
	quote := byte(0)
	for index := open; index < len(value); index++ {
		character := value[index]
		if quote != 0 {
			if character == quote {
				if index+1 < len(value) && value[index+1] == quote {
					index++
					continue
				}
				quote = 0
			}
			continue
		}
		switch character {
		case '`', '\'', '"':
			quote = character
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index, nil
			}
		}
	}
	return 0, fmt.Errorf("unterminated ClickHouse column list")
}

func clickHouseCatalogIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func clickHouseSourcePolicy(operation, typ string) error {
	return &schema.PolicyError{
		Operation: "discover ClickHouse 24.8 source " + operation,
		Type:      typ,
		Target:    string(schema.ClickHouse),
	}
}

// normalizeClickHouseType remains the broad display normalization used by
// existing callers. Source certification does not rely on it; the pinned
// discovery path above admits exact raw catalog types instead.
func normalizeClickHouseType(value string) string {
	value = strings.ToLower(value)
	value = strings.TrimPrefix(value, "nullable(")
	value = strings.TrimSuffix(value, ")")
	switch value {
	case "int8", "int16", "int32", "uint8", "uint16", "uint32":
		return "integer"
	case "int64", "uint64":
		return "bigint"
	case "string", "fixedstring":
		return "text"
	case "bool", "boolean":
		return "boolean"
	case "datetime", "datetime64":
		return "timestamp"
	default:
		return value
	}
}
