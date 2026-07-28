package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/johndauphine/DMTX/internal/schema"
)

func inspectSQLiteSchema(ctx context.Context, database *sql.DB, name string) (schema.Table, []string, error) {
	createSQL, err := sqliteCreateTableSQL(ctx, database, name)
	if err != nil {
		return schema.Table{}, nil, err
	}
	if isSQLiteVirtualTableSQL(createSQL) {
		return schema.Table{}, nil, &schema.PolicyError{Operation: "discover SQLite virtual table", Type: name, Target: string(schema.SQLite)}
	}

	rows, err := database.QueryContext(ctx, "PRAGMA table_xinfo("+quote(name)+")")
	if err != nil {
		return schema.Table{}, nil, fmt.Errorf("inspect %s: %w", name, err)
	}
	defer rows.Close()
	table := schema.Table{Name: name}
	var names []string
	for rows.Next() {
		var position, notNull, primaryKeyPosition, hidden int
		var columnName, columnType string
		var defaultValue sql.NullString
		if err := rows.Scan(&position, &columnName, &columnType, &notNull, &defaultValue, &primaryKeyPosition, &hidden); err != nil {
			return schema.Table{}, nil, fmt.Errorf("read SQLite column metadata for %s: %w", name, err)
		}
		if hidden != 0 {
			return schema.Table{}, nil, &schema.PolicyError{Operation: "discover SQLite generated or hidden column", Type: columnName, Target: string(schema.SQLite)}
		}
		declaredType, err := schema.ParseSQLiteDeclaredType(columnType)
		if err != nil {
			return schema.Table{}, nil, fmt.Errorf("inspect %s column %s: %w", name, columnName, err)
		}
		var defaultExpression *schema.Expression
		if defaultValue.Valid {
			defaultExpression, err = schema.ParseSQLiteDefault(defaultValue.String)
			if err != nil {
				return schema.Table{}, nil, fmt.Errorf("inspect %s column %s: %w", name, columnName, err)
			}
		}
		table.Columns = append(table.Columns, schema.Column{
			Name:               columnName,
			Type:               declaredType.Base,
			Nullable:           notNull == 0,
			PrimaryKey:         primaryKeyPosition > 0,
			PrimaryKeyPosition: primaryKeyPosition,
			DeclaredType:       declaredType,
			Default:            defaultExpression,
		})
		names = append(names, columnName)
	}
	if err := rows.Err(); err != nil {
		return schema.Table{}, nil, fmt.Errorf("iterate SQLite column metadata for %s: %w", name, err)
	}
	if len(table.Columns) == 0 {
		return schema.Table{}, nil, &schema.PolicyError{Operation: "discover SQLite table", Type: "table without visible columns", Target: string(schema.SQLite)}
	}

	if err := applySQLiteCreateTableFeatures(&table, createSQL); err != nil {
		return schema.Table{}, nil, fmt.Errorf("inspect %s: %w", name, err)
	}
	table.Indexes, err = inspectSQLiteIndexes(ctx, database, name)
	if err != nil {
		return schema.Table{}, nil, err
	}
	table.ForeignKeys, err = inspectSQLiteForeignKeys(ctx, database, name)
	if err != nil {
		return schema.Table{}, nil, err
	}
	if table.AutoIncrementColumn != "" {
		table.SQLiteSequence, err = inspectSQLiteSequence(ctx, database, name)
		if err != nil {
			return schema.Table{}, nil, err
		}
	}
	return table, names, nil
}

func sqliteCreateTableSQL(ctx context.Context, database *sql.DB, name string) (string, error) {
	var statement sql.NullString
	err := database.QueryRowContext(ctx, `SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`, name).Scan(&statement)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("inspect %s: source table disappeared", name)
	}
	if err != nil {
		return "", fmt.Errorf("inspect SQLite table SQL for %s: %w", name, err)
	}
	if !statement.Valid || strings.TrimSpace(statement.String) == "" {
		return "", &schema.PolicyError{Operation: "discover SQLite table SQL", Type: name, Target: string(schema.SQLite)}
	}
	return statement.String, nil
}

func rejectSQLiteTableTriggers(ctx context.Context, database *sql.DB, table string) error {
	var trigger string
	err := database.QueryRowContext(
		ctx,
		`SELECT name FROM sqlite_schema WHERE type = 'trigger' AND tbl_name = ? ORDER BY name LIMIT 1`,
		table,
	).Scan(&trigger)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect SQLite triggers for %s: %w", table, err)
	}
	return &schema.PolicyError{
		Operation: "discover SQLite table trigger",
		Type:      table + "." + trigger,
		Target:    string(schema.SQLite),
	}
}

func isSQLiteVirtualTableSQL(statement string) bool {
	normalized := strings.ToUpper(strings.Join(strings.Fields(statement), " "))
	return strings.HasPrefix(normalized, "CREATE VIRTUAL TABLE ")
}

func applySQLiteCreateTableFeatures(table *schema.Table, statement string) error {
	bodyStart, bodyEnd, err := sqliteTableBodyBounds(statement)
	if err != nil {
		return err
	}
	body := statement[bodyStart+1 : bodyEnd]
	if err := rejectSQLiteColumnCollations(table, body); err != nil {
		return err
	}
	if containsSQLKeyword(body, "CONFLICT") {
		return &schema.PolicyError{Operation: "discover SQLite conflict algorithm", Type: table.Name, Target: string(schema.SQLite)}
	}
	if containsSQLKeyword(body, "CONSTRAINT") {
		return &schema.PolicyError{Operation: "discover named SQLite constraint", Type: table.Name, Target: string(schema.SQLite)}
	}
	if containsSQLKeyword(body, "DESC") {
		return &schema.PolicyError{Operation: "discover SQLite table ordering", Type: table.Name, Target: string(schema.SQLite)}
	}
	if containsSQLKeyword(body, "MATCH") {
		return &schema.PolicyError{Operation: "discover SQLite MATCH semantics", Type: table.Name, Target: string(schema.SQLite)}
	}
	tail := statement[bodyEnd+1:]
	table.SQLiteStrict = containsSQLKeyword(tail, "STRICT")
	table.SQLiteWithoutRowID = containsSQLKeyword(tail, "WITHOUT") && containsSQLKeyword(tail, "ROWID")
	if containsSQLKeyword(statement, "DEFERRABLE") {
		return &schema.PolicyError{Operation: "discover SQLite deferred foreign key", Type: table.Name, Target: string(schema.SQLite)}
	}

	checks, err := extractSQLiteChecks(statement[:bodyEnd+1])
	if err != nil {
		return err
	}
	table.Checks = checks

	if !containsSQLKeyword(statement, "AUTOINCREMENT") {
		return nil
	}
	keys := make([]schema.Column, 0, 1)
	for _, column := range table.Columns {
		if column.PrimaryKeyPosition > 0 {
			keys = append(keys, column)
		}
	}
	if len(keys) != 1 || keys[0].DeclaredType == nil || keys[0].DeclaredType.Base != "integer" {
		return &schema.PolicyError{Operation: "discover SQLite AUTOINCREMENT", Type: table.Name, Target: string(schema.SQLite)}
	}
	table.AutoIncrementColumn = keys[0].Name
	return nil
}

func sqliteTableBodyBounds(statement string) (int, int, error) {
	start := -1
	for index := 0; index < len(statement); index++ {
		if statement[index] == '\'' || statement[index] == '"' || statement[index] == '`' {
			next, ok := consumeSQLiteQuoted(statement, index, statement[index])
			if !ok {
				return 0, 0, &schema.PolicyError{Operation: "parse SQLite table SQL quote", Type: statement, Target: string(schema.SQLite)}
			}
			index = next
			continue
		}
		if statement[index] == '[' {
			next := strings.IndexByte(statement[index+1:], ']')
			if next < 0 {
				return 0, 0, &schema.PolicyError{Operation: "parse SQLite table SQL identifier", Type: statement, Target: string(schema.SQLite)}
			}
			index += next + 1
			continue
		}
		if statement[index] == '(' {
			start = index
			break
		}
	}
	if start < 0 {
		return 0, 0, &schema.PolicyError{Operation: "parse SQLite table SQL", Type: statement, Target: string(schema.SQLite)}
	}
	end, ok := matchingSQLiteParenthesis(statement, start)
	if !ok {
		return 0, 0, &schema.PolicyError{Operation: "parse SQLite table SQL parentheses", Type: statement, Target: string(schema.SQLite)}
	}
	return start, end, nil
}

func rejectSQLiteColumnCollations(table *schema.Table, body string) error {
	definitions, err := splitSQLiteDefinitions(body)
	if err != nil {
		return err
	}
	for _, definition := range definitions {
		token, quoted := leadingSQLiteDefinitionToken(definition)
		if !quoted {
			switch strings.ToUpper(token) {
			case "CONSTRAINT", "PRIMARY", "UNIQUE", "CHECK", "FOREIGN":
				continue
			}
		}
		if containsSQLKeyword(definition, "COLLATE") {
			return &schema.PolicyError{Operation: "discover SQLite column collation", Type: table.Name + "." + token, Target: string(schema.SQLite)}
		}
	}
	return nil
}

func splitSQLiteDefinitions(body string) ([]string, error) {
	definitions := make([]string, 0)
	start, depth := 0, 0
	for index := 0; index < len(body); index++ {
		if body[index] == '\'' || body[index] == '"' || body[index] == '`' {
			next, ok := consumeSQLiteQuoted(body, index, body[index])
			if !ok {
				return nil, &schema.PolicyError{Operation: "parse SQLite column definition quote", Type: body, Target: string(schema.SQLite)}
			}
			index = next
			continue
		}
		if body[index] == '[' {
			next := strings.IndexByte(body[index+1:], ']')
			if next < 0 {
				return nil, &schema.PolicyError{Operation: "parse SQLite column definition identifier", Type: body, Target: string(schema.SQLite)}
			}
			index += next + 1
			continue
		}
		switch body[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, &schema.PolicyError{Operation: "parse SQLite column definition parentheses", Type: body, Target: string(schema.SQLite)}
			}
		case ',':
			if depth == 0 {
				definitions = append(definitions, strings.TrimSpace(body[start:index]))
				start = index + 1
			}
		}
	}
	if depth != 0 {
		return nil, &schema.PolicyError{Operation: "parse SQLite column definition parentheses", Type: body, Target: string(schema.SQLite)}
	}
	definitions = append(definitions, strings.TrimSpace(body[start:]))
	return definitions, nil
}

func leadingSQLiteDefinitionToken(definition string) (string, bool) {
	trimmed := strings.TrimSpace(definition)
	if trimmed == "" {
		return "", false
	}
	if trimmed[0] == '\'' || trimmed[0] == '"' || trimmed[0] == '`' {
		end, ok := consumeSQLiteQuoted(trimmed, 0, trimmed[0])
		if !ok {
			return "", true
		}
		return strings.ReplaceAll(trimmed[1:end], string([]byte{trimmed[0], trimmed[0]}), string(trimmed[0])), true
	}
	if trimmed[0] == '[' {
		if end := strings.IndexByte(trimmed[1:], ']'); end >= 0 {
			return trimmed[1 : end+1], true
		}
		return "", true
	}
	end := 0
	for end < len(trimmed) && isSQLiteIdentifierPart(trimmed[end]) {
		end++
	}
	return trimmed[:end], false
}

func extractSQLiteChecks(statement string) ([]schema.CheckConstraint, error) {
	checks := make([]schema.CheckConstraint, 0)
	for index := 0; index < len(statement); {
		if statement[index] == '\'' || statement[index] == '"' || statement[index] == '`' {
			next, ok := consumeSQLiteQuoted(statement, index, statement[index])
			if !ok {
				return nil, &schema.PolicyError{Operation: "parse SQLite CHECK quote", Type: statement, Target: string(schema.SQLite)}
			}
			index = next + 1
			continue
		}
		if statement[index] == '[' {
			next := strings.IndexByte(statement[index+1:], ']')
			if next < 0 {
				return nil, &schema.PolicyError{Operation: "parse SQLite CHECK identifier", Type: statement, Target: string(schema.SQLite)}
			}
			index += next + 2
			continue
		}
		if !isSQLiteIdentifierStart(statement[index]) {
			index++
			continue
		}
		start := index
		for index < len(statement) && isSQLiteIdentifierPart(statement[index]) {
			index++
		}
		if !strings.EqualFold(statement[start:index], "CHECK") {
			continue
		}
		for index < len(statement) && (statement[index] == ' ' || statement[index] == '\t' || statement[index] == '\r' || statement[index] == '\n') {
			index++
		}
		if index >= len(statement) || statement[index] != '(' {
			return nil, &schema.PolicyError{Operation: "parse SQLite CHECK", Type: statement[start:], Target: string(schema.SQLite)}
		}
		end, ok := matchingSQLiteParenthesis(statement, index)
		if !ok {
			return nil, &schema.PolicyError{Operation: "parse SQLite CHECK parentheses", Type: statement[start:], Target: string(schema.SQLite)}
		}
		expression, err := schema.ParseSQLiteCheckExpression(statement[index+1 : end])
		if err != nil {
			return nil, err
		}
		checks = append(checks, schema.CheckConstraint{Expression: expression})
		index = end + 1
	}
	sort.SliceStable(checks, func(left, right int) bool {
		return checks[left].Expression.CanonicalSQL() < checks[right].Expression.CanonicalSQL()
	})
	return checks, nil
}

func inspectSQLiteIndexes(ctx context.Context, database *sql.DB, table string) ([]schema.Index, error) {
	rows, err := database.QueryContext(ctx, "PRAGMA index_list("+quote(table)+")")
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite indexes for %s: %w", table, err)
	}
	type sqliteIndexMetadata struct {
		sequence int
		name     string
		unique   int
		origin   string
		partial  int
	}
	metadata := make([]sqliteIndexMetadata, 0)
	for rows.Next() {
		var index sqliteIndexMetadata
		if err := rows.Scan(&index.sequence, &index.name, &index.unique, &index.origin, &index.partial); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("read SQLite index metadata for %s: %w", table, err)
		}
		metadata = append(metadata, index)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("iterate SQLite indexes for %s: %w", table, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite indexes for %s: %w", table, err)
	}
	indexes := make([]schema.Index, 0, len(metadata))
	for _, metadata := range metadata {
		name, unique, origin, partial := metadata.name, metadata.unique, metadata.origin, metadata.partial
		if origin == "pk" {
			columns, err := inspectSQLiteIndexColumns(ctx, database, name)
			if err != nil {
				return nil, err
			}
			for _, column := range columns {
				if column.Descending || column.Collation != "" && !strings.EqualFold(column.Collation, "BINARY") {
					return nil, &schema.PolicyError{Operation: "discover SQLite primary-key ordering", Type: name, Target: string(schema.SQLite)}
				}
			}
			continue
		}
		if partial != 0 {
			return nil, &schema.PolicyError{Operation: "discover SQLite partial index", Type: name, Target: string(schema.SQLite)}
		}
		if origin != "c" && origin != "u" {
			return nil, &schema.PolicyError{Operation: "discover SQLite index origin", Type: origin, Target: string(schema.SQLite)}
		}
		columns, err := inspectSQLiteIndexColumns(ctx, database, name)
		if err != nil {
			return nil, err
		}
		index := schema.Index{Name: name, Unique: unique != 0, Inline: origin == "u", Columns: columns}
		if index.Inline {
			index.Name = ""
		}
		indexes = append(indexes, index)
	}
	sort.SliceStable(indexes, func(left, right int) bool {
		return sqliteIndexSortKey(indexes[left]) < sqliteIndexSortKey(indexes[right])
	})
	return indexes, nil
}

func inspectSQLiteIndexColumns(ctx context.Context, database *sql.DB, indexName string) ([]schema.IndexColumn, error) {
	rows, err := database.QueryContext(ctx, "PRAGMA index_xinfo("+quote(indexName)+")")
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite index columns for %s: %w", indexName, err)
	}
	defer rows.Close()
	type indexedColumn struct {
		sequence int
		column   schema.IndexColumn
	}
	indexed := make([]indexedColumn, 0)
	for rows.Next() {
		var sequence, columnID, descending, key int
		var name, collation sql.NullString
		if err := rows.Scan(&sequence, &columnID, &name, &descending, &collation, &key); err != nil {
			return nil, fmt.Errorf("read SQLite index columns for %s: %w", indexName, err)
		}
		if key == 0 {
			continue
		}
		if columnID < 0 || !name.Valid {
			return nil, &schema.PolicyError{Operation: "discover SQLite expression index", Type: indexName, Target: string(schema.SQLite)}
		}
		indexed = append(indexed, indexedColumn{sequence: sequence, column: schema.IndexColumn{Name: name.String, Descending: descending != 0, Collation: strings.ToUpper(collation.String)}})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite index columns for %s: %w", indexName, err)
	}
	sort.SliceStable(indexed, func(left, right int) bool { return indexed[left].sequence < indexed[right].sequence })
	columns := make([]schema.IndexColumn, len(indexed))
	for index, value := range indexed {
		columns[index] = value.column
	}
	if len(columns) == 0 {
		return nil, &schema.PolicyError{Operation: "discover SQLite index", Type: indexName, Target: string(schema.SQLite)}
	}
	return columns, nil
}

func sqliteIndexSortKey(index schema.Index) string {
	parts := []string{strconv.FormatBool(index.Inline), strconv.FormatBool(index.Unique), index.Name}
	for _, column := range index.Columns {
		parts = append(parts, column.Name, strconv.FormatBool(column.Descending), column.Collation)
	}
	return strings.Join(parts, "\x00")
}

func inspectSQLiteForeignKeys(ctx context.Context, database *sql.DB, table string) ([]schema.ForeignKey, error) {
	rows, err := database.QueryContext(ctx, "PRAGMA foreign_key_list("+quote(table)+")")
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite foreign keys for %s: %w", table, err)
	}
	defer rows.Close()
	type foreignKeyPart struct {
		id, sequence                 int
		referencedTable, localColumn string
		referencedColumn             sql.NullString
		onUpdate, onDelete, match    string
	}
	parts := make([]foreignKeyPart, 0)
	for rows.Next() {
		var part foreignKeyPart
		if err := rows.Scan(&part.id, &part.sequence, &part.referencedTable, &part.localColumn, &part.referencedColumn, &part.onUpdate, &part.onDelete, &part.match); err != nil {
			return nil, fmt.Errorf("read SQLite foreign keys for %s: %w", table, err)
		}
		parts = append(parts, part)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate SQLite foreign keys for %s: %w", table, err)
	}
	sort.SliceStable(parts, func(left, right int) bool {
		if parts[left].id == parts[right].id {
			return parts[left].sequence < parts[right].sequence
		}
		return parts[left].id < parts[right].id
	})
	byID := make(map[int]*schema.ForeignKey)
	for _, part := range parts {
		foreignKey := byID[part.id]
		if foreignKey == nil {
			foreignKey = &schema.ForeignKey{ReferencedTable: part.referencedTable, OnUpdate: strings.ToUpper(part.onUpdate), OnDelete: strings.ToUpper(part.onDelete), Match: strings.ToUpper(part.match)}
			byID[part.id] = foreignKey
		}
		foreignKey.Columns = append(foreignKey.Columns, part.localColumn)
		if part.referencedColumn.Valid {
			foreignKey.ReferencedColumns = append(foreignKey.ReferencedColumns, part.referencedColumn.String)
		}
	}
	foreignKeys := make([]schema.ForeignKey, 0, len(byID))
	for _, foreignKey := range byID {
		foreignKeys = append(foreignKeys, *foreignKey)
	}
	sort.SliceStable(foreignKeys, func(left, right int) bool {
		return sqliteForeignKeySortKey(foreignKeys[left]) < sqliteForeignKeySortKey(foreignKeys[right])
	})
	return foreignKeys, nil
}

func sqliteForeignKeySortKey(foreignKey schema.ForeignKey) string {
	return strings.Join(append(append([]string{}, foreignKey.Columns...), append([]string{foreignKey.ReferencedTable}, foreignKey.ReferencedColumns...)...), "\x00") + "\x00" + foreignKey.OnUpdate + "\x00" + foreignKey.OnDelete + "\x00" + foreignKey.Match
}

func inspectSQLiteSequence(ctx context.Context, database *sql.DB, table string) (*int64, error) {
	var sequence int64
	err := database.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name = ?`, table).Scan(&sequence)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect SQLite AUTOINCREMENT sequence for %s: %w", table, err)
	}
	return &sequence, nil
}

func containsSQLKeyword(value, keyword string) bool {
	for index := 0; index < len(value); {
		if value[index] == '\'' || value[index] == '"' || value[index] == '`' {
			next, ok := consumeSQLiteQuoted(value, index, value[index])
			if !ok {
				return false
			}
			index = next + 1
			continue
		}
		if value[index] == '[' {
			next := strings.IndexByte(value[index+1:], ']')
			if next < 0 {
				return false
			}
			index += next + 2
			continue
		}
		if !isSQLiteIdentifierStart(value[index]) {
			index++
			continue
		}
		start := index
		for index < len(value) && isSQLiteIdentifierPart(value[index]) {
			index++
		}
		if strings.EqualFold(value[start:index], keyword) {
			return true
		}
	}
	return false
}

func matchingSQLiteParenthesis(value string, start int) (int, bool) {
	depth := 0
	for index := start; index < len(value); index++ {
		if value[index] == '\'' || value[index] == '"' || value[index] == '`' {
			next, ok := consumeSQLiteQuoted(value, index, value[index])
			if !ok {
				return 0, false
			}
			index = next
			continue
		}
		if value[index] == '[' {
			next := strings.IndexByte(value[index+1:], ']')
			if next < 0 {
				return 0, false
			}
			index += next + 1
			continue
		}
		switch value[index] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return index, true
			}
		}
	}
	return 0, false
}

func consumeSQLiteQuoted(value string, start int, delimiter byte) (int, bool) {
	for index := start + 1; index < len(value); index++ {
		if value[index] != delimiter {
			continue
		}
		if index+1 < len(value) && value[index+1] == delimiter {
			index++
			continue
		}
		return index, true
	}
	return 0, false
}

func isSQLiteIdentifierStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func isSQLiteIdentifierPart(value byte) bool {
	return isSQLiteIdentifierStart(value) || value >= '0' && value <= '9'
}

func validateSQLiteSchemaBeforeMutation(ctx context.Context, source, target *sql.DB, names []string, mode string) error {
	for _, name := range names {
		if err := rejectSQLiteTableTriggers(ctx, source, name); err != nil {
			return err
		}
		sourceTable, _, err := inspectSQLiteSchema(ctx, source, name)
		if err != nil {
			return err
		}
		if _, err := schema.CreateTable(schema.SQLite, sourceTable); err != nil {
			return fmt.Errorf("plan SQLite table %s before mutation: %w", name, err)
		}
		if _, err := schema.CreateIndexes(schema.SQLite, sourceTable); err != nil {
			return fmt.Errorf("plan SQLite indexes for %s before mutation: %w", name, err)
		}
		if !hasPrimaryKey(sourceTable) {
			return fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name)
		}
		if mode != "upsert" {
			continue
		}
		exists, err := tableExists(ctx, target, name)
		if err != nil {
			return fmt.Errorf("check target table %s before mutation: %w", name, err)
		}
		if !exists {
			continue
		}
		targetTable, _, err := inspectSQLiteSchema(ctx, target, name)
		if err != nil {
			return fmt.Errorf("inspect upsert target %s: %w", name, err)
		}
		if err := validateSQLiteUpsertCompatibility(sourceTable, targetTable); err != nil {
			return err
		}
	}
	return nil
}

func validateSQLiteUpsertCompatibility(source, target schema.Table) error {
	targetColumns := make(map[string]schema.Column, len(target.Columns))
	for _, column := range target.Columns {
		targetColumns[column.Name] = column
	}
	sourceColumns := make(map[string]bool, len(source.Columns))
	for _, sourceColumn := range source.Columns {
		sourceColumns[sourceColumn.Name] = true
		targetColumn, ok := targetColumns[sourceColumn.Name]
		if !ok {
			return &schema.PolicyError{Operation: "validate SQLite upsert target column", Type: source.Name + "." + sourceColumn.Name, Target: string(schema.SQLite)}
		}
		if !sameSQLiteDeclaredType(sourceColumn.DeclaredType, targetColumn.DeclaredType) || sourceColumn.Nullable != targetColumn.Nullable {
			return &schema.PolicyError{Operation: "validate SQLite upsert target column shape", Type: source.Name + "." + sourceColumn.Name, Target: string(schema.SQLite)}
		}
	}
	for _, targetColumn := range target.Columns {
		if sourceColumns[targetColumn.Name] {
			continue
		}
		if !targetColumn.Nullable && targetColumn.Default == nil {
			return &schema.PolicyError{Operation: "validate SQLite upsert target extra column", Type: target.Name + "." + targetColumn.Name, Target: string(schema.SQLite)}
		}
	}
	if !equalStrings(primaryKeyColumns(source), primaryKeyColumns(target)) {
		return &schema.PolicyError{Operation: "validate SQLite upsert target primary key", Type: source.Name, Target: string(schema.SQLite)}
	}
	return nil
}

func sameSQLiteDeclaredType(left, right *schema.DeclaredType) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Base != right.Base || len(left.Arguments) != len(right.Arguments) {
		return false
	}
	for index := range left.Arguments {
		if left.Arguments[index] != right.Arguments[index] {
			return false
		}
	}
	return true
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
