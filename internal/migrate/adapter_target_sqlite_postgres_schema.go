package migrate

import (
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectPostgresTableForSQLite maps only PostgreSQL 16 source shapes whose
// stored values and relational behavior have a conservative SQLite
// representation. PostgreSQL catalog SQL is never copied into target DDL:
// defaults and CHECK constraints are reconstructed through schema's
// structural parsers.
func projectPostgresTableForSQLite(
	source schema.Table,
) (schema.Table, error) {
	if strings.TrimSpace(source.Schema) == "" ||
		source.Name == "" ||
		strings.ContainsRune(source.Name, '\x00') ||
		!utf8.ValidString(source.Name) ||
		source.SQLiteStrict ||
		source.SQLiteWithoutRowID ||
		strings.TrimSpace(source.MySQLCollation) != "" ||
		len(source.ClickHouseOrderBy) != 0 {
		return schema.Table{}, sqlitePostgresProjectionPolicy(
			"map PostgreSQL table metadata",
			source.Name,
		)
	}
	if strings.HasPrefix(strings.ToLower(source.Name), "sqlite_") {
		return schema.Table{}, sqlitePostgresProjectionPolicy(
			"map reserved SQLite object name",
			source.Name,
		)
	}

	projected := clonePostgresSQLiteTable(source)
	projected.Schema = ""
	projected.ClickHouseOrderBy = nil

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	foldedColumns := make(map[string]string, len(source.Columns))
	primaryPositions := make(map[int]string)
	for index, column := range source.Columns {
		if column.Name == "" ||
			strings.ContainsRune(column.Name, '\x00') ||
			!utf8.ValidString(column.Name) {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL column name",
				source.Name+"."+column.Name,
			)
		}
		folded := strings.ToLower(column.Name)
		if earlier, duplicate := foldedColumns[folded]; duplicate {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map case-insensitive SQLite column names",
				source.Name+"."+earlier+" and "+column.Name,
			)
		}
		foldedColumns[folded] = column.Name
		sourceColumns[column.Name] = column

		hasPrimaryPosition := column.PrimaryKeyPosition > 0
		if column.PrimaryKey != hasPrimaryPosition {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL primary-key shape",
				source.Name+"."+column.Name,
			)
		}
		if hasPrimaryPosition {
			if column.Nullable {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL primary-key nullability",
					source.Name+"."+column.Name,
				)
			}
			if earlier, duplicate :=
				primaryPositions[column.PrimaryKeyPosition]; duplicate {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL primary-key order",
					source.Name+"."+earlier+" and "+column.Name,
				)
			}
			primaryPositions[column.PrimaryKeyPosition] = column.Name
			if !postgresSQLiteExactReference(column) {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL primary-key comparison",
					source.Name+"."+column.Name,
				)
			}
		}

		target, err := projectPostgresColumnForSQLite(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL column %s.%s to SQLite: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}
	for position := 1; position <= len(primaryPositions); position++ {
		if _, exists := primaryPositions[position]; !exists {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL primary-key order",
				source.Name,
			)
		}
	}

	for _, index := range source.Indexes {
		if index.Inline ||
			index.Name == "" ||
			strings.ContainsRune(index.Name, '\x00') ||
			!utf8.ValidString(index.Name) ||
			len(index.Columns) == 0 {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL index shape",
				source.Name+"."+index.Name,
			)
		}
		if strings.HasPrefix(strings.ToLower(index.Name), "sqlite_") {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map reserved SQLite object name",
				index.Name,
			)
		}
		seen := make(map[string]struct{}, len(index.Columns))
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL index column",
					source.Name+"."+index.Name+"."+indexed.Name,
				)
			}
			if _, duplicate := seen[indexed.Name]; duplicate {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL index columns",
					source.Name+"."+index.Name+"."+indexed.Name,
				)
			}
			seen[indexed.Name] = struct{}{}
			if column.Nullable ||
				!postgresSQLiteExactIndexComparison(column, indexed) {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL index comparison",
					source.Name+"."+index.Name+"."+indexed.Name,
				)
			}
		}
	}

	for index := range projected.ForeignKeys {
		sourceForeignKey := source.ForeignKeys[index]
		foreignKey := &projected.ForeignKeys[index]
		if sourceForeignKey.Name == "" ||
			strings.ContainsRune(sourceForeignKey.Name, '\x00') ||
			!utf8.ValidString(sourceForeignKey.Name) ||
			len(sourceForeignKey.Columns) == 0 ||
			len(sourceForeignKey.Columns) !=
				len(sourceForeignKey.ReferencedColumns) ||
			sourceForeignKey.ReferencedTable == "" ||
			strings.ContainsRune(
				sourceForeignKey.ReferencedTable,
				'\x00',
			) ||
			!utf8.ValidString(sourceForeignKey.ReferencedTable) {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL foreign-key shape",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		switch strings.ToUpper(strings.TrimSpace(
			sourceForeignKey.Match,
		)) {
		case "SIMPLE":
			// SQLite implements the same nullable-column behavior as
			// PostgreSQL MATCH SIMPLE, but emits no MATCH clause.
			foreignKey.Match = "NONE"
		default:
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL foreign-key match",
				source.Name+"."+sourceForeignKey.Name,
			)
		}
		foreignKey.OnUpdate = strings.ToUpper(strings.TrimSpace(
			sourceForeignKey.OnUpdate,
		))
		foreignKey.OnDelete = strings.ToUpper(strings.TrimSpace(
			sourceForeignKey.OnDelete,
		))
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			switch action {
			case "NO ACTION", "RESTRICT", "CASCADE":
			case "SET NULL":
				for _, name := range sourceForeignKey.Columns {
					column, exists := sourceColumns[name]
					if !exists || !column.Nullable {
						return schema.Table{},
							sqlitePostgresProjectionPolicy(
								"map PostgreSQL SET NULL foreign key",
								source.Name+"."+
									sourceForeignKey.Name,
							)
					}
				}
			default:
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key action",
					source.Name+"."+sourceForeignKey.Name,
				)
			}
		}
		seenLocal := make(map[string]struct{}, len(sourceForeignKey.Columns))
		seenReferenced := make(
			map[string]struct{},
			len(sourceForeignKey.ReferencedColumns),
		)
		for pairIndex, name := range sourceForeignKey.Columns {
			referencedName :=
				sourceForeignKey.ReferencedColumns[pairIndex]
			if name == "" ||
				referencedName == "" ||
				strings.ContainsRune(name, '\x00') ||
				strings.ContainsRune(referencedName, '\x00') ||
				!utf8.ValidString(name) ||
				!utf8.ValidString(referencedName) {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key columns",
					source.Name+"."+sourceForeignKey.Name,
				)
			}
			if _, duplicate := seenLocal[name]; duplicate {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key columns",
					source.Name+"."+sourceForeignKey.Name+"."+name,
				)
			}
			if _, duplicate := seenReferenced[referencedName]; duplicate {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key referenced columns",
					source.Name+"."+sourceForeignKey.Name+"."+
						referencedName,
				)
			}
			seenLocal[name] = struct{}{}
			seenReferenced[referencedName] = struct{}{}
			column, exists := sourceColumns[name]
			if !exists || !postgresSQLiteExactReference(column) {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL foreign-key comparison",
					source.Name+"."+sourceForeignKey.Name+"."+name,
				)
			}
		}
		// SQLite discovery intentionally rejects named table constraints.
		// The behavior is preserved; the PostgreSQL catalog name is not.
		foreignKey.Name = ""
	}

	projected.Checks = make(
		[]schema.CheckConstraint,
		0,
		len(source.Checks)+len(source.Columns),
	)
	for _, sourceCheck := range source.Checks {
		if sourceCheck.Name == "" ||
			strings.ContainsRune(sourceCheck.Name, '\x00') ||
			!utf8.ValidString(sourceCheck.Name) {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL CHECK shape",
				source.Name,
			)
		}
		referenced, err := schema.ReferencedCheckColumns(
			sourceCheck.Expression,
			source.Columns,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL CHECK %s.%s to SQLite: %w",
				source.Name,
				sourceCheck.Name,
				err,
			)
		}
		for _, name := range referenced {
			column, exists := sourceColumns[name]
			if !exists || !postgresSQLiteExactCheck(column) {
				return schema.Table{}, sqlitePostgresProjectionPolicy(
					"map PostgreSQL CHECK comparison",
					source.Name+"."+sourceCheck.Name+"."+name,
				)
			}
		}
		expression, err := schema.ParseSQLiteCheckExpression(
			sourceCheck.Expression.CanonicalSQL(),
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL CHECK %s.%s to SQLite: %w",
				source.Name,
				sourceCheck.Name,
				err,
			)
		}
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{Expression: expression},
		)
	}

	for _, column := range source.Columns {
		if !postgresSQLiteBoolean(column) {
			continue
		}
		expression, err := schema.ParseSQLiteCheckExpression(
			sqlitePostgresIdentifier(column.Name) + " IN (0, 1)",
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"plan SQLite boolean domain for %s.%s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{Expression: expression},
		)
	}

	if projected.Identity != nil {
		identityColumn, exists := sourceColumns[projected.Identity.Column]
		if !exists ||
			!postgresSQLiteBigint(identityColumn) ||
			identityColumn.Default != nil ||
			identityColumn.PrimaryKeyPosition != 1 ||
			len(primaryPositions) != 1 ||
			projected.Identity.Generation != schema.IdentityByDefault ||
			projected.Identity.Frontier != nil &&
				*projected.Identity.Frontier < 1 {
			return schema.Table{}, sqlitePostgresProjectionPolicy(
				"map PostgreSQL identity",
				source.Name,
			)
		}
	}

	if _, err := schema.CreateTable(schema.SQLite, projected); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite table %s: %w",
			source.Name,
			err,
		)
	}
	if _, err := schema.CreateIndexes(schema.SQLite, projected); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite indexes for %s: %w",
			source.Name,
			err,
		)
	}
	if _, err := schema.SQLiteSequencePlan(projected); err != nil {
		return schema.Table{}, fmt.Errorf(
			"plan SQLite identity for %s: %w",
			source.Name,
			err,
		)
	}
	return projected, nil
}

func clonePostgresSQLiteTable(source schema.Table) schema.Table {
	cloned := source
	cloned.Identity = cloneSchemaIdentity(source.Identity)
	cloned.ClickHouseOrderBy = append(
		[]string(nil),
		source.ClickHouseOrderBy...,
	)
	cloned.Columns = append([]schema.Column(nil), source.Columns...)
	for index := range cloned.Columns {
		if source.Columns[index].DeclaredType != nil {
			declaration := *source.Columns[index].DeclaredType
			declaration.Arguments = append(
				[]int(nil),
				source.Columns[index].DeclaredType.Arguments...,
			)
			cloned.Columns[index].DeclaredType = &declaration
		}
		cloned.Columns[index].Default = cloneSchemaExpression(
			source.Columns[index].Default,
		)
	}
	cloned.Indexes = clonePostgresProjectionIndexes(source.Indexes)
	cloned.ForeignKeys = clonePostgresProjectionForeignKeys(
		source.ForeignKeys,
	)
	cloned.Checks = append([]schema.CheckConstraint(nil), source.Checks...)
	return cloned
}

func projectPostgresColumnForSQLite(
	source schema.Column,
) (schema.Column, error) {
	target := source
	target.Default = nil
	sourceType := normalizedPostgresSQLiteType(source.Type)
	declaredBase := sourceType
	var arguments []int
	if source.DeclaredType != nil {
		declaredBase = normalizedPostgresSQLiteType(
			source.DeclaredType.Base,
		)
		arguments = append(
			[]int(nil),
			source.DeclaredType.Arguments...,
		)
	}
	declaration := func(name string, values ...int) {
		target.DeclaredType = &schema.DeclaredType{
			Base:      name,
			Arguments: append([]int(nil), values...),
		}
	}
	fail := func(operation string) (schema.Column, error) {
		return schema.Column{}, sqlitePostgresProjectionPolicy(
			operation,
			source.Name+"."+declaredBase,
		)
	}
	requireImplicitDeclaration := func() bool {
		return source.DeclaredType == nil && len(arguments) == 0
	}

	switch sourceType {
	case "integer":
		if declaredBase != "integer" || !requireImplicitDeclaration() {
			return fail("map PostgreSQL integer type")
		}
		target.Type = sourceType
		declaration("integer")
	case "bigint":
		if declaredBase != "bigint" || !requireImplicitDeclaration() {
			return fail("map PostgreSQL bigint type")
		}
		target.Type = sourceType
		declaration("bigint")
	case "boolean":
		if declaredBase != "boolean" || !requireImplicitDeclaration() {
			return fail("map PostgreSQL boolean type")
		}
		target.Type = sourceType
		declaration("boolean")
	case "numeric":
		if declaredBase != "numeric" ||
			source.DeclaredType == nil ||
			len(arguments) != 2 ||
			arguments[0] < 1 ||
			arguments[0] > 18 ||
			arguments[1] != 0 {
			return fail("map PostgreSQL exact numeric type")
		}
		target.Type = sourceType
		declaration("bigint")
	case "text":
		if declaredBase != "text" || !requireImplicitDeclaration() {
			return fail("map PostgreSQL text type")
		}
		target.Type = sourceType
		declaration("text")
	case "varchar":
		if declaredBase != "varchar" ||
			source.DeclaredType == nil ||
			len(arguments) != 1 ||
			arguments[0] < 1 ||
			arguments[0] > 10_485_760 {
			return fail("map PostgreSQL varying text type")
		}
		target.Type = sourceType
		declaration("varchar", arguments[0])
	case "bytea":
		if declaredBase != "bytea" || !requireImplicitDeclaration() {
			return fail("map PostgreSQL binary type")
		}
		target.Type = sourceType
		declaration("blob")
	case "date":
		if declaredBase != "date" || !requireImplicitDeclaration() {
			return fail("map PostgreSQL date type")
		}
		target.Type = sourceType
		declaration("date")
	case "time", "timestamp":
		if declaredBase != sourceType {
			return fail("map PostgreSQL temporal type")
		}
		switch {
		case source.DeclaredType == nil:
			if len(arguments) != 0 {
				return fail("map PostgreSQL temporal type")
			}
			target.Type = sourceType
			declaration(sourceType)
		case len(arguments) == 1 &&
			arguments[0] >= 0 &&
			arguments[0] <= 6:
			target.Type = sourceType
			declaration(sourceType, arguments[0])
		default:
			return fail("map PostgreSQL temporal type")
		}
	case "char":
		return fail("map PostgreSQL fixed-width blank-padding type")
	case "real", "double precision":
		return fail("map PostgreSQL floating-point type")
	case "timestamptz":
		return fail("map PostgreSQL timezone-aware temporal type")
	case "json", "jsonb":
		return fail("map PostgreSQL JSON type")
	case "uuid":
		return fail("map PostgreSQL UUID type")
	default:
		return fail("map PostgreSQL type")
	}

	projectedDefault, err := projectPostgresDefaultForSQLite(source)
	if err != nil {
		return schema.Column{}, err
	}
	target.Default = projectedDefault
	return target, nil
}

func projectPostgresDefaultForSQLite(
	source schema.Column,
) (*schema.Expression, error) {
	if source.Default == nil {
		return nil, nil
	}
	signature, err := schema.PlannedPostgresDefaultSignature(source)
	if err != nil {
		return nil, sqlitePostgresProjectionPolicy(
			"map PostgreSQL default",
			source.Name,
		)
	}
	if !signature.Present {
		return nil, nil
	}

	var value string
	switch signature.Kind {
	case schema.PostgresDefaultBoolean:
		value = strings.ToUpper(signature.Value)
	case schema.PostgresDefaultInteger,
		schema.PostgresDefaultBigint,
		schema.PostgresDefaultNumeric:
		value = signature.Value
	case schema.PostgresDefaultText,
		schema.PostgresDefaultVarchar:
		value = "'" + strings.ReplaceAll(
			signature.Value,
			"'",
			"''",
		) + "'"
	case schema.PostgresDefaultBytea:
		value = "X'" + strings.ToLower(signature.Value) + "'"
	case schema.PostgresDefaultCurrentTime:
		value = "CURRENT_TIME"
	case schema.PostgresDefaultCurrentDate:
		value = "CURRENT_DATE"
	case schema.PostgresDefaultCurrentTimestamp:
		value = "CURRENT_TIMESTAMP"
	default:
		return nil, sqlitePostgresProjectionPolicy(
			"map PostgreSQL default",
			source.Name,
		)
	}
	expression, err := schema.ParseSQLiteDefault(value)
	if err != nil {
		return nil, fmt.Errorf(
			"map PostgreSQL default for %s to SQLite: %w",
			source.Name,
			err,
		)
	}
	return expression, nil
}

func postgresSQLiteExactIndexComparison(
	column schema.Column,
	indexed schema.IndexColumn,
) bool {
	collation := strings.ToUpper(strings.TrimSpace(indexed.Collation))
	if postgresSQLiteExactReference(column) {
		return collation == ""
	}
	switch normalizedPostgresSQLiteColumnBase(column) {
	case "text", "varchar":
		return collation == "BINARY"
	default:
		return false
	}
}

func postgresSQLiteExactReference(column schema.Column) bool {
	return postgresSQLiteExactReferenceKind(column) != ""
}

func postgresSQLiteExactReferenceKind(column schema.Column) string {
	base := normalizedPostgresSQLiteColumnBase(column)
	switch base {
	case "integer":
		if column.DeclaredType == nil {
			return "integer"
		}
	case "bigint":
		if column.DeclaredType == nil {
			return "bigint"
		}
	case "boolean":
		if column.DeclaredType == nil {
			return "boolean"
		}
	case "numeric":
		if column.DeclaredType != nil &&
			len(column.DeclaredType.Arguments) == 2 {
			precision := column.DeclaredType.Arguments[0]
			scale := column.DeclaredType.Arguments[1]
			if precision >= 1 && precision <= 18 && scale == 0 {
				return fmt.Sprintf("numeric(%d,0)", precision)
			}
		}
	}
	return ""
}

func postgresSQLiteExactCheck(column schema.Column) bool {
	return postgresSQLiteExactReference(column)
}

func postgresSQLiteBoolean(column schema.Column) bool {
	return normalizedPostgresSQLiteColumnBase(column) == "boolean" &&
		column.DeclaredType == nil
}

func postgresSQLiteBigint(column schema.Column) bool {
	return normalizedPostgresSQLiteColumnBase(column) == "bigint" &&
		column.DeclaredType == nil
}

func normalizedPostgresSQLiteColumnBase(column schema.Column) string {
	if column.DeclaredType != nil {
		return normalizedPostgresSQLiteType(column.DeclaredType.Base)
	}
	return normalizedPostgresSQLiteType(column.Type)
}

func normalizedPostgresSQLiteType(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func sqlitePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func sqlitePostgresProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLite),
	}
}
