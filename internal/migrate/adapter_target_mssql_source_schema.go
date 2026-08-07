package migrate

import (
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// projectSQLServerTargetTable converts one already-discovered source table
// into the conservative shape accepted by the native SQL Server 2022 target.
// It never copies executable catalog text.
func projectSQLServerTargetTable(
	sourceEngine string,
	source schema.Table,
) (schema.Table, error) {
	var target schema.Table
	var err error
	switch sourceEngine {
	case "mssql":
		target = cloneSQLServerTargetTable(source)
	case "postgres":
		target, err = projectPostgresTableForSQLServer(source)
	case "mysql":
		target, err = projectMySQLTableForSQLServer(source)
	case "sqlite":
		target, err = projectSQLiteTableForSQLServer(source)
	default:
		return schema.Table{}, fmt.Errorf(
			"SQL Server target does not support source engine %q",
			sourceEngine,
		)
	}
	if err != nil {
		return schema.Table{}, err
	}
	if err := canonicalizeSQLServerTargetChecks(&target); err != nil {
		return schema.Table{}, err
	}
	if err := canonicalizeSQLServerTargetForeignKeys(&target); err != nil {
		return schema.Table{}, err
	}
	return target, nil
}

// canonicalizeSQLServerTargetChecks freezes a portable CHECK in exactly the
// AST form returned by SQL Server's catalog after DMTX renders it. SQL Server
// always brackets identifiers, so retaining a source spelling such as id > 0
// would otherwise make an immediately reread post-DDL catalog look like mixed
// drift despite identical CHECK semantics.
func canonicalizeSQLServerTargetChecks(table *schema.Table) error {
	if table == nil {
		return fmt.Errorf("SQL Server target CHECK canonicalization table is nil")
	}
	for index := range table.Checks {
		rendered, err := schema.RenderPortableCheckForSQLServer(
			table.Checks[index].Expression,
			table.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"canonicalize SQL Server target CHECK %s.%s: %w",
				table.Name,
				table.Checks[index].Name,
				err,
			)
		}
		expression, err := schema.ParseSQLServerCatalogCheck(
			rendered,
			table.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"parse planned SQL Server target CHECK %s.%s: %w",
				table.Name,
				table.Checks[index].Name,
				err,
			)
		}
		table.Checks[index].Expression = expression
	}
	return nil
}

// canonicalizeSQLServerTargetForeignKeys freezes the SQL Server catalog's
// only supported MATCH spelling. SQL Server implements SIMPLE semantics and
// reports that canonical form even when a portable source model uses NONE.
func canonicalizeSQLServerTargetForeignKeys(table *schema.Table) error {
	if table == nil {
		return fmt.Errorf("SQL Server target foreign-key canonicalization table is nil")
	}
	for index := range table.ForeignKeys {
		foreignKey := &table.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "SIMPLE"
		default:
			return fmt.Errorf(
				"canonicalize SQL Server target foreign key %s.%s: unsupported MATCH %q",
				table.Name,
				foreignKey.Name,
				foreignKey.Match,
			)
		}
	}
	return nil
}

func cloneSQLServerTargetTable(source schema.Table) schema.Table {
	cloned := source
	cloned.Identity = cloneSchemaIdentity(source.Identity)
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

func projectPostgresTableForSQLServer(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID ||
		source.MySQLCollation != "" {
		return schema.Table{}, sqlServerProjectionPolicy(
			"map PostgreSQL table metadata",
			source.Name,
		)
	}
	projected := cloneSQLServerTargetTable(source)
	for index, column := range source.Columns {
		target, err := projectPostgresColumnForSQLServer(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL column %s.%s to SQL Server: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}

	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		sourceColumns[column.Name] = column
		if column.PrimaryKeyPosition > 0 &&
			postgresTextColumnForSQLServer(column) {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map PostgreSQL text primary-key collation",
				source.Name+"."+column.Name,
			)
		}
		if column.PrimaryKeyPosition > 0 &&
			postgresUUIDColumnForSQLServer(column) {
			return schema.Table{}, sqlServerProjectionPolicy(
				"map PostgreSQL UUID primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
	}
	for _, index := range source.Indexes {
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL index column",
					source.Name+"."+indexed.Name,
				)
			}
			if postgresTextColumnForSQLServer(column) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL text index comparison",
					source.Name+"."+indexed.Name,
				)
			}
			if postgresUUIDColumnForSQLServer(column) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL UUID index comparison",
					source.Name+"."+indexed.Name,
				)
			}
			if index.Unique && column.Nullable {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL nullable unique index",
					source.Name+"."+index.Name,
				)
			}
		}
	}
	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "SIMPLE"
		default:
			return schema.Table{}, sqlServerProjectionPolicy(
				"map PostgreSQL foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, name := range foreignKey.Columns {
			if postgresTextColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL text foreign key collation",
					source.Name+"."+name,
				)
			}
			if postgresUUIDColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL UUID foreign-key comparison",
					source.Name+"."+name,
				)
			}
		}
	}
	for _, check := range source.Checks {
		referenced, err := schema.ReferencedCheckColumns(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return schema.Table{}, err
		}
		for _, name := range referenced {
			if postgresTextColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL text CHECK collation",
					source.Name+"."+name,
				)
			}
			if postgresUUIDColumnForSQLServer(sourceColumns[name]) {
				return schema.Table{}, sqlServerProjectionPolicy(
					"map PostgreSQL UUID CHECK comparison",
					source.Name+"."+name,
				)
			}
		}
	}
	return projected, nil
}

// projectPostgresColumnForSQLServer routes through the canonical type.
//
// The second route to leave the pairwise projection, and the first with a
// target other than PostgreSQL - which is what made it worth doing next, since
// it exercises the SQL Server side of the renderer rather than a second variant
// of the same one.
//
// Proved equivalent before the switch by
// TestCanonicalMatchesPairwiseForPostgresToSQLServer, and then failed the armed
// live gate anyway, on a bare timestamp the converter was recording without a
// precision. Both facts are worth keeping: the differential test is necessary
// and it is not sufficient, because its fixtures are still fixtures.
//
// The fact this route contributes to the canonical layer is the character-to-
// byte widening. PostgreSQL's varchar(n) is n characters; SQL Server's
// varchar(n) under the UTF-8 collation dmtx writes is n bytes, and a character
// can spend four. That multiplication lived here; it lives in
// canonicalToSQLServerText now, next to the halving that applies going the
// other way. One fact, two directions, and only the direction says which
// arithmetic is right.
func projectPostgresColumnForSQLServer(
	source schema.Column,
) (schema.Column, error) {
	// A refusal names its reason, not just the column. Flattening every
	// converter error into one message told an operator with a fixed-width char
	// column that something about its declaration was wrong and nothing about
	// what - and blank-padding is not a thing anyone guesses.
	if base := postgresSourceBase(source); base == "char" || base == "bpchar" {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL character type",
			"fixed-width blank-padding semantics cannot be preserved",
		)
	}
	canonical, err := schema.CanonicalFromPostgres(source, false)
	if err != nil {
		return schema.Column{}, sqlServerProjectionPolicy(
			"map PostgreSQL declared type",
			source.Name+": "+err.Error(),
		)
	}
	targetType, declared, err := schema.CanonicalToDeclared(
		canonical,
		schema.SQLServer,
	)
	if err != nil {
		return schema.Column{}, err
	}
	target := source
	target.Default = cloneSchemaExpression(source.Default)
	target.Type = targetType
	target.DeclaredType = declared

	// A clock default stays a projection concern rather than moving into the
	// canonical type, because it is a property of the DEFAULT and not of the
	// type. CURRENT_TIMESTAMP means "when the row was written", and a row
	// written by a migration was written now rather than when the source wrote
	// it - so carrying the expression across would silently restamp history.
	if target.Default != nil {
		switch strings.ToUpper(strings.TrimSpace(
			target.Default.CanonicalSQL(),
		)) {
		case "CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP":
			return schema.Column{}, sqlServerProjectionPolicy(
				"map PostgreSQL clock default",
				source.Name,
			)
		}
	}
	return target, nil
}

// postgresSourceBase is the declared base a PostgreSQL column carries, or its
// portable type when the catalog recorded no declaration - which is the
// ordinary case for text, uuid, bytea, json, bool and date.
func postgresSourceBase(column schema.Column) string {
	if column.DeclaredType != nil {
		return strings.ToLower(strings.TrimSpace(column.DeclaredType.Base))
	}
	return strings.ToLower(strings.TrimSpace(column.Type))
}

func postgresTextColumnForSQLServer(column schema.Column) bool {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "char", "varchar", "text", "json", "jsonb":
		return true
	default:
		return false
	}
}

func postgresUUIDColumnForSQLServer(column schema.Column) bool {
	return strings.EqualFold(strings.TrimSpace(column.Type), "uuid")
}

func sqlServerProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.SQLServer),
	}
}
