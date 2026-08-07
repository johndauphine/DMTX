package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

// projectMySQLTargetTable converts one already-discovered source table into the
// exact, conservative shape accepted by the selected native MySQL-family
// target. The result contains no executable catalog text: defaults and CHECKs
// remain structured schema expressions.
func projectMySQLTargetTable(
	sourceEngine string,
	sourceTable schema.Table,
	targetFlavor engine.MySQLServerFlavor,
) (schema.Table, error) {
	var target schema.Table
	var err error
	switch sourceEngine {
	case "mysql":
		target = cloneMySQLTargetTable(sourceTable)
	case "postgres":
		target, err = projectPostgresTableForMySQL(sourceTable)
	case "mssql":
		target, err = projectSQLServerTableForMySQL(sourceTable)
	case "sqlite":
		target, err = projectSQLiteTableForMySQL(sourceTable)
	default:
		return schema.Table{}, fmt.Errorf(
			"MySQL target does not support source engine %q",
			sourceEngine,
		)
	}
	if err != nil {
		return schema.Table{}, err
	}
	if err := validateMySQLSpatialTargetProjection(
		sourceEngine,
		target,
		targetFlavor,
	); err != nil {
		return schema.Table{}, err
	}
	if err := normalizeMySQLTargetCollation(
		sourceEngine,
		&target,
		targetFlavor,
	); err != nil {
		return schema.Table{}, err
	}
	normalizeMySQLTargetDeclaredTypeArguments(&target)
	if err := canonicalizeMySQLTargetChecks(&target); err != nil {
		return schema.Table{}, err
	}
	return target, nil
}

// normalizeMySQLTargetDeclaredTypeArguments removes a serialization-only
// distinction from a target-ready projection. Schema snapshots deliberately
// canonicalize an absent argument list as an empty JSON array, while native
// MySQL discovery returns nil for a declaration with no type modifiers. Both
// mean the same catalog declaration (for example BIGINT), so retaining the
// slice representation would make a schema evolution DDL it just applied fail
// the retained-table recheck.
func normalizeMySQLTargetDeclaredTypeArguments(table *schema.Table) {
	if table == nil {
		return
	}
	for index := range table.Columns {
		declared := table.Columns[index].DeclaredType
		if declared != nil && len(declared.Arguments) == 0 {
			declared.Arguments = nil
		}
	}
}

// canonicalizeMySQLTargetChecks freezes a CHECK in the same portable AST form
// returned by the exact MySQL/MariaDB catalog reader after DMTX renders and
// installs it. Without this conversion, a syntactically equivalent source
// expression such as `code <> ”` can differ from the catalog's quoted
// identifier form and make a post-DDL recovery prefix appear mixed.
func canonicalizeMySQLTargetChecks(table *schema.Table) error {
	if table == nil {
		return fmt.Errorf("MySQL target CHECK canonicalization table is nil")
	}
	for index := range table.Checks {
		rendered, err := schema.RenderPortableCheckForMySQL(
			table.Checks[index].Expression,
			table.Columns,
		)
		if err != nil {
			return fmt.Errorf(
				"canonicalize MySQL target CHECK %s.%s: %w",
				table.Name,
				table.Checks[index].Name,
				err,
			)
		}
		expression, err := schema.ParseMySQLCatalogCheck(rendered, table.Columns)
		if err != nil {
			return fmt.Errorf(
				"parse planned MySQL target CHECK %s.%s: %w",
				table.Name,
				table.Checks[index].Name,
				err,
			)
		}
		table.Checks[index].Expression = expression
	}
	return nil
}

func validateMySQLSpatialTargetProjection(
	sourceEngine string,
	table schema.Table,
	targetFlavor engine.MySQLServerFlavor,
) error {
	spatialColumns := make(map[string]struct{})
	for _, column := range table.Columns {
		if column.DeclaredType == nil ||
			column.DeclaredType.Spatial == nil {
			continue
		}
		if sourceEngine != "mysql" ||
			targetFlavor != engine.MySQLServerFlavorOracle80 {
			return mysqlProjectionPolicy(
				"map spatial metadata to MySQL-family target",
				table.Name+"."+column.Name,
			)
		}
		if err := schema.ValidateDeclaredType(
			*column.DeclaredType,
		); err != nil {
			return mysqlProjectionPolicy(
				"map MySQL spatial metadata",
				table.Name+"."+column.Name,
			)
		}
		subtype := string(column.DeclaredType.Spatial.Subtype)
		if column.Type != subtype ||
			column.DeclaredType.Base != mySQLSpatialCatalogBase(
				column.DeclaredType.Spatial.Subtype,
			) ||
			column.Default != nil ||
			column.PrimaryKey ||
			column.PrimaryKeyPosition != 0 {
			return mysqlProjectionPolicy(
				"map MySQL spatial column",
				table.Name+"."+column.Name,
			)
		}
		spatialColumns[column.Name] = struct{}{}
	}
	if len(spatialColumns) == 0 {
		return nil
	}
	for _, index := range table.Indexes {
		for _, column := range index.Columns {
			if _, spatial := spatialColumns[column.Name]; spatial {
				return mysqlProjectionPolicy(
					"map MySQL spatial index",
					table.Name+"."+index.Name+"."+column.Name,
				)
			}
		}
	}
	for _, foreignKey := range table.ForeignKeys {
		for _, column := range foreignKey.Columns {
			if _, spatial := spatialColumns[column]; spatial {
				return mysqlProjectionPolicy(
					"map MySQL spatial foreign key",
					table.Name+"."+foreignKey.Name+"."+column,
				)
			}
		}
	}
	return nil
}

func mySQLSpatialCatalogBase(
	subtype schema.SpatialSubtype,
) string {
	if subtype == schema.SpatialSubtypeGeometryCollection {
		return "geomcollection"
	}
	return string(subtype)
}

func normalizeMySQLTargetCollation(
	sourceEngine string,
	table *schema.Table,
	targetFlavor engine.MySQLServerFlavor,
) error {
	collation := strings.ToLower(strings.TrimSpace(table.MySQLCollation))
	switch targetFlavor {
	case engine.MySQLServerFlavorOracle80:
		if sourceEngine == "postgres" ||
			sourceEngine == "mssql" ||
			sourceEngine == "sqlite" {
			table.MySQLCollation = "utf8mb4_0900_bin"
			return nil
		}
		switch collation {
		case "utf8mb4_bin", "utf8mb4_0900_bin":
			table.MySQLCollation = collation
			return nil
		}
	case engine.MySQLServerFlavorMariaDB1011:
		if sourceEngine == "postgres" ||
			sourceEngine == "mssql" ||
			sourceEngine == "sqlite" {
			table.MySQLCollation = "utf8mb4_nopad_bin"
			return nil
		}
		if collation == "utf8mb4_nopad_bin" {
			table.MySQLCollation = collation
			return nil
		}
	default:
		return fmt.Errorf("unsupported MySQL target flavor")
	}
	return mysqlProjectionPolicy(
		"map MySQL-family table collation",
		table.Name+"."+table.MySQLCollation,
	)
}

func cloneMySQLTargetTable(source schema.Table) schema.Table {
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
			if source.Columns[index].DeclaredType.Spatial != nil {
				spatial := *source.Columns[index].DeclaredType.Spatial
				if spatial.SRID != nil {
					srid := *spatial.SRID
					spatial.SRID = &srid
				}
				declaration.Spatial = &spatial
			}
			if source.Columns[index].DeclaredType.MySQL != nil {
				mysql := *source.Columns[index].DeclaredType.MySQL
				mysql.EnumMembers = append(
					[]string(nil),
					mysql.EnumMembers...,
				)
				mysql.SetMembers = append(
					[]string(nil),
					mysql.SetMembers...,
				)
				if mysql.BitWidth != nil {
					width := *mysql.BitWidth
					mysql.BitWidth = &width
				}
				declaration.MySQL = &mysql
			}
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

// projectSQLServerTableForMySQL maps only the SQL Server 2022 source shape
// whose stored values and relational objects have an exact MySQL-family
// representation. Scalar text is safe because both admitted targets use a
// deterministic binary/no-pad collation, but text, binary, and UUID
// comparison roles remain fail-closed because their engine equality and
// padding contracts differ.
func projectSQLServerTableForMySQL(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID ||
		strings.TrimSpace(source.MySQLCollation) != "" {
		return schema.Table{}, mysqlProjectionPolicy(
			"map SQL Server table metadata",
			source.Name,
		)
	}
	projected := cloneMySQLTargetTable(source)
	for index, column := range source.Columns {
		target, err := projectSQLServerColumnForMySQL(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map SQL Server column %s.%s to MySQL: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}

	sourceColumns := make(
		map[string]schema.Column,
		len(source.Columns),
	)
	for _, column := range source.Columns {
		if column.Name == "" {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server column",
				source.Name,
			)
		}
		if _, exists := sourceColumns[column.Name]; exists {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server columns",
				source.Name+"."+column.Name,
			)
		}
		sourceColumns[column.Name] = column
		if (column.PrimaryKey ||
			column.PrimaryKeyPosition > 0) &&
			sqlServerMySQLNonportableComparison(column) {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server primary-key comparison",
				source.Name+"."+column.Name,
			)
		}
	}

	for _, index := range source.Indexes {
		if index.Inline {
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server index shape",
				source.Name+"."+index.Name,
			)
		}
		for _, indexed := range index.Columns {
			column, exists := sourceColumns[indexed.Name]
			if !exists ||
				strings.TrimSpace(indexed.Collation) != "" ||
				sqlServerMySQLNonportableComparison(column) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server index comparison",
					source.Name+"."+index.Name+"."+indexed.Name,
				)
			}
			if index.Unique && column.Nullable {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server nullable unique index",
					source.Name+"."+index.Name,
				)
			}
		}
	}

	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "NONE"
		default:
			return schema.Table{}, mysqlProjectionPolicy(
				"map SQL Server foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			if strings.EqualFold(
				strings.TrimSpace(action),
				"SET DEFAULT",
			) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server foreign-key action",
					source.Name+"."+foreignKey.Name,
				)
			}
		}
		for _, name := range foreignKey.Columns {
			column, exists := sourceColumns[name]
			if !exists ||
				sqlServerMySQLNonportableComparison(column) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server foreign-key comparison",
					source.Name+"."+foreignKey.Name+"."+name,
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
			return schema.Table{}, fmt.Errorf(
				"map SQL Server CHECK %s.%s to MySQL: %w",
				source.Name,
				check.Name,
				err,
			)
		}
		for _, name := range referenced {
			column, exists := sourceColumns[name]
			if !exists ||
				sqlServerMySQLNonportableComparison(column) ||
				sqlServerMySQLRealColumn(column) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map SQL Server CHECK comparison",
					source.Name+"."+check.Name+"."+name,
				)
			}
		}
	}

	for _, column := range projected.Columns {
		if !mySQLProjectedBoolean(column) {
			continue
		}
		expression, err := schema.ParseMySQLCatalogCheck(
			mySQLIdentifier(column.Name)+" IN (0, 1)",
			projected.Columns,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"plan MySQL boolean domain for %s.%s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Checks = append(
			projected.Checks,
			schema.CheckConstraint{
				Name: mySQLBooleanCheckName(
					source,
					column.Name,
				),
				Expression: expression,
			},
		)
	}
	return projected, nil
}

// projectSQLServerColumnForMySQL routes through the canonical type.
//
// The fifth route, and the first onto a MySQL target - so the vocabulary it
// projects into is new: canonicalToMySQLDeclared, in internal/schema.
//
// Every shape was compared against the pairwise projection before the swap and
// they agree on all but one, including the cases that have gone wrong before:
// an nvarchar's length passing through unmultiplied, a smalldatetime arriving as
// datetime(0), a REAL collapsing into DOUBLE, a uniqueidentifier as char(36).
//
// The one disagreement is a defect in the pairwise code. SQL Server's BINARY
// runs to 8000 and MySQL's stops at 255, and the projection carried the width
// under the same keyword, so a BINARY(300) was declared as MySQL BINARY(300) -
// which MySQL's own renderer refuses:
//
//	render MySQL declared type type "binary(300)" is unsupported for mysql
//
// The canonical target widens it to VARBINARY(300). The padding bytes are part
// of the stored value and travel either way; what is lost is the target
// re-applying the padding, which is a schema fact rather than a data one. The
// alternative was a table that could not be created.
func projectSQLServerColumnForMySQL(
	source schema.Column,
) (schema.Column, error) {
	if source.DeclaredType == nil {
		return schema.Column{}, mysqlProjectionPolicy(
			"map SQL Server declared type",
			source.Name,
		)
	}
	// A REAL default is refused while REAL itself is carried, because the
	// refusal is about the DEFAULT rather than the type. Every binary32 value
	// is exactly representable in binary64, so the column is safe; but the
	// default travels as a decimal token, and re-evaluating that token as a
	// double would not reproduce SQL Server's binary32 rounding.
	if sqlServerMySQLRealColumn(source) && source.Default != nil {
		return schema.Column{}, mysqlProjectionPolicy(
			"map SQL Server default",
			source.Name+": a REAL default re-evaluated as DOUBLE would not"+
				" reproduce the source's binary32 rounding",
		)
	}

	canonical, err := schema.CanonicalFromSQLServer(source, "", false)
	if err != nil {
		return schema.Column{}, mysqlProjectionPolicy(
			"map SQL Server declared type",
			source.Name+": "+err.Error(),
		)
	}
	targetType, declared, err := schema.CanonicalToDeclared(
		canonical,
		schema.MySQL,
	)
	if err != nil {
		return schema.Column{}, nameTheColumn(err, source.Name)
	}

	target := source
	target.Type = targetType
	target.DeclaredType = declared
	target.Default = cloneSchemaExpression(source.Default)
	if target.Default != nil {
		normalized, err := schema.NormalizeMySQLDefault(target)
		if err != nil {
			return schema.Column{}, fmt.Errorf(
				"normalize MySQL default for %s: %w",
				source.Name,
				err,
			)
		}
		target.Default = normalized
	}
	return target, nil
}

func sqlServerMySQLNonportableComparison(
	column schema.Column,
) bool {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if column.DeclaredType != nil {
		base = strings.ToLower(strings.Join(
			strings.Fields(column.DeclaredType.Base),
			" ",
		))
	}
	switch base {
	case "char", "varchar", "nchar", "nvarchar", "text",
		"binary", "varbinary", "blob",
		"uuid", "uniqueidentifier":
		// The national spellings belong here for the same reason as the narrow
		// ones: an nvarchar column's comparison is no more portable than a
		// varchar's, and leaving them out would have exempted them from a
		// check that exists because comparison semantics differ across engines.
		return true
	default:
		return false
	}
}

func sqlServerMySQLRealColumn(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	return strings.EqualFold(
		strings.TrimSpace(column.DeclaredType.Base),
		"real",
	)
}

func projectPostgresTableForMySQL(
	source schema.Table,
) (schema.Table, error) {
	if source.SQLiteStrict || source.SQLiteWithoutRowID {
		return schema.Table{}, mysqlProjectionPolicy(
			"map PostgreSQL table metadata",
			source.Name,
		)
	}
	projected := cloneMySQLTargetTable(source)
	projected.MySQLCollation = "utf8mb4_0900_bin"
	for index, column := range source.Columns {
		target, err := projectPostgresColumnForMySQL(column)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"map PostgreSQL column %s.%s to MySQL: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Columns[index] = target
	}
	for index := range projected.ForeignKeys {
		foreignKey := &projected.ForeignKeys[index]
		switch strings.ToUpper(strings.TrimSpace(foreignKey.Match)) {
		case "", "NONE", "SIMPLE":
			foreignKey.Match = "NONE"
		default:
			return schema.Table{}, mysqlProjectionPolicy(
				"map PostgreSQL foreign-key match",
				source.Name+"."+foreignKey.Name,
			)
		}
		for _, action := range []string{
			foreignKey.OnUpdate,
			foreignKey.OnDelete,
		} {
			if strings.EqualFold(strings.TrimSpace(action), "SET DEFAULT") {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL foreign-key action",
					source.Name+"."+foreignKey.Name,
				)
			}
		}
	}
	sourceColumns := make(map[string]schema.Column, len(source.Columns))
	for _, column := range source.Columns {
		sourceColumns[column.Name] = column
		if column.PrimaryKeyPosition > 0 &&
			postgresTextColumnForMySQL(column) {
			return schema.Table{}, mysqlProjectionPolicy(
				"map PostgreSQL text primary key collation",
				source.Name+"."+column.Name,
			)
		}
	}
	for _, index := range source.Indexes {
		for _, indexed := range index.Columns {
			if postgresTextColumnForMySQL(sourceColumns[indexed.Name]) &&
				!strings.EqualFold(indexed.Collation, "BINARY") {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL text index collation",
					source.Name+"."+indexed.Name,
				)
			}
		}
	}
	for _, foreignKey := range source.ForeignKeys {
		for _, name := range foreignKey.Columns {
			if postgresTextColumnForMySQL(sourceColumns[name]) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL text foreign key collation",
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
			if postgresTextColumnForMySQL(sourceColumns[name]) {
				return schema.Table{}, mysqlProjectionPolicy(
					"map PostgreSQL text CHECK collation",
					source.Name+"."+name,
				)
			}
		}
	}
	for _, column := range projected.Columns {
		if !mySQLProjectedBoolean(column) {
			continue
		}
		expression, err := schema.ParseMySQLCatalogCheck(
			mySQLIdentifier(column.Name)+" IN (0, 1)",
			projected.Columns,
		)
		if err != nil {
			return schema.Table{}, fmt.Errorf(
				"plan MySQL boolean domain for %s.%s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		projected.Checks = append(projected.Checks, schema.CheckConstraint{
			Name:       mySQLBooleanCheckName(source, column.Name),
			Expression: expression,
		})
	}
	return projected, nil
}

func postgresTextColumnForMySQL(column schema.Column) bool {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "char", "varchar", "text":
		return true
	default:
		return false
	}
}

func mySQLBooleanCheckName(table schema.Table, column string) string {
	digest := sha256.Sum256([]byte(
		table.Schema + "\x00" + table.Name + "\x00" + column,
	))
	return "dmtx_bool_" + hex.EncodeToString(digest[:8])
}

// projectPostgresColumnForMySQL routes through the canonical type.
//
// The sixth route, and the second onto the MySQL vocabulary written for the
// fifth - so this one adds no target code at all, which is the first time that
// has been true. That is the lattice paying for itself: N converters and N
// vocabularies, and a new pair costs neither.
//
// Every accepted shape matched the pairwise projection before the swap.
func projectPostgresColumnForMySQL(
	source schema.Column,
) (schema.Column, error) {
	if err := refusePostgresTypeThisRouteHasNotProved(source); err != nil {
		return schema.Column{}, err
	}

	canonical, err := schema.CanonicalFromPostgres(source, false)
	if err != nil {
		return schema.Column{}, mysqlProjectionPolicy(
			"map PostgreSQL declared type",
			source.Name+": "+err.Error(),
		)
	}
	targetType, declared, err := schema.CanonicalToDeclared(
		canonical,
		schema.MySQL,
	)
	if err != nil {
		return schema.Column{}, nameTheColumn(err, source.Name)
	}

	target := source
	target.Type = targetType
	target.DeclaredType = declared
	target.Default = cloneSchemaExpression(source.Default)

	// A boolean's DEFAULT has to be rewritten, not just its type. PostgreSQL
	// writes TRUE and FALSE where MySQL's tinyint(1) wants 1 and 0, and this is
	// a property of the default rather than of the type - so it stays with the
	// route.
	if canonical.Kind == schema.KindBoolean && target.Default != nil {
		value := strings.ToUpper(strings.TrimSpace(
			target.Default.CanonicalSQL(),
		))
		switch value {
		case "TRUE":
			value = "1"
		case "FALSE":
			value = "0"
		default:
			return schema.Column{}, mysqlProjectionPolicy(
				"map PostgreSQL boolean default",
				source.Name,
			)
		}
		expression, err := schema.ParseMySQLCatalogDefault(target, &value, false)
		if err != nil {
			return schema.Column{}, err
		}
		target.Default = expression
	}

	if target.Default != nil {
		normalized, err := schema.NormalizeMySQLDefault(target)
		if err != nil {
			return schema.Column{}, fmt.Errorf(
				"normalize MySQL default for %s: %w",
				source.Name,
				err,
			)
		}
		target.Default = normalized
	}
	return target, nil
}

// refusePostgresTypeThisRouteHasNotProved keeps three refusals the canonical
// layer would not make.
//
// All three are types the MySQL vocabulary CAN express and the mssql -> mysql
// route already carries - uuid as char(36), json as longtext, real as double.
// Accepting them here would make the two routes agree, which is the point of
// the lattice, and it is not what this change is for.
//
// The reason is the DATA path rather than the schema. SQL Server's source
// adapter converts a uniqueidentifier to its text form explicitly, in
// adapter_source_mssql_values.go; PostgreSQL's has no such layer, so what a
// uuid or a json document becomes on the way to MySQL is whatever the driver
// hands over, and nothing has measured it. The SO2010 corpus has no column of
// any of these three types, so the armed gate cannot answer it either.
//
// A schema that accepts what the data path cannot carry is worse than a
// refusal: it produces a target that loads and is wrong. So these stay refused
// until there is a live test, which is task #45.
func refusePostgresTypeThisRouteHasNotProved(source schema.Column) error {
	base := strings.ToLower(strings.TrimSpace(source.Type))
	if source.DeclaredType != nil {
		base = strings.ToLower(strings.TrimSpace(source.DeclaredType.Base))
	}
	// The column's name leads every one of these. A refusal that says only
	// which TYPE was refused leaves an operator grepping a schema for it, and
	// the rest of this file has named the column since before the swap.
	switch base {
	case "uuid":
		return mysqlProjectionPolicy(
			"map PostgreSQL type",
			source.Name+" (uuid): the SQL Server route carries this as"+
				" char(36), and the PostgreSQL value path for it has not been"+
				" measured",
		)
	case "json":
		return mysqlProjectionPolicy(
			"map PostgreSQL type",
			source.Name+" (json): jsonb is carried as longtext and this would"+
				" be too, but the value path for it has not been measured",
		)
	case "real", "float4":
		return mysqlProjectionPolicy(
			"map PostgreSQL type",
			source.Name+" (real): the SQL Server route carries this as double,"+
				" and the PostgreSQL value path for it has not been measured",
		)
	}
	return nil
}

func mySQLProjectedBoolean(column schema.Column) bool {
	if column.DeclaredType == nil {
		return false
	}
	return strings.EqualFold(column.DeclaredType.Base, "tinyint") &&
		len(column.DeclaredType.Arguments) == 1 &&
		column.DeclaredType.Arguments[0] == 1
}

func mysqlProjectionPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: operation,
		Type:      value,
		Target:    string(schema.MySQL),
	}
}
