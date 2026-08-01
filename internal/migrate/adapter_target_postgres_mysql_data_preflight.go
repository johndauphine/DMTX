package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresMySQLSourceDataProbeKind uint8

const (
	postgresMySQLSourceDataProbeInvalid postgresMySQLSourceDataProbeKind = iota
	postgresMySQLSourceDataProbeNUL
	postgresMySQLSourceDataProbeCheck
	postgresMySQLSourceDataProbeForeignKey
	postgresMySQLSourceDataProbeUniqueIndex
)

type postgresMySQLSourceDataProbe struct {
	kind   postgresMySQLSourceDataProbeKind
	table  string
	object string
	query  string
}

type postgresMySQLSourceDataProbeRunner interface {
	hasInvalidRow(context.Context, string) (bool, error)
}

type postgresMySQLSourceDataDatabase struct {
	database *sql.DB
}

// PreflightSourceData rejects MySQL-family rows that PostgreSQL cannot admit,
// or that would make deferred target objects fail after target mutation. The
// shared MySQL source role covers both certified MySQL and MariaDB flavors.
func (*postgresTargetAdapter) PreflightSourceData(
	ctx context.Context,
	source sourceAdapter,
	plans []adapterTablePlan,
	_ string,
) error {
	if source == nil {
		return fmt.Errorf(
			"preflight source data for PostgreSQL: source adapter is required",
		)
	}
	if source.Engine() != "mysql" {
		return nil
	}
	provider, ok := source.(mysqlDatabaseHandleProvider)
	if !ok {
		return fmt.Errorf(
			"preflight MySQL source data for PostgreSQL: source database is not available",
		)
	}
	database := provider.mySQLDatabaseHandle()
	if database == nil {
		return fmt.Errorf(
			"preflight MySQL source data for PostgreSQL: source database is not available",
		)
	}
	probes, err := planPostgresMySQLSourceDataProbes(plans)
	if err != nil {
		return err
	}
	return runPostgresMySQLSourceDataProbes(
		ctx,
		postgresMySQLSourceDataDatabase{
			database: database,
		},
		probes,
	)
}

func planPostgresMySQLSourceDataProbes(
	plans []adapterTablePlan,
) ([]postgresMySQLSourceDataProbe, error) {
	probes := make([]postgresMySQLSourceDataProbe, 0)
	for _, plan := range plans {
		tableProbes, err := planPostgresMySQLTableDataProbes(plan)
		if err != nil {
			return nil, err
		}
		probes = append(probes, tableProbes...)
	}
	return probes, nil
}

func planPostgresMySQLTableDataProbes(
	plan adapterTablePlan,
) ([]postgresMySQLSourceDataProbe, error) {
	source := plan.source
	target := plan.target
	if source.Schema == "" || source.Name == "" {
		return nil, fmt.Errorf(
			"plan MySQL source data preflight for PostgreSQL: source table identity is incomplete",
		)
	}
	if target.Name != source.Name {
		return nil, fmt.Errorf(
			"plan MySQL source data preflight for PostgreSQL table %s: target table name is %q",
			source.Name,
			target.Name,
		)
	}
	sourceColumns, err := postgresMySQLColumnsByName(
		source.Name,
		"source",
		source.Columns,
	)
	if err != nil {
		return nil, err
	}
	targetColumns, err := postgresMySQLColumnsByName(
		source.Name,
		"target",
		target.Columns,
	)
	if err != nil {
		return nil, err
	}

	qualified := mySQLQualified(source.Schema, source.Name)
	probes := make([]postgresMySQLSourceDataProbe, 0)
	for _, column := range source.Columns {
		text, err := postgresMySQLTextColumn(column)
		if err != nil {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s column %s: %w",
				source.Name,
				column.Name,
				err,
			)
		}
		if !text {
			continue
		}
		targetColumn, exists := targetColumns[column.Name]
		if !exists {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: target text column %s is missing",
				source.Name,
				column.Name,
			)
		}
		if !postgresMySQLTargetTextColumn(targetColumn) {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: target column %s has type %q, want text or varchar",
				source.Name,
				column.Name,
				targetColumn.Type,
			)
		}
		identifier := mySQLIdentifier(column.Name)
		probes = append(probes, postgresMySQLSourceDataProbe{
			kind:   postgresMySQLSourceDataProbeNUL,
			table:  source.Name,
			object: column.Name,
			query: "SELECT EXISTS (SELECT 1 FROM " + qualified +
				" WHERE LOCATE(0x00, CAST(" + identifier +
				" AS BINARY)) > 0)",
		})
	}

	checkNames := make(map[string]struct{}, len(source.Checks))
	for _, check := range source.Checks {
		if check.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: CHECK name is empty",
				source.Name,
			)
		}
		if _, exists := checkNames[check.Name]; exists {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: duplicate CHECK %s",
				source.Name,
				check.Name,
			)
		}
		checkNames[check.Name] = struct{}{}
		rendered, err := schema.RenderPortableCheckForMySQL(
			check.Expression,
			source.Columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL CHECK %s.%s: %w",
				source.Name,
				check.Name,
				err,
			)
		}
		probes = append(probes, postgresMySQLSourceDataProbe{
			kind:   postgresMySQLSourceDataProbeCheck,
			table:  source.Name,
			object: check.Name,
			query: "SELECT EXISTS (SELECT 1 FROM " + qualified +
				" WHERE NOT (" + rendered + "))",
		})
	}

	foreignKeyNames := make(
		map[string]struct{},
		len(source.ForeignKeys),
	)
	for _, foreignKey := range source.ForeignKeys {
		if foreignKey.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: foreign key name is empty",
				source.Name,
			)
		}
		if _, exists := foreignKeyNames[foreignKey.Name]; exists {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: duplicate foreign key %s",
				source.Name,
				foreignKey.Name,
			)
		}
		foreignKeyNames[foreignKey.Name] = struct{}{}
		query, err := postgresMySQLForeignKeyProbeQuery(
			source,
			sourceColumns,
			foreignKey,
		)
		if err != nil {
			return nil, err
		}
		probes = append(probes, postgresMySQLSourceDataProbe{
			kind:   postgresMySQLSourceDataProbeForeignKey,
			table:  source.Name,
			object: foreignKey.Name,
			query:  query,
		})
	}

	indexNames := make(map[string]struct{}, len(source.Indexes))
	for _, index := range source.Indexes {
		if !index.Unique {
			continue
		}
		if index.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: unique index name is empty",
				source.Name,
			)
		}
		if _, exists := indexNames[index.Name]; exists {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: duplicate unique index %s",
				source.Name,
				index.Name,
			)
		}
		indexNames[index.Name] = struct{}{}
		query, err := postgresMySQLUniqueIndexProbeQuery(
			source,
			sourceColumns,
			index,
		)
		if err != nil {
			return nil, err
		}
		probes = append(probes, postgresMySQLSourceDataProbe{
			kind:   postgresMySQLSourceDataProbeUniqueIndex,
			table:  source.Name,
			object: index.Name,
			query:  query,
		})
	}
	return probes, nil
}

// postgresMySQLTargetTextColumn accepts the two PostgreSQL catalog shapes the
// MySQL projection can deliberately produce. Both reject NUL bytes and retain
// the same string values; CHAR is intentionally excluded because its padding
// semantics are not part of the certified mapping.
func postgresMySQLTargetTextColumn(column schema.Column) bool {
	switch strings.ToLower(strings.TrimSpace(column.Type)) {
	case "text", "varchar":
		return true
	default:
		return false
	}
}

func postgresMySQLColumnsByName(
	table string,
	role string,
	columns []schema.Column,
) (map[string]schema.Column, error) {
	byName := make(map[string]schema.Column, len(columns))
	for _, column := range columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: %s column name is empty",
				table,
				role,
			)
		}
		if _, exists := byName[column.Name]; exists {
			return nil, fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL table %s: duplicate %s column %s",
				table,
				role,
				column.Name,
			)
		}
		byName[column.Name] = column
	}
	return byName, nil
}

func postgresMySQLTextColumn(column schema.Column) (bool, error) {
	if column.DeclaredType == nil {
		return false, fmt.Errorf("declared type is missing")
	}
	switch strings.ToLower(strings.TrimSpace(column.DeclaredType.Base)) {
	case "char", "character", "varchar", "character varying",
		"tinytext", "text", "mediumtext", "longtext":
		return true, nil
	default:
		return false, nil
	}
}

func postgresMySQLForeignKeyProbeQuery(
	table schema.Table,
	columns map[string]schema.Column,
	foreignKey schema.ForeignKey,
) (string, error) {
	if foreignKey.ReferencedTable == "" ||
		len(foreignKey.Columns) == 0 ||
		len(foreignKey.Columns) != len(foreignKey.ReferencedColumns) {
		return "", fmt.Errorf(
			"plan MySQL source data preflight for PostgreSQL foreign key %s.%s: incomplete column metadata",
			table.Name,
			foreignKey.Name,
		)
	}
	const childAlias = "dmtx_child"
	const parentAlias = "dmtx_parent"
	joins := make([]string, len(foreignKey.Columns))
	nonnull := make([]string, len(foreignKey.Columns))
	seenLocal := make(map[string]struct{}, len(foreignKey.Columns))
	seenReferenced := make(
		map[string]struct{},
		len(foreignKey.ReferencedColumns),
	)
	for index, local := range foreignKey.Columns {
		referenced := foreignKey.ReferencedColumns[index]
		if _, exists := columns[local]; !exists || referenced == "" {
			return "", fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL foreign key %s.%s: invalid column pair at position %d",
				table.Name,
				foreignKey.Name,
				index+1,
			)
		}
		if _, exists := seenLocal[local]; exists {
			return "", fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL foreign key %s.%s: duplicate local column %s",
				table.Name,
				foreignKey.Name,
				local,
			)
		}
		if _, exists := seenReferenced[referenced]; exists {
			return "", fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL foreign key %s.%s: duplicate referenced column %s",
				table.Name,
				foreignKey.Name,
				referenced,
			)
		}
		seenLocal[local] = struct{}{}
		seenReferenced[referenced] = struct{}{}
		localName := postgresMySQLAliasedColumn(childAlias, local)
		referencedName := postgresMySQLAliasedColumn(
			parentAlias,
			referenced,
		)
		joins[index] = localName + " = " + referencedName
		nonnull[index] = localName + " IS NOT NULL"
	}
	return "SELECT EXISTS (SELECT 1 FROM " +
			mySQLQualified(table.Schema, table.Name) + " AS " +
			mySQLIdentifier(childAlias) + " LEFT JOIN " +
			mySQLQualified(table.Schema, foreignKey.ReferencedTable) + " AS " +
			mySQLIdentifier(parentAlias) + " ON " +
			strings.Join(joins, " AND ") + " WHERE " +
			strings.Join(nonnull, " AND ") + " AND " +
			postgresMySQLAliasedColumn(
				parentAlias,
				foreignKey.ReferencedColumns[0],
			) + " IS NULL)",
		nil
}

func postgresMySQLUniqueIndexProbeQuery(
	table schema.Table,
	columns map[string]schema.Column,
	index schema.Index,
) (string, error) {
	if len(index.Columns) == 0 {
		return "", fmt.Errorf(
			"plan MySQL source data preflight for PostgreSQL unique index %s.%s: no columns",
			table.Name,
			index.Name,
		)
	}
	quoted := make([]string, len(index.Columns))
	nonnull := make([]string, len(index.Columns))
	seen := make(map[string]struct{}, len(index.Columns))
	for position, indexedColumn := range index.Columns {
		if _, exists := columns[indexedColumn.Name]; !exists {
			return "", fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL unique index %s.%s: unknown column %s",
				table.Name,
				index.Name,
				indexedColumn.Name,
			)
		}
		if _, exists := seen[indexedColumn.Name]; exists {
			return "", fmt.Errorf(
				"plan MySQL source data preflight for PostgreSQL unique index %s.%s: duplicate column %s",
				table.Name,
				index.Name,
				indexedColumn.Name,
			)
		}
		seen[indexedColumn.Name] = struct{}{}
		quoted[position] = mySQLIdentifier(indexedColumn.Name)
		nonnull[position] = quoted[position] + " IS NOT NULL"
	}
	return "SELECT EXISTS (SELECT 1 FROM " +
			mySQLQualified(table.Schema, table.Name) + " WHERE " +
			strings.Join(nonnull, " AND ") + " GROUP BY " +
			strings.Join(quoted, ", ") + " HAVING COUNT(*) > 1)",
		nil
}

func postgresMySQLAliasedColumn(alias, column string) string {
	return mySQLIdentifier(alias) + "." + mySQLIdentifier(column)
}

func runPostgresMySQLSourceDataProbes(
	ctx context.Context,
	runner postgresMySQLSourceDataProbeRunner,
	probes []postgresMySQLSourceDataProbe,
) error {
	if runner == nil {
		return fmt.Errorf(
			"preflight MySQL source data for PostgreSQL: probe runner is required",
		)
	}
	for _, probe := range probes {
		description, err := postgresMySQLSourceDataProbeDescription(probe)
		if err != nil {
			return err
		}
		invalid, err := runner.hasInvalidRow(ctx, probe.query)
		if err != nil {
			return fmt.Errorf(
				"inspect MySQL table %s for PostgreSQL %s preflight: %w",
				probe.table,
				description,
				err,
			)
		}
		if !invalid {
			continue
		}
		switch probe.kind {
		case postgresMySQLSourceDataProbeNUL:
			return fmt.Errorf(
				"preflight MySQL table %s for PostgreSQL: text column %s contains an embedded NUL",
				probe.table,
				probe.object,
			)
		case postgresMySQLSourceDataProbeCheck:
			return fmt.Errorf(
				"preflight MySQL table %s for PostgreSQL: CHECK %s is violated by historical rows",
				probe.table,
				probe.object,
			)
		case postgresMySQLSourceDataProbeForeignKey:
			return fmt.Errorf(
				"preflight MySQL table %s for PostgreSQL: foreign key %s has orphan rows",
				probe.table,
				probe.object,
			)
		case postgresMySQLSourceDataProbeUniqueIndex:
			return fmt.Errorf(
				"preflight MySQL table %s for PostgreSQL: unique index %s has duplicate fully-nonnull keys",
				probe.table,
				probe.object,
			)
		default:
			return fmt.Errorf(
				"preflight MySQL table %s for PostgreSQL: unsupported probe kind %d",
				probe.table,
				probe.kind,
			)
		}
	}
	return nil
}

func postgresMySQLSourceDataProbeDescription(
	probe postgresMySQLSourceDataProbe,
) (string, error) {
	if probe.table == "" || probe.object == "" || probe.query == "" {
		return "", fmt.Errorf(
			"preflight MySQL source data for PostgreSQL: incomplete probe metadata",
		)
	}
	switch probe.kind {
	case postgresMySQLSourceDataProbeNUL:
		return "embedded-NUL column " + probe.object, nil
	case postgresMySQLSourceDataProbeCheck:
		return "CHECK " + probe.object, nil
	case postgresMySQLSourceDataProbeForeignKey:
		return "foreign key " + probe.object, nil
	case postgresMySQLSourceDataProbeUniqueIndex:
		return "unique index " + probe.object, nil
	default:
		return "", fmt.Errorf(
			"preflight MySQL table %s for PostgreSQL: unsupported probe kind %d",
			probe.table,
			probe.kind,
		)
	}
}

func (runner postgresMySQLSourceDataDatabase) hasInvalidRow(
	ctx context.Context,
	query string,
) (bool, error) {
	if runner.database == nil {
		return false, fmt.Errorf("source database is not configured")
	}
	var exists int
	if err := runner.database.QueryRowContext(ctx, query).Scan(&exists); err != nil {
		return false, err
	}
	switch exists {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf(
			"unexpected EXISTS result %d",
			exists,
		)
	}
}

var _ adapterTargetSourceDataPreflighter = (*postgresTargetAdapter)(nil)
