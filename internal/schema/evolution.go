package schema

import (
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"
)

// ColumnEvolutionKind identifies one non-destructive, in-place column change
// admitted by the Stage 4 schema-contract planner. The zero value is invalid.
type ColumnEvolutionKind uint8

const (
	AddNullableColumnEvolution ColumnEvolutionKind = iota + 1
	RelaxNullabilityEvolution
	WidenTypeEvolution
)

// ColumnEvolution is an opaque, immutable proof that one structural column
// change satisfies DMTX's bounded in-place evolution preconditions. Callers
// must use one of the Plan functions below; RenderColumnEvolution never accepts
// catalog SQL or a caller-assembled type declaration.
//
// The embedded table and columns are deep copies. A caller therefore cannot
// change the operation after its preconditions have been checked.
type ColumnEvolution struct {
	kind          ColumnEvolutionKind
	previousTable *SnapshotTable
	table         Table
	previous      *Column
	current       Column
}

// Kind reports the already-proved operation kind without exposing mutable
// operation evidence.
func (operation ColumnEvolution) Kind() ColumnEvolutionKind {
	return operation.kind
}

// CompleteEvolutionCatalog is an opaque assertion boundary for exhaustive
// target-catalog discovery. An adapter may construct this value only after it
// has discovered every table, index, foreign key, and CHECK in the target
// namespace. The constructor validates deterministic structural uniqueness
// and deep-copies all evidence; it cannot itself perform database I/O or prove
// that discovery was exhaustive.
//
// A complete catalog is required for type widening because incoming foreign
// keys live on other tables. Treating only the changed table as dependency
// evidence could authorize a destructive standalone ALTER.
type CompleteEvolutionCatalog struct {
	tables []Table
}

// NewCompleteEvolutionCatalog validates and owns an exhaustive target catalog
// supplied by an engine adapter. The adapter remains responsible for proving
// that the input covers the entire target namespace. The constructor proves
// that every dependency member can be resolved without relying on a target's
// identifier-folding rules; incomplete relation metadata is not a safe
// assertion that a widening has no dependent objects.
func NewCompleteEvolutionCatalog(
	tables []Table,
) (CompleteEvolutionCatalog, error) {
	const operation = "prove complete evolution catalog"
	if len(tables) == 0 {
		return CompleteEvolutionCatalog{}, evolutionPolicy(
			operation,
			"catalog is empty",
			"schema contract",
		)
	}
	for index, table := range tables {
		if !validEvolutionCatalogIdentifier(table.Name, false) {
			return CompleteEvolutionCatalog{}, evolutionPolicy(
				operation,
				"table name is empty or invalid",
				"schema contract",
			)
		}
		if !validEvolutionCatalogIdentifier(table.Schema, true) {
			return CompleteEvolutionCatalog{}, evolutionPolicy(
				operation,
				"schema name is invalid for table "+table.Name,
				"schema contract",
			)
		}
		for previous := 0; previous < index; previous++ {
			if evolutionIdentifiersMayAlias(
				tables[previous].Schema,
				table.Schema,
			) && evolutionIdentifiersMayAlias(
				tables[previous].Name,
				table.Name,
			) {
				return CompleteEvolutionCatalog{}, evolutionPolicy(
					operation,
					"catalog contains case-aliased table "+
						table.Schema+"."+table.Name,
					"schema contract",
				)
			}
		}
		if err := validateEvolutionCatalogTable(table); err != nil {
			return CompleteEvolutionCatalog{}, err
		}
	}
	for _, table := range tables {
		if err := validateEvolutionCatalogRelations(tables, table); err != nil {
			return CompleteEvolutionCatalog{}, err
		}
	}
	result := CompleteEvolutionCatalog{
		tables: make([]Table, len(tables)),
	}
	for index, table := range tables {
		result.tables[index] = cloneEvolutionTable(table)
	}
	return result, nil
}

func validateEvolutionCatalogTable(table Table) error {
	const operation = "prove complete evolution catalog"
	if len(table.Columns) == 0 {
		return evolutionPolicy(
			operation,
			"table "+table.Name+" has no columns",
			"schema contract",
		)
	}
	for index, column := range table.Columns {
		if !validEvolutionCatalogIdentifier(column.Name, false) {
			return evolutionPolicy(
				operation,
				"column name is empty or invalid in table "+table.Name,
				"schema contract",
			)
		}
		if strings.TrimSpace(column.Type) == "" {
			return evolutionPolicy(
				operation,
				"column type is empty for "+table.Name+"."+column.Name,
				"schema contract",
			)
		}
		if column.DeclaredType != nil {
			if err := ValidateDeclaredType(*column.DeclaredType); err != nil {
				return evolutionPolicy(
					operation,
					"declared type is invalid for "+
						table.Name+"."+column.Name+": "+err.Error(),
					"schema contract",
				)
			}
		}
		if column.Default != nil &&
			!validEvolutionCatalogDefault(*column.Default) {
			return evolutionPolicy(
				operation,
				"default evidence is invalid for "+
					table.Name+"."+column.Name,
				"schema contract",
			)
		}
		for previous := 0; previous < index; previous++ {
			if evolutionIdentifiersMayAlias(
				table.Columns[previous].Name,
				column.Name,
			) {
				return evolutionPolicy(
					operation,
					"table "+table.Name+
						" contains case-aliased column "+column.Name,
					"schema contract",
				)
			}
		}
	}
	if err := validateEvolutionCatalogPrimaryKey(table); err != nil {
		return err
	}
	if err := validateEvolutionCatalogIdentity(table); err != nil {
		return err
	}
	if len(table.ClickHouseOrderBy) > 0 {
		if err := validateEvolutionCatalogColumnMembers(
			table,
			table.ClickHouseOrderBy,
			"ClickHouse ordering",
		); err != nil {
			return err
		}
	}
	return nil
}

func validateEvolutionCatalogPrimaryKey(table Table) error {
	const operation = "prove complete evolution catalog"
	positions := make(map[int]struct{})
	count := 0
	for _, column := range table.Columns {
		member := column.PrimaryKeyPosition > 0
		if column.PrimaryKey != member {
			return evolutionPolicy(
				operation,
				"primary-key membership and position disagree for "+
					table.Name+"."+column.Name,
				"schema contract",
			)
		}
		if !member {
			if column.PrimaryKeyPosition < 0 {
				return evolutionPolicy(
					operation,
					"primary-key position is invalid for "+
						table.Name+"."+column.Name,
					"schema contract",
				)
			}
			continue
		}
		if column.Nullable {
			return evolutionPolicy(
				operation,
				"primary-key column is nullable: "+
					table.Name+"."+column.Name,
				"schema contract",
			)
		}
		if _, duplicate := positions[column.PrimaryKeyPosition]; duplicate {
			return evolutionPolicy(
				operation,
				"table "+table.Name+
					" has duplicate primary-key positions",
				"schema contract",
			)
		}
		positions[column.PrimaryKeyPosition] = struct{}{}
		count++
	}
	for position := 1; position <= count; position++ {
		if _, exists := positions[position]; !exists {
			return evolutionPolicy(
				operation,
				"table "+table.Name+
					" primary-key positions are not contiguous",
				"schema contract",
			)
		}
	}
	return nil
}

func validateEvolutionCatalogIdentity(table Table) error {
	const operation = "prove complete evolution catalog"
	if table.Identity == nil {
		return nil
	}
	identity := table.Identity
	if identity.Generation != IdentityByDefault ||
		!validEvolutionCatalogIdentifier(identity.Column, false) ||
		identity.Frontier != nil && *identity.Frontier < 0 {
		return evolutionPolicy(
			operation,
			"table "+table.Name+" has invalid identity metadata",
			"schema contract",
		)
	}
	column, exists := evolutionCatalogColumn(table, identity.Column)
	if !exists ||
		column.Nullable ||
		column.Default != nil ||
		!column.PrimaryKey ||
		column.PrimaryKeyPosition != 1 ||
		len(orderedPrimaryKeyColumns(table)) != 1 ||
		canonicalEvolutionGenericType(column.Type) != "bigint" ||
		!evolutionTypeEvidenceConsistent(column) {
		return evolutionPolicy(
			operation,
			"table "+table.Name+
				" identity is not its sole non-null primary key",
			"schema contract",
		)
	}
	return nil
}

func validateEvolutionCatalogRelations(
	tables []Table,
	table Table,
) error {
	const operation = "prove complete evolution catalog"
	indexNames := make([]string, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		if index.Inline && !index.Unique {
			return evolutionPolicy(
				operation,
				"table "+table.Name+" has a non-unique inline index",
				"schema contract",
			)
		}
		if err := validateEvolutionCatalogOptionalObjectName(
			index.Name,
			indexNames,
			"index",
			table.Name,
		); err != nil {
			return err
		}
		if index.Name != "" {
			indexNames = append(indexNames, index.Name)
		}
		members := make([]string, len(index.Columns))
		for position, column := range index.Columns {
			members[position] = column.Name
			if column.Collation != "" &&
				(!utf8.ValidString(column.Collation) ||
					strings.ContainsRune(column.Collation, '\x00')) {
				return evolutionPolicy(
					operation,
					"table "+table.Name+
						" has invalid index collation evidence",
					"schema contract",
				)
			}
		}
		if err := validateEvolutionCatalogColumnMembers(
			table,
			members,
			"index",
		); err != nil {
			return err
		}
	}

	checkNames := make([]string, 0, len(table.Checks))
	for _, check := range table.Checks {
		if err := validateEvolutionCatalogOptionalObjectName(
			check.Name,
			checkNames,
			"CHECK",
			table.Name,
		); err != nil {
			return err
		}
		if check.Name != "" {
			checkNames = append(checkNames, check.Name)
		}
		if check.Expression.kind != expressionCheck ||
			strings.TrimSpace(check.Expression.sql) == "" {
			return evolutionPolicy(
				operation,
				"table "+table.Name+" has invalid CHECK evidence",
				"schema contract",
			)
		}
	}

	foreignKeyNames := make([]string, 0, len(table.ForeignKeys))
	for _, foreignKey := range table.ForeignKeys {
		if err := validateEvolutionCatalogOptionalObjectName(
			foreignKey.Name,
			foreignKeyNames,
			"foreign key",
			table.Name,
		); err != nil {
			return err
		}
		if foreignKey.Name != "" {
			foreignKeyNames = append(
				foreignKeyNames,
				foreignKey.Name,
			)
		}
		if err := validateEvolutionCatalogForeignKey(
			tables,
			table,
			foreignKey,
		); err != nil {
			return err
		}
	}
	return nil
}

func validateEvolutionCatalogForeignKey(
	tables []Table,
	owner Table,
	foreignKey ForeignKey,
) error {
	const operation = "prove complete evolution catalog"
	if !validEvolutionCatalogIdentifier(
		foreignKey.ReferencedTable,
		false,
	) || !validEvolutionCatalogIdentifier(
		foreignKey.ReferencedSchema,
		true,
	) {
		return evolutionPolicy(
			operation,
			"table "+owner.Name+" has an incomplete foreign key",
			"schema contract",
		)
	}
	if err := validateEvolutionCatalogColumnMembers(
		owner,
		foreignKey.Columns,
		"foreign-key owner",
	); err != nil {
		return err
	}
	if len(foreignKey.ReferencedColumns) != len(foreignKey.Columns) {
		return evolutionPolicy(
			operation,
			"table "+owner.Name+
				" has mismatched foreign-key member counts",
			"schema contract",
		)
	}
	referenced, err := evolutionCatalogReferencedTable(
		tables,
		owner,
		foreignKey,
	)
	if err != nil {
		return err
	}
	if err := validateEvolutionCatalogColumnMembers(
		referenced,
		foreignKey.ReferencedColumns,
		"foreign-key referenced",
	); err != nil {
		return err
	}
	if !evolutionCatalogUniqueKey(
		referenced,
		foreignKey.ReferencedColumns,
	) {
		return evolutionPolicy(
			operation,
			"foreign key from "+owner.Name+
				" does not reference a proven unique key",
			"schema contract",
		)
	}
	if !validEvolutionCatalogForeignKeyAction(foreignKey.OnUpdate) ||
		!validEvolutionCatalogForeignKeyAction(foreignKey.OnDelete) ||
		!validEvolutionCatalogForeignKeyMatch(foreignKey.Match) {
		return evolutionPolicy(
			operation,
			"table "+owner.Name+" has invalid foreign-key actions",
			"schema contract",
		)
	}
	return nil
}

func evolutionCatalogReferencedTable(
	tables []Table,
	owner Table,
	foreignKey ForeignKey,
) (Table, error) {
	const operation = "prove complete evolution catalog"
	referencedSchema := foreignKey.ReferencedSchema
	if referencedSchema == "" {
		referencedSchema = owner.Schema
	}
	matches := make([]Table, 0, 1)
	for _, candidate := range tables {
		if evolutionIdentifiersMayAlias(
			candidate.Schema,
			referencedSchema,
		) && evolutionIdentifiersMayAlias(
			candidate.Name,
			foreignKey.ReferencedTable,
		) {
			matches = append(matches, candidate)
		}
	}
	if len(matches) != 1 {
		return Table{}, evolutionPolicy(
			operation,
			"foreign key from "+owner.Name+
				" has a missing or ambiguous referenced table "+
				referencedSchema+"."+
				foreignKey.ReferencedTable,
			"schema contract",
		)
	}
	referenced := matches[0]
	if referenced.Name != foreignKey.ReferencedTable ||
		referenced.Schema != referencedSchema {
		return Table{}, evolutionPolicy(
			operation,
			"foreign key from "+owner.Name+
				" has an unproved qualified table reference "+
				referencedSchema+"."+
				foreignKey.ReferencedTable,
			"schema contract",
		)
	}
	return referenced, nil
}

func validateEvolutionCatalogColumnMembers(
	table Table,
	members []string,
	kind string,
) error {
	const operation = "prove complete evolution catalog"
	if len(members) == 0 {
		return evolutionPolicy(
			operation,
			"table "+table.Name+" has an empty "+kind+" member list",
			"schema contract",
		)
	}
	seen := make([]string, 0, len(members))
	for _, member := range members {
		if !validEvolutionCatalogIdentifier(member, false) {
			return evolutionPolicy(
				operation,
				"table "+table.Name+" has an invalid "+kind+" member",
				"schema contract",
			)
		}
		for _, previous := range seen {
			if evolutionIdentifiersMayAlias(previous, member) {
				return evolutionPolicy(
					operation,
					"table "+table.Name+" has duplicate "+kind+
						" member "+member,
					"schema contract",
				)
			}
		}
		seen = append(seen, member)
		if _, exists := evolutionCatalogColumn(table, member); !exists {
			return evolutionPolicy(
				operation,
				"table "+table.Name+" has unknown or case-aliased "+
					kind+" member "+member,
				"schema contract",
			)
		}
	}
	return nil
}

func evolutionCatalogColumn(table Table, name string) (Column, bool) {
	for _, column := range table.Columns {
		if column.Name == name {
			return column, true
		}
	}
	return Column{}, false
}

func evolutionCatalogUniqueKey(table Table, members []string) bool {
	primaryKey := orderedPrimaryKeyColumns(table)
	if len(primaryKey) == len(members) {
		matches := true
		for index, member := range members {
			if primaryKey[index].Name != member {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	for _, index := range table.Indexes {
		if !index.Unique || len(index.Columns) != len(members) {
			continue
		}
		matches := true
		for position, member := range members {
			if index.Columns[position].Name != member {
				matches = false
				break
			}
		}
		if matches {
			return true
		}
	}
	return false
}

func validateEvolutionCatalogOptionalObjectName(
	name string,
	existing []string,
	kind string,
	table string,
) error {
	const operation = "prove complete evolution catalog"
	if name == "" {
		return nil
	}
	if !validEvolutionCatalogIdentifier(name, false) {
		return evolutionPolicy(
			operation,
			"table "+table+" has an invalid "+kind+" name",
			"schema contract",
		)
	}
	for _, previous := range existing {
		if evolutionIdentifiersMayAlias(previous, name) {
			return evolutionPolicy(
				operation,
				"table "+table+" has case-aliased "+kind+
					" names",
				"schema contract",
			)
		}
	}
	return nil
}

func validEvolutionCatalogIdentifier(
	value string,
	allowEmpty bool,
) bool {
	if value == "" {
		return allowEmpty
	}
	return utf8.ValidString(value) &&
		!strings.ContainsRune(value, '\x00')
}

func validEvolutionCatalogDefault(expression Expression) bool {
	if strings.TrimSpace(expression.sql) == "" {
		return false
	}
	switch expression.kind {
	case expressionNull,
		expressionBoolean,
		expressionNumber,
		expressionString,
		expressionBlob,
		expressionCurrentTime,
		expressionCurrentDate,
		expressionCurrentTimestamp:
		return true
	default:
		return false
	}
}

func validEvolutionCatalogForeignKeyAction(value string) bool {
	switch strings.ToUpper(strings.Join(strings.Fields(value), " ")) {
	case "", "NO ACTION", "RESTRICT", "CASCADE", "SET NULL", "SET DEFAULT":
		return true
	default:
		return false
	}
}

func validEvolutionCatalogForeignKeyMatch(value string) bool {
	switch strings.ToUpper(strings.Join(strings.Fields(value), " ")) {
	case "", "NONE", "SIMPLE", "FULL", "PARTIAL":
		return true
	default:
		return false
	}
}

// PlanAddNullableColumn proves the narrow Stage 4 add-column boundary: the
// column is absent from the prior shape, present exactly once in the
// target-ready current shape, is nullable, and is neither a primary-key nor
// identity column. Only an absent default or a structurally parsed scalar
// literal is admitted.
func PlanAddNullableColumn(
	previousTable SnapshotTable,
	table Table,
	column Column,
) (ColumnEvolution, error) {
	const operation = "plan nullable column addition"
	if err := validateEvolutionTableColumn(table, column, operation); err != nil {
		return ColumnEvolution{}, err
	}
	normalizedPrevious, err := normalizeSnapshotTable(previousTable)
	if err != nil {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"prior table snapshot is invalid: "+err.Error(),
			"schema contract",
		)
	}
	if normalizedPrevious.Schema != table.Schema ||
		normalizedPrevious.Name != table.Name {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"prior and current table identities differ",
			"schema contract",
		)
	}
	for _, previousColumn := range normalizedPrevious.Columns {
		if previousColumn.Name == column.Name {
			return ColumnEvolution{}, evolutionPolicy(
				operation,
				"column already exists in the prior table shape",
				"schema contract",
			)
		}
	}
	if !column.Nullable {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"new column must be nullable",
			"schema contract",
		)
	}
	if column.PrimaryKey || column.PrimaryKeyPosition != 0 {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"new column cannot participate in the primary key",
			"schema contract",
		)
	}
	identity, err := evolutionIdentityColumn(table, operation)
	if err != nil {
		return ColumnEvolution{}, err
	}
	if identity == column.Name {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"new column cannot be an identity",
			"schema contract",
		)
	}
	if column.Default != nil && !evolutionLiteralDefault(column.Default) {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"new column default is not a deterministic scalar literal",
			"schema contract",
		)
	}
	result := newColumnEvolution(
		AddNullableColumnEvolution,
		table,
		nil,
		column,
	)
	clonedPrevious := normalizedPrevious
	result.previousTable = &clonedPrevious
	return result, nil
}

// PlanRelaxNullability proves that nullable is the only changed column
// property. Primary-key and identity columns are rejected because making
// either nullable would invalidate their target contract.
func PlanRelaxNullability(
	table Table,
	previous Column,
	current Column,
) (ColumnEvolution, error) {
	const operation = "plan nullability relaxation"
	if err := validateEvolutionTableColumn(table, current, operation); err != nil {
		return ColumnEvolution{}, err
	}
	if previous.Name == "" || previous.Name != current.Name {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"previous and current column identities differ",
			"schema contract",
		)
	}
	if previous.Nullable || !current.Nullable {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"change is not NOT NULL to nullable",
			"schema contract",
		)
	}
	if current.PrimaryKey || current.PrimaryKeyPosition != 0 ||
		previous.PrimaryKey || previous.PrimaryKeyPosition != 0 {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"primary-key nullability cannot be relaxed",
			"schema contract",
		)
	}
	identity, err := evolutionIdentityColumn(table, operation)
	if err != nil {
		return ColumnEvolution{}, err
	}
	if identity == current.Name {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"identity nullability cannot be relaxed",
			"schema contract",
		)
	}
	normalizedPrevious := previous
	normalizedPrevious.Nullable = current.Nullable
	if !reflect.DeepEqual(normalizedPrevious, current) {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"nullability change is coupled to other column drift",
			"schema contract",
		)
	}
	return newColumnEvolution(
		RelaxNullabilityEvolution,
		table,
		&previous,
		current,
	), nil
}

// PlanSafeTypeWidening independently proves the target-ready form of the
// planner's lossless widening boundary. It does not infer an evolution policy:
// the caller must invoke it only for a widen_type contract decision. This
// defense-in-depth check is intentionally no broader than the planner's
// admitted integer, numeric, variable-width, temporal, text/blob, and
// real-to-double relations.
//
// Existing primary-key and identity columns are rejected. Their dependent
// objects require a wider, engine-specific lifecycle than one ALTER primitive
// can preserve uniformly across all supported relational targets.
func PlanSafeTypeWidening(
	catalog CompleteEvolutionCatalog,
	table Table,
	previous Column,
	current Column,
) (ColumnEvolution, error) {
	const operation = "plan safe type widening"
	if err := validateEvolutionTableColumn(table, current, operation); err != nil {
		return ColumnEvolution{}, err
	}
	if previous.Name == "" || previous.Name != current.Name {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"previous and current column identities differ",
			"schema contract",
		)
	}
	if previous.PrimaryKey || previous.PrimaryKeyPosition != 0 ||
		current.PrimaryKey || current.PrimaryKeyPosition != 0 {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"primary-key type evolution requires a dependent-object lifecycle",
			"schema contract",
		)
	}
	identity, err := evolutionIdentityColumn(table, operation)
	if err != nil {
		return ColumnEvolution{}, err
	}
	if identity == current.Name {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"identity type evolution requires an identity lifecycle",
			"schema contract",
		)
	}
	if err := validateEvolutionWideningDependencies(
		catalog,
		table,
		current.Name,
	); err != nil {
		return ColumnEvolution{}, err
	}

	normalizedPrevious := previous
	normalizedPrevious.Type = current.Type
	normalizedPrevious.DeclaredType = cloneEvolutionDeclaredType(
		current.DeclaredType,
	)
	if !reflect.DeepEqual(normalizedPrevious, current) {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"type change is coupled to other column drift",
			"schema contract",
		)
	}
	if !safeEvolutionTypeWidening(previous, current) {
		return ColumnEvolution{}, evolutionPolicy(
			operation,
			"type change is narrowing, lossy, ambiguous, or unchanged",
			"schema contract",
		)
	}
	return newColumnEvolution(
		WidenTypeEvolution,
		table,
		&previous,
		current,
	), nil
}

// RenderColumnEvolution renders one already-proved operation for a
// target-ready table. SQLite has no admitted deterministic in-place evolution
// subset. ClickHouse evolution is deliberately rebuild-only.
func RenderColumnEvolution(
	target Dialect,
	operation ColumnEvolution,
) (string, error) {
	if err := validateEvolutionTarget(target); err != nil {
		return "", err
	}
	if operation.kind < AddNullableColumnEvolution ||
		operation.kind > WidenTypeEvolution ||
		operation.table.Name == "" ||
		operation.current.Name == "" {
		return "", evolutionPolicy(
			"render column evolution",
			"invalid or unproved operation",
			string(target),
		)
	}
	if err := validateEvolutionTableColumn(
		operation.table,
		operation.current,
		"render column evolution",
	); err != nil {
		return "", err
	}
	if err := validateEvolutionTargetTable(target, operation.table); err != nil {
		return "", fmt.Errorf("validate evolved target table: %w", err)
	}

	switch operation.kind {
	case AddNullableColumnEvolution:
		if operation.previousTable == nil ||
			operation.previousTable.Schema != operation.table.Schema ||
			operation.previousTable.Name != operation.table.Name {
			return "", evolutionPolicy(
				"render nullable column addition",
				"missing or mismatched prior-table absence proof",
				string(target),
			)
		}
		for _, previousColumn := range operation.previousTable.Columns {
			if previousColumn.Name == operation.current.Name ||
				target != Postgres &&
					strings.EqualFold(
						previousColumn.Name,
						operation.current.Name,
					) {
				return "", evolutionPolicy(
					"render nullable column addition",
					"column exists in prior-table evidence",
					string(target),
				)
			}
		}
		definition, err := renderEvolutionColumnDefinition(
			target,
			operation.current,
			true,
		)
		if err != nil {
			return "", err
		}
		add := " ADD COLUMN "
		if target == SQLServer {
			add = " ADD "
		}
		return "ALTER TABLE " + qualified(
			target,
			operation.table.Schema,
			operation.table.Name,
		) + add + definition + ";", nil

	case RelaxNullabilityEvolution:
		if operation.previous == nil {
			return "", evolutionPolicy(
				"render nullability relaxation",
				"missing previous column proof",
				string(target),
			)
		}
		switch target {
		case Postgres:
			if err := validateEvolutionDefault(target, operation.current); err != nil {
				return "", err
			}
			return "ALTER TABLE " + qualified(
				target,
				operation.table.Schema,
				operation.table.Name,
			) + " ALTER COLUMN " + quote(
				target,
				operation.current.Name,
			) + " DROP NOT NULL;", nil
		case SQLServer:
			columnType, err := renderEvolutionColumnType(
				target,
				operation.current,
			)
			if err != nil {
				return "", err
			}
			if err := validateEvolutionDefault(target, operation.current); err != nil {
				return "", err
			}
			return "ALTER TABLE " + qualified(
				target,
				operation.table.Schema,
				operation.table.Name,
			) + " ALTER COLUMN " + quote(
				target,
				operation.current.Name,
			) + " " + columnType + " NULL;", nil
		case MySQL:
			definition, err := renderEvolutionColumnDefinition(
				target,
				operation.current,
				true,
			)
			if err != nil {
				return "", err
			}
			return "ALTER TABLE " + qualified(
				target,
				operation.table.Schema,
				operation.table.Name,
			) + " MODIFY COLUMN " + definition + ";", nil
		}

	case WidenTypeEvolution:
		if operation.previous == nil {
			return "", evolutionPolicy(
				"render safe type widening",
				"missing previous column proof",
				string(target),
			)
		}
		if err := validatePreviousEvolutionTargetTable(
			target,
			operation,
		); err != nil {
			return "", err
		}
		previousType, err := renderEvolutionColumnType(
			target,
			*operation.previous,
		)
		if err != nil {
			return "", err
		}
		currentType, err := renderEvolutionColumnType(
			target,
			operation.current,
		)
		if err != nil {
			return "", err
		}
		if strings.EqualFold(previousType, currentType) {
			return "", evolutionPolicy(
				"render safe type widening",
				"proved columns render to the same target type",
				string(target),
			)
		}
		if err := validateEvolutionDefault(target, operation.current); err != nil {
			return "", err
		}
		switch target {
		case Postgres:
			return "ALTER TABLE " + qualified(
				target,
				operation.table.Schema,
				operation.table.Name,
			) + " ALTER COLUMN " + quote(
				target,
				operation.current.Name,
			) + " TYPE " + currentType + ";", nil
		case SQLServer:
			nullability := " NOT NULL"
			if operation.current.Nullable {
				nullability = " NULL"
			}
			return "ALTER TABLE " + qualified(
				target,
				operation.table.Schema,
				operation.table.Name,
			) + " ALTER COLUMN " + quote(
				target,
				operation.current.Name,
			) + " " + currentType + nullability + ";", nil
		case MySQL:
			definition, err := renderEvolutionColumnDefinition(
				target,
				operation.current,
				true,
			)
			if err != nil {
				return "", err
			}
			return "ALTER TABLE " + qualified(
				target,
				operation.table.Schema,
				operation.table.Name,
			) + " MODIFY COLUMN " + definition + ";", nil
		}
	}
	return "", evolutionPolicy(
		"render column evolution",
		"unreachable operation or target",
		string(target),
	)
}

func validateEvolutionWideningDependencies(
	catalog CompleteEvolutionCatalog,
	table Table,
	columnName string,
) error {
	const operation = "plan safe type widening"
	matches := 0
	for _, candidate := range catalog.tables {
		if candidate.Schema == table.Schema && candidate.Name == table.Name {
			matches++
			if !reflect.DeepEqual(candidate, table) {
				return evolutionPolicy(
					operation,
					"complete catalog table differs from current table evidence",
					"schema contract",
				)
			}
		}
	}
	if matches != 1 {
		return evolutionPolicy(
			operation,
			"current table must occur exactly once in the complete catalog",
			"schema contract",
		)
	}

	for _, index := range table.Indexes {
		for _, column := range index.Columns {
			if evolutionIdentifiersMayAlias(column.Name, columnName) {
				return evolutionPolicy(
					operation,
					"secondary index depends on the widened column",
					"schema contract",
				)
			}
		}
	}
	// CHECK expressions deliberately retain validated SQL rather than a full
	// dependency AST. Without a proven dependency set, even an apparently
	// unrelated CHECK must block a standalone type ALTER.
	if len(table.Checks) > 0 {
		return evolutionPolicy(
			operation,
			"CHECK dependency cannot be excluded for standalone widening",
			"schema contract",
		)
	}
	for _, owner := range catalog.tables {
		for _, foreignKey := range owner.ForeignKeys {
			if owner.Schema == table.Schema && owner.Name == table.Name &&
				evolutionStringsContain(
					foreignKey.Columns,
					columnName,
				) {
				return evolutionPolicy(
					operation,
					"foreign key depends on the widened owner column",
					"schema contract",
				)
			}
			referenced, err := evolutionCatalogReferencedTable(
				catalog.tables,
				owner,
				foreignKey,
			)
			if err != nil {
				return evolutionPolicy(
					operation,
					"complete catalog relation proof is invalid: "+
						err.Error(),
					"schema contract",
				)
			}
			if referenced.Schema != table.Schema ||
				referenced.Name != table.Name {
				continue
			}
			if evolutionStringsContain(
				foreignKey.ReferencedColumns,
				columnName,
			) {
				return evolutionPolicy(
					operation,
					"incoming foreign key depends on the widened column",
					"schema contract",
				)
			}
		}
	}
	return nil
}

func evolutionStringsContain(values []string, expected string) bool {
	for _, value := range values {
		if evolutionIdentifiersMayAlias(value, expected) {
			return true
		}
	}
	return false
}

func evolutionIdentifiersMayAlias(left, right string) bool {
	return left == right || strings.EqualFold(left, right)
}

func validateEvolutionTarget(target Dialect) error {
	switch target {
	case Postgres, SQLServer, MySQL, SQLite:
		return nil
	case ClickHouse:
		return evolutionPolicy(
			"render column evolution",
			"ClickHouse column evolution requires rebuild mode",
			string(target),
		)
	default:
		return evolutionPolicy(
			"render column evolution",
			"unknown target dialect",
			string(target),
		)
	}
}

func validateEvolutionTargetTable(target Dialect, table Table) error {
	switch target {
	case Postgres, MySQL, SQLite:
		_, err := CreateTable(target, table)
		return err
	case SQLServer:
		_, err := CreateSQLServerTable(table)
		return err
	default:
		return evolutionPolicy(
			"validate evolved target table",
			"unsupported target",
			string(target),
		)
	}
}

func validatePreviousEvolutionTargetTable(
	target Dialect,
	operation ColumnEvolution,
) error {
	previous := cloneEvolutionTable(operation.table)
	replaced := false
	for index := range previous.Columns {
		if previous.Columns[index].Name == operation.current.Name {
			previous.Columns[index] = cloneEvolutionColumn(*operation.previous)
			replaced = true
			break
		}
	}
	if !replaced {
		return evolutionPolicy(
			"validate previous target type",
			"evolved column is absent",
			string(target),
		)
	}
	if err := validateEvolutionTargetTable(target, previous); err != nil {
		return fmt.Errorf("validate previous target table: %w", err)
	}
	return nil
}

func renderEvolutionColumnDefinition(
	target Dialect,
	column Column,
	includeDefault bool,
) (string, error) {
	columnType, err := renderEvolutionColumnType(target, column)
	if err != nil {
		return "", err
	}
	nullability := " NOT NULL"
	if column.Nullable {
		nullability = " NULL"
	}
	definition := quote(target, column.Name) + " " +
		columnType + nullability
	if includeDefault && column.Default != nil {
		rendered, err := renderEvolutionDefault(target, column)
		if err != nil {
			return "", err
		}
		definition += " DEFAULT " + rendered
	}
	return definition, nil
}

func renderEvolutionColumnType(
	target Dialect,
	column Column,
) (string, error) {
	if target == SQLServer {
		return renderSQLServerDeclaredColumn(column)
	}
	return renderColumnType(column, target)
}

func renderEvolutionDefault(
	target Dialect,
	column Column,
) (string, error) {
	if target == SQLServer {
		return RenderSQLServerDefault(column)
	}
	return renderDefault(target, column)
}

func validateEvolutionDefault(target Dialect, column Column) error {
	if column.Default == nil {
		return nil
	}
	_, err := renderEvolutionDefault(target, column)
	return err
}

func validateEvolutionTableColumn(
	table Table,
	column Column,
	operation string,
) error {
	if table.Name == "" {
		return evolutionPolicy(
			operation,
			"table name is empty",
			"schema contract",
		)
	}
	if column.Name == "" {
		return evolutionPolicy(
			operation,
			"column name is empty",
			"schema contract",
		)
	}
	matches := 0
	for _, candidate := range table.Columns {
		if candidate.Name != column.Name {
			continue
		}
		matches++
		if !reflect.DeepEqual(candidate, column) {
			return evolutionPolicy(
				operation,
				"table column does not equal current evolution evidence",
				"schema contract",
			)
		}
	}
	if matches != 1 {
		return evolutionPolicy(
			operation,
			fmt.Sprintf(
				"current column must occur exactly once, found %d",
				matches,
			),
			"schema contract",
		)
	}
	return nil
}

func evolutionIdentityColumn(
	table Table,
	operation string,
) (string, error) {
	if table.Identity == nil {
		return "", nil
	}
	identity := table.Identity
	if identity.Column == "" ||
		identity.Generation != IdentityByDefault {
		return "", evolutionPolicy(
			operation,
			"table identity metadata is invalid",
			"schema contract",
		)
	}
	matches := 0
	for _, column := range table.Columns {
		if column.Name == identity.Column {
			matches++
		}
	}
	if matches != 1 {
		return "", evolutionPolicy(
			operation,
			"identity column must occur exactly once",
			"schema contract",
		)
	}
	return identity.Column, nil
}

func evolutionLiteralDefault(expression *Expression) bool {
	if expression == nil {
		return true
	}
	switch expression.kind {
	case expressionNull,
		expressionBoolean,
		expressionNumber,
		expressionString,
		expressionBlob:
		return true
	default:
		return false
	}
}

func safeEvolutionTypeWidening(previous, current Column) bool {
	if !evolutionTypeEvidenceConsistent(previous) ||
		!evolutionTypeEvidenceConsistent(current) {
		return false
	}
	generic := evolutionGenericTypeRelation(previous.Type, current.Type)
	if generic == evolutionTypeInvalid {
		return false
	}
	if previous.DeclaredType == nil || current.DeclaredType == nil {
		return previous.DeclaredType == nil &&
			current.DeclaredType == nil &&
			generic == evolutionTypeWidening
	}
	declared := evolutionDeclaredTypeRelation(
		*previous.DeclaredType,
		*current.DeclaredType,
	)
	if declared == evolutionTypeInvalid {
		return false
	}
	switch generic {
	case evolutionTypeEqual:
		return declared == evolutionTypeWidening
	case evolutionTypeWidening:
		return declared == evolutionTypeWidening &&
			evolutionDeclaredWideningMatchesGeneric(
				previous.Type,
				current.Type,
				*previous.DeclaredType,
				*current.DeclaredType,
			)
	default:
		return false
	}
}

type evolutionTypeRelation uint8

const (
	evolutionTypeInvalid evolutionTypeRelation = iota
	evolutionTypeEqual
	evolutionTypeWidening
)

func evolutionGenericTypeRelation(
	previous,
	current string,
) evolutionTypeRelation {
	previous = canonicalEvolutionGenericType(previous)
	current = canonicalEvolutionGenericType(current)
	if previous == "" || current == "" {
		return evolutionTypeInvalid
	}
	if previous == current {
		return evolutionTypeEqual
	}
	switch {
	case previous == "integer" && current == "bigint",
		previous == "real" && current == "double",
		previous == "varchar" && current == "text",
		previous == "varbinary" && current == "blob":
		return evolutionTypeWidening
	default:
		return evolutionTypeInvalid
	}
}

func evolutionDeclaredTypeRelation(
	previous,
	current DeclaredType,
) evolutionTypeRelation {
	previousBase := normalizeEvolutionType(previous.Base)
	currentBase := normalizeEvolutionType(current.Base)
	if equivalentEvolutionDeclaredBase(previousBase, currentBase) &&
		evolutionDeclaredModifiersEqual(previous, current) {
		return evolutionTypeEqual
	}
	if catalogTypeUsesStructuredModifiers(previous) ||
		catalogTypeUsesStructuredModifiers(current) {
		// Named catalog modifiers are preserved by the evolution proof, but no
		// widening is inferred until every target renderer consumes them.
		return evolutionTypeInvalid
	}
	if rank, ok := evolutionIntegerRank(previousBase); ok {
		currentRank, currentOK := evolutionIntegerRank(currentBase)
		if currentOK && currentRank > rank {
			return evolutionTypeWidening
		}
		return evolutionTypeInvalid
	}
	if evolutionNumericBase(previousBase) &&
		evolutionNumericBase(currentBase) {
		previousPrecision, previousScale, previousOK :=
			evolutionNumericModifiers(previous.Arguments)
		currentPrecision, currentScale, currentOK :=
			evolutionNumericModifiers(current.Arguments)
		if previousOK && currentOK &&
			currentScale >= previousScale &&
			currentPrecision-currentScale >=
				previousPrecision-previousScale &&
			(currentPrecision > previousPrecision ||
				currentScale > previousScale) {
			return evolutionTypeWidening
		}
		return evolutionTypeInvalid
	}
	if equivalentEvolutionDeclaredBase(previousBase, currentBase) {
		switch canonicalEvolutionDeclaredBase(previousBase) {
		case "varchar", "nvarchar", "varbinary":
			if len(previous.Arguments) == 1 &&
				len(current.Arguments) == 1 &&
				current.Arguments[0] > previous.Arguments[0] {
				return evolutionTypeWidening
			}
		case "time", "datetime", "timestamp", "timestamptz":
			if len(previous.Arguments) == 1 &&
				len(current.Arguments) == 1 &&
				current.Arguments[0] > previous.Arguments[0] {
				return evolutionTypeWidening
			}
		}
		return evolutionTypeInvalid
	}
	if evolutionTextRank(previousBase) >= 0 &&
		evolutionTextRank(currentBase) > evolutionTextRank(previousBase) {
		return evolutionTypeWidening
	}
	if evolutionBlobRank(previousBase) >= 0 &&
		evolutionBlobRank(currentBase) > evolutionBlobRank(previousBase) {
		return evolutionTypeWidening
	}
	if evolutionVariableTextBase(previousBase) &&
		evolutionTextRank(currentBase) >= evolutionTextRank("text") {
		return evolutionTypeWidening
	}
	if previousBase == "varbinary" &&
		evolutionBlobRank(currentBase) >= evolutionBlobRank("blob") {
		return evolutionTypeWidening
	}
	if (previousBase == "real" || previousBase == "float4") &&
		(currentBase == "double" ||
			currentBase == "double precision" ||
			currentBase == "float8") {
		return evolutionTypeWidening
	}
	return evolutionTypeInvalid
}

func evolutionDeclaredWideningMatchesGeneric(
	previousType,
	currentType string,
	previous,
	current DeclaredType,
) bool {
	previousType = canonicalEvolutionGenericType(previousType)
	currentType = canonicalEvolutionGenericType(currentType)
	previousBase := normalizeEvolutionType(previous.Base)
	currentBase := normalizeEvolutionType(current.Base)
	switch {
	case previousType == "integer" && currentType == "bigint":
		previousRank, previousOK := evolutionIntegerRank(previousBase)
		currentRank, currentOK := evolutionIntegerRank(currentBase)
		return previousOK && currentOK &&
			previousRank < 5 && currentRank == 5
	case previousType == "real" && currentType == "double":
		return (previousBase == "real" || previousBase == "float4") &&
			(currentBase == "double" ||
				currentBase == "double precision" ||
				currentBase == "float8")
	case previousType == "varchar" && currentType == "text":
		return evolutionVariableTextBase(previousBase) &&
			evolutionTextRank(currentBase) >= evolutionTextRank("text")
	case previousType == "varbinary" && currentType == "blob":
		return previousBase == "varbinary" &&
			evolutionBlobRank(currentBase) >= evolutionBlobRank("blob")
	default:
		return false
	}
}

func evolutionTypeEvidenceConsistent(column Column) bool {
	if column.DeclaredType == nil {
		return true
	}
	if catalogTypeUsesStructuredModifiers(*column.DeclaredType) &&
		ValidateDeclaredType(*column.DeclaredType) != nil {
		return false
	}
	generic := canonicalEvolutionGenericType(column.Type)
	base := normalizeEvolutionType(column.DeclaredType.Base)
	switch generic {
	case "integer":
		rank, ok := evolutionIntegerRank(base)
		return ok && rank < 5
	case "bigint":
		rank, ok := evolutionIntegerRank(base)
		return ok && rank == 5
	case "numeric":
		return evolutionNumericBase(base)
	case "real":
		return base == "real" || base == "float4"
	case "double":
		return base == "double" ||
			base == "double precision" ||
			base == "float8" ||
			base == "float"
	case "varchar":
		return evolutionVariableTextBase(base)
	case "char":
		return base == "char" ||
			base == "character" ||
			base == "nchar"
	case "text":
		return evolutionVariableTextBase(base) ||
			base == "char" ||
			base == "character" ||
			base == "nchar" ||
			evolutionTextRank(base) >= 0
	case "binary":
		return base == "binary"
	case "varbinary":
		return base == "varbinary"
	case "blob":
		return base == "binary" ||
			base == "varbinary" ||
			base == "bytea" ||
			evolutionBlobRank(base) >= 0
	case "time":
		return base == "time"
	case "date":
		return base == "date"
	case "timestamp":
		return base == "timestamp" || base == "datetime"
	case "datetime":
		return base == "datetime" ||
			base == "datetime2" ||
			base == "timestamp" ||
			base == "smalldatetime"
	case "timestamptz":
		return base == "timestamptz"
	case "boolean":
		return base == "bool" ||
			base == "boolean" ||
			base == "bit"
	case "uuid":
		return base == "uuid" || base == "uniqueidentifier"
	case "json":
		return base == "json"
	case "jsonb":
		return base == "jsonb"
	default:
		return false
	}
}

func canonicalEvolutionGenericType(value string) string {
	switch normalizeEvolutionType(value) {
	case "int", "integer", "int4":
		return "integer"
	case "bigint", "int8":
		return "bigint"
	case "real", "float4":
		return "real"
	case "double", "double precision", "float8":
		return "double"
	case "decimal", "numeric":
		return "numeric"
	case "varchar", "character varying", "varying character":
		return "varchar"
	case "char", "character":
		return "char"
	case "bool", "boolean":
		return "boolean"
	default:
		return normalizeEvolutionType(value)
	}
}

func equivalentEvolutionDeclaredBase(left, right string) bool {
	return canonicalEvolutionDeclaredBase(left) ==
		canonicalEvolutionDeclaredBase(right)
}

func canonicalEvolutionDeclaredBase(value string) string {
	switch normalizeEvolutionType(value) {
	case "decimal", "numeric":
		return "numeric"
	case "character varying", "varying character", "varchar":
		return "varchar"
	case "native character", "nvarchar":
		return "nvarchar"
	case "double", "double precision", "float8":
		return "double"
	default:
		return normalizeEvolutionType(value)
	}
}

func normalizeEvolutionType(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func evolutionVariableTextBase(value string) bool {
	switch canonicalEvolutionDeclaredBase(value) {
	case "varchar", "nvarchar":
		return true
	default:
		return false
	}
}

func evolutionNumericModifiers(
	arguments []int,
) (precision int, scale int, ok bool) {
	switch len(arguments) {
	case 1:
		return arguments[0], 0, arguments[0] > 0
	case 2:
		return arguments[0], arguments[1],
			arguments[0] > 0 &&
				arguments[1] >= 0 &&
				arguments[1] <= arguments[0]
	default:
		return 0, 0, false
	}
}

func evolutionIntegerRank(value string) (int, bool) {
	switch value {
	case "tinyint":
		return 1, true
	case "smallint", "int2":
		return 2, true
	case "mediumint":
		return 3, true
	case "int", "integer", "int4":
		return 4, true
	case "bigint", "int8":
		return 5, true
	default:
		return 0, false
	}
}

func evolutionNumericBase(value string) bool {
	return value == "numeric" || value == "decimal"
}

func evolutionTextRank(value string) int {
	switch value {
	case "tinytext":
		return 0
	case "text":
		return 1
	case "mediumtext":
		return 2
	case "longtext":
		return 3
	default:
		return -1
	}
}

func evolutionBlobRank(value string) int {
	switch value {
	case "tinyblob":
		return 0
	case "blob":
		return 1
	case "mediumblob":
		return 2
	case "longblob":
		return 3
	default:
		return -1
	}
}

func newColumnEvolution(
	kind ColumnEvolutionKind,
	table Table,
	previous *Column,
	current Column,
) ColumnEvolution {
	result := ColumnEvolution{
		kind:    kind,
		table:   cloneEvolutionTable(table),
		current: cloneEvolutionColumn(current),
	}
	if previous != nil {
		cloned := cloneEvolutionColumn(*previous)
		result.previous = &cloned
	}
	return result
}

func cloneEvolutionTable(value Table) Table {
	cloned := value
	if value.Identity != nil {
		identity := *value.Identity
		if value.Identity.Frontier != nil {
			frontier := *value.Identity.Frontier
			identity.Frontier = &frontier
		}
		cloned.Identity = &identity
	}
	cloned.Columns = make([]Column, len(value.Columns))
	for index, column := range value.Columns {
		cloned.Columns[index] = cloneEvolutionColumn(column)
	}
	cloned.ClickHouseOrderBy = cloneEvolutionStrings(
		value.ClickHouseOrderBy,
	)
	cloned.Indexes = cloneEvolutionIndexes(value.Indexes)
	for index := range cloned.Indexes {
		if value.Indexes[index].Columns != nil {
			cloned.Indexes[index].Columns = append(
				[]IndexColumn{},
				value.Indexes[index].Columns...,
			)
		}
	}
	cloned.ForeignKeys = cloneEvolutionForeignKeys(value.ForeignKeys)
	for index := range cloned.ForeignKeys {
		cloned.ForeignKeys[index].Columns = cloneEvolutionStrings(
			value.ForeignKeys[index].Columns,
		)
		cloned.ForeignKeys[index].ReferencedColumns = cloneEvolutionStrings(
			value.ForeignKeys[index].ReferencedColumns,
		)
	}
	if value.Checks != nil {
		cloned.Checks = append([]CheckConstraint{}, value.Checks...)
	}
	return cloned
}

func cloneEvolutionColumn(value Column) Column {
	cloned := value
	cloned.DeclaredType = cloneEvolutionDeclaredType(value.DeclaredType)
	if value.Default != nil {
		expression := *value.Default
		cloned.Default = &expression
	}
	return cloned
}

func cloneEvolutionDeclaredType(value *DeclaredType) *DeclaredType {
	if value == nil {
		return nil
	}
	cloned := *value
	if value.Arguments != nil {
		cloned.Arguments = append([]int{}, value.Arguments...)
	}
	cloned.Length = cloneInt64Pointer(value.Length)
	cloned.Precision = cloneInt64Pointer(value.Precision)
	cloned.Scale = cloneInt64Pointer(value.Scale)
	cloned.FractionalSecondPrecision = cloneInt64Pointer(
		value.FractionalSecondPrecision,
	)
	if value.Spatial != nil {
		spatial := *value.Spatial
		spatial.SRID = cloneUint32Pointer(value.Spatial.SRID)
		cloned.Spatial = &spatial
	}
	if value.MySQL != nil {
		mysql := *value.MySQL
		mysql.BitWidth = cloneInt64Pointer(value.MySQL.BitWidth)
		mysql.EnumMembers = cloneOptionalStrings(value.MySQL.EnumMembers)
		mysql.SetMembers = cloneOptionalStrings(value.MySQL.SetMembers)
		cloned.MySQL = &mysql
	}
	return &cloned
}

func evolutionDeclaredModifiersEqual(
	previous,
	current DeclaredType,
) bool {
	previous.Base = ""
	current.Base = ""
	return reflect.DeepEqual(previous, current)
}

func cloneEvolutionStrings(value []string) []string {
	if value == nil {
		return nil
	}
	return append([]string{}, value...)
}

func cloneEvolutionIndexes(value []Index) []Index {
	if value == nil {
		return nil
	}
	return append([]Index{}, value...)
}

func cloneEvolutionForeignKeys(value []ForeignKey) []ForeignKey {
	if value == nil {
		return nil
	}
	return append([]ForeignKey{}, value...)
}

func evolutionPolicy(
	operation,
	detail,
	target string,
) error {
	return &PolicyError{
		Operation: operation,
		Type:      detail,
		Target:    target,
	}
}
