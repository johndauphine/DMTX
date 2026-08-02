package migrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Type comparison: which declared and generic type changes count as safe
// widening, and when the evidence supports saying so.

func safeSchemaContractDefault(value string) bool {
	value = strings.TrimSpace(value)
	switch strings.ToUpper(value) {
	case "NULL", "TRUE", "FALSE":
		return true
	}
	if schemaContractNumberDefault.MatchString(value) {
		return true
	}
	if validSchemaContractQuotedLiteral(value, '\'') {
		return true
	}
	if len(value) >= 3 &&
		(value[0] == 'X' || value[0] == 'x') &&
		validSchemaContractQuotedLiteral(value[1:], '\'') {
		for _, digit := range value[2 : len(value)-1] {
			if !strings.ContainsRune("0123456789abcdefABCDEF", digit) {
				return false
			}
		}
		return (len(value)-3)%2 == 0
	}
	return false
}

func validSchemaContractQuotedLiteral(value string, quote byte) bool {
	if len(value) < 2 || value[0] != quote || value[len(value)-1] != quote {
		return false
	}
	for index := 1; index < len(value)-1; index++ {
		if value[index] != quote {
			continue
		}
		if index+1 >= len(value)-1 || value[index+1] != quote {
			return false
		}
		index++
	}
	return true
}

func safeSchemaContractTypeWidening(
	previous,
	current schema.SnapshotColumn,
) bool {
	if !schemaContractTypeEvidenceConsistent(previous) ||
		!schemaContractTypeEvidenceConsistent(current) {
		return false
	}
	generic := schemaContractGenericTypeRelation(
		previous.Type,
		current.Type,
	)
	if generic == schemaContractTypeInvalid {
		return false
	}
	if previous.DeclaredType == nil || current.DeclaredType == nil {
		return previous.DeclaredType == nil &&
			current.DeclaredType == nil &&
			generic == schemaContractTypeWidening
	}
	declared := schemaContractDeclaredTypeRelation(
		*previous.DeclaredType,
		*current.DeclaredType,
	)
	if declared == schemaContractTypeInvalid {
		return false
	}
	switch generic {
	case schemaContractTypeEqual:
		return declared == schemaContractTypeWidening
	case schemaContractTypeWidening:
		// When both evidence channels are present they must independently
		// prove the same widening. A stale, unchanged declaration is not
		// sufficient corroboration for a changed generic type.
		return declared == schemaContractTypeWidening &&
			schemaContractDeclaredWideningMatchesGeneric(
				previous.Type,
				current.Type,
				*previous.DeclaredType,
				*current.DeclaredType,
			)
	default:
		return false
	}
}

func schemaContractDeclaredWideningMatchesGeneric(
	previousType,
	currentType string,
	previous,
	current schema.SnapshotDeclaredType,
) bool {
	previousType = canonicalSchemaContractGenericType(previousType)
	currentType = canonicalSchemaContractGenericType(currentType)
	previousBase := normalizeSchemaContractType(previous.Base)
	currentBase := normalizeSchemaContractType(current.Base)
	switch {
	case previousType == "integer" && currentType == "bigint":
		previousRank, previousOK := schemaContractIntegerRank(previousBase)
		currentRank, currentOK := schemaContractIntegerRank(currentBase)
		return previousOK && currentOK &&
			previousRank < 5 && currentRank == 5
	case previousType == "real" && currentType == "double":
		return (previousBase == "real" || previousBase == "float4") &&
			(currentBase == "double" ||
				currentBase == "double precision" ||
				currentBase == "float8")
	case previousType == "varchar" && currentType == "text":
		return schemaContractVariableTextBase(previousBase) &&
			schemaContractTextRank(currentBase) >=
				schemaContractTextRank("text")
	case previousType == "varbinary" && currentType == "blob":
		return previousBase == "varbinary" &&
			schemaContractBlobRank(currentBase) >=
				schemaContractBlobRank("blob")
	default:
		return false
	}
}

func normalizeSchemaContractType(value string) string {
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

type schemaContractTypeRelation uint8

const (
	schemaContractTypeInvalid schemaContractTypeRelation = iota
	schemaContractTypeEqual
	schemaContractTypeWidening
)

func schemaContractGenericTypeRelation(
	previous,
	current string,
) schemaContractTypeRelation {
	previous = canonicalSchemaContractGenericType(previous)
	current = canonicalSchemaContractGenericType(current)
	if previous == "" || current == "" {
		return schemaContractTypeInvalid
	}
	if previous == current {
		return schemaContractTypeEqual
	}
	switch {
	case previous == "integer" && current == "bigint",
		previous == "real" && current == "double",
		previous == "varchar" && current == "text",
		previous == "varbinary" && current == "blob":
		return schemaContractTypeWidening
	default:
		return schemaContractTypeInvalid
	}
}

func canonicalSchemaContractGenericType(value string) string {
	switch normalizeSchemaContractType(value) {
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
		return normalizeSchemaContractType(value)
	}
}

func schemaContractDeclaredTypeRelation(
	previous,
	current schema.SnapshotDeclaredType,
) schemaContractTypeRelation {
	previousBase := normalizeSchemaContractType(previous.Base)
	currentBase := normalizeSchemaContractType(current.Base)
	if schemaContractEquivalentDeclaredBase(previousBase, currentBase) &&
		reflect.DeepEqual(previous.Arguments, current.Arguments) {
		return schemaContractTypeEqual
	}
	if rank, ok := schemaContractIntegerRank(previousBase); ok {
		currentRank, currentOK := schemaContractIntegerRank(currentBase)
		if currentOK && currentRank > rank {
			return schemaContractTypeWidening
		}
		return schemaContractTypeInvalid
	}
	if schemaContractNumericBase(previousBase) &&
		schemaContractNumericBase(currentBase) {
		previousPrecision, previousScale, previousOK :=
			schemaContractNumericModifiers(previous.Arguments)
		currentPrecision, currentScale, currentOK :=
			schemaContractNumericModifiers(current.Arguments)
		if previousOK && currentOK &&
			currentScale >= previousScale &&
			currentPrecision-currentScale >=
				previousPrecision-previousScale &&
			(currentPrecision > previousPrecision ||
				currentScale > previousScale) {
			return schemaContractTypeWidening
		}
		return schemaContractTypeInvalid
	}
	if schemaContractEquivalentDeclaredBase(previousBase, currentBase) {
		switch canonicalSchemaContractDeclaredBase(previousBase) {
		case "varchar", "nvarchar", "varbinary":
			if len(previous.Arguments) == 1 &&
				len(current.Arguments) == 1 &&
				current.Arguments[0] > previous.Arguments[0] {
				return schemaContractTypeWidening
			}
		case "time", "datetime", "timestamp", "timestamptz":
			if len(previous.Arguments) == 1 &&
				len(current.Arguments) == 1 &&
				current.Arguments[0] > previous.Arguments[0] {
				return schemaContractTypeWidening
			}
		}
		return schemaContractTypeInvalid
	}
	if schemaContractTextRank(previousBase) >= 0 &&
		schemaContractTextRank(currentBase) >
			schemaContractTextRank(previousBase) {
		return schemaContractTypeWidening
	}
	if schemaContractBlobRank(previousBase) >= 0 &&
		schemaContractBlobRank(currentBase) >
			schemaContractBlobRank(previousBase) {
		return schemaContractTypeWidening
	}
	if schemaContractVariableTextBase(previousBase) &&
		schemaContractTextRank(currentBase) >=
			schemaContractTextRank("text") {
		return schemaContractTypeWidening
	}
	if previousBase == "varbinary" &&
		schemaContractBlobRank(currentBase) >=
			schemaContractBlobRank("blob") {
		return schemaContractTypeWidening
	}
	if (previousBase == "real" || previousBase == "float4") &&
		(currentBase == "double" ||
			currentBase == "double precision" ||
			currentBase == "float8") {
		return schemaContractTypeWidening
	}
	return schemaContractTypeInvalid
}

func schemaContractTypeEvidenceConsistent(
	column schema.SnapshotColumn,
) bool {
	if column.DeclaredType == nil {
		return true
	}
	generic := canonicalSchemaContractGenericType(column.Type)
	base := normalizeSchemaContractType(column.DeclaredType.Base)
	switch generic {
	case "integer":
		rank, ok := schemaContractIntegerRank(base)
		return ok && rank < 5
	case "bigint":
		rank, ok := schemaContractIntegerRank(base)
		return ok && rank == 5
	case "numeric":
		return schemaContractNumericBase(base)
	case "real":
		return base == "real" || base == "float4"
	case "double":
		return base == "double" ||
			base == "double precision" ||
			base == "float8" ||
			base == "float"
	case "varchar":
		return schemaContractVariableTextBase(base)
	case "char":
		return base == "char" ||
			base == "character" ||
			base == "nchar"
	case "text":
		return schemaContractVariableTextBase(base) ||
			base == "char" ||
			base == "character" ||
			base == "nchar" ||
			schemaContractTextRank(base) >= 0
	case "binary":
		return base == "binary"
	case "varbinary":
		return base == "varbinary"
	case "blob":
		return base == "binary" ||
			base == "varbinary" ||
			base == "bytea" ||
			schemaContractBlobRank(base) >= 0
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

func schemaContractEquivalentDeclaredBase(left, right string) bool {
	return canonicalSchemaContractDeclaredBase(left) ==
		canonicalSchemaContractDeclaredBase(right)
}

func canonicalSchemaContractDeclaredBase(value string) string {
	switch normalizeSchemaContractType(value) {
	case "decimal", "numeric":
		return "numeric"
	case "character varying", "varying character", "varchar":
		return "varchar"
	case "native character", "nvarchar":
		return "nvarchar"
	case "double", "double precision", "float8":
		return "double"
	default:
		return normalizeSchemaContractType(value)
	}
}

func schemaContractVariableTextBase(value string) bool {
	switch canonicalSchemaContractDeclaredBase(value) {
	case "varchar", "nvarchar":
		return true
	default:
		return false
	}
}

func schemaContractNumericModifiers(arguments []int) (int, int, bool) {
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

func schemaContractIntegerRank(value string) (int, bool) {
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

func schemaContractNumericBase(value string) bool {
	return value == "numeric" || value == "decimal"
}

func schemaContractTextRank(value string) int {
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

func schemaContractBlobRank(value string) int {
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

func applySafeUpsertDecisions(
	upsert *schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	decisions []SchemaContractDecision,
) {
	for _, decision := range decisions {
		key := schemaContractTableKey{
			schema: decision.Object.Schema,
			table:  decision.Object.Table,
		}
		switch decision.Action {
		case SchemaContractCreateTable:
			if table, ok := findSchemaContractTable(current, key); ok {
				setSchemaContractTable(upsert, table)
			}
		case SchemaContractAddColumn:
			applySchemaContractColumnAdd(
				upsert,
				current,
				key,
				decision.Object.Column,
			)
		case SchemaContractRelaxNullability:
			updateSchemaContractColumn(
				upsert,
				current,
				key,
				decision.Object.Column,
				func(target *schema.SnapshotColumn, source schema.SnapshotColumn) {
					target.Nullable = source.Nullable
				},
			)
		case SchemaContractWidenType:
			updateSchemaContractColumn(
				upsert,
				current,
				key,
				decision.Object.Column,
				func(target *schema.SnapshotColumn, source schema.SnapshotColumn) {
					target.Type = source.Type
					target.DeclaredType = cloneSnapshotDeclaredType(source.DeclaredType)
				},
			)
		}
	}
}

func applySchemaContractColumnAdd(
	target *schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	key schemaContractTableKey,
	columnName string,
) {
	currentTable, ok := findSchemaContractTable(current, key)
	if !ok {
		return
	}
	var currentColumn schema.SnapshotColumn
	currentPosition := -1
	for index, column := range currentTable.Columns {
		if column.Name == columnName {
			currentColumn = cloneSnapshotColumn(column)
			currentPosition = index
			break
		}
	}
	if currentPosition < 0 {
		return
	}
	for tableIndex := range target.Tables {
		if schemaContractKey(target.Tables[tableIndex]) != key {
			continue
		}
		columns := target.Tables[tableIndex].Columns
		insertAt := len(columns)
		for index := currentPosition + 1; index < len(currentTable.Columns); index++ {
			next := currentTable.Columns[index].Name
			for targetIndex, targetColumn := range columns {
				if targetColumn.Name == next {
					insertAt = targetIndex
					index = len(currentTable.Columns)
					break
				}
			}
		}
		columns = append(columns, schema.SnapshotColumn{})
		copy(columns[insertAt+1:], columns[insertAt:])
		columns[insertAt] = currentColumn
		target.Tables[tableIndex].Columns = columns
		return
	}
}

func updateSchemaContractColumn(
	target *schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	key schemaContractTableKey,
	columnName string,
	update func(*schema.SnapshotColumn, schema.SnapshotColumn),
) {
	currentTable, ok := findSchemaContractTable(current, key)
	if !ok {
		return
	}
	var currentColumn schema.SnapshotColumn
	found := false
	for _, column := range currentTable.Columns {
		if column.Name == columnName {
			currentColumn = column
			found = true
			break
		}
	}
	if !found {
		return
	}
	for tableIndex := range target.Tables {
		if schemaContractKey(target.Tables[tableIndex]) != key {
			continue
		}
		for columnIndex := range target.Tables[tableIndex].Columns {
			if target.Tables[tableIndex].Columns[columnIndex].Name == columnName {
				update(
					&target.Tables[tableIndex].Columns[columnIndex],
					currentColumn,
				)
				return
			}
		}
	}
}

func projectSchemaContractSnapshot(
	target *schema.SchemaSnapshot,
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
	retainedTypeColumns map[schemaContractTableKey]map[string]struct{},
) {
	tables := make([]schema.SnapshotTable, 0, len(target.Tables))
	for _, table := range target.Tables {
		key := schemaContractKey(table)
		if _, discarded := discardedTables[key]; discarded {
			continue
		}
		columns := discardedColumns[key]
		retained := retainedTypeColumns[key]
		if len(columns) > 0 {
			filtered := make([]schema.SnapshotColumn, 0, len(table.Columns))
			for _, column := range table.Columns {
				if _, discard := columns[column.Name]; discard {
					if _, retain := retained[column.Name]; !retain {
						continue
					}
				}
				filtered = append(filtered, column)
			}
			table.Columns = filtered
		}
		table = pruneSchemaContractDependencies(
			table,
			columns,
			discardedTables,
			discardedColumns,
		)
		tables = append(tables, table)
	}
	target.Tables = tables
}

func restoreDiscardedTypeEvidence(
	target *schema.SchemaSnapshot,
	previous schema.SchemaSnapshot,
	columns map[schemaContractTableKey]map[string]struct{},
) {
	for tableIndex := range target.Tables {
		key := schemaContractKey(target.Tables[tableIndex])
		previousTable, ok := findSchemaContractTable(previous, key)
		if !ok {
			continue
		}
		for columnIndex := range target.Tables[tableIndex].Columns {
			name := target.Tables[tableIndex].Columns[columnIndex].Name
			if _, restore := columns[key][name]; !restore {
				continue
			}
			for _, previousColumn := range previousTable.Columns {
				if previousColumn.Name == name {
					target.Tables[tableIndex].Columns[columnIndex] =
						cloneSnapshotColumn(previousColumn)
					break
				}
			}
		}
	}
}

func restoreRebuildSourceDrops(
	target *schema.SchemaSnapshot,
	previous schema.SchemaSnapshot,
	decisions []SchemaContractDecision,
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) error {
	droppedTables := make(map[schemaContractTableKey]struct{})
	droppedColumns := make(
		map[schemaContractTableKey]map[string]struct{},
	)
	for _, decision := range decisions {
		if decision.Action != SchemaContractRetainTarget &&
			decision.Action != SchemaContractReport {
			continue
		}
		key := schemaContractTableKey{
			schema: decision.Object.Schema,
			table:  decision.Object.Table,
		}
		switch decision.ChangeKind {
		case schema.SchemaDriftTableDropped:
			droppedTables[key] = struct{}{}
		case schema.SchemaDriftColumnDropped:
			addSchemaContractColumn(
				droppedColumns,
				key,
				decision.Object.Column,
			)
		}
	}
	if len(droppedTables) == 0 && len(droppedColumns) == 0 {
		return nil
	}

	for _, key := range sortedSchemaContractTableKeys(droppedTables) {
		if _, excluded := discardedTables[key]; excluded {
			continue
		}
		previousTable, ok := findSchemaContractTable(previous, key)
		if !ok {
			return fmt.Errorf(
				"dropped table %s has no previous evidence",
				schemaContractQualifiedKey(key),
			)
		}
		if _, exists := findSchemaContractTable(*target, key); exists {
			return fmt.Errorf(
				"dropped table %s conflicts with current evidence",
				schemaContractQualifiedKey(key),
			)
		}
		setSchemaContractTable(target, previousTable)
	}

	for _, key := range sortedSchemaContractColumnKeys(droppedColumns) {
		if _, excluded := discardedTables[key]; excluded {
			continue
		}
		previousTable, ok := findSchemaContractTable(previous, key)
		if !ok {
			return fmt.Errorf(
				"dropped columns for %s have no previous table evidence",
				schemaContractQualifiedKey(key),
			)
		}
		tableIndex := schemaContractTableIndex(*target, key)
		if tableIndex < 0 {
			return fmt.Errorf(
				"dropped columns for %s have no current rebuild table",
				schemaContractQualifiedKey(key),
			)
		}
		if err := restoreRebuildTableColumns(
			&target.Tables[tableIndex],
			previousTable,
			droppedColumns[key],
			discardedTables,
			discardedColumns,
		); err != nil {
			return err
		}
	}

	// Restore inbound FKs after every retained table/column exists. This
	// prevents rebuilding another selected table from destructively dropping
	// a relationship whose referenced source object disappeared.
	for _, previousOwner := range previous.Tables {
		ownerKey := schemaContractKey(previousOwner)
		if _, excluded := discardedTables[ownerKey]; excluded {
			continue
		}
		ownerIndex := schemaContractTableIndex(*target, ownerKey)
		if ownerIndex < 0 {
			continue
		}
		for _, foreignKey := range previousOwner.ForeignKeys {
			if !schemaContractForeignKeyDependsOnSourceDrop(
				previousOwner,
				foreignKey,
				droppedTables,
				droppedColumns,
			) {
				continue
			}
			if schemaContractRestoredForeignKeyExcluded(
				previousOwner,
				foreignKey,
				discardedTables,
				discardedColumns,
			) {
				continue
			}
			if err := mergeSchemaContractForeignKey(
				&target.Tables[ownerIndex].ForeignKeys,
				foreignKey,
			); err != nil {
				return fmt.Errorf(
					"retain source-drop foreign key on %s: %w",
					schemaContractQualifiedKey(ownerKey),
					err,
				)
			}
		}
	}
	// Restoration is deliberately last, so reapply every active exclusion as
	// a final invariant. This covers complete restored tables and any
	// multi-column dependency spanning a retained drop and a discarded object.
	projectSchemaContractSnapshot(
		target,
		discardedTables,
		discardedColumns,
		nil,
	)
	return nil
}

func restoreRebuildTableColumns(
	target *schema.SnapshotTable,
	previous schema.SnapshotTable,
	dropped map[string]struct{},
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) error {
	existing := make(map[string]struct{}, len(target.Columns))
	for _, column := range target.Columns {
		existing[column.Name] = struct{}{}
	}
	for previousIndex, column := range previous.Columns {
		if _, restore := dropped[column.Name]; !restore {
			continue
		}
		if _, collision := existing[column.Name]; collision {
			return fmt.Errorf(
				"retained column %s.%s conflicts with current evidence",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				column.Name,
			)
		}
		insertAt := len(target.Columns)
		for next := previousIndex + 1; next < len(previous.Columns); next++ {
			for targetIndex, targetColumn := range target.Columns {
				if targetColumn.Name == previous.Columns[next].Name {
					insertAt = targetIndex
					next = len(previous.Columns)
					break
				}
			}
		}
		target.Columns = append(target.Columns, schema.SnapshotColumn{})
		copy(target.Columns[insertAt+1:], target.Columns[insertAt:])
		target.Columns[insertAt] = cloneSnapshotColumn(column)
		existing[column.Name] = struct{}{}
	}

	if previous.Identity != nil {
		if _, restore := dropped[previous.Identity.Column]; restore {
			switch {
			case target.Identity == nil:
				identity := *previous.Identity
				target.Identity = &identity
			case !reflect.DeepEqual(target.Identity, previous.Identity):
				return fmt.Errorf(
					"retained identity on %s conflicts with current identity",
					schemaContractQualifiedKey(schemaContractKey(previous)),
				)
			}
		}
	}
	for _, index := range previous.Indexes {
		if !schemaContractIndexUsesAny(index, dropped) {
			continue
		}
		if schemaContractIndexUsesAny(
			index,
			discardedColumns[schemaContractKey(previous)],
		) {
			continue
		}
		if err := mergeSchemaContractIndex(&target.Indexes, index); err != nil {
			return fmt.Errorf(
				"retain source-drop index on %s: %w",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				err,
			)
		}
	}
	for _, check := range previous.Checks {
		if !schemaContractCheckUsesAny(check.Expression, dropped) {
			continue
		}
		if schemaContractCheckUsesAny(
			check.Expression,
			discardedColumns[schemaContractKey(previous)],
		) {
			continue
		}
		if err := mergeSchemaContractCheck(&target.Checks, check); err != nil {
			return fmt.Errorf(
				"retain source-drop check on %s: %w",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				err,
			)
		}
	}
	for _, foreignKey := range previous.ForeignKeys {
		if !schemaContractStringsUseAny(foreignKey.Columns, dropped) {
			continue
		}
		if schemaContractRestoredForeignKeyExcluded(
			previous,
			foreignKey,
			discardedTables,
			discardedColumns,
		) {
			continue
		}
		if err := mergeSchemaContractForeignKey(
			&target.ForeignKeys,
			foreignKey,
		); err != nil {
			return fmt.Errorf(
				"retain source-drop foreign key on %s: %w",
				schemaContractQualifiedKey(schemaContractKey(previous)),
				err,
			)
		}
	}
	return nil
}

func schemaContractForeignKeyDependsOnSourceDrop(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	droppedTables map[schemaContractTableKey]struct{},
	droppedColumns map[schemaContractTableKey]map[string]struct{},
) bool {
	for key := range droppedTables {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) {
			return true
		}
	}
	for key, columns := range droppedColumns {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) &&
			schemaContractStringsUseAny(
				foreignKey.ReferencedColumns,
				columns,
			) {
			return true
		}
	}
	return false
}

func schemaContractRestoredForeignKeyExcluded(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) bool {
	if schemaContractStringsUseAny(
		foreignKey.Columns,
		discardedColumns[schemaContractKey(owner)],
	) {
		return true
	}
	if schemaContractFKReferencesDiscarded(
		owner,
		foreignKey,
		discardedTables,
	) {
		return true
	}
	return schemaContractFKReferencesDiscardedColumns(
		owner,
		foreignKey,
		discardedColumns,
	)
}

func mergeSchemaContractIndex(
	target *[]schema.SnapshotIndex,
	value schema.SnapshotIndex,
) error {
	for _, existing := range *target {
		if schemaContractSideObjectMatches(
			existing.Name,
			value.Name,
			existing,
			value,
		) {
			if reflect.DeepEqual(existing, value) {
				return nil
			}
			return fmt.Errorf("index %q has conflicting current metadata", value.Name)
		}
	}
	*target = append(*target, value)
	return nil
}

func mergeSchemaContractForeignKey(
	target *[]schema.SnapshotForeignKey,
	value schema.SnapshotForeignKey,
) error {
	for _, existing := range *target {
		if schemaContractSideObjectMatches(
			existing.Name,
			value.Name,
			existing,
			value,
		) {
			if reflect.DeepEqual(existing, value) {
				return nil
			}
			return fmt.Errorf(
				"foreign key %q has conflicting current metadata",
				value.Name,
			)
		}
	}
	*target = append(*target, value)
	return nil
}

func mergeSchemaContractCheck(
	target *[]schema.SnapshotCheckConstraint,
	value schema.SnapshotCheckConstraint,
) error {
	for _, existing := range *target {
		if schemaContractSideObjectMatches(
			existing.Name,
			value.Name,
			existing,
			value,
		) {
			if reflect.DeepEqual(existing, value) {
				return nil
			}
			return fmt.Errorf("check %q has conflicting current metadata", value.Name)
		}
	}
	*target = append(*target, value)
	return nil
}

func schemaContractSideObjectMatches(
	leftName,
	rightName string,
	left,
	right any,
) bool {
	if leftName != "" || rightName != "" {
		return leftName == rightName
	}
	return reflect.DeepEqual(left, right)
}

func sortedSchemaContractTableKeys(
	values map[schemaContractTableKey]struct{},
) []schemaContractTableKey {
	result := make([]schemaContractTableKey, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sortSchemaContractTableKeys(result)
	return result
}

func sortedSchemaContractColumnKeys(
	values map[schemaContractTableKey]map[string]struct{},
) []schemaContractTableKey {
	result := make([]schemaContractTableKey, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sortSchemaContractTableKeys(result)
	return result
}

func sortSchemaContractTableKeys(values []schemaContractTableKey) {
	sort.Slice(values, func(left, right int) bool {
		if values[left].schema != values[right].schema {
			return values[left].schema < values[right].schema
		}
		return values[left].table < values[right].table
	})
}

func schemaContractQualifiedKey(key schemaContractTableKey) string {
	if key.schema == "" {
		return key.table
	}
	return key.schema + "." + key.table
}

func schemaContractTableIndex(
	snapshot schema.SchemaSnapshot,
	key schemaContractTableKey,
) int {
	for index := range snapshot.Tables {
		if schemaContractKey(snapshot.Tables[index]) == key {
			return index
		}
	}
	return -1
}

func pruneSchemaContractDependencies(
	table schema.SnapshotTable,
	columns map[string]struct{},
	discardedTables map[schemaContractTableKey]struct{},
	discardedColumns map[schemaContractTableKey]map[string]struct{},
) schema.SnapshotTable {
	if len(columns) > 0 {
		indexes := table.Indexes[:0]
		for _, index := range table.Indexes {
			if !schemaContractIndexUsesAny(index, columns) {
				indexes = append(indexes, index)
			}
		}
		table.Indexes = indexes

		checks := table.Checks[:0]
		for _, check := range table.Checks {
			if !schemaContractCheckUsesAny(check.Expression, columns) {
				checks = append(checks, check)
			}
		}
		table.Checks = checks
	}

	foreignKeys := table.ForeignKeys[:0]
	for _, foreignKey := range table.ForeignKeys {
		if schemaContractStringsUseAny(foreignKey.Columns, columns) ||
			schemaContractFKReferencesDiscarded(
				table,
				foreignKey,
				discardedTables,
			) {
			continue
		}
		if schemaContractFKReferencesDiscardedColumns(
			table,
			foreignKey,
			discardedColumns,
		) {
			continue
		}
		foreignKeys = append(foreignKeys, foreignKey)
	}
	table.ForeignKeys = foreignKeys
	return table
}

func schemaContractIndexUsesAny(
	index schema.SnapshotIndex,
	columns map[string]struct{},
) bool {
	for _, column := range index.Columns {
		if _, exists := columns[column.Name]; exists {
			return true
		}
	}
	return false
}

func schemaContractStringsUseAny(
	values []string,
	columns map[string]struct{},
) bool {
	for _, value := range values {
		if _, exists := columns[value]; exists {
			return true
		}
	}
	return false
}

func schemaContractFKReferencesDiscarded(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	discarded map[schemaContractTableKey]struct{},
) bool {
	for key := range discarded {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) {
			return true
		}
	}
	return false
}

func schemaContractFKReferencesDiscardedColumns(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	discarded map[schemaContractTableKey]map[string]struct{},
) bool {
	for key, columns := range discarded {
		if schemaContractReferencedTableMatches(owner, foreignKey, key) &&
			schemaContractStringsUseAny(
				foreignKey.ReferencedColumns,
				columns,
			) {
			return true
		}
	}
	return false
}

func schemaContractReferencedTableMatches(
	owner schema.SnapshotTable,
	foreignKey schema.SnapshotForeignKey,
	key schemaContractTableKey,
) bool {
	reference := foreignKey.ReferencedTable
	if foreignKey.ReferencedSchema != "" {
		return foreignKey.ReferencedSchema == key.schema &&
			reference == key.table
	}
	if reference == key.schema+"."+key.table && key.schema != "" {
		return true
	}
	if reference != key.table {
		return false
	}
	// An unqualified reference is conservatively treated as matching every
	// same-named discarded table. Over-pruning is safe; retaining a dangling FK
	// is not.
	return owner.Schema == key.schema || key.schema == ""
}

func schemaContractCheckUsesAny(
	expression string,
	columns map[string]struct{},
) bool {
	identifiers, complete := schemaContractSQLIdentifiers(expression)
	if !complete {
		return true
	}
	for _, identifier := range identifiers {
		if _, exists := columns[identifier]; exists {
			return true
		}
	}
	return false
}

func schemaContractSQLIdentifiers(value string) ([]string, bool) {
	result := make([]string, 0)
	for index := 0; index < len(value); {
		switch value[index] {
		case '\'':
			index++
			closed := false
			for index < len(value) {
				if value[index] != '\'' {
					index++
					continue
				}
				if index+1 < len(value) && value[index+1] == '\'' {
					index += 2
					continue
				}
				index++
				closed = true
				break
			}
			if !closed {
				return nil, false
			}
		case '"':
			index++
			var identifier strings.Builder
			closed := false
			for index < len(value) {
				if value[index] != '"' {
					identifier.WriteByte(value[index])
					index++
					continue
				}
				if index+1 < len(value) && value[index+1] == '"' {
					identifier.WriteByte('"')
					index += 2
					continue
				}
				index++
				closed = true
				break
			}
			if !closed {
				return nil, false
			}
			result = append(result, identifier.String())
		default:
			if value[index] == '_' ||
				unicode.IsLetter(rune(value[index])) {
				start := index
				index++
				for index < len(value) &&
					(value[index] == '_' ||
						value[index] == '$' ||
						unicode.IsLetter(rune(value[index])) ||
						unicode.IsDigit(rune(value[index]))) {
					index++
				}
				result = append(result, value[start:index])
				continue
			}
			index++
		}
	}
	return result, true
}

func normalizeSchemaContractPlan(plan *SchemaContractPlan) error {
	for _, item := range []struct {
		name   string
		target *schema.SchemaSnapshot
	}{
		{name: "transfer", target: &plan.TransferSnapshot},
		{name: "validation", target: &plan.ValidationSnapshot},
		{name: "successful", target: &plan.SuccessfulSnapshot},
		{name: "upsert", target: &plan.UpsertSnapshot},
		{name: "rebuild", target: &plan.RebuildSnapshot},
	} {
		normalized, err := canonicalSchemaSnapshot(*item.target)
		if err != nil {
			return fmt.Errorf("%s snapshot: %w", item.name, err)
		}
		if _, err := schema.CompareSchemaSnapshots(normalized, normalized); err != nil {
			return fmt.Errorf("%s snapshot: %w", item.name, err)
		}
		*item.target = normalized
	}
	sort.SliceStable(plan.Decisions, func(left, right int) bool {
		leftJSON, _ := json.Marshal(plan.Decisions[left])
		rightJSON, _ := json.Marshal(plan.Decisions[right])
		return bytes.Compare(leftJSON, rightJSON) < 0
	})
	return nil
}

func canonicalSchemaSnapshot(
	value schema.SchemaSnapshot,
) (schema.SchemaSnapshot, error) {
	encoded, err := value.CanonicalJSON()
	if err != nil {
		return schema.SchemaSnapshot{}, err
	}
	return schema.ParseSchemaSnapshot(encoded)
}

func cloneSchemaSnapshot(value schema.SchemaSnapshot) schema.SchemaSnapshot {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("clone schema contract snapshot: %v", err))
	}
	var result schema.SchemaSnapshot
	if err := json.Unmarshal(encoded, &result); err != nil {
		panic(fmt.Sprintf("clone schema contract snapshot: %v", err))
	}
	return result
}

func cloneSnapshotColumn(value schema.SnapshotColumn) schema.SnapshotColumn {
	result := value
	result.DeclaredType = cloneSnapshotDeclaredType(value.DeclaredType)
	if value.Default != nil {
		defaultValue := *value.Default
		result.Default = &defaultValue
	}
	return result
}

func cloneSnapshotDeclaredType(
	value *schema.SnapshotDeclaredType,
) *schema.SnapshotDeclaredType {
	if value == nil {
		return nil
	}
	result := *value
	result.Arguments = append([]int(nil), value.Arguments...)
	return &result
}

func indexSchemaContractTables(
	snapshot schema.SchemaSnapshot,
) map[schemaContractTableKey]schema.SnapshotTable {
	result := make(
		map[schemaContractTableKey]schema.SnapshotTable,
		len(snapshot.Tables),
	)
	for _, table := range snapshot.Tables {
		result[schemaContractKey(table)] = table
	}
	return result
}

func schemaContractKey(table schema.SnapshotTable) schemaContractTableKey {
	return schemaContractTableKey{schema: table.Schema, table: table.Name}
}

func findSchemaContractTable(
	snapshot schema.SchemaSnapshot,
	key schemaContractTableKey,
) (schema.SnapshotTable, bool) {
	for _, table := range snapshot.Tables {
		if schemaContractKey(table) == key {
			return table, true
		}
	}
	return schema.SnapshotTable{}, false
}

func setSchemaContractTable(
	snapshot *schema.SchemaSnapshot,
	table schema.SnapshotTable,
) {
	key := schemaContractKey(table)
	for index := range snapshot.Tables {
		if schemaContractKey(snapshot.Tables[index]) == key {
			snapshot.Tables[index] = cloneSchemaSnapshot(
				schema.SchemaSnapshot{
					Version: schema.SchemaSnapshotVersion,
					Tables:  []schema.SnapshotTable{table},
				},
			).Tables[0]
			return
		}
	}
	snapshot.Tables = append(snapshot.Tables, cloneSchemaSnapshot(
		schema.SchemaSnapshot{
			Version: schema.SchemaSnapshotVersion,
			Tables:  []schema.SnapshotTable{table},
		},
	).Tables[0])
}

func decodeSchemaContractEvidence(
	raw json.RawMessage,
	target any,
) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing schema contract evidence")
		}
		return err
	}
	return nil
}

func addSchemaContractColumn(
	target map[schemaContractTableKey]map[string]struct{},
	key schemaContractTableKey,
	column string,
) {
	if target[key] == nil {
		target[key] = make(map[string]struct{})
	}
	target[key][column] = struct{}{}
}

func schemaContractStringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

func equalSchemaContractStrings(left, right []string) bool {
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
