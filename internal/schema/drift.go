package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
)

// SchemaContractEntity selects the schema-contract mode that will eventually
// decide a drift fact. Comparison remains policy-free: it never mutates a plan
// or silently classifies an unsafe change as evolvable.
type SchemaContractEntity string

const (
	SchemaContractTables   SchemaContractEntity = "tables"
	SchemaContractColumns  SchemaContractEntity = "columns"
	SchemaContractDataType SchemaContractEntity = "data_type"
)

// SchemaDriftChangeKind is deliberately structural rather than policy-laden.
// A later decision layer can combine it with target mode and contract mode.
type SchemaDriftChangeKind string

const (
	SchemaDriftTableAdded         SchemaDriftChangeKind = "table_added"
	SchemaDriftTableDropped       SchemaDriftChangeKind = "table_dropped"
	SchemaDriftColumnAdded        SchemaDriftChangeKind = "column_added"
	SchemaDriftColumnDropped      SchemaDriftChangeKind = "column_dropped"
	SchemaDriftColumnOrderChanged SchemaDriftChangeKind = "column_order_changed"
	SchemaDriftDataTypeChanged    SchemaDriftChangeKind = "data_type_changed"
	SchemaDriftDefaultChanged     SchemaDriftChangeKind = "default_changed"
	SchemaDriftNullabilityChanged SchemaDriftChangeKind = "nullability_changed"
	SchemaDriftPrimaryKeyChanged  SchemaDriftChangeKind = "primary_key_changed"
	SchemaDriftIdentityChanged    SchemaDriftChangeKind = "identity_changed"
	SchemaDriftIndexAdded         SchemaDriftChangeKind = "index_added"
	SchemaDriftIndexDropped       SchemaDriftChangeKind = "index_dropped"
	SchemaDriftIndexChanged       SchemaDriftChangeKind = "index_changed"
	SchemaDriftForeignKeyAdded    SchemaDriftChangeKind = "foreign_key_added"
	SchemaDriftForeignKeyDropped  SchemaDriftChangeKind = "foreign_key_dropped"
	SchemaDriftForeignKeyChanged  SchemaDriftChangeKind = "foreign_key_changed"
	SchemaDriftCheckAdded         SchemaDriftChangeKind = "check_added"
	SchemaDriftCheckDropped       SchemaDriftChangeKind = "check_dropped"
	SchemaDriftCheckChanged       SchemaDriftChangeKind = "check_changed"
	SchemaDriftTableOptionChanged SchemaDriftChangeKind = "table_option_changed"
)

type SchemaDriftObjectKind string

const (
	SchemaDriftObjectTable       SchemaDriftObjectKind = "table"
	SchemaDriftObjectColumn      SchemaDriftObjectKind = "column"
	SchemaDriftObjectColumnOrder SchemaDriftObjectKind = "column_order"
	SchemaDriftObjectDataType    SchemaDriftObjectKind = "data_type"
	SchemaDriftObjectDefault     SchemaDriftObjectKind = "default"
	SchemaDriftObjectNullability SchemaDriftObjectKind = "nullability"
	SchemaDriftObjectPrimaryKey  SchemaDriftObjectKind = "primary_key"
	SchemaDriftObjectIdentity    SchemaDriftObjectKind = "identity"
	SchemaDriftObjectIndex       SchemaDriftObjectKind = "index"
	SchemaDriftObjectForeignKey  SchemaDriftObjectKind = "foreign_key"
	SchemaDriftObjectCheck       SchemaDriftObjectKind = "check"
	SchemaDriftObjectTableOption SchemaDriftObjectKind = "table_option"
)

// SchemaDriftObject is a delimiter-free structured identity. Name identifies
// a side object or table option; Column identifies a source column.
type SchemaDriftObject struct {
	Kind   SchemaDriftObjectKind `json:"kind"`
	Schema string                `json:"schema,omitempty"`
	Table  string                `json:"table"`
	Column string                `json:"column,omitempty"`
	Name   string                `json:"name,omitempty"`
}

// SchemaDriftFact is complete audit-ready comparison evidence. Previous and
// Current are canonical JSON for the smallest complete structural object that
// explains the change. An absent side is encoded as JSON null.
type SchemaDriftFact struct {
	Entity     SchemaContractEntity  `json:"entity"`
	ChangeKind SchemaDriftChangeKind `json:"change_kind"`
	Object     SchemaDriftObject     `json:"object"`
	Previous   json.RawMessage       `json:"previous"`
	Current    json.RawMessage       `json:"current"`
}

// UncorrelatableUnnamedSchemaObjectError reports side-object definitions that
// changed without a stable catalog name. When unmatched unnamed definitions
// remain on both sides, comparison cannot safely distinguish a mutation from
// independent removal and addition.
type UncorrelatableUnnamedSchemaObjectError struct {
	ObjectKind        SchemaDriftObjectKind
	Schema            string
	Table             string
	PreviousUnmatched int
	CurrentUnmatched  int
}

func (err *UncorrelatableUnnamedSchemaObjectError) Error() string {
	table := err.Table
	if err.Schema != "" {
		table = err.Schema + "." + err.Table
	}
	return fmt.Sprintf(
		"schema drift: table %s has uncorrelatable unnamed %s metadata "+
			"(%d previous unmatched, %d current unmatched)",
		table,
		err.ObjectKind,
		err.PreviousUnmatched,
		err.CurrentUnmatched,
	)
}

// CompareSchemaSnapshotJSON strictly parses both durable snapshots before
// comparing them. Unknown fields, trailing documents, unsupported versions,
// duplicate identities, and ambiguous object identities fail closed.
func CompareSchemaSnapshotJSON(previous, current []byte) ([]SchemaDriftFact, error) {
	if err := rejectAmbiguousSnapshotJSON(previous); err != nil {
		return nil, fmt.Errorf("previous schema snapshot: %w", err)
	}
	if err := rejectAmbiguousSnapshotJSON(current); err != nil {
		return nil, fmt.Errorf("current schema snapshot: %w", err)
	}
	previousSnapshot, err := ParseSchemaSnapshot(previous)
	if err != nil {
		return nil, fmt.Errorf("previous schema snapshot: %w", err)
	}
	currentSnapshot, err := ParseSchemaSnapshot(current)
	if err != nil {
		return nil, fmt.Errorf("current schema snapshot: %w", err)
	}
	return CompareSchemaSnapshots(previousSnapshot, currentSnapshot)
}

func rejectAmbiguousSnapshotJSON(encoded []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := inspectSnapshotJSONValue(decoder); err != nil {
		return fmt.Errorf("decode schema snapshot object identity: %w", err)
	}
	if _, err := decoder.Token(); err != nil && err != io.EOF {
		return fmt.Errorf("decode schema snapshot trailing JSON: %w", err)
	}
	return nil
}

func inspectSnapshotJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delimiter {
	case '{':
		fields := make(map[string]string)
		for decoder.More() {
			fieldToken, err := decoder.Token()
			if err != nil {
				return err
			}
			field, ok := fieldToken.(string)
			if !ok {
				return fmt.Errorf("object field name is %T", fieldToken)
			}
			canonicalField := strings.ToLower(field)
			if previous, duplicate := fields[canonicalField]; duplicate {
				return fmt.Errorf("duplicate JSON field %q conflicts with %q", field, previous)
			}
			if field != canonicalField {
				return fmt.Errorf("non-canonical JSON field %q", field)
			}
			fields[canonicalField] = field
			if err := inspectSnapshotJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim('}') {
			return fmt.Errorf("object closed by %v", closing)
		}
	case '[':
		for decoder.More() {
			if err := inspectSnapshotJSONValue(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil {
			return err
		}
		if closing != json.Delim(']') {
			return fmt.Errorf("array closed by %v", closing)
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
	return nil
}

// CompareSchemaSnapshots returns every structural change in stable order.
// Discovery order for tables and side objects is immaterial; source column
// order and ordered object members remain semantically significant.
func CompareSchemaSnapshots(previous, current SchemaSnapshot) ([]SchemaDriftFact, error) {
	previous, err := previous.normalized()
	if err != nil {
		return nil, fmt.Errorf("previous schema snapshot: %w", err)
	}
	current, err = current.normalized()
	if err != nil {
		return nil, fmt.Errorf("current schema snapshot: %w", err)
	}
	if err := validateSnapshotForDrift(previous); err != nil {
		return nil, fmt.Errorf("previous schema snapshot: %w", err)
	}
	if err := validateSnapshotForDrift(current); err != nil {
		return nil, fmt.Errorf("current schema snapshot: %w", err)
	}

	previousTables := make(map[string]SnapshotTable, len(previous.Tables))
	currentTables := make(map[string]SnapshotTable, len(current.Tables))
	for _, table := range previous.Tables {
		previousTables[snapshotTableKey(table)] = table
	}
	for _, table := range current.Tables {
		currentTables[snapshotTableKey(table)] = table
	}

	facts := make([]SchemaDriftFact, 0)
	tableKeys := unionKeys(previousTables, currentTables)
	for _, key := range tableKeys {
		previousTable, hadPrevious := previousTables[key]
		currentTable, hasCurrent := currentTables[key]
		switch {
		case !hadPrevious:
			facts = append(facts, newSchemaDriftFact(
				SchemaContractTables,
				SchemaDriftTableAdded,
				tableDriftObject(currentTable),
				nil,
				currentTable,
			))
		case !hasCurrent:
			facts = append(facts, newSchemaDriftFact(
				SchemaContractTables,
				SchemaDriftTableDropped,
				tableDriftObject(previousTable),
				previousTable,
				nil,
			))
		default:
			tableFacts, err := compareSnapshotTable(previousTable, currentTable)
			if err != nil {
				return nil, err
			}
			facts = append(facts, tableFacts...)
		}
	}
	sortSchemaDriftFacts(facts)
	if facts == nil {
		return []SchemaDriftFact{}, nil
	}
	return facts, nil
}

func compareSnapshotTable(previous, current SnapshotTable) ([]SchemaDriftFact, error) {
	facts := make([]SchemaDriftFact, 0)
	previousColumns := make(map[string]SnapshotColumn, len(previous.Columns))
	currentColumns := make(map[string]SnapshotColumn, len(current.Columns))
	for _, column := range previous.Columns {
		previousColumns[column.Name] = column
	}
	for _, column := range current.Columns {
		currentColumns[column.Name] = column
	}

	for _, name := range unionKeys(previousColumns, currentColumns) {
		previousColumn, hadPrevious := previousColumns[name]
		currentColumn, hasCurrent := currentColumns[name]
		object := columnDriftObject(current, name, SchemaDriftObjectColumn)
		switch {
		case !hadPrevious:
			facts = append(facts, newSchemaDriftFact(
				SchemaContractColumns,
				SchemaDriftColumnAdded,
				object,
				nil,
				currentColumn,
			))
		case !hasCurrent:
			object = columnDriftObject(previous, name, SchemaDriftObjectColumn)
			facts = append(facts, newSchemaDriftFact(
				SchemaContractColumns,
				SchemaDriftColumnDropped,
				object,
				previousColumn,
				nil,
			))
		default:
			if previousColumn.Type != currentColumn.Type ||
				!reflect.DeepEqual(
					previousColumn.DeclaredType,
					currentColumn.DeclaredType,
				) {
				facts = append(facts, newSchemaDriftFact(
					SchemaContractDataType,
					SchemaDriftDataTypeChanged,
					columnDriftObject(current, name, SchemaDriftObjectDataType),
					previousColumn,
					currentColumn,
				))
			}
			if !equalStringPointers(previousColumn.Default, currentColumn.Default) {
				facts = append(facts, newSchemaDriftFact(
					SchemaContractColumns,
					SchemaDriftDefaultChanged,
					columnDriftObject(current, name, SchemaDriftObjectDefault),
					previousColumn,
					currentColumn,
				))
			}
			if previousColumn.Nullable != currentColumn.Nullable {
				facts = append(facts, newSchemaDriftFact(
					SchemaContractColumns,
					SchemaDriftNullabilityChanged,
					columnDriftObject(current, name, SchemaDriftObjectNullability),
					previousColumn,
					currentColumn,
				))
			}
		}
	}

	if previousOrder, currentOrder, changed := snapshotColumnOrder(previous.Columns, current.Columns); changed {
		facts = append(facts, newSchemaDriftFact(
			SchemaContractColumns,
			SchemaDriftColumnOrderChanged,
			SchemaDriftObject{
				Kind: SchemaDriftObjectColumnOrder, Schema: current.Schema, Table: current.Name,
			},
			previousOrder,
			currentOrder,
		))
	}

	previousPrimaryKey, err := snapshotPrimaryKey(previous)
	if err != nil {
		return nil, fmt.Errorf("previous table %s: %w", snapshotQualifiedName(previous), err)
	}
	currentPrimaryKey, err := snapshotPrimaryKey(current)
	if err != nil {
		return nil, fmt.Errorf("current table %s: %w", snapshotQualifiedName(current), err)
	}
	if !reflect.DeepEqual(previousPrimaryKey, currentPrimaryKey) {
		facts = append(facts, newSchemaDriftFact(
			SchemaContractTables,
			SchemaDriftPrimaryKeyChanged,
			SchemaDriftObject{
				Kind: SchemaDriftObjectPrimaryKey, Schema: current.Schema,
				Table: current.Name, Name: "primary_key",
			},
			previousPrimaryKey,
			currentPrimaryKey,
		))
	}
	if !reflect.DeepEqual(previous.Identity, current.Identity) {
		facts = append(facts, newSchemaDriftFact(
			SchemaContractTables,
			SchemaDriftIdentityChanged,
			SchemaDriftObject{
				Kind: SchemaDriftObjectIdentity, Schema: current.Schema,
				Table: current.Name, Name: "identity",
			},
			previous.Identity,
			current.Identity,
		))
	}

	indexFacts, err := compareSnapshotObjects(
		previous,
		current,
		SchemaDriftObjectIndex,
		SchemaDriftIndexAdded,
		SchemaDriftIndexDropped,
		SchemaDriftIndexChanged,
		previous.Indexes,
		current.Indexes,
	)
	if err != nil {
		return nil, err
	}
	facts = append(facts, indexFacts...)
	foreignKeyFacts, err := compareSnapshotObjects(
		previous,
		current,
		SchemaDriftObjectForeignKey,
		SchemaDriftForeignKeyAdded,
		SchemaDriftForeignKeyDropped,
		SchemaDriftForeignKeyChanged,
		previous.ForeignKeys,
		current.ForeignKeys,
	)
	if err != nil {
		return nil, err
	}
	facts = append(facts, foreignKeyFacts...)
	checkFacts, err := compareSnapshotObjects(
		previous,
		current,
		SchemaDriftObjectCheck,
		SchemaDriftCheckAdded,
		SchemaDriftCheckDropped,
		SchemaDriftCheckChanged,
		previous.Checks,
		current.Checks,
	)
	if err != nil {
		return nil, err
	}
	facts = append(facts, checkFacts...)

	facts = appendTableOptionFact(
		facts,
		current,
		"mysql_collation",
		previous.MySQLCollation,
		current.MySQLCollation,
	)
	facts = appendTableOptionFactSlice(
		facts,
		current,
		"clickhouse_order_by",
		previous.ClickHouseOrderBy,
		current.ClickHouseOrderBy,
	)
	facts = appendTableOptionFact(
		facts,
		current,
		"sqlite_without_rowid",
		previous.SQLiteWithoutRowID,
		current.SQLiteWithoutRowID,
	)
	facts = appendTableOptionFact(
		facts,
		current,
		"sqlite_strict",
		previous.SQLiteStrict,
		current.SQLiteStrict,
	)
	return facts, nil
}

type snapshotPrimaryKeyColumn struct {
	Name     string `json:"name"`
	Position int    `json:"position"`
}

func snapshotPrimaryKey(table SnapshotTable) ([]snapshotPrimaryKeyColumn, error) {
	primaryKey := make([]snapshotPrimaryKeyColumn, 0)
	positions := make(map[int]string)
	for _, column := range table.Columns {
		if !column.PrimaryKey {
			if column.PrimaryKeyPosition != 0 {
				return nil, fmt.Errorf(
					"non-primary-key column %s has position %d",
					column.Name,
					column.PrimaryKeyPosition,
				)
			}
			continue
		}
		if column.PrimaryKeyPosition <= 0 {
			return nil, fmt.Errorf("primary-key column %s has no positive position", column.Name)
		}
		if previous, duplicate := positions[column.PrimaryKeyPosition]; duplicate {
			return nil, fmt.Errorf(
				"primary-key position %d is shared by %s and %s",
				column.PrimaryKeyPosition,
				previous,
				column.Name,
			)
		}
		positions[column.PrimaryKeyPosition] = column.Name
		primaryKey = append(primaryKey, snapshotPrimaryKeyColumn{
			Name: column.Name, Position: column.PrimaryKeyPosition,
		})
	}
	sort.Slice(primaryKey, func(left, right int) bool {
		return primaryKey[left].Position < primaryKey[right].Position
	})
	for index, column := range primaryKey {
		if column.Position != index+1 {
			return nil, fmt.Errorf("primary-key positions are not contiguous from one")
		}
	}
	return primaryKey, nil
}

func snapshotColumnOrder(
	previous,
	current []SnapshotColumn,
) ([]string, []string, bool) {
	previousOrder := make([]string, len(previous))
	currentOrder := make([]string, len(current))
	for index, column := range previous {
		previousOrder[index] = column.Name
	}
	for index, column := range current {
		currentOrder[index] = column.Name
	}
	return previousOrder, currentOrder, !reflect.DeepEqual(previousOrder, currentOrder)
}

func compareSnapshotObjects[T any](
	previousTable SnapshotTable,
	currentTable SnapshotTable,
	objectKind SchemaDriftObjectKind,
	added SchemaDriftChangeKind,
	dropped SchemaDriftChangeKind,
	changed SchemaDriftChangeKind,
	previous []T,
	current []T,
) ([]SchemaDriftFact, error) {
	previousObjects, err := indexedSnapshotObjects(previous)
	if err != nil {
		return nil, fmt.Errorf(
			"previous table %s %s metadata: %w",
			snapshotQualifiedName(previousTable),
			objectKind,
			err,
		)
	}
	currentObjects, err := indexedSnapshotObjects(current)
	if err != nil {
		return nil, fmt.Errorf(
			"current table %s %s metadata: %w",
			snapshotQualifiedName(currentTable),
			objectKind,
			err,
		)
	}
	previousUnmatched, currentUnmatched := unmatchedUnnamedSnapshotObjects(
		previousObjects,
		currentObjects,
	)
	if previousUnmatched > 0 && currentUnmatched > 0 {
		return nil, &UncorrelatableUnnamedSchemaObjectError{
			ObjectKind:        objectKind,
			Schema:            currentTable.Schema,
			Table:             currentTable.Name,
			PreviousUnmatched: previousUnmatched,
			CurrentUnmatched:  currentUnmatched,
		}
	}
	facts := make([]SchemaDriftFact, 0)
	for _, key := range unionKeys(previousObjects, currentObjects) {
		previousObject, hadPrevious := previousObjects[key]
		currentObject, hasCurrent := currentObjects[key]
		name := snapshotObjectName(currentObject)
		if !hasCurrent {
			name = snapshotObjectName(previousObject)
		}
		object := SchemaDriftObject{
			Kind: objectKind, Schema: currentTable.Schema,
			Table: currentTable.Name, Name: name,
		}
		switch {
		case !hadPrevious:
			facts = append(facts, newSchemaDriftFact(
				SchemaContractTables, added, object, nil, currentObject,
			))
		case !hasCurrent:
			facts = append(facts, newSchemaDriftFact(
				SchemaContractTables, dropped, object, previousObject, nil,
			))
		case !reflect.DeepEqual(previousObject, currentObject):
			facts = append(facts, newSchemaDriftFact(
				SchemaContractTables, changed, object, previousObject, currentObject,
			))
		}
	}
	return facts, nil
}

func unmatchedUnnamedSnapshotObjects[T any](
	previous,
	current map[string]T,
) (int, int) {
	previousUnmatched := 0
	currentUnmatched := 0
	for key := range previous {
		if !strings.HasPrefix(key, "signature:") {
			continue
		}
		if _, exists := current[key]; !exists {
			previousUnmatched++
		}
	}
	for key := range current {
		if !strings.HasPrefix(key, "signature:") {
			continue
		}
		if _, exists := previous[key]; !exists {
			currentUnmatched++
		}
	}
	return previousUnmatched, currentUnmatched
}

func indexedSnapshotObjects[T any](objects []T) (map[string]T, error) {
	indexed := make(map[string]T, len(objects))
	for _, object := range objects {
		name := snapshotObjectName(object)
		key := ""
		if name != "" {
			key = "name:" + name
		} else {
			encoded, err := json.Marshal(object)
			if err != nil {
				return nil, fmt.Errorf("encode unnamed object identity: %w", err)
			}
			key = "signature:" + string(encoded)
		}
		if _, duplicate := indexed[key]; duplicate {
			if name == "" {
				return nil, fmt.Errorf("duplicate unnamed object")
			}
			return nil, fmt.Errorf("ambiguous duplicate object name %q", name)
		}
		indexed[key] = object
	}
	return indexed, nil
}

func snapshotObjectName(value any) string {
	switch object := value.(type) {
	case SnapshotIndex:
		return object.Name
	case SnapshotForeignKey:
		return object.Name
	case SnapshotCheckConstraint:
		return object.Name
	default:
		panic(fmt.Sprintf("unsupported schema snapshot object %T", value))
	}
}

func appendTableOptionFact[T comparable](
	facts []SchemaDriftFact,
	table SnapshotTable,
	name string,
	previous T,
	current T,
) []SchemaDriftFact {
	if previous == current {
		return facts
	}
	return append(facts, newSchemaDriftFact(
		SchemaContractTables,
		SchemaDriftTableOptionChanged,
		SchemaDriftObject{
			Kind: SchemaDriftObjectTableOption, Schema: table.Schema,
			Table: table.Name, Name: name,
		},
		previous,
		current,
	))
}

func appendTableOptionFactSlice(
	facts []SchemaDriftFact,
	table SnapshotTable,
	name string,
	previous []string,
	current []string,
) []SchemaDriftFact {
	if reflect.DeepEqual(previous, current) {
		return facts
	}
	return append(facts, newSchemaDriftFact(
		SchemaContractTables,
		SchemaDriftTableOptionChanged,
		SchemaDriftObject{
			Kind: SchemaDriftObjectTableOption, Schema: table.Schema,
			Table: table.Name, Name: name,
		},
		previous,
		current,
	))
}

func newSchemaDriftFact(
	entity SchemaContractEntity,
	changeKind SchemaDriftChangeKind,
	object SchemaDriftObject,
	previous any,
	current any,
) SchemaDriftFact {
	return SchemaDriftFact{
		Entity:     entity,
		ChangeKind: changeKind,
		Object:     object,
		Previous:   marshalSchemaDriftEvidence(previous),
		Current:    marshalSchemaDriftEvidence(current),
	}
}

func marshalSchemaDriftEvidence(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage("null")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("encode schema drift evidence: %v", err))
	}
	return encoded
}

func tableDriftObject(table SnapshotTable) SchemaDriftObject {
	return SchemaDriftObject{
		Kind: SchemaDriftObjectTable, Schema: table.Schema, Table: table.Name,
	}
}

func columnDriftObject(
	table SnapshotTable,
	column string,
	kind SchemaDriftObjectKind,
) SchemaDriftObject {
	return SchemaDriftObject{
		Kind: kind, Schema: table.Schema, Table: table.Name, Column: column,
	}
}

func equalStringPointers(left, right *string) bool {
	return left == nil && right == nil ||
		left != nil && right != nil && *left == *right
}

func unionKeys[T any](left, right map[string]T) []string {
	keys := make([]string, 0, len(left)+len(right))
	seen := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range right {
		if _, exists := seen[key]; exists {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortSchemaDriftFacts(facts []SchemaDriftFact) {
	sort.SliceStable(facts, func(left, right int) bool {
		leftKey := schemaDriftFactKey(facts[left])
		rightKey := schemaDriftFactKey(facts[right])
		return bytes.Compare(leftKey, rightKey) < 0
	})
}

func schemaDriftFactKey(fact SchemaDriftFact) []byte {
	encoded, err := json.Marshal(struct {
		Schema     string
		Table      string
		ObjectKind SchemaDriftObjectKind
		Column     string
		Name       string
		ChangeKind SchemaDriftChangeKind
		Previous   json.RawMessage
		Current    json.RawMessage
	}{
		Schema: fact.Object.Schema, Table: fact.Object.Table,
		ObjectKind: fact.Object.Kind, Column: fact.Object.Column,
		Name: fact.Object.Name, ChangeKind: fact.ChangeKind,
		Previous: fact.Previous, Current: fact.Current,
	})
	if err != nil {
		panic(fmt.Sprintf("encode schema drift sort key: %v", err))
	}
	return encoded
}

func validateSnapshotForDrift(snapshot SchemaSnapshot) error {
	for _, table := range snapshot.Tables {
		if len(table.Columns) == 0 {
			return fmt.Errorf("table %s has no columns", snapshotQualifiedName(table))
		}
		if table.MySQLCollation != "" &&
			strings.TrimSpace(table.MySQLCollation) == "" {
			return fmt.Errorf(
				"table %s has a blank MySQL collation",
				snapshotQualifiedName(table),
			)
		}
		columns := make(map[string]struct{}, len(table.Columns))
		for _, column := range table.Columns {
			if strings.TrimSpace(column.Type) == "" {
				return fmt.Errorf(
					"table %s column %s has no catalog type",
					snapshotQualifiedName(table),
					column.Name,
				)
			}
			if column.DeclaredType != nil {
				if err := validateSnapshotDeclaredType(*column.DeclaredType); err != nil {
					return fmt.Errorf(
						"table %s column %s: %w",
						snapshotQualifiedName(table),
						column.Name,
						err,
					)
				}
			}
			if column.Default != nil && strings.TrimSpace(*column.Default) == "" {
				return fmt.Errorf(
					"table %s column %s has a blank default",
					snapshotQualifiedName(table),
					column.Name,
				)
			}
			columns[column.Name] = struct{}{}
		}
		if _, err := snapshotPrimaryKey(table); err != nil {
			return fmt.Errorf("table %s: %w", snapshotQualifiedName(table), err)
		}
		if _, err := indexedSnapshotObjects(table.Indexes); err != nil {
			return fmt.Errorf("table %s index metadata: %w", snapshotQualifiedName(table), err)
		}
		for _, index := range table.Indexes {
			if len(index.Columns) == 0 {
				return fmt.Errorf(
					"table %s index %q has no columns",
					snapshotQualifiedName(table),
					index.Name,
				)
			}
			for _, column := range index.Columns {
				if _, exists := columns[column.Name]; !exists {
					return fmt.Errorf(
						"table %s index %q references unknown column %s",
						snapshotQualifiedName(table),
						index.Name,
						column.Name,
					)
				}
				if column.Collation != "" &&
					strings.TrimSpace(column.Collation) == "" {
					return fmt.Errorf(
						"table %s index %q column %s has a blank collation",
						snapshotQualifiedName(table),
						index.Name,
						column.Name,
					)
				}
			}
		}
		if _, err := indexedSnapshotObjects(table.ForeignKeys); err != nil {
			return fmt.Errorf("table %s foreign key metadata: %w", snapshotQualifiedName(table), err)
		}
		for _, foreignKey := range table.ForeignKeys {
			if len(foreignKey.Columns) == 0 ||
				len(foreignKey.Columns) != len(foreignKey.ReferencedColumns) ||
				foreignKey.ReferencedTable == "" {
				return fmt.Errorf(
					"table %s foreign key %q has an incomplete column mapping",
					snapshotQualifiedName(table),
					foreignKey.Name,
				)
			}
			for _, column := range foreignKey.Columns {
				if _, exists := columns[column]; !exists {
					return fmt.Errorf(
						"table %s foreign key %q references unknown local column %s",
						snapshotQualifiedName(table),
						foreignKey.Name,
						column,
					)
				}
			}
			for _, column := range foreignKey.ReferencedColumns {
				if column == "" {
					return fmt.Errorf(
						"table %s foreign key %q has an empty referenced column",
						snapshotQualifiedName(table),
						foreignKey.Name,
					)
				}
			}
			for _, action := range []struct {
				name  string
				value string
			}{
				{name: "ON UPDATE", value: foreignKey.OnUpdate},
				{name: "ON DELETE", value: foreignKey.OnDelete},
				{name: "MATCH", value: foreignKey.Match},
			} {
				if action.value != "" && strings.TrimSpace(action.value) == "" {
					return fmt.Errorf(
						"table %s foreign key %q has blank %s metadata",
						snapshotQualifiedName(table),
						foreignKey.Name,
						action.name,
					)
				}
			}
		}
		if _, err := indexedSnapshotObjects(table.Checks); err != nil {
			return fmt.Errorf("table %s check metadata: %w", snapshotQualifiedName(table), err)
		}
		for _, check := range table.Checks {
			if strings.TrimSpace(check.Expression) == "" {
				return fmt.Errorf(
					"table %s check %q has an empty expression",
					snapshotQualifiedName(table),
					check.Name,
				)
			}
		}
		for _, column := range table.ClickHouseOrderBy {
			if _, exists := columns[column]; !exists {
				return fmt.Errorf(
					"table %s ClickHouse order references unknown column %s",
					snapshotQualifiedName(table),
					column,
				)
			}
		}
	}
	return nil
}

func validateSnapshotDeclaredType(value SnapshotDeclaredType) error {
	if strings.TrimSpace(value.Base) == "" {
		return fmt.Errorf("has an empty declared type")
	}
	if len(value.Arguments) > 2 {
		return fmt.Errorf(
			"declared type %q has too many modifiers: %v",
			value.Base,
			value.Arguments,
		)
	}
	for _, argument := range value.Arguments {
		if argument < 0 {
			return fmt.Errorf(
				"declared type %q has a negative modifier: %d",
				value.Base,
				argument,
			)
		}
	}
	if err := ValidateCatalogType(
		snapshotDeclaredTypeToCatalog(value),
	); err != nil {
		return err
	}
	return nil
}
