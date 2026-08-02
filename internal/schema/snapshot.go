package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode/utf8"
)

const SchemaSnapshotVersion = 1

// SchemaSnapshot is the durable, engine-neutral schema evidence used for
// Stage 4 drift decisions. Dynamic generator frontiers are deliberately
// excluded: allocating another identity value is data movement, not schema
// drift.
type SchemaSnapshot struct {
	Version int             `json:"version"`
	Tables  []SnapshotTable `json:"tables"`
}

type SnapshotTable struct {
	Schema             string                    `json:"schema"`
	Name               string                    `json:"name"`
	MySQLCollation     string                    `json:"mysql_collation"`
	ClickHouseOrderBy  []string                  `json:"clickhouse_order_by"`
	Identity           *SnapshotIdentity         `json:"identity"`
	Columns            []SnapshotColumn          `json:"columns"`
	Indexes            []SnapshotIndex           `json:"indexes"`
	ForeignKeys        []SnapshotForeignKey      `json:"foreign_keys"`
	Checks             []SnapshotCheckConstraint `json:"checks"`
	SQLiteWithoutRowID bool                      `json:"sqlite_without_rowid"`
	SQLiteStrict       bool                      `json:"sqlite_strict"`
}

type SnapshotIdentity struct {
	Column     string             `json:"column"`
	Generation IdentityGeneration `json:"generation"`
}

type SnapshotColumn struct {
	Name               string                `json:"name"`
	Type               string                `json:"type"`
	Nullable           bool                  `json:"nullable"`
	PrimaryKey         bool                  `json:"primary_key"`
	PrimaryKeyPosition int                   `json:"primary_key_position"`
	DeclaredType       *SnapshotDeclaredType `json:"declared_type"`
	Default            *string               `json:"default"`
}

type SnapshotDeclaredType struct {
	Base                      string                       `json:"base"`
	Arguments                 []int                        `json:"arguments"`
	Length                    *int64                       `json:"length,omitempty"`
	Precision                 *int64                       `json:"precision,omitempty"`
	Scale                     *int64                       `json:"scale,omitempty"`
	FractionalSecondPrecision *int64                       `json:"fractional_second_precision,omitempty"`
	Spatial                   *SnapshotSpatialTypeMetadata `json:"spatial,omitempty"`
	MySQL                     *SnapshotMySQLTypeMetadata   `json:"mysql,omitempty"`
}

type SnapshotCatalogType = SnapshotDeclaredType

type SnapshotSpatialTypeMetadata struct {
	Subtype SpatialSubtype `json:"subtype"`
	SRID    *uint32        `json:"srid,omitempty"`
}

type SnapshotMySQLTypeMetadata struct {
	Unsigned    bool     `json:"unsigned,omitempty"`
	Zerofill    bool     `json:"zerofill,omitempty"`
	TinyIntOne  bool     `json:"tinyint_one,omitempty"`
	BitWidth    *int64   `json:"bit_width,omitempty"`
	EnumMembers []string `json:"enum_members,omitempty"`
	SetMembers  []string `json:"set_members,omitempty"`
}

type SnapshotIndexColumn struct {
	Name       string `json:"name"`
	Descending bool   `json:"descending"`
	Collation  string `json:"collation"`
}

type SnapshotIndex struct {
	Name    string                `json:"name"`
	Unique  bool                  `json:"unique"`
	Inline  bool                  `json:"inline"`
	Columns []SnapshotIndexColumn `json:"columns"`
}

type SnapshotForeignKey struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns"`
	ReferencedSchema  string   `json:"referenced_schema,omitempty"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns"`
	OnUpdate          string   `json:"on_update"`
	OnDelete          string   `json:"on_delete"`
	Match             string   `json:"match"`
}

type SnapshotCheckConstraint struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// NewSchemaSnapshot converts discovered metadata into stable durable
// evidence. Table and side-object discovery order does not affect the
// encoding; source column order and ordered object members remain significant.
func NewSchemaSnapshot(tables []Table) (SchemaSnapshot, error) {
	snapshot := SchemaSnapshot{
		Version: SchemaSnapshotVersion,
		Tables:  make([]SnapshotTable, len(tables)),
	}
	for tableIndex, table := range tables {
		converted := SnapshotTable{
			Schema:             table.Schema,
			Name:               table.Name,
			MySQLCollation:     table.MySQLCollation,
			ClickHouseOrderBy:  cloneStrings(table.ClickHouseOrderBy),
			Columns:            make([]SnapshotColumn, len(table.Columns)),
			Indexes:            make([]SnapshotIndex, len(table.Indexes)),
			ForeignKeys:        make([]SnapshotForeignKey, len(table.ForeignKeys)),
			Checks:             make([]SnapshotCheckConstraint, len(table.Checks)),
			SQLiteWithoutRowID: table.SQLiteWithoutRowID,
			SQLiteStrict:       table.SQLiteStrict,
		}
		if table.Identity != nil {
			converted.Identity = &SnapshotIdentity{
				Column:     table.Identity.Column,
				Generation: table.Identity.Generation,
			}
		}
		for columnIndex, column := range table.Columns {
			convertedColumn := SnapshotColumn{
				Name:               column.Name,
				Type:               column.Type,
				Nullable:           column.Nullable,
				PrimaryKey:         column.PrimaryKey,
				PrimaryKeyPosition: column.PrimaryKeyPosition,
			}
			if column.DeclaredType != nil {
				convertedColumn.DeclaredType =
					snapshotDeclaredTypeFromCatalog(*column.DeclaredType)
			}
			if column.Default != nil {
				value := column.Default.CanonicalSQL()
				convertedColumn.Default = &value
			}
			converted.Columns[columnIndex] = convertedColumn
		}
		for indexPosition, index := range table.Indexes {
			convertedIndex := SnapshotIndex{
				Name:    index.Name,
				Unique:  index.Unique,
				Inline:  index.Inline,
				Columns: make([]SnapshotIndexColumn, len(index.Columns)),
			}
			for columnPosition, column := range index.Columns {
				convertedIndex.Columns[columnPosition] = SnapshotIndexColumn{
					Name:       column.Name,
					Descending: column.Descending,
					Collation:  column.Collation,
				}
			}
			converted.Indexes[indexPosition] = convertedIndex
		}
		for foreignKeyPosition, foreignKey := range table.ForeignKeys {
			converted.ForeignKeys[foreignKeyPosition] = SnapshotForeignKey{
				Name:              foreignKey.Name,
				Columns:           cloneStrings(foreignKey.Columns),
				ReferencedSchema:  foreignKey.ReferencedSchema,
				ReferencedTable:   foreignKey.ReferencedTable,
				ReferencedColumns: cloneStrings(foreignKey.ReferencedColumns),
				OnUpdate:          foreignKey.OnUpdate,
				OnDelete:          foreignKey.OnDelete,
				Match:             foreignKey.Match,
			}
		}
		for checkPosition, check := range table.Checks {
			converted.Checks[checkPosition] = SnapshotCheckConstraint{
				Name:       check.Name,
				Expression: check.Expression.CanonicalSQL(),
			}
		}
		snapshot.Tables[tableIndex] = converted
	}
	return snapshot.normalized()
}

// ParseSchemaSnapshot rejects unknown fields, trailing documents, unsupported
// versions, and ambiguous duplicate identities instead of silently accepting
// state that a newer or older binary may interpret differently.
func ParseSchemaSnapshot(data []byte) (SchemaSnapshot, error) {
	if !utf8.Valid(data) {
		return SchemaSnapshot{}, fmt.Errorf(
			"decode schema snapshot: invalid UTF-8",
		)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var snapshot SchemaSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return SchemaSnapshot{}, fmt.Errorf("decode schema snapshot: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return SchemaSnapshot{}, fmt.Errorf("decode schema snapshot: trailing JSON document")
		}
		return SchemaSnapshot{}, fmt.Errorf("decode schema snapshot: %w", err)
	}
	return snapshot.normalized()
}

// CanonicalJSON returns the unique durable representation of a snapshot.
func (snapshot SchemaSnapshot) CanonicalJSON() ([]byte, error) {
	normalized, err := snapshot.normalized()
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("encode schema snapshot: %w", err)
	}
	return encoded, nil
}

func (snapshot SchemaSnapshot) Digest() (string, error) {
	encoded, err := snapshot.CanonicalJSON()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func SchemaSnapshotsEqual(left, right SchemaSnapshot) (bool, error) {
	leftJSON, err := left.CanonicalJSON()
	if err != nil {
		return false, err
	}
	rightJSON, err := right.CanonicalJSON()
	if err != nil {
		return false, err
	}
	return bytes.Equal(leftJSON, rightJSON), nil
}

func (snapshot SchemaSnapshot) normalized() (SchemaSnapshot, error) {
	if snapshot.Version != SchemaSnapshotVersion {
		return SchemaSnapshot{}, fmt.Errorf(
			"unsupported schema snapshot version %d",
			snapshot.Version,
		)
	}
	normalized := SchemaSnapshot{
		Version: snapshot.Version,
		Tables:  make([]SnapshotTable, len(snapshot.Tables)),
	}
	for index, table := range snapshot.Tables {
		normalizedTable, err := normalizeSnapshotTable(table)
		if err != nil {
			return SchemaSnapshot{}, err
		}
		normalized.Tables[index] = normalizedTable
	}
	sort.Slice(normalized.Tables, func(left, right int) bool {
		return snapshotTableKey(normalized.Tables[left]) <
			snapshotTableKey(normalized.Tables[right])
	})
	for index := 1; index < len(normalized.Tables); index++ {
		if snapshotTableKey(normalized.Tables[index-1]) ==
			snapshotTableKey(normalized.Tables[index]) {
			return SchemaSnapshot{}, fmt.Errorf(
				"schema snapshot contains duplicate table %s",
				snapshotQualifiedName(normalized.Tables[index]),
			)
		}
	}
	return normalized, nil
}

func normalizeSnapshotTable(table SnapshotTable) (SnapshotTable, error) {
	if table.Name == "" {
		return SnapshotTable{}, fmt.Errorf("schema snapshot contains an empty table name")
	}
	normalized := SnapshotTable{
		Schema:             table.Schema,
		Name:               table.Name,
		MySQLCollation:     table.MySQLCollation,
		ClickHouseOrderBy:  cloneStrings(table.ClickHouseOrderBy),
		Columns:            make([]SnapshotColumn, len(table.Columns)),
		Indexes:            cloneSnapshotIndexes(table.Indexes),
		ForeignKeys:        cloneSnapshotForeignKeys(table.ForeignKeys),
		Checks:             append(make([]SnapshotCheckConstraint, 0, len(table.Checks)), table.Checks...),
		SQLiteWithoutRowID: table.SQLiteWithoutRowID,
		SQLiteStrict:       table.SQLiteStrict,
	}
	if table.Identity != nil {
		normalized.Identity = &SnapshotIdentity{
			Column:     table.Identity.Column,
			Generation: table.Identity.Generation,
		}
		if normalized.Identity.Column == "" {
			return SnapshotTable{}, fmt.Errorf(
				"schema snapshot table %s has an identity with no column",
				snapshotQualifiedName(table),
			)
		}
		if normalized.Identity.Generation != IdentityByDefault {
			return SnapshotTable{}, fmt.Errorf(
				"schema snapshot table %s has unsupported identity generation %q",
				snapshotQualifiedName(table),
				normalized.Identity.Generation,
			)
		}
	}
	columns := make(map[string]struct{}, len(table.Columns))
	for index, column := range table.Columns {
		if column.Name == "" {
			return SnapshotTable{}, fmt.Errorf(
				"schema snapshot table %s contains an empty column name",
				snapshotQualifiedName(table),
			)
		}
		if _, exists := columns[column.Name]; exists {
			return SnapshotTable{}, fmt.Errorf(
				"schema snapshot table %s contains duplicate column %s",
				snapshotQualifiedName(table),
				column.Name,
			)
		}
		columns[column.Name] = struct{}{}
		normalizedColumn := column
		if column.DeclaredType != nil {
			if err := validateSnapshotCatalogBoundary(
				*column.DeclaredType,
			); err != nil {
				return SnapshotTable{}, fmt.Errorf(
					"schema snapshot table %s column %s: %w",
					snapshotQualifiedName(table),
					column.Name,
					err,
				)
			}
			normalizedColumn.DeclaredType =
				cloneSnapshotDeclaredType(column.DeclaredType)
		}
		if column.Default != nil {
			value := *column.Default
			normalizedColumn.Default = &value
		}
		normalized.Columns[index] = normalizedColumn
	}
	if normalized.Identity != nil {
		if _, exists := columns[normalized.Identity.Column]; !exists {
			return SnapshotTable{}, fmt.Errorf(
				"schema snapshot table %s identity references unknown column %s",
				snapshotQualifiedName(table),
				normalized.Identity.Column,
			)
		}
	}
	sort.Slice(normalized.Indexes, func(left, right int) bool {
		return snapshotObjectKey(normalized.Indexes[left]) <
			snapshotObjectKey(normalized.Indexes[right])
	})
	for _, foreignKey := range normalized.ForeignKeys {
		if !utf8.ValidString(foreignKey.ReferencedSchema) ||
			strings.ContainsRune(
				foreignKey.ReferencedSchema,
				'\x00',
			) {
			return SnapshotTable{}, fmt.Errorf(
				"schema snapshot table %s foreign key %s has an invalid referenced schema",
				snapshotQualifiedName(table),
				foreignKey.Name,
			)
		}
	}
	sort.Slice(normalized.ForeignKeys, func(left, right int) bool {
		return snapshotObjectKey(normalized.ForeignKeys[left]) <
			snapshotObjectKey(normalized.ForeignKeys[right])
	})
	sort.Slice(normalized.Checks, func(left, right int) bool {
		return snapshotObjectKey(normalized.Checks[left]) <
			snapshotObjectKey(normalized.Checks[right])
	})
	if err := rejectDuplicateSnapshotObjects(
		snapshotQualifiedName(table),
		"index",
		normalized.Indexes,
	); err != nil {
		return SnapshotTable{}, err
	}
	if err := rejectDuplicateSnapshotObjects(
		snapshotQualifiedName(table),
		"foreign key",
		normalized.ForeignKeys,
	); err != nil {
		return SnapshotTable{}, err
	}
	if err := rejectDuplicateSnapshotObjects(
		snapshotQualifiedName(table),
		"check",
		normalized.Checks,
	); err != nil {
		return SnapshotTable{}, err
	}
	return normalized, nil
}

func cloneSnapshotIndexes(indexes []SnapshotIndex) []SnapshotIndex {
	result := make([]SnapshotIndex, len(indexes))
	for index, value := range indexes {
		result[index] = SnapshotIndex{
			Name:    value.Name,
			Unique:  value.Unique,
			Inline:  value.Inline,
			Columns: append(make([]SnapshotIndexColumn, 0, len(value.Columns)), value.Columns...),
		}
	}
	return result
}

func cloneSnapshotForeignKeys(foreignKeys []SnapshotForeignKey) []SnapshotForeignKey {
	result := make([]SnapshotForeignKey, len(foreignKeys))
	for index, value := range foreignKeys {
		result[index] = SnapshotForeignKey{
			Name:              value.Name,
			Columns:           cloneStrings(value.Columns),
			ReferencedSchema:  value.ReferencedSchema,
			ReferencedTable:   value.ReferencedTable,
			ReferencedColumns: cloneStrings(value.ReferencedColumns),
			OnUpdate:          value.OnUpdate,
			OnDelete:          value.OnDelete,
			Match:             value.Match,
		}
	}
	return result
}

func validateSnapshotCatalogBoundary(value SnapshotDeclaredType) error {
	catalog := snapshotDeclaredTypeToCatalog(value)
	if strings.TrimSpace(catalog.Base) == "" {
		return fmt.Errorf("has an empty declared type")
	}
	if len(catalog.Arguments) > 2 {
		return fmt.Errorf(
			"declared type %q has too many modifiers: %v",
			catalog.Base,
			catalog.Arguments,
		)
	}
	base := canonicalCatalogBase(catalog.Base)
	for index, argument := range catalog.Arguments {
		postgresNumericScale := (base == "numeric" ||
			base == "decimal") &&
			index == 1
		if argument < 0 && !postgresNumericScale {
			return fmt.Errorf(
				"declared type %q has a negative modifier: %d",
				catalog.Base,
				argument,
			)
		}
	}
	return ValidateCatalogType(catalog)
}

func snapshotDeclaredTypeFromCatalog(
	value DeclaredType,
) *SnapshotDeclaredType {
	result := &SnapshotDeclaredType{
		Base:                      value.Base,
		Arguments:                 cloneInts(value.Arguments),
		Length:                    cloneInt64Pointer(value.Length),
		Precision:                 cloneInt64Pointer(value.Precision),
		Scale:                     cloneInt64Pointer(value.Scale),
		FractionalSecondPrecision: cloneInt64Pointer(value.FractionalSecondPrecision),
	}
	if value.Spatial != nil {
		result.Spatial = &SnapshotSpatialTypeMetadata{
			Subtype: value.Spatial.Subtype,
			SRID:    cloneUint32Pointer(value.Spatial.SRID),
		}
	}
	if value.MySQL != nil {
		result.MySQL = &SnapshotMySQLTypeMetadata{
			Unsigned:    value.MySQL.Unsigned,
			Zerofill:    value.MySQL.Zerofill,
			TinyIntOne:  value.MySQL.TinyIntOne,
			BitWidth:    cloneInt64Pointer(value.MySQL.BitWidth),
			EnumMembers: cloneOptionalStrings(value.MySQL.EnumMembers),
			SetMembers:  cloneOptionalStrings(value.MySQL.SetMembers),
		}
	}
	return result
}

func snapshotDeclaredTypeToCatalog(
	value SnapshotDeclaredType,
) DeclaredType {
	result := DeclaredType{
		Base:                      value.Base,
		Arguments:                 cloneInts(value.Arguments),
		Length:                    cloneInt64Pointer(value.Length),
		Precision:                 cloneInt64Pointer(value.Precision),
		Scale:                     cloneInt64Pointer(value.Scale),
		FractionalSecondPrecision: cloneInt64Pointer(value.FractionalSecondPrecision),
	}
	if value.Spatial != nil {
		result.Spatial = &SpatialTypeMetadata{
			Subtype: value.Spatial.Subtype,
			SRID:    cloneUint32Pointer(value.Spatial.SRID),
		}
	}
	if value.MySQL != nil {
		result.MySQL = &MySQLTypeMetadata{
			Unsigned:    value.MySQL.Unsigned,
			Zerofill:    value.MySQL.Zerofill,
			TinyIntOne:  value.MySQL.TinyIntOne,
			BitWidth:    cloneInt64Pointer(value.MySQL.BitWidth),
			EnumMembers: cloneOptionalStrings(value.MySQL.EnumMembers),
			SetMembers:  cloneOptionalStrings(value.MySQL.SetMembers),
		}
	}
	return result
}

func cloneSnapshotDeclaredType(
	value *SnapshotDeclaredType,
) *SnapshotDeclaredType {
	if value == nil {
		return nil
	}
	return snapshotDeclaredTypeFromCatalog(
		snapshotDeclaredTypeToCatalog(*value),
	)
}

func rejectDuplicateSnapshotObjects[T any](
	table string,
	kind string,
	values []T,
) error {
	for index := 1; index < len(values); index++ {
		if snapshotObjectKey(values[index-1]) == snapshotObjectKey(values[index]) {
			return fmt.Errorf(
				"schema snapshot table %s contains duplicate %s metadata",
				table,
				kind,
			)
		}
	}
	return nil
}

func snapshotObjectKey(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(fmt.Sprintf("encode schema snapshot object key: %v", err))
	}
	return string(encoded)
}

func snapshotTableKey(table SnapshotTable) string {
	return table.Schema + "\x00" + table.Name
}

func snapshotQualifiedName(table SnapshotTable) string {
	if table.Schema == "" {
		return table.Name
	}
	return table.Schema + "." + table.Name
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	return append([]string(nil), values...)
}

func cloneInts(values []int) []int {
	if len(values) == 0 {
		return []int{}
	}
	return append([]int(nil), values...)
}

func cloneOptionalStrings(values []string) []string {
	if values == nil {
		return nil
	}
	return append([]string{}, values...)
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneUint32Pointer(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
