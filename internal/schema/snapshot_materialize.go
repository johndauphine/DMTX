package schema

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaterializeSchemaSnapshot reconstructs the rich schema model from durable
// canonical snapshot evidence. The conversion is deliberately stricter than
// ordinary drift comparison: every expression is parsed, every object member
// and foreign-key relation is resolved, and the reconstructed model must
// reproduce the exact canonical snapshot before it is returned.
//
// Identity frontiers are nil because SchemaSnapshot intentionally excludes
// runtime generator state.
func MaterializeSchemaSnapshot(
	snapshot SchemaSnapshot,
) ([]Table, error) {
	return materializeSchemaSnapshot(snapshot, materializeSnapshotCheck)
}

// MaterializeSchemaSnapshotForDialect reconstructs a durable target snapshot
// using the target's already-authenticated expression semantics. SQLite CHECK
// expressions must not be reinterpreted as PostgreSQL expressions: SQLite's
// integer boolean-domain checks (for example, enabled IN (0, 1)) are exact
// target authority but need not be type-compatible PostgreSQL predicates.
// Other dialects retain the established portable/PostgreSQL proof until they
// provide an equally strict target-specific reparser.
func MaterializeSchemaSnapshotForDialect(
	snapshot SchemaSnapshot,
	dialect Dialect,
) ([]Table, error) {
	checkMaterializer := materializeSnapshotCheck
	if dialect == SQLite {
		checkMaterializer = materializeSnapshotSQLiteCheck
	}
	return materializeSchemaSnapshot(snapshot, checkMaterializer)
}

func materializeSchemaSnapshot(
	snapshot SchemaSnapshot,
	checkMaterializer func(string, []Column) (Expression, error),
) ([]Table, error) {
	normalized, err := snapshot.normalized()
	if err != nil {
		return nil, fmt.Errorf(
			"materialize schema snapshot: %w",
			err,
		)
	}

	tables := make([]Table, len(normalized.Tables))
	for index, snapshotTable := range normalized.Tables {
		table, err := materializeSnapshotTableWithCheck(
			snapshotTable,
			checkMaterializer,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"materialize schema snapshot table %s: %w",
				snapshotQualifiedName(snapshotTable),
				err,
			)
		}
		tables[index] = table
	}

	// The evolution catalog validator supplies the cross-table assertion
	// boundary: local members, unique referenced keys, exact qualified
	// references, identity shape, and case aliases must all be unambiguous.
	// Empty snapshots are valid durable evidence but are not useful as a
	// complete widening catalog, so validate that shape directly by round trip.
	if len(tables) != 0 {
		if _, err := NewCompleteEvolutionCatalog(tables); err != nil {
			return nil, fmt.Errorf(
				"materialize schema snapshot relations: %w",
				err,
			)
		}
	}

	reconstructed, err := NewSchemaSnapshot(tables)
	if err != nil {
		return nil, fmt.Errorf(
			"materialize schema snapshot round trip: %w",
			err,
		)
	}
	equal, err := SchemaSnapshotsEqual(normalized, reconstructed)
	if err != nil {
		return nil, fmt.Errorf(
			"materialize schema snapshot round trip: %w",
			err,
		)
	}
	if !equal {
		return nil, fmt.Errorf(
			"materialize schema snapshot: reconstructed metadata does not exactly round-trip",
		)
	}
	return tables, nil
}

func materializeSnapshotTableWithCheck(
	snapshot SnapshotTable,
	checkMaterializer func(string, []Column) (Expression, error),
) (Table, error) {
	if err := validateSnapshotMaterializedText(
		"MySQL collation",
		snapshot.MySQLCollation,
		true,
	); err != nil {
		return Table{}, err
	}

	table := Table{
		Schema:             snapshot.Schema,
		Name:               snapshot.Name,
		MySQLCollation:     snapshot.MySQLCollation,
		ClickHouseOrderBy:  cloneStrings(snapshot.ClickHouseOrderBy),
		Columns:            make([]Column, len(snapshot.Columns)),
		Indexes:            make([]Index, len(snapshot.Indexes)),
		ForeignKeys:        make([]ForeignKey, len(snapshot.ForeignKeys)),
		Checks:             make([]CheckConstraint, len(snapshot.Checks)),
		SQLiteWithoutRowID: snapshot.SQLiteWithoutRowID,
		SQLiteStrict:       snapshot.SQLiteStrict,
	}
	if snapshot.Identity != nil {
		table.Identity = &Identity{
			Column:     snapshot.Identity.Column,
			Generation: snapshot.Identity.Generation,
		}
	}

	for index, snapshotColumn := range snapshot.Columns {
		if err := validateSnapshotMaterializedText(
			"column type",
			snapshotColumn.Type,
			false,
		); err != nil {
			return Table{}, fmt.Errorf(
				"column %s: %w",
				snapshotColumn.Name,
				err,
			)
		}
		column := Column{
			Name:               snapshotColumn.Name,
			Type:               snapshotColumn.Type,
			Nullable:           snapshotColumn.Nullable,
			PrimaryKey:         snapshotColumn.PrimaryKey,
			PrimaryKeyPosition: snapshotColumn.PrimaryKeyPosition,
		}
		if snapshotColumn.DeclaredType != nil {
			declared := snapshotDeclaredTypeToCatalog(
				*snapshotColumn.DeclaredType,
			)
			if err := ValidateDeclaredType(declared); err != nil {
				return Table{}, fmt.Errorf(
					"column %s declared type: %w",
					snapshotColumn.Name,
					err,
				)
			}
			column.DeclaredType = &declared
		}
		if snapshotColumn.Default != nil {
			defaultExpression, err := materializeSnapshotDefault(
				*snapshotColumn.Default,
			)
			if err != nil {
				return Table{}, fmt.Errorf(
					"column %s default: %w",
					snapshotColumn.Name,
					err,
				)
			}
			column.Default = defaultExpression
		}
		table.Columns[index] = column
	}

	for index, snapshotIndex := range snapshot.Indexes {
		materialized := Index{
			Name:    snapshotIndex.Name,
			Unique:  snapshotIndex.Unique,
			Inline:  snapshotIndex.Inline,
			Columns: make([]IndexColumn, len(snapshotIndex.Columns)),
		}
		for position, snapshotColumn := range snapshotIndex.Columns {
			if err := validateSnapshotMaterializedText(
				"index collation",
				snapshotColumn.Collation,
				true,
			); err != nil {
				return Table{}, fmt.Errorf(
					"index %q column %s: %w",
					snapshotIndex.Name,
					snapshotColumn.Name,
					err,
				)
			}
			materialized.Columns[position] = IndexColumn{
				Name:       snapshotColumn.Name,
				Descending: snapshotColumn.Descending,
				Collation:  snapshotColumn.Collation,
			}
		}
		table.Indexes[index] = materialized
	}

	for index, snapshotForeignKey := range snapshot.ForeignKeys {
		table.ForeignKeys[index] = ForeignKey{
			Name:              snapshotForeignKey.Name,
			Columns:           cloneStrings(snapshotForeignKey.Columns),
			ReferencedSchema:  snapshotForeignKey.ReferencedSchema,
			ReferencedTable:   snapshotForeignKey.ReferencedTable,
			ReferencedColumns: cloneStrings(snapshotForeignKey.ReferencedColumns),
			OnUpdate:          snapshotForeignKey.OnUpdate,
			OnDelete:          snapshotForeignKey.OnDelete,
			Match:             snapshotForeignKey.Match,
		}
	}

	for index, snapshotCheck := range snapshot.Checks {
		expression, err := checkMaterializer(
			snapshotCheck.Expression,
			table.Columns,
		)
		if err != nil {
			return Table{}, fmt.Errorf(
				"CHECK %q: %w",
				snapshotCheck.Name,
				err,
			)
		}
		table.Checks[index] = CheckConstraint{
			Name:       snapshotCheck.Name,
			Expression: expression,
		}
	}
	return table, nil
}

func materializeSnapshotDefault(value string) (*Expression, error) {
	if !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		value != strings.TrimSpace(value) {
		return nil, fmt.Errorf(
			"default is not canonical UTF-8 scalar SQL",
		)
	}
	if expression, err := ParseSQLiteDefault(value); err == nil {
		if expression != nil &&
			expression.CanonicalSQL() == value &&
			validEvolutionCatalogDefault(*expression) {
			return expression, nil
		}
		return nil, fmt.Errorf(
			"default is not in canonical scalar form",
		)
	}

	expression, err := materializeSnapshotPostgresString(value)
	if err != nil {
		return nil, fmt.Errorf(
			"default is not a supported canonical scalar: %w",
			err,
		)
	}
	return expression, nil
}

// PostgreSQL string defaults use the canonical E'...' form generated by
// postgresStringLiteral. Only doubled quotes and doubled backslashes are valid
// escapes here; accepting arbitrary E-string escape syntax would reintroduce
// executable catalog text into the neutral snapshot boundary.
func materializeSnapshotPostgresString(
	value string,
) (*Expression, error) {
	if len(value) < 3 ||
		!strings.HasPrefix(value, "E'") ||
		value[len(value)-1] != '\'' {
		return nil, fmt.Errorf("unsupported scalar syntax")
	}
	inner := value[2 : len(value)-1]
	var literal strings.Builder
	literal.Grow(len(inner))
	for index := 0; index < len(inner); index++ {
		switch inner[index] {
		case '\'':
			if index+1 >= len(inner) || inner[index+1] != '\'' {
				return nil, fmt.Errorf(
					"PostgreSQL string contains an unmatched quote",
				)
			}
			literal.WriteByte('\'')
			index++
		case '\\':
			if index+1 >= len(inner) || inner[index+1] != '\\' {
				return nil, fmt.Errorf(
					"PostgreSQL string contains a non-canonical escape",
				)
			}
			literal.WriteByte('\\')
			index++
		default:
			literal.WriteByte(inner[index])
		}
	}
	decoded := literal.String()
	if !utf8.ValidString(decoded) ||
		strings.ContainsRune(decoded, '\x00') ||
		postgresStringLiteral(decoded) != value {
		return nil, fmt.Errorf(
			"PostgreSQL string does not round-trip canonically",
		)
	}
	return &Expression{
		sql:     value,
		kind:    expressionString,
		literal: decoded,
	}, nil
}

func materializeSnapshotCheck(
	value string,
	columns []Column,
) (Expression, error) {
	if !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		value != strings.TrimSpace(value) {
		return Expression{}, fmt.Errorf(
			"CHECK is not canonical UTF-8 expression SQL",
		)
	}
	expression := Expression{
		sql:  value,
		kind: expressionCheck,
	}
	root, err := parsePlannedPostgresCheck(expression, columns)
	if err != nil {
		return Expression{}, fmt.Errorf(
			"parse canonical CHECK: %w",
			err,
		)
	}

	// Reparse the generated canonical form and compare structural signatures.
	// This proves that the retained spelling denotes one complete portable AST
	// rather than relying on a token-only safety scan.
	canonical := Expression{
		sql: renderCanonicalPortableCheck(
			root,
			portableCheckPrecedenceLowest,
		),
		kind: expressionCheck,
	}
	reparsed, err := parsePlannedPostgresCheck(canonical, columns)
	if err != nil ||
		makePostgresCheckSignature(root) !=
			makePostgresCheckSignature(reparsed) {
		return Expression{}, fmt.Errorf(
			"canonical CHECK does not structurally round-trip",
		)
	}
	return expression, nil
}

func materializeSnapshotSQLiteCheck(
	value string,
	columns []Column,
) (Expression, error) {
	if !utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		value != strings.TrimSpace(value) {
		return Expression{}, fmt.Errorf(
			"CHECK is not canonical UTF-8 expression SQL",
		)
	}
	// SQLite stores booleans as the exact integer domain used by the target
	// projection (and enforced with enabled IN (0, 1)). Reuse the structural
	// portable predicate parser after adapting only that authenticated storage
	// representation. This deliberately admits the proven portable subset, not
	// arbitrary SQLite grammar: parser member resolution rejects unknown
	// columns and malformed predicates before durable evidence is trusted.
	resolved := make([]Column, len(columns))
	for index, column := range columns {
		resolved[index] = column
		if canonicalEvolutionGenericType(column.Type) != "boolean" {
			continue
		}
		resolved[index].Type = "integer"
		resolved[index].DeclaredType = &DeclaredType{Base: "integer"}
	}
	expression := Expression{sql: value, kind: expressionCheck}
	root, err := parsePlannedPostgresCheck(expression, resolved)
	if err != nil {
		return Expression{}, fmt.Errorf(
			"parse canonical SQLite CHECK structurally: %w", err,
		)
	}
	canonical := Expression{
		sql: renderCanonicalPortableCheck(
			root,
			portableCheckPrecedenceLowest,
		),
		kind: expressionCheck,
	}
	reparsed, err := parsePlannedPostgresCheck(canonical, resolved)
	if err != nil || makePostgresCheckSignature(root) !=
		makePostgresCheckSignature(reparsed) {
		return Expression{}, fmt.Errorf(
			"canonical SQLite CHECK does not structurally round-trip",
		)
	}
	return canonical, nil
}

func validateSnapshotMaterializedText(
	kind string,
	value string,
	allowEmpty bool,
) error {
	if value == "" && allowEmpty {
		return nil
	}
	if value == "" ||
		!utf8.ValidString(value) ||
		strings.ContainsRune(value, '\x00') ||
		value != strings.TrimSpace(value) ||
		strings.Join(strings.Fields(value), " ") != value {
		return fmt.Errorf("%s is empty or non-canonical", kind)
	}
	return nil
}
