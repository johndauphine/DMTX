package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Copy statements, retained-data authority, and the digest that records what
// the evolution actually produced.

func sqliteTargetEvolutionCopySwapCopyStatement(
	table schema.Table,
	temporary string,
) (string, error) {
	if strings.TrimSpace(temporary) == "" || len(table.Columns) == 0 {
		return "", fmt.Errorf("copy/swap table or temporary identity is empty")
	}
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		if strings.TrimSpace(column.Name) == "" {
			return "", fmt.Errorf("copy/swap table has an empty column identity")
		}
		columns[index] = quote(column.Name)
	}
	return "INSERT INTO " + quote(temporary) + " (" + strings.Join(columns, ", ") + ") SELECT " +
		strings.Join(columns, ", ") + " FROM " + quote(table.Name), nil
}

func sqliteTargetEvolutionVerifyCopiedRows(
	ctx context.Context,
	queryer sqliteQueryer,
	before schema.Table,
	temporary string,
) error {
	keys, err := sqliteTargetEvolutionCopySwapPrimaryKey(before)
	if err != nil {
		return err
	}
	var beforeCount, copiedCount int64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(before.Name),
	).Scan(&beforeCount); err != nil {
		return fmt.Errorf("count original rows: %w", err)
	}
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+quote(temporary),
	).Scan(&copiedCount); err != nil {
		return fmt.Errorf("count copied rows: %w", err)
	}
	if beforeCount != copiedCount {
		return fmt.Errorf("row count changed from %d to %d", beforeCount, copiedCount)
	}
	join := make([]string, len(keys))
	for index, key := range keys {
		join[index] = `"source".` + quote(key.Name) + ` IS "copy".` + quote(key.Name)
	}
	comparisons := make([]string, len(before.Columns))
	for index, column := range before.Columns {
		left := `"source".` + quote(column.Name)
		right := `"copy".` + quote(column.Name)
		comparisons[index] = "(typeof(" + left + ") IS " +
			"typeof(" + right + ") AND quote(" + left + ") IS quote(" + right + "))"
	}
	query := "SELECT COUNT(*) FROM " + quote(before.Name) + ` AS "source" LEFT JOIN ` +
		quote(temporary) + ` AS "copy" ON ` + strings.Join(join, " AND ") +
		` WHERE "copy".` + quote(keys[0].Name) + " IS NULL OR NOT (" +
		strings.Join(comparisons, " AND ") + ")"
	var mismatches int64
	if err := queryer.QueryRowContext(ctx, query).Scan(&mismatches); err != nil {
		return fmt.Errorf("compare original and copied values: %w", err)
	}
	if mismatches != 0 {
		return fmt.Errorf("%d retained rows changed storage class or value", mismatches)
	}
	return nil
}

type sqliteTargetEvolutionSequenceAuthority struct {
	present bool
	value   int64
}

// sqliteTargetEvolutionRetainedDataAuthority is captured from the old table
// while BEGIN IMMEDIATE owns the writer fence. It is deliberately streaming:
// no retained target row is held in memory, but the ordered complete primary
// key/value sequence and row count remain independently verifiable after a
// COMMIT acknowledgement loss.
type sqliteTargetEvolutionRetainedDataAuthority struct {
	table    string
	columns  []string
	keys     []string
	rows     int64
	digest   [sha256.Size]byte
	sequence sqliteTargetEvolutionSequenceAuthority
}

func (authority sqliteTargetEvolutionRetainedDataAuthority) sameData(
	other sqliteTargetEvolutionRetainedDataAuthority,
) bool {
	return reflect.DeepEqual(authority.columns, other.columns) &&
		reflect.DeepEqual(authority.keys, other.keys) &&
		authority.rows == other.rows && authority.digest == other.digest
}

func sqliteTargetEvolutionCaptureRetainedDataAuthority(
	ctx context.Context,
	queryer sqliteQueryer,
	table schema.Table,
) (sqliteTargetEvolutionRetainedDataAuthority, error) {
	keys, err := sqliteTargetEvolutionCopySwapPrimaryKey(table)
	if err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	columns := make([]string, len(table.Columns))
	for index, column := range table.Columns {
		if strings.TrimSpace(column.Name) == "" {
			return sqliteTargetEvolutionRetainedDataAuthority{}, fmt.Errorf("table has an empty column identity")
		}
		columns[index] = column.Name
	}
	keyNames := make([]string, len(keys))
	for index, key := range keys {
		keyNames[index] = key.Name
	}
	queryColumns := make([]string, len(columns))
	for index, column := range columns {
		queryColumns[index] = quote(column)
	}
	queryKeys := make([]string, len(keyNames))
	for index, key := range keyNames {
		queryKeys[index] = quote(key)
	}
	rows, err := queryer.QueryContext(
		ctx,
		"SELECT "+strings.Join(queryColumns, ", ")+" FROM "+quote(table.Name)+
			" ORDER BY "+strings.Join(queryKeys, ", "),
	)
	if err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	defer rows.Close()
	digest := sha256.New()
	writeSQLiteTargetEvolutionDigestString(digest, "dmtx-sqlite-copy-swap-data-v1")
	for _, column := range columns {
		writeSQLiteTargetEvolutionDigestString(digest, column)
	}
	for _, key := range keyNames {
		writeSQLiteTargetEvolutionDigestString(digest, key)
	}
	values := make([]any, len(columns))
	scanned := make([]any, len(columns))
	for index := range scanned {
		scanned[index] = &values[index]
	}
	var count int64
	for rows.Next() {
		if err := rows.Scan(scanned...); err != nil {
			return sqliteTargetEvolutionRetainedDataAuthority{}, err
		}
		for _, value := range values {
			if err := writeSQLiteTargetEvolutionDigestValue(digest, value); err != nil {
				return sqliteTargetEvolutionRetainedDataAuthority{}, err
			}
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	if err := rows.Close(); err != nil {
		return sqliteTargetEvolutionRetainedDataAuthority{}, err
	}
	writeSQLiteTargetEvolutionDigestInt64(digest, count)
	var sum [sha256.Size]byte
	copy(sum[:], digest.Sum(nil))
	return sqliteTargetEvolutionRetainedDataAuthority{
		table:   table.Name,
		columns: columns,
		keys:    keyNames,
		rows:    count,
		digest:  sum,
	}, nil
}

func writeSQLiteTargetEvolutionDigestString(digest hash.Hash, value string) {
	writeSQLiteTargetEvolutionDigestBytes(digest, []byte(value))
}

func writeSQLiteTargetEvolutionDigestBytes(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func writeSQLiteTargetEvolutionDigestInt64(digest hash.Hash, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = digest.Write(encoded[:])
}

func writeSQLiteTargetEvolutionDigestValue(digest hash.Hash, value any) error {
	switch typed := value.(type) {
	case nil:
		_, _ = digest.Write([]byte{0})
	case int64:
		_, _ = digest.Write([]byte{1})
		writeSQLiteTargetEvolutionDigestInt64(digest, typed)
	case float64:
		_, _ = digest.Write([]byte{2})
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], math.Float64bits(typed))
		_, _ = digest.Write(encoded[:])
	case bool:
		_, _ = digest.Write([]byte{3})
		if typed {
			_, _ = digest.Write([]byte{1})
		} else {
			_, _ = digest.Write([]byte{0})
		}
	case string:
		_, _ = digest.Write([]byte{4})
		writeSQLiteTargetEvolutionDigestString(digest, typed)
	case []byte:
		_, _ = digest.Write([]byte{5})
		writeSQLiteTargetEvolutionDigestBytes(digest, typed)
	case time.Time:
		_, _ = digest.Write([]byte{6})
		writeSQLiteTargetEvolutionDigestString(digest, typed.UTC().Format(time.RFC3339Nano))
	default:
		return fmt.Errorf("copy/swap retained data has unsupported SQLite scan type %T", value)
	}
	return nil
}

func sqliteTargetEvolutionReadSequence(
	ctx context.Context,
	queryer sqliteQueryer,
	table string,
	required bool,
) (sqliteTargetEvolutionSequenceAuthority, error) {
	exists, err := sqliteTargetEvolutionSequenceTableExists(ctx, queryer)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, err
	}
	if !exists {
		if required {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf(
				"sqlite_sequence is absent for AUTOINCREMENT table %s", table,
			)
		}
		return sqliteTargetEvolutionSequenceAuthority{}, nil
	}
	rows, err := queryer.QueryContext(
		ctx,
		`SELECT name, seq FROM sqlite_sequence WHERE name = ? COLLATE NOCASE`,
		table,
	)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("read sqlite_sequence: %w", err)
	}
	defer rows.Close()
	var result sqliteTargetEvolutionSequenceAuthority
	for rows.Next() {
		var name string
		var sequence int64
		if err := rows.Scan(&name, &sequence); err != nil {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("scan sqlite_sequence: %w", err)
		}
		if stage4SQLiteIdentifier(name) != stage4SQLiteIdentifier(table) || sequence < 0 || result.present {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf(
				"sqlite_sequence authority for table %s is ambiguous or invalid", table,
			)
		}
		result.present = true
		result.value = sequence
	}
	if err := rows.Err(); err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("iterate sqlite_sequence: %w", err)
	}
	return result, nil
}

func sqliteTargetEvolutionSequenceTableExists(
	ctx context.Context,
	queryer sqliteQueryer,
) (bool, error) {
	var name string
	err := queryer.QueryRowContext(
		ctx,
		`SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'sqlite_sequence'`,
	).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("authenticate sqlite_sequence catalog object: %w", err)
	}
	if name != "sqlite_sequence" {
		return false, fmt.Errorf("sqlite_sequence catalog object has unexpected identity %q", name)
	}
	return true, nil
}

func sqliteTargetEvolutionPositiveMaximum(
	ctx context.Context,
	queryer sqliteQueryer,
	table schema.Table,
) (int64, bool, error) {
	if table.Identity == nil || strings.TrimSpace(table.Identity.Column) == "" {
		return 0, false, nil
	}
	var maximum sql.NullInt64
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT MAX("+quote(table.Identity.Column)+") FROM "+quote(table.Name)+
			" WHERE "+quote(table.Identity.Column)+" > 0",
	).Scan(&maximum); err != nil {
		return 0, false, err
	}
	if !maximum.Valid {
		return 0, false, nil
	}
	return maximum.Int64, true, nil
}

func sqliteTargetEvolutionRestoreSequence(
	ctx context.Context,
	executor interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	},
	queryer sqliteQueryer,
	table string,
	temporary string,
	prior sqliteTargetEvolutionSequenceAuthority,
	priorMaximum int64,
	priorMaximumKnown bool,
	copiedMaximum int64,
	copiedMaximumKnown bool,
) (sqliteTargetEvolutionSequenceAuthority, error) {
	frontier := sqliteTargetEvolutionSequenceAuthority{}
	for _, candidate := range []sqliteTargetEvolutionSequenceAuthority{prior} {
		if candidate.present && (!frontier.present || candidate.value > frontier.value) {
			frontier = candidate
		}
	}
	for _, candidate := range []struct {
		value int64
		known bool
	}{
		{value: priorMaximum, known: priorMaximumKnown},
		{value: copiedMaximum, known: copiedMaximumKnown},
	} {
		if candidate.known && (!frontier.present || candidate.value > frontier.value) {
			frontier = sqliteTargetEvolutionSequenceAuthority{present: true, value: candidate.value}
		}
	}
	for _, name := range []string{table, temporary} {
		if _, err := executor.ExecContext(
			ctx,
			`DELETE FROM sqlite_sequence WHERE name = ? COLLATE NOCASE`,
			name,
		); err != nil {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("clear sqlite_sequence entry %s: %w", name, err)
		}
	}
	if frontier.present {
		if _, err := executor.ExecContext(
			ctx,
			`INSERT INTO sqlite_sequence(name, seq) VALUES (?, ?)`,
			table,
			frontier.value,
		); err != nil {
			return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("restore sqlite_sequence frontier %d: %w", frontier.value, err)
		}
	}
	restored, err := sqliteTargetEvolutionReadSequence(ctx, queryer, table, true)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("authenticate restored sqlite_sequence: %w", err)
	}
	if restored != frontier {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("restored sqlite_sequence frontier differs from authenticated safe frontier")
	}
	temporaryEntry, err := sqliteTargetEvolutionReadSequence(ctx, queryer, temporary, true)
	if err != nil {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("authenticate temporary sqlite_sequence cleanup: %w", err)
	}
	if temporaryEntry.present {
		return sqliteTargetEvolutionSequenceAuthority{}, fmt.Errorf("sqlite_sequence retained an orphan temporary-name entry")
	}
	return frontier, nil
}

func sqliteTargetEvolutionAssertTemporaryObjectAbsent(
	ctx context.Context,
	queryer sqliteQueryer,
	temporary string,
) error {
	rows, err := queryer.QueryContext(ctx, `SELECT type, name FROM sqlite_schema ORDER BY type, name`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var kind, name string
		if err := rows.Scan(&kind, &name); err != nil {
			return err
		}
		if stage4SQLiteIdentifier(name) == stage4SQLiteIdentifier(temporary) {
			return fmt.Errorf("temporary %s object %s remains", kind, name)
		}
	}
	return rows.Err()
}

func verifySQLiteTargetEvolutionCopySwapIntegrity(
	ctx context.Context,
	queryer sqliteQueryer,
) error {
	if err := preflightSQLiteForeignKeyIntegrity(ctx, queryer, ""); err != nil {
		return fmt.Errorf("verify SQLite copy/swap foreign-key integrity: %w", err)
	}
	rows, err := queryer.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("run SQLite copy/swap quick_check: %w", err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate SQLite copy/swap quick_check: %w", err)
		}
		return fmt.Errorf("SQLite copy/swap quick_check returned no authority")
	}
	var result string
	if err := rows.Scan(&result); err != nil {
		return fmt.Errorf("read SQLite copy/swap quick_check: %w", err)
	}
	if result != "ok" || rows.Next() {
		return fmt.Errorf("SQLite copy/swap quick_check is not exact ok authority")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite copy/swap quick_check: %w", err)
	}
	return nil
}

func readSQLiteTargetEvolutionCatalog(
	ctx context.Context,
	queryer sqliteQueryer,
) (TargetSchemaEvolutionCatalog, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT type, name, tbl_name, sql
		  FROM sqlite_schema
		 WHERE name NOT LIKE 'sqlite_%'
		 ORDER BY type, name`)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"list complete SQLite target schema catalog: %w", err,
		)
	}
	defer rows.Close()
	tableNames := make([]string, 0)
	reservations := make([]TargetSchemaEvolutionNameReservation, 0)
	for rows.Next() {
		var kind, name, tableName string
		var statement sql.NullString
		if err := rows.Scan(&kind, &name, &tableName, &statement); err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"read SQLite target schema catalog: %w", err,
			)
		}
		switch kind {
		case "table":
			tableNames = append(tableNames, name)
		case "index":
			// Every supported SQLite index is represented by its owning table's
			// exact inspector shape. An index with no table authority cannot be
			// preserved or checked as a deterministic evolution state.
			if tableName == "" {
				return TargetSchemaEvolutionCatalog{}, targetSchemaEvolutionError(
					TargetSchemaEvolutionReadFailed,
					"catalog",
					"SQLite index "+name+" has no owning table",
					nil,
				)
			}
		case "view":
			var reservationErr error
			reservations, reservationErr = appendSQLiteTargetEvolutionObjectReservations(
				reservations, "relation", "view", name, statement,
			)
			if reservationErr != nil {
				return TargetSchemaEvolutionCatalog{}, reservationErr
			}
		case "trigger":
			var reservationErr error
			reservations, reservationErr = appendSQLiteTargetEvolutionObjectReservations(
				reservations, "trigger", "trigger", name, statement,
			)
			if reservationErr != nil {
				return TargetSchemaEvolutionCatalog{}, reservationErr
			}
		default:
			return TargetSchemaEvolutionCatalog{}, targetSchemaEvolutionError(
				TargetSchemaEvolutionReadFailed,
				"catalog",
				"SQLite sqlite_schema contains unsupported object type "+kind,
				nil,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"iterate SQLite target schema catalog: %w", err,
		)
	}
	if err := rows.Close(); err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"close SQLite target schema catalog: %w", err,
		)
	}
	sort.Strings(tableNames)
	tables := make([]schema.Table, 0, len(tableNames))
	for _, name := range tableNames {
		table, _, err := inspectSQLiteSchema(ctx, queryer, name)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
				"inspect complete SQLite target table %s: %w", name, err,
			)
		}
		// SQLite materializes the portable identity contract as INTEGER PRIMARY
		// KEY AUTOINCREMENT. Its physical declared type is necessarily INTEGER,
		// but its value domain is the signed 64-bit domain represented by the
		// portable bigint identity contract. The complete evolution catalog is
		// semantic authority consumed alongside projected target shapes, so
		// normalize only this independently authenticated identity column back
		// to that portable representation. Ordinary retained-shape preflight
		// continues to compare the physical INTEGER AUTOINCREMENT form.
		table, err = canonicalizeSQLiteTargetEvolutionIdentity(table)
		if err != nil {
			return TargetSchemaEvolutionCatalog{}, err
		}
		tables = append(tables, table)
	}
	sortTargetSchemaEvolutionTables(tables)
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, reservations)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"validate complete SQLite target catalog: %w", err,
		)
	}
	return catalog, nil
}

func canonicalizeSQLiteTargetEvolutionIdentity(
	table schema.Table,
) (schema.Table, error) {
	if table.Identity == nil {
		return table, nil
	}
	identity := table.Identity.Column
	for index := range table.Columns {
		column := &table.Columns[index]
		if column.Name != identity {
			continue
		}
		if !column.PrimaryKey || column.PrimaryKeyPosition != 1 ||
			column.DeclaredType == nil ||
			!strings.EqualFold(
				strings.TrimSpace(column.DeclaredType.Base), "integer",
			) || len(column.DeclaredType.Arguments) != 0 {
			return schema.Table{}, targetSchemaEvolutionError(
				TargetSchemaEvolutionReadFailed,
				"catalog",
				"SQLite identity "+table.Name+"."+identity+
					" is not the exact INTEGER PRIMARY KEY AUTOINCREMENT shape",
				nil,
			)
		}
		column.Type = "bigint"
		column.Nullable = false
		column.DeclaredType = &schema.DeclaredType{Base: "bigint"}
		return table, nil
	}
	return schema.Table{}, targetSchemaEvolutionError(
		TargetSchemaEvolutionReadFailed,
		"catalog",
		"SQLite identity metadata references missing column "+
			table.Name+"."+identity,
		nil,
	)
}

func appendSQLiteTargetEvolutionObjectReservations(
	reservations []TargetSchemaEvolutionNameReservation,
	collisionScope string,
	definitionScope string,
	name string,
	statement sql.NullString,
) ([]TargetSchemaEvolutionNameReservation, error) {
	// Both the user-visible name and a stable statement fingerprint are needed:
	// the former protects new relation allocation and the latter makes a
	// same-name trigger/view rewrite catalog drift rather than trusted state.
	if !statement.Valid || strings.TrimSpace(statement.String) == "" {
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionReadFailed,
			"catalog",
			"persistent SQLite "+definitionScope+" "+name+" has no exact sqlite_schema SQL authority",
			nil,
		)
	}
	reservations = append(reservations, TargetSchemaEvolutionNameReservation{
		Scope: collisionScope, Namespace: sqliteTargetEvolutionNamespace, Name: name,
	})
	hash := sha256.Sum256([]byte(strings.TrimSpace(statement.String)))
	return append(reservations, TargetSchemaEvolutionNameReservation{
		Scope:     definitionScope + "_definition",
		Namespace: sqliteTargetEvolutionNamespace,
		Name:      name + "@" + fmt.Sprintf("%x", hash[:]),
	}), nil
}

type sqliteTargetSchemaEvolutionCreatePlanner struct{}

func (sqliteTargetSchemaEvolutionCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesiredTables []schema.Table,
	actualCatalog TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	if target != schema.SQLite {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"SQLite target create planner cannot render %q", target,
		)
	}
	created := cloneTargetSchemaEvolutionTables(tables)
	sortTargetSchemaEvolutionTables(created)
	if len(created) == 0 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"SQLite target create planner has no tables",
		)
	}
	desired := cloneTargetSchemaEvolutionTables(completeDesiredTables)
	sortTargetSchemaEvolutionTables(desired)
	if err := validateSQLiteTargetEvolutionCreateNames(
		created, desired, actualCatalog,
	); err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}

	state := make([]schema.Table, 0, len(created))
	steps := make([]TargetSchemaCreateStep, 0, len(created)*2)
	for _, table := range created {
		statement, err := schema.CreateTableDDL(schema.SQLite, table)
		if err != nil {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"plan complete SQLite target table %s: %w", table.Name, err,
			)
		}
		base := cloneStage4RichTable(table)
		base.Indexes = sqliteTargetEvolutionInlineIndexes(table.Indexes)
		state = append(state, base)
		sortTargetSchemaEvolutionTables(state)
		steps = append(steps, TargetSchemaCreateStep{
			Statement: statement, ResultTables: cloneTargetSchemaEvolutionTables(state),
		})
	}
	for _, table := range created {
		indexes := sqliteTargetEvolutionStandaloneIndexes(table.Indexes)
		for _, index := range indexes {
			statement, err := schema.SQLitePlannedIndexDDL(table, index)
			if err != nil {
				return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
					"seal SQLite target index %s on %s: %w", index.Name, table.Name, err,
				)
			}
			stateIndex := findTargetSchemaEvolutionTable(state, targetSchemaEvolutionTableKey{table: table.Name})
			if stateIndex < 0 {
				return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
					"SQLite target create state lost table %s", table.Name,
				)
			}
			state[stateIndex].Indexes = append(state[stateIndex].Indexes, index)
			steps = append(steps, TargetSchemaCreateStep{
				Statement: statement, ResultTables: cloneTargetSchemaEvolutionTables(state),
			})
		}
	}
	return NewCompleteTargetSchemaCreateBundle(schema.SQLite, created, steps)
}

func sqliteTargetEvolutionInlineIndexes(indexes []schema.Index) []schema.Index {
	result := make([]schema.Index, 0, len(indexes))
	for _, index := range indexes {
		if index.Inline {
			result = append(result, index)
		}
	}
	return result
}

func sqliteTargetEvolutionStandaloneIndexes(indexes []schema.Index) []schema.Index {
	result := make([]schema.Index, 0, len(indexes))
	for _, index := range indexes {
		if !index.Inline {
			result = append(result, index)
		}
	}
	sort.SliceStable(result, func(left, right int) bool {
		return sqliteIndexSortKey(result[left]) < sqliteIndexSortKey(result[right])
	})
	return result
}

func validateSQLiteTargetEvolutionCreateNames(
	created []schema.Table,
	desired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) error {
	createdNames := make(map[string]string, len(created))
	type sqliteExistingObject struct {
		kind  string
		owner string
	}
	existingObjects := make(map[string]sqliteExistingObject)
	addExisting := func(name, kind, owner string) error {
		key := stage4SQLiteIdentifier(name)
		if earlier, exists := existingObjects[key]; exists {
			return fmt.Errorf(
				"SQLite target catalog has case-insensitive object collision between %s and %s",
				earlier.kind, kind,
			)
		}
		existingObjects[key] = sqliteExistingObject{kind: kind, owner: owner}
		return nil
	}
	for _, table := range actual.Tables() {
		if err := addExisting(table.Name, "table "+table.Name, table.Name); err != nil {
			return err
		}
		for _, index := range table.Indexes {
			if index.Inline || index.Name == "" {
				continue
			}
			if err := addExisting(index.Name, "index "+index.Name, table.Name); err != nil {
				return err
			}
		}
	}
	for _, reservation := range actual.Reservations() {
		if reservation.Namespace != sqliteTargetEvolutionNamespace {
			return fmt.Errorf("SQLite target catalog has reservation outside main")
		}
		if reservation.Scope != "relation" && reservation.Scope != "trigger" {
			continue
		}
		if err := addExisting(reservation.Name, reservation.Scope+" "+reservation.Name, ""); err != nil {
			return err
		}
	}
	addCreated := func(name, kind, owner string) error {
		key := stage4SQLiteIdentifier(name)
		if existing, exists := existingObjects[key]; exists {
			// A complete table or index may already be present only as an
			// authenticated immutable prefix for the same created table. The
			// generic state machine compares that full shape before permitting a
			// resume; this narrow exception merely lets its planner rebuild.
			if stage4SQLiteIdentifier(existing.owner) != stage4SQLiteIdentifier(owner) ||
				!strings.HasPrefix(existing.kind, strings.Fields(kind)[0]+" ") {
				return fmt.Errorf("SQLite target create %s collides with existing %s", kind, existing.kind)
			}
		}
		if earlier, exists := createdNames[key]; exists {
			return fmt.Errorf("SQLite target create %s collides with planned %s", kind, earlier)
		}
		createdNames[key] = kind
		return nil
	}
	for _, table := range created {
		if table.Schema != "" || strings.TrimSpace(table.Name) == "" ||
			strings.HasPrefix(strings.ToLower(table.Name), "sqlite_") {
			return fmt.Errorf(
				"SQLite target create table has an unsupported namespace or reserved name %q", table.Name,
			)
		}
		if err := addCreated(table.Name, "table "+table.Name, table.Name); err != nil {
			return err
		}
		for _, index := range table.Indexes {
			if index.Inline && index.Name != "" {
				return fmt.Errorf(
					"SQLite target create table %s has a named inline constraint %s that cannot be recovered from the exact catalog",
					table.Name, index.Name,
				)
			}
			if !index.Inline && strings.TrimSpace(index.Name) == "" {
				return fmt.Errorf(
					"SQLite target create table %s has an unnamed standalone index", table.Name,
				)
			}
			if !index.Inline {
				if err := addCreated(index.Name, "index "+index.Name, table.Name); err != nil {
					return err
				}
			}
		}
		for _, check := range table.Checks {
			if check.Name != "" {
				return fmt.Errorf(
					"SQLite target create table %s has a named CHECK constraint %s that cannot be recovered from the exact catalog",
					table.Name, check.Name,
				)
			}
		}
		for _, foreignKey := range table.ForeignKeys {
			if foreignKey.Name != "" {
				return fmt.Errorf(
					"SQLite target create table %s has a named foreign key %s that cannot be recovered from the exact catalog",
					table.Name, foreignKey.Name,
				)
			}
		}
	}
	_ = desired // desired coverage is authenticated by CompleteTargetSchemaCreateBundle.
	return nil
}

func rollbackSQLiteTargetEvolutionTransaction(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	cleanupCtx, cancel := sqliteTargetEvolutionDetachedContext(ctx)
	defer cancel()
	_, err := connection.ExecContext(cleanupCtx, "ROLLBACK")
	return err
}

func sqliteTargetEvolutionDetachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.WithoutCancel(ctx),
		targetSchemaEvolutionVerificationTimeout,
	)
}

func discardSQLiteTargetEvolutionConnection(connection *sql.Conn) {
	if connection == nil {
		return
	}
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
}

func joinSQLiteTargetEvolutionCleanupError(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	return errors.Join(primary, targetSchemaEvolutionError(
		TargetSchemaEvolutionVerifyFailed,
		"DDL fence cleanup",
		targetSchemaEvolutionRecoveryWording(
			"SQLite target schema evolution could not release its pinned mutation connection",
		),
		cleanup,
	))
}

func verifySQLiteTargetEvolutionCommittedCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQLite evolution committed but an independent complete catalog snapshot could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQLite evolution committed but the independent complete catalog snapshot is structurally invalid",
			),
			err,
		)
	}
	if _, err := matchTargetSchemaEvolutionState(
		[][]schema.Table{plan.states[len(plan.states)-1]}, plan.reservations, actual,
	); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-commit verification",
			targetSchemaEvolutionRecoveryWording(
				"SQLite evolution committed but an independent snapshot found concurrent or unexpected catalog drift",
			),
			err,
		)
	}
	return nil
}

// verifySQLiteTargetEvolutionCommittedAuthority supplements exact catalog
// comparison with the retained-row authority captured under the writer fence.
// A COMMIT/no-ack path is not success merely because the DDL shape matches:
// every copied table must still have the same ordered typed key/value sequence
// and the exact safe AUTOINCREMENT frontier.
func (adapter *sqliteTargetAdapter) verifySQLiteTargetEvolutionCommittedAuthority(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	retained []sqliteTargetEvolutionRetainedDataAuthority,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
) error {
	if err := verifySQLiteTargetEvolutionCommittedCatalog(plan, actual, readErr); err != nil {
		return err
	}
	if len(retained) == 0 {
		return nil
	}
	tables := actual.Tables()
	for _, expected := range retained {
		index := findTargetSchemaEvolutionTable(
			tables,
			targetSchemaEvolutionTableKey{table: expected.table},
		)
		if index < 0 {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionVerifyFailed,
				"post-commit retained data verification",
				targetSchemaEvolutionRecoveryWording(
					"SQLite evolution committed but a retained-data table is absent from exact catalog authority",
				),
				nil,
			)
		}
		observed, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
			ctx,
			adapter.database,
			tables[index],
		)
		if err != nil {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionVerifyFailed,
				"post-commit retained data verification",
				targetSchemaEvolutionRecoveryWording(
					"SQLite evolution committed but retained data could not be independently read",
				),
				err,
			)
		}
		if tables[index].Identity != nil {
			observed.sequence, err = sqliteTargetEvolutionReadSequence(
				ctx,
				adapter.database,
				tables[index].Name,
				true,
			)
			if err != nil {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionVerifyFailed,
					"post-commit retained data verification",
					targetSchemaEvolutionRecoveryWording(
						"SQLite evolution committed but AUTOINCREMENT authority could not be independently read",
					),
					err,
				)
			}
		}
		if !expected.sameData(observed) || expected.sequence != observed.sequence {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionVerifyFailed,
				"post-commit retained data verification",
				targetSchemaEvolutionRecoveryWording(
					"SQLite evolution committed but retained rows or AUTOINCREMENT frontier differ from writer-fenced authority",
				),
				nil,
			)
		}
	}
	return nil
}

func (adapter *sqliteTargetAdapter) classifySQLiteTargetEvolutionCommitAmbiguity(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	retained []sqliteTargetEvolutionRetainedDataAuthority,
	commitErr error,
) error {
	actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err := classifySQLiteTargetEvolutionCommitCatalog(
		plan,
		actual,
		readErr,
		commitErr,
	); err != nil {
		return err
	}
	return adapter.verifySQLiteTargetEvolutionCommittedAuthority(
		ctx,
		plan,
		retained,
		actual,
		readErr,
	)
}

func classifySQLiteTargetEvolutionCommitCatalog(
	plan TargetSchemaEvolutionPlan,
	actual TargetSchemaEvolutionCatalog,
	readErr error,
	commitErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQLite commit returned an error and an independent complete catalog snapshot could not be read",
			),
			errors.Join(commitErr, readErr),
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(actual); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQLite commit returned an error and the independent catalog is structurally invalid",
			),
			errors.Join(commitErr, err),
		)
	}
	prefix, err := matchTargetSchemaEvolutionState(plan.states, plan.reservations, actual)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"commit",
			targetSchemaEvolutionRecoveryWording(
				"SQLite commit returned an error and the independent catalog has unexpected drift",
			),
			errors.Join(commitErr, err),
		)
	}
	if prefix == len(plan.operations) {
		return nil
	}
	return targetSchemaEvolutionError(
		TargetSchemaEvolutionApplyFailed,
		"commit",
		targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
			"SQLite commit returned an error after exact verified prefix %d of %d",
			prefix, len(plan.operations),
		)),
		commitErr,
	)
}
