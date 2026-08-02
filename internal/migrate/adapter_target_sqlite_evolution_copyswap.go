package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// SQLite copy-swap safety: the incoming foreign keys, shape, and object
// checks that must hold before a table is rebuilt in place.

func validateSQLiteTargetEvolutionPlannedIncomingForeignKeys(
	plan TargetSchemaEvolutionPlan,
	operationIndex int,
	table string,
) error {
	if operationIndex < 0 || operationIndex >= len(plan.states) ||
		strings.TrimSpace(table) == "" {
		return fmt.Errorf("copy/swap planned incoming foreign-key authority is incomplete")
	}
	state := plan.states[operationIndex]
	type incoming struct {
		child    string
		foreign  int
		onDelete string
		onUpdate string
	}
	var dependencies []incoming
	for _, candidate := range state {
		if strings.TrimSpace(candidate.Name) == "" {
			return fmt.Errorf("copy/swap planned incoming foreign-key authority has an empty child table identity")
		}
		for index, foreignKey := range candidate.ForeignKeys {
			if stage4SQLiteIdentifier(foreignKey.ReferencedTable) != stage4SQLiteIdentifier(table) {
				continue
			}
			onDelete := strings.TrimSpace(foreignKey.OnDelete)
			if onDelete == "" {
				onDelete = "UNKNOWN"
			}
			onUpdate := strings.TrimSpace(foreignKey.OnUpdate)
			if onUpdate == "" {
				onUpdate = "UNKNOWN"
			}
			dependencies = append(dependencies, incoming{
				child:    candidate.Name,
				foreign:  index,
				onDelete: onDelete,
				onUpdate: onUpdate,
			})
		}
	}
	if len(dependencies) == 0 {
		return nil
	}
	parts := make([]string, len(dependencies))
	for index, dependency := range dependencies {
		parts[index] = fmt.Sprintf(
			"%s(foreign_key=%d,on_delete=%s,on_update=%s)",
			dependency.child,
			dependency.foreign,
			dependency.onDelete,
			dependency.onUpdate,
		)
	}
	return fmt.Errorf(
		"copy/swap table %s has planned incoming foreign-key dependencies (%s)",
		table,
		strings.Join(parts, ", "),
	)
}

// validateSQLiteTargetEvolutionCopySwapRendering proves every DDL fragment
// needed to reconstruct the sealed final table before the writer reservation
// or any destructive operation. Catalog equality alone does not establish
// that a target-owned index/constraint can be rendered faithfully.
func validateSQLiteTargetEvolutionCopySwapRendering(after schema.Table) error {
	statement, err := schema.CreateTableDDL(schema.SQLite, after)
	if err != nil {
		return fmt.Errorf("render replacement table %s: %w", after.Name, err)
	}
	if _, err := schema.RenderDDLStatement(statement, schema.SQLite); err != nil {
		return fmt.Errorf("authenticate replacement table %s: %w", after.Name, err)
	}
	for _, index := range sqliteTargetEvolutionStandaloneIndexes(after.Indexes) {
		statement, err := schema.SQLitePlannedIndexDDL(after, index)
		if err != nil {
			return fmt.Errorf("render replacement index %s on %s: %w", index.Name, after.Name, err)
		}
		if _, err := schema.RenderDDLStatement(statement, schema.SQLite); err != nil {
			return fmt.Errorf("authenticate replacement index %s on %s: %w", index.Name, after.Name, err)
		}
	}
	return nil
}

func sqliteTargetEvolutionCopySwapOperation(
	plan TargetSchemaEvolutionPlan,
	operationIndex int,
	operation TargetSchemaEvolutionOperation,
) (schema.Table, schema.Table, string, error) {
	if operationIndex < 0 || operationIndex >= len(plan.operations) ||
		operationIndex+1 >= len(plan.states) {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation index %d is outside the immutable plan", operationIndex,
		)
	}
	if operation.Action() != SchemaContractRelaxNullability &&
		operation.Action() != SchemaContractWidenType {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d has unsupported action %s",
			operationIndex,
			operation.Action(),
		)
	}
	statements := operation.Statements()
	if len(statements) != 1 || statements[0] != sqliteTargetEvolutionCopySwapStatement {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d does not carry the immutable SQLite marker",
			operationIndex,
		)
	}
	objects := operation.Objects()
	if len(objects) != 1 || objects[0].Schema != "" ||
		strings.TrimSpace(objects[0].Table) == "" ||
		strings.TrimSpace(objects[0].Column) == "" {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d has incomplete SQLite table/column authority",
			operationIndex,
		)
	}
	key := targetSchemaEvolutionTableKey{table: objects[0].Table}
	beforeIndex := findTargetSchemaEvolutionTable(plan.states[operationIndex], key)
	afterIndex := findTargetSchemaEvolutionTable(plan.states[operationIndex+1], key)
	if beforeIndex < 0 || afterIndex < 0 {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap operation %d is missing its table from an exact catalog state",
			operationIndex,
		)
	}
	before := cloneStage4RichTable(plan.states[operationIndex][beforeIndex])
	after := cloneStage4RichTable(plan.states[operationIndex+1][afterIndex])
	if err := validateSQLiteTargetEvolutionCopySwapShape(
		before,
		after,
		operation.Action(),
		objects[0].Column,
	); err != nil {
		return schema.Table{}, schema.Table{}, "", err
	}
	if stage4SQLiteIdentifier(before.Name) == stage4SQLiteIdentifier(sqliteDeleteJournalTable) {
		return schema.Table{}, schema.Table{}, "", fmt.Errorf(
			"copy/swap refuses private DMTX delete receipt table %s", before.Name,
		)
	}
	temporary, err := sqliteTargetEvolutionCopySwapTemporaryName(plan, operationIndex)
	if err != nil {
		return schema.Table{}, schema.Table{}, "", err
	}
	return before, after, temporary, nil
}

func validateSQLiteTargetEvolutionCopySwapShape(
	before schema.Table,
	after schema.Table,
	action SchemaContractAction,
	column string,
) error {
	if before.Schema != "" || after.Schema != "" ||
		strings.TrimSpace(before.Name) == "" ||
		before.Name != after.Name || len(before.Columns) != len(after.Columns) {
		return fmt.Errorf("copy/swap changes table identity or column cardinality")
	}
	changed := -1
	for index := range before.Columns {
		if before.Columns[index].Name != after.Columns[index].Name {
			return fmt.Errorf("copy/swap changes column ordering or identity")
		}
		if before.Columns[index].Name == column {
			changed = index
		}
	}
	if changed < 0 {
		return fmt.Errorf("copy/swap changed column %s is absent", column)
	}
	if _, err := sqliteTargetEvolutionCopySwapPrimaryKey(before); err != nil {
		return err
	}
	if _, err := sqliteTargetEvolutionCopySwapPrimaryKey(after); err != nil {
		return err
	}
	normalizedBefore := cloneStage4RichTable(before)
	normalizedAfter := cloneStage4RichTable(after)
	if normalizedBefore.Identity != nil {
		normalizedBefore.Identity.Frontier = nil
	}
	if normalizedAfter.Identity != nil {
		normalizedAfter.Identity.Frontier = nil
	}
	switch action {
	case SchemaContractRelaxNullability:
		if normalizedBefore.Columns[changed].Nullable ||
			!normalizedAfter.Columns[changed].Nullable {
			return fmt.Errorf("copy/swap relax_nullability does not change NOT NULL to NULL")
		}
		normalizedAfter.Columns[changed].Nullable = normalizedBefore.Columns[changed].Nullable
	case SchemaContractWidenType:
		if normalizedBefore.Columns[changed].Type == normalizedAfter.Columns[changed].Type &&
			reflect.DeepEqual(
				normalizedBefore.Columns[changed].DeclaredType,
				normalizedAfter.Columns[changed].DeclaredType,
			) {
			return fmt.Errorf("copy/swap widen_type does not change the declared type")
		}
		normalizedAfter.Columns[changed].Type = normalizedBefore.Columns[changed].Type
		normalizedAfter.Columns[changed].DeclaredType = cloneStage4RichColumn(
			normalizedBefore.Columns[changed],
		).DeclaredType
	default:
		return fmt.Errorf("copy/swap action %s is unsupported", action)
	}
	if !reflect.DeepEqual(normalizedBefore, normalizedAfter) {
		return fmt.Errorf(
			"copy/swap %s changes objects beyond %s.%s",
			action,
			before.Name,
			column,
		)
	}
	return nil
}

func sqliteTargetEvolutionCopySwapPrimaryKey(table schema.Table) ([]schema.Column, error) {
	keys := make([]schema.Column, 0)
	for _, column := range table.Columns {
		if !column.PrimaryKey {
			continue
		}
		if column.PrimaryKeyPosition <= 0 || column.Nullable {
			return nil, fmt.Errorf(
				"copy/swap table %s has no complete non-null primary-key authority",
				table.Name,
			)
		}
		keys = append(keys, cloneStage4RichColumn(column))
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf(
			"copy/swap table %s has no complete primary-key authority", table.Name,
		)
	}
	sort.Slice(keys, func(left, right int) bool {
		return keys[left].PrimaryKeyPosition < keys[right].PrimaryKeyPosition
	})
	for index := range keys {
		if keys[index].PrimaryKeyPosition != index+1 {
			return nil, fmt.Errorf(
				"copy/swap table %s has non-contiguous primary-key authority",
				table.Name,
			)
		}
	}
	return keys, nil
}

func sqliteTargetEvolutionCopySwapTemporaryName(
	plan TargetSchemaEvolutionPlan,
	operationIndex int,
) (string, error) {
	if !plan.valid() || operationIndex < 0 || operationIndex >= len(plan.operations) {
		return "", fmt.Errorf("copy/swap temporary name has incomplete immutable plan authority")
	}
	reserved := make(map[string]struct{})
	for _, state := range plan.states {
		for _, table := range state {
			reserved[stage4SQLiteIdentifier(table.Name)] = struct{}{}
			for _, index := range table.Indexes {
				if index.Name != "" {
					reserved[stage4SQLiteIdentifier(index.Name)] = struct{}{}
				}
			}
		}
	}
	for _, reservation := range plan.reservations {
		if reservation.Namespace != sqliteTargetEvolutionNamespace ||
			(reservation.Scope != "relation" && reservation.Scope != "trigger") {
			continue
		}
		reserved[stage4SQLiteIdentifier(reservation.Name)] = struct{}{}
	}
	hash := sha256.Sum256([]byte(plan.Digest() + "\\x00" + strconv.Itoa(operationIndex)))
	prefix := "__dmtx_evolve_" + fmt.Sprintf("%x", hash[:12])
	for attempt := 0; attempt < 1024; attempt++ {
		candidate := prefix + "_" + strconv.Itoa(attempt)
		if _, exists := reserved[stage4SQLiteIdentifier(candidate)]; !exists {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("copy/swap temporary name collides with immutable SQLite catalog authority")
}

func validateSQLiteTargetEvolutionCopySwapObjects(
	ctx context.Context,
	queryer sqliteQueryer,
	table string,
	temporary string,
) error {
	if queryer == nil {
		return fmt.Errorf("copy/swap object authority queryer is not configured")
	}
	if stage4SQLiteIdentifier(table) == stage4SQLiteIdentifier(sqliteDeleteJournalTable) {
		return fmt.Errorf("copy/swap refuses private DMTX delete receipt table %s", table)
	}
	rows, err := queryer.QueryContext(ctx, `SELECT type, name, tbl_name FROM sqlite_schema ORDER BY type, name`)
	if err != nil {
		return fmt.Errorf("list SQLite copy/swap object authority: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind, name, owner string
		if err := rows.Scan(&kind, &name, &owner); err != nil {
			return fmt.Errorf("read SQLite copy/swap object authority: %w", err)
		}
		if stage4SQLiteIdentifier(name) == stage4SQLiteIdentifier(temporary) {
			return fmt.Errorf(
				"copy/swap deterministic temporary name %s collides with existing %s %s",
				temporary,
				kind,
				name,
			)
		}
		// DROP TABLE deliberately leaves views alone, so their exact SQL
		// reservations remain authenticated. SQLite drops triggers owned by
		// the table, however, and there is no sealed trigger renderer in this
		// bounded slice. Refuse before any DDL rather than execute catalog SQL.
		if kind == "trigger" && stage4SQLiteIdentifier(owner) == stage4SQLiteIdentifier(table) {
			return fmt.Errorf(
				"copy/swap table %s owns trigger %s which cannot be faithfully reconstructed",
				table,
				name,
			)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite copy/swap object authority: %w", err)
	}
	return nil
}

// validateSQLiteTargetEvolutionNoIncomingForeignKeys rejects any table that
// is referenced by another SQLite table. DROP TABLE applies incoming ON
// DELETE actions (including CASCADE, SET NULL, and SET DEFAULT) immediately;
// PRAGMA defer_foreign_keys delays constraint checks only and cannot protect
// dependent rows. This read-only admission screen is deliberately repeated
// under the writer fence immediately before DDL.
func validateSQLiteTargetEvolutionNoIncomingForeignKeys(
	ctx context.Context,
	queryer sqliteQueryer,
	table string,
) error {
	if queryer == nil {
		return fmt.Errorf("copy/swap incoming foreign-key authority queryer is not configured")
	}
	if strings.TrimSpace(table) == "" {
		return fmt.Errorf("copy/swap incoming foreign-key authority has an empty table identity")
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT name
		  FROM sqlite_schema
		 WHERE type = 'table'
		   AND name NOT LIKE 'sqlite_%'
		 ORDER BY name`)
	if err != nil {
		return fmt.Errorf("list SQLite tables for incoming foreign-key authority: %w", err)
	}
	defer rows.Close()
	var children []string
	for rows.Next() {
		var child string
		if err := rows.Scan(&child); err != nil {
			return fmt.Errorf("read SQLite table for incoming foreign-key authority: %w", err)
		}
		if strings.TrimSpace(child) == "" {
			return fmt.Errorf("SQLite incoming foreign-key authority has an empty child table identity")
		}
		children = append(children, child)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate SQLite tables for incoming foreign-key authority: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close SQLite tables for incoming foreign-key authority: %w", err)
	}
	type incoming struct {
		child    string
		id       int
		sequence int
		onDelete string
		onUpdate string
	}
	var dependencies []incoming
	for _, child := range children {
		foreignKeys, err := queryer.QueryContext(
			ctx,
			"PRAGMA foreign_key_list("+quote(child)+")",
		)
		if err != nil {
			return fmt.Errorf("inspect SQLite foreign keys for table %s: %w", child, err)
		}
		for foreignKeys.Next() {
			var (
				id, sequence                          int
				referenced, local, onUpdate, onDelete string
				match                                 string
				referencedColumn                      sql.NullString
			)
			if err := foreignKeys.Scan(
				&id,
				&sequence,
				&referenced,
				&local,
				&referencedColumn,
				&onUpdate,
				&onDelete,
				&match,
			); err != nil {
				_ = foreignKeys.Close()
				return fmt.Errorf("read SQLite foreign keys for table %s: %w", child, err)
			}
			if id < 0 || sequence < 0 || strings.TrimSpace(referenced) == "" ||
				strings.TrimSpace(local) == "" || strings.TrimSpace(onUpdate) == "" ||
				strings.TrimSpace(onDelete) == "" || strings.TrimSpace(match) == "" {
				_ = foreignKeys.Close()
				return fmt.Errorf("SQLite foreign-key authority for table %s is incomplete", child)
			}
			if stage4SQLiteIdentifier(referenced) == stage4SQLiteIdentifier(table) {
				dependencies = append(dependencies, incoming{
					child:    child,
					id:       id,
					sequence: sequence,
					onDelete: onDelete,
					onUpdate: onUpdate,
				})
			}
		}
		if err := foreignKeys.Err(); err != nil {
			_ = foreignKeys.Close()
			return fmt.Errorf("iterate SQLite foreign keys for table %s: %w", child, err)
		}
		if err := foreignKeys.Close(); err != nil {
			return fmt.Errorf("close SQLite foreign keys for table %s: %w", child, err)
		}
	}
	if len(dependencies) == 0 {
		return nil
	}
	parts := make([]string, len(dependencies))
	for index, dependency := range dependencies {
		parts[index] = fmt.Sprintf(
			"%s(id=%d,seq=%d,on_delete=%s,on_update=%s)",
			dependency.child,
			dependency.id,
			dependency.sequence,
			dependency.onDelete,
			dependency.onUpdate,
		)
	}
	return fmt.Errorf(
		"copy/swap table %s has incoming foreign-key dependencies (%s); SQLite DROP TABLE can apply their ON DELETE actions",
		table,
		strings.Join(parts, ", "),
	)
}

func (session *sqliteTargetEvolutionMutationSession) executeSQLiteTargetEvolutionCopySwap(
	ctx context.Context,
	operationIndex int,
	operation TargetSchemaEvolutionOperation,
) error {
	before, after, temporary, err := sqliteTargetEvolutionCopySwapOperation(
		session.plan,
		operationIndex,
		operation,
	)
	if err != nil {
		return fmt.Errorf("validate SQLite copy/swap operation %d: %w", operationIndex, err)
	}
	if err := validateSQLiteTargetEvolutionCopySwapRendering(after); err != nil {
		return fmt.Errorf("revalidate SQLite copy/swap reconstruction for operation %d: %w", operationIndex, err)
	}
	if err := validateSQLiteTargetEvolutionCopySwapObjects(
		ctx,
		session.queryer,
		before.Name,
		temporary,
	); err != nil {
		return fmt.Errorf("revalidate SQLite copy/swap operation %d: %w", operationIndex, err)
	}
	if err := validateSQLiteTargetEvolutionPlannedIncomingForeignKeys(
		session.plan,
		operationIndex,
		before.Name,
	); err != nil {
		return fmt.Errorf(
			"revalidate SQLite copy/swap planned incoming foreign-key authority for operation %d: %w",
			operationIndex,
			err,
		)
	}
	if err := validateSQLiteTargetEvolutionNoIncomingForeignKeys(
		ctx,
		session.queryer,
		before.Name,
	); err != nil {
		return fmt.Errorf(
			"revalidate SQLite copy/swap incoming foreign-key authority for operation %d: %w",
			operationIndex,
			err,
		)
	}
	// Deferred foreign-key checks keep outbound references valid while the
	// sealed replacement is populated. They do not make incoming references
	// safe; validateSQLiteTargetEvolutionNoIncomingForeignKeys has already
	// rejected those before the first DROP TABLE.
	if _, err := session.executor.ExecContext(ctx, "PRAGMA defer_foreign_keys = ON"); err != nil {
		return fmt.Errorf("defer SQLite foreign keys for copy/swap operation %d: %w", operationIndex, err)
	}
	retained, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
		ctx,
		session.queryer,
		before,
	)
	if err != nil {
		return fmt.Errorf("capture retained SQLite data authority before operation %d: %w", operationIndex, err)
	}
	priorSequence := sqliteTargetEvolutionSequenceAuthority{}
	if before.Identity != nil {
		priorSequence, err = sqliteTargetEvolutionReadSequence(
			ctx,
			session.queryer,
			before.Name,
			true,
		)
		if err != nil {
			return fmt.Errorf("authenticate SQLite sequence before copy/swap operation %d: %w", operationIndex, err)
		}
	}
	priorMaximum, priorMaximumKnown, err := sqliteTargetEvolutionPositiveMaximum(
		ctx,
		session.queryer,
		before,
	)
	if err != nil {
		return fmt.Errorf("read SQLite copied-row frontier before operation %d: %w", operationIndex, err)
	}
	temporaryTable := cloneStage4RichTable(after)
	temporaryTable.Name = temporary
	temporaryTable.Schema = ""
	create, err := schema.CreateTableDDL(schema.SQLite, temporaryTable)
	if err != nil {
		return fmt.Errorf("render SQLite copy/swap replacement table %s: %w", before.Name, err)
	}
	createSQL, err := schema.RenderDDLStatement(create, schema.SQLite)
	if err != nil {
		return fmt.Errorf("authenticate SQLite copy/swap replacement table %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, createSQL); err != nil {
		return fmt.Errorf("create SQLite copy/swap replacement table %s: %w", before.Name, err)
	}
	copySQL, err := sqliteTargetEvolutionCopySwapCopyStatement(before, temporary)
	if err != nil {
		return fmt.Errorf("render SQLite copy/swap copy for %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, copySQL); err != nil {
		return fmt.Errorf("copy retained SQLite rows for %s: %w", before.Name, err)
	}
	if err := sqliteTargetEvolutionVerifyCopiedRows(ctx, session.queryer, before, temporary); err != nil {
		return fmt.Errorf("verify retained SQLite rows for %s: %w", before.Name, err)
	}
	copiedAuthority, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(
		ctx,
		session.queryer,
		temporaryTable,
	)
	if err != nil {
		return fmt.Errorf("capture copied SQLite data authority for %s: %w", before.Name, err)
	}
	if !retained.sameData(copiedAuthority) {
		return fmt.Errorf("retained SQLite data authority changed during copy/swap")
	}
	copiedMaximum, copiedMaximumKnown, err := sqliteTargetEvolutionPositiveMaximum(
		ctx,
		session.queryer,
		temporaryTable,
	)
	if err != nil {
		return fmt.Errorf("read SQLite copied-row frontier after operation %d: %w", operationIndex, err)
	}
	if _, err := session.executor.ExecContext(ctx, "DROP TABLE "+quote(before.Name)); err != nil {
		return fmt.Errorf("drop replaced SQLite table %s: %w", before.Name, err)
	}
	// Do not rename the temporary table into place. SQLite validates views
	// during ALTER TABLE RENAME while the original relation is absent, and a
	// view that correctly references the final name would be rejected in that
	// transient interval. Recreate the original name from sealed after-state
	// metadata, copy from the verified temporary table, then remove it.
	finalCreate, err := schema.CreateTableDDL(schema.SQLite, after)
	if err != nil {
		return fmt.Errorf("render SQLite final copy/swap table %s: %w", before.Name, err)
	}
	finalCreateSQL, err := schema.RenderDDLStatement(finalCreate, schema.SQLite)
	if err != nil {
		return fmt.Errorf("authenticate SQLite final copy/swap table %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, finalCreateSQL); err != nil {
		return fmt.Errorf("restore SQLite copy/swap table name %s: %w", before.Name, err)
	}
	copyFinalSQL, err := sqliteTargetEvolutionCopySwapCopyStatement(temporaryTable, before.Name)
	if err != nil {
		return fmt.Errorf("render SQLite final copy/swap copy for %s: %w", before.Name, err)
	}
	if _, err := session.executor.ExecContext(ctx, copyFinalSQL); err != nil {
		return fmt.Errorf("restore retained SQLite rows for %s: %w", before.Name, err)
	}
	if err := sqliteTargetEvolutionVerifyCopiedRows(ctx, session.queryer, temporaryTable, before.Name); err != nil {
		return fmt.Errorf("verify restored SQLite rows for %s: %w", before.Name, err)
	}
	finalAuthority, err := sqliteTargetEvolutionCaptureRetainedDataAuthority(ctx, session.queryer, after)
	if err != nil {
		return fmt.Errorf("capture restored SQLite data authority for %s: %w", before.Name, err)
	}
	if !retained.sameData(finalAuthority) {
		return fmt.Errorf("retained SQLite data authority changed while restoring original table name")
	}
	if _, err := session.executor.ExecContext(ctx, "DROP TABLE "+quote(temporary)); err != nil {
		return fmt.Errorf("remove SQLite copy/swap temporary table %s: %w", temporary, err)
	}
	for _, index := range sqliteTargetEvolutionStandaloneIndexes(after.Indexes) {
		statement, renderErr := schema.SQLitePlannedIndexDDL(after, index)
		if renderErr != nil {
			return fmt.Errorf("render SQLite copy/swap index %s on %s: %w", index.Name, before.Name, renderErr)
		}
		statementSQL, renderErr := schema.RenderDDLStatement(statement, schema.SQLite)
		if renderErr != nil {
			return fmt.Errorf("authenticate SQLite copy/swap index %s on %s: %w", index.Name, before.Name, renderErr)
		}
		if _, err := session.executor.ExecContext(ctx, statementSQL); err != nil {
			return fmt.Errorf("restore SQLite copy/swap index %s on %s: %w", index.Name, before.Name, err)
		}
	}
	restoredSequence := sqliteTargetEvolutionSequenceAuthority{}
	if before.Identity != nil {
		restoredSequence, err = sqliteTargetEvolutionRestoreSequence(
			ctx,
			session.executor,
			session.queryer,
			before.Name,
			temporary,
			priorSequence,
			priorMaximum,
			priorMaximumKnown,
			copiedMaximum,
			copiedMaximumKnown,
		)
		if err != nil {
			return fmt.Errorf("restore SQLite sequence for copy/swap table %s: %w", before.Name, err)
		}
	}
	if err := sqliteTargetEvolutionAssertTemporaryObjectAbsent(
		ctx,
		session.queryer,
		temporary,
	); err != nil {
		return fmt.Errorf("verify SQLite copy/swap temporary object cleanup: %w", err)
	}
	retained.sequence = restoredSequence
	session.retained = append(session.retained, retained)
	return nil
}
