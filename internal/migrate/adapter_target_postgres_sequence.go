package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	"github.com/johndauphine/dmtx/internal/schema"
)

type postgresQueryRower interface {
	QueryRowContext(
		context.Context,
		string,
		...any,
	) *sql.Row
}

type postgresIdentitySequenceState struct {
	objectID    int64
	namespace   string
	name        string
	persistence string
	dataType    string
	start       int64
	increment   int64
	minimum     int64
	maximum     int64
	cache       int64
	cycle       bool
	lastValue   sql.NullInt64
	canRead     bool
	canUpdate   bool
	canAlter    bool
}

const postgresIdentitySequenceCatalogQuery = `
	SELECT
		sequence_relation.oid::bigint,
		sequence_namespace.nspname::text,
		sequence_relation.relname::text,
		sequence_relation.relpersistence::text,
		pg_catalog.format_type(sequence.seqtypid, NULL),
		sequence.seqstart,
		sequence.seqincrement,
		sequence.seqmin,
		sequence.seqmax,
		sequence.seqcache,
		sequence.seqcycle,
		sequence_view.last_value,
		pg_catalog.has_sequence_privilege(
			current_user,
			sequence_relation.oid,
			'SELECT'
		),
		pg_catalog.has_sequence_privilege(
			current_user,
			sequence_relation.oid,
			'UPDATE'
		),
		pg_catalog.pg_has_role(
			sequence_relation.relowner,
			'USAGE'
		)
	FROM pg_catalog.pg_class AS table_relation
	JOIN pg_catalog.pg_namespace AS table_namespace
	  ON table_namespace.oid = table_relation.relnamespace
	JOIN pg_catalog.pg_attribute AS attribute
	  ON attribute.attrelid = table_relation.oid
	JOIN pg_catalog.pg_depend AS dependency
	  ON dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
	 AND dependency.refobjid = table_relation.oid
	 AND dependency.refobjsubid = attribute.attnum
	 AND dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
	 AND dependency.objsubid = 0
	 AND dependency.deptype = 'i'
	JOIN pg_catalog.pg_class AS sequence_relation
	  ON sequence_relation.oid = dependency.objid
	 AND sequence_relation.relkind = 'S'
	JOIN pg_catalog.pg_namespace AS sequence_namespace
	  ON sequence_namespace.oid = sequence_relation.relnamespace
	JOIN pg_catalog.pg_sequence AS sequence
	  ON sequence.seqrelid = sequence_relation.oid
	LEFT JOIN pg_catalog.pg_sequences AS sequence_view
	  ON sequence_view.schemaname = sequence_namespace.nspname
	 AND sequence_view.sequencename = sequence_relation.relname
	WHERE table_namespace.nspname = $1
	  AND table_relation.relname = $2
	  AND table_relation.relkind = 'r'
	  AND attribute.attname = $3
	  AND attribute.attidentity = 'd'
	  AND attribute.attnum > 0
	  AND NOT attribute.attisdropped
`

func readPostgresIdentitySequenceState(
	ctx context.Context,
	database postgresQueryRower,
	table schema.Table,
) (postgresIdentitySequenceState, error) {
	if table.Identity == nil {
		return postgresIdentitySequenceState{}, fmt.Errorf(
			"PostgreSQL identity column is not configured for table %s",
			table.Name,
		)
	}
	identityColumn := table.Identity.Column
	var state postgresIdentitySequenceState
	err := database.QueryRowContext(
		ctx,
		postgresIdentitySequenceCatalogQuery,
		table.Schema,
		table.Name,
		identityColumn,
	).Scan(
		&state.objectID,
		&state.namespace,
		&state.name,
		&state.persistence,
		&state.dataType,
		&state.start,
		&state.increment,
		&state.minimum,
		&state.maximum,
		&state.cache,
		&state.cycle,
		&state.lastValue,
		&state.canRead,
		&state.canUpdate,
		&state.canAlter,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return postgresIdentitySequenceState{}, fmt.Errorf(
			"PostgreSQL table %s identity sequence is missing or is not owned by column %s",
			table.Name,
			identityColumn,
		)
	}
	if err != nil {
		return postgresIdentitySequenceState{}, fmt.Errorf(
			"inspect PostgreSQL table %s identity sequence: %w",
			table.Name,
			err,
		)
	}
	return state, nil
}

func validatePostgresIdentitySequenceState(
	table schema.Table,
	state postgresIdentitySequenceState,
) error {
	if state.objectID <= 0 ||
		state.namespace == "" ||
		state.name == "" ||
		state.persistence != "p" ||
		state.dataType != "bigint" ||
		state.start != 1 ||
		state.increment != 1 ||
		state.minimum != 1 ||
		state.maximum != math.MaxInt64 ||
		state.cache != 1 ||
		state.cycle ||
		!state.canRead ||
		!state.canUpdate ||
		!state.canAlter {
		return fmt.Errorf(
			"PostgreSQL table %s identity sequence is not a permanent, owned BIGINT sequence with start 1, increment 1, bounds 1..%d, cache 1, no cycle, and required SELECT/UPDATE/ALTER authority",
			table.Name,
			int64(math.MaxInt64),
		)
	}
	if state.lastValue.Valid &&
		(state.lastValue.Int64 < state.minimum ||
			state.lastValue.Int64 > state.maximum) {
		return fmt.Errorf(
			"PostgreSQL table %s identity sequence last value is outside supported bounds",
			table.Name,
		)
	}
	return nil
}

func preflightPostgresIdentitySequence(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) error {
	state, err := readPostgresIdentitySequenceState(
		ctx,
		database,
		table,
	)
	if err != nil {
		return err
	}
	if err := validatePostgresIdentitySequenceState(
		table,
		state,
	); err != nil {
		return err
	}
	return nil
}

func postgresIdentitySequenceFrontier(
	sourceSequence *int64,
	targetMaximum sql.NullInt64,
	currentSequence sql.NullInt64,
) (int64, bool, error) {
	if sourceSequence != nil && *sourceSequence < 0 {
		return 0, false, fmt.Errorf(
			"source identity frontier cannot be negative",
		)
	}
	frontier := int64(0)
	if sourceSequence != nil && *sourceSequence > frontier {
		frontier = *sourceSequence
	}
	if targetMaximum.Valid && targetMaximum.Int64 > frontier {
		frontier = targetMaximum.Int64
	}
	if currentSequence.Valid && currentSequence.Int64 > frontier {
		frontier = currentSequence.Int64
	}
	if frontier <= 0 {
		return 0, false, nil
	}
	return frontier, true, nil
}

func postgresIdentitySequenceLockStatement(
	state postgresIdentitySequenceState,
) string {
	return "ALTER SEQUENCE " +
		postgresQualified(state.namespace, state.name) +
		" NO CYCLE"
}

func postgresIdentitySequenceRestartStatement(
	state postgresIdentitySequenceState,
	frontier int64,
) string {
	return "ALTER SEQUENCE " +
		postgresQualified(state.namespace, state.name) +
		" RESTART WITH " +
		strconv.FormatInt(frontier, 10)
}

func postgresIdentityTableLockStatement(table schema.Table) string {
	return "LOCK TABLE " +
		postgresQualified(table.Schema, table.Name) +
		" IN SHARE ROW EXCLUSIVE MODE"
}

func finalizePostgresIdentitySequences(
	ctx context.Context,
	transaction *sql.Tx,
	tables []schema.Table,
) error {
	identityTables := make([]schema.Table, 0, len(tables))
	for _, table := range tables {
		if table.Identity != nil {
			identityTables = append(identityTables, table)
		}
	}
	sort.Slice(identityTables, func(left, right int) bool {
		if identityTables[left].Schema != identityTables[right].Schema {
			return identityTables[left].Schema < identityTables[right].Schema
		}
		return identityTables[left].Name < identityTables[right].Name
	})

	// Lock every identity table in a deterministic order before taking any
	// sequence lock. This prevents explicit identity writes from landing
	// between MAX(id) and sequence restart and avoids table/sequence lock
	// inversion with ordinary INSERT transactions.
	for _, table := range identityTables {
		if _, err := transaction.ExecContext(
			ctx,
			postgresIdentityTableLockStatement(table),
		); err != nil {
			return fmt.Errorf(
				"lock PostgreSQL table %s for identity finalization: %w",
				table.Name,
				err,
			)
		}
	}

	for _, table := range identityTables {
		initialState, err := readPostgresIdentitySequenceState(
			ctx,
			transaction,
			table,
		)
		if err != nil {
			return err
		}
		if err := validatePostgresIdentitySequenceState(
			table,
			initialState,
		); err != nil {
			return err
		}
		if _, err := transaction.ExecContext(
			ctx,
			postgresIdentitySequenceLockStatement(initialState),
		); err != nil {
			return fmt.Errorf(
				"lock PostgreSQL table %s identity sequence: %w",
				table.Name,
				err,
			)
		}
		state, err := readPostgresIdentitySequenceState(
			ctx,
			transaction,
			table,
		)
		if err != nil {
			return err
		}
		if err := validatePostgresIdentitySequenceState(table, state); err != nil {
			return err
		}
		if state.objectID != initialState.objectID {
			return fmt.Errorf(
				"PostgreSQL table %s identity sequence changed while acquiring its restart lock",
				table.Name,
			)
		}

		var targetMaximum sql.NullInt64
		if err := transaction.QueryRowContext(
			ctx,
			"SELECT MAX("+postgresIdentifier(table.Identity.Column)+
				") FROM "+postgresQualified(table.Schema, table.Name),
		).Scan(&targetMaximum); err != nil {
			return fmt.Errorf(
				"read PostgreSQL table %s identity maximum: %w",
				table.Name,
				err,
			)
		}
		frontier, set, err := postgresIdentitySequenceFrontier(
			table.Identity.Frontier,
			targetMaximum,
			state.lastValue,
		)
		if err != nil {
			return fmt.Errorf(
				"plan PostgreSQL table %s identity frontier: %w",
				table.Name,
				err,
			)
		}
		if !set {
			continue
		}
		if _, err := transaction.ExecContext(
			ctx,
			postgresIdentitySequenceRestartStatement(state, frontier),
		); err != nil {
			return fmt.Errorf(
				"restart PostgreSQL table %s identity sequence: %w",
				table.Name,
				err,
			)
		}
		var applied int64
		if err := transaction.QueryRowContext(
			ctx,
			`SELECT pg_catalog.nextval($1::oid::pg_catalog.regclass)`,
			state.objectID,
		).Scan(&applied); err != nil {
			return fmt.Errorf(
				"advance PostgreSQL table %s identity sequence to its restart frontier: %w",
				table.Name,
				err,
			)
		}
		if applied != frontier {
			return fmt.Errorf(
				"restart PostgreSQL table %s identity sequence returned an unexpected frontier",
				table.Name,
			)
		}
	}
	return nil
}
