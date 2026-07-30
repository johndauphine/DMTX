package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

const mysqlTargetCleanupTimeout = 5 * time.Second

func prepareMySQLTargets(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	ordered := append([]schema.Table(nil), tables...)
	sort.Slice(ordered, func(left, right int) bool {
		return adapterSourceTableKey(
			ordered[left].Schema,
			ordered[left].Name,
		) < adapterSourceTableKey(
			ordered[right].Schema,
			ordered[right].Name,
		)
	})

	drops := make([]string, len(ordered))
	creates := make([]string, len(ordered))
	for index, table := range ordered {
		var err error
		drops[index], err = schema.DropTable(schema.MySQL, table)
		if err != nil {
			return fmt.Errorf(
				"plan MySQL table %s drop: %w",
				table.Name,
				err,
			)
		}
		creates[index], err = schema.CreateTable(schema.MySQL, table)
		if err != nil {
			return fmt.Errorf(
				"plan MySQL table %s: %w",
				table.Name,
				err,
			)
		}
	}

	return withMySQLForeignKeyChecksDisabled(
		ctx,
		database,
		func(connection *sql.Conn) error {
			for index, statement := range drops {
				if _, err := connection.ExecContext(
					ctx,
					statement,
				); err != nil {
					return newMySQLSafeOperationError(
						"drop MySQL table",
						ordered[index].Name,
						err,
					)
				}
			}
			for index, statement := range creates {
				if _, err := connection.ExecContext(
					ctx,
					statement,
				); err != nil {
					return newMySQLSafeOperationError(
						"create MySQL table",
						ordered[index].Name,
						err,
					)
				}
			}
			return nil
		},
	)
}

func withMySQLForeignKeyChecksDisabled(
	ctx context.Context,
	database *sql.DB,
	operation func(*sql.Conn) error,
) (result error) {
	connection, err := database.Conn(ctx)
	if err != nil {
		return newMySQLSafeOperationError(
			"acquire MySQL target connection for",
			"schema preparation",
			err,
		)
	}
	defer connection.Close()

	var enabled int
	if err := connection.QueryRowContext(
		ctx,
		"SELECT @@SESSION.FOREIGN_KEY_CHECKS",
	).Scan(&enabled); err != nil {
		return newMySQLSafeOperationError(
			"inspect MySQL foreign-key checks for",
			"schema preparation",
			err,
		)
	}
	if enabled != 1 {
		return fmt.Errorf(
			"prepare MySQL target: session FOREIGN_KEY_CHECKS must begin enabled",
		)
	}
	if _, err := connection.ExecContext(
		ctx,
		"SET SESSION FOREIGN_KEY_CHECKS = 0",
	); err != nil {
		return newMySQLSafeOperationError(
			"disable MySQL foreign-key checks for",
			"schema preparation",
			err,
		)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			mysqlTargetCleanupTimeout,
		)
		defer cancel()
		_, cleanupErr := connection.ExecContext(
			cleanupContext,
			"SET SESSION FOREIGN_KEY_CHECKS = 1",
		)
		if cleanupErr == nil {
			return
		}
		discardMySQLConnection(connection)
		safeCleanup := newMySQLSafeOperationError(
			"restore MySQL foreign-key checks after",
			"schema preparation",
			cleanupErr,
		)
		if result == nil {
			result = safeCleanup
			return
		}
		result = errors.Join(result, safeCleanup)
	}()
	return operation(connection)
}

func finalizeMySQLTargets(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
	mode string,
) error {
	var objectPlan []schema.MySQLObjectStatement
	if mode == "drop_recreate" {
		var err error
		objectPlan, err = schema.PlanMySQLDropRecreateObjects(tables)
		if err != nil {
			return fmt.Errorf("plan MySQL post-load objects: %w", err)
		}
	}
	for _, table := range tables {
		if table.Identity == nil {
			continue
		}
		frontier := int64(0)
		if table.Identity.Frontier != nil {
			frontier = *table.Identity.Frontier
		}
		if _, err := schema.MySQLAutoIncrementPlan(
			table,
			frontier,
		); err != nil {
			return fmt.Errorf(
				"plan MySQL table %s identity: %w",
				table.Name,
				err,
			)
		}
	}

	for _, statement := range objectPlan {
		if _, err := database.ExecContext(
			ctx,
			statement.SQL,
		); err != nil {
			return newMySQLSafeOperationError(
				"create MySQL post-load object on",
				statement.Table,
				err,
			)
		}
	}
	return finalizeMySQLIdentityFrontiers(
		ctx,
		database,
		tables,
	)
}

func finalizeMySQLIdentityFrontiers(
	ctx context.Context,
	database *sql.DB,
	tables []schema.Table,
) error {
	identityTables := make([]schema.Table, 0, len(tables))
	for _, table := range tables {
		if table.Identity != nil {
			identityTables = append(identityTables, table)
		}
	}
	sort.Slice(identityTables, func(left, right int) bool {
		return adapterSourceTableKey(
			identityTables[left].Schema,
			identityTables[left].Name,
		) < adapterSourceTableKey(
			identityTables[right].Schema,
			identityTables[right].Name,
		)
	})
	for _, table := range identityTables {
		if err := finalizeMySQLIdentityFrontier(
			ctx,
			database,
			table,
		); err != nil {
			return err
		}
	}
	return nil
}

func finalizeMySQLIdentityFrontier(
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) (result error) {
	connection, err := database.Conn(ctx)
	if err != nil {
		return newMySQLSafeOperationError(
			"acquire MySQL identity connection for",
			table.Name,
			err,
		)
	}
	defer connection.Close()

	if _, err := connection.ExecContext(
		ctx,
		"LOCK TABLES "+mySQLQualified(table.Schema, table.Name)+" WRITE",
	); err != nil {
		return newMySQLSafeOperationError(
			"lock MySQL identity table",
			table.Name,
			err,
		)
	}
	defer func() {
		cleanupContext, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			mysqlTargetCleanupTimeout,
		)
		defer cancel()
		_, cleanupErr := connection.ExecContext(
			cleanupContext,
			"UNLOCK TABLES",
		)
		if cleanupErr == nil {
			return
		}
		discardMySQLConnection(connection)
		safeCleanup := newMySQLSafeOperationError(
			"unlock MySQL identity table",
			table.Name,
			cleanupErr,
		)
		if result == nil {
			result = safeCleanup
			return
		}
		result = errors.Join(result, safeCleanup)
	}()

	var targetMaximum sql.NullInt64
	if err := connection.QueryRowContext(
		ctx,
		"SELECT MAX("+mySQLIdentifier(table.Identity.Column)+
			") FROM "+mySQLQualified(table.Schema, table.Name),
	).Scan(&targetMaximum); err != nil {
		return newMySQLSafeOperationError(
			"read MySQL identity maximum for",
			table.Name,
			err,
		)
	}
	var currentNext sql.NullInt64
	if err := connection.QueryRowContext(
		ctx,
		`SELECT AUTO_INCREMENT
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
		table.Schema,
		table.Name,
	).Scan(&currentNext); err != nil {
		return newMySQLSafeOperationError(
			"read MySQL identity frontier for",
			table.Name,
			err,
		)
	}
	frontier, err := mySQLIdentityFrontier(
		table.Identity.Frontier,
		targetMaximum,
		currentNext,
	)
	if err != nil {
		return fmt.Errorf(
			"plan MySQL table %s identity frontier: %w",
			table.Name,
			err,
		)
	}
	statement, err := schema.MySQLAutoIncrementPlan(table, frontier)
	if err != nil {
		return fmt.Errorf(
			"plan MySQL table %s identity: %w",
			table.Name,
			err,
		)
	}
	if statement.SQL == "" {
		return nil
	}
	if _, err := connection.ExecContext(
		ctx,
		statement.SQL,
		statement.Args...,
	); err != nil {
		return newMySQLSafeOperationError(
			"reset MySQL identity frontier for",
			table.Name,
			err,
		)
	}
	return nil
}

func discardMySQLConnection(connection *sql.Conn) {
	_ = connection.Raw(func(any) error {
		return driver.ErrBadConn
	})
}

func mySQLIdentityFrontier(
	source *int64,
	targetMaximum sql.NullInt64,
	currentNext sql.NullInt64,
) (int64, error) {
	if source != nil && *source < 0 {
		return 0, fmt.Errorf("source identity frontier cannot be negative")
	}
	frontier := int64(0)
	if source != nil && *source > frontier {
		frontier = *source
	}
	if targetMaximum.Valid && targetMaximum.Int64 > frontier {
		frontier = targetMaximum.Int64
	}
	if currentNext.Valid {
		if currentNext.Int64 < 1 {
			return 0, fmt.Errorf(
				"target AUTO_INCREMENT next value must be positive",
			)
		}
		currentFrontier := currentNext.Int64 - 1
		if currentFrontier > frontier {
			frontier = currentFrontier
		}
	}
	return frontier, nil
}
