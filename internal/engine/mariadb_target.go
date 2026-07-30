package engine

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type mariaDB1011TargetServerCatalog struct {
	source                mariaDB1011SourceServerCatalog
	foreignKeyChecks      int
	uniqueChecks          int
	checkConstraintChecks int
	innodbPageSize        int64
	innodbForcePrimaryKey int
}

const mariaDB1011TargetServerCatalogQuery = `
	SELECT
		VERSION(),
		@@version_comment,
		@@session.sql_mode,
		@@session.time_zone,
		@@system_time_zone,
		@@session.auto_increment_increment,
		@@session.auto_increment_offset,
		@@lower_case_table_names,
		@@explicit_defaults_for_timestamp,
		@@session.foreign_key_checks,
		@@session.unique_checks,
		@@session.check_constraint_checks,
		@@innodb_page_size,
		@@innodb_force_primary_key
`

// OpenMariaDB1011Target opens a MariaDB 10.11 native target with all supported
// constraint checks and explicit zero-valued AUTO_INCREMENT identities pinned
// on its sole pooled session. MariaDB must never receive Oracle MySQL's
// information_schema_stats_expiry connection parameter.
func OpenMariaDB1011Target(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	probe, err := OpenMySQL(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	sourceCatalog, err := readMariaDB1011SourceServerCatalog(ctx, probe)
	if err == nil {
		err = validateMariaDB1011SourceServerCatalog(sourceCatalog)
	}
	if err != nil {
		_ = probe.Close()
		return nil, fmt.Errorf(
			"verify MariaDB 10.11 target connection: %w",
			err,
		)
	}
	sqlMode, err := mariaDB1011TargetSQLMode(sourceCatalog.sqlMode)
	if err != nil {
		_ = probe.Close()
		return nil, err
	}
	if err := probe.Close(); err != nil {
		return nil, fmt.Errorf(
			"close MariaDB 10.11 target verification connection: %w",
			err,
		)
	}

	database, err := openMySQLWithSessionParams(
		ctx,
		endpoint,
		false,
		map[string]string{
			"check_constraint_checks": "1",
			"foreign_key_checks":      "1",
			"sql_mode":                sqlMode,
			"unique_checks":           "1",
		},
	)
	if err != nil {
		return nil, err
	}
	// MariaDB has no information_schema_stats_expiry session variable. Pin the
	// already-opened connection directly instead of abusing that Oracle-only
	// DSN switch merely to obtain the native adapter's single-session pool.
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	if err := VerifyMariaDB1011Target(ctx, database); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf(
			"verify MariaDB 10.11 target session: %w",
			err,
		)
	}
	return database, nil
}

// VerifyMariaDB1011Target verifies the MariaDB source contract plus every
// session and server invariant required by the native target lifecycle.
func VerifyMariaDB1011Target(
	ctx context.Context,
	database *sql.DB,
) error {
	catalog, err := readMariaDB1011TargetServerCatalog(ctx, database)
	if err != nil {
		return err
	}
	return validateMariaDB1011TargetServerCatalog(catalog)
}

func readMariaDB1011TargetServerCatalog(
	ctx context.Context,
	database *sql.DB,
) (mariaDB1011TargetServerCatalog, error) {
	var catalog mariaDB1011TargetServerCatalog
	err := database.QueryRowContext(
		ctx,
		mariaDB1011TargetServerCatalogQuery,
	).Scan(
		&catalog.source.version,
		&catalog.source.versionComment,
		&catalog.source.sqlMode,
		&catalog.source.sessionTimeZone,
		&catalog.source.systemTimeZone,
		&catalog.source.autoIncrementIncrement,
		&catalog.source.autoIncrementOffset,
		&catalog.source.lowerCaseTableNames,
		&catalog.source.explicitTimestampDefaults,
		&catalog.foreignKeyChecks,
		&catalog.uniqueChecks,
		&catalog.checkConstraintChecks,
		&catalog.innodbPageSize,
		&catalog.innodbForcePrimaryKey,
	)
	if err != nil {
		return mariaDB1011TargetServerCatalog{}, fmt.Errorf(
			"read MariaDB 10.11 target version and session contract: %w",
			err,
		)
	}
	return catalog, nil
}

func validateMariaDB1011TargetServerCatalog(
	catalog mariaDB1011TargetServerCatalog,
) error {
	if err := validateMariaDB1011SourceServerCatalog(catalog.source); err != nil {
		return err
	}
	modes := mysqlSQLModes(catalog.source.sqlMode)
	if !modes["NO_AUTO_VALUE_ON_ZERO"] {
		return mariaDB1011TargetPolicy(
			"SQL mode",
			"required mode NO_AUTO_VALUE_ON_ZERO is absent",
		)
	}
	if catalog.foreignKeyChecks != 1 ||
		catalog.uniqueChecks != 1 ||
		catalog.checkConstraintChecks != 1 {
		return mariaDB1011TargetPolicy(
			"constraint enforcement",
			fmt.Sprintf(
				"foreign_key_checks=%d unique_checks=%d check_constraint_checks=%d; all must be enabled",
				catalog.foreignKeyChecks,
				catalog.uniqueChecks,
				catalog.checkConstraintChecks,
			),
		)
	}
	if catalog.innodbPageSize != 16_384 {
		return mariaDB1011TargetPolicy(
			"InnoDB page size",
			fmt.Sprintf(
				"innodb_page_size=%d; 16384 is required",
				catalog.innodbPageSize,
			),
		)
	}
	if catalog.innodbForcePrimaryKey != 0 {
		return mariaDB1011TargetPolicy(
			"primary-key policy",
			fmt.Sprintf(
				"innodb_force_primary_key=%d; value 0 is required",
				catalog.innodbForcePrimaryKey,
			),
		)
	}
	return nil
}

func mariaDB1011TargetSQLMode(value string) (string, error) {
	modes := mysqlSQLModes(value)
	modes["NO_AUTO_VALUE_ON_ZERO"] = true
	names := make([]string, 0, len(modes))
	for mode := range modes {
		if mode == "" {
			continue
		}
		for _, character := range mode {
			if (character < 'A' || character > 'Z') &&
				character != '_' {
				return "", fmt.Errorf(
					"configure MariaDB 10.11 target SQL mode: invalid mode %q",
					mode,
				)
			}
		}
		names = append(names, mode)
	}
	sort.Strings(names)
	return "'" + strings.Join(names, ",") + "'", nil
}

func mariaDB1011TargetPolicy(operation, value string) error {
	return &schema.PolicyError{
		Operation: "verify MariaDB 10.11 target " + operation,
		Type:      value,
		Target:    string(schema.MySQL),
	}
}
