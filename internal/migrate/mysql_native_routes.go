package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

type mysqlDatabaseHandleProvider interface {
	mySQLDatabaseHandle() *sql.DB
}

type mysqlDatabaseIdentity struct {
	flavor         engine.MySQLServerFlavor
	serverIdentity string
	database       string
}

// MySQLToMySQLWithObserver migrates a version-pinned MySQL-family schema to a
// distinct, compatible MySQL-family database through the shared adapter
// runner. Cross-flavor metadata must pass the target's exact planning policy.
func MySQLToMySQLWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mysql" || cfg.Target.Type != "mysql" {
		return Result{}, fmt.Errorf(
			"MySQL-to-MySQL requires source.type and target.type mysql",
		)
	}
	if sameConfiguredMySQLDatabase(cfg.Source, cfg.Target) {
		return Result{}, fmt.Errorf(
			"MySQL-to-MySQL requires distinct source and target databases",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mysql", target: "mysql"},
	)
}

func sameConfiguredMySQLDatabase(
	source config.Endpoint,
	target config.Endpoint,
) bool {
	sourcePort := source.Port
	if sourcePort == 0 {
		sourcePort = 3306
	}
	targetPort := target.Port
	if targetPort == 0 {
		targetPort = 3306
	}
	return strings.EqualFold(
		strings.TrimSpace(source.Host),
		strings.TrimSpace(target.Host),
	) &&
		sourcePort == targetPort &&
		source.Database != "" &&
		source.Database == target.Database
}

func requireDistinctLiveMySQLDatabases(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
) error {
	sourceProvider, sourceOK := source.(mysqlDatabaseHandleProvider)
	targetProvider, targetOK := target.(mysqlDatabaseHandleProvider)
	if !sourceOK || !targetOK ||
		sourceProvider.mySQLDatabaseHandle() == nil ||
		targetProvider.mySQLDatabaseHandle() == nil {
		return fmt.Errorf(
			"MySQL-to-MySQL cannot verify distinct live source and target databases",
		)
	}
	sourceIdentity, err := readMySQLDatabaseIdentity(
		ctx,
		sourceProvider.mySQLDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live MySQL source: %w", err)
	}
	targetIdentity, err := readMySQLDatabaseIdentity(
		ctx,
		targetProvider.mySQLDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live MySQL target: %w", err)
	}
	if sameMySQLDatabaseIdentity(sourceIdentity, targetIdentity) {
		return fmt.Errorf(
			"MySQL-to-MySQL requires distinct live source and target databases",
		)
	}
	return nil
}

func readMySQLDatabaseIdentity(
	ctx context.Context,
	database *sql.DB,
) (mysqlDatabaseIdentity, error) {
	flavor, err := engine.DetectMySQLServerFlavor(ctx, database)
	if err != nil {
		return mysqlDatabaseIdentity{}, err
	}
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		return readOracleMySQLDatabaseIdentity(ctx, database)
	case engine.MySQLServerFlavorMariaDB1011:
		return readMariaDBDatabaseIdentity(ctx, database)
	default:
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"unsupported MySQL server flavor for database identity",
		)
	}
}

func readOracleMySQLDatabaseIdentity(
	ctx context.Context,
	database *sql.DB,
) (mysqlDatabaseIdentity, error) {
	var serverUUID, selectedDatabase sql.NullString
	var replicationChannels, groupMembers int
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			@@server_uuid,
			DATABASE(),
			(SELECT COUNT(*)
			 FROM performance_schema.replication_connection_configuration),
			(SELECT COUNT(*)
			 FROM performance_schema.replication_group_members)`,
	).Scan(
		&serverUUID,
		&selectedDatabase,
		&replicationChannels,
		&groupMembers,
	); err != nil {
		return mysqlDatabaseIdentity{}, err
	}
	identity := mysqlDatabaseIdentity{
		flavor:         engine.MySQLServerFlavorOracle80,
		serverIdentity: strings.TrimSpace(serverUUID.String),
		database:       selectedDatabase.String,
	}
	if !serverUUID.Valid || identity.serverIdentity == "" ||
		!selectedDatabase.Valid || identity.database == "" {
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"MySQL server UUID and selected database are required",
		)
	}
	if replicationChannels != 0 || groupMembers != 0 {
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"replicated MySQL endpoints are unsupported for native MySQL-to-MySQL migration",
		)
	}
	return identity, nil
}

func readMariaDBDatabaseIdentity(
	ctx context.Context,
	database *sql.DB,
) (mysqlDatabaseIdentity, error) {
	var serverUID, selectedDatabase sql.NullString
	var wsrepOn int
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			@@global.server_uid,
			DATABASE(),
			@@global.wsrep_on`,
	).Scan(
		&serverUID,
		&selectedDatabase,
		&wsrepOn,
	); err != nil {
		return mysqlDatabaseIdentity{}, err
	}
	replicationChannels, err := countMariaDBReplicationChannels(
		ctx,
		database,
	)
	if err != nil {
		return mysqlDatabaseIdentity{}, err
	}
	return mariaDBDatabaseIdentityFromCatalog(
		serverUID,
		selectedDatabase,
		wsrepOn,
		replicationChannels,
	)
}

func mariaDBDatabaseIdentityFromCatalog(
	serverUID sql.NullString,
	selectedDatabase sql.NullString,
	wsrepOn int,
	replicationChannels int,
) (mysqlDatabaseIdentity, error) {
	identity := mysqlDatabaseIdentity{
		flavor:         engine.MySQLServerFlavorMariaDB1011,
		serverIdentity: strings.TrimSpace(serverUID.String),
		database:       selectedDatabase.String,
	}
	if !serverUID.Valid || identity.serverIdentity == "" ||
		!selectedDatabase.Valid || identity.database == "" {
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"MariaDB server UID and selected database are required",
		)
	}
	if wsrepOn != 0 || replicationChannels != 0 {
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"replicated MariaDB endpoints are unsupported for native MySQL-to-MySQL migration",
		)
	}
	return identity, nil
}

func countMariaDBReplicationChannels(
	ctx context.Context,
	database *sql.DB,
) (count int, result error) {
	rows, err := database.QueryContext(ctx, "SHOW ALL SLAVES STATUS")
	if err != nil {
		return 0, fmt.Errorf("inspect MariaDB replication channels: %w", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && result == nil {
			result = fmt.Errorf(
				"close MariaDB replication channel catalog: %w",
				closeErr,
			)
		}
	}()

	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf(
			"inspect MariaDB replication channel shape: %w",
			err,
		)
	}
	if err := validateMariaDBReplicationStatusColumns(columns); err != nil {
		return 0, err
	}
	values := make([]sql.RawBytes, len(columns))
	destinations := make([]any, len(columns))
	for index := range values {
		destinations[index] = &values[index]
	}
	for rows.Next() {
		if err := rows.Scan(destinations...); err != nil {
			return 0, fmt.Errorf(
				"read MariaDB replication channel: %w",
				err,
			)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf(
			"iterate MariaDB replication channels: %w",
			err,
		)
	}
	return count, nil
}

func validateMariaDBReplicationStatusColumns(columns []string) error {
	seen := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		key := strings.ToLower(strings.TrimSpace(column))
		if key == "" {
			return fmt.Errorf(
				"unexpected MariaDB replication channel catalog shape",
			)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf(
				"unexpected MariaDB replication channel catalog shape",
			)
		}
		seen[key] = struct{}{}
	}
	for _, required := range []string{
		"connection_name",
		"master_host",
		"slave_io_running",
		"slave_sql_running",
	} {
		if _, present := seen[required]; !present {
			return fmt.Errorf(
				"unexpected MariaDB replication channel catalog shape",
			)
		}
	}
	return nil
}

func sameMySQLDatabaseIdentity(
	source mysqlDatabaseIdentity,
	target mysqlDatabaseIdentity,
) bool {
	if source.flavor != target.flavor ||
		source.database != target.database {
		return false
	}
	switch source.flavor {
	case engine.MySQLServerFlavorOracle80:
		return strings.EqualFold(
			source.serverIdentity,
			target.serverIdentity,
		)
	case engine.MySQLServerFlavorMariaDB1011:
		return source.serverIdentity == target.serverIdentity
	default:
		return false
	}
}

// PostgresToMySQLWithObserver migrates deterministic PostgreSQL 16 metadata
// and rows to an admitted Oracle MySQL or MariaDB target through the shared
// source/target adapter runner.
func PostgresToMySQLWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "mysql" {
		return Result{}, fmt.Errorf(
			"PostgreSQL-to-MySQL requires source.type postgres and target.type mysql",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "postgres", target: "mysql"},
	)
}
