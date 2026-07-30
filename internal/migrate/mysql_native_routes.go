package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

type mysqlDatabaseHandleProvider interface {
	mySQLDatabaseHandle() *sql.DB
}

type mysqlDatabaseIdentity struct {
	serverUUID          string
	database            string
	replicationChannels int
	groupMembers        int
}

// MySQLToMySQLWithObserver migrates a MySQL 8 schema to a distinct MySQL 8
// database through the shared source/target adapter runner.
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
		serverUUID:          strings.TrimSpace(serverUUID.String),
		database:            selectedDatabase.String,
		replicationChannels: replicationChannels,
		groupMembers:        groupMembers,
	}
	if !serverUUID.Valid || identity.serverUUID == "" ||
		!selectedDatabase.Valid || identity.database == "" {
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"MySQL server UUID and selected database are required",
		)
	}
	if identity.replicationChannels != 0 || identity.groupMembers != 0 {
		return mysqlDatabaseIdentity{}, fmt.Errorf(
			"replicated MySQL endpoints are unsupported for native MySQL-to-MySQL migration",
		)
	}
	return identity, nil
}

func sameMySQLDatabaseIdentity(
	source mysqlDatabaseIdentity,
	target mysqlDatabaseIdentity,
) bool {
	return strings.EqualFold(source.serverUUID, target.serverUUID) &&
		source.database == target.database
}

// PostgresToMySQLWithObserver migrates deterministic PostgreSQL 16 metadata
// and rows through the shared source/target adapter runner.
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
