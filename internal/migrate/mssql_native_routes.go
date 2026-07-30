package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

type sqlServerDatabaseHandleProvider interface {
	sqlServerDatabaseHandle() *sql.DB
}

type sqlServerDatabaseIdentity struct {
	databaseGUID string
	database     string
}

// SQLServerToSQLServerWithObserver migrates a SQL Server 2022 schema to a
// distinct SQL Server 2022 database through the shared source/target adapter
// runner.
func SQLServerToSQLServerWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "mssql" || cfg.Target.Type != "mssql" {
		return Result{}, fmt.Errorf(
			"SQL Server-to-SQL Server requires source.type and target.type mssql",
		)
	}
	if sameConfiguredSQLServerDatabase(cfg.Source, cfg.Target) {
		return Result{}, fmt.Errorf(
			"SQL Server-to-SQL Server requires distinct source and target databases",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "mssql", target: "mssql"},
	)
}

func sameConfiguredSQLServerDatabase(
	source config.Endpoint,
	target config.Endpoint,
) bool {
	sourcePort := source.Port
	if sourcePort == 0 {
		sourcePort = 1433
	}
	targetPort := target.Port
	if targetPort == 0 {
		targetPort = 1433
	}
	return strings.EqualFold(
		strings.TrimSpace(source.Host),
		strings.TrimSpace(target.Host),
	) &&
		sourcePort == targetPort &&
		source.Database != "" &&
		strings.EqualFold(source.Database, target.Database)
}

func requireDistinctLiveSQLServerDatabases(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
) error {
	sourceProvider, sourceOK := source.(sqlServerDatabaseHandleProvider)
	targetProvider, targetOK := target.(sqlServerDatabaseHandleProvider)
	if !sourceOK || !targetOK ||
		sourceProvider.sqlServerDatabaseHandle() == nil ||
		targetProvider.sqlServerDatabaseHandle() == nil {
		return fmt.Errorf(
			"SQL Server-to-SQL Server cannot verify distinct live source and target databases",
		)
	}
	sourceIdentity, err := readSQLServerDatabaseIdentity(
		ctx,
		sourceProvider.sqlServerDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live SQL Server source: %w", err)
	}
	targetIdentity, err := readSQLServerDatabaseIdentity(
		ctx,
		targetProvider.sqlServerDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live SQL Server target: %w", err)
	}
	if sameSQLServerDatabaseIdentity(sourceIdentity, targetIdentity) {
		return fmt.Errorf(
			"SQL Server-to-SQL Server requires distinct live source and target databases",
		)
	}
	return nil
}

func readSQLServerDatabaseIdentity(
	ctx context.Context,
	database *sql.DB,
) (sqlServerDatabaseIdentity, error) {
	var databaseGUID, databaseName sql.NullString
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			CONVERT(varchar(36), recovery.database_guid),
			DB_NAME()
		   FROM sys.database_recovery_status AS recovery
		  WHERE recovery.database_id = DB_ID()`,
	).Scan(&databaseGUID, &databaseName); err != nil {
		return sqlServerDatabaseIdentity{}, err
	}
	identity := sqlServerDatabaseIdentity{
		databaseGUID: strings.TrimSpace(databaseGUID.String),
		database:     strings.TrimSpace(databaseName.String),
	}
	if !databaseGUID.Valid || identity.databaseGUID == "" ||
		!databaseName.Valid || identity.database == "" {
		return sqlServerDatabaseIdentity{}, fmt.Errorf(
			"SQL Server database GUID and selected database are required",
		)
	}
	return identity, nil
}

func sameSQLServerDatabaseIdentity(
	source sqlServerDatabaseIdentity,
	target sqlServerDatabaseIdentity,
) bool {
	return strings.EqualFold(
		source.databaseGUID,
		target.databaseGUID,
	)
}

// PostgresToSQLServerWithObserver migrates deterministic PostgreSQL 16
// metadata and rows through the shared source/target adapter runner.
func PostgresToSQLServerWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "mssql" {
		return Result{}, fmt.Errorf(
			"PostgreSQL-to-SQL Server requires source.type postgres and target.type mssql",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "postgres", target: "mssql"},
	)
}
