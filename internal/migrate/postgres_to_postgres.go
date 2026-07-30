package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

type postgresDatabaseHandleProvider interface {
	postgresDatabaseHandle() *sql.DB
}

type postgresDatabaseIdentity struct {
	systemIdentifier string
	databaseOID      uint32
	database         string
}

// PostgresToPostgresWithObserver migrates a PostgreSQL schema to a distinct
// PostgreSQL endpoint through the shared source/target adapter runner.
func PostgresToPostgresWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "postgres" || cfg.Target.Type != "postgres" {
		return Result{}, fmt.Errorf(
			"PostgreSQL-to-PostgreSQL requires source.type and target.type postgres",
		)
	}
	if sameConfiguredPostgresDatabase(cfg.Source, cfg.Target) {
		return Result{}, fmt.Errorf(
			"PostgreSQL-to-PostgreSQL requires distinct source and target databases",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "postgres", target: "postgres"},
	)
}

func sameConfiguredPostgresDatabase(
	source config.Endpoint,
	target config.Endpoint,
) bool {
	sourcePort := source.Port
	if sourcePort == 0 {
		sourcePort = 5432
	}
	targetPort := target.Port
	if targetPort == 0 {
		targetPort = 5432
	}
	return strings.EqualFold(
		strings.TrimSpace(source.Host),
		strings.TrimSpace(target.Host),
	) &&
		sourcePort == targetPort &&
		source.Database != "" &&
		source.Database == target.Database
}

func requireDistinctLivePostgresDatabases(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
) error {
	sourceProvider, sourceOK := source.(postgresDatabaseHandleProvider)
	targetProvider, targetOK := target.(postgresDatabaseHandleProvider)
	if !sourceOK || !targetOK ||
		sourceProvider.postgresDatabaseHandle() == nil ||
		targetProvider.postgresDatabaseHandle() == nil {
		return fmt.Errorf(
			"PostgreSQL-to-PostgreSQL cannot verify distinct live source and target databases",
		)
	}
	sourceIdentity, err := readPostgresDatabaseIdentity(
		ctx,
		sourceProvider.postgresDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live PostgreSQL source: %w", err)
	}
	targetIdentity, err := readPostgresDatabaseIdentity(
		ctx,
		targetProvider.postgresDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live PostgreSQL target: %w", err)
	}
	if samePostgresDatabaseIdentity(sourceIdentity, targetIdentity) {
		return fmt.Errorf(
			"PostgreSQL-to-PostgreSQL requires distinct live source and target databases",
		)
	}
	return nil
}

func readPostgresDatabaseIdentity(
	ctx context.Context,
	database *sql.DB,
) (postgresDatabaseIdentity, error) {
	var (
		systemIdentifier sql.NullString
		databaseName     sql.NullString
		databaseOID      int64
	)
	if err := database.QueryRowContext(
		ctx,
		`SELECT
			control.system_identifier::text,
			selected.datname,
			selected.oid::bigint
		   FROM pg_catalog.pg_control_system() AS control
		   JOIN pg_catalog.pg_database AS selected
		     ON selected.datname = pg_catalog.current_database()`,
	).Scan(
		&systemIdentifier,
		&databaseName,
		&databaseOID,
	); err != nil {
		return postgresDatabaseIdentity{}, err
	}
	identity := postgresDatabaseIdentity{
		systemIdentifier: strings.TrimSpace(systemIdentifier.String),
		database:         databaseName.String,
	}
	if !systemIdentifier.Valid ||
		identity.systemIdentifier == "" ||
		!databaseName.Valid ||
		identity.database == "" ||
		databaseOID <= 0 ||
		databaseOID > math.MaxUint32 {
		return postgresDatabaseIdentity{}, fmt.Errorf(
			"PostgreSQL system identifier, selected database, and database OID are required",
		)
	}
	identity.databaseOID = uint32(databaseOID)
	return identity, nil
}

func samePostgresDatabaseIdentity(
	source postgresDatabaseIdentity,
	target postgresDatabaseIdentity,
) bool {
	return source.systemIdentifier == target.systemIdentifier &&
		source.databaseOID == target.databaseOID
}
