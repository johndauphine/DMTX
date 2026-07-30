package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

type clickHouseDatabaseHandleProvider interface {
	clickHouseDatabaseHandle() *sql.DB
}

type clickHouseDatabaseIdentity struct {
	uuid     string
	database string
}

// ClickHouseToClickHouseWithObserver rebuilds the admitted ClickHouse 24.8
// shape into a distinct Atomic database without treating ordering metadata as
// relational uniqueness.
func ClickHouseToClickHouseWithObserver(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "clickhouse" ||
		cfg.Target.Type != "clickhouse" {
		return Result{}, fmt.Errorf(
			"ClickHouse-to-ClickHouse requires source.type and target.type clickhouse",
		)
	}
	if sameConfiguredClickHouseDatabase(cfg.Source, cfg.Target) {
		return Result{}, fmt.Errorf(
			"ClickHouse-to-ClickHouse requires distinct source and target databases",
		)
	}
	return executeBuiltInComposedRoute(
		ctx,
		cfg,
		observer,
		adapterPair{source: "clickhouse", target: "clickhouse"},
	)
}

func sameConfiguredClickHouseDatabase(
	source config.Endpoint,
	target config.Endpoint,
) bool {
	sourcePort := source.Port
	if sourcePort == 0 {
		sourcePort = 9440
	}
	targetPort := target.Port
	if targetPort == 0 {
		targetPort = 9440
	}
	return strings.EqualFold(
		strings.TrimSpace(source.Host),
		strings.TrimSpace(target.Host),
	) &&
		sourcePort == targetPort &&
		source.Database != "" &&
		source.Database == target.Database
}

func requireDistinctLiveClickHouseDatabases(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
) error {
	sourceProvider, sourceOK := source.(clickHouseDatabaseHandleProvider)
	targetProvider, targetOK := target.(clickHouseDatabaseHandleProvider)
	if !sourceOK || !targetOK ||
		sourceProvider.clickHouseDatabaseHandle() == nil ||
		targetProvider.clickHouseDatabaseHandle() == nil {
		return fmt.Errorf(
			"ClickHouse-to-ClickHouse cannot verify distinct live source and target databases",
		)
	}
	sourceIdentity, err := readClickHouseDatabaseIdentity(
		ctx,
		sourceProvider.clickHouseDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live ClickHouse source: %w", err)
	}
	targetIdentity, err := readClickHouseDatabaseIdentity(
		ctx,
		targetProvider.clickHouseDatabaseHandle(),
	)
	if err != nil {
		return fmt.Errorf("identify live ClickHouse target: %w", err)
	}
	if sameClickHouseDatabaseIdentity(sourceIdentity, targetIdentity) {
		return fmt.Errorf(
			"ClickHouse-to-ClickHouse requires distinct live source and target databases",
		)
	}
	return nil
}

func readClickHouseDatabaseIdentity(
	ctx context.Context,
	database *sql.DB,
) (clickHouseDatabaseIdentity, error) {
	var uuid, databaseName, engineName string
	err := database.QueryRowContext(
		ctx,
		`SELECT toString(uuid), name, engine
		   FROM system.databases
		  WHERE name = currentDatabase()`,
	).Scan(&uuid, &databaseName, &engineName)
	if err != nil {
		return clickHouseDatabaseIdentity{}, err
	}
	identity := clickHouseDatabaseIdentity{
		uuid:     strings.TrimSpace(uuid),
		database: databaseName,
	}
	if identity.uuid == "" ||
		identity.uuid == "00000000-0000-0000-0000-000000000000" ||
		identity.database == "" {
		return clickHouseDatabaseIdentity{}, fmt.Errorf(
			"ClickHouse database UUID and selected database are required",
		)
	}
	if engineName != "Atomic" {
		return clickHouseDatabaseIdentity{}, fmt.Errorf(
			"ClickHouse selected database uses unsupported engine %q",
			engineName,
		)
	}
	return identity, nil
}

func sameClickHouseDatabaseIdentity(
	source clickHouseDatabaseIdentity,
	target clickHouseDatabaseIdentity,
) bool {
	return strings.EqualFold(source.uuid, target.uuid)
}
