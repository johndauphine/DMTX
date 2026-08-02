package app

import (
	"context"
	"database/sql"
	"os"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/migrate"
)

type productionEndpointProbe struct {
	endpoint config.Endpoint
	side     migrate.PreflightSide
	database *sql.DB

	reachable              bool
	authenticated          bool
	databaseAbsent         bool
	versionVerified        bool
	capabilityErr          bool
	schemaExists           bool
	schemaUsable           bool
	encoding               string
	poolVerified           bool
	privilegeRead          bool
	privilegeWrite         bool
	privilegeDelete        bool
	schemaCreate           bool
	filesystemWriteKnown   bool
	maxPacketBytes         int64
	maxPacketKnown         bool
	localInfile            bool
	localInfileKnown       bool
	snapshotIsolation      bool
	snapshotIsolationKnown bool
	sizeBytes              int64
	sizeVerified           bool
	selectedTables         []string
}

func probeProductionPreflight(
	ctx context.Context,
	cfg config.Config,
) ([]productionPreflightFact, bool) {
	routeErr := migrate.ValidateMigration(cfg)
	source := inspectProductionEndpoint(
		ctx,
		cfg.Source,
		migrate.PreflightSource,
		cfg,
	)
	defer source.close()
	target := inspectProductionEndpoint(
		ctx,
		cfg.Target,
		migrate.PreflightTarget,
		cfg,
	)
	defer target.close()

	facts := make([]productionPreflightFact, 0, 32)
	facts = append(
		facts,
		commonProductionEndpointFacts(source, routeErr)...,
	)
	facts = append(
		facts,
		commonProductionEndpointFacts(target, routeErr)...,
	)
	facts = append(facts, sourceReadFact(source))
	facts = append(facts, sourceSizeFact(source))
	facts = append(facts, targetWriteFact(target))
	if cfg.Source.Type == "mysql" {
		facts = append(facts, mySQLMaxPacketFact(source))
	}
	if cfg.Target.Type == "mysql" {
		facts = append(
			facts,
			mySQLBulkPathFact(target),
			mySQLMaxPacketFact(target),
		)
	}
	if cfg.Migration.StrictConsistency &&
		cfg.Migration.StrictConsistencyScope ==
			config.StrictConsistencyMigration &&
		cfg.Source.Type == "mssql" {
		facts = append(facts, sqlServerSnapshotIsolationFact(source))
	}
	if cfg.Migration.Deletes.Mode == config.DeleteModeReconcile {
		facts = append(facts, targetDeletePrivilegeFact(target))
	}

	switch cfg.Migration.TargetMode {
	case "drop_recreate":
		facts = append(facts, targetSchemaCreateFact(target))
		facts = append(
			facts,
			targetDestructiveAcknowledgementFact(
				ctx,
				cfg,
				source,
				target,
			),
		)
	case "upsert":
		facts = append(facts, targetUpsertCapabilityFact(target))
	}
	if cfg.Migration.StrictConsistency {
		facts = append(
			facts,
			strictConsistencyPrerequisiteFact(ctx, source),
		)
	}
	if source.sizeVerified {
		facts = append(
			facts,
			targetDiskCapacityFact(target, source.sizeBytes),
		)
	}
	return sortedProductionPreflightFacts(facts), source.sizeVerified
}

func inspectProductionEndpoint(
	ctx context.Context,
	endpoint config.Endpoint,
	side migrate.PreflightSide,
	cfg config.Config,
) productionEndpointProbe {
	probe := productionEndpointProbe{endpoint: endpoint, side: side}
	if endpoint.Type == "sqlite" {
		inspectSQLiteProductionEndpoint(ctx, &probe, cfg)
		return probe
	}
	if !probeNetworkReachability(ctx, endpoint) {
		return probe
	}
	probe.reachable = true
	password, err := config.ExpandSecret(endpoint.Password)
	if err != nil {
		return probe
	}
	endpoint.Password = password
	probe.endpoint = endpoint
	database, err := openGenericPreflightDatabase(ctx, endpoint)
	if err != nil {
		return probe
	}
	probe.database = database
	probe.authenticated = true

	if endpoint.Type == "mysql" {
		var verified *sql.DB
		if side == migrate.PreflightSource {
			verified, err = engine.OpenMySQLSource(ctx, endpoint)
		} else {
			verified, _, err = engine.OpenMySQLTarget(ctx, endpoint)
		}
		if err == nil {
			_ = probe.database.Close()
			probe.database = verified
		} else {
			probe.capabilityErr = true
		}
	}
	probe.versionVerified = verifyProductionEndpointVersion(
		ctx,
		probe.database,
		endpoint,
		side,
	)
	if !probe.versionVerified {
		probe.capabilityErr = true
	}
	probe.databaseAbsent = false
	probe.inspectDatabaseFacts(ctx, cfg)
	return probe
}

func inspectSQLiteProductionEndpoint(
	ctx context.Context,
	probe *productionEndpointProbe,
	cfg config.Config,
) {
	path := probe.endpoint.Database
	info, err := os.Stat(path)
	if err != nil {
		if probe.side == migrate.PreflightTarget && os.IsNotExist(err) {
			probe.databaseAbsent = true
			probe.versionVerified, probe.encoding =
				probeSQLiteRuntime(ctx)
			probe.poolVerified = true
			probe.privilegeWrite,
				probe.filesystemWriteKnown =
				sqliteTargetPathWriteEvidence(path)
			probe.privilegeDelete = probe.privilegeWrite
			probe.schemaCreate = probe.privilegeWrite
			return
		}
		return
	}
	if info.IsDir() {
		return
	}
	probe.reachable = true
	database, err := sql.Open("sqlite", sqlitePreflightReadOnlyURI(path))
	if err != nil {
		return
	}
	if err := database.PingContext(ctx); err != nil {
		_ = database.Close()
		return
	}
	probe.database = database
	probe.authenticated = true
	probe.versionVerified = querySQLiteVersion(ctx, database)
	probe.inspectDatabaseFacts(ctx, cfg)
	if probe.side == migrate.PreflightTarget {
		probe.privilegeWrite,
			probe.filesystemWriteKnown =
			sqliteTargetPathWriteEvidence(path)
		probe.privilegeDelete = probe.privilegeWrite
		probe.schemaCreate = probe.privilegeWrite
	}
	if probe.side == migrate.PreflightSource {
		probe.sizeBytes = info.Size()
		probe.sizeVerified = info.Size() >= 0
	}
}

func (probe *productionEndpointProbe) inspectDatabaseFacts(
	ctx context.Context,
	cfg config.Config,
) {
	if probe.database == nil {
		return
	}
	probe.schemaExists, probe.schemaUsable = probeSchemaAccess(
		ctx,
		probe.database,
		probe.endpoint,
	)
	probe.encoding = probeEndpointEncoding(
		ctx,
		probe.database,
		probe.endpoint,
	)
	probe.poolVerified = probeConnectionPoolHeadroom(
		ctx,
		probe.database,
		probe.endpoint,
		cfg.Migration.ConnectionLimit,
	)
	if probe.side == migrate.PreflightSource {
		probe.selectedTables = probeSelectedSourceTables(
			ctx,
			probe.database,
			probe.endpoint,
			cfg,
		)
		probe.privilegeRead = probeSourceReadPrivileges(
			ctx,
			probe.database,
			probe.endpoint,
		)
		probe.sizeBytes, probe.sizeVerified = probeSourceSize(
			ctx,
			probe.database,
			probe.endpoint,
		)
	} else {
		probe.privilegeWrite, probe.schemaCreate =
			probeTargetPrivileges(
				ctx,
				probe.database,
				probe.endpoint,
				cfg.Migration.TargetMode,
			)
		probe.privilegeDelete = probeTargetDeletePrivilege(
			ctx,
			probe.database,
			probe.endpoint,
		)
	}
	if probe.endpoint.Type == "mysql" {
		probe.maxPacketBytes,
			probe.maxPacketKnown,
			probe.localInfile,
			probe.localInfileKnown =
			probeMySQLTransportCapabilities(ctx, probe.database)
	}
	if probe.endpoint.Type == "mssql" &&
		probe.side == migrate.PreflightSource {
		probe.snapshotIsolation,
			probe.snapshotIsolationKnown =
			probeSQLServerSnapshotIsolation(ctx, probe.database)
	}
}

func (probe *productionEndpointProbe) close() {
	if probe.database != nil {
		_ = probe.database.Close()
	}
}
