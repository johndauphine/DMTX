package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

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

func commonProductionEndpointFacts(
	probe productionEndpointProbe,
	routeErr error,
) []productionPreflightFact {
	return []productionPreflightFact{
		endpointReachabilityFact(probe),
		endpointAuthenticationFact(probe),
		booleanProductionFact(
			"server.version",
			probe.side,
			probe.versionVerified,
			"server version matches the certified adapter contract",
			"server version could not be admitted",
			"use the version line certified for this adapter",
			"certified_version_verified",
			"certified_version_unverified",
		),
		databaseExistsFact(probe),
		endpointSchemaExistsFact(probe),
		endpointSchemaUsageFact(probe),
		encodingCompatibilityFact(probe),
		poolHeadroomFact(probe),
		engineCapabilityFact(probe, routeErr),
	}
}

func endpointReachabilityFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if plannedSQLiteTarget(probe) {
		return unverifiedProductionFact(
			"connection.reachability",
			probe.side,
			"target SQLite file is absent; parent-path readiness was checked instead",
			"verify the target path before starting the run",
			"target_file_not_opened",
		)
	}
	return booleanProductionFact(
		"connection.reachability",
		probe.side,
		probe.reachable,
		"endpoint transport is reachable",
		"endpoint transport could not be reached",
		"verify the endpoint host, port, path, and network route",
		"transport_verified",
		"transport_unavailable",
	)
}

func endpointAuthenticationFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if plannedSQLiteTarget(probe) {
		return unverifiedProductionFact(
			"connection.authentication",
			probe.side,
			"target SQLite file has no authentication boundary before creation",
			"protect the target path with operating-system permissions",
			"filesystem_identity_pending",
		)
	}
	return booleanProductionFact(
		"connection.authentication",
		probe.side,
		probe.authenticated,
		"endpoint authentication succeeded",
		"endpoint authentication could not be proven",
		"provide resolvable credentials accepted by the endpoint",
		"session_authenticated",
		"authentication_unverified",
	)
}

func endpointSchemaExistsFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if plannedSQLiteTarget(probe) {
		return unverifiedProductionFact(
			"schema.exists",
			probe.side,
			"target SQLite main schema will exist only after file creation",
			"verify the planned target path",
			"target_schema_pending",
		)
	}
	return booleanProductionFact(
		"schema.exists",
		probe.side,
		probe.schemaExists,
		"configured schema exists",
		"configured schema existence could not be proven",
		"create the schema or select an existing schema",
		"schema_catalog_verified",
		"schema_catalog_unverified",
	)
}

func endpointSchemaUsageFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if plannedSQLiteTarget(probe) {
		return unverifiedProductionFact(
			"schema.usage",
			probe.side,
			"target SQLite schema usage depends on creating the planned file",
			"verify parent-directory access before starting the run",
			"target_schema_usage_pending",
		)
	}
	return booleanProductionFact(
		"schema.usage",
		probe.side,
		probe.schemaUsable,
		"configured principal can use the schema",
		"schema usage could not be proven",
		"grant schema usage to the configured principal",
		"schema_usage_verified",
		"schema_usage_unverified",
	)
}

func plannedSQLiteTarget(probe productionEndpointProbe) bool {
	return probe.side == migrate.PreflightTarget &&
		probe.endpoint.Type == "sqlite" &&
		probe.databaseAbsent
}

func unverifiedProductionFact(
	check string,
	side migrate.PreflightSide,
	message string,
	remedy string,
	evidence string,
) productionPreflightFact {
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityWarning,
			Check:    check,
			Side:     side,
			Message:  message,
			Remedy:   remedy,
		},
		Class:    preflightClassUnverified,
		Evidence: evidence,
	}
}

func blockingUnverifiedProductionFact(
	check string,
	side migrate.PreflightSide,
	message string,
	remedy string,
	evidence string,
) productionPreflightFact {
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityError,
			Check:    check,
			Side:     side,
			Message:  message,
			Remedy:   remedy,
		},
		Class:    preflightClassUnverified,
		Evidence: evidence,
	}
}

func booleanProductionFact(
	check string,
	side migrate.PreflightSide,
	passed bool,
	successMessage string,
	failureMessage string,
	remedy string,
	successEvidence string,
	failureEvidence string,
) productionPreflightFact {
	severity := migrate.PreflightSeverityInfo
	class := preflightClassPassed
	message := successMessage
	evidence := successEvidence
	if !passed {
		severity = migrate.PreflightSeverityError
		class = preflightClassFailed
		message = failureMessage
		evidence = failureEvidence
	}
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: severity,
			Check:    check,
			Side:     side,
			Message:  message,
			Remedy:   remedy,
		},
		Class:    class,
		Evidence: evidence,
	}
}

func databaseExistsFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.databaseAbsent && probe.side == migrate.PreflightTarget &&
		probe.endpoint.Type == "sqlite" {
		return productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityWarning,
				Check:    "database.exists",
				Side:     probe.side,
				Message: "target SQLite database does not exist yet; the run " +
					"may create it after preflight",
				Remedy: "verify the target parent directory before starting " +
					"the run",
			},
			Class:    preflightClassUnverified,
			Evidence: "target_file_absent",
		}
	}
	return booleanProductionFact(
		"database.exists",
		probe.side,
		probe.authenticated,
		"configured database exists and accepted a session",
		"configured database existence could not be proven",
		"create the database or select an existing database",
		"database_session_verified",
		"database_unverified",
	)
}

func encodingCompatibilityFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	compatible := probe.encoding != ""
	return booleanProductionFact(
		"encoding.compatibility",
		probe.side,
		compatible,
		"endpoint encoding is admitted by the adapter",
		"endpoint encoding compatibility could not be proven",
		"configure a Unicode-compatible database encoding",
		"encoding_contract_verified",
		"encoding_contract_unverified",
	)
}

func poolHeadroomFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.poolVerified {
		return booleanProductionFact(
			"connection.pool_headroom",
			probe.side,
			true,
			"configured connection demand fits reported endpoint limits",
			"",
			"no operator action is required",
			"connection_limit_verified",
			"",
		)
	}
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityWarning,
			Check:    "connection.pool_headroom",
			Side:     probe.side,
			Message:  "connection-pool headroom could not be proven",
			Remedy:   "confirm the endpoint can admit the configured connection limit",
		},
		Class:    preflightClassUnverified,
		Evidence: "connection_limit_unverified",
	}
}

func engineCapabilityFact(
	probe productionEndpointProbe,
	routeErr error,
) productionPreflightFact {
	return booleanProductionFact(
		"engine.capability",
		probe.side,
		routeErr == nil && !probe.capabilityErr &&
			probe.versionVerified,
		"engine role and migration route are certified",
		"engine role or migration route is not certified",
		"select a certified source, target, mode, and consistency scope",
		"certified_route_verified",
		"certified_route_unverified",
	)
}

func sourceReadFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	return booleanProductionFact(
		"privileges.read",
		migrate.PreflightSource,
		probe.privilegeRead,
		"source read privileges are present",
		"source read privileges could not be proven",
		"grant read and catalog visibility required by the source adapter",
		"source_read_verified",
		"source_read_unverified",
	)
}

func sourceSizeFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.sizeVerified {
		return productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityInfo,
				Check:    "source.size_evidence",
				Side:     migrate.PreflightSource,
				Message:  "source size evidence is available",
				Remedy:   "no operator action is required",
			},
			Class: preflightClassPassed,
			Evidence: "source_bytes=" +
				strconv.FormatInt(probe.sizeBytes, 10),
		}
	}
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityWarning,
			Check:    "source.size_evidence",
			Side:     migrate.PreflightSource,
			Message:  "source size evidence is unavailable",
			Remedy:   "collect a source size estimate before capacity planning",
		},
		Class:    preflightClassUnverified,
		Evidence: "source_size_unavailable",
	}
}

func targetWriteFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.endpoint.Type == "sqlite" && !probe.filesystemWriteKnown {
		return blockingUnverifiedProductionFact(
			"privileges.write",
			migrate.PreflightTarget,
			"target SQLite file and parent-directory write access could not be proven without mutation",
			"verify that the current operating-system principal can create database journal files and write the target",
			"target_filesystem_write_unverified",
		)
	}
	return booleanProductionFact(
		"privileges.write",
		migrate.PreflightTarget,
		probe.privilegeWrite,
		"target mode-specific write privileges are present",
		"target mode-specific write privileges could not be proven",
		"grant the target privileges required by the selected target mode",
		"target_write_verified",
		"target_write_unverified",
	)
}

func targetDeletePrivilegeFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.endpoint.Type == "sqlite" && !probe.filesystemWriteKnown {
		return blockingUnverifiedProductionFact(
			"privileges.delete_reconcile",
			migrate.PreflightTarget,
			"target SQLite delete authority could not be proven without filesystem mutation",
			"verify that the current operating-system principal can write the target and its journal files",
			"target_delete_filesystem_unverified",
		)
	}
	return booleanProductionFact(
		"privileges.delete_reconcile",
		migrate.PreflightTarget,
		probe.privilegeDelete,
		"target delete-reconciliation privilege is present",
		"target delete-reconciliation privilege could not be proven",
		"grant DELETE authority required by delete reconciliation",
		"target_delete_verified",
		"target_delete_unverified",
	)
}

func mySQLMaxPacketFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.maxPacketKnown && probe.maxPacketBytes > 0 {
		return productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityInfo,
				Check:    "engine.mysql.max_allowed_packet",
				Side:     probe.side,
				Message:  "MySQL-family packet ceiling is available for deterministic batching",
				Remedy:   "no operator action is required",
			},
			Class: preflightClassPassed,
			Evidence: "max_allowed_packet_bytes=" +
				strconv.FormatInt(probe.maxPacketBytes, 10),
		}
	}
	return booleanProductionFact(
		"engine.mysql.max_allowed_packet",
		probe.side,
		false,
		"",
		"MySQL-family packet ceiling could not be proven",
		"grant access to session variables and configure max_allowed_packet",
		"",
		"max_allowed_packet_unverified",
	)
}

func mySQLBulkPathFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if !probe.localInfileKnown {
		return booleanProductionFact(
			"engine.mysql.bulk_path",
			migrate.PreflightTarget,
			false,
			"",
			"MySQL-family local-infile state could not be proven",
			"grant access to local_infile state or explicitly skip this check",
			"",
			"local_infile_unverified",
		)
	}
	evidence := "bounded_insert_fallback_certified;local_infile=off"
	if probe.localInfile {
		evidence = "bounded_insert_fallback_certified;local_infile=on"
	}
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityInfo,
			Check:    "engine.mysql.bulk_path",
			Side:     migrate.PreflightTarget,
			Message:  "MySQL-family bulk path and strict fallback are known",
			Remedy:   "no operator action is required",
		},
		Class:    preflightClassPassed,
		Evidence: evidence,
	}
}

func sqlServerSnapshotIsolationFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	return booleanProductionFact(
		"engine.mssql.snapshot_isolation",
		migrate.PreflightSource,
		probe.snapshotIsolationKnown && probe.snapshotIsolation,
		"SQL Server snapshot isolation is enabled",
		"SQL Server snapshot isolation support could not be proven",
		"enable snapshot isolation before migration-scoped strict reads",
		"snapshot_isolation_on",
		"snapshot_isolation_unverified",
	)
}

func targetSchemaCreateFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	if probe.endpoint.Type == "sqlite" && !probe.filesystemWriteKnown {
		return blockingUnverifiedProductionFact(
			"schema.create_access",
			migrate.PreflightTarget,
			"target SQLite schema-write access could not be proven without filesystem mutation",
			"verify that the current operating-system principal can create the target and its journal files",
			"target_schema_filesystem_unverified",
		)
	}
	return booleanProductionFact(
		"schema.create_access",
		migrate.PreflightTarget,
		probe.schemaCreate,
		"target schema create access is present",
		"target schema create access could not be proven",
		"grant create access on the target schema",
		"schema_create_verified",
		"schema_create_unverified",
	)
}

func targetUpsertCapabilityFact(
	probe productionEndpointProbe,
) productionPreflightFact {
	capability, exists := engine.TargetCapability(probe.endpoint.Type)
	return booleanProductionFact(
		"target.upsert_capability",
		migrate.PreflightTarget,
		exists && capability.Upsert && probe.versionVerified &&
			!probe.capabilityErr,
		"target adapter has a certified upsert path",
		"target adapter has no certified upsert path",
		"select an upsert-capable target or use drop_recreate",
		"upsert_capability_verified",
		"upsert_capability_unverified",
	)
}

func targetDiskCapacityFact(
	probe productionEndpointProbe,
	sourceBytes int64,
) productionPreflightFact {
	if probe.endpoint.Type == "sqlite" {
		free, ok := sqliteTargetFreeBytes(probe.endpoint.Database)
		return sqliteTargetDiskCapacityFact(sourceBytes, free, ok)
	}
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityWarning,
			Check:    "target.disk_capacity",
			Side:     migrate.PreflightTarget,
			Message:  "target free-space capacity could not be proven",
			Remedy:   "confirm target free space exceeds the source size estimate",
		},
		Class:    preflightClassUnverified,
		Evidence: "target_capacity_unavailable",
	}
}

func sqliteTargetDiskCapacityFact(
	sourceBytes int64,
	freeBytes uint64,
	known bool,
) productionPreflightFact {
	if !known || sourceBytes < 0 {
		return productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityWarning,
				Check:    "target.disk_capacity",
				Side:     migrate.PreflightTarget,
				Message:  "target free-space capacity could not be proven",
				Remedy:   "confirm target free space exceeds the source size estimate",
			},
			Class:    preflightClassUnverified,
			Evidence: "target_capacity_unavailable",
		}
	}
	evidence := "required_bytes=" +
		strconv.FormatInt(sourceBytes, 10) +
		";free_bytes=" + strconv.FormatUint(freeBytes, 10)
	if freeBytes < uint64(sourceBytes) {
		return productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityError,
				Check:    "target.disk_capacity",
				Side:     migrate.PreflightTarget,
				Message:  "target filesystem has less free space than the estimated source size",
				Remedy:   "free target disk space or select a target with sufficient capacity",
			},
			Class:    preflightClassFailed,
			Evidence: evidence,
		}
	}
	return productionPreflightFact{
		Finding: migrate.PreflightFinding{
			Severity: migrate.PreflightSeverityInfo,
			Check:    "target.disk_capacity",
			Side:     migrate.PreflightTarget,
			Message:  "target filesystem has at least the estimated source size",
			Remedy:   "no operator action is required",
		},
		Class:    preflightClassPassed,
		Evidence: evidence,
	}
}

func strictConsistencyPrerequisiteFact(
	ctx context.Context,
	probe productionEndpointProbe,
) productionPreflightFact {
	passed := false
	if probe.endpoint.Type == "postgres" &&
		probe.database != nil &&
		probe.versionVerified {
		transaction, err := probe.database.BeginTx(
			ctx,
			&sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
				ReadOnly:  true,
			},
		)
		if err == nil {
			var reference string
			err = transaction.QueryRowContext(
				ctx,
				`SELECT pg_export_snapshot()`,
			).Scan(&reference)
			rollbackErr := transaction.Rollback()
			passed = err == nil && rollbackErr == nil &&
				strings.TrimSpace(reference) != ""
		}
	}
	return booleanProductionFact(
		"consistency.strict_prerequisites",
		migrate.PreflightSource,
		passed,
		"source can establish and release an exported stable snapshot",
		"strict-consistency snapshot prerequisites could not be proven",
		"grant snapshot access and use the certified PostgreSQL strict route",
		"exported_snapshot_verified_and_released",
		"exported_snapshot_unverified",
	)
}

func targetDestructiveAcknowledgementFact(
	ctx context.Context,
	cfg config.Config,
	source productionEndpointProbe,
	target productionEndpointProbe,
) productionPreflightFact {
	if cfg.Migration.DestructiveAcknowledged {
		return booleanProductionFact(
			"target.destructive_acknowledgement",
			migrate.PreflightTarget,
			true,
			"operator supplied destructive acknowledgement",
			"",
			"no operator action is required",
			"operator_acknowledged",
			"",
		)
	}
	nonEmpty, known := probeSelectedTargetNonEmpty(
		ctx,
		source.selectedTables,
		target,
	)
	if known && !nonEmpty {
		return booleanProductionFact(
			"target.destructive_acknowledgement",
			migrate.PreflightTarget,
			true,
			"selected target tables contain no rows",
			"",
			"no destructive acknowledgement is required",
			"selected_target_empty",
			"",
		)
	}
	if !known {
		return productionPreflightFact{
			Finding: migrate.PreflightFinding{
				Severity: migrate.PreflightSeverityError,
				Check:    "target.destructive_acknowledgement",
				Side:     migrate.PreflightTarget,
				Message:  "selected target occupancy could not be proven",
				Remedy:   "verify the target or explicitly acknowledge destructive rebuild",
			},
			Class:    preflightClassUnverified,
			Evidence: "target_occupancy_unverified",
		}
	}
	return booleanProductionFact(
		"target.destructive_acknowledgement",
		migrate.PreflightTarget,
		false,
		"",
		"selected target tables contain rows without destructive acknowledgement",
		"back up the target and explicitly acknowledge destructive rebuild",
		"",
		"populated_target_unacknowledged",
	)
}

func probeNetworkReachability(
	ctx context.Context,
	endpoint config.Endpoint,
) bool {
	if strings.TrimSpace(endpoint.Host) == "" {
		return false
	}
	port := endpoint.Port
	if port == 0 {
		switch endpoint.Type {
		case "postgres":
			port = 5432
		case "mysql":
			port = 3306
		case "mssql":
			port = 1433
		case "clickhouse":
			port = 9440
		default:
			return false
		}
	}
	connection, err := (&net.Dialer{Timeout: 3 * time.Second}).DialContext(
		ctx,
		"tcp",
		net.JoinHostPort(endpoint.Host, strconv.Itoa(port)),
	)
	if err != nil {
		return false
	}
	return connection.Close() == nil
}

func openGenericPreflightDatabase(
	ctx context.Context,
	endpoint config.Endpoint,
) (*sql.DB, error) {
	switch endpoint.Type {
	case "postgres":
		return engine.OpenPostgres(ctx, endpoint)
	case "mysql":
		return engine.OpenMySQL(ctx, endpoint)
	case "mssql":
		return engine.OpenSQLServer(ctx, endpoint)
	case "clickhouse":
		return engine.OpenClickHouse(ctx, endpoint)
	default:
		return nil, fmt.Errorf("unsupported preflight engine")
	}
}

func verifyProductionEndpointVersion(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	side migrate.PreflightSide,
) bool {
	if database == nil {
		return false
	}
	var err error
	switch endpoint.Type {
	case "postgres":
		err = engine.VerifyPostgres16Source(ctx, database)
	case "mysql":
		if side == migrate.PreflightSource {
			err = engine.VerifyMySQLSource(ctx, database)
		} else {
			_, err = engine.VerifyMySQLTarget(ctx, database)
		}
	case "mssql":
		if side == migrate.PreflightSource {
			err = engine.VerifySQLServer2022Source(ctx, database)
		} else {
			err = engine.VerifySQLServer2022Target(ctx, database)
		}
	case "clickhouse":
		if side == migrate.PreflightSource {
			err = engine.VerifyClickHouse248Source(
				ctx,
				database,
				endpoint.Database,
			)
		} else {
			err = engine.VerifyClickHouse248Target(
				ctx,
				database,
				endpoint.Database,
			)
		}
	default:
		return false
	}
	return err == nil
}

func querySQLiteVersion(ctx context.Context, database *sql.DB) bool {
	var version string
	return database.QueryRowContext(
		ctx,
		`SELECT sqlite_version()`,
	).Scan(&version) == nil && strings.TrimSpace(version) != ""
}

func probeSQLiteRuntime(ctx context.Context) (bool, string) {
	database, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return false, ""
	}
	defer database.Close()
	if err := database.PingContext(ctx); err != nil {
		return false, ""
	}
	versionVerified := querySQLiteVersion(ctx, database)
	encoding := probeEndpointEncoding(
		ctx,
		database,
		config.Endpoint{Type: "sqlite"},
	)
	return versionVerified, encoding
}

func probeSchemaAccess(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) (bool, bool) {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		var count int
		err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM pragma_database_list WHERE name = 'main'`,
		).Scan(&count)
		return err == nil && count == 1, err == nil && count == 1
	case "postgres":
		var exists, usable bool
		err := database.QueryRowContext(
			ctx,
			`SELECT
				to_regnamespace($1) IS NOT NULL,
				has_schema_privilege(current_user, $1, 'USAGE')`,
			namespace,
		).Scan(&exists, &usable)
		return err == nil && exists, err == nil && exists && usable
	case "mysql":
		var count int
		err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			 FROM information_schema.schemata
			 WHERE schema_name = ?`,
			namespace,
		).Scan(&count)
		return err == nil && count == 1, err == nil && count == 1
	case "mssql":
		var schemaID sql.NullInt64
		var usable int
		err := database.QueryRowContext(
			ctx,
			`SELECT
				SCHEMA_ID(@p1),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT')`,
			namespace,
		).Scan(&schemaID, &usable)
		return err == nil && schemaID.Valid,
			err == nil && schemaID.Valid && usable == 1
	case "clickhouse":
		var count uint64
		err := database.QueryRowContext(
			ctx,
			`SELECT count()
			 FROM system.databases
			 WHERE name = ?`,
			namespace,
		).Scan(&count)
		return err == nil && count == 1, err == nil && count == 1
	default:
		return false, false
	}
}

func probeEndpointEncoding(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) string {
	var value string
	switch endpoint.Type {
	case "sqlite":
		if database.QueryRowContext(
			ctx,
			`PRAGMA encoding`,
		).Scan(&value) != nil || !strings.EqualFold(value, "UTF-8") {
			return ""
		}
	case "postgres":
		if database.QueryRowContext(
			ctx,
			`SHOW server_encoding`,
		).Scan(&value) != nil || !strings.EqualFold(value, "UTF8") {
			return ""
		}
	case "mysql":
		if database.QueryRowContext(
			ctx,
			`SELECT @@character_set_database`,
		).Scan(&value) != nil ||
			!strings.HasPrefix(strings.ToLower(value), "utf8") {
			return ""
		}
	case "mssql":
		if database.QueryRowContext(
			ctx,
			`SELECT CONVERT(varchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Collation'))`,
		).Scan(&value) != nil || strings.TrimSpace(value) == "" {
			return ""
		}
	case "clickhouse":
		value = "byte_or_utf8_adapter_contract"
	default:
		return ""
	}
	return value
}

func probeConnectionPoolHeadroom(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	connectionLimit int,
) bool {
	if connectionLimit <= 0 {
		return false
	}
	switch endpoint.Type {
	case "sqlite":
		return true
	case "postgres":
		var maximum, active int
		return database.QueryRowContext(
			ctx,
			`SELECT
				current_setting('max_connections')::integer
				  - current_setting('superuser_reserved_connections')::integer,
				(SELECT COUNT(*) FROM pg_catalog.pg_stat_activity)`,
		).Scan(&maximum, &active) == nil &&
			maximum-active >= connectionLimit
	case "mysql":
		var maximum, userMaximum, active int
		if database.QueryRowContext(
			ctx,
			`SELECT @@max_connections, @@max_user_connections`,
		).Scan(&maximum, &userMaximum) != nil {
			return false
		}
		var variable string
		if database.QueryRowContext(
			ctx,
			`SHOW GLOBAL STATUS LIKE 'Threads_connected'`,
		).Scan(&variable, &active) != nil ||
			!strings.EqualFold(variable, "Threads_connected") {
			return false
		}
		if userMaximum > 0 && userMaximum < maximum {
			maximum = userMaximum
		}
		return maximum-active >= connectionLimit
	case "mssql":
		var maximum, active int64
		return database.QueryRowContext(
			ctx,
			`SELECT @@MAX_CONNECTIONS, COUNT_BIG(*)
			 FROM sys.dm_exec_connections`,
		).Scan(&maximum, &active) == nil &&
			maximum-active >= int64(connectionLimit)
	case "clickhouse":
		var maximum, active uint64
		err := database.QueryRowContext(
			ctx,
			`SELECT
				toUInt64(value),
				(
					SELECT count()
					FROM system.processes
					WHERE user = currentUser()
				)
			 FROM system.settings
			 WHERE name = 'max_concurrent_queries_for_user'`,
		).Scan(&maximum, &active)
		return err == nil && (maximum == 0 ||
			maximum >= active &&
				maximum-active >= uint64(connectionLimit))
	default:
		return false
	}
}

func probeSelectedSourceTables(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	cfg config.Config,
) []string {
	tables, err := listPreflightTables(ctx, database, endpoint)
	if err != nil {
		return nil
	}
	selected, err := config.SelectTables(
		tables,
		cfg.Migration.IncludeTables,
		cfg.Migration.ExcludeTables,
	)
	if err != nil {
		return nil
	}
	return selected
}

func listPreflightTables(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) ([]string, error) {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		rows, err := database.QueryContext(ctx, `
			SELECT name
			FROM sqlite_schema
			WHERE type = 'table'
			  AND name NOT LIKE 'sqlite_%'
			ORDER BY name
		`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var result []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, err
			}
			result = append(result, name)
		}
		return result, rows.Err()
	case "postgres":
		return engine.ListPostgresTables(ctx, database, namespace)
	case "mysql":
		return engine.ListMySQLTables(ctx, database, namespace)
	case "mssql":
		return engine.ListSQLServerTables(ctx, database, namespace)
	case "clickhouse":
		return engine.ListClickHouseTables(ctx, database, namespace)
	default:
		return nil, fmt.Errorf("unsupported preflight table list")
	}
}

func probeSourceReadPrivileges(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) bool {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		var count int
		return database.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM sqlite_schema`,
		).Scan(&count) == nil
	case "postgres":
		var allowed bool
		err := database.QueryRowContext(ctx, `
			SELECT
				has_schema_privilege(current_user, $1, 'USAGE')
				AND COALESCE(bool_and(
					has_table_privilege(relation.oid, 'SELECT')
				), true)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relkind IN ('r', 'p')
		`, namespace).Scan(&allowed)
		return err == nil && allowed
	case "mysql":
		return probeMySQLGrants(
			ctx,
			database,
			namespace,
			[]string{"SELECT"},
		)
	case "mssql":
		var allowed int
		return database.QueryRowContext(
			ctx,
			`SELECT HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT')`,
			namespace,
		).Scan(&allowed) == nil && allowed == 1
	case "clickhouse":
		return probeClickHouseGrant(
			ctx,
			database,
			"SELECT",
			namespace,
		)
	default:
		return false
	}
}

func probeTargetPrivileges(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	mode string,
) (bool, bool) {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		writable := sqliteTargetPathWritable(endpoint.Database)
		return writable, writable
	case "postgres":
		var (
			create        bool
			insert        bool
			selectAllowed bool
			update        bool
			deleteRows    bool
			truncate      bool
			owns          bool
		)
		err := database.QueryRowContext(ctx, `
			SELECT
				has_schema_privilege(current_user, $1, 'CREATE'),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'INSERT')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'SELECT')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'UPDATE')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'DELETE')
				), true),
				COALESCE(bool_and(
					has_table_privilege(relation.oid, 'TRUNCATE')
				), true),
				COALESCE(bool_and(
					pg_has_role(current_user, relation.relowner, 'USAGE')
				), true)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relkind IN ('r', 'p')
		`, namespace).Scan(
			&create,
			&insert,
			&selectAllowed,
			&update,
			&deleteRows,
			&truncate,
			&owns,
		)
		if err != nil {
			return false, false
		}
		return admitPostgresTargetPrivileges(
			mode,
			create,
			insert,
			selectAllowed,
			update,
			deleteRows,
			truncate,
			owns,
		)
	case "mysql":
		required := []string{"INSERT", "SELECT", "UPDATE"}
		if mode == "drop_recreate" {
			required = []string{
				"ALTER",
				"CREATE",
				"DROP",
				"INDEX",
				"INSERT",
				"REFERENCES",
				"SELECT",
			}
		}
		write := probeMySQLGrants(
			ctx,
			database,
			namespace,
			required,
		)
		create := probeMySQLGrants(
			ctx,
			database,
			namespace,
			[]string{"CREATE"},
		)
		return write, create
	case "mssql":
		var insert, selectAllowed, update, alter, create int
		err := database.QueryRowContext(ctx, `
			SELECT
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'INSERT'),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'SELECT'),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'UPDATE'),
				HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'ALTER'),
				HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')
		`, namespace).Scan(
			&insert,
			&selectAllowed,
			&update,
			&alter,
			&create,
		)
		if err != nil {
			return false, false
		}
		createAllowed := alter == 1 && create == 1
		write := insert == 1 && selectAllowed == 1 && update == 1
		if mode == "drop_recreate" {
			write = write && createAllowed
		}
		return write, createAllowed
	case "clickhouse":
		insert := probeClickHouseGrant(
			ctx,
			database,
			"INSERT",
			namespace,
		)
		create := probeClickHouseGrant(
			ctx,
			database,
			"CREATE TABLE",
			namespace,
		)
		drop := probeClickHouseGrant(
			ctx,
			database,
			"DROP TABLE",
			namespace,
		)
		selectAllowed := probeClickHouseGrant(
			ctx,
			database,
			"SELECT",
			namespace,
		)
		return insert && selectAllowed &&
			(mode != "drop_recreate" || create && drop), create
	default:
		return false, false
	}
}

func admitPostgresTargetPrivileges(
	mode string,
	create bool,
	insert bool,
	selectAllowed bool,
	update bool,
	deleteRows bool,
	truncate bool,
	ownsExistingTables bool,
) (bool, bool) {
	write := insert && selectAllowed && update
	if mode == "drop_recreate" {
		return write && deleteRows && truncate && create &&
			ownsExistingTables, create
	}
	return write, create
}

func probeTargetDeletePrivilege(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) bool {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "sqlite":
		return sqliteTargetPathWritable(endpoint.Database)
	case "postgres":
		var allowed bool
		err := database.QueryRowContext(ctx, `
			SELECT COALESCE(bool_and(
				has_table_privilege(relation.oid, 'DELETE')
			), true)
			FROM pg_catalog.pg_class AS relation
			JOIN pg_catalog.pg_namespace AS namespace
			  ON namespace.oid = relation.relnamespace
			WHERE namespace.nspname = $1
			  AND relation.relkind IN ('r', 'p')
		`, namespace).Scan(&allowed)
		return err == nil && allowed
	case "mysql":
		return probeMySQLGrants(
			ctx,
			database,
			namespace,
			[]string{"DELETE"},
		)
	case "mssql":
		var allowed int
		return database.QueryRowContext(
			ctx,
			`SELECT HAS_PERMS_BY_NAME(@p1, 'SCHEMA', 'DELETE')`,
			namespace,
		).Scan(&allowed) == nil && allowed == 1
	default:
		return false
	}
}

func probeMySQLTransportCapabilities(
	ctx context.Context,
	database *sql.DB,
) (int64, bool, bool, bool) {
	if database == nil {
		return 0, false, false, false
	}
	var (
		maximum   int64
		localFile int
	)
	err := database.QueryRowContext(
		ctx,
		`SELECT @@max_allowed_packet, @@GLOBAL.local_infile`,
	).Scan(&maximum, &localFile)
	if err != nil || maximum <= 0 ||
		(localFile != 0 && localFile != 1) {
		return 0, false, false, false
	}
	return maximum, true, localFile == 1, true
}

func probeSQLServerSnapshotIsolation(
	ctx context.Context,
	database *sql.DB,
) (bool, bool) {
	if database == nil {
		return false, false
	}
	var state string
	err := database.QueryRowContext(
		ctx,
		`SELECT snapshot_isolation_state_desc
		 FROM sys.databases
		 WHERE database_id = DB_ID()`,
	).Scan(&state)
	if err != nil || strings.TrimSpace(state) == "" {
		return false, false
	}
	return strings.EqualFold(state, "ON"), true
}

func probeSourceSize(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
) (int64, bool) {
	var size sql.NullInt64
	var err error
	switch endpoint.Type {
	case "sqlite":
		info, statErr := os.Stat(endpoint.Database)
		if statErr != nil {
			return 0, false
		}
		return info.Size(), info.Size() >= 0
	case "postgres":
		err = database.QueryRowContext(
			ctx,
			`SELECT pg_database_size(current_database())::bigint`,
		).Scan(&size)
	case "mysql":
		err = database.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(data_length + index_length), 0)
			FROM information_schema.tables
			WHERE table_schema = DATABASE()
		`).Scan(&size)
	case "mssql":
		err = database.QueryRowContext(
			ctx,
			`SELECT COALESCE(SUM(CONVERT(bigint, size)) * 8192, 0)
			 FROM sys.database_files`,
		).Scan(&size)
	case "clickhouse":
		err = database.QueryRowContext(ctx, `
			SELECT toInt64(sum(bytes_on_disk))
			FROM system.parts
			WHERE active AND database = currentDatabase()
		`).Scan(&size)
	default:
		return 0, false
	}
	return size.Int64, err == nil && size.Valid && size.Int64 >= 0
}

func probeMySQLGrants(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	required []string,
) bool {
	rows, err := database.QueryContext(ctx, `SHOW GRANTS`)
	if err != nil {
		return false
	}
	defer rows.Close()
	found := make(map[string]bool, len(required))
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return false
		}
		upper := strings.ToUpper(grant)
		if !mySQLGrantCoversNamespace(upper, namespace) {
			continue
		}
		if strings.Contains(upper, "GRANT ALL PRIVILEGES ") {
			for _, privilege := range required {
				found[privilege] = true
			}
			continue
		}
		for _, privilege := range required {
			if strings.Contains(
				upper,
				"GRANT "+privilege+",",
			) || strings.Contains(
				upper,
				", "+privilege+",",
			) || strings.Contains(
				upper,
				", "+privilege+" ON ",
			) || strings.Contains(
				upper,
				"GRANT "+privilege+" ON ",
			) {
				found[privilege] = true
			}
		}
	}
	if rows.Err() != nil {
		return false
	}
	for _, privilege := range required {
		if !found[privilege] {
			return false
		}
	}
	return true
}

func mySQLGrantCoversNamespace(grant string, namespace string) bool {
	scope := strings.ToUpper(
		"`" + strings.ReplaceAll(namespace, "`", "``") + "`.*",
	)
	return strings.Contains(grant, " ON *.* ") ||
		strings.Contains(grant, " ON "+scope+" ")
}

func probeClickHouseGrant(
	ctx context.Context,
	database *sql.DB,
	privilege string,
	namespace string,
) bool {
	query := "CHECK GRANT " + privilege + " ON " +
		clickHousePreflightIdentifier(namespace) + ".*"
	var allowed uint8
	return database.QueryRowContext(ctx, query).Scan(&allowed) == nil &&
		allowed == 1
}

func probeSelectedTargetNonEmpty(
	ctx context.Context,
	selected []string,
	target productionEndpointProbe,
) (bool, bool) {
	if target.databaseAbsent && target.endpoint.Type == "sqlite" {
		return false, true
	}
	if target.database == nil || selected == nil {
		return false, false
	}
	targetTables, err := listPreflightTables(
		ctx,
		target.database,
		target.endpoint,
	)
	if err != nil {
		return false, false
	}
	existing := make(map[string]struct{}, len(targetTables))
	for _, table := range targetTables {
		existing[table] = struct{}{}
	}
	for _, table := range selected {
		if _, exists := existing[table]; !exists {
			continue
		}
		nonEmpty, err := preflightTableNonEmpty(
			ctx,
			target.database,
			target.endpoint,
			table,
		)
		if err != nil {
			return false, false
		}
		if nonEmpty {
			return true, true
		}
	}
	return false, true
}

func preflightTableNonEmpty(
	ctx context.Context,
	database *sql.DB,
	endpoint config.Endpoint,
	table string,
) (bool, error) {
	qualified := preflightQualifiedTable(endpoint, table)
	query := "SELECT 1 FROM " + qualified + " LIMIT 1"
	if endpoint.Type == "mssql" {
		query = "SELECT TOP (1) 1 FROM " + qualified
	}
	var one int
	err := database.QueryRowContext(ctx, query).Scan(&one)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, err
	}
}

func preflightNamespace(endpoint config.Endpoint) string {
	if endpoint.Schema != "" {
		return endpoint.Schema
	}
	switch endpoint.Type {
	case "postgres":
		return "public"
	case "mssql":
		return "dbo"
	case "mysql", "clickhouse":
		return endpoint.Database
	default:
		return "main"
	}
}

func preflightQualifiedTable(
	endpoint config.Endpoint,
	table string,
) string {
	namespace := preflightNamespace(endpoint)
	switch endpoint.Type {
	case "mysql", "clickhouse":
		return clickHousePreflightIdentifier(namespace) + "." +
			clickHousePreflightIdentifier(table)
	case "mssql":
		return "[" + strings.ReplaceAll(namespace, "]", "]]") +
			"].[" + strings.ReplaceAll(table, "]", "]]") + "]"
	default:
		return quotePreflightIdentifier(namespace) + "." +
			quotePreflightIdentifier(table)
	}
}

func quotePreflightIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func clickHousePreflightIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func sqlitePreflightReadOnlyURI(path string) string {
	normalized := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	location := url.URL{Scheme: "file", Path: normalized}
	query := location.Query()
	query.Set("mode", "ro")
	location.RawQuery = query.Encode()
	return location.String()
}

func sqliteTargetPathWriteEvidence(path string) (bool, bool) {
	info, err := os.Stat(path)
	if err == nil {
		if info.IsDir() {
			return false, true
		}
		file, openErr := os.OpenFile(path, os.O_WRONLY, 0)
		if openErr != nil {
			if os.IsPermission(openErr) {
				return false, true
			}
			return false, false
		}
		fileWritable := file.Close() == nil
		parentWritable, parentKnown :=
			sqliteTargetParentWriteAccess(filepath.Dir(path))
		if !parentKnown {
			return false, false
		}
		return fileWritable && parentWritable, true
	}
	if !os.IsNotExist(err) {
		return false, false
	}
	return sqliteTargetParentWriteAccess(filepath.Dir(path))
}

func sqliteTargetPathWritable(path string) bool {
	writable, known := sqliteTargetPathWriteEvidence(path)
	return known && writable
}
