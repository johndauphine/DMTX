package app

import (
	"context"
	"database/sql"
	"strconv"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/migrate"
)

// The individual preflight facts: what each check asserts about an endpoint
// and how it reports insufficient evidence.

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
