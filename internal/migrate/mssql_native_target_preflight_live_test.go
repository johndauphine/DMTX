package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerTargetPreflightHazardsLive(t *testing.T) {
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA to run SQL Server target preflight hazards",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Second,
	)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"preflight target",
		endpoint,
	)
	adapter := &sqlServerTargetAdapter{
		database:  database,
		namespace: "dbo",
	}
	prefix := "dmtx_pf_" + strconv.FormatInt(time.Now().UnixNano(), 36)

	t.Run("restricted principal lacks identity frontier authority", func(t *testing.T) {
		principalName := prefix + "_identity_restricted"
		principal := sqlServerIdentifier(principalName)
		connection, err := database.Conn(ctx)
		if err != nil {
			t.Fatalf("pin restricted-principal connection: %v", err)
		}
		defer connection.Close()

		principalCreated := false
		impersonated := false
		defer cleanupSQLServerRestrictedPrincipal(
			t,
			database,
			connection,
			principal,
			&principalCreated,
			&impersonated,
		)

		if _, err := connection.ExecContext(
			ctx,
			"CREATE USER "+principal+" WITHOUT LOGIN",
		); err != nil {
			t.Fatalf("create restricted SQL Server principal: %v", err)
		}
		principalCreated = true
		grants := []string{
			"GRANT VIEW DEFINITION TO " + principal,
			"GRANT CONTROL ON SCHEMA::[dbo] TO " + principal,
			"GRANT CREATE TABLE TO " + principal,
		}
		for _, statement := range grants {
			if _, err := connection.ExecContext(
				ctx,
				statement,
			); err != nil {
				t.Fatalf(
					"grant restricted SQL Server target authority: %v",
					err,
				)
			}
		}

		if _, err := connection.ExecContext(
			ctx,
			"EXECUTE AS USER = N'"+
				strings.ReplaceAll(principalName, "'", "''")+"'",
		); err != nil {
			t.Fatalf("execute as restricted SQL Server principal: %v", err)
		}
		impersonated = true
		if err := preflightSQLServerTargetPermissions(
			ctx,
			connection,
			"dbo",
			"drop_recreate",
			false,
		); err != nil {
			t.Fatalf(
				"restricted principal lacks ordinary target authority: %v",
				err,
			)
		}
		err = preflightSQLServerDDLTriggers(ctx, connection)
		if err == nil ||
			!strings.Contains(err.Error(), "VIEW ANY DEFINITION") {
			t.Fatalf(
				"restricted server-trigger visibility error = %v",
				err,
			)
		}
		err = preflightSQLServerTargetPermissions(
			ctx,
			connection,
			"dbo",
			"drop_recreate",
			true,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "identity frontiers") {
			t.Fatalf(
				"restricted identity-authority preflight error = %v",
				err,
			)
		}

		if err := revertSQLServerPreflightPrincipal(
			ctx,
			connection,
		); err != nil {
			t.Fatalf("revert restricted SQL Server principal: %v", err)
		}
		impersonated = false
		var fixedRoles int
		if err := connection.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			   FROM sys.database_role_members AS membership
			   JOIN sys.database_principals AS member_principal
			     ON member_principal.principal_id =
			        membership.member_principal_id
			   JOIN sys.database_principals AS role_principal
			     ON role_principal.principal_id =
			        membership.role_principal_id
			  WHERE member_principal.name = @p1
			    AND role_principal.name IN ('db_owner', 'db_ddladmin')`,
			principalName,
		).Scan(&fixedRoles); err != nil {
			t.Fatalf("read restricted principal role membership: %v", err)
		}
		if fixedRoles != 0 {
			t.Fatalf(
				"restricted principal fixed-role memberships = %d",
				fixedRoles,
			)
		}
	})

	t.Run("object DENY blocks all retained tables before mutation", func(t *testing.T) {
		firstName := prefix + "_permission_first"
		secondName := prefix + "_permission_second"
		first := sqlServerPreflightSentinelTable(firstName)
		second := sqlServerPreflightSentinelTable(secondName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", firstName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", secondName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, first)
		createSQLServerPreflightSentinel(t, ctx, database, second)

		principalName := prefix + "_permission_restricted"
		principal := sqlServerIdentifier(principalName)
		connection, err := database.Conn(ctx)
		if err != nil {
			t.Fatalf("pin object-DENY connection: %v", err)
		}
		defer connection.Close()
		principalCreated := false
		impersonated := false
		defer cleanupSQLServerRestrictedPrincipal(
			t,
			database,
			connection,
			principal,
			&principalCreated,
			&impersonated,
		)

		if _, err := connection.ExecContext(
			ctx,
			"CREATE USER "+principal+" WITHOUT LOGIN",
		); err != nil {
			t.Fatalf("create object-DENY principal: %v", err)
		}
		principalCreated = true
		grants := []string{
			"GRANT VIEW DEFINITION TO " + principal,
			"GRANT CONTROL ON SCHEMA::[dbo] TO " + principal,
			"GRANT CREATE TABLE TO " + principal,
		}
		for _, statement := range grants {
			if _, err := connection.ExecContext(
				ctx,
				statement,
			); err != nil {
				t.Fatalf(
					"grant object-DENY target authority: %v",
					err,
				)
			}
		}
		if _, err := connection.ExecContext(
			ctx,
			"DENY UPDATE ON OBJECT::"+
				sqlServerQualified("dbo", secondName)+
				" TO "+principal,
		); err != nil {
			t.Fatalf("deny second-table UPDATE: %v", err)
		}
		if _, err := connection.ExecContext(
			ctx,
			"EXECUTE AS USER = N'"+
				strings.ReplaceAll(principalName, "'", "''")+"'",
		); err != nil {
			t.Fatalf("execute as object-DENY principal: %v", err)
		}
		impersonated = true
		if err := preflightSQLServerTargetPermissions(
			ctx,
			connection,
			"dbo",
			"upsert",
			false,
		); err != nil {
			t.Fatalf(
				"object-DENY principal lacks schema preflight authority: %v",
				err,
			)
		}
		err = preflightSQLServerTargetObjectPermissions(
			ctx,
			connection,
			[]schema.Table{first, second},
			"upsert",
		)
		if err == nil ||
			!strings.Contains(err.Error(), secondName) ||
			!strings.Contains(err.Error(), "UPDATE") {
			t.Fatalf("second-table object-DENY error = %v", err)
		}
		if err := revertSQLServerPreflightPrincipal(
			ctx,
			connection,
		); err != nil {
			t.Fatalf("revert object-DENY principal: %v", err)
		}
		impersonated = false
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			connection,
			firstName,
		)
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			connection,
			secondName,
		)
	})

	t.Run("schema component DENY blocks drop before mutation", func(t *testing.T) {
		firstName := prefix + "_schema_deny_first"
		secondName := prefix + "_schema_deny_second"
		first := sqlServerPreflightSentinelTable(firstName)
		second := sqlServerPreflightSentinelTable(secondName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", firstName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", secondName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, first)
		createSQLServerPreflightSentinel(t, ctx, database, second)

		principalName := prefix + "_schema_deny_restricted"
		principal := sqlServerIdentifier(principalName)
		connection, err := database.Conn(ctx)
		if err != nil {
			t.Fatalf("pin schema-DENY connection: %v", err)
		}
		defer connection.Close()
		principalCreated := false
		impersonated := false
		defer cleanupSQLServerRestrictedPrincipal(
			t,
			database,
			connection,
			principal,
			&principalCreated,
			&impersonated,
		)
		if _, err := connection.ExecContext(
			ctx,
			"CREATE USER "+principal+" WITHOUT LOGIN",
		); err != nil {
			t.Fatalf("create schema-DENY principal: %v", err)
		}
		principalCreated = true
		grants := []string{
			"GRANT VIEW DEFINITION TO " + principal,
			"GRANT CONTROL ON SCHEMA::[dbo] TO " + principal,
			"GRANT CREATE TABLE TO " + principal,
			"DENY INSERT ON SCHEMA::[dbo] TO " + principal,
		}
		for _, statement := range grants {
			if _, err := connection.ExecContext(
				ctx,
				statement,
			); err != nil {
				t.Fatalf(
					"configure schema-DENY target authority: %v",
					err,
				)
			}
		}
		if _, err := connection.ExecContext(
			ctx,
			"EXECUTE AS USER = N'"+
				strings.ReplaceAll(principalName, "'", "''")+"'",
		); err != nil {
			t.Fatalf("execute as schema-DENY principal: %v", err)
		}
		impersonated = true
		var schemaControl, schemaInsert bool
		if err := connection.QueryRowContext(
			ctx,
			`SELECT
				CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
					'dbo', 'SCHEMA', 'CONTROL'
				), 0)),
				CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
					'dbo', 'SCHEMA', 'INSERT'
				), 0))`,
		).Scan(&schemaControl, &schemaInsert); err != nil {
			t.Fatalf("read schema-DENY effective permissions: %v", err)
		}
		if !schemaControl || schemaInsert {
			t.Fatalf(
				"schema-DENY permissions control=%t insert=%t",
				schemaControl,
				schemaInsert,
			)
		}
		err = preflightSQLServerTargetPermissions(
			ctx,
			connection,
			"dbo",
			"drop_recreate",
			false,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "effective schema") {
			t.Fatalf("schema-DENY preflight error = %v", err)
		}
		if err := revertSQLServerPreflightPrincipal(
			ctx,
			connection,
		); err != nil {
			t.Fatalf("revert schema-DENY principal: %v", err)
		}
		impersonated = false
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			connection,
			firstName,
		)
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			connection,
			secondName,
		)
	})

	t.Run("catalog spelling collision has zero mutation", func(t *testing.T) {
		actualName := prefix + "_CaseAlias"
		plannedName := strings.ToLower(actualName)
		actual := sqlServerPreflightSentinelTable(actualName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TABLE IF EXISTS "+sqlServerQualified("dbo", actualName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, actual)

		planned := sqlServerPreflightSentinelTable(plannedName)
		err := adapter.PreflightTables(
			ctx,
			[]schema.Table{planned},
			"drop_recreate",
		)
		if err == nil || !strings.Contains(err.Error(), "catalog spelling") {
			t.Fatalf("catalog-spelling preflight error = %v", err)
		}
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			database,
			actualName,
		)
		var catalogName string
		if err := database.QueryRowContext(
			ctx,
			`SELECT target_table.name
			   FROM sys.tables AS target_table
			   JOIN sys.schemas AS target_schema
			     ON target_schema.schema_id = target_table.schema_id
			  WHERE target_schema.name = 'dbo'
			    AND target_table.name = @p1`,
			plannedName,
		).Scan(&catalogName); err != nil {
			t.Fatalf("read case-alias target spelling: %v", err)
		}
		if catalogName != actualName {
			t.Fatalf(
				"case-alias target spelling = %q, want %q",
				catalogName,
				actualName,
			)
		}
	})

	t.Run("external inbound FK blocks both modes", func(t *testing.T) {
		targetName := prefix + "_fk_target"
		externalName := prefix + "_fk_external"
		foreignKeyName := prefix + "_inbound_fk"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", externalName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+sqlServerQualified("dbo", externalName)+
				" ([id] BIGINT NOT NULL, [target_id] BIGINT NOT NULL, "+
				"CONSTRAINT "+sqlServerIdentifier(foreignKeyName)+
				" FOREIGN KEY ([target_id]) REFERENCES "+
				sqlServerQualified("dbo", targetName)+" ([id]))",
		); err != nil {
			t.Fatalf("create external inbound-FK table: %v", err)
		}
		if _, err := database.ExecContext(
			ctx,
			"INSERT INTO "+sqlServerQualified("dbo", externalName)+
				" ([id], [target_id]) VALUES (1, 1)",
		); err != nil {
			t.Fatalf("insert external inbound-FK sentinel: %v", err)
		}

		for _, mode := range []string{"drop_recreate", "upsert"} {
			err := adapter.PreflightTables(
				ctx,
				[]schema.Table{target},
				mode,
			)
			if err == nil ||
				!strings.Contains(err.Error(), "external table") ||
				!strings.Contains(err.Error(), foreignKeyName) {
				t.Fatalf("%s external-FK preflight error = %v", mode, err)
			}
			assertSQLServerPreflightSentinel(
				t,
				ctx,
				database,
				targetName,
			)
			var externalRows int
			if err := database.QueryRowContext(
				ctx,
				"SELECT COUNT(*) FROM "+
					sqlServerQualified("dbo", externalName)+
					" WHERE [id] = 1 AND [target_id] = 1",
			).Scan(&externalRows); err != nil {
				t.Fatalf("read external inbound-FK sentinel: %v", err)
			}
			if externalRows != 1 {
				t.Fatalf("external inbound-FK sentinel rows = %d", externalRows)
			}
		}
		var foreignKeys int
		if err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			   FROM sys.foreign_keys
			  WHERE name = @p1`,
			foreignKeyName,
		).Scan(&foreignKeys); err != nil {
			t.Fatalf("read external inbound FK: %v", err)
		}
		if foreignKeys != 1 {
			t.Fatalf("external inbound FK count = %d", foreignKeys)
		}
	})

	t.Run("outgoing FK to unselected table blocks both modes", func(t *testing.T) {
		targetName := prefix + "_outgoing_target"
		externalName := prefix + "_outgoing_external"
		foreignKeyName := prefix + "_outgoing_fk"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", externalName),
		)
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+sqlServerQualified("dbo", externalName)+
				" ([id] BIGINT NOT NULL PRIMARY KEY)",
		); err != nil {
			t.Fatalf("create outgoing-FK referenced table: %v", err)
		}
		if _, err := database.ExecContext(
			ctx,
			"INSERT INTO "+sqlServerQualified("dbo", externalName)+
				" ([id]) VALUES (1)",
		); err != nil {
			t.Fatalf("insert outgoing-FK referenced sentinel: %v", err)
		}
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			"ALTER TABLE "+sqlServerQualified("dbo", targetName)+
				" ADD CONSTRAINT "+
				sqlServerIdentifier(foreignKeyName)+
				" FOREIGN KEY ([id]) REFERENCES "+
				sqlServerQualified("dbo", externalName)+" ([id])",
		); err != nil {
			t.Fatalf("create outgoing foreign key: %v", err)
		}

		for _, mode := range []string{"drop_recreate", "upsert"} {
			err := adapter.PreflightTables(
				ctx,
				[]schema.Table{target},
				mode,
			)
			if err == nil ||
				!strings.Contains(
					err.Error(),
					"references unselected table",
				) ||
				!strings.Contains(err.Error(), foreignKeyName) {
				t.Fatalf("%s outgoing-FK preflight error = %v", mode, err)
			}
			assertSQLServerPreflightSentinel(
				t,
				ctx,
				database,
				targetName,
			)
		}
	})

	t.Run("attached DML trigger blocks both modes", func(t *testing.T) {
		targetName := prefix + "_attached_trigger_target"
		triggerName := prefix + "_attached_trigger"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TRIGGER IF EXISTS "+
				sqlServerQualified("dbo", triggerName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			"CREATE TRIGGER "+sqlServerQualified("dbo", triggerName)+
				" ON "+sqlServerQualified("dbo", targetName)+
				" AFTER INSERT AS BEGIN SET NOCOUNT ON; END",
		); err != nil {
			t.Fatalf("create attached DML trigger: %v", err)
		}

		for _, mode := range []string{"drop_recreate", "upsert"} {
			err := adapter.PreflightTables(
				ctx,
				[]schema.Table{target},
				mode,
			)
			if err == nil ||
				!strings.Contains(err.Error(), "DML triggers") {
				t.Fatalf("%s attached-trigger preflight error = %v", mode, err)
			}
			assertSQLServerPreflightSentinel(
				t,
				ctx,
				database,
				targetName,
			)
		}
	})

	t.Run("table lock on bulk load blocks both modes", func(t *testing.T) {
		targetName := prefix + "_bulk_lock_target"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			`EXEC sys.sp_tableoption
				@TableNamePattern = @p1,
				@OptionName = 'table lock on bulk load',
				@OptionValue = 'true'`,
			"dbo."+targetName,
		); err != nil {
			t.Fatalf("enable table lock on bulk load: %v", err)
		}
		var enabled bool
		if err := database.QueryRowContext(
			ctx,
			`SELECT target_table.lock_on_bulk_load
			   FROM sys.tables AS target_table
			   JOIN sys.schemas AS target_schema
			     ON target_schema.schema_id =
			        target_table.schema_id
			  WHERE target_schema.name = 'dbo'
			    AND target_table.name = @p1`,
			targetName,
		).Scan(&enabled); err != nil {
			t.Fatalf("read table lock on bulk load: %v", err)
		}
		if !enabled {
			t.Fatal("table lock on bulk load fixture was not enabled")
		}

		for _, mode := range []string{"drop_recreate", "upsert"} {
			err := adapter.PreflightTables(
				ctx,
				[]schema.Table{target},
				mode,
			)
			if err == nil ||
				!strings.Contains(
					err.Error(),
					"table lock on bulk load",
				) {
				t.Fatalf("%s bulk-lock preflight error = %v", mode, err)
			}
			assertSQLServerPreflightSentinel(
				t,
				ctx,
				database,
				targetName,
			)
		}
	})

	t.Run("current-database three-part dependency blocks drop", func(t *testing.T) {
		targetName := prefix + "_three_part_target"
		procedureName := prefix + "_three_part_procedure"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP PROCEDURE IF EXISTS "+
				sqlServerQualified("dbo", procedureName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		var databaseName string
		if err := database.QueryRowContext(
			ctx,
			"SELECT DB_NAME()",
		).Scan(&databaseName); err != nil {
			t.Fatalf("read SQL Server target database name: %v", err)
		}
		if _, err := database.ExecContext(
			ctx,
			"CREATE PROCEDURE "+
				sqlServerQualified("dbo", procedureName)+
				" AS BEGIN SET NOCOUNT ON; "+
				"SELECT COUNT_BIG(*) FROM "+
				sqlServerIdentifier(databaseName)+"."+
				sqlServerQualified("dbo", targetName)+"; END",
		); err != nil {
			t.Fatalf("create current-database three-part procedure: %v", err)
		}
		createSQLServerPreflightSentinel(t, ctx, database, target)
		var referencedID sql.NullInt64
		if err := database.QueryRowContext(
			ctx,
			`SELECT dependency.referenced_id
			   FROM sys.sql_expression_dependencies AS dependency
			  WHERE dependency.referencing_id = OBJECT_ID(@p1)
			    AND dependency.referenced_database_name = DB_NAME()
			    AND dependency.referenced_schema_name = 'dbo'
			    AND dependency.referenced_entity_name = @p2`,
			"dbo."+procedureName,
			targetName,
		).Scan(&referencedID); err != nil {
			t.Fatalf("read current-database three-part dependency: %v", err)
		}
		if referencedID.Valid {
			t.Logf(
				"SQL Server resolved deferred current-database three-part dependency to object_id %d",
				referencedID.Int64,
			)
		}

		err := adapter.PreflightTables(
			ctx,
			[]schema.Table{target},
			"drop_recreate",
		)
		if err == nil || !strings.Contains(err.Error(), procedureName) {
			t.Fatalf("three-part dependency preflight error = %v", err)
		}
		if !referencedID.Valid &&
			!strings.Contains(err.Error(), "name-based") {
			t.Fatalf("unresolved three-part dependency error = %v", err)
		}
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			database,
			targetName,
		)
	})

	t.Run("enabled database DDL trigger blocks drop", func(t *testing.T) {
		targetName := prefix + "_ddl_trigger_target"
		triggerName := prefix + "_database_ddl_trigger"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TRIGGER IF EXISTS "+
				sqlServerIdentifier(triggerName)+" ON DATABASE",
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			"CREATE TRIGGER "+sqlServerIdentifier(triggerName)+
				" ON DATABASE FOR CREATE_TABLE AS "+
				"BEGIN SET NOCOUNT ON; END",
		); err != nil {
			t.Fatalf("create database DDL trigger: %v", err)
		}
		err := adapter.PreflightTables(
			ctx,
			[]schema.Table{target},
			"drop_recreate",
		)
		if err == nil ||
			!strings.Contains(err.Error(), "database DDL trigger") ||
			!strings.Contains(err.Error(), triggerName) {
			t.Fatalf("database DDL-trigger preflight error = %v", err)
		}
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			database,
			targetName,
		)
	})

	t.Run("external DML trigger dependency blocks drop", func(t *testing.T) {
		targetName := prefix + "_trigger_target"
		externalName := prefix + "_trigger_external"
		triggerName := prefix + "_external_trigger"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP TRIGGER IF EXISTS "+
				sqlServerQualified("dbo", triggerName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", externalName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			"CREATE TABLE "+sqlServerQualified("dbo", externalName)+
				" ([id] BIGINT NOT NULL)",
		); err != nil {
			t.Fatalf("create external trigger table: %v", err)
		}
		if _, err := database.ExecContext(
			ctx,
			"CREATE TRIGGER "+sqlServerQualified("dbo", triggerName)+
				" ON "+sqlServerQualified("dbo", externalName)+
				" AFTER INSERT AS BEGIN SET NOCOUNT ON; "+
				"DECLARE @rows BIGINT; SELECT @rows = COUNT_BIG(*) FROM "+
				sqlServerQualified("dbo", targetName)+"; END",
		); err != nil {
			t.Fatalf("create external dependent trigger: %v", err)
		}
		var dependencies int
		if err := database.QueryRowContext(
			ctx,
			`SELECT COUNT(*)
			   FROM sys.sql_expression_dependencies
			  WHERE referencing_id = OBJECT_ID(@p1)
			    AND referenced_id = OBJECT_ID(@p2)`,
			"dbo."+triggerName,
			"dbo."+targetName,
		).Scan(&dependencies); err != nil {
			t.Fatalf("read trigger dependency fixture: %v", err)
		}
		if dependencies != 1 {
			t.Fatalf("trigger dependency count = %d, want 1", dependencies)
		}

		err := adapter.PreflightTables(
			ctx,
			[]schema.Table{target},
			"drop_recreate",
		)
		if err == nil ||
			!strings.Contains(err.Error(), "SQL_TRIGGER") ||
			!strings.Contains(err.Error(), triggerName) {
			t.Fatalf("external-trigger preflight error = %v", err)
		}
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			database,
			targetName,
		)
		var triggers int
		if err := database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM sys.triggers WHERE name = @p1",
			triggerName,
		).Scan(&triggers); err != nil {
			t.Fatalf("read external dependent trigger: %v", err)
		}
		if triggers != 1 {
			t.Fatalf("external dependent trigger count = %d", triggers)
		}
	})

	t.Run("any synonym prevents dependency proof", func(t *testing.T) {
		targetName := prefix + "_synonym_target"
		synonymName := prefix + "_synonym"
		target := sqlServerPreflightSentinelTable(targetName)
		cleanupSQLServerPreflightLive(
			t,
			database,
			"DROP SYNONYM IF EXISTS "+
				sqlServerQualified("dbo", synonymName),
			"DROP TABLE IF EXISTS "+
				sqlServerQualified("dbo", targetName),
		)
		createSQLServerPreflightSentinel(t, ctx, database, target)
		if _, err := database.ExecContext(
			ctx,
			"CREATE SYNONYM "+sqlServerQualified("dbo", synonymName)+
				" FOR "+sqlServerQualified("dbo", targetName),
		); err != nil {
			t.Fatalf("create SQL Server synonym fixture: %v", err)
		}

		err := adapter.PreflightTables(
			ctx,
			[]schema.Table{target},
			"drop_recreate",
		)
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"prevents a complete dependency proof",
			) {
			t.Fatalf("synonym preflight error = %v", err)
		}
		assertSQLServerPreflightSentinel(
			t,
			ctx,
			database,
			targetName,
		)
		var synonyms int
		if err := database.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM sys.synonyms WHERE name = @p1",
			synonymName,
		).Scan(&synonyms); err != nil {
			t.Fatalf("read SQL Server synonym fixture: %v", err)
		}
		if synonyms != 1 {
			t.Fatalf("SQL Server synonym fixture count = %d", synonyms)
		}
	})
}

func sqlServerPreflightSentinelTable(name string) schema.Table {
	return schema.Table{
		Schema: "dbo",
		Name:   name,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
			{
				Name: "payload",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{32},
				},
			},
		},
	}
}

func createSQLServerPreflightSentinel(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	table schema.Table,
) {
	t.Helper()
	statement, err := schema.CreateSQLServerTable(table)
	if err != nil {
		t.Fatalf("render SQL Server preflight sentinel: %v", err)
	}
	if _, err := database.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create SQL Server preflight sentinel %s: %v", table.Name, err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+sqlServerQualified(table.Schema, table.Name)+
			" ([id], [payload]) VALUES (1, 'must remain')",
	); err != nil {
		t.Fatalf("insert SQL Server preflight sentinel %s: %v", table.Name, err)
	}
}

func assertSQLServerPreflightSentinel(
	t *testing.T,
	ctx context.Context,
	queryer sqlServerCatalogQueryer,
	name string,
) {
	t.Helper()
	var payload string
	if err := queryer.QueryRowContext(
		ctx,
		"SELECT [payload] FROM "+sqlServerQualified("dbo", name)+
			" WHERE [id] = 1",
	).Scan(&payload); err != nil {
		t.Fatalf("read SQL Server preflight sentinel %s: %v", name, err)
	}
	if payload != "must remain" {
		t.Fatalf("SQL Server preflight sentinel %s = %q", name, payload)
	}
}

func revertSQLServerPreflightPrincipal(
	ctx context.Context,
	connection *sql.Conn,
) error {
	var marker int
	if err := connection.QueryRowContext(
		ctx,
		"REVERT; SELECT 1",
	).Scan(&marker); err != nil {
		return err
	}
	if marker != 1 {
		return fmt.Errorf("unexpected REVERT marker")
	}
	return nil
}

func cleanupSQLServerRestrictedPrincipal(
	t *testing.T,
	database *sql.DB,
	connection *sql.Conn,
	principal string,
	principalCreated *bool,
	impersonated *bool,
) {
	t.Helper()
	cleanupContext, cleanupCancel := context.WithTimeout(
		context.Background(),
		15*time.Second,
	)
	defer cleanupCancel()
	usePinnedConnection := true
	if *impersonated {
		if err := revertSQLServerPreflightPrincipal(
			cleanupContext,
			connection,
		); err != nil {
			t.Errorf("revert restricted SQL Server principal: %v", err)
			// Never return an unproven impersonated session to database/sql.
			_ = connection.Raw(func(any) error {
				return driver.ErrBadConn
			})
			_ = connection.Close()
			usePinnedConnection = false
		}
		*impersonated = false
	}
	if *principalCreated {
		var err error
		if usePinnedConnection {
			_, err = connection.ExecContext(
				cleanupContext,
				"DROP USER IF EXISTS "+principal,
			)
		} else {
			_, err = database.ExecContext(
				cleanupContext,
				"DROP USER IF EXISTS "+principal,
			)
		}
		if err != nil {
			t.Errorf("drop restricted SQL Server principal: %v", err)
		} else {
			*principalCreated = false
		}
	}
}

func cleanupSQLServerPreflightLive(
	t *testing.T,
	database *sql.DB,
	statements ...string,
) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cancel()
		for _, statement := range statements {
			if _, err := database.ExecContext(ctx, statement); err != nil {
				t.Errorf(
					"clean SQL Server preflight fixture with %q: %v",
					statement,
					err,
				)
			}
		}
	})
}
