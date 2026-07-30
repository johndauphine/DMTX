# Stage 3 requirements and test evidence

This file maps the literal Stage 3 deliverable and exit criterion in
`RECREATE_DMT.md` to the repository's test fixtures. The implementations and
named fixtures are present, and the normal and race live matrices have passed.
The literal Stage 3 exit criterion is complete. This is the reproducible
execution checklist for that evidence.

## Required environment

Set `DMTX_STAGE3_LIVE_REQUIRED=1` for an exit-gate run. The
`TestStage3LiveMatrixEnvironmentRequired` sentinel then fails instead of
allowing live tests to skip when any required PostgreSQL, MySQL 8, MariaDB
10.11, SQL Server 2022, or ClickHouse 24.8 TLS setting is absent.

Every route requires encrypted TLS. MySQL/MariaDB, SQL Server, and ClickHouse
live endpoints use CA verification. The PostgreSQL bootstrap DSN is
`verify-full`, while the adapter currently uses `sslmode=require`; Stage 3
does not claim PostgreSQL adapter certificate verification.

The required variables are:

- `DMTX_TEST_POSTGRES_DSN`
- `DMTX_TEST_MYSQL_DSN`, `DMTX_TEST_MYSQL_TARGET_DSN`,
  `DMTX_TEST_MYSQL_CA`
- `DMTX_TEST_MARIADB_DSN`, `DMTX_TEST_MARIADB_TARGET_DSN`,
  `DMTX_TEST_MARIADB_CA`
- `DMTX_TEST_MSSQL_DSN`, `DMTX_TEST_MSSQL_TARGET_DSN`,
  `DMTX_TEST_MSSQL_CA`
- `DMTX_TEST_CLICKHOUSE_DSN`, `DMTX_TEST_CLICKHOUSE_SOURCE_DSN`,
  `DMTX_TEST_CLICKHOUSE_TARGET_DSN`, `DMTX_TEST_CLICKHOUSE_CA`

The MariaDB native-path sentinels additionally require global
`local_infile=ON` and target `CREATE TEMPORARY TABLES` permission. The Oracle
MySQL fallback sentinel intentionally uses a target with `local_infile=OFF`;
unavailable local infile or staging must warn once and latch strict bounded
inserts rather than silently skipping the fixture.

## Exit-criterion map

| Requirement | Exact fixtures |
|---|---|
| PostgreSQL, SQL Server, MySQL/MariaDB source discovery and target admission | `TestInspectPostgres16SourceSchemaLive`, `TestInspectSQLServer2022SourceSchemaLive`, `TestInspectMySQL80SourceSchemaLive`, `TestInspectMariaDB1011SourceSchemaLive`, `TestOpenMariaDB1011TargetLive`, plus the target route fixtures below |
| PostgreSQL native COPY and staged upsert | `TestPostgresNativeWriterLive`, plus the PostgreSQL-target route fixtures below |
| SQL Server native target and upsert | `TestSQLServerToSQLServerCommonFixtureLive`, `TestPostgresToSQLServerCommonFixtureLive`, `TestSQLiteToSQLServerCommonFixtureLive`, `TestMySQLToSQLServerCommonFixtureLive`, `TestMariaDBToSQLServerCommonFixtureLive` |
| MySQL/MariaDB native target and upsert | `TestMySQLToMySQLCommonFixtureLive`, `TestMariaDBToMariaDBCommonFixtureLive`, and the MySQL-family target routes below |
| MySQL/MariaDB native bulk strictness | `TestMySQLNativeWriterLocalInfileFallbackLive`, `TestMariaDBNativeWriterLocalInfileRoundTripLive`, `TestMariaDBNativeWriterLocalInfileWarningLeavesTargetUntouchedLive`; all passed in the mandatory live matrix |
| Twelve directed relational pairs | PostgreSQL → SQL Server/MySQL/SQLite: `TestPostgresToSQLServerCommonFixtureLive`, `TestPostgresToMySQLCommonFixtureLive`, `TestPostgresToSQLiteCommonFixtureLive`; SQL Server → PostgreSQL/MySQL/SQLite: `TestSQLServerToPostgresCommonFixtureLive`, `TestSQLServerToMySQLCommonFixtureLive`, `TestSQLServerToSQLiteCommonFixtureLive`; MySQL → PostgreSQL/SQL Server/SQLite: `TestMySQLToPostgresCommonFixtureLive`, `TestMySQLToSQLServerCommonFixtureLive`, `TestMySQLToSQLiteCommonFixtureLive`; SQLite → PostgreSQL/SQL Server/MySQL: `TestSQLiteToPostgresComposedRouteLive`, `TestSQLiteToPostgresRichSchemaObjectsLive`, `TestSQLiteToSQLServerCommonFixtureLive`, `TestSQLiteToMySQLCommonFixtureLive` |
| MariaDB certification for MySQL-family directions | `TestPostgresToMariaDBCommonFixtureLive`, `TestSQLServerToMariaDBCommonFixtureLive`, `TestMariaDBToPostgresCommonFixtureLive`, `TestMariaDBToSQLServerCommonFixtureLive`, `TestMariaDBToSQLiteCommonFixtureLive`, `TestSQLiteToMariaDBCommonFixtureLive` |
| Same-engine fixtures | `TestPostgresToPostgresCommonFixtureLive`, `TestSQLServerToSQLServerCommonFixtureLive`, `TestMySQLToMySQLCommonFixtureLive`, `TestMariaDBToMariaDBCommonFixtureLive`, `TestSQLiteToSQLitePreservesRichSemanticSchemaAndExactRows` |
| Same-database alias guards | `TestPostgresToPostgresRejectsLiveSameDatabaseAlias`, `TestSQLServerToSQLServerRejectsLiveSameDatabaseAlias`, `TestMySQLToMySQLRejectsLiveSameDatabaseAlias`, `TestMariaDBToMariaDBRejectsLiveSameDatabaseAlias`; the ClickHouse same-database sentinel is part of `TestClickHouse248ToClickHouse248RebuildLive` |
| ClickHouse conformance and live integration | `TestClickHouse248SourceDiscoveryLive`, `TestSQLiteToClickHouse248ComposedRouteLive`, `TestClickHouse248ToClickHouse248RebuildLive`, `TestClickHouse248TargetCheckpointRaceLive`, `TestClickHouse248PrepareDropsAllBeforeCreateFailureLive`, `TestClickHouseNativeWriterDurablySendsBoundedBatch` |
| Unsupported capabilities fail before adapter construction or mutation | `TestCapabilityValidationPrecedesAdapterConstruction`, `TestClickHouseNativeRouteFailsUnsupportedCapabilitiesBeforeOpen`, `TestBuiltInRoutesRejectStrictConsistencyScopes` |
| Certified route inventory remains exact | `TestBuiltInAdaptersPreserveCertifiedRoutes` |

## Aggregate Stage 3 gate

Run this only against disposable live databases. The regular test suite may
skip live fixtures; this command makes missing live configuration fatal.

```sh
DMTX_STAGE3_LIVE_REQUIRED=1 go test -v ./internal/engine ./internal/migrate -count=1 -run \
'^(TestStage3LiveMatrixEnvironmentRequired|'\
'TestInspect(Postgres16|SQLServer2022|MySQL80|MariaDB1011)SourceSchemaLive|'\
'TestOpenMariaDB1011TargetLive|TestPostgresNativeWriterLive|'\
'Test(PostgresTo(Postgres|SQLServer|MySQL|MariaDB|SQLite)|SQLServerTo(SQLServer|Postgres|MySQL|MariaDB|SQLite)|MySQLTo(MySQL|Postgres|SQLServer|SQLite)|MariaDBTo(MariaDB|Postgres|SQLServer|SQLite)|SQLiteTo(SQLServer|MySQL|MariaDB))CommonFixtureLive|'\
'Test(PostgresToPostgres|SQLServerToSQLServer|MySQLToMySQL|MariaDBToMariaDB)RejectsLiveSameDatabaseAlias|'\
'TestSQLiteToPostgres(ComposedRoute|RichSchemaObjects)Live|'\
'TestSQLiteToSQLitePreservesRichSemanticSchemaAndExactRows|'\
'Test(MySQLNativeWriterLocalInfileFallback|MariaDBNativeWriterLocalInfileRoundTrip|MariaDBNativeWriterLocalInfileWarningLeavesTargetUntouched)Live|'\
'TestClickHouse248(SourceDiscovery|ToClickHouse248Rebuild|TargetCheckpointRace|PrepareDropsAllBeforeCreateFailure)Live|'\
'TestSQLiteToClickHouse248ComposedRouteLive|'\
'TestClickHouseNativeWriterDurablySendsBoundedBatch|'\
'TestTargetCapabilitiesMatchRequiredDifferences|'\
'TestCapabilityValidationPrecedesAdapterConstruction|'\
'TestClickHouseNativeRouteFailsUnsupportedCapabilitiesBeforeOpen|'\
'TestBuiltInRoutesRejectStrictConsistencyScopes|'\
'TestBuiltInAdaptersPreserveCertifiedRoutes)$'
```

Repeat the same command with `go test -race -v` in place of `go test -v`.
Operators must inspect both verbose outputs and confirm that none of the named
fixtures produced a `SKIP` line.

Recorded on 2026-07-30:

- the complete mandatory-environment TLS matrix passed verbosely on Linux,
  with every named fixture running and no skips: engine `0.909s`, migrate
  `9.009s`;
- its complete `go test -race -v` counterpart also passed with no skips:
  engine `1.824s`, migrate `14.396s`;
- the network/ClickHouse subset passed in normal and race modes on macOS; the
  SQLite-to-SQLite fixture runs in Linux because DMTX's safe finite-memory
  probe does not admit migrations on Darwin;
- full Linux `go test ./...` passed; and
- full Linux `go test -race ./...` passed, including the SQLite same-engine
  fixture.

Compilation, `go vet ./...`, native and Windows command builds, and
`git diff --check` also passed. Every named fixture is present and passed; the
literal Stage 3 exit criterion is complete at implementation checkpoint
`4ea4bc7a90ab4f07c2537715b4e9138ad9c8a2d3`.
