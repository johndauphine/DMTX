package migrate

import "github.com/johndauphine/dmtx/internal/engine"

// builtInAdapters contains the recognized source and target roles and the
// routes whose current implementations are certified for execution.
//
// SQLite sources compose with the PostgreSQL target. PostgreSQL and
// version-pinned MySQL/MariaDB sources compose with PostgreSQL, SQLite, and
// the native MySQL and SQL Server targets where explicitly certified;
// unsupported flavor/target combinations still fail during read-only
// planning. SQL Server and PostgreSQL sources compose with both the native
// SQL Server target and the admitted native MySQL-family targets where
// explicitly certified. SQLite and SQL Server compose through their shared
// conservative contracts; unsupported SQLite storage and comparison shapes
// remain fail-closed. SQL Server's SQLite target is likewise certified for its
// conservative upsert and drop/recreate contract. SQLite composes with the pinned
// ClickHouse 24.8 target under its strict rebuild-only contract.
// ClickHouse 24.8 also composes with a distinct ClickHouse Atomic database for
// the narrow same-engine rebuild shape whose ordering metadata is explicitly
// non-unique.
// The SQLite-to-SQLite compatibility implementation remains outside the
// network-adapter composition layer.
var builtInAdapters = mustBuildAdapterRegistry(
	[]sourceRole{
		{
			engine:   "sqlite",
			validate: validateSQLiteSourceEndpoint,
			open:     openSQLiteSourceAdapter,
		},
		{
			engine:   "postgres",
			validate: validatePostgresSourceEndpoint,
			open:     openPostgresSourceAdapter,
		},
		{engine: "mysql", open: openMySQLSourceAdapter},
		{engine: "mssql", open: openSQLServerSourceAdapter},
		{
			engine:   "clickhouse",
			validate: validateClickHouseSourceEndpoint,
			open:     openClickHouseSourceAdapter,
		},
	},
	[]targetRole{
		builtInTargetRole(
			"sqlite",
			validateSQLiteTargetEndpoint,
			openSQLiteTargetAdapter,
		),
		builtInTargetRole(
			"postgres",
			validatePostgresTargetEndpoint,
			openPostgresTargetAdapter,
		),
		builtInTargetRole(
			"mysql",
			validateMySQLTargetEndpoint,
			openMySQLTargetAdapter,
		),
		builtInTargetRole(
			"mssql",
			validateSQLServerTargetEndpoint,
			openSQLServerTargetAdapter,
		),
		builtInTargetRole(
			"clickhouse",
			validateClickHouseTargetEndpoint,
			openClickHouseTargetAdapter,
		),
	},
	[]adapterPair{
		{source: "sqlite", target: "sqlite"},
		{source: "sqlite", target: "postgres"},
		{source: "sqlite", target: "mysql"},
		{source: "sqlite", target: "mssql"},
		{source: "sqlite", target: "clickhouse"},
		{source: "postgres", target: "postgres"},
		{source: "postgres", target: "sqlite"},
		{source: "postgres", target: "mysql"},
		{source: "postgres", target: "mssql"},
		{source: "mysql", target: "postgres"},
		{source: "mysql", target: "sqlite"},
		{source: "mysql", target: "mysql"},
		{source: "mysql", target: "mssql"},
		{source: "mssql", target: "postgres"},
		{source: "mssql", target: "sqlite"},
		{source: "mssql", target: "mysql"},
		{source: "mssql", target: "mssql"},
		{source: "clickhouse", target: "clickhouse"},
	},
	[]adapterOverride{
		{pair: adapterPair{source: "sqlite", target: "sqlite"}, run: SQLiteToSQLiteWithObserver},
	},
)

func builtInTargetRole(
	name string,
	validate endpointValidator,
	open targetAdapterFactory,
) targetRole {
	capability, ok := engine.TargetCapability(name)
	if !ok {
		panic("target adapter " + name + " has no capability declaration")
	}
	return targetRole{
		engine:     name,
		capability: capability,
		validate:   validate,
		open:       open,
	}
}

func mustBuildAdapterRegistry(
	sources []sourceRole,
	targets []targetRole,
	certified []adapterPair,
	overrides []adapterOverride,
) adapterRegistry {
	registry, err := newAdapterRegistry(sources, targets, certified, overrides)
	if err != nil {
		panic("build production adapter registry: " + err.Error())
	}
	return registry
}
