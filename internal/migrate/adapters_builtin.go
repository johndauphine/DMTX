package migrate

import "github.com/johndauphine/dmtx/internal/engine"

// builtInAdapters contains the recognized source and target roles and the
// routes whose current implementations are certified for execution.
//
// SQLite sources compose with the PostgreSQL target. PostgreSQL and
// MySQL/MariaDB sources compose with the PostgreSQL and SQLite targets, while
// SQL Server sources compose with the SQLite target. Compatibility overrides
// preserve the remaining routes until both sides move behind the shared
// contracts.
var builtInAdapters = mustBuildAdapterRegistry(
	[]sourceRole{
		{
			engine:   "sqlite",
			validate: validateSQLiteSourceEndpoint,
			open:     openSQLiteSourceAdapter,
		},
		{engine: "postgres", open: openPostgresSourceAdapter},
		{engine: "mysql", open: openMySQLSourceAdapter},
		{engine: "mssql", open: openSQLServerSourceAdapter},
		{engine: "clickhouse"},
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
		builtInTargetRole("mysql", nil, nil),
		builtInTargetRole("mssql", nil, nil),
		builtInTargetRole("clickhouse", nil, nil),
	},
	[]adapterPair{
		{source: "sqlite", target: "sqlite"},
		{source: "sqlite", target: "postgres"},
		{source: "sqlite", target: "mysql"},
		{source: "sqlite", target: "mssql"},
		{source: "sqlite", target: "clickhouse"},
		{source: "postgres", target: "postgres"},
		{source: "postgres", target: "sqlite"},
		{source: "mysql", target: "postgres"},
		{source: "mysql", target: "sqlite"},
		{source: "mssql", target: "sqlite"},
	},
	[]adapterOverride{
		{pair: adapterPair{source: "sqlite", target: "sqlite"}, run: SQLiteToSQLiteWithObserver},
		{pair: adapterPair{source: "sqlite", target: "mysql"}, run: SQLiteToMySQLWithObserver},
		{pair: adapterPair{source: "sqlite", target: "mssql"}, run: SQLiteToSQLServerWithObserver},
		{pair: adapterPair{source: "sqlite", target: "clickhouse"}, run: SQLiteToClickHouseWithObserver},
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
