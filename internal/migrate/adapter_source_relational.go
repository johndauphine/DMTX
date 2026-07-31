package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

type relationalSourceSpec struct {
	engine           string
	displayName      string
	defaultNamespace func(config.Endpoint) string
	open             func(context.Context, config.Endpoint) (*sql.DB, error)
	verify           func(context.Context, *sql.DB) error
	listTables       func(context.Context, *sql.DB, string) ([]string, error)
	inspectTable     func(context.Context, *sql.DB, string, string) (schema.Table, error)
	readQuery        func(string, schema.Table, []string) string
	qualifiedTable   func(string, string) string
	wrapRows         func(adapterRows, schema.Table, []string) adapterRows
	preflightRows    func(context.Context, *sql.DB, string, []schema.Table) error
}

type relationalSourceAdapter struct {
	spec      relationalSourceSpec
	database  *sql.DB
	namespace string
	// mySQLFlavor is live server evidence recorded before a table-stable
	// session pins the source's only connection. It is revalidated by the
	// flavor-specific inspector inside that pinned transaction.
	mySQLFlavor engine.MySQLServerFlavor
}

func (adapter *relationalSourceAdapter) postgresDatabaseHandle() *sql.DB {
	if adapter == nil || adapter.spec.engine != "postgres" {
		return nil
	}
	return adapter.database
}

func (adapter *relationalSourceAdapter) mySQLDatabaseHandle() *sql.DB {
	if adapter == nil || adapter.spec.engine != "mysql" {
		return nil
	}
	return adapter.database
}

func (adapter *relationalSourceAdapter) sqlServerDatabaseHandle() *sql.DB {
	if adapter == nil || adapter.spec.engine != "mssql" {
		return nil
	}
	return adapter.database
}

func openPostgresSourceAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (sourceAdapter, error) {
	return openRelationalSourceAdapter(ctx, endpoint, relationalSourceSpec{
		engine:      "postgres",
		displayName: "PostgreSQL",
		defaultNamespace: func(config.Endpoint) string {
			return "public"
		},
		open:           engine.OpenPostgres,
		verify:         engine.VerifyPostgres16Source,
		listTables:     engine.ListPostgresTables,
		inspectTable:   engine.InspectPostgresTable,
		readQuery:      postgresReadQuery,
		qualifiedTable: postgresQualified,
	})
}

func openMySQLSourceAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (sourceAdapter, error) {
	source, err := openRelationalSourceAdapter(ctx, endpoint, relationalSourceSpec{
		engine:      "mysql",
		displayName: "MySQL/MariaDB",
		defaultNamespace: func(endpoint config.Endpoint) string {
			return endpoint.Database
		},
		open:           engine.OpenMySQLSource,
		verify:         engine.VerifyMySQLSource,
		listTables:     engine.ListMySQLTables,
		inspectTable:   engine.InspectMySQLTable,
		readQuery:      mySQLReadQuery,
		qualifiedTable: mySQLQualified,
		wrapRows:       wrapMySQLSourceRows,
		preflightRows:  preflightMySQLSourceRows,
	})
	if err != nil {
		return nil, err
	}
	adapter, ok := source.(*relationalSourceAdapter)
	if !ok || adapter == nil {
		_ = source.Close()
		return nil, fmt.Errorf(
			"MySQL-family source opened with an invalid adapter",
		)
	}
	flavor, err := engine.DetectMySQLServerFlavor(ctx, adapter.database)
	if err != nil {
		if closeErr := adapter.Close(); closeErr != nil {
			return nil, fmt.Errorf(
				"record MySQL-family source flavor: %w (close: %v)",
				err,
				closeErr,
			)
		}
		return nil, fmt.Errorf(
			"record MySQL-family source flavor: %w",
			err,
		)
	}
	adapter.mySQLFlavor = flavor
	return adapter, nil
}

func openSQLServerSourceAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
) (sourceAdapter, error) {
	return openRelationalSourceAdapter(ctx, endpoint, relationalSourceSpec{
		engine:      "mssql",
		displayName: "SQL Server",
		defaultNamespace: func(config.Endpoint) string {
			return "dbo"
		},
		open:           engine.OpenSQLServer2022Source,
		verify:         engine.VerifySQLServer2022Source,
		listTables:     engine.ListSQLServerTables,
		inspectTable:   engine.InspectSQLServerTable,
		readQuery:      sqlServerReadQuery,
		qualifiedTable: sqlServerQualified,
		wrapRows:       wrapSQLServerSourceRows,
	})
}

func openRelationalSourceAdapter(
	ctx context.Context,
	endpoint config.Endpoint,
	spec relationalSourceSpec,
) (sourceAdapter, error) {
	resolved, err := resolvedEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	database, err := spec.open(ctx, resolved)
	if err != nil {
		return nil, err
	}
	if spec.verify != nil {
		if err := spec.verify(ctx, database); err != nil {
			if closeErr := database.Close(); closeErr != nil {
				return nil, fmt.Errorf(
					"verify %s source: %w (close: %v)",
					spec.displayName,
					err,
					closeErr,
				)
			}
			return nil, fmt.Errorf(
				"verify %s source: %w",
				spec.displayName,
				err,
			)
		}
	}
	namespace := resolved.Schema
	if namespace == "" {
		namespace = spec.defaultNamespace(resolved)
	}
	return &relationalSourceAdapter{
		spec:      spec,
		database:  database,
		namespace: namespace,
	}, nil
}

func resolvedEndpoint(endpoint config.Endpoint) (config.Endpoint, error) {
	password, err := config.ExpandSecret(endpoint.Password)
	if err != nil {
		return config.Endpoint{}, fmt.Errorf("resolve source password: %w", err)
	}
	endpoint.Password = password
	return endpoint, nil
}

func (adapter *relationalSourceAdapter) Engine() string {
	return adapter.spec.engine
}

func (adapter *relationalSourceAdapter) DisplayName() string {
	return adapter.spec.displayName
}

func (adapter *relationalSourceAdapter) ListTables(
	ctx context.Context,
) ([]string, error) {
	return adapter.spec.listTables(ctx, adapter.database, adapter.namespace)
}

func (adapter *relationalSourceAdapter) InspectTable(
	ctx context.Context,
	name string,
) (schema.Table, error) {
	return adapter.spec.inspectTable(
		ctx,
		adapter.database,
		adapter.namespace,
		name,
	)
}

func (adapter *relationalSourceAdapter) OpenRows(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (adapterRows, error) {
	rows, err := adapter.database.QueryContext(
		ctx,
		adapter.spec.readQuery(adapter.namespace, table, columns),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"read %s table %s: %w",
			adapter.spec.displayName,
			table.Name,
			err,
		)
	}
	var result adapterRows = rows
	if adapter.spec.wrapRows != nil {
		result = adapter.spec.wrapRows(result, table, columns)
	}
	return result, nil
}

func (adapter *relationalSourceAdapter) PreflightRows(
	ctx context.Context,
	tables []schema.Table,
) error {
	if adapter.spec.preflightRows == nil {
		return nil
	}
	return adapter.spec.preflightRows(
		ctx,
		adapter.database,
		adapter.namespace,
		tables,
	)
}

func (adapter *relationalSourceAdapter) CountRows(
	ctx context.Context,
	table schema.Table,
) (int, error) {
	var count int
	err := adapter.database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			adapter.spec.qualifiedTable(adapter.namespace, table.Name),
	).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf(
			"count %s table %s: %w",
			adapter.spec.displayName,
			table.Name,
			err,
		)
	}
	return count, nil
}

func (adapter *relationalSourceAdapter) Close() error {
	return adapter.database.Close()
}
