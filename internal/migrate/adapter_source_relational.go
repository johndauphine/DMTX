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
}

type relationalSourceAdapter struct {
	spec      relationalSourceSpec
	database  *sql.DB
	namespace string
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
	return openRelationalSourceAdapter(ctx, endpoint, relationalSourceSpec{
		engine:      "mysql",
		displayName: "MySQL",
		defaultNamespace: func(endpoint config.Endpoint) string {
			return endpoint.Database
		},
		open:           engine.OpenMySQL,
		verify:         engine.VerifyMySQL80Source,
		listTables:     engine.ListMySQLTables,
		inspectTable:   engine.InspectMySQLTable,
		readQuery:      mySQLReadQuery,
		qualifiedTable: mySQLQualified,
		wrapRows:       wrapMySQLSourceRows,
	})
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
		open:           engine.OpenSQLServer,
		listTables:     engine.ListSQLServerTables,
		inspectTable:   engine.InspectSQLServerTable,
		readQuery:      sqlServerReadQuery,
		qualifiedTable: sqlServerQualified,
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
