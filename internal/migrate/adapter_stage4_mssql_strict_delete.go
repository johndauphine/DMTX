package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/johndauphine/dmtx/internal/schema"
)

// sqlServerStrictDeleteSourceCapability is deliberately narrower than the
// ordinary SQL Server delete source. Its only query path is the retained
// strict reader transaction supplied by a table lock or a database snapshot;
// it has no database-pool handle to accidentally fall back to a live source.
type sqlServerStrictDeleteSourceCapability struct {
	view      *adapterRetainedStableRelationalView
	authority sqlServerDeleteCatalogAuthority
}

// newSQLServerStrictDeleteSourceCapability authenticates the exact source
// table through a retained SQL Server strict view. The caller must remain in
// the reader callback until all returned rows are consumed and closed.
func newSQLServerStrictDeleteSourceCapability(
	ctx context.Context,
	stable adapterStableNetworkSource,
	table schema.Table,
) (*sqlServerStrictDeleteSourceCapability, error) {
	if ctx == nil {
		return nil, errors.New("SQL Server strict delete source context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	view, ok := stable.(*adapterRetainedStableRelationalView)
	if !ok || view == nil {
		return nil, errors.New("SQL Server strict delete source requires a retained strict relational view")
	}
	view.mu.Lock()
	source := view.source
	queryer := view.view
	strict := view.sqlServerStrict
	scope := view.tableScope
	catalog := view.tableCatalog
	view.mu.Unlock()
	if source == nil || isNilInterface(queryer) || source.spec.engine != "mssql" {
		return nil, errors.New("SQL Server strict delete source view is unavailable")
	}
	if !strict {
		return nil, errors.New("SQL Server delete reconciliation requires a retained strict reader, not an ordinary stable source view")
	}
	if scope == nil || catalog == nil ||
		scope.schema != table.Schema || scope.table != table.Name ||
		catalog.Schema != table.Schema || catalog.Name != table.Name {
		return nil, errors.New("SQL Server strict delete source table differs from the retained strict view authority")
	}
	if table.Schema != source.namespace ||
		isSQLServerDeleteJournalNamespace(table.Schema) {
		return nil, errors.New("SQL Server strict delete source table is outside its configured namespace or is reserved private receipt state")
	}
	// InspectTable selects the migration-snapshot catalog reader when needed.
	// That distinction is essential: a database snapshot is read-only and its
	// exact schema authority must not be reopened through the mutable source.
	live, err := view.InspectTable(ctx, table.Name)
	if err != nil {
		return nil, fmt.Errorf("inspect retained SQL Server strict delete source: %w", err)
	}
	authority, err := inspectSQLServerDeleteCatalogAuthorityFromLiveTable(
		ctx,
		queryer,
		source.namespace,
		table,
		live,
	)
	if err != nil {
		return nil, fmt.Errorf("validate retained SQL Server strict delete catalog: %w", err)
	}
	if !authority.CanSelect {
		return nil, errors.New("SQL Server strict delete source requires exact table SELECT privilege")
	}
	// A snapshot may have a different database identity than its writable
	// origin. That is expected, but it is still bound to the concrete retained
	// reader above; source/target endpoint distinctness remains checked against
	// the preflighted writable source identity by the paired constructor below.
	return &sqlServerStrictDeleteSourceCapability{
		view:      view,
		authority: authority,
	}, nil
}

// OpenDeletePrimaryKeys implements the complete source scan exclusively on
// the retained strict reader. It never opens source.database, starts another
// transaction, or owns the reader lifecycle; Close only closes the result
// rows before the surrounding strict callback releases its transaction.
func (capability *sqlServerStrictDeleteSourceCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if ctx == nil {
		return nil, errors.New("SQL Server strict delete key context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if capability == nil || capability.view == nil {
		return nil, errors.New("SQL Server strict delete source is unavailable")
	}
	view := capability.view
	view.mu.Lock()
	source := view.source
	queryer := view.view
	strict := view.sqlServerStrict
	view.mu.Unlock()
	if source == nil || isNilInterface(queryer) || source.spec.engine != "mssql" || !strict {
		return nil, errors.New("SQL Server strict delete source view is unavailable")
	}
	if err := view.admitTable(table); err != nil {
		return nil, err
	}
	live, err := view.InspectTable(ctx, table.Name)
	if err != nil {
		return nil, fmt.Errorf("inspect retained SQL Server strict delete keys: %w", err)
	}
	current, err := inspectSQLServerDeleteCatalogAuthorityFromLiveTable(
		ctx,
		queryer,
		source.namespace,
		table,
		live,
	)
	if err != nil {
		return nil, err
	}
	if !sameSQLServerDeleteCatalogAuthority(capability.authority, current) {
		return nil, errors.New("SQL Server retained strict delete catalog authority changed after admission")
	}
	if len(columns) != len(current.PrimaryKey) {
		return nil, errors.New("SQL Server strict delete key request width differs from the live primary key")
	}
	for index := range columns {
		if columns[index] != current.PrimaryKey[index].Name {
			return nil, errors.New("SQL Server strict delete key request is not in exact primary-key order")
		}
	}
	rows, err := queryer.QueryContext(
		ctx,
		"SELECT "+sqlServerQuotedColumns(columns)+" FROM "+
			sqlServerQualified(source.namespace, table.Name)+
			" ORDER BY "+sqlServerQuotedColumns(columns),
	)
	if err != nil {
		return nil, fmt.Errorf("open retained SQL Server strict delete primary keys for %s.%s: %w", table.Schema, table.Name, err)
	}
	return &sqlServerDeleteKeyRows{rows: rows, width: len(columns)}, nil
}

// newSQLServerStrictDeleteReconciliationCapabilities combines an already
// admitted same-engine target with a source authority read through the active
// strict view. It intentionally keeps the source and target construction
// independent: the only certified equality proof remains the existing
// SQL-Server integer-primary-key proof, and no cross-engine route is widened.
func newSQLServerStrictDeleteReconciliationCapabilities(
	ctx context.Context,
	stable adapterStableNetworkSource,
	admitted postgresDeleteReconciliationCapabilities,
	sourceTable schema.Table,
	targetTable schema.Table,
) (postgresDeleteReconciliationCapabilities, error) {
	if ctx == nil {
		return postgresDeleteReconciliationCapabilities{}, errors.New("SQL Server strict delete capability context is required")
	}
	if err := ctx.Err(); err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	preflightSource, ok := admitted.source.(*sqlServerDeleteSourceCapability)
	if !ok || preflightSource == nil {
		return postgresDeleteReconciliationCapabilities{}, errors.New("SQL Server strict delete route lacks its admitted SQL Server source authority")
	}
	preflightTarget, ok := admitted.target.(*sqlServerDeleteTargetCapability)
	if !ok || preflightTarget == nil || preflightTarget.adapter == nil {
		return postgresDeleteReconciliationCapabilities{}, errors.New("SQL Server strict delete route lacks its admitted SQL Server target authority")
	}
	strictSource, err := newSQLServerStrictDeleteSourceCapability(
		ctx,
		stable,
		sourceTable,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	currentTarget, err := newSQLServerDeleteTargetCapability(
		ctx,
		preflightTarget.adapter,
		targetTable,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	if err := requireDistinctSQLServerDeleteEndpoints(
		preflightSource.authority.Endpoint,
		currentTarget.authority.Endpoint,
	); err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	canonicalizer, err := newSQLServerDeleteKeyCanonicalizer(
		sourceTable,
		targetTable,
		strictSource.authority,
		currentTarget.authority,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	return postgresDeleteReconciliationCapabilities{
		source:        strictSource,
		target:        currentTarget,
		canonicalizer: canonicalizer,
	}, nil
}

// sqlServerStrictDeleteSourceBound proves the strict-specific source half is
// never accidentally replaced with an ordinary live source in a future route.
var _ deleteKeySource = (*sqlServerStrictDeleteSourceCapability)(nil)
