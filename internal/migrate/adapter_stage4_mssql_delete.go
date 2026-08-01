package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	// SQL Server accepts at most 2100 parameters in one statement. The delete
	// core additionally applies its durable batch-byte ceiling before this
	// adapter receives a page; this local ceiling prevents a malformed replay
	// from widening that authority at the driver boundary.
	sqlServerDeleteMaximumParameters = 2100
	sqlServerDeleteMaximumBatchBytes = 64 << 20
	sqlServerDeleteCleanupTimeout    = 15 * time.Second
)

// sqlServerDeleteReconciliationCapabilities is intentionally built from
// independently constructible source and target halves. A future mixed-engine
// route must provide its own canonical equality proof; it must not inherit
// same-engine admission merely because it can open both endpoints.
type sqlServerDeleteReconciliationCapabilities struct {
	source        deleteKeySource
	target        deleteKeyTarget
	canonicalizer deleteKeyCanonicalizer
}

type sqlServerDeleteSourceCapability struct {
	adapter   *relationalSourceAdapter
	authority sqlServerDeleteCatalogAuthority
}

type sqlServerDeleteTargetCapability struct {
	adapter   *sqlServerTargetAdapter
	authority sqlServerDeleteCatalogAuthority
}

// sqlServerDeleteEndpointIdentity is deliberately credential-free. It binds
// source/target relation evidence to one SQL Server database incarnation, not
// merely a configured host string or a pooled *sql.DB pointer.
type sqlServerDeleteEndpointIdentity struct {
	Server      string
	Database    string
	DatabaseID  int64
	CreatedAt   time.Time
	Collation   string
	Principal   string
	Major       int
	Version     string
	IdentityKey string
}

// sqlServerDeleteCatalogAuthority is the exact stable relation authority used
// by the complete-key scanners and the target batch writer. CatalogDigest is
// recomputed from the value itself before it is used in a canonicalizer or a
// target receipt, so a stale in-memory value cannot become authority.
type sqlServerDeleteCatalogAuthority struct {
	Endpoint      sqlServerDeleteEndpointIdentity
	Namespace     string
	TableShape    schema.Table
	PrimaryKey    []schema.Column
	SchemaID      int64
	ObjectID      int64
	CanSelect     bool
	CanDelete     bool
	CatalogDigest string
}

// newSQLServerDeleteReconciliationCapabilities admits only the same-engine
// cell currently proven here. The individual constructors stay source/target
// independent so later composition can pair a certified half with a separate
// equality proof rather than turning this function into an engine allowlist.
func newSQLServerDeleteReconciliationCapabilities(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
) (postgresDeleteReconciliationCapabilities, error) {
	if ctx == nil {
		return postgresDeleteReconciliationCapabilities{}, errors.New("SQL Server delete reconciliation context is required")
	}
	if err := ctx.Err(); err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	sourceCapability, err := newSQLServerDeleteSourceCapability(ctx, source, sourceTable)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	targetCapability, err := newSQLServerDeleteTargetCapability(ctx, target, targetTable)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	if err := requireDistinctSQLServerDeleteEndpoints(
		sourceCapability.authority.Endpoint,
		targetCapability.authority.Endpoint,
	); err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	canonicalizer, err := newSQLServerDeleteKeyCanonicalizer(
		sourceTable,
		targetTable,
		sourceCapability.authority,
		targetCapability.authority,
	)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	return postgresDeleteReconciliationCapabilities{
		source:        sourceCapability,
		target:        targetCapability,
		canonicalizer: canonicalizer,
	}, nil
}

// newSQLServerDeleteSourceCapability performs only source-side admission. It
// never opens or assumes a target, which keeps stable source scanning reusable
// by a future cross-engine route once that route can prove equality semantics.
func newSQLServerDeleteSourceCapability(
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
) (*sqlServerDeleteSourceCapability, error) {
	adapter, ok := source.(*relationalSourceAdapter)
	if !ok || adapter == nil || adapter.database == nil ||
		adapter.spec.engine != "mssql" {
		return nil, errors.New("delete reconciliation requires a verified SQL Server relational source adapter")
	}
	if table.Schema != adapter.namespace ||
		isSQLServerDeleteJournalNamespace(table.Schema) {
		return nil, fmt.Errorf("SQL Server delete source table is outside its configured namespace or is reserved private receipt state")
	}
	authority, err := inspectSQLServerDeleteCatalogAuthority(
		ctx,
		adapter.database,
		adapter.namespace,
		table,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("validate SQL Server delete source catalog: %w", err)
	}
	if !authority.CanSelect {
		return nil, errors.New("SQL Server delete source requires exact table SELECT privilege")
	}
	return &sqlServerDeleteSourceCapability{adapter: adapter, authority: authority}, nil
}

// newSQLServerDeleteTargetCapability is likewise target-only. It establishes
// complete relation and private-journal admission without assuming a source
// engine or importing a mixed-engine equality claim.
func newSQLServerDeleteTargetCapability(
	ctx context.Context,
	target targetAdapter,
	table schema.Table,
) (*sqlServerDeleteTargetCapability, error) {
	adapter, ok := target.(*sqlServerTargetAdapter)
	if !ok || adapter == nil || adapter.database == nil {
		return nil, errors.New("delete reconciliation requires a verified SQL Server target adapter")
	}
	if table.Schema != adapter.namespace ||
		isSQLServerDeleteJournalNamespace(table.Schema) {
		return nil, fmt.Errorf("SQL Server delete target table is outside its configured namespace or is reserved private receipt state")
	}
	authority, err := inspectSQLServerDeleteCatalogAuthority(
		ctx,
		adapter.database,
		adapter.namespace,
		table,
		true,
	)
	if err != nil {
		return nil, fmt.Errorf("validate SQL Server delete target catalog: %w", err)
	}
	if !authority.CanSelect || !authority.CanDelete {
		return nil, errors.New("SQL Server delete target requires exact table SELECT and DELETE privileges")
	}
	if err := preflightSQLServerDeleteReceiptJournal(ctx, adapter); err != nil {
		return nil, fmt.Errorf("preflight SQL Server delete receipt journal: %w", err)
	}
	return &sqlServerDeleteTargetCapability{adapter: adapter, authority: authority}, nil
}

func isSQLServerDeleteJournalNamespace(namespace string) bool {
	return strings.EqualFold(strings.TrimSpace(namespace), sqlServerDeleteJournalSchema)
}

func readSQLServerDeleteEndpointIdentity(
	ctx context.Context,
	queryer engine.SQLServerCatalogQueryer,
	namespace string,
) (sqlServerDeleteEndpointIdentity, error) {
	if queryer == nil || strings.TrimSpace(namespace) == "" {
		return sqlServerDeleteEndpointIdentity{}, errors.New("SQL Server delete endpoint identity is unavailable")
	}
	var identity sqlServerDeleteEndpointIdentity
	var createdAt time.Time
	if err := queryer.QueryRowContext(ctx, `
		SELECT
			CONVERT(varchar(256), SERVERPROPERTY('ServerName')),
			target_database.name,
			CONVERT(bigint, target_database.database_id),
			target_database.create_date,
			target_database.collation_name,
			USER_NAME(),
			CONVERT(int, SERVERPROPERTY('ProductMajorVersion')),
			CONVERT(varchar(128), SERVERPROPERTY('ProductVersion'))
		  FROM sys.databases AS target_database
		 WHERE target_database.database_id = DB_ID()
	`).Scan(
		&identity.Server,
		&identity.Database,
		&identity.DatabaseID,
		&createdAt,
		&identity.Collation,
		&identity.Principal,
		&identity.Major,
		&identity.Version,
	); err != nil {
		return sqlServerDeleteEndpointIdentity{}, fmt.Errorf("read SQL Server delete endpoint identity: %w", err)
	}
	identity.Server = strings.TrimSpace(identity.Server)
	identity.Database = strings.TrimSpace(identity.Database)
	identity.Collation = strings.TrimSpace(identity.Collation)
	identity.Principal = strings.TrimSpace(identity.Principal)
	identity.Version = strings.TrimSpace(identity.Version)
	identity.CreatedAt = createdAt.UTC()
	if identity.Server == "" || identity.Database == "" || identity.DatabaseID <= 0 ||
		identity.CreatedAt.IsZero() || identity.Collation == "" || identity.Principal == "" || identity.Major != 16 ||
		identity.Version == "" {
		return sqlServerDeleteEndpointIdentity{}, errors.New("SQL Server delete endpoint identity is incomplete or not SQL Server 2022")
	}
	identity.IdentityKey = fmt.Sprintf(
		"mssql-delete-v1 server=%s database=%s database_id=%d created_at=%s collation=%s namespace=%s",
		identity.Server,
		identity.Database,
		identity.DatabaseID,
		identity.CreatedAt.Format(time.RFC3339Nano),
		identity.Collation,
		namespace,
	)
	return identity, nil
}

func sameSQLServerDeleteEndpointIdentity(
	left sqlServerDeleteEndpointIdentity,
	right sqlServerDeleteEndpointIdentity,
) bool {
	return strings.EqualFold(left.Server, right.Server) &&
		strings.EqualFold(left.Database, right.Database) &&
		left.DatabaseID == right.DatabaseID &&
		left.CreatedAt.Equal(right.CreatedAt)
}

func requireDistinctSQLServerDeleteEndpoints(
	source sqlServerDeleteEndpointIdentity,
	target sqlServerDeleteEndpointIdentity,
) error {
	if sameSQLServerDeleteEndpointIdentity(source, target) {
		return errors.New(
			"SQL Server delete reconciliation rejects a source and target in the same database endpoint",
		)
	}
	return nil
}

func inspectSQLServerDeleteCatalogAuthority(
	ctx context.Context,
	queryer engine.SQLServerCatalogQueryer,
	namespace string,
	expected schema.Table,
	target bool,
) (sqlServerDeleteCatalogAuthority, error) {
	if queryer == nil || strings.TrimSpace(namespace) == "" ||
		expected.Schema != namespace || strings.TrimSpace(expected.Name) == "" ||
		isSQLServerDeleteJournalNamespace(namespace) {
		return sqlServerDeleteCatalogAuthority{}, errors.New("SQL Server delete catalog identity is incomplete or reserved")
	}
	var live schema.Table
	var err error
	if target {
		live, err = engine.InspectSQLServerTargetTableWithQueryer(
			ctx, queryer, namespace, expected.Name,
		)
	} else {
		live, err = engine.InspectSQLServerTableWithQueryer(
			ctx, queryer, namespace, expected.Name,
		)
	}
	if err != nil {
		return sqlServerDeleteCatalogAuthority{}, err
	}
	return inspectSQLServerDeleteCatalogAuthorityFromLiveTable(
		ctx,
		queryer,
		namespace,
		expected,
		live,
	)
}

// inspectSQLServerDeleteCatalogAuthorityFromLiveTable binds native relation
// metadata to a catalog shape already read through the caller's verified
// view. The strict SQL Server delete path uses it with a retained table lock
// or database snapshot; the ordinary source/target path above supplies the
// corresponding pool-backed inspection result.
func inspectSQLServerDeleteCatalogAuthorityFromLiveTable(
	ctx context.Context,
	queryer engine.SQLServerCatalogQueryer,
	namespace string,
	expected schema.Table,
	live schema.Table,
) (sqlServerDeleteCatalogAuthority, error) {
	if queryer == nil || strings.TrimSpace(namespace) == "" ||
		expected.Schema != namespace || strings.TrimSpace(expected.Name) == "" ||
		isSQLServerDeleteJournalNamespace(namespace) {
		return sqlServerDeleteCatalogAuthority{}, errors.New("SQL Server delete catalog identity is incomplete or reserved")
	}
	// The native SQL Server catalog reader represents an absent object list as
	// an allocated empty slice, while planned schemas may retain nil. Those
	// encodings carry the same exact catalog fact; normalize only that transport
	// difference before binding the immutable authority. Nonempty objects remain
	// byte-for-byte structural evidence and still fail closed on any drift.
	expected = normalizeSQLServerDeleteTableShape(expected)
	live = normalizeSQLServerDeleteTableShape(live)
	if !reflect.DeepEqual(live, expected) {
		return sqlServerDeleteCatalogAuthority{}, errors.New("SQL Server delete catalog shape changed after discovery")
	}
	identity, err := readSQLServerDeleteEndpointIdentity(ctx, queryer, namespace)
	if err != nil {
		return sqlServerDeleteCatalogAuthority{}, err
	}
	primaryKey, err := deletePrimaryKeyColumns(live)
	if err != nil {
		return sqlServerDeleteCatalogAuthority{}, err
	}
	var (
		authority sqlServerDeleteCatalogAuthority
		viewDef   bool
	)
	if err := queryer.QueryRowContext(ctx, `
		SELECT
			target_schema.schema_id,
			target_object.object_id,
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				QUOTENAME(target_schema.name) + N'.' + QUOTENAME(target_object.name),
				'OBJECT', 'SELECT'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				QUOTENAME(target_schema.name) + N'.' + QUOTENAME(target_object.name),
				'OBJECT', 'DELETE'
			), 0)),
			CONVERT(bit, COALESCE(HAS_PERMS_BY_NAME(
				QUOTENAME(target_schema.name) + N'.' + QUOTENAME(target_object.name),
				'OBJECT', 'VIEW DEFINITION'
			), 0))
		  FROM sys.schemas AS target_schema
		  JOIN sys.objects AS target_object
		    ON target_object.schema_id = target_schema.schema_id
		 WHERE target_schema.name = @p1
		   AND target_object.name = @p2
		   AND target_object.type = 'U'
	`, namespace, expected.Name).Scan(
		&authority.SchemaID,
		&authority.ObjectID,
		&authority.CanSelect,
		&authority.CanDelete,
		&viewDef,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return sqlServerDeleteCatalogAuthority{}, errors.New("SQL Server delete relation is absent or not a user table")
		}
		return sqlServerDeleteCatalogAuthority{}, fmt.Errorf("read SQL Server delete table identity and privileges: %w", err)
	}
	if authority.SchemaID <= 0 || authority.ObjectID <= 0 || !viewDef || !authority.CanSelect {
		return sqlServerDeleteCatalogAuthority{}, errors.New("SQL Server delete relation lacks exact catalog or SELECT authority")
	}
	authority.Endpoint = identity
	authority.Namespace = namespace
	authority.TableShape = cloneStage4RichTable(live)
	authority.PrimaryKey = append([]schema.Column(nil), primaryKey...)
	authority.CatalogDigest, err = sqlServerDeleteAuthorityDigestValue(authority)
	if err != nil {
		return sqlServerDeleteCatalogAuthority{}, err
	}
	return authority, nil
}

// normalizeSQLServerDeleteTableShape canonicalizes the sole known transport
// distinction between a planned rich table and SQL Server catalog discovery:
// nil and allocated-zero object lists. It intentionally does not drop,
// reorder, or rewrite nonempty schema objects.
func normalizeSQLServerDeleteTableShape(table schema.Table) schema.Table {
	table = cloneStage4RichTable(table)
	if len(table.ClickHouseOrderBy) == 0 {
		table.ClickHouseOrderBy = nil
	}
	if len(table.Indexes) == 0 {
		table.Indexes = nil
	}
	if len(table.ForeignKeys) == 0 {
		table.ForeignKeys = nil
	}
	if len(table.Checks) == 0 {
		table.Checks = nil
	}
	return table
}

func sqlServerDeleteAuthorityDigestValue(
	authority sqlServerDeleteCatalogAuthority,
) (string, error) {
	if authority.SchemaID <= 0 || authority.ObjectID <= 0 ||
		strings.TrimSpace(authority.Namespace) == "" ||
		strings.TrimSpace(authority.Endpoint.IdentityKey) == "" ||
		len(authority.PrimaryKey) == 0 {
		return "", errors.New("SQL Server delete catalog authority is incomplete")
	}
	payload, err := json.Marshal(struct {
		Endpoint   string          `json:"endpoint"`
		Namespace  string          `json:"namespace"`
		SchemaID   int64           `json:"schema_id"`
		ObjectID   int64           `json:"object_id"`
		Table      schema.Table    `json:"table"`
		PrimaryKey []schema.Column `json:"primary_key"`
		CanSelect  bool            `json:"can_select"`
		CanDelete  bool            `json:"can_delete"`
	}{
		Endpoint:   authority.Endpoint.IdentityKey,
		Namespace:  authority.Namespace,
		SchemaID:   authority.SchemaID,
		ObjectID:   authority.ObjectID,
		Table:      authority.TableShape,
		PrimaryKey: authority.PrimaryKey,
		CanSelect:  authority.CanSelect,
		CanDelete:  authority.CanDelete,
	})
	if err != nil {
		return "", fmt.Errorf("encode SQL Server delete catalog authority: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func sameSQLServerDeleteCatalogAuthority(
	left sqlServerDeleteCatalogAuthority,
	right sqlServerDeleteCatalogAuthority,
) bool {
	leftDigest, leftErr := sqlServerDeleteAuthorityDigestValue(left)
	rightDigest, rightErr := sqlServerDeleteAuthorityDigestValue(right)
	return leftErr == nil && rightErr == nil &&
		left.CatalogDigest == leftDigest && right.CatalogDigest == rightDigest &&
		leftDigest == rightDigest
}

type sqlServerDeleteKeyRows struct {
	rows              *sql.Rows
	connection        *sql.Conn
	width             int
	active            bool
	sessionConfigured bool
}

func (rows *sqlServerDeleteKeyRows) Next() bool {
	return rows != nil && rows.rows != nil && rows.rows.Next()
}

func (rows *sqlServerDeleteKeyRows) Values() ([]any, error) {
	if rows == nil || rows.rows == nil || rows.width <= 0 {
		return nil, errors.New("SQL Server delete key reader is closed")
	}
	values := make([]any, rows.width)
	destinations := make([]any, rows.width)
	for index := range values {
		destinations[index] = &values[index]
	}
	if err := rows.rows.Scan(destinations...); err != nil {
		return nil, err
	}
	return values, nil
}

func (rows *sqlServerDeleteKeyRows) Err() error {
	if rows == nil || rows.rows == nil {
		return nil
	}
	return rows.rows.Err()
}

func sqlServerDeleteDetachedContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), sqlServerDeleteCleanupTimeout)
}

func discardSQLServerDeleteConnection(connection *sql.Conn) {
	if connection == nil {
		return
	}
	_ = connection.Raw(func(any) error { return driver.ErrBadConn })
}

func (rows *sqlServerDeleteKeyRows) Close() error {
	if rows == nil {
		return nil
	}
	var result error
	if rows.rows != nil {
		result = rows.rows.Close()
		rows.rows = nil
	}
	discarded := false
	if rows.active && rows.connection != nil {
		rollbackErr := rollbackSQLServerDeleteTransaction(context.Background(), rows.connection)
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			discardSQLServerDeleteConnection(rows.connection)
			discarded = true
			result = errors.Join(result, fmt.Errorf("roll back SQL Server delete key snapshot: %w", rollbackErr))
		}
		rows.active = false
	}
	if rows.sessionConfigured && rows.connection != nil && !discarded {
		if resetErr := resetSQLServerDeleteSession(context.Background(), rows.connection); resetErr != nil {
			discardSQLServerDeleteConnection(rows.connection)
			discarded = true
			result = errors.Join(result, fmt.Errorf("reset SQL Server delete key snapshot session: %w", resetErr))
		}
		rows.sessionConfigured = false
	}
	if rows.connection != nil {
		if closeErr := rows.connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			result = errors.Join(result, fmt.Errorf("close SQL Server delete key snapshot connection: %w", closeErr))
		}
		rows.connection = nil
	}
	return result
}

func openSQLServerDeletePrimaryKeys(
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table schema.Table,
	columns []string,
	expectedAuthority sqlServerDeleteCatalogAuthority,
	target bool,
) (_ deleteKeyRows, resultErr error) {
	if database == nil {
		return nil, errors.New("SQL Server delete key database is unavailable")
	}
	connection, err := database.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire pinned SQL Server delete key connection: %w", err)
	}
	active := false
	sessionConfigured := false
	defer func() {
		if resultErr == nil {
			return
		}
		discarded := false
		if active {
			rollbackErr := rollbackSQLServerDeleteTransaction(ctx, connection)
			if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				discardSQLServerDeleteConnection(connection)
				discarded = true
				resultErr = errors.Join(resultErr, fmt.Errorf("roll back SQL Server delete key snapshot: %w", rollbackErr))
			}
		}
		if sessionConfigured && !discarded {
			if resetErr := resetSQLServerDeleteSession(ctx, connection); resetErr != nil {
				discardSQLServerDeleteConnection(connection)
				resultErr = errors.Join(resultErr, fmt.Errorf("reset SQL Server delete key snapshot session: %w", resetErr))
			}
		}
		if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
			resultErr = errors.Join(resultErr, fmt.Errorf("close SQL Server delete key connection: %w", closeErr))
		}
	}()
	// Setup deliberately retains the caller context. A detached context is safe
	// only once we are cleaning up an indeterminate transaction; using one here
	// could let a cancelled run wedge behind a lock indefinitely.
	sessionConfigured = true
	if err := configureSQLServerDeleteTransaction(ctx, connection); err != nil {
		return nil, fmt.Errorf("configure SQL Server delete key transaction: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		// A failed BEGIN is setup failure, not an established transaction. The
		// deferred path still resets/discards/closes the session, but does not
		// turn this primary failure into a detached rollback classification.
		return nil, fmt.Errorf("begin SQL Server delete key snapshot: %w", err)
	}
	active = true
	qualified := sqlServerQualified(namespace, table.Name)
	if err := acquireSQLServerDeleteTableLock(ctx, connection, qualified, false); err != nil {
		return nil, fmt.Errorf("hold SQL Server delete key table lock: %w", err)
	}
	liveAuthority, err := inspectSQLServerDeleteCatalogAuthority(
		ctx, connection, namespace, table, target,
	)
	if err != nil {
		return nil, err
	}
	if !sameSQLServerDeleteCatalogAuthority(expectedAuthority, liveAuthority) {
		return nil, errors.New("SQL Server delete key relation/catalog authority changed after plan admission")
	}
	if len(columns) != len(liveAuthority.PrimaryKey) {
		return nil, errors.New("SQL Server delete key request width differs from the live primary key")
	}
	for index := range columns {
		if columns[index] != liveAuthority.PrimaryKey[index].Name {
			return nil, errors.New("SQL Server delete key request is not in exact primary-key order")
		}
	}
	sqlRows, err := connection.QueryContext(
		ctx,
		"SELECT "+sqlServerQuotedColumns(columns)+" FROM "+qualified+
			" WITH (TABLOCK, HOLDLOCK) ORDER BY "+sqlServerQuotedColumns(columns),
	)
	if err != nil {
		return nil, fmt.Errorf("open SQL Server delete primary keys for %s.%s: %w", namespace, table.Name, err)
	}
	return &sqlServerDeleteKeyRows{
		rows:              sqlRows,
		connection:        connection,
		width:             len(columns),
		active:            true,
		sessionConfigured: true,
	}, nil
}

func (capability *sqlServerDeleteSourceCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil ||
		capability.adapter.database == nil || capability.adapter.spec.engine != "mssql" {
		return nil, errors.New("SQL Server delete source is unavailable")
	}
	return openSQLServerDeletePrimaryKeys(
		ctx,
		capability.adapter.database,
		capability.adapter.namespace,
		table,
		columns,
		capability.authority,
		false,
	)
}

func (capability *sqlServerDeleteTargetCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil {
		return nil, errors.New("SQL Server delete target is unavailable")
	}
	return openSQLServerDeletePrimaryKeys(
		ctx,
		capability.adapter.database,
		capability.adapter.namespace,
		table,
		columns,
		capability.authority,
		true,
	)
}

func (*sqlServerDeleteTargetCapability) MaxDeleteParameters() int {
	return sqlServerDeleteMaximumParameters
}

type sqlServerDeleteKeyCanonicalizer struct {
	sourceTable schema.Table
	targetTable schema.Table
	proof       deleteKeyEqualityProof
}

func newSQLServerDeleteKeyCanonicalizer(
	sourceTable schema.Table,
	targetTable schema.Table,
	sourceAuthority sqlServerDeleteCatalogAuthority,
	targetAuthority sqlServerDeleteCatalogAuthority,
) (*sqlServerDeleteKeyCanonicalizer, error) {
	sourceTable = normalizeSQLServerDeleteTableShape(sourceTable)
	targetTable = normalizeSQLServerDeleteTableShape(targetTable)
	if err := validateSQLServerDeleteKeyPair(sourceAuthority.PrimaryKey, targetAuthority.PrimaryKey); err != nil {
		return nil, err
	}
	for side, authority := range map[string]sqlServerDeleteCatalogAuthority{
		"source": sourceAuthority,
		"target": targetAuthority,
	} {
		expected, err := sqlServerDeleteAuthorityDigestValue(authority)
		if err != nil || authority.CatalogDigest != expected ||
			validateLowerSHA256("SQL Server delete "+side+" catalog authority digest", authority.CatalogDigest) != nil {
			return nil, fmt.Errorf("SQL Server delete %s catalog authority digest differs from exact catalog evidence", side)
		}
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(sourceTable, sourceAuthority.PrimaryKey)
	if err != nil {
		return nil, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(targetTable, targetAuthority.PrimaryKey)
	if err != nil {
		return nil, err
	}
	routeAuthority := sha256.Sum256([]byte(
		sourceAuthority.CatalogDigest + "\x00" + targetAuthority.CatalogDigest,
	))
	proof := deleteKeyEqualityProof{
		CanonicalizerID:   "mssql-exact-integer-primary-key-v1:" + hex.EncodeToString(routeAuthority[:]),
		SourceFingerprint: sourceFingerprint,
		TargetFingerprint: targetFingerprint,
		Columns:           make([]deleteKeyColumnProof, len(sourceAuthority.PrimaryKey)),
	}
	for index, column := range sourceAuthority.PrimaryKey {
		if _, err := sqlServerDeleteProofSemantics(column); err != nil {
			return nil, err
		}
		proof.Columns[index] = deleteKeyColumnProof{Semantics: "integer"}
	}
	if _, err := validateDeleteKeyEqualityProof(
		proof, sourceTable, targetTable, sourceAuthority.PrimaryKey, targetAuthority.PrimaryKey,
	); err != nil {
		return nil, err
	}
	return &sqlServerDeleteKeyCanonicalizer{
		sourceTable: cloneStage4RichTable(sourceTable),
		targetTable: cloneStage4RichTable(targetTable),
		proof:       proof,
	}, nil
}

func validateSQLServerDeleteKeyPair(sourceKey, targetKey []schema.Column) error {
	if len(sourceKey) == 0 || len(sourceKey) != len(targetKey) {
		return errors.New("SQL Server delete source and target primary-key widths differ")
	}
	for index := range sourceKey {
		source := sourceKey[index]
		target := targetKey[index]
		if source.Name != target.Name || source.PrimaryKeyPosition != index+1 ||
			target.PrimaryKeyPosition != index+1 || source.Nullable || target.Nullable ||
			!reflect.DeepEqual(source, target) {
			return fmt.Errorf("SQL Server delete primary-key column %d is not preserved exactly", index+1)
		}
		if _, err := sqlServerDeleteProofSemantics(source); err != nil {
			return fmt.Errorf("SQL Server delete primary-key column %s: %w", source.Name, err)
		}
	}
	return nil
}

// The first SQL Server delete cell is deliberately limited to integer primary
// keys. SQL Server collations, decimal scale/driver bind behavior, temporal
// precision, and binary keys need their own per-domain proof; accepting them
// on same-engine labels alone would violate the reconciliation equality rule.
func sqlServerDeleteProofSemantics(column schema.Column) (string, error) {
	kind, err := validationKindForColumn(column)
	if err != nil {
		return "", err
	}
	if kind != validationInteger {
		return "", fmt.Errorf("SQL Server delete primary-key type %q is not yet certified; only integer keys are admitted", column.Type)
	}
	return "integer", nil
}

func (canonicalizer *sqlServerDeleteKeyCanonicalizer) ProveDeleteKeyEquality(
	sourceTable schema.Table,
	targetTable schema.Table,
	sourcePrimaryKey []schema.Column,
	targetPrimaryKey []schema.Column,
) (deleteKeyEqualityProof, error) {
	sourceTable = normalizeSQLServerDeleteTableShape(sourceTable)
	targetTable = normalizeSQLServerDeleteTableShape(targetTable)
	if canonicalizer == nil || !reflect.DeepEqual(sourceTable, canonicalizer.sourceTable) ||
		!reflect.DeepEqual(targetTable, canonicalizer.targetTable) {
		return deleteKeyEqualityProof{}, errors.New("SQL Server delete key proof was requested for different tables")
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(sourceTable, sourcePrimaryKey)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(targetTable, targetPrimaryKey)
	if err != nil {
		return deleteKeyEqualityProof{}, err
	}
	if sourceFingerprint != canonicalizer.proof.SourceFingerprint ||
		targetFingerprint != canonicalizer.proof.TargetFingerprint {
		return deleteKeyEqualityProof{}, errors.New("SQL Server delete primary-key metadata changed after capability admission")
	}
	proof := canonicalizer.proof
	proof.Columns = append([]deleteKeyColumnProof(nil), canonicalizer.proof.Columns...)
	return proof, nil
}

func (canonicalizer *sqlServerDeleteKeyCanonicalizer) CanonicalizeDeleteKeyValue(
	side deleteKeySide,
	proof deleteKeyEqualityProof,
	index int,
	value any,
) (deleteCanonicalValue, error) {
	if canonicalizer == nil || !reflect.DeepEqual(proof, canonicalizer.proof) ||
		index < 0 || index >= len(proof.Columns) || proof.Columns[index].Semantics != "integer" {
		return deleteCanonicalValue{}, errors.New("SQL Server delete key canonicalization proof differs")
	}
	canonical, err := canonicalValidationInteger(value)
	if err != nil {
		return deleteCanonicalValue{}, err
	}
	result := deleteCanonicalValue{Canonical: append([]byte(nil), canonical...)}
	switch side {
	case deleteKeySourceSide:
		return result, nil
	case deleteKeyTargetSide:
		parameter, err := driver.DefaultParameterConverter.ConvertValue(value)
		if err != nil {
			return deleteCanonicalValue{}, fmt.Errorf("convert SQL Server delete parameter: %w", err)
		}
		result.Parameter, err = stableDeleteParameter(parameter)
		if err != nil {
			return deleteCanonicalValue{}, err
		}
		return result, nil
	default:
		return deleteCanonicalValue{}, fmt.Errorf("unknown SQL Server delete key side %q", side)
	}
}

func validateSQLServerDeleteBatch(
	namespace string,
	batch deleteTargetBatch,
) ([][]driver.Value, error) {
	return validateSQLServerDeleteBatchWithLimits(
		namespace,
		batch,
		sqlServerDeleteMaximumParameters,
		sqlServerDeleteMaximumBatchBytes,
	)
}

func validateSQLServerDeleteBatchWithLimits(
	namespace string,
	batch deleteTargetBatch,
	maximumParameters int,
	maximumBytes int64,
) ([][]driver.Value, error) {
	if strings.TrimSpace(namespace) == "" || batch.Table.Schema != namespace ||
		strings.TrimSpace(batch.Table.Name) == "" || len(batch.Columns) == 0 ||
		len(batch.Keys) == 0 || strings.TrimSpace(batch.PlanID) == "" || batch.Sequence < 0 {
		return nil, errors.New("SQL Server delete batch identity is incomplete")
	}
	if maximumParameters <= 0 || maximumBytes <= 0 {
		return nil, errors.New("SQL Server delete batch limits are invalid")
	}
	if _, err := hex.DecodeString(batch.PlanID); err != nil || len(batch.PlanID) != 32 || batch.PlanID != strings.ToLower(batch.PlanID) {
		return nil, errors.New("SQL Server delete batch plan ID must be 32 lowercase hexadecimal characters")
	}
	if err := validateLowerSHA256("SQL Server delete batch token", batch.Token); err != nil {
		return nil, err
	}
	if err := validateLowerSHA256("SQL Server delete batch digest", batch.BatchDigest); err != nil {
		return nil, err
	}
	if len(batch.Keys) > maximumParameters/len(batch.Columns) {
		return nil, fmt.Errorf("SQL Server delete batch exceeds the %d-parameter limit", maximumParameters)
	}
	arguments := make([][]driver.Value, len(batch.Keys))
	var encodedBytes int64
	for rowIndex, key := range batch.Keys {
		if len(key) != len(batch.Columns) {
			return nil, fmt.Errorf("SQL Server delete key %d width differs", rowIndex)
		}
		arguments[rowIndex] = make([]driver.Value, len(key))
		for columnIndex, value := range key {
			stable, err := stableDeleteParameter(value)
			if err != nil || stable == nil {
				return nil, fmt.Errorf("SQL Server delete key %d column %d is not parameter-safe", rowIndex, columnIndex)
			}
			valueBytes := int64(16)
			switch typed := stable.(type) {
			case string:
				valueBytes = int64(len(typed))
			case []byte:
				valueBytes = int64(len(typed))
			}
			if valueBytes > maximumBytes-encodedBytes {
				return nil, fmt.Errorf("SQL Server delete batch exceeds the %d-byte adapter ceiling", maximumBytes)
			}
			encodedBytes += valueBytes
			arguments[rowIndex][columnIndex] = stable
		}
	}
	return arguments, nil
}

func validateSQLServerDeleteBatchAuthority(
	batch deleteTargetBatch,
	authority sqlServerDeleteCatalogAuthority,
) error {
	expected, err := sqlServerDeleteAuthorityDigestValue(authority)
	if err != nil || authority.CatalogDigest != expected {
		return errors.New("SQL Server delete target authority digest is invalid")
	}
	if !authority.CanSelect || !authority.CanDelete ||
		!reflect.DeepEqual(normalizeSQLServerDeleteTableShape(batch.Table), authority.TableShape) ||
		len(batch.Columns) != len(authority.PrimaryKey) {
		return errors.New("SQL Server delete batch differs from admitted target authority")
	}
	for index := range batch.Columns {
		if batch.Columns[index] != authority.PrimaryKey[index].Name {
			return errors.New("SQL Server delete batch columns are not in exact admitted primary-key order")
		}
	}
	return nil
}

func sqlServerDeleteBatchStatement(
	table schema.Table,
	columns []string,
	rowCount int,
) (string, error) {
	if strings.TrimSpace(table.Schema) == "" || strings.TrimSpace(table.Name) == "" ||
		len(columns) == 0 || rowCount <= 0 {
		return "", errors.New("SQL Server delete statement shape is incomplete")
	}
	if rowCount > sqlServerDeleteMaximumParameters/len(columns) {
		return "", errors.New("SQL Server delete statement exceeds the parameter limit")
	}
	clauses := make([]string, rowCount)
	parameter := 1
	for row := range clauses {
		terms := make([]string, len(columns))
		for column := range columns {
			terms[column] = sqlServerIdentifier(columns[column]) + " = @p" + fmt.Sprint(parameter)
			parameter++
		}
		clauses[row] = "(" + strings.Join(terms, " AND ") + ")"
	}
	return "DELETE FROM " + sqlServerQualified(table.Schema, table.Name) +
		" WHERE " + strings.Join(clauses, " OR "), nil
}

func flattenSQLServerDeleteArguments(keys [][]driver.Value) []any {
	arguments := make([]any, 0)
	for _, key := range keys {
		for _, value := range key {
			arguments = append(arguments, value)
		}
	}
	return arguments
}

func sqlServerDeleteBatchLockResource(token string) (string, error) {
	if err := validateLowerSHA256("SQL Server delete batch token", token); err != nil {
		return "", err
	}
	return "dmtx.stage4.delete.v1." + token, nil
}

func acquireSQLServerDeleteBatchLock(
	ctx context.Context,
	connection *sql.Conn,
	resource string,
) error {
	if connection == nil || strings.TrimSpace(resource) == "" {
		return errors.New("SQL Server delete batch lock connection is unavailable")
	}
	var result int
	if err := connection.QueryRowContext(ctx, `
		DECLARE @result int;
		EXEC @result = sys.sp_getapplock
			@Resource = @p1,
			@LockMode = 'Exclusive',
			@LockOwner = 'Transaction',
			@LockTimeout = @p2,
			@DbPrincipal = 'public';
		SELECT @result;
	`, resource, int(sqlServerDeleteCleanupTimeout/time.Millisecond)).Scan(&result); err != nil {
		return err
	}
	if result < 0 {
		return fmt.Errorf("sp_getapplock returned %d", result)
	}
	return nil
}

func (adapter *sqlServerTargetAdapter) commitSQLServerDeleteReceipt(
	ctx context.Context,
	connection *sql.Conn,
) (sql.Result, error) {
	if adapter != nil && adapter.deleteCommit != nil {
		return adapter.deleteCommit(ctx, connection)
	}
	return connection.ExecContext(ctx, "COMMIT TRANSACTION")
}

// configureSQLServerDeleteTransaction establishes the auditable stable-view
// contract used by both complete-key scans and delete application: every
// statement is in an explicit SERIALIZABLE transaction and table locks are
// acquired before catalog authority is reread. Callers retain their context
// and deadline here; cleanup is the only detached operation.
func configureSQLServerDeleteTransaction(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return errors.New("SQL Server delete transaction connection is unavailable")
	}
	if _, err := connection.ExecContext(ctx, "SET XACT_ABORT ON"); err != nil {
		return fmt.Errorf("set XACT_ABORT: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"); err != nil {
		return fmt.Errorf("set SERIALIZABLE isolation: %w", err)
	}
	return nil
}

// resetSQLServerDeleteSession returns the pool connection to its conservative
// defaults. It is intentionally bounded and detached: a caller may already
// be cancelled while a transaction must still be made safe for reuse.
func resetSQLServerDeleteSession(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	cleanupCtx, cancel := sqlServerDeleteDetachedContext(ctx)
	defer cancel()
	_, xactErr := connection.ExecContext(cleanupCtx, "SET XACT_ABORT OFF")
	_, isolationErr := connection.ExecContext(cleanupCtx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
	return errors.Join(xactErr, isolationErr)
}

// acquireSQLServerDeleteTableLock performs a real table access under a table
// lock before catalog revalidation. TOP (0) is deliberately not used: SQL
// Server may optimize it away. A TOP (1) probe establishes the normal path;
// an empty relation executes COUNT_BIG under the same hint so empty and
// nonempty tables have the same lock-before-catalog authority.
func acquireSQLServerDeleteTableLock(
	ctx context.Context,
	connection *sql.Conn,
	qualified string,
	exclusive bool,
) error {
	if connection == nil || strings.TrimSpace(qualified) == "" {
		return errors.New("SQL Server delete table-lock connection is unavailable")
	}
	probe, emptyProbe, err := sqlServerDeleteTableLockQueries(qualified, exclusive)
	if err != nil {
		return err
	}
	var marker int
	err = connection.QueryRowContext(ctx, probe).Scan(&marker)
	if err == nil {
		if marker != 1 {
			return fmt.Errorf("unexpected SQL Server delete table-lock probe value %d", marker)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var count int64
	if err := connection.QueryRowContext(
		ctx,
		emptyProbe,
	).Scan(&count); err != nil {
		return fmt.Errorf("verify empty SQL Server delete table lock: %w", err)
	}
	if count != 0 {
		return fmt.Errorf("SQL Server delete table changed during empty-table lock probe: count=%d", count)
	}
	return nil
}

func sqlServerDeleteTableLockQueries(qualified string, exclusive bool) (string, string, error) {
	if strings.TrimSpace(qualified) == "" {
		return "", "", errors.New("SQL Server delete table-lock relation is unavailable")
	}
	hint := "TABLOCK, HOLDLOCK"
	if exclusive {
		hint = "TABLOCKX, HOLDLOCK"
	}
	return "SELECT TOP (1) 1 FROM " + qualified + " WITH (" + hint + ")",
		"SELECT COUNT_BIG(*) FROM " + qualified + " WITH (" + hint + ")", nil
}

func rollbackSQLServerDeleteTransaction(ctx context.Context, connection *sql.Conn) error {
	if connection == nil {
		return nil
	}
	cleanupCtx, cancel := sqlServerDeleteDetachedContext(ctx)
	defer cancel()
	_, err := connection.ExecContext(cleanupCtx, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION")
	return err
}

func (capability *sqlServerDeleteTargetCapability) classifySQLServerDeleteCommitAmbiguity(
	ctx context.Context,
	connection *sql.Conn,
	closed *bool,
	batch deleteTargetBatch,
	commitErr error,
) (deleteTargetBatchReceipt, error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil ||
		connection == nil || closed == nil {
		return deleteTargetBatchReceipt{}, errors.Join(commitErr, errors.New("SQL Server delete commit ambiguity authority is unavailable"))
	}
	discardSQLServerDeleteConnection(connection)
	closeErr := connection.Close()
	*closed = true
	if errors.Is(closeErr, sql.ErrConnDone) {
		closeErr = nil
	}
	verificationCtx, cancel := sqlServerDeleteDetachedContext(ctx)
	defer cancel()
	current, verifyErr := inspectSQLServerDeleteCatalogAuthority(
		verificationCtx,
		capability.adapter.database,
		capability.adapter.namespace,
		capability.authority.TableShape,
		true,
	)
	if verifyErr == nil && !sameSQLServerDeleteCatalogAuthority(capability.authority, current) {
		verifyErr = errors.New("SQL Server delete target catalog changed after commit acknowledgement failure")
	}
	var journal sqlServerDeleteJournalCatalog
	if verifyErr == nil {
		journal, verifyErr = inspectSQLServerDeleteReceiptJournal(
			verificationCtx, capability.adapter.database,
		)
		if verifyErr == nil && !journal.Exists {
			verifyErr = errors.New("SQL Server delete receipt journal is absent after commit acknowledgement failure")
		}
	}
	var stored deleteTargetBatchReceipt
	var found bool
	if verifyErr == nil {
		stored, found, verifyErr = loadSQLServerDeleteReceipt(
			verificationCtx, capability.adapter.database, batch.Token, capability.authority, journal,
		)
		if verifyErr == nil && !found {
			verifyErr = errors.New("SQL Server delete receipt is absent after commit acknowledgement failure")
		}
	}
	if verifyErr == nil {
		verifyErr = validateSQLServerDeleteReceipt(batch, stored, capability.authority, journal)
	}
	if verifyErr == nil && closeErr == nil {
		return stored, nil
	}
	return deleteTargetBatchReceipt{}, errors.Join(
		fmt.Errorf("SQL Server delete commit outcome is unknown; resume the existing run with the same pending batch token: %w", commitErr),
		closeErr,
		verifyErr,
	)
}

func (capability *sqlServerDeleteTargetCapability) ApplyDeleteBatch(
	ctx context.Context,
	batch deleteTargetBatch,
) (result deleteTargetBatchReceipt, resultErr error) {
	if capability == nil || capability.adapter == nil || capability.adapter.database == nil {
		return result, errors.New("SQL Server delete target is unavailable")
	}
	if err := validateSQLServerDeleteBatchAuthority(batch, capability.authority); err != nil {
		return result, err
	}
	keys, err := validateSQLServerDeleteBatch(capability.adapter.namespace, batch)
	if err != nil {
		return result, err
	}
	statement, err := sqlServerDeleteBatchStatement(batch.Table, batch.Columns, len(keys))
	if err != nil {
		return result, err
	}
	lockResource, err := sqlServerDeleteBatchLockResource(batch.Token)
	if err != nil {
		return result, err
	}
	connection, err := capability.adapter.database.Conn(ctx)
	if err != nil {
		return result, fmt.Errorf("acquire pinned SQL Server delete receipt connection: %w", err)
	}
	active := false
	closed := false
	sessionConfigured := false
	defer func() {
		discarded := false
		if active {
			if rollbackErr := rollbackSQLServerDeleteTransaction(ctx, connection); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
				discardSQLServerDeleteConnection(connection)
				discarded = true
				result = deleteTargetBatchReceipt{}
				resultErr = errors.Join(resultErr, fmt.Errorf("roll back SQL Server delete receipt transaction: %w", rollbackErr))
			}
		}
		if sessionConfigured && !closed && !discarded {
			if resetErr := resetSQLServerDeleteSession(ctx, connection); resetErr != nil {
				discardSQLServerDeleteConnection(connection)
				discarded = true
				// A committed receipt remains durable authority. For an incomplete
				// transaction, surface every cleanup failure to preserve the core's
				// no-unmarked-mutation contract.
				if active || resultErr != nil {
					result = deleteTargetBatchReceipt{}
					resultErr = errors.Join(resultErr, fmt.Errorf("reset SQL Server delete receipt session: %w", resetErr))
				}
			}
		}
		if !closed {
			if closeErr := connection.Close(); closeErr != nil && !errors.Is(closeErr, sql.ErrConnDone) {
				// A close failure after an unacknowledged transaction has already
				// been discarded above. On a clean committed receipt the database
				// result remains authoritative; discard the pool handle rather
				// than reporting a target-mutation error without a durable marker.
				discardSQLServerDeleteConnection(connection)
				if active || resultErr != nil {
					result = deleteTargetBatchReceipt{}
					resultErr = errors.Join(resultErr, fmt.Errorf("close pinned SQL Server delete receipt connection: %w", closeErr))
				}
			}
		}
	}()
	// Do not detach setup from cancellation. A delete batch that is waiting on
	// its target lock must stop at the caller's deadline; only rollback/reset
	// below get a bounded detached cleanup context.
	sessionConfigured = true
	if err := configureSQLServerDeleteTransaction(ctx, connection); err != nil {
		return result, fmt.Errorf("configure SQL Server delete transaction: %w", err)
	}
	if _, err := connection.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		// See the key-reader path: only a confirmed BEGIN owns transaction
		// cleanup; a setup failure is reset/discard/close cleanup only.
		return result, fmt.Errorf("begin SQL Server delete receipt transaction: %w", err)
	}
	active = true
	if err := acquireSQLServerDeleteBatchLock(ctx, connection, lockResource); err != nil {
		return result, fmt.Errorf("acquire SQL Server transaction-owned delete batch lock: %w", err)
	}
	qualified := sqlServerQualified(batch.Table.Schema, batch.Table.Name)
	if err := acquireSQLServerDeleteTableLock(ctx, connection, qualified, true); err != nil {
		return result, fmt.Errorf("reserve SQL Server delete target table: %w", err)
	}
	if capability.adapter.deleteAfterReservation != nil {
		if err := capability.adapter.deleteAfterReservation(ctx, connection); err != nil {
			return result, fmt.Errorf("run SQL Server delete post-reservation test seam: %w", err)
		}
	}
	lockedAuthority, err := inspectSQLServerDeleteCatalogAuthority(
		ctx, connection, capability.adapter.namespace, capability.authority.TableShape, true,
	)
	if err != nil {
		return result, fmt.Errorf("revalidate reserved SQL Server delete target catalog: %w", err)
	}
	if !sameSQLServerDeleteCatalogAuthority(capability.authority, lockedAuthority) ||
		!lockedAuthority.CanSelect || !lockedAuthority.CanDelete {
		return result, errors.New("reserved SQL Server delete target authority changed; no delete or receipt was committed")
	}
	journal, err := ensureSQLServerDeleteReceiptJournal(ctx, connection, false)
	if err != nil {
		return result, fmt.Errorf("verify exact private SQL Server delete receipt journal: %w", err)
	}
	stored, found, err := loadSQLServerDeleteReceipt(ctx, connection, batch.Token, capability.authority, journal)
	if err != nil {
		return result, err
	}
	if found {
		if err := validateSQLServerDeleteReceipt(batch, stored, capability.authority, journal); err != nil {
			return result, err
		}
		if _, commitErr := capability.adapter.commitSQLServerDeleteReceipt(ctx, connection); commitErr != nil {
			active = false
			return capability.classifySQLServerDeleteCommitAmbiguity(ctx, connection, &closed, batch, commitErr)
		}
		active = false
		return stored, nil
	}
	deleteResult, err := connection.ExecContext(ctx, statement, flattenSQLServerDeleteArguments(keys)...)
	if err != nil {
		return result, fmt.Errorf("SQL Server delete batch rolled back with no receipt: %w", err)
	}
	deletedRows, err := deleteResult.RowsAffected()
	if err != nil || deletedRows < 0 || deletedRows > int64(len(keys)) {
		return result, fmt.Errorf("SQL Server delete batch returned unsafe affected-row count: affected=%d err=%w", deletedRows, err)
	}
	receipt := deleteTargetBatchReceipt{
		PlanID:      batch.PlanID,
		Token:       batch.Token,
		Sequence:    batch.Sequence,
		BatchDigest: batch.BatchDigest,
		Candidates:  int64(len(keys)),
		DeletedRows: deletedRows,
	}
	receipt.ReceiptDigest, err = sqlServerDeleteReceiptDigest(receipt, capability.authority, journal)
	if err != nil {
		return result, err
	}
	if err := insertSQLServerDeleteReceipt(ctx, connection, receipt, capability.authority, journal); err != nil {
		return result, fmt.Errorf("persist SQL Server delete receipt: %w", err)
	}
	if _, commitErr := capability.adapter.commitSQLServerDeleteReceipt(ctx, connection); commitErr != nil {
		active = false
		return capability.classifySQLServerDeleteCommitAmbiguity(ctx, connection, &closed, batch, commitErr)
	}
	active = false
	return receipt, nil
}

var (
	_ deleteKeySource        = (*sqlServerDeleteSourceCapability)(nil)
	_ deleteKeyTarget        = (*sqlServerDeleteTargetCapability)(nil)
	_ deleteKeyCanonicalizer = (*sqlServerDeleteKeyCanonicalizer)(nil)
)
