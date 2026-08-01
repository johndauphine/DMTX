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
	mysqlDeleteMaximumParameters = 65535
	mysqlDeleteMaximumBatchBytes = 64 << 20
)

// mysqlDeleteReconciliationCapabilities is deliberately reached only through
// the existing Stage 4 delete-capability constructor.  Both endpoints use the
// canonical mysql adapter name, so live flavor equality is an explicit part of
// this capability rather than an assumption made from configuration aliases.
type mysqlDeleteReconciliationCapabilities struct {
	source        deleteKeySource
	target        deleteKeyTarget
	canonicalizer deleteKeyCanonicalizer
}

type mysqlDeleteSourceCapability struct {
	adapter          *relationalSourceAdapter
	authority        mysqlDeleteCatalogAuthority
	endpointIdentity string
}

type mysqlDeleteTargetCapability struct {
	adapter        *mysqlTargetAdapter
	authority      mysqlDeleteCatalogAuthority
	targetIdentity string
}

// mysqlDeleteCatalogAuthority binds a key reader or writer to one complete
// InnoDB relation catalog.  The digest includes no values or credentials; it
// changes when the exact discovered table/key metadata changes.
type mysqlDeleteCatalogAuthority struct {
	Flavor        engine.MySQLServerFlavor
	Namespace     string
	Table         schema.Table
	PrimaryKey    []schema.Column
	CatalogDigest string
	CanSelect     bool
	CanDelete     bool
}

// newMySQLDeleteReconciliationCapabilities admits only same-flavor native
// MySQL-family routes.  Mixed MySQL/MariaDB cells deliberately remain refused:
// their textual and parameter equality rules have not been proven here.
func newMySQLDeleteReconciliationCapabilities(
	ctx context.Context,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
) (postgresDeleteReconciliationCapabilities, error) {
	if ctx == nil {
		return postgresDeleteReconciliationCapabilities{}, errors.New(
			"MySQL delete reconciliation context is required",
		)
	}
	if err := ctx.Err(); err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	sourceCapability, err := newMySQLDeleteSourceCapability(ctx, source, sourceTable)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	targetCapability, err := newMySQLDeleteTargetCapability(ctx, target, targetTable)
	if err != nil {
		return postgresDeleteReconciliationCapabilities{}, err
	}
	if sourceCapability.endpointIdentity == targetCapability.targetIdentity &&
		sourceTable.Name == targetTable.Name {
		return postgresDeleteReconciliationCapabilities{}, errors.New(
			"MySQL delete reconciliation rejects an identical source and target relation",
		)
	}
	canonicalizer, err := newMySQLDeleteKeyCanonicalizer(
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

// newMySQLDeleteSourceCapability has no target assumptions. A later route
// can pair this complete native snapshot reader with another target only when
// it supplies an independently proven canonical key-equality contract.
func newMySQLDeleteSourceCapability(
	ctx context.Context,
	source sourceAdapter,
	table schema.Table,
) (*mysqlDeleteSourceCapability, error) {
	adapter, ok := source.(*relationalSourceAdapter)
	if !ok || adapter == nil || adapter.database == nil || adapter.spec.engine != "mysql" ||
		!supportedMySQLDeleteFlavor(adapter.mySQLFlavor) {
		return nil, errors.New("delete reconciliation requires a verified MySQL 8.0 or MariaDB 10.11 relational source adapter")
	}
	if table.Schema != adapter.namespace || table.Name == mysqlDeleteJournalTable {
		return nil, fmt.Errorf("MySQL delete source table is outside its namespace or is reserved private receipt state")
	}
	identity, err := readMySQLDatabaseIdentity(ctx, adapter.database)
	if err != nil {
		return nil, fmt.Errorf("identify MySQL delete source: %w", err)
	}
	if identity.flavor != adapter.mySQLFlavor || identity.database != adapter.namespace {
		return nil, errors.New("MySQL delete source live endpoint identity differs from its opened adapter")
	}
	canonicalIdentity, err := mysqlDeleteCanonicalTargetIdentity(mysqlDeleteEndpointIdentity{
		flavor: identity.flavor, serverIdentity: identity.serverIdentity, database: identity.database, version: "identity-only",
	})
	if err != nil {
		return nil, fmt.Errorf("canonicalize MySQL delete source identity: %w", err)
	}
	authority, err := inspectMySQLDeleteCatalogAuthority(ctx, adapter.database, adapter.mySQLFlavor, adapter.namespace, table, true, false)
	if err != nil {
		return nil, fmt.Errorf("validate MySQL delete source catalog: %w", err)
	}
	return &mysqlDeleteSourceCapability{adapter: adapter, authority: authority, endpointIdentity: canonicalIdentity}, nil
}

// newMySQLDeleteTargetCapability is similarly target-only. It authenticates
// the private journal admission without assuming a particular source engine
// or a same-flavor pair; those constraints belong to the pair canonicalizer.
func newMySQLDeleteTargetCapability(
	ctx context.Context,
	target targetAdapter,
	table schema.Table,
) (*mysqlDeleteTargetCapability, error) {
	adapter, ok := target.(*mysqlTargetAdapter)
	if !ok || adapter == nil || adapter.database == nil || !supportedMySQLDeleteFlavor(adapter.flavor) {
		return nil, errors.New("delete reconciliation requires a verified MySQL 8.0 or MariaDB 10.11 target adapter")
	}
	if table.Schema != adapter.namespace || table.Name == mysqlDeleteJournalTable {
		return nil, fmt.Errorf("MySQL delete target table is outside its namespace or is reserved private receipt state")
	}
	identity, err := readMySQLDatabaseIdentity(ctx, adapter.database)
	if err != nil {
		return nil, fmt.Errorf("identify MySQL delete target: %w", err)
	}
	if identity.flavor != adapter.flavor || identity.database != adapter.namespace {
		return nil, errors.New("MySQL delete target live endpoint identity differs from its opened adapter")
	}
	canonicalIdentity, err := mysqlDeleteCanonicalTargetIdentity(mysqlDeleteEndpointIdentity{
		flavor: identity.flavor, serverIdentity: identity.serverIdentity, database: identity.database, version: "identity-only",
	})
	if err != nil {
		return nil, fmt.Errorf("canonicalize MySQL delete target identity: %w", err)
	}
	authority, err := inspectMySQLDeleteCatalogAuthority(ctx, adapter.database, adapter.flavor, adapter.namespace, table, true, true)
	if err != nil {
		return nil, fmt.Errorf("validate MySQL delete target catalog: %w", err)
	}
	if err := preflightMySQLDeleteReceiptJournal(ctx, adapter); err != nil {
		return nil, fmt.Errorf("preflight MySQL delete receipt journal: %w", err)
	}
	return &mysqlDeleteTargetCapability{adapter: adapter, authority: authority, targetIdentity: canonicalIdentity}, nil
}

func supportedMySQLDeleteFlavor(flavor engine.MySQLServerFlavor) bool {
	switch flavor {
	case engine.MySQLServerFlavorOracle80,
		engine.MySQLServerFlavorMariaDB1011:
		return true
	default:
		return false
	}
}

func inspectMySQLDeleteCatalogAuthority(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	flavor engine.MySQLServerFlavor,
	namespace string,
	expected schema.Table,
	requireSelect bool,
	requireDelete bool,
) (mysqlDeleteCatalogAuthority, error) {
	if queryer == nil || !supportedMySQLDeleteFlavor(flavor) ||
		strings.TrimSpace(namespace) == "" || expected.Schema != namespace ||
		strings.TrimSpace(expected.Name) == "" {
		return mysqlDeleteCatalogAuthority{}, errors.New(
			"MySQL delete catalog identity is incomplete",
		)
	}
	if isMySQLDeleteJournalRelation(expected.Name) {
		return mysqlDeleteCatalogAuthority{}, fmt.Errorf(
			"MySQL delete catalog relation %s is reserved", expected.Name,
		)
	}
	live, err := engine.InspectMySQLTableForFlavor(
		ctx,
		queryer,
		flavor,
		namespace,
		expected.Name,
	)
	if err != nil {
		return mysqlDeleteCatalogAuthority{}, err
	}
	// Catalog discovery allocates empty object lists while an unchanged target
	// projection may retain nil. Canonicalize only that wire-level distinction
	// before binding authority; every nonempty object remains exact evidence.
	live = normalizeMySQLDeleteTableShape(live)
	expected = normalizeMySQLDeleteTableShape(expected)
	if !reflect.DeepEqual(live, expected) {
		return mysqlDeleteCatalogAuthority{}, errors.New(
			"MySQL delete catalog shape changed after discovery",
		)
	}
	primaryKey, err := deletePrimaryKeyColumns(live)
	if err != nil {
		return mysqlDeleteCatalogAuthority{}, err
	}
	privileges, err := mysqlDeletePrivileges(ctx, queryer, namespace, live.Name)
	if err != nil {
		return mysqlDeleteCatalogAuthority{}, err
	}
	if requireSelect && !privileges["SELECT"] {
		return mysqlDeleteCatalogAuthority{}, fmt.Errorf(
			"MySQL delete %s requires exact table SELECT privilege",
			live.Name,
		)
	}
	if requireDelete && !privileges["DELETE"] {
		return mysqlDeleteCatalogAuthority{}, fmt.Errorf(
			"MySQL delete target requires exact table DELETE privilege",
		)
	}
	if requireDelete {
		if err := proveMySQLDeletePrivilege(ctx, queryer, live); err != nil {
			return mysqlDeleteCatalogAuthority{}, err
		}
	}
	authority := mysqlDeleteCatalogAuthority{
		Flavor:     flavor,
		Namespace:  namespace,
		Table:      cloneStage4RichTable(live),
		PrimaryKey: append([]schema.Column(nil), primaryKey...),
		CanSelect:  privileges["SELECT"],
		CanDelete:  privileges["DELETE"],
	}
	authority.CatalogDigest, err = mysqlDeleteCatalogAuthorityDigest(
		authority,
	)
	if err != nil {
		return mysqlDeleteCatalogAuthority{}, err
	}
	return authority, nil
}

func normalizeMySQLDeleteTableShape(table schema.Table) schema.Table {
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

// mysqlDeleteTableMatchesAuthority compares the catalog form that the MySQL
// reader itself canonicalizes. Discovery may allocate empty object slices
// while the immutable plan retains nil; those are one wire representation of
// the same table. Every populated catalog field remains part of the exact
// authority and therefore still fails closed on drift.
func mysqlDeleteTableMatchesAuthority(
	table schema.Table,
	authority schema.Table,
) bool {
	return reflect.DeepEqual(
		normalizeMySQLDeleteTableShape(table),
		normalizeMySQLDeleteTableShape(authority),
	)
}

func mysqlDeleteCatalogAuthorityDigest(
	authority mysqlDeleteCatalogAuthority,
) (string, error) {
	if !supportedMySQLDeleteFlavor(authority.Flavor) ||
		strings.TrimSpace(authority.Namespace) == "" ||
		authority.Table.Schema != authority.Namespace ||
		strings.TrimSpace(authority.Table.Name) == "" ||
		len(authority.PrimaryKey) == 0 {
		return "", errors.New("MySQL delete catalog authority is incomplete")
	}
	payload := struct {
		Version    int                      `json:"version"`
		Flavor     engine.MySQLServerFlavor `json:"flavor"`
		Namespace  string                   `json:"namespace"`
		Table      schema.Table             `json:"table"`
		PrimaryKey []schema.Column          `json:"primary_key"`
		CanSelect  bool                     `json:"can_select"`
		CanDelete  bool                     `json:"can_delete"`
	}{
		Version: 1, Flavor: authority.Flavor, Namespace: authority.Namespace,
		Table: authority.Table, PrimaryKey: authority.PrimaryKey,
		CanSelect: authority.CanSelect, CanDelete: authority.CanDelete,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode MySQL delete catalog authority: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func sameMySQLDeleteCatalogAuthority(
	left mysqlDeleteCatalogAuthority,
	right mysqlDeleteCatalogAuthority,
) bool {
	if left.CatalogDigest == "" || right.CatalogDigest == "" ||
		left.CatalogDigest != right.CatalogDigest {
		return false
	}
	leftDigest, leftErr := mysqlDeleteCatalogAuthorityDigest(left)
	rightDigest, rightErr := mysqlDeleteCatalogAuthorityDigest(right)
	return leftErr == nil && rightErr == nil &&
		left.CatalogDigest == leftDigest &&
		right.CatalogDigest == rightDigest &&
		reflect.DeepEqual(left, right)
}

func mysqlDeletePrivileges(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	namespace string,
	table string,
) (map[string]bool, error) {
	if queryer == nil || strings.TrimSpace(namespace) == "" ||
		strings.TrimSpace(table) == "" {
		return nil, errors.New("MySQL delete privilege identity is incomplete")
	}
	rows, err := queryer.QueryContext(ctx, `
		SELECT PRIVILEGE_TYPE
		  FROM information_schema.USER_PRIVILEGES
		 WHERE GRANTEE = CONCAT(
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', 1)),
			'@',
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', -1))
		 )
		UNION
		SELECT PRIVILEGE_TYPE
		  FROM information_schema.SCHEMA_PRIVILEGES
		 WHERE TABLE_SCHEMA = ?
		   AND GRANTEE = CONCAT(
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', 1)),
			'@',
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', -1))
		 )
		UNION
		SELECT PRIVILEGE_TYPE
		  FROM information_schema.TABLE_PRIVILEGES
		 WHERE TABLE_SCHEMA = ?
		   AND TABLE_NAME = ?
		   AND GRANTEE = CONCAT(
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', 1)),
			'@',
			QUOTE(SUBSTRING_INDEX(CURRENT_USER(), '@', -1))
		 )`, namespace, namespace, table)
	if err != nil {
		return nil, fmt.Errorf("inspect MySQL delete privileges: %w", err)
	}
	result := make(map[string]bool)
	for rows.Next() {
		var privilege string
		if err := rows.Scan(&privilege); err != nil {
			closeErr := rows.Close()
			return nil, errors.Join(
				fmt.Errorf("read MySQL delete privilege: %w", err),
				closeErr,
			)
		}
		result[strings.ToUpper(strings.TrimSpace(privilege))] = true
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, fmt.Errorf("iterate MySQL delete privileges: %w", err)
	}
	if result["ALL PRIVILEGES"] {
		for _, privilege := range []string{"SELECT", "INSERT", "CREATE", "DELETE"} {
			result[privilege] = true
		}
	}
	return result, nil
}

func proveMySQLDeletePrivilege(
	ctx context.Context,
	queryer engine.MySQLCatalogQueryer,
	table schema.Table,
) error {
	rows, err := queryer.QueryContext(
		ctx,
		"EXPLAIN DELETE FROM "+mySQLQualified(table.Schema, table.Name)+
			" WHERE 1 = 0",
	)
	if err != nil {
		return fmt.Errorf("prove MySQL target DELETE privilege: %w", err)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("close MySQL target DELETE privilege proof: %w", err)
	}
	return nil
}

type mysqlDeleteKeyRows struct {
	rows       *sql.Rows
	connection *sql.Conn
	width      int
}

func (rows *mysqlDeleteKeyRows) Next() bool {
	return rows != nil && rows.rows != nil && rows.rows.Next()
}

func (rows *mysqlDeleteKeyRows) Values() ([]any, error) {
	if rows == nil || rows.rows == nil || rows.width <= 0 {
		return nil, errors.New("MySQL delete key reader is closed")
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

func (rows *mysqlDeleteKeyRows) Err() error {
	if rows == nil || rows.rows == nil {
		return nil
	}
	return rows.rows.Err()
}

func (rows *mysqlDeleteKeyRows) Close() (result error) {
	if rows == nil {
		return nil
	}
	if rows.rows != nil {
		result = errors.Join(result, rows.rows.Close())
		rows.rows = nil
	}
	if rows.connection != nil {
		cleanupCtx, cancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		_, rollbackErr := rows.connection.ExecContext(cleanupCtx, "ROLLBACK")
		cancel()
		if rollbackErr != nil {
			discardMySQLConnection(rows.connection)
			result = errors.Join(
				result,
				fmt.Errorf("roll back MySQL delete key snapshot: %w", rollbackErr),
			)
		}
		result = errors.Join(result, rows.connection.Close())
		rows.connection = nil
	}
	return result
}

func openMySQLDeletePrimaryKeys(
	ctx context.Context,
	database *sql.DB,
	flavor engine.MySQLServerFlavor,
	namespace string,
	table schema.Table,
	columns []string,
	authority mysqlDeleteCatalogAuthority,
) (deleteKeyRows, error) {
	if database == nil || !supportedMySQLDeleteFlavor(flavor) {
		return nil, errors.New("MySQL delete key reader is unavailable")
	}
	if !sameMySQLDeleteCatalogAuthority(authority, mysqlDeleteCatalogAuthority{
		Flavor: authority.Flavor, Namespace: authority.Namespace,
		Table: authority.Table, PrimaryKey: authority.PrimaryKey,
		CatalogDigest: authority.CatalogDigest, CanSelect: authority.CanSelect,
		CanDelete: authority.CanDelete,
	}) {
		return nil, errors.New("MySQL delete key authority is malformed")
	}
	if !mysqlDeleteTableMatchesAuthority(table, authority.Table) {
		return nil, errors.New("MySQL delete key table differs from admitted authority")
	}
	if len(columns) != len(authority.PrimaryKey) {
		return nil, errors.New("MySQL delete key request width differs from the primary key")
	}
	for index := range columns {
		if columns[index] != authority.PrimaryKey[index].Name {
			return nil, errors.New("MySQL delete key request is not in exact primary-key order")
		}
	}
	connection, err := database.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire MySQL delete key snapshot connection: %w", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
	); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("set MySQL delete key snapshot isolation: %w", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		"START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY",
	); err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("begin MySQL delete key consistent snapshot: %w", err)
	}
	abort := func(cause error) error {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		_, rollbackErr := connection.ExecContext(cleanupCtx, "ROLLBACK")
		cancel()
		if rollbackErr != nil {
			discardMySQLConnection(connection)
		}
		closeErr := connection.Close()
		if errors.Is(closeErr, sql.ErrConnDone) {
			closeErr = nil
		}
		return errors.Join(cause, rollbackErr, closeErr)
	}
	if err := acquireMySQLDeleteSnapshotMetadataLock(ctx, connection, table); err != nil {
		return nil, abort(fmt.Errorf("acquire MySQL delete key snapshot metadata lock: %w", err))
	}
	// This reader deliberately remains read-only. The target's delete
	// privilege was actively proven during capability admission and is still
	// represented by CanDelete below; issuing EXPLAIN DELETE here is rejected
	// by supported MySQL-family servers inside START TRANSACTION READ ONLY.
	// Re-reading privileges and comparing the full authority detects a later
	// revoke without smuggling a DML-shaped statement into the snapshot.
	current, err := inspectMySQLDeleteCatalogAuthority(
		ctx,
		connection,
		flavor,
		namespace,
		table,
		true,
		false,
	)
	if err != nil {
		return nil, abort(fmt.Errorf("revalidate MySQL delete key catalog in consistent snapshot: %w", err))
	}
	if !sameMySQLDeleteCatalogAuthority(authority, current) {
		return nil, abort(errors.New("MySQL delete key catalog authority changed in consistent snapshot"))
	}
	sqlRows, err := connection.QueryContext(
		ctx,
		mysqlDeletePrimaryKeyQuery(namespace, table.Name, columns),
	)
	if err != nil {
		return nil, abort(fmt.Errorf("open MySQL delete primary keys for %s.%s: %w", namespace, table.Name, err))
	}
	return &mysqlDeleteKeyRows{
		rows: sqlRows, connection: connection, width: len(columns),
	}, nil
}

// acquireMySQLDeleteSnapshotMetadataLock uses an ordinary consistent-snapshot
// read, not LOCK IN SHARE MODE. MySQL-family servers reject locking reads in a
// READ ONLY transaction on some supported versions. A regular SELECT still
// holds the table's shared metadata lock until the pinned transaction closes,
// which keeps the exact catalog revalidation and PK stream behind one
// non-mutating schema-stability boundary.
func acquireMySQLDeleteSnapshotMetadataLock(
	ctx context.Context,
	connection *sql.Conn,
	table schema.Table,
) error {
	if connection == nil {
		return errors.New("MySQL delete snapshot connection is unavailable")
	}
	rows, err := connection.QueryContext(ctx,
		"SELECT 1 FROM "+mySQLQualified(table.Schema, table.Name)+" LIMIT 1")
	if err != nil {
		return err
	}
	return errors.Join(rows.Err(), rows.Close())
}

func mysqlDeletePrimaryKeyQuery(
	namespace string,
	table string,
	columns []string,
) string {
	quoted := mySQLQuotedColumns(columns)
	return "SELECT " + quoted + " FROM " +
		mySQLQualified(namespace, table) + " ORDER BY " + quoted
}

func (capability *mysqlDeleteSourceCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil ||
		capability.adapter.database == nil ||
		capability.adapter.spec.engine != "mysql" {
		return nil, errors.New("MySQL delete source authority is unavailable")
	}
	return openMySQLDeletePrimaryKeys(
		ctx,
		capability.adapter.database,
		capability.adapter.mySQLFlavor,
		capability.adapter.namespace,
		table,
		columns,
		capability.authority,
	)
}

func (capability *mysqlDeleteTargetCapability) OpenDeletePrimaryKeys(
	ctx context.Context,
	table schema.Table,
	columns []string,
) (deleteKeyRows, error) {
	if capability == nil || capability.adapter == nil ||
		capability.adapter.database == nil {
		return nil, errors.New("MySQL delete target authority is unavailable")
	}
	return openMySQLDeletePrimaryKeys(
		ctx,
		capability.adapter.database,
		capability.adapter.flavor,
		capability.adapter.namespace,
		table,
		columns,
		capability.authority,
	)
}

func (*mysqlDeleteTargetCapability) MaxDeleteParameters() int {
	return mysqlDeleteMaximumParameters
}

type mysqlDeleteKeyCanonicalizer struct {
	sourceTable schema.Table
	targetTable schema.Table
	proof       deleteKeyEqualityProof
}

func newMySQLDeleteKeyCanonicalizer(
	source schema.Table,
	target schema.Table,
	sourceAuthority mysqlDeleteCatalogAuthority,
	targetAuthority mysqlDeleteCatalogAuthority,
) (*mysqlDeleteKeyCanonicalizer, error) {
	if !sameMySQLDeleteFlavor(sourceAuthority.Flavor, targetAuthority.Flavor) {
		return nil, errors.New("MySQL delete key canonicalizer requires matching live server flavors")
	}
	if !sameMySQLDeleteCatalogAuthority(
		sourceAuthority,
		mustMySQLDeleteCatalogAuthority(sourceAuthority),
	) || !sameMySQLDeleteCatalogAuthority(
		targetAuthority,
		mustMySQLDeleteCatalogAuthority(targetAuthority),
	) {
		return nil, errors.New("MySQL delete key catalog authority digest differs")
	}
	sourceKey := sourceAuthority.PrimaryKey
	targetKey := targetAuthority.PrimaryKey
	if len(sourceKey) == 0 || len(sourceKey) != len(targetKey) {
		return nil, errors.New("MySQL delete source and target primary-key widths differ")
	}
	semantics := make([]string, len(sourceKey))
	hasTextKey := false
	for index := range sourceKey {
		if sourceKey[index].Name != targetKey[index].Name ||
			sourceKey[index].PrimaryKeyPosition != index+1 ||
			targetKey[index].PrimaryKeyPosition != index+1 ||
			sourceKey[index].Nullable || targetKey[index].Nullable ||
			!reflect.DeepEqual(sourceKey[index], targetKey[index]) {
			return nil, fmt.Errorf(
				"MySQL delete primary-key column %d is not preserved exactly",
				index+1,
			)
		}
		semanticsValue, err := mysqlDeleteProofSemantics(sourceKey[index])
		if err != nil {
			return nil, fmt.Errorf(
				"MySQL delete primary-key column %s: %w",
				sourceKey[index].Name,
				err,
			)
		}
		semantics[index] = semanticsValue
		hasTextKey = hasTextKey || semantics[index] == "binary_text"
	}
	// Integer, boolean, and byte-key comparison is collation-independent.
	// Only an actual text key needs proof that both servers use the same
	// certified binary equality domain.
	if hasTextKey && (source.MySQLCollation == "" ||
		source.MySQLCollation != target.MySQLCollation ||
		!mysqlDeleteBinaryCollation(sourceAuthority.Flavor, source.MySQLCollation)) {
		return nil, errors.New("MySQL text primary-key equality requires matching binary table collation authority")
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(source, sourceKey)
	if err != nil {
		return nil, err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(target, targetKey)
	if err != nil {
		return nil, err
	}
	routeDigest := sha256.Sum256([]byte(
		sourceAuthority.CatalogDigest + "\x00" + targetAuthority.CatalogDigest,
	))
	proof := deleteKeyEqualityProof{
		CanonicalizerID: "mysql-exact-primary-key-v1:" +
			hex.EncodeToString(routeDigest[:]),
		SourceFingerprint: sourceFingerprint,
		TargetFingerprint: targetFingerprint,
		Columns:           make([]deleteKeyColumnProof, len(sourceKey)),
	}
	for index := range sourceKey {
		proof.Columns[index].Semantics = semantics[index]
		if semantics[index] == "binary_text" {
			proof.Columns[index].CollationEvidence = fmt.Sprintf(
				"mysql-%d-exact-binary-pk-collation:%s",
				sourceAuthority.Flavor,
				source.MySQLCollation,
			)
		}
	}
	if _, err := validateDeleteKeyEqualityProof(
		proof,
		source,
		target,
		sourceKey,
		targetKey,
	); err != nil {
		return nil, err
	}
	return &mysqlDeleteKeyCanonicalizer{
		sourceTable: cloneStage4RichTable(source),
		targetTable: cloneStage4RichTable(target),
		proof:       proof,
	}, nil
}

// mustMySQLDeleteCatalogAuthority recomputes only deterministic local fields.
// It is intentionally named after the invariant, not a panic: callers use it
// with sameMySQLDeleteCatalogAuthority so malformed input simply fails closed.
func mustMySQLDeleteCatalogAuthority(
	authority mysqlDeleteCatalogAuthority,
) mysqlDeleteCatalogAuthority {
	digest, err := mysqlDeleteCatalogAuthorityDigest(authority)
	if err != nil {
		return mysqlDeleteCatalogAuthority{}
	}
	authority.CatalogDigest = digest
	return authority
}

func sameMySQLDeleteFlavor(
	left engine.MySQLServerFlavor,
	right engine.MySQLServerFlavor,
) bool {
	return supportedMySQLDeleteFlavor(left) && left == right
}

func mysqlDeleteBinaryCollation(
	flavor engine.MySQLServerFlavor,
	collation string,
) bool {
	collation = strings.ToLower(strings.TrimSpace(collation))
	switch flavor {
	case engine.MySQLServerFlavorOracle80:
		return collation == "utf8mb4_bin" ||
			collation == "utf8mb4_0900_bin"
	case engine.MySQLServerFlavorMariaDB1011:
		return collation == "utf8mb4_nopad_bin"
	default:
		return false
	}
}

func mysqlDeleteProofSemantics(
	column schema.Column,
) (string, error) {
	base := strings.ToLower(strings.TrimSpace(column.Type))
	if opening := strings.IndexByte(base, '('); opening >= 0 {
		base = strings.TrimSpace(base[:opening])
	}
	switch base {
	case "char", "character", "binary":
		return "", fmt.Errorf(
			"fixed-width MySQL key type %q has padded equality and is unsupported",
			column.Type,
		)
	case "json", "enum", "set":
		return "", fmt.Errorf(
			"MySQL key type %q lacks an exact portable equality domain",
			column.Type,
		)
	}
	kind, err := validationKindForColumn(column)
	if err != nil {
		return "", err
	}
	switch kind {
	case validationBoolean:
		return "boolean", nil
	case validationInteger:
		return "integer", nil
	case validationBytes:
		return "binary", nil
	case validationText:
		return "binary_text", nil
	default:
		return "", fmt.Errorf(
			"MySQL key type %q lacks a certified canonical equality domain",
			column.Type,
		)
	}
}

func (canonicalizer *mysqlDeleteKeyCanonicalizer) ProveDeleteKeyEquality(
	source schema.Table,
	target schema.Table,
	sourceKey []schema.Column,
	targetKey []schema.Column,
) (deleteKeyEqualityProof, error) {
	if canonicalizer == nil ||
		!reflect.DeepEqual(source, canonicalizer.sourceTable) ||
		!reflect.DeepEqual(target, canonicalizer.targetTable) {
		return deleteKeyEqualityProof{}, errors.New(
			"MySQL delete key proof was requested for different tables",
		)
	}
	if _, err := validateDeleteKeyEqualityProof(
		canonicalizer.proof,
		source,
		target,
		sourceKey,
		targetKey,
	); err != nil {
		return deleteKeyEqualityProof{}, err
	}
	proof := canonicalizer.proof
	proof.Columns = append([]deleteKeyColumnProof(nil), proof.Columns...)
	return proof, nil
}

func (canonicalizer *mysqlDeleteKeyCanonicalizer) CanonicalizeDeleteKeyValue(
	side deleteKeySide,
	proof deleteKeyEqualityProof,
	index int,
	value any,
) (deleteCanonicalValue, error) {
	if canonicalizer == nil || !reflect.DeepEqual(proof, canonicalizer.proof) ||
		index < 0 || index >= len(proof.Columns) {
		return deleteCanonicalValue{}, errors.New(
			"MySQL delete key canonicalization proof differs",
		)
	}
	var (
		canonical []byte
		err       error
	)
	switch proof.Columns[index].Semantics {
	case "boolean":
		canonical, err = canonicalValidationBoolean(value)
	case "integer":
		canonical, err = canonicalValidationInteger(value)
	case "binary":
		canonical, err = canonicalValidationBytes(value)
	case "binary_text":
		canonical, err = canonicalValidationText(value)
	default:
		err = fmt.Errorf(
			"unsupported MySQL delete key semantics %q",
			proof.Columns[index].Semantics,
		)
	}
	if err != nil {
		return deleteCanonicalValue{}, err
	}
	result := deleteCanonicalValue{Canonical: append([]byte(nil), canonical...)}
	if side == deleteKeyTargetSide {
		parameter, err := driver.DefaultParameterConverter.ConvertValue(value)
		if err != nil {
			return deleteCanonicalValue{}, fmt.Errorf(
				"convert MySQL delete parameter: %w", err,
			)
		}
		result.Parameter, err = stableDeleteParameter(parameter)
		if err != nil {
			return deleteCanonicalValue{}, err
		}
	} else if side != deleteKeySourceSide {
		return deleteCanonicalValue{}, fmt.Errorf(
			"unknown MySQL delete key side %q", side,
		)
	}
	return result, nil
}

func validateMySQLDeleteBatch(
	namespace string,
	batch deleteTargetBatch,
) ([][]driver.Value, error) {
	return validateMySQLDeleteBatchWithLimits(
		namespace,
		batch,
		mysqlDeleteMaximumParameters,
		mysqlDeleteMaximumBatchBytes,
	)
}

func validateMySQLDeleteBatchWithLimits(
	namespace string,
	batch deleteTargetBatch,
	maximumParameters int,
	maximumBytes int64,
) ([][]driver.Value, error) {
	if maximumParameters <= 0 || maximumBytes <= 0 {
		return nil, errors.New("MySQL delete batch limits are invalid")
	}
	if strings.TrimSpace(namespace) == "" ||
		batch.Table.Schema != namespace ||
		strings.TrimSpace(batch.Table.Name) == "" ||
		len(batch.Columns) == 0 || len(batch.Keys) == 0 ||
		strings.TrimSpace(batch.PlanID) == "" || batch.Sequence < 0 {
		return nil, errors.New("MySQL delete batch identity is incomplete")
	}
	if _, err := hex.DecodeString(batch.PlanID); err != nil ||
		len(batch.PlanID) != 32 || batch.PlanID != strings.ToLower(batch.PlanID) {
		return nil, errors.New("MySQL delete batch plan ID must be 32 lowercase hexadecimal characters")
	}
	if err := validateLowerSHA256("MySQL delete batch token", batch.Token); err != nil {
		return nil, err
	}
	if err := validateLowerSHA256("MySQL delete batch digest", batch.BatchDigest); err != nil {
		return nil, err
	}
	if len(batch.Keys) > maximumParameters/len(batch.Columns) {
		return nil, fmt.Errorf(
			"MySQL delete batch exceeds the %d-parameter limit",
			maximumParameters,
		)
	}
	var encodedBytes int64
	result := make([][]driver.Value, len(batch.Keys))
	for rowIndex, key := range batch.Keys {
		if len(key) != len(batch.Columns) {
			return nil, fmt.Errorf("MySQL delete key %d width differs", rowIndex)
		}
		result[rowIndex] = make([]driver.Value, len(key))
		for columnIndex, value := range key {
			stable, err := stableDeleteParameter(value)
			if err != nil || stable == nil {
				return nil, fmt.Errorf(
					"MySQL delete key %d column %d is not parameter-safe",
					rowIndex,
					columnIndex,
				)
			}
			var valueBytes int64 = 16
			switch typed := stable.(type) {
			case string:
				valueBytes = int64(len(typed))
			case []byte:
				valueBytes = int64(len(typed))
			}
			if valueBytes > maximumBytes-encodedBytes {
				return nil, fmt.Errorf(
					"MySQL delete batch exceeds the %d-byte adapter ceiling",
					maximumBytes,
				)
			}
			encodedBytes += valueBytes
			result[rowIndex][columnIndex] = stable
		}
	}
	return result, nil
}

func mysqlDeleteBatchStatement(
	table schema.Table,
	columns []string,
	rowCount int,
) (string, error) {
	if strings.TrimSpace(table.Schema) == "" ||
		strings.TrimSpace(table.Name) == "" || len(columns) == 0 ||
		rowCount <= 0 {
		return "", errors.New("MySQL delete statement shape is incomplete")
	}
	tuples := make([]string, rowCount)
	for row := range tuples {
		placeholders := make([]string, len(columns))
		for column := range placeholders {
			placeholders[column] = "?"
		}
		tuples[row] = "(" + strings.Join(placeholders, ", ") + ")"
	}
	return "DELETE FROM " + mySQLQualified(table.Schema, table.Name) +
		" WHERE (" + mySQLQuotedColumns(columns) + ") IN (" +
		strings.Join(tuples, ", ") + ")", nil
}

func flattenMySQLDeleteArguments(keys [][]driver.Value) []any {
	result := make([]any, 0)
	for _, key := range keys {
		for _, value := range key {
			result = append(result, value)
		}
	}
	return result
}

var (
	_ deleteKeySource        = (*mysqlDeleteSourceCapability)(nil)
	_ deleteKeyTarget        = (*mysqlDeleteTargetCapability)(nil)
	_ deleteKeyCanonicalizer = (*mysqlDeleteKeyCanonicalizer)(nil)
)
