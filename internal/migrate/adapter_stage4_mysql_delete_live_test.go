package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// These tests use isolated, granted target databases because the receipt
// journal's deliberately fixed private name is real target authority. They
// therefore prove the native MySQL/MariaDB behavior without sharing or
// deleting state from the common route fixtures.
func TestMySQLDeleteJournalEmptyPrefixRestartLive(t *testing.T) {
	for _, fixture := range mysqlDeleteJournalLiveFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			testMySQLDeleteJournalEmptyPrefixRestartLive(t, fixture)
		})
	}
}

func TestMySQLDeleteReadOnlySnapshotMetadataLockLive(t *testing.T) {
	for _, fixture := range mysqlDeleteJournalLiveFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			testMySQLDeleteReadOnlySnapshotMetadataLockLive(t, fixture)
		})
	}
}

func TestMySQLDeleteReceiptReplaysExactCommittedBatchLive(t *testing.T) {
	for _, fixture := range mysqlDeleteJournalLiveFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			ctx, database, source, target, sourceTable, targetTable, batch, openTarget :=
				openMySQLDeleteReceiptCapabilityLive(t, fixture)
			capabilities, err := newMySQLDeleteReconciliationCapabilities(
				ctx, source, target, sourceTable, targetTable,
			)
			if err != nil {
				t.Fatalf("admit %s delete receipt capability: %v", fixture.name, err)
			}
			first, err := capabilities.target.ApplyDeleteBatch(ctx, batch)
			if err != nil {
				t.Fatalf("apply first %s delete receipt batch: %v", fixture.name, err)
			}
			if first.Candidates != 1 || first.DeletedRows != 1 {
				t.Fatalf("first %s delete receipt = %#v", fixture.name, first)
			}
			if err := target.Close(); err != nil {
				t.Fatalf("close first %s delete target: %v", fixture.name, err)
			}
			reopenedValue := openTarget()
			reopened, ok := reopenedValue.(*mysqlTargetAdapter)
			if !ok {
				t.Fatalf("reopened %s delete target = %T", fixture.name, reopenedValue)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			reopenedCapabilities, err := newMySQLDeleteReconciliationCapabilities(
				ctx, source, reopened, sourceTable, targetTable,
			)
			if err != nil {
				t.Fatalf("re-admit reopened %s delete receipt capability: %v", fixture.name, err)
			}
			replayed, err := reopenedCapabilities.target.ApplyDeleteBatch(ctx, batch)
			if err != nil {
				t.Fatalf("replay committed %s delete receipt batch: %v", fixture.name, err)
			}
			if replayed != first {
				t.Fatalf("replayed %s delete receipt differs\nfirst=%#v\nreplayed=%#v", fixture.name, first, replayed)
			}
			var rows int
			if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(targetTable.Schema, targetTable.Name)).Scan(&rows); err != nil {
				t.Fatalf("count replayed %s target rows: %v", fixture.name, err)
			}
			if rows != 1 {
				t.Fatalf("replayed %s delete changed committed rows: %d", fixture.name, rows)
			}
		})
	}
}

func TestMySQLDeleteReceiptCommitAckRecoveryReturnsExactReceiptAfterReopenLive(t *testing.T) {
	for _, fixture := range mysqlDeleteJournalLiveFixtures() {
		fixture := fixture
		t.Run(fixture.name, func(t *testing.T) {
			ctx, _, source, target, sourceTable, targetTable, batch, openTarget :=
				openMySQLDeleteReceiptCapabilityLive(t, fixture)
			capabilities, err := newMySQLDeleteReconciliationCapabilities(
				ctx, source, target, sourceTable, targetTable,
			)
			if err != nil {
				t.Fatalf("admit %s commit-ack delete capability: %v", fixture.name, err)
			}
			target.deleteCommit = func(ctx context.Context, connection *sql.Conn) (sql.Result, error) {
				result, err := connection.ExecContext(ctx, "COMMIT")
				if err != nil {
					return result, err
				}
				return result, errors.New("injected MySQL delete commit acknowledgement loss")
			}
			first, err := capabilities.target.ApplyDeleteBatch(ctx, batch)
			if err != nil {
				t.Fatalf("classify committed %s delete receipt after commit-ack loss: %v", fixture.name, err)
			}
			target.deleteCommit = nil
			if first.Candidates != 1 || first.DeletedRows != 1 {
				t.Fatalf("commit-ack %s delete receipt = %#v", fixture.name, first)
			}
			if err := target.Close(); err != nil {
				t.Fatalf("close commit-ack %s delete target: %v", fixture.name, err)
			}
			reopenedValue := openTarget()
			reopened, ok := reopenedValue.(*mysqlTargetAdapter)
			if !ok {
				t.Fatalf("reopened %s commit-ack target = %T", fixture.name, reopenedValue)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			reopenedCapabilities, err := newMySQLDeleteReconciliationCapabilities(
				ctx, source, reopened, sourceTable, targetTable,
			)
			if err != nil {
				t.Fatalf("re-admit reopened %s commit-ack delete capability: %v", fixture.name, err)
			}
			replayed, err := reopenedCapabilities.target.ApplyDeleteBatch(ctx, batch)
			if err != nil {
				t.Fatalf("replay %s commit-ack receipt after reopen: %v", fixture.name, err)
			}
			if replayed != first {
				t.Fatalf("replayed %s commit-ack receipt differs\nfirst=%#v\nreplayed=%#v", fixture.name, first, replayed)
			}
		})
	}
}

type mysqlDeleteJournalLiveFixture struct {
	name        string
	targetDSN   string
	adminDSN    string
	caPath      string
	tlsConfig   string
	flavor      engine.MySQLServerFlavor
	collation   string
	refreshInfo bool
}

func mysqlDeleteJournalLiveFixtures() []mysqlDeleteJournalLiveFixture {
	return []mysqlDeleteJournalLiveFixture{
		{
			name:        "mysql80",
			targetDSN:   os.Getenv("DMTX_TEST_MYSQL_TARGET_DSN"),
			adminDSN:    os.Getenv("DMTX_TEST_MYSQL_ADMIN_DSN"),
			caPath:      os.Getenv("DMTX_TEST_MYSQL_CA"),
			tlsConfig:   "dmtx_test",
			flavor:      engine.MySQLServerFlavorOracle80,
			collation:   "utf8mb4_0900_bin",
			refreshInfo: true,
		},
		{
			name:        "mariadb1011",
			targetDSN:   os.Getenv("DMTX_TEST_MARIADB_TARGET_DSN"),
			adminDSN:    os.Getenv("DMTX_TEST_MARIADB_ADMIN_DSN"),
			caPath:      os.Getenv("DMTX_TEST_MARIADB_CA"),
			tlsConfig:   "dmtx_mariadb_test",
			flavor:      engine.MySQLServerFlavorMariaDB1011,
			collation:   "utf8mb4_nopad_bin",
			refreshInfo: false,
		},
	}
}

func mysqlDeleteJournalLiveTarget(
	t *testing.T,
	fixture mysqlDeleteJournalLiveFixture,
) (context.Context, *sql.DB, string, string, func() targetAdapter, func() sourceAdapter) {
	t.Helper()
	if fixture.targetDSN == "" || fixture.adminDSN == "" || fixture.caPath == "" {
		t.Skip(
			"set a verified-TLS target/admin DSN and CA for " + fixture.name +
				" delete-journal live recovery coverage",
		)
	}
	registerMySQLCommonFixtureTLSNamed(t, fixture.caPath, fixture.tlsConfig)
	parsed := parseMySQLNativeTargetDSNForTLS(
		t,
		fixture.name+" delete-journal target",
		fixture.targetDSN,
		fixture.tlsConfig,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	temporaryDSN, endpoint := mysqlTargetEvolutionLiveTemporaryDatabase(
		t,
		ctx,
		mysqlTargetEvolutionLiveFixture{
			name:        "delete_" + fixture.name,
			adminDSNEnv: "unused",
			collation:   fixture.collation,
			tlsConfig:   fixture.tlsConfig,
			refreshInfo: fixture.refreshInfo,
		},
		parsed,
		fixture.adminDSN,
		fixture.caPath,
		nil,
	)
	database := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		fixture.name+" delete-journal temporary target",
		temporaryDSN,
		fixture.refreshInfo,
	)
	openTarget := func() targetAdapter {
		opened, err := openMySQLTargetAdapter(ctx, endpoint)
		if err != nil {
			t.Fatalf("open %s delete-journal target adapter: %v", fixture.name, err)
		}
		return opened
	}
	openSource := func() sourceAdapter {
		opened, err := openMySQLSourceAdapter(ctx, endpoint)
		if err != nil {
			t.Fatalf("open %s delete-journal source adapter: %v", fixture.name, err)
		}
		return opened
	}
	return ctx, database, endpoint.Database, temporaryDSN, openTarget, openSource
}

func createMySQLDeleteJournalPrefixLive(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	creatorDigest string,
	journalIdentity string,
) {
	t.Helper()
	createSQL, err := mysqlDeleteJournalCreateSQL(creatorDigest, journalIdentity)
	if err != nil {
		t.Fatalf("build exact MySQL delete journal prefix: %v", err)
	}
	if _, err := database.ExecContext(ctx, createSQL); err != nil {
		t.Fatalf("create exact MySQL delete journal prefix: %v", err)
	}
}

func openMySQLDeleteReceiptCapabilityLive(
	t *testing.T,
	fixture mysqlDeleteJournalLiveFixture,
) (
	context.Context,
	*sql.DB,
	*relationalSourceAdapter,
	*mysqlTargetAdapter,
	schema.Table,
	schema.Table,
	deleteTargetBatch,
	func() targetAdapter,
) {
	t.Helper()
	ctx, database, namespace, _, openTarget, openSource := mysqlDeleteJournalLiveTarget(t, fixture)
	sourceName := "dmtx_delete_source_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	targetName := "dmtx_delete_target_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, database, sourceName, targetName, mysqlDeleteJournalTable)
	for _, relation := range []struct {
		name string
		rows string
	}{
		{sourceName, "(1, 'source')"},
		{targetName, "(1, 'source'), (2, 'target-only')"},
	} {
		if _, err := database.ExecContext(ctx,
			"CREATE TABLE "+mySQLIdentifier(relation.name)+" (id BIGINT NOT NULL, payload VARCHAR(32) NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE="+fixture.collation,
		); err != nil {
			t.Fatalf("create %s delete receipt fixture relation %s: %v", fixture.name, relation.name, err)
		}
		if _, err := database.ExecContext(ctx,
			"INSERT INTO "+mySQLIdentifier(relation.name)+" (id, payload) VALUES "+relation.rows,
		); err != nil {
			t.Fatalf("seed %s delete receipt fixture relation %s: %v", fixture.name, relation.name, err)
		}
	}
	sourceValue := openSource()
	source, ok := sourceValue.(*relationalSourceAdapter)
	if !ok {
		t.Fatalf("open %s delete receipt source = %T", fixture.name, sourceValue)
	}
	t.Cleanup(func() { _ = source.Close() })
	targetValue := openTarget()
	target, ok := targetValue.(*mysqlTargetAdapter)
	if !ok {
		t.Fatalf("open %s delete receipt target = %T", fixture.name, targetValue)
	}
	t.Cleanup(func() { _ = target.Close() })
	sourceTable, err := source.InspectTable(ctx, sourceName)
	if err != nil {
		t.Fatalf("inspect %s delete receipt source table: %v", fixture.name, err)
	}
	targetTable, err := engine.InspectMySQLTableForFlavor(
		ctx,
		target.database,
		target.flavor,
		namespace,
		targetName,
	)
	if err != nil {
		t.Fatalf("inspect %s delete receipt target table: %v", fixture.name, err)
	}
	if _, err := target.PrepareStage4DeleteJournalReadiness(ctx, Stage4DeleteJournalReadinessRequest{
		RunID:           "mysql-delete-receipt-" + fixture.name + "-" + targetName,
		InventoryDigest: strings.Repeat("e", 64),
	}); err != nil {
		t.Fatalf("prepare %s delete receipt journal: %v", fixture.name, err)
	}
	batch := deleteTargetBatch{
		Table: targetTable, Columns: []string{"id"},
		PlanID: strings.Repeat("a", 32), Token: strings.Repeat("b", 64),
		Sequence: 0, BatchDigest: strings.Repeat("c", 64),
		Keys: [][]driver.Value{{int64(2)}},
	}
	return ctx, database, source, target, sourceTable, targetTable, batch, openTarget
}

func testMySQLDeleteJournalEmptyPrefixRestartLive(
	t *testing.T,
	fixture mysqlDeleteJournalLiveFixture,
) {
	t.Helper()
	ctx, database, namespace, _, openTarget, openSource := mysqlDeleteJournalLiveTarget(t, fixture)
	identity, err := readMySQLDeleteEndpointIdentity(ctx, database, fixture.flavor)
	if err != nil {
		t.Fatalf("read %s delete target identity: %v", fixture.name, err)
	}
	targetIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil {
		t.Fatalf("canonicalize %s delete target identity: %v", fixture.name, err)
	}
	requestA := Stage4DeleteJournalReadinessRequest{
		RunID:           "mysql-delete-prefix-a-" + fixture.name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		InventoryDigest: strings.Repeat("a", 64),
	}
	requestB := Stage4DeleteJournalReadinessRequest{
		RunID:           "mysql-delete-prefix-b-" + fixture.name + "-" + strconv.FormatInt(time.Now().UnixNano(), 36),
		InventoryDigest: strings.Repeat("b", 64),
	}
	creatorA, err := mysqlDeleteJournalCreatorDigest(requestA, targetIdentity)
	if err != nil {
		t.Fatalf("construct %s run A journal creator: %v", fixture.name, err)
	}
	creatorB, err := mysqlDeleteJournalCreatorDigest(requestB, targetIdentity)
	if err != nil {
		t.Fatalf("construct %s run B journal creator: %v", fixture.name, err)
	}

	// A syntactically exact but different run's empty prefix is an
	// unauthenticated lookalike. It must never be adopted by a fresh run.
	createMySQLDeleteJournalPrefixLive(t, ctx, database, creatorB, strings.Repeat("1", 64))
	lookalike := openTarget()
	lookalikeTarget, ok := lookalike.(*mysqlTargetAdapter)
	if !ok {
		t.Fatalf("lookalike %s delete target = %T", fixture.name, lookalike)
	}
	if _, err := lookalikeTarget.PrepareStage4DeleteJournalReadiness(ctx, requestA); err == nil ||
		!strings.Contains(err.Error(), "prefix creator authority differs") {
		t.Fatalf("%s adopted unrelated exact empty journal prefix: %v", fixture.name, err)
	}
	catalog, err := inspectMySQLDeleteReceiptJournal(ctx, database, fixture.flavor, namespace)
	if err != nil || !catalog.Exists || !catalog.EmptyPrefix || catalog.CreatorDigest != creatorB {
		t.Fatalf("%s unrelated prefix refusal mutated authority: %#v, %v", fixture.name, catalog, err)
	}
	if err := lookalike.Close(); err != nil {
		t.Fatalf("close %s lookalike target: %v", fixture.name, err)
	}
	if _, err := database.ExecContext(ctx, "DROP TABLE "+mySQLIdentifier(mysqlDeleteJournalTable)); err != nil {
		t.Fatalf("drop %s unrelated empty journal prefix: %v", fixture.name, err)
	}
	createMySQLDeleteJournalPrefixLive(t, ctx, database, creatorA, strings.Repeat("2", 64))

	// Simulate process death immediately after CREATE TABLE commits: the first
	// opened adapter only proves read-only preflight can recognize this exact
	// zero-row prefix, then is closed without issuing its header INSERT.
	first := openTarget()
	firstTarget, ok := first.(*mysqlTargetAdapter)
	if !ok {
		t.Fatalf("first %s delete target = %T", fixture.name, first)
	}
	if err := firstTarget.PreflightStage4DeleteJournalReadiness(ctx); err != nil {
		t.Fatalf("preflight exact empty %s journal prefix: %v", fixture.name, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close crashed %s delete target: %v", fixture.name, err)
	}

	resumed := openTarget()
	t.Cleanup(func() { _ = resumed.Close() })
	resumedTarget, ok := resumed.(*mysqlTargetAdapter)
	if !ok {
		t.Fatalf("resumed %s delete target = %T", fixture.name, resumed)
	}
	if _, err := resumedTarget.PrepareStage4DeleteJournalReadiness(ctx, requestB); err == nil ||
		!strings.Contains(err.Error(), "prefix creator authority differs") {
		t.Fatalf("%s run B adopted run A empty journal prefix: %v", fixture.name, err)
	}
	catalog, err = inspectMySQLDeleteReceiptJournal(ctx, database, fixture.flavor, namespace)
	if err != nil || !catalog.Exists || !catalog.EmptyPrefix || catalog.CreatorDigest != creatorA {
		t.Fatalf("%s changed-run prefix refusal mutated authority: %#v, %v", fixture.name, catalog, err)
	}
	ready, err := resumedTarget.PrepareStage4DeleteJournalReadiness(ctx, requestA)
	if err != nil {
		t.Fatalf("resume exact empty %s journal prefix: %v", fixture.name, err)
	}
	if err := ready.Validate(); err != nil {
		t.Fatalf("resumed %s readiness is invalid: %v", fixture.name, err)
	}
	catalog, err = inspectMySQLDeleteReceiptJournal(ctx, database, fixture.flavor, namespace)
	if err != nil || !catalog.Exists || catalog.EmptyPrefix {
		t.Fatalf("resumed %s journal = %#v, %v; want authenticated authority", fixture.name, catalog, err)
	}
	readyB, err := resumedTarget.PrepareStage4DeleteJournalReadiness(ctx, requestB)
	if err != nil {
		t.Fatalf("reuse authenticated %s journal for run B: %v", fixture.name, err)
	}
	if err := readyB.Validate(); err != nil {
		t.Fatalf("run B %s readiness is invalid: %v", fixture.name, err)
	}
	if readyB.Equal(ready) || readyB.JournalCatalogDigest != ready.JournalCatalogDigest ||
		readyB.TargetIdentity != ready.TargetIdentity || readyB.TargetFlavorVersionDigest != ready.TargetFlavorVersionDigest {
		t.Fatalf("%s run B did not reuse the exact authenticated journal authority: runA=%#v runB=%#v", fixture.name, ready, readyB)
	}
	evolutionCatalog, err := resumedTarget.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read %s target evolution catalog with private journal: %v", fixture.name, err)
	}
	if len(evolutionCatalog.Tables()) != 0 {
		t.Fatalf("%s target evolution catalog exposed private journal: %#v", fixture.name, evolutionCatalog.Tables())
	}
	source := openSource()
	tables, listErr := source.ListTables(ctx)
	_, inspectErr := source.InspectTable(ctx, mysqlDeleteJournalTable)
	if closeErr := source.Close(); closeErr != nil {
		t.Fatalf("close %s source journal filter probe: %v", fixture.name, closeErr)
	}
	if listErr != nil {
		t.Fatalf("list %s source tables with private journal: %v", fixture.name, listErr)
	}
	for _, table := range tables {
		if table == mysqlDeleteJournalTable {
			t.Fatalf("%s source discovery exposed private journal", fixture.name)
		}
	}
	if inspectErr == nil || !strings.Contains(inspectErr.Error(), "private DMTX") {
		t.Fatalf("%s source InspectTable admitted private journal: %v", fixture.name, inspectErr)
	}

	// Altering the authenticated header is a target replacement/tampering
	// signal, not a new authority. A later run must fail closed before it can
	// reuse the journal.
	if _, err := database.ExecContext(ctx,
		"UPDATE "+mySQLIdentifier(mysqlDeleteJournalTable)+" SET `journal_identity` = ? WHERE `entry_kind` = ?",
		strings.Repeat("3", 64), mysqlDeleteJournalHeaderKind,
	); err != nil {
		t.Fatalf("alter %s journal header for refusal coverage: %v", fixture.name, err)
	}
	if _, err := resumedTarget.PrepareStage4DeleteJournalReadiness(ctx, requestB); err == nil ||
		!strings.Contains(err.Error(), "header") {
		t.Fatalf("%s run B accepted altered authenticated journal header: %v", fixture.name, err)
	}

	// Once state has persisted this authority, a replacement with the exact
	// DDL but no header is not a recoverable prefix. Prepare must leave it
	// empty rather than authenticating or recreating it.
	if _, err := database.ExecContext(ctx, "DROP TABLE "+mySQLIdentifier(mysqlDeleteJournalTable)); err != nil {
		t.Fatalf("replace %s journal for durable-receipt guard: %v", fixture.name, err)
	}
	createMySQLDeleteJournalPrefixLive(t, ctx, database, creatorA, strings.Repeat("4", 64))
	stored := mysqlDeleteJournalLiveReceipt(t, ready)
	requestWithExisting := requestA
	requestWithExisting.Existing = &stored
	if _, err := resumedTarget.PrepareStage4DeleteJournalReadiness(ctx, requestWithExisting); err == nil ||
		!strings.Contains(err.Error(), "lacks its immutable header") {
		t.Fatalf("durable %s readiness accepted empty replacement prefix: %v", fixture.name, err)
	}
	catalog, err = inspectMySQLDeleteReceiptJournal(ctx, database, fixture.flavor, namespace)
	if err != nil || !catalog.Exists || !catalog.EmptyPrefix {
		t.Fatalf("durable %s refusal mutated replacement journal: %#v, %v", fixture.name, catalog, err)
	}
	if _, err := resumedTarget.ReadTargetSchemaEvolutionCatalog(ctx); err == nil ||
		!strings.Contains(err.Error(), "lacks immutable authenticated authority") {
		t.Fatalf("%s target evolution accepted empty private-journal prefix: %v", fixture.name, err)
	}
	source = openSource()
	_, listErr = source.ListTables(ctx)
	if closeErr := source.Close(); closeErr != nil {
		t.Fatalf("close %s malformed source journal probe: %v", fixture.name, closeErr)
	}
	if listErr == nil || !strings.Contains(listErr.Error(), "complete authenticated authority") {
		t.Fatalf("%s source discovery hid empty private-journal prefix: %v", fixture.name, listErr)
	}
	if _, err := database.ExecContext(ctx, "DROP TABLE "+mySQLIdentifier(mysqlDeleteJournalTable)); err != nil {
		t.Fatalf("drop %s empty replacement prefix: %v", fixture.name, err)
	}
	if _, err := database.ExecContext(ctx,
		"CREATE TABLE "+mySQLIdentifier(mysqlDeleteJournalTable)+" (token BIGINT NOT NULL, PRIMARY KEY (token)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE="+fixture.collation,
	); err != nil {
		t.Fatalf("create malformed %s private-name collision: %v", fixture.name, err)
	}
	if err := resumedTarget.PreflightStage4DeleteJournalReadiness(ctx); err == nil ||
		!strings.Contains(err.Error(), "collides") {
		t.Fatalf("%s readiness preflight accepted malformed private-name collision: %v", fixture.name, err)
	}
	if _, err := resumedTarget.ReadTargetSchemaEvolutionCatalog(ctx); err == nil ||
		!strings.Contains(err.Error(), "collides") {
		t.Fatalf("%s target evolution accepted malformed private-name collision: %v", fixture.name, err)
	}
	source = openSource()
	_, listErr = source.ListTables(ctx)
	if closeErr := source.Close(); closeErr != nil {
		t.Fatalf("close %s colliding source journal probe: %v", fixture.name, closeErr)
	}
	if listErr == nil || !strings.Contains(listErr.Error(), "collides") {
		t.Fatalf("%s source discovery hid malformed private-name collision: %v", fixture.name, listErr)
	}
}

func mysqlDeleteJournalLiveReceipt(
	t *testing.T,
	ready state.Stage4DeleteJournalReadiness,
) state.Stage4DeleteJournalReadinessReceipt {
	t.Helper()
	encoded, err := json.Marshal(ready)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	receipt := state.Stage4DeleteJournalReadinessReceipt{
		Readiness: ready,
		Digest:    hex.EncodeToString(digest[:]),
	}
	if err := receipt.Validate(); err != nil {
		t.Fatalf("construct durable delete readiness receipt: %v", err)
	}
	return receipt
}

func testMySQLDeleteReadOnlySnapshotMetadataLockLive(
	t *testing.T,
	fixture mysqlDeleteJournalLiveFixture,
) {
	t.Helper()
	ctx, database, namespace, temporaryDSN, _, _ := mysqlDeleteJournalLiveTarget(t, fixture)
	table := "dmtx_delete_snapshot_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupMySQLNativeTables(t, database, table)
	if _, err := database.ExecContext(ctx,
		"CREATE TABLE "+mySQLIdentifier(table)+" (id BIGINT NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE="+fixture.collation,
	); err != nil {
		t.Fatalf("create %s snapshot metadata-lock table: %v", fixture.name, err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO "+mySQLIdentifier(table)+" (id) VALUES (1)"); err != nil {
		t.Fatalf("seed %s snapshot metadata-lock table: %v", fixture.name, err)
	}
	probeMySQLDeleteReadOnlyLockingReadLive(t, ctx, database, namespace, table, fixture.name)

	reader, err := database.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		t.Fatalf("set %s snapshot isolation: %v", fixture.name, err)
	}
	if _, err := reader.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		t.Fatalf("start %s read-only consistent snapshot: %v", fixture.name, err)
	}
	active := true
	defer func() {
		if active {
			_, _ = reader.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := acquireMySQLDeleteSnapshotMetadataLock(ctx, reader, schema.Table{Schema: namespace, Name: table}); err != nil {
		t.Fatalf("acquire %s read-only snapshot metadata lock: %v", fixture.name, err)
	}

	contenderDatabase := openMySQLNativeLiveDatabaseForFlavor(
		t,
		ctx,
		fixture.name+" snapshot metadata-lock DDL contender",
		temporaryDSN,
		fixture.refreshInfo,
	)
	contender, err := contenderDatabase.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	if _, err := contender.ExecContext(ctx, "SET SESSION lock_wait_timeout = 1"); err != nil {
		t.Fatalf("set %s DDL contender lock wait: %v", fixture.name, err)
	}
	_, alterErr := contender.ExecContext(ctx, "ALTER TABLE "+mySQLIdentifier(table)+" ADD COLUMN raced INT NULL")
	if alterErr == nil {
		t.Fatalf("%s DDL entered despite read-only snapshot metadata lock", fixture.name)
	}
	if _, err := reader.ExecContext(ctx, "ROLLBACK"); err != nil {
		t.Fatalf("roll back %s read-only snapshot: %v", fixture.name, err)
	}
	active = false
	if _, err := contender.ExecContext(ctx, "ALTER TABLE "+mySQLIdentifier(table)+" ADD COLUMN released INT NULL"); err != nil {
		t.Fatalf("%s metadata lock did not release after rollback (prior error %v): %v", fixture.name, alterErr, err)
	}
}

// probeMySQLDeleteReadOnlyLockingReadLive records the live behavior that led
// this capability to use the ordinary SELECT contract below. Some compatible
// MySQL-family sessions reject LOCK IN SHARE MODE after START ... READ ONLY;
// either outcome is observational evidence only, while the production path
// is required to pass the portable ordinary-read/MDL assertion above.
func probeMySQLDeleteReadOnlyLockingReadLive(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
	flavor string,
) {
	t.Helper()
	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire %s read-only locking-read probe: %v", flavor, err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		t.Fatalf("set %s locking-read probe isolation: %v", flavor, err)
	}
	if _, err := connection.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT, READ ONLY"); err != nil {
		t.Fatalf("start %s locking-read probe snapshot: %v", flavor, err)
	}
	rows, lockingReadErr := connection.QueryContext(
		ctx,
		"SELECT 1 FROM "+mySQLQualified(namespace, table)+" LIMIT 1 LOCK IN SHARE MODE",
	)
	if rows != nil {
		lockingReadErr = errors.Join(lockingReadErr, rows.Err(), rows.Close())
	}
	if _, rollbackErr := connection.ExecContext(ctx, "ROLLBACK"); rollbackErr != nil {
		t.Fatalf("roll back %s locking-read probe: %v", flavor, rollbackErr)
	}
	if lockingReadErr != nil {
		t.Logf("%s rejects/limits LOCK IN SHARE MODE in a read-only consistent snapshot: %v", flavor, lockingReadErr)
		return
	}
	t.Logf("%s accepted LOCK IN SHARE MODE in a read-only consistent snapshot; production uses the portable ordinary SELECT MDL contract", flavor)
}
