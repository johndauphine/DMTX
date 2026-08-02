package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func mysqlDeleteTestTable(
	namespace string,
	name string,
	collation string,
) schema.Table {
	return schema.Table{
		Schema: namespace, Name: name, MySQLCollation: collation,
		Columns: []schema.Column{
			{Name: "tenant_id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "external_id", Type: "varchar(64)", PrimaryKey: true, PrimaryKeyPosition: 2},
		},
	}
}

func mysqlDeleteTestAuthority(
	t *testing.T,
	flavor engine.MySQLServerFlavor,
	table schema.Table,
	canDelete bool,
) mysqlDeleteCatalogAuthority {
	t.Helper()
	primaryKey, err := deletePrimaryKeyColumns(table)
	if err != nil {
		t.Fatal(err)
	}
	authority := mysqlDeleteCatalogAuthority{
		Flavor: flavor, Namespace: table.Schema,
		Table: table, PrimaryKey: primaryKey,
		CanSelect: true, CanDelete: canDelete,
	}
	authority.CatalogDigest, err = mysqlDeleteCatalogAuthorityDigest(authority)
	if err != nil {
		t.Fatal(err)
	}
	return authority
}

func mysqlDeleteTestWorkloadIdentity(t *testing.T, namespace string) string {
	t.Helper()
	identity, err := config.NetworkEndpointWorkloadIdentity(config.Endpoint{
		Type: "mysql", Host: "mysql-delete-test", Port: 3306,
		Database: namespace, Schema: namespace,
	})
	if err != nil {
		t.Fatalf("build MySQL delete test workload identity: %v", err)
	}
	return identity
}

func TestMySQLDeleteCatalogShapeNormalizesOnlyEmptyObjectLists(t *testing.T) {
	expected := mysqlDeleteTestTable("target", "items", "utf8mb4_0900_bin")
	live := cloneStage4RichTable(expected)
	live.ClickHouseOrderBy = []string{}
	live.Indexes = []schema.Index{}
	live.ForeignKeys = []schema.ForeignKey{}
	live.Checks = []schema.CheckConstraint{}
	if !reflect.DeepEqual(
		normalizeMySQLDeleteTableShape(live),
		normalizeMySQLDeleteTableShape(expected),
	) {
		t.Fatal("allocated empty MySQL catalog object lists changed delete authority")
	}
	drifted := cloneStage4RichTable(live)
	drifted.Indexes = []schema.Index{{Name: "unexpected_index"}}
	if reflect.DeepEqual(
		normalizeMySQLDeleteTableShape(drifted),
		normalizeMySQLDeleteTableShape(expected),
	) {
		t.Fatal("nonempty MySQL catalog object-list drift was normalized away")
	}
	if !mysqlDeleteTableMatchesAuthority(live, expected) {
		t.Fatal("MySQL delete key authority rejected allocated empty catalog object lists")
	}
	if mysqlDeleteTableMatchesAuthority(drifted, expected) {
		t.Fatal("MySQL delete key authority accepted nonempty catalog object-list drift")
	}
	authority := mysqlDeleteTestAuthority(
		t,
		engine.MySQLServerFlavorOracle80,
		expected,
		true,
	)
	batch := deleteTargetBatch{
		Table: live, Columns: []string{"tenant_id", "external_id"},
	}
	if err := validateMySQLDeleteBatchAuthority(batch, authority); err != nil {
		t.Fatalf("MySQL delete batch authority rejected allocated empty catalog object lists: %v", err)
	}
	batch.Table = drifted
	if err := validateMySQLDeleteBatchAuthority(batch, authority); err == nil {
		t.Fatal("MySQL delete batch authority accepted nonempty catalog object-list drift")
	}
}

func TestMySQLDeleteCanonicalizerKeepsSameFlavorGateAtPairBoundary(t *testing.T) {
	source := mysqlDeleteTestTable("source", "items", "utf8mb4_0900_bin")
	target := mysqlDeleteTestTable("target", "items", "utf8mb4_0900_bin")
	sourceAuthority := mysqlDeleteTestAuthority(t, engine.MySQLServerFlavorOracle80, source, false)
	targetAuthority := mysqlDeleteTestAuthority(t, engine.MySQLServerFlavorOracle80, target, true)
	canonicalizer, err := newMySQLDeleteKeyCanonicalizer(source, target, sourceAuthority, targetAuthority)
	if err != nil {
		t.Fatal(err)
	}
	sourceKey, err := deletePrimaryKeyColumns(source)
	if err != nil {
		t.Fatal(err)
	}
	targetKey, err := deletePrimaryKeyColumns(target)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := canonicalizer.ProveDeleteKeyEquality(source, target, sourceKey, targetKey)
	if err != nil {
		t.Fatal(err)
	}
	for index, values := range []struct{ source, target any }{
		{int64(42), []byte("42")},
		{"item-42", []byte("item-42")},
	} {
		left, err := canonicalizer.CanonicalizeDeleteKeyValue(deleteKeySourceSide, proof, index, values.source)
		if err != nil {
			t.Fatal(err)
		}
		right, err := canonicalizer.CanonicalizeDeleteKeyValue(deleteKeyTargetSide, proof, index, values.target)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(left.Canonical, right.Canonical) || right.Parameter == nil {
			t.Fatalf("canonical key %d differs source=%#v target=%#v", index, left, right)
		}
	}
	mariaTarget := mysqlDeleteTestTable("target", "items", "utf8mb4_nopad_bin")
	mariaAuthority := mysqlDeleteTestAuthority(t, engine.MySQLServerFlavorMariaDB1011, mariaTarget, true)
	if _, err := newMySQLDeleteKeyCanonicalizer(source, mariaTarget, sourceAuthority, mariaAuthority); err == nil || !strings.Contains(err.Error(), "matching live server flavors") {
		t.Fatalf("cross-flavor key pair was admitted: %v", err)
	}
}

func TestMySQLDeleteNonTextCompositeKeyIgnoresTableCollation(t *testing.T) {
	source := schema.Table{
		Schema: "source", Name: "items", MySQLCollation: "utf8mb4_0900_ai_ci",
		Columns: []schema.Column{
			{Name: "tenant_id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "enabled", Type: "boolean", PrimaryKey: true, PrimaryKeyPosition: 2},
			{Name: "opaque_id", Type: "varbinary(16)", PrimaryKey: true, PrimaryKeyPosition: 3},
		},
	}
	target := source
	target.Schema = "target"
	target.MySQLCollation = "utf8mb4_0900_as_ci"
	sourceAuthority := mysqlDeleteTestAuthority(
		t,
		engine.MySQLServerFlavorOracle80,
		source,
		false,
	)
	targetAuthority := mysqlDeleteTestAuthority(
		t,
		engine.MySQLServerFlavorOracle80,
		target,
		true,
	)
	canonicalizer, err := newMySQLDeleteKeyCanonicalizer(
		source,
		target,
		sourceAuthority,
		targetAuthority,
	)
	if err != nil {
		t.Fatalf("ordinary-collation non-text composite key was refused: %v", err)
	}
	sourceKey, err := deletePrimaryKeyColumns(source)
	if err != nil {
		t.Fatal(err)
	}
	targetKey, err := deletePrimaryKeyColumns(target)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := canonicalizer.ProveDeleteKeyEquality(source, target, sourceKey, targetKey)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := []string{
		proof.Columns[0].Semantics,
		proof.Columns[1].Semantics,
		proof.Columns[2].Semantics,
	}, []string{"integer", "boolean", "binary"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("non-text composite proof semantics = %#v, want %#v", got, want)
	}
	for index, column := range proof.Columns {
		if column.CollationEvidence != "" {
			t.Fatalf("non-text composite proof column %d has unnecessary collation evidence %q", index, column.CollationEvidence)
		}
	}
	for index, values := range []struct{ source, target any }{
		{int64(42), []byte("42")},
		{true, int64(1)},
		{[]byte{0x00, 0xff, 0x04}, []byte{0x00, 0xff, 0x04}},
	} {
		left, err := canonicalizer.CanonicalizeDeleteKeyValue(deleteKeySourceSide, proof, index, values.source)
		if err != nil {
			t.Fatal(err)
		}
		right, err := canonicalizer.CanonicalizeDeleteKeyValue(deleteKeyTargetSide, proof, index, values.target)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(left.Canonical, right.Canonical) || right.Parameter == nil {
			t.Fatalf("non-text key %d differs source=%#v target=%#v", index, left, right)
		}
	}
}

func TestMySQLDeleteTextKeyStillRequiresMatchingBinaryCollation(t *testing.T) {
	source := mysqlDeleteTestTable("source", "items", "utf8mb4_0900_bin")
	target := mysqlDeleteTestTable("target", "items", "utf8mb4_0900_ai_ci")
	sourceAuthority := mysqlDeleteTestAuthority(t, engine.MySQLServerFlavorOracle80, source, false)
	targetAuthority := mysqlDeleteTestAuthority(t, engine.MySQLServerFlavorOracle80, target, true)
	if _, err := newMySQLDeleteKeyCanonicalizer(source, target, sourceAuthority, targetAuthority); err == nil ||
		!strings.Contains(err.Error(), "matching binary table collation") {
		t.Fatalf("text key with ordinary target collation was admitted: %v", err)
	}
	target.MySQLCollation = source.MySQLCollation
	targetAuthority = mysqlDeleteTestAuthority(t, engine.MySQLServerFlavorOracle80, target, true)
	if _, err := newMySQLDeleteKeyCanonicalizer(source, target, sourceAuthority, targetAuthority); err != nil {
		t.Fatalf("text key with matching binary table collation was refused: %v", err)
	}
}

func TestMySQLDeleteBatchCompositeParameterAndByteLimits(t *testing.T) {
	table := mysqlDeleteTestTable("target", "items", "utf8mb4_0900_bin")
	batch := deleteTargetBatch{
		Table: table, Columns: []string{"tenant_id", "external_id"},
		PlanID: strings.Repeat("a", 32), Token: strings.Repeat("b", 64),
		Sequence: 4, BatchDigest: strings.Repeat("c", 64),
		Keys: [][]driver.Value{{int64(1), "first"}, {int64(2), "second"}},
	}
	keys, err := validateMySQLDeleteBatchWithLimits("target", batch, 4, 128)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || len(keys[0]) != 2 {
		t.Fatalf("validated composite keys = %#v", keys)
	}
	statement, err := mysqlDeleteBatchStatement(table, batch.Columns, len(keys))
	if err != nil {
		t.Fatal(err)
	}
	want := "DELETE FROM `target`.`items` WHERE (`tenant_id`, `external_id`) IN ((?, ?), (?, ?))"
	if statement != want {
		t.Fatalf("statement = %q want %q", statement, want)
	}
	if _, err := validateMySQLDeleteBatchWithLimits("target", batch, 3, 128); err == nil || !strings.Contains(err.Error(), "parameter") {
		t.Fatalf("over-parameterized composite batch was admitted: %v", err)
	}
	batch.Keys = [][]driver.Value{{int64(1), strings.Repeat("x", 120)}}
	if _, err := validateMySQLDeleteBatchWithLimits("target", batch, 4, 128); err == nil || !strings.Contains(err.Error(), "byte") {
		t.Fatalf("over-byte-bound batch was admitted: %v", err)
	}
}

func TestMySQLDeleteJournalColumnTypeAcceptsOnlyMariaDBRenderedIntegerWidths(t *testing.T) {
	for _, test := range []struct {
		name   string
		flavor engine.MySQLServerFlavor
		input  string
		want   string
	}{
		{"mysql smallint remains exact", engine.MySQLServerFlavorOracle80, "smallint unsigned", "smallint unsigned"},
		{"mysql width remains mismatch", engine.MySQLServerFlavorOracle80, "smallint(5) unsigned", "smallint(5) unsigned"},
		{"mariadb smallint rendered width", engine.MySQLServerFlavorMariaDB1011, "smallint(5) unsigned", "smallint unsigned"},
		{"mariadb bigint rendered width", engine.MySQLServerFlavorMariaDB1011, "bigint(20) unsigned", "bigint unsigned"},
		{"mariadb wrong smallint width remains mismatch", engine.MySQLServerFlavorMariaDB1011, "smallint(6) unsigned", "smallint(6) unsigned"},
		{"mariadb signed bigint remains mismatch", engine.MySQLServerFlavorMariaDB1011, "bigint(20)", "bigint(20)"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := mysqlDeleteJournalColumnType(test.flavor, test.input); got != test.want {
				t.Fatalf("canonical journal column type = %q, want %q", got, test.want)
			}
		})
	}
}

func TestMySQLDeleteJournalAuthorityAndReceiptAreImmutable(t *testing.T) {
	identity := mysqlDeleteEndpointIdentity{
		flavor:         engine.MySQLServerFlavorOracle80,
		serverIdentity: "6B5E8731-583A-11EF-8C2A-0242AC120002",
		database:       "target", version: "8.0.46",
	}
	request := Stage4DeleteJournalReadinessRequest{
		RunID: "run-mysql-delete", InventoryDigest: strings.Repeat("a", 64),
	}
	workloadIdentity := mysqlDeleteTestWorkloadIdentity(t, identity.database)
	canonicalIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	creatorDigest, err := mysqlDeleteJournalCreatorDigest(request, canonicalIdentity)
	if err != nil {
		t.Fatal(err)
	}
	journalIdentity := strings.Repeat("d", 64)
	journalDigest, err := mysqlDeleteJournalCatalogDigest(identity.flavor, identity.database, creatorDigest, journalIdentity)
	if err != nil {
		t.Fatal(err)
	}
	journal := mysqlDeleteJournalCatalog{Exists: true, CreatorDigest: creatorDigest, JournalIdentity: journalIdentity, CatalogDigest: journalDigest}
	ready, err := mysqlDeleteReadinessFromCatalog(
		request,
		workloadIdentity,
		identity,
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := ready.Validate(); err != nil {
		t.Fatal(err)
	}
	if ready.TargetIdentity != workloadIdentity {
		t.Fatalf("readiness target identity = %q, want configured workload identity %q", ready.TargetIdentity, workloadIdentity)
	}
	changedJournal := journal
	changedJournal.JournalIdentity = strings.Repeat("e", 64)
	changedJournal.CatalogDigest, err = mysqlDeleteJournalCatalogDigest(identity.flavor, identity.database, changedJournal.CreatorDigest, changedJournal.JournalIdentity)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := mysqlDeleteReadinessFromCatalog(
		request,
		workloadIdentity,
		identity,
		changedJournal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if ready.Equal(changed) {
		t.Fatal("replaced MySQL journal reused readiness authority")
	}
	replacedTarget := identity
	replacedTarget.serverIdentity = "6B5E8731-583A-11EF-8C2A-0242AC120003"
	replaced, err := mysqlDeleteReadinessFromCatalog(
		request,
		workloadIdentity,
		replacedTarget,
		journal,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.JournalCatalogDigest == ready.JournalCatalogDigest {
		t.Fatal("replaced native MySQL target reused readiness catalog authority")
	}
	target := mysqlDeleteTestTable("target", "items", "utf8mb4_0900_bin")
	authority := mysqlDeleteTestAuthority(t, identity.flavor, target, true)
	batch := deleteTargetBatch{
		Table: target, Columns: []string{"tenant_id", "external_id"},
		PlanID: strings.Repeat("1", 32), Token: strings.Repeat("2", 64),
		Sequence: 3, BatchDigest: strings.Repeat("3", 64),
		Keys: [][]driver.Value{{int64(1), "only"}},
	}
	receipt := deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token, Sequence: batch.Sequence,
		BatchDigest: batch.BatchDigest, Candidates: 1, DeletedRows: 1,
	}
	receipt.ReceiptDigest, err = mysqlDeleteReceiptDigest(receipt, authority, mysqlDeleteIdentityDigest(canonicalIdentity), journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLDeleteReceipt(batch, receipt, authority, mysqlDeleteIdentityDigest(canonicalIdentity), journal); err != nil {
		t.Fatal(err)
	}
	receipt.DeletedRows = 0
	if err := validateMySQLDeleteReceipt(batch, receipt, authority, mysqlDeleteIdentityDigest(canonicalIdentity), journal); err == nil {
		t.Fatal("modified MySQL delete receipt was accepted")
	}
	if ready.ReadyAt.After(time.Now().UTC()) {
		t.Fatal("readiness timestamp is not canonical UTC current time")
	}
}

func TestMySQLDeleteJournalEmptyPrefixRecoveryPolicy(t *testing.T) {
	emptyPrefix := mysqlDeleteJournalCatalog{
		Exists:          true,
		EmptyPrefix:     true,
		CreatorDigest:   strings.Repeat("a", 64),
		JournalIdentity: strings.Repeat("b", 64),
	}
	for _, test := range []struct {
		name     string
		catalog  mysqlDeleteJournalCatalog
		existing bool
		want     mysqlDeleteJournalPrepareAction
		wantErr  string
	}{
		{
			name:    "fresh missing journal creates",
			catalog: mysqlDeleteJournalCatalog{},
			want:    mysqlDeleteJournalPrepareCreate,
		},
		{
			name:    "crash after create before header resumes prefix authentication",
			catalog: emptyPrefix,
			want:    mysqlDeleteJournalPrepareAuthenticatePrefix,
		},
		{
			name:    "restart before state receipt continues exact empty prefix",
			catalog: emptyPrefix,
			want:    mysqlDeleteJournalPrepareAuthenticatePrefix,
		},
		{
			name:     "durable receipt refuses absent journal",
			catalog:  mysqlDeleteJournalCatalog{},
			existing: true,
			wantErr:  "absent",
		},
		{
			name:     "durable receipt refuses headerless prefix",
			catalog:  emptyPrefix,
			existing: true,
			wantErr:  "lacks its immutable header",
		},
		{
			name: "authenticated journal is verification only",
			catalog: mysqlDeleteJournalCatalog{
				Exists:          true,
				JournalIdentity: strings.Repeat("d", 64),
				CatalogDigest:   strings.Repeat("e", 64),
			},
			existing: true,
			want:     mysqlDeleteJournalPrepareVerify,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := mysqlDeleteJournalPreparationAction(
				test.catalog,
				test.existing,
			)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("preparation action error = %v, want %q", err, test.wantErr)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("preparation action = %v, %v; want %v, nil", got, err, test.want)
			}
		})
	}

	identity := mysqlDeleteEndpointIdentity{
		flavor: engine.MySQLServerFlavorOracle80, serverIdentity: "server-id",
		database: "target", version: "8.0.46",
	}
	workloadIdentity := mysqlDeleteTestWorkloadIdentity(t, identity.database)
	_, err := mysqlDeleteReadinessFromCatalog(
		Stage4DeleteJournalReadinessRequest{
			RunID: "empty-prefix", InventoryDigest: strings.Repeat("a", 64),
		},
		workloadIdentity,
		identity,
		emptyPrefix,
	)
	if err == nil || !strings.Contains(err.Error(), "lacks immutable authenticated authority") {
		t.Fatalf("empty journal prefix produced readiness authority: %v", err)
	}
}

func TestMySQLDeleteJournalCreatorMarkerGuardsOnlyEmptyPrefix(t *testing.T) {
	identity := mysqlDeleteEndpointIdentity{
		flavor: engine.MySQLServerFlavorOracle80, serverIdentity: "server-id",
		database: "target", version: "8.0.46",
	}
	workloadIdentity := mysqlDeleteTestWorkloadIdentity(t, identity.database)
	targetIdentity, err := mysqlDeleteCanonicalTargetIdentity(identity)
	if err != nil {
		t.Fatal(err)
	}
	runA := Stage4DeleteJournalReadinessRequest{
		RunID: "journal-run-a", InventoryDigest: strings.Repeat("a", 64),
	}
	runB := Stage4DeleteJournalReadinessRequest{
		RunID: "journal-run-b", InventoryDigest: strings.Repeat("b", 64),
	}
	creatorA, err := mysqlDeleteJournalCreatorDigest(runA, targetIdentity)
	if err != nil {
		t.Fatal(err)
	}
	creatorB, err := mysqlDeleteJournalCreatorDigest(runB, targetIdentity)
	if err != nil {
		t.Fatal(err)
	}
	journalIdentity := strings.Repeat("c", 64)
	prefixA, err := mysqlDeleteJournalHeaderlessCatalog(0, creatorA, journalIdentity)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateMySQLDeleteJournalPrefixCreator(prefixA, creatorA); err != nil {
		t.Fatalf("run A could not recover its exact prefix: %v", err)
	}
	if err := validateMySQLDeleteJournalPrefixCreator(prefixA, creatorB); err == nil ||
		!strings.Contains(err.Error(), "prefix creator authority differs") {
		t.Fatalf("run B adopted run A empty prefix: %v", err)
	}

	catalogDigest, err := mysqlDeleteJournalCatalogDigest(
		identity.flavor,
		identity.database,
		creatorA,
		journalIdentity,
	)
	if err != nil {
		t.Fatal(err)
	}
	authenticated := mysqlDeleteJournalCatalog{
		Exists:          true,
		CreatorDigest:   creatorA,
		JournalIdentity: journalIdentity,
		CatalogDigest:   catalogDigest,
	}
	readyA, err := mysqlDeleteReadinessFromCatalog(
		runA,
		workloadIdentity,
		identity,
		authenticated,
	)
	if err != nil {
		t.Fatalf("construct run A readiness from authenticated journal: %v", err)
	}
	readyB, err := mysqlDeleteReadinessFromCatalog(
		runB,
		workloadIdentity,
		identity,
		authenticated,
	)
	if err != nil {
		t.Fatalf("run B could not reuse authenticated journal: %v", err)
	}
	if readyA.Equal(readyB) || readyA.JournalCatalogDigest != readyB.JournalCatalogDigest ||
		readyA.TargetIdentity != readyB.TargetIdentity {
		t.Fatalf("authenticated journal was not reused as exact native authority: runA=%#v runB=%#v", readyA, readyB)
	}

	headerDigest, err := mysqlDeleteJournalHeaderDigest(creatorA, journalIdentity)
	if err != nil {
		t.Fatal(err)
	}
	header := mysqlDeleteJournalHeader{
		JournalVersion:       mysqlDeleteJournalVersion,
		EntryKind:            mysqlDeleteJournalHeaderKind,
		Token:                mysqlDeleteJournalHeaderToken,
		PlanID:               mysqlDeleteJournalZeroPlanID,
		BatchDigest:          mysqlDeleteJournalHeaderToken,
		TargetCatalogDigest:  mysqlDeleteJournalHeaderToken,
		TargetIdentityDigest: mysqlDeleteJournalHeaderToken,
		JournalIdentity:      journalIdentity,
		ReceiptDigest:        headerDigest,
	}
	if err := validateMySQLDeleteJournalHeader(header, creatorA, journalIdentity); err != nil {
		t.Fatalf("exact authenticated header was refused: %v", err)
	}
	header.JournalIdentity = strings.Repeat("d", 64)
	if err := validateMySQLDeleteJournalHeader(header, creatorA, journalIdentity); err == nil {
		t.Fatal("altered authenticated journal header was accepted")
	}
}

func TestMySQLDeleteJournalHeaderlessShapeRequiresZeroRows(t *testing.T) {
	creatorDigest := strings.Repeat("a", 64)
	journalIdentity := strings.Repeat("b", 64)
	catalog, err := mysqlDeleteJournalHeaderlessCatalog(0, creatorDigest, journalIdentity)
	if err != nil || !catalog.Exists || !catalog.EmptyPrefix {
		t.Fatalf("zero-row exact journal prefix = %#v, %v; want recoverable prefix", catalog, err)
	}
	for _, test := range []struct {
		count int
		want  string
	}{
		{-1, "row count"},
		{1, "immutable header"},
		{2, "immutable header"},
	} {
		if _, err := mysqlDeleteJournalHeaderlessCatalog(test.count, creatorDigest, journalIdentity); err == nil ||
			!strings.Contains(err.Error(), test.want) {
			t.Fatalf("headerless journal with %d rows was admitted: %v", test.count, err)
		}
	}
}

func TestMySQLSourceOmitsOnlyExactPrivateJournalName(t *testing.T) {
	adapter := &relationalSourceAdapter{spec: relationalSourceSpec{
		engine: "mysql",
		listTables: func(context.Context, *sql.DB, string) ([]string, error) {
			return []string{"items", mysqlDeleteJournalTable, strings.ToUpper(mysqlDeleteJournalTable)}, nil
		},
		inspectTable: func(context.Context, *sql.DB, string, string) (schema.Table, error) {
			return schema.Table{Name: "ordinary"}, nil
		},
	}}
	tables, err := adapter.ListTables(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tables, []string{"items", strings.ToUpper(mysqlDeleteJournalTable)}) {
		t.Fatalf("MySQL source private-table filtering = %#v", tables)
	}
	if _, err := adapter.InspectTable(t.Context(), mysqlDeleteJournalTable); err == nil || !strings.Contains(err.Error(), "private DMTX") {
		t.Fatalf("MySQL private journal inspect error = %v", err)
	}
}
