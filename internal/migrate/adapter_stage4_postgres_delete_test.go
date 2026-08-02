package migrate

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresDeletePrimaryKeyQueryIsExactAndQuoted(t *testing.T) {
	query := postgresDeletePrimaryKeyQuery(
		`source"schema`,
		`order`,
		[]string{`tenant"id`, "item_id"},
	)
	want := `SELECT "tenant""id", "item_id" FROM ` +
		`"source""schema"."order" ORDER BY "tenant""id", "item_id"`
	if query != want {
		t.Fatalf("query = %q want %q", query, want)
	}
}

func TestPostgresDeleteKeyPairFailsClosed(t *testing.T) {
	valid := []schema.Column{
		{
			Name: "tenant_id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		},
		{
			Name: "item_id", Type: "uuid",
			PrimaryKey: true, PrimaryKeyPosition: 2,
		},
	}
	if err := validatePostgresDeleteKeyPair(valid, valid); err != nil {
		t.Fatalf("valid key pair: %v", err)
	}
	tests := map[string]func([]schema.Column){
		"target type": func(columns []schema.Column) {
			columns[1].Type = "text"
		},
		"target order": func(columns []schema.Column) {
			columns[0].PrimaryKeyPosition = 2
			columns[1].PrimaryKeyPosition = 1
		},
		"nullable": func(columns []schema.Column) {
			columns[0].Nullable = true
		},
		"fixed text": func(columns []schema.Column) {
			columns[0].Type = "character(12)"
		},
		"dynamic": func(columns []schema.Column) {
			columns[0].Type = "any"
		},
		"json equality": func(columns []schema.Column) {
			columns[0].Type = "jsonb"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			target := append([]schema.Column(nil), valid...)
			mutate(target)
			if err := validatePostgresDeleteKeyPair(
				valid,
				target,
			); err == nil {
				t.Fatal("expected fail-closed key-pair error")
			}
		})
	}
}

func TestPostgresDeleteCanonicalizerPreservesExactValues(t *testing.T) {
	source := postgresDeleteTestTable("source", []schema.Column{
		{
			Name: "tenant_id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		},
		{
			Name: "entity_id", Type: "uuid",
			PrimaryKey: true, PrimaryKeyPosition: 2,
		},
	})
	target := source
	target.Schema = "target"
	target.Columns = append([]schema.Column(nil), source.Columns...)
	sourceKey, err := deletePrimaryKeyColumns(source)
	if err != nil {
		t.Fatal(err)
	}
	targetKey, err := deletePrimaryKeyColumns(target)
	if err != nil {
		t.Fatal(err)
	}
	sourceAuthority := postgresDeleteCatalogAuthority{
		ServerAddress:  "127.0.0.1",
		CurrentUser:    "test-user",
		DatabaseOID:    1,
		SchemaOID:      2,
		RelationOID:    3,
		ConstraintOID:  4,
		IndexOID:       5,
		Database:       "test",
		Schema:         source.Schema,
		Table:          source.Name,
		Constraint:     "items_pkey",
		TableShape:     source,
		PrimaryKey:     sourceKey,
		ServerEncoding: "UTF8",
		ServerVersion:  160000,
		IndexKeys: []postgresDeleteIndexKeyAuthority{
			{
				Position: 1, Column: "tenant_id",
				OperatorClassNamespace: "pg_catalog",
				OperatorClass:          "int8_ops",
			},
			{
				Position: 2, Column: "entity_id",
				OperatorClassNamespace: "pg_catalog",
				OperatorClass:          "uuid_ops",
			},
		},
	}
	sourceAuthority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(sourceAuthority)
	if err != nil {
		t.Fatal(err)
	}
	targetAuthority := sourceAuthority
	targetAuthority.SchemaOID = 6
	targetAuthority.RelationOID = 7
	targetAuthority.ConstraintOID = 8
	targetAuthority.IndexOID = 9
	targetAuthority.Schema = target.Schema
	targetAuthority.TableShape = target
	targetAuthority.PrimaryKey = targetKey
	targetAuthority.IndexKeys = append(
		[]postgresDeleteIndexKeyAuthority(nil),
		sourceAuthority.IndexKeys...,
	)
	targetAuthority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(targetAuthority)
	if err != nil {
		t.Fatal(err)
	}
	canonicalizer, err := newPostgresDeleteKeyCanonicalizer(
		source,
		target,
		sourceAuthority,
		targetAuthority,
	)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := canonicalizer.ProveDeleteKeyEquality(
		source,
		target,
		sourceKey,
		targetKey,
	)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		index  int
		source any
		target any
		want   string
	}{
		{index: 0, source: int64(42), target: []byte("42"), want: "42"},
		{
			index:  1,
			source: "550E8400-E29B-41D4-A716-446655440000",
			target: []byte("550e8400e29b41d4a716446655440000"),
			want:   "550e8400e29b41d4a716446655440000",
		},
	}
	for _, test := range tests {
		fromSource, err := canonicalizer.CanonicalizeDeleteKeyValue(
			deleteKeySourceSide,
			proof,
			test.index,
			test.source,
		)
		if err != nil {
			t.Fatal(err)
		}
		fromTarget, err := canonicalizer.CanonicalizeDeleteKeyValue(
			deleteKeyTargetSide,
			proof,
			test.index,
			test.target,
		)
		if err != nil {
			t.Fatal(err)
		}
		if string(fromSource.Canonical) != test.want ||
			!reflect.DeepEqual(
				fromSource.Canonical,
				fromTarget.Canonical,
			) ||
			fromTarget.Parameter == nil {
			t.Fatalf(
				"canonical values source=%#v target=%#v",
				fromSource,
				fromTarget,
			)
		}
	}
	changed := source
	changed.Columns = append([]schema.Column(nil), source.Columns...)
	changed.Columns[0].Type = "integer"
	if _, err := canonicalizer.ProveDeleteKeyEquality(
		changed,
		target,
		sourceKey,
		targetKey,
	); err == nil {
		t.Fatal("metadata drift reused PostgreSQL delete proof")
	}
	recreatedTargetAuthority := targetAuthority
	recreatedTargetAuthority.RelationOID++
	recreatedTargetAuthority.ConstraintOID++
	recreatedTargetAuthority.IndexOID++
	recreatedTargetAuthority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(recreatedTargetAuthority)
	if err != nil {
		t.Fatal(err)
	}
	recreatedCanonicalizer, err :=
		newPostgresDeleteKeyCanonicalizer(
			source,
			target,
			sourceAuthority,
			recreatedTargetAuthority,
		)
	if err != nil {
		t.Fatal(err)
	}
	recreatedProof, err :=
		recreatedCanonicalizer.ProveDeleteKeyEquality(
			source,
			target,
			sourceKey,
			targetKey,
		)
	if err != nil {
		t.Fatal(err)
	}
	if recreatedProof.CanonicalizerID == proof.CanonicalizerID {
		t.Fatal(
			"identical-shape target relation recreation reused delete authority proof",
		)
	}
	tamperedAuthority := targetAuthority
	tamperedAuthority.RelationOID++
	if _, err := newPostgresDeleteKeyCanonicalizer(
		source,
		target,
		sourceAuthority,
		tamperedAuthority,
	); err == nil {
		t.Fatal("tampered target catalog digest was accepted")
	}
}

func TestPostgresDeleteAuthorityUsesStableSchemaEvidence(t *testing.T) {
	frontier := int64(41)
	table := postgresDeleteTestTable("target", []schema.Column{
		{
			Name: "id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		},
		{Name: "status", Type: "text"},
	})
	table.Identity = &schema.Identity{
		Column:     "id",
		Generation: schema.IdentityByDefault,
		Frontier:   &frontier,
	}
	table.Columns[1].Default = mustPostgresDeleteCatalogDefault(
		t,
		table.Columns[1],
		`'active'::text`,
	)
	table.Checks = []schema.CheckConstraint{{
		Name: "items_status_check",
		Expression: mustPostgresDeleteCatalogCheck(
			t,
			`(status COLLATE "C") = 'active'::text`,
			table.Columns,
		),
	}}
	authority := postgresDeleteCatalogAuthority{
		ServerAddress:    "127.0.0.1",
		SystemIdentifier: "7668098435510087718",
		CurrentUser:      "test-user",
		DatabaseOID:      1,
		SchemaOID:        2,
		RelationOID:      3,
		ConstraintOID:    4,
		IndexOID:         5,
		Database:         "test",
		Schema:           table.Schema,
		Table:            table.Name,
		Constraint:       "items_pkey",
		TableShape:       table,
		PrimaryKey:       []schema.Column{table.Columns[0]},
		ServerEncoding:   "UTF8",
		ServerVersion:    160000,
	}
	baseDigest, err := postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		t.Fatal(err)
	}
	aliasAuthority := authority
	aliasAuthority.ServerAddress = "127.0.0.2"
	aliasAuthority.ServerPort = 6543
	aliasDigest, err := postgresDeleteAuthorityDigestValue(aliasAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if aliasDigest != baseDigest {
		t.Fatal("connection alias changed physical delete catalog authority")
	}

	frontierChanged := clonePostgresDeleteAuthorityTestTable(table)
	nextFrontier := int64(99)
	frontierChanged.Identity.Frontier = &nextFrontier
	authority.TableShape = frontierChanged
	frontierDigest, err := postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		t.Fatal(err)
	}
	if frontierDigest != baseDigest {
		t.Fatal("mutable identity frontier changed delete catalog authority")
	}
	stable, err := samePostgresDeleteStableTableShape(
		table,
		frontierChanged,
	)
	if err != nil || !stable {
		t.Fatalf(
			"identity-frontier-only shape comparison = %v, error = %v",
			stable,
			err,
		)
	}

	defaultChanged := clonePostgresDeleteAuthorityTestTable(table)
	defaultChanged.Columns[1].Default =
		mustPostgresDeleteCatalogDefault(
			t,
			defaultChanged.Columns[1],
			`'paused'::text`,
		)
	authority.TableShape = defaultChanged
	defaultDigest, err := postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		t.Fatal(err)
	}
	if defaultDigest == baseDigest {
		t.Fatal("PostgreSQL default drift reused delete catalog authority")
	}

	checkChanged := clonePostgresDeleteAuthorityTestTable(table)
	checkChanged.Checks[0].Expression =
		mustPostgresDeleteCatalogCheck(
			t,
			`(status COLLATE "C") = 'paused'::text`,
			checkChanged.Columns,
		)
	authority.TableShape = checkChanged
	checkDigest, err := postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		t.Fatal(err)
	}
	if checkDigest == baseDigest {
		t.Fatal("PostgreSQL CHECK drift reused delete catalog authority")
	}
	stable, err = samePostgresDeleteStableTableShape(table, checkChanged)
	if err != nil || stable {
		t.Fatalf(
			"CHECK-drift shape comparison = %v, error = %v",
			stable,
			err,
		)
	}
}

func TestPostgresDeleteSameRelationUsesClusterIdentity(t *testing.T) {
	source := postgresDeleteCatalogAuthority{
		ServerAddress:    "127.0.0.1",
		ServerPort:       5432,
		SystemIdentifier: "7668098435510087718",
		DatabaseOID:      16384,
		RelationOID:      32768,
	}
	alias := source
	alias.ServerAddress = "192.0.2.10"
	alias.ServerPort = 6432
	if !samePostgresDeleteRelation(source, alias) {
		t.Fatal("PostgreSQL listen-address alias hid an identical relation")
	}
	otherCluster := alias
	otherCluster.SystemIdentifier = "8668098435510087718"
	if samePostgresDeleteRelation(source, otherCluster) {
		t.Fatal("different PostgreSQL clusters shared relation authority")
	}
	otherDatabase := alias
	otherDatabase.DatabaseOID++
	if samePostgresDeleteRelation(source, otherDatabase) {
		t.Fatal("different PostgreSQL databases shared relation authority")
	}
	otherRelation := alias
	otherRelation.RelationOID++
	if samePostgresDeleteRelation(source, otherRelation) {
		t.Fatal("different PostgreSQL relations shared relation authority")
	}
}

func TestPostgresDeleteTextProofRequiresExactIndexAuthority(t *testing.T) {
	column := schema.Column{
		Name: "external_id", Type: "text",
		PrimaryKey: true, PrimaryKeyPosition: 1,
	}
	source := postgresDeleteTestTable(
		"source",
		[]schema.Column{column},
	)
	target := source
	target.Schema = "target"
	target.Columns = append([]schema.Column(nil), source.Columns...)
	sourceAuthority := postgresDeleteCatalogAuthority{
		ServerAddress: "127.0.0.1",
		CurrentUser:   "test-user",
		DatabaseOID:   1,
		SchemaOID:     2,
		RelationOID:   3,
		ConstraintOID: 4,
		IndexOID:      5,
		Database:      "test",
		Schema:        source.Schema,
		Table:         source.Name,
		Constraint:    "items_pkey",
		TableShape:    source,
		PrimaryKey:    append([]schema.Column(nil), source.Columns...),
		IndexKeys: []postgresDeleteIndexKeyAuthority{{
			Position:               1,
			Column:                 column.Name,
			OperatorClassNamespace: "pg_catalog",
			OperatorClass:          "text_ops",
			CollationOID:           100,
			CollationNamespace:     "pg_catalog",
			Collation:              "default",
			CollationProvider:      "d",
			CollationDeterministic: true,
		}},
		ServerEncoding: "UTF8",
		ServerVersion:  160000,
	}
	var err error
	sourceAuthority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(sourceAuthority)
	if err != nil {
		t.Fatal(err)
	}
	targetAuthority := sourceAuthority
	targetAuthority.SchemaOID++
	targetAuthority.RelationOID++
	targetAuthority.ConstraintOID++
	targetAuthority.IndexOID++
	targetAuthority.Schema = target.Schema
	targetAuthority.TableShape = target
	targetAuthority.PrimaryKey = append(
		[]schema.Column(nil),
		target.Columns...,
	)
	targetAuthority.IndexKeys = append(
		[]postgresDeleteIndexKeyAuthority(nil),
		sourceAuthority.IndexKeys...,
	)
	targetAuthority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(targetAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newPostgresDeleteKeyCanonicalizer(
		source, target,
		sourceAuthority, targetAuthority,
	); err != nil {
		t.Fatalf("exact deterministic text authority: %v", err)
	}
	unsafe := targetAuthority
	unsafe.IndexKeys = append(
		[]postgresDeleteIndexKeyAuthority(nil),
		targetAuthority.IndexKeys...,
	)
	unsafe.IndexKeys[0].CollationDeterministic = false
	unsafe.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(unsafe)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newPostgresDeleteKeyCanonicalizer(
		source, target,
		sourceAuthority, unsafe,
	); err == nil {
		t.Fatal("nondeterministic target text collation was accepted")
	}
	unsafe = targetAuthority
	unsafe.IndexKeys = append(
		[]postgresDeleteIndexKeyAuthority(nil),
		targetAuthority.IndexKeys...,
	)
	unsafe.IndexKeys[0].OperatorClass = "varchar_ops"
	unsafe.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(unsafe)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newPostgresDeleteKeyCanonicalizer(
		source, target,
		sourceAuthority, unsafe,
	); err == nil {
		t.Fatal("mismatched target text operator class was accepted")
	}
}

func TestPostgresDeleteBatchPlanningIsBounded(t *testing.T) {
	table := postgresDeleteTestTable("target", []schema.Column{
		{
			Name: "tenant_id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		},
		{
			Name: "item_id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 2,
		},
	})
	batch := deleteTargetBatch{
		Table:       table,
		Columns:     []string{"tenant_id", "item_id"},
		PlanID:      "00112233445566778899aabbccddeeff",
		Token:       strings.Repeat("a", 64),
		Sequence:    3,
		BatchDigest: strings.Repeat("b", 64),
		Keys: [][]driver.Value{
			{int64(1), int64(2)},
			{int64(3), int64(4)},
		},
	}
	keys, err := validatePostgresDeleteBatch("target", batch)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := postgresDeleteBatchStatement(
		table,
		batch.Columns,
		len(keys),
	)
	if err != nil {
		t.Fatal(err)
	}
	want := `DELETE FROM "target"."items" ` +
		`WHERE ("tenant_id", "item_id") IN (($1, $2), ($3, $4))`
	if statement != want {
		t.Fatalf("statement = %q want %q", statement, want)
	}
	arguments := flattenPostgresDeleteArguments(keys)
	if !reflect.DeepEqual(arguments, []any{
		int64(1), int64(2), int64(3), int64(4),
	}) {
		t.Fatalf("arguments = %#v", arguments)
	}
	if _, err := validatePostgresDeleteBatchWithLimits(
		"target",
		batch,
		3,
		postgresDeleteMaximumBatchBytes,
	); err == nil || !strings.Contains(err.Error(), "parameter") {
		t.Fatalf("parameter ceiling error = %v", err)
	}
	byteLimited := batch
	byteLimited.Keys = [][]driver.Value{{
		int64(1),
		strings.Repeat("x", 9),
	}}
	if _, err := validatePostgresDeleteBatchWithLimits(
		"target",
		byteLimited,
		postgresDeleteMaximumParameters,
		8,
	); err == nil || !strings.Contains(err.Error(), "byte") {
		t.Fatalf("byte ceiling error = %v", err)
	}
	tests := map[string]func(*deleteTargetBatch){
		"namespace": func(value *deleteTargetBatch) {
			value.Table.Schema = "other"
		},
		"column width": func(value *deleteTargetBatch) {
			value.Keys[0] = []driver.Value{int64(1)}
		},
		"nil key": func(value *deleteTargetBatch) {
			value.Keys[0][0] = nil
		},
		"token": func(value *deleteTargetBatch) {
			value.Token = "not-a-token"
		},
		"digest": func(value *deleteTargetBatch) {
			value.BatchDigest = strings.Repeat("A", 64)
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := batch
			value.Columns = append([]string(nil), batch.Columns...)
			value.Keys = make([][]driver.Value, len(batch.Keys))
			for index := range batch.Keys {
				value.Keys[index] = append(
					[]driver.Value(nil),
					batch.Keys[index]...,
				)
			}
			mutate(&value)
			if _, err := validatePostgresDeleteBatch(
				"target",
				value,
			); err == nil {
				t.Fatal("expected unsafe batch rejection")
			}
		})
	}
}

func TestPostgresDeleteReceiptIsImmutable(t *testing.T) {
	batch := deleteTargetBatch{
		Table:    schema.Table{Schema: "target", Name: "items"},
		Columns:  []string{"id"},
		PlanID:   "00112233445566778899aabbccddeeff",
		Token:    strings.Repeat("a", 64),
		Sequence: 2, BatchDigest: strings.Repeat("b", 64),
		Keys: [][]driver.Value{{int64(1)}, {int64(2)}},
	}
	receipt := deleteTargetBatchReceipt{
		PlanID: batch.PlanID, Token: batch.Token,
		Sequence: batch.Sequence, BatchDigest: batch.BatchDigest,
		Candidates: 2, DeletedRows: 2,
	}
	authority := postgresDeleteCatalogAuthority{
		ServerAddress:  "127.0.0.1",
		CurrentUser:    "test-user",
		DatabaseOID:    1,
		SchemaOID:      2,
		RelationOID:    3,
		Database:       "test",
		Schema:         batch.Table.Schema,
		Table:          batch.Table.Name,
		TableShape:     batch.Table,
		ServerEncoding: "UTF8",
		ServerVersion:  160000,
	}
	var err error
	authority.CatalogDigest, err =
		postgresDeleteAuthorityDigestValue(authority)
	if err != nil {
		t.Fatal(err)
	}
	receipt.ReceiptDigest, err = postgresDeleteReceiptDigest(
		receipt,
		authority,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePostgresDeleteReceipt(
		batch,
		receipt,
		authority,
	); err != nil {
		t.Fatal(err)
	}
	changed := receipt
	changed.DeletedRows = 1
	if err := validatePostgresDeleteReceipt(
		batch,
		changed,
		authority,
	); err == nil {
		t.Fatal("tampered receipt was accepted")
	}
	changed = receipt
	changed.FailClosedReason = "target_mutation_failed"
	if err := validatePostgresDeleteReceipt(
		batch,
		changed,
		authority,
	); err == nil {
		t.Fatal("failure evidence masqueraded as a replayable receipt")
	}
	changed = receipt
	changed.ReceiptDigest = strings.Repeat("c", 64)
	if err := validatePostgresDeleteReceipt(
		batch,
		changed,
		authority,
	); err == nil {
		t.Fatal("shape-valid but recomputation-invalid receipt was accepted")
	}
	if _, err := postgresDeleteAdvisoryLockKey(
		"not-a-token",
	); err == nil {
		t.Fatal("invalid advisory-lock token was accepted")
	}
}

func TestPostgresDeleteCapabilitiesRejectUnsupportedAdapters(t *testing.T) {
	_, err := newPostgresDeleteReconciliationCapabilities(
		t.Context(),
		nil,
		nil,
		schema.Table{},
		schema.Table{},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "PostgreSQL relational source") {
		t.Fatalf("unsupported capability error = %v", err)
	}
	var typed *postgresDeleteTargetCapability
	if _, err := typed.ApplyDeleteBatch(
		t.Context(),
		deleteTargetBatch{},
	); err == nil {
		t.Fatal("nil PostgreSQL target accepted delete batch")
	}
	if (*postgresDeleteTargetCapability)(nil).MaxDeleteParameters() !=
		postgresDeleteMaximumParameters {
		t.Fatal("PostgreSQL delete parameter ceiling differs")
	}
}

func TestPostgresDeleteRecoveryErrorsAreActionable(t *testing.T) {
	errorsToCheck := []error{
		fmtPostgresDeleteTestError(
			"PostgreSQL delete batch failed atomically before receipt publication; repair the target error and resume this existing run and attempt",
		),
		fmtPostgresDeleteTestError(
			"PostgreSQL delete commit outcome is unknown; resume this existing run and attempt with the same batch token",
		),
	}
	for _, err := range errorsToCheck {
		if !strings.Contains(err.Error(), "resume this existing run and attempt") {
			t.Fatalf("recovery wording = %v", err)
		}
	}
	if !errors.Is(errors.Join(errorsToCheck...), errorsToCheck[0]) {
		t.Fatal("test error identity was lost")
	}
}

func fmtPostgresDeleteTestError(message string) error {
	return errors.New(message)
}

func postgresDeleteTestTable(
	namespace string,
	columns []schema.Column,
) schema.Table {
	return schema.Table{
		Schema:  namespace,
		Name:    "items",
		Columns: append([]schema.Column(nil), columns...),
	}
}

func clonePostgresDeleteAuthorityTestTable(
	table schema.Table,
) schema.Table {
	cloned := table
	cloned.Columns = append([]schema.Column(nil), table.Columns...)
	cloned.Checks = append(
		[]schema.CheckConstraint(nil),
		table.Checks...,
	)
	if table.Identity != nil {
		identity := *table.Identity
		if table.Identity.Frontier != nil {
			frontier := *table.Identity.Frontier
			identity.Frontier = &frontier
		}
		cloned.Identity = &identity
	}
	return cloned
}

func mustPostgresDeleteCatalogDefault(
	t *testing.T,
	column schema.Column,
	catalog string,
) *schema.Expression {
	t.Helper()
	expression, err := schema.ParsePostgresCatalogDefault(
		column,
		&catalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	if expression == nil {
		t.Fatal("PostgreSQL catalog default is absent")
	}
	return expression
}

func mustPostgresDeleteCatalogCheck(
	t *testing.T,
	catalog string,
	columns []schema.Column,
) schema.Expression {
	t.Helper()
	expression, err := schema.ParsePostgresCatalogCheck(
		catalog,
		columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	return expression
}
