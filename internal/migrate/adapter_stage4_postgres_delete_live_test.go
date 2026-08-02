package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestPostgresDeleteReconciliationAdapterLiveTLS(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL delete-reconciliation adapter sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL delete DSN: %T", err)
	}
	if !postgresDeleteLiveRequiresVerifiedTLS(parsed) {
		t.Fatal(
			"DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL server certificate and hostname",
		)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL delete fixture: %T", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL delete fixture: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL delete fixture: %T", err)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	sourceNamespace := "dmtx_delete_source_" + suffix
	targetNamespace := "dmtx_delete_target_" + suffix
	tableName := "items"
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(sourceNamespace)+
			"; CREATE SCHEMA "+postgresIdentifier(targetNamespace),
	); err != nil {
		t.Fatalf("create PostgreSQL delete schemas: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			15*time.Second,
		)
		defer cleanupCancel()
		for _, namespace := range []string{
			sourceNamespace,
			targetNamespace,
		} {
			if _, err := database.ExecContext(
				cleanupCtx,
				"DROP SCHEMA IF EXISTS "+
					postgresIdentifier(namespace)+
					" CASCADE",
			); err != nil {
				t.Errorf(
					"drop PostgreSQL delete schema %s: %v",
					namespace,
					err,
				)
			}
		}
	})
	sourceQualified := postgresQualified(sourceNamespace, tableName)
	targetQualified := postgresQualified(targetNamespace, tableName)
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE `+sourceQualified+` (
			tenant_id bigint NOT NULL,
			external_id text NOT NULL,
			payload text NOT NULL,
			PRIMARY KEY (tenant_id, external_id)
		);
		CREATE TABLE `+targetQualified+` (
			tenant_id bigint NOT NULL,
			external_id text NOT NULL,
			payload text NOT NULL,
			PRIMARY KEY (tenant_id, external_id)
		);
		INSERT INTO `+sourceQualified+` VALUES
			(1, 'alpha', 'source-a'),
			(1, 'beta', 'source-b'),
			(2, 'keep', 'source-c');
		INSERT INTO `+targetQualified+` VALUES
			(1, 'alpha', 'target-a'),
			(1, 'beta', 'target-b'),
			(2, 'keep', 'target-c'),
			(3, 'delete-a', 'target-only-a'),
			(4, 'delete-b', 'target-only-b'),
			(5, 'delete-c', 'target-only-c');
	`); err != nil {
		t.Fatalf("create PostgreSQL delete tables: %v", err)
	}
	sourceTable, err := engine.InspectPostgresTable(
		ctx,
		database,
		sourceNamespace,
		tableName,
	)
	if err != nil {
		t.Fatalf("inspect PostgreSQL delete source: %v", err)
	}
	targetTable, err := engine.InspectPostgresTable(
		ctx,
		database,
		targetNamespace,
		tableName,
	)
	if err != nil {
		t.Fatalf("inspect PostgreSQL delete target: %v", err)
	}
	sourceAdapter := &relationalSourceAdapter{
		spec: relationalSourceSpec{
			engine:      "postgres",
			displayName: "PostgreSQL",
		},
		database:  database,
		namespace: sourceNamespace,
	}
	targetAdapter := &postgresTargetAdapter{
		database:  database,
		namespace: targetNamespace,
	}
	capabilities, err :=
		newPostgresDeleteReconciliationCapabilities(
			ctx,
			sourceAdapter,
			targetAdapter,
			sourceTable,
			targetTable,
		)
	if err != nil {
		t.Fatalf("admit PostgreSQL delete capabilities: %v", err)
	}
	sameTarget := &postgresTargetAdapter{
		database:  database,
		namespace: sourceNamespace,
	}
	if _, err := newPostgresDeleteReconciliationCapabilities(
		ctx,
		sourceAdapter,
		sameTarget,
		sourceTable,
		sourceTable,
	); err == nil || !strings.Contains(
		err.Error(),
		"identical source and target relation",
	) {
		t.Fatalf("identical PostgreSQL relation error = %v", err)
	}
	keyColumns := []string{"tenant_id", "external_id"}
	if rows, err := capabilities.source.OpenDeletePrimaryKeys(
		ctx,
		sourceTable,
		[]string{"external_id", "tenant_id"},
	); err == nil {
		_ = rows.Close()
		t.Fatal("PostgreSQL delete reader accepted reversed primary-key order")
	}
	changed := sourceTable
	changed.Columns = append([]schema.Column(nil), sourceTable.Columns...)
	changed.Columns[0].Type = "integer"
	if _, err := newPostgresDeleteReconciliationCapabilities(
		ctx,
		sourceAdapter,
		targetAdapter,
		changed,
		targetTable,
	); err == nil {
		t.Fatal("PostgreSQL delete capability accepted stale key metadata")
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	request := deleteReconcileRequest{
		RunID:     "postgres-delete-" + suffix,
		AttemptID: "attempt-1",
		Task: state.TaskKey{
			Type: "table-copy", Schema: sourceNamespace,
			Table: tableName,
		},
		SourceTable: sourceTable,
		TargetTable: targetTable,
		TargetMode:  "upsert",
		Policy: config.DeletePolicy{
			Mode:           config.DeleteModeReconcile,
			TargetBehavior: config.DeleteTargetHard,
			Reconcile: config.DeleteReconcilePolicy{
				Schedule:          config.DeleteScheduleInterval,
				Interval:          24 * time.Hour,
				BatchSize:         2,
				RequirePrimaryKey: true,
			},
		},
		Now:            now,
		SpoolDirectory: t.TempDir(),
		MaxBatchBytes:  1 << 20,
	}
	backend := newDeleteFakeState()
	reconciler := deleteReconciler{
		state:         backend,
		source:        capabilities.source,
		target:        capabilities.target,
		canonicalizer: capabilities.canonicalizer,
		protector:     &deleteFakeProtector{},
		now: func() time.Time {
			return now.Add(time.Minute)
		},
	}
	outcome, err := reconciler.reconcile(ctx, request)
	if err != nil {
		t.Fatalf("reconcile PostgreSQL deletes: %v", err)
	}
	if outcome.Record.Status != state.DeleteReconciliationCompleted ||
		outcome.Record.Candidates != 3 ||
		outcome.Record.DeletedRows != 3 ||
		outcome.Record.CommittedBatches != 2 ||
		!outcome.StrictCountValidation {
		t.Fatalf("PostgreSQL delete outcome = %#v", outcome)
	}
	if backend.record.Plan != nil {
		planID := backend.record.Plan.PlanID
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cleanupCancel()
			if _, err := database.ExecContext(
				cleanupCtx,
				"DELETE FROM "+
					postgresQualified(
						postgresDeleteJournalSchema,
						postgresDeleteJournalTable,
					)+
					" WHERE plan_id = $1",
				planID,
			); err != nil {
				t.Errorf(
					"clean PostgreSQL delete receipts: %v",
					err,
				)
			}
		})
	}
	var remaining int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+targetQualified,
	).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 3 {
		t.Fatalf("PostgreSQL target rows after reconciliation = %d", remaining)
	}
	var targetOnly int
	if err := database.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM "+targetQualified+
			" WHERE tenant_id >= 3",
	).Scan(&targetOnly); err != nil {
		t.Fatal(err)
	}
	if targetOnly != 0 {
		t.Fatalf("PostgreSQL target-only rows remain = %d", targetOnly)
	}
	if outcome.Record.LastBatchCommit == nil {
		t.Fatal("PostgreSQL delete outcome lacks a target receipt")
	}
	last := *outcome.Record.LastBatchCommit
	replayedReceipt, err := capabilities.target.ApplyDeleteBatch(
		ctx,
		deleteTargetBatch{
			Table:       targetTable,
			Columns:     keyColumns,
			PlanID:      last.PlanID,
			Token:       last.Token,
			Sequence:    last.Sequence,
			BatchDigest: last.BatchDigest,
			Keys: [][]driver.Value{{
				int64(5),
				"delete-c",
			}},
		},
	)
	if err != nil {
		t.Fatalf("replay PostgreSQL delete receipt: %v", err)
	}
	if replayedReceipt.ReceiptDigest != last.ReceiptDigest ||
		replayedReceipt.DeletedRows != last.DeletedRows {
		t.Fatalf(
			"replayed receipt=%#v durable=%#v",
			replayedReceipt,
			last,
		)
	}
	restoreReceiptDigest := func(testingContext context.Context) error {
		_, err := database.ExecContext(
			testingContext,
			"UPDATE "+
				postgresQualified(
					postgresDeleteJournalSchema,
					postgresDeleteJournalTable,
				)+
				" SET receipt_digest = $1 WHERE token = $2",
			last.ReceiptDigest,
			last.Token,
		)
		return err
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if err := restoreReceiptDigest(cleanupCtx); err != nil {
			t.Errorf("restore PostgreSQL delete receipt digest: %v", err)
		}
	})
	if _, err := database.ExecContext(
		ctx,
		"UPDATE "+
			postgresQualified(
				postgresDeleteJournalSchema,
				postgresDeleteJournalTable,
			)+
			" SET receipt_digest = $1 WHERE token = $2",
		strings.Repeat("c", 64),
		last.Token,
	); err != nil {
		t.Fatalf("tamper PostgreSQL delete receipt digest: %v", err)
	}
	if _, err := capabilities.target.ApplyDeleteBatch(
		ctx,
		deleteTargetBatch{
			Table:       targetTable,
			Columns:     keyColumns,
			PlanID:      last.PlanID,
			Token:       last.Token,
			Sequence:    last.Sequence,
			BatchDigest: last.BatchDigest,
			Keys: [][]driver.Value{{
				int64(5),
				"delete-c",
			}},
		},
	); err == nil || !strings.Contains(
		err.Error(),
		"receipt digest differs",
	) {
		t.Fatalf("tampered PostgreSQL receipt replay error = %v", err)
	}
	if err := restoreReceiptDigest(ctx); err != nil {
		t.Fatalf("restore PostgreSQL delete receipt digest: %v", err)
	}
	replayed, err := reconciler.reconcile(ctx, request)
	if err != nil ||
		replayed.Record.Status != state.DeleteReconciliationCompleted ||
		!replayed.StrictCountValidation {
		t.Fatalf("replay completed reconciliation = %#v err=%v", replayed, err)
	}

	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+targetQualified+
			" VALUES (9, 'dry-only', 'dry-run')",
	); err != nil {
		t.Fatal(err)
	}
	dryRequest := request
	dryRequest.AttemptID = "dry-run"
	dryRequest.DryRun = true
	dryBackend := newDeleteFakeState()
	dryReconciler := reconciler
	dryReconciler.state = dryBackend
	dryOutcome, err := dryReconciler.reconcile(ctx, dryRequest)
	if err != nil {
		t.Fatalf("dry-run PostgreSQL delete reconciliation: %v", err)
	}
	if dryOutcome.Record.Status != state.DeleteReconciliationDryRun ||
		dryOutcome.Record.Candidates != 1 ||
		dryOutcome.Record.DeletedRows != 0 ||
		dryOutcome.StrictCountValidation ||
		dryBackend.beginCalls != 0 {
		t.Fatalf("PostgreSQL delete dry-run outcome = %#v", dryOutcome)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+targetQualified+
			" WHERE tenant_id = 9",
	).Scan(&targetOnly); err != nil {
		t.Fatal(err)
	}
	if targetOnly != 1 {
		t.Fatal("PostgreSQL delete dry-run mutated the target")
	}

	notDueRequest := request
	notDueRequest.AttemptID = "not-due"
	notDueRequest.Now = now.Add(2 * time.Hour)
	notDueBackend := newDeleteFakeState()
	notDueBackend.latest = deleteCompletedEvidence(
		notDueRequest,
		now,
	)
	notDueBackend.latestFound = true
	notDueReconciler := reconciler
	notDueReconciler.state = notDueBackend
	notDueOutcome, err := notDueReconciler.reconcile(
		ctx,
		notDueRequest,
	)
	if err != nil {
		t.Fatalf("not-due PostgreSQL delete reconciliation: %v", err)
	}
	if notDueOutcome.Record.Status !=
		state.DeleteReconciliationNotDue ||
		notDueOutcome.Record.Due ||
		notDueOutcome.StrictCountValidation ||
		!strings.Contains(
			notDueOutcome.Record.Reason,
			"interval has not elapsed",
		) {
		t.Fatalf(
			"PostgreSQL delete not-due outcome = %#v",
			notDueOutcome,
		)
	}

	blockerQualified := postgresQualified(
		targetNamespace,
		"delete_blockers",
	)
	if _, err := database.ExecContext(ctx, `
		INSERT INTO `+targetQualified+`
			VALUES (10, 'blocked', 'must-survive-failure');
		CREATE TABLE `+blockerQualified+` (
			id bigint NOT NULL PRIMARY KEY,
			tenant_id bigint NOT NULL,
			external_id text NOT NULL,
			CONSTRAINT delete_blockers_target_fk
				FOREIGN KEY (tenant_id, external_id)
				REFERENCES `+targetQualified+`
					(tenant_id, external_id)
				ON DELETE RESTRICT
		);
		INSERT INTO `+blockerQualified+`
			VALUES (1, 10, 'blocked');
	`); err != nil {
		t.Fatalf("create PostgreSQL restricted delete failure: %v", err)
	}
	failedToken := strings.Repeat("d", 64)
	failedBatch := deleteTargetBatch{
		Table:       targetTable,
		Columns:     keyColumns,
		PlanID:      "11223344556677889900aabbccddeeff",
		Token:       failedToken,
		Sequence:    0,
		BatchDigest: strings.Repeat("e", 64),
		Keys: [][]driver.Value{{
			int64(10),
			"blocked",
		}},
	}
	if _, err := capabilities.target.ApplyDeleteBatch(
		ctx,
		failedBatch,
	); err == nil ||
		!strings.Contains(
			err.Error(),
			"failed atomically and no receipt was published",
		) ||
		!strings.Contains(err.Error(), "same token") {
		t.Fatalf("PostgreSQL failed-delete recovery error = %v", err)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+targetQualified+
			" WHERE tenant_id = 10 AND external_id = 'blocked'",
	).Scan(&targetOnly); err != nil {
		t.Fatal(err)
	}
	if targetOnly != 1 {
		t.Fatal("failed PostgreSQL delete did not roll back its row mutation")
	}
	var failedReceipts int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			postgresQualified(
				postgresDeleteJournalSchema,
				postgresDeleteJournalTable,
			)+
			" WHERE token = $1",
		failedToken,
	).Scan(&failedReceipts); err != nil {
		t.Fatal(err)
	}
	if failedReceipts != 0 {
		t.Fatal("failed PostgreSQL delete published a replayable receipt")
	}
	if _, err := database.ExecContext(
		ctx,
		"DROP TABLE "+blockerQualified+
			"; DELETE FROM "+targetQualified+
			" WHERE tenant_id = 10 AND external_id = 'blocked'",
	); err != nil {
		t.Fatalf("clean PostgreSQL restricted delete failure: %v", err)
	}

	cascadeQualified := postgresQualified(
		targetNamespace,
		"delete_cascade_child",
	)
	if _, err := database.ExecContext(ctx, `
		CREATE TABLE `+cascadeQualified+` (
			id bigint NOT NULL PRIMARY KEY,
			tenant_id bigint NOT NULL,
			external_id text NOT NULL,
			CONSTRAINT delete_cascade_target_fk
				FOREIGN KEY (tenant_id, external_id)
				REFERENCES `+targetQualified+`
					(tenant_id, external_id)
				ON DELETE CASCADE
		)
	`); err != nil {
		t.Fatalf("create PostgreSQL cascading incoming FK: %v", err)
	}
	if _, err := newPostgresDeleteReconciliationCapabilities(
		ctx,
		sourceAdapter,
		targetAdapter,
		sourceTable,
		targetTable,
	); err == nil || !strings.Contains(
		err.Error(),
		"hook-free PostgreSQL 16 heap",
	) {
		t.Fatalf("PostgreSQL cascading incoming FK error = %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"DROP TABLE "+cascadeQualified,
	); err != nil {
		t.Fatalf("drop PostgreSQL cascading incoming FK: %v", err)
	}

	aclTx, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := aclTx.ExecContext(
		ctx,
		"GRANT SELECT ON TABLE "+
			postgresQualified(
				postgresDeleteJournalSchema,
				postgresDeleteJournalTable,
			)+
			" TO PUBLIC",
	); err != nil {
		_ = aclTx.Rollback()
		t.Fatalf("tamper PostgreSQL receipt ACL: %v", err)
	}
	if _, err := inspectPostgresDeleteReceiptJournal(
		ctx,
		aclTx,
	); err == nil || !strings.Contains(
		err.Error(),
		"owner-only",
	) {
		_ = aclTx.Rollback()
		t.Fatalf("unsafe PostgreSQL receipt ACL error = %v", err)
	}
	if err := aclTx.Rollback(); err != nil {
		t.Fatalf("rollback PostgreSQL receipt ACL tamper: %v", err)
	}
	indexTx, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := indexTx.ExecContext(
		ctx,
		"CREATE INDEX delete_batch_receipts_unexpected_idx ON "+
			postgresQualified(
				postgresDeleteJournalSchema,
				postgresDeleteJournalTable,
			)+
			" (plan_id)",
	); err != nil {
		_ = indexTx.Rollback()
		t.Fatalf("tamper PostgreSQL receipt indexes: %v", err)
	}
	if _, err := inspectPostgresDeleteReceiptJournal(
		ctx,
		indexTx,
	); err == nil || !strings.Contains(
		err.Error(),
		"index",
	) {
		_ = indexTx.Rollback()
		t.Fatalf("unexpected PostgreSQL receipt index error = %v", err)
	}
	if err := indexTx.Rollback(); err != nil {
		t.Fatalf("rollback PostgreSQL receipt index tamper: %v", err)
	}
	expressionIndexTx, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := expressionIndexTx.ExecContext(
		ctx,
		"CREATE INDEX delete_batch_receipts_hidden_expression_idx ON "+
			postgresQualified(
				postgresDeleteJournalSchema,
				postgresDeleteJournalTable,
			)+
			" ((pg_catalog.length(plan_id))) "+
			"WHERE journal_version = 1",
	); err != nil {
		_ = expressionIndexTx.Rollback()
		t.Fatalf(
			"tamper PostgreSQL receipt expression index: %v",
			err,
		)
	}
	if _, err := inspectPostgresDeleteReceiptJournal(
		ctx,
		expressionIndexTx,
	); err == nil ||
		!strings.Contains(err.Error(), "expression or partial index") {
		_ = expressionIndexTx.Rollback()
		t.Fatalf(
			"hidden PostgreSQL receipt expression-index error = %v",
			err,
		)
	}
	if err := expressionIndexTx.Rollback(); err != nil {
		t.Fatalf(
			"rollback PostgreSQL receipt expression-index tamper: %v",
			err,
		)
	}
	if err := preflightPostgresDeleteReceiptJournal(
		ctx,
		database,
	); err != nil {
		t.Fatalf("restored PostgreSQL receipt journal: %v", err)
	}

	oldProof, err := capabilities.canonicalizer.
		ProveDeleteKeyEquality(
			sourceTable,
			targetTable,
			mustDeletePrimaryKeyColumns(t, sourceTable),
			mustDeletePrimaryKeyColumns(t, targetTable),
		)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, `
		DROP TABLE `+targetQualified+`;
		CREATE TABLE `+targetQualified+` (
			tenant_id bigint NOT NULL,
			external_id text NOT NULL,
			payload text NOT NULL,
			PRIMARY KEY (tenant_id, external_id)
		)
	`); err != nil {
		t.Fatalf("recreate identical-shape PostgreSQL target: %v", err)
	}
	if rows, err := capabilities.target.OpenDeletePrimaryKeys(
		ctx,
		targetTable,
		keyColumns,
	); err == nil {
		_ = rows.Close()
		t.Fatal(
			"old PostgreSQL delete capability accepted an identical-shape recreated target",
		)
	} else if !strings.Contains(err.Error(), "authority changed") {
		t.Fatalf("PostgreSQL recreated-target error = %v", err)
	}
	recreatedTarget, err := engine.InspectPostgresTable(
		ctx,
		database,
		targetNamespace,
		tableName,
	)
	if err != nil {
		t.Fatalf("inspect recreated PostgreSQL target: %v", err)
	}
	recreatedCapabilities, err :=
		newPostgresDeleteReconciliationCapabilities(
			ctx,
			sourceAdapter,
			targetAdapter,
			sourceTable,
			recreatedTarget,
		)
	if err != nil {
		t.Fatalf("admit recreated PostgreSQL target: %v", err)
	}
	newProof, err := recreatedCapabilities.canonicalizer.
		ProveDeleteKeyEquality(
			sourceTable,
			recreatedTarget,
			mustDeletePrimaryKeyColumns(t, sourceTable),
			mustDeletePrimaryKeyColumns(t, recreatedTarget),
		)
	if err != nil {
		t.Fatal(err)
	}
	if oldProof.CanonicalizerID == newProof.CanonicalizerID {
		t.Fatal(
			"PostgreSQL relation recreation did not change the durable equality-proof authority",
		)
	}
}

func mustDeletePrimaryKeyColumns(
	t *testing.T,
	table schema.Table,
) []schema.Column {
	t.Helper()
	columns, err := deletePrimaryKeyColumns(table)
	if err != nil {
		t.Fatal(err)
	}
	return columns
}

func postgresDeleteLiveRequiresVerifiedTLS(
	parsed *pgx.ConnConfig,
) bool {
	if parsed == nil ||
		parsed.TLSConfig == nil ||
		parsed.TLSConfig.InsecureSkipVerify ||
		parsed.TLSConfig.RootCAs == nil ||
		strings.TrimSpace(parsed.TLSConfig.ServerName) == "" {
		return false
	}
	for _, fallback := range parsed.Fallbacks {
		if fallback.TLSConfig == nil ||
			fallback.TLSConfig.InsecureSkipVerify ||
			fallback.TLSConfig.RootCAs == nil ||
			strings.TrimSpace(
				fallback.TLSConfig.ServerName,
			) == "" {
			return false
		}
	}
	return true
}
