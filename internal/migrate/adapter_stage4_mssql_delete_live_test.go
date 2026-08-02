package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestSQLServerDeleteCapabilitiesLiveTLS exercises the native source scanner,
// private header/journal, receipt replay, commit-ack recovery, and lock shape
// against an isolated verified-TLS SQL Server 2022 database pair. It is kept
// separate from the composed route below so the local authority failures have
// a concise diagnosis when a server upgrade changes a catalog fact.
func TestSQLServerDeleteCapabilitiesLiveTLS(t *testing.T) {
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
	t.Run("isolated source and target", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &sourceCleanup)
		targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &targetCleanup)
		sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "delete source", sourceEndpoint)
		targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "delete target", targetEndpoint)

		items := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		empty := "empty_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, items)
		createSQLServerDeleteLiveTable(t, ctx, targetDatabase, targetEndpoint.Schema, items)
		createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, empty)
		if _, err := sourceDatabase.ExecContext(ctx,
			"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, items)+
				" ([tenant_id], [item_id], [payload]) VALUES (1, 1, 'source'), (1, 2, 'source')",
		); err != nil {
			t.Fatalf("seed SQL Server delete source: %v", err)
		}
		if _, err := targetDatabase.ExecContext(ctx,
			"INSERT INTO "+sqlServerQualified(targetEndpoint.Schema, items)+
				" ([tenant_id], [item_id], [payload]) VALUES (1, 1, 'source'), (1, 2, 'source'), (9, 9, 'orphan')",
		); err != nil {
			t.Fatalf("seed SQL Server delete target: %v", err)
		}

		sourceValue, err := openSQLServerSourceAdapter(ctx, sourceEndpoint)
		if err != nil {
			t.Fatalf("open SQL Server delete source adapter: %v", err)
		}
		source := sourceValue.(*relationalSourceAdapter)
		t.Cleanup(func() { _ = source.Close() })
		targetValue, err := openSQLServerTargetAdapter(ctx, targetEndpoint)
		if err != nil {
			t.Fatalf("open SQL Server delete target adapter: %v", err)
		}
		target := targetValue.(*sqlServerTargetAdapter)
		t.Cleanup(func() { _ = target.Close() })
		// SQL Server requires CREATE SCHEMA to occupy its own batch.
		if _, err := target.database.ExecContext(ctx,
			"CREATE SCHEMA "+sqlServerIdentifier(sqlServerDeleteJournalSchema)+
				" AUTHORIZATION [dbo]",
		); err != nil {
			t.Fatalf("create malformed SQL Server delete journal schema: %v", err)
		}
		if _, err := target.database.ExecContext(ctx,
			"CREATE TABLE "+
				sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable)+
				" ([token] bigint NOT NULL)",
		); err != nil {
			t.Fatalf("create malformed SQL Server delete journal fixture: %v", err)
		}
		if err := target.PreflightStage4DeleteJournalReadiness(ctx); err == nil {
			t.Fatal("malformed SQL Server delete journal passed read-only preflight")
		}
		if _, err := target.database.ExecContext(ctx,
			"DROP TABLE "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable),
		); err != nil {
			t.Fatalf("remove malformed SQL Server delete journal fixture: %v", err)
		}
		// CREATE VIEW must be the first statement in its SQL Server batch.
		if _, err := target.database.ExecContext(ctx,
			"CREATE VIEW "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable)+
				" AS SELECT CAST(1 AS int) AS [token]",
		); err != nil {
			t.Fatalf("create colliding SQL Server delete journal view: %v", err)
		}
		if err := target.PreflightStage4DeleteJournalReadiness(ctx); err == nil {
			t.Fatal("same-name non-table SQL Server delete journal collision passed read-only preflight")
		}
		if _, err := target.database.ExecContext(ctx,
			"DROP VIEW "+sqlServerQualified(sqlServerDeleteJournalSchema, sqlServerDeleteJournalTable),
		); err != nil {
			t.Fatalf("remove colliding SQL Server delete journal view: %v", err)
		}
		if _, err := target.database.ExecContext(ctx,
			"DROP SCHEMA "+sqlServerIdentifier(sqlServerDeleteJournalSchema),
		); err != nil {
			t.Fatalf("remove malformed SQL Server delete journal fixture: %v", err)
		}

		sourceTable, err := source.InspectTable(ctx, items)
		if err != nil {
			t.Fatalf("inspect SQL Server delete source table: %v", err)
		}
		targetTable, err := engine.InspectSQLServerTargetTableWithQueryer(
			ctx, target.database, targetEndpoint.Schema, items,
		)
		if err != nil {
			t.Fatalf("inspect SQL Server delete target table: %v", err)
		}
		capabilities, err := newSQLServerDeleteReconciliationCapabilities(
			ctx, source, target, sourceTable, targetTable,
		)
		if err != nil {
			t.Fatalf("admit SQL Server delete capabilities: %v", err)
		}
		if _, err := target.PrepareStage4DeleteJournalReadiness(ctx, Stage4DeleteJournalReadinessRequest{
			RunID: "mssql-delete-live-" + items, InventoryDigest: strings.Repeat("a", 64),
		}); err != nil {
			t.Fatalf("prepare SQL Server delete journal: %v", err)
		}
		journal, err := inspectSQLServerDeleteReceiptJournal(ctx, target.database)
		if err != nil || !journal.Exists || journal.HeaderIdentity == "" {
			t.Fatalf("inspect live SQL Server delete journal = %#v, %v", journal, err)
		}

		assertSQLServerDeleteLiveScanBlocksWriter(t, ctx, source, sourceTable, sourceDatabase, false)
		emptyTable, err := source.InspectTable(ctx, empty)
		if err != nil {
			t.Fatal(err)
		}
		emptyCapability, err := newSQLServerDeleteSourceCapability(ctx, source, emptyTable)
		if err != nil {
			t.Fatalf("admit empty SQL Server delete source: %v", err)
		}
		assertSQLServerDeleteLiveScanBlocksWriter(t, ctx, source, emptyTable, sourceDatabase, true)
		_ = emptyCapability // admission above ensures the empty-table authority itself is exact.

		externalTarget, err := engine.OpenSQLServer(ctx, targetEndpoint)
		if err != nil {
			t.Fatalf("open SQL Server delete target contention probe: %v", err)
		}
		t.Cleanup(func() { _ = externalTarget.Close() })
		target.deleteAfterReservation = func(ctx context.Context, _ *sql.Conn) error {
			raceCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
			defer cancel()
			_, raceErr := externalTarget.ExecContext(
				raceCtx,
				"ALTER TABLE "+sqlServerQualified(targetEndpoint.Schema, items)+" ADD [unexpected_race] int NULL",
			)
			if raceErr == nil {
				return errors.New("external SQL Server schema writer entered while delete table lock was held")
			}
			return nil
		}
		batch := deleteTargetBatch{
			Table: targetTable, Columns: []string{"tenant_id", "item_id"},
			PlanID: strings.Repeat("a", 32), Token: strings.Repeat("b", 64),
			Sequence: 0, BatchDigest: strings.Repeat("c", 64),
			Keys: [][]driver.Value{{int64(9), int64(9)}},
		}
		target.deleteCommit = func(ctx context.Context, connection *sql.Conn) (sql.Result, error) {
			result, err := connection.ExecContext(ctx, "COMMIT TRANSACTION")
			if err != nil {
				return result, err
			}
			return result, errors.New("injected SQL Server delete commit acknowledgement loss")
		}
		first, err := capabilities.target.ApplyDeleteBatch(ctx, batch)
		if err != nil {
			t.Fatalf("recover committed SQL Server delete after acknowledgement loss: %v", err)
		}
		if first.Candidates != 1 || first.DeletedRows != 1 {
			t.Fatalf("first SQL Server delete receipt = %#v", first)
		}
		target.deleteAfterReservation = nil
		target.deleteCommit = nil
		if err := target.Close(); err != nil {
			t.Fatalf("close first SQL Server delete target: %v", err)
		}

		reopenedValue, err := openSQLServerTargetAdapter(ctx, targetEndpoint)
		if err != nil {
			t.Fatalf("reopen SQL Server delete target: %v", err)
		}
		reopened := reopenedValue.(*sqlServerTargetAdapter)
		t.Cleanup(func() { _ = reopened.Close() })
		replayedCapabilities, err := newSQLServerDeleteReconciliationCapabilities(
			ctx, source, reopened, sourceTable, targetTable,
		)
		if err != nil {
			t.Fatalf("re-admit reopened SQL Server delete capabilities: %v", err)
		}
		replayed, err := replayedCapabilities.target.ApplyDeleteBatch(ctx, batch)
		if err != nil {
			t.Fatalf("replay committed SQL Server delete receipt after reopen: %v", err)
		}
		if !reflect.DeepEqual(first, replayed) {
			t.Fatalf("replayed SQL Server delete receipt differs\nfirst=%#v\nreplayed=%#v", first, replayed)
		}
		var orphanRows int
		if err := reopened.database.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, items)+" WHERE [tenant_id] = 9 AND [item_id] = 9",
		).Scan(&orphanRows); err != nil || orphanRows != 0 {
			t.Fatalf("replayed SQL Server delete changed target rows=%d err=%v", orphanRows, err)
		}
		if _, err := reopened.database.ExecContext(ctx,
			"INSERT INTO "+sqlServerQualified(targetEndpoint.Schema, items)+
				" ([tenant_id], [item_id], [payload]) VALUES (8, 8, 'must-remain'); "+
				"ALTER TABLE "+sqlServerQualified(targetEndpoint.Schema, items)+" ADD [catalog_drift] int NULL",
		); err != nil {
			t.Fatalf("inject SQL Server target catalog drift: %v", err)
		}
		driftBatch := batch
		driftBatch.Token = strings.Repeat("d", 64)
		driftBatch.Sequence++
		driftBatch.BatchDigest = strings.Repeat("e", 64)
		driftBatch.Keys = [][]driver.Value{{int64(8), int64(8)}}
		if _, err := replayedCapabilities.target.ApplyDeleteBatch(ctx, driftBatch); err == nil || !strings.Contains(err.Error(), "catalog") {
			t.Fatalf("catalog-drift SQL Server delete batch = %v", err)
		}
		var driftRows int
		if err := reopened.database.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, items)+" WHERE [tenant_id] = 8 AND [item_id] = 8",
		).Scan(&driftRows); err != nil || driftRows != 1 {
			t.Fatalf("catalog-drift SQL Server delete mutated target rows=%d err=%v", driftRows, err)
		}
	})
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
}

// TestStage4SQLServerDeleteCompositionLiveTLS proves the capability is wired
// through the production Stage 4 lifecycle. The generic readiness suite
// separately proves relative lifecycle ordering; this route exercises its
// real SQL Server preparer before target data writes.
func TestStage4SQLServerDeleteCompositionLiveTLS(t *testing.T) {
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	var sourceCleanup, targetCleanup sqlServerTargetEvolutionLiveCleanupEvidence
	t.Run("composed mssql to mssql", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
		defer cancel()
		sourceEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &sourceCleanup)
		targetEndpoint := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &targetCleanup)
		sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "composed delete source", sourceEndpoint)
		targetDatabase := openSQLServerNativeLiveDatabase(t, ctx, "composed delete target", targetEndpoint)
		tableName := "items_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		createSQLServerDeleteLiveTable(t, ctx, sourceDatabase, sourceEndpoint.Schema, tableName)
		createSQLServerDeleteLiveTable(t, ctx, targetDatabase, targetEndpoint.Schema, tableName)
		if _, err := sourceDatabase.ExecContext(ctx,
			"INSERT INTO "+sqlServerQualified(sourceEndpoint.Schema, tableName)+
				" ([tenant_id], [item_id], [payload]) VALUES (1, 1, 'source'), (1, 2, 'source')",
		); err != nil {
			t.Fatalf("seed composed SQL Server delete source: %v", err)
		}
		if _, err := targetDatabase.ExecContext(ctx,
			"INSERT INTO "+sqlServerQualified(targetEndpoint.Schema, tableName)+
				" ([tenant_id], [item_id], [payload]) VALUES (1, 1, 'stale'), (1, 2, 'stale'), (9, 9, 'orphan')",
		); err != nil {
			t.Fatalf("seed composed SQL Server delete target: %v", err)
		}
		cfg := config.Config{
			Source: sourceEndpoint,
			Target: targetEndpoint,
			Migration: config.Migration{
				TargetMode:         "upsert",
				IncludeTables:      []string{tableName},
				ConnectionLimit:    4,
				ReaderParallelism:  1,
				WriterParallelism:  1,
				MemoryCeilingBytes: 64 << 20,
				Validation: config.ValidationPolicy{
					Mode:                   config.ValidationCountOnly,
					FailOnMismatch:         true,
					FailOnTimeout:          true,
					FailOnEstimateMismatch: true,
				},
				Deletes: config.DeletePolicy{
					Mode:           config.DeleteModeReconcile,
					TargetBehavior: config.DeleteTargetHard,
					Reconcile: config.DeleteReconcilePolicy{
						Schedule:          config.DeleteScheduleInterval,
						Interval:          time.Hour,
						BatchSize:         1,
						RequirePrimaryKey: true,
					},
				},
			},
		}
		backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
		runID := "stage4-mssql-delete-live-" + tableName
		initializeStage4SQLServerStrictDeleteLifecycleRun(
			t,
			backend,
			runID,
			sourceEndpoint,
			targetEndpoint,
			time.Now().UTC().Add(-time.Minute),
		)
		events := make([]string, 0)
		observer := stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run:                    stage4LifecycleRunContext(t, backend, runID, false),
		}
		result, err := SQLServerToSQLServerWithObserver(ctx, cfg, observer)
		if err != nil {
			t.Fatalf("run composed SQL Server delete reconciliation: %v", err)
		}
		if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
			t.Fatalf("composed SQL Server delete result = %#v", result)
		}
		var orphanRows, sourceRows int
		if err := targetDatabase.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, tableName)+" WHERE [tenant_id] = 9 AND [item_id] = 9",
		).Scan(&orphanRows); err != nil || orphanRows != 0 {
			t.Fatalf("composed target orphan rows=%d err=%v", orphanRows, err)
		}
		if err := targetDatabase.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM "+sqlServerQualified(targetEndpoint.Schema, tableName),
		).Scan(&sourceRows); err != nil || sourceRows != 2 {
			t.Fatalf("composed target rows=%d err=%v", sourceRows, err)
		}
		ready, found, err := backend.LoadStage4DeleteJournalReadiness(runID)
		if err != nil || !found || ready.Readiness.TargetEngine != "mssql" {
			t.Fatalf("composed SQL Server delete readiness found=%t receipt=%#v err=%v", found, ready, err)
		}
		record, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(
			runID,
			state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: sourceEndpoint.Schema, Table: tableName},
		)
		if err != nil || !found || record.Candidates != 1 || record.DeletedRows != 1 {
			t.Fatalf("composed SQL Server delete reconciliation found=%t record=%#v err=%v", found, record, err)
		}
		journal, err := inspectSQLServerDeleteReceiptJournal(ctx, targetDatabase)
		if err != nil || !journal.Exists || journal.HeaderIdentity == "" {
			t.Fatalf("composed SQL Server delete journal = %#v err=%v", journal, err)
		}
	})
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, sourceCleanup)
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, targetCleanup)
}

// TestSQLServerDeleteJournalTwoSchemaContentionLiveTLS proves the app-lock is
// scoped to the database incarnation, not the configured user schema. This
// is deliberately a same-principal test; cross-principal shared-schema reuse
// remains explicitly fail-closed in admission.
func TestSQLServerDeleteJournalTwoSchemaContentionLiveTLS(t *testing.T) {
	base := sqlServerTargetEvolutionLiveEndpoint(t)
	var cleanup sqlServerTargetEvolutionLiveCleanupEvidence
	t.Run("same database distinct schemas", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		endpointA := sqlServerTargetEvolutionLiveTemporaryDatabase(t, ctx, base, &cleanup)
		endpointB := endpointA
		endpointB.Schema = "dmtx_delete_other_" + strconv.FormatInt(time.Now().UnixNano(), 36)
		database := openSQLServerNativeLiveDatabase(t, ctx, "delete journal two-schema setup", endpointA)
		if _, err := database.ExecContext(ctx,
			"CREATE SCHEMA "+sqlServerIdentifier(endpointB.Schema)+" AUTHORIZATION [dbo]",
		); err != nil {
			t.Fatalf("create second SQL Server delete target schema: %v", err)
		}
		firstValue, err := openSQLServerTargetAdapter(ctx, endpointA)
		if err != nil {
			t.Fatal(err)
		}
		first := firstValue.(*sqlServerTargetAdapter)
		t.Cleanup(func() { _ = first.Close() })
		secondValue, err := openSQLServerTargetAdapter(ctx, endpointB)
		if err != nil {
			t.Fatal(err)
		}
		second := secondValue.(*sqlServerTargetAdapter)
		t.Cleanup(func() { _ = second.Close() })

		holder, err := first.database.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Close()
		if err := configureSQLServerDeleteTransaction(ctx, holder); err != nil {
			t.Fatal(err)
		}
		if _, err := holder.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
			t.Fatal(err)
		}
		identity, err := readSQLServerDeleteEndpointIdentity(ctx, holder, endpointA.Schema)
		if err != nil {
			_ = rollbackSQLServerDeleteTransaction(ctx, holder)
			t.Fatal(err)
		}
		resource, err := sqlServerDeleteJournalLockResource(identity)
		if err != nil {
			_ = rollbackSQLServerDeleteTransaction(ctx, holder)
			t.Fatal(err)
		}
		if err := acquireSQLServerDeleteBatchLock(ctx, holder, resource); err != nil {
			_ = rollbackSQLServerDeleteTransaction(ctx, holder)
			t.Fatal(err)
		}
		contendedCtx, cancelContended := context.WithTimeout(ctx, 125*time.Millisecond)
		_, contentionErr := second.PrepareStage4DeleteJournalReadiness(contendedCtx, Stage4DeleteJournalReadinessRequest{
			RunID: "mssql-delete-two-schema-contended", InventoryDigest: strings.Repeat("a", 64),
		})
		cancelContended()
		if contentionErr == nil {
			_ = rollbackSQLServerDeleteTransaction(ctx, holder)
			t.Fatal("distinct schema readiness entered while database-global journal lock was held")
		}
		if err := rollbackSQLServerDeleteTransaction(ctx, holder); err != nil {
			t.Fatal(err)
		}
		if err := resetSQLServerDeleteSession(ctx, holder); err != nil {
			t.Fatal(err)
		}
		if _, err := second.PrepareStage4DeleteJournalReadiness(ctx, Stage4DeleteJournalReadinessRequest{
			RunID: "mssql-delete-two-schema-after-release", InventoryDigest: strings.Repeat("a", 64),
		}); err != nil {
			t.Fatalf("distinct schema readiness did not acquire lock after release: %v", err)
		}
	})
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, base, cleanup)
}

func createSQLServerDeleteLiveTable(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	name string,
) {
	t.Helper()
	statement := fmt.Sprintf(
		"CREATE TABLE %s ([tenant_id] BIGINT NOT NULL, [item_id] INT NOT NULL, [payload] VARCHAR(32) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL, CONSTRAINT %s PRIMARY KEY CLUSTERED ([tenant_id], [item_id]))",
		sqlServerQualified(namespace, name),
		sqlServerIdentifier("pk_"+name),
	)
	if _, err := database.ExecContext(ctx, statement); err != nil {
		t.Fatalf("create SQL Server delete table %s: %v", name, err)
	}
}

func assertSQLServerDeleteLiveScanBlocksWriter(
	t *testing.T,
	ctx context.Context,
	source *relationalSourceAdapter,
	table schema.Table,
	database *sql.DB,
	empty bool,
) {
	t.Helper()
	capability, err := newSQLServerDeleteSourceCapability(ctx, source, table)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := capability.OpenDeletePrimaryKeys(ctx, table, []string{"tenant_id", "item_id"})
	if err != nil {
		t.Fatalf("open SQL Server delete stable key scan empty=%t: %v", empty, err)
	}
	if !empty {
		if !rows.Next() {
			_ = rows.Close()
			t.Fatalf("nonempty SQL Server stable key scan had no rows: %v", rows.Err())
		}
		if _, err := rows.Values(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
	} else if rows.Next() {
		_ = rows.Close()
		t.Fatal("empty SQL Server stable key scan returned a row")
	}
	writeCtx, cancel := context.WithTimeout(ctx, 125*time.Millisecond)
	defer cancel()
	_, writeErr := database.ExecContext(
		writeCtx,
		"INSERT INTO "+sqlServerQualified(table.Schema, table.Name)+
			" ([tenant_id], [item_id], [payload]) VALUES (77, 77, 'blocked')",
	)
	if writeErr == nil {
		_ = rows.Close()
		t.Fatalf("SQL Server delete stable key scan empty=%t did not hold a table lock", empty)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close SQL Server delete stable key scan: %v", err)
	}
	if _, err := database.ExecContext(ctx,
		"INSERT INTO "+sqlServerQualified(table.Schema, table.Name)+
			" ([tenant_id], [item_id], [payload]) VALUES (77, 77, 'released')",
	); err != nil {
		t.Fatalf("SQL Server delete stable key lock did not release: %v", err)
	}
}
