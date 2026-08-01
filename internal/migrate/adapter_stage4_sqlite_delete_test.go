package migrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func openSQLiteDeleteCapabilityFixture(t *testing.T) (
	*sqliteSourceAdapter,
	*sqliteTargetAdapter,
	schema.Table,
	schema.Table,
	postgresDeleteReconciliationCapabilities,
	string,
) {
	t.Helper()
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	targetPath := filepath.Join(filepath.Dir(sourcePath), "target.db")
	for _, fixture := range []struct{ path, rows string }{
		{sourcePath, "(1, 'present')"},
		{targetPath, "(1, 'present'), (2, 'target-only')"},
	} {
		database, err := openSQLiteTargetDatabase(ctx, fixture.path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.ExecContext(ctx, "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, payload TEXT); INSERT INTO items VALUES "+fixture.rows); err != nil {
			database.Close()
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	sourceValue, err := openSQLiteSourceAdapter(ctx, config.Endpoint{Database: sourcePath})
	if err != nil {
		t.Fatal(err)
	}
	source := sourceValue.(*sqliteSourceAdapter)
	t.Cleanup(func() { _ = source.Close() })
	targetValue, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: targetPath})
	if err != nil {
		t.Fatal(err)
	}
	target := targetValue.(*sqliteTargetAdapter)
	t.Cleanup(func() { _ = target.Close() })
	sourceTable, err := source.InspectTable(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	targetTable, _, err := inspectSQLiteSchema(ctx, target.database, "items")
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := newStage4DeleteReconciliationCapabilities(ctx, source, target, sourceTable, targetTable)
	if err != nil {
		t.Fatal(err)
	}
	return source, target, sourceTable, targetTable, capabilities, targetPath
}

func sqliteDeleteFixtureBatch(table schema.Table) deleteTargetBatch {
	return deleteTargetBatch{
		Table: table, Columns: []string{"id"},
		PlanID: strings.Repeat("a", 32), Token: strings.Repeat("b", 64),
		Sequence: 0, BatchDigest: strings.Repeat("c", 64),
		Keys: [][]driver.Value{{int64(2)}},
	}
}

func TestSQLiteDeleteReceiptReplaysExactCommittedBatch(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	targetPath := filepath.Join(t.TempDir(), "target.db")
	for _, fixture := range []struct{ path, rows string }{
		{sourcePath, "(1, 'present')"},
		{targetPath, "(1, 'present'), (2, 'target-only')"},
	} {
		database, err := openSQLiteTargetDatabase(ctx, fixture.path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = database.ExecContext(ctx, "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, payload TEXT); INSERT INTO items VALUES "+fixture.rows)
		if err != nil {
			t.Fatal(err)
		}
		if err := database.Close(); err != nil {
			t.Fatal(err)
		}
	}
	source, err := openSQLiteSourceAdapter(ctx, config.Endpoint{Database: sourcePath})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	target, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: targetPath})
	if err != nil {
		t.Fatal(err)
	}
	sourceTable, err := source.InspectTable(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	targetTable, _, err := inspectSQLiteSchema(ctx, target.(*sqliteTargetAdapter).database, "items")
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := newStage4DeleteReconciliationCapabilities(ctx, source, target, sourceTable, targetTable)
	if err != nil {
		t.Fatal(err)
	}
	batch := deleteTargetBatch{
		Table: targetTable, Columns: []string{"id"},
		PlanID: strings.Repeat("a", 32), Token: strings.Repeat("b", 64),
		Sequence: 0, BatchDigest: strings.Repeat("c", 64),
		Keys: [][]driver.Value{{int64(2)}},
	}
	first, err := capabilities.target.ApplyDeleteBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if first.Candidates != 1 || first.DeletedRows != 1 {
		t.Fatalf("unexpected first receipt %#v", first)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: targetPath})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedCapabilities, err := newStage4DeleteReconciliationCapabilities(ctx, source, reopened, sourceTable, targetTable)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopenedCapabilities.target.ApplyDeleteBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, replayed) {
		t.Fatalf("replay receipt differs\nfirst=%#v\nreplayed=%#v", first, replayed)
	}
	var rows int
	if err := reopened.(*sqliteTargetAdapter).database.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("replayed delete changed committed result: rows=%d", rows)
	}
}

func TestSQLiteDeleteReceiptWriterReservationBlocksSchemaRace(t *testing.T) {
	ctx := context.Background()
	_, target, _, targetTable, capabilities, targetPath := openSQLiteDeleteCapabilityFixture(t)
	external, err := openSQLiteTargetDatabase(ctx, targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer external.Close()
	if _, err := external.ExecContext(ctx, "PRAGMA busy_timeout = 25"); err != nil {
		t.Fatal(err)
	}
	target.deleteAfterReservation = func(
		ctx context.Context,
		_ *sql.Conn,
	) error {
		raceCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		_, raceErr := external.ExecContext(
			raceCtx,
			"ALTER TABLE items ADD COLUMN raced TEXT",
		)
		if raceErr == nil {
			return errors.New("external schema writer entered while SQLite delete writer reservation was held")
		}
		return nil
	}
	if _, err := capabilities.target.ApplyDeleteBatch(ctx, sqliteDeleteFixtureBatch(targetTable)); err != nil {
		t.Fatal(err)
	}
	if err := validateSQLiteDeleteTableAuthority(ctx, target.database, targetTable); err != nil {
		t.Fatalf("external schema writer changed target despite writer reservation: %v", err)
	}
}

func TestSQLiteDeleteReceiptRejectsCatalogDriftAfterCapabilityAdmission(t *testing.T) {
	ctx := context.Background()
	_, target, _, targetTable, capabilities, _ := openSQLiteDeleteCapabilityFixture(t)
	if _, err := target.database.ExecContext(ctx, "ALTER TABLE items ADD COLUMN raced TEXT"); err != nil {
		t.Fatal(err)
	}
	if _, err := capabilities.target.ApplyDeleteBatch(ctx, sqliteDeleteFixtureBatch(targetTable)); err == nil || !strings.Contains(err.Error(), "writer-reserved SQLite delete target catalog") {
		t.Fatalf("catalog drift reached delete path: %v", err)
	}
	var remaining, journal int
	if err := target.database.QueryRowContext(ctx, "SELECT COUNT(*) FROM items WHERE id = 2").Scan(&remaining); err != nil || remaining != 1 {
		t.Fatalf("catalog drift mutated target row: remaining=%d err=%v", remaining, err)
	}
	if err := target.database.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND lower(name) = lower(?)", sqliteDeleteJournalTable).Scan(&journal); err != nil || journal != 0 {
		t.Fatalf("catalog drift created journal before rejection: journal=%d err=%v", journal, err)
	}
}

func TestSQLiteDeleteReceiptCommitAckRecoveryReturnsExactReceiptAfterReopen(t *testing.T) {
	ctx := context.Background()
	source, target, sourceTable, targetTable, capabilities, targetPath := openSQLiteDeleteCapabilityFixture(t)
	target.deleteCommit = func(ctx context.Context, connection *sql.Conn) (sql.Result, error) {
		result, err := connection.ExecContext(ctx, "COMMIT")
		if err != nil {
			return result, err
		}
		return result, errors.New("injected SQLite commit acknowledgement loss")
	}
	batch := sqliteDeleteFixtureBatch(targetTable)
	first, err := capabilities.target.ApplyDeleteBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	target.deleteCommit = nil
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	reopenedValue, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: targetPath})
	if err != nil {
		t.Fatal(err)
	}
	reopened := reopenedValue.(*sqliteTargetAdapter)
	defer reopened.Close()
	reopenedCapabilities, err := newStage4DeleteReconciliationCapabilities(ctx, source, reopened, sourceTable, targetTable)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := reopenedCapabilities.target.ApplyDeleteBatch(ctx, batch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, replayed) {
		t.Fatalf("commit-ack replay receipt differs\nfirst=%#v\nreplayed=%#v", first, replayed)
	}
}

func TestSQLiteDeleteJournalAdmissionRejectsCollisionsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	for name, journalDDL := range map[string]string{
		"same_columns_token_not_unique": "CREATE TABLE dmtx_internal_delete_batch_receipts (token TEXT NOT NULL, plan_id TEXT NOT NULL, sequence INTEGER NOT NULL, batch_digest TEXT NOT NULL, candidates INTEGER NOT NULL, deleted_rows INTEGER NOT NULL, receipt_digest TEXT NOT NULL)",
		"same_name_view":                "CREATE VIEW dmtx_internal_delete_batch_receipts AS SELECT 1 AS token",
		"exact_table_with_trigger":      sqliteDeleteJournalCreateSQL + "; CREATE TRIGGER dmtx_delete_receipt_watch AFTER INSERT ON dmtx_internal_delete_batch_receipts BEGIN SELECT 1; END",
	} {
		t.Run(name, func(t *testing.T) {
			sourcePath := filepath.Join(t.TempDir(), "source.db")
			targetPath := filepath.Join(t.TempDir(), "target.db")
			for _, fixture := range []struct{ path, ddl string }{
				{sourcePath, "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY)"},
				{targetPath, "CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY); " + journalDDL},
			} {
				database, err := openSQLiteTargetDatabase(ctx, fixture.path)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := database.ExecContext(ctx, fixture.ddl); err != nil {
					database.Close()
					t.Fatal(err)
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
			}
			source, err := openSQLiteSourceAdapter(ctx, config.Endpoint{Database: sourcePath})
			if err != nil {
				t.Fatal(err)
			}
			defer source.Close()
			target, err := openSQLiteTargetAdapter(ctx, config.Endpoint{Database: targetPath})
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			sourceTable, err := source.InspectTable(ctx, "items")
			if err != nil {
				t.Fatal(err)
			}
			targetTable, _, err := inspectSQLiteSchema(ctx, target.(*sqliteTargetAdapter).database, "items")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := newSQLiteDeleteReconciliationCapabilities(ctx, source, target, sourceTable, targetTable); err == nil || !strings.Contains(err.Error(), "receipt journal") {
				t.Fatalf("colliding journal was admitted: %v", err)
			}
			var count int
			if err := target.(*sqliteTargetAdapter).database.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&count); err != nil || count != 0 {
				t.Fatalf("admission mutated target data: count=%d err=%v", count, err)
			}
		})
	}
}

func TestSQLiteSourceOmitsPrivateDeleteReceiptJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	database, err := openSQLiteTargetDatabase(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY); "+sqliteDeleteJournalCreateSQL); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := openSQLiteSourceAdapter(ctx, config.Endpoint{Database: path})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	tables, err := source.ListTables(ctx)
	if err != nil || !reflect.DeepEqual(tables, []string{"items"}) {
		t.Fatalf("private journal source inventory = %#v err=%v", tables, err)
	}
	if _, err := source.InspectTable(ctx, sqliteDeleteJournalTable); err == nil || !strings.Contains(err.Error(), "private DMTX") {
		t.Fatalf("private journal inspect error = %v", err)
	}
}

func TestSQLiteToSQLiteCompatibilityRouteOmitsPrivateDeleteReceiptJournal(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := openSQLiteTargetDatabase(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.ExecContext(ctx, `CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT); INSERT INTO items VALUES (1, 'user-row'); CREATE TABLE DMTX_INTERNAL_DELETE_BATCH_RECEIPTS (token TEXT PRIMARY KEY)`); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	result, err := SQLiteToSQLiteWithObserver(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 1 {
		t.Fatalf("compatibility route copied private receipt state: %#v", result)
	}
	target, err := openSQLiteTargetDatabase(ctx, targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	var items, private int
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM items").Scan(&items); err != nil || items != 1 {
		t.Fatalf("copied user table count=%d err=%v", items, err)
	}
	if err := target.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND lower(name) = lower(?)", sqliteDeleteJournalTable).Scan(&private); err != nil || private != 0 {
		t.Fatalf("compatibility route materialized private receipt table: count=%d err=%v", private, err)
	}
}

func TestStage4SQLiteDeleteReconcileEvolvesAbsentTargetForDueZero(
	t *testing.T,
) {
	ctx := context.Background()
	for backendName, newBackend := range stage4LifecycleBackendFactories() {
		backendName, newBackend := backendName, newBackend
		t.Run(backendName, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.sqlite")
			targetPath := filepath.Join(directory, "target.sqlite")
			sourceDatabase, err := openSQLiteTargetDatabase(ctx, sourcePath)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := sourceDatabase.ExecContext(
				ctx,
				"CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL); INSERT INTO items(id, payload) VALUES (1, 'source-row')",
			); err != nil {
				_ = sourceDatabase.Close()
				t.Fatal(err)
			}
			if err := sourceDatabase.Close(); err != nil {
				t.Fatal(err)
			}

			sourceValue, err := openSQLiteSourceAdapter(
				ctx,
				config.Endpoint{Type: "sqlite", Database: sourcePath},
			)
			if err != nil {
				t.Fatal(err)
			}
			source := sourceValue.(*sqliteSourceAdapter)
			t.Cleanup(func() { _ = source.Close() })
			targetValue, err := openSQLiteTargetAdapter(
				ctx,
				config.Endpoint{Type: "sqlite", Database: targetPath},
			)
			if err != nil {
				t.Fatal(err)
			}
			target := targetValue.(*sqliteTargetAdapter)
			t.Cleanup(func() { _ = target.Close() })

			backend := newBackend(t)
			runID := "sqlite-delete-evolve-absent-" + backendName
			started := time.Now().UTC().Add(-time.Minute)
			initializeStage4LifecycleRun(t, backend, runID, started)
			events := make([]string, 0)
			observer := stage4AdapterObserver{
				recordingTableObserver: recordingTableObserver{events: &events},
				run:                    stage4LifecycleRunContext(t, backend, runID, false),
			}
			cfg := stage4AdapterTestConfig(t, "source-password", "target-password")
			cfg.Source = config.Endpoint{Type: "sqlite", Database: sourcePath}
			cfg.Target = config.Endpoint{Type: "sqlite", Database: targetPath}
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.IncludeTables = []string{"items"}
			cfg.Migration.Partitions = 1
			cfg.Migration.MemoryCeilingBytes = 64 << 20
			cfg.Migration.SchemaContract = &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			}
			cfg.Migration.Deletes = config.DeletePolicy{
				Mode:           config.DeleteModeReconcile,
				TargetBehavior: config.DeleteTargetHard,
				Reconcile: config.DeleteReconcilePolicy{
					Schedule:          config.DeleteScheduleInterval,
					Interval:          time.Hour,
					BatchSize:         1,
					RequirePrimaryKey: true,
				},
			}
			result, err := migrateWithStage4Adapters(
				ctx,
				cfg,
				observer,
				source,
				target,
				"upsert",
				observer.run,
			)
			if err != nil {
				t.Fatalf("run absent-target due-zero delete reconciliation: %v", err)
			}
			if result != (Result{Tables: 1, Rows: 1, Validated: true}) {
				t.Fatalf("absent-target due-zero result = %#v", result)
			}
			var rows int
			if err := target.database.QueryRowContext(
				ctx,
				"SELECT COUNT(*) FROM items",
			).Scan(&rows); err != nil || rows != 1 {
				t.Fatalf("evolved target items rows = %d, %v", rows, err)
			}
			record, found, err := backend.LoadLatestSuccessfulDeleteReconciliation(
				runID,
				state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"},
			)
			if err != nil || !found {
				t.Fatalf("load absent-target due-zero reconciliation found=%v record=%#v err=%v", found, record, err)
			}
			if record.Status != state.DeleteReconciliationCompleted ||
				record.Candidates != 0 || record.DeletedRows != 0 ||
				record.CommittedBatches != 0 {
				t.Fatalf("absent-target due-zero reconciliation = %#v", record)
			}
			assertStage4TerminalSchemaSentinels(t, backend, runID, false)
			published, err := PublishStage4RunCompletion(
				ctx,
				observer.run,
				"SQLite delete reconciliation completed",
				time.Now().UTC(),
			)
			if err != nil || !published {
				t.Fatalf(
					"publish aggregate SQLite delete completion published=%t err=%v",
					published,
					err,
				)
			}
			assertStage4TerminalSchemaSentinels(t, backend, runID, true)
		})
	}
}

func TestStage4DeleteCapabilityGateKeepsUnimplementedCellsClosed(t *testing.T) {
	table := schema.Table{Name: "items", Columns: []schema.Column{{Name: "id", Type: "integer", PrimaryKey: true, PrimaryKeyPosition: 1}}}
	prepared := stage4AdapterPrepared{mode: "upsert", plans: []adapterTablePlan{{source: table, target: table}}, work: []stage4AdapterWork{{task: state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"}, strategy: stage4AdapterCopyStrategy, topology: "topology"}}}
	cfg := config.Config{Migration: config.Migration{TargetMode: "upsert", Deletes: config.DeletePolicy{Mode: config.DeleteModeReconcile, TargetBehavior: config.DeleteTargetHard, Reconcile: config.DeleteReconcilePolicy{Schedule: config.DeleteScheduleInterval, Interval: time.Hour, BatchSize: 1, RequirePrimaryKey: true}}}}
	for _, source := range []string{"postgres", "mysql", "mssql", "sqlite"} {
		for _, target := range []string{"postgres", "mysql", "mssql", "sqlite"} {
			err := requireStage4AdapterPostgresDeleteComposition(cfg, source, target, prepared)
			certified := (source == "postgres" && target == "postgres") ||
				(source == "sqlite" && target == "sqlite") ||
				(source == "mysql" && target == "mysql") ||
				(source == "mssql" && target == "mssql")
			if certified && err != nil {
				t.Fatalf("certified %s-to-%s cell was refused: %v", source, target, err)
			}
			if !certified && (err == nil || !strings.Contains(err.Error(), "atomic-receipt")) {
				t.Fatalf("uncertified %s-to-%s cell unexpectedly admitted: %v", source, target, err)
			}
		}
	}
}
