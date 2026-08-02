package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestSQLiteStrictConfigurationAdmitsOnlyTableScopeAndSupportedTargets(t *testing.T) {
	for _, target := range []string{"postgres", "mysql", "mssql", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			cfg := config.Config{
				Source: strictConsistencyTestEndpoint("sqlite", "source"),
				Target: strictConsistencyTestEndpoint(target, "target"),
			}
			cfg.Migration.TargetMode = "upsert"
			cfg.Migration.StrictConsistency = true
			cfg.Migration.StrictConsistencyScope = config.StrictConsistencyTable
			if err := ValidateMigration(cfg); err != nil {
				t.Fatalf("ValidateMigration: %v", err)
			}
			if err := ValidateStage4ComposedConfiguration(cfg); err != nil {
				t.Fatalf("composed admission: %v", err)
			}
		})
	}
	cfg := config.Config{
		Source: strictConsistencyTestEndpoint("sqlite", "source"),
		Target: strictConsistencyTestEndpoint("postgres", "target"),
	}
	cfg.Migration.TargetMode = "upsert"
	cfg.Migration.StrictConsistency = true
	cfg.Migration.StrictConsistencyScope = config.StrictConsistencyMigration
	if err := ValidateMigration(cfg); err == nil || !strings.Contains(err.Error(), "table scope only") {
		t.Fatalf("migration scope error = %v", err)
	}
	if err := ValidateStage4ComposedConfiguration(cfg); err == nil || ClassifyTransferError(err) != ErrorClassPolicy || !strings.Contains(err.Error(), "table scope only") {
		t.Fatalf("composed migration scope error = %v", err)
	}
}

func TestSQLiteStrictMigrationScopeRefusesBeforeEndpointMutation(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY)`); err != nil {
		source.Close()
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{
			TargetMode:             "upsert",
			StrictConsistency:      true,
			StrictConsistencyScope: config.StrictConsistencyMigration,
		},
	}
	result, err := Execute(context.Background(), cfg, nil)
	if result != (Result{}) || err == nil ||
		!strings.Contains(err.Error(), "table scope only") {
		t.Fatalf("migration scope result=%#v err=%v", result, err)
	}
	if _, statErr := os.Stat(targetPath); !os.IsNotExist(statErr) {
		t.Fatalf("migration scope refusal opened or created target: %v", statErr)
	}
}

func TestSQLiteStrictSourcePreflightPrecedesCheckpointAndTargetMutation(t *testing.T) {
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "closed.db"))
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	events := []string{}
	target := &recordingAdapterTarget{events: &events}
	result, err := migrateWithStage4SQLiteStrictAdapters(
		context.Background(),
		config.Config{Migration: config.Migration{
			TargetMode:             "upsert",
			StrictConsistency:      true,
			StrictConsistencyScope: config.StrictConsistencyTable,
		}},
		recordingTableObserver{events: &events},
		&sqliteSourceAdapter{database: database},
		target,
		stage4AdapterPrepared{mode: "upsert", plans: []adapterTablePlan{{source: schema.Table{Name: "items"}}}},
		&stage4AdapterNetworkExecution{deferred: true},
		false,
		nil,
	)
	if result != (Result{}) || err == nil ||
		!strings.Contains(err.Error(), "preflight") {
		t.Fatalf("preflight result=%#v err=%v", result, err)
	}
	if len(events) != 0 || len(target.prepared) != 0 ||
		len(target.written) != 0 {
		t.Fatalf("failed strict source preflight crossed checkpoint or target mutation: events=%#v target=%#v", events, target)
	}
}

func TestSQLiteStrictBindingAndPlanningCursor(t *testing.T) {
	task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Table: "items"}
	capture := StrictConsistencyCapture{Tables: []StrictConsistencyTableCapture{{
		Task:                task,
		SnapshotReference:   "sqlite-view-0123456789abcdef",
		ExactSourceRowCount: 3,
	}}}
	binding, err := newStage4SQLiteStrictEpochBinding("sqlite-process-1", capture)
	if err != nil {
		t.Fatal(err)
	}
	work, err := binding.finalizeWork(stage4AdapterWork{
		task: task, topology: "base", ranges: []state.RangeState{{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if work.topology == "base" || work.ranges[0].TopologyHash != work.topology {
		t.Fatalf("work topology = %#v", work)
	}
	if _, err := newStage4SQLiteStrictEpochBinding("sqlite-process-1", StrictConsistencyCapture{Tables: append(capture.Tables, capture.Tables[0])}); err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("duplicate capture error = %v", err)
	}

	execution := &stage4AdapterNetworkExecution{nextGlobalRange: 17}
	for _, returnErr := range []error{nil, errors.New("planned table close failed")} {
		plan := func() error {
			defer stage4SQLiteStrictPlanningRangeCursor(execution)()
			execution.mu.Lock()
			execution.nextGlobalRange += 5
			execution.mu.Unlock()
			return returnErr
		}
		if err := plan(); !errors.Is(err, returnErr) {
			t.Fatalf("planning error = %v, want %v", err, returnErr)
		}
		execution.mu.Lock()
		got := execution.nextGlobalRange
		execution.mu.Unlock()
		if got != 17 {
			t.Fatalf("planning cursor after %v = %d, want 17", returnErr, got)
		}
	}
}

func TestSQLiteStrictStableReaderUsesPinnedTransaction(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "source.db")
	setup, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()
	if _, err := setup.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'before-view');
	`); err != nil {
		t.Fatal(err)
	}
	raw, err := openSQLiteSourceAdapter(ctx, config.Endpoint{Type: "sqlite", Database: path})
	if err != nil {
		t.Fatal(err)
	}
	source := raw.(*sqliteSourceAdapter)
	defer source.Close()
	table, err := source.InspectTable(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	opener, err := NewSQLiteStrictConsistencyOpener(source.database)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := opener.OpenStrictConsistency(ctx, sqliteStrictRequest("items"))
	if err != nil {
		t.Fatal(err)
	}
	session := opened.(*SQLiteStrictConsistencySession)
	defer func() {
		if closeErr := session.Close(context.Background()); closeErr != nil {
			t.Error(closeErr)
		}
	}()
	if _, err := session.CaptureSameViewEvidence(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := setup.ExecContext(ctx, `INSERT INTO items VALUES (2, 'after-view')`); err != nil {
		t.Fatalf("commit concurrent WAL mutation: %v", err)
	}
	if err := RunSQLiteAdapterStableNetworkReader(ctx, session, state.TaskKey{Type: "table-copy", Table: "items"}, source, table, func(readerCtx context.Context, stable adapterStableNetworkSource) error {
		catalog, err := stable.InspectTable(readerCtx, "items")
		if err != nil {
			return err
		}
		if !stage4AdapterNetworkCatalogsMatch(t, catalog, table) {
			return errors.New("stable reader catalog differs from the pinned source catalog")
		}
		count, err := stable.CountRows(readerCtx, table)
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("stable count = %d, want 1", count)
		}
		plan, err := stable.PlanPagination(readerCtx, table, 1)
		if err != nil {
			return err
		}
		width, err := stable.PlanRetainedRowWidth(readerCtx, table, []string{"id", "payload"})
		if err != nil {
			return err
		}
		page, err := stable.ReadNetworkRangePage(readerCtx, table, []string{"id", "payload"}, plan, plan.Ranges[0], stableRangePageRequest(table.Schema, table.Name, plan, 0, width.UpperBoundBytes, 16))
		if err != nil {
			return err
		}
		if len(page.Rows) != 1 || page.Rows[0][0] != int64(1) {
			return fmt.Errorf("stable rows = %#v, want only the pre-view row", page.Rows)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func stage4AdapterNetworkCatalogsMatch(t *testing.T, left, right schema.Table) bool {
	t.Helper()
	equal, err := stage4AdapterNetworkCatalogEqual(left, right)
	if err != nil {
		t.Fatalf("compare stable catalogs: %v", err)
	}
	return equal
}

type sqliteStrictMutationObserver struct {
	stage4AdapterObserver
	mutate func() error
	once   sync.Once
	err    error
}

func (observer *sqliteStrictMutationObserver) BeforeTable(
	ctx context.Context,
	table string,
) error {
	if err := observer.stage4AdapterObserver.BeforeTable(ctx, table); err != nil {
		return err
	}
	observer.once.Do(func() { observer.err = observer.mutate() })
	return observer.err
}

// TestStage4SQLiteStrictComposedWAL excludes a post-view concurrent source
// commit from both transfer and validation. It uses the production adapter
// route, a file-backed WAL source, and real aggregate state. BeforeTable is
// deliberately late: strict evidence and exact planning already exist, while
// no source page has yet been transferred.
func TestStage4SQLiteStrictComposedWAL(t *testing.T) {
	for name, newBackend := range map[string]func(string) stage4LiveStateBackend{
		"yaml": func(path string) stage4LiveStateBackend {
			return state.YAMLStore{Path: path}
		},
		"sqlite": func(path string) stage4LiveStateBackend {
			return state.SQLiteStore{Path: path}
		},
	} {
		t.Run(name, func(t *testing.T) {
			directory := t.TempDir()
			testStage4SQLiteStrictComposedWAL(
				t,
				directory,
				newBackend(filepath.Join(directory, "state."+name)),
				filepath.Join(directory, "state."+name),
			)
		})
	}
}

func testStage4SQLiteStrictComposedWAL(
	t *testing.T,
	directory string,
	backend stage4LiveStateBackend,
	statePath string,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	sourceSetup, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sourceSetup.ExecContext(ctx, `
		PRAGMA journal_mode = WAL;
		CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL);
		INSERT INTO items VALUES (1, 'one'), (2, 'two'), (3, 'three');
	`); err != nil {
		sourceSetup.Close()
		t.Fatal(err)
	}
	if err := sourceSetup.Close(); err != nil {
		t.Fatal(err)
	}
	targetSetup, err := openSQLiteTargetDatabase(ctx, targetPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := targetSetup.ExecContext(ctx, `CREATE TABLE items (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)`); err != nil {
		targetSetup.Close()
		t.Fatal(err)
	}
	if err := targetSetup.Close(); err != nil {
		t.Fatal(err)
	}
	writer, err := sql.Open("sqlite", sqliteSourceTestURI(sourcePath, "rw"))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()

	runID := "stage4-sqlite-strict-wal"
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         "source",
		Target:         "target",
		SourceEngine:   "sqlite",
		SourceIdentity: "sqlite:" + sourcePath,
		TargetIdentity: "sqlite:" + targetPath,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      time.Now().UTC().Add(-time.Minute),
	}, "configuration-"+runID); err != nil {
		t.Fatal(err)
	}
	config := config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{
			TargetMode:             "upsert",
			IncludeTables:          []string{"items"},
			ConnectionLimit:        3,
			Workers:                2,
			ChunkSize:              1,
			Partitions:             1,
			ReaderParallelism:      4,
			WriterParallelism:      1,
			ReadAhead:              1,
			MaxRetries:             0,
			StrictConsistency:      true,
			StrictConsistencyScope: config.StrictConsistencyTable,
			Validation:             config.ValidationPolicy{Mode: "count_only"},
		},
	}
	events := []string{}
	observer := &sqliteStrictMutationObserver{
		stage4AdapterObserver: stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run: Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			},
		},
		mutate: func() error {
			_, err := writer.ExecContext(ctx, `INSERT INTO items VALUES (4, 'after-view')`)
			return err
		},
	}
	result, err := Execute(ctx, config, observer)
	if err != nil {
		t.Fatalf("composed SQLite strict run: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("result = %#v, want one validated three-row table", result)
	}
	var sourceCount, targetCount int
	if err := writer.QueryRowContext(ctx, `SELECT COUNT(*) FROM items`).Scan(&sourceCount); err != nil || sourceCount != 4 {
		t.Fatalf("source count after concurrent commit = %d, err=%v", sourceCount, err)
	}
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM items`).Scan(&targetCount); err != nil || targetCount != 3 {
		t.Fatalf("target count = %d, err=%v; strict validation must use the persisted view count", targetCount, err)
	}
	var copiedAfterView int
	if err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM items WHERE id = 4`).Scan(&copiedAfterView); err != nil || copiedAfterView != 0 {
		t.Fatalf("post-view source row reached target: count=%d err=%v", copiedAfterView, err)
	}
	work, ranges, err := backend.ListWork(runID)
	if err != nil {
		t.Fatal(err)
	}
	var networkWork state.WorkTask
	for _, candidate := range work {
		if candidate.Key.Type == stage4AdapterNetworkTaskType &&
			candidate.Key.Table == "items" {
			networkWork = candidate
		}
	}
	if networkWork.Status != "completed" {
		t.Fatalf("durable work = %#v", work)
	}
	// The strict attempt is bound before any page is issued, when the durable
	// work plan has attempt counter zero. WorkTask.Attempts is subsequently
	// incremented by the three one-row range delivery attempts; it is not a
	// second EnsureWorkPlan authority and must never retroactively change the
	// immutable same-view evidence identity.
	if networkWork.Attempts != result.Rows {
		t.Fatalf("network work delivery attempts = %d, want one per copied row (%d)", networkWork.Attempts, result.Rows)
	}
	var networkRange state.RangeState
	for _, candidate := range ranges {
		if candidate.Task == networkWork.Key {
			networkRange = candidate
		}
	}
	if networkRange.ID == "" || networkRange.Attempts != networkWork.Attempts {
		t.Fatalf("network range attempt accounting = %#v, work=%#v", networkRange, networkWork)
	}
	attempt, err := BuildStrictConsistencyAttemptID(networkWork.Key, networkWork.TopologyHash, 0)
	if err != nil {
		t.Fatal(err)
	}
	evidence, found, err := backend.LoadStrictSnapshotEvidence(runID, networkWork.Key, attempt)
	if err != nil || !found || evidence.AttemptID != attempt || evidence.ExactSourceRowCount != 3 || evidence.SourceEngine != "sqlite" || evidence.Scope != state.StrictSnapshotTable || evidence.ProcessEpoch == "" || evidence.SnapshotReference == "" {
		t.Fatalf("strict evidence found=%t value=%#v err=%v", found, evidence, err)
	}
	// A completed table is resumed from its immutable evidence rather than a
	// later current-source count. The WAL writer has already committed row 4,
	// so this also proves the resume path does not reopen an ordinary source
	// snapshot merely to revalidate a completed strict table.
	observer.run.Resume = true
	resumed, err := ExecuteResume(
		ctx,
		config,
		CompletedTableCheckpoints{"items": {Rows: 3}},
		observer,
	)
	if err != nil {
		t.Fatalf("resume completed SQLite strict WAL table: %v", err)
	}
	if resumed != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("resume result = %#v, want one validated three-row table", resumed)
	}
	resumedEvidence, found, err := backend.LoadStrictSnapshotEvidence(runID, networkWork.Key, attempt)
	if err != nil || !found || !reflect.DeepEqual(resumedEvidence, evidence) {
		t.Fatalf("resume changed immutable strict evidence: found=%t evidence=%#v prior=%#v err=%v", found, resumedEvidence, evidence, err)
	}
	if err := target.QueryRowContext(ctx, `SELECT COUNT(*) FROM items`).Scan(&targetCount); err != nil || targetCount != 3 {
		t.Fatalf("target count after strict resume = %d, err=%v", targetCount, err)
	}
	for _, path := range []string{targetPath + "-wal", targetPath + "-shm", targetPath + "-journal", statePath + "-wal", statePath + "-shm", statePath + "-journal"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("strict composition left an unexpected target/state artifact %s: %v", path, err)
		}
	}
}
