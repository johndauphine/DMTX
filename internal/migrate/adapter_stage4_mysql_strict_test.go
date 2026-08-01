package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestMySQLStrictConfigurationAdmitsOnlyTableScopeAndSupportedTargets(t *testing.T) {
	for _, target := range []string{"postgres", "mysql", "mssql", "sqlite"} {
		t.Run(target, func(t *testing.T) {
			cfg := config.Config{Source: strictConsistencyTestEndpoint("mysql", "source"), Target: strictConsistencyTestEndpoint(target, "target")}
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
	cfg := config.Config{Source: strictConsistencyTestEndpoint("mysql", "source"), Target: strictConsistencyTestEndpoint("postgres", "target")}
	cfg.Migration.TargetMode, cfg.Migration.StrictConsistency = "upsert", true
	cfg.Migration.StrictConsistencyScope = config.StrictConsistencyMigration
	if err := ValidateMigration(cfg); err == nil || !strings.Contains(err.Error(), "table scope only") {
		t.Fatalf("migration scope error = %v", err)
	}
	if err := ValidateStage4ComposedConfiguration(cfg); err == nil || ClassifyTransferError(err) != ErrorClassPolicy || !strings.Contains(err.Error(), "table scope only") {
		t.Fatalf("composed migration scope error = %v", err)
	}
}

func TestConfigureStage4MySQLTableStrictSourcePoolReservesLockHolderAndReaders(t *testing.T) {
	database, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = database.Close() })
	source := &relationalSourceAdapter{
		spec:     relationalSourceSpec{engine: "mysql"},
		database: database,
	}
	resources := config.EffectiveTransferPlan{
		ConnectionLimit: config.EffectiveInt{Value: 4},
		Readers:         config.EffectiveInt{Value: 3},
	}
	if err := configureStage4MySQLTableStrictSourcePool(source, resources); err != nil {
		t.Fatalf("configure MySQL table-strict source pool: %v", err)
	}
	if got := database.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("MySQL table-strict source pool max connections=%d, want lock holder plus readers=4", got)
	}

	resources.ConnectionLimit.Value = 3
	if err := configureStage4MySQLTableStrictSourcePool(source, resources); err == nil {
		t.Fatal("configure MySQL table-strict source pool accepted lock holder plus readers beyond connection_limit")
	}
	if got := database.Stats().MaxOpenConnections; got != 4 {
		t.Fatalf("rejected MySQL table-strict pool reconfigured database to %d, want unchanged 4", got)
	}
}

func TestMySQLStrictCleanupContextIsBoundedAfterCancellation(t *testing.T) {
	caller, cancel := context.WithCancel(context.Background())
	cancel()
	cleanup, cleanupCancel := mysqlStrictCleanupContext(caller)
	defer cleanupCancel()
	if err := cleanup.Err(); err != nil {
		t.Fatalf("cleanup inherited cancellation: %v", err)
	}
	deadline, ok := cleanup.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > strictConsistencyCleanupTimeout {
		t.Fatalf("cleanup deadline = %v, ok=%t", deadline, ok)
	}
}

func TestMySQLStrictCleanupContextPreservesEarlierCallerDeadline(t *testing.T) {
	caller, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	cleanup, cleanupCancel := mysqlStrictCleanupContext(caller)
	defer cleanupCancel()
	deadline, ok := cleanup.Deadline()
	callerDeadline, callerOK := caller.Deadline()
	if !ok || !callerOK || deadline.After(callerDeadline) {
		t.Fatalf("cleanup deadline = %v (caller %v)", deadline, callerDeadline)
	}
}

func TestMySQLStrictCleanupContextReplacesExpiredCallerDeadline(t *testing.T) {
	caller, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(2 * time.Millisecond)
	cleanup, cleanupCancel := mysqlStrictCleanupContext(caller)
	defer cleanupCancel()
	if err := cleanup.Err(); err != nil {
		t.Fatalf("cleanup inherited expired deadline: %v", err)
	}
	deadline, ok := cleanup.Deadline()
	if !ok || time.Until(deadline) <= 0 {
		t.Fatalf("replacement cleanup deadline = %v, ok=%t", deadline, ok)
	}
}

func TestMySQLStrictViewTokenIsUniquePerPhysicalView(t *testing.T) {
	first := mysqlStrictViewToken("run-1", "mysql-process-1", "nonce-1")
	second := mysqlStrictViewToken("run-1", "mysql-process-1", "nonce-2")
	if first == second || !strings.HasPrefix(first, "mysql-view-") {
		t.Fatalf("view references first=%q second=%q", first, second)
	}
}

func TestMySQLStrictCloseDiscardsConnectionAfterManualRollbackFailure(t *testing.T) {
	state := &stableRollbackTestState{}
	driverName := fmt.Sprintf("dmtx_mysql_strict_rollback_%d", stableRollbackDriverSequence.Add(1))
	sql.Register(driverName, stableRollbackTestDriver{state: state})
	database, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	connection, err := database.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	session := &MySQLStrictConsistencySession{connections: []*sql.Conn{connection}, closeDone: make(chan struct{})}
	if err := session.Close(context.Background()); err == nil {
		t.Fatal("manual rollback failure unexpectedly succeeded")
	}
	if err := database.PingContext(context.Background()); err != nil {
		t.Fatalf("ping after discarded connection: %v", err)
	}
	opened, closed := state.counts()
	if opened != 2 || closed != 1 {
		t.Fatalf("connection counts after rollback discard = open:%d close:%d", opened, closed)
	}
}

func TestMySQLStrictBindingRejectsDuplicateAndPersistsSourceIdentity(t *testing.T) {
	task := state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: "inventory", Table: "items"}
	capture := StrictConsistencyCapture{Tables: []StrictConsistencyTableCapture{{Task: task, SnapshotReference: "mysql-view-0123456789abcdef", ExactSourceRowCount: 3}}}
	binding, err := newStage4MySQLStrictEpochBinding("mysql-process-1", capture)
	if err != nil {
		t.Fatal(err)
	}
	work, err := binding.finalizeWork(stage4AdapterWork{task: task, topology: "base", ranges: []state.RangeState{{}}})
	if err != nil {
		t.Fatal(err)
	}
	if work.topology == "base" || work.ranges[0].TopologyHash != work.topology {
		t.Fatalf("work topology = %#v", work)
	}
	_, err = newStage4MySQLStrictEpochBinding("mysql-process-1", StrictConsistencyCapture{Tables: append(capture.Tables, capture.Tables[0])})
	if err == nil || !strings.Contains(err.Error(), "duplicates") {
		t.Fatalf("duplicate capture error = %v", err)
	}
}

func TestMySQLStrictPlanningRestoresGlobalRangeCursor(t *testing.T) {
	execution := &stage4AdapterNetworkExecution{nextGlobalRange: 41}
	for _, returnErr := range []error{nil, errors.New("planned table close failed")} {
		plan := func() error {
			defer stage4MySQLStrictPlanningRangeCursor(execution)()
			execution.mu.Lock()
			execution.nextGlobalRange += 9
			execution.mu.Unlock()
			return returnErr
		}
		if err := plan(); !errors.Is(err, returnErr) {
			t.Fatalf("planning error = %v, want %v", err, returnErr)
		}
		execution.mu.Lock()
		got := execution.nextGlobalRange
		execution.mu.Unlock()
		if got != 41 {
			t.Fatalf("planning cursor after %v = %d, want 41", returnErr, got)
		}
	}
}
