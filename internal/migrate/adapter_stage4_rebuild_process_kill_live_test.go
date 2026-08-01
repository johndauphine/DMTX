package migrate

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	stage4RebuildProcessKillCellEnv       = "DMTX_STAGE4_REBUILD_PROCESS_KILL_CELL"
	stage4RebuildProcessKillStateKindEnv  = "DMTX_STAGE4_REBUILD_PROCESS_KILL_STATE_KIND"
	stage4RebuildProcessKillStatePathEnv  = "DMTX_STAGE4_REBUILD_PROCESS_KILL_STATE_PATH"
	stage4RebuildProcessKillConfigPathEnv = "DMTX_STAGE4_REBUILD_PROCESS_KILL_CONFIG_PATH"
	stage4RebuildProcessKillRunIDEnv      = "DMTX_STAGE4_REBUILD_PROCESS_KILL_RUN_ID"
	stage4RebuildProcessKillSpoolEnv      = "DMTX_STAGE4_REBUILD_PROCESS_KILL_SPOOL"
	stage4RebuildProcessKillEventEnv      = "DMTX_STAGE4_REBUILD_PROCESS_KILL_EVENT"
	stage4RebuildProcessKillStatusEnv     = "DMTX_STAGE4_REBUILD_PROCESS_KILL_STATUS"
)

// stage4RebuildProcessKillStore deliberately includes the complete durable
// aggregate/recovery surface. The helper intercepts only the state save after
// native finalization and validation have returned, giving the parent a real
// process-loss boundary for the exact problem this test covers.
type stage4RebuildProcessKillStore interface {
	state.Backend
	Stage4StateBackend
	state.Stage4AggregateBackend
	state.Stage4RebuildRecoveryBackend
}

type stage4RebuildProcessKillBlockingBackend struct {
	stage4RebuildProcessKillStore
	eventPath string
}

func (backend *stage4RebuildProcessKillBlockingBackend) SaveStage4RebuildReady(
	state.Stage4RebuildReady,
) error {
	if err := os.WriteFile(
		backend.eventPath,
		[]byte("native-finalize-and-validate-complete\n"),
		0o600,
	); err != nil {
		return err
	}
	select {}
}

type stage4RebuildProcessKillFixture struct {
	cell         string
	cfg          config.Config
	parent       string
	child        string
	mutateSource func(context.Context) error
	assertTarget func(*testing.T, context.Context, bool)
}

// stage4RebuildProcessKillRoute is deliberately not config.Config. Config
// carries unexported parsed-provenance state which JSON cannot preserve; the
// child reconstructs the parsed config from this minimal private payload.
type stage4RebuildProcessKillRoute struct {
	Source config.Endpoint `json:"source"`
	Target config.Endpoint `json:"target"`
	Parent string          `json:"parent"`
	Child  string          `json:"child"`
}

// TestStage4RebuildFinalizeProcessKillLive is the process-loss proof for the
// terminal rebuild boundary. Every admitted target family runs twice, once
// against YAML replacement state and once against SQLite transactional state.
// The child dies after real target FinalizeTables and validation, but before
// the terminal-ready receipt. Resume must authenticate the finished target and
// publish the original run without another drop/recreate or finalizer call.
func TestStage4RebuildFinalizeProcessKillLive(t *testing.T) {
	cells := []struct {
		name  string
		setup func(*testing.T) stage4RebuildProcessKillFixture
	}{
		{name: "postgres", setup: stage4RebuildProcessKillPostgresFixture},
		{name: "mysql80", setup: func(t *testing.T) stage4RebuildProcessKillFixture {
			return stage4RebuildProcessKillMySQLFixture(t, "mysql80")
		}},
		{name: "mariadb1011", setup: func(t *testing.T) stage4RebuildProcessKillFixture {
			return stage4RebuildProcessKillMySQLFixture(t, "mariadb1011")
		}},
		{name: "mssql", setup: stage4RebuildProcessKillSQLServerFixture},
		{name: "sqlite", setup: stage4RebuildProcessKillSQLiteFixture},
	}
	for _, cell := range cells {
		cell := cell
		for _, stateKind := range []string{"yaml", "sqlite"} {
			stateKind := stateKind
			t.Run(cell.name+"/"+stateKind, func(t *testing.T) {
				fixture := cell.setup(t)
				stage4RunRebuildProcessKill(t, fixture, stateKind)
			})
		}
	}
}

// TestStage4RebuildFinalizeProcessKillHelperProcess is parent-only coverage.
// Credentials are kept in a private JSON file; statuses intentionally never
// include driver output or endpoint facts.
func TestStage4RebuildFinalizeProcessKillHelperProcess(t *testing.T) {
	if os.Getenv(stage4RebuildProcessKillCellEnv) == "" {
		return
	}
	stage4WriteRebuildProcessKillStatus("started")
	encoded, err := os.ReadFile(os.Getenv(stage4RebuildProcessKillConfigPathEnv))
	if err != nil {
		stage4WriteRebuildProcessKillStatus("config-read-failed")
		t.Fatal("read process-kill rebuild configuration")
	}
	var route stage4RebuildProcessKillRoute
	if err := json.Unmarshal(encoded, &route); err != nil {
		stage4WriteRebuildProcessKillStatus("config-decode-failed")
		t.Fatal("decode process-kill rebuild configuration")
	}
	cfg, err := newStage4RebuildProcessKillConfig(
		route.Source,
		route.Target,
		route.Parent,
		route.Child,
	)
	if err != nil {
		stage4WriteRebuildProcessKillStatus("config-parse-failed")
		t.Fatal("parse process-kill rebuild configuration")
	}
	store, err := stage4RebuildProcessKillOpenStore(
		os.Getenv(stage4RebuildProcessKillStateKindEnv),
		os.Getenv(stage4RebuildProcessKillStatePathEnv),
	)
	if err != nil {
		stage4WriteRebuildProcessKillStatus("state-open-failed")
		t.Fatal("open process-kill rebuild state")
	}
	runID, spool := os.Getenv(stage4RebuildProcessKillRunIDEnv), os.Getenv(stage4RebuildProcessKillSpoolEnv)
	if runID == "" || spool == "" {
		stage4WriteRebuildProcessKillStatus("missing-run-context")
		t.Fatal("process-kill rebuild child lacks run context")
	}
	blocking := &stage4RebuildProcessKillBlockingBackend{
		stage4RebuildProcessKillStore: store,
		eventPath:                     os.Getenv(stage4RebuildProcessKillEventEnv),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        blocking,
			SpoolDirectory: spool,
		},
	}
	stage4WriteRebuildProcessKillStatus("route-dispatched")
	if _, err := Execute(ctx, cfg, observer); err != nil {
		stage4WriteRebuildProcessKillStatus("route-returned-" + string(ClassifyTransferError(err)) + "-" + stage4RebuildProcessKillSafeError(err, cfg))
		t.Fatal("rebuild route returned before terminal-ready process-kill boundary")
	}
	stage4WriteRebuildProcessKillStatus("route-completed")
	t.Fatal("rebuild route completed before terminal-ready process-kill boundary")
}

func stage4RunRebuildProcessKill(
	t *testing.T,
	fixture stage4RebuildProcessKillFixture,
	stateKind string,
) {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal("resolve rebuild process-kill test directory")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal("protect rebuild process-kill test directory")
	}
	statePath := filepath.Join(root, "state."+stateKind)
	configPath := filepath.Join(root, "route.json")
	eventPath := filepath.Join(root, "finalized")
	statusPath := filepath.Join(root, "child-status")
	encoded, err := json.Marshal(stage4RebuildProcessKillRoute{
		Source: fixture.cfg.Source,
		Target: fixture.cfg.Target,
		Parent: fixture.parent,
		Child:  fixture.child,
	})
	if err != nil {
		t.Fatalf("encode %s rebuild route configuration: %T", fixture.cell, err)
	}
	if err := os.WriteFile(configPath, encoded, 0o600); err != nil {
		t.Fatal("write private rebuild route configuration")
	}
	runID := "stage4-rebuild-process-kill-" + fixture.cell + "-" + stateKind + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	spool := stage4RebuildProcessKillSpool(t, root, runID)
	store, err := stage4RebuildProcessKillOpenStore(stateKind, statePath)
	if err != nil {
		t.Fatalf("open %s rebuild process-kill state: %T", stateKind, err)
	}
	stage4RebuildProcessKillInitializeRun(t, store, runID, fixture.cfg)

	command := exec.Command(os.Args[0], "-test.run=^TestStage4RebuildFinalizeProcessKillHelperProcess$")
	command.Env = append(os.Environ(),
		stage4RebuildProcessKillCellEnv+"="+fixture.cell,
		stage4RebuildProcessKillStateKindEnv+"="+stateKind,
		stage4RebuildProcessKillStatePathEnv+"="+statePath,
		stage4RebuildProcessKillConfigPathEnv+"="+configPath,
		stage4RebuildProcessKillRunIDEnv+"="+runID,
		stage4RebuildProcessKillSpoolEnv+"="+spool,
		stage4RebuildProcessKillEventEnv+"="+eventPath,
		stage4RebuildProcessKillStatusEnv+"="+statusPath,
	)
	if err := command.Start(); err != nil {
		t.Fatal("start rebuild process-kill child")
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	reaped := false
	t.Cleanup(func() {
		if reaped {
			return
		}
		_ = command.Process.Kill()
		<-wait
	})
	stage4WaitForRebuildProcessKillBoundary(t, eventPath, statusPath, wait, &reaped)
	if err := command.Process.Kill(); err != nil {
		waitErr := <-wait
		reaped = true
		t.Fatalf("kill rebuild process-kill child: %T / %T", err, waitErr)
	}
	if err := <-wait; err == nil {
		reaped = true
		t.Fatal("rebuild process-kill child exited normally")
	}
	reaped = true

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	stage4AssertRebuildProcessKillPreResume(t, ctx, store, runID, fixture)
	if err := fixture.mutateSource(ctx); err != nil {
		t.Fatalf("mutate %s source after child kill: %T", fixture.cell, err)
	}
	resumeEvents := make([]string, 0)
	resumeRun := Stage4RunContext{
		RunID:          runID,
		Backend:        store,
		Resume:         true,
		SpoolDirectory: spool,
	}
	result, err := ExecuteResume(
		ctx,
		fixture.cfg,
		CompletedTableCheckpoints{},
		stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &resumeEvents},
			run:                    resumeRun,
		},
	)
	if err != nil {
		t.Fatalf("resume hard-killed %s rebuild: %T", fixture.cell, err)
	}
	if result != (Result{Tables: 2, Rows: 4, Validated: true}) {
		t.Fatalf("resumed hard-killed %s result = %#v", fixture.cell, result)
	}
	fixture.assertTarget(t, ctx, true)
	published, err := PublishStage4RunCompletion(
		ctx,
		resumeRun,
		"rebuild process-kill recovered",
		time.Now().UTC(),
	)
	if err != nil || !published {
		t.Fatalf(
			"publish original %s rebuild run = published:%t err:%s",
			fixture.cell,
			published,
			stage4RebuildProcessKillSafeError(err, fixture.cfg),
		)
	}
	runs, err := store.List()
	if err != nil || len(runs) != 2 {
		t.Fatalf(
			"resumed %s rebuild run history = %#v, %T; want initial and terminal records",
			fixture.cell,
			runs,
			err,
		)
	}
	for _, run := range runs {
		if run.ID != runID {
			t.Fatalf("resumed %s rebuild published a different run %#v", fixture.cell, run)
		}
	}
	latest, found, err := store.Latest()
	if err != nil || !found || latest.ID != runID ||
		latest.Outcome != state.Success || latest.Resumable {
		t.Fatalf(
			"resumed %s rebuild terminal state found=%t run=%#v err=%T",
			fixture.cell,
			found,
			latest,
			err,
		)
	}
}

func stage4WaitForRebuildProcessKillBoundary(
	t *testing.T,
	eventPath string,
	statusPath string,
	wait <-chan error,
	reaped *bool,
) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(75 * time.Second)
	defer timeout.Stop()
	for {
		if _, err := os.Stat(eventPath); err == nil {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			*reaped = true
			if value, err := os.ReadFile(statusPath); err == nil {
				t.Fatalf("rebuild child did not reach terminal-ready boundary: status=%s", strings.TrimSpace(string(value)))
			}
			t.Fatal("rebuild child did not reach terminal-ready boundary")
		case <-wait:
			*reaped = true
			if value, err := os.ReadFile(statusPath); err == nil {
				t.Fatalf("rebuild child exited before terminal-ready boundary: status=%s", strings.TrimSpace(string(value)))
			}
			t.Fatal("rebuild child exited before terminal-ready boundary")
		}
	}
}

func stage4WriteRebuildProcessKillStatus(status string) {
	if path := os.Getenv(stage4RebuildProcessKillStatusEnv); path != "" {
		_ = os.WriteFile(path, []byte(status), 0o600)
	}
}

func stage4RebuildProcessKillSafeError(err error, cfg config.Config) string {
	if err == nil {
		return "none"
	}
	detail := stage4RebuildProcessKillRedactedError(err, cfg)
	var policy *schema.PolicyError
	if errors.As(err, &policy) {
		// PolicyError.Type can contain a live table name. Its operation is a
		// fixed projection category, so report only that bounded category to
		// the parent instead of a raw database error.
		switch policy.Operation {
		case "map SQLite schema namespace":
			return "sqlite-schema-namespace"
		case "map SQLite STRICT table":
			return "sqlite-strict"
		case "map SQLite WITHOUT ROWID table":
			return "sqlite-without-rowid"
		case "map SQLite implicit rowid identity":
			return "sqlite-implicit-rowid"
		case "map SQLite declared type":
			return "sqlite-declared-type"
		case "map SQLite type":
			return "sqlite-type"
		case "map SQLite type modifier":
			return "sqlite-type-modifier"
		default:
			return "schema-policy-" + detail
		}
	}
	var contract *SchemaContractError
	if errors.As(err, &contract) {
		return "schema-contract-" + string(contract.Kind) + "-" + detail
	}
	value := strings.ToLower(err.Error())
	for _, token := range []struct {
		match string
		name  string
	}{
		{"destructive acknowledgement", "destructive-ack"},
		{"replay-safe", "replay-safety"},
		{"network replay", "network-replay"},
		{"large_table_threshold", "large-table"},
		{"checkpoint_frequency", "checkpoint-frequency"},
		{"finalization", "finalization"},
		{"terminal-recovery", "terminal-recovery"},
		{"preflight", "preflight"},
		{"foreign key", "foreign-key"},
		{"schema", "schema"},
		{"pagination", "pagination"},
		{"runtime tuning", "runtime-tuning"},
		{"configuration", "configuration"},
		{"source", "source"},
		{"target", "target"},
	} {
		if strings.Contains(value, token.match) {
			return token.name + "-" + detail
		}
	}
	return "other-" + detail
}

func stage4RebuildProcessKillRedactedError(
	err error,
	cfg config.Config,
) string {
	if err == nil {
		return "none"
	}
	// The parent needs a useful first-live-cell diagnostic, but child errors
	// must never turn a private fixture endpoint into test output. Every
	// endpoint component held by the fixture config is replaced independently;
	// this also covers URI-formatted driver errors. Keep the bounded result on
	// one line so the status file cannot be confused with structured output.
	redacted := err.Error()
	for _, endpoint := range []config.Endpoint{cfg.Source, cfg.Target} {
		for _, value := range []string{
			endpoint.Password,
			endpoint.User,
			endpoint.Host,
			endpoint.Database,
			endpoint.TLSCAFile,
		} {
			if strings.TrimSpace(value) != "" {
				redacted = strings.ReplaceAll(redacted, value, "[redacted]")
			}
		}
	}
	redacted = strings.ReplaceAll(redacted, "\n", " ")
	redacted = strings.TrimSpace(redacted)
	const maximumDiagnostic = 360
	if len(redacted) > maximumDiagnostic {
		redacted = redacted[:maximumDiagnostic] + "..."
	}
	if redacted == "" {
		return "other"
	}
	return redacted
}

func stage4AssertRebuildProcessKillPreResume(
	t *testing.T,
	ctx context.Context,
	store stage4RebuildProcessKillStore,
	runID string,
	fixture stage4RebuildProcessKillFixture,
) {
	t.Helper()
	for _, phase := range []state.Stage4RebuildFinalizationPhase{
		state.Stage4RebuildFinalizationPlanned,
		state.Stage4RebuildFinalizationStarted,
	} {
		receipt, found, err := store.LoadStage4RebuildFinalization(runID, phase)
		if err != nil || !found || receipt.Finalization.InventoryDigest == "" {
			t.Fatalf("hard-killed %s rebuild lacks %s finalization receipt", fixture.cell, phase)
		}
	}
	if _, found, err := store.LoadStage4RebuildReady(runID); err != nil || found {
		t.Fatalf("hard-killed %s rebuild terminal-ready found=%t err=%T", fixture.cell, found, err)
	}
	runs, err := store.List()
	if err != nil || len(runs) != 1 || runs[0].ID != runID ||
		runs[0].Outcome != state.Running || !runs[0].Resumable {
		t.Fatalf("hard-killed %s rebuild is not truthfully active", fixture.cell)
	}
	fixture.assertTarget(t, ctx, true)
}

func stage4RebuildProcessKillOpenStore(
	kind string,
	path string,
) (stage4RebuildProcessKillStore, error) {
	switch kind {
	case "yaml":
		return state.YAMLStore{Path: path}, nil
	case "sqlite":
		return state.SQLiteStore{Path: path}, nil
	default:
		return nil, fmt.Errorf("unknown rebuild process-kill state backend")
	}
}

func stage4RebuildProcessKillSpool(t *testing.T, root, runID string) string {
	t.Helper()
	parent := filepath.Join(root, "spool")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal("create rebuild process-kill spool parent")
	}
	path := filepath.Join(parent, stage4LifecycleRunDigest(runID))
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal("create rebuild process-kill spool")
	}
	return path
}

func stage4RebuildProcessKillInitializeRun(
	t *testing.T,
	backend state.Backend,
	runID string,
	cfg config.Config,
) {
	t.Helper()
	sourceEngine, err := config.CanonicalEngine(cfg.Source.Type)
	if err != nil {
		t.Fatal("identify rebuild process-kill source engine")
	}
	sourceIdentity, err := stage4RebuildProcessKillEndpointIdentity(cfg.Source)
	if err != nil {
		t.Fatal("identify rebuild process-kill source identity")
	}
	targetIdentity, err := stage4RebuildProcessKillEndpointIdentity(cfg.Target)
	if err != nil {
		t.Fatal("identify rebuild process-kill target identity")
	}
	hash, err := config.Hash(cfg)
	if err != nil {
		t.Fatal("hash rebuild process-kill configuration")
	}
	if err := backend.InitializeRun(state.Run{
		ID:             runID,
		Source:         cfg.Source.Database,
		Target:         cfg.Target.Database,
		SourceEngine:   sourceEngine,
		SourceIdentity: sourceIdentity,
		TargetIdentity: targetIdentity,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "running",
		StartedAt:      time.Now().UTC().Add(-time.Minute),
	}, hash); err != nil {
		t.Fatalf("initialize rebuild process-kill run: %T", err)
	}
}

func stage4RebuildProcessKillEndpointIdentity(endpoint config.Endpoint) (string, error) {
	engineName, err := config.CanonicalEngine(endpoint.Type)
	if err != nil {
		return "", err
	}
	if engineName == "sqlite" {
		return "sqlite:" + filepath.Clean(endpoint.Database), nil
	}
	return config.NetworkEndpointWorkloadIdentity(endpoint)
}

func stage4RebuildProcessKillConfig(
	t *testing.T,
	source config.Endpoint,
	target config.Endpoint,
	parent string,
	child string,
) config.Config {
	t.Helper()
	cfg, err := newStage4RebuildProcessKillConfig(source, target, parent, child)
	if err != nil {
		t.Fatalf("parse rebuild process-kill configuration: %v", err)
	}
	return cfg
}

func newStage4RebuildProcessKillConfig(
	source config.Endpoint,
	target config.Endpoint,
	parent string,
	child string,
) (config.Config, error) {
	// Parse a representative configuration rather than constructing Config
	// directly. The production parser records setting provenance and supplies
	// the schema-contract defaults consumed by the Stage 4 admission path.
	// Replacing only endpoints/table names preserves that exact contract while
	// keeping credentials in the private fixture configuration.
	cfg, err := config.Parse([]byte(`
source:
  type: sqlite
  database: /tmp/dmtx-stage4-rebuild-source-placeholder.db
target:
  type: postgres
  host: placeholder.invalid
  database: placeholder
  user: placeholder
migration:
  target_mode: drop_recreate
  include_tables:
    - placeholder
  partitions: 1
  connection_limit: 4
  reader_parallelism: 1
  writer_parallelism: 1
  memory_ceiling_bytes: 67108864
  runtime_tuning: false
  validation:
    mode: count_only
    fail_on_mismatch: true
    fail_on_timeout: true
    fail_on_estimate_mismatch: true
`))
	if err != nil {
		return config.Config{}, err
	}
	cfg.Source = source
	cfg.Target = target
	cfg.Migration.IncludeTables = []string{parent, child}
	cfg.Migration.DestructiveAcknowledged = true
	return cfg, nil
}

func stage4RebuildProcessKillNames(prefix string) (string, string) {
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	return prefix + "_parents_" + suffix, prefix + "_children_" + suffix
}

func stage4RebuildProcessKillSQLiteSource(
	t *testing.T,
	ctx context.Context,
	parent string,
	child string,
) (config.Endpoint, func(context.Context) error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "source.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal("open rebuild SQLite source")
	}
	for _, statement := range []string{
		// BIGINT avoids SQLite's implicit INTEGER PRIMARY KEY rowid alias. The
		// native target projections certify this explicit stable key shape,
		// making the fixture a real admitted rebuild route rather than a
		// negative identity-admission test.
		"CREATE TABLE " + quote(parent) + " (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL)",
		"CREATE TABLE " + quote(child) + " (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL, FOREIGN KEY (parent_id) REFERENCES " + quote(parent) + "(id))",
		"INSERT INTO " + quote(parent) + " (id, payload) VALUES (1, 'parent-one'), (2, 'parent-two')",
		"INSERT INTO " + quote(child) + " (id, parent_id, payload) VALUES (10, 1, 'child-one'), (20, 2, 'child-two')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			_ = database.Close()
			t.Fatalf("create rebuild SQLite source fixture: %T", err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal("close rebuild SQLite source")
	}
	return config.Endpoint{Type: "sqlite", Database: path}, func(ctx context.Context) error {
		database, err := sql.Open("sqlite", path)
		if err != nil {
			return err
		}
		defer database.Close()
		_, err = database.ExecContext(ctx, "UPDATE "+quote(parent)+" SET payload = 'changed-after-kill' WHERE id = 1")
		return err
	}
}

func stage4RebuildProcessKillPostgresFixture(
	t *testing.T,
) stage4RebuildProcessKillFixture {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DMTX_TEST_POSTGRES_DSN for PostgreSQL rebuild process-kill coverage")
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL rebuild process-kill DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must verify the PostgreSQL certificate and hostname")
	}
	caPath := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL rebuild process-kill target: %T", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL rebuild process-kill target: %T", err)
	}
	namespace := "dmtx_s4_rebuild_kill_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	if _, err := database.ExecContext(ctx, "CREATE SCHEMA "+postgresIdentifier(namespace)); err != nil {
		t.Fatalf("create PostgreSQL rebuild process-kill schema: %T", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if _, err := database.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL rebuild process-kill schema: %T", err)
		}
	})
	parent, child := stage4RebuildProcessKillNames("pg")
	source, mutate := stage4RebuildProcessKillSQLiteSource(t, ctx, parent, child)
	for _, statement := range []string{
		"CREATE TABLE " + postgresQualified(namespace, parent) + " (id BIGINT NOT NULL PRIMARY KEY, payload TEXT NOT NULL)",
		"CREATE TABLE " + postgresQualified(namespace, child) + " (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT NOT NULL, payload TEXT NOT NULL, CONSTRAINT " + postgresIdentifier(child+"_parent_fk") + " FOREIGN KEY (parent_id) REFERENCES " + postgresQualified(namespace, parent) + "(id))",
		"INSERT INTO " + postgresQualified(namespace, parent) + " (id, payload) VALUES (1, 'stale-parent')",
		"INSERT INTO " + postgresQualified(namespace, child) + " (id, parent_id, payload) VALUES (10, 1, 'stale-child')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed PostgreSQL rebuild process-kill target: %T", err)
		}
	}
	target := config.Endpoint{
		Type: "postgres", Host: parsed.Host, Port: int(parsed.Port), Database: parsed.Database,
		User: parsed.User, Password: parsed.Password, Schema: namespace,
		SSLMode: "verify-full", TLSCAFile: caPath,
	}
	return stage4RebuildProcessKillFixture{
		cell: "postgres", parent: parent, child: child,
		cfg:          stage4RebuildProcessKillConfig(t, source, target, parent, child),
		mutateSource: mutate,
		assertTarget: func(t *testing.T, ctx context.Context, original bool) {
			t.Helper()
			stage4AssertRebuildGraphPostgres(t, ctx, database, namespace, parent, child, original)
		},
	}
}

func stage4RebuildProcessKillMySQLFixture(
	t *testing.T,
	flavor string,
) stage4RebuildProcessKillFixture {
	t.Helper()
	var (
		dsnEnv, caEnv, tlsName, prefix, collation string
		refresh                                   bool
	)
	switch flavor {
	case "mysql80":
		dsnEnv, caEnv, tlsName, prefix, collation, refresh = "DMTX_TEST_MYSQL_TARGET_DSN", "DMTX_TEST_MYSQL_CA", "dmtx_test", "mysql", "utf8mb4_0900_bin", true
	case "mariadb1011":
		dsnEnv, caEnv, tlsName, prefix, collation = "DMTX_TEST_MARIADB_TARGET_DSN", "DMTX_TEST_MARIADB_CA", "dmtx_mariadb_test", "maria", "utf8mb4_nopad_bin"
	default:
		t.Fatal("unknown MySQL-family rebuild process-kill target")
	}
	dsn, caPath := os.Getenv(dsnEnv), os.Getenv(caEnv)
	if dsn == "" || caPath == "" {
		t.Skip("set verified TLS fixture values for " + flavor + " rebuild process-kill coverage")
	}
	registerMySQLCommonFixtureTLSNamed(t, caPath, tlsName)
	parsed := parseMySQLNativeTargetDSNForTLS(t, flavor+" rebuild process-kill target", dsn, tlsName)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	database := openMySQLNativeLiveDatabaseForFlavor(t, ctx, flavor+" rebuild process-kill target", dsn, refresh)
	parent, child := stage4RebuildProcessKillNames(prefix)
	cleanupMySQLNativeTables(t, database, child, parent)
	for _, statement := range []string{
		"CREATE TABLE " + mySQLIdentifier(parent) + " (`id` BIGINT NOT NULL, `payload` TEXT NOT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=" + collation,
		"CREATE TABLE " + mySQLIdentifier(child) + " (`id` BIGINT NOT NULL, `parent_id` BIGINT NOT NULL, `payload` TEXT NOT NULL, PRIMARY KEY (`id`), CONSTRAINT " + mySQLIdentifier(child+"_parent_fk") + " FOREIGN KEY (`parent_id`) REFERENCES " + mySQLIdentifier(parent) + " (`id`)) ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 COLLATE=" + collation,
		"INSERT INTO " + mySQLIdentifier(parent) + " (`id`, `payload`) VALUES (1, 'stale-parent')",
		"INSERT INTO " + mySQLIdentifier(child) + " (`id`, `parent_id`, `payload`) VALUES (10, 1, 'stale-child')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed %s rebuild process-kill target: %T", flavor, err)
		}
	}
	source, mutate := stage4RebuildProcessKillSQLiteSource(t, ctx, parent, child)
	target := mysqlNativeTargetEndpoint(t, parsed, caPath)
	return stage4RebuildProcessKillFixture{
		cell: flavor, parent: parent, child: child,
		cfg:          stage4RebuildProcessKillConfig(t, source, target, parent, child),
		mutateSource: mutate,
		assertTarget: func(t *testing.T, ctx context.Context, original bool) {
			t.Helper()
			stage4AssertRebuildGraphMySQL(t, ctx, database, parsed.DBName, parent, child, original)
		},
	}
}

func stage4RebuildProcessKillSQLServerFixture(
	t *testing.T,
) stage4RebuildProcessKillFixture {
	t.Helper()
	dsn, caPath := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN"), os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip("set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA for SQL Server rebuild process-kill coverage")
	}
	target := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	database := openSQLServerNativeLiveDatabase(t, ctx, "rebuild process-kill target", target)
	parent, child := stage4RebuildProcessKillNames("mssql")
	cleanupSQLServerNativeTables(t, database, child, parent)
	for _, statement := range []string{
		"CREATE TABLE " + sqlServerQualified(target.Schema, parent) + " ([id] BIGINT NOT NULL, [payload] VARCHAR(64) NOT NULL, CONSTRAINT " + sqlServerIdentifier(parent+"_pk") + " PRIMARY KEY ([id]))",
		"CREATE TABLE " + sqlServerQualified(target.Schema, child) + " ([id] BIGINT NOT NULL, [parent_id] BIGINT NOT NULL, [payload] VARCHAR(64) NOT NULL, CONSTRAINT " + sqlServerIdentifier(child+"_pk") + " PRIMARY KEY ([id]), CONSTRAINT " + sqlServerIdentifier(child+"_parent_fk") + " FOREIGN KEY ([parent_id]) REFERENCES " + sqlServerQualified(target.Schema, parent) + " ([id]))",
		"INSERT INTO " + sqlServerQualified(target.Schema, parent) + " ([id], [payload]) VALUES (1, 'stale-parent')",
		"INSERT INTO " + sqlServerQualified(target.Schema, child) + " ([id], [parent_id], [payload]) VALUES (10, 1, 'stale-child')",
	} {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed SQL Server rebuild process-kill target: %T", err)
		}
	}
	source, mutate := stage4RebuildProcessKillSQLiteSource(t, ctx, parent, child)
	return stage4RebuildProcessKillFixture{
		cell: "mssql", parent: parent, child: child,
		cfg:          stage4RebuildProcessKillConfig(t, source, target, parent, child),
		mutateSource: mutate,
		assertTarget: func(t *testing.T, ctx context.Context, original bool) {
			t.Helper()
			stage4AssertRebuildGraphSQLServer(t, ctx, database, target.Schema, parent, child, original)
		},
	}
}

func stage4RebuildProcessKillSQLiteFixture(
	t *testing.T,
) stage4RebuildProcessKillFixture {
	t.Helper()
	dsn, caPath := os.Getenv("DMTX_TEST_MSSQL_DSN"), os.Getenv("DMTX_TEST_MSSQL_CA")
	if dsn == "" || caPath == "" {
		t.Skip("set DMTX_TEST_MSSQL_DSN and DMTX_TEST_MSSQL_CA for SQL Server-to-SQLite rebuild process-kill coverage")
	}
	source := sqlServerCommonFixtureEndpoint(t, dsn, caPath)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	sourceDatabase := openSQLServerNativeLiveDatabase(t, ctx, "rebuild process-kill source", source)
	parent, child := stage4RebuildProcessKillNames("sqlite")
	cleanupSQLServerNativeTables(t, sourceDatabase, child, parent)
	for _, statement := range []string{
		"CREATE TABLE " + sqlServerQualified(source.Schema, parent) + " ([id] BIGINT NOT NULL, [payload] VARCHAR(64) NOT NULL, CONSTRAINT " + sqlServerIdentifier(parent+"_pk") + " PRIMARY KEY ([id]))",
		"CREATE TABLE " + sqlServerQualified(source.Schema, child) + " ([id] BIGINT NOT NULL, [parent_id] BIGINT NOT NULL, [payload] VARCHAR(64) NOT NULL, CONSTRAINT " + sqlServerIdentifier(child+"_pk") + " PRIMARY KEY ([id]), CONSTRAINT " + sqlServerIdentifier(child+"_parent_fk") + " FOREIGN KEY ([parent_id]) REFERENCES " + sqlServerQualified(source.Schema, parent) + " ([id]))",
		"INSERT INTO " + sqlServerQualified(source.Schema, parent) + " ([id], [payload]) VALUES (1, 'parent-one'), (2, 'parent-two')",
		"INSERT INTO " + sqlServerQualified(source.Schema, child) + " ([id], [parent_id], [payload]) VALUES (10, 1, 'child-one'), (20, 2, 'child-two')",
	} {
		if _, err := sourceDatabase.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed SQL Server rebuild process-kill source: %T", err)
		}
	}
	targetPath := filepath.Join(t.TempDir(), "target.db")
	targetDatabase, err := openSQLiteTargetDatabase(ctx, targetPath)
	if err != nil {
		t.Fatalf("open SQLite rebuild process-kill target: %T", err)
	}
	for _, statement := range []string{
		"CREATE TABLE " + quote(parent) + " (id INTEGER NOT NULL PRIMARY KEY, payload TEXT NOT NULL)",
		"CREATE TABLE " + quote(child) + " (id INTEGER NOT NULL PRIMARY KEY, parent_id INTEGER NOT NULL, payload TEXT NOT NULL, FOREIGN KEY (parent_id) REFERENCES " + quote(parent) + "(id))",
		"INSERT INTO " + quote(parent) + " (id, payload) VALUES (1, 'stale-parent')",
		"INSERT INTO " + quote(child) + " (id, parent_id, payload) VALUES (10, 1, 'stale-child')",
	} {
		if _, err := targetDatabase.ExecContext(ctx, statement); err != nil {
			_ = targetDatabase.Close()
			t.Fatalf("seed SQLite rebuild process-kill target: %v", err)
		}
	}
	if err := targetDatabase.Close(); err != nil {
		t.Fatal("close SQLite rebuild process-kill target")
	}
	return stage4RebuildProcessKillFixture{
		cell: "sqlite", parent: parent, child: child,
		cfg: stage4RebuildProcessKillConfig(
			t,
			source,
			config.Endpoint{Type: "sqlite", Database: targetPath},
			parent,
			child,
		),
		mutateSource: func(ctx context.Context) error {
			_, err := sourceDatabase.ExecContext(ctx, "UPDATE "+sqlServerQualified(source.Schema, parent)+" SET [payload] = 'changed-after-kill' WHERE [id] = 1")
			return err
		},
		assertTarget: func(t *testing.T, ctx context.Context, original bool) {
			t.Helper()
			database, err := openSQLiteTargetDatabase(ctx, targetPath)
			if err != nil {
				t.Fatalf("open SQLite rebuild process-kill assertion target: %T", err)
			}
			defer database.Close()
			stage4AssertRebuildGraphSQLite(t, ctx, database, parent, child, original)
		},
	}
}

func stage4AssertRebuildGraphPostgres(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	parent string,
	child string,
	original bool,
) {
	t.Helper()
	stage4AssertRebuildGraphCountsPostgres(t, ctx, database, namespace, parent, child, original)
	var foreignKeys int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM information_schema.table_constraints
		 WHERE table_schema = $1
		   AND table_name = $2
		   AND constraint_type = 'FOREIGN KEY'
	`, namespace, child).Scan(&foreignKeys); err != nil {
		t.Fatalf("read PostgreSQL rebuild FK: %T", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("PostgreSQL rebuilt FK count = %d", foreignKeys)
	}
}

func stage4AssertRebuildGraphCountsPostgres(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	parent string,
	child string,
	original bool,
) {
	t.Helper()
	var parentRows, childRows int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(namespace, parent)).Scan(&parentRows); err != nil {
		t.Fatalf("count PostgreSQL rebuilt parent: %T", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+postgresQualified(namespace, child)).Scan(&childRows); err != nil {
		t.Fatalf("count PostgreSQL rebuilt child: %T", err)
	}
	if parentRows != 2 || childRows != 2 {
		t.Fatalf("PostgreSQL rebuilt graph rows = parents:%d children:%d", parentRows, childRows)
	}
	expected := "changed-after-kill"
	if original {
		expected = "parent-one"
	}
	var payload string
	if err := database.QueryRowContext(ctx, "SELECT payload FROM "+postgresQualified(namespace, parent)+" WHERE id = 1").Scan(&payload); err != nil {
		t.Fatalf("read PostgreSQL rebuilt parent payload: %T", err)
	}
	if payload != expected {
		t.Fatalf("PostgreSQL rebuilt parent payload = %q, want %q", payload, expected)
	}
}

func stage4AssertRebuildGraphMySQL(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	parent string,
	child string,
	original bool,
) {
	t.Helper()
	var parentRows, childRows int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(namespace, parent)).Scan(&parentRows); err != nil {
		t.Fatalf("count MySQL rebuilt parent: %T", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+mySQLQualified(namespace, child)).Scan(&childRows); err != nil {
		t.Fatalf("count MySQL rebuilt child: %T", err)
	}
	if parentRows != 2 || childRows != 2 {
		t.Fatalf("MySQL rebuilt graph rows = parents:%d children:%d", parentRows, childRows)
	}
	expected := "changed-after-kill"
	if original {
		expected = "parent-one"
	}
	var payload string
	if err := database.QueryRowContext(ctx, "SELECT `payload` FROM "+mySQLQualified(namespace, parent)+" WHERE `id` = ?", 1).Scan(&payload); err != nil {
		t.Fatalf("read MySQL rebuilt parent payload: %T", err)
	}
	if payload != expected {
		t.Fatalf("MySQL rebuilt parent payload = %q, want %q", payload, expected)
	}
	var foreignKeys int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM information_schema.KEY_COLUMN_USAGE
		 WHERE TABLE_SCHEMA = ?
		   AND TABLE_NAME = ?
		   AND REFERENCED_TABLE_SCHEMA = ?
		   AND REFERENCED_TABLE_NAME = ?
	`, namespace, child, namespace, parent).Scan(&foreignKeys); err != nil {
		t.Fatalf("read MySQL rebuilt FK: %T", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("MySQL rebuilt FK count = %d", foreignKeys)
	}
}

func stage4AssertRebuildGraphSQLServer(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	parent string,
	child string,
	original bool,
) {
	t.Helper()
	var parentRows, childRows int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(namespace, parent)).Scan(&parentRows); err != nil {
		t.Fatalf("count SQL Server rebuilt parent: %T", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+sqlServerQualified(namespace, child)).Scan(&childRows); err != nil {
		t.Fatalf("count SQL Server rebuilt child: %T", err)
	}
	if parentRows != 2 || childRows != 2 {
		t.Fatalf("SQL Server rebuilt graph rows = parents:%d children:%d", parentRows, childRows)
	}
	expected := "changed-after-kill"
	if original {
		expected = "parent-one"
	}
	var payload string
	if err := database.QueryRowContext(ctx, "SELECT [payload] FROM "+sqlServerQualified(namespace, parent)+" WHERE [id] = @p1", 1).Scan(&payload); err != nil {
		t.Fatalf("read SQL Server rebuilt parent payload: %T", err)
	}
	if payload != expected {
		t.Fatalf("SQL Server rebuilt parent payload = %q, want %q", payload, expected)
	}
	var foreignKeys int
	if err := database.QueryRowContext(ctx, `
		SELECT COUNT(*)
		  FROM sys.foreign_keys AS foreign_key
		  JOIN sys.tables AS child_table
		    ON child_table.object_id = foreign_key.parent_object_id
		  JOIN sys.schemas AS child_schema
		    ON child_schema.schema_id = child_table.schema_id
		  JOIN sys.tables AS parent_table
		    ON parent_table.object_id = foreign_key.referenced_object_id
		  JOIN sys.schemas AS parent_schema
		    ON parent_schema.schema_id = parent_table.schema_id
		 WHERE child_schema.name = @p1
		   AND child_table.name = @p2
		   AND parent_schema.name = @p3
		   AND parent_table.name = @p4
	`, namespace, child, namespace, parent).Scan(&foreignKeys); err != nil {
		t.Fatalf("read SQL Server rebuilt FK: %T", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("SQL Server rebuilt FK count = %d", foreignKeys)
	}
}

func stage4AssertRebuildGraphSQLite(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	parent string,
	child string,
	original bool,
) {
	t.Helper()
	var parentRows, childRows int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(parent)).Scan(&parentRows); err != nil {
		t.Fatalf("count SQLite rebuilt parent: %T", err)
	}
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quote(child)).Scan(&childRows); err != nil {
		t.Fatalf("count SQLite rebuilt child: %T", err)
	}
	if parentRows != 2 || childRows != 2 {
		t.Fatalf("SQLite rebuilt graph rows = parents:%d children:%d", parentRows, childRows)
	}
	expected := "changed-after-kill"
	if original {
		expected = "parent-one"
	}
	var payload string
	if err := database.QueryRowContext(ctx, "SELECT payload FROM "+quote(parent)+" WHERE id = ?", 1).Scan(&payload); err != nil {
		t.Fatalf("read SQLite rebuilt parent payload: %T", err)
	}
	if payload != expected {
		t.Fatalf("SQLite rebuilt parent payload = %q, want %q", payload, expected)
	}
	var foreignKeys int
	if err := database.QueryRowContext(ctx, "SELECT COUNT(*) FROM pragma_foreign_key_list(?)", child).Scan(&foreignKeys); err != nil {
		t.Fatalf("read SQLite rebuilt FK: %T", err)
	}
	if foreignKeys != 1 {
		t.Fatalf("SQLite rebuilt FK count = %d", foreignKeys)
	}
}
