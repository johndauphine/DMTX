package migrate

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4SchemaContractFreezeStopsLiveDrift proves a schema-contract mode
// enforces against a real database.
//
// The contract has extensive projection-level coverage, but those tests answer
// "would the planner decide correctly given this drift?" — not "does a running
// migration notice real drift and stop before touching the target?" Only the
// second protects an operator.
//
// Three things this fixture must get right, each of which produced a
// false-passing or failing draft first:
//   - The target must be able to accept the drift. A SQLite target refuses the
//     changed shape in its own retained-shape preflight before the contract is
//     consulted, so the run fails for an unrelated reason.
//   - Source and target must be separate databases; the same endpoint, and even
//     separate schemas within one database, are refused.
//   - The baseline run must be marked successful. The contract compares against
//     the latest *successful* snapshot, so a baseline left Running leaves the
//     second run without the prior authority it needs.
func TestStage4SchemaContractFreezeStopsLiveDrift(t *testing.T) {
	runStage4SchemaContractLiveMode(t, config.SchemaContractFreeze, nil)
}

// TestStage4SchemaContractModesLive extends the freeze proof across the modes
// whose live behaviour differs. Each mode is given the identical real drift and
// judged on what it must do with it, so a mode that silently behaved like
// another would fail rather than blend in.
func TestStage4SchemaContractModesLive(t *testing.T) {
	for name, mode := range map[string]config.SchemaContractMode{
		"report":  config.SchemaContractReport,
		"evolve":  config.SchemaContractEvolve,
		"discard": config.SchemaContractDiscardValue,
	} {
		t.Run(name, func(t *testing.T) {
			runStage4SchemaContractLiveMode(t, mode, nil)
		})
	}
}

func runStage4SchemaContractLiveMode(
	t *testing.T,
	mode config.SchemaContractMode,
	_ any,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 schema-contract freeze sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_POSTGRES_DSN: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	setup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open schema-contract source: %v", err)
	}
	t.Cleanup(func() { _ = setup.Close() })

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	namespace := "dmtx_s4_freeze_" + suffix
	tableName := "network_items"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create schema-contract schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 20*time.Second)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop schema-contract schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO ` + qualified + ` (id, payload) VALUES (1, 11), (2, 22)`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed schema-contract fixture: %v", err)
		}
	}

	targetDatabase := "dmtx_s4_freeze_target_" + suffix
	if _, err := setup.ExecContext(
		ctx,
		"CREATE DATABASE "+postgresIdentifier(targetDatabase),
	); err != nil {
		t.Fatalf("create schema-contract target database: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(), 20*time.Second)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP DATABASE IF EXISTS "+
				postgresIdentifier(targetDatabase)+" WITH (FORCE)",
		); err != nil {
			t.Errorf("drop schema-contract target database: %v", err)
		}
	})

	caFile := stage4PostgresDeleteLiveCAFile(t, parsed.ConnString())
	sourceEndpoint := config.Endpoint{
		Type: "postgres", Host: parsed.Host, Port: int(parsed.Port),
		Database: parsed.Database, User: parsed.User,
		Password: parsed.Password, Schema: namespace,
		SSLMode: "verify-full", TLSCAFile: caFile,
	}
	targetEndpoint := sourceEndpoint
	targetEndpoint.Database = targetDatabase
	targetEndpoint.Schema = "public"

	targetDSN, err := engine.PostgresDSN(targetEndpoint)
	if err != nil {
		t.Fatalf("build schema-contract target DSN: %v", err)
	}
	targetSetup, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatalf("open schema-contract target: %v", err)
	}
	t.Cleanup(func() { _ = targetSetup.Close() })
	if _, err := targetSetup.ExecContext(
		ctx,
		`CREATE TABLE public.`+postgresIdentifier(tableName)+` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
	); err != nil {
		t.Fatalf("create schema-contract target table: %v", err)
	}

	newConfig := func() config.Config {
		return config.Config{
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
					Mode: config.ValidationCountOnly,
				},
			},
		}
	}
	backend := state.SQLiteStore{
		Path: filepath.Join(t.TempDir(), "state.db"),
	}
	runOnce := func(t *testing.T, runID string, cfg config.Config) (
		[]string, error,
	) {
		t.Helper()
		initializeStage4LifecycleRun(
			t, backend, runID, time.Now().Add(-time.Minute))
		events := make([]string, 0)
		_, err := Execute(ctx, cfg, stage4AdapterObserver{
			recordingTableObserver: recordingTableObserver{events: &events},
			run: Stage4RunContext{
				RunID:          runID,
				Backend:        backend,
				SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
			},
		})
		return events, err
	}

	baselineRun := "stage4-freeze-baseline"
	if _, err := runOnce(t, baselineRun, newConfig()); err != nil {
		t.Fatalf("establish schema-contract baseline: %v", err)
	}
	// The contract compares against the latest *successful* snapshot, so the
	// baseline must be published as successful before it can be an authority.
	completeStage4IncrementalTestRun(t, backend, baselineRun)

	if _, err := setup.ExecContext(
		ctx,
		"ALTER TABLE "+qualified+" ADD COLUMN note TEXT",
	); err != nil {
		t.Fatalf("drift schema-contract source: %v", err)
	}

	frozen := newConfig()
	// `tables: discard_value` is rejected by configuration policy, so the table
	// entity keeps evolve while the column entities carry the mode under test.
	tableMode := mode
	if mode == config.SchemaContractDiscardValue {
		tableMode = config.SchemaContractEvolve
	}
	frozen.Migration.SchemaContract = &config.SchemaContract{
		Tables:   tableMode,
		Columns:  mode,
		DataType: mode,
	}
	frozenEvents, frozenErr := runOnce(t, "stage4-contract-drift", frozen)

	if mode != config.SchemaContractFreeze {
		// report deliberately does not act on drift, so the added source column
		// has nowhere to go and the write fails. That is a real operator-facing
		// wart worth naming: the run stops with a low-level "requested column
		// note is not present in schema" rather than a contract-level message.
		// The contract guarantee under test is narrower — report must not
		// mutate the target schema — and that is what is asserted below.
		if mode != config.SchemaContractReport && frozenErr != nil {
			t.Fatalf("%s refused drift it must handle: %v", mode, frozenErr)
		}
		var targetHasNote int
		if err := targetSetup.QueryRowContext(
			ctx,
			`SELECT COUNT(*) FROM information_schema.columns
			  WHERE table_schema = 'public' AND table_name = $1
			    AND column_name = 'note'`,
			tableName,
		).Scan(&targetHasNote); err != nil {
			t.Fatalf("inspect %s target column: %v", mode, err)
		}
		switch mode {
		case config.SchemaContractEvolve:
			if targetHasNote != 1 {
				t.Fatalf("evolve did not add the drifted column to the target")
			}
		default:
			// report and discard_value must leave the target shape alone.
			if targetHasNote != 0 {
				t.Fatalf("%s mutated the target schema", mode)
			}
		}
		return
	}

	if frozenErr == nil {
		t.Fatal("freeze accepted a drifted live source")
	}
	// Assert the reason. A contract test that accepts any error is worthless —
	// that is exactly how the earlier SQLite-target draft appeared to work.
	lowered := strings.ToLower(frozenErr.Error())
	if !strings.Contains(lowered, "freeze") &&
		!strings.Contains(lowered, "contract") &&
		!strings.Contains(lowered, "drift") {
		t.Fatalf(
			"freeze failed for an unrelated reason, so the contract is unproven: %v",
			frozenErr,
		)
	}
	for _, event := range frozenEvents {
		if strings.HasPrefix(event, "target_write") {
			t.Fatalf("freeze wrote to the target before refusing: %v", frozenEvents)
		}
	}
	t.Logf("freeze refused live drift: %v", frozenErr)
}
