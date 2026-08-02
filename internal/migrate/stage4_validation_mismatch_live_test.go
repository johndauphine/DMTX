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

	"github.com/johndauphine/dmtx/internal/state"
)

// TestStage4ValidationDetectsTargetMismatchLive proves that live validation can
// actually fail.
//
// Every other live fixture asserts the happy path — Validated: true. That
// leaves the most important question untested: would validation notice if the
// target were wrong? A validation step that cannot fail against a real database
// is indistinguishable from one that is not running, so this test removes rows
// from the target behind the tool's back and requires the next run to refuse to
// treat the table as complete.
func TestStage4ValidationDetectsTargetMismatchLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the Stage 4 validation mismatch sentinel",
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
		t.Fatalf("open Stage 4 validation mismatch source: %v", err)
	}
	t.Cleanup(func() { _ = setup.Close() })

	namespace := "dmtx_stage4_mismatch_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	tableName := "network_items"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create Stage 4 validation mismatch schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			20*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop Stage 4 validation mismatch schema: %v", err)
		}
	})
	qualified := postgresQualified(namespace, tableName)
	for _, statement := range []string{
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL,
			payload BIGINT NOT NULL,
			PRIMARY KEY (id)
		)`,
		`INSERT INTO ` + qualified + ` (id, payload) VALUES
			(1, 11), (2, 22), (3, 33)`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("seed Stage 4 validation mismatch fixture: %v", err)
		}
	}

	targetPath := filepath.Join(t.TempDir(), "target.db")
	createStage4NetworkSQLiteTarget(t, ctx, targetPath, tableName)
	cfg := stage4NetworkLifecycleLiveConfig(
		t,
		parsed,
		namespace,
		targetPath,
		tableName,
	)
	backend := state.SQLiteStore{
		Path: filepath.Join(t.TempDir(), "state.db"),
	}
	runID := "stage4-validation-mismatch"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	events := make([]string, 0)
	observer := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        backend,
			SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
		},
	}
	result, err := PostgresToSQLiteWithObserver(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("Stage 4 validation mismatch baseline: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 3, Validated: true}) {
		t.Fatalf("baseline result = %#v", result)
	}

	// Remove a row from the target behind the tool's back. Nothing in the
	// durable state knows about this, so only a real target-side check can
	// notice it.
	target, err := sql.Open("sqlite", targetPath)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	if _, err := target.ExecContext(
		ctx,
		"DELETE FROM "+quote(tableName)+" WHERE id = 2",
	); err != nil {
		t.Fatalf("tamper Stage 4 validation target: %v", err)
	}

	// A resume that treats the table as already complete must refuse: the
	// completed-table skip is only permitted after exact target agreement.
	resumeObserver := stage4AdapterObserver{
		recordingTableObserver: recordingTableObserver{events: &events},
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        backend,
			Resume:         true,
			SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
		},
	}
	_, resumeErr := ExecuteResume(
		ctx,
		cfg,
		CompletedTableCheckpoints{tableName: {Rows: 3}},
		resumeObserver,
	)
	if resumeErr == nil {
		t.Fatal(
			"a resumed run accepted a target missing a row as already complete",
		)
	}
	// Assert the reason, not merely that something failed. An unrelated error —
	// a misbound spool, a bad DSN — would otherwise let this test pass while
	// mismatch detection was entirely broken, which is the exact failure mode
	// it exists to rule out.
	if !strings.Contains(resumeErr.Error(), "rows") &&
		!strings.Contains(resumeErr.Error(), "count") &&
		!strings.Contains(resumeErr.Error(), "agreement") {
		t.Fatalf(
			"resume failed for an unrelated reason, so mismatch detection is unproven: %v",
			resumeErr,
		)
	}
	t.Logf("target mismatch was detected: %v", resumeErr)
}
