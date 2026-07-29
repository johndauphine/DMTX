package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresIdentitySequenceRestartLockLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL identity-sequence lock test",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse live PostgreSQL DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("live PostgreSQL DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open live PostgreSQL connection: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live PostgreSQL connection: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify live PostgreSQL connection: %v", err)
	}

	namespace := fmt.Sprintf(
		"dmtx_sequence_lock_%d_%d",
		os.Getpid(),
		time.Now().UnixNano(),
	)
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create identity lock schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop identity lock schema: %v", err)
		}
	})

	table := schema.Table{
		Schema:              namespace,
		Name:                "items",
		AutoIncrementColumn: "id",
		Columns: []schema.Column{{
			Name:       "id",
			Type:       "bigint",
			PrimaryKey: true,
		}},
	}
	create, err := schema.CreateTable(schema.Postgres, table)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(ctx, create); err != nil {
		t.Fatalf("create identity lock table: %v", err)
	}
	state, err := readPostgresIdentitySequenceState(
		ctx,
		database,
		table,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := validatePostgresIdentitySequenceState(table, state); err != nil {
		t.Fatal(err)
	}

	lockingTransaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin identity lock transaction: %v", err)
	}
	defer lockingTransaction.Rollback()
	if _, err := lockingTransaction.ExecContext(
		ctx,
		postgresIdentitySequenceLockStatement(state),
	); err != nil {
		t.Fatalf("acquire identity sequence restart lock: %v", err)
	}

	waitingConnection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("reserve waiting PostgreSQL connection: %v", err)
	}
	defer waitingConnection.Close()
	var waitingPID int
	if err := waitingConnection.QueryRowContext(
		ctx,
		"SELECT pg_catalog.pg_backend_pid()",
	).Scan(&waitingPID); err != nil {
		t.Fatalf("read waiting PostgreSQL backend PID: %v", err)
	}

	type nextValueResult struct {
		value int64
		err   error
	}
	nextValue := make(chan nextValueResult, 1)
	go func() {
		var value int64
		err := waitingConnection.QueryRowContext(
			ctx,
			`SELECT pg_catalog.nextval($1::oid::pg_catalog.regclass)`,
			state.objectID,
		).Scan(&value)
		nextValue <- nextValueResult{value: value, err: err}
	}()

	waitUntilPostgresBackendIsLockBlocked(
		t,
		ctx,
		database,
		waitingPID,
	)
	select {
	case result := <-nextValue:
		t.Fatalf(
			"nextval returned before restart lock release: (%d, %v)",
			result.value,
			result.err,
		)
	default:
	}
	if err := lockingTransaction.Commit(); err != nil {
		t.Fatalf("release identity sequence restart lock: %v", err)
	}
	select {
	case result := <-nextValue:
		if result.err != nil || result.value != 1 {
			t.Fatalf(
				"nextval after restart lock = (%d, %v), want (1, nil)",
				result.value,
				result.err,
			)
		}
	case <-ctx.Done():
		t.Fatalf("nextval remained blocked after restart lock release: %v", ctx.Err())
	}

	tableLockingTransaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin identity table-lock transaction: %v", err)
	}
	defer tableLockingTransaction.Rollback()
	if _, err := tableLockingTransaction.ExecContext(
		ctx,
		postgresIdentityTableLockStatement(table),
	); err != nil {
		t.Fatalf("acquire identity table finalization lock: %v", err)
	}

	explicitInsert := make(chan error, 1)
	go func() {
		_, err := waitingConnection.ExecContext(
			ctx,
			"INSERT INTO "+postgresQualified(namespace, table.Name)+
				" ("+postgresIdentifier("id")+") VALUES (100)",
		)
		explicitInsert <- err
	}()

	waitUntilPostgresBackendIsLockBlocked(
		t,
		ctx,
		database,
		waitingPID,
	)
	select {
	case err := <-explicitInsert:
		t.Fatalf(
			"explicit identity insert returned before table lock release: %v",
			err,
		)
	default:
	}
	if err := tableLockingTransaction.Commit(); err != nil {
		t.Fatalf("release identity table finalization lock: %v", err)
	}
	select {
	case err := <-explicitInsert:
		if err != nil {
			t.Fatalf(
				"explicit identity insert after table lock release: %v",
				err,
			)
		}
	case <-ctx.Done():
		t.Fatalf(
			"explicit identity insert remained blocked after table lock release: %v",
			ctx.Err(),
		)
	}
}

func waitUntilPostgresBackendIsLockBlocked(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	backendPID int,
) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waitEventType sql.NullString
		if err := database.QueryRowContext(
			ctx,
			`SELECT wait_event_type
			   FROM pg_catalog.pg_stat_activity
			  WHERE pid = $1`,
			backendPID,
		).Scan(&waitEventType); err != nil {
			t.Fatalf("inspect waiting PostgreSQL backend: %v", err)
		}
		if waitEventType.Valid && waitEventType.String == "Lock" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PostgreSQL backend did not block on sequence lock: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
