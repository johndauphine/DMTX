package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type sqliteRetryTestError struct {
	code int
}

func (err sqliteRetryTestError) Error() string {
	return fmt.Sprintf("injected SQLite result code %d", err.code)
}

func (err sqliteRetryTestError) Code() int {
	return err.code
}

type sqliteRetryProtector struct {
	failuresBeforeMutation int
	failAfterMutation      bool
	attempts               int
}

func (*sqliteRetryProtector) BeforeTable(context.Context, string) error {
	return nil
}

func (*sqliteRetryProtector) AfterTable(context.Context, string, int) error {
	return nil
}

func (protector *sqliteRetryProtector) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	protector.attempts++
	if protector.attempts <= protector.failuresBeforeMutation {
		return sqliteRetryTestError{code: 5}
	}
	if err := mutation(); err != nil {
		return err
	}
	if protector.failAfterMutation {
		return sqliteRetryTestError{code: 5}
	}
	return nil
}

func TestSQLiteWriteRetryPolicyAtTransactionBoundary(t *testing.T) {
	table := schema.Table{
		Name: "items",
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true, PrimaryKeyPosition: 1},
			{Name: "value"},
		},
	}
	columns := []string{"id", "value"}
	rows := [][]any{{int64(1), "one"}}

	t.Run("transient not-committed attempts retry and succeed", func(t *testing.T) {
		target := openSQLiteRetryDatabase(t)
		defer target.Close()
		protector := &sqliteRetryProtector{failuresBeforeMutation: 2}
		receipt, err := writeSQLiteBatchReceiptWithPolicy(
			context.Background(), target, table, columns, "drop_recreate", rows,
			protector, nil, RetryPolicy{MaxRetries: 2},
		)
		if err != nil {
			t.Fatal(err)
		}
		if protector.attempts != 3 || receipt.Certainty != CommitDurable || receipt.CommittedRows != 1 {
			t.Fatalf("attempts = %d, receipt = %+v", protector.attempts, receipt)
		}
		assertSQLiteRetryRowCount(t, target, 1)
	})

	t.Run("max retries zero makes one attempt", func(t *testing.T) {
		target := openSQLiteRetryDatabase(t)
		defer target.Close()
		protector := &sqliteRetryProtector{failuresBeforeMutation: 1}
		receipt, err := writeSQLiteBatchReceiptWithPolicy(
			context.Background(), target, table, columns, "drop_recreate", rows,
			protector, nil, RetryPolicy{MaxRetries: 0},
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassTransient {
			t.Fatalf("receipt = %+v, error = %v", receipt, err)
		}
		if protector.attempts != 1 || receipt.Certainty != CommitNotCommitted {
			t.Fatalf("attempts = %d, receipt = %+v", protector.attempts, receipt)
		}
		assertSQLiteRetryRowCount(t, target, 0)
	})

	t.Run("error after durable commit never retries", func(t *testing.T) {
		target := openSQLiteRetryDatabase(t)
		defer target.Close()
		protector := &sqliteRetryProtector{failAfterMutation: true}
		receipt, err := writeSQLiteBatchReceiptWithPolicy(
			context.Background(), target, table, columns, "drop_recreate", rows,
			protector, nil, RetryPolicy{MaxRetries: 3},
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassState {
			t.Fatalf("receipt = %+v, error = %v", receipt, err)
		}
		if protector.attempts != 1 || receipt.Certainty != CommitDurable {
			t.Fatalf("attempts = %d, receipt = %+v", protector.attempts, receipt)
		}
		assertSQLiteRetryRowCount(t, target, 1)
	})
}

func TestSQLiteWriteRetryExhaustionCommitUnknownAndCancellation(t *testing.T) {
	notCommitted := WriteReceipt{Certainty: CommitNotCommitted, AttemptedRows: 1}
	unknown := WriteReceipt{Certainty: CommitUnknown, AttemptedRows: 1}

	t.Run("exhaustion is bounded", func(t *testing.T) {
		attempts := 0
		receipt, err := retrySQLiteWriteAttempts(
			context.Background(), RetryPolicy{MaxRetries: 2},
			func(context.Context, int) (WriteReceipt, error) {
				attempts++
				return notCommitted, sqliteRetryTestError{code: 6}
			},
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassTransient {
			t.Fatalf("receipt = %+v, error = %v", receipt, err)
		}
		if attempts != 3 || receipt.Certainty != CommitNotCommitted {
			t.Fatalf("attempts = %d, receipt = %+v", attempts, receipt)
		}
	})

	t.Run("commit unknown fails closed", func(t *testing.T) {
		attempts := 0
		receipt, err := retrySQLiteWriteAttempts(
			context.Background(), RetryPolicy{MaxRetries: 3},
			func(context.Context, int) (WriteReceipt, error) {
				attempts++
				return unknown, sqliteRetryTestError{code: 5}
			},
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassState ||
			!errors.Is(err, sqliteRetryTestError{code: 5}) {
			t.Fatalf("receipt = %+v, error = %v", receipt, err)
		}
		if attempts != 1 || receipt.Certainty != CommitUnknown {
			t.Fatalf("attempts = %d, receipt = %+v", attempts, receipt)
		}
	})

	t.Run("cancellation stops backoff", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		go func() {
			<-started
			cancel()
		}()
		attempts := 0
		_, err := retrySQLiteWriteAttempts(
			ctx,
			RetryPolicy{MaxRetries: 3, InitialBackoff: time.Hour, MaxBackoff: time.Hour},
			func(context.Context, int) (WriteReceipt, error) {
				attempts++
				if attempts == 1 {
					close(started)
				}
				return notCommitted, sqliteRetryTestError{code: 5}
			},
		)
		if !errors.Is(err, context.Canceled) || attempts != 1 {
			t.Fatalf("attempts = %d, error = %v", attempts, err)
		}
	})
}

func TestEffectiveSQLiteTransferSettingsPreservesZeroRetries(t *testing.T) {
	override := config.EffectiveTransferPlan{
		TargetMode:   "drop_recreate",
		MemoryBudget: config.EffectiveBytes{Value: 1 << 20},
		Readers:      config.EffectiveInt{Value: 1},
		Writers:      config.EffectiveInt{Value: 1},
		QueueDepth:   config.EffectiveInt{Value: 1},
		ChunkRows:    config.EffectiveInt{Value: 1},
	}
	settings, err := effectiveSQLiteTransferSettings(
		context.Background(), config.Migration{MaxRetries: 0}, &override,
	)
	if err != nil {
		t.Fatal(err)
	}
	if settings.maxRetries != 0 {
		t.Fatalf("max retries = %d, want explicit zero", settings.maxRetries)
	}
}

func openSQLiteRetryDatabase(t *testing.T) *sql.DB {
	t.Helper()
	target, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "target.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := target.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		target.Close()
		t.Fatal(err)
	}
	return target
}

func assertSQLiteRetryRowCount(t *testing.T, target *sql.DB, want int) {
	t.Helper()
	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("target rows = %d, want %d", count, want)
	}
}
