package migrate

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"syscall"
	"testing"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	mssql "github.com/microsoft/go-mssqldb"
)

func TestClassifyEngineRetryMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		engine     string
		boundary   EngineRetryBoundary
		err        error
		wantClass  TransferErrorClass
		wantCode   string
		wantReason string
	}{
		{
			name:   "postgres serialization rolled back",
			engine: "postgres", boundary: EngineRetryRolledBack,
			err:       &pgconn.PgError{Code: "40001"},
			wantClass: ErrorClassTransient, wantCode: "40001",
			wantReason: "serialization_failure",
		},
		{
			name:   "postgres syntax permanent",
			engine: "postgres", boundary: EngineRetryReadOnly,
			err:       &pgconn.PgError{Code: "42601"},
			wantClass: ErrorClassPermanent, wantCode: "42601",
			wantReason: "postgres_permanent",
		},
		{
			name:   "postgres connection class read",
			engine: "postgres", boundary: EngineRetryReadOnly,
			err:       &pgconn.PgError{Code: "08006"},
			wantClass: ErrorClassTransient, wantCode: "08006",
			wantReason: "connection_exception",
		},
		{
			name:   "mysql deadlock rolled back",
			engine: "mysql", boundary: EngineRetryRolledBack,
			err:       &mysql.MySQLError{Number: 1213},
			wantClass: ErrorClassTransient, wantCode: "1213",
			wantReason: "deadlock_victim",
		},
		{
			name:   "mariadb lock timeout idempotent",
			engine: "mariadb", boundary: EngineRetryIdempotent,
			err:       &mysql.MySQLError{Number: 1205},
			wantClass: ErrorClassTransient, wantCode: "1205",
			wantReason: "lock_timeout",
		},
		{
			name:   "mysql conversion permanent",
			engine: "mysql", boundary: EngineRetryRolledBack,
			err:       &mysql.MySQLError{Number: 1366},
			wantClass: ErrorClassPermanent, wantCode: "1366",
			wantReason: "mysql_permanent",
		},
		{
			name:   "sql server deadlock",
			engine: "mssql", boundary: EngineRetryRolledBack,
			err:       mssql.Error{Number: 1205},
			wantClass: ErrorClassTransient, wantCode: "1205",
			wantReason: "deadlock_victim",
		},
		{
			name:   "sql server constraint permanent",
			engine: "mssql", boundary: EngineRetryRolledBack,
			err:       mssql.Error{Number: 2627},
			wantClass: ErrorClassPermanent, wantCode: "2627",
			wantReason: "mssql_permanent",
		},
		{
			name:   "sqlite busy extended",
			engine: "sqlite", boundary: EngineRetryRolledBack,
			err:       engineRetrySQLiteError{code: 5 | 3<<8},
			wantClass: ErrorClassTransient, wantCode: "773",
			wantReason: "database_busy",
		},
		{
			name:   "sqlite constraint permanent",
			engine: "sqlite", boundary: EngineRetryIdempotent,
			err:       engineRetrySQLiteError{code: 19},
			wantClass: ErrorClassPermanent, wantCode: "19",
			wantReason: "sqlite_permanent",
		},
		{
			name:   "clickhouse query pressure",
			engine: "clickhouse", boundary: EngineRetryReadOnly,
			err:       &clickhouseproto.Exception{Code: 202},
			wantClass: ErrorClassTransient, wantCode: "202",
			wantReason: "too_many_simultaneous_queries",
		},
		{
			name:   "clickhouse conversion permanent",
			engine: "clickhouse", boundary: EngineRetryIdempotent,
			err:       &clickhouseproto.Exception{Code: 70},
			wantClass: ErrorClassPermanent, wantCode: "70",
			wantReason: "clickhouse_permanent",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fact := ClassifyEngineRetry(
				test.engine,
				test.boundary,
				fmt.Errorf("wrapped: %w", test.err),
			)
			if fact.Class != test.wantClass ||
				fact.Code != test.wantCode ||
				fact.Reason != test.wantReason {
				t.Fatalf("fact = %#v", fact)
			}
		})
	}
}

func TestClassifyEngineRetryRequiresSafeReplayBoundary(t *testing.T) {
	t.Parallel()

	transientErrors := map[string]error{
		"postgres": &pgconn.PgError{Code: "40P01"},
		"mysql":    &mysql.MySQLError{Number: 1213},
		"mariadb":  &mysql.MySQLError{Number: 1205},
		"mssql":    mssql.Error{Number: 1205},
		"sqlite":   engineRetrySQLiteError{code: 5},
		"clickhouse": &clickhouseproto.Exception{
			Code: 202,
		},
	}
	for engine, sourceError := range transientErrors {
		engine, sourceError := engine, sourceError
		t.Run(engine, func(t *testing.T) {
			t.Parallel()
			fact := ClassifyEngineRetry(
				engine,
				EngineRetryUnknownCommit,
				sourceError,
			)
			if fact.Class != ErrorClassPermanent ||
				fact.Code != "" ||
				fact.Reason != "commit_outcome_unknown" {
				t.Fatalf("unknown-commit fact = %#v", fact)
			}
			wrapped := WrapEngineRetryError(
				engine,
				EngineRetryUnknownCommit,
				sourceError,
			)
			var transfer *TransferError
			if IsRetryable(wrapped) ||
				ClassifyTransferError(wrapped) != ErrorClassPermanent ||
				!errors.As(wrapped, &transfer) ||
				!reflect.DeepEqual(transfer.Err, sourceError) {
				t.Fatalf("unknown-commit wrapper = %v", wrapped)
			}
		})
	}
}

func TestClassifyEngineRetryTransportBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		err           error
		wantCode      string
		rolledBackTry bool
	}{
		{
			name: "driver bad connection", err: driver.ErrBadConn,
			wantCode: "driver_bad_connection", rolledBackTry: true,
		},
		{
			name: "unexpected eof", err: io.ErrUnexpectedEOF,
			wantCode: "unexpected_eof",
		},
		{
			name: "eof", err: io.EOF,
			wantCode: "eof",
		},
		{
			name: "network closed", err: net.ErrClosed,
			wantCode: "network_closed",
		},
		{
			name: "connection reset", err: syscall.ECONNRESET,
			wantCode: "connection_reset",
		},
		{
			name: "connection aborted", err: syscall.ECONNABORTED,
			wantCode: "connection_aborted",
		},
		{
			name: "connection refused", err: syscall.ECONNREFUSED,
			wantCode: "connection_refused", rolledBackTry: true,
		},
		{
			name: "broken pipe", err: syscall.EPIPE,
			wantCode: "broken_pipe",
		},
		{
			name: "network timeout", err: engineRetryNetworkError{},
			wantCode: "network_temporary",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, boundary := range []EngineRetryBoundary{
				EngineRetryReadOnly,
				EngineRetryPreMutation,
				EngineRetryIdempotent,
			} {
				fact := ClassifyEngineRetry(
					"postgres",
					boundary,
					test.err,
				)
				if fact.Class != ErrorClassTransient ||
					fact.Code != test.wantCode {
					t.Fatalf(
						"boundary %q fact = %#v",
						boundary,
						fact,
					)
				}
			}
			rolledBack := ClassifyEngineRetry(
				"postgres",
				EngineRetryRolledBack,
				test.err,
			)
			want := ErrorClassPermanent
			if test.rolledBackTry {
				want = ErrorClassTransient
			}
			if rolledBack.Class != want {
				t.Fatalf("rolled-back fact = %#v", rolledBack)
			}
		})
	}
}

func TestClassifyEngineRetryUnknownCommitRejectsAllTransportEvidence(
	t *testing.T,
) {
	t.Parallel()

	errorsToReject := []error{
		driver.ErrBadConn,
		net.ErrClosed,
		io.ErrUnexpectedEOF,
		io.EOF,
		syscall.ECONNRESET,
		syscall.ECONNABORTED,
		syscall.ECONNREFUSED,
		syscall.EPIPE,
		engineRetryNetworkError{},
		pgconn.ErrConnClosed,
		mysql.ErrInvalidConn,
	}
	for _, sourceError := range errorsToReject {
		fact := ClassifyEngineRetry(
			"postgres",
			EngineRetryUnknownCommit,
			sourceError,
		)
		if fact.Class != ErrorClassPermanent ||
			fact.Code != "" ||
			fact.Reason != "commit_outcome_unknown" {
			t.Fatalf(
				"unknown-commit transport %T fact = %#v",
				sourceError,
				fact,
			)
		}
		if IsRetryable(
			WrapEngineRetryError(
				"postgres",
				EngineRetryUnknownCommit,
				sourceError,
			),
		) {
			t.Fatalf(
				"unknown-commit transport %T became retryable",
				sourceError,
			)
		}
	}
}

func TestClassifyEngineRetryStructuralServerErrorBeatsJoinedTransport(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		engine     string
		err        error
		wantCode   string
		wantReason string
	}{
		{
			name:   "postgres",
			engine: "postgres",
			err: &pgconn.PgError{
				Code:    "42601",
				Message: "password=postgres-secret",
			},
			wantCode: "42601", wantReason: "postgres_permanent",
		},
		{
			name:   "mysql",
			engine: "mysql",
			err: &mysql.MySQLError{
				Number:  1366,
				Message: "password=mysql-secret",
			},
			wantCode: "1366", wantReason: "mysql_permanent",
		},
		{
			name:   "mariadb",
			engine: "mariadb",
			err: &mysql.MySQLError{
				Number:  1366,
				Message: "password=mariadb-secret",
			},
			wantCode: "1366", wantReason: "mysql_permanent",
		},
		{
			name:   "sql server",
			engine: "mssql",
			err: mssql.Error{
				Number:  2627,
				Message: "password=mssql-secret",
			},
			wantCode: "2627", wantReason: "mssql_permanent",
		},
		{
			name:   "sqlite",
			engine: "sqlite",
			err: engineRetrySQLiteError{
				code: 19,
				text: "password=sqlite-secret",
			},
			wantCode: "19", wantReason: "sqlite_permanent",
		},
		{
			name:   "clickhouse",
			engine: "clickhouse",
			err: &clickhouseproto.Exception{
				Code:    70,
				Message: "password=clickhouse-secret",
			},
			wantCode: "70", wantReason: "clickhouse_permanent",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			fact := ClassifyEngineRetry(
				test.engine,
				EngineRetryIdempotent,
				errors.Join(test.err, io.ErrUnexpectedEOF),
			)
			if fact.Class != ErrorClassPermanent ||
				fact.Code != test.wantCode ||
				fact.Reason != test.wantReason {
				t.Fatalf("fact = %#v", fact)
			}
			if strings.Contains(
				fmt.Sprintf("%#v", fact),
				"secret",
			) {
				t.Fatalf("fact leaked driver message: %#v", fact)
			}
		})
	}
}

func TestClassifyEngineRetryPreservesExplicitClassAndCancellation(
	t *testing.T,
) {
	t.Parallel()

	explicit := NewTransferError(
		ErrorClassValidation,
		errors.New("count mismatch"),
	)
	fact := ClassifyEngineRetry(
		"postgres",
		EngineRetryReadOnly,
		explicit,
	)
	if fact.Class != ErrorClassValidation ||
		fact.Reason != "explicit_transfer_class" {
		t.Fatalf("explicit fact = %#v", fact)
	}
	if got := WrapEngineRetryError(
		"postgres",
		EngineRetryReadOnly,
		explicit,
	); got != explicit {
		t.Fatalf("explicit wrapper identity changed: %v", got)
	}
	for _, input := range []struct {
		engine   string
		boundary EngineRetryBoundary
	}{
		{
			engine:   "unknown",
			boundary: EngineRetryReadOnly,
		},
		{
			engine:   "postgres",
			boundary: EngineRetryBoundary("unknown"),
		},
		{
			engine:   "unknown",
			boundary: EngineRetryBoundary("unknown"),
		},
	} {
		fact := ClassifyEngineRetry(
			input.engine,
			input.boundary,
			explicit,
		)
		if fact.Class != ErrorClassValidation ||
			fact.Reason != "explicit_transfer_class" ||
			WrapEngineRetryError(
				input.engine,
				input.boundary,
				explicit,
			) != explicit {
			t.Fatalf(
				"explicit class lost for engine=%q boundary=%q: %#v",
				input.engine,
				input.boundary,
				fact,
			)
		}
	}
	explicitTransient := NewTransferError(
		ErrorClassTransient,
		io.ErrUnexpectedEOF,
	)
	suppressed := ClassifyEngineRetry(
		"postgres",
		EngineRetryUnknownCommit,
		explicitTransient,
	)
	if suppressed.Class != ErrorClassPermanent ||
		suppressed.Reason != "commit_outcome_unknown" ||
		IsRetryable(
			WrapEngineRetryError(
				"postgres",
				EngineRetryUnknownCommit,
				explicitTransient,
			),
		) {
		t.Fatalf("unknown-commit explicit transient = %#v", suppressed)
	}
	explicitTransientUnknownEngine := ClassifyEngineRetry(
		"unknown",
		EngineRetryIdempotent,
		explicitTransient,
	)
	if explicitTransientUnknownEngine.Class != ErrorClassTransient ||
		explicitTransientUnknownEngine.Reason !=
			"explicit_transfer_class" {
		t.Fatalf(
			"explicit transient unknown-engine fact = %#v",
			explicitTransientUnknownEngine,
		)
	}
	explicitTransientUnknownBoundary := ClassifyEngineRetry(
		"postgres",
		EngineRetryBoundary("unknown"),
		explicitTransient,
	)
	if explicitTransientUnknownBoundary.Class != ErrorClassPermanent ||
		explicitTransientUnknownBoundary.Reason !=
			"unknown_replay_boundary" {
		t.Fatalf(
			"explicit transient unknown-boundary fact = %#v",
			explicitTransientUnknownBoundary,
		)
	}
	invalidExplicit := engineRetryInvalidClassError{}
	invalid := ClassifyEngineRetry(
		"postgres",
		EngineRetryReadOnly,
		invalidExplicit,
	)
	if invalid.Class != ErrorClassPermanent ||
		invalid.Reason != "invalid_explicit_transfer_class" {
		t.Fatalf("invalid explicit fact = %#v", invalid)
	}

	canceled := ClassifyEngineRetry(
		"postgres",
		EngineRetryIdempotent,
		errors.Join(
			&pgconn.PgError{Code: "40001"},
			context.Canceled,
		),
	)
	if canceled.Class != ErrorClassCanceled ||
		canceled.Reason != "context_canceled" {
		t.Fatalf("canceled fact = %#v", canceled)
	}

	explicitTransientCanceled := NewTransferError(
		ErrorClassTransient,
		context.Canceled,
	)
	wrappedCanceled := WrapEngineRetryError(
		"postgres",
		EngineRetryIdempotent,
		explicitTransientCanceled,
	)
	if ClassifyTransferError(wrappedCanceled) != ErrorClassCanceled ||
		!errors.Is(wrappedCanceled, context.Canceled) {
		t.Fatalf(
			"explicit transient cancellation wrapper = %v",
			wrappedCanceled,
		)
	}
	var outer *TransferError
	if !errors.As(wrappedCanceled, &outer) ||
		outer.Class != ErrorClassCanceled {
		t.Fatalf(
			"cancellation did not replace explicit transient outer class: %v",
			wrappedCanceled,
		)
	}
}

func TestClassifyEngineRetryRejectsUnknownInputsAndIsDeterministic(
	t *testing.T,
) {
	t.Parallel()

	sourceError := &mysql.MySQLError{Number: 1213}
	unknownEngine := ClassifyEngineRetry(
		"oracle",
		EngineRetryRolledBack,
		sourceError,
	)
	if unknownEngine.Class != ErrorClassPermanent ||
		unknownEngine.Reason != "unknown_engine" ||
		unknownEngine.Engine != "" ||
		unknownEngine.Boundary != EngineRetryRolledBack {
		t.Fatalf("unknown-engine fact = %#v", unknownEngine)
	}
	unknownBoundary := ClassifyEngineRetry(
		"mysql",
		EngineRetryBoundary("guess"),
		sourceError,
	)
	if unknownBoundary.Class != ErrorClassPermanent ||
		unknownBoundary.Reason != "unknown_replay_boundary" ||
		unknownBoundary.Engine != "mysql" ||
		unknownBoundary.Boundary != "" {
		t.Fatalf("unknown-boundary fact = %#v", unknownBoundary)
	}
	none := ClassifyEngineRetry(
		"postgres",
		EngineRetryReadOnly,
		nil,
	)
	if none.Class != "" || none.Code != "" || none.Reason != "" {
		t.Fatalf("nil fact = %#v", none)
	}
	invalidPostgresCode := ClassifyEngineRetry(
		"postgres",
		EngineRetryReadOnly,
		&pgconn.PgError{
			Code:    "password=postgres-secret",
			Message: "password=message-secret",
		},
	)
	if invalidPostgresCode.Class != ErrorClassPermanent ||
		invalidPostgresCode.Code != "" ||
		invalidPostgresCode.Reason != "invalid_postgres_sqlstate" ||
		strings.Contains(
			fmt.Sprintf("%#v", invalidPostgresCode),
			"secret",
		) {
		t.Fatalf(
			"invalid PostgreSQL code fact = %#v",
			invalidPostgresCode,
		)
	}

	want := ClassifyEngineRetry(
		"mysql",
		EngineRetryRolledBack,
		sourceError,
	)
	const workers = 32
	results := make(chan EngineRetryFact, workers)
	for index := 0; index < workers; index++ {
		go func() {
			results <- ClassifyEngineRetry(
				"mysql",
				EngineRetryRolledBack,
				sourceError,
			)
		}()
	}
	for index := 0; index < workers; index++ {
		if got := <-results; !reflect.DeepEqual(got, want) {
			t.Fatalf("concurrent fact = %#v, want %#v", got, want)
		}
	}
}

func TestWrapEngineRetryErrorComposesWithBoundedRetry(t *testing.T) {
	t.Parallel()

	attempts := 0
	err := RetryWithPolicy(
		context.Background(),
		RetryPolicy{MaxRetries: 2},
		func(context.Context, int) error {
			attempts++
			if attempts < 3 {
				return WrapEngineRetryError(
					"postgres",
					EngineRetryRolledBack,
					&pgconn.PgError{Code: "40P01"},
				)
			}
			return nil
		},
	)
	if err != nil || attempts != 3 {
		t.Fatalf("rolled-back retry attempts=%d error=%v", attempts, err)
	}

	attempts = 0
	err = RetryWithPolicy(
		context.Background(),
		RetryPolicy{MaxRetries: 9},
		func(context.Context, int) error {
			attempts++
			return WrapEngineRetryError(
				"postgres",
				EngineRetryUnknownCommit,
				&pgconn.PgError{Code: "40P01"},
			)
		},
	)
	if err == nil ||
		ClassifyTransferError(err) != ErrorClassPermanent ||
		attempts != 1 {
		t.Fatalf("unknown-commit attempts=%d error=%v", attempts, err)
	}
}

type engineRetrySQLiteError struct {
	code int
	text string
}

func (err engineRetrySQLiteError) Error() string {
	if err.text != "" {
		return err.text
	}
	return "sqlite test error"
}
func (err engineRetrySQLiteError) Code() int { return err.code }

type engineRetryNetworkError struct{}

func (engineRetryNetworkError) Error() string   { return "network test error" }
func (engineRetryNetworkError) Timeout() bool   { return true }
func (engineRetryNetworkError) Temporary() bool { return true }

var _ net.Error = engineRetryNetworkError{}

type engineRetryInvalidClassError struct{}

func (engineRetryInvalidClassError) Error() string {
	return "invalid explicit class"
}

func (engineRetryInvalidClassError) TransferErrorClass() TransferErrorClass {
	return TransferErrorClass("guess")
}
