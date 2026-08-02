package migrate

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"syscall"

	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
)

// EngineRetryBoundary states what the caller can prove about replaying the
// failed operation. Error shape alone never proves that a target write is safe
// to repeat after a lost acknowledgement.
type EngineRetryBoundary string

const (
	// EngineRetryReadOnly admits transient read/query failures.
	EngineRetryReadOnly EngineRetryBoundary = "read_only"
	// EngineRetryPreMutation admits connection and resource failures before the
	// first target mutation.
	EngineRetryPreMutation EngineRetryBoundary = "pre_mutation"
	// EngineRetryRolledBack admits failures whose engine contract proves the
	// complete attempted transaction was rolled back.
	EngineRetryRolledBack EngineRetryBoundary = "rolled_back"
	// EngineRetryIdempotent admits complete replay through an independently
	// proven idempotent or duplicate-safe path.
	EngineRetryIdempotent EngineRetryBoundary = "idempotent"
	// EngineRetryUnknownCommit deliberately admits no automatic retry.
	EngineRetryUnknownCommit EngineRetryBoundary = "unknown_commit"
)

// EngineRetryFact is a deterministic, secret-free explanation of an engine
// error classification. Code contains only a SQLSTATE, numeric server code,
// or a fixed transport token; it never contains the driver message.
type EngineRetryFact struct {
	Engine   string
	Boundary EngineRetryBoundary
	Class    TransferErrorClass
	Code     string
	Reason   string
}

// ClassifyEngineRetry classifies a raw driver error at an explicit replay
// boundary. Existing TransferError classifications and cancellation always
// win. Unrecognized errors and unknown-commit operations remain permanent.
func ClassifyEngineRetry(
	engine string,
	boundary EngineRetryBoundary,
	err error,
) EngineRetryFact {
	fact := EngineRetryFact{
		Class:  ErrorClassPermanent,
		Reason: "unclassified",
	}
	if knownRetryEngine(engine) {
		fact.Engine = engine
	}
	if knownEngineRetryBoundary(boundary) {
		fact.Boundary = boundary
	}
	if err == nil {
		fact.Class = ""
		fact.Reason = ""
		return fact
	}
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		fact.Class = ErrorClassCanceled
		fact.Reason = "context_canceled"
		return fact
	}
	var explicitlyClassified transferErrorClassifier
	if errors.As(err, &explicitlyClassified) {
		class := explicitlyClassified.TransferErrorClass()
		if !isKnownTransferErrorClass(class) {
			fact.Reason = "invalid_explicit_transfer_class"
			return fact
		}
		if class == ErrorClassTransient {
			if !knownEngineRetryBoundary(boundary) {
				fact.Reason = "unknown_replay_boundary"
				return fact
			}
			if boundary == EngineRetryUnknownCommit {
				fact.Reason = "commit_outcome_unknown"
				return fact
			}
		}
		fact.Class = class
		fact.Reason = "explicit_transfer_class"
		return fact
	}
	if !knownEngineRetryBoundary(boundary) {
		fact.Reason = "unknown_replay_boundary"
		return fact
	}
	if !knownRetryEngine(engine) {
		fact.Reason = "unknown_engine"
		return fact
	}
	if boundary == EngineRetryUnknownCommit {
		fact.Reason = "commit_outcome_unknown"
		return fact
	}

	switch engine {
	case "postgres":
		classifyPostgresRetry(&fact, boundary, err)
	case "mysql", "mariadb":
		classifyMySQLRetry(&fact, boundary, err)
	case "mssql":
		classifySQLServerRetry(&fact, boundary, err)
	case "sqlite":
		classifySQLiteRetry(&fact, boundary, err)
	case "clickhouse":
		classifyClickHouseRetry(&fact, boundary, err)
	}
	// A structural server error proves that the server parsed and rejected the
	// operation. It is stronger evidence than a generic transport cause joined
	// to the same error tree and must win conservatively.
	if fact.Reason != "unclassified" {
		return fact
	}
	if code, reason, safeBeforeSend := transportRetryEvidence(err); code != "" {
		fact.Code = code
		fact.Reason = reason
		if boundaryAllowsTransportRetry(boundary, safeBeforeSend) {
			fact.Class = ErrorClassTransient
		}
	}
	return fact
}

// WrapEngineRetryError attaches the boundary-aware class without replacing
// the original cause. Callers may pass the result to Retry/RetryWithPolicy.
func WrapEngineRetryError(
	engine string,
	boundary EngineRetryBoundary,
	err error,
) error {
	if err == nil {
		return nil
	}
	fact := ClassifyEngineRetry(engine, boundary, err)
	var explicitlyClassified transferErrorClassifier
	if errors.As(err, &explicitlyClassified) {
		class := explicitlyClassified.TransferErrorClass()
		if isKnownTransferErrorClass(class) &&
			class == fact.Class &&
			fact.Reason == "explicit_transfer_class" {
			return err
		}
	}
	return NewTransferError(fact.Class, err)
}

func knownRetryEngine(engine string) bool {
	switch engine {
	case "postgres", "mysql", "mariadb", "mssql", "sqlite", "clickhouse":
		return true
	default:
		return false
	}
}

func knownEngineRetryBoundary(boundary EngineRetryBoundary) bool {
	switch boundary {
	case EngineRetryReadOnly,
		EngineRetryPreMutation,
		EngineRetryRolledBack,
		EngineRetryIdempotent,
		EngineRetryUnknownCommit:
		return true
	default:
		return false
	}
}

func boundaryAllowsTransportRetry(
	boundary EngineRetryBoundary,
	safeBeforeSend bool,
) bool {
	switch boundary {
	case EngineRetryReadOnly,
		EngineRetryPreMutation,
		EngineRetryIdempotent:
		return true
	case EngineRetryRolledBack:
		return safeBeforeSend
	default:
		return false
	}
}

func boundaryAllowsRolledBackRetry(boundary EngineRetryBoundary) bool {
	switch boundary {
	case EngineRetryReadOnly,
		EngineRetryPreMutation,
		EngineRetryRolledBack,
		EngineRetryIdempotent:
		return true
	default:
		return false
	}
}

func transportRetryEvidence(err error) (
	code string,
	reason string,
	safeBeforeSend bool,
) {
	switch {
	case errors.Is(err, driver.ErrBadConn):
		return "driver_bad_connection", "connection_unavailable_before_use", true
	case errors.Is(err, net.ErrClosed):
		return "network_closed", "connection_closed", false
	case errors.Is(err, io.ErrUnexpectedEOF):
		return "unexpected_eof", "connection_interrupted", false
	case errors.Is(err, io.EOF):
		return "eof", "connection_interrupted", false
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset", "connection_interrupted", false
	case errors.Is(err, syscall.ECONNABORTED):
		return "connection_aborted", "connection_interrupted", false
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused", "connection_unavailable", true
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe", "connection_interrupted", false
	}
	// net.Error.Temporary is deprecated and deliberately retained. Go's guidance
	// is that most temporary errors are timeouts, which Timeout covers, and the
	// rest are ill-defined. For a migration tool the asymmetry matters: treating
	// a transient network fault as permanent fails a migration outright, while
	// retrying an unretryable one costs an attempt. Dropping this narrows retry
	// classification for a lint result, which is the wrong trade. Revisit only
	// with evidence about which concrete errors it still admits.
	var networkError net.Error
	if errors.As(err, &networkError) &&
		//nolint:staticcheck // SA1019: see the note above.
		(networkError.Timeout() || networkError.Temporary()) {
		return "network_temporary", "temporary_network_failure", false
	}
	return "", "", false
}

func classifyPostgresRetry(
	fact *EngineRetryFact,
	boundary EngineRetryBoundary,
	err error,
) {
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) {
		if !validPostgresSQLState(postgresError.Code) {
			fact.Reason = "invalid_postgres_sqlstate"
			return
		}
		fact.Code = postgresError.Code
		switch {
		case postgresError.Code == "40001":
			fact.Reason = "serialization_failure"
		case postgresError.Code == "40P01":
			fact.Reason = "deadlock_victim"
		case postgresError.Code == "55P03":
			fact.Reason = "lock_unavailable"
		case postgresError.Code == "53300":
			fact.Reason = "connection_capacity"
		case postgresError.Code == "57P01" ||
			postgresError.Code == "57P02" ||
			postgresError.Code == "57P03":
			fact.Reason = "server_temporarily_unavailable"
		case strings.HasPrefix(postgresError.Code, "08"):
			fact.Reason = "connection_exception"
		default:
			fact.Reason = "postgres_permanent"
			return
		}
		if boundaryAllowsRolledBackRetry(boundary) {
			fact.Class = ErrorClassTransient
		}
		return
	}
	if errors.Is(err, pgconn.ErrConnClosed) {
		fact.Code = "postgres_connection_closed"
		fact.Reason = "connection_closed"
		if boundaryAllowsTransportRetry(boundary, false) {
			fact.Class = ErrorClassTransient
		}
		return
	}
	if pgconn.SafeToRetry(err) {
		fact.Code = "postgres_safe_before_send"
		fact.Reason = "connection_unavailable_before_use"
		if boundaryAllowsTransportRetry(boundary, true) {
			fact.Class = ErrorClassTransient
		}
		return
	}
}

func validPostgresSQLState(code string) bool {
	if len(code) != 5 {
		return false
	}
	for _, character := range code {
		if character >= '0' && character <= '9' ||
			character >= 'A' && character <= 'Z' {
			continue
		}
		return false
	}
	return true
}

func classifyMySQLRetry(
	fact *EngineRetryFact,
	boundary EngineRetryBoundary,
	err error,
) {
	var mysqlError *mysql.MySQLError
	if errors.As(err, &mysqlError) {
		fact.Code = fmt.Sprintf("%d", mysqlError.Number)
		switch mysqlError.Number {
		case 1040:
			fact.Reason = "connection_capacity"
		case 1053:
			fact.Reason = "server_shutdown"
		case 1158, 1159, 1160, 1161:
			fact.Reason = "connection_interrupted"
		case 1205:
			fact.Reason = "lock_timeout"
		case 1213:
			fact.Reason = "deadlock_victim"
		case 3572:
			fact.Reason = "lock_unavailable"
		default:
			fact.Reason = "mysql_permanent"
			return
		}
		if boundaryAllowsRolledBackRetry(boundary) {
			fact.Class = ErrorClassTransient
		}
		return
	}
	if errors.Is(err, mysql.ErrInvalidConn) {
		fact.Code = "mysql_invalid_connection"
		fact.Reason = "connection_closed"
		if boundaryAllowsTransportRetry(boundary, false) {
			fact.Class = ErrorClassTransient
		}
		return
	}
}

type engineRetrySQLServerNumberedError interface {
	error
	SQLErrorNumber() int32
}

func classifySQLServerRetry(
	fact *EngineRetryFact,
	boundary EngineRetryBoundary,
	err error,
) {
	var sqlServerError engineRetrySQLServerNumberedError
	if !errors.As(err, &sqlServerError) {
		return
	}
	number := sqlServerError.SQLErrorNumber()
	fact.Code = fmt.Sprintf("%d", number)
	switch number {
	case 64, 233, 10053, 10054, 10060:
		fact.Reason = "connection_interrupted"
	case 1205:
		fact.Reason = "deadlock_victim"
	case 1222:
		fact.Reason = "lock_timeout"
	case 8645, 8651:
		fact.Reason = "temporary_resource_pressure"
	case 10928, 10929, 40197, 40501, 40613, 49918, 49919, 49920:
		fact.Reason = "server_temporarily_unavailable"
	default:
		fact.Reason = "mssql_permanent"
		return
	}
	if boundaryAllowsRolledBackRetry(boundary) {
		fact.Class = ErrorClassTransient
	}
}

type sqliteCodedError interface {
	error
	Code() int
}

func classifySQLiteRetry(
	fact *EngineRetryFact,
	boundary EngineRetryBoundary,
	err error,
) {
	var sqliteError sqliteCodedError
	if !errors.As(err, &sqliteError) {
		return
	}
	// SQLite extended result codes retain the primary result in the low byte.
	code := sqliteError.Code()
	primary := code & 0xff
	fact.Code = fmt.Sprintf("%d", code)
	switch primary {
	case 5:
		fact.Reason = "database_busy"
	case 6:
		fact.Reason = "database_locked"
	default:
		fact.Reason = "sqlite_permanent"
		return
	}
	if boundaryAllowsRolledBackRetry(boundary) {
		fact.Class = ErrorClassTransient
	}
}

func classifyClickHouseRetry(
	fact *EngineRetryFact,
	boundary EngineRetryBoundary,
	err error,
) {
	var exception *clickhouseproto.Exception
	if !errors.As(err, &exception) {
		return
	}
	fact.Code = fmt.Sprintf("%d", exception.Code)
	switch exception.Code {
	case 202:
		fact.Reason = "too_many_simultaneous_queries"
	case 203:
		fact.Reason = "no_free_connection"
	default:
		fact.Reason = "clickhouse_permanent"
		return
	}
	if boundaryAllowsRolledBackRetry(boundary) {
		fact.Class = ErrorClassTransient
	}
}
