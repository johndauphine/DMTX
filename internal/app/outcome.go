package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

type attemptDisposition struct {
	outcome         state.Outcome
	resumable       bool
	acceptedPartial bool
	exitCode        int
	auditSuffix     string
}

type acceptedPartialResult struct {
	migrate.Result
	Outcome   state.Outcome `json:"outcome"`
	Resumable bool          `json:"resumable"`
}

const runtimeTuningAuditEvent = "runtime_tuning"

// runtimeTuningAuditPayload is deliberately separate from the runtime report
// itself. It records only the terminal invocation context needed to interpret
// a controller snapshot and never retains the migration error text, source
// values, or credentials.
type runtimeTuningAuditPayload struct {
	Invocation    string                      `json:"invocation"`
	Disposition   string                      `json:"disposition"`
	Outcome       state.Outcome               `json:"outcome"`
	Resumable     bool                        `json:"resumable"`
	ExitCode      int                         `json:"exit_code"`
	ExitClass     string                      `json:"exit_class"`
	ErrorClass    migrate.TransferErrorClass  `json:"error_class"`
	RuntimeTuning migrate.RuntimeTuningReport `json:"runtime_tuning"`
}

// appendAttemptTerminalAudit preserves terminal audit ordering for ordinary
// migration failures. When a transfer reached a runtime-tuning controller, its
// redacted report must be durable before the terminal outcome fact. This is a
// library/API audit surface only: callers retain the existing stdout contract.
func appendAttemptTerminalAudit(
	configPath string,
	runID string,
	invocation string,
	result migrate.Result,
	disposition attemptDisposition,
	cause error,
) error {
	if result.RuntimeTuning != nil {
		payload := runtimeTuningAuditPayload{
			Invocation:    invocation,
			Disposition:   disposition.auditSuffix,
			Outcome:       disposition.outcome,
			Resumable:     disposition.resumable,
			ExitCode:      disposition.exitCode,
			ExitClass:     migrationExitClass(disposition.exitCode),
			ErrorClass:    migrate.ClassifyTransferError(cause),
			RuntimeTuning: *result.RuntimeTuning,
		}
		if err := appendAuditPayload(
			configPath,
			runID,
			runtimeTuningAuditEvent,
			payload,
		); err != nil {
			return fmt.Errorf("record runtime tuning audit: %w", err)
		}
	}
	if err := appendAudit(
		configPath,
		runID,
		invocation+"_"+disposition.auditSuffix,
	); err != nil {
		return err
	}
	return nil
}

// migrationExitClass is the operator-facing classification of the process
// outcome. It intentionally remains distinct from ErrorClass: an underlying
// transient transfer can still end in a state-class exit if the lease or
// outcome checkpoint fails after the transfer boundary.
func migrationExitClass(exitCode int) string {
	switch exitCode {
	case Success:
		return "success"
	case ConfigurationError:
		return "configuration"
	case ConnectionError:
		return "connection"
	case TransferError:
		return "transfer"
	case ValidationError:
		return "validation"
	case Cancelled:
		return "cancelled"
	case StateError:
		return "state"
	case FileError:
		return "file"
	default:
		return "unknown"
	}
}

// migrationAttemptDisposition keeps outcome, resumability, and process exit
// status separate. In particular, allow_partial never converts cancellation,
// lease/state loss, or a zero-progress failure into success.
func migrationAttemptDisposition(
	result migrate.Result,
	err error,
	migration config.Migration,
) attemptDisposition {
	snapshotReleased := errors.Is(
		err,
		migrate.ErrSQLServerMigrationSnapshotNotResumable,
	)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return attemptDisposition{
			outcome:     state.Cancelled,
			resumable:   !snapshotReleased,
			exitCode:    Cancelled,
			auditSuffix: attemptAuditSuffix("cancelled", snapshotReleased),
		}
	}

	hasProgress := result.Tables > 0 || result.Rows > 0
	if hasProgress {
		disposition := attemptDisposition{
			outcome:     state.Partial,
			resumable:   !snapshotReleased,
			exitCode:    migrationExitCode(err),
			auditSuffix: attemptAuditSuffix("partial", snapshotReleased),
		}
		if !snapshotReleased && migration.AllowPartial &&
			!isStateOrLeaseFailure(err) &&
			!errors.Is(err, migrate.ErrDestructiveAcknowledgement) {
			disposition.resumable = false
			disposition.acceptedPartial = true
			disposition.exitCode = Success
			disposition.auditSuffix = "partial_accepted"
		}
		return disposition
	}

	return attemptDisposition{
		outcome:     state.Failed,
		resumable:   !snapshotReleased,
		exitCode:    migrationExitCode(err),
		auditSuffix: attemptAuditSuffix("failed", snapshotReleased),
	}
}

func attemptAuditSuffix(base string, snapshotReleased bool) string {
	if snapshotReleased {
		return base + "_snapshot_released"
	}
	return base
}

func isStateOrLeaseFailure(err error) bool {
	if errors.Is(err, state.ErrState) || errors.Is(err, state.ErrLeaseLost) {
		return true
	}
	switch migrate.ClassifyTransferError(err) {
	case migrate.ErrorClassState, migrate.ErrorClassLease:
		return true
	default:
		return false
	}
}

func persistAttemptDisposition(
	store state.Backend,
	runID string,
	disposition attemptDisposition,
	reason string,
	endedAt time.Time,
) error {
	if disposition.resumable || disposition.acceptedPartial {
		if err := store.UpdateRecoverableOutcome(
			runID,
			disposition.outcome,
			reason,
			endedAt,
		); err != nil {
			return err
		}
	} else {
		terminal, ok := store.(state.TerminalOutcomeBackend)
		if !ok {
			return errors.New("state backend cannot persist a non-resumable terminal migration outcome")
		}
		if err := terminal.UpdateNonResumableOutcome(
			runID,
			disposition.outcome,
			reason,
			endedAt,
		); err != nil {
			return err
		}
	}
	if !disposition.acceptedPartial {
		return nil
	}
	return store.AbandonRun(
		runID,
		fmt.Sprintf("accepted partial outcome: %s", reason),
		endedAt,
	)
}
