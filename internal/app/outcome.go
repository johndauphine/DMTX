package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
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

// migrationAttemptDisposition keeps outcome, resumability, and process exit
// status separate. In particular, allow_partial never converts cancellation,
// lease/state loss, or a zero-progress failure into success.
func migrationAttemptDisposition(
	result migrate.Result,
	err error,
	migration config.Migration,
) attemptDisposition {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return attemptDisposition{
			outcome:     state.Cancelled,
			resumable:   true,
			exitCode:    Cancelled,
			auditSuffix: "cancelled",
		}
	}

	hasProgress := result.Tables > 0 || result.Rows > 0
	if hasProgress {
		disposition := attemptDisposition{
			outcome:     state.Partial,
			resumable:   true,
			exitCode:    migrationExitCode(err),
			auditSuffix: "partial",
		}
		if migration.AllowPartial &&
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
		resumable:   true,
		exitCode:    migrationExitCode(err),
		auditSuffix: "failed",
	}
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
	if err := store.UpdateRecoverableOutcome(
		runID,
		disposition.outcome,
		reason,
		endedAt,
	); err != nil {
		return err
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
