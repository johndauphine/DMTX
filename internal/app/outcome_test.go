package app

import (
	"context"
	"errors"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
)

func TestMigrationAttemptDisposition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		result    migrate.Result
		err       error
		allow     bool
		outcome   state.Outcome
		resumable bool
		accepted  bool
		exitCode  int
		suffix    string
	}{
		{
			name:    "ordinary failure",
			err:     errors.New("writer failed"),
			outcome: state.Failed, resumable: true, exitCode: TransferError, suffix: "failed",
		},
		{
			name:   "ordinary partial",
			result: migrate.Result{Rows: 1}, err: errors.New("writer failed"),
			outcome: state.Partial, resumable: true, exitCode: TransferError, suffix: "partial",
		},
		{
			name:   "accepted partial",
			result: migrate.Result{Tables: 1}, err: errors.New("writer failed"), allow: true,
			outcome: state.Partial, resumable: false, accepted: true, exitCode: Success, suffix: "partial_accepted",
		},
		{
			name:   "cancelled remains resumable",
			result: migrate.Result{Rows: 1}, err: context.Canceled, allow: true,
			outcome: state.Cancelled, resumable: true, exitCode: Cancelled, suffix: "cancelled",
		},
		{
			name:    "deadline remains cancelled",
			err:     context.DeadlineExceeded,
			outcome: state.Cancelled, resumable: true, exitCode: Cancelled, suffix: "cancelled",
		},
		{
			name:   "state failure partial cannot be accepted",
			result: migrate.Result{Rows: 1}, err: errors.Join(state.ErrState, errors.New("checkpoint failed")), allow: true,
			outcome: state.Partial, resumable: true, exitCode: StateError, suffix: "partial",
		},
		{
			name:    "classified state failure partial cannot be accepted",
			result:  migrate.Result{Rows: 1},
			err:     transferErrorForTest(migrate.ErrorClassState),
			allow:   true,
			outcome: state.Partial, resumable: true, exitCode: StateError, suffix: "partial",
		},
		{
			name:    "classified lease failure partial cannot be accepted",
			result:  migrate.Result{Rows: 1},
			err:     transferErrorForTest(migrate.ErrorClassLease),
			allow:   true,
			outcome: state.Partial, resumable: true, exitCode: StateError, suffix: "partial",
		},
		{
			name:   "lease loss partial cannot be accepted",
			result: migrate.Result{Rows: 1}, err: state.ErrLeaseLost, allow: true,
			outcome: state.Partial, resumable: true, exitCode: StateError, suffix: "partial",
		},
		{
			name: "zero progress cannot be accepted",
			err:  errors.New("preflight failed"), allow: true,
			outcome: state.Failed, resumable: true, exitCode: TransferError, suffix: "failed",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := migrationAttemptDisposition(
				test.result,
				test.err,
				config.Migration{AllowPartial: test.allow},
			)
			if got.outcome != test.outcome || got.resumable != test.resumable ||
				got.acceptedPartial != test.accepted || got.exitCode != test.exitCode ||
				got.auditSuffix != test.suffix {
				t.Fatalf("disposition = %#v, want outcome=%q resumable=%t accepted=%t exit=%d suffix=%q",
					got, test.outcome, test.resumable, test.accepted, test.exitCode, test.suffix)
			}
		})
	}
}

func transferErrorForTest(class migrate.TransferErrorClass) error {
	return migrate.NewTransferError(class, errors.New("injected classified failure"))
}
