package migrate

import (
	"context"
	"errors"
	"fmt"

	"github.com/johndauphine/dmtx/internal/state"
)

// Execution lifecycle: run activity, epoch ownership, and the durable
// evidence a strict execution carries.

func (execution *StrictConsistencyExecution) markRunInactive() {
	execution.runMu.Lock()
	execution.runActive = false
	execution.runMu.Unlock()
}

// BeginStrictConsistency establishes an engine-owned stable view, captures and
// persists exact same-view evidence, and only then returns executable state.
// Unsupported capability combinations fail before the opener is called.
func BeginStrictConsistency(
	ctx context.Context,
	request StrictConsistencyRequest,
	opener StrictConsistencyOpener,
) (*StrictConsistencyExecution, error) {
	normalized, err := normalizeStrictConsistencyRequest(request)
	if err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	if err := validateStrictConsistencyCapability(
		normalized.SourceEngine,
		normalized.Scope,
	); err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("strict consistency context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNilInterface(normalized.State) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("strict consistency state backend is required"),
		)
	}
	if isNilInterface(opener) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("strict consistency opener is required"),
		)
	}
	if err := requireStrictConsistencyWorkTasks(normalized); err != nil {
		return nil, NewTransferError(ErrorClassState, err)
	}
	existingEvidence, err := loadStrictConsistencyAttemptEvidence(normalized)
	if err != nil {
		return nil, NewTransferError(ErrorClassState, err)
	}

	var latest state.StrictMigrationSnapshot
	var latestFound bool
	if normalized.Scope == state.StrictSnapshotMigration &&
		(normalized.SourceEngine == StrictConsistencyPostgres ||
			normalized.SourceEngine == StrictConsistencyMSSQL) {
		latest, latestFound, err = normalized.State.
			LoadLatestStrictMigrationSnapshot(normalized.RunID)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf("load durable strict migration snapshot: %w", err),
			)
		}
		if latestFound {
			if err := validateDurableMigrationSnapshot(
				normalized,
				latest,
			); err != nil {
				return nil, NewTransferError(ErrorClassState, err)
			}
			if !normalized.Resume {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New(
						"a durable strict migration snapshot already exists; continuing this run requires explicit resume",
					),
				)
			}
		}
	}

	openRequest := StrictConsistencyOpenRequest{
		RunID:        normalized.RunID,
		SourceEngine: normalized.SourceEngine,
		Scope:        normalized.Scope,
		Resume:       normalized.Resume,
		ProcessEpoch: normalized.ProcessEpoch,
		Tables: append(
			[]StrictConsistencyTable(nil),
			normalized.Tables...,
		),
	}
	if normalized.SourceEngine == StrictConsistencyMSSQL &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume {
		if !latestFound {
			return nil, NewTransferError(
				ErrorClassState,
				errors.New(
					"SQL Server migration resume requires the surviving durable database snapshot; it is missing and a replacement source instant is forbidden",
				),
			)
		}
		if normalized.ProcessEpoch == latest.ProcessEpoch {
			return nil, NewTransferError(
				ErrorClassPolicy,
				errors.New(
					"SQL Server migration resume requires a fresh coordinator process epoch",
				),
			)
		}
		required := latest
		openRequest.RequiredMigrationSnapshot = &required
	}
	if normalized.SourceEngine == StrictConsistencyPostgres &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume && latestFound &&
		normalized.ProcessEpoch == latest.ProcessEpoch {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"PostgreSQL migration resume requires a fresh process epoch and a new exported snapshot",
			),
		)
	}

	session, err := opener.OpenStrictConsistency(ctx, openRequest)
	if err != nil {
		if normalized.SourceEngine == StrictConsistencyMSSQL &&
			normalized.Scope == state.StrictSnapshotMigration &&
			normalized.Resume {
			err = fmt.Errorf(
				"reopen surviving SQL Server database snapshot; resume fails closed because a replacement source instant is forbidden: %w",
				err,
			)
		}
		primary := NewTransferError(ErrorClassPermanent, err)
		primary = markMissingSQLServerMigrationSnapshotResume(
			normalized.SourceEngine,
			normalized.Scope,
			normalized.Resume,
			primary,
		)
		if !isNilInterface(session) {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				primary,
			)
		}
		return nil, primary
	}
	if isNilInterface(session) {
		return nil, NewTransferError(
			ErrorClassPermanent,
			errors.New("strict consistency opener returned a nil session"),
		)
	}

	capture, err := session.CaptureSameViewEvidence(ctx)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				fmt.Errorf("capture strict same-view evidence: %w", err),
			),
		)
	}
	if normalized.SourceEngine == StrictConsistencyPostgres &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume && latestFound &&
		(capture.MigrationEpochID == latest.EpochID ||
			capture.MigrationSnapshotReference == latest.SnapshotReference) {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				errors.New(
					"PostgreSQL migration resume must open a new exported snapshot epoch and reference",
				),
			),
		)
	}
	evidence, owner, err := buildStrictConsistencyEvidence(
		normalized,
		capture,
		openRequest.RequiredMigrationSnapshot,
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassPermanent, err),
		)
	}
	if err := reconcileStrictConsistencyAttemptEvidence(
		normalized,
		evidence,
		owner,
		existingEvidence,
	); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassState, err),
		)
	}
	for index := range evidence {
		if prior, found := existingEvidence[evidence[index].Task]; found {
			evidence[index] = prior
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
	}
	if owner != nil {
		if err := normalized.State.SaveStrictMigrationSnapshot(*owner); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"persist strict migration snapshot before target mutation: %w",
						err,
					),
				),
			)
		}
	}
	for _, record := range evidence {
		if err := ctx.Err(); err != nil {
			return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
		}
		if err := normalized.State.SaveStrictSnapshotEvidence(record); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"persist strict evidence for %s.%s before target mutation: %w",
						record.Task.Schema,
						record.Task.Table,
						err,
					),
				),
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
	}
	if err := requireStrictConsistencyWorkTasks(normalized); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"revalidate durable strict work immediately before authorization: %w",
					err,
				),
			),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(ctx, session, err)
	}

	execution := &StrictConsistencyExecution{
		runID:        normalized.RunID,
		sourceEngine: normalized.SourceEngine,
		scope:        normalized.Scope,
		processEpoch: normalized.ProcessEpoch,
		state:        normalized.State,
		tables: append(
			[]StrictConsistencyTable(nil),
			normalized.Tables...,
		),
		evidence: append(
			[]state.StrictSnapshotEvidence(nil),
			evidence...,
		),
		session: session,
	}
	if owner != nil {
		copyOwner := *owner
		execution.migrationSnapshot = &copyOwner
	}
	return execution, nil
}

// BeginPlannedStrictConsistency opens a stable source epoch before exact work
// planning, then withholds execution authority until the planner has durably
// checkpointed that exact work and the coordinator has persisted same-view
// count evidence. This is intentionally a distinct API from
// BeginStrictConsistency: silently opening a disposable planning snapshot
// would violate migration-scoped point-in-time semantics.
func BeginPlannedStrictConsistency(
	ctx context.Context,
	request PlannedStrictConsistencyRequest,
	opener PlannedStrictConsistencyOpener,
	plan PlannedStrictConsistencyPlanner,
) (*StrictConsistencyExecution, error) {
	normalized, err := normalizePlannedStrictConsistencyRequest(request)
	if err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	if err := validateStrictConsistencyCapability(
		normalized.SourceEngine,
		normalized.Scope,
	); err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("planned strict consistency context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNilInterface(normalized.State) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New(
				"planned strict consistency state backend is required",
			),
		)
	}
	if isNilInterface(opener) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("planned strict consistency opener is required"),
		)
	}
	if isNilInterface(plan) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			errors.New("planned strict consistency planner is required"),
		)
	}

	var latest state.StrictMigrationSnapshot
	var latestFound bool
	var requiredOwner *state.StrictMigrationSnapshot
	resumeUnownedSnapshot := false
	if normalized.Scope == state.StrictSnapshotMigration {
		latest, latestFound, err = normalized.State.
			LoadLatestStrictMigrationSnapshot(normalized.RunID)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"load durable planned strict migration snapshot: %w",
					err,
				),
			)
		}
		if latestFound {
			base := StrictConsistencyRequest{
				RunID:        normalized.RunID,
				SourceEngine: normalized.SourceEngine,
				Scope:        normalized.Scope,
				Resume:       normalized.Resume,
				ProcessEpoch: normalized.ProcessEpoch,
				State:        normalized.State,
			}
			if err := validateDurableMigrationSnapshot(
				base,
				latest,
			); err != nil {
				return nil, NewTransferError(ErrorClassState, err)
			}
			if !normalized.Resume {
				return nil, NewTransferError(
					ErrorClassState,
					errors.New(
						"a durable strict migration snapshot already exists; continuing this run requires explicit resume",
					),
				)
			}
			if normalized.SourceEngine == StrictConsistencyPostgres &&
				normalized.ProcessEpoch == latest.ProcessEpoch {
				return nil, NewTransferError(
					ErrorClassPolicy,
					errors.New(
						"PostgreSQL migration resume requires a fresh process epoch and a new exported snapshot",
					),
				)
			}
		}
		if normalized.SourceEngine == StrictConsistencyMSSQL &&
			normalized.Resume {
			if !latestFound {
				// The snapshot is created before SaveStrictMigrationSnapshot.
				// A hard process stop in that tiny interval leaves an authenticated,
				// deterministic SQL Server snapshot but no state owner. Permit only
				// its exact reopening; the source opener refuses to create a new one.
				resumeUnownedSnapshot = true
			} else {
				if normalized.ProcessEpoch == latest.ProcessEpoch {
					return nil, NewTransferError(
						ErrorClassPolicy,
						errors.New(
							"SQL Server migration resume requires a fresh coordinator process epoch",
						),
					)
				}
				copyOwner := latest
				requiredOwner = &copyOwner
			}
		}
	}

	session, err := opener.OpenPlannedStrictConsistency(
		ctx,
		PlannedStrictConsistencyOpenRequest{
			RunID:        normalized.RunID,
			SourceEngine: normalized.SourceEngine,
			Scope:        normalized.Scope,
			Resume:       normalized.Resume,
			ProcessEpoch: normalized.ProcessEpoch,
			Tasks: append(
				[]state.TaskKey(nil),
				normalized.Tasks...,
			),
			RequiredMigrationSnapshot:      requiredOwner,
			ReopenUnownedMigrationSnapshot: resumeUnownedSnapshot,
		},
	)
	if err != nil {
		primary := NewTransferError(ErrorClassPermanent, err)
		primary = markMissingSQLServerMigrationSnapshotResume(
			normalized.SourceEngine,
			normalized.Scope,
			normalized.Resume,
			primary,
		)
		if !isNilInterface(session) {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				primary,
			)
		}
		return nil, primary
	}
	if isNilInterface(session) {
		return nil, NewTransferError(
			ErrorClassPermanent,
			errors.New(
				"planned strict consistency opener returned a nil session",
			),
		)
	}

	capture, err := session.CaptureSameViewEvidence(ctx)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				fmt.Errorf(
					"capture planned strict same-view evidence: %w",
					err,
				),
			),
		)
	}
	if err := validateUnboundStrictConsistencyCapture(
		normalized,
		capture,
	); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassPermanent, err),
		)
	}
	if normalized.SourceEngine == StrictConsistencyPostgres &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume && latestFound &&
		(capture.MigrationEpochID == latest.EpochID ||
			capture.MigrationSnapshotReference ==
				latest.SnapshotReference) {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				errors.New(
					"PostgreSQL migration resume must open a new exported snapshot epoch and reference",
				),
			),
		)
	}
	if normalized.SourceEngine == StrictConsistencyMSSQL &&
		normalized.Scope == state.StrictSnapshotMigration &&
		normalized.Resume && latestFound &&
		(capture.MigrationEpochID != latest.EpochID ||
			capture.MigrationSnapshotReference != latest.SnapshotReference) {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassPermanent,
				errors.New(
					"SQL Server migration resume did not reopen the required durable database snapshot",
				),
			),
		)
	}

	finalized, err := plan(
		ctx,
		session,
		cloneStrictConsistencyCapture(capture),
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			err,
		)
	}
	finalRequest, err := normalizeStrictConsistencyRequest(
		StrictConsistencyRequest{
			RunID:        normalized.RunID,
			SourceEngine: normalized.SourceEngine,
			Scope:        normalized.Scope,
			Resume:       normalized.Resume,
			ProcessEpoch: normalized.ProcessEpoch,
			State:        normalized.State,
			Tables:       finalized,
		},
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassPolicy, err),
		)
	}
	if err := requirePlannedStrictConsistencyTaskSet(
		normalized.Tasks,
		finalRequest.Tables,
	); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassState, err),
		)
	}
	if err := requireStrictConsistencyWorkTasks(finalRequest); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassState, err),
		)
	}
	existingEvidence, err := loadStrictConsistencyAttemptEvidence(
		finalRequest,
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassState, err),
		)
	}
	capture, err = bindPlannedStrictConsistencyAttempts(
		capture,
		finalRequest.Tables,
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassPermanent, err),
		)
	}
	evidence, owner, err := buildStrictConsistencyEvidence(
		finalRequest,
		capture,
		requiredOwner,
	)
	if err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassPermanent, err),
		)
	}
	if err := reconcileStrictConsistencyAttemptEvidence(
		finalRequest,
		evidence,
		owner,
		existingEvidence,
	); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(ErrorClassState, err),
		)
	}
	if owner != nil {
		if err := normalized.State.SaveStrictMigrationSnapshot(
			*owner,
		); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"persist planned strict migration snapshot before target mutation: %w",
						err,
					),
				),
			)
		}
	}
	for _, record := range evidence {
		if err := ctx.Err(); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				err,
			)
		}
		if err := normalized.State.SaveStrictSnapshotEvidence(
			record,
		); err != nil {
			return nil, closeStrictConsistencyAfterFailure(
				ctx,
				session,
				NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"persist planned strict evidence for %s.%s before target mutation: %w",
						record.Task.Schema,
						record.Task.Table,
						err,
					),
				),
			)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			err,
		)
	}
	if err := requireStrictConsistencyWorkTasks(finalRequest); err != nil {
		return nil, closeStrictConsistencyAfterFailure(
			ctx,
			session,
			NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"revalidate durable planned strict work immediately before authorization: %w",
					err,
				),
			),
		)
	}

	execution := &StrictConsistencyExecution{
		runID:        finalRequest.RunID,
		sourceEngine: finalRequest.SourceEngine,
		scope:        finalRequest.Scope,
		processEpoch: finalRequest.ProcessEpoch,
		state:        finalRequest.State,
		tables: append(
			[]StrictConsistencyTable(nil),
			finalRequest.Tables...,
		),
		evidence: append(
			[]state.StrictSnapshotEvidence(nil),
			evidence...,
		),
		session: session,
	}
	if owner != nil {
		copyOwner := *owner
		execution.migrationSnapshot = &copyOwner
	}
	return execution, nil
}
