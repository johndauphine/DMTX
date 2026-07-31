package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4AdapterPostgresDeleteTransferredTable authenticates the crash window
// after every network range has reached its durable frontier but before the
// ordinary table checkpoint has necessarily been published. Keeping the
// final work identity prevents resume from resetting the plan and selecting a
// different delete attempt.
type stage4AdapterPostgresDeleteTransferredTable struct {
	work                  stage4AdapterWork
	rows                  int
	taskCompleted         bool
	ordinaryCompleted     bool
	terminalAuthenticated bool
	terminalStrict        bool
}

func (
	execution *stage4AdapterNetworkExecution,
) bindStage4AdapterPostgresDeleteTransferredTable(
	planIndex int,
	bound stage4AdapterPostgresDeleteTransferredTable,
) error {
	if execution == nil || execution.prepared.deletes == nil ||
		planIndex < 0 || planIndex >= len(execution.prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete transfer binding is unavailable"),
		)
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if execution.deleteTransferred == nil {
		execution.deleteTransferred = make(
			map[int]stage4AdapterPostgresDeleteTransferredTable,
			len(execution.prepared.plans),
		)
	}
	if _, duplicate := execution.deleteTransferred[planIndex]; duplicate {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete transfer for table %s was bound twice",
				execution.prepared.plans[planIndex].source.Name,
			),
		)
	}
	bound.work = cloneStage4AdapterNetworkWork(bound.work)
	execution.deleteTransferred[planIndex] = bound
	return nil
}

func (
	execution *stage4AdapterNetworkExecution,
) stage4AdapterPostgresDeleteTransferredTable(
	planIndex int,
) (stage4AdapterPostgresDeleteTransferredTable, bool) {
	if execution == nil {
		return stage4AdapterPostgresDeleteTransferredTable{}, false
	}
	execution.mu.Lock()
	defer execution.mu.Unlock()
	bound, found := execution.deleteTransferred[planIndex]
	bound.work = cloneStage4AdapterNetworkWork(bound.work)
	return bound, found
}

// classifyStage4AdapterPostgresDeleteTransferredTable is read-only. Partial
// work deliberately returns found=false so the ordinary reset/replay path can
// recover it. Fully transferred work is reconstructed from durable bounds and
// never from a fresh source pagination observation.
func (
	execution *stage4AdapterNetworkExecution,
) classifyStage4AdapterPostgresDeleteTransferredTable(
	ctx context.Context,
	planIndex int,
) (stage4AdapterPostgresDeleteTransferredTable, bool, error) {
	var result stage4AdapterPostgresDeleteTransferredTable
	if execution == nil || execution.prepared.deletes == nil ||
		planIndex < 0 || planIndex >= len(execution.prepared.work) {
		return result, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete durable transfer inventory is unavailable"),
		)
	}
	inventory, err := loadStage4WorkInventory(ctx, execution.prepared.run)
	if err != nil {
		return result, false, err
	}
	if err := execution.validateDurableTableTaskInventory(inventory); err != nil {
		return result, false, err
	}
	base := execution.prepared.work[planIndex]
	task, found := inventory.tasks[base.task]
	if !found {
		return result, false, nil
	}
	if task.Status != "running" && task.Status != "completed" {
		return result, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete table %s has unsafe durable task status %q",
				base.task.Table,
				task.Status,
			),
		)
	}
	item, err := execution.reconstructCompletedTableWork(planIndex, inventory)
	if err != nil {
		return result, false, err
	}
	task, ranges, _, err := exactStage4AdapterWork(inventory, item, false)
	if err != nil {
		return result, false, err
	}
	for _, workRange := range ranges {
		if workRange.Status != "completed" {
			return result, false, nil
		}
	}
	if task.RunID != execution.prepared.run.RunID ||
		task.Error != "" || task.StartedAt.IsZero() ||
		task.UpdatedAt.IsZero() || task.UpdatedAt.Before(task.StartedAt) {
		return result, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete table %s has malformed durable task evidence",
				item.task.Table,
			),
		)
	}
	switch task.Status {
	case "running":
		if !task.CompletedAt.IsZero() {
			return result, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"running Stage 4 PostgreSQL delete table %s has a completion timestamp",
					item.task.Table,
				),
			)
		}
	case "completed":
		if task.CompletedAt.IsZero() ||
			!task.UpdatedAt.Equal(task.CompletedAt) {
			return result, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 PostgreSQL delete table %s has malformed completion evidence",
					item.task.Table,
				),
			)
		}
	}

	var rows int64
	for index, workRange := range ranges {
		restore, restoreErr := networkRestoreFromState(
			networkStateRangeBinding{
				RangeIndex: uint64(index),
				Task:       item.task,
				Initial:    item.ranges[index],
			},
			workRange,
		)
		if restoreErr != nil ||
			workRange.RunID != execution.prepared.run.RunID ||
			workRange.Task != item.task || workRange.Error != "" ||
			workRange.CommittedPrefix != 0 ||
			workRange.UpdatedAt.IsZero() || workRange.CompletedAt.IsZero() ||
			!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
			workRange.CompletedAt.Before(task.StartedAt) ||
			task.Status == "completed" && workRange.CompletedAt.After(task.CompletedAt) ||
			!restore.Complete || restore.SequenceOffset != 0 ||
			len(restore.Issued) != 0 ||
			workRange.RowsDone > math.MaxInt64-rows {
			if restoreErr == nil {
				restoreErr = fmt.Errorf("completed range retains incomplete or overflowing work")
			}
			return result, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete table %s has invalid durable range %q: %w",
					item.task.Table,
					workRange.ID,
					restoreErr,
				),
			)
		}
		if err := validateCompletedStage4AdapterRange(
			task,
			item,
			index,
			workRange,
			restore,
		); err != nil {
			return result, false, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete table %s range %q has invalid strategy evidence: %w",
					item.task.Table,
					workRange.ID,
					err,
				),
			)
		}
		rows += workRange.RowsDone
	}
	if rows < 0 || uint64(rows) > uint64(^uint(0)>>1) {
		return result, false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete table %s row total exceeds the platform integer range",
				item.task.Table,
			),
		)
	}
	result.work = item
	result.rows = int(rows)
	result.taskCompleted = task.Status == "completed"
	return result, true, nil
}

func authenticateStage4AdapterPostgresDeleteTerminal(
	ctx context.Context,
	composition *stage4AdapterPostgresDeletePrepared,
	planIndex int,
	work stage4AdapterWork,
) (bool, error) {
	if composition == nil || planIndex < 0 ||
		planIndex >= len(composition.entries) {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete terminal evidence inventory is unavailable"),
		)
	}
	if ctx == nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete terminal context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	entry := composition.entries[planIndex]
	reconciler, request, err := composition.requestFor(
		entry,
		work,
	)
	if err != nil {
		return false, NewTransferError(ErrorClassState, err)
	}
	currentAuthority, err := composition.currentCanonicalizer(ctx, entry)
	if err != nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"derive current terminal Stage 4 PostgreSQL delete authority for table %s: %w",
				work.task.Table,
				err,
			),
		)
	}
	reconciler.canonicalizer = currentAuthority
	if err := ctx.Err(); err != nil {
		return false, err
	}
	record, found, err := composition.run.Backend.LoadDeleteReconciliation(
		request.RunID,
		request.Task,
		request.AttemptID,
	)
	if err != nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf("load terminal Stage 4 PostgreSQL delete evidence: %w", err),
		)
	}
	if !found {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"completed Stage 4 table %s lacks terminal delete reconciliation evidence",
				work.task.Table,
			),
		)
	}
	outcome, err := terminalDeleteReconcileOutcome(record)
	if err != nil {
		return false, NewTransferError(ErrorClassState, err)
	}
	if err := composition.authenticateTerminalAuthority(
		ctx,
		entry,
		reconciler,
		request,
		outcome,
	); err != nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"completed Stage 4 table %s has stale delete authority: %w",
				work.task.Table,
				err,
			),
		)
	}
	strict, err := stage4AdapterPostgresDeleteTerminalStrictness(outcome)
	if err != nil {
		return false, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"completed Stage 4 table %s has unusable delete reconciliation evidence: %w",
				work.task.Table,
				err,
			),
		)
	}
	return strict, nil
}

// authenticateStage4AdapterPostgresDeleteAttemptBeforeFinalize closes the
// fully-transferred/running-task crash window. FinalizeTables is a target
// mutation, so an existing delete attempt for this exact immutable topology
// must be rebound to freshly read PostgreSQL catalog authority before either
// BeforeTable or finalization can run. A missing attempt, or a Running attempt
// that crashed before saving a candidate plan, may proceed after that fresh
// catalog read because neither has durable candidate/delete authority yet.
func authenticateStage4AdapterPostgresDeleteAttemptBeforeFinalize(
	ctx context.Context,
	composition *stage4AdapterPostgresDeletePrepared,
	planIndex int,
	work stage4AdapterWork,
) error {
	if composition == nil || planIndex < 0 ||
		planIndex >= len(composition.entries) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete pre-finalize authority is unavailable"),
		)
	}
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete pre-finalize context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	entry := composition.entries[planIndex]
	reconciler, request, err := composition.requestFor(entry, work)
	if err != nil {
		return NewTransferError(ErrorClassState, err)
	}
	currentAuthority, err := composition.currentCanonicalizer(ctx, entry)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"derive current pre-finalize Stage 4 PostgreSQL delete authority for table %s: %w",
				work.task.Table,
				err,
			),
		)
	}
	reconciler.canonicalizer = currentAuthority
	admittedKeyPlan, err := validateDeleteReconcileRequest(
		request,
		entry.capabilities.canonicalizer,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"derive admitted Stage 4 PostgreSQL delete authority before finalization: %w",
				err,
			),
		)
	}
	currentKeyPlan, err := validateDeleteReconcileRequest(
		request,
		reconciler.canonicalizer,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"derive current Stage 4 PostgreSQL delete authority before finalization: %w",
				err,
			),
		)
	}
	authorityChanged :=
		admittedKeyPlan.proofDigest != currentKeyPlan.proofDigest ||
			len(admittedKeyPlan.targetColumns) !=
				len(currentKeyPlan.targetColumns)
	record, found, err := composition.run.Backend.LoadDeleteReconciliation(
		request.RunID,
		request.Task,
		request.AttemptID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"load Stage 4 PostgreSQL delete attempt before finalization: %w",
				err,
			),
		)
	}
	if !found {
		if authorityChanged {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete catalog or key-equality authority changed after transfer admission and before finalization",
				),
			)
		}
		return nil
	}
	if err := state.ValidateDeleteReconciliationEvidence(record); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"validate Stage 4 PostgreSQL delete attempt before finalization: %w",
				err,
			),
		)
	}
	if record.RunID != request.RunID || record.Task != request.Task ||
		record.AttemptID != request.AttemptID {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete attempt before finalization differs from the exact run, task, or topology attempt",
			),
		)
	}
	if record.Status != state.DeleteReconciliationRunning {
		outcome, terminalErr := terminalDeleteReconcileOutcome(record)
		var cleanupErr error
		if record.Plan != nil {
			if err := removeDeleteSpoolPath(
				request.SpoolDirectory,
				record.Plan.SpoolPath,
			); err != nil {
				cleanupErr = fmt.Errorf(
					"terminal delete reconciliation spool cleanup before finalization failed: %w",
					err,
				)
			}
		}
		if terminalErr != nil || cleanupErr != nil {
			return NewTransferError(
				ErrorClassState,
				errors.Join(terminalErr, cleanupErr),
			)
		}
		if authorityChanged {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete catalog or key-equality authority changed after transfer admission and before finalization",
				),
			)
		}
		if terminalErr := composition.authenticateTerminalAuthority(
			ctx,
			entry,
			reconciler,
			request,
			outcome,
		); terminalErr != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"authenticate terminal Stage 4 PostgreSQL delete attempt before finalization for table %s: %w",
					work.task.Table,
					terminalErr,
				),
			)
		}
		if _, terminalErr :=
			stage4AdapterPostgresDeleteTerminalStrictness(outcome); terminalErr != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete attempt before finalization is not reusable: %w",
					terminalErr,
				),
			)
		}
		return nil
	}
	if authorityChanged {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete catalog or key-equality authority changed after transfer admission and before finalization",
			),
		)
	}

	if record.Plan == nil {
		// A crash after BeginDeleteReconciliation but before SavePlan has not
		// selected candidates or authorized a target delete. Fresh catalog
		// admission above is sufficient; the core may safely build its first
		// immutable plan after idempotent finalization.
		return nil
	}
	if isNilInterface(reconciler.target) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"running Stage 4 PostgreSQL delete attempt lacks current target batching authority",
			),
		)
	}
	batchSize, err := deleteBatchLimit(
		request.Policy.Reconcile.BatchSize,
		reconciler.target.MaxDeleteParameters(),
		len(currentKeyPlan.targetColumns),
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"derive current running Stage 4 PostgreSQL delete batch authority before finalization: %w",
				err,
			),
		)
	}
	if record.Plan.EqualityProofDigest != currentKeyPlan.proofDigest ||
		record.Plan.KeyWidth != len(currentKeyPlan.targetColumns) ||
		record.Plan.BatchSize != batchSize ||
		record.Plan.BatchByteLimit != request.MaxBatchBytes {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"running Stage 4 PostgreSQL delete plan before finalization differs from current catalog, key-equality, or batching authority",
			),
		)
	}
	return nil
}

// rejectStage4AdapterPostgresDeleteAttemptBeforeReplay closes the only safe
// reset boundary: partial network work may be replayed only while no delete
// attempt is bound to its old final topology. Once an attempt exists, resetting
// that task would orphan its durable spool/receipt identity.
func (
	execution *stage4AdapterNetworkExecution,
) rejectStage4AdapterPostgresDeleteAttemptBeforeReplay(
	ctx context.Context,
	planIndex int,
) error {
	if execution == nil || execution.prepared.deletes == nil ||
		planIndex < 0 || planIndex >= len(execution.prepared.work) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete replay admission is unavailable"),
		)
	}
	inventory, err := loadStage4WorkInventory(ctx, execution.prepared.run)
	if err != nil {
		return err
	}
	base := execution.prepared.work[planIndex]
	if _, found := inventory.tasks[base.task]; !found {
		return nil
	}
	work, err := execution.reconstructCompletedTableWork(planIndex, inventory)
	if err != nil {
		return err
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(
		execution.prepared.run.RunID,
		work,
	)
	if err != nil {
		return NewTransferError(ErrorClassState, err)
	}
	record, found, err := execution.prepared.run.Backend.
		LoadDeleteReconciliation(
			execution.prepared.run.RunID,
			work.task,
			attemptID,
		)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("load Stage 4 PostgreSQL delete attempt before replay: %w", err),
		)
	}
	if !found {
		return nil
	}
	if err := state.ValidateDeleteReconciliationEvidence(record); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("partial Stage 4 PostgreSQL delete attempt is malformed: %w", err),
		)
	}
	return NewTransferError(
		ErrorClassState,
		fmt.Errorf(
			"partial Stage 4 network work for table %s already has delete attempt %s; resetting it would orphan durable delete evidence, so repair state and resume the same topology",
			work.task.Table,
			attemptID,
		),
	)
}

func (
	execution *stage4AdapterNetworkExecution,
) advanceStage4AdapterPostgresDeleteTransferredTable(
	ctx context.Context,
	planIndex int,
	expected stage4AdapterPostgresDeleteTransferredTable,
) error {
	actual, found, err :=
		execution.classifyStage4AdapterPostgresDeleteTransferredTable(
			ctx,
			planIndex,
		)
	if err != nil {
		return err
	}
	if !found || actual.rows != expected.rows ||
		actual.taskCompleted != expected.taskCompleted ||
		actual.work.task != expected.work.task ||
		actual.work.strategy != expected.work.strategy ||
		actual.work.topology != expected.work.topology ||
		len(actual.work.ranges) != len(expected.work.ranges) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 PostgreSQL delete transferred work for table %s changed after checkpoint admission",
				expected.work.task.Table,
			),
		)
	}
	rangeCount := uint64(len(actual.work.ranges))
	execution.mu.Lock()
	defer execution.mu.Unlock()
	if rangeCount > maximumRuntimeTuningRanges-execution.nextGlobalRange {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 global network range inventory is unbounded"),
		)
	}
	execution.nextGlobalRange += rangeCount
	return nil
}

func prevalidateStage4AdapterPostgresDeleteCompletedTargets(
	ctx context.Context,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
) error {
	if prepared.deletes == nil {
		return nil
	}
	for planIndex, plan := range prepared.plans {
		bound, found := execution.stage4AdapterPostgresDeleteTransferredTable(
			planIndex,
		)
		if !found || !bound.terminalAuthenticated {
			continue
		}
		targetRows, err := target.CountRows(ctx, plan.target)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"count completed PostgreSQL delete target table %s: %w",
					plan.source.Name,
					err,
				),
			)
		}
		if targetRows < bound.rows ||
			bound.terminalStrict && targetRows != bound.rows {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"completed Stage 4 table %s delete evidence is not reusable: checkpoint has %d rows and target has %d rows",
					plan.source.Name,
					bound.rows,
					targetRows,
				),
			)
		}
	}
	return nil
}

func runStage4AdapterPostgresDeleteNetworkTables(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	execution *stage4AdapterNetworkExecution,
	resume bool,
	completed map[string]int,
) (result Result, resultErr error) {
	if prepared.deletes == nil || execution == nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete network lifecycle is unavailable"),
		)
	}
	finalWork := make([]stage4AdapterWork, len(prepared.plans))
	rows := make([]int, len(prepared.plans))
	ordinaryCompleted := make([]bool, len(prepared.plans))

	for planIndex, plan := range prepared.plans {
		if checkpointRows, complete := completed[plan.source.Name]; complete {
			bound, found := execution.stage4AdapterPostgresDeleteTransferredTable(
				planIndex,
			)
			if !found || !bound.taskCompleted ||
				!bound.ordinaryCompleted || bound.rows != checkpointRows ||
				!bound.terminalAuthenticated {
				return result, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"completed Stage 4 PostgreSQL delete table %s lacks its authenticated transfer binding",
						plan.source.Name,
					),
				)
			}
			if err := execution.advanceCompletedTable(
				ctx,
				planIndex,
				checkpointRows,
			); err != nil {
				return result, err
			}
			finalWork[planIndex] = bound.work
			rows[planIndex] = checkpointRows
			ordinaryCompleted[planIndex] = true
			result.Tables++
			result.Rows += checkpointRows
			continue
		}

		if bound, found :=
			execution.stage4AdapterPostgresDeleteTransferredTable(planIndex); found {
			if !bound.taskCompleted {
				if err := authenticateStage4AdapterPostgresDeleteAttemptBeforeFinalize(
					ctx,
					prepared.deletes,
					planIndex,
					bound.work,
				); err != nil {
					return result, err
				}
			}
			if err := execution.advanceStage4AdapterPostgresDeleteTransferredTable(
				ctx,
				planIndex,
				bound,
			); err != nil {
				return result, err
			}
			if !bound.taskCompleted {
				if err := checkpointStage4AdapterPostgresDeleteBeforeTable(
					ctx,
					observer,
					plan.source.Name,
					resume,
				); err != nil {
					return result, err
				}
				mutationObserver := stage4AdapterPostgresDeleteMutationObserver(
					ctx,
					observer,
					plan.source.Name,
					resume,
				)
				if _, err := protectAdapterTargetMutationOnce(
					ctx,
					mutationObserver,
					"refinalize resumed Stage 4 PostgreSQL delete table "+plan.source.Name,
					func() error {
						return target.FinalizeTables(
							ctx,
							[]schema.Table{cloneStage4RichTable(plan.target)},
							prepared.mode,
						)
					},
				); err != nil {
					return result, err
				}
			}
			finalWork[planIndex] = bound.work
			rows[planIndex] = bound.rows
			result.Tables++
			result.Rows += bound.rows
			continue
		}

		tableExecution, err := execution.openTable(ctx, planIndex, resume)
		if err != nil {
			return result, err
		}
		copied, final, err := runStage4AdapterPostgresDeleteNetworkTable(
			ctx,
			observer,
			target,
			prepared,
			planIndex,
			tableExecution,
			resume,
		)
		if err != nil {
			return result, err
		}
		finalWork[planIndex] = final
		rows[planIndex] = copied
		result.Tables++
		result.Rows += copied
	}

	deleteResult, err := prepared.deletes.reconcile(ctx, finalWork)
	if err != nil {
		if ClassifyTransferError(err) == ErrorClassPermanent {
			err = NewTransferError(ErrorClassState, err)
		}
		return result, err
	}
	prepared.deleteReconciliationStrict = deleteResult.strictByTable
	if err := validateStage4AdapterPostgresDeleteCheckpointRows(
		ctx,
		target,
		prepared,
		rows,
	); err != nil {
		return result, err
	}
	if err := validateStage4AdapterRun(
		ctx,
		cfg,
		execution.source,
		target,
		prepared,
	); err != nil {
		return result, err
	}

	for planIndex, plan := range prepared.plans {
		if ordinaryCompleted[planIndex] {
			continue
		}
		if err := completeStage4AdapterWorkItem(
			ctx,
			prepared.run,
			finalWork[planIndex],
		); err != nil {
			return result, fmt.Errorf(
				"complete Stage 4 PostgreSQL delete work for %s: %w",
				plan.source.Name,
				err,
			)
		}
		if err := observer.AfterTable(ctx, plan.source.Name, rows[planIndex]); err != nil {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf("checkpoint after %s: %w", plan.source.Name, err),
			)
		}
	}
	return result, nil
}

func runStage4AdapterPostgresDeleteNetworkTable(
	ctx context.Context,
	observer TableObserver,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	planIndex int,
	execution *stage4AdapterNetworkTableExecution,
	resume bool,
) (_ int, _ stage4AdapterWork, resultErr error) {
	if execution == nil {
		return 0, stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete table execution is unavailable"),
		)
	}
	defer func() {
		if closeErr := execution.Close(); closeErr != nil {
			if resultErr == nil {
				resultErr = closeErr
			} else {
				resultErr = errors.Join(resultErr, closeErr)
			}
		}
	}()
	plan := prepared.plans[planIndex]
	if err := checkpointStage4AdapterPostgresDeleteBeforeTable(
		ctx,
		observer,
		plan.source.Name,
		resume,
	); err != nil {
		return 0, stage4AdapterWork{}, err
	}
	mutationObserver := stage4AdapterPostgresDeleteMutationObserver(
		ctx,
		observer,
		plan.source.Name,
		resume,
	)
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		mutationObserver,
		"prepare Stage 4 PostgreSQL delete table "+plan.source.Name,
		func() error {
			return target.PrepareTables(
				ctx,
				[]schema.Table{cloneStage4RichTable(plan.target)},
				prepared.mode,
			)
		},
	); err != nil {
		return 0, stage4AdapterWork{}, err
	}
	copied, err := execution.run(ctx, mutationObserver)
	if err != nil {
		return 0, stage4AdapterWork{}, err
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		mutationObserver,
		"finalize Stage 4 PostgreSQL delete table "+plan.source.Name,
		func() error {
			return target.FinalizeTables(
				ctx,
				[]schema.Table{cloneStage4RichTable(plan.target)},
				prepared.mode,
			)
		},
	); err != nil {
		return 0, stage4AdapterWork{}, err
	}
	return copied, cloneStage4AdapterNetworkWork(execution.work), nil
}

func checkpointStage4AdapterPostgresDeleteBeforeTable(
	ctx context.Context,
	observer TableObserver,
	table string,
	resume bool,
) error {
	if resume {
		if err := checkAdapterResumeContext(ctx, "checkpoint before "+table); err != nil {
			return err
		}
	}
	if err := observer.BeforeTable(ctx, table); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("checkpoint before %s: %w", table, err),
		)
	}
	return nil
}

func stage4AdapterPostgresDeleteMutationObserver(
	ctx context.Context,
	observer TableObserver,
	table string,
	resume bool,
) TableObserver {
	if !resume {
		return observer
	}
	return adapterResumeMutationGuard{
		ctx:      ctx,
		delegate: observer,
		boundary: "mutate resumed Stage 4 PostgreSQL delete table " + table,
	}
}

func validateStage4AdapterPostgresDeleteCheckpointRows(
	ctx context.Context,
	target targetAdapter,
	prepared stage4AdapterPrepared,
	rows []int,
) error {
	if len(rows) != len(prepared.plans) ||
		len(prepared.deleteReconciliationStrict) != len(prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete validation evidence is incomplete"),
		)
	}
	for index, plan := range prepared.plans {
		key := stage4RichTableKey{
			schema: plan.source.Schema,
			table:  plan.source.Name,
		}
		strict, found := prepared.deleteReconciliationStrict[key]
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete validation lacks table %s",
					plan.source.Name,
				),
			)
		}
		targetRows, err := target.CountRows(ctx, plan.target)
		if err != nil {
			return err
		}
		if targetRows < rows[index] || strict && targetRows != rows[index] {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 PostgreSQL delete table %s cannot publish a reusable checkpoint: transferred %d rows, target now has %d rows, reconciliation_strict=%t; rerun after source mutation quiesces",
					plan.source.Name,
					rows[index],
					targetRows,
					strict,
				),
			)
		}
	}
	return nil
}
