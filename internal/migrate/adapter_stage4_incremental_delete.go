package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// stage4AdapterSQLiteIncrementalDeletePrepared is deliberately narrower than
// the legacy-named delete composition. A fresh incremental delete pass may
// only scan keys from the exact retained SQLite source transaction that armed
// its upper fence. A new process cannot recreate that view, so resume may
// replay an already durable candidate plan or terminal receipt, but never
// create a new plan from a newer source snapshot.
type stage4AdapterSQLiteIncrementalDeletePrepared struct {
	source                         *sqliteSourceAdapter
	target                         *sqliteTargetAdapter
	tables                         []stage4AdapterSQLiteIncrementalDeleteTable
	resumeFreshSourceScanByPlanIdx map[int]bool
}

type stage4AdapterSQLiteIncrementalDeleteTable struct {
	planIndex            int
	incrementalAttemptID string
	deleteAttemptID      string
	dateColumn           string
	work                 stage4AdapterWork
}

func prepareStage4AdapterSQLiteIncrementalDeleteComposition(
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	prepared *stage4AdapterPrepared,
) error {
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete composition context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if prepared == nil || prepared.incremental == nil ||
		prepared.deletes == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete composition requires admitted incremental and delete plans"),
		)
	}
	if cfg.Migration.Deletes.Mode != config.DeleteModeReconcile ||
		len(cfg.Migration.DateUpdatedColumns) == 0 ||
		source.Engine() != "sqlite" || target.Engine() != "sqlite" {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 incremental delete reconciliation requires the SQLite-to-SQLite date-window route"),
		)
	}
	sourceSQLite, sourceOK := source.(*sqliteSourceAdapter)
	targetSQLite, targetOK := target.(*sqliteTargetAdapter)
	if !sourceOK || sourceSQLite == nil || sourceSQLite.snapshot == nil ||
		sourceSQLite.incrementalDeleteMonitor == nil || !targetOK ||
		targetSQLite == nil || targetSQLite.database == nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 SQLite incremental delete reconciliation requires one retained source snapshot, change monitor, and live SQLite target"),
		)
	}
	if err := requireDistinctLiveSQLiteDatabases(
		ctx,
		sourceSQLite,
		targetSQLite,
	); err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 SQLite incremental delete reconciliation: %w", err),
		)
	}
	incrementalSource, sourceMatches := prepared.incremental.source.(*sqliteSourceAdapter)
	incrementalTarget, targetMatches := prepared.incremental.target.(*sqliteTargetAdapter)
	if !sourceMatches || incrementalSource != sourceSQLite ||
		!targetMatches || incrementalTarget != targetSQLite {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete composition differs from the admitted source or target authority"),
		)
	}
	if len(prepared.incremental.tables) != len(prepared.plans) ||
		len(prepared.work) != len(prepared.plans) ||
		len(prepared.deletes.entries) != len(prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete composition differs from the immutable table plan"),
		)
	}

	composition := &stage4AdapterSQLiteIncrementalDeletePrepared{
		source: sourceSQLite,
		target: targetSQLite,
		tables: make(
			[]stage4AdapterSQLiteIncrementalDeleteTable,
			len(prepared.incremental.tables),
		),
	}
	for index, table := range prepared.incremental.tables {
		if table.planIndex != index || table.plan.FullTableUpsert ||
			table.plan.DateColumn == nil || table.plan.DateColumn.Name == "" {
			return NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf("Stage 4 SQLite incremental delete reconciliation requires an exact date-window plan for table %s", prepared.plans[index].source.Name),
			)
		}
		if !sameStage4AdapterIncrementalDeleteWork(
			table.work,
			prepared.work[index],
		) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 SQLite incremental delete work for table %s differs from the immutable incremental topology", prepared.plans[index].source.Name),
			)
		}
		deleteAttemptID, err := stage4AdapterPostgresDeleteAttemptID(
			prepared.run.RunID,
			table.work,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("bind Stage 4 SQLite incremental delete attempt for table %s: %w", prepared.plans[index].source.Name, err),
			)
		}
		composition.tables[index] = stage4AdapterSQLiteIncrementalDeleteTable{
			planIndex:            index,
			incrementalAttemptID: table.attemptID,
			deleteAttemptID:      deleteAttemptID,
			dateColumn:           table.plan.DateColumn.Name,
			work:                 cloneStage4AdapterNetworkWork(table.work),
		}
	}
	prepared.incremental.deletes = composition
	return nil
}

func sameStage4AdapterIncrementalDeleteWork(
	left stage4AdapterWork,
	right stage4AdapterWork,
) bool {
	if left.task != right.task || left.strategy != right.strategy ||
		left.topology != right.topology || len(left.ranges) != len(right.ranges) {
		return false
	}
	for index := range left.ranges {
		if left.ranges[index].ID != right.ranges[index].ID ||
			left.ranges[index].Strategy != right.ranges[index].Strategy ||
			left.ranges[index].TopologyHash != right.ranges[index].TopologyHash {
			return false
		}
	}
	return true
}

// reconcileStage4AdapterSQLiteIncrementalDeletes runs only after every
// selected incremental table has published its completed upper-fence attempt.
// It is intentionally placed immediately before ValidationCore: delete
// receipts affect full-table target strictness, while the incremental
// validation evidence remains scoped to the exact transferred rows.
func reconcileStage4AdapterSQLiteIncrementalDeletes(
	ctx context.Context,
	prepared *stage4AdapterPrepared,
	resume bool,
) error {
	if prepared == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete preparation is unavailable"),
		)
	}
	if prepared.incremental == nil || prepared.incremental.deletes == nil {
		return nil
	}
	composition := prepared.incremental.deletes
	if composition.source == nil || composition.target == nil ||
		prepared.deletes == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete composition is incomplete"),
		)
	}
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete execution context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(composition.tables) != len(prepared.incremental.tables) ||
		len(prepared.work) != len(composition.tables) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete composition no longer covers the immutable table plan"),
		)
	}

	freshSourceScan := false
	for index, binding := range composition.tables {
		table := prepared.incremental.tables[index]
		if binding.planIndex != index ||
			binding.incrementalAttemptID != table.attemptID ||
			binding.dateColumn == "" || table.plan.DateColumn == nil ||
			binding.dateColumn != table.plan.DateColumn.Name ||
			!sameStage4AdapterIncrementalDeleteWork(binding.work, table.work) ||
			!sameStage4AdapterIncrementalDeleteWork(binding.work, prepared.work[index]) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 SQLite incremental delete table %s differs from its admitted upper-fence binding", prepared.plans[index].source.Name),
			)
		}
		attempt, found, err := prepared.run.Backend.LoadIncrementalAttempt(
			prepared.run.RunID,
			binding.work.task,
			binding.incrementalAttemptID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("load completed Stage 4 SQLite incremental attempt for table %s: %w", prepared.plans[index].source.Name, err),
			)
		}
		if !found {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 SQLite incremental delete reconciliation requires a completed stored upper-fence attempt for table %s", prepared.plans[index].source.Name),
			)
		}
		request := stage4AdapterIncrementalRequest(*prepared, table, true, nil, nil)
		if err := validateStoredIncrementalAttempt(request, attempt); err != nil {
			return err
		}
		if attempt.Status != state.IncrementalCompleted || !attempt.TableSucceeded {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 SQLite incremental delete reconciliation requires completed upper-fence transfer evidence for table %s", prepared.plans[index].source.Name),
			)
		}

		record, recordFound, err := prepared.run.Backend.LoadDeleteReconciliation(
			prepared.run.RunID,
			binding.work.task,
			binding.deleteAttemptID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("load Stage 4 SQLite incremental delete evidence for table %s: %w", prepared.plans[index].source.Name, err),
			)
		}
		if !recordFound {
			if resume && !composition.resumeFreshSourceScanByPlanIdx[index] {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("cannot resume Stage 4 SQLite incremental delete reconciliation for table %s without its original retained source view or a durable delete plan", prepared.plans[index].source.Name),
				)
			}
			freshSourceScan = true
			continue
		}
		if err := state.ValidateDeleteReconciliationEvidence(record); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("stored Stage 4 SQLite incremental delete evidence for table %s is malformed: %w", prepared.plans[index].source.Name, err),
			)
		}
		switch record.Status {
		case state.DeleteReconciliationCompleted, state.DeleteReconciliationNotDue:
			// Terminal receipts never rescan the source.
		case state.DeleteReconciliationRunning:
			if record.Plan == nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("cannot resume Stage 4 SQLite incremental delete reconciliation for table %s without its durable source-key plan", prepared.plans[index].source.Name),
				)
			}
		default:
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 SQLite incremental delete reconciliation for table %s has non-replayable status %q", prepared.plans[index].source.Name, record.Status),
			)
		}
	}
	if freshSourceScan {
		if err := composition.source.VerifyIncrementalDeleteAuthority(ctx); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("verify retained SQLite source authority before incremental delete reconciliation: %w", err),
			)
		}
	}
	result, err := prepared.deletes.reconcile(ctx, prepared.work)
	if err != nil {
		return err
	}
	prepared.deleteReconciliationStrict = result.strictByTable
	return nil
}

// preflightStage4AdapterSQLiteIncrementalDeleteResume runs before completed
// table revalidation opens any source rows.  An interrupted, still-running
// incremental transfer has not yet published a completed transfer or begun a
// delete pass, so ordinary incremental replay may establish a new retained
// view and restart its bounded work.  Once a table transfer is completed,
// however, a fresh delete key scan from a new process could turn later source
// deletions into target deletes.  At that boundary resume is allowed only when
// the delete pass already has immutable source-key authority: a terminal
// receipt, or a running durable candidate plan to replay.  This distinction
// preserves ordinary mid-transfer resume without widening post-transfer
// source authority.
func preflightStage4AdapterSQLiteIncrementalDeleteResume(
	ctx context.Context,
	prepared stage4AdapterPrepared,
	resume bool,
) error {
	if !resume || prepared.incremental == nil ||
		prepared.incremental.deletes == nil {
		return nil
	}
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete resume context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(prepared.incremental.deletes.tables) != len(prepared.incremental.tables) ||
		len(prepared.plans) != len(prepared.incremental.tables) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 SQLite incremental delete resume composition differs from the immutable table plan"),
		)
	}
	composition := prepared.incremental.deletes
	composition.resumeFreshSourceScanByPlanIdx = make(map[int]bool)
	for index, table := range prepared.incremental.tables {
		attempt, found, err := prepared.run.Backend.LoadIncrementalAttempt(
			prepared.run.RunID,
			table.work.task,
			table.attemptID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("load Stage 4 SQLite incremental delete resume attempt for table %s: %w", prepared.plans[index].source.Name, err),
			)
		}
		if !found {
			// There is no stored transfer for this table, so a resumed run
			// will create a fresh retained view before it can reach deletes.
			composition.resumeFreshSourceScanByPlanIdx[index] = true
			continue
		}
		request := stage4AdapterIncrementalRequest(prepared, table, true, nil, nil)
		if err := validateStoredIncrementalAttempt(request, attempt); err != nil {
			return err
		}
		if attempt.Status != state.IncrementalCompleted ||
			!attempt.TableSucceeded {
			// Running attempts have no completed source/target transfer to
			// reconcile. ExecuteIncrementalTable will reopen and replay the
			// ordinary incremental read before a fresh delete plan is allowed.
			composition.resumeFreshSourceScanByPlanIdx[index] = true
			continue
		}
		binding := composition.tables[index]
		record, recordFound, err := prepared.run.Backend.LoadDeleteReconciliation(
			prepared.run.RunID,
			binding.work.task,
			binding.deleteAttemptID,
		)
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("load completed Stage 4 SQLite incremental delete evidence for table %s: %w", prepared.plans[index].source.Name, err),
			)
		}
		if !recordFound {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("cannot resume Stage 4 SQLite incremental delete reconciliation for completed table %s without its original retained source view or a durable source-key plan", prepared.plans[index].source.Name),
			)
		}
		if err := state.ValidateDeleteReconciliationEvidence(record); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("stored Stage 4 SQLite incremental delete evidence for completed table %s is malformed: %w", prepared.plans[index].source.Name, err),
			)
		}
		switch record.Status {
		case state.DeleteReconciliationCompleted,
			state.DeleteReconciliationNotDue:
			// Terminal evidence is immutable; reconciliation will reread and
			// authenticate it without a source-key scan.
		case state.DeleteReconciliationRunning:
			if record.Plan == nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf("cannot resume Stage 4 SQLite incremental delete reconciliation for completed table %s without its durable source-key plan", prepared.plans[index].source.Name),
				)
			}
		default:
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 SQLite incremental delete reconciliation for completed table %s has non-replayable status %q", prepared.plans[index].source.Name, record.Status),
			)
		}
	}
	return nil
}
