package migrate

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const (
	dryRunDeleteCandidateDueStateUnavailable  = "durable delete due-state is unavailable; exact candidate impact was not scanned"
	dryRunDeleteCandidateTargetUnavailable    = "target read-only preflight is unavailable; exact candidate impact was not scanned"
	dryRunDeleteCandidateRouteUnavailable     = "certified source/target delete-key capability is unavailable; exact candidate impact was not scanned"
	dryRunDeleteCandidateAuthorityUnavailable = "complete primary-key candidate authority could not be established read-only"
	dryRunDeleteCandidateSnapshotUnavailable  = "SQLite endpoint artifacts could not be captured and verified read-only; exact candidate impact was not scanned"
)

type dryRunDeleteTableIdentity struct {
	schema string
	table  string
}

type dryRunDeleteCandidateImpact struct {
	candidates  int64
	digest      string
	proofDigest string
	batches     int64
}

// ApplyDryRunDeleteCandidateImpact attaches exact target-only primary-key
// candidate evidence to every due delete-reconciliation table when the route's
// existing production key-reader and atomic-receipt capability can prove it.
//
// It intentionally does not instantiate a state backend, target mutation
// protector, receipt journal, schema evolution, or watermark path. The only
// writable artifact is a private, short-lived key spool, which is removed
// before the dry-run plan is returned. Routes that cannot establish the same
// complete primary-key and equality authority used by production reconciliation
// are disclosed as unavailable and block proceeding rather than guessed.
func ApplyDryRunDeleteCandidateImpact(
	ctx context.Context,
	cfg config.Config,
	plan *Plan,
) {
	if plan == nil || plan.Deletes == nil ||
		cfg.Migration.Deletes.Mode != config.DeleteModeReconcile {
		return
	}
	for index := range plan.Deletes.Tables {
		clearDryRunDeleteCandidateImpact(&plan.Deletes.Tables[index])
	}
	if len(plan.Deletes.Tables) == 0 {
		return
	}
	if ctx == nil || !plan.Deletes.DueStateKnown {
		markDryRunDeleteCandidatesUnavailable(
			plan,
			dryRunDeleteCandidateDueStateUnavailable,
		)
		return
	}

	due := make([]int, 0, len(plan.Deletes.Tables))
	for index := range plan.Deletes.Tables {
		table := &plan.Deletes.Tables[index]
		if !table.DueStateKnown {
			markDryRunDeleteCandidateUnavailable(
				table,
				dryRunDeleteCandidateDueStateUnavailable,
			)
			plan.Proceed = false
			continue
		}
		if !table.Due {
			table.CandidateImpactStatus = PlannedDeleteCandidateImpactNotDue
			continue
		}
		due = append(due, index)
	}
	if len(due) == 0 {
		return
	}
	if plan.Target == nil ||
		plan.Target.Preflight != PlannedTargetPreflightPassed {
		markDryRunDeleteCandidatesUnavailable(
			plan,
			dryRunDeleteCandidateTargetUnavailable,
		)
		return
	}

	route, err := resolveMigration(cfg, builtInAdapters)
	if err != nil || route.source.open == nil || route.target.open == nil {
		markDryRunDeleteCandidatesUnavailable(
			plan,
			dryRunDeleteCandidateRouteUnavailable,
		)
		return
	}
	snapshotCfg, cleanup, err := dryRunSQLiteEndpointSnapshots(cfg, true, true)
	if err != nil {
		markDryRunDeleteCandidatesUnavailable(
			plan,
			dryRunDeleteCandidateSnapshotUnavailable,
		)
		return
	}
	source, err := route.source.open(ctx, snapshotCfg.Source)
	if err != nil {
		snapshotErr := cleanup()
		markDryRunDeleteCandidatesUnavailable(
			plan,
			candidateSnapshotLimitation(nil, snapshotErr),
		)
		return
	}
	target, err := route.target.open(ctx, snapshotCfg.Target)
	if err != nil {
		closeErr := source.Close()
		snapshotErr := cleanup()
		markDryRunDeleteCandidatesUnavailable(
			plan,
			candidateSnapshotLimitation(closeErr, snapshotErr),
		)
		return
	}

	inspectErr := func() error {
		if source.Engine() != route.source.engine ||
			target.Engine() != route.target.engine {
			return fmt.Errorf("dry-run delete route factories returned unexpected engines")
		}
		return applyDryRunDeleteCandidateTables(ctx, cfg, plan, source, target)
	}()
	closeErr := errors.Join(target.Close(), source.Close())
	snapshotErr := cleanup()
	if snapshotErr != nil {
		markDryRunDeleteCandidatesUnavailable(
			plan,
			dryRunDeleteCandidateSnapshotUnavailable,
		)
		return
	}
	if inspectErr != nil || closeErr != nil {
		markDryRunDeleteCandidatesUnavailable(
			plan,
			dryRunDeleteCandidateAuthorityUnavailable,
		)
	}
}

func candidateSnapshotLimitation(closeErr, snapshotErr error) string {
	if snapshotErr != nil {
		return dryRunDeleteCandidateSnapshotUnavailable
	}
	if closeErr != nil {
		return dryRunDeleteCandidateAuthorityUnavailable
	}
	return dryRunDeleteCandidateRouteUnavailable
}

func applyDryRunDeleteCandidateTables(
	ctx context.Context,
	cfg config.Config,
	plan *Plan,
	source sourceAdapter,
	target targetAdapter,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sourceTables := make([]schema.Table, len(plan.Tables))
	indices := make(map[dryRunDeleteTableIdentity]int, len(plan.Tables))
	for index, planned := range plan.Tables {
		table, err := source.InspectTable(ctx, planned.Name)
		if err != nil {
			return fmt.Errorf("inspect source table for delete candidates: %w", err)
		}
		if table.Name != planned.Name {
			return fmt.Errorf("source table identity changed during delete candidate inspection")
		}
		identity := dryRunDeleteTableIdentity{schema: table.Schema, table: table.Name}
		if _, duplicate := indices[identity]; duplicate {
			return fmt.Errorf("selected delete candidate tables are not uniquely identified")
		}
		indices[identity] = index
		sourceTables[index] = table
	}
	targetTables, err := target.PlanTables(
		source.Engine(),
		sourceTables,
		cfg.Migration.TargetMode,
	)
	if err != nil {
		return fmt.Errorf("plan target tables for delete candidates: %w", err)
	}
	if len(targetTables) != len(sourceTables) {
		return fmt.Errorf("target delete candidate plan does not preserve selected table count")
	}

	for index := range plan.Deletes.Tables {
		planned := &plan.Deletes.Tables[index]
		if !planned.DueStateKnown || !planned.Due {
			continue
		}
		identity := dryRunDeleteTableIdentity{schema: planned.Schema, table: planned.Table}
		tableIndex, found := indices[identity]
		if !found {
			markDryRunDeleteCandidateUnavailable(
				planned,
				dryRunDeleteCandidateAuthorityUnavailable,
			)
			plan.Proceed = false
			continue
		}
		impact, impactErr := inspectDryRunDeleteCandidateImpact(
			ctx,
			cfg,
			source,
			target,
			sourceTables[tableIndex],
			targetTables[tableIndex],
			*planned,
		)
		if impactErr != nil {
			markDryRunDeleteCandidateUnavailable(
				planned,
				dryRunDeleteCandidateAuthorityUnavailable,
			)
			plan.Proceed = false
			continue
		}
		markDryRunDeleteCandidateExact(planned, impact)
	}
	return nil
}

func inspectDryRunDeleteCandidateImpact(
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	sourceTable schema.Table,
	targetTable schema.Table,
	planned PlannedDeleteTable,
) (impact dryRunDeleteCandidateImpact, resultErr error) {
	if planned.Schema != sourceTable.Schema ||
		planned.Table != sourceTable.Name {
		return impact, fmt.Errorf("durable delete schedule task differs from the inspected source table")
	}
	capabilities, err := newStage4DeleteReconciliationCapabilities(
		ctx,
		source,
		target,
		sourceTable,
		targetTable,
	)
	if err != nil {
		return impact, err
	}
	maxBatchBytes, err := stage4AdapterPostgresDeleteBatchByteLimit(
		cfg.Migration.MemoryCeilingBytes,
	)
	if err != nil {
		return impact, err
	}
	directory, err := os.MkdirTemp("", "dmtx-dryrun-delete-")
	if err != nil {
		return impact, fmt.Errorf("create private dry-run delete spool directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(directory); err != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("remove private dry-run delete spool directory: %w", err),
			)
		}
	}()

	request := deleteReconcileRequest{
		RunID:     "dry-run-delete-candidates",
		AttemptID: "dry-run-delete-candidates",
		Task:      state.TaskKey{Type: stage4AdapterNetworkTaskType, Schema: sourceTable.Schema, Table: sourceTable.Name},
		// The capability binds its equality proof to the inspected rich table
		// values. Validation only reads these values, so retain them exactly
		// rather than rebuilding a parallel schema representation.
		SourceTable:    sourceTable,
		TargetTable:    targetTable,
		TargetMode:     cfg.Migration.TargetMode,
		Policy:         cfg.Migration.Deletes,
		DryRun:         true,
		SpoolDirectory: directory,
		MaxBatchBytes:  maxBatchBytes,
	}
	keyPlan, err := validateDeleteReconcileRequest(request, capabilities.canonicalizer)
	if err != nil {
		return impact, err
	}
	planID, err := newDeletePlanID()
	if err != nil {
		return impact, err
	}
	reconciler := deleteReconciler{
		source:        capabilities.source,
		target:        capabilities.target,
		canonicalizer: capabilities.canonicalizer,
	}
	spool, candidates, digest, err := reconciler.buildSpool(
		ctx,
		request,
		keyPlan,
		planID,
	)
	if spool != nil {
		defer func() {
			if cleanupErr := cleanupDeleteKeySpool(directory, spool); cleanupErr != nil {
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf("clean private dry-run delete spool: %w", cleanupErr),
				)
			}
		}()
	}
	if err != nil {
		return impact, err
	}
	batchSize, err := deleteBatchLimit(
		cfg.Migration.Deletes.Reconcile.BatchSize,
		capabilities.target.MaxDeleteParameters(),
		len(keyPlan.targetColumns),
	)
	if err != nil {
		return impact, err
	}
	if candidates > 0 {
		snapshot, err := spool.beginReadSnapshot(ctx)
		if err != nil {
			return impact, err
		}
		defer func() {
			if closeErr := snapshot.Close(); closeErr != nil {
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf("close dry-run delete candidate spool snapshot: %w", closeErr),
				)
			}
		}()
		var offset int64
		for offset < candidates {
			batch, batchDigest, _, batchErr := snapshot.candidateBatch(
				ctx,
				offset,
				batchSize,
				maxBatchBytes,
			)
			if batchErr != nil || len(batch) == 0 || batchDigest == "" {
				if batchErr != nil {
					return impact, batchErr
				}
				return impact, fmt.Errorf("delete candidate batch lacks exact bounded evidence")
			}
			offset += int64(len(batch))
			if offset > candidates {
				return impact, fmt.Errorf("delete candidate batches exceed the exact candidate count")
			}
			impact.batches++
		}
		if offset != candidates {
			return impact, fmt.Errorf("delete candidate batches do not cover the exact candidate count")
		}
	}
	impact.candidates = candidates
	impact.digest = digest
	impact.proofDigest = keyPlan.proofDigest
	return impact, nil
}

func clearDryRunDeleteCandidateImpact(table *PlannedDeleteTable) {
	if table == nil {
		return
	}
	table.CandidateImpactStatus = ""
	table.CandidateCount = nil
	table.CandidateDigest = ""
	table.CandidateEqualityProofDigest = ""
	table.CandidateBatchCount = nil
	table.CandidateProvenance = ""
	table.CandidateLimitations = nil
}

func markDryRunDeleteCandidateExact(
	table *PlannedDeleteTable,
	impact dryRunDeleteCandidateImpact,
) {
	clearDryRunDeleteCandidateImpact(table)
	candidates, batches := impact.candidates, impact.batches
	table.CandidateImpactStatus = PlannedDeleteCandidateImpactExact
	table.CandidateCount = &candidates
	table.CandidateDigest = impact.digest
	table.CandidateEqualityProofDigest = impact.proofDigest
	table.CandidateBatchCount = &batches
	table.CandidateProvenance =
		PlannedDeleteCandidateImpactPrimaryKeySetDifference
}

func markDryRunDeleteCandidateUnavailable(
	table *PlannedDeleteTable,
	limitation string,
) {
	clearDryRunDeleteCandidateImpact(table)
	table.CandidateImpactStatus = PlannedDeleteCandidateImpactUnavailable
	table.CandidateLimitations = []string{limitation}
}

func markDryRunDeleteCandidatesUnavailable(plan *Plan, limitation string) {
	if plan == nil || plan.Deletes == nil {
		return
	}
	for index := range plan.Deletes.Tables {
		table := &plan.Deletes.Tables[index]
		if table.DueStateKnown && !table.Due {
			clearDryRunDeleteCandidateImpact(table)
			table.CandidateImpactStatus = PlannedDeleteCandidateImpactNotDue
			continue
		}
		markDryRunDeleteCandidateUnavailable(table, limitation)
	}
	plan.Proceed = false
}
