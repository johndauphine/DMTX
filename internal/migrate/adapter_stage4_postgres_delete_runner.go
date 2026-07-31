package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

const stage4AdapterPostgresDeleteAttemptVersion = 1

// stage4AdapterPostgresDeletePrepared is the PostgreSQL-only bridge from
// completed network table work to the engine-neutral delete reconciliation
// core. Construction is read-only: it authenticates every source/target key
// relation before the surrounding lifecycle is permitted to mutate either
// schema or data.
type stage4AdapterPostgresDeletePrepared struct {
	run           Stage4RunContext
	policy        config.DeletePolicy
	maxBatchBytes int64
	protector     adapterTargetMutationProtector
	entries       []stage4AdapterPostgresDeleteEntry
	now           func() time.Time
}

type stage4AdapterPostgresDeleteEntry struct {
	planIndex        int
	source           schema.Table
	target           schema.Table
	capabilities     postgresDeleteReconciliationCapabilities
	currentAuthority func(context.Context) (deleteKeyCanonicalizer, error)
}

// stage4AdapterPostgresDeleteTableOutcome is ordered by actual execution,
// which is deliberately the reverse of the admitted parent-before-child load
// plan. That makes hard deletes child-before-parent while constraints remain
// active.
type stage4AdapterPostgresDeleteTableOutcome struct {
	planIndex int
	work      stage4AdapterWork
	outcome   deleteReconcileOutcome
}

type stage4AdapterPostgresDeleteResult struct {
	tables        []stage4AdapterPostgresDeleteTableOutcome
	strictByTable map[stage4RichTableKey]bool
}

// prepareStage4AdapterPostgresDeleteComposition performs no target mutation
// and writes no durable state. In particular, the PostgreSQL capability
// constructor performs catalog/privilege/journal admission reads only.
func prepareStage4AdapterPostgresDeleteComposition(
	ctx context.Context,
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) (*stage4AdapterPostgresDeletePrepared, error) {
	if ctx == nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete composition context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if isNilInterface(source) || isNilInterface(target) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 PostgreSQL delete composition requires live source and target adapters"),
		)
	}
	if err := requireStage4AdapterPostgresDeleteComposition(
		cfg,
		source.Engine(),
		target.Engine(),
		prepared,
	); err != nil {
		return nil, err
	}
	if err := prepared.run.Validate(); err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("validate Stage 4 PostgreSQL delete run context: %w", err),
		)
	}
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete reconciliation requires a lease-fenced target mutation protector"),
		)
	}
	maxBatchBytes, err := stage4AdapterPostgresDeleteBatchByteLimit(
		cfg.Migration.MemoryCeilingBytes,
	)
	if err != nil {
		return nil, NewTransferError(ErrorClassPolicy, err)
	}
	mutationProtector := protector
	if prepared.run.Resume {
		mutationProtector = adapterResumeMutationGuard{
			ctx:      ctx,
			delegate: observer,
			boundary: "mutate resumed Stage 4 PostgreSQL delete reconciliation",
		}
	}

	entries := make(
		[]stage4AdapterPostgresDeleteEntry,
		len(prepared.plans),
	)
	for index, plan := range prepared.plans {
		capabilities, capabilityErr :=
			newPostgresDeleteReconciliationCapabilities(
				ctx,
				source,
				target,
				plan.source,
				plan.target,
			)
		if capabilityErr != nil {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"preflight Stage 4 PostgreSQL delete reconciliation for table %s: %w",
					plan.source.Name,
					capabilityErr,
				),
			)
		}
		entry := stage4AdapterPostgresDeleteEntry{
			planIndex:    index,
			source:       cloneStage4RichTable(plan.source),
			target:       cloneStage4RichTable(plan.target),
			capabilities: capabilities,
		}
		entry.currentAuthority = func(
			ctx context.Context,
		) (deleteKeyCanonicalizer, error) {
			current, err := newPostgresDeleteReconciliationCapabilities(
				ctx,
				source,
				target,
				entry.source,
				entry.target,
			)
			if err != nil {
				return nil, err
			}
			if isNilInterface(current.canonicalizer) {
				return nil, fmt.Errorf(
					"current PostgreSQL delete key authority is unavailable",
				)
			}
			return current.canonicalizer, nil
		}
		entries[index] = entry
	}
	return &stage4AdapterPostgresDeletePrepared{
		run:           prepared.run,
		policy:        cfg.Migration.Deletes,
		maxBatchBytes: maxBatchBytes,
		protector:     mutationProtector,
		entries:       entries,
		now:           func() time.Time { return time.Now().UTC() },
	}, nil
}

// requireStage4AdapterPostgresDeleteComposition is the pure, fail-closed
// route boundary. Wider combinations need their own snapshot and topology
// proofs; admitting them here would make a passing delete pass look stronger
// than the source view that produced it.
func requireStage4AdapterPostgresDeleteComposition(
	cfg config.Config,
	sourceEngine string,
	targetEngine string,
	prepared stage4AdapterPrepared,
) error {
	if sourceEngine != "postgres" || targetEngine != "postgres" {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 delete reconciliation route %s-to-%s is not certified; only postgres-to-postgres is admitted",
				sourceEngine,
				targetEngine,
			),
		)
	}
	mode, err := normalizeAdapterTargetMode(cfg.Migration.TargetMode)
	if err != nil {
		return NewTransferError(ErrorClassPolicy, err)
	}
	if mode != "upsert" || prepared.mode != "upsert" {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 PostgreSQL delete reconciliation requires target mode upsert"),
		)
	}
	if cfg.Migration.StrictConsistency {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 PostgreSQL delete reconciliation is not yet composed with strict consistency"),
		)
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 ||
		prepared.incremental != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 PostgreSQL delete reconciliation currently requires full-table, non-incremental work"),
		)
	}
	if prepared.evolution != nil && !prepared.evolution.plan.Complete() {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 PostgreSQL delete reconciliation requires target schema evolution to be complete before any delete pass"),
		)
	}
	if err := validateStage4AdapterPostgresDeletePolicy(
		cfg.Migration.Deletes,
	); err != nil {
		return NewTransferError(ErrorClassPolicy, err)
	}
	if len(prepared.plans) == 0 ||
		len(prepared.work) != len(prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete composition requires one network work identity per selected table"),
		)
	}
	seen := make(map[state.TaskKey]struct{}, len(prepared.work))
	for index := range prepared.plans {
		if err := validateStage4AdapterPostgresDeleteWork(
			prepared.plans[index],
			prepared.work[index],
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("admit Stage 4 PostgreSQL delete table %d: %w", index, err),
			)
		}
		if _, duplicate := seen[prepared.work[index].task]; duplicate {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("Stage 4 PostgreSQL delete composition contains duplicate table work %#v", prepared.work[index].task),
			)
		}
		seen[prepared.work[index].task] = struct{}{}
	}
	return nil
}

func validateStage4AdapterPostgresDeletePolicy(
	policy config.DeletePolicy,
) error {
	if policy.Mode != config.DeleteModeReconcile ||
		policy.TargetBehavior != config.DeleteTargetHard ||
		policy.Reconcile.Schedule != config.DeleteScheduleInterval ||
		policy.Reconcile.Interval <= 0 ||
		policy.Reconcile.BatchSize <= 0 ||
		!policy.Reconcile.RequirePrimaryKey {
		return fmt.Errorf(
			"Stage 4 PostgreSQL delete composition requires reconcile/hard/interval with a positive interval, positive batch size, and primary-key enforcement",
		)
	}
	return nil
}

func validateStage4AdapterPostgresDeleteWork(
	plan adapterTablePlan,
	work stage4AdapterWork,
) error {
	if plan.source.Name == "" || plan.target.Name == "" ||
		plan.source.Name != plan.target.Name {
		return fmt.Errorf("source and target table identities are incomplete or differ")
	}
	if err := work.task.Validate(); err != nil {
		return fmt.Errorf("network task: %w", err)
	}
	if work.task.Type != stage4AdapterNetworkTaskType ||
		work.task.Partition != "" ||
		work.task.Schema != plan.source.Schema ||
		work.task.Table != plan.source.Name {
		return fmt.Errorf("network work is not the exact unpartitioned source-table task")
	}
	if work.strategy != stage4AdapterCopyStrategy ||
		!validNetworkFactToken(work.topology) {
		return fmt.Errorf("network work lacks its exact strategy or topology identity")
	}
	return nil
}

func stage4AdapterPostgresDeleteBatchByteLimit(
	memoryCeilingBytes int64,
) (int64, error) {
	if memoryCeilingBytes <= 0 {
		return 0, fmt.Errorf("Stage 4 PostgreSQL delete composition requires a positive memory ceiling")
	}
	limit := memoryCeilingBytes / 4
	if limit <= 0 {
		return 0, fmt.Errorf("Stage 4 PostgreSQL delete batch byte limit is not positive")
	}
	if limit > postgresDeleteMaximumBatchBytes {
		limit = postgresDeleteMaximumBatchBytes
	}
	return limit, nil
}

type stage4AdapterPostgresDeleteAttemptWire struct {
	Version      int           `json:"version"`
	Kind         string        `json:"kind"`
	RunID        string        `json:"run_id"`
	Task         state.TaskKey `json:"task"`
	Strategy     string        `json:"strategy"`
	TopologyHash string        `json:"topology_hash"`
}

// stage4AdapterPostgresDeleteAttemptID binds an attempt to the immutable run
// and the final persisted network work identity. Clocks, retry counters,
// candidate counts, random spool/plan IDs, and credentials are intentionally
// absent, so a hard-crash resume selects the same attempt and durable spool.
func stage4AdapterPostgresDeleteAttemptID(
	runID string,
	work stage4AdapterWork,
) (string, error) {
	if strings.TrimSpace(runID) == "" ||
		runID != strings.TrimSpace(runID) {
		return "", fmt.Errorf("Stage 4 PostgreSQL delete attempt requires a canonical run ID")
	}
	if err := work.task.Validate(); err != nil {
		return "", fmt.Errorf("Stage 4 PostgreSQL delete attempt task: %w", err)
	}
	if work.task.Type != stage4AdapterNetworkTaskType ||
		work.task.Partition != "" {
		return "", fmt.Errorf("Stage 4 PostgreSQL delete attempt requires an unpartitioned network table task")
	}
	if strings.TrimSpace(work.strategy) == "" ||
		work.strategy != strings.TrimSpace(work.strategy) ||
		!validNetworkFactToken(work.strategy) {
		return "", fmt.Errorf("Stage 4 PostgreSQL delete attempt requires a canonical persisted strategy")
	}
	if !validNetworkFactToken(work.topology) {
		return "", fmt.Errorf("Stage 4 PostgreSQL delete attempt requires a canonical persisted topology")
	}
	wire := stage4AdapterPostgresDeleteAttemptWire{
		Version:      stage4AdapterPostgresDeleteAttemptVersion,
		Kind:         "postgres-delete-reconciliation",
		RunID:        runID,
		Task:         work.task,
		Strategy:     work.strategy,
		TopologyHash: work.topology,
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode Stage 4 PostgreSQL delete attempt identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (composition *stage4AdapterPostgresDeletePrepared) requestFor(
	entry stage4AdapterPostgresDeleteEntry,
	work stage4AdapterWork,
) (deleteReconciler, deleteReconcileRequest, error) {
	if composition == nil {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete composition is unavailable")
	}
	if entry.planIndex < 0 || entry.planIndex >= len(composition.entries) {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete plan index is outside the admitted composition")
	}
	admitted := composition.entries[entry.planIndex]
	if admitted.planIndex != entry.planIndex ||
		admitted.source.Schema != entry.source.Schema ||
		admitted.source.Name != entry.source.Name ||
		admitted.target.Schema != entry.target.Schema ||
		admitted.target.Name != entry.target.Name {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete entry differs from the admitted plan")
	}
	if composition.run.RunID == "" ||
		stage4BackendIsNil(composition.run.Backend) ||
		composition.run.SpoolDirectory == "" {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete composition lacks exact run state or spool authority")
	}
	if networkMutationProtectorIsNil(composition.protector) {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete composition lacks target mutation protection")
	}
	if composition.maxBatchBytes <= 0 {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete composition has no positive batch byte limit")
	}
	plan := adapterTablePlan{source: entry.source, target: entry.target}
	if err := validateStage4AdapterPostgresDeleteWork(plan, work); err != nil {
		return deleteReconciler{}, deleteReconcileRequest{}, err
	}
	attemptID, err := stage4AdapterPostgresDeleteAttemptID(
		composition.run.RunID,
		work,
	)
	if err != nil {
		return deleteReconciler{}, deleteReconcileRequest{}, err
	}
	now := time.Now().UTC()
	if composition.now != nil {
		now = composition.now().UTC()
	}
	if now.IsZero() {
		return deleteReconciler{}, deleteReconcileRequest{}, fmt.Errorf("Stage 4 PostgreSQL delete composition current time is required")
	}
	protector := stage4AdapterPostgresDeleteMutationProtector{
		target: composition.protector,
	}
	reconciler := deleteReconciler{
		state:         composition.run.Backend,
		source:        entry.capabilities.source,
		target:        entry.capabilities.target,
		canonicalizer: entry.capabilities.canonicalizer,
		protector:     protector,
		now:           composition.now,
	}
	request := deleteReconcileRequest{
		RunID:          composition.run.RunID,
		AttemptID:      attemptID,
		Task:           work.task,
		SourceTable:    cloneStage4RichTable(entry.source),
		TargetTable:    cloneStage4RichTable(entry.target),
		TargetMode:     "upsert",
		Policy:         composition.policy,
		DryRun:         false,
		Now:            now,
		SpoolDirectory: composition.run.SpoolDirectory,
		MaxBatchBytes:  composition.maxBatchBytes,
	}
	return reconciler, request, nil
}

type stage4AdapterPostgresDeleteMutationProtector struct {
	target adapterTargetMutationProtector
}

func (protector stage4AdapterPostgresDeleteMutationProtector) ProtectDeleteMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if networkMutationProtectorIsNil(protector.target) {
		return fmt.Errorf("Stage 4 PostgreSQL delete mutation protector is unavailable")
	}
	if mutation == nil {
		return fmt.Errorf("Stage 4 PostgreSQL delete mutation is unavailable")
	}
	return protector.target.ProtectTargetMutation(ctx, mutation)
}

// reconcile executes only after every selected table has completed its data
// transfer/finalization. Callers pass those final work identities rather than
// the pre-pagination identities from initial admission.
func (composition *stage4AdapterPostgresDeletePrepared) reconcile(
	ctx context.Context,
	finalWork []stage4AdapterWork,
) (stage4AdapterPostgresDeleteResult, error) {
	if composition == nil {
		return stage4AdapterPostgresDeleteResult{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete composition is unavailable"),
		)
	}
	result := stage4AdapterPostgresDeleteResult{
		tables: make(
			[]stage4AdapterPostgresDeleteTableOutcome,
			0,
			len(composition.entries),
		),
		strictByTable: make(
			map[stage4RichTableKey]bool,
			len(composition.entries),
		),
	}
	if ctx == nil {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete execution context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	bound, err := composition.bindFinalWork(finalWork)
	if err != nil {
		return result, NewTransferError(ErrorClassState, err)
	}
	inventory, err := loadStage4WorkInventory(ctx, composition.run)
	if err != nil {
		return result, err
	}
	if err := composition.authenticateFinalWork(bound, inventory); err != nil {
		return result, NewTransferError(ErrorClassState, err)
	}
	deleteOrder, err := stage4AdapterPostgresDeleteReversePlanIndexes(
		len(composition.entries),
	)
	if err != nil {
		return result, NewTransferError(ErrorClassState, err)
	}
	for _, order := range deleteOrder {
		entry := composition.entries[order]
		work := bound[entry.planIndex]
		reconciler, request, requestErr := composition.requestFor(
			entry,
			work,
		)
		if requestErr != nil {
			return result, NewTransferError(ErrorClassState, requestErr)
		}
		currentAuthority, authorityErr :=
			composition.currentCanonicalizer(ctx, entry)
		if authorityErr != nil {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"derive current Stage 4 PostgreSQL delete authority for table %s: %w",
					entry.source.Name,
					authorityErr,
				),
			)
		}
		reconciler.canonicalizer = currentAuthority
		outcome, reconcileErr := reconciler.reconcile(ctx, request)
		if reconcileErr != nil {
			return result, fmt.Errorf(
				"reconcile Stage 4 PostgreSQL target-only rows for table %s: %w",
				entry.source.Name,
				reconcileErr,
			)
		}
		if authorityErr := composition.authenticateTerminalAuthority(
			ctx,
			entry,
			reconciler,
			request,
			outcome,
		); authorityErr != nil {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"authenticate terminal Stage 4 PostgreSQL delete authority for table %s: %w",
					entry.source.Name,
					authorityErr,
				),
			)
		}
		strict, strictErr :=
			stage4AdapterPostgresDeleteTerminalStrictness(outcome)
		if strictErr != nil {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"authenticate terminal Stage 4 PostgreSQL delete evidence for table %s: %w",
					entry.source.Name,
					strictErr,
				),
			)
		}
		tableOutcome := stage4AdapterPostgresDeleteTableOutcome{
			planIndex: entry.planIndex,
			work:      cloneStage4AdapterNetworkWork(work),
			outcome:   outcome,
		}
		result.tables = append(result.tables, tableOutcome)
		key := stage4RichTableKey{
			schema: entry.source.Schema,
			table:  entry.source.Name,
		}
		if _, duplicate := result.strictByTable[key]; duplicate {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf("duplicate Stage 4 PostgreSQL delete outcome for table (%q, %q)", key.schema, key.table),
			)
		}
		result.strictByTable[key] = strict
	}
	if len(result.strictByTable) != len(composition.entries) {
		return result, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 PostgreSQL delete outcomes do not cover every selected table"),
		)
	}
	return result, nil
}

func (composition *stage4AdapterPostgresDeletePrepared) currentCanonicalizer(
	ctx context.Context,
	entry stage4AdapterPostgresDeleteEntry,
) (deleteKeyCanonicalizer, error) {
	if composition == nil {
		return nil, fmt.Errorf("Stage 4 PostgreSQL delete composition is unavailable")
	}
	if ctx == nil {
		return nil, fmt.Errorf("Stage 4 PostgreSQL delete authority context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if entry.currentAuthority == nil {
		return nil, fmt.Errorf(
			"current PostgreSQL source/target catalog authority reader is unavailable",
		)
	}
	canonicalizer, err := entry.currentAuthority(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"derive current PostgreSQL source/target catalog authority: %w",
			err,
		)
	}
	if isNilInterface(canonicalizer) {
		return nil, fmt.Errorf(
			"current PostgreSQL delete key equality authority is unavailable",
		)
	}
	return canonicalizer, nil
}

// authenticateTerminalAuthority prevents an immutable terminal status from
// becoming a stale catalog-authority bypass. PostgreSQL relation OIDs,
// ownership/current-user privilege facts, exact primary-key/index metadata,
// and equality semantics are re-read after the terminal record is loaded.
// The resulting proof must match the durable candidate plan before the
// enclosing lifecycle may reuse the outcome for validation or checkpoints.
//
// NotDue records deliberately contain no plan. Their authority is therefore
// inherited only from the exact latest successful Completed record that made
// the interval not due; absence or mismatch of that plan fails closed.
func (composition *stage4AdapterPostgresDeletePrepared) authenticateTerminalAuthority(
	ctx context.Context,
	entry stage4AdapterPostgresDeleteEntry,
	reconciler deleteReconciler,
	request deleteReconcileRequest,
	outcome deleteReconcileOutcome,
) error {
	if composition == nil {
		return fmt.Errorf("Stage 4 PostgreSQL delete composition is unavailable")
	}
	if ctx == nil {
		return fmt.Errorf("Stage 4 PostgreSQL delete authority context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if outcome.Record.Status != state.DeleteReconciliationCompleted &&
		outcome.Record.Status != state.DeleteReconciliationNotDue {
		return nil
	}
	if outcome.Record.RunID != request.RunID ||
		outcome.Record.Task != request.Task ||
		outcome.Record.AttemptID != request.AttemptID {
		return fmt.Errorf(
			"terminal delete evidence differs from the exact run, task, or attempt",
		)
	}

	if isNilInterface(reconciler.canonicalizer) {
		return fmt.Errorf(
			"current PostgreSQL delete key equality authority is unavailable",
		)
	}
	keyPlan, err := validateDeleteReconcileRequest(
		request,
		reconciler.canonicalizer,
	)
	if err != nil {
		return fmt.Errorf(
			"derive current PostgreSQL delete key equality proof: %w",
			err,
		)
	}
	authorityRecord := outcome.Record
	if outcome.Record.Status == state.DeleteReconciliationNotDue {
		if reconciler.state == nil {
			return fmt.Errorf(
				"delete reconciliation state authority is unavailable",
			)
		}
		latest, found, loadErr := reconciler.state.
			LoadLatestSuccessfulDeleteReconciliation(
				request.RunID,
				request.Task,
			)
		if loadErr != nil {
			return fmt.Errorf(
				"load successful delete authority for not-due evidence: %w",
				loadErr,
			)
		}
		if !found {
			return fmt.Errorf(
				"not-due delete evidence has no successful reconciliation authority",
			)
		}
		if latest.Task != request.Task ||
			latest.Status != state.DeleteReconciliationCompleted {
			return fmt.Errorf(
				"not-due delete evidence has no exact completed reconciliation authority",
			)
		}
		due, dueErr := deleteReconciliationDue(
			request.Now,
			request.Policy.Reconcile.Interval,
			latest,
			true,
		)
		if dueErr != nil {
			return fmt.Errorf(
				"authenticate not-due delete schedule authority: %w",
				dueErr,
			)
		}
		if due.Due {
			return fmt.Errorf(
				"stored not-due delete evidence is no longer current for this interval",
			)
		}
		authorityRecord = latest
	}
	if err := state.ValidateDeleteReconciliationEvidence(
		authorityRecord,
	); err != nil {
		return fmt.Errorf(
			"durable successful delete authority is malformed: %w",
			err,
		)
	}
	if authorityRecord.Status != state.DeleteReconciliationCompleted ||
		authorityRecord.Plan == nil {
		return fmt.Errorf(
			"terminal delete evidence lacks a durable completed candidate plan",
		)
	}
	plan := authorityRecord.Plan
	if plan.EqualityProofDigest != keyPlan.proofDigest ||
		plan.KeyWidth != len(keyPlan.targetColumns) {
		return fmt.Errorf(
			"durable terminal delete plan differs from current PostgreSQL catalog or key-equality authority",
		)
	}
	if outcome.Record.Status == state.DeleteReconciliationCompleted {
		if isNilInterface(reconciler.target) {
			return fmt.Errorf(
				"current PostgreSQL delete target batching authority is unavailable",
			)
		}
		batchSize, batchErr := deleteBatchLimit(
			request.Policy.Reconcile.BatchSize,
			reconciler.target.MaxDeleteParameters(),
			len(keyPlan.targetColumns),
		)
		if batchErr != nil {
			return fmt.Errorf(
				"derive current PostgreSQL delete batch authority: %w",
				batchErr,
			)
		}
		if plan.BatchSize != batchSize ||
			plan.BatchByteLimit != request.MaxBatchBytes {
			return fmt.Errorf(
				"durable terminal delete plan differs from current PostgreSQL batching authority",
			)
		}
	}
	return nil
}

func stage4AdapterPostgresDeleteReversePlanIndexes(
	tableCount int,
) ([]int, error) {
	if tableCount <= 0 {
		return nil, fmt.Errorf("Stage 4 PostgreSQL delete execution requires at least one selected table")
	}
	result := make([]int, tableCount)
	for index := range result {
		result[index] = tableCount - index - 1
	}
	return result, nil
}

func (composition *stage4AdapterPostgresDeletePrepared) bindFinalWork(
	finalWork []stage4AdapterWork,
) ([]stage4AdapterWork, error) {
	if composition == nil || len(composition.entries) == 0 ||
		len(finalWork) != len(composition.entries) {
		return nil, fmt.Errorf("Stage 4 PostgreSQL delete execution requires one final work identity per admitted table")
	}
	byTask := make(map[state.TaskKey]stage4AdapterWork, len(finalWork))
	for _, work := range finalWork {
		if _, duplicate := byTask[work.task]; duplicate {
			return nil, fmt.Errorf("Stage 4 PostgreSQL delete execution received duplicate final work %#v", work.task)
		}
		byTask[work.task] = cloneStage4AdapterNetworkWork(work)
	}
	bound := make([]stage4AdapterWork, len(composition.entries))
	for _, entry := range composition.entries {
		task := state.TaskKey{
			Type:   stage4AdapterNetworkTaskType,
			Schema: entry.source.Schema,
			Table:  entry.source.Name,
		}
		work, found := byTask[task]
		if !found {
			return nil, fmt.Errorf("Stage 4 PostgreSQL delete execution lacks final work for table (%q, %q)", task.Schema, task.Table)
		}
		if err := validateStage4AdapterPostgresDeleteWork(
			adapterTablePlan{source: entry.source, target: entry.target},
			work,
		); err != nil {
			return nil, fmt.Errorf("validate final Stage 4 PostgreSQL delete work for table %s: %w", task.Table, err)
		}
		if entry.planIndex < 0 || entry.planIndex >= len(bound) {
			return nil, fmt.Errorf("Stage 4 PostgreSQL delete entry has an invalid plan index")
		}
		bound[entry.planIndex] = work
	}
	return bound, nil
}

// authenticateFinalWork makes "final" a durable fact rather than a caller
// assertion. A delete pass may begin while the enclosing table task is still
// running (task completion is intentionally deferred until validation), but
// every one of its exact ranges must already be durably complete.
func (composition *stage4AdapterPostgresDeletePrepared) authenticateFinalWork(
	bound []stage4AdapterWork,
	inventory stage4WorkInventory,
) error {
	if composition == nil || len(bound) != len(composition.entries) {
		return fmt.Errorf("Stage 4 PostgreSQL delete final-work authentication is incomplete")
	}
	for _, entry := range composition.entries {
		if entry.planIndex < 0 || entry.planIndex >= len(bound) {
			return fmt.Errorf("Stage 4 PostgreSQL delete final-work plan index is invalid")
		}
		work := bound[entry.planIndex]
		task, durableRanges, found, err := exactStage4AdapterWork(
			inventory,
			work,
			false,
		)
		if err != nil {
			return fmt.Errorf(
				"authenticate Stage 4 PostgreSQL delete table %s final work: %w",
				work.task.Table,
				err,
			)
		}
		if !found {
			return fmt.Errorf("Stage 4 PostgreSQL delete table %s lacks durable final work", work.task.Table)
		}
		if task.RunID != composition.run.RunID ||
			task.Key != work.task ||
			task.Strategy != work.strategy ||
			task.TopologyHash != work.topology ||
			task.Error != "" ||
			(task.Status != "running" && task.Status != "completed") ||
			task.StartedAt.IsZero() || task.UpdatedAt.IsZero() ||
			task.UpdatedAt.Before(task.StartedAt) {
			return fmt.Errorf("Stage 4 PostgreSQL delete table %s has unauthenticated durable task identity", work.task.Table)
		}
		if task.Status == "running" && !task.CompletedAt.IsZero() ||
			task.Status == "completed" &&
				(task.CompletedAt.IsZero() || !task.UpdatedAt.Equal(task.CompletedAt)) {
			return fmt.Errorf("Stage 4 PostgreSQL delete table %s has contradictory durable task completion", work.task.Table)
		}
		if len(work.ranges) == 0 {
			return fmt.Errorf("Stage 4 PostgreSQL delete table %s has no final range inventory", work.task.Table)
		}
		for index, workRange := range durableRanges {
			restore, restoreErr := networkRestoreFromState(
				networkStateRangeBinding{
					RangeIndex: uint64(index),
					Task:       work.task,
					Initial:    work.ranges[index],
				},
				workRange,
			)
			if restoreErr != nil {
				return fmt.Errorf(
					"Stage 4 PostgreSQL delete table %s range %s has unsafe restore evidence: %w",
					work.task.Table,
					workRange.ID,
					restoreErr,
				)
			}
			if workRange.RunID != composition.run.RunID ||
				workRange.Task != work.task ||
				workRange.Strategy != work.strategy ||
				workRange.TopologyHash != work.topology ||
				workRange.Status != "completed" ||
				workRange.Error != "" ||
				len(workRange.Pending) != 0 ||
				workRange.SequenceOffset != 0 ||
				workRange.CommittedPrefix != 0 ||
				workRange.CompletedAt.IsZero() ||
				workRange.UpdatedAt.IsZero() ||
				!workRange.UpdatedAt.Equal(workRange.CompletedAt) ||
				workRange.CompletedAt.Before(task.StartedAt) ||
				task.Status == "completed" &&
					workRange.CompletedAt.After(task.CompletedAt) ||
				!restore.Complete || restore.SequenceOffset != 0 ||
				len(restore.Issued) != 0 {
				return fmt.Errorf("Stage 4 PostgreSQL delete table %s range %s is not durably complete", work.task.Table, workRange.ID)
			}
			if err := validateCompletedStage4AdapterRange(
				task,
				work,
				index,
				workRange,
				restore,
			); err != nil {
				return fmt.Errorf(
					"Stage 4 PostgreSQL delete table %s range %s has invalid completion evidence: %w",
					work.task.Table,
					workRange.ID,
					err,
				)
			}
		}
	}
	return nil
}

func stage4AdapterPostgresDeleteTerminalStrictness(
	outcome deleteReconcileOutcome,
) (bool, error) {
	if err := state.ValidateDeleteReconciliationEvidence(
		outcome.Record,
	); err != nil {
		return false, fmt.Errorf("delete reconciliation evidence is malformed: %w", err)
	}
	var strict bool
	switch outcome.Record.Status {
	case state.DeleteReconciliationCompleted:
		strict = true
	case state.DeleteReconciliationNotDue:
		strict = false
	case state.DeleteReconciliationDryRun:
		return false, fmt.Errorf("production delete reconciliation returned dry-run evidence")
	case state.DeleteReconciliationRunning:
		return false, fmt.Errorf("delete reconciliation remains running")
	case state.DeleteReconciliationIncomplete:
		return false, fmt.Errorf("delete reconciliation is incomplete: %s", outcome.Record.Reason)
	default:
		return false, fmt.Errorf("delete reconciliation has unknown terminal status %q", outcome.Record.Status)
	}
	if outcome.StrictCountValidation != strict {
		return false, fmt.Errorf("delete reconciliation strict-count outcome contradicts its authenticated terminal status")
	}
	return strict, nil
}
