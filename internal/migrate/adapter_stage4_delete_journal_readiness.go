package migrate

import (
	"context"
	"fmt"

	"github.com/johndauphine/dmtx/internal/state"
)

// Stage4DeleteJournalReadinessRequest is the durable boundary supplied to an
// optional target-owned delete-journal preparer. Existing is present only when
// a prior process durably saved a matching receipt. A preparer must always
// perform an exact native journal reread: Existing is evidence to compare, not
// permission to trust an in-memory flag.
type Stage4DeleteJournalReadinessRequest struct {
	RunID           string
	InventoryDigest string
	Existing        *state.Stage4DeleteJournalReadinessReceipt
}

// adapterStage4DeleteJournalReadinessPreflighter is the read-only admission
// seam for a private delete journal. It checks reserved journal names and
// target capability before any ordinary task/work checkpoint is written. It
// must not create a journal, mutate a table, or write any target data.
type adapterStage4DeleteJournalReadinessPreflighter interface {
	PreflightStage4DeleteJournalReadiness(context.Context) error
}

// adapterStage4DeleteJournalReadinessPreparer creates or verifies a
// table-independent, target-private delete journal after immutable work is
// durably bound. Its implementation may issue private journal DDL, but it
// must then reread the native catalog and return exactly what that reread
// observed. On recovery after DDL but before the state receipt was saved, it
// must use that same native reread; an in-memory success flag is not authority.
type adapterStage4DeleteJournalReadinessPreparer interface {
	PrepareStage4DeleteJournalReadiness(
		context.Context,
		Stage4DeleteJournalReadinessRequest,
	) (state.Stage4DeleteJournalReadiness, error)
}

type stage4AdapterDeleteJournalReadinessCapability struct {
	targetEngine string
	preparer     adapterStage4DeleteJournalReadinessPreparer
}

// admitStage4DeleteJournalReadiness is deliberately read-only and runs before
// ordinary Stage 4 checkpointing. A target must provide both halves of the
// protocol; accepting only a preparer would silently skip journal collision
// and capability admission, while accepting only a preflight would provide no
// durable target authority.
func admitStage4DeleteJournalReadiness(
	ctx context.Context,
	target targetAdapter,
) (*stage4AdapterDeleteJournalReadinessCapability, error) {
	preflight, hasPreflight := target.(adapterStage4DeleteJournalReadinessPreflighter)
	preparer, hasPreparer := target.(adapterStage4DeleteJournalReadinessPreparer)
	if !hasPreflight && !hasPreparer {
		return nil, nil
	}
	if !hasPreflight || !hasPreparer || isNilInterface(preflight) ||
		isNilInterface(preparer) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target delete-journal readiness requires both read-only preflight and native reread preparation",
			),
		)
	}
	if err := preflight.PreflightStage4DeleteJournalReadiness(ctx); err != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("preflight Stage 4 delete-journal readiness: %w", err),
		)
	}
	return &stage4AdapterDeleteJournalReadinessCapability{
		targetEngine: target.Engine(),
		preparer:     preparer,
	}, nil
}

// requireStage4AdapterDeleteJournalReadinessPrecheckpointCapabilities keeps
// purely local capability failures ahead of the first ordinary Stage 4
// checkpoint. The target preflight has already run by this point, but this
// function intentionally performs no state read or write and invokes no
// target mutation. ensureStage4AdapterDeleteJournalReadiness repeats these
// checks at the native-DDL boundary because an observer or backend can still
// be replaced while a run is being recovered.
func requireStage4AdapterDeleteJournalReadinessPrecheckpointCapabilities(
	observer TableObserver,
	run Stage4RunContext,
	capability *stage4AdapterDeleteJournalReadinessCapability,
) error {
	if capability == nil {
		return nil
	}
	if isNilInterface(capability.preparer) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness preparer is unavailable"),
		)
	}
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 delete-journal readiness requires a lease-fenced target mutation protector",
			),
		)
	}
	aggregate, ok := run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness requires aggregate table inventory state"),
		)
	}
	readiness, ok := run.Backend.(state.Stage4DeleteJournalReadinessBackend)
	if !ok || isNilInterface(readiness) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness backend is unavailable"),
		)
	}
	return nil
}

// admitStage4AdapterDeleteJournalReadinessForRun is the pre-checkpoint
// readiness admission used by prepareStage4AdapterRun. It preserves the
// target-owned read-only preflight while rejecting a missing state authority
// or mutation fence before table inventory, ordinary tasks, or work plans can
// be persisted.
func admitStage4AdapterDeleteJournalReadinessForRun(
	ctx context.Context,
	observer TableObserver,
	run Stage4RunContext,
	target targetAdapter,
) (*stage4AdapterDeleteJournalReadinessCapability, error) {
	capability, err := admitStage4DeleteJournalReadiness(ctx, target)
	if err != nil {
		return nil, err
	}
	if err := requireStage4AdapterDeleteJournalReadinessPrecheckpointCapabilities(
		observer,
		run,
		capability,
	); err != nil {
		return nil, err
	}
	return capability, nil
}

// ensureStage4AdapterDeleteJournalReadiness runs at one narrow lifecycle
// boundary: immutable table inventory, ordinary tasks, and structured work
// must already be durable and pristine. Target schema evolution may already
// have been applied and exactly reverified, but no PrepareTables, row write,
// delete, incremental attempt, or completed/advanced schema sentinel may be
// reachable. A stored receipt is loaded before native preparation so a commit
// that later reports an error resumes with the exact original ReadyAt. Native
// preparation is itself a target mutation and therefore runs only inside the
// route's lease-fenced mutation protector; state receipt fencing alone is too
// late for an auto-committing journal DDL.
func ensureStage4AdapterDeleteJournalReadiness(
	ctx context.Context,
	observer TableObserver,
	prepared stage4AdapterPrepared,
) error {
	capability := prepared.deleteJournalReadiness
	if capability == nil {
		return nil
	}
	if ctx == nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness context is required"),
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isNilInterface(capability.preparer) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness preparer is unavailable"),
		)
	}
	protector, protected := observer.(adapterTargetMutationProtector)
	if !protected || networkMutationProtectorIsNil(protector) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 delete-journal readiness requires a lease-fenced target mutation protector",
			),
		)
	}
	aggregate, ok := prepared.run.Backend.(state.Stage4AggregateBackend)
	if !ok || nilStage4AggregateBackend(aggregate) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness requires aggregate table inventory state"),
		)
	}
	readiness, ok := prepared.run.Backend.(state.Stage4DeleteJournalReadinessBackend)
	if !ok || isNilInterface(readiness) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness backend is unavailable"),
		)
	}
	inventory, found, err := aggregate.LoadStage4TableInventory(prepared.run.RunID)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read immutable Stage 4 table inventory for delete-journal readiness: %w", err),
		)
	}
	if !found {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 delete-journal readiness requires immutable table inventory"),
		)
	}
	stored, storedFound, err := readiness.LoadStage4DeleteJournalReadiness(
		prepared.run.RunID,
	)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("read Stage 4 delete-journal readiness receipt: %w", err),
		)
	}
	if !storedFound {
		if err := readiness.ValidateStage4DeleteJournalReadinessBoundary(
			state.Stage4DeleteJournalReadinessBoundary{
				RunID:           prepared.run.RunID,
				InventoryDigest: inventory.Digest,
			},
		); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("validate pristine Stage 4 delete-journal readiness boundary: %w", err),
			)
		}
	}
	request := Stage4DeleteJournalReadinessRequest{
		RunID:           prepared.run.RunID,
		InventoryDigest: inventory.Digest,
	}
	if storedFound {
		storedCopy := stored.Clone()
		request.Existing = &storedCopy
	}
	var observed state.Stage4DeleteJournalReadiness
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"prepare Stage 4 delete journal",
		func() error {
			var prepareErr error
			observed, prepareErr = capability.preparer.PrepareStage4DeleteJournalReadiness(
				ctx,
				request,
			)
			return prepareErr
		},
	); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"lease-fence prepare and native-reread Stage 4 delete journal: %w",
				err,
			),
		)
	}
	if err := observed.Validate(); err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("validate native Stage 4 delete-journal readiness: %w", err),
		)
	}
	if observed.RunID != prepared.run.RunID ||
		observed.InventoryDigest != inventory.Digest ||
		observed.TargetEngine != capability.targetEngine {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("native Stage 4 delete-journal readiness differs from run inventory or target engine"),
		)
	}
	if storedFound {
		// Native catalog facts must still match every stored authority field.
		// Only the observation timestamp is deliberately carried from durable
		// state, otherwise a committed receipt followed by a returned error
		// would be impossible to reproduce on resume.
		observed.ReadyAt = stored.Readiness.ReadyAt
		if !observed.Equal(stored.Readiness) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("native Stage 4 delete-journal reread differs from durable readiness receipt"),
			)
		}
	}
	receipt, _, err := readiness.EnsureStage4DeleteJournalReadiness(observed)
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("durably record Stage 4 delete-journal readiness: %w", err),
		)
	}
	if !receipt.Readiness.Equal(observed) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("durable Stage 4 delete-journal readiness differs from native reread"),
		)
	}
	return nil
}
