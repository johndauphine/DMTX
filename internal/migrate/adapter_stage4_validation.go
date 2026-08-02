package migrate

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// Stage 4 validation: probe construction, the count/sample gate, and the
// per-table specs a run validates against.

func requireStage4AdapterSeams(
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	gate Stage4SchemaGateResult,
	mode string,
) error {
	if err := requireStage4AdapterConfigurationSeams(cfg); err != nil {
		return err
	}
	if gate.RebuildRequiresTargetCatalog {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 schema rebuild retains prior-only objects but route %s-to-%s has no composed target-catalog rebuild seam",
				source.Engine(),
				target.Engine(),
			),
		)
	}
	// Date-based incremental validation is admitted and executed through its
	// attempt-bound evidence probe. It must not construct the ordinary
	// whole-table validation probe, which would observe a later live source
	// state rather than the immutable transferred window.
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		return nil
	}
	validationMode := cfg.Migration.Validation.Mode
	if validationMode == "" ||
		validationMode == config.ValidationCountOnly {
		return nil
	}
	if stage4ValidationProvider(observer, source, target) == nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q requires a composed adapter validation probe seam",
				validationMode,
			),
		)
	}
	return nil
}

func stage4ValidationProvider(
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
) adapterStage4ValidationProbeProvider {
	for _, candidate := range []any{observer, source, target} {
		if provider, ok := candidate.(adapterStage4ValidationProbeProvider); ok &&
			!isNilInterface(provider) {
			return provider
		}
	}
	return nil
}

func stage4AdapterValidationProbe(
	cfg config.Config,
	observer TableObserver,
	source sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
	providerSources ...sourceAdapter,
) (ValidationCoreProbe, error) {
	mode := cfg.Migration.Validation.Mode
	providerSource := source
	if len(providerSources) > 1 {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 validation accepts at most one provider source",
			),
		)
	}
	if len(providerSources) == 1 {
		providerSource = providerSources[0]
	}
	// Count through the supplied provider, not the pool adapter. A stable
	// network table validates while its source view still holds a pinned
	// connection, and MySQL, MariaDB, and SQL Server cap that pool at one
	// connection, so counting through the pool waits forever for a connection
	// the caller itself is holding. Counting through the stable view is also
	// the more truthful measurement: it counts the same snapshot that was
	// transferred rather than whatever the source looks like afterwards.
	if mode == "" || mode == config.ValidationCountOnly {
		return &stage4AdapterCountProbe{
			source: providerSource,
			target: target,
			plans:  stage4AdapterPlansBySource(plans),
		}, nil
	}
	provider := stage4ValidationProvider(
		observer,
		providerSource,
		target,
	)
	if provider == nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q requires a composed adapter validation probe seam",
				mode,
			),
		)
	}
	probe, err := provider.Stage4ValidationProbe(
		source,
		target,
		append([]adapterTablePlan(nil), plans...),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"construct Stage 4 validation probe: %w",
			err,
		)
	}
	if isNilInterface(probe) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation probe provider returned no probe",
			),
		)
	}
	return probe, nil
}

func prepareStage4AdapterValidationPrimaryKeyEqualityProofs(
	validationMode config.ValidationMode,
	targetMode string,
	probe ValidationCoreProbe,
	tables []schema.Table,
) (map[stage4RichTableKey]string, error) {
	if targetMode != "upsert" ||
		(validationMode != config.ValidationNullParity &&
			validationMode != config.ValidationSample) {
		return nil, nil
	}
	provider, ok := probe.(adapterStage4ValidationEqualityProofProvider)
	if !ok || isNilInterface(provider) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 validation mode %q in upsert mode requires a composed route-bound primary-key equality proof seam",
				validationMode,
			),
		)
	}
	proofs := make(
		map[stage4RichTableKey]string,
		len(tables),
	)
	for _, table := range tables {
		key := stage4RichTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, exists := proofs[key]; exists {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 validation equality proof inventory duplicates table (%q, %q)",
					table.Schema,
					table.Name,
				),
			)
		}
		proof, err := provider.Stage4ValidationPrimaryKeyEqualityProof(
			cloneStage4RichTable(table),
		)
		if err != nil {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"prepare Stage 4 primary-key equality proof for table (%q, %q): %w",
					table.Schema,
					table.Name,
					err,
				),
			)
		}
		if !validValidationEqualityProofDigest(proof) {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 primary-key equality proof for table (%q, %q) is not a canonical SHA-256 digest",
					table.Schema,
					table.Name,
				),
			)
		}
		proofs[key] = proof
	}
	return proofs, nil
}

func stage4AdapterPlansBySource(
	plans []adapterTablePlan,
) map[stage4RichTableKey]adapterTablePlan {
	result := make(
		map[stage4RichTableKey]adapterTablePlan,
		len(plans),
	)
	for _, plan := range plans {
		result[stage4RichTableKey{
			schema: plan.source.Schema,
			table:  plan.source.Name,
		}] = plan
	}
	return result
}

type stage4AdapterCountProbe struct {
	source     sourceAdapter
	target     targetAdapter
	plans      map[stage4RichTableKey]adapterTablePlan
	sourceGate stage4AdapterProbeGate
	targetGate stage4AdapterProbeGate
}

// stage4AdapterProbeGate serializes operations against one adapter while
// allowing a caller that has already timed out or been canceled to stop
// waiting. Source and target use separate gates so independent engines remain
// concurrent.
type stage4AdapterProbeGate struct {
	once  sync.Once
	token chan struct{}
}

func (gate *stage4AdapterProbeGate) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	gate.once.Do(func() {
		gate.token = make(chan struct{}, 1)
		gate.token <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate.token:
		if err := ctx.Err(); err != nil {
			gate.release()
			return err
		}
		return nil
	}
}

func (gate *stage4AdapterProbeGate) release() {
	gate.token <- struct{}{}
}

func (probe *stage4AdapterCountProbe) ExactCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	plan, err := probe.plan(table)
	if err != nil {
		return 0, err
	}
	var (
		count int
		gate  *stage4AdapterProbeGate
	)
	switch side {
	case ValidationSource:
		gate = &probe.sourceGate
	case ValidationTarget:
		gate = &probe.targetGate
	default:
		return 0, fmt.Errorf("unknown Stage 4 validation side %q", side)
	}
	if err := gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer gate.release()
	switch side {
	case ValidationSource:
		count, err = probe.source.CountRows(ctx, plan.source)
	case ValidationTarget:
		count, err = probe.target.CountRows(ctx, plan.target)
	}
	return int64(count), err
}

func (probe *stage4AdapterCountProbe) EstimateCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	plan, err := probe.plan(table)
	if err != nil {
		return 0, err
	}
	var (
		estimator adapterRowCountEstimator
		selected  schema.Table
		gate      *stage4AdapterProbeGate
	)
	switch side {
	case ValidationSource:
		estimator, _ = probe.source.(adapterRowCountEstimator)
		selected = plan.source
		gate = &probe.sourceGate
	case ValidationTarget:
		estimator, _ = probe.target.(adapterRowCountEstimator)
		selected = plan.target
		gate = &probe.targetGate
	default:
		return 0, fmt.Errorf("unknown Stage 4 validation side %q", side)
	}
	if estimator == nil {
		return 0, fmt.Errorf(
			"Stage 4 %s count estimate is unavailable; exact count was not relabeled",
			side,
		)
	}
	if err := gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer gate.release()
	estimate, err := estimator.EstimateRows(ctx, selected)
	if err != nil {
		return 0, err
	}
	if estimate < 0 {
		return 0, fmt.Errorf(
			"Stage 4 %s count estimate is negative",
			side,
		)
	}
	return estimate, nil
}

func (probe *stage4AdapterCountProbe) NullCounts(
	context.Context,
	ValidationSide,
	schema.Table,
	[]string,
	ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	return ValidationNullCountEvidence{}, fmt.Errorf(
		"Stage 4 count-only adapter probe does not implement NULL parity",
	)
}

func (probe *stage4AdapterCountProbe) SampleSourceRows(
	context.Context,
	schema.Table,
	[]string,
	int,
) ([]ValidationSampleRow, error) {
	return nil, fmt.Errorf(
		"Stage 4 count-only adapter probe does not implement row sampling",
	)
}

func (probe *stage4AdapterCountProbe) SampleTargetRows(
	context.Context,
	schema.Table,
	[]string,
	[]ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	return nil, fmt.Errorf(
		"Stage 4 count-only adapter probe does not implement row sampling",
	)
}

func (probe *stage4AdapterCountProbe) plan(
	table schema.Table,
) (adapterTablePlan, error) {
	plan, ok := probe.plans[stage4RichTableKey{
		schema: table.Schema,
		table:  table.Name,
	}]
	if !ok {
		return adapterTablePlan{}, fmt.Errorf(
			"no Stage 4 adapter plan for validation table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	return plan, nil
}

func validateStage4AdapterRun(
	ctx context.Context,
	cfg config.Config,
	source sourceAdapter,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		return err
	}
	report, err := RunValidationCore(
		ctx,
		ValidationCoreOptions{
			Mode:                   cfg.Migration.Validation.Mode,
			TargetMode:             prepared.mode,
			FailOnMismatch:         cfg.Migration.Validation.FailOnMismatch,
			FailOnTimeout:          cfg.Migration.Validation.FailOnTimeout,
			FailOnEstimateMismatch: cfg.Migration.Validation.FailOnEstimateMismatch,
			ExactCountTimeout:      30 * time.Second,
			TableTimeout:           2 * time.Minute,
			TableConcurrency:       stage4ValidationConcurrency(len(specs)),
			SampleLimit:            100,
		},
		specs,
		prepared.validation,
	)
	if err != nil {
		return fmt.Errorf("run Stage 4 validation core: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !report.Passed {
		return NewTransferError(
			ErrorClassValidation,
			fmt.Errorf(
				"Stage 4 post-finalize validation failed for route %s-to-%s",
				source.Engine(),
				target.Engine(),
			),
		)
	}
	return nil
}

func stage4AdapterValidationTableSpecs(
	prepared stage4AdapterPrepared,
) ([]ValidationTableSpec, error) {
	plans := stage4AdapterPlansBySource(prepared.plans)
	specs := make(
		[]ValidationTableSpec,
		0,
		len(prepared.gate.ValidationTables),
	)
	primaryKeyEqualityProofs := prepared.validationPrimaryKeyEqualityProofs
	if prepared.incremental != nil {
		primaryKeyEqualityProofs =
			prepared.incremental.validationPrimaryKeyEqualityProofs
		if primaryKeyEqualityProofs == nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"prepared Stage 4 incremental primary-key equality proof inventory is unavailable",
				),
			)
		}
	}
	for _, table := range prepared.gate.ValidationTables {
		if _, ok := plans[stage4RichTableKey{
			schema: table.Schema,
			table:  table.Name,
		}]; !ok {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"Stage 4 validation projection contains table (%q, %q) outside the transfer plan",
					table.Schema,
					table.Name,
				),
			)
		}
		var primaryKeyEqualityProof string
		if prepared.mode == "upsert" &&
			primaryKeyEqualityProofs != nil {
			primaryKeyEqualityProof =
				primaryKeyEqualityProofs[stage4RichTableKey{
					schema: table.Schema,
					table:  table.Name,
				}]
			if !validValidationEqualityProofDigest(
				primaryKeyEqualityProof,
			) {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"prepared Stage 4 primary-key equality proof for validation table (%q, %q) is missing or invalid",
						table.Schema,
						table.Name,
					),
				)
			}
		}
		var strictSourceRows *int64
		if prepared.strictSourceRows != nil {
			count, ok := prepared.strictSourceRows[stage4RichTableKey{
				schema: table.Schema,
				table:  table.Name,
			}]
			if !ok || count < 0 {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"prepared Stage 4 strict-snapshot count for validation table (%q, %q) is missing or invalid",
						table.Schema,
						table.Name,
					),
				)
			}
			value := count
			strictSourceRows = &value
		}
		reconciliationStrict := false
		if prepared.deletes != nil {
			key := stage4RichTableKey{
				schema: table.Schema,
				table:  table.Name,
			}
			strict, ok := prepared.deleteReconciliationStrict[key]
			if !ok {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"prepared Stage 4 delete-reconciliation outcome for validation table (%q, %q) is missing",
						table.Schema,
						table.Name,
					),
				)
			}
			reconciliationStrict = strict
		}
		specs = append(specs, ValidationTableSpec{
			Table:                   table,
			Projection:              adapterColumnNames(table),
			StrictSourceRows:        strictSourceRows,
			ReconciliationStrict:    reconciliationStrict,
			PrimaryKeyEqualityProof: primaryKeyEqualityProof,
		})
	}
	return specs, nil
}

func stage4ValidationConcurrency(tableCount int) int {
	if tableCount <= 1 {
		return 1
	}
	if tableCount > 8 {
		return 8
	}
	return tableCount
}
