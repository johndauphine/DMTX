package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// Stage 4 target schema evolution: preparing, applying, and preflighting the
// target shape a run will write into.

type stage4AdapterTargetSchemaEvolution struct {
	capability adapterTargetSchemaEvolutionCapability
	authority  Stage4TargetShapeAuthority
	pending    state.SchemaSnapshot
	request    TargetSchemaEvolutionRequest
	plan       TargetSchemaEvolutionPlan
	prior      []schema.Table
	current    []schema.Table
}

func prepareStage4AdapterTargetSchema(
	ctx context.Context,
	run Stage4RunContext,
	gateOptions Stage4SchemaGateOptions,
	source sourceAdapter,
	target targetAdapter,
	mode string,
	gate Stage4SchemaGateResult,
) (*stage4AdapterTargetSchemaEvolution, error) {
	// Drop/recreate owns its complete target lifecycle through the target
	// adapter's ordinary deterministic planner. The in-place catalog
	// evolution protocol is intentionally upsert-only.
	if mode != "upsert" {
		return nil, nil
	}
	requiresEvolution, decision := stage4AdapterTargetEvolutionDecision(
		mode,
		gate.Plan.Decisions,
	)
	capability, ok := target.(adapterTargetSchemaEvolutionCapability)
	if !ok || stage4AdapterTargetSchemaEvolutionCapabilityIsNil(capability) {
		if !requiresEvolution {
			return nil, nil
		}
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 upsert schema action %q for table %s requires a composed target-catalog evolution executor seam",
				decision.Action,
				decision.Object.Table,
			),
		)
	}
	dialect := capability.TargetSchemaEvolutionDialect()
	if dialect == "" {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target-catalog evolution capability for %s returned an empty target dialect",
				target.Engine(),
			),
		)
	}
	createPlanner := capability.TargetSchemaEvolutionCreatePlanner()
	if targetSchemaEvolutionCreatePlannerIsNil(createPlanner) {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 target-catalog evolution capability for %s returned a nil create planner",
				target.Engine(),
			),
		)
	}
	authority, err := PrepareStage4TargetShapeAuthority(
		run,
		gate,
		gateOptions,
		Stage4TargetShapeSeed{},
	)
	if errors.Is(err, ErrStage4TargetShapeSeedRequired) {
		catalog, readErr :=
			capability.ReadTargetSchemaEvolutionCatalog(ctx)
		if readErr != nil {
			return nil, fmt.Errorf(
				"read exact Stage 4 target catalog for shape authority: %w",
				readErr,
			)
		}
		seed, seedErr := NewStage4TargetShapeSeed(catalog)
		if seedErr != nil {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"freeze exact Stage 4 target catalog seed: %w",
					seedErr,
				),
			)
		}
		authority, err = PrepareStage4TargetShapeAuthority(
			run,
			gate,
			gateOptions,
			seed,
		)
	}
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"prepare Stage 4 target-shape authority: %w",
				err,
			),
		)
	}
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		source.Engine(),
		target,
		mode,
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"project Stage 4 target-catalog evolution: %w",
				err,
			),
		)
	}
	pending, err := BindStage4TargetShapeProjection(
		authority,
		projection,
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"bind Stage 4 target-shape projection: %w",
				err,
			),
		)
	}
	request, err := NewTargetSchemaEvolutionRequest(
		dialect,
		projection,
		createPlanner,
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"authorize Stage 4 target-catalog evolution: %w",
				err,
			),
		)
	}
	plan, err := capability.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil {
		return nil, fmt.Errorf(
			"preflight Stage 4 target-catalog evolution: %w",
			err,
		)
	}
	if plan.Digest() == "" || plan.Target() != dialect {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"preflight Stage 4 target-catalog evolution returned invalid plan authority",
			),
		)
	}
	return &stage4AdapterTargetSchemaEvolution{
		capability: capability,
		authority:  authority,
		pending:    pending,
		request:    request,
		plan:       plan,
		prior:      projection.PriorTables(),
		current:    projection.CurrentTables(),
	}, nil
}

func applyStage4AdapterTargetSchema(
	ctx context.Context,
	observer TableObserver,
	run Stage4RunContext,
	gate Stage4SchemaGateResult,
	evolution *stage4AdapterTargetSchemaEvolution,
) error {
	if err := stageStage4SchemaGateSnapshots(
		ctx,
		run,
		gate,
		evolution,
	); err != nil {
		return err
	}
	if evolution == nil || evolution.plan.Complete() {
		return nil
	}
	if _, err := protectAdapterTargetMutationOnce(
		ctx,
		observer,
		"apply Stage 4 target schema evolution",
		func() error {
			return evolution.capability.ApplyTargetSchemaEvolutionPlan(
				ctx,
				evolution.plan,
			)
		},
	); err != nil {
		return fmt.Errorf(
			"apply Stage 4 target-catalog evolution: %w",
			err,
		)
	}
	verified, err := evolution.capability.PreflightTargetSchemaEvolution(
		ctx,
		evolution.request,
	)
	if err != nil {
		return fmt.Errorf(
			"reverify Stage 4 target-catalog evolution: %w",
			err,
		)
	}
	if !verified.Complete() {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reverify Stage 4 target-catalog evolution: target catalog remains incomplete after apply",
			),
		)
	}
	if verified.Digest() != evolution.plan.Digest() {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reverify Stage 4 target-catalog evolution: plan digest changed from %s to %s",
				evolution.plan.Digest(),
				verified.Digest(),
			),
		)
	}
	return nil
}

func stage4AdapterExistingEvolutionTargetTables(
	evolution *stage4AdapterTargetSchemaEvolution,
	desired []schema.Table,
) ([]schema.Table, error) {
	if evolution == nil {
		return cloneTargetSchemaEvolutionTables(desired), nil
	}
	// PreflightTargetSchemaEvolution has already authenticated the exact live
	// catalog prefix. A process can fail after target DDL commits but before the
	// immediate post-apply reverify/state completion; on resume that prefix is
	// final, not the original prior. Use it for retained-table preflight so an
	// already-committed immutable evolution is not rejected as a shape drift.
	// The same rule handles a target dialect whose durable plan can expose an
	// authenticated nonzero partial prefix.
	existingTables := evolution.prior
	if evolution.plan.valid() {
		existingTables = evolution.plan.states[evolution.plan.AppliedPrefix()]
	}
	prior := make(
		map[targetSchemaEvolutionTableKey]schema.Table,
		len(existingTables),
	)
	for _, table := range existingTables {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, duplicate := prior[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 target-shape authority contains duplicate prior table %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		prior[key] = cloneStage4RichTable(table)
	}
	result := make([]schema.Table, 0, len(desired))
	for _, table := range desired {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		existing, found := prior[key]
		if !found {
			continue
		}
		result = append(result, existing)
	}
	sortTargetSchemaEvolutionTables(result)
	return result, nil
}

func stage4AdapterCurrentEvolutionTargetTables(
	evolution *stage4AdapterTargetSchemaEvolution,
	transfer []schema.Table,
) ([]schema.Table, error) {
	if evolution == nil {
		return cloneTargetSchemaEvolutionTables(transfer), nil
	}
	current := make(
		map[targetSchemaEvolutionTableKey]schema.Table,
		len(evolution.current),
	)
	for _, table := range evolution.current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, duplicate := current[key]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 target-shape projection contains duplicate current table %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		current[key] = cloneStage4RichTable(table)
	}
	result := make([]schema.Table, 0, len(transfer))
	for _, table := range transfer {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		authenticated, found := current[key]
		if !found {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 target-shape projection is missing current transfer table %s.%s",
					key.schema,
					key.table,
				),
			)
		}
		result = append(result, authenticated)
	}
	sortTargetSchemaEvolutionTables(result)
	return result, nil
}

func preflightStage4AdapterDesiredTargetAfterEvolution(
	ctx context.Context,
	target targetAdapter,
	prepared stage4AdapterPrepared,
) error {
	if prepared.evolution == nil {
		return nil
	}
	currentTables, err := stage4AdapterCurrentEvolutionTargetTables(
		prepared.evolution,
		prepared.targetTables,
	)
	if err != nil {
		return err
	}
	if err := target.PreflightTables(
		ctx,
		cloneTargetSchemaEvolutionTables(currentTables),
		prepared.mode,
	); err != nil {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"preflight desired Stage 4 target tables after schema evolution: %w",
				err,
			),
		)
	}
	if prepared.mode == "upsert" {
		if err := preflightStage4NetworkReplayIsolation(
			ctx,
			target,
			cloneTargetSchemaEvolutionTables(
				currentTables,
			),
		); err != nil {
			return fmt.Errorf(
				"preflight desired Stage 4 network replay isolation after schema evolution: %w",
				err,
			)
		}
	}
	return ctx.Err()
}

func stage4AdapterTargetEvolutionDecision(
	mode string,
	decisions []SchemaContractDecision,
) (bool, SchemaContractDecision) {
	if mode != "upsert" {
		return false, SchemaContractDecision{}
	}
	for _, decision := range decisions {
		switch decision.Action {
		case SchemaContractCreateTable,
			SchemaContractAddColumn,
			SchemaContractRelaxNullability,
			SchemaContractWidenType:
			return true, decision
		}
	}
	return false, SchemaContractDecision{}
}

func stage4AdapterTargetSchemaEvolutionCapabilityIsNil(
	capability adapterTargetSchemaEvolutionCapability,
) bool {
	if capability == nil {
		return true
	}
	value := reflect.ValueOf(capability)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func targetSchemaEvolutionCreatePlannerIsNil(
	planner TargetSchemaEvolutionCreatePlanner,
) bool {
	if planner == nil {
		return true
	}
	value := reflect.ValueOf(planner)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
