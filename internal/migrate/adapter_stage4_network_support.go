package migrate

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

// Stage 4 network plumbing: task identity, mode admission, resource sizing,
// wave construction, and dependency ordering.

func stage4AdapterDurableTableTaskType(value string) bool {
	switch value {
	case stage4AdapterNetworkTaskType,
		"table-copy",
		"analytical-table-copy":
		return true
	default:
		return false
	}
}

func (
	execution *stage4AdapterNetworkExecution,
) reconstructCompletedTableWork(
	planIndex int,
	inventory stage4WorkInventory,
) (stage4AdapterWork, error) {
	base := cloneStage4AdapterNetworkWork(
		execution.prepared.work[planIndex],
	)
	table := cloneStage4RichTable(
		execution.prepared.plans[planIndex].source,
	)
	persisted := inventory.ranges[base.task]
	if len(persisted) == 0 ||
		uint64(len(persisted)) > maximumRuntimeTuningRanges {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s has no bounded durable ranges",
				base.task.Table,
			),
		)
	}
	requestedPartitions := execution.partitions
	if requestedPartitions == 0 {
		requestedPartitions = config.DefaultPartitions
	}
	if requestedPartitions < 1 ||
		uint64(requestedPartitions) > maximumRuntimeTuningRanges {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s has an invalid partition count",
				base.task.Table,
			),
		)
	}

	byID := make(map[string]state.RangeState, len(persisted))
	for _, workRange := range persisted {
		if _, duplicate := byID[workRange.ID]; duplicate {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s duplicates durable range %q",
					base.task.Table,
					workRange.ID,
				),
			)
		}
		byID[workRange.ID] = workRange
	}

	keyColumns, err := adapterPaginationPrimaryKey(
		execution.source.Engine(),
		table.Schema,
		table,
	)
	if err != nil {
		return stage4AdapterWork{}, err
	}
	keys := make([]KeySpec, len(keyColumns))
	evidence := make(
		[]adapterPaginationKeyEvidence,
		len(keyColumns),
	)
	for index, column := range keyColumns {
		keys[index] = KeySpec{
			Name: column.Name,
			Kind: adapterPaginationKeyKind(
				execution.source.Engine(),
				column,
			),
		}
		evidence[index] = adapterPaginationKeyEvidence{
			Name:     column.Name,
			Type:     column.Type,
			Nullable: column.Nullable,
			Position: column.PrimaryKeyPosition,
			Declaration: cloneAdapterPaginationDeclaration(
				column.DeclaredType,
			),
		}
	}
	strategy := adapterPaginationStrategy(
		execution.source.Engine(),
		table,
		keyColumns,
	)
	plannedRanges := make(
		[]PaginationRange,
		len(persisted),
	)
	for index := range plannedRanges {
		rangeID := fmt.Sprintf("range/%d", index)
		workRange, ok := byID[rangeID]
		if !ok {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s lacks exact durable range %q",
					base.task.Table,
					rangeID,
				),
			)
		}
		delete(byID, rangeID)
		lower, tupleErr := stage4AdapterKeyTupleFromState(
			workRange.Lower,
		)
		if tupleErr != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s range %q lower bound: %w",
					base.task.Table,
					rangeID,
					tupleErr,
				),
			)
		}
		upper, tupleErr := stage4AdapterKeyTupleFromState(
			workRange.Upper,
		)
		if tupleErr != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 completed table %s range %q upper bound: %w",
					base.task.Table,
					rangeID,
					tupleErr,
				),
			)
		}
		plannedRanges[index] = PaginationRange{
			ID:       index,
			Lower:    lower,
			Upper:    upper,
			FirstRow: workRange.FirstRow,
			LastRow:  workRange.LastRow,
			Empty: len(persisted) == 1 &&
				lower == nil && upper == nil &&
				(strategy == PaginationRowNumber &&
					workRange.FirstRow == 1 &&
					workRange.LastRow == 0 ||
					strategy != PaginationRowNumber &&
						workRange.FirstRow == 0 &&
						workRange.LastRow == 0),
		}
	}
	if len(byID) != 0 {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 completed table %s contains unexpected durable ranges",
				base.task.Table,
			),
		)
	}

	pagination := PaginationPlan{
		Strategy: strategy,
		Keys:     keys,
		Ranges:   plannedRanges,
	}
	pagination.TopologyHash, err = adapterPaginationTopologyHash(
		execution.source.Engine(),
		table,
		requestedPartitions,
		evidence,
		pagination,
	)
	if err != nil {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reconstruct Stage 4 completed pagination for %s: %w",
				base.task.Table,
				err,
			),
		)
	}
	if err := validateStage4AdapterPagination(
		execution.source.Engine(),
		table,
		requestedPartitions,
		pagination,
	); err != nil {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"validate Stage 4 completed pagination for %s: %w",
				base.task.Table,
				err,
			),
		)
	}
	base.topology, err = stage4AdapterNetworkTopology(
		base.topology,
		requestedPartitions,
		pagination,
	)
	if err != nil {
		return stage4AdapterWork{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"reconstruct Stage 4 completed topology for %s: %w",
				base.task.Table,
				err,
			),
		)
	}
	base.pagination = cloneStage4AdapterPagination(pagination)
	base.ranges = make([]state.RangeState, len(plannedRanges))
	for index, planned := range plannedRanges {
		base.ranges[index], err = stage4AdapterStateRange(
			planned,
			base.topology,
		)
		if err != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"reconstruct Stage 4 completed range %d for %s: %w",
					index,
					base.task.Table,
					err,
				),
			)
		}
	}
	if execution.finalizeWork != nil {
		base, err = execution.finalizeWork(base)
		if err != nil {
			return stage4AdapterWork{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"restore Stage 4 completed strict topology for %s: %w",
					base.task.Table,
					err,
				),
			)
		}
	}
	return base, nil
}

func stage4AdapterKeyTupleFromState(
	tuple state.TypedTuple,
) (*KeyTuple, error) {
	if len(tuple) == 0 {
		return nil, nil
	}
	if err := tuple.Validate(false); err != nil {
		return nil, err
	}
	result := make(KeyTuple, len(tuple))
	for index, value := range tuple {
		switch value.Kind {
		case state.ValueInt64:
			result[index] = KeyValue{
				Kind:    KeyInteger,
				Encoded: value.Encoded,
			}
		case state.ValueText:
			result[index] = KeyValue{
				Kind:    KeyText,
				Encoded: value.Encoded,
			}
		case state.ValueBytes:
			result[index] = KeyValue{
				Kind:    KeyBytes,
				Encoded: value.Encoded,
			}
		default:
			return nil, fmt.Errorf(
				"typed key %d has unsupported kind %q",
				index,
				value.Kind,
			)
		}
	}
	return &result, nil
}

func requireStage4AdapterNetworkMode(
	cfg config.Config,
	prepared stage4AdapterPrepared,
	options stage4AdapterNetworkAdmissionOptions,
) error {
	if cfg.Migration.TargetMode != prepared.mode {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network runner target mode differs from prepared work",
			),
		)
	}
	switch prepared.mode {
	case "upsert", "drop_recreate":
	default:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network runner received unsupported target mode %q",
				prepared.mode,
			),
		)
	}
	if cfg.Migration.StrictConsistency &&
		!options.strictSnapshotComposition {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 strict consistency requires a composed network snapshot runner",
			),
		)
	}
	if !cfg.Migration.StrictConsistency &&
		options.strictSnapshotComposition {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 strict network snapshot composition was enabled without strict consistency",
			),
		)
	}
	if len(cfg.Migration.DateUpdatedColumns) != 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 incremental windows require a composed network incremental runner",
			),
		)
	}
	deleteEnabled := false
	switch cfg.Migration.Deletes.Mode {
	case "", config.DeleteModeOff:
	case config.DeleteModeReconcile:
		deleteEnabled = true
	default:
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network runner received unsupported delete mode %q",
				cfg.Migration.Deletes.Mode,
			),
		)
	}
	if deleteEnabled !=
		options.deleteReconciliationComposition {
		class := ErrorClassPolicy
		message := "Stage 4 delete reconciliation requires a composed network delete runner"
		if !deleteEnabled {
			class = ErrorClassState
			message = "Stage 4 network delete composition was enabled without delete reconciliation"
		}
		return NewTransferError(
			class,
			fmt.Errorf("%s", message),
		)
	}
	if deleteEnabled != (prepared.deletes != nil) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 prepared delete reconciliation differs from network admission",
			),
		)
	}
	if cfg.Migration.MaxRetries < 0 {
		return NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf("Stage 4 max retries must not be negative"),
		)
	}
	return nil
}

func canOpenAdapterStableNetworkTableSource(source sourceAdapter) bool {
	if isNilInterface(source) {
		return false
	}
	if _, ok := source.(adapterStableNetworkTableSessionOpener); ok {
		return true
	}
	switch source.(type) {
	case *relationalSourceAdapter, *sqliteSourceAdapter:
		return true
	default:
		return false
	}
}

func validateStage4AdapterDeferredNetworkInventory(
	prepared stage4AdapterPrepared,
) error {
	if prepared.network != nil ||
		len(prepared.work) != len(prepared.plans) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 deferred network inventory is inconsistent",
			),
		)
	}
	for index, item := range prepared.work {
		plan := prepared.plans[index]
		if item.task.Schema != plan.source.Schema ||
			item.task.Table != plan.source.Name ||
			item.task.Type != stage4AdapterNetworkTaskType ||
			item.strategy != stage4AdapterCopyStrategy ||
			!validNetworkFactToken(item.topology) ||
			len(item.ranges) != 0 ||
			!reflect.DeepEqual(item.pagination, PaginationPlan{}) {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 deferred network work differs from table plan %d",
					index,
				),
			)
		}
	}
	return nil
}

func cloneStage4AdapterNetworkPrepared(
	value stage4AdapterPrepared,
) stage4AdapterPrepared {
	result := value
	result.names = append([]string(nil), value.names...)
	result.targetTables = make([]schema.Table, len(value.targetTables))
	for index := range value.targetTables {
		result.targetTables[index] =
			cloneStage4RichTable(value.targetTables[index])
	}
	result.plans = make([]adapterTablePlan, len(value.plans))
	for index := range value.plans {
		result.plans[index] =
			cloneStage4AdapterNetworkTablePlan(value.plans[index])
	}
	result.work = make([]stage4AdapterWork, len(value.work))
	for index := range value.work {
		result.work[index] =
			cloneStage4AdapterNetworkWork(value.work[index])
	}
	result.sourceCatalog = make(
		map[stage4RichTableKey]schema.Table,
		len(value.sourceCatalog),
	)
	for key, table := range value.sourceCatalog {
		result.sourceCatalog[key] = cloneStage4RichTable(table)
	}
	if value.validationPrimaryKeyEqualityProofs != nil {
		result.validationPrimaryKeyEqualityProofs = make(
			map[stage4RichTableKey]string,
			len(value.validationPrimaryKeyEqualityProofs),
		)
		for key, proof := range value.validationPrimaryKeyEqualityProofs {
			result.validationPrimaryKeyEqualityProofs[key] = proof
		}
	}
	return result
}

func stage4AdapterNetworkRelationalEngine(engine string) bool {
	switch engine {
	case "postgres", "mysql", "mariadb", "mssql", "sqlite":
		return true
	default:
		return false
	}
}

func stage4AdapterNetworkReplayMode(mode string) NetworkReplayMode {
	if mode == "drop_recreate" {
		return NetworkReplayDuplicateSafeInsertOnly
	}
	return NetworkReplayIdempotentUpsert
}

func stage4AdapterNetworkResources(
	ctx context.Context,
	cfg config.Config,
	sourceEngine string,
	targetEngine string,
	override *config.EffectiveTransferPlan,
) (config.EffectiveTransferPlan, error) {
	var (
		resources config.EffectiveTransferPlan
		err       error
	)
	if override == nil {
		resources, err = config.ResolveSystemEffectiveTransferPlan(
			ctx,
			cfg.Migration,
			config.TransferPlanOptions{},
		)
		if err != nil {
			return resources, fmt.Errorf(
				"resolve Stage 4 network resources: %w",
				err,
			)
		}
	} else {
		resources = *override
	}
	if resources.TargetMode != cfg.Migration.TargetMode {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 network resources target mode differs from migration target mode",
			),
		)
	}
	switch resources.TargetMode {
	case "upsert", "drop_recreate":
	default:
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network resources received unsupported target mode %q",
				resources.TargetMode,
			),
		)
	}
	if sourceEngine == "sqlite" && resources.Readers.Value != 1 {
		resources.Readers = config.EffectiveInt{
			Value:      1,
			Provenance: config.ProvenanceSafetyClamped,
		}
	}
	if targetEngine == "sqlite" && resources.Writers.Value != 1 {
		resources.Writers = config.EffectiveInt{
			Value:      1,
			Provenance: config.ProvenanceSafetyClamped,
		}
	}
	requiredWorkers := resources.Readers.Value + resources.Writers.Value
	if resources.Workers.Value < requiredWorkers ||
		resources.ConnectionLimit.Value < requiredWorkers {
		return config.EffectiveTransferPlan{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 network resources cannot safely schedule the admitted readers and writers",
			),
		)
	}
	return resources, nil
}

func buildStage4AdapterNetworkWaves(
	plan NetworkTransferPlan,
	ranges []stage4AdapterNetworkRange,
	tableCount int,
) ([]stage4AdapterNetworkWave, error) {
	if tableCount < 0 ||
		len(plan.Ranges) != len(ranges) ||
		(tableCount == 0) != (len(ranges) == 0) {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 dependency-wave inventory is inconsistent",
			),
		)
	}
	if plan.RuntimeTuning != nil {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"Stage 4 dependency waves require a composed migration-wide runtime tuning controller",
			),
		)
	}
	restores := make(
		map[uint64]NetworkRangeRestore,
		len(plan.Restores),
	)
	for _, restore := range plan.Restores {
		if _, duplicate := restores[restore.RangeIndex]; duplicate {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency-wave restore %d is duplicated",
					restore.RangeIndex,
				),
			)
		}
		restores[restore.RangeIndex] = cloneNetworkRestore(restore)
	}

	waves := make([]stage4AdapterNetworkWave, 0, tableCount)
	cursor := 0
	for tableIndex := 0; tableIndex < tableCount; tableIndex++ {
		first := cursor
		for cursor < len(ranges) &&
			ranges[cursor].planIndex == tableIndex {
			if ranges[cursor].globalIndex != uint64(cursor) ||
				plan.Ranges[cursor].RangeIndex != uint64(cursor) {
				return nil, NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 dependency-wave global range order changed",
					),
				)
			}
			cursor++
		}
		if first == cursor {
			return nil, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency wave for table %d has no ranges",
					tableIndex,
				),
			)
		}
		globalRanges := append(
			[]NetworkRangePlan(nil),
			plan.Ranges[first:cursor]...,
		)
		localPlan := plan
		localPlan.Ranges = make(
			[]NetworkRangePlan,
			len(globalRanges),
		)
		localPlan.Restores = make(
			[]NetworkRangeRestore,
			0,
			len(globalRanges),
		)
		for localIndex, globalRange := range globalRanges {
			localRange := globalRange
			localRange.RangeIndex = uint64(localIndex)
			localPlan.Ranges[localIndex] = localRange
			if restore, exists := restores[globalRange.RangeIndex]; exists {
				delete(restores, globalRange.RangeIndex)
				for issuedIndex := range restore.Issued {
					if restore.Issued[issuedIndex].RangeIndex !=
						globalRange.RangeIndex {
						return nil, NewTransferError(
							ErrorClassState,
							fmt.Errorf(
								"Stage 4 dependency-wave issued restore changed global range identity",
							),
						)
					}
					restore.Issued[issuedIndex].RangeIndex =
						uint64(localIndex)
				}
				restore.RangeIndex = uint64(localIndex)
				localPlan.Restores = append(
					localPlan.Restores,
					restore,
				)
			}
		}
		waves = append(waves, stage4AdapterNetworkWave{
			plan:   localPlan,
			global: globalRanges,
		})
	}
	if cursor != len(ranges) || len(restores) != 0 {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"Stage 4 dependency waves do not cover the global range inventory",
			),
		)
	}
	return waves, nil
}

func (wave stage4AdapterNetworkWave) callbacks(
	base NetworkTransferCallbacks,
) (NetworkTransferCallbacks, error) {
	if len(wave.global) == 0 ||
		len(wave.plan.Ranges) != len(wave.global) {
		return NetworkTransferCallbacks{}, NewTransferError(
			ErrorClassState,
			fmt.Errorf("Stage 4 dependency wave is empty or incomplete"),
		)
	}
	globalRange := func(
		local NetworkRangePlan,
	) (NetworkRangePlan, error) {
		if local.RangeIndex >= uint64(len(wave.global)) {
			return NetworkRangePlan{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency wave references an unknown local range",
				),
			)
		}
		expected := wave.plan.Ranges[local.RangeIndex]
		if !reflect.DeepEqual(local, expected) {
			return NetworkRangePlan{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 dependency-wave range differs from immutable admission",
				),
			)
		}
		return wave.global[local.RangeIndex], nil
	}
	return NetworkTransferCallbacks{
		ReadPage: func(
			ctx context.Context,
			request NetworkReadRequest,
		) (NetworkReadPage, error) {
			global, err := globalRange(request.Range)
			if err != nil {
				return NetworkReadPage{}, err
			}
			localIndex := request.Range.RangeIndex
			request.Range = global
			if request.ReplayExpected != nil {
				if request.ReplayExpected.RangeIndex != localIndex {
					return NetworkReadPage{}, NewTransferError(
						ErrorClassState,
						fmt.Errorf(
							"Stage 4 dependency-wave replay identity changed",
						),
					)
				}
				replay := *request.ReplayExpected
				replay.RangeIndex = global.RangeIndex
				request.ReplayExpected = &replay
			}
			return base.ReadPage(ctx, request)
		},
		WritePage: func(
			ctx context.Context,
			request NetworkWriteRequest,
		) (WriteReceipt, error) {
			global, err := globalRange(request.Range)
			if err != nil {
				return networkStateFailedReceipt(request), err
			}
			request.Range = global
			return base.WritePage(ctx, request)
		},
		RecordIssued: func(
			ctx context.Context,
			issued NetworkIssuedChunk,
		) error {
			if issued.RangeIndex >= uint64(len(wave.global)) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 dependency wave issued an unknown local range",
					),
				)
			}
			issued.RangeIndex =
				wave.global[issued.RangeIndex].RangeIndex
			return base.RecordIssued(ctx, issued)
		},
		Checkpoint: func(
			ctx context.Context,
			checkpoint NetworkRangeCheckpoint,
		) error {
			if checkpoint.RangeIndex >= uint64(len(wave.global)) {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"Stage 4 dependency wave checkpointed an unknown local range",
					),
				)
			}
			globalIndex :=
				wave.global[checkpoint.RangeIndex].RangeIndex
			checkpoint.RangeIndex = globalIndex
			checkpoint.Frontier.RangeID =
				fmt.Sprintf("range/%d", globalIndex)
			return base.Checkpoint(ctx, checkpoint)
		},
	}, nil
}

func validateStage4AdapterNetworkDependencyOrder(
	plans []adapterTablePlan,
) error {
	selected := make(
		map[stage4RichTableKey]int,
		len(plans),
	)
	for index, plan := range plans {
		key := stage4RichTableKey{
			schema: plan.target.Schema,
			table:  plan.target.Name,
		}
		if _, duplicate := selected[key]; duplicate {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"Stage 4 network target table (%q, %q) is duplicated",
					key.schema,
					key.table,
				),
			)
		}
		selected[key] = index
	}
	for childIndex, plan := range plans {
		for _, foreignKey := range plan.target.ForeignKeys {
			referencedSchema := foreignKey.ReferencedSchema
			if referencedSchema == "" {
				referencedSchema = plan.target.Schema
			}
			referenced := stage4RichTableKey{
				schema: referencedSchema,
				table:  foreignKey.ReferencedTable,
			}
			parentIndex, inScope, err :=
				stage4AdapterNetworkSelectedTableIndex(
					selected,
					referenced,
				)
			if err != nil {
				return err
			}
			if inScope && parentIndex >= childIndex {
				return NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 network table %s is not ordered after foreign-key parent %s",
						plan.target.Name,
						foreignKey.ReferencedTable,
					),
				)
			}
		}
	}
	return nil
}

func stage4AdapterNetworkSelectedTableIndex(
	selected map[stage4RichTableKey]int,
	referenced stage4RichTableKey,
) (int, bool, error) {
	if index, exists := selected[referenced]; exists {
		return index, true, nil
	}
	matched := -1
	for key, index := range selected {
		if strings.EqualFold(key.schema, referenced.schema) &&
			strings.EqualFold(key.table, referenced.table) {
			if matched >= 0 {
				return 0, false, NewTransferError(
					ErrorClassPolicy,
					fmt.Errorf(
						"Stage 4 network foreign-key identity (%q, %q) is ambiguous under target identifier folding",
						referenced.schema,
						referenced.table,
					),
				)
			}
			matched = index
		}
	}
	if matched >= 0 {
		return matched, true, nil
	}
	return 0, false, nil
}
