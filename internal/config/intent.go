package config

import "slices"

// SettingProvenance reports whether a parsed migration value was explicitly
// requested or generated from defaults. YAML field names are used so status
// and debug surfaces can match the public configuration directly.
func (migration Migration) SettingProvenance(
	field string,
) (SettingProvenance, bool) {
	if !isTransferOrTuningField(field) {
		return "", false
	}
	if migration.fieldWasSet(field) {
		return ProvenanceRequested, true
	}
	return ProvenanceDerived, true
}

func (migration Migration) fieldWasSet(field string) bool {
	if migration.parsed {
		if _, exists := migration.explicitFields[field]; exists {
			return true
		}
		if migration.parsedBaseline == nil {
			return false
		}
		return !sameMigrationField(
			migration,
			*migration.parsedBaseline,
			field,
		)
	}
	return migration.programmaticFieldSet(field)
}

func (migration *Migration) captureParsedBaseline() {
	baseline := *migration
	baseline.IncludeTables = cloneStringSlice(migration.IncludeTables)
	baseline.ExcludeTables = cloneStringSlice(migration.ExcludeTables)
	baseline.DateUpdatedColumns = cloneStringSlice(
		migration.DateUpdatedColumns,
	)
	baseline.Preflight.SkipChecks = cloneStringSlice(
		migration.Preflight.SkipChecks,
	)
	if migration.SchemaContract != nil {
		contract := *migration.SchemaContract
		baseline.SchemaContract = &contract
	}
	baseline.explicitFields = nil
	baseline.parsedBaseline = nil
	migration.parsedBaseline = &baseline
}

func sameMigrationField(left, right Migration, field string) bool {
	switch field {
	case "target_mode":
		return left.TargetMode == right.TargetMode
	case "include_tables":
		return slices.Equal(left.IncludeTables, right.IncludeTables)
	case "exclude_tables":
		return slices.Equal(left.ExcludeTables, right.ExcludeTables)
	case "date_updated_columns":
		return slices.Equal(
			left.DateUpdatedColumns,
			right.DateUpdatedColumns,
		)
	case "connection_limit":
		return left.ConnectionLimit == right.ConnectionLimit
	case "workers":
		return left.Workers == right.Workers
	case "chunk_size":
		return left.ChunkSize == right.ChunkSize
	case "partitions":
		return left.Partitions == right.Partitions
	case "large_table_threshold":
		return left.LargeTableThreshold == right.LargeTableThreshold
	case "reader_parallelism":
		return left.ReaderParallelism == right.ReaderParallelism
	case "writer_parallelism":
		return left.WriterParallelism == right.WriterParallelism
	case "read_ahead":
		return left.ReadAhead == right.ReadAhead
	case "upsert_merge_size":
		return left.UpsertMergeSize == right.UpsertMergeSize
	case "memory_ceiling_bytes":
		return left.MemoryCeilingBytes == right.MemoryCeilingBytes
	case "checkpoint_frequency":
		return left.CheckpointFrequency == right.CheckpointFrequency
	case "max_retries":
		return left.MaxRetries == right.MaxRetries
	case "strict_consistency":
		return left.StrictConsistency == right.StrictConsistency
	case "strict_consistency_scope":
		return left.StrictConsistencyScope == right.StrictConsistencyScope
	case "fail_on_schema_drift":
		return left.FailOnSchemaDrift == right.FailOnSchemaDrift
	case "schema_contract":
		return sameSchemaContract(
			left.SchemaContract,
			right.SchemaContract,
		)
	case "validation.mode":
		return left.Validation.Mode == right.Validation.Mode
	case "validation.fail_on_mismatch":
		return left.Validation.FailOnMismatch ==
			right.Validation.FailOnMismatch
	case "validation.fail_on_timeout":
		return left.Validation.FailOnTimeout ==
			right.Validation.FailOnTimeout
	case "validation.fail_on_estimate_mismatch":
		return left.Validation.FailOnEstimateMismatch ==
			right.Validation.FailOnEstimateMismatch
	case "preflight.skip_checks":
		return slices.Equal(
			left.Preflight.SkipChecks,
			right.Preflight.SkipChecks,
		)
	case "deletes.mode":
		return left.Deletes.Mode == right.Deletes.Mode
	case "deletes.target_behavior":
		return left.Deletes.TargetBehavior ==
			right.Deletes.TargetBehavior
	case "deletes.reconcile":
		return left.Deletes.Reconcile == right.Deletes.Reconcile
	case "deletes.reconcile.schedule":
		return left.Deletes.Reconcile.Schedule ==
			right.Deletes.Reconcile.Schedule
	case "deletes.reconcile.interval":
		return left.Deletes.Reconcile.Interval ==
			right.Deletes.Reconcile.Interval
	case "deletes.reconcile.batch_size":
		return left.Deletes.Reconcile.BatchSize ==
			right.Deletes.Reconcile.BatchSize
	case "deletes.reconcile.require_primary_key":
		return left.Deletes.Reconcile.RequirePrimaryKey ==
			right.Deletes.Reconcile.RequirePrimaryKey
	case "history_retention_days":
		return left.HistoryRetentionDays == right.HistoryRetentionDays
	case "tuning":
		return left.Tuning == right.Tuning
	case "runtime_tuning":
		return left.RuntimeTuning == right.RuntimeTuning
	case "runtime_tuning_interval":
		return left.RuntimeTuningInterval ==
			right.RuntimeTuningInterval
	case "allow_partial":
		return left.AllowPartial == right.AllowPartial
	default:
		return true
	}
}

func sameSchemaContract(left, right *SchemaContract) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

func (migration *Migration) markFieldSet(field string) {
	if migration.explicitFields == nil {
		migration.explicitFields = make(map[string]struct{})
	}
	migration.explicitFields[field] = struct{}{}
}

func isTransferOrTuningField(field string) bool {
	switch field {
	case "target_mode",
		"include_tables",
		"exclude_tables",
		"date_updated_columns",
		"connection_limit",
		"workers",
		"chunk_size",
		"partitions",
		"large_table_threshold",
		"reader_parallelism",
		"writer_parallelism",
		"read_ahead",
		"upsert_merge_size",
		"memory_ceiling_bytes",
		"checkpoint_frequency",
		"max_retries",
		"strict_consistency",
		"strict_consistency_scope",
		"history_retention_days",
		"tuning",
		"runtime_tuning",
		"runtime_tuning_interval":
		return true
	default:
		return false
	}
}

func (migration Migration) programmaticFieldSet(field string) bool {
	switch field {
	case "target_mode":
		return migration.TargetMode != ""
	case "include_tables":
		return migration.IncludeTables != nil
	case "exclude_tables":
		return migration.ExcludeTables != nil
	case "date_updated_columns":
		return migration.DateUpdatedColumns != nil
	case "connection_limit":
		return migration.ConnectionLimit != 0
	case "workers":
		return migration.Workers != 0
	case "chunk_size":
		return migration.ChunkSize != 0
	case "partitions":
		return migration.Partitions != 0
	case "large_table_threshold":
		return migration.LargeTableThreshold != 0
	case "reader_parallelism":
		return migration.ReaderParallelism != 0
	case "writer_parallelism":
		return migration.WriterParallelism != 0
	case "read_ahead":
		return migration.ReadAhead != 0
	case "upsert_merge_size":
		return migration.UpsertMergeSize != 0
	case "memory_ceiling_bytes":
		return migration.MemoryCeilingBytes != 0
	case "checkpoint_frequency":
		return migration.CheckpointFrequency != 0
	case "max_retries":
		return migration.MaxRetries != 0
	case "strict_consistency":
		return migration.StrictConsistency
	case "strict_consistency_scope":
		return migration.StrictConsistencyScope != ""
	case "fail_on_schema_drift":
		return migration.FailOnSchemaDrift
	case "schema_contract":
		return migration.SchemaContract != nil ||
			migration.SchemaEvolution != nil
	case "validation.mode":
		return migration.Validation.Mode != ""
	case "validation.fail_on_mismatch":
		return migration.Validation.FailOnMismatch
	case "validation.fail_on_timeout":
		return migration.Validation.FailOnTimeout
	case "validation.fail_on_estimate_mismatch":
		return migration.Validation.FailOnEstimateMismatch
	case "preflight.skip_checks":
		return migration.Preflight.SkipChecks != nil
	case "deletes.mode":
		return migration.Deletes.Mode != ""
	case "deletes.target_behavior":
		return migration.Deletes.TargetBehavior != ""
	case "deletes.reconcile":
		return migration.Deletes.Reconcile != (DeleteReconcilePolicy{})
	case "deletes.reconcile.schedule":
		return migration.Deletes.Reconcile.Schedule != ""
	case "deletes.reconcile.interval":
		return migration.Deletes.Reconcile.Interval != 0
	case "deletes.reconcile.batch_size":
		return migration.Deletes.Reconcile.BatchSize != 0
	case "deletes.reconcile.require_primary_key":
		return migration.Deletes.Reconcile.RequirePrimaryKey
	case "history_retention_days":
		return migration.HistoryRetentionDays != 0
	case "tuning":
		return migration.Tuning != ""
	case "runtime_tuning":
		return migration.RuntimeTuning
	case "runtime_tuning_interval":
		return migration.RuntimeTuningInterval != 0
	case "allow_partial":
		return migration.AllowPartial
	default:
		return false
	}
}
