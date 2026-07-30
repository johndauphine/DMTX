package config

import "time"

// canonicalMigrationIntentFieldOrder is the stable public-field order used by
// configuration hashing and canonical YAML. Deprecated spellings deliberately
// do not appear here: Parse marks their preferred replacements as requested.
var canonicalMigrationIntentFieldOrder = []string{
	"target_mode",
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
	"fail_on_schema_drift",
	"schema_contract",
	"validation.mode",
	"validation.fail_on_mismatch",
	"validation.fail_on_timeout",
	"validation.fail_on_estimate_mismatch",
	"deletes.mode",
	"deletes.target_behavior",
	"deletes.reconcile.schedule",
	"deletes.reconcile.interval",
	"deletes.reconcile.batch_size",
	"deletes.reconcile.require_primary_key",
	"history_retention_days",
	"tuning",
	"runtime_tuning",
	"runtime_tuning_interval",
	"allow_partial",
}

func canonicalMigrationIntent(migration Migration) []string {
	requested := make([]string, 0, len(canonicalMigrationIntentFieldOrder))
	for _, field := range canonicalMigrationIntentFieldOrder {
		if migration.fieldWasSet(field) {
			requested = append(requested, field)
		}
	}
	return requested
}

// MarshalYAML preserves parsed user intent instead of serializing generated
// defaults as if the operator had pinned them. Programmatically constructed
// values retain the historical all-fields representation because they do not
// carry YAML presence evidence.
func (migration Migration) MarshalYAML() (any, error) {
	if !migration.parsed {
		type programmaticMigration Migration
		return programmaticMigration(migration), nil
	}

	var wire canonicalMigrationYAML
	if migration.fieldWasSet("target_mode") {
		wire.TargetMode = pointerTo(migration.TargetMode)
	}
	if migration.fieldWasSet("include_tables") {
		wire.IncludeTables = pointerTo(cloneStringSlice(migration.IncludeTables))
	}
	if migration.fieldWasSet("exclude_tables") {
		wire.ExcludeTables = pointerTo(cloneStringSlice(migration.ExcludeTables))
	}
	if migration.fieldWasSet("date_updated_columns") {
		wire.DateUpdatedColumns = pointerTo(
			cloneStringSlice(migration.DateUpdatedColumns),
		)
	}
	if migration.fieldWasSet("connection_limit") {
		wire.ConnectionLimit = pointerTo(migration.ConnectionLimit)
	}
	if migration.fieldWasSet("workers") {
		wire.Workers = pointerTo(migration.Workers)
	}
	if migration.fieldWasSet("chunk_size") {
		wire.ChunkSize = pointerTo(migration.ChunkSize)
	}
	if migration.fieldWasSet("partitions") {
		wire.Partitions = pointerTo(migration.Partitions)
	}
	if migration.fieldWasSet("large_table_threshold") {
		wire.LargeTableThreshold = pointerTo(migration.LargeTableThreshold)
	}
	if migration.fieldWasSet("reader_parallelism") {
		wire.ReaderParallelism = pointerTo(migration.ReaderParallelism)
	}
	if migration.fieldWasSet("writer_parallelism") {
		wire.WriterParallelism = pointerTo(migration.WriterParallelism)
	}
	if migration.fieldWasSet("read_ahead") {
		wire.ReadAhead = pointerTo(migration.ReadAhead)
	}
	if migration.fieldWasSet("upsert_merge_size") {
		wire.UpsertMergeSize = pointerTo(migration.UpsertMergeSize)
	}
	if migration.fieldWasSet("memory_ceiling_bytes") {
		wire.MemoryCeilingBytes = pointerTo(migration.MemoryCeilingBytes)
	}
	if migration.fieldWasSet("checkpoint_frequency") {
		wire.CheckpointFrequency = pointerTo(migration.CheckpointFrequency)
	}
	if migration.fieldWasSet("max_retries") {
		wire.MaxRetries = pointerTo(migration.MaxRetries)
	}
	if migration.fieldWasSet("strict_consistency") {
		wire.StrictConsistency = pointerTo(migration.StrictConsistency)
	}
	if migration.fieldWasSet("strict_consistency_scope") {
		wire.StrictConsistencyScope = pointerTo(
			migration.StrictConsistencyScope,
		)
	}
	if migration.fieldWasSet("fail_on_schema_drift") {
		wire.FailOnSchemaDrift = pointerTo(migration.FailOnSchemaDrift)
	}
	if migration.fieldWasSet("schema_contract") &&
		migration.SchemaContract != nil {
		contract := *migration.SchemaContract
		wire.SchemaContract = &contract
	}

	validation := canonicalValidationYAML{}
	if migration.fieldWasSet("validation.mode") {
		validation.Mode = pointerTo(migration.Validation.Mode)
	}
	if migration.fieldWasSet("validation.fail_on_mismatch") {
		validation.FailOnMismatch = pointerTo(
			migration.Validation.FailOnMismatch,
		)
	}
	if migration.fieldWasSet("validation.fail_on_timeout") {
		validation.FailOnTimeout = pointerTo(
			migration.Validation.FailOnTimeout,
		)
	}
	if migration.fieldWasSet("validation.fail_on_estimate_mismatch") {
		validation.FailOnEstimateMismatch = pointerTo(
			migration.Validation.FailOnEstimateMismatch,
		)
	}
	if validation.hasValues() {
		wire.Validation = &validation
	}

	deletes := canonicalDeletesYAML{}
	if migration.fieldWasSet("deletes.mode") {
		deletes.Mode = pointerTo(migration.Deletes.Mode)
	}
	if migration.fieldWasSet("deletes.target_behavior") {
		deletes.TargetBehavior = pointerTo(
			migration.Deletes.TargetBehavior,
		)
	}
	reconcile := canonicalDeleteReconcileYAML{}
	if migration.fieldWasSet("deletes.reconcile.schedule") {
		reconcile.Schedule = pointerTo(
			migration.Deletes.Reconcile.Schedule,
		)
	}
	if migration.fieldWasSet("deletes.reconcile.interval") {
		reconcile.Interval = pointerTo(
			migration.Deletes.Reconcile.Interval,
		)
	}
	if migration.fieldWasSet("deletes.reconcile.batch_size") {
		reconcile.BatchSize = pointerTo(
			migration.Deletes.Reconcile.BatchSize,
		)
	}
	if migration.fieldWasSet("deletes.reconcile.require_primary_key") {
		reconcile.RequirePrimaryKey = pointerTo(
			migration.Deletes.Reconcile.RequirePrimaryKey,
		)
	}
	if reconcile.hasValues() {
		deletes.Reconcile = &reconcile
	}
	if deletes.hasValues() {
		wire.Deletes = &deletes
	}

	if migration.fieldWasSet("history_retention_days") {
		wire.HistoryRetentionDays = pointerTo(
			migration.HistoryRetentionDays,
		)
	}
	if migration.fieldWasSet("tuning") {
		wire.Tuning = pointerTo(migration.Tuning)
	}
	if migration.fieldWasSet("runtime_tuning") {
		wire.RuntimeTuning = pointerTo(migration.RuntimeTuning)
	}
	if migration.fieldWasSet("runtime_tuning_interval") {
		wire.RuntimeTuningInterval = pointerTo(
			migration.RuntimeTuningInterval,
		)
	}
	if migration.fieldWasSet("allow_partial") {
		wire.AllowPartial = pointerTo(migration.AllowPartial)
	}
	return wire, nil
}

type canonicalMigrationYAML struct {
	TargetMode             *string                  `yaml:"target_mode,omitempty"`
	IncludeTables          *[]string                `yaml:"include_tables,omitempty"`
	ExcludeTables          *[]string                `yaml:"exclude_tables,omitempty"`
	DateUpdatedColumns     *[]string                `yaml:"date_updated_columns,omitempty"`
	ConnectionLimit        *int                     `yaml:"connection_limit,omitempty"`
	Workers                *int                     `yaml:"workers,omitempty"`
	ChunkSize              *int                     `yaml:"chunk_size,omitempty"`
	Partitions             *int                     `yaml:"partitions,omitempty"`
	LargeTableThreshold    *int64                   `yaml:"large_table_threshold,omitempty"`
	ReaderParallelism      *int                     `yaml:"reader_parallelism,omitempty"`
	WriterParallelism      *int                     `yaml:"writer_parallelism,omitempty"`
	ReadAhead              *int                     `yaml:"read_ahead,omitempty"`
	UpsertMergeSize        *int                     `yaml:"upsert_merge_size,omitempty"`
	MemoryCeilingBytes     *int64                   `yaml:"memory_ceiling_bytes,omitempty"`
	CheckpointFrequency    *int                     `yaml:"checkpoint_frequency,omitempty"`
	MaxRetries             *int                     `yaml:"max_retries,omitempty"`
	StrictConsistency      *bool                    `yaml:"strict_consistency,omitempty"`
	StrictConsistencyScope *StrictConsistencyScope  `yaml:"strict_consistency_scope,omitempty"`
	FailOnSchemaDrift      *bool                    `yaml:"fail_on_schema_drift,omitempty"`
	SchemaContract         *SchemaContract          `yaml:"schema_contract,omitempty"`
	Validation             *canonicalValidationYAML `yaml:"validation,omitempty"`
	Deletes                *canonicalDeletesYAML    `yaml:"deletes,omitempty"`
	HistoryRetentionDays   *int                     `yaml:"history_retention_days,omitempty"`
	Tuning                 *TuningMode              `yaml:"tuning,omitempty"`
	RuntimeTuning          *bool                    `yaml:"runtime_tuning,omitempty"`
	RuntimeTuningInterval  *time.Duration           `yaml:"runtime_tuning_interval,omitempty"`
	AllowPartial           *bool                    `yaml:"allow_partial,omitempty"`
}

type canonicalValidationYAML struct {
	Mode                   *ValidationMode `yaml:"mode,omitempty"`
	FailOnMismatch         *bool           `yaml:"fail_on_mismatch,omitempty"`
	FailOnTimeout          *bool           `yaml:"fail_on_timeout,omitempty"`
	FailOnEstimateMismatch *bool           `yaml:"fail_on_estimate_mismatch,omitempty"`
}

func (value canonicalValidationYAML) hasValues() bool {
	return value.Mode != nil ||
		value.FailOnMismatch != nil ||
		value.FailOnTimeout != nil ||
		value.FailOnEstimateMismatch != nil
}

type canonicalDeletesYAML struct {
	Mode           *DeleteMode                   `yaml:"mode,omitempty"`
	TargetBehavior *DeleteTargetBehavior         `yaml:"target_behavior,omitempty"`
	Reconcile      *canonicalDeleteReconcileYAML `yaml:"reconcile,omitempty"`
}

func (value canonicalDeletesYAML) hasValues() bool {
	return value.Mode != nil ||
		value.TargetBehavior != nil ||
		value.Reconcile != nil
}

type canonicalDeleteReconcileYAML struct {
	Schedule          *DeleteSchedule `yaml:"schedule,omitempty"`
	Interval          *time.Duration  `yaml:"interval,omitempty"`
	BatchSize         *int            `yaml:"batch_size,omitempty"`
	RequirePrimaryKey *bool           `yaml:"require_primary_key,omitempty"`
}

func (value canonicalDeleteReconcileYAML) hasValues() bool {
	return value.Schedule != nil ||
		value.Interval != nil ||
		value.BatchSize != nil ||
		value.RequirePrimaryKey != nil
}

func pointerTo[T any](value T) *T {
	return &value
}

func cloneStringSlice(value []string) []string {
	if value == nil {
		return nil
	}
	return append([]string(nil), value...)
}
