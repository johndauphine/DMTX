package config

import (
	"fmt"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// StrictConsistencyScope is an alias so existing integrations that pass
// strings remain source-compatible while the public values are named.
type StrictConsistencyScope = string

const (
	StrictConsistencyTable     StrictConsistencyScope = "table"
	StrictConsistencyMigration StrictConsistencyScope = "migration"
)

// SchemaContractMode controls one class of source-schema drift.
type SchemaContractMode string

const (
	SchemaContractEvolve       SchemaContractMode = "evolve"
	SchemaContractFreeze       SchemaContractMode = "freeze"
	SchemaContractDiscardRow   SchemaContractMode = "discard_row"
	SchemaContractDiscardValue SchemaContractMode = "discard_value"
	SchemaContractReport       SchemaContractMode = "report"
)

// SchemaContract is the canonical entity-specific schema drift policy. YAML
// also accepts one scalar mode and expands it across all three entities.
type SchemaContract struct {
	Tables   SchemaContractMode `yaml:"tables" json:"tables"`
	Columns  SchemaContractMode `yaml:"columns" json:"columns"`
	DataType SchemaContractMode `yaml:"data_type" json:"data_type"`
}

func (contract *SchemaContract) UnmarshalYAML(node *yaml.Node) error {
	if node == nil {
		return fmt.Errorf("schema contract is required")
	}
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Tag == "!!null" || strings.TrimSpace(node.Value) == "" {
			return fmt.Errorf("schema contract mode must not be blank or null")
		}
		var mode SchemaContractMode
		if err := node.Decode(&mode); err != nil {
			return fmt.Errorf("decode schema contract mode: %w", err)
		}
		contract.Tables = mode
		contract.Columns = mode
		contract.DataType = mode
		return nil
	case yaml.MappingNode:
		*contract = SchemaContract{
			Tables:   SchemaContractEvolve,
			Columns:  SchemaContractEvolve,
			DataType: SchemaContractEvolve,
		}
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index].Value
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate schema contract entity %q", key)
			}
			seen[key] = struct{}{}
			if node.Content[index+1].Tag == "!!null" ||
				strings.TrimSpace(node.Content[index+1].Value) == "" {
				return fmt.Errorf(
					"schema contract %s mode must not be blank or null",
					key,
				)
			}
			var mode SchemaContractMode
			if err := node.Content[index+1].Decode(&mode); err != nil {
				return fmt.Errorf("decode schema contract %s mode: %w", key, err)
			}
			switch key {
			case "tables":
				contract.Tables = mode
			case "columns":
				contract.Columns = mode
			case "data_type":
				contract.DataType = mode
			default:
				return fmt.Errorf("unknown schema contract entity %q", key)
			}
		}
		return nil
	default:
		return fmt.Errorf("schema contract must be a mode or mapping")
	}
}

// ValidationMode values are inclusive: each mode adds checks to the modes
// before it. Full remains reserved until a proven whole-table algorithm exists.
type ValidationMode string

const (
	ValidationCountOnly  ValidationMode = "count_only"
	ValidationNullParity ValidationMode = "null_parity"
	ValidationSample     ValidationMode = "sample"
	ValidationFull       ValidationMode = "full"
)

type ValidationPolicy struct {
	Mode                   ValidationMode `yaml:"mode" json:"mode"`
	FailOnMismatch         bool           `yaml:"fail_on_mismatch" json:"fail_on_mismatch"`
	FailOnTimeout          bool           `yaml:"fail_on_timeout" json:"fail_on_timeout"`
	FailOnEstimateMismatch bool           `yaml:"fail_on_estimate_mismatch" json:"fail_on_estimate_mismatch"`
}

type DeleteMode string
type DeleteTargetBehavior string
type DeleteSchedule string

const (
	DeleteModeOff       DeleteMode = "off"
	DeleteModeReconcile DeleteMode = "reconcile"

	DeleteTargetHard DeleteTargetBehavior = "hard"

	DeleteScheduleInterval DeleteSchedule = "interval"
)

type DeleteReconcilePolicy struct {
	Schedule          DeleteSchedule `yaml:"schedule,omitempty" json:"schedule"`
	Interval          time.Duration  `yaml:"interval,omitempty" json:"interval"`
	BatchSize         int            `yaml:"batch_size,omitempty" json:"batch_size"`
	RequirePrimaryKey bool           `yaml:"require_primary_key,omitempty" json:"require_primary_key"`
}

type DeletePolicy struct {
	Mode           DeleteMode            `yaml:"mode" json:"mode"`
	TargetBehavior DeleteTargetBehavior  `yaml:"target_behavior,omitempty" json:"target_behavior"`
	Reconcile      DeleteReconcilePolicy `yaml:"reconcile,omitempty" json:"reconcile"`
}

type TuningMode string

const (
	TuningAuto TuningMode = "auto"
	TuningOff  TuningMode = "off"
)

const (
	DefaultHistoryRetentionDays  = 30
	DefaultDeleteBatchSize       = 10_000
	DefaultDeleteInterval        = 168 * time.Hour
	DefaultRuntimeTuningInterval = 5 * time.Second
)

func applyProductionSemanticsDefaults(migration *Migration) {
	if migration.SchemaContract != nil {
		applySchemaContractDefaults(migration.SchemaContract)
	}
	if !migration.fieldWasSet("validation.mode") {
		migration.Validation.Mode = ValidationCountOnly
	}
	if !migration.fieldWasSet("validation.fail_on_mismatch") {
		migration.Validation.FailOnMismatch = true
	}
	if !migration.fieldWasSet("validation.fail_on_timeout") {
		migration.Validation.FailOnTimeout = true
	}
	if !migration.fieldWasSet("validation.fail_on_estimate_mismatch") {
		migration.Validation.FailOnEstimateMismatch = true
	}
	if !migration.fieldWasSet("deletes.mode") {
		migration.Deletes.Mode = DeleteModeOff
	}
	if migration.Deletes.Mode == DeleteModeReconcile {
		if !migration.fieldWasSet("deletes.target_behavior") {
			migration.Deletes.TargetBehavior = DeleteTargetHard
		}
		if !migration.fieldWasSet("deletes.reconcile.schedule") {
			migration.Deletes.Reconcile.Schedule = DeleteScheduleInterval
		}
		if !migration.fieldWasSet("deletes.reconcile.interval") {
			migration.Deletes.Reconcile.Interval = DefaultDeleteInterval
		}
		if !migration.fieldWasSet("deletes.reconcile.batch_size") {
			migration.Deletes.Reconcile.BatchSize = DefaultDeleteBatchSize
		}
		if !migration.fieldWasSet("deletes.reconcile.require_primary_key") {
			migration.Deletes.Reconcile.RequirePrimaryKey = true
		}
	}
	if !migration.fieldWasSet("history_retention_days") {
		migration.HistoryRetentionDays = DefaultHistoryRetentionDays
	}
	if !migration.fieldWasSet("tuning") {
		migration.Tuning = TuningAuto
	}
	if !migration.fieldWasSet("runtime_tuning") {
		migration.RuntimeTuning = true
	}
	if !migration.fieldWasSet("runtime_tuning_interval") {
		migration.RuntimeTuningInterval = DefaultRuntimeTuningInterval
	}
}

func applySchemaContractDefaults(contract *SchemaContract) {
	if contract.Tables == "" {
		contract.Tables = SchemaContractEvolve
	}
	if contract.Columns == "" {
		contract.Columns = SchemaContractEvolve
	}
	if contract.DataType == "" {
		contract.DataType = SchemaContractEvolve
	}
}

func validateProductionSemantics(migration Migration) error {
	if migration.SchemaContract != nil {
		for _, entity := range []struct {
			name string
			mode SchemaContractMode
		}{
			{name: "tables", mode: migration.SchemaContract.Tables},
			{name: "columns", mode: migration.SchemaContract.Columns},
			{name: "data_type", mode: migration.SchemaContract.DataType},
		} {
			if !validSchemaContractMode(entity.mode) {
				return fmt.Errorf(
					"migration.schema_contract.%s has invalid mode %q",
					entity.name,
					entity.mode,
				)
			}
		}
		if migration.SchemaContract.Tables == SchemaContractDiscardValue {
			return fmt.Errorf(
				"migration.schema_contract.tables cannot use discard_value",
			)
		}
	}

	seenDateColumns := make(map[string]struct{}, len(migration.DateUpdatedColumns))
	for index, name := range migration.DateUpdatedColumns {
		if strings.TrimSpace(name) == "" {
			return fmt.Errorf(
				"migration.date_updated_columns[%d] must not be empty",
				index,
			)
		}
		if _, exists := seenDateColumns[name]; exists {
			return fmt.Errorf(
				"migration.date_updated_columns contains duplicate %q",
				name,
			)
		}
		seenDateColumns[name] = struct{}{}
	}

	switch migration.Validation.Mode {
	case ValidationCountOnly, ValidationNullParity, ValidationSample:
	case ValidationFull:
		return fmt.Errorf(
			"migration.validation.mode full is reserved and unsupported",
		)
	default:
		return fmt.Errorf(
			"migration.validation.mode has invalid value %q",
			migration.Validation.Mode,
		)
	}

	switch migration.Deletes.Mode {
	case DeleteModeOff:
		if migration.fieldWasSet("deletes.target_behavior") ||
			migration.fieldWasSet("deletes.reconcile") {
			return fmt.Errorf(
				"migration.deletes settings require mode reconcile",
			)
		}
	case DeleteModeReconcile:
		if migration.TargetMode != "upsert" {
			return fmt.Errorf(
				"migration.deletes.mode reconcile requires target_mode upsert",
			)
		}
		if migration.Deletes.TargetBehavior != DeleteTargetHard {
			return fmt.Errorf(
				"migration.deletes.target_behavior has invalid value %q; only hard is supported",
				migration.Deletes.TargetBehavior,
			)
		}
		if migration.Deletes.Reconcile.Schedule != DeleteScheduleInterval {
			return fmt.Errorf(
				"migration.deletes.reconcile.schedule has invalid value %q; only interval is supported",
				migration.Deletes.Reconcile.Schedule,
			)
		}
		if migration.Deletes.Reconcile.Interval <= 0 {
			return fmt.Errorf(
				"migration.deletes.reconcile.interval must be positive",
			)
		}
		if migration.Deletes.Reconcile.BatchSize <= 0 {
			return fmt.Errorf(
				"migration.deletes.reconcile.batch_size must be positive",
			)
		}
		if !migration.Deletes.Reconcile.RequirePrimaryKey {
			return fmt.Errorf(
				"migration.deletes.reconcile.require_primary_key must be true",
			)
		}
	default:
		return fmt.Errorf(
			"migration.deletes.mode has invalid value %q",
			migration.Deletes.Mode,
		)
	}

	if migration.HistoryRetentionDays < 0 {
		return fmt.Errorf(
			"migration.history_retention_days must not be negative",
		)
	}
	switch migration.Tuning {
	case TuningAuto, TuningOff:
	default:
		return fmt.Errorf(
			"migration.tuning has invalid value %q",
			migration.Tuning,
		)
	}
	if migration.RuntimeTuningInterval <= 0 {
		return fmt.Errorf(
			"migration.runtime_tuning_interval must be positive",
		)
	}
	return nil
}

func validSchemaContractMode(mode SchemaContractMode) bool {
	switch mode {
	case SchemaContractEvolve,
		SchemaContractFreeze,
		SchemaContractDiscardRow,
		SchemaContractDiscardValue,
		SchemaContractReport:
		return true
	default:
		return false
	}
}
