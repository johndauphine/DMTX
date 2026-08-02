package config

import (
	"strings"
	"testing"
)

func TestValidateBoundedStage4SettingsRejectsRemainingExplicitUnconsumedSettings(t *testing.T) {
	tests := []struct {
		name  string
		yaml  string
		field string
	}{
		{name: "history retention", yaml: "history_retention_days: 7", field: "history_retention_days"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Parse([]byte("migration:\n  " + test.yaml + "\n"))
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateBoundedStage4Settings(cfg.Migration); err == nil ||
				!strings.Contains(err.Error(), test.field) {
				t.Fatalf("setting error = %v, want %q", err, test.field)
			}
		})
	}
}

func TestValidateBoundedStage4SettingsDefersLargeTableThresholdToRouteAdmission(t *testing.T) {
	cfg, err := Parse([]byte("migration:\n  large_table_threshold: 100\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		t.Fatalf("route-owned large table threshold rejected globally: %v", err)
	}
}

func TestValidateBoundedStage4SettingsAllowsExplicitUpsertMergeSize(t *testing.T) {
	cfg, err := Parse([]byte("migration:\n  upsert_merge_size: 100\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		t.Fatalf("consumed upsert merge size rejected: %v", err)
	}
}

func TestValidateBoundedStage4SettingsAllowsCheckpointFrequency(t *testing.T) {
	cfg, err := Parse([]byte("migration:\n  checkpoint_frequency: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		t.Fatalf("composed checkpoint frequency rejected: %v", err)
	}
}

func TestValidateBoundedStage4SettingsAllowsRuntimeTuningSettings(t *testing.T) {
	cfg, err := Parse([]byte("source: {}\ntarget: {}\nmigration:\n  runtime_tuning: true\n  runtime_tuning_interval: 7s\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		t.Fatalf("composed runtime tuning rejected: %v", err)
	}

	defaultConfig, err := Parse([]byte("source: {}\ntarget: {}\nmigration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateBoundedStage4Settings(defaultConfig.Migration); err != nil {
		t.Fatalf("omitted deferred settings rejected: %v", err)
	}
}
