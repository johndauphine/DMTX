package engine

import (
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
)

// Capability describes the target operations DMTX may select explicitly.
// A false value means the operation is unavailable, never silently skipped.
type Capability struct {
	BulkPath            string
	Upsert              bool
	SequenceReset       bool
	PostLoadConstraints bool
	StrictConsistency   string
}

var targetCapabilities = map[string]Capability{
	"postgres":   {BulkPath: "COPY", Upsert: true, SequenceReset: true, PostLoadConstraints: true, StrictConsistency: "table_and_migration"},
	"mssql":      {BulkPath: "TDS bulk copy", Upsert: true, SequenceReset: true, PostLoadConstraints: true, StrictConsistency: "table_and_migration"},
	"mysql":      {BulkPath: "LOAD DATA LOCAL INFILE or bounded insert", Upsert: true, SequenceReset: true, PostLoadConstraints: true, StrictConsistency: "table"},
	"sqlite":     {BulkPath: "bounded batched insert", Upsert: true, SequenceReset: true, PostLoadConstraints: false, StrictConsistency: "table"},
	"clickhouse": {BulkPath: "native or batched columnar insert", Upsert: false, SequenceReset: false, PostLoadConstraints: false, StrictConsistency: "unsupported"},
}

// TargetCapability returns the immutable capability declaration for one
// canonical engine.
func TargetCapability(name string) (Capability, bool) {
	capability, ok := targetCapabilities[name]
	return capability, ok
}

// ValidateMigration rejects unsupported configured behavior before a caller
// opens state, acquires a lease, or mutates a target.
func ValidateMigration(cfg config.Config) error {
	target, ok := TargetCapability(cfg.Target.Type)
	if !ok {
		return fmt.Errorf("target engine %q is not registered", cfg.Target.Type)
	}
	if _, ok := TargetCapability(cfg.Source.Type); !ok {
		return fmt.Errorf("source engine %q is not registered", cfg.Source.Type)
	}
	if cfg.Migration.TargetMode == "upsert" && !target.Upsert {
		return fmt.Errorf("target engine %s does not support upsert mode", cfg.Target.Type)
	}
	if !implementedPair(cfg.Source.Type, cfg.Target.Type) {
		return fmt.Errorf("migration pair %s-to-%s is not implemented", cfg.Source.Type, cfg.Target.Type)
	}
	return nil
}

func implementedPair(source, target string) bool {
	return (source == "sqlite" && target == "sqlite") || (source == "postgres" && target == "sqlite")
}
