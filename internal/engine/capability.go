package engine

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
