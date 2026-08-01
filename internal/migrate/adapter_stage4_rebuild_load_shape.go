package migrate

import "github.com/johndauphine/dmtx/internal/schema"

// stage4RebuildPreFinalizeTable returns the exact table authority that is
// expected between set-wide PrepareTables and set-wide FinalizeTables. The
// relational rebuild lifecycle deliberately creates only the base table plus
// its primary key before data transfer; secondary indexes, CHECKs, and foreign
// keys are all planner-sealed post-load objects. A page writer must therefore
// reject drift from this base shape, rather than incorrectly demand objects
// which the runner is required to create only after every selected table has
// transferred.
//
// The clone is important: callers retain the full final target shape for
// finalization and terminal validation, and a page-local proof must never
// mutate that immutable plan.
func stage4RebuildPreFinalizeTable(table schema.Table) schema.Table {
	base := cloneStage4RichTable(table)
	base.Indexes = nil
	base.Checks = nil
	base.ForeignKeys = nil
	return base
}
