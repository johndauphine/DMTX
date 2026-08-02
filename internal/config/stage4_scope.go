package config

import "fmt"

// ValidateBoundedStage4Settings rejects an explicitly requested setting until
// the bounded Stage 4 runner has a real consumer for it. Parsing a documented
// field remains useful for compatibility and intent preservation, but allowing
// an operator to believe an inert setting changed execution is unsafe.
//
// Omitted settings retain their parser defaults. Runtime tuning is excluded
// because the deferred production runner composes it at chunk boundaries.
// large_table_threshold is admitted by the Stage 4 route gate instead: only a
// route that can bind an exact table size to its retained source view may
// consume it.
func ValidateBoundedStage4Settings(migration Migration) error {
	for _, setting := range []string{
		"history_retention_days",
	} {
		if migration.fieldWasSet(setting) {
			return fmt.Errorf(
				"migration.%s is not implemented by the bounded Stage 4 runner",
				setting,
			)
		}
	}
	return nil
}
