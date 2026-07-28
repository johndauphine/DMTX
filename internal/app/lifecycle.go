package app

const (
	runSuccessReason    = "migration completed"
	resumeSuccessReason = "migration resumed and completed"
)

// appLifecycleBoundary is an internal fault-injection seam. Production keeps
// the no-op implementation; subprocess tests replace it before invoking Run.
var appLifecycleBoundary = func(string) error { return nil }
