package state

import "time"

// Backend is the durable migration state used by the application lifecycle.
//
// Lease management is intentionally separate because not every state backend
// is required to expose the SQLite lease implementation.
type Backend interface {
	InitializeRun(Run, string) error
	Append(Run) error
	List() ([]Run, error)
	Latest() (Run, bool, error)
	LatestResumableForTarget(string) (Run, bool, error)
	BindRunLease(string, Lease) error
	ReactivateRun(string, string) error
	UpdateFailure(string, string, time.Time) error
	UpdateRecoverableOutcome(string, Outcome, string, time.Time) error
	AbandonRun(string, string, time.Time) error

	CreateTask(Task) error
	CreateTasks([]Task) error
	AdvanceIntegerKeysetTask(string, string, int, int64) error
	AdvanceRowNumberTask(string, string, int, int64) error
	CompleteTask(string, string, int, time.Time) error
	ListTasks(string) ([]Task, error)

	SaveConfigHash(string, string) error
	ConfigHash(string) (string, bool, error)
	SaveResumeCompatibilityHash(string, string) error
	ResumeCompatibilityHash(string) (string, bool, error)
	AcknowledgeConfigOverride(string, string, string) error
}

var (
	_ Backend = SQLiteStore{}
	_ Backend = YAMLStore{}
)
