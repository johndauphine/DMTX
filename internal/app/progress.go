package app

import "sync"

// Progress kinds. A run reports the table set once, then a pair of events per
// table as it starts and finishes.
const (
	ProgressTablesPlanned = "tables_planned"
	ProgressTableStarted  = "table_started"
	ProgressTableFinished = "table_finished"
)

// Progress is a report of how far a migration has got.
//
// It is a report, not a promise: a caller may see none of these, and a caller
// that stops reading loses nothing but the view. Nothing in the engine waits on
// it and nothing decides anything from it.
type Progress struct {
	Kind string `json:"kind"`

	// Table is the table this report concerns, for the per-table kinds.
	Table string `json:"table,omitempty"`

	// Tables is the full planned set, sent once with ProgressTablesPlanned so a
	// watcher knows the denominator before anything moves.
	Tables []string `json:"tables,omitempty"`

	// Rows is how many rows the finished table carried.
	Rows int `json:"rows,omitempty"`

	// Done and Total count tables, so a watcher can render a fraction without
	// keeping its own tally and without needing every event to have arrived.
	Done  int `json:"done"`
	Total int `json:"total"`
}

// ProgressFunc receives progress reports.
//
// It is called from the migration's own goroutine, so it should return
// promptly. It cannot fail: there is no error to return, deliberately.
type ProgressFunc func(Progress)

// progressReporter turns observer callbacks into reports and keeps the tally.
//
// A pointer, because the observer that holds it is copied by value as it passes
// through the engine, and a count kept in a copy counts nothing.
type progressReporter struct {
	emit ProgressFunc

	mutex sync.Mutex
	total int
	done  int
}

// newProgressReporter returns nil when there is nobody watching, so the whole
// mechanism costs one nil check on the migration's path.
func newProgressReporter(emit ProgressFunc) *progressReporter {
	if emit == nil {
		return nil
	}
	return &progressReporter{emit: emit}
}

// report delivers one event, and cannot let a watcher affect the migration.
//
// This is the property that matters. Observer callbacks return errors that stop
// a run, and they are called at durable checkpoint boundaries; a report raised
// on that path would mean the act of watching a migration could kill it. So the
// panic of a badly behaved sink is swallowed here rather than unwound into the
// engine. A watcher gets to see the run, not to end it.
func (reporter *progressReporter) report(event Progress) {
	if reporter == nil {
		return
	}
	reporter.mutex.Lock()
	event.Done = reporter.done
	event.Total = reporter.total
	emit := reporter.emit
	reporter.mutex.Unlock()

	defer func() { _ = recover() }()
	emit(event)
}

// planned records the table set and reports it.
func (reporter *progressReporter) planned(tables []string) {
	if reporter == nil {
		return
	}
	reporter.mutex.Lock()
	reporter.total = len(tables)
	reporter.done = 0
	reporter.mutex.Unlock()

	reporter.report(Progress{
		Kind:   ProgressTablesPlanned,
		Tables: append([]string(nil), tables...),
	})
}

// starting reports that a table has begun.
func (reporter *progressReporter) starting(table string) {
	reporter.report(Progress{Kind: ProgressTableStarted, Table: table})
}

// finished counts a completed table and reports it.
func (reporter *progressReporter) finished(table string, rows int) {
	if reporter == nil {
		return
	}
	reporter.mutex.Lock()
	reporter.done++
	reporter.mutex.Unlock()

	reporter.report(Progress{
		Kind:  ProgressTableFinished,
		Table: table,
		Rows:  rows,
	})
}
