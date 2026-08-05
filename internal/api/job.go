package api

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/app"
)

// jobRetention is how long a finished job stays readable.
//
// Long enough that an operator whose laptop slept through the end of a run can
// wake up and still see how it went; short enough that a server left running
// for weeks does not accumulate every outcome it ever produced.
//
// A variable so tests can shorten it; nothing else reassigns it.
var jobRetention = time.Hour

// Event kinds. A job emits exactly one started and, eventually, exactly one
// finished. Between them come progress reports: one naming the planned table
// set, then a pair as each table begins and completes.
const (
	eventStarted  = "started"
	eventProgress = "progress"
	eventFinished = "finished"
)

// maxRetainedEvents bounds a job's replay buffer.
//
// A migration reports once for the planned table set and twice per table, so a
// large workload would otherwise hold tens of thousands of events for an hour
// after it finished. Trimming costs a reconnecting client little, because every
// progress report carries its own running tally - but only for the events that
// restate themselves. See announcedOnce: the ones that do not are held back
// from trimming, because losing one cannot be recovered from anything later.
//
// Sequence numbers come from a counter rather than the buffer length so that
// trimming cannot make them repeat. They are therefore not contiguous after a
// trim, and nothing may assume they are.
const maxRetainedEvents = 2048

var errNoSuchJob = errors.New("no such job")

// jobEvent is one entry in a job's stream.
//
// Sequence is what makes reconnection work: a client that drops sends the last
// sequence it saw and is given everything after it, so a closed lid does not
// lose the end of a migration.
type jobEvent struct {
	Sequence int             `json:"sequence"`
	Kind     string          `json:"kind"`
	Data     json.RawMessage `json:"data,omitempty"`
}

// job is one command running independently of whoever asked for it.
type job struct {
	id      string
	command string

	mutex    sync.Mutex
	events   []jobEvent
	kept     []jobEvent
	sequence int
	outcome  *app.Outcome
	finished time.Time
	changed  chan struct{}
	done     chan struct{}
	cancel   context.CancelFunc
}

// ID identifies the job in URLs.
func (running *job) ID() string { return running.id }

// Whether an event survives trimming, named at the call sites so the decision
// reads there rather than having to be inferred from a bare true.
//
// An announced-once event is one the engine sends a single time and never
// restates: which command is running, and which tables are planned. A client
// that reconnects after it has been trimmed away cannot recover it from any
// later event, so it is held. Everything else carries enough of its own state
// to be read on its own.
const (
	trimmable     = false
	announcedOnce = true
)

// maxAnnouncedOnce bounds how many events a job may hold back from trimming.
//
// Two are expected: started, and tables_planned. The bound exists so that a
// future emitter marking events announcedOnce in a loop cannot grow the buffer
// past its cap or shorten the trim window to nothing - it would fail loudly at
// the slice bound instead, which is a worse way to find out.
const maxAnnouncedOnce = 8

// emit appends an event and wakes every reader waiting on this job.
func (running *job) emit(kind string, data json.RawMessage, retention bool) {
	running.mutex.Lock()
	defer running.mutex.Unlock()
	running.appendLocked(kind, data, retention)
}

// appendLocked records an event and wakes every reader. The caller holds the
// lock, so a caller with more than one thing to change can make all of it
// visible at once.
func (running *job) appendLocked(kind string, data json.RawMessage, retention bool) {
	running.sequence++
	event := jobEvent{Sequence: running.sequence, Kind: kind, Data: data}
	running.events = append(running.events, event)
	if retention && len(running.kept) < maxAnnouncedOnce {
		running.kept = append(running.kept, event)
	}
	if len(running.events) > maxRetainedEvents {
		// The newest events, then whichever announced-once events fell out of
		// that window put back in front of them.
		//
		// This used to keep events[0] and nothing else, which kept the started
		// event and dropped tables_planned - event two - so a reconnect to any
		// migration wide enough to trim lost the planned set, and with it the
		// denominator of every later report.
		//
		// The window is shortened by the number of announced-once events rather
		// than the buffer being allowed to grow past the cap, and the events
		// still inside it are not put back a second time. Written as a
		// comparison against the window's oldest sequence rather than as index
		// arithmetic, because the arithmetic is only correct while every
		// announced-once event happens to arrive first.
		tail := running.events[len(running.events)-(maxRetainedEvents-len(running.kept)):]
		keep := make([]jobEvent, 0, maxRetainedEvents)
		for _, kept := range running.kept {
			if kept.Sequence < tail[0].Sequence {
				keep = append(keep, kept)
			}
		}
		running.events = append(keep, tail...)
	}
	// Closing the current channel and replacing it broadcasts to everyone
	// selecting on it, which a buffered channel could not do without knowing
	// how many readers there are.
	close(running.changed)
	running.changed = make(chan struct{})
}

// reportProgress turns an engine report into an event on this job's stream.
//
// A report that cannot be encoded is dropped rather than raised. This is called
// from the migration's own goroutine at durable checkpoint boundaries, so
// anything that escaped here would be a watcher interfering with the work being
// watched.
func (running *job) reportProgress(event app.Progress) {
	encoded, err := json.Marshal(event)
	if err != nil {
		return
	}
	// The planned set is sent once and never restated, unlike the per-table
	// reports which each carry Done and Total.
	retention := trimmable
	if event.Kind == app.ProgressTablesPlanned {
		retention = announcedOnce
	}
	running.emit(eventProgress, encoded, retention)
}

// complete records the outcome and closes the job.
//
// The outcome and the finished event become visible together, under one lock.
// Set apart, a reader can see a job that has ended while its finished event has
// not been appended yet - so the stream reports the end and returns without
// ever sending it, and the operator watches a completed migration that never
// says how it went.
func (running *job) complete(outcome app.Outcome) {
	encoded, err := json.Marshal(outcome)
	if err != nil {
		// An outcome that will not marshal still has to end the job, or a
		// client waits on a stream that has already stopped producing.
		encoded = json.RawMessage(`{"error":"outcome could not be encoded"}`)
	}
	running.mutex.Lock()
	running.outcome = &outcome
	running.finished = time.Now()
	// Trimmable, and it makes no difference: nothing follows it, so it can
	// never be trimmed away.
	running.appendLocked(eventFinished, encoded, trimmable)
	running.mutex.Unlock()

	close(running.done)
}

// next returns the events after a sequence number, whether the job has ended,
// and a channel closed when the next event arrives.
//
// All three come from one acquisition of the lock, and that is the point. Read
// the events and subscribe separately and there is a gap between them: an event
// landing in it is missing from the read and has already closed the channel the
// caller then waits on, so a stream waits forever for a job that has already
// finished. Handing back the channel from inside the same lock leaves no gap to
// land in, so a caller cannot get the order wrong - there is no order to get
// wrong.
//
// This was a real hazard, not a theoretical one: the first version of this code
// did read and subscribe separately, in the correct order, guarded only by a
// comment. A test written to hold that order honest could not catch it being
// reversed, because the window is microseconds wide. Removing the window beat
// testing for it.
//
// Ended is read from the events rather than from the outcome field for the same
// reason. Two sources of truth can disagree for an instant, and the instant
// that matters is the one where a job reports it has ended before the event
// saying so exists - a stream would report the end and return without ever
// sending it. Taking the answer from the events makes that unsayable: the
// finished event is what ending means.
func (running *job) next(sequence int) ([]jobEvent, bool, <-chan struct{}) {
	running.mutex.Lock()
	defer running.mutex.Unlock()
	var pending []jobEvent
	for _, event := range running.events {
		if event.Sequence > sequence {
			pending = append(pending, event)
		}
	}
	ended := len(running.events) > 0 &&
		running.events[len(running.events)-1].Kind == eventFinished
	return pending, ended, running.changed
}

// result reports the outcome, once there is one.
func (running *job) result() (app.Outcome, bool) {
	running.mutex.Lock()
	defer running.mutex.Unlock()
	if running.outcome == nil {
		return app.Outcome{}, false
	}
	return *running.outcome, true
}

// jobs holds every job this server has started.
type jobs struct {
	mutex    sync.Mutex
	byID     map[string]*job
	activity *activity

	// execute is the seam between the job machinery and the command layer.
	// Tests replace it to drive a command they control - one that blocks,
	// notices cancellation, or reports progress - which is the only way to
	// check this machinery without standing up a database to be slow at.
	execute func(context.Context, app.Request, app.ProgressFunc) app.Outcome
}

func newJobs(tracker *activity) *jobs {
	return &jobs{
		byID:     make(map[string]*job),
		activity: tracker,
		execute:  app.ExecuteWithProgress,
	}
}

// start runs a command in the background and returns immediately.
//
// The context is deliberately not the HTTP request's. A migration must outlive
// the tab that started it: a closed lid, a dropped connection, or an operator
// who navigated away must not roll back hours of work. Cancelling is something
// they ask for, not something their network does to them.
func (registry *jobs) start(request app.Request) (*job, error) {
	id, err := newToken()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	running := &job{
		id:      id,
		command: request.Command,
		changed: make(chan struct{}),
		done:    make(chan struct{}),
		cancel:  cancel,
	}

	registry.mutex.Lock()
	registry.forgetFinished()
	registry.byID[id] = running
	registry.mutex.Unlock()

	started, err := json.Marshal(map[string]string{
		"id":      id,
		"command": request.Command,
	})
	if err != nil {
		cancel()
		return nil, err
	}
	running.emit(eventStarted, started, announcedOnce)

	// Counted as activity for as long as it runs. The idle watchdog's guard was
	// written when commands ran inside the handler; without this a migration
	// with nobody watching produces no requests, and the server would stop
	// itself in the middle of one.
	registry.activity.begin()
	go func() {
		defer registry.activity.end()
		defer cancel()
		running.complete(registry.execute(ctx, request, running.reportProgress))
	}()
	return running, nil
}

// find looks a job up by id.
func (registry *jobs) find(id string) (*job, bool) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	running, ok := registry.byID[id]
	return running, ok
}

// cancel asks a job to stop. Stopping is cooperative: the command decides where
// it is safe to give up, which is the same contract Ctrl-C has on the command
// line.
func (registry *jobs) cancel(id string) error {
	running, ok := registry.find(id)
	if !ok {
		return errNoSuchJob
	}
	running.cancel()
	return nil
}

// forgetFinished drops jobs whose outcome has been available longer than
// jobRetention. The caller holds the registry lock.
func (registry *jobs) forgetFinished() {
	for id, running := range registry.byID {
		running.mutex.Lock()
		expired := running.outcome != nil &&
			!running.finished.IsZero() &&
			time.Since(running.finished) > jobRetention
		running.mutex.Unlock()
		if expired {
			delete(registry.byID, id)
		}
	}
}
