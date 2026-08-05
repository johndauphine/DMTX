package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/app"
)

// blockingCommand is a command a test can hold open and then release, standing
// in for a migration that takes hours.
type blockingCommand struct {
	entered  chan struct{}
	release  chan struct{}
	observed chan context.Context
	report   app.ProgressFunc
}

func newBlockingCommand() *blockingCommand {
	return &blockingCommand{
		entered:  make(chan struct{}, 1),
		release:  make(chan struct{}),
		observed: make(chan context.Context, 1),
	}
}

// run is the executor a test installs in place of app.Execute.
func (command *blockingCommand) run(ctx context.Context, request app.Request, report app.ProgressFunc) app.Outcome {
	command.report = report
	select {
	case command.entered <- struct{}{}:
	default:
	}
	select {
	case command.observed <- ctx:
	default:
	}
	select {
	case <-command.release:
		return app.Outcome{Command: request.Command, ExitCode: 0}
	case <-ctx.Done():
		// Cancellation is reported the way a real command reports being
		// stopped: an outcome, not a dropped connection.
		return app.Outcome{Command: request.Command, ExitCode: 130}
	}
}

// waitFor polls until a condition holds, so tests do not depend on a fixed
// sleep being long enough on a loaded machine.
func waitFor(condition func() bool) bool {
	for attempt := 0; attempt < 400; attempt++ {
		if condition() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestAJobOutlivesTheRequestThatStartedIt is the defect this whole model exists
// to fix.
//
// Commands used to run inside the HTTP handler on the request's context, so
// closing the browser tab cancelled the migration underneath the operator -
// hours of work discarded by a lid closing. The request going away must now end
// the response and nothing else.
func TestAJobOutlivesTheRequestThatStartedIt(t *testing.T) {
	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run

	ctx, cancel := context.WithCancel(context.Background())
	body := strings.NewReader(`{"command":"run"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/execute", body).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	done := make(chan struct{})
	go func() {
		server.routes().ServeHTTP(httptest.NewRecorder(), request)
		close(done)
	}()

	<-command.entered
	cancel() // the tab closes
	<-done   // the handler gives up on writing

	// The command must still be running, and must still be able to finish.
	observed := <-command.observed
	if observed.Err() != nil {
		t.Fatalf("the command's context was cancelled with the request: %v", observed.Err())
	}
	close(command.release)

	if !waitFor(func() bool { return anyJobFinished(server) }) {
		t.Fatal("the job never finished after its request was abandoned")
	}
}

// anyJobFinished reports whether the server holds a completed job. The request
// that started it never returned an id, which is exactly the situation being
// tested.
func anyJobFinished(server *Server) bool {
	server.jobs.mutex.Lock()
	defer server.jobs.mutex.Unlock()
	for _, running := range server.jobs.byID {
		if _, ok := running.result(); ok {
			return true
		}
	}
	return false
}

// TestARunningJobKeepsTheServerAlive pins the interaction between jobs and the
// idle watchdog.
//
// The watchdog's guard was written when commands ran inside the handler, so an
// in-flight request meant work in progress. Once work happens in a job, a
// migration nobody is watching produces no requests at all - and a server that
// counted that as idleness would shut itself down in the middle of one.
func TestARunningJobKeepsTheServerAlive(t *testing.T) {
	const timeout = 20 * time.Millisecond
	server := newIdleTestServer(t, timeout)
	command := newBlockingCommand()
	server.jobs.execute = command.run

	if _, err := server.jobs.start(app.Request{Command: "run"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-command.entered

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := serveInBackground(t, server, ctx)

	select {
	case <-done:
		t.Fatal("the server stopped for idleness while a job was running")
	case <-time.After(10 * timeout):
	}

	close(command.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the server never stopped once the job finished")
	}
}

// TestCancellingAJobStopsTheCommand pins that stopping is possible, since a
// command that outlives its request would otherwise be unstoppable short of
// killing the server.
func TestCancellingAJobStopsTheCommand(t *testing.T) {
	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run

	running, err := server.jobs.start(app.Request{Command: "run"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-command.entered

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost, "/api/v1/jobs/"+running.ID()+"/cancel", nil,
	)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("cancel returned %d", recorder.Code)
	}

	if !waitFor(func() bool { _, ok := running.result(); return ok }) {
		t.Fatal("a cancelled job never finished")
	}
	outcome, _ := running.result()
	if outcome.ExitCode == 0 {
		t.Error("a cancelled command reported success")
	}
}

// TestJobEventsStreamUntilTheJobEnds pins the stream's shape and its ending.
func TestJobEventsStreamUntilTheJobEnds(t *testing.T) {
	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run

	running, err := server.jobs.start(app.Request{Command: "run"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-command.entered
	close(command.release)
	if !waitFor(func() bool { _, ok := running.result(); return ok }) {
		t.Fatal("job never finished")
	}

	events := streamEvents(t, server, running.ID(), 0)
	if len(events) != 2 {
		t.Fatalf("expected started and finished, got %d: %v", len(events), events)
	}
	if events[0].Kind != eventStarted || events[1].Kind != eventFinished {
		t.Errorf("event kinds are %s, %s", events[0].Kind, events[1].Kind)
	}
	// The finished event has to carry the outcome, or a client that only
	// watched the stream never learns how the command went.
	var outcome app.Outcome
	if err := json.Unmarshal(events[1].Data, &outcome); err != nil {
		t.Fatalf("finished event does not carry an outcome: %s", events[1].Data)
	}
	if outcome.Command != "run" {
		t.Errorf("outcome is for %q", outcome.Command)
	}
}

// TestAReconnectingClientResumesWhereItStopped is what makes a closed lid
// survivable.
//
// A browser's EventSource resends Last-Event-ID by itself, so a client that
// dropped mid-run must be given what it missed rather than the stream from the
// beginning or, worse, only what happens next.
func TestAReconnectingClientResumesWhereItStopped(t *testing.T) {
	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run

	running, err := server.jobs.start(app.Request{Command: "run"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-command.entered
	close(command.release)
	if !waitFor(func() bool { _, ok := running.result(); return ok }) {
		t.Fatal("job never finished")
	}

	// As if the client had seen the started event and then dropped.
	resumed := streamEvents(t, server, running.ID(), 1)
	if len(resumed) != 1 {
		t.Fatalf("resuming from 1 replayed %d events, want just the finished one", len(resumed))
	}
	if resumed[0].Kind != eventFinished {
		t.Errorf("resumed stream began with %s", resumed[0].Kind)
	}

	// And a client that has seen everything is told nothing twice.
	if replayed := streamEvents(t, server, running.ID(), 2); len(replayed) != 0 {
		t.Errorf("resuming from the end replayed %d events", len(replayed))
	}
}

// TestLastEventIDHeaderIsHonoured pins the browser's own reconnect mechanism,
// which sends a header rather than a query parameter.
func TestLastEventIDHeaderIsHonoured(t *testing.T) {
	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run
	running, _ := server.jobs.start(app.Request{Command: "run"})
	<-command.entered
	close(command.release)
	if !waitFor(func() bool { _, ok := running.result(); return ok }) {
		t.Fatal("job never finished")
	}

	request := httptest.NewRequest(
		http.MethodGet, "/api/v1/jobs/"+running.ID()+"/events", nil,
	)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	request.Header.Set("Last-Event-ID", "1")
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)

	events := parseEvents(t, recorder.Body.String())
	if len(events) != 1 || events[0].Kind != eventFinished {
		t.Errorf("Last-Event-ID was ignored: got %v", events)
	}
}

// TestJobStatusReportsTheOutcomeWithoutTheStream pins the path a reconnecting
// client takes when it would rather ask than replay.
func TestJobStatusReportsTheOutcomeWithoutTheStream(t *testing.T) {
	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run
	running, _ := server.jobs.start(app.Request{Command: "run"})
	<-command.entered

	body := statusOf(t, server, running.ID())
	if body["state"] != "running" {
		t.Errorf("a running job reports state %v", body["state"])
	}
	if _, present := body["outcome"]; present {
		t.Error("a running job already carries an outcome")
	}

	close(command.release)
	if !waitFor(func() bool { _, ok := running.result(); return ok }) {
		t.Fatal("job never finished")
	}
	body = statusOf(t, server, running.ID())
	if body["state"] != "finished" {
		t.Errorf("a finished job reports state %v", body["state"])
	}
	if _, present := body["outcome"]; !present {
		t.Error("a finished job carries no outcome")
	}
}

// TestUnknownJobIsNotFound pins that an invented id is a 404 rather than a
// panic or an empty stream that never ends.
func TestUnknownJobIsNotFound(t *testing.T) {
	server := newTestServer(t)
	for _, target := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/jobs/nope"},
		{http.MethodGet, "/api/v1/jobs/nope/events"},
		{http.MethodPost, "/api/v1/jobs/nope/cancel"},
	} {
		request := httptest.NewRequest(target.method, target.path, nil)
		request.Header.Set("Authorization", "Bearer "+server.auth.session)
		recorder := httptest.NewRecorder()
		server.routes().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusNotFound {
			t.Errorf("%s %s returned %d, want 404", target.method, target.path, recorder.Code)
		}
	}
}

// TestJobRoutesRequireAuthentication pins that starting and cancelling
// migrations is behind the session like everything else.
func TestJobRoutesRequireAuthentication(t *testing.T) {
	server := newTestServer(t)
	for _, target := range []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/api/v1/jobs"},
		{http.MethodGet, "/api/v1/jobs/any"},
		{http.MethodGet, "/api/v1/jobs/any/events"},
		{http.MethodPost, "/api/v1/jobs/any/cancel"},
	} {
		request := httptest.NewRequest(target.method, target.path, strings.NewReader("{}"))
		recorder := httptest.NewRecorder()
		server.routes().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf(
				"%s %s without a session returned %d, want 401",
				target.method, target.path, recorder.Code,
			)
		}
	}
}

// TestFinishedJobsAreForgottenEventually pins that a long-lived server does not
// accumulate every outcome it ever produced.
func TestFinishedJobsAreForgottenEventually(t *testing.T) {
	previous := jobRetention
	jobRetention = time.Millisecond
	defer func() { jobRetention = previous }()

	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run
	stale, _ := server.jobs.start(app.Request{Command: "status"})
	<-command.entered
	close(command.release)
	if !waitFor(func() bool { _, ok := stale.result(); return ok }) {
		t.Fatal("job never finished")
	}
	time.Sleep(5 * time.Millisecond)

	// Sweeping happens when the next job starts.
	second := newBlockingCommand()
	server.jobs.execute = second.run
	if _, err := server.jobs.start(app.Request{Command: "status"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-second.entered
	close(second.release)

	if _, found := server.jobs.find(stale.ID()); found {
		t.Error("a job finished well past its retention is still held")
	}
}

// TestARunningJobIsNeverForgotten pins the other side of eviction: a sweep must
// not drop something still running, which would make it unreachable and
// uncancellable.
func TestARunningJobIsNeverForgotten(t *testing.T) {
	previous := jobRetention
	jobRetention = time.Nanosecond
	defer func() { jobRetention = previous }()

	server := newTestServer(t)
	command := newBlockingCommand()
	server.jobs.execute = command.run
	running, _ := server.jobs.start(app.Request{Command: "run"})
	<-command.entered

	time.Sleep(5 * time.Millisecond)
	second := newBlockingCommand()
	server.jobs.execute = second.run
	if _, err := server.jobs.start(app.Request{Command: "status"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	<-second.entered
	close(second.release)

	if _, found := server.jobs.find(running.ID()); !found {
		t.Fatal("a running job was evicted, so it can no longer be watched or cancelled")
	}
	close(command.release)
}

// TestAStreamFollowsAJobThatFinishesWhileItIsWatching covers the ordinary live
// case: a client attaches to a running job and is told when it ends.
//
// It does not, and cannot, prove that reading events and subscribing for more
// happen in a safe order. That gap is microseconds wide, and a version of this
// test written to guard it passed just as happily against code with the order
// reversed - a placebo. job.next removes the gap instead, by handing back the
// events and the wake-up channel from one acquisition of the lock, so there is
// no order left to get wrong.
//
// Repeated because the timing varies and one attempt shows little.
func TestAStreamFollowsAJobThatFinishesWhileItIsWatching(t *testing.T) {
	for attempt := 0; attempt < 50; attempt++ {
		server := newTestServer(t)
		command := newBlockingCommand()
		server.jobs.execute = command.run
		running, err := server.jobs.start(app.Request{Command: "run"})
		if err != nil {
			t.Fatalf("start: %v", err)
		}
		<-command.entered

		body := make(chan string, 1)
		go func() {
			request := httptest.NewRequest(
				http.MethodGet, "/api/v1/jobs/"+running.ID()+"/events", nil,
			)
			request.Header.Set("Authorization", "Bearer "+server.auth.session)
			recorder := httptest.NewRecorder()
			server.routes().ServeHTTP(recorder, request)
			body <- recorder.Body.String()
		}()

		// Long enough for the stream to have read the started event and be
		// waiting, which is the moment that matters.
		time.Sleep(time.Millisecond)
		close(command.release)

		select {
		case streamed := <-body:
			events := parseEvents(t, streamed)
			if len(events) == 0 || events[len(events)-1].Kind != eventFinished {
				t.Fatalf("attempt %d: stream ended without the finished event: %v", attempt, events)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf(
				"attempt %d: the stream never delivered the finished event; "+
					"an event emitted while it was subscribing was lost",
				attempt,
			)
		}
	}
}

// TestAJobNeverReportsEndingBeforeItsFinishedEventExists pins the invariant the
// stream loop depends on.
//
// jobEvents writes whatever next returns and then stops if it says the job has
// ended. So a moment where ended is true and the finished event is not yet
// among the events is a stream that reports the end without ever sending it -
// the operator watches a completed migration that never says how it went.
//
// Hammered concurrently because that moment, if it exists, is narrow.
func TestAJobNeverReportsEndingBeforeItsFinishedEventExists(t *testing.T) {
	for attempt := 0; attempt < 300; attempt++ {
		running := &job{
			id:      "under-test",
			changed: make(chan struct{}),
			done:    make(chan struct{}),
			cancel:  func() {},
		}
		running.emit(eventStarted, nil, retainStarted)

		violations := make(chan string, 1)
		watching := make(chan struct{})
		go func() {
			close(watching)
			for {
				events, ended, _ := running.next(0)
				if ended {
					last := ""
					if len(events) > 0 {
						last = events[len(events)-1].Kind
					}
					if last != eventFinished {
						select {
						case violations <- last:
						default:
						}
					}
					return
				}
				select {
				case <-running.done:
					// The job ended and the watcher never saw it; the final
					// read below settles whether that is a violation.
				default:
				}
			}
		}()

		<-watching
		running.complete(app.Outcome{Command: "run"})

		select {
		case last := <-violations:
			t.Fatalf(
				"attempt %d: a job reported ending while its last event was %q; "+
					"a stream would return without sending the outcome",
				attempt, last,
			)
		default:
		}
	}
}

// TestProgressReportsBecomeStreamEvents pins the last link in the chain: what
// the engine reports has to reach the client watching.
func TestProgressReportsBecomeStreamEvents(t *testing.T) {
	server := newTestServer(t)
	reported := make(chan struct{})
	server.jobs.execute = func(
		_ context.Context, request app.Request, report app.ProgressFunc,
	) app.Outcome {
		report(app.Progress{
			Kind:   app.ProgressTablesPlanned,
			Tables: []string{"orders", "customers"},
			Total:  2,
		})
		report(app.Progress{
			Kind: app.ProgressTableFinished, Table: "orders", Rows: 7, Done: 1, Total: 2,
		})
		close(reported)
		return app.Outcome{Command: request.Command}
	}

	running, err := server.jobs.start(app.Request{Command: "run"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-reported
	if !waitFor(func() bool { _, ok := running.result(); return ok }) {
		t.Fatal("job never finished")
	}

	events := streamEvents(t, server, running.ID(), 0)
	var progress []app.Progress
	for _, event := range events {
		if event.Kind != eventProgress {
			continue
		}
		var decoded app.Progress
		if err := json.Unmarshal(event.Data, &decoded); err != nil {
			t.Fatalf("progress event is not a Progress: %s", event.Data)
		}
		progress = append(progress, decoded)
	}
	if len(progress) != 2 {
		t.Fatalf("expected two progress events, got %d of %d total", len(progress), len(events))
	}
	if progress[0].Kind != app.ProgressTablesPlanned || len(progress[0].Tables) != 2 {
		t.Errorf("the planned report lost its table set: %+v", progress[0])
	}
	if progress[1].Table != "orders" || progress[1].Rows != 7 || progress[1].Done != 1 {
		t.Errorf("the finished report lost its detail: %+v", progress[1])
	}
	// Ordering matters: a client renders these as they arrive.
	if events[0].Kind != eventStarted || events[len(events)-1].Kind != eventFinished {
		t.Errorf("progress did not arrive between started and finished: %v", events)
	}
}

// TestTheEventBufferIsBounded pins that a migration with a great many tables
// does not hold every report it ever made for an hour after finishing.
func TestTheEventBufferIsBounded(t *testing.T) {
	running := &job{
		id:      "under-test",
		changed: make(chan struct{}),
		done:    make(chan struct{}),
		cancel:  func() {},
	}
	running.emit(eventStarted, nil, retainStarted)
	for index := 0; index < maxRetainedEvents*2; index++ {
		running.emit(eventProgress, nil, trimmable)
	}

	events, _, _ := running.next(0)
	if len(events) > maxRetainedEvents {
		t.Errorf("the buffer holds %d events, over the cap of %d", len(events), maxRetainedEvents)
	}
	// The started event survives trimming: it is what tells a late arrival
	// which command it is watching.
	if events[0].Kind != eventStarted {
		t.Errorf("trimming dropped the started event; the buffer begins with %s", events[0].Kind)
	}
	// Sequences stay strictly increasing, or a resuming client would be sent
	// events it has already seen.
	for index := 1; index < len(events); index++ {
		if events[index].Sequence <= events[index-1].Sequence {
			t.Fatalf(
				"sequences repeat after trimming: %d then %d",
				events[index-1].Sequence, events[index].Sequence,
			)
		}
	}
}

// streamEvents reads a job's stream to its end.
func streamEvents(t *testing.T, server *Server, id string, from int) []jobEvent {
	t.Helper()
	target := "/api/v1/jobs/" + id + "/events"
	if from > 0 {
		target += "?from=" + strconv.Itoa(from)
	}
	request := httptest.NewRequest(http.MethodGet, target, nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("stream returned %d: %s", recorder.Code, recorder.Body)
	}
	return parseEvents(t, recorder.Body.String())
}

// parseEvents reads server-sent events out of a response body.
func parseEvents(t *testing.T, body string) []jobEvent {
	t.Helper()
	var events []jobEvent
	var current jobEvent
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if current.Kind != "" {
				events = append(events, current)
			}
			current = jobEvent{}
		case strings.HasPrefix(line, "id: "):
			sequence, err := strconv.Atoi(strings.TrimPrefix(line, "id: "))
			if err != nil {
				t.Fatalf("event id is not a number: %q", line)
			}
			current.Sequence = sequence
		case strings.HasPrefix(line, "event: "):
			current.Kind = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			current.Data = json.RawMessage(strings.TrimPrefix(line, "data: "))
		}
	}
	return events
}

func statusOf(t *testing.T, server *Server, id string) map[string]any {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/jobs/"+id, nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status returned %d", recorder.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	return body
}

// TestTrimmingKeepsThePlannedTableSet is the claim in docs/STAGE5_DESIGN.md
// stated where it can fail: "a client that missed events can still render
// correctly from one recent event".
//
// That was true of Done and Total, which every report restates, and false of
// the planned table set, which is sent once. Trimming kept events[0] and
// nothing else, so on any migration wide enough to trim - about a thousand
// tables, since each emits two events - a reconnecting client got the run's
// tail with no idea which tables were in it.
//
// Driven through reportProgress rather than emit, because the decision this
// protects is the one reportProgress makes. Calling emit directly would pass
// with that decision deleted.
func TestTrimmingKeepsThePlannedTableSet(t *testing.T) {
	running := &job{
		id:      "under-test",
		changed: make(chan struct{}),
		done:    make(chan struct{}),
		cancel:  func() {},
	}
	running.emit(eventStarted, nil, retainStarted)

	planned := []string{"alpha", "beta", "gamma"}
	running.reportProgress(app.Progress{
		Kind:   app.ProgressTablesPlanned,
		Tables: planned,
		Total:  len(planned),
	})
	for index := 0; index < maxRetainedEvents*2; index++ {
		running.reportProgress(app.Progress{
			Kind:  app.ProgressTableFinished,
			Table: "alpha",
			Done:  1,
			Total: len(planned),
		})
	}

	events, _, _ := running.next(0)
	if len(events) > maxRetainedEvents {
		t.Errorf("the buffer holds %d events, over the cap of %d", len(events), maxRetainedEvents)
	}

	var found *app.Progress
	seen := 0
	for _, event := range events {
		if event.Kind != eventProgress {
			continue
		}
		var report app.Progress
		if err := json.Unmarshal(event.Data, &report); err != nil {
			t.Fatal(err)
		}
		if report.Kind == app.ProgressTablesPlanned {
			seen++
			found = &report
		}
	}
	if found == nil {
		t.Fatal("trimming dropped the planned table set")
	}
	// Exactly once. Held back from trimming and also still present in the tail
	// would have a client counting the same tables twice.
	if seen != 1 {
		t.Fatalf("the planned set appears %d times", seen)
	}
	if len(found.Tables) != len(planned) {
		t.Fatalf("planned set = %v, want %v", found.Tables, planned)
	}
	for index, table := range planned {
		if found.Tables[index] != table {
			t.Fatalf("planned set = %v, want %v", found.Tables, planned)
		}
	}

	// And it still arrives before the reports that depend on it.
	if events[0].Kind != eventStarted {
		t.Fatalf("the buffer begins with %s", events[0].Kind)
	}
	for index := 1; index < len(events); index++ {
		if events[index].Sequence <= events[index-1].Sequence {
			t.Fatalf(
				"sequence went backwards: %d then %d",
				events[index-1].Sequence,
				events[index].Sequence,
			)
		}
	}
}

// TestRetentionIsBoundedByLabelNotByCount pins what replaced maxAnnouncedOnce.
//
// The first version of this held announced-once events in a slice capped at a
// constant, and silently stopped retaining past it - which would reintroduce
// the very failure the retention exists to prevent, at whatever number nobody
// was watching. Labels name slots instead, so an emitter marking the same thing
// repeatedly replaces rather than accumulates, and the held-back set cannot
// outgrow the labels this package spells.
func TestRetentionIsBoundedByLabelNotByCount(t *testing.T) {
	running := &job{
		id:      "under-test",
		changed: make(chan struct{}),
		done:    make(chan struct{}),
		cancel:  func() {},
	}
	for index := 0; index < maxRetainedEvents; index++ {
		running.emit(eventProgress, nil, retainPlanned)
	}
	if len(running.kept) != 1 {
		t.Fatalf("held back %d events under one label, want 1", len(running.kept))
	}

	// The survivor is the newest, not the first. A restated announcement means
	// the earlier one is no longer true.
	if running.kept[retainPlanned].Sequence != maxRetainedEvents {
		t.Fatalf(
			"held back sequence %d, want the newest (%d)",
			running.kept[retainPlanned].Sequence,
			maxRetainedEvents,
		)
	}

	// And the trim window never collapses, however much is marked.
	for index := 0; index < maxRetainedEvents*2; index++ {
		running.emit(eventProgress, nil, trimmable)
	}
	events, _, _ := running.next(0)
	if len(events) > maxRetainedEvents {
		t.Fatalf("the buffer holds %d events, over the cap of %d", len(events), maxRetainedEvents)
	}
	for index := 1; index < len(events); index++ {
		if events[index].Sequence <= events[index-1].Sequence {
			t.Fatalf(
				"sequence went backwards: %d then %d",
				events[index-1].Sequence,
				events[index].Sequence,
			)
		}
	}
}
