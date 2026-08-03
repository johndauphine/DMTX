package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newIdleTestServer builds a server whose watchdog fires quickly, so a test
// spends milliseconds rather than the half hour an operator would.
func newIdleTestServer(t *testing.T, timeout time.Duration) *Server {
	t.Helper()
	server, err := New(Options{IdleTimeout: timeout})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })
	return server
}

// serveInBackground starts Serve and returns a channel carrying its result.
func serveInBackground(t *testing.T, server *Server, ctx context.Context) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	return done
}

// TestServerDoesNotStopWhileACommandIsRunning is the safety property the whole
// feature depends on.
//
// app.Execute runs a migration inside the HTTP handler, so a run lasting hours
// produces no second request. A watchdog that measured only elapsed time would
// shut the server down in the middle of one - and a migration killed by its own
// console is a worse failure than any convenience this feature buys. The test
// holds a request in flight for ten timeout periods and requires the server to
// still be up, then releases it and requires the server to stop.
func TestServerDoesNotStopWhileACommandIsRunning(t *testing.T) {
	const timeout = 20 * time.Millisecond
	server := newIdleTestServer(t, timeout)
	server.activity.begin() // as the tracking middleware records a live request

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := serveInBackground(t, server, ctx)

	select {
	case <-done:
		t.Fatal("server stopped while a command was still running")
	case <-time.After(10 * timeout):
	}

	server.activity.end()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server never stopped after the command finished")
	}
	if !server.ExitedIdle() {
		t.Error("server stopped but did not report that idleness was the reason")
	}
}

// TestIdleServerStopsOnItsOwn pins the feature itself: an unused server does
// not wait for a signal.
func TestIdleServerStopsOnItsOwn(t *testing.T) {
	server := newIdleTestServer(t, 20*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := serveInBackground(t, server, ctx)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("idle server never stopped")
	}
	if !server.ExitedIdle() {
		t.Error("ExitedIdle is false after an idle shutdown")
	}
}

// TestZeroIdleTimeoutNeverStops pins that the feature is genuinely disableable,
// which is what a host meant to keep serving asks for.
func TestZeroIdleTimeoutNeverStops(t *testing.T) {
	server := newIdleTestServer(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := serveInBackground(t, server, ctx)

	select {
	case <-done:
		t.Fatal("server with idle timeout disabled stopped anyway")
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned %v", err)
	}
	if server.ExitedIdle() {
		t.Error("a cancelled server reported that it stopped for idleness")
	}
}

// TestCancellationIsNotReportedAsIdleness pins that the two reasons stay
// distinguishable, because the terminal message tells the operator which
// happened.
func TestCancellationIsNotReportedAsIdleness(t *testing.T) {
	server := newIdleTestServer(t, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	done := serveInBackground(t, server, ctx)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned %v", err)
	}
	if server.ExitedIdle() {
		t.Error("a cancelled server claimed it stopped for idleness")
	}
}

// TestEveryRouteCountsAsActivity pins that the tracking middleware wraps the
// whole mux rather than some of it.
//
// A route outside the tracker would be invisible to the watchdog, so an
// operator actively using that route would watch the server stop underneath
// them. The clock is set an hour back before each request: only a request that
// actually passed through the tracker can bring it forward.
func TestEveryRouteCountsAsActivity(t *testing.T) {
	for _, route := range []struct {
		method string
		target string
		body   string
	}{
		{http.MethodGet, "/login?token=wrong", ""},
		{http.MethodGet, "/", ""},
		{http.MethodGet, "/api/v1/commands", ""},
		{http.MethodPost, "/api/v1/execute", `{"command":"status"}`},
	} {
		t.Run(route.method+" "+route.target, func(t *testing.T) {
			server := newTestServer(t)
			server.activity.last = time.Now().Add(-time.Hour)

			request := httptest.NewRequest(
				route.method, route.target, strings.NewReader(route.body),
			)
			request.Header.Set("Authorization", "Bearer "+server.auth.session)
			server.routes().ServeHTTP(httptest.NewRecorder(), request)

			if idle := server.activity.idleFor(time.Now()); idle > time.Minute {
				t.Errorf(
					"after a request the server still looked idle for %s; "+
						"this route is not behind the activity tracker",
					idle,
				)
			}
		})
	}
}

// TestRefusedRequestsStillCountAsActivity pins that authentication failures
// keep the server alive. They are someone at the keyboard getting it wrong,
// which is the opposite of an unattended machine.
func TestRefusedRequestsStillCountAsActivity(t *testing.T) {
	server := newTestServer(t)
	server.activity.last = time.Now().Add(-time.Hour)

	request := httptest.NewRequest(http.MethodGet, "/api/v1/commands", nil)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expected the request to be refused, got %d", recorder.Code)
	}
	if idle := server.activity.idleFor(time.Now()); idle > time.Minute {
		t.Errorf("a refused request did not count as activity (idle %s)", idle)
	}
}

// TestInFlightWorkIsNeverIdle pins idleFor's contract directly, since the
// watchdog is only as safe as this answer.
func TestInFlightWorkIsNeverIdle(t *testing.T) {
	tracker := &activity{last: time.Now().Add(-time.Hour)}
	if idle := tracker.idleFor(time.Now()); idle < time.Minute {
		t.Fatalf("an unused tracker reported %s idle, want about an hour", idle)
	}
	tracker.begin()
	if idle := tracker.idleFor(time.Now()); idle != 0 {
		t.Errorf("work in flight reported %s idle, want 0", idle)
	}
	tracker.end()
	if idle := tracker.idleFor(time.Now()); idle > time.Minute {
		t.Errorf("after finishing, the clock restarted at %s, want about now", idle)
	}
}

// TestTheIdleClockRestartsWhenALongCommandFinishes pins that the clock runs
// from when work ended, not from when it started.
//
// Measuring from the start would mean a migration lasting longer than the
// timeout is followed by an immediate shutdown: the run finishes, the server
// reads as hours idle, and it stops while the operator is still looking at the
// result on screen. The window in which they can act on what just happened is
// exactly the window this restart creates.
func TestTheIdleClockRestartsWhenALongCommandFinishes(t *testing.T) {
	tracker := &activity{last: time.Now()}
	tracker.begin()
	// The stamp a long-running command leaves behind: begin recorded the clock
	// when it started, and nothing has touched it for an hour since.
	tracker.last = time.Now().Add(-time.Hour)

	tracker.end()

	if idle := tracker.idleFor(time.Now()); idle > time.Minute {
		t.Fatalf(
			"a command that ran for an hour left the server looking %s idle; "+
				"it would be shut down the moment the run finished",
			idle,
		)
	}
}

// TestParseArgumentsReadsTheIdleTimeout covers the flag, including the default
// applied when it is absent.
func TestParseArgumentsReadsTheIdleTimeout(t *testing.T) {
	for _, accepted := range []struct {
		args []string
		want time.Duration
	}{
		{nil, defaultIdleTimeout},
		{[]string{"--idle-timeout", "5m"}, 5 * time.Minute},
		{[]string{"--idle-timeout", "90s"}, 90 * time.Second},
		{[]string{"--idle-timeout", "0"}, 0},
	} {
		options, ok := parseArguments(accepted.args)
		if !ok {
			t.Errorf("serve refused %v", accepted.args)
			continue
		}
		if options.IdleTimeout != accepted.want {
			t.Errorf(
				"%v gave idle timeout %s, want %s",
				accepted.args, options.IdleTimeout, accepted.want,
			)
		}
	}

	for _, refused := range [][]string{
		{"--idle-timeout"},
		{"--idle-timeout", "soon"},
		{"--idle-timeout", "30"},  // no unit: ParseDuration rejects it
		{"--idle-timeout", "-5m"}, // negative is a typo, not "off"
		{"--idle-timeout", "5m", "--idle-timeout", "10m"},
	} {
		if _, ok := parseArguments(refused); ok {
			t.Errorf("serve accepted %v", refused)
		}
	}
}
