package api

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// activity records when the server was last used and how much work is running,
// so the watchdog can tell an idle server from a busy one.
type activity struct {
	mutex    sync.Mutex
	last     time.Time
	inFlight int
}

// begin marks a request as started.
func (tracker *activity) begin() {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.inFlight++
	tracker.last = time.Now()
}

// end marks a request as finished and restarts the idle clock from now, not
// from when the request arrived: a command that ran for an hour leaves the
// server freshly used, not an hour stale.
func (tracker *activity) end() {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.inFlight--
	tracker.last = time.Now()
}

// idleFor reports how long the server has gone unused.
//
// A request in flight means not idle at all, however long ago it started. This
// is the whole safety property: app.Execute runs a migration inside the
// handler, so a run lasting hours produces no second request, and a watchdog
// measuring only elapsed time would shut down the server in the middle of it.
func (tracker *activity) idleFor(now time.Time) time.Duration {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	if tracker.inFlight > 0 {
		return 0
	}
	return now.Sub(tracker.last)
}

// track counts every request, including the unauthenticated login route.
// Arriving to exchange a token is a use of the server like any other.
func (tracker *activity) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		tracker.begin()
		defer tracker.end()
		next.ServeHTTP(writer, request)
	})
}

// watchIdle stops the server once it has gone unused for the configured
// timeout. It returns when ctx is done.
//
// The timer is re-armed for exactly the time remaining rather than polled on a
// fixed interval, so a busy server costs one wakeup per timeout period and an
// idle one stops on time instead of up to an interval late.
func (server *Server) watchIdle(ctx context.Context, stop func()) {
	timer := time.NewTimer(server.idleTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			remaining := server.idleTimeout - server.activity.idleFor(time.Now())
			if remaining <= 0 {
				// A cancelled context wins even when the timer is also ready.
				// select chooses arbitrarily between ready cases, so without
				// this an operator pressing Ctrl-C at the wrong instant would
				// be told the server stopped for idleness - a plausible,
				// confident, wrong account of what just happened.
				if ctx.Err() != nil {
					return
				}
				server.exitedIdle.Store(true)
				stop()
				return
			}
			timer.Reset(remaining)
		}
	}
}
