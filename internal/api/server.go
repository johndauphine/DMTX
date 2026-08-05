// Package api serves the Outcome seam over HTTP so a browser front end can
// drive the same commands the command line does.
//
// It consumes internal/app and never internal/migrate. Stage 5's rule is that a
// surface presents the facts the engine decided rather than re-deriving them,
// and an import-boundary test in internal/app enforces it.
package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

// Options configures the server. There is deliberately no bind-address field.
//
// The supported remote path is an SSH forward:
//
//	ssh -L 8484:localhost:8484 dbhost
//
// which reaches a loopback listener without exposing a port. Offering a bind
// address would make exposing a migration console a one-flag mistake, and a
// config file carrying one gets copied. Anyone who genuinely needs remote
// exposure can put a reverse proxy in front, which is a decision they make and
// audit rather than one this tool enables by default.
type Options struct {
	// Port to listen on. Zero asks the operating system for a free one, which
	// is what makes a second instance possible without a port conflict.
	Port int

	// OpenBrowser launches the operator's browser at the authenticated URL once
	// the listener is up.
	OpenBrowser bool

	// IdleTimeout stops the server once it has gone this long without a
	// request. Zero disables it, which is what a caller that never wants an
	// unattended shutdown asks for.
	//
	// A command in flight is never idle, so this cannot end a running
	// migration; see activity.idleFor.
	IdleTimeout time.Duration

	// NewInstance starts a server even when one is already running, instead of
	// handing the operator to it. The escape hatch matters because handoff is a
	// guess about intent, and a guess with no way to override it is a trap.
	NewInstance bool

	// SessionPath is where console defaults are remembered. Empty keeps them in
	// memory only, which is what a caller that is not the serve command wants.
	SessionPath string

	// Root confines @ path completion. Empty disables completion rather than
	// widening it: an endpoint that enumerates the filesystem is worth having
	// only when someone has said which part of it.
	Root string

	// configPath names the migration project this console is for. Today it only
	// supplies Root's default - serve does not load it - so it is unexported
	// and resolved by RunCommand rather than offered as a knob that promises
	// more than it does.
	configPath string
}

// Server owns the listener and the routes behind it.
type Server struct {
	listener net.Listener
	http     *http.Server
	auth     *authenticator
	url      string

	openBrowser bool
	idleTimeout time.Duration
	activity    activity

	// root bounds @ path completion. Nil means completion is off.
	root *pathRoot

	// defaults are the paths a console operator would otherwise retype into
	// every command. Held here rather than in internal/app because the command
	// line deliberately does not consult them.
	defaults *sessionDefaults

	// jobs holds every command this server has started. Commands run here
	// rather than inside a handler so that losing the client does not lose the
	// work.
	jobs *jobs

	// handoffSecret authenticates the handoff handshake. It is a third secret,
	// separate from the launch and session values, because it authenticates a
	// different party for a different purpose: another process on this machine
	// asking to be let in, not a browser that is already in.
	handoffSecret string

	// exitedIdle records that the watchdog, not the operator, stopped the
	// server, so the terminal can say why rather than silently returning to a
	// prompt.
	exitedIdle atomic.Bool
}

// New binds a loopback listener and prepares the routes. It does not serve
// until Serve is called, so a caller can read URL first and know where to point
// a browser.
func New(options Options) (*Server, error) {
	launch, err := newToken()
	if err != nil {
		return nil, err
	}
	session, err := newToken()
	if err != nil {
		return nil, err
	}
	auth := &authenticator{launch: launch, session: session}

	// Explicitly 127.0.0.1 rather than localhost: the name can resolve to an
	// interface that is not loopback, and this listener must never be one.
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", options.Port))
	if err != nil {
		return nil, fmt.Errorf("listen on loopback: %w", err)
	}
	address, ok := listener.Addr().(*net.TCPAddr)
	if !ok || !address.IP.IsLoopback() {
		_ = listener.Close()
		return nil, errors.New(
			"refusing to serve: listener is not bound to loopback",
		)
	}

	handoffSecret, err := newToken()
	if err != nil {
		_ = listener.Close()
		return nil, err
	}

	server := &Server{
		listener:      listener,
		auth:          auth,
		openBrowser:   options.OpenBrowser,
		idleTimeout:   options.IdleTimeout,
		handoffSecret: handoffSecret,
		url:           loginURL(address.Port, launch),
	}

	// A root that will not resolve leaves completion off. Refusing to serve
	// would be worse - the operator asked for a console, and completion is one
	// convenience within it - but silently completing against somewhere else
	// would be worse still, so the caller is told; see RunCommand.
	if options.Root != "" {
		if root, err := newPathRoot(options.Root); err == nil {
			server.root = root
		}
	}
	server.jobs = newJobs(&server.activity)
	server.defaults = newSessionDefaults(options.SessionPath)

	// Started now rather than left at the zero time, or a server that has not
	// yet had its first request would look infinitely idle and the watchdog
	// would stop it before the browser finished opening.
	server.activity.last = time.Now()

	server.http = &http.Server{
		Handler: server.routes(),
		// WriteTimeout is deliberately unset. It bounds the whole exchange, so
		// any value would also be a cap on how long a migration may run, and
		// the first run that outlived it would be cut off mid-write.
		//
		// The read side has no such constraint: requests are a handful of short
		// strings, bounded again at 64KB by maxRequestBytes.
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		// Bounds a kept-alive connection between requests, so a client that
		// opens sockets and goes quiet does not hold them for the life of the
		// process. Unrelated to Options.IdleTimeout, which stops the server
		// itself.
		IdleTimeout: 120 * time.Second,
	}
	return server, nil
}

// loginURL is the one place a launch URL is built, so the address a handoff
// sends an operator to cannot drift from the one a fresh server prints.
//
// 127.0.0.1 rather than localhost, for the same reason the listener uses it:
// the name can resolve somewhere that is not loopback.
func loginURL(port int, token string) string {
	return (&url.URL{
		Scheme:   "http",
		Host:     fmt.Sprintf("127.0.0.1:%d", port),
		Path:     "/login",
		RawQuery: url.Values{"token": {token}}.Encode(),
	}).String()
}

// URL is the authenticated address to open. It carries the launch token, which
// is exchanged for a session cookie on first request.
func (server *Server) URL() string { return server.url }

// Addr is where the server is actually listening, which matters when Port was
// zero.
func (server *Server) Addr() string { return server.listener.Addr().String() }

// CompletionRoot is the directory @ completion may enumerate, or empty when
// completion is off.
func (server *Server) CompletionRoot() string {
	if server.root == nil {
		return ""
	}
	return server.root.resolved
}

// ExitedIdle reports whether Serve returned because the server went unused
// rather than because it was asked to stop. Read after Serve returns.
func (server *Server) ExitedIdle() bool { return server.exitedIdle.Load() }

func (server *Server) routes() http.Handler {
	mux := http.NewServeMux()
	// The login route is deliberately unauthenticated: it is where a request
	// becomes authenticated. It accepts only the launch token.
	mux.HandleFunc("GET /login", server.auth.grant)
	// Also unauthenticated, for the same reason: it is where a second dmtx
	// process becomes authenticated. It proves itself with the handoff secret
	// instead of a session; see Server.handoff.
	mux.HandleFunc("POST /api/v1/handoff", server.handoff)
	mux.Handle("POST /api/v1/execute", server.auth.require(
		http.HandlerFunc(server.execute),
	))
	mux.Handle("GET /api/v1/commands", server.auth.require(
		http.HandlerFunc(server.commands),
	))
	mux.Handle("GET /api/v1/complete", server.auth.require(
		http.HandlerFunc(server.complete),
	))
	mux.Handle("GET /api/v1/configs", server.auth.require(
		http.HandlerFunc(server.configs),
	))
	mux.Handle("GET /api/v1/session", server.auth.require(
		http.HandlerFunc(server.session),
	))
	mux.Handle("POST /api/v1/session", server.auth.require(
		http.HandlerFunc(server.setSession),
	))
	mux.Handle("DELETE /api/v1/session/{key}", server.auth.require(
		http.HandlerFunc(server.clearSession),
	))
	mux.Handle("POST /api/v1/jobs", server.auth.require(
		http.HandlerFunc(server.startJob),
	))
	mux.Handle("GET /api/v1/jobs/{id}", server.auth.require(
		http.HandlerFunc(server.jobStatus),
	))
	mux.Handle("GET /api/v1/jobs/{id}/events", server.auth.require(
		http.HandlerFunc(server.jobEvents),
	))
	mux.Handle("POST /api/v1/jobs/{id}/cancel", server.auth.require(
		http.HandlerFunc(server.cancelJob),
	))
	mux.Handle("GET /", server.auth.require(
		http.HandlerFunc(server.placeholder),
	))
	return server.activity.track(mux)
}

// Serve runs until ctx is cancelled, then shuts down gracefully so an in-flight
// command is not cut off mid-write.
func (server *Server) Serve(ctx context.Context) error {
	// Derived so the idle watchdog can stop the server through the same path a
	// signal takes; there is one shutdown sequence, not two.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 1)
	go func() {
		err := server.http.Serve(server.listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errs <- err
	}()
	if server.openBrowser {
		go launchBrowser(server.url)
	}
	if server.idleTimeout > 0 {
		go server.watchIdle(ctx, cancel)
	}
	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx),
			10*time.Second,
		)
		defer cancel()
		if err := server.http.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return <-errs
	}
}
