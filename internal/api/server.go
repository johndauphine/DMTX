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
}

// Server owns the listener and the routes behind it.
type Server struct {
	listener net.Listener
	http     *http.Server
	auth     *authenticator
	url      string

	openBrowser bool
}

// New binds a loopback listener and prepares the routes. It does not serve
// until Serve is called, so a caller can read URL first and know where to point
// a browser.
func New(options Options) (*Server, error) {
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	auth := &authenticator{token: token}

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

	server := &Server{
		listener:    listener,
		auth:        auth,
		openBrowser: options.OpenBrowser,
		url: (&url.URL{
			Scheme:   "http",
			Host:     address.String(),
			Path:     "/login",
			RawQuery: url.Values{"token": {token}}.Encode(),
		}).String(),
	}
	server.http = &http.Server{
		Handler: server.routes(),
		// A migration can run for a long time, so the write timeout has to be
		// generous; the read timeout does not, because requests are small.
		ReadHeaderTimeout: 10 * time.Second,
	}
	return server, nil
}

// URL is the authenticated address to open. It carries the launch token, which
// is exchanged for a session cookie on first request.
func (server *Server) URL() string { return server.url }

// Addr is where the server is actually listening, which matters when Port was
// zero.
func (server *Server) Addr() string { return server.listener.Addr().String() }

func (server *Server) routes() http.Handler {
	mux := http.NewServeMux()
	// The login route is deliberately unauthenticated: it is where a request
	// becomes authenticated. It accepts only the launch token.
	mux.HandleFunc("GET /login", server.auth.grant)
	mux.Handle("POST /api/v1/execute", server.auth.require(
		http.HandlerFunc(server.execute),
	))
	mux.Handle("GET /api/v1/commands", server.auth.require(
		http.HandlerFunc(server.commands),
	))
	mux.Handle("GET /", server.auth.require(
		http.HandlerFunc(server.placeholder),
	))
	return mux
}

// Serve runs until ctx is cancelled, then shuts down gracefully so an in-flight
// command is not cut off mid-write.
func (server *Server) Serve(ctx context.Context) error {
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
