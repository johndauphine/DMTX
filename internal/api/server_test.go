package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/app"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	server, err := New(Options{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })
	return server
}

// TestServerBindsOnlyToLoopback pins the decision that there is no bind
// address. The supported remote path is an SSH forward, and a listener reachable
// from the network would put a console that starts destructive migrations on it.
func TestServerBindsOnlyToLoopback(t *testing.T) {
	server := newTestServer(t)
	host, _, err := net.SplitHostPort(server.Addr())
	if err != nil {
		t.Fatalf("split address: %v", err)
	}
	address := net.ParseIP(host)
	if address == nil || !address.IsLoopback() {
		t.Fatalf("server bound to %s, which is not loopback", server.Addr())
	}
}

// TestUnauthenticatedRequestsAreRefused is the reason the token exists.
//
// Binding to loopback is not an authorization boundary: any page the operator
// visits can issue requests to 127.0.0.1. If this test ever passes without a
// token, a visited web page can start a migration.
func TestUnauthenticatedRequestsAreRefused(t *testing.T) {
	server := newTestServer(t)
	for _, target := range []string{"/", "/api/v1/execute", "/api/v1/commands"} {
		method := http.MethodGet
		if strings.Contains(target, "execute") {
			method = http.MethodPost
		}
		request := httptest.NewRequest(method, target, strings.NewReader("{}"))
		recorder := httptest.NewRecorder()
		server.routes().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf(
				"%s %s without a token returned %d, want 401",
				method, target, recorder.Code,
			)
		}
	}
}

// TestWrongTokenIsRefused pins that any token will not do.
func TestWrongTokenIsRefused(t *testing.T) {
	server := newTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/login?token=not-the-token", nil)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token returned %d, want 401", recorder.Code)
	}
	if cookies := recorder.Result().Cookies(); len(cookies) != 0 {
		t.Fatalf("wrong token still set a cookie: %#v", cookies)
	}
}

// TestLoginExchangesTokenForASessionAndHidesIt pins the one-click flow: the
// launch URL carries the token, and the redirect leaves it out of the address
// bar so it does not linger in history or a shared screenshot.
func TestLoginExchangesTokenForASessionAndHidesIt(t *testing.T) {
	server := newTestServer(t)
	request := httptest.NewRequest(
		http.MethodGet,
		"/login?token="+server.auth.token,
		nil,
	)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusFound {
		t.Fatalf("login returned %d, want a redirect", recorder.Code)
	}
	if location := recorder.Header().Get("Location"); location != "/" {
		t.Fatalf("login redirected to %q, want /", location)
	}
	var session *http.Cookie
	for _, cookie := range recorder.Result().Cookies() {
		if cookie.Name == sessionCookie {
			session = cookie
		}
	}
	if session == nil {
		t.Fatal("login set no session cookie")
	}
	if !session.HttpOnly {
		t.Error("session cookie is readable by scripts")
	}
	if session.SameSite != http.SameSiteStrictMode {
		t.Error("session cookie is not SameSite=Strict, so a cross-site navigation could carry it")
	}
}

// TestSessionCookieAuthenticatesSubsequentRequests closes the loop: after
// login, ordinary requests work without the token.
func TestSessionCookieAuthenticatesSubsequentRequests(t *testing.T) {
	server := newTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/commands", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookie, Value: server.auth.token})
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("authenticated request returned %d", recorder.Code)
	}
}

// TestExecuteRejectsUnknownFields pins that a client cannot believe it asked
// for something the server ignored. A caller sending force_resume to a server
// that does not know the field must be told, not silently obeyed differently.
func TestExecuteRejectsUnknownFields(t *testing.T) {
	server := newTestServer(t)
	body := strings.NewReader(`{"command":"status","not_a_field":true}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/execute", body)
	request.Header.Set("Authorization", "Bearer "+server.auth.token)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("unknown field returned %d, want 400", recorder.Code)
	}
}

// TestFailedCommandIsNotAnHTTPError pins that a command failing is reported in
// the Outcome, not as a transport failure.
//
// Mapping exit codes onto HTTP status would make this surface re-decide what
// the engine already decided, and two surfaces that classify the same failure
// differently is exactly what the parity criterion forbids.
func TestFailedCommandIsNotAnHTTPError(t *testing.T) {
	server := newTestServer(t)
	// No config path: the command refuses.
	body := strings.NewReader(`{"command":"validate"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/execute", body)
	request.Header.Set("Authorization", "Bearer "+server.auth.token)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("a refused command returned HTTP %d; the request succeeded", recorder.Code)
	}
	var outcome app.Outcome
	if err := json.NewDecoder(recorder.Body).Decode(&outcome); err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	if outcome.ExitCode == 0 {
		t.Fatal("refused command reported success")
	}
	if len(outcome.Messages) == 0 {
		t.Fatal("refused command carried no explanation")
	}
}

// TestAPIAndCLIProduceIdenticalOutcomes is Stage 5's exit criterion made real.
//
// Section 21.1 requires that identical command requests produce identical
// orchestration outcomes across surfaces. This compares the serialised Outcome
// the API returns against the one the CLI path produces for the same Request -
// bytes, not Go values, because bytes are what each surface actually emits.
func TestAPIAndCLIProduceIdenticalOutcomes(t *testing.T) {
	server := newTestServer(t)
	for _, request := range []app.Request{
		{Command: "validate"},
		{Command: "preflight"},
		{Command: "status"},
		{Command: "history"},
	} {
		t.Run(request.Command, func(t *testing.T) {
			direct := app.Execute(context.Background(), request)
			expected, err := json.Marshal(direct)
			if err != nil {
				t.Fatalf("marshal direct outcome: %v", err)
			}

			body, err := json.Marshal(request)
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			httpRequest := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/execute",
				bytes.NewReader(body),
			)
			httpRequest.Header.Set("Authorization", "Bearer "+server.auth.token)
			recorder := httptest.NewRecorder()
			server.routes().ServeHTTP(recorder, httpRequest)

			var served app.Outcome
			if err := json.NewDecoder(recorder.Body).Decode(&served); err != nil {
				t.Fatalf("decode served outcome: %v", err)
			}
			actual, err := json.Marshal(served)
			if err != nil {
				t.Fatalf("marshal served outcome: %v", err)
			}
			if string(actual) != string(expected) {
				t.Errorf(
					"surfaces disagree for %q:\n  cli: %s\n  api: %s",
					request.Command, expected, actual,
				)
			}
		})
	}
}

// TestServeStopsWhenContextIsCancelled pins that the server shuts down rather
// than leaking a listener, which matters because exit-when-idle will depend on
// it.
func TestServeStopsWhenContextIsCancelled(t *testing.T) {
	server := newTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve returned %v after cancellation", err)
	}
}

// TestParseArgumentsRefusesABindAddress pins the decision that there is no way
// to ask for a non-loopback listener, so exposure cannot be a mistyped flag.
func TestParseArgumentsRefusesABindAddress(t *testing.T) {
	for _, args := range [][]string{
		{"--addr", "0.0.0.0:8484"},
		{"--bind", "0.0.0.0"},
		{"--host", "0.0.0.0"},
	} {
		if _, ok := parseArguments(args); ok {
			t.Errorf("serve accepted %v; there must be no way to bind off loopback", args)
		}
	}
}
