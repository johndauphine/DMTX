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
		"/login?token="+server.auth.launch,
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
	request.AddCookie(&http.Cookie{Name: sessionCookie, Value: server.auth.session})
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
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
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
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
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
// TestAPIAndCLIProduceIdenticalOutcomes is the §21.1 criterion.
//
// It used to hold only where session defaults were switched off. newTestServer
// builds with Options{}, so SessionPath is empty, so sessionDefaults is empty,
// so applyTo is the identity function - and the test compared Execute(request)
// against an API that had done nothing to request. Once the console makes
// defaults routine the two surfaces legitimately diverge, and this would have
// gone on passing while saying nothing about the case that had started to
// matter.
//
// The claim is therefore stated against the resolved request: whatever the API
// would run, the command line running that same thing produces the same bytes.
// That reduces to the old assertion when no defaults are set, and is the actual
// property when some are.
//
// The subtest at the bottom is what stops this becoming the same test again: it
// asserts the configurations produce *different* bytes. Without it, a defaults
// case that silently stopped applying defaults would pass by matching a command
// line that also had none.
func TestAPIAndCLIProduceIdenticalOutcomes(t *testing.T) {
	// Paths that do not exist. Both surfaces then fail identically and the
	// failure names the path, which is what makes an applied default visible in
	// the bytes rather than something the test has to take on trust.
	const defaultConfig = "session-default.yaml"
	const defaultState = "session-default.state.db"

	configurations := []struct {
		name     string
		defaults map[string]string
	}{
		{name: "no session defaults"},
		{name: "a config default", defaults: map[string]string{SessionConfig: defaultConfig}},
		{name: "a state default", defaults: map[string]string{SessionState: defaultState}},
		{name: "both defaults", defaults: map[string]string{
			SessionConfig: defaultConfig,
			SessionState:  defaultState,
		}},
	}
	requests := []app.Request{
		{Command: "validate"},
		{Command: "preflight"},
		{Command: "status"},
		{Command: "history"},
		// Explicit paths, to pin that a default does not overwrite one. The
		// precedence is the whole reason applyTo is not a merge.
		{Command: "validate", ConfigPath: "explicit.yaml"},
		{Command: "status", StatePath: "explicit.state.db"},
	}

	// Kept so the last subtest can prove the defaults did something.
	responses := map[string][]byte{}

	for _, configuration := range configurations {
		t.Run(configuration.name, func(t *testing.T) {
			// status and history create the state database they are pointed
			// at, and these paths are relative, so without this the test
			// deposits .state.db files in the package directory and they get
			// committed. Per configuration rather than for the whole test, so
			// one configuration's artifacts cannot change another's answers -
			// while both surfaces within a configuration still share a
			// directory, which is what makes their comparison meaningful.
			t.Chdir(t.TempDir())
			server := newTestServer(t)
			for key, value := range configuration.defaults {
				if err := server.defaults.set(key, value); err != nil {
					t.Fatalf("set %s: %v", key, err)
				}
			}

			for _, request := range requests {
				name := request.Command
				if request.ConfigPath != "" || request.StatePath != "" {
					name += " with an explicit path"
				}
				t.Run(name, func(t *testing.T) {
					// The command line consults no session defaults - see
					// applyTo - so the comparison is against what the API
					// resolved the request to, not against what was sent.
					direct := app.Execute(
						context.Background(),
						server.defaults.applyTo(request),
					)
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
					httpRequest.Header.Set("Authorization", "Bearer "+server.auth.session)
					recorder := httptest.NewRecorder()
					server.routes().ServeHTTP(recorder, httpRequest)

					// The response body verbatim. Decoding and re-marshalling
					// would normalise away exactly the differences worth
					// catching - encoder settings, field order, whitespace -
					// and leave a test that claims to compare emitted bytes
					// while comparing Go values.
					actual := bytes.TrimSpace(recorder.Body.Bytes())
					expected = bytes.TrimSpace(expected)
					if string(actual) != string(expected) {
						t.Errorf(
							"surfaces disagree for %q:\n  cli: %s\n  api: %s",
							request.Command, expected, actual,
						)
					}
					responses[configuration.name+"/"+name] = actual
				})
			}
		})
	}

	// Without this the defaults cases could pass by doing nothing.
	t.Run("the defaults changed what ran", func(t *testing.T) {
		// Every lookup goes through this, because a missing key is the way
		// this subtest would fail to do its job while reporting that it had.
		// bytes.Contains(nil, x) is false and bytes.Equal(nil, x) is false, so
		// a key that was never recorded - a renamed subtest, an early
		// t.Fatalf - reads as "the default did not leak" and "the answers
		// differed". Both are the conclusion this test exists to reach, drawn
		// from no evidence.
		recorded := func(key string) []byte {
			t.Helper()
			response, ok := responses[key]
			if !ok || len(response) == 0 {
				t.Fatalf("no recorded response for %q", key)
			}
			return response
		}

		for _, command := range []string{"validate", "preflight"} {
			without := recorded("no session defaults/" + command)
			with := recorded("a config default/" + command)
			if bytes.Equal(without, with) {
				t.Errorf(
					"%q answered identically with and without a config default,"+
						" so the default case proves nothing: %s",
					command, without,
				)
			}
			if !bytes.Contains(with, []byte(defaultConfig)) {
				t.Errorf("%q did not name the default config it used: %s", command, with)
			}
		}
		for _, command := range []string{"status", "history"} {
			without := recorded("no session defaults/" + command)
			with := recorded("a state default/" + command)
			if bytes.Equal(without, with) {
				t.Errorf("%q answered identically with and without a state default", command)
			}
			// Named differently from the config case above, because the
			// evidence is different. status and history do not echo the path;
			// with no state they answer with usage and a non-zero exit, and
			// with one they succeed against a database that has no runs yet.
			// Going from refusal to success is what shows the default arrived.
			if !bytes.Contains(without, []byte(`"exit_code":1`)) {
				t.Errorf("%q without a state default did not refuse: %s", command, without)
			}
			if !bytes.Contains(with, []byte(`"exit_code":0`)) {
				t.Errorf("%q with a state default did not succeed: %s", command, with)
			}
		}
		// And an explicit path is untouched by a default that would have
		// filled it.
		explicit := recorded("both defaults/validate with an explicit path")
		if bytes.Contains(explicit, []byte(defaultConfig)) {
			t.Errorf("a session default overwrote an explicit config path: %s", explicit)
		}
		if !bytes.Contains(explicit, []byte("explicit.yaml")) {
			t.Errorf("the explicit config path is not in the answer: %s", explicit)
		}
	})
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

// TestLaunchTokenIsRedeemableOnce pins that the token in the URL really is
// single-use.
//
// It is described that way to the operator, and the description has to be true:
// a URL that stays valid is a long-lived bearer secret wherever it comes to
// rest - shell history, a pasted message, a screenshot.
func TestLaunchTokenIsRedeemableOnce(t *testing.T) {
	server := newTestServer(t)
	launch := server.auth.launch

	first := httptest.NewRecorder()
	server.routes().ServeHTTP(
		first,
		httptest.NewRequest(http.MethodGet, "/login?token="+launch, nil),
	)
	if first.Code != http.StatusFound {
		t.Fatalf("first redemption returned %d, want a redirect", first.Code)
	}

	second := httptest.NewRecorder()
	server.routes().ServeHTTP(
		second,
		httptest.NewRequest(http.MethodGet, "/login?token="+launch, nil),
	)
	if second.Code != http.StatusUnauthorized {
		t.Fatalf("launch token was redeemable twice: second attempt returned %d", second.Code)
	}
}

// TestLaunchTokenIsNotABearerCredential pins that the two secrets are separate.
// If the launch token also authenticated API calls, redeeming it once would not
// stop a leaked URL from driving migrations.
func TestLaunchTokenIsNotABearerCredential(t *testing.T) {
	server := newTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/commands", nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.launch)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("launch token authenticated an API call: %d", recorder.Code)
	}
}

// TestExecuteRejectsTrailingDocuments pins that a body holding two requests is
// refused rather than half-obeyed.
func TestExecuteRejectsTrailingDocuments(t *testing.T) {
	server := newTestServer(t)
	body := strings.NewReader(`{"command":"status"}{"command":"run"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/execute", body)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("trailing document returned %d, want 400", recorder.Code)
	}
}
