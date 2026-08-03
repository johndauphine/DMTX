package api

import (
	"bytes"
	"context"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// startTestServer runs a real server and records it, which is the state a
// second invocation of dmtx serve would find.
func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	server, err := New(Options{})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve returned %v", err)
		}
	})

	path := filepath.Join(t.TempDir(), "serve.json")
	if err := server.recordInstance(path); err != nil {
		t.Fatalf("record instance: %v", err)
	}
	return server, path
}

// portOf extracts the port from a test server's URL.
func portOf(t *testing.T, raw string) int {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse %q: %v", raw, err)
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil {
		t.Fatalf("port of %q: %v", raw, err)
	}
	return port
}

// recordState writes a state file naming a port and secret, standing in for
// whatever a previous instance left behind.
func recordState(t *testing.T, port int, secret string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "serve.json")
	if err := writeInstanceState(path, instanceState{Port: port, Secret: secret}); err != nil {
		t.Fatalf("write state: %v", err)
	}
	return path
}

// TestHandoffSendsASecondInvocationToTheRunningServer is the feature end to
// end: the URL a handoff produces must land an authenticated console on the
// server that was already running.
func TestHandoffSendsASecondInvocationToTheRunningServer(t *testing.T) {
	server, path := startTestServer(t)

	target, handedOff := handOff(path, 0)
	if !handedOff {
		t.Fatal("handoff declined a server that was running and recorded")
	}
	if !strings.Contains(target, server.Addr()) {
		t.Errorf("handoff produced %q, which is not the running server at %s", target, server.Addr())
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookie jar: %v", err)
	}
	client := &http.Client{Jar: jar}
	response, err := client.Get(target)
	if err != nil {
		t.Fatalf("follow the handoff URL: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf(
			"the handoff URL ended at %d; a second invocation would open a "+
				"browser that is not logged in",
			response.StatusCode,
		)
	}
}

// TestHandoffMintsAFreshTokenRatherThanReusingTheOriginal pins that a handoff
// does not resurrect the launch token printed at startup, which may already
// have been redeemed.
func TestHandoffMintsAFreshTokenRatherThanReusingTheOriginal(t *testing.T) {
	server, path := startTestServer(t)
	original := server.auth.launch

	target, handedOff := handOff(path, 0)
	if !handedOff {
		t.Fatal("handoff declined a running server")
	}
	if strings.Contains(target, original) {
		t.Error("handoff handed back the original launch token")
	}
	if server.auth.launch == original {
		t.Error("handoff did not replace the launch token")
	}
}

// TestHandoffTokenIsStillSingleUse pins that the property survives reminting: a
// handoff URL is as short-lived as the one printed at startup.
func TestHandoffTokenIsStillSingleUse(t *testing.T) {
	_, path := startTestServer(t)
	target, handedOff := handOff(path, 0)
	if !handedOff {
		t.Fatal("handoff declined a running server")
	}

	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	first, err := client.Get(target)
	if err != nil {
		t.Fatalf("first use: %v", err)
	}
	_ = first.Body.Close()
	if first.StatusCode != http.StatusFound {
		t.Fatalf("first use of the handoff URL returned %d", first.StatusCode)
	}

	second, err := client.Get(target)
	if err != nil {
		t.Fatalf("second use: %v", err)
	}
	_ = second.Body.Close()
	if second.StatusCode != http.StatusUnauthorized {
		t.Fatalf("a handoff URL was reusable: second use returned %d", second.StatusCode)
	}
}

// TestHandoffRefusesAnImpostorServer is the reason the secret never crosses the
// wire.
//
// The recorded port belongs to this instance only while it is alive. Once it
// dies, anyone on the machine - including another account - can bind that port.
// A handoff that trusted whatever answered would open the operator's browser at
// an attacker's page, on loopback, looking exactly like the real console. The
// impostor here replies in the right shape with a plausible token; only the
// proof gives it away.
func TestHandoffRefusesAnImpostorServer(t *testing.T) {
	impostor := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			writeJSON(writer, http.StatusOK, handoffReply{
				Proof: strings.Repeat("00", 32),
				Token: "a-token-the-attacker-chose",
			})
		},
	))
	defer impostor.Close()

	path := recordState(t, portOf(t, impostor.URL), "the-real-secret")

	target, handedOff := handOff(path, 0)
	if handedOff {
		t.Fatalf(
			"handoff accepted a server that could not prove it holds the "+
				"secret, and would have opened a browser at %s",
			target,
		)
	}
}

// TestTheHandoffSecretNeverCrossesTheWire pins the property the whole handshake
// exists for. If the secret were sent, an impostor on the recorded port would
// simply be handed it.
func TestTheHandoffSecretNeverCrossesTheWire(t *testing.T) {
	const secret = "6f7a1c88b2e94d55a0f3e7c1d9b64a2e"
	var captured bytes.Buffer
	listener := httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			_, _ = io.Copy(&captured, request.Body)
			writer.WriteHeader(http.StatusUnauthorized)
		},
	))
	defer listener.Close()

	path := recordState(t, portOf(t, listener.URL), secret)
	_, _ = handOff(path, 0)

	if captured.Len() == 0 {
		t.Fatal("no handoff request was sent, so this proves nothing")
	}
	if strings.Contains(captured.String(), secret) {
		t.Fatalf("the handoff request carried the secret: %s", captured.String())
	}
}

// TestServerRefusesAHandoffWithoutTheSecret pins the other direction: a caller
// that cannot prove itself gets no token.
func TestServerRefusesAHandoffWithoutTheSecret(t *testing.T) {
	server := newTestServer(t)
	nonce, err := newToken()
	if err != nil {
		t.Fatalf("nonce: %v", err)
	}

	for name, asked := range map[string]handoffRequest{
		"no proof":     {Nonce: nonce},
		"wrong proof":  {Nonce: nonce, Proof: strings.Repeat("00", 32)},
		"wrong secret": {Nonce: nonce, Proof: proof("not-the-secret", clientProofLabel, nonce)},
		// The reply's proof replayed as a request. Without distinct labels this
		// would authenticate, because both sides would be signing the same
		// bytes with the same key.
		"server proof replayed": {
			Nonce: nonce,
			Proof: proof(server.handoffSecret, serverProofLabel, nonce),
		},
	} {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(asked)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			request := httptest.NewRequest(
				http.MethodPost, "/api/v1/handoff", bytes.NewReader(body),
			)
			recorder := httptest.NewRecorder()
			server.routes().ServeHTTP(recorder, request)

			if recorder.Code != http.StatusUnauthorized {
				t.Fatalf("returned %d, want 401", recorder.Code)
			}
			if strings.Contains(recorder.Body.String(), "token") {
				t.Errorf("a refused handoff still returned a token: %s", recorder.Body)
			}
		})
	}
}

// TestHandoffRejectsAShortNonce pins that a caller cannot pick a nonce small
// enough to steer the handshake onto a value it has seen answered.
func TestHandoffRejectsAShortNonce(t *testing.T) {
	server := newTestServer(t)
	asked := handoffRequest{Nonce: "abc"}
	asked.Proof = proof(server.handoffSecret, clientProofLabel, asked.Nonce)
	body, err := json.Marshal(asked)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/handoff", bytes.NewReader(body))
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("a three-character nonce returned %d, want 400", recorder.Code)
	}
}

// TestStaleStateIsForgotten pins that a record of a server that is gone does
// not make every future start probe a dead port.
func TestStaleStateIsForgotten(t *testing.T) {
	// A port that is definitely closed: bind one, learn its number, release it.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	path := recordState(t, port, "whatever-the-dead-server-used")
	if _, handedOff := handOff(path, 0); handedOff {
		t.Fatal("handoff claimed to reach a server on a closed port")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the stale state file survived: %v", err)
	}
}

// TestHandoffDeclinesWhenAnExplicitPortDisagrees pins that asking for a
// specific port is a request for a server there, not a request to be sent
// somewhere else.
func TestHandoffDeclinesWhenAnExplicitPortDisagrees(t *testing.T) {
	server, path := startTestServer(t)
	running := portOf(t, "http://"+server.Addr())

	if _, handedOff := handOff(path, running+1); handedOff {
		t.Error("handoff ignored an explicit port and sent the operator elsewhere")
	}
	if _, handedOff := handOff(path, running); !handedOff {
		t.Error("handoff declined even though the explicit port matched")
	}
}

// TestStateFileIsNotReadableByOtherAccounts pins the file mode. The handshake
// is what makes a leaked secret useless, but leaving a credential
// world-readable is not a habit worth keeping either way.
func TestStateFileIsNotReadableByOtherAccounts(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	path := recordState(t, 8484, "a-secret")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("state file mode is %04o; it is readable beyond its owner", mode)
	}
}

// TestRecordingOverAnExistingFileDoesNotInheritItsMode pins the case the
// mode test above cannot reach.
//
// A mode argument applies only when a file is created, so writing over a
// serve.json left world-readable by an earlier version - or by a hand edit -
// would silently keep that mode. TestStateFileIsNotReadableByOtherAccounts
// writes into an empty directory and so never exercises this at all.
func TestRecordingOverAnExistingFileDoesNotInheritItsMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	path := filepath.Join(t.TempDir(), "serve.json")
	if err := os.WriteFile(path, []byte(`{"port":1,"secret":"old"}`), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := writeInstanceState(path, instanceState{Port: 2, Secret: "new"}); err != nil {
		t.Fatalf("write: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf(
			"rewriting an existing state file left it %04o; the secret is "+
				"readable by other accounts",
			mode,
		)
	}
	recorded, found := readInstanceState(path)
	if !found || recorded.Port != 2 {
		t.Errorf("the rewrite did not take: %+v found=%v", recorded, found)
	}
}

// TestRecordingLeavesNoTemporaryFilesBehind pins that the write-and-rename does
// not litter the operator's config directory.
func TestRecordingLeavesNoTemporaryFilesBehind(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "serve.json")
	for attempt := 0; attempt < 3; attempt++ {
		if err := writeInstanceState(path, instanceState{Port: 8484, Secret: "s"}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatalf("read dir: %v", err)
	}
	if len(entries) != 1 {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("expected only serve.json, found %v", names)
	}
}

// TestHandoffDoesNotClaimToOpenABrowserItWillNotOpen pins that the handoff
// message matches what happens.
//
// Only the --no-browser path is driven here: the other one launches a real
// browser, which a test suite has no business doing.
func TestHandoffDoesNotClaimToOpenABrowserItWillNotOpen(t *testing.T) {
	_, path := startTestServer(t)
	redirectStatePath(t, path)

	var out bytes.Buffer
	if code := RunCommand([]string{"--no-browser"}, &out, io.Discard); code != success {
		t.Fatalf("handoff returned %d", code)
	}

	if strings.Contains(strings.ToLower(out.String()), "opening") {
		t.Errorf("--no-browser handoff said it was opening a browser: %q", out.String())
	}
	// It still has to hand over the URL, or --no-browser leaves the operator
	// with nothing to act on.
	if !strings.Contains(out.String(), "/login?token=") {
		t.Errorf("--no-browser handoff printed no usable URL: %q", out.String())
	}
}

// TestReadInstanceStateRefusesNonsense pins that a damaged or hand-edited file
// means "there is nobody to hand off to" rather than a crash or a probe of
// something arbitrary.
func TestReadInstanceStateRefusesNonsense(t *testing.T) {
	directory := t.TempDir()
	for name, contents := range map[string]string{
		"empty":         "",
		"not json":      "{{{",
		"no port":       `{"secret":"abc"}`,
		"no secret":     `{"port":8484}`,
		"port zero":     `{"port":0,"secret":"abc"}`,
		"port too high": `{"port":70000,"secret":"abc"}`,
		"port negative": `{"port":-1,"secret":"abc"}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(directory, "serve.json")
			if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			if _, found := readInstanceState(path); found {
				t.Errorf("accepted %q as a usable instance", contents)
			}
		})
	}

	if _, found := readInstanceState(filepath.Join(directory, "absent.json")); found {
		t.Error("a missing state file was read as a usable instance")
	}
}

// redirectStatePath points the handoff machinery at a temporary file for the
// duration of a test. Tests using it must not call t.Parallel.
func redirectStatePath(t *testing.T, path string) {
	t.Helper()
	previous := statePath
	statePath = func() (string, error) { return path, nil }
	t.Cleanup(func() { statePath = previous })
}

// TestForgetInstanceLeavesAnotherServersRecordAlone pins that a server cleans
// up after itself and nobody else.
//
// Removing a record written by a different server strands it: it keeps running
// and serving, but nothing can find it, so the next invocation starts a third
// server instead of handing off to the first.
func TestForgetInstanceLeavesAnotherServersRecordAlone(t *testing.T) {
	server := newTestServer(t)
	path := recordState(t, 65000, "a-secret-belonging-to-someone-else")

	server.forgetInstance(path)

	recorded, found := readInstanceState(path)
	if !found {
		t.Fatal("a server removed a record it did not write")
	}
	if recorded.Port != 65000 {
		t.Errorf("the surviving record was altered: port %d", recorded.Port)
	}
}

// TestForgetInstanceRemovesOurOwnRecord is the other half: cleanup still has to
// happen, or every start probes a dead port.
func TestForgetInstanceRemovesOurOwnRecord(t *testing.T) {
	server := newTestServer(t)
	path := filepath.Join(t.TempDir(), "serve.json")
	if err := server.recordInstance(path); err != nil {
		t.Fatalf("record: %v", err)
	}

	server.forgetInstance(path)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("a server left its own record behind: %v", err)
	}
}

// TestServeRecordsItselfWhileRunningAndForgetsOnExit covers the lifecycle
// through the real command entry point.
func TestServeRecordsItselfWhileRunningAndForgetsOnExit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "serve.json")
	redirectStatePath(t, path)

	done := make(chan int, 1)
	go func() {
		done <- RunCommand(
			[]string{"--no-browser", "--idle-timeout", "2s"},
			io.Discard, io.Discard,
		)
	}()

	if !waitForRecord(path, true) {
		t.Fatal("a running server never recorded itself, so handoff could not find it")
	}
	if code := <-done; code != success {
		t.Fatalf("serve returned %d", code)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the record outlived the server: %v", err)
	}
}

// TestNewInstanceLeavesTheExistingRecordIntact pins the escape hatch's manners.
//
// --new-instance means "I want my own server", not "redirect everyone to me".
// A second server that claimed the record would point every later invocation at
// itself and strand the instance already running - and worse, would delete the
// record on its way out, so the survivor became unreachable by handoff
// entirely.
func TestNewInstanceLeavesTheExistingRecordIntact(t *testing.T) {
	running, path := startTestServer(t)
	redirectStatePath(t, path)
	expected := portOf(t, "http://"+running.Addr())

	code := RunCommand(
		[]string{"--new-instance", "--no-browser", "--idle-timeout", "100ms"},
		io.Discard, io.Discard,
	)
	if code != success {
		t.Fatalf("serve --new-instance returned %d", code)
	}

	recorded, found := readInstanceState(path)
	if !found {
		t.Fatal("--new-instance deleted the running instance's record")
	}
	if recorded.Port != expected {
		t.Errorf(
			"--new-instance took over the record: it names port %d, but the "+
				"instance still running is on %d",
			recorded.Port, expected,
		)
	}
}

// waitForRecord polls until a state file appears, since the server records
// itself on another goroutine.
func waitForRecord(path string, want bool) bool {
	for attempt := 0; attempt < 200; attempt++ {
		if _, found := readInstanceState(path); found == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestTheTwoProofsAreNotInterchangeable pins the domain separation directly.
func TestTheTwoProofsAreNotInterchangeable(t *testing.T) {
	const secret, nonce = "a-secret", "a-nonce-long-enough-to-be-accepted"
	if proof(secret, clientProofLabel, nonce) == proof(secret, serverProofLabel, nonce) {
		t.Fatal("the two proofs are the same value, so either can stand in for the other")
	}
}

// TestParseArgumentsReadsNewInstance covers the escape hatch.
func TestParseArgumentsReadsNewInstance(t *testing.T) {
	options, ok := parseArguments([]string{"--new-instance"})
	if !ok {
		t.Fatal("serve refused --new-instance")
	}
	if !options.NewInstance {
		t.Error("--new-instance did not set NewInstance")
	}

	if options, ok := parseArguments(nil); !ok || options.NewInstance {
		t.Error("NewInstance is set without the flag")
	}
	if _, ok := parseArguments([]string{"--new-instance", "--new-instance"}); ok {
		t.Error("serve accepted a repeated --new-instance")
	}
}

// TestUsageNamesEveryFlagParseArgumentsAccepts pins that the line an operator
// is shown after a mistake lists the flags that would actually have worked.
//
// The flag list is read out of parseArguments itself rather than restated here.
// A hand-written list would pass while both it and the usage string fell behind
// the parser, which is the drift this test exists to catch.
func TestUsageNamesEveryFlagParseArgumentsAccepts(t *testing.T) {
	accepted := flagsParseArgumentsAccepts(t)
	if len(accepted) < 4 {
		t.Fatalf(
			"found only %d flags in parseArguments (%v); the reader is broken, "+
				"not the code under test",
			len(accepted), accepted,
		)
	}
	for _, flag := range accepted {
		if !strings.Contains(usage, flag) {
			t.Errorf("parseArguments accepts %s but usage does not mention it", flag)
		}
	}
}

// flagsParseArgumentsAccepts reads the case labels of the switch in
// parseArguments, so the test cannot disagree with the parser about what a flag
// is.
func flagsParseArgumentsAccepts(t *testing.T) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "command.go", nil, 0)
	if err != nil {
		t.Fatalf("parse command.go: %v", err)
	}

	var found []string
	ast.Inspect(file, func(node ast.Node) bool {
		function, ok := node.(*ast.FuncDecl)
		if !ok || function.Name.Name != "parseArguments" {
			return true
		}
		ast.Inspect(function.Body, func(inner ast.Node) bool {
			clause, ok := inner.(*ast.CaseClause)
			if !ok {
				return true
			}
			for _, expression := range clause.List {
				literal, ok := expression.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(literal.Value)
				if err == nil && strings.HasPrefix(value, "--") {
					found = append(found, value)
				}
			}
			return true
		})
		return false
	})
	return found
}
