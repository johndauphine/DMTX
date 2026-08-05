package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/app"
)

func sessionServer(t *testing.T) (*Server, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "session.json")
	server, err := New(Options{SessionPath: path})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })
	return server, path
}

func setDefault(t *testing.T, server *Server, key, value string) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]string{"key": key, "value": value})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/session", strings.NewReader(string(body)))
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	return recorder
}

// TestADefaultFillsInWhatARequestDidNotSay is the feature: an operator should
// not retype a config path into every command.
func TestADefaultFillsInWhatARequestDidNotSay(t *testing.T) {
	server, _ := sessionServer(t)
	if code := setDefault(t, server, SessionConfig, "/projects/shop/migration.yaml").Code; code != http.StatusOK {
		t.Fatalf("setting a default returned %d", code)
	}

	filled := server.defaults.applyTo(app.Request{Command: "validate"})
	if filled.ConfigPath != "/projects/shop/migration.yaml" {
		t.Errorf("the default was not applied: %q", filled.ConfigPath)
	}

	// And through the API, which is the part that matters and the part the
	// check above does not reach. Calling applyTo directly tests the function;
	// removing the call from decodeRequest left that passing, so the wiring
	// needs its own evidence.
	//
	// validate names the file it could not read, so the outcome says whether
	// the default arrived - without needing the file to exist.
	body := strings.NewReader(`{"command":"validate"}`)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/execute", body)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)

	var outcome app.Outcome
	if err := json.Unmarshal(recorder.Body.Bytes(), &outcome); err != nil {
		t.Fatalf("decode outcome: %v", err)
	}
	said := ""
	for _, message := range outcome.Messages {
		said += message.Text + "\n"
	}
	if !strings.Contains(said, "/projects/shop/migration.yaml") {
		t.Errorf(
			"a command sent through the API did not pick up the default:\n%s"+
				"the default is applied in decodeRequest; if that call is gone, "+
				"the console fills in nothing",
			said,
		)
	}
}

// TestAnExplicitValueBeatsADefault pins the precedence, which is the part that
// matters: a request that names a file must use that file.
//
// Getting this backwards would mean a console operator who typed one config
// path watched dmtx act on another, which is the worst available failure for a
// tool that drops and recreates tables.
func TestAnExplicitValueBeatsADefault(t *testing.T) {
	server, _ := sessionServer(t)
	setDefault(t, server, SessionConfig, "/the/default.yaml")
	setDefault(t, server, SessionState, "/the/default.state.db")

	filled := server.defaults.applyTo(app.Request{
		Command:    "validate",
		ConfigPath: "/what/they/asked/for.yaml",
	})
	if filled.ConfigPath != "/what/they/asked/for.yaml" {
		t.Errorf(
			"a default overrode an explicit config: %q\n"+
				"an operator would watch dmtx act on a file they did not name",
			filled.ConfigPath,
		)
	}
	// The unnamed one is still filled, or the precedence is all-or-nothing
	// rather than per-field.
	if filled.StatePath != "/the/default.state.db" {
		t.Errorf("the state default was not applied: %q", filled.StatePath)
	}
}

// TestTheCommandLineIgnoresSessionDefaults pins that defaults never reach the
// command line.
//
// dmtx run is destructive. A run whose target depends on state set an hour ago
// in a browser cannot be reviewed by reading the command, which is why the
// filling happens in the API layer and nowhere else.
func TestTheCommandLineIgnoresSessionDefaults(t *testing.T) {
	server, path := sessionServer(t)
	setDefault(t, server, SessionConfig, "/the/default.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("the default was not persisted, so this proves nothing: %v", err)
	}

	// app.Execute is what the command line calls, and it has no way to reach
	// these: they live in internal/api. The import boundary is the guarantee,
	// so this asserts the boundary rather than a behaviour.
	//
	// Every file in the package, not just app.go. internal/app is a dozen
	// files; scanning one of them would pass while another referenced these,
	// which is the assertion looking thorough and holding almost nothing.
	entries, err := os.ReadDir("../app")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") ||
			strings.HasSuffix(name, "_test.go") {
			continue
		}
		source, err := os.ReadFile(filepath.Join("../app", name))
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		for _, forbidden := range []string{"sessionDefaults", "SessionConfig", "SessionState"} {
			if strings.Contains(string(source), forbidden) {
				t.Errorf(
					"internal/app/%s refers to %s; the command line would then "+
						"act on state that does not appear in the command",
					name, forbidden,
				)
			}
		}
	}
	if scanned < 5 {
		t.Fatalf("scanned only %d files in internal/app; the reader is broken", scanned)
	}
}

// TestDefaultsSurviveTheServerStopping is why these are on disk rather than in
// memory, where DMT keeps them.
//
// dmtx stops itself when idle after thirty minutes. An in-memory default would
// evaporate over a lunch break, and nothing would say so.
func TestDefaultsSurviveTheServerStopping(t *testing.T) {
	server, path := sessionServer(t)
	setDefault(t, server, SessionConfig, "/projects/shop/migration.yaml")

	// A second server over the same file, as a restart or a handoff would be.
	restarted := newSessionDefaults(path)
	if got := restarted.get(SessionConfig); got != "/projects/shop/migration.yaml" {
		t.Errorf("a default did not survive a restart: %q", got)
	}
}

// TestAnUnknownKeyIsRefusedAndTheKnownOnesNamed pins that this is a closed set
// rather than a key-value store.
//
// An accepted-then-ignored key is the failure worth avoiding: the operator sets
// it, sees success, and wonders why nothing changed.
func TestAnUnknownKeyIsRefusedAndTheKnownOnesNamed(t *testing.T) {
	server, _ := sessionServer(t)
	recorder := setDefault(t, server, "confgi", "/typo.yaml")
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("an unknown key returned %d, want 404", recorder.Code)
	}
	var answer struct {
		Error string   `json:"error"`
		Known []string `json:"known"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(answer.Known) == 0 {
		t.Error("the refusal does not name the keys that would have worked")
	}
	for _, key := range answer.Known {
		if _, real := sessionKeys[key]; !real {
			t.Errorf("the refusal names %q, which is not a key either", key)
		}
	}
}

// TestClearingADefaultForgetsIt pins removal, including from disk.
func TestClearingADefaultForgetsIt(t *testing.T) {
	server, path := sessionServer(t)
	setDefault(t, server, SessionConfig, "/projects/shop/migration.yaml")

	request := httptest.NewRequest(http.MethodDelete, "/api/v1/session/"+SessionConfig, nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("clearing returned %d", recorder.Code)
	}
	if got := server.defaults.get(SessionConfig); got != "" {
		t.Errorf("the default is still set: %q", got)
	}
	if reloaded := newSessionDefaults(path); reloaded.get(SessionConfig) != "" {
		t.Error("the default came back after a restart, so it was not cleared on disk")
	}
}

// TestAKeyRemovedFromDmtxIsNotHonouredOnLoad pins that the closed set is
// enforced when reading too.
//
// A file written by a later dmtx, or edited by hand, must not reintroduce a key
// this build does not act on - it would sit in the settings list looking
// effective.
func TestAKeyRemovedFromDmtxIsNotHonouredOnLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(path, []byte(
		`{"config":"/real.yaml","from-a-later-dmtx":"/not-honoured.yaml"}`,
	), 0o600); err != nil {
		t.Fatal(err)
	}

	loaded := newSessionDefaults(path)
	if loaded.get(SessionConfig) != "/real.yaml" {
		t.Errorf("a known key was dropped: %q", loaded.get(SessionConfig))
	}
	if _, present := loaded.all()["from-a-later-dmtx"]; present {
		t.Error("a key this dmtx does not honour was loaded and would look effective")
	}
}

// TestADamagedFileIsNoDefaultsRatherThanAnError pins that a convenience cannot
// stop a console starting.
func TestADamagedFileIsNoDefaultsRatherThanAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.json")
	if err := os.WriteFile(path, []byte("{{{ not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if loaded := newSessionDefaults(path); len(loaded.all()) != 0 {
		t.Error("a damaged file produced defaults")
	}
	// And a server still starts.
	server, err := New(Options{SessionPath: path})
	if err != nil {
		t.Fatalf("a damaged session file stopped the server starting: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })
}

// TestTheSessionFileIsNotWorldReadable pins the mode. It holds paths rather
// than credentials, but those paths describe an operator's estate.
func TestTheSessionFileIsNotWorldReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	server, path := sessionServer(t)
	setDefault(t, server, SessionConfig, "/projects/shop/migration.yaml")

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the session file is %04o", mode)
	}
}

// TestSessionRoutesRequireAuthentication pins that they sit behind the session
// like everything else.
func TestSessionRoutesRequireAuthentication(t *testing.T) {
	server, _ := sessionServer(t)
	for _, target := range []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/api/v1/session"},
		{http.MethodPost, "/api/v1/session"},
		{http.MethodDelete, "/api/v1/session/config"},
	} {
		request := httptest.NewRequest(target.method, target.path, strings.NewReader("{}"))
		recorder := httptest.NewRecorder()
		server.routes().ServeHTTP(recorder, request)
		if recorder.Code != http.StatusUnauthorized {
			t.Errorf("%s %s returned %d, want 401", target.method, target.path, recorder.Code)
		}
	}
}

// TestSessionListingDescribesEveryKey pins that a console can build its own
// settings list from the registry rather than hard-coding one that drifts.
func TestSessionListingDescribesEveryKey(t *testing.T) {
	server, _ := sessionServer(t)
	setDefault(t, server, SessionConfig, "/set.yaml")

	request := httptest.NewRequest(http.MethodGet, "/api/v1/session", nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("listing returned %d", recorder.Code)
	}

	var answer struct {
		Defaults []struct {
			Key         string `json:"key"`
			Description string `json:"description"`
			Value       string `json:"value"`
		} `json:"defaults"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(answer.Defaults) != len(sessionKeys) {
		t.Fatalf("listed %d keys, want every one of %d", len(answer.Defaults), len(sessionKeys))
	}
	for _, described := range answer.Defaults {
		if described.Description == "" {
			t.Errorf("the key %q is listed with no description to show", described.Key)
		}
	}
}
