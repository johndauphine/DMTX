package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const migrationYAML = "source:\n  type: sqlite\n  database: a.db\ntarget:\n  type: sqlite\n  database: b.db\n"

// writeFile creates a file and the directories above it.
func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// discoveryFixture builds a tree and a server rooted at it.
func discoveryFixture(t *testing.T) (*Server, string) {
	t.Helper()
	base := t.TempDir()
	root := filepath.Join(base, "project")

	writeFile(t, filepath.Join(root, "migration.yaml"), migrationYAML)
	writeFile(t, filepath.Join(root, "staging.yml"), migrationYAML)
	writeFile(t, filepath.Join(root, "envs", "prod.yaml"), migrationYAML)
	// Not a migration config: right extension, wrong contents.
	writeFile(t, filepath.Join(root, "docker-compose.yaml"), "services:\n  db:\n    image: postgres\n")
	// A comment mentioning the keys must not qualify a file that lacks them.
	writeFile(t, filepath.Join(root, "notes.yaml"), "# source: see migration.yaml\n# target: also there\n")
	// Right contents, wrong extension.
	writeFile(t, filepath.Join(root, "migration.txt"), migrationYAML)
	// Outside the root entirely.
	writeFile(t, filepath.Join(base, "elsewhere", "secret.yaml"), migrationYAML)

	server, err := New(Options{Root: root})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })
	if server.CompletionRoot() == "" {
		t.Fatal("the fixture root did not resolve, so nothing below tests anything")
	}
	return server, root
}

func discovered(t *testing.T, server *Server) []string {
	t.Helper()
	found, err := server.discoverConfigs()
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	names := make([]string, 0, len(found))
	for _, config := range found {
		names = append(names, config.Relative)
	}
	return names
}

// TestDiscoveryFindsMigrationConfigs is the feature: an operator should be able
// to pick a config without typing a path.
func TestDiscoveryFindsMigrationConfigs(t *testing.T) {
	server, _ := discoveryFixture(t)
	got := strings.Join(discovered(t, server), ",")
	want := "envs/prod.yaml,migration.yaml,staging.yml"
	if got != want {
		t.Errorf("discovered %q, want %q", got, want)
	}
}

// TestDiscoveryExcludesYAMLThatIsNotAMigration pins that the picker stays
// short. A list padded with compose files and lint configs is one an operator
// stops reading.
func TestDiscoveryExcludesYAMLThatIsNotAMigration(t *testing.T) {
	server, _ := discoveryFixture(t)
	for _, unwanted := range []string{"docker-compose.yaml", "notes.yaml", "migration.txt"} {
		for _, found := range discovered(t, server) {
			if found == unwanted {
				t.Errorf("discovery offered %s", unwanted)
			}
		}
	}
}

// TestDiscoveryNeverLeavesTheRoot pins the confinement this endpoint inherits
// from completion.
func TestDiscoveryNeverLeavesTheRoot(t *testing.T) {
	server, root := discoveryFixture(t)
	found, err := server.discoverConfigs()
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(found) == 0 {
		t.Fatal("nothing discovered, so containment proves nothing")
	}
	for _, config := range found {
		if !strings.HasPrefix(config.Path, server.CompletionRoot()) {
			t.Errorf("discovered %s, which is outside the root %s", config.Path, root)
		}
		if strings.Contains(config.Relative, "..") {
			t.Errorf("a discovered path walks upward: %s", config.Relative)
		}
		if config.Name == "secret.yaml" {
			t.Error("discovery reached a config outside the root")
		}
	}
}

// TestDiscoveryDoesNotDescendForever pins the depth bound, which is what keeps
// a console responsive when someone serves from a home directory.
func TestDiscoveryDoesNotDescendForever(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "project")
	writeFile(t, filepath.Join(root, "shallow.yaml"), migrationYAML)

	deep := root
	for level := 1; level <= maxDiscoveryDepth+2; level++ {
		deep = filepath.Join(deep, fmt.Sprintf("level%d", level))
		writeFile(t, filepath.Join(deep, "config.yaml"), migrationYAML)
	}

	server, err := New(Options{Root: root})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })

	// Both sides of the bound. Checking only that nothing came back too deep
	// lets an off-by-one through, because a walk that stopped one level early
	// satisfies it just as well as a correct one - and this test did exactly
	// that until a reviewer pointed it out.
	names := discovered(t, server)
	if len(names) == 0 {
		t.Fatal("the depth bound excluded everything, including what is in the root")
	}

	segments := make([]string, 0, maxDiscoveryDepth)
	for level := 1; level <= maxDiscoveryDepth; level++ {
		segments = append(segments, fmt.Sprintf("level%d", level))
	}
	atBound := strings.Join(segments, "/") + "/config.yaml"
	beyond := strings.Join(append(segments, fmt.Sprintf("level%d", maxDiscoveryDepth+1)), "/") + "/config.yaml"

	found := map[string]bool{}
	for _, name := range names {
		found[name] = true
		if depth := strings.Count(name, "/"); depth > maxDiscoveryDepth {
			t.Errorf("discovered %q at depth %d, past the bound of %d", name, depth, maxDiscoveryDepth)
		}
	}
	if !found[atBound] {
		t.Errorf(
			"a config at exactly the depth bound was not discovered: %q\nfound: %v",
			atBound, names,
		)
	}
	if found[beyond] {
		t.Errorf("a config past the depth bound was discovered: %q", beyond)
	}
}

// TestDiscoveryIsCapped pins that one keystroke cannot produce an unbounded
// response.
func TestDiscoveryIsCapped(t *testing.T) {
	root := t.TempDir()
	for index := 0; index < maxDiscoveredConfigs+25; index++ {
		writeFile(t, filepath.Join(root, fmt.Sprintf("config-%03d.yaml", index)), migrationYAML)
	}
	server, err := New(Options{Root: root})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })

	if names := discovered(t, server); len(names) > maxDiscoveredConfigs {
		t.Errorf("discovery returned %d configs, over the cap of %d", len(names), maxDiscoveredConfigs)
	}
}

// TestDiscoverySkipsThingsThatAreNotRegularFiles pins that a FIFO named
// migration.yaml cannot hang the walk. Reading one blocks until something else
// writes, and a console waiting on that is a console that has stopped.
func TestDiscoverySkipsThingsThatAreNotRegularFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no mkfifo on Windows")
	}
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "real.yaml"), migrationYAML)
	if err := makeFIFO(filepath.Join(root, "trap.yaml")); err != nil {
		t.Skipf("cannot create a FIFO here: %v", err)
	}

	server, err := New(Options{Root: root})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })

	// Bounded, because the failure here is a hang rather than a wrong answer:
	// opening a FIFO with no writer blocks forever. Left to the package
	// timeout this test would take ten minutes to say so, and would report a
	// timeout rather than the reason. Measured: removing the regular-file
	// check makes this fail in two seconds with the sentence below.
	type result struct {
		found []discoveredConfig
		err   error
	}
	done := make(chan result, 1)
	go func() {
		found, err := server.discoverConfigs()
		done <- result{found, err}
	}()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("discover: %v", got.err)
		}
		for _, config := range got.found {
			if config.Name == "trap.yaml" {
				t.Error("discovery offered a FIFO")
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal(
			"discovery did not finish with a FIFO in the root: it opened one " +
				"and is blocked until something writes to it, which for a " +
				"console means it has stopped answering",
		)
	}
}

// TestDiscoveryIsOffWithoutARoot pins that no root means no enumeration, the
// same way completion fails closed.
func TestDiscoveryIsOffWithoutARoot(t *testing.T) {
	server := newTestServer(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/configs", nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("discovery without a root returned %d, want 400", recorder.Code)
	}
}

// TestDiscoveryRequiresAuthentication pins that it sits behind the session,
// like every other route that reads the filesystem.
func TestDiscoveryRequiresAuthentication(t *testing.T) {
	server, _ := discoveryFixture(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/configs", nil)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("discovery without a session returned %d, want 401", recorder.Code)
	}
}

// TestDiscoveryReplyShape pins the JSON a console will read.
func TestDiscoveryReplyShape(t *testing.T) {
	server, _ := discoveryFixture(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/configs", nil)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("discovery returned %d: %s", recorder.Code, recorder.Body)
	}

	var reply struct {
		Root    string             `json:"root"`
		Configs []discoveredConfig `json:"configs"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &reply); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if reply.Root != server.CompletionRoot() {
		t.Errorf("reply names root %q, want %q", reply.Root, server.CompletionRoot())
	}
	if len(reply.Configs) == 0 {
		t.Fatal("reply carried no configs")
	}
	for _, config := range reply.Configs {
		if !filepath.IsAbs(config.Path) {
			t.Errorf("path %q is not absolute, so it is not usable as a command argument", config.Path)
		}
		if config.Name == "" || config.Relative == "" {
			t.Errorf("entry is missing display fields: %+v", config)
		}
		if strings.Contains(config.Relative, `\`) {
			t.Errorf("relative path %q is not slash-separated", config.Relative)
		}
	}
}
