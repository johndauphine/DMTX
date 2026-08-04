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
)

// completionFixture builds a small tree and a server rooted at it.
//
//	root/
//	  cfg.yaml
//	  config-two.yaml
//	  queries/
//	    up.sql
//	outside/
//	  secret.txt
//	root-evil/          a sibling whose name begins with the root's own
//	  secret.txt
//
// root-evil is not decoration. It is the only shape in which a string-prefix
// containment check and a real one disagree: "/base/root-evil" starts with
// "/base/root" but is not inside it. Without it, a test suite can watch the
// boundary being enforced by the wrong mechanism and see nothing wrong.
func completionFixture(t *testing.T) (*Server, string, string) {
	t.Helper()
	base := t.TempDir()
	root := filepath.Join(base, "root")
	outside := filepath.Join(base, "outside")
	sibling := root + "-evil"
	for _, directory := range []string{root, outside, sibling, filepath.Join(root, "queries")} {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", directory, err)
		}
	}
	for path, contents := range map[string]string{
		filepath.Join(root, "cfg.yaml"):          "source: {}\n",
		filepath.Join(root, "config-two.yaml"):   "source: {}\n",
		filepath.Join(root, "queries", "up.sql"): "select 1;\n",
		filepath.Join(outside, "secret.txt"):     "not for you\n",
		filepath.Join(sibling, "secret.txt"):     "not for you either\n",
	} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}

	server, err := New(Options{Root: root})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })
	if server.CompletionRoot() == "" {
		t.Fatal("the fixture root did not resolve, so nothing below tests anything")
	}
	return server, root, outside
}

// names reduces a completion result to the names it offered.
func names(entries []entry) []string {
	found := make([]string, 0, len(entries))
	for _, item := range entries {
		found = append(found, item.Name)
	}
	return found
}

// TestCompletionListsWhatIsInsideTheRoot is the feature working at all.
func TestCompletionListsWhatIsInsideTheRoot(t *testing.T) {
	server, _, _ := completionFixture(t)

	for prefix, want := range map[string][]string{
		"":           {"queries", "cfg.yaml", "config-two.yaml"},
		"c":          {"cfg.yaml", "config-two.yaml"},
		"cfg":        {"cfg.yaml"},
		"queries/":   {"up.sql"},
		"queries/u":  {"up.sql"},
		"queries/zz": {},
	} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			entries, err := server.completions(prefix)
			if err != nil {
				t.Fatalf("completions(%q): %v", prefix, err)
			}
			got := names(entries)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("completions(%q) = %v, want %v", prefix, got, want)
			}
		})
	}
}

// TestCompletionRefusesToLeaveTheRoot is the reason this endpoint needs care.
//
// Completion is the only place dmtx takes a filesystem path from a client. A
// prefix that escapes turns a console into a way to read the directory
// structure of the machine it runs on, over a port the operator opened for
// convenience.
func TestCompletionRefusesToLeaveTheRoot(t *testing.T) {
	server, _, outside := completionFixture(t)

	for _, prefix := range []string{
		"..",
		"../",
		"../outside/",
		"../../",
		"queries/../../outside/",
		"./../outside/",
		outside,               // an absolute path to a real directory
		outside + "/",         //
		"/etc/",               // somewhere that exists on every unix
		"queries/../../../..", // walking out the long way
		// The sibling that a string-prefix check would wave through.
		"../root-evil/",
		"queries/../../root-evil/",
	} {
		t.Run(prefix, func(t *testing.T) {
			entries, err := server.completions(prefix)
			if err == nil {
				t.Fatalf(
					"completions(%q) succeeded and offered %v; the root is not a boundary",
					prefix, names(entries),
				)
			}
		})
	}
}

// TestAnAbsolutePrefixIsReadRelativeToTheRoot pins the chroot-like reading.
//
// "/" is the root, not the filesystem root. This is safe rather than merely
// convenient: every absolute path a client sends lands inside the root or
// nowhere, so there is no spelling of an absolute path that reaches out.
func TestAnAbsolutePrefixIsReadRelativeToTheRoot(t *testing.T) {
	server, _, _ := completionFixture(t)

	entries, err := server.completions("/")
	if err != nil {
		t.Fatalf(`completions("/"): %v`, err)
	}
	if got := names(entries); strings.Join(got, ",") != "queries,cfg.yaml,config-two.yaml" {
		t.Errorf(`completions("/") = %v, want the root's own entries`, got)
	}

	// And it is a reading, not a bypass: the same spelling of somewhere real
	// outside the root still finds nothing.
	if _, err := server.completions("/etc/"); err == nil {
		t.Error(`completions("/etc/") succeeded; the absolute reading is not confined`)
	}
}

// TestCompletionDoesNotSayWhyItRefused pins that the endpoint is not an oracle.
//
// If "outside the root" and "does not exist" produced different answers, a
// caller could map the filesystem above the root by asking about paths and
// watching which refusal came back - reading the whole disk's shape without
// ever being allowed to read a file.
func TestCompletionDoesNotSayWhyItRefused(t *testing.T) {
	server, _, outside := completionFixture(t)

	existing := replyFor(t, server, outside+"/")
	absent := replyFor(t, server, filepath.Join(outside, "no-such-directory")+"/")
	nonsense := replyFor(t, server, "../../../../../../nowhere/")

	if existing.status != absent.status || existing.body != absent.body {
		t.Errorf(
			"a real directory outside the root and an imaginary one answer "+
				"differently:\n  real:      %d %s\n  imaginary: %d %s",
			existing.status, existing.body, absent.status, absent.body,
		)
	}
	if existing.status != nonsense.status || existing.body != nonsense.body {
		t.Errorf(
			"a real directory outside the root and a nonsense path answer "+
				"differently:\n  real:     %d %s\n  nonsense: %d %s",
			existing.status, existing.body, nonsense.status, nonsense.body,
		)
	}
}

type completionReply struct {
	status int
	body   string
}

func replyFor(t *testing.T, server *Server, prefix string) completionReply {
	t.Helper()
	request := httptest.NewRequest(
		http.MethodGet, "/api/v1/complete?prefix="+prefix, nil,
	)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	return completionReply{
		status: recorder.Code,
		body:   strings.TrimSpace(recorder.Body.String()),
	}
}

// TestCompletionWillNotFollowASymlinkOutOfTheRoot pins the containment check
// against the trick it exists for.
//
// A symlink inside the root looks contained until it is followed. Checking
// before resolving would accept it, and completion through it would enumerate
// wherever it points.
func TestCompletionWillNotFollowASymlinkOutOfTheRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation on Windows needs privileges this test should not require")
	}
	server, root, outside := completionFixture(t)
	escape := filepath.Join(root, "escape")
	if err := os.Symlink(outside, escape); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Not offered as a candidate...
	entries, err := server.completions("")
	if err != nil {
		t.Fatalf("completions: %v", err)
	}
	for _, name := range names(entries) {
		if name == "escape" {
			t.Error("a symlink pointing out of the root was offered for completion")
		}
	}

	// ...and not usable as a way through.
	if _, err := server.completions("escape/"); err == nil {
		t.Error("completion followed a symlink out of the root")
	}
}

// TestCompletionOffersASymlinkThatStaysInside pins that the check is
// containment, not a blanket refusal of symlinks: a link to a sibling directory
// inside the root is ordinary and useful.
func TestCompletionOffersASymlinkThatStaysInside(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation on Windows needs privileges this test should not require")
	}
	server, root, _ := completionFixture(t)
	if err := os.Symlink(filepath.Join(root, "queries"), filepath.Join(root, "sql")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	// Offered in the listing, and offered as a directory. Without the symlink
	// branch in describe, an Lstat says "symlink" - neither a regular file nor
	// a directory - and the entry is dropped, so an ordinary internal link
	// disappears from completion for a reason nobody asked for.
	entries, err := server.completions("")
	if err != nil {
		t.Fatalf("completions: %v", err)
	}
	var offered *entry
	for index, item := range entries {
		if item.Name == "sql" {
			offered = &entries[index]
		}
	}
	if offered == nil {
		t.Fatalf("a symlink to a directory inside the root was not offered: %v", names(entries))
	}
	if !offered.Dir {
		t.Error("a symlink to a directory was not reported as a directory")
	}

	// And usable as a way through.
	entries, err = server.completions("sql/")
	if err != nil {
		t.Fatalf("completions through an internal symlink: %v", err)
	}
	if got := names(entries); strings.Join(got, ",") != "up.sql" {
		t.Errorf("completion through an internal symlink gave %v, want [up.sql]", got)
	}
}

// TestCompletionSkipsThingsThatAreNotFilesOrDirectories pins that a FIFO is
// never offered. Whatever the operator does with a completion next may open it,
// and reading a FIFO blocks until something else writes.
func TestCompletionSkipsThingsThatAreNotFilesOrDirectories(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no mkfifo on Windows")
	}
	server, root, _ := completionFixture(t)
	if err := makeFIFO(filepath.Join(root, "pipe")); err != nil {
		t.Skipf("cannot create a FIFO here: %v", err)
	}

	entries, err := server.completions("")
	if err != nil {
		t.Fatalf("completions: %v", err)
	}
	for _, name := range names(entries) {
		if name == "pipe" {
			t.Error("a FIFO was offered as a completion")
		}
	}
}

// TestCompletionIsOffWithoutARoot pins that no root means no enumeration,
// rather than a fallback to somewhere convenient.
func TestCompletionIsOffWithoutARoot(t *testing.T) {
	server := newTestServer(t)
	if server.CompletionRoot() != "" {
		t.Fatalf("a server built without a root has one: %s", server.CompletionRoot())
	}
	if _, err := server.completions(""); err == nil {
		t.Error("completion answered with no root configured")
	}
}

// TestUnresolvableRootLeavesCompletionOff pins that a bad root fails closed. A
// root that silently widened to the working directory would be the worst
// outcome available.
func TestUnresolvableRootLeavesCompletionOff(t *testing.T) {
	file := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	roots := map[string]string{
		"empty":         "",
		"missing":       filepath.Join(t.TempDir(), "not-there"),
		"not-directory": file,
	}
	if runtime.GOOS != "windows" {
		broken := filepath.Join(t.TempDir(), "dangling")
		if err := os.Symlink(filepath.Join(t.TempDir(), "gone"), broken); err == nil {
			roots["dangling-symlink"] = broken
		}
	}

	for name, root := range roots {
		t.Run(name, func(t *testing.T) {
			server, err := New(Options{Root: root})
			if err != nil {
				t.Fatalf("new server: %v", err)
			}
			t.Cleanup(func() { _ = server.listener.Close() })

			if server.CompletionRoot() != "" {
				t.Errorf("completion is on with root %q", root)
			}
			if _, err := server.completions("cfg"); err == nil {
				t.Error("completion answered despite an unusable root")
			}
		})
	}
}

// TestCompletionRequiresAuthentication pins that the endpoint is behind the
// session like everything else. An unauthenticated path enumerator on loopback
// is reachable from any page the operator visits.
func TestCompletionRequiresAuthentication(t *testing.T) {
	server, _, _ := completionFixture(t)
	request := httptest.NewRequest(http.MethodGet, "/api/v1/complete?prefix=", nil)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("completion without a session returned %d, want 401", recorder.Code)
	}
}

// TestCompletionResultsAreBounded pins the cap, so one keystroke in a large
// directory does not become a large response.
func TestCompletionResultsAreBounded(t *testing.T) {
	base := t.TempDir()
	for index := 0; index < maxCompletions+50; index++ {
		name := filepath.Join(base, fmt.Sprintf("file-%04d", index))
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	server, err := New(Options{Root: base})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	t.Cleanup(func() { _ = server.listener.Close() })

	entries, err := server.completions("")
	if err != nil {
		t.Fatalf("completions: %v", err)
	}
	if len(entries) > maxCompletions {
		t.Errorf("completion returned %d entries, over the cap of %d", len(entries), maxCompletions)
	}
}

// TestCompletionReplyShape pins the JSON a front end will read.
func TestCompletionReplyShape(t *testing.T) {
	server, _, _ := completionFixture(t)
	// The resolved root, not the fixture's path: on macOS /var is a symlink to
	// /private/var, so the two spellings differ and comparing against the
	// unresolved one fails for a reason that has nothing to do with the code.
	root := server.CompletionRoot()
	reply := replyFor(t, server, "cfg")
	if reply.status != http.StatusOK {
		t.Fatalf("completion returned %d", reply.status)
	}
	var decoded struct {
		Entries []entry `json:"entries"`
	}
	if err := json.Unmarshal([]byte(reply.body), &decoded); err != nil {
		t.Fatalf("decode %s: %v", reply.body, err)
	}
	if len(decoded.Entries) != 1 {
		t.Fatalf("expected one entry, got %v", decoded.Entries)
	}
	got := decoded.Entries[0]
	if got.Name != "cfg.yaml" {
		t.Errorf("name is %q", got.Name)
	}
	if got.Dir {
		t.Error("a file was reported as a directory")
	}
	// The path has to be usable as a command argument whatever directory the
	// server was started in, which a root-relative one would not be.
	if !filepath.IsAbs(got.Path) {
		t.Errorf("path %q is not absolute", got.Path)
	}
	if !strings.HasPrefix(got.Path, root) {
		t.Errorf("path %q is not inside the root %q", got.Path, root)
	}
}

// TestCompletionRootPrefersConfigThenWorkingDirectory covers the resolution
// order without starting a server.
func TestCompletionRootPrefersConfigThenWorkingDirectory(t *testing.T) {
	working, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for name, expectation := range map[string]struct {
		options Options
		want    string
	}{
		"explicit root wins": {
			options: Options{Root: "/explicit", configPath: "/project/cfg.yaml"},
			want:    "/explicit",
		},
		"config directory next": {
			options: Options{configPath: filepath.Join("/project", "cfg.yaml")},
			want:    "/project",
		},
		"working directory last": {
			options: Options{},
			want:    working,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := completionRoot(expectation.options); got != expectation.want {
				t.Errorf("completionRoot = %q, want %q", got, expectation.want)
			}
		})
	}
}

// TestParseArgumentsReadsTheRootFlags covers the two new flags.
func TestParseArgumentsReadsTheRootFlags(t *testing.T) {
	options, ok := parseArguments([]string{"--root", "/somewhere", "--config", "/p/cfg.yaml"})
	if !ok {
		t.Fatal("serve refused --root with --config")
	}
	if options.Root != "/somewhere" {
		t.Errorf("Root is %q", options.Root)
	}
	if options.configPath != "/p/cfg.yaml" {
		t.Errorf("configPath is %q", options.configPath)
	}

	for _, refused := range [][]string{
		{"--root"},
		{"--config"},
		{"--root", "/a", "--root", "/b"},
		{"--config", "/a", "--config", "/b"},
	} {
		if _, ok := parseArguments(refused); ok {
			t.Errorf("serve accepted %v", refused)
		}
	}
}
