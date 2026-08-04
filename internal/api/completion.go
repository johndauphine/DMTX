package api

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// maxCompletions bounds a reply. A completion list is something a person reads;
// a directory with fifty thousand entries is not, and serialising it would turn
// a keystroke into a stall.
const maxCompletions = 200

// errNoCompletion is the only error completion reports.
//
// The guarantee that matters is enforced in complete, which answers every
// failure with one status and one sentence whatever it was handed. That is
// deliberate: distinguishing "outside the root" from "does not exist" would
// make this endpoint an oracle for probing the filesystem above the root, and a
// caller who can tell those apart can map the whole disk without reading a byte
// of it. This value keeps the internals honest to the same rule, but it is the
// handler, not the value, that a test should hold to it.
var errNoCompletion = errors.New("cannot complete that path")

// pathRoot is the boundary @ completion may not cross.
//
// The path is resolved once, at startup, with symlinks followed. Resolving on
// every request would be slower and no safer, and resolving the root lazily
// would mean a root that changed underneath the process was silently obeyed.
type pathRoot struct {
	resolved string
}

// newPathRoot resolves a completion root. A root that cannot be resolved
// produces no root at all rather than a permissive fallback: completion that
// quietly widens to the working directory - or to "/" - is worse than
// completion that does not work.
func newPathRoot(path string) (*pathRoot, error) {
	if path == "" {
		return nil, errors.New("no completion root given")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("completion root is not a directory")
	}
	return &pathRoot{resolved: resolved}, nil
}

// contains reports whether a resolved path is the root or lies beneath it.
//
// filepath.Rel rather than a string prefix: /home/user-evil is not inside
// /home/user, but a prefix test says it is.
func (root *pathRoot) contains(resolved string) bool {
	relative, err := filepath.Rel(root.resolved, resolved)
	if err != nil {
		return false
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

// entry is one completion candidate.
type entry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Dir  bool   `json:"dir"`
}

// complete answers a completion request.
func (server *Server) complete(writer http.ResponseWriter, request *http.Request) {
	entries, err := server.completions(request.URL.Query().Get("prefix"))
	if err != nil {
		// Always the same status and the same words, whatever went wrong.
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": errNoCompletion.Error(),
		})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"entries": entries})
}

// completions lists what the operator could have meant by prefix.
//
// prefix is whatever followed @ in the console: a partial name, a partial name
// inside a subdirectory, or a directory with a trailing separator. It is
// resolved against the root and may not leave it.
//
// An absolute prefix is read as relative to the root, the way a path inside a
// chroot is. "/" therefore means the root rather than the filesystem root, and
// "/etc/" means a directory of that name inside the root - which will usually
// not exist, and is refused like anything else that does not. Nothing is
// mis-sent as a result: an entry's Path is always the real absolute path, so
// what the console passes to a command is what it completed to.
func (server *Server) completions(prefix string) ([]entry, error) {
	if server.root == nil {
		return nil, errNoCompletion
	}

	directory, partial := splitCompletion(prefix)
	// Join cleans the result, so "a/../../etc" collapses before it is used - but
	// it collapses to somewhere outside the root, which is why the containment
	// check below is what actually enforces the boundary, not this call.
	joined := filepath.Join(server.root.resolved, directory)
	resolved, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return nil, errNoCompletion
	}
	// Checked after resolving, not before. A symlink inside the root pointing
	// out of it looks contained until it is followed.
	if !server.root.contains(resolved) {
		return nil, errNoCompletion
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return nil, errNoCompletion
	}

	listed, err := os.ReadDir(resolved)
	if err != nil {
		return nil, errNoCompletion
	}
	found := make([]entry, 0, len(listed))
	for _, item := range listed {
		if len(found) == maxCompletions {
			break
		}
		if partial != "" && !strings.HasPrefix(item.Name(), partial) {
			continue
		}
		candidate, ok := server.describe(filepath.Join(resolved, item.Name()))
		if !ok {
			continue
		}
		found = append(found, candidate)
	}
	sort.Slice(found, func(first, second int) bool {
		// Directories first, then by name: the operator is usually walking down
		// a tree, and the thing they want next is a directory.
		if found[first].Dir != found[second].Dir {
			return found[first].Dir
		}
		return found[first].Name < found[second].Name
	})
	return found, nil
}

// describe decides whether one directory item may be offered, and as what.
func (server *Server) describe(path string) (entry, bool) {
	// Lstat, so a symlink is examined as a symlink rather than as whatever it
	// points at.
	info, err := os.Lstat(path)
	if err != nil {
		return entry{}, false
	}

	if info.Mode()&os.ModeSymlink != 0 {
		// A symlink out of the root would otherwise make completion a way to
		// enumerate the filesystem above it: plant one, then complete through
		// it.
		target, err := filepath.EvalSymlinks(path)
		if err != nil || !server.root.contains(target) {
			return entry{}, false
		}
		if info, err = os.Stat(target); err != nil {
			return entry{}, false
		}
	}

	// Regular files and directories only. A FIFO or a device offered here could
	// be opened by whatever the operator does next, and reading one blocks
	// until something else writes to it.
	if !info.Mode().IsRegular() && !info.IsDir() {
		return entry{}, false
	}
	return entry{
		Name: filepath.Base(path),
		Path: path,
		Dir:  info.IsDir(),
	}, true
}

// splitCompletion divides a prefix into the directory to list and the partial
// name to match within it.
//
// A trailing separator means the operator has finished naming a directory and
// wants its contents, so there is no partial name to filter by.
func splitCompletion(prefix string) (directory, partial string) {
	normalised := filepath.FromSlash(prefix)
	if normalised == "" {
		return ".", ""
	}
	if strings.HasSuffix(normalised, string(filepath.Separator)) {
		return normalised, ""
	}
	directory, partial = filepath.Dir(normalised), filepath.Base(normalised)
	// "." and ".." name a directory; they are not the beginning of a filename.
	// Treated as a partial they would be matched against the root's entries,
	// match nothing, and come back as an empty success - which tells the
	// operator "there is nothing there" when the truth is "that is not
	// somewhere you may look".
	if partial == "." || partial == ".." {
		return normalised, ""
	}
	return directory, partial
}
