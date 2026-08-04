package api

import (
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Discovery bounds. The root is normally a project directory, so these are
// generous for the intended case and are there for the one that is not: an
// operator who served from a home directory should get a short answer quickly
// rather than a filesystem walk.
const (
	maxDiscoveredConfigs = 100
	// Inclusive: a config in a directory this many levels below the root is
	// found, one level deeper is not.
	maxDiscoveryDepth = 3
	configProbeBytes  = 8 << 10
)

// discoveredConfig is one candidate an operator could pick.
type discoveredConfig struct {
	Name string `json:"name"`
	// Path is absolute, so it can be sent straight back as a command argument
	// whatever directory the server was started in.
	Path string `json:"path"`
	// Relative is for display: an operator recognises "queries/shop.yaml", not
	// a long absolute path repeated down a list.
	Relative string `json:"relative"`
}

// configs lists the configuration files an operator could pick.
//
// It is confined by the same root as @ completion, which is what makes it safe
// to accept at all: completion already lists every file under that root, so
// showing the config-shaped subset of the same tree grants nothing new. One
// boundary, one set of tests holding it.
func (server *Server) configs(writer http.ResponseWriter, request *http.Request) {
	if server.root == nil {
		// The same uniform refusal completion gives, for the same reason:
		// nothing here should report on a filesystem it was not pointed at.
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": errNoCompletion.Error(),
		})
		return
	}
	found, err := server.discoverConfigs()
	if err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": errNoCompletion.Error(),
		})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"root":    server.root.resolved,
		"configs": found,
	})
}

// discoverConfigs walks the root for files that look like migration
// configurations.
func (server *Server) discoverConfigs() ([]discoveredConfig, error) {
	root := server.root.resolved
	found := make([]discoveredConfig, 0, 8)

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			// An unreadable directory is skipped rather than fatal: one
			// permission denied somewhere under the root should not deny the
			// operator the configs they can read.
			if entry != nil && entry.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if len(found) >= maxDiscoveredConfigs {
			return filepath.SkipAll
		}
		if entry.IsDir() {
			if path == root {
				return nil
			}
			// Deeper than the bound, not at it. Skipping a directory stops its
			// contents being read, so refusing at exactly maxDiscoveryDepth
			// would make the effective depth one less than the name promises.
			if depthUnder(root, path) > maxDiscoveryDepth {
				return fs.SkipDir
			}
			return nil
		}
		// Regular files only. WalkDir reports entries by Lstat, so a symlink is
		// typed as a symlink and skipped here - along with FIFOs and devices,
		// which is the point: reading one blocks until something else writes,
		// and a walk that hung would take the console with it.
		if !entry.Type().IsRegular() {
			return nil
		}
		if !hasConfigExtension(path) || !looksLikeMigrationConfig(path) {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return nil
		}
		found = append(found, discoveredConfig{
			Name:     filepath.Base(path),
			Path:     path,
			Relative: filepath.ToSlash(relative),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(found, func(first, second int) bool {
		return found[first].Relative < found[second].Relative
	})
	return found, nil
}

// depthUnder counts how many directories separate a path from the root.
func depthUnder(root, path string) int {
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == "." {
		return 0
	}
	return strings.Count(relative, string(filepath.Separator)) + 1
}

func hasConfigExtension(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return true
	}
	return false
}

// looksLikeMigrationConfig reads a bounded prefix and reports whether the file
// declares both endpoints.
//
// The point is to keep unrelated YAML - compose files, CI workflows, a linter
// config - out of a picker an operator is meant to choose from quickly. The
// keys are matched at the start of a line, so a comment mentioning source: does
// not qualify a file that has no source.
func looksLikeMigrationConfig(path string) bool {
	// Checked again here even though the walk already filtered by type: this
	// function opens the file, and between the walk and the open the entry
	// could have been replaced.
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }()

	buffer := make([]byte, configProbeBytes)
	read, err := file.Read(buffer)
	if read == 0 && err != nil {
		return false
	}
	var source, target bool
	for _, line := range strings.Split(string(buffer[:read]), "\n") {
		switch {
		case strings.HasPrefix(line, "source:"):
			source = true
		case strings.HasPrefix(line, "target:"):
			target = true
		}
	}
	return source && target
}
