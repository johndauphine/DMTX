package state

import (
	"errors"
	"path/filepath"
	"strings"
)

// NewBackend selects the headless YAML backend for YAML paths and the full
// SQLite backend for every other state path.
func NewBackend(path string) (Backend, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("state path is required")
	}
	switch strings.ToLower(filepath.Ext(path)) {
	case ".yaml", ".yml":
		return YAMLStore{Path: path}, nil
	default:
		return SQLiteStore{Path: path}, nil
	}
}
