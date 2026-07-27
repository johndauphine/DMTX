package app

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/johndauphine/DMTX/internal/state"
)

func acquireSQLiteTargetLease(target, runID string) (state.SQLiteStore, state.Lease, error) {
	canonicalTarget, err := filepath.Abs(target)
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, fmt.Errorf("canonicalize SQLite target: %w", err)
	}
	store := state.SQLiteStore{Path: canonicalTarget + ".dmtx-lease.db"}
	lease, err := store.AcquireLease("sqlite:"+canonicalTarget, runID, 30*time.Second)
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, err
	}
	return store, lease, nil
}
