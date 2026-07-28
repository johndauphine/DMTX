package app

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/state"
)

// acquireTargetLease uses the actual endpoint identity. SQLite keeps its
// adjacent lease database; network targets share a stable cache location so
// separate config files still contend for the same physical endpoint.
func acquireTargetLease(target config.Endpoint, runID string) (state.SQLiteStore, state.Lease, error) {
	identity, storePath, err := targetLeaseLocation(target)
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, err
	}
	store := state.SQLiteStore{Path: storePath}
	lease, err := store.AcquireLease(identity, runID, 30*time.Second)
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, err
	}
	return store, lease, nil
}

func targetLeaseLocation(target config.Endpoint) (identity, storePath string, err error) {
	if target.Type == "sqlite" {
		canonicalTarget, err := config.CanonicalSQLitePath(target.Database)
		if err != nil {
			return "", "", fmt.Errorf("canonicalize SQLite target: %w", err)
		}
		fileIdentity, multipleLinks, err := sqliteLeaseFileIdentity(canonicalTarget)
		if err != nil {
			return "", "", fmt.Errorf("identify SQLite target: %w", err)
		}
		if !multipleLinks {
			identity = "sqlite:path:" + canonicalTarget
			return identity, canonicalTarget + ".dmtx-lease.db", nil
		}
		identity = "sqlite:" + fileIdentity
		storePath, err = cachedLeasePath(identity)
		return identity, storePath, err
	}
	if target.Host == "" || target.Database == "" {
		return "", "", fmt.Errorf("network target host and database are required for lease")
	}
	port := target.Port
	if port == 0 {
		port = defaultLeasePort(target.Type)
	}
	identity = strings.Join([]string{target.Type, strings.ToLower(target.Host), strconv.Itoa(port), target.Database, target.Schema}, ":")
	storePath, err = cachedLeasePath(identity)
	return identity, storePath, err
}

func cachedLeasePath(identity string) (string, error) {
	cache, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("locate DMTX lease cache: %w", err)
	}
	directory := filepath.Join(cache, "dmtx", "leases")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", fmt.Errorf("create DMTX lease cache: %w", err)
	}
	digest := sha256.Sum256([]byte(identity))
	return filepath.Join(directory, hex.EncodeToString(digest[:])+".db"), nil
}

func defaultLeasePort(engine string) int {
	switch engine {
	case "postgres":
		return 5432
	case "mysql":
		return 3306
	case "mssql":
		return 1433
	case "clickhouse":
		return 9440
	default:
		return 0
	}
}
