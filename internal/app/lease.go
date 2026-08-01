package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// acquireTargetLease uses the actual endpoint identity. SQLite keeps its
// adjacent lease database; network targets share a stable cache location so
// separate config files still contend for the same physical endpoint.
func acquireTargetLease(target config.Endpoint, runID string) (state.SQLiteStore, state.Lease, error) {
	engine, err := config.CanonicalEngine(target.Type)
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, err
	}
	identity, storePath, err := targetLeaseLocation(target)
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, err
	}
	store := state.SQLiteStore{Path: storePath}
	var lease state.Lease
	if engine == "sqlite" {
		lease, err = store.AcquireLeaseMatching(
			identity,
			runID,
			30*time.Second,
			func(existing string) (bool, error) {
				return equivalentSQLiteLeaseIdentity(identity, existing)
			},
		)
	} else {
		lease, err = store.AcquireLease(identity, runID, 30*time.Second)
	}
	if err != nil {
		return state.SQLiteStore{}, state.Lease{}, err
	}
	return store, lease, nil
}

func equivalentSQLiteLeaseIdentity(left, right string) (bool, error) {
	if left == right {
		return true, nil
	}
	const (
		filePrefix = "sqlite:file:"
		pathPrefix = "sqlite:path:"
	)
	if strings.HasPrefix(left, filePrefix) &&
		strings.HasPrefix(right, filePrefix) {
		return false, nil
	}
	if strings.HasPrefix(left, pathPrefix) &&
		strings.HasPrefix(right, pathPrefix) {
		leftPath, err := parseSQLitePathLeaseIdentity(left)
		if err != nil {
			return false, err
		}
		rightPath, err := parseSQLitePathLeaseIdentity(right)
		if err != nil {
			return false, err
		}
		return leftPath.Path == rightPath.Path ||
			leftPath.Folded != "" &&
				leftPath.Folded == rightPath.Folded, nil
	}
	var fileIdentity, pathIdentity string
	switch {
	case strings.HasPrefix(left, filePrefix) &&
		strings.HasPrefix(right, pathPrefix):
		fileIdentity, pathIdentity = left, right
	case strings.HasPrefix(right, filePrefix) &&
		strings.HasPrefix(left, pathPrefix):
		fileIdentity, pathIdentity = right, left
	default:
		return false, fmt.Errorf("unrecognized SQLite lease identity")
	}
	path, err := parseSQLitePathLeaseIdentity(pathIdentity)
	if err != nil {
		return false, err
	}
	resolved, _, err := sqliteLeaseFileIdentity(path.Path)
	if err != nil {
		return false, err
	}
	return "sqlite:"+resolved == fileIdentity, nil
}

type sqlitePathLeaseKey struct {
	Version int    `json:"version"`
	Path    string `json:"path"`
	Folded  string `json:"folded,omitempty"`
}

func sqlitePathLeaseIdentity(path string) (string, error) {
	encoded, err := json.Marshal(sqlitePathLeaseKey{
		Version: 1,
		Path:    path,
		Folded:  sqliteLeaseFoldedPath(path),
	})
	if err != nil {
		return "", fmt.Errorf("encode SQLite path lease identity: %w", err)
	}
	return "sqlite:path:" + string(encoded), nil
}

func parseSQLitePathLeaseIdentity(identity string) (sqlitePathLeaseKey, error) {
	const prefix = "sqlite:path:"
	if !strings.HasPrefix(identity, prefix) {
		return sqlitePathLeaseKey{}, fmt.Errorf(
			"identity is not a SQLite path lease",
		)
	}
	var key sqlitePathLeaseKey
	if err := json.Unmarshal([]byte(strings.TrimPrefix(identity, prefix)), &key); err != nil {
		return sqlitePathLeaseKey{}, fmt.Errorf(
			"decode SQLite path lease identity: %w",
			err,
		)
	}
	if key.Version != 1 || strings.TrimSpace(key.Path) == "" {
		return sqlitePathLeaseKey{}, fmt.Errorf(
			"unsupported or incomplete SQLite path lease identity",
		)
	}
	return key, nil
}

func targetLeaseLocation(target config.Endpoint) (identity, storePath string, err error) {
	engine, err := config.CanonicalEngine(target.Type)
	if err != nil {
		return "", "", err
	}
	target.Type = engine
	if engine == "sqlite" {
		canonicalTarget, err := config.CanonicalSQLitePath(target.Database)
		if err != nil {
			return "", "", fmt.Errorf("canonicalize SQLite target: %w", err)
		}
		fileIdentity, multipleLinks, err := sqliteLeaseFileIdentity(canonicalTarget)
		if err != nil {
			return "", "", fmt.Errorf("identify SQLite target: %w", err)
		}
		if multipleLinks {
			identity = "sqlite:" + fileIdentity
		} else {
			identity, err = sqlitePathLeaseIdentity(canonicalTarget)
			if err != nil {
				return "", "", err
			}
		}
		storePath, err = cachedSQLiteLeasePath()
		return identity, storePath, err
	}
	identity, err = networkEndpointIdentity(target, true)
	if err != nil {
		return "", "", fmt.Errorf("identify network target for lease: %w", err)
	}
	storePath, err = cachedLeasePath(identity)
	return identity, storePath, err
}

// endpointWorkloadIdentity returns a credential-free, collision-safe identity
// for durable cross-run evidence. Network aliases are deliberately not
// resolved: an alias change may conservatively prevent reuse, but can never
// attach a watermark or schema snapshot to an unproven endpoint.
func endpointWorkloadIdentity(endpoint config.Endpoint) (string, error) {
	engine, err := config.CanonicalEngine(endpoint.Type)
	if err != nil {
		return "", err
	}
	if engine == "sqlite" {
		canonicalPath, err := config.CanonicalSQLitePath(endpoint.Database)
		if err != nil {
			return "", fmt.Errorf("canonicalize SQLite endpoint: %w", err)
		}
		fileIdentity, multipleLinks, err := sqliteLeaseFileIdentity(canonicalPath)
		if err != nil {
			return "", fmt.Errorf("identify SQLite endpoint: %w", err)
		}
		if multipleLinks {
			return "sqlite:" + fileIdentity, nil
		}
		return sqlitePathLeaseIdentity(canonicalPath)
	}
	return config.NetworkEndpointWorkloadIdentity(endpoint)
}

func networkEndpointIdentity(
	endpoint config.Endpoint,
	collapseLoopback bool,
) (string, error) {
	engine, err := config.CanonicalEngine(endpoint.Type)
	if err != nil {
		return "", err
	}
	if engine == "sqlite" {
		return "", fmt.Errorf("network endpoint identity cannot represent SQLite")
	}
	if strings.TrimSpace(endpoint.Host) == "" ||
		strings.TrimSpace(endpoint.Database) == "" {
		return "", fmt.Errorf("network endpoint host and database are required")
	}
	port := endpoint.Port
	if port == 0 {
		port = defaultLeasePort(engine)
	}
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("network endpoint port %d is outside 1..65535", port)
	}
	identity := struct {
		Version  int    `json:"version"`
		Engine   string `json:"engine"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Database string `json:"database"`
		Schema   string `json:"schema"`
	}{
		Version:  1,
		Engine:   engine,
		Host:     canonicalLeaseHost(endpoint.Host, collapseLoopback),
		Port:     port,
		Database: endpoint.Database,
		Schema:   endpoint.Schema,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode endpoint workload identity: %w", err)
	}
	return string(encoded), nil
}

func canonicalLeaseHost(host string, collapseLoopback bool) string {
	host = strings.TrimRight(
		strings.ToLower(strings.TrimSpace(host)),
		".",
	)
	unbracketed := host
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		unbracketed = host[1 : len(host)-1]
	}
	if address, err := netip.ParseAddr(unbracketed); err == nil {
		address = address.Unmap()
		if collapseLoopback && address.IsLoopback() {
			return "loopback"
		}
		return address.String()
	}
	if collapseLoopback && host == "localhost" {
		return "loopback"
	}
	return host
}

func cachedLeasePath(identity string) (string, error) {
	directory, err := leaseCacheDirectory()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(identity))
	return filepath.Join(directory, hex.EncodeToString(digest[:])+".db"), nil
}

func cachedSQLiteLeasePath() (string, error) {
	directory, err := leaseCacheDirectory()
	if err != nil {
		return "", err
	}
	return filepath.Join(directory, "sqlite-targets.db"), nil
}

func leaseCacheDirectory() (string, error) {
	cache := strings.TrimSpace(os.Getenv("XDG_CACHE_HOME"))
	if cache == "" {
		var err error
		cache, err = os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("locate DMTX lease cache: %w", err)
		}
	} else if !filepath.IsAbs(cache) {
		return "", fmt.Errorf(
			"XDG_CACHE_HOME must be absolute for cross-process target leases",
		)
	}
	directory := filepath.Join(cache, "dmtx", "leases")
	if err := os.MkdirAll(directory, 0700); err != nil {
		return "", fmt.Errorf("create DMTX lease cache: %w", err)
	}
	return directory, nil
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
