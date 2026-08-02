package state

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"
)

// Lease is an exclusive, fenced claim on a canonical migration target.
type Lease struct {
	Target     string
	RunID      string
	OwnerToken string
	Generation int64
}

// AcquireLease claims a target unless another live owner holds it.
func (store SQLiteStore) AcquireLease(target, runID string, ttl time.Duration) (Lease, error) {
	return store.AcquireLeaseMatching(target, runID, ttl, nil)
}

// AcquireLeaseMatching atomically claims a target while treating any target
// accepted by equivalent as the same physical endpoint. It is used for local
// file aliases whose stable file identity can appear after an initially
// missing path is opened. All acquisitions in the store are serialized before
// equivalence is evaluated, so two different textual keys cannot both pass an
// empty read and become live owners.
func (store SQLiteStore) AcquireLeaseMatching(
	target,
	runID string,
	ttl time.Duration,
	equivalent func(string) (bool, error),
) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("lease TTL must be positive")
	}
	if target == "" || runID == "" {
		return Lease{}, fmt.Errorf("lease target and run ID are required")
	}
	database, err := store.Open()
	if err != nil {
		return Lease{}, err
	}
	defer database.Close()
	if _, err := database.Exec(`
		CREATE TABLE IF NOT EXISTS leases (
			target TEXT PRIMARY KEY, owner_token TEXT NOT NULL, run_id TEXT NOT NULL,
			generation INTEGER NOT NULL, heartbeat_at DATETIME NOT NULL
		);
		CREATE TABLE IF NOT EXISTS lease_acquisition_lock (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			nonce INTEGER NOT NULL
		);
		INSERT OR IGNORE INTO lease_acquisition_lock(id, nonce) VALUES (1, 0);
	`); err != nil {
		return Lease{}, fmt.Errorf("initialize target leases: %w", err)
	}

	tx, err := database.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Lease{}, fmt.Errorf("begin lease acquisition: %w", err)
	}
	defer tx.Rollback()

	// Force a write reservation before reading the alias set. A second
	// acquisition must observe this transaction's result rather than making a
	// decision from the same stale snapshot under a different target key.
	if _, err := tx.Exec(`
		UPDATE lease_acquisition_lock SET nonce = nonce + 1 WHERE id = 1
	`); err != nil {
		return Lease{}, fmt.Errorf("serialize lease acquisition: %w", err)
	}

	now := time.Now().UTC()
	rows, err := tx.Query(`
		SELECT target, generation, heartbeat_at
		FROM leases
		ORDER BY target
	`)
	if err != nil {
		return Lease{}, fmt.Errorf("read target leases: %w", err)
	}
	var generation int64
	matchedTargets := make([]string, 0, 1)
	for rows.Next() {
		var existingTarget string
		var existingGeneration int64
		var heartbeat time.Time
		if err := rows.Scan(
			&existingTarget,
			&existingGeneration,
			&heartbeat,
		); err != nil {
			rows.Close()
			return Lease{}, fmt.Errorf("decode target lease: %w", err)
		}
		matches := existingTarget == target
		if !matches && equivalent != nil {
			matches, err = equivalent(existingTarget)
			if err != nil {
				rows.Close()
				return Lease{}, fmt.Errorf(
					"compare target lease alias %q: %w",
					existingTarget,
					err,
				)
			}
		}
		if !matches {
			continue
		}
		matchedTargets = append(matchedTargets, existingTarget)
		if heartbeat.Add(ttl).After(now) {
			rows.Close()
			return Lease{}, fmt.Errorf(
				"target already has a live migration lease",
			)
		}
		if existingGeneration > generation {
			generation = existingGeneration
		}
	}
	if err := finishSQLiteRows(
		rows,
		"iterate target leases",
		"close target lease query",
	); err != nil {
		return Lease{}, err
	}
	generation++

	token, err := randomToken()
	if err != nil {
		return Lease{}, err
	}
	for _, alias := range matchedTargets {
		if alias == target {
			continue
		}
		if _, err := tx.Exec(`
			UPDATE leases
			SET owner_token = '', run_id = '', generation = ?, heartbeat_at = ?
			WHERE target = ?
		`, generation, time.Unix(0, 0).UTC(), alias); err != nil {
			return Lease{}, fmt.Errorf(
				"invalidate stale target lease alias %q: %w",
				alias,
				err,
			)
		}
	}
	_, err = tx.Exec(`
		INSERT INTO leases (target, owner_token, run_id, generation, heartbeat_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(target) DO UPDATE SET
			owner_token = excluded.owner_token,
			run_id = excluded.run_id,
			generation = excluded.generation,
			heartbeat_at = excluded.heartbeat_at
	`, target, token, runID, generation, now)
	if err != nil {
		return Lease{}, fmt.Errorf("write target lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Lease{}, fmt.Errorf("commit target lease: %w", err)
	}
	return Lease{Target: target, RunID: runID, OwnerToken: token, Generation: generation}, nil
}

// RenewLease extends a lease only while this owner still holds its generation.
func (store SQLiteStore) RenewLease(lease Lease) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Exec(`
		UPDATE leases SET heartbeat_at = ?
		WHERE target = ? AND run_id = ? AND owner_token = ? AND generation = ?
	`, time.Now().UTC(), lease.Target, lease.RunID, lease.OwnerToken, lease.Generation)
	if err != nil {
		return fmt.Errorf("renew target lease: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify target lease renewal: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("target lease is no longer owned by this migration")
	}
	return nil
}

// ReleaseLease removes a lease only when the owner token and generation match.
func (store SQLiteStore) ReleaseLease(lease Lease) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Exec(`
		UPDATE leases SET owner_token = '', run_id = '', heartbeat_at = ?
		WHERE target = ? AND run_id = ? AND owner_token = ? AND generation = ?
	`, time.Unix(0, 0).UTC(), lease.Target, lease.RunID, lease.OwnerToken, lease.Generation)
	if err != nil {
		return fmt.Errorf("release target lease: %w", err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify target lease release: %w", err)
	}
	if removed != 1 {
		return fmt.Errorf("target lease is no longer owned by this migration")
	}
	return nil
}

func randomToken() (string, error) {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate lease owner token: %w", err)
	}
	return hex.EncodeToString(bytes), nil
}
