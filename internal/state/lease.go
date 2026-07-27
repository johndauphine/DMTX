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
	OwnerToken string
	Generation int64
}

// AcquireLease claims a target unless another live owner holds it.
func (store SQLiteStore) AcquireLease(target, runID string, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, fmt.Errorf("lease TTL must be positive")
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
		)
	`); err != nil {
		return Lease{}, fmt.Errorf("initialize target leases: %w", err)
	}

	tx, err := database.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return Lease{}, fmt.Errorf("begin lease acquisition: %w", err)
	}
	defer tx.Rollback()

	var generation int64
	var heartbeat time.Time
	err = tx.QueryRow(`SELECT generation, heartbeat_at FROM leases WHERE target = ?`, target).Scan(&generation, &heartbeat)
	now := time.Now().UTC()
	switch {
	case err == sql.ErrNoRows:
		generation = 1
	case err != nil:
		return Lease{}, fmt.Errorf("read target lease: %w", err)
	case heartbeat.Add(ttl).After(now):
		return Lease{}, fmt.Errorf("target already has a live migration lease")
	default:
		generation++
	}

	token, err := randomToken()
	if err != nil {
		return Lease{}, err
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
	return Lease{Target: target, OwnerToken: token, Generation: generation}, nil
}

// ReleaseLease removes a lease only when the owner token and generation match.
func (store SQLiteStore) ReleaseLease(lease Lease) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	result, err := database.Exec(`DELETE FROM leases WHERE target = ? AND owner_token = ? AND generation = ?`, lease.Target, lease.OwnerToken, lease.Generation)
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
