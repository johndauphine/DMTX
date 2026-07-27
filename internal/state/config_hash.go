package state

import (
	"database/sql"
	"fmt"
)

// SaveConfigHash records the sanitized data-plane configuration for a run.
func (store SQLiteStore) SaveConfigHash(runID, hash string) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE IF NOT EXISTS run_config_hashes (run_id TEXT PRIMARY KEY, config_hash TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("initialize configuration hashes: %w", err)
	}
	if _, err := database.Exec(`INSERT INTO run_config_hashes (run_id, config_hash) VALUES (?, ?)`, runID, hash); err != nil {
		return fmt.Errorf("save configuration hash: %w", err)
	}
	return nil
}

// ConfigHash returns the recorded sanitized data-plane configuration hash.
func (store SQLiteStore) ConfigHash(runID string) (string, bool, error) {
	database, err := store.Open()
	if err != nil {
		return "", false, err
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE IF NOT EXISTS run_config_hashes (run_id TEXT PRIMARY KEY, config_hash TEXT NOT NULL)`); err != nil {
		return "", false, fmt.Errorf("initialize configuration hashes: %w", err)
	}
	var hash string
	err = database.QueryRow(`SELECT config_hash FROM run_config_hashes WHERE run_id = ?`, runID).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read configuration hash: %w", err)
	}
	return hash, true, nil
}
