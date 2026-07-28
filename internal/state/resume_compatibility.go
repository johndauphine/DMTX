package state

import (
	"database/sql"
	"fmt"
)

// SaveResumeCompatibilityHash records structural resume evidence separately
// from the exact current configuration hash.
func (store SQLiteStore) SaveResumeCompatibilityHash(runID, hash string) error {
	if runID == "" || hash == "" {
		return fmt.Errorf("save resume compatibility hash: run ID and hash are required")
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if err := ensureSQLiteResumeCompatibilitySchema(database); err != nil {
		return err
	}
	if _, err := database.Exec(
		`INSERT INTO run_resume_compatibility (run_id, compatibility_hash) VALUES (?, ?)`,
		runID,
		hash,
	); err != nil {
		return fmt.Errorf("save resume compatibility hash: %w", err)
	}
	return nil
}

// ResumeCompatibilityHash returns persisted structural resume evidence.
func (store SQLiteStore) ResumeCompatibilityHash(runID string) (string, bool, error) {
	database, err := store.Open()
	if err != nil {
		return "", false, err
	}
	defer database.Close()
	if err := ensureSQLiteResumeCompatibilitySchema(database); err != nil {
		return "", false, err
	}
	var hash string
	err = database.QueryRow(
		`SELECT compatibility_hash FROM run_resume_compatibility WHERE run_id = ?`,
		runID,
	).Scan(&hash)
	if err == sql.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read resume compatibility hash: %w", err)
	}
	return hash, true, nil
}

// AcknowledgeConfigOverride atomically advances the exact hash after the
// operator has forced a structurally compatible resume.
func (store SQLiteStore) AcknowledgeConfigOverride(
	runID,
	configHash,
	compatibilityHash string,
) error {
	if runID == "" || configHash == "" || compatibilityHash == "" {
		return fmt.Errorf("acknowledge configuration override: run ID and hashes are required")
	}
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()
	if err := ensureSQLiteResumeCompatibilitySchema(database); err != nil {
		return err
	}
	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin configuration override: %w", err)
	}
	defer transaction.Rollback()
	configResult, err := transaction.Exec(
		`UPDATE run_config_hashes SET config_hash = ? WHERE run_id = ?`,
		configHash,
		runID,
	)
	if err != nil {
		return fmt.Errorf("update configuration hash override: %w", err)
	}
	compatibilityResult, err := transaction.Exec(
		`UPDATE run_resume_compatibility SET compatibility_hash = ? WHERE run_id = ?`,
		compatibilityHash,
		runID,
	)
	if err != nil {
		return fmt.Errorf("update resume compatibility override: %w", err)
	}
	configRows, configErr := configResult.RowsAffected()
	compatibilityRows, compatibilityErr := compatibilityResult.RowsAffected()
	if configErr != nil || compatibilityErr != nil ||
		configRows != 1 || compatibilityRows != 1 {
		return fmt.Errorf("acknowledge configuration override: missing compatibility evidence")
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit configuration override: %w", err)
	}
	return nil
}

func ensureSQLiteResumeCompatibilitySchema(database *sql.DB) error {
	if _, err := database.Exec(`
		CREATE TABLE IF NOT EXISTS run_config_hashes (
			run_id TEXT PRIMARY KEY,
			config_hash TEXT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS run_resume_compatibility (
			run_id TEXT PRIMARY KEY,
			compatibility_hash TEXT NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("initialize resume compatibility hashes: %w", err)
	}
	return nil
}

// SaveResumeCompatibilityHash records structural evidence in the atomic YAML
// document.
func (store YAMLStore) SaveResumeCompatibilityHash(runID, hash string) error {
	if runID == "" || hash == "" {
		return fmt.Errorf("save resume compatibility hash: run ID and hash are required")
	}
	return store.update(func(document *yamlStateDocument) error {
		if document.ResumeCompatibilityHashes == nil {
			document.ResumeCompatibilityHashes = make(map[string]string)
		}
		if _, exists := document.ResumeCompatibilityHashes[runID]; exists {
			return fmt.Errorf("save resume compatibility hash: duplicate run %q", runID)
		}
		document.ResumeCompatibilityHashes[runID] = hash
		return nil
	})
}

// ResumeCompatibilityHash returns YAML structural resume evidence.
func (store YAMLStore) ResumeCompatibilityHash(runID string) (string, bool, error) {
	var hash string
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		hash, found = document.ResumeCompatibilityHashes[runID]
		return nil
	})
	return hash, found, err
}

// AcknowledgeConfigOverride updates both YAML hashes under one file lock and
// one atomic replacement.
func (store YAMLStore) AcknowledgeConfigOverride(
	runID,
	configHash,
	compatibilityHash string,
) error {
	if runID == "" || configHash == "" || compatibilityHash == "" {
		return fmt.Errorf("acknowledge configuration override: run ID and hashes are required")
	}
	return store.update(func(document *yamlStateDocument) error {
		if _, exists := document.ConfigHashes[runID]; !exists {
			return fmt.Errorf("acknowledge configuration override: missing configuration hash")
		}
		if _, exists := document.ResumeCompatibilityHashes[runID]; !exists {
			return fmt.Errorf("acknowledge configuration override: missing compatibility evidence")
		}
		document.ConfigHashes[runID] = configHash
		document.ResumeCompatibilityHashes[runID] = compatibilityHash
		return nil
	})
}
