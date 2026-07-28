package state

import "fmt"

// InitializeRun atomically records a run's initial state and compatibility
// hash so a hard stop cannot expose one without the other.
func (store SQLiteStore) InitializeRun(run Run, configHash string) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin run initialization: %w", err)
	}
	defer transaction.Rollback()

	if _, err := transaction.Exec(`
		INSERT INTO runs (id, source, target, outcome, resumable, reason, started_at, ended_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, run.ID, run.Source, run.Target, run.Outcome, run.Resumable, run.Reason, run.StartedAt.UTC(), nullableTime(run.EndedAt)); err != nil {
		return fmt.Errorf("record initial run state: %w", err)
	}
	if _, err := transaction.Exec(`CREATE TABLE IF NOT EXISTS run_config_hashes (run_id TEXT PRIMARY KEY, config_hash TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("initialize configuration hashes: %w", err)
	}
	if _, err := transaction.Exec(`INSERT INTO run_config_hashes (run_id, config_hash) VALUES (?, ?)`, run.ID, configHash); err != nil {
		return fmt.Errorf("save initial configuration hash: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit run initialization: %w", err)
	}
	return nil
}

// CreateTasks atomically creates all selected table checkpoints before target
// mutation begins.
func (store SQLiteStore) CreateTasks(tasks []Task) error {
	database, err := store.Open()
	if err != nil {
		return err
	}
	defer database.Close()

	transaction, err := database.Begin()
	if err != nil {
		return fmt.Errorf("begin table checkpoint creation: %w", err)
	}
	defer transaction.Rollback()

	for _, task := range tasks {
		if _, err := transaction.Exec(`
			INSERT INTO tasks (run_id, table_name, status, rows_done, started_at, completed_at)
			VALUES (?, ?, 'running', 0, ?, NULL)
		`, task.RunID, task.Table, task.StartedAt.UTC()); err != nil {
			return fmt.Errorf("create table checkpoint %q: %w", task.Table, err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit table checkpoints: %w", err)
	}
	return nil
}
