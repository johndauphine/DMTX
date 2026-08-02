package state

import "fmt"

// InitializeRun atomically records a run's initial state and compatibility
// hash in one locked replacement cycle.
func (store YAMLStore) InitializeRun(run Run, configHash string) error {
	if err := validateRunRecord(run); err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		for _, existing := range document.Runs {
			if existing.ID == run.ID && existing.Outcome == run.Outcome {
				return fmt.Errorf("record initial run state: duplicate run outcome %q/%q", run.ID, run.Outcome)
			}
		}
		if _, exists := document.ConfigHashes[run.ID]; exists {
			return fmt.Errorf("save initial configuration hash: duplicate run %q", run.ID)
		}

		run.StartedAt = run.StartedAt.UTC()
		if !run.EndedAt.IsZero() {
			run.EndedAt = run.EndedAt.UTC()
		}
		document.Runs = append(document.Runs, run)
		if document.ConfigHashes == nil {
			document.ConfigHashes = make(map[string]string)
		}
		document.ConfigHashes[run.ID] = configHash
		return nil
	})
}

// CreateTasks atomically creates all selected table checkpoints in one locked
// replacement cycle.
func (store YAMLStore) CreateTasks(tasks []Task) error {
	return store.update(func(document *yamlStateDocument) error {
		type taskKey struct {
			runID string
			table string
		}
		known := make(map[taskKey]struct{}, len(document.Tasks)+len(tasks))
		for _, task := range document.Tasks {
			known[taskKey{runID: task.RunID, table: task.Table}] = struct{}{}
		}
		for _, task := range tasks {
			key := taskKey{runID: task.RunID, table: task.Table}
			if _, exists := known[key]; exists {
				return fmt.Errorf("create table checkpoint: duplicate task %q", task.Table)
			}
			known[key] = struct{}{}
		}
		for _, task := range tasks {
			document.Tasks = append(document.Tasks, Task{
				RunID:     task.RunID,
				Table:     task.Table,
				Status:    "running",
				StartedAt: task.StartedAt.UTC(),
			})
		}
		return nil
	})
}
