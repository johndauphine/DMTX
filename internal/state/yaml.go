package state

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

const yamlStateVersion = 5

var (
	yamlStateBeforeReplace = func(string, string) error { return nil }
	yamlStateAfterReplace  = func(string) error { return nil }
)

// YAMLStore persists restartable migration state in one human-readable file.
//
// Every mutation holds an operating-system file lock across the complete
// read/compare/write cycle. The replacement file is written in the same
// directory, flushed, and atomically renamed over the previous state.
type YAMLStore struct {
	Path string
}

type yamlStateDocument struct {
	Version                       int                                   `yaml:"version"`
	Runs                          []Run                                 `yaml:"runs,omitempty"`
	Tasks                         []Task                                `yaml:"tasks,omitempty"`
	ConfigHashes                  map[string]string                     `yaml:"config_hashes,omitempty"`
	ResumeCompatibilityHashes     map[string]string                     `yaml:"resume_compatibility_hashes,omitempty"`
	WorkTasks                     []WorkTask                            `yaml:"work_tasks,omitempty"`
	WorkRanges                    []RangeState                          `yaml:"work_ranges,omitempty"`
	SchemaSnapshots               []SchemaSnapshot                      `yaml:"schema_snapshots,omitempty"`
	Stage4TableInventories        []Stage4TableInventoryReceipt         `yaml:"stage4_table_inventories,omitempty"`
	Stage4TableCompletions        []Stage4TableCompletionReceipt        `yaml:"stage4_table_completions,omitempty"`
	Stage4RebuildFinalizations    []Stage4RebuildFinalizationReceipt    `yaml:"stage4_rebuild_finalizations,omitempty"`
	Stage4RebuildReadiness        []Stage4RebuildReadyReceipt           `yaml:"stage4_rebuild_readiness,omitempty"`
	Stage4DeleteJournalReadiness  []Stage4DeleteJournalReadinessReceipt `yaml:"stage4_delete_journal_readiness,omitempty"`
	IncrementalAttempts           []IncrementalAttempt                  `yaml:"incremental_attempts,omitempty"`
	DeleteReconciliations         []DeleteReconciliation                `yaml:"delete_reconciliations,omitempty"`
	StrictMigrationSnapshots      []StrictMigrationSnapshot             `yaml:"strict_migration_snapshots,omitempty"`
	StrictMigrationCleanupIntents []StrictMigrationCleanupIntent        `yaml:"strict_migration_cleanup_intents,omitempty"`
	StrictSnapshotEvidence        []StrictSnapshotEvidence              `yaml:"strict_snapshot_evidence,omitempty"`
}

// Append records a state transition for a migration run.
func (store YAMLStore) Append(run Run) error {
	if err := validateRunRecord(run); err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		for _, existing := range document.Runs {
			if existing.ID != run.ID {
				continue
			}
			var err error
			run, err = inheritRunWorkloadIdentity(existing, run)
			if err != nil {
				return err
			}
		}
		for _, existing := range document.Runs {
			if existing.ID == run.ID && existing.Outcome == run.Outcome {
				return fmt.Errorf("record run state: duplicate run outcome %q/%q", run.ID, run.Outcome)
			}
		}
		run.StartedAt = run.StartedAt.UTC()
		if !run.EndedAt.IsZero() {
			run.EndedAt = run.EndedAt.UTC()
		}
		document.Runs = append(document.Runs, run)
		return nil
	})
}

// List returns migration runs in chronological order.
func (store YAMLStore) List() ([]Run, error) {
	var runs []Run
	err := store.read(func(document yamlStateDocument) error {
		runs = orderedRuns(document.Runs)
		return nil
	})
	return runs, err
}

// Latest returns the most recently recorded run state.
func (store YAMLStore) Latest() (Run, bool, error) {
	var latest Run
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		runs := orderedRuns(document.Runs)
		if len(runs) == 0 {
			return nil
		}
		latest = runs[len(runs)-1]
		found = true
		return nil
	})
	return latest, found, err
}

// LatestResumableForTarget selects the newest resumable run that has not been
// superseded by a successful migration to the same target.
func (store YAMLStore) LatestResumableForTarget(target string) (Run, bool, error) {
	var selected Run
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		runs := orderedRuns(document.Runs)
		selected, found = latestResumableRun(runs, target)
		return nil
	})
	return selected, found, err
}

// UpdateFailure records the latest recoverable error for an existing run.
func (store YAMLStore) UpdateFailure(runID, reason string, endedAt time.Time) error {
	return store.UpdateRecoverableOutcome(runID, Failed, reason, endedAt)
}

// CreateTask writes a table checkpoint before its target mutation begins.
func (store YAMLStore) CreateTask(task Task) error {
	return store.update(func(document *yamlStateDocument) error {
		for _, existing := range document.Tasks {
			if existing.RunID == task.RunID && existing.Table == task.Table {
				return fmt.Errorf("create table checkpoint: duplicate task %q", task.Table)
			}
		}
		document.Tasks = append(document.Tasks, Task{
			RunID:     task.RunID,
			Table:     task.Table,
			Status:    "running",
			StartedAt: task.StartedAt.UTC(),
		})
		return nil
	})
}

// AdvanceIntegerKeysetTask records a target-acknowledged page frontier.
func (store YAMLStore) AdvanceIntegerKeysetTask(runID, table string, rowsDone int, watermark int64) error {
	return store.advanceTask(runID, table, rowsDone, func(task *Task) {
		task.IntegerWatermark = int64Pointer(watermark)
	})
}

// AdvanceRowNumberTask records a target-acknowledged row-number frontier.
func (store YAMLStore) AdvanceRowNumberTask(runID, table string, rowsDone int, watermark int64) error {
	return store.advanceTask(runID, table, rowsDone, func(task *Task) {
		task.RowNumberWatermark = int64Pointer(watermark)
	})
}

func (store YAMLStore) advanceTask(runID, table string, rowsDone int, update func(*Task)) error {
	return store.update(func(document *yamlStateDocument) error {
		for index := range document.Tasks {
			task := &document.Tasks[index]
			if task.RunID == runID && task.Table == table && task.Status == "running" {
				task.RowsDone = rowsDone
				update(task)
				return nil
			}
		}
		return fmt.Errorf("advance table checkpoint: unknown or non-running task %q", table)
	})
}

// CompleteTask records the validated completion frontier for a table.
func (store YAMLStore) CompleteTask(runID, table string, rowsDone int, completedAt time.Time) error {
	return store.update(func(document *yamlStateDocument) error {
		for index := range document.Tasks {
			task := &document.Tasks[index]
			if task.RunID == runID && task.Table == table && task.Status == "running" {
				task.Status = "completed"
				task.RowsDone = rowsDone
				task.CompletedAt = completedAt.UTC()
				return nil
			}
		}
		return fmt.Errorf("complete table checkpoint: unknown or non-running task %q", table)
	})
}

// ListTasks returns a run's table checkpoints in deterministic table order.
func (store YAMLStore) ListTasks(runID string) ([]Task, error) {
	var tasks []Task
	err := store.read(func(document yamlStateDocument) error {
		for _, task := range document.Tasks {
			if task.RunID == runID {
				tasks = append(tasks, task)
			}
		}
		sort.SliceStable(tasks, func(left, right int) bool {
			return tasks[left].Table < tasks[right].Table
		})
		return nil
	})
	return tasks, err
}

// SaveConfigHash records the sanitized data-plane configuration for a run.
func (store YAMLStore) SaveConfigHash(runID, hash string) error {
	return store.update(func(document *yamlStateDocument) error {
		if document.ConfigHashes == nil {
			document.ConfigHashes = make(map[string]string)
		}
		if _, exists := document.ConfigHashes[runID]; exists {
			return fmt.Errorf("save configuration hash: duplicate run %q", runID)
		}
		document.ConfigHashes[runID] = hash
		return nil
	})
}

// ConfigHash returns the recorded sanitized data-plane configuration hash.
func (store YAMLStore) ConfigHash(runID string) (string, bool, error) {
	var hash string
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		hash, found = document.ConfigHashes[runID]
		return nil
	})
	return hash, found, err
}

func (store YAMLStore) read(read func(yamlStateDocument) error) error {
	return store.withLock(false, func() error {
		document, err := store.loadUnlocked()
		if err != nil {
			return err
		}
		return read(document)
	})
}

func (store YAMLStore) update(update func(*yamlStateDocument) error) error {
	return store.withLock(true, func() error {
		document, err := store.loadUnlocked()
		if err != nil {
			return err
		}
		if err := update(&document); err != nil {
			return err
		}
		return store.writeUnlocked(document)
	})
}

func (store YAMLStore) withLock(exclusive bool, operation func() error) (err error) {
	if store.Path == "" {
		return errors.New("YAML state path is required")
	}
	directory := filepath.Dir(store.Path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	lockFile, err := os.OpenFile(store.Path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open state lock: %w", err)
	}
	locked := false
	defer func() {
		if locked {
			if unlockErr := unlockStateFile(lockFile); unlockErr != nil {
				err = errors.Join(err, fmt.Errorf("unlock state file: %w", unlockErr))
			}
		}
		if closeErr := lockFile.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close state lock: %w", closeErr))
		}
	}()
	if err := lockStateFile(lockFile, exclusive); err != nil {
		return fmt.Errorf("lock state file: %w", err)
	}
	locked = true
	return operation()
}

func (store YAMLStore) loadUnlocked() (yamlStateDocument, error) {
	document := yamlStateDocument{Version: yamlStateVersion}
	data, err := os.ReadFile(store.Path)
	if os.IsNotExist(err) {
		return document, nil
	}
	if err != nil {
		return yamlStateDocument{}, fmt.Errorf("read YAML state: %w", err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return document, nil
	}
	if err := yaml.Unmarshal(data, &document); err != nil {
		return yamlStateDocument{}, fmt.Errorf("decode YAML state: %w", err)
	}
	if document.Version < 0 || document.Version > yamlStateVersion {
		return yamlStateDocument{}, fmt.Errorf("decode YAML state: unsupported version %d", document.Version)
	}
	for _, run := range document.Runs {
		if err := validateRunRecord(run); err != nil {
			return yamlStateDocument{}, fmt.Errorf("decode YAML state: %w", err)
		}
	}
	// Older layouts are retained verbatim. Stage 2 work topology and Stage 4
	// restartability evidence are added on the next mutation.
	document.Version = yamlStateVersion
	return document, nil
}

func (store YAMLStore) writeUnlocked(document yamlStateDocument) error {
	document.Version = yamlStateVersion
	data, err := yaml.Marshal(document)
	if err != nil {
		return fmt.Errorf("encode YAML state: %w", err)
	}

	directory := filepath.Dir(store.Path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.Path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary YAML state: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()

	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure temporary YAML state: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary YAML state: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("flush temporary YAML state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary YAML state: %w", err)
	}
	if err := yamlStateBeforeReplace(temporaryPath, store.Path); err != nil {
		return fmt.Errorf("prepare YAML state replacement: %w", err)
	}
	if err := replaceStateFile(temporaryPath, store.Path); err != nil {
		return fmt.Errorf("replace YAML state: %w", err)
	}
	removeTemporary = false
	if err := yamlStateAfterReplace(store.Path); err != nil {
		return fmt.Errorf("acknowledge YAML state replacement: %w", err)
	}
	if err := syncStateDirectory(directory); err != nil {
		return fmt.Errorf("flush YAML state directory: %w", err)
	}
	return nil
}

func orderedRuns(runs []Run) []Run {
	ordered := append([]Run(nil), runs...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].StartedAt.Before(ordered[right].StartedAt)
	})
	return ordered
}

func int64Pointer(value int64) *int64 {
	return &value
}
