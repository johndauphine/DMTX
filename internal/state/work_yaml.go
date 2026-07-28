package state

import (
	"fmt"
	"sort"
	"time"
)

func (store YAMLStore) EnsureWorkPlan(task WorkTask, ranges []RangeState) (bool, error) {
	task, ranges, err := validateWorkPlan(task, ranges)
	if err != nil {
		return false, err
	}
	created := false
	err = store.update(func(document *yamlStateDocument) error {
		for _, existing := range document.WorkTasks {
			if existing.RunID != task.RunID || existing.Key != task.Key {
				continue
			}
			var existingRanges []RangeState
			for _, workRange := range document.WorkRanges {
				if workRange.RunID == task.RunID && workRange.Task == task.Key {
					existingRanges = append(existingRanges, workRange)
				}
			}
			sort.Slice(existingRanges, func(left, right int) bool { return existingRanges[left].ID < existingRanges[right].ID })
			if !workPlanEqual(existing, existingRanges, task, ranges) {
				return fmt.Errorf("%w for task %s", ErrTopologyChanged, task.Key.Table)
			}
			return nil
		}
		document.WorkTasks = append(document.WorkTasks, task)
		document.WorkRanges = append(document.WorkRanges, ranges...)
		created = true
		return nil
	})
	return created, err
}

func (store YAMLStore) ResetWorkPlan(task WorkTask, ranges []RangeState) error {
	task, ranges, err := validateWorkPlan(task, ranges)
	if err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		found := false
		for index := range document.WorkTasks {
			if document.WorkTasks[index].RunID == task.RunID && document.WorkTasks[index].Key == task.Key {
				document.WorkTasks[index] = task
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w: task %s", ErrUnknownWork, task.Key.Table)
		}
		kept := document.WorkRanges[:0]
		for _, workRange := range document.WorkRanges {
			if workRange.RunID != task.RunID || workRange.Task != task.Key {
				kept = append(kept, workRange)
			}
		}
		document.WorkRanges = append(kept, ranges...)
		return nil
	})
}

func (store YAMLStore) ListWork(runID string) ([]WorkTask, []RangeState, error) {
	var tasks []WorkTask
	var ranges []RangeState
	err := store.read(func(document yamlStateDocument) error {
		for _, task := range document.WorkTasks {
			if task.RunID == runID {
				tasks = append(tasks, task)
			}
		}
		for _, workRange := range document.WorkRanges {
			if workRange.RunID == runID {
				ranges = append(ranges, workRange)
			}
		}
		sort.Slice(tasks, func(left, right int) bool {
			leftKey, _ := tasks[left].Key.canonical()
			rightKey, _ := tasks[right].Key.canonical()
			return leftKey < rightKey
		})
		sort.Slice(ranges, func(left, right int) bool {
			leftKey, _ := ranges[left].Task.canonical()
			rightKey, _ := ranges[right].Task.canonical()
			if leftKey != rightKey {
				return leftKey < rightKey
			}
			return ranges[left].ID < ranges[right].ID
		})
		return nil
	})
	return tasks, ranges, err
}

func (store YAMLStore) BeginRangeChunk(intent RangeChunkIntent) error {
	return store.update(func(document *yamlStateDocument) error {
		for index := range document.WorkRanges {
			workRange := document.WorkRanges[index]
			if workRange.RunID != intent.RunID || workRange.Task != intent.Task || workRange.ID != intent.RangeID {
				continue
			}
			updated, err := applyRangeChunkIntent(workRange, intent)
			if err != nil {
				return err
			}
			document.WorkRanges[index] = updated
			return nil
		}
		return fmt.Errorf("%w: range %q", ErrUnknownWork, intent.RangeID)
	})
}

func (store YAMLStore) RecordRangeAttempt(attempt RangeAttempt) error {
	return store.update(func(document *yamlStateDocument) error {
		taskIndex := -1
		for index := range document.WorkTasks {
			workTask := document.WorkTasks[index]
			if workTask.RunID == attempt.RunID && workTask.Key == attempt.Task {
				taskIndex = index
				break
			}
		}
		if taskIndex < 0 {
			return fmt.Errorf("%w: task %s", ErrUnknownWork, attempt.Task.Table)
		}
		rangeIndex := -1
		for index := range document.WorkRanges {
			workRange := document.WorkRanges[index]
			if workRange.RunID == attempt.RunID && workRange.Task == attempt.Task && workRange.ID == attempt.RangeID {
				rangeIndex = index
				break
			}
		}
		if rangeIndex < 0 {
			return fmt.Errorf("%w: range %q", ErrUnknownWork, attempt.RangeID)
		}
		updatedTask, updatedRange, err := applyRangeAttempt(
			document.WorkTasks[taskIndex],
			document.WorkRanges[rangeIndex],
			attempt,
		)
		if err != nil {
			return err
		}
		document.WorkTasks[taskIndex] = updatedTask
		document.WorkRanges[rangeIndex] = updatedRange
		return nil
	})
}

func (store YAMLStore) AcknowledgeRange(acknowledgement RangeAcknowledgement) (RangeState, error) {
	var updated RangeState
	err := store.update(func(document *yamlStateDocument) error {
		for index := range document.WorkRanges {
			workRange := document.WorkRanges[index]
			if workRange.RunID != acknowledgement.RunID || workRange.Task != acknowledgement.Task || workRange.ID != acknowledgement.RangeID {
				continue
			}
			next, err := applyRangeAcknowledgement(workRange, acknowledgement)
			if err != nil {
				return err
			}
			document.WorkRanges[index] = next
			updated = next
			return nil
		}
		return fmt.Errorf("%w: range %q", ErrUnknownWork, acknowledgement.RangeID)
	})
	return updated, err
}

func (store YAMLStore) CompleteRange(runID string, task TaskKey, rangeID, topologyHash string, expectedNext uint64, completedAt time.Time) error {
	return store.update(func(document *yamlStateDocument) error {
		for index := range document.WorkRanges {
			workRange := &document.WorkRanges[index]
			if workRange.RunID != runID || workRange.Task != task || workRange.ID != rangeID {
				continue
			}
			if workRange.TopologyHash != topologyHash {
				return ErrTopologyChanged
			}
			if workRange.Status != "running" || workRange.NextSequence != expectedNext || workRange.SequenceOffset != 0 || len(workRange.Pending) != 0 {
				return fmt.Errorf("%w: range %q has incomplete acknowledgements", ErrRangeOrder, rangeID)
			}
			workRange.Status = "completed"
			workRange.CompletedAt, workRange.UpdatedAt = completedAt.UTC(), completedAt.UTC()
			return nil
		}
		return fmt.Errorf("%w: range %q", ErrUnknownWork, rangeID)
	})
}

func (store YAMLStore) CompleteWorkTask(runID string, task TaskKey, topologyHash string, completedAt time.Time) error {
	return store.update(func(document *yamlStateDocument) error {
		foundTask := -1
		for index := range document.WorkTasks {
			if document.WorkTasks[index].RunID == runID && document.WorkTasks[index].Key == task {
				foundTask = index
				break
			}
		}
		if foundTask < 0 {
			return fmt.Errorf("%w: task %s", ErrUnknownWork, task.Table)
		}
		for _, workRange := range document.WorkRanges {
			if workRange.RunID == runID && workRange.Task == task &&
				(workRange.TopologyHash != topologyHash || workRange.Status != "completed") {
				return fmt.Errorf("%w: task has incomplete or stale ranges", ErrRangeOrder)
			}
		}
		workTask := &document.WorkTasks[foundTask]
		if workTask.TopologyHash != topologyHash || workTask.Status != "running" {
			return fmt.Errorf("%w: task topology or status", ErrTopologyChanged)
		}
		workTask.Status = "completed"
		workTask.CompletedAt, workTask.UpdatedAt = completedAt.UTC(), completedAt.UTC()
		return nil
	})
}
