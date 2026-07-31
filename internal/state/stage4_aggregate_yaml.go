package state

import "fmt"

func (store YAMLStore) EnsureStage4TableInventory(
	inventory Stage4TableInventory,
) error {
	normalized, err := normalizeStage4TableInventory(inventory)
	if err != nil {
		return stage4AggregateError("table inventory", err)
	}
	err = store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		var runs []Run
		for _, run := range document.Runs {
			if run.ID == normalized.RunID {
				runs = append(runs, run)
			}
		}
		latest, err := validateStage4RunIdentity(runs, normalized.RunID)
		if err != nil {
			return false, err
		}
		var stored Stage4TableInventoryReceipt
		found := false
		for _, candidate := range document.Stage4TableInventories {
			if candidate.Inventory.RunID != normalized.RunID {
				continue
			}
			if found {
				return false, fmt.Errorf(
					"%w: duplicate Stage 4 table inventory",
					ErrImmutableEvidence,
				)
			}
			stored, found = candidate, true
		}
		if found {
			return false, validateStage4TableInventoryReceipt(
				stored,
				normalized,
			)
		}
		if latest.Outcome != Running || !latest.Resumable {
			return false, fmt.Errorf(
				"%w: table inventory requires an active resumable run",
				ErrStateTransition,
			)
		}
		for _, task := range document.Tasks {
			if task.RunID == normalized.RunID {
				return false, fmt.Errorf(
					"%w: table inventory must precede ordinary table evidence",
					ErrStateTransition,
				)
			}
		}
		var workTasks []WorkTask
		var workRanges []RangeState
		for _, task := range document.WorkTasks {
			if task.RunID == normalized.RunID {
				workTasks = append(workTasks, task)
			}
		}
		for _, workRange := range document.WorkRanges {
			if workRange.RunID == normalized.RunID {
				workRanges = append(workRanges, workRange)
			}
		}
		if err := validateStage4InventoryAuthority(
			normalized,
			workTasks,
			workRanges,
		); err != nil {
			return false, err
		}
		snapshots := make(map[TaskKey]SchemaSnapshot)
		for _, snapshot := range document.SchemaSnapshots {
			if snapshot.RunID != normalized.RunID {
				continue
			}
			if _, duplicate := snapshots[snapshot.Task]; duplicate {
				return false, fmt.Errorf(
					"%w: duplicate schema snapshot for task %#v",
					ErrImmutableEvidence,
					snapshot.Task,
				)
			}
			snapshots[snapshot.Task] = snapshot
		}
		if err := validateStage4InventorySnapshot(
			normalized,
			snapshots,
		); err != nil {
			return false, err
		}
		for _, receipt := range document.Stage4TableCompletions {
			if receipt.Completion.RunID == normalized.RunID {
				return false, fmt.Errorf(
					"%w: table inventory must precede table mutation evidence",
					ErrStateTransition,
				)
			}
		}
		for _, attempt := range document.IncrementalAttempts {
			if attempt.RunID == normalized.RunID {
				return false, fmt.Errorf(
					"%w: table inventory must precede table mutation evidence",
					ErrStateTransition,
				)
			}
		}
		for _, record := range document.DeleteReconciliations {
			if record.RunID == normalized.RunID {
				return false, fmt.Errorf(
					"%w: table inventory must precede table mutation evidence",
					ErrStateTransition,
				)
			}
		}
		receipt, err := newStage4TableInventoryReceipt(normalized)
		if err != nil {
			return false, err
		}
		document.Stage4TableInventories = append(
			document.Stage4TableInventories,
			receipt,
		)
		if err := stage4BeforeInventoryCommit(); err != nil {
			return false, fmt.Errorf(
				"prepare Stage 4 table inventory commit: %w",
				err,
			)
		}
		return true, nil
	})
	if err != nil {
		return stage4AggregateError("table inventory", err)
	}
	return nil
}

func (store YAMLStore) CompleteStage4Table(
	completion Stage4TableCompletion,
) error {
	normalized, err := normalizeStage4TableCompletion(completion)
	if err != nil {
		return stage4AggregateError("table completion", err)
	}
	err = store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		taskIndex := -1
		for index, task := range document.Tasks {
			if task.RunID != normalized.RunID ||
				task.Table != normalized.Table {
				continue
			}
			if taskIndex >= 0 {
				return false, fmt.Errorf(
					"%w: duplicate ordinary task %q",
					ErrImmutableEvidence,
					normalized.Table,
				)
			}
			taskIndex = index
		}
		if taskIndex < 0 {
			return false, fmt.Errorf(
				"%w: ordinary task %q",
				ErrUnknownWork,
				normalized.Table,
			)
		}
		var runs []Run
		for _, run := range document.Runs {
			if run.ID == normalized.RunID {
				runs = append(runs, run)
			}
		}
		if err := validateStage4TableRun(
			runs,
			normalized,
			document.Tasks[taskIndex].Status == "completed",
		); err != nil {
			return false, err
		}
		var inventory Stage4TableInventoryReceipt
		inventoryFound := false
		for _, candidate := range document.Stage4TableInventories {
			if candidate.Inventory.RunID != normalized.RunID {
				continue
			}
			if inventoryFound {
				return false, fmt.Errorf(
					"%w: duplicate Stage 4 table inventory",
					ErrImmutableEvidence,
				)
			}
			inventory, inventoryFound = candidate, true
		}
		if !inventoryFound {
			return false, fmt.Errorf(
				"%w: durable Stage 4 table inventory",
				ErrUnknownWork,
			)
		}
		workTaskIndex := -1
		for index, task := range document.WorkTasks {
			if task.RunID != normalized.RunID ||
				task.Key != normalized.Task {
				continue
			}
			if workTaskIndex >= 0 {
				return false, fmt.Errorf(
					"%w: duplicate structured task",
					ErrImmutableEvidence,
				)
			}
			workTaskIndex = index
		}
		if workTaskIndex < 0 {
			return false, fmt.Errorf(
				"%w: structured table task",
				ErrUnknownWork,
			)
		}
		var ranges []RangeState
		var rangeIndexes []int
		for index, workRange := range document.WorkRanges {
			if workRange.RunID == normalized.RunID &&
				workRange.Task == normalized.Task {
				ranges = append(ranges, workRange)
				rangeIndexes = append(rangeIndexes, index)
			}
		}
		if err := validateStage4InventoryAuthorizesTable(
			inventory,
			normalized,
			document.WorkTasks[workTaskIndex],
			ranges,
		); err != nil {
			return false, err
		}
		var attempt *IncrementalAttempt
		attemptIndex := -1
		for index, candidate := range document.IncrementalAttempts {
			if candidate.RunID != normalized.RunID ||
				candidate.Task != normalized.Task {
				continue
			}
			if attempt != nil {
				return false, fmt.Errorf(
					"%w: multiple incremental attempts for table",
					ErrStateTransition,
				)
			}
			copy := candidate
			attempt = &copy
			attemptIndex = index
		}
		var receipt Stage4TableCompletionReceipt
		receiptFound := false
		for _, candidate := range document.Stage4TableCompletions {
			if candidate.Completion.RunID != normalized.RunID ||
				candidate.Completion.Task != normalized.Task {
				continue
			}
			if receiptFound {
				return false, fmt.Errorf(
					"%w: duplicate aggregate table receipt",
					ErrImmutableEvidence,
				)
			}
			receipt, receiptFound = candidate, true
		}
		replay := document.Tasks[taskIndex].Status == "completed"
		ordinary, workTask, nextRanges, nextAttempt, err :=
			applyStage4TableCompletion(
				normalized,
				document.Tasks[taskIndex],
				document.WorkTasks[workTaskIndex],
				ranges,
				attempt,
			)
		if err != nil {
			return false, err
		}
		if replay {
			if !receiptFound {
				return false, fmt.Errorf(
					"%w: completed table lacks an aggregate receipt",
					ErrStateTransition,
				)
			}
			return false, validateStage4TableCompletionReceipt(
				receipt,
				normalized,
			)
		}
		if receiptFound {
			return false, fmt.Errorf(
				"%w: running table already has an aggregate receipt",
				ErrStateTransition,
			)
		}
		document.Tasks[taskIndex] = ordinary
		document.WorkTasks[workTaskIndex] = workTask
		rangeByID := make(map[string]RangeState, len(nextRanges))
		for _, workRange := range nextRanges {
			rangeByID[workRange.ID] = workRange
		}
		for _, index := range rangeIndexes {
			document.WorkRanges[index] =
				rangeByID[document.WorkRanges[index].ID]
		}
		if nextAttempt != nil {
			document.IncrementalAttempts[attemptIndex] = *nextAttempt
		}
		nextReceipt, err := newStage4TableCompletionReceipt(normalized)
		if err != nil {
			return false, err
		}
		document.Stage4TableCompletions = append(
			document.Stage4TableCompletions,
			nextReceipt,
		)
		if err := stage4BeforeAggregateTableCommit(); err != nil {
			return false, fmt.Errorf("prepare aggregate table commit: %w", err)
		}
		return true, nil
	})
	if err != nil {
		return stage4AggregateError("table completion", err)
	}
	return nil
}

func (store YAMLStore) CompleteStage4Run(
	completion Stage4RunCompletion,
) error {
	normalized, err := normalizeStage4RunCompletion(completion)
	if err != nil {
		return stage4AggregateError("run completion", err)
	}
	err = store.updateStage4Aggregate(func(
		document *yamlStateDocument,
	) (bool, error) {
		var runs []Run
		for _, run := range document.Runs {
			if run.ID == normalized.RunID {
				runs = append(runs, run)
			}
		}
		latest, err := validateStage4RunIdentity(runs, normalized.RunID)
		if err != nil {
			return false, err
		}
		replay := latest.Outcome == Success
		switch {
		case replay && !stage4RunReplayMatches(latest, normalized):
			return false, fmt.Errorf(
				"%w: successful run publication differs",
				ErrImmutableEvidence,
			)
		case !replay && (latest.Outcome != Running || !latest.Resumable):
			return false, fmt.Errorf(
				"%w: run is not an active resumable migration",
				ErrStateTransition,
			)
		case normalized.CompletedAt.Before(latest.StartedAt):
			return false, fmt.Errorf(
				"%w: run completion precedes start",
				ErrStateTransition,
			)
		}
		var inventory Stage4TableInventoryReceipt
		inventoryFound := false
		for _, candidate := range document.Stage4TableInventories {
			if candidate.Inventory.RunID != normalized.RunID {
				continue
			}
			if inventoryFound {
				return false, fmt.Errorf(
					"%w: duplicate Stage 4 table inventory",
					ErrImmutableEvidence,
				)
			}
			inventory, inventoryFound = candidate, true
		}
		if !inventoryFound {
			return false, fmt.Errorf(
				"%w: durable Stage 4 table inventory",
				ErrUnknownWork,
			)
		}
		if err := validateStage4RunInventory(
			inventory,
			normalized,
		); err != nil {
			return false, err
		}

		var ordinary []Task
		for _, task := range document.Tasks {
			if task.RunID == normalized.RunID {
				ordinary = append(ordinary, task)
			}
		}
		if err := validateStage4RunRowsComplete(
			normalized.RunID,
			normalized.CompletedAt,
			ordinary,
		); err != nil {
			return false, err
		}

		var workTasks []WorkTask
		var workRanges []RangeState
		documentTaskIndexes := make(map[TaskKey]int)
		documentRangeIndexes := make(map[TaskKey][]int)
		for index, task := range document.WorkTasks {
			if task.RunID != normalized.RunID {
				continue
			}
			if _, duplicate := documentTaskIndexes[task.Key]; duplicate {
				return false, fmt.Errorf(
					"%w: duplicate structured task %#v",
					ErrImmutableEvidence,
					task.Key,
				)
			}
			documentTaskIndexes[task.Key] = index
			workTasks = append(workTasks, task)
		}
		for index, workRange := range document.WorkRanges {
			if workRange.RunID != normalized.RunID {
				continue
			}
			documentRangeIndexes[workRange.Task] = append(
				documentRangeIndexes[workRange.Task],
				index,
			)
			workRanges = append(workRanges, workRange)
		}
		if err := validateStage4KnownWorkInventory(workTasks); err != nil {
			return false, err
		}
		_, rangesByTask, err := indexStage4AggregateWork(
			normalized.RunID,
			workTasks,
			workRanges,
		)
		if err != nil {
			return false, err
		}
		if err := validateStage4DurableInventoryWork(
			inventory,
			workTasks,
			rangesByTask,
		); err != nil {
			return false, err
		}
		if err := validateStage4SentinelInventory(
			normalized,
			workTasks,
			rangesByTask,
		); err != nil {
			return false, err
		}
		snapshots := make(map[TaskKey]SchemaSnapshot)
		for _, candidate := range document.SchemaSnapshots {
			if candidate.RunID != normalized.RunID {
				continue
			}
			if _, duplicate := snapshots[candidate.Task]; duplicate {
				return false, fmt.Errorf(
					"%w: duplicate schema snapshot for task %#v",
					ErrImmutableEvidence,
					candidate.Task,
				)
			}
			snapshots[candidate.Task] = candidate
		}
		if len(snapshots) != len(normalized.Sentinels) {
			return false, fmt.Errorf(
				"%w: exact schema sentinel inventory differs",
				ErrImmutableEvidence,
			)
		}
		for _, sentinel := range normalized.Sentinels {
			taskIndex, ok := documentTaskIndexes[sentinel.Task]
			if !ok {
				return false, fmt.Errorf(
					"%w: schema sentinel task %#v",
					ErrUnknownWork,
					sentinel.Task,
				)
			}
			stored, snapshotFound := snapshots[sentinel.Task]
			if !snapshotFound {
				return false, fmt.Errorf(
					"%w: schema snapshot for sentinel %#v",
					ErrUnknownWork,
					sentinel.Task,
				)
			}
			if !stage4SnapshotMatches(stored, sentinel.Snapshot) {
				return false, fmt.Errorf(
					"%w: schema snapshot for sentinel %#v differs",
					ErrImmutableEvidence,
					sentinel.Task,
				)
			}
			nextTask, nextRanges, wasComplete, err :=
				applyStage4StructuredCompletion(
					normalized.RunID,
					sentinel.Task,
					sentinel.TopologyHash,
					[]Stage4RangeCompletion{{
						ID:           sentinel.RangeID,
						NextSequence: sentinel.NextSequence,
					}},
					normalized.CompletedAt,
					document.WorkTasks[taskIndex],
					rangesByTask[sentinel.Task],
					true,
				)
			if err != nil {
				return false, err
			}
			if replay != wasComplete {
				return false, fmt.Errorf(
					"%w: schema sentinel and run publication are partial",
					ErrStateTransition,
				)
			}
			document.WorkTasks[taskIndex] = nextTask
			rangesByTask[sentinel.Task] = nextRanges
			if !replay {
				rangeByID := make(map[string]RangeState, len(nextRanges))
				for _, workRange := range nextRanges {
					rangeByID[workRange.ID] = workRange
				}
				for _, index := range documentRangeIndexes[sentinel.Task] {
					document.WorkRanges[index] =
						rangeByID[document.WorkRanges[index].ID]
				}
			}
		}
		updatedTasks := make([]WorkTask, 0, len(documentTaskIndexes))
		for _, index := range documentTaskIndexes {
			updatedTasks = append(updatedTasks, document.WorkTasks[index])
		}
		if err := validateStage4AggregateWorkComplete(
			updatedTasks,
			rangesByTask,
			normalized.CompletedAt,
		); err != nil {
			return false, err
		}
		var incremental []IncrementalAttempt
		for _, attempt := range document.IncrementalAttempts {
			if attempt.RunID == normalized.RunID {
				incremental = append(incremental, attempt)
			}
		}
		var deletes []DeleteReconciliation
		for _, record := range document.DeleteReconciliations {
			if record.RunID == normalized.RunID {
				deletes = append(deletes, record)
			}
		}
		receipts := make(map[TaskKey]Stage4TableCompletionReceipt)
		for _, receipt := range document.Stage4TableCompletions {
			if receipt.Completion.RunID != normalized.RunID {
				continue
			}
			task := receipt.Completion.Task
			if _, duplicate := receipts[task]; duplicate {
				return false, fmt.Errorf(
					"%w: duplicate aggregate table receipt for task %#v",
					ErrImmutableEvidence,
					task,
				)
			}
			receipts[task] = receipt
		}
		if err := validateStage4ExpectedTableInventory(
			normalized,
			ordinary,
			updatedTasks,
			rangesByTask,
			incremental,
			receipts,
		); err != nil {
			return false, err
		}
		if err := validateStage4TerminalRecords(
			incremental,
			deletes,
			normalized,
		); err != nil {
			return false, err
		}
		if replay {
			return false, nil
		}
		success := latest
		success.Outcome = Success
		success.Resumable = false
		success.Reason = normalized.Reason
		success.EndedAt = normalized.CompletedAt
		document.Runs = append(document.Runs, success)
		if err := stage4BeforeAggregateRunCommit(); err != nil {
			return false, fmt.Errorf("prepare aggregate run commit: %w", err)
		}
		return true, nil
	})
	if err != nil {
		return stage4AggregateError("run completion", err)
	}
	return nil
}

func (store YAMLStore) updateStage4Aggregate(
	update func(*yamlStateDocument) (bool, error),
) error {
	return store.withLock(true, func() error {
		document, err := store.loadUnlocked()
		if err != nil {
			return err
		}
		changed, err := update(&document)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		return store.writeUnlocked(document)
	})
}

var _ Stage4AggregateBackend = YAMLStore{}
