package state

import (
	"fmt"
	"reflect"
)

func requireYAMLStage4Identity(document yamlStateDocument, runID string, task TaskKey) error {
	if err := validateStage4Identity(runID, task); err != nil {
		return err
	}
	runFound := false
	for _, run := range document.Runs {
		if run.ID == runID {
			runFound = true
			break
		}
	}
	if !runFound {
		return fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	for _, workTask := range document.WorkTasks {
		if workTask.RunID == runID && workTask.Key == task {
			return nil
		}
	}
	taskKey, _ := task.canonical()
	return fmt.Errorf("%w: task %s", ErrUnknownWork, taskKey)
}

func requireYAMLRun(document yamlStateDocument, runID string) (Run, error) {
	var selected Run
	var found bool
	for _, run := range document.Runs {
		if run.ID != runID {
			continue
		}
		if found && (run.Source != selected.Source ||
			run.Target != selected.Target ||
			run.SourceEngine != selected.SourceEngine ||
			run.SourceIdentity != selected.SourceIdentity ||
			run.TargetIdentity != selected.TargetIdentity ||
			run.LeaseTarget != selected.LeaseTarget ||
			run.LeaseOwnerToken != selected.LeaseOwnerToken ||
			run.LeaseGeneration != selected.LeaseGeneration) {
			return Run{}, fmt.Errorf("%w: run %q endpoint identity changed", ErrImmutableEvidence, runID)
		}
		if !found || !run.StartedAt.Before(selected.StartedAt) {
			selected = run
		}
		found = true
	}
	if !found {
		return Run{}, fmt.Errorf("%w: run %q", ErrUnknownWork, runID)
	}
	if err := validateRunRecord(selected); err != nil {
		return Run{}, fmt.Errorf("%w: run %q: %v", ErrImmutableEvidence, runID, err)
	}
	return selected, nil
}

func loadYAMLLatestCommittedIncremental(
	document yamlStateDocument,
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	run, err := requireYAMLRun(document, runID)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	var latest IncrementalAttempt
	var found bool
	var sameRun IncrementalAttempt
	var sameRunFound bool
	var identityUnavailable bool
	var latestOrder, sameRunOrder stage4EvidenceOrder
	for _, candidate := range document.IncrementalAttempts {
		if candidate.Task != task ||
			candidate.Status != IncrementalCompleted ||
			!candidate.TableSucceeded {
			continue
		}
		candidateRun, err := requireYAMLRun(document, candidate.RunID)
		if err != nil {
			return IncrementalAttempt{}, false, err
		}
		candidateOrder := incrementalEvidenceOrder(
			candidateRun.StartedAt,
			candidate,
		)
		if candidate.RunID == runID {
			if !sameRunFound || laterStage4Evidence(candidateOrder, sameRunOrder) {
				sameRun, sameRunFound = candidate, true
				sameRunOrder = candidateOrder
			}
			continue
		}
		if candidate.RunID != runID {
			if candidateRun.Outcome != Success {
				continue
			}
			if run.SourceIdentity == "" || run.TargetIdentity == "" ||
				candidateRun.SourceIdentity == "" || candidateRun.TargetIdentity == "" {
				identityUnavailable = true
				continue
			}
			if candidateRun.SourceIdentity != run.SourceIdentity ||
				candidateRun.TargetIdentity != run.TargetIdentity {
				continue
			}
		}
		if !found || laterStage4Evidence(candidateOrder, latestOrder) {
			latest, found = candidate, true
			latestOrder = candidateOrder
		}
	}
	if sameRunFound {
		return sameRun, true, nil
	}
	if identityUnavailable {
		return IncrementalAttempt{}, false, fmt.Errorf(
			"%w: cannot select an incremental frontier for run %q",
			ErrCrossRunIdentityUnavailable,
			runID,
		)
	}
	if found {
		return latest, true, nil
	}
	return IncrementalAttempt{}, false, nil
}

func loadYAMLLatestSuccessfulDelete(
	document yamlStateDocument,
	runID string,
	task TaskKey,
) (DeleteReconciliation, bool, error) {
	run, err := requireYAMLRun(document, runID)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	var latest DeleteReconciliation
	var found bool
	var sameRun DeleteReconciliation
	var sameRunFound bool
	var identityUnavailable bool
	var latestOrder, sameRunOrder stage4EvidenceOrder
	for _, candidate := range document.DeleteReconciliations {
		if candidate.Task != task || candidate.Status != DeleteReconciliationCompleted {
			continue
		}
		candidateRun, err := requireYAMLRun(document, candidate.RunID)
		if err != nil {
			return DeleteReconciliation{}, false, err
		}
		candidateOrder := deleteEvidenceOrder(
			candidateRun.StartedAt,
			candidate,
		)
		if candidate.RunID == runID {
			if !sameRunFound || laterStage4Evidence(candidateOrder, sameRunOrder) {
				sameRun, sameRunFound = candidate, true
				sameRunOrder = candidateOrder
			}
			continue
		}
		if candidate.RunID != runID {
			if candidateRun.Outcome != Success {
				continue
			}
			if run.SourceIdentity == "" || run.TargetIdentity == "" ||
				candidateRun.SourceIdentity == "" || candidateRun.TargetIdentity == "" {
				identityUnavailable = true
				continue
			}
			if candidateRun.SourceIdentity != run.SourceIdentity ||
				candidateRun.TargetIdentity != run.TargetIdentity {
				continue
			}
		}
		if !found || laterStage4Evidence(candidateOrder, latestOrder) {
			latest, found = candidate, true
			latestOrder = candidateOrder
		}
	}
	if sameRunFound {
		return sameRun, true, nil
	}
	if identityUnavailable {
		return DeleteReconciliation{}, false, fmt.Errorf(
			"%w: cannot select delete reconciliation history for run %q",
			ErrCrossRunIdentityUnavailable,
			runID,
		)
	}
	if found {
		return latest, true, nil
	}
	return DeleteReconciliation{}, false, nil
}

func (store YAMLStore) SaveSchemaSnapshot(snapshot SchemaSnapshot) error {
	snapshot, err := normalizeSchemaSnapshot(snapshot)
	if err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(*document, snapshot.RunID, snapshot.Task); err != nil {
			return err
		}
		for _, existing := range document.SchemaSnapshots {
			if existing.RunID != snapshot.RunID || existing.Task != snapshot.Task {
				continue
			}
			if reflect.DeepEqual(existing, snapshot) {
				return nil
			}
			return fmt.Errorf("%w: schema snapshot", ErrImmutableEvidence)
		}
		document.SchemaSnapshots = append(document.SchemaSnapshots, snapshot)
		return nil
	})
}

func (store YAMLStore) LoadSchemaSnapshot(
	runID string,
	task TaskKey,
) (SchemaSnapshot, bool, error) {
	var snapshot SchemaSnapshot
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		for _, candidate := range document.SchemaSnapshots {
			if candidate.RunID == runID && candidate.Task == task {
				snapshot, found = candidate, true
				return nil
			}
		}
		return nil
	})
	return snapshot, found, err
}

func (store YAMLStore) LoadLatestApplicableSchemaSnapshot(
	runID string,
	task TaskKey,
) (SchemaSnapshot, bool, error) {
	var snapshot SchemaSnapshot
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		run, err := requireYAMLRun(document, runID)
		if err != nil {
			return err
		}
		var identityUnavailable bool
		var sameRun SchemaSnapshot
		var sameRunFound bool
		var latestOrder, sameRunOrder stage4EvidenceOrder
		for _, candidate := range document.SchemaSnapshots {
			if candidate.Task != task {
				continue
			}
			candidateRun, err := requireYAMLRun(document, candidate.RunID)
			if err != nil {
				return err
			}
			if candidateRun.Outcome != Success {
				continue
			}
			candidateOrder := schemaEvidenceOrder(
				candidateRun.StartedAt,
				candidate,
			)
			if candidate.RunID == runID {
				if !sameRunFound || laterStage4Evidence(candidateOrder, sameRunOrder) {
					sameRun, sameRunFound = candidate, true
					sameRunOrder = candidateOrder
				}
				continue
			}
			if run.SourceIdentity == "" || run.TargetIdentity == "" ||
				candidateRun.SourceIdentity == "" || candidateRun.TargetIdentity == "" {
				identityUnavailable = true
				continue
			}
			if candidateRun.SourceIdentity != run.SourceIdentity ||
				candidateRun.TargetIdentity != run.TargetIdentity {
				continue
			}
			if !found || laterStage4Evidence(candidateOrder, latestOrder) {
				snapshot, found = candidate, true
				latestOrder = candidateOrder
			}
		}
		if sameRunFound {
			snapshot, found = sameRun, true
			return nil
		}
		if identityUnavailable {
			return fmt.Errorf(
				"%w: cannot select schema history for run %q",
				ErrCrossRunIdentityUnavailable,
				runID,
			)
		}
		return nil
	})
	return snapshot, found, err
}

func (store YAMLStore) BeginIncrementalAttempt(
	attempt IncrementalAttempt,
) (IncrementalAttempt, bool, error) {
	attempt, err := normalizeIncrementalAttempt(attempt)
	if err != nil {
		return IncrementalAttempt{}, false, err
	}
	var stored IncrementalAttempt
	var created bool
	err = store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(*document, attempt.RunID, attempt.Task); err != nil {
			return err
		}
		for _, existing := range document.IncrementalAttempts {
			if existing.RunID != attempt.RunID || existing.Task != attempt.Task {
				continue
			}
			if !incrementalBeginMatches(existing, attempt) {
				return fmt.Errorf(
					"%w: one immutable incremental attempt already exists for this run and task",
					ErrImmutableEvidence,
				)
			}
			stored = existing
			return nil
		}
		if attempt.Mode == IncrementalWindow {
			latest, found, err := loadYAMLLatestCommittedIncremental(
				*document,
				attempt.RunID,
				attempt.Task,
			)
			if err != nil {
				return err
			}
			if err := validateIncrementalLowerWatermark(attempt, latest, found); err != nil {
				return err
			}
		}
		document.IncrementalAttempts = append(document.IncrementalAttempts, attempt)
		stored, created = attempt, true
		return nil
	})
	return stored, created, err
}

func (store YAMLStore) LoadIncrementalAttempt(
	runID string,
	task TaskKey,
	attemptID string,
) (IncrementalAttempt, bool, error) {
	var attempt IncrementalAttempt
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		for _, candidate := range document.IncrementalAttempts {
			if candidate.RunID == runID &&
				candidate.Task == task &&
				candidate.AttemptID == attemptID {
				attempt, found = candidate, true
				return nil
			}
		}
		return nil
	})
	return attempt, found, err
}

func (store YAMLStore) LoadActiveIncrementalAttempt(
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	var attempt IncrementalAttempt
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		for _, candidate := range document.IncrementalAttempts {
			if candidate.RunID == runID &&
				candidate.Task == task &&
				candidate.Status == IncrementalRunning {
				if found {
					return fmt.Errorf("%w: multiple active incremental attempts", ErrStateTransition)
				}
				attempt, found = candidate, true
			}
		}
		return nil
	})
	return attempt, found, err
}

func (store YAMLStore) LoadLatestCommittedIncrementalAttempt(
	runID string,
	task TaskKey,
) (IncrementalAttempt, bool, error) {
	var attempt IncrementalAttempt
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		var err error
		attempt, found, err = loadYAMLLatestCommittedIncremental(document, runID, task)
		return err
	})
	return attempt, found, err
}

func (store YAMLStore) CommitIncrementalAttempt(commit IncrementalCommit) error {
	if err := validateStage4Identity(commit.RunID, commit.Task); err != nil {
		return err
	}
	if commit.AttemptID == "" {
		return fmt.Errorf("incremental attempt ID is required")
	}
	return store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(*document, commit.RunID, commit.Task); err != nil {
			return err
		}
		workTaskIndex := -1
		for index := range document.WorkTasks {
			if document.WorkTasks[index].RunID == commit.RunID &&
				document.WorkTasks[index].Key == commit.Task {
				workTaskIndex = index
				break
			}
		}
		if workTaskIndex < 0 {
			return fmt.Errorf("%w: incremental aggregate work task", ErrUnknownWork)
		}
		workTask := document.WorkTasks[workTaskIndex]
		if workTask.TopologyHash != commit.TopologyHash {
			return fmt.Errorf("%w: incremental aggregate work task", ErrTopologyChanged)
		}
		for _, workRange := range document.WorkRanges {
			if workRange.RunID == commit.RunID && workRange.Task == commit.Task &&
				(workRange.TopologyHash != commit.TopologyHash || workRange.Status != "completed") {
				return fmt.Errorf("%w: incremental task has incomplete or stale ranges", ErrRangeOrder)
			}
		}
		for index := range document.IncrementalAttempts {
			attempt := document.IncrementalAttempts[index]
			if attempt.RunID != commit.RunID ||
				attempt.Task != commit.Task ||
				attempt.AttemptID != commit.AttemptID {
				continue
			}
			completed, err := applyIncrementalCommit(attempt, commit)
			if err != nil {
				return err
			}
			switch {
			case attempt.Status == IncrementalRunning && workTask.Status == "running":
				workTask.Status = "completed"
				workTask.CompletedAt = commit.CompletedAt.UTC()
				workTask.UpdatedAt = commit.CompletedAt.UTC()
			case attempt.Status == IncrementalCompleted && workTask.Status == "completed":
				if !workTask.CompletedAt.Equal(commit.CompletedAt.UTC()) {
					return fmt.Errorf("%w: incremental aggregate completion differs", ErrImmutableEvidence)
				}
			default:
				return fmt.Errorf(
					"%w: incremental attempt %q and aggregate task %q are inconsistent",
					ErrStateTransition,
					attempt.Status,
					workTask.Status,
				)
			}
			document.IncrementalAttempts[index] = completed
			document.WorkTasks[workTaskIndex] = workTask
			if err := stage4BeforeIncrementalCommit(); err != nil {
				return fmt.Errorf("prepare atomic incremental completion: %w", err)
			}
			return nil
		}
		return fmt.Errorf("%w: incremental attempt %q", ErrUnknownWork, commit.AttemptID)
	})
}

func (store YAMLStore) BeginDeleteReconciliation(
	record DeleteReconciliation,
) (DeleteReconciliation, bool, error) {
	record, err := normalizeDeleteReconciliation(record)
	if err != nil {
		return DeleteReconciliation{}, false, err
	}
	var stored DeleteReconciliation
	var created bool
	err = store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(*document, record.RunID, record.Task); err != nil {
			return err
		}
		for _, existing := range document.DeleteReconciliations {
			if existing.RunID != record.RunID ||
				existing.Task != record.Task ||
				existing.AttemptID != record.AttemptID {
				continue
			}
			if !deleteReconciliationBeginMatches(existing, record) {
				return fmt.Errorf("%w: delete reconciliation", ErrImmutableEvidence)
			}
			stored = existing
			return nil
		}
		document.DeleteReconciliations = append(document.DeleteReconciliations, record)
		stored, created = record, true
		return nil
	})
	return stored, created, err
}

func (store YAMLStore) LoadDeleteReconciliation(
	runID string,
	task TaskKey,
	attemptID string,
) (DeleteReconciliation, bool, error) {
	var record DeleteReconciliation
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		for _, candidate := range document.DeleteReconciliations {
			if candidate.RunID == runID &&
				candidate.Task == task &&
				candidate.AttemptID == attemptID {
				record, found = candidate, true
				return nil
			}
		}
		return nil
	})
	return record, found, err
}

func (store YAMLStore) LoadLatestSuccessfulDeleteReconciliation(
	runID string,
	task TaskKey,
) (DeleteReconciliation, bool, error) {
	var record DeleteReconciliation
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		var err error
		record, found, err = loadYAMLLatestSuccessfulDelete(document, runID, task)
		return err
	})
	return record, found, err
}

func (store YAMLStore) SaveDeleteReconciliationPlan(
	plan DeleteReconciliationPlan,
) error {
	return store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(
			*document,
			plan.RunID,
			plan.Task,
		); err != nil {
			return err
		}
		for index := range document.DeleteReconciliations {
			record := document.DeleteReconciliations[index]
			if record.RunID != plan.RunID ||
				record.Task != plan.Task ||
				record.AttemptID != plan.AttemptID {
				continue
			}
			next, err := applyDeleteReconciliationPlan(record, plan)
			if err != nil {
				return err
			}
			document.DeleteReconciliations[index] = next
			return nil
		}
		return fmt.Errorf(
			"%w: delete reconciliation %q",
			ErrUnknownWork,
			plan.AttemptID,
		)
	})
}

func (store YAMLStore) BeginDeleteReconciliationBatch(
	batch DeleteReconciliationBatch,
) (DeleteReconciliationBatch, bool, error) {
	var stored DeleteReconciliationBatch
	var created bool
	err := store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(
			*document,
			batch.RunID,
			batch.Task,
		); err != nil {
			return err
		}
		for index := range document.DeleteReconciliations {
			record := document.DeleteReconciliations[index]
			if record.RunID != batch.RunID ||
				record.Task != batch.Task ||
				record.AttemptID != batch.AttemptID {
				continue
			}
			next, normalized, wasCreated, err :=
				applyBeginDeleteReconciliationBatch(record, batch)
			if err != nil {
				return err
			}
			document.DeleteReconciliations[index] = next
			stored, created = normalized, wasCreated
			return nil
		}
		return fmt.Errorf(
			"%w: delete reconciliation %q",
			ErrUnknownWork,
			batch.AttemptID,
		)
	})
	return stored, created, err
}

func (store YAMLStore) CommitDeleteReconciliationBatch(
	commit DeleteReconciliationBatchCommit,
) error {
	return store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(
			*document,
			commit.RunID,
			commit.Task,
		); err != nil {
			return err
		}
		for index := range document.DeleteReconciliations {
			record := document.DeleteReconciliations[index]
			if record.RunID != commit.RunID ||
				record.Task != commit.Task ||
				record.AttemptID != commit.AttemptID {
				continue
			}
			next, err := applyDeleteReconciliationBatchCommit(
				record,
				commit,
			)
			if err != nil {
				return err
			}
			document.DeleteReconciliations[index] = next
			return nil
		}
		return fmt.Errorf(
			"%w: delete reconciliation %q",
			ErrUnknownWork,
			commit.AttemptID,
		)
	})
}

func (store YAMLStore) FinishDeleteReconciliation(result DeleteReconciliationResult) error {
	if err := validateStage4Identity(result.RunID, result.Task); err != nil {
		return err
	}
	if result.AttemptID == "" {
		return fmt.Errorf("delete reconciliation attempt ID is required")
	}
	return store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(*document, result.RunID, result.Task); err != nil {
			return err
		}
		for index := range document.DeleteReconciliations {
			record := document.DeleteReconciliations[index]
			if record.RunID != result.RunID ||
				record.Task != result.Task ||
				record.AttemptID != result.AttemptID {
				continue
			}
			completed, err := applyDeleteReconciliationResult(record, result)
			if err != nil {
				return err
			}
			document.DeleteReconciliations[index] = completed
			return nil
		}
		return fmt.Errorf("%w: delete reconciliation %q", ErrUnknownWork, result.AttemptID)
	})
}

func (store YAMLStore) SaveStrictMigrationSnapshot(snapshot StrictMigrationSnapshot) error {
	snapshot, err := normalizeStrictMigrationSnapshot(snapshot)
	if err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		run, err := requireYAMLRun(*document, snapshot.RunID)
		if err != nil {
			return err
		}
		if err := requireStrictRunSourceEngine(run, snapshot.SourceEngine); err != nil {
			return err
		}
		for _, existing := range document.StrictMigrationSnapshots {
			if existing.RunID != snapshot.RunID {
				continue
			}
			if existing.SourceEngine != snapshot.SourceEngine {
				return fmt.Errorf("%w: strict migration source engine differs", ErrImmutableEvidence)
			}
			if existing.SourceEngine == "postgres" {
				if existing.ProcessEpoch == snapshot.ProcessEpoch &&
					!reflect.DeepEqual(existing, snapshot) {
					return fmt.Errorf(
						"%w: PostgreSQL process epoch already owns a different migration snapshot",
						ErrImmutableEvidence,
					)
				}
				if existing.SnapshotReference == snapshot.SnapshotReference &&
					existing.ProcessEpoch != snapshot.ProcessEpoch {
					return fmt.Errorf(
						"%w: PostgreSQL snapshot reference cannot cross process epochs",
						ErrImmutableEvidence,
					)
				}
			}
			if existing.EpochID == snapshot.EpochID {
				if reflect.DeepEqual(existing, snapshot) {
					return nil
				}
				return fmt.Errorf("%w: strict migration snapshot", ErrImmutableEvidence)
			}
			if snapshot.SourceEngine == "mssql" || existing.SourceEngine == "mssql" {
				return fmt.Errorf(
					"%w: SQL Server migration snapshot must be reused for the entire run",
					ErrImmutableEvidence,
				)
			}
		}
		document.StrictMigrationSnapshots = append(document.StrictMigrationSnapshots, snapshot)
		return nil
	})
}

func (store YAMLStore) LoadStrictMigrationSnapshot(
	runID string,
	epochID string,
) (StrictMigrationSnapshot, bool, error) {
	var snapshot StrictMigrationSnapshot
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if _, err := requireYAMLRun(document, runID); err != nil {
			return err
		}
		for _, candidate := range document.StrictMigrationSnapshots {
			if candidate.RunID == runID && candidate.EpochID == epochID {
				snapshot, found = candidate, true
				return nil
			}
		}
		return nil
	})
	return snapshot, found, err
}

func (store YAMLStore) LoadLatestStrictMigrationSnapshot(
	runID string,
) (StrictMigrationSnapshot, bool, error) {
	var snapshot StrictMigrationSnapshot
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if _, err := requireYAMLRun(document, runID); err != nil {
			return err
		}
		for _, candidate := range document.StrictMigrationSnapshots {
			if candidate.RunID == runID &&
				(!found || laterStrictMigrationSnapshot(candidate, snapshot)) {
				snapshot, found = candidate, true
			}
		}
		return nil
	})
	return snapshot, found, err
}

func (store YAMLStore) SaveStrictMigrationCleanupIntent(
	intent StrictMigrationCleanupIntent,
) error {
	intent, err := normalizeStrictMigrationCleanupIntent(intent)
	if err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		run, err := requireYAMLRun(*document, intent.RunID)
		if err != nil {
			return err
		}
		if err := requireStrictRunSourceEngine(run, intent.SourceEngine); err != nil {
			return err
		}
		var owner StrictMigrationSnapshot
		ownerFound := false
		for _, candidate := range document.StrictMigrationSnapshots {
			if candidate.RunID == intent.RunID && candidate.EpochID == intent.EpochID {
				owner, ownerFound = candidate, true
				break
			}
		}
		if !ownerFound {
			return fmt.Errorf("%w: strict migration snapshot cleanup owner %q", ErrUnknownWork, intent.EpochID)
		}
		if owner.RunID != intent.RunID || owner.SourceEngine != intent.SourceEngine ||
			owner.SnapshotReference != intent.SnapshotReference ||
			owner.ProcessEpoch != intent.ProcessEpoch ||
			!owner.CapturedAt.Equal(intent.CapturedAt) {
			return fmt.Errorf("%w: strict migration cleanup intent differs from durable owner", ErrImmutableEvidence)
		}
		for _, existing := range document.StrictMigrationCleanupIntents {
			if existing.RunID != intent.RunID || existing.EpochID != intent.EpochID {
				continue
			}
			if reflect.DeepEqual(existing, intent) {
				return nil
			}
			return fmt.Errorf("%w: strict migration cleanup intent", ErrImmutableEvidence)
		}
		document.StrictMigrationCleanupIntents = append(
			document.StrictMigrationCleanupIntents,
			intent,
		)
		return nil
	})
}

func (store YAMLStore) LoadStrictMigrationCleanupIntent(
	runID string,
	epochID string,
) (StrictMigrationCleanupIntent, bool, error) {
	var intent StrictMigrationCleanupIntent
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if _, err := requireYAMLRun(document, runID); err != nil {
			return err
		}
		for _, candidate := range document.StrictMigrationCleanupIntents {
			if candidate.RunID == runID && candidate.EpochID == epochID {
				intent, found = candidate, true
				return nil
			}
		}
		return nil
	})
	return intent, found, err
}

func (store YAMLStore) SaveStrictSnapshotEvidence(evidence StrictSnapshotEvidence) error {
	evidence, err := normalizeStrictSnapshotEvidence(evidence)
	if err != nil {
		return err
	}
	return store.update(func(document *yamlStateDocument) error {
		if err := requireYAMLStage4Identity(*document, evidence.RunID, evidence.Task); err != nil {
			return err
		}
		run, err := requireYAMLRun(*document, evidence.RunID)
		if err != nil {
			return err
		}
		if err := requireStrictRunSourceEngine(run, evidence.SourceEngine); err != nil {
			return err
		}
		if evidence.Scope == StrictSnapshotMigration {
			var owner StrictMigrationSnapshot
			var found bool
			for _, candidate := range document.StrictMigrationSnapshots {
				if candidate.RunID == evidence.RunID &&
					candidate.EpochID == evidence.MigrationEpochID {
					owner, found = candidate, true
					break
				}
			}
			if !found {
				return fmt.Errorf("%w: strict migration snapshot %q", ErrUnknownWork, evidence.MigrationEpochID)
			}
			if owner.SourceEngine != evidence.SourceEngine ||
				owner.SnapshotReference != evidence.SnapshotReference ||
				owner.ProcessEpoch != evidence.ProcessEpoch {
				return fmt.Errorf("%w: strict table evidence differs from its migration snapshot", ErrImmutableEvidence)
			}
		}
		for _, existing := range document.StrictSnapshotEvidence {
			if existing.RunID != evidence.RunID ||
				existing.Task != evidence.Task ||
				existing.AttemptID != evidence.AttemptID {
				continue
			}
			if reflect.DeepEqual(existing, evidence) {
				return nil
			}
			return fmt.Errorf("%w: strict snapshot evidence", ErrImmutableEvidence)
		}
		document.StrictSnapshotEvidence = append(document.StrictSnapshotEvidence, evidence)
		return nil
	})
}

func (store YAMLStore) LoadStrictSnapshotEvidence(
	runID string,
	task TaskKey,
	attemptID string,
) (StrictSnapshotEvidence, bool, error) {
	var evidence StrictSnapshotEvidence
	var found bool
	err := store.read(func(document yamlStateDocument) error {
		if err := requireYAMLStage4Identity(document, runID, task); err != nil {
			return err
		}
		for _, candidate := range document.StrictSnapshotEvidence {
			if candidate.RunID == runID &&
				candidate.Task == task &&
				candidate.AttemptID == attemptID {
				evidence, found = candidate, true
				return nil
			}
		}
		return nil
	})
	return evidence, found, err
}
