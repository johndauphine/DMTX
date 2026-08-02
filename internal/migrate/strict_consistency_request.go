package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

// Request normalization, attempt binding, and the capability checks that admit
// or refuse a strict route.

func normalizePlannedStrictConsistencyRequest(
	request PlannedStrictConsistencyRequest,
) (PlannedStrictConsistencyRequest, error) {
	if request.RunID == "" ||
		strings.TrimSpace(request.RunID) != request.RunID {
		return PlannedStrictConsistencyRequest{}, errors.New(
			"planned strict consistency run ID is required and must not have surrounding whitespace",
		)
	}
	engine, err := normalizeStrictConsistencyEngine(request.SourceEngine)
	if err != nil {
		return PlannedStrictConsistencyRequest{}, err
	}
	if engine != StrictConsistencyPostgres &&
		engine != StrictConsistencyMSSQL &&
		engine != StrictConsistencyMySQL &&
		engine != StrictConsistencyMariaDB &&
		engine != StrictConsistencySQLite {
		return PlannedStrictConsistencyRequest{}, fmt.Errorf(
			"planned strict consistency is not certified for source engine %q",
			engine,
		)
	}
	if request.ProcessEpoch == "" ||
		strings.TrimSpace(request.ProcessEpoch) !=
			request.ProcessEpoch {
		return PlannedStrictConsistencyRequest{}, errors.New(
			"planned strict consistency process epoch is required and must not have surrounding whitespace",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"process epoch",
		request.ProcessEpoch,
	); err != nil {
		return PlannedStrictConsistencyRequest{}, err
	}
	if len(request.Tasks) == 0 {
		return PlannedStrictConsistencyRequest{}, errors.New(
			"planned strict consistency requires at least one selected table",
		)
	}
	tasks := append([]state.TaskKey(nil), request.Tasks...)
	seen := make(map[state.TaskKey]struct{}, len(tasks))
	for index, task := range tasks {
		if err := task.Validate(); err != nil {
			return PlannedStrictConsistencyRequest{}, fmt.Errorf(
				"planned strict consistency task %d: %w",
				index,
				err,
			)
		}
		requiresSchema := engine != StrictConsistencySQLite
		if task.Type != stage4AdapterNetworkTaskType ||
			(requiresSchema && task.Schema == "") ||
			(!requiresSchema && task.Schema != "") ||
			task.Partition != "" {
			schemaRequirement := "an explicit schema"
			if !requiresSchema {
				schemaRequirement = "no source schema"
			}
			return PlannedStrictConsistencyRequest{}, fmt.Errorf(
				"planned strict consistency task %d requires one unpartitioned %s task with %s",
				index,
				stage4AdapterNetworkTaskType,
				schemaRequirement,
			)
		}
		if _, duplicate := seen[task]; duplicate {
			return PlannedStrictConsistencyRequest{}, fmt.Errorf(
				"planned strict consistency task is duplicated: type=%q schema=%q table=%q partition=%q",
				task.Type,
				task.Schema,
				task.Table,
				task.Partition,
			)
		}
		seen[task] = struct{}{}
	}
	sort.Slice(tasks, func(left, right int) bool {
		return strictConsistencyTaskLess(tasks[left], tasks[right])
	})
	request.SourceEngine = engine
	request.Tasks = tasks
	return request, nil
}

func validateUnboundStrictConsistencyCapture(
	request PlannedStrictConsistencyRequest,
	capture StrictConsistencyCapture,
) error {
	migrationScoped := request.Scope == state.StrictSnapshotMigration
	if migrationScoped {
		if err := validateCredentialFreeIdentifier(
			"migration epoch",
			capture.MigrationEpochID,
		); err != nil {
			return err
		}
		if err := validateSnapshotReference(
			capture.MigrationSnapshotReference,
		); err != nil {
			return err
		}
		if capture.MigrationCapturedAt.IsZero() {
			return errors.New(
				"planned strict migration snapshot capture time is required",
			)
		}
	} else if capture.MigrationEpochID != "" ||
		capture.MigrationSnapshotReference != "" ||
		!capture.MigrationCapturedAt.IsZero() {
		return errors.New(
			"planned table-scoped strict evidence cannot claim a migration epoch or snapshot",
		)
	}
	expected := make(map[state.TaskKey]struct{}, len(request.Tasks))
	for _, task := range request.Tasks {
		expected[task] = struct{}{}
	}
	seen := make(map[state.TaskKey]struct{}, len(capture.Tables))
	for index, table := range capture.Tables {
		if _, ok := expected[table.Task]; !ok {
			return fmt.Errorf(
				"planned strict session returned an unexpected task at index %d",
				index,
			)
		}
		if _, duplicate := seen[table.Task]; duplicate {
			return fmt.Errorf(
				"planned strict session returned duplicate evidence for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if table.AttemptID != "" {
			return fmt.Errorf(
				"planned strict session prematurely bound attempt evidence for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if table.ExactSourceRowCount < 0 ||
			table.CapturedAt.IsZero() {
			return fmt.Errorf(
				"planned strict session returned invalid count evidence for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if err := validateSnapshotReference(
			table.SnapshotReference,
		); err != nil {
			return fmt.Errorf(
				"planned strict session reference for %s.%s: %w",
				table.Task.Schema,
				table.Task.Table,
				err,
			)
		}
		if migrationScoped &&
			table.SnapshotReference !=
				capture.MigrationSnapshotReference {
			return fmt.Errorf(
				"planned strict session table %s.%s differs from the migration snapshot",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		seen[table.Task] = struct{}{}
	}
	if len(seen) != len(expected) {
		return errors.New(
			"planned strict session omitted selected table evidence",
		)
	}
	return nil
}

func requirePlannedStrictConsistencyTaskSet(
	expected []state.TaskKey,
	finalized []StrictConsistencyTable,
) error {
	if len(expected) != len(finalized) {
		return errors.New(
			"planned strict work does not cover the selected table set",
		)
	}
	selected := make(map[state.TaskKey]struct{}, len(expected))
	for _, task := range expected {
		selected[task] = struct{}{}
	}
	for _, table := range finalized {
		if _, ok := selected[table.Task]; !ok {
			return fmt.Errorf(
				"planned strict work returned an unexpected task: type=%q schema=%q table=%q partition=%q",
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
			)
		}
		delete(selected, table.Task)
	}
	if len(selected) != 0 {
		return errors.New(
			"planned strict work omitted a selected table task",
		)
	}
	return nil
}

func bindPlannedStrictConsistencyAttempts(
	capture StrictConsistencyCapture,
	tables []StrictConsistencyTable,
) (StrictConsistencyCapture, error) {
	attempts := make(
		map[state.TaskKey]string,
		len(tables),
	)
	for _, table := range tables {
		attempts[table.Task] = table.AttemptID
	}
	result := cloneStrictConsistencyCapture(capture)
	for index := range result.Tables {
		attemptID, ok := attempts[result.Tables[index].Task]
		if !ok {
			return StrictConsistencyCapture{}, fmt.Errorf(
				"planned strict evidence has no finalized work for %s.%s",
				result.Tables[index].Task.Schema,
				result.Tables[index].Task.Table,
			)
		}
		if result.Tables[index].AttemptID != "" &&
			result.Tables[index].AttemptID != attemptID {
			return StrictConsistencyCapture{}, fmt.Errorf(
				"planned strict evidence changed its finalized attempt for %s.%s",
				result.Tables[index].Task.Schema,
				result.Tables[index].Task.Table,
			)
		}
		result.Tables[index].AttemptID = attemptID
	}
	return result, nil
}

func cloneStrictConsistencyCapture(
	capture StrictConsistencyCapture,
) StrictConsistencyCapture {
	capture.Tables = append(
		[]StrictConsistencyTableCapture(nil),
		capture.Tables...,
	)
	return capture
}

func strictConsistencyTaskLess(left, right state.TaskKey) bool {
	return strictConsistencyTableLess(
		StrictConsistencyTable{Task: left},
		StrictConsistencyTable{Task: right},
	)
}

func normalizeStrictConsistencyRequest(
	request StrictConsistencyRequest,
) (StrictConsistencyRequest, error) {
	if request.RunID == "" || strings.TrimSpace(request.RunID) != request.RunID {
		return StrictConsistencyRequest{}, errors.New(
			"strict consistency run ID is required and must not have surrounding whitespace",
		)
	}
	engine, err := normalizeStrictConsistencyEngine(request.SourceEngine)
	if err != nil {
		return StrictConsistencyRequest{}, err
	}
	if request.ProcessEpoch == "" ||
		strings.TrimSpace(request.ProcessEpoch) != request.ProcessEpoch {
		return StrictConsistencyRequest{}, errors.New(
			"strict consistency process epoch is required and must not have surrounding whitespace",
		)
	}
	if err := validateCredentialFreeIdentifier(
		"process epoch",
		request.ProcessEpoch,
	); err != nil {
		return StrictConsistencyRequest{}, err
	}
	if len(request.Tables) == 0 {
		return StrictConsistencyRequest{}, errors.New(
			"strict consistency requires at least one selected table",
		)
	}
	tables := append([]StrictConsistencyTable(nil), request.Tables...)
	seen := make(map[state.TaskKey]struct{}, len(tables))
	for index, table := range tables {
		if err := table.Task.Validate(); err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d: %w",
				index,
				err,
			)
		}
		if table.AttemptID == "" ||
			strings.TrimSpace(table.AttemptID) != table.AttemptID {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d attempt ID is required and must not have surrounding whitespace",
				index,
			)
		}
		if err := validateCredentialFreeIdentifier(
			"attempt ID",
			table.AttemptID,
		); err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d: %w",
				index,
				err,
			)
		}
		if table.WorkTopologyHash == "" {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d durable work topology hash is required",
				index,
			)
		}
		if err := validateCredentialFreeIdentifier(
			"durable work topology hash",
			table.WorkTopologyHash,
		); err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d: %w",
				index,
				err,
			)
		}
		if table.DurableWorkAttempts < 0 {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d durable work attempts must not be negative",
				index,
			)
		}
		expectedAttemptID, err := BuildStrictConsistencyAttemptID(
			table.Task,
			table.WorkTopologyHash,
			table.DurableWorkAttempts,
		)
		if err != nil {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d durable attempt identity: %w",
				index,
				err,
			)
		}
		if table.AttemptID != expectedAttemptID {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency table %d attempt ID does not match its durable task, topology, and attempt counter; expected %q",
				index,
				expectedAttemptID,
			)
		}
		if _, duplicate := seen[table.Task]; duplicate {
			return StrictConsistencyRequest{}, fmt.Errorf(
				"strict consistency selected task is duplicated: type=%q schema=%q table=%q partition=%q",
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
			)
		}
		seen[table.Task] = struct{}{}
	}
	sort.Slice(tables, func(left, right int) bool {
		return strictConsistencyTableLess(tables[left], tables[right])
	})
	request.SourceEngine = engine
	request.Tables = tables
	return request, nil
}

func normalizeStrictConsistencyEngine(
	engine StrictConsistencyEngine,
) (StrictConsistencyEngine, error) {
	switch strings.ToLower(strings.TrimSpace(string(engine))) {
	case "postgres", "postgresql", "pg":
		return StrictConsistencyPostgres, nil
	case "mssql", "sqlserver", "sql-server":
		return StrictConsistencyMSSQL, nil
	case "mysql":
		return StrictConsistencyMySQL, nil
	case "mariadb", "maria":
		return StrictConsistencyMariaDB, nil
	case "sqlite", "sqlite3", "sqlitedb":
		return StrictConsistencySQLite, nil
	case "clickhouse":
		return StrictConsistencyClickHouse, nil
	default:
		return "", fmt.Errorf(
			"unknown strict consistency source engine %q",
			engine,
		)
	}
}

func strictConsistencyStateEngine(engine StrictConsistencyEngine) string {
	if engine == StrictConsistencyMariaDB {
		return "mysql"
	}
	return string(engine)
}

func validateStrictConsistencyCapability(
	engine StrictConsistencyEngine,
	scope state.StrictSnapshotScope,
) error {
	switch scope {
	case state.StrictSnapshotTable:
		switch engine {
		case StrictConsistencyPostgres,
			StrictConsistencyMSSQL,
			StrictConsistencyMySQL,
			StrictConsistencyMariaDB,
			StrictConsistencySQLite:
			return nil
		case StrictConsistencyClickHouse:
			return errors.New(
				"ClickHouse does not support strict consistency",
			)
		}
	case state.StrictSnapshotMigration:
		switch engine {
		case StrictConsistencyPostgres, StrictConsistencyMSSQL:
			return nil
		case StrictConsistencyMySQL, StrictConsistencyMariaDB:
			return errors.New(
				"MySQL and MariaDB do not support migration-scoped strict consistency",
			)
		case StrictConsistencySQLite:
			return errors.New(
				"SQLite does not support migration-scoped strict consistency",
			)
		case StrictConsistencyClickHouse:
			return errors.New(
				"ClickHouse does not support strict consistency",
			)
		}
	default:
		return fmt.Errorf("unknown strict consistency scope %q", scope)
	}
	return fmt.Errorf(
		"source engine %q does not support strict consistency scope %q",
		engine,
		scope,
	)
}

func requireStrictConsistencyWorkTasks(
	request StrictConsistencyRequest,
) error {
	tasks, _, err := request.State.ListWork(request.RunID)
	if err != nil {
		return fmt.Errorf("list durable strict work tasks: %w", err)
	}
	counts := make(map[state.TaskKey]int, len(tasks))
	for index, task := range tasks {
		if task.RunID != request.RunID {
			return fmt.Errorf(
				"durable work task %d belongs to run %q, not %q",
				index,
				task.RunID,
				request.RunID,
			)
		}
		if err := task.Key.Validate(); err != nil {
			return fmt.Errorf(
				"durable work task %d has invalid structured identity: %w",
				index,
				err,
			)
		}
		counts[task.Key]++
	}
	for _, selected := range request.Tables {
		switch counts[selected.Task] {
		case 0:
			return fmt.Errorf(
				"strict consistency work task does not exist before source snapshot creation: type=%q schema=%q table=%q partition=%q",
				selected.Task.Type,
				selected.Task.Schema,
				selected.Task.Table,
				selected.Task.Partition,
			)
		case 1:
			var durable state.WorkTask
			for _, candidate := range tasks {
				if candidate.Key == selected.Task {
					durable = candidate
					break
				}
			}
			if durable.Status != "running" {
				return fmt.Errorf(
					"strict consistency work task is %q, not running: type=%q schema=%q table=%q partition=%q",
					durable.Status,
					selected.Task.Type,
					selected.Task.Schema,
					selected.Task.Table,
					selected.Task.Partition,
				)
			}
			if durable.TopologyHash != selected.WorkTopologyHash ||
				durable.Attempts != selected.DurableWorkAttempts {
				return fmt.Errorf(
					"strict consistency attempt %q does not match durable work identity: topology=%q attempts=%d, expected topology=%q attempts=%d",
					selected.AttemptID,
					durable.TopologyHash,
					durable.Attempts,
					selected.WorkTopologyHash,
					selected.DurableWorkAttempts,
				)
			}
		default:
			return fmt.Errorf(
				"strict consistency work task is duplicated in durable state: type=%q schema=%q table=%q partition=%q",
				selected.Task.Type,
				selected.Task.Schema,
				selected.Task.Table,
				selected.Task.Partition,
			)
		}
	}
	return nil
}

func loadStrictConsistencyAttemptEvidence(
	request StrictConsistencyRequest,
) (map[state.TaskKey]state.StrictSnapshotEvidence, error) {
	existing := make(
		map[state.TaskKey]state.StrictSnapshotEvidence,
		len(request.Tables),
	)
	for _, selected := range request.Tables {
		record, found, err := request.State.LoadStrictSnapshotEvidence(
			request.RunID,
			selected.Task,
			selected.AttemptID,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"load strict attempt evidence for %s.%s: %w",
				selected.Task.Schema,
				selected.Task.Table,
				err,
			)
		}
		if !found {
			continue
		}
		if record.RunID != request.RunID ||
			record.Task != selected.Task ||
			record.AttemptID != selected.AttemptID {
			return nil, fmt.Errorf(
				"strict attempt evidence lookup returned a mismatched structural identity for %s.%s",
				selected.Task.Schema,
				selected.Task.Table,
			)
		}
		if request.SourceEngine != StrictConsistencyMSSQL ||
			request.Scope != state.StrictSnapshotMigration ||
			!request.Resume {
			return nil, fmt.Errorf(
				"strict attempt %q for %s.%s already has immutable evidence; its prior stable view is not reusable, so resume with a fresh process epoch, advance the durable work attempt, and use its derived new strict attempt ID",
				selected.AttemptID,
				selected.Task.Schema,
				selected.Task.Table,
			)
		}
		existing[selected.Task] = record
	}
	return existing, nil
}

func reconcileStrictConsistencyAttemptEvidence(
	request StrictConsistencyRequest,
	captured []state.StrictSnapshotEvidence,
	owner *state.StrictMigrationSnapshot,
	existing map[state.TaskKey]state.StrictSnapshotEvidence,
) error {
	if len(existing) == 0 {
		return nil
	}
	if request.SourceEngine != StrictConsistencyMSSQL ||
		request.Scope != state.StrictSnapshotMigration ||
		!request.Resume ||
		owner == nil {
		return errors.New(
			"existing strict evidence may only be reused with its surviving SQL Server migration snapshot",
		)
	}
	for _, candidate := range captured {
		prior, found := existing[candidate.Task]
		if !found {
			continue
		}
		if prior.SourceEngine != "mssql" ||
			prior.Scope != state.StrictSnapshotMigration ||
			prior.MigrationEpochID != owner.EpochID ||
			prior.SnapshotReference != owner.SnapshotReference ||
			prior.ProcessEpoch != owner.ProcessEpoch ||
			prior.ExactSourceRowCount < 0 ||
			prior.CapturedAt.IsZero() {
			return fmt.Errorf(
				"existing SQL Server strict evidence for %s.%s does not belong to the one surviving durable snapshot",
				candidate.Task.Schema,
				candidate.Task.Table,
			)
		}
		if prior.ExactSourceRowCount != candidate.ExactSourceRowCount {
			return fmt.Errorf(
				"existing SQL Server strict count for %s.%s is %d but the surviving snapshot now reports %d",
				candidate.Task.Schema,
				candidate.Task.Table,
				prior.ExactSourceRowCount,
				candidate.ExactSourceRowCount,
			)
		}
	}
	return nil
}

func validateDurableMigrationSnapshot(
	request StrictConsistencyRequest,
	snapshot state.StrictMigrationSnapshot,
) error {
	if snapshot.RunID != request.RunID {
		return fmt.Errorf(
			"durable strict migration snapshot belongs to run %q, not %q",
			snapshot.RunID,
			request.RunID,
		)
	}
	expectedEngine := strictConsistencyStateEngine(request.SourceEngine)
	if snapshot.SourceEngine != expectedEngine {
		return fmt.Errorf(
			"durable strict migration snapshot engine %q differs from source engine %q",
			snapshot.SourceEngine,
			expectedEngine,
		)
	}
	if err := validateCredentialFreeIdentifier(
		"migration epoch",
		snapshot.EpochID,
	); err != nil {
		return fmt.Errorf("invalid durable strict migration snapshot: %w", err)
	}
	if err := validateSnapshotReference(snapshot.SnapshotReference); err != nil {
		return fmt.Errorf("invalid durable strict migration snapshot: %w", err)
	}
	if err := validateCredentialFreeIdentifier(
		"owner process epoch",
		snapshot.ProcessEpoch,
	); err != nil {
		return fmt.Errorf("invalid durable strict migration snapshot: %w", err)
	}
	if snapshot.CapturedAt.IsZero() {
		return errors.New(
			"durable strict migration snapshot capture time is missing",
		)
	}
	return nil
}

func buildStrictConsistencyEvidence(
	request StrictConsistencyRequest,
	capture StrictConsistencyCapture,
	requiredOwner *state.StrictMigrationSnapshot,
) (
	[]state.StrictSnapshotEvidence,
	*state.StrictMigrationSnapshot,
	error,
) {
	migrationScoped := request.Scope == state.StrictSnapshotMigration
	if migrationScoped {
		if err := validateCredentialFreeIdentifier(
			"migration epoch",
			capture.MigrationEpochID,
		); err != nil {
			return nil, nil, err
		}
		if err := validateSnapshotReference(
			capture.MigrationSnapshotReference,
		); err != nil {
			return nil, nil, err
		}
		if capture.MigrationCapturedAt.IsZero() {
			return nil, nil, errors.New(
				"strict migration snapshot capture time is required",
			)
		}
	} else if capture.MigrationEpochID != "" ||
		capture.MigrationSnapshotReference != "" ||
		!capture.MigrationCapturedAt.IsZero() {
		return nil, nil, errors.New(
			"table-scoped strict evidence cannot claim a migration epoch or snapshot",
		)
	}

	type captureIdentity struct {
		task      state.TaskKey
		attemptID string
	}
	expected := make(map[captureIdentity]StrictConsistencyTable, len(request.Tables))
	for _, table := range request.Tables {
		expected[captureIdentity{task: table.Task, attemptID: table.AttemptID}] = table
	}
	captured := make(map[captureIdentity]StrictConsistencyTableCapture, len(capture.Tables))
	for index, table := range capture.Tables {
		identity := captureIdentity{task: table.Task, attemptID: table.AttemptID}
		if _, exists := expected[identity]; !exists {
			return nil, nil, fmt.Errorf(
				"strict session returned unexpected or mismatched table evidence at index %d: type=%q schema=%q table=%q partition=%q attempt=%q",
				index,
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
				table.AttemptID,
			)
		}
		if _, duplicate := captured[identity]; duplicate {
			return nil, nil, fmt.Errorf(
				"strict session returned duplicate evidence for type=%q schema=%q table=%q partition=%q attempt=%q",
				table.Task.Type,
				table.Task.Schema,
				table.Task.Table,
				table.Task.Partition,
				table.AttemptID,
			)
		}
		if table.ExactSourceRowCount < 0 {
			return nil, nil, fmt.Errorf(
				"strict session returned a negative exact row count for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if table.CapturedAt.IsZero() {
			return nil, nil, fmt.Errorf(
				"strict session omitted the capture time for %s.%s",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		if err := validateSnapshotReference(table.SnapshotReference); err != nil {
			return nil, nil, fmt.Errorf(
				"strict session reference for %s.%s: %w",
				table.Task.Schema,
				table.Task.Table,
				err,
			)
		}
		if migrationScoped &&
			table.SnapshotReference != capture.MigrationSnapshotReference {
			return nil, nil, fmt.Errorf(
				"strict session table %s.%s reference %q differs from migration snapshot reference %q",
				table.Task.Schema,
				table.Task.Table,
				table.SnapshotReference,
				capture.MigrationSnapshotReference,
			)
		}
		if migrationScoped &&
			table.CapturedAt.Before(capture.MigrationCapturedAt) {
			return nil, nil, fmt.Errorf(
				"strict session table %s.%s capture time precedes its migration snapshot",
				table.Task.Schema,
				table.Task.Table,
			)
		}
		captured[identity] = table
	}
	if len(captured) != len(expected) {
		for _, selected := range request.Tables {
			identity := captureIdentity{
				task: selected.Task, attemptID: selected.AttemptID,
			}
			if _, exists := captured[identity]; !exists {
				return nil, nil, fmt.Errorf(
					"strict session omitted evidence for type=%q schema=%q table=%q partition=%q attempt=%q",
					selected.Task.Type,
					selected.Task.Schema,
					selected.Task.Table,
					selected.Task.Partition,
					selected.AttemptID,
				)
			}
		}
		return nil, nil, errors.New(
			"strict session returned an invalid evidence cardinality",
		)
	}

	stateEngine := strictConsistencyStateEngine(request.SourceEngine)
	processEpoch := request.ProcessEpoch
	migrationEpoch := ""
	var owner *state.StrictMigrationSnapshot
	if migrationScoped {
		migrationEpoch = capture.MigrationEpochID
		candidate := state.StrictMigrationSnapshot{
			RunID:             request.RunID,
			EpochID:           capture.MigrationEpochID,
			SourceEngine:      stateEngine,
			SnapshotReference: capture.MigrationSnapshotReference,
			ProcessEpoch:      request.ProcessEpoch,
			CapturedAt:        capture.MigrationCapturedAt.UTC(),
		}
		if requiredOwner != nil {
			if request.SourceEngine != StrictConsistencyMSSQL ||
				!request.Resume {
				return nil, nil, errors.New(
					"a durable migration owner may only be required for SQL Server resume",
				)
			}
			if capture.MigrationEpochID != requiredOwner.EpochID ||
				capture.MigrationSnapshotReference !=
					requiredOwner.SnapshotReference {
				return nil, nil, errors.New(
					"SQL Server resume did not reuse the one surviving durable database snapshot; replacement is forbidden",
				)
			}
			candidate = *requiredOwner
			migrationEpoch = requiredOwner.EpochID
			processEpoch = requiredOwner.ProcessEpoch
		}
		owner = &candidate
	}

	evidence := make([]state.StrictSnapshotEvidence, 0, len(request.Tables))
	for _, selected := range request.Tables {
		table := captured[captureIdentity{
			task: selected.Task, attemptID: selected.AttemptID,
		}]
		evidence = append(evidence, state.StrictSnapshotEvidence{
			RunID:               request.RunID,
			Task:                selected.Task,
			AttemptID:           selected.AttemptID,
			SourceEngine:        stateEngine,
			Scope:               request.Scope,
			MigrationEpochID:    migrationEpoch,
			SnapshotReference:   table.SnapshotReference,
			ProcessEpoch:        processEpoch,
			ExactSourceRowCount: table.ExactSourceRowCount,
			CapturedAt:          table.CapturedAt.UTC(),
		})
	}
	return evidence, owner, nil
}

func validateSnapshotReference(reference string) error {
	return validateCredentialFreeIdentifier(
		"snapshot reference",
		reference,
	)
}

// validateCredentialFreeIdentifier accepts only a short opaque token. Engine
// adapters must encode or hash a snapshot handle into this grammar and must
// never place credentials in it. The core deliberately does not guess whether
// otherwise-valid token text has secret meaning.
func validateCredentialFreeIdentifier(label, value string) error {
	if value == "" {
		return fmt.Errorf("%s is required", label)
	}
	const maximumOpaqueTokenBytes = 256
	if len(value) > maximumOpaqueTokenBytes {
		return fmt.Errorf(
			"%s exceeds %d bytes",
			label,
			maximumOpaqueTokenBytes,
		)
	}
	isASCIIAlphanumeric := func(character byte) bool {
		return character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9'
	}
	if !isASCIIAlphanumeric(value[0]) ||
		!isASCIIAlphanumeric(value[len(value)-1]) {
		return fmt.Errorf(
			"%s must begin and end with an ASCII letter or digit",
			label,
		)
	}
	for index := 1; index < len(value)-1; index++ {
		character := value[index]
		if !isASCIIAlphanumeric(character) &&
			character != '.' &&
			character != '_' &&
			character != '-' {
			return fmt.Errorf(
				"%s must contain only ASCII letters, digits, dot, underscore, or hyphen",
				label,
			)
		}
	}
	return nil
}

func strictConsistencyTableLess(
	left StrictConsistencyTable,
	right StrictConsistencyTable,
) bool {
	if left.Task.Type != right.Task.Type {
		return left.Task.Type < right.Task.Type
	}
	if left.Task.Schema != right.Task.Schema {
		return left.Task.Schema < right.Task.Schema
	}
	if left.Task.Table != right.Task.Table {
		return left.Task.Table < right.Task.Table
	}
	if left.Task.Partition != right.Task.Partition {
		return left.Task.Partition < right.Task.Partition
	}
	return left.AttemptID < right.AttemptID
}

func closeStrictConsistencyAfterFailure(
	ctx context.Context,
	session StrictConsistencySession,
	primary error,
) error {
	cleanupErr := closeStrictConsistencySession(ctx, session)
	if cleanupErr == nil {
		return markSQLServerMigrationSnapshotNotResumable(session, primary)
	}
	return markSQLServerMigrationSnapshotNotResumable(session, joinStrictConsistencyCleanup(
		primary,
		fmt.Errorf("release strict source snapshot after failure: %w", cleanupErr),
	))
}

func markSQLServerMigrationSnapshotNotResumable(
	session StrictConsistencySession,
	err error,
) error {
	if err == nil || errors.Is(err, ErrSQLServerMigrationSnapshotNotResumable) {
		return err
	}
	reporter, ok := session.(strictMigrationSnapshotResumeReporter)
	if !ok || reporter.StrictMigrationSnapshotResumeAvailable() {
		return err
	}
	return errors.Join(err, ErrSQLServerMigrationSnapshotNotResumable)
}

func markMissingSQLServerMigrationSnapshotResume(
	source StrictConsistencyEngine,
	scope state.StrictSnapshotScope,
	resume bool,
	err error,
) error {
	if source != StrictConsistencyMSSQL ||
		scope != state.StrictSnapshotMigration || !resume ||
		!errors.Is(err, errSQLServerMigrationSnapshotMissing) {
		return err
	}
	return errors.Join(err, ErrSQLServerMigrationSnapshotNotResumable)
}

// joinStrictConsistencyCleanup keeps a cleanup deadline operationally visible
// without letting its context identity override a primary state, policy, or
// source failure in ClassifyTransferError. A cancellation primary remains
// cancellation, and a standalone Close still exposes its context cause.
func joinStrictConsistencyCleanup(primary error, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	if primary != nil &&
		ClassifyTransferError(primary) != ErrorClassCanceled &&
		(errors.Is(cleanup, context.Canceled) ||
			errors.Is(cleanup, context.DeadlineExceeded)) {
		cleanup = errors.New(cleanup.Error())
	}
	return errors.Join(primary, cleanup)
}

func closeStrictConsistencySession(
	caller context.Context,
	session StrictConsistencySession,
) error {
	if isNilInterface(session) {
		return errors.New("strict consistency session is unavailable")
	}
	base := context.Background()
	if caller != nil {
		base = context.WithoutCancel(caller)
	}
	now := time.Now()
	deadline := now.Add(strictConsistencyCleanupTimeout)
	if caller != nil {
		if callerDeadline, found := caller.Deadline(); found &&
			callerDeadline.After(now) &&
			callerDeadline.Before(deadline) {
			deadline = callerDeadline
		}
	}
	cleanupContext, cancel := context.WithDeadline(base, deadline)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- session.Close(cleanupContext)
	}()
	select {
	case err := <-result:
		return err
	case <-cleanupContext.Done():
		return fmt.Errorf(
			"strict source snapshot cleanup exceeded its bounded deadline: %w",
			cleanupContext.Err(),
		)
	}
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
