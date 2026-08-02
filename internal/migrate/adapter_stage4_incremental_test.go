package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

type stage4IncrementalTestRow struct {
	id      int64
	payload any
	updated *time.Time
}

type stage4IncrementalTestSource struct {
	events *[]string
	table  schema.Table
	rows   []stage4IncrementalTestRow
	engine string
}

func (source *stage4IncrementalTestSource) Engine() string {
	if source.engine != "" {
		return source.engine
	}
	return "postgres"
}

func (source *stage4IncrementalTestSource) DisplayName() string {
	switch source.Engine() {
	case "mssql":
		return "SQL Server"
	case "mysql":
		return "MySQL"
	case "sqlite":
		return "SQLite"
	default:
		return "PostgreSQL"
	}
}

func (source *stage4IncrementalTestSource) ListTables(
	context.Context,
) ([]string, error) {
	*source.events = append(*source.events, "source_list")
	return []string{source.table.Name}, nil
}

func (source *stage4IncrementalTestSource) InspectTable(
	_ context.Context,
	name string,
) (schema.Table, error) {
	*source.events = append(*source.events, "source_inspect")
	if name != source.table.Name {
		return schema.Table{}, fmt.Errorf("unexpected table %q", name)
	}
	return cloneStage4RichTable(source.table), nil
}

func (source *stage4IncrementalTestSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, fmt.Errorf("ordinary source reads are forbidden")
}

func (source *stage4IncrementalTestSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return len(source.rows), nil
}

func (*stage4IncrementalTestSource) Close() error { return nil }

func (source *stage4IncrementalTestSource) IncrementalTable(
	table schema.Table,
) (IncrementalTable, error) {
	namespace := table.Schema
	if source.Engine() == "sqlite" {
		namespace = ""
	}
	return buildAdapterIncrementalTable(source.Engine(), namespace, table)
}

func (source *stage4IncrementalTestSource) SampleIncrementalUpperFence(
	_ context.Context,
	_ schema.Table,
	column IncrementalColumn,
) (*time.Time, error) {
	*source.events = append(*source.events, "source_sample_fence")
	if column.Name != "updated_at" {
		return nil, fmt.Errorf("unexpected fence column %q", column.Name)
	}
	var maximum *time.Time
	for _, row := range source.rows {
		if row.updated == nil {
			continue
		}
		if maximum == nil || row.updated.After(*maximum) {
			copy := row.updated.UTC()
			maximum = &copy
		}
	}
	return maximum, nil
}

func (source *stage4IncrementalTestSource) OpenIncrementalRows(
	_ context.Context,
	_ schema.Table,
	columns []string,
	read IncrementalReadPlan,
) (adapterRows, error) {
	*source.events = append(*source.events, "source_incremental_rows")
	if len(columns) != 3 ||
		columns[0] != "id" ||
		columns[1] != "payload" ||
		columns[2] != "updated_at" {
		return nil, fmt.Errorf("unexpected projection %#v", columns)
	}
	selected := make([]stage4IncrementalTestRow, 0, len(source.rows))
	for _, row := range source.rows {
		if read.Scope == IncrementalReadWindow &&
			!read.Window.Contains(row.updated) {
			continue
		}
		selected = append(selected, row)
	}
	sort.Slice(selected, func(left, right int) bool {
		leftTime, rightTime := selected[left].updated, selected[right].updated
		if leftTime == nil || rightTime == nil {
			if leftTime == nil && rightTime == nil {
				return selected[left].id < selected[right].id
			}
			return leftTime == nil
		}
		if leftTime.Equal(*rightTime) {
			return selected[left].id < selected[right].id
		}
		return leftTime.Before(*rightTime)
	})
	values := make([][]any, len(selected))
	for index, row := range selected {
		var updated any
		if row.updated != nil {
			updated = row.updated.UTC()
		}
		values[index] = []any{row.id, row.payload, updated}
	}
	return &stage4IncrementalTestRows{values: values, index: -1}, nil
}

type stage4IncrementalTestRows struct {
	values [][]any
	index  int
}

func (rows *stage4IncrementalTestRows) Next() bool {
	rows.index++
	return rows.index < len(rows.values)
}

func (rows *stage4IncrementalTestRows) Scan(destinations ...any) error {
	if rows.index < 0 || rows.index >= len(rows.values) {
		return fmt.Errorf("row index is outside the stream")
	}
	if len(destinations) != len(rows.values[rows.index]) {
		return fmt.Errorf("destination count differs")
	}
	for index, value := range rows.values[rows.index] {
		destination, ok := destinations[index].(*any)
		if !ok {
			return fmt.Errorf("destination %d has type %T", index, destinations[index])
		}
		*destination = value
	}
	return nil
}

func (*stage4IncrementalTestRows) Err() error   { return nil }
func (*stage4IncrementalTestRows) Close() error { return nil }

type stage4IncrementalTestTarget struct {
	events                  *[]string
	rows                    map[int64][]any
	engine                  string
	incrementalAdmission    error
	afterBatchValidation    func(int, *stage4IncrementalTestTarget)
	batchValidationCount    int
	finalReadKeys           []int64
	finalReadBatches        int
	finalNullReadKeys       []int64
	finalNullReadBatches    int
	finalSampleReadKeys     []int64
	finalSampleReadBatches  int
	forbidFinalFullRows     bool
	freezeFinalSnapshot     bool
	afterFinalRead          func(int, *stage4IncrementalTestTarget)
	validateFinalProjection bool
}

func (target *stage4IncrementalTestTarget) Engine() string {
	if target.engine != "" {
		return target.engine
	}
	return "postgres"
}

func (*stage4IncrementalTestTarget) stage4NetworkIdempotentUpsertTarget() {}

func (target *stage4IncrementalTestTarget) PreflightStage4IncrementalUpsert(
	_ context.Context,
	_ []schema.Table,
) error {
	if target.incrementalAdmission != nil {
		return target.incrementalAdmission
	}
	*target.events = append(*target.events, "target_incremental_admission")
	return nil
}

func (target *stage4IncrementalTestTarget) PlanTables(
	sourceEngine string,
	tables []schema.Table,
	mode string,
) ([]schema.Table, error) {
	*target.events = append(*target.events, "target_plan")
	if sourceEngine != "postgres" || mode != "upsert" {
		return nil, fmt.Errorf("unexpected route %s/%s", sourceEngine, mode)
	}
	result := make([]schema.Table, len(tables))
	for index := range tables {
		result[index] = cloneStage4RichTable(tables[index])
	}
	return result, nil
}

func (target *stage4IncrementalTestTarget) PreflightTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_preflight")
	return nil
}

func (target *stage4IncrementalTestTarget) PreflightStage4NetworkReplayIsolation(
	context.Context,
	[]schema.Table,
) error {
	*target.events = append(*target.events, "target_isolation")
	return nil
}

func (target *stage4IncrementalTestTarget) PrepareTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_prepare")
	return nil
}

func (target *stage4IncrementalTestTarget) WriteBatch(
	ctx context.Context,
	table schema.Table,
	columns []string,
	_ string,
	rows [][]any,
) (WriteReceipt, error) {
	return target.WriteStage4NetworkBatch(ctx, table, columns, rows)
}

func (target *stage4IncrementalTestTarget) WriteStage4NetworkBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) (WriteReceipt, error) {
	*target.events = append(*target.events, "target_write")
	if target.rows == nil {
		target.rows = make(map[int64][]any)
	}
	for _, row := range rows {
		id, ok := row[0].(int64)
		if !ok {
			return WriteReceipt{}, fmt.Errorf("unexpected key type %T", row[0])
		}
		target.rows[id] = cloneAdapterRow(row)
	}
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

func (target *stage4IncrementalTestTarget) ValidateStage4IncrementalBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	rows [][]any,
) error {
	*target.events = append(*target.events, "target_validate")
	for _, row := range rows {
		id, ok := row[0].(int64)
		if !ok {
			return fmt.Errorf("unexpected validation key type %T", row[0])
		}
		stored, found := target.rows[id]
		if !found || !reflect.DeepEqual(stored, row) {
			return fmt.Errorf("target row differs for a transferred key")
		}
	}
	target.batchValidationCount++
	if target.afterBatchValidation != nil {
		target.afterBatchValidation(target.batchValidationCount, target)
	}
	return nil
}

func (target *stage4IncrementalTestTarget) ValidateStage4IncrementalValidationTarget(
	table schema.Table,
	projection []string,
	requireSampleTypes bool,
) error {
	if target.validateFinalProjection {
		_, err := validateValidationCoreProjection(
			table,
			projection,
			requireSampleTypes,
		)
		return err
	}
	return nil
}

func (target *stage4IncrementalTestTarget) OpenStage4IncrementalValidationTargetSnapshot(
	_ context.Context,
	_ schema.Table,
) (stage4AdapterIncrementalTargetEvidenceSnapshot, error) {
	*target.events = append(*target.events, "target_final_snapshot")
	snapshot := &stage4IncrementalTestTargetSnapshot{target: target}
	if target.freezeFinalSnapshot {
		snapshot.rows = make(map[int64][]any, len(target.rows))
		for id, row := range target.rows {
			snapshot.rows[id] = cloneAdapterRow(row)
		}
	}
	return snapshot, nil
}

type stage4IncrementalTestTargetSnapshot struct {
	target *stage4IncrementalTestTarget
	rows   map[int64][]any
}

func (snapshot *stage4IncrementalTestTargetSnapshot) ReadStage4IncrementalValidationTargetKeys(
	_ context.Context,
	_ schema.Table,
	keys []ValidationPrimaryKey,
) ([]ValidationPrimaryKey, error) {
	if snapshot == nil || snapshot.target == nil {
		return nil, fmt.Errorf("test incremental target snapshot is unavailable")
	}
	*snapshot.target.events = append(*snapshot.target.events, "target_final_keys")
	ids, err := snapshot.validationIDs(keys)
	if err != nil {
		return nil, err
	}
	result := make([]ValidationPrimaryKey, 0, len(ids))
	for _, id := range ids {
		snapshot.target.finalReadKeys = append(snapshot.target.finalReadKeys, id)
		if _, found := snapshot.validationRows()[id]; found {
			result = append(result, ValidationPrimaryKey{Values: []any{id}})
		}
	}
	snapshot.target.finalReadBatches++
	if snapshot.target.afterFinalRead != nil {
		snapshot.target.afterFinalRead(
			snapshot.target.finalReadBatches,
			snapshot.target,
		)
	}
	return result, nil
}

func (snapshot *stage4IncrementalTestTargetSnapshot) ReadStage4IncrementalValidationTargetNullCounts(
	_ context.Context,
	_ schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) (stage4AdapterIncrementalTargetNullCounts, error) {
	if snapshot == nil || snapshot.target == nil {
		return stage4AdapterIncrementalTargetNullCounts{}, fmt.Errorf(
			"test incremental target snapshot is unavailable",
		)
	}
	*snapshot.target.events = append(*snapshot.target.events, "target_final_nulls")
	ids, err := snapshot.validationIDs(keys)
	if err != nil {
		return stage4AdapterIncrementalTargetNullCounts{}, err
	}
	result := stage4AdapterIncrementalTargetNullCounts{
		Counts: zeroStage4AdapterIncrementalNullCounts(projection),
	}
	for _, id := range ids {
		snapshot.target.finalNullReadKeys = append(snapshot.target.finalNullReadKeys, id)
		row, found := snapshot.validationRows()[id]
		if !found {
			continue
		}
		result.Rows++
		for index, column := range projection {
			if index >= len(row) {
				return stage4AdapterIncrementalTargetNullCounts{}, fmt.Errorf(
					"test incremental target row is missing column %s",
					column,
				)
			}
			if row[index] == nil {
				result.Counts[column]++
			}
		}
	}
	snapshot.target.finalNullReadBatches++
	return result, nil
}

func (snapshot *stage4IncrementalTestTargetSnapshot) ReadStage4IncrementalValidationTargetSampleRows(
	_ context.Context,
	_ schema.Table,
	_ []string,
	keys []ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	if snapshot == nil || snapshot.target == nil {
		return nil, fmt.Errorf("test incremental target snapshot is unavailable")
	}
	if len(keys) > stage4AdapterIncrementalValidationSampleLimit {
		return nil, fmt.Errorf(
			"test target received %d full sample rows, limit is %d",
			len(keys),
			stage4AdapterIncrementalValidationSampleLimit,
		)
	}
	if snapshot.target.forbidFinalFullRows {
		return nil, fmt.Errorf("test target forbids a full-projection final read")
	}
	*snapshot.target.events = append(*snapshot.target.events, "target_final_samples")
	ids, err := snapshot.validationIDs(keys)
	if err != nil {
		return nil, err
	}
	result := make([]ValidationSampleRow, 0, len(ids))
	for _, id := range ids {
		snapshot.target.finalSampleReadKeys = append(
			snapshot.target.finalSampleReadKeys,
			id,
		)
		if row, found := snapshot.validationRows()[id]; found {
			result = append(result, ValidationSampleRow{Values: cloneAdapterRow(row)})
		}
	}
	snapshot.target.finalSampleReadBatches++
	return result, nil
}

func (snapshot *stage4IncrementalTestTargetSnapshot) validationIDs(
	keys []ValidationPrimaryKey,
) ([]int64, error) {
	ids := make([]int64, len(keys))
	for index, key := range keys {
		if len(key.Values) != 1 {
			return nil, fmt.Errorf(
				"test incremental final key width = %d, want one",
				len(key.Values),
			)
		}
		id, ok := key.Values[0].(int64)
		if !ok {
			return nil, fmt.Errorf(
				"test incremental final key type = %T, want int64",
				key.Values[0],
			)
		}
		ids[index] = id
	}
	return ids, nil
}

func (snapshot *stage4IncrementalTestTargetSnapshot) validationRows() map[int64][]any {
	if snapshot.rows != nil {
		return snapshot.rows
	}
	return snapshot.target.rows
}

func (*stage4IncrementalTestTargetSnapshot) Close() error { return nil }

func (target *stage4IncrementalTestTarget) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	*target.events = append(*target.events, "target_count")
	return len(target.rows), nil
}

func (target *stage4IncrementalTestTarget) FinalizeTables(
	context.Context,
	[]schema.Table,
	string,
) error {
	*target.events = append(*target.events, "target_finalize")
	return nil
}

func (*stage4IncrementalTestTarget) Close() error { return nil }

type stage4IncrementalTestObserver struct {
	events          *[]string
	backend         stage4IncrementalTestState
	run             Stage4RunContext
	resume          bool
	validationProbe ValidationCoreProbe
}

type stage4IncrementalTestState interface {
	state.Backend
	Stage4StateBackend
}

func (observer stage4IncrementalTestObserver) Stage4RunContext() (
	Stage4RunContext,
	error,
) {
	return observer.run, nil
}

func (stage4IncrementalTestObserver) ObserveStage4SchemaDecisions(
	context.Context,
	Stage4SchemaDecisionReport,
) error {
	return nil
}

func (observer stage4IncrementalTestObserver) Stage4ValidationProbe(
	_source sourceAdapter,
	_target targetAdapter,
	_plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if observer.validationProbe == nil {
		return nil, fmt.Errorf("test deep-validation probe is not configured")
	}
	return observer.validationProbe, nil
}

func (observer stage4IncrementalTestObserver) ProtectTargetMutation(
	ctx context.Context,
	mutation func() error,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutation()
}

func (observer stage4IncrementalTestObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	*observer.events = append(*observer.events, "before_tables")
	if observer.resume {
		return nil
	}
	tasks := make([]state.Task, len(tables))
	startedAt := time.Now().UTC()
	for index, table := range tables {
		tasks[index] = state.Task{
			RunID:     observer.run.RunID,
			Table:     table,
			StartedAt: startedAt,
		}
	}
	return observer.backend.CreateTasks(tasks)
}

func (observer stage4IncrementalTestObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	*observer.events = append(*observer.events, "before:"+table)
	return nil
}

func (observer stage4IncrementalTestObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	return fmt.Errorf("aggregate Stage 4 completion must not call AfterTable")
}

// stage4IncrementalDeepValidationProbe supplies the route-bound equality proof
// during pre-mutation admission. The composed incremental runner must not use
// it for final validation: that validation is reconstructed from exact-window
// batch evidence so post-fence source rows cannot enter any §12 pass.
type stage4IncrementalDeepValidationProbe struct {
	source *stage4IncrementalTestSource
	target *stage4IncrementalTestTarget

	mu               sync.Mutex
	exactCalls       map[ValidationSide]int
	nullCalls        map[ValidationSide]int
	sampleSourceRuns int
	sampleTargetRuns int
	targetNullScopes []ValidationNullScope
}

func (probe *stage4IncrementalDeepValidationProbe) ExactCount(
	_ context.Context,
	side ValidationSide,
	_ schema.Table,
) (int64, error) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.exactCalls == nil {
		probe.exactCalls = make(map[ValidationSide]int)
	}
	probe.exactCalls[side]++
	if side == ValidationSource {
		return int64(len(probe.source.rows)), nil
	}
	return int64(len(probe.target.rows)), nil
}

func (*stage4IncrementalDeepValidationProbe) EstimateCount(
	context.Context,
	ValidationSide,
	schema.Table,
) (int64, error) {
	return 0, fmt.Errorf("exact incremental deep validation count unexpectedly fell back to an estimate")
}

func (probe *stage4IncrementalDeepValidationProbe) NullCounts(
	_ context.Context,
	side ValidationSide,
	_ schema.Table,
	projection []string,
	scope ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	probe.mu.Lock()
	if probe.nullCalls == nil {
		probe.nullCalls = make(map[ValidationSide]int)
	}
	probe.nullCalls[side]++
	if side == ValidationTarget {
		probe.targetNullScopes = append(
			probe.targetNullScopes,
			cloneValidationNullScope(scope),
		)
	}
	probe.mu.Unlock()

	var rows [][]any
	switch side {
	case ValidationSource:
		if scope.Kind != ValidationNullScopeTransferredSource {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"source NULL scope = %q, want transferred_source",
				scope.Kind,
			)
		}
		rows = probe.sourceRows()
	case ValidationTarget:
		if scope.Kind != ValidationNullScopeTargetSourcePrimaryKeys {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"target NULL scope = %q, want target_source_primary_keys",
				scope.Kind,
			)
		}
		rows = probe.targetRowsForSourceKeys()
	default:
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"unknown deep-validation side %q",
			side,
		)
	}
	if len(projection) != 3 || projection[0] != "id" ||
		projection[1] != "payload" || projection[2] != "updated_at" {
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"unexpected deep-validation projection %#v",
			projection,
		)
	}
	counts := make(map[string]int64, len(projection))
	for _, row := range rows {
		if len(row) != len(projection) {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"deep-validation row width = %d, want %d",
				len(row),
				len(projection),
			)
		}
		for index, value := range row {
			if value == nil {
				counts[projection[index]]++
			}
		}
	}
	return ValidationNullCountEvidence{
		Scope:  cloneValidationNullScope(scope),
		Rows:   int64(len(rows)),
		Counts: counts,
	}, nil
}

func (probe *stage4IncrementalDeepValidationProbe) SampleSourceRows(
	_ context.Context,
	_ schema.Table,
	projection []string,
	limit int,
) ([]ValidationSampleRow, error) {
	if len(projection) != 3 || limit <= 0 {
		return nil, fmt.Errorf("unexpected source sample request")
	}
	probe.mu.Lock()
	probe.sampleSourceRuns++
	probe.mu.Unlock()
	rows := probe.sourceRows()
	if len(rows) > limit {
		rows = rows[:limit]
	}
	result := make([]ValidationSampleRow, len(rows))
	for index, row := range rows {
		result[index] = ValidationSampleRow{Values: cloneAdapterRow(row)}
	}
	return result, nil
}

func (probe *stage4IncrementalDeepValidationProbe) SampleTargetRows(
	_ context.Context,
	_ schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	if len(projection) != 3 {
		return nil, fmt.Errorf("unexpected target sample projection")
	}
	probe.mu.Lock()
	probe.sampleTargetRuns++
	probe.mu.Unlock()
	result := make([]ValidationSampleRow, 0, len(keys))
	for _, key := range keys {
		if len(key.Values) != 1 {
			return nil, fmt.Errorf("sample key width = %d, want 1", len(key.Values))
		}
		id, ok := key.Values[0].(int64)
		if !ok {
			return nil, fmt.Errorf("sample key type = %T, want int64", key.Values[0])
		}
		row, found := probe.target.rows[id]
		if !found {
			continue
		}
		result = append(result, ValidationSampleRow{Values: cloneAdapterRow(row)})
	}
	return result, nil
}

func (*stage4IncrementalDeepValidationProbe) Stage4ValidationPrimaryKeyEqualityProof(
	schema.Table,
) (string, error) {
	return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nil
}

func (probe *stage4IncrementalDeepValidationProbe) sourceRows() [][]any {
	rows := append([]stage4IncrementalTestRow(nil), probe.source.rows...)
	sort.Slice(rows, func(left, right int) bool {
		return rows[left].id < rows[right].id
	})
	result := make([][]any, len(rows))
	for index, row := range rows {
		var updated any
		if row.updated != nil {
			updated = row.updated.UTC()
		}
		result[index] = []any{row.id, row.payload, updated}
	}
	return result
}

func (probe *stage4IncrementalDeepValidationProbe) targetRowsForSourceKeys() [][]any {
	keys := make(map[int64]struct{}, len(probe.source.rows))
	for _, row := range probe.source.rows {
		keys[row.id] = struct{}{}
	}
	ids := make([]int64, 0, len(keys))
	for id := range keys {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(left, right int) bool { return ids[left] < ids[right] })
	result := make([][]any, 0, len(ids))
	for _, id := range ids {
		if row, found := probe.target.rows[id]; found {
			result = append(result, cloneAdapterRow(row))
		}
	}
	return result
}

func (probe *stage4IncrementalDeepValidationProbe) calls() (
	exact map[ValidationSide]int,
	nulls map[ValidationSide]int,
	sourceSamples int,
	targetSamples int,
	scopes []ValidationNullScope,
) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	exact = make(map[ValidationSide]int, len(probe.exactCalls))
	for side, count := range probe.exactCalls {
		exact[side] = count
	}
	nulls = make(map[ValidationSide]int, len(probe.nullCalls))
	for side, count := range probe.nullCalls {
		nulls[side] = count
	}
	scopes = append([]ValidationNullScope(nil), probe.targetNullScopes...)
	return exact, nulls, probe.sampleSourceRuns, probe.sampleTargetRuns, scopes
}

func TestStage4AdapterIncrementalPersistsFenceBeforeTargetMutationAndPublishesAggregate(
	t *testing.T,
) {
	events := make([]string, 0)
	first := time.Date(2026, 7, 20, 1, 2, 3, 0, time.UTC)
	second := first.Add(time.Hour)
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	source := &stage4IncrementalTestSource{
		events: &events,
		table:  table,
		rows: []stage4IncrementalTestRow{
			{id: 1, payload: "first", updated: &first},
			{id: 2, payload: "second", updated: &second},
		},
	}
	target := &stage4IncrementalTestTarget{events: &events}
	backend := state.YAMLStore{
		Path: filepath.Join(t.TempDir(), "state.yaml"),
	}
	runID := "stage4-incremental-fresh"
	initializeStage4LifecycleRun(
		t,
		backend,
		runID,
		time.Now().Add(-time.Minute),
	)
	observer := stage4IncrementalTestObserver{
		events:  &events,
		backend: backend,
		run: stage4LifecycleRunContext(
			t,
			backend,
			runID,
			false,
		),
	}
	cfg := config.Config{
		Source: config.Endpoint{
			Type:     "postgres",
			Host:     "source.example",
			Port:     5432,
			Database: "source",
		},
		Target: config.Endpoint{
			Type:     "postgres",
			Host:     "target.example",
			Port:     5432,
			Database: "target",
		},
		Migration: config.Migration{
			TargetMode:         "upsert",
			DateUpdatedColumns: []string{"updated_at"},
			Validation: config.ValidationPolicy{
				Mode:           config.ValidationCountOnly,
				FailOnMismatch: true,
				FailOnTimeout:  true,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
	result, err := migrateWithAdapters(
		context.Background(),
		cfg,
		observer,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("migrateWithAdapters: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("result = %#v", result)
	}
	assertStage4AdapterEventBefore(
		t,
		events,
		"source_sample_fence",
		"target_prepare",
	)
	assertStage4AdapterEventBefore(
		t,
		events,
		"source_sample_fence",
		"target_write",
	)
	if len(target.rows) != 2 {
		t.Fatalf("target rows = %d", len(target.rows))
	}
	task := state.TaskKey{
		Type:   stage4AdapterNetworkTaskType,
		Schema: "public",
		Table:  "items",
	}
	attempt, found, err := backend.LoadLatestCommittedIncrementalAttempt(
		runID,
		task,
	)
	if err != nil || !found {
		t.Fatalf("committed attempt found=%v err=%v", found, err)
	}
	if attempt.Status != state.IncrementalCompleted ||
		attempt.CommittedWatermark == nil ||
		attempt.CommittedWatermark.Column != "updated_at" ||
		!attempt.CommittedWatermark.Value.Equal(second) {
		t.Fatalf("committed attempt = %#v", attempt)
	}
	tasks, err := backend.ListTasks(runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 ||
		tasks[0].Status != "completed" ||
		tasks[0].RowsDone != 2 {
		t.Fatalf("ordinary tasks = %#v", tasks)
	}

	pristine := make(map[int64][]any, len(target.rows))
	for id, row := range target.rows {
		pristine[id] = cloneAdapterRow(row)
	}
	for _, test := range []struct {
		name   string
		tamper func(map[int64][]any)
		want   string
	}{
		{
			name: "delete",
			tamper: func(rows map[int64][]any) {
				delete(rows, 1)
			},
			want: "checkpoint has 2 rows and target has 1",
		},
		{
			name: "value tamper",
			tamper: func(rows map[int64][]any) {
				rows[1][1] = "tampered"
			},
			want: "revalidate exact completed Stage 4 incremental target values",
		},
	} {
		t.Run("resume rejects "+test.name, func(t *testing.T) {
			target.rows = make(map[int64][]any, len(pristine))
			for id, row := range pristine {
				target.rows[id] = cloneAdapterRow(row)
			}
			test.tamper(target.rows)
			resumeObserver := stage4IncrementalTestObserver{
				events:  &events,
				backend: backend,
				resume:  true,
				run: stage4LifecycleRunContext(
					t,
					backend,
					runID,
					true,
				),
			}
			_, err := resumeWithAdapters(
				context.Background(),
				cfg,
				CompletedTableCheckpoints{
					"items": {Rows: 2},
				},
				resumeObserver,
				resumeObserver,
				source,
				target,
			)
			if err == nil ||
				!containsStage4AdapterIncrementalText(
					err.Error(),
					test.want,
				) {
				t.Fatalf("resume error = %v", err)
			}
		})
	}

	target.rows = make(map[int64][]any, len(pristine))
	for id, row := range pristine {
		target.rows[id] = cloneAdapterRow(row)
	}
	completeStage4IncrementalTestRun(t, backend, runID)
	nextRunID := "stage4-incremental-unchanged-window"
	initializeStage4LifecycleRun(
		t,
		backend,
		nextRunID,
		time.Now().Add(-time.Minute),
	)
	delete(target.rows, 2)
	nextObserver := stage4IncrementalTestObserver{
		events:  &events,
		backend: backend,
		run: stage4LifecycleRunContext(
			t,
			backend,
			nextRunID,
			false,
		),
	}
	writesBeforeUnchangedRun := stage4IncrementalEventCount(events, "target_write")
	result, err = migrateWithAdapters(
		context.Background(),
		cfg,
		nextObserver,
		source,
		target,
	)
	if err != nil {
		t.Fatalf("run unchanged incremental window: %v", err)
	}
	// The first completed incremental run established the durable watermark at
	// second. A new run with no newer source timestamp must use the strict
	// lower bound and validate an empty exact attempt; it must not replay old
	// rows merely to repair an out-of-band target deletion.
	if result != (Result{Tables: 1, Rows: 0, Validated: true}) {
		t.Fatalf("incremental replay result = %#v", result)
	}
	if _, found := target.rows[2]; found {
		t.Fatal("unchanged incremental window unexpectedly replayed a historical target row")
	}
	if writes := stage4IncrementalEventCount(events, "target_write"); writes != writesBeforeUnchangedRun {
		t.Fatalf("unchanged incremental window performed %d new target writes", writes-writesBeforeUnchangedRun)
	}
}

func TestPrepareStage4AdapterIncrementalAdmitsInclusiveDeepValidationModes(
	t *testing.T,
) {
	for _, mode := range []config.ValidationMode{
		config.ValidationNullParity,
		config.ValidationSample,
	} {
		t.Run(string(mode), func(t *testing.T) {
			events := make([]string, 0)
			table := stage4IncrementalMatrixTable("postgres", "items")
			cfg := stage4IncrementalMatrixConfig()
			cfg.Migration.Validation.Mode = mode
			incremental, work, err := prepareStage4AdapterIncremental(
				context.Background(),
				cfg,
				&stage4IncrementalTestSource{events: &events, table: table},
				&stage4IncrementalTestTarget{events: &events},
				stage4IncrementalMatrixPrepared(t, table, table),
			)
			if err != nil {
				t.Fatalf("prepare %s incremental validation: %v", mode, err)
			}
			if incremental == nil || len(work) != 1 {
				t.Fatalf("incremental=%#v work=%#v", incremental, work)
			}
			assertStage4AdapterEventBefore(
				t,
				events,
				"target_isolation",
				"target_incremental_admission",
			)
		})
	}

	t.Run("full remains rejected before target preflight", func(t *testing.T) {
		events := make([]string, 0)
		table := stage4IncrementalMatrixTable("postgres", "items")
		cfg := stage4IncrementalMatrixConfig()
		cfg.Migration.Validation.Mode = config.ValidationFull
		_, _, err := prepareStage4AdapterIncremental(
			context.Background(),
			cfg,
			&stage4IncrementalTestSource{events: &events, table: table},
			&stage4IncrementalTestTarget{events: &events},
			stage4IncrementalMatrixPrepared(t, table, table),
		)
		if err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
			!stage4AdapterIncrementalErrorHas(
				err,
				"validation mode full is reserved and unsupported",
			) {
			t.Fatalf("full incremental validation error = %v", err)
		}
		if len(events) != 0 {
			t.Fatalf("full validation performed target preflight events = %#v", events)
		}
	})
}

func TestStage4AdapterIncrementalValidationSpecsUseAdmittedPrimaryKeyProofs(
	t *testing.T,
) {
	events := make([]string, 0)
	table := stage4IncrementalMatrixTable("postgres", "items")
	prepared := stage4IncrementalMatrixPrepared(t, table, table)
	cfg := stage4IncrementalMatrixConfig()
	cfg.Migration.Validation.Mode = config.ValidationSample
	incremental, work, err := prepareStage4AdapterIncremental(
		context.Background(),
		cfg,
		&stage4IncrementalTestSource{events: &events, table: table},
		&stage4IncrementalTestTarget{events: &events},
		prepared,
	)
	if err != nil {
		t.Fatalf("prepare incremental validation: %v", err)
	}
	defer func() {
		if err := incremental.validation.Close(); err != nil {
			t.Errorf("close incremental validation evidence: %v", err)
		}
	}()
	prepared.incremental = incremental
	prepared.work = work
	prepared.gate.ValidationTables = []schema.Table{table}
	specs, err := stage4AdapterValidationTableSpecs(prepared)
	if err != nil {
		t.Fatalf("build incremental validation specs: %v", err)
	}
	if len(specs) != 1 ||
		!validValidationEqualityProofDigest(specs[0].PrimaryKeyEqualityProof) {
		t.Fatalf("incremental validation specs = %#v", specs)
	}
	key := stage4RichTableKey{schema: table.Schema, table: table.Name}
	if specs[0].PrimaryKeyEqualityProof !=
		incremental.validationPrimaryKeyEqualityProofs[key] {
		t.Fatalf("incremental validation proof did not retain admitted authority")
	}
}

func TestStage4AdapterIncrementalEvidenceUsesFullProjectionOnlyForSample(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "opaque_value", Type: "geography"},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	projection := []string{"id", "opaque_value", "updated_at"}
	for _, testCase := range []struct {
		mode    config.ValidationMode
		wantErr bool
	}{
		{mode: config.ValidationCountOnly},
		{mode: config.ValidationNullParity},
		{mode: config.ValidationSample, wantErr: true},
	} {
		t.Run(string(testCase.mode), func(t *testing.T) {
			events := make([]string, 0)
			evidence, err := newStage4AdapterIncrementalValidationEvidence(
				testCase.mode,
				[]adapterTablePlan{{
					source:  table,
					target:  table,
					columns: projection,
				}},
				&stage4IncrementalTestTarget{events: &events},
				t.TempDir(),
			)
			if testCase.wantErr {
				if err == nil || !containsStage4AdapterIncrementalText(
					err.Error(),
					"unsupported validation column type",
				) {
					t.Fatalf("sample projection admission error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s projection admission: %v", testCase.mode, err)
			}
			if err := evidence.Close(); err != nil {
				t.Fatalf("cleanup %s projection evidence: %v", testCase.mode, err)
			}
		})
	}
}

func TestStage4AdapterIncrementalSampleRefusesUnsupportedSourceBeforeMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	sourceTable := stage4IncrementalMatrixTable("postgres", "items")
	targetTable := cloneStage4RichTable(sourceTable)
	for index := range sourceTable.Columns {
		if sourceTable.Columns[index].Name == "payload" {
			sourceTable.Columns[index].Type = "geography"
			targetTable.Columns[index].Type = "text"
			break
		}
	}
	projection := []string{"tenant_id", "item_id", "payload", "updated_at"}
	if _, err := validateValidationCoreProjection(
		targetTable,
		projection,
		true,
	); err != nil {
		t.Fatalf("target sample projection must remain supported: %v", err)
	}
	cfg := stage4IncrementalMatrixConfig()
	cfg.Migration.Validation.Mode = config.ValidationSample
	_, _, err := prepareStage4AdapterIncremental(
		context.Background(),
		cfg,
		&stage4IncrementalTestSource{events: &events, table: sourceTable},
		&stage4IncrementalTestTarget{
			events:                  &events,
			validateFinalProjection: true,
		},
		stage4IncrementalMatrixPrepared(t, sourceTable, targetTable),
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
		!stage4AdapterIncrementalErrorHas(
			err,
			"incremental validation source projection",
		) ||
		!stage4AdapterIncrementalErrorHas(
			err,
			"unsupported validation column type",
		) {
		t.Fatalf("unsupported source sample projection error = %v", err)
	}
	for _, forbidden := range []string{
		"source_sample_fence",
		"source_incremental_rows",
		"target_prepare",
		"target_write",
	} {
		if count := stage4IncrementalEventCount(events, forbidden); count != 0 {
			t.Fatalf(
				"unsupported source sample projection performed %s %d times: %#v",
				forbidden,
				count,
				events,
			)
		}
	}
}

func TestStage4AdapterIncrementalSampleBindsFinalValidationToTransferredAttempt(
	t *testing.T,
) {
	events := make([]string, 0)
	first := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
			{Name: "updated_at", Type: "timestamp", Nullable: true},
		},
	}
	source := &stage4IncrementalTestSource{
		events: &events,
		table:  table,
	}
	for id := 1; id <= sqliteWriteBatchSize+1; id++ {
		payload := any(fmt.Sprintf("row-%d", id))
		if id == 1 {
			payload = nil
		}
		source.rows = append(source.rows, stage4IncrementalTestRow{
			id:      int64(id),
			payload: payload,
			updated: &first,
		})
	}
	target := &stage4IncrementalTestTarget{
		events: &events,
		rows: map[int64][]any{
			99: {int64(99), "target-only", nil},
		},
	}
	probe := &stage4IncrementalDeepValidationProbe{
		source: source,
		target: target,
	}
	backend := state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")}
	cfg := config.Config{
		Source: config.Endpoint{
			Type: "postgres", Host: "source.example", Port: 5432, Database: "source",
		},
		Target: config.Endpoint{
			Type: "postgres", Host: "target.example", Port: 5432, Database: "target",
		},
		Migration: config.Migration{
			TargetMode:         "upsert",
			DateUpdatedColumns: []string{"updated_at"},
			Validation: config.ValidationPolicy{
				Mode:           config.ValidationSample,
				FailOnMismatch: true,
				FailOnTimeout:  true,
			},
			Deletes: config.DeletePolicy{Mode: config.DeleteModeOff},
		},
	}
	task := state.TaskKey{
		Type: stage4AdapterNetworkTaskType, Schema: table.Schema, Table: table.Name,
	}
	postFenceID := int64(sqliteWriteBatchSize + 2)
	target.afterBatchValidation = func(
		validated int,
		current *stage4IncrementalTestTarget,
	) {
		if validated != 2 {
			return
		}
		// This happens after the second (and final) exact batch proof. The
		// final §12 target read must see the deletion, while it must not let a
		// later source row alter the transferred-attempt source facts.
		delete(current.rows, 1)
		source.rows = append(source.rows, stage4IncrementalTestRow{
			id: postFenceID, payload: "post-fence", updated: &second,
		})
	}
	freshRun := "stage4-incremental-attempt-evidence-fresh"
	initializeStage4LifecycleRun(
		t,
		backend,
		freshRun,
		time.Now().Add(-time.Minute),
	)
	freshObserver := stage4IncrementalTestObserver{
		events:          &events,
		backend:         backend,
		run:             stage4LifecycleRunContext(t, backend, freshRun, false),
		validationProbe: probe,
	}
	_, err := migrateWithAdapters(
		context.Background(),
		cfg,
		freshObserver,
		source,
		target,
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassValidation {
		t.Fatalf("fresh transferred-attempt sample mismatch error = %v", err)
	}
	entries, readErr := os.ReadDir(freshObserver.run.SpoolDirectory)
	if readErr != nil {
		t.Fatalf("read fresh incremental evidence spool directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("fresh failed incremental validation left spool artifacts %#v", entries)
	}
	if _, found := target.rows[99]; !found {
		t.Fatal("source-owned final validation removed a target-only row")
	}
	if target.batchValidationCount != 2 {
		t.Fatalf("fresh exact batch validations = %d, want two", target.batchValidationCount)
	}
	if len(target.finalReadKeys) != sqliteWriteBatchSize+1 {
		t.Fatalf("fresh final target key reads = %d, want %d", len(target.finalReadKeys), sqliteWriteBatchSize+1)
	}
	for _, id := range target.finalReadKeys {
		if id == postFenceID {
			t.Fatal("final target validation queried a post-fence source key")
		}
	}
	exact, nulls, sourceSamples, targetSamples, _ := probe.calls()
	if len(exact) != 0 || len(nulls) != 0 || sourceSamples != 0 || targetSamples != 0 {
		t.Fatalf(
			"final validation used the live whole-source probe exact=%#v nulls=%#v source_samples=%d target_samples=%d",
			exact,
			nulls,
			sourceSamples,
			targetSamples,
		)
	}
	attempt, found, err := backend.LoadLatestCommittedIncrementalAttempt(
		freshRun,
		task,
	)
	if err != nil || !found || attempt.Status != state.IncrementalCompleted ||
		attempt.CommittedWatermark == nil || !attempt.CommittedWatermark.Value.Equal(first) {
		t.Fatalf("completed fresh attempt found=%v attempt=%#v err=%v", found, attempt, err)
	}

	// The initial baseline is a full-table attempt, so remove the later source
	// mutation before its completed-attempt replay. Introduce a new mutation
	// only after the resumed stream has selected and re-proved its final batch:
	// final validation must use the scoped evidence rather than a later source
	// query, while still refusing the target deletion.
	if len(source.rows) == 0 || source.rows[len(source.rows)-1].id != postFenceID {
		t.Fatalf("fresh post-fence source row was not retained %#v", source.rows)
	}
	source.rows = source.rows[:len(source.rows)-1]
	resumePostFenceID := postFenceID + 1
	target.rows[1] = []any{int64(1), nil, first}
	target.finalReadKeys = nil
	target.afterBatchValidation = func(
		validated int,
		current *stage4IncrementalTestTarget,
	) {
		if validated == 4 {
			delete(current.rows, 1)
			source.rows = append(source.rows, stage4IncrementalTestRow{
				id:      resumePostFenceID,
				payload: "resume-post-fence",
				updated: &second,
			})
		}
	}
	resumeObserver := stage4IncrementalTestObserver{
		events:          &events,
		backend:         backend,
		resume:          true,
		run:             stage4LifecycleRunContext(t, backend, freshRun, true),
		validationProbe: probe,
	}
	_, err = resumeWithAdapters(
		context.Background(),
		cfg,
		CompletedTableCheckpoints{table.Name: {Rows: sqliteWriteBatchSize + 1}},
		resumeObserver,
		resumeObserver,
		source,
		target,
	)
	if err == nil || ClassifyTransferError(err) != ErrorClassValidation {
		t.Fatalf("completed-window final sample mismatch error = %v", err)
	}
	entries, readErr = os.ReadDir(resumeObserver.run.SpoolDirectory)
	if readErr != nil {
		t.Fatalf("read resumed incremental evidence spool directory: %v", readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("resumed failed incremental validation left spool artifacts %#v", entries)
	}
	if target.batchValidationCount != 4 {
		t.Fatalf("completed-window exact batch validations = %d, want four", target.batchValidationCount)
	}
	if len(target.finalReadKeys) != sqliteWriteBatchSize+1 {
		t.Fatalf("resume final target key reads = %d, want %d", len(target.finalReadKeys), sqliteWriteBatchSize+1)
	}
	for _, id := range target.finalReadKeys {
		if id == postFenceID || id == resumePostFenceID {
			t.Fatalf(
				"resume final target validation queried a later source key %d",
				id,
			)
		}
	}
}

func TestStage4AdapterIncrementalFinalEvidencePinsLaterTargetKeyBatches(
	t *testing.T,
) {
	events := make([]string, 0)
	observedAt := time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC)
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text", Nullable: true},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	projection := []string{"id", "payload", "updated_at"}
	target := &stage4IncrementalTestTarget{
		events: &events,
		rows:   make(map[int64][]any, adapterValidationMaximumKeyBatch+1),
	}
	sourceRows := make([][]any, 0, adapterValidationMaximumKeyBatch+1)
	for id := 1; id <= adapterValidationMaximumKeyBatch+1; id++ {
		payload := any(fmt.Sprintf("row-%d", id))
		if id == 1 {
			payload = nil
		}
		row := []any{int64(id), payload, observedAt}
		sourceRows = append(sourceRows, row)
		target.rows[int64(id)] = cloneAdapterRow(row)
	}
	spoolDirectory := t.TempDir()
	evidence, err := newStage4AdapterIncrementalValidationEvidence(
		config.ValidationNullParity,
		[]adapterTablePlan{{
			source:  table,
			target:  table,
			columns: projection,
		}},
		target,
		spoolDirectory,
	)
	if err != nil {
		t.Fatalf("construct incremental final evidence: %v", err)
	}
	if err := evidence.RecordExactBatch(
		context.Background(),
		table,
		projection,
		sourceRows,
	); err != nil {
		t.Fatalf("record exact incremental batch: %v", err)
	}

	// This models two external writes after exact batch proof. The early row
	// becomes non-NULL before final validation. A later, as-yet-unread key is
	// changed to NULL between target key batches. A live per-batch reader would
	// combine those two instants into a false null-parity pass; a pinned target
	// view must retain the pre-second-write target facts and report the drift.
	target.rows[1][1] = "external-early-change"
	target.freezeFinalSnapshot = true
	target.afterFinalRead = func(
		readBatch int,
		current *stage4IncrementalTestTarget,
	) {
		if readBatch == 1 {
			current.rows[int64(adapterValidationMaximumKeyBatch+1)][1] = nil
		}
	}
	report, err := RunValidationCore(
		context.Background(),
		ValidationCoreOptions{
			Mode:              config.ValidationNullParity,
			TargetMode:        "upsert",
			FailOnMismatch:    true,
			FailOnTimeout:     true,
			ExactCountTimeout: 5 * time.Second,
			TableTimeout:      5 * time.Second,
			TableConcurrency:  1,
		},
		[]ValidationTableSpec{{
			Table:                   table,
			Projection:              projection,
			PrimaryKeyEqualityProof: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}},
		evidence,
	)
	if err != nil {
		t.Fatalf("run incremental final validation core: %v", err)
	}
	if report.Passed {
		t.Fatalf("torn target batches passed NULL parity: %#v", report.Findings)
	}
	finding := findCoreValidationFinding(
		t,
		report,
		"items",
		"validation.null_parity.compare",
		"payload",
		0,
	)
	if finding.Outcome != ValidationOutcomeMismatch {
		t.Fatalf("pinned target NULL finding = %#v", finding)
	}
	if target.finalReadBatches != 2 {
		t.Fatalf("final target read batches = %d, want two", target.finalReadBatches)
	}
	if row := target.rows[int64(adapterValidationMaximumKeyBatch+1)]; row[1] != nil {
		t.Fatalf("later-key mutation did not execute: %#v", row)
	}
	if err := evidence.Close(); err != nil {
		t.Fatalf("cleanup incremental final evidence: %v", err)
	}
	entries, err := os.ReadDir(spoolDirectory)
	if err != nil {
		t.Fatalf("read incremental validation spool directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("incremental validation spool cleanup left %#v", entries)
	}
}

func TestStage4AdapterIncrementalSQLServerFinalTargetViewContract(
	t *testing.T,
) {
	options := stage4AdapterIncrementalTargetSnapshotOptions(
		adapterValidationSQLServer,
	)
	if options.Isolation != sql.LevelSerializable || options.ReadOnly {
		t.Fatalf("SQL Server final validation options = %#v", options)
	}
	query, err := stage4AdapterIncrementalSQLServerTargetLockQuery(
		adapterValidationSQLEndpoint{
			engine:    adapterValidationSQLServer,
			namespace: "dbo",
		},
		schema.Table{Schema: "dbo", Name: "items"},
	)
	if err != nil {
		t.Fatalf("build SQL Server final target lock query: %v", err)
	}
	if query != "SELECT TOP (1) 1 FROM [dbo].[items] WITH (TABLOCK, HOLDLOCK)" {
		t.Fatalf("SQL Server final target lock query = %q", query)
	}
	ordinary := stage4AdapterIncrementalTargetSnapshotOptions(
		adapterValidationPostgres,
	)
	if ordinary.Isolation != sql.LevelRepeatableRead || !ordinary.ReadOnly {
		t.Fatalf("ordinary final validation options = %#v", ordinary)
	}
}

func TestStage4AdapterIncrementalFinalEvidenceAvoidsWidePayloadReadsOutsideSample(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "blob", Nullable: true},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	projection := []string{"id", "payload", "updated_at"}
	widePayload := make([]byte, 2<<20)
	widePayload[len(widePayload)-1] = 1
	observedAt := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	row := []any{int64(1), widePayload, observedAt}

	for _, mode := range []config.ValidationMode{
		config.ValidationCountOnly,
		config.ValidationNullParity,
	} {
		t.Run(string(mode), func(t *testing.T) {
			events := make([]string, 0)
			target := &stage4IncrementalTestTarget{
				events:              &events,
				rows:                map[int64][]any{1: cloneAdapterRow(row)},
				forbidFinalFullRows: true,
			}
			evidence, err := newStage4AdapterIncrementalValidationEvidence(
				mode,
				[]adapterTablePlan{{
					source:  table,
					target:  table,
					columns: projection,
				}},
				target,
				t.TempDir(),
			)
			if err != nil {
				t.Fatalf("construct incremental final evidence: %v", err)
			}
			defer func() {
				if err := evidence.Close(); err != nil {
					t.Errorf("cleanup incremental final evidence: %v", err)
				}
			}()
			if err := evidence.RecordExactBatch(
				context.Background(),
				table,
				projection,
				[][]any{cloneAdapterRow(row)},
			); err != nil {
				t.Fatalf("record exact incremental batch: %v", err)
			}
			report, err := RunValidationCore(
				context.Background(),
				ValidationCoreOptions{
					Mode:              mode,
					TargetMode:        "upsert",
					FailOnMismatch:    true,
					FailOnTimeout:     true,
					ExactCountTimeout: time.Second,
					TableTimeout:      time.Second,
					TableConcurrency:  1,
				},
				[]ValidationTableSpec{{
					Table:                   table,
					Projection:              projection,
					PrimaryKeyEqualityProof: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				}},
				evidence,
			)
			if err != nil || !report.Passed {
				t.Fatalf("run %s final validation report=%#v err=%v", mode, report, err)
			}
			if len(target.finalSampleReadKeys) != 0 || target.finalSampleReadBatches != 0 {
				t.Fatalf(
					"%s fetched full target payload rows keys=%#v batches=%d",
					mode,
					target.finalSampleReadKeys,
					target.finalSampleReadBatches,
				)
			}
			if len(target.finalReadKeys) != 1 {
				t.Fatalf("%s target key reads = %#v, want [1]", mode, target.finalReadKeys)
			}
			if mode == config.ValidationCountOnly {
				if target.finalNullReadBatches != 0 {
					t.Fatalf("count-only target NULL reads = %d, want zero", target.finalNullReadBatches)
				}
			} else if target.finalNullReadBatches != 1 ||
				len(target.finalNullReadKeys) != 1 {
				t.Fatalf(
					"null-parity compact target NULL reads batches=%d keys=%#v",
					target.finalNullReadBatches,
					target.finalNullReadKeys,
				)
			}
		})
	}

	endpoint := adapterValidationSQLEndpoint{
		engine:    adapterValidationPostgres,
		namespace: "public",
	}
	predicate := `("id" = $1)`
	keyQuery := stage4AdapterIncrementalTargetKeyQuery(
		endpoint,
		table,
		[]schema.Column{table.Columns[0]},
		predicate,
	)
	if keyQuery != `SELECT "id" FROM "public"."items" WHERE ("id" = $1)` {
		t.Fatalf("target key query = %q", keyQuery)
	}
	nullQuery := stage4AdapterIncrementalTargetNullCountQuery(
		endpoint,
		table,
		projection,
		predicate,
	)
	if strings.Contains(nullQuery, `SELECT "id", "payload"`) ||
		!strings.Contains(nullQuery, `CASE WHEN "payload" IS NULL`) {
		t.Fatalf("target compact NULL query = %q", nullQuery)
	}
}

func TestStage4AdapterIncrementalEvidenceSpoolsWideSourceSamples(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "blob"},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	projection := []string{"id", "payload", "updated_at"}
	events := make([]string, 0)
	spoolDirectory := t.TempDir()
	evidence, err := newStage4AdapterIncrementalValidationEvidence(
		config.ValidationSample,
		[]adapterTablePlan{{
			source:  table,
			target:  table,
			columns: projection,
		}},
		&stage4IncrementalTestTarget{events: &events},
		spoolDirectory,
	)
	if err != nil {
		t.Fatalf("construct incremental source sample evidence: %v", err)
	}
	defer func() {
		if err := evidence.Close(); err != nil {
			t.Errorf("cleanup incremental source sample evidence: %v", err)
		}
	}()

	const payloadBytes = 256 << 10
	observedAt := time.Date(2026, 8, 1, 13, 0, 0, 0, time.UTC)
	rows := make([][]any, 0, stage4AdapterIncrementalValidationSampleLimit+1)
	for id := 1; id <= stage4AdapterIncrementalValidationSampleLimit+1; id++ {
		payload := make([]byte, payloadBytes)
		payload[0] = byte(id)
		payload[len(payload)-1] = byte(id >> 1)
		rows = append(rows, []any{int64(id), payload, observedAt})
	}
	if err := evidence.RecordExactBatch(
		context.Background(),
		table,
		projection,
		rows,
	); err != nil {
		t.Fatalf("record wide incremental source samples: %v", err)
	}
	item, err := evidence.table(table)
	if err != nil {
		t.Fatalf("load incremental source sample evidence: %v", err)
	}
	item.mu.Lock()
	if len(item.samples) != stage4AdapterIncrementalValidationSampleLimit {
		item.mu.Unlock()
		t.Fatalf("retained source sample metadata = %d, want %d", len(item.samples), stage4AdapterIncrementalValidationSampleLimit)
	}
	metadataBytes := int64(0)
	for _, sample := range item.samples {
		keyBytes, err := measureAdapterRetainedRowBytes(sample.keyValues)
		if err != nil {
			item.mu.Unlock()
			t.Fatalf("measure retained source sample key metadata: %v", err)
		}
		metadataBytes += int64(len(sample.canonical)) + keyBytes
	}
	spool := item.keySpool
	item.mu.Unlock()
	if spool == nil || spool.db == nil {
		t.Fatal("wide incremental source sample spool is unavailable")
	}
	if metadataBytes >= int64(payloadBytes) {
		t.Fatalf(
			"source sample metadata retained %d bytes, want less than one %d-byte payload",
			metadataBytes,
			payloadBytes,
		)
	}
	var storedRows int
	var storedBytes int64
	if err := spool.db.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(LENGTH(row_values)), 0) FROM incremental_samples`,
	).Scan(&storedRows, &storedBytes); err != nil {
		t.Fatalf("inspect wide incremental source sample spool: %v", err)
	}
	if storedRows != stage4AdapterIncrementalValidationSampleLimit ||
		storedBytes <= int64(payloadBytes)*stage4AdapterIncrementalValidationSampleLimit {
		t.Fatalf(
			"wide incremental source samples are not durably private-spooled rows=%d bytes=%d",
			storedRows,
			storedBytes,
		)
	}
	// Mutating the transfer batch after recording cannot alter the selected
	// source sample, which proves the evidence retained it in the private spool
	// rather than in the in-memory metadata slice.
	for _, row := range rows {
		row[1].([]byte)[0] = 0
	}
	samples, err := evidence.SampleSourceRows(
		context.Background(),
		table,
		projection,
		stage4AdapterIncrementalValidationSampleLimit,
	)
	if err != nil {
		t.Fatalf("read wide incremental source samples: %v", err)
	}
	if len(samples) != stage4AdapterIncrementalValidationSampleLimit {
		t.Fatalf("wide source samples = %d, want %d", len(samples), stage4AdapterIncrementalValidationSampleLimit)
	}
	for index, sample := range samples {
		payload, ok := sample.Values[1].([]byte)
		if !ok || len(payload) != payloadBytes || payload[0] != byte(index+1) {
			t.Fatalf("wide source sample %d payload was not preserved", index+1)
		}
	}
	if err := evidence.Close(); err != nil {
		t.Fatalf("cleanup wide incremental source sample evidence: %v", err)
	}
	entries, err := os.ReadDir(spoolDirectory)
	if err != nil {
		t.Fatalf("read wide incremental source sample spool directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("wide incremental source sample cleanup left %#v", entries)
	}
}

func TestStage4AdapterIncrementalFinalSampleReadsOnlySelectedWideRows(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "blob", Nullable: true},
			{Name: "updated_at", Type: "timestamp"},
		},
	}
	projection := []string{"id", "payload", "updated_at"}
	observedAt := time.Date(2026, 8, 1, 13, 30, 0, 0, time.UTC)
	const payloadBytes = 128 << 10
	rows := make([][]any, 0, stage4AdapterIncrementalValidationSampleLimit+1)
	targetRows := make(map[int64][]any, stage4AdapterIncrementalValidationSampleLimit+1)
	for id := 1; id <= stage4AdapterIncrementalValidationSampleLimit+1; id++ {
		payload := make([]byte, payloadBytes)
		payload[0] = byte(id)
		row := []any{int64(id), payload, observedAt}
		rows = append(rows, row)
		targetRows[int64(id)] = cloneAdapterRow(row)
	}
	events := make([]string, 0)
	target := &stage4IncrementalTestTarget{
		events: &events,
		rows:   targetRows,
	}
	evidence, err := newStage4AdapterIncrementalValidationEvidence(
		config.ValidationSample,
		[]adapterTablePlan{{
			source:  table,
			target:  table,
			columns: projection,
		}},
		target,
		t.TempDir(),
	)
	if err != nil {
		t.Fatalf("construct sampled target evidence: %v", err)
	}
	defer func() {
		if err := evidence.Close(); err != nil {
			t.Errorf("cleanup sampled target evidence: %v", err)
		}
	}()
	if err := evidence.RecordExactBatch(
		context.Background(),
		table,
		projection,
		rows,
	); err != nil {
		t.Fatalf("record sampled target evidence: %v", err)
	}
	report, err := RunValidationCore(
		context.Background(),
		ValidationCoreOptions{
			Mode:              config.ValidationSample,
			TargetMode:        "upsert",
			FailOnMismatch:    true,
			FailOnTimeout:     true,
			ExactCountTimeout: 5 * time.Second,
			TableTimeout:      5 * time.Second,
			TableConcurrency:  1,
			SampleLimit:       stage4AdapterIncrementalValidationSampleLimit,
		},
		[]ValidationTableSpec{{
			Table:                   table,
			Projection:              projection,
			PrimaryKeyEqualityProof: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}},
		evidence,
	)
	if err != nil || !report.Passed {
		t.Fatalf("run sampled target validation report=%#v err=%v", report, err)
	}
	if len(target.finalReadKeys) != stage4AdapterIncrementalValidationSampleLimit+1 {
		t.Fatalf("exact target membership reads = %d, want %d", len(target.finalReadKeys), stage4AdapterIncrementalValidationSampleLimit+1)
	}
	if len(target.finalSampleReadKeys) != stage4AdapterIncrementalValidationSampleLimit {
		t.Fatalf("full target sample reads = %d, want %d", len(target.finalSampleReadKeys), stage4AdapterIncrementalValidationSampleLimit)
	}
	for _, id := range target.finalSampleReadKeys {
		if id > stage4AdapterIncrementalValidationSampleLimit {
			t.Fatalf("full target payload read for non-sampled key %d", id)
		}
	}
}

func TestStage4AdapterIncrementalEvidenceSpoolSampleRoundTripsTypedRows(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "public",
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "nullable_text", Type: "text", Nullable: true},
			{Name: "payload", Type: "blob", Nullable: true},
			{Name: "large_unsigned", Type: "decimal"},
			{Name: "observed_at", Type: "timestamp"},
		},
	}
	projection := []string{
		"id", "nullable_text", "payload", "large_unsigned", "observed_at",
	}
	observedAt := time.Date(2026, 8, 1, 14, 0, 0, 123456789, time.UTC)
	rows := [][]any{
		{int64(2), "second", []byte{0, 2, 255}, uint64(1 << 63), observedAt.Add(time.Second)},
		{int64(1), nil, []byte{0, 1, 255}, ^uint64(0), observedAt},
	}
	events := make([]string, 0)
	target := &stage4IncrementalTestTarget{
		events: &events,
		rows: map[int64][]any{
			1: cloneAdapterRow(rows[1]),
			2: cloneAdapterRow(rows[0]),
		},
	}
	evidence, err := newStage4AdapterIncrementalValidationEvidence(
		config.ValidationSample,
		[]adapterTablePlan{{
			source:  table,
			target:  table,
			columns: projection,
		}},
		target,
		t.TempDir(),
	)
	if err != nil {
		t.Fatalf("construct typed incremental source sample evidence: %v", err)
	}
	defer func() {
		if err := evidence.Close(); err != nil {
			t.Errorf("cleanup typed incremental source sample evidence: %v", err)
		}
	}()
	if err := evidence.RecordExactBatch(
		context.Background(),
		table,
		projection,
		rows,
	); err != nil {
		t.Fatalf("record typed incremental source samples: %v", err)
	}
	report, err := RunValidationCore(
		context.Background(),
		ValidationCoreOptions{
			Mode:              config.ValidationSample,
			TargetMode:        "upsert",
			FailOnMismatch:    true,
			FailOnTimeout:     true,
			ExactCountTimeout: time.Second,
			TableTimeout:      time.Second,
			TableConcurrency:  1,
			SampleLimit:       2,
		},
		[]ValidationTableSpec{{
			Table:                   table,
			Projection:              projection,
			PrimaryKeyEqualityProof: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}},
		evidence,
	)
	if err != nil || !report.Passed {
		t.Fatalf("run typed incremental spool sample validation report=%#v err=%v", report, err)
	}
	if got := target.finalSampleReadKeys; !reflect.DeepEqual(got, []int64{1, 2}) {
		t.Fatalf("typed incremental target sample keys = %#v, want [1 2]", got)
	}
}

func stage4IncrementalEventCount(events []string, want string) int {
	count := 0
	for _, event := range events {
		if event == want {
			count++
		}
	}
	return count
}

func TestPrepareStage4AdapterIncrementalAdmitsCertifiedRelationalSQLiteMatrix(
	t *testing.T,
) {
	for _, sourceEngine := range []string{
		"postgres",
		"mssql",
		"mysql",
		"sqlite",
	} {
		for _, targetEngine := range []string{
			"postgres",
			"mssql",
			"mysql",
			"sqlite",
		} {
			t.Run(sourceEngine+"-to-"+targetEngine, func(t *testing.T) {
				events := make([]string, 0)
				sourceTable := stage4IncrementalMatrixTable(
					sourceEngine,
					"items",
				)
				targetTable := sourceTable
				targetTable.Schema = stage4IncrementalMatrixNamespace(
					targetEngine,
				)
				source := &stage4IncrementalTestSource{
					events: &events,
					table:  sourceTable,
					engine: sourceEngine,
				}
				target := &stage4IncrementalTestTarget{
					events: &events,
					engine: targetEngine,
				}
				prepared := stage4IncrementalMatrixPrepared(
					t,
					sourceTable,
					targetTable,
				)
				incremental, work, err := prepareStage4AdapterIncremental(
					context.Background(),
					stage4IncrementalMatrixConfig(),
					source,
					target,
					prepared,
				)
				if err != nil {
					t.Fatalf("prepare incremental route: %v", err)
				}
				if incremental == nil || len(incremental.tables) != 1 ||
					len(work) != 1 ||
					work[0].strategy != stage4AdapterIncrementalStrategy {
					t.Fatalf(
						"incremental=%#v work=%#v",
						incremental,
						work,
					)
				}
				assertStage4AdapterEventBefore(
					t,
					events,
					"target_isolation",
					"target_incremental_admission",
				)
			})
		}
	}
}

func TestStage4IncrementalRoutesSQLiteCompatibilityThroughComposedRunner(
	t *testing.T,
) {
	route := resolvedAdapterRoute{
		source: sourceRole{engine: "sqlite"},
		target: targetRole{engine: "sqlite"},
	}
	legacy := stage4IncrementalMatrixConfig()
	legacy.Migration.DateUpdatedColumns = nil
	if stage4SQLiteCompatibilityRouteRequiresComposition(legacy, route) {
		t.Fatal("non-incremental SQLite route unexpectedly bypassed compatibility runner")
	}
	if !stage4SQLiteCompatibilityRouteRequiresComposition(
		stage4IncrementalMatrixConfig(),
		route,
	) {
		t.Fatal("SQLite incremental route did not select composed Stage 4 runner")
	}
}

func TestPrepareStage4AdapterIncrementalFailsClosedForUnsupportedCapabilities(
	t *testing.T,
) {
	cfg := config.Config{
		Migration: config.Migration{
			TargetMode:         "upsert",
			DateUpdatedColumns: []string{"updated_at"},
		},
	}
	events := make([]string, 0)
	source := &stage4IncrementalTestSource{events: &events}
	target := &recordingAdapterTarget{events: &events}
	_, _, err := prepareStage4AdapterIncremental(
		context.Background(),
		cfg,
		source,
		target,
		stage4AdapterPrepared{mode: "upsert"},
	)
	if err == nil ||
		!stage4AdapterIncrementalErrorHas(
			err,
			"no certified exact incremental window validation path",
		) {
		t.Fatalf("error = %v", err)
	}

	for name, fixture := range map[string]struct {
		sourceEngine string
		targetEngine string
		want         string
	}{
		"ClickHouse source": {
			sourceEngine: "clickhouse",
			targetEngine: "postgres",
			want:         "source engine \"clickhouse\" is not certified",
		},
		"ClickHouse target": {
			sourceEngine: "postgres",
			targetEngine: "clickhouse",
			want:         "target engine \"clickhouse\" is not certified",
		},
	} {
		t.Run(name, func(t *testing.T) {
			events := make([]string, 0)
			sourceTable := stage4IncrementalMatrixTable(
				fixture.sourceEngine,
				"items",
			)
			source := &stage4IncrementalTestSource{
				events: &events,
				table:  sourceTable,
				engine: fixture.sourceEngine,
			}
			target := &stage4IncrementalTestTarget{
				events: &events,
				engine: fixture.targetEngine,
			}
			_, _, err := prepareStage4AdapterIncremental(
				context.Background(),
				stage4IncrementalMatrixConfig(),
				source,
				target,
				stage4IncrementalMatrixPrepared(t, sourceTable, sourceTable),
			)
			if err == nil || !stage4AdapterIncrementalErrorHas(err, fixture.want) {
				t.Fatalf("error = %v", err)
			}
		})
	}

	t.Run("writer", func(t *testing.T) {
		events := make([]string, 0)
		table := stage4IncrementalMatrixTable("postgres", "items")
		source := &stage4IncrementalTestSource{
			events: &events,
			table:  table,
		}
		target := &stage4IncrementalTestTarget{
			events:               &events,
			incrementalAdmission: fmt.Errorf("native writer is unavailable"),
		}
		_, _, err := prepareStage4AdapterIncremental(
			context.Background(),
			stage4IncrementalMatrixConfig(),
			source,
			target,
			stage4IncrementalMatrixPrepared(t, table, table),
		)
		if err == nil || !stage4AdapterIncrementalErrorHas(
			err,
			"native writer is unavailable",
		) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestStage4IncrementalValidationUsesExactCompositeKeysAcrossDriverIntegers(
	t *testing.T,
) {
	database := openAdapterValidationSQLiteTestDatabase(
		t,
		filepath.Join(t.TempDir(), "incremental-validation.db"),
	)
	if _, err := database.Exec(`CREATE TABLE items (
		tenant_id INTEGER NOT NULL,
		item_id INTEGER NOT NULL,
		payload TEXT NOT NULL,
		PRIMARY KEY (tenant_id, item_id)
	)`); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO items
		(tenant_id, item_id, payload) VALUES (7, 11, 'first'), (8, 12, 'second')`); err != nil {
		t.Fatal(err)
	}
	table := schema.Table{
		Name: "items",
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "item_id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{Name: "payload", Type: "text"},
		},
	}
	adapter := &sqliteTargetAdapter{database: database}
	if err := adapter.ValidateStage4IncrementalBatch(
		context.Background(),
		table,
		[]string{"tenant_id", "item_id", "payload"},
		[][]any{
			{int(7), int64(11), "first"},
			{int64(8), int(12), "second"},
		},
	); err != nil {
		t.Fatalf("validate integer-normalized composite keys: %v", err)
	}
	if err := adapter.ValidateStage4IncrementalBatch(
		context.Background(),
		table,
		[]string{"tenant_id", "item_id", "payload"},
		[][]any{{int64(7), int64(11), "changed"}},
	); err == nil || !stage4AdapterIncrementalErrorHas(
		err,
		"target row differs",
	) {
		t.Fatalf("changed source value validation error = %v", err)
	}
}

func stage4IncrementalMatrixConfig() config.Config {
	return config.Config{Migration: config.Migration{
		TargetMode:         "upsert",
		DateUpdatedColumns: []string{"updated_at"},
		Validation: config.ValidationPolicy{
			Mode: config.ValidationCountOnly,
		},
	}}
}

func stage4IncrementalMatrixNamespace(engine string) string {
	switch engine {
	case "postgres":
		return "public"
	case "mssql":
		return "dbo"
	case "mysql":
		return "dmtx"
	case "sqlite":
		return ""
	default:
		return "public"
	}
}

func stage4IncrementalMatrixTable(engine string, name string) schema.Table {
	dateColumn := schema.Column{Name: "updated_at", Type: "timestamp"}
	switch engine {
	case "mysql":
		dateColumn.Type = "datetime"
		dateColumn.DeclaredType = &schema.DeclaredType{
			Base:      "datetime",
			Arguments: []int{3},
		}
	case "mssql":
		dateColumn.Type = "datetime"
		dateColumn.DeclaredType = &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{3},
		}
	case "sqlite":
		precision := int64(3)
		dateColumn.Type = "datetime"
		dateColumn.DeclaredType = &schema.DeclaredType{
			Base:                      "datetime",
			FractionalSecondPrecision: &precision,
		}
	}
	return schema.Table{
		Schema: stage4IncrementalMatrixNamespace(engine),
		Name:   name,
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "item_id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{Name: "payload", Type: "text"},
			dateColumn,
		},
	}
}

func stage4IncrementalMatrixPrepared(
	t *testing.T,
	source schema.Table,
	target schema.Table,
) stage4AdapterPrepared {
	t.Helper()
	const runID = "stage4-incremental-matrix"
	return stage4AdapterPrepared{
		run: Stage4RunContext{
			RunID:          runID,
			Backend:        state.YAMLStore{Path: filepath.Join(t.TempDir(), "state.yaml")},
			SpoolDirectory: stage4LifecyclePrivateSpool(t, runID),
		},
		mode: "upsert",
		plans: []adapterTablePlan{{
			source:  source,
			target:  target,
			columns: []string{"tenant_id", "item_id", "payload", "updated_at"},
		}},
		targetTables: []schema.Table{target},
		work: []stage4AdapterWork{{
			task: state.TaskKey{
				Type:   stage4AdapterNetworkTaskType,
				Schema: source.Schema,
				Table:  source.Name,
			},
			topology: "matrix",
		}},
	}
}

func stage4AdapterIncrementalErrorHas(err error, text string) bool {
	return err != nil && len(text) != 0 &&
		fmt.Sprintf("%v", err) != "" &&
		containsStage4AdapterIncrementalText(err.Error(), text)
}

func containsStage4AdapterIncrementalText(value, text string) bool {
	for index := 0; index+len(text) <= len(value); index++ {
		if value[index:index+len(text)] == text {
			return true
		}
	}
	return false
}

func completeStage4IncrementalTestRun(
	t *testing.T,
	backend state.Backend,
	runID string,
) {
	t.Helper()
	runs, err := backend.List()
	if err != nil {
		t.Fatal(err)
	}
	var startedAt time.Time
	for _, run := range runs {
		if run.ID == runID && run.Outcome == state.Running {
			startedAt = run.StartedAt
		}
	}
	if startedAt.IsZero() {
		t.Fatalf("running state for %s was not found", runID)
	}
	if err := backend.Append(state.Run{
		ID:        runID,
		Outcome:   state.Success,
		Resumable: false,
		Reason:    "test migration completed",
		StartedAt: startedAt,
		EndedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
}
