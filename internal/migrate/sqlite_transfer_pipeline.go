package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/schema"
	"github.com/johndauphine/DMTX/internal/state"
)

// SQLiteTransferPlan is the immutable, pre-mutation execution plan exposed to
// optional observers. SQLite always uses one target writer.
type SQLiteTransferPlan struct {
	Table        string
	Pagination   PaginationPlan
	ChunkRows    int
	Partitions   int
	Readers      int
	Writers      int
	QueueDepth   int
	MemoryBudget int64
	Resources    config.EffectiveTransferPlan
}

// SQLiteTransferPlanObserver receives every table plan after all plans have
// been derived and before the first target mutation.
type SQLiteTransferPlanObserver interface {
	AfterSQLiteTransferPlan(context.Context, SQLiteTransferPlan) error
}

// SQLiteRangeProgress is one safely checkpointable contiguous range frontier.
type SQLiteRangeProgress struct {
	Table                string
	TopologyHash         string
	Range                PaginationRange
	Frontier             AckFrontier
	Watermark            *KeyTuple
	RowNumberWatermark   int64
	Memory               ByteBudgetStats
	Complete             bool
	ExpectedNextSequence uint64
}

// SQLiteRangeProgressObserver persists or records topology-aware range state.
// Legacy PageObserver remains supported for compatible single-range plans.
type SQLiteRangeProgressObserver interface {
	AfterSQLiteRangeProgress(context.Context, SQLiteRangeProgress) error
}

// SQLiteChunkInfo identifies a logical source chunk without exposing row data.
type SQLiteChunkInfo struct {
	Table    string
	RangeID  int
	Sequence uint64
	Rows     int
}

// SQLiteRangeChunk is the issued-intent identity persisted before target
// mutation. End is a typed key frontier; EndRow is used by ROW_NUMBER plans.
type SQLiteRangeChunk struct {
	Table        string
	TopologyHash string
	Range        PaginationRange
	Sequence     uint64
	ChunkRows    int
	End          *KeyTuple
	EndRow       int64
	Replay       bool
}

// SQLiteRangeChunkObserver brackets target mutation with durable transfer
// intent and receipt callbacks. Implementations must not hold the target
// mutation guard while checkpointing these callbacks.
type SQLiteRangeChunkObserver interface {
	BeforeSQLiteRangeChunk(context.Context, SQLiteRangeChunk) error
	AfterSQLiteRangeChunk(
		context.Context,
		SQLiteRangeChunk,
		WriteReceipt,
		AckFrontier,
	) error
}

// SQLiteRangeAttemptObserver records each target-write attempt for an issued
// range chunk. The callback runs inside the retry operation, after the durable
// chunk intent exists and before the target mutation guard is entered.
type SQLiteRangeAttemptObserver interface {
	BeforeSQLiteRangeAttempt(context.Context, SQLiteRangeChunk) error
}

// SQLiteRangeRestore is the exact persisted state for one planned range.
// SQLite transactional writes require SequenceOffset to be zero. Issued, when
// present, is the one intent durably recorded before a missing acknowledgement.
type SQLiteRangeRestore struct {
	Table              string
	TopologyHash       string
	Range              PaginationRange
	NextSequence       uint64
	SequenceOffset     int64
	Complete           bool
	RowsDone           int64
	Watermark          *KeyTuple
	RowNumberWatermark int64
	Issued             *SQLiteRangeChunk
}

// SQLiteRangeRestoreProvider reconciles a plan with durable range state before
// the table's first target mutation.
type SQLiteRangeRestoreProvider interface {
	RestoreSQLiteRanges(context.Context, SQLiteTransferPlan) ([]SQLiteRangeRestore, error)
}

// SQLiteChunkReadObserver is an optional deterministic scheduling/fault hook.
type SQLiteChunkReadObserver interface {
	BeforeSQLiteChunkRead(context.Context, SQLiteChunkInfo) error
}

// SQLiteChunkAcknowledgeObserver is invoked after a durable target commit and
// before its receipt enters the contiguous acknowledgement tracker. Multiple
// callbacks may run concurrently, allowing tests to prove out-of-order safety.
type SQLiteChunkAcknowledgeObserver interface {
	BeforeSQLiteChunkAcknowledge(context.Context, SQLiteChunkInfo, WriteReceipt) error
}

// SQLiteTargetMutationProtector optionally fences every durable SQLite target
// mutation. Notifications and checkpoint callbacks run after the guard exits.
type SQLiteTargetMutationProtector interface {
	ProtectTargetMutation(context.Context, func() error) error
}

func protectSQLiteTargetMutation(ctx context.Context, observer TableObserver, mutation func() error) error {
	if protector, ok := observer.(SQLiteTargetMutationProtector); ok {
		return protector.ProtectTargetMutation(ctx, mutation)
	}
	return mutation()
}

type sqliteEffectiveTransferSettings struct {
	targetMode string
	chunkRows  int
	partitions int
	readers    int
	queueDepth int
	memory     int64
	maxRetries int
	resources  config.EffectiveTransferPlan
}

func effectiveSQLiteTransferSettings(
	ctx context.Context,
	migration config.Migration,
	override *config.EffectiveTransferPlan,
) (sqliteEffectiveTransferSettings, error) {
	normalized := migration
	if normalized.TargetMode == "" {
		normalized.TargetMode = "drop_recreate"
	}
	normalized.ConnectionLimit = positiveOrDefault(normalized.ConnectionLimit, config.DefaultConnectionLimit)
	normalized.Workers = positiveOrDefault(normalized.Workers, config.DefaultWorkers)
	normalized.ChunkSize = positiveOrDefault(normalized.ChunkSize, config.DefaultChunkSize)
	normalized.Partitions = positiveOrDefault(normalized.Partitions, config.DefaultPartitions)
	normalized.ReaderParallelism = positiveOrDefault(normalized.ReaderParallelism, config.DefaultReaderParallelism)
	normalized.WriterParallelism = positiveOrDefault(normalized.WriterParallelism, config.DefaultWriterParallelism)
	normalized.ReadAhead = positiveOrDefault(normalized.ReadAhead, config.DefaultReadAhead)
	if normalized.MemoryCeilingBytes <= 0 {
		normalized.MemoryCeilingBytes = config.DefaultMemoryCeilingBytes
	}

	var resources config.EffectiveTransferPlan
	var err error
	if override != nil {
		resources = *override
	} else {
		resources, err = config.ResolveSystemEffectiveTransferPlan(
			ctx,
			normalized,
			config.TransferPlanOptions{},
		)
		if err != nil {
			return sqliteEffectiveTransferSettings{}, err
		}
	}
	if resources.MemoryBudget.Value <= 0 || resources.ChunkRows.Value <= 0 ||
		resources.Readers.Value <= 0 || resources.QueueDepth.Value <= 0 {
		return sqliteEffectiveTransferSettings{}, fmt.Errorf("invalid effective SQLite transfer resources")
	}

	readers := resources.Readers.Value
	if normalized.ConnectionLimit > 1 && readers > normalized.ConnectionLimit-1 {
		readers = normalized.ConnectionLimit - 1
	}
	if normalized.StrictConsistency {
		readers = 1
	}
	if readers < 1 {
		readers = 1
	}
	if resources.Readers.Value != readers {
		resources.Readers = config.EffectiveInt{
			Value: readers, Provenance: config.ProvenanceSafetyClamped,
		}
	}
	if resources.Writers.Value != 1 {
		resources.Writers = config.EffectiveInt{
			Value: 1, Provenance: config.ProvenanceSafetyClamped,
		}
	}
	maxRetries := migration.MaxRetries
	if maxRetries < 0 {
		return sqliteEffectiveTransferSettings{}, fmt.Errorf("migration.max_retries must not be negative")
	}
	return sqliteEffectiveTransferSettings{
		targetMode: resources.TargetMode,
		chunkRows:  resources.ChunkRows.Value,
		partitions: normalized.Partitions,
		readers:    readers,
		queueDepth: resources.QueueDepth.Value,
		memory:     resources.MemoryBudget.Value,
		maxRetries: maxRetries,
		resources:  resources,
	}, nil
}

func positiveOrDefault(value, fallback int) int {
	if value <= 0 {
		return fallback
	}
	return value
}

type sqlitePlannedTable struct {
	table       schema.Table
	columns     []string
	pagination  PaginationPlan
	maxRowBytes int64
}

func planSQLiteTransferTables(
	ctx context.Context,
	source *sql.DB,
	names []string,
	settings sqliteEffectiveTransferSettings,
) ([]sqlitePlannedTable, error) {
	plans := make([]sqlitePlannedTable, 0, len(names))
	for _, name := range names {
		table, columns, err := inspectTable(ctx, source, name)
		if err != nil {
			return nil, err
		}
		if !hasPrimaryKey(table) {
			return nil, NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf("table %s has no primary key; deterministic transfer requires a primary key", name),
			)
		}
		if err := requireSQLiteReplaySafePrimaryKey(table); err != nil {
			return nil, err
		}
		maxRowBytes, err := sqliteMaximumRowReservation(ctx, source, table.Name, columns)
		if err != nil {
			return nil, fmt.Errorf("plan SQLite memory admission for %s: %w", name, err)
		}
		if maxRowBytes > settings.memory {
			return nil, NewTransferError(ErrorClassPolicy, fmt.Errorf(
				"table %s row reservation %d exceeds memory budget %d: %w",
				name, maxRowBytes, settings.memory, ErrByteRequestExceedsBudget,
			))
		}
		pagination, err := PlanSQLitePagination(ctx, source, table, settings.partitions)
		if err != nil {
			return nil, fmt.Errorf("plan SQLite pagination for %s: %w", name, err)
		}
		plans = append(plans, sqlitePlannedTable{
			table:       table,
			columns:     append([]string(nil), columns...),
			pagination:  pagination,
			maxRowBytes: maxRowBytes,
		})
	}
	return plans, nil
}

func requireSQLiteReplaySafePrimaryKey(table schema.Table) error {
	keys := make([]schema.Column, 0)
	for _, column := range table.Columns {
		if column.PrimaryKey {
			keys = append(keys, column)
		}
	}
	if table.SQLiteWithoutRowID ||
		len(keys) == 1 && keys[0].DeclaredType != nil && keys[0].DeclaredType.Base == "integer" {
		return nil
	}
	for _, column := range keys {
		if column.Nullable {
			return NewTransferError(
				ErrorClassPrimaryKey,
				fmt.Errorf(
					"table %s primary key column %s is nullable; SQLite cannot prove deterministic duplicate-safe replay",
					table.Name,
					column.Name,
				),
			)
		}
	}
	return nil
}

func sqliteMaximumRowReservation(
	ctx context.Context,
	source *sql.DB,
	table string,
	columns []string,
) (int64, error) {
	base := int64(unsafe.Sizeof([]any{})) + int64(len(columns))*int64(unsafe.Sizeof(any(nil)))
	terms := make([]string, len(columns))
	for index, column := range columns {
		quoted := quote(column)
		// Reserve for both SQLite's current value and DMTX's owned copy before
		// Rows.Next materializes it. The fixed allowance dominates every scalar
		// representation accepted by sqliteRetainedRowBytes.
		terms[index] = "CASE WHEN " + quoted + " IS NULL THEN 0 ELSE 64 + " +
			"2 * COALESCE(length(CAST(" + quoted + " AS BLOB)), 0) END"
	}
	expression := fmt.Sprintf("%d", base)
	if len(terms) > 0 {
		expression += " + " + strings.Join(terms, " + ")
	}
	var maximum sql.NullInt64
	if err := source.QueryRowContext(
		ctx,
		"SELECT MAX("+expression+") FROM "+quote(table),
	).Scan(&maximum); err != nil {
		return 0, fmt.Errorf("measure retained rows for %s: %w", table, err)
	}
	if !maximum.Valid {
		return base + int64(len(columns))*64, nil
	}
	return maximum.Int64, nil
}

func notifySQLiteTransferPlans(
	ctx context.Context,
	observer TableObserver,
	plans []sqlitePlannedTable,
	settings sqliteEffectiveTransferSettings,
	completed map[string]int,
	progress map[string]TableProgress,
	resume bool,
) error {
	planObserver, ok := observer.(SQLiteTransferPlanObserver)
	if !ok {
		return nil
	}
	for _, planned := range plans {
		_, complete := completed[planned.table.Name]
		if resume && complete {
			continue
		}
		legacy := progress[planned.table.Name]
		if resume &&
			settings.targetMode == "upsert" &&
			hasLegacyTableProgress(legacy) {
			continue
		}
		publicPlan := publicSQLiteTransferPlan(planned, settings)
		if err := planObserver.AfterSQLiteTransferPlan(ctx, publicPlan); err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("observe SQLite transfer plan for %s: %w", planned.table.Name, err),
			)
		}
	}
	return nil
}

func clonePaginationPlan(plan PaginationPlan) PaginationPlan {
	cloned := plan
	cloned.Keys = append([]KeySpec(nil), plan.Keys...)
	cloned.Ranges = make([]PaginationRange, len(plan.Ranges))
	for index, transferRange := range plan.Ranges {
		cloned.Ranges[index] = clonePaginationRange(transferRange)
	}
	return cloned
}

func clonePaginationRange(transferRange PaginationRange) PaginationRange {
	cloned := transferRange
	cloned.Lower = cloneKeyTuplePointer(transferRange.Lower)
	cloned.Upper = cloneKeyTuplePointer(transferRange.Upper)
	return cloned
}

func cloneKeyTuplePointer(tuple *KeyTuple) *KeyTuple {
	if tuple == nil {
		return nil
	}
	cloned := append(KeyTuple(nil), (*tuple)...)
	return &cloned
}

func publicSQLiteTransferPlan(
	planned sqlitePlannedTable,
	settings sqliteEffectiveTransferSettings,
) SQLiteTransferPlan {
	return SQLiteTransferPlan{
		Table:        planned.table.Name,
		Pagination:   clonePaginationPlan(planned.pagination),
		ChunkRows:    settings.chunkRows,
		Partitions:   settings.partitions,
		Readers:      settings.readers,
		Writers:      1,
		QueueDepth:   settings.queueDepth,
		MemoryBudget: settings.memory,
		Resources:    settings.resources,
	}
}

func restoreSQLiteRanges(
	ctx context.Context,
	observer TableObserver,
	planned sqlitePlannedTable,
	settings sqliteEffectiveTransferSettings,
) (map[int]SQLiteRangeRestore, error) {
	provider, ok := observer.(SQLiteRangeRestoreProvider)
	if !ok {
		return nil, nil
	}
	restored, err := provider.RestoreSQLiteRanges(
		ctx,
		publicSQLiteTransferPlan(planned, settings),
	)
	if err != nil {
		return nil, NewTransferError(
			ErrorClassState,
			fmt.Errorf("restore SQLite ranges for %s: %w", planned.table.Name, err),
		)
	}
	byRange := make(map[int]SQLiteRangeRestore, len(restored))
	plannedRanges := make(map[int]PaginationRange, len(planned.pagination.Ranges))
	for _, transferRange := range planned.pagination.Ranges {
		plannedRanges[transferRange.ID] = transferRange
	}
	for _, saved := range restored {
		if saved.Table != "" && saved.Table != planned.table.Name {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"restored SQLite range table %q does not match %q",
				saved.Table, planned.table.Name,
			))
		}
		if saved.TopologyHash != planned.pagination.TopologyHash {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"restored SQLite topology for %s changed from %q to %q",
				planned.table.Name, saved.TopologyHash, planned.pagination.TopologyHash,
			))
		}
		expected, exists := plannedRanges[saved.Range.ID]
		if !exists || !reflect.DeepEqual(saved.Range, expected) {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"restored SQLite range %d bounds do not match the current plan",
				saved.Range.ID,
			))
		}
		if _, duplicate := byRange[saved.Range.ID]; duplicate {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"duplicate restored SQLite range %d", saved.Range.ID,
			))
		}
		if saved.RowsDone < 0 || saved.SequenceOffset != 0 {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"invalid restored SQLite frontier for range %d: rows=%d offset=%d",
				saved.Range.ID, saved.RowsDone, saved.SequenceOffset,
			))
		}
		if planned.pagination.Strategy == PaginationRowNumber {
			minimum := saved.Range.FirstRow - 1
			if saved.Range.Empty {
				minimum = 0
			}
			pristine := !saved.Complete && saved.NextSequence == 0 &&
				saved.RowsDone == 0 && saved.Watermark == nil && saved.Issued == nil
			if pristine && saved.RowNumberWatermark == 0 {
				// A topology reset persists numeric progress fields as zero. For
				// later ROW_NUMBER ranges, zero means unstarted, not row zero.
				saved.RowNumberWatermark = minimum
			}
			if saved.RowNumberWatermark < minimum ||
				!saved.Range.Empty && saved.RowNumberWatermark > saved.Range.LastRow {
				return nil, NewTransferError(ErrorClassState, fmt.Errorf(
					"invalid ROW_NUMBER restore watermark %d for range %d",
					saved.RowNumberWatermark, saved.Range.ID,
				))
			}
		} else if saved.RowsDone > 0 && saved.Watermark == nil {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"restored keyset range %d is missing its typed watermark",
				saved.Range.ID,
			))
		}
		if saved.Complete && saved.Issued != nil {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"completed SQLite range %d retains an issued chunk", saved.Range.ID,
			))
		}
		if saved.Issued != nil {
			issued := cloneSQLiteRangeChunk(*saved.Issued)
			if issued.Table != planned.table.Name ||
				issued.TopologyHash != planned.pagination.TopologyHash ||
				issued.Range.ID != saved.Range.ID ||
				!reflect.DeepEqual(issued.Range, saved.Range) ||
				issued.Sequence != saved.NextSequence || issued.ChunkRows <= 0 {
				return nil, NewTransferError(ErrorClassState, fmt.Errorf(
					"issued SQLite chunk does not match restored range %d frontier",
					saved.Range.ID,
				))
			}
			issued.Replay = true
			saved.Issued = &issued
		}
		saved.Table = planned.table.Name
		saved.Range = clonePaginationRange(saved.Range)
		saved.Watermark = cloneKeyTuplePointer(saved.Watermark)
		byRange[saved.Range.ID] = saved
	}
	return byRange, nil
}

func hasLegacyTableProgress(progress TableProgress) bool {
	return progress.RowsDone != 0 ||
		progress.IntegerWatermark != nil ||
		progress.RowNumberWatermark != nil
}

func validateSQLiteLegacyProgress(
	plans []sqlitePlannedTable,
	progress map[string]TableProgress,
	settings sqliteEffectiveTransferSettings,
	resume bool,
) error {
	if !resume || settings.targetMode != "upsert" {
		return nil
	}
	for _, planned := range plans {
		legacy := progress[planned.table.Name]
		if !hasLegacyTableProgress(legacy) {
			continue
		}
		ambiguous := func(reason string) error {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w for SQLite table %q: %s",
					state.ErrAmbiguousLegacy,
					planned.table.Name,
					reason,
				),
			)
		}
		if settings.partitions != 1 {
			return ambiguous("a legacy frontier can resume only through one compatible range")
		}
		if legacy.RowsDone <= 0 {
			return ambiguous("a saved watermark requires a positive durable row count")
		}
		if legacy.IntegerWatermark != nil &&
			legacy.RowNumberWatermark != nil {
			return ambiguous("integer and ROW_NUMBER watermarks are both present")
		}
		switch planned.pagination.Strategy {
		case PaginationIntegerKeyset:
			if legacy.IntegerWatermark == nil ||
				legacy.RowNumberWatermark != nil {
				return ambiguous("the saved frontier does not match integer-keyset pagination")
			}
		case PaginationRowNumber:
			if legacy.RowNumberWatermark == nil ||
				legacy.IntegerWatermark != nil {
				return ambiguous("the saved frontier does not match ROW_NUMBER pagination")
			}
			if *legacy.RowNumberWatermark != int64(legacy.RowsDone) {
				return ambiguous("ROW_NUMBER watermark and durable row count disagree")
			}
		case PaginationTupleKeyset:
			return ambiguous("the legacy format cannot represent a typed tuple frontier")
		default:
			return ambiguous("the current pagination strategy is unknown")
		}
	}
	return nil
}

func runSQLiteToSQLite(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	progress map[string]TableProgress,
	observer TableObserver,
	resume bool,
) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("SQLite first pass requires source.type and target.type to be sqlite")
	}
	if cfg.Source.Database == "" || cfg.Target.Database == "" {
		return Result{}, fmt.Errorf("SQLite source and target database paths are required")
	}
	if config.SameEndpoint(cfg.Source, cfg.Target) {
		return Result{}, fmt.Errorf("source and target SQLite databases must differ")
	}
	if cfg.Migration.StrictConsistency {
		scope := cfg.Migration.StrictConsistencyScope
		if scope == "" {
			scope = "table"
		}
		return Result{}, NewTransferError(
			ErrorClassPolicy,
			&schema.PolicyError{
				Operation: "enable strict consistency", Type: scope, Target: string(schema.SQLite),
			},
		)
	}

	settings, err := effectiveSQLiteTransferSettings(ctx, cfg.Migration, nil)
	if err != nil {
		return Result{}, err
	}
	if settings.targetMode != "drop_recreate" && settings.targetMode != "upsert" {
		return Result{}, fmt.Errorf("invalid target_mode %q", settings.targetMode)
	}
	cfg.Migration.TargetMode = settings.targetMode

	source, err := sql.Open("sqlite", cfg.Source.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open source: %w", err)
	}
	defer source.Close()
	source.SetMaxOpenConns(settings.readers)
	source.SetMaxIdleConns(settings.readers)

	target, err := sql.Open("sqlite", cfg.Target.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open target: %w", err)
	}
	defer target.Close()
	target.SetMaxOpenConns(1)
	target.SetMaxIdleConns(1)

	names, err := userTables(ctx, source)
	if err != nil {
		return Result{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Result{}, err
	}
	plans, err := planSQLiteTransferTables(ctx, source, names, settings)
	if err != nil {
		return Result{}, err
	}
	if err := validateSQLiteLegacyProgress(plans, progress, settings, resume); err != nil {
		return Result{}, err
	}

	if err := requireSQLiteDestructiveAcknowledgement(ctx, target, names, cfg.Migration); err != nil {
		return Result{}, err
	}
	if err := validateSQLiteSchemaBeforeMutation(ctx, source, target, names, settings.targetMode); err != nil {
		return Result{}, err
	}
	validatedCompleted, err := validateCompletedSQLiteTableCheckpoints(
		ctx, source, target, names, completed, resume,
	)
	if err != nil {
		return Result{}, err
	}
	if setObserver, ok := observer.(TableSetObserver); ok {
		tables := append([]string(nil), names...)
		if err := setObserver.BeforeTables(ctx, tables); err != nil {
			return Result{}, fmt.Errorf("checkpoint table set: %w", err)
		}
		if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryTableSetCheckpoint, ""); err != nil {
			return Result{}, err
		}
	}
	if err := notifySQLiteTransferPlans(
		ctx, observer, plans, settings, validatedCompleted, progress, resume,
	); err != nil {
		return Result{}, err
	}

	budget, err := NewByteBudget(settings.memory)
	if err != nil {
		return Result{}, err
	}
	result := Result{}
	for _, planned := range plans {
		name := planned.table.Name
		if rows, complete := validatedCompleted[name]; resume && complete {
			result.Tables++
			result.Rows += rows
			continue
		}
		if observer != nil {
			if err := observer.BeforeTable(ctx, name); err != nil {
				return result, fmt.Errorf("checkpoint before %s: %w", name, err)
			}
		}

		tableProgress := progress[name]
		hasLegacyProgress := hasLegacyTableProgress(tableProgress)
		var restored map[int]SQLiteRangeRestore
		if !(resume && settings.targetMode == "upsert" && hasLegacyProgress) {
			restored, err = restoreSQLiteRanges(ctx, observer, planned, settings)
			if err != nil {
				return result, err
			}
		}
		var copied int
		if resume && settings.targetMode == "upsert" && hasLegacyProgress {
			copied, err = copyTable(
				ctx, source, target, name, settings.targetMode, observer, tableProgress, true,
			)
		} else {
			copied, err = copySQLitePlannedTable(
				ctx, source, target, planned, settings, budget, observer, resume, restored,
			)
			tableProgress = TableProgress{}
		}
		if err != nil {
			result.Rows += copied
			return result, err
		}
		rowsDone := tableProgress.RowsDone + copied
		result.Rows += rowsDone
		if err := validateCount(ctx, source, target, name, settings.targetMode); err != nil {
			return result, err
		}
		if observer != nil {
			if err := observer.AfterTable(ctx, name, rowsDone); err != nil {
				return result, fmt.Errorf("checkpoint after %s: %w", name, err)
			}
		}
		result.Tables++
	}
	if stats := budget.Stats(); stats.Current != 0 {
		return result, fmt.Errorf("SQLite transfer leaked %d admitted bytes", stats.Current)
	}
	result.Validated = true
	return result, nil
}

func copySQLitePlannedTable(
	ctx context.Context,
	source, target *sql.DB,
	planned sqlitePlannedTable,
	settings sqliteEffectiveTransferSettings,
	budget *ByteBudget,
	observer TableObserver,
	resumeExisting bool,
	restored map[int]SQLiteRangeRestore,
) (int, error) {
	prepareMode := settings.targetMode
	hasRestoredProgress := resumeExisting && sqliteRestoredRangesHaveProgress(restored)
	if hasRestoredProgress {
		exists, err := tableExists(ctx, target, planned.table.Name)
		if err != nil {
			return 0, fmt.Errorf("inspect resumable target %s: %w", planned.table.Name, err)
		}
		if !exists {
			return 0, NewTransferError(ErrorClassState, fmt.Errorf(
				"resumable target table %s is missing; reset its range state before rebuilding",
				planned.table.Name,
			))
		}
		matches, err := sqliteTableMatchesPlannedDefinition(ctx, target, planned.table)
		if err != nil {
			return 0, err
		}
		if !matches {
			return 0, NewTransferError(ErrorClassState, fmt.Errorf(
				"resumable target table %s no longer matches its planned definition",
				planned.table.Name,
			))
		}
	}
	if hasRestoredProgress && prepareMode == "drop_recreate" {
		prepareMode = "upsert"
	}
	created, err := prepareTargetWithStatus(ctx, target, planned.table, prepareMode, observer)
	if err != nil {
		return 0, err
	}
	finalizeExisting := false
	if !created && resumeExisting {
		finalizeExisting, err = sqliteTableMatchesPlannedDefinition(ctx, target, planned.table)
		if err != nil {
			return 0, err
		}
	}
	copied, err := executeSQLiteTransferPlan(
		ctx, source, target, planned, settings, budget, observer, restored,
	)
	if err != nil {
		return copied, err
	}
	if created || finalizeExisting {
		if err := finalizeSQLiteTarget(ctx, target, planned.table, observer); err != nil {
			return copied, err
		}
	}
	return copied, nil
}

func sqliteRestoredRangesHaveProgress(restored map[int]SQLiteRangeRestore) bool {
	for _, saved := range restored {
		if saved.Complete || saved.RowsDone > 0 || saved.NextSequence > 0 ||
			saved.Watermark != nil || saved.Issued != nil {
			return true
		}
	}
	return false
}

type sqliteRangeJob struct {
	index         int
	transferRange PaginationRange
	restore       *SQLiteRangeRestore
}

type sqliteBufferedChunk struct {
	rangeIndex   int
	sequence     uint64
	rows         [][]any
	reservations []*ByteReservation
	end          *KeyTuple
	endRow       int64
	replay       bool
}

func (chunk *sqliteBufferedChunk) release() {
	if chunk == nil {
		return
	}
	for _, reservation := range chunk.reservations {
		reservation.Release()
	}
	chunk.reservations = nil
}

type sqliteAcknowledgementTask struct {
	chunk   SQLiteRangeChunk
	receipt WriteReceipt
	done    chan struct{}
}

type sqliteRangeAckState struct {
	transferRange PaginationRange
	tracker       *ContiguousAckTracker
	pending       map[uint64]SQLiteRangeChunk
	nextNotified  uint64
	notifiedRows  int64
	baseRows      int64
	complete      bool
}

func executeSQLiteTransferPlan(
	ctx context.Context,
	source, target *sql.DB,
	planned sqlitePlannedTable,
	settings sqliteEffectiveTransferSettings,
	budget *ByteBudget,
	observer TableObserver,
	restored map[int]SQLiteRangeRestore,
) (int, error) {
	if len(planned.pagination.Ranges) == 0 {
		return 0, nil
	}
	pipelineCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var failureMu sync.Mutex
	var firstFailure error
	fail := func(err error) {
		if err == nil {
			return
		}
		failureMu.Lock()
		if firstFailure == nil {
			firstFailure = err
			cancel()
		}
		failureMu.Unlock()
	}
	getFailure := func() error {
		failureMu.Lock()
		defer failureMu.Unlock()
		return firstFailure
	}

	rangeStates := make(map[int]*sqliteRangeAckState, len(planned.pagination.Ranges))
	for _, transferRange := range planned.pagination.Ranges {
		if _, exists := rangeStates[transferRange.ID]; exists {
			return 0, fmt.Errorf("duplicate SQLite range ID %d", transferRange.ID)
		}
		restoredRange := restored[transferRange.ID]
		rangeStates[transferRange.ID] = &sqliteRangeAckState{
			transferRange: clonePaginationRange(transferRange),
			tracker: NewContiguousAckTracker(
				fmt.Sprintf("%s/%d", planned.table.Name, transferRange.ID),
				restoredRange.NextSequence,
			),
			pending:      make(map[uint64]SQLiteRangeChunk),
			nextNotified: restoredRange.NextSequence,
			notifiedRows: restoredRange.RowsDone,
			baseRows:     restoredRange.RowsDone,
			complete:     restoredRange.Complete,
		}
	}

	rangeJobs := make(chan sqliteRangeJob)
	chunks := make(chan *sqliteBufferedChunk, settings.queueDepth)
	acknowledgements := make(chan sqliteAcknowledgementTask, settings.queueDepth)
	readyAcknowledgements := make(chan sqliteAcknowledgementTask, settings.queueDepth)

	go func() {
		defer close(rangeJobs)
		for index, transferRange := range planned.pagination.Ranges {
			var restoredRange *SQLiteRangeRestore
			if saved, ok := restored[transferRange.ID]; ok {
				copyOfSaved := saved
				restoredRange = &copyOfSaved
			}
			if restoredRange != nil && restoredRange.Complete {
				continue
			}
			select {
			case <-pipelineCtx.Done():
				return
			case rangeJobs <- sqliteRangeJob{
				index: index, transferRange: transferRange, restore: restoredRange,
			}:
			}
		}
	}()

	readerCount := settings.readers
	if readerCount > len(planned.pagination.Ranges) {
		readerCount = len(planned.pagination.Ranges)
	}
	if readerCount < 1 {
		readerCount = 1
	}
	var readers sync.WaitGroup
	readers.Add(readerCount)
	for reader := 0; reader < readerCount; reader++ {
		go func() {
			defer readers.Done()
			for job := range rangeJobs {
				if err := readSQLiteRange(
					pipelineCtx,
					source,
					planned,
					job,
					settings.chunkRows,
					sqliteRetryPolicy(settings.maxRetries),
					budget,
					observer,
					chunks,
				); err != nil && pipelineCtx.Err() == nil {
					fail(err)
				}
			}
		}()
	}
	go func() {
		readers.Wait()
		close(chunks)
	}()

	var observerMu sync.Mutex
	go func() {
		defer close(acknowledgements)
		for chunk := range chunks {
			if pipelineCtx.Err() != nil {
				chunk.release()
				continue
			}
			transferRange := planned.pagination.Ranges[chunk.rangeIndex]
			issued := SQLiteRangeChunk{
				Table:        planned.table.Name,
				TopologyHash: planned.pagination.TopologyHash,
				Range:        clonePaginationRange(transferRange),
				Sequence:     chunk.sequence,
				ChunkRows:    len(chunk.rows),
				End:          cloneKeyTuplePointer(chunk.end),
				EndRow:       chunk.endRow,
				Replay:       chunk.replay,
			}
			if rangeObserver, ok := observer.(SQLiteRangeChunkObserver); ok && !chunk.replay {
				observerMu.Lock()
				err := rangeObserver.BeforeSQLiteRangeChunk(pipelineCtx, cloneSQLiteRangeChunk(issued))
				observerMu.Unlock()
				if err != nil {
					chunk.release()
					fail(NewTransferError(ErrorClassState, fmt.Errorf(
						"record issued SQLite chunk %s range %d sequence %d: %w",
						planned.table.Name, transferRange.ID, chunk.sequence, err,
					)))
					continue
				}
			}

			writeMode := settings.targetMode
			if chunk.replay {
				writeMode = sqliteInsertOnlyReplayMode
			}
			receipt, err := writeSQLiteRangeBatchReceiptWithPolicy(
				pipelineCtx,
				target,
				planned.table,
				planned.columns,
				writeMode,
				chunk.rows,
				issued,
				observer,
				&observerMu,
				sqliteRetryPolicy(settings.maxRetries),
			)
			chunk.release()
			if err != nil {
				fail(err)
				continue
			}
			task := sqliteAcknowledgementTask{
				chunk: issued, receipt: receipt, done: make(chan struct{}),
			}
			select {
			case <-pipelineCtx.Done():
			case acknowledgements <- task:
				// Do not persist the next issued intent until this receipt and
				// its contiguous frontier are durable. This leaves at most one
				// issued-but-unacknowledged SQLite chunk after a crash.
				select {
				case <-pipelineCtx.Done():
				case <-task.done:
				}
			}
		}
	}()

	// SQLite has one writer and deliberately persists its durable receipts in
	// sequence. This makes a restored issued intent the only unacknowledged
	// durable ambiguity; no out-of-order pending receipt state can be created.
	const acknowledgerCount = 1
	var acknowledgers sync.WaitGroup
	acknowledgers.Add(acknowledgerCount)
	for worker := 0; worker < acknowledgerCount; worker++ {
		go func() {
			defer acknowledgers.Done()
			for task := range acknowledgements {
				if pipelineCtx.Err() != nil {
					close(task.done)
					continue
				}
				if hook, ok := observer.(SQLiteChunkAcknowledgeObserver); ok {
					info := SQLiteChunkInfo{
						Table:    task.chunk.Table,
						RangeID:  task.chunk.Range.ID,
						Sequence: task.chunk.Sequence,
						Rows:     task.chunk.ChunkRows,
					}
					if err := hook.BeforeSQLiteChunkAcknowledge(pipelineCtx, info, task.receipt); err != nil {
						close(task.done)
						fail(fmt.Errorf("observe SQLite chunk acknowledgement: %w", err))
						continue
					}
				}
				select {
				case <-pipelineCtx.Done():
					close(task.done)
				case readyAcknowledgements <- task:
				}
			}
		}()
	}
	go func() {
		acknowledgers.Wait()
		close(readyAcknowledgements)
	}()

	for task := range readyAcknowledgements {
		func() {
			defer close(task.done)
			if pipelineCtx.Err() != nil {
				return
			}
			state := rangeStates[task.chunk.Range.ID]
			if state == nil {
				fail(fmt.Errorf("unknown SQLite range ID %d", task.chunk.Range.ID))
				return
			}
			state.pending[task.chunk.Sequence] = cloneSQLiteRangeChunk(task.chunk)
			frontier, err := state.tracker.Acknowledge(
				task.chunk.Sequence,
				int64(task.chunk.ChunkRows),
				task.receipt,
			)
			if err != nil {
				fail(err)
				return
			}
			absoluteFrontier := frontier
			absoluteFrontier.Rows += state.baseRows
			if rangeObserver, ok := observer.(SQLiteRangeChunkObserver); ok {
				observerMu.Lock()
				err = rangeObserver.AfterSQLiteRangeChunk(
					pipelineCtx,
					cloneSQLiteRangeChunk(task.chunk),
					task.receipt,
					absoluteFrontier,
				)
				observerMu.Unlock()
				if err != nil {
					fail(NewTransferError(ErrorClassState, fmt.Errorf(
						"acknowledge SQLite chunk %s range %d sequence %d: %w",
						task.chunk.Table, task.chunk.Range.ID, task.chunk.Sequence, err,
					)))
					return
				}
			}
			for state.nextNotified < frontier.NextSequence {
				checkpoint, ok := state.pending[state.nextNotified]
				if !ok {
					fail(fmt.Errorf(
						"SQLite acknowledgement frontier skipped missing sequence %d",
						state.nextNotified,
					))
					break
				}
				state.notifiedRows += int64(checkpoint.ChunkRows)
				safeFrontier := AckFrontier{
					RangeID:      frontier.RangeID,
					NextSequence: state.nextNotified + 1,
					Rows:         state.notifiedRows,
				}
				if err := notifySQLiteContiguousRangeProgress(
					pipelineCtx, observer, &observerMu, planned, checkpoint,
					safeFrontier, budget.Stats(),
				); err != nil {
					fail(err)
					break
				}
				delete(state.pending, state.nextNotified)
				state.nextNotified++
			}
		}()
	}

	if err := getFailure(); err != nil {
		return sqliteSafeRowCount(rangeStates), err
	}
	if err := ctx.Err(); err != nil {
		return sqliteSafeRowCount(rangeStates), err
	}
	for _, state := range rangeStates {
		if len(state.pending) != 0 {
			return sqliteSafeRowCount(rangeStates), fmt.Errorf(
				"SQLite range %d retained uncheckpointed receipts", state.transferRange.ID,
			)
		}
		if state.complete {
			continue
		}
		frontier := state.tracker.Frontier()
		frontier.Rows += state.baseRows
		if err := notifySQLiteRangeCompletion(
			ctx, observer, &observerMu, planned, state.transferRange, frontier, budget.Stats(),
		); err != nil {
			return sqliteSafeRowCount(rangeStates), err
		}
	}
	total := int64(sqliteSafeRowCount(rangeStates))
	if total > int64(math.MaxInt) {
		return 0, fmt.Errorf("SQLite transferred row count exceeds int range")
	}
	return int(total), nil
}

func sqliteSafeRowCount(states map[int]*sqliteRangeAckState) int {
	var total int64
	for _, state := range states {
		total += state.notifiedRows
	}
	if total > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(total)
}

func cloneSQLiteRangeChunk(chunk SQLiteRangeChunk) SQLiteRangeChunk {
	chunk.Range = clonePaginationRange(chunk.Range)
	chunk.End = cloneKeyTuplePointer(chunk.End)
	return chunk
}

func readSQLiteRange(
	ctx context.Context,
	source *sql.DB,
	planned sqlitePlannedTable,
	job sqliteRangeJob,
	chunkRows int,
	retryPolicy RetryPolicy,
	budget *ByteBudget,
	observer TableObserver,
	output chan<- *sqliteBufferedChunk,
) error {
	if job.transferRange.Empty {
		return nil
	}
	sequence := uint64(0)
	lower := cloneKeyTuplePointer(job.transferRange.Lower)
	rowCursor := job.transferRange.FirstRow - 1
	if job.restore != nil {
		sequence = job.restore.NextSequence
		if job.restore.Watermark != nil {
			lower = cloneKeyTuplePointer(job.restore.Watermark)
		}
		if planned.pagination.Strategy == PaginationRowNumber {
			rowCursor = job.restore.RowNumberWatermark
		}
		if job.restore.Issued != nil {
			replayed, err := readSQLiteIssuedChunk(
				ctx, source, planned, job, lower, rowCursor, retryPolicy, budget, observer, output,
			)
			if err != nil {
				return err
			}
			sequence++
			if replayed.end != nil {
				lower = cloneKeyTuplePointer(replayed.end)
			}
			if planned.pagination.Strategy == PaginationRowNumber {
				rowCursor = replayed.endRow
			}
		}
	}

	if planned.pagination.Strategy == PaginationRowNumber && rowCursor >= job.transferRange.LastRow {
		return nil
	}

	for {
		effectiveChunkRows, err := budget.adjustedChunkRows(ctx, chunkRows)
		if err != nil {
			return fmt.Errorf("apply heap-pressure backstop for %s range %d: %w",
				planned.table.Name, job.transferRange.ID, err)
		}
		query, arguments, err := sqliteRangePageQuery(
			planned,
			job.transferRange,
			lower,
			rowCursor,
			effectiveChunkRows,
		)
		if err != nil {
			return err
		}
		rows, err := querySQLiteRowsWithRetry(
			ctx, source, retryPolicy, query, arguments...,
		)
		if err != nil {
			return fmt.Errorf("read %s range %d: %w", planned.table.Name, job.transferRange.ID, err)
		}
		scanned, lastKey, nextRow, err := scanSQLiteRangePage(
			ctx,
			rows,
			planned,
			job,
			&sequence,
			rowCursor,
			effectiveChunkRows,
			budget,
			observer,
			output,
		)
		closeErr := rows.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return fmt.Errorf("close %s range %d rows: %w", planned.table.Name, job.transferRange.ID, closeErr)
		}
		if scanned == 0 {
			return nil
		}
		if lastKey != nil {
			lower = cloneKeyTuplePointer(lastKey)
		}
		rowCursor = nextRow
		if planned.pagination.Strategy == PaginationRowNumber && rowCursor >= job.transferRange.LastRow {
			return nil
		}
		if scanned < effectiveChunkRows {
			return nil
		}
	}
}

type sqliteOwnedValueScanner struct {
	value *any
}

func (scanner sqliteOwnedValueScanner) Scan(value any) error {
	*scanner.value = cloneSQLiteValue(value)
	return nil
}

func scanSQLiteRangePage(
	ctx context.Context,
	rows *sql.Rows,
	planned sqlitePlannedTable,
	job sqliteRangeJob,
	nextSequence *uint64,
	rowCursor int64,
	chunkRows int,
	budget *ByteBudget,
	observer TableObserver,
	output chan<- *sqliteBufferedChunk,
) (int, *KeyTuple, int64, error) {
	chunk := &sqliteBufferedChunk{rangeIndex: job.index, sequence: *nextSequence}
	var scanned int
	var lastKey *KeyTuple
	releaseCurrent := func() {
		chunk.release()
	}
	for {
		reservation, admitted, err := budget.TryAcquire(ctx, planned.maxRowBytes)
		if err != nil {
			releaseCurrent()
			return 0, nil, rowCursor, err
		}
		if !admitted && len(chunk.rows) > 0 {
			if err := emitSQLiteBufferedChunk(ctx, planned.table.Name, job.transferRange.ID, observer, output, chunk); err != nil {
				return 0, nil, rowCursor, err
			}
			*nextSequence = *nextSequence + 1
			chunk = &sqliteBufferedChunk{rangeIndex: job.index, sequence: *nextSequence}
		}
		if !admitted {
			reservation, err = budget.Acquire(ctx, planned.maxRowBytes)
			if err != nil {
				releaseCurrent()
				return 0, nil, rowCursor, err
			}
		}
		// Capacity is owned before Rows.Next can materialize the next payload.
		if !rows.Next() {
			reservation.Release()
			break
		}
		values := make([]any, len(planned.columns))
		scanners := make([]any, len(values))
		for index := range scanners {
			scanners[index] = sqliteOwnedValueScanner{value: &values[index]}
		}
		if err := rows.Scan(scanners...); err != nil {
			reservation.Release()
			releaseCurrent()
			return 0, nil, rowCursor, fmt.Errorf("scan %s range %d: %w", planned.table.Name, job.transferRange.ID, err)
		}
		rowBytes, err := sqliteRetainedRowBytes(values)
		if err != nil {
			reservation.Release()
			releaseCurrent()
			return 0, nil, rowCursor, NewTransferError(ErrorClassConversion, err)
		}
		if rowBytes > reservation.Bytes() {
			reservation.Release()
			releaseCurrent()
			return 0, nil, rowCursor, NewTransferError(ErrorClassState, fmt.Errorf(
				"SQLite row in %s exceeded its planned reservation: row=%d reservation=%d",
				planned.table.Name, rowBytes, planned.maxRowBytes,
			))
		}
		chunk.rows = append(chunk.rows, values)
		chunk.reservations = append(chunk.reservations, reservation)
		scanned++
		if planned.pagination.Strategy == PaginationRowNumber {
			rowCursor++
			chunk.endRow = rowCursor
		} else {
			tuple, keyErr := sqliteKeyTupleFromRow(values, planned.columns, planned.pagination.Keys)
			if keyErr != nil {
				releaseCurrent()
				return 0, nil, rowCursor, NewTransferError(ErrorClassConversion, keyErr)
			}
			lastKey = &tuple
			chunk.end = cloneKeyTuplePointer(lastKey)
		}
		if len(chunk.rows) == chunkRows {
			if err := emitSQLiteBufferedChunk(ctx, planned.table.Name, job.transferRange.ID, observer, output, chunk); err != nil {
				return 0, nil, rowCursor, err
			}
			*nextSequence = *nextSequence + 1
			chunk = &sqliteBufferedChunk{rangeIndex: job.index, sequence: *nextSequence}
		}
	}
	if err := rows.Err(); err != nil {
		releaseCurrent()
		return 0, nil, rowCursor, fmt.Errorf("iterate %s range %d: %w", planned.table.Name, job.transferRange.ID, err)
	}
	if len(chunk.rows) > 0 {
		if err := emitSQLiteBufferedChunk(ctx, planned.table.Name, job.transferRange.ID, observer, output, chunk); err != nil {
			return 0, nil, rowCursor, err
		}
		*nextSequence = *nextSequence + 1
	} else {
		chunk.release()
	}
	return scanned, lastKey, rowCursor, nil
}

func readSQLiteIssuedChunk(
	ctx context.Context,
	source *sql.DB,
	planned sqlitePlannedTable,
	job sqliteRangeJob,
	lower *KeyTuple,
	rowCursor int64,
	retryPolicy RetryPolicy,
	budget *ByteBudget,
	observer TableObserver,
	output chan<- *sqliteBufferedChunk,
) (*sqliteBufferedChunk, error) {
	issued := cloneSQLiteRangeChunk(*job.restore.Issued)
	issuedRange := clonePaginationRange(job.transferRange)
	if planned.pagination.Strategy == PaginationRowNumber {
		issuedRange.LastRow = issued.EndRow
	} else {
		issuedRange.Upper = cloneKeyTuplePointer(issued.End)
	}
	query, arguments, err := sqliteRangePageQuery(
		planned, issuedRange, lower, rowCursor, issued.ChunkRows,
	)
	if err != nil {
		return nil, err
	}
	if _, err := budget.adjustedChunkRows(ctx, issued.ChunkRows); err != nil {
		return nil, fmt.Errorf("apply heap-pressure backstop for issued SQLite chunk %s range %d: %w",
			planned.table.Name, issued.Range.ID, err)
	}
	if int64(issued.ChunkRows) > math.MaxInt64/planned.maxRowBytes {
		return nil, fmt.Errorf("issued SQLite chunk %s range %d reservation overflow", planned.table.Name, issued.Range.ID)
	}
	reservationBytes := int64(issued.ChunkRows) * planned.maxRowBytes
	reservation, err := budget.Acquire(ctx, reservationBytes)
	if err != nil {
		return nil, fmt.Errorf("reserve issued SQLite chunk %s range %d: %w", planned.table.Name, issued.Range.ID, err)
	}
	rows, err := querySQLiteRowsWithRetry(
		ctx, source, retryPolicy, query, arguments...,
	)
	if err != nil {
		reservation.Release()
		return nil, fmt.Errorf("read issued %s range %d: %w", planned.table.Name, issued.Range.ID, err)
	}
	defer rows.Close()

	chunk := &sqliteBufferedChunk{
		rangeIndex: job.index,
		sequence:   issued.Sequence,
		replay:     true,
	}
	chunk.reservations = append(chunk.reservations, reservation)
	for rows.Next() {
		values := make([]any, len(planned.columns))
		scanners := make([]any, len(values))
		for index := range scanners {
			scanners[index] = sqliteOwnedValueScanner{value: &values[index]}
		}
		if err := rows.Scan(scanners...); err != nil {
			chunk.release()
			return nil, fmt.Errorf("scan issued %s range %d: %w", planned.table.Name, issued.Range.ID, err)
		}
		rowBytes, err := sqliteRetainedRowBytes(values)
		if err != nil {
			chunk.release()
			return nil, NewTransferError(ErrorClassConversion, err)
		}
		if rowBytes > planned.maxRowBytes {
			chunk.release()
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"issued SQLite row in %s exceeded its planned reservation: row=%d reservation=%d",
				planned.table.Name, rowBytes, planned.maxRowBytes,
			))
		}
		chunk.rows = append(chunk.rows, values)
		if planned.pagination.Strategy == PaginationRowNumber {
			rowCursor++
			chunk.endRow = rowCursor
		} else {
			tuple, keyErr := sqliteKeyTupleFromRow(values, planned.columns, planned.pagination.Keys)
			if keyErr != nil {
				chunk.release()
				return nil, NewTransferError(ErrorClassConversion, keyErr)
			}
			chunk.end = &tuple
		}
	}
	if err := rows.Err(); err != nil {
		chunk.release()
		return nil, fmt.Errorf("iterate issued %s range %d: %w", planned.table.Name, issued.Range.ID, err)
	}
	if len(chunk.rows) != issued.ChunkRows || chunk.endRow != issued.EndRow ||
		!reflect.DeepEqual(chunk.end, issued.End) {
		chunk.release()
		return nil, NewTransferError(ErrorClassState, fmt.Errorf(
			"issued SQLite chunk %s range %d no longer matches its source frontier",
			planned.table.Name, issued.Range.ID,
		))
	}
	if err := emitSQLiteBufferedChunk(ctx, planned.table.Name, issued.Range.ID, observer, output, chunk); err != nil {
		return nil, err
	}
	return chunk, nil
}

func emitSQLiteBufferedChunk(
	ctx context.Context,
	table string,
	rangeID int,
	observer TableObserver,
	output chan<- *sqliteBufferedChunk,
	chunk *sqliteBufferedChunk,
) error {
	info := SQLiteChunkInfo{
		Table: table, RangeID: rangeID, Sequence: chunk.sequence, Rows: len(chunk.rows),
	}
	if hook, ok := observer.(SQLiteChunkReadObserver); ok {
		if err := hook.BeforeSQLiteChunkRead(ctx, info); err != nil {
			chunk.release()
			return fmt.Errorf("observe SQLite chunk read: %w", err)
		}
	}
	select {
	case <-ctx.Done():
		chunk.release()
		return ctx.Err()
	case output <- chunk:
		return nil
	}
}

func sqliteRangePageQuery(
	planned sqlitePlannedTable,
	transferRange PaginationRange,
	lower *KeyTuple,
	rowCursor int64,
	limit int,
) (string, []any, error) {
	if planned.pagination.Strategy == PaginationRowNumber {
		if transferRange.Empty || rowCursor >= transferRange.LastRow {
			return "", nil, fmt.Errorf("ROW_NUMBER range %d has no remaining rows", transferRange.ID)
		}
		remaining := transferRange.LastRow - rowCursor
		pageRows := int64(limit)
		if remaining < pageRows {
			pageRows = remaining
		}
		upper := rowCursor + pageRows
		rowNumberAlias := sqliteRowNumberAlias(planned.columns)
		quotedRowNumberAlias := quote(rowNumberAlias)
		order := quotedColumns(primaryKeyColumns(planned.table))
		query := "SELECT " + quotedColumns(planned.columns) +
			" FROM (SELECT " + quotedColumns(planned.columns) +
			", ROW_NUMBER() OVER (ORDER BY " + order + ") AS " + quotedRowNumberAlias + " FROM " +
			quote(planned.table.Name) +
			") WHERE " + quotedRowNumberAlias + " > ? AND " + quotedRowNumberAlias +
			" <= ? ORDER BY " + quotedRowNumberAlias
		return query, []any{rowCursor, upper}, nil
	}

	conditions := make([]string, 0, 2)
	arguments := make([]any, 0, len(planned.pagination.Keys)*2+1)
	if lower != nil {
		predicate, values, err := sqliteTuplePredicate(planned.pagination.Keys, ">", *lower)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, predicate)
		arguments = append(arguments, values...)
	}
	if transferRange.Upper != nil {
		predicate, values, err := sqliteTuplePredicate(
			planned.pagination.Keys, "<=", *transferRange.Upper,
		)
		if err != nil {
			return "", nil, err
		}
		conditions = append(conditions, predicate)
		arguments = append(arguments, values...)
	}
	query := "SELECT " + quotedColumns(planned.columns) + " FROM " + quote(planned.table.Name)
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	keyNames := make([]string, len(planned.pagination.Keys))
	for index, key := range planned.pagination.Keys {
		keyNames[index] = key.Name
	}
	query += " ORDER BY " + quotedColumns(keyNames) + " LIMIT ?"
	arguments = append(arguments, limit)
	return query, arguments, nil
}

func sqliteRowNumberAlias(columns []string) string {
	const base = "dmtx_row_number"
	existing := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		existing[strings.ToLower(column)] = struct{}{}
	}
	alias := base
	for suffix := 1; ; suffix++ {
		if _, collision := existing[strings.ToLower(alias)]; !collision {
			return alias
		}
		alias = fmt.Sprintf("%s_%d", base, suffix)
	}
}

func sqliteTuplePredicate(keys []KeySpec, operator string, tuple KeyTuple) (string, []any, error) {
	if len(keys) == 0 || len(tuple) != len(keys) {
		return "", nil, fmt.Errorf("SQLite tuple bound width does not match pagination key")
	}
	values := make([]any, len(tuple))
	for index, encoded := range tuple {
		if encoded.Kind != keys[index].Kind {
			return "", nil, fmt.Errorf("SQLite tuple bound kind does not match key %s", keys[index].Name)
		}
		value, err := encoded.SQLValue()
		if err != nil {
			return "", nil, err
		}
		values[index] = value
	}
	if len(keys) == 1 {
		return quote(keys[0].Name) + " " + operator + " ?", values, nil
	}
	names := make([]string, len(keys))
	for index, key := range keys {
		names[index] = key.Name
	}
	return "(" + quotedColumns(names) + ") " + operator + " (" + placeholders(len(keys)) + ")", values, nil
}

func sqliteKeyTupleFromRow(values []any, columns []string, keys []KeySpec) (KeyTuple, error) {
	tuple := make(KeyTuple, len(keys))
	for index, key := range keys {
		position := columnIndex(columns, key.Name)
		if position < 0 {
			return nil, fmt.Errorf("SQLite pagination key %s is not selected", key.Name)
		}
		value, err := sqliteTypedKey(values[position], key.Kind)
		if err != nil {
			return nil, fmt.Errorf("SQLite pagination key %s: %w", key.Name, err)
		}
		tuple[index] = value
	}
	return tuple, nil
}

func sqliteRetainedRowBytes(values []any) (int64, error) {
	size := int64(unsafe.Sizeof([]any{})) + int64(len(values))*int64(unsafe.Sizeof(any(nil)))
	for _, value := range values {
		var retained int64
		switch typed := value.(type) {
		case nil:
		case []byte:
			retained = int64(unsafe.Sizeof([]byte(nil))) + int64(len(typed))
		case string:
			retained = int64(unsafe.Sizeof("")) + int64(len(typed))
		case int64:
			retained = int64(unsafe.Sizeof(typed))
		case int32:
			retained = int64(unsafe.Sizeof(typed))
		case int:
			retained = int64(unsafe.Sizeof(typed))
		case float64:
			retained = int64(unsafe.Sizeof(typed))
		case float32:
			retained = int64(unsafe.Sizeof(typed))
		case bool:
			retained = int64(unsafe.Sizeof(typed))
		case time.Time:
			retained = int64(unsafe.Sizeof(typed))
		default:
			return 0, fmt.Errorf("SQLite row has unsupported retained value type %T", value)
		}
		if retained > math.MaxInt64-size {
			return 0, fmt.Errorf("SQLite retained row byte count overflow")
		}
		size += retained
	}
	return size, nil
}

func notifySQLiteContiguousRangeProgress(
	ctx context.Context,
	observer TableObserver,
	observerMu *sync.Mutex,
	planned sqlitePlannedTable,
	checkpoint SQLiteRangeChunk,
	frontier AckFrontier,
	memory ByteBudgetStats,
) error {
	if observer == nil {
		return nil
	}
	observerMu.Lock()
	defer observerMu.Unlock()
	notified := false
	if rangeObserver, ok := observer.(SQLiteRangeProgressObserver); ok {
		progress := SQLiteRangeProgress{
			Table:              planned.table.Name,
			TopologyHash:       planned.pagination.TopologyHash,
			Range:              clonePaginationRange(checkpoint.Range),
			Frontier:           frontier,
			Watermark:          cloneKeyTuplePointer(checkpoint.End),
			RowNumberWatermark: checkpoint.EndRow,
			Memory:             memory,
		}
		if err := rangeObserver.AfterSQLiteRangeProgress(ctx, progress); err != nil {
			return NewTransferError(ErrorClassState, fmt.Errorf(
				"checkpoint SQLite range %d: %w", checkpoint.Range.ID, err,
			))
		}
		notified = true
	}
	_, topologyAware := observer.(SQLiteRangeChunkObserver)
	if !topologyAware && len(planned.pagination.Ranges) == 1 {
		if pageObserver, ok := observer.(PageObserver); ok {
			if frontier.Rows > int64(math.MaxInt) {
				return fmt.Errorf("SQLite legacy page row count exceeds int range")
			}
			var err error
			switch planned.pagination.Strategy {
			case PaginationIntegerKeyset:
				if checkpoint.End == nil || len(*checkpoint.End) != 1 {
					return fmt.Errorf("SQLite integer checkpoint is missing its typed key")
				}
				value, decodeErr := (*checkpoint.End)[0].SQLValue()
				if decodeErr != nil {
					return decodeErr
				}
				integer, ok := value.(int64)
				if !ok {
					return fmt.Errorf("SQLite integer checkpoint has type %T", value)
				}
				err = pageObserver.AfterIntegerKeysetPage(
					ctx, planned.table.Name, int(frontier.Rows), integer,
				)
			case PaginationRowNumber:
				err = pageObserver.AfterRowNumberPage(
					ctx, planned.table.Name, int(frontier.Rows), checkpoint.EndRow,
				)
			}
			if err != nil {
				return NewTransferError(ErrorClassState, fmt.Errorf(
					"checkpoint page for %s: %w", planned.table.Name, err,
				))
			}
			notified = true
		}
	}
	if notified {
		return notifySQLiteWriteBoundary(
			ctx, observer, SQLiteBoundaryPageCheckpointed, planned.table.Name,
		)
	}
	return nil
}

func notifySQLiteRangeCompletion(
	ctx context.Context,
	observer TableObserver,
	observerMu *sync.Mutex,
	planned sqlitePlannedTable,
	transferRange PaginationRange,
	frontier AckFrontier,
	memory ByteBudgetStats,
) error {
	rangeObserver, ok := observer.(SQLiteRangeProgressObserver)
	if !ok {
		return nil
	}
	observerMu.Lock()
	defer observerMu.Unlock()
	progress := SQLiteRangeProgress{
		Table:                planned.table.Name,
		TopologyHash:         planned.pagination.TopologyHash,
		Range:                clonePaginationRange(transferRange),
		Frontier:             frontier,
		Memory:               memory,
		Complete:             true,
		ExpectedNextSequence: frontier.NextSequence,
	}
	if err := rangeObserver.AfterSQLiteRangeProgress(ctx, progress); err != nil {
		return NewTransferError(ErrorClassState, fmt.Errorf(
			"complete SQLite range %d: %w", transferRange.ID, err,
		))
	}
	return notifySQLiteWriteBoundary(
		ctx, observer, SQLiteBoundaryPageCheckpointed, planned.table.Name,
	)
}
