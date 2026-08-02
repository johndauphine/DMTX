package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// stage4AdapterIncrementalValidationSampleLimit is deliberately the same
// bounded sample cardinality used by the composed Stage 4 validation runner.
// Incremental evidence retains only the lowest complete primary keys in
// memory. Every transferred key is held only in a private, process-local
// spool so final validation can re-read the actual target subset.
const stage4AdapterIncrementalValidationSampleLimit = 100

// stage4AdapterIncrementalTargetEvidenceReader opens one stable target read
// view. It is deliberately narrower than ValidationCoreProbe: the source side
// is already the exact rows transferred by this attempt, while this reader
// must re-query the target for those complete keys after transfer is done.
type stage4AdapterIncrementalTargetEvidenceReader interface {
	ValidateStage4IncrementalValidationTarget(
		schema.Table,
		[]string,
		bool,
	) error
	OpenStage4IncrementalValidationTargetSnapshot(
		context.Context,
		schema.Table,
	) (stage4AdapterIncrementalTargetEvidenceSnapshot, error)
}

type stage4AdapterIncrementalTargetEvidenceSnapshot interface {
	// ReadStage4IncrementalValidationTargetKeys proves target membership using
	// only the complete target primary key. Count-only validation must never
	// fetch arbitrary transferred values merely to count an exact key scope.
	ReadStage4IncrementalValidationTargetKeys(
		context.Context,
		schema.Table,
		[]ValidationPrimaryKey,
	) ([]ValidationPrimaryKey, error)
	// ReadStage4IncrementalValidationTargetNullCounts returns compact aggregate
	// NULL facts for the exact key batch. It must not select the underlying
	// values, which may be arbitrarily wide BLOB or TEXT columns.
	ReadStage4IncrementalValidationTargetNullCounts(
		context.Context,
		schema.Table,
		[]string,
		[]ValidationPrimaryKey,
	) (stage4AdapterIncrementalTargetNullCounts, error)
	// ReadStage4IncrementalValidationTargetSampleRows is intentionally the
	// sole full-projection read. The caller supplies no more than the bounded
	// deterministic sample keys.
	ReadStage4IncrementalValidationTargetSampleRows(
		context.Context,
		schema.Table,
		[]string,
		[]ValidationPrimaryKey,
	) ([]ValidationSampleRow, error)
	Close() error
}

type stage4AdapterIncrementalTargetNullCounts struct {
	Rows   int64
	Counts map[string]int64
}

// stage4AdapterIncrementalValidationEvidence is a read-only ValidationCore
// probe assembled from exact batches that have already received a durable
// target receipt and an exact canonical target-row proof. It is scoped to the
// rows actually transferred by the saved attempt. In particular, §9.1 does
// not let a later live whole-source query redefine that set merely because a
// row has a value at or below an upper fence.
//
// Source counts, NULL counts, and bounded deterministic sample selectors are
// captured from those batches; selected source row values themselves live only
// in the private spool. The final target facts are intentionally not copied
// from that batch-time proof: every complete primary key is spooled and queried
// again from the real target in one repeatable-read validation view.
type stage4AdapterIncrementalValidationEvidence struct {
	tables         map[stage4RichTableKey]*stage4AdapterIncrementalTableEvidence
	targetReader   stage4AdapterIncrementalTargetEvidenceReader
	spoolDirectory string
}

type stage4AdapterIncrementalTableEvidence struct {
	source                  schema.Table
	target                  schema.Table
	projection              []string
	primaryKeyIndexes       []int
	targetPrimaryKeyIndexes []int
	primaryKeyDescriptor    validationSampleDescriptor
	targetPrimaryKey        []schema.Column
	requiresNulls           bool
	sampleLimit             int
	spoolPlanID             string

	mu       sync.Mutex
	sealed   bool
	rows     int64
	nulls    map[string]int64
	samples  []stage4AdapterIncrementalEvidenceSample
	keySpool *deleteKeySpool

	targetOnce    sync.Once
	targetRows    int64
	targetNulls   map[string]int64
	targetSamples map[string]ValidationSampleRow
	targetErr     error
}

type stage4AdapterIncrementalEvidenceSample struct {
	canonical string
	keyValues []any
}

type stage4AdapterIncrementalEvidenceSampleWrite struct {
	canonical string
	row       []any
}

type stage4AdapterIncrementalEvidenceSpoolKey struct {
	canonical []byte
	values    []any
}

type stage4AdapterIncrementalEvidenceKey struct {
	canonical []byte
	values    []any
}

func newStage4AdapterIncrementalValidationEvidence(
	mode config.ValidationMode,
	plans []adapterTablePlan,
	target targetAdapter,
	spoolDirectory string,
) (*stage4AdapterIncrementalValidationEvidence, error) {
	validationPlan, err := BuildValidationPlan(mode)
	if err != nil {
		return nil, err
	}
	targetReader, err := newStage4AdapterIncrementalTargetEvidenceReader(
		target,
	)
	if err != nil {
		return nil, err
	}
	evidence := &stage4AdapterIncrementalValidationEvidence{
		tables: make(
			map[stage4RichTableKey]*stage4AdapterIncrementalTableEvidence,
			len(plans),
		),
		targetReader:   targetReader,
		spoolDirectory: spoolDirectory,
	}
	for _, plan := range plans {
		key := stage4RichTableKey{
			schema: plan.source.Schema,
			table:  plan.source.Name,
		}
		if _, exists := evidence.tables[key]; exists {
			return nil, fmt.Errorf(
				"incremental validation evidence duplicates table (%q, %q)",
				key.schema,
				key.table,
			)
		}
		requireSampleTypes := planIncludes(
			validationPlan,
			ValidationPassSample,
		)
		_, err := validateValidationCoreProjection(
			plan.source,
			plan.columns,
			requireSampleTypes,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"incremental validation source projection for table (%q, %q): %w",
				key.schema,
				key.table,
				err,
			)
		}
		if err := targetReader.ValidateStage4IncrementalValidationTarget(
			plan.target,
			plan.columns,
			requireSampleTypes,
		); err != nil {
			return nil, fmt.Errorf(
				"incremental validation target projection for table (%q, %q): %w",
				key.schema,
				key.table,
				err,
			)
		}
		_, keyIndexes, keyDescriptor, err := stage4AdapterIncrementalKeyProjection(
			plan.source,
			plan.columns,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"incremental validation primary key for table (%q, %q): %w",
				key.schema,
				key.table,
				err,
			)
		}
		targetPrimaryKey, targetKeyIndexes, _, err :=
			stage4AdapterIncrementalKeyProjection(
				plan.target,
				plan.columns,
			)
		if err != nil {
			return nil, fmt.Errorf(
				"incremental validation target primary key for table (%q, %q): %w",
				key.schema,
				key.table,
				err,
			)
		}
		spoolPlanID := stage4AdapterIncrementalValidationSpoolPlanID(
			plan.source,
			plan.target,
			plan.columns,
		)
		if err := stage4AdapterIncrementalCleanupStaleValidationSpool(
			spoolDirectory,
			spoolPlanID,
		); err != nil {
			return nil, fmt.Errorf(
				"prepare incremental validation spool for table (%q, %q): %w",
				key.schema,
				key.table,
				err,
			)
		}
		tableEvidence := &stage4AdapterIncrementalTableEvidence{
			source:                  cloneStage4RichTable(plan.source),
			target:                  cloneStage4RichTable(plan.target),
			projection:              append([]string(nil), plan.columns...),
			primaryKeyIndexes:       keyIndexes,
			targetPrimaryKeyIndexes: targetKeyIndexes,
			primaryKeyDescriptor:    keyDescriptor,
			targetPrimaryKey:        append([]schema.Column(nil), targetPrimaryKey...),
			requiresNulls: planIncludes(
				validationPlan,
				ValidationPassNullParity,
			),
			nulls:       make(map[string]int64, len(plan.columns)),
			spoolPlanID: spoolPlanID,
		}
		for _, column := range plan.columns {
			tableEvidence.nulls[column] = 0
		}
		if planIncludes(validationPlan, ValidationPassSample) {
			tableEvidence.sampleLimit =
				stage4AdapterIncrementalValidationSampleLimit
		}
		evidence.tables[key] = tableEvidence
	}
	return evidence, nil
}

// stage4AdapterIncrementalValidationSpoolPlanID is deterministic only within
// a run-private spool directory. A completed-window replay reconstructs the
// same table projection, so it can first remove an artifact left by a hard
// process stop instead of accumulating inaccessible random spool files.
func stage4AdapterIncrementalValidationSpoolPlanID(
	source schema.Table,
	target schema.Table,
	projection []string,
) string {
	identity := strings.Join(
		[]string{
			source.Schema,
			source.Name,
			target.Schema,
			target.Name,
			strings.Join(projection, "\x00"),
		},
		"\x00",
	)
	digest := sha256.Sum256([]byte(identity))
	return "incremental-" + hex.EncodeToString(digest[:])
}

// stage4AdapterIncrementalCleanupStaleValidationSpool removes only the exact
// deterministic private spool and SQLite sidecars for this table. Every path
// goes through the same no-symlink/private-file authentication as delete
// spools, so tampering is a pre-mutation failure rather than a broad cleanup.
func stage4AdapterIncrementalCleanupStaleValidationSpool(
	directory string,
	planID string,
) error {
	if strings.TrimSpace(planID) == "" {
		return fmt.Errorf("incremental validation spool plan ID is required")
	}
	path := filepath.Join(directory, "dmtx-delete-"+planID+".db")
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		if err := removeDeleteSpoolPath(directory, path+suffix); err != nil {
			return fmt.Errorf("remove stale incremental validation spool artifact: %w", err)
		}
	}
	return nil
}

func newStage4AdapterIncrementalTargetEvidenceReader(
	target targetAdapter,
) (stage4AdapterIncrementalTargetEvidenceReader, error) {
	if custom, ok := target.(stage4AdapterIncrementalTargetEvidenceReader); ok &&
		!isNilInterface(custom) {
		return custom, nil
	}
	endpoint, err := adapterValidationTargetEndpoint(target)
	if err != nil {
		return nil, fmt.Errorf(
			"incremental final target validation reader is unavailable: %w",
			err,
		)
	}
	if err := validateAdapterValidationEndpoint(endpoint); err != nil {
		return nil, fmt.Errorf(
			"incremental final target validation endpoint is invalid: %w",
			err,
		)
	}
	return &stage4AdapterIncrementalSQLTargetEvidenceReader{
		endpoint: endpoint,
	}, nil
}

type stage4AdapterIncrementalSQLTargetEvidenceReader struct {
	endpoint adapterValidationSQLEndpoint
}

func (reader *stage4AdapterIncrementalSQLTargetEvidenceReader) ValidateStage4IncrementalValidationTarget(
	table schema.Table,
	projection []string,
	requireSampleTypes bool,
) error {
	_, err := stage4AdapterIncrementalValidationTargetPrimaryKey(
		reader.endpoint,
		table,
		projection,
		requireSampleTypes,
	)
	return err
}

func stage4AdapterIncrementalValidationTargetPrimaryKey(
	endpoint adapterValidationSQLEndpoint,
	table schema.Table,
	projection []string,
	requireSampleTypes bool,
) ([]schema.Column, error) {
	if err := stage4AdapterIncrementalValidateTargetTable(endpoint, table); err != nil {
		return nil, err
	}
	if _, err := validateValidationCoreProjection(
		table,
		projection,
		requireSampleTypes,
	); err != nil {
		return nil, err
	}
	return adapterValidationPrimaryKey(table)
}

func stage4AdapterIncrementalValidateTargetTable(
	endpoint adapterValidationSQLEndpoint,
	table schema.Table,
) error {
	if endpoint.engine == adapterValidationSQLite {
		if table.Schema != "" {
			return fmt.Errorf(
				"SQLite target table %s has schema %q",
				table.Name,
				table.Schema,
			)
		}
	} else if table.Schema != endpoint.namespace {
		return fmt.Errorf(
			"planned schema %q differs from target namespace %q",
			table.Schema,
			endpoint.namespace,
		)
	}
	return nil
}

// stage4AdapterIncrementalKeyProjection admits only the complete primary-key
// domains needed to scope exact target reads. Non-key projected columns are
// intentionally not type-canonicalized here: count_only needs counts, and
// null_parity needs NULL facts. Sample mode owns full-row canonicalization in
// ValidationCore, preserving the inclusive §12 boundary without newly
// refusing a non-sample route for an otherwise irrelevant value domain.
func stage4AdapterIncrementalKeyProjection(
	table schema.Table,
	projection []string,
) ([]schema.Column, []int, validationSampleDescriptor, error) {
	primaryKey, err := adapterValidationPrimaryKey(table)
	if err != nil {
		return nil, nil, validationSampleDescriptor{}, err
	}
	descriptor, err := adapterValidationKeyDescriptor(primaryKey)
	if err != nil {
		return nil, nil, validationSampleDescriptor{}, err
	}
	positions := make(map[string]int, len(projection))
	for index, column := range projection {
		if _, exists := positions[column]; exists {
			return nil, nil, validationSampleDescriptor{}, fmt.Errorf(
				"primary-key projection duplicates column %s",
				column,
			)
		}
		positions[column] = index
	}
	indexes := make([]int, len(primaryKey))
	for index, column := range primaryKey {
		position, found := positions[column.Name]
		if !found {
			return nil, nil, validationSampleDescriptor{}, fmt.Errorf(
				"primary-key projection omits column %s",
				column.Name,
			)
		}
		indexes[index] = position
	}
	return primaryKey, indexes, descriptor, nil
}

func (reader *stage4AdapterIncrementalSQLTargetEvidenceReader) OpenStage4IncrementalValidationTargetSnapshot(
	ctx context.Context,
	table schema.Table,
) (stage4AdapterIncrementalTargetEvidenceSnapshot, error) {
	if reader == nil || reader.endpoint.database == nil {
		return nil, fmt.Errorf("incremental final target validation database is unavailable")
	}
	if ctx == nil {
		return nil, fmt.Errorf("incremental final target validation context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := stage4AdapterIncrementalTargetSnapshotOptions(
		reader.endpoint.engine,
	)
	transaction, err := reader.endpoint.database.BeginTx(ctx, options)
	if err != nil {
		return nil, fmt.Errorf(
			"begin repeatable incremental target validation read: %w",
			err,
		)
	}
	snapshot := &stage4AdapterIncrementalSQLTargetEvidenceSnapshot{
		endpoint:    reader.endpoint,
		transaction: transaction,
	}
	if reader.endpoint.engine == adapterValidationSQLServer {
		if err := snapshot.lockSQLServerTargetTable(ctx, table); err != nil {
			return nil, errors.Join(
				fmt.Errorf("lock SQL Server incremental final target table: %w", err),
				snapshot.Close(),
			)
		}
	}
	return snapshot, nil
}

func stage4AdapterIncrementalTargetSnapshotOptions(engine string) *sql.TxOptions {
	options := &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	}
	if engine == adapterValidationSQLServer {
		// go-mssqldb rejects TxOptions.ReadOnly. A repeatable read is not enough
		// here: later keyed batches could observe a phantom write. Hold a shared
		// table lock for the complete validation read below, under serializable
		// isolation, before any keyed batch is issued.
		options.Isolation = sql.LevelSerializable
		options.ReadOnly = false
	}
	return options
}

type stage4AdapterIncrementalSQLTargetEvidenceSnapshot struct {
	endpoint    adapterValidationSQLEndpoint
	transaction *sql.Tx
	mu          sync.Mutex
	closed      bool
}

// lockSQLServerTargetTable establishes the target validation view before the
// first key batch. TABLOCK plus HOLDLOCK keeps a shared table lock through the
// serializable transaction, so a writer cannot change an as-yet-unread later
// key between batches and make the aggregate facts a torn view. TOP (1) avoids
// a full-table scan while still requiring SQL Server to acquire the table lock
// even when the target subset is empty.
func (snapshot *stage4AdapterIncrementalSQLTargetEvidenceSnapshot) lockSQLServerTargetTable(
	ctx context.Context,
	table schema.Table,
) error {
	if snapshot == nil || snapshot.transaction == nil {
		return fmt.Errorf("SQL Server incremental final target transaction is unavailable")
	}
	if snapshot.endpoint.engine != adapterValidationSQLServer {
		return fmt.Errorf("SQL Server target table lock was requested for engine %q", snapshot.endpoint.engine)
	}
	query, err := stage4AdapterIncrementalSQLServerTargetLockQuery(
		snapshot.endpoint,
		table,
	)
	if err != nil {
		return err
	}
	rows, err := snapshot.transaction.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	var value int
	if rows.Next() {
		if err := rows.Scan(&value); err != nil {
			closeErr := rows.Close()
			return errors.Join(err, closeErr)
		}
	}
	if err := rows.Err(); err != nil {
		closeErr := rows.Close()
		return errors.Join(err, closeErr)
	}
	return rows.Close()
}

func stage4AdapterIncrementalSQLServerTargetLockQuery(
	endpoint adapterValidationSQLEndpoint,
	table schema.Table,
) (string, error) {
	if endpoint.engine != adapterValidationSQLServer {
		return "", fmt.Errorf("SQL Server target lock requested for engine %q", endpoint.engine)
	}
	if table.Schema != endpoint.namespace {
		return "", fmt.Errorf(
			"planned schema %q differs from target namespace %q",
			table.Schema,
			endpoint.namespace,
		)
	}
	return "SELECT TOP (1) 1 FROM " + endpoint.qualified(table) +
		" WITH (TABLOCK, HOLDLOCK)", nil
}

func (snapshot *stage4AdapterIncrementalSQLTargetEvidenceSnapshot) ReadStage4IncrementalValidationTargetKeys(
	ctx context.Context,
	table schema.Table,
	keys []ValidationPrimaryKey,
) ([]ValidationPrimaryKey, error) {
	if err := stage4AdapterIncrementalValidateTargetReadContext(ctx); err != nil {
		return nil, err
	}
	primaryKey, predicate, arguments, err :=
		snapshot.stage4AdapterIncrementalTargetReadPlan(table, keys)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []ValidationPrimaryKey{}, nil
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.transaction == nil {
		return nil, fmt.Errorf("incremental final target validation snapshot is closed")
	}
	query := stage4AdapterIncrementalTargetKeyQuery(
		snapshot.endpoint,
		table,
		primaryKey,
		predicate,
	)
	rows, err := snapshot.transaction.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf(
			"fetch exact incremental final target primary keys for table %s: %w",
			table.Name,
			err,
		)
	}
	defer rows.Close()
	result := make([]ValidationPrimaryKey, 0, len(keys))
	for rows.Next() {
		if len(result) >= len(keys) {
			return nil, fmt.Errorf(
				"incremental final target validation returned more primary keys than requested",
			)
		}
		key, err := scanAdapterValidationPrimaryKey(rows, len(primaryKey))
		if err != nil {
			return nil, fmt.Errorf(
				"scan incremental final target primary key: %w",
				err,
			)
		}
		result = append(result, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate incremental final target primary keys: %w",
			err,
		)
	}
	return result, nil
}

func stage4AdapterIncrementalTargetKeyQuery(
	endpoint adapterValidationSQLEndpoint,
	table schema.Table,
	primaryKey []schema.Column,
	predicate string,
) string {
	return "SELECT " + adapterValidationQuotedColumns(
		endpoint,
		adapterValidationColumnNames(primaryKey),
	) + " FROM " + endpoint.qualified(table) + " WHERE " + predicate
}

func (snapshot *stage4AdapterIncrementalSQLTargetEvidenceSnapshot) ReadStage4IncrementalValidationTargetNullCounts(
	ctx context.Context,
	table schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) (stage4AdapterIncrementalTargetNullCounts, error) {
	if err := stage4AdapterIncrementalValidateTargetReadContext(ctx); err != nil {
		return stage4AdapterIncrementalTargetNullCounts{}, err
	}
	if _, err := validateValidationCoreProjection(table, projection, false); err != nil {
		return stage4AdapterIncrementalTargetNullCounts{}, err
	}
	_, predicate, arguments, err := snapshot.stage4AdapterIncrementalTargetReadPlan(
		table,
		keys,
	)
	if err != nil {
		return stage4AdapterIncrementalTargetNullCounts{}, err
	}
	if len(keys) == 0 {
		return stage4AdapterIncrementalTargetNullCounts{
			Counts: zeroStage4AdapterIncrementalNullCounts(projection),
		}, nil
	}
	query := stage4AdapterIncrementalTargetNullCountQuery(
		snapshot.endpoint,
		table,
		projection,
		predicate,
	)
	result := stage4AdapterIncrementalTargetNullCounts{
		Counts: zeroStage4AdapterIncrementalNullCounts(projection),
	}
	destinations := make([]any, len(projection)+1)
	destinations[0] = &result.Rows
	counts := make([]int64, len(projection))
	for index := range projection {
		destinations[index+1] = &counts[index]
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.transaction == nil {
		return stage4AdapterIncrementalTargetNullCounts{}, fmt.Errorf(
			"incremental final target validation snapshot is closed",
		)
	}
	if err := snapshot.transaction.QueryRowContext(
		ctx,
		query,
		arguments...,
	).Scan(destinations...); err != nil {
		return stage4AdapterIncrementalTargetNullCounts{}, fmt.Errorf(
			"collect exact incremental final target NULL counts for table %s: %w",
			table.Name,
			err,
		)
	}
	if result.Rows < 0 || result.Rows > int64(len(keys)) {
		return stage4AdapterIncrementalTargetNullCounts{}, fmt.Errorf(
			"incremental final target NULL row count is invalid",
		)
	}
	for index, column := range projection {
		result.Counts[column] = counts[index]
		if counts[index] < 0 || counts[index] > result.Rows {
			return stage4AdapterIncrementalTargetNullCounts{}, fmt.Errorf(
				"incremental final target NULL count for column %s is invalid",
				column,
			)
		}
	}
	return result, nil
}

func stage4AdapterIncrementalTargetNullCountQuery(
	endpoint adapterValidationSQLEndpoint,
	table schema.Table,
	projection []string,
	predicate string,
) string {
	terms := make([]string, 0, len(projection)+1)
	terms = append(terms, "COUNT(*)")
	for _, column := range projection {
		quoted := endpoint.quote(column)
		terms = append(
			terms,
			"COALESCE(SUM(CASE WHEN "+quoted+
				" IS NULL THEN 1 ELSE 0 END), 0)",
		)
	}
	return "SELECT " + strings.Join(terms, ", ") +
		" FROM " + endpoint.qualified(table) + " WHERE " + predicate
}

func (snapshot *stage4AdapterIncrementalSQLTargetEvidenceSnapshot) ReadStage4IncrementalValidationTargetSampleRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	if err := stage4AdapterIncrementalValidateTargetReadContext(ctx); err != nil {
		return nil, err
	}
	if _, err := validateValidationCoreProjection(table, projection, true); err != nil {
		return nil, err
	}
	_, predicate, arguments, err := snapshot.stage4AdapterIncrementalTargetReadPlan(
		table,
		keys,
	)
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return []ValidationSampleRow{}, nil
	}
	snapshot.mu.Lock()
	defer snapshot.mu.Unlock()
	if snapshot.closed || snapshot.transaction == nil {
		return nil, fmt.Errorf("incremental final target validation snapshot is closed")
	}
	query := "SELECT " + adapterValidationQuotedColumns(
		snapshot.endpoint,
		projection,
	) + " FROM " + snapshot.endpoint.qualified(table) +
		" WHERE " + predicate
	rows, err := snapshot.transaction.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf(
			"fetch sampled incremental final target rows for table %s: %w",
			table.Name,
			err,
		)
	}
	return scanAdapterValidationRows(
		rows,
		len(projection),
		len(keys),
		"sampled incremental final target validation",
	)
}

func stage4AdapterIncrementalValidateTargetReadContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("incremental final target validation context is required")
	}
	return ctx.Err()
}

func (snapshot *stage4AdapterIncrementalSQLTargetEvidenceSnapshot) stage4AdapterIncrementalTargetReadPlan(
	table schema.Table,
	keys []ValidationPrimaryKey,
) ([]schema.Column, string, []any, error) {
	if snapshot == nil {
		return nil, "", nil, fmt.Errorf(
			"incremental final target validation snapshot is unavailable",
		)
	}
	if err := stage4AdapterIncrementalValidateTargetTable(
		snapshot.endpoint,
		table,
	); err != nil {
		return nil, "", nil, err
	}
	primaryKey, err := adapterValidationPrimaryKey(table)
	if err != nil {
		return nil, "", nil, err
	}
	batchSize, err := adapterValidationKeyBatchSize(
		snapshot.endpoint.parameterLimit,
		len(primaryKey),
	)
	if err != nil {
		return nil, "", nil, err
	}
	if len(keys) == 0 {
		return primaryKey, "", nil, nil
	}
	if len(keys) > batchSize {
		return nil, "", nil, fmt.Errorf(
			"incremental final target validation key batch exceeds its certified parameter bound",
		)
	}
	predicate, arguments, err := adapterValidationKeyPredicate(
		snapshot.endpoint,
		primaryKey,
		keys,
	)
	if err != nil {
		return nil, "", nil, err
	}
	return primaryKey, predicate, arguments, nil
}

func (snapshot *stage4AdapterIncrementalSQLTargetEvidenceSnapshot) Close() error {
	if snapshot == nil {
		return nil
	}
	snapshot.mu.Lock()
	if snapshot.closed {
		snapshot.mu.Unlock()
		return nil
	}
	snapshot.closed = true
	transaction := snapshot.transaction
	snapshot.transaction = nil
	snapshot.mu.Unlock()
	if transaction == nil {
		return nil
	}
	err := transaction.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

// RecordExactBatch may run only after ValidateStage4IncrementalBatch returned
// success. The batch proof remains the immediate write proof; this method
// additionally records source facts and every exact complete primary key for
// the later target re-read. Sample values live only in the private process
// spool, never in durable Stage 4 state or logs.
func (evidence *stage4AdapterIncrementalValidationEvidence) RecordExactBatch(
	ctx context.Context,
	table schema.Table,
	projection []string,
	rows [][]any,
) error {
	if evidence == nil {
		return fmt.Errorf("incremental validation evidence is required")
	}
	if ctx == nil {
		return fmt.Errorf("incremental validation evidence context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	item, err := evidence.table(table)
	if err != nil {
		return err
	}
	if !sameStage4AdapterIncrementalProjection(item.projection, projection) {
		return fmt.Errorf(
			"incremental validation projection differs for table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	if len(rows) == 0 {
		return nil
	}

	evidenceKeys := make([]stage4AdapterIncrementalEvidenceKey, len(rows))
	parameters := make([][]byte, len(rows))
	batchNulls := make(map[string]int64, len(item.projection))
	sampleRows := make(map[string][]any)
	for index, row := range rows {
		if len(row) != len(item.projection) {
			return fmt.Errorf(
				"incremental validation row has %d values for %d projected columns",
				len(row),
				len(item.projection),
			)
		}
		keyValues, err := stage4AdapterIncrementalKeyValues(
			item.primaryKeyIndexes,
			row,
		)
		if err != nil {
			return fmt.Errorf("read incremental validation complete primary key %d: %w", index+1, err)
		}
		canonical, err := adapterValidationCanonicalKey(
			item.primaryKeyDescriptor,
			keyValues,
		)
		if err != nil {
			return fmt.Errorf("canonicalize incremental validation complete primary key %d: %w", index+1, err)
		}
		encoded, err := stage4AdapterIncrementalEncodePrimaryKey(
			keyValues,
		)
		if err != nil {
			return fmt.Errorf(
				"encode incremental validation complete primary key %d: %w",
				index+1,
				err,
			)
		}
		evidenceKeys[index] = stage4AdapterIncrementalEvidenceKey{
			canonical: append([]byte(nil), canonical...),
			values:    keyValues,
		}
		parameters[index] = encoded
		if item.sampleLimit != 0 {
			sampleRows[string(canonical)] = row
		}
		for index, value := range row {
			if value == nil {
				batchNulls[item.projection[index]]++
			}
		}
	}

	item.mu.Lock()
	defer item.mu.Unlock()
	if item.sealed {
		return fmt.Errorf(
			"incremental validation evidence for table (%q, %q) is already sealed",
			table.Schema,
			table.Name,
		)
	}
	if int64(len(rows)) > math.MaxInt64-item.rows {
		return fmt.Errorf(
			"incremental validation row count overflows for table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	updatedSamples, sampleWrites, sampleDeletes, err := item.updatedSamples(
		evidenceKeys,
		sampleRows,
	)
	if err != nil {
		return err
	}
	createdSpool := false
	if item.keySpool == nil {
		spool, err := newDeleteKeySpool(
			evidence.spoolDirectory,
			item.spoolPlanID,
		)
		if err != nil {
			return fmt.Errorf("open incremental validation key spool: %w", err)
		}
		item.keySpool = spool
		createdSpool = true
	}
	if item.sampleLimit != 0 {
		if err := stage4AdapterIncrementalEnsureSampleSpool(
			ctx,
			item.keySpool,
		); err != nil {
			if createdSpool {
				spool := item.keySpool
				item.keySpool = nil
				return errors.Join(
					err,
					cleanupDeleteKeySpool(evidence.spoolDirectory, spool),
				)
			}
			return err
		}
	}
	if err := stage4AdapterIncrementalStoreEvidenceKeys(
		ctx,
		item.keySpool,
		evidenceKeys,
		parameters,
		sampleWrites,
		sampleDeletes,
	); err != nil {
		if createdSpool {
			spool := item.keySpool
			item.keySpool = nil
			return errors.Join(
				err,
				cleanupDeleteKeySpool(evidence.spoolDirectory, spool),
			)
		}
		return err
	}
	item.rows += int64(len(rows))
	for column, count := range batchNulls {
		item.nulls[column] += count
	}
	item.samples = updatedSamples
	return nil
}

func stage4AdapterIncrementalEncodePrimaryKey(values []any) ([]byte, error) {
	parameters := make([]driver.Value, len(values))
	for index, value := range values {
		if value == nil {
			return nil, fmt.Errorf("primary-key column %d is NULL", index+1)
		}
		parameter := cloneAdapterValidationValue(value)
		if !driver.IsValue(parameter) {
			return nil, fmt.Errorf(
				"primary-key column %d has unsupported parameter type %T",
				index+1,
				value,
			)
		}
		if bytes, ok := parameter.([]byte); ok {
			parameter = append([]byte(nil), bytes...)
		}
		parameters[index] = parameter
	}
	return encodeDeleteParameters(parameters)
}

func stage4AdapterIncrementalKeyValues(
	indexes []int,
	values []any,
) ([]any, error) {
	if len(indexes) == 0 {
		return nil, fmt.Errorf("complete primary key is required")
	}
	result := make([]any, len(indexes))
	for index, position := range indexes {
		if position < 0 || position >= len(values) {
			return nil, fmt.Errorf("primary-key projection is incomplete")
		}
		if values[position] == nil {
			return nil, fmt.Errorf("primary-key column %d is NULL", index+1)
		}
		result[index] = cloneAdapterValidationValue(values[position])
	}
	return result, nil
}

func stage4AdapterIncrementalStoreEvidenceKeys(
	ctx context.Context,
	spool *deleteKeySpool,
	keys []stage4AdapterIncrementalEvidenceKey,
	parameters [][]byte,
	sampleWrites []stage4AdapterIncrementalEvidenceSampleWrite,
	sampleDeletes []string,
) (storeErr error) {
	if spool == nil || spool.db == nil {
		return fmt.Errorf("incremental validation key spool is unavailable")
	}
	if len(keys) != len(parameters) {
		return fmt.Errorf("incremental validation key spool inputs differ")
	}
	transaction, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin incremental validation key spool write: %w", err)
	}
	defer func() {
		if storeErr == nil {
			return
		}
		rollbackErr := transaction.Rollback()
		if rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			storeErr = errors.Join(storeErr, fmt.Errorf(
				"rollback incremental validation key spool write: %w",
				rollbackErr,
			))
		}
	}()
	for index, key := range keys {
		result, err := transaction.ExecContext(
			ctx,
			`INSERT INTO target_keys (canonical, parameters) VALUES (?, ?)`,
			key.canonical,
			parameters[index],
		)
		if err != nil {
			return fmt.Errorf(
				"record exact incremental validation primary key: %w",
				err,
			)
		}
		stored, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"confirm exact incremental validation primary key record: %w",
				err,
			)
		}
		if stored != 1 {
			return fmt.Errorf(
				"incremental validation primary-key spool stored %d rows, want one",
				stored,
			)
		}
	}
	for _, canonical := range sampleDeletes {
		result, err := transaction.ExecContext(
			ctx,
			`DELETE FROM incremental_samples WHERE canonical = ?`,
			[]byte(canonical),
		)
		if err != nil {
			return fmt.Errorf(
				"remove evicted incremental validation source sample: %w",
				err,
			)
		}
		removed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"confirm evicted incremental validation source sample removal: %w",
				err,
			)
		}
		if removed != 1 {
			return fmt.Errorf(
				"incremental validation source sample spool removed %d rows, want one",
				removed,
			)
		}
	}
	for _, sample := range sampleWrites {
		encoded, err := stage4AdapterIncrementalEncodeSampleRow(sample.row)
		if err != nil {
			return fmt.Errorf(
				"encode bounded incremental validation source sample: %w",
				err,
			)
		}
		result, err := transaction.ExecContext(
			ctx,
			`INSERT INTO incremental_samples (canonical, row_values) VALUES (?, ?)`,
			[]byte(sample.canonical),
			encoded,
		)
		if err != nil {
			return fmt.Errorf(
				"record bounded incremental validation source sample: %w",
				err,
			)
		}
		stored, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"confirm bounded incremental validation source sample record: %w",
				err,
			)
		}
		if stored != 1 {
			return fmt.Errorf(
				"incremental validation source sample spool stored %d rows, want one",
				stored,
			)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit incremental validation key spool write: %w", err)
	}
	return nil
}

func stage4AdapterIncrementalEnsureSampleSpool(
	ctx context.Context,
	spool *deleteKeySpool,
) error {
	if spool == nil || spool.db == nil {
		return fmt.Errorf("incremental validation sample spool is unavailable")
	}
	if _, err := spool.db.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS incremental_samples (
			canonical BLOB PRIMARY KEY,
			row_values BLOB NOT NULL
		) WITHOUT ROWID`,
	); err != nil {
		return fmt.Errorf("initialize incremental validation sample spool: %w", err)
	}
	return nil
}

func (item *stage4AdapterIncrementalTableEvidence) updatedSamples(
	keys []stage4AdapterIncrementalEvidenceKey,
	rows map[string][]any,
) (
	[]stage4AdapterIncrementalEvidenceSample,
	[]stage4AdapterIncrementalEvidenceSampleWrite,
	[]string,
	error,
) {
	if item.sampleLimit == 0 {
		return item.samples, nil, nil, nil
	}
	updated := cloneStage4AdapterIncrementalEvidenceSamples(item.samples)
	previous := make(map[string]struct{}, len(item.samples))
	for _, sample := range item.samples {
		previous[sample.canonical] = struct{}{}
	}
	for _, candidate := range keys {
		var err error
		updated, err = stage4AdapterIncrementalInsertSample(
			updated,
			item.sampleLimit,
			item.primaryKeyDescriptor,
			string(candidate.canonical),
			candidate.values,
		)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	current := make(map[string]struct{}, len(updated))
	writes := make(
		[]stage4AdapterIncrementalEvidenceSampleWrite,
		0,
		len(updated),
	)
	for _, sample := range updated {
		current[sample.canonical] = struct{}{}
		if _, exists := previous[sample.canonical]; exists {
			continue
		}
		row, found := rows[sample.canonical]
		if !found {
			return nil, nil, nil, fmt.Errorf(
				"incremental validation source sample has no current batch row",
			)
		}
		writes = append(writes, stage4AdapterIncrementalEvidenceSampleWrite{
			canonical: sample.canonical,
			row:       row,
		})
	}
	deletes := make([]string, 0, len(item.samples))
	for _, sample := range item.samples {
		if _, exists := current[sample.canonical]; !exists {
			deletes = append(deletes, sample.canonical)
		}
	}
	return updated, writes, deletes, nil
}

func cloneStage4AdapterIncrementalEvidenceSamples(
	values []stage4AdapterIncrementalEvidenceSample,
) []stage4AdapterIncrementalEvidenceSample {
	result := make([]stage4AdapterIncrementalEvidenceSample, len(values))
	for index, value := range values {
		result[index] = stage4AdapterIncrementalEvidenceSample{
			canonical: value.canonical,
			keyValues: append([]any(nil), value.keyValues...),
		}
	}
	return result
}

func stage4AdapterIncrementalInsertSample(
	samples []stage4AdapterIncrementalEvidenceSample,
	limit int,
	descriptor validationSampleDescriptor,
	canonical string,
	key []any,
) ([]stage4AdapterIncrementalEvidenceSample, error) {
	position := len(samples)
	for index, existing := range samples {
		comparison, err := compareValidationPrimaryKeyValues(
			descriptor,
			key,
			existing.keyValues,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"compare incremental validation sample keys: %w",
				err,
			)
		}
		if comparison == 0 || canonical == existing.canonical {
			return nil, fmt.Errorf(
				"incremental validation evidence contains a duplicate complete primary key",
			)
		}
		if comparison < 0 {
			position = index
			break
		}
	}
	if position >= limit {
		return samples, nil
	}
	candidate := stage4AdapterIncrementalEvidenceSample{
		canonical: canonical,
		keyValues: append([]any(nil), key...),
	}
	samples = append(samples, stage4AdapterIncrementalEvidenceSample{})
	copy(samples[position+1:], samples[position:])
	samples[position] = candidate
	if len(samples) > limit {
		samples = samples[:limit]
	}
	return samples, nil
}

func stage4AdapterIncrementalEncodeSampleRow(values []any) ([]byte, error) {
	if uint64(len(values)) > math.MaxUint32 {
		return nil, fmt.Errorf("incremental validation source sample is too wide")
	}
	encoded := make([]byte, 5)
	encoded[0] = 1
	binary.BigEndian.PutUint32(encoded[1:], uint32(len(values)))
	for index, value := range values {
		kind, payload, err := stage4AdapterIncrementalEncodeSampleValue(value)
		if err != nil {
			return nil, fmt.Errorf("encode sample column %d: %w", index+1, err)
		}
		encoded = append(encoded, kind)
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(payload)))
		encoded = append(encoded, length...)
		encoded = append(encoded, payload...)
	}
	return encoded, nil
}

// stage4AdapterIncrementalEncodeSampleValue is deliberately independent of
// database/sql's parameter conversion. Source drivers may return unsigned
// integers (notably MySQL unsigned values) that ValidationCore can compare
// canonically but DefaultParameterConverter rejects above MaxInt64. The wire
// format preserves the validator's admitted raw scalar shapes without
// conflating NULL, text, bytes, or timestamps.
func stage4AdapterIncrementalEncodeSampleValue(value any) (byte, []byte, error) {
	switch typed := value.(type) {
	case nil:
		return 0, nil, nil
	case int:
		return stage4AdapterIncrementalEncodeSampleInt64(int64(typed))
	case int8:
		return stage4AdapterIncrementalEncodeSampleInt64(int64(typed))
	case int16:
		return stage4AdapterIncrementalEncodeSampleInt64(int64(typed))
	case int32:
		return stage4AdapterIncrementalEncodeSampleInt64(int64(typed))
	case int64:
		return stage4AdapterIncrementalEncodeSampleInt64(typed)
	case uint:
		return stage4AdapterIncrementalEncodeSampleUint64(uint64(typed))
	case uint8:
		return stage4AdapterIncrementalEncodeSampleUint64(uint64(typed))
	case uint16:
		return stage4AdapterIncrementalEncodeSampleUint64(uint64(typed))
	case uint32:
		return stage4AdapterIncrementalEncodeSampleUint64(uint64(typed))
	case uint64:
		return stage4AdapterIncrementalEncodeSampleUint64(typed)
	case float32:
		return stage4AdapterIncrementalEncodeSampleFloat64(float64(typed))
	case float64:
		return stage4AdapterIncrementalEncodeSampleFloat64(typed)
	case bool:
		if typed {
			return 3, []byte{1}, nil
		}
		return 3, []byte{0}, nil
	case []byte:
		return 4, append([]byte(nil), typed...), nil
	case sql.RawBytes:
		return 4, append([]byte(nil), typed...), nil
	case string:
		return 5, []byte(typed), nil
	case time.Time:
		return 6, []byte(typed.UTC().Format(time.RFC3339Nano)), nil
	default:
		return 0, nil, fmt.Errorf("unsupported validation value type %T", value)
	}
}

func stage4AdapterIncrementalEncodeSampleInt64(value int64) (byte, []byte, error) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(value))
	return 1, payload, nil
}

func stage4AdapterIncrementalEncodeSampleUint64(value uint64) (byte, []byte, error) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, value)
	return 7, payload, nil
}

func stage4AdapterIncrementalEncodeSampleFloat64(value float64) (byte, []byte, error) {
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, math.Float64bits(value))
	return 2, payload, nil
}

func stage4AdapterIncrementalDecodeSampleRow(encoded []byte) ([]any, error) {
	if len(encoded) < 5 || encoded[0] != 1 {
		return nil, fmt.Errorf("incremental validation source sample encoding is invalid")
	}
	countWire := binary.BigEndian.Uint32(encoded[1:5])
	encoded = encoded[5:]
	if uint64(countWire) > uint64(len(encoded)/9) {
		return nil, fmt.Errorf("incremental validation source sample is truncated")
	}
	count := int(countWire)
	values := make([]any, count)
	for index := 0; index < count; index++ {
		if len(encoded) < 9 {
			return nil, fmt.Errorf(
				"incremental validation source sample column %d is truncated",
				index+1,
			)
		}
		kind := encoded[0]
		length := binary.BigEndian.Uint64(encoded[1:9])
		encoded = encoded[9:]
		if length > uint64(len(encoded)) {
			return nil, fmt.Errorf(
				"incremental validation source sample column %d length is invalid",
				index+1,
			)
		}
		payload := encoded[:int(length)]
		encoded = encoded[int(length):]
		switch kind {
		case 0:
			if len(payload) != 0 {
				return nil, fmt.Errorf("invalid nil incremental validation sample")
			}
			values[index] = nil
		case 1:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid int64 incremental validation sample")
			}
			values[index] = int64(binary.BigEndian.Uint64(payload))
		case 2:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid float64 incremental validation sample")
			}
			values[index] = math.Float64frombits(binary.BigEndian.Uint64(payload))
		case 3:
			if len(payload) != 1 || payload[0] > 1 {
				return nil, fmt.Errorf("invalid boolean incremental validation sample")
			}
			values[index] = payload[0] == 1
		case 4:
			values[index] = append([]byte(nil), payload...)
		case 5:
			values[index] = string(payload)
		case 6:
			value, err := time.Parse(time.RFC3339Nano, string(payload))
			if err != nil {
				return nil, fmt.Errorf(
					"invalid timestamp incremental validation sample: %w",
					err,
				)
			}
			values[index] = value
		case 7:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid uint64 incremental validation sample")
			}
			values[index] = binary.BigEndian.Uint64(payload)
		default:
			return nil, fmt.Errorf(
				"unknown incremental validation sample kind %d",
				kind,
			)
		}
	}
	if len(encoded) != 0 {
		return nil, fmt.Errorf("incremental validation source sample has trailing data")
	}
	return values, nil
}

func (evidence *stage4AdapterIncrementalValidationEvidence) ExactCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	if side != ValidationSource && side != ValidationTarget {
		return 0, fmt.Errorf("unknown incremental validation side %q", side)
	}
	item, err := evidence.table(table)
	if err != nil {
		return 0, err
	}
	if side == ValidationSource {
		item.mu.Lock()
		defer item.mu.Unlock()
		return item.rows, nil
	}
	if err := evidence.materializeTarget(ctx, item); err != nil {
		return 0, err
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	return item.targetRows, nil
}

func (*stage4AdapterIncrementalValidationEvidence) EstimateCount(
	context.Context,
	ValidationSide,
	schema.Table,
) (int64, error) {
	return 0, fmt.Errorf("incremental exact validation evidence must not fall back to an estimate")
}

func (evidence *stage4AdapterIncrementalValidationEvidence) NullCounts(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
	projection []string,
	scope ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	item, err := evidence.table(table)
	if err != nil {
		return ValidationNullCountEvidence{}, err
	}
	if !sameStage4AdapterIncrementalProjection(item.projection, projection) {
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"incremental NULL validation projection differs for table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	switch side {
	case ValidationSource:
		if scope.Kind != ValidationNullScopeTransferredSource ||
			len(scope.PrimaryKeyColumns) != 0 || scope.EqualityProofDigest != "" {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"incremental source NULL validation requested an invalid scope",
			)
		}
		item.mu.Lock()
		defer item.mu.Unlock()
		return ValidationNullCountEvidence{
			Scope:  cloneValidationNullScope(scope),
			Rows:   item.rows,
			Counts: cloneStage4AdapterIncrementalNullCounts(item.nulls),
		}, nil
	case ValidationTarget:
		if scope.Kind != ValidationNullScopeTargetSourcePrimaryKeys ||
			len(scope.PrimaryKeyColumns) == 0 ||
			!validValidationEqualityProofDigest(scope.EqualityProofDigest) {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"incremental target NULL validation requires exact source-owned primary-key scope",
			)
		}
		if err := evidence.materializeTarget(ctx, item); err != nil {
			return ValidationNullCountEvidence{}, err
		}
		item.mu.Lock()
		defer item.mu.Unlock()
		return ValidationNullCountEvidence{
			Scope:  cloneValidationNullScope(scope),
			Rows:   item.targetRows,
			Counts: cloneStage4AdapterIncrementalNullCounts(item.targetNulls),
		}, nil
	default:
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"unknown incremental NULL validation side %q",
			side,
		)
	}
}

func cloneStage4AdapterIncrementalNullCounts(
	counts map[string]int64,
) map[string]int64 {
	result := make(map[string]int64, len(counts))
	for column, count := range counts {
		result[column] = count
	}
	return result
}

func (evidence *stage4AdapterIncrementalValidationEvidence) SampleSourceRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	limit int,
) ([]ValidationSampleRow, error) {
	item, err := evidence.table(table)
	if err != nil {
		return nil, err
	}
	if !sameStage4AdapterIncrementalProjection(item.projection, projection) {
		return nil, fmt.Errorf("incremental source sample projection differs")
	}
	if ctx == nil {
		return nil, fmt.Errorf("incremental source sample context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	item.mu.Lock()
	if item.sampleLimit == 0 || limit < 1 || limit > item.sampleLimit {
		item.mu.Unlock()
		return nil, fmt.Errorf("incremental source sample limit %d is unsupported", limit)
	}
	count := len(item.samples)
	if count > limit {
		count = limit
	}
	samples := cloneStage4AdapterIncrementalEvidenceSamples(item.samples[:count])
	spool := item.keySpool
	item.mu.Unlock()
	if len(samples) == 0 {
		return []ValidationSampleRow{}, nil
	}
	if spool == nil || spool.db == nil {
		return nil, fmt.Errorf("incremental source sample spool is unavailable")
	}
	result := make([]ValidationSampleRow, len(samples))
	for index, sample := range samples {
		var encoded []byte
		if err := spool.db.QueryRowContext(
			ctx,
			`SELECT row_values FROM incremental_samples WHERE canonical = ?`,
			[]byte(sample.canonical),
		).Scan(&encoded); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf(
					"incremental source sample spool is missing a selected primary key",
				)
			}
			return nil, fmt.Errorf("read incremental source sample spool: %w", err)
		}
		values, err := stage4AdapterIncrementalDecodeSampleRow(encoded)
		if err != nil {
			return nil, fmt.Errorf("decode incremental source sample spool: %w", err)
		}
		if len(values) != len(item.projection) {
			return nil, fmt.Errorf("incremental source sample spool row width is invalid")
		}
		result[index] = ValidationSampleRow{Values: values}
	}
	return result, nil
}

func (evidence *stage4AdapterIncrementalValidationEvidence) SampleTargetRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	item, err := evidence.table(table)
	if err != nil {
		return nil, err
	}
	if !sameStage4AdapterIncrementalProjection(item.projection, projection) {
		return nil, fmt.Errorf("incremental target sample projection differs")
	}
	item.mu.Lock()
	if item.sampleLimit == 0 || len(keys) > item.sampleLimit {
		item.mu.Unlock()
		return nil, fmt.Errorf(
			"incremental target sample key count %d is unsupported",
			len(keys),
		)
	}
	sourceSamples := make(map[string]struct{}, len(item.samples))
	for _, sample := range item.samples {
		sourceSamples[sample.canonical] = struct{}{}
	}
	item.mu.Unlock()
	for _, key := range keys {
		canonical, err := adapterValidationCanonicalKey(
			item.primaryKeyDescriptor,
			key.Values,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize requested incremental target sample key: %w",
				err,
			)
		}
		if _, found := sourceSamples[string(canonical)]; !found {
			return nil, fmt.Errorf(
				"requested incremental target sample key is outside exact transferred evidence",
			)
		}
	}
	if err := evidence.materializeTarget(ctx, item); err != nil {
		return nil, err
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	result := make([]ValidationSampleRow, 0, len(keys))
	for _, key := range keys {
		canonical, err := adapterValidationCanonicalKey(
			item.primaryKeyDescriptor,
			key.Values,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"canonicalize requested incremental target sample key: %w",
				err,
			)
		}
		row, found := item.targetSamples[string(canonical)]
		if !found {
			continue
		}
		result = append(result, ValidationSampleRow{
			Values: cloneAdapterRow(row.Values),
		})
	}
	return result, nil
}

func (evidence *stage4AdapterIncrementalValidationEvidence) materializeTarget(
	ctx context.Context,
	item *stage4AdapterIncrementalTableEvidence,
) error {
	if ctx == nil {
		return fmt.Errorf("incremental final target validation context is required")
	}
	item.targetOnce.Do(func() {
		item.targetErr = evidence.collectTargetFacts(ctx, item)
	})
	return item.targetErr
}

func (evidence *stage4AdapterIncrementalValidationEvidence) collectTargetFacts(
	ctx context.Context,
	item *stage4AdapterIncrementalTableEvidence,
) (collectErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	item.mu.Lock()
	if item.sealed {
		item.mu.Unlock()
		return fmt.Errorf("incremental validation evidence is already finalizing")
	}
	item.sealed = true
	spool := item.keySpool
	sourceRows := item.rows
	sampleKeys := make(map[string]struct{}, len(item.samples))
	for _, sample := range item.samples {
		sampleKeys[sample.canonical] = struct{}{}
	}
	item.mu.Unlock()

	if sourceRows == 0 {
		item.mu.Lock()
		item.targetRows = 0
		item.targetNulls = zeroStage4AdapterIncrementalNullCounts(item.projection)
		item.targetSamples = make(map[string]ValidationSampleRow)
		item.mu.Unlock()
		return nil
	}
	if spool == nil || spool.db == nil {
		return fmt.Errorf("incremental validation source rows have no complete-primary-key spool")
	}
	snapshot, err := evidence.targetReader.OpenStage4IncrementalValidationTargetSnapshot(
		ctx,
		item.target,
	)
	if err != nil {
		return fmt.Errorf("open incremental final target validation snapshot: %w", err)
	}
	defer func() {
		if closeErr := snapshot.Close(); closeErr != nil {
			collectErr = errors.Join(collectErr, fmt.Errorf(
				"close incremental final target validation snapshot: %w",
				closeErr,
			))
		}
	}()
	spoolRows, err := spool.db.QueryContext(
		ctx,
		`SELECT canonical, parameters FROM target_keys ORDER BY canonical`,
	)
	if err != nil {
		return fmt.Errorf("read incremental validation key spool: %w", err)
	}
	defer func() {
		if closeErr := spoolRows.Close(); closeErr != nil {
			collectErr = errors.Join(collectErr, fmt.Errorf(
				"close incremental validation key spool read: %w",
				closeErr,
			))
		}
	}()
	batchSize, err := adapterValidationKeyBatchSize(
		adapterValidationParameterLimitForTable(
			evidence.targetReader,
			item.targetPrimaryKey,
		),
		len(item.targetPrimaryKey),
	)
	if err != nil {
		return fmt.Errorf("plan incremental final target validation key batches: %w", err)
	}
	targetNulls := zeroStage4AdapterIncrementalNullCounts(item.projection)
	targetSamples := make(map[string]ValidationSampleRow, len(sampleKeys))
	var targetRows int64
	batch := make([]stage4AdapterIncrementalEvidenceSpoolKey, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		keys := make([]ValidationPrimaryKey, len(batch))
		expected := make(map[string]struct{}, len(batch))
		for index, value := range batch {
			keys[index] = ValidationPrimaryKey{Values: append([]any(nil), value.values...)}
			expected[string(value.canonical)] = struct{}{}
		}
		fetched, err := snapshot.ReadStage4IncrementalValidationTargetKeys(
			ctx,
			item.target,
			keys,
		)
		if err != nil {
			return err
		}
		if len(fetched) > len(batch) {
			return fmt.Errorf(
				"incremental final target validation returned more primary keys than requested",
			)
		}
		seen := make(map[string]struct{}, len(fetched))
		for _, fetchedKey := range fetched {
			canonical, err := adapterValidationCanonicalKey(
				item.primaryKeyDescriptor,
				fetchedKey.Values,
			)
			if err != nil {
				return fmt.Errorf("canonicalize incremental final target complete primary key: %w", err)
			}
			key := string(canonical)
			if _, found := expected[key]; !found {
				return fmt.Errorf(
					"incremental final target validation returned an unrequested complete primary key",
				)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf(
					"incremental final target validation returned a duplicate complete primary key",
				)
			}
			seen[key] = struct{}{}
			if targetRows == math.MaxInt64 {
				return fmt.Errorf("incremental final target validation row count overflows")
			}
			targetRows++
		}
		if item.requiresNulls {
			nulls, err := snapshot.ReadStage4IncrementalValidationTargetNullCounts(
				ctx,
				item.target,
				item.projection,
				keys,
			)
			if err != nil {
				return err
			}
			if nulls.Rows != int64(len(seen)) {
				return fmt.Errorf(
					"incremental final target NULL facts cover %d rows, want %d exact target primary keys",
					nulls.Rows,
					len(seen),
				)
			}
			for _, column := range item.projection {
				count, found := nulls.Counts[column]
				if !found || count < 0 || count > nulls.Rows {
					return fmt.Errorf(
						"incremental final target NULL count for column %s is invalid",
						column,
					)
				}
				if count > math.MaxInt64-targetNulls[column] {
					return fmt.Errorf(
						"incremental final target NULL count for column %s overflows",
						column,
					)
				}
				targetNulls[column] += count
			}
		}
		if item.sampleLimit != 0 {
			sampleBatch := make([]ValidationPrimaryKey, 0, len(batch))
			sampleExpected := make(map[string]struct{}, len(batch))
			for index, value := range batch {
				key := string(value.canonical)
				if _, sampled := sampleKeys[key]; !sampled {
					continue
				}
				sampleBatch = append(sampleBatch, keys[index])
				sampleExpected[key] = struct{}{}
			}
			if len(sampleBatch) != 0 {
				rows, err := snapshot.ReadStage4IncrementalValidationTargetSampleRows(
					ctx,
					item.target,
					item.projection,
					sampleBatch,
				)
				if err != nil {
					return err
				}
				if len(rows) > len(sampleBatch) {
					return fmt.Errorf(
						"incremental final target validation returned more sampled rows than requested",
					)
				}
				sampleSeen := make(map[string]struct{}, len(rows))
				for _, row := range rows {
					keyValues, err := stage4AdapterIncrementalKeyValues(
						item.targetPrimaryKeyIndexes,
						row.Values,
					)
					if err != nil {
						return fmt.Errorf(
							"read incremental final target sampled primary key: %w",
							err,
						)
					}
					canonical, err := adapterValidationCanonicalKey(
						item.primaryKeyDescriptor,
						keyValues,
					)
					if err != nil {
						return fmt.Errorf(
							"canonicalize incremental final target sampled primary key: %w",
							err,
						)
					}
					key := string(canonical)
					if _, found := sampleExpected[key]; !found {
						return fmt.Errorf(
							"incremental final target validation returned an unrequested sampled primary key",
						)
					}
					if _, found := seen[key]; !found {
						return fmt.Errorf(
							"incremental final target sample is absent from exact target primary-key evidence",
						)
					}
					if _, duplicate := sampleSeen[key]; duplicate {
						return fmt.Errorf(
							"incremental final target validation returned a duplicate sampled primary key",
						)
					}
					sampleSeen[key] = struct{}{}
					targetSamples[key] = ValidationSampleRow{
						Values: cloneAdapterRow(row.Values),
					}
				}
			}
		}
		batch = batch[:0]
		return nil
	}
	for spoolRows.Next() {
		var canonical []byte
		var encoded []byte
		if err := spoolRows.Scan(&canonical, &encoded); err != nil {
			return fmt.Errorf("scan incremental validation key spool: %w", err)
		}
		parameters, err := decodeDeleteParameters(encoded)
		if err != nil {
			return fmt.Errorf("decode incremental validation key spool: %w", err)
		}
		values := make([]any, len(parameters))
		for index, value := range parameters {
			values[index] = cloneAdapterValidationValue(value)
		}
		batch = append(batch, stage4AdapterIncrementalEvidenceSpoolKey{
			canonical: append([]byte(nil), canonical...),
			values:    values,
		})
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := spoolRows.Err(); err != nil {
		return fmt.Errorf("iterate incremental validation key spool: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}
	item.mu.Lock()
	item.targetRows = targetRows
	item.targetNulls = targetNulls
	item.targetSamples = targetSamples
	item.mu.Unlock()
	return nil
}

// adapterValidationParameterLimitForTable keeps a target-reader capability
// private to the actual reader. Custom test/readers never receive a batch
// wider than the conservative public maximum; production SQL readers expose
// their dialect's certified parameter limit below.
func adapterValidationParameterLimitForTable(
	reader stage4AdapterIncrementalTargetEvidenceReader,
	primaryKey []schema.Column,
) int {
	if sqlReader, ok := reader.(*stage4AdapterIncrementalSQLTargetEvidenceReader); ok &&
		sqlReader != nil {
		return sqlReader.endpoint.parameterLimit
	}
	// The custom reader contract is used only by tests and is still bounded by
	// the narrowest certified production endpoint (SQLite).
	if len(primaryKey) > adapterValidationSQLiteParameterLimit {
		return 0
	}
	return adapterValidationSQLiteParameterLimit
}

func zeroStage4AdapterIncrementalNullCounts(
	projection []string,
) map[string]int64 {
	result := make(map[string]int64, len(projection))
	for _, column := range projection {
		result[column] = 0
	}
	return result
}

func (evidence *stage4AdapterIncrementalValidationEvidence) Close() error {
	if evidence == nil {
		return nil
	}
	var cleanupErr error
	for _, item := range evidence.tables {
		item.mu.Lock()
		spool := item.keySpool
		item.keySpool = nil
		item.mu.Unlock()
		if spool == nil {
			continue
		}
		if err := cleanupDeleteKeySpool(evidence.spoolDirectory, spool); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf(
				"cleanup incremental validation key spool: %w",
				err,
			))
		}
	}
	return cleanupErr
}

func (evidence *stage4AdapterIncrementalValidationEvidence) table(
	table schema.Table,
) (*stage4AdapterIncrementalTableEvidence, error) {
	if evidence == nil {
		return nil, fmt.Errorf("incremental validation evidence is required")
	}
	item, found := evidence.tables[stage4RichTableKey{
		schema: table.Schema,
		table:  table.Name,
	}]
	if !found {
		return nil, fmt.Errorf(
			"incremental validation evidence has no table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	return item, nil
}

func sameStage4AdapterIncrementalProjection(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
