package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

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
