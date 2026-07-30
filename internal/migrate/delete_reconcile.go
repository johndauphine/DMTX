package migrate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

type deleteKeyRows interface {
	Next() bool
	Values() ([]any, error)
	Err() error
	Close() error
}

type deleteKeySource interface {
	OpenDeletePrimaryKeys(
		context.Context,
		schema.Table,
		[]string,
	) (deleteKeyRows, error)
}

// deleteTargetBatch must be applied atomically with a target-side durable
// receipt keyed by Token. Replaying a token must return the original receipt,
// even when the rows are already absent. This closes the target-commit/state-
// acknowledgement crash window without guessing from affected-row counts.
type deleteTargetBatch struct {
	Table       schema.Table
	Columns     []string
	PlanID      string
	Token       string
	Sequence    int64
	BatchDigest string
	Keys        [][]driver.Value
}

type deleteTargetBatchReceipt struct {
	PlanID        string
	Token         string
	Sequence      int64
	BatchDigest   string
	Candidates    int64
	DeletedRows   int64
	ReceiptDigest string
	// FailClosedReason is target-atomic journal evidence. A target that
	// returns a durable receipt and an error must store
	// target_mutation_failed here so receipt replay cannot erase the error.
	// Protector errors are deliberately not stored in the target journal:
	// they describe local mutation authority and may be retried under a
	// healthy protector if their state acknowledgement did not commit.
	FailClosedReason string
}

type deleteKeyTarget interface {
	OpenDeletePrimaryKeys(
		context.Context,
		schema.Table,
		[]string,
	) (deleteKeyRows, error)
	MaxDeleteParameters() int
	ApplyDeleteBatch(
		context.Context,
		deleteTargetBatch,
	) (deleteTargetBatchReceipt, error)
}

// deleteMutationProtector must prove current target ownership immediately
// around every driver mutation. Returning without invoking mutation is the
// required stale-lease behavior.
type deleteMutationProtector interface {
	ProtectDeleteMutation(context.Context, func() error) error
}

type deleteReconciliationState interface {
	BeginDeleteReconciliation(
		state.DeleteReconciliation,
	) (state.DeleteReconciliation, bool, error)
	LoadDeleteReconciliation(
		string,
		state.TaskKey,
		string,
	) (state.DeleteReconciliation, bool, error)
	LoadLatestSuccessfulDeleteReconciliation(
		string,
		state.TaskKey,
	) (state.DeleteReconciliation, bool, error)
	SaveDeleteReconciliationPlan(state.DeleteReconciliationPlan) error
	BeginDeleteReconciliationBatch(
		state.DeleteReconciliationBatch,
	) (state.DeleteReconciliationBatch, bool, error)
	CommitDeleteReconciliationBatch(
		state.DeleteReconciliationBatchCommit,
	) error
	FinishDeleteReconciliation(state.DeleteReconciliationResult) error
}

var _ deleteReconciliationState = (state.Stage4Backend)(nil)

type deleteKeySide string

const (
	deleteKeySourceSide deleteKeySide = "source"
	deleteKeyTargetSide deleteKeySide = "target"
)

type deleteKeyColumnProof struct {
	Semantics         string `json:"semantics"`
	CollationEvidence string `json:"collation_evidence,omitempty"`
}

// deleteKeyEqualityProof is route-owned evidence that source and target key
// values share one exact equality domain. The core binds its digest into the
// durable candidate plan; it never infers text/collation semantics itself.
type deleteKeyEqualityProof struct {
	CanonicalizerID   string                 `json:"canonicalizer_id"`
	SourceFingerprint string                 `json:"source_fingerprint"`
	TargetFingerprint string                 `json:"target_fingerprint"`
	Columns           []deleteKeyColumnProof `json:"columns"`
}

type deleteCanonicalValue struct {
	Canonical []byte
	Parameter driver.Value
}

type deleteKeyCanonicalizer interface {
	ProveDeleteKeyEquality(
		schema.Table,
		schema.Table,
		[]schema.Column,
		[]schema.Column,
	) (deleteKeyEqualityProof, error)
	CanonicalizeDeleteKeyValue(
		deleteKeySide,
		deleteKeyEqualityProof,
		int,
		any,
	) (deleteCanonicalValue, error)
}

type deleteReconcileRequest struct {
	RunID          string
	AttemptID      string
	Task           state.TaskKey
	SourceTable    schema.Table
	TargetTable    schema.Table
	TargetMode     string
	Policy         config.DeletePolicy
	DryRun         bool
	Now            time.Time
	SpoolDirectory string
	MaxBatchBytes  int64
}

type deleteDueFacts struct {
	Due              bool
	Reason           string
	LastSuccessfulAt *time.Time
	NextDueAt        *time.Time
}

type deleteReconcileOutcome struct {
	Record                state.DeleteReconciliation
	DueFacts              deleteDueFacts
	StrictCountValidation bool
}

type deleteReconciler struct {
	state         deleteReconciliationState
	source        deleteKeySource
	target        deleteKeyTarget
	canonicalizer deleteKeyCanonicalizer
	protector     deleteMutationProtector
	now           func() time.Time
}

type deleteKeyPlan struct {
	sourcePrimaryKey []schema.Column
	targetPrimaryKey []schema.Column
	sourceColumns    []string
	targetColumns    []string
	proof            deleteKeyEqualityProof
	proofDigest      string
}

func deleteReconciliationDue(
	now time.Time,
	interval time.Duration,
	last state.DeleteReconciliation,
	found bool,
) (deleteDueFacts, error) {
	if now.IsZero() {
		return deleteDueFacts{}, fmt.Errorf(
			"delete reconciliation current time is required",
		)
	}
	if interval <= 0 {
		return deleteDueFacts{}, fmt.Errorf(
			"delete reconciliation interval must be positive",
		)
	}
	now = now.UTC()
	if !found {
		return deleteDueFacts{
			Due: true, Reason: "no prior successful reconciliation",
		}, nil
	}
	if err := state.ValidateDeleteReconciliationEvidence(last); err != nil {
		return deleteDueFacts{}, fmt.Errorf(
			"latest successful delete reconciliation is malformed: %w",
			err,
		)
	}
	if last.Status != state.DeleteReconciliationCompleted {
		return deleteDueFacts{}, fmt.Errorf(
			"latest successful delete reconciliation is not completed",
		)
	}
	completedAt := last.CompletedAt.UTC()
	if completedAt.After(now) {
		return deleteDueFacts{}, fmt.Errorf(
			"latest successful delete reconciliation completion is in the future",
		)
	}
	nextDueAt := completedAt.Add(interval)
	if nextDueAt.Before(completedAt) {
		return deleteDueFacts{}, fmt.Errorf(
			"delete reconciliation due time overflowed",
		)
	}
	lastAt, nextAt := completedAt, nextDueAt
	facts := deleteDueFacts{
		Due:              !now.Before(nextDueAt),
		LastSuccessfulAt: &lastAt,
		NextDueAt:        &nextAt,
	}
	if facts.Due {
		facts.Reason = "reconciliation interval elapsed"
	} else {
		facts.Reason = "reconciliation interval has not elapsed; next due at " +
			nextDueAt.Format(time.RFC3339Nano)
	}
	return facts, nil
}

func validateDeleteReconcileRequest(
	request deleteReconcileRequest,
	canonicalizer deleteKeyCanonicalizer,
) (deleteKeyPlan, error) {
	if strings.TrimSpace(request.RunID) == "" ||
		strings.TrimSpace(request.AttemptID) == "" {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation run and attempt IDs are required",
		)
	}
	if err := request.Task.Validate(); err != nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation task: %w",
			err,
		)
	}
	if request.Task.Type != "table-copy" ||
		request.Task.Partition != "" {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires one unpartitioned table-copy task",
		)
	}
	if request.SourceTable.Name == "" ||
		request.TargetTable.Name == "" ||
		request.Task.Table != request.SourceTable.Name ||
		request.Task.Schema != request.SourceTable.Schema ||
		request.TargetTable.Name != request.SourceTable.Name {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation source, target, and task table identities differ",
		)
	}
	if request.TargetMode != "upsert" {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires target mode upsert",
		)
	}
	if request.Policy.Mode != config.DeleteModeReconcile ||
		request.Policy.TargetBehavior != config.DeleteTargetHard ||
		request.Policy.Reconcile.Schedule !=
			config.DeleteScheduleInterval ||
		request.Policy.Reconcile.Interval <= 0 ||
		request.Policy.Reconcile.BatchSize <= 0 ||
		!request.Policy.Reconcile.RequirePrimaryKey {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires reconcile/hard/interval with a positive interval, positive batch size, and primary-key enforcement",
		)
	}
	if request.MaxBatchBytes <= 0 {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation maximum batch bytes must be positive",
		)
	}
	if canonicalizer == nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation requires an explicit key-equality canonicalizer",
		)
	}
	sourcePrimaryKey, err := deletePrimaryKeyColumns(
		request.SourceTable,
	)
	if err != nil {
		return deleteKeyPlan{}, err
	}
	targetPrimaryKey, err := deletePrimaryKeyColumns(
		request.TargetTable,
	)
	if err != nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"target delete primary key: %w",
			err,
		)
	}
	if len(sourcePrimaryKey) != len(targetPrimaryKey) {
		return deleteKeyPlan{}, fmt.Errorf(
			"delete reconciliation source and target primary-key widths differ",
		)
	}
	sourceColumns := make([]string, len(sourcePrimaryKey))
	targetColumns := make([]string, len(targetPrimaryKey))
	for index := range sourcePrimaryKey {
		sourceColumns[index] = sourcePrimaryKey[index].Name
		targetColumns[index] = targetPrimaryKey[index].Name
		if sourceColumns[index] != targetColumns[index] {
			return deleteKeyPlan{}, fmt.Errorf(
				"delete reconciliation source and target primary-key column %d differs",
				index+1,
			)
		}
	}
	proof, err := canonicalizer.ProveDeleteKeyEquality(
		request.SourceTable,
		request.TargetTable,
		sourcePrimaryKey,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyPlan{}, fmt.Errorf(
			"prove delete key equality: %w",
			err,
		)
	}
	proofDigest, err := validateDeleteKeyEqualityProof(
		proof,
		request.SourceTable,
		request.TargetTable,
		sourcePrimaryKey,
		targetPrimaryKey,
	)
	if err != nil {
		return deleteKeyPlan{}, err
	}
	return deleteKeyPlan{
		sourcePrimaryKey: sourcePrimaryKey,
		targetPrimaryKey: targetPrimaryKey,
		sourceColumns:    sourceColumns,
		targetColumns:    targetColumns,
		proof:            proof,
		proofDigest:      proofDigest,
	}, nil
}

func deletePrimaryKeyColumns(
	table schema.Table,
) ([]schema.Column, error) {
	seenNames := make(map[string]struct{}, len(table.Columns))
	positions := make(map[int]schema.Column)
	for _, column := range table.Columns {
		if column.Name == "" {
			return nil, fmt.Errorf(
				"delete reconciliation table %s has an empty column name",
				table.Name,
			)
		}
		if _, duplicate := seenNames[column.Name]; duplicate {
			return nil, fmt.Errorf(
				"delete reconciliation table %s has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		seenNames[column.Name] = struct{}{}
		if !column.PrimaryKey {
			if column.PrimaryKeyPosition != 0 {
				return nil, fmt.Errorf(
					"delete reconciliation table %s non-primary-key column %s has a key position",
					table.Name,
					column.Name,
				)
			}
			continue
		}
		if column.Nullable {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key column %s is nullable",
				table.Name,
				column.Name,
			)
		}
		if column.PrimaryKeyPosition <= 0 {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key column %s has no positive position",
				table.Name,
				column.Name,
			)
		}
		if previous, duplicate := positions[column.PrimaryKeyPosition]; duplicate {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key position %d is shared by %s and %s",
				table.Name,
				column.PrimaryKeyPosition,
				previous.Name,
				column.Name,
			)
		}
		positions[column.PrimaryKeyPosition] = column
	}
	if len(positions) == 0 {
		return nil, fmt.Errorf(
			"delete reconciliation table %s has no primary key",
			table.Name,
		)
	}
	primaryKey := make([]schema.Column, len(positions))
	for position := 1; position <= len(positions); position++ {
		column, exists := positions[position]
		if !exists {
			return nil, fmt.Errorf(
				"delete reconciliation table %s primary-key positions are not contiguous from one",
				table.Name,
			)
		}
		kind, err := validationKindForColumn(column)
		if err != nil {
			return nil, fmt.Errorf(
				"delete reconciliation primary-key column %s: %w",
				column.Name,
				err,
			)
		}
		if kind == validationDynamic {
			return nil, fmt.Errorf(
				"delete reconciliation rejects dynamic primary-key column %s",
				column.Name,
			)
		}
		primaryKey[position-1] = column
	}
	return primaryKey, nil
}

type deleteTableKeyFingerprint struct {
	Schema         string          `json:"schema"`
	Table          string          `json:"table"`
	MySQLCollation string          `json:"mysql_collation"`
	Columns        []schema.Column `json:"columns"`
}

func deleteKeyMetadataFingerprint(
	table schema.Table,
	primaryKey []schema.Column,
) (string, error) {
	payload, err := json.Marshal(deleteTableKeyFingerprint{
		Schema: table.Schema, Table: table.Name,
		MySQLCollation: strings.TrimSpace(table.MySQLCollation),
		Columns:        primaryKey,
	})
	if err != nil {
		return "", fmt.Errorf(
			"encode delete key metadata: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateDeleteKeyEqualityProof(
	proof deleteKeyEqualityProof,
	sourceTable schema.Table,
	targetTable schema.Table,
	sourcePrimaryKey []schema.Column,
	targetPrimaryKey []schema.Column,
) (string, error) {
	if strings.TrimSpace(proof.CanonicalizerID) == "" {
		return "", fmt.Errorf(
			"delete key equality proof has no canonicalizer ID",
		)
	}
	sourceFingerprint, err := deleteKeyMetadataFingerprint(
		sourceTable,
		sourcePrimaryKey,
	)
	if err != nil {
		return "", err
	}
	targetFingerprint, err := deleteKeyMetadataFingerprint(
		targetTable,
		targetPrimaryKey,
	)
	if err != nil {
		return "", err
	}
	if proof.SourceFingerprint != sourceFingerprint ||
		proof.TargetFingerprint != targetFingerprint {
		return "", fmt.Errorf(
			"delete key equality proof does not bind the selected source and target primary keys",
		)
	}
	if len(proof.Columns) != len(sourcePrimaryKey) {
		return "", fmt.Errorf(
			"delete key equality proof column width differs",
		)
	}
	for index, columnProof := range proof.Columns {
		sourceKind, err := validationKindForColumn(
			sourcePrimaryKey[index],
		)
		if err != nil {
			return "", err
		}
		targetKind, err := validationKindForColumn(
			targetPrimaryKey[index],
		)
		if err != nil {
			return "", err
		}
		if err := validateDeleteColumnSemantics(
			columnProof,
			sourceKind,
			targetKind,
		); err != nil {
			return "", fmt.Errorf(
				"delete key equality proof column %s: %w",
				sourcePrimaryKey[index].Name,
				err,
			)
		}
	}
	payload, err := json.Marshal(proof)
	if err != nil {
		return "", fmt.Errorf(
			"encode delete key equality proof: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateDeleteColumnSemantics(
	proof deleteKeyColumnProof,
	sourceKind validationValueKind,
	targetKind validationValueKind,
) error {
	sourceTextual := sourceKind == validationText ||
		sourceKind == validationUUID
	targetTextual := targetKind == validationText ||
		targetKind == validationUUID
	textual := sourceTextual || targetTextual
	if sourceKind == validationDynamic ||
		targetKind == validationDynamic {
		return fmt.Errorf("dynamic key equality is unsupported")
	}
	switch proof.Semantics {
	case "integer":
		if sourceKind != validationInteger ||
			targetKind != validationInteger {
			return fmt.Errorf("integer proof does not match metadata")
		}
	case "boolean":
		if (sourceKind != validationBoolean &&
			sourceKind != validationInteger) ||
			(targetKind != validationBoolean &&
				targetKind != validationInteger) {
			return fmt.Errorf("boolean proof does not match metadata")
		}
	case "decimal":
		if (sourceKind != validationDecimal &&
			sourceKind != validationInteger) ||
			(targetKind != validationDecimal &&
				targetKind != validationInteger) {
			return fmt.Errorf("decimal proof does not match metadata")
		}
	case "float_exact":
		if sourceKind != validationFloat ||
			targetKind != validationFloat {
			return fmt.Errorf("float proof does not match metadata")
		}
	case "binary":
		if sourceKind != validationBytes ||
			targetKind != validationBytes {
			return fmt.Errorf("binary proof does not match metadata")
		}
	case "date":
		if sourceKind != validationDate ||
			targetKind != validationDate {
			return fmt.Errorf("date proof does not match metadata")
		}
	case "time":
		if sourceKind != validationTime ||
			targetKind != validationTime {
			return fmt.Errorf("time proof does not match metadata")
		}
	case "timestamp":
		if sourceKind != validationTimestamp ||
			targetKind != validationTimestamp {
			return fmt.Errorf("timestamp proof does not match metadata")
		}
	case "binary_text", "uuid_binary_text":
		if !sourceTextual || !targetTextual {
			return fmt.Errorf("text proof does not match metadata")
		}
		if strings.TrimSpace(proof.CollationEvidence) == "" {
			return fmt.Errorf(
				"text/UUID equality requires explicit binary-collation evidence",
			)
		}
	default:
		return fmt.Errorf(
			"unsupported key equality semantics %q",
			proof.Semantics,
		)
	}
	if textual && proof.Semantics != "binary_text" &&
		proof.Semantics != "uuid_binary_text" {
		return fmt.Errorf(
			"text/UUID key equality lacks binary semantics",
		)
	}
	return nil
}

const deleteKeyEncodingVersion = "dmtx-delete-key-v2"

func canonicalDeleteKey(
	canonicalizer deleteKeyCanonicalizer,
	side deleteKeySide,
	proof deleteKeyEqualityProof,
	values []any,
) ([]byte, []driver.Value, error) {
	if len(values) != len(proof.Columns) {
		return nil, nil, fmt.Errorf(
			"delete key has %d values for %d proof columns",
			len(values),
			len(proof.Columns),
		)
	}
	encoded := make([]byte, 0, len(values)*48)
	encoded = appendFrame(
		encoded,
		"version",
		[]byte(deleteKeyEncodingVersion),
	)
	encoded = appendFrame(
		encoded,
		"columns",
		[]byte(strconv.Itoa(len(values))),
	)
	var parameters []driver.Value
	if side == deleteKeyTargetSide {
		parameters = make([]driver.Value, len(values))
	}
	for index, value := range values {
		if value == nil {
			return nil, nil, fmt.Errorf(
				"delete key column %d is NULL",
				index+1,
			)
		}
		canonical, err := canonicalizer.
			CanonicalizeDeleteKeyValue(side, proof, index, value)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"canonicalize delete key column %d: %w",
				index+1,
				err,
			)
		}
		encoded = appendFrame(
			encoded,
			proof.Columns[index].Semantics,
			canonical.Canonical,
		)
		if side == deleteKeyTargetSide {
			parameter, err := stableDeleteParameter(
				canonical.Parameter,
			)
			if err != nil {
				return nil, nil, fmt.Errorf(
					"delete key column %d: %w",
					index+1,
					err,
				)
			}
			parameters[index] = parameter
		}
	}
	return encoded, parameters, nil
}

func stableDeleteParameter(value driver.Value) (driver.Value, error) {
	if value == nil || !driver.IsValue(value) {
		return nil, fmt.Errorf(
			"canonicalizer returned a non-parameter-safe value",
		)
	}
	if bytesValue, ok := value.([]byte); ok {
		return append([]byte(nil), bytesValue...), nil
	}
	return value, nil
}

type deleteKeySpool struct {
	path string
	db   *sql.DB
}

type deleteKeySpoolSnapshot struct {
	transaction *sql.Tx
}

func (spool *deleteKeySpool) beginReadSnapshot(
	ctx context.Context,
) (*deleteKeySpoolSnapshot, error) {
	if spool == nil || spool.db == nil {
		return nil, fmt.Errorf(
			"delete reconciliation spool is not open",
		)
	}
	transaction, err := spool.db.BeginTx(
		ctx,
		&sql.TxOptions{ReadOnly: true},
	)
	if err != nil {
		return nil, fmt.Errorf(
			"begin delete reconciliation spool read snapshot: %w",
			err,
		)
	}
	return &deleteKeySpoolSnapshot{transaction: transaction}, nil
}

func (snapshot *deleteKeySpoolSnapshot) Close() error {
	if snapshot == nil || snapshot.transaction == nil {
		return nil
	}
	err := snapshot.transaction.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}
	return err
}

func newDeletePlanID() (string, error) {
	random := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return "", fmt.Errorf(
			"generate delete reconciliation plan ID: %w",
			err,
		)
	}
	return hex.EncodeToString(random), nil
}

func validateDeleteSpoolDirectory(directory string) (string, error) {
	if strings.TrimSpace(directory) == "" {
		return "", fmt.Errorf(
			"delete reconciliation spool directory is required",
		)
	}
	absolute, err := filepath.Abs(filepath.Clean(directory))
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool directory: %w",
			err,
		)
	}
	info, err := os.Stat(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"inspect delete reconciliation spool directory: %w",
			err,
		)
	}
	if !info.IsDir() {
		return "", fmt.Errorf(
			"delete reconciliation spool path is not a directory",
		)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool directory symlinks: %w",
			err,
		)
	}
	return filepath.Clean(resolved), nil
}

func newDeleteKeySpool(
	directory string,
	planID string,
) (*deleteKeySpool, error) {
	directory, err := validateDeleteSpoolDirectory(directory)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(
		directory,
		"dmtx-delete-"+planID+".db",
	)
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_EXCL|os.O_RDWR,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"create delete reconciliation spool: %w",
			err,
		)
	}
	if err := file.Close(); err != nil {
		cleanupErr := removeDeleteSpoolPath(directory, path)
		return nil, errors.Join(
			fmt.Errorf(
				"close new delete reconciliation spool: %w",
				err,
			),
			cleanupErr,
		)
	}
	spool, err := openDeleteKeySpool(directory, path)
	if err != nil {
		return nil, errors.Join(
			err,
			removeDeleteSpoolPath(directory, path),
		)
	}
	statements := []string{
		`PRAGMA journal_mode=DELETE`,
		`PRAGMA synchronous=FULL`,
		`PRAGMA temp_store=FILE`,
		`CREATE TABLE source_keys (
			canonical BLOB PRIMARY KEY
		) WITHOUT ROWID`,
		`CREATE TABLE target_keys (
			canonical BLOB PRIMARY KEY,
			parameters BLOB NOT NULL
		) WITHOUT ROWID`,
		`CREATE TABLE plan_meta (
			name TEXT PRIMARY KEY,
			value TEXT NOT NULL
		) WITHOUT ROWID`,
	}
	for _, statement := range statements {
		if _, err := spool.db.Exec(statement); err != nil {
			return nil, errors.Join(
				fmt.Errorf(
					"initialize delete reconciliation spool: %w",
					err,
				),
				cleanupDeleteKeySpool(directory, spool),
			)
		}
	}
	return spool, nil
}

func openDeleteKeySpool(
	directory string,
	path string,
) (*deleteKeySpool, error) {
	resolved, err := validateDeleteSpoolPath(directory, path)
	if err != nil {
		return nil, err
	}
	database, err := sql.Open("sqlite", resolved)
	if err != nil {
		return nil, fmt.Errorf(
			"open delete reconciliation spool: %w",
			err,
		)
	}
	database.SetMaxOpenConns(1)
	if err := database.Ping(); err != nil {
		database.Close()
		return nil, fmt.Errorf(
			"ping delete reconciliation spool: %w",
			err,
		)
	}
	return &deleteKeySpool{path: resolved, db: database}, nil
}

func validateDeleteSpoolPath(
	directory string,
	path string,
) (string, error) {
	directory, err := validateDeleteSpoolDirectory(directory)
	if err != nil {
		return "", err
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool path: %w",
			err,
		)
	}
	linkInfo, err := os.Lstat(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"inspect delete reconciliation spool path: %w",
			err,
		)
	}
	if linkInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf(
			"delete reconciliation spool must not be a symbolic link",
		)
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf(
			"resolve delete reconciliation spool symlinks: %w",
			err,
		)
	}
	resolved = filepath.Clean(resolved)
	relative, err := filepath.Rel(directory, resolved)
	if err != nil || relative == "." ||
		relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf(
			"delete reconciliation spool escapes its configured directory",
		)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf(
			"inspect delete reconciliation spool: %w",
			err,
		)
	}
	if !info.Mode().IsRegular() ||
		info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf(
			"delete reconciliation spool must be a private regular file",
		)
	}
	return resolved, nil
}

func removeDeleteSpoolPath(directory string, path string) error {
	resolved, err := validateDeleteSpoolPath(directory, path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := os.Remove(resolved); err != nil &&
		!errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf(
			"remove private delete reconciliation spool: %w",
			err,
		)
	}
	return nil
}

func (spool *deleteKeySpool) Close() error {
	if spool == nil || spool.db == nil {
		return nil
	}
	return spool.db.Close()
}

func cleanupDeleteKeySpool(
	directory string,
	spool *deleteKeySpool,
) error {
	if spool == nil {
		return nil
	}
	closeErr := spool.Close()
	removeErr := removeDeleteSpoolPath(directory, spool.path)
	return errors.Join(closeErr, removeErr)
}

func (spool *deleteKeySpool) sync() error {
	file, err := os.OpenFile(spool.path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open delete spool for sync: %w", err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync delete reconciliation spool: %w", err)
	}
	return nil
}

func encodeDeleteParameters(values []driver.Value) ([]byte, error) {
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, uint32(len(values)))
	for _, value := range values {
		var kind byte
		var payload []byte
		switch typed := value.(type) {
		case int64:
			kind = 1
			payload = make([]byte, 8)
			binary.BigEndian.PutUint64(payload, uint64(typed))
		case float64:
			kind = 2
			payload = make([]byte, 8)
			binary.BigEndian.PutUint64(
				payload,
				math.Float64bits(typed),
			)
		case bool:
			kind = 3
			if typed {
				payload = []byte{1}
			} else {
				payload = []byte{0}
			}
		case []byte:
			kind = 4
			payload = append([]byte(nil), typed...)
		case string:
			kind = 5
			payload = []byte(typed)
		case time.Time:
			kind = 6
			payload = []byte(
				typed.UTC().Format(time.RFC3339Nano),
			)
		default:
			return nil, fmt.Errorf(
				"unsupported delete parameter type %T",
				value,
			)
		}
		encoded = append(encoded, kind)
		length := make([]byte, 8)
		binary.BigEndian.PutUint64(length, uint64(len(payload)))
		encoded = append(encoded, length...)
		encoded = append(encoded, payload...)
	}
	return encoded, nil
}

func decodeDeleteParameters(encoded []byte) ([]driver.Value, error) {
	if len(encoded) < 4 {
		return nil, fmt.Errorf("delete parameter payload is truncated")
	}
	count := int(binary.BigEndian.Uint32(encoded[:4]))
	encoded = encoded[4:]
	values := make([]driver.Value, count)
	for index := 0; index < count; index++ {
		if len(encoded) < 9 {
			return nil, fmt.Errorf(
				"delete parameter %d is truncated",
				index,
			)
		}
		kind := encoded[0]
		length := binary.BigEndian.Uint64(encoded[1:9])
		encoded = encoded[9:]
		if length > uint64(len(encoded)) {
			return nil, fmt.Errorf(
				"delete parameter %d length is invalid",
				index,
			)
		}
		payload := encoded[:int(length)]
		encoded = encoded[int(length):]
		switch kind {
		case 1:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid int64 parameter")
			}
			values[index] = int64(binary.BigEndian.Uint64(payload))
		case 2:
			if len(payload) != 8 {
				return nil, fmt.Errorf("invalid float64 parameter")
			}
			values[index] = math.Float64frombits(
				binary.BigEndian.Uint64(payload),
			)
		case 3:
			if len(payload) != 1 || payload[0] > 1 {
				return nil, fmt.Errorf("invalid boolean parameter")
			}
			values[index] = payload[0] == 1
		case 4:
			values[index] = append([]byte(nil), payload...)
		case 5:
			values[index] = string(payload)
		case 6:
			value, err := time.Parse(
				time.RFC3339Nano,
				string(payload),
			)
			if err != nil {
				return nil, fmt.Errorf(
					"invalid timestamp parameter: %w",
					err,
				)
			}
			values[index] = value
		default:
			return nil, fmt.Errorf(
				"unknown delete parameter kind %d",
				kind,
			)
		}
	}
	if len(encoded) != 0 {
		return nil, fmt.Errorf(
			"delete parameter payload contains trailing data",
		)
	}
	return values, nil
}

func (spool *deleteKeySpool) scanKeys(
	ctx context.Context,
	side deleteKeySide,
	table schema.Table,
	columns []string,
	proof deleteKeyEqualityProof,
	canonicalizer deleteKeyCanonicalizer,
	maxKeyBytes int64,
	open func(
		context.Context,
		schema.Table,
		[]string,
	) (deleteKeyRows, error),
) error {
	if maxKeyBytes <= 0 {
		return fmt.Errorf(
			"delete key byte ceiling must be positive",
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	rows, err := open(
		ctx,
		table,
		append([]string(nil), columns...),
	)
	if err != nil {
		return fmt.Errorf("open %s delete keys: %w", side, err)
	}
	if rows == nil {
		return fmt.Errorf("%s delete key reader returned nil", side)
	}
	transaction, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		rows.Close()
		return fmt.Errorf("begin %s delete key spool: %w", side, err)
	}
	defer transaction.Rollback()
	statement := `INSERT OR IGNORE INTO source_keys (canonical) VALUES (?)`
	if side == deleteKeyTargetSide {
		statement = `INSERT OR IGNORE INTO target_keys
			(canonical, parameters) VALUES (?, ?)`
	}
	insert, err := transaction.PrepareContext(ctx, statement)
	if err != nil {
		rows.Close()
		return fmt.Errorf("prepare %s delete key spool: %w", side, err)
	}
	var scanErr error
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			scanErr = err
			break
		}
		values, err := rows.Values()
		if err != nil {
			scanErr = fmt.Errorf("read %s delete key: %w", side, err)
			break
		}
		canonical, parameters, err := canonicalDeleteKey(
			canonicalizer,
			side,
			proof,
			values,
		)
		if err != nil {
			scanErr = fmt.Errorf("%s delete key: %w", side, err)
			break
		}
		var encodedParameters []byte
		if side == deleteKeyTargetSide {
			encodedParameters, err = encodeDeleteParameters(
				parameters,
			)
			if err != nil {
				scanErr = err
				break
			}
		}
		encodedBytes := int64(len(canonical) +
			len(encodedParameters) + 16)
		if encodedBytes > maxKeyBytes {
			scanErr = fmt.Errorf(
				"%s delete key requires %d encoded bytes, exceeding the %d-byte ceiling",
				side,
				encodedBytes,
				maxKeyBytes,
			)
			break
		}
		var result sql.Result
		if side == deleteKeySourceSide {
			result, err = insert.ExecContext(ctx, canonical)
		} else {
			result, err = insert.ExecContext(
				ctx,
				canonical,
				encodedParameters,
			)
		}
		if err != nil {
			scanErr = fmt.Errorf("spool %s delete key: %w", side, err)
			break
		}
		affected, err := result.RowsAffected()
		if err != nil {
			scanErr = fmt.Errorf(
				"verify %s delete key uniqueness: %w",
				side,
				err,
			)
			break
		}
		if affected != 1 {
			scanErr = fmt.Errorf(
				"%s delete keys contain a duplicate complete primary key",
				side,
			)
			break
		}
	}
	if scanErr == nil {
		if err := rows.Err(); err != nil {
			scanErr = fmt.Errorf(
				"iterate %s delete keys: %w",
				side,
				err,
			)
		}
	}
	closeErr := rows.Close()
	statementErr := insert.Close()
	if scanErr == nil {
		scanErr = errors.Join(closeErr, statementErr)
	}
	if scanErr != nil {
		return scanErr
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit %s delete key spool: %w", side, err)
	}
	return spool.sync()
}

func writeDeleteHashFrame(
	digest hash.Hash,
	label string,
	payload []byte,
) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(label)))
	digest.Write(length[:])
	digest.Write([]byte(label))
	binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
	digest.Write(length[:])
	digest.Write(payload)
}

type deleteSpoolQueryer interface {
	QueryContext(
		context.Context,
		string,
		...any,
	) (*sql.Rows, error)
}

func deleteCandidateEvidence(
	ctx context.Context,
	queryer deleteSpoolQueryer,
) (int64, string, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT target.canonical, target.parameters
		FROM target_keys AS target
		LEFT JOIN source_keys AS source
		  ON source.canonical = target.canonical
		WHERE source.canonical IS NULL
		ORDER BY target.canonical
	`)
	if err != nil {
		return 0, "", fmt.Errorf(
			"open delete candidate stream: %w",
			err,
		)
	}
	defer rows.Close()
	digest := sha256.New()
	var count int64
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return 0, "", err
		}
		var canonical, parameters []byte
		if err := rows.Scan(&canonical, &parameters); err != nil {
			return 0, "", fmt.Errorf(
				"read delete candidate evidence: %w",
				err,
			)
		}
		writeDeleteHashFrame(digest, "key", canonical)
		writeDeleteHashFrame(digest, "parameters", parameters)
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf(
			"iterate delete candidate evidence: %w",
			err,
		)
	}
	return count, hex.EncodeToString(digest.Sum(nil)), nil
}

func (spool *deleteKeySpool) finalize(
	ctx context.Context,
	planID string,
	proofDigest string,
) (int64, string, error) {
	candidates, candidateDigest, err := deleteCandidateEvidence(
		ctx,
		spool.db,
	)
	if err != nil {
		return 0, "", err
	}
	transaction, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", fmt.Errorf("begin delete spool metadata: %w", err)
	}
	defer transaction.Rollback()
	meta := map[string]string{
		"plan_id":          planID,
		"proof_digest":     proofDigest,
		"candidate_digest": candidateDigest,
		"candidates":       strconv.FormatInt(candidates, 10),
	}
	for name, value := range meta {
		if _, err := transaction.ExecContext(
			ctx,
			`INSERT INTO plan_meta (name, value) VALUES (?, ?)`,
			name,
			value,
		); err != nil {
			return 0, "", fmt.Errorf(
				"write delete spool metadata: %w",
				err,
			)
		}
	}
	if err := transaction.Commit(); err != nil {
		return 0, "", fmt.Errorf(
			"commit delete spool metadata: %w",
			err,
		)
	}
	if err := spool.sync(); err != nil {
		return 0, "", err
	}
	return candidates, candidateDigest, nil
}

func (snapshot *deleteKeySpoolSnapshot) verify(
	ctx context.Context,
	plan state.DeleteReconciliationPlan,
	proofDigest string,
) error {
	expected := map[string]string{
		"plan_id":          plan.PlanID,
		"proof_digest":     proofDigest,
		"candidate_digest": plan.CandidateDigest,
		"candidates":       strconv.FormatInt(plan.Candidates, 10),
	}
	rows, err := snapshot.transaction.QueryContext(
		ctx,
		`SELECT name, value FROM plan_meta`,
	)
	if err != nil {
		return fmt.Errorf("read delete spool metadata: %w", err)
	}
	found := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			rows.Close()
			return fmt.Errorf("scan delete spool metadata: %w", err)
		}
		if _, duplicate := found[name]; duplicate {
			rows.Close()
			return fmt.Errorf("delete spool metadata is duplicated")
		}
		found[name] = value
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if err := errors.Join(rowsErr, closeErr); err != nil {
		return fmt.Errorf("iterate delete spool metadata: %w", err)
	}
	if len(found) != len(expected) {
		return fmt.Errorf("delete spool metadata shape differs")
	}
	for name, value := range expected {
		if found[name] != value {
			return fmt.Errorf(
				"delete spool metadata %s differs",
				name,
			)
		}
	}
	candidates, digest, err := deleteCandidateEvidence(
		ctx,
		snapshot.transaction,
	)
	if err != nil {
		return err
	}
	if candidates != plan.Candidates ||
		digest != plan.CandidateDigest {
		return fmt.Errorf(
			"delete spool candidate evidence differs from durable plan",
		)
	}
	return nil
}

type deleteSpoolCandidate struct {
	canonical  []byte
	parameters []driver.Value
}

func (snapshot *deleteKeySpoolSnapshot) candidateBatch(
	ctx context.Context,
	offset int64,
	limit int,
	maxBytes int64,
) ([]deleteSpoolCandidate, string, int64, error) {
	if offset < 0 || limit <= 0 || maxBytes <= 0 {
		return nil, "", 0, fmt.Errorf(
			"invalid delete candidate batch range",
		)
	}
	lengthRows, err := snapshot.transaction.QueryContext(ctx, `
		SELECT length(target.canonical), length(target.parameters)
		FROM target_keys AS target
		LEFT JOIN source_keys AS source
		  ON source.canonical = target.canonical
		WHERE source.canonical IS NULL
		ORDER BY target.canonical
		LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		return nil, "", 0, fmt.Errorf(
			"open delete candidate batch lengths: %w",
			err,
		)
	}
	selected := 0
	var encodedBytes int64
	for lengthRows.Next() {
		var keyBytes, parameterBytes int64
		if err := lengthRows.Scan(
			&keyBytes,
			&parameterBytes,
		); err != nil {
			lengthRows.Close()
			return nil, "", 0, fmt.Errorf(
				"read delete candidate batch lengths: %w",
				err,
			)
		}
		rowBytes := keyBytes + parameterBytes + 16
		if rowBytes <= 0 || rowBytes > maxBytes {
			lengthRows.Close()
			return nil, "", 0, fmt.Errorf(
				"delete candidate requires %d encoded bytes, exceeding the %d-byte batch ceiling",
				rowBytes,
				maxBytes,
			)
		}
		if encodedBytes+rowBytes > maxBytes {
			break
		}
		encodedBytes += rowBytes
		selected++
	}
	lengthErr := lengthRows.Err()
	closeErr := lengthRows.Close()
	if err := errors.Join(lengthErr, closeErr); err != nil {
		return nil, "", 0, fmt.Errorf(
			"iterate delete candidate batch lengths: %w",
			err,
		)
	}
	if selected == 0 {
		return nil, "", 0, fmt.Errorf(
			"delete candidate batch byte ceiling admitted no rows",
		)
	}
	rows, err := snapshot.transaction.QueryContext(ctx, `
		SELECT target.canonical, target.parameters
		FROM target_keys AS target
		LEFT JOIN source_keys AS source
		  ON source.canonical = target.canonical
		WHERE source.canonical IS NULL
		ORDER BY target.canonical
		LIMIT ? OFFSET ?
	`, selected, offset)
	if err != nil {
		return nil, "", 0, fmt.Errorf(
			"open delete candidate batch: %w",
			err,
		)
	}
	defer rows.Close()
	candidates := make([]deleteSpoolCandidate, 0, selected)
	digest := sha256.New()
	for rows.Next() {
		var canonical, encoded []byte
		if err := rows.Scan(&canonical, &encoded); err != nil {
			return nil, "", 0, fmt.Errorf(
				"read delete candidate batch: %w",
				err,
			)
		}
		parameters, err := decodeDeleteParameters(encoded)
		if err != nil {
			return nil, "", 0, err
		}
		writeDeleteHashFrame(digest, "key", canonical)
		writeDeleteHashFrame(digest, "parameters", encoded)
		candidates = append(candidates, deleteSpoolCandidate{
			canonical:  append([]byte(nil), canonical...),
			parameters: parameters,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, fmt.Errorf(
			"iterate delete candidate batch: %w",
			err,
		)
	}
	if len(candidates) != selected {
		return nil, "", 0, fmt.Errorf(
			"delete candidate batch changed between length admission and read",
		)
	}
	return candidates, hex.EncodeToString(digest.Sum(nil)),
		encodedBytes, nil
}

func deleteBatchLimit(
	configured int,
	parameterLimit int,
	keyWidth int,
) (int, error) {
	if configured <= 0 || parameterLimit <= 0 || keyWidth <= 0 {
		return 0, fmt.Errorf(
			"delete batch size, parameter limit, and key width must be positive",
		)
	}
	parameterBound := parameterLimit / keyWidth
	if parameterBound == 0 {
		return 0, fmt.Errorf(
			"delete primary-key width %d exceeds target parameter limit %d",
			keyWidth,
			parameterLimit,
		)
	}
	if configured < parameterBound {
		return configured, nil
	}
	return parameterBound, nil
}

func deleteBatchToken(
	planID string,
	sequence int64,
	batchDigest string,
) string {
	digest := sha256.Sum256([]byte(
		planID + "\x00" +
			strconv.FormatInt(sequence, 10) + "\x00" +
			batchDigest,
	))
	return hex.EncodeToString(digest[:])
}

func (reconciler deleteReconciler) completionTime(
	startedAt time.Time,
) (time.Time, error) {
	value := time.Now()
	if reconciler.now != nil {
		value = reconciler.now()
	}
	if value.IsZero() || value.Before(startedAt) {
		return time.Time{}, fmt.Errorf(
			"delete reconciliation completion time is absent or precedes its start",
		)
	}
	return value.UTC(), nil
}

func (reconciler deleteReconciler) loadLatestDueFacts(
	request deleteReconcileRequest,
) (deleteDueFacts, error) {
	last, found, err := reconciler.state.
		LoadLatestSuccessfulDeleteReconciliation(
			request.RunID,
			request.Task,
		)
	if err != nil {
		return deleteDueFacts{}, fmt.Errorf(
			"load latest successful delete reconciliation: %w",
			err,
		)
	}
	return deleteReconciliationDue(
		request.Now,
		request.Policy.Reconcile.Interval,
		last,
		found,
	)
}

func (reconciler deleteReconciler) buildSpool(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
	planID string,
) (*deleteKeySpool, int64, string, error) {
	spool, err := newDeleteKeySpool(
		request.SpoolDirectory,
		planID,
	)
	if err != nil {
		return nil, 0, "", err
	}
	fail := func(err error) (*deleteKeySpool, int64, string, error) {
		return spool, 0, "", err
	}
	if err := spool.scanKeys(
		ctx,
		deleteKeySourceSide,
		request.SourceTable,
		keyPlan.sourceColumns,
		keyPlan.proof,
		reconciler.canonicalizer,
		request.MaxBatchBytes,
		reconciler.source.OpenDeletePrimaryKeys,
	); err != nil {
		return fail(err)
	}
	if err := spool.scanKeys(
		ctx,
		deleteKeyTargetSide,
		request.TargetTable,
		keyPlan.targetColumns,
		keyPlan.proof,
		reconciler.canonicalizer,
		request.MaxBatchBytes,
		reconciler.target.OpenDeletePrimaryKeys,
	); err != nil {
		return fail(err)
	}
	candidates, digest, err := spool.finalize(
		ctx,
		planID,
		keyPlan.proofDigest,
	)
	if err != nil {
		return fail(err)
	}
	return spool, candidates, digest, nil
}

func (reconciler deleteReconciler) reconcile(
	ctx context.Context,
	request deleteReconcileRequest,
) (deleteReconcileOutcome, error) {
	if reconciler.state == nil {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"delete reconciliation state backend is required",
		)
	}
	keyPlan, err := validateDeleteReconcileRequest(
		request,
		reconciler.canonicalizer,
	)
	if err != nil {
		return deleteReconcileOutcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return deleteReconcileOutcome{}, err
	}

	if request.DryRun {
		return reconciler.reconcileDryRun(
			ctx,
			request,
			keyPlan,
		)
	}
	return reconciler.reconcileMutating(
		ctx,
		request,
		keyPlan,
	)
}

func (reconciler deleteReconciler) reconcileDryRun(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
) (outcome deleteReconcileOutcome, resultErr error) {
	dueFacts, err := reconciler.loadLatestDueFacts(request)
	if err != nil {
		return deleteReconcileOutcome{}, err
	}
	outcome = deleteReconcileOutcome{
		DueFacts: dueFacts,
		Record: state.DeleteReconciliation{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			Due:       dueFacts.Due, DryRun: true,
			Status:      state.DeleteReconciliationDryRun,
			Reason:      dueFacts.Reason,
			StartedAt:   request.Now.UTC(),
			CompletedAt: request.Now.UTC(),
		},
	}
	if !dueFacts.Due {
		outcome.Record.Status = state.DeleteReconciliationNotDue
		outcome.Record.DryRun = false
		return outcome, nil
	}
	if reconciler.source == nil || reconciler.target == nil {
		return outcome, fmt.Errorf(
			"due dry-run delete reconciliation requires source and target key readers",
		)
	}
	planID, err := newDeletePlanID()
	if err != nil {
		return outcome, err
	}
	spool, candidates, _, err := reconciler.buildSpool(
		ctx,
		request,
		keyPlan,
		planID,
	)
	if spool != nil {
		defer func() {
			if err := cleanupDeleteKeySpool(
				request.SpoolDirectory,
				spool,
			); err != nil {
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf(
						"dry-run delete reconciliation spool cleanup failed: %w",
						err,
					),
				)
			}
		}()
	}
	if err != nil {
		return outcome, err
	}
	outcome.Record.Candidates = candidates
	outcome.Record.Reason = "dry run; target rows were not mutated"
	completedAt, err := reconciler.completionTime(
		outcome.Record.StartedAt,
	)
	if err != nil {
		return outcome, err
	}
	outcome.Record.CompletedAt = completedAt
	return outcome, nil
}

func (reconciler deleteReconciler) reconcileMutating(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
) (deleteReconcileOutcome, error) {
	existing, found, err := reconciler.state.LoadDeleteReconciliation(
		request.RunID,
		request.Task,
		request.AttemptID,
	)
	if err != nil {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"load delete reconciliation attempt: %w",
			err,
		)
	}
	if found {
		if err := state.ValidateDeleteReconciliationEvidence(
			existing,
		); err != nil {
			return deleteReconcileOutcome{}, fmt.Errorf(
				"loaded delete reconciliation is malformed: %w",
				err,
			)
		}
		if existing.Status !=
			state.DeleteReconciliationRunning {
			return terminalDeleteReconcileOutcome(existing)
		}
	}
	dueFacts, err := reconciler.loadLatestDueFacts(request)
	if err != nil {
		return deleteReconcileOutcome{}, err
	}
	if found && !dueFacts.Due {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"running delete reconciliation conflicts with not-due schedule",
		)
	}
	record := existing
	if !found {
		record = state.DeleteReconciliation{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			Due:       dueFacts.Due,
			StartedAt: request.Now.UTC(),
		}
		if !dueFacts.Due {
			record.Reason = dueFacts.Reason
		}
		record, _, err = reconciler.state.
			BeginDeleteReconciliation(record)
		if err != nil {
			return deleteReconcileOutcome{}, fmt.Errorf(
				"begin delete reconciliation: %w",
				err,
			)
		}
		if err := state.ValidateDeleteReconciliationEvidence(
			record,
		); err != nil {
			return deleteReconcileOutcome{}, fmt.Errorf(
				"begun delete reconciliation is malformed: %w",
				err,
			)
		}
	}
	outcome := deleteReconcileOutcome{
		Record: record, DueFacts: dueFacts,
	}
	if !dueFacts.Due {
		return outcome, nil
	}
	if record.LastBatchCommit != nil &&
		record.LastBatchCommit.FailClosedReason != "" {
		reason := record.LastBatchCommit.FailClosedReason
		return reconciler.finishIncomplete(
			outcome,
			record,
			fmt.Errorf(
				"delete reconciliation is fail-closed after its last target receipt (%s)",
				reason,
			),
			reason,
			request.SpoolDirectory,
		)
	}
	if reconciler.source == nil || reconciler.target == nil {
		return reconciler.finishIncomplete(
			outcome,
			record,
			errors.New(
				"due delete reconciliation requires source and target key readers",
			),
			state.DeleteReconciliationReasonKeyReadersUnavailable,
			request.SpoolDirectory,
		)
	}
	if reconciler.protector == nil {
		return reconciler.finishIncomplete(
			outcome,
			record,
			errors.New(
				"delete reconciliation requires a target lease/fencing mutation protector",
			),
			state.DeleteReconciliationReasonMutationProtectionUnavailable,
			request.SpoolDirectory,
		)
	}
	batchSize, err := deleteBatchLimit(
		request.Policy.Reconcile.BatchSize,
		reconciler.target.MaxDeleteParameters(),
		len(keyPlan.targetColumns),
	)
	if err != nil {
		return reconciler.finishIncomplete(
			outcome,
			record,
			err,
			state.DeleteReconciliationReasonUnsafeBatchLimits,
			request.SpoolDirectory,
		)
	}

	var spool *deleteKeySpool
	if record.Plan == nil {
		planID, err := newDeletePlanID()
		if err != nil {
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonPlanCreationFailed,
				request.SpoolDirectory,
			)
		}
		var candidates int64
		var candidateDigest string
		spool, candidates, candidateDigest, err =
			reconciler.buildSpool(
				ctx,
				request,
				keyPlan,
				planID,
			)
		if err != nil {
			if spool != nil {
				if cleanupErr := cleanupDeleteKeySpool(
					request.SpoolDirectory,
					spool,
				); cleanupErr != nil {
					err = errors.Join(
						err,
						fmt.Errorf(
							"delete-key scan spool cleanup failed: %w",
							cleanupErr,
						),
					)
				}
			}
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonKeyScanFailed,
				request.SpoolDirectory,
			)
		}
		plannedAt, err := reconciler.completionTime(
			record.StartedAt,
		)
		if err != nil {
			if cleanupErr := cleanupDeleteKeySpool(
				request.SpoolDirectory,
				spool,
			); cleanupErr != nil {
				err = errors.Join(
					err,
					fmt.Errorf(
						"delete-plan spool cleanup failed: %w",
						cleanupErr,
					),
				)
			}
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonClockInvalid,
				request.SpoolDirectory,
			)
		}
		plan := state.DeleteReconciliationPlan{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			PlanID:    planID, SpoolPath: spool.path,
			EqualityProofDigest: keyPlan.proofDigest,
			CandidateDigest:     candidateDigest,
			Candidates:          candidates, BatchSize: batchSize,
			BatchByteLimit: request.MaxBatchBytes,
			KeyWidth:       len(keyPlan.targetColumns),
			PlannedAt:      plannedAt,
		}
		if err := reconciler.state.
			SaveDeleteReconciliationPlan(plan); err != nil {
			spool.Close()
			return outcome, fmt.Errorf(
				"persist delete reconciliation plan: %w",
				err,
			)
		}
		record.Plan = &plan
		record.Candidates = candidates
		outcome.Record = record
	} else {
		if record.Plan.EqualityProofDigest !=
			keyPlan.proofDigest ||
			record.Plan.BatchSize != batchSize ||
			record.Plan.BatchByteLimit !=
				request.MaxBatchBytes ||
			record.Plan.KeyWidth != len(keyPlan.targetColumns) {
			return reconciler.finishIncomplete(
				outcome,
				record,
				errors.New(
					"durable delete plan no longer matches route proof or batching limits",
				),
				state.DeleteReconciliationReasonDurablePlanMismatch,
				request.SpoolDirectory,
			)
		}
		spool, err = openDeleteKeySpool(
			request.SpoolDirectory,
			record.Plan.SpoolPath,
		)
		if err != nil {
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonSpoolUnavailable,
				request.SpoolDirectory,
			)
		}
	}
	snapshot, err := spool.beginReadSnapshot(ctx)
	if err != nil {
		spool.Close()
		return reconciler.finishIncomplete(
			outcome,
			record,
			err,
			state.DeleteReconciliationReasonSpoolVerificationFailed,
			request.SpoolDirectory,
		)
	}
	if err := snapshot.verify(
		ctx,
		*record.Plan,
		keyPlan.proofDigest,
	); err != nil {
		snapshot.Close()
		spool.Close()
		return reconciler.finishIncomplete(
			outcome,
			record,
			err,
			state.DeleteReconciliationReasonSpoolVerificationFailed,
			request.SpoolDirectory,
		)
	}
	defer spool.Close()
	defer snapshot.Close()
	outcome, err = reconciler.applyPlannedDeletes(
		ctx,
		request,
		keyPlan,
		snapshot,
		spool,
		outcome,
	)
	if outcome.Record.Status !=
		state.DeleteReconciliationRunning {
		snapshotErr := snapshot.Close()
		cleanupErr := cleanupDeleteKeySpool(
			request.SpoolDirectory, spool,
		)
		if snapshotErr != nil || cleanupErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"terminal delete reconciliation spool cleanup failed: %w",
					errors.Join(snapshotErr, cleanupErr),
				),
			)
		}
	}
	return outcome, err
}

func (reconciler deleteReconciler) applyPlannedDeletes(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
	snapshot *deleteKeySpoolSnapshot,
	spool *deleteKeySpool,
	outcome deleteReconcileOutcome,
) (deleteReconcileOutcome, error) {
	record := outcome.Record
	for record.Frontier < record.Candidates {
		if err := ctx.Err(); err != nil {
			return outcome, err
		}
		var intent state.DeleteReconciliationBatch
		var candidates []deleteSpoolCandidate
		if record.PendingBatch != nil {
			intent = *record.PendingBatch
			var digest string
			var encodedBytes int64
			var err error
			candidates, digest, encodedBytes, err = snapshot.candidateBatch(
				ctx,
				intent.FirstCandidate,
				int(intent.Candidates),
				record.Plan.BatchByteLimit,
			)
			if err != nil {
				return outcome, err
			}
			if int64(len(candidates)) != intent.Candidates ||
				encodedBytes != intent.EncodedBytes ||
				digest != intent.BatchDigest ||
				deleteBatchToken(
					intent.PlanID,
					intent.Sequence,
					digest,
				) != intent.Token {
				return outcome, fmt.Errorf(
					"pending delete batch differs from durable spool evidence",
				)
			}
		} else {
			remaining := record.Candidates - record.Frontier
			limit := record.Plan.BatchSize
			if remaining < int64(limit) {
				limit = int(remaining)
			}
			var digest string
			var encodedBytes int64
			var err error
			candidates, digest, encodedBytes, err = snapshot.candidateBatch(
				ctx,
				record.Frontier,
				limit,
				record.Plan.BatchByteLimit,
			)
			if err != nil {
				return outcome, err
			}
			if len(candidates) == 0 ||
				len(candidates) > limit {
				return outcome, fmt.Errorf(
					"delete spool returned an invalid candidate batch size",
				)
			}
			beganAt, err := reconciler.completionTime(
				record.Plan.PlannedAt,
			)
			if err != nil {
				return outcome, err
			}
			intent = state.DeleteReconciliationBatch{
				RunID: request.RunID, Task: request.Task,
				AttemptID:      request.AttemptID,
				PlanID:         record.Plan.PlanID,
				Sequence:       record.CommittedBatches,
				FirstCandidate: record.Frontier,
				Candidates:     int64(len(candidates)),
				EncodedBytes:   encodedBytes,
				BatchDigest:    digest,
				BeganAt:        beganAt,
			}
			intent.Token = deleteBatchToken(
				intent.PlanID,
				intent.Sequence,
				intent.BatchDigest,
			)
			stored, _, err := reconciler.state.
				BeginDeleteReconciliationBatch(intent)
			if err != nil {
				return outcome, fmt.Errorf(
					"persist delete batch intent: %w",
					err,
				)
			}
			if stored != intent {
				return outcome, fmt.Errorf(
					"stored delete batch intent differs",
				)
			}
			record.PendingBatch = &stored
			outcome.Record = record
		}
		keys := make([][]driver.Value, len(candidates))
		for index := range candidates {
			keys[index] = append(
				[]driver.Value(nil),
				candidates[index].parameters...,
			)
		}
		targetBatch := deleteTargetBatch{
			Table: request.TargetTable,
			Columns: append(
				[]string(nil),
				keyPlan.targetColumns...,
			),
			PlanID: intent.PlanID, Token: intent.Token,
			Sequence:    intent.Sequence,
			BatchDigest: intent.BatchDigest,
			Keys:        keys,
		}
		var receipt deleteTargetBatchReceipt
		var targetErr error
		invocations := 0
		protectedErr := reconciler.protector.
			ProtectDeleteMutation(ctx, func() error {
				invocations++
				if invocations != 1 {
					return fmt.Errorf(
						"target mutation protector invoked one delete batch multiple times",
					)
				}
				receipt, targetErr =
					reconciler.target.ApplyDeleteBatch(
						ctx,
						targetBatch,
					)
				return targetErr
			})
		if invocations == 0 {
			if protectedErr == nil {
				protectedErr = errors.New(
					"mutation protector returned without invoking the protected operation",
				)
			}
			return outcome, fmt.Errorf(
				"target delete mutation protection denied the driver call; resume this existing run after target ownership is restored: %w",
				protectedErr,
			)
		}
		applyErr := errors.Join(targetErr, protectedErr)
		if err := validateDeleteTargetReceipt(
			intent,
			receipt,
			targetErr,
		); err != nil {
			if applyErr != nil {
				return outcome, errors.Join(applyErr, err)
			}
			return outcome, err
		}
		failClosedReason := ""
		switch {
		case receipt.FailClosedReason != "":
			// Target-returned errors are part of the target-atomic receipt
			// journal. Receipt replay must reproduce this terminal evidence
			// even though it no longer returns the original error.
			failClosedReason = receipt.FailClosedReason
		case protectedErr != nil:
			// A protector error after the callback is local authority
			// evidence. Commit it fail-closed when possible, but do not
			// contaminate an otherwise clean target receipt: if this state
			// write fails, replay under a healthy protector may continue.
			failClosedReason = deleteIncompleteReason(
				state.DeleteReconciliationReasonTargetMutationFailed,
				protectedErr,
			)
		case receipt.DeletedRows != intent.Candidates:
			failClosedReason =
				state.DeleteReconciliationReasonTargetReceiptIncomplete
		}
		committedAt, err := reconciler.completionTime(
			intent.BeganAt,
		)
		if err != nil {
			return outcome, err
		}
		commit := state.DeleteReconciliationBatchCommit{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			PlanID:    intent.PlanID, Token: intent.Token,
			Sequence:         intent.Sequence,
			FirstCandidate:   intent.FirstCandidate,
			BatchDigest:      intent.BatchDigest,
			Candidates:       intent.Candidates,
			EncodedBytes:     intent.EncodedBytes,
			DeletedRows:      receipt.DeletedRows,
			ReceiptDigest:    receipt.ReceiptDigest,
			FailClosedReason: failClosedReason,
			CommittedAt:      committedAt,
		}
		if err := reconciler.state.
			CommitDeleteReconciliationBatch(commit); err != nil {
			ackErr := fmt.Errorf(
				"target delete batch committed, but durable frontier acknowledgement failed; repair state and resume this existing run and attempt; do not start a fresh run: %w",
				err,
			)
			return outcome, errors.Join(applyErr, ackErr)
		}
		record.Frontier += commit.Candidates
		record.CommittedBatches++
		record.DeletedRows += commit.DeletedRows
		record.PendingBatch = nil
		record.LastBatchCommit = &commit
		outcome.Record = record
		if failClosedReason != "" {
			primary := applyErr
			if primary == nil {
				if receipt.DeletedRows != intent.Candidates {
					primary = fmt.Errorf(
						"target delete batch deleted %d of %d candidates",
						receipt.DeletedRows,
						intent.Candidates,
					)
				} else {
					primary = fmt.Errorf(
						"target delete batch receipt carries fail-closed evidence: %s",
						failClosedReason,
					)
				}
			} else {
				primary = fmt.Errorf(
					"target delete batch returned a durable receipt and error: %w",
					primary,
				)
			}
			if err := errors.Join(
				snapshot.Close(),
				spool.Close(),
			); err != nil {
				primary = errors.Join(
					primary,
					fmt.Errorf(
						"close terminal delete reconciliation spool snapshot: %w",
						err,
					),
				)
			}
			return reconciler.finishIncomplete(
				outcome,
				record,
				primary,
				failClosedReason,
				request.SpoolDirectory,
			)
		}
		if err := ctx.Err(); err != nil {
			return outcome, err
		}
	}
	completedAt, err := reconciler.completionTime(
		record.StartedAt,
	)
	if err != nil {
		return outcome, err
	}
	result := state.DeleteReconciliationResult{
		RunID: record.RunID, Task: record.Task,
		AttemptID:   record.AttemptID,
		Status:      state.DeleteReconciliationCompleted,
		Candidates:  record.Candidates,
		DeletedRows: record.DeletedRows,
		SkippedRows: 0,
		CompletedAt: completedAt,
	}
	if err := ctx.Err(); err != nil {
		return outcome, err
	}
	if err := reconciler.state.
		FinishDeleteReconciliation(result); err != nil {
		return outcome, fmt.Errorf(
			"persist completed delete reconciliation after target deletes; repair state and resume this existing run and attempt; do not start a fresh run: %w",
			err,
		)
	}
	outcome.Record = deleteRecordFromResult(record, result)
	outcome.StrictCountValidation = true
	return outcome, nil
}

func validateDeleteTargetReceipt(
	intent state.DeleteReconciliationBatch,
	receipt deleteTargetBatchReceipt,
	targetErr error,
) error {
	if receipt.PlanID != intent.PlanID ||
		receipt.Token != intent.Token ||
		receipt.Sequence != intent.Sequence ||
		receipt.BatchDigest != intent.BatchDigest ||
		receipt.Candidates != intent.Candidates {
		return fmt.Errorf(
			"target delete batch receipt identity differs from durable intent",
		)
	}
	if receipt.DeletedRows < 0 ||
		receipt.DeletedRows > receipt.Candidates {
		return fmt.Errorf(
			"target delete batch receipt has invalid deleted-row count",
		)
	}
	if err := validateLowerSHA256(
		"target delete batch receipt digest",
		receipt.ReceiptDigest,
	); err != nil {
		return err
	}
	if receipt.FailClosedReason != "" &&
		receipt.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed {
		return fmt.Errorf(
			"target delete batch receipt has invalid fail-closed reason",
		)
	}
	if targetErr != nil &&
		receipt.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed {
		return fmt.Errorf(
			"target delete batch receipt returned with an error without target-atomic fail-closed evidence",
		)
	}
	return nil
}

func validateLowerSHA256(label, value string) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size ||
		value != strings.ToLower(value) {
		return fmt.Errorf(
			"%s must be a lowercase SHA-256 digest",
			label,
		)
	}
	return nil
}

func deleteIncompleteReason(suggested string, primary error) string {
	switch {
	case errors.Is(primary, context.Canceled),
		errors.Is(primary, context.DeadlineExceeded):
		return state.DeleteReconciliationReasonCancelled
	case errors.Is(primary, state.ErrLeaseLost):
		return state.DeleteReconciliationReasonLeaseLost
	default:
		return suggested
	}
}

func (reconciler deleteReconciler) finishIncomplete(
	outcome deleteReconcileOutcome,
	record state.DeleteReconciliation,
	primary error,
	reason string,
	spoolDirectory string,
) (deleteReconcileOutcome, error) {
	if primary == nil {
		primary = errors.New("delete reconciliation incomplete")
	}
	if record.PendingBatch != nil {
		return outcome, errors.Join(
			primary,
			errors.New(
				"cannot terminalize delete reconciliation with an unresolved target receipt",
			),
		)
	}
	if record.LastBatchCommit != nil &&
		record.LastBatchCommit.FailClosedReason != "" {
		// Target-atomic failure evidence is authoritative. In particular, a
		// target may return context cancellation with a durable failed receipt;
		// generic diagnostic classification must not rewrite that receipt.
		reason = record.LastBatchCommit.FailClosedReason
	} else {
		reason = deleteIncompleteReason(reason, primary)
	}
	completedAt, clockErr := reconciler.completionTime(
		record.StartedAt,
	)
	if clockErr != nil {
		primary = errors.Join(primary, clockErr)
		completedAt = record.StartedAt.UTC()
	}
	result := state.DeleteReconciliationResult{
		RunID: record.RunID, Task: record.Task,
		AttemptID:   record.AttemptID,
		Status:      state.DeleteReconciliationIncomplete,
		Candidates:  record.Candidates,
		DeletedRows: record.DeletedRows,
		SkippedRows: record.Candidates - record.DeletedRows,
		Reason:      reason,
		CompletedAt: completedAt,
	}
	if err := reconciler.state.
		FinishDeleteReconciliation(result); err != nil {
		return outcome, errors.Join(
			primary,
			fmt.Errorf(
				"persist incomplete delete reconciliation; repair state and resume this existing run and attempt; do not start a fresh run: %w",
				err,
			),
		)
	}
	outcome.Record = deleteRecordFromResult(record, result)
	if record.Plan != nil {
		if err := removeDeleteSpoolPath(
			spoolDirectory,
			record.Plan.SpoolPath,
		); err != nil {
			return outcome, errors.Join(
				primary,
				fmt.Errorf(
					"terminal delete reconciliation spool cleanup failed: %w",
					err,
				),
			)
		}
	}
	return outcome, primary
}

func terminalDeleteReconcileOutcome(
	record state.DeleteReconciliation,
) (deleteReconcileOutcome, error) {
	if err := state.ValidateDeleteReconciliationEvidence(record); err != nil {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"terminal delete reconciliation is malformed: %w",
			err,
		)
	}
	outcome := deleteReconcileOutcome{
		Record: record,
		DueFacts: deleteDueFacts{
			Due: record.Due, Reason: record.Reason,
		},
		StrictCountValidation: record.Status ==
			state.DeleteReconciliationCompleted,
	}
	if record.Status == state.DeleteReconciliationIncomplete {
		return outcome, fmt.Errorf(
			"delete reconciliation attempt %q is already incomplete: %s",
			record.AttemptID,
			record.Reason,
		)
	}
	return outcome, nil
}

func deleteRecordFromResult(
	record state.DeleteReconciliation,
	result state.DeleteReconciliationResult,
) state.DeleteReconciliation {
	record.Status = result.Status
	record.Candidates = result.Candidates
	record.DeletedRows = result.DeletedRows
	record.SkippedRows = result.SkippedRows
	record.Reason = result.Reason
	record.CompletedAt = result.CompletedAt.UTC()
	return record
}
