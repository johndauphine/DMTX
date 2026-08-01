package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode/utf8"
)

// Stage4DeleteJournalReadinessVersion is the wire version of the immutable
// authority that permits a target-owned delete receipt journal to exist before
// ordinary table preparation or data mutations begin. Target schema evolution
// may have been applied and exactly reverified while its aggregate sentinels
// remain pristine.
const Stage4DeleteJournalReadinessVersion = 1

// Stage4DeleteJournalReadinessBackend is an additive state capability for a
// target whose delete receipts depend on a private, auto-committing journal.
// It remains separate from Stage4Backend: a route must explicitly require the
// stronger authority rather than making every existing Stage 4 backend appear
// able to recover that journal boundary.
type Stage4DeleteJournalReadinessBackend interface {
	// ValidateStage4DeleteJournalReadinessBoundary is a read-only exact check
	// of the pre-mutation state boundary. Call it before native journal DDL
	// when no receipt exists; Ensure repeats the same check atomically before
	// saving so a concurrent state mutation cannot bypass the boundary.
	ValidateStage4DeleteJournalReadinessBoundary(Stage4DeleteJournalReadinessBoundary) error
	SaveStage4DeleteJournalReadiness(Stage4DeleteJournalReadiness) error
	EnsureStage4DeleteJournalReadiness(Stage4DeleteJournalReadiness) (Stage4DeleteJournalReadinessReceipt, bool, error)
	LoadStage4DeleteJournalReadiness(string) (Stage4DeleteJournalReadinessReceipt, bool, error)
}

// Stage4DeleteJournalReadinessBoundary identifies the durable work state that
// must be pristine before a target may create its private journal. It contains
// no target catalog details because those facts can be known only after the
// native journal reread; the eventual readiness receipt binds them exactly.
type Stage4DeleteJournalReadinessBoundary struct {
	RunID           string
	InventoryDigest string
}

// Stage4DeleteJournalReadiness is the exact target authority observed after a
// target-owned journal preparer has created or verified its private journal.
// TargetIdentity is the credential-free canonical run identity, never a DSN.
// Its digest, engine/flavor/version digest, exact journal catalog digest, and
// immutable work-inventory digest make an accidental cross-target reuse fail
// closed.
type Stage4DeleteJournalReadiness struct {
	Version                   int       `json:"version" yaml:"version"`
	RunID                     string    `json:"run_id" yaml:"run_id"`
	InventoryDigest           string    `json:"inventory_digest" yaml:"inventory_digest"`
	TargetIdentity            string    `json:"target_identity" yaml:"target_identity"`
	TargetIdentityDigest      string    `json:"target_identity_digest" yaml:"target_identity_digest"`
	TargetEngine              string    `json:"target_engine" yaml:"target_engine"`
	TargetFlavor              string    `json:"target_flavor" yaml:"target_flavor"`
	TargetVersion             string    `json:"target_version" yaml:"target_version"`
	TargetFlavorVersionDigest string    `json:"target_flavor_version_digest" yaml:"target_flavor_version_digest"`
	JournalCatalogDigest      string    `json:"journal_catalog_digest" yaml:"journal_catalog_digest"`
	JournalVersion            int       `json:"journal_version" yaml:"journal_version"`
	ReadyAt                   time.Time `json:"ready_at" yaml:"ready_at"`
}

// Stage4DeleteJournalReadinessReceipt adds a content digest to the versioned
// authority, so YAML replacement and SQLite record storage detect corruption
// or an attempted in-place authority substitution.
type Stage4DeleteJournalReadinessReceipt struct {
	Readiness Stage4DeleteJournalReadiness `json:"readiness" yaml:"readiness"`
	Digest    string                       `json:"digest" yaml:"digest"`
}

// NewStage4DeleteJournalReadiness builds the credential-free authority a
// target preparer returns after it has created or revalidated its private
// journal. Callers provide only canonical target identity and exact catalog
// facts; derived digests are calculated here.
func NewStage4DeleteJournalReadiness(
	runID string,
	inventoryDigest string,
	targetIdentity string,
	targetEngine string,
	targetFlavor string,
	targetVersion string,
	journalCatalogDigest string,
	journalVersion int,
	readyAt time.Time,
) (Stage4DeleteJournalReadiness, error) {
	ready := Stage4DeleteJournalReadiness{
		Version:              Stage4DeleteJournalReadinessVersion,
		RunID:                runID,
		InventoryDigest:      inventoryDigest,
		TargetIdentity:       targetIdentity,
		TargetEngine:         targetEngine,
		TargetFlavor:         targetFlavor,
		TargetVersion:        targetVersion,
		JournalCatalogDigest: journalCatalogDigest,
		JournalVersion:       journalVersion,
		ReadyAt:              readyAt,
	}
	ready.TargetIdentityDigest = stage4DeleteJournalDigestString(
		ready.TargetIdentity,
	)
	flavorDigest, err := stage4DeleteJournalFlavorVersionDigest(
		ready.TargetEngine,
		ready.TargetFlavor,
		ready.TargetVersion,
	)
	if err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	ready.TargetFlavorVersionDigest = flavorDigest
	return normalizeStage4DeleteJournalReadiness(ready)
}

// Clone returns an independent authority value. The authority intentionally
// contains only scalar credential-free fields, but this method makes copying
// an explicit boundary for lifecycle and backend callers.
func (ready Stage4DeleteJournalReadiness) Clone() Stage4DeleteJournalReadiness {
	return ready
}

// Equal compares canonical authorities. Invalid authorities are never equal.
func (ready Stage4DeleteJournalReadiness) Equal(
	other Stage4DeleteJournalReadiness,
) bool {
	left, leftErr := normalizeStage4DeleteJournalReadiness(ready)
	right, rightErr := normalizeStage4DeleteJournalReadiness(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

// Validate verifies that this authority is canonical and self-authenticating.
func (ready Stage4DeleteJournalReadiness) Validate() error {
	_, err := normalizeStage4DeleteJournalReadiness(ready)
	return err
}

// Clone returns an independent immutable receipt value.
func (receipt Stage4DeleteJournalReadinessReceipt) Clone() Stage4DeleteJournalReadinessReceipt {
	receipt.Readiness = receipt.Readiness.Clone()
	return receipt
}

// Equal compares fully validated receipts, including their content digests.
func (receipt Stage4DeleteJournalReadinessReceipt) Equal(
	other Stage4DeleteJournalReadinessReceipt,
) bool {
	left, leftErr := normalizeStoredStage4DeleteJournalReadiness(receipt)
	right, rightErr := normalizeStoredStage4DeleteJournalReadiness(other)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(left, right)
}

// Validate verifies receipt content and digest integrity.
func (receipt Stage4DeleteJournalReadinessReceipt) Validate() error {
	_, err := normalizeStoredStage4DeleteJournalReadiness(receipt)
	return err
}

func normalizeStage4DeleteJournalReadiness(
	ready Stage4DeleteJournalReadiness,
) (Stage4DeleteJournalReadiness, error) {
	if ready.Version != Stage4DeleteJournalReadinessVersion {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"delete-journal readiness version %d is unsupported",
			ready.Version,
		)
	}
	if strings.TrimSpace(ready.RunID) == "" || ready.RunID != strings.TrimSpace(ready.RunID) {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"delete-journal readiness run ID is required",
		)
	}
	var err error
	if ready.InventoryDigest, err = normalizeStage4DeleteJournalDigest(
		"inventory digest",
		ready.InventoryDigest,
	); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if err := validateStage4DeleteJournalTargetIdentity(ready.TargetIdentity); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	identityDigest := stage4DeleteJournalDigestString(ready.TargetIdentity)
	if ready.TargetIdentityDigest, err = normalizeStage4DeleteJournalDigest(
		"target identity digest",
		ready.TargetIdentityDigest,
	); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if ready.TargetIdentityDigest != identityDigest {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"%w: delete-journal readiness target identity digest differs",
			ErrImmutableEvidence,
		)
	}
	if ready.TargetEngine, err = normalizeStage4DeleteJournalEngine(
		ready.TargetEngine,
	); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if ready.TargetFlavor, err = normalizeStage4DeleteJournalToken(
		"target flavor",
		ready.TargetFlavor,
	); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if strings.TrimSpace(ready.TargetVersion) == "" ||
		ready.TargetVersion != strings.TrimSpace(ready.TargetVersion) ||
		!utf8.ValidString(ready.TargetVersion) ||
		len(ready.TargetVersion) > 256 {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"delete-journal readiness target version is invalid",
		)
	}
	flavorDigest, err := stage4DeleteJournalFlavorVersionDigest(
		ready.TargetEngine,
		ready.TargetFlavor,
		ready.TargetVersion,
	)
	if err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if ready.TargetFlavorVersionDigest, err = normalizeStage4DeleteJournalDigest(
		"target flavor/version digest",
		ready.TargetFlavorVersionDigest,
	); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if ready.TargetFlavorVersionDigest != flavorDigest {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"%w: delete-journal readiness target flavor/version digest differs",
			ErrImmutableEvidence,
		)
	}
	if ready.JournalCatalogDigest, err = normalizeStage4DeleteJournalDigest(
		"journal catalog digest",
		ready.JournalCatalogDigest,
	); err != nil {
		return Stage4DeleteJournalReadiness{}, err
	}
	if ready.JournalVersion < 1 || ready.JournalVersion > 65535 {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"delete-journal readiness journal version is invalid",
		)
	}
	if ready.ReadyAt.IsZero() {
		return Stage4DeleteJournalReadiness{}, fmt.Errorf(
			"delete-journal readiness time is required",
		)
	}
	ready.ReadyAt = ready.ReadyAt.UTC()
	return ready, nil
}

func normalizeStage4DeleteJournalReadinessBoundary(
	boundary Stage4DeleteJournalReadinessBoundary,
) (Stage4DeleteJournalReadinessBoundary, error) {
	if strings.TrimSpace(boundary.RunID) == "" ||
		boundary.RunID != strings.TrimSpace(boundary.RunID) {
		return Stage4DeleteJournalReadinessBoundary{}, fmt.Errorf(
			"delete-journal readiness boundary run ID is required",
		)
	}
	var err error
	boundary.InventoryDigest, err = normalizeStage4DeleteJournalDigest(
		"inventory digest",
		boundary.InventoryDigest,
	)
	if err != nil {
		return Stage4DeleteJournalReadinessBoundary{}, err
	}
	return boundary, nil
}

func newStage4DeleteJournalReadinessReceipt(
	ready Stage4DeleteJournalReadiness,
) (Stage4DeleteJournalReadinessReceipt, error) {
	normalized, err := normalizeStage4DeleteJournalReadiness(ready)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, fmt.Errorf(
			"encode delete-journal readiness receipt: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return Stage4DeleteJournalReadinessReceipt{
		Readiness: normalized,
		Digest:    hex.EncodeToString(digest[:]),
	}, nil
}

func normalizeStoredStage4DeleteJournalReadiness(
	stored Stage4DeleteJournalReadinessReceipt,
) (Stage4DeleteJournalReadinessReceipt, error) {
	normalized, err := normalizeStage4DeleteJournalReadiness(stored.Readiness)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, err
	}
	expected, err := newStage4DeleteJournalReadinessReceipt(normalized)
	if err != nil {
		return Stage4DeleteJournalReadinessReceipt{}, err
	}
	if stored.Digest != expected.Digest ||
		!reflect.DeepEqual(stored.Readiness, normalized) {
		return Stage4DeleteJournalReadinessReceipt{}, fmt.Errorf(
			"%w: delete-journal readiness receipt differs",
			ErrImmutableEvidence,
		)
	}
	stored.Readiness = normalized
	return stored, nil
}

func validateStage4DeleteJournalReadinessReceipt(
	stored Stage4DeleteJournalReadinessReceipt,
	requested Stage4DeleteJournalReadiness,
) error {
	normalizedStored, err := normalizeStoredStage4DeleteJournalReadiness(stored)
	if err != nil {
		return err
	}
	normalizedRequested, err := normalizeStage4DeleteJournalReadiness(requested)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(normalizedStored.Readiness, normalizedRequested) {
		return fmt.Errorf(
			"%w: delete-journal readiness authority differs",
			ErrImmutableEvidence,
		)
	}
	return nil
}

func validateStage4DeleteJournalReadinessAuthority(
	inventoryReceipt Stage4TableInventoryReceipt,
	boundary Stage4DeleteJournalReadinessBoundary,
	ordinary []Task,
	workTasks []WorkTask,
	workRanges []RangeState,
) error {
	inventoryReceipt, err := normalizeStoredStage4TableInventory(inventoryReceipt)
	if err != nil {
		return err
	}
	if boundary.RunID != inventoryReceipt.Inventory.RunID ||
		boundary.InventoryDigest != inventoryReceipt.Digest {
		return fmt.Errorf(
			"%w: delete-journal readiness differs from table inventory",
			ErrImmutableEvidence,
		)
	}
	if len(ordinary) != len(inventoryReceipt.Inventory.Tables) {
		return fmt.Errorf(
			"%w: delete-journal readiness ordinary task inventory differs",
			ErrImmutableEvidence,
		)
	}
	ordinaryByTable := make(map[string]Task, len(ordinary))
	for _, task := range ordinary {
		if task.RunID != boundary.RunID {
			return fmt.Errorf(
				"%w: delete-journal readiness ordinary task run differs",
				ErrImmutableEvidence,
			)
		}
		if _, duplicate := ordinaryByTable[task.Table]; duplicate {
			return fmt.Errorf(
				"%w: duplicate delete-journal readiness ordinary task %q",
				ErrImmutableEvidence,
				task.Table,
			)
		}
		ordinaryByTable[task.Table] = task
	}
	if err := validateStage4KnownWorkInventory(workTasks); err != nil {
		return err
	}
	taskIndexes, rangesByTask, err := indexStage4AggregateWork(
		boundary.RunID,
		workTasks,
		workRanges,
	)
	if err != nil {
		return err
	}
	if err := validateStage4DeleteJournalReadinessSentinels(
		inventoryReceipt.Inventory,
		workTasks,
		rangesByTask,
	); err != nil {
		return err
	}
	expected := make(map[TaskKey]struct{}, len(inventoryReceipt.Inventory.Tables))
	for _, entry := range inventoryReceipt.Inventory.Tables {
		expected[entry.Task] = struct{}{}
		ordinaryTask, found := ordinaryByTable[entry.Table]
		if !found || ordinaryTask.Status != "running" ||
			ordinaryTask.RowsDone != 0 || !ordinaryTask.CompletedAt.IsZero() ||
			ordinaryTask.StartedAt.IsZero() ||
			ordinaryTask.IntegerWatermark != nil ||
			ordinaryTask.RowNumberWatermark != nil {
			return fmt.Errorf(
				"%w: delete-journal readiness ordinary task %q is not pristine",
				ErrStateTransition,
				entry.Table,
			)
		}
		index, found := taskIndexes[entry.Task]
		if !found {
			return fmt.Errorf(
				"%w: delete-journal readiness structured task %#v",
				ErrUnknownWork,
				entry.Task,
			)
		}
		workTask := workTasks[index]
		if workTask.Strategy != entry.Strategy ||
			workTask.TopologyHash != entry.TopologyHash ||
			workTask.Status != "running" ||
			workTask.StartedAt.IsZero() || workTask.UpdatedAt.IsZero() ||
			workTask.UpdatedAt.Before(workTask.StartedAt) ||
			workTask.Attempts != 0 || workTask.Retries != 0 ||
			workTask.Error != "" || !workTask.CompletedAt.IsZero() {
			return fmt.Errorf(
				"%w: delete-journal readiness structured task %#v is not pristine",
				ErrStateTransition,
				entry.Task,
			)
		}
		ranges := rangesByTask[entry.Task]
		if len(ranges) != len(entry.Ranges) {
			return fmt.Errorf(
				"%w: delete-journal readiness ranges for table %q differ",
				ErrImmutableEvidence,
				entry.Table,
			)
		}
		for index, plannedRange := range entry.Ranges {
			workRange := ranges[index]
			if workRange.ID != plannedRange.ID ||
				workRange.Strategy != entry.Strategy ||
				workRange.TopologyHash != entry.TopologyHash ||
				workRange.Status != "running" ||
				workRange.NextSequence != 0 ||
				workRange.SequenceOffset != 0 ||
				workRange.RowsDone != 0 ||
				workRange.CommittedPrefix != 0 ||
				workRange.Attempts != 0 || workRange.Retries != 0 ||
				workRange.Error != "" || len(workRange.Frontier) != 0 ||
				workRange.FrontierValid || len(workRange.Pending) != 0 ||
				workRange.UpdatedAt.IsZero() || !workRange.CompletedAt.IsZero() {
				return fmt.Errorf(
					"%w: delete-journal readiness range %q for table %q is not pristine",
					ErrStateTransition,
					plannedRange.ID,
					entry.Table,
				)
			}
		}
	}
	for key := range taskIndexes {
		if !stage4TableWorkType(key.Type) {
			continue
		}
		if _, found := expected[key]; !found {
			return fmt.Errorf(
				"%w: delete-journal readiness has unexpected structured task %#v",
				ErrImmutableEvidence,
				key,
			)
		}
	}
	return nil
}

// validateStage4DeleteJournalReadinessSentinels keeps the private-journal
// boundary before aggregate sentinel completion and ordinary table/data work.
// Schema snapshots may already be staged and physical target schema evolution
// may have been exactly reverified, but the sentinel work/range records must
// still be the untouched plans that inventory authenticated. A target-shape
// sentinel, when evolution prepared one, shares the gate topology because it
// is bound to the same source contract and target mode.
func validateStage4DeleteJournalReadinessSentinels(
	inventory Stage4TableInventory,
	workTasks []WorkTask,
	rangesByTask map[TaskKey][]RangeState,
) error {
	schemaFound := false
	for _, task := range workTasks {
		rangeID, strategy, sentinel := stage4SentinelDefinition(task.Key)
		if !sentinel {
			continue
		}
		if task.Key == inventory.SchemaTask {
			schemaFound = true
		}
		if task.RunID != inventory.RunID ||
			task.Strategy != strategy ||
			task.TopologyHash != inventory.SchemaTopologyHash ||
			task.Status != "running" || task.StartedAt.IsZero() ||
			task.UpdatedAt.IsZero() || task.UpdatedAt.Before(task.StartedAt) ||
			task.Attempts != 0 || task.Retries != 0 || task.Error != "" ||
			!task.CompletedAt.IsZero() {
			return fmt.Errorf(
				"%w: delete-journal readiness sentinel %#v is not pristine",
				ErrStateTransition,
				task.Key,
			)
		}
		ranges := rangesByTask[task.Key]
		if len(ranges) != 1 {
			return fmt.Errorf(
				"%w: delete-journal readiness sentinel %#v range inventory differs",
				ErrImmutableEvidence,
				task.Key,
			)
		}
		workRange := ranges[0]
		if workRange.RunID != inventory.RunID ||
			workRange.Task != task.Key || workRange.ID != rangeID ||
			workRange.Strategy != strategy ||
			workRange.TopologyHash != inventory.SchemaTopologyHash ||
			workRange.Status != "running" ||
			len(workRange.Lower) != 0 || len(workRange.Upper) != 0 ||
			workRange.LowerInclusive || workRange.UpperInclusive ||
			workRange.FirstRow != 0 || workRange.LastRow != 0 ||
			len(workRange.Frontier) != 0 || workRange.FrontierValid ||
			workRange.NextSequence != 0 || workRange.SequenceOffset != 0 ||
			workRange.RowsDone != 0 || workRange.RowsTotal != 0 ||
			workRange.CommittedPrefix != 0 || workRange.Attempts != 0 ||
			workRange.Retries != 0 || workRange.Error != "" ||
			len(workRange.Pending) != 0 || workRange.UpdatedAt.IsZero() ||
			!workRange.CompletedAt.IsZero() {
			return fmt.Errorf(
				"%w: delete-journal readiness sentinel range %q is not pristine",
				ErrStateTransition,
				workRange.ID,
			)
		}
	}
	if !schemaFound {
		return fmt.Errorf(
			"%w: delete-journal readiness schema sentinel is missing",
			ErrUnknownWork,
		)
	}
	return nil
}

func normalizeStage4DeleteJournalDigest(
	label string,
	value string,
) (string, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size {
		return "", fmt.Errorf(
			"delete-journal readiness %s must be a SHA-256 digest",
			label,
		)
	}
	return strings.ToLower(value), nil
}

func stage4DeleteJournalDigestString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func stage4DeleteJournalFlavorVersionDigest(
	engine string,
	flavor string,
	version string,
) (string, error) {
	payload, err := json.Marshal(struct {
		Engine  string `json:"engine"`
		Flavor  string `json:"flavor"`
		Version string `json:"version"`
	}{
		Engine: engine, Flavor: flavor, Version: version,
	})
	if err != nil {
		return "", fmt.Errorf(
			"encode delete-journal target flavor/version authority: %w",
			err,
		)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validateStage4DeleteJournalTargetIdentity(identity string) error {
	if strings.TrimSpace(identity) == "" || identity != strings.TrimSpace(identity) ||
		!utf8.ValidString(identity) || len(identity) > 4096 {
		return fmt.Errorf("delete-journal readiness target identity is invalid")
	}
	for _, character := range identity {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("delete-journal readiness target identity contains control data")
		}
	}
	lower := strings.ToLower(identity)
	for _, marker := range []string{
		"://", "password=", "passwd=", "pwd=", "token=", "secret=",
	} {
		if strings.Contains(lower, marker) {
			return fmt.Errorf(
				"delete-journal readiness target identity must be credential-free",
			)
		}
	}
	return nil
}

func normalizeStage4DeleteJournalEngine(engine string) (string, error) {
	if strings.TrimSpace(engine) == "" || engine != strings.TrimSpace(engine) ||
		engine != strings.ToLower(engine) {
		return "", fmt.Errorf("delete-journal readiness target engine is invalid")
	}
	switch engine {
	case "postgres", "mysql", "mssql", "sqlite", "clickhouse":
		return engine, nil
	default:
		return "", fmt.Errorf(
			"delete-journal readiness target engine %q is unsupported",
			engine,
		)
	}
}

func normalizeStage4DeleteJournalToken(label string, value string) (string, error) {
	if strings.TrimSpace(value) == "" || value != strings.TrimSpace(value) ||
		value != strings.ToLower(value) || len(value) > 128 {
		return "", fmt.Errorf("delete-journal readiness %s is invalid", label)
	}
	for index, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return "", fmt.Errorf(
			"delete-journal readiness %s character %d is invalid",
			label,
			index,
		)
	}
	return value, nil
}
