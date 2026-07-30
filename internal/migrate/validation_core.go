package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// ValidationPass is one deterministic pass in an inclusive validation mode.
type ValidationPass string

const (
	ValidationPassCount      ValidationPass = "count"
	ValidationPassNullParity ValidationPass = "null_parity"
	ValidationPassSample     ValidationPass = "sample"
)

// ValidationPlan is the normalized, inclusive sequence of validation passes.
// Passes are ordered from the least expensive evidence to the deepest.
type ValidationPlan struct {
	Mode   config.ValidationMode `json:"mode"`
	Passes []ValidationPass      `json:"passes"`
}

// BuildValidationPlan normalizes an omitted mode to count_only and rejects
// full explicitly. Full must never silently weaken to a shallower mode.
func BuildValidationPlan(mode config.ValidationMode) (ValidationPlan, error) {
	if mode == "" {
		mode = config.ValidationCountOnly
	}
	plan := ValidationPlan{Mode: mode}
	switch mode {
	case config.ValidationCountOnly:
		plan.Passes = []ValidationPass{ValidationPassCount}
	case config.ValidationNullParity:
		plan.Passes = []ValidationPass{
			ValidationPassCount,
			ValidationPassNullParity,
		}
	case config.ValidationSample:
		plan.Passes = []ValidationPass{
			ValidationPassCount,
			ValidationPassNullParity,
			ValidationPassSample,
		}
	case config.ValidationFull:
		return ValidationPlan{}, fmt.Errorf(
			"validation mode full is reserved and unsupported",
		)
	default:
		return ValidationPlan{}, fmt.Errorf(
			"invalid validation mode %q",
			mode,
		)
	}
	return plan, nil
}

type ValidationSide string

const (
	ValidationSource ValidationSide = "source"
	ValidationTarget ValidationSide = "target"
)

type ValidationSeverity string

const (
	ValidationSeverityInfo    ValidationSeverity = "info"
	ValidationSeverityWarning ValidationSeverity = "warning"
	ValidationSeverityError   ValidationSeverity = "error"
)

type ValidationOutcome string

const (
	ValidationOutcomePass        ValidationOutcome = "pass"
	ValidationOutcomeMismatch    ValidationOutcome = "mismatch"
	ValidationOutcomeTimeout     ValidationOutcome = "timeout"
	ValidationOutcomeUnavailable ValidationOutcome = "unavailable"
)

type ValidationEvidenceKind string

const (
	ValidationEvidenceExact          ValidationEvidenceKind = "exact"
	ValidationEvidenceEstimate       ValidationEvidenceKind = "estimate"
	ValidationEvidenceStrictSnapshot ValidationEvidenceKind = "strict_snapshot"
)

// CoreValidationFinding is a stable, value-safe validation fact. It may contain
// counts, but it never contains sampled keys or row values.
type CoreValidationFinding struct {
	Schema         string                 `json:"schema,omitempty"`
	TableName      string                 `json:"table_name"`
	Table          string                 `json:"table"`
	Check          string                 `json:"check"`
	Side           ValidationSide         `json:"side,omitempty"`
	Column         string                 `json:"column,omitempty"`
	Sample         int                    `json:"sample,omitempty"`
	Severity       ValidationSeverity     `json:"severity"`
	Outcome        ValidationOutcome      `json:"outcome"`
	Message        string                 `json:"message"`
	Remedy         string                 `json:"remedy,omitempty"`
	SourceRows     *int64                 `json:"source_rows,omitempty"`
	TargetRows     *int64                 `json:"target_rows,omitempty"`
	SourceEvidence ValidationEvidenceKind `json:"source_evidence,omitempty"`
	TargetEvidence ValidationEvidenceKind `json:"target_evidence,omitempty"`
}

// CoreValidationReport contains findings in stable table and pass order.
type CoreValidationReport struct {
	Mode     config.ValidationMode   `json:"mode"`
	Passed   bool                    `json:"passed"`
	Findings []CoreValidationFinding `json:"findings"`
}

// ValidationCoreOptions carries policy which affects pass/fail without changing
// the observed facts.
type ValidationCoreOptions struct {
	Mode                   config.ValidationMode
	TargetMode             string
	FailOnMismatch         bool
	FailOnTimeout          bool
	FailOnEstimateMismatch bool
	ExactCountTimeout      time.Duration
	TableTimeout           time.Duration
	TableConcurrency       int
	SampleLimit            int
}

// ValidationTableSpec binds the effective validation table to its exact
// transferred projection. Table must already reflect schema-contract decisions:
// a discard_value column is removed from both Table.Columns and Projection.
// Requiring Projection to cover that effective table prevents an accidental
// omission from silently weakening deep validation. StrictSourceRows, when
// present, is persisted strict-snapshot evidence and replaces a new live
// source count. ReconciliationStrict is the effective result for this table,
// not a migration-wide policy. PrimaryKeyEqualityProof is a lowercase SHA-256
// digest over the route, workload identity, ordered primary-key metadata, and
// adapter-certified source/target equality semantics. It is required when a
// non-strict upsert NULL pass scopes the target to source-owned primary keys.
type ValidationTableSpec struct {
	Table                   schema.Table
	Projection              []string
	StrictSourceRows        *int64
	ReconciliationStrict    bool
	PrimaryKeyEqualityProof string
}

// ValidationSampleRow contains one complete projected row. Values are
// process-local and must not be logged, persisted, audited, or sent to an
// advisory service.
type ValidationSampleRow struct {
	Values []any
}

// ValidationPrimaryKey contains one complete source primary key used to fetch
// the corresponding target row. Values have the same process-local restriction
// as ValidationSampleRow.
type ValidationPrimaryKey struct {
	Values []any
}

type ValidationNullScopeKind string

const (
	// ValidationNullScopeTransferredSource is the complete transferred source
	// view for one table.
	ValidationNullScopeTransferredSource ValidationNullScopeKind = "transferred_source"
	// ValidationNullScopeWholeTarget is every row in the selected target table.
	ValidationNullScopeWholeTarget ValidationNullScopeKind = "whole_target"
	// ValidationNullScopeTargetSourcePrimaryKeys is the target subset whose
	// complete primary keys exist in the transferred source view.
	ValidationNullScopeTargetSourcePrimaryKeys ValidationNullScopeKind = "target_source_primary_keys"
)

// ValidationNullScope makes the ownership of NULL-count rows explicit.
// PrimaryKeyColumns and EqualityProofDigest are populated only for a
// source-owned target subset.
type ValidationNullScope struct {
	Kind                ValidationNullScopeKind `json:"kind"`
	PrimaryKeyColumns   []string                `json:"primary_key_columns,omitempty"`
	EqualityProofDigest string                  `json:"equality_proof_digest,omitempty"`
}

// ValidationNullCountEvidence echoes the requested scope and its exact row
// cardinality. Counts must contain every projected column and no others.
type ValidationNullCountEvidence struct {
	Scope  ValidationNullScope `json:"scope"`
	Rows   int64               `json:"rows"`
	Counts map[string]int64    `json:"counts"`
}

// ValidationCoreProbe is the small adapter seam for deterministic validation.
// Every method must honor context cancellation. All source methods for one
// table must observe the same source view when strict consistency is active.
// SampleSourceRows must select at most limit rows in strictly increasing
// complete-primary-key order under the typed validation ordering. The core
// verifies and preserves that order; route certification must prove the
// first-N selection query uses matching binary text/byte semantics.
// SampleTargetRows must fetch only the requested keys and may return them in
// any order.
type ValidationCoreProbe interface {
	ExactCount(context.Context, ValidationSide, schema.Table) (int64, error)
	EstimateCount(context.Context, ValidationSide, schema.Table) (int64, error)
	NullCounts(
		context.Context,
		ValidationSide,
		schema.Table,
		[]string,
		ValidationNullScope,
	) (ValidationNullCountEvidence, error)
	SampleSourceRows(
		context.Context,
		schema.Table,
		[]string,
		int,
	) ([]ValidationSampleRow, error)
	SampleTargetRows(
		context.Context,
		schema.Table,
		[]string,
		[]ValidationPrimaryKey,
	) ([]ValidationSampleRow, error)
}

const (
	maxValidationTableConcurrency = 64
	maxValidationSampleRows       = 10_000
)

// RunValidationCore executes validation without mutating either endpoint.
// Runtime probe failures are returned as structured findings; error is reserved
// for an invalid plan or invocation.
func RunValidationCore(
	ctx context.Context,
	options ValidationCoreOptions,
	tables []ValidationTableSpec,
	probe ValidationCoreProbe,
) (CoreValidationReport, error) {
	plan, err := BuildValidationPlan(options.Mode)
	if err != nil {
		return CoreValidationReport{}, err
	}
	if probe == nil {
		return CoreValidationReport{}, fmt.Errorf("validation probe is required")
	}
	if options.TargetMode == "" {
		options.TargetMode = "drop_recreate"
	}
	switch options.TargetMode {
	case "drop_recreate", "upsert":
	default:
		return CoreValidationReport{}, fmt.Errorf(
			"invalid validation target mode %q",
			options.TargetMode,
		)
	}
	if options.ExactCountTimeout <= 0 {
		return CoreValidationReport{}, fmt.Errorf(
			"exact count timeout must be positive",
		)
	}
	if options.TableTimeout <= 0 {
		return CoreValidationReport{}, fmt.Errorf(
			"validation table timeout must be positive",
		)
	}
	if options.TableConcurrency <= 0 ||
		options.TableConcurrency > maxValidationTableConcurrency {
		return CoreValidationReport{}, fmt.Errorf(
			"validation table concurrency must be between 1 and %d",
			maxValidationTableConcurrency,
		)
	}
	if planIncludes(plan, ValidationPassSample) &&
		(options.SampleLimit <= 0 ||
			options.SampleLimit > maxValidationSampleRows) {
		return CoreValidationReport{}, fmt.Errorf(
			"validation sample limit must be between 1 and %d",
			maxValidationSampleRows,
		)
	}

	ordered := append([]ValidationTableSpec(nil), tables...)
	for _, spec := range ordered {
		if strings.TrimSpace(spec.Table.Name) == "" {
			return CoreValidationReport{}, fmt.Errorf(
				"validation table name is required",
			)
		}
	}
	sort.SliceStable(ordered, func(left, right int) bool {
		if ordered[left].Table.Schema != ordered[right].Table.Schema {
			return ordered[left].Table.Schema < ordered[right].Table.Schema
		}
		return ordered[left].Table.Name < ordered[right].Table.Name
	})
	for index := 1; index < len(ordered); index++ {
		current := ordered[index].Table
		previous := ordered[index-1].Table
		if current.Schema == previous.Schema &&
			current.Name == previous.Name {
			return CoreValidationReport{}, fmt.Errorf(
				"duplicate validation table %q",
				validationTableName(current),
			)
		}
	}

	results := make([][]CoreValidationFinding, len(ordered))
	if len(ordered) != 0 {
		workers := options.TableConcurrency
		if workers > len(ordered) {
			workers = len(ordered)
		}
		jobs := make(chan int)
		var group sync.WaitGroup
		group.Add(workers)
		for worker := 0; worker < workers; worker++ {
			go func() {
				defer group.Done()
				for index := range jobs {
					tableCtx, cancel := context.WithTimeout(
						ctx,
						options.TableTimeout,
					)
					results[index] = runValidationTable(
						tableCtx,
						options,
						plan,
						ordered[index],
						probe,
					)
					cancel()
				}
			}()
		}
		for index := range ordered {
			jobs <- index
		}
		close(jobs)
		group.Wait()
	}

	report := CoreValidationReport{
		Mode:     plan.Mode,
		Passed:   true,
		Findings: make([]CoreValidationFinding, 0),
	}
	for tableIndex, tableFindings := range results {
		for _, finding := range tableFindings {
			finding.Schema = ordered[tableIndex].Table.Schema
			finding.TableName = ordered[tableIndex].Table.Name
			finding.Table = validationTableName(ordered[tableIndex].Table)
			if finding.Severity == ValidationSeverityError {
				report.Passed = false
			}
			report.Findings = append(report.Findings, finding)
		}
	}
	return report, nil
}

func planIncludes(plan ValidationPlan, pass ValidationPass) bool {
	for _, candidate := range plan.Passes {
		if candidate == pass {
			return true
		}
	}
	return false
}

func validationTableName(table schema.Table) string {
	if table.Schema == "" {
		return validationDisplayIdentifier(table.Name)
	}
	return validationDisplayIdentifier(table.Schema) + "." +
		validationDisplayIdentifier(table.Name)
}

func validationDisplayIdentifier(value string) string {
	for index, character := range value {
		if character == '_' ||
			character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			index > 0 && character >= '0' && character <= '9' {
			continue
		}
		return strconv.Quote(value)
	}
	if value == "" {
		return strconv.Quote(value)
	}
	return value
}

type validationCountEvidence struct {
	value     int64
	kind      ValidationEvidenceKind
	available bool
}

func runValidationTable(
	ctx context.Context,
	options ValidationCoreOptions,
	plan ValidationPlan,
	spec ValidationTableSpec,
	probe ValidationCoreProbe,
) []CoreValidationFinding {
	tableName := validationTableName(spec.Table)
	findings, sourceCount, targetCount := runValidationCounts(
		ctx,
		options,
		spec,
		probe,
	)
	if !planIncludes(plan, ValidationPassNullParity) {
		return findings
	}

	descriptor, projectionErr := validateValidationCoreProjection(
		spec.Table,
		spec.Projection,
		planIncludes(plan, ValidationPassSample),
	)
	if projectionErr != nil {
		findings = append(findings, CoreValidationFinding{
			Table:    tableName,
			Check:    "validation.projection",
			Severity: ValidationSeverityError,
			Outcome:  ValidationOutcomeUnavailable,
			Message:  "deep validation projection is incomplete or invalid",
			Remedy:   "rebuild the validation projection from complete discovered table metadata",
		})
		return findings
	}

	findings = append(
		findings,
		runValidationNullParity(
			ctx,
			options,
			spec,
			sourceCount,
			targetCount,
			probe,
		)...,
	)
	if !planIncludes(plan, ValidationPassSample) {
		return findings
	}
	findings = append(
		findings,
		runValidationSample(
			ctx,
			options,
			spec,
			descriptor,
			sourceCount,
			probe,
		)...,
	)
	return findings
}

type validationSideCountResult struct {
	side            ValidationSide
	value           int64
	err             error
	deadlineExpired bool
}

func runValidationCounts(
	ctx context.Context,
	options ValidationCoreOptions,
	spec ValidationTableSpec,
	probe ValidationCoreProbe,
) (
	[]CoreValidationFinding,
	validationCountEvidence,
	validationCountEvidence,
) {
	tableName := validationTableName(spec.Table)
	findings := make([]CoreValidationFinding, 0, 7)
	evidence := map[ValidationSide]validationCountEvidence{}
	exactResults := make(map[ValidationSide]validationSideCountResult, 2)

	sides := []ValidationSide{ValidationSource, ValidationTarget}
	resultChannel := make(chan validationSideCountResult, len(sides))
	expectedResults := 0
	if spec.StrictSourceRows != nil {
		value := *spec.StrictSourceRows
		if value < 0 {
			findings = append(findings, CoreValidationFinding{
				Table:    tableName,
				Check:    "validation.count.strict_snapshot",
				Side:     ValidationSource,
				Severity: ValidationSeverityError,
				Outcome:  ValidationOutcomeUnavailable,
				Message:  "persisted strict-snapshot source count is invalid",
				Remedy:   "repair or recapture strict-snapshot validation evidence",
			})
		} else {
			evidence[ValidationSource] = validationCountEvidence{
				value: value, kind: ValidationEvidenceStrictSnapshot,
				available: true,
			}
			findings = append(findings, countSideFinding(
				tableName,
				ValidationSource,
				"validation.count.strict_snapshot",
				ValidationSeverityInfo,
				ValidationOutcomePass,
				"persisted strict-snapshot source count is authoritative",
				value,
				ValidationEvidenceStrictSnapshot,
			))
		}
	} else {
		expectedResults++
		go func() {
			exactCtx, cancel := context.WithTimeout(
				ctx,
				options.ExactCountTimeout,
			)
			value, err := probe.ExactCount(
				exactCtx,
				ValidationSource,
				spec.Table,
			)
			deadlineExpired := validationDeadlineExpired(exactCtx, err)
			cancel()
			resultChannel <- validationSideCountResult{
				side: ValidationSource, value: value, err: err,
				deadlineExpired: deadlineExpired,
			}
		}()
	}
	expectedResults++
	go func() {
		exactCtx, cancel := context.WithTimeout(
			ctx,
			options.ExactCountTimeout,
		)
		value, err := probe.ExactCount(
			exactCtx,
			ValidationTarget,
			spec.Table,
		)
		deadlineExpired := validationDeadlineExpired(exactCtx, err)
		cancel()
		resultChannel <- validationSideCountResult{
			side: ValidationTarget, value: value, err: err,
			deadlineExpired: deadlineExpired,
		}
	}()
	for result := 0; result < expectedResults; result++ {
		item := <-resultChannel
		exactResults[item.side] = item
	}
	timedOut := make(map[ValidationSide]bool, 2)
	for _, side := range sides {
		if side == ValidationSource && spec.StrictSourceRows != nil {
			continue
		}
		result := exactResults[side]
		switch {
		case result.deadlineExpired:
			timedOut[side] = true
			severity := ValidationSeverityError
			if !options.FailOnTimeout {
				severity = ValidationSeverityWarning
			}
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.count.exact",
				Side: side, Severity: severity,
				Outcome: ValidationOutcomeTimeout,
				Message: fmt.Sprintf("exact %s count timed out", side),
				Remedy:  "increase the exact-count timeout or use explicit log-only timeout policy",
			})
		case result.err == nil && result.value >= 0:
			evidence[side] = validationCountEvidence{
				value: result.value, kind: ValidationEvidenceExact,
				available: true,
			}
			findings = append(findings, countSideFinding(
				tableName,
				side,
				"validation.count.exact",
				ValidationSeverityInfo,
				ValidationOutcomePass,
				fmt.Sprintf("exact %s count collected", side),
				result.value,
				ValidationEvidenceExact,
			))
		case result.err == nil:
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.count.exact",
				Side: side, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: fmt.Sprintf(
					"exact %s count evidence is invalid",
					side,
				),
				Remedy: "inspect the engine count adapter and retry validation",
			})
		default:
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.count.exact",
				Side: side, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: fmt.Sprintf("exact %s count failed", side),
				Remedy:  "restore count-query access and retry validation",
			})
		}
	}

	estimateResults := readValidationEstimates(
		ctx,
		spec.Table,
		timedOut,
		probe,
	)
	for _, side := range sides {
		if !timedOut[side] {
			continue
		}
		result := estimateResults[side]
		if result.deadlineExpired || result.err != nil || result.value < 0 {
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.count.estimate",
				Side: side, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: fmt.Sprintf(
					"fallback %s count estimate is unavailable",
					side,
				),
				Remedy: "restore estimate-query access or obtain an exact count",
			})
			continue
		}
		evidence[side] = validationCountEvidence{
			value: result.value, kind: ValidationEvidenceEstimate,
			available: true,
		}
		findings = append(findings, countSideFinding(
			tableName,
			side,
			"validation.count.estimate",
			ValidationSeverityInfo,
			ValidationOutcomePass,
			fmt.Sprintf("fallback %s count estimate collected", side),
			result.value,
			ValidationEvidenceEstimate,
		))
	}

	source := evidence[ValidationSource]
	target := evidence[ValidationTarget]
	if !source.available || !target.available {
		findings = append(findings, CoreValidationFinding{
			Table: tableName, Check: "validation.count.compare",
			Severity: ValidationSeverityError,
			Outcome:  ValidationOutcomeUnavailable,
			Message:  "row-count comparison lacks complete source and target evidence",
			Remedy:   "restore count evidence for both sides and retry validation",
		})
		return findings, source, target
	}

	match := validationCountsMatch(
		options.TargetMode,
		spec.ReconciliationStrict,
		source.value,
		target.value,
	)
	finding := CoreValidationFinding{
		Table: tableName, Check: "validation.count.compare",
		Severity: ValidationSeverityInfo,
		Outcome:  ValidationOutcomePass,
		Message: validationCountMatchMessage(
			options.TargetMode,
			spec.ReconciliationStrict,
		),
		SourceRows:     int64Pointer(source.value),
		TargetRows:     int64Pointer(target.value),
		SourceEvidence: source.kind,
		TargetEvidence: target.kind,
	}
	if !match {
		finding.Outcome = ValidationOutcomeMismatch
		finding.Message = validationCountMismatchMessage(
			options.TargetMode,
			spec.ReconciliationStrict,
		)
		usesEstimate := source.kind == ValidationEvidenceEstimate ||
			target.kind == ValidationEvidenceEstimate
		fail := options.FailOnMismatch
		if usesEstimate {
			fail = options.FailOnEstimateMismatch
			finding.Remedy = "obtain exact counts or use explicit log-only estimate-mismatch policy"
		} else {
			finding.Remedy = "repair the target row set or use explicit log-only mismatch policy"
		}
		if fail {
			finding.Severity = ValidationSeverityError
		} else {
			finding.Severity = ValidationSeverityWarning
		}
	}
	findings = append(findings, finding)
	return findings, source, target
}

func readValidationEstimates(
	ctx context.Context,
	table schema.Table,
	timedOut map[ValidationSide]bool,
	probe ValidationCoreProbe,
) map[ValidationSide]validationSideCountResult {
	results := make(map[ValidationSide]validationSideCountResult, 2)
	resultChannel := make(chan validationSideCountResult, 2)
	expected := 0
	for _, side := range []ValidationSide{ValidationSource, ValidationTarget} {
		if !timedOut[side] {
			continue
		}
		expected++
		go func(side ValidationSide) {
			value, err := probe.EstimateCount(ctx, side, table)
			deadlineExpired := validationDeadlineExpired(ctx, err)
			resultChannel <- validationSideCountResult{
				side: side, value: value, err: err,
				deadlineExpired: deadlineExpired,
			}
		}(side)
	}
	for result := 0; result < expected; result++ {
		item := <-resultChannel
		results[item.side] = item
	}
	return results
}

func countSideFinding(
	table string,
	side ValidationSide,
	check string,
	severity ValidationSeverity,
	outcome ValidationOutcome,
	message string,
	value int64,
	kind ValidationEvidenceKind,
) CoreValidationFinding {
	finding := CoreValidationFinding{
		Table: table, Check: check, Side: side, Severity: severity,
		Outcome: outcome, Message: message,
	}
	if side == ValidationSource {
		finding.SourceRows = int64Pointer(value)
		finding.SourceEvidence = kind
	} else {
		finding.TargetRows = int64Pointer(value)
		finding.TargetEvidence = kind
	}
	return finding
}

func int64Pointer(value int64) *int64 {
	return &value
}

func validationDeadlineExpired(ctx context.Context, err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(ctx.Err(), context.DeadlineExceeded)
}

func validationCountsMatch(
	targetMode string,
	reconciliationStrict bool,
	sourceRows int64,
	targetRows int64,
) bool {
	if targetMode == "upsert" && !reconciliationStrict {
		return targetRows >= sourceRows
	}
	return targetRows == sourceRows
}

func validationCountMatchMessage(
	targetMode string,
	reconciliationStrict bool,
) string {
	if targetMode == "upsert" && !reconciliationStrict {
		return "target row count contains the source row set"
	}
	return "source and target row counts are equal"
}

func validationCountMismatchMessage(
	targetMode string,
	reconciliationStrict bool,
) string {
	if targetMode == "upsert" && !reconciliationStrict {
		return "target row count is smaller than the source row count"
	}
	return "source and target row counts differ"
}

type validationNullResult struct {
	side            ValidationSide
	evidence        ValidationNullCountEvidence
	err             error
	deadlineExpired bool
}

func validationNullScopes(
	options ValidationCoreOptions,
	spec ValidationTableSpec,
) (ValidationNullScope, ValidationNullScope, error) {
	source := ValidationNullScope{
		Kind: ValidationNullScopeTransferredSource,
	}
	target := ValidationNullScope{
		Kind: ValidationNullScopeWholeTarget,
	}
	if options.TargetMode != "upsert" ||
		spec.ReconciliationStrict {
		return source, target, nil
	}
	if !validValidationEqualityProofDigest(
		spec.PrimaryKeyEqualityProof,
	) {
		return ValidationNullScope{}, ValidationNullScope{}, fmt.Errorf(
			"route-bound primary-key equality proof is required",
		)
	}
	primaryKey, err := validationNullScopePrimaryKey(spec.Table)
	if err != nil {
		return ValidationNullScope{}, ValidationNullScope{}, err
	}
	target = ValidationNullScope{
		Kind:                ValidationNullScopeTargetSourcePrimaryKeys,
		PrimaryKeyColumns:   primaryKey,
		EqualityProofDigest: spec.PrimaryKeyEqualityProof,
	}
	return source, target, nil
}

func validationNullScopePrimaryKey(table schema.Table) ([]string, error) {
	type positionedColumn struct {
		name     string
		position int
	}
	keys := make([]positionedColumn, 0)
	positions := make(map[int]string)
	for _, column := range table.Columns {
		if !column.PrimaryKey {
			if column.PrimaryKeyPosition != 0 {
				return nil, fmt.Errorf(
					"non-primary-key column %s has a primary-key position",
					column.Name,
				)
			}
			continue
		}
		if column.Nullable {
			return nil, fmt.Errorf(
				"primary-key column %s is nullable",
				column.Name,
			)
		}
		if column.PrimaryKeyPosition <= 0 {
			return nil, fmt.Errorf(
				"primary-key column %s has no positive position",
				column.Name,
			)
		}
		if previous, exists := positions[column.PrimaryKeyPosition]; exists {
			return nil, fmt.Errorf(
				"primary-key position %d is shared by %s and %s",
				column.PrimaryKeyPosition,
				previous,
				column.Name,
			)
		}
		kind, err := validationKindForColumn(column)
		if err != nil || !validationNullScopeKeyKindSupported(kind) {
			return nil, fmt.Errorf(
				"primary-key column %s has an unsupported NULL-scope type",
				column.Name,
			)
		}
		positions[column.PrimaryKeyPosition] = column.Name
		keys = append(keys, positionedColumn{
			name: column.Name, position: column.PrimaryKeyPosition,
		})
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("primary key is required")
	}
	sort.Slice(keys, func(left, right int) bool {
		return keys[left].position < keys[right].position
	})
	result := make([]string, len(keys))
	for index, key := range keys {
		if key.position != index+1 {
			return nil, fmt.Errorf(
				"primary-key positions are not contiguous from one",
			)
		}
		result[index] = key.name
	}
	return result, nil
}

func validationNullScopeKeyKindSupported(kind validationValueKind) bool {
	switch kind {
	case validationBoolean,
		validationInteger,
		validationDecimal,
		validationText,
		validationBytes,
		validationDate,
		validationTime,
		validationTimestamp,
		validationUUID:
		return true
	default:
		return false
	}
}

func validValidationEqualityProofDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			if character < 'a' || character > 'f' {
				return false
			}
		}
	}
	return true
}

func cloneValidationNullScope(scope ValidationNullScope) ValidationNullScope {
	return ValidationNullScope{
		Kind:                scope.Kind,
		EqualityProofDigest: scope.EqualityProofDigest,
		PrimaryKeyColumns: append(
			[]string(nil),
			scope.PrimaryKeyColumns...,
		),
	}
}

func sameValidationNullScope(
	left ValidationNullScope,
	right ValidationNullScope,
) bool {
	if left.Kind != right.Kind ||
		left.EqualityProofDigest != right.EqualityProofDigest ||
		len(left.PrimaryKeyColumns) != len(right.PrimaryKeyColumns) {
		return false
	}
	for index := range left.PrimaryKeyColumns {
		if left.PrimaryKeyColumns[index] != right.PrimaryKeyColumns[index] {
			return false
		}
	}
	return true
}

func runValidationNullParity(
	ctx context.Context,
	options ValidationCoreOptions,
	spec ValidationTableSpec,
	sourceCount validationCountEvidence,
	targetCount validationCountEvidence,
	probe ValidationCoreProbe,
) []CoreValidationFinding {
	tableName := validationTableName(spec.Table)
	sourceScope, targetScope, err := validationNullScopes(
		options,
		spec,
	)
	if err != nil {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.null_parity.scope",
			Severity: ValidationSeverityError,
			Outcome:  ValidationOutcomeUnavailable,
			Message:  "NULL parity cannot prove a safe complete row scope",
			Remedy:   "use a complete non-null supported key with a route-bound primary-key equality proof, or strict whole-target reconciliation",
		}}
	}
	scopes := map[ValidationSide]ValidationNullScope{
		ValidationSource: sourceScope,
		ValidationTarget: targetScope,
	}
	resultChannel := make(chan validationNullResult, 2)
	for _, side := range []ValidationSide{ValidationSource, ValidationTarget} {
		go func(side ValidationSide) {
			evidence, err := probe.NullCounts(
				ctx,
				side,
				spec.Table,
				append([]string(nil), spec.Projection...),
				cloneValidationNullScope(scopes[side]),
			)
			deadlineExpired := validationDeadlineExpired(ctx, err)
			resultChannel <- validationNullResult{
				side: side, evidence: evidence, err: err,
				deadlineExpired: deadlineExpired,
			}
		}(side)
	}
	results := make(map[ValidationSide]validationNullResult, 2)
	for result := 0; result < 2; result++ {
		item := <-resultChannel
		results[item.side] = item
	}

	findings := make([]CoreValidationFinding, 0, len(spec.Projection)+2)
	validCounts := make(map[ValidationSide]map[string]int64, 2)
	incompleteFatal := false
	for _, side := range []ValidationSide{ValidationSource, ValidationTarget} {
		result := results[side]
		switch {
		case result.deadlineExpired:
			severity := ValidationSeverityError
			if !options.FailOnTimeout {
				severity = ValidationSeverityWarning
			}
			if severity == ValidationSeverityError {
				incompleteFatal = true
			}
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.null_parity.collect",
				Side: side, Severity: severity,
				Outcome: ValidationOutcomeTimeout,
				Message: fmt.Sprintf(
					"%s NULL-count validation timed out",
					side,
				),
				Remedy: "restore bounded NULL-count queries and retry validation",
			})
		case result.err != nil:
			incompleteFatal = true
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.null_parity.collect",
				Side: side, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: fmt.Sprintf(
					"%s NULL-count validation failed",
					side,
				),
				Remedy: "restore bounded NULL-count queries and retry validation",
			})
		default:
			if !sameValidationNullScope(
				result.evidence.Scope,
				scopes[side],
			) {
				incompleteFatal = true
				findings = append(findings, CoreValidationFinding{
					Table:    tableName,
					Check:    "validation.null_parity.scope",
					Side:     side,
					Severity: ValidationSeverityError,
					Outcome:  ValidationOutcomeUnavailable,
					Message: fmt.Sprintf(
						"%s NULL-count evidence did not echo the requested row scope",
						side,
					),
					Remedy: "repair the NULL-count probe to execute and echo the exact requested scope",
				})
				continue
			}
			rowCount := sourceCount
			if side == ValidationTarget {
				rowCount = targetCount
			}
			counts, ok := validateNullCountEvidence(
				result.evidence,
				scopes[side],
				spec.Projection,
				rowCount,
			)
			if !ok {
				incompleteFatal = true
				findings = append(findings, CoreValidationFinding{
					Table: tableName, Check: "validation.null_parity.collect",
					Side: side, Severity: ValidationSeverityError,
					Outcome: ValidationOutcomeUnavailable,
					Message: fmt.Sprintf(
						"%s NULL-count evidence is incomplete, invalid, or exceeds its authoritative row count",
						side,
					),
					Remedy: "return one non-negative NULL count no greater than the authoritative row count for every projected column",
				})
				continue
			}
			validCounts[side] = counts
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.null_parity.collect",
				Side: side, Severity: ValidationSeverityInfo,
				Outcome: ValidationOutcomePass,
				Message: fmt.Sprintf(
					"%s NULL counts collected for scope %s",
					side,
					scopes[side].Kind,
				),
			})
		}
	}

	source, sourceOK := validCounts[ValidationSource]
	target, targetOK := validCounts[ValidationTarget]
	if !sourceOK || !targetOK {
		severity := ValidationSeverityWarning
		if incompleteFatal {
			severity = ValidationSeverityError
		}
		findings = append(findings, CoreValidationFinding{
			Table: tableName, Check: "validation.null_parity.compare",
			Severity: severity,
			Outcome:  ValidationOutcomeUnavailable,
			Message:  "NULL parity lacks complete source and target evidence",
			Remedy:   "restore NULL-count evidence for both sides and retry validation",
		})
		return findings
	}
	if targetScope.Kind ==
		ValidationNullScopeTargetSourcePrimaryKeys {
		if results[ValidationTarget].evidence.Rows !=
			results[ValidationSource].evidence.Rows {
			findings = append(findings, CoreValidationFinding{
				Table: tableName, Check: "validation.null_parity.scope",
				Side: ValidationTarget, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: "target NULL-count scope did not match every transferred source primary key",
				Remedy:  "repair source-owned target key matching and retry validation",
			})
			return findings
		}
		for _, column := range targetScope.PrimaryKeyColumns {
			if source[column] != 0 {
				findings = append(findings, CoreValidationFinding{
					Table: tableName, Check: "validation.null_parity.scope",
					Side: ValidationSource, Column: column,
					Severity: ValidationSeverityError,
					Outcome:  ValidationOutcomeUnavailable,
					Message:  "source-owned target NULL scope contains a NULL primary-key value",
					Remedy:   "repair source primary-key integrity before scoped validation",
				})
				return findings
			}
		}
	}
	for _, column := range spec.Projection {
		sourceCount := source[column]
		targetCount := target[column]
		finding := CoreValidationFinding{
			Table: tableName, Check: "validation.null_parity.compare",
			Column: column, Severity: ValidationSeverityInfo,
			Outcome:        ValidationOutcomePass,
			Message:        "source and target NULL counts are equal",
			SourceRows:     int64Pointer(sourceCount),
			TargetRows:     int64Pointer(targetCount),
			SourceEvidence: ValidationEvidenceExact,
			TargetEvidence: ValidationEvidenceExact,
		}
		if sourceCount != targetCount {
			finding.Outcome = ValidationOutcomeMismatch
			finding.Message = "source and target NULL counts differ"
			finding.Remedy = "inspect the column mapping for NULL conversion or target-only rows"
			if options.FailOnMismatch {
				finding.Severity = ValidationSeverityError
			} else {
				finding.Severity = ValidationSeverityWarning
			}
		}
		findings = append(findings, finding)
	}
	return findings
}

func validateNullCountEvidence(
	evidence ValidationNullCountEvidence,
	requestedScope ValidationNullScope,
	projection []string,
	rowCount validationCountEvidence,
) (map[string]int64, bool) {
	if !sameValidationNullScope(evidence.Scope, requestedScope) ||
		evidence.Rows < 0 {
		return nil, false
	}
	counts := evidence.Counts
	if len(counts) != len(projection) {
		return nil, false
	}
	result := make(map[string]int64, len(counts))
	for _, column := range projection {
		count, exists := counts[column]
		if !exists || count < 0 {
			return nil, false
		}
		if count > evidence.Rows {
			return nil, false
		}
		result[column] = count
	}
	if rowCount.available &&
		rowCount.kind != ValidationEvidenceEstimate {
		switch requestedScope.Kind {
		case ValidationNullScopeTransferredSource,
			ValidationNullScopeWholeTarget:
			if evidence.Rows != rowCount.value {
				return nil, false
			}
		case ValidationNullScopeTargetSourcePrimaryKeys:
			if evidence.Rows > rowCount.value {
				return nil, false
			}
		default:
			return nil, false
		}
	}
	return result, true
}

func validateValidationCoreProjection(
	table schema.Table,
	projection []string,
	requirePrimaryKey bool,
) (validationSampleDescriptor, error) {
	if strings.TrimSpace(table.Name) == "" {
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation table name is required",
		)
	}
	if len(table.Columns) == 0 {
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation table %s has no columns",
			table.Name,
		)
	}
	if len(projection) == 0 {
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation projection for table %s is empty",
			table.Name,
		)
	}
	columns := make(map[string]struct{}, len(table.Columns))
	for _, column := range table.Columns {
		if strings.TrimSpace(column.Name) == "" {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s has an empty column name",
				table.Name,
			)
		}
		if _, exists := columns[column.Name]; exists {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation table %s has duplicate column %s",
				table.Name,
				column.Name,
			)
		}
		columns[column.Name] = struct{}{}
	}
	if len(projection) != len(columns) {
		return validationSampleDescriptor{}, fmt.Errorf(
			"validation projection for table %s is incomplete",
			table.Name,
		)
	}
	seen := make(map[string]struct{}, len(projection))
	for _, name := range projection {
		if _, exists := columns[name]; !exists {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation projection for table %s contains unknown column %s",
				table.Name,
				name,
			)
		}
		if _, duplicate := seen[name]; duplicate {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation projection for table %s duplicates column %s",
				table.Name,
				name,
			)
		}
		seen[name] = struct{}{}
	}
	if !requirePrimaryKey {
		return validationSampleDescriptor{}, nil
	}
	return newValidationSampleDescriptor(table, projection)
}

type canonicalValidationSample struct {
	key       []byte
	keyValues []any
	row       []byte
}

func runValidationSample(
	ctx context.Context,
	options ValidationCoreOptions,
	spec ValidationTableSpec,
	descriptor validationSampleDescriptor,
	sourceCount validationCountEvidence,
	probe ValidationCoreProbe,
) []CoreValidationFinding {
	tableName := validationTableName(spec.Table)
	sourceRows, err := probe.SampleSourceRows(
		ctx,
		spec.Table,
		append([]string(nil), spec.Projection...),
		options.SampleLimit,
	)
	sourceDeadlineExpired := validationDeadlineExpired(ctx, err)
	if err != nil || sourceDeadlineExpired {
		return []CoreValidationFinding{sampleCollectionFailure(
			tableName,
			ValidationSource,
			sourceDeadlineExpired,
			options.FailOnTimeout,
		)}
	}
	if len(sourceRows) > options.SampleLimit {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationSource, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "source sample exceeded the configured bound",
			Remedy:  "repair the source sampler to honor the exact sample limit",
		}}
	}
	expectedKnown := sourceCount.available &&
		sourceCount.kind != ValidationEvidenceEstimate
	if expectedKnown {
		expected := sourceCount.value
		if expected > int64(options.SampleLimit) {
			expected = int64(options.SampleLimit)
		}
		if int64(len(sourceRows)) != expected {
			return []CoreValidationFinding{{
				Table: tableName, Check: "validation.sample.collect",
				Side: ValidationSource, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: "source sample is incomplete for the authoritative source count",
				Remedy:  "repair deterministic source sampling and retry validation",
			}}
		}
	} else if len(sourceRows) < options.SampleLimit {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationSource, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "source sample completeness cannot be proven without an exact source count",
			Remedy:  "obtain an exact source count or return a full bounded sample",
		}}
	}

	primaryKeyIndexes, primaryKeyDescriptor, err :=
		validationPrimaryKeyDescriptor(spec.Table, descriptor)
	if err != nil {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.projection",
			Severity: ValidationSeverityError,
			Outcome:  ValidationOutcomeUnavailable,
			Message:  "sample projection does not contain a complete ordered primary key",
			Remedy:   "rebuild sample metadata from the complete primary key",
		}}
	}
	canonicalSource, err := canonicalizeValidationSamples(
		descriptor,
		primaryKeyDescriptor,
		primaryKeyIndexes,
		sourceRows,
	)
	if err != nil {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.canonicalize",
			Side: ValidationSource, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "source sample contains incomplete or unsupported typed values",
			Remedy:  "repair source decoding or the validation type descriptor",
		}}
	}
	if err := validateIncreasingValidationPrimaryKeys(
		primaryKeyDescriptor,
		canonicalSource,
	); err != nil {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.order",
			Side: ValidationSource, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "source sample is not in strictly increasing complete-primary-key order",
			Remedy:  "repair the source sampler to use adapter-certified ascending typed primary-key order",
		}}
	}
	if duplicateCanonicalValidationKey(canonicalSource) {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationSource, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "source sample contains a duplicate canonical primary key",
			Remedy:  "repair complete-primary-key discovery or deterministic sampling",
		}}
	}

	keys := make([]ValidationPrimaryKey, len(canonicalSource))
	for index, row := range canonicalSource {
		keys[index] = ValidationPrimaryKey{
			Values: append([]any(nil), row.keyValues...),
		}
	}
	targetRows, err := probe.SampleTargetRows(
		ctx,
		spec.Table,
		append([]string(nil), spec.Projection...),
		keys,
	)
	targetDeadlineExpired := validationDeadlineExpired(ctx, err)
	if err != nil || targetDeadlineExpired {
		return []CoreValidationFinding{sampleCollectionFailure(
			tableName,
			ValidationTarget,
			targetDeadlineExpired,
			options.FailOnTimeout,
		)}
	}
	if len(targetRows) > len(keys) {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationTarget, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "target sample returned more rows than requested primary keys",
			Remedy:  "repair complete-primary-key target lookup",
		}}
	}
	canonicalTarget, err := canonicalizeValidationSamples(
		descriptor,
		primaryKeyDescriptor,
		primaryKeyIndexes,
		targetRows,
	)
	if err != nil {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.canonicalize",
			Side: ValidationTarget, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "target sample contains incomplete or unsupported typed values",
			Remedy:  "repair target decoding or the validation type descriptor",
		}}
	}
	if duplicateCanonicalValidationKey(canonicalTarget) {
		return []CoreValidationFinding{{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationTarget, Severity: ValidationSeverityError,
			Outcome: ValidationOutcomeUnavailable,
			Message: "target sample contains a duplicate canonical primary key",
			Remedy:  "repair complete-primary-key target lookup",
		}}
	}

	targetByKey := make(map[string][]byte, len(canonicalTarget))
	requested := make(map[string]struct{}, len(canonicalSource))
	for _, row := range canonicalSource {
		requested[string(row.key)] = struct{}{}
	}
	for _, row := range canonicalTarget {
		key := string(row.key)
		if _, exists := requested[key]; !exists {
			return []CoreValidationFinding{{
				Table: tableName, Check: "validation.sample.collect",
				Side: ValidationTarget, Severity: ValidationSeverityError,
				Outcome: ValidationOutcomeUnavailable,
				Message: "target sample returned an unrequested primary key",
				Remedy:  "repair complete-primary-key target lookup",
			}}
		}
		targetByKey[key] = row.row
	}

	findings := make([]CoreValidationFinding, 0, len(canonicalSource)+2)
	findings = append(findings,
		CoreValidationFinding{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationSource, Severity: ValidationSeverityInfo,
			Outcome: ValidationOutcomePass,
			Message: "bounded deterministic source sample collected",
		},
		CoreValidationFinding{
			Table: tableName, Check: "validation.sample.collect",
			Side: ValidationTarget, Severity: ValidationSeverityInfo,
			Outcome: ValidationOutcomePass,
			Message: "target rows fetched by complete sampled primary keys",
		},
	)
	for index, source := range canonicalSource {
		target, exists := targetByKey[string(source.key)]
		finding := CoreValidationFinding{
			Table: tableName, Check: "validation.sample.compare",
			Sample: index + 1, Severity: ValidationSeverityInfo,
			Outcome: ValidationOutcomePass,
			Message: "sampled source and target rows are equal",
		}
		switch {
		case !exists:
			finding.Outcome = ValidationOutcomeMismatch
			finding.Message = "sampled source row is missing from the target"
			finding.Remedy = "repair the missing target row and rerun validation"
		case !bytes.Equal(source.row, target):
			finding.Outcome = ValidationOutcomeMismatch
			finding.Message = "sampled source and target rows differ"
			finding.Remedy = "inspect type mapping and transferred column values"
		}
		if finding.Outcome == ValidationOutcomeMismatch {
			if options.FailOnMismatch {
				finding.Severity = ValidationSeverityError
			} else {
				finding.Severity = ValidationSeverityWarning
			}
		}
		findings = append(findings, finding)
	}
	return findings
}

func sampleCollectionFailure(
	table string,
	side ValidationSide,
	deadlineExpired bool,
	failOnTimeout bool,
) CoreValidationFinding {
	outcome := ValidationOutcomeUnavailable
	message := fmt.Sprintf("%s sample collection failed", side)
	severity := ValidationSeverityError
	if deadlineExpired {
		outcome = ValidationOutcomeTimeout
		message = fmt.Sprintf("%s sample collection timed out", side)
		if !failOnTimeout {
			severity = ValidationSeverityWarning
		}
	}
	return CoreValidationFinding{
		Table: table, Check: "validation.sample.collect", Side: side,
		Severity: severity, Outcome: outcome,
		Message: message,
		Remedy:  "restore bounded complete-primary-key sampling and retry validation",
	}
}

func validationPrimaryKeyDescriptor(
	table schema.Table,
	descriptor validationSampleDescriptor,
) ([]int, validationSampleDescriptor, error) {
	positions := make(map[string]int, len(descriptor.Columns))
	for index, column := range descriptor.Columns {
		positions[column.Name] = index
	}
	type keyColumn struct {
		position int
		index    int
		column   validationColumnDescriptor
	}
	keys := make([]keyColumn, 0)
	for _, column := range table.Columns {
		if !column.PrimaryKey {
			continue
		}
		index, exists := positions[column.Name]
		if !exists || column.PrimaryKeyPosition <= 0 {
			return nil, validationSampleDescriptor{}, fmt.Errorf(
				"incomplete primary key",
			)
		}
		keys = append(keys, keyColumn{
			position: column.PrimaryKeyPosition,
			index:    index,
			column:   descriptor.Columns[index],
		})
	}
	if len(keys) == 0 {
		return nil, validationSampleDescriptor{}, fmt.Errorf(
			"primary key is required",
		)
	}
	sort.Slice(keys, func(left, right int) bool {
		return keys[left].position < keys[right].position
	})
	indexes := make([]int, len(keys))
	keyDescriptor := validationSampleDescriptor{
		Columns: make([]validationColumnDescriptor, len(keys)),
	}
	for index, key := range keys {
		if key.position != index+1 {
			return nil, validationSampleDescriptor{}, fmt.Errorf(
				"primary-key positions are not contiguous",
			)
		}
		indexes[index] = key.index
		keyDescriptor.Columns[index] = key.column
	}
	return indexes, keyDescriptor, nil
}

func canonicalizeValidationSamples(
	descriptor validationSampleDescriptor,
	primaryKeyDescriptor validationSampleDescriptor,
	primaryKeyIndexes []int,
	rows []ValidationSampleRow,
) ([]canonicalValidationSample, error) {
	result := make([]canonicalValidationSample, len(rows))
	for index, row := range rows {
		encoded, err := canonicalValidationRow(descriptor, row.Values)
		if err != nil {
			return nil, err
		}
		keyValues := make([]any, len(primaryKeyIndexes))
		for keyIndex, valueIndex := range primaryKeyIndexes {
			if valueIndex < 0 || valueIndex >= len(row.Values) {
				return nil, fmt.Errorf("primary-key projection is incomplete")
			}
			keyValues[keyIndex] = row.Values[valueIndex]
		}
		key, err := canonicalValidationRow(
			primaryKeyDescriptor,
			keyValues,
		)
		if err != nil {
			return nil, err
		}
		result[index] = canonicalValidationSample{
			key:       key,
			keyValues: keyValues,
			row:       encoded,
		}
	}
	return result, nil
}

func duplicateCanonicalValidationKey(rows []canonicalValidationSample) bool {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		key := string(row.key)
		if _, exists := seen[key]; exists {
			return true
		}
		seen[key] = struct{}{}
	}
	return false
}

func validateIncreasingValidationPrimaryKeys(
	descriptor validationSampleDescriptor,
	rows []canonicalValidationSample,
) error {
	for index := 1; index < len(rows); index++ {
		comparison, err := compareValidationPrimaryKeyValues(
			descriptor,
			rows[index-1].keyValues,
			rows[index].keyValues,
		)
		if err != nil {
			return err
		}
		if comparison >= 0 {
			return fmt.Errorf(
				"source primary keys are not strictly increasing",
			)
		}
	}
	return nil
}
