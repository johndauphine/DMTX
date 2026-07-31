package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/johndauphine/dmtx/internal/schema"
)

const (
	adapterValidationPostgres = "postgres"
	adapterValidationSQLite   = "sqlite"

	adapterValidationPostgresParameterLimit = 65535
	adapterValidationSQLiteParameterLimit   = 900
	adapterValidationMaximumKeyBatch        = 256
)

// adapterStage4ValidationEqualityProofProvider exposes the exact proof digest
// accepted by the database-backed probe for source-owned target NULL scopes.
// The digest contains metadata only. It never contains sampled keys or values.
type adapterStage4ValidationEqualityProofProvider interface {
	Stage4ValidationPrimaryKeyEqualityProof(schema.Table) (string, error)
}

type adapterValidationSQLQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type adapterValidationSQLEndpoint struct {
	engine         string
	namespace      string
	queryer        adapterValidationSQLQueryer
	database       *sql.DB
	parameterLimit int
}

type adapterDatabaseValidationProbe struct {
	source adapterValidationSQLEndpoint
	target adapterValidationSQLEndpoint
	plans  map[stage4RichTableKey]adapterTablePlan

	sourceGate adapterValidationProbeGate
	targetGate adapterValidationProbeGate
}

type adapterValidationProbeGate struct {
	once  sync.Once
	token chan struct{}
}

func (gate *adapterValidationProbeGate) acquire(ctx context.Context) error {
	if ctx == nil {
		return errors.New("validation probe context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	gate.once.Do(func() {
		gate.token = make(chan struct{}, 1)
		gate.token <- struct{}{}
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate.token:
		if err := ctx.Err(); err != nil {
			gate.release()
			return err
		}
		return nil
	}
}

func (gate *adapterValidationProbeGate) release() {
	gate.token <- struct{}{}
}

func (adapter *relationalSourceAdapter) Stage4ValidationProbe(
	source sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if adapter == nil || adapter.database == nil {
		return nil, errors.New(
			"database-backed validation requires an open relational source",
		)
	}
	if isNilInterface(source) || source != adapter {
		return nil, errors.New(
			"database-backed validation source differs from its provider",
		)
	}
	if adapter.spec.engine != adapterValidationPostgres {
		return nil, fmt.Errorf(
			"database-backed deep validation for source engine %q is not certified",
			adapter.spec.engine,
		)
	}
	return newAdapterDatabaseValidationProbe(
		adapterValidationSQLEndpoint{
			engine: adapterValidationPostgres, namespace: adapter.namespace,
			queryer:        adapter.database,
			database:       adapter.database,
			parameterLimit: adapterValidationPostgresParameterLimit,
		},
		target,
		plans,
	)
}

func (view *adapterRetainedStableRelationalView) Stage4ValidationProbe(
	source sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if view == nil || view.source == nil || isNilInterface(view.view) {
		return nil, errors.New(
			"database-backed validation requires a live stable source view",
		)
	}
	if isNilInterface(source) || source != view.source {
		return nil, errors.New(
			"stable validation view differs from the route source",
		)
	}
	if view.source.spec.engine != adapterValidationPostgres ||
		view.view.retainedStableViewEngine() != adapterValidationPostgres {
		return nil, fmt.Errorf(
			"stable database-backed deep validation for source engine %q is not certified",
			view.source.spec.engine,
		)
	}
	for _, plan := range plans {
		if err := view.admitTable(plan.source); err != nil {
			return nil, fmt.Errorf(
				"admit stable validation table %s: %w",
				plan.source.Name,
				err,
			)
		}
	}
	return newAdapterDatabaseValidationProbe(
		adapterValidationSQLEndpoint{
			engine:         adapterValidationPostgres,
			namespace:      view.source.namespace,
			queryer:        view.view,
			database:       view.source.database,
			parameterLimit: adapterValidationPostgresParameterLimit,
		},
		target,
		plans,
	)
}

func (adapter *sqliteSourceAdapter) Stage4ValidationProbe(
	source sourceAdapter,
	target targetAdapter,
	plans []adapterTablePlan,
) (ValidationCoreProbe, error) {
	if adapter == nil || adapter.snapshot == nil {
		return nil, errors.New(
			"database-backed validation requires an open SQLite source snapshot",
		)
	}
	if isNilInterface(source) || source != adapter {
		return nil, errors.New(
			"database-backed validation source differs from its provider",
		)
	}
	return newAdapterDatabaseValidationProbe(
		adapterValidationSQLEndpoint{
			engine: adapterValidationSQLite, queryer: adapter.snapshot,
			database:       adapter.database,
			parameterLimit: adapterValidationSQLiteParameterLimit,
		},
		target,
		plans,
	)
}

func newAdapterDatabaseValidationProbe(
	source adapterValidationSQLEndpoint,
	targetAdapter targetAdapter,
	plans []adapterTablePlan,
) (*adapterDatabaseValidationProbe, error) {
	if err := validateAdapterValidationEndpoint(source); err != nil {
		return nil, fmt.Errorf("validation source: %w", err)
	}
	target, err := adapterValidationTargetEndpoint(targetAdapter)
	if err != nil {
		return nil, err
	}
	if source.engine != target.engine {
		return nil, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"deep validation route %s-to-%s lacks a certified cross-engine primary-key and value domain",
				source.engine,
				target.engine,
			),
		)
	}
	result := &adapterDatabaseValidationProbe{
		source: source,
		target: target,
		plans: make(
			map[stage4RichTableKey]adapterTablePlan,
			len(plans),
		),
	}
	for index, plan := range plans {
		if err := validateAdapterValidationPlan(
			source,
			target,
			plan,
		); err != nil {
			return nil, fmt.Errorf(
				"validation table plan %d: %w",
				index,
				err,
			)
		}
		sameDatabase := source.database != nil &&
			source.database == target.database
		if (sameDatabase || sameAdapterValidationQueryer(
			source.queryer,
			target.queryer,
		)) &&
			plan.source.Schema == plan.target.Schema &&
			plan.source.Name == plan.target.Name {
			return nil, NewTransferError(
				ErrorClassPolicy,
				fmt.Errorf(
					"validation source and target resolve to the same live relation (%q, %q)",
					plan.source.Schema,
					plan.source.Name,
				),
			)
		}
		key := stage4RichTableKey{
			schema: plan.source.Schema,
			table:  plan.source.Name,
		}
		if _, duplicate := result.plans[key]; duplicate {
			return nil, fmt.Errorf(
				"validation route contains duplicate source table (%q, %q)",
				key.schema,
				key.table,
			)
		}
		result.plans[key] = adapterTablePlan{
			source:  cloneStage4RichTable(plan.source),
			target:  cloneStage4RichTable(plan.target),
			columns: append([]string(nil), plan.columns...),
		}
	}
	return result, nil
}

func validateAdapterValidationEndpoint(
	endpoint adapterValidationSQLEndpoint,
) error {
	if endpoint.engine != adapterValidationPostgres &&
		endpoint.engine != adapterValidationSQLite {
		return fmt.Errorf(
			"engine %q is not certified",
			endpoint.engine,
		)
	}
	if isNilAdapterValidationQueryer(endpoint.queryer) {
		return errors.New("queryer is unavailable")
	}
	if endpoint.parameterLimit < 1 {
		return errors.New("parameter limit is invalid")
	}
	if endpoint.engine == adapterValidationSQLite &&
		endpoint.namespace != "" {
		return errors.New("SQLite namespace must be empty")
	}
	if endpoint.engine == adapterValidationPostgres &&
		strings.TrimSpace(endpoint.namespace) == "" {
		return errors.New("PostgreSQL namespace is required")
	}
	return nil
}

func adapterValidationTargetEndpoint(
	target targetAdapter,
) (adapterValidationSQLEndpoint, error) {
	switch typed := target.(type) {
	case *postgresTargetAdapter:
		if typed == nil || typed.database == nil {
			return adapterValidationSQLEndpoint{}, errors.New(
				"database-backed validation requires an open PostgreSQL target",
			)
		}
		return adapterValidationSQLEndpoint{
			engine: adapterValidationPostgres, namespace: typed.namespace,
			queryer:        typed.database,
			database:       typed.database,
			parameterLimit: adapterValidationPostgresParameterLimit,
		}, nil
	case *sqliteTargetAdapter:
		if typed == nil || typed.database == nil {
			return adapterValidationSQLEndpoint{}, errors.New(
				"database-backed validation requires an open SQLite target",
			)
		}
		return adapterValidationSQLEndpoint{
			engine: adapterValidationSQLite, queryer: typed.database,
			database:       typed.database,
			parameterLimit: adapterValidationSQLiteParameterLimit,
		}, nil
	default:
		engine := ""
		if !isNilInterface(target) {
			engine = target.Engine()
		}
		return adapterValidationSQLEndpoint{}, NewTransferError(
			ErrorClassPolicy,
			fmt.Errorf(
				"database-backed deep validation target engine %q is not certified",
				engine,
			),
		)
	}
}

func validateAdapterValidationPlan(
	source adapterValidationSQLEndpoint,
	target adapterValidationSQLEndpoint,
	plan adapterTablePlan,
) error {
	if strings.TrimSpace(plan.source.Name) == "" ||
		strings.TrimSpace(plan.target.Name) == "" ||
		plan.source.Name != plan.target.Name {
		return errors.New(
			"source and target table identities are absent or differ",
		)
	}
	if plan.source.Schema != source.namespace ||
		plan.target.Schema != target.namespace {
		return errors.New(
			"source or target table namespace differs from its live endpoint",
		)
	}
	if _, err := validateValidationCoreProjection(
		plan.source,
		plan.columns,
		false,
	); err != nil {
		return fmt.Errorf("source projection: %w", err)
	}
	if _, err := validateValidationCoreProjection(
		plan.target,
		plan.columns,
		false,
	); err != nil {
		return fmt.Errorf("target projection: %w", err)
	}
	return nil
}

func (probe *adapterDatabaseValidationProbe) ExactCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	endpoint, selected, gate, err := probe.side(side, table)
	if err != nil {
		return 0, err
	}
	if err := gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer gate.release()
	var count int64
	if err := endpoint.queryer.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+endpoint.qualified(selected),
	).Scan(&count); err != nil {
		return 0, fmt.Errorf(
			"collect exact %s validation count for table %s: %w",
			side,
			table.Name,
			err,
		)
	}
	if count < 0 {
		return 0, fmt.Errorf(
			"exact %s validation count for table %s is negative",
			side,
			table.Name,
		)
	}
	return count, nil
}

func (probe *adapterDatabaseValidationProbe) EstimateCount(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
) (int64, error) {
	endpoint, selected, gate, err := probe.side(side, table)
	if err != nil {
		return 0, err
	}
	if endpoint.engine != adapterValidationPostgres {
		return 0, fmt.Errorf(
			"%s validation estimates are unavailable for engine %s",
			side,
			endpoint.engine,
		)
	}
	if err := gate.acquire(ctx); err != nil {
		return 0, err
	}
	defer gate.release()
	var estimate int64
	if err := endpoint.queryer.QueryRowContext(
		ctx,
		`SELECT GREATEST(c.reltuples::bigint, 0)
		   FROM pg_catalog.pg_class AS c
		   JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
		  WHERE n.nspname = $1
		    AND c.relname = $2
		    AND c.relkind IN ('r', 'p')`,
		selected.Schema,
		selected.Name,
	).Scan(&estimate); err != nil {
		return 0, fmt.Errorf(
			"collect PostgreSQL %s validation estimate for table %s: %w",
			side,
			table.Name,
			err,
		)
	}
	if estimate < 0 {
		return 0, fmt.Errorf(
			"PostgreSQL %s validation estimate for table %s is negative",
			side,
			table.Name,
		)
	}
	return estimate, nil
}

func (probe *adapterDatabaseValidationProbe) NullCounts(
	ctx context.Context,
	side ValidationSide,
	table schema.Table,
	projection []string,
	scope ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	plan, err := probe.plan(table)
	if err != nil {
		return ValidationNullCountEvidence{}, err
	}
	switch side {
	case ValidationSource:
		if scope.Kind != ValidationNullScopeTransferredSource ||
			len(scope.PrimaryKeyColumns) != 0 ||
			scope.EqualityProofDigest != "" {
			return ValidationNullCountEvidence{}, errors.New(
				"source NULL validation requested an invalid scope",
			)
		}
		if err := validateAdapterValidationProjection(
			plan.source,
			projection,
		); err != nil {
			return ValidationNullCountEvidence{}, err
		}
		if err := probe.sourceGate.acquire(ctx); err != nil {
			return ValidationNullCountEvidence{}, err
		}
		defer probe.sourceGate.release()
		return queryAdapterValidationNullCounts(
			ctx,
			probe.source,
			plan.source,
			projection,
			scope,
			"",
			nil,
		)
	case ValidationTarget:
		if err := validateAdapterValidationProjection(
			plan.target,
			projection,
		); err != nil {
			return ValidationNullCountEvidence{}, err
		}
		switch scope.Kind {
		case ValidationNullScopeWholeTarget:
			if len(scope.PrimaryKeyColumns) != 0 ||
				scope.EqualityProofDigest != "" {
				return ValidationNullCountEvidence{}, errors.New(
					"whole-target NULL validation requested an invalid scope",
				)
			}
			if err := probe.targetGate.acquire(ctx); err != nil {
				return ValidationNullCountEvidence{}, err
			}
			defer probe.targetGate.release()
			return queryAdapterValidationNullCounts(
				ctx,
				probe.target,
				plan.target,
				projection,
				scope,
				"",
				nil,
			)
		case ValidationNullScopeTargetSourcePrimaryKeys:
			return probe.scopedTargetNullCounts(
				ctx,
				plan,
				projection,
				scope,
			)
		default:
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"target NULL validation requested unknown scope %q",
				scope.Kind,
			)
		}
	default:
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"unknown validation side %q",
			side,
		)
	}
}

func (probe *adapterDatabaseValidationProbe) SampleSourceRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	limit int,
) ([]ValidationSampleRow, error) {
	if limit < 1 || limit > maxValidationSampleRows {
		return nil, fmt.Errorf(
			"source validation sample limit %d is outside the supported bound",
			limit,
		)
	}
	plan, err := probe.plan(table)
	if err != nil {
		return nil, err
	}
	if err := validateAdapterValidationProjection(
		plan.source,
		projection,
	); err != nil {
		return nil, err
	}
	primaryKey, _, _, err := adapterValidationEqualityProof(
		probe.source.engine,
		plan,
	)
	if err != nil {
		return nil, err
	}
	order, err := adapterValidationOrderBy(
		probe.source,
		primaryKey,
	)
	if err != nil {
		return nil, err
	}
	query := "SELECT " + adapterValidationQuotedColumns(
		probe.source,
		projection,
	) + " FROM " + probe.source.qualified(plan.source) +
		" ORDER BY " + order +
		" LIMIT " + probe.source.placeholder(1)
	if err := probe.sourceGate.acquire(ctx); err != nil {
		return nil, err
	}
	defer probe.sourceGate.release()
	rows, err := probe.source.queryer.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf(
			"collect deterministic source validation sample for table %s: %w",
			table.Name,
			err,
		)
	}
	return scanAdapterValidationRows(
		rows,
		len(projection),
		limit,
		"source validation sample",
	)
}

func (probe *adapterDatabaseValidationProbe) SampleTargetRows(
	ctx context.Context,
	table schema.Table,
	projection []string,
	keys []ValidationPrimaryKey,
) ([]ValidationSampleRow, error) {
	if len(keys) > maxValidationSampleRows {
		return nil, fmt.Errorf(
			"target validation key count %d exceeds the supported bound",
			len(keys),
		)
	}
	if len(keys) == 0 {
		return []ValidationSampleRow{}, nil
	}
	plan, err := probe.plan(table)
	if err != nil {
		return nil, err
	}
	if err := validateAdapterValidationProjection(
		plan.target,
		projection,
	); err != nil {
		return nil, err
	}
	_, targetPrimaryKey, _, err := adapterValidationEqualityProof(
		probe.source.engine,
		plan,
	)
	if err != nil {
		return nil, err
	}
	requestedKeys, err := adapterValidationCanonicalKeySet(
		targetPrimaryKey,
		keys,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"validate requested target sample keys: %w",
			err,
		)
	}
	batchSize, err := adapterValidationKeyBatchSize(
		probe.target.parameterLimit,
		len(targetPrimaryKey),
	)
	if err != nil {
		return nil, err
	}
	if err := probe.targetGate.acquire(ctx); err != nil {
		return nil, err
	}
	defer probe.targetGate.release()
	result := make([]ValidationSampleRow, 0, len(keys))
	for offset := 0; offset < len(keys); offset += batchSize {
		end := offset + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		predicate, arguments, err :=
			adapterValidationKeyPredicate(
				probe.target,
				targetPrimaryKey,
				keys[offset:end],
			)
		if err != nil {
			return nil, err
		}
		query := "SELECT " + adapterValidationQuotedColumns(
			probe.target,
			projection,
		) + " FROM " + probe.target.qualified(plan.target) +
			" WHERE " + predicate
		rows, err := probe.target.queryer.QueryContext(
			ctx,
			query,
			arguments...,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"fetch target validation sample for table %s failed with error type %T",
				table.Name,
				err,
			)
		}
		batch, err := scanAdapterValidationRows(
			rows,
			len(projection),
			end-offset,
			"target validation sample",
		)
		if err != nil {
			return nil, err
		}
		result = append(result, batch...)
	}
	if err := validateAdapterValidationTargetRows(
		plan.target,
		projection,
		targetPrimaryKey,
		requestedKeys,
		result,
	); err != nil {
		return nil, err
	}
	return result, nil
}

func (probe *adapterDatabaseValidationProbe) Stage4ValidationPrimaryKeyEqualityProof(
	table schema.Table,
) (string, error) {
	plan, err := probe.plan(table)
	if err != nil {
		return "", err
	}
	_, _, digest, err := adapterValidationEqualityProof(
		probe.source.engine,
		plan,
	)
	return digest, err
}

func (probe *adapterDatabaseValidationProbe) scopedTargetNullCounts(
	ctx context.Context,
	plan adapterTablePlan,
	projection []string,
	scope ValidationNullScope,
) (ValidationNullCountEvidence, error) {
	sourcePrimaryKey, targetPrimaryKey, expectedDigest, err :=
		adapterValidationEqualityProof(
			probe.source.engine,
			plan,
		)
	if err != nil {
		return ValidationNullCountEvidence{}, err
	}
	if !validValidationEqualityProofDigest(
		scope.EqualityProofDigest,
	) ||
		scope.EqualityProofDigest != expectedDigest ||
		!reflect.DeepEqual(
			scope.PrimaryKeyColumns,
			adapterValidationColumnNames(sourcePrimaryKey),
		) {
		return ValidationNullCountEvidence{}, errors.New(
			"target NULL scope does not carry this route's certified primary-key equality proof",
		)
	}
	batchSize, err := adapterValidationKeyBatchSize(
		probe.target.parameterLimit,
		len(targetPrimaryKey),
	)
	if err != nil {
		return ValidationNullCountEvidence{}, err
	}
	if err := probe.sourceGate.acquire(ctx); err != nil {
		return ValidationNullCountEvidence{}, err
	}
	defer probe.sourceGate.release()
	if err := probe.targetGate.acquire(ctx); err != nil {
		return ValidationNullCountEvidence{}, err
	}
	defer probe.targetGate.release()

	order, err := adapterValidationOrderBy(
		probe.source,
		sourcePrimaryKey,
	)
	if err != nil {
		return ValidationNullCountEvidence{}, err
	}
	query := "SELECT " + adapterValidationQuotedColumns(
		probe.source,
		adapterValidationColumnNames(sourcePrimaryKey),
	) + " FROM " + probe.source.qualified(plan.source) +
		" ORDER BY " + order
	rows, err := probe.source.queryer.QueryContext(ctx, query)
	if err != nil {
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"collect source primary keys for scoped target NULL validation: %w",
			err,
		)
	}
	defer rows.Close()

	evidence := ValidationNullCountEvidence{
		Scope:  cloneValidationNullScope(scope),
		Counts: make(map[string]int64, len(projection)),
	}
	for _, column := range projection {
		evidence.Counts[column] = 0
	}
	keys := make([]ValidationPrimaryKey, 0, batchSize)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		predicate, arguments, err :=
			adapterValidationKeyPredicate(
				probe.target,
				targetPrimaryKey,
				keys,
			)
		if err != nil {
			return err
		}
		batch, err := queryAdapterValidationNullCounts(
			ctx,
			probe.target,
			plan.target,
			projection,
			scope,
			predicate,
			arguments,
		)
		if err != nil {
			return err
		}
		evidence.Rows += batch.Rows
		for _, column := range projection {
			evidence.Counts[column] += batch.Counts[column]
		}
		keys = keys[:0]
		return nil
	}
	for rows.Next() {
		key, err := scanAdapterValidationPrimaryKey(
			rows,
			len(sourcePrimaryKey),
		)
		if err != nil {
			return ValidationNullCountEvidence{}, err
		}
		keys = append(keys, key)
		if len(keys) == batchSize {
			if err := flush(); err != nil {
				return ValidationNullCountEvidence{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"iterate source primary keys for scoped target NULL validation: %w",
			err,
		)
	}
	if err := flush(); err != nil {
		return ValidationNullCountEvidence{}, err
	}
	return evidence, nil
}

func (probe *adapterDatabaseValidationProbe) side(
	side ValidationSide,
	table schema.Table,
) (
	adapterValidationSQLEndpoint,
	schema.Table,
	*adapterValidationProbeGate,
	error,
) {
	plan, err := probe.plan(table)
	if err != nil {
		return adapterValidationSQLEndpoint{}, schema.Table{}, nil, err
	}
	switch side {
	case ValidationSource:
		return probe.source, plan.source, &probe.sourceGate, nil
	case ValidationTarget:
		return probe.target, plan.target, &probe.targetGate, nil
	default:
		return adapterValidationSQLEndpoint{}, schema.Table{}, nil,
			fmt.Errorf("unknown validation side %q", side)
	}
}

func (probe *adapterDatabaseValidationProbe) plan(
	table schema.Table,
) (adapterTablePlan, error) {
	if probe == nil {
		return adapterTablePlan{}, errors.New(
			"database-backed validation probe is unavailable",
		)
	}
	plan, ok := probe.plans[stage4RichTableKey{
		schema: table.Schema,
		table:  table.Name,
	}]
	if !ok {
		return adapterTablePlan{}, fmt.Errorf(
			"database-backed validation has no plan for table (%q, %q)",
			table.Schema,
			table.Name,
		)
	}
	return plan, nil
}

func queryAdapterValidationNullCounts(
	ctx context.Context,
	endpoint adapterValidationSQLEndpoint,
	table schema.Table,
	projection []string,
	scope ValidationNullScope,
	predicate string,
	arguments []any,
) (ValidationNullCountEvidence, error) {
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
	query := "SELECT " + strings.Join(terms, ", ") +
		" FROM " + endpoint.qualified(table)
	if predicate != "" {
		query += " WHERE " + predicate
	}
	evidence := ValidationNullCountEvidence{
		Scope:  cloneValidationNullScope(scope),
		Counts: make(map[string]int64, len(projection)),
	}
	destinations := make([]any, len(projection)+1)
	destinations[0] = &evidence.Rows
	counts := make([]int64, len(projection))
	for index := range counts {
		destinations[index+1] = &counts[index]
	}
	if err := endpoint.queryer.QueryRowContext(
		ctx,
		query,
		arguments...,
	).Scan(destinations...); err != nil {
		if len(arguments) != 0 {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"collect %s NULL validation counts for table %s failed with error type %T",
				scope.Kind,
				table.Name,
				err,
			)
		}
		return ValidationNullCountEvidence{}, fmt.Errorf(
			"collect %s NULL validation counts for table %s: %w",
			scope.Kind,
			table.Name,
			err,
		)
	}
	if evidence.Rows < 0 {
		return ValidationNullCountEvidence{}, errors.New(
			"NULL validation row count is negative",
		)
	}
	for index, column := range projection {
		if counts[index] < 0 || counts[index] > evidence.Rows {
			return ValidationNullCountEvidence{}, fmt.Errorf(
				"NULL validation count for column %s is invalid",
				column,
			)
		}
		evidence.Counts[column] = counts[index]
	}
	return evidence, nil
}

func validateAdapterValidationProjection(
	table schema.Table,
	projection []string,
) error {
	if _, err := validateValidationCoreProjection(
		table,
		projection,
		false,
	); err != nil {
		return fmt.Errorf(
			"validate database-backed projection for table %s: %w",
			table.Name,
			err,
		)
	}
	return nil
}

func adapterValidationEqualityProof(
	engineName string,
	plan adapterTablePlan,
) ([]schema.Column, []schema.Column, string, error) {
	sourcePrimaryKey, err := adapterValidationPrimaryKey(
		plan.source,
	)
	if err != nil {
		return nil, nil, "", fmt.Errorf(
			"source validation primary key: %w",
			err,
		)
	}
	targetPrimaryKey, err := adapterValidationPrimaryKey(
		plan.target,
	)
	if err != nil {
		return nil, nil, "", fmt.Errorf(
			"target validation primary key: %w",
			err,
		)
	}
	if len(sourcePrimaryKey) != len(targetPrimaryKey) {
		return nil, nil, "", errors.New(
			"source and target validation primary-key widths differ",
		)
	}
	type proofColumn struct {
		Name       string
		Position   int
		SourceKind validationValueKind
		TargetKind validationValueKind
		SourceType string
		TargetType string
	}
	wire := struct {
		Version      int
		Engine       string
		SourceSchema string
		TargetSchema string
		Table        string
		Columns      []proofColumn
	}{
		Version: 1, Engine: engineName,
		SourceSchema: plan.source.Schema,
		TargetSchema: plan.target.Schema,
		Table:        plan.source.Name,
		Columns:      make([]proofColumn, len(sourcePrimaryKey)),
	}
	for index := range sourcePrimaryKey {
		sourceColumn := sourcePrimaryKey[index]
		targetColumn := targetPrimaryKey[index]
		if sourceColumn.Name != targetColumn.Name ||
			sourceColumn.PrimaryKeyPosition !=
				targetColumn.PrimaryKeyPosition {
			return nil, nil, "", fmt.Errorf(
				"source and target validation primary-key column %d differ",
				index+1,
			)
		}
		sourceKind, err := validationKindForColumn(sourceColumn)
		if err != nil {
			return nil, nil, "", err
		}
		targetKind, err := validationKindForColumn(targetColumn)
		if err != nil {
			return nil, nil, "", err
		}
		if !sameAdapterValidationKeyType(
			sourceColumn.Type,
			targetColumn.Type,
		) {
			return nil, nil, "", fmt.Errorf(
				"source and target validation primary-key column %s types differ",
				sourceColumn.Name,
			)
		}
		if sourceKind != targetKind ||
			!adapterValidationKeyKindCertified(
				engineName,
				sourceKind,
			) {
			return nil, nil, "", fmt.Errorf(
				"validation primary-key column %s lacks a certified %s equality and ordering domain",
				sourceColumn.Name,
				engineName,
			)
		}
		wire.Columns[index] = proofColumn{
			Name:       sourceColumn.Name,
			Position:   sourceColumn.PrimaryKeyPosition,
			SourceKind: sourceKind,
			TargetKind: targetKind,
			SourceType: sourceColumn.Type,
			TargetType: targetColumn.Type,
		}
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		return nil, nil, "", fmt.Errorf(
			"encode validation primary-key equality proof: %w",
			err,
		)
	}
	digest := sha256.Sum256(encoded)
	return sourcePrimaryKey, targetPrimaryKey,
		hex.EncodeToString(digest[:]), nil
}

func adapterValidationKeyKindCertified(
	engineName string,
	kind validationValueKind,
) bool {
	switch engineName {
	case adapterValidationPostgres:
		switch kind {
		case validationBoolean,
			validationInteger,
			validationDecimal,
			validationBytes,
			validationDate,
			validationTime,
			validationTimestamp,
			validationUUID:
			return true
		}
	case adapterValidationSQLite:
		return kind == validationInteger ||
			kind == validationBytes
	}
	return false
}

func sameAdapterValidationKeyType(left string, right string) bool {
	normalize := func(value string) string {
		return strings.ToLower(strings.Join(
			strings.Fields(value),
			" ",
		))
	}
	return normalize(left) != "" &&
		normalize(left) == normalize(right)
}

func adapterValidationPrimaryKey(
	table schema.Table,
) ([]schema.Column, error) {
	positions := make(map[int]schema.Column)
	for _, column := range table.Columns {
		if !column.PrimaryKey {
			if column.PrimaryKeyPosition != 0 {
				return nil, fmt.Errorf(
					"non-primary-key column %s has a key position",
					column.Name,
				)
			}
			continue
		}
		if column.Nullable || column.PrimaryKeyPosition <= 0 {
			return nil, fmt.Errorf(
				"primary-key column %s is nullable or unpositioned",
				column.Name,
			)
		}
		if _, duplicate := positions[column.PrimaryKeyPosition]; duplicate {
			return nil, fmt.Errorf(
				"primary-key position %d is duplicated",
				column.PrimaryKeyPosition,
			)
		}
		positions[column.PrimaryKeyPosition] = column
	}
	if len(positions) == 0 {
		return nil, errors.New("primary key is required")
	}
	result := make([]schema.Column, len(positions))
	for position := 1; position <= len(result); position++ {
		column, ok := positions[position]
		if !ok {
			return nil, errors.New(
				"primary-key positions are not contiguous",
			)
		}
		result[position-1] = column
	}
	return result, nil
}

func adapterValidationOrderBy(
	endpoint adapterValidationSQLEndpoint,
	primaryKey []schema.Column,
) (string, error) {
	terms := make([]string, len(primaryKey))
	for index, column := range primaryKey {
		kind, err := validationKindForColumn(column)
		if err != nil {
			return "", err
		}
		if !adapterValidationKeyKindCertified(
			endpoint.engine,
			kind,
		) {
			return "", fmt.Errorf(
				"primary-key column %s lacks certified ordering",
				column.Name,
			)
		}
		terms[index] = endpoint.quote(column.Name)
	}
	return strings.Join(terms, ", "), nil
}

func adapterValidationKeyBatchSize(
	parameterLimit int,
	keyWidth int,
) (int, error) {
	if parameterLimit < 1 || keyWidth < 1 ||
		keyWidth > parameterLimit {
		return 0, errors.New(
			"validation key width exceeds the target parameter limit",
		)
	}
	result := parameterLimit / keyWidth
	if result > adapterValidationMaximumKeyBatch {
		result = adapterValidationMaximumKeyBatch
	}
	if result < 1 {
		return 0, errors.New(
			"validation key batch size is invalid",
		)
	}
	return result, nil
}

func adapterValidationKeyPredicate(
	endpoint adapterValidationSQLEndpoint,
	primaryKey []schema.Column,
	keys []ValidationPrimaryKey,
) (string, []any, error) {
	if len(keys) == 0 {
		return "", nil, errors.New(
			"validation key predicate requires at least one key",
		)
	}
	if len(primaryKey) == 0 ||
		len(keys)*len(primaryKey) > endpoint.parameterLimit {
		return "", nil, errors.New(
			"validation key predicate exceeds the parameter limit",
		)
	}
	arguments := make([]any, 0, len(keys)*len(primaryKey))
	clauses := make([]string, len(keys))
	position := 1
	for keyIndex, key := range keys {
		if len(key.Values) != len(primaryKey) {
			return "", nil, fmt.Errorf(
				"validation key %d has an invalid width",
				keyIndex,
			)
		}
		terms := make([]string, len(primaryKey))
		for columnIndex, column := range primaryKey {
			value := key.Values[columnIndex]
			if value == nil {
				return "", nil, fmt.Errorf(
					"validation key %d contains NULL",
					keyIndex,
				)
			}
			terms[columnIndex] = endpoint.quote(column.Name) +
				" = " + endpoint.placeholder(position)
			position++
			arguments = append(
				arguments,
				cloneAdapterValidationValue(value),
			)
		}
		clauses[keyIndex] = "(" + strings.Join(terms, " AND ") + ")"
	}
	return "(" + strings.Join(clauses, " OR ") + ")", arguments, nil
}

func adapterValidationCanonicalKeySet(
	primaryKey []schema.Column,
	keys []ValidationPrimaryKey,
) (map[string]struct{}, error) {
	descriptor, err := adapterValidationKeyDescriptor(primaryKey)
	if err != nil {
		return nil, err
	}
	result := make(map[string]struct{}, len(keys))
	for index, key := range keys {
		canonical, err := adapterValidationCanonicalKey(
			descriptor,
			key.Values,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"validation key %d is invalid: %w",
				index,
				err,
			)
		}
		if _, duplicate := result[string(canonical)]; duplicate {
			return nil, fmt.Errorf(
				"validation key %d duplicates an earlier complete primary key",
				index,
			)
		}
		result[string(canonical)] = struct{}{}
	}
	return result, nil
}

func adapterValidationKeyDescriptor(
	primaryKey []schema.Column,
) (validationSampleDescriptor, error) {
	if len(primaryKey) == 0 {
		return validationSampleDescriptor{}, errors.New(
			"validation primary key is empty",
		)
	}
	descriptor := validationSampleDescriptor{
		Columns: make([]validationColumnDescriptor, len(primaryKey)),
	}
	for index, column := range primaryKey {
		if column.Name == "" {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation primary-key column %d has no name",
				index+1,
			)
		}
		kind, err := validationKindForColumn(column)
		if err != nil {
			return validationSampleDescriptor{}, fmt.Errorf(
				"validation primary-key column %d: %w",
				index+1,
				err,
			)
		}
		descriptor.Columns[index] = validationColumnDescriptor{
			Name: column.Name,
			Kind: kind,
		}
	}
	return descriptor, nil
}

func adapterValidationCanonicalKey(
	descriptor validationSampleDescriptor,
	values []any,
) ([]byte, error) {
	if len(values) != len(descriptor.Columns) {
		return nil, errors.New(
			"validation key width does not match its descriptor",
		)
	}
	for index, value := range values {
		if value == nil {
			return nil, fmt.Errorf(
				"validation primary-key column %d is NULL",
				index+1,
			)
		}
	}
	return canonicalValidationRow(descriptor, values)
}

func validateAdapterValidationTargetRows(
	table schema.Table,
	projection []string,
	primaryKey []schema.Column,
	requested map[string]struct{},
	rows []ValidationSampleRow,
) error {
	descriptor, err := adapterValidationKeyDescriptor(primaryKey)
	if err != nil {
		return err
	}
	projectionIndexes := make(map[string]int, len(projection))
	for index, name := range projection {
		projectionIndexes[name] = index
	}
	keyIndexes := make([]int, len(primaryKey))
	for index, column := range primaryKey {
		position, exists := projectionIndexes[column.Name]
		if !exists {
			return fmt.Errorf(
				"target validation projection for table %s omits primary-key column %s",
				table.Name,
				column.Name,
			)
		}
		keyIndexes[index] = position
	}
	seen := make(map[string]struct{}, len(rows))
	for rowIndex, row := range rows {
		if len(row.Values) != len(projection) {
			return fmt.Errorf(
				"target validation sample row %d has an invalid width",
				rowIndex,
			)
		}
		values := make([]any, len(keyIndexes))
		for index, projectionIndex := range keyIndexes {
			values[index] = row.Values[projectionIndex]
		}
		canonical, err := adapterValidationCanonicalKey(
			descriptor,
			values,
		)
		if err != nil {
			return fmt.Errorf(
				"target validation sample row %d has an invalid primary key: %w",
				rowIndex,
				err,
			)
		}
		key := string(canonical)
		if _, exists := requested[key]; !exists {
			return fmt.Errorf(
				"target validation sample row %d has an unrequested primary key",
				rowIndex,
			)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf(
				"target validation sample row %d duplicates an earlier primary key",
				rowIndex,
			)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func scanAdapterValidationRows(
	rows *sql.Rows,
	width int,
	limit int,
	operation string,
) ([]ValidationSampleRow, error) {
	if rows == nil {
		return nil, fmt.Errorf("%s returned no row stream", operation)
	}
	defer rows.Close()
	result := make([]ValidationSampleRow, 0)
	for rows.Next() {
		if len(result) >= limit {
			return nil, fmt.Errorf(
				"%s exceeded its admitted row bound",
				operation,
			)
		}
		values, destinations := adapterValidationScanBuffers(width)
		if err := rows.Scan(destinations...); err != nil {
			return nil, fmt.Errorf("%s row scan failed: %w", operation, err)
		}
		for index := range values {
			values[index] = cloneAdapterValidationValue(values[index])
		}
		result = append(result, ValidationSampleRow{Values: values})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s iteration failed: %w", operation, err)
	}
	return result, nil
}

func scanAdapterValidationPrimaryKey(
	rows *sql.Rows,
	width int,
) (ValidationPrimaryKey, error) {
	values, destinations := adapterValidationScanBuffers(width)
	if err := rows.Scan(destinations...); err != nil {
		return ValidationPrimaryKey{}, fmt.Errorf(
			"scan source validation primary key: %w",
			err,
		)
	}
	for index := range values {
		if values[index] == nil {
			return ValidationPrimaryKey{}, fmt.Errorf(
				"source validation primary-key column %d is NULL",
				index+1,
			)
		}
		values[index] = cloneAdapterValidationValue(values[index])
	}
	return ValidationPrimaryKey{Values: values}, nil
}

func adapterValidationScanBuffers(width int) ([]any, []any) {
	values := make([]any, width)
	destinations := make([]any, width)
	for index := range values {
		destinations[index] = &values[index]
	}
	return values, destinations
}

func cloneAdapterValidationValue(value any) any {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...)
	case sql.RawBytes:
		return append([]byte(nil), typed...)
	default:
		return value
	}
}

func adapterValidationColumnNames(
	columns []schema.Column,
) []string {
	result := make([]string, len(columns))
	for index, column := range columns {
		result[index] = column.Name
	}
	return result
}

func adapterValidationQuotedColumns(
	endpoint adapterValidationSQLEndpoint,
	columns []string,
) string {
	result := make([]string, len(columns))
	for index, column := range columns {
		result[index] = endpoint.quote(column)
	}
	return strings.Join(result, ", ")
}

func (endpoint adapterValidationSQLEndpoint) qualified(
	table schema.Table,
) string {
	if endpoint.engine == adapterValidationSQLite {
		return endpoint.quote(table.Name)
	}
	return endpoint.quote(table.Schema) + "." +
		endpoint.quote(table.Name)
}

func (endpoint adapterValidationSQLEndpoint) quote(
	identifier string,
) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func (endpoint adapterValidationSQLEndpoint) placeholder(
	position int,
) string {
	if endpoint.engine == adapterValidationPostgres {
		return fmt.Sprintf("$%d", position)
	}
	return "?"
}

func isNilAdapterValidationQueryer(
	queryer adapterValidationSQLQueryer,
) bool {
	if queryer == nil {
		return true
	}
	value := reflect.ValueOf(queryer)
	switch value.Kind() {
	case reflect.Chan,
		reflect.Func,
		reflect.Interface,
		reflect.Map,
		reflect.Pointer,
		reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func sameAdapterValidationQueryer(
	left adapterValidationSQLQueryer,
	right adapterValidationSQLQueryer,
) bool {
	if isNilAdapterValidationQueryer(left) ||
		isNilAdapterValidationQueryer(right) {
		return false
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if leftValue.Type() != rightValue.Type() ||
		leftValue.Kind() != reflect.Pointer ||
		rightValue.Kind() != reflect.Pointer {
		return false
	}
	return leftValue.Pointer() == rightValue.Pointer()
}

var (
	_ adapterStage4ValidationProbeProvider         = (*relationalSourceAdapter)(nil)
	_ adapterStage4ValidationProbeProvider         = (*adapterRetainedStableRelationalView)(nil)
	_ adapterStage4ValidationProbeProvider         = (*sqliteSourceAdapter)(nil)
	_ adapterStage4ValidationEqualityProofProvider = (*adapterDatabaseValidationProbe)(nil)
	_ ValidationCoreProbe                          = (*adapterDatabaseValidationProbe)(nil)
)
