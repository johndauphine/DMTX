package migrate

import (
	"context"
	"fmt"
	"reflect"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Preflight and apply: the two public entry points that decide whether a
// target may evolve and then perform it.

func PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
	reader TargetSchemaEvolutionCatalogReader,
) (TargetSchemaEvolutionPlan, error) {
	if ctx == nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target schema evolution context is required",
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if isNilInterface(reader) {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target catalog reader is required",
			nil,
		)
	}
	catalog, err := reader.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionReadFailed,
			"preflight",
			"read complete target catalog",
			err,
		)
	}
	return BuildTargetSchemaEvolutionPlan(request, catalog)
}

// BuildTargetSchemaEvolutionPlan is pure: it consumes an exact catalog and
// returns an immutable plan without database I/O or target mutation.
func BuildTargetSchemaEvolutionPlan(
	request TargetSchemaEvolutionRequest,
	catalog TargetSchemaEvolutionCatalog,
) (TargetSchemaEvolutionPlan, error) {
	definition, err := prepareTargetSchemaEvolutionDefinition(request)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	if err := validateTargetSchemaEvolutionCatalog(catalog); err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"preflight",
			"target catalog is not complete and structurally valid",
			err,
		)
	}
	if !reflect.DeepEqual(
		canonicalTargetSchemaEvolutionReservations(catalog.reservations),
		canonicalTargetSchemaEvolutionReservations(
			request.targetAuthorityReservations,
		),
	) {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"preflight",
			"target catalog reservations differ from durable target-shape authority",
			nil,
		)
	}
	if err := prepareTargetSchemaEvolutionCreateBundle(
		&definition,
		request.createPlanner,
		catalog,
	); err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}

	states, operations, err := buildTargetSchemaEvolutionStates(
		definition,
		catalog.tables,
	)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	prefix, err := matchTargetSchemaEvolutionState(
		states,
		catalog.reservations,
		catalog,
	)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, err
	}
	digest, err := digestTargetSchemaEvolutionPlan(
		definition.target,
		request.authorityDigest,
		catalog.reservations,
		operations,
		states,
	)
	if err != nil {
		return TargetSchemaEvolutionPlan{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"encode immutable evolution plan",
			err,
		)
	}
	return TargetSchemaEvolutionPlan{
		target:          definition.target,
		operations:      cloneTargetSchemaEvolutionOperations(operations),
		states:          cloneTargetSchemaEvolutionStates(states),
		reservations:    cloneTargetSchemaEvolutionReservations(catalog.reservations),
		observedPrefix:  prefix,
		authorityDigest: request.authorityDigest,
		digest:          digest,
	}, nil
}

// ApplyTargetSchemaEvolution re-verifies the exact preflight state, executes
// only the pending suffix, and then verifies the exact desired catalog. An
// executor error is never returned without a best-effort catalog read that
// classifies a verified prefix when possible.
func ApplyTargetSchemaEvolution(
	ctx context.Context,
	plan TargetSchemaEvolutionPlan,
	session TargetSchemaEvolutionMutationSession,
) error {
	if ctx == nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"target schema evolution context is required",
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !plan.valid() {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"schema evolution plan is zero or internally incomplete",
			nil,
		)
	}
	if isNilInterface(session) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"apply",
			"same-session target catalog reader and executor are required",
			nil,
		)
	}
	before, err := session.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionReadFailed,
			"pre-apply verification",
			"read complete target catalog",
			err,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(before); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"pre-apply verification",
			"target catalog is not complete and structurally valid",
			err,
		)
	}
	prefix, err := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		before,
	)
	if err != nil {
		return err
	}
	if prefix != plan.observedPrefix {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"pre-apply verification",
			fmt.Sprintf(
				"catalog moved from verified prefix %d to prefix %d; rerun preflight",
				plan.observedPrefix,
				prefix,
			),
			nil,
		)
	}
	if prefix == len(plan.operations) {
		return nil
	}
	pending := cloneTargetSchemaEvolutionOperations(plan.operations[prefix:])
	executeErr := session.ExecuteTargetSchemaEvolution(ctx, pending)
	verificationCtx, cancelVerification := context.WithTimeout(
		context.WithoutCancel(ctx),
		targetSchemaEvolutionVerificationTimeout,
	)
	defer cancelVerification()
	after, readErr := session.ReadTargetSchemaEvolutionCatalog(
		verificationCtx,
	)
	if executeErr != nil {
		return classifyTargetSchemaEvolutionExecutionFailure(
			plan,
			prefix,
			after,
			readErr,
			executeErr,
		)
	}
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"execution returned success but the complete target catalog could not be read",
			),
			readErr,
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(after); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"execution returned success but the target catalog is structurally invalid",
			),
			err,
		)
	}
	afterPrefix, matchErr := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		after,
	)
	if matchErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(
				"execution returned success but the catalog has unexpected or mixed drift",
			),
			matchErr,
		)
	}
	if afterPrefix != len(plan.operations) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionVerifyFailed,
			"post-apply verification",
			targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
				"execution returned success after only prefix %d of %d",
				afterPrefix,
				len(plan.operations),
			)),
			nil,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}

type targetSchemaEvolutionDefinition struct {
	target         schema.Dialect
	prior          []schema.Table
	current        []schema.Table
	priorIndex     map[targetSchemaEvolutionTableKey]schema.Table
	currentIndex   map[targetSchemaEvolutionTableKey]schema.Table
	specifications []targetSchemaEvolutionSpecification
	createObjects  []schema.SchemaDriftObject
	createTables   []schema.Table
	createBundle   CompleteTargetSchemaCreateBundle
}

type targetSchemaEvolutionSpecification struct {
	action SchemaContractAction
	object schema.SchemaDriftObject
	order  int
}

type targetSchemaEvolutionTableKey struct {
	schema string
	table  string
}
