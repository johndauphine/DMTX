package migrate

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

// Building an evolution plan: definition preparation, column-order and
// decision validation, and the per-object states it produces.

func prepareTargetSchemaEvolutionDefinition(
	request TargetSchemaEvolutionRequest,
) (targetSchemaEvolutionDefinition, error) {
	if request.authorityDigest == "" {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target schema evolution request was not constructed from projection authority",
			nil,
		)
	}
	recomputed, digestErr := digestTargetSchemaEvolutionAuthority(request)
	if digestErr != nil || recomputed != request.authorityDigest {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target schema evolution request authority changed after construction",
			digestErr,
		)
	}
	switch request.target {
	case schema.Postgres, schema.SQLServer, schema.MySQL, schema.SQLite:
	default:
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			fmt.Sprintf(
				"target dialect %q has no in-place evolution renderer",
				request.target,
			),
			nil,
		)
	}
	priorSnapshot, err := schema.NewSchemaSnapshot(request.priorTables)
	if err != nil {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target-ready prior projection is invalid",
			err,
		)
	}
	currentSnapshot, err := schema.NewSchemaSnapshot(request.currentTables)
	if err != nil {
		return targetSchemaEvolutionDefinition{}, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target-ready current projection is invalid",
			err,
		)
	}
	_ = priorSnapshot
	_ = currentSnapshot

	definition := targetSchemaEvolutionDefinition{
		target:       request.target,
		prior:        cloneTargetSchemaEvolutionTables(request.priorTables),
		current:      cloneTargetSchemaEvolutionTables(request.currentTables),
		priorIndex:   indexTargetSchemaEvolutionTables(request.priorTables),
		currentIndex: indexTargetSchemaEvolutionTables(request.currentTables),
	}
	if err := collectTargetSchemaEvolutionSpecifications(
		request.decisions,
		&definition,
	); err != nil {
		return targetSchemaEvolutionDefinition{}, err
	}
	if err := validateTargetSchemaEvolutionColumnOrder(definition); err != nil {
		return targetSchemaEvolutionDefinition{}, err
	}
	if err := validateTargetSchemaEvolutionManagedSets(&definition); err != nil {
		return targetSchemaEvolutionDefinition{}, err
	}
	return definition, nil
}

func prepareTargetSchemaEvolutionCreateBundle(
	definition *targetSchemaEvolutionDefinition,
	planner TargetSchemaEvolutionCreatePlanner,
	actual TargetSchemaEvolutionCatalog,
) error {
	if len(definition.createTables) == 0 {
		return nil
	}
	if isNilInterface(planner) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create planner is required for create_table decisions",
			nil,
		)
	}
	createBaseline := cloneTargetSchemaEvolutionTables(definition.createTables)
	desiredBaseline := cloneTargetSchemaEvolutionTables(definition.current)
	actualBaseline := cloneTargetSchemaEvolutionCatalog(actual)

	firstCreate := cloneTargetSchemaEvolutionTables(createBaseline)
	firstDesired := cloneTargetSchemaEvolutionTables(desiredBaseline)
	firstActual := cloneTargetSchemaEvolutionCatalog(actualBaseline)
	first, err := planner.PlanCompleteTargetSchemaCreates(
		definition.target,
		firstCreate,
		firstDesired,
		firstActual,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"plan complete target create bundle",
			err,
		)
	}
	if !reflect.DeepEqual(firstCreate, createBaseline) ||
		!reflect.DeepEqual(firstDesired, desiredBaseline) ||
		!reflect.DeepEqual(firstActual, actualBaseline) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create planner mutated immutable planning evidence",
			nil,
		)
	}

	secondCreate := cloneTargetSchemaEvolutionTables(createBaseline)
	secondDesired := cloneTargetSchemaEvolutionTables(desiredBaseline)
	secondActual := cloneTargetSchemaEvolutionCatalog(actualBaseline)
	second, err := planner.PlanCompleteTargetSchemaCreates(
		definition.target,
		secondCreate,
		secondDesired,
		secondActual,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"repeat complete target create planning",
			err,
		)
	}
	if !reflect.DeepEqual(secondCreate, createBaseline) ||
		!reflect.DeepEqual(secondDesired, desiredBaseline) ||
		!reflect.DeepEqual(secondActual, actualBaseline) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"repeated complete target create planner mutated immutable planning evidence",
			nil,
		)
	}
	if !reflect.DeepEqual(first, second) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create planner returned nondeterministic statement boundaries or catalog shapes",
			nil,
		)
	}
	if first.target != definition.target {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create bundle is bound to a different dialect",
			nil,
		)
	}
	requestedSnapshot, err := schema.NewSchemaSnapshot(
		definition.createTables,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"snapshot requested complete target create tables",
			err,
		)
	}
	equal, err := schema.SchemaSnapshotsEqual(
		first.snapshot,
		requestedSnapshot,
	)
	if err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"compare complete target create bundle coverage",
			err,
		)
	}
	if !equal {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"complete target create bundle does not cover the exact requested tables and dependent objects",
			nil,
		)
	}
	definition.createBundle = first
	return nil
}

// validateTargetSchemaEvolutionColumnOrder proves that the durable source
// snapshot can reconstruct the exact physical target column order on every
// later run. In-place PostgreSQL ADD COLUMN can only append, so a source that
// inserted a new column before any retained column must fail closed instead of
// creating a target order that the next source snapshot cannot reproduce.
func validateTargetSchemaEvolutionColumnOrder(
	definition targetSchemaEvolutionDefinition,
) error {
	added := make(
		map[targetSchemaEvolutionTableKey]map[string]struct{},
	)
	for _, specification := range definition.specifications {
		if specification.action != SchemaContractAddColumn {
			continue
		}
		key := targetSchemaEvolutionTableKey{
			schema: specification.object.Schema,
			table:  specification.object.Table,
		}
		if added[key] == nil {
			added[key] = make(map[string]struct{})
		}
		added[key][specification.object.Column] = struct{}{}
	}
	for tableIndex := range definition.current {
		table := definition.current[tableIndex]
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		prior, existed := definition.priorIndex[key]
		if !existed {
			continue
		}
		priorIndex := 0
		sawAdded := false
		for _, currentColumn := range table.Columns {
			if _, isAdded := added[key][currentColumn.Name]; isAdded {
				sawAdded = true
				continue
			}
			if sawAdded ||
				priorIndex >= len(prior.Columns) ||
				prior.Columns[priorIndex].Name != currentColumn.Name {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"independent proof",
					fmt.Sprintf(
						"add_column cannot preserve exact durable target column order for %s: newly admitted columns must follow every retained column",
						targetSchemaEvolutionTableName(key),
					),
					nil,
				)
			}
			priorIndex++
		}
		if priorIndex != len(prior.Columns) {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"independent proof",
				fmt.Sprintf(
					"target column order for %s omits or reorders durable prior columns",
					targetSchemaEvolutionTableName(key),
				),
				nil,
			)
		}
	}
	return nil
}

func collectTargetSchemaEvolutionSpecifications(
	decisions []boundTargetSchemaEvolutionDecision,
	definition *targetSchemaEvolutionDefinition,
) error {
	seen := make(map[string]struct{})
	for _, bound := range decisions {
		decision := bound.contract
		if decision.Action == SchemaContractAbort {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"schema contract contains an abort decision",
				nil,
			)
		}
		switch decision.Action {
		case SchemaContractCreateTable,
			SchemaContractAddColumn,
			SchemaContractRelaxNullability,
			SchemaContractWidenType:
		default:
			continue
		}
		if err := validateTargetSchemaEvolutionDecision(decision); err != nil {
			return err
		}
		if bound.targetObject.Kind != decision.Object.Kind ||
			bound.targetObject.Name != decision.Object.Name ||
			bound.targetObject.Table == "" ||
			(decision.Object.Column != "" &&
				bound.targetObject.Column == "") {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"executable decision has invalid target object authority for "+
					targetSchemaEvolutionObjectName(decision.Object),
				nil,
			)
		}
		key := string(decision.Action) + "\x00" +
			bound.targetObject.Schema + "\x00" +
			bound.targetObject.Table + "\x00" +
			bound.targetObject.Column
		if _, duplicate := seen[key]; duplicate {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"schema contract contains a duplicate executable decision for "+
					targetSchemaEvolutionObjectName(bound.targetObject),
				nil,
			)
		}
		seen[key] = struct{}{}
		specification := targetSchemaEvolutionSpecification{
			action: decision.Action,
			object: bound.targetObject,
			order:  targetSchemaEvolutionActionOrder(decision.Action),
		}
		if decision.Action == SchemaContractCreateTable {
			definition.createObjects = append(
				definition.createObjects,
				bound.targetObject,
			)
			continue
		}
		definition.specifications = append(
			definition.specifications,
			specification,
		)
	}
	sort.Slice(definition.createObjects, func(left, right int) bool {
		return targetSchemaEvolutionObjectName(
			definition.createObjects[left],
		) < targetSchemaEvolutionObjectName(definition.createObjects[right])
	})
	for _, object := range definition.createObjects {
		table, exists := definition.currentIndex[targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}]
		if !exists {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"create_table decision has no target-ready current table "+
					targetSchemaEvolutionObjectName(object),
				nil,
			)
		}
		definition.createTables = append(
			definition.createTables,
			cloneStage4RichTable(table),
		)
	}
	sort.Slice(definition.specifications, func(left, right int) bool {
		leftSpec := definition.specifications[left]
		rightSpec := definition.specifications[right]
		leftTable := leftSpec.object.Schema + "\x00" + leftSpec.object.Table
		rightTable := rightSpec.object.Schema + "\x00" + rightSpec.object.Table
		if leftTable != rightTable {
			return leftTable < rightTable
		}
		if leftSpec.order != rightSpec.order {
			return leftSpec.order < rightSpec.order
		}
		if leftSpec.action == SchemaContractAddColumn {
			leftPosition := targetSchemaEvolutionColumnPosition(
				definition.currentIndex,
				leftSpec.object,
			)
			rightPosition := targetSchemaEvolutionColumnPosition(
				definition.currentIndex,
				rightSpec.object,
			)
			if leftPosition != rightPosition {
				return leftPosition < rightPosition
			}
		}
		if leftSpec.object.Column != rightSpec.object.Column {
			return leftSpec.object.Column < rightSpec.object.Column
		}
		return leftSpec.action < rightSpec.action
	})
	return nil
}

func validateTargetSchemaEvolutionDecision(
	decision SchemaContractDecision,
) error {
	if strings.TrimSpace(decision.Reason) == "" ||
		decision.Reason != strings.TrimSpace(decision.Reason) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"executable decision has no canonical reason",
			nil,
		)
	}
	if len(decision.Previous) == 0 || !json.Valid(decision.Previous) ||
		len(decision.Current) == 0 || !json.Valid(decision.Current) {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"executable decision has invalid previous/current evidence",
			nil,
		)
	}
	if decision.Mode != config.SchemaContractEvolve {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			fmt.Sprintf(
				"executable decision %s has non-evolve mode %q",
				decision.Action,
				decision.Mode,
			),
			nil,
		)
	}
	if decision.Object.Table == "" {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"executable decision has no table identity",
			nil,
		)
	}
	var (
		expectedEntity schema.SchemaContractEntity
		expectedChange schema.SchemaDriftChangeKind
		expectedKind   schema.SchemaDriftObjectKind
	)
	switch decision.Action {
	case SchemaContractCreateTable:
		expectedEntity = schema.SchemaContractTables
		expectedChange = schema.SchemaDriftTableAdded
		expectedKind = schema.SchemaDriftObjectTable
		if decision.Object.Column != "" {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"create_table decision unexpectedly names a column",
				nil,
			)
		}
	case SchemaContractAddColumn,
		SchemaContractRelaxNullability:
		expectedEntity = schema.SchemaContractColumns
		if decision.Action == SchemaContractAddColumn {
			expectedChange = schema.SchemaDriftColumnAdded
			expectedKind = schema.SchemaDriftObjectColumn
		} else {
			expectedChange = schema.SchemaDriftNullabilityChanged
			expectedKind = schema.SchemaDriftObjectNullability
		}
		if decision.Object.Column == "" {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(decision.Action)+" decision has no column identity",
				nil,
			)
		}
	case SchemaContractWidenType:
		expectedEntity = schema.SchemaContractDataType
		expectedChange = schema.SchemaDriftDataTypeChanged
		expectedKind = schema.SchemaDriftObjectDataType
		if decision.Object.Column == "" {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(decision.Action)+" decision has no column identity",
				nil,
			)
		}
	}
	if decision.Entity != expectedEntity ||
		decision.ChangeKind != expectedChange ||
		decision.Object.Kind != expectedKind {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			fmt.Sprintf(
				"%s decision metadata does not match its executable action for %s",
				decision.Action,
				targetSchemaEvolutionObjectName(decision.Object),
			),
			nil,
		)
	}
	return nil
}

func validateTargetSchemaEvolutionManagedSets(
	definition *targetSchemaEvolutionDefinition,
) error {
	create := make(map[targetSchemaEvolutionTableKey]struct{})
	pendingCreateObjects := make(
		[]schema.SchemaDriftObject,
		0,
		len(definition.createObjects),
	)
	pendingCreateTables := make(
		[]schema.Table,
		0,
		len(definition.createTables),
	)
	for _, object := range definition.createObjects {
		key := targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}
		if prior, exists := definition.priorIndex[key]; exists {
			current, currentExists := definition.currentIndex[key]
			if !currentExists {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"create_table authority has no current table "+
						targetSchemaEvolutionObjectName(object),
					nil,
				)
			}
			equal, err := equalCanonicalTargetSchemaEvolutionCatalog(
				[]schema.Table{prior},
				[]schema.Table{current},
			)
			if err != nil {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"compare already-satisfied create_table authority for "+
						targetSchemaEvolutionObjectName(object),
					err,
				)
			}
			if !equal {
				return targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"create_table prior projection contains an incompatible table "+
						targetSchemaEvolutionObjectName(object),
					nil,
				)
			}
			// The immutable target-shape authority proves this table already
			// satisfies the complete source-backed desired shape. Preserve the
			// audit decision but emit no DDL operation.
			continue
		}
		create[key] = struct{}{}
		pendingCreateObjects = append(pendingCreateObjects, object)
		current, exists := definition.currentIndex[key]
		if !exists {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"create_table decision has no target-ready current table "+
					targetSchemaEvolutionObjectName(object),
				nil,
			)
		}
		pendingCreateTables = append(
			pendingCreateTables,
			cloneStage4RichTable(current),
		)
	}
	definition.createObjects = pendingCreateObjects
	definition.createTables = pendingCreateTables
	for _, specification := range definition.specifications {
		key := targetSchemaEvolutionTableKey{
			schema: specification.object.Schema,
			table:  specification.object.Table,
		}
		if _, prior := definition.priorIndex[key]; !prior {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(specification.action)+
					" requires a target-ready prior projection for "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		if _, current := definition.currentIndex[key]; !current {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				string(specification.action)+
					" requires a target-ready current projection for "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
	}
	for key := range definition.currentIndex {
		if _, existed := definition.priorIndex[key]; existed {
			continue
		}
		if _, authorized := create[key]; !authorized {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"target-ready current projection adds table "+
					targetSchemaEvolutionTableName(key)+
					" without a create_table decision",
				nil,
			)
		}
	}
	for key := range definition.priorIndex {
		if _, retained := definition.currentIndex[key]; !retained {
			return targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"target-ready current projection drops prior table "+
					targetSchemaEvolutionTableName(key),
				nil,
			)
		}
	}
	return nil
}

func buildTargetSchemaEvolutionStates(
	definition targetSchemaEvolutionDefinition,
	actual []schema.Table,
) ([][]schema.Table, []TargetSchemaEvolutionOperation, error) {
	managed := make(map[targetSchemaEvolutionTableKey]struct{})
	for key := range definition.priorIndex {
		managed[key] = struct{}{}
	}
	for key := range definition.currentIndex {
		managed[key] = struct{}{}
	}
	baseline := make([]schema.Table, 0, len(actual)+len(definition.prior))
	for _, table := range actual {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, isManaged := managed[key]; isManaged {
			continue
		}
		baseline = append(baseline, cloneStage4RichTable(table))
	}
	baseline = append(
		baseline,
		cloneTargetSchemaEvolutionTables(definition.prior)...,
	)
	sortTargetSchemaEvolutionTables(baseline)

	states := [][]schema.Table{
		cloneTargetSchemaEvolutionTables(baseline),
	}
	operations := make(
		[]TargetSchemaEvolutionOperation,
		0,
		len(definition.createBundle.steps)+len(definition.specifications),
	)
	currentState := cloneTargetSchemaEvolutionTables(baseline)
	if len(definition.createTables) > 0 {
		var previousCreated []schema.Table
		for _, step := range definition.createBundle.steps {
			beforeDigest, err := digestTargetSchemaEvolutionCatalog(currentState)
			if err != nil {
				return nil, nil, err
			}
			currentState = replaceTargetSchemaEvolutionCreatedTables(
				currentState,
				definition.createObjects,
				step.tables,
			)
			if err := validateTargetSchemaEvolutionCatalog(
				TargetSchemaEvolutionCatalog{tables: currentState},
			); err != nil {
				return nil, nil, targetSchemaEvolutionError(
					TargetSchemaEvolutionInvalidPlan,
					"preflight",
					"target create step does not produce a complete valid catalog",
					err,
				)
			}
			afterDigest, err := digestTargetSchemaEvolutionCatalog(currentState)
			if err != nil {
				return nil, nil, err
			}
			objects := changedTargetSchemaCreateObjects(
				previousCreated,
				step.tables,
			)
			operations = append(operations, TargetSchemaEvolutionOperation{
				action:       SchemaContractCreateTable,
				objects:      objects,
				statements:   []string{step.statement},
				beforeDigest: beforeDigest,
				afterDigest:  afterDigest,
			})
			states = append(
				states,
				cloneTargetSchemaEvolutionTables(currentState),
			)
			previousCreated = step.tables
		}
	}
	for _, specification := range definition.specifications {
		before := cloneTargetSchemaEvolutionTables(currentState)
		after, err := applyTargetSchemaEvolutionSpecification(
			before,
			definition.currentIndex,
			specification,
		)
		if err != nil {
			return nil, nil, err
		}
		statement, err := proveAndRenderTargetSchemaEvolution(
			definition.target,
			before,
			after,
			specification,
		)
		if err != nil {
			return nil, nil, err
		}
		beforeDigest, err := digestTargetSchemaEvolutionCatalog(before)
		if err != nil {
			return nil, nil, err
		}
		afterDigest, err := digestTargetSchemaEvolutionCatalog(after)
		if err != nil {
			return nil, nil, err
		}
		operations = append(operations, TargetSchemaEvolutionOperation{
			action:       specification.action,
			objects:      []schema.SchemaDriftObject{specification.object},
			statements:   []string{statement},
			beforeDigest: beforeDigest,
			afterDigest:  afterDigest,
		})
		currentState = after
		states = append(
			states,
			cloneTargetSchemaEvolutionTables(currentState),
		)
	}

	expectedFinal := make([]schema.Table, 0, len(currentState))
	for _, table := range baseline {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, isManaged := managed[key]; isManaged {
			continue
		}
		expectedFinal = append(expectedFinal, cloneStage4RichTable(table))
	}
	expectedFinal = append(
		expectedFinal,
		cloneTargetSchemaEvolutionTables(definition.current)...,
	)
	sortTargetSchemaEvolutionTables(expectedFinal)
	equal, err := equalCanonicalTargetSchemaEvolutionCatalog(
		expectedFinal,
		currentState,
	)
	if err != nil {
		return nil, nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"compare simulated and requested target-ready current projections",
			err,
		)
	}
	if !equal {
		return nil, nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"target-ready prior and current projections contain drift not represented by executable schema-contract decisions",
			nil,
		)
	}
	return states, operations, nil
}

func applyTargetSchemaEvolutionSpecification(
	tables []schema.Table,
	current map[targetSchemaEvolutionTableKey]schema.Table,
	specification targetSchemaEvolutionSpecification,
) ([]schema.Table, error) {
	result := cloneTargetSchemaEvolutionTables(tables)
	key := targetSchemaEvolutionTableKey{
		schema: specification.object.Schema,
		table:  specification.object.Table,
	}
	tableIndex := findTargetSchemaEvolutionTable(result, key)
	if tableIndex < 0 {
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"prior target projection is missing "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	desiredTable := current[key]
	desiredColumn, desired := findTargetSchemaEvolutionColumn(
		desiredTable,
		specification.object.Column,
	)
	if !desired {
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"current target projection is missing "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	columnIndex := findTargetSchemaEvolutionColumnIndex(
		result[tableIndex],
		specification.object.Column,
	)
	switch specification.action {
	case SchemaContractAddColumn:
		if columnIndex >= 0 {
			return nil, targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"add_column prior target projection already contains "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		result[tableIndex].Columns = append(
			result[tableIndex].Columns,
			cloneStage4RichColumn(desiredColumn),
		)
	case SchemaContractRelaxNullability:
		if columnIndex < 0 {
			return nil, targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"relax_nullability prior target projection is missing "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		result[tableIndex].Columns[columnIndex].Nullable =
			desiredColumn.Nullable
	case SchemaContractWidenType:
		if columnIndex < 0 {
			return nil, targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"widen_type prior target projection is missing "+
					targetSchemaEvolutionObjectName(specification.object),
				nil,
			)
		}
		result[tableIndex].Columns[columnIndex].Type = desiredColumn.Type
		result[tableIndex].Columns[columnIndex].DeclaredType =
			cloneStage4RichColumn(desiredColumn).DeclaredType
	default:
		return nil, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"unsupported executable action "+string(specification.action),
			nil,
		)
	}
	return result, nil
}
