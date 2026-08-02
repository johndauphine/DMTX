package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/johndauphine/dmtx/internal/schema"
)

// Proving and rendering an evolution, classifying execution failures, and
// validating the catalog the target actually ended up with.

func proveAndRenderTargetSchemaEvolution(
	target schema.Dialect,
	before []schema.Table,
	after []schema.Table,
	specification targetSchemaEvolutionSpecification,
) (string, error) {
	key := targetSchemaEvolutionTableKey{
		schema: specification.object.Schema,
		table:  specification.object.Table,
	}
	beforeIndex := findTargetSchemaEvolutionTable(before, key)
	afterIndex := findTargetSchemaEvolutionTable(after, key)
	if beforeIndex < 0 || afterIndex < 0 {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"evolution proof table is missing for "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	beforeColumn, beforeExists := findTargetSchemaEvolutionColumn(
		before[beforeIndex],
		specification.object.Column,
	)
	afterColumn, afterExists := findTargetSchemaEvolutionColumn(
		after[afterIndex],
		specification.object.Column,
	)
	if !afterExists {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"evolution proof current column is missing for "+
				targetSchemaEvolutionObjectName(specification.object),
			nil,
		)
	}
	var (
		proof schema.ColumnEvolution
		err   error
	)
	switch specification.action {
	case SchemaContractAddColumn:
		if beforeExists {
			return "", targetSchemaEvolutionError(
				TargetSchemaEvolutionInvalidPlan,
				"preflight",
				"nullable-column proof found the column in prior projection",
				nil,
			)
		}
		beforeSnapshot, snapshotErr := schema.NewSchemaSnapshot(
			[]schema.Table{before[beforeIndex]},
		)
		if snapshotErr != nil {
			err = snapshotErr
			break
		}
		proof, err = schema.PlanAddNullableColumn(
			beforeSnapshot.Tables[0],
			after[afterIndex],
			afterColumn,
		)
	case SchemaContractRelaxNullability:
		if !beforeExists {
			err = fmt.Errorf("prior column is missing")
			break
		}
		proof, err = schema.PlanRelaxNullability(
			after[afterIndex],
			beforeColumn,
			afterColumn,
		)
	case SchemaContractWidenType:
		if !beforeExists {
			err = fmt.Errorf("prior column is missing")
			break
		}
		complete, catalogErr := schema.NewCompleteEvolutionCatalog(after)
		if catalogErr != nil {
			err = catalogErr
			break
		}
		proof, err = schema.PlanSafeTypeWidening(
			complete,
			after[afterIndex],
			beforeColumn,
			afterColumn,
		)
	}
	if err != nil {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"independent evolution proof rejected "+
				string(specification.action)+" for "+
				targetSchemaEvolutionObjectName(specification.object),
			err,
		)
	}
	// SQLite has no safe in-place form for either operation.  The generic
	// proof above remains the authority for what can change; only the target
	// adapter owns the physical retained-row rebuild needed to enact it.
	if target == schema.SQLite &&
		(specification.action == SchemaContractRelaxNullability ||
			specification.action == SchemaContractWidenType) {
		return sqliteTargetEvolutionCopySwapStatement, nil
	}
	statement, err := schema.RenderColumnEvolution(target, proof)
	if err != nil {
		return "", targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"preflight",
			"render proved "+string(specification.action)+" for "+
				targetSchemaEvolutionObjectName(specification.object),
			err,
		)
	}
	return statement, nil
}

func matchTargetSchemaEvolutionState(
	states [][]schema.Table,
	reservations []TargetSchemaEvolutionNameReservation,
	actual TargetSchemaEvolutionCatalog,
) (int, error) {
	if !reflect.DeepEqual(
		canonicalTargetSchemaEvolutionReservations(reservations),
		canonicalTargetSchemaEvolutionReservations(actual.reservations),
	) {
		return 0, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"catalog comparison",
			"target namespace name reservations changed outside the deterministic evolution plan",
			nil,
		)
	}
	matches := make([]int, 0, 1)
	for index, expected := range states {
		equal, err := equalCanonicalTargetSchemaEvolutionCatalog(
			expected,
			actual.tables,
		)
		if err != nil {
			return 0, targetSchemaEvolutionError(
				TargetSchemaEvolutionCatalogDrift,
				"catalog comparison",
				fmt.Sprintf("compare catalog with evolution prefix %d", index),
				err,
			)
		}
		if equal {
			matches = append(matches, index)
		}
	}
	if len(matches) == 0 {
		return 0, targetSchemaEvolutionError(
			TargetSchemaEvolutionCatalogDrift,
			"catalog comparison",
			"target catalog is neither the exact prior shape, the exact desired shape, nor a deterministic applied prefix",
			nil,
		)
	}
	if len(matches) != 1 {
		return 0, targetSchemaEvolutionError(
			TargetSchemaEvolutionInvalidPlan,
			"catalog comparison",
			"multiple evolution prefixes have indistinguishable catalog shapes",
			nil,
		)
	}
	return matches[0], nil
}

func classifyTargetSchemaEvolutionExecutionFailure(
	plan TargetSchemaEvolutionPlan,
	startPrefix int,
	after TargetSchemaEvolutionCatalog,
	readErr error,
	executeErr error,
) error {
	if readErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(
				"execution failed and the complete target catalog could not be read",
			),
			fmt.Errorf("%w; catalog read: %v", executeErr, readErr),
		)
	}
	if err := validateTargetSchemaEvolutionCatalog(after); err != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(
				"execution failed and left structurally invalid catalog evidence",
			),
			fmt.Errorf("%w; catalog validation: %v", executeErr, err),
		)
	}
	prefix, matchErr := matchTargetSchemaEvolutionState(
		plan.states,
		plan.reservations,
		after,
	)
	if matchErr != nil {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(
				"execution failed and left unexpected or mixed catalog drift",
			),
			fmt.Errorf("%w; catalog classification: %v", executeErr, matchErr),
		)
	}
	if prefix < startPrefix {
		return targetSchemaEvolutionError(
			TargetSchemaEvolutionApplyFailed,
			"execution",
			targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
				"execution failed and catalog regressed from prefix %d to %d",
				startPrefix,
				prefix,
			)),
			executeErr,
		)
	}
	return targetSchemaEvolutionError(
		TargetSchemaEvolutionApplyFailed,
		"execution",
		targetSchemaEvolutionRecoveryWording(fmt.Sprintf(
			"execution failed after verified prefix %d of %d",
			prefix,
			len(plan.operations),
		)),
		executeErr,
	)
}

func targetSchemaEvolutionRecoveryWording(detail string) string {
	return detail +
		"; rerun the same migration or resume so DMTX can re-read the complete target catalog and continue only from an exact verified prefix; if preflight reports catalog drift, repair or restore the target, or rebuild the affected target shape, before retrying"
}

func validateTargetSchemaEvolutionCatalog(
	catalog TargetSchemaEvolutionCatalog,
) error {
	if len(catalog.tables) != 0 {
		if _, err := schema.NewCompleteEvolutionCatalog(catalog.tables); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(catalog.reservations))
	for index, reservation := range catalog.reservations {
		if strings.TrimSpace(reservation.Scope) == "" ||
			reservation.Scope != strings.TrimSpace(reservation.Scope) ||
			strings.TrimSpace(reservation.Namespace) == "" ||
			reservation.Namespace != strings.TrimSpace(reservation.Namespace) ||
			strings.TrimSpace(reservation.Name) == "" ||
			reservation.Name != strings.TrimSpace(reservation.Name) {
			return fmt.Errorf(
				"target name reservation %d has a non-canonical scope, namespace, or name",
				index,
			)
		}
		key := reservation.Scope + "\x00" +
			reservation.Namespace + "\x00" +
			reservation.Name
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf(
				"duplicate target name reservation %s/%s/%s",
				reservation.Scope,
				reservation.Namespace,
				reservation.Name,
			)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateTargetSchemaCreateStepSubset(
	previous schema.SchemaSnapshot,
	current schema.SchemaSnapshot,
	requested schema.SchemaSnapshot,
) error {
	previousTables := indexTargetSchemaCreateSnapshotTables(previous)
	currentTables := indexTargetSchemaCreateSnapshotTables(current)
	requestedTables := indexTargetSchemaCreateSnapshotTables(requested)
	for key, table := range currentTables {
		finalTable, exists := requestedTables[key]
		if !exists {
			return fmt.Errorf(
				"statement introduces unrequested table %s",
				targetSchemaEvolutionTableName(key),
			)
		}
		if !equalTargetSchemaCreateTableCore(table, finalTable) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Indexes,
				finalTable.Indexes,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.ForeignKeys,
				finalTable.ForeignKeys,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Checks,
				finalTable.Checks,
			) {
			return fmt.Errorf(
				"statement result for %s is not a structural subset of the requested complete shape",
				targetSchemaEvolutionTableName(key),
			)
		}
	}
	for key, table := range previousTables {
		next, exists := currentTables[key]
		if !exists {
			return fmt.Errorf(
				"statement removes previously created table %s",
				targetSchemaEvolutionTableName(key),
			)
		}
		if !equalTargetSchemaCreateTableCore(table, next) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Indexes,
				next.Indexes,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.ForeignKeys,
				next.ForeignKeys,
			) ||
			!targetSchemaEvolutionSnapshotSubset(
				table.Checks,
				next.Checks,
			) {
			return fmt.Errorf(
				"statement removes or changes previously created shape for %s",
				targetSchemaEvolutionTableName(key),
			)
		}
	}
	if previous.Version != 0 {
		equal, err := schema.SchemaSnapshotsEqual(previous, current)
		if err != nil {
			return err
		}
		if equal {
			return fmt.Errorf("statement does not advance the declared catalog shape")
		}
	}
	return nil
}

func indexTargetSchemaCreateSnapshotTables(
	snapshot schema.SchemaSnapshot,
) map[targetSchemaEvolutionTableKey]schema.SnapshotTable {
	result := make(
		map[targetSchemaEvolutionTableKey]schema.SnapshotTable,
		len(snapshot.Tables),
	)
	for _, table := range snapshot.Tables {
		result[targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = table
	}
	return result
}

func equalTargetSchemaCreateTableCore(
	left schema.SnapshotTable,
	right schema.SnapshotTable,
) bool {
	left.Indexes = nil
	left.ForeignKeys = nil
	left.Checks = nil
	right.Indexes = nil
	right.ForeignKeys = nil
	right.Checks = nil
	return reflect.DeepEqual(left, right)
}

func targetSchemaEvolutionSnapshotSubset[T any](
	subset []T,
	superset []T,
) bool {
	counts := make(map[string]int, len(superset))
	for _, value := range superset {
		encoded, err := json.Marshal(value)
		if err != nil {
			return false
		}
		counts[string(encoded)]++
	}
	for _, value := range subset {
		encoded, err := json.Marshal(value)
		if err != nil {
			return false
		}
		key := string(encoded)
		if counts[key] == 0 {
			return false
		}
		counts[key]--
	}
	return true
}

func replaceTargetSchemaEvolutionCreatedTables(
	current []schema.Table,
	objects []schema.SchemaDriftObject,
	created []schema.Table,
) []schema.Table {
	keys := make(map[targetSchemaEvolutionTableKey]struct{}, len(objects))
	for _, object := range objects {
		keys[targetSchemaEvolutionTableKey{
			schema: object.Schema,
			table:  object.Table,
		}] = struct{}{}
	}
	result := make([]schema.Table, 0, len(current)+len(created))
	for _, table := range current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		if _, isCreated := keys[key]; isCreated {
			continue
		}
		result = append(result, cloneStage4RichTable(table))
	}
	result = append(result, cloneTargetSchemaEvolutionTables(created)...)
	sortTargetSchemaEvolutionTables(result)
	return result
}

func changedTargetSchemaCreateObjects(
	previous []schema.Table,
	current []schema.Table,
) []schema.SchemaDriftObject {
	previousSnapshots := make(map[targetSchemaEvolutionTableKey]schema.SchemaSnapshot)
	for _, table := range previous {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		snapshot, _ := schema.NewSchemaSnapshot([]schema.Table{table})
		previousSnapshots[key] = snapshot
	}
	result := make([]schema.SchemaDriftObject, 0, len(current))
	for _, table := range current {
		key := targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}
		currentSnapshot, _ := schema.NewSchemaSnapshot([]schema.Table{table})
		previousSnapshot, existed := previousSnapshots[key]
		equal := false
		if existed {
			equal, _ = schema.SchemaSnapshotsEqual(
				previousSnapshot,
				currentSnapshot,
			)
		}
		if !equal {
			result = append(result, schema.SchemaDriftObject{
				Kind:   schema.SchemaDriftObjectTable,
				Schema: table.Schema,
				Table:  table.Name,
			})
		}
	}
	sort.Slice(result, func(left, right int) bool {
		return targetSchemaEvolutionObjectName(result[left]) <
			targetSchemaEvolutionObjectName(result[right])
	})
	return result
}

func equalCanonicalTargetSchemaEvolutionCatalog(
	expected []schema.Table,
	actual []schema.Table,
) (bool, error) {
	expectedSnapshot, err := schema.NewSchemaSnapshot(expected)
	if err != nil {
		return false, err
	}
	actualSnapshot, err := schema.NewSchemaSnapshot(actual)
	if err != nil {
		return false, err
	}
	return schema.SchemaSnapshotsEqual(expectedSnapshot, actualSnapshot)
}

func digestTargetSchemaEvolutionPlan(
	target schema.Dialect,
	authorityDigest string,
	reservations []TargetSchemaEvolutionNameReservation,
	operations []TargetSchemaEvolutionOperation,
	states [][]schema.Table,
) (string, error) {
	type digestOperation struct {
		Action       SchemaContractAction       `json:"action"`
		Objects      []schema.SchemaDriftObject `json:"objects"`
		Statements   []string                   `json:"statements"`
		BeforeDigest string                     `json:"before_digest"`
		AfterDigest  string                     `json:"after_digest"`
	}
	value := struct {
		Target          schema.Dialect                         `json:"target"`
		AuthorityDigest string                                 `json:"authority_digest"`
		Reservations    []TargetSchemaEvolutionNameReservation `json:"reservations"`
		Operations      []digestOperation                      `json:"operations"`
		States          []string                               `json:"states"`
	}{
		Target:          target,
		AuthorityDigest: authorityDigest,
		Reservations: canonicalTargetSchemaEvolutionReservations(
			reservations,
		),
		Operations: make([]digestOperation, len(operations)),
		States:     make([]string, len(states)),
	}
	for index, operation := range operations {
		value.Operations[index] = digestOperation{
			Action:       operation.action,
			Objects:      append([]schema.SchemaDriftObject(nil), operation.objects...),
			Statements:   append([]string(nil), operation.statements...),
			BeforeDigest: operation.beforeDigest,
			AfterDigest:  operation.afterDigest,
		}
	}
	for index, state := range states {
		digest, err := digestTargetSchemaEvolutionCatalog(state)
		if err != nil {
			return "", err
		}
		value.States[index] = digest
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func digestTargetSchemaEvolutionCatalog(tables []schema.Table) (string, error) {
	snapshot, err := schema.NewSchemaSnapshot(tables)
	if err != nil {
		return "", err
	}
	return snapshot.Digest()
}

func indexTargetSchemaEvolutionTables(
	tables []schema.Table,
) map[targetSchemaEvolutionTableKey]schema.Table {
	result := make(map[targetSchemaEvolutionTableKey]schema.Table, len(tables))
	for _, table := range tables {
		result[targetSchemaEvolutionTableKey{
			schema: table.Schema,
			table:  table.Name,
		}] = cloneStage4RichTable(table)
	}
	return result
}

func findTargetSchemaEvolutionTable(
	tables []schema.Table,
	key targetSchemaEvolutionTableKey,
) int {
	for index, table := range tables {
		if table.Schema == key.schema && table.Name == key.table {
			return index
		}
	}
	return -1
}

func findTargetSchemaEvolutionColumn(
	table schema.Table,
	name string,
) (schema.Column, bool) {
	index := findTargetSchemaEvolutionColumnIndex(table, name)
	if index < 0 {
		return schema.Column{}, false
	}
	return cloneStage4RichColumn(table.Columns[index]), true
}

func findTargetSchemaEvolutionColumnIndex(
	table schema.Table,
	name string,
) int {
	for index, column := range table.Columns {
		if column.Name == name {
			return index
		}
	}
	return -1
}

func targetSchemaEvolutionColumnPosition(
	tables map[targetSchemaEvolutionTableKey]schema.Table,
	object schema.SchemaDriftObject,
) int {
	table := tables[targetSchemaEvolutionTableKey{
		schema: object.Schema,
		table:  object.Table,
	}]
	return findTargetSchemaEvolutionColumnIndex(table, object.Column)
}

func targetSchemaEvolutionActionOrder(action SchemaContractAction) int {
	switch action {
	case SchemaContractCreateTable:
		return 0
	case SchemaContractAddColumn:
		return 1
	case SchemaContractRelaxNullability:
		return 2
	case SchemaContractWidenType:
		return 3
	default:
		return 100
	}
}

func targetSchemaEvolutionObjectName(object schema.SchemaDriftObject) string {
	name := object.Table
	if object.Schema != "" {
		name = object.Schema + "." + name
	}
	if object.Column != "" {
		name += "." + object.Column
	}
	return name
}

func targetSchemaEvolutionTableName(key targetSchemaEvolutionTableKey) string {
	if key.schema == "" {
		return key.table
	}
	return key.schema + "." + key.table
}

func sortTargetSchemaEvolutionTables(tables []schema.Table) {
	sort.Slice(tables, func(left, right int) bool {
		leftKey := tables[left].Schema + "\x00" + tables[left].Name
		rightKey := tables[right].Schema + "\x00" + tables[right].Name
		return leftKey < rightKey
	})
}

func cloneTargetSchemaEvolutionTables(
	tables []schema.Table,
) []schema.Table {
	if tables == nil {
		return nil
	}
	result := make([]schema.Table, len(tables))
	for index, table := range tables {
		result[index] = cloneStage4RichTable(table)
	}
	return result
}

func cloneTargetSchemaEvolutionStates(
	states [][]schema.Table,
) [][]schema.Table {
	result := make([][]schema.Table, len(states))
	for index, state := range states {
		result[index] = cloneTargetSchemaEvolutionTables(state)
	}
	return result
}

func cloneTargetSchemaEvolutionCatalog(
	catalog TargetSchemaEvolutionCatalog,
) TargetSchemaEvolutionCatalog {
	return TargetSchemaEvolutionCatalog{
		tables: cloneTargetSchemaEvolutionTables(catalog.tables),
		reservations: cloneTargetSchemaEvolutionReservations(
			catalog.reservations,
		),
	}
}

func cloneTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) []TargetSchemaEvolutionNameReservation {
	return append(
		[]TargetSchemaEvolutionNameReservation(nil),
		reservations...,
	)
}

func sortTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) {
	sort.Slice(reservations, func(left, right int) bool {
		leftKey := reservations[left].Scope + "\x00" +
			reservations[left].Namespace + "\x00" +
			reservations[left].Name
		rightKey := reservations[right].Scope + "\x00" +
			reservations[right].Namespace + "\x00" +
			reservations[right].Name
		return leftKey < rightKey
	})
}

func canonicalTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) []TargetSchemaEvolutionNameReservation {
	result := cloneTargetSchemaEvolutionReservations(reservations)
	sortTargetSchemaEvolutionReservations(result)
	return result
}

func cloneTargetSchemaEvolutionContractDecision(
	decision SchemaContractDecision,
) SchemaContractDecision {
	decision.Previous = append(json.RawMessage(nil), decision.Previous...)
	decision.Current = append(json.RawMessage(nil), decision.Current...)
	return decision
}

func sortTargetSchemaEvolutionDecisions(
	decisions []boundTargetSchemaEvolutionDecision,
) {
	sort.Slice(decisions, func(left, right int) bool {
		leftJSON, _ := json.Marshal(struct {
			Contract SchemaContractDecision   `json:"contract"`
			Target   schema.SchemaDriftObject `json:"target"`
		}{
			Contract: decisions[left].contract,
			Target:   decisions[left].targetObject,
		})
		rightJSON, _ := json.Marshal(struct {
			Contract SchemaContractDecision   `json:"contract"`
			Target   schema.SchemaDriftObject `json:"target"`
		}{
			Contract: decisions[right].contract,
			Target:   decisions[right].targetObject,
		})
		return string(leftJSON) < string(rightJSON)
	})
}

func validateTargetSchemaEvolutionProjectionAuthority(
	request TargetSchemaEvolutionRequest,
	projection Stage4TargetSchemaEvolutionProjection,
) error {
	if request.targetMode != "upsert" {
		return fmt.Errorf(
			"in-place schema evolution requires target mode upsert, got %q",
			request.targetMode,
		)
	}
	if request.sourceEngine == "" ||
		request.targetEngine == "" ||
		request.targetEngine != string(request.target) {
		return fmt.Errorf(
			"projection route %q-to-%q does not match target dialect %q",
			request.sourceEngine,
			request.targetEngine,
			request.target,
		)
	}
	if request.sourcePrior == "" ||
		request.sourceCurrent == "" ||
		request.sourcePrior != projection.SourcePriorDigest() ||
		request.sourceCurrent != projection.SourceCurrentDigest() {
		return fmt.Errorf(
			"projection has missing or unstable durable source endpoint digests",
		)
	}
	if request.targetAuthorityTopology == "" ||
		request.targetAuthorityCatalog == "" ||
		request.targetAuthorityTopology !=
			projection.TargetAuthorityTopologyHash() ||
		request.targetAuthorityCatalog !=
			projection.TargetAuthorityCatalogDigest() ||
		!reflect.DeepEqual(
			request.targetAuthorityReservations,
			projection.TargetAuthorityReservations(),
		) {
		return fmt.Errorf(
			"projection has missing or unstable durable target catalog authority",
		)
	}
	priorDigest, err := digestTargetSchemaEvolutionCatalog(
		request.priorTables,
	)
	if err != nil {
		return fmt.Errorf("digest target-ready prior projection: %w", err)
	}
	currentDigest, err := digestTargetSchemaEvolutionCatalog(
		request.currentTables,
	)
	if err != nil {
		return fmt.Errorf("digest target-ready current projection: %w", err)
	}
	if request.projectionPrior == "" ||
		request.projectionNext == "" ||
		request.projectionPrior != priorDigest ||
		request.projectionNext != currentDigest {
		return fmt.Errorf(
			"projection digests do not match its target-ready endpoint tables",
		)
	}
	priorSnapshot, err := schema.NewSchemaSnapshot(request.priorTables)
	if err != nil {
		return fmt.Errorf(
			"snapshot target-ready prior catalog authority: %w",
			err,
		)
	}
	priorCatalogDigest, err := stage4TargetShapeCatalogDigest(
		priorSnapshot,
		request.targetAuthorityReservations,
	)
	if err != nil {
		return fmt.Errorf(
			"digest target-ready prior catalog authority: %w",
			err,
		)
	}
	if priorCatalogDigest != request.targetAuthorityCatalog {
		return fmt.Errorf(
			"projection target catalog authority does not match prior tables and reservations",
		)
	}
	if !reflect.DeepEqual(
		request.mappings,
		projection.ObjectMappings(),
	) {
		return fmt.Errorf("projection object mappings changed during construction")
	}
	seenSource := make(map[Stage4SchemaObjectIdentity]struct{}, len(request.mappings))
	seenTarget := make(map[Stage4SchemaObjectIdentity]Stage4SchemaObjectIdentity, len(request.mappings))
	for index, mapping := range request.mappings {
		if mapping.Source.Table == "" || mapping.Target.Table == "" {
			return fmt.Errorf("projection object mapping %d has no table identity", index)
		}
		if (mapping.Source.Column == "") != (mapping.Target.Column == "") {
			return fmt.Errorf(
				"projection object mapping %d changes table/column identity kind",
				index,
			)
		}
		if _, duplicate := seenSource[mapping.Source]; duplicate {
			return fmt.Errorf(
				"projection repeats source object %s",
				stage4TargetSchemaObjectIdentityString(mapping.Source),
			)
		}
		if priorSource, collision := seenTarget[mapping.Target]; collision &&
			priorSource != mapping.Source {
			return fmt.Errorf(
				"projection aliases source objects %s and %s to target object %s",
				stage4TargetSchemaObjectIdentityString(priorSource),
				stage4TargetSchemaObjectIdentityString(mapping.Source),
				stage4TargetSchemaObjectIdentityString(mapping.Target),
			)
		}
		seenSource[mapping.Source] = struct{}{}
		seenTarget[mapping.Target] = mapping.Source
	}
	requiredTargets := make(map[Stage4SchemaObjectIdentity]struct{})
	for _, tables := range [][]schema.Table{
		request.priorTables,
		request.currentTables,
	} {
		for _, table := range tables {
			requiredTargets[Stage4SchemaObjectIdentity{
				Schema: table.Schema,
				Table:  table.Name,
			}] = struct{}{}
			for _, column := range table.Columns {
				requiredTargets[Stage4SchemaObjectIdentity{
					Schema: table.Schema,
					Table:  table.Name,
					Column: column.Name,
				}] = struct{}{}
			}
		}
	}
	for target := range seenTarget {
		if _, found := requiredTargets[target]; !found {
			return fmt.Errorf(
				"projection mapping invents absent target object %s",
				stage4TargetSchemaObjectIdentityString(target),
			)
		}
	}
	return nil
}

func digestTargetSchemaEvolutionAuthority(
	request TargetSchemaEvolutionRequest,
) (string, error) {
	priorTablesDigest, err := digestTargetSchemaEvolutionCatalog(
		request.priorTables,
	)
	if err != nil {
		return "", err
	}
	currentTablesDigest, err := digestTargetSchemaEvolutionCatalog(
		request.currentTables,
	)
	if err != nil {
		return "", err
	}
	decisions := append(
		[]boundTargetSchemaEvolutionDecision(nil),
		request.decisions...,
	)
	for index := range decisions {
		decisions[index].contract = cloneTargetSchemaEvolutionContractDecision(
			decisions[index].contract,
		)
	}
	sortTargetSchemaEvolutionDecisions(decisions)
	mappings := append(
		[]Stage4TargetSchemaObjectMapping(nil),
		request.mappings...,
	)
	sort.Slice(mappings, func(left, right int) bool {
		leftKey := stage4TargetSchemaObjectIdentityKey(mappings[left].Source) +
			"\x00" +
			stage4TargetSchemaObjectIdentityKey(mappings[left].Target)
		rightKey := stage4TargetSchemaObjectIdentityKey(mappings[right].Source) +
			"\x00" +
			stage4TargetSchemaObjectIdentityKey(mappings[right].Target)
		return leftKey < rightKey
	})
	type digestDecision struct {
		Contract SchemaContractDecision   `json:"contract"`
		Target   schema.SchemaDriftObject `json:"target"`
	}
	value := struct {
		Target                      schema.Dialect                         `json:"target"`
		SourceEngine                string                                 `json:"source_engine"`
		TargetEngine                string                                 `json:"target_engine"`
		TargetMode                  string                                 `json:"target_mode"`
		SourcePrior                 string                                 `json:"source_prior"`
		SourceCurrent               string                                 `json:"source_current"`
		ProjectionPrior             string                                 `json:"projection_prior"`
		ProjectionNext              string                                 `json:"projection_next"`
		TargetAuthorityTopology     string                                 `json:"target_authority_topology"`
		TargetAuthorityCatalog      string                                 `json:"target_authority_catalog"`
		TargetAuthorityReservations []TargetSchemaEvolutionNameReservation `json:"target_authority_reservations"`
		Mappings                    []Stage4TargetSchemaObjectMapping      `json:"mappings"`
		Decisions                   []digestDecision                       `json:"decisions"`
		PriorTablesDigest           string                                 `json:"prior_tables_digest"`
		CurrentTablesDigest         string                                 `json:"current_tables_digest"`
	}{
		Target:                  request.target,
		SourceEngine:            request.sourceEngine,
		TargetEngine:            request.targetEngine,
		TargetMode:              request.targetMode,
		SourcePrior:             request.sourcePrior,
		SourceCurrent:           request.sourceCurrent,
		ProjectionPrior:         request.projectionPrior,
		ProjectionNext:          request.projectionNext,
		TargetAuthorityTopology: request.targetAuthorityTopology,
		TargetAuthorityCatalog:  request.targetAuthorityCatalog,
		TargetAuthorityReservations: canonicalTargetSchemaEvolutionReservations(
			request.targetAuthorityReservations,
		),
		Mappings:            mappings,
		Decisions:           make([]digestDecision, len(decisions)),
		PriorTablesDigest:   priorTablesDigest,
		CurrentTablesDigest: currentTablesDigest,
	}
	for index, decision := range decisions {
		value.Decisions[index] = digestDecision{
			Contract: decision.contract,
			Target:   decision.targetObject,
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func cloneTargetSchemaEvolutionOperations(
	operations []TargetSchemaEvolutionOperation,
) []TargetSchemaEvolutionOperation {
	if operations == nil {
		return nil
	}
	result := make([]TargetSchemaEvolutionOperation, len(operations))
	for index, operation := range operations {
		result[index] = TargetSchemaEvolutionOperation{
			action:       operation.action,
			objects:      append([]schema.SchemaDriftObject(nil), operation.objects...),
			statements:   append([]string(nil), operation.statements...),
			beforeDigest: operation.beforeDigest,
			afterDigest:  operation.afterDigest,
		}
	}
	return result
}

func targetSchemaEvolutionError(
	kind TargetSchemaEvolutionErrorKind,
	phase string,
	reason string,
	cause error,
) error {
	return &TargetSchemaEvolutionError{
		Kind:   kind,
		Phase:  phase,
		Reason: reason,
		Cause:  cause,
	}
}
