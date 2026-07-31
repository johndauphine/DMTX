package migrate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestTargetSchemaEvolutionPlanIsDeterministicImmutableAndComplete(
	t *testing.T,
) {
	t.Parallel()

	request, priorCatalog := targetSchemaEvolutionFixture(t)
	plan, err := BuildTargetSchemaEvolutionPlan(request, priorCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if plan.OperationCount() != 7 || plan.AppliedPrefix() != 0 ||
		plan.Complete() {
		t.Fatalf(
			"plan summary = operations %d, prefix %d, complete %t",
			plan.OperationCount(),
			plan.AppliedPrefix(),
			plan.Complete(),
		)
	}
	operations := plan.PendingOperations()
	wantActions := []SchemaContractAction{
		SchemaContractCreateTable,
		SchemaContractCreateTable,
		SchemaContractCreateTable,
		SchemaContractCreateTable,
		SchemaContractAddColumn,
		SchemaContractRelaxNullability,
		SchemaContractWidenType,
	}
	gotActions := make([]SchemaContractAction, len(operations))
	for index, operation := range operations {
		gotActions[index] = operation.Action()
		if len(operation.Statements()) != 1 {
			t.Fatalf(
				"operation %d has %d statements; want one resumable statement boundary",
				index,
				len(operation.Statements()),
			)
		}
	}
	if !reflect.DeepEqual(gotActions, wantActions) {
		t.Fatalf("actions = %#v, want %#v", gotActions, wantActions)
	}
	if got := operations[4].Statements()[0]; got !=
		`ALTER TABLE "tenant"."accounts" ADD COLUMN "label" TEXT NULL;` {
		t.Fatalf("add statement = %q", got)
	}
	if got := operations[5].Statements()[0]; got !=
		`ALTER TABLE "tenant"."accounts" ALTER COLUMN "note" DROP NOT NULL;` {
		t.Fatalf("relax statement = %q", got)
	}
	if got := operations[6].Statements()[0]; got !=
		`ALTER TABLE "tenant"."accounts" ALTER COLUMN "score" TYPE BIGINT;` {
		t.Fatalf("widen statement = %q", got)
	}

	reorderedPrior := reverseTargetSchemaEvolutionTables(request.priorTables)
	reorderedCurrent := reverseTargetSchemaEvolutionTables(request.currentTables)
	reorderedDecisions := make(
		[]SchemaContractDecision,
		len(request.decisions),
	)
	for index, decision := range request.decisions {
		reorderedDecisions[index] =
			cloneTargetSchemaEvolutionContractDecision(decision.contract)
	}
	reorderedDecisions = reverseTargetSchemaEvolutionDecisions(
		reorderedDecisions,
	)
	reordered, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		targetSchemaEvolutionFixtureProjection(
			t,
			reorderedPrior,
			reorderedCurrent,
			reorderedDecisions,
		),
		targetSchemaEvolutionFixtureCreatePlanner{},
	)
	if err != nil {
		t.Fatal(err)
	}
	reorderedCatalog, err := NewTargetSchemaEvolutionCatalog(
		reverseTargetSchemaEvolutionTables(priorCatalog.tables),
		reverseTargetSchemaEvolutionReservations(priorCatalog.reservations),
	)
	if err != nil {
		t.Fatal(err)
	}
	reorderedPlan, err := BuildTargetSchemaEvolutionPlan(
		reordered,
		reorderedCatalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Digest() != reorderedPlan.Digest() {
		t.Fatalf(
			"plan digest changed with input order: %s != %s",
			plan.Digest(),
			reorderedPlan.Digest(),
		)
	}

	request.currentTables[0].Columns[0].Name = "caller mutation"
	statementCopy := operations[0].Statements()
	statementCopy[0] = "caller mutation"
	objectCopy := operations[0].Objects()
	objectCopy[0].Table = "caller mutation"
	fresh := plan.PendingOperations()
	if fresh[0].Statements()[0] == "caller mutation" ||
		fresh[0].Objects()[0].Table == "caller mutation" {
		t.Fatal("caller mutation changed immutable evolution plan")
	}
}

func TestTargetSchemaEvolutionAcceptsOnlyExactDeterministicPrefixes(
	t *testing.T,
) {
	t.Parallel()

	request, priorCatalog := targetSchemaEvolutionFixture(t)
	initial, err := BuildTargetSchemaEvolutionPlan(request, priorCatalog)
	if err != nil {
		t.Fatal(err)
	}
	for prefix := 0; prefix <= initial.OperationCount(); prefix++ {
		prefix := prefix
		t.Run(fmt.Sprintf("prefix_%d", prefix), func(t *testing.T) {
			t.Parallel()
			resumed, buildErr := BuildTargetSchemaEvolutionPlan(
				request,
				targetSchemaEvolutionTestCatalog(
					t,
					initial.states[prefix],
					initial.reservations,
				),
			)
			if buildErr != nil {
				t.Fatal(buildErr)
			}
			if resumed.AppliedPrefix() != prefix {
				t.Fatalf(
					"applied prefix = %d, want %d",
					resumed.AppliedPrefix(),
					prefix,
				)
			}
			if resumed.Complete() != (prefix == initial.OperationCount()) {
				t.Fatalf("complete = %t at prefix %d", resumed.Complete(), prefix)
			}
		})
	}

	mixed := cloneTargetSchemaEvolutionTables(initial.states[4])
	accounts := findTargetSchemaEvolutionTable(
		mixed,
		targetSchemaEvolutionTableKey{schema: "tenant", table: "accounts"},
	)
	score := findTargetSchemaEvolutionColumnIndex(
		mixed[accounts],
		"score",
	)
	mixed[accounts].Columns[score].Type = "bigint"
	if _, err := BuildTargetSchemaEvolutionPlan(
		request,
		targetSchemaEvolutionTestCatalog(t, mixed, initial.reservations),
	); err == nil {
		t.Fatal("out-of-order mixed catalog drift was admitted")
	} else {
		assertTargetSchemaEvolutionErrorKind(
			t,
			err,
			TargetSchemaEvolutionCatalogDrift,
		)
	}

	unexpectedSafePrior := cloneTargetSchemaEvolutionTables(priorCatalog.tables)
	accounts = findTargetSchemaEvolutionTable(
		unexpectedSafePrior,
		targetSchemaEvolutionTableKey{schema: "tenant", table: "accounts"},
	)
	score = findTargetSchemaEvolutionColumnIndex(
		unexpectedSafePrior[accounts],
		"score",
	)
	unexpectedSafePrior[accounts].Columns[score].Type = "smallint"
	if _, err := BuildTargetSchemaEvolutionPlan(
		request,
		targetSchemaEvolutionTestCatalog(
			t,
			unexpectedSafePrior,
			priorCatalog.reservations,
		),
	); err == nil {
		t.Fatal("merely safe but unexpected prior target shape was admitted")
	} else {
		assertTargetSchemaEvolutionErrorKind(
			t,
			err,
			TargetSchemaEvolutionCatalogDrift,
		)
	}
}

func TestTargetSchemaEvolutionPreflightIsReadOnlyAndApplyUsesOneSession(
	t *testing.T,
) {
	t.Parallel()

	request, priorCatalog := targetSchemaEvolutionFixture(t)
	reader := &targetSchemaEvolutionFakeReader{
		catalogs: []targetSchemaEvolutionCatalogRead{{
			tables: priorCatalog.tables,
		}},
	}
	plan, err := PreflightTargetSchemaEvolution(
		context.Background(),
		request,
		reader,
	)
	if err != nil {
		t.Fatal(err)
	}
	if reader.calls != 1 {
		t.Fatalf("preflight catalog reads = %d, want 1", reader.calls)
	}

	session := &targetSchemaEvolutionFakeSession{
		catalogs: []targetSchemaEvolutionCatalogRead{
			{tables: plan.states[0]},
			{tables: plan.states[len(plan.states)-1]},
		},
	}
	if err := ApplyTargetSchemaEvolution(
		context.Background(),
		plan,
		session,
	); err != nil {
		t.Fatal(err)
	}
	if got, want := session.calls, []string{"read", "execute", "read"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("same-session calls = %#v, want %#v", got, want)
	}
	if len(session.executed) != plan.OperationCount() {
		t.Fatalf(
			"executed operations = %d, want %d",
			len(session.executed),
			plan.OperationCount(),
		)
	}
}

func TestTargetSchemaEvolutionFailureReportsVerifiedPrefixAndResumes(
	t *testing.T,
) {
	t.Parallel()

	request, priorCatalog := targetSchemaEvolutionFixture(t)
	plan, err := BuildTargetSchemaEvolutionPlan(request, priorCatalog)
	if err != nil {
		t.Fatal(err)
	}
	const durablePrefix = 5
	session := &targetSchemaEvolutionFakeSession{
		catalogs: []targetSchemaEvolutionCatalogRead{
			{tables: plan.states[0]},
			{tables: plan.states[durablePrefix]},
		},
		executeErr: errors.New("injected implicit-commit failure"),
	}
	err = ApplyTargetSchemaEvolution(
		context.Background(),
		plan,
		session,
	)
	if err == nil {
		t.Fatal("execution failure was reported as success")
	}
	assertTargetSchemaEvolutionErrorKind(
		t,
		err,
		TargetSchemaEvolutionApplyFailed,
	)
	for _, fragment := range []string{
		"verified prefix 5 of 7",
		"rerun the same migration or resume",
		"repair or restore the target",
	} {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("failure %q does not contain %q", err, fragment)
		}
	}

	resumed, err := BuildTargetSchemaEvolutionPlan(
		request,
		targetSchemaEvolutionTestCatalog(
			t,
			plan.states[durablePrefix],
			plan.reservations,
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.AppliedPrefix() != durablePrefix ||
		len(resumed.PendingOperations()) != 2 {
		t.Fatalf(
			"resumed prefix/pending = %d/%d, want 5/2",
			resumed.AppliedPrefix(),
			len(resumed.PendingOperations()),
		)
	}
	resumeSession := &targetSchemaEvolutionFakeSession{
		catalogs: []targetSchemaEvolutionCatalogRead{
			{tables: plan.states[durablePrefix]},
			{tables: plan.states[len(plan.states)-1]},
		},
	}
	if err := ApplyTargetSchemaEvolution(
		context.Background(),
		resumed,
		resumeSession,
	); err != nil {
		t.Fatal(err)
	}
	if len(resumeSession.executed) != 2 {
		t.Fatalf(
			"resume executed %d operations, want only remaining 2",
			len(resumeSession.executed),
		)
	}
}

func TestTargetSchemaEvolutionRejectsPartialCreateAndFreezesUnrelatedObjects(
	t *testing.T,
) {
	t.Parallel()

	request, priorCatalog := targetSchemaEvolutionFixture(t)
	plan, err := BuildTargetSchemaEvolutionPlan(request, priorCatalog)
	if err != nil {
		t.Fatal(err)
	}

	partialStatement := cloneTargetSchemaEvolutionTables(plan.states[1])
	events := findTargetSchemaEvolutionTable(
		partialStatement,
		targetSchemaEvolutionTableKey{schema: "tenant", table: "events"},
	)
	partialStatement[events].Indexes = append(
		partialStatement[events].Indexes,
		plan.states[2][findTargetSchemaEvolutionTable(
			plan.states[2],
			targetSchemaEvolutionTableKey{schema: "tenant", table: "events"},
		)].Indexes[0],
	)
	partialStatement[events].Indexes[0].Name = "half_written_unknown_index"
	if _, err := BuildTargetSchemaEvolutionPlan(
		request,
		targetSchemaEvolutionTestCatalog(
			t,
			partialStatement,
			plan.reservations,
		),
	); err == nil {
		t.Fatal("partially applied opaque create statement was admitted")
	} else if !strings.Contains(err.Error(), "deterministic applied prefix") {
		t.Fatalf("partial-create error lacks prefix boundary: %v", err)
	}

	changedUnrelated := cloneTargetSchemaEvolutionTables(
		plan.states[len(plan.states)-1],
	)
	audit := findTargetSchemaEvolutionTable(
		changedUnrelated,
		targetSchemaEvolutionTableKey{schema: "tenant", table: "operator_audit"},
	)
	changedUnrelated[audit].Columns = append(
		changedUnrelated[audit].Columns,
		schema.Column{Name: "external_change", Type: "text", Nullable: true},
	)
	session := &targetSchemaEvolutionFakeSession{
		catalogs: []targetSchemaEvolutionCatalogRead{
			{tables: plan.states[0]},
			{tables: changedUnrelated},
		},
	}
	err = ApplyTargetSchemaEvolution(
		context.Background(),
		plan,
		session,
	)
	if err == nil {
		t.Fatal("unrelated target mutation during apply was admitted")
	}
	assertTargetSchemaEvolutionErrorKind(
		t,
		err,
		TargetSchemaEvolutionVerifyFailed,
	)
	if !strings.Contains(err.Error(), "unexpected or mixed drift") {
		t.Fatalf("unrelated-target error = %v", err)
	}
}

func TestTargetSchemaEvolutionFailsClosedOnIncompleteIntentAndDependencies(
	t *testing.T,
) {
	t.Parallel()

	t.Run("missing prior target projection", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		request.priorTables = nil
		refreshTargetSchemaEvolutionTestAuthority(t, &request)
		if _, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		); err == nil {
			t.Fatal("non-create evolution without prior projection was admitted")
		} else if !strings.Contains(
			err.Error(),
			"requires a target-ready prior projection",
		) {
			t.Fatalf("missing-prior error = %v", err)
		}
	})

	t.Run("executable decision requires explicit evolve mode", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		request.decisions[0].contract.Mode = ""
		request.authorityDigest = targetSchemaEvolutionTestAuthorityDigest(
			t,
			request,
		)
		if _, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		); err == nil ||
			!strings.Contains(err.Error(), "non-evolve mode") {
			t.Fatalf("implicit-mode executable decision error = %v", err)
		}
	})

	t.Run("incomplete create object bundle", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		request.createPlanner =
			targetSchemaEvolutionIncompleteCreatePlanner{}
		if _, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		); err == nil {
			t.Fatal("create bundle omitting dependent objects was admitted")
		} else if !strings.Contains(
			err.Error(),
			"does not cover the exact requested tables and dependent objects",
		) {
			t.Fatalf("incomplete-create error = %v", err)
		}
	})

	t.Run("widening dependent index", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		for tableIndex := range request.priorTables {
			if request.priorTables[tableIndex].Name != "accounts" {
				continue
			}
			index := schema.Index{
				Name: "accounts_score_idx",
				Columns: []schema.IndexColumn{{
					Name: "score",
				}},
			}
			request.priorTables[tableIndex].Indexes = []schema.Index{index}
		}
		for tableIndex := range request.currentTables {
			if request.currentTables[tableIndex].Name != "accounts" {
				continue
			}
			request.currentTables[tableIndex].Indexes = []schema.Index{{
				Name: "accounts_score_idx",
				Columns: []schema.IndexColumn{{
					Name: "score",
				}},
			}}
		}
		for tableIndex := range priorCatalog.tables {
			if priorCatalog.tables[tableIndex].Name == "accounts" {
				priorCatalog.tables[tableIndex].Indexes =
					request.priorTables[0].Indexes
			}
		}
		refreshTargetSchemaEvolutionTestAuthority(t, &request)
		if _, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		); err == nil {
			t.Fatal("dependent indexed type widening was admitted")
		} else if !strings.Contains(
			err.Error(),
			"independent evolution proof rejected widen_type",
		) {
			t.Fatalf("dependent-widen error = %v", err)
		}
	})
}

func TestTargetSchemaEvolutionRejectsMidSourceColumnThatCannotRebaseline(
	t *testing.T,
) {
	t.Parallel()

	request, priorCatalog := targetSchemaEvolutionFixture(t)
	for tableIndex := range request.currentTables {
		table := &request.currentTables[tableIndex]
		if table.Name != "accounts" {
			continue
		}
		label := table.Columns[len(table.Columns)-1]
		table.Columns = append(
			[]schema.Column{table.Columns[0], label},
			table.Columns[1:len(table.Columns)-1]...,
		)
	}
	refreshTargetSchemaEvolutionTestAuthority(t, &request)
	plan, err := BuildTargetSchemaEvolutionPlan(
		request,
		priorCatalog,
	)
	if err == nil || plan.valid() ||
		!strings.Contains(
			err.Error(),
			"newly admitted columns must follow every retained column",
		) {
		t.Fatalf("mid-source add plan=%#v error=%v", plan, err)
	}
}

func TestTargetSchemaEvolutionTailAddReconstructsOnNextRun(t *testing.T) {
	t.Parallel()

	firstRequest, priorCatalog := targetSchemaEvolutionFixture(t)
	firstPlan, err := BuildTargetSchemaEvolutionPlan(
		firstRequest,
		priorCatalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	finalCatalogTables := cloneTargetSchemaEvolutionTables(
		firstPlan.states[len(firstPlan.states)-1],
	)
	finalCatalog, err := NewTargetSchemaEvolutionCatalog(
		finalCatalogTables,
		firstPlan.reservations,
	)
	if err != nil {
		t.Fatal(err)
	}
	nextProjection := targetSchemaEvolutionFixtureProjection(
		t,
		firstRequest.currentTables,
		firstRequest.currentTables,
		nil,
	)
	nextRequest, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		nextProjection,
		targetSchemaEvolutionFixtureCreatePlanner{},
	)
	if err != nil {
		t.Fatal(err)
	}
	nextPlan, err := BuildTargetSchemaEvolutionPlan(
		nextRequest,
		finalCatalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !nextPlan.Complete() || len(nextPlan.PendingOperations()) != 0 {
		t.Fatalf(
			"next-run plan complete=%v pending=%#v",
			nextPlan.Complete(),
			nextPlan.PendingOperations(),
		)
	}
}

func TestTargetSchemaEvolutionCanonicalComparisonIgnoresIdentityFrontierOnly(
	t *testing.T,
) {
	t.Parallel()

	first := int64(10)
	second := int64(99)
	expected := []schema.Table{{
		Schema: "tenant",
		Name:   "identity_table",
		Identity: &schema.Identity{
			Column: "id", Generation: schema.IdentityByDefault,
			Frontier: &first,
		},
		Columns: []schema.Column{{
			Name: "id", Type: "bigint",
			PrimaryKey: true, PrimaryKeyPosition: 1,
		}},
	}}
	actual := cloneTargetSchemaEvolutionTables(expected)
	actual[0].Identity.Frontier = &second
	equal, err := equalCanonicalTargetSchemaEvolutionCatalog(expected, actual)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatal("dynamic identity frontier changed logical schema equality")
	}
	actual[0].Columns[0].Nullable = true
	equal, err = equalCanonicalTargetSchemaEvolutionCatalog(expected, actual)
	if err == nil && equal {
		t.Fatal("logical column drift was silently normalized")
	}
}

func TestTargetSchemaEvolutionBindsSourceDecisionsToTargetNamespace(
	t *testing.T,
) {
	t.Parallel()

	base, _ := targetSchemaEvolutionFixture(t)
	sourcePrior := cloneTargetSchemaEvolutionTables(base.priorTables)
	sourceCurrent := cloneTargetSchemaEvolutionTables(base.currentTables)
	for _, tables := range [][]schema.Table{sourcePrior, sourceCurrent} {
		for tableIndex := range tables {
			tables[tableIndex].Schema = "source_schema"
			for foreignKeyIndex := range tables[tableIndex].ForeignKeys {
				if tables[tableIndex].ForeignKeys[foreignKeyIndex].
					ReferencedSchema != "" {
					tables[tableIndex].ForeignKeys[foreignKeyIndex].
						ReferencedSchema = "source_schema"
				}
			}
		}
	}
	gate := stage4TargetSchemaProjectionGate(
		t,
		sourcePrior,
		sourceCurrent,
		"upsert",
		false,
	)
	projectionTarget := &stage4TargetSchemaProjectionTestTarget{
		engine:       "postgres",
		targetSchema: "tenant",
	}
	authority := stage4TargetSchemaProjectionAuthority(
		t,
		gate,
		"mssql",
		projectionTarget,
		"upsert",
	)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"mssql",
		projectionTarget,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	actualTables := projection.PriorTables()
	actualTables = append(actualTables, schema.Table{
		Schema: "tenant",
		Name:   "operator_audit",
		Columns: []schema.Column{{
			Name: "id", Type: "bigint",
		}},
	})
	catalog, err := NewTargetSchemaEvolutionCatalog(actualTables, nil)
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		projection,
		targetSchemaEvolutionFixtureCreatePlanner{},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildTargetSchemaEvolutionPlan(request, catalog)
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range plan.PendingOperations() {
		for _, object := range operation.Objects() {
			if object.Schema != "tenant" {
				t.Fatalf(
					"executable object retained source namespace: %#v",
					object,
				)
			}
		}
		for _, statement := range operation.Statements() {
			if strings.Contains(statement, "source_schema") {
				t.Fatalf(
					"executable statement retained source namespace: %s",
					statement,
				)
			}
		}
	}
}

func TestTargetSchemaEvolutionRejectsAuthoritySubstitutionAndIncompleteEvidence(
	t *testing.T,
) {
	t.Parallel()

	t.Run("projection endpoint substitution", func(t *testing.T) {
		t.Parallel()
		request, catalog := targetSchemaEvolutionFixture(t)
		request.priorTables = catalog.Tables()
		_, err := BuildTargetSchemaEvolutionPlan(request, catalog)
		if err == nil ||
			!strings.Contains(err.Error(), "authority changed after construction") {
			t.Fatalf("substituted-prior error = %v", err)
		}
	})

	t.Run("missing audit evidence", func(t *testing.T) {
		t.Parallel()
		request, catalog := targetSchemaEvolutionFixture(t)
		request.decisions[0].contract.Reason = ""
		request.authorityDigest = targetSchemaEvolutionTestAuthorityDigest(
			t,
			request,
		)
		_, err := BuildTargetSchemaEvolutionPlan(request, catalog)
		if err == nil ||
			!strings.Contains(err.Error(), "canonical reason") {
			t.Fatalf("missing-reason error = %v", err)
		}
	})
}

func TestTargetSchemaEvolutionFreezesGlobalNameReservations(
	t *testing.T,
) {
	t.Parallel()

	request, baseCatalog := targetSchemaEvolutionFixture(t)
	reservation := TargetSchemaEvolutionNameReservation{
		Scope:     "relation",
		Namespace: "tenant",
		Name:      "events_account_idx",
	}
	catalog := targetSchemaEvolutionTestCatalog(
		t,
		baseCatalog.tables,
		[]TargetSchemaEvolutionNameReservation{reservation},
	)
	targetSchemaEvolutionBindAuthorityReservations(
		t,
		&request,
		catalog.reservations,
	)
	guard := &targetSchemaEvolutionGuardCreatePlanner{
		rejectReservation: "events_account_idx",
	}
	request.createPlanner = guard
	_, err := BuildTargetSchemaEvolutionPlan(request, catalog)
	if err == nil ||
		!strings.Contains(err.Error(), "reserved target object name") {
		t.Fatalf("reserved-name error = %v", err)
	}
	if guard.calls != 1 || !guard.sawCompleteDesired ||
		!guard.sawActualCatalog {
		t.Fatalf(
			"planner evidence calls=%d complete=%t actual=%t",
			guard.calls,
			guard.sawCompleteDesired,
			guard.sawActualCatalog,
		)
	}

	guard = &targetSchemaEvolutionGuardCreatePlanner{}
	request.createPlanner = guard
	catalog = targetSchemaEvolutionTestCatalog(
		t,
		baseCatalog.tables,
		[]TargetSchemaEvolutionNameReservation{{
			Scope:     "relation",
			Namespace: "tenant",
			Name:      "unrelated_sequence",
		}},
	)
	targetSchemaEvolutionBindAuthorityReservations(
		t,
		&request,
		catalog.reservations,
	)
	plan, err := BuildTargetSchemaEvolutionPlan(request, catalog)
	if err != nil {
		t.Fatal(err)
	}
	session := &targetSchemaEvolutionFakeSession{
		catalogs: []targetSchemaEvolutionCatalogRead{{
			tables: plan.states[0],
			reservations: []TargetSchemaEvolutionNameReservation{{
				Scope:     "relation",
				Namespace: "tenant",
				Name:      "concurrent_sequence",
			}},
		}},
	}
	err = ApplyTargetSchemaEvolution(context.Background(), plan, session)
	if err == nil ||
		!strings.Contains(err.Error(), "name reservations changed") {
		t.Fatalf("changed-reservation error = %v", err)
	}
	if !reflect.DeepEqual(session.calls, []string{"read"}) {
		t.Fatalf("changed reservations reached mutation: %#v", session.calls)
	}
}

func targetSchemaEvolutionBindAuthorityReservations(
	t *testing.T,
	request *TargetSchemaEvolutionRequest,
	reservations []TargetSchemaEvolutionNameReservation,
) {
	t.Helper()
	request.targetAuthorityReservations =
		canonicalTargetSchemaEvolutionReservations(reservations)
	snapshot, err := schema.NewSchemaSnapshot(request.priorTables)
	if err != nil {
		t.Fatal(err)
	}
	request.targetAuthorityCatalog, err = stage4TargetShapeCatalogDigest(
		snapshot,
		request.targetAuthorityReservations,
	)
	if err != nil {
		t.Fatal(err)
	}
	request.authorityDigest = targetSchemaEvolutionTestAuthorityDigest(
		t,
		*request,
	)
}

func TestTargetSchemaEvolutionRejectsUnsafeOrNondeterministicCreateBoundaries(
	t *testing.T,
) {
	t.Parallel()

	table := schema.Table{
		Schema: "tenant",
		Name:   "items",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "integer",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	sqlServerStatement, err := schema.CreateTableDDL(
		schema.SQLServer,
		table,
	)
	if err != nil {
		t.Fatal(err)
	}
	for name, statement := range map[string]schema.DDLStatement{
		"zero":          {},
		"cross dialect": sqlServerStatement,
	} {
		if _, err := NewCompleteTargetSchemaCreateBundle(
			schema.Postgres,
			[]schema.Table{table},
			[]TargetSchemaCreateStep{{
				Statement:    statement,
				ResultTables: []schema.Table{table},
			}},
		); err == nil {
			t.Fatalf("%s opaque create boundary was admitted", name)
		}
	}

	t.Run("planner mutates evidence", func(t *testing.T) {
		t.Parallel()
		request, catalog := targetSchemaEvolutionFixture(t)
		request.createPlanner = &targetSchemaEvolutionGuardCreatePlanner{
			mutateActual: true,
		}
		_, err := BuildTargetSchemaEvolutionPlan(request, catalog)
		if err == nil ||
			!strings.Contains(err.Error(), "mutated immutable planning evidence") {
			t.Fatalf("mutating-planner error = %v", err)
		}
	})

	t.Run("planner changes repeated result", func(t *testing.T) {
		t.Parallel()
		request, catalog := targetSchemaEvolutionFixture(t)
		request.createPlanner = &targetSchemaEvolutionGuardCreatePlanner{
			nondeterministic: true,
		}
		_, err := BuildTargetSchemaEvolutionPlan(request, catalog)
		if err == nil ||
			!strings.Contains(err.Error(), "nondeterministic statement boundaries") {
			t.Fatalf("nondeterministic-planner error = %v", err)
		}
	})
}

func TestTargetSchemaEvolutionLifecycleFailsClosedAtEveryBoundary(
	t *testing.T,
) {
	t.Parallel()

	t.Run("nil and cancelled preflight context", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		reader := &targetSchemaEvolutionFakeReader{
			catalogs: []targetSchemaEvolutionCatalogRead{{
				tables: priorCatalog.tables,
			}},
		}
		if _, err := PreflightTargetSchemaEvolution(
			nil,
			request,
			reader,
		); err == nil ||
			!strings.Contains(err.Error(), "context is required") {
			t.Fatalf("nil-context preflight error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := PreflightTargetSchemaEvolution(
			ctx,
			request,
			reader,
		); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled preflight error = %v", err)
		}
		if reader.calls != 0 {
			t.Fatalf(
				"invalid preflight contexts performed %d catalog reads",
				reader.calls,
			)
		}
	})

	t.Run("typed nil interfaces", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		var reader *targetSchemaEvolutionFakeReader
		if _, err := PreflightTargetSchemaEvolution(
			context.Background(),
			request,
			reader,
		); err == nil ||
			!strings.Contains(err.Error(), "catalog reader is required") {
			t.Fatalf("typed-nil reader error = %v", err)
		}

		plan, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		)
		if err != nil {
			t.Fatal(err)
		}
		var session *targetSchemaEvolutionFakeSession
		if err := ApplyTargetSchemaEvolution(
			context.Background(),
			plan,
			session,
		); err == nil ||
			!strings.Contains(err.Error(), "same-session") {
			t.Fatalf("typed-nil session error = %v", err)
		}

		request, priorCatalog = targetSchemaEvolutionFixture(t)
		request.createPlanner =
			(*targetSchemaEvolutionNilCreatePlanner)(nil)
		if _, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		); err == nil ||
			!strings.Contains(
				err.Error(),
				"complete target create planner is required",
			) {
			t.Fatalf("typed-nil create planner error = %v", err)
		}
	})

	t.Run("fully applied plan is read-only no-op", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		initial, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		)
		if err != nil {
			t.Fatal(err)
		}
		finalCatalog := initial.states[len(initial.states)-1]
		complete, err := BuildTargetSchemaEvolutionPlan(
			request,
			targetSchemaEvolutionTestCatalog(
				t,
				finalCatalog,
				initial.reservations,
			),
		)
		if err != nil {
			t.Fatal(err)
		}
		session := &targetSchemaEvolutionFakeSession{
			catalogs: []targetSchemaEvolutionCatalogRead{{
				tables: finalCatalog,
			}},
		}
		if err := ApplyTargetSchemaEvolution(
			context.Background(),
			complete,
			session,
		); err != nil {
			t.Fatal(err)
		}
		if got, want := session.calls, []string{"read"}; !reflect.DeepEqual(
			got,
			want,
		) {
			t.Fatalf("fully applied calls = %#v, want %#v", got, want)
		}
	})

	t.Run("catalog moves after preflight", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		plan, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		)
		if err != nil {
			t.Fatal(err)
		}
		session := &targetSchemaEvolutionFakeSession{
			catalogs: []targetSchemaEvolutionCatalogRead{{
				tables: plan.states[1],
			}},
		}
		err = ApplyTargetSchemaEvolution(
			context.Background(),
			plan,
			session,
		)
		if err == nil ||
			!strings.Contains(err.Error(), "moved from verified prefix") {
			t.Fatalf("moved-catalog error = %v", err)
		}
		if got, want := session.calls, []string{"read"}; !reflect.DeepEqual(
			got,
			want,
		) {
			t.Fatalf("moved-catalog calls = %#v, want %#v", got, want)
		}
	})

	t.Run("successful executor with partial catalog fails verification", func(t *testing.T) {
		t.Parallel()
		request, priorCatalog := targetSchemaEvolutionFixture(t)
		plan, err := BuildTargetSchemaEvolutionPlan(
			request,
			priorCatalog,
		)
		if err != nil {
			t.Fatal(err)
		}
		session := &targetSchemaEvolutionFakeSession{
			catalogs: []targetSchemaEvolutionCatalogRead{
				{tables: plan.states[0]},
				{tables: plan.states[2]},
			},
		}
		err = ApplyTargetSchemaEvolution(
			context.Background(),
			plan,
			session,
		)
		if err == nil ||
			!strings.Contains(
				err.Error(),
				"execution returned success after only prefix 2",
			) {
			t.Fatalf("partial-success error = %v", err)
		}
		assertTargetSchemaEvolutionErrorKind(
			t,
			err,
			TargetSchemaEvolutionVerifyFailed,
		)
	})

	t.Run("post-execution verification survives cancellation", func(t *testing.T) {
		t.Parallel()
		request, catalog := targetSchemaEvolutionFixture(t)
		plan, err := BuildTargetSchemaEvolutionPlan(request, catalog)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		session := &targetSchemaEvolutionFakeSession{
			catalogs: []targetSchemaEvolutionCatalogRead{
				{tables: plan.states[0]},
				{tables: plan.states[len(plan.states)-1]},
			},
			executeHook: cancel,
		}
		err = ApplyTargetSchemaEvolution(ctx, plan, session)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled apply error = %v", err)
		}
		if got, want := session.calls, []string{
			"read",
			"execute",
			"read",
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf(
				"cancelled verification calls = %#v, want %#v",
				got,
				want,
			)
		}
	})
}

type targetSchemaEvolutionNilCreatePlanner struct{}

func (*targetSchemaEvolutionNilCreatePlanner) PlanCompleteTargetSchemaCreates(
	schema.Dialect,
	[]schema.Table,
	[]schema.Table,
	TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	panic("typed nil create planner must not be called")
}

type targetSchemaEvolutionFixtureCreatePlanner struct{}

func (targetSchemaEvolutionFixtureCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	if target != schema.Postgres || len(tables) != 1 {
		return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
			"unexpected fixture create request",
		)
	}
	return (postgresTargetSchemaEvolutionCreatePlanner{}).
		PlanCompleteTargetSchemaCreates(
			target,
			tables,
			completeDesired,
			actual,
		)
}

type targetSchemaEvolutionGuardCreatePlanner struct {
	calls              int
	rejectReservation  string
	mutateActual       bool
	nondeterministic   bool
	sawCompleteDesired bool
	sawActualCatalog   bool
}

func (planner *targetSchemaEvolutionGuardCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	completeDesired []schema.Table,
	actual TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	planner.calls++
	planner.sawCompleteDesired =
		len(completeDesired) == 2 &&
			findTargetSchemaEvolutionTable(
				completeDesired,
				targetSchemaEvolutionTableKey{
					schema: "tenant",
					table:  "accounts",
				},
			) >= 0 &&
			findTargetSchemaEvolutionTable(
				completeDesired,
				targetSchemaEvolutionTableKey{
					schema: "tenant",
					table:  "events",
				},
			) >= 0
	planner.sawActualCatalog = len(actual.tables) == 2
	for _, reservation := range actual.reservations {
		if reservation.Name == planner.rejectReservation {
			return CompleteTargetSchemaCreateBundle{}, fmt.Errorf(
				"reserved target object name %s collides with create plan",
				reservation.Name,
			)
		}
	}
	if planner.mutateActual && len(actual.tables) != 0 {
		actual.tables[0].Name = "mutated_by_planner"
	}
	bundle, err := (targetSchemaEvolutionFixtureCreatePlanner{}).
		PlanCompleteTargetSchemaCreates(
			target,
			tables,
			completeDesired,
			actual,
		)
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}
	if planner.nondeterministic && planner.calls%2 == 0 {
		bundle.steps[0].statement += " "
	}
	return bundle, nil
}

type targetSchemaEvolutionIncompleteCreatePlanner struct{}

func (targetSchemaEvolutionIncompleteCreatePlanner) PlanCompleteTargetSchemaCreates(
	target schema.Dialect,
	tables []schema.Table,
	_ []schema.Table,
	_ TargetSchemaEvolutionCatalog,
) (CompleteTargetSchemaCreateBundle, error) {
	incomplete := cloneTargetSchemaEvolutionTables(tables)
	incomplete[0].Indexes = nil
	incomplete[0].Checks = nil
	incomplete[0].ForeignKeys = nil
	statement, err := schema.CreateTableDDL(target, incomplete[0])
	if err != nil {
		return CompleteTargetSchemaCreateBundle{}, err
	}
	return NewCompleteTargetSchemaCreateBundle(
		target,
		incomplete,
		[]TargetSchemaCreateStep{{
			Statement:    statement,
			ResultTables: incomplete,
		}},
	)
}

type targetSchemaEvolutionCatalogRead struct {
	tables       []schema.Table
	reservations []TargetSchemaEvolutionNameReservation
	err          error
}

type targetSchemaEvolutionFakeReader struct {
	catalogs []targetSchemaEvolutionCatalogRead
	calls    int
}

func (reader *targetSchemaEvolutionFakeReader) ReadTargetSchemaEvolutionCatalog(
	context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	if reader.calls >= len(reader.catalogs) {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"unexpected catalog read %d",
			reader.calls,
		)
	}
	result := reader.catalogs[reader.calls]
	reader.calls++
	catalog, err := NewTargetSchemaEvolutionCatalog(
		result.tables,
		result.reservations,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	return catalog, result.err
}

type targetSchemaEvolutionFakeSession struct {
	catalogs    []targetSchemaEvolutionCatalogRead
	catalogPos  int
	executeErr  error
	executeHook func()
	executed    []TargetSchemaEvolutionOperation
	calls       []string
}

func (session *targetSchemaEvolutionFakeSession) ReadTargetSchemaEvolutionCatalog(
	context.Context,
) (TargetSchemaEvolutionCatalog, error) {
	session.calls = append(session.calls, "read")
	if session.catalogPos >= len(session.catalogs) {
		return TargetSchemaEvolutionCatalog{}, fmt.Errorf(
			"unexpected session catalog read %d",
			session.catalogPos,
		)
	}
	result := session.catalogs[session.catalogPos]
	session.catalogPos++
	catalog, err := NewTargetSchemaEvolutionCatalog(
		result.tables,
		result.reservations,
	)
	if err != nil {
		return TargetSchemaEvolutionCatalog{}, err
	}
	return catalog, result.err
}

func (session *targetSchemaEvolutionFakeSession) ExecuteTargetSchemaEvolution(
	_ context.Context,
	operations []TargetSchemaEvolutionOperation,
) error {
	session.calls = append(session.calls, "execute")
	session.executed = cloneTargetSchemaEvolutionOperations(operations)
	if session.executeHook != nil {
		session.executeHook()
	}
	return session.executeErr
}

func targetSchemaEvolutionFixture(
	t *testing.T,
) (TargetSchemaEvolutionRequest, TargetSchemaEvolutionCatalog) {
	t.Helper()
	checkExpression, err := schema.ParseSQLiteCheckExpression(`"id" > 0`)
	if err != nil {
		t.Fatal(err)
	}
	priorAccounts := schema.Table{
		Schema: "tenant",
		Name:   "accounts",
		Columns: []schema.Column{
			{
				Name: "id", Type: "integer",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{Name: "score", Type: "integer", Nullable: true},
			{
				Name: "note", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
		},
	}
	currentAccounts := cloneStage4RichTable(priorAccounts)
	currentAccounts.Columns[1].Type = "bigint"
	currentAccounts.Columns[2].Nullable = true
	currentAccounts.Columns = append(
		currentAccounts.Columns,
		schema.Column{Name: "label", Type: "text", Nullable: true},
	)
	events := schema.Table{
		Schema: "tenant",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name: "id", Type: "integer",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{Name: "account_id", Type: "integer"},
		},
		Indexes: []schema.Index{{
			Name: "events_account_idx",
			Columns: []schema.IndexColumn{{
				Name: "account_id",
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "events_id_positive",
			Expression: checkExpression,
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_account_fk",
			Columns:           []string{"account_id"},
			ReferencedSchema:  "tenant",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "CASCADE",
			Match:             "SIMPLE",
		}},
	}
	operatorAudit := schema.Table{
		Schema: "tenant",
		Name:   "operator_audit",
		Columns: []schema.Column{{
			Name: "id", Type: "bigint",
		}},
	}
	prior := []schema.Table{priorAccounts}
	current := []schema.Table{events, currentAccounts}
	priorSnapshot, err := schema.NewSchemaSnapshot(prior)
	if err != nil {
		t.Fatal(err)
	}
	currentSnapshot, err := schema.NewSchemaSnapshot(current)
	if err != nil {
		t.Fatal(err)
	}
	contractPlan, err := BuildSchemaContractPlan(
		priorSnapshot,
		currentSnapshot,
		SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
			TargetMode: "upsert",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	projection := targetSchemaEvolutionFixtureProjection(
		t,
		prior,
		current,
		contractPlan.Decisions,
	)
	request, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		projection,
		targetSchemaEvolutionFixtureCreatePlanner{},
	)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewTargetSchemaEvolutionCatalog(
		[]schema.Table{operatorAudit, priorAccounts},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return request, catalog
}

func targetSchemaEvolutionFixtureProjection(
	t *testing.T,
	prior []schema.Table,
	current []schema.Table,
	decisions []SchemaContractDecision,
) Stage4TargetSchemaEvolutionProjection {
	t.Helper()
	priorDigest, err := digestTargetSchemaEvolutionCatalog(prior)
	if err != nil {
		t.Fatal(err)
	}
	currentDigest, err := digestTargetSchemaEvolutionCatalog(current)
	if err != nil {
		t.Fatal(err)
	}
	priorSnapshot, err := schema.NewSchemaSnapshot(prior)
	if err != nil {
		t.Fatal(err)
	}
	catalogDigest, err := stage4TargetShapeCatalogDigest(
		priorSnapshot,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	byObject := make(map[Stage4SchemaObjectIdentity]struct{})
	for _, tables := range [][]schema.Table{prior, current} {
		for _, table := range tables {
			byObject[Stage4SchemaObjectIdentity{
				Schema: table.Schema,
				Table:  table.Name,
			}] = struct{}{}
			for _, column := range table.Columns {
				byObject[Stage4SchemaObjectIdentity{
					Schema: table.Schema,
					Table:  table.Name,
					Column: column.Name,
				}] = struct{}{}
			}
		}
	}
	mappings := make(
		[]Stage4TargetSchemaObjectMapping,
		0,
		len(byObject),
	)
	for identity := range byObject {
		mappings = append(mappings, Stage4TargetSchemaObjectMapping{
			Source: identity,
			Target: identity,
		})
	}
	sort.Slice(mappings, func(left, right int) bool {
		return stage4TargetSchemaObjectIdentityKey(mappings[left].Source) <
			stage4TargetSchemaObjectIdentityKey(mappings[right].Source)
	})
	return Stage4TargetSchemaEvolutionProjection{
		sourceEngine:                 "mssql",
		targetEngine:                 "postgres",
		targetMode:                   "upsert",
		sourcePriorDigest:            priorDigest,
		sourceCurrentDigest:          currentDigest,
		targetAuthorityTopologyHash:  "target-evolution-fixture-topology",
		targetAuthorityPriorDigest:   priorDigest,
		targetAuthorityCatalogDigest: catalogDigest,
		priorDigest:                  priorDigest,
		currentDigest:                currentDigest,
		decisions:                    cloneStage4TargetSchemaProjectionDecisions(decisions),
		priorTables:                  cloneTargetSchemaEvolutionTables(prior),
		currentTables:                cloneTargetSchemaEvolutionTables(current),
		objectMappings:               mappings,
	}
}

func reverseTargetSchemaEvolutionTables(
	tables []schema.Table,
) []schema.Table {
	result := cloneTargetSchemaEvolutionTables(tables)
	for left, right := 0, len(result)-1; left < right; left, right =
		left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}

func reverseTargetSchemaEvolutionReservations(
	reservations []TargetSchemaEvolutionNameReservation,
) []TargetSchemaEvolutionNameReservation {
	result := cloneTargetSchemaEvolutionReservations(reservations)
	for left, right := 0, len(result)-1; left < right; left, right =
		left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}

func targetSchemaEvolutionTestCatalog(
	t *testing.T,
	tables []schema.Table,
	reservations []TargetSchemaEvolutionNameReservation,
) TargetSchemaEvolutionCatalog {
	t.Helper()
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, reservations)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func targetSchemaEvolutionTestAuthorityDigest(
	t *testing.T,
	request TargetSchemaEvolutionRequest,
) string {
	t.Helper()
	digest, err := digestTargetSchemaEvolutionAuthority(request)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}

func refreshTargetSchemaEvolutionTestAuthority(
	t *testing.T,
	request *TargetSchemaEvolutionRequest,
) {
	t.Helper()
	prior, err := digestTargetSchemaEvolutionCatalog(request.priorTables)
	if err != nil {
		t.Fatal(err)
	}
	current, err := digestTargetSchemaEvolutionCatalog(request.currentTables)
	if err != nil {
		t.Fatal(err)
	}
	request.projectionPrior = prior
	request.projectionNext = current
	request.authorityDigest = targetSchemaEvolutionTestAuthorityDigest(
		t,
		*request,
	)
}

func reverseTargetSchemaEvolutionDecisions(
	decisions []SchemaContractDecision,
) []SchemaContractDecision {
	result := append([]SchemaContractDecision(nil), decisions...)
	for left, right := 0, len(result)-1; left < right; left, right =
		left+1, right-1 {
		result[left], result[right] = result[right], result[left]
	}
	return result
}

func assertTargetSchemaEvolutionErrorKind(
	t *testing.T,
	err error,
	want TargetSchemaEvolutionErrorKind,
) {
	t.Helper()
	var classified *TargetSchemaEvolutionError
	if !errors.As(err, &classified) {
		t.Fatalf("error %T is not TargetSchemaEvolutionError: %v", err, err)
	}
	if classified.Kind != want {
		t.Fatalf("error kind = %q, want %q: %v", classified.Kind, want, err)
	}
}
