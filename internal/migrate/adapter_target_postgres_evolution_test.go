package migrate

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresTargetEvolutionCreatePlannerOrdersCompleteObjects(
	t *testing.T,
) {
	t.Parallel()

	check, err := schema.ParseSQLiteCheckExpression(`value <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	parent := schema.Table{
		Schema: "tenant",
		Name:   "parents",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	child := schema.Table{
		Schema: "tenant",
		Name:   "children",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "parent_id", Type: "bigint"},
			{Name: "value", Type: "text", Nullable: true},
		},
		Indexes: []schema.Index{{
			Name: "children_value_idx",
			Columns: []schema.IndexColumn{{
				Name: "value",
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "children_value_check",
			Expression: check,
		}},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "children_parent_fk",
			Columns:           []string{"parent_id"},
			ReferencedSchema:  "tenant",
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
		}},
	}

	planner := postgresTargetSchemaEvolutionCreatePlanner{}
	bundle, err := planner.PlanCompleteTargetSchemaCreates(
		schema.Postgres,
		[]schema.Table{child},
		[]schema.Table{child, parent},
		postgresTargetEvolutionTestCatalog(nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.steps) != 4 {
		t.Fatalf("create step count = %d, want 4", len(bundle.steps))
	}
	prefixes := []string{
		`CREATE TABLE "tenant"."children"`,
		`CREATE INDEX "children_value_idx"`,
		`ALTER TABLE "tenant"."children" ADD CONSTRAINT "children_value_check" CHECK`,
		`ALTER TABLE "tenant"."children" ADD CONSTRAINT "children_parent_fk" FOREIGN KEY`,
	}
	for index, prefix := range prefixes {
		if !strings.HasPrefix(bundle.steps[index].statement, prefix) {
			t.Fatalf(
				"step %d statement = %q, want prefix %q",
				index,
				bundle.steps[index].statement,
				prefix,
			)
		}
	}
	for index, expected := range []struct {
		indexes    int
		checks     int
		foreignKey int
	}{
		{},
		{indexes: 1},
		{indexes: 1, checks: 1},
		{indexes: 1, checks: 1, foreignKey: 1},
	} {
		if len(bundle.steps[index].tables) != 1 {
			t.Fatalf(
				"step %d table count = %d, want 1",
				index,
				len(bundle.steps[index].tables),
			)
		}
		table := bundle.steps[index].tables[0]
		if len(table.Indexes) != expected.indexes ||
			len(table.Checks) != expected.checks ||
			len(table.ForeignKeys) != expected.foreignKey {
			t.Fatalf(
				"step %d shape = indexes:%d checks:%d foreign_keys:%d",
				index,
				len(table.Indexes),
				len(table.Checks),
				len(table.ForeignKeys),
			)
		}
	}
}

func TestPostgresTargetEvolutionCreatePlannerRejectsGeneratedNameDrift(
	t *testing.T,
) {
	t.Parallel()

	check, parseErr := schema.ParseSQLiteCheckExpression(`id > 0`)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	table := postgresTargetEvolutionTestTable("tenant", "events")
	table.Checks = []schema.CheckConstraint{{
		Expression: check,
	}}
	_, err := (postgresTargetSchemaEvolutionCreatePlanner{}).
		PlanCompleteTargetSchemaCreates(
			schema.Postgres,
			[]schema.Table{table},
			[]schema.Table{table},
			postgresTargetEvolutionTestCatalog(nil),
		)
	if err == nil ||
		!strings.Contains(err.Error(), "explicit round-trippable object names") {
		t.Fatalf("generated-name error = %v", err)
	}
}

func TestPostgresTargetEvolutionCreatePlannerRejectsActualRelationCollision(
	t *testing.T,
) {
	t.Parallel()

	created := postgresTargetEvolutionTestTable("tenant", "events")
	audit := postgresTargetEvolutionTestTable("tenant", "audit")
	audit.Indexes = []schema.Index{{
		Name:    "events",
		Columns: []schema.IndexColumn{{Name: "id"}},
	}}
	actual := postgresTargetEvolutionTestCatalog([]schema.Table{audit})
	_, err := (postgresTargetSchemaEvolutionCreatePlanner{}).
		PlanCompleteTargetSchemaCreates(
			schema.Postgres,
			[]schema.Table{created},
			[]schema.Table{created},
			actual,
		)
	if err == nil ||
		!strings.Contains(err.Error(), "collides with an existing relation") {
		t.Fatalf("relation collision error = %v", err)
	}
}

func TestPostgresTargetPlanTablesMaterializesExactObjectNames(t *testing.T) {
	t.Parallel()

	check, err := schema.ParseSQLiteCheckExpression(`code <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	parent := postgresTargetEvolutionTestTable("source", "parents")
	child := schema.Table{
		Schema: "source",
		Name:   "children",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "parent_id", Type: "bigint"},
			{Name: "code", Type: "text"},
		},
		Indexes: []schema.Index{{
			Unique:  true,
			Inline:  true,
			Columns: []schema.IndexColumn{{Name: "code"}},
		}},
		Checks: []schema.CheckConstraint{{Expression: check}},
		ForeignKeys: []schema.ForeignKey{{
			Columns:           []string{"parent_id"},
			ReferencedSchema:  "source",
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
		}},
	}
	adapter := &postgresTargetAdapter{namespace: "tenant"}
	planned, err := adapter.PlanTables(
		"postgres",
		[]schema.Table{child, parent},
		"drop_recreate",
	)
	if err != nil {
		t.Fatal(err)
	}
	var plannedChild schema.Table
	for _, table := range planned {
		if table.Name == "children" {
			plannedChild = table
		}
	}
	if plannedChild.Name == "" ||
		plannedChild.Indexes[0].Name == "" ||
		plannedChild.Checks[0].Name == "" ||
		plannedChild.ForeignKeys[0].Name == "" {
		t.Fatalf("target-ready child has unmaterialized names: %#v", plannedChild)
	}
	objects, err := schema.PlanPostgresDropRecreateObjects(
		planned,
		schema.PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[schema.PostgresObjectKind]string)
	for _, object := range objects {
		if object.Table() == "children" {
			names[object.Kind()] = object.Name()
		}
	}
	if names[schema.PostgresIndexObject] != plannedChild.Indexes[0].Name ||
		names[schema.PostgresCheckObject] != plannedChild.Checks[0].Name ||
		names[schema.PostgresForeignKeyObject] !=
			plannedChild.ForeignKeys[0].Name {
		t.Fatalf("DDL names %#v do not match target-ready metadata", names)
	}
}

func TestPostgresTargetEvolutionUnitOfWorkUsesOneExactSession(
	t *testing.T,
) {
	t.Parallel()

	plan, before, after := postgresTargetEvolutionTestPlan()
	var calls []string
	catalogs := [][]schema.Table{before, after, after}
	readPosition := 0
	session := &postgresTargetEvolutionMutationSession{
		executor: postgresTargetEvolutionRecordingExecutor{
			calls: &calls,
		},
		namespace: "tenant",
		plan:      plan,
		readCatalog: func(
			context.Context,
		) (
			TargetSchemaEvolutionCatalog,
			[]postgresTargetEvolutionRelation,
			error,
		) {
			calls = append(calls, "read")
			if readPosition >= len(catalogs) {
				return TargetSchemaEvolutionCatalog{}, nil,
					errors.New("unexpected catalog read")
			}
			result := catalogs[readPosition]
			readPosition++
			return postgresTargetEvolutionTestCatalog(result), nil, nil
		},
	}
	err := runPostgresTargetEvolutionUnitOfWork(
		context.Background(),
		plan,
		session,
		func() error {
			calls = append(calls, "commit")
			return nil
		},
		func() error {
			calls = append(calls, "rollback")
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"read",
		`exec:ALTER TABLE "tenant"."events" ADD COLUMN "note" text;`,
		"read",
		"read",
		"commit",
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls = %#v, want %#v", calls, want)
	}
}

func TestPostgresTargetEvolutionUnitOfWorkRollsBackMismatch(
	t *testing.T,
) {
	t.Parallel()

	plan, before, _ := postgresTargetEvolutionTestPlan()
	var calls []string
	session := &postgresTargetEvolutionMutationSession{
		executor: postgresTargetEvolutionRecordingExecutor{
			calls: &calls,
		},
		namespace: "tenant",
		plan:      plan,
		readCatalog: func(
			context.Context,
		) (
			TargetSchemaEvolutionCatalog,
			[]postgresTargetEvolutionRelation,
			error,
		) {
			calls = append(calls, "read")
			return postgresTargetEvolutionTestCatalog(before), nil, nil
		},
	}
	err := runPostgresTargetEvolutionUnitOfWork(
		context.Background(),
		plan,
		session,
		func() error {
			calls = append(calls, "commit")
			return nil
		},
		func() error {
			calls = append(calls, "rollback")
			return nil
		},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "exact declared cumulative state") {
		t.Fatalf("mismatch error = %v", err)
	}
	if calls[len(calls)-1] != "rollback" ||
		contains(calls, "commit") {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestPostgresTargetEvolutionUnitOfWorkReportsRollbackAndUnknownCommit(
	t *testing.T,
) {
	t.Parallel()

	t.Run("rollback failure", func(t *testing.T) {
		plan, before, _ := postgresTargetEvolutionTestPlan()
		session := &postgresTargetEvolutionMutationSession{
			executor: postgresTargetEvolutionRecordingExecutor{
				err: errors.New("DDL failed"),
			},
			plan: plan,
			readCatalog: func(
				context.Context,
			) (
				TargetSchemaEvolutionCatalog,
				[]postgresTargetEvolutionRelation,
				error,
			) {
				return postgresTargetEvolutionTestCatalog(before), nil, nil
			},
		}
		err := runPostgresTargetEvolutionUnitOfWork(
			context.Background(),
			plan,
			session,
			func() error { return nil },
			func() error { return errors.New("rollback lost") },
		)
		if err == nil ||
			!strings.Contains(err.Error(), "rollback also failed") ||
			!strings.Contains(err.Error(), "rollback lost") {
			t.Fatalf("rollback error = %v", err)
		}
	})

	t.Run("commit outcome unknown", func(t *testing.T) {
		plan, before, after := postgresTargetEvolutionTestPlan()
		catalogs := [][]schema.Table{before, after, after}
		position := 0
		session := &postgresTargetEvolutionMutationSession{
			executor: postgresTargetEvolutionRecordingExecutor{},
			plan:     plan,
			readCatalog: func(
				context.Context,
			) (
				TargetSchemaEvolutionCatalog,
				[]postgresTargetEvolutionRelation,
				error,
			) {
				result := catalogs[position]
				position++
				return postgresTargetEvolutionTestCatalog(result), nil, nil
			},
		}
		err := runPostgresTargetEvolutionUnitOfWork(
			context.Background(),
			plan,
			session,
			func() error { return errors.New("network EOF") },
			func() error { return nil },
		)
		if err == nil ||
			!strings.Contains(err.Error(), "commit outcome is unknown") ||
			!strings.Contains(err.Error(), "rerun the same migration or resume") {
			t.Fatalf("commit error = %v", err)
		}
	})
}

func TestPostgresTargetEvolutionIndependentPostCommitVerification(
	t *testing.T,
) {
	t.Parallel()

	plan, _, after := postgresTargetEvolutionTestPlan()
	if err := verifyPostgresTargetEvolutionCommittedCatalog(
		plan,
		postgresTargetEvolutionTestCatalog(after),
		nil,
	); err != nil {
		t.Fatal(err)
	}
	drift := cloneTargetSchemaEvolutionTables(after)
	drift = append(
		drift,
		postgresTargetEvolutionTestTable("tenant", "external_create"),
	)
	err := verifyPostgresTargetEvolutionCommittedCatalog(
		plan,
		postgresTargetEvolutionTestCatalog(drift),
		nil,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "independent snapshot found concurrent") {
		t.Fatalf("post-commit drift error = %v", err)
	}
	err = verifyPostgresTargetEvolutionCommittedCatalog(
		plan,
		TargetSchemaEvolutionCatalog{},
		errors.New("catalog connection lost"),
	)
	if err == nil ||
		!strings.Contains(err.Error(), "independent complete catalog snapshot") {
		t.Fatalf("post-commit read error = %v", err)
	}
}

func TestPostgresTargetEvolutionRejectsStatementsOutsidePlan(
	t *testing.T,
) {
	t.Parallel()

	plan, _, _ := postgresTargetEvolutionTestPlan()
	var calls []string
	session := &postgresTargetEvolutionMutationSession{
		executor: postgresTargetEvolutionRecordingExecutor{
			calls: &calls,
		},
		plan: plan,
	}
	foreign := plan.PendingOperations()
	foreign[0].statements[0] = "DROP SCHEMA tenant CASCADE"
	err := session.ExecuteTargetSchemaEvolution(
		context.Background(),
		foreign,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "outside the immutable pending suffix") {
		t.Fatalf("foreign statement error = %v", err)
	}
	if len(calls) != 0 {
		t.Fatalf("unexpected executor calls: %#v", calls)
	}
}

func TestValidatePostgresTargetEvolutionRelationsFailsClosed(
	t *testing.T,
) {
	t.Parallel()

	table := postgresTargetEvolutionTestRelation(
		1,
		"tenant",
		"events",
		"r",
	)
	table.canAlter = true
	index := postgresTargetEvolutionTestRelation(
		2,
		"tenant",
		"events_pkey",
		"i",
	)
	index.indexOwnerNamespace = "tenant"
	index.indexOwnerTable = "events"
	sequence := postgresTargetEvolutionTestRelation(
		3,
		"tenant",
		"events_id_seq",
		"S",
	)
	sequence.sequenceOwnerNamespace = "tenant"
	sequence.sequenceOwnerTable = "events"
	names, err := validatePostgresTargetEvolutionRelations(
		"tenant",
		[]postgresTargetEvolutionRelation{table, index, sequence},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(names, []string{"events"}) {
		t.Fatalf("table names = %#v", names)
	}

	tests := []struct {
		name      string
		relations []postgresTargetEvolutionRelation
		want      string
	}{
		{
			name: "hidden view",
			relations: []postgresTargetEvolutionRelation{
				table,
				postgresTargetEvolutionTestRelation(
					4,
					"tenant",
					"events_view",
					"v",
				),
			},
			want: "unsupported view relation",
		},
		{
			name: "unowned table",
			relations: []postgresTargetEvolutionRelation{
				postgresTargetEvolutionTestRelation(
					1,
					"tenant",
					"events",
					"r",
				),
			},
			want: "ALTER privilege cannot be proved",
		},
		{
			name:      "standalone sequence",
			relations: []postgresTargetEvolutionRelation{sequence},
			want:      "not owned by an enumerated ordinary table",
		},
		{
			name:      "orphan index",
			relations: []postgresTargetEvolutionRelation{index},
			want:      "not owned by an enumerated ordinary table",
		},
		{
			name:      "duplicate catalog name",
			relations: []postgresTargetEvolutionRelation{table, table},
			want:      "duplicate relation name",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := validatePostgresTargetEvolutionRelations(
				"tenant",
				test.relations,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("relation validation error = %v", err)
			}
		})
	}
}

func TestPostgresTargetEvolutionReservationsExposeOnlyUnmodeledNames(
	t *testing.T,
) {
	t.Parallel()

	table := postgresTargetEvolutionTestTable("tenant", "events")
	table.Indexes = []schema.Index{{
		Name:    "events_payload_idx",
		Columns: []schema.IndexColumn{{Name: "id"}},
	}}
	relations := []postgresTargetEvolutionRelation{
		postgresTargetEvolutionTestRelation(
			1,
			"tenant",
			"events",
			"r",
		),
		postgresTargetEvolutionTestRelation(
			2,
			"tenant",
			"events_pkey",
			"i",
		),
		postgresTargetEvolutionTestRelation(
			3,
			"tenant",
			"events_payload_idx",
			"i",
		),
		postgresTargetEvolutionTestRelation(
			4,
			"tenant",
			"custom_identity_sequence",
			"S",
		),
	}
	relations[0].canAlter = true
	for index := 1; index <= 2; index++ {
		relations[index].indexOwnerNamespace = "tenant"
		relations[index].indexOwnerTable = "events"
	}
	relations[3].sequenceOwnerNamespace = "tenant"
	relations[3].sequenceOwnerTable = "events"
	reservations, err := postgresTargetEvolutionReservations(
		[]schema.Table{table},
		relations,
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []TargetSchemaEvolutionNameReservation{{
		Scope:     "relation",
		Namespace: "tenant",
		Name:      "custom_identity_sequence",
	}}
	if !reflect.DeepEqual(reservations, want) {
		t.Fatalf("reservations = %#v, want %#v", reservations, want)
	}
}

func TestPostgresTargetEvolutionCatalogQueriesAreExhaustivePgCatalog(
	t *testing.T,
) {
	t.Parallel()

	for name, query := range map[string]string{
		"environment":  postgresTargetEvolutionEnvironmentQuery,
		"relations":    postgresTargetEvolutionRelationQuery,
		"dependencies": postgresTargetEvolutionDependencyQuery,
	} {
		if !strings.Contains(query, "pg_catalog.") {
			t.Fatalf("%s query does not use pg_catalog: %s", name, query)
		}
		if strings.Contains(query, "information_schema.") {
			t.Fatalf("%s query uses privilege-filtered information_schema", name)
		}
	}
	if !strings.Contains(
		postgresTargetEvolutionRelationQuery,
		"WHERE namespace.nspname = $1",
	) || !strings.Contains(
		postgresTargetEvolutionRelationQuery,
		"ORDER BY relation.relname, relation.oid",
	) {
		t.Fatal("relation query is not exact and deterministic")
	}
}

func TestValidatePostgresTargetEvolutionEnvironment(
	t *testing.T,
) {
	t.Parallel()

	valid := postgresTargetEvolutionEnvironment{
		namespaceObjectID: 1,
		databaseName:      "target",
		version:           160011,
		canUseNamespace:   true,
		canCreate:         true,
	}
	if err := validatePostgresTargetEvolutionEnvironment(
		"tenant",
		valid,
	); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		namespace string
		mutate    func(*postgresTargetEvolutionEnvironment)
		want      string
	}{
		{
			name:      "system namespace",
			namespace: "pg_catalog",
			mutate:    func(*postgresTargetEvolutionEnvironment) {},
			want:      "not an evolvable user namespace",
		},
		{
			name:      "wrong major",
			namespace: "tenant",
			mutate: func(value *postgresTargetEvolutionEnvironment) {
				value.version = 170000
			},
			want: "requires PostgreSQL 16",
		},
		{
			name:      "no create privilege",
			namespace: "tenant",
			mutate: func(value *postgresTargetEvolutionEnvironment) {
				value.canCreate = false
			},
			want: "USAGE and CREATE",
		},
		{
			name:      "recovery",
			namespace: "tenant",
			mutate: func(value *postgresTargetEvolutionEnvironment) {
				value.inRecovery = true
			},
			want: "read-only or in recovery",
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			value := valid
			test.mutate(&value)
			err := validatePostgresTargetEvolutionEnvironment(
				test.namespace,
				value,
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("environment validation error = %v", err)
			}
		})
	}
}

func TestValidatePostgresTargetEvolutionPlanNamespace(t *testing.T) {
	t.Parallel()

	plan, _, _ := postgresTargetEvolutionTestPlan()
	if err := validatePostgresTargetEvolutionPlanNamespace(
		plan,
		"tenant",
	); err != nil {
		t.Fatal(err)
	}
	plan.states[1][0].Schema = "other"
	err := validatePostgresTargetEvolutionPlanNamespace(plan, "tenant")
	if err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("namespace error = %v", err)
	}
}

type postgresTargetEvolutionRecordingExecutor struct {
	calls *[]string
	err   error
}

func (executor postgresTargetEvolutionRecordingExecutor) ExecContext(
	_ context.Context,
	statement string,
	_ ...any,
) (sql.Result, error) {
	if executor.calls != nil {
		*executor.calls = append(*executor.calls, "exec:"+statement)
	}
	return postgresTargetEvolutionResult(0), executor.err
}

type postgresTargetEvolutionResult int64

func (result postgresTargetEvolutionResult) LastInsertId() (int64, error) {
	return int64(result), nil
}

func (result postgresTargetEvolutionResult) RowsAffected() (int64, error) {
	return int64(result), nil
}

func postgresTargetEvolutionTestPlan() (
	TargetSchemaEvolutionPlan,
	[]schema.Table,
	[]schema.Table,
) {
	before := []schema.Table{
		postgresTargetEvolutionTestTable("tenant", "events"),
	}
	after := cloneTargetSchemaEvolutionTables(before)
	after[0].Columns = append(after[0].Columns, schema.Column{
		Name:     "note",
		Type:     "text",
		Nullable: true,
	})
	statement := `ALTER TABLE "tenant"."events" ADD COLUMN "note" text;`
	operation := TargetSchemaEvolutionOperation{
		action:       SchemaContractAddColumn,
		objects:      []schema.SchemaDriftObject{{Schema: "tenant", Table: "events", Column: "note"}},
		statements:   []string{statement},
		beforeDigest: "before",
		afterDigest:  "after",
	}
	return TargetSchemaEvolutionPlan{
		target:          schema.Postgres,
		operations:      []TargetSchemaEvolutionOperation{operation},
		states:          [][]schema.Table{before, after},
		observedPrefix:  0,
		authorityDigest: "authority",
		digest:          "plan",
	}, before, after
}

func postgresTargetEvolutionTestCatalog(
	tables []schema.Table,
) TargetSchemaEvolutionCatalog {
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, nil)
	if err != nil {
		panic(err)
	}
	return catalog
}

func postgresTargetEvolutionTestTable(
	namespace string,
	name string,
) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
}

func postgresTargetEvolutionTestRelation(
	objectID int64,
	namespace string,
	name string,
	kind string,
) postgresTargetEvolutionRelation {
	return postgresTargetEvolutionRelation{
		postgresExistingRelationName: postgresExistingRelationName{
			objectID:     objectID,
			namespace:    namespace,
			name:         name,
			relationKind: kind,
		},
	}
}
