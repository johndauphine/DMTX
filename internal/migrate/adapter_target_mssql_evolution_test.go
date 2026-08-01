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

func TestSQLServerTargetEvolutionCreatePlannerOrdersAndSealsCompleteObjects(
	t *testing.T,
) {
	t.Parallel()
	check, err := schema.ParseSQLiteCheckExpression(`value <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	parent := sqlServerTargetEvolutionTestTable("dbo", "parents")
	child := sqlServerTargetEvolutionTestTable("dbo", "children")
	child.Columns = append(child.Columns,
		schema.Column{
			Name: "parent_id", Type: "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
		schema.Column{
			Name: "value", Type: "text", Nullable: true,
			DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{20}},
		},
	)
	child.Indexes = []schema.Index{{
		Name: "children_value_idx", Columns: []schema.IndexColumn{{Name: "value"}},
	}}
	child.Checks = []schema.CheckConstraint{{
		Name: "children_value_check", Expression: check,
	}}
	child.ForeignKeys = []schema.ForeignKey{{
		Name: "children_parent_fk", Columns: []string{"parent_id"},
		ReferencedSchema: "dbo", ReferencedTable: "parents",
		ReferencedColumns: []string{"id"}, OnUpdate: "NO ACTION",
		OnDelete: "NO ACTION", Match: "SIMPLE",
	}}
	catalog, err := NewTargetSchemaEvolutionCatalog(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := (sqlServerTargetSchemaEvolutionCreatePlanner{}).
		PlanCompleteTargetSchemaCreates(
			schema.SQLServer,
			[]schema.Table{child},
			[]schema.Table{parent, child},
			catalog,
		)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.steps) != 4 {
		t.Fatalf("create step count = %d, want 4", len(bundle.steps))
	}
	want := []string{
		"CREATE TABLE [dbo].[children]",
		"CREATE NONCLUSTERED INDEX [children_value_idx]",
		"ALTER TABLE [dbo].[children] WITH CHECK ADD CONSTRAINT [children_value_check] CHECK",
		"ALTER TABLE [dbo].[children] WITH CHECK ADD CONSTRAINT [children_parent_fk] FOREIGN KEY",
	}
	for index, prefix := range want {
		if !strings.HasPrefix(bundle.steps[index].statement, prefix) {
			t.Fatalf("step %d statement = %q, want prefix %q", index, bundle.steps[index].statement, prefix)
		}
	}
	for index, expected := range []struct{ indexes, checks, foreignKeys int }{
		{}, {indexes: 1}, {indexes: 1, checks: 1}, {indexes: 1, checks: 1, foreignKeys: 1},
	} {
		table := bundle.steps[index].tables[0]
		if len(table.Indexes) != expected.indexes ||
			len(table.Checks) != expected.checks ||
			len(table.ForeignKeys) != expected.foreignKeys {
			t.Fatalf("step %d shape = indexes:%d checks:%d foreign_keys:%d", index, len(table.Indexes), len(table.Checks), len(table.ForeignKeys))
		}
	}
}

func TestSQLServerTargetEvolutionCreatePlannerRejectsReservationsAndUnnamedObjects(
	t *testing.T,
) {
	t.Parallel()
	created := sqlServerTargetEvolutionTestTable("dbo", "events")
	planner := sqlServerTargetSchemaEvolutionCreatePlanner{}
	t.Run("relation reservation", func(t *testing.T) {
		catalog, err := NewTargetSchemaEvolutionCatalog(nil, []TargetSchemaEvolutionNameReservation{{
			Scope: "relation", Namespace: "dbo", Name: "events",
		}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = planner.PlanCompleteTargetSchemaCreates(
			schema.SQLServer,
			[]schema.Table{created},
			[]schema.Table{created},
			catalog,
		)
		if err == nil || !strings.Contains(err.Error(), "relation reservation") {
			t.Fatalf("relation collision error = %v", err)
		}
	})
	t.Run("unnamed object", func(t *testing.T) {
		value := created
		value.Indexes = []schema.Index{{
			Columns: []schema.IndexColumn{{Name: "id"}},
		}}
		catalog, err := NewTargetSchemaEvolutionCatalog(nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = planner.PlanCompleteTargetSchemaCreates(
			schema.SQLServer,
			[]schema.Table{value},
			[]schema.Table{value},
			catalog,
		)
		if err == nil || !strings.Contains(err.Error(), "no explicit catalog name") {
			t.Fatalf("unnamed object error = %v", err)
		}
	})
}

func TestSQLServerTargetEvolutionSessionClassifiesCommittedPrefixAfterError(
	t *testing.T,
) {
	t.Parallel()
	plan, before, after := sqlServerTargetEvolutionTestPlan()
	reads := []TargetSchemaEvolutionCatalog{
		mustSQLServerTargetEvolutionCatalog(t, before),
		mustSQLServerTargetEvolutionCatalog(t, after),
	}
	readIndex := 0
	injected := errors.New("injected post-DDL transport error")
	session := &sqlServerTargetEvolutionMutationSession{
		executor: sqlServerTargetEvolutionRecordingExecutor{err: injected},
		readCatalog: func(context.Context) (TargetSchemaEvolutionCatalog, error) {
			if readIndex >= len(reads) {
				return TargetSchemaEvolutionCatalog{}, errors.New("unexpected catalog read")
			}
			catalog := reads[readIndex]
			readIndex++
			return catalog, nil
		},
		plan: plan,
	}
	err := ApplyTargetSchemaEvolution(context.Background(), plan, session)
	if err == nil || !errors.Is(err, injected) ||
		!strings.Contains(err.Error(), "verified prefix 1 of 1") {
		t.Fatalf("post-DDL error = %v", err)
	}
	var classified *TargetSchemaEvolutionError
	if !errors.As(err, &classified) || classified.Kind != TargetSchemaEvolutionApplyFailed {
		t.Fatalf("post-DDL error classification = %#v", err)
	}
}

func TestSQLServerTargetEvolutionCommitAmbiguityClassifiesExactCatalog(
	t *testing.T,
) {
	t.Parallel()
	plan, before, after := sqlServerTargetEvolutionTestPlan()
	commit := errors.New("injected lost commit acknowledgement")
	if err := classifySQLServerTargetEvolutionCommitCatalog(
		plan,
		mustSQLServerTargetEvolutionCatalog(t, after),
		nil,
		commit,
	); err != nil {
		t.Fatalf("exact final catalog should resolve commit ambiguity: %v", err)
	}
	err := classifySQLServerTargetEvolutionCommitCatalog(
		plan,
		mustSQLServerTargetEvolutionCatalog(t, before),
		nil,
		commit,
	)
	if err == nil || !errors.Is(err, commit) ||
		!strings.Contains(err.Error(), "prefix 0 of 1") {
		t.Fatalf("exact prior catalog commit ambiguity = %v", err)
	}
	drift := cloneTargetSchemaEvolutionTables(after)
	drift[0].Columns = append(drift[0].Columns, schema.Column{
		Name: "concurrent", Type: "text", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{16}},
	})
	err = classifySQLServerTargetEvolutionCommitCatalog(
		plan,
		mustSQLServerTargetEvolutionCatalog(t, drift),
		nil,
		commit,
	)
	if err == nil || !errors.Is(err, commit) ||
		!strings.Contains(err.Error(), "unexpected drift") {
		t.Fatalf("concurrent drift commit ambiguity = %v", err)
	}
}

func TestSQLServerTargetEvolutionReservationsPreserveTargetOnlyAndRejectUnknownShapes(
	t *testing.T,
) {
	t.Parallel()
	targetOnly := sqlServerTargetEvolutionTestTable("dbo", "audit")
	primary, err := schema.SQLServerPrimaryKeyConstraintName(targetOnly)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := sqlServerTargetEvolutionReservations("dbo", []sqlServerTargetEvolutionRelation{
		{objectID: 10, name: "audit", objectType: "U", typeDescription: "USER_TABLE"},
		{objectID: 11, name: primary, objectType: "PK", typeDescription: "PRIMARY_KEY_CONSTRAINT", parentObjectID: 10},
		{objectID: 12, name: "target_only_view", objectType: "V", typeDescription: "VIEW"},
	}, map[int64]schema.Table{10: targetOnly})
	if err != nil {
		t.Fatal(err)
	}
	if len(reservations) != 1 || reservations[0] != (TargetSchemaEvolutionNameReservation{
		Scope: "relation", Namespace: "dbo", Name: "target_only_view",
	}) {
		t.Fatalf("reservations = %#v", reservations)
	}
	_, err = sqlServerTargetEvolutionReservations("dbo", []sqlServerTargetEvolutionRelation{{
		objectID: 13, name: "unsafe_trigger", objectType: "TR", typeDescription: "SQL_TRIGGER", parentObjectID: 10,
	}}, map[int64]schema.Table{10: targetOnly})
	if err == nil || !strings.Contains(err.Error(), "unsupported attached") {
		t.Fatalf("unsupported attached relation error = %v", err)
	}
}

func TestSQLServerTargetEvolutionDefaultConstraintNameReservesCreateNamespace(
	t *testing.T,
) {
	t.Parallel()
	targetOnly := sqlServerTargetEvolutionTestTable("dbo", "audit")
	created := sqlServerTargetEvolutionTestTable("dbo", "events")
	createdPrimary, err := schema.SQLServerPrimaryKeyConstraintName(created)
	if err != nil {
		t.Fatal(err)
	}
	reservations, err := sqlServerTargetEvolutionReservations("dbo", []sqlServerTargetEvolutionRelation{
		{objectID: 10, name: "audit", objectType: "U", typeDescription: "USER_TABLE"},
		{objectID: 11, name: createdPrimary, objectType: "D", typeDescription: "DEFAULT_CONSTRAINT", parentObjectID: 10},
	}, map[int64]schema.Table{10: targetOnly})
	if err != nil {
		t.Fatal(err)
	}
	want := []TargetSchemaEvolutionNameReservation{{
		Scope: "constraint", Namespace: "dbo", Name: createdPrimary,
	}}
	if !reflect.DeepEqual(reservations, want) {
		t.Fatalf("default-constraint reservations = %#v, want %#v", reservations, want)
	}
	catalog, err := NewTargetSchemaEvolutionCatalog([]schema.Table{targetOnly}, reservations)
	if err != nil {
		t.Fatal(err)
	}
	_, err = (sqlServerTargetSchemaEvolutionCreatePlanner{}).PlanCompleteTargetSchemaCreates(
		schema.SQLServer,
		[]schema.Table{created},
		[]schema.Table{targetOnly, created},
		catalog,
	)
	if err == nil || !strings.Contains(err.Error(), "constraint reservation") {
		t.Fatalf("default-constraint create collision = %v", err)
	}
}

func TestValidateSQLServerTargetEvolutionEnvironmentFailsClosed(t *testing.T) {
	t.Parallel()
	valid := sqlServerTargetEvolutionEnvironment{
		databaseName: "target", compatibilityLevel: 160, state: "ONLINE",
		userAccess: "MULTI_USER", containment: "NONE", productMajor: 16,
		engineEdition: 3, productVersion: "16.0.1000.1", namespaceID: 1,
		namespace: "dbo", viewDefinition: true, schemaControl: true,
		createTable: true, schemaAlter: true,
	}
	if err := validateSQLServerTargetEvolutionEnvironment("dbo", valid); err != nil {
		t.Fatal(err)
	}
	valid.compatibilityLevel = 150
	if err := validateSQLServerTargetEvolutionEnvironment("dbo", valid); err == nil ||
		!strings.Contains(err.Error(), "certified writable") {
		t.Fatalf("compatibility error = %v", err)
	}
}

func TestJoinSQLServerTargetEvolutionRollbackErrorPreservesPrimary(t *testing.T) {
	t.Parallel()
	primary := errors.New("injected operation failure")
	rollback := errors.New("injected rollback failure")
	err := joinSQLServerTargetEvolutionRollbackError(primary, rollback)
	if !errors.Is(err, primary) || !errors.Is(err, rollback) {
		t.Fatalf("rollback join = %v", err)
	}
	closeErr := errors.New("injected pinned-connection close failure")
	err = joinSQLServerTargetEvolutionConnectionCloseError(primary, closeErr)
	if !errors.Is(err, primary) || !errors.Is(err, closeErr) {
		t.Fatalf("connection-close join = %v", err)
	}
}

func TestSQLServerTargetEvolutionUsesTransactionOwnedFence(t *testing.T) {
	t.Parallel()
	if !strings.Contains(sqlServerTargetEvolutionAcquireLockQuery, "@LockOwner = 'Transaction'") {
		t.Fatalf("application fence is not transaction-owned: %s", sqlServerTargetEvolutionAcquireLockQuery)
	}
	if strings.Contains(sqlServerTargetEvolutionAcquireLockQuery, "@LockOwner = 'Session'") {
		t.Fatalf("application fence retains session ownership: %s", sqlServerTargetEvolutionAcquireLockQuery)
	}
}

func TestCanonicalizeSQLServerTargetChecksMatchesCatalogRoundTrip(t *testing.T) {
	t.Parallel()
	check, err := schema.ParseSQLiteCheckExpression("id > 0")
	if err != nil {
		t.Fatal(err)
	}
	table := sqlServerTargetEvolutionTestTable("dbo", "checked")
	table.Checks = []schema.CheckConstraint{{Name: "checked_id", Expression: check}}
	if err := canonicalizeSQLServerTargetChecks(&table); err != nil {
		t.Fatal(err)
	}
	if got := table.Checks[0].Expression.CanonicalSQL(); got != `"id" > 0` {
		t.Fatalf("canonical SQL = %q, want %q", got, `"id" > 0`)
	}
	first := table.Checks[0].Expression.CanonicalSQL()
	if err := canonicalizeSQLServerTargetChecks(&table); err != nil {
		t.Fatal(err)
	}
	if got := table.Checks[0].Expression.CanonicalSQL(); got != first {
		t.Fatalf("second canonicalization = %q, want %q", got, first)
	}
}

func TestCanonicalizeSQLServerTargetForeignKeysMatchesCatalogRoundTrip(t *testing.T) {
	t.Parallel()
	table := sqlServerTargetEvolutionTestTable("dbo", "children")
	table.ForeignKeys = []schema.ForeignKey{{
		Name: "children_parent", Columns: []string{"id"},
		ReferencedSchema: "dbo", ReferencedTable: "parents",
		ReferencedColumns: []string{"id"}, Match: "NONE",
	}}
	if err := canonicalizeSQLServerTargetForeignKeys(&table); err != nil {
		t.Fatal(err)
	}
	if got := table.ForeignKeys[0].Match; got != "SIMPLE" {
		t.Fatalf("canonical MATCH = %q, want SIMPLE", got)
	}
	table.ForeignKeys[0].Match = "PARTIAL"
	if err := canonicalizeSQLServerTargetForeignKeys(&table); err == nil || !strings.Contains(err.Error(), "unsupported MATCH") {
		t.Fatalf("unsupported MATCH error = %v", err)
	}
}

func TestSQLServerTargetEvolutionRejectsNewDefaultConstraintBeforeAdapterAdmission(
	t *testing.T,
) {
	t.Parallel()
	defaultValue, err := schema.ParseSQLiteDefault("7")
	if err != nil {
		t.Fatal(err)
	}
	prior := []schema.Table{sqlServerTargetEvolutionTestTable("dbo", "events")}
	newTable := sqlServerTargetEvolutionTestTable("dbo", "new_events")
	newTable.Columns = append(newTable.Columns, schema.Column{
		Name: "status", Type: "integer", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "int"},
		Default:      defaultValue,
	})
	addColumn := cloneTargetSchemaEvolutionTables(prior)
	addColumn[0].Columns = append(addColumn[0].Columns, schema.Column{
		Name: "status", Type: "integer", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "int"},
		Default:      defaultValue,
	})
	for _, test := range []struct {
		name    string
		current []schema.Table
	}{
		{name: "new eligible table", current: append(cloneTargetSchemaEvolutionTables(prior), newTable)},
		{name: "nullable added column", current: addColumn},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			adapter := &sqlServerTargetAdapter{}
			_, err := adapter.PreflightTargetSchemaEvolution(
				context.Background(),
				TargetSchemaEvolutionRequest{
					target: schema.SQLServer, priorTables: prior, currentTables: test.current,
				},
			)
			if err == nil || !strings.Contains(err.Error(), "does not support creating default constraints") {
				t.Fatalf("default-constraint preflight error = %v", err)
			}
			if strings.Contains(err.Error(), "adapter is not configured") {
				t.Fatalf("default transition reached adapter admission: %v", err)
			}
		})
	}
}

func TestValidateSQLServerTargetEvolutionPlanNamespace(t *testing.T) {
	t.Parallel()
	plan, _, _ := sqlServerTargetEvolutionTestPlan()
	if err := validateSQLServerTargetEvolutionPlanNamespace(plan, "dbo"); err != nil {
		t.Fatal(err)
	}
	plan.states[1][0].Schema = "other"
	err := validateSQLServerTargetEvolutionPlanNamespace(plan, "dbo")
	if err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("namespace error = %v", err)
	}
}

func TestValidateSQLServerTargetEvolutionEnvironmentAcceptsConfiguredSchema(t *testing.T) {
	t.Parallel()
	value := sqlServerTargetEvolutionEnvironment{
		databaseName: "target", compatibilityLevel: 160, state: "ONLINE",
		userAccess: "MULTI_USER", containment: "NONE", productMajor: 16,
		engineEdition: 3, productVersion: "16.0.1000.1", namespaceID: 7,
		namespace: "dmtx_stage4", viewDefinition: true, schemaControl: true,
		createTable: true, schemaAlter: true,
	}
	if err := validateSQLServerTargetEvolutionEnvironment("dmtx_stage4", value); err != nil {
		t.Fatalf("validate configured SQL Server schema: %v", err)
	}
}

func sqlServerTargetEvolutionTestTable(namespace, name string) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
	}
}

func sqlServerTargetEvolutionTestPlan() (TargetSchemaEvolutionPlan, []schema.Table, []schema.Table) {
	before := []schema.Table{sqlServerTargetEvolutionTestTable("dbo", "events")}
	after := cloneTargetSchemaEvolutionTables(before)
	after[0].Columns = append(after[0].Columns, schema.Column{
		Name: "note", Type: "text", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{32}},
	})
	return TargetSchemaEvolutionPlan{
		target: schema.SQLServer,
		operations: []TargetSchemaEvolutionOperation{{
			action: SchemaContractAddColumn,
			objects: []schema.SchemaDriftObject{{
				Schema: "dbo", Table: "events", Column: "note",
			}},
			statements:   []string{"ALTER TABLE [dbo].[events] ADD [note] VARCHAR(32) NULL;"},
			beforeDigest: "before",
			afterDigest:  "after",
		}},
		states:          [][]schema.Table{before, after},
		observedPrefix:  0,
		authorityDigest: "authority",
		digest:          "plan",
	}, before, after
}

func mustSQLServerTargetEvolutionCatalog(t *testing.T, tables []schema.Table) TargetSchemaEvolutionCatalog {
	t.Helper()
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, nil)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

type sqlServerTargetEvolutionRecordingExecutor struct{ err error }

func (executor sqlServerTargetEvolutionRecordingExecutor) ExecContext(
	context.Context,
	string,
	...any,
) (sql.Result, error) {
	return sqlServerTargetEvolutionTestResult(0), executor.err
}

type sqlServerTargetEvolutionTestResult int64

func (result sqlServerTargetEvolutionTestResult) LastInsertId() (int64, error) {
	return int64(result), nil
}

func (result sqlServerTargetEvolutionTestResult) RowsAffected() (int64, error) {
	return int64(result), nil
}
