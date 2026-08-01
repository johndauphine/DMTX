package migrate

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLTargetEvolutionCreatePlannerOrdersCompleteObjects(t *testing.T) {
	t.Parallel()
	check, err := schema.ParseSQLiteCheckExpression(`value <> ''`)
	if err != nil {
		t.Fatal(err)
	}
	parent := mysqlTargetEvolutionTestTable("tenant", "parents")
	child := mysqlTargetEvolutionTestTable("tenant", "children")
	child.Columns = append(child.Columns, schema.Column{
		Name: "parent_id", Type: "bigint",
	}, schema.Column{
		Name: "value", Type: "varchar", Nullable: true,
		DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{20}},
	})
	child.Indexes = []schema.Index{{
		Name: "children_value_idx", Columns: []schema.IndexColumn{{Name: "value"}},
	}}
	child.Checks = []schema.CheckConstraint{{
		Name: "children_value_check", Expression: check,
	}}
	child.ForeignKeys = []schema.ForeignKey{{
		Name: "children_parent_fk", Columns: []string{"parent_id"},
		ReferencedSchema: "tenant", ReferencedTable: "parents",
		ReferencedColumns: []string{"id"},
	}}
	catalog, err := NewTargetSchemaEvolutionCatalog(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := (mysqlTargetSchemaEvolutionCreatePlanner{
		flavor: engine.MySQLServerFlavorOracle80,
	}).PlanCompleteTargetSchemaCreates(
		schema.MySQL,
		[]schema.Table{child},
		[]schema.Table{child, parent},
		catalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(bundle.steps) != 4 {
		t.Fatalf("create step count = %d, want 4", len(bundle.steps))
	}
	want := []string{
		"CREATE TABLE `tenant`.`children`",
		"CREATE INDEX `children_value_idx`",
		"ALTER TABLE `tenant`.`children` ADD CONSTRAINT `children_value_check` CHECK",
		"ALTER TABLE `tenant`.`children` ADD CONSTRAINT `children_parent_fk` FOREIGN KEY",
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

func TestMySQLTargetEvolutionCreatePlannerRejectsAuthorityCollisions(t *testing.T) {
	t.Parallel()
	check, err := schema.ParseSQLiteCheckExpression(`id > 0`)
	if err != nil {
		t.Fatal(err)
	}
	created := mysqlTargetEvolutionTestTable("tenant", "events")
	created.Checks = []schema.CheckConstraint{{Name: "shared_check", Expression: check}}
	planner := mysqlTargetSchemaEvolutionCreatePlanner{
		flavor: engine.MySQLServerFlavorMariaDB1011,
	}
	t.Run("relation reservation", func(t *testing.T) {
		catalog, err := NewTargetSchemaEvolutionCatalog(nil, []TargetSchemaEvolutionNameReservation{{
			Scope: "relation", Namespace: "tenant", Name: "events",
		}})
		if err != nil {
			t.Fatal(err)
		}
		_, err = planner.PlanCompleteTargetSchemaCreates(schema.MySQL, []schema.Table{created}, []schema.Table{created}, catalog)
		if err == nil || !strings.Contains(err.Error(), "collides with an existing relation reservation") {
			t.Fatalf("relation collision error = %v", err)
		}
	})
	t.Run("constraint", func(t *testing.T) {
		audit := mysqlTargetEvolutionTestTable("tenant", "audit")
		audit.Checks = []schema.CheckConstraint{{Name: "shared_check", Expression: check}}
		catalog, err := NewTargetSchemaEvolutionCatalog([]schema.Table{audit}, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = planner.PlanCompleteTargetSchemaCreates(schema.MySQL, []schema.Table{created}, []schema.Table{created}, catalog)
		if err == nil || !strings.Contains(err.Error(), "collides with existing target constraint") {
			t.Fatalf("constraint collision error = %v", err)
		}
	})
}

func TestMySQLTargetEvolutionSessionClassifiesCommittedPrefixAfterError(t *testing.T) {
	t.Parallel()
	plan, before, after := mysqlTargetEvolutionTestPlan()
	reads := []TargetSchemaEvolutionCatalog{
		mustMySQLTargetEvolutionCatalog(t, before),
		mustMySQLTargetEvolutionCatalog(t, after),
	}
	readIndex := 0
	injected := errors.New("injected post-DDL transport error")
	session := &mysqlTargetEvolutionMutationSession{
		executor: mysqlTargetEvolutionRecordingExecutor{err: injected},
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

func TestValidateMySQLTargetEvolutionPlanNamespace(t *testing.T) {
	t.Parallel()
	plan, _, _ := mysqlTargetEvolutionTestPlan()
	if err := validateMySQLTargetEvolutionPlanNamespace(plan, "tenant"); err != nil {
		t.Fatal(err)
	}
	plan.states[1][0].Schema = "other"
	err := validateMySQLTargetEvolutionPlanNamespace(plan, "tenant")
	if err == nil || !strings.Contains(err.Error(), "outside") {
		t.Fatalf("namespace error = %v", err)
	}
}

func TestJoinMySQLTargetEvolutionReleaseErrorPreservesPrimaryAndCleanup(t *testing.T) {
	t.Parallel()
	primary := errors.New("injected operation failure")
	release := errors.New("injected RELEASE_LOCK failure")
	err := joinMySQLTargetEvolutionReleaseError(primary, release)
	if !errors.Is(err, primary) || !errors.Is(err, release) {
		t.Fatalf("joined error = %v; want both primary and release causes", err)
	}
	var classified *TargetSchemaEvolutionError
	if !errors.As(err, &classified) || classified.Kind != TargetSchemaEvolutionVerifyFailed {
		t.Fatalf("release cleanup classification = %#v", err)
	}
	if !strings.Contains(err.Error(), "could not be released") ||
		!strings.Contains(err.Error(), "re-read the complete target catalog") {
		t.Fatalf("release cleanup recovery guidance = %v", err)
	}
}

func TestMySQLTargetEvolutionCapabilityRunsThroughComposedStage4BeforeData(t *testing.T) {
	fixture := newStage4AdapterEvolutionFixture(t)
	fixture.cfg.Target = config.Endpoint{
		Type: "mysql", Host: "target.example", Port: 3306,
		Database: "tenant", User: "target-user", Password: "target-password",
	}
	target := &mysqlTargetEvolutionStage4Target{
		stage4AdapterEvolutionTarget: fixture.target,
	}
	result, err := migrateWithStage4Adapters(
		context.Background(),
		fixture.cfg,
		fixture.observer,
		fixture.source,
		target,
		"upsert",
		fixture.observer.run,
	)
	if err != nil {
		t.Fatalf("run composed MySQL target-evolution route: %v", err)
	}
	if result != (Result{Tables: 1, Rows: 2, Validated: true}) {
		t.Fatalf("composed MySQL target-evolution result = %#v", result)
	}
	if target.applyCalls != 1 || len(target.preflightTargets) != 2 {
		t.Fatalf(
			"MySQL composed evolution apply=%d target preflights=%v",
			target.applyCalls,
			target.preflightTargets,
		)
	}
	for _, dialect := range target.preflightTargets {
		if dialect != schema.MySQL {
			t.Fatalf("composed capability preflight dialect = %q, want mysql", dialect)
		}
	}
	assertStage4AdapterEventBefore(
		t,
		*fixture.events,
		"evolution_apply",
		"target_write",
	)
}

// mysqlTargetEvolutionStage4Target keeps the existing composed-runner fixture
// but advertises the real MySQL evolution seams. It verifies that Stage 4
// reaches a MySQL-family capability and applies its plan before page writes;
// database-specific DDL and recovery remain covered by the native tests.
type mysqlTargetEvolutionStage4Target struct {
	*stage4AdapterEvolutionTarget
	preflightTargets []schema.Dialect
}

func (*mysqlTargetEvolutionStage4Target) Engine() string {
	return "mysql"
}

func (*mysqlTargetEvolutionStage4Target) TargetSchemaEvolutionDialect() schema.Dialect {
	return schema.MySQL
}

func (*mysqlTargetEvolutionStage4Target) TargetSchemaEvolutionCreatePlanner() TargetSchemaEvolutionCreatePlanner {
	return mysqlTargetSchemaEvolutionCreatePlanner{
		flavor: engine.MySQLServerFlavorOracle80,
	}
}

func (target *mysqlTargetEvolutionStage4Target) PreflightTargetSchemaEvolution(
	ctx context.Context,
	request TargetSchemaEvolutionRequest,
) (TargetSchemaEvolutionPlan, error) {
	target.preflightTargets = append(target.preflightTargets, request.target)
	return target.stage4AdapterEvolutionTarget.PreflightTargetSchemaEvolution(ctx, request)
}

func mysqlTargetEvolutionTestTable(namespace, name string) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{{
			Name: "id", Type: "bigint", PrimaryKey: true, PrimaryKeyPosition: 1,
		}},
	}
}

func mysqlTargetEvolutionTestPlan() (TargetSchemaEvolutionPlan, []schema.Table, []schema.Table) {
	before := []schema.Table{mysqlTargetEvolutionTestTable("tenant", "events")}
	after := cloneTargetSchemaEvolutionTables(before)
	after[0].Columns = append(after[0].Columns, schema.Column{
		Name: "note", Type: "text", Nullable: true,
	})
	return TargetSchemaEvolutionPlan{
		target: schema.MySQL,
		operations: []TargetSchemaEvolutionOperation{{
			action: SchemaContractAddColumn,
			objects: []schema.SchemaDriftObject{{
				Schema: "tenant", Table: "events", Column: "note",
			}},
			statements:   []string{"ALTER TABLE `tenant`.`events` ADD COLUMN `note` TEXT;"},
			beforeDigest: "before",
			afterDigest:  "after",
		}},
		states:          [][]schema.Table{before, after},
		observedPrefix:  0,
		authorityDigest: "authority",
		digest:          "plan",
	}, before, after
}

func mustMySQLTargetEvolutionCatalog(t *testing.T, tables []schema.Table) TargetSchemaEvolutionCatalog {
	t.Helper()
	catalog, err := NewTargetSchemaEvolutionCatalog(tables, nil)
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

type mysqlTargetEvolutionRecordingExecutor struct{ err error }

func (executor mysqlTargetEvolutionRecordingExecutor) ExecContext(
	context.Context,
	string,
	...any,
) (sql.Result, error) {
	return mysqlTargetEvolutionTestResult(0), executor.err
}

type mysqlTargetEvolutionTestResult int64

func (result mysqlTargetEvolutionTestResult) LastInsertId() (int64, error) {
	return int64(result), nil
}

func (result mysqlTargetEvolutionTestResult) RowsAffected() (int64, error) {
	return int64(result), nil
}
