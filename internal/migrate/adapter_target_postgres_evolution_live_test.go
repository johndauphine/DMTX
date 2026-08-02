package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresTargetEvolutionCatalogFenceLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL evolution fence sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL evolution sentinel DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL evolution sentinel: %T", err)
	}
	database.SetMaxOpenConns(4)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL evolution sentinel: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL evolution sentinel: %T", err)
	}

	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	namespace := "dmtx_pg_evolution_" + suffix
	dependentNamespace := "dmtx_pg_evolution_dependent_" + suffix
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL evolution namespace: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(dependentNamespace),
	); err != nil {
		t.Fatalf("create PostgreSQL dependent namespace: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		for _, name := range []string{
			dependentNamespace,
			namespace,
		} {
			if _, err := database.ExecContext(
				cleanupCtx,
				"DROP SCHEMA IF EXISTS "+
					postgresIdentifier(name)+" CASCADE",
			); err != nil {
				t.Errorf("drop PostgreSQL sentinel schema %s: %v", name, err)
			}
		}
	})
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+postgresQualified(namespace, "events")+
			` ("id" bigint NOT NULL PRIMARY KEY)`,
	); err != nil {
		t.Fatalf("create PostgreSQL evolution table: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(dependentNamespace, "external_child")+
			` ("id" bigint NOT NULL PRIMARY KEY, "event_id" bigint NOT NULL)`,
	); err != nil {
		t.Fatalf("create PostgreSQL dependent table: %v", err)
	}

	connection, err := database.Conn(ctx)
	if err != nil {
		t.Fatalf("acquire PostgreSQL evolution fence connection: %v", err)
	}
	defer connection.Close()
	transaction, err := connection.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		t.Fatalf("begin PostgreSQL evolution fence: %v", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(
		ctx,
		postgresTargetEvolutionAdvisoryLockStatement,
		postgresTargetEvolutionAdvisoryKey(namespace),
	); err != nil {
		t.Fatalf("acquire PostgreSQL evolution advisory fence: %v", err)
	}
	before, _, err := readPostgresTargetEvolutionCatalog(
		ctx,
		transaction,
		namespace,
		true,
	)
	if err != nil {
		t.Fatalf("read locked PostgreSQL evolution catalog: %v", err)
	}
	beforeTables := before.Tables()
	if len(beforeTables) != 1 || beforeTables[0].Name != "events" {
		t.Fatalf("locked catalog = %#v", beforeTables)
	}

	assertPostgresEvolutionLockTimeout(
		t,
		ctx,
		database,
		"ALTER TABLE "+postgresQualified(namespace, "events")+
			` ADD COLUMN "blocked_alter" text`,
	)
	assertPostgresEvolutionLockTimeout(
		t,
		ctx,
		database,
		"ALTER TABLE "+
			postgresQualified(dependentNamespace, "external_child")+
			` ADD CONSTRAINT "blocked_external_fk" FOREIGN KEY ("event_id") REFERENCES `+
			postgresQualified(namespace, "events")+` ("id")`,
	)

	// PostgreSQL has no user-level schema lock that blocks arbitrary CREATE.
	// The serializable transaction keeps its old snapshot, making a separate
	// post-commit snapshot a required part of the success boundary.
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			postgresQualified(namespace, "concurrent_create")+
			` ("id" bigint NOT NULL PRIMARY KEY)`,
	); err != nil {
		t.Fatalf("create concurrent PostgreSQL namespace table: %v", err)
	}
	stale, _, err := readPostgresTargetEvolutionCatalog(
		ctx,
		transaction,
		namespace,
		true,
	)
	if err != nil {
		t.Fatalf("reread PostgreSQL transaction snapshot: %v", err)
	}
	staleTables := stale.Tables()
	if len(staleTables) != 1 || staleTables[0].Name != "events" {
		t.Fatalf("transaction snapshot unexpectedly moved: %#v", staleTables)
	}
	if err := transaction.Rollback(); err != nil {
		t.Fatalf("roll back PostgreSQL evolution fence: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("release PostgreSQL evolution fence connection: %v", err)
	}

	adapter := &postgresTargetAdapter{
		database:  database,
		namespace: namespace,
	}
	if _, err := database.ExecContext(
		ctx,
		"ALTER TABLE "+
			postgresQualified(dependentNamespace, "external_child")+
			` ADD CONSTRAINT "external_fk_hazard" FOREIGN KEY ("event_id") REFERENCES `+
			postgresQualified(namespace, "events")+` ("id")`,
	); err != nil {
		t.Fatalf("create PostgreSQL external FK hazard: %v", err)
	}
	if _, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx); err == nil ||
		!strings.Contains(err.Error(), "external foreign-key dependency") {
		t.Fatalf("external PostgreSQL FK dependency error = %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"ALTER TABLE "+
			postgresQualified(dependentNamespace, "external_child")+
			` DROP CONSTRAINT "external_fk_hazard"`,
	); err != nil {
		t.Fatalf("drop PostgreSQL external FK hazard: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE VIEW "+
			postgresQualified(dependentNamespace, "events_view")+
			" AS SELECT id FROM "+
			postgresQualified(namespace, "events"),
	); err != nil {
		t.Fatalf("create PostgreSQL external view hazard: %v", err)
	}
	if _, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx); err == nil ||
		!strings.Contains(err.Error(), "external view dependency") {
		t.Fatalf("external PostgreSQL view dependency error = %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"DROP VIEW "+
			postgresQualified(dependentNamespace, "events_view"),
	); err != nil {
		t.Fatalf("drop PostgreSQL external view hazard: %v", err)
	}
	committed, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read independent PostgreSQL catalog snapshot: %v", err)
	}
	committedTables := committed.Tables()
	if len(committedTables) != 2 ||
		committedTables[0].Name != "concurrent_create" ||
		committedTables[1].Name != "events" {
		t.Fatalf(
			"independent catalog did not expose concurrent CREATE: %#v",
			committedTables,
		)
	}

	desired := cloneTargetSchemaEvolutionTables(committedTables)
	eventsIndex := findTargetSchemaEvolutionTable(
		desired,
		targetSchemaEvolutionTableKey{
			schema: namespace,
			table:  "events",
		},
	)
	if eventsIndex < 0 {
		t.Fatal("independent catalog omitted events table")
	}
	desired[eventsIndex].Columns = append(
		desired[eventsIndex].Columns,
		schema.Column{
			Name:     "note",
			Type:     "text",
			Nullable: true,
		},
	)
	statement := "ALTER TABLE " +
		postgresQualified(namespace, "events") +
		` ADD COLUMN "note" TEXT NULL;`
	plan := TargetSchemaEvolutionPlan{
		target: schema.Postgres,
		operations: []TargetSchemaEvolutionOperation{{
			action: SchemaContractAddColumn,
			objects: []schema.SchemaDriftObject{{
				Kind:   schema.SchemaDriftObjectColumn,
				Schema: namespace,
				Table:  "events",
				Column: "note",
			}},
			statements:   []string{statement},
			beforeDigest: "live-before",
			afterDigest:  "live-after",
		}},
		states:          [][]schema.Table{committedTables, desired},
		reservations:    committed.Reservations(),
		authorityDigest: "live-authority",
		digest:          "live-plan",
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, plan); err != nil {
		t.Fatalf("apply PostgreSQL target evolution live: %v", err)
	}
	verified, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("verify applied PostgreSQL target evolution live: %v", err)
	}
	verifiedTables := verified.Tables()
	verifiedEvents := findTargetSchemaEvolutionTable(
		verifiedTables,
		targetSchemaEvolutionTableKey{
			schema: namespace,
			table:  "events",
		},
	)
	if verifiedEvents < 0 ||
		findTargetSchemaEvolutionColumnIndex(
			verifiedTables[verifiedEvents],
			"note",
		) < 0 {
		t.Fatalf(
			"applied PostgreSQL evolution is absent from catalog: %#v",
			verifiedTables,
		)
	}
}

func TestPostgresTargetEvolutionRealPlannerLive(t *testing.T) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL evolution planner sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL evolution planner DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL evolution planner sentinel: %T", err)
	}
	database.SetMaxOpenConns(4)
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close PostgreSQL evolution planner sentinel: %v", err)
		}
	})
	if err := database.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL evolution planner sentinel: %T", err)
	}

	suffix := fmt.Sprintf("%d_%d", os.Getpid(), time.Now().UnixNano())
	namespace := "dmtx_pg_evolution_planner_" + suffix
	if _, err := database.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL evolution planner namespace: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := database.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf(
				"drop PostgreSQL evolution planner schema %s: %v",
				namespace,
				err,
			)
		}
	})

	labelColumn := schema.Column{
		Name: "label", Type: "text", Nullable: true,
	}
	labelCatalogDefault := `'queued'::text`
	labelDefault, err := schema.ParsePostgresCatalogDefault(
		labelColumn,
		&labelCatalogDefault,
	)
	if err != nil {
		t.Fatal(err)
	}
	labelColumn.Default = labelDefault
	amountColumn := schema.Column{Name: "amount", Type: "integer"}
	amountCatalogDefault := `0`
	amountDefault, err := schema.ParsePostgresCatalogDefault(
		amountColumn,
		&amountCatalogDefault,
	)
	if err != nil {
		t.Fatal(err)
	}
	amountColumn.Default = amountDefault
	amountCheck, err := schema.ParsePostgresCatalogCheck(
		`amount >= 0`,
		[]schema.Column{amountColumn},
	)
	if err != nil {
		t.Fatal(err)
	}
	sourceTables := []schema.Table{
		{
			Schema: "source",
			Name:   "parents",
			Identity: &schema.Identity{
				Column:     "id",
				Generation: schema.IdentityByDefault,
			},
			Columns: []schema.Column{
				{
					Name: "id", Type: "bigint",
					PrimaryKey: true, PrimaryKeyPosition: 1,
				},
				{
					Name: "code", Type: "varchar",
					DeclaredType: &schema.DeclaredType{
						Base: "varchar", Arguments: []int{32},
					},
				},
			},
			Indexes: []schema.Index{{
				Name:   "parents_code_key",
				Unique: true,
				Columns: []schema.IndexColumn{{
					Name:      "code",
					Collation: "BINARY",
				}},
			}},
		},
		{
			Schema: "source",
			Name:   "children",
			Identity: &schema.Identity{
				Column:     "id",
				Generation: schema.IdentityByDefault,
			},
			Columns: []schema.Column{
				{
					Name: "id", Type: "bigint",
					PrimaryKey: true, PrimaryKeyPosition: 1,
				},
				{Name: "parent_id", Type: "bigint"},
				labelColumn,
				amountColumn,
			},
			Indexes: []schema.Index{{
				Name: "children_parent_idx",
				Columns: []schema.IndexColumn{{
					Name: "parent_id",
				}},
			}},
			Checks: []schema.CheckConstraint{{
				Expression: amountCheck,
			}},
			ForeignKeys: []schema.ForeignKey{{
				Columns:           []string{"parent_id"},
				ReferencedSchema:  "source",
				ReferencedTable:   "parents",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "NO ACTION",
				OnDelete:          "CASCADE",
				Match:             "SIMPLE",
			}},
		},
	}
	gate := stage4TargetSchemaProjectionGate(
		t,
		nil,
		sourceTables,
		"upsert",
		false,
	)
	adapter := &postgresTargetAdapter{
		database:  database,
		namespace: namespace,
	}
	initialCatalog, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read initial PostgreSQL target authority: %v", err)
	}
	authority := stage4TargetSchemaProjectionAuthorityFromCatalog(
		t,
		gate,
		"postgres",
		"postgres",
		"upsert",
		initialCatalog,
	)
	projection, err := BuildStage4TargetSchemaEvolutionProjection(
		gate,
		authority,
		"postgres",
		adapter,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range projection.CurrentTables() {
		for _, index := range table.Indexes {
			if index.Name == "" {
				t.Fatalf("target index name was not materialized: %#v", table)
			}
		}
		for _, check := range table.Checks {
			if check.Name == "" {
				t.Fatalf("target CHECK name was not materialized: %#v", table)
			}
		}
		for _, foreignKey := range table.ForeignKeys {
			if foreignKey.Name == "" {
				t.Fatalf(
					"target foreign-key name was not materialized: %#v",
					table,
				)
			}
		}
	}
	request, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		projection,
		adapter.TargetSchemaEvolutionCreatePlanner(),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil {
		t.Fatalf("preflight real PostgreSQL evolution plan: %v", err)
	}
	if plan.Complete() || plan.OperationCount() < 6 {
		t.Fatalf(
			"initial real plan complete=%v operations=%d",
			plan.Complete(),
			plan.OperationCount(),
		)
	}
	firstOperation := plan.PendingOperations()[0]
	firstStatements := firstOperation.Statements()
	if len(firstStatements) != 1 {
		t.Fatalf(
			"first real operation statements=%#v, want one crash boundary",
			firstStatements,
		)
	}
	if _, err := database.ExecContext(ctx, firstStatements[0]); err != nil {
		t.Fatalf("simulate first real PostgreSQL evolution prefix: %v", err)
	}
	resumed, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil {
		actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
		var facts []schema.SchemaDriftFact
		if readErr == nil {
			expectedSnapshot, expectedErr := schema.NewSchemaSnapshot(
				plan.states[1],
			)
			actualSnapshot, actualErr := schema.NewSchemaSnapshot(
				actual.Tables(),
			)
			if expectedErr == nil && actualErr == nil {
				facts, _ = schema.CompareSchemaSnapshots(
					expectedSnapshot,
					actualSnapshot,
				)
			}
		}
		t.Fatalf(
			"resume real PostgreSQL evolution prefix: %v; catalog read=%v facts=%#v",
			err,
			readErr,
			facts,
		)
	}
	if resumed.AppliedPrefix() != 1 ||
		resumed.Digest() != plan.Digest() {
		t.Fatalf(
			"resumed real plan prefix=%d digest=%s want prefix=1 digest=%s",
			resumed.AppliedPrefix(),
			resumed.Digest(),
			plan.Digest(),
		)
	}
	for resumed.OperationCount()-resumed.AppliedPrefix() > 1 {
		nextPrefix := resumed.AppliedPrefix() + 1
		nextOperation := resumed.PendingOperations()[0]
		nextStatements := nextOperation.Statements()
		if len(nextStatements) != 1 {
			t.Fatalf(
				"real operation %d statements=%#v, want one crash boundary",
				resumed.AppliedPrefix(),
				nextStatements,
			)
		}
		if _, err := database.ExecContext(ctx, nextStatements[0]); err != nil {
			t.Fatalf(
				"simulate real PostgreSQL evolution prefix %d: %v",
				nextPrefix,
				err,
			)
		}
		next, preflightErr := adapter.PreflightTargetSchemaEvolution(
			ctx,
			request,
		)
		if preflightErr != nil {
			actual, readErr := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
			var facts []schema.SchemaDriftFact
			if readErr == nil {
				expectedSnapshot, expectedErr := schema.NewSchemaSnapshot(
					plan.states[nextPrefix],
				)
				actualSnapshot, actualErr := schema.NewSchemaSnapshot(
					actual.Tables(),
				)
				if expectedErr == nil && actualErr == nil {
					facts, _ = schema.CompareSchemaSnapshots(
						expectedSnapshot,
						actualSnapshot,
					)
				}
			}
			t.Fatalf(
				"resume real PostgreSQL evolution prefix %d: %v; catalog read=%v facts=%#v",
				nextPrefix,
				preflightErr,
				readErr,
				facts,
			)
		}
		if next.AppliedPrefix() != nextPrefix ||
			next.Digest() != plan.Digest() {
			t.Fatalf(
				"real plan after prefix %d has prefix=%d digest=%s want=%s",
				nextPrefix,
				next.AppliedPrefix(),
				next.Digest(),
				plan.Digest(),
			)
		}
		resumed = next
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(ctx, resumed); err != nil {
		t.Fatalf("apply real PostgreSQL evolution plan: %v", err)
	}
	completed, err := adapter.PreflightTargetSchemaEvolution(ctx, request)
	if err != nil {
		t.Fatalf("re-preflight real PostgreSQL evolution plan: %v", err)
	}
	if !completed.Complete() ||
		completed.Digest() != plan.Digest() ||
		len(completed.PendingOperations()) != 0 {
		t.Fatalf(
			"completed real plan complete=%v digest=%s want=%s pending=%#v",
			completed.Complete(),
			completed.Digest(),
			plan.Digest(),
			completed.PendingOperations(),
		)
	}
	actual, err := adapter.ReadTargetSchemaEvolutionCatalog(ctx)
	if err != nil {
		t.Fatalf("read completed real PostgreSQL catalog: %v", err)
	}
	equal, err := equalCanonicalTargetSchemaEvolutionCatalog(
		projection.CurrentTables(),
		actual.Tables(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !equal {
		t.Fatalf(
			"completed real PostgreSQL catalog = %#v, want %#v",
			actual.Tables(),
			projection.CurrentTables(),
		)
	}

	// A PostgreSQL source can legally reuse an index name in a different
	// source schema. Both schemas collapse into this one target namespace, so a
	// later lexically earlier table must receive an alternate name without
	// renaming the retained index that already exists on the target.
	added := schema.Table{
		Schema: "other_source",
		Name:   "aardvark",
		Columns: []schema.Column{
			{
				Name: "id", Type: "bigint",
				PrimaryKey: true, PrimaryKeyPosition: 1,
			},
			{Name: "code", Type: "text"},
		},
		Indexes: []schema.Index{{
			Name: "parents_code_key",
			Columns: []schema.IndexColumn{{
				Name: "code", Collation: "BINARY",
			}},
		}},
	}
	nextSourceTables := append(
		cloneTargetSchemaEvolutionTables(sourceTables),
		added,
	)
	nextGate := stage4TargetSchemaProjectionGate(
		t,
		sourceTables,
		nextSourceTables,
		"upsert",
		false,
	)
	nextAuthority := stage4TargetSchemaProjectionAuthorityFromCatalog(
		t,
		nextGate,
		"postgres",
		"postgres",
		"upsert",
		actual,
	)
	nextProjection, err := BuildStage4TargetSchemaEvolutionProjection(
		nextGate,
		nextAuthority,
		"postgres",
		adapter,
		"upsert",
	)
	if err != nil {
		t.Fatal(err)
	}
	retainedParents := stage4TargetSchemaProjectionFindTable(
		t,
		nextProjection.CurrentTables(),
		"parents",
	)
	addedAardvark := stage4TargetSchemaProjectionFindTable(
		t,
		nextProjection.CurrentTables(),
		"aardvark",
	)
	if retainedParents.Indexes[0].Name != "parents_code_key" ||
		addedAardvark.Indexes[0].Name == "" ||
		addedAardvark.Indexes[0].Name == "parents_code_key" {
		t.Fatalf(
			"retained/new live index names = %q/%q",
			retainedParents.Indexes[0].Name,
			addedAardvark.Indexes[0].Name,
		)
	}
	nextRequest, err := NewTargetSchemaEvolutionRequest(
		schema.Postgres,
		nextProjection,
		adapter.TargetSchemaEvolutionCreatePlanner(),
	)
	if err != nil {
		t.Fatal(err)
	}
	nextPlan, err := adapter.PreflightTargetSchemaEvolution(
		ctx,
		nextRequest,
	)
	if err != nil {
		t.Fatalf("preflight live retained-name table add: %v", err)
	}
	if nextPlan.Complete() || nextPlan.OperationCount() < 2 {
		t.Fatalf(
			"live retained-name add complete=%v operations=%d",
			nextPlan.Complete(),
			nextPlan.OperationCount(),
		)
	}
	if err := adapter.ApplyTargetSchemaEvolutionPlan(
		ctx,
		nextPlan,
	); err != nil {
		t.Fatalf("apply live retained-name table add: %v", err)
	}
	nextComplete, err := adapter.PreflightTargetSchemaEvolution(
		ctx,
		nextRequest,
	)
	if err != nil {
		t.Fatalf("re-preflight live retained-name table add: %v", err)
	}
	if !nextComplete.Complete() ||
		nextComplete.Digest() != nextPlan.Digest() {
		t.Fatalf(
			"live retained-name add complete=%v digest=%s want=%s",
			nextComplete.Complete(),
			nextComplete.Digest(),
			nextPlan.Digest(),
		)
	}
}

func assertPostgresEvolutionLockTimeout(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	statement string,
) {
	t.Helper()
	transaction, err := database.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin competing PostgreSQL DDL: %v", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(
		ctx,
		`SET LOCAL lock_timeout = '250ms'`,
	); err != nil {
		t.Fatalf("set competing PostgreSQL lock timeout: %v", err)
	}
	_, err = transaction.ExecContext(ctx, statement)
	if err == nil {
		t.Fatalf("competing PostgreSQL DDL was not fenced: %s", statement)
	}
	var postgresError *pgconn.PgError
	if !strings.Contains(err.Error(), "lock timeout") &&
		(!errors.As(err, &postgresError) ||
			postgresError.Code != "55P03") {
		t.Fatalf("competing PostgreSQL DDL error = %v", err)
	}
}
