package migrate

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerTargetEndpointValidationDoesNotResolveSecrets(t *testing.T) {
	endpoint := config.Endpoint{
		Host:     "sql.example",
		Database: "target",
		User:     "dmtx",
		Password: "${DMTX_TEST_MISSING_SECRET}",
		Schema:   "dbo",
	}
	if err := validateSQLServerTargetEndpoint(endpoint); err != nil {
		t.Fatalf("validate SQL Server target endpoint: %v", err)
	}
	endpoint.Schema = "other"
	if err := validateSQLServerTargetEndpoint(endpoint); err != nil {
		t.Fatalf("validate configured SQL Server target schema: %v", err)
	}
}

func TestProjectSQLServerTargetTableClonesSameEngineMetadata(t *testing.T) {
	defaultValue := "((0.00))"
	parsedDefault, err := schema.ParseSQLServerCatalogDefault(
		schema.Column{
			Name: "amount",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{12, 2},
			},
		},
		&defaultValue,
	)
	if err != nil {
		t.Fatal(err)
	}
	check, err := schema.ParseSQLServerCatalogCheck(
		"[amount] >= (0)",
		[]schema.Column{{
			Name: "amount",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{12, 2},
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(41)
	source := schema.Table{
		Schema: "dbo",
		Name:   "payments",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name:    "amount",
				Type:    "numeric",
				Default: parsedDefault,
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 2},
				},
			},
		},
		Indexes: []schema.Index{{
			Name:   "payments_amount_uq",
			Unique: true,
			Columns: []schema.IndexColumn{{
				Name:       "amount",
				Descending: true,
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "payments_amount_ck",
			Expression: check,
		}},
	}
	before := cloneSQLServerTargetTable(source)
	projected, err := projectSQLServerTargetTable("mssql", source)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(projected, source) {
		t.Fatalf("same-engine projection = %#v, want %#v", projected, source)
	}
	projected.Columns[1].DeclaredType.Arguments[0] = 9
	projected.Indexes[0].Columns[0].Name = "changed"
	*projected.Identity.Frontier = 7
	if !reflect.DeepEqual(source, before) {
		t.Fatal("same-engine SQL Server projection mutated source metadata")
	}
}

func TestProjectPostgresTableForSQLServerMapsConservativeShape(t *testing.T) {
	stringDefault, err := schema.ParseSQLiteDefault("'guest'")
	if err != nil {
		t.Fatal(err)
	}
	numericDefault, err := schema.ParseSQLiteDefault("0.00")
	if err != nil {
		t.Fatal(err)
	}
	check, err := schema.ParseSQLiteCheckExpression("balance >= 0")
	if err != nil {
		t.Fatal(err)
	}
	source := schema.Table{
		Schema: "public",
		Name:   "accounts",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:    "code",
				Type:    "varchar",
				Default: stringDefault,
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
			},
			{
				Name:    "balance",
				Type:    "numeric",
				Default: numericDefault,
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
			},
			{Name: "enabled", Type: "boolean"},
			{Name: "payload", Type: "bytea", Nullable: true},
			{
				Name: "created_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
		},
		Indexes: []schema.Index{{
			Name:   "accounts_id_uq",
			Unique: true,
			Columns: []schema.IndexColumn{{
				Name: "id",
			}},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "accounts_balance_ck",
			Expression: check,
		}},
	}
	projected, err := projectPostgresTableForSQLServer(source)
	if err != nil {
		t.Fatal(err)
	}
	wantDeclarations := []schema.DeclaredType{
		{Base: "bigint"},
		{Base: "varchar", Arguments: []int{96}},
		{Base: "decimal", Arguments: []int{12, 2}},
		{Base: "bool"},
		{Base: "blob"},
		{Base: "timestamp", Arguments: []int{3}},
	}
	for index, want := range wantDeclarations {
		if projected.Columns[index].DeclaredType == nil ||
			!reflect.DeepEqual(
				*projected.Columns[index].DeclaredType,
				want,
			) {
			t.Fatalf(
				"projected column %s declaration = %#v, want %#v",
				projected.Columns[index].Name,
				projected.Columns[index].DeclaredType,
				want,
			)
		}
	}
	if projected.Schema != source.Schema ||
		projected.Columns[1].Default == source.Columns[1].Default {
		t.Fatal("PostgreSQL projection lost identity or aliased defaults")
	}
}

func TestProjectPostgresTableForSQLServerFailsClosed(t *testing.T) {
	clock, err := schema.ParseSQLiteDefault("CURRENT_TIMESTAMP")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		table  schema.Table
		needle string
	}{
		{
			name: "clock default",
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{{
					Name:    "created_at",
					Type:    "timestamp",
					Default: clock,
				}},
			},
			needle: "clock default",
		},
		{
			name: "fixed char",
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{{
					Name: "code",
					Type: "char",
					DeclaredType: &schema.DeclaredType{
						Base:      "char",
						Arguments: []int{10},
					},
				}},
			},
			needle: "fixed-width",
		},
		{
			name: "nullable unique",
			table: schema.Table{
				Name: "events",
				Columns: []schema.Column{{
					Name:     "code",
					Type:     "integer",
					Nullable: true,
				}},
				Indexes: []schema.Index{{
					Name:   "events_code_uq",
					Unique: true,
					Columns: []schema.IndexColumn{{
						Name: "code",
					}},
				}},
			},
			needle: "nullable unique",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := projectPostgresTableForSQLServer(test.table)
			var policy *schema.PolicyError
			if err == nil ||
				!errors.As(err, &policy) ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf("projection error = %v", err)
			}
		})
	}
}

func TestValidateSQLServerRetainedTableShapeIgnoresOnlyOrderAndFrontier(
	t *testing.T,
) {
	first := int64(7)
	second := int64(41)
	planned := schema.Table{
		Schema: "dbo",
		Name:   "accounts",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &first,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
		Indexes: []schema.Index{
			{Name: "z", Columns: []schema.IndexColumn{{Name: "id"}}},
			{Name: "a", Columns: []schema.IndexColumn{{Name: "id"}}},
		},
	}
	actual := cloneSQLServerTargetTable(planned)
	actual.Identity.Frontier = &second
	actual.Indexes[0], actual.Indexes[1] =
		actual.Indexes[1], actual.Indexes[0]
	if err := validateSQLServerRetainedTableShape(
		planned,
		actual,
	); err != nil {
		t.Fatalf("equivalent retained shape: %v", err)
	}
	actual.Columns[0].Nullable = true
	if err := validateSQLServerRetainedTableShape(
		planned,
		actual,
	); err == nil {
		t.Fatal("retained nullability drift was accepted")
	}
}

func TestValidateSQLServerRetainedTableShapeCanonicalizesChecks(
	t *testing.T,
) {
	plannedExpression, err := schema.ParseSQLiteCheckExpression(
		"balance >= 0",
	)
	if err != nil {
		t.Fatal(err)
	}
	actualExpression, err := schema.ParseSQLiteCheckExpression(
		`"balance" >= 0`,
	)
	if err != nil {
		t.Fatal(err)
	}
	planned := schema.Table{
		Schema: "dbo",
		Name:   "accounts",
		Columns: []schema.Column{{
			Name: "balance",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{12, 2},
			},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "accounts_balance_ck",
			Expression: plannedExpression,
		}},
	}
	actual := cloneSQLServerTargetTable(planned)
	actual.Checks[0].Expression = actualExpression
	if err := validateSQLServerRetainedTableShape(
		planned,
		actual,
	); err != nil {
		t.Fatalf("canonical-equivalent retained CHECK: %v", err)
	}

	actual.Checks[0].Expression, err =
		schema.ParseSQLiteCheckExpression("balance > 0")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateSQLServerRetainedTableShape(
		planned,
		actual,
	); err == nil {
		t.Fatal("retained CHECK drift was accepted")
	}
}

func TestPreflightSQLServerUpsertRejectsMutableReferencedUniqueKey(
	t *testing.T,
) {
	parent := schema.Table{
		Name: "accounts",
		Columns: []schema.Column{
			{
				Name:               "id",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "external_id"},
		},
	}
	child := schema.Table{
		Name: "events",
		ForeignKeys: []schema.ForeignKey{{
			Name:              "events_account_fk",
			ReferencedTable:   "accounts",
			ReferencedColumns: []string{"external_id"},
		}},
	}
	err := preflightSQLServerUpsertForeignKeyKeys(
		[]schema.Table{parent, child},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"mutable non-primary unique key",
	) {
		t.Fatalf("non-primary reference error = %v", err)
	}

	child.ForeignKeys[0].ReferencedColumns = []string{"id"}
	if err := preflightSQLServerUpsertForeignKeyKeys(
		[]schema.Table{parent, child},
	); err != nil {
		t.Fatalf("primary-key reference: %v", err)
	}
}

func TestValidateSQLServerTargetPermissionsFailsClosed(t *testing.T) {
	tests := []struct {
		name                      string
		permissions               sqlServerTargetPermissionCatalog
		mode                      string
		requiresIdentityAuthority bool
		wantError                 bool
	}{
		{
			name: "drop recreate complete",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition: true,
				schemaControl:  true,
				createTable:    true,
				schemaAlter:    true,
				schemaSelect:   true,
				schemaInsert:   true,
			},
			mode: "drop_recreate",
		},
		{
			name: "upsert needs no DDL grant",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition: true,
				schemaControl:  true,
			},
			mode: "upsert",
		},
		{
			name: "identity authority complete",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition:    true,
				schemaControl:     true,
				identityAuthority: true,
			},
			mode:                      "upsert",
			requiresIdentityAuthority: true,
		},
		{
			name: "identity authority missing",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition: true,
				schemaControl:  true,
			},
			mode:                      "upsert",
			requiresIdentityAuthority: true,
			wantError:                 true,
		},
		{
			name: "metadata hidden",
			permissions: sqlServerTargetPermissionCatalog{
				schemaControl: true,
			},
			mode:      "upsert",
			wantError: true,
		},
		{
			name: "schema not controlled",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition: true,
			},
			mode:      "upsert",
			wantError: true,
		},
		{
			name: "drop lacks create",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition: true,
				schemaControl:  true,
				schemaAlter:    true,
				schemaSelect:   true,
				schemaInsert:   true,
			},
			mode:      "drop_recreate",
			wantError: true,
		},
		{
			name: "drop lacks effective insert",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition: true,
				schemaControl:  true,
				createTable:    true,
				schemaAlter:    true,
				schemaSelect:   true,
			},
			mode:      "drop_recreate",
			wantError: true,
		},
		{
			name: "drop identity complete",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition:    true,
				schemaControl:     true,
				createTable:       true,
				schemaAlter:       true,
				schemaSelect:      true,
				schemaInsert:      true,
				schemaDelete:      true,
				identityAuthority: true,
			},
			mode:                      "drop_recreate",
			requiresIdentityAuthority: true,
		},
		{
			name: "drop identity lacks delete",
			permissions: sqlServerTargetPermissionCatalog{
				viewDefinition:    true,
				schemaControl:     true,
				createTable:       true,
				schemaAlter:       true,
				schemaSelect:      true,
				schemaInsert:      true,
				identityAuthority: true,
			},
			mode:                      "drop_recreate",
			requiresIdentityAuthority: true,
			wantError:                 true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSQLServerTargetPermissions(
				test.permissions,
				test.mode,
				test.requiresIdentityAuthority,
			)
			if (err != nil) != test.wantError {
				t.Fatalf("permission validation error = %v", err)
			}
		})
	}
}

func TestSQLServerTargetPlanHasIdentity(t *testing.T) {
	if sqlServerTargetPlanHasIdentity([]schema.Table{{Name: "plain"}}) {
		t.Fatal("plain table plan requires identity authority")
	}
	if !sqlServerTargetPlanHasIdentity([]schema.Table{{
		Name:     "identified",
		Identity: &schema.Identity{Column: "id"},
	}}) {
		t.Fatal("identity table plan does not require identity authority")
	}
}

func TestValidateSQLServerTargetObjectPermissionsFailsClosed(
	t *testing.T,
) {
	table := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			{
				Name:               "id",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload"},
		},
	}
	complete := sqlServerTargetObjectPermissionCatalog{
		selectRows: true,
		insertRows: true,
		updateRows: true,
		alterTable: true,
	}
	if err := validateSQLServerTargetObjectPermissions(
		table,
		"upsert",
		complete,
	); err != nil {
		t.Fatalf("complete upsert permissions: %v", err)
	}
	missingUpdate := complete
	missingUpdate.updateRows = false
	if err := validateSQLServerTargetObjectPermissions(
		table,
		"upsert",
		missingUpdate,
	); err == nil || !strings.Contains(err.Error(), "UPDATE") {
		t.Fatalf("missing upsert UPDATE permission error = %v", err)
	}
	primaryKeyOnly := table
	primaryKeyOnly.Columns = primaryKeyOnly.Columns[:1]
	if err := validateSQLServerTargetObjectPermissions(
		primaryKeyOnly,
		"upsert",
		missingUpdate,
	); err != nil {
		t.Fatalf("primary-key-only upsert permissions: %v", err)
	}
	identity := table
	identity.Identity = &schema.Identity{Column: "id"}
	missingAlter := complete
	missingAlter.alterTable = false
	if err := validateSQLServerTargetObjectPermissions(
		identity,
		"upsert",
		missingAlter,
	); err == nil || !strings.Contains(err.Error(), "ALTER") {
		t.Fatalf("missing identity ALTER permission error = %v", err)
	}
	if err := validateSQLServerTargetObjectPermissions(
		table,
		"drop_recreate",
		missingAlter,
	); err == nil || !strings.Contains(err.Error(), "ALTER") {
		t.Fatalf("missing drop ALTER permission error = %v", err)
	}
}

func TestValidateSQLServerTargetTableCatalogFailsClosed(t *testing.T) {
	base := sqlServerTargetTableCatalog{
		name:                     "events",
		objectID:                 42,
		objectType:               "U",
		typeDescription:          "USER_TABLE",
		durability:               "SCHEMA_AND_DATA",
		baseIndexCount:           1,
		baseIndexType:            1,
		baseIndexTypeDescription: "CLUSTERED",
		baseIndexDataSpaceType:   "FG",
		maxPartition:             1,
	}
	if err := validateSQLServerTargetTableCatalog(
		"events",
		base,
	); err != nil {
		t.Fatalf("valid ordinary clustered table catalog: %v", err)
	}
	heap := base
	heap.baseIndexType = 0
	heap.baseIndexTypeDescription = "HEAP"
	if err := validateSQLServerTargetTableCatalog(
		"events",
		heap,
	); err != nil {
		t.Fatalf("valid ordinary heap catalog: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*sqlServerTargetTableCatalog)
		needle string
	}{
		{
			name: "catalog shape",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.typeDescription = "secret-unmodeled-shape"
			},
			needle: "catalog shape",
		},
		{
			name: "temporal",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.temporalType = 2
			},
			needle: "temporal-table",
		},
		{
			name: "memory optimized",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.memoryOptimized = true
			},
			needle: "durability",
		},
		{
			name: "FILESTREAM",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.fileStreamDataSpaceID = 7
			},
			needle: "FILESTREAM",
		},
		{
			name: "replication",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.replicationFilter = true
			},
			needle: "replication",
		},
		{
			name: "lock on bulk load",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.lockOnBulkLoad = true
			},
			needle: "table lock on bulk load",
		},
		{
			name: "large value option",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.largeValuesOutOfRow = true
			},
			needle: "large-value",
		},
		{
			name: "base columnstore",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.baseIndexType = 5
				value.baseIndexTypeDescription =
					"CLUSTERED COLUMNSTORE"
			},
			needle: "non-rowstore base",
		},
		{
			name: "partitioned base",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.maxPartition = 2
			},
			needle: "partitioned",
		},
		{
			name: "partitioned secondary index",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.partitionSchemeCount = 1
			},
			needle: "partitioned",
		},
		{
			name: "compressed partition",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.compressedPartitionCount = 1
			},
			needle: "compressed",
		},
		{
			name: "non-rowstore secondary index",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.nonRowstoreIndexCount = 1
			},
			needle: "non-rowstore index",
		},
		{
			name: "included index column",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.includedIndexColumnCount = 1
			},
			needle: "unmodeled index options",
		},
		{
			name: "unmodeled column feature",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.unmodeledColumnFeatureCount = 1
			},
			needle: "unmodeled column features",
		},
		{
			name: "trigger",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.triggerCount = 1
			},
			needle: "DML triggers",
		},
		{
			name: "row security",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.securityPredicateCount = 1
			},
			needle: "row-level security",
		},
		{
			name: "full text",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.fullTextIndexCount = 1
			},
			needle: "full-text",
		},
		{
			name: "change tracking",
			mutate: func(value *sqlServerTargetTableCatalog) {
				value.changeTrackingCount = 1
			},
			needle: "change tracking",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := base
			test.mutate(&value)
			err := validateSQLServerTargetTableCatalog(
				"events",
				value,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf("catalog validation error = %v", err)
			}
			if strings.Contains(
				err.Error(),
				"secret-unmodeled-shape",
			) {
				t.Fatalf("catalog error exposes raw catalog value: %v", err)
			}
		})
	}
}

func TestValidateSQLServerForeignKeyDependencyRejectsExternalEdges(
	t *testing.T,
) {
	selected := map[string]schema.Table{
		"accounts": {Schema: "dbo", Name: "accounts"},
		"events":   {Schema: "dbo", Name: "events"},
	}
	tests := []struct {
		name       string
		dependency sqlServerForeignKeyDependency
		needle     string
	}{
		{
			name: "external inbound",
			dependency: sqlServerForeignKeyDependency{
				parentSchema:     "audit",
				parentTable:      "archive",
				constraint:       "archive_account_fk",
				referencedSchema: "dbo",
				referencedTable:  "accounts",
			},
			needle: "external table",
		},
		{
			name: "same schema unselected inbound",
			dependency: sqlServerForeignKeyDependency{
				parentSchema:     "dbo",
				parentTable:      "archive",
				constraint:       "archive_account_fk",
				referencedSchema: "dbo",
				referencedTable:  "accounts",
			},
			needle: "external table",
		},
		{
			name: "external outgoing",
			dependency: sqlServerForeignKeyDependency{
				parentSchema:     "dbo",
				parentTable:      "events",
				constraint:       "events_archive_fk",
				referencedSchema: "audit",
				referencedTable:  "archive",
			},
			needle: "references unselected table",
		},
		{
			name: "same schema unselected outgoing",
			dependency: sqlServerForeignKeyDependency{
				parentSchema:     "dbo",
				parentTable:      "events",
				constraint:       "events_archive_fk",
				referencedSchema: "dbo",
				referencedTable:  "archive",
			},
			needle: "references unselected table",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateSQLServerForeignKeyDependency(
				"dbo",
				selected,
				test.dependency,
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf("foreign-key dependency error = %v", err)
			}
		})
	}
	internal := sqlServerForeignKeyDependency{
		parentSchema:     "dbo",
		parentTable:      "events",
		constraint:       "events_account_fk",
		referencedSchema: "dbo",
		referencedTable:  "accounts",
	}
	if err := validateSQLServerForeignKeyDependency(
		"dbo",
		selected,
		internal,
	); err != nil {
		t.Fatalf("selected internal foreign key: %v", err)
	}
}

func TestSQLServerExpressionDependencyErrorMarksNameBasedProof(
	t *testing.T,
) {
	err := sqlServerExpressionDependencyError(
		"accounts",
		"dbo",
		"read_accounts",
		"SQL_STORED_PROCEDURE",
		true,
	)
	if !strings.Contains(err.Error(), "name-based") ||
		!strings.Contains(err.Error(), "read_accounts") {
		t.Fatalf("name-based dependency error = %v", err)
	}
}

func TestValidateSQLServerServerTriggerVisibilityFailsClosed(
	t *testing.T,
) {
	if err := validateSQLServerServerTriggerVisibility(true); err != nil {
		t.Fatalf("complete server-trigger visibility: %v", err)
	}
	err := validateSQLServerServerTriggerVisibility(false)
	if err == nil ||
		!strings.Contains(err.Error(), "VIEW ANY DEFINITION") {
		t.Fatalf("incomplete server-trigger visibility error = %v", err)
	}
}
