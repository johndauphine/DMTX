package migrate

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLTargetEndpointValidationDoesNotResolveSecrets(t *testing.T) {
	endpoint := config.Endpoint{
		Host:     "database.example",
		Database: "target",
		User:     "migrator",
		Password: "${file:/path/that/does/not/exist}",
	}
	if err := validateMySQLTargetEndpoint(endpoint); err != nil {
		t.Fatalf("validateMySQLTargetEndpoint: %v", err)
	}

	endpoint.Schema = "other"
	err := validateMySQLTargetEndpoint(endpoint)
	if err == nil || !strings.Contains(err.Error(), "must be empty or match") {
		t.Fatalf("schema mismatch error = %v", err)
	}

	endpoint.Schema = ""
	endpoint.Host = ""
	err = validateMySQLTargetEndpoint(endpoint)
	if err == nil ||
		err.Error() != "MySQL host, database, and user are required" {
		t.Fatalf("missing-host error = %v", err)
	}
}

func TestMySQLTargetEndpointRejectsSystemDatabases(t *testing.T) {
	for _, database := range []string{
		"information_schema",
		"MYSQL",
		"Performance_Schema",
		"sys",
	} {
		t.Run(database, func(t *testing.T) {
			err := validateMySQLTargetEndpoint(config.Endpoint{
				Host:     "database.example",
				Database: database,
				User:     "migrator",
			})
			if err == nil ||
				!strings.Contains(err.Error(), "reserved system database") {
				t.Fatalf("system database error = %v", err)
			}
		})
	}
}

func TestMariaDBBuiltInSystemViewRecognition(t *testing.T) {
	for _, testCase := range []struct {
		schema  string
		definer string
		want    bool
	}{
		{schema: "mysql", definer: "mariadb.sys@localhost", want: true},
		{schema: "sys", definer: "mariadb.sys@localhost", want: true},
		{schema: "application", definer: "mariadb.sys@localhost"},
		{schema: "sys", definer: "root@localhost"},
	} {
		if got := isMariaDBBuiltInSystemView(
			testCase.schema,
			testCase.definer,
		); got != testCase.want {
			t.Fatalf(
				"isMariaDBBuiltInSystemView(%q, %q) = %t, want %t",
				testCase.schema,
				testCase.definer,
				got,
				testCase.want,
			)
		}
	}
}

func TestMariaDBViewVisibilityFailsClosed(t *testing.T) {
	if err := validateMariaDBGlobalViewVisibility(false); err == nil ||
		!strings.Contains(err.Error(), "global SHOW VIEW") {
		t.Fatalf("missing SHOW VIEW error = %v", err)
	}
	if err := validateMariaDBGlobalViewVisibility(true); err != nil {
		t.Fatalf("global SHOW VIEW was rejected: %v", err)
	}

	visible, err := validateMariaDBViewDefinition(
		"application",
		"hidden_view",
		"",
		"dmtx@%",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "has no visible definition") ||
		visible {
		t.Fatalf(
			"hidden application view result = %t, error = %v",
			visible,
			err,
		)
	}
	visible, err = validateMariaDBViewDefinition(
		"sys",
		"host_summary",
		"",
		"mariadb.sys@localhost",
	)
	if err != nil || visible {
		t.Fatalf(
			"hidden built-in view result = %t, error = %v",
			visible,
			err,
		)
	}
	visible, err = validateMariaDBViewDefinition(
		"application",
		"visible_view",
		"select 1 AS `one`",
		"dmtx@%",
	)
	if err != nil || !visible {
		t.Fatalf(
			"visible application view result = %t, error = %v",
			visible,
			err,
		)
	}
}

func TestMySQLTargetPlansWithoutMutatingMySQLSource(t *testing.T) {
	frontier := int64(41)
	source := schema.Table{
		Schema:         "source_db",
		Name:           "events",
		MySQLCollation: "utf8mb4_0900_bin",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			Nullable:           false,
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
	}
	before := cloneMySQLTargetTable(source)
	adapter := &mysqlTargetAdapter{namespace: "target_db"}
	adapter.flavor = engine.MySQLServerFlavorOracle80
	planned, err := adapter.PlanTables(
		"mysql",
		[]schema.Table{source},
		"drop_recreate",
	)
	if err != nil {
		t.Fatalf("PlanTables: %v", err)
	}
	if len(planned) != 1 ||
		planned[0].Schema != "target_db" ||
		planned[0].Name != "events" {
		t.Fatalf("planned tables = %#v", planned)
	}
	if !reflect.DeepEqual(source, before) {
		t.Fatalf("source table was mutated:\n%#v\nwant\n%#v", source, before)
	}
	if adapter.Engine() != "mysql" {
		t.Fatalf("Engine() = %q, want mysql", adapter.Engine())
	}
}

func TestMySQLTargetWriteBatchDelegatesToConfiguredWriter(t *testing.T) {
	writer := &mysqlTargetWriterRecorder{
		receipt: WriteReceipt{
			Certainty:     CommitDurable,
			AttemptedRows: 1,
			CommittedRows: 1,
		},
	}
	adapter := &mysqlTargetAdapter{
		batchWriter: writer,
		namespace:   "target_db",
	}
	table := mysqlNativeTestTable()
	rows := [][]any{{int64(1), "payload"}}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if writer.calls != 1 ||
		writer.table.Name != "events" ||
		writer.mode != "drop_recreate" ||
		len(writer.rows) != 1 {
		t.Fatalf("delegated write = %#v", writer)
	}
	if receipt != writer.receipt {
		t.Fatalf("receipt = %#v, want %#v", receipt, writer.receipt)
	}
}

func TestMySQLTargetWriteBatchRejectsMissingWriter(t *testing.T) {
	adapter := &mysqlTargetAdapter{namespace: "target_db"}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		mysqlNativeTestTable(),
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{{int64(1), "payload"}},
	)
	if err == nil ||
		err.Error() != "MySQL native batch writer is not configured" {
		t.Fatalf("error = %v", err)
	}
	assertMySQLNativeReceipt(t, receipt, CommitNotCommitted, 1, 0)
}

func TestMySQLIdentityFrontierChoosesHighestObservedValue(t *testing.T) {
	source := int64(41)
	frontier, err := mySQLIdentityFrontier(
		&source,
		sql.NullInt64{Int64: 52, Valid: true},
		sql.NullInt64{Int64: 51, Valid: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if frontier != 52 {
		t.Fatalf("frontier = %d, want 52", frontier)
	}

	source = -1
	if _, err := mySQLIdentityFrontier(
		&source,
		sql.NullInt64{},
		sql.NullInt64{},
	); err == nil {
		t.Fatal("negative source frontier was accepted")
	}
}

func TestValidateMySQLRetainedTableShapeReportsFirstMismatch(t *testing.T) {
	planned := mysqlNativeTestTable()
	actual := planned
	actual.Columns = append([]schema.Column(nil), planned.Columns...)
	actual.Columns[1].Nullable = !actual.Columns[1].Nullable
	err := validateMySQLRetainedTableShape(planned, actual)
	if err == nil ||
		!strings.Contains(err.Error(), "column 2 (payload)") {
		t.Fatalf("shape error = %v", err)
	}
}

func TestValidateMySQLRetainedColumnTreatsUUIDAsPhysicalVarchar36(
	t *testing.T,
) {
	planned := schema.Column{
		Name:     "external_id",
		Type:     "uuid",
		Nullable: false,
		DeclaredType: &schema.DeclaredType{
			Base:      "varchar",
			Arguments: []int{36},
		},
	}
	actual := planned
	actual.Type = "varchar"
	if err := validateMySQLRetainedColumn(planned, actual); err != nil {
		t.Fatalf("physical UUID retained column was rejected: %v", err)
	}

	actual.DeclaredType.Arguments = []int{35}
	if err := validateMySQLRetainedColumn(planned, actual); err == nil {
		t.Fatal("non-VARCHAR(36) retained UUID shape was accepted")
	}
}

func TestValidateMySQLRetainedTableShapeIgnoresObjectCatalogOrder(
	t *testing.T,
) {
	firstCheck, err := schema.ParseSQLiteCheckExpression("id > 0")
	if err != nil {
		t.Fatal(err)
	}
	secondCheck, err := schema.ParseSQLiteCheckExpression(
		"payload IS NOT NULL",
	)
	if err != nil {
		t.Fatal(err)
	}
	planned := mysqlNativeTestTable()
	planned.Indexes = []schema.Index{
		{Name: "z_index", Columns: []schema.IndexColumn{{Name: "id"}}},
		{Name: "a_index", Columns: []schema.IndexColumn{{Name: "payload"}}},
	}
	planned.Checks = []schema.CheckConstraint{
		{Name: "z_check", Expression: firstCheck},
		{Name: "a_check", Expression: secondCheck},
	}
	planned.ForeignKeys = []schema.ForeignKey{
		{
			Name:              "z_fkey",
			Columns:           []string{"id"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "CASCADE",
			OnDelete:          "RESTRICT",
			Match:             "NONE",
		},
		{
			Name:              "a_fkey",
			Columns:           []string{"payload"},
			ReferencedTable:   "parents",
			ReferencedColumns: []string{"payload"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "CASCADE",
			Match:             "NONE",
		},
	}
	actual := planned
	actual.Indexes = []schema.Index{
		planned.Indexes[1],
		planned.Indexes[0],
	}
	actual.Checks = []schema.CheckConstraint{
		planned.Checks[1],
		planned.Checks[0],
	}
	actual.ForeignKeys = []schema.ForeignKey{
		planned.ForeignKeys[1],
		planned.ForeignKeys[0],
	}
	if err := validateMySQLRetainedTableShape(
		planned,
		actual,
	); err != nil {
		t.Fatalf("reordered retained objects were rejected: %v", err)
	}
}

type mysqlTargetWriterRecorder struct {
	calls   int
	table   schema.Table
	columns []string
	mode    string
	rows    [][]any
	receipt WriteReceipt
	err     error
}

func (writer *mysqlTargetWriterRecorder) WriteBatch(
	_ context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	writer.calls++
	writer.table = table
	writer.columns = append([]string(nil), columns...)
	writer.mode = mode
	writer.rows = rows
	return writer.receipt, writer.err
}
