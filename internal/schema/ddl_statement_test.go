package schema

import (
	"strings"
	"testing"
)

func TestDDLStatementIsRendererOwnedAndDialectBound(t *testing.T) {
	t.Parallel()

	table := Table{
		Schema: "public",
		Name:   "items",
		Columns: []Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	statement, err := CreateTableDDL(Postgres, table)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := RenderDDLStatement(statement, Postgres)
	if err != nil {
		t.Fatal(err)
	}
	want, err := CreateTable(Postgres, table)
	if err != nil {
		t.Fatal(err)
	}
	if rendered != want {
		t.Fatalf("opaque create SQL = %q, want %q", rendered, want)
	}
	if _, err := RenderDDLStatement(statement, SQLServer); err == nil {
		t.Fatal("cross-dialect opaque statement reuse was admitted")
	}
	if _, err := RenderDDLStatement(DDLStatement{}, Postgres); err == nil {
		t.Fatal("zero opaque statement was admitted")
	}
}

func TestPostgresObjectDDLAcceptsOnlyCompletePlannerObjects(t *testing.T) {
	t.Parallel()

	objects, err := PlanPostgresDropRecreateObjects(
		[]Table{{
			Schema: "public",
			Name:   "items",
			Columns: []Column{
				{
					Name:               "id",
					Type:               "bigint",
					PrimaryKey:         true,
					PrimaryKeyPosition: 1,
				},
				{Name: "label", Type: "text"},
			},
			Indexes: []Index{{
				Name: "items_label_idx",
				Columns: []IndexColumn{{
					Name: "label",
				}},
			}},
		}},
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("planned objects = %d, want 1", len(objects))
	}
	statement, err := PostgresObjectDDL(objects[0])
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := RenderDDLStatement(statement, Postgres)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rendered, "CREATE INDEX ") {
		t.Fatalf("opaque PostgreSQL object SQL = %q", rendered)
	}
	if _, err := PostgresObjectDDL(PostgresObjectStatement{}); err == nil {
		t.Fatal("zero PostgreSQL object statement was admitted")
	}
	fabricated := objects[0]
	fabricated.sql = `DROP TABLE "public"."items"; ` +
		`CREATE INDEX "items_label_idx" ON "public"."items" ("label");`
	if _, err := PostgresObjectDDL(fabricated); err == nil {
		t.Fatal("fabricated destructive multi-statement SQL was admitted")
	}
}

func TestMySQLPlannedObjectDDLAcceptsOnlyCompletePlannerObjects(t *testing.T) {
	t.Parallel()
	tables := []Table{{
		Schema: "tenant",
		Name:   "items",
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "label", Type: "varchar", DeclaredType: &DeclaredType{Base: "varchar", Arguments: []int{16}}},
		},
		Indexes: []Index{{
			Name: "items_label_idx", Columns: []IndexColumn{{Name: "label"}},
		}},
	}}
	objects, err := PlanMySQLDropRecreateObjects(tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("planned MySQL objects = %d, want 1", len(objects))
	}
	statement, err := MySQLPlannedObjectDDL(tables, objects[0])
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := RenderDDLStatement(statement, MySQL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rendered, "CREATE INDEX `items_label_idx`") {
		t.Fatalf("opaque MySQL object SQL = %q", rendered)
	}
	fabricated := objects[0]
	fabricated.SQL = "DROP TABLE `tenant`.`items`;"
	if _, err := MySQLPlannedObjectDDL(tables, fabricated); err == nil {
		t.Fatal("fabricated destructive MySQL object SQL was admitted")
	}
}

func TestSQLServerPlannedObjectDDLAcceptsOnlyCompletePlannerObjects(
	t *testing.T,
) {
	t.Parallel()
	tables := []Table{{
		Schema: "dbo",
		Name:   "items",
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name: "label", Type: "text",
				DeclaredType: &DeclaredType{Base: "varchar", Arguments: []int{16}},
			},
		},
		Indexes: []Index{{
			Name: "items_label_idx", Columns: []IndexColumn{{Name: "label"}},
		}},
	}}
	objects, err := PlanSQLServerDropRecreateObjects(tables)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("planned SQL Server objects = %d, want 1", len(objects))
	}
	statement, err := SQLServerPlannedObjectDDL(tables, objects[0])
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := RenderDDLStatement(statement, SQLServer)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(rendered, "CREATE NONCLUSTERED INDEX [items_label_idx]") {
		t.Fatalf("opaque SQL Server object SQL = %q", rendered)
	}
	fabricated := objects[0]
	fabricated.SQL = "DROP TABLE [dbo].[items];"
	if _, err := SQLServerPlannedObjectDDL(tables, fabricated); err == nil {
		t.Fatal("fabricated destructive SQL Server object SQL was admitted")
	}
}
