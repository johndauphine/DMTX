package schema

import (
	"reflect"
	"strings"
	"testing"
)

func TestQualifiedForeignKeyPlannersSelectExactSchema(t *testing.T) {
	t.Parallel()

	tables := qualifiedForeignKeyPlannerTables()
	postgres, err := PlanPostgresDropRecreateObjects(
		tables,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatalf("PostgreSQL plan: %v", err)
	}
	if len(postgres) != 1 ||
		!strings.Contains(
			postgres[0].SQL(),
			`REFERENCES "identity"."accounts"`,
		) {
		t.Fatalf("PostgreSQL qualified plan = %#v", postgres)
	}

	mysql, err := PlanMySQLDropRecreateObjects(tables)
	if err != nil {
		t.Fatalf("MySQL plan: %v", err)
	}
	if len(mysql) != 1 ||
		!strings.Contains(
			mysql[0].SQL,
			"REFERENCES `identity`.`accounts`",
		) {
		t.Fatalf("MySQL qualified plan = %#v", mysql)
	}

	sqlServer, err := PlanSQLServerDropRecreateObjects(tables)
	if err != nil {
		t.Fatalf("SQL Server plan: %v", err)
	}
	if len(sqlServer) != 1 ||
		!strings.Contains(
			sqlServer[0].SQL,
			"REFERENCES [identity].[accounts]",
		) {
		t.Fatalf("SQL Server qualified plan = %#v", sqlServer)
	}
}

func TestQualifiedForeignKeyPlannersNeverFallBackToOwnerSameName(
	t *testing.T,
) {
	t.Parallel()

	tables := qualifiedForeignKeyPlannerTables()
	tables = tables[1:]
	for name, plan := range map[string]func([]Table) error{
		"PostgreSQL": func(values []Table) error {
			_, err := PlanPostgresDropRecreateObjects(
				values,
				PostgresObjectPlanOptions{},
			)
			return err
		},
		"MySQL": func(values []Table) error {
			_, err := PlanMySQLDropRecreateObjects(values)
			return err
		},
		"SQL Server": func(values []Table) error {
			_, err := PlanSQLServerDropRecreateObjects(values)
			return err
		},
	} {
		if err := plan(tables); err == nil {
			t.Fatalf("%s silently rebound identity.accounts to sales.accounts", name)
		}
	}
}

func TestQualifiedForeignKeySchemasParticipateInPlannerIdentity(
	t *testing.T,
) {
	t.Parallel()

	left := ForeignKey{
		Columns:           []string{"account_id"},
		ReferencedSchema:  "identity",
		ReferencedTable:   "accounts",
		ReferencedColumns: []string{"id"},
	}
	right := left
	right.ReferencedSchema = "archive"
	if postgresForeignKeySortKey(left) == postgresForeignKeySortKey(right) {
		t.Fatal("PostgreSQL/MySQL foreign-key identity omitted referenced schema")
	}
	if sqlServerForeignKeySortKey(left) == sqlServerForeignKeySortKey(right) {
		t.Fatal("SQL Server foreign-key identity omitted referenced schema")
	}
}

func TestForeignKeySortKeyFramesEveryIdentityField(t *testing.T) {
	t.Parallel()

	left, right := foreignKeySortKeyCollisionPair()
	if postgresForeignKeySortKey(left) == postgresForeignKeySortKey(right) {
		t.Fatal("PostgreSQL/MySQL foreign-key identity fields collided")
	}
}

func TestForeignKeyFramingMakesPlannerAndImplicitIndexesOrderIndependent(
	t *testing.T,
) {
	t.Parallel()

	left := foreignKeySortKeyCollisionTables()
	right := foreignKeySortKeyCollisionTables()
	right[2].ForeignKeys[0], right[2].ForeignKeys[1] =
		right[2].ForeignKeys[1], right[2].ForeignKeys[0]

	leftPostgres, err := PlanPostgresDropRecreateObjects(
		left,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatalf("plan left PostgreSQL objects: %v", err)
	}
	rightPostgres, err := PlanPostgresDropRecreateObjects(
		right,
		PostgresObjectPlanOptions{},
	)
	if err != nil {
		t.Fatalf("plan right PostgreSQL objects: %v", err)
	}
	if !reflect.DeepEqual(leftPostgres, rightPostgres) {
		t.Fatalf(
			"PostgreSQL plan depends on foreign-key input order:\nleft: %#v\nright: %#v",
			leftPostgres,
			rightPostgres,
		)
	}

	leftMySQL, err := PlanMySQLDropRecreateObjects(left)
	if err != nil {
		t.Fatalf("plan left MySQL objects: %v", err)
	}
	rightMySQL, err := PlanMySQLDropRecreateObjects(right)
	if err != nil {
		t.Fatalf("plan right MySQL objects: %v", err)
	}
	if !reflect.DeepEqual(leftMySQL, rightMySQL) {
		t.Fatalf(
			"MySQL plan depends on foreign-key input order:\nleft: %#v\nright: %#v",
			leftMySQL,
			rightMySQL,
		)
	}

	leftIndexed, err := AddMySQLForeignKeyIndexes(left[2])
	if err != nil {
		t.Fatalf("add left MySQL foreign-key indexes: %v", err)
	}
	rightIndexed, err := AddMySQLForeignKeyIndexes(right[2])
	if err != nil {
		t.Fatalf("add right MySQL foreign-key indexes: %v", err)
	}
	if !reflect.DeepEqual(leftIndexed.Indexes, rightIndexed.Indexes) {
		t.Fatalf(
			"MySQL implicit indexes depend on foreign-key input order:\nleft: %#v\nright: %#v",
			leftIndexed.Indexes,
			rightIndexed.Indexes,
		)
	}
}

func TestSQLiteRejectsQualifiedForeignKeyBeforeDDL(t *testing.T) {
	t.Parallel()

	tables := qualifiedForeignKeyPlannerTables()
	child := tables[2]
	child.Schema = ""
	if _, err := CreateTable(SQLite, child); err == nil ||
		!strings.Contains(err.Error(), "qualified referenced table") {
		t.Fatalf("SQLite qualified foreign-key error = %v", err)
	}
}

func foreignKeySortKeyCollisionPair() (ForeignKey, ForeignKey) {
	return ForeignKey{
			Columns:          []string{"a"},
			ReferencedSchema: "b",
			ReferencedTable:  "p",
		}, ForeignKey{
			Columns:         []string{"a", "b"},
			ReferencedTable: "p",
		}
}

func foreignKeySortKeyCollisionTables() []Table {
	key := func(
		namespace string,
		columns ...string,
	) Table {
		result := Table{
			Schema:  namespace,
			Name:    "p",
			Columns: make([]Column, len(columns)),
		}
		for index, name := range columns {
			result.Columns[index] = Column{
				Name:               name,
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: index + 1,
				DeclaredType:       &DeclaredType{Base: "int"},
			}
		}
		return result
	}
	left, right := foreignKeySortKeyCollisionPair()
	return []Table{
		key("b", "x"),
		key("sales", "x", "y"),
		{
			Schema: "sales",
			Name:   "events",
			Columns: []Column{
				{
					Name:         "a",
					Type:         "integer",
					DeclaredType: &DeclaredType{Base: "int"},
				},
				{
					Name:         "b",
					Type:         "integer",
					DeclaredType: &DeclaredType{Base: "int"},
				},
			},
			ForeignKeys: []ForeignKey{left, right},
		},
	}
}

func qualifiedForeignKeyPlannerTables() []Table {
	keyTable := func(namespace string) Table {
		return Table{
			Schema: namespace,
			Name:   "accounts",
			Columns: []Column{{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "int"},
			}},
		}
	}
	return []Table{
		keyTable("identity"),
		keyTable("sales"),
		{
			Schema: "sales",
			Name:   "events",
			Columns: []Column{{
				Name:         "account_id",
				Type:         "integer",
				DeclaredType: &DeclaredType{Base: "int"},
			}},
			ForeignKeys: []ForeignKey{{
				Columns:           []string{"account_id"},
				ReferencedSchema:  "identity",
				ReferencedTable:   "accounts",
				ReferencedColumns: []string{"id"},
				OnUpdate:          "NO ACTION",
				OnDelete:          "NO ACTION",
				Match:             "SIMPLE",
			}},
		},
	}
}
