package schema

import (
	"errors"
	"strconv"
	"strings"
	"testing"
)

func mustMySQLTestDefault(t *testing.T, value string) *Expression {
	t.Helper()
	expression, err := ParseSQLiteDefault(value)
	if err != nil {
		t.Fatal(err)
	}
	return expression
}

func TestCreateMySQLTableRendersExactTypesDefaultsAndIdentity(t *testing.T) {
	frontier := int64(41)
	table := Table{
		Schema: "target",
		Name:   "accounts",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name: "code",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{24},
				},
				Default: mustMySQLTestDefault(t, "'guest'"),
			},
			{
				Name: "balance",
				Type: "numeric",
				DeclaredType: &DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 2},
				},
				Default: mustMySQLTestDefault(t, "0.00"),
			},
			{
				Name: "enabled",
				Type: "integer",
				DeclaredType: &DeclaredType{
					Base:      "tinyint",
					Arguments: []int{1},
				},
				Default: mustMySQLTestDefault(t, "1"),
			},
			{
				Name:     "payload",
				Type:     "blob",
				Nullable: true,
				DeclaredType: &DeclaredType{
					Base: "longblob",
				},
				Default: mustMySQLTestDefault(t, "X'00FF'"),
			},
			{
				Name: "created_at",
				Type: "datetime",
				DeclaredType: &DeclaredType{
					Base:      "datetime",
					Arguments: []int{3},
				},
				Default: mustMySQLTestDefault(t, "CURRENT_TIMESTAMP"),
			},
			{
				Name:         "document",
				Type:         "json",
				Nullable:     true,
				DeclaredType: &DeclaredType{Base: "json"},
			},
		},
	}

	got, err := CreateTable(MySQL, table)
	if err != nil {
		t.Fatal(err)
	}
	const want = "CREATE TABLE `target`.`accounts` (" +
		"`id` BIGINT AUTO_INCREMENT NOT NULL, " +
		"`code` VARCHAR(24) NOT NULL DEFAULT 'guest', " +
		"`balance` DECIMAL(12,2) NOT NULL DEFAULT 0.00, " +
		"`enabled` TINYINT(1) NOT NULL DEFAULT 1, " +
		"`payload` LONGBLOB DEFAULT (X'00ff'), " +
		"`created_at` DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), " +
		"`document` JSON, PRIMARY KEY (`id`)) " +
		"ENGINE=InnoDB DEFAULT CHARACTER SET=utf8mb4 " +
		"COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC;"
	if got != want {
		t.Fatalf("MySQL table DDL:\n got: %s\nwant: %s", got, want)
	}

	reset, err := MySQLAutoIncrementPlan(table, 41)
	if err != nil {
		t.Fatal(err)
	}
	if reset.SQL != "ALTER TABLE `target`.`accounts` AUTO_INCREMENT = 42;" ||
		len(reset.Args) != 0 {
		t.Fatalf("auto-increment plan = %#v", reset)
	}
}

func TestCreateMySQLTableEscapesStringDefaults(t *testing.T) {
	table := Table{
		Name: "notes",
		Columns: []Column{{
			Name: "body",
			Type: "varchar",
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{80},
			},
			Default: mustMySQLTestDefault(
				t,
				`'O''Brien\draft'`,
			),
		}},
	}
	got, err := CreateTable(MySQL, table)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `DEFAULT 'O''Brien\\draft'`) {
		t.Fatalf("unsafe or lossy MySQL string default: %s", got)
	}
}

func TestCreateMySQLTableUsesValidatedExplicitCollation(t *testing.T) {
	table := Table{
		Name:           "notes",
		MySQLCollation: "utf8mb4_0900_bin",
		Columns: []Column{{
			Name:         "id",
			Type:         "integer",
			DeclaredType: &DeclaredType{Base: "int"},
		}},
	}
	statement, err := CreateTable(MySQL, table)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, "COLLATE=utf8mb4_0900_bin") {
		t.Fatalf("explicit MySQL collation was not rendered: %s", statement)
	}
	table.MySQLCollation = "utf8mb4_0900_ai_ci"
	if _, err := CreateTable(MySQL, table); err == nil {
		t.Fatal("unsafe MySQL target collation was accepted")
	}
}

func TestCreateMySQLTableSupportsMariaDBNoPadBinaryCollation(t *testing.T) {
	table := Table{
		Schema:         "app",
		Name:           "events",
		MySQLCollation: "utf8mb4_nopad_bin",
		Columns: []Column{{
			Name: "id",
			Type: "bigint",
		}},
	}
	statement, err := CreateTable(MySQL, table)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statement, "COLLATE=utf8mb4_nopad_bin") {
		t.Fatalf("statement = %q", statement)
	}
}

func TestCreateMySQLTableRejectsInvalidTypesDefaultsAndIdentity(t *testing.T) {
	frontier := int64(4)
	base := Table{
		Name: "accounts",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &DeclaredType{Base: "bigint"},
			},
			{
				Name: "label",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{8},
				},
			},
		},
	}
	tests := map[string]func(*Table){
		"unsupported generation": func(table *Table) {
			table.Identity.Generation = "always"
		},
		"nullable identity": func(table *Table) {
			table.Columns[0].Nullable = true
		},
		"non bigint identity": func(table *Table) {
			table.Columns[0].DeclaredType.Base = "int"
		},
		"identity default": func(table *Table) {
			table.Columns[0].Default = mustMySQLTestDefault(t, "1")
		},
		"oversized varchar": func(table *Table) {
			table.Columns[1].DeclaredType.Arguments[0] =
				mysqlMaximumVarcharCharacters + 1
		},
		"wrong string default family": func(table *Table) {
			table.Columns[0].Default = mustMySQLTestDefault(t, "'bad'")
			table.Identity = nil
		},
		"overlength varbinary default": func(table *Table) {
			table.Columns[1].Type = "blob"
			table.Columns[1].DeclaredType = &DeclaredType{
				Base:      "varbinary",
				Arguments: []int{1},
			}
			table.Columns[1].Default = mustMySQLTestDefault(t, "X'0001'")
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			table := base
			table.Columns = append([]Column(nil), base.Columns...)
			for index := range table.Columns {
				if base.Columns[index].DeclaredType != nil {
					declaration := *base.Columns[index].DeclaredType
					declaration.Arguments = append(
						[]int(nil),
						base.Columns[index].DeclaredType.Arguments...,
					)
					table.Columns[index].DeclaredType = &declaration
				}
			}
			identity := *base.Identity
			table.Identity = &identity
			mutate(&table)
			_, err := CreateTable(MySQL, table)
			var policy *PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestMySQLAutoIncrementPlanRejectsUnsafeFrontiers(t *testing.T) {
	table := Table{
		Name: "accounts",
		Identity: &Identity{
			Column:     "id",
			Generation: IdentityByDefault,
		},
		Columns: []Column{{
			Name:       "id",
			Type:       "bigint",
			PrimaryKey: true,
		}},
	}
	if _, err := MySQLAutoIncrementPlan(table, -1); err == nil {
		t.Fatal("negative frontier unexpectedly planned")
	}
	exhausted, err := MySQLAutoIncrementPlan(
		table,
		int64(^uint64(0)>>1),
	)
	if err != nil || exhausted.SQL != "" || len(exhausted.Args) != 0 {
		t.Fatalf(
			"exhausted frontier plan = %#v, error = %v; want no-op",
			exhausted,
			err,
		)
	}
}

func TestCreateMySQLTableEnforcesDeclaredRowByteBudget(t *testing.T) {
	boundary := Table{
		Name: "boundary",
		Columns: []Column{
			{
				Name: "payload",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{16_382},
				},
			},
			{
				Name:         "medium_value",
				Type:         "integer",
				DeclaredType: &DeclaredType{Base: "mediumint"},
			},
			{
				Name:         "tiny_value",
				Type:         "integer",
				DeclaredType: &DeclaredType{Base: "tinyint"},
			},
		},
	}
	if _, err := CreateTable(MySQL, boundary); err != nil {
		t.Fatalf("65,535-byte declared row was rejected: %v", err)
	}

	oversized := boundary
	oversized.Name = "oversized"
	oversized.Columns = append(
		append([]Column(nil), boundary.Columns...),
		Column{
			Name:         "one_more",
			Type:         "integer",
			DeclaredType: &DeclaredType{Base: "tinyint"},
		},
	)
	_, err := CreateTable(MySQL, oversized)
	var policy *PolicyError
	if !errors.As(err, &policy) {
		t.Fatalf("oversized row error = %v, want PolicyError", err)
	}
	if policy.Operation != "render MySQL declared row" {
		t.Fatalf("oversized row policy = %#v", policy)
	}

	twoMaximumVarchars := Table{
		Name: "two_maximum_varchars",
		Columns: []Column{
			{
				Name: "left_value",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{mysqlMaximumVarcharCharacters},
				},
			},
			{
				Name: "right_value",
				Type: "varchar",
				DeclaredType: &DeclaredType{
					Base:      "varchar",
					Arguments: []int{mysqlMaximumVarcharCharacters},
				},
			},
		},
	}
	if _, err := CreateTable(MySQL, twoMaximumVarchars); err == nil {
		t.Fatal("aggregate oversized VARCHAR row unexpectedly planned")
	}

	nullableBitmapOverflow := Table{
		Name: "nullable_bitmap_overflow",
		Columns: []Column{{
			Name:     "payload",
			Type:     "varchar",
			Nullable: true,
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{16_380},
			},
		}},
	}
	for index := 0; index < 12; index++ {
		nullableBitmapOverflow.Columns = append(
			nullableBitmapOverflow.Columns,
			Column{
				Name:         "nullable_" + strconv.Itoa(index),
				Type:         "integer",
				Nullable:     true,
				DeclaredType: &DeclaredType{Base: "tinyint"},
			},
		)
	}
	if _, err := CreateTable(
		MySQL,
		nullableBitmapOverflow,
	); err == nil {
		t.Fatal("nullable-bitmap row overhead was not enforced")
	}

	fixedWidth := Table{Name: "fixed_width"}
	for index := 0; index < 300; index++ {
		fixedWidth.Columns = append(fixedWidth.Columns, Column{
			Name: "c" + strconv.Itoa(index),
			Type: "char",
			DeclaredType: &DeclaredType{
				Base:      "char",
				Arguments: []int{10},
			},
		})
	}
	if _, err := CreateTable(MySQL, fixedWidth); err == nil {
		t.Fatal("InnoDB local-row limit was not enforced")
	}

	shortVarchars := Table{Name: "short_varchars"}
	for index := 0; index < 350; index++ {
		shortVarchars.Columns = append(shortVarchars.Columns, Column{
			Name: "c" + strconv.Itoa(index),
			Type: "varchar",
			DeclaredType: &DeclaredType{
				Base:      "varchar",
				Arguments: []int{6},
			},
		})
	}
	if _, err := CreateTable(MySQL, shortVarchars); err == nil {
		t.Fatal("short inline VARCHAR local-row limit was not enforced")
	}

	manyTexts := Table{Name: "many_texts"}
	for index := 0; index < 660; index++ {
		manyTexts.Columns = append(manyTexts.Columns, Column{
			Name:         "c" + strconv.Itoa(index),
			Type:         "text",
			DeclaredType: &DeclaredType{Base: "text"},
		})
	}
	if _, err := CreateTable(MySQL, manyTexts); err == nil {
		t.Fatal("LOB pointer local-row limit was not enforced")
	}

	mysqlLiveRejectedLOBShape := Table{Name: "live_rejected_lobs"}
	for index := 0; index < 198; index++ {
		mysqlLiveRejectedLOBShape.Columns = append(
			mysqlLiveRejectedLOBShape.Columns,
			Column{
				Name:         "c" + strconv.Itoa(index),
				Type:         "text",
				DeclaredType: &DeclaredType{Base: "tinytext"},
			},
		)
	}
	if _, err := CreateTable(
		MySQL,
		mysqlLiveRejectedLOBShape,
	); err == nil {
		t.Fatal("live-rejected 198-column LOB shape unexpectedly planned")
	}

	tooManyColumns := Table{Name: "too_many_columns"}
	for index := 0; index <= mysqlMaximumInnoDBColumns; index++ {
		tooManyColumns.Columns = append(tooManyColumns.Columns, Column{
			Name:         "c" + strconv.Itoa(index),
			Type:         "integer",
			DeclaredType: &DeclaredType{Base: "tinyint"},
		})
	}
	if _, err := CreateTable(MySQL, tooManyColumns); err == nil {
		t.Fatal("InnoDB column-count limit was not enforced")
	}
}

func TestNormalizeMySQLDefaultMatchesDiscoveryCanonicalForm(t *testing.T) {
	stringColumn := Column{
		Name: "code",
		Type: "varchar",
		DeclaredType: &DeclaredType{
			Base:      "varchar",
			Arguments: []int{24},
		},
	}
	stringCatalog := `'guest'::character varying`
	stringDefault, err := ParsePostgresCatalogDefault(
		stringColumn,
		&stringCatalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	stringColumn.Default = stringDefault
	normalizedString, err := NormalizeMySQLDefault(stringColumn)
	if err != nil {
		t.Fatal(err)
	}
	if normalizedString == stringDefault ||
		normalizedString.CanonicalSQL() != "'guest'" {
		t.Fatalf("normalized string default = %#v", normalizedString)
	}

	sourceBlob := Column{Name: "payload", Type: "bytea"}
	blobCatalog := `decode('00FF'::text, 'hex'::text)`
	blobDefault, err := ParsePostgresCatalogDefault(
		sourceBlob,
		&blobCatalog,
	)
	if err != nil {
		t.Fatal(err)
	}
	targetBlob := Column{
		Name:     "payload",
		Type:     "blob",
		Nullable: true,
		DeclaredType: &DeclaredType{
			Base: "longblob",
		},
		Default: blobDefault,
	}
	normalizedBlob, err := NormalizeMySQLDefault(targetBlob)
	if err != nil {
		t.Fatal(err)
	}
	if normalizedBlob == blobDefault ||
		normalizedBlob.CanonicalSQL() != "X'00ff'" {
		t.Fatalf("normalized blob default = %#v", normalizedBlob)
	}

	nullDefault := mustMySQLTestDefault(t, "NULL")
	nullable := Column{
		Name:     "optional",
		Type:     "varchar",
		Nullable: true,
		DeclaredType: &DeclaredType{
			Base:      "varchar",
			Arguments: []int{8},
		},
		Default: nullDefault,
	}
	normalizedNull, err := NormalizeMySQLDefault(nullable)
	if err != nil || normalizedNull != nil {
		t.Fatalf(
			"normalized DEFAULT NULL = %#v, %v",
			normalizedNull,
			err,
		)
	}
}
