package engine

import (
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func validMariaDB1011ServerCatalog() mariaDB1011SourceServerCatalog {
	return mariaDB1011SourceServerCatalog{
		version:                   "10.11.18-MariaDB",
		versionComment:            "mariadb.org binary distribution",
		sqlMode:                   "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		sessionTimeZone:           "+00:00",
		systemTimeZone:            "UTC",
		autoIncrementIncrement:    1,
		autoIncrementOffset:       1,
		lowerCaseTableNames:       0,
		explicitTimestampDefaults: 1,
	}
}

func TestMySQLServerFlavorFromCatalogRequiresConsistentIdentity(
	t *testing.T,
) {
	tests := []struct {
		name    string
		catalog mysqlServerFlavorCatalog
		want    mysqlServerFlavor
	}{
		{
			name: "Oracle MySQL",
			catalog: mysqlServerFlavorCatalog{
				version:        "8.0.46",
				versionComment: "MySQL Community Server - GPL",
			},
			want: mysqlServerFlavorOracle80,
		},
		{
			name: "MariaDB",
			catalog: mysqlServerFlavorCatalog{
				version:        "10.11.18-MariaDB",
				versionComment: "mariadb.org binary distribution",
			},
			want: mysqlServerFlavorMariaDB1011,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := mysqlServerFlavorFromCatalog(test.catalog)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("flavor = %d, want %d", got, test.want)
			}
		})
	}

	for name, catalog := range map[string]mysqlServerFlavorCatalog{
		"Maria version with MySQL comment": {
			version:        "10.11.18-MariaDB",
			versionComment: "MySQL Community Server",
		},
		"Maria comment with MySQL version": {
			version:        "8.0.46",
			versionComment: "MariaDB Server",
		},
		"unknown distribution": {
			version:        "8.0.36",
			versionComment: "Compatible database",
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := mysqlServerFlavorFromCatalog(catalog); err == nil {
				t.Fatal("expected inconsistent flavor to fail closed")
			}
		})
	}
}

func TestValidateMariaDB1011SourceServerCatalog(t *testing.T) {
	if err := validateMariaDB1011SourceServerCatalog(
		validMariaDB1011ServerCatalog(),
	); err != nil {
		t.Fatalf("valid MariaDB server catalog: %v", err)
	}

	tests := map[string]func(*mariaDB1011SourceServerCatalog){
		"old patch": func(value *mariaDB1011SourceServerCatalog) {
			value.version = "10.11.7-MariaDB"
		},
		"later series": func(value *mariaDB1011SourceServerCatalog) {
			value.version = "11.4.5-MariaDB"
		},
		"Oracle flavor": func(value *mariaDB1011SourceServerCatalog) {
			value.version = "8.0.46"
			value.versionComment = "MySQL Community Server - GPL"
		},
		"missing strict mode": func(value *mariaDB1011SourceServerCatalog) {
			value.sqlMode = strings.ReplaceAll(
				value.sqlMode,
				"STRICT_TRANS_TABLES,",
				"",
			)
		},
		"unsafe escaping": func(value *mariaDB1011SourceServerCatalog) {
			value.sqlMode += ",NO_BACKSLASH_ESCAPES"
		},
		"empty strings become NULL": func(
			value *mariaDB1011SourceServerCatalog,
		) {
			value.sqlMode += ",EMPTY_STRING_IS_NULL"
		},
		"CHAR padding changes reads": func(
			value *mariaDB1011SourceServerCatalog,
		) {
			value.sqlMode += ",PAD_CHAR_TO_FULL_LENGTH"
		},
		"non UTC": func(value *mariaDB1011SourceServerCatalog) {
			value.sessionTimeZone = "-05:00"
		},
		"auto increment stride": func(value *mariaDB1011SourceServerCatalog) {
			value.autoIncrementIncrement = 2
		},
		"folded identifiers": func(value *mariaDB1011SourceServerCatalog) {
			value.lowerCaseTableNames = 1
		},
		"implicit timestamp defaults": func(
			value *mariaDB1011SourceServerCatalog,
		) {
			value.explicitTimestampDefaults = 0
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validMariaDB1011ServerCatalog()
			mutate(&value)
			if err := validateMariaDB1011SourceServerCatalog(
				value,
			); err == nil {
				t.Fatal("expected server catalog to fail closed")
			}
		})
	}
}

func TestValidateMariaDB1011SourceTableRequiresNoPadBinaryCollation(
	t *testing.T,
) {
	valid := validMySQL80TableCatalog()
	valid.tableCollation.String = "utf8mb4_nopad_bin"
	if err := validateMariaDB1011SourceTableCatalog(
		"app",
		"events",
		valid,
	); err != nil {
		t.Fatalf("valid no-pad MariaDB table: %v", err)
	}

	for _, collation := range []string{
		"utf8mb4_bin",
		"utf8mb4_general_ci",
	} {
		t.Run(collation, func(t *testing.T) {
			value := valid
			value.tableCollation.String = collation
			if err := validateMariaDB1011SourceTableCatalog(
				"app",
				"events",
				value,
			); err == nil {
				t.Fatalf(
					"expected MariaDB collation %q to fail closed",
					collation,
				)
			}
		})
	}

	jsonAlias := baseMariaDB1011ColumnCatalog(
		"document",
		"longtext",
		"longtext",
	)
	jsonAlias.characterLength = sql.NullInt64{
		Int64: 1_073_741_823,
		Valid: true,
	}
	jsonAlias.octetLength = sql.NullInt64{
		Int64: 4_294_967_295,
		Valid: true,
	}
	jsonAlias.characterSet = sql.NullString{
		String: "utf8mb4",
		Valid:  true,
	}
	jsonAlias.collation = sql.NullString{
		String: "utf8mb4_bin",
		Valid:  true,
	}
	jsonAlias.columnCheckCount = 1
	if _, _, err := mariaDB1011SourceColumnFromCatalog(
		jsonAlias,
	); err != nil {
		t.Fatalf("valid MariaDB JSON alias candidate: %v", err)
	}
	for name, checkCount := range map[string]int{
		"missing column check":   0,
		"duplicate column check": 2,
	} {
		t.Run(name, func(t *testing.T) {
			value := jsonAlias
			value.columnCheckCount = checkCount
			if _, _, err := mariaDB1011SourceColumnFromCatalog(
				value,
			); err == nil {
				t.Fatal("expected MariaDB JSON alias candidate to fail closed")
			}
		})
	}
}

func baseMariaDB1011ColumnCatalog(
	name string,
	dataType string,
	columnType string,
) mariaDB1011SourceColumnCatalog {
	return mariaDB1011SourceColumnCatalog{
		position:    1,
		name:        name,
		dataType:    dataType,
		columnType:  columnType,
		nullable:    "NO",
		isGenerated: "NEVER",
		columnKey:   "",
	}
}

func TestMariaDB1011SourceColumnAcceptsExactIntegerDisplayWidths(
	t *testing.T,
) {
	tests := []struct {
		dataType   string
		columnType string
		precision  int64
		wantBase   string
		wantArgs   []int
	}{
		{"tinyint", "tinyint(4)", 3, "tinyint", nil},
		{"tinyint", "tinyint(1)", 3, "tinyint", []int{1}},
		{"smallint", "smallint(6)", 5, "smallint", nil},
		{"mediumint", "mediumint(9)", 7, "mediumint", nil},
		{"int", "int(11)", 10, "int", nil},
		{"bigint", "bigint(20)", 19, "bigint", nil},
	}
	for _, test := range tests {
		t.Run(test.columnType, func(t *testing.T) {
			catalog := baseMariaDB1011ColumnCatalog(
				"value",
				test.dataType,
				test.columnType,
			)
			catalog.numericPrecision = sql.NullInt64{
				Int64: test.precision,
				Valid: true,
			}
			catalog.numericScale = sql.NullInt64{Valid: true}
			column, _, err := mariaDB1011SourceColumnFromCatalog(
				catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if column.DeclaredType == nil ||
				column.DeclaredType.Base != test.wantBase ||
				!equalInts(
					column.DeclaredType.Arguments,
					test.wantArgs,
				) {
				t.Fatalf(
					"declared type = %#v, want %s%v",
					column.DeclaredType,
					test.wantBase,
					test.wantArgs,
				)
			}
		})
	}
}

func TestMariaDB1011SourceColumnPreservesExactTimePrecision(
	t *testing.T,
) {
	tests := []struct {
		columnType string
		precision  int64
	}{
		{columnType: "time", precision: 0},
		{columnType: "time(6)", precision: 6},
	}
	for _, test := range tests {
		t.Run(test.columnType, func(t *testing.T) {
			catalog := baseMariaDB1011ColumnCatalog(
				"local_time",
				"time",
				test.columnType,
			)
			catalog.datetimePrecision = sql.NullInt64{
				Int64: test.precision,
				Valid: true,
			}
			column, _, err := mariaDB1011SourceColumnFromCatalog(
				catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if column.Type != "time" ||
				column.DeclaredType == nil ||
				column.DeclaredType.Base != "time" ||
				!equalInts(
					column.DeclaredType.Arguments,
					[]int{int(test.precision)},
				) {
				t.Fatalf("TIME column = %#v", column)
			}
		})
	}
}

func TestMariaDB1011SourceColumnFailsClosedOnFlavorShapes(
	t *testing.T,
) {
	valid := baseMariaDB1011ColumnCatalog("value", "int", "int(11)")
	valid.numericPrecision = sql.NullInt64{Int64: 10, Valid: true}
	valid.numericScale = sql.NullInt64{Valid: true}

	tests := map[string]func(*mariaDB1011SourceColumnCatalog){
		"custom display width": func(value *mariaDB1011SourceColumnCatalog) {
			value.columnType = "int(7)"
		},
		"unsigned": func(value *mariaDB1011SourceColumnCatalog) {
			value.columnType = "int(11) unsigned"
		},
		"generated": func(value *mariaDB1011SourceColumnCatalog) {
			value.isGenerated = "ALWAYS"
			value.generation = sql.NullString{
				String: "`value` + 1",
				Valid:  true,
			}
		},
		"Oracle extra": func(value *mariaDB1011SourceColumnCatalog) {
			value.extra = "DEFAULT_GENERATED"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if _, _, err := mariaDB1011SourceColumnFromCatalog(
				value,
			); err == nil {
				t.Fatal("expected MariaDB column shape to fail closed")
			}
		})
	}

	text := baseMariaDB1011ColumnCatalog(
		"note",
		"varchar",
		"varchar(24)",
	)
	text.characterLength = sql.NullInt64{Int64: 24, Valid: true}
	text.octetLength = sql.NullInt64{Int64: 96, Valid: true}
	text.characterSet = sql.NullString{String: "utf8mb4", Valid: true}
	text.collation = sql.NullString{
		String: "utf8mb4_nopad_bin",
		Valid:  true,
	}
	if _, _, err := mariaDB1011SourceColumnFromCatalog(text); err != nil {
		t.Fatalf("valid no-pad MariaDB text column: %v", err)
	}
	for _, collation := range []string{
		"utf8mb4_bin",
		"utf8mb4_general_ci",
	} {
		t.Run("collation "+collation, func(t *testing.T) {
			value := text
			value.collation.String = collation
			if _, _, err := mariaDB1011SourceColumnFromCatalog(
				value,
			); err == nil {
				t.Fatalf(
					"expected MariaDB collation %q to fail closed",
					collation,
				)
			}
		})
	}
}

func TestApplyMariaDB1011SourceChecksRecognizesJSONAlias(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "app",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name:         "document",
				Type:         "text",
				Nullable:     true,
				DeclaredType: &schema.DeclaredType{Base: "longtext"},
			},
			{
				Name:         "event_id",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
		},
	}
	checks, err := applyMariaDB1011SourceChecks(
		&table,
		[]mariaDB1011CheckCatalog{
			{
				name:        "document",
				typ:         "CHECK",
				level:       "Column",
				checkClause: "json_valid(`document`)",
			},
			{
				name:        "event_positive",
				typ:         "CHECK",
				level:       "Table",
				checkClause: "`event_id` > 0",
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if table.Columns[0].Type != "json" ||
		table.Columns[0].DeclaredType == nil ||
		table.Columns[0].DeclaredType.Base != "json" {
		t.Fatalf("JSON column = %#v", table.Columns[0])
	}
	if len(checks) != 1 ||
		checks[0].Name != "event_positive" ||
		checks[0].Expression.CanonicalSQL() != `"event_id" > 0` {
		t.Fatalf("table CHECKs = %#v", checks)
	}
}

func TestApplyMariaDB1011SourceChecksFailsClosedOnColumnCheck(
	t *testing.T,
) {
	table := schema.Table{
		Schema: "app",
		Name:   "events",
		Columns: []schema.Column{{
			Name:         "document",
			Type:         "text",
			DeclaredType: &schema.DeclaredType{Base: "longtext"},
		}},
	}
	for name, catalog := range map[string]mariaDB1011CheckCatalog{
		"unrepresented column CHECK": {
			name:        "document",
			typ:         "CHECK",
			level:       "Column",
			checkClause: "char_length(`document`) > 0",
		},
		"mismatched JSON column": {
			name:        "other",
			typ:         "CHECK",
			level:       "Column",
			checkClause: "json_valid(`document`)",
		},
	} {
		t.Run(name, func(t *testing.T) {
			copyTable := table
			copyTable.Columns = append(
				[]schema.Column(nil),
				table.Columns...,
			)
			if _, err := applyMariaDB1011SourceChecks(
				&copyTable,
				[]mariaDB1011CheckCatalog{catalog},
			); err == nil {
				t.Fatal("expected column CHECK to fail closed")
			}
		})
	}
}

func TestApplyMariaDB1011SourceChecksRejectsJSONDefault(
	t *testing.T,
) {
	column := schema.Column{
		Name:         "document",
		Type:         "text",
		DeclaredType: &schema.DeclaredType{Base: "longtext"},
	}
	catalogDefault := "'{}'"
	expression, err := schema.ParseMariaDBCatalogDefault(
		column,
		&catalogDefault,
	)
	if err != nil {
		t.Fatal(err)
	}
	column.Default = expression
	table := schema.Table{
		Schema:  "app",
		Name:    "events",
		Columns: []schema.Column{column},
	}
	_, err = applyMariaDB1011SourceChecks(
		&table,
		[]mariaDB1011CheckCatalog{{
			name:        "document",
			typ:         "CHECK",
			level:       "Column",
			checkClause: "json_valid(`document`)",
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "JSON default") {
		t.Fatalf("JSON default error = %v", err)
	}
}

func TestMariaDB1011CatalogQueriesUseFlavorSpecificColumns(
	t *testing.T,
) {
	for name, query := range map[string]string{
		"columns": mariaDB1011SourceColumnsQuery,
		"primary": mariaDB1011SourcePrimaryKeyQuery,
		"indexes": mariaDB1011SourceIndexesQuery,
		"checks":  mariaDB1011SourceChecksQuery,
	} {
		t.Run(name, func(t *testing.T) {
			for _, forbidden := range []string{
				".ENFORCED",
				".IS_VISIBLE",
				".EXPRESSION",
				"SRS_ID",
			} {
				if strings.Contains(query, forbidden) {
					t.Fatalf(
						"MariaDB query contains Oracle field %q: %s",
						forbidden,
						query,
					)
				}
			}
		})
	}
	if !strings.Contains(
		mariaDB1011SourceIndexesQuery,
		".IGNORED",
	) || !strings.Contains(
		mariaDB1011SourceChecksQuery,
		".LEVEL",
	) || !strings.Contains(
		mariaDB1011SourceColumnsQuery,
		"IS_GENERATED",
	) {
		t.Fatal("MariaDB queries omit required flavor-specific fields")
	}
}

func TestTranslateMariaDB1011PolicyRetainsClassification(
	t *testing.T,
) {
	err := translateMariaDB1011Policy(mysqlSourcePolicy("type", "int"))
	var policy *schema.PolicyError
	if !errors.As(err, &policy) ||
		!strings.Contains(policy.Operation, "MariaDB 10.11") ||
		strings.Contains(policy.Operation, "MySQL 8.0") {
		t.Fatalf("translated policy = %#v", err)
	}
}

func equalInts(left, right []int) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
