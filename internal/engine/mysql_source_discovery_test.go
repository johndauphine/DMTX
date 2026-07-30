package engine

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func validMySQL80ServerCatalog() mysql80SourceServerCatalog {
	return mysql80SourceServerCatalog{
		version:                   "8.0.46",
		versionComment:            "MySQL Community Server - GPL",
		sqlMode:                   "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION",
		sessionTimeZone:           "+00:00",
		systemTimeZone:            "UTC",
		autoIncrementIncrement:    1,
		autoIncrementOffset:       1,
		lowerCaseTableNames:       0,
		explicitTimestampDefaults: 1,
		foreignKeyChecks:          1,
		uniqueChecks:              1,
		innodbPageSize:            16_384,
	}
}

func TestValidateMySQL80SourceServerCatalog(t *testing.T) {
	if err := validateMySQL80SourceServerCatalog(
		validMySQL80ServerCatalog(),
	); err != nil {
		t.Fatalf("valid server catalog: %v", err)
	}

	tests := map[string]func(*mysql80SourceServerCatalog){
		"MariaDB flavor": func(value *mysql80SourceServerCatalog) {
			value.version = "10.11.8-MariaDB"
			value.versionComment = "mariadb.org binary distribution"
		},
		"later major": func(value *mysql80SourceServerCatalog) {
			value.version = "8.4.0"
		},
		"old patch": func(value *mysql80SourceServerCatalog) {
			value.version = "8.0.15"
		},
		"unsafe mode": func(value *mysql80SourceServerCatalog) {
			value.sqlMode += ",NO_BACKSLASH_ESCAPES"
		},
		"CHAR padding changes reads": func(
			value *mysql80SourceServerCatalog,
		) {
			value.sqlMode += ",PAD_CHAR_TO_FULL_LENGTH"
		},
		"missing strict mode": func(value *mysql80SourceServerCatalog) {
			value.sqlMode = strings.ReplaceAll(
				value.sqlMode,
				"STRICT_TRANS_TABLES,",
				"",
			)
		},
		"non-UTC session": func(value *mysql80SourceServerCatalog) {
			value.sessionTimeZone = "-05:00"
		},
		"increment stride": func(value *mysql80SourceServerCatalog) {
			value.autoIncrementIncrement = 2
		},
		"folded identifiers": func(value *mysql80SourceServerCatalog) {
			value.lowerCaseTableNames = 1
		},
		"implicit timestamp defaults": func(value *mysql80SourceServerCatalog) {
			value.explicitTimestampDefaults = 0
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validMySQL80ServerCatalog()
			mutate(&value)
			if err := validateMySQL80SourceServerCatalog(value); err == nil {
				t.Fatal("expected catalog policy error")
			}
		})
	}
}

func TestValidateMySQL80TargetServerCatalogRequiresRowAliasUpsert(
	t *testing.T,
) {
	value := validMySQL80ServerCatalog()
	value.version = "8.0.30"
	value.sqlMode += ",NO_AUTO_VALUE_ON_ZERO"
	if err := validateMySQL80TargetServerCatalog(value); err != nil {
		t.Fatalf("minimum native target version: %v", err)
	}
	value.version = "8.0.29"
	if err := validateMySQL80TargetServerCatalog(value); err == nil ||
		!strings.Contains(err.Error(), "native target session contract") {
		t.Fatalf(
			"old native target version error = %v, want upsert policy error",
			err,
		)
	}

	for name, mutate := range map[string]func(*mysql80SourceServerCatalog){
		"zero identity mode": func(value *mysql80SourceServerCatalog) {
			value.sqlMode = strings.ReplaceAll(
				value.sqlMode,
				",NO_AUTO_VALUE_ON_ZERO",
				"",
			)
		},
		"foreign key enforcement": func(value *mysql80SourceServerCatalog) {
			value.foreignKeyChecks = 0
		},
		"unique enforcement": func(value *mysql80SourceServerCatalog) {
			value.uniqueChecks = 0
		},
		"InnoDB page size": func(value *mysql80SourceServerCatalog) {
			value.innodbPageSize = 8_192
		},
	} {
		t.Run(name, func(t *testing.T) {
			value := validMySQL80ServerCatalog()
			value.version = "8.0.30"
			value.sqlMode += ",NO_AUTO_VALUE_ON_ZERO"
			mutate(&value)
			if err := validateMySQL80TargetServerCatalog(value); err == nil {
				t.Fatal("unsafe target session was accepted")
			}
		})
	}
}

func validMySQL80TableCatalog() mysql80SourceTableCatalog {
	return mysql80SourceTableCatalog{
		tableType:      "BASE TABLE",
		engine:         sql.NullString{String: "InnoDB", Valid: true},
		version:        sql.NullInt64{Int64: 10, Valid: true},
		rowFormat:      sql.NullString{String: "Dynamic", Valid: true},
		tableCollation: sql.NullString{String: "utf8mb4_bin", Valid: true},
		columnCount:    3,
	}
}

func TestValidateMySQL80SourceTableCatalogFailsClosed(t *testing.T) {
	if err := validateMySQL80SourceTableCatalog(
		"crm",
		"events",
		validMySQL80TableCatalog(),
	); err != nil {
		t.Fatalf("valid table catalog: %v", err)
	}

	tests := map[string]func(*mysql80SourceTableCatalog){
		"view": func(value *mysql80SourceTableCatalog) {
			value.tableType = "VIEW"
		},
		"non-InnoDB": func(value *mysql80SourceTableCatalog) {
			value.engine.String = "MyISAM"
		},
		"nonbinary collation": func(value *mysql80SourceTableCatalog) {
			value.tableCollation.String = "utf8mb4_0900_ai_ci"
		},
		"different binary collation semantics": func(
			value *mysql80SourceTableCatalog,
		) {
			value.tableCollation.String = "utf8mb4_unmodeled_bin"
		},
		"table options": func(value *mysql80SourceTableCatalog) {
			value.createOptions = "stats_persistent=1"
		},
		"comment": func(value *mysql80SourceTableCatalog) {
			value.tableComment = "not retained"
		},
		"partition": func(value *mysql80SourceTableCatalog) {
			value.partitionCount = 2
		},
		"trigger": func(value *mysql80SourceTableCatalog) {
			value.triggerCount = 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := validMySQL80TableCatalog()
			mutate(&value)
			if err := validateMySQL80SourceTableCatalog(
				"crm",
				"events",
				value,
			); err == nil {
				t.Fatal("expected catalog policy error")
			}
		})
	}
}

func baseMySQL80ColumnCatalog(
	name string,
	dataType string,
	columnType string,
) mysql80SourceColumnCatalog {
	return mysql80SourceColumnCatalog{
		position:   1,
		name:       name,
		dataType:   dataType,
		columnType: columnType,
		nullable:   "NO",
	}
}

func TestMySQL80SourceColumnFromCatalogPreservesModifiers(t *testing.T) {
	tests := []struct {
		name       string
		catalog    mysql80SourceColumnCatalog
		columnType string
		base       string
		arguments  []int
	}{
		{
			name: "tinyint one",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"enabled",
					"tinyint",
					"tinyint(1)",
				)
				value.numericPrecision = sql.NullInt64{
					Int64: 3,
					Valid: true,
				}
				value.numericScale = sql.NullInt64{Valid: true}
				return value
			}(),
			columnType: "integer",
			base:       "tinyint",
			arguments:  []int{1},
		},
		{
			name: "bigint",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"id",
					"bigint",
					"bigint",
				)
				value.numericPrecision = sql.NullInt64{
					Int64: 19,
					Valid: true,
				}
				value.numericScale = sql.NullInt64{Valid: true}
				return value
			}(),
			columnType: "bigint",
			base:       "bigint",
		},
		{
			name: "decimal",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"amount",
					"decimal",
					"decimal(12,3)",
				)
				value.numericPrecision = sql.NullInt64{
					Int64: 12,
					Valid: true,
				}
				value.numericScale = sql.NullInt64{
					Int64: 3,
					Valid: true,
				}
				return value
			}(),
			columnType: "numeric",
			base:       "decimal",
			arguments:  []int{12, 3},
		},
		{
			name: "varchar",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"label",
					"varchar",
					"varchar(40)",
				)
				value.characterLength = sql.NullInt64{
					Int64: 40,
					Valid: true,
				}
				value.octetLength = sql.NullInt64{
					Int64: 160,
					Valid: true,
				}
				value.characterSet = sql.NullString{
					String: "utf8mb4",
					Valid:  true,
				}
				value.collation = sql.NullString{
					String: "utf8mb4_bin",
					Valid:  true,
				}
				return value
			}(),
			columnType: "varchar",
			base:       "varchar",
			arguments:  []int{40},
		},
		{
			name: "datetime precision zero",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"occurred_at",
					"datetime",
					"datetime",
				)
				value.datetimePrecision = sql.NullInt64{
					Valid: true,
				}
				return value
			}(),
			columnType: "datetime",
			base:       "datetime",
			arguments:  []int{0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			column, _, err := mySQL80SourceColumnFromCatalog(
				test.catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if column.Type != test.columnType ||
				column.DeclaredType == nil ||
				column.DeclaredType.Base != test.base ||
				!equalMySQLSourceArguments(
					column.DeclaredType.Arguments,
					test.arguments,
				) {
				t.Fatalf("column = %#v", column)
			}
		})
	}
}

func TestMySQL80SourceColumnFromCatalogFailsClosed(t *testing.T) {
	catalog := baseMySQL80ColumnCatalog(
		"id",
		"bigint",
		"bigint unsigned",
	)
	catalog.numericPrecision = sql.NullInt64{
		Int64: 20,
		Valid: true,
	}
	catalog.numericScale = sql.NullInt64{Valid: true}
	if _, _, err := mySQL80SourceColumnFromCatalog(catalog); err == nil {
		t.Fatal("expected unsigned type to be rejected")
	}

	catalog = baseMySQL80ColumnCatalog(
		"generated_value",
		"int",
		"int",
	)
	catalog.numericPrecision = sql.NullInt64{
		Int64: 10,
		Valid: true,
	}
	catalog.numericScale = sql.NullInt64{Valid: true}
	catalog.extra = "STORED GENERATED"
	catalog.generation = "(`id` * 2)"
	if _, _, err := mySQL80SourceColumnFromCatalog(catalog); err == nil {
		t.Fatal("expected generated column to be rejected")
	}
}

func TestDiscoverMySQL80SourceIdentityPreservesFrontier(t *testing.T) {
	next := int64(42)
	table := schema.Table{
		Schema: "crm",
		Name:   "accounts",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType: &schema.DeclaredType{
				Base: "bigint",
			},
		}},
	}
	identity, err := discoverMySQL80SourceIdentity(
		mysql80SourceTableCatalog{
			autoIncrement: sql.NullInt64{
				Int64: next,
				Valid: true,
			},
		},
		table,
		"id",
	)
	if err != nil {
		t.Fatal(err)
	}
	if identity.Frontier == nil || *identity.Frontier != 41 {
		t.Fatalf("identity = %#v", identity)
	}

	table.Columns[0].Type = "integer"
	if _, err := discoverMySQL80SourceIdentity(
		mysql80SourceTableCatalog{
			autoIncrement: sql.NullInt64{
				Int64: next,
				Valid: true,
			},
		},
		table,
		"id",
	); err == nil {
		t.Fatal("expected narrow auto-increment to fail closed")
	}
}

func TestValidMySQLForeignKeyCatalog(t *testing.T) {
	value := mysql80ForeignKeyCatalog{
		position:               1,
		referencedSchema:       sql.NullString{String: "crm", Valid: true},
		referencedTable:        sql.NullString{String: "accounts", Valid: true},
		referencedColumn:       sql.NullString{String: "id", Valid: true},
		uniquePosition:         sql.NullInt64{Int64: 1, Valid: true},
		uniqueConstraintSchema: sql.NullString{String: "crm", Valid: true},
		uniqueConstraintName:   sql.NullString{String: "PRIMARY", Valid: true},
		match:                  "NONE",
		onUpdate:               "CASCADE",
		onDelete:               "RESTRICT",
	}
	if !validMySQLForeignKeyCatalog(
		schema.Table{Schema: "crm"},
		value,
	) {
		t.Fatal("expected valid foreign key catalog")
	}
	value.referencedSchema.String = "other"
	if validMySQLForeignKeyCatalog(
		schema.Table{Schema: "crm"},
		value,
	) {
		t.Fatal("expected cross-schema foreign key to fail closed")
	}
}

func equalMySQLSourceArguments(left, right []int) bool {
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
