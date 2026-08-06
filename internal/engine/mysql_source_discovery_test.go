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
		{
			name: "time precision zero",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"local_time",
					"time",
					"time",
				)
				value.datetimePrecision = sql.NullInt64{
					Valid: true,
				}
				return value
			}(),
			columnType: "time",
			base:       "time",
			arguments:  []int{0},
		},
		{
			name: "time fractional precision",
			catalog: func() mysql80SourceColumnCatalog {
				value := baseMySQL80ColumnCatalog(
					"local_time",
					"time",
					"time(6)",
				)
				value.datetimePrecision = sql.NullInt64{
					Int64: 6,
					Valid: true,
				}
				return value
			}(),
			columnType: "time",
			base:       "time",
			arguments:  []int{6},
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

func TestMySQL80SourceColumnFromCatalogPreservesSpatialMetadata(
	t *testing.T,
) {
	explicitZero := sql.NullInt64{Valid: true}
	maximumSRID := sql.NullInt64{
		Int64: int64(^uint32(0)),
		Valid: true,
	}
	tests := []struct {
		dataType string
		subtype  schema.SpatialSubtype
		srid     sql.NullInt64
	}{
		{
			dataType: "geometry",
			subtype:  schema.SpatialSubtypeGeometry,
		},
		{
			dataType: "point",
			subtype:  schema.SpatialSubtypePoint,
			srid:     explicitZero,
		},
		{
			dataType: "linestring",
			subtype:  schema.SpatialSubtypeLineString,
			srid:     maximumSRID,
		},
		{
			dataType: "polygon",
			subtype:  schema.SpatialSubtypePolygon,
		},
		{
			dataType: "multipoint",
			subtype:  schema.SpatialSubtypeMultiPoint,
		},
		{
			dataType: "multilinestring",
			subtype:  schema.SpatialSubtypeMultiLineString,
		},
		{
			dataType: "multipolygon",
			subtype:  schema.SpatialSubtypeMultiPolygon,
		},
		{
			dataType: "geomcollection",
			subtype:  schema.SpatialSubtypeGeometryCollection,
		},
	}
	for _, test := range tests {
		t.Run(test.dataType, func(t *testing.T) {
			catalog := baseMySQL80ColumnCatalog(
				"shape",
				test.dataType,
				test.dataType,
			)
			catalog.srid = test.srid
			column, metadata, err := mySQL80SourceColumnFromCatalog(
				catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if metadata != (mysql80SourceColumnMetadata{}) ||
				column.Type != string(test.subtype) ||
				column.DeclaredType == nil ||
				column.DeclaredType.Base != test.dataType ||
				column.DeclaredType.Spatial == nil ||
				column.DeclaredType.Spatial.Subtype != test.subtype {
				t.Fatalf("spatial column = %#v, metadata = %#v", column, metadata)
			}
			gotSRID := column.DeclaredType.Spatial.SRID
			if !test.srid.Valid {
				if gotSRID != nil {
					t.Fatalf("unspecified SRID = %d, want nil", *gotSRID)
				}
			} else if gotSRID == nil ||
				*gotSRID != uint32(test.srid.Int64) {
				t.Fatalf("SRID = %v, want %d", gotSRID, test.srid.Int64)
			}
			if err := schema.ValidateDeclaredType(
				*column.DeclaredType,
			); err != nil {
				t.Fatalf("canonical spatial metadata: %v", err)
			}
		})
	}
}

func TestMySQL80SourceSpatialColumnFailsClosedOnCatalogMismatch(
	t *testing.T,
) {
	valid := baseMySQL80ColumnCatalog(
		"shape",
		"point",
		"point",
	)
	tests := map[string]func(*mysql80SourceColumnCatalog){
		"column type mismatch": func(value *mysql80SourceColumnCatalog) {
			value.columnType = "geometry"
		},
		"negative SRID": func(value *mysql80SourceColumnCatalog) {
			value.srid = sql.NullInt64{Int64: -1, Valid: true}
		},
		"oversized SRID": func(value *mysql80SourceColumnCatalog) {
			value.srid = sql.NullInt64{
				Int64: int64(^uint32(0)) + 1,
				Valid: true,
			}
		},
		"character length": func(value *mysql80SourceColumnCatalog) {
			value.characterLength = sql.NullInt64{Int64: 32, Valid: true}
		},
		"octet length": func(value *mysql80SourceColumnCatalog) {
			value.octetLength = sql.NullInt64{Int64: 32, Valid: true}
		},
		"numeric precision": func(value *mysql80SourceColumnCatalog) {
			value.numericPrecision = sql.NullInt64{Int64: 10, Valid: true}
		},
		"numeric scale": func(value *mysql80SourceColumnCatalog) {
			value.numericScale = sql.NullInt64{Valid: true}
		},
		"datetime precision": func(value *mysql80SourceColumnCatalog) {
			value.datetimePrecision = sql.NullInt64{Valid: true}
		},
		"character set": func(value *mysql80SourceColumnCatalog) {
			value.characterSet = sql.NullString{
				String: "binary",
				Valid:  true,
			}
		},
		"collation": func(value *mysql80SourceColumnCatalog) {
			value.collation = sql.NullString{
				String: "binary",
				Valid:  true,
			}
		},
		"default": func(value *mysql80SourceColumnCatalog) {
			value.defaultValue = sql.NullString{
				String: "point(1 2)",
				Valid:  true,
			}
		},
		"default generated": func(value *mysql80SourceColumnCatalog) {
			value.extra = "DEFAULT_GENERATED"
		},
		"auto increment": func(value *mysql80SourceColumnCatalog) {
			value.extra = "auto_increment"
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if _, _, err := mySQL80SourceColumnFromCatalog(value); err == nil {
				t.Fatal("expected malformed spatial catalog to fail closed")
			}
		})
	}

	nonSpatial := baseMySQL80ColumnCatalog("payload", "json", "json")
	nonSpatial.srid = sql.NullInt64{Valid: true}
	if _, _, err := mySQL80SourceColumnFromCatalog(nonSpatial); err == nil {
		t.Fatal("expected SRS_ID on a non-spatial type to fail closed")
	}
	unknown := baseMySQL80ColumnCatalog(
		"shape",
		"circularstring",
		"circularstring",
	)
	if _, _, err := mySQL80SourceColumnFromCatalog(unknown); err == nil {
		t.Fatal("expected unknown spatial subtype to fail closed")
	}
}

func TestMySQL80SourceTimeColumnFailsClosedOnUnexpectedCatalogShapes(
	t *testing.T,
) {
	valid := baseMySQL80ColumnCatalog(
		"local_time",
		"time",
		"time(3)",
	)
	valid.datetimePrecision = sql.NullInt64{Int64: 3, Valid: true}

	tests := map[string]func(*mysql80SourceColumnCatalog){
		"missing precision": func(value *mysql80SourceColumnCatalog) {
			value.datetimePrecision = sql.NullInt64{}
		},
		"negative precision": func(value *mysql80SourceColumnCatalog) {
			value.datetimePrecision.Int64 = -1
		},
		"excess precision": func(value *mysql80SourceColumnCatalog) {
			value.datetimePrecision.Int64 = 7
		},
		"precision omitted from column type": func(
			value *mysql80SourceColumnCatalog,
		) {
			value.columnType = "time"
		},
		"explicit zero column type": func(
			value *mysql80SourceColumnCatalog,
		) {
			value.datetimePrecision.Int64 = 0
			value.columnType = "time(0)"
		},
		"numeric precision metadata": func(
			value *mysql80SourceColumnCatalog,
		) {
			value.numericPrecision = sql.NullInt64{
				Int64: 8,
				Valid: true,
			}
		},
		"numeric scale metadata": func(
			value *mysql80SourceColumnCatalog,
		) {
			value.numericScale = sql.NullInt64{Valid: true}
		},
		"character length metadata": func(
			value *mysql80SourceColumnCatalog,
		) {
			value.characterLength = sql.NullInt64{
				Int64: 12,
				Valid: true,
			}
		},
		"character set metadata": func(
			value *mysql80SourceColumnCatalog,
		) {
			value.characterSet = sql.NullString{
				String: "utf8mb4",
				Valid:  true,
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if _, _, err := mySQL80SourceColumnFromCatalog(
				value,
			); err == nil {
				t.Fatal("expected TIME catalog shape to fail closed")
			}
		})
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

// TestMySQLSourceKeyCollationsMustOrderTheSameOnBothEngines pins the rule that
// moved rather than the one that went away.
//
// A table's collation is the default its columns inherit, so requiring a binary
// one there refused every ordinary MySQL table - utf8mb4_0900_ai_ci is 8.0's
// own default - while deciding nothing. Ordering is a property of the columns a
// paged read is ordered by, so that is where it is asked now.
//
// Measured against MySQL 8.0 rather than argued from the names:
//
//	utf8mb4_unicode_ci  orders [EUR, y-diaeresis]  and matches 'Ü' to 'ü'
//	utf8mb4_bin         orders [y-diaeresis, EUR]  and matches neither
//	PostgreSQL          orders [y-diaeresis, EUR]
func TestMySQLSourceKeyCollationsMustOrderTheSameOnBothEngines(t *testing.T) {
	keyed := schema.Table{
		Schema: "so",
		Name:   "tags",
		Columns: []schema.Column{{
			Name:               "tagname",
			Type:               "text",
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "varchar", Arguments: []int{40}},
		}},
	}

	for _, refused := range []struct{ reason, collation string }{
		{
			reason:    "case-insensitive: MySQL calls two different strings equal",
			collation: "utf8mb4_unicode_ci",
		},
		{
			reason:    "8.0's own default, also accent-insensitive",
			collation: "utf8mb4_0900_ai_ci",
		},
		{
			// Binary by name, and not one dmtx has modelled. Ordering is only
			// portable when it has been established, not when it looks like it
			// ought to be - this case moved here from the table-level check.
			reason:    "an unmodelled binary collation",
			collation: "utf8mb4_unmodeled_bin",
		},
	} {
		if err := checkMySQLSourceKeyCollations(
			keyed,
			map[string]string{"tagname": refused.collation},
		); err == nil {
			t.Errorf("accepted a text key with %s (%s)", refused.collation, refused.reason)
		}
	}

	for _, accepted := range []string{"utf8mb4_bin", "utf8mb4_0900_bin"} {
		if err := checkMySQLSourceKeyCollations(
			keyed,
			map[string]string{"tagname": accepted},
		); err != nil {
			t.Errorf("refused %s, whose ordering matches: %v", accepted, err)
		}
	}

	// The same collation on a data column is not asked the question. The value
	// transfers byte for byte whatever it orders by.
	data := keyed
	data.Columns = []schema.Column{{
		Name:         "tagname",
		Type:         "text",
		DeclaredType: &schema.DeclaredType{Base: "varchar", Arguments: []int{40}},
	}}
	if err := checkMySQLSourceKeyCollations(
		data,
		map[string]string{"tagname": "utf8mb4_unicode_ci"},
	); err != nil {
		t.Errorf("refused an ordinary collation on a data column: %v", err)
	}
}
