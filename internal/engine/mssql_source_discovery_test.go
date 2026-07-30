package engine

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestValidateSQLServer2022SourceCatalog(t *testing.T) {
	base := sqlServer2022SourceCatalog{
		productMajorVersion: 16,
		engineEdition:       3,
		productVersion:      "16.0.4250.1",
		edition:             "Developer Edition (64-bit)",
		databaseName:        "dmtx_source",
		compatibilityLevel:  160,
		state:               "ONLINE",
		userAccess:          "MULTI_USER",
		containment:         "NONE",
	}
	if err := validateSQLServer2022SourceCatalog(base); err != nil {
		t.Fatalf("valid SQL Server 2022 catalog: %v", err)
	}
	tests := map[string]func(*sqlServer2022SourceCatalog){
		"future major": func(value *sqlServer2022SourceCatalog) {
			value.productMajorVersion = 17
			value.productVersion = "17.0.1000.1"
		},
		"Azure edition": func(value *sqlServer2022SourceCatalog) {
			value.engineEdition = 5
		},
		"old compatibility": func(value *sqlServer2022SourceCatalog) {
			value.compatibilityLevel = 150
		},
		"read only": func(value *sqlServer2022SourceCatalog) {
			value.readOnly = true
		},
		"snapshot clone": func(value *sqlServer2022SourceCatalog) {
			value.sourceDatabaseID.Valid = true
			value.sourceDatabaseID.Int64 = 7
		},
		"replication": func(value *sqlServer2022SourceCatalog) {
			value.published = true
		},
		"change data capture": func(value *sqlServer2022SourceCatalog) {
			value.changeDataCapture = true
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			if err := validateSQLServer2022SourceCatalog(value); !errors.As(
				err,
				&policy,
			) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestValidSQLServerSourceIdentifierRejectsLossySurrogateDecode(
	t *testing.T,
) {
	if validSQLServerSourceIdentifier("safe\uFFFDname") {
		t.Fatal("replacement rune was accepted at the sysname boundary")
	}
}

func TestValidateSQLServerSourceTableCatalogFailsClosed(t *testing.T) {
	base := sqlServerSourceTableCatalog{
		objectID:        42,
		typeDescription: "USER_TABLE",
		durability:      "SCHEMA_AND_DATA",
		columnCount:     3,
		maxPartition:    1,
	}
	if err := validateSQLServerSourceTableCatalog(
		"dbo",
		"events",
		base,
	); err != nil {
		t.Fatalf("valid SQL Server table catalog: %v", err)
	}
	tests := map[string]func(*sqlServerSourceTableCatalog){
		"system table": func(value *sqlServerSourceTableCatalog) {
			value.systemShipped = true
		},
		"temporal": func(value *sqlServerSourceTableCatalog) {
			value.temporalType = 2
		},
		"memory optimized": func(value *sqlServerSourceTableCatalog) {
			value.memoryOptimized = true
		},
		"filestream": func(value *sqlServerSourceTableCatalog) {
			value.fileStreamDataSpaceID.Valid = true
			value.fileStreamDataSpaceID.Int64 = 2
		},
		"replicated": func(value *sqlServerSourceTableCatalog) {
			value.replicated = true
		},
		"graph": func(value *sqlServerSourceTableCatalog) {
			value.edge = true
		},
		"ledger": func(value *sqlServerSourceTableCatalog) {
			value.ledgerType = 2
		},
		"partitioned": func(value *sqlServerSourceTableCatalog) {
			value.maxPartition = 2
		},
		"triggered": func(value *sqlServerSourceTableCatalog) {
			value.triggerCount = 1
		},
		"row security": func(value *sqlServerSourceTableCatalog) {
			value.securityPredicateCount = 1
		},
		"full text": func(value *sqlServerSourceTableCatalog) {
			value.fullTextIndexCount = 1
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			if err := validateSQLServerSourceTableCatalog(
				"dbo",
				"events",
				value,
			); !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}
}

func TestSQLServerSourceColumnFromCatalogPreservesModifiers(t *testing.T) {
	tests := []struct {
		name    string
		catalog sqlServerSourceColumnCatalog
		want    schema.Column
	}{
		{
			name: "bigint",
			catalog: sqlServerSourceTestColumn(
				"id",
				"bigint",
				8,
				19,
				0,
			),
			want: schema.Column{
				Name: "id",
				Type: "bigint",
				DeclaredType: &schema.DeclaredType{
					Base: "bigint",
				},
			},
		},
		{
			name: "decimal",
			catalog: sqlServerSourceTestColumn(
				"amount",
				"decimal",
				9,
				12,
				3,
			),
			want: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{12, 3},
				},
			},
		},
		{
			name: "UTF8 varchar",
			catalog: func() sqlServerSourceColumnCatalog {
				value := sqlServerSourceTestColumn(
					"note",
					"varchar",
					80,
					0,
					0,
				)
				value.ansiPadded = true
				value.collation.Valid = true
				value.collation.String =
					"Latin1_General_100_BIN2_UTF8"
				return value
			}(),
			want: schema.Column{
				Name: "note",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{80},
				},
			},
		},
		{
			name: "datetime2 microseconds",
			catalog: sqlServerSourceTestColumn(
				"occurred_at",
				"datetime2",
				8,
				26,
				6,
			),
			want: schema.Column{
				Name: "occurred_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{6},
				},
			},
		},
		{
			name: "smalldatetime exact minute",
			catalog: sqlServerSourceTestColumn(
				"minute_at",
				"smalldatetime",
				4,
				16,
				0,
			),
			want: schema.Column{
				Name: "minute_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "smalldatetime",
				},
			},
		},
		{
			name: "varbinary max",
			catalog: func() sqlServerSourceColumnCatalog {
				value := sqlServerSourceTestColumn(
					"payload",
					"varbinary",
					-1,
					0,
					0,
				)
				value.ansiPadded = true
				value.nullable = true
				return value
			}(),
			want: schema.Column{
				Name:     "payload",
				Type:     "blob",
				Nullable: true,
				DeclaredType: &schema.DeclaredType{
					Base: "blob",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, identity, err := sqlServerSourceColumnFromCatalog(
				test.catalog,
			)
			if err != nil {
				t.Fatal(err)
			}
			if identity != nil || !reflect.DeepEqual(got, test.want) {
				t.Fatalf(
					"column = %#v, identity = %#v; want %#v, nil",
					got,
					identity,
					test.want,
				)
			}
		})
	}
}

func TestSQLServerSourceColumnFromCatalogRejectsUnsafeShapes(t *testing.T) {
	base := sqlServerSourceTestColumn("id", "bigint", 8, 19, 0)
	tests := map[string]func(*sqlServerSourceColumnCatalog){
		"user type": func(value *sqlServerSourceColumnCatalog) {
			value.userDefined = true
		},
		"computed": func(value *sqlServerSourceColumnCatalog) {
			value.computed = true
		},
		"masked": func(value *sqlServerSourceColumnCatalog) {
			value.masked = true
		},
		"encrypted": func(value *sqlServerSourceColumnCatalog) {
			value.encryptionType.Valid = true
			value.encryptionType.Int64 = 1
		},
		"rule bound": func(value *sqlServerSourceColumnCatalog) {
			value.ruleObjectID = 8
		},
		"bad integer width": func(value *sqlServerSourceColumnCatalog) {
			value.maxLength = 4
		},
		"unexpected ANSI padding": func(value *sqlServerSourceColumnCatalog) {
			value.ansiPadded = true
		},
		"datetime2 precision mismatch": func(value *sqlServerSourceColumnCatalog) {
			value.typeName = "datetime2"
			value.maxLength = 8
			value.precision = 25
			value.scale = 6
		},
		"datetime2 nanoseconds": func(value *sqlServerSourceColumnCatalog) {
			value.typeName = "datetime2"
			value.maxLength = 8
			value.precision = 27
			value.scale = 7
		},
		"legacy datetime driver rounds source ticks": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "datetime"
			value.maxLength = 8
			value.precision = 23
			value.scale = 3
		},
		"datetimeoffset loses source offset": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "datetimeoffset"
			value.maxLength = 10
			value.precision = 33
			value.scale = 6
		},
		"char max catalog shape": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "char"
			value.maxLength = -1
			value.precision = 0
			value.ansiPadded = true
			value.collation = sql.NullString{
				String: "Latin1_General_100_BIN2_UTF8",
				Valid:  true,
			}
		},
		"nchar max catalog shape": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "nchar"
			value.maxLength = -1
			value.precision = 0
			value.ansiPadded = true
			value.collation = sql.NullString{
				String: "Latin1_General_100_BIN2",
				Valid:  true,
			}
		},
		"national text can contain unpaired UTF-16 surrogates": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "nvarchar"
			value.maxLength = 80
			value.precision = 0
			value.ansiPadded = true
			value.collation = sql.NullString{
				String: "Latin1_General_100_BIN2",
				Valid:  true,
			}
		},
		"binary max catalog shape": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "binary"
			value.maxLength = -1
			value.precision = 0
			value.ansiPadded = true
		},
		"non-UTF8 narrow text": func(
			value *sqlServerSourceColumnCatalog,
		) {
			value.typeName = "varchar"
			value.maxLength = 24
			value.precision = 0
			value.ansiPadded = true
			value.collation = sql.NullString{
				String: "Latin1_General_100_BIN2",
				Valid:  true,
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			value := base
			mutate(&value)
			var policy *schema.PolicyError
			_, _, err := sqlServerSourceColumnFromCatalog(value)
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want PolicyError", err)
			}
		})
	}

	text := sqlServerSourceTestColumn(
		"label",
		"varchar",
		20,
		0,
		0,
	)
	text.ansiPadded = true
	text.collation.Valid = true
	text.collation.String = "SQL_Latin1_General_CP1_CI_AS"
	if _, _, err := sqlServerSourceColumnFromCatalog(text); err == nil {
		t.Fatal("nonbinary text collation was accepted")
	}
}

func TestApplySQLServerSourceIdentityRequiresSingleBigintPrimaryKey(
	t *testing.T,
) {
	frontier := int64(41)
	table := schema.Table{
		Schema: "dbo",
		Name:   "accounts",
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
		}},
	}
	if err := applySQLServerSourceIdentity(
		&table,
		&sqlServerSourceIdentityCatalog{
			column:   "id",
			frontier: &frontier,
		},
	); err != nil {
		t.Fatal(err)
	}
	if table.Identity == nil ||
		table.Identity.Column != "id" ||
		table.Identity.Frontier == nil ||
		*table.Identity.Frontier != 41 {
		t.Fatalf("identity = %#v", table.Identity)
	}

	table.Columns = append(table.Columns, schema.Column{
		Name:               "tenant",
		Type:               "integer",
		PrimaryKey:         true,
		PrimaryKeyPosition: 2,
	})
	if err := applySQLServerSourceIdentity(
		&table,
		&sqlServerSourceIdentityCatalog{column: "id"},
	); err == nil {
		t.Fatal("composite-key SQL Server identity was accepted")
	}
}

func sqlServerSourceTestColumn(
	name string,
	typeName string,
	maxLength int,
	precision int,
	scale int,
) sqlServerSourceColumnCatalog {
	return sqlServerSourceColumnCatalog{
		position:    1,
		name:        name,
		typeSchema:  "sys",
		typeName:    typeName,
		maxLength:   maxLength,
		precision:   precision,
		scale:       scale,
		defaultName: sql.NullString{},
	}
}
