package migrate

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestValidatePostgresUpsertCatalogShapeAcceptsExactScalarShape(
	t *testing.T,
) {
	planned := postgresUpsertPreflightPlannedTable()
	actual := postgresUpsertPreflightCatalogShape()

	if err := validatePostgresUpsertCatalogShape(planned, actual); err != nil {
		t.Fatalf("validate exact PostgreSQL catalog shape: %v", err)
	}
}

func TestValidatePostgresUpsertCatalogShapeFailsClosed(
	t *testing.T,
) {
	tests := []struct {
		name   string
		want   string
		mutate func(*postgresCatalogTableShape)
	}{
		{
			name: "extra column",
			want: "requires exactly 4",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns = append(
					shape.columns,
					postgresCatalogColumnShape{
						name: "extra",
						columnType: postgresCatalogTypeShape{
							name: "text",
						},
					},
				)
			},
		},
		{
			name: "column order",
			want: `column 1 is "tenant"`,
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[0], shape.columns[1] =
					shape.columns[1], shape.columns[0]
			},
		},
		{
			name: "base type",
			want: "type is int8; planned type is numeric(7,2)",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[2].columnType = postgresCatalogTypeShape{
					name: "int8",
				}
			},
		},
		{
			name: "fixed instead of varying character",
			want: "type is character(8); planned type is character varying(8)",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[1].columnType.name = "bpchar"
			},
		},
		{
			name: "character length",
			want: "character varying(9)",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[1].columnType.characterLength =
					intPointer(9)
			},
		},
		{
			name: "numeric precision",
			want: "numeric(8,2)",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[2].columnType.numericPrecision =
					intPointer(8)
			},
		},
		{
			name: "numeric scale",
			want: "numeric(7,3)",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[2].columnType.numericScale =
					intPointer(3)
			},
		},
		{
			name: "timestamp precision",
			want: "timestamp(0) without time zone; planned type is timestamp(6) without time zone",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[3].columnType.timestampPrecision =
					intPointer(0)
			},
		},
		{
			name: "primary key effective nullability",
			want: "effective nullability differs",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[0].notNull = false
			},
		},
		{
			name: "ordinary effective nullability",
			want: "effective nullability differs",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[3].notNull = true
			},
		},
		{
			name: "primary key order",
			want: "primary key is (tenant, id)",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.primaryKey[0], shape.primaryKey[1] =
					shape.primaryKey[1], shape.primaryKey[0]
			},
		},
		{
			name: "generated column",
			want: `column "occurred_at" is generated`,
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[3].generated = "s"
			},
		},
		{
			name: "identity column",
			want: "identity generation differs",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[0].identity = "d"
			},
		},
		{
			name: "user trigger",
			want: "2 non-internal user triggers",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.userTriggers = 2
			},
		},
		{
			name: "non-default collation",
			want: "uses a non-default collation",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.columns[3].defaultCollation = false
			},
		},
		{
			name: "rewrite rule",
			want: "3 user rewrite rules",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.userRules = 3
			},
		},
		{
			name: "row-level security",
			want: "row-level security enabled or forced",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.rowSecurity = true
			},
		},
		{
			name: "unlogged relation",
			want: "persistence is UNLOGGED",
			mutate: func(shape *postgresCatalogTableShape) {
				shape.persistence = "u"
			},
		},
		{
			name: "partitioned relation",
			want: `catalog relation kind "p"`,
			mutate: func(shape *postgresCatalogTableShape) {
				shape.relationKind = "p"
			},
		},
	}

	planned := postgresUpsertPreflightPlannedTable()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := clonePostgresCatalogTableShape(
				postgresUpsertPreflightCatalogShape(),
			)
			test.mutate(&actual)
			err := validatePostgresUpsertCatalogShape(planned, actual)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("catalog mismatch error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestExpectedPostgresCatalogTypeMatchesRenderedScalarTypes(
	t *testing.T,
) {
	tests := []struct {
		name   string
		column schema.Column
		want   postgresCatalogTypeShape
	}{
		{
			name:   "unmodified varchar renders text",
			column: schema.Column{Name: "value", Type: "varchar"},
			want:   postgresCatalogTypeShape{name: "text"},
		},
		{
			name: "declared character",
			column: schema.Column{
				Name: "value",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
			},
			want: postgresCatalogTypeShape{
				name:            "bpchar",
				characterLength: intPointer(4),
			},
		},
		{
			name: "declared varying character",
			column: schema.Column{
				Name: "value",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{12},
				},
			},
			want: postgresCatalogTypeShape{
				name:            "varchar",
				characterLength: intPointer(12),
			},
		},
		{
			name:   "default numeric modifiers",
			column: schema.Column{Name: "value", Type: "numeric"},
			want: postgresCatalogTypeShape{
				name:             "numeric",
				numericPrecision: intPointer(38),
				numericScale:     intPointer(10),
			},
		},
		{
			name: "declared numeric modifiers",
			column: schema.Column{
				Name: "value",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{7, 2},
				},
			},
			want: postgresCatalogTypeShape{
				name:             "numeric",
				numericPrecision: intPointer(7),
				numericScale:     intPointer(2),
			},
		},
		{
			name:   "timestamp default precision",
			column: schema.Column{Name: "value", Type: "timestamp"},
			want: postgresCatalogTypeShape{
				name:               "timestamp",
				timestampPrecision: intPointer(6),
			},
		},
		{
			name:   "timestamp with time zone default precision",
			column: schema.Column{Name: "value", Type: "timestamptz"},
			want: postgresCatalogTypeShape{
				name:               "timestamptz",
				timestampPrecision: intPointer(6),
			},
		},
		{
			name: "declared timestamp precision",
			column: schema.Column{
				Name: "value",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
			want: postgresCatalogTypeShape{
				name:               "timestamp",
				timestampPrecision: intPointer(3),
			},
		},
		{
			name: "declared timestamp with time zone precision",
			column: schema.Column{
				Name: "value",
				Type: "timestamptz",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamptz",
					Arguments: []int{2},
				},
			},
			want: postgresCatalogTypeShape{
				name:               "timestamptz",
				timestampPrecision: intPointer(2),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := expectedPostgresCatalogType(test.column)
			if err != nil {
				t.Fatalf("expectedPostgresCatalogType: %v", err)
			}
			if !samePostgresCatalogType(got, test.want) {
				t.Fatalf(
					"catalog type = %s, want %s",
					describePostgresCatalogType(got),
					describePostgresCatalogType(test.want),
				)
			}
		})
	}
}

func TestPostgresCatalogTypeFromModifierDecodesExactModifiers(t *testing.T) {
	varchar := postgresCatalogTypeFromModifier("varchar", 4+23)
	if varchar.characterLength == nil ||
		*varchar.characterLength != 23 {
		t.Fatalf("varchar catalog type = %#v", varchar)
	}

	numeric := postgresCatalogTypeFromModifier(
		"numeric",
		int32(4+(19<<16)+6),
	)
	if numeric.numericPrecision == nil ||
		*numeric.numericPrecision != 19 ||
		numeric.numericScale == nil ||
		*numeric.numericScale != 6 {
		t.Fatalf("numeric catalog type = %#v", numeric)
	}

	negativeScale := postgresCatalogTypeFromModifier(
		"numeric",
		int32(4+(8<<16)+0x7ff),
	)
	if negativeScale.numericScale == nil ||
		*negativeScale.numericScale != -1 {
		t.Fatalf("negative-scale catalog type = %#v", negativeScale)
	}

	defaultTimestamp := postgresCatalogTypeFromModifier("timestamp", -1)
	if defaultTimestamp.timestampPrecision == nil ||
		*defaultTimestamp.timestampPrecision != 6 {
		t.Fatalf("default timestamp catalog type = %#v", defaultTimestamp)
	}
	secondTimestamp := postgresCatalogTypeFromModifier("timestamp", 0)
	if secondTimestamp.timestampPrecision == nil ||
		*secondTimestamp.timestampPrecision != 0 {
		t.Fatalf("second timestamp catalog type = %#v", secondTimestamp)
	}
	microsecondTimestampTZ := postgresCatalogTypeFromModifier("timestamptz", 6)
	if microsecondTimestampTZ.timestampPrecision == nil ||
		*microsecondTimestampTZ.timestampPrecision != 6 {
		t.Fatalf("timestamptz catalog type = %#v", microsecondTimestampTZ)
	}
}

func postgresUpsertPreflightPlannedTable() schema.Table {
	return schema.Table{
		Schema: "archive",
		Name:   "measurements",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				Nullable:           true,
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "tenant",
				Type:               "text",
				Nullable:           true,
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{8},
				},
			},
			{
				Name:     "amount",
				Type:     "numeric",
				Nullable: false,
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{7, 2},
				},
			},
			{Name: "occurred_at", Type: "timestamp", Nullable: true},
		},
	}
}

func postgresUpsertPreflightCatalogShape() postgresCatalogTableShape {
	return postgresCatalogTableShape{
		relationKind: "r",
		persistence:  "p",
		columns: []postgresCatalogColumnShape{
			{
				name:             "id",
				columnType:       postgresCatalogTypeShape{name: "int8"},
				notNull:          true,
				defaultCollation: true,
			},
			{
				name: "tenant",
				columnType: postgresCatalogTypeShape{
					name:            "varchar",
					characterLength: intPointer(8),
				},
				notNull:          true,
				defaultCollation: true,
			},
			{
				name: "amount",
				columnType: postgresCatalogTypeShape{
					name:             "numeric",
					numericPrecision: intPointer(7),
					numericScale:     intPointer(2),
				},
				notNull:          true,
				defaultCollation: true,
			},
			{
				name: "occurred_at",
				columnType: postgresCatalogTypeShape{
					name:               "timestamp",
					timestampPrecision: intPointer(6),
				},
				defaultCollation: true,
			},
		},
		primaryKey: []string{"id", "tenant"},
	}
}

func clonePostgresCatalogTableShape(
	source postgresCatalogTableShape,
) postgresCatalogTableShape {
	cloned := source
	cloned.columns = append([]postgresCatalogColumnShape(nil), source.columns...)
	for index := range cloned.columns {
		columnType := cloned.columns[index].columnType
		if columnType.characterLength != nil {
			columnType.characterLength = intPointer(*columnType.characterLength)
		}
		if columnType.numericPrecision != nil {
			columnType.numericPrecision = intPointer(*columnType.numericPrecision)
		}
		if columnType.numericScale != nil {
			columnType.numericScale = intPointer(*columnType.numericScale)
		}
		if columnType.timestampPrecision != nil {
			columnType.timestampPrecision = intPointer(*columnType.timestampPrecision)
		}
		cloned.columns[index].columnType = columnType
	}
	cloned.primaryKey = append([]string(nil), source.primaryKey...)
	return cloned
}
