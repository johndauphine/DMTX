package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestValidatePostgres16SourceVersion(t *testing.T) {
	for _, version := range []int{160000, 160013, 169999} {
		if err := validatePostgres16SourceVersion(version); err != nil {
			t.Fatalf("version %d: %v", version, err)
		}
	}
	for _, version := range []int{150999, 170000, 180001} {
		err := validatePostgres16SourceVersion(version)
		var policy *schema.PolicyError
		if !errors.As(err, &policy) ||
			policy.Operation !=
				"discover PostgreSQL source verify catalog version" {
			t.Fatalf("version %d error = %v", version, err)
		}
	}
}

func validPostgresSourceColumnCatalog(
	name string,
	typeName string,
	typeModifier int32,
) postgresSourceColumnCatalog {
	return postgresSourceColumnCatalog{
		position:         1,
		name:             name,
		typeNamespace:    "pg_catalog",
		typeName:         typeName,
		typeKind:         "b",
		typeModifier:     typeModifier,
		formattedType:    typeName,
		notNull:          true,
		local:            true,
		defaultCollation: true,
	}
}

func TestPostgresSourceColumnFromCatalogPreservesExactModifiers(
	t *testing.T,
) {
	numericModifier := int32(4 + (12 << 16) + 2)
	tests := []struct {
		name     string
		catalog  postgresSourceColumnCatalog
		expected schema.Column
	}{
		{
			name:    "integer",
			catalog: validPostgresSourceColumnCatalog("id", "int4", -1),
			expected: schema.Column{
				Name: "id",
				Type: "integer",
			},
		},
		{
			name:    "varchar length",
			catalog: validPostgresSourceColumnCatalog("code", "varchar", 16),
			expected: schema.Column{
				Name: "code",
				Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{12},
				},
			},
		},
		{
			name: "numeric precision and scale",
			catalog: validPostgresSourceColumnCatalog(
				"amount",
				"numeric",
				numericModifier,
			),
			expected: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{12, 2},
				},
			},
		},
		{
			name: "explicit timestamp precision",
			catalog: validPostgresSourceColumnCatalog(
				"created_at",
				"timestamp",
				3,
			),
			expected: schema.Column{
				Name: "created_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
		},
		{
			name: "default timestamptz precision",
			catalog: validPostgresSourceColumnCatalog(
				"observed_at",
				"timestamptz",
				-1,
			),
			expected: schema.Column{
				Name: "observed_at",
				Type: "timestamptz",
			},
		},
		{
			name:    "nullable boolean",
			catalog: validPostgresSourceColumnCatalog("enabled", "bool", -1),
			expected: schema.Column{
				Name: "enabled",
				Type: "boolean",
			},
		},
	}
	tests[len(tests)-1].catalog.notNull = false
	tests[len(tests)-1].expected.Nullable = true

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := postgresSourceColumnFromCatalog(test.catalog)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, test.expected) {
				t.Fatalf("column = %#v, want %#v", actual, test.expected)
			}
		})
	}
}

func TestPostgresSourceColumnFromCatalogFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*postgresSourceColumnCatalog)
	}{
		{
			name: "domain",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.typeKind = "d"
			},
		},
		{
			name: "array",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.typeElement = 23
			},
		},
		{
			name: "unbounded varchar",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.typeName = "varchar"
				value.typeModifier = -1
			},
		},
		{
			name: "unconstrained numeric",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.typeName = "numeric"
				value.typeModifier = -1
			},
		},
		{
			name: "real would widen",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.typeName = "float4"
			},
		},
		{
			name: "generated",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.generated = "s"
			},
		},
		{
			name: "identity always",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.identity = "a"
			},
		},
		{
			name: "nondefault collation",
			mutate: func(value *postgresSourceColumnCatalog) {
				value.defaultCollation = false
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := validPostgresSourceColumnCatalog(
				"value",
				"int8",
				-1,
			)
			test.mutate(&catalog)
			_, err := postgresSourceColumnFromCatalog(catalog)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want policy error", err)
			}
		})
	}
}

func validPostgresSourceTableCatalog() postgresSourceTableCatalog {
	return postgresSourceTableCatalog{
		objectID:        42,
		relationKind:    "r",
		persistence:     "p",
		accessMethod:    "heap",
		attributeCount:  3,
		replicaIdentity: "d",
	}
}

func TestValidatePostgresSourceTableCatalogFailsClosed(t *testing.T) {
	if err := validatePostgresSourceTableCatalog(
		"source",
		"events",
		validPostgresSourceTableCatalog(),
	); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*postgresSourceTableCatalog)
	}{
		{
			name: "partition",
			mutate: func(value *postgresSourceTableCatalog) {
				value.partition = true
			},
		},
		{
			name: "inheritance",
			mutate: func(value *postgresSourceTableCatalog) {
				value.parents = 1
			},
		},
		{
			name: "row security",
			mutate: func(value *postgresSourceTableCatalog) {
				value.rowSecurity = true
			},
		},
		{
			name: "user trigger",
			mutate: func(value *postgresSourceTableCatalog) {
				value.userTriggers = 1
			},
		},
		{
			name: "storage options",
			mutate: func(value *postgresSourceTableCatalog) {
				value.relationOptions = 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			catalog := validPostgresSourceTableCatalog()
			test.mutate(&catalog)
			err := validatePostgresSourceTableCatalog(
				"source",
				"events",
				catalog,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) {
				t.Fatalf("error = %v, want policy error", err)
			}
		})
	}
}

func TestPostgresSourceIndexFromCatalog(t *testing.T) {
	table := schema.Table{
		Schema: "source",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "code", Type: "text"},
			{Name: "created_at", Type: "timestamp"},
		},
	}
	catalog := postgresSourceIndexCatalog{
		objectID:       91,
		namespace:      "source",
		name:           "events_code_created_idx",
		unique:         true,
		valid:          true,
		ready:          true,
		live:           true,
		method:         "btree",
		keyColumns:     2,
		totalColumns:   2,
		constraintType: "",
		columns: []postgresSourceIndexColumn{
			{
				name:            "code",
				nullsFirst:      true,
				collationSchema: "pg_catalog",
				collationName:   "C",
				defaultOperator: true,
			},
			{
				name:            "created_at",
				descending:      true,
				nullsFirst:      false,
				defaultOperator: true,
			},
		},
	}
	index, err := postgresSourceIndexFromCatalog(table, catalog)
	if err != nil {
		t.Fatal(err)
	}
	want := schema.Index{
		Name:   catalog.name,
		Unique: true,
		Columns: []schema.IndexColumn{
			{Name: "code", Collation: "BINARY"},
			{Name: "created_at", Descending: true},
		},
	}
	if !reflect.DeepEqual(index, want) {
		t.Fatalf("index = %#v, want %#v", index, want)
	}

	catalog.nullsNotDistinct = true
	if _, err := postgresSourceIndexFromCatalog(table, catalog); err == nil {
		t.Fatal("NULLS NOT DISTINCT index unexpectedly succeeded")
	}
	catalog.nullsNotDistinct = false
	catalog.columns[0].nullsFirst = false
	if _, err := postgresSourceIndexFromCatalog(table, catalog); err == nil {
		t.Fatal("noncanonical NULL ordering unexpectedly succeeded")
	}
}

func TestPostgresSourceForeignKeyFromCatalog(t *testing.T) {
	table := schema.Table{
		Schema: "source",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "tenant_id", Type: "bigint"},
			{Name: "account_id", Type: "bigint"},
		},
	}
	catalog := postgresSourceForeignKeyCatalog{
		objectID:         101,
		name:             "events_account_fkey",
		validated:        true,
		noInherit:        true,
		local:            true,
		onUpdate:         "c",
		onDelete:         "r",
		match:            "s",
		referencedSchema: "source",
		referencedTable:  "accounts",
		columns: []postgresSourceForeignKeyColumn{
			{local: "tenant_id", referenced: "tenant_id"},
			{local: "account_id", referenced: "id"},
		},
	}
	foreignKey, err := postgresSourceForeignKeyFromCatalog(table, catalog)
	if err != nil {
		t.Fatal(err)
	}
	want := schema.ForeignKey{
		Name:              "events_account_fkey",
		Columns:           []string{"tenant_id", "account_id"},
		ReferencedTable:   "accounts",
		ReferencedColumns: []string{"tenant_id", "id"},
		OnUpdate:          "CASCADE",
		OnDelete:          "RESTRICT",
		Match:             "SIMPLE",
	}
	if !reflect.DeepEqual(foreignKey, want) {
		t.Fatalf("foreign key = %#v, want %#v", foreignKey, want)
	}
	catalog.deferrable = true
	if _, err := postgresSourceForeignKeyFromCatalog(
		table,
		catalog,
	); err == nil {
		t.Fatal("deferrable foreign key unexpectedly succeeded")
	}
	catalog.deferrable = false
	catalog.referencedSchema = "other"
	if _, err := postgresSourceForeignKeyFromCatalog(
		table,
		catalog,
	); err == nil {
		t.Fatal("cross-schema foreign key unexpectedly succeeded")
	}
}
