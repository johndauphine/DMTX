package engine

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func validClickHouseSourceCatalog() (
	clickHouseSourceTableCatalog,
	[]clickHouseSourceColumnCatalog,
) {
	columns := []clickHouseSourceColumnCatalog{
		{
			position:     1,
			name:         "tenant_id",
			rawType:      "Int64",
			inSortingKey: 1,
			inPrimaryKey: 1,
		},
		{
			position:     2,
			name:         "event_id",
			rawType:      "Int64",
			inSortingKey: 1,
			inPrimaryKey: 1,
		},
		{
			position: 3,
			name:     "score",
			rawType:  "Nullable(Float64)",
		},
		{
			position: 4,
			name:     "note",
			rawType:  "Nullable(String)",
		},
		{
			position: 5,
			name:     "payload",
			rawType:  "String",
		},
	}
	engineFull := "MergeTree ORDER BY (tenant_id, event_id) " +
		"SETTINGS index_granularity = 8192"
	definitions := make([]string, len(columns))
	for index, column := range columns {
		definitions[index] = clickHouseCatalogIdentifier(column.name) +
			" " + column.rawType
	}
	return clickHouseSourceTableCatalog{
		engine:        "MergeTree",
		engineFull:    engineFull,
		sortingKey:    "tenant_id, event_id",
		primaryKey:    "tenant_id, event_id",
		storagePolicy: "default",
		createTableQuery: "CREATE TABLE analytics.events (" +
			strings.Join(definitions, ", ") + ") ENGINE = " + engineFull,
	}, columns
}

func TestValidateClickHouse248SourceVersion(t *testing.T) {
	for _, version := range []string{"24.8.0.1", "24.8.14.39"} {
		if err := validateClickHouse248Version(
			version,
			"source",
		); err != nil {
			t.Fatalf("version %s: %v", version, err)
		}
	}
	for _, version := range []string{"24.7.9.1", "24.9.1.1", "25.8.1.1"} {
		if err := validateClickHouse248Version(
			version,
			"source",
		); err == nil || !strings.Contains(err.Error(), "unsupported") {
			t.Fatalf("version %s error = %v", version, err)
		}
	}
}

func TestClickHouseSourceCatalogPreservesOrderingWithoutRelationalKey(
	t *testing.T,
) {
	tableCatalog, columnCatalogs := validClickHouseSourceCatalog()
	table, err := clickHouseSourceTableFromCatalog(
		"analytics",
		"events",
		tableCatalog,
		columnCatalogs,
		0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if table.Schema != "analytics" || table.Name != "events" {
		t.Fatalf("table identity = %#v", table)
	}
	if want := []string{"tenant_id", "event_id"}; !reflect.DeepEqual(
		table.ClickHouseOrderBy,
		want,
	) {
		t.Fatalf(
			"ClickHouse order = %v, want %v",
			table.ClickHouseOrderBy,
			want,
		)
	}
	wantColumns := []schema.Column{
		{Name: "tenant_id", Type: "bigint"},
		{Name: "event_id", Type: "bigint"},
		{Name: "score", Type: "double", Nullable: true},
		{Name: "note", Type: "text", Nullable: true},
		{Name: "payload", Type: "text"},
	}
	if !reflect.DeepEqual(table.Columns, wantColumns) {
		t.Fatalf("columns = %#v, want %#v", table.Columns, wantColumns)
	}
	for _, column := range table.Columns {
		if column.PrimaryKey || column.PrimaryKeyPosition != 0 {
			t.Fatalf(
				"ClickHouse ordering was mislabeled as relational key: %#v",
				column,
			)
		}
	}
}

func TestClickHouseSourceCatalogFailsClosedOnTableShapes(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*clickHouseSourceTableCatalog)
		skippingIndex uint64
		want          string
	}{
		{
			name: "engine",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.engine = "ReplacingMergeTree"
			},
			want: "table engine",
		},
		{
			name: "partition",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.partitionKey = "tenant_id"
			},
			want: "partition key",
		},
		{
			name: "sampling",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.samplingKey = "tenant_id"
			},
			want: "sampling key",
		},
		{
			name: "empty order",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.sortingKey = ""
				value.primaryKey = ""
			},
			want: "empty ordering key",
		},
		{
			name: "expression order",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.sortingKey = "cityHash64(tenant_id)"
				value.primaryKey = value.sortingKey
			},
			want: "ordering key",
		},
		{
			name: "different sparse primary key",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.primaryKey = "tenant_id"
			},
			want: "sparse primary key",
		},
		{
			name: "settings",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.engineFull += ", index_granularity_bytes = 0"
			},
			want: "MergeTree settings",
		},
		{
			name: "explicit sparse primary key",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.engineFull = "MergeTree PRIMARY KEY tenant_id " +
					"ORDER BY (tenant_id, event_id) " +
					"SETTINGS index_granularity = 8192"
				value.createTableQuery = strings.Replace(
					value.createTableQuery,
					"ENGINE = MergeTree",
					"ENGINE = MergeTree PRIMARY KEY tenant_id",
					1,
				)
			},
			want: "MergeTree settings",
		},
		{
			name: "storage",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.storagePolicy = "cold"
			},
			want: "storage policy",
		},
		{
			name: "comment",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.comment = "important"
			},
			want: "table comment",
		},
		{
			name: "dependency",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.dependenciesDatabase = []string{"analytics"}
				value.dependenciesTable = []string{"events_mv"}
			},
			want: "dependencies",
		},
		{
			name:          "skipping index",
			mutate:        func(*clickHouseSourceTableCatalog) {},
			skippingIndex: 1,
			want:          "data-skipping indexes",
		},
		{
			name: "projection or constraint definition",
			mutate: func(value *clickHouseSourceTableCatalog) {
				value.createTableQuery = strings.Replace(
					value.createTableQuery,
					") ENGINE",
					", PROJECTION by_score (SELECT * ORDER BY score)) ENGINE",
					1,
				)
			},
			want: "table definition",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableCatalog, columns := validClickHouseSourceCatalog()
			test.mutate(&tableCatalog)
			_, err := clickHouseSourceTableFromCatalog(
				"analytics",
				"events",
				tableCatalog,
				columns,
				test.skippingIndex,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"error = %T %v, want policy containing %q",
					err,
					err,
					test.want,
				)
			}
		})
	}
}

func TestClickHouseSourceCatalogFailsClosedOnColumnShapes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func([]clickHouseSourceColumnCatalog)
		want   string
	}{
		{
			name: "position",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[1].position = 3
			},
			want: "column position",
		},
		{
			name: "unsupported type",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[2].rawType = "Decimal(18, 2)"
			},
			want: "column type",
		},
		{
			name: "default",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[2].defaultKind = "DEFAULT"
				columns[2].defaultExpression = "0"
			},
			want: "default",
		},
		{
			name: "codec",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[2].compressionCodec = "ZSTD(3)"
			},
			want: "compression codec",
		},
		{
			name: "comment",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[2].comment = "score"
			},
			want: "column comment",
		},
		{
			name: "membership mismatch",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[0].inPrimaryKey = 0
			},
			want: "key membership",
		},
		{
			name: "nullable order",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[0].rawType = "Nullable(Int64)"
			},
			want: "nullable ordering",
		},
		{
			name: "floating order",
			mutate: func(columns []clickHouseSourceColumnCatalog) {
				columns[0].rawType = "Float64"
			},
			want: "floating-point ordering",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableCatalog, columns := validClickHouseSourceCatalog()
			test.mutate(columns)
			definitions := make([]string, len(columns))
			for index, column := range columns {
				definitions[index] = clickHouseCatalogIdentifier(column.name) +
					" " + column.rawType
			}
			tableCatalog.createTableQuery =
				"CREATE TABLE analytics.events (" +
					strings.Join(definitions, ", ") +
					") ENGINE = " + tableCatalog.engineFull
			_, err := clickHouseSourceTableFromCatalog(
				"analytics",
				"events",
				tableCatalog,
				columns,
				0,
			)
			var policy *schema.PolicyError
			if !errors.As(err, &policy) ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"error = %T %v, want policy containing %q",
					err,
					err,
					test.want,
				)
			}
		})
	}
}

func TestParseClickHouseDirectColumnKey(t *testing.T) {
	got, err := parseClickHouseDirectColumnKey(
		"tenant_id, `event``id`",
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"tenant_id", "event`id"}; !reflect.DeepEqual(
		got,
		want,
	) {
		t.Fatalf("parsed key = %v, want %v", got, want)
	}
	for _, value := range []string{
		"",
		"tuple()",
		"cityHash64(tenant_id)",
		"tenant_id DESC",
		"tenant_id, tenant_id + 1",
		"`unterminated",
	} {
		if _, err := parseClickHouseDirectColumnKey(value); err == nil {
			t.Fatalf("unsafe ordering key %q was accepted", value)
		}
	}
}
