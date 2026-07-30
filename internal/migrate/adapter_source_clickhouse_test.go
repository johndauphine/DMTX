package migrate

import (
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func clickHouseSourceTestTable() schema.Table {
	return schema.Table{
		Schema:            "source",
		Name:              "events",
		ClickHouseOrderBy: []string{"tenant_id", "event_id"},
		Columns: []schema.Column{
			{Name: "payload", Type: "text"},
			{Name: "tenant_id", Type: "bigint"},
			{Name: "event_id", Type: "bigint"},
			{Name: "score", Type: "double", Nullable: true},
			{Name: "note", Type: "text", Nullable: true},
		},
	}
}

func TestClickHouseSourceRebuildOrderUsesAllColumnsAsTieBreakers(
	t *testing.T,
) {
	table := clickHouseSourceTestTable()
	order, err := clickHouseRebuildRowOrder(table)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"tenant_id",
		"event_id",
		"payload",
		"score",
		"note",
	}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("row order = %v, want %v", order, want)
	}
	for _, column := range table.Columns {
		if column.PrimaryKey || column.PrimaryKeyPosition != 0 {
			t.Fatalf("ordering became relational uniqueness: %#v", column)
		}
	}
}

func TestClickHouseSourceReadQueryPreservesProjectionAndTotalOrder(
	t *testing.T,
) {
	table := clickHouseSourceTestTable()
	query, err := clickHouseSourceReadQuery(
		table,
		adapterColumnNames(table),
	)
	if err != nil {
		t.Fatal(err)
	}
	const want = `SELECT "payload", "tenant_id", "event_id", "score", ` +
		`"note" FROM "source"."events" ORDER BY "tenant_id", ` +
		`"event_id", "payload", "score", "note"`
	if query != want {
		t.Fatalf("query:\n got: %s\nwant: %s", query, want)
	}
	if _, err := clickHouseSourceReadQuery(
		table,
		[]string{"tenant_id"},
	); err == nil || !strings.Contains(err.Error(), "source column order") {
		t.Fatalf("partial projection error = %v", err)
	}
}

func TestClickHouseSourceOrderFailsClosed(t *testing.T) {
	base := clickHouseSourceTestTable()
	tests := []struct {
		name   string
		mutate func(*schema.Table)
	}{
		{
			name: "missing key",
			mutate: func(table *schema.Table) {
				table.ClickHouseOrderBy = nil
			},
		},
		{
			name: "unknown key",
			mutate: func(table *schema.Table) {
				table.ClickHouseOrderBy = []string{"missing"}
			},
		},
		{
			name: "duplicate key",
			mutate: func(table *schema.Table) {
				table.ClickHouseOrderBy = []string{
					"tenant_id",
					"tenant_id",
				}
			},
		},
		{
			name: "duplicate column",
			mutate: func(table *schema.Table) {
				table.Columns[1].Name = table.Columns[0].Name
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := base
			table.Columns = append([]schema.Column(nil), base.Columns...)
			table.ClickHouseOrderBy = append(
				[]string(nil),
				base.ClickHouseOrderBy...,
			)
			test.mutate(&table)
			if _, err := clickHouseRebuildRowOrder(table); err == nil {
				t.Fatalf("unsafe row order accepted: %#v", table)
			}
		})
	}
}

func TestAdapterRunnerAdmitsNonUniqueClickHouseOrderForRebuildOnly(
	t *testing.T,
) {
	source := &clickHouseSourceAdapter{}
	table := clickHouseSourceTestTable()
	if err := requireAdapterSourceRowOrder(
		source,
		table,
		"drop_recreate",
	); err != nil {
		t.Fatalf("admit ClickHouse rebuild order: %v", err)
	}
	if err := requireAdapterSourceRowOrder(
		source,
		table,
		"upsert",
	); err == nil || !strings.Contains(err.Error(), "primary key") {
		t.Fatalf("upsert order error = %v", err)
	}
}

type scriptedRebuildOrderSource struct {
	*recordingAdapterSource
	order []string
}

func (source *scriptedRebuildOrderSource) RebuildRowOrder(
	schema.Table,
) ([]string, error) {
	return append([]string(nil), source.order...), nil
}

func TestAdapterRunnerRequiresCompleteRebuildRowOrder(t *testing.T) {
	table := schema.Table{
		Name: "events",
		Columns: []schema.Column{
			{Name: "tenant_id"},
			{Name: "event_id"},
		},
	}
	tests := []struct {
		name  string
		order []string
		want  string
	}{
		{
			name:  "missing",
			order: []string{"tenant_id"},
			want:  "1 columns for a 2-column",
		},
		{
			name:  "unknown",
			order: []string{"tenant_id", "missing"},
			want:  "unknown column",
		},
		{
			name:  "duplicate",
			order: []string{"tenant_id", "tenant_id"},
			want:  "duplicate column",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &scriptedRebuildOrderSource{
				recordingAdapterSource: &recordingAdapterSource{},
				order:                  test.order,
			}
			err := requireAdapterSourceRowOrder(
				source,
				table,
				"drop_recreate",
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestValidateClickHouseSourceEndpoint(t *testing.T) {
	valid := config.Endpoint{
		Host:     "127.0.0.1",
		Database: "analytics",
		User:     "dmtx",
		Schema:   "analytics",
	}
	if err := validateClickHouseSourceEndpoint(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*config.Endpoint)
		want   string
	}{
		{
			name: "missing host",
			mutate: func(endpoint *config.Endpoint) {
				endpoint.Host = ""
			},
			want: "host, database, and user",
		},
		{
			name: "reserved database",
			mutate: func(endpoint *config.Endpoint) {
				endpoint.Database = "SYSTEM"
				endpoint.Schema = ""
			},
			want: "reserved system database",
		},
		{
			name: "schema mismatch",
			mutate: func(endpoint *config.Endpoint) {
				endpoint.Schema = "other"
			},
			want: "schema must be empty or match",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint := valid
			test.mutate(&endpoint)
			if err := validateClickHouseSourceEndpoint(endpoint); err == nil ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClickHouseSourceAdapterContractMethods(t *testing.T) {
	adapter := &clickHouseSourceAdapter{}
	if adapter.Engine() != "clickhouse" ||
		adapter.DisplayName() != "ClickHouse" {
		t.Fatalf(
			"source identity = %s/%s",
			adapter.Engine(),
			adapter.DisplayName(),
		)
	}
	var _ sourceAdapter = adapter
	var _ adapterSourceRebuildRowOrderer = adapter
}
