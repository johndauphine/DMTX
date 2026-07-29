package migrate

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
)

func TestSQLiteSourceAdapterProjectsTemporalDeclarationsAsText(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "temporal.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE temporal_values (
			tenant TEXT NOT NULL,
			id INTEGER NOT NULL,
			event_date DATE,
			local_datetime DATETIME,
			local_timestamp TIMESTAMP,
			PRIMARY KEY (tenant, id)
		);
		INSERT INTO temporal_values VALUES
			(
				'alpha',
				1,
				'2026-07-29',
				'2026-07-29 12:34:56.123456',
				'2026-07-29T12:34:56.123456-05:00'
			),
			('beta', 2, NULL, NULL, NULL);
	`)

	adapter := openSQLiteSourceAdapterForTest(t, path)
	table, err := adapter.InspectTable(
		context.Background(),
		"temporal_values",
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.OpenRows(
		context.Background(),
		table,
		[]string{
			"tenant",
			"id",
			"event_date",
			"local_datetime",
			"local_timestamp",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	var rows [][]any
	for stream.Next() {
		values := make([]any, 5)
		destinations := make([]any, len(values))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := stream.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, values)
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	want := [][]any{
		{
			"alpha",
			int64(1),
			"2026-07-29",
			"2026-07-29 12:34:56.123456",
			"2026-07-29T12:34:56.123456-05:00",
		},
		{
			"beta",
			int64(2),
			nil,
			nil,
			nil,
		},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("temporal rows = %#v, want %#v", rows, want)
	}

	if _, err := normalizePostgresValue(
		"timestamp",
		rows[0][3],
	); err != nil {
		t.Fatalf("normalize zone-less SQLite datetime: %v", err)
	}
	if _, err := normalizePostgresValue(
		"timestamp",
		rows[0][4],
	); err == nil {
		t.Fatal("offset SQLite timestamp unexpectedly lost its zone")
	}
}
