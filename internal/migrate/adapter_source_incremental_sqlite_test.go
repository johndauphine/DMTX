package migrate

import (
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLiteIncrementalWindow(t *testing.T) {
	path := t.TempDir() + "/incremental.db"
	writer, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	if _, err := writer.Exec(`
		PRAGMA journal_mode = WAL;
		CREATE TABLE events (
			tenant_id INTEGER NOT NULL,
			id INTEGER NOT NULL,
			updated_at TIMESTAMP(3),
			payload TEXT,
			PRIMARY KEY (tenant_id, id)
		);
		INSERT INTO events VALUES
			(1, 1, NULL, 'null'),
			(1, 2, '2026-07-30 12:00:00.000', 'equal lower'),
			(1, 3, '2026-07-30 12:00:01.000', 'inside'),
			(1, 4, '2026-07-30 12:00:02.000', 'equal upper');
		CREATE TABLE empty_events (
			id INTEGER NOT NULL PRIMARY KEY,
			updated_at TIMESTAMP(3),
			payload TEXT
		);
		INSERT INTO empty_events VALUES (1, NULL, 'null only');
		CREATE TABLE bad_events (
			id INTEGER NOT NULL PRIMARY KEY,
			updated_at TIMESTAMP(3)
		);
		INSERT INTO bad_events VALUES
			(1, '2026-07-30 12:00:00.12');
		CREATE TABLE malformed_nonmax_events (
			id INTEGER NOT NULL PRIMARY KEY,
			updated_at TIMESTAMP(3)
		);
		INSERT INTO malformed_nonmax_events VALUES
			(1, '0000-01-01 00:00:00.000'),
			(2, '2025-01-01 24:00:00.000'),
			(3, '2026-07-30 12:00:00.000');
		CREATE TABLE malformed_nonmax_dates (
			id INTEGER NOT NULL PRIMARY KEY,
			updated_at DATE
		);
		INSERT INTO malformed_nonmax_dates VALUES
			(1, '0000-01-01'),
			(2, '2026-07-30');
		CREATE TABLE nanosecond_events (
			id INTEGER NOT NULL PRIMARY KEY,
			updated_at TIMESTAMP(9),
			payload TEXT
		);
		INSERT INTO nanosecond_events VALUES
			(1, '2026-07-30 12:00:00.123456788', 'lower'),
			(2, '2026-07-30 12:00:00.123456789', 'upper');
	`); err != nil {
		t.Fatal(err)
	}

	source, err := openSQLiteSourceAdapter(
		context.Background(),
		config.Endpoint{Database: path},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := source.Close(); err != nil {
			t.Errorf("close SQLite incremental source: %v", err)
		}
	}()
	incremental, err := requireIncrementalSourceAdapter(source)
	if err != nil {
		t.Fatal(err)
	}
	table, err := source.InspectTable(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := incremental.IncrementalTable(table)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildIncrementalTablePlan(
		mapped,
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.DateColumn == nil {
		t.Fatal("SQLite timestamp was not admitted")
	}
	upper, err := incremental.SampleIncrementalUpperFence(
		context.Background(),
		table,
		*plan.DateColumn,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantUpper := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		2,
		0,
		time.UTC,
	)
	if upper == nil || !upper.Equal(wantUpper) {
		t.Fatalf("upper fence = %v, want %v", upper, wantUpper)
	}

	// This write occurs after the immutable upper fence and after the source
	// read-only snapshot was established. Neither fact may allow it into the
	// current attempt.
	if _, err := writer.Exec(`
		INSERT INTO events VALUES
			(1, 5, '2026-07-30 12:00:03.000', 'after fence')
	`); err != nil {
		t.Fatal(err)
	}
	lower := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		0,
		time.UTC,
	)
	read := IncrementalReadPlan{
		Table:    mapped,
		Scope:    IncrementalReadWindow,
		Ordering: windowIncrementalOrdering(plan.Ordering),
		Window: &IncrementalWindow{
			Column:         "updated_at",
			Lower:          &lower,
			Upper:          upper,
			LowerExclusive: true,
			UpperInclusive: true,
			ExcludeNull:    true,
		},
		Resumed:                  true,
		ReplayFromLowerWatermark: true,
	}
	rows, err := incremental.OpenIncrementalRows(
		context.Background(),
		table,
		[]string{"tenant_id", "id", "updated_at", "payload"},
		read,
	)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	var timestamps []string
	for rows.Next() {
		var tenantID, id any
		var updatedAt, payload any
		if err := rows.Scan(
			&tenantID,
			&id,
			&updatedAt,
			&payload,
		); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		ids = append(ids, id.(int64))
		timestamps = append(timestamps, updatedAt.(string))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ids, []int64{3, 4}) {
		t.Fatalf(
			"strict (lower, upper] ids = %v, want [3 4]",
			ids,
		)
	}
	if !reflect.DeepEqual(
		timestamps,
		[]string{
			"2026-07-30 12:00:01.000",
			"2026-07-30 12:00:02.000",
		},
	) {
		t.Fatalf("projected timestamps = %#v", timestamps)
	}

	emptyRead := read
	emptyRead.Window = &IncrementalWindow{
		Column:         "updated_at",
		Lower:          upper,
		Upper:          upper,
		LowerExclusive: true,
		UpperInclusive: true,
		ExcludeNull:    true,
		Empty:          true,
	}
	rows, err = incremental.OpenIncrementalRows(
		context.Background(),
		table,
		[]string{"tenant_id", "id", "updated_at"},
		emptyRead,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		_ = rows.Close()
		t.Fatal("empty incremental fence returned a row")
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	nanosecondTable, err := source.InspectTable(
		context.Background(),
		"nanosecond_events",
	)
	if err != nil {
		t.Fatal(err)
	}
	nanosecondMapped, err := incremental.IncrementalTable(nanosecondTable)
	if err != nil {
		t.Fatal(err)
	}
	nanosecondPlan, err := BuildIncrementalTablePlan(
		nanosecondMapped,
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	nanosecondUpper, err := incremental.SampleIncrementalUpperFence(
		context.Background(),
		nanosecondTable,
		*nanosecondPlan.DateColumn,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantNanosecondUpper := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_456_789,
		time.UTC,
	)
	if nanosecondUpper == nil ||
		!nanosecondUpper.Equal(wantNanosecondUpper) {
		t.Fatalf(
			"precision-9 upper fence = %v, want %v",
			nanosecondUpper,
			wantNanosecondUpper,
		)
	}
	nanosecondLower := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_456_788,
		time.UTC,
	)
	rows, err = incremental.OpenIncrementalRows(
		context.Background(),
		nanosecondTable,
		[]string{"id", "updated_at", "payload"},
		IncrementalReadPlan{
			Table:    nanosecondMapped,
			Scope:    IncrementalReadWindow,
			Ordering: windowIncrementalOrdering(nanosecondPlan.Ordering),
			Window: &IncrementalWindow{
				Column:         "updated_at",
				Lower:          &nanosecondLower,
				Upper:          nanosecondUpper,
				LowerExclusive: true,
				UpperInclusive: true,
				ExcludeNull:    true,
			},
			Resumed:                  true,
			ReplayFromLowerWatermark: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		_ = rows.Close()
		t.Fatal("precision-9 window returned no row")
	}
	var nanosecondID, nanosecondValue, nanosecondPayload any
	if err := rows.Scan(
		&nanosecondID,
		&nanosecondValue,
		&nanosecondPayload,
	); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if nanosecondID != int64(2) ||
		nanosecondValue != "2026-07-30 12:00:00.123456789" ||
		nanosecondPayload != "upper" {
		_ = rows.Close()
		t.Fatalf(
			"precision-9 row = %#v %#v %#v",
			nanosecondID,
			nanosecondValue,
			nanosecondPayload,
		)
	}
	if rows.Next() {
		_ = rows.Close()
		t.Fatal("precision-9 window returned more than one row")
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}

	emptyTable, err := source.InspectTable(
		context.Background(),
		"empty_events",
	)
	if err != nil {
		t.Fatal(err)
	}
	emptyMapped, err := incremental.IncrementalTable(emptyTable)
	if err != nil {
		t.Fatal(err)
	}
	emptyPlan, err := BuildIncrementalTablePlan(
		emptyMapped,
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	emptyUpper, err := incremental.SampleIncrementalUpperFence(
		context.Background(),
		emptyTable,
		*emptyPlan.DateColumn,
	)
	if err != nil {
		t.Fatal(err)
	}
	if emptyUpper != nil {
		t.Fatalf("all-NULL upper fence = %v, want nil", emptyUpper)
	}

	badTable, err := source.InspectTable(
		context.Background(),
		"bad_events",
	)
	if err != nil {
		t.Fatal(err)
	}
	badMapped, err := incremental.IncrementalTable(badTable)
	if err != nil {
		t.Fatal(err)
	}
	badPlan, err := BuildIncrementalTablePlan(
		badMapped,
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := incremental.SampleIncrementalUpperFence(
		context.Background(),
		badTable,
		*badPlan.DateColumn,
	); err == nil ||
		!strings.Contains(err.Error(), "exact declared temporal shape") {
		t.Fatalf("invalid precision error = %v", err)
	}

	for _, name := range []string{
		"malformed_nonmax_events",
		"malformed_nonmax_dates",
	} {
		table, err := source.InspectTable(
			context.Background(),
			name,
		)
		if err != nil {
			t.Fatal(err)
		}
		mapped, err := incremental.IncrementalTable(table)
		if err != nil {
			t.Fatal(err)
		}
		plan, err := BuildIncrementalTablePlan(
			mapped,
			[]string{"updated_at"},
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := incremental.SampleIncrementalUpperFence(
			context.Background(),
			table,
			*plan.DateColumn,
		); ClassifyTransferError(err) != ErrorClassPolicy ||
			!strings.Contains(
				err.Error(),
				"exact declared temporal shape",
			) {
			t.Fatalf(
				"%s malformed non-max error = %v",
				name,
				err,
			)
		}
	}
}
