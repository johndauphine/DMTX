package migrate

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestOpenSQLiteSourceAdapterRejectsInvalidPathsWithoutCreation(
	t *testing.T,
) {
	t.Run("missing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "missing source.db")
		_, err := openSQLiteSourceAdapter(
			context.Background(),
			config.Endpoint{Type: "sqlite", Database: path},
		)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error = %v, want missing-file error", err)
		}
		if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
			t.Fatalf("missing source was created: %v", statErr)
		}
	})

	t.Run("directory", func(t *testing.T) {
		path := t.TempDir()
		_, err := openSQLiteSourceAdapter(
			context.Background(),
			config.Endpoint{Type: "sqlite", Database: path},
		)
		if err == nil || !strings.Contains(err.Error(), "is a directory") {
			t.Fatalf("directory error = %v", err)
		}
	})
}

func TestValidateSQLiteSourceEndpointChecksConfigurationOnly(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing source.db")
	if err := validateSQLiteSourceEndpoint(config.Endpoint{
		Type:     "sqlite",
		Database: missing,
	}); err != nil {
		t.Fatalf("missing but valid path: %v", err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validation created missing source: %v", err)
	}

	directory := t.TempDir()
	if err := validateSQLiteSourceEndpoint(config.Endpoint{
		Type:     "sqlite",
		Database: directory,
	}); err != nil {
		t.Fatalf("directory syntax: %v", err)
	}

	for _, test := range []struct {
		name     string
		database string
		want     string
	}{
		{
			name: "empty",
			want: "SQLite database path is required",
		},
		{
			name:     "URI",
			database: "file:source.db?mode=ro",
			want:     "SQLite URI database paths are unsupported",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateSQLiteSourceEndpoint(config.Endpoint{
				Type:     "sqlite",
				Database: test.database,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestSQLiteSourceAdapterIsReadOnlyAndListsOnlyOrdinaryMainTables(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "source # % & +.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE "z table" (id INTEGER PRIMARY KEY);
		CREATE TABLE alpha (id INTEGER PRIMARY KEY);
		CREATE VIRTUAL TABLE search USING fts5(content);
	`)

	adapter := openSQLiteSourceAdapterForTest(t, path)
	if adapter.Engine() != "sqlite" || adapter.DisplayName() != "SQLite" {
		t.Fatalf(
			"adapter identity = (%q, %q)",
			adapter.Engine(),
			adapter.DisplayName(),
		)
	}
	names, err := adapter.ListTables(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"alpha", "z table"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("tables = %#v, want %#v", names, want)
	}
	if _, err := adapter.database.Exec(
		`CREATE TABLE source_was_mutated (id INTEGER PRIMARY KEY)`,
	); err == nil {
		t.Fatal("read-only SQLite source accepted a write")
	}
}

func TestSQLiteSourceAdapterOrdersQuotedCompositeKeysAndProjectsNumerics(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "rows.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE "odd""table" (
			"tenant""id" TEXT NOT NULL,
			"seq?" INTEGER NOT NULL,
			amount DECIMAL(12,2),
			payload BLOB,
			PRIMARY KEY ("seq?", "tenant""id")
		);
		INSERT INTO "odd""table" VALUES
			('b', 1, 12.50, X'FF00'),
			('a', 2, NULL, X''),
			('a', 1, 9007199254740993, X'0102'),
			('a', 3, 0.00, NULL);
	`)

	adapter := openSQLiteSourceAdapterForTest(t, path)
	table, err := adapter.InspectTable(context.Background(), `odd"table`)
	if err != nil {
		t.Fatal(err)
	}
	columns := []string{`tenant"id`, `seq?`, "amount", "payload"}
	stream, err := adapter.OpenRows(context.Background(), table, columns)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	type observedRow struct {
		tenant  string
		seq     int64
		amount  any
		payload any
	}
	var observed []observedRow
	var firstPayload []byte
	for stream.Next() {
		values := make([]any, len(columns))
		destinations := make([]any, len(columns))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := stream.Scan(destinations...); err != nil {
			t.Fatal(err)
		}
		row := observedRow{
			tenant:  values[0].(string),
			seq:     values[1].(int64),
			amount:  values[2],
			payload: values[3],
		}
		if len(observed) == 0 {
			firstPayload = row.payload.([]byte)
		}
		observed = append(observed, row)
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}

	if len(observed) != 4 {
		t.Fatalf("rows = %#v", observed)
	}
	gotOrder := []string{
		observed[0].tenant,
		observed[1].tenant,
		observed[2].tenant,
		observed[3].tenant,
	}
	gotSequences := []int64{
		observed[0].seq,
		observed[1].seq,
		observed[2].seq,
		observed[3].seq,
	}
	if !reflect.DeepEqual(gotOrder, []string{"a", "b", "a", "a"}) ||
		!reflect.DeepEqual(gotSequences, []int64{1, 1, 2, 3}) {
		t.Fatalf("ordered keys = %#v / %#v", gotOrder, gotSequences)
	}
	if observed[0].amount != "9007199254740993" ||
		observed[1].amount != "12.5" ||
		observed[2].amount != nil ||
		observed[3].amount != "0" {
		t.Fatalf("numeric projections = %#v", observed)
	}
	if !bytes.Equal(firstPayload, []byte{1, 2}) {
		t.Fatalf("first BLOB changed after later scans: %#v", firstPayload)
	}
	empty, ok := observed[2].payload.([]byte)
	if !ok || len(empty) != 0 {
		t.Fatalf("empty BLOB = %#v (%T)", observed[2].payload, observed[2].payload)
	}
	if observed[3].payload != nil {
		t.Fatalf("NULL BLOB = %#v", observed[3].payload)
	}

	count, err := adapter.CountRows(context.Background(), table)
	if err != nil || count != 4 {
		t.Fatalf("count = %d, error = %v", count, err)
	}
}

func TestSQLiteSourceAdapterProjectsJSONAsTextAndPreservesNull(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "json.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE documents (
			id INTEGER PRIMARY KEY,
			payload JSON
		);
		INSERT INTO documents VALUES
			(1, 123),
			(2, '{"kind":"object"}'),
			(3, '[1,2,3]'),
			(4, NULL);
	`)

	adapter := openSQLiteSourceAdapterForTest(t, path)
	table, err := adapter.InspectTable(
		context.Background(),
		"documents",
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := adapter.OpenRows(
		context.Background(),
		table,
		[]string{"id", "payload"},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	var payloads []any
	for stream.Next() {
		var id any
		var payload any
		if err := stream.Scan(&id, &payload); err != nil {
			t.Fatal(err)
		}
		payloads = append(payloads, payload)
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	want := []any{
		"123",
		`{"kind":"object"}`,
		"[1,2,3]",
		nil,
	}
	if !reflect.DeepEqual(payloads, want) {
		t.Fatalf("JSON payloads = %#v, want %#v", payloads, want)
	}

	for index, payload := range payloads[:3] {
		if _, err := normalizePostgresValue("json", payload); err != nil {
			t.Fatalf(
				"normalize projected JSON row %d: %v",
				index+1,
				err,
			)
		}
	}
	if _, err := normalizePostgresValue(
		"json",
		"not valid JSON",
	); err == nil {
		t.Fatal("strict PostgreSQL JSON normalization accepted invalid JSON")
	}
}

func TestSQLiteSourceAdapterAppliesSchemaSafetyChecks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "schema.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE safe (
			id INTEGER PRIMARY KEY,
			amount DECIMAL(12,2) DEFAULT 0,
			CHECK (amount >= 0)
		);
		CREATE TABLE triggered (id INTEGER PRIMARY KEY, value TEXT);
		CREATE TRIGGER triggered_after_insert AFTER INSERT ON triggered
		BEGIN
			UPDATE triggered SET value = NEW.value WHERE id = NEW.id;
		END;
		CREATE TABLE unsafe (
			tenant TEXT,
			item_id TEXT,
			PRIMARY KEY (tenant, item_id)
		);
	`)

	adapter := openSQLiteSourceAdapterForTest(t, path)
	table, err := adapter.InspectTable(context.Background(), "safe")
	if err != nil {
		t.Fatal(err)
	}
	if len(table.Columns) != 2 ||
		table.Columns[1].DeclaredType == nil ||
		!reflect.DeepEqual(table.Columns[1].DeclaredType.Arguments, []int{12, 2}) ||
		table.Columns[1].Default == nil ||
		len(table.Checks) != 1 {
		t.Fatalf("safe schema = %#v", table)
	}

	_, err = adapter.InspectTable(context.Background(), "triggered")
	var policyError *schema.PolicyError
	if !errors.As(err, &policyError) ||
		policyError.Operation != "discover SQLite table trigger" {
		t.Fatalf("trigger error = %v", err)
	}

	_, err = adapter.InspectTable(context.Background(), "unsafe")
	if ClassifyTransferError(err) != ErrorClassPrimaryKey ||
		!strings.Contains(err.Error(), "cannot prove deterministic") {
		t.Fatalf("nullable primary-key error = %v", err)
	}
}

func TestSQLiteSourceAdapterCountErrorsAreContextualAndCloseIsEffective(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "count.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE items (id INTEGER PRIMARY KEY);
		INSERT INTO items VALUES (1);
	`)
	adapter := openSQLiteSourceAdapterForTest(t, path)
	table, err := adapter.InspectTable(context.Background(), "items")
	if err != nil {
		t.Fatal(err)
	}
	if count, err := adapter.CountRows(context.Background(), table); err != nil ||
		count != 1 {
		t.Fatalf("count = %d, error = %v", count, err)
	}
	if _, err := adapter.CountRows(
		context.Background(),
		schema.Table{Name: "missing"},
	); err == nil || !strings.Contains(err.Error(), "count SQLite table missing") {
		t.Fatalf("missing-table count error = %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.CountRows(
		context.Background(),
		table,
	); err == nil || !strings.Contains(err.Error(), "count SQLite table items") {
		t.Fatalf("closed count error = %v", err)
	}
}

func TestSQLiteSourceAdapterPinsWALSnapshotAcrossSQLServerPreflightAndCopy(
	t *testing.T,
) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wal-snapshot.db")
	createSQLiteSourceTestDatabase(t, path, `
		CREATE TABLE items (
			id BIGINT NOT NULL PRIMARY KEY,
			amount DECIMAL(18,0) NOT NULL
		);
		INSERT INTO items VALUES (1, 7);
	`)

	writer, err := sql.Open("sqlite", sqliteSourceTestURI(path, "rw"))
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var journalMode string
	if err := writer.QueryRowContext(
		ctx,
		`PRAGMA journal_mode = WAL`,
	).Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("journal mode = %q, want WAL", journalMode)
	}

	adapter := openSQLiteSourceAdapterForTest(t, path)
	sourceTable, err := adapter.InspectTable(ctx, "items")
	if err != nil {
		t.Fatal(err)
	}
	targetTable := sourceTable
	targetTable.Schema = "dbo"
	targetTable.Columns = append(
		[]schema.Column(nil),
		sourceTable.Columns...,
	)
	targetTable.Columns[1].Type = "numeric"
	plan := adapterTablePlan{
		source:  sourceTable,
		target:  targetTable,
		columns: []string{"id", "amount"},
	}
	if err := preflightSQLiteSQLServerSourceData(
		ctx,
		adapter,
		[]adapterTablePlan{plan},
	); err != nil {
		t.Fatalf("initial SQL Server source preflight: %v", err)
	}

	if _, err := writer.ExecContext(ctx, `
		UPDATE items SET amount = 1.5 WHERE id = 1;
		INSERT INTO items VALUES (2, 8);
	`); err != nil {
		t.Fatalf("concurrent WAL writer: %v", err)
	}
	var writerCount int
	var writerStorage string
	if err := writer.QueryRowContext(
		ctx,
		`SELECT
			(SELECT COUNT(*) FROM items),
			typeof(amount)
		 FROM items
		 WHERE id = 1`,
	).Scan(&writerCount, &writerStorage); err != nil {
		t.Fatal(err)
	}
	if writerCount != 2 || writerStorage != "real" {
		t.Fatalf(
			"writer state = (%d, %q), want (2, real)",
			writerCount,
			writerStorage,
		)
	}

	// A second preflight proves the raw typeof(...) probes use the same
	// transaction as row reads instead of escaping through the database pool.
	if err := preflightSQLiteSQLServerSourceData(
		ctx,
		adapter,
		[]adapterTablePlan{plan},
	); err != nil {
		t.Fatalf("snapshot SQL Server source preflight: %v", err)
	}

	rows, err := adapter.OpenRows(
		ctx,
		sourceTable,
		plan.columns,
	)
	if err != nil {
		t.Fatal(err)
	}
	var copied [][]any
	for rows.Next() {
		var id any
		var amount any
		if err := rows.Scan(&id, &amount); err != nil {
			_ = rows.Close()
			t.Fatal(err)
		}
		copied = append(copied, []any{id, amount})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if want := [][]any{{int64(1), "7"}}; !reflect.DeepEqual(copied, want) {
		t.Fatalf("copied snapshot rows = %#v, want %#v", copied, want)
	}
	count, err := adapter.CountRows(ctx, sourceTable)
	if err != nil || count != 1 {
		t.Fatalf("snapshot count = %d, error = %v", count, err)
	}
}

func openSQLiteSourceAdapterForTest(
	t *testing.T,
	path string,
) *sqliteSourceAdapter {
	t.Helper()
	source, err := openSQLiteSourceAdapter(
		context.Background(),
		config.Endpoint{Type: "sqlite", Database: path},
	)
	if err != nil {
		t.Fatal(err)
	}
	adapter := source.(*sqliteSourceAdapter)
	t.Cleanup(func() {
		_ = adapter.Close()
	})
	return adapter
}

func createSQLiteSourceTestDatabase(
	t *testing.T,
	path string,
	statement string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", sqliteSourceTestURI(path, "rwc"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(statement); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func sqliteSourceTestURI(path, mode string) string {
	normalized := filepath.ToSlash(path)
	if runtime.GOOS == "windows" && !strings.HasPrefix(normalized, "/") {
		normalized = "/" + normalized
	}
	location := url.URL{Scheme: "file", Path: normalized}
	query := location.Query()
	query.Set("mode", mode)
	location.RawQuery = query.Encode()
	return location.String()
}
