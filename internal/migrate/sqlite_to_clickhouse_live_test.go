package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	_ "modernc.org/sqlite"
)

type clickHouseLiveCompletion struct {
	table string
	rows  int
}

type clickHouseLiveObserver struct {
	tableSets [][]string
	before    []string
	after     []clickHouseLiveCompletion
}

func (observer *clickHouseLiveObserver) BeforeTables(
	_ context.Context,
	tables []string,
) error {
	observer.tableSets = append(
		observer.tableSets,
		append([]string(nil), tables...),
	)
	return nil
}

func (observer *clickHouseLiveObserver) BeforeTable(
	_ context.Context,
	table string,
) error {
	observer.before = append(observer.before, table)
	return nil
}

func (observer *clickHouseLiveObserver) AfterTable(
	_ context.Context,
	table string,
	rows int,
) error {
	observer.after = append(
		observer.after,
		clickHouseLiveCompletion{table: table, rows: rows},
	)
	return nil
}

func TestSQLiteToClickHouse248ComposedRouteLive(t *testing.T) {
	endpoint := clickHouseLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		90*time.Second,
	)
	defer cancel()

	target, err := engine.OpenClickHouse(ctx, endpoint)
	if err != nil {
		t.Fatalf("open live ClickHouse target: %v", err)
	}
	t.Cleanup(func() {
		if err := target.Close(); err != nil {
			t.Errorf("close live ClickHouse target: %v", err)
		}
	})
	if err := engine.VerifyClickHouse248Target(
		ctx,
		target,
		endpoint.Database,
	); err != nil {
		t.Fatal(err)
	}

	tableName := fmt.Sprintf(
		"dmtx_clickhouse_live_%d",
		time.Now().UnixNano(),
	)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		_, _ = target.ExecContext(
			cleanupCtx,
			"DROP TABLE IF EXISTS "+
				clickHouseQualified(endpoint.Database, tableName),
		)
	})

	sourcePath := filepath.Join(t.TempDir(), "source.db")
	createClickHouseLiveSQLiteSource(t, sourcePath, tableName)
	cfg := config.Config{
		Source: config.Endpoint{
			Type:     "sqlite",
			Database: sourcePath,
		},
		Target: endpoint,
		Migration: config.Migration{
			TargetMode: "drop_recreate",
		},
	}
	observer := &clickHouseLiveObserver{}
	result, err := Execute(ctx, cfg, observer)
	if err != nil {
		t.Fatalf("execute SQLite-to-ClickHouse route: %v", err)
	}
	if result.Tables != 1 ||
		result.Rows != sqliteWriteBatchSize+1 ||
		!result.Validated {
		t.Fatalf("result = %+v", result)
	}
	if !matchesClickHouseObserver(
		observer,
		tableName,
		sqliteWriteBatchSize+1,
	) {
		t.Fatalf("observer = %+v", observer)
	}
	assertClickHouseLiveSchemaAndRows(
		t,
		ctx,
		target,
		endpoint.Database,
		tableName,
	)

	// A second run must replace, rather than append to, the target.
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+
			clickHouseQualified(endpoint.Database, tableName)+
			" VALUES (?, ?, ?, ?, ?, ?)",
		int64(99),
		int64(99),
		int64(99),
		float64(99),
		"target-only",
		[]byte("target-only"),
	); err != nil {
		t.Fatalf("insert target-only ClickHouse row: %v", err)
	}
	result, err = Execute(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("rerun SQLite-to-ClickHouse route: %v", err)
	}
	if result.Rows != sqliteWriteBatchSize+1 || !result.Validated {
		t.Fatalf("rerun result = %+v", result)
	}
	var targetOnly int
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			clickHouseQualified(endpoint.Database, tableName)+
			" WHERE note = 'target-only'",
	).Scan(&targetOnly); err != nil {
		t.Fatalf("count target-only ClickHouse rows: %v", err)
	}
	if targetOnly != 0 {
		t.Fatalf("target-only rows after rebuild = %d", targetOnly)
	}

	// Existing non-MergeTree objects are rejected during read-only preflight.
	if _, err := target.ExecContext(
		ctx,
		"DROP TABLE "+
			clickHouseQualified(endpoint.Database, tableName),
	); err != nil {
		t.Fatalf("drop live ClickHouse table for hazard fixture: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"CREATE TABLE "+
			clickHouseQualified(endpoint.Database, tableName)+
			" (sentinel Int64) ENGINE = Log",
	); err != nil {
		t.Fatalf("create ClickHouse Log hazard: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+
			clickHouseQualified(endpoint.Database, tableName)+
			" VALUES (7)",
	); err != nil {
		t.Fatalf("insert ClickHouse Log sentinel: %v", err)
	}
	result, err = Execute(ctx, cfg, nil)
	if err == nil || !strings.Contains(
		err.Error(),
		"replace existing target engine",
	) {
		t.Fatalf("unsafe existing-engine result = %+v, error = %v", result, err)
	}
	var engineName string
	var sentinelCount int
	if err := target.QueryRowContext(
		ctx,
		`SELECT engine
		 FROM system.tables
		 WHERE database = ? AND name = ?`,
		endpoint.Database,
		tableName,
	).Scan(&engineName); err != nil {
		t.Fatalf("inspect rejected ClickHouse target engine: %v", err)
	}
	if err := target.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			clickHouseQualified(endpoint.Database, tableName)+
			" WHERE sentinel = 7",
	).Scan(&sentinelCount); err != nil {
		t.Fatalf("inspect rejected ClickHouse target sentinel: %v", err)
	}
	if engineName != "Log" || sentinelCount != 1 {
		t.Fatalf(
			"rejected target mutated: engine=%s sentinel=%d",
			engineName,
			sentinelCount,
		)
	}
}

func clickHouseLiveEndpoint(t *testing.T) config.Endpoint {
	t.Helper()
	dsn := os.Getenv("DMTX_TEST_CLICKHOUSE_DSN")
	caPath := os.Getenv("DMTX_TEST_CLICKHOUSE_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_CLICKHOUSE_DSN and DMTX_TEST_CLICKHOUSE_CA " +
				"to run the ClickHouse 24.8 TLS route test",
		)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse DMTX_TEST_CLICKHOUSE_DSN: %v", err)
	}
	if parsed.Scheme != "clickhouse" ||
		parsed.Hostname() == "" ||
		strings.TrimPrefix(parsed.Path, "/") == "" ||
		parsed.User == nil {
		t.Fatal(
			"DMTX_TEST_CLICKHOUSE_DSN must be a complete clickhouse URI",
		)
	}
	if parsed.Query().Get("secure") != "true" ||
		parsed.Query().Get("skip_verify") == "true" {
		t.Fatal(
			"DMTX_TEST_CLICKHOUSE_DSN must require verified TLS",
		)
	}
	port := 9440
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil {
			t.Fatalf("parse ClickHouse live port: %v", err)
		}
	}
	password, _ := parsed.User.Password()
	return config.Endpoint{
		Type:      "clickhouse",
		Host:      parsed.Hostname(),
		Port:      port,
		Database:  strings.TrimPrefix(parsed.Path, "/"),
		User:      parsed.User.Username(),
		Password:  password,
		SSLMode:   "verify-full",
		TLSCAFile: caPath,
	}
}

func createClickHouseLiveSQLiteSource(
	t *testing.T,
	path string,
	tableName string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	statement := `CREATE TABLE ` + quote(tableName) + ` (
		tenant_id INTEGER NOT NULL,
		event_id INT NOT NULL,
		signed_value INTEGER NOT NULL,
		score REAL,
		note TEXT NOT NULL,
		payload BLOB,
		PRIMARY KEY (tenant_id, event_id)
	) STRICT`
	if _, err := database.Exec(statement); err != nil {
		t.Fatalf("create strict SQLite ClickHouse fixture: %v", err)
	}
	insert, err := database.Prepare(
		`INSERT INTO ` + quote(tableName) +
			` (tenant_id, event_id, signed_value, score, note, payload)` +
			` VALUES (?, ?, ?, ?, ?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer insert.Close()
	for index := 0; index <= sqliteWriteBatchSize; index++ {
		signed := int64(index)
		score := any(float64(index) + 0.25)
		note := fmt.Sprintf("row-%d", index)
		payload := any([]byte{byte(index), 0, 255})
		switch index {
		case 0:
			signed = math.MinInt64
			score = nil
			note = "snowman ☃"
			payload = nil
		case 1:
			signed = math.MaxInt64
			score = float64(-1.25)
			note = ""
			payload = []byte{}
		}
		if _, err := insert.Exec(
			int64(index%3),
			int64(index),
			signed,
			score,
			note,
			payload,
		); err != nil {
			t.Fatalf("insert strict SQLite ClickHouse row %d: %v", index, err)
		}
	}
}

func matchesClickHouseObserver(
	observer *clickHouseLiveObserver,
	table string,
	rows int,
) bool {
	return len(observer.tableSets) == 1 &&
		len(observer.tableSets[0]) == 1 &&
		observer.tableSets[0][0] == table &&
		len(observer.before) == 1 &&
		observer.before[0] == table &&
		len(observer.after) == 1 &&
		observer.after[0].table == table &&
		observer.after[0].rows == rows
}

func assertClickHouseLiveSchemaAndRows(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) {
	t.Helper()
	rows, err := database.QueryContext(
		ctx,
		`SELECT name, type
		 FROM system.columns
		 WHERE database = ? AND table = ?
		 ORDER BY position`,
		namespace,
		table,
	)
	if err != nil {
		t.Fatalf("inspect ClickHouse live columns: %v", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("read ClickHouse live column: %v", err)
		}
		columns = append(columns, name+" "+typ)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ClickHouse live columns: %v", err)
	}
	wantColumns := []string{
		"tenant_id Int64",
		"event_id Int64",
		"signed_value Int64",
		"score Nullable(Float64)",
		"note String",
		"payload Nullable(String)",
	}
	if strings.Join(columns, "\n") != strings.Join(wantColumns, "\n") {
		t.Fatalf("ClickHouse columns = %v, want %v", columns, wantColumns)
	}
	var engineName, sortingKey, primaryKey string
	if err := database.QueryRowContext(
		ctx,
		`SELECT engine, sorting_key, primary_key
		 FROM system.tables
		 WHERE database = ? AND name = ?`,
		namespace,
		table,
	).Scan(&engineName, &sortingKey, &primaryKey); err != nil {
		t.Fatalf("inspect ClickHouse live table: %v", err)
	}
	if engineName != "MergeTree" ||
		sortingKey != "tenant_id, event_id" ||
		primaryKey != "tenant_id, event_id" {
		t.Fatalf(
			"ClickHouse table engine/order = %q %q %q",
			engineName,
			sortingKey,
			primaryKey,
		)
	}

	var signed int64
	var score sql.NullFloat64
	var note, payloadHex string
	var payloadNull uint8
	if err := database.QueryRowContext(
		ctx,
		"SELECT signed_value, score, note, hex(ifNull(payload, '')), "+
			"isNull(payload) FROM "+
			clickHouseQualified(namespace, table)+
			" WHERE tenant_id = 0 AND event_id = 0",
	).Scan(
		&signed,
		&score,
		&note,
		&payloadHex,
		&payloadNull,
	); err != nil {
		t.Fatalf("read ClickHouse null/unicode row: %v", err)
	}
	if signed != math.MinInt64 ||
		score.Valid ||
		note != "snowman ☃" ||
		payloadHex != "" ||
		payloadNull != 1 {
		t.Fatalf(
			"ClickHouse null/unicode row = %d %+v %q %q %d",
			signed,
			score,
			note,
			payloadHex,
			payloadNull,
		)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT signed_value, score, note, hex(payload), "+
			"isNull(payload) FROM "+
			clickHouseQualified(namespace, table)+
			" WHERE tenant_id = 1 AND event_id = 1",
	).Scan(
		&signed,
		&score,
		&note,
		&payloadHex,
		&payloadNull,
	); err != nil {
		t.Fatalf("read ClickHouse empty-value row: %v", err)
	}
	if signed != math.MaxInt64 ||
		!score.Valid ||
		score.Float64 != -1.25 ||
		note != "" ||
		payloadHex != "" ||
		payloadNull != 0 {
		t.Fatalf(
			"ClickHouse empty-value row = %d %+v %q %q %d",
			signed,
			score,
			note,
			payloadHex,
			payloadNull,
		)
	}
	var count int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			clickHouseQualified(namespace, table),
	).Scan(&count); err != nil {
		t.Fatalf("count ClickHouse live rows: %v", err)
	}
	if count != sqliteWriteBatchSize+1 {
		t.Fatalf(
			"ClickHouse live rows = %d, want %d",
			count,
			sqliteWriteBatchSize+1,
		)
	}
}
