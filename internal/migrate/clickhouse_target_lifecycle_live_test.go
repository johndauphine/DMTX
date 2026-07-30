package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestClickHouse248TargetCheckpointRaceLive(t *testing.T) {
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

	prefix := fmt.Sprintf(
		"dmtx_clickhouse_race_%d",
		time.Now().UnixNano(),
	)
	populatedName := prefix + "_a_populated"
	emptyName := prefix + "_z_empty"
	names := []string{populatedName, emptyName}
	cleanupClickHouseLifecycleTables(
		t,
		target,
		endpoint.Database,
		names...,
	)
	sourcePath := filepath.Join(t.TempDir(), "clickhouse-race.sqlite")
	createClickHouseLifecycleSQLiteSource(
		t,
		sourcePath,
		populatedName,
		emptyName,
	)
	cfg := config.Config{
		Source: config.Endpoint{
			Type:     "sqlite",
			Database: sourcePath,
		},
		Target: endpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: names,
		},
	}
	observer := &clickHouseCheckpointRaceObserver{
		database:  target,
		namespace: endpoint.Database,
		tables:    names,
		populated: populatedName,
	}
	result, err := Execute(ctx, cfg, observer)
	if !errors.Is(err, ErrDestructiveAcknowledgement) {
		t.Fatalf(
			"post-checkpoint target race result = %+v, error = %v",
			result,
			err,
		)
	}
	if observer.beforeSets != 1 ||
		observer.beforeTables != 0 ||
		observer.afterTables != 0 {
		t.Fatalf("post-checkpoint observer = %+v", observer)
	}
	assertClickHouseLifecycleSentinel(
		t,
		ctx,
		target,
		endpoint.Database,
		populatedName,
		1,
	)
	assertClickHouseLifecycleSentinel(
		t,
		ctx,
		target,
		endpoint.Database,
		emptyName,
		0,
	)

	cfg.Migration.DestructiveAcknowledged = true
	result, err = Execute(ctx, cfg, nil)
	if err != nil {
		t.Fatalf("acknowledged post-checkpoint rebuild: %v", err)
	}
	if result.Tables != 2 || result.Rows != 2 || !result.Validated {
		t.Fatalf("acknowledged lifecycle result = %+v", result)
	}
	for index, name := range names {
		var columns []string
		rows, err := target.QueryContext(
			ctx,
			`SELECT name
			   FROM system.columns
			  WHERE database = ? AND table = ?
			  ORDER BY position`,
			endpoint.Database,
			name,
		)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var column string
			if err := rows.Scan(&column); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			columns = append(columns, column)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(
			columns,
			[]string{"tenant_id", "event_id", "note"},
		) {
			t.Fatalf("rebuilt ClickHouse columns for %s = %v", name, columns)
		}
		var note string
		if err := target.QueryRowContext(
			ctx,
			"SELECT note FROM "+
				clickHouseQualified(endpoint.Database, name)+
				" WHERE tenant_id = 1 AND event_id = ?",
			int64(index+1),
		).Scan(&note); err != nil {
			t.Fatal(err)
		}
		if note != "source-"+fmt.Sprint(index+1) {
			t.Fatalf("rebuilt ClickHouse note for %s = %q", name, note)
		}
	}
}

func TestClickHouse248PrepareDropsAllBeforeCreateFailureLive(t *testing.T) {
	endpoint := clickHouseLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
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

	prefix := fmt.Sprintf(
		"dmtx_clickhouse_prepare_%d",
		time.Now().UnixNano(),
	)
	invalidName := prefix + "_a_invalid"
	laterName := prefix + "_z_later"
	cleanupClickHouseLifecycleTables(
		t,
		target,
		endpoint.Database,
		invalidName,
		laterName,
	)
	for _, name := range []string{invalidName, laterName} {
		if _, err := target.ExecContext(
			ctx,
			"CREATE TABLE "+
				clickHouseQualified(endpoint.Database, name)+
				" (sentinel Int64) ENGINE = MergeTree ORDER BY sentinel",
		); err != nil {
			t.Fatalf("create ClickHouse preparation sentinel %s: %v", name, err)
		}
		if _, err := target.ExecContext(
			ctx,
			"INSERT INTO "+
				clickHouseQualified(endpoint.Database, name)+
				" VALUES (7)",
		); err != nil {
			t.Fatalf("populate ClickHouse preparation sentinel %s: %v", name, err)
		}
	}

	invalid := schema.Table{
		Schema:            endpoint.Database,
		Name:              invalidName,
		ClickHouseOrderBy: []string{"id"},
		Columns: []schema.Column{{
			Name:     "id",
			Type:     "bigint",
			Nullable: true,
		}},
	}
	later := schema.Table{
		Schema:            endpoint.Database,
		Name:              laterName,
		ClickHouseOrderBy: []string{"id"},
		Columns: []schema.Column{{
			Name: "id",
			Type: "bigint",
		}},
	}
	adapter := &clickHouseTargetAdapter{
		database:                target,
		namespace:               endpoint.Database,
		destructiveAcknowledged: true,
	}
	err = adapter.PrepareTables(
		ctx,
		[]schema.Table{later, invalid},
		"drop_recreate",
	)
	if err == nil ||
		!strings.Contains(err.Error(), "target preparation may be partial") ||
		!strings.Contains(
			err.Error(),
			"rerun the full migration in drop_recreate mode",
		) ||
		!strings.Contains(err.Error(), "rebuild all selected targets") {
		t.Fatalf("live partial preparation error = %v", err)
	}
	for _, name := range []string{invalidName, laterName} {
		var exists uint64
		if err := target.QueryRowContext(
			ctx,
			`SELECT count()
			   FROM system.tables
			  WHERE database = ? AND name = ?`,
			endpoint.Database,
			name,
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != 0 {
			t.Fatalf(
				"ClickHouse table %s survived all-drop preparation: %d",
				name,
				exists,
			)
		}
	}

	invalid.Columns[0].Nullable = false
	if err := adapter.PrepareTables(
		ctx,
		[]schema.Table{later, invalid},
		"drop_recreate",
	); err != nil {
		t.Fatalf("rerun ClickHouse rebuild recovery: %v", err)
	}
	for _, name := range []string{invalidName, laterName} {
		var exists uint64
		if err := target.QueryRowContext(
			ctx,
			`SELECT count()
			   FROM system.tables
			  WHERE database = ? AND name = ?`,
			endpoint.Database,
			name,
		).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists != 1 {
			t.Fatalf(
				"recovered ClickHouse table %s count = %d",
				name,
				exists,
			)
		}
	}
}

type clickHouseCheckpointRaceObserver struct {
	database     *sql.DB
	namespace    string
	tables       []string
	populated    string
	beforeSets   int
	beforeTables int
	afterTables  int
}

func (observer *clickHouseCheckpointRaceObserver) BeforeTables(
	ctx context.Context,
	tables []string,
) error {
	observer.beforeSets++
	if !reflect.DeepEqual(tables, observer.tables) {
		return fmt.Errorf(
			"checkpoint tables = %v, want %v",
			tables,
			observer.tables,
		)
	}
	for _, table := range observer.tables {
		if _, err := observer.database.ExecContext(
			ctx,
			"CREATE TABLE "+
				clickHouseQualified(observer.namespace, table)+
				" (sentinel Int64) ENGINE = MergeTree ORDER BY sentinel",
		); err != nil {
			return fmt.Errorf(
				"create post-checkpoint ClickHouse sentinel %s: %w",
				table,
				err,
			)
		}
	}
	if _, err := observer.database.ExecContext(
		ctx,
		"INSERT INTO "+
			clickHouseQualified(observer.namespace, observer.populated)+
			" VALUES (101)",
	); err != nil {
		return fmt.Errorf("populate post-checkpoint ClickHouse sentinel: %w", err)
	}
	return nil
}

func (observer *clickHouseCheckpointRaceObserver) BeforeTable(
	context.Context,
	string,
) error {
	observer.beforeTables++
	return nil
}

func (observer *clickHouseCheckpointRaceObserver) AfterTable(
	context.Context,
	string,
	int,
) error {
	observer.afterTables++
	return nil
}

func createClickHouseLifecycleSQLiteSource(
	t *testing.T,
	path string,
	tables ...string,
) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for index, table := range tables {
		if _, err := database.Exec(
			"CREATE TABLE " + quote(table) + ` (
				tenant_id INTEGER NOT NULL,
				event_id INT NOT NULL,
				note TEXT NOT NULL,
				PRIMARY KEY (tenant_id, event_id)
			) STRICT`,
		); err != nil {
			t.Fatalf("create ClickHouse lifecycle SQLite source %s: %v", table, err)
		}
		if _, err := database.Exec(
			"INSERT INTO "+quote(table)+
				" (tenant_id, event_id, note) VALUES (1, ?, ?)",
			index+1,
			"source-"+fmt.Sprint(index+1),
		); err != nil {
			t.Fatalf("populate ClickHouse lifecycle SQLite source %s: %v", table, err)
		}
	}
}

func assertClickHouseLifecycleSentinel(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
	wantRows uint64,
) {
	t.Helper()
	var columns, rows uint64
	if err := database.QueryRowContext(
		ctx,
		`SELECT count()
		   FROM system.columns
		  WHERE database = ? AND table = ? AND name = 'sentinel'`,
		namespace,
		table,
	).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 1 {
		t.Fatalf("ClickHouse sentinel column count for %s = %d", table, columns)
	}
	if err := database.QueryRowContext(
		ctx,
		"SELECT count() FROM "+clickHouseQualified(namespace, table),
	).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != wantRows {
		t.Fatalf(
			"ClickHouse sentinel rows for %s = %d, want %d",
			table,
			rows,
			wantRows,
		)
	}
	if wantRows != 0 {
		var sentinel int64
		if err := database.QueryRowContext(
			ctx,
			"SELECT sentinel FROM "+
				clickHouseQualified(namespace, table),
		).Scan(&sentinel); err != nil {
			t.Fatal(err)
		}
		if sentinel != 101 {
			t.Fatalf("ClickHouse sentinel for %s = %d", table, sentinel)
		}
	}
}

func cleanupClickHouseLifecycleTables(
	t *testing.T,
	database *sql.DB,
	namespace string,
	tables ...string,
) {
	t.Helper()
	for _, table := range tables {
		if _, err := database.Exec(
			"DROP TABLE IF EXISTS " +
				clickHouseQualified(namespace, table),
		); err != nil {
			t.Fatalf("drop stale ClickHouse lifecycle table %s: %v", table, err)
		}
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		for _, table := range tables {
			_, _ = database.ExecContext(
				ctx,
				"DROP TABLE IF EXISTS "+
					clickHouseQualified(namespace, table),
			)
		}
	})
}
