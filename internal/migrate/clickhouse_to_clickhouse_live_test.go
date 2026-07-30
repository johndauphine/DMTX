package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

func TestClickHouse248ToClickHouse248RebuildLive(t *testing.T) {
	sourceEndpoint := clickHouseLiveEndpointFromEnvironment(
		t,
		"DMTX_TEST_CLICKHOUSE_SOURCE_DSN",
	)
	targetEndpoint := clickHouseLiveEndpointFromEnvironment(
		t,
		"DMTX_TEST_CLICKHOUSE_TARGET_DSN",
	)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		120*time.Second,
	)
	defer cancel()

	source := openClickHouseLiveDatabase(
		t,
		ctx,
		sourceEndpoint,
		"source",
	)
	target := openClickHouseLiveDatabase(
		t,
		ctx,
		targetEndpoint,
		"target",
	)
	if err := requireDistinctLiveClickHouseDatabases(
		ctx,
		&clickHouseSourceAdapter{database: source},
		&clickHouseTargetAdapter{database: source},
	); err == nil || !strings.Contains(err.Error(), "distinct live") {
		t.Fatalf("same live ClickHouse database error = %v", err)
	}

	tableName := fmt.Sprintf(
		"dmtx_clickhouse_rebuild_%d",
		time.Now().UnixNano(),
	)
	hazardName := tableName + "_hazard"
	materializedViewName := tableName + "_mv"
	for _, fixture := range []struct {
		database  *sql.DB
		namespace string
		table     string
	}{
		{source, sourceEndpoint.Database, tableName},
		{target, targetEndpoint.Database, tableName},
		{source, sourceEndpoint.Database, hazardName},
		{target, targetEndpoint.Database, hazardName},
		{target, targetEndpoint.Database, materializedViewName},
	} {
		fixture := fixture
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cleanupCancel()
			_, _ = fixture.database.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+
					clickHouseQualified(
						fixture.namespace,
						fixture.table,
					),
			)
		})
	}

	createClickHouseNativeSourceFixture(
		t,
		ctx,
		source,
		sourceEndpoint.Database,
		tableName,
	)
	cfg := config.Config{
		Source: sourceEndpoint,
		Target: targetEndpoint,
		Migration: config.Migration{
			TargetMode:    "drop_recreate",
			IncludeTables: []string{tableName},
		},
	}
	observer := &clickHouseLiveObserver{}
	result, err := ClickHouseToClickHouseWithObserver(
		ctx,
		cfg,
		observer,
	)
	const fixtureRows = sqliteWriteBatchSize + 5
	if err != nil {
		t.Fatalf("execute ClickHouse rebuild route: %v", err)
	}
	if result.Tables != 1 ||
		result.Rows != fixtureRows ||
		!result.Validated {
		t.Fatalf("result = %+v", result)
	}
	if !matchesClickHouseObserver(observer, tableName, fixtureRows) {
		t.Fatalf("observer = %+v", observer)
	}
	assertClickHouseNativeRebuild(
		t,
		ctx,
		source,
		target,
		sourceEndpoint.Database,
		targetEndpoint.Database,
		tableName,
		fixtureRows,
	)

	// A rerun must rebuild rather than retain a target-only row.
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+
			clickHouseQualified(targetEndpoint.Database, tableName)+
			" VALUES (?, ?, ?, ?, ?, ?)",
		int64(99),
		int64(9999),
		int64(9999),
		float64(9999),
		"target-only",
		"target-only",
	); err != nil {
		t.Fatalf("insert target-only ClickHouse row: %v", err)
	}
	if result, err = ClickHouseToClickHouseWithObserver(
		ctx,
		cfg,
		nil,
	); err != nil {
		t.Fatalf("rerun ClickHouse rebuild route: %v", err)
	}
	if result.Rows != fixtureRows || !result.Validated {
		t.Fatalf("rerun result = %+v", result)
	}
	var targetOnly uint64
	if err := target.QueryRowContext(
		ctx,
		"SELECT count() FROM "+
			clickHouseQualified(targetEndpoint.Database, tableName)+
			" WHERE note = 'target-only'",
	).Scan(&targetOnly); err != nil {
		t.Fatalf("count target-only row: %v", err)
	}
	if targetOnly != 0 {
		t.Fatalf("target-only rows after rebuild = %d", targetOnly)
	}

	// Expression ordering is outside the direct-column preservation contract
	// and must fail during source planning before touching the target object.
	if _, err := source.ExecContext(
		ctx,
		"CREATE TABLE "+
			clickHouseQualified(sourceEndpoint.Database, hazardName)+
			" (tenant_id Int64, payload String) ENGINE = MergeTree "+
			"ORDER BY cityHash64(tenant_id)",
	); err != nil {
		t.Fatalf("create ClickHouse source hazard: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"CREATE TABLE "+
			clickHouseQualified(targetEndpoint.Database, hazardName)+
			" (sentinel Int64) ENGINE = MergeTree ORDER BY sentinel",
	); err != nil {
		t.Fatalf("create ClickHouse target sentinel: %v", err)
	}
	if _, err := target.ExecContext(
		ctx,
		"INSERT INTO "+
			clickHouseQualified(targetEndpoint.Database, hazardName)+
			" VALUES (7)",
	); err != nil {
		t.Fatalf("insert ClickHouse target sentinel: %v", err)
	}
	hazardConfig := cfg
	hazardConfig.Migration.IncludeTables = []string{tableName, hazardName}
	result, err = ClickHouseToClickHouseWithObserver(
		ctx,
		hazardConfig,
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "ordering key") {
		t.Fatalf("hazard result = %+v, error = %v", result, err)
	}
	var sentinel uint64
	if err := target.QueryRowContext(
		ctx,
		"SELECT count() FROM "+
			clickHouseQualified(targetEndpoint.Database, hazardName)+
			" WHERE sentinel = 7",
	).Scan(&sentinel); err != nil {
		t.Fatalf("inspect rejected target sentinel: %v", err)
	}
	if sentinel != 1 {
		t.Fatalf("rejected target was mutated: sentinel=%d", sentinel)
	}

	// A dependent materialized view makes replacement destructive outside the
	// selected table. Target preflight must reject it before dropping rows.
	if _, err := target.ExecContext(
		ctx,
		"CREATE MATERIALIZED VIEW "+
			clickHouseQualified(
				targetEndpoint.Database,
				materializedViewName,
			)+
			" ENGINE = MergeTree ORDER BY tenant_id AS "+
			"SELECT tenant_id, event_id FROM "+
			clickHouseQualified(targetEndpoint.Database, tableName),
	); err != nil {
		t.Fatalf("create dependent ClickHouse materialized view: %v", err)
	}
	result, err = ClickHouseToClickHouseWithObserver(ctx, cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "dependent objects") {
		t.Fatalf(
			"dependent-target result = %+v, error = %v",
			result,
			err,
		)
	}
	var retainedRows uint64
	if err := target.QueryRowContext(
		ctx,
		"SELECT count() FROM "+
			clickHouseQualified(targetEndpoint.Database, tableName),
	).Scan(&retainedRows); err != nil {
		t.Fatalf("count dependency-rejected target rows: %v", err)
	}
	if retainedRows != fixtureRows {
		t.Fatalf(
			"dependency-rejected target rows = %d, want %d",
			retainedRows,
			fixtureRows,
		)
	}
}

func openClickHouseLiveDatabase(
	t *testing.T,
	ctx context.Context,
	endpoint config.Endpoint,
	role string,
) *sql.DB {
	t.Helper()
	database, err := engine.OpenClickHouse(ctx, endpoint)
	if err != nil {
		t.Fatalf("open live ClickHouse %s: %v", role, err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close live ClickHouse %s: %v", role, err)
		}
	})
	if role == "source" {
		err = engine.VerifyClickHouse248Source(
			ctx,
			database,
			endpoint.Database,
		)
	} else {
		err = engine.VerifyClickHouse248Target(
			ctx,
			database,
			endpoint.Database,
		)
	}
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func createClickHouseNativeSourceFixture(
	t *testing.T,
	ctx context.Context,
	database *sql.DB,
	namespace string,
	table string,
) {
	t.Helper()
	qualified := clickHouseQualified(namespace, table)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+qualified+" ("+
			"tenant_id Int64, "+
			"event_id Int64, "+
			"signed_value Int64, "+
			"score Nullable(Float64), "+
			"note String, "+
			"payload Nullable(String)) "+
			"ENGINE = MergeTree ORDER BY (tenant_id, event_id)",
	); err != nil {
		t.Fatalf("create ClickHouse source fixture: %v", err)
	}
	if _, err := database.ExecContext(
		ctx,
		"INSERT INTO "+qualified+" "+
			"SELECT "+
			"toInt64(number % 3), "+
			"toInt64(number), "+
			"toInt64(number), "+
			"if(number % 7 = 0, NULL, toFloat64(number) + 0.25), "+
			"concat('row-', toString(number)), "+
			"if(number % 13 = 0, NULL, concat('payload-', toString(number))) "+
			"FROM numbers(?)",
		uint64(sqliteWriteBatchSize+1),
	); err != nil {
		t.Fatalf("insert generated ClickHouse source rows: %v", err)
	}
	specialRows := [][]any{
		{
			int64(7),
			int64(-1),
			int64(math.MinInt64),
			nil,
			"snowman ☃",
			nil,
		},
		{
			int64(7),
			int64(-2),
			int64(math.MaxInt64),
			float64(-1.25),
			"",
			"",
		},
		{
			int64(7),
			int64(-3),
			int64(-3),
			float64(3.5),
			"duplicate",
			string([]byte{0, 255, 128}),
		},
		{
			int64(7),
			int64(-3),
			int64(-3),
			float64(3.5),
			"duplicate",
			string([]byte{0, 255, 128}),
		},
	}
	for index, row := range specialRows {
		if _, err := database.ExecContext(
			ctx,
			"INSERT INTO "+qualified+" VALUES (?, ?, ?, ?, ?, ?)",
			row...,
		); err != nil {
			t.Fatalf(
				"insert special ClickHouse source row %d: %v",
				index,
				err,
			)
		}
	}
}

func assertClickHouseNativeRebuild(
	t *testing.T,
	ctx context.Context,
	source *sql.DB,
	target *sql.DB,
	sourceNamespace string,
	targetNamespace string,
	table string,
	wantRows int,
) {
	t.Helper()
	var engineName, sortingKey, primaryKey string
	if err := target.QueryRowContext(
		ctx,
		`SELECT engine, sorting_key, primary_key
		   FROM system.tables
		  WHERE database = ? AND name = ?`,
		targetNamespace,
		table,
	).Scan(&engineName, &sortingKey, &primaryKey); err != nil {
		t.Fatalf("inspect rebuilt ClickHouse table: %v", err)
	}
	if engineName != "MergeTree" ||
		sortingKey != "tenant_id, event_id" ||
		primaryKey != "tenant_id, event_id" {
		t.Fatalf(
			"rebuilt engine/order = %q %q %q",
			engineName,
			sortingKey,
			primaryKey,
		)
	}
	rows, err := target.QueryContext(
		ctx,
		`SELECT name, type
		   FROM system.columns
		  WHERE database = ? AND table = ?
		  ORDER BY position`,
		targetNamespace,
		table,
	)
	if err != nil {
		t.Fatalf("inspect rebuilt ClickHouse columns: %v", err)
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatalf("read rebuilt ClickHouse column: %v", err)
		}
		columns = append(columns, name+" "+typ)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate rebuilt ClickHouse columns: %v", err)
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
		t.Fatalf("rebuilt columns = %v, want %v", columns, wantColumns)
	}

	projection := "tenant_id, event_id, signed_value, score, note, payload"
	sourceTable := clickHouseQualified(sourceNamespace, table)
	targetTable := clickHouseQualified(targetNamespace, table)
	var sourceOnly, targetOnly uint64
	if err := source.QueryRowContext(
		ctx,
		"SELECT count() FROM (SELECT "+projection+" FROM "+
			sourceTable+" EXCEPT ALL SELECT "+projection+" FROM "+
			targetTable+")",
	).Scan(&sourceOnly); err != nil {
		t.Fatalf("compare source-only ClickHouse rows: %v", err)
	}
	if err := source.QueryRowContext(
		ctx,
		"SELECT count() FROM (SELECT "+projection+" FROM "+
			targetTable+" EXCEPT ALL SELECT "+projection+" FROM "+
			sourceTable+")",
	).Scan(&targetOnly); err != nil {
		t.Fatalf("compare target-only ClickHouse rows: %v", err)
	}
	if sourceOnly != 0 || targetOnly != 0 {
		t.Fatalf(
			"rebuilt row differences: source-only=%d target-only=%d",
			sourceOnly,
			targetOnly,
		)
	}
	var count, duplicates uint64
	if err := target.QueryRowContext(
		ctx,
		"SELECT count(), countIf(event_id = -3) FROM "+targetTable,
	).Scan(&count, &duplicates); err != nil {
		t.Fatalf("count rebuilt ClickHouse rows: %v", err)
	}
	if count != uint64(wantRows) || duplicates != 2 {
		t.Fatalf(
			"rebuilt counts = rows:%d duplicates:%d, want %d/2",
			count,
			duplicates,
			wantRows,
		)
	}
	var payloadHex string
	if err := target.QueryRowContext(
		ctx,
		"SELECT any(hex(payload)) FROM "+targetTable+
			" WHERE event_id = -3",
	).Scan(&payloadHex); err != nil {
		t.Fatalf("read rebuilt arbitrary String bytes: %v", err)
	}
	if payloadHex != "00FF80" {
		t.Fatalf("rebuilt arbitrary String bytes = %q", payloadHex)
	}
}
