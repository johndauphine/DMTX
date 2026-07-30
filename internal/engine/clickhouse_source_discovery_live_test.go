package engine

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestClickHouse248SourceDiscoveryLive(t *testing.T) {
	endpoint := clickHouseSourceLiveEndpoint(t)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database, err := OpenClickHouse(ctx, endpoint)
	if err != nil {
		t.Fatalf("open live ClickHouse source: %v", err)
	}
	defer database.Close()
	if err := VerifyClickHouse248Source(
		ctx,
		database,
		endpoint.Database,
	); err != nil {
		t.Fatal(err)
	}

	tableName := fmt.Sprintf(
		"dmtx_clickhouse_source_%d",
		time.Now().UnixNano(),
	)
	hazardName := tableName + "_hazard"
	for _, name := range []string{tableName, hazardName} {
		name := name
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cleanupCancel()
			_, _ = database.ExecContext(
				cleanupCtx,
				"DROP TABLE IF EXISTS "+
					clickHouseLiveQualified(endpoint.Database, name),
			)
		})
	}
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			clickHouseLiveQualified(endpoint.Database, tableName)+
			" ("+
			"tenant_id Int64, "+
			"event_id Int64, "+
			"score Nullable(Float64), "+
			"note Nullable(String), "+
			"payload String) "+
			"ENGINE = MergeTree ORDER BY (tenant_id, event_id)",
	); err != nil {
		t.Fatalf("create live ClickHouse source table: %v", err)
	}
	table, err := InspectClickHouseTable(
		ctx,
		database,
		endpoint.Database,
		tableName,
	)
	if err != nil {
		t.Fatalf("inspect live ClickHouse source table: %v", err)
	}
	if want := []string{"tenant_id", "event_id"}; !reflect.DeepEqual(
		table.ClickHouseOrderBy,
		want,
	) {
		t.Fatalf(
			"live ClickHouse ordering = %v, want %v",
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
		t.Fatalf(
			"live ClickHouse columns = %#v, want %#v",
			table.Columns,
			wantColumns,
		)
	}
	for _, column := range table.Columns {
		if column.PrimaryKey || column.PrimaryKeyPosition != 0 {
			t.Fatalf(
				"live ClickHouse ordering became relational key: %#v",
				column,
			)
		}
	}
	tables, err := ListClickHouseTables(
		ctx,
		database,
		endpoint.Database,
	)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, name := range tables {
		if name == tableName {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("live ClickHouse table list omitted %s: %v", tableName, tables)
	}

	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+
			clickHouseLiveQualified(endpoint.Database, hazardName)+
			" (tenant_id Int64, payload String) ENGINE = MergeTree "+
			"ORDER BY cityHash64(tenant_id)",
	); err != nil {
		t.Fatalf("create live ClickHouse ordering hazard: %v", err)
	}
	if _, err := InspectClickHouseTable(
		ctx,
		database,
		endpoint.Database,
		hazardName,
	); err == nil || !strings.Contains(err.Error(), "ordering key") {
		t.Fatalf("live ClickHouse hazard error = %v", err)
	}
}

func clickHouseSourceLiveEndpoint(t *testing.T) config.Endpoint {
	t.Helper()
	const dsnVariable = "DMTX_TEST_CLICKHOUSE_SOURCE_DSN"
	dsn := os.Getenv(dsnVariable)
	caPath := os.Getenv("DMTX_TEST_CLICKHOUSE_CA")
	if dsn == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_CLICKHOUSE_SOURCE_DSN and " +
				"DMTX_TEST_CLICKHOUSE_CA to run ClickHouse source discovery",
		)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse %s: %v", dsnVariable, err)
	}
	if parsed.Scheme != "clickhouse" ||
		parsed.Hostname() == "" ||
		strings.TrimPrefix(parsed.Path, "/") == "" ||
		parsed.User == nil ||
		parsed.Query().Get("secure") != "true" ||
		parsed.Query().Get("skip_verify") == "true" {
		t.Fatalf("%s must be a complete verified-TLS URI", dsnVariable)
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

func clickHouseLiveQualified(database, table string) string {
	quote := func(value string) string {
		return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
	}
	return quote(database) + "." + quote(table)
}
