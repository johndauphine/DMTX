package migrate

import (
	"context"
	"database/sql"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresStableCatalogAndRowsUseExactTransactionLiveTLS(
	t *testing.T,
) {
	dsn := os.Getenv("DMTX_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip(
			"set DMTX_TEST_POSTGRES_DSN to run the PostgreSQL " +
				"stable catalog transaction sentinel",
		)
	}
	parsed, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL stable catalog DSN: %T", err)
	}
	if !postgresRouteLiveRequiresTLS(parsed) {
		t.Fatal("DMTX_TEST_POSTGRES_DSN must require TLS")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		45*time.Second,
	)
	defer cancel()
	setup, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL stable catalog setup: %T", err)
	}
	t.Cleanup(func() {
		if err := setup.Close(); err != nil {
			t.Errorf("close PostgreSQL stable catalog setup: %v", err)
		}
	})
	if err := setup.PingContext(ctx); err != nil {
		t.Fatalf("verify PostgreSQL stable catalog setup: %T", err)
	}
	var tlsActive bool
	if err := setup.QueryRowContext(
		ctx,
		`SELECT ssl
		   FROM pg_stat_ssl
		  WHERE pid = pg_backend_pid()`,
	).Scan(&tlsActive); err != nil {
		t.Fatalf("inspect PostgreSQL stable catalog TLS session: %v", err)
	}
	if !tlsActive {
		t.Fatal("PostgreSQL stable catalog setup established a non-TLS session")
	}

	namespace := "dmtx_stable_catalog_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	parentName := "parents"
	tableName := "events"
	if _, err := setup.ExecContext(
		ctx,
		"CREATE SCHEMA "+postgresIdentifier(namespace),
	); err != nil {
		t.Fatalf("create PostgreSQL stable catalog schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cleanupCancel()
		if _, err := setup.ExecContext(
			cleanupCtx,
			"DROP SCHEMA IF EXISTS "+
				postgresIdentifier(namespace)+" CASCADE",
		); err != nil {
			t.Errorf("drop PostgreSQL stable catalog schema: %v", err)
		}
	})
	parentQualified := postgresQualified(namespace, parentName)
	qualified := postgresQualified(namespace, tableName)
	for _, statement := range []string{
		`CREATE TABLE ` + parentQualified + ` (
			id BIGINT NOT NULL PRIMARY KEY
		)`,
		`INSERT INTO ` + parentQualified + ` VALUES (1), (2), (3)`,
		`CREATE TABLE ` + qualified + ` (
			id BIGINT NOT NULL PRIMARY KEY,
			parent_id BIGINT NOT NULL,
			payload TEXT NOT NULL
		)`,
		`INSERT INTO ` + qualified + ` VALUES
			(1, 1, 'one'),
			(2, 2, 'two')`,
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf("create PostgreSQL stable catalog fixture: %v", err)
		}
	}

	source, err := openPostgresSourceAdapter(ctx, config.Endpoint{
		Type:     "postgres",
		Host:     parsed.Host,
		Port:     int(parsed.Port),
		Database: parsed.Database,
		User:     parsed.User,
		Password: parsed.Password,
		Schema:   namespace,
	})
	if err != nil {
		t.Fatalf("open PostgreSQL stable catalog source: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("close PostgreSQL stable catalog source: %v", err)
		}
	})
	baseline, err := source.InspectTable(ctx, tableName)
	if err != nil {
		t.Fatalf("inspect PostgreSQL stable catalog baseline: %v", err)
	}
	if len(baseline.ForeignKeys) != 0 {
		t.Fatalf(
			"PostgreSQL stable catalog baseline foreign keys = %#v, want none",
			baseline.ForeignKeys,
		)
	}

	session, err := OpenAdapterStableNetworkTableSource(
		ctx,
		source,
		baseline,
	)
	if err != nil {
		t.Fatalf("open PostgreSQL stable catalog transaction: %v", err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Errorf("close PostgreSQL stable catalog transaction: %v", err)
		}
	})
	stable, err := session.Source()
	if err != nil {
		t.Fatal(err)
	}
	insideBefore, err := stable.InspectTable(ctx, tableName)
	if err != nil {
		t.Fatalf("inspect PostgreSQL catalog inside stable transaction: %v", err)
	}
	assertPostgresStableCatalogEqual(t, insideBefore, baseline)

	constraintName := "events_parent_fk"
	for _, statement := range []string{
		"ALTER TABLE " + qualified +
			" ADD CONSTRAINT " + postgresIdentifier(constraintName) +
			" FOREIGN KEY (parent_id) REFERENCES " +
			parentQualified + " (id) NOT VALID",
		"ALTER TABLE " + qualified +
			" VALIDATE CONSTRAINT " + postgresIdentifier(constraintName),
		"INSERT INTO " + qualified + " VALUES (3, 3, 'three')",
	} {
		if _, err := setup.ExecContext(ctx, statement); err != nil {
			t.Fatalf(
				"mutate PostgreSQL catalog beside stable transaction: %v",
				err,
			)
		}
	}

	fresh, err := source.InspectTable(ctx, tableName)
	if err != nil {
		t.Fatalf("inspect fresh PostgreSQL catalog after DDL: %v", err)
	}
	if len(fresh.ForeignKeys) != 1 ||
		fresh.ForeignKeys[0].Name != constraintName {
		t.Fatalf(
			"fresh PostgreSQL catalog foreign keys = %#v, want %q",
			fresh.ForeignKeys,
			constraintName,
		)
	}
	if same, err := stage4AdapterNetworkCatalogEqual(
		fresh,
		baseline,
	); err != nil {
		t.Fatalf("compare fresh PostgreSQL catalog: %v", err)
	} else if same {
		t.Fatal("fresh PostgreSQL catalog did not observe concurrent DDL")
	}

	insideAfter, err := stable.InspectTable(ctx, tableName)
	if err != nil {
		t.Fatalf(
			"reinspect PostgreSQL catalog inside stable transaction: %v",
			err,
		)
	}
	assertPostgresStableCatalogEqual(t, insideAfter, baseline)
	if len(insideAfter.ForeignKeys) != 0 {
		t.Fatalf(
			"stable PostgreSQL catalog observed concurrent foreign key: %#v",
			insideAfter.ForeignKeys,
		)
	}
	stableCount, err := stable.CountRows(ctx, baseline)
	if err != nil {
		t.Fatalf("count PostgreSQL rows inside stable transaction: %v", err)
	}
	freshCount, err := source.CountRows(ctx, fresh)
	if err != nil {
		t.Fatalf("count fresh PostgreSQL rows: %v", err)
	}
	if stableCount != 2 || freshCount != 3 {
		t.Fatalf(
			"PostgreSQL stable/fresh row counts = %d/%d, want 2/3",
			stableCount,
			freshCount,
		)
	}

	columns := adapterColumnNames(baseline)
	pagination, err := stable.PlanPagination(ctx, baseline, 1)
	if err != nil {
		t.Fatalf("plan PostgreSQL stable catalog pagination: %v", err)
	}
	evidence, err := stable.PlanRetainedRowWidth(
		ctx,
		baseline,
		columns,
	)
	if err != nil {
		t.Fatalf("plan PostgreSQL stable catalog retained width: %v", err)
	}
	request := NetworkReadRequest{
		Range: NetworkRangePlan{
			RangeIndex:   0,
			TableSchema:  baseline.Schema,
			TableName:    baseline.Name,
			TopologyHash: strings.Repeat("c", 64),
			Pagination:   pagination.Strategy,
			MaxRowBytes:  evidence.UpperBoundBytes,
		},
		MaxRows: 1,
	}
	var identifiers []int64
	for {
		page, err := stable.ReadNetworkRangePage(
			ctx,
			baseline,
			columns,
			pagination,
			pagination.Ranges[0],
			request,
		)
		if err != nil {
			t.Fatalf("read PostgreSQL stable catalog page: %v", err)
		}
		if len(page.Rows) != 1 {
			t.Fatalf(
				"PostgreSQL stable catalog page rows = %d, want 1",
				len(page.Rows),
			)
		}
		identifier, ok := page.Rows[0][0].(int64)
		if !ok {
			t.Fatalf(
				"PostgreSQL stable catalog page identifier = %T, want int64",
				page.Rows[0][0],
			)
		}
		identifiers = append(identifiers, identifier)
		if page.Exhausted {
			break
		}
		request.Sequence++
		request.StartFrontier = cloneNetworkBytes(page.EndFrontier)
	}
	if !reflect.DeepEqual(identifiers, []int64{1, 2}) {
		t.Fatalf(
			"PostgreSQL stable catalog identifiers = %#v, want [1 2]",
			identifiers,
		)
	}
}

func assertPostgresStableCatalogEqual(
	t *testing.T,
	actual schema.Table,
	want schema.Table,
) {
	t.Helper()
	same, err := stage4AdapterNetworkCatalogEqual(actual, want)
	if err != nil {
		t.Fatalf("compare PostgreSQL stable catalog: %v", err)
	}
	if !same {
		t.Fatalf(
			"PostgreSQL stable catalog changed: actual=%#v want=%#v",
			actual,
			want,
		)
	}
}
