package migrate

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestAdapterIncrementalCatalogAdmission(t *testing.T) {
	t.Parallel()

	for _, sourceEngine := range []string{
		"postgres",
		"mysql",
		"mssql",
		"sqlite",
	} {
		sourceEngine := sourceEngine
		t.Run(sourceEngine, func(t *testing.T) {
			t.Parallel()

			table, namespace := adapterIncrementalFixture(sourceEngine)
			mapped, err := buildAdapterIncrementalTable(
				sourceEngine,
				namespace,
				table,
			)
			if err != nil {
				t.Fatal(err)
			}
			if mapped.Schema != table.Schema ||
				mapped.Name != table.Name ||
				len(mapped.Columns) != 4 {
				t.Fatalf("mapped table = %#v", mapped)
			}
			if mapped.Columns[0].PrimaryKeyPosition != 1 ||
				mapped.Columns[0].OrderAdmission != IncrementalOrderExact ||
				mapped.Columns[1].PrimaryKeyPosition != 2 ||
				mapped.Columns[1].OrderAdmission != IncrementalOrderExact {
				t.Fatalf("mapped primary key = %#v", mapped.Columns[:2])
			}
			if mapped.Columns[2].TemporalKind !=
				IncrementalTemporalTimestamp ||
				mapped.Columns[2].OrderAdmission != IncrementalOrderExact {
				t.Fatalf("mapped timestamp = %#v", mapped.Columns[2])
			}
			if mapped.Columns[3].TemporalKind != IncrementalTemporalNone ||
				mapped.Columns[3].OrderAdmission != "" {
				t.Fatalf("mapped TIME column = %#v", mapped.Columns[3])
			}
			plan, err := BuildIncrementalTablePlan(
				mapped,
				[]string{"missing", "clock_at", "updated_at"},
			)
			if err != nil {
				t.Fatal(err)
			}
			if plan.DateColumn == nil ||
				plan.DateColumn.Name != "updated_at" ||
				len(plan.CandidateDecisions) != 3 ||
				plan.CandidateDecisions[1].Action !=
					IncrementalCandidateIncompatibleType {
				t.Fatalf("candidate plan = %#v", plan)
			}
		})
	}
}

func TestAdapterIncrementalCatalogFailsClosed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
		mutate func(*schema.Table)
	}{
		{
			name:   "nullable primary key",
			engine: "postgres",
			mutate: func(table *schema.Table) {
				table.Columns[0].Nullable = true
			},
		},
		{
			name:   "contradictory primary key flag",
			engine: "mysql",
			mutate: func(table *schema.Table) {
				table.Columns[0].PrimaryKey = false
			},
		},
		{
			name:   "PostgreSQL timestamp wrong declaration base",
			engine: "postgres",
			mutate: func(table *schema.Table) {
				table.Columns[2].DeclaredType.Base = "datetime"
			},
		},
		{
			name:   "MySQL timestamp missing precision",
			engine: "mysql",
			mutate: func(table *schema.Table) {
				table.Columns[2].DeclaredType = nil
			},
		},
		{
			name:   "MySQL timestamp missing explicit precision",
			engine: "mysql",
			mutate: func(table *schema.Table) {
				table.Columns[2].DeclaredType.Arguments = nil
			},
		},
		{
			name:   "SQL Server datetime precision too wide",
			engine: "mssql",
			mutate: func(table *schema.Table) {
				table.Columns[2].DeclaredType.Arguments[0] = 7
			},
		},
		{
			name:   "SQL Server datetime missing explicit precision",
			engine: "mssql",
			mutate: func(table *schema.Table) {
				table.Columns[2].DeclaredType.Arguments = nil
			},
		},
		{
			name:   "SQLite timestamp ambiguous modifiers",
			engine: "sqlite",
			mutate: func(table *schema.Table) {
				precision := int64(3)
				table.Columns[2].DeclaredType.
					FractionalSecondPrecision = &precision
			},
		},
		{
			name:   "PostgreSQL table identifier exceeds catalog bound",
			engine: "postgres",
			mutate: func(table *schema.Table) {
				table.Name = strings.Repeat("t", 64)
			},
		},
		{
			name:   "MySQL column identifier exceeds catalog bound",
			engine: "mysql",
			mutate: func(table *schema.Table) {
				table.Columns[0].Name = strings.Repeat("c", 65)
			},
		},
		{
			name:   "SQL Server column identifier exceeds catalog bound",
			engine: "mssql",
			mutate: func(table *schema.Table) {
				table.Columns[0].Name = strings.Repeat("c", 129)
			},
		},
		{
			name:   "SQLite table identifier contains NUL",
			engine: "sqlite",
			mutate: func(table *schema.Table) {
				table.Name = "events\x00shadow"
			},
		},
		{
			name:   "invalid UTF-8 column identifier",
			engine: "postgres",
			mutate: func(table *schema.Table) {
				table.Columns[0].Name = string([]byte{0xff})
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			table, namespace := adapterIncrementalFixture(test.engine)
			test.mutate(&table)
			if _, err := buildAdapterIncrementalTable(
				test.engine,
				namespace,
				table,
			); err == nil {
				t.Fatal("unsafe incremental catalog unexpectedly succeeded")
			}
		})
	}
}

func TestAdapterIncrementalCatalogRejectsInvalidNamespace(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name      string
		engine    string
		namespace string
	}{
		{
			name:      "PostgreSQL overlong",
			engine:    "postgres",
			namespace: strings.Repeat("s", 64),
		},
		{
			name:      "MySQL overlong",
			engine:    "mysql",
			namespace: strings.Repeat("界", 65),
		},
		{
			name:      "SQL Server overlong",
			engine:    "mssql",
			namespace: strings.Repeat("s", 129),
		},
		{
			name:      "invalid UTF-8",
			engine:    "postgres",
			namespace: string([]byte{0xff}),
		},
		{
			name:      "NUL",
			engine:    "mysql",
			namespace: "source\x00shadow",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			table, _ := adapterIncrementalFixture(test.engine)
			table.Schema = test.namespace
			if _, err := buildAdapterIncrementalTable(
				test.engine,
				test.namespace,
				table,
			); ClassifyTransferError(err) != ErrorClassPolicy {
				t.Fatalf(
					"invalid namespace error = %v, want policy",
					err,
				)
			}
		})
	}
}

func TestAdapterIncrementalQuotedWhitespaceIdentifiersAndExactCase(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		engine string
		table  string
		first  string
		date   string
	}{
		{
			engine: "postgres",
			table:  `" events "`,
			first:  `" tenant_id "`,
			date:   `"Updated At"`,
		},
		{
			engine: "mysql",
			table:  "` events `",
			first:  "` tenant_id `",
			date:   "`Updated At`",
		},
		{
			engine: "mssql",
			table:  "[ events ]",
			first:  "[ tenant_id ]",
			date:   "[Updated At]",
		},
		{
			engine: "sqlite",
			table:  `" events "`,
			first:  `" tenant_id "`,
			date:   `"Updated At"`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.engine, func(t *testing.T) {
			t.Parallel()

			table, namespace := adapterIncrementalFixture(test.engine)
			table.Name = " events "
			table.Columns[0].Name = " tenant_id "
			table.Columns[1].Name = "id with space"
			table.Columns[2].Name = "Updated At"
			mapped, err := buildAdapterIncrementalTable(
				test.engine,
				namespace,
				table,
			)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := BuildIncrementalTablePlan(
				mapped,
				[]string{"updated at", "Updated At"},
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.CandidateDecisions) != 2 ||
				plan.CandidateDecisions[0].Action !=
					IncrementalCandidateMissing ||
				plan.DateColumn == nil ||
				plan.DateColumn.Name != "Updated At" {
				t.Fatalf("case-sensitive candidate plan = %#v", plan)
			}
			lower := time.Date(
				2026,
				time.July,
				30,
				12,
				0,
				0,
				123_000_000,
				time.UTC,
			)
			upper := lower.Add(time.Second)
			query, err := buildAdapterIncrementalReadQuery(
				test.engine,
				namespace,
				table,
				[]string{
					" tenant_id ",
					"id with space",
					"Updated At",
				},
				IncrementalReadPlan{
					Table:    mapped,
					Scope:    IncrementalReadWindow,
					Ordering: windowIncrementalOrdering(plan.Ordering),
					Window: &IncrementalWindow{
						Column: "Updated At", Lower: &lower, Upper: &upper,
						LowerExclusive: true, UpperInclusive: true,
						ExcludeNull: true,
					},
					Resumed:                  true,
					ReplayFromLowerWatermark: true,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, fragment := range []string{
				test.table,
				test.first,
				test.date,
			} {
				if !strings.Contains(query.SQL, fragment) {
					t.Fatalf(
						"quoted whitespace query %q lacks %q",
						query.SQL,
						fragment,
					)
				}
			}
		})
	}
}

func TestAdapterIncrementalCatalogTemporalVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		engine    string
		column    schema.Column
		wantKind  IncrementalTemporalKind
		wantBase  string
		wantScale int
		wantZone  bool
	}{
		{
			name:     "PostgreSQL date",
			engine:   "postgres",
			column:   schema.Column{Name: "updated_at", Type: "date"},
			wantKind: IncrementalTemporalDate,
			wantBase: "date",
		},
		{
			name:   "PostgreSQL timestamptz default precision",
			engine: "postgres",
			column: schema.Column{
				Name: "updated_at",
				Type: "timestamptz",
			},
			wantKind:  IncrementalTemporalTimestamp,
			wantBase:  "timestamptz",
			wantScale: 6,
			wantZone:  true,
		},
		{
			name:   "PostgreSQL structured timestamp precision",
			engine: "postgres",
			column: schema.Column{
				Name: "updated_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:                      "timestamp",
					FractionalSecondPrecision: adapterIncrementalInt64(4),
				},
			},
			wantKind:  IncrementalTemporalTimestamp,
			wantBase:  "timestamp",
			wantScale: 4,
		},
		{
			name:   "MySQL date",
			engine: "mysql",
			column: schema.Column{
				Name: "updated_at",
				Type: "date",
				DeclaredType: &schema.DeclaredType{
					Base: "date",
				},
			},
			wantKind: IncrementalTemporalDate,
			wantBase: "date",
		},
		{
			name:   "MySQL timestamp",
			engine: "mysql",
			column: schema.Column{
				Name: "updated_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{6},
				},
			},
			wantKind:  IncrementalTemporalTimestamp,
			wantBase:  "timestamp",
			wantScale: 6,
			wantZone:  true,
		},
		{
			name:   "SQL Server date",
			engine: "mssql",
			column: schema.Column{
				Name: "updated_at",
				Type: "date",
				DeclaredType: &schema.DeclaredType{
					Base: "date",
				},
			},
			wantKind: IncrementalTemporalDate,
			wantBase: "date",
		},
		{
			name:   "SQL Server smalldatetime",
			engine: "mssql",
			column: schema.Column{
				Name: "updated_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base: "smalldatetime",
				},
			},
			wantKind: IncrementalTemporalTimestamp,
			wantBase: "smalldatetime",
		},
		{
			name:   "SQLite datetime",
			engine: "sqlite",
			column: schema.Column{
				Name: "updated_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:                      "datetime",
					FractionalSecondPrecision: adapterIncrementalInt64(9),
				},
			},
			wantKind:  IncrementalTemporalTimestamp,
			wantBase:  "datetime",
			wantScale: 9,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			temporal, admitted, err := adapterIncrementalTemporalColumn(
				test.engine,
				test.column,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !admitted ||
				temporal.kind != test.wantKind ||
				temporal.base != test.wantBase ||
				temporal.precision != test.wantScale ||
				temporal.zoneAware != test.wantZone {
				t.Fatalf("temporal admission = %#v", temporal)
			}
		})
	}
}

func TestAdapterIncrementalReadQueryShapes(t *testing.T) {
	t.Parallel()

	lower := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_000_000,
		time.UTC,
	)
	upper := lower.Add(time.Second)
	tests := []struct {
		engine        string
		wantQualified string
		wantPredicate string
		wantOrder     string
		wantArgs      []any
	}{
		{
			engine:        "postgres",
			wantQualified: `"stage4"."events"`,
			wantPredicate: `"updated_at" IS NOT NULL AND "updated_at" > $1 AND "updated_at" <= $2`,
			wantOrder:     `"updated_at" ASC, "tenant_id" ASC, "id" ASC`,
			wantArgs:      []any{lower, upper},
		},
		{
			engine:        "mysql",
			wantQualified: "`stage4`.`events`",
			wantPredicate: "`updated_at` IS NOT NULL AND `updated_at` > ? AND `updated_at` <= ?",
			wantOrder:     "`updated_at` ASC, `tenant_id` ASC, `id` ASC",
			wantArgs: []any{
				"2026-07-30 12:00:00.123",
				"2026-07-30 12:00:01.123",
			},
		},
		{
			engine:        "mssql",
			wantQualified: "[stage4].[events]",
			wantPredicate: "[updated_at] IS NOT NULL AND [updated_at] > @p1 AND [updated_at] <= @p2",
			wantOrder:     "[updated_at] ASC, [tenant_id] ASC, [id] ASC",
			wantArgs:      []any{lower, upper},
		},
		{
			engine:        "sqlite",
			wantQualified: `"events"`,
			wantPredicate: `"updated_at" IS NOT NULL AND "updated_at" > ? AND "updated_at" <= ?`,
			wantOrder:     `"updated_at" ASC, "tenant_id" ASC, "id" ASC`,
			wantArgs: []any{
				"2026-07-30 12:00:00.123",
				"2026-07-30 12:00:01.123",
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.engine, func(t *testing.T) {
			t.Parallel()

			table, namespace := adapterIncrementalFixture(test.engine)
			mapped, err := buildAdapterIncrementalTable(
				test.engine,
				namespace,
				table,
			)
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
			read := IncrementalReadPlan{
				Table:    mapped,
				Scope:    IncrementalReadWindow,
				Ordering: windowIncrementalOrdering(plan.Ordering),
				Window: &IncrementalWindow{
					Column:         "updated_at",
					Lower:          &lower,
					Upper:          &upper,
					LowerExclusive: true,
					UpperInclusive: true,
					ExcludeNull:    true,
				},
				Resumed:                  true,
				ReplayFromLowerWatermark: true,
			}
			query, err := buildAdapterIncrementalReadQuery(
				test.engine,
				namespace,
				table,
				[]string{"tenant_id", "id", "updated_at", "clock_at"},
				read,
			)
			if err != nil {
				t.Fatal(err)
			}
			for _, fragment := range []string{
				"SELECT ",
				" FROM " + test.wantQualified,
				" WHERE " + test.wantPredicate,
				" ORDER BY " + test.wantOrder,
			} {
				if !strings.Contains(query.SQL, fragment) {
					t.Fatalf(
						"query %q does not contain %q",
						query.SQL,
						fragment,
					)
				}
			}
			if strings.Contains(query.SQL, "2026-07-30") {
				t.Fatalf("bound value was rendered into SQL: %s", query.SQL)
			}
			if !reflect.DeepEqual(query.Args, test.wantArgs) {
				t.Fatalf("query args = %#v, want %#v", query.Args, test.wantArgs)
			}
			if test.engine == "mysql" &&
				!strings.Contains(
					query.SQL,
					"CAST(`updated_at` AS CHAR) AS `updated_at`",
				) {
				t.Fatalf("MySQL temporal wrapper projection was lost: %s", query.SQL)
			}
			if test.engine == "sqlite" &&
				!strings.Contains(
					query.SQL,
					`CAST("updated_at" AS TEXT) AS "updated_at"`,
				) {
				t.Fatalf("SQLite temporal projection was lost: %s", query.SQL)
			}
		})
	}
}

func TestAdapterIncrementalBaselineOrderingMatrix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		engine    string
		wantOrder string
	}{
		{
			engine: "postgres",
			wantOrder: `"updated_at" ASC NULLS FIRST, ` +
				`"tenant_id" ASC, "id" ASC`,
		},
		{
			engine: "mysql",
			wantOrder: "CASE WHEN `updated_at` IS NULL " +
				"THEN 0 ELSE 1 END ASC, `updated_at` ASC, " +
				"`tenant_id` ASC, `id` ASC",
		},
		{
			engine: "mssql",
			wantOrder: "CASE WHEN [updated_at] IS NULL " +
				"THEN 0 ELSE 1 END ASC, [updated_at] ASC, " +
				"[tenant_id] ASC, [id] ASC",
		},
		{
			engine: "sqlite",
			wantOrder: `CASE WHEN "updated_at" IS NULL ` +
				`THEN 0 ELSE 1 END ASC, "updated_at" ASC, ` +
				`"tenant_id" ASC, "id" ASC`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.engine, func(t *testing.T) {
			t.Parallel()

			table, namespace := adapterIncrementalFixture(test.engine)
			mapped, err := buildAdapterIncrementalTable(
				test.engine,
				namespace,
				table,
			)
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
			query, err := buildAdapterIncrementalReadQuery(
				test.engine,
				namespace,
				table,
				[]string{"tenant_id", "id", "updated_at"},
				IncrementalReadPlan{
					Table:    mapped,
					Scope:    IncrementalReadFullTable,
					Ordering: baselineIncrementalOrdering(plan.Ordering),
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(query.SQL, " WHERE ") ||
				!strings.HasSuffix(
					query.SQL,
					" ORDER BY "+test.wantOrder,
				) {
				t.Fatalf("baseline query = %s", query.SQL)
			}
		})
	}
}

func TestAdapterIncrementalBaselineAndEmptyWindowQueries(t *testing.T) {
	t.Parallel()

	table, namespace := adapterIncrementalFixture("postgres")
	mapped, err := buildAdapterIncrementalTable(
		"postgres",
		namespace,
		table,
	)
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
	baseline, err := buildAdapterIncrementalReadQuery(
		"postgres",
		namespace,
		table,
		[]string{"tenant_id", "id", "updated_at"},
		IncrementalReadPlan{
			Table:    mapped,
			Scope:    IncrementalReadFullTable,
			Ordering: baselineIncrementalOrdering(plan.Ordering),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(baseline.SQL, " WHERE ") ||
		!strings.HasSuffix(
			baseline.SQL,
			`ORDER BY "updated_at" ASC NULLS FIRST, "tenant_id" ASC, "id" ASC`,
		) {
		t.Fatalf("baseline query = %s", baseline.SQL)
	}

	unchanged := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_000_000,
		time.UTC,
	)
	emptyRead, err := incrementalAttemptRead(
		plan,
		state.IncrementalAttempt{
			Mode: state.IncrementalWindow,
			LowerWatermark: &state.TimestampWatermark{
				Column: "updated_at",
				Value:  unchanged,
			},
			UpperFence: &state.TimestampWatermark{
				Column: "updated_at",
				Value:  unchanged,
			},
		},
		true,
	)
	if err != nil {
		t.Fatal(err)
	}
	if emptyRead.Window == nil ||
		!emptyRead.Window.Empty ||
		emptyRead.Window.Lower == nil ||
		emptyRead.Window.Upper == nil ||
		!emptyRead.Window.Lower.Equal(*emptyRead.Window.Upper) {
		t.Fatalf("core unchanged window = %#v", emptyRead.Window)
	}
	empty, err := buildAdapterIncrementalReadQuery(
		"postgres",
		namespace,
		table,
		[]string{"tenant_id", "id", "updated_at"},
		emptyRead,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(empty.SQL, " WHERE 1 = 0 ORDER BY ") ||
		len(empty.Args) != 0 {
		t.Fatalf("empty query = %#v", empty)
	}
}

func TestAdapterIncrementalWindowRejectsContradictoryEmptyShapes(
	t *testing.T,
) {
	t.Parallel()

	table, namespace := adapterIncrementalFixture("postgres")
	mapped, err := buildAdapterIncrementalTable(
		"postgres",
		namespace,
		table,
	)
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
	lower := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_000_000,
		time.UTC,
	)
	upper := lower.Add(time.Second)
	for name, window := range map[string]*IncrementalWindow{
		"empty upper without lower": {
			Column: "updated_at", Upper: &upper, Empty: true,
			LowerExclusive: true, UpperInclusive: true, ExcludeNull: true,
		},
		"empty increasing bounds": {
			Column: "updated_at", Lower: &lower, Upper: &upper, Empty: true,
			LowerExclusive: true, UpperInclusive: true, ExcludeNull: true,
		},
		"empty regressing bounds": {
			Column: "updated_at", Lower: &upper, Upper: &lower, Empty: true,
			LowerExclusive: true, UpperInclusive: true, ExcludeNull: true,
		},
		"non-empty equal bounds": {
			Column: "updated_at", Lower: &lower, Upper: &lower,
			LowerExclusive: true, UpperInclusive: true, ExcludeNull: true,
		},
	} {
		window := window
		t.Run(name, func(t *testing.T) {
			_, err := buildAdapterIncrementalReadQuery(
				"postgres",
				namespace,
				table,
				[]string{"tenant_id", "id", "updated_at"},
				IncrementalReadPlan{
					Table:                    mapped,
					Scope:                    IncrementalReadWindow,
					Ordering:                 windowIncrementalOrdering(plan.Ordering),
					Window:                   window,
					Resumed:                  true,
					ReplayFromLowerWatermark: true,
				},
			)
			if err == nil ||
				!strings.Contains(err.Error(), "contradictory bounds") {
				t.Fatalf("contradictory window error = %v", err)
			}
		})
	}
}

func TestAdapterIncrementalReadRejectsPositionalOrImpreciseResume(t *testing.T) {
	t.Parallel()

	table, namespace := adapterIncrementalFixture("mysql")
	mapped, err := buildAdapterIncrementalTable("mysql", namespace, table)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildIncrementalTablePlan(mapped, []string{"updated_at"})
	if err != nil {
		t.Fatal(err)
	}
	upper := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_000_001,
		time.UTC,
	)
	read := IncrementalReadPlan{
		Table:    mapped,
		Scope:    IncrementalReadWindow,
		Ordering: windowIncrementalOrdering(plan.Ordering),
		Window: &IncrementalWindow{
			Column:         "updated_at",
			Upper:          &upper,
			LowerExclusive: true,
			UpperInclusive: true,
			ExcludeNull:    true,
		},
		Resumed: true,
	}
	if _, err := buildAdapterIncrementalReadQuery(
		"mysql",
		namespace,
		table,
		[]string{"tenant_id", "id", "updated_at"},
		read,
	); err == nil {
		t.Fatal("resume without full-window replay unexpectedly succeeded")
	}
	read.ReplayFromLowerWatermark = true
	read.PositionalRestoreAllowed = true
	if _, err := buildAdapterIncrementalReadQuery(
		"mysql",
		namespace,
		table,
		[]string{"tenant_id", "id", "updated_at"},
		read,
	); err == nil {
		t.Fatal("positional incremental resume unexpectedly succeeded")
	}
	read.PositionalRestoreAllowed = false
	if _, err := buildAdapterIncrementalReadQuery(
		"mysql",
		namespace,
		table,
		[]string{"tenant_id", "id", "updated_at"},
		read,
	); err == nil {
		t.Fatal("sub-millisecond fence unexpectedly succeeded")
	}
}

func TestAdapterIncrementalFenceQueriesAndNormalization(t *testing.T) {
	t.Parallel()

	for _, sourceEngine := range []string{
		"postgres",
		"mysql",
		"mssql",
		"sqlite",
	} {
		sourceEngine := sourceEngine
		t.Run(sourceEngine, func(t *testing.T) {
			t.Parallel()

			table, namespace := adapterIncrementalFixture(sourceEngine)
			column := table.Columns[2]
			query, err := buildAdapterIncrementalFenceQuery(
				sourceEngine,
				namespace,
				table,
				column,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(query.SQL, "MAX(") ||
				!strings.Contains(query.SQL, " IS NOT NULL") {
				t.Fatalf("fence query = %s", query.SQL)
			}
			if sourceEngine == "sqlite" {
				for _, fragment := range []string{
					"typeof(",
					"length(",
					"GLOB",
					"datetime(",
				} {
					if !strings.Contains(query.SQL, fragment) {
						t.Fatalf(
							"SQLite fence query %q lacks %q",
							query.SQL,
							fragment,
						)
					}
				}
			}

			want := time.Date(
				2026,
				time.July,
				30,
				12,
				34,
				56,
				123_000_000,
				time.UTC,
			)
			raw := any(want)
			if sourceEngine == "mysql" || sourceEngine == "sqlite" {
				raw = "2026-07-30 12:34:56.123"
			}
			got, err := normalizeAdapterIncrementalFence(
				sourceEngine,
				column,
				raw,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || !got.Equal(want) {
				t.Fatalf("normalized fence = %v, want %v", got, want)
			}
		})
	}

	table, _ := adapterIncrementalFixture("mysql")
	if _, err := normalizeAdapterIncrementalFence(
		"mysql",
		table.Columns[2],
		"2026-07-30 12:34:56.12",
	); err == nil {
		t.Fatal("MySQL fence with wrong explicit precision unexpectedly succeeded")
	}
}

func TestAdapterIncrementalInstantBoundsNormalizeUTC(t *testing.T) {
	t.Parallel()

	local := time.Date(
		2026,
		time.July,
		30,
		12,
		0,
		0,
		123_000_000,
		time.FixedZone("fixture-west", -5*60*60),
	)
	tests := []struct {
		name   string
		engine string
		column schema.Column
		want   any
	}{
		{
			name:   "PostgreSQL timestamptz",
			engine: "postgres",
			column: schema.Column{
				Name: "updated_at",
				Type: "timestamptz",
			},
			want: local.UTC(),
		},
		{
			name:   "MySQL-family timestamp",
			engine: "mysql",
			column: schema.Column{
				Name: "updated_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
			want: "2026-07-30 17:00:00.123",
		},
		{
			name:   "MySQL-family datetime remains wall clock",
			engine: "mysql",
			column: schema.Column{
				Name: "updated_at",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "datetime",
					Arguments: []int{3},
				},
			},
			want: "2026-07-30 12:00:00.123",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			temporal, admitted, err :=
				adapterIncrementalTemporalColumn(
					test.engine,
					test.column,
				)
			if err != nil || !admitted {
				t.Fatalf(
					"temporal=%#v admitted=%v error=%v",
					temporal,
					admitted,
					err,
				)
			}
			got, err := adapterIncrementalBoundValue(
				test.engine,
				temporal,
				local,
			)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("bound = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestAdapterIncrementalAcceptsMinimumSQLTimestamp(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		engine string
		column schema.Column
		raw    any
	}{
		{
			name:   "PostgreSQL timestamp",
			engine: "postgres",
			column: schema.Column{
				Name: "updated_at",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{0},
				},
			},
			raw: time.Time{},
		},
		{
			name:   "SQLite date",
			engine: "sqlite",
			column: schema.Column{
				Name: "updated_at",
				Type: "date",
				DeclaredType: &schema.DeclaredType{
					Base: "date",
				},
			},
			raw: "0001-01-01",
		},
		{
			name:   "SQL Server date",
			engine: "mssql",
			column: schema.Column{
				Name: "updated_at",
				Type: "date",
				DeclaredType: &schema.DeclaredType{
					Base: "date",
				},
			},
			raw: time.Time{},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := normalizeAdapterIncrementalFence(
				test.engine,
				test.column,
				test.raw,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || !got.Equal(time.Time{}) {
				t.Fatalf("minimum SQL timestamp = %v", got)
			}
		})
	}
}

func TestAdapterIncrementalSQLiteNanosecondFenceAndBounds(t *testing.T) {
	t.Parallel()

	table, namespace := adapterIncrementalFixture("sqlite")
	table.Columns[2].DeclaredType.Arguments = []int{9}
	column := table.Columns[2]
	query, err := buildAdapterIncrementalFenceQuery(
		"sqlite",
		namespace,
		table,
		column,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		`length("updated_at") = 29`,
		strings.Repeat("[0-9]", 9),
	} {
		if !strings.Contains(query.SQL, fragment) {
			t.Fatalf("precision-9 fence query %q lacks %q", query.SQL, fragment)
		}
	}
	want := time.Date(
		2026,
		time.July,
		30,
		12,
		34,
		56,
		123_456_789,
		time.UTC,
	)
	normalized, err := normalizeAdapterIncrementalFence(
		"sqlite",
		column,
		"2026-07-30 12:34:56.123456789",
	)
	if err != nil {
		t.Fatal(err)
	}
	if normalized == nil || !normalized.Equal(want) {
		t.Fatalf("precision-9 normalized fence = %v, want %v", normalized, want)
	}
	temporal, admitted, err := adapterIncrementalTemporalColumn(
		"sqlite",
		column,
	)
	if err != nil || !admitted {
		t.Fatalf("precision-9 temporal admission = %#v, %v", temporal, err)
	}
	bound, err := adapterIncrementalBoundValue("sqlite", temporal, want)
	if err != nil {
		t.Fatal(err)
	}
	if bound != "2026-07-30 12:34:56.123456789" {
		t.Fatalf("precision-9 bound = %#v", bound)
	}
	if !validAdapterIncrementalTemporalPrecision(123_456_789, 9) ||
		validAdapterIncrementalTemporalPrecision(123_456_789, 8) ||
		validAdapterIncrementalTemporalPrecision(0, -1) ||
		validAdapterIncrementalTemporalPrecision(0, 10) {
		t.Fatal("engine-neutral precision admission is not exact over 0..9")
	}
}

func TestRequireIncrementalSourceRejectsClickHouse(t *testing.T) {
	t.Parallel()

	_, err := requireIncrementalSourceAdapter(
		adapterIncrementalUnsupportedSource{engine: "clickhouse"},
	)
	if err == nil || !strings.Contains(err.Error(), "ClickHouse") {
		t.Fatalf("ClickHouse capability error = %v", err)
	}
}

func adapterIncrementalFixture(
	sourceEngine string,
) (schema.Table, string) {
	namespace := "stage4"
	table := schema.Table{
		Schema: namespace,
		Name:   "events",
		Columns: []schema.Column{
			{
				Name:               "tenant_id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 2,
			},
			{
				Name:     "updated_at",
				Nullable: true,
			},
			{
				Name: "clock_at",
				Type: "time",
			},
		},
	}
	switch sourceEngine {
	case "postgres":
		table.Columns[2].Type = "timestamp"
		table.Columns[2].DeclaredType = &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{3},
		}
		table.Columns[3].DeclaredType = &schema.DeclaredType{
			Base:      "time",
			Arguments: []int{3},
		}
	case "mysql":
		table.Columns[2].Type = "datetime"
		table.Columns[2].DeclaredType = &schema.DeclaredType{
			Base:      "datetime",
			Arguments: []int{3},
		}
		table.Columns[3].DeclaredType = &schema.DeclaredType{
			Base:      "time",
			Arguments: []int{3},
		}
	case "mssql":
		table.Columns[2].Type = "datetime"
		table.Columns[2].DeclaredType = &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{3},
		}
		table.Columns[3].DeclaredType = &schema.DeclaredType{
			Base:      "time",
			Arguments: []int{3},
		}
	case "sqlite":
		namespace = ""
		table.Schema = ""
		table.Columns[2].Type = "timestamp"
		table.Columns[2].DeclaredType = &schema.DeclaredType{
			Base:      "timestamp",
			Arguments: []int{3},
		}
		table.Columns[3].DeclaredType = &schema.DeclaredType{
			Base:      "time",
			Arguments: []int{3},
		}
	}
	return table, namespace
}

func adapterIncrementalInt64(value int64) *int64 {
	return &value
}

type adapterIncrementalUnsupportedSource struct {
	engine string
}

func (source adapterIncrementalUnsupportedSource) Engine() string {
	return source.engine
}

func (adapterIncrementalUnsupportedSource) DisplayName() string {
	return "unsupported"
}

func (adapterIncrementalUnsupportedSource) ListTables(
	context.Context,
) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (adapterIncrementalUnsupportedSource) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	return schema.Table{}, errors.New("not implemented")
}

func (adapterIncrementalUnsupportedSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, errors.New("not implemented")
}

func (adapterIncrementalUnsupportedSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, errors.New("not implemented")
}

func (adapterIncrementalUnsupportedSource) Close() error {
	return nil
}
