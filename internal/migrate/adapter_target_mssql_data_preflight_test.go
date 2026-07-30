package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerTargetValueDomainsAcceptExactValues(t *testing.T) {
	table, columns := sqlServerTargetValueFixture()
	values := []any{
		time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
		time.Date(
			9999,
			time.December,
			31,
			23,
			59,
			59,
			123000000,
			time.UTC,
		),
		float64(float32(0.1)),
		math.MaxFloat64,
		nil,
	}
	if err := validateSQLServerTargetBatchValues(
		table,
		columns,
		[][]any{values},
	); err != nil {
		t.Fatalf("validate exact SQL Server target values: %v", err)
	}
}

func TestSQLServerTargetTimeValueDomainAcceptsDriverShapes(t *testing.T) {
	tests := []struct {
		name      string
		precision int
		value     any
	}{
		{
			name:      "pgx text protocol whole second",
			precision: 0,
			value:     "23:59:59",
		},
		{
			name:      "pgx binary result rendered as text",
			precision: 6,
			value:     "23:59:59.999999",
		},
		{
			name:      "pgx text protocol trims trailing zeroes",
			precision: 3,
			value:     "12:34:56.123",
		},
		{
			name:      "go-mssqldb native time",
			precision: 4,
			value: time.Date(
				1,
				time.January,
				1,
				12,
				34,
				56,
				123400000,
				time.FixedZone("driver location", -6*60*60),
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table, columns := sqlServerTargetTimeValueFixture(
				test.precision,
			)
			if err := validateSQLServerTargetBatchValues(
				table,
				columns,
				[][]any{{test.value}},
			); err != nil {
				t.Fatalf("validate exact SQL Server TIME value: %v", err)
			}
		})
	}
}

func TestSQLServerTargetTimeValueDomainFailsClosed(t *testing.T) {
	tests := []struct {
		name      string
		precision int
		value     any
	}{
		{
			name:      "PostgreSQL end of day",
			precision: 6,
			value:     "24:00:00.000000",
		},
		{
			name:      "precision loss",
			precision: 3,
			value:     "12:34:56.123456",
		},
		{
			name:      "unexpected timestamp anchor",
			precision: 6,
			value: time.Date(
				2026,
				time.July,
				29,
				12,
				34,
				56,
				0,
				time.UTC,
			),
		},
		{
			name:      "invalid minute",
			precision: 6,
			value:     "12:60:00.000000",
		},
		{
			name:      "time zone suffix",
			precision: 6,
			value:     "12:34:56-06",
		},
		{
			name:      "unexpected driver shape",
			precision: 6,
			value:     []byte("do-not-leak-source-time"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table, columns := sqlServerTargetTimeValueFixture(
				test.precision,
			)
			err := validateSQLServerTargetBatchValues(
				table,
				columns,
				[][]any{{test.value}},
			)
			if err == nil || !strings.Contains(err.Error(), "TIME") {
				t.Fatalf("validation error = %v, want TIME rejection", err)
			}
			if strings.Contains(err.Error(), "do-not-leak-source-time") {
				t.Fatalf("validation error leaked a source value: %v", err)
			}
		})
	}
}

func TestSQLServerTargetTimeDeclarationFailsClosed(t *testing.T) {
	tests := []schema.Column{
		{Name: "clock", Type: "time"},
		{
			Name: "clock",
			Type: "time",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{7},
			},
		},
		{
			Name: "clock",
			Type: "text",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{6},
			},
		},
	}
	for index, column := range tests {
		table := schema.Table{
			Schema:  "dbo",
			Name:    "clocks",
			Columns: []schema.Column{column},
		}
		err := validateSQLServerTargetBatchValues(
			table,
			[]string{"clock"},
			[][]any{{"12:34:56.000000"}},
		)
		if err == nil || !strings.Contains(err.Error(), "TIME") &&
			!strings.Contains(err.Error(), "declared type") {
			t.Fatalf("case %d declaration error = %v", index, err)
		}
	}
}

func TestSQLServerTargetValueDomainsFailClosed(t *testing.T) {
	table, columns := sqlServerTargetValueFixture()
	valid := []any{
		time.Date(2026, time.July, 29, 0, 0, 0, 0, time.UTC),
		time.Date(
			2026,
			time.July,
			29,
			12,
			30,
			45,
			123000000,
			time.UTC,
		),
		float64(float32(0.1)),
		1.25,
		nil,
	}
	tests := []struct {
		name   string
		index  int
		value  any
		needle string
	}{
		{
			name:   "DATE year zero",
			index:  0,
			value:  time.Date(0, time.January, 1, 0, 0, 0, 0, time.UTC),
			needle: "DATE",
		},
		{
			name:   "DATE carries a time",
			index:  0,
			value:  time.Date(2026, time.July, 29, 1, 0, 0, 0, time.UTC),
			needle: "DATE",
		},
		{
			name:  "DATETIME2 year ten thousand",
			index: 1,
			value: time.Date(
				10000,
				time.January,
				1,
				0,
				0,
				0,
				0,
				time.UTC,
			),
			needle: "DATETIME2",
		},
		{
			name:  "DATETIME2 precision loss",
			index: 1,
			value: time.Date(
				2026,
				time.July,
				29,
				12,
				30,
				45,
				123400000,
				time.UTC,
			),
			needle: "DATETIME2",
		},
		{
			name:   "REAL rounding",
			index:  2,
			value:  float64(0.1),
			needle: "REAL",
		},
		{
			name:   "REAL infinity",
			index:  2,
			value:  math.Inf(1),
			needle: "REAL",
		},
		{
			name:   "FLOAT NaN",
			index:  3,
			value:  math.NaN(),
			needle: "FLOAT",
		},
		{
			name:   "unexpected driver shape",
			index:  3,
			value:  "do-not-leak-source-value",
			needle: "FLOAT",
		},
		{
			name:   "NULL in required column",
			index:  0,
			value:  nil,
			needle: "non-nullable",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := append([]any(nil), valid...)
			values[test.index] = test.value
			err := validateSQLServerTargetBatchValues(
				table,
				columns,
				[][]any{values},
			)
			if err == nil || !strings.Contains(err.Error(), test.needle) {
				t.Fatalf("validation error = %v, want %q", err, test.needle)
			}
			if strings.Contains(err.Error(), "do-not-leak-source-value") {
				t.Fatalf("validation error leaked a source value: %v", err)
			}
		})
	}
}

func TestSQLServerTargetTimePreflightAndWriteBoundaryRejectEndOfDay(
	t *testing.T,
) {
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "clocks",
		Columns: []schema.Column{{
			Name: "clock",
			Type: "time",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{6},
			},
		}},
	}
	targetTable := sourceTable
	targetTable.Schema = "dbo"
	source := &sqlServerTargetValueFixtureSource{
		table: sourceTable,
		rows:  [][]any{{"24:00:00.000000"}},
	}
	adapter := &sqlServerTargetAdapter{}
	err := adapter.PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"clock"},
		}},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "TIME") {
		t.Fatalf("preflight error = %v, want TIME rejection", err)
	}
	if source.opens != 1 || source.closes != 1 {
		t.Fatalf(
			"source stream opens=%d closes=%d, want 1/1",
			source.opens,
			source.closes,
		)
	}

	writer := &sqlServerTargetValueFixtureWriter{}
	adapter.batchWriter = writer
	receipt, err := adapter.WriteBatch(
		context.Background(),
		targetTable,
		[]string{"clock"},
		"drop_recreate",
		[][]any{{"24:00:00.000000"}},
	)
	if err == nil || !strings.Contains(err.Error(), "TIME") {
		t.Fatalf("write-boundary error = %v, want TIME rejection", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer called %d times after TIME rejection", writer.calls)
	}
	if receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != 1 {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestSQLServerTargetPreflightsPostgresValuesBeforeMutation(
	t *testing.T,
) {
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name: "occurred",
				Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
		},
	}
	targetTable := cloneSQLServerTargetTable(sourceTable)
	targetTable.Schema = "dbo"
	targetTable.Columns[1].Type = "datetime"
	source := &sqlServerTargetValueFixtureSource{
		table: sourceTable,
		rows: [][]any{
			{
				int64(1),
				time.Date(
					10000,
					time.January,
					1,
					0,
					0,
					0,
					0,
					time.UTC,
				),
			},
		},
	}
	adapter := &sqlServerTargetAdapter{}
	err := adapter.PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id", "occurred"},
		}},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "DATETIME2") {
		t.Fatalf("preflight error = %v", err)
	}
	if source.opens != 1 || source.closes != 1 {
		t.Fatalf(
			"source stream opens=%d closes=%d, want 1/1",
			source.opens,
			source.closes,
		)
	}
}

func TestSQLServerTargetPreflightsMySQLValuesBeforeMutation(
	t *testing.T,
) {
	sourceTable := schema.Table{
		Schema: "dmtx",
		Name:   "clocks",
		Columns: []schema.Column{{
			Name: "clock",
			Type: "time",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{6},
			},
		}},
	}
	targetTable := sourceTable
	targetTable.Schema = "dbo"
	sourceFixture := &sqlServerTargetValueFixtureSource{
		table: sourceTable,
		rows:  [][]any{{"24:00:00.000000"}},
	}
	source := &mySQLServerTargetValueFixtureSource{
		sqlServerTargetValueFixtureSource: sourceFixture,
	}

	err := (&sqlServerTargetAdapter{}).PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"clock"},
		}},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "TIME") {
		t.Fatalf("preflight error = %v, want TIME rejection", err)
	}
	if sourceFixture.opens != 1 || sourceFixture.closes != 1 {
		t.Fatalf(
			"source stream opens=%d closes=%d, want 1/1",
			sourceFixture.opens,
			sourceFixture.closes,
		)
	}
}

func TestSQLServerTargetPreflightsEmptyIdentityPrimerBeforeMutation(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression("id < 0")
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(7)
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "empty_identity",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "ck_empty_identity",
			Expression: check,
		}},
	}
	targetTable := cloneSQLServerTargetTable(sourceTable)
	targetTable.Schema = "dbo"
	source := &sqlServerTargetValueFixtureSource{table: sourceTable}
	adapter := &sqlServerTargetAdapter{}

	err = adapter.PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id"},
		}},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "CHECK") {
		t.Fatalf("empty identity CHECK preflight error = %v", err)
	}
	if source.opens != 0 {
		t.Fatalf(
			"source stream opened %d times after identity preflight rejection",
			source.opens,
		)
	}
}

func TestSQLServerTargetEmptyIdentityPrimerPreflightRejectsForeignKeys(
	t *testing.T,
) {
	frontier := int64(7)
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "empty_identity",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{
				Name:         "parent_id",
				Type:         "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
				Nullable:     true,
			},
		},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "fk_empty_identity_parent",
			Columns:           []string{"parent_id"},
			ReferencedTable:   "empty_identity",
			ReferencedColumns: []string{"id"},
			OnUpdate:          "NO ACTION",
			OnDelete:          "NO ACTION",
			Match:             "SIMPLE",
		}},
	}
	targetTable := cloneSQLServerTargetTable(sourceTable)
	targetTable.Schema = "dbo"
	source := &sqlServerTargetValueFixtureSource{table: sourceTable}
	adapter := &sqlServerTargetAdapter{}

	err := adapter.PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id", "parent_id"},
		}},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "foreign key") {
		t.Fatalf("empty identity FK preflight error = %v", err)
	}
	if source.opens != 0 {
		t.Fatalf(
			"source stream opened %d times after identity preflight rejection",
			source.opens,
		)
	}
}

func TestSQLServerTargetIdentityPrimerPreflightAdmitsNonemptySource(
	t *testing.T,
) {
	check, err := schema.ParseSQLiteCheckExpression("id > 0")
	if err != nil {
		t.Fatal(err)
	}
	frontier := int64(7)
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "populated_identity",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "ck_populated_identity",
			Expression: check,
		}},
	}
	targetTable := cloneSQLServerTargetTable(sourceTable)
	targetTable.Schema = "dbo"
	source := &sqlServerTargetValueFixtureSource{
		table: sourceTable,
		rows:  [][]any{{int64(100)}},
	}

	if err := (&sqlServerTargetAdapter{}).PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id"},
		}},
		"drop_recreate",
	); err != nil {
		t.Fatalf("nonempty identity preflight: %v", err)
	}
}

func TestSQLServerTargetIdentityPreflightRejectsAmbiguousEmptyUpsert(
	t *testing.T,
) {
	frontier := int64(7)
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "empty_identity",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	targetTable := cloneSQLServerTargetTable(sourceTable)
	targetTable.Schema = "dbo"
	source := &sqlServerTargetValueFixtureSource{table: sourceTable}
	database := openSQLServerLifecycleTestDatabase(
		t,
		&sqlServerLifecycleDriverState{},
	)

	err := (&sqlServerTargetAdapter{
		database: database,
	}).PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id"},
		}},
		"upsert",
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"cannot be combined with uncalled retained target generator state exactly",
	) {
		t.Fatalf("empty upsert identity preflight error = %v", err)
	}
}

func TestSQLServerTargetIdentityPreflightAdmitsEmptyUpsertWithRetainedState(
	t *testing.T,
) {
	frontier := int64(7)
	sourceTable := schema.Table{
		Schema: "public",
		Name:   "empty_identity",
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
			Frontier:   &frontier,
		},
		Columns: []schema.Column{{
			Name:               "id",
			Type:               "bigint",
			DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			PrimaryKey:         true,
			PrimaryKeyPosition: 1,
		}},
	}
	targetTable := cloneSQLServerTargetTable(sourceTable)
	targetTable.Schema = "dbo"
	source := &sqlServerTargetValueFixtureSource{table: sourceTable}
	database := openSQLServerLifecycleTestDatabase(
		t,
		&sqlServerLifecycleDriverState{
			maximumValue: int64(100),
			currentValue: int64(90),
		},
	)

	if err := (&sqlServerTargetAdapter{
		database: database,
	}).PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  targetTable,
			columns: []string{"id"},
		}},
		"upsert",
	); err != nil {
		t.Fatalf("retained upsert identity preflight: %v", err)
	}
}

func TestSQLServerTargetWriteBoundaryRejectsDriftBeforeWriter(
	t *testing.T,
) {
	table, columns := sqlServerTargetValueFixture()
	writer := &sqlServerTargetValueFixtureWriter{}
	adapter := &sqlServerTargetAdapter{batchWriter: writer}
	rows := [][]any{{
		time.Date(2026, time.July, 29, 0, 0, 0, 0, time.UTC),
		time.Date(
			2026,
			time.July,
			29,
			12,
			30,
			45,
			123400000,
			time.UTC,
		),
		float64(float32(0.1)),
		1.25,
		nil,
	}}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		columns,
		"drop_recreate",
		rows,
	)
	if err == nil || !strings.Contains(err.Error(), "DATETIME2") {
		t.Fatalf("write-boundary error = %v", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer called %d times after value-domain rejection", writer.calls)
	}
	if receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != 1 {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestAdapterRunnerInvokesSourceDataPreflightBeforeMutation(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &recordingAdapterSource{
		events: &events,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
	}
	base := &recordingAdapterTarget{events: &events}
	target := &rejectingSourceDataPreflightTarget{
		recordingAdapterTarget: base,
		events:                 &events,
		err:                    errors.New("source domain rejected"),
	}
	_, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if err == nil || !strings.Contains(err.Error(), "source domain rejected") {
		t.Fatalf("runner error = %v", err)
	}
	if len(base.prepared) != 0 || len(base.written) != 0 {
		t.Fatalf(
			"target mutated after source preflight failure: prepare=%v write=%v",
			base.prepared,
			base.written,
		)
	}
	if got := fmt.Sprint(events); got != fmt.Sprint([]string{
		"source_list",
		"source_inspect",
		"target_plan",
		"target_preflight",
		"target_source_data_preflight",
	}) {
		t.Fatalf("events = %s", got)
	}
}

func sqlServerTargetValueFixture() (schema.Table, []string) {
	table := schema.Table{
		Schema: "dbo",
		Name:   "measurements",
		Columns: []schema.Column{
			{
				Name:         "day",
				Type:         "date",
				DeclaredType: &schema.DeclaredType{Base: "date"},
			},
			{
				Name: "occurred",
				Type: "datetime",
				DeclaredType: &schema.DeclaredType{
					Base:      "timestamp",
					Arguments: []int{3},
				},
			},
			{
				Name:         "ratio",
				Type:         "real",
				DeclaredType: &schema.DeclaredType{Base: "real"},
			},
			{
				Name: "score",
				Type: "double precision",
				DeclaredType: &schema.DeclaredType{
					Base: "double precision",
				},
			},
			{
				Name:         "optional_ratio",
				Type:         "real",
				Nullable:     true,
				DeclaredType: &schema.DeclaredType{Base: "real"},
			},
		},
	}
	return table, []string{
		"day",
		"occurred",
		"ratio",
		"score",
		"optional_ratio",
	}
}

func sqlServerTargetTimeValueFixture(
	precision int,
) (schema.Table, []string) {
	return schema.Table{
		Schema: "dbo",
		Name:   "clocks",
		Columns: []schema.Column{{
			Name: "clock",
			Type: "time",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{precision},
			},
		}},
	}, []string{"clock"}
}

type sqlServerTargetValueFixtureSource struct {
	table  schema.Table
	rows   [][]any
	opens  int
	closes int
}

func (*sqlServerTargetValueFixtureSource) Engine() string {
	return "postgres"
}

func (*sqlServerTargetValueFixtureSource) DisplayName() string {
	return "PostgreSQL"
}

type mySQLServerTargetValueFixtureSource struct {
	*sqlServerTargetValueFixtureSource
}

func (*mySQLServerTargetValueFixtureSource) Engine() string {
	return "mysql"
}

func (*mySQLServerTargetValueFixtureSource) DisplayName() string {
	return "MySQL"
}

func (source *sqlServerTargetValueFixtureSource) ListTables(
	context.Context,
) ([]string, error) {
	return []string{source.table.Name}, nil
}

func (source *sqlServerTargetValueFixtureSource) InspectTable(
	_ context.Context,
	name string,
) (schema.Table, error) {
	if name != source.table.Name {
		return schema.Table{}, fmt.Errorf("unexpected table %s", name)
	}
	return source.table, nil
}

func (source *sqlServerTargetValueFixtureSource) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	source.opens++
	return &sqlServerTargetValueFixtureRows{
		source: source,
		rows:   source.rows,
		index:  -1,
	}, nil
}

func (source *sqlServerTargetValueFixtureSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return len(source.rows), nil
}

func (*sqlServerTargetValueFixtureSource) Close() error {
	return nil
}

type sqlServerTargetValueFixtureRows struct {
	source *sqlServerTargetValueFixtureSource
	rows   [][]any
	index  int
}

func (rows *sqlServerTargetValueFixtureRows) Next() bool {
	rows.index++
	return rows.index < len(rows.rows)
}

func (rows *sqlServerTargetValueFixtureRows) Scan(destinations ...any) error {
	if rows.index < 0 || rows.index >= len(rows.rows) {
		return fmt.Errorf("fixture row index %d is invalid", rows.index)
	}
	values := rows.rows[rows.index]
	if len(destinations) != len(values) {
		return fmt.Errorf(
			"fixture destination count = %d, want %d",
			len(destinations),
			len(values),
		)
	}
	for index, destination := range destinations {
		pointer, ok := destination.(*any)
		if !ok {
			return fmt.Errorf(
				"fixture destination %d has type %T",
				index,
				destination,
			)
		}
		*pointer = values[index]
	}
	return nil
}

func (*sqlServerTargetValueFixtureRows) Err() error {
	return nil
}

func (rows *sqlServerTargetValueFixtureRows) Close() error {
	rows.source.closes++
	return nil
}

type sqlServerTargetValueFixtureWriter struct {
	calls int
}

func (writer *sqlServerTargetValueFixtureWriter) WriteBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	_ string,
	rows [][]any,
) (WriteReceipt, error) {
	writer.calls++
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

type rejectingSourceDataPreflightTarget struct {
	*recordingAdapterTarget
	events *[]string
	err    error
}

func (target *rejectingSourceDataPreflightTarget) PreflightSourceData(
	context.Context,
	sourceAdapter,
	[]adapterTablePlan,
	string,
) error {
	*target.events = append(*target.events, "target_source_data_preflight")
	return target.err
}
