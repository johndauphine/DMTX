package migrate

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestMySQLTargetAcceptsExactSQLServerTemporalValues(t *testing.T) {
	table, columns := mySQLTargetSQLServerTemporalFixture()
	values := []any{
		time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC),
		"23:59:59.123456",
		time.Date(
			9999,
			12,
			31,
			23,
			59,
			59,
			123000000,
			time.UTC,
		),
		nil,
	}
	if err := validateMySQLTargetSQLServerBatchValues(
		table,
		columns,
		[][]any{values},
	); err != nil {
		t.Fatalf("validate exact SQL Server temporal values: %v", err)
	}
}

func TestMySQLTargetRejectsUnsafeSQLServerTemporalValues(t *testing.T) {
	table, columns := mySQLTargetSQLServerTemporalFixture()
	valid := []any{
		time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
		"12:34:56.123456",
		time.Date(2026, 7, 30, 12, 34, 56, 123000000, time.UTC),
		nil,
	}
	tests := []struct {
		name   string
		index  int
		value  any
		needle string
	}{
		{
			name:   "DATE below MySQL range",
			index:  0,
			value:  time.Date(999, 12, 31, 0, 0, 0, 0, time.UTC),
			needle: "DATE",
		},
		{
			name:   "DATE carries time",
			index:  0,
			value:  time.Date(2026, 7, 30, 1, 0, 0, 0, time.UTC),
			needle: "DATE",
		},
		{
			name:   "DATE unexpected shape",
			index:  0,
			value:  "secret-date-value",
			needle: "DATE",
		},
		{
			name:   "DATE non-UTC location",
			index:  0,
			value:  time.Date(2026, 7, 30, 0, 0, 0, 0, time.FixedZone("CST", -21600)),
			needle: "DATE",
		},
		{
			name:   "TIME missing exact fraction",
			index:  1,
			value:  "12:34:56",
			needle: "TIME",
		},
		{
			name:   "TIME end of day",
			index:  1,
			value:  "24:00:00.000000",
			needle: "TIME",
		},
		{
			name:   "TIME unexpected bytes",
			index:  1,
			value:  []byte("secret-time-value"),
			needle: "TIME",
		},
		{
			name:  "DATETIME below MySQL range",
			index: 2,
			value: time.Date(
				999, 12, 31, 23, 59, 59, 0, time.UTC,
			),
			needle: "DATETIME",
		},
		{
			name:  "DATETIME precision loss",
			index: 2,
			value: time.Date(
				2026, 7, 30, 12, 34, 56, 123400000, time.UTC,
			),
			needle: "DATETIME",
		},
		{
			name:   "DATETIME unexpected shape",
			index:  2,
			value:  "secret-datetime-value",
			needle: "DATETIME",
		},
		{
			name:   "NULL in required DATE",
			index:  0,
			value:  nil,
			needle: "non-nullable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := append([]any(nil), valid...)
			values[test.index] = test.value
			err := validateMySQLTargetSQLServerBatchValues(
				table,
				columns,
				[][]any{values},
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.needle) {
				t.Fatalf(
					"validation error = %v, want %q",
					err,
					test.needle,
				)
			}
			for _, secret := range []string{
				"secret-date-value",
				"secret-time-value",
				"secret-datetime-value",
			} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("validation error leaked value: %v", err)
				}
			}
		})
	}
}

func TestMySQLTargetSQLServerTemporalDeclarationsFailClosed(t *testing.T) {
	tests := []schema.Column{
		{Name: "value", Type: "date"},
		{
			Name: "value",
			Type: "date",
			DeclaredType: &schema.DeclaredType{
				Base:      "date",
				Arguments: []int{1},
			},
		},
		{
			Name: "value",
			Type: "time",
			DeclaredType: &schema.DeclaredType{
				Base:      "time",
				Arguments: []int{7},
			},
		},
		{
			Name: "value",
			Type: "datetime",
			DeclaredType: &schema.DeclaredType{
				Base:      "timestamp",
				Arguments: []int{6},
			},
		},
	}
	for index, column := range tests {
		err := validateMySQLTargetSQLServerBatchValues(
			schema.Table{
				Name:    "events",
				Columns: []schema.Column{column},
			},
			[]string{"value"},
			[][]any{{
				time.Date(
					2026,
					7,
					30,
					12,
					0,
					0,
					0,
					time.UTC,
				),
			}},
		)
		if err == nil {
			t.Fatalf("case %d unexpectedly accepted", index)
		}
	}
}

func TestMySQLTargetPreflightsAndRechecksSQLServerValues(t *testing.T) {
	table, columns := mySQLTargetSQLServerTemporalFixture()
	sourceTable := table
	sourceTable.Schema = "dbo"
	source := &mySQLTargetSQLServerFixtureSource{
		table: sourceTable,
		rows: [][]any{{
			time.Date(999, 1, 1, 0, 0, 0, 0, time.UTC),
			"12:34:56.123456",
			time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC),
			nil,
		}},
	}
	writer := &mysqlTargetWriterRecorder{
		receipt: WriteReceipt{
			Certainty:     CommitDurable,
			AttemptedRows: 1,
			CommittedRows: 1,
		},
	}
	adapter := &mysqlTargetAdapter{batchWriter: writer}
	err := adapter.PreflightSourceData(
		context.Background(),
		source,
		[]adapterTablePlan{{
			source:  sourceTable,
			target:  table,
			columns: columns,
		}},
		"drop_recreate",
	)
	if err == nil || !strings.Contains(err.Error(), "DATE") {
		t.Fatalf("preflight error = %v, want DATE rejection", err)
	}
	if source.opens != 1 || source.closes != 1 {
		t.Fatalf(
			"source stream opens=%d closes=%d, want 1/1",
			source.opens,
			source.closes,
		)
	}

	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		columns,
		"drop_recreate",
		[][]any{{
			time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
			"12:34:56.123456",
			time.Date(
				2026,
				7,
				30,
				12,
				0,
				0,
				123400000,
				time.UTC,
			),
			nil,
		}},
	)
	if err == nil || !strings.Contains(err.Error(), "DATETIME") {
		t.Fatalf("write-boundary error = %v, want DATETIME rejection", err)
	}
	if writer.calls != 0 {
		t.Fatalf("writer called %d times after rejection", writer.calls)
	}
	if receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != 1 ||
		receipt.CommittedRows != 0 {
		t.Fatalf("receipt = %#v", receipt)
	}

	receipt, err = adapter.WriteBatch(
		context.Background(),
		table,
		columns,
		"drop_recreate",
		[][]any{{
			time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC),
			"12:34:56.123456",
			time.Date(
				2026,
				7,
				30,
				12,
				0,
				0,
				123000000,
				time.UTC,
			),
			nil,
		}},
	)
	if err != nil {
		t.Fatalf("write exact changed source row: %v", err)
	}
	if writer.calls != 1 || receipt != writer.receipt {
		t.Fatalf(
			"valid write calls=%d receipt=%#v, want 1 and %#v",
			writer.calls,
			receipt,
			writer.receipt,
		)
	}
}

func TestMySQLTargetSkipsSQLServerShapeForOtherSources(t *testing.T) {
	adapter := &mysqlTargetAdapter{
		validateSQLServerSourceValues: true,
	}
	source := &mySQLTargetSQLServerFixtureSource{
		engine: "postgres",
	}
	if err := adapter.PreflightSourceData(
		context.Background(),
		source,
		nil,
		"drop_recreate",
	); err != nil {
		t.Fatal(err)
	}
	if adapter.validateSQLServerSourceValues {
		t.Fatal("SQL Server write-boundary validation remained enabled")
	}
	if source.opens != 0 {
		t.Fatalf("non-SQL Server source opened %d times", source.opens)
	}
}

func mySQLTargetSQLServerTemporalFixture() (
	schema.Table,
	[]string,
) {
	return schema.Table{
			Schema: "target",
			Name:   "events",
			Columns: []schema.Column{
				{
					Name: "event_date",
					Type: "date",
					DeclaredType: &schema.DeclaredType{
						Base: "date",
					},
				},
				{
					Name: "event_time",
					Type: "time",
					DeclaredType: &schema.DeclaredType{
						Base:      "time",
						Arguments: []int{6},
					},
				},
				{
					Name: "occurred_at",
					Type: "datetime",
					DeclaredType: &schema.DeclaredType{
						Base:      "datetime",
						Arguments: []int{3},
					},
				},
				{
					Name:     "optional_date",
					Type:     "date",
					Nullable: true,
					DeclaredType: &schema.DeclaredType{
						Base: "date",
					},
				},
			},
		}, []string{
			"event_date",
			"event_time",
			"occurred_at",
			"optional_date",
		}
}

type mySQLTargetSQLServerFixtureSource struct {
	engine string
	table  schema.Table
	rows   [][]any
	opens  int
	closes int
}

func (source *mySQLTargetSQLServerFixtureSource) Engine() string {
	if source.engine == "" {
		return "mssql"
	}
	return source.engine
}

func (*mySQLTargetSQLServerFixtureSource) DisplayName() string {
	return "fixture"
}

func (*mySQLTargetSQLServerFixtureSource) ListTables(
	context.Context,
) ([]string, error) {
	return nil, nil
}

func (source *mySQLTargetSQLServerFixtureSource) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	return source.table, nil
}

func (source *mySQLTargetSQLServerFixtureSource) OpenRows(
	_ context.Context,
	_ schema.Table,
	_ []string,
) (adapterRows, error) {
	source.opens++
	return &mySQLTargetSQLServerFixtureRows{
		source: source,
		rows:   source.rows,
	}, nil
}

func (*mySQLTargetSQLServerFixtureSource) PreflightRows(
	context.Context,
	[]schema.Table,
) error {
	return nil
}

func (source *mySQLTargetSQLServerFixtureSource) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return len(source.rows), nil
}

func (*mySQLTargetSQLServerFixtureSource) Close() error {
	return nil
}

type mySQLTargetSQLServerFixtureRows struct {
	source *mySQLTargetSQLServerFixtureSource
	rows   [][]any
	index  int
}

func (*mySQLTargetSQLServerFixtureRows) Columns() ([]string, error) {
	return nil, nil
}

func (rows *mySQLTargetSQLServerFixtureRows) Next() bool {
	return rows.index < len(rows.rows)
}

func (rows *mySQLTargetSQLServerFixtureRows) Scan(
	destinations ...any,
) error {
	values := rows.rows[rows.index]
	rows.index++
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

func (*mySQLTargetSQLServerFixtureRows) Err() error {
	return nil
}

func (rows *mySQLTargetSQLServerFixtureRows) Close() error {
	rows.source.closes++
	return nil
}
