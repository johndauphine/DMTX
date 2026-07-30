package migrate

import (
	"fmt"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerSourceRowsNormalizeNumericAndUUID(t *testing.T) {
	source := &sqlServerSourceFixtureRows{values: []any{
		[]byte("-12345678901234567890.123456"),
		[]byte{
			0xff, 0x19, 0x96, 0x6f,
			0x86, 0x8b,
			0x11, 0xd0,
			0xb4, 0x2d,
			0x00, 0xc0, 0x4f, 0xc9, 0x64, 0xff,
		},
		nil,
		[]byte{0x00, 0xff},
	}}
	table := schema.Table{Columns: []schema.Column{
		{
			Name: "amount",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "decimal",
				Arguments: []int{30, 6},
			},
		},
		{
			Name: "record_id",
			Type: "uuid",
			DeclaredType: &schema.DeclaredType{
				Base: "uuid",
			},
		},
		{
			Name: "optional_amount",
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base:      "numeric",
				Arguments: []int{8, 2},
			},
		},
		{
			Name: "payload",
			Type: "blob",
		},
	}}
	rows := wrapSQLServerSourceRows(
		source,
		table,
		[]string{"amount", "record_id", "optional_amount", "payload"},
	)
	destinations := make([]any, 4)
	pointers := make([]any, 4)
	for index := range destinations {
		pointers[index] = &destinations[index]
	}
	if err := rows.Scan(pointers...); err != nil {
		t.Fatalf("scan normalized values: %v", err)
	}
	if got, want := destinations[0], "-12345678901234567890.123456"; got != want {
		t.Fatalf("numeric = %#v, want %q", got, want)
	}
	if got, want := destinations[1], "ff19966f-868b-11d0-b42d-00c04fc964ff"; got != want {
		t.Fatalf("UUID = %#v, want %q", got, want)
	}
	if destinations[2] != nil {
		t.Fatalf("NULL numeric = %#v", destinations[2])
	}
	payload, ok := destinations[3].([]byte)
	if !ok || len(payload) != 2 || payload[0] != 0 || payload[1] != 0xff {
		t.Fatalf("binary payload = %#v", destinations[3])
	}
}

func TestSQLServerSourceRowsRejectUnexpectedValueShapes(t *testing.T) {
	tests := []struct {
		name       string
		column     schema.Column
		value      any
		wantReason string
	}{
		{
			name: "numeric text instead of driver bytes",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{8, 2},
				},
			},
			value:      "12.34",
			wantReason: "unexpected value shape",
		},
		{
			name: "numeric exponent",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{8, 2},
				},
			},
			value:      []byte("1e2"),
			wantReason: "invalid exact numeric",
		},
		{
			name: "numeric scale mismatch",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "decimal",
					Arguments: []int{8, 2},
				},
			},
			value:      []byte("12.3"),
			wantReason: "invalid exact numeric",
		},
		{
			name: "numeric integer overflow",
			column: schema.Column{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{5, 2},
				},
			},
			value:      []byte("1234.56"),
			wantReason: "invalid exact numeric",
		},
		{
			name: "short UUID",
			column: schema.Column{
				Name: "record_id",
				Type: "uuid",
				DeclaredType: &schema.DeclaredType{
					Base: "uuid",
				},
			},
			value:      []byte{1, 2, 3},
			wantReason: "invalid UUID",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := scanSQLServerSourceFixture(test.column, test.value)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("error = %v, want reason %q", err, test.wantReason)
			}
			if strings.Contains(err.Error(), fmt.Sprint(test.value)) {
				t.Fatalf("error leaked source value: %v", err)
			}
		})
	}
}

func TestSQLServerSourceRowsRejectInvalidMetadataAndMissingColumn(
	t *testing.T,
) {
	tests := []struct {
		name    string
		table   schema.Table
		columns []string
		value   any
		reason  string
	}{
		{
			name: "numeric modifiers absent",
			table: schema.Table{Columns: []schema.Column{{
				Name: "amount",
				Type: "numeric",
			}}},
			columns: []string{"amount"},
			value:   []byte("1.00"),
			reason:  "numeric declaration is missing",
		},
		{
			name: "UUID declaration mismatch",
			table: schema.Table{Columns: []schema.Column{{
				Name: "record_id",
				Type: "uuid",
				DeclaredType: &schema.DeclaredType{
					Base: "uniqueidentifier",
				},
			}}},
			columns: []string{"record_id"},
			value:   make([]byte, 16),
			reason:  "UUID declaration is invalid",
		},
		{
			name:    "selected column absent",
			columns: []string{"missing"},
			value:   int64(1),
			reason:  "column is absent from discovered schema",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &sqlServerSourceFixtureRows{
				values: []any{test.value},
			}
			rows := wrapSQLServerSourceRows(
				source,
				test.table,
				test.columns,
			)
			var got any
			err := rows.Scan(&got)
			if err == nil || !strings.Contains(err.Error(), test.reason) {
				t.Fatalf("error = %v, want reason %q", err, test.reason)
			}
		})
	}
}

func TestSQLServerSourceRowsLeaveOtherTypesUnwrapped(t *testing.T) {
	source := &sqlServerSourceFixtureRows{values: []any{[]byte{1}}}
	rows := wrapSQLServerSourceRows(
		source,
		schema.Table{Columns: []schema.Column{{
			Name: "payload",
			Type: "blob",
		}}},
		[]string{"payload"},
	)
	if rows != source {
		t.Fatal("non-converted SQL Server rows were unnecessarily wrapped")
	}
}

func scanSQLServerSourceFixture(
	column schema.Column,
	value any,
) error {
	source := &sqlServerSourceFixtureRows{values: []any{value}}
	rows := wrapSQLServerSourceRows(
		source,
		schema.Table{Columns: []schema.Column{column}},
		[]string{column.Name},
	)
	var got any
	return rows.Scan(&got)
}

type sqlServerSourceFixtureRows struct {
	values []any
}

func (rows *sqlServerSourceFixtureRows) Next() bool {
	return true
}

func (rows *sqlServerSourceFixtureRows) Scan(destinations ...any) error {
	if len(destinations) != len(rows.values) {
		return fmt.Errorf(
			"fixture destination count = %d, want %d",
			len(destinations),
			len(rows.values),
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
		*pointer = rows.values[index]
	}
	return nil
}

func (rows *sqlServerSourceFixtureRows) Err() error {
	return nil
}

func (rows *sqlServerSourceFixtureRows) Close() error {
	return nil
}
