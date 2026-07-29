package migrate

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/johndauphine/dmtx/internal/schema"
)

func postgresModifiedScalarTable() schema.Table {
	return schema.Table{
		Schema: "public",
		Name:   "scalar_values",
		Columns: []schema.Column{
			{
				Name: "code",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "varchar",
					Arguments: []int{5},
				},
			},
			{
				Name: "fixed",
				Type: "text",
				DeclaredType: &schema.DeclaredType{
					Base:      "char",
					Arguments: []int{4},
				},
			},
			{
				Name: "amount",
				Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base:      "numeric",
					Arguments: []int{5, 2},
				},
			},
		},
	}
}

func TestNormalizePostgresRowsHonorsDeclaredScalarModifiers(t *testing.T) {
	table := postgresModifiedScalarTable()
	rows, err := normalizePostgresRows(
		table,
		[]string{"code", "fixed", "amount"},
		[][]any{{"ééééé", "AB", "999.9900"}},
	)
	if err != nil {
		t.Fatalf("normalizePostgresRows: %v", err)
	}
	if rows[0][0] != "ééééé" || rows[0][1] != "AB" {
		t.Fatalf("normalized text values = %#v", rows[0][:2])
	}
	numeric, ok := rows[0][2].(pgtype.Numeric)
	if !ok || !numeric.Valid ||
		numeric.Int.String() != "99999" || numeric.Exp != -2 {
		t.Fatalf("normalized numeric = %#v", rows[0][2])
	}
}

func TestNormalizePostgresRowsRejectsModifierViolationsBeforeCopy(
	t *testing.T,
) {
	tests := []struct {
		name       string
		columns    []string
		row        []any
		want       string
		secretPart string
	}{
		{
			name:       "varchar rune length",
			columns:    []string{"code"},
			row:        []any{"éééééé"},
			want:       "character length 5",
			secretPart: "éééééé",
		},
		{
			name:       "char rune length",
			columns:    []string{"fixed"},
			row:        []any{"abcde"},
			want:       "character length 4",
			secretPart: "abcde",
		},
		{
			name:       "numeric scale",
			columns:    []string{"amount"},
			row:        []any{"1.234"},
			want:       "DECIMAL(5,2) scale",
			secretPart: "1.234",
		},
		{
			name:       "numeric integer precision",
			columns:    []string{"amount"},
			row:        []any{"1000.00"},
			want:       "DECIMAL(5,2) integer precision",
			secretPart: "1000.00",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := normalizePostgresRows(
				postgresModifiedScalarTable(),
				test.columns,
				[][]any{test.row},
			)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
			if strings.Contains(err.Error(), test.secretPart) {
				t.Fatalf("normalization error leaked row value: %v", err)
			}
		})
	}
}

func TestNormalizePostgresRowsRejectsInvalidNumericDeclaration(t *testing.T) {
	table := postgresModifiedScalarTable()
	table.Columns[2].DeclaredType.Arguments = []int{0, 0}
	_, err := normalizePostgresRows(
		table,
		[]string{"amount"},
		[][]any{{"0"}},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "invalid PostgreSQL numeric precision") {
		t.Fatalf("invalid declaration error = %v", err)
	}
}
