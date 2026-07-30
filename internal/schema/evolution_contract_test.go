package schema_test

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/schema"
)

// This cross-package boundary test prevents the defensive target renderer from
// silently broadening beyond the authoritative Stage 4 contract planner. The
// renderer may remain narrower when an engine needs a dependent-object
// lifecycle, but every renderer-admitted type relation must also produce the
// planner's explicit widen_type action.
func TestEvolutionTypeAdmissionIsNoBroaderThanContractPlanner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		previous schema.Column
		current  schema.Column
	}{
		{
			name:     "integer to bigint",
			previous: schema.Column{Name: "value", Type: "integer"},
			current:  schema.Column{Name: "value", Type: "bigint"},
		},
		{
			name: "smallint to int",
			previous: schema.Column{
				Name: "value", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "smallint"},
			},
			current: schema.Column{
				Name: "value", Type: "integer",
				DeclaredType: &schema.DeclaredType{Base: "int"},
			},
		},
		{
			name: "varchar length",
			previous: schema.Column{
				Name: "value", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{20},
				},
			},
			current: schema.Column{
				Name: "value", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "numeric capacity",
			previous: schema.Column{
				Name: "value", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base: "decimal", Arguments: []int{12, 2},
				},
			},
			current: schema.Column{
				Name: "value", Type: "numeric",
				DeclaredType: &schema.DeclaredType{
					Base: "numeric", Arguments: []int{16, 4},
				},
			},
		},
		{
			name: "timestamp precision",
			previous: schema.Column{
				Name: "value", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{3},
				},
			},
			current: schema.Column{
				Name: "value", Type: "timestamp",
				DeclaredType: &schema.DeclaredType{
					Base: "timestamp", Arguments: []int{6},
				},
			},
		},
		{
			name:     "real to double",
			previous: schema.Column{Name: "value", Type: "real"},
			current:  schema.Column{Name: "value", Type: "double"},
		},
		{
			name: "text capacity",
			previous: schema.Column{
				Name: "value", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "text"},
			},
			current: schema.Column{
				Name: "value", Type: "text",
				DeclaredType: &schema.DeclaredType{Base: "longtext"},
			},
		},
		{
			name:     "integer narrowing",
			previous: schema.Column{Name: "value", Type: "bigint"},
			current:  schema.Column{Name: "value", Type: "integer"},
		},
		{
			name: "varchar narrowing",
			previous: schema.Column{
				Name: "value", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{80},
				},
			},
			current: schema.Column{
				Name: "value", Type: "varchar",
				DeclaredType: &schema.DeclaredType{
					Base: "varchar", Arguments: []int{40},
				},
			},
		},
		{
			name: "char length is not variable width",
			previous: schema.Column{
				Name: "value", Type: "char",
				DeclaredType: &schema.DeclaredType{
					Base: "char", Arguments: []int{20},
				},
			},
			current: schema.Column{
				Name: "value", Type: "char",
				DeclaredType: &schema.DeclaredType{
					Base: "char", Arguments: []int{40},
				},
			},
		},
		{
			name: "one-sided declaration",
			previous: schema.Column{
				Name: "value", Type: "integer",
			},
			current: schema.Column{
				Name: "value", Type: "bigint",
				DeclaredType: &schema.DeclaredType{Base: "bigint"},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertEvolutionAdmissionNoBroader(
				t,
				test.previous,
				test.current,
			)
		})
	}
}

func TestEvolutionTypeAdmissionGridIsNoBroaderThanContractPlanner(
	t *testing.T,
) {
	t.Parallel()

	forms := []schema.Column{
		{Type: "integer"},
		{Type: "bigint"},
		{Type: "real"},
		{Type: "double"},
		{Type: "varchar"},
		{Type: "text"},
		{Type: "varbinary"},
		{Type: "blob"},
		{
			Type:         "integer",
			DeclaredType: &schema.DeclaredType{Base: "smallint"},
		},
		{
			Type:         "integer",
			DeclaredType: &schema.DeclaredType{Base: "int"},
		},
		{
			Type:         "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
		{
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base: "decimal", Arguments: []int{12, 2},
			},
		},
		{
			Type: "numeric",
			DeclaredType: &schema.DeclaredType{
				Base: "numeric", Arguments: []int{16, 4},
			},
		},
		{
			Type: "varchar",
			DeclaredType: &schema.DeclaredType{
				Base: "varchar", Arguments: []int{20},
			},
		},
		{
			Type: "varchar",
			DeclaredType: &schema.DeclaredType{
				Base: "varchar", Arguments: []int{40},
			},
		},
		{
			Type: "varbinary",
			DeclaredType: &schema.DeclaredType{
				Base: "varbinary", Arguments: []int{20},
			},
		},
		{
			Type: "varbinary",
			DeclaredType: &schema.DeclaredType{
				Base: "varbinary", Arguments: []int{40},
			},
		},
		{
			Type:         "text",
			DeclaredType: &schema.DeclaredType{Base: "tinytext"},
		},
		{
			Type:         "text",
			DeclaredType: &schema.DeclaredType{Base: "text"},
		},
		{
			Type:         "text",
			DeclaredType: &schema.DeclaredType{Base: "longtext"},
		},
		{
			Type:         "blob",
			DeclaredType: &schema.DeclaredType{Base: "tinyblob"},
		},
		{
			Type:         "blob",
			DeclaredType: &schema.DeclaredType{Base: "blob"},
		},
		{
			Type:         "blob",
			DeclaredType: &schema.DeclaredType{Base: "longblob"},
		},
		{
			Type: "timestamp",
			DeclaredType: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{3},
			},
		},
		{
			Type: "timestamp",
			DeclaredType: &schema.DeclaredType{
				Base: "timestamp", Arguments: []int{6},
			},
		},
		{
			Type:         "real",
			DeclaredType: &schema.DeclaredType{Base: "real"},
		},
		{
			Type:         "double",
			DeclaredType: &schema.DeclaredType{Base: "double"},
		},
	}
	for previousIndex := range forms {
		for currentIndex := range forms {
			previous := forms[previousIndex]
			previous.Name = "value"
			current := forms[currentIndex]
			current.Name = "value"
			assertEvolutionAdmissionNoBroader(t, previous, current)
		}
	}
}

func assertEvolutionAdmissionNoBroader(
	t *testing.T,
	previousColumn schema.Column,
	currentColumn schema.Column,
) {
	t.Helper()
	previousTable := schema.Table{
		Schema: "target", Name: "items",
		Columns: []schema.Column{previousColumn},
	}
	currentTable := schema.Table{
		Schema: "target", Name: "items",
		Columns: []schema.Column{currentColumn},
	}
	catalog, err := schema.NewCompleteEvolutionCatalog(
		[]schema.Table{currentTable},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, rendererError := schema.PlanSafeTypeWidening(
		catalog,
		currentTable,
		previousColumn,
		currentColumn,
	)

	previous, err := schema.NewSchemaSnapshot(
		[]schema.Table{previousTable},
	)
	if err != nil {
		t.Fatal(err)
	}
	current, err := schema.NewSchemaSnapshot(
		[]schema.Table{currentTable},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan, plannerError := migrate.BuildSchemaContractPlan(
		previous,
		current,
		migrate.SchemaContractOptions{
			Contract: &config.SchemaContract{
				Tables:   config.SchemaContractEvolve,
				Columns:  config.SchemaContractEvolve,
				DataType: config.SchemaContractEvolve,
			},
			TargetMode: "upsert",
		},
	)
	plannerAdmitted := plannerError == nil &&
		hasWidenTypeAction(plan.Decisions)
	if rendererError == nil && !plannerAdmitted {
		t.Fatalf(
			"renderer admitted relation %q/%#v -> %q/%#v "+
				"broader than planner; planner error = %v, decisions = %#v",
			previousColumn.Type,
			previousColumn.DeclaredType,
			currentColumn.Type,
			currentColumn.DeclaredType,
			plannerError,
			plan.Decisions,
		)
	}
}

func hasWidenTypeAction(
	decisions []migrate.SchemaContractDecision,
) bool {
	for _, decision := range decisions {
		if decision.Action == migrate.SchemaContractWidenType {
			return true
		}
	}
	return false
}
