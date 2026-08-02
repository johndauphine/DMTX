package migrate

import (
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4SpatialMetadataRouteMatrixFailsClosedBeforeMutation(
	t *testing.T,
) {
	t.Parallel()

	source := stage4SpatialMetadataTestTable()
	oracle := &mysqlTargetAdapter{
		flavor:    engine.MySQLServerFlavorOracle80,
		namespace: "target",
	}
	planned, err := oracle.PlanTables(
		"mysql",
		[]schema.Table{source},
		"drop_recreate",
	)
	if err != nil {
		t.Fatalf("Oracle MySQL native spatial plan: %v", err)
	}
	if len(planned) != 1 ||
		planned[0].Columns[1].DeclaredType == nil ||
		planned[0].Columns[1].DeclaredType.Spatial == nil {
		t.Fatalf("Oracle MySQL native spatial plan = %#v", planned)
	}

	tests := []struct {
		name string
		plan func() error
	}{
		{
			name: "MariaDB target",
			plan: func() error {
				_, err := (&mysqlTargetAdapter{
					flavor:    engine.MySQLServerFlavorMariaDB1011,
					namespace: "target",
				}).PlanTables(
					"mysql",
					[]schema.Table{source},
					"drop_recreate",
				)
				return err
			},
		},
		{
			name: "PostgreSQL target",
			plan: func() error {
				_, err := (&postgresTargetAdapter{
					namespace: "public",
				}).PlanTables(
					"mysql",
					[]schema.Table{source},
					"drop_recreate",
				)
				return err
			},
		},
		{
			name: "SQL Server target",
			plan: func() error {
				_, err := (&sqlServerTargetAdapter{
					namespace: "dbo",
				}).PlanTables(
					"mysql",
					[]schema.Table{source},
					"drop_recreate",
				)
				return err
			},
		},
		{
			name: "SQLite target",
			plan: func() error {
				_, err := (&sqliteTargetAdapter{}).PlanTables(
					"mysql",
					[]schema.Table{source},
					"drop_recreate",
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.plan(); err == nil {
				t.Fatal("non-preserving spatial route was admitted")
			}
		})
	}

	if err := ValidateMigration(config.Config{
		Source: config.Endpoint{Type: "mysql"},
		Target: config.Endpoint{Type: "clickhouse"},
		Migration: config.Migration{
			TargetMode: "drop_recreate",
		},
	}); err == nil {
		t.Fatal("uncertified MySQL-to-ClickHouse route was admitted")
	}
}

func stage4SpatialMetadataTestTable() schema.Table {
	zero := uint32(0)
	return schema.Table{
		Schema:         "source",
		Name:           "places",
		MySQLCollation: "utf8mb4_0900_bin",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
				DeclaredType:       &schema.DeclaredType{Base: "bigint"},
			},
			{
				Name: "position",
				Type: "point",
				DeclaredType: &schema.DeclaredType{
					Base: "point",
					Spatial: &schema.SpatialTypeMetadata{
						Subtype: schema.SpatialSubtypePoint,
						SRID:    &zero,
					},
				},
			},
		},
	}
}
