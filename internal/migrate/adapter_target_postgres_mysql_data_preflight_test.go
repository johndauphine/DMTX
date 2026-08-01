package migrate

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPlanPostgresMySQLSourceDataProbesBuildsExactQueries(
	t *testing.T,
) {
	sourceColumns := []schema.Column{
		{
			Name:         "id",
			Type:         "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
		{
			Name: "tenant`key",
			Type: "varchar",
			DeclaredType: &schema.DeclaredType{
				Base:      "varchar",
				Arguments: []int{24},
			},
		},
		{
			Name:         "payload",
			Type:         "longtext",
			DeclaredType: &schema.DeclaredType{Base: "longtext"},
		},
		{
			Name:         "amount",
			Type:         "decimal",
			DeclaredType: &schema.DeclaredType{Base: "decimal", Arguments: []int{12, 2}},
		},
		{
			Name:         "parent_id",
			Type:         "bigint",
			DeclaredType: &schema.DeclaredType{Base: "bigint"},
		},
		{
			Name:         "parent_revision",
			Type:         "integer",
			DeclaredType: &schema.DeclaredType{Base: "int"},
		},
		{
			Name:         "binary_payload",
			Type:         "varbinary",
			DeclaredType: &schema.DeclaredType{Base: "varbinary", Arguments: []int{32}},
		},
	}
	check, err := schema.ParseMySQLCatalogCheck(
		"`amount` >= 0 AND `payload` IS NOT NULL",
		sourceColumns,
	)
	if err != nil {
		t.Fatal(err)
	}
	source := schema.Table{
		Schema:  "app`data",
		Name:    "event`items",
		Columns: sourceColumns,
		Indexes: []schema.Index{
			{
				Name: "ix_parent",
				Columns: []schema.IndexColumn{
					{Name: "parent_id"},
				},
			},
			{
				Name:   "ux_tenant_revision",
				Unique: true,
				Columns: []schema.IndexColumn{
					{Name: "tenant`key", Descending: true},
					{Name: "parent_revision"},
				},
			},
		},
		ForeignKeys: []schema.ForeignKey{{
			Name:              "fk_parent",
			Columns:           []string{"parent_id", "parent_revision"},
			ReferencedTable:   "parent`items",
			ReferencedColumns: []string{"id", "revision"},
		}},
		Checks: []schema.CheckConstraint{{
			Name:       "ck_amount_payload",
			Expression: check,
		}},
	}
	target := source
	target.Schema = "public"
	target.Columns = append([]schema.Column(nil), source.Columns...)
	target.Columns[1].Type = "varchar"
	target.Columns[1].DeclaredType = &schema.DeclaredType{
		Base:      "varchar",
		Arguments: []int{24},
	}
	target.Columns[2].Type = "text"
	target.Columns[2].DeclaredType = nil
	target.Columns[6].Type = "bytea"
	target.Columns[6].DeclaredType = nil

	got, err := planPostgresMySQLSourceDataProbes([]adapterTablePlan{{
		source: source,
		target: target,
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []postgresMySQLSourceDataProbe{
		{
			kind:   postgresMySQLSourceDataProbeNUL,
			table:  "event`items",
			object: "tenant`key",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" WHERE LOCATE(0x00, CAST(`tenant``key` AS BINARY)) > 0)",
		},
		{
			kind:   postgresMySQLSourceDataProbeNUL,
			table:  "event`items",
			object: "payload",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" WHERE LOCATE(0x00, CAST(`payload` AS BINARY)) > 0)",
		},
		{
			kind:   postgresMySQLSourceDataProbeCheck,
			table:  "event`items",
			object: "ck_amount_payload",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" WHERE NOT (`amount` >= 0 AND `payload` IS NOT NULL))",
		},
		{
			kind:   postgresMySQLSourceDataProbeForeignKey,
			table:  "event`items",
			object: "fk_parent",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" AS `dmtx_child` LEFT JOIN `app``data`.`parent``items`" +
				" AS `dmtx_parent` ON `dmtx_child`.`parent_id` =" +
				" `dmtx_parent`.`id` AND `dmtx_child`.`parent_revision` =" +
				" `dmtx_parent`.`revision` WHERE" +
				" `dmtx_child`.`parent_id` IS NOT NULL AND" +
				" `dmtx_child`.`parent_revision` IS NOT NULL AND" +
				" `dmtx_parent`.`id` IS NULL)",
		},
		{
			kind:   postgresMySQLSourceDataProbeUniqueIndex,
			table:  "event`items",
			object: "ux_tenant_revision",
			query: "SELECT EXISTS (SELECT 1 FROM `app``data`.`event``items`" +
				" WHERE `tenant``key` IS NOT NULL AND" +
				" `parent_revision` IS NOT NULL GROUP BY" +
				" `tenant``key`, `parent_revision` HAVING COUNT(*) > 1)",
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("probes = %#v, want %#v", got, want)
	}
}

func TestPlanPostgresMySQLSourceDataProbesFailsClosed(
	t *testing.T,
) {
	base := func() adapterTablePlan {
		source := schema.Table{
			Schema: "app",
			Name:   "items",
			Columns: []schema.Column{
				{
					Name:         "id",
					Type:         "bigint",
					DeclaredType: &schema.DeclaredType{Base: "bigint"},
				},
				{
					Name:         "payload",
					Type:         "longtext",
					DeclaredType: &schema.DeclaredType{Base: "longtext"},
				},
			},
		}
		target := source
		target.Schema = "public"
		target.Columns = append([]schema.Column(nil), source.Columns...)
		target.Columns[1].Type = "text"
		target.Columns[1].DeclaredType = nil
		return adapterTablePlan{source: source, target: target}
	}
	tests := []struct {
		name    string
		mutate  func(*adapterTablePlan)
		wantErr string
	}{
		{
			name: "target text column missing",
			mutate: func(plan *adapterTablePlan) {
				plan.target.Columns = plan.target.Columns[:1]
			},
			wantErr: "target text column payload is missing",
		},
		{
			name: "source declared type missing",
			mutate: func(plan *adapterTablePlan) {
				plan.source.Columns[1].DeclaredType = nil
			},
			wantErr: "column payload: declared type is missing",
		},
		{
			name: "target text type mismatch",
			mutate: func(plan *adapterTablePlan) {
				plan.target.Columns[1].Type = "bytea"
			},
			wantErr: `target column payload has type "bytea", want text or varchar`,
		},
		{
			name: "unrenderable CHECK",
			mutate: func(plan *adapterTablePlan) {
				plan.source.Checks = []schema.CheckConstraint{{
					Name: "ck_payload",
				}}
			},
			wantErr: "CHECK items.ck_payload",
		},
		{
			name: "foreign key width mismatch",
			mutate: func(plan *adapterTablePlan) {
				plan.source.ForeignKeys = []schema.ForeignKey{{
					Name:              "fk_parent",
					Columns:           []string{"id", "payload"},
					ReferencedTable:   "parents",
					ReferencedColumns: []string{"id"},
				}}
			},
			wantErr: "incomplete column metadata",
		},
		{
			name: "foreign key unknown local column",
			mutate: func(plan *adapterTablePlan) {
				plan.source.ForeignKeys = []schema.ForeignKey{{
					Name:              "fk_parent",
					Columns:           []string{"missing"},
					ReferencedTable:   "parents",
					ReferencedColumns: []string{"id"},
				}}
			},
			wantErr: "invalid column pair at position 1",
		},
		{
			name: "unique index without columns",
			mutate: func(plan *adapterTablePlan) {
				plan.source.Indexes = []schema.Index{{
					Name:   "ux_payload",
					Unique: true,
				}}
			},
			wantErr: "unique index items.ux_payload: no columns",
		},
		{
			name: "unique index unknown column",
			mutate: func(plan *adapterTablePlan) {
				plan.source.Indexes = []schema.Index{{
					Name:   "ux_payload",
					Unique: true,
					Columns: []schema.IndexColumn{{
						Name: "missing",
					}},
				}}
			},
			wantErr: "unknown column missing",
		},
		{
			name: "duplicate source column",
			mutate: func(plan *adapterTablePlan) {
				plan.source.Columns = append(
					plan.source.Columns,
					plan.source.Columns[0],
				)
			},
			wantErr: "duplicate source column id",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := base()
			test.mutate(&plan)
			probes, err := planPostgresMySQLSourceDataProbes(
				[]adapterTablePlan{plan},
			)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("probes = %#v, error = %v", probes, err)
			}
			if probes != nil {
				t.Fatalf("partial probes escaped failure: %#v", probes)
			}
		})
	}
}

type scriptedPostgresMySQLSourceDataRunner struct {
	results []struct {
		invalid bool
		err     error
	}
	queries []string
}

func (runner *scriptedPostgresMySQLSourceDataRunner) hasInvalidRow(
	_ context.Context,
	query string,
) (bool, error) {
	runner.queries = append(runner.queries, query)
	if len(runner.results) == 0 {
		return false, errors.New("unexpected probe")
	}
	result := runner.results[0]
	runner.results = runner.results[1:]
	return result.invalid, result.err
}

func TestRunPostgresMySQLSourceDataProbesRunsInPlanOrder(
	t *testing.T,
) {
	probes := []postgresMySQLSourceDataProbe{
		{
			kind:   postgresMySQLSourceDataProbeNUL,
			table:  "items",
			object: "payload",
			query:  "nul-query",
		},
		{
			kind:   postgresMySQLSourceDataProbeCheck,
			table:  "items",
			object: "ck_items",
			query:  "check-query",
		},
	}
	runner := &scriptedPostgresMySQLSourceDataRunner{
		results: []struct {
			invalid bool
			err     error
		}{
			{},
			{},
		},
	}
	if err := runPostgresMySQLSourceDataProbes(
		context.Background(),
		runner,
		probes,
	); err != nil {
		t.Fatal(err)
	}
	if want := []string{"nul-query", "check-query"}; !reflect.DeepEqual(
		runner.queries,
		want,
	) {
		t.Fatalf("queries = %#v, want %#v", runner.queries, want)
	}
}

func TestRunPostgresMySQLSourceDataProbesReportsSafeViolations(
	t *testing.T,
) {
	tests := []struct {
		name    string
		kind    postgresMySQLSourceDataProbeKind
		object  string
		wantErr string
	}{
		{
			name:    "embedded NUL",
			kind:    postgresMySQLSourceDataProbeNUL,
			object:  "payload",
			wantErr: "text column payload contains an embedded NUL",
		},
		{
			name:    "CHECK",
			kind:    postgresMySQLSourceDataProbeCheck,
			object:  "ck_amount",
			wantErr: "CHECK ck_amount is violated by historical rows",
		},
		{
			name:    "foreign key",
			kind:    postgresMySQLSourceDataProbeForeignKey,
			object:  "fk_parent",
			wantErr: "foreign key fk_parent has orphan rows",
		},
		{
			name:    "unique index",
			kind:    postgresMySQLSourceDataProbeUniqueIndex,
			object:  "ux_email",
			wantErr: "unique index ux_email has duplicate fully-nonnull keys",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner := &scriptedPostgresMySQLSourceDataRunner{
				results: []struct {
					invalid bool
					err     error
				}{{invalid: true}},
			}
			err := runPostgresMySQLSourceDataProbes(
				context.Background(),
				runner,
				[]postgresMySQLSourceDataProbe{{
					kind:   test.kind,
					table:  "items",
					object: test.object,
					query:  "probe-query",
				}},
			)
			if err == nil ||
				!strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v", err)
			}
			if strings.Contains(err.Error(), "secret-row-value") {
				t.Fatalf("error leaked a row value: %v", err)
			}
		})
	}
}

func TestRunPostgresMySQLSourceDataProbesStopsAndWrapsQueryError(
	t *testing.T,
) {
	forced := errors.New("catalog connection failed")
	runner := &scriptedPostgresMySQLSourceDataRunner{
		results: []struct {
			invalid bool
			err     error
		}{
			{},
			{err: forced},
			{},
		},
	}
	err := runPostgresMySQLSourceDataProbes(
		context.Background(),
		runner,
		[]postgresMySQLSourceDataProbe{
			{
				kind:   postgresMySQLSourceDataProbeNUL,
				table:  "items",
				object: "payload",
				query:  "first",
			},
			{
				kind:   postgresMySQLSourceDataProbeForeignKey,
				table:  "items",
				object: "fk_parent",
				query:  "second",
			},
			{
				kind:   postgresMySQLSourceDataProbeCheck,
				table:  "items",
				object: "ck_items",
				query:  "third",
			},
		},
	)
	if !errors.Is(err, forced) ||
		!strings.Contains(err.Error(), "foreign key fk_parent preflight") {
		t.Fatalf("error = %v", err)
	}
	if want := []string{"first", "second"}; !reflect.DeepEqual(
		runner.queries,
		want,
	) {
		t.Fatalf("queries = %#v, want %#v", runner.queries, want)
	}
}

type postgresMySQLSourceDataTestAdapter struct {
	engine string
}

func (source *postgresMySQLSourceDataTestAdapter) Engine() string {
	return source.engine
}

func (*postgresMySQLSourceDataTestAdapter) DisplayName() string {
	return "test source"
}

func (*postgresMySQLSourceDataTestAdapter) ListTables(
	context.Context,
) ([]string, error) {
	return nil, nil
}

func (*postgresMySQLSourceDataTestAdapter) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	return schema.Table{}, nil
}

func (*postgresMySQLSourceDataTestAdapter) OpenRows(
	context.Context,
	schema.Table,
	[]string,
) (adapterRows, error) {
	return nil, nil
}

func (*postgresMySQLSourceDataTestAdapter) CountRows(
	context.Context,
	schema.Table,
) (int, error) {
	return 0, nil
}

func (*postgresMySQLSourceDataTestAdapter) Close() error {
	return nil
}

type postgresMySQLSourceDataProviderTestAdapter struct {
	*postgresMySQLSourceDataTestAdapter
	database *sql.DB
}

func (
	source *postgresMySQLSourceDataProviderTestAdapter,
) mySQLDatabaseHandle() *sql.DB {
	return source.database
}

func TestPostgresTargetSourceDataPreflightScopesMySQLFamily(
	t *testing.T,
) {
	target := &postgresTargetAdapter{}
	if err := target.PreflightSourceData(
		context.Background(),
		&postgresMySQLSourceDataTestAdapter{engine: "postgres"},
		nil,
		"",
	); err != nil {
		t.Fatalf("PostgreSQL source was not ignored: %v", err)
	}
	err := target.PreflightSourceData(
		context.Background(),
		&postgresMySQLSourceDataTestAdapter{engine: "mysql"},
		nil,
		"",
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"source database is not available",
	) {
		t.Fatalf("missing-provider error = %v", err)
	}
	err = target.PreflightSourceData(
		context.Background(),
		&postgresMySQLSourceDataProviderTestAdapter{
			postgresMySQLSourceDataTestAdapter: &postgresMySQLSourceDataTestAdapter{
				engine: "mysql",
			},
		},
		nil,
		"",
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"source database is not available",
	) {
		t.Fatalf("nil-database error = %v", err)
	}
}
