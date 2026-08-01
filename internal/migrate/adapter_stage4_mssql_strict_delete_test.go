package migrate

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// sqlServerStrictDeleteTestView is deliberately not backed by a pool.  The
// strict-delete constructor must reject it before any query can be issued when
// the strict bridge marker is absent.
type sqlServerStrictDeleteTestView struct{}

func (*sqlServerStrictDeleteTestView) QueryContext(
	context.Context,
	string,
	...any,
) (*sql.Rows, error) {
	return nil, errors.New("unexpected strict-delete test query")
}

func (*sqlServerStrictDeleteTestView) QueryRowContext(
	context.Context,
	string,
	...any,
) *sql.Row {
	return nil
}

func (*sqlServerStrictDeleteTestView) retainedStableViewEngine() string {
	return "mssql"
}

func TestStage4SQLServerStrictDeleteAdmissionRemainsSameEngineOnly(t *testing.T) {
	cfg, prepared := stage4PostgresDeleteRunnerFixture()
	cfg.Migration.StrictConsistency = true

	for _, scope := range []config.StrictConsistencyScope{
		config.StrictConsistencyTable,
		config.StrictConsistencyMigration,
	} {
		t.Run(string(scope), func(t *testing.T) {
			strictCfg := cfg
			strictCfg.Migration.StrictConsistencyScope = scope
			if err := requireStage4AdapterPostgresDeleteComposition(
				strictCfg,
				"mssql",
				"mssql",
				prepared,
			); err != nil {
				t.Fatalf("MSSQL strict-delete %s scope was refused: %v", scope, err)
			}
		})
	}

	for name, testCase := range map[string]struct {
		engines [2]string
		want    string
	}{
		"postgres": {engines: [2]string{"postgres", "postgres"}, want: "strict consistency"},
		"sqlite":   {engines: [2]string{"sqlite", "sqlite"}, want: "strict consistency"},
		"mysql":    {engines: [2]string{"mysql", "mysql"}, want: "strict consistency"},
		"cross":    {engines: [2]string{"mssql", "postgres"}, want: "atomic-receipt capability"},
	} {
		t.Run("refuse_"+name, func(t *testing.T) {
			strictCfg := cfg
			strictCfg.Migration.StrictConsistencyScope = config.StrictConsistencyTable
			err := requireStage4AdapterPostgresDeleteComposition(
				strictCfg,
				testCase.engines[0],
				testCase.engines[1],
				prepared,
			)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("uncertified strict-delete route %s-to-%s error = %v", testCase.engines[0], testCase.engines[1], err)
			}
		})
	}
}

func TestSQLServerStrictDeleteSourceRefusesOrdinaryRetainedView(t *testing.T) {
	table := sqlServerDeleteTestTable("dbo", "items")
	view := &adapterRetainedStableRelationalView{
		source: &relationalSourceAdapter{
			spec:      relationalSourceSpec{engine: "mssql"},
			namespace: "dbo",
		},
		view: &sqlServerStrictDeleteTestView{},
	}
	if _, err := newSQLServerStrictDeleteSourceCapability(
		context.Background(),
		view,
		table,
	); err == nil || !strings.Contains(err.Error(), "retained strict reader") {
		t.Fatalf("ordinary retained SQL Server view was admitted for strict delete scan: %v", err)
	}
}

func TestSQLServerMigrationStrictDeletePartialEvidenceUsesDurableRangeAttempts(t *testing.T) {
	work := stage4AdapterWork{
		task: state.TaskKey{
			Type: stage4AdapterNetworkTaskType, Schema: "dbo", Table: "items",
		},
		topology: "strict-delete-partial-attempts-v1",
	}
	// Three one-row deliveries have durably completed before the process loses
	// only its ordinary table-publication acknowledgement.
	durable := state.WorkTask{
		Key:          work.task,
		Status:       "running",
		TopologyHash: work.topology,
		Attempts:     3,
	}
	actual, err := newStage4SQLServerMigrationStrictDeletePartialEvidenceTable(durable, work)
	if err != nil {
		t.Fatal(err)
	}
	want, err := BuildStrictConsistencyAttemptID(work.task, work.topology, 3)
	if err != nil {
		t.Fatal(err)
	}
	if actual.DurableWorkAttempts != 3 || actual.AttemptID != want {
		t.Fatalf("partial strict evidence = %#v, want durable attempt 3 / %q", actual, want)
	}
	zero, err := BuildStrictConsistencyAttemptID(work.task, work.topology, 0)
	if err != nil {
		t.Fatal(err)
	}
	if actual.AttemptID == zero {
		t.Fatalf("partial strict evidence reused pre-delivery attempt %q", zero)
	}
}
