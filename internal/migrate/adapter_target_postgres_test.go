package migrate

import (
	"context"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPostgresTargetEndpointValidationDoesNotResolveSecrets(t *testing.T) {
	endpoint := config.Endpoint{
		Host:     "database.example",
		Database: "target",
		User:     "migrator",
		Password: "${file:/path/that/does/not/exist}",
	}
	if err := validatePostgresTargetEndpoint(endpoint); err != nil {
		t.Fatalf("validatePostgresTargetEndpoint: %v", err)
	}

	endpoint.Host = ""
	err := validatePostgresTargetEndpoint(endpoint)
	if err == nil ||
		err.Error() != "PostgreSQL host, database, and user are required" {
		t.Fatalf("missing-host error = %v", err)
	}
}

func TestPostgresTargetSchemaMappingDoesNotMutateSource(t *testing.T) {
	if got := postgresTargetNamespace(config.Endpoint{}); got != "public" {
		t.Fatalf("default namespace = %q, want public", got)
	}
	if got := postgresTargetNamespace(config.Endpoint{Schema: "archive"}); got != "archive" {
		t.Fatalf("explicit namespace = %q, want archive", got)
	}

	source := schema.Table{
		Schema: "source_schema",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true},
		},
	}
	target := postgresTargetTable(source, "target_schema")
	if source.Schema != "source_schema" {
		t.Fatalf("source schema mutated to %q", source.Schema)
	}
	if target.Schema != "target_schema" || target.Name != source.Name {
		t.Fatalf("target table = %#v", target)
	}

	adapter := &postgresTargetAdapter{namespace: "target_schema"}
	if adapter.Engine() != "postgres" {
		t.Fatalf("Engine() = %q, want postgres", adapter.Engine())
	}
}

func TestPostgresTargetWriteBatchDelegatesToConfiguredWriter(t *testing.T) {
	writer := &postgresTargetWriterRecorder{
		receipt: WriteReceipt{
			Certainty:     CommitDurable,
			AttemptedRows: 1,
			CommittedRows: 1,
		},
	}
	adapter := &postgresTargetAdapter{
		batchWriter: writer,
		namespace:   "public",
	}
	table := schema.Table{
		Schema: "public",
		Name:   "events",
		Columns: []schema.Column{
			{Name: "id", Type: "integer", PrimaryKey: true},
		},
	}
	rows := [][]any{{int64(1)}}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		table,
		[]string{"id"},
		"drop_recreate",
		rows,
	)
	if err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if writer.calls != 1 ||
		writer.table.Name != "events" ||
		writer.mode != "drop_recreate" ||
		len(writer.rows) != 1 {
		t.Fatalf("delegated write = %#v", writer)
	}
	if receipt != writer.receipt {
		t.Fatalf("receipt = %#v, want %#v", receipt, writer.receipt)
	}
}

func TestPostgresTargetWriteBatchRejectsMissingWriter(t *testing.T) {
	adapter := &postgresTargetAdapter{namespace: "public"}
	receipt, err := adapter.WriteBatch(
		context.Background(),
		schema.Table{Schema: "public", Name: "events"},
		[]string{"id"},
		"drop_recreate",
		[][]any{{int64(1)}},
	)
	if err == nil ||
		err.Error() != "PostgreSQL native batch writer is not configured" {
		t.Fatalf("error = %v", err)
	}
	if receipt.Certainty != CommitNotCommitted ||
		receipt.AttemptedRows != 1 ||
		receipt.CommittedRows != 0 {
		t.Fatalf("receipt = %#v", receipt)
	}
}

func TestPostgresPairWrappersRejectWrongTypes(t *testing.T) {
	tests := []struct {
		name string
		run  migrationRunner
		cfg  config.Config
		want string
	}{
		{
			name: "postgres source",
			run:  PostgresToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "mysql"},
				Target: config.Endpoint{Type: "postgres"},
			},
			want: "PostgreSQL-to-PostgreSQL requires source.type and target.type postgres",
		},
		{
			name: "postgres target",
			run:  PostgresToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "postgres"},
				Target: config.Endpoint{Type: "mysql"},
			},
			want: "PostgreSQL-to-PostgreSQL requires source.type and target.type postgres",
		},
		{
			name: "mysql source",
			run:  MySQLToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "postgres"},
				Target: config.Endpoint{Type: "postgres"},
			},
			want: "MySQL-to-PostgreSQL requires source.type mysql and target.type postgres",
		},
		{
			name: "mysql target",
			run:  MySQLToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "mysql"},
				Target: config.Endpoint{Type: "sqlite"},
			},
			want: "MySQL-to-PostgreSQL requires source.type mysql and target.type postgres",
		},
		{
			name: "sql server source",
			run:  SQLServerToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "postgres"},
				Target: config.Endpoint{Type: "postgres"},
			},
			want: "SQL Server-to-PostgreSQL requires source.type mssql and target.type postgres",
		},
		{
			name: "sql server target",
			run:  SQLServerToPostgresWithObserver,
			cfg: config.Config{
				Source: config.Endpoint{Type: "mssql"},
				Target: config.Endpoint{Type: "sqlite"},
			},
			want: "SQL Server-to-PostgreSQL requires source.type mssql and target.type postgres",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.run(context.Background(), test.cfg, nil)
			if err == nil || err.Error() != test.want {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSQLServerToPostgresRouteIsCertified(t *testing.T) {
	err := ValidateMigration(config.Config{
		Source: config.Endpoint{
			Type:     "mssql",
			Host:     "source.example.test",
			Database: "source",
			User:     "reader",
		},
		Target: config.Endpoint{
			Type:     "postgres",
			Host:     "target.example.test",
			Database: "target",
			User:     "writer",
		},
	})
	if err != nil {
		t.Fatalf("ValidateMigration(SQL Server-to-PostgreSQL): %v", err)
	}
}

type postgresTargetWriterRecorder struct {
	calls   int
	table   schema.Table
	columns []string
	mode    string
	rows    [][]any
	receipt WriteReceipt
	err     error
}

func (writer *postgresTargetWriterRecorder) WriteBatch(
	_ context.Context,
	table schema.Table,
	columns []string,
	mode string,
	rows [][]any,
) (WriteReceipt, error) {
	writer.calls++
	writer.table = table
	writer.columns = append([]string(nil), columns...)
	writer.mode = mode
	writer.rows = rows
	return writer.receipt, writer.err
}
