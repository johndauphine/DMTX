package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4NetworkRebuildAdmissionMatrixRequiresCertifiedKeyedWriters(
	t *testing.T,
) {
	writer := &stage4RelationalRebuildAdmissionWriter{}
	tests := []struct {
		name    string
		schema  string
		adapter adapterStage4NetworkRebuildTarget
	}{
		{
			name:   "PostgreSQL",
			schema: "public",
			adapter: &postgresTargetAdapter{
				batchWriter: writer,
				namespace:   "public",
			},
		},
		{
			name:   "MySQL",
			schema: "target_db",
			adapter: &mysqlTargetAdapter{
				batchWriter: writer,
				namespace:   "target_db",
			},
		},
		{
			name:   "SQL Server",
			schema: "dbo",
			adapter: &sqlServerTargetAdapter{
				batchWriter: writer,
				namespace:   "dbo",
			},
		},
		{
			name:   "SQLite",
			schema: "",
			adapter: &sqliteTargetAdapter{
				stage4BatchWriter: writer,
			},
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			table := stage4RebuildAdmissionTable(testCase.schema)
			if err := testCase.adapter.PreflightStage4NetworkRebuild(
				context.Background(),
				[]schema.Table{table},
			); err != nil {
				t.Fatalf("keyed admission: %v", err)
			}

			table.Columns[0].PrimaryKey = false
			table.Columns[0].PrimaryKeyPosition = 0
			err := testCase.adapter.PreflightStage4NetworkRebuild(
				context.Background(),
				[]schema.Table{table},
			)
			if err == nil || !strings.Contains(err.Error(), "no primary key") {
				t.Fatalf("keyless admission error = %v", err)
			}
			if ClassifyTransferError(err) != ErrorClassPolicy {
				t.Fatalf("keyless admission class = %q", ClassifyTransferError(err))
			}
		})
	}
}

func TestStage4NetworkRebuildAdmissionRejectsWrongTargetIdentity(
	t *testing.T,
) {
	writer := &stage4RelationalRebuildAdmissionWriter{}
	tests := []struct {
		name    string
		adapter adapterStage4NetworkRebuildTarget
		table   schema.Table
	}{
		{
			name: "PostgreSQL",
			adapter: &postgresTargetAdapter{
				batchWriter: writer,
				namespace:   "tenant_a",
			},
			table: stage4RebuildAdmissionTable("tenant_b"),
		},
		{
			name: "MySQL",
			adapter: &mysqlTargetAdapter{
				batchWriter: writer,
				namespace:   "database_a",
			},
			table: stage4RebuildAdmissionTable("database_b"),
		},
		{
			name: "SQL Server",
			adapter: &sqlServerTargetAdapter{
				batchWriter: writer,
				namespace:   "dbo",
			},
			table: stage4RebuildAdmissionTable("other"),
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.adapter.PreflightStage4NetworkRebuild(
				context.Background(),
				[]schema.Table{testCase.table},
			)
			if err == nil || !strings.Contains(err.Error(), "differs") {
				t.Fatalf("target identity admission error = %v", err)
			}
		})
	}
}

func TestStage4NetworkRebuildAdmissionRejectsUncertifiedDelegates(
	t *testing.T,
) {
	ordinary := &stage4OrdinaryAdmissionWriter{}
	for _, testCase := range []struct {
		name      string
		preflight func() error
	}{
		{
			name: "PostgreSQL",
			preflight: func() error {
				return (&postgresTargetAdapter{
					batchWriter: ordinary,
					namespace:   "public",
				}).PreflightStage4NetworkRebuild(
					context.Background(),
					[]schema.Table{stage4RebuildAdmissionTable("public")},
				)
			},
		},
		{
			name: "MySQL",
			preflight: func() error {
				return (&mysqlTargetAdapter{
					batchWriter: ordinary,
					namespace:   "target_db",
				}).PreflightStage4NetworkRebuild(
					context.Background(),
					[]schema.Table{stage4RebuildAdmissionTable("target_db")},
				)
			},
		},
		{
			name: "SQL Server",
			preflight: func() error {
				return (&sqlServerTargetAdapter{
					batchWriter: ordinary,
					namespace:   "dbo",
				}).PreflightStage4NetworkRebuild(
					context.Background(),
					[]schema.Table{stage4RebuildAdmissionTable("dbo")},
				)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := testCase.preflight()
			if err == nil || !strings.Contains(err.Error(), "certified rebuild writer") {
				t.Fatalf("uncertified delegate error = %v", err)
			}
			if ClassifyTransferError(err) != ErrorClassState {
				t.Fatalf("uncertified delegate class = %q", ClassifyTransferError(err))
			}
		})
	}
}

func TestStage4NetworkRebuildSupportMatrixKeepsClickHouseRejected(
	t *testing.T,
) {
	for name, target := range map[string]targetAdapter{
		"PostgreSQL": &postgresTargetAdapter{},
		"MySQL":      &mysqlTargetAdapter{},
		"SQL Server": &sqlServerTargetAdapter{},
		"SQLite":     &sqliteTargetAdapter{},
		"ClickHouse": &clickHouseTargetAdapter{},
	} {
		_, supported := target.(adapterStage4NetworkRebuildTarget)
		if name == "ClickHouse" && supported {
			t.Fatal("ClickHouse advertised an uncertified rebuild replay path")
		}
		if name != "ClickHouse" && !supported {
			t.Fatalf("%s does not advertise relational rebuild admission", name)
		}
	}
}

func TestRelationalTargetAdaptersPreserveRebuildWriteMode(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		write func(*stage4RelationalRebuildAdmissionWriter) (WriteReceipt, error)
	}{
		{
			name: "PostgreSQL",
			write: func(writer *stage4RelationalRebuildAdmissionWriter) (WriteReceipt, error) {
				return (&postgresTargetAdapter{batchWriter: writer}).
					WriteStage4NetworkRebuildBatch(
						context.Background(),
						stage4RebuildAdmissionTable("public"),
						[]string{"id", "payload"},
						NetworkWriteDuplicateSafeInsertOnly,
						[][]any{{int64(1), "one"}},
					)
			},
		},
		{
			name: "MySQL",
			write: func(writer *stage4RelationalRebuildAdmissionWriter) (WriteReceipt, error) {
				return (&mysqlTargetAdapter{batchWriter: writer}).
					WriteStage4NetworkRebuildBatch(
						context.Background(),
						stage4RebuildAdmissionTable("target_db"),
						[]string{"id", "payload"},
						NetworkWriteDuplicateSafeInsertOnly,
						[][]any{{int64(1), "one"}},
					)
			},
		},
		{
			name: "SQL Server",
			write: func(writer *stage4RelationalRebuildAdmissionWriter) (WriteReceipt, error) {
				return (&sqlServerTargetAdapter{batchWriter: writer}).
					WriteStage4NetworkRebuildBatch(
						context.Background(),
						stage4RebuildAdmissionTable("dbo"),
						[]string{"id", "payload"},
						NetworkWriteDuplicateSafeInsertOnly,
						[][]any{{int64(1), "one"}},
					)
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			writer := &stage4RelationalRebuildAdmissionWriter{}
			receipt, err := testCase.write(writer)
			if err != nil {
				t.Fatalf("adapter rebuild write: %v", err)
			}
			if writer.rebuildCalls != 1 ||
				writer.rebuildMode != NetworkWriteDuplicateSafeInsertOnly {
				t.Fatalf(
					"rebuild delegate calls=%d mode=%q",
					writer.rebuildCalls,
					writer.rebuildMode,
				)
			}
			if receipt.Certainty != CommitDurable ||
				receipt.CommittedRows != 1 {
				t.Fatalf("rebuild receipt = %#v", receipt)
			}
		})
	}
}

func stage4RebuildAdmissionTable(namespace string) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   "items",
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "bigint",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "payload", Type: "text"},
		},
	}
}

type stage4RelationalRebuildAdmissionWriter struct {
	rebuildCalls int
	rebuildMode  NetworkWriteMode
}

func (*stage4RelationalRebuildAdmissionWriter) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	return WriteReceipt{}, errors.New("unexpected ordinary write")
}

func (*stage4RelationalRebuildAdmissionWriter) WriteStage4NetworkBatch(
	context.Context,
	schema.Table,
	[]string,
	[][]any,
) (WriteReceipt, error) {
	return WriteReceipt{}, errors.New("unexpected upsert write")
}

func (writer *stage4RelationalRebuildAdmissionWriter) WriteStage4NetworkRebuildBatch(
	_ context.Context,
	_ schema.Table,
	_ []string,
	mode NetworkWriteMode,
	rows [][]any,
) (WriteReceipt, error) {
	writer.rebuildCalls++
	writer.rebuildMode = mode
	count := int64(len(rows))
	return WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: count,
		CommittedRows: count,
	}, nil
}

type stage4OrdinaryAdmissionWriter struct{}

func (*stage4OrdinaryAdmissionWriter) WriteBatch(
	context.Context,
	schema.Table,
	[]string,
	string,
	[][]any,
) (WriteReceipt, error) {
	return WriteReceipt{}, errors.New("unexpected ordinary write")
}
