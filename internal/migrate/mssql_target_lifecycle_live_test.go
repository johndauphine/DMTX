package migrate

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestSQLServerTargetLifecyclePreservesNeutralIdentityFrontierLive(
	t *testing.T,
) {
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server identity frontier fixture",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"identity frontier target",
		endpoint,
	)

	tests := []struct {
		name      string
		frontier  *int64
		wantNext  int64
		rowSuffix string
		explicit  bool
	}{
		{
			name:      "allocated frontier below explicit row maximum",
			frontier:  sqlServerIdentityFrontierPointer(7),
			wantNext:  8,
			rowSuffix: "allocated",
			explicit:  true,
		},
		{
			name:      "uncalled frontier with explicit rows",
			frontier:  nil,
			wantNext:  1,
			rowSuffix: "uncalled",
			explicit:  true,
		},
		{
			name:      "allocated frontier on an empty source",
			frontier:  sqlServerIdentityFrontierPointer(7),
			wantNext:  8,
			rowSuffix: "empty",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableName := "dmtx_ss_identity_frontier_" +
				test.rowSuffix + "_" +
				strconv.FormatInt(time.Now().UnixNano(), 36)
			cleanupSQLServerNativeTables(t, database, tableName)
			if _, err := database.ExecContext(
				ctx,
				"CREATE TABLE "+
					sqlServerQualified("dbo", tableName)+
					" ([id] BIGINT IDENTITY(1,1) NOT NULL, "+
					"[payload] VARCHAR(32) NULL, "+
					"CONSTRAINT "+
					sqlServerIdentifier("pk_"+tableName)+
					" PRIMARY KEY CLUSTERED ([id] ASC))",
			); err != nil {
				t.Fatalf("create identity frontier fixture: %v", err)
			}
			qualified := sqlServerQualified("dbo", tableName)
			if test.explicit {
				if _, err := database.ExecContext(
					ctx,
					"SET IDENTITY_INSERT "+qualified+" ON; "+
						"INSERT INTO "+qualified+
						" ([id], [payload]) VALUES (100, 'explicit'); "+
						"SET IDENTITY_INSERT "+qualified+" OFF",
				); err != nil {
					_, _ = database.ExecContext(
						context.Background(),
						"SET IDENTITY_INSERT "+qualified+" OFF",
					)
					t.Fatalf("seed explicit identity row: %v", err)
				}
			}

			table := sqlServerTargetLifecycleIdentityTable(0)
			table.Name = tableName
			table.Indexes = nil
			table.Identity.Frontier = test.frontier
			if err := finalizeSQLServerTargets(
				ctx,
				database,
				[]schema.Table{table},
				"drop_recreate",
			); err != nil {
				t.Fatalf("finalize exact identity frontier: %v", err)
			}

			var generated int64
			if err := database.QueryRowContext(
				ctx,
				"INSERT INTO "+qualified+
					" ([payload]) OUTPUT INSERTED.[id] "+
					"VALUES ('generated')",
			).Scan(&generated); err != nil {
				t.Fatalf("insert generated identity: %v", err)
			}
			if generated != test.wantNext {
				t.Fatalf(
					"next generated identity = %d, want %d",
					generated,
					test.wantNext,
				)
			}
		})
	}
}

func TestSQLServerTargetRejectsUnsafeEmptyIdentityPrimerLive(
	t *testing.T,
) {
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server empty identity preflight fixture",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"empty identity preflight target",
		endpoint,
	)

	tests := []struct {
		name   string
		needle string
		shape  func(*testing.T, string) schema.Table
	}{
		{
			name:   "CHECK",
			needle: "CHECK",
			shape: func(t *testing.T, tableName string) schema.Table {
				t.Helper()
				check, err := schema.ParseSQLiteCheckExpression(
					"id < 0",
				)
				if err != nil {
					t.Fatal(err)
				}
				table := sqlServerEmptyIdentityLiveSourceTable(
					tableName,
				)
				table.Checks = []schema.CheckConstraint{{
					Name:       "ck_" + tableName,
					Expression: check,
				}}
				return table
			},
		},
		{
			name: "foreign key",
			// The refusal arrives one layer earlier than this case's name
			// implies. The shape below is a single self-referencing table, and
			// relational table ordering rejects that as a dependency cycle
			// before any empty-identity primer analysis runs — see
			// orderAdapterSourceTablesForMode, which refuses a lone in-scope
			// self-reference outright. Asserting the loose phrase "foreign key"
			// hid that: the real message hyphenates it, so the case failed
			// against correct behaviour. Assert the reason that actually
			// applies rather than the one the name suggests.
			needle: "foreign-key dependency cycle",
			shape: func(
				_ *testing.T,
				tableName string,
			) schema.Table {
				table := sqlServerEmptyIdentityLiveSourceTable(
					tableName,
				)
				table.Columns = append(
					table.Columns,
					schema.Column{
						Name:         "parent_id",
						Type:         "bigint",
						DeclaredType: &schema.DeclaredType{Base: "bigint"},
						Nullable:     true,
					},
				)
				table.ForeignKeys = []schema.ForeignKey{{
					Name:              "fk_" + tableName,
					Columns:           []string{"parent_id"},
					ReferencedTable:   tableName,
					ReferencedColumns: []string{"id"},
					OnUpdate:          "NO ACTION",
					OnDelete:          "NO ACTION",
					Match:             "SIMPLE",
				}}
				return table
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableName := "dmtx_ss_empty_identity_" +
				strconv.FormatInt(time.Now().UnixNano(), 36)
			cleanupSQLServerNativeTables(t, database, tableName)
			sourceTable := test.shape(t, tableName)
			source := &sqlServerTargetValueFixtureSource{
				table: sourceTable,
			}
			observer := &sqlServerNativePreflightObserver{}
			target := &sqlServerTargetAdapter{
				database:    database,
				batchWriter: newSQLServerNativeWriter(database),
				namespace:   "dbo",
			}

			_, err := migrateWithAdapters(
				ctx,
				config.Config{
					Migration: config.Migration{
						TargetMode: "drop_recreate",
					},
				},
				observer,
				source,
				target,
			)
			if err == nil || !strings.Contains(
				err.Error(),
				test.needle,
			) {
				t.Fatalf(
					"empty identity %s preflight error = %v",
					test.name,
					err,
				)
			}
			if observer.mutations != 0 {
				t.Fatalf(
					"empty identity %s reached %d target mutations",
					test.name,
					observer.mutations,
				)
			}
			var exists int
			if err := database.QueryRowContext(
				ctx,
				`SELECT COUNT(*)
				   FROM sys.tables AS target_table
				   JOIN sys.schemas AS target_schema
				     ON target_schema.schema_id =
				        target_table.schema_id
				  WHERE target_schema.name = @p1
				    AND target_table.name = @p2`,
				"dbo",
				tableName,
			).Scan(&exists); err != nil {
				t.Fatalf("inspect empty identity target: %v", err)
			}
			if exists != 0 {
				t.Fatalf(
					"empty identity %s created a target table",
					test.name,
				)
			}
		})
	}
}

func TestSQLServerTargetRejectsAmbiguousEmptyIdentityUpsertLive(
	t *testing.T,
) {
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server empty identity upsert fixture",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"empty identity upsert target",
		endpoint,
	)
	tableName := "dmtx_ss_empty_upsert_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupSQLServerNativeTables(t, database, tableName)
	qualified := sqlServerQualified("dbo", tableName)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+qualified+
			" ([id] BIGINT IDENTITY(1,1) NOT NULL, "+
			"CONSTRAINT "+sqlServerIdentifier("pk_"+tableName)+
			" PRIMARY KEY CLUSTERED ([id] ASC))",
	); err != nil {
		t.Fatalf("create empty retained identity target: %v", err)
	}

	sourceTable := sqlServerEmptyIdentityLiveSourceTable(tableName)
	source := &sqlServerTargetValueFixtureSource{table: sourceTable}
	observer := &sqlServerNativePreflightObserver{}
	target := &sqlServerTargetAdapter{
		database:    database,
		batchWriter: newSQLServerNativeWriter(database),
		namespace:   "dbo",
	}
	_, err := migrateWithAdapters(
		ctx,
		config.Config{
			Migration: config.Migration{TargetMode: "upsert"},
		},
		observer,
		source,
		target,
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"uncalled retained target generator state",
	) {
		t.Fatalf("ambiguous empty identity upsert error = %v", err)
	}
	if observer.mutations != 0 {
		t.Fatalf(
			"ambiguous empty identity upsert reached %d target mutations",
			observer.mutations,
		)
	}

	var generated int64
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+qualified+" OUTPUT INSERTED.[id] "+
			"DEFAULT VALUES",
	).Scan(&generated); err != nil {
		t.Fatalf("verify retained identity remained uncalled: %v", err)
	}
	if generated != 1 {
		t.Fatalf(
			"retained identity next value = %d after rejected upsert, want 1",
			generated,
		)
	}
}

func TestSQLServerNativeWriterIdentityRollbackFrontierLive(
	t *testing.T,
) {
	targetDSN := os.Getenv("DMTX_TEST_MSSQL_TARGET_DSN")
	caPath := os.Getenv("DMTX_TEST_MSSQL_CA")
	if targetDSN == "" || caPath == "" {
		t.Skip(
			"set DMTX_TEST_MSSQL_TARGET_DSN and DMTX_TEST_MSSQL_CA to run the SQL Server identity rollback fixture",
		)
	}
	endpoint := sqlServerCommonFixtureEndpoint(t, targetDSN, caPath)
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	database := openSQLServerNativeLiveDatabase(
		t,
		ctx,
		"identity rollback target",
		endpoint,
	)
	// Force cleanup validation to reuse the writer's physical session.
	database.SetMaxOpenConns(1)

	tableName := "dmtx_ss_identity_rollback_" +
		strconv.FormatInt(time.Now().UnixNano(), 36)
	cleanupSQLServerNativeTables(t, database, tableName)
	if _, err := database.ExecContext(
		ctx,
		"CREATE TABLE "+sqlServerQualified("dbo", tableName)+
			" ([id] BIGINT IDENTITY(1,1) NOT NULL, "+
			"[payload] VARCHAR(32) NOT NULL, "+
			"CONSTRAINT "+sqlServerIdentifier("pk_"+tableName)+
			" PRIMARY KEY ([id]), "+
			"CONSTRAINT "+sqlServerIdentifier("uq_"+tableName)+
			" UNIQUE ([payload]))",
	); err != nil {
		t.Fatalf("create SQL Server identity rollback fixture: %v", err)
	}

	table := schema.Table{
		Schema: "dbo",
		Name:   tableName,
		Identity: &schema.Identity{
			Column:     "id",
			Generation: schema.IdentityByDefault,
		},
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
	receipt, err := newSQLServerNativeWriter(database).WriteBatch(
		ctx,
		table,
		[]string{"id", "payload"},
		"drop_recreate",
		[][]any{
			{int64(100), "duplicate"},
			{int64(101), "duplicate"},
		},
	)
	if err == nil {
		t.Fatal("duplicate identity batch unexpectedly succeeded")
	}
	assertSQLServerNativeReceipt(t, receipt, CommitUnknown, 2, 0)

	var rows int
	if err := database.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM "+
			sqlServerQualified("dbo", tableName),
	).Scan(&rows); err != nil {
		t.Fatalf("count rolled-back SQL Server identity rows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("rolled-back SQL Server identity rows = %d, want 0", rows)
	}

	// The next generated value proves both relevant contracts: the row
	// transaction rolled back without restoring the identity frontier, and
	// IDENTITY_INSERT was disabled before the pinned session reentered the
	// pool. Because exact frontier restoration would race other writers, the
	// receipt above must remain CommitUnknown.
	var generated int64
	if err := database.QueryRowContext(
		ctx,
		"INSERT INTO "+sqlServerQualified("dbo", tableName)+
			" ([payload]) OUTPUT INSERTED.[id] VALUES (@p1)",
		"after-rollback",
	).Scan(&generated); err != nil {
		t.Fatalf(
			"insert generated SQL Server identity after rollback: %v",
			err,
		)
	}
	if generated <= 100 {
		t.Fatalf(
			"generated SQL Server identity after rollback = %d, want > 100",
			generated,
		)
	}
}

func sqlServerIdentityFrontierPointer(value int64) *int64 {
	return &value
}

func sqlServerEmptyIdentityLiveSourceTable(
	tableName string,
) schema.Table {
	frontier := int64(7)
	return schema.Table{
		Schema: "public",
		Name:   tableName,
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
}
