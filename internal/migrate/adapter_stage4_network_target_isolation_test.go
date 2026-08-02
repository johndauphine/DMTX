package migrate

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/schema"
)

func TestStage4NetworkReplayIsolationTargetCoverage(t *testing.T) {
	var _ adapterStage4NetworkReplayIsolationTarget = (*postgresTargetAdapter)(nil)
	var _ adapterStage4NetworkReplayIsolationTarget = (*mysqlTargetAdapter)(nil)
	var _ adapterStage4NetworkReplayIsolationTarget = (*sqlServerTargetAdapter)(nil)
	var _ adapterStage4NetworkReplayIsolationTarget = (*sqliteTargetAdapter)(nil)
}

func TestPreflightStage4NetworkReplayIsolationFailsClosedWithoutTarget(
	t *testing.T,
) {
	err := preflightStage4NetworkReplayIsolation(
		context.Background(),
		nil,
		nil,
	)
	if err == nil || !strings.Contains(
		err.Error(),
		`target engine "" has no certified network replay-isolation preflight`,
	) {
		t.Fatalf("preflight error = %v", err)
	}
	var transfer *TransferError
	if !errors.As(err, &transfer) ||
		transfer.Class != ErrorClassPolicy {
		t.Fatalf("preflight error class = %v", err)
	}
}

func TestValidateStage4NetworkIncomingForeignKeyIsolation(t *testing.T) {
	parent := stage4ReplayIsolationTable("public", "parents")
	child := stage4ReplayIsolationTable("public", "children")
	profiles, err := stage4NetworkReplayTableProfiles(
		"PostgreSQL",
		[]schema.Table{parent},
		stage4ExactIdentifier,
	)
	if err != nil {
		t.Fatalf("profiles: %v", err)
	}
	parentProfile := profiles[adapterSourceTableKey("public", "parents")]
	base := stage4NetworkIncomingForeignKey{
		parentNamespace:     "external",
		parentTable:         "children",
		name:                "children_parent_code_fkey",
		referencedNamespace: "public",
		referencedTable:     "parents",
		referencedColumn:    "code",
		updateAction:        "CASCADE",
	}

	t.Run("mutable cascade is rejected", func(t *testing.T) {
		err := validateStage4NetworkIncomingForeignKey(
			"PostgreSQL",
			parentProfile,
			base,
			stage4ExactIdentifier,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"dependent table external.children retains foreign key "+
				"children_parent_code_fkey with ON UPDATE CASCADE "+
				"on mutable column code",
		) {
			t.Fatalf("validation error = %v", err)
		}
	})

	t.Run("immutable primary key cascade is safe", func(t *testing.T) {
		dependency := base
		dependency.referencedColumn = "id"
		if err := validateStage4NetworkIncomingForeignKey(
			"PostgreSQL",
			parentProfile,
			dependency,
			stage4ExactIdentifier,
		); err != nil {
			t.Fatalf("validation error = %v", err)
		}
	})

	t.Run("non-mutating action is safe", func(t *testing.T) {
		dependency := base
		dependency.updateAction = "NO ACTION"
		if err := validateStage4NetworkIncomingForeignKey(
			"PostgreSQL",
			parentProfile,
			dependency,
			stage4ExactIdentifier,
		); err != nil {
			t.Fatalf("validation error = %v", err)
		}
	})

	t.Run("selected child still crosses a page boundary", func(t *testing.T) {
		selectedProfiles, err := stage4NetworkReplayTableProfiles(
			"PostgreSQL",
			[]schema.Table{parent, child},
			stage4ExactIdentifier,
		)
		if err != nil {
			t.Fatalf("profiles: %v", err)
		}
		dependency := base
		dependency.parentNamespace = "public"
		err = validateStage4NetworkIncomingForeignKey(
			"PostgreSQL",
			selectedProfiles[adapterSourceTableKey("public", "parents")],
			dependency,
			stage4ExactIdentifier,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"dependent table public.children",
		) {
			t.Fatalf("validation error = %v", err)
		}
	})

	t.Run("unknown action fails closed", func(t *testing.T) {
		dependency := base
		dependency.updateAction = "MAGIC"
		err := validateStage4NetworkIncomingForeignKey(
			"PostgreSQL",
			parentProfile,
			dependency,
			stage4ExactIdentifier,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			`unexpected ON UPDATE action "MAGIC"`,
		) {
			t.Fatalf("validation error = %v", err)
		}
	})

	t.Run("unknown referenced column fails closed", func(t *testing.T) {
		dependency := base
		dependency.updateAction = "RESTRICT"
		dependency.referencedColumn = "not_in_plan"
		err := validateStage4NetworkIncomingForeignKey(
			"PostgreSQL",
			parentProfile,
			dependency,
			stage4ExactIdentifier,
		)
		if err == nil || !strings.Contains(
			err.Error(),
			"references unknown column not_in_plan",
		) {
			t.Fatalf("validation error = %v", err)
		}
	})
}

func TestStage4NetworkReplayCatalogIdentityRules(t *testing.T) {
	caseVariants := []schema.Table{
		stage4ReplayIsolationTable("public", "Orders"),
		stage4ReplayIsolationTable("public", "orders"),
	}
	if _, err := stage4NetworkReplayTableProfiles(
		"PostgreSQL",
		caseVariants,
		stage4ExactIdentifier,
	); err != nil {
		t.Fatalf("PostgreSQL exact identity: %v", err)
	}
	sqliteVariants := []schema.Table{
		stage4ReplayIsolationTable("", "Orders"),
		stage4ReplayIsolationTable("", "orders"),
	}
	if _, err := stage4NetworkReplayTableProfiles(
		"SQLite",
		sqliteVariants,
		stage4SQLiteIdentifier,
	); err == nil || !strings.Contains(
		err.Error(),
		"same catalog identity",
	) {
		t.Fatalf("SQLite case collision error = %v", err)
	}
	if stage4SQLiteIdentifier("Ä") == stage4SQLiteIdentifier("ä") {
		t.Fatal("SQLite identifier normalization Unicode-folded bytes")
	}
}

func TestStage4MySQLReplayIdentifierContracts(t *testing.T) {
	exact, query, err := stage4MySQLReplayIdentifierContract(0)
	if err != nil {
		t.Fatalf("exact contract: %v", err)
	}
	if exact("Orders") != "Orders" ||
		query != stage4MySQLIncomingForeignKeysExactQuery {
		t.Fatalf("exact contract = %q, %t", exact("Orders"), query ==
			stage4MySQLIncomingForeignKeysExactQuery)
	}
	for _, mode := range []int{1, 2} {
		folded, query, err := stage4MySQLReplayIdentifierContract(mode)
		if err != nil {
			t.Fatalf("folded contract %d: %v", mode, err)
		}
		if folded("Orders") != "orders" ||
			query != stage4MySQLIncomingForeignKeysFoldedQuery {
			t.Fatalf(
				"folded contract %d = %q, query match %t",
				mode,
				folded("Orders"),
				query == stage4MySQLIncomingForeignKeysFoldedQuery,
			)
		}
		if _, err := stage4NetworkReplayTableProfiles(
			"MySQL",
			[]schema.Table{
				stage4ReplayIsolationTable("app", "Orders"),
				stage4ReplayIsolationTable("APP", "orders"),
			},
			folded,
		); err == nil || !strings.Contains(
			err.Error(),
			"same catalog identity",
		) {
			t.Fatalf(
				"folded profile collision for mode %d = %v",
				mode,
				err,
			)
		}
	}
	if _, _, err := stage4MySQLReplayIdentifierContract(3); err == nil ||
		!strings.Contains(
			err.Error(),
			"unsupported lower_case_table_names=3",
		) {
		t.Fatalf("unsupported contract error = %v", err)
	}
}

func TestStage4SQLServerMetadataVisibilityFailsClosedOnMissingProofOrDeny(
	t *testing.T,
) {
	if err := validateStage4SQLServerNetworkMetadataVisibility(
		true,
		true,
		false,
	); err != nil {
		t.Fatalf("complete metadata visibility: %v", err)
	}
	for _, testCase := range []struct {
		name         string
		definition   bool
		security     bool
		overriding   bool
		wantFragment string
	}{
		{
			name:         "missing view definition",
			security:     true,
			wantFragment: "VIEW DEFINITION and VIEW SECURITY DEFINITION",
		},
		{
			name:         "missing security definition",
			definition:   true,
			wantFragment: "VIEW DEFINITION and VIEW SECURITY DEFINITION",
		},
		{
			name:         "applicable deny",
			definition:   true,
			security:     true,
			overriding:   true,
			wantFragment: "metadata DENY",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateStage4SQLServerNetworkMetadataVisibility(
				testCase.definition,
				testCase.security,
				testCase.overriding,
			)
			if err == nil || !strings.Contains(
				err.Error(),
				testCase.wantFragment,
			) {
				t.Fatalf("visibility error = %v", err)
			}
			var transfer *TransferError
			if !errors.As(err, &transfer) ||
				transfer.Class != ErrorClassPolicy {
				t.Fatalf("visibility error class = %v", err)
			}
		})
	}
}

func TestStage4MySQLForeignKeyMetadataVisibilityFailsClosed(
	t *testing.T,
) {
	for _, testCase := range []struct {
		name       string
		engineName string
		visible    bool
		partial    int
		want       string
	}{
		{
			name:       "MySQL missing global visibility",
			engineName: "MySQL",
			want:       "global REFERENCES or SELECT",
		},
		{
			name:       "MySQL partial revokes",
			engineName: "MySQL",
			visible:    true,
			partial:    1,
			want:       "partial_revokes=0",
		},
		{
			name:       "MySQL complete visibility",
			engineName: "MySQL",
			visible:    true,
		},
		{
			name:       "MariaDB missing global visibility",
			engineName: "MariaDB",
			want:       "global REFERENCES or SELECT",
		},
		{
			name:       "MariaDB complete visibility",
			engineName: "MariaDB",
			visible:    true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateStage4MySQLForeignKeyMetadataVisibility(
				testCase.engineName,
				testCase.visible,
				testCase.partial,
			)
			if testCase.want == "" {
				if err != nil {
					t.Fatalf("visibility error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(
				err.Error(),
				testCase.want,
			) {
				t.Fatalf("visibility error = %v", err)
			}
		})
	}
}

func TestStage4PostgresForeignKeyUpdateActions(t *testing.T) {
	for code, expected := range map[string]string{
		"a": "NO ACTION",
		"r": "RESTRICT",
		"c": "CASCADE",
		"n": "SET NULL",
		"d": "SET DEFAULT",
	} {
		actual, err := stage4PostgresForeignKeyUpdateAction(code)
		if err != nil {
			t.Fatalf("action %q: %v", code, err)
		}
		if actual != expected {
			t.Fatalf("action %q = %q, want %q", code, actual, expected)
		}
	}
	if _, err := stage4PostgresForeignKeyUpdateAction("x"); err == nil {
		t.Fatal("unexpected PostgreSQL action was accepted")
	}
}

func TestStage4NetworkReplayCatalogQueriesPinIdentityAndAction(t *testing.T) {
	for _, fragment := range []string{
		"foreign_key.confupdtype",
		"unnest(foreign_key.confkey)",
		"referenced_column.attname",
		"referenced_namespace.nspname = $1",
		"referenced_table.relname = $2",
	} {
		if !strings.Contains(
			stage4PostgresIncomingForeignKeysQuery,
			fragment,
		) {
			t.Fatalf(
				"PostgreSQL replay-isolation query lacks %q",
				fragment,
			)
		}
	}
	for _, fragment := range []string{
		"referential.UPDATE_RULE",
		"column_usage.REFERENCED_COLUMN_NAME",
		"BINARY column_usage.REFERENCED_TABLE_SCHEMA = BINARY ?",
		"BINARY column_usage.REFERENCED_TABLE_NAME = BINARY ?",
	} {
		if !strings.Contains(
			stage4MySQLIncomingForeignKeysExactQuery,
			fragment,
		) {
			t.Fatalf(
				"MySQL replay-isolation query lacks %q",
				fragment,
			)
		}
	}
	for _, fragment := range []string{
		"information_schema.USER_PRIVILEGES",
		"PRIVILEGE_TYPE IN ('REFERENCES', 'SELECT')",
		"@@global.partial_revokes",
	} {
		if !strings.Contains(
			stage4OracleMySQLForeignKeyVisibilityQuery,
			fragment,
		) {
			t.Fatalf(
				"MySQL visibility query fixture lacks %q",
				fragment,
			)
		}
	}
}

func TestStage4SQLServerNetworkNamespaceIdentityPinsCatalogSpelling(
	t *testing.T,
) {
	table := stage4ReplayIsolationTable("dbo", "parents")
	if err := validateStage4SQLServerNetworkNamespaceIdentity(
		"dbo",
		"dbo",
		[]schema.Table{table},
	); err != nil {
		t.Fatalf("canonical namespace: %v", err)
	}
	if err := validateStage4SQLServerNetworkNamespaceIdentity(
		"DBO",
		"dbo",
		[]schema.Table{
			stage4ReplayIsolationTable("DBO", "parents"),
		},
	); err == nil || !strings.Contains(
		err.Error(),
		`schema spelling "DBO" differs from catalog identity "dbo"`,
	) {
		t.Fatalf("case-alias namespace error = %v", err)
	}
	if err := validateStage4SQLServerNetworkNamespaceIdentity(
		"dbo",
		"dbo",
		[]schema.Table{
			stage4ReplayIsolationTable("DBO", "parents"),
		},
	); err == nil || !strings.Contains(
		err.Error(),
		`table parents schema "DBO" differs from target schema "dbo"`,
	) {
		t.Fatalf("case-alias table error = %v", err)
	}
}

func TestPreflightStage4SQLiteNetworkReplayIsolation(t *testing.T) {
	fixtures := []struct {
		name       string
		setup      string
		selected   []schema.Table
		want       string
		wantPolicy bool
	}{
		{
			name: "external mutable cascade",
			setup: `
				CREATE TABLE "Parents" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"code" TEXT NOT NULL UNIQUE
				);
				CREATE TABLE "external_children" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"parent_code" TEXT,
					FOREIGN KEY ("parent_code")
						REFERENCES "Parents" ("code")
						ON UPDATE CASCADE
				);`,
			selected: []schema.Table{
				stage4ReplayIsolationTable("", "Parents"),
			},
			want:       "ON UPDATE CASCADE on mutable column code",
			wantPolicy: true,
		},
		{
			name: "external set null through case variant",
			setup: `
				CREATE TABLE "Parents" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"code" TEXT NOT NULL UNIQUE
				);
				CREATE TABLE "external_children" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"parent_code" TEXT,
					FOREIGN KEY ("parent_code")
						REFERENCES "pArEnTs" ("CoDe")
						ON UPDATE SET NULL
				);`,
			selected: []schema.Table{
				stage4ReplayIsolationTable("", "PARENTS"),
			},
			want:       "ON UPDATE SET NULL on mutable column code",
			wantPolicy: true,
		},
		{
			name: "legal sqlite prefix table is inspected",
			setup: `
				CREATE TABLE "parents" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"code" TEXT NOT NULL UNIQUE
				);
				CREATE TABLE "sqliteXchildren" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"parent_code" TEXT,
					FOREIGN KEY ("parent_code")
						REFERENCES "parents" ("code")
						ON UPDATE CASCADE
				);`,
			selected: []schema.Table{
				stage4ReplayIsolationTable("", "parents"),
			},
			want:       "dependent table .sqliteXchildren retains foreign key",
			wantPolicy: true,
		},
		{
			name: "external primary key cascade",
			setup: `
				CREATE TABLE "parents" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"code" TEXT NOT NULL UNIQUE
				);
				CREATE TABLE "external_children" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"parent_id" INTEGER,
					FOREIGN KEY ("parent_id")
						REFERENCES "parents" ("id")
						ON UPDATE CASCADE
				);`,
			selected: []schema.Table{
				stage4ReplayIsolationTable("", "parents"),
			},
		},
		{
			name: "external mutable no action",
			setup: `
				CREATE TABLE "parents" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"code" TEXT NOT NULL UNIQUE
				);
				CREATE TABLE "external_children" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"parent_code" TEXT,
					FOREIGN KEY ("parent_code")
						REFERENCES "parents" ("code")
						ON UPDATE NO ACTION
				);`,
			selected: []schema.Table{
				stage4ReplayIsolationTable("", "parents"),
			},
		},
		{
			name: "selected child cascade still crosses page boundary",
			setup: `
				CREATE TABLE "parents" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"code" TEXT NOT NULL UNIQUE
				);
				CREATE TABLE "children" (
					"id" INTEGER NOT NULL PRIMARY KEY,
					"parent_code" TEXT,
					"code" TEXT NOT NULL,
					FOREIGN KEY ("parent_code")
						REFERENCES "parents" ("code")
						ON UPDATE CASCADE
				);`,
			selected: []schema.Table{
				stage4ReplayIsolationTable("", "parents"),
				{
					Name: "children",
					Columns: []schema.Column{
						{Name: "id", PrimaryKey: true},
						{Name: "parent_code"},
						{Name: "code"},
					},
				},
			},
			want:       "dependent table .children retains foreign key",
			wantPolicy: true,
		},
	}
	for _, fixture := range fixtures {
		t.Run(fixture.name, func(t *testing.T) {
			database, err := openSQLiteTargetDatabase(
				context.Background(),
				filepath.Join(t.TempDir(), "target.db"),
			)
			if err != nil {
				t.Fatalf("open SQLite target: %v", err)
			}
			t.Cleanup(func() {
				_ = database.Close()
			})
			if _, err := database.Exec(fixture.setup); err != nil {
				t.Fatalf("create SQLite fixture: %v", err)
			}
			err = preflightStage4SQLiteNetworkReplayIsolation(
				context.Background(),
				database,
				fixture.selected,
			)
			if fixture.want == "" {
				if err != nil {
					t.Fatalf("preflight error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(
				err.Error(),
				fixture.want,
			) {
				t.Fatalf("preflight error = %v", err)
			}
			if fixture.wantPolicy {
				var transfer *TransferError
				if !errors.As(err, &transfer) ||
					transfer.Class != ErrorClassPolicy {
					t.Fatalf("preflight error class = %v", err)
				}
			}
		})
	}
}

func stage4ReplayIsolationTable(
	namespace string,
	name string,
) schema.Table {
	return schema.Table{
		Schema: namespace,
		Name:   name,
		Columns: []schema.Column{
			{
				Name:               "id",
				Type:               "integer",
				PrimaryKey:         true,
				PrimaryKeyPosition: 1,
			},
			{Name: "code", Type: "text"},
		},
	}
}
