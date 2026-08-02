package migrate

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestStage4SchemaContractTargetMatrixLive is the bounded target-side live
// matrix for RECREATE_DMT §7.4. It deliberately does not form an all-pairs
// source/target cross product: schema-contract evolution consumes the
// canonical schema.Table capability supplied by the generic schema gate, so a
// portable relational projection is sufficient to prove each production target
// executor. Source discovery/normalization has its own live coverage.
//
// Each network cell requires verified TLS and invokes both (a) a composed
// Stage 4 upsert route and (b) the native exact-catalog executor proof. The
// combination covers deterministic create/add/relax/widen behavior, retained
// target-only authority, and a live pre-mutation collision or foreign-key
// refusal where the target can express it. SQLite is local by definition; its
// equivalent cell uses a WAL target and both YAML/SQLite durable state paths.
func TestStage4SchemaContractTargetMatrixLive(t *testing.T) {
	stage4RequireSchemaContractTargetMatrixEnvironment(t)

	cells := []struct {
		name  string
		basis string
		run   func(*testing.T)
	}{
		{
			name:  "postgresql",
			basis: "portable relational projection -> PostgreSQL target capability",
			run: func(t *testing.T) {
				TestStage4AdapterPostgresSchemaEvolutionComposedRouteLiveTLS(t)
				TestPostgresTargetEvolutionCatalogFenceLive(t)
				TestPostgresTargetEvolutionRealPlannerLive(t)
			},
		},
		{
			name:  "mysql80",
			basis: "portable relational projection -> MySQL 8.0 exact catalog/DDL capability",
			run: func(t *testing.T) {
				stage4RunMySQLTargetSchemaContractMatrixCell(t, mysqlTargetEvolutionLiveFixture{
					name: "mysql80", dsnEnv: "DMTX_TEST_MYSQL_TARGET_DSN",
					adminDSNEnv: "DMTX_TEST_MYSQL_ADMIN_DSN", caEnv: "DMTX_TEST_MYSQL_CA", tlsConfig: "dmtx_test",
					collation: "utf8mb4_0900_bin", refreshInfo: true,
				})
			},
		},
		{
			name:  "mariadb1011",
			basis: "portable relational projection -> MariaDB 10.11 exact catalog/DDL capability",
			run: func(t *testing.T) {
				stage4RunMySQLTargetSchemaContractMatrixCell(t, mysqlTargetEvolutionLiveFixture{
					name: "mariadb1011", dsnEnv: "DMTX_TEST_MARIADB_TARGET_DSN",
					adminDSNEnv: "DMTX_TEST_MARIADB_ADMIN_DSN", caEnv: "DMTX_TEST_MARIADB_CA", tlsConfig: "dmtx_mariadb_test",
					collation: "utf8mb4_nopad_bin",
				})
			},
		},
		{
			name:  "sqlserver2022",
			basis: "portable relational projection -> SQL Server 2022 exact catalog/transactional DDL capability",
			run: func(t *testing.T) {
				stage4RunSQLServerTargetSchemaContractMatrixCell(t)
			},
		},
		{
			name:  "sqlite",
			basis: "portable relational projection -> SQLite WAL exact catalog/copy-swap capability",
			run: func(t *testing.T) {
				TestStage4AdapterSQLiteTargetEvolutionComposedWAL(t)
				TestStage4AdapterSQLiteTargetEvolutionRelaxResumeWAL(t)
				TestSQLiteTargetEvolutionCopySwapPreservesRetainedRowsAndAuthority(t)
				TestSQLiteTargetEvolutionCopySwapRejectsIncomingForeignKeysBeforeMutation(t)
				TestSQLiteTargetEvolutionCopySwapRejectsConcurrentTemporaryNameCollision(t)
			},
		},
	}
	for _, cell := range cells {
		cell := cell
		t.Run(cell.name, func(t *testing.T) {
			t.Logf("schema-contract target matrix basis: %s", cell.basis)
			cell.run(t)
		})
	}
}

func stage4RequireSchemaContractTargetMatrixEnvironment(t *testing.T) {
	t.Helper()
	if os.Getenv("DMTX_STAGE4_LIVE_REQUIRED") != "1" {
		t.Skip("set DMTX_STAGE4_LIVE_REQUIRED=1 to arm the schema-contract target matrix")
	}
	required := []string{
		"DMTX_TEST_POSTGRES_DSN",
		"DMTX_TEST_MYSQL_TARGET_DSN",
		"DMTX_TEST_MYSQL_ADMIN_DSN",
		"DMTX_TEST_MYSQL_CA",
		"DMTX_TEST_MARIADB_TARGET_DSN",
		"DMTX_TEST_MARIADB_ADMIN_DSN",
		"DMTX_TEST_MARIADB_CA",
		"DMTX_TEST_MSSQL_TARGET_DSN",
		"DMTX_TEST_MSSQL_CA",
	}
	missing := make([]string, 0, len(required))
	for _, name := range required {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Fatalf("armed schema-contract target matrix is missing fixture variables: %s", strings.Join(missing, ", "))
	}
	issues := make([]string, 0)
	issues = append(issues, stage4ValidatePostgresLiveEnvironment(os.Getenv)...)
	issues = append(issues, stage4ValidateMySQLFamilyLiveEnvironment(
		os.Getenv,
		"DMTX_TEST_MYSQL_DSN",
		"DMTX_TEST_MYSQL_TARGET_DSN",
		"DMTX_TEST_MYSQL_ADMIN_DSN",
		"DMTX_TEST_MYSQL_CA",
		"dmtx_test",
	)...)
	issues = append(issues, stage4ValidateMySQLFamilyLiveEnvironment(
		os.Getenv,
		"DMTX_TEST_MARIADB_DSN",
		"DMTX_TEST_MARIADB_TARGET_DSN",
		"DMTX_TEST_MARIADB_ADMIN_DSN",
		"DMTX_TEST_MARIADB_CA",
		"dmtx_mariadb_test",
	)...)
	issues = append(issues, stage4ValidateSQLServerLiveEnvironment(os.Getenv)...)
	if len(issues) != 0 {
		t.Fatalf("armed schema-contract target matrix environment is invalid: %s", strings.Join(issues, "; "))
	}
}

func stage4RunMySQLTargetSchemaContractMatrixCell(
	t *testing.T,
	fixture mysqlTargetEvolutionLiveFixture,
) {
	t.Helper()
	var nativeCleanup mysqlTargetEvolutionLiveCleanupEvidence
	t.Run("native_exact_catalog", func(t *testing.T) {
		testMySQLTargetSchemaEvolutionLive(t, fixture, &nativeCleanup)
	})
	if nativeCleanup.granted {
		assertMySQLTargetEvolutionLiveGrantRemoved(t, fixture, nativeCleanup)
	}
	var composedCleanup mysqlTargetEvolutionLiveCleanupEvidence
	t.Run("composed_upsert", func(t *testing.T) {
		testStage4AdapterMySQLSchemaEvolutionComposedRouteLiveTLS(
			t,
			fixture,
			&composedCleanup,
		)
	})
	if composedCleanup.granted {
		assertMySQLTargetEvolutionLiveGrantRemoved(t, fixture, composedCleanup)
	}
}

func stage4RunSQLServerTargetSchemaContractMatrixCell(t *testing.T) {
	t.Helper()
	endpoint := sqlServerTargetEvolutionLiveEndpoint(t)
	var nativeCleanup sqlServerTargetEvolutionLiveCleanupEvidence
	t.Run("native_exact_catalog", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()
		testSQLServerTargetSchemaEvolutionLive(t, ctx, endpoint, &nativeCleanup)
	})
	assertSQLServerTargetEvolutionLiveDatabaseRemoved(t, endpoint, nativeCleanup)
	t.Run("composed_upsert", func(t *testing.T) {
		TestStage4AdapterSQLServerSchemaEvolutionComposedRouteLiveTLS(t)
	})
}
