package migrate

import (
	"os"
	"strings"
	"testing"
)

var stage3LiveEnvironment = []string{
	"DMTX_TEST_POSTGRES_DSN",
	"DMTX_TEST_MYSQL_DSN",
	"DMTX_TEST_MYSQL_TARGET_DSN",
	"DMTX_TEST_MYSQL_CA",
	"DMTX_TEST_MARIADB_DSN",
	"DMTX_TEST_MARIADB_TARGET_DSN",
	"DMTX_TEST_MARIADB_CA",
	"DMTX_TEST_MSSQL_DSN",
	"DMTX_TEST_MSSQL_TARGET_DSN",
	"DMTX_TEST_MSSQL_CA",
	"DMTX_TEST_CLICKHOUSE_DSN",
	"DMTX_TEST_CLICKHOUSE_SOURCE_DSN",
	"DMTX_TEST_CLICKHOUSE_TARGET_DSN",
	"DMTX_TEST_CLICKHOUSE_CA",
}

func TestStage3LiveMatrixEnvironmentRequired(t *testing.T) {
	if os.Getenv("DMTX_STAGE3_LIVE_REQUIRED") != "1" {
		t.Skip(
			"set DMTX_STAGE3_LIVE_REQUIRED=1 to require the complete Stage 3 live environment",
		)
	}

	missing := make([]string, 0, len(stage3LiveEnvironment))
	for _, name := range stage3LiveEnvironment {
		if strings.TrimSpace(os.Getenv(name)) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) != 0 {
		t.Fatalf(
			"Stage 3 live matrix is required but environment variables are missing: %s",
			strings.Join(missing, ", "),
		)
	}
}
