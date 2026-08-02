package migrate

import (
	"os"
	"strings"
	"testing"
)

// stage4LiveEnvironment is every connection fact the Stage 4 exit gate
// depends on. Stage 4 adds no engine beyond Stage 3, but its MySQL/MariaDB
// evolution and delete-recovery fixtures also need administrators to create,
// grant, verify, and remove isolated target databases.
var stage4LiveEnvironment = []string{
	"DMTX_TEST_POSTGRES_DSN",
	"DMTX_TEST_MYSQL_DSN",
	"DMTX_TEST_MYSQL_TARGET_DSN",
	"DMTX_TEST_MYSQL_ADMIN_DSN",
	"DMTX_TEST_MYSQL_CA",
	"DMTX_TEST_MARIADB_DSN",
	"DMTX_TEST_MARIADB_TARGET_DSN",
	"DMTX_TEST_MARIADB_ADMIN_DSN",
	"DMTX_TEST_MARIADB_CA",
	"DMTX_TEST_MSSQL_DSN",
	"DMTX_TEST_MSSQL_TARGET_DSN",
	"DMTX_TEST_MSSQL_CA",
	"DMTX_TEST_CLICKHOUSE_DSN",
	"DMTX_TEST_CLICKHOUSE_SOURCE_DSN",
	"DMTX_TEST_CLICKHOUSE_TARGET_DSN",
	"DMTX_TEST_CLICKHOUSE_CA",
}

// TestStage4LiveMatrixEnvironmentRequired is mandatory Stage 4 gate 4. It must
// fail, not skip, when the exit gate is enabled and any pinned endpoint is
// absent.
//
// The failure mode this exists to prevent is the quiet one: every live fixture
// in this repository begins with a t.Skip when its DSN is unset, so an exit-gate
// run against a half-provisioned environment produces a green suite that proves
// almost nothing. A reviewer reading "ok" cannot distinguish a passing live
// matrix from a skipped one. This test converts that silence into a failure
// naming the exact variables that are missing.
func TestStage4LiveMatrixEnvironmentRequired(t *testing.T) {
	if os.Getenv("DMTX_STAGE4_LIVE_REQUIRED") != "1" {
		t.Skip(
			"set DMTX_STAGE4_LIVE_REQUIRED=1 to require the complete Stage 4 live environment",
		)
	}

	missing := stage4LiveEnvironmentMissing(os.Getenv)
	if len(missing) != 0 {
		t.Fatalf(
			"Stage 4 live matrix is required but environment variables are missing: %s",
			strings.Join(missing, ", "),
		)
	}
	if failures := stage4LiveEnvironmentPreflight(
		os.Getenv,
		verifyStage4ClickHouseTLSHostname,
	); len(failures) != 0 {
		t.Fatalf(
			"Stage 4 live matrix is required but environment preflight failed: %s",
			strings.Join(failures, "; "),
		)
	}
}

// TestStage4LiveMatrixEnvironmentCoversEveryPinnedEndpoint proves the gate list
// itself cannot silently fall behind. A Stage 4 route that starts depending on a
// new endpoint must add it here, or the exit gate would pass while that route
// skipped.
func TestStage4LiveMatrixEnvironmentCoversEveryPinnedEndpoint(t *testing.T) {
	required := make(map[string]struct{}, len(stage4LiveEnvironment))
	for _, name := range stage4LiveEnvironment {
		if _, duplicate := required[name]; duplicate {
			t.Fatalf("duplicate Stage 4 live environment entry %q", name)
		}
		required[name] = struct{}{}
	}
	// Stage 4 must require at least everything Stage 3 required; dropping an
	// endpoint would narrow the gate without anyone noticing.
	for _, name := range stage3LiveEnvironment {
		if _, found := required[name]; !found {
			t.Fatalf(
				"Stage 4 live environment omits Stage 3 endpoint %q",
				name,
			)
		}
	}
}
