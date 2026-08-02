# Source this to export the sixteen variables the armed live gate requires.
#
#   source ./env.sh
#   DMTX_STAGE4_LIVE_REQUIRED=1 go test ./... -count=1
#
# DMTX_STAGE4_LIVE_REQUIRED is deliberately not set here. It is the operator's
# choice whether a missing endpoint should skip or fail, and exporting it as a
# side effect of sourcing an environment file would make that choice invisible.
#
# The credentials below are throwaway values for local containers that listen
# only on 127.0.0.1. They are in version control on purpose: reconstructing
# them was the blocker that kept the Stage 4 claim tied to a single machine.
# They protect nothing and must never be reused.

# This file uses bash-only syntax below. Say so plainly rather than letting a
# dash or sh user hit an opaque "bad substitution".
if [ -z "${BASH_VERSION:-}" ]; then
  echo "env.sh must be sourced from bash: bash -c 'source env.sh && ...'" >&2
  return 1 2>/dev/null || exit 1
fi

_fixtures_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
_certs="${_fixtures_dir}/certs"

_pg_port="${DMTX_PG_PORT:-55432}"
_mysql_port="${DMTX_MYSQL_PORT:-53306}"
_mariadb_port="${DMTX_MARIADB_PORT:-54306}"
_mssql_port="${DMTX_MSSQL_PORT:-51433}"
_clickhouse_port="${DMTX_CLICKHOUSE_PORT:-59440}"

# verify-full, not require: the tests exist partly to prove the tool validates
# the chain and the hostname rather than accepting any TLS peer.
export DMTX_TEST_POSTGRES_DSN="postgres://dmtx:dmtx_test_only@127.0.0.1:${_pg_port}/dmtx_test?sslmode=verify-full&sslrootcert=${_certs}/ca.pem"

export DMTX_TEST_MYSQL_CA="${_certs}/ca.pem"
export DMTX_TEST_MARIADB_CA="${_certs}/ca.pem"
export DMTX_TEST_MSSQL_CA="${_certs}/ca.pem"
export DMTX_TEST_CLICKHOUSE_CA="${_certs}/ca.pem"

# The tls= names are registered by the test helpers from the CA above and must
# match exactly. parseTime is required or bulk round-trips fail scanning
# DATETIME into time.Time.
export DMTX_TEST_MYSQL_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:${_mysql_port})/dmtx?tls=dmtx_test&parseTime=true"
export DMTX_TEST_MYSQL_TARGET_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:${_mysql_port})/dmtx_target?tls=dmtx_test&parseTime=true"
# Root is needed for the binary-log-safe trigger sentinel and for creating the
# restricted account the LOCK TABLES rejection test uses. Note the suite fails
# rather than skips when the target DSN is set and this is not: partial
# provisioning is treated as an error on purpose.
export DMTX_TEST_MYSQL_ADMIN_DSN="root:dmtx_root_test_only@tcp(127.0.0.1:${_mysql_port})/dmtx_target?tls=dmtx_test&parseTime=true"

export DMTX_TEST_MARIADB_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:${_mariadb_port})/dmtx_source?tls=dmtx_mariadb_test&parseTime=true"
export DMTX_TEST_MARIADB_TARGET_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:${_mariadb_port})/dmtx_target?tls=dmtx_mariadb_test&parseTime=true"
export DMTX_TEST_MARIADB_ADMIN_DSN="root:dmtx_root_test_only@tcp(127.0.0.1:${_mariadb_port})/dmtx_target?tls=dmtx_mariadb_test&parseTime=true"

# SQL Server additionally requires guid conversion and a TLS floor.
export DMTX_TEST_MSSQL_DSN="sqlserver://sa:TestPass2024@127.0.0.1:${_mssql_port}?database=master&encrypt=true&tlsmin=1.2&guid+conversion=true&certificate=${_certs}/ca.pem"
export DMTX_TEST_MSSQL_TARGET_DSN="sqlserver://sa:TestPass2024@127.0.0.1:${_mssql_port}?database=dmtx_target&encrypt=true&tlsmin=1.2&guid+conversion=true&certificate=${_certs}/ca.pem"

# 127.0.0.1 rather than localhost: the certificate is verified against the IP
# SAN, and a hostname that does not appear in the SAN fails verification.
export DMTX_TEST_CLICKHOUSE_DSN="clickhouse://dmtx:dmtx_source_test@127.0.0.1:${_clickhouse_port}/dmtx_stage4_live_default?secure=true"
export DMTX_TEST_CLICKHOUSE_SOURCE_DSN="clickhouse://dmtx:dmtx_source_test@127.0.0.1:${_clickhouse_port}/dmtx_stage4_live_source?secure=true"
export DMTX_TEST_CLICKHOUSE_TARGET_DSN="clickhouse://dmtx:dmtx_source_test@127.0.0.1:${_clickhouse_port}/dmtx_stage4_live_target?secure=true"

unset _fixtures_dir _certs _pg_port _mysql_port _mariadb_port _mssql_port _clickhouse_port

echo "exported 16 DMTX_TEST_* variables"
