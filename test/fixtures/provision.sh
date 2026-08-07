#!/usr/bin/env bash
# Create the databases the suite expects but the images do not create.
#
# Every engine needs a target distinct from its source, because the route
# refuses a migration whose endpoints resolve to the same place. The images
# create one database from their environment, so the second is made here.
#
# Safe to run repeatedly.
#
# The root password travels via MYSQL_PWD rather than -p. That silences the
# client's "password on the command line is insecure" warning without also
# silencing real errors: redirecting stderr to /dev/null would hide a genuine
# grant or syntax failure and leave provisioning silently incomplete, which is
# far worse in CI than a noisy warning.
set -euo pipefail

prefix="${DMTX_FIXTURE_PREFIX:-dmtx}"
mysql_container="${prefix}-mysql80-tls"
mariadb_container="${prefix}-mariadb1011-tls"
mssql_container="${prefix}-mssql2022-tls"
clickhouse_container="${prefix}-clickhouse248-tls"

echo "provisioning MySQL target database"
docker exec -e MYSQL_PWD=dmtx_root_test_only "$mysql_container" mysql -uroot -e "
  CREATE DATABASE IF NOT EXISTS dmtx_target;
  GRANT ALL ON dmtx_target.* TO 'dmtx'@'%';
  FLUSH PRIVILEGES;"

# The StackOverflow corpus creates its own source database rather than living
# in dmtx's, because it is dmt's fixture kept byte-identical and that is what it
# does there. So the grant has to exist before the fixture runs: the corpus can
# create the database as root and still be unreadable by the test user, which
# surfaces as "Access denied for user 'dmtx'@'%'" from the loader rather than
# anything about permissions being missing.
echo "provisioning MySQL StackOverflow corpus grants"
docker exec -e MYSQL_PWD=dmtx_root_test_only "$mysql_container" mysql -uroot -e "
  CREATE DATABASE IF NOT EXISTS so2010_minimal_src;
  GRANT ALL ON so2010_minimal_src.* TO 'dmtx'@'%';
  FLUSH PRIVILEGES;"

# The corpus matrix needs a target of its own, not the shared dmtx_target.
# It writes the StackOverflow tables, MySQL lower-cases them, and every other
# test that validates dmtx_target then refuses on a case-aliased table - which
# is a test polluting its neighbours rather than a product defect, and the
# cheapest fix is a database nobody else reads.
echo "provisioning MySQL StackOverflow corpus target"
docker exec -e MYSQL_PWD=dmtx_root_test_only "$mysql_container" mysql -uroot -e "
  CREATE DATABASE IF NOT EXISTS so2010_minimal_tgt;
  GRANT ALL ON so2010_minimal_tgt.* TO 'dmtx'@'%';
  FLUSH PRIVILEGES;"

echo "provisioning MariaDB target database"
docker exec -e MYSQL_PWD=dmtx_root_test_only "$mariadb_container" mariadb -uroot -e "
  CREATE DATABASE IF NOT EXISTS dmtx_target;
  GRANT ALL ON dmtx_target.* TO 'dmtx'@'%';
  FLUSH PRIVILEGES;"

# Source identity detection reads replication metadata from performance_schema.
# The image grants the app account nothing there, so without this the MySQL and
# MariaDB routes fail with "SELECT command denied ... for table
# replication_connection_configuration" the moment they identify a source.
# The global REFERENCES grant is required by the tool, not merely convenient:
# MySQL only exposes foreign-key metadata a user holds privileges on, so
# without a global grant the replay-isolation preflight cannot prove it saw
# every foreign key and refuses with "requires global REFERENCES or SELECT
# privilege to prove complete foreign-key metadata visibility". REFERENCES is
# the narrower of the two options the tool accepts.
echo "granting MySQL performance_schema read and global REFERENCES"
docker exec -e MYSQL_PWD=dmtx_root_test_only "$mysql_container" mysql -uroot -e "
  GRANT SELECT ON performance_schema.* TO 'dmtx'@'%';
  GRANT REPLICATION CLIENT ON *.* TO 'dmtx'@'%';
  GRANT REFERENCES ON *.* TO 'dmtx'@'%';
  GRANT SHOW VIEW ON *.* TO 'dmtx'@'%';
  FLUSH PRIVILEGES;"

echo "granting MariaDB performance_schema and global REFERENCES"
docker exec -e MYSQL_PWD=dmtx_root_test_only "$mariadb_container" mariadb -uroot -e "
  GRANT SELECT ON performance_schema.* TO 'dmtx'@'%';
  GRANT REPLICATION CLIENT ON *.* TO 'dmtx'@'%';
  GRANT REFERENCES ON *.* TO 'dmtx'@'%';
  GRANT SHOW VIEW ON *.* TO 'dmtx'@'%';
  GRANT SLAVE MONITOR ON *.* TO 'dmtx'@'%';
  FLUSH PRIVILEGES;"

echo "provisioning SQL Server target database"
docker exec "$mssql_container" /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'TestPass2024' -C -b \
  -Q "IF DB_ID('dmtx_target') IS NULL CREATE DATABASE dmtx_target;" >/dev/null

# The ClickHouse account is granted ALL on exactly these three databases and
# cannot create others, so the names here must match users.d/dmtx.xml.
echo "provisioning ClickHouse databases"
for database in dmtx_stage4_live_default dmtx_stage4_live_source dmtx_stage4_live_target; do
  docker exec "$clickhouse_container" clickhouse-client \
    --query "CREATE DATABASE IF NOT EXISTS ${database}"
done

echo
echo "fixtures provisioned. Now run:  source ./env.sh"
