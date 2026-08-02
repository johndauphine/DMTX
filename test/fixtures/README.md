# Live test fixtures

Five verified-TLS database endpoints, plus the databases, accounts, and grants
the Stage 4 suite needs.

Before this existed, the entire Stage 4 claim rested on containers that had been
built by hand on one machine. Only their DSNs were ever written down, so nobody
else could reproduce the armed gate, and losing that Docker state would have
made the claim unverifiable rather than merely unverified.

## Use

```sh
./generate-certs.sh          # once; writes certs/ (git-ignored)
docker compose up -d
./provision.sh               # databases and grants the images do not create
source ./env.sh              # exports the 16 DMTX_TEST_* variables
```

Then, from the repository root:

```sh
DMTX_STAGE4_LIVE_REQUIRED=1 go test ./... -count=1
DMTX_STAGE4_LIVE_REQUIRED=1 go test -race ./... -count=1
```

`DMTX_STAGE4_LIVE_REQUIRED=1` is the part that matters. Without it a missing
endpoint makes a live test **skip** while the suite still prints `ok` — which is
how dozens of gaps stayed hidden. Armed, an absent fixture fails instead.

`env.sh` deliberately does not set it. Whether a missing endpoint should skip or
fail is the operator's decision, and hiding that in an environment file would
make it invisible.

## Running alongside an existing stack

Ports and container names are overridable, so this can be brought up next to
another set for comparison without disturbing it:

```sh
DMTX_FIXTURE_PREFIX=dmtxv DMTX_PG_PORT=55433 DMTX_MYSQL_PORT=53307 \
DMTX_MARIADB_PORT=54307 DMTX_MSSQL_PORT=51434 DMTX_CLICKHOUSE_PORT=59441 \
  docker compose -p dmtx-fixtures-verify up -d
```

That is how this file set was validated: the composed stack ran on alternate
ports beside the hand-built one, the armed gate was run against it, and only
then was it trusted. Verifying by replacing the working fixtures would have
destroyed the only environment capable of checking the result.

## Things that are load-bearing, not incidental

Each of these cost a debugging cycle. They look like details and are not.

- **The CA must be named `ca.pem`.** The SQL Server driver dispatches on file
  extension and rejects `.crt` outright: *certificate type .crt is not
  supported*. Other engines accept any name.
- **The server certificate needs `IP:127.0.0.1` in its SAN.** The DSNs connect
  by IP, and Go verifies the IP SAN, not the CN. A certificate with only a CN or
  only a DNS SAN fails `verify-full`.
- **ClickHouse mounts individual files, not directories.** Its entrypoint writes
  `users.d/default-user.xml` at startup; a read-only `users.d` makes the
  container exit before serving anything.
- **ClickHouse's app account is defined only in `users.d`,** with no
  `CLICKHOUSE_USER`/`DB` environment variables. Those would make the entrypoint
  create a database *as* the least-privilege account, which holds no
  `CREATE DATABASE` grant. The hand-built fixture only worked because it was
  restricted *after* the database existed — an ordering a compose file cannot
  reproduce.
- **`clickhouse-client` reads its own config,** not the server's, so the CA is
  named again in `client-config.xml`. Its healthcheck runs over the secure port
  on purpose: a local-socket query succeeds even when TLS is broken and 9440
  never binds, which is precisely the failure worth catching.
- **MySQL and MariaDB need global grants,** because the tool requires provable
  completeness rather than an apparently-empty result: `REFERENCES` for
  foreign-key metadata visibility, `SHOW VIEW` for target views, plus
  `performance_schema` reads for source identity. MariaDB additionally needs
  `SLAVE MONITOR`; it refuses channel inspection without it even when
  `REPLICATION CLIENT` is held.
- **MariaDB's server flags are part of the contract.** Charset, collation, row
  format, timestamp handling, and identifier case are all asserted by exact
  catalog comparisons, so leaving them to image defaults makes results drift
  between machines.

## Credentials

Throwaway values for containers that listen only on `127.0.0.1`. They are in
version control on purpose: reconstructing them was the blocker this directory
exists to remove. They protect nothing and must never be reused anywhere.

The generated TLS material under `certs/` is **not** committed — it is
reproducible from `generate-certs.sh`, and committing a private key, even a
worthless one, teaches the wrong habit and trips secret scanners.
