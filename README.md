# dmtx

DMTX is a clean-room Go reimplementation of DMT, guided by the reconstruction
specification in [docs/RECREATE_DMT.md](docs/RECREATE_DMT.md).

The Stage 2-supported migration path is SQLite-to-SQLite. It is executable,
bounded, fenced, and restartable rather than a mock interface. Stage 3 adds the
fresh-run network adapter and capability scope described below. Its mandatory
network/ClickHouse matrices and repository gates pass in normal and race modes.
Stage 3 does not extend SQLite's Stage 2 restart guarantees to network routes.

## Current SQLite workflow

Build the command:

```sh
go build -o dmtx ./cmd/dmtx
```

Create `migration.yaml`:

```yaml
source:
  type: sqlite
  database: /absolute/path/source.db
target:
  type: sqlite
  database: /absolute/path/target.db
migration:
  target_mode: drop_recreate
  include_tables: ["*"]
  exclude_tables: ["temp_*"]
```

Run the migration:

```sh
./dmtx run --config migration.yaml
```

If a selected target table already contains rows, rebuild mode stops before
target mutation. After confirming a backup and the destructive replacement,
acknowledge it explicitly:

```sh
./dmtx run --config migration.yaml --acknowledge-destructive
```

The default state database is `migration.yaml.state.db`. For headless workflows,
select the YAML state backend explicitly:

```sh
./dmtx run --config migration.yaml --state migration.state.yaml
```

Files ending in `.yaml` or `.yml` select the YAML backend; other state paths
select SQLite. YAML mutations hold a cross-process operating-system lock across
the read/compare/write cycle, flush a complete temporary file, and atomically
replace the prior state.

The command emits a JSON result containing table and row totals. `drop_recreate`
recreates each migrated table. `upsert` retains an existing compatible target
table and applies SQLite upsert-mode writes.

`include_tables` and `exclude_tables` use Go path-style glob matching. Source
tables are considered in deterministic name order; an empty include list means
all tables, and an exclude pattern always wins. A configuration that selects no
source tables fails before target mutation.

## Safety behavior implemented today

- Source and target SQLite files must differ.
- Source tables require a primary key before DMTX changes the target.
- The complete selected schema is planned before target mutation. Unsupported
  SQLite semantics, including table triggers, generated columns, and
  expression/partial indexes, fail with a schema-policy error.
- Target operations are protected by a local exclusive lease.
  Generation fencing prevents a stale owner from mutating the target or state
  after takeover.
- Each table is checkpointed before target mutation and marked complete only
  after row-count validation succeeds.
- Run history and checkpoints use a local SQLite state database next to the
  configuration file by default, or a user-selected atomic YAML state file for
  headless operation.
- Safe single-integer primary keys use bounded integer-keyset pages. Safe
  composite keys use typed tuple-keyset pages. Keys whose binding, type, NULL,
  or ordering behavior is not proven safe use deterministic `ROW_NUMBER()`
  pages in complete primary-key order.
- Range bounds, topology, typed frontiers, issued chunk identity, attempt and
  retry counts, and the lowest contiguous durable acknowledgement are stored
  before progress can advance.
- A target write requires a durable intent and attempt authorization. A hard
  stop after target commit but before state acknowledgement replays that exact
  chunk through insert-only conflict handling, so replay cannot overwrite a
  changed row.
- One migration-wide byte budget accounts for retained scanned rows before
  they are materialized. Reader count, queue depth, and SQLite writers are
  bounded; persistent heap pressure serializes forced collection and reduces
  future chunks only at chunk boundaries.
- SQLite lock retries are bounded and cancellation-aware. Unknown commit
  outcomes, state failures, lease loss, validation failures, and policy errors
  are not retried as transient writes.
- `dmtx resume --config migration.yaml` reuses the interrupted run, verifies a
  completed table before skipping it, and rejects changed data-plane settings.
  Both target modes restore exact range state; a possibly committed rebuild
  chunk uses duplicate-safe insert-only replay.

Resume with the explicit YAML state file and inspect its current or full run
history with:

```sh
./dmtx resume --config migration.yaml --state migration.state.yaml
./dmtx status --state migration.state.yaml
./dmtx history --state migration.state.yaml
```

Every rebuild resume reruns the destructive-target gate. A populated target
requires explicit operator confirmation:

```sh
./dmtx resume --config migration.yaml --acknowledge-destructive
```

The same inspection commands accept the default SQLite path
`migration.yaml.state.db`.

## Stage 3 fresh-run network adapters

The Stage 3 branch implements all 12 directed cross-engine relational pairs
among SQLite, PostgreSQL, SQL Server, and Oracle MySQL, together with the four
same-engine fixtures. MariaDB is independently certified as the MySQL-family
source or target for PostgreSQL, SQL Server, and SQLite routes and for
MariaDB-to-MariaDB. Oracle-MySQL-to-MariaDB and MariaDB-to-Oracle-MySQL are not
claimed; unsupported cross-flavor catalog or collation semantics fail closed.
SQLite-to-ClickHouse and distinct-database ClickHouse-to-ClickHouse provide the
separate analytical rebuild routes.

Live certification is version-pinned:

- PostgreSQL 16.x;
- SQL Server 2022 (product major 16 and database compatibility level 160);
- Oracle MySQL 8.0.16 or later as a source and 8.0.30 or later as a native
  target, within the 8.0 line;
- MariaDB 10.11.8 or later, within the 10.11 line; and
- ClickHouse 24.8.x with Atomic source and target databases.

Every Stage 3 network route requires encrypted TLS. The MySQL/MariaDB, SQL
Server, and ClickHouse live endpoints use CA-verified TLS. The current
PostgreSQL adapter emits `sslmode=require`, which encrypts the connection but
does not verify the server certificate; its live-fixture bootstrap DSN uses
`verify-full`, but certificate verification is not claimed for the adapter.
New major server or catalog lines require separate admission instead of
silently inheriting these contracts.

The relational targets expose their admitted rebuild and upsert capabilities:
PostgreSQL uses transactional COPY/staging, SQL Server uses transactional TDS
bulk copy/staging, and MySQL-family targets use guarded upserts plus the native
bulk path described below. SQLite remains a bounded single-writer target.
ClickHouse uses bounded native batches and is rebuild-only; upsert is rejected
before adapter construction or target mutation.

This is a fresh `dmtx run` certification boundary. It does not certify
network-engine resume after process termination, checkpoint replay and fencing
under faults, strict source consistency, incremental watermarks, delete
reconciliation, or schema evolution. Those are Stage 4 concerns. Strict
consistency is currently rejected before adapter construction;
SQLite-to-SQLite remains the only route with the Stage 2 restartability
guarantees documented above.

The SQL Server-to-SQLite route currently supports fresh drop/recreate only.
It preserves the admitted integral, bit, floating-point, UTF-8 text, binary,
UUID, temporal, identity, and relational-object subset. Exact
`DECIMAL`/`NUMERIC` values are admitted only with scale zero and precision at
most 18, so every value fits SQLite's signed `INTEGER` storage. Fractional or
wider exact numerics, padding-sensitive comparison roles, unsafe nullable
unique indexes, unsupported foreign-key or CHECK semantics, and SQLite-global
object-name collisions fail before target preparation. Upsert remains
fail-closed until retained SQLite shape equivalence is fully proven.

The SQLite-to-ClickHouse 24.8 route is a rebuild-only analytical projection.
It admits SQLite `STRICT` tables with deterministic primary keys and maps
`INTEGER`, `REAL`, `TEXT`, and `BLOB` to `Int64`, `Float64`, and ClickHouse
`String`, preserving nullability with `Nullable` wrappers. Source primary-key
order becomes the MergeTree `ORDER BY` key and is not represented as a
relational uniqueness guarantee. `ANY`, non-`STRICT` tables, declared type
modifiers, defaults, identities, indexes, foreign keys, CHECK constraints,
upsert, strict consistency, non-Atomic target databases, unpinned server
versions, and unsafe existing target engines fail before target mutation.
Writes use bounded native ClickHouse batches over verified TLS.

The ClickHouse-to-ClickHouse route admits plain `MergeTree` tables in Atomic
databases with a nonempty direct-column `ORDER BY`, default sparse primary key
and engine settings, and ordered `Int64`, `Float64`, and `String` columns with
optional `Nullable` wrappers. The source sorting key is preserved as dedicated
ClickHouse ordering metadata; it is never labeled as a relational primary key
or uniqueness constraint. Reads order by that key followed by every remaining
source column, so identical duplicate rows are retained without a uniqueness
claim. Partitioning, sampling, expression, nullable, or floating-point order
keys, custom engine settings, defaults/generated columns, codecs, TTLs,
comments, indexes, projections, constraints, dependencies, and other types
fail before target mutation. Same-engine rebuild also verifies distinct live
Atomic database UUIDs before planning or mutation.

The native Oracle MySQL-to-MySQL route requires read access to
`performance_schema.replication_connection_configuration` and
`performance_schema.replication_group_members`. It fails closed when that
topology cannot be inspected. The native MariaDB route requires the global
`SLAVE MONITOR` privilege so it can inspect `SHOW ALL SLAVES STATUS` and the
global `SHOW VIEW` privilege so drop/recreate preflight can enumerate
cross-database view dependencies. It fails closed if an enumerated view
definition is hidden; view-specific or global `SELECT` access may therefore
be needed when other databases contain views. The route also rejects
WSREP/Galera endpoints. Both native routes currently reject replicated
endpoints, so a target change cannot flow back into the live source through
replication. Native targets require Oracle MySQL 8.0.30 or later or MariaDB
10.11.8 or later in the 10.11 series and verify their session, InnoDB
page-size, constraint-enforcement, and primary-key contracts before planning
a migration.

For native MySQL-family bulk loading, the target server must have global
`local_infile=ON`, and the target account needs `CREATE TEMPORARY TABLES`.
DMTX registers a cryptographically random, one-use in-memory reader and keeps
the driver's arbitrary-file option disabled; it does not grant the server
access to a client filesystem path. If `local_infile` is disabled or the local
infile/staging command is unavailable, DMTX emits one visible warning, latches
that writer to strict bounded inserts, and does not repeatedly retry the
unavailable bulk path. Upsert also uses guarded strict inserts because
`LOAD DATA` cannot preserve its conflict semantics. A native load that reports
an unexpected row count, warning, staged count, or merge count fails without
acknowledging the batch.

## Scope and roadmap

This is not yet the full DMT compatibility target. Stage 4 data semantics,
richer schema evolution, deep validation, WebUI/TUI, and release hardening
remain staged work. The complete specification and staged acceptance
requirements are in
[docs/RECREATE_DMT.md](docs/RECREATE_DMT.md).

## Development

Production packages and their tests are kept under separate files in
`internal/`. Run the complete verification suite with:

```sh
go test ./...
go build -o dmtx ./cmd/dmtx
```
