# dmtx

DMTX is a clean-room Go reimplementation of DMT, guided by the reconstruction
specification in [docs/RECREATE_DMT.md](docs/RECREATE_DMT.md).

The Stage 2-supported migration path is SQLite-to-SQLite. It is executable,
bounded, fenced, and restartable rather than a mock interface. Preliminary
fresh-run network paths also exist in the codebase, but they do not receive the
Stage 2 guarantees described below.

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

## Preliminary network paths

The Stage 3 adapter registry currently certifies these fresh-run
implementations:

- SQLite to PostgreSQL and ClickHouse, plus MySQL/MariaDB and SQL Server
  compatibility routes;
- PostgreSQL to PostgreSQL, SQLite, Oracle MySQL 8.0, MariaDB 10.11, or
  SQL Server 2022;
- Oracle MySQL 8.0 to PostgreSQL, SQLite, or Oracle MySQL 8.0;
- MariaDB 10.11 to PostgreSQL, SQLite, or MariaDB 10.11; and
- SQL Server 2022 to PostgreSQL, SQLite, Oracle MySQL 8.0, MariaDB 10.11,
  or SQL Server 2022; and
- ClickHouse 24.8 to a distinct ClickHouse 24.8 Atomic database.

These paths remain incomplete Stage 3 implementations. They do not yet share
SQLite's full range checkpoint, replay, fencing, resume, and fault-injection
matrix, and they have not passed the Stage 3 native-bulk and live-engine
conformance suite. Treat them as experimental, not as Stage 2-certified
migrations.

SQLite, PostgreSQL, Oracle MySQL 8.0, MariaDB 10.11, and SQL Server 2022
sources now compose independently with the PostgreSQL target. PostgreSQL,
each admitted MySQL-family source, and SQL Server compose with the SQLite
target behind shared contracts. SQLite also composes with the native
ClickHouse 24.8 target. ClickHouse 24.8 sources compose with a distinct native
ClickHouse target for the admitted same-engine rebuild shape. PostgreSQL and
SQL Server sources compose with either native MySQL-family target, while each
MySQL-family source composes with its matching native flavor. PostgreSQL and
SQL Server sources also compose with the native SQL Server target.
Cross-flavor Oracle-MySQL/MariaDB copies remain fail-closed where exact
collation and catalog semantics differ.

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

## Scope and roadmap

This is not yet the full DMT compatibility target. Network-engine conformance,
richer schema evolution, deep validation, WebUI/TUI, and release hardening
remain staged work. The complete specification and staged acceptance
requirements are in [docs/RECREATE_DMT.md](docs/RECREATE_DMT.md).

## Development

Production packages and their tests are kept under separate files in
`internal/`. Run the complete verification suite with:

```sh
go test ./...
go build -o dmtx ./cmd/dmtx
```
