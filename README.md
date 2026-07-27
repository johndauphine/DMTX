# DMTX

DMTX is a clean-room Go reimplementation of DMT, guided by the reconstruction
specification in [docs/RECREATE_DMT.md](docs/RECREATE_DMT.md).

The implemented migration slice currently supports SQLite-to-SQLite transfers.
It is intentionally small, but it is executable and safety-oriented rather
than a mock interface.

## Current SQLite workflow

Build the command:

```sh
go build -o dmt ./cmd/dmt
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
./dmt run --config migration.yaml
```

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
- Target operations are protected by a local exclusive lease.
- Each table is checkpointed before target mutation and marked complete only
  after row-count validation succeeds.
- Run history and checkpoints are stored in a local SQLite state database next
  to the configuration file.
- Single-integer primary keys transfer through bounded, ascending keyset pages.
  A target-acknowledged page records its signed integer frontier locally.
- Text and composite primary keys use deterministic `ROW_NUMBER()` pages in
  primary-key order rather than an unbounded source read. A
  target-acknowledged page records its row-number frontier locally.
- `dmt resume --config migration.yaml` reuses the interrupted run, verifies a
  completed table before skipping it, and rejects changed data-plane settings.
  In upsert mode, an interrupted table resumes after its last acknowledged
  keyset or row-number page; drop-recreate restarts an incomplete table safely.

Inspect state with:

```sh
./dmt status --state migration.yaml.state.db
./dmt history --state migration.yaml.state.db
```

## Scope and roadmap

This is not yet the full DMT compatibility target. Network engines, richer
schema evolution, deep validation, WebUI/TUI, and release hardening remain
staged work. The complete specification and staged acceptance requirements are
in [docs/RECREATE_DMT.md](docs/RECREATE_DMT.md).

## Development

Production packages and their tests are kept under separate files in
`internal/`. Run the complete verification suite with:

```sh
go test ./...
go build ./cmd/dmt
```
