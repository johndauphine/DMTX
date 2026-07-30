# DMTX Stage 3 transfer handoff

Date: 2026-07-30

## Current checkpoint

- Branch: `codex/stage-3-network-adapters`
- Stage 3 implementation checkpoint:
  `4ea4bc7a90ab4f07c2537715b4e9138ad9c8a2d3`
- The following documentation/evidence-only closeout commit will naturally
  have a newer hash without changing that implementation checkpoint.
- Preserved remote checkpoint:
  `5631ccab37d39f60975375bccfcb67c72301c926`
- At the implementation checkpoint, the local branch is 21 commits ahead of
  that preserved remote checkpoint.
- `main` has not been merged or otherwise used as the Stage 3 checkout.
- The literal Stage 3 exit criterion is complete at this checkpoint. This does
  not authorize a push or merge and does not include the Stage 4 guarantees
  listed below.

The original `72c160a` foundation checkpoint is superseded by the branch
history summarized below. Use the implementation checkpoint and recorded gate
evidence here as the authoritative Stage 3 handoff.

## Implemented Stage 3 scope

- PostgreSQL 16 exact source discovery and the PostgreSQL-to-PostgreSQL common
  fixture, including type modifiers, defaults, ordered primary keys,
  identity/frontier state, indexes, CHECK constraints, foreign keys, and
  fail-closed catalog-shape validation.
- Oracle MySQL 8.0 source discovery and native target behavior, including
  lifecycle/preflight safety, retained upsert, alias protection, exact catalog
  round trips, and native live routes.
- MariaDB 10.11 source discovery and native target certification under its
  separate catalog, privilege, session, replication, and version contract.
- SQL Server 2022 source discovery and native target behavior, including
  transactional bulk/upsert, lifecycle/preflight safety, and live routes.
- All 12 directed cross-engine relational pairs among PostgreSQL, SQL Server,
  Oracle MySQL, and SQLite; same-engine fixtures; and the MariaDB variants
  listed in `STAGE3_REQUIREMENTS_TESTS.md`.
- SQLite-to-ClickHouse analytical rebuild plus distinct-Atomic-database
  ClickHouse-to-ClickHouse rebuild, with pinned ClickHouse 24.8 discovery,
  ordering metadata, bounded native writes, destructive acknowledgement,
  drop-all-before-create preparation, checkpoint fencing, and fail-closed
  lifecycle recovery.
- Same-endpoint and database-alias guards for the native same-engine routes.
- Capability validation before adapter construction or mutation, including
  rejection of ClickHouse upsert and the not-yet-implemented strict
  consistency scopes.
- A fail-closed MySQL/MariaDB native bulk path using in-memory
  `LOAD DATA LOCAL INFILE` staging when safely available, with exact
  row/warning/count proof and a visible, latched strict-insert fallback.

## Verification recorded on 2026-07-30

- The complete mandatory-environment TLS matrix passed verbosely on Linux with
  every named fixture running and no skips: engine `0.909s`, migrate `9.009s`.
- The same complete matrix passed under `go test -race -v` on Linux, again
  with no skips: engine `1.824s`, migrate `14.396s`.
- The network/ClickHouse subset also passed in normal and race modes on macOS.
  The SQLite-to-SQLite fixture is intentionally run in Linux because DMTX's
  safe finite-memory probe does not admit migrations on Darwin.
- Full Linux `go test ./...` and `go test -race ./...` passed, including the
  SQLite same-engine fixture.
- Compilation, `go vet ./...`, native and Windows command builds, and
  `git diff --check` passed.

## Supported live environment

The final gate is pinned to PostgreSQL 16.x, SQL Server 2022 product major 16
with compatibility level 160, Oracle MySQL 8.0.16+ sources and 8.0.30+ native
targets within the 8.0 line, MariaDB 10.11.8+ within the 10.11 line, and
ClickHouse 24.8.x Atomic databases.

Every network route requires encrypted TLS. The MySQL/MariaDB, SQL Server, and
ClickHouse live endpoints are CA-verified. The current PostgreSQL adapter uses
`sslmode=require`; the fixture bootstrap DSN is `verify-full`, but PostgreSQL
adapter certificate verification is not part of the Stage 3 claim.

The native MySQL-family bulk path needs global `local_infile=ON` and target
`CREATE TEMPORARY TABLES` permission to use staging. Disabled or unavailable
local infile/staging falls back once, visibly, to strict bounded inserts after
safe cleanup. Arbitrary client-file loading remains disabled.

For an exit-gate run, set every variable listed in
`STAGE3_REQUIREMENTS_TESTS.md` and `DMTX_STAGE3_LIVE_REQUIRED=1`. With that
flag enabled, any missing variable is fatal. Without the flag, the sentinel
skips by design so the ordinary offline suite remains usable.

## Completion boundary and next work

Stage 3 is complete at the implementation checkpoint above. A subsequent
documentation/evidence commit records this handoff without changing the
implementation boundary. Do not push, merge, or touch `main` without explicit
user direction.

The Stage 3 boundary is fresh-run adapter/capability certification. Network
resume/replay under process termination, strict consistency, incremental
watermarks, delete reconciliation, and schema evolution remain Stage 4 work;
do not imply those guarantees in a Stage 3 completion report.

## Working rules

- Keep production code modular and human-readable.
- Keep test-only functionality in `_test.go` files.
- Preserve unrelated or externally dirty worktree changes.
- Work in bounded, verified local commits.
- Do not push, merge, or touch `main` without explicit user direction.
