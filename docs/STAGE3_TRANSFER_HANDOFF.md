# DMTX Stage 3 transfer handoff

Date: 2026-07-29

## Safe checkpoint

- Branch: `codex/stage-3-network-adapters`
- Checkpoint: `72c160a9c8f0ebe87915ff7670289f9f0a7e18ee`
- Fetch the remote, check out that branch, and pull with fast-forward only before continuing.
- Stage 3 remains incomplete. Do not merge this branch into `main` yet.

## Completed foundation

- Modular source/target adapter and capability framework.
- Neutral identity metadata is the sole portable identity representation.
- SQLite and PostgreSQL identity/frontier preservation, including PostgreSQL sequence lifecycle and locking.
- Native PostgreSQL target composition, COPY, upsert, scalar fidelity, and rich retained-schema handling.
- Safe PostgreSQL catalog default and CHECK-expression constructors.
- Fail-closed guards for numeric forms, `ANY` expressions, float forms, and `CHAR` behavior.
- PostgreSQL 16 `indnullsnotdistinct` catalog-shape guard.

## Verification already passed

The saved foundation passed:

- `go test ./... -count=1`
- `go test -race ./internal/schema ./internal/migrate -count=1`
- `go vet ./...`
- `git diff --check`
- PostgreSQL 16 TLS live tests covering retained defaults/CHECKs, identity/frontier behavior, and sequence/table locking

These gates cover the completed foundation only; they do not prove the Stage 3 exit criteria.

## Exact next task

Implement modular PostgreSQL 16 source discovery. It must discover or fail closed on:

- exact type modifiers
- defaults
- primary-key column order
- identity metadata and frontier
- indexes
- CHECK constraints
- foreign keys
- unexpected PostgreSQL catalog shapes

After that discovery is complete, run and close the PostgreSQL-to-PostgreSQL common fixture.

## Remaining Stage 3 scope

- MySQL/MariaDB source and native target routes
- SQL Server source and native target routes
- ClickHouse source/target rebuild support
- all 12 directed relational migration pairs, same-engine cases, and the required live capability/conformance matrix

## Working rules

- Keep production code modular and human-readable.
- Keep test-only functionality in `_test.go` files; do not place fakes or test helpers in production code.
- Work in bounded, verified slices, then commit and push each slice.
- Merge the Stage 3 branch into `main` only after every Stage 3 exit criterion passes.
