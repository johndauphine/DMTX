# DMTX Stage 4 handoff

Updated 2026-07-31. This is a continuation note for the next AI working in
the local repository. Preserve the existing worktree; do not reset, clean, or
checkout another branch.

## Current repository state

- Branch: `codex/stage-4-production-semantics`
- HEAD: `ccc985b stage4: compose production preflight`
- Main was not changed, and nothing was pushed or merged.
- The worktree is intentionally dirty with the PostgreSQL delete-reconciliation
  slice below. None of these files are staged.

Modified:

- `internal/migrate/adapter_stage4.go`
- `internal/migrate/adapter_stage4_network_runner.go`
- `internal/migrate/adapter_stage4_test.go`
- `internal/migrate/delete_reconcile.go`
- `internal/migrate/delete_reconcile_test.go`

Untracked implementation/tests:

- `internal/migrate/adapter_stage4_postgres_delete_composed_live_test.go`
- `internal/migrate/adapter_stage4_postgres_delete_lifecycle.go`
- `internal/migrate/adapter_stage4_postgres_delete_lifecycle_test.go`
- `internal/migrate/adapter_stage4_postgres_delete_runner.go`
- `internal/migrate/adapter_stage4_postgres_delete_runner_test.go`

This handoff file, `docs/STAGE4_HANDOFF.md`, is also newly untracked and
should be preserved; it may be committed with the bounded slice once normal
git approval is available.

`docs/STAGE4_REQUIREMENTS_TESTS.md` is an existing user-owned untracked
requirements map. Do not edit, stage, or delete it.

## What is already committed

The branch contains bounded Stage 4 commits for resumable relational network
transfers, schema snapshots/contracts/evolution, deep validation, spatial and
type metadata, deterministic preflight, PostgreSQL incremental windows,
PostgreSQL strict consistency, and supporting atomic state primitives. The
most recent commits are:

```text
ccc985b stage4: compose production preflight
a77c015 stage4: compose PostgreSQL strict consistency
df0d4bb stage4: harden PostgreSQL delete authority
4d70f42 stage4: publish completion atomically
db1e2b6 stage4: compose PostgreSQL schema evolution
6e7fe06 stage4: validate relational data deeply
```

These commits do not prove that the full Stage 4 matrix is complete.

## Uncommitted slice: PostgreSQL delete reconciliation

The current patch composes a PostgreSQL-to-PostgreSQL, upsert, non-strict,
non-incremental delete-reconciliation route. It includes:

- hard/interval/PK-enforced due scheduling;
- child-first target-only deletion planning;
- durable attempts, receipts, fenced mutations, and stable attempt IDs;
- crash replay after target delete/receipt but before state acknowledgement;
- fully transferred resume without recopying data;
- strict-count propagation and cross-run NotDue scheduling;
- fresh catalog authority checks for relation identity, owner, privileges,
  primary key/index/opclass/collation/equality, including terminal reuse;
- pre-finalize authority verification before range advancement or mutation;
- path-confined retry cleanup for crash-leftover terminal spools.

The implementation is intentionally fail-closed outside this certified route.

## Verification already obtained

On the current patch, the following non-live gates were green:

- `go test ./... -count=1`
- `go test -race ./... -count=1`
- `go vet ./...`
- `gofmt -d` over all changed Go files (empty output)
- `git diff --check`
- cross-builds for Linux, Windows, and Darwin on amd64 and arm64
- focused delete, authority, spool-cleanup, and focused race tests

Existing verified-TLS PostgreSQL live tests were green before the final
read-only authority/spool hardening, including direct delete, composed
parent/child-FK, zero-candidate strict, cross-run NotDue, and crash receipt
replay/resume routes. A final Docker TLS rerun was attempted but the approval
service rejected the command because the account usage/approval quota was
exhausted (retry date reported as 2026-08-06). Do not work around that limit.

Known local Docker endpoint from earlier validation:

- container: `dmtx-postgres16-tls`
- host port: `127.0.0.1:55432`
- TLS server certificate was present under `/private/tmp/dmtx-postgres16-tls.zlfSES/server.crt`

Do not assume credentials from this note; inspect the existing test setup when
the live gate can be run.

## Immediate safe next steps

1. Re-read `git status --short --branch` and preserve every dirty file listed
   above. Do not include `docs/STAGE4_REQUIREMENTS_TESTS.md` in any operation.
2. When command approval is available, stage exactly the five modified files,
   five untracked implementation/test files, and this handoff file, rerun the
   focused tests, and make one bounded local commit for the delete slice. Do
   not stage the requirements map. Do not push or merge.
3. Rerun the PostgreSQL TLS live matrix when the approval quota permits it.
4. Implement the next highest-priority Stage 4 gap: compose atomic aggregate
   run completion through production lifecycle paths. The state interfaces
   already exist in `internal/state/stage4_aggregate.go` and the SQLite/YAML
   implementations/tests are present, but `CompleteStage4Run` has no
   production caller.

## Atomic-completion design warning

Current production code completes table/work/sentinel state and then appends a
successful run separately in `internal/app/app.go` and `internal/app/resume.go`.
That leaves a crash window where table evidence is terminal but the run outcome
is not. The next slice must route successful completion through
`Stage4AggregateBackend.CompleteStage4Run` and preserve exact replay semantics.

Important constraints discovered during the audit:

- `EnsureStage4TableInventory` requires an active resumable run, the schema
  sentinel and source snapshot, and zero ordinary table tasks/structured table
  work at inventory creation time.
- Stable relational network planning currently creates/ensures table work later
  in `checkpointStage4AdapterStableNetworkWork`; inventory timing must be
  reconciled without weakening fail-closed state validation.
- `CompleteStage4Table` reconciles the ordinary task, structured ranges, and
  optional incremental evidence; `CompleteStage4Run` additionally requires the
  exact inventory, sentinel snapshots/work, terminal incremental/delete
  evidence, and a stable success reason/time.
- `Stage4RunContext` currently carries run/backend/resume/spool context but not
  the application’s exact success reason/completion timestamp. Fresh and resume
  paths currently use different success reasons.

Audit these paths before editing:

- `internal/state/stage4_aggregate.go`
- `internal/state/stage4_aggregate_sqlite.go`
- `internal/state/stage4_aggregate_yaml.go`
- `internal/app/app.go`
- `internal/app/resume.go`
- `internal/app/checkpoints.go`
- `internal/migrate/stage4_lifecycle.go`
- `internal/migrate/adapter_stage4_incremental.go`
- `internal/migrate/adapter_stage4_postgres_delete_lifecycle.go`

After the aggregate slice, continue the requirements/test map in
`docs/STAGE4_REQUIREMENTS_TESTS.md`, especially deterministic tuning/dry-run,
broader certified relational routes, schema/validation/spatial coverage, and
ClickHouse boundaries. Keep unsupported combinations explicitly fail-closed.

## Do not claim Stage 4 complete yet

Stage 4 remains incomplete. The current evidence is strong for the bounded
PostgreSQL delete slice and several committed primitives, but the aggregate
production composition, final TLS rerun, and broader matrix acceptance are
still outstanding.
