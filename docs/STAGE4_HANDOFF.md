# DMTX Stage 4 handoff

Updated 2026-07-31. This is a continuation note for the next AI working in
the local repository. Preserve the existing worktree; do not reset, clean, or
checkout another branch.

## Current repository state

- Branch: `codex/stage-4-production-semantics`
- HEAD: `1afd447 stage4: reconcile PostgreSQL deletes`
- Main was not changed, and nothing was pushed or merged.
- The worktree is clean except for `docs/STAGE4_REQUIREMENTS_TESTS.md`.

`docs/STAGE4_REQUIREMENTS_TESTS.md` is an existing user-owned untracked
requirements map. Do not edit, stage, or delete it.

## What is already committed

The branch contains bounded Stage 4 commits for resumable relational network
transfers, schema snapshots/contracts/evolution, deep validation, spatial and
type metadata, deterministic preflight, PostgreSQL incremental windows,
PostgreSQL strict consistency, PostgreSQL delete reconciliation, and supporting
atomic state primitives. The most recent commits are:

```text
1afd447 stage4: reconcile PostgreSQL deletes
ccc985b stage4: compose production preflight
a77c015 stage4: compose PostgreSQL strict consistency
df0d4bb stage4: harden PostgreSQL delete authority
4d70f42 stage4: publish completion atomically
db1e2b6 stage4: compose PostgreSQL schema evolution
```

These commits do not prove that the full Stage 4 matrix is complete.

## Committed slice: PostgreSQL delete reconciliation

`1afd447` composes a PostgreSQL-to-PostgreSQL, upsert, non-strict,
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

Rerun immediately before committing `1afd447`, all green:

- `go test ./... -count=1`
- `go test -race ./internal/migrate/ -count=1 -run 'Delete|Authority|Spool'`
- `go vet ./...`
- `gofmt -l` over all changed Go files (empty output)
- `git diff --check`

Earlier on the same patch: full `go test -race ./... -count=1` and cross-builds
for Linux, Windows, and Darwin on amd64 and arm64.

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

## Atomic aggregate run completion: audit findings

Audited 2026-07-31 across `internal/state/stage4_aggregate*.go`,
`internal/state/fence.go`, `internal/app/{app,resume,checkpoints,lifecycle}.go`,
and `internal/migrate/{stage4_lifecycle,adapter_stage4,adapter_stage4_incremental,adapter_stage4_network_runner}.go`.

The problem is real: production completes table/work/sentinel state and then
appends a successful run separately in `internal/app/app.go` and
`internal/app/resume.go`, leaving a crash window where table evidence is
terminal but the run outcome is not. The audit found the fix is not one slice.

### Only the incremental route can reach run completion today

| Aggregate call | Production callers |
| --- | --- |
| `EnsureStage4TableInventory` | `adapter_stage4_incremental.go:290` only |
| `CompleteStage4Table` | `adapter_stage4_incremental.go:440`, `:463` only |
| `CompleteStage4Run` | none |

The stable-network route (which owns the delete slice) finalizes through
`observer.AfterTable` plus `completeStage4AdapterWork` as separate mutations,
with no inventory and no per-table receipts. `CompleteStage4Run` requires both,
so it is reachable only on the date-based incremental route.

### Blocking findings

1. **No exported aggregate read API.** `CompleteStage4Run` digest-matches every
   supplied `Stage4TableCompletion` against its stored receipt, so the caller
   must reproduce each one byte-identically including `CompletedAt`. On resume,
   tables completed in an earlier process were never built by the current one,
   and `readSQLiteStage4TableInventory` / `readSQLiteAggregateTableReceipts` are
   unexported. `Stage4AggregateBackend` is publish-only.
2. **Sentinel completion conflicts.** `CompleteStage4Run` completes the schema
   sentinels itself with `requireAllRunning=true` and fails closed when they are
   already terminal (`replay != wasComplete`). Both routes currently call
   `completeStage4SchemaGateSentinels` before the app appends success, so
   sentinel completion must move into the aggregate publication.
3. **Network-route inventory timing.** Inventory creation requires zero ordinary
   tasks and only running sentinel work (SQLite and YAML enforce this
   identically). `checkpointStage4AdapterStableNetworkWork` creates ordinary
   tasks first, and exact range IDs only exist after `openTable` binds live
   stable pagination. The incremental route is exempt because its range set is
   static.

### Constraints to preserve

- `Stage4RunContext` carries run/backend/resume/spool context but not the
  application's success reason or completion timestamp; fresh and resume paths
  use different reasons (`runSuccessReason` / `resumeSuccessReason`).
- `CompleteStage4Run` must publish exactly those two existing reason strings.
  The terminal-repair path in `internal/app/resume.go` switches on `run.Reason`
  and refuses repair for unknown provenance.
- `Stage4RunContext.Backend` is `RangeBackend` + `Stage4Backend` only; aggregate
  access is by type assertion, as at `adapter_stage4_incremental.go:108`.
- `CompleteStage4Run` copies run identity from the latest record, which is
  stricter than resume's `Append` (that relies on `inheritRunWorkloadIdentity`).
  This is an improvement, not a regression.
- `EnsureStage4TableInventory` requires an active resumable run, the schema
  sentinel and source snapshot, and zero ordinary/structured table work.
- `CompleteStage4Table` reconciles the ordinary task, structured ranges, and
  optional incremental evidence.

### Revised sequencing

1. **Done.** Exported aggregate read API (`LoadStage4TableInventory`,
   `LoadStage4TableCompletions`) on `Stage4AggregateBackend`, `SQLiteStore`,
   `YAMLStore`, and the fence wrapper. Reads are unfenced, matching the existing
   `LoadSchemaSnapshot` convention. Both re-normalize each stored receipt and
   prove its digest before returning it, and completions come back ordered by
   ordinary table — the exact order `normalizeStage4RunCompletion` imposes, so a
   recovered slice can be supplied to `CompleteStage4Run` verbatim. Nothing
   calls these yet; `migrate` must reach them by the same
   `state.Stage4AggregateBackend` type assertion used at
   `adapter_stage4_incremental.go:108`.
2. Compose run completion on the date-based incremental route only: carry the
   reason and completion time into `Stage4RunContext`, move sentinel completion
   into the aggregate publication for that route, and keep every other route on
   the existing `store.Append`, fail-closed.
3. Then rework stable-network inventory timing as its own slice.

## Immediate safe next steps

1. Re-read `git status --short --branch`. Do not include
   `docs/STAGE4_REQUIREMENTS_TESTS.md` in any operation.
2. Implement step 2 of the revised sequencing above.
3. Rerun the PostgreSQL TLS live matrix when the approval quota permits it.
4. After the aggregate slices, continue the requirements/test map in
   `docs/STAGE4_REQUIREMENTS_TESTS.md`, especially deterministic tuning/dry-run,
   broader certified relational routes, schema/validation/spatial coverage, and
   ClickHouse boundaries. Keep unsupported combinations explicitly fail-closed.

## Do not claim Stage 4 complete yet

Stage 4 remains incomplete. The current evidence is strong for the bounded
PostgreSQL delete slice and several committed primitives, but the aggregate
production composition, final TLS rerun, and broader matrix acceptance are
still outstanding.
