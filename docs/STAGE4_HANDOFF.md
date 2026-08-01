# DMTX Stage 4 handoff

Updated 2026-07-31. This is a continuation note for the next AI working in
the local repository. Preserve the existing worktree; do not reset, clean, or
checkout another branch.

## Current repository state

- Branch: `codex/stage-4-production-semantics`
- HEAD: `20bcbb1 stage4: project intra-schema foreign keys into SQLite`
- Main was not changed; nothing was pushed or merged.
- The worktree is **fully clean**. `docs/STAGE4_REQUIREMENTS_TESTS.md` is now
  tracked and committed.

Verified green at HEAD, **including the full live TLS matrix**:
`go test ./... -count=1` and `go test -race ./... -count=1` with every live
endpoint enabled, plus `go vet ./...`, `gofmt -l`, and `git diff --check`.

`docs/STAGE4_REQUIREMENTS_TESTS.md` is the requirements map. It was user-owned
and off-limits until 2026-07-31, when John directed that it be continued; it is
now maintained alongside the code. Refresh it when a slice lands rather than
letting it drift — the version committed on 2026-07-31 had to be reconciled
against roughly 450 tests it never named. Its "Remaining work to declare Stage 4
complete" section is the current definition of done.

## What is already committed

The branch contains bounded Stage 4 commits for resumable relational network
transfers, schema snapshots/contracts/evolution, deep validation, spatial and
type metadata, deterministic preflight, PostgreSQL incremental windows,
PostgreSQL strict consistency, PostgreSQL delete reconciliation, and supporting
atomic state primitives. The most recent commits are:

The 2026-07-31 session added the commits below on top of the delete slice. The
last three matter most: the live matrix was run for the first time, and the two
real bugs it exposed were fixed.

```text
20bcbb1 stage4: project intra-schema foreign keys into SQLite
c3762af stage4: count validation through the stable source view
170bdb0 stage4: run the live TLS matrix and record results
bea627f stage4: record why the remaining strict engines need servers
54cd53e stage4: compose SQLite strict through the coordinator
06753d0 stage4: close the strict rejection row
efd6958 stage4: implement SQLite strict consistency
36496f6 stage4: prove run record round-trip and token redaction
c67c2c7 stage4: refresh the handoff to current state
0a75352 stage4: correct the upsert contract-creation row
ee1b9e3 stage4: reconcile four more stale requirement rows
08cf998 stage4: crash-test YAML replacement with expanded evidence
5ab55aa stage4: record the dry-run target preflight hazard
eb26238 stage4: disclose dry-run pagination selection
d263bac stage4: require the complete live matrix environment
275b1b6 stage4: label dry-run row count provenance
7826093 stage4: disclose dry-run tuning and delete policy
95e2557 stage4: refine the inert configuration assessment
88c9f53 stage4: correct the schema-contract section of the requirements map
1df2b0f stage4: record the inert configuration audit
1bb4eec stage4: prove cross-process target lease exclusivity
6f7dc6f stage4: prove every required-write failure exits state
943b7a9 stage4: prove deterministic tuning preserves pinned intent
5ee4c7a stage4: correct the validation section of the requirements map
9f07eba stage4: reconcile the requirements map with committed evidence
5b6d6bd stage4: compose stable network aggregate completion
b2ad045 stage4: allow pre-mutation table inventory revision
83e60c1 stage4: separate stable network planning from durable work
554d5e0 stage4: publish run completion atomically
4190435 stage4: read aggregate completion evidence
1afd447 stage4: reconcile PostgreSQL deletes
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

## Live TLS matrix: RUN 2026-07-31

**The 2026-08-06 block did not apply.** It was a Codex approval-service quota,
not a property of this repository or environment. All five TLS containers were
already running and healthy, and the matrix was executed directly. Do not
re-propagate the "blocked until 2026-08-06" claim; it is wrong.

Working environment (local test containers, throwaway credentials). Two target
databases must exist first — the fixtures need a target distinct from the
source:

```sh
docker exec dmtx-mysql80-tls mysql -uroot -pdmtx_root_test_only \
  -e "CREATE DATABASE IF NOT EXISTS dmtx_target; GRANT ALL ON dmtx_target.* TO 'dmtx'@'%'; FLUSH PRIVILEGES;"
docker exec dmtx-mariadb1011-tls mariadb -uroot -pdmtx_root_test_only \
  -e "CREATE DATABASE IF NOT EXISTS dmtx_target; GRANT ALL ON dmtx_target.* TO 'dmtx'@'%'; FLUSH PRIVILEGES;"
docker exec dmtx-mssql2022-tls /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
  -P 'TestPass2024' -C -Q "IF DB_ID('dmtx_target') IS NULL CREATE DATABASE dmtx_target;"
```

```sh
export DMTX_TEST_POSTGRES_DSN="postgres://dmtx:dmtx_test_only@127.0.0.1:55432/dmtx_test?sslmode=verify-full&sslrootcert=/private/tmp/dmtx-postgres16-tls.zlfSES/server.crt"
export DMTX_TEST_MYSQL_CA=/private/tmp/dmtx-mysql80-tls/certs/ca.pem
export DMTX_TEST_MARIADB_CA=/private/tmp/dmtx-mysql80-tls/certs/ca.pem
export DMTX_TEST_MSSQL_CA=/private/tmp/dmtx-mssql2022-tls/certs/ca.pem
export DMTX_TEST_MYSQL_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:53306)/dmtx?tls=dmtx_test&parseTime=true"
export DMTX_TEST_MYSQL_TARGET_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:53306)/dmtx_target?tls=dmtx_test&parseTime=true"
export DMTX_TEST_MYSQL_ADMIN_DSN="root:dmtx_root_test_only@tcp(127.0.0.1:53306)/dmtx_target?tls=dmtx_test&parseTime=true"
export DMTX_TEST_MARIADB_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:54306)/dmtx_source?tls=dmtx_mariadb_test&parseTime=true"
export DMTX_TEST_MARIADB_TARGET_DSN="dmtx:dmtx_test_only@tcp(127.0.0.1:54306)/dmtx_target?tls=dmtx_mariadb_test&parseTime=true"
export DMTX_TEST_MSSQL_DSN="sqlserver://sa:TestPass2024@127.0.0.1:51433?database=master&encrypt=true&tlsmin=1.2&guid+conversion=true&certificate=/private/tmp/dmtx-mssql2022-tls/certs/ca.pem"
export DMTX_TEST_MSSQL_TARGET_DSN="sqlserver://sa:TestPass2024@127.0.0.1:51433?database=dmtx_target&encrypt=true&tlsmin=1.2&guid+conversion=true&certificate=/private/tmp/dmtx-mssql2022-tls/certs/ca.pem"
```

Non-obvious requirements the fixtures enforce, each of which cost a debugging
cycle to discover:

- TLS config names are fixed: `dmtx_test` for MySQL, `dmtx_mariadb_test` for
  MariaDB.
- SQL Server additionally requires `guid conversion=true` and `tlsmin=1.2`.
- MySQL and MariaDB DSNs need `parseTime=true`, or bulk-writer round-trips fail
  scanning DATETIME into `time.Time`.
- `DMTX_TEST_MYSQL_ADMIN_DSN` (root) is needed for the binary-log-safe trigger
  sentinel. Note it **fails rather than skips** when the target DSN is set but
  the admin DSN is not — deliberate fail-loud behaviour on partial provisioning.

**Provision all of it.** With only the source DSNs set, 38 live tests skip
silently. With the full set above, 9 skip and everything else runs.

### Result: the whole matrix is green

`go test ./... -count=1` and `go test -race ./... -count=1` both pass with the
**full** environment above — every engine, both target databases, and the admin
DSN. All eight packages green under both, no data races.

This is the widest configuration the repository supports. Reproduce it with the
provisioning commands and exports above; anything narrower silently skips tests
rather than failing, which is how the gaps below stayed hidden.

The run initially surfaced six failures, all in `internal/migrate`. Each was
verified **pre-existing** by rerunning it in a detached worktree at `ccc985b`,
the pre-session commit — the 2026-07-31 session introduced no live regression.
They resolved to two root causes, both now fixed.

**1. Validation counted through the pool while the stable view held the only
connection** (`c3762af`). `stage4AdapterValidationProbe` returned early for
`count_only` mode and built the probe from the pool adapter, ignoring the
`providerSource` the caller supplied. The deeper validation modes used it
correctly; only the default mode did not. MySQL, MariaDB, and SQL Server cap
their source pool at `MaxOpenConns(1)` (`internal/engine/mysql.go`,
`internal/engine/mssql.go`) while PostgreSQL does not, so only those three
deadlocked — waiting for a connection the caller was itself holding, until the
context deadline. Diagnosed by watching the MySQL process list during the hang:
two idle connections, zero blocked queries, which ruled out a server-side lock
and pointed at client-side pool starvation. Counting through the stable view is
also the more truthful measurement: it counts the snapshot that was transferred.
`TestStage4MySQLStableRunnerLiveTLS` went from a 20s deadline exceeded to 0.1s.

**2. Intra-schema foreign keys were never unqualified for SQLite** (`20bcbb1`).
The SQLite projections normalized `Match`, `OnUpdate`, and `OnDelete` but left
`ReferencedSchema` set, and `renderSQLiteForeignKey` refuses any qualified
reference. SQLite has one namespace, so a reference inside the migrated schema
is expressible only unqualified — the SQLite database *is* that schema. The
projections now clear the qualifier for same-schema references and still refuse
cross-schema ones, which genuinely name a relation the migration does not carry.
Fixed in the projection rather than the renderer: the renderer cannot know the
mapping. This also restored the destructive-acknowledgement gate, which the
schema error had been masking.

## Decisions waiting on John

Three items are blocked on a product decision, not on effort. Nothing further in
those areas should be built until they are answered.

1. **Target preflight in dry-run.** How should a not-yet-existing target be
   treated? For a `drop_recreate` migration into a fresh SQLite file, absence is
   the normal case, not an error. See the hazard note in the requirements map:
   `db.Ping()` creates a SQLite file, so a naive preflight would violate
   dry-run's zero-mutation guarantee. **This decision also gates schema drift
   reporting**, the other open item in block E: drift needs the target catalog,
   and dry-run opens no target today. Answering this unblocks both.
2. **May dry-run open state read-only?** Delete due-ness needs the durable
   last-success time. Today `Plan.Deletes.DueStateKnown` is permanently false
   and the reporting half of the requirement cannot close either way until this
   is settled.
3. **The five inert settings** (`checkpoint_frequency`, `upsert_merge_size`,
   `large_table_threshold`, `runtime_tuning_interval`, `history_retention_days`).
   Implement, reject at parse, or remove — per row. See block F2 in the
   requirements map; note the warning there against "fixing" them by stripping
   them from the resume projection.

## Correction: strict route admission is not a gate flip

Recorded 2026-08-01, after John chose "admit all four". My recommendation
understated the cost and should not be acted on as described.

What is actually true:

- The four openers (`SQLiteStrictConsistencyOpener`,
  `MySQLStrictConsistencyOpener`, `SQLServerStrictConsistencyOpener`, and the
  MariaDB path through the MySQL opener) are implemented and their **opener
  contract** is live-proven: open a stable view, hold it, refuse invalid scopes.
- **Nothing consumes them.** Verified by locating every non-test reference:
  each constructor is referenced only by its own definition and its tests. They
  are inert in exactly the sense block F2 uses for the five settings.
- Admission is gated in **three** places, not one: `migration_validation.go`
  (route resolution), `requireStage4PostgresStrictRoute` (adapter admission),
  and `sqlite_transfer_pipeline.go` (SQLite refuses strict as policy).
- The composed strict route is PostgreSQL-typed throughout — roughly 1,400 lines
  asserting `*PostgresStrictConsistencySession`, `*postgresTargetAdapter`, and
  `*relationalSourceAdapter`, plus postgres-specific epoch binding, network
  planning, retained row bounds, catalog equality, and completion evidence. The
  stable-network-view layer takes a `*PostgresStrictConsistencySession` directly.

So opening the gate would admit configurations the composed route cannot serve.
Delivering strict consistency on those engines means building a Stage 4 composed
route per engine — network planning, durable ranges, epoch binding, resume, and
validation evidence — which is comparable in size to what Stage 4 already did
for PostgreSQL, not a flag change. For SQLite it is larger still, because SQLite
is not a certified Stage 4 **source** at all, so strict would ride on top of a
route that does not exist yet.

This needs a fresh decision. The honest options are to scope it as its own
stage, to pick one engine (MySQL and MariaDB share an opener and are the
cheapest pair), or to leave the openers unreachable and say so in the docs
rather than carrying them as if they shipped.

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
| `CompleteStage4Run` | `migrate.PublishStage4RunCompletion` (added by step 2) |

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
2. **Done.** `migrate.PublishStage4RunCompletion` completes every durable schema
   sentinel and publishes the successful run in one mutation. It recovers table
   completions and sentinels from durable state rather than in-memory context,
   so a resumed process publishes exactly what the original would have, and no
   field was added to `migrate.Result` (that struct is the JSON stdout
   contract). The date-based incremental route no longer calls
   `completeStage4SchemaGateSentinels`; its sentinels stay running until
   publication. `internal/app/app.go` and `internal/app/resume.go` attempt the
   publication after their `validation_completed` audit and fall back to
   `store.Append` when it reports false.

   Two decisions worth preserving. The publication must stay caller-driven and
   run *after* the validation audit: terminal repair refuses a successful run
   whose validation evidence is missing, so publishing inside `migrate` would
   strand any run that crashed in that gap. And it deliberately uses
   `context.Background()` rather than the cancellable migration context, so a
   late signal cannot strand an outcome whose transfer already succeeded and
   whose lease was reverified.

   Route discrimination is state-driven: a durable table inventory is the marker
   that a route composed aggregate evidence. No inventory means skip and fall
   back; an inventory with incomplete composition fails closed. Step 3 therefore
   inherits aggregate completion as soon as the network route publishes an
   inventory.

   Coverage gap: the app's fallback branch is covered by the existing SQLite
   route tests, but the published-true branch needs a PostgreSQL incremental
   route through `app`, which is live-only. Add it to the TLS matrix rerun.
3. **Done.** Stable-network aggregate composition, landed as two commits.

   `b2ad045` opens a narrow revision window in the state layer. A resumed run
   whose source grew during an outage replans its range set, and the inventory
   pins the exact range identities a table completion is validated against, so
   freezing the first plan made such a run *unrecoverable* rather than merely
   failed. `EnsureStage4TableInventory` now replaces a differing inventory when
   the run has zero aggregate receipts and zero completed ordinary tables; the
   schema authority stays immutable across a revision, and the window closes
   permanently once any table publishes terminal evidence.

   The route commit then publishes the inventory on fresh runs in the only order
   the state layer accepts — plan every table without writing durable work,
   stage the schema snapshot, publish the inventory, checkpoint the ordinary
   table set, commit the work plans — and replaces the separate
   `completeStage4AdapterWorkItem` plus `observer.AfterTable` pair with one
   `CompleteStage4Table` on both the stable-network and PostgreSQL delete
   routes. On resume the inventory is adopted, and revised by a plan-only
   prepass when no table has completed yet.

   Consequences worth knowing. A composed route no longer emits `AfterTable`;
   the ordinary task is completed by the aggregate mutation, exactly as on the
   incremental route. A failed table completion now leaves no terminal evidence
   at all rather than a structurally complete table whose ordinary checkpoint
   never landed. Runs without an inventory — anything resumed from before this
   change — keep the older pair, so nothing in flight is stranded.

   Test observers in `internal/migrate` now persist ordinary table rows the way
   the application's checkpoint observer does, creating only missing ones on
   resume. An observer that merely records events cannot represent a Stage 4 run
   any more, because aggregate completion reconciles the ordinary task.

   Earlier note in this file claimed the change reversed a deliberately-tested
   "tasks before snapshot" ordering invariant. That was wrong: staging moved
   inside the aggregate-capability guard, and all three evolution tests pass
   untouched because they exercise non-composed routes.

   Superseded planning notes follow for context. `planTable` is now
   split out of `openTable` in `adapter_stage4_network_runner.go`: it
   materializes a table's exact pagination plan, range inventory, and transfer
   plan while writing nothing durable, and hands back the open session. The
   extraction is behavior-preserving — `openTable` still owns the guard, the
   session close on failure, the durable write, and the global range offset
   advance, in that order.

   **The remaining work must land as one coupled slice, not two.** Publishing a
   table inventory without also publishing per-table receipts would break every
   stable-network migration: `PublishStage4RunCompletion` treats a durable
   inventory as proof the route composed aggregate evidence, and
   `validateStage4RunInventory` then requires exactly one
   `CompleteStage4Table` receipt per inventory table. An inventory with zero
   receipts fails closed at the end of an otherwise successful run. So the
   ordering change and `CompleteStage4Table` adoption on this route are a single
   atomic change.

   Ordering the change needs, on a fresh run only:

   1. plan every table with `planTable`, collecting task/strategy/topology and
      range IDs, closing each session;
   2. `EnsureStage4TableInventory` from the collected plans;
   3. `checkpointStage4AdapterTableSet` (this is what creates ordinary tasks,
      and it currently runs *first*, which is the whole problem);
   4. commit the durable work plans.

   On resume, do not rebuild the inventory: completed tables are skipped so the
   exact original inventory cannot be reconstructed, and ordinary tasks already
   exist. Read it with `LoadStage4TableInventory` instead. A resumed run whose
   inventory was never published simply has none, and
   `PublishStage4RunCompletion` already degrades to the ordinary `store.Append`
   path for it.

   Also still required on this route: replace the `observer.AfterTable` plus
   `completeStage4AdapterWork` pair with `CompleteStage4Table`, including in
   `runStage4AdapterPostgresDeleteNetworkTables`. Verify first that the sum of
   durable range `RowsDone` equals the ordinary copied count per table —
   `applyStage4TableCompletion` enforces that equality and it is unproven on
   this route.

## Immediate safe next steps

1. Re-read `git status --short --branch`. The worktree should be clean.
2. **Rerun the TLS live matrix once the approval quota permits it** (retry date
   2026-08-06). **Arm the exit gate**: set `DMTX_STAGE4_LIVE_REQUIRED=1` so
   `TestStage4LiveMatrixEnvironmentRequired` fails on any missing endpoint.
   Every live fixture skips on an unset DSN, so an unarmed run against a
   half-provisioned environment reports success while proving almost nothing.
   This is the highest-value action available and it unblocks most of what
   remains.
3. Get answers to the three decisions above before building in those areas.
4. Then work the requirements map's "Remaining work to declare Stage 4
   complete" list. After the 2026-07-31 reconciliation the largest genuine gaps
   are strict consistency for MySQL, MariaDB, SQL Server, and SQLite; the
   per-engine live route matrices; and dry-run target preflight and drift
   reporting. Keep unsupported combinations explicitly fail-closed.

**Read the map before assuming anything is unbuilt.** Eight rows that read
"missing" or "in progress" on 2026-07-31 turned out to be finished under
different fixture names. Verify by name against the test inventory first:
`grep -rho "^func Test[A-Za-z0-9_]*" internal/ | sed 's/func //' | sort -u`

## Do not claim Stage 4 complete yet

Stage 4 remains incomplete, but the shape of "incomplete" changed on
2026-07-31. Aggregate production composition **landed** for the incremental,
stable-network, and PostgreSQL delete routes. The reconciliation then showed
that validation, schema contract, and engine retry classification were already
finished under fixture names the map never used.

What is genuinely outstanding is now mostly **the live gate**, not unbuilt
logic: the TLS matrix rerun, per-engine route matrices, strict consistency for
four engines, and dry-run target preflight and drift reporting.

Stage 4 cannot be declared complete until the live matrix runs. That is a
calendar constraint (2026-08-06), not an effort one — no amount of local work
closes it, and a locally green `go test ./...` is not evidence about the live
gates.
