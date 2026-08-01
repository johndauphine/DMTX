# Stage 4 closeout handoff

Updated 2026-08-01. This is the single restart point for the next AI. It
replaces the prior Stage 3 and Stage 4 handoff documents; the detailed
requirements map remains [STAGE4_REQUIREMENTS_TESTS.md](STAGE4_REQUIREMENTS_TESTS.md).

## Safety and workspace rules

- Preserve the shared dirty worktree. Do **not** run reset, clean, checkout,
  or any broad destructive operation.
- Nothing in this checkpoint was committed, staged, pushed, or put in a PR.
- The user authorized any necessary work in ephemeral local Docker databases
  and containers. Do not print DSNs, passwords, certificate paths, or other
  credentials in test output or handoff notes.
- Use Terra for follow-on implementation where a model choice is available.
- Avoid review loops. Act only on findings that change correctness, safety, or
  conformance; otherwise record the result and continue.

## Pause state

Work was intentionally paused to conserve the final weekly usage budget. The
best honest estimate at pause was about **90% overall**:

- implementation: roughly 95%;
- Stage 4 exit evidence: roughly 85–90%.

The broad normal/race and fully armed TLS suites passed **before** the final
closeout additions listed below. Do not treat that earlier green result as the
final gate for the current dirty tree: re-run it after the remaining work is
finished.

Start with:

```sh
git status --short --branch
git diff --check
go test ./internal/migrate -run '^$' -count=1
```

Those commands are intentionally cheap and establish whether a paused
in-progress test file needs repair before any broad test run.

## Verified completed closeout evidence

These slices reported focused normal/race success before the pause. Keep their
explicit refusal boundaries intact.

1. **Strict consistency hard-kill matrix**
   - Added `internal/migrate/adapter_stage4_strict_process_kill_live_test.go`.
   - Uses external child-process hard kill, durable evidence, source mutation,
     and original-run resume for PostgreSQL table/migration, SQL Server
     table/migration, MySQL table, MariaDB table, and SQLite table scope.
   - Each source/scope contract was exercised against YAML and SQLite state
     (14 cells). SQL Server migration reopens its durable snapshot; the other
     contracts mint a fresh valid epoch after process loss.
   - A real MySQL/Maria strict pool-capacity defect was fixed in
     `adapter_stage4_mysql_strict.go` and unit-covered.
   - Keep MySQL/MariaDB and SQLite migration scope plus ClickHouse strict
     refused before mutation.

2. **Same-engine delete reconciliation hard-kill matrix**
   - Added/extended `internal/migrate/adapter_stage4_delete_process_kill_live_test.go`.
   - Real hard kill after native receipt commit and before state acknowledgement
     passed for PostgreSQL, MySQL 8.0, MariaDB 10.11, SQL Server 2022, and
     SQLite, each with YAML and SQLite state.
   - Cross-engine delete remains deliberately refused.

3. **Incremental route matrix**
   - `internal/migrate/adapter_stage4_incremental_matrix_live_test.go` passed
     a real TLS 4×4 canonical PostgreSQL/SQL Server/MySQL/SQLite matrix and an
     explicit MariaDB 10.11 alias representative.
   - SQLite-to-SQLite has real subprocess crash/resume proof on both state
     backends, including post-fence mutation exclusion and spool cleanup.
   - Known repaired issues: legacy SQLite incremental routing, stale crash
     spool cleanup, and MySQL-to-PostgreSQL varchar/collation projection.
   - Remaining evidence gap: network-backed incremental cells do not yet each
     have their own external hard-kill route. Do not claim that broader matrix
     complete unless it is added and run.

4. **Deep validation route matrix**
   - Added `internal/migrate/adapter_validation_route_matrix_live_test.go`.
   - Reported passing focused normal and race runs for the same 4×4 canonical
     real TLS matrix plus MariaDB flavor coverage.
   - Covers exact count, NULL parity, typed sample canonicalization, binary
     collation authority, pre-mutation refusals for unproven text/collation
     keys and ClickHouse, and a real PostgreSQL lock-based timeout policy test.
   - This result still needs the final shared-tree gate after other paused
     additions settle.

5. **Aggregate-publication lifecycle repair**
   - `internal/migrate/adapter_stage4.go` now has
     `completeStage4AdapterTerminalSchemaGateSentinels`.
   - Routes with durable aggregate inventory leave schema sentinels pending so
     `PublishStage4RunCompletion` can give them one atomic terminal timestamp
     with the successful run. Legacy/no-inventory routes retain direct
     completion.
   - The defect was found by real delete and rebuild kill/resume work: direct
     sentinel completion beforehand caused `ErrImmutableEvidence` during
     aggregate publication.
   - Regressions were added for normal, rebuild, delete, and no-inventory
     paths. The rebuild/app external-kill harness still needs its post-fix
     rerun.

## Paused work that must be completed before declaring Stage 4 done

Treat every item below as unverified until its focused and final gates pass.

1. **Rebuild and application aggregate publication process-kill rerun**
   - Harnesses exist in
     `internal/migrate/adapter_stage4_rebuild_process_kill_live_test.go` and
     `internal/app/stage4_rebuild_aggregate_process_kill_live_test.go`.
   - Re-run them using the full local TLS fixture mapping after the aggregate
     sentinel repair. They must prove each admitted rebuild target family and
     both state backends recover finalization/validation/publication, preserve
     FK/object/row facts, and complete the original run truthfully.
   - The prior attempt skipped one selector solely because its shell lacked the
     SQL Server fixture variables; that is not an acceptable final result.

2. **Schema-contract/evolution target matrix**
   - A Terra worker was actively adding this matrix when stopped. Inspect its
     dirty changes before editing.
   - Close `TestStage4SchemaContractTargetMatrixLive` (or an equivalent named
     gate) for PostgreSQL, MySQL, MariaDB, SQL Server, and SQLite targets.
   - Exercise safe create/add/relax/widen behavior through production
     composition, target-only object preservation, and collision/FK
     fail-closed cases. Do not claim unsupported DEFAULT/incoming-FK/copy-swap
     shapes are supported merely to fill a cell.

3. **Ordinary upsert/network hard-kill replay matrix**
   - A Terra worker had just started this slice. Find its partial work before
     creating duplicate fixtures.
   - Add a bounded external-child interruption/replay proof for every admitted
     idempotent upsert target family: PostgreSQL, MySQL 8.0, MariaDB 10.11,
     SQL Server 2022, and SQLite.
   - Kill after native target write/receipt and before durable acknowledgement;
     resume the original run and prove exact rows, no duplicate/no overwrite,
     target-only retention, durable range/frontier, and truthful aggregate
     completion. Use both state backends where the protocol differs.

4. **Dry-run/preflight live route proof**
   - A Terra worker had just started this slice. Check for partial files before
     writing a new one.
   - Add a bounded armed TLS capability-representative matrix that proves
     source/target preflight, schema-drift and read-only delete-candidate
     disclosure, configuration-only refusal before endpoint/artifact mutation,
     and absent SQLite target non-proceed without target/state/spool creation.

5. **Final evidence reconciliation**
   - Update `docs/STAGE4_REQUIREMENTS_TESTS.md` and this handoff only after
     the new matrices actually pass.
   - Remove an "open" claim only when a named test and the exact live/race
     command provide the evidence. Keep all deliberate refusals explicit.

## Local armed live gate

The final gate must use `DMTX_STAGE4_LIVE_REQUIRED=1`. The preflight requires
these variable *names* (never record their values here):

```text
DMTX_TEST_POSTGRES_DSN
DMTX_TEST_MYSQL_DSN
DMTX_TEST_MYSQL_TARGET_DSN
DMTX_TEST_MYSQL_ADMIN_DSN
DMTX_TEST_MYSQL_CA
DMTX_TEST_MARIADB_DSN
DMTX_TEST_MARIADB_TARGET_DSN
DMTX_TEST_MARIADB_ADMIN_DSN
DMTX_TEST_MARIADB_CA
DMTX_TEST_MSSQL_DSN
DMTX_TEST_MSSQL_TARGET_DSN
DMTX_TEST_MSSQL_CA
DMTX_TEST_CLICKHOUSE_DSN
DMTX_TEST_CLICKHOUSE_SOURCE_DSN
DMTX_TEST_CLICKHOUSE_TARGET_DSN
DMTX_TEST_CLICKHOUSE_CA
```

The local ephemeral fixture set has PostgreSQL 16, MySQL 8.0, MariaDB 10.11,
SQL Server 2022, ClickHouse 24.8, and SQLite routes. It was provisioned with
verified TLS and least-privilege ClickHouse grants. Reconstruct the in-memory
environment mapping from the local containers or existing fixture helpers; do
not persist secrets in the repository.

After targeted matrices pass, run:

```sh
DMTX_STAGE4_LIVE_REQUIRED=1 go test ./... -count=1
DMTX_STAGE4_LIVE_REQUIRED=1 go test -race ./... -count=1
go vet ./...
git diff --check
```

Run the full gates only after the focused matrix files compile. If a full gate
fails, distinguish a real regression from a partially edited shared file and
fix only the material issue.

## Definition of done

Phase Four may be declared complete only when:

1. the paused matrices above have real focused evidence (normal and race);
2. the final armed normal and race gates pass on the current tree;
3. `go vet ./...` and `git diff --check` pass;
4. the requirements map accurately distinguishes implemented capability cells
   from deliberate pre-mutation refusals; and
5. the run/recovery evidence proves truthful completion of the original run,
   not merely a successful child-process exit.

Until then, report Stage Four as approximately 90% complete rather than done.
