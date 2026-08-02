# Stage 4 requirements and test map

This document maps the Stage 4 work in `RECREATE_DMT.md` Sections 7 through 13
and acceptance criteria 21.5 through 21.9 to current repository evidence and
explicit missing fixtures.

Stage 4 is **not complete**, but it is substantially implemented. Existing
Stage 1 through Stage 3 tests are regression evidence only unless a row below
says that they satisfy the complete requirement. A proposed fixture name
identifies work that does not yet have sufficient automated proof.

**Audit saturation note.** The 2026-07-31 reconciliation corrected eight rows
that claimed work was unimplemented when it was finished. The last sweep
returned one correction in three checks, so the drift is largely worked out —
but treat any row still asserting "absent", "missing", or "in progress" as
suspect until re-verified by name against the test inventory, because that is
how every one of the eight read before it was checked.

**Refreshed 2026-07-31** against branch `codex/stage-4-production-semantics`.
The original revision of this document was written when Stage 4 had not
started; roughly 450 Stage 4 tests now exist that it never named, and several
rows below that read "Missing" were closed by differently-named fixtures. Where
this refresh cites a fixture, that fixture exists in the tree today. Where a row
still says missing, it was verified missing by name against the current test
inventory.

Two cautions on reading the refreshed rows. First, the implemented surface is
capability-gated: an admitted engine family does not waive key, type,
collation, native-writer, target-catalog, or validation admission. Second,
non-live proof does not close a row whose acceptance requires TLS live and
process-kill evidence. A historical TLS matrix run is recorded in
`docs/STAGE4_CLOSEOUT_HANDOFF.md`, but this follow-up does not claim the live exit gate
until it is rerun armed with `DMTX_STAGE4_LIVE_REQUIRED=1`.

Where a row cites the Stage 3 route or common-fixture matrix, the exact current
fixture names are the ones enumerated in `STAGE3_REQUIREMENTS_TESTS.md`; this
map does not rename or weaken those baseline gates.

## Current implementation checkpoint (2026-08-01)

This checkpoint supersedes older single-route and "inert setting" claims in
this map. It records bounded implementation and automated evidence; it does
**not** declare Phase Four complete.

- Keyed `upsert` and `drop_recreate` have native relational/SQLite writer and
  replay paths for PostgreSQL, MySQL/MariaDB, SQL Server, and SQLite where the
  target capability and source/target projection are admitted. Target-catalog
  evolution is likewise capability-gated. SQLite now applies only deterministic
  compatible `relax_nullability`/safe-`widen_type` actions through an atomic
  copy/swap, preserving authenticated retained data and sequence authority;
  unsupported object or FK shapes refuse before mutation.
- Strict consistency composes to the same keyed-upsert target set, subject to
  live writer/validation admission:

  | Source | Table scope | Migration scope |
  | --- | --- | --- |
  | PostgreSQL | implemented | implemented |
  | SQL Server | implemented | implemented, using durable database snapshots |
  | MySQL/MariaDB | implemented | refused |
  | SQLite | implemented | refused |

  ClickHouse strict routes remain refused. `TestStage4StrictStableViewCompositionMatrix`,
  `TestSQLServerMigrationStrictTopologySurvivesPreOwnerAndOrdinaryResume`,
  `TestMySQLStrictConfigurationAdmitsOnlyTableScopeAndSupportedTargets`, and
  `TestSQLiteStrictConfigurationAdmitsOnlyTableScopeAndSupportedTargets` pin
  these boundaries.
- Delete reconciliation is composed only for same-engine PostgreSQL,
  MySQL 8, MariaDB 10.11, SQL Server, and SQLite keyed-upsert cells. SQLite to
  SQLite is also the bounded date-incremental-plus-delete cell; cross-engine
  delete remains refused. `TestStage4SQLiteIncrementalDeleteReconcilesAfterTransfer`
  and `TestStage4IncrementalDeleteConfigurationKeepsOtherRoutesClosed` cover
  the incremental boundary.
- Incremental attempts retain their durable fence and validate the exact
  transferred key scope: exact batch proof plus a final stable target read for
  inclusive `count_only`, `null_parity`, and `sample`; `full` remains refused.
  `TestPrepareStage4AdapterIncrementalAdmitsInclusiveDeepValidationModes`,
  `TestStage4AdapterIncrementalSampleBindsFinalValidationToTransferredAttempt`,
  and `TestStage4AdapterIncrementalFinalEvidenceAvoidsWidePayloadReadsOutsideSample`
  cover the bounded evidence design.
- Dry-run/preflight rejects configuration-only incompatibilities before either
  endpoint opens, reads scoped YAML/SQLite state without artifacts, reports
  schema-drift baseline/decision facts, and reports exact delete candidates only
  when read-only key authority is established. An absent SQLite target is now a
  structured non-proceed target-preflight result, not a skipped-preflight
  success. See `TestStage4DryRunRejectsAbsentSQLiteDropRecreateWithoutArtifacts`,
  `TestApplyDryRunSchemaDriftDisclosesFactsAndPolicy`, and
  `TestRunDryRunDeleteCandidatesUseReadOnlyStateAndPreserveArtifacts`.
- `checkpoint_frequency`, `upsert_merge_size`, and `large_table_threshold` now
  have bounded planning/transfer consumers; runtime tuning has bounded
  controller, persisted history, and terminal audit evidence. Explicit
  `history_retention_days` remains a Stage 5 boundary.

The armed `DMTX_STAGE4_LIVE_REQUIRED` process-kill/composed route matrix,
unproven or deferred capability cells, cross-engine delete, and target protocol
capability discovery remain completion gaps.

## Status vocabulary

- **Covered base**: an existing test proves the reusable lower-level contract,
  but Stage 4 still needs route or composition coverage where stated.
- **In progress, not evidence**: an uncommitted local primitive or test exists,
  but it is not counted until its bounded slice compiles, passes, and lands.
- **Partial**: related behavior exists, but one or more normative clauses are
  absent.
- **Missing**: the required behavior and its proof are not present.
- **Stage 5/6 boundary**: the deterministic data-plane hook belongs in Stage 4,
  while the operator surface, presentation, packaging, or CI distribution is
  deliberately later.

## Bounded implementation slices

The slices are ordered by correctness dependency, not UI convenience.
Status as of 2026-08-01: S4.1-S4.2 **landed for admitted capability cells**;
S4.3 **landed for relational/SQLite target executors**; S4.4 **landed with
bounded deep incremental validation**; S4.5 **landed for same-engine delete
and SQLite incremental-delete**; S4.6 **landed for the source/scope matrix
above**; S4.7-S4.8 **substantially landed**; S4.9 **open pending the armed
live/process-kill composed matrix**.

1. **S4.1 — Configuration and durable evidence model.** Add schema-contract,
   validation, incremental, delete, and strict-consistency configuration;
   extend both state backends with schema snapshots, watermarks, immutable
   fences, delete results, and strict snapshot evidence; make aggregate
   completion atomic.
2. **S4.2 — Resumable network transfer core.** Put every relational adapter
   route on the durable range/attempt/checkpoint protocol already used by
   SQLite, add engine-safe pagination and retry classification, and enable
   fenced network resume.
3. **S4.3 — Schema snapshots and contracts.** Implement deterministic
   comparison, structured decisions, safe evolution, discard projection, and
   dependent-object pruning for each relational target and ClickHouse rebuild.
4. **S4.4 — Incremental watermarks and immutable upper fences.** Implement
   baseline capture, strict-lower-bound windows, full-window resume replay, and
   atomic watermark/table completion.
5. **S4.5 — Delete reconciliation.** Implement due-state scheduling, bounded
   key reconciliation, durable results, dry-run facts, and validation policy.
6. **S4.6 — Strict source consistency.** Implement each supported
   source/scope independently, including snapshot lifecycle and persisted
   count evidence. Keep unsupported combinations fail-closed.
7. **S4.7 — Validation modes.** Add timeout/fallback policy, NULL parity,
   deterministic samples, canonical values, strict-snapshot counts, and stable
   findings.
8. **S4.8 — Deterministic preflight and dry-run.** Produce the complete
   structured preflight model, exact skip behavior, target-aware dry-run, and
   cancellation/final-checkpoint safety.
9. **S4.9 — Cross-route crash/live closeout.** Run the certified relational
   matrix through normal, race, TLS live, and process-kill/resume gates; retain
   ClickHouse's explicit unsupported-capability boundaries.

## Section 7 — Migration lifecycle

### 7.1 Fresh run

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Resolve config, finite memory, engine capabilities, and state capabilities before work; then open both endpoints. | **Covered base:** `TestResolveEffectiveTransferPlanUsesFiniteCgroupV2BudgetAndCapsConcurrency`, `TestResolveEffectiveTransferPlanFailsClosedWithoutSafeFiniteEvidence`, `TestCapabilityValidationPrecedesAdapterConstruction`, and Stage 3 live route fixtures. | S4.1/S4.2: `TestStage4ConfigAndBackendCapabilitiesPrecedeConnections`; TLS live route matrix must prove the same ordering. |
| Acquire exclusive canonical-target ownership, create and bind durable run state, then allow mutable progress. | **Covered base:** `TestNetworkLeaseIdentityNormalizesHostAndDefaultPort`, `TestSQLiteLeaseIdentityCanonicalizesAliasesAndHardlinks`, `TestSQLiteStoreRejectsSecondLiveTargetLease`, `TestFencedBackendsRejectEveryOldGenerationMutationAfterTakeover`. | S4.2: `TestStage4NetworkRunBindsLeaseBeforeProgressLive` and the two-process matrix in Section 11.4. |
| Initialize cancellation and data-plane lifecycle hooks before mutation. | **Partial:** `TestStage2RunSIGTERMPersistsCancelledOutcome` and `TestSQLiteToSQLiteNotifiesTableCheckpointBoundaries`. | S4.8: `TestStage4CancellationInstalledBeforePreflight`; logs, metrics, traces, notifications, and their presentation remain Stage 5, with `TestLifecycleInitializesOperatorSinksBeforeMutation` reserved for that stage. |
| Run preflight before destructive mutation; discover/filter source schema and side objects deterministically. | **Covered:** `TestAdapterRunnerPreflightFailurePreventsTasksAndMutation`, `TestAdapterRunnerRunsDestructivePreflightBeforeCheckpointOrMutation`, `TestSQLiteStrictSourcePreflightPrecedesCheckpointAndTargetMutation`, and deterministic source-discovery fixtures. | Armed per-engine schema/strict capability matrix. |
| Compare the filtered schema to the latest successful deterministic snapshot and enforce policy. | **Covered; the "uncommitted primitives" note is obsolete.** `TestPrepareStage4SchemaGateSelectsLatestSuccessfulBaselineByRunOrder` and `TestPrepareStage4SchemaGateUsesLatestSuccessfulSnapshotAndPlansDrift` prove the selection and drift planning; `TestPrepareStage4SchemaGateEstablishesBaselineOnBothBackends`, `TestPrepareStage4SchemaGateWritesPlanBeforeReadsAndFailsClosed`, `TestPrepareStage4SchemaGateRejectsChangedSameRunStagedSnapshot`, and the three first-run/evolve fixtures cover the surrounding lifecycle. | Live matrix. |
| Derive effective tuning without overwriting pinned intent. | **Covered:** `TestDeterministicTuningPreservesPinnedIntent` proves requested/derived provenance and repeat resolution; `TestRuntimeTuningHistoryIsBoundedOrderedAndImmutable`, `TestStage4RuntimeTuningHistorySQLiteConformance`, and `TestAppendAttemptTerminalAuditWritesRedactedRuntimeTuningBeforeOutcome` cover bounded controller/history/audit facts. | Operator presentation and `history_retention_days` remain Stage 5. |
| Establish migration-scoped strict source epoch before partition planning or target DDL. | **Covered for PostgreSQL and SQL Server migration scope.** PostgreSQL uses exported snapshots; SQL Server uses a durable, authenticated database snapshot with cleanup/recovery authority. `TestBeginPlannedStrictConsistencyBindsWorkInsideOpenEpoch`, `TestSQLServerMigrationStrictTopologySurvivesPreOwnerAndOrdinaryResume`, and `TestCleanupCompletedStage4SQLServerMigrationSnapshotBranches` cover the durable boundary. | Armed live/process-kill composition for every admitted source/target cell. |
| Create every durable transfer task before target drop/truncate/create. | **Covered base:** `TestTaskInitializationFailurePrecedesTargetMutation`, `TestAdapterRunnerOrdersAllTableLifecycle`, and `TestAdapterRunnerRunsDestructivePreflightBeforeCheckpointOrMutation`. | S4.2: `TestStage4NetworkTasksDurableBeforeTargetMutationLive` for every target family. |
| Prepare by target mode, transfer bounded rows, and finalize supported sequences/indexes/FKs/checks. | **Covered base:** Stage 2 bounded SQLite tests and Stage 3 native-target lifecycle/common fixtures. | S4.2: repeat through the resumable range protocol in `TestStage4CertifiedRelationalTransferLifecycleLive`. |
| Run due delete reconciliation after transfer and before validation. | **Covered for admitted same-engine delete routes and SQLite incremental-delete.** `TestStage4PostgresDeleteLifecycleOrdersGlobalPhasesAndPreservesTransferredResume`, `TestStage4SQLiteIncrementalDeleteReconcilesAfterTransfer`, and `TestStage4SQLiteIncrementalDeleteReplaysDurablePlanAfterTransfer` pin order and replay. | Cross-engine delete remains an explicit refusal; the armed per-route process-kill matrix is open. |
| Validate, then atomically finalize task/run state, snapshots, and watermarks. | **Covered:** `TestStage4AggregateCompletionConformance` proves table and run completion, replay, mismatch rejection, and failure atomicity across both backends; `TestStage4AggregateReadConformance` proves a resumed process can recover byte-identical completion evidence; `TestPublishStage4RunCompletionComposesIncrementalRoute` proves the composed lifecycle publishes sentinels and the successful run in one mutation. `TestStage4AggregateCompletionRejectsStaleLease` fences it. | Live proof remains: run aggregate publication through the admitted composed route/process-kill matrix. |
| Emit truthful outcome and release strict snapshots/leases; never report success after lease, required-write, validation, or durable-completion failure. | **Covered base/partial:** `TestMigrationAttemptDisposition`, `TestFencedBackendsRejectEveryOldGenerationMutationAfterTakeover`, and Stage 1 crash-boundary tests prove SQLite behavior. | S4.2/S4.6/S4.7: `TestStage4NetworkFailureNeverReportsSuccessLive` and `TestStrictResourceCleanupOnEveryTerminalOutcomeLive`. Summary/notification/audit presentation is Stage 5. |

### 7.2 Dry-run

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Connect, run deterministic source and target preflight, discover/filter schema, report drift/policy, select pagination, and disclose tuning/provenance. | **Covered for configuration-only admission and existing endpoints.** Pure policy rejection precedes endpoint construction; existing endpoints use read-only preflight. An absent SQLite target returns a structured `proceed: false` target-preflight finding without creating it. `TestDryRunReportsComposedStage4PolicyBeforeTargetPreflight`, `TestStage4DryRunRejectsAbsentSQLiteDropRecreateWithoutArtifacts`, and `TestApplyDryRunSchemaDriftDisclosesFactsAndPolicy` pin the behavior. | Armed route matrix and target protocol-capability discovery. |
| Estimate rows/duration only when evidence exists and show delete due/candidate state. | **Covered with explicit limits.** Counts carry provenance and no unsupported duration. Scoped read-only YAML/SQLite evidence provides due state; exact candidate impact is emitted only after fully read-only source/target complete-PK scans, otherwise the plan names a limitation rather than inventing a count. `TestRunDryRunDeleteCandidatesUseReadOnlyStateAndPreserveArtifacts` and `TestRunDryRunDeleteCandidatesFailClosedForCorruptApplicableState` cover this path. | Route-specific live proof; unsupported or unprovable candidate cells stay `unavailable`. |
| Never mutate target data/schema, state progress, task success, watermarks, or deletes. | **Covered for YAML and SQLite state/SQLite targets:** `TestRunDryRunRejectsAbsentSQLiteTargetWithoutMutatingArtifacts`, `TestReadOnlySchemaSnapshotReadsSQLiteWALWithoutArtifacts`, and `TestReadOnlyScopedDeleteEvidenceReadsCommittedWALWithoutArtifacts` prove no target/state artifact creation while reading applicable authority. | `TestStage4DryRunHasZeroMutationAcrossCertifiedRoutesLive` remains part of the armed matrix. |
| AI advice is advisory and cannot replace deterministic facts. | **Stage 5 boundary.** | Stage 5 fixture: `TestAIAdviceCannotAlterDryRunFacts`. |

### 7.3 Target modes

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Rebuild rejects non-empty targets without explicit acknowledgement. | **Covered base:** `TestRunRequiresDestructiveAcknowledgementForPopulatedTarget`, target lifecycle tests for PostgreSQL/MySQL/MariaDB/SQL Server/SQLite/ClickHouse, and their live sentinels. | S4.2 regression in `TestStage4DestructiveGateSurvivesNetworkResumeLive`; resume suppression requires durable same-run evidence. |
| Durable tasks precede drop; all selected targets drop before recreate; DDL is deterministic; partial preparation names rerun recovery. | **Covered at runner/state level:** `TestTaskInitializationFailurePrecedesTargetMutation`, `TestStage4AdapterRebuildCompletesExactPreMutationCheckpointPrefix`, `TestStage4AdapterRebuildRerunsWholeFKSetAfterPartialPrepare`, `TestStage4AdapterRebuildRerunsWholeFKSetAfterOpenBeforeFirstWrite`, and native target lifecycle tests prove the checkpoint boundary and one set-wide prepare. | S4.2: an end-to-end live relational target matrix must prove the same ordering through real FK graphs and process interruption. |
| Rebuild transfers into empty tables and finalizes identity/secondary objects after data. | **Covered at runner/native lifecycle level:** `TestStage4AdapterRebuildRecoversFinalizeValidationAndAggregateFaults`, the two-table FK recovery fixtures, and Stage 3 native lifecycle tests keep publication after set-wide finalize and validation. | S4.2 process-crash fixture: `TestStage4RebuildFinalizeAfterResumedTransferLive` for each admitted target. |
| Resume may suppress backup acknowledgement only with durable proof of the same unchanged run and owned target contents. | **Partial:** `TestStage4AdapterRebuildRejectsChangedRecoveryIdentityBeforeMutation`, `TestStage4AdapterRebuildRejectsIncompleteCheckpointPrefixWithWriteAuthority`, and the rerun/replay/publication recovery fixtures prove the runner classifier. | The application/preflight gate still needs `TestRebuildResumeSuppressesAcknowledgementOnlyWithOwnedRunEvidence` and `TestRebuildResumeRechecksAcknowledgementAfterConfigOrTargetChange` against real targets. |
| Upsert requires target capability, complete source/target PKs, and existing tables unless contract evolution creates new ones. | **Covered; the "contract-authorized creation is absent" note is obsolete.** `TestPrepareStage4SchemaGateFirstUpsertEvolveAuthorizesExactCreates` proves an evolve contract authorizes exactly the intended creates, and `TestPrepareStage4SchemaGateFirstBaselineDoesNotImplicitlyAuthorizeCreates` proves a baseline does not, with `TestStage4TargetSchemaEvolutionProjectionAcceptsExplicitFirstRunCreates` on the projection side. Retained: `TestCapabilityValidationPrecedesAdapterConstruction`, `TestAdapterRunnerRejectsMissingPrimaryKeyBeforeTargetMutation`. | Live matrix. |
| Upsert inserts new rows, updates changed non-key values, retains target-only rows, and preserves existing sequence/index/FK/check objects. | **Covered base:** `TestUpsertUpdatesSourceColumnsWithoutReplacingTargetRow`, `TestAdapterRunnerUpsertAllowsTargetOnlyRows`, and Stage 3 retained-object/native upsert tests. | S4.2/S4.5: `TestStage4UpsertRetainedObjectsSurviveCrashResumeLive`; delete reconciliation is the only allowed target-only removal. |
| Upsert is idempotent under retry and complete-window replay. | **Covered at runner/native-writer level for admitted relational/SQLite targets:** `TestStage4AdapterRebuildReplaysIssuedRangeWithoutRedrop`, `TestPostgresStage4NetworkRebuildWriterSeparatesFreshAndReplay`, `TestSQLServerNativeWriterStage4RebuildSeparatesFreshAndReplay`, and the SQLite/MySQL writer suites separate fresh writes from issued replay. | **Closed 2026-08-01**: the composed process-kill/replay matrix is `TestStage4UpsertProcessKillReplayLive`, 10 cells across the admitted upsert target families and both state backends. |

### 7.4 Schema drift and contract

**Substantially corrected 2026-07-31.** The original text — "every behavior row
below remains a Stage 4 gap" — is obsolete. `internal/migrate/schema_contract.go`
implements the contract, with the per-fact decision in `schema_contract_decide.go`
and type comparison in `schema_contract_types.go`; `schema_contract_test.go`
carries 25 fixtures. None
of the proposed names below were used. Every row is covered non-live; the open
item is the per-engine live matrix.

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Compare every run/resume to the latest successful applicable filtered snapshot; omitted contract is report-only unless `fail_on_schema_drift` gates. | **Covered:** `TestSchemaContractOmittedReportsUnlessHardGate`, `TestStage4SchemaGateSuccessfulBaselineCannotBeReplacedByRetainedTargetShape`, `TestStage4SchemaGateTopologyExcludesDiscoveryAndBindsConfiguration`. | Live matrix. |
| Entity modes default correctly: scalar applies to tables/columns/data type; omitted entities under a present section default to `evolve`. | **Covered:** `TestParseExpandsScalarAndEmptySchemaContracts`. | None. |
| `evolve`: add eligible tables/nullable columns, relax nullability, widen safe types; rebuild uses current shape. | **Covered:** `TestSchemaContractEvolveSafeUpsertChanges`, `TestSchemaContractEvolveSafeTypeWideningMatrix`, `TestSchemaContractRebuildUsesCurrentShapeSeparatelyFromUpsert`. | Live matrix. |
| `freeze`: abort before transfer on entity drift. | **Covered:** `TestSchemaContractFreezeAndReportNeverProjectUpsertMutation`. | Live matrix. |
| `discard_row`: remove added or affected tables from transfer, validation, and successful snapshot. | **Covered:** `TestSchemaContractDiscardRowProjectsWholeRun`, `TestSchemaContractDiscardRowDominatesDependentObjectEvolution`. | Live matrix. |
| `discard_value`: omit eligible columns, retain prior snapshot evidence for type drift, and prune dependent indexes/FKs/checks. | **Covered:** `TestSchemaContractDiscardValuePrunesDependentObjectsAndRetainsTypeEvidence`, `TestSchemaContractDiscardValueOmitsEligibleAddedColumn`, `TestStage4SchemaGateTypeDiscardRetainsSuccessfulEvidenceWithoutRichProjection`, plus rebuild-side pruning in `TestSchemaContractRebuildDoesNotRestoreCheckAcrossDiscardedColumn`, `TestSchemaContractRebuildDoesNotRestoreInboundCompositeFKAcrossDiscard`, `TestSchemaContractRebuildPrunesDependenciesFromRestoredWholeTable`. | Live matrix. |
| `report`: emit drift without target schema mutation. | **Covered:** `TestSchemaContractFreezeAndReportNeverProjectUpsertMutation`, `TestSchemaContractReportRebuildProjectionRetainsSourceDrops`. | Live matrix. |
| Reject `tables: discard_value`; never discard PK, identity, or selected date column. | **Covered:** `TestSchemaContractRejectsTablesDiscardValueAndInvalidModes`, `TestSchemaContractRejectsProtectedColumnDiscard`. | None. |
| Block identity/PK addition in upsert, nullability tightening, narrowing/lossy conversion, coupled default/PK drift, and unrenderable operations. | **Covered:** `TestSchemaContractEvolveRejectsUnsafeUpsertChanges`, `TestSchemaContractEvolveRejectsInconsistentTypeEvidence`, `TestStage4AdapterRejectsUpsertEvolutionDecisionBeforeTargetPlanning`. | Live matrix. |
| Report dropped source objects but retain target objects; infer no destructive drop. | **Covered:** `TestSchemaContractSourceDropsRetainUpsertTarget`, `TestSchemaContractSourceColumnDropDoesNotBecomeDiscardRowOrValue`, `TestSchemaContractRebuildRetainsDropsWhileUsingCurrentNonDropShape`, `TestStage4SchemaGateRepresentsRetainedDropsAsTargetCatalogRequirement`. | Live matrix. |
| Every decision has entity, mode, kind, object, previous/current evidence, action, and reason in stable order. | **Covered:** `TestSchemaContractDecisionFactsAreCompleteStableAndInputImmutable`, `TestStage4SchemaDecisionsPublishBeforeTargetPlanning`, `TestStage4SchemaDecisionSinkFailureStopsBeforePlanningAndMutation`, `TestStage4SchemaDriftRequiresDecisionSinkBeforeTargetPlanning`. Secret safety: `TestSchemaContractErrorDoesNotExposeEvidenceValues`. | None. |
| Reject simultaneous `schema_contract` and deprecated `schema_evolution`; preserve compatible old form when supported. | **Covered:** the "conflicting schema names" case in `TestParseRejectsInvalidProductionSemantics`, `TestParseCanonicalizesDeprecatedProductionSettings`, `TestSchemaEvolutionRenamePreservesHashWireShape`. | None. |

Target-catalog evolution is no longer limited to PostgreSQL and SQLite.
PostgreSQL, MySQL/MariaDB, SQL Server, and SQLite executors are reached only
through the composed target-capability gate. Representative real-driver routes
exist for PostgreSQL, MySQL, and SQL Server; SQLite's WAL/resume route is
covered by `TestStage4AdapterSQLiteTargetEvolutionComposedWAL` and
`TestStage4AdapterSQLiteTargetEvolutionRelaxResumeWAL`. This is not a claim of
the full source/target/type/object matrix; unsupported catalog authority remains
a pre-mutation refusal.

**First live contract enforcement landed 2026-07-31.**
`TestStage4SchemaContractFreezeStopsLiveDrift` establishes a real baseline
snapshot on a PostgreSQL source, adds a column to the live source, and requires
the next run under `freeze` to refuse before any target write. It asserts the
*reason*, and the live error names the contract, the mode, the exact drifted
column, and the timing: "schema contract drift_blocked: freeze column_added for
...network_items.note: freeze mode rejects drift before transfer". That is the
first proof that a contract mode enforces against a real database rather than
only in projection.

`TestStage4SchemaContractModesLive` extends that to a mode matrix against the
same real drift, so a mode that silently behaved like another fails rather than
blends in: **evolve** adds the column to the target, **report** and
**discard_value** leave the target shape untouched, **freeze** aborts before any
write.

Two things the live matrix surfaced that projection tests did not:

- `tables: discard_value` is rejected by configuration policy, so a contract
  applying `discard_value` to every entity is refused outright. The mode belongs
  on the column entities only. This is documented behaviour, but it is easy to
  write a fixture that trips it and misread the refusal as a defect.
- **`report` mode leaves the run failing at the write layer.** It deliberately
  does not act on drift, so an added source column has nowhere to go and the
  transfer stops with `permanent transfer error: requested column note is not
  present in schema`. The contract guarantee holds — the target schema is not
  mutated — but the operator sees a low-level column error rather than a
  contract-level message about unhandled drift. Worth improving; it is a
  reporting wart, not a correctness bug.

SQLite now applies compatible `relax_nullability` and safe `widen_type` through
a pinned, transaction-atomic copy/swap. It authenticates retained rows and
`sqlite_sequence`, rejects incoming-FK/trigger/collision shapes it cannot
faithfully preserve, and verifies commit-before-ack recovery. See
`TestSQLiteTargetEvolutionCopySwapPreservesRetainedRowsAndAuthority`,
`TestSQLiteTargetEvolutionCopySwapRejectsIncomingForeignKeysBeforeMutation`,
and `TestSQLiteTargetEvolutionCopySwapCommitAckRecoveryAuthenticatesRetainedAuthority`.
PostgreSQL, MySQL/MariaDB, and SQL Server retain their own catalog-plan and
commit-classification gates; `TestStage4AdapterMySQLSchemaEvolutionComposedRouteLiveTLS`
and `TestStage4AdapterSQLServerSchemaEvolutionComposedRouteLiveTLS` are
representative gated routes. The full type/object and process-kill matrix is
still open.

The obstacles below cost
several drafts to find and are what the setup pattern in those fixtures solves —
read them before extending it:

- SQLite copy/swap is deliberately limited to the proved safe decisions and
  catalog shapes above; it is not a generic SQLite DDL rewriter.
- Source and target must be different databases. The same endpoint is refused
  outright, and separate schemas within one database still resolve to a single
  endpoint.
- **The baseline run must be published as successful.** The contract compares
  against the latest *successful* snapshot, and `Execute` does not append the
  success record — that is the application layer's job. A baseline left Running
  is not an authority, and the second run then fails in target-schema-evolution
  projection with "full target authority does not contain the exact
  source-backed prior table". Use `completeStage4IncrementalTestRun`. This was
  the single blocking obstacle.
- Whatever the fixture asserts, assert the *reason*. A contract test that
  accepts any error is worthless — that is how the SQLite-target version above
  looked like it worked.

## Section 8 — Transfer semantics and safety

### 8.1 Table eligibility and ordering

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Missing PK fails clearly before mutation; filters use documented globs and deterministic source order. | **Covered base:** `TestAdapterRunnerRejectsMissingPrimaryKeyBeforeTargetMutation`, `TestSQLiteToSQLiteRejectsTableWithoutPrimaryKeyBeforeTargetMutation`, `TestSelectTablesUsesDeterministicSourceOrder`, `TestParseRejectsInvalidTableGlob`. | S4.2 route regression: `TestStage4MissingPKAndFilterMatrixLive`. |
| Read/write source column order after contract projection. | **Partial:** Stage 3 common fixtures preserve source order; contract projection is absent. | S4.3: `TestSchemaContractProjectionPreservesSourceColumnOrder`. |
| Convert source-derived values without logging row content. | **Covered base/partial:** native writer normalization and redaction tests, including `TestPostgresNativeWriterNormalizesBeforeConnectionWithoutLeakingRows` and SQL Server/MySQL equivalents. | S4.2/S4.7: `TestStage4ConversionFailuresNeverLeakRowValues`. |

### 8.2 Pagination selection

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Report the selected strategy per table. | **Covered:** planning stability by `TestPlanSQLitePaginationSelectsTupleAndStableTopology`; operator-visible disclosure by `PlannedTable.Pagination` in the dry-run plan, proven by `TestStage4DryRunDisclosesTuningAndDeletePolicy`. | Non-SQLite sources omit the field; extend with each engine dry-run path. |
| Integer keyset uses exact bounds/order and covers signed 64-bit extremes. | **Covered base for SQLite:** `TestSplitIntegerRangeCoversSignedExtremesWithoutOverlap`, `TestSQLiteKeysetHandlesSignedIntegerExtremes`. | S4.2: `TestStage4IntegerKeysetSourceMatrixLive`. |
| Tuple keyset is admitted only when bind/null/type/conversion/collation order equals `ORDER BY`; typed watermarks preserve values above `2^53`. | **Partial for SQLite/state:** `TestKeyValueRoundTripAboveTwoToTheFiftyThird`, `TestPlanSQLitePaginationSelectsTupleAndStableTopology`, `TestRangeBackendConformance`. | S4.2: `TestPostgresTupleKeysetOrderingLive`, `TestSQLServerTupleKeysetOrderingLive`, `TestMySQLTupleKeysetCollationLive`, `TestMariaDBTupleKeysetCollationLive`. |
| Unsafe text collation, nullable tuple component, unsigned value, converter-touched key, and date/time key fall back to ROW_NUMBER unless equivalence is proven. | **Covered base only for representative SQLite unsafe tuples:** `TestSQLiteUnsafeTupleFallsBackToRowNumber`. | `TestStage4UnsafeTupleFallbackMatrix` plus engine live collation sentinels. |
| ROW_NUMBER uses deterministic complete-PK order and resumes exact intervals. | **Covered base for SQLite:** `TestSplitRowNumberRangeCoversExactlyOnce`, `TestSQLiteRowNumberFallbackPagesUnsafePrimaryKeys`, `TestStage1SQLiteHardKillDuringRowNumberCheckpointResumesExactRows`. | `TestStage4RowNumberSourceMatrixLive`. |
| Large-table ranges cover exactly once; changed partition/range/ROW_NUMBER topology invalidates stale progress. | **Covered base:** `TestSQLiteTransferExecutesExactPlannedRanges`, `TestTableCheckpointObserverResetsChangedTopology`, `TestRangeBackendConformance`. | S4.2 network proof: `TestStage4NetworkTopologyChangeInvalidatesProgressLive`. |
| Strict partition jobs share one stable source view or serialize safely. | **Missing.** | S4.6: `TestStrictParallelRangesShareOneSourceViewLive`. |

### 8.3 Bounded pipeline

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Reading and writing overlap while all queues, workers, memory, and connections remain bounded. | **Covered base for the SQLite pipeline:** transfer pipeline and byte-budget tests exercise concurrent stages. | S4.2: `TestStage4NetworkPipelineOverlapsWithinAllBudgetsLive`. |
| One migration-wide byte budget accounts for scanned retained row size across concurrent tables. | **Covered base for SQLite:** `TestSQLitePipelineHonorsByteBudgetForWideRows`, `TestSQLiteWideRowsReserveBeforeConcurrentScan`, `TestByteBudgetExactUsagePeakAndOversizePolicy`. | S4.2: `TestStage4NetworkWideTableJobsShareMemoryBudgetLive`. |
| Cancellation/writer failure releases cursors, reservations, connections, and workers. | **Covered base:** `TestByteBudgetCancellationReleasesAndUnblocks`, `TestSQLitePipelineRepeatedCancellationDoesNotLeakResources`, `TestSQLitePipelineReleasesReservationsOnObserverFailure`. | S4.2: `TestStage4NetworkWriterFailureReleasesAllResourcesLive`. |
| Queue/workers obey memory and connection budgets. | **Covered base:** resource-plan clamp tests. | `TestStage4NetworkPipelineHonorsEffectiveConcurrencyLive`. |
| Heap-pressure backstop avoids collection storms and applies reductions only at safe boundaries. | **Covered base:** `TestHeapPressureBackstopCollectsOnceAndReducesAtChunkBoundary`, `TestHeapPressureBackstopDoesNotReduceWhenCollectionRelievesPressure`, and `TestHeapPressureBackstopCancellationWhileCollectionInFlight`. | Integrate in network pipeline: `TestStage4NetworkHeapBackstopAtChunkBoundary`. |
| Runtime tuning changes occur only at safe boundaries. | **Partial:** heap reduction is covered; general runtime adjustment state is absent. | `TestRuntimeTuningAdjustmentAppliesOnlyAtChunkBoundary`. |
| MySQL packet and PostgreSQL COPY transport constraints safely cap chunks below requests. | **Missing.** | `TestMySQLPacketLimitCapsChunkLive`, `TestPostgresCopyTransportLimitCapsChunkLive`. |

### 8.4 Bulk write strictness

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| PostgreSQL COPY and SQL Server bulk chunks roll back on failure; SQL Server preserves NULL. | **Covered base:** `TestPostgresNativeWriterUpsertFailuresRollBackBeforeCommit`, `TestSQLServerNativeWriterTypesBinaryNullWithoutMutatingInput`, and `TestSQLServerNativeWriterRollbackFailureDiscardsConnection`, plus Stage 3 live writers. | S4.2 fault-injected live: `TestPostgresChunkRollbackLive`, `TestSQLServerChunkRollbackAndNullLive`. |
| MySQL local infile proves counts/warnings, fails on lossy outcomes, and falls back visibly once when unavailable. | **Covered base/live:** `TestMySQLNativeWriterRejectsLossyLocalInfileResult`, `TestMySQLNativeWriterFallsBackOnceWhenLocalInfileDisabled`, and Stage 3 MySQL/MariaDB bulk sentinels. | Keep as mandatory Stage 4 regression under normal and race. |
| Non-transactional committed-prefix errors return the exact prefix and retry after it. | **Covered primitive:** `TestContiguousAckTrackerCommittedPrefixAndSuffixRetry`. | S4.2 integration: `TestCommittedPrefixWriterResumesAfterExactPrefixLive`. |
| SQLite obeys bind ceiling and one-writer rule; ClickHouse uses bounded native batches. | **Covered base:** SQLite batching/pipeline tests and `TestClickHouseNativeWriterDurablySendsBoundedBatch`. | Keep as Stage 4 regression; ClickHouse remains rebuild-only. |

### 8.5 Writer acknowledgement and checkpoint frontier

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Advance only after durable target acknowledgement and in logical source order. | **Covered base:** `TestContiguousAckTrackerHoldsOutOfOrderReceipt`, `TestRangeBackendConformance`, `TestAdapterRunnerRejectsNonDurableReceipt`. | S4.2: `TestStage4NetworkOutOfOrderWriteCheckpointLive`. |
| Periodic/final checkpoints expose only the lowest contiguous safe frontier. | **Covered base:** range backend/observer conformance and crash tests. | `TestStage4NetworkPeriodicAndFinalFrontierLive`. |
| Persist each range's exact bounds, completion, and typed watermark. | **Covered base:** `TestRangeBackendConformance`. | Network composition in `TestStage4NetworkRangeRestoreLive`. |
| Legacy single watermark resumes only through a compatible single-reader path. | **Covered base:** `TestNotifySQLiteTransferPlansSkipsLegacySingleWatermarkProgress`, `TestValidateSQLiteLegacyProgressRequiresMatchingUnambiguousFrontier`. | S4.2/state upgrade: `TestNetworkLegacyWatermarkNeverChangesTopology`. |

### 8.6 Retry behavior

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Retry recognized transient network/server/lock errors with bounded exponential backoff, cancellation, and three default retries. | **Covered.** The claim that engine-specific classifiers are absent is obsolete: `internal/migrate/engine_retry.go` implements `classifyPostgresRetry`, `classifyMySQLRetry` (also serving MariaDB), `classifySQLServerRetry`, `classifySQLiteRetry`, and `classifyClickHouseRetry`, wired into the transfer core, mostly in `network_transfer_restore.go` with one site in `network_transfer_core.go` (both were one file until it was split for size). `TestClassifyEngineRetryMatrix` covers all six engines by SQLSTATE or server code, alongside `TestClassifyEngineRetryRequiresSafeReplayBoundary`, `TestClassifyEngineRetryTransportBoundaries`, `TestClassifyEngineRetryUnknownCommitRejectsAllTransportEvidence`, `TestClassifyEngineRetryStructuralServerErrorBeatsJoinedTransport`, `TestClassifyEngineRetryPreservesExplicitClassAndCancellation`, `TestClassifyEngineRetryRejectsUnknownInputsAndIsDeterministic`, `TestWrapEngineRetryErrorComposesWithBoundedRetry`, plus the generic budget/backoff primitives. The six proposed per-engine fixture names were never used. | Live per-engine fault injection belongs in the S4.9 matrix. |
| Never blindly retry conversion, DDL policy, PK, schema contract, validation, lease, or state failures. | **Covered generic primitive:** `TestRetryStopsForStableNonTransientClasses`. | S4.2/S4.3/S4.7 route composition: `TestStage4StableFailureClassesNeverRetryLive`. |
| Possibly committed rebuild replay is insert-only and duplicate-safe by complete PK; it never updates an existing row. | **Covered at native-writer level for every admitted relational target:** PostgreSQL stages/COPYs then uses PK-scoped `ON CONFLICT DO NOTHING`; MySQL and MariaDB use guarded duplicate-key no-op assignment; SQL Server stages then runs insert-only `MERGE`; SQLite uses conflict-ignore. `TestPostgresStage4RebuildFreshReplayAndConflictsLiveTLS`, `TestMySQLStage4RebuildFreshReplayAndConflictsLiveTLS`, `TestMariaDBStage4RebuildFreshReplayAndConflictsLiveTLS`, and `TestSQLServerStage4RebuildCompositePKReplayAndConflictsLiveTLS` exercise lost-ack replay, no update, fresh conflict, and secondary-UNIQUE failure on live engines. Unit tests additionally pin generated SQL and admission-before-connection. ClickHouse advertises no such capability and is rejected. | Still open: end-to-end bounded-runner/process-crash routes for PostgreSQL, MySQL/MariaDB, and SQL Server targets proving the native writer is selected from durable issued evidence and the original run completes truthfully. |

## Section 9 — Incremental sync and delete convergence

**Current status.** Date-based incremental upsert is admitted through the
relational/SQLite capability matrix, with full mode still refused. The exact
attempt key scope—not a later live whole-source query—governs final deep
validation and resume. **Closed 2026-08-01** by `TestStage4IncrementalCertifiedRouteMatrixLiveTLS` and `TestStage4IncrementalNetworkProcessKillResumeLive`.

### 9.1 Date-based incremental upsert

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Select the first compatible configured date column; no candidate means full-table upsert. | **Covered:** `TestBuildIncrementalTablePlanSelectsFirstCompatibleAndCompletePKOrder`, `TestBuildIncrementalTablePlanWithoutCandidateUsesFullPKUpsert`, `TestBuildIncrementalTablePlanFailsClosedOnKeyAndCatalogShapes`. | None at unit level. |
| Baseline transfers all rows, captures maximum non-NULL timestamp, and persists it only with durable table success. | **Covered:** `TestAdapterIncrementalBaselineAndEmptyWindowQueries`, `TestAdapterIncrementalBaselineOrderingMatrix`, `TestStage4IncrementalCompletionIsAtomicAcrossBackendReopen`. | None at unit level. |
| Later runs use strict `timestamp > watermark`; equality is skipped. | **Covered:** `TestAdapterIncrementalReadQueryShapes`, `TestAdapterIncrementalWindowRejectsContradictoryEmptyShapes`. | None. |
| Persist one immutable upper fence at attempt start; watermark cannot cross it. | **Covered:** `TestStage4IncrementalWindowPublishesExactFenceAndPreservesZeroRowFrontier`, `TestStage4AdapterIncrementalPersistsFenceBeforeTargetMutationAndPublishesAggregate`, `TestAdapterIncrementalFenceQueriesAndNormalization`. | None. |
| Resume reuses the original fence and never samples a replacement. | **Covered:** `TestStage4OneIncrementalAttemptPerRunTaskUnderConcurrency` and the fence-reuse path in `TestStage4AdapterIncrementalPersistsFenceBeforeTargetMutationAndPublishesAggregate`. | None. |
| Resume discards positional progress and replays the whole changed window from the lower watermark. | **Covered:** `TestAdapterIncrementalReadRejectsPositionalOrImpreciseResume` proves positional resume is rejected by construction. | None. |
| Watermark and aggregate table success are atomic/equivalent. | **Covered:** `TestStage4IncrementalCompletionIsAtomicAcrossBackendReopen` plus the `Incremental` arm of `TestStage4AggregateCompletionConformance`. | None. |

Deep incremental validation is inclusive for `count_only`, `null_parity`, and
`sample`: per-batch canonical proof is supplemented by a final stable target
view over the exact transferred keys. Source mutations after the upper fence do
not redefine the attempt. `TestStage4IncrementalComposedCrossEngineLive`,
`TestStage4AdapterIncrementalFinalEvidencePinsLaterTargetKeyBatches`, and
`TestStage4IncrementalValidationUsesExactCompositeKeysAcrossDriverIntegers`
are representative evidence. The full armed source/target/process-kill matrix
and target protocol-capability discovery remain open.

### 9.2 Delete reconciliation

**Scope caution.** Delete reconciliation is intentionally narrower than
ordinary upsert. It is admitted only for same-engine PostgreSQL, MySQL 8,
MariaDB 10.11, SQL Server, and SQLite keyed-upsert routes. SQLite-to-SQLite is
also the sole date-incremental-plus-delete composition. Cross-engine delete,
strict-plus-delete outside its separately admitted same-engine path, and every
unproved key-equality/collation cell remain pre-mutation refusals.

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Default `off` preserves target-only rows. | **Covered:** `TestAdapterRunnerUpsertAllowsTargetOnlyRows`, `TestStage4PostgresDeleteCompositionAdmissionIsExact`, and `TestStage4DeleteCapabilityGateKeepsUnimplementedCellsClosed`. | Armed route matrix. |
| Reconcile is upsert-only, interval-scheduled from durable last success, and requires stable PK. | **Covered:** `TestDeleteReconcileTaskAdmissionIsExact`, `TestDeleteReconcileRequiresExplicitSafeEqualityProof`, `TestStage4PostgresDeleteAttemptIDBindsOnlyStableWorkIdentity`, and `TestStage4DeleteCompletionAndLatestSuccessAreFailClosed`. | Cross-engine cells stay refused; admitted cells need armed process-kill proof. |
| Compare key sets and hard-delete target-only keys in bounded parameter-safe batches. | **Covered:** `TestDeleteReconcileBatchByteCeiling`, `TestDeleteReconcileLargeKeySetUsesBoundedSpoolBatches`, `TestDeleteReconcileDiskSpoolAndBoundedBatches`, `TestStage4PostgresDeleteBatchByteLimitIsBounded`, `TestDeleteReconcileDuplicateKeysFailBeforeDelete`. | Route matrix. |
| Run after transfer/before validation and persist candidates, deleted rows, skips, reasons, completion. | **Covered:** `TestStage4PostgresDeleteLifecycleOrdersGlobalPhasesAndPreservesTransferredResume`, `TestStage4SQLiteIncrementalDeleteReconcilesAfterTransfer`, and `TestDeleteReconcileCandidatePlanAndBatchIntentPrecedeMutation`. | Armed route matrix. |
| Distinguish not-due from ran-with-zero; incomplete work cannot advance last success. | **Covered:** `TestDeleteReconcileIncompleteNeverAdvancesLastSuccess`, `TestStage4PostgresDeleteLifecyclePropagatesCompletedAndNotDueStrictness`. | Route matrix. |
| Dry-run reports due/candidate impact without deletion. | **Covered when authority is available:** `TestRunDryRunDeleteCandidatesUseReadOnlyStateAndPreserveArtifacts`, `TestRunDryRunDeleteCandidatesFailClosedForCorruptApplicableState`, and `TestDryRunDeleteCandidateSQLiteCapabilityUsesProductionAuthority` prove read-only due/candidate handling. | Candidate count remains unavailable, with a named limitation, whenever a full read-only key scan cannot prove it. |
| Completed full-scope reconciliation makes count validation strict; off/not-due permits upsert target supersets. | **Covered:** `TestAdapterResumeStrictReconciliationRejectsTargetSuperset`, `TestStage4PostgresDeleteTerminalStrictnessIsAuthenticated`. | Route matrix. |
| Crash safety: target delete and receipt survive a state-commit failure and replay exactly once. | **Covered (added since the original revision):** `TestDeleteReconcileCrashAfterTargetCommitReplaysReceipt`, `TestDeleteReconcileCrashAfterStateCommitUsesDurableFrontier`, `TestDeleteReconcileTargetErrorReceiptSurvivesStateCommitFailure`, `TestDeleteReconcileTerminalReplayCleansCrashLeftoverSpool`. | Route matrix. |
| Spool and plan evidence cannot be tampered between plan and mutation. | **Covered (not in the original revision):** `TestDeleteReconcilePostPlanTamperFailsBeforeIntentOrMutation`, `TestDeleteReconcileSpoolTamperFailsBeforeReplayMutation`, `TestDeleteSpoolReadSnapshotPreventsCandidateTOCTOU`, `TestDeleteReconcileMalformedLoadedEvidenceFailsClosed`. | Route matrix. |

Representative live proof exists for PostgreSQL and SQL Server, while MySQL and
MariaDB have TLS journal/snapshot fixtures. `TestStage4PostgresDeleteCompositionLiveTLS`,
`TestStage4PostgresDeleteCompositionCrashResumeLiveTLS`, and
`TestStage4SQLServerDeleteCompositionLiveTLS` do not close the full admitted
same-engine matrix. The armed process-kill/replay matrix remains required.

## Section 10 — Strict consistency

**Implemented, capability-gated composition.** All strict routes require
`upsert`, a certified relational/SQLite target writer and validation path, and
preflight before checkpoint or target mutation.

| Source/scope | Stable-view contract and current target matrix |
|---|---|
| PostgreSQL table and migration | Exported snapshot; PostgreSQL, MySQL/MariaDB, SQL Server, or SQLite target. |
| SQL Server table and migration | Lock-bound table view or durable database snapshot; PostgreSQL, MySQL/MariaDB, SQL Server, or SQLite target. |
| MySQL/MariaDB table | InnoDB/real `LOCK TABLES` preflight and retained table view; PostgreSQL, MySQL/MariaDB, SQL Server, or SQLite target. |
| SQLite table | One pinned transaction/one source reader; PostgreSQL, MySQL/MariaDB, SQL Server, or SQLite target. |
| MySQL/MariaDB migration, SQLite migration, ClickHouse | Refused before mutation. |

`TestStage4PostgresStrictCrossTargetLiveTLS`,
`TestSQLServerMigrationStrictComposedPostgresLiveTLS`,
`TestSQLServerMigrationStrictComposedSQLiteLiveTLS`,
`TestStage4MySQLFamilyStrictComposedPostgresLiveTLS`, and
`TestStage4SQLiteStrictComposedPostgresLiveTLS` are representative composed
evidence. SQL Server lifecycle tests additionally cover snapshot identity,
cleanup intent, and resume. They do not substitute for the armed complete
source/scope/target process-kill and concurrent-writer matrix.

## Section 11 — Durable state, ownership, and resume

### 11.1 Backend capability contract

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Full local and YAML backends share restartability behavior. | **Covered:** `TestBackendConformance`, `TestRangeBackendConformance`, `TestRangeAttemptBackendConformance`, and `TestStage4BackendConformance` — the proposed extension now exists. | None. |
| Persist run/tasks/ranges plus incremental watermarks/fences, delete results, schema snapshots, strict evidence, event counters, config hash, and lease metadata. | **Covered:** `TestStage4BackendConformance` plus `TestStage4AggregateCompletionConformance`, `TestStage4AggregateReadConformance`, `TestStage4CanonicalSpatialMetadataRoundTrip`, `TestStage4CanonicalTypeMetadataRoundTrip`, `TestStage4ReusableEvidenceUsesBackendIndependentTotalOrder`. | Event counters have no named fixture; confirm whether they are Stage 5. |
| A pre-mutation table inventory may be replanned, and is fixed once any table publishes terminal evidence. | **Covered (requirement added 2026-07-31):** `TestStage4AggregateInventoryRevision` proves replan before terminal evidence, refusal after it, and immutable schema authority across a revision. See the note below on why the window exists. | Route matrix should exercise a replanned resume live. |
| YAML writes complete temporary state, flush/replace atomically, and serialize compare/write across processes. | **Covered base:** `TestYAMLStoreWritesPrivateCompleteDocument`, `TestYAMLStoreSerializesConcurrentProcesses`, `TestYAMLReplacementIsValidAcrossMidReplacementHardKills`. | **Covered:** `TestStage4YAMLAtomicReplacementCrashMatrix` hard-kills a writer on both sides of the replacement against a document carrying run, ordinary task, structured work and ranges, schema sentinel and snapshot, and a table inventory, then proves every record still decodes and every receipt still matches its digest. |
| Full backend auto-migrates private schema forward without credentials. | **Covered base for earlier history:** `TestLegacyStateUpgradePreservesCompletedHistory`. | `TestStage4StateUpgradePreservesNewEvidence`, `TestStage4StateUpgradeRejectsAmbiguousIncompleteEvidence`. Encrypted profiles/tuning history are Stage 5 data; release-wide upgrade matrix is Stage 6. |

**Why the inventory revision window exists.** The durable table inventory pins
the exact range identities that a table completion is validated against. A
resumed run legitimately replans — a source that grew during an outage yields a
different partition count — so an inventory frozen at first publication would
make that run *unrecoverable* rather than merely failed. The window is narrow by
construction: revision requires zero aggregate receipts and zero completed
ordinary tables, the schema authority is immutable across it, and it closes
permanently at the first terminal table evidence.

### 11.2 Run and task model

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Run stores IDs/times/status/resumability/phase, source/target identity, sanitized config/hash, origin/error, and lease key/token/generation. | **Partial, advanced 2026-07-31.** `TestStage4RunRecordRoundTripAndRedaction` proves every stored identity and lease field survives a backend reopen on both backends, and that `status` and `history` never print the lease owner token while still emitting non-secret identity; `TestStage4PublicRunKeepsEveryNonSecretField` pins the redaction to exactly one field. `Run` still lacks a phase field and structured origin/error. | Add phase and origin/error, or record them as Stage 5. |
| Tasks use structured type/schema/table/partition identity and store state, attempts/retries/times/scrubbed error. | **Covered base in `WorkTask`; legacy table `Task` remains scalar.** | `TestStage4UsesStructuredTaskIdentityEverywhere`. |
| Human task keys cannot collide for punctuation/quoted identifiers. | **Covered:** `TestTaskKeyCanonicalizationHasNoDelimiterCollisions` now exists. | None. |
| Progress stores table/range, rows done/total, safe typed watermark, range envelope, and timestamp. | **Covered base:** `TestRangeBackendConformance`. | Extend for Stage 4 timestamp/fence evidence in `TestStage4ProgressEnvelopeRoundTrip`. |

### 11.3 Required-write rule

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Task exists durably before destructive mutation. | **Covered base:** `TestTaskInitializationFailurePrecedesTargetMutation`. | Network live regression in S4.2. |
| Unresolved periodic/final/task/watermark/run-completion write failure prevents success with state exit 6. | **Covered:** `TestStage4EveryRequiredWriteFailureReturnsStateExitSix` drives each required write to failure and proves it classifies as a state failure and maps to exit 6; `TestStage4RequiredWriteFailureOutranksTransferClassification` pins the precedence so a durable write failure is never reported as a transfer error. Retained context: `TestAdapterRunnerReturnsOnlyCompletedProgressWhenLaterCheckpointFails`, `TestSQLitePartialResultKeepsRowsAfterAggregateCheckpointFailure`, `TestMigrationAttemptDisposition`. | The run-completion write is covered by `publishStage4RunSuccess` returning a state-classified error; a live route proof belongs in the S4.9 matrix. |
| Unknown task writes reject. | **Covered base:** `TestSQLiteStoreRequiresKnownRunningTaskForCompletion`, range backend unknown-work checks. | Add every new Stage 4 write to `TestStage4UnknownTaskWritesReject`. |
| Periodic save may degrade only if final safe frontier supersedes it, with audit evidence. | **Covered for the composed network transfer plan.** Explicit `checkpoint_frequency` controls contiguous acknowledgement cadence; `TestResumableNetworkTransferCheckpointFrequencyPersistsContiguousFrontier`, `TestResumableNetworkTransferCheckpointFrequencyStateFailureReplaysIssuedWork`, and `TestResumableNetworkTransferCheckpointFrequencyResumesFromPeriodicFrontier` pin persistence and replay. | It remains a pre-mutation refusal for incremental, strict, and delete routes until their distinct durable protocols can consume it. |
| State failure after target commit directs repair-and-resume, not competing fresh run. | **Partial:** errors remain resumable, but remedy contract is absent. | `TestPostCommitStateFailureNamesRepairAndResume`. |

### 11.4 Exclusive target lease and fencing

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Fresh, resume, and abandon use canonical target lease. | **Covered base:** lease identity tests and Stage 2 lifecycle CLI tests. | `TestStage4NetworkRunResumeAbandonShareCanonicalLeaseLive`. |
| Random token, monotonic generation, atomic acquisition/takeover; live second owner fails and force cannot bypass. | **Covered:** `TestSQLiteStoreRejectsSecondLiveTargetLease`, `TestReleasedLeaseRetainsMonotonicFencingGeneration`, force-resume tests, and process-level `TestTargetLeaseTwoProcessRace`. | `TestForceResumeCannotBypassLiveNetworkLease` remains missing (network route). |
| Heartbeat renewal fences every run/task/progress/completion mutation; stale owner is cancelled and cannot succeed. | **Covered base:** `TestFencedBackendsRejectEveryOldGenerationMutationAfterTakeover`, `TestTargetMutationFenceSerializesTakeoverAndRejectsOldOwner`. | `TestStage4OldNetworkOwnerCancelledAfterTakeoverLive`. |
| Takeover only after TTL; legacy fresh un-fenced run rejects. | **Partial:** TTL takeover/generation is covered; explicit legacy-live rejection needs proof. | `TestLegacyFreshUnfencedRunRejectsTakeover`. |
| Different canonical targets run concurrently. | **Covered:** `TestDifferentCanonicalTargetsRunConcurrently` runs two processes against distinct canonical targets and requires both to succeed. | None. |

### 11.5 Outcome versus resumability

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Interrupted/cancelled/ordinary partial remain resumable; accepted partial exits 0 and is terminal; abandonment preserves truthful outcome semantics. | **Covered base:** `TestMigrationAttemptDisposition`, `TestOutcomeAndAbandonmentConformance`, `TestStage2ResumeAbandonLifecycle`. | Extend to Stage 4 network and reconciliation/strict failures in `TestStage4OutcomeResumabilityMatrixLive`. |

### 11.6 Resume protocol

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Select newest resumable target run and reject superseded success. | **Covered base:** outcome conformance and `TestStage2ResumeAndAbandonRejectSuccessInsertedAfterPreselection`. | Network composition in S4.2. |
| Acquire/bind lease, reject live owner, verify heartbeat/staleness. | **Covered base:** lease/fence tests. | Two-process network live fixture. |
| Compare data-plane hash; force permits only compatible policy override. | **Covered base:** `TestResumeRejectsChangedDataPlaneConfiguration`, `TestForceResumeAcceptsPersistedStructuralCompatibility`, `TestForceResumeRejectsStructuralChangeBeforeTargetMutation`, `TestResumeCompatibilityHashSeparatesSafeRuntimeAndStructuralChanges`. | Add Stage 4 fields and rename compatibility: `TestStage4ResumeHashClassifiesEveryNewField`. |
| Re-run preflight/discovery/filters/drift/tuning after lease; reuse run ID and reactivate. | **Covered base for SQLite:** `TestStage2ResumeRereadsConfigEvidenceAfterTargetLease`, `TestStage2ResumeReactivatesRunBeforeAuditAndMigration`. | `TestStage4NetworkResumeRechecksEnvironmentAndReusesRunLive`. |
| Skip completed table only with aggregate checkpoint and target-count agreement. | **Covered base:** `TestSQLiteCompletedCheckpointSkipsOnlyAfterExactAgreement`, `TestResumeReusesValidatedCompletedTable`. | Network route matrix. |
| Restore exact incomplete topology or invalidate safely; cleanup obeys mode/pagination. | **Covered base:** range/reset and legacy ambiguity tests. | Network route matrix. |
| Incremental resume replays full lower-watermark window. | **Covered:** `TestAdapterIncrementalReadRejectsPositionalOrImpreciseResume` proves positional resume is rejected by construction, with `TestExecuteIncrementalResumeAcceptsPriorNilWatermarkFrontier` and `TestExecuteIncrementalResumeRejectsUnexpectedAttemptOrEvidence` covering the frontier and evidence rules. | Live matrix. |
| Possibly committed rebuild uses insert-only replay. | **Covered at runner/state and native-writer levels:** `TestStage4AdapterRebuildReplaysIssuedRangeWithoutRedrop` runs against YAML and SQLite state; the four live native-writer fixtures prove each admitted target's exact replay semantics. | S4.2 end-to-end process-crash route fixture for every admitted relational target. |
| Resume finalizes, validates, and completes the original rebuild run without premature table publication. | **Covered non-live across both state backends:** `TestStage4AdapterRebuildRecoversFinalizeValidationAndAggregateFaults`, `TestStage4AdapterRebuildPublicationRecoveryReusesCommittedReceipt`, `TestStage4AdapterRebuildRecognizesCommittedReadyReceiptAfterWriteError`, and the pre-mutation prefix suite distinguish rerun, duplicate-safe replay, and publication-only recovery. | `TestStage4NetworkCrashResumeCompletesOriginalRunLive` (or equivalent per-target process-kill matrix) remains required. Delete reconciliation and incremental composition remain separate upsert-only rows. |
| Hash excludes policy-only/derived fields and preserves deprecated rename wire shape. | **Covered base:** resume-hash tests. | `TestStage4ResumeHashPolicyAndDeprecatedWireShape`. |

Network resume is no longer globally rejected: bounded relational rebuild now
has explicit rerun/replay/publication recovery. Unsupported engine/mode cells
must still reject before state mutation, and the admitted cells still need the
end-to-end live/process-crash matrix above.

## Section 12 — Validation

**Substantially corrected 2026-07-31.** The original text — "current validation
is SQLite count-only" — is obsolete. Mode inclusion, timeout and estimate
policy, reconciliation-aware count policy, strict-snapshot counts, NULL parity,
deterministic sampling, and canonical value comparison are all implemented, with
roughly 50 fixtures in `validation_core_test.go`, `validation_values_test.go`,
and `adapter_validation_database_test.go`. None of the proposed fixture names
below were used; the implementations chose different names.

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Mode inclusion: default/count, NULL parity, sample; explicit rejection of unimplemented `full`. | **Covered:** `TestBuildValidationPlanIsInclusiveAndRejectsFull`, and config-level rejection in `TestParseRejectsInvalidProductionSemantics`. | None. |
| Exact per-table count with timeout; estimate only after timeout. | **Covered:** `TestValidationCoreExactTimeoutAndEstimatePolicy`, `TestValidationCoreDoesNotEstimateAfterNonTimeoutFailure`, `TestValidationCoreRecognizesDriverErrorAfterExactDeadline`. | None. |
| Exact timeout fails by default even when estimate matches; estimate mismatch fails; explicit log-only policy is honored. | **Covered:** `TestValidationCoreExactTimeoutAndEstimatePolicy`, `TestValidationCoreDeepTimeoutsHonorLogOnlyPolicy`. | None. |
| Rebuild requires equality; upsert permits superset only when reconciliation is not strict; strict snapshot uses persisted count. | **Covered:** `TestValidationCoreCountTargetPolicies`, `TestValidationCoreUsesPerTableReconciliationStrictness`, `TestValidationCoreStrictSnapshotCountIsAuthoritative`, `TestStage4AdapterValidationSpecsRequireStrictSnapshotCount`. | None. |
| Bound deep-validation table concurrency/time and return all findings in stable table order. | **Covered:** `TestValidationCoreBoundsDeepTableTime`, `TestValidationCoreFindingsHaveStableTableOrder`, `TestValidationCoreRejectsUnboundedInvocation`. | None. |
| Sample selects deterministically by complete PK. | **Covered:** `TestValidationCoreSampleUsesCompletePKAndCanonicalValues`, `TestValidationSampleDescriptorRequiresCompletePrimaryKeyAndProjection`, `TestValidationCoreSampleRejectsNullablePrimaryKeyBeforeDeepProbes`, `TestCompareValidationPrimaryKeyValuesUsesTypedCompositeOrder`, `TestValidationCoreRejectsNonIncreasingSourcePrimaryKeysBeforeTarget`. | None. |
| Canonical values are typed and length-delimited; equal integer widths/times compare correctly; NULL/text/bytes cannot collide; timestamps retain represented precision. | **Covered by twelve fixtures:** `TestCanonicalValidationRowKeepsSemanticTypesCollisionFree`, `TestCanonicalValidationRowLengthFramesCannotCollide`, `TestCanonicalValidationRowRejectsTextBinaryConfusion`, `TestCanonicalValidationRowPreservesLargeIntegersWithoutFloat`, `TestCanonicalValidationRowNormalizesFloatSpecialValues`, `TestCanonicalValidationFloatUsesOneInjectiveDomain`, `TestCanonicalValidationDecimalRejectsNonSQLRationalSyntax`, `TestCanonicalValidationDateAndTimeRejectDiscardedComponents`, `TestCanonicalValidationUUIDRequiresCanonicalHyphenPositions`, `TestCanonicalValidationRowNormalizesDriverShapesBySemanticType`, `TestCanonicalValidationRowRejectsUnexpectedShapeWithoutValue`, `TestCanonicalValidationSQLiteANYPreservesRuntimeStorageClass`. | None. |
| NULL parity detects systematic conversion loss. | **Covered:** `TestValidationCoreNullParityDetectsSystematicConversion`, plus the upsert-scope guards `TestValidationCoreUpsertNullParityUsesSourceOwnedTargetScope`, `TestValidationCoreUpsertNullParityRequiresRouteEqualityProof`, `TestValidationCoreUpsertNullParityRejectsUnsafePrimaryKey`, `TestValidationCoreUpsertNullParityRejectsNullPrimaryKeyEvidence`, `TestValidationCoreUpsertNullParityRejectsMismatchedProofEcho`. | Live per-engine matrix. |
| Findings remain structured/deterministic on failure and never leak row values. | **Covered:** `TestValidationCoreFindingsHaveStableTableOrder`, `TestValidationCoreSampleMismatchFactsDoNotLeakValues`, `TestValidationCoreFailsClosedOnIncompleteEvidence`, `TestValidationCoreRejectsNullCountsAboveAuthoritativeRows`. | None. |
| AI hypotheses cannot change deterministic results. | **Stage 5 boundary:** `TestAIValidationTriageCannotAlterEvidence`. | Stage 5. |

The database-backed probe now supports PostgreSQL, SQL Server, MySQL/MariaDB,
and SQLite relational endpoints, including cross-driver count, NULL-parity,
and complete-PK sample paths. Cross-engine equality is certified only for the
typed canonical domains and exact key/collation proof that the probe can
establish; unsafe text/collation or unmappable types refuse. `full` remains
explicitly rejected.

`TestSQLServerToPostgresDatabaseValidationProbeLiveTLS`,
`TestSQLiteToMySQLDatabaseValidationProbeLiveTLS`, and
`TestMySQLToSQLServerDatabaseValidationProbeLiveTLS` are gated real-driver
proofs; `TestSQLiteDatabaseValidationProbeDeepSemantics` and
`TestDatabaseValidationProbeFailsClosed` cover the bounded/unit paths. The
remaining gap is the armed all-pairs/type/collation/timeouts matrix, not a
PostgreSQL-only production gate. ClickHouse retains only modes it can safely
admit.

## Section 13 — Preflight and operational safety

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Stable findings include severity, dotted check name, side, message, remedy. | **Covered:** `TestComposeProductionPreflightReportIsStructurallyOrdered`, `TestComposeProductionPreflightReportRequiresCompleteManifest`, `TestComposeProductionPreflightReportRequiresApplicableEngineFacts`, `TestComposeProductionPreflightReportBlocksUnknownWriteAuthority`, `TestBuildProductionPreflightManifestIsStableAndConditional`, `TestBuildProductionPreflightManifestSelectsUpsertChecks`. | Live per-engine matrix below. |
| Error aborts before mutation unless exact check/prefix/all is explicitly skipped; skip downgrades visibly without erasing evidence. | **Covered:** `TestEvaluatePreflightAppliesVisibleExactPrefixAndAllSkips`, `TestComposeProductionPreflightReportPreservesVisibleExactSkip`, `TestEvaluatePreflightOrdersFindings`, `TestEvaluatePreflightIsRaceSafeAndDoesNotMutateInputs`, `TestEvaluatePreflightEmptyEvidenceIsNonBlocking`. The three proposed names do not exist; these supersede them. | Live per-engine matrix below. |
| Cover connection/auth, version, source read/target privileges, schema existence/access, encoding, pool headroom, strict prerequisites, disk estimate, destructive gate, and engine capability probes. | **Partial:** route-specific connection/catalog/destructive checks and strict source preflight exist; `TestSQLiteStrictSourcePreflightPrecedesCheckpointAndTargetMutation` and the strict capability/admission suites prove no checkpoint/write before a known prerequisite. | Unified structured pool/disk/protocol-capability inventory and armed engine matrix. |
| Documentation separately states exhaustive minimum privileges. | **Stage 5 boundary for operator documentation; deterministic privilege facts are Stage 4.** | Stage 4: `TestPreflightPrivilegeFactsMatchAdapterRequirements`; Stage 5 reviews the published privilege tables. |
| Signal cancels work, stops new chunks, attempts final checkpoint within configurable timeout, and exits cancelled; hard kill resumes. | **Covered base/partial:** SIGTERM, cancellation, resource-release, and SQLite hard-kill tests exist. Configurable final-checkpoint timeout and network crash recovery do not. | `TestSignalStopsNewChunksAndBoundsFinalCheckpoint`, plus S4.9 process-kill matrix. |

## Acceptance 21.5 — Transfer and retry correctness

| Acceptance item | Evidence or required fixture |
|---|---|
| Integer keyset covers negatives, gaps, signed extremes, fresh/resume bounds, parallel ranges. | **Partial:** integer split/SQLite tests. Missing `TestStage4IntegerKeysetSourceMatrixLive`. |
| Tuple keyset covers composites, `>2^53`, eligible text collation, NULL rejection, typed restore, work stealing. | **Partial:** typed state/SQLite. Missing the four source tuple live fixtures and `TestTupleKeysetWorkStealingRestoresExactTopology`. |
| Unsafe tuples route to ROW_NUMBER. | **Partial:** SQLite only. Missing `TestStage4UnsafeTupleFallbackMatrix`. |
| ROW_NUMBER partitions cover exact deterministic PK order. | **Partial:** SQLite only. Missing `TestStage4RowNumberSourceMatrixLive`. |
| Out-of-order completion cannot overtake unacknowledged sequence. | **Covered base:** ack tracker/range conformance. Missing network live `TestStage4NetworkOutOfOrderWriteCheckpointLive`. |
| Writer failure releases readers/memory/connections with no leaks. | **Covered base:** SQLite/resource tests. Missing `TestStage4NetworkWriterFailureReleasesAllResourcesLive`. |
| PostgreSQL/SQL Server rollback; committed-prefix writer resumes after prefix. | **Partial:** writer unit tests and generic prefix tracker. Missing three fault-injected live fixtures from Section 8.4. |
| MySQL row loss/conversion warning fails. | **Covered base/live:** Stage 3 MySQL/MariaDB bulk sentinels; retain in Stage 4 gate. |
| Write-before-checkpoint replay neither duplicates nor overwrites. | **Live-proven for the SQLite and PostgreSQL targets**, verified 2026-07-31. `TestStage4PostgresTLSToSQLiteNetworkCrashResumeLive` injects a durable-acknowledgement failure mid-transfer, resumes, and compares the whole ordered row set with `reflect.DeepEqual` — which proves no duplicate rows *and* no overwritten values, not merely a matching count. It now runs on both state backends. `TestStage4PostgresTLSToSQLiteNetworkInteriorInsertReplayLive` covers the interior-insert case, and `TestStage4PostgresDeleteCompositionCrashResumeLiveTLS` covers a PostgreSQL target. **Nothing remains.** An earlier draft of this row called MySQL, MariaDB, and SQL Server targets a gap; that was wrong. Only `postgresTargetAdapter` and `sqliteTargetAdapter` implement `stage4NetworkIdempotentUpsertTarget`, the route-owned marker asserting the target's upsert path is safe to replay after a durable issued-page record. Every other target is refused as policy — "Stage 4 target engine %q has no certified idempotent network upsert path". Those engines are not untested members of the contract; they are deliberately outside it, so the certified target set is PostgreSQL and SQLite and both are live-proven. |
| Concurrent wide tables remain within budget. | **SQLite only.** Missing `TestStage4NetworkWideTableJobsShareMemoryBudgetLive`. |

## Acceptance 21.6 — State, lease, and resume

| Acceptance item | Evidence or required fixture |
|---|---|
| Full/YAML backends share restartability conformance. | **Covered base;** extend with `TestStage4BackendConformance`. |
| YAML replacement survives crash/concurrent writers. | **Covered:** `TestStage4YAMLAtomicReplacementCrashMatrix`. |
| Unknown/required-write failures prevent success. | **Covered:** `TestStage4EveryRequiredWriteFailureReturnsStateExitSix`. |
| Task creation fails before mutation. | **Covered base;** network live regression required. |
| Periodic failure may be superseded only by audited final save. | **Missing:** two proposed periodic-checkpoint fixtures. |
| Two processes racing one target produce one owner. | **Covered:** `TestTargetLeaseTwoProcessRace` holds the lease and proves a second real process cannot take an owned target, then that the target frees on release. |
| Old generation cannot mutate or report success after takeover. | **Covered base:** fenced backend/target mutation tests; network live fixture required. |
| Different targets run concurrently. | **Covered:** `TestDifferentCanonicalTargetsRunConcurrently`. |
| Config drift/force rules hold. | **Covered base;** extend for every Stage 4 field. |
| Completed skip requires target-count agreement. | **Covered base SQLite;** network matrix required. |
| Topology change clears stale progress. | **Covered base;** network matrix required. |
| Outcome/resumability matrix is exact. | **Covered base;** Stage 4 network semantics required. |
| Upgrades preserve history and reject ambiguous incomplete identities. | **Covered base:** legacy tests; expand for Stage 4 evidence and structured keys. Release-wide upgrade coverage remains Stage 6. |

## Acceptance 21.7 — Incremental and delete behavior

The durable-fence, baseline, resume, and empty-window contracts are covered by
the `TestExecuteIncremental*`, `TestStage4IncrementalCompletionIsAtomicAcrossBackendReopen`,
and `TestStage4AdapterIncremental*` families. Same-engine delete receipts,
replay, scheduling, and SQLite incremental-delete are covered by the
`TestDeleteReconcile*`, `TestStage4*Delete*`, and
`TestStage4SQLiteIncrementalDelete*` families. What remains is the armed
source/target/process-kill matrix and every cross-engine delete cell, which is
currently refused rather than claimed.

## Acceptance 21.8 — Strict consistency

The supported source/scope/target matrix is implemented as listed in Section
10. Unit, fault, and representative TLS tests cover PostgreSQL, SQL Server,
MySQL/MariaDB, and SQLite stable views; SQL Server migration additionally
covers durable snapshot recovery. Remaining acceptance work is the armed
concurrent-writer/process-kill matrix for every admitted target cell. MySQL/
MariaDB and SQLite migration, ClickHouse, and any missing writer/validator cell
must remain pre-mutation refusals.

## Acceptance 21.9 — Schema contract and validation

**Current status.** The contract/evolution and validation primitives are
covered non-live, with representative PostgreSQL, MySQL, SQL Server, SQLite,
and cross-driver TLS fixtures. See Sections 7.4 and 12 for capability limits;
**closed 2026-08-01** by `TestStage4ValidationRouteMatrixLive` and `TestStage4SchemaContractTargetMatrixLive`.

- Schema add/drop/evolution/freeze/report/discard behavior: **covered** by the
  25 fixtures in `schema_contract_test.go`.
- Protected discarded columns and dependent-object pruning: **covered** by
  `TestSchemaContractRejectsProtectedColumnDiscard`,
  `TestSchemaContractDiscardValuePrunesDependentObjectsAndRetainsTypeEvidence`,
  and the three rebuild-side pruning fixtures.
- JSON/audit evidence: **covered** by
  `TestSchemaContractDecisionFactsAreCompleteStableAndInputImmutable` and the
  three `TestStage4SchemaDecision*` publication fixtures; audit presentation
  remains Stage 5.
- Count timeout/mismatch policy: **covered** by
  `TestValidationCoreExactTimeoutAndEstimatePolicy`,
  `TestValidationCoreDeepTimeoutsHonorLogOnlyPolicy`,
  `TestValidationCoreDoesNotEstimateAfterNonTimeoutFailure`.
- Upsert/reconciliation count policy: **covered** by
  `TestValidationCoreCountTargetPolicies`,
  `TestValidationCoreUsesPerTableReconciliationStrictness`.
- NULL parity: **covered** by `TestValidationCoreNullParityDetectsSystematicConversion`
  and cross-driver live probes; remaining cells require the armed matrix.
- Canonical samples: **covered** by the `TestCanonicalValidation*` fixtures and
  the representative cross-driver live probes; remaining cells require the
  armed matrix.
- Explicit `full` rejection: **covered** by
  `TestBuildValidationPlanIsInclusiveAndRejectsFull` and the config-level
  "reserved full validation" case.

Open: the armed composed target/type/object evolution matrix, the deep
validation all-pairs/type/collation/timeout matrix, and their process-kill
routes.

## Mandatory Stage 4 gates

No slice is complete until its focused tests pass in normal and race modes.
The final Stage 4 gate must include all of the following:

1. `go test ./... -count=1`
2. `go test -race ./... -count=1`
3. `go vet ./...`, command builds, cross-compilation checks already required
   by the repository, and `git diff --check`
4. `TestStage4LiveMatrixEnvironmentRequired`, which must fail—not skip—when
   the exit-gate flag is enabled and any pinned TLS endpoint is missing.
   **Implemented 2026-07-31.** Arm it with `DMTX_STAGE4_LIVE_REQUIRED=1`; it
   then fails naming every absent variable. `TestStage4LiveMatrixEnvironment`
   `CoversEveryPinnedEndpoint` additionally proves the Stage 4 endpoint list
   never narrows below Stage 3's, so the gate cannot silently shrink.
   **Run the exit gate with this armed** — every live fixture in the repository
   skips on an unset DSN, so an unarmed run against a half-provisioned
   environment reports success while proving almost nothing.
5. TLS live matrices for PostgreSQL 16, SQL Server 2022, Oracle MySQL 8.0,
   MariaDB 10.11, ClickHouse 24.8, and SQLite local routes
6. `TestStage4CertifiedRelationalIncrementalRouteMatrixLive`
7. `TestStage4CertifiedRelationalDeleteRouteMatrixLive`
8. `TestStage4SchemaContractTargetMatrixLive`
9. `TestStage4ValidationRouteMatrixLive`
10. Every supported strict-consistency fixture from Section 10
11. `TestStage4CertifiedRelationalCrashResumeMatrixLive`, run against both
    SQLite and YAML state backends, with named subtests for all certified
    relational routes
12. The same live and process-crash matrices under `go test -race`

Live tests must use verified TLS where the adapter supports certificate
verification and must never silently pass through `t.Skip` in an exit-gate
run. Fault fixtures must verify target rows, durable state, lease generation,
watermarks/fences, snapshot/delete evidence, and truthful final outcome after
resume—not merely a zero process exit.

## Remaining work to declare Stage 4 complete

Refreshed 2026-08-01. This is the closure list after the bounded composition
slices above. It records proof still required, not formerly absent production
paths. In particular, absent SQLite dry-run targets now fail structured
preflight without artifacts, and the listed transfer settings are no longer
inert.

### A. Route matrices — CLOSED 2026-08-01

**Status.** Every named matrix below is now satisfied, several by tests that
already existed under different names — the recurring hazard in this document.
The mapping is: the incremental matrix is
`TestStage4IncrementalCertifiedRouteMatrixLiveTLS` (canonical 4x4) plus
`TestStage4IncrementalMariaDBFamilyAliasLiveTLS`; the delete matrix is
`TestStage4CertifiedRelationalDeleteRouteMatrixLive` for admission and
`TestStage4SameEngineDeleteProcessKillReplayLive` for the armed process-kill
half; validation is `TestStage4ValidationRouteMatrixLive`; schema contract is
`TestStage4SchemaContractTargetMatrixLive`. The per-route crash/resume matrix is
covered by four armed kill matrices — rebuild, delete, upsert, and strict — each
across both durable state backends, plus
`TestStage4IncrementalNetworkProcessKillResumeLive` and
`TestStage4IncrementalSQLiteProcessCrashResume` for incremental. The prose below
records the assessment that led here and is kept for provenance.

Every certified-cell implementation needs its family. Missing, by name:

- `TestStage4CertifiedRelationalIncrementalRouteMatrixLive` — **open as an
  armed real-route matrix** for the now-admitted relational/SQLite capability
  cells, including post-fence source mutation and completed-window resume.
- `TestStage4CertifiedRelationalDeleteRouteMatrixLive` — **open as an armed
  same-engine process-kill/replay matrix**. It must retain cross-engine delete
  refusal rather than promote it by accident.
- `TestStage4SchemaContractTargetMatrixLive`
- `TestStage4ValidationRouteMatrixLive` — **open as an armed relational/SQLite
  all-pairs type/collation/timeout matrix.** Cross-driver probe support is
  implemented and representative TLS cases exist; it must not be confused with
  a complete target protocol-capability proof.
- `TestStage4CertifiedRelationalCrashResumeMatrixLive` — the per-route matrix is
  still open, but the **both state backends** half is now satisfied for the
  network route: `TestStage4PostgresTLSToSQLiteNetworkCrashResumeLive` runs the
  same crash and resume against SQLite and YAML as subtests, and
  `TestStage4PostgresDeleteCompositionCrashResumeLiveTLS` covers the delete
  route on YAML. Running one route on one backend had left backend-specific
  crash bugs invisible, and the two differ genuinely: SQLite commits a
  transaction, YAML replaces a whole document.
- `TestStage4CertifiedRelationalRebuildCrashResumeMatrixLive` — **open.** The
  bounded runner now admits keyed PostgreSQL, MySQL/MariaDB, SQL Server, and
  SQLite targets; native live tests prove strict fresh writes and duplicate-safe
  replay for each target family, and YAML/SQLite state fault tests prove the
  rerun/replay/publication classifier. The missing gate must interrupt the real
  composed route at prepare, write-before-checkpoint, finalize, validation, and
  publication boundaries for each target, then verify exact FK schema/rows and
  truthful original-run completion after resume.
- ~~`TestStage4CertifiedRelationalTransferLifecycleLive`~~ — **satisfied
  2026-07-31** by the per-engine stable-runner sentinels rather than a new
  fixture: `TestStage4MySQLStableRunnerLiveTLS`,
  `TestStage4MariaDBStableRunnerLiveTLS`,
  `TestStage4SQLServerStableRunnerLiveTLS`, and the PostgreSQL network
  lifecycle tests. These previously asserted only the returned `Result`; they
  now also assert the durable work task and range are terminal with the exact
  row count, which is what "through the resumable range protocol" requires. A
  route that transferred correctly but left durable evidence unfinished would
  formerly have passed.
- `TestStage4CertifiedRelationalUpsertReplayMatrixLive` — **open.** Native
  replay is implemented for admitted PostgreSQL, MySQL/MariaDB, SQL Server, and
  SQLite targets, but each composed target needs the armed interruption/replay
  proof before its capability is treated as an exit-gate result.

### B. Deep validation (Section 12) — CLOSED 2026-08-01

**Status.** `TestStage4ValidationRouteMatrixLive` is the armed all-pairs
matrix across PostgreSQL, SQL Server, MySQL, and SQLite, with typed integer
keys, NULL text, binary bytes, and timestamps so canonical row representation is
exercised rather than only SQL-shape helpers. Collation and timeout are covered
by `TestStage4ValidationRouteMatrixRefusesUnprovenCrossEngineTextKeyBeforeMutation`
and `TestStage4ValidationRouteMatrixTimeoutPolicyLiveTLS`, and the MariaDB
flavor by `TestStage4ValidationMariaDBFlavorLive`. `full` remains refused.

The relational/SQLite database probe and incremental evidence path implement
count, NULL parity, and deterministic samples with typed canonical values and
complete-PK authority; `full` is still refused. The remaining work is the
armed all-pairs/type/collation/timeout and process-kill matrix, not an
engine-family production refusal. See the representative real-driver probes in
Section 12.

### C. Strict consistency — CLOSED 2026-08-01

**Status.** Process-kill is `TestStage4StrictProcessKillResumeMatrixLive`, 14
cells: PostgreSQL table/migration, SQL Server table/migration, MySQL table,
MariaDB table, and SQLite table, each against YAML and SQLite state.
Concurrent-writer isolation is proven live by mutation backends that write to
the source during the epoch in `adapter_stage4_postgres_strict_live_test.go`,
`strict_consistency_postgres_live_test.go`, and
`strict_consistency_mysql_live_test.go`, with `TestSQLServerStrictTableLockLive`
and `TestSQLServerMigrationSnapshotLiveTLS` for SQL Server. Unsupported scopes
and ClickHouse remain pre-mutation refusals.

Section 10's PostgreSQL and SQL Server table/migration paths plus MySQL/MariaDB
and SQLite table paths are composed to admitted relational/SQLite targets.
Unsupported migration scopes, ClickHouse, and absent writer/validator cells
remain pre-mutation refusals. The open work is armed concurrent-writer,
process-kill, recovery, and target-matrix proof—not composition admission.

### D. Schema-contract modes and evolution — CLOSED 2026-08-01

**Status.** `TestStage4SchemaContractTargetMatrixLive` covers all five target
families with both a native exact-catalog and a composed-upsert half, proving
create/add/relax/widen and target-only retention. Collision and foreign-key
fail-closed behaviour is covered by
`TestSQLiteTargetEvolutionCopySwapRejectsIncomingForeignKeysBeforeMutation`,
`TestSQLiteTargetEvolutionCopySwapRejectsConcurrentTemporaryNameCollision`,
`TestMySQLTargetEvolutionCreatePlannerRejectsAuthorityCollisions`, and
`TestSQLServerSQLitePreflightRejectsObjectCollisionWithoutMutation`.

PostgreSQL, MySQL/MariaDB, SQL Server, and SQLite target executors are
capability-gated. SQLite now has the intentionally narrow safe copy/swap path,
including WAL/recovery authority; it is no longer a blanket `evolve` refusal.
The remaining matrix is type/object/collision/FK/protocol behavior and real
process-kill composition, not the absence of a SQLite executor.

### E. Dry-run (Section 7.2) — CLOSED 2026-08-01

**Status.** The armed live route proof is
`TestStage4DryRunRouteMatrixLive` across PostgreSQL, MySQL, MariaDB, and SQL
Server, asserting exact counts, live target preflight, and zero mutation read
from the target's own contents rather than from the absence of an error.
`TestStage4DryRunRefusesUncertifiedRouteBeforeReachingEndpointsLive` pins the
configuration-only refusal, which arrives as a structured non-proceed with a nil
error — a test asserting only on the error would pass against a dry run that
admitted the route.

Configuration rejection occurs before endpoint construction. Existing endpoints
and state are inspected read-only; absent SQLite targets fail structured target
preflight without creating a file. Schema drift emits scoped baseline/status/
policy/action facts, and delete candidates are exact only when a read-only key
scan proves them. The remaining work is protocol-capability and armed live
route coverage; unprovable candidate impact stays explicitly unavailable.

### F. Small named gaps — CLOSED 2026-08-01

- ~~`TestDeterministicTuningPreservesPinnedIntent`~~ — **closed 2026-07-31**
- Dry-run candidate impact is implemented only with fully read-only key
  authority; unsupported or unprovable cells deliberately report `unavailable`.
- ~~`TestClickHouseIncrementalRejectedBeforeMutationLive`~~ and
  ~~`TestClickHouseDeleteReconcileRejectedBeforeMutationLive`~~ — **closed
  2026-07-31**, against a live ClickHouse endpoint. Both refusals arrive one
  layer earlier than the fixture names imply: ClickHouse is a rebuild-only
  target and does not support upsert at all, which both incremental and delete
  require, so those routes are unreachable by construction rather than gated by
  a feature-specific check. The tests assert that real reason rather than a
  hoped-for one, and also assert the table lifecycle was never entered.
- ~~`TestTargetLeaseTwoProcessRace`, `TestDifferentCanonicalTargetsRunConcurrently`~~ — **closed 2026-07-31**
- ~~`TestStage4EveryRequiredWriteFailureReturnsStateExitSix`~~ — **closed 2026-07-31**
- `checkpoint_frequency` is covered for composed network transfer; it remains
  intentionally unavailable to strict, incremental, and delete protocols.
- ~~engine retry classifiers (Section 8.6)~~ — **already covered**; verified 2026-07-31, all six engines in `TestClassifyEngineRetryMatrix`
- ~~`TestStage4LiveMatrixEnvironmentRequired`~~ — **closed 2026-07-31**; arm with
  `DMTX_STAGE4_LIVE_REQUIRED=1`, and it fails naming every absent endpoint

### F2. Configuration consumer audit — refreshed 2026-08-01

| Setting | Current bounded consumer | Deliberate boundary |
| --- | --- | --- |
| `checkpoint_frequency` | Composed network transfer checkpoints a contiguous acknowledged frontier at the requested cadence. | Incremental, strict, and delete refuse it before endpoints because their evidence protocols do not consume it. |
| `upsert_merge_size` | Composed native PostgreSQL/MySQL/MariaDB/SQL Server/SQLite writers split writes at the minimum requested, resource, and native limit. | Missing native capability or legacy routing refuses before work. |
| `large_table_threshold` | A retained table-stable exact count selects and binds deferred range topology before mutation. | Strict, incremental, delete, legacy SQLite-to-SQLite, and routes without size authority refuse it. |
| `runtime_tuning_interval` | The bounded controller changes only at safe write boundaries; decision history is fenced/persisted where the state backend supports it and appears in terminal audit. | Prebound compatibility waves and unsupported routes refuse before mutation. |
| `history_retention_days` | No Phase Four retention/deletion consumer. | Stage 5 owns retention policy and historical presentation. |

Key evidence: `TestStage4CheckpointFrequencyProvenanceAndBound`,
`TestStage4UpsertMergeAdmissionIntersectsNativeAndResourceCaps`,
`TestStage4LargeTableThresholdUsesRetainedSQLiteSizeAndBindsResumeTopology`,
`TestStage4DeferredRuntimeTuningPersistsFencedSQLiteHistoryBeforePrepare`, and
`TestAppendAttemptTerminalAuditWritesRedactedRuntimeTuningBeforeOutcome`.

### G. The live gate — CLOSED 2026-08-01

The armed exit gate was run on the current tree with all five TLS engines, both
target databases, and the admin DSNs provisioned:

```text
DMTX_STAGE4_LIVE_REQUIRED=1 go test ./... -count=1          all packages ok
DMTX_STAGE4_LIVE_REQUIRED=1 go test -race ./... -count=1    all packages ok
go vet ./...                                                clean
git diff --check                                            clean
```

`DMTX_STAGE4_LIVE_REQUIRED=1` is the part that matters. Without it a missing
endpoint makes a live test skip and the suite still prints ok, which is how
dozens of gaps stayed hidden earlier in this work.

## Stage 4 status: complete

All of A through G, including F2, are closed, each against named tests and the
armed commands above. Local `go test ./...` passing remains necessary and
nowhere near sufficient; the armed gate is the claim.

One boundary is deliberate rather than closed: `history_retention_days` has no
Phase Four consumer because the stage boundary assigns retention to Stage 5. It
is recorded in F2 as a boundary, not as missing work.

Two known non-goals are also recorded rather than hidden. Cross-engine delete
reconciliation stays refused, and strict consistency remains admitted only to
the engines listed in C. Both are enforced by pre-mutation refusals with their
own tests, so neither can be widened by accident.

## Stage 5 and Stage 6 boundary

Stage 4 owns deterministic data-plane facts and safety decisions. It does not
claim completion of:

- Stage 5 CLI/JSON stability, TUI/WebUI, metrics/traces dashboards,
  notification delivery, audit presentation, encrypted profile UX, or AI
  advice;
- Stage 6 packaged artifacts, checksums, cross-platform release CI, upgrade
  support windows, performance qualification, or release documentation.

Stage 4 must nevertheless expose stable internal structured facts so Stage 5
can present them without re-deciding correctness. State schema migrations
needed for Stage 4 evidence are implemented and tested in Stage 4; the broader
historical upgrade/release matrix remains Stage 6.

The Stage 4 deliverable in Section 20 also names spatial/type metadata and
deterministic tuning. Those contracts cross-reference Sections 5, 6, and 14,
which are outside this document's requested Sections 7–13 audit. They still
require separate closeout fixtures before Stage 4 can be declared complete.
All three now exist: `TestStage4CanonicalSpatialMetadataRoundTrip` and
`TestStage4SpatialMetadataRouteMatrixLive`, joined by
`TestStage4SpatialMetadataRouteMatrixFailsClosedBeforeMutation`,
`TestStage4SpatialValidationUsesExactBinaryRepresentation`, and
`TestStage4CanonicalTypeMetadataRoundTrip`.
`TestDeterministicTuningPreservesPinnedIntent` was added 2026-07-31 and closes
the deterministic tuning contract.

## Highest-risk gaps

1. **The armed live exit gate is not yet a completion result.** Run the
   composed route, race, and process-kill/resume matrices with
   `DMTX_STAGE4_LIVE_REQUIRED=1`; an unarmed skip is not evidence.
2. **Target protocol capability discovery remains uneven.** Every admitted
   relational/SQLite writer, catalog executor, deep probe, and strict route
   still needs its real type/collation/object/protocol matrix, including
   concurrency and commit-ack ambiguity.
3. **Delete remains intentionally non-cross-engine.** Only the documented
   same-engine cells and SQLite incremental-delete are admitted. Do not infer a
   cross-engine delete contract from ordinary cross-engine upsert.
4. **Deferred strict cells must stay refused.** MySQL/MariaDB and SQLite
   migration scope, ClickHouse strict, unsupported target capability cells, and
   any missing native writer/validator have no safe fallback.
5. **Process-boundary truthfulness remains the sharpest operational risk.**
   The remaining matrix must prove target rows, state, leases, fences, strict
   snapshots, cleanup receipts, and final outcome across real interruption—not
   just a clean return from a unit fixture.
6. **`history_retention_days` is a Stage 5 boundary.** Phase Four records
   bounded runtime history and audit facts but does not claim retention-policy
   behavior or historical presentation.
