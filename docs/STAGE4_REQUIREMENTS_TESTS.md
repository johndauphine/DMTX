# Stage 4 requirements and test map

This document maps the Stage 4 work in `RECREATE_DMT.md` Sections 7 through 13
and acceptance criteria 21.5 through 21.9 to current repository evidence and
explicit missing fixtures.

Stage 4 is **not complete**, but it is substantially implemented. Existing
Stage 1 through Stage 3 tests are regression evidence only unless a row below
says that they satisfy the complete requirement. A proposed fixture name
identifies work that does not yet have sufficient automated proof.

**Refreshed 2026-07-31** against branch `codex/stage-4-production-semantics`.
The original revision of this document was written when Stage 4 had not
started; roughly 450 Stage 4 tests now exist that it never named, and several
rows below that read "Missing" were closed by differently-named fixtures. Where
this refresh cites a fixture, that fixture exists in the tree today. Where a row
still says missing, it was verified missing by name against the current test
inventory.

Two cautions on reading the refreshed rows. First, an implemented route is
usually **one certified cell, not a family**: PostgreSQL-to-PostgreSQL upsert is
proven where the row matrix asks for six engines. Second, non-live proof does
not close a row whose acceptance requires TLS live evidence; the live matrix
has not been rerun since 2026-07-31 because the approval quota is exhausted
until 2026-08-06.

Where a row cites the Stage 3 route or common-fixture matrix, the exact current
fixture names are the ones enumerated in `STAGE3_REQUIREMENTS_TESTS.md`; this
map does not rename or weaken those baseline gates.

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
Status as of 2026-07-31: S4.1 **landed**; S4.2 **landed for certified cells**;
S4.3 **landed for PostgreSQL**; S4.4 **landed**; S4.5 **landed for one
certified route**; S4.6 **landed for PostgreSQL only**; S4.7 **partial**;
S4.8 **partial**; S4.9 **blocked on the live matrix rerun**.

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
| Run preflight before destructive mutation; discover/filter source schema and side objects deterministically. | **Covered base:** `TestAdapterRunnerPreflightFailurePreventsTasksAndMutation`, `TestAdapterRunnerRunsDestructivePreflightBeforeCheckpointOrMutation`, `TestSelectTablesUsesDeterministicSourceOrder`, and Stage 3 source-discovery/live fixtures. | S4.8: `TestStage4StructuredPreflightPrecedesAllTargetMutation`; include schema-contract and strict prerequisites. |
| Compare the filtered schema to the latest successful deterministic snapshot and enforce policy. | **Missing as composed behavior.** Uncommitted S4.1 configuration/state primitives are in progress, but they do not yet enforce a contract in the lifecycle. | S4.1/S4.3: `TestFreshRunSelectsLatestApplicableSchemaSnapshot` and `TestFreshRunEnforcesSchemaContractBeforeMutation`. |
| Derive effective tuning without overwriting pinned intent. | **Covered:** `TestDeterministicTuningPreservesPinnedIntent` proves requested/derived provenance per field, pinned values surviving derivation unchanged, downward-only labelled clamping, and repeat-resolution determinism; `TestResolveEffectiveTransferPlanUserCeilingCanOnlyLowerDetectedLimit` covers the memory ceiling. Configuration rejects unsatisfiable pins outright rather than clamping them, which is stronger than this row required. | Long-lived tuning history and advisory presentation are Stage 5. |
| Establish migration-scoped strict source epoch before partition planning or target DDL. | **Missing:** supported strict modes are currently rejected. | S4.6: `TestStrictMigrationEpochPrecedesPlanningAndTargetDDL` plus source-specific live fixtures. |
| Create every durable transfer task before target drop/truncate/create. | **Covered base:** `TestTaskInitializationFailurePrecedesTargetMutation`, `TestAdapterRunnerOrdersAllTableLifecycle`, and `TestAdapterRunnerRunsDestructivePreflightBeforeCheckpointOrMutation`. | S4.2: `TestStage4NetworkTasksDurableBeforeTargetMutationLive` for every target family. |
| Prepare by target mode, transfer bounded rows, and finalize supported sequences/indexes/FKs/checks. | **Covered base:** Stage 2 bounded SQLite tests and Stage 3 native-target lifecycle/common fixtures. | S4.2: repeat through the resumable range protocol in `TestStage4CertifiedRelationalTransferLifecycleLive`. |
| Run due delete reconciliation after transfer and before validation. | **Missing.** | S4.5: `TestLifecycleRunsDueReconciliationBetweenTransferAndValidation`. |
| Validate, then atomically finalize task/run state, snapshots, and watermarks. | **Covered:** `TestStage4AggregateCompletionConformance` proves table and run completion, replay, mismatch rejection, and failure atomicity across both backends; `TestStage4AggregateReadConformance` proves a resumed process can recover byte-identical completion evidence; `TestPublishStage4RunCompletionComposesIncrementalRoute` proves the composed lifecycle publishes sentinels and the successful run in one mutation. `TestStage4AggregateCompletionRejectsStaleLease` fences it. | Live proof only: the composed publication has no live-route coverage, and the application's published-true branch is reachable only through a PostgreSQL route. Add to the S4.9 matrix. |
| Emit truthful outcome and release strict snapshots/leases; never report success after lease, required-write, validation, or durable-completion failure. | **Covered base/partial:** `TestMigrationAttemptDisposition`, `TestFencedBackendsRejectEveryOldGenerationMutationAfterTakeover`, and Stage 1 crash-boundary tests prove SQLite behavior. | S4.2/S4.6/S4.7: `TestStage4NetworkFailureNeverReportsSuccessLive` and `TestStrictResourceCleanupOnEveryTerminalOutcomeLive`. Summary/notification/audit presentation is Stage 5. |

### 7.2 Dry-run

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Connect, run deterministic source and target preflight, discover/filter schema, report drift/policy, select pagination, and disclose tuning/provenance. | **Partial:** `TestRunDryRunDiscoversSQLiteWithoutMutatingTargetOrState`; `DryRun` currently avoids the target, state, lease, and audit and reports only source row counts. | S4.8: `TestStage4DryRunReportsTargetPreflightDriftPaginationAndTuning` and `TestStage4DryRunNetworkRouteMatrixLive`. |
| Estimate rows/duration only when evidence exists and show delete due/candidate state. | **Missing.** | S4.5/S4.8: `TestDryRunLabelsEstimateProvenance` and `TestDeleteReconcileDryRunReportsDueCandidates`. |
| Never mutate target data/schema, state progress, task success, watermarks, or deletes. | **Covered base for SQLite only:** `TestRunDryRunDiscoversSQLiteWithoutMutatingTargetOrState`. | S4.8: `TestStage4DryRunHasZeroMutationAcrossCertifiedRoutesLive`. |
| AI advice is advisory and cannot replace deterministic facts. | **Stage 5 boundary.** | Stage 5 fixture: `TestAIAdviceCannotAlterDryRunFacts`. |

### 7.3 Target modes

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Rebuild rejects non-empty targets without explicit acknowledgement. | **Covered base:** `TestRunRequiresDestructiveAcknowledgementForPopulatedTarget`, target lifecycle tests for PostgreSQL/MySQL/MariaDB/SQL Server/SQLite/ClickHouse, and their live sentinels. | S4.2 regression in `TestStage4DestructiveGateSurvivesNetworkResumeLive`; resume suppression requires durable same-run evidence. |
| Durable tasks precede drop; all selected targets drop before recreate; DDL is deterministic; partial preparation names rerun recovery. | **Covered base:** `TestTaskInitializationFailurePrecedesTargetMutation`, `TestSQLiteTargetPreparationDropsAllTablesBeforeAnyCreate`, `TestMySQLTargetPreparationUsesOneLockedDropBeforeCreates`, `TestClickHousePrepareDropsEveryTableBeforeCreatingAny`, and target live lifecycle tests. | S4.2: `TestStage4RebuildPreparationOrderingWithDurableTasksLive`. |
| Rebuild transfers into empty tables and finalizes identity/secondary objects after data. | **Covered base:** Stage 3 native same-engine/common fixtures and `TestAdapterRunnerOrdersAllTableLifecycle`. | S4.2 crash fixture: `TestStage4RebuildFinalizeAfterResumedTransferLive`. |
| Resume may suppress backup acknowledgement only with durable proof of the same unchanged run and owned target contents. | **Missing as a complete rule.** | S4.2: `TestRebuildResumeSuppressesAcknowledgementOnlyWithOwnedRunEvidence` and `TestRebuildResumeRechecksAcknowledgementAfterConfigOrTargetChange`. |
| Upsert requires target capability, complete source/target PKs, and existing tables unless contract evolution creates new ones. | **Partial:** `TestCapabilityValidationPrecedesAdapterConstruction`, `TestAdapterRunnerRejectsMissingPrimaryKeyBeforeTargetMutation`, and retained-target preflight tests. Contract-authorized table creation is absent. | S4.3: `TestUpsertCreatesNewTableOnlyUnderEvolveContract`. |
| Upsert inserts new rows, updates changed non-key values, retains target-only rows, and preserves existing sequence/index/FK/check objects. | **Covered base:** `TestUpsertUpdatesSourceColumnsWithoutReplacingTargetRow`, `TestAdapterRunnerUpsertAllowsTargetOnlyRows`, and Stage 3 retained-object/native upsert tests. | S4.2/S4.5: `TestStage4UpsertRetainedObjectsSurviveCrashResumeLive`; delete reconciliation is the only allowed target-only removal. |
| Upsert is idempotent under retry and complete-window replay. | **Partial for SQLite:** `TestSQLiteUpsertReplaysAfterRowNumberCheckpointFailure`. Native fresh-run writer tests do not prove durable replay. | S4.2/S4.4: `TestStage4CertifiedRelationalUpsertReplayMatrixLive`. |

### 7.4 Schema drift and contract

The committed Stage 3 baseline does not enforce this contract. Uncommitted S4.1
configuration parsing and state-evidence primitives are in progress; their
current unit names include `TestParseProductionSemanticsSurface`,
`TestParseExpandsScalarAndEmptySchemaContracts`,
`TestParseCanonicalizesDeprecatedProductionSettings`,
`TestParseRejectsInvalidProductionSemantics`,
`TestResumeCompatibilityHashCoversProductionDataSemantics`, and
`TestSchemaEvolutionRenamePreservesHashWireShape`. They are not completion
evidence until the slice compiles, is backend-conformant, and is composed into
the migration lifecycle. Every behavior row below remains a Stage 4 gap.

| Normative behavior | Required missing fixtures |
|---|---|
| Compare every run/resume to the latest successful applicable filtered snapshot; omitted contract is report-only unless `fail_on_schema_drift` gates. | `TestSchemaContractUsesLatestSuccessfulFilteredSnapshot`, `TestResumeReevaluatesSchemaContract`, `TestOmittedSchemaContractReportsOnly`, `TestFailOnSchemaDriftStopsBeforeMutation`. |
| Entity modes default correctly: scalar applies to tables/columns/data type; omitted entities under a present section default to `evolve`. | `TestSchemaContractEntityDefaultsAndScalarExpansion`. |
| `evolve`: add eligible tables/nullable columns, relax nullability, widen safe types; rebuild uses current shape. | `TestSchemaContractEvolveSafeChanges`, `TestSchemaContractEvolveRejectsUnsafeChanges`, `TestSchemaContractRebuildUsesCurrentShape`. |
| `freeze`: abort before transfer on entity drift. | `TestSchemaContractFreezeStopsBeforeTargetMutation`. |
| `discard_row`: remove added or affected tables from transfer, validation, and successful snapshot. | `TestSchemaContractDiscardRowProjectsWholeRun`. |
| `discard_value`: omit eligible columns, retain prior snapshot evidence for type drift, and prune dependent indexes/FKs/checks. | `TestSchemaContractDiscardValuePrunesDependentObjects`, `TestDiscardValueRetainsPriorSnapshotEvidence`. |
| `report`: emit drift without target schema mutation. | `TestSchemaContractReportIsReadOnly`. |
| Reject `tables: discard_value`; never discard PK, identity, or selected date column. | `TestSchemaContractRejectsTableDiscardValue`, `TestSchemaContractRejectsProtectedColumnDiscard`. |
| Block identity/PK addition in upsert, nullability tightening, narrowing/lossy conversion, coupled default/PK drift, and unrenderable operations. | `TestSchemaContractUnsafeEvolutionMatrix`. |
| Report dropped source objects but retain target objects; infer no destructive drop. | `TestSchemaContractSourceDropRetainsTarget`. |
| Every decision has entity, mode, kind, object, previous/current evidence, action, and reason in stable order. | `TestSchemaContractDecisionFactsAreCompleteAndStable`. |
| Reject simultaneous `schema_contract` and deprecated `schema_evolution`; preserve compatible old form when supported. | `TestSchemaContractRejectsMixedDeprecatedSurface`, `TestSchemaEvolutionCompatibilityHashWireShape`. |

Required live proof is
`TestStage4SchemaContractTargetMatrixLive`, with subtests for PostgreSQL,
SQL Server, Oracle MySQL, MariaDB, SQLite, and ClickHouse rebuild.

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
| Report the selected strategy per table. | **Partial:** pagination plans carry strategy; no complete Stage 4 JSON/audit fact. | `TestPaginationDecisionFactIsStablePerTable`. |
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
| Retry recognized transient network/server/lock errors with bounded exponential backoff, cancellation, and three default retries. | **Covered generic primitive:** `TestRetryUsesDefaultThreeRetryBudget`, `TestRetryTransientSuccessAndCancellationDuringBackoff`. Engine-specific classifiers are absent. | `TestPostgresRetryClassification`, `TestSQLServerRetryClassification`, `TestMySQLRetryClassification`, `TestMariaDBRetryClassification`, `TestSQLiteRetryClassification`, `TestClickHouseRetryClassification`. |
| Never blindly retry conversion, DDL policy, PK, schema contract, validation, lease, or state failures. | **Covered generic primitive:** `TestRetryStopsForStableNonTransientClasses`. | S4.2/S4.3/S4.7 route composition: `TestStage4StableFailureClassesNeverRetryLive`. |
| Possibly committed rebuild replay is insert-only and duplicate-safe by complete PK; it never updates an existing row. | **Covered only for SQLite:** `TestSQLiteIssuedReplayUsesInsertOnlyConflictIgnore`, `TestStage2SQLiteTupleKeysetWriteBeforeAckResumesExactRows`. | `TestPostgresInsertOnlyReplayLive`, `TestSQLServerInsertOnlyReplayLive`, `TestMySQLInsertOnlyReplayLive`, `TestMariaDBInsertOnlyReplayLive`; target without a safe path: `TestUnsafeReplayRequiresTableRestart`. |

## Section 9 — Incremental sync and delete convergence

**Refreshed.** A composed incremental lifecycle and a composed
PostgreSQL delete lifecycle are both committed. The original claim that neither
exists is obsolete. Roughly 53 incremental and 57 delete fixtures exist.

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

Required live proof — **five of seven now exist**:

- `TestPostgresIncrementalWindowLive` — exists
- `TestSQLServerIncrementalWindowLive` — exists
- `TestMySQLIncrementalWindowLive` — exists
- `TestMariaDBIncrementalWindowLive` — exists
- `TestSQLiteIncrementalWindow` — exists
- `TestStage4PostgresIncrementalCompositionLiveTLS` — exists, and is the only
  composed end-to-end incremental route proof
- `TestStage4CertifiedRelationalIncrementalRouteMatrixLive` — **missing**; the
  per-engine window fixtures above prove the window, not the composed route for
  every certified source/target pair
- `TestClickHouseIncrementalRejectedBeforeMutationLive` — **missing**

### 9.2 Delete reconciliation

**Scope caution.** The committed delete lifecycle certifies exactly one route:
PostgreSQL-to-PostgreSQL, upsert, non-strict, non-incremental. Everything
outside it is deliberately fail-closed. The rows below are covered *for that
cell*; the route matrix remains the open acceptance item.

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Default `off` preserves target-only rows. | **Covered:** `TestAdapterRunnerUpsertAllowsTargetOnlyRows` with delete mode off, and `TestStage4PostgresDeleteCompositionAdmissionIsExact`. | Route matrix. |
| Reconcile is upsert-only, interval-scheduled from durable last success, and requires stable PK. | **Covered:** `TestDeleteReconcileTaskAdmissionIsExact`, `TestDeleteReconcileRequiresExplicitSafeEqualityProof`, `TestStage4PostgresDeleteAttemptIDBindsOnlyStableWorkIdentity`, `TestStage4DeleteCompletionAndLatestSuccessAreFailClosed`. | Route matrix. |
| Compare key sets and hard-delete target-only keys in bounded parameter-safe batches. | **Covered:** `TestDeleteReconcileBatchByteCeiling`, `TestDeleteReconcileLargeKeySetUsesBoundedSpoolBatches`, `TestDeleteReconcileDiskSpoolAndBoundedBatches`, `TestStage4PostgresDeleteBatchByteLimitIsBounded`, `TestDeleteReconcileDuplicateKeysFailBeforeDelete`. | Route matrix. |
| Run after transfer/before validation and persist candidates, deleted rows, skips, reasons, completion. | **Covered:** `TestStage4PostgresDeleteLifecycleOrdersGlobalPhasesAndPreservesTransferredResume`, `TestDeleteReconcileCandidatePlanAndBatchIntentPrecedeMutation`. | Route matrix. |
| Distinguish not-due from ran-with-zero; incomplete work cannot advance last success. | **Covered:** `TestDeleteReconcileIncompleteNeverAdvancesLastSuccess`, `TestStage4PostgresDeleteLifecyclePropagatesCompletedAndNotDueStrictness`. | Route matrix. |
| Dry-run reports due/candidate impact without deletion. | **Partial:** `TestDeleteReconcileDryRunHasNoDurableWrites` proves the no-mutation half. The reporting half has no fixture. | `TestDeleteReconcileDryRunReportsDueCandidates` remains missing. |
| Completed full-scope reconciliation makes count validation strict; off/not-due permits upsert target supersets. | **Covered:** `TestAdapterResumeStrictReconciliationRejectsTargetSuperset`, `TestStage4PostgresDeleteTerminalStrictnessIsAuthenticated`. | Route matrix. |
| Crash safety: target delete and receipt survive a state-commit failure and replay exactly once. | **Covered (added since the original revision):** `TestDeleteReconcileCrashAfterTargetCommitReplaysReceipt`, `TestDeleteReconcileCrashAfterStateCommitUsesDurableFrontier`, `TestDeleteReconcileTargetErrorReceiptSurvivesStateCommitFailure`, `TestDeleteReconcileTerminalReplayCleansCrashLeftoverSpool`. | Route matrix. |
| Spool and plan evidence cannot be tampered between plan and mutation. | **Covered (not in the original revision):** `TestDeleteReconcilePostPlanTamperFailsBeforeIntentOrMutation`, `TestDeleteReconcileSpoolTamperFailsBeforeReplayMutation`, `TestDeleteSpoolReadSnapshotPreventsCandidateTOCTOU`, `TestDeleteReconcileMalformedLoadedEvidenceFailsClosed`. | Route matrix. |

Required live proof — `TestStage4PostgresDeleteCompositionLiveTLS` and
`TestStage4PostgresDeleteCompositionCrashResumeLiveTLS` **exist** and cover the
certified cell. Still missing:
`TestStage4CertifiedRelationalDeleteRouteMatrixLive` and
`TestClickHouseDeleteReconcileRejectedBeforeMutationLive`.

## Section 10 — Strict consistency

**Refreshed.** PostgreSQL strict consistency is now composed and live-proven.
Every other engine still rejects, which remains valid fail-closed behavior but
does not satisfy its supported Stage 4 combination. The rejection fixture was
renamed to `TestBuiltInRoutesRejectUncertifiedStrictConsistencyScopes`; the old
name `TestBuiltInRoutesRejectStrictConsistencyScopes` no longer exists.

Epoch and evidence primitives now covered by
`TestBeginStrictConsistencyOrdersEvidenceBeforeExecutableState`,
`TestBeginStrictConsistencyRevalidatesBeforeAuthorization`,
`TestBeginPlannedStrictConsistencyBindsWorkInsideOpenEpoch`,
`TestBeginPlannedStrictConsistencyEvidenceFailureClosesEpoch`,
`TestBeginPlannedStrictConsistencyPlannerFailureClosesEpoch`,
`TestStage4StrictEvidenceRequiresImmutableRunSourceEngine`, and
`TestStage4PostgresStrictWorkIdentityBindsEpochAndSnapshot`.

| Source/scope contract | Required missing fixtures |
|---|---|
| PostgreSQL table: exported MVCC snapshot shared by parallel readers, no writer blocking. | **Covered:** `TestStage4PostgresStrictComposedRouteStableEpochLiveTLS`, `TestStage4PostgresStrictParallelSourceOverlapsWithinBound`, `TestStage4PostgresStrictSnapshotOwnerStaysWithinConnectionBound`. |
| PostgreSQL migration: one exported snapshot across tables/partitions for one process epoch. | **Covered:** `TestStage4StrictMigrationSnapshotOwnsEveryTableEvidence`, `TestStage4PostgresStrictMixedCompletedResumeLiveTLS`. |
| SQL Server table: shared table view/lock; writes to that table wait. | `TestSQLServerStrictTableLockLive`. |
| SQL Server migration: one supported database snapshot; writers do not block. | `TestSQLServerStrictMigrationDatabaseSnapshotLive`. |
| MySQL/MariaDB table: parallel InnoDB repeatable-read sessions opened under brief `LOCK TABLES`; verify engine and privilege. | `TestMySQLStrictTableSnapshotLive`, `TestMariaDBStrictTableSnapshotLive`, `TestMySQLStrictRejectsEngineOrLockPrivilegeLive`. |
| SQLite table: one serializable reader and no parallel source readers. | `TestSQLiteStrictTableSnapshot`. |
| MySQL/SQLite migration and every ClickHouse strict scope reject before mutation. | `TestStrictConsistencyUnsupportedScopesBeforeMutation` and TLS live sentinels. |
| Full-table strict count comes from the same view, is persisted, and controls validation; later live drift is informational. | `TestStrictSnapshotCountIsPersistedAndAuthoritativeLive`. |
| PostgreSQL process resume opens and reports a new epoch while preserving per-table replay correctness. | **Covered:** `TestStage4PostgresStrictResumeUsesNewEpochAndReplaysLiveTLS`. |
| SQL Server snapshot survives and is reused; missing snapshot fails closed. | `TestSQLServerStrictResumeReusesDatabaseSnapshotLive`, `TestSQLServerStrictResumeMissingSnapshotFailsClosedLive`. |
| Owned snapshots release on success/failure/cancel; cleanup failure is visible. | `TestStrictSnapshotCleanupOnTerminalOutcomesLive`. |
| Strict partition jobs share the same view. | `TestStrictParallelRangesShareOneSourceViewLive`. |

Every strict fixture must run with concurrent source writes and under
`go test -race`; static mocks are not sufficient.

## Section 11 — Durable state, ownership, and resume

### 11.1 Backend capability contract

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Full local and YAML backends share restartability behavior. | **Covered:** `TestBackendConformance`, `TestRangeBackendConformance`, `TestRangeAttemptBackendConformance`, and `TestStage4BackendConformance` — the proposed extension now exists. | None. |
| Persist run/tasks/ranges plus incremental watermarks/fences, delete results, schema snapshots, strict evidence, event counters, config hash, and lease metadata. | **Covered:** `TestStage4BackendConformance` plus `TestStage4AggregateCompletionConformance`, `TestStage4AggregateReadConformance`, `TestStage4CanonicalSpatialMetadataRoundTrip`, `TestStage4CanonicalTypeMetadataRoundTrip`, `TestStage4ReusableEvidenceUsesBackendIndependentTotalOrder`. | Event counters have no named fixture; confirm whether they are Stage 5. |
| A pre-mutation table inventory may be replanned, and is fixed once any table publishes terminal evidence. | **Covered (requirement added 2026-07-31):** `TestStage4AggregateInventoryRevision` proves replan before terminal evidence, refusal after it, and immutable schema authority across a revision. See the note below on why the window exists. | Route matrix should exercise a replanned resume live. |
| YAML writes complete temporary state, flush/replace atomically, and serialize compare/write across processes. | **Covered base:** `TestYAMLStoreWritesPrivateCompleteDocument`, `TestYAMLStoreSerializesConcurrentProcesses`, `TestYAMLReplacementIsValidAcrossMidReplacementHardKills`. | Rerun after schema expansion in `TestStage4YAMLAtomicReplacementCrashMatrix`. |
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
| Run stores IDs/times/status/resumability/phase, source/target identity, sanitized config/hash, origin/error, and lease key/token/generation. | **Partial:** `Run` stores only a subset; lease is guarded externally. | `TestStage4RunRecordRoundTripAndRedaction`. |
| Tasks use structured type/schema/table/partition identity and store state, attempts/retries/times/scrubbed error. | **Covered base in `WorkTask`; legacy table `Task` remains scalar.** | `TestStage4UsesStructuredTaskIdentityEverywhere`. |
| Human task keys cannot collide for punctuation/quoted identifiers. | **Covered:** `TestTaskKeyCanonicalizationHasNoDelimiterCollisions` now exists. | None. |
| Progress stores table/range, rows done/total, safe typed watermark, range envelope, and timestamp. | **Covered base:** `TestRangeBackendConformance`. | Extend for Stage 4 timestamp/fence evidence in `TestStage4ProgressEnvelopeRoundTrip`. |

### 11.3 Required-write rule

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Task exists durably before destructive mutation. | **Covered base:** `TestTaskInitializationFailurePrecedesTargetMutation`. | Network live regression in S4.2. |
| Unresolved periodic/final/task/watermark/run-completion write failure prevents success with state exit 6. | **Covered:** `TestStage4EveryRequiredWriteFailureReturnsStateExitSix` drives each required write to failure and proves it classifies as a state failure and maps to exit 6; `TestStage4RequiredWriteFailureOutranksTransferClassification` pins the precedence so a durable write failure is never reported as a transfer error. Retained context: `TestAdapterRunnerReturnsOnlyCompletedProgressWhenLaterCheckpointFails`, `TestSQLitePartialResultKeepsRowsAfterAggregateCheckpointFailure`, `TestMigrationAttemptDisposition`. | The run-completion write is covered by `publishStage4RunSuccess` returning a state-classified error; a live route proof belongs in the S4.9 matrix. |
| Unknown task writes reject. | **Covered base:** `TestSQLiteStoreRequiresKnownRunningTaskForCompletion`, range backend unknown-work checks. | Add every new Stage 4 write to `TestStage4UnknownTaskWritesReject`. |
| Periodic save may degrade only if final safe frontier supersedes it, with audit evidence. | **Missing as complete behavior.** | `TestPeriodicCheckpointFailureCanBeSupersededAndAudited`, `TestPeriodicCheckpointFailureWithoutFinalSaveIsFatal`. |
| State failure after target commit directs repair-and-resume, not competing fresh run. | **Partial:** errors remain resumable, but remedy contract is absent. | `TestPostCommitStateFailureNamesRepairAndResume`. |

### 11.4 Exclusive target lease and fencing

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Fresh, resume, and abandon use canonical target lease. | **Covered base:** lease identity tests and Stage 2 lifecycle CLI tests. | `TestStage4NetworkRunResumeAbandonShareCanonicalLeaseLive`. |
| Random token, monotonic generation, atomic acquisition/takeover; live second owner fails and force cannot bypass. | **Covered base:** `TestSQLiteStoreRejectsSecondLiveTargetLease`, `TestReleasedLeaseRetainsMonotonicFencingGeneration`, force-resume tests. | Process-level `TestTargetLeaseTwoProcessRace` and `TestForceResumeCannotBypassLiveNetworkLease`. |
| Heartbeat renewal fences every run/task/progress/completion mutation; stale owner is cancelled and cannot succeed. | **Covered base:** `TestFencedBackendsRejectEveryOldGenerationMutationAfterTakeover`, `TestTargetMutationFenceSerializesTakeoverAndRejectsOldOwner`. | `TestStage4OldNetworkOwnerCancelledAfterTakeoverLive`. |
| Takeover only after TTL; legacy fresh un-fenced run rejects. | **Partial:** TTL takeover/generation is covered; explicit legacy-live rejection needs proof. | `TestLegacyFreshUnfencedRunRejectsTakeover`. |
| Different canonical targets run concurrently. | **Missing explicit process fixture.** | `TestDifferentCanonicalTargetsRunConcurrently`. |

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
| Incremental resume replays full lower-watermark window. | **Missing.** | S4.4 fixtures. |
| Possibly committed rebuild uses insert-only replay. | **Covered only for SQLite.** | S4.2 per-target replay fixtures. |
| Resume finalizes, reconciles, validates, and completes the original run. | **Partial for SQLite without Stage 4 semantics.** | `TestStage4NetworkCrashResumeCompletesOriginalRunLive`. |
| Hash excludes policy-only/derived fields and preserves deprecated rename wire shape. | **Covered base:** resume-hash tests. | `TestStage4ResumeHashPolicyAndDeprecatedWireShape`. |

`TestResumeRejectsNetworkPairBeforeStateOrLeaseAccess` documents the current
hard blocker: network resume is intentionally rejected and must be replaced,
not weakened, by S4.2.

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

Required live fixtures — this is the only part of Section 12 still open. The
deep-probe contract has two live proofs today,
`TestPostgresDatabaseValidationProbeStableTLSLive` and
`TestStage4AdapterPostgresStableDeepValidationComposedRouteLiveTLS`, plus
non-live deep semantics in `TestSQLiteDatabaseValidationProbeDeepSemantics` and
`TestDatabaseValidationProbeFailsClosed`. Still **missing**:

- `TestSQLServerValidationModesLive`
- `TestMySQLValidationModesLive`
- `TestMariaDBValidationModesLive`
- `TestStage4ValidationRouteMatrixLive`
- `TestValidationTimeoutFallbackEngineMatrixLive`

ClickHouse keeps rebuild equality validation and must explicitly reject modes
it cannot implement safely until separately admitted.

## Section 13 — Preflight and operational safety

| Normative behavior | Current evidence | Remaining proof |
|---|---|---|
| Stable findings include severity, dotted check name, side, message, remedy. | **Covered:** `TestComposeProductionPreflightReportIsStructurallyOrdered`, `TestComposeProductionPreflightReportRequiresCompleteManifest`, `TestComposeProductionPreflightReportRequiresApplicableEngineFacts`, `TestComposeProductionPreflightReportBlocksUnknownWriteAuthority`, `TestBuildProductionPreflightManifestIsStableAndConditional`, `TestBuildProductionPreflightManifestSelectsUpsertChecks`. | Live per-engine matrix below. |
| Error aborts before mutation unless exact check/prefix/all is explicitly skipped; skip downgrades visibly without erasing evidence. | **Covered:** `TestEvaluatePreflightAppliesVisibleExactPrefixAndAllSkips`, `TestComposeProductionPreflightReportPreservesVisibleExactSkip`, `TestEvaluatePreflightOrdersFindings`, `TestEvaluatePreflightIsRaceSafeAndDoesNotMutateInputs`, `TestEvaluatePreflightEmptyEvidenceIsNonBlocking`. The three proposed names do not exist; these supersede them. | Live per-engine matrix below. |
| Cover connection/auth, version, source read/target privileges, schema existence/access, encoding, pool headroom, strict prerequisites, disk estimate, destructive gate, and engine capability probes. | **Partial:** connection tests, Stage 3 version/privilege/catalog/destructive tests, MySQL local-infile and SQL Server hazards exist. There is no unified structured matrix, pool/disk proof, or strict prerequisite support. | `TestStage4PreflightCheckInventory`; live `TestPostgresPreflightMatrixLive`, `TestSQLServerPreflightMatrixLive`, `TestMySQLPreflightMatrixLive`, `TestMariaDBPreflightMatrixLive`, `TestSQLitePreflightMatrix`, `TestClickHousePreflightMatrixLive`. |
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
| Write-before-checkpoint replay neither duplicates nor overwrites. | **SQLite only.** Missing per-target insert-only replay live fixtures. |
| Concurrent wide tables remain within budget. | **SQLite only.** Missing `TestStage4NetworkWideTableJobsShareMemoryBudgetLive`. |

## Acceptance 21.6 — State, lease, and resume

| Acceptance item | Evidence or required fixture |
|---|---|
| Full/YAML backends share restartability conformance. | **Covered base;** extend with `TestStage4BackendConformance`. |
| YAML replacement survives crash/concurrent writers. | **Covered base;** rerun expanded `TestStage4YAMLAtomicReplacementCrashMatrix`. |
| Unknown/required-write failures prevent success. | **Covered:** `TestStage4EveryRequiredWriteFailureReturnsStateExitSix`. |
| Task creation fails before mutation. | **Covered base;** network live regression required. |
| Periodic failure may be superseded only by audited final save. | **Missing:** two proposed periodic-checkpoint fixtures. |
| Two processes racing one target produce one owner. | **Missing explicit process test:** `TestTargetLeaseTwoProcessRace`. |
| Old generation cannot mutate or report success after takeover. | **Covered base:** fenced backend/target mutation tests; network live fixture required. |
| Different targets run concurrently. | **Missing:** `TestDifferentCanonicalTargetsRunConcurrently`. |
| Config drift/force rules hold. | **Covered base;** extend for every Stage 4 field. |
| Completed skip requires target-count agreement. | **Covered base SQLite;** network matrix required. |
| Topology change clears stale progress. | **Covered base;** network matrix required. |
| Outcome/resumability matrix is exact. | **Covered base;** Stage 4 network semantics required. |
| Upgrades preserve history and reject ambiguous incomplete identities. | **Covered base:** legacy tests; expand for Stage 4 evidence and structured keys. Release-wide upgrade coverage remains Stage 6. |

## Acceptance 21.7 — Incremental and delete behavior

All acceptance items are **missing** and are owned by S4.4/S4.5:

- `TestIncrementalBaselineWatermarkAtomicCompletion`
- `TestIncrementalStrictLowerBoundSkipsEqualTimestamp`
- `TestIncrementalUnchangedRunTransfersZeroRows`
- `TestIncrementalWithoutCandidateUsesFullUpsert`
- `TestIncrementalResumeReplaysFullWindowBehindPrimaryKey`
- `TestIncrementalResumeReusesImmutableUpperFence`
- `TestDeleteReconcileHardDeleteAndPersistedCountsLive`
- `TestDeleteOffAndNotDuePreserveTargetSupersetLive`
- `TestDeleteReconcileDryRunReportsDueCandidates`

The certified relational incremental and delete route matrices are mandatory;
one engine-only fixture cannot close this acceptance section.

## Acceptance 21.8 — Strict consistency

All supported-mode acceptance items are **missing**. Use the exact live
fixtures listed in Section 10 to prove:

- PostgreSQL table and migration stable views without writer blocking;
- MySQL/MariaDB parallel repeatable-read sessions and prerequisite rejection;
- SQL Server table writer blocking and migration snapshot non-blocking;
- one SQLite stable reader;
- unsupported scope rejection before mutation;
- persisted snapshot count validation;
- SQL Server surviving-snapshot resume/fail-closed behavior; and
- PostgreSQL's explicit new resume epoch.

The current all-routes rejection tests remain useful only as a fail-closed
baseline until each supported cell is admitted.

## Acceptance 21.9 — Schema contract and validation

All acceptance items are **missing or partial**:

- Schema add/drop/evolution/freeze/report/discard behavior:
  `TestSchemaContractModeAcceptanceMatrix`.
- Protected discarded columns and dependent-object pruning:
  `TestSchemaContractDiscardValuePrunesDependentObjects`,
  `TestSchemaContractRejectsProtectedColumnDiscard`.
- JSON/audit evidence:
  `TestSchemaContractDecisionFactsAreCompleteAndStable` (audit presentation
  remains Stage 5).
- Count timeout/mismatch policy:
  `TestValidationTimeoutPolicyMatrix`,
  `TestValidationEstimateMismatchPolicyMatrix`.
- Upsert/reconciliation count policy:
  `TestValidationCountPolicyByModeAndReconciliation`.
- NULL parity:
  `TestNullParityDetectsSystematicConversionLive`.
- Canonical samples:
  `TestValidationCanonicalValueMatrix` and
  `TestStage4SampleValidationRouteMatrixLive`.
- Explicit `full` rejection:
  `TestValidationFullModeRejected`.

## Mandatory Stage 4 gates

No slice is complete until its focused tests pass in normal and race modes.
The final Stage 4 gate must include all of the following:

1. `go test ./... -count=1`
2. `go test -race ./... -count=1`
3. `go vet ./...`, command builds, cross-compilation checks already required
   by the repository, and `git diff --check`
4. `TestStage4LiveMatrixEnvironmentRequired`, which must fail—not skip—when
   the exit-gate flag is enabled and any pinned TLS endpoint is missing
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

Added 2026-07-31. This is the closure list implied by the refreshed rows above,
ordered by how much is left rather than by section number. Nothing here is
started.

### A. Route matrices — the largest block

Every certified-cell implementation needs its family. Missing, by name:

- `TestStage4CertifiedRelationalIncrementalRouteMatrixLive`
- `TestStage4CertifiedRelationalDeleteRouteMatrixLive`
- `TestStage4SchemaContractTargetMatrixLive`
- `TestStage4ValidationRouteMatrixLive`
- `TestStage4CertifiedRelationalCrashResumeMatrixLive` (both state backends)
- `TestStage4CertifiedRelationalTransferLifecycleLive`
- `TestStage4CertifiedRelationalUpsertReplayMatrixLive`

### B. Validation (Section 12) — logic done, live matrix open

Not a slice. The mode, timeout, policy, NULL-parity, sampling, and canonical
value contracts are all implemented and covered non-live. What remains is
`TestSQLServerValidationModesLive`, `TestMySQLValidationModesLive`,
`TestMariaDBValidationModesLive`, `TestStage4ValidationRouteMatrixLive`, and
`TestValidationTimeoutFallbackEngineMatrixLive` — all gated on block G.

### C. Strict consistency for the four remaining engines

MySQL, MariaDB, SQL Server, and SQLite each need their supported scope
implemented and live-proven; every ClickHouse scope must reject. PostgreSQL is
done and is the reference implementation.

### D. Schema-contract modes

`freeze`, `report`, `discard_value` dependent-object pruning, the entity-default
matrix, and the unsafe-evolution matrix have no fixtures. `evolve` and
projection are proven for PostgreSQL only.

### E. Dry-run (Section 7.2)

Still avoids target, state, lease, and audit, and reports only source row
counts. Needs target preflight, drift, pagination, tuning disclosure, delete
due/candidate reporting, and estimate provenance.

### F. Small named gaps

- ~~`TestDeterministicTuningPreservesPinnedIntent`~~ — **closed 2026-07-31**
- `TestDeleteReconcileDryRunReportsDueCandidates` — reporting half of dry-run
- `TestClickHouseIncrementalRejectedBeforeMutationLive`
- `TestClickHouseDeleteReconcileRejectedBeforeMutationLive`
- `TestTargetLeaseTwoProcessRace`, `TestDifferentCanonicalTargetsRunConcurrently`
- ~~`TestStage4EveryRequiredWriteFailureReturnsStateExitSix`~~ — **closed 2026-07-31**
- periodic-checkpoint supersession pair (Section 11.3)
- engine retry classifiers (Section 8.6) — six fixtures
- `TestStage4LiveMatrixEnvironmentRequired`

### G. The live gate

Every live row above reflects a tree from before the last hardening. The full
TLS matrix must be rerun, and the application's aggregate publication path needs
its first live coverage. **Blocked until 2026-08-06** by the approval quota; do
not work around that limit.

Stage 4 cannot be declared complete while any of A through G is open. Local
`go test ./...` passing is necessary and nowhere near sufficient.

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

Reordered 2026-07-31. The two original top risks are closed.

1. **Every implemented route is one certified cell, not a family.** Delete
   reconciliation is PostgreSQL-to-PostgreSQL upsert only; strict consistency is
   PostgreSQL only; the composed incremental route has one live proof. The
   acceptance sections ask for six-engine matrices. This is now the single
   largest distance between "implemented" and "complete", and it is easy to
   mistake a green suite for coverage.
2. **The live TLS matrix has not been rerun since the last hardening.** The
   approval quota is exhausted until 2026-08-06. Until it runs, every live row
   in this document reflects an earlier tree, and `go test ./...` passing locally
   proves nothing about the live gates.
3. **Validation logic is implemented but proven on one engine.** Section 12's
   contracts are covered by roughly 50 non-live fixtures; what is missing is the
   per-engine live matrix. The risk is inverted from what it looks like: the
   algorithms are done, so the remaining failure mode is an engine-specific
   driver or type behavior that only live proof surfaces.
4. **Schema-contract modes are partially implemented.** `evolve` and projection
   are proven for PostgreSQL; `freeze`, `report`, `discard_value` pruning, and
   the entity-default matrix have no fixtures. Treating the contract as DDL-only
   would create false success.
5. **Strict consistency for MySQL, MariaDB, SQL Server, and SQLite is
   unimplemented.** Snapshot ownership, crash cleanup, and SQL Server snapshot
   reuse remain the highest-risk cells.
6. **Dry-run does not meet its target-aware contract.** It still avoids the
   target, state, lease, and audit, and reports only source row counts.
7. **The application's aggregate publication path has no live coverage.** The
   composed run completion is proven at the migrate layer, but the `app` branch
   that calls it is reachable only through a PostgreSQL route.
8. **Closed:** network resume now exists
   (`TestStage4PostgresTLSToSQLiteNetworkCrashResumeLive`,
   `TestStage4AdapterNetworkResumeReplansChangedRangeCount`, and the
   `TestStage4AdapterNetworkResume*` family), and the backend-conformant Stage 4
   state model landed (`TestStage4BackendConformance`).
