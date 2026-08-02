package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
	_ "modernc.org/sqlite"
)

// Batch construction: turning spooled candidates into bounded delete batches
// with their completion accounting.

type deleteSpoolQueryer interface {
	QueryContext(
		context.Context,
		string,
		...any,
	) (*sql.Rows, error)
}

func deleteCandidateEvidence(
	ctx context.Context,
	queryer deleteSpoolQueryer,
) (int64, string, error) {
	rows, err := queryer.QueryContext(ctx, `
		SELECT target.canonical, target.parameters
		FROM target_keys AS target
		LEFT JOIN source_keys AS source
		  ON source.canonical = target.canonical
		WHERE source.canonical IS NULL
		ORDER BY target.canonical
	`)
	if err != nil {
		return 0, "", fmt.Errorf(
			"open delete candidate stream: %w",
			err,
		)
	}
	defer rows.Close()
	digest := sha256.New()
	var count int64
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return 0, "", err
		}
		var canonical, parameters []byte
		if err := rows.Scan(&canonical, &parameters); err != nil {
			return 0, "", fmt.Errorf(
				"read delete candidate evidence: %w",
				err,
			)
		}
		writeDeleteHashFrame(digest, "key", canonical)
		writeDeleteHashFrame(digest, "parameters", parameters)
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf(
			"iterate delete candidate evidence: %w",
			err,
		)
	}
	return count, hex.EncodeToString(digest.Sum(nil)), nil
}

func (spool *deleteKeySpool) finalize(
	ctx context.Context,
	planID string,
	proofDigest string,
) (int64, string, error) {
	candidates, candidateDigest, err := deleteCandidateEvidence(
		ctx,
		spool.db,
	)
	if err != nil {
		return 0, "", err
	}
	transaction, err := spool.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, "", fmt.Errorf("begin delete spool metadata: %w", err)
	}
	defer transaction.Rollback()
	meta := map[string]string{
		"plan_id":          planID,
		"proof_digest":     proofDigest,
		"candidate_digest": candidateDigest,
		"candidates":       strconv.FormatInt(candidates, 10),
	}
	for name, value := range meta {
		if _, err := transaction.ExecContext(
			ctx,
			`INSERT INTO plan_meta (name, value) VALUES (?, ?)`,
			name,
			value,
		); err != nil {
			return 0, "", fmt.Errorf(
				"write delete spool metadata: %w",
				err,
			)
		}
	}
	if err := transaction.Commit(); err != nil {
		return 0, "", fmt.Errorf(
			"commit delete spool metadata: %w",
			err,
		)
	}
	if err := spool.sync(); err != nil {
		return 0, "", err
	}
	return candidates, candidateDigest, nil
}

func (snapshot *deleteKeySpoolSnapshot) verify(
	ctx context.Context,
	plan state.DeleteReconciliationPlan,
	proofDigest string,
) error {
	expected := map[string]string{
		"plan_id":          plan.PlanID,
		"proof_digest":     proofDigest,
		"candidate_digest": plan.CandidateDigest,
		"candidates":       strconv.FormatInt(plan.Candidates, 10),
	}
	rows, err := snapshot.transaction.QueryContext(
		ctx,
		`SELECT name, value FROM plan_meta`,
	)
	if err != nil {
		return fmt.Errorf("read delete spool metadata: %w", err)
	}
	found := make(map[string]string)
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			rows.Close()
			return fmt.Errorf("scan delete spool metadata: %w", err)
		}
		if _, duplicate := found[name]; duplicate {
			rows.Close()
			return fmt.Errorf("delete spool metadata is duplicated")
		}
		found[name] = value
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if err := errors.Join(rowsErr, closeErr); err != nil {
		return fmt.Errorf("iterate delete spool metadata: %w", err)
	}
	if len(found) != len(expected) {
		return fmt.Errorf("delete spool metadata shape differs")
	}
	for name, value := range expected {
		if found[name] != value {
			return fmt.Errorf(
				"delete spool metadata %s differs",
				name,
			)
		}
	}
	candidates, digest, err := deleteCandidateEvidence(
		ctx,
		snapshot.transaction,
	)
	if err != nil {
		return err
	}
	if candidates != plan.Candidates ||
		digest != plan.CandidateDigest {
		return fmt.Errorf(
			"delete spool candidate evidence differs from durable plan",
		)
	}
	return nil
}

type deleteSpoolCandidate struct {
	canonical  []byte
	parameters []driver.Value
}

func (snapshot *deleteKeySpoolSnapshot) candidateBatch(
	ctx context.Context,
	offset int64,
	limit int,
	maxBytes int64,
) ([]deleteSpoolCandidate, string, int64, error) {
	if offset < 0 || limit <= 0 || maxBytes <= 0 {
		return nil, "", 0, fmt.Errorf(
			"invalid delete candidate batch range",
		)
	}
	lengthRows, err := snapshot.transaction.QueryContext(ctx, `
		SELECT length(target.canonical), length(target.parameters)
		FROM target_keys AS target
		LEFT JOIN source_keys AS source
		  ON source.canonical = target.canonical
		WHERE source.canonical IS NULL
		ORDER BY target.canonical
		LIMIT ? OFFSET ?
	`, limit, offset)
	if err != nil {
		return nil, "", 0, fmt.Errorf(
			"open delete candidate batch lengths: %w",
			err,
		)
	}
	selected := 0
	var encodedBytes int64
	for lengthRows.Next() {
		var keyBytes, parameterBytes int64
		if err := lengthRows.Scan(
			&keyBytes,
			&parameterBytes,
		); err != nil {
			lengthRows.Close()
			return nil, "", 0, fmt.Errorf(
				"read delete candidate batch lengths: %w",
				err,
			)
		}
		rowBytes := keyBytes + parameterBytes + 16
		if rowBytes <= 0 || rowBytes > maxBytes {
			lengthRows.Close()
			return nil, "", 0, fmt.Errorf(
				"delete candidate requires %d encoded bytes, exceeding the %d-byte batch ceiling",
				rowBytes,
				maxBytes,
			)
		}
		if encodedBytes+rowBytes > maxBytes {
			break
		}
		encodedBytes += rowBytes
		selected++
	}
	lengthErr := lengthRows.Err()
	closeErr := lengthRows.Close()
	if err := errors.Join(lengthErr, closeErr); err != nil {
		return nil, "", 0, fmt.Errorf(
			"iterate delete candidate batch lengths: %w",
			err,
		)
	}
	if selected == 0 {
		return nil, "", 0, fmt.Errorf(
			"delete candidate batch byte ceiling admitted no rows",
		)
	}
	rows, err := snapshot.transaction.QueryContext(ctx, `
		SELECT target.canonical, target.parameters
		FROM target_keys AS target
		LEFT JOIN source_keys AS source
		  ON source.canonical = target.canonical
		WHERE source.canonical IS NULL
		ORDER BY target.canonical
		LIMIT ? OFFSET ?
	`, selected, offset)
	if err != nil {
		return nil, "", 0, fmt.Errorf(
			"open delete candidate batch: %w",
			err,
		)
	}
	defer rows.Close()
	candidates := make([]deleteSpoolCandidate, 0, selected)
	digest := sha256.New()
	for rows.Next() {
		var canonical, encoded []byte
		if err := rows.Scan(&canonical, &encoded); err != nil {
			return nil, "", 0, fmt.Errorf(
				"read delete candidate batch: %w",
				err,
			)
		}
		parameters, err := decodeDeleteParameters(encoded)
		if err != nil {
			return nil, "", 0, err
		}
		writeDeleteHashFrame(digest, "key", canonical)
		writeDeleteHashFrame(digest, "parameters", encoded)
		candidates = append(candidates, deleteSpoolCandidate{
			canonical:  append([]byte(nil), canonical...),
			parameters: parameters,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, "", 0, fmt.Errorf(
			"iterate delete candidate batch: %w",
			err,
		)
	}
	if len(candidates) != selected {
		return nil, "", 0, fmt.Errorf(
			"delete candidate batch changed between length admission and read",
		)
	}
	return candidates, hex.EncodeToString(digest.Sum(nil)),
		encodedBytes, nil
}

func deleteBatchLimit(
	configured int,
	parameterLimit int,
	keyWidth int,
) (int, error) {
	if configured <= 0 || parameterLimit <= 0 || keyWidth <= 0 {
		return 0, fmt.Errorf(
			"delete batch size, parameter limit, and key width must be positive",
		)
	}
	parameterBound := parameterLimit / keyWidth
	if parameterBound == 0 {
		return 0, fmt.Errorf(
			"delete primary-key width %d exceeds target parameter limit %d",
			keyWidth,
			parameterLimit,
		)
	}
	if configured < parameterBound {
		return configured, nil
	}
	return parameterBound, nil
}

func deleteBatchToken(
	planID string,
	sequence int64,
	batchDigest string,
) string {
	digest := sha256.Sum256([]byte(
		planID + "\x00" +
			strconv.FormatInt(sequence, 10) + "\x00" +
			batchDigest,
	))
	return hex.EncodeToString(digest[:])
}

func (reconciler deleteReconciler) completionTime(
	startedAt time.Time,
) (time.Time, error) {
	value := time.Now()
	if reconciler.now != nil {
		value = reconciler.now()
	}
	if value.IsZero() || value.Before(startedAt) {
		return time.Time{}, fmt.Errorf(
			"delete reconciliation completion time is absent or precedes its start",
		)
	}
	return value.UTC(), nil
}

func (reconciler deleteReconciler) loadLatestDueFacts(
	request deleteReconcileRequest,
) (deleteDueFacts, error) {
	last, found, err := reconciler.state.
		LoadLatestSuccessfulDeleteReconciliation(
			request.RunID,
			request.Task,
		)
	if err != nil {
		return deleteDueFacts{}, fmt.Errorf(
			"load latest successful delete reconciliation: %w",
			err,
		)
	}
	return deleteReconciliationDue(
		request.Now,
		request.Policy.Reconcile.Interval,
		last,
		found,
	)
}

func (reconciler deleteReconciler) buildSpool(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
	planID string,
) (*deleteKeySpool, int64, string, error) {
	spool, err := newDeleteKeySpool(
		request.SpoolDirectory,
		planID,
	)
	if err != nil {
		return nil, 0, "", err
	}
	fail := func(err error) (*deleteKeySpool, int64, string, error) {
		return spool, 0, "", err
	}
	if err := spool.scanKeys(
		ctx,
		deleteKeySourceSide,
		request.SourceTable,
		keyPlan.sourceColumns,
		keyPlan.proof,
		reconciler.canonicalizer,
		request.MaxBatchBytes,
		reconciler.source.OpenDeletePrimaryKeys,
	); err != nil {
		return fail(err)
	}
	if err := spool.scanKeys(
		ctx,
		deleteKeyTargetSide,
		request.TargetTable,
		keyPlan.targetColumns,
		keyPlan.proof,
		reconciler.canonicalizer,
		request.MaxBatchBytes,
		reconciler.target.OpenDeletePrimaryKeys,
	); err != nil {
		return fail(err)
	}
	candidates, digest, err := spool.finalize(
		ctx,
		planID,
		keyPlan.proofDigest,
	)
	if err != nil {
		return fail(err)
	}
	return spool, candidates, digest, nil
}

func (reconciler deleteReconciler) reconcile(
	ctx context.Context,
	request deleteReconcileRequest,
) (deleteReconcileOutcome, error) {
	if reconciler.state == nil {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"delete reconciliation state backend is required",
		)
	}
	keyPlan, err := validateDeleteReconcileRequest(
		request,
		reconciler.canonicalizer,
	)
	if err != nil {
		return deleteReconcileOutcome{}, err
	}
	if err := ctx.Err(); err != nil {
		return deleteReconcileOutcome{}, err
	}

	if request.DryRun {
		return reconciler.reconcileDryRun(
			ctx,
			request,
			keyPlan,
		)
	}
	return reconciler.reconcileMutating(
		ctx,
		request,
		keyPlan,
	)
}

func (reconciler deleteReconciler) reconcileDryRun(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
) (outcome deleteReconcileOutcome, resultErr error) {
	dueFacts, err := reconciler.loadLatestDueFacts(request)
	if err != nil {
		return deleteReconcileOutcome{}, err
	}
	outcome = deleteReconcileOutcome{
		DueFacts: dueFacts,
		Record: state.DeleteReconciliation{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			Due:       dueFacts.Due, DryRun: true,
			Status:      state.DeleteReconciliationDryRun,
			Reason:      dueFacts.Reason,
			StartedAt:   request.Now.UTC(),
			CompletedAt: request.Now.UTC(),
		},
	}
	if !dueFacts.Due {
		outcome.Record.Status = state.DeleteReconciliationNotDue
		outcome.Record.DryRun = false
		return outcome, nil
	}
	if reconciler.source == nil || reconciler.target == nil {
		return outcome, fmt.Errorf(
			"due dry-run delete reconciliation requires source and target key readers",
		)
	}
	planID, err := newDeletePlanID()
	if err != nil {
		return outcome, err
	}
	spool, candidates, _, err := reconciler.buildSpool(
		ctx,
		request,
		keyPlan,
		planID,
	)
	if spool != nil {
		defer func() {
			if err := cleanupDeleteKeySpool(
				request.SpoolDirectory,
				spool,
			); err != nil {
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf(
						"dry-run delete reconciliation spool cleanup failed: %w",
						err,
					),
				)
			}
		}()
	}
	if err != nil {
		return outcome, err
	}
	outcome.Record.Candidates = candidates
	outcome.Record.Reason = "dry run; target rows were not mutated"
	completedAt, err := reconciler.completionTime(
		outcome.Record.StartedAt,
	)
	if err != nil {
		return outcome, err
	}
	outcome.Record.CompletedAt = completedAt
	return outcome, nil
}

func (reconciler deleteReconciler) reconcileMutating(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
) (deleteReconcileOutcome, error) {
	existing, found, err := reconciler.state.LoadDeleteReconciliation(
		request.RunID,
		request.Task,
		request.AttemptID,
	)
	if err != nil {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"load delete reconciliation attempt: %w",
			err,
		)
	}
	if found {
		if err := state.ValidateDeleteReconciliationEvidence(
			existing,
		); err != nil {
			return deleteReconcileOutcome{}, fmt.Errorf(
				"loaded delete reconciliation is malformed: %w",
				err,
			)
		}
		if existing.Status !=
			state.DeleteReconciliationRunning {
			outcome, terminalErr := terminalDeleteReconcileOutcome(existing)
			var cleanupErr error
			if existing.Plan != nil {
				if err := removeDeleteSpoolPath(
					request.SpoolDirectory,
					existing.Plan.SpoolPath,
				); err != nil {
					cleanupErr = fmt.Errorf(
						"terminal delete reconciliation spool cleanup failed: %w",
						err,
					)
				}
			}
			return outcome, errors.Join(terminalErr, cleanupErr)
		}
	}
	dueFacts, err := reconciler.loadLatestDueFacts(request)
	if err != nil {
		return deleteReconcileOutcome{}, err
	}
	if found && !dueFacts.Due {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"running delete reconciliation conflicts with not-due schedule",
		)
	}
	record := existing
	if !found {
		record = state.DeleteReconciliation{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			Due:       dueFacts.Due,
			StartedAt: request.Now.UTC(),
		}
		if !dueFacts.Due {
			record.Reason = dueFacts.Reason
		}
		record, _, err = reconciler.state.
			BeginDeleteReconciliation(record)
		if err != nil {
			return deleteReconcileOutcome{}, fmt.Errorf(
				"begin delete reconciliation: %w",
				err,
			)
		}
		if err := state.ValidateDeleteReconciliationEvidence(
			record,
		); err != nil {
			return deleteReconcileOutcome{}, fmt.Errorf(
				"begun delete reconciliation is malformed: %w",
				err,
			)
		}
	}
	outcome := deleteReconcileOutcome{
		Record: record, DueFacts: dueFacts,
	}
	if !dueFacts.Due {
		return outcome, nil
	}
	if record.LastBatchCommit != nil &&
		record.LastBatchCommit.FailClosedReason != "" {
		reason := record.LastBatchCommit.FailClosedReason
		return reconciler.finishIncomplete(
			outcome,
			record,
			fmt.Errorf(
				"delete reconciliation is fail-closed after its last target receipt (%s)",
				reason,
			),
			reason,
			request.SpoolDirectory,
		)
	}
	if reconciler.source == nil || reconciler.target == nil {
		return reconciler.finishIncomplete(
			outcome,
			record,
			errors.New(
				"due delete reconciliation requires source and target key readers",
			),
			state.DeleteReconciliationReasonKeyReadersUnavailable,
			request.SpoolDirectory,
		)
	}
	if reconciler.protector == nil {
		return reconciler.finishIncomplete(
			outcome,
			record,
			errors.New(
				"delete reconciliation requires a target lease/fencing mutation protector",
			),
			state.DeleteReconciliationReasonMutationProtectionUnavailable,
			request.SpoolDirectory,
		)
	}
	batchSize, err := deleteBatchLimit(
		request.Policy.Reconcile.BatchSize,
		reconciler.target.MaxDeleteParameters(),
		len(keyPlan.targetColumns),
	)
	if err != nil {
		return reconciler.finishIncomplete(
			outcome,
			record,
			err,
			state.DeleteReconciliationReasonUnsafeBatchLimits,
			request.SpoolDirectory,
		)
	}

	var spool *deleteKeySpool
	if record.Plan == nil {
		planID, err := newDeletePlanID()
		if err != nil {
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonPlanCreationFailed,
				request.SpoolDirectory,
			)
		}
		var candidates int64
		var candidateDigest string
		spool, candidates, candidateDigest, err =
			reconciler.buildSpool(
				ctx,
				request,
				keyPlan,
				planID,
			)
		if err != nil {
			if spool != nil {
				if cleanupErr := cleanupDeleteKeySpool(
					request.SpoolDirectory,
					spool,
				); cleanupErr != nil {
					err = errors.Join(
						err,
						fmt.Errorf(
							"delete-key scan spool cleanup failed: %w",
							cleanupErr,
						),
					)
				}
			}
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonKeyScanFailed,
				request.SpoolDirectory,
			)
		}
		plannedAt, err := reconciler.completionTime(
			record.StartedAt,
		)
		if err != nil {
			if cleanupErr := cleanupDeleteKeySpool(
				request.SpoolDirectory,
				spool,
			); cleanupErr != nil {
				err = errors.Join(
					err,
					fmt.Errorf(
						"delete-plan spool cleanup failed: %w",
						cleanupErr,
					),
				)
			}
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonClockInvalid,
				request.SpoolDirectory,
			)
		}
		plan := state.DeleteReconciliationPlan{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			PlanID:    planID, SpoolPath: spool.path,
			EqualityProofDigest: keyPlan.proofDigest,
			CandidateDigest:     candidateDigest,
			Candidates:          candidates, BatchSize: batchSize,
			BatchByteLimit: request.MaxBatchBytes,
			KeyWidth:       len(keyPlan.targetColumns),
			PlannedAt:      plannedAt,
		}
		if err := reconciler.state.
			SaveDeleteReconciliationPlan(plan); err != nil {
			spool.Close()
			return outcome, fmt.Errorf(
				"persist delete reconciliation plan: %w",
				err,
			)
		}
		record.Plan = &plan
		record.Candidates = candidates
		outcome.Record = record
	} else {
		if record.Plan.EqualityProofDigest !=
			keyPlan.proofDigest ||
			record.Plan.BatchSize != batchSize ||
			record.Plan.BatchByteLimit !=
				request.MaxBatchBytes ||
			record.Plan.KeyWidth != len(keyPlan.targetColumns) {
			return reconciler.finishIncomplete(
				outcome,
				record,
				errors.New(
					"durable delete plan no longer matches route proof or batching limits",
				),
				state.DeleteReconciliationReasonDurablePlanMismatch,
				request.SpoolDirectory,
			)
		}
		spool, err = openDeleteKeySpool(
			request.SpoolDirectory,
			record.Plan.SpoolPath,
		)
		if err != nil {
			return reconciler.finishIncomplete(
				outcome,
				record,
				err,
				state.DeleteReconciliationReasonSpoolUnavailable,
				request.SpoolDirectory,
			)
		}
	}
	snapshot, err := spool.beginReadSnapshot(ctx)
	if err != nil {
		spool.Close()
		return reconciler.finishIncomplete(
			outcome,
			record,
			err,
			state.DeleteReconciliationReasonSpoolVerificationFailed,
			request.SpoolDirectory,
		)
	}
	if err := snapshot.verify(
		ctx,
		*record.Plan,
		keyPlan.proofDigest,
	); err != nil {
		snapshot.Close()
		spool.Close()
		return reconciler.finishIncomplete(
			outcome,
			record,
			err,
			state.DeleteReconciliationReasonSpoolVerificationFailed,
			request.SpoolDirectory,
		)
	}
	defer spool.Close()
	defer snapshot.Close()
	outcome, err = reconciler.applyPlannedDeletes(
		ctx,
		request,
		keyPlan,
		snapshot,
		spool,
		outcome,
	)
	if outcome.Record.Status !=
		state.DeleteReconciliationRunning {
		snapshotErr := snapshot.Close()
		cleanupErr := cleanupDeleteKeySpool(
			request.SpoolDirectory, spool,
		)
		if snapshotErr != nil || cleanupErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"terminal delete reconciliation spool cleanup failed: %w",
					errors.Join(snapshotErr, cleanupErr),
				),
			)
		}
	}
	return outcome, err
}

func (reconciler deleteReconciler) applyPlannedDeletes(
	ctx context.Context,
	request deleteReconcileRequest,
	keyPlan deleteKeyPlan,
	snapshot *deleteKeySpoolSnapshot,
	spool *deleteKeySpool,
	outcome deleteReconcileOutcome,
) (deleteReconcileOutcome, error) {
	record := outcome.Record
	for record.Frontier < record.Candidates {
		if err := ctx.Err(); err != nil {
			return outcome, err
		}
		var intent state.DeleteReconciliationBatch
		var candidates []deleteSpoolCandidate
		if record.PendingBatch != nil {
			intent = *record.PendingBatch
			var digest string
			var encodedBytes int64
			var err error
			candidates, digest, encodedBytes, err = snapshot.candidateBatch(
				ctx,
				intent.FirstCandidate,
				int(intent.Candidates),
				record.Plan.BatchByteLimit,
			)
			if err != nil {
				return outcome, err
			}
			if int64(len(candidates)) != intent.Candidates ||
				encodedBytes != intent.EncodedBytes ||
				digest != intent.BatchDigest ||
				deleteBatchToken(
					intent.PlanID,
					intent.Sequence,
					digest,
				) != intent.Token {
				return outcome, fmt.Errorf(
					"pending delete batch differs from durable spool evidence",
				)
			}
		} else {
			remaining := record.Candidates - record.Frontier
			limit := record.Plan.BatchSize
			if remaining < int64(limit) {
				limit = int(remaining)
			}
			var digest string
			var encodedBytes int64
			var err error
			candidates, digest, encodedBytes, err = snapshot.candidateBatch(
				ctx,
				record.Frontier,
				limit,
				record.Plan.BatchByteLimit,
			)
			if err != nil {
				return outcome, err
			}
			if len(candidates) == 0 ||
				len(candidates) > limit {
				return outcome, fmt.Errorf(
					"delete spool returned an invalid candidate batch size",
				)
			}
			beganAt, err := reconciler.completionTime(
				record.Plan.PlannedAt,
			)
			if err != nil {
				return outcome, err
			}
			intent = state.DeleteReconciliationBatch{
				RunID: request.RunID, Task: request.Task,
				AttemptID:      request.AttemptID,
				PlanID:         record.Plan.PlanID,
				Sequence:       record.CommittedBatches,
				FirstCandidate: record.Frontier,
				Candidates:     int64(len(candidates)),
				EncodedBytes:   encodedBytes,
				BatchDigest:    digest,
				BeganAt:        beganAt,
			}
			intent.Token = deleteBatchToken(
				intent.PlanID,
				intent.Sequence,
				intent.BatchDigest,
			)
			stored, _, err := reconciler.state.
				BeginDeleteReconciliationBatch(intent)
			if err != nil {
				return outcome, fmt.Errorf(
					"persist delete batch intent: %w",
					err,
				)
			}
			if stored != intent {
				return outcome, fmt.Errorf(
					"stored delete batch intent differs",
				)
			}
			record.PendingBatch = &stored
			outcome.Record = record
		}
		keys := make([][]driver.Value, len(candidates))
		for index := range candidates {
			keys[index] = append(
				[]driver.Value(nil),
				candidates[index].parameters...,
			)
		}
		targetBatch := deleteTargetBatch{
			Table: request.TargetTable,
			Columns: append(
				[]string(nil),
				keyPlan.targetColumns...,
			),
			PlanID: intent.PlanID, Token: intent.Token,
			Sequence:    intent.Sequence,
			BatchDigest: intent.BatchDigest,
			Keys:        keys,
		}
		var receipt deleteTargetBatchReceipt
		var targetErr error
		invocations := 0
		protectedErr := reconciler.protector.
			ProtectDeleteMutation(ctx, func() error {
				invocations++
				if invocations != 1 {
					return fmt.Errorf(
						"target mutation protector invoked one delete batch multiple times",
					)
				}
				receipt, targetErr =
					reconciler.target.ApplyDeleteBatch(
						ctx,
						targetBatch,
					)
				return targetErr
			})
		if invocations == 0 {
			if protectedErr == nil {
				protectedErr = errors.New(
					"mutation protector returned without invoking the protected operation",
				)
			}
			return outcome, fmt.Errorf(
				"target delete mutation protection denied the driver call; resume this existing run after target ownership is restored: %w",
				protectedErr,
			)
		}
		applyErr := errors.Join(targetErr, protectedErr)
		if err := validateDeleteTargetReceipt(
			intent,
			receipt,
			targetErr,
		); err != nil {
			if applyErr != nil {
				return outcome, errors.Join(applyErr, err)
			}
			return outcome, err
		}
		failClosedReason := ""
		switch {
		case receipt.FailClosedReason != "":
			// Target-returned errors are part of the target-atomic receipt
			// journal. Receipt replay must reproduce this terminal evidence
			// even though it no longer returns the original error.
			failClosedReason = receipt.FailClosedReason
		case protectedErr != nil:
			// A protector error after the callback is local authority
			// evidence. Commit it fail-closed when possible, but do not
			// contaminate an otherwise clean target receipt: if this state
			// write fails, replay under a healthy protector may continue.
			failClosedReason = deleteIncompleteReason(
				state.DeleteReconciliationReasonTargetMutationFailed,
				protectedErr,
			)
		case receipt.DeletedRows != intent.Candidates:
			failClosedReason =
				state.DeleteReconciliationReasonTargetReceiptIncomplete
		}
		committedAt, err := reconciler.completionTime(
			intent.BeganAt,
		)
		if err != nil {
			return outcome, err
		}
		commit := state.DeleteReconciliationBatchCommit{
			RunID: request.RunID, Task: request.Task,
			AttemptID: request.AttemptID,
			PlanID:    intent.PlanID, Token: intent.Token,
			Sequence:         intent.Sequence,
			FirstCandidate:   intent.FirstCandidate,
			BatchDigest:      intent.BatchDigest,
			Candidates:       intent.Candidates,
			EncodedBytes:     intent.EncodedBytes,
			DeletedRows:      receipt.DeletedRows,
			ReceiptDigest:    receipt.ReceiptDigest,
			FailClosedReason: failClosedReason,
			CommittedAt:      committedAt,
		}
		if err := reconciler.state.
			CommitDeleteReconciliationBatch(commit); err != nil {
			ackErr := fmt.Errorf(
				"target delete batch committed, but durable frontier acknowledgement failed; repair state and resume this existing run and attempt; do not start a fresh run: %w",
				err,
			)
			return outcome, errors.Join(applyErr, ackErr)
		}
		record.Frontier += commit.Candidates
		record.CommittedBatches++
		record.DeletedRows += commit.DeletedRows
		record.PendingBatch = nil
		record.LastBatchCommit = &commit
		outcome.Record = record
		if failClosedReason != "" {
			primary := applyErr
			if primary == nil {
				if receipt.DeletedRows != intent.Candidates {
					primary = fmt.Errorf(
						"target delete batch deleted %d of %d candidates",
						receipt.DeletedRows,
						intent.Candidates,
					)
				} else {
					primary = fmt.Errorf(
						"target delete batch receipt carries fail-closed evidence: %s",
						failClosedReason,
					)
				}
			} else {
				primary = fmt.Errorf(
					"target delete batch returned a durable receipt and error: %w",
					primary,
				)
			}
			if err := errors.Join(
				snapshot.Close(),
				spool.Close(),
			); err != nil {
				primary = errors.Join(
					primary,
					fmt.Errorf(
						"close terminal delete reconciliation spool snapshot: %w",
						err,
					),
				)
			}
			return reconciler.finishIncomplete(
				outcome,
				record,
				primary,
				failClosedReason,
				request.SpoolDirectory,
			)
		}
		if err := ctx.Err(); err != nil {
			return outcome, err
		}
	}
	completedAt, err := reconciler.completionTime(
		record.StartedAt,
	)
	if err != nil {
		return outcome, err
	}
	result := state.DeleteReconciliationResult{
		RunID: record.RunID, Task: record.Task,
		AttemptID:   record.AttemptID,
		Status:      state.DeleteReconciliationCompleted,
		Candidates:  record.Candidates,
		DeletedRows: record.DeletedRows,
		SkippedRows: 0,
		CompletedAt: completedAt,
	}
	if err := ctx.Err(); err != nil {
		return outcome, err
	}
	if err := reconciler.state.
		FinishDeleteReconciliation(result); err != nil {
		return outcome, fmt.Errorf(
			"persist completed delete reconciliation after target deletes; repair state and resume this existing run and attempt; do not start a fresh run: %w",
			err,
		)
	}
	outcome.Record = deleteRecordFromResult(record, result)
	outcome.StrictCountValidation = true
	return outcome, nil
}

func validateDeleteTargetReceipt(
	intent state.DeleteReconciliationBatch,
	receipt deleteTargetBatchReceipt,
	targetErr error,
) error {
	if receipt.PlanID != intent.PlanID ||
		receipt.Token != intent.Token ||
		receipt.Sequence != intent.Sequence ||
		receipt.BatchDigest != intent.BatchDigest ||
		receipt.Candidates != intent.Candidates {
		return fmt.Errorf(
			"target delete batch receipt identity differs from durable intent",
		)
	}
	if receipt.DeletedRows < 0 ||
		receipt.DeletedRows > receipt.Candidates {
		return fmt.Errorf(
			"target delete batch receipt has invalid deleted-row count",
		)
	}
	if err := validateLowerSHA256(
		"target delete batch receipt digest",
		receipt.ReceiptDigest,
	); err != nil {
		return err
	}
	if receipt.FailClosedReason != "" &&
		receipt.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed {
		return fmt.Errorf(
			"target delete batch receipt has invalid fail-closed reason",
		)
	}
	if targetErr != nil &&
		receipt.FailClosedReason !=
			state.DeleteReconciliationReasonTargetMutationFailed {
		return fmt.Errorf(
			"target delete batch receipt returned with an error without target-atomic fail-closed evidence",
		)
	}
	return nil
}

func validateLowerSHA256(label, value string) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size ||
		value != strings.ToLower(value) {
		return fmt.Errorf(
			"%s must be a lowercase SHA-256 digest",
			label,
		)
	}
	return nil
}

func deleteIncompleteReason(suggested string, primary error) string {
	switch {
	case errors.Is(primary, context.Canceled),
		errors.Is(primary, context.DeadlineExceeded):
		return state.DeleteReconciliationReasonCancelled
	case errors.Is(primary, state.ErrLeaseLost):
		return state.DeleteReconciliationReasonLeaseLost
	default:
		return suggested
	}
}

func (reconciler deleteReconciler) finishIncomplete(
	outcome deleteReconcileOutcome,
	record state.DeleteReconciliation,
	primary error,
	reason string,
	spoolDirectory string,
) (deleteReconcileOutcome, error) {
	if primary == nil {
		primary = errors.New("delete reconciliation incomplete")
	}
	if record.PendingBatch != nil {
		return outcome, errors.Join(
			primary,
			errors.New(
				"cannot terminalize delete reconciliation with an unresolved target receipt",
			),
		)
	}
	if record.LastBatchCommit != nil &&
		record.LastBatchCommit.FailClosedReason != "" {
		// Target-atomic failure evidence is authoritative. In particular, a
		// target may return context cancellation with a durable failed receipt;
		// generic diagnostic classification must not rewrite that receipt.
		reason = record.LastBatchCommit.FailClosedReason
	} else {
		reason = deleteIncompleteReason(reason, primary)
	}
	completedAt, clockErr := reconciler.completionTime(
		record.StartedAt,
	)
	if clockErr != nil {
		primary = errors.Join(primary, clockErr)
		completedAt = record.StartedAt.UTC()
	}
	result := state.DeleteReconciliationResult{
		RunID: record.RunID, Task: record.Task,
		AttemptID:   record.AttemptID,
		Status:      state.DeleteReconciliationIncomplete,
		Candidates:  record.Candidates,
		DeletedRows: record.DeletedRows,
		SkippedRows: record.Candidates - record.DeletedRows,
		Reason:      reason,
		CompletedAt: completedAt,
	}
	if err := reconciler.state.
		FinishDeleteReconciliation(result); err != nil {
		return outcome, errors.Join(
			primary,
			fmt.Errorf(
				"persist incomplete delete reconciliation; repair state and resume this existing run and attempt; do not start a fresh run: %w",
				err,
			),
		)
	}
	outcome.Record = deleteRecordFromResult(record, result)
	if record.Plan != nil {
		if err := removeDeleteSpoolPath(
			spoolDirectory,
			record.Plan.SpoolPath,
		); err != nil {
			return outcome, errors.Join(
				primary,
				fmt.Errorf(
					"terminal delete reconciliation spool cleanup failed: %w",
					err,
				),
			)
		}
	}
	return outcome, primary
}

func terminalDeleteReconcileOutcome(
	record state.DeleteReconciliation,
) (deleteReconcileOutcome, error) {
	if err := state.ValidateDeleteReconciliationEvidence(record); err != nil {
		return deleteReconcileOutcome{}, fmt.Errorf(
			"terminal delete reconciliation is malformed: %w",
			err,
		)
	}
	outcome := deleteReconcileOutcome{
		Record: record,
		DueFacts: deleteDueFacts{
			Due: record.Due, Reason: record.Reason,
		},
		StrictCountValidation: record.Status ==
			state.DeleteReconciliationCompleted,
	}
	if record.Status == state.DeleteReconciliationIncomplete {
		return outcome, fmt.Errorf(
			"delete reconciliation attempt %q is already incomplete: %s",
			record.AttemptID,
			record.Reason,
		)
	}
	return outcome, nil
}

func deleteRecordFromResult(
	record state.DeleteReconciliation,
	result state.DeleteReconciliationResult,
) state.DeleteReconciliation {
	record.Status = result.Status
	record.Candidates = result.Candidates
	record.DeletedRows = result.DeletedRows
	record.SkippedRows = result.SkippedRows
	record.Reason = result.Reason
	record.CompletedAt = result.CompletedAt.UTC()
	return record
}
