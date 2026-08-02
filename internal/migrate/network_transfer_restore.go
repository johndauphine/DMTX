package migrate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/johndauphine/dmtx/internal/config"
)

// Validating a durable restore and the fact tokens that prove a resumed range
// is the one that was interrupted.

func validateNetworkRestore(
	restore NetworkRangeRestore,
	planned NetworkRangePlan,
	resources config.EffectiveTransferPlan,
) (NetworkRangeRestore, error) {
	if restore.RowsDone < 0 ||
		restore.SequenceOffset < 0 ||
		restore.NextSequence == math.MaxUint64 ||
		len(restore.Frontier) > maximumNetworkFrontierBytes ||
		(restore.RowsDone > 0 || restore.NextSequence > 0) &&
			len(restore.Frontier) == 0 {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: restored frontier is malformed",
			ErrInvalidNetworkRestore,
		)
	}
	if !validNetworkRestoreRowSequenceEvidence(
		restore.RowsDone,
		restore.NextSequence,
	) {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: restored rows and completed sequence count disagree",
			ErrInvalidNetworkRestore,
		)
	}
	if restore.Complete {
		if restore.SequenceOffset != 0 || len(restore.Issued) != 0 {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: completed range retains incomplete work",
				ErrInvalidNetworkRestore,
			)
		}
		return cloneNetworkRestore(restore), nil
	}
	maxIssued := resources.QueueDepth.Value + resources.Writers.Value
	if len(restore.Issued) > maxIssued {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: issued work exceeds the bounded pipeline",
			ErrInvalidNetworkRestore,
		)
	}
	if uint64(len(restore.Issued)) >
		math.MaxUint64-restore.NextSequence-1 {
		return NetworkRangeRestore{}, fmt.Errorf(
			"%w: issued sequence inventory overflows",
			ErrInvalidNetworkRestore,
		)
	}
	for index, issued := range restore.Issued {
		if issued.RangeIndex != planned.RangeIndex ||
			issued.Sequence != restore.NextSequence+uint64(index) ||
			issued.Rows <= 0 ||
			issued.Rows > config.MaxTransferChunkRows ||
			len(issued.StartFrontier) >
				maximumNetworkFrontierBytes ||
			len(issued.EndFrontier) == 0 ||
			len(issued.EndFrontier) >
				maximumNetworkFrontierBytes ||
			!validNetworkFactToken(issued.Fingerprint) ||
			index > 0 && !bytes.Equal(
				issued.StartFrontier,
				restore.Issued[index-1].EndFrontier,
			) ||
			index < len(restore.Issued)-1 && issued.Exhausted {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: issued chunk inventory is malformed",
				ErrInvalidNetworkRestore,
			)
		}
		if index == 0 &&
			!bytes.Equal(issued.StartFrontier, restore.Frontier) {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: issued chunk does not extend the safe frontier",
				ErrInvalidNetworkRestore,
			)
		}
		if int64(issued.Rows) >
			resources.MemoryBudget.Value/planned.MaxRowBytes {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: issued chunk exceeds memory budget",
				ErrInvalidNetworkRestore,
			)
		}
	}
	if restore.SequenceOffset > 0 {
		if len(restore.Issued) == 0 ||
			restore.SequenceOffset >= int64(restore.Issued[0].Rows) {
			return NetworkRangeRestore{}, fmt.Errorf(
				"%w: durable prefix has no matching issued chunk",
				ErrInvalidNetworkRestore,
			)
		}
	}
	return cloneNetworkRestore(restore), nil
}

func validNetworkRestoreRowSequenceEvidence(
	rowsDone int64,
	nextSequence uint64,
) bool {
	if rowsDone == 0 {
		return nextSequence == 0
	}
	if rowsDone < 0 ||
		nextSequence == 0 ||
		nextSequence > uint64(rowsDone) {
		return false
	}
	maxRows := uint64(config.MaxTransferChunkRows)
	completedRows := uint64(rowsDone)
	minimumSequences := completedRows / maxRows
	if completedRows%maxRows != 0 {
		minimumSequences++
	}
	return nextSequence >= minimumSequences
}

func validNetworkIdentifier(value string, allowEmpty bool) bool {
	if value == "" {
		return allowEmpty
	}
	return utf8.ValidString(value) &&
		len(value) <= 512 &&
		!strings.ContainsRune(value, '\x00')
}

func validNetworkFactToken(value string) bool {
	if value == "" || len(value) > maximumNetworkFactToken {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("._:-", character) {
			continue
		}
		return false
	}
	return true
}

func validNetworkPagination(value PaginationStrategy) bool {
	switch value {
	case PaginationIntegerKeyset,
		PaginationTupleKeyset,
		PaginationRowNumber:
		return true
	default:
		return false
	}
}

func cloneNetworkRestore(
	restore NetworkRangeRestore,
) NetworkRangeRestore {
	result := restore
	result.Frontier = cloneNetworkBytes(restore.Frontier)
	result.Issued = make([]NetworkIssuedChunk, len(restore.Issued))
	for index, issued := range restore.Issued {
		result.Issued[index] = cloneNetworkIssuedChunk(issued)
	}
	return result
}

func cloneNetworkIssuedChunk(
	issued NetworkIssuedChunk,
) NetworkIssuedChunk {
	issued.StartFrontier = cloneNetworkBytes(issued.StartFrontier)
	issued.EndFrontier = cloneNetworkBytes(issued.EndFrontier)
	return issued
}

func cloneNetworkBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func (runtime *networkTransferRuntime) liveChunkRows() int {
	if runtime.plan.RuntimeTuning == nil {
		return runtime.plan.Resources.ChunkRows.Value
	}
	return runtime.plan.RuntimeTuning.
		Snapshot().Effective.ChunkRows.Value
}

// liveWriteChunkRows applies an immutable explicit merge ceiling only at the
// target write boundary. Source pagination continues to use liveChunkRows so
// issued pages, source snapshots, and durable replay identities are unchanged.
func (runtime *networkTransferRuntime) liveWriteChunkRows() int {
	rows := runtime.liveChunkRows()
	if runtime.plan.UpsertMergeRows > 0 &&
		runtime.plan.UpsertMergeRows < rows {
		return runtime.plan.UpsertMergeRows
	}
	return rows
}

func (runtime *networkTransferRuntime) liveWriterLimit() int {
	if runtime.plan.RuntimeTuning == nil {
		return runtime.plan.Resources.Writers.Value
	}
	return runtime.plan.RuntimeTuning.
		Snapshot().Effective.Writers.Value
}

func (runtime *networkTransferRuntime) liveQueueLimit() int {
	if runtime.plan.RuntimeTuning == nil {
		return runtime.plan.Resources.QueueDepth.Value
	}
	return runtime.plan.RuntimeTuning.
		Snapshot().Effective.BufferDepth.Value
}

func (runtime *networkTransferRuntime) readRange(
	ctx context.Context,
	state *networkRangeState,
	output chan<- *networkBufferedChunk,
) error {
	sequence := state.restore.NextSequence
	start := cloneNetworkBytes(state.restore.Frontier)
	exhausted := false
	for index, expected := range state.restore.Issued {
		request := NetworkReadRequest{
			Range:          state.plan,
			Sequence:       sequence,
			MaxRows:        expected.Rows,
			StartFrontier:  cloneNetworkBytes(start),
			ReplayExpected: pointerToNetworkIssued(expected),
		}
		chunk, err := runtime.readPage(ctx, state, request)
		if err != nil {
			return err
		}
		chunk.replay = true
		if index == 0 {
			chunk.initialOffset = state.restore.SequenceOffset
		}
		if err := runtime.emitChunk(ctx, output, chunk); err != nil {
			return err
		}
		start = cloneNetworkBytes(expected.EndFrontier)
		sequence++
		exhausted = expected.Exhausted
	}

	for !exhausted {
		maxRows := runtime.liveChunkRows()
		memoryRows := runtime.plan.Resources.MemoryBudget.Value /
			state.plan.MaxRowBytes
		if memoryRows < int64(maxRows) {
			maxRows = int(memoryRows)
		}
		if maxRows < 1 {
			return fmt.Errorf(
				"%w: row reservation admits no source page",
				ErrInvalidNetworkTransferPlan,
			)
		}
		request := NetworkReadRequest{
			Range:         state.plan,
			Sequence:      sequence,
			MaxRows:       maxRows,
			StartFrontier: cloneNetworkBytes(start),
		}
		chunk, err := runtime.readPage(ctx, state, request)
		if err != nil {
			return err
		}
		if len(chunk.page.Rows) == 0 {
			chunk.release()
			exhausted = true
			break
		}
		if err := runtime.emitChunk(ctx, output, chunk); err != nil {
			return err
		}
		start = cloneNetworkBytes(chunk.issued.EndFrontier)
		sequence++
		exhausted = chunk.issued.Exhausted
	}
	if err := state.turn.wait(ctx, sequence); err != nil {
		return err
	}
	if err := runtime.completeRange(ctx, state); err != nil {
		return err
	}
	return state.turn.advance(sequence)
}

func pointerToNetworkIssued(
	value NetworkIssuedChunk,
) *NetworkIssuedChunk {
	cloned := cloneNetworkIssuedChunk(value)
	return &cloned
}

func (runtime *networkTransferRuntime) readPage(
	ctx context.Context,
	state *networkRangeState,
	request NetworkReadRequest,
) (*networkBufferedChunk, error) {
	if request.MaxRows <= 0 ||
		int64(request.MaxRows) >
			runtime.plan.Resources.MemoryBudget.Value/
				state.plan.MaxRowBytes {
		return nil, fmt.Errorf(
			"%w: source request exceeds memory budget",
			ErrInvalidNetworkTransferPlan,
		)
	}
	reservationBytes := int64(request.MaxRows) *
		state.plan.MaxRowBytes
	reservation, err := runtime.budget.Acquire(
		ctx,
		reservationBytes,
	)
	if err != nil {
		return nil, err
	}
	var page NetworkReadPage
	retryRequest := request
	err = RetryWithPolicy(
		ctx,
		runtime.plan.RetryPolicy,
		func(ctx context.Context, attempt int) error {
			retryRequest.Attempt = attempt
			runtime.activity.addReader(1)
			current, readErr := runtime.callbacks.ReadPage(
				ctx,
				cloneNetworkReadRequest(retryRequest),
			)
			runtime.activity.addReader(-1)
			if readErr != nil {
				fact := ClassifyEngineRetry(
					runtime.plan.SourceEngine,
					EngineRetryReadOnly,
					readErr,
				)
				runtime.retryFacts.append(NetworkRetryFact{
					RangeIndex: request.Range.RangeIndex,
					Sequence:   request.Sequence,
					Attempt:    uint32(attempt - 1),
					Operation:  NetworkRetryRead,
					Fact:       fact,
				})
				return NewTransferError(fact.Class, readErr)
			}
			page = current
			return nil
		},
	)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	normalized, err := validateNetworkPage(
		page,
		request,
		state.plan,
		reservation.Bytes(),
	)
	if err != nil {
		reservation.Release()
		return nil, err
	}
	issued := NetworkIssuedChunk{
		RangeIndex:    state.plan.RangeIndex,
		Sequence:      request.Sequence,
		Rows:          len(normalized.Rows),
		StartFrontier: cloneNetworkBytes(request.StartFrontier),
		EndFrontier:   cloneNetworkBytes(normalized.EndFrontier),
		Fingerprint:   normalized.Fingerprint,
		Exhausted:     normalized.Exhausted,
	}
	return &networkBufferedChunk{
		state:       state,
		issued:      issued,
		page:        normalized,
		reservation: reservation,
	}, nil
}

func cloneNetworkReadRequest(
	request NetworkReadRequest,
) NetworkReadRequest {
	request.StartFrontier = cloneNetworkBytes(request.StartFrontier)
	if request.ReplayExpected != nil {
		cloned := cloneNetworkIssuedChunk(*request.ReplayExpected)
		request.ReplayExpected = &cloned
	}
	return request
}

func validateNetworkPage(
	page NetworkReadPage,
	request NetworkReadRequest,
	planned NetworkRangePlan,
	reservedBytes int64,
) (NetworkReadPage, error) {
	if len(page.Rows) > request.MaxRows ||
		len(page.RowBytes) != len(page.Rows) ||
		len(page.EndFrontier) > maximumNetworkFrontierBytes {
		return NetworkReadPage{}, fmt.Errorf(
			"%w: page shape exceeds the bounded request",
			ErrInvalidNetworkPage,
		)
	}
	if len(page.Rows) == 0 {
		if !page.Exhausted ||
			page.RetainedBytes != 0 ||
			page.Fingerprint != "" ||
			len(page.RowBytes) != 0 ||
			request.ReplayExpected != nil {
			return NetworkReadPage{}, fmt.Errorf(
				"%w: empty page must be a new terminal read with no payload facts",
				ErrInvalidNetworkPage,
			)
		}
		page.EndFrontier = cloneNetworkBytes(request.StartFrontier)
		return page, nil
	}
	if !validNetworkFactToken(page.Fingerprint) ||
		len(page.EndFrontier) == 0 ||
		bytes.Equal(page.EndFrontier, request.StartFrontier) {
		return NetworkReadPage{}, fmt.Errorf(
			"%w: non-empty page lacks a stable advancing frontier",
			ErrInvalidNetworkPage,
		)
	}
	total := int64(0)
	for _, retained := range page.RowBytes {
		if retained <= 0 ||
			retained > planned.MaxRowBytes ||
			total > math.MaxInt64-retained {
			return NetworkReadPage{}, fmt.Errorf(
				"%w: retained row bytes are invalid",
				ErrInvalidNetworkPage,
			)
		}
		total += retained
	}
	if total != page.RetainedBytes ||
		total > reservedBytes {
		return NetworkReadPage{}, fmt.Errorf(
			"%w: retained bytes differ from memory admission",
			ErrInvalidNetworkPage,
		)
	}
	if request.ReplayExpected != nil {
		expected := request.ReplayExpected
		if len(page.Rows) != expected.Rows ||
			!bytes.Equal(page.EndFrontier, expected.EndFrontier) ||
			page.Fingerprint != expected.Fingerprint ||
			page.Exhausted != expected.Exhausted {
			return NetworkReadPage{}, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: replay page differs from durable issued intent",
					ErrInvalidNetworkPage,
				),
			)
		}
	}
	// ReadPage relinquishes exclusive ownership of the page. Keep the payload
	// in its admitted backing buffers instead of briefly retaining an
	// unaccounted duplicate beside it.
	return page, nil
}

func (runtime *networkTransferRuntime) emitChunk(
	ctx context.Context,
	output chan<- *networkBufferedChunk,
	chunk *networkBufferedChunk,
) error {
	if err := runtime.queueGate.acquire(
		ctx,
		runtime.liveQueueLimit,
	); err != nil {
		chunk.release()
		return err
	}
	select {
	case <-ctx.Done():
		runtime.queueGate.release()
		chunk.release()
		return ctx.Err()
	case output <- chunk:
		return nil
	}
}

func (runtime *networkTransferRuntime) processChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
) error {
	if err := chunk.state.turn.wait(
		ctx,
		chunk.issued.Sequence,
	); err != nil {
		return err
	}
	if !bytes.Equal(
		chunk.issued.StartFrontier,
		chunk.state.frontier,
	) {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: issued chunk does not extend the durable frontier",
				ErrInvalidNetworkRestore,
			),
		)
	}
	if !chunk.replay {
		runtime.stateMu.Lock()
		err := runtime.callbacks.RecordIssued(
			ctx,
			cloneNetworkIssuedChunk(chunk.issued),
		)
		runtime.stateMu.Unlock()
		if err != nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf("record issued network chunk: %w", err),
			)
		}
	}
	if err := runtime.writeChunk(ctx, chunk); err != nil {
		return err
	}
	return chunk.state.turn.advance(chunk.issued.Sequence)
}

func (runtime *networkTransferRuntime) writeChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
) error {
	offset := chunk.initialOffset
	if offset < 0 || offset >= int64(len(chunk.page.Rows)) {
		return fmt.Errorf(
			"%w: chunk write offset is invalid",
			ErrInvalidNetworkRestore,
		)
	}
	var attempt uint32
	retryFailures := 0
	delay := runtime.plan.RetryPolicy.InitialBackoff
	if runtime.plan.RetryPolicy.MaxBackoff > 0 &&
		delay > runtime.plan.RetryPolicy.MaxBackoff {
		delay = runtime.plan.RetryPolicy.MaxBackoff
	}
	for offset < int64(len(chunk.page.Rows)) {
		if err := ctx.Err(); err != nil {
			return err
		}
		remaining := len(chunk.page.Rows) - int(offset)
		attemptRows := runtime.liveWriteChunkRows()
		if attemptRows > remaining {
			attemptRows = remaining
		}
		if attemptRows < 1 {
			return fmt.Errorf(
				"%w: live chunk limit is not positive",
				ErrInvalidNetworkTransferPlan,
			)
		}
		end := int(offset) + attemptRows
		mode := runtime.writeMode(chunk)
		request := NetworkWriteRequest{
			Range:         chunk.state.plan,
			Sequence:      chunk.issued.Sequence,
			Attempt:       attempt,
			AttemptOffset: offset,
			Mode:          mode,
			Rows:          chunk.page.Rows[int(offset):end],
		}
		if err := runtime.writerGate.acquire(
			ctx,
			runtime.liveWriterLimit,
		); err != nil {
			return err
		}
		receipt, writeErr := runtime.callbacks.WritePage(
			ctx,
			request,
		)
		runtime.writerGate.release()
		protocolLimit := isNetworkWriteProtocolLimit(writeErr)
		attemptBytes, bytesErr := networkRowBytes(
			chunk.page.RowBytes[int(offset):end],
		)
		if bytesErr != nil {
			return bytesErr
		}
		if receiptErr := validateNetworkWriteReceipt(
			receipt,
			offset,
			int64(attemptRows),
		); receiptErr != nil {
			if observeErr := runtime.tuning.observe(
				ctx,
				chunk,
				attemptRows,
				attemptBytes,
				RuntimeWriteFatalError,
			); observeErr != nil {
				return observeErr
			}
			if writeErr != nil && !protocolLimit {
				receiptErr = errors.Join(receiptErr, writeErr)
			}
			return NewTransferError(
				ErrorClassState,
				receiptErr,
			)
		}
		if protocolLimit &&
			receipt.Certainty != CommitNotCommitted {
			if observeErr := runtime.tuning.observe(
				ctx,
				chunk,
				attemptRows,
				attemptBytes,
				RuntimeWriteFatalError,
			); observeErr != nil {
				return observeErr
			}
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: protocol-limit signal requires a not-committed receipt",
					ErrInvalidWriteReceipt,
				),
			)
		}

		fact, classified := runtime.classifyWrite(
			chunk,
			mode,
			receipt,
			writeErr,
		)
		if classified != nil {
			runtime.retryFacts.append(NetworkRetryFact{
				RangeIndex: chunk.issued.RangeIndex,
				Sequence:   chunk.issued.Sequence,
				Attempt:    attempt,
				Operation:  NetworkRetryWrite,
				Fact:       fact,
			})
		}
		acknowledged := receipt.AcknowledgedRows()
		completesChunk :=
			acknowledged > 0 &&
				offset+acknowledged == int64(len(chunk.page.Rows))
		outcome := RuntimeWriteSucceeded
		if classified != nil {
			if protocolLimit &&
				fact.Class == ErrorClassTransient {
				outcome = RuntimeWriteProtocolError
			} else if fact.Class == ErrorClassTransient {
				outcome = RuntimeWriteRetryableError
			} else {
				outcome = RuntimeWriteFatalError
			}
			if completesChunk {
				outcome = RuntimeWriteFatalError
			}
		} else if receipt.Certainty == CommitNotCommitted {
			outcome = RuntimeWriteFatalError
		}
		if err := runtime.tuning.observe(
			ctx,
			chunk,
			attemptRows,
			attemptBytes,
			outcome,
		); err != nil {
			return err
		}

		if acknowledged > 0 {
			frontier, err := chunk.state.tracker.Acknowledge(
				chunk.issued.Sequence,
				int64(len(chunk.page.Rows)),
				receipt,
			)
			if err != nil {
				return NewTransferError(
					ErrorClassState,
					err,
				)
			}
			if err := runtime.checkpointAcknowledgedChunk(
				ctx,
				chunk,
				frontier,
			); err != nil {
				return err
			}
			offset += acknowledged
		}
		if offset == int64(len(chunk.page.Rows)) {
			if classified != nil {
				return classified
			}
			return nil
		}

		switch receipt.Certainty {
		case CommitUnknown:
			if classified != nil {
				return classified
			}
			return NewTransferError(
				ErrorClassState,
				ErrUnknownNetworkCommit,
			)
		case CommitNotCommitted:
			if classified == nil {
				return NewTransferError(
					ErrorClassState,
					fmt.Errorf(
						"%w: target returned no durable progress and no error",
						ErrInvalidWriteReceipt,
					),
				)
			}
		case CommitDurable, CommitDurablePrefix:
			if classified == nil {
				if attempt == math.MaxUint32 {
					return fmt.Errorf(
						"%w: write attempt counter overflow",
						ErrInvalidNetworkTransferPlan,
					)
				}
				attempt++
				continue
			}
		}
		if classified == nil {
			return NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: target write made no usable progress",
					ErrInvalidWriteReceipt,
				),
			)
		}
		if !IsRetryable(classified) ||
			retryFailures >= runtime.plan.RetryPolicy.MaxRetries {
			return classified
		}
		retryFailures++
		if err := waitForRetry(ctx, delay); err != nil {
			return err
		}
		delay = nextRetryDelay(
			delay,
			runtime.plan.RetryPolicy.MaxBackoff,
		)
		if attempt == math.MaxUint32 {
			return fmt.Errorf(
				"%w: write attempt counter overflow",
				ErrInvalidNetworkTransferPlan,
			)
		}
		attempt++
	}
	return nil
}

func validateNetworkWriteReceipt(
	receipt WriteReceipt,
	offset int64,
	attempted int64,
) error {
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf(
			"%w: target returned an invalid write receipt",
			ErrInvalidWriteReceipt,
		)
	}
	if receipt.AttemptOffset != offset ||
		receipt.AttemptedRows != attempted {
		return fmt.Errorf(
			"%w: target receipt does not match the exact attempt",
			ErrInvalidWriteReceipt,
		)
	}
	return nil
}

func networkRowBytes(values []int64) (int64, error) {
	total := int64(0)
	for _, value := range values {
		if value <= 0 || total > math.MaxInt64-value {
			return 0, fmt.Errorf(
				"%w: attempt byte count is invalid",
				ErrInvalidNetworkPage,
			)
		}
		total += value
	}
	return total, nil
}

func (runtime *networkTransferRuntime) writeMode(
	chunk *networkBufferedChunk,
) NetworkWriteMode {
	if runtime.plan.Resources.TargetMode == "upsert" {
		return NetworkWriteIdempotentUpsert
	}
	if chunk.replay {
		return NetworkWriteDuplicateSafeInsertOnly
	}
	return NetworkWriteFreshInsert
}

func (runtime *networkTransferRuntime) classifyWrite(
	chunk *networkBufferedChunk,
	mode NetworkWriteMode,
	receipt WriteReceipt,
	writeErr error,
) (EngineRetryFact, error) {
	if receipt.Certainty == CommitUnknown {
		cause := error(ErrUnknownNetworkCommit)
		if writeErr != nil {
			cause = errors.Join(ErrUnknownNetworkCommit, writeErr)
		}
		fact := ClassifyEngineRetry(
			runtime.plan.TargetEngine,
			EngineRetryUnknownCommit,
			cause,
		)
		return fact, NewTransferError(fact.Class, cause)
	}
	if writeErr == nil {
		return EngineRetryFact{}, nil
	}
	boundary := EngineRetryRolledBack
	if mode == NetworkWriteIdempotentUpsert ||
		mode == NetworkWriteDuplicateSafeInsertOnly {
		boundary = EngineRetryIdempotent
	}
	if isNetworkWriteProtocolLimit(writeErr) {
		class := ErrorClassPermanent
		reason := "runtime_tuning_unavailable"
		if runtime.plan.RuntimeTuning != nil {
			class = ErrorClassTransient
			reason = "runtime_chunk_reduction"
		}
		fact := EngineRetryFact{
			Engine:   runtime.plan.TargetEngine,
			Boundary: boundary,
			Class:    class,
			Code:     "protocol_limit",
			Reason:   reason,
		}
		return fact, NewTransferError(
			class,
			&NetworkWriteProtocolLimitError{},
		)
	}
	fact := ClassifyEngineRetry(
		runtime.plan.TargetEngine,
		boundary,
		writeErr,
	)
	return fact, NewTransferError(fact.Class, writeErr)
}

func isNetworkWriteProtocolLimit(err error) bool {
	var protocolLimit *NetworkWriteProtocolLimitError
	return errors.As(err, &protocolLimit)
}

func (runtime *networkTransferRuntime) checkpointChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
	frontier AckFrontier,
) error {
	if frontier.Rows > math.MaxInt64-chunk.state.baseRows {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: checkpoint row count overflow",
				ErrInvalidAcknowledgement,
			),
		)
	}
	frontier.Rows += chunk.state.baseRows
	frontierBytes := chunk.state.frontier
	if frontier.NextSequence > chunk.issued.Sequence {
		frontierBytes = chunk.issued.EndFrontier
	}
	checkpoint := NetworkRangeCheckpoint{
		RangeIndex:    chunk.state.plan.RangeIndex,
		TopologyHash:  chunk.state.plan.TopologyHash,
		Frontier:      frontier,
		FrontierBytes: cloneNetworkBytes(frontierBytes),
		Memory:        runtime.budget.Stats(),
	}
	runtime.stateMu.Lock()
	err := runtime.callbacks.Checkpoint(ctx, checkpoint)
	runtime.stateMu.Unlock()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("persist network range checkpoint: %w", err),
		)
	}
	chunk.state.safeRows = frontier.Rows
	chunk.state.frontier = cloneNetworkBytes(frontierBytes)
	chunk.state.pendingCheckpointAcks = 0
	return nil
}

// checkpointAcknowledgedChunk applies the configured periodic cadence only
// after ContiguousAckTracker has established the safe range frontier. Deferred
// acknowledgements still advance the in-memory logical frontier so subsequent
// issued pages cannot overlap; they do not change safeRows or durable state.
// On a crash, their already-recorded issued chunks replay through the existing
// idempotent/duplicate-safe route rather than being mistaken for checkpointed
// progress.
func (runtime *networkTransferRuntime) checkpointAcknowledgedChunk(
	ctx context.Context,
	chunk *networkBufferedChunk,
	frontier AckFrontier,
) error {
	if runtime.plan.CheckpointFrequency == 0 {
		return runtime.checkpointChunk(ctx, chunk, frontier)
	}
	chunk.state.pendingCheckpointAcks++
	if chunk.state.pendingCheckpointAcks >=
		runtime.plan.CheckpointFrequency {
		return runtime.checkpointChunk(ctx, chunk, frontier)
	}
	// A partial prefix has no new typed end frontier. For a complete chunk,
	// preserve its end frontier locally so the next issued sequence proves it
	// extends the same logical source position even though persistence waits for
	// the next cadence boundary.
	if frontier.NextSequence > chunk.issued.Sequence {
		chunk.state.frontier = cloneNetworkBytes(chunk.issued.EndFrontier)
	}
	return nil
}

func (runtime *networkTransferRuntime) completeRange(
	ctx context.Context,
	state *networkRangeState,
) error {
	frontier := state.tracker.Frontier()
	if frontier.NextSequence != state.turn.next ||
		frontier.SequenceOffset != 0 {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: range completion has an incomplete durable frontier",
				ErrInvalidAcknowledgement,
			),
		)
	}
	if frontier.Rows > math.MaxInt64-state.baseRows {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf(
				"%w: completion row count overflow",
				ErrInvalidAcknowledgement,
			),
		)
	}
	frontier.Rows += state.baseRows
	checkpoint := NetworkRangeCheckpoint{
		RangeIndex:    state.plan.RangeIndex,
		TopologyHash:  state.plan.TopologyHash,
		Frontier:      frontier,
		FrontierBytes: cloneNetworkBytes(state.frontier),
		Complete:      true,
		Memory:        runtime.budget.Stats(),
	}
	runtime.stateMu.Lock()
	err := runtime.callbacks.Checkpoint(ctx, checkpoint)
	runtime.stateMu.Unlock()
	if err != nil {
		return NewTransferError(
			ErrorClassState,
			fmt.Errorf("persist network range completion: %w", err),
		)
	}
	state.safeRows = frontier.Rows
	state.pendingCheckpointAcks = 0
	state.complete = true
	return nil
}

func (runtime *networkTransferRuntime) result(
	states []*networkRangeState,
) (NetworkTransferResult, error) {
	result := NetworkTransferResult{
		Pagination: make(
			[]NetworkPaginationFact,
			0,
			len(states),
		),
		Retries: runtime.retryFacts.snapshot(),
		Memory:  runtime.budget.Stats(),
	}
	for _, state := range states {
		if state.safeRows > math.MaxInt64-result.Rows {
			return result, NewTransferError(
				ErrorClassState,
				fmt.Errorf(
					"%w: aggregate checkpoint row count overflow",
					ErrInvalidAcknowledgement,
				),
			)
		}
		result.Rows += state.safeRows
		if state.complete {
			result.CompletedRanges++
		}
		result.Pagination = append(
			result.Pagination,
			NetworkPaginationFact{
				RangeIndex:   state.plan.RangeIndex,
				TableSchema:  state.plan.TableSchema,
				TableName:    state.plan.TableName,
				TopologyHash: state.plan.TopologyHash,
				Pagination:   state.plan.Pagination,
			},
		)
	}
	if runtime.plan.RuntimeTuning != nil {
		result.HasRuntimeTuning = true
		result.RuntimeTuning =
			runtime.plan.RuntimeTuning.Snapshot()
	}
	return result, nil
}
