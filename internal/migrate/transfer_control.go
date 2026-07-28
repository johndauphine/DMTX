package migrate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

var (
	ErrInvalidByteBudget        = errors.New("invalid byte budget")
	ErrInvalidByteRequest       = errors.New("invalid byte request")
	ErrByteRequestExceedsBudget = errors.New("byte request exceeds budget")
	ErrInvalidWriteReceipt      = errors.New("invalid write receipt")
	ErrInvalidAcknowledgement   = errors.New("invalid acknowledgement")
	ErrInvalidRetryPolicy       = errors.New("invalid retry policy")
	ErrNilRetryOperation        = errors.New("nil retry operation")
)

// ByteBudget limits the exact number of retained row bytes across a migration.
// Callers must pass the exact encoded or retained byte count; ByteBudget never
// estimates it. A request larger than the limit is rejected rather than being
// admitted above the limit.
type ByteBudget struct {
	mu       sync.Mutex
	limit    int64
	current  int64
	peak     int64
	changed  chan struct{}
	pressure *heapPressureBackstop
}

// ByteBudgetStats is an atomic snapshot of a ByteBudget.
type ByteBudgetStats struct {
	Limit   int64
	Current int64
	Peak    int64
}

// NewByteBudget constructs a migration-wide byte admission budget.
func NewByteBudget(limit int64) (*ByteBudget, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("%w: limit must be positive", ErrInvalidByteBudget)
	}
	return &ByteBudget{
		limit:    limit,
		changed:  make(chan struct{}),
		pressure: newRuntimeHeapPressureBackstop(limit),
	}, nil
}

// Acquire waits until exactBytes can be admitted. The returned reservation is
// released automatically if ctx is cancelled, and Release is idempotent.
func (b *ByteBudget) Acquire(ctx context.Context, exactBytes int64) (*ByteReservation, error) {
	if b == nil {
		return nil, fmt.Errorf("%w: nil budget", ErrInvalidByteBudget)
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidByteRequest)
	}
	if exactBytes < 0 {
		return nil, fmt.Errorf("%w: bytes must be non-negative", ErrInvalidByteRequest)
	}
	if exactBytes > b.limit {
		return nil, fmt.Errorf(
			"%w: request=%d limit=%d",
			ErrByteRequestExceedsBudget,
			exactBytes,
			b.limit,
		)
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		b.mu.Lock()
		if exactBytes <= b.limit-b.current {
			b.current += exactBytes
			if b.current > b.peak {
				b.peak = b.current
			}
			reservation := &ByteReservation{
				budget:   b,
				bytes:    exactBytes,
				released: make(chan struct{}),
			}
			b.mu.Unlock()

			if err := ctx.Err(); err != nil {
				reservation.Release()
				return nil, err
			}
			if ctx.Done() != nil {
				go reservation.releaseOnCancellation(ctx)
			}
			return reservation, nil
		}
		changed := b.changed
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

// TryAcquire admits exactBytes without waiting. A false result means the
// request is valid but currently lacks capacity. Oversize requests still fail
// closed so a reader can distinguish backpressure from an impossible row.
func (b *ByteBudget) TryAcquire(ctx context.Context, exactBytes int64) (*ByteReservation, bool, error) {
	if b == nil {
		return nil, false, fmt.Errorf("%w: nil budget", ErrInvalidByteBudget)
	}
	if ctx == nil {
		return nil, false, fmt.Errorf("%w: nil context", ErrInvalidByteRequest)
	}
	if exactBytes < 0 {
		return nil, false, fmt.Errorf("%w: bytes must be non-negative", ErrInvalidByteRequest)
	}
	if exactBytes > b.limit {
		return nil, false, fmt.Errorf(
			"%w: request=%d limit=%d",
			ErrByteRequestExceedsBudget,
			exactBytes,
			b.limit,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	b.mu.Lock()
	if exactBytes > b.limit-b.current {
		b.mu.Unlock()
		return nil, false, nil
	}
	b.current += exactBytes
	if b.current > b.peak {
		b.peak = b.current
	}
	reservation := &ByteReservation{
		budget:   b,
		bytes:    exactBytes,
		released: make(chan struct{}),
	}
	b.mu.Unlock()

	if err := ctx.Err(); err != nil {
		reservation.Release()
		return nil, false, err
	}
	if ctx.Done() != nil {
		go reservation.releaseOnCancellation(ctx)
	}
	return reservation, true, nil
}

// Stats returns the current usage, peak usage, and configured limit.
func (b *ByteBudget) Stats() ByteBudgetStats {
	if b == nil {
		return ByteBudgetStats{}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return ByteBudgetStats{
		Limit:   b.limit,
		Current: b.current,
		Peak:    b.peak,
	}
}

func (b *ByteBudget) adjustedChunkRows(
	ctx context.Context,
	requested int,
) (int, error) {
	if b == nil || b.pressure == nil {
		return 0, fmt.Errorf("%w: nil budget", ErrInvalidByteBudget)
	}
	return b.pressure.adjustedChunkRows(ctx, requested)
}

// ByteReservation owns bytes admitted by a ByteBudget.
type ByteReservation struct {
	budget   *ByteBudget
	bytes    int64
	once     sync.Once
	released chan struct{}
}

// Bytes returns the exact number of bytes owned by the reservation.
func (r *ByteReservation) Bytes() int64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

// Release returns the reservation to its budget. It is safe to call more than
// once and may race with context cancellation.
func (r *ByteReservation) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.budget.mu.Lock()
		r.budget.current -= r.bytes
		changed := r.budget.changed
		r.budget.changed = make(chan struct{})
		close(changed)
		r.budget.mu.Unlock()
		close(r.released)
	})
}

func (r *ByteReservation) releaseOnCancellation(ctx context.Context) {
	select {
	case <-ctx.Done():
		r.Release()
	case <-r.released:
	}
}

// CommitCertainty describes what a destination writer knows about a write.
type CommitCertainty string

const (
	CommitUnknown       CommitCertainty = "unknown"
	CommitNotCommitted  CommitCertainty = "not_committed"
	CommitDurable       CommitCertainty = "durable"
	CommitDurablePrefix CommitCertainty = "durable_prefix"
)

// WriteReceipt reports the durable outcome of one write attempt. AttemptOffset
// is relative to the start of its range chunk and makes suffix retries
// unambiguous.
type WriteReceipt struct {
	Certainty     CommitCertainty
	AttemptOffset int64
	AttemptedRows int64
	CommittedRows int64
}

// Validate rejects internally inconsistent commit claims.
func (r WriteReceipt) Validate() error {
	if r.AttemptOffset < 0 || r.AttemptedRows < 0 || r.CommittedRows < 0 {
		return fmt.Errorf("%w: row counts must be non-negative", ErrInvalidWriteReceipt)
	}
	if r.CommittedRows > r.AttemptedRows {
		return fmt.Errorf("%w: committed rows exceed attempted rows", ErrInvalidWriteReceipt)
	}

	switch r.Certainty {
	case CommitUnknown, CommitNotCommitted:
		if r.CommittedRows != 0 {
			return fmt.Errorf(
				"%w: %s receipt cannot acknowledge rows",
				ErrInvalidWriteReceipt,
				r.Certainty,
			)
		}
	case CommitDurable:
		if r.CommittedRows != r.AttemptedRows {
			return fmt.Errorf(
				"%w: durable receipt must commit the full attempt",
				ErrInvalidWriteReceipt,
			)
		}
	case CommitDurablePrefix:
		if r.CommittedRows <= 0 || r.CommittedRows >= r.AttemptedRows {
			return fmt.Errorf(
				"%w: durable-prefix receipt must commit a strict non-empty prefix",
				ErrInvalidWriteReceipt,
			)
		}
	default:
		return fmt.Errorf("%w: unknown commit certainty %q", ErrInvalidWriteReceipt, r.Certainty)
	}
	return nil
}

// AcknowledgedRows returns the rows this receipt can safely acknowledge.
func (r WriteReceipt) AcknowledgedRows() int64 {
	switch r.Certainty {
	case CommitDurable, CommitDurablePrefix:
		return r.CommittedRows
	default:
		return 0
	}
}

type acknowledgedChunk struct {
	total    int64
	durable  int64
	reported int64
}

// AckFrontier is the durable contiguous frontier for one range. NextSequence
// is the first incomplete chunk and SequenceOffset is its durable prefix.
type AckFrontier struct {
	RangeID        string
	NextSequence   uint64
	SequenceOffset int64
	Rows           int64
}

// ContiguousAckTracker prevents out-of-order durable receipts from advancing a
// range past a gap.
type ContiguousAckTracker struct {
	mu      sync.Mutex
	rangeID string
	next    uint64
	rows    int64
	chunks  map[uint64]*acknowledgedChunk
}

// NewContiguousAckTracker starts a tracker at firstSequence.
func NewContiguousAckTracker(rangeID string, firstSequence uint64) *ContiguousAckTracker {
	return &ContiguousAckTracker{
		rangeID: rangeID,
		next:    firstSequence,
		chunks:  make(map[uint64]*acknowledgedChunk),
	}
}

// Acknowledge records a receipt for sequence. Repeated writes must begin at the
// currently durable prefix, preventing overlapping retries from double-counting.
func (t *ContiguousAckTracker) Acknowledge(
	sequence uint64,
	chunkRows int64,
	receipt WriteReceipt,
) (AckFrontier, error) {
	if t == nil {
		return AckFrontier{}, fmt.Errorf("%w: nil tracker", ErrInvalidAcknowledgement)
	}
	if chunkRows <= 0 {
		return AckFrontier{}, fmt.Errorf("%w: chunk rows must be positive", ErrInvalidAcknowledgement)
	}
	if err := receipt.Validate(); err != nil {
		return AckFrontier{}, fmt.Errorf("%w: %v", ErrInvalidAcknowledgement, err)
	}
	if receipt.AttemptOffset > chunkRows ||
		receipt.AttemptedRows > chunkRows-receipt.AttemptOffset {
		return AckFrontier{}, fmt.Errorf(
			"%w: attempt lies outside chunk",
			ErrInvalidAcknowledgement,
		)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if sequence < t.next {
		return t.frontierLocked(), fmt.Errorf(
			"%w: sequence %d is behind frontier %d",
			ErrInvalidAcknowledgement,
			sequence,
			t.next,
		)
	}

	acknowledged := receipt.AcknowledgedRows()
	if acknowledged == 0 {
		return t.frontierLocked(), nil
	}

	chunk, ok := t.chunks[sequence]
	if !ok {
		if receipt.AttemptOffset != 0 {
			return t.frontierLocked(), fmt.Errorf(
				"%w: first receipt for sequence %d starts at offset %d",
				ErrInvalidAcknowledgement,
				sequence,
				receipt.AttemptOffset,
			)
		}
		chunk = &acknowledgedChunk{total: chunkRows}
		t.chunks[sequence] = chunk
	} else if chunk.total != chunkRows {
		return t.frontierLocked(), fmt.Errorf(
			"%w: sequence %d changed size from %d to %d",
			ErrInvalidAcknowledgement,
			sequence,
			chunk.total,
			chunkRows,
		)
	}

	if receipt.AttemptOffset != chunk.durable {
		return t.frontierLocked(), fmt.Errorf(
			"%w: sequence %d expected offset %d, got %d",
			ErrInvalidAcknowledgement,
			sequence,
			chunk.durable,
			receipt.AttemptOffset,
		)
	}
	if acknowledged > chunk.total-chunk.durable {
		return t.frontierLocked(), fmt.Errorf(
			"%w: acknowledgement exceeds chunk",
			ErrInvalidAcknowledgement,
		)
	}
	chunk.durable += acknowledged

	if err := t.advanceLocked(); err != nil {
		return t.frontierLocked(), err
	}
	return t.frontierLocked(), nil
}

// Frontier returns an atomic snapshot of the contiguous durable frontier.
func (t *ContiguousAckTracker) Frontier() AckFrontier {
	if t == nil {
		return AckFrontier{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.frontierLocked()
}

func (t *ContiguousAckTracker) advanceLocked() error {
	for {
		chunk, ok := t.chunks[t.next]
		if !ok {
			return nil
		}
		delta := chunk.durable - chunk.reported
		if delta > math.MaxInt64-t.rows {
			return fmt.Errorf("%w: acknowledged row count overflow", ErrInvalidAcknowledgement)
		}
		t.rows += delta
		chunk.reported = chunk.durable
		if chunk.durable < chunk.total {
			return nil
		}
		if t.next == math.MaxUint64 {
			return fmt.Errorf("%w: sequence overflow", ErrInvalidAcknowledgement)
		}
		delete(t.chunks, t.next)
		t.next++
	}
}

func (t *ContiguousAckTracker) frontierLocked() AckFrontier {
	offset := int64(0)
	if chunk, ok := t.chunks[t.next]; ok {
		offset = chunk.reported
	}
	return AckFrontier{
		RangeID:        t.rangeID,
		NextSequence:   t.next,
		SequenceOffset: offset,
		Rows:           t.rows,
	}
}

// TransferErrorClass is a stable retry classification.
type TransferErrorClass string

const (
	ErrorClassTransient  TransferErrorClass = "transient"
	ErrorClassConversion TransferErrorClass = "conversion"
	ErrorClassPolicy     TransferErrorClass = "policy"
	ErrorClassPrimaryKey TransferErrorClass = "primary_key"
	ErrorClassValidation TransferErrorClass = "validation"
	ErrorClassLease      TransferErrorClass = "lease"
	ErrorClassState      TransferErrorClass = "state"
	ErrorClassPermanent  TransferErrorClass = "permanent"
	ErrorClassCanceled   TransferErrorClass = "canceled"
)

// TransferError attaches a stable retry class without discarding the cause.
type TransferError struct {
	Class TransferErrorClass
	Err   error
}

func (e *TransferError) Error() string {
	if e == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s transfer error: %v", e.Class, e.Err)
}

func (e *TransferError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// TransferErrorClass exposes the classification to ClassifyTransferError.
func (e *TransferError) TransferErrorClass() TransferErrorClass {
	if e == nil {
		return ErrorClassPermanent
	}
	return e.Class
}

// NewTransferError wraps err with class. A nil cause remains nil.
func NewTransferError(class TransferErrorClass, err error) error {
	if err == nil {
		return nil
	}
	return &TransferError{Class: class, Err: err}
}

type transferErrorClassifier interface {
	TransferErrorClass() TransferErrorClass
}

// ClassifyTransferError is deliberately conservative: only an explicit
// transient classification is retryable, while unclassified errors are
// permanent.
func ClassifyTransferError(err error) TransferErrorClass {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return ErrorClassCanceled
	}
	var classified transferErrorClassifier
	if errors.As(err, &classified) {
		class := classified.TransferErrorClass()
		if isKnownTransferErrorClass(class) {
			return class
		}
	}
	return ErrorClassPermanent
}

// IsRetryable reports whether err is explicitly transient.
func IsRetryable(err error) bool {
	return ClassifyTransferError(err) == ErrorClassTransient
}

func isKnownTransferErrorClass(class TransferErrorClass) bool {
	switch class {
	case ErrorClassTransient,
		ErrorClassConversion,
		ErrorClassPolicy,
		ErrorClassPrimaryKey,
		ErrorClassValidation,
		ErrorClassLease,
		ErrorClassState,
		ErrorClassPermanent,
		ErrorClassCanceled:
		return true
	default:
		return false
	}
}

// DefaultMaxRetries is the number of retries after the initial attempt.
const DefaultMaxRetries = 3

// RetryPolicy controls bounded exponential retry.
type RetryPolicy struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultRetryPolicy returns three retries with a bounded exponential delay.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:     DefaultMaxRetries,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}
}

// RetryOperation receives a one-based attempt number.
type RetryOperation func(context.Context, int) error

// Retry executes operation once plus, for explicit transient failures, the
// default three retries.
func Retry(ctx context.Context, operation RetryOperation) error {
	return RetryWithPolicy(ctx, DefaultRetryPolicy(), operation)
}

// RetryWithPolicy executes a cancellation-aware bounded exponential retry.
func RetryWithPolicy(
	ctx context.Context,
	policy RetryPolicy,
	operation RetryOperation,
) error {
	if ctx == nil {
		return fmt.Errorf("%w: nil context", ErrInvalidRetryPolicy)
	}
	if operation == nil {
		return ErrNilRetryOperation
	}
	if policy.MaxRetries < 0 || policy.InitialBackoff < 0 || policy.MaxBackoff < 0 {
		return fmt.Errorf("%w: retry counts and durations must be non-negative", ErrInvalidRetryPolicy)
	}

	delay := policy.InitialBackoff
	if policy.MaxBackoff > 0 && delay > policy.MaxBackoff {
		delay = policy.MaxBackoff
	}

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation(ctx, attempt)
		if err == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !IsRetryable(err) || attempt > policy.MaxRetries {
			return err
		}
		if err := waitForRetry(ctx, delay); err != nil {
			return err
		}
		delay = nextRetryDelay(delay, policy.MaxBackoff)
	}
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if delay == 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func nextRetryDelay(current, maximum time.Duration) time.Duration {
	if current == 0 {
		return 0
	}
	if maximum > 0 && current >= maximum {
		return maximum
	}
	if current > time.Duration(math.MaxInt64/2) {
		if maximum > 0 {
			return maximum
		}
		return time.Duration(math.MaxInt64)
	}
	next := current * 2
	if maximum > 0 && next > maximum {
		return maximum
	}
	return next
}
