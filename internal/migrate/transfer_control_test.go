package migrate

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestByteBudgetExactUsagePeakAndOversizePolicy(t *testing.T) {
	budget, err := NewByteBudget(10)
	if err != nil {
		t.Fatal(err)
	}

	first, err := budget.Acquire(context.Background(), 6)
	if err != nil {
		t.Fatal(err)
	}
	if got := budget.Stats(); got != (ByteBudgetStats{Limit: 10, Current: 6, Peak: 6}) {
		t.Fatalf("unexpected initial stats: %+v", got)
	}

	started := make(chan struct{})
	acquired := make(chan *ByteReservation, 1)
	go func() {
		close(started)
		reservation, acquireErr := budget.Acquire(context.Background(), 5)
		if acquireErr != nil {
			return
		}
		acquired <- reservation
	}()
	<-started
	select {
	case reservation := <-acquired:
		reservation.Release()
		t.Fatal("request was admitted above the byte limit")
	case <-time.After(20 * time.Millisecond):
	}

	first.Release()
	var second *ByteReservation
	select {
	case second = <-acquired:
	case <-time.After(time.Second):
		t.Fatal("blocked acquisition did not resume after release")
	}
	if got := budget.Stats(); got != (ByteBudgetStats{Limit: 10, Current: 5, Peak: 6}) {
		t.Fatalf("unexpected resumed stats: %+v", got)
	}
	second.Release()

	if _, err := budget.Acquire(context.Background(), 11); !errors.Is(err, ErrByteRequestExceedsBudget) {
		t.Fatalf("oversize request error = %v", err)
	}
	if got := budget.Stats(); got.Current != 0 || got.Peak != 6 {
		t.Fatalf("unexpected final stats: %+v", got)
	}
}

func TestByteBudgetCancellationReleasesAndUnblocks(t *testing.T) {
	budget, err := NewByteBudget(10)
	if err != nil {
		t.Fatal(err)
	}
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	owner, err := budget.Acquire(ownerCtx, 10)
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan struct {
		reservation *ByteReservation
		err         error
	}, 1)
	go func() {
		reservation, acquireErr := budget.Acquire(context.Background(), 4)
		result <- struct {
			reservation *ByteReservation
			err         error
		}{reservation: reservation, err: acquireErr}
	}()

	select {
	case got := <-result:
		if got.reservation != nil {
			got.reservation.Release()
		}
		t.Fatalf("waiter unexpectedly completed before cancellation: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}

	cancelOwner()
	var waiter *ByteReservation
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("waiter failed: %v", got.err)
		}
		waiter = got.reservation
	case <-time.After(time.Second):
		t.Fatal("cancellation did not release bytes and unblock waiter")
	}
	if got := budget.Stats(); got.Current != 4 || got.Peak != 10 {
		t.Fatalf("unexpected stats after cancellation: %+v", got)
	}

	owner.Release()
	waiter.Release()
	waiter.Release()
	if got := budget.Stats(); got.Current != 0 || got.Peak != 10 {
		t.Fatalf("release was not idempotent: %+v", got)
	}
}

func TestByteBudgetWaitingCancellationAcquiresNothing(t *testing.T) {
	budget, err := NewByteBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := budget.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := budget.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want context.Canceled", err)
	}
	if got := budget.Stats().Current; got != 1 {
		t.Fatalf("cancelled acquisition changed current bytes to %d", got)
	}
}

func TestContiguousAckTrackerHoldsOutOfOrderReceipt(t *testing.T) {
	tracker := NewContiguousAckTracker("range-a", 0)

	frontier, err := tracker.Acknowledge(1, 3, WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: 3,
		CommittedRows: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if frontier.NextSequence != 0 || frontier.Rows != 0 {
		t.Fatalf("out-of-order receipt advanced frontier: %+v", frontier)
	}

	frontier, err = tracker.Acknowledge(0, 2, WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: 2,
		CommittedRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := AckFrontier{RangeID: "range-a", NextSequence: 2, Rows: 5}
	if frontier != want {
		t.Fatalf("frontier = %+v, want %+v", frontier, want)
	}
}

func TestContiguousAckTrackerCommittedPrefixAndSuffixRetry(t *testing.T) {
	tracker := NewContiguousAckTracker("range-b", 0)

	frontier, err := tracker.Acknowledge(0, 5, WriteReceipt{
		Certainty:     CommitDurablePrefix,
		AttemptedRows: 5,
		CommittedRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := AckFrontier{
		RangeID:        "range-b",
		NextSequence:   0,
		SequenceOffset: 2,
		Rows:           2,
	}
	if frontier != wantPrefix {
		t.Fatalf("prefix frontier = %+v, want %+v", frontier, wantPrefix)
	}

	frontier, err = tracker.Acknowledge(1, 4, WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: 4,
		CommittedRows: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if frontier != wantPrefix {
		t.Fatalf("later receipt advanced past prefix gap: %+v", frontier)
	}

	frontier, err = tracker.Acknowledge(0, 5, WriteReceipt{
		Certainty:     CommitDurable,
		AttemptOffset: 2,
		AttemptedRows: 3,
		CommittedRows: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantComplete := AckFrontier{RangeID: "range-b", NextSequence: 2, Rows: 9}
	if frontier != wantComplete {
		t.Fatalf("completed frontier = %+v, want %+v", frontier, wantComplete)
	}
}

func TestContiguousAckTrackerDoesNotAcknowledgeUncertainCommit(t *testing.T) {
	tracker := NewContiguousAckTracker("range-c", 7)
	frontier, err := tracker.Acknowledge(7, 2, WriteReceipt{
		Certainty:     CommitUnknown,
		AttemptedRows: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if frontier != (AckFrontier{RangeID: "range-c", NextSequence: 7}) {
		t.Fatalf("uncertain receipt advanced frontier: %+v", frontier)
	}
}

func TestWriteReceiptRejectsInconsistentCertainty(t *testing.T) {
	tests := []WriteReceipt{
		{Certainty: CommitUnknown, AttemptedRows: 2, CommittedRows: 1},
		{Certainty: CommitNotCommitted, AttemptedRows: 2, CommittedRows: 1},
		{Certainty: CommitDurable, AttemptedRows: 2, CommittedRows: 1},
		{Certainty: CommitDurablePrefix, AttemptedRows: 2, CommittedRows: 2},
	}
	for _, receipt := range tests {
		if err := receipt.Validate(); !errors.Is(err, ErrInvalidWriteReceipt) {
			t.Fatalf("Validate(%+v) error = %v", receipt, err)
		}
	}
}

func TestRetryUsesDefaultThreeRetryBudget(t *testing.T) {
	attempts := 0
	err := Retry(context.Background(), func(context.Context, int) error {
		attempts++
		return NewTransferError(ErrorClassTransient, errors.New("temporary"))
	})
	if err == nil {
		t.Fatal("Retry unexpectedly succeeded")
	}
	if attempts != 1+DefaultMaxRetries {
		t.Fatalf("attempts = %d, want %d", attempts, 1+DefaultMaxRetries)
	}
	if got := DefaultRetryPolicy().MaxRetries; got != 3 {
		t.Fatalf("default MaxRetries = %d, want 3", got)
	}
}

func TestRetryStopsForStableNonTransientClasses(t *testing.T) {
	classes := []TransferErrorClass{
		ErrorClassConversion,
		ErrorClassPolicy,
		ErrorClassPrimaryKey,
		ErrorClassValidation,
		ErrorClassLease,
		ErrorClassState,
	}
	for _, class := range classes {
		t.Run(string(class), func(t *testing.T) {
			attempts := 0
			err := RetryWithPolicy(
				context.Background(),
				RetryPolicy{MaxRetries: 10},
				func(context.Context, int) error {
					attempts++
					return NewTransferError(class, fmt.Errorf("%s failure", class))
				},
			)
			if err == nil {
				t.Fatal("RetryWithPolicy unexpectedly succeeded")
			}
			if attempts != 1 {
				t.Fatalf("attempts = %d, want 1", attempts)
			}
			if got := ClassifyTransferError(err); got != class {
				t.Fatalf("class = %q, want %q", got, class)
			}
		})
	}
}

func TestRetryTransientSuccessAndCancellationDuringBackoff(t *testing.T) {
	attempts := 0
	err := RetryWithPolicy(
		context.Background(),
		RetryPolicy{MaxRetries: 3},
		func(context.Context, int) error {
			attempts++
			if attempts < 3 {
				return NewTransferError(ErrorClassTransient, errors.New("temporary"))
			}
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	attempts = 0
	started := time.Now()
	err = RetryWithPolicy(
		ctx,
		RetryPolicy{
			MaxRetries:     3,
			InitialBackoff: time.Hour,
			MaxBackoff:     time.Hour,
		},
		func(context.Context, int) error {
			attempts++
			cancel()
			return NewTransferError(ErrorClassTransient, errors.New("temporary"))
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts after cancellation = %d, want 1", attempts)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cancellation took too long: %s", elapsed)
	}
}
