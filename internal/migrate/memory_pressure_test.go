package migrate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHeapPressureBackstopCollectsOnceAndReducesAtChunkBoundary(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	coordinator := newHeapCollectionCoordinator(time.Hour, func() time.Time {
		return now
	})
	var collections atomic.Int64
	collectionStarted := make(chan struct{})
	releaseCollection := make(chan struct{})
	var startOnce sync.Once
	backstop := &heapPressureBackstop{
		budget:   1_000,
		baseline: 100,
		sample: func() uint64 {
			return 1_000
		},
		collectHeap: func() {
			collections.Add(1)
			startOnce.Do(func() { close(collectionStarted) })
			<-releaseCollection
		},
		coordinator: coordinator,
	}

	const callers = 8
	results := make(chan int, callers)
	errorsChannel := make(chan error, callers)
	var waiters sync.WaitGroup
	waiters.Add(callers)
	for index := 0; index < callers; index++ {
		go func() {
			defer waiters.Done()
			rows, err := backstop.adjustedChunkRows(context.Background(), 100)
			results <- rows
			errorsChannel <- err
		}()
	}
	<-collectionStarted
	close(releaseCollection)
	waiters.Wait()
	close(results)
	close(errorsChannel)

	if got := collections.Load(); got != 1 {
		t.Fatalf("heap collections = %d, want one process-wide collection", got)
	}
	for err := range errorsChannel {
		if err != nil {
			t.Fatal(err)
		}
	}
	for rows := range results {
		if rows != 50 {
			t.Fatalf("adjusted chunk rows = %d, want 50", rows)
		}
	}

	rows, err := backstop.adjustedChunkRows(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 50 || collections.Load() != 1 {
		t.Fatalf("cooldown result rows=%d collections=%d, want 50 and 1", rows, collections.Load())
	}
}

func TestHeapPressureBackstopDoesNotReduceWhenCollectionRelievesPressure(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	var heap atomic.Uint64
	heap.Store(1_000)
	backstop := &heapPressureBackstop{
		budget:   1_000,
		baseline: 100,
		sample:   heap.Load,
		collectHeap: func() {
			heap.Store(200)
		},
		coordinator: newHeapCollectionCoordinator(time.Hour, func() time.Time {
			return now
		}),
	}

	rows, err := backstop.adjustedChunkRows(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if rows != 100 {
		t.Fatalf("adjusted chunk rows = %d, want unchanged 100", rows)
	}
}

func TestHeapPressureBackstopCancellationWhileCollectionInFlight(t *testing.T) {
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	coordinator := newHeapCollectionCoordinator(time.Hour, func() time.Time {
		return now
	})
	collectionStarted := make(chan struct{})
	releaseCollection := make(chan struct{})
	backstop := &heapPressureBackstop{
		budget:   1_000,
		baseline: 100,
		sample: func() uint64 {
			return 1_000
		},
		collectHeap: func() {
			close(collectionStarted)
			<-releaseCollection
		},
		coordinator: coordinator,
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := backstop.adjustedChunkRows(context.Background(), 100)
		firstDone <- err
	}()
	<-collectionStarted

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backstop.adjustedChunkRows(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled pressure check error = %v", err)
	}
	close(releaseCollection)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
