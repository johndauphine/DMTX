package migrate

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

const (
	heapPressureNumerator         = uint64(4)
	heapPressureDenominator       = uint64(5)
	defaultHeapCollectionCooldown = 2 * time.Second
)

type heapCollectionCoordinator struct {
	mu         sync.Mutex
	active     chan struct{}
	generation uint64
	last       time.Time
	cooldown   time.Duration
	now        func() time.Time
}

func newHeapCollectionCoordinator(
	cooldown time.Duration,
	now func() time.Time,
) *heapCollectionCoordinator {
	if now == nil {
		now = time.Now
	}
	return &heapCollectionCoordinator{cooldown: cooldown, now: now}
}

func (coordinator *heapCollectionCoordinator) snapshotGeneration() uint64 {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.generation
}

func (coordinator *heapCollectionCoordinator) collect(
	ctx context.Context,
	observedGeneration uint64,
	collector func(),
) (uint64, error) {
	if ctx == nil {
		return 0, fmt.Errorf("%w: nil context", ErrInvalidByteRequest)
	}
	if collector == nil {
		return 0, fmt.Errorf("%w: nil heap collector", ErrInvalidByteRequest)
	}

	coordinator.mu.Lock()
	if coordinator.generation != observedGeneration {
		generation := coordinator.generation
		coordinator.mu.Unlock()
		return generation, nil
	}
	if coordinator.active != nil {
		active := coordinator.active
		coordinator.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-active:
			return coordinator.snapshotGeneration(), nil
		}
	}
	now := coordinator.now()
	if !coordinator.last.IsZero() &&
		coordinator.cooldown > 0 &&
		now.Sub(coordinator.last) < coordinator.cooldown {
		generation := coordinator.generation
		coordinator.mu.Unlock()
		return generation, nil
	}
	active := make(chan struct{})
	coordinator.active = active
	coordinator.mu.Unlock()

	func() {
		defer func() {
			coordinator.mu.Lock()
			coordinator.generation++
			coordinator.last = coordinator.now()
			coordinator.active = nil
			close(active)
			coordinator.mu.Unlock()
		}()
		collector()
	}()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return coordinator.snapshotGeneration(), nil
}

var processHeapCollections = newHeapCollectionCoordinator(
	defaultHeapCollectionCooldown,
	time.Now,
)

type heapPressureBackstop struct {
	budget      uint64
	baseline    uint64
	sample      func() uint64
	collectHeap func()
	coordinator *heapCollectionCoordinator

	mu                    sync.Mutex
	chunkCap              int
	lastReducedGeneration uint64
	hasReducedGeneration  bool
}

func newRuntimeHeapPressureBackstop(budget int64) *heapPressureBackstop {
	sample := func() uint64 {
		var statistics runtime.MemStats
		runtime.ReadMemStats(&statistics)
		return statistics.HeapAlloc
	}
	return &heapPressureBackstop{
		budget:      uint64(budget),
		baseline:    sample(),
		sample:      sample,
		collectHeap: runtime.GC,
		coordinator: processHeapCollections,
	}
}

func (backstop *heapPressureBackstop) adjustedChunkRows(
	ctx context.Context,
	requested int,
) (int, error) {
	if backstop == nil || backstop.sample == nil ||
		backstop.collectHeap == nil || backstop.coordinator == nil {
		return 0, fmt.Errorf("%w: incomplete heap-pressure backstop", ErrInvalidByteBudget)
	}
	if ctx == nil {
		return 0, fmt.Errorf("%w: nil context", ErrInvalidByteRequest)
	}
	if requested <= 0 {
		return 0, fmt.Errorf("%w: chunk rows must be positive", ErrInvalidByteRequest)
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if !backstop.underPressure(backstop.sample()) {
		return backstop.effectiveChunkRows(requested), nil
	}

	observed := backstop.coordinator.snapshotGeneration()
	generation, err := backstop.coordinator.collect(
		ctx,
		observed,
		backstop.collectHeap,
	)
	if err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if backstop.underPressure(backstop.sample()) {
		backstop.reduceChunkRows(generation, requested)
	}
	return backstop.effectiveChunkRows(requested), nil
}

func (backstop *heapPressureBackstop) underPressure(heap uint64) bool {
	if heap <= backstop.baseline || backstop.budget == 0 {
		return false
	}
	transferHeap := heap - backstop.baseline
	threshold := backstop.budget / heapPressureDenominator * heapPressureNumerator
	if threshold == 0 {
		threshold = backstop.budget
	}
	return transferHeap >= threshold
}

func (backstop *heapPressureBackstop) reduceChunkRows(
	generation uint64,
	requested int,
) {
	backstop.mu.Lock()
	defer backstop.mu.Unlock()
	if backstop.hasReducedGeneration &&
		backstop.lastReducedGeneration == generation {
		return
	}
	base := requested
	if backstop.chunkCap > 0 && backstop.chunkCap < base {
		base = backstop.chunkCap
	}
	next := base / 2
	if next < 1 {
		next = 1
	}
	backstop.chunkCap = next
	backstop.lastReducedGeneration = generation
	backstop.hasReducedGeneration = true
}

func (backstop *heapPressureBackstop) effectiveChunkRows(requested int) int {
	backstop.mu.Lock()
	defer backstop.mu.Unlock()
	if backstop.chunkCap > 0 && backstop.chunkCap < requested {
		return backstop.chunkCap
	}
	return requested
}
