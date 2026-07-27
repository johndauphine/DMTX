package app

import (
	"context"
	"sync"
	"time"

	"github.com/johndauphine/DMTX/internal/state"
)

type leaseHeartbeat struct {
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	err    error
}

func startLeaseHeartbeat(parent context.Context, store state.SQLiteStore, lease state.Lease, ttl time.Duration) (context.Context, *leaseHeartbeat) {
	ctx, cancel := context.WithCancel(parent)
	heartbeat := &leaseHeartbeat{cancel: cancel, done: make(chan struct{})}
	interval := ttl / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	go func() {
		defer close(heartbeat.done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := store.RenewLease(lease); err != nil {
					heartbeat.mu.Lock()
					heartbeat.err = err
					heartbeat.mu.Unlock()
					cancel()
					return
				}
			}
		}
	}()
	return ctx, heartbeat
}

func (heartbeat *leaseHeartbeat) Stop() error {
	heartbeat.cancel()
	<-heartbeat.done
	heartbeat.mu.Lock()
	defer heartbeat.mu.Unlock()
	return heartbeat.err
}
