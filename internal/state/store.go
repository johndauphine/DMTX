// Package state persists durable migration run history and restart checkpoints.
package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type Outcome string

const (
	Running Outcome = "running"
	Success Outcome = "success"
	Failed  Outcome = "failed"
)

type Run struct {
	ID        string    `json:"id"`
	Source    string    `json:"source"`
	Target    string    `json:"target"`
	Outcome   Outcome   `json:"outcome"`
	Resumable bool      `json:"resumable"`
	Reason    string    `json:"resumability_reason"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
}

type Store struct{ Path string }

func (store Store) Append(run Run) error {
	runs, err := store.List()
	if err != nil {
		return err
	}
	runs = append(runs, run)
	data, err := json.MarshalIndent(runs, "", "  ")
	if err != nil {
		return fmt.Errorf("encode run state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(store.Path), 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	if err := os.WriteFile(store.Path, data, 0o600); err != nil {
		return fmt.Errorf("write run state: %w", err)
	}
	return nil
}

func (store Store) List() ([]Run, error) {
	data, err := os.ReadFile(store.Path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read run state: %w", err)
	}
	var runs []Run
	if err := json.Unmarshal(data, &runs); err != nil {
		return nil, fmt.Errorf("decode run state: %w", err)
	}
	return runs, nil
}

func (store Store) Latest() (Run, bool, error) {
	runs, err := store.List()
	if err != nil || len(runs) == 0 {
		return Run{}, false, err
	}
	return runs[len(runs)-1], true, nil
}
