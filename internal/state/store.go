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
	ID             string    `json:"id" yaml:"id"`
	Source         string    `json:"source" yaml:"source"`
	Target         string    `json:"target" yaml:"target"`
	SourceEngine   string    `json:"source_engine,omitempty" yaml:"source_engine,omitempty"`
	SourceIdentity string    `json:"source_identity,omitempty" yaml:"source_identity,omitempty"`
	TargetIdentity string    `json:"target_identity,omitempty" yaml:"target_identity,omitempty"`
	Outcome        Outcome   `json:"outcome" yaml:"outcome"`
	Resumable      bool      `json:"resumable" yaml:"resumable"`
	Reason         string    `json:"resumability_reason" yaml:"resumability_reason"`
	StartedAt      time.Time `json:"started_at" yaml:"started_at"`
	EndedAt        time.Time `json:"ended_at,omitempty" yaml:"ended_at,omitempty"`
}

func inheritRunWorkloadIdentity(existing, next Run) (Run, error) {
	fields := []struct {
		name     string
		existing string
		next     *string
	}{
		{name: "source", existing: existing.Source, next: &next.Source},
		{name: "target", existing: existing.Target, next: &next.Target},
		{name: "source engine", existing: existing.SourceEngine, next: &next.SourceEngine},
		{name: "source identity", existing: existing.SourceIdentity, next: &next.SourceIdentity},
		{name: "target identity", existing: existing.TargetIdentity, next: &next.TargetIdentity},
	}
	for _, field := range fields {
		if *field.next == "" {
			*field.next = field.existing
			continue
		}
		if *field.next != field.existing {
			return Run{}, fmt.Errorf(
				"%w: run %q %s changed",
				ErrImmutableEvidence,
				next.ID,
				field.name,
			)
		}
	}
	if err := validateRunSourceEngine(next.SourceEngine); err != nil {
		return Run{}, err
	}
	return next, nil
}

func validateRunSourceEngine(engine string) error {
	switch engine {
	case "", "postgres", "mssql", "mysql", "sqlite", "clickhouse":
		return nil
	default:
		return fmt.Errorf(
			"run source engine %q is not a canonical supported engine",
			engine,
		)
	}
}

type Store struct{ Path string }

func (store Store) Append(run Run) error {
	if err := validateRunSourceEngine(run.SourceEngine); err != nil {
		return err
	}
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
