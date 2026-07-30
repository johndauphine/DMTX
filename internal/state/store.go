// Package state persists durable migration run history and restart checkpoints.
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Outcome string

const (
	Running Outcome = "running"
	Success Outcome = "success"
	Failed  Outcome = "failed"
)

// ErrRunLeaseEvidenceUnavailable means a run predates target-lease binding or
// otherwise has no complete, trustworthy fencing tuple. Legacy runs remain
// readable, but callers that need ownership proof must fail closed on this
// error.
var ErrRunLeaseEvidenceUnavailable = errors.New("run target lease evidence unavailable")

type Run struct {
	ID              string    `json:"id" yaml:"id"`
	Source          string    `json:"source" yaml:"source"`
	Target          string    `json:"target" yaml:"target"`
	SourceEngine    string    `json:"source_engine,omitempty" yaml:"source_engine,omitempty"`
	SourceIdentity  string    `json:"source_identity,omitempty" yaml:"source_identity,omitempty"`
	TargetIdentity  string    `json:"target_identity,omitempty" yaml:"target_identity,omitempty"`
	LeaseTarget     string    `json:"lease_target,omitempty" yaml:"lease_target,omitempty"`
	LeaseOwnerToken string    `json:"lease_owner_token,omitempty" yaml:"lease_owner_token,omitempty"`
	LeaseGeneration int64     `json:"lease_generation,omitempty" yaml:"lease_generation,omitempty"`
	Outcome         Outcome   `json:"outcome" yaml:"outcome"`
	Resumable       bool      `json:"resumable" yaml:"resumable"`
	Reason          string    `json:"resumability_reason" yaml:"resumability_reason"`
	StartedAt       time.Time `json:"started_at" yaml:"started_at"`
	EndedAt         time.Time `json:"ended_at,omitempty" yaml:"ended_at,omitempty"`
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
	switch {
	case next.LeaseTarget == "" &&
		next.LeaseOwnerToken == "" &&
		next.LeaseGeneration == 0:
		next.LeaseTarget = existing.LeaseTarget
		next.LeaseOwnerToken = existing.LeaseOwnerToken
		next.LeaseGeneration = existing.LeaseGeneration
	case next.LeaseTarget != existing.LeaseTarget ||
		next.LeaseOwnerToken != existing.LeaseOwnerToken ||
		next.LeaseGeneration != existing.LeaseGeneration:
		return Run{}, fmt.Errorf(
			"%w: run %q target lease evidence changed",
			ErrImmutableEvidence,
			next.ID,
		)
	}
	if err := validateRunRecord(next); err != nil {
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

func validateRunLeaseEvidence(run Run) error {
	if run.LeaseTarget == "" &&
		run.LeaseOwnerToken == "" &&
		run.LeaseGeneration == 0 {
		return nil
	}
	targetPresent := strings.TrimSpace(run.LeaseTarget) != ""
	tokenPresent := strings.TrimSpace(run.LeaseOwnerToken) != ""
	generationPresent := run.LeaseGeneration != 0
	if !targetPresent || !tokenPresent || !generationPresent {
		return fmt.Errorf(
			"run %q target lease evidence must include target, owner token, and generation",
			run.ID,
		)
	}
	if run.LeaseGeneration < 1 {
		return fmt.Errorf(
			"run %q target lease generation must be positive",
			run.ID,
		)
	}
	return nil
}

func validateRunRecord(run Run) error {
	if err := validateRunSourceEngine(run.SourceEngine); err != nil {
		return err
	}
	return validateRunLeaseEvidence(run)
}

// BoundLease returns the immutable target-ownership evidence recorded on a
// run. It deliberately rejects readable legacy runs without fencing metadata.
func (run Run) BoundLease() (Lease, error) {
	if err := validateRunLeaseEvidence(run); err != nil {
		return Lease{}, fmt.Errorf(
			"%w: run %q: %v",
			ErrRunLeaseEvidenceUnavailable,
			run.ID,
			err,
		)
	}
	if run.LeaseTarget == "" {
		return Lease{}, fmt.Errorf(
			"%w: run %q",
			ErrRunLeaseEvidenceUnavailable,
			run.ID,
		)
	}
	if run.ID == "" {
		return Lease{}, fmt.Errorf("bound target lease requires run ID")
	}
	return Lease{
		Target:     run.LeaseTarget,
		RunID:      run.ID,
		OwnerToken: run.LeaseOwnerToken,
		Generation: run.LeaseGeneration,
	}, nil
}

type Store struct{ Path string }

func (store Store) Append(run Run) error {
	if err := validateRunRecord(run); err != nil {
		return err
	}
	runs, err := store.List()
	if err != nil {
		return err
	}
	for _, existing := range runs {
		if existing.ID != run.ID {
			continue
		}
		run, err = inheritRunWorkloadIdentity(existing, run)
		if err != nil {
			return err
		}
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
	for _, run := range runs {
		if err := validateRunRecord(run); err != nil {
			return nil, fmt.Errorf("decode run state: %w", err)
		}
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
