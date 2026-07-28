package state

import (
	"time"

	"gopkg.in/yaml.v3"
)

type runYAML struct {
	ID        string    `yaml:"id"`
	Source    string    `yaml:"source"`
	Target    string    `yaml:"target"`
	Outcome   Outcome   `yaml:"outcome"`
	Resumable bool      `yaml:"resumable"`
	Reason    string    `yaml:"resumability_reason"`
	StartedAt time.Time `yaml:"started_at"`
	EndedAt   time.Time `yaml:"ended_at,omitempty"`
}

func (run Run) MarshalYAML() (any, error) {
	return runYAML{
		ID:        run.ID,
		Source:    run.Source,
		Target:    run.Target,
		Outcome:   run.Outcome,
		Resumable: run.Resumable,
		Reason:    run.Reason,
		StartedAt: run.StartedAt,
		EndedAt:   run.EndedAt,
	}, nil
}

func (run *Run) UnmarshalYAML(node *yaml.Node) error {
	var encoded runYAML
	if err := node.Decode(&encoded); err != nil {
		return err
	}
	*run = Run{
		ID:        encoded.ID,
		Source:    encoded.Source,
		Target:    encoded.Target,
		Outcome:   encoded.Outcome,
		Resumable: encoded.Resumable,
		Reason:    encoded.Reason,
		StartedAt: encoded.StartedAt,
		EndedAt:   encoded.EndedAt,
	}
	return nil
}

type taskYAML struct {
	RunID              string    `yaml:"run_id"`
	Table              string    `yaml:"table"`
	Status             string    `yaml:"status"`
	RowsDone           int       `yaml:"rows_done"`
	IntegerWatermark   *int64    `yaml:"integer_watermark,omitempty"`
	RowNumberWatermark *int64    `yaml:"row_number_watermark,omitempty"`
	StartedAt          time.Time `yaml:"started_at"`
	CompletedAt        time.Time `yaml:"completed_at,omitempty"`
}

func (task Task) MarshalYAML() (any, error) {
	return taskYAML{
		RunID:              task.RunID,
		Table:              task.Table,
		Status:             task.Status,
		RowsDone:           task.RowsDone,
		IntegerWatermark:   task.IntegerWatermark,
		RowNumberWatermark: task.RowNumberWatermark,
		StartedAt:          task.StartedAt,
		CompletedAt:        task.CompletedAt,
	}, nil
}

func (task *Task) UnmarshalYAML(node *yaml.Node) error {
	var encoded taskYAML
	if err := node.Decode(&encoded); err != nil {
		return err
	}
	*task = Task{
		RunID:              encoded.RunID,
		Table:              encoded.Table,
		Status:             encoded.Status,
		RowsDone:           encoded.RowsDone,
		IntegerWatermark:   encoded.IntegerWatermark,
		RowNumberWatermark: encoded.RowNumberWatermark,
		StartedAt:          encoded.StartedAt,
		CompletedAt:        encoded.CompletedAt,
	}
	return nil
}
