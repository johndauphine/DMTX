package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type scriptedAdapterMutationProtector struct {
	invocations []int
	call        int
	after       int
}

func (*scriptedAdapterMutationProtector) BeforeTable(
	context.Context,
	string,
) error {
	return nil
}

func (protector *scriptedAdapterMutationProtector) AfterTable(
	context.Context,
	string,
	int,
) error {
	protector.after++
	return nil
}

func (protector *scriptedAdapterMutationProtector) ProtectTargetMutation(
	_ context.Context,
	mutation func() error,
) error {
	invocations := 1
	if protector.call < len(protector.invocations) {
		invocations = protector.invocations[protector.call]
	}
	protector.call++

	var result error
	for invocation := 0; invocation < invocations; invocation++ {
		if err := mutation(); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func validAdapterRunnerSource(events *[]string) *recordingAdapterSource {
	return &recordingAdapterSource{
		events: events,
		table: schema.Table{
			Schema: "public",
			Name:   "items",
			Columns: []schema.Column{
				{Name: "id", PrimaryKey: true},
				{Name: "payload"},
			},
		},
	}
}

func TestAdapterRunnerRejectsInvalidMutationProtectorInvocationCounts(
	t *testing.T,
) {
	tests := []struct {
		name          string
		invocations   []int
		wantPrepared  int
		wantCaptured  int
		wantFinalized int
	}{
		{
			name:        "prepare omitted",
			invocations: []int{0},
		},
		{
			name:         "prepare repeated",
			invocations:  []int{2},
			wantPrepared: 1,
		},
		{
			name:         "write omitted",
			invocations:  []int{1, 0},
			wantPrepared: 1,
		},
		{
			name:         "write repeated",
			invocations:  []int{1, 2},
			wantPrepared: 1,
			wantCaptured: 2,
		},
		{
			name:          "finalize omitted",
			invocations:   []int{1, 1, 0},
			wantPrepared:  1,
			wantCaptured:  2,
			wantFinalized: 0,
		},
		{
			name:          "finalize repeated",
			invocations:   []int{1, 1, 2},
			wantPrepared:  1,
			wantCaptured:  2,
			wantFinalized: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := make([]string, 0)
			protector := &scriptedAdapterMutationProtector{
				invocations: test.invocations,
			}
			target := &recordingAdapterTarget{events: &events}
			result, err := migrateWithAdapters(
				context.Background(),
				config.Config{},
				protector,
				validAdapterRunnerSource(&events),
				target,
			)
			if ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), "expected exactly once") {
				t.Fatalf("result = %#v, error = %v", result, err)
			}
			if result != (Result{}) {
				t.Fatalf("partial result = %#v", result)
			}
			if len(target.prepared) != test.wantPrepared {
				t.Fatalf(
					"prepared = %d, want %d",
					len(target.prepared),
					test.wantPrepared,
				)
			}
			if len(target.captured) != test.wantCaptured {
				t.Fatalf(
					"captured = %d, want %d",
					len(target.captured),
					test.wantCaptured,
				)
			}
			if len(target.finalized) != test.wantFinalized {
				t.Fatalf(
					"finalized = %d, want %d",
					len(target.finalized),
					test.wantFinalized,
				)
			}
			if protector.after != 0 {
				t.Fatalf("invalid fenced mutation was checkpointed")
			}
		})
	}
}

func TestAdapterRunnerInvalidReceiptPreservesWriteErrorWithoutCheckpoint(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced invalid commit response")
	receipt := WriteReceipt{
		Certainty:     CommitDurable,
		AttemptedRows: 2,
		CommittedRows: 1,
	}
	target := &recordingAdapterTarget{
		events:   &events,
		receipt:  &receipt,
		writeErr: forced,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		validAdapterRunnerSource(&events),
		target,
	)
	if !errors.Is(err, forced) ||
		!errors.Is(err, ErrInvalidWriteReceipt) ||
		ClassifyTransferError(err) != ErrorClassState {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	for _, event := range events {
		if event == "after:items" {
			t.Fatalf("invalid receipt was checkpointed: %v", events)
		}
	}
}

func TestAdapterRunnerNotCommittedReceiptPreservesWriteError(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("forced write rollback")
	receipt := WriteReceipt{
		Certainty:     CommitNotCommitted,
		AttemptedRows: 2,
	}
	target := &recordingAdapterTarget{
		events:   &events,
		receipt:  &receipt,
		writeErr: forced,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		validAdapterRunnerSource(&events),
		target,
	)
	if !errors.Is(err, forced) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	for _, event := range events {
		if event == "after:items" {
			t.Fatalf("rolled-back write was checkpointed: %v", events)
		}
	}
}
