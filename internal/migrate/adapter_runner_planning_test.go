package migrate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

type rejectingAdapterPlanTarget struct {
	*recordingAdapterTarget
	reject string
	err    error
}

func (target *rejectingAdapterPlanTarget) PlanTable(
	sourceEngine string,
	sourceTable schema.Table,
	mode string,
) (schema.Table, error) {
	planned, err := target.recordingAdapterTarget.PlanTable(
		sourceEngine,
		sourceTable,
		mode,
	)
	if err != nil {
		return schema.Table{}, err
	}
	if sourceTable.Name == target.reject {
		return schema.Table{}, target.err
	}
	return planned, nil
}

func TestAdapterRunnerPlanFailurePreventsAllTargetMutation(t *testing.T) {
	events := make([]string, 0)
	forced := errors.New("forced target planning rejection")
	source := &recordingAdapterSource{
		events: &events,
		tables: []schema.Table{
			{
				Schema: "public",
				Name:   "first",
				Columns: []schema.Column{
					{Name: "id", PrimaryKey: true},
				},
			},
			{
				Schema: "public",
				Name:   "blocked",
				Columns: []schema.Column{
					{Name: "id", PrimaryKey: true},
				},
			},
		},
	}
	recordingTarget := &recordingAdapterTarget{events: &events}
	target := &rejectingAdapterPlanTarget{
		recordingAdapterTarget: recordingTarget,
		reject:                 "blocked",
		err:                    forced,
	}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if !errors.Is(err, forced) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	if len(recordingTarget.planned) != 2 {
		t.Fatalf("planned tables = %v, want both tables", recordingTarget.planned)
	}
	if len(recordingTarget.prepared) != 0 ||
		len(recordingTarget.written) != 0 {
		t.Fatalf(
			"target mutated after failed preflight: prepare=%v write=%v",
			recordingTarget.prepared,
			recordingTarget.written,
		)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") {
			t.Fatalf("checkpoint created before planning completed: %v", events)
		}
	}
}

type mismatchedAdapterTableNameSource struct {
	*recordingAdapterSource
	listed    string
	inspected string
}

func (source *mismatchedAdapterTableNameSource) ListTables(
	context.Context,
) ([]string, error) {
	*source.events = append(*source.events, "source_list")
	return []string{source.listed}, nil
}

func (source *mismatchedAdapterTableNameSource) InspectTable(
	context.Context,
	string,
) (schema.Table, error) {
	*source.events = append(*source.events, "source_inspect")
	return schema.Table{
		Schema: "public",
		Name:   source.inspected,
		Columns: []schema.Column{
			{Name: "id", PrimaryKey: true},
		},
	}, nil
}

func TestAdapterRunnerRejectsInspectedSourceNameMismatchBeforePlanning(
	t *testing.T,
) {
	events := make([]string, 0)
	source := &mismatchedAdapterTableNameSource{
		recordingAdapterSource: &recordingAdapterSource{events: &events},
		listed:                 "listed",
		inspected:              "different",
	}
	target := &recordingAdapterTarget{events: &events}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if err == nil || !strings.Contains(
		err.Error(),
		`source adapter postgres inspected table "listed" as "different"`,
	) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	if len(target.planned) != 0 ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 {
		t.Fatalf(
			"target activity after name mismatch: plan=%v prepare=%v write=%v",
			target.planned,
			target.prepared,
			target.written,
		)
	}
	for _, event := range events {
		if strings.HasPrefix(event, "before") {
			t.Fatalf("checkpoint created after name mismatch: %v", events)
		}
	}
}
