package migrate

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

func TestNormalizeIncrementalDateColumnsPreservesOrderedExactIdentifiers(t *testing.T) {
	got, err := NormalizeIncrementalDateColumns(
		[]string{" modified_at ", "UpdatedAt", "updatedat"},
	)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"modified_at", "UpdatedAt", "updatedat"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized candidates = %#v, want %#v", got, want)
	}
	for _, candidates := range [][]string{
		{""},
		{" \t "},
		{"updated_at", " updated_at "},
	} {
		if _, err := NormalizeIncrementalDateColumns(candidates); err == nil {
			t.Fatalf("NormalizeIncrementalDateColumns(%#v) succeeded", candidates)
		}
	}
}

func TestBuildIncrementalTablePlanSelectsFirstCompatibleAndCompletePKOrder(t *testing.T) {
	table := incrementalTestTable()
	table.Columns = append(
		[]IncrementalColumn{
			{
				Name: "unsafe_time", TemporalKind: IncrementalTemporalTimestamp,
				OrderAdmission: "",
			},
		},
		table.Columns...,
	)
	plan, err := BuildIncrementalTablePlan(table, []string{
		"missing_time",
		"payload",
		"unsafe_time",
		"updated_at",
		"created_at",
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.FullTableUpsert || plan.DateColumn == nil ||
		plan.DateColumn.Name != "updated_at" ||
		plan.DateColumn.TemporalKind != IncrementalTemporalTimestamp ||
		!plan.DateColumn.Nullable {
		t.Fatalf("selected date column = %#v, full=%v", plan.DateColumn, plan.FullTableUpsert)
	}
	if got, want := incrementalColumnNames(plan.PrimaryKey), []string{"tenant_id", "id"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("primary key = %#v, want %#v", got, want)
	}
	if got, want := incrementalOrderNames(plan.Ordering), []string{
		"updated_at:update_column:first",
		"tenant_id:primary_key:",
		"id:primary_key:",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ordering = %#v, want %#v", got, want)
	}
	if got, want := incrementalDecisionActions(plan.CandidateDecisions), []IncrementalCandidateAction{
		IncrementalCandidateMissing,
		IncrementalCandidateIncompatibleType,
		IncrementalCandidateUnsafeOrder,
		IncrementalCandidateSelected,
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("candidate decisions = %#v, want %#v", got, want)
	}
	second, err := BuildIncrementalTablePlan(table, []string{
		"missing_time",
		"payload",
		"unsafe_time",
		"updated_at",
		"created_at",
	})
	if err != nil || second.PlanHash != plan.PlanHash {
		t.Fatalf("deterministic plan hash = %q, %q, err=%v", plan.PlanHash, second.PlanHash, err)
	}
	table.Columns[1].Name = "mutated_after_plan"
	if plan.PrimaryKey[0].Name != "tenant_id" || plan.Table.Columns[1].Name == "mutated_after_plan" {
		t.Fatal("plan aliases caller metadata")
	}
}

func TestBuildIncrementalTablePlanWithoutCandidateUsesFullPKUpsert(t *testing.T) {
	plan, err := BuildIncrementalTablePlan(
		incrementalTestTable(),
		[]string{"missing", "payload"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.FullTableUpsert || plan.DateColumn != nil {
		t.Fatalf("full-table decision = %#v", plan)
	}
	if got, want := incrementalOrderNames(plan.Ordering), []string{
		"tenant_id:primary_key:",
		"id:primary_key:",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ordering = %#v, want %#v", got, want)
	}
}

func TestBuildIncrementalTablePlanFailsClosedOnKeyAndCatalogShapes(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*IncrementalTable)
		wantClass TransferErrorClass
		want      string
	}{
		{
			name: "no primary key",
			mutate: func(table *IncrementalTable) {
				for index := range table.Columns {
					table.Columns[index].PrimaryKeyPosition = 0
				}
			},
			wantClass: ErrorClassPrimaryKey,
			want:      "has no primary key",
		},
		{
			name: "nullable primary key",
			mutate: func(table *IncrementalTable) {
				table.Columns[0].Nullable = true
			},
			wantClass: ErrorClassPrimaryKey,
			want:      "is nullable",
		},
		{
			name: "primary key gap",
			mutate: func(table *IncrementalTable) {
				table.Columns[1].PrimaryKeyPosition = 3
			},
			wantClass: ErrorClassPrimaryKey,
			want:      "incomplete at position 2",
		},
		{
			name: "primary key duplicate position",
			mutate: func(table *IncrementalTable) {
				table.Columns[1].PrimaryKeyPosition = 1
			},
			wantClass: ErrorClassPrimaryKey,
			want:      "position 1 is shared",
		},
		{
			name: "unsafe primary key order",
			mutate: func(table *IncrementalTable) {
				table.Columns[0].OrderAdmission = ""
			},
			wantClass: ErrorClassPrimaryKey,
			want:      "lacks exact ordering admission",
		},
		{
			name: "unknown temporal admission",
			mutate: func(table *IncrementalTable) {
				table.Columns[2].TemporalKind = "mystery"
			},
			wantClass: ErrorClassPolicy,
			want:      "unknown temporal admission",
		},
		{
			name: "unknown order admission",
			mutate: func(table *IncrementalTable) {
				table.Columns[2].OrderAdmission = "locale_guess"
			},
			wantClass: ErrorClassPolicy,
			want:      "unknown order admission",
		},
		{
			name: "duplicate catalog column",
			mutate: func(table *IncrementalTable) {
				table.Columns = append(table.Columns, table.Columns[0])
			},
			wantClass: ErrorClassPolicy,
			want:      "duplicate column",
		},
		{
			name: "negative primary key position",
			mutate: func(table *IncrementalTable) {
				table.Columns[3].PrimaryKeyPosition = -1
			},
			wantClass: ErrorClassPrimaryKey,
			want:      "negative primary-key position",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			table := incrementalTestTable()
			test.mutate(&table)
			_, err := BuildIncrementalTablePlan(table, []string{"updated_at"})
			if err == nil || ClassifyTransferError(err) != test.wantClass ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, class=%q", err, ClassifyTransferError(err))
			}
		})
	}
}

func TestIncrementalWindowIsStrictLowerInclusiveUpperAndExcludesNull(t *testing.T) {
	lower := time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)
	upper := lower.Add(time.Hour)
	window := IncrementalWindow{
		Column: "updated_at", Lower: &lower, Upper: &upper,
		LowerExclusive: true, UpperInclusive: true, ExcludeNull: true,
	}
	before := lower.Add(-time.Nanosecond)
	inside := lower.Add(time.Nanosecond)
	after := upper.Add(time.Nanosecond)
	for name, test := range map[string]struct {
		value *time.Time
		want  bool
	}{
		"NULL":        {value: nil, want: false},
		"before":      {value: &before, want: false},
		"lower equal": {value: &lower, want: false},
		"inside":      {value: &inside, want: true},
		"upper equal": {value: &upper, want: true},
		"after":       {value: &after, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			if got := window.Contains(test.value); got != test.want {
				t.Fatalf("Contains() = %v, want %v", got, test.want)
			}
		})
	}
	window.Empty = true
	if window.Contains(&inside) {
		t.Fatal("empty window contains a timestamp")
	}
}

func TestExecuteIncrementalBaselinePersistsFenceBeforeReadAndCommitsExactFence(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	started := time.Date(2026, 7, 30, 13, 0, 0, 0, time.FixedZone("local", -5*60*60))
	upper := started.Add(time.Hour)
	completed := started.Add(2 * time.Hour)
	var observed IncrementalReadPlan
	request := incrementalTestRequest(store, plan, started)
	request.SampleUpperFence = func(
		_ context.Context,
		table IncrementalTable,
		column IncrementalColumn,
	) (*time.Time, error) {
		store.record("sample")
		if table.Name != "events" || column.Name != "updated_at" {
			t.Fatalf("sample metadata = %#v %#v", table, column)
		}
		return &upper, nil
	}
	request.Transfer = func(_ context.Context, read IncrementalReadPlan) error {
		store.record("transfer")
		observed = read
		if !store.hasActive() {
			t.Fatal("transfer started before durable incremental attempt")
		}
		return nil
	}
	request.CompletionTime = func() time.Time { return completed }
	result, err := ExecuteIncrementalTable(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.CreatedAttempt || result.ResumedAttempt || !result.Completed ||
		result.Attempt.Mode != state.IncrementalBaseline ||
		result.Attempt.UpperFence == nil ||
		!result.Attempt.UpperFence.Value.Equal(upper) ||
		result.Attempt.UpperFence.Value.Location() != time.UTC {
		t.Fatalf("result = %#v", result)
	}
	if observed.Scope != IncrementalReadFullTable || observed.Window != nil ||
		observed.ReplayFromLowerWatermark || observed.PositionalRestoreAllowed {
		t.Fatalf("baseline read = %#v", observed)
	}
	if observed.Ordering[0].Nulls != IncrementalNullsFirst {
		t.Fatalf("baseline NULL order = %#v", observed.Ordering)
	}
	if got, want := store.eventsSnapshot(), []string{
		"load_active",
		"load_latest",
		"sample",
		"begin",
		"transfer",
		"commit",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v, want %#v", got, want)
	}
	if store.commit.Watermark == nil ||
		!store.commit.Watermark.Value.Equal(upper) ||
		!store.commit.CompletedAt.Equal(completed) {
		t.Fatalf("commit = %#v", store.commit)
	}
}

func TestExecuteIncrementalBaselineWithOnlyNullTimestampsCommitsNil(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	request := incrementalTestRequest(
		store,
		plan,
		time.Date(2026, 7, 30, 14, 0, 0, 0, time.UTC),
	)
	request.SampleUpperFence = func(
		context.Context,
		IncrementalTable,
		IncrementalColumn,
	) (*time.Time, error) {
		return nil, nil
	}
	request.Transfer = func(context.Context, IncrementalReadPlan) error { return nil }
	if _, err := ExecuteIncrementalTable(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if store.commit.Watermark != nil {
		t.Fatalf("nil baseline maximum committed as %#v", store.commit.Watermark)
	}
}

func TestExecuteIncrementalWindowUsesDurableLowerAndImmutableUpper(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	lower := time.Date(2026, 7, 30, 10, 0, 0, 123456000, time.UTC)
	upper := lower.Add(time.Hour)
	store.latest = completedIncrementalAttempt(
		"prior-run",
		"prior-attempt",
		lower.Add(-time.Hour),
		&state.TimestampWatermark{Column: "updated_at", Value: lower},
	)
	store.latestFound = true
	request := incrementalTestRequest(store, plan, upper.Add(time.Hour))
	request.SampleUpperFence = func(
		context.Context,
		IncrementalTable,
		IncrementalColumn,
	) (*time.Time, error) {
		return &upper, nil
	}
	request.Transfer = func(_ context.Context, read IncrementalReadPlan) error {
		if read.Scope != IncrementalReadWindow || read.Window == nil ||
			read.Window.Empty || !read.ReplayFromLowerWatermark ||
			read.PositionalRestoreAllowed || read.Resumed ||
			read.Window.Lower == nil || !read.Window.Lower.Equal(lower) ||
			read.Window.Upper == nil || !read.Window.Upper.Equal(upper) {
			t.Fatalf("incremental read = %#v", read)
		}
		if read.Window.Contains(&lower) || !read.Window.Contains(&upper) {
			t.Fatal("incremental read does not implement (lower, upper]")
		}
		if read.Ordering[0].Nulls != IncrementalNullsExcluded {
			t.Fatalf("window NULL order = %#v", read.Ordering)
		}
		return nil
	}
	result, err := ExecuteIncrementalTable(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Attempt.Mode != state.IncrementalWindow ||
		result.Attempt.LowerWatermark == nil ||
		!result.Attempt.LowerWatermark.Value.Equal(lower) ||
		store.commit.Watermark == nil ||
		!store.commit.Watermark.Value.Equal(upper) {
		t.Fatalf("attempt/result = %#v commit=%#v", result.Attempt, store.commit)
	}
}

func TestExecuteIncrementalUnchangedWindowIsExplicitlyEmpty(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	lower := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	store.latest = completedIncrementalAttempt(
		"prior-run",
		"prior-attempt",
		lower.Add(-time.Hour),
		&state.TimestampWatermark{Column: "updated_at", Value: lower},
	)
	store.latestFound = true
	request := incrementalTestRequest(store, plan, lower.Add(time.Hour))
	request.SampleUpperFence = func(
		context.Context,
		IncrementalTable,
		IncrementalColumn,
	) (*time.Time, error) {
		return &lower, nil
	}
	request.Transfer = func(_ context.Context, read IncrementalReadPlan) error {
		if read.Window == nil || !read.Window.Empty || read.Window.Contains(&lower) {
			t.Fatalf("unchanged window = %#v", read.Window)
		}
		return nil
	}
	if _, err := ExecuteIncrementalTable(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if store.commit.Watermark == nil || !store.commit.Watermark.Value.Equal(lower) {
		t.Fatalf("unchanged commit = %#v", store.commit)
	}
}

func TestExecuteIncrementalResumeReusesFenceAndReplaysWholeWindow(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	lower := time.Date(2026, 7, 30, 10, 0, 0, 0, time.UTC)
	upper := lower.Add(2 * time.Hour)
	started := lower.Add(3 * time.Hour)
	store.active = state.IncrementalAttempt{
		RunID: "run-1",
		Task: state.TaskKey{
			Type: "table-copy", Schema: "public", Table: "events",
		},
		AttemptID:      "original-attempt",
		Mode:           state.IncrementalWindow,
		LowerWatermark: &state.TimestampWatermark{Column: "updated_at", Value: lower},
		UpperFence:     &state.TimestampWatermark{Column: "updated_at", Value: upper},
		Status:         state.IncrementalRunning,
		StartedAt:      started,
	}
	store.activeFound = true
	store.latest = completedIncrementalAttempt(
		"prior-run",
		"prior-attempt",
		lower.Add(-2*time.Hour),
		&state.TimestampWatermark{Column: "updated_at", Value: lower},
	)
	store.latestFound = true
	request := incrementalTestRequest(store, plan, started.Add(time.Hour))
	request.AttemptID = ""
	request.SampleUpperFence = nil
	request.Transfer = func(_ context.Context, read IncrementalReadPlan) error {
		store.record("transfer")
		if !read.Resumed || !read.ReplayFromLowerWatermark ||
			read.PositionalRestoreAllowed || read.Window == nil ||
			read.Window.Lower == nil || !read.Window.Lower.Equal(lower) ||
			read.Window.Upper == nil || !read.Window.Upper.Equal(upper) {
			t.Fatalf("resume read = %#v", read)
		}
		return nil
	}
	result, err := ExecuteIncrementalTable(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.ResumedAttempt || result.CreatedAttempt ||
		result.Attempt.AttemptID != "original-attempt" ||
		store.commit.AttemptID != "original-attempt" ||
		store.commit.Watermark == nil ||
		!store.commit.Watermark.Value.Equal(upper) {
		t.Fatalf("resume result = %#v commit=%#v", result, store.commit)
	}
	if got, want := store.eventsSnapshot(), []string{
		"load_active",
		"load_latest",
		"transfer",
		"commit",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resume events = %#v, want %#v", got, want)
	}
}

func TestExecuteIncrementalResumeRequiresExactCommittedFrontier(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 13, 0, 0, 0, time.UTC)
	lower := started.Add(-2 * time.Hour)
	upper := started.Add(-time.Hour)
	prior := completedIncrementalAttempt(
		"prior-run",
		"prior-attempt",
		lower.Add(-time.Hour),
		&state.TimestampWatermark{Column: "updated_at", Value: lower},
	)
	for _, test := range []struct {
		name        string
		active      state.IncrementalAttempt
		latest      state.IncrementalAttempt
		latestFound bool
		want        string
	}{
		{
			name: "window without committed baseline",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode:       state.IncrementalWindow,
				UpperFence: &state.TimestampWatermark{Column: "updated_at", Value: upper},
				Status:     state.IncrementalRunning, StartedAt: started,
			},
			want: "has no prior committed baseline frontier",
		},
		{
			name: "window lower differs from committed frontier",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode: state.IncrementalWindow,
				LowerWatermark: &state.TimestampWatermark{
					Column: "updated_at", Value: lower.Add(time.Nanosecond),
				},
				UpperFence: &state.TimestampWatermark{Column: "updated_at", Value: upper},
				Status:     state.IncrementalRunning, StartedAt: started,
			},
			latest:      prior,
			latestFound: true,
			want:        "does not match the prior committed frontier",
		},
		{
			name: "baseline conflicts with committed frontier",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode:   state.IncrementalBaseline,
				Status: state.IncrementalRunning, StartedAt: started,
			},
			latest:      prior,
			latestFound: true,
			want:        "baseline conflicts with a prior committed frontier",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			store.active = test.active
			store.activeFound = true
			store.latest = test.latest
			store.latestFound = test.latestFound
			request := incrementalTestRequest(store, plan, started)
			request.SampleUpperFence = nil
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("unproven active frontier reached transfer")
				return nil
			}
			result, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || result.Completed ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"result=%#v error=%v class=%q",
					result,
					err,
					ClassifyTransferError(err),
				)
			}
			if store.commit.AttemptID != "" {
				t.Fatalf("unproven frontier committed %#v", store.commit)
			}
			if got, want := store.eventsSnapshot(), []string{
				"load_active",
				"load_latest",
			}; !reflect.DeepEqual(got, want) {
				t.Fatalf("events=%#v want=%#v", got, want)
			}
		})
	}
}

func TestExecuteIncrementalResumeAcceptsPriorNilWatermarkFrontier(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	started := time.Date(2026, 7, 30, 13, 30, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	store.latest = completedIncrementalAttempt(
		"prior-run",
		"prior-attempt",
		started.Add(-2*time.Hour),
		nil,
	)
	store.latestFound = true
	store.active = state.IncrementalAttempt{
		RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
		Mode:       state.IncrementalWindow,
		UpperFence: &state.TimestampWatermark{Column: "updated_at", Value: upper},
		Status:     state.IncrementalRunning, StartedAt: started,
	}
	store.activeFound = true
	request := incrementalTestRequest(store, plan, started)
	request.SampleUpperFence = nil
	request.Transfer = func(_ context.Context, read IncrementalReadPlan) error {
		if !read.Resumed || read.Window == nil || read.Window.Empty ||
			read.Window.Lower != nil || read.Window.Upper == nil ||
			!read.Window.Upper.Equal(upper) {
			t.Fatalf("nil-frontier resume read = %#v", read)
		}
		return nil
	}
	result, err := ExecuteIncrementalTable(context.Background(), request)
	if err != nil || !result.Completed || !result.ResumedAttempt {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestExecuteIncrementalFullUpsertNeedsNoIncrementalState(t *testing.T) {
	plan, err := BuildIncrementalTablePlan(incrementalTestTable(), nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	result, err := ExecuteIncrementalTable(
		context.Background(),
		IncrementalExecutionRequest{
			Plan: plan,
			Transfer: func(_ context.Context, read IncrementalReadPlan) error {
				called = true
				if read.Scope != IncrementalReadFullTable || read.Window != nil ||
					gotPKOrder(read.Ordering) != "tenant_id,id" {
					t.Fatalf("full upsert read = %#v", read)
				}
				return nil
			},
		},
	)
	if err != nil || !called || !result.Completed || result.Attempt.AttemptID != "" {
		t.Fatalf("full upsert result = %#v called=%v err=%v", result, called, err)
	}
}

func TestExecuteIncrementalPreflightRejectsMissingSafetyInputs(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 14, 30, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		configure func(*IncrementalExecutionRequest)
		wantClass TransferErrorClass
		want      string
	}{
		{
			name: "transfer callback",
			configure: func(request *IncrementalExecutionRequest) {
				request.Transfer = nil
			},
			wantClass: ErrorClassPolicy,
			want:      "transfer callback is required",
		},
		{
			name: "typed nil state",
			configure: func(request *IncrementalExecutionRequest) {
				var typedNil *incrementalFakeState
				request.State = typedNil
			},
			wantClass: ErrorClassState,
			want:      "state backend is required",
		},
		{
			name: "topology",
			configure: func(request *IncrementalExecutionRequest) {
				request.TopologyHash = ""
			},
			wantClass: ErrorClassPolicy,
			want:      "topology hash is required",
		},
		{
			name: "durable plan topology binding",
			configure: func(request *IncrementalExecutionRequest) {
				request.VerifyDurableBinding = nil
			},
			wantClass: ErrorClassState,
			want:      "durable plan/topology binding verifier is required",
		},
		{
			name: "run ID",
			configure: func(request *IncrementalExecutionRequest) {
				request.RunID = ""
			},
			wantClass: ErrorClassPolicy,
			want:      "run ID is required",
		},
		{
			name: "task identity",
			configure: func(request *IncrementalExecutionRequest) {
				request.Task = state.TaskKey{}
			},
			wantClass: ErrorClassPolicy,
			want:      "task type and table are required",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			request := incrementalTestRequest(store, plan, started)
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("invalid request reached transfer")
				return nil
			}
			test.configure(&request)
			_, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || ClassifyTransferError(err) != test.wantClass ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v class=%q", err, ClassifyTransferError(err))
			}
			if store.hasActive() || store.commit.AttemptID != "" {
				t.Fatalf("invalid request mutated state: %#v %#v", store.active, store.commit)
			}
		})
	}
}

func TestExecuteIncrementalDurableBindingFailurePreventsTransferOrSkip(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 14, 45, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	injected := errors.New("stale durable plan binding")
	for _, test := range []struct {
		name      string
		configure func(*incrementalFakeState, *IncrementalExecutionRequest)
	}{
		{
			name: "new attempt",
			configure: func(
				_ *incrementalFakeState,
				request *IncrementalExecutionRequest,
			) {
				request.SampleUpperFence = fixedIncrementalFence(upper)
			},
		},
		{
			name: "resumed attempt",
			configure: func(
				store *incrementalFakeState,
				request *IncrementalExecutionRequest,
			) {
				lower := started.Add(-2 * time.Hour)
				store.latest = completedIncrementalAttempt(
					"prior-run",
					"prior-attempt",
					lower.Add(-time.Hour),
					&state.TimestampWatermark{
						Column: "updated_at",
						Value:  lower,
					},
				)
				store.latestFound = true
				store.active = state.IncrementalAttempt{
					RunID: "run-1", Task: incrementalTask(),
					AttemptID: "attempt-1", Mode: state.IncrementalWindow,
					LowerWatermark: &state.TimestampWatermark{
						Column: "updated_at",
						Value:  lower,
					},
					UpperFence: &state.TimestampWatermark{
						Column: "updated_at",
						Value:  upper,
					},
					Status: state.IncrementalRunning, StartedAt: started,
				}
				store.activeFound = true
				request.SampleUpperFence = nil
			},
		},
		{
			name: "completed reuse",
			configure: func(
				store *incrementalFakeState,
				request *IncrementalExecutionRequest,
			) {
				store.latest = completedIncrementalAttempt(
					"run-1",
					"attempt-1",
					started,
					&state.TimestampWatermark{
						Column: "updated_at",
						Value:  upper,
					},
				)
				store.latestFound = true
				request.VerifyCompletedTable = func(
					context.Context,
					state.IncrementalAttempt,
				) error {
					t.Fatal("completed-table proof ran after binding failure")
					return nil
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			request := incrementalTestRequest(store, plan, started)
			test.configure(store, &request)
			bindingCalls := 0
			request.VerifyDurableBinding = func(
				_ context.Context,
				attempt state.IncrementalAttempt,
				planHash string,
				topologyHash string,
			) error {
				bindingCalls++
				if attempt.AttemptID != "attempt-1" ||
					planHash != plan.PlanHash ||
					topologyHash != request.TopologyHash {
					t.Fatalf(
						"binding evidence attempt=%#v plan=%q topology=%q",
						attempt,
						planHash,
						topologyHash,
					)
				}
				return injected
			}
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("stale durable binding reached transfer")
				return nil
			}
			result, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || result.Completed || bindingCalls != 1 ||
				!errors.Is(err, injected) ||
				ClassifyTransferError(err) != ErrorClassState {
				t.Fatalf(
					"result=%#v calls=%d err=%v class=%q",
					result,
					bindingCalls,
					err,
					ClassifyTransferError(err),
				)
			}
			if store.commit.AttemptID != "" {
				t.Fatalf("binding failure committed %#v", store.commit)
			}
		})
	}
}

func TestExecuteIncrementalCancellationAndFailureNeverPublishWatermark(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 15, 0, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	injected := errors.New("injected transfer failure")
	tests := []struct {
		name      string
		run       func(context.Context, context.CancelFunc, *incrementalFakeState) error
		wantBegin bool
	}{
		{
			name: "canceled before sampling",
			run: func(ctx context.Context, _ context.CancelFunc, store *incrementalFakeState) error {
				request := incrementalTestRequest(store, plan, started)
				request.SampleUpperFence = func(
					context.Context,
					IncrementalTable,
					IncrementalColumn,
				) (*time.Time, error) {
					t.Fatal("sampler called")
					return nil, nil
				}
				request.Transfer = func(context.Context, IncrementalReadPlan) error {
					t.Fatal("transfer called")
					return nil
				}
				_, err := ExecuteIncrementalTable(ctx, request)
				return err
			},
		},
		{
			name: "canceled after sample before begin",
			run: func(ctx context.Context, cancel context.CancelFunc, store *incrementalFakeState) error {
				request := incrementalTestRequest(store, plan, started)
				request.SampleUpperFence = func(
					context.Context,
					IncrementalTable,
					IncrementalColumn,
				) (*time.Time, error) {
					cancel()
					return &upper, nil
				}
				request.Transfer = func(context.Context, IncrementalReadPlan) error {
					t.Fatal("transfer called")
					return nil
				}
				_, err := ExecuteIncrementalTable(ctx, request)
				return err
			},
		},
		{
			name: "transfer failure leaves running attempt",
			run: func(ctx context.Context, _ context.CancelFunc, store *incrementalFakeState) error {
				request := incrementalTestRequest(store, plan, started)
				request.SampleUpperFence = fixedIncrementalFence(upper)
				request.Transfer = func(context.Context, IncrementalReadPlan) error {
					return injected
				}
				_, err := ExecuteIncrementalTable(ctx, request)
				return err
			},
			wantBegin: true,
		},
		{
			name: "canceled after transfer leaves running attempt",
			run: func(ctx context.Context, cancel context.CancelFunc, store *incrementalFakeState) error {
				request := incrementalTestRequest(store, plan, started)
				request.SampleUpperFence = fixedIncrementalFence(upper)
				request.Transfer = func(context.Context, IncrementalReadPlan) error {
					cancel()
					return nil
				}
				_, err := ExecuteIncrementalTable(ctx, request)
				return err
			},
			wantBegin: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			ctx, cancel := context.WithCancel(context.Background())
			if test.name == "canceled before sampling" {
				cancel()
			}
			err := test.run(ctx, cancel, store)
			if err == nil {
				t.Fatal("execution succeeded")
			}
			if test.name == "transfer failure leaves running attempt" {
				if !errors.Is(err, injected) {
					t.Fatalf("error = %v", err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
			if got := store.hasActive(); got != test.wantBegin {
				t.Fatalf("active attempt = %v, want %v", got, test.wantBegin)
			}
			if store.commit.AttemptID != "" {
				t.Fatalf("canceled/failed attempt published commit %#v", store.commit)
			}
		})
	}
}

func TestExecuteIncrementalStateFailuresAreClassifiedAndCompletionIsAtomic(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 16, 0, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	for _, test := range []struct {
		name      string
		configure func(*incrementalFakeState)
	}{
		{
			name: "load active",
			configure: func(store *incrementalFakeState) {
				store.loadActiveErr = errors.New("load active failed")
			},
		},
		{
			name: "load latest",
			configure: func(store *incrementalFakeState) {
				store.loadLatestErr = errors.New("load latest failed")
			},
		},
		{
			name: "begin",
			configure: func(store *incrementalFakeState) {
				store.beginErr = errors.New("begin failed")
			},
		},
		{
			name: "commit",
			configure: func(store *incrementalFakeState) {
				store.commitErr = errors.New("commit failed")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			test.configure(store)
			request := incrementalTestRequest(store, plan, started)
			request.SampleUpperFence = fixedIncrementalFence(upper)
			request.Transfer = func(context.Context, IncrementalReadPlan) error { return nil }
			_, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || ClassifyTransferError(err) != ErrorClassState {
				t.Fatalf("error = %v, class=%q", err, ClassifyTransferError(err))
			}
			if test.name == "commit" {
				if !store.hasActive() || store.active.Status != state.IncrementalRunning ||
					store.active.CommittedWatermark != nil || store.active.TableSucceeded {
					t.Fatalf("failed atomic commit exposed evidence %#v", store.active)
				}
				for _, required := range []string{
					"target writes may already be committed",
					"repair state and resume the existing run",
					"do not start a competing fresh run",
				} {
					if !strings.Contains(err.Error(), required) {
						t.Fatalf(
							"commit error %q lacks recovery guidance %q",
							err,
							required,
						)
					}
				}
			}
		})
	}
}

func TestExecuteIncrementalUsesDurableStage4AtomicCompletion(t *testing.T) {
	type durableIncrementalBackend interface {
		state.Backend
		state.RangeBackend
		state.Stage4Backend
	}
	for _, backendCase := range []struct {
		name    string
		backend func(*testing.T) durableIncrementalBackend
	}{
		{
			name: "yaml",
			backend: func(t *testing.T) durableIncrementalBackend {
				return state.YAMLStore{
					Path: filepath.Join(t.TempDir(), "state.yaml"),
				}
			},
		},
		{
			name: "sqlite",
			backend: func(t *testing.T) durableIncrementalBackend {
				return state.SQLiteStore{
					Path: filepath.Join(t.TempDir(), "state.db"),
				}
			},
		},
	} {
		t.Run(backendCase.name, func(t *testing.T) {
			for _, completeRange := range []bool{false, true} {
				name := "incomplete_range"
				if completeRange {
					name = "complete_range"
				}
				t.Run(name, func(t *testing.T) {
					backend := backendCase.backend(t)
					started := time.Date(2026, 7, 30, 16, 30, 0, 0, time.UTC)
					task := incrementalTask()
					topology := "durable-incremental-topology"
					if err := backend.InitializeRun(state.Run{
						ID: "run-1", Source: "source", Target: "target",
						SourceEngine:   "sqlite",
						SourceIdentity: "sqlite:/source.db",
						TargetIdentity: "sqlite:/target.db",
						Outcome:        state.Running, Resumable: true,
						Reason: "running", StartedAt: started,
					}, "config-hash"); err != nil {
						t.Fatal(err)
					}
					if created, err := backend.EnsureWorkPlan(
						state.WorkTask{
							RunID: "run-1", Key: task, Strategy: "incremental_window",
							TopologyHash: topology, StartedAt: started,
						},
						[]state.RangeState{{ID: "0"}},
					); err != nil || !created {
						t.Fatalf("ensure work plan created=%v err=%v", created, err)
					}
					upper := started.Add(time.Hour)
					request := incrementalTestRequest(
						backend,
						incrementalTestPlan(t),
						started,
					)
					request.TopologyHash = topology
					request.SampleUpperFence = fixedIncrementalFence(upper)
					request.Transfer = func(
						_ context.Context,
						_ IncrementalReadPlan,
					) error {
						if !completeRange {
							return nil
						}
						return backend.CompleteRange(
							"run-1",
							task,
							"0",
							topology,
							0,
							started.Add(90*time.Minute),
						)
					}
					result, err := ExecuteIncrementalTable(
						context.Background(),
						request,
					)
					if completeRange {
						if err != nil || !result.Completed {
							t.Fatalf("completed result=%#v err=%v", result, err)
						}
					} else if err == nil ||
						ClassifyTransferError(err) != ErrorClassState {
						t.Fatalf(
							"incomplete range error=%v class=%q",
							err,
							ClassifyTransferError(err),
						)
					}
					stored, found, loadErr := backend.LoadIncrementalAttempt(
						"run-1",
						task,
						"attempt-1",
					)
					if loadErr != nil || !found {
						t.Fatalf("stored attempt=%#v found=%v err=%v", stored, found, loadErr)
					}
					workTasks, _, loadErr := backend.ListWork("run-1")
					if loadErr != nil || len(workTasks) != 1 {
						t.Fatalf("work tasks=%#v err=%v", workTasks, loadErr)
					}
					if completeRange {
						if stored.Status != state.IncrementalCompleted ||
							!stored.TableSucceeded ||
							stored.CommittedWatermark == nil ||
							!stored.CommittedWatermark.Value.Equal(upper) ||
							workTasks[0].Status != "completed" {
							t.Fatalf(
								"atomic completion attempt=%#v work=%#v",
								stored,
								workTasks[0],
							)
						}
					} else if stored.Status != state.IncrementalRunning ||
						stored.TableSucceeded ||
						stored.CommittedWatermark != nil ||
						workTasks[0].Status != "running" {
						t.Fatalf(
							"failed completion leaked evidence attempt=%#v work=%#v",
							stored,
							workTasks[0],
						)
					}
				})
			}
		})
	}
}

func TestExecuteIncrementalCompletedSameRunDoesNotReplay(t *testing.T) {
	plan := incrementalTestPlan(t)
	store := newIncrementalFakeState()
	started := time.Date(2026, 7, 30, 17, 0, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	store.latest = completedIncrementalAttempt(
		"run-1",
		"attempt-1",
		started,
		&state.TimestampWatermark{Column: "updated_at", Value: upper},
	)
	store.latestFound = true
	request := incrementalTestRequest(store, plan, started.Add(2*time.Hour))
	completedProofs := 0
	request.VerifyCompletedTable = func(
		_ context.Context,
		attempt state.IncrementalAttempt,
	) error {
		completedProofs++
		if attempt.AttemptID != "attempt-1" ||
			attempt.Status != state.IncrementalCompleted ||
			!attempt.TableSucceeded {
			t.Fatalf("completed proof attempt = %#v", attempt)
		}
		return nil
	}
	request.SampleUpperFence = func(
		context.Context,
		IncrementalTable,
		IncrementalColumn,
	) (*time.Time, error) {
		t.Fatal("completed attempt sampled")
		return nil, nil
	}
	request.Transfer = func(context.Context, IncrementalReadPlan) error {
		t.Fatal("completed attempt replayed")
		return nil
	}
	result, err := ExecuteIncrementalTable(context.Background(), request)
	if err != nil || !result.AlreadyCompleted || !result.Completed ||
		result.Attempt.AttemptID != "attempt-1" || completedProofs != 1 {
		t.Fatalf("result = %#v proofs=%d err=%v", result, completedProofs, err)
	}
}

func TestExecuteIncrementalCompletedReuseRequiresSuccessfulCountProof(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 17, 15, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	injected := errors.New("target count differs from durable aggregate")
	for _, test := range []struct {
		name     string
		verifier IncrementalCompletedTableVerifier
		want     string
		cause    error
	}{
		{
			name: "missing verifier",
			want: "requires aggregate checkpoint and target-count revalidation",
		},
		{
			name: "failed verifier",
			verifier: func(context.Context, state.IncrementalAttempt) error {
				return injected
			},
			want:  "revalidate completed incremental aggregate checkpoint and target row count",
			cause: injected,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			store.latest = completedIncrementalAttempt(
				"run-1",
				"attempt-1",
				started,
				&state.TimestampWatermark{Column: "updated_at", Value: upper},
			)
			store.latestFound = true
			request := incrementalTestRequest(store, plan, started.Add(2*time.Hour))
			request.VerifyCompletedTable = test.verifier
			request.SampleUpperFence = func(
				context.Context,
				IncrementalTable,
				IncrementalColumn,
			) (*time.Time, error) {
				t.Fatal("unproven completed attempt sampled")
				return nil, nil
			}
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("unproven completed attempt transferred")
				return nil
			}
			result, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || result.Completed || result.AlreadyCompleted ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf(
					"result=%#v err=%v class=%q",
					result,
					err,
					ClassifyTransferError(err),
				)
			}
			if test.cause != nil && !errors.Is(err, test.cause) {
				t.Fatalf("error %v does not preserve %v", err, test.cause)
			}
			if store.commit.AttemptID != "" {
				t.Fatalf("unproven completed attempt committed %#v", store.commit)
			}
		})
	}
}

func TestExecuteIncrementalCompletedSameRunRequiresExactAttemptIdentity(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 17, 30, 0, 0, time.UTC)
	upper := started.Add(time.Hour)
	for _, requestedAttempt := range []string{"", "recovery-attempt"} {
		name := requestedAttempt
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			store := newIncrementalFakeState()
			store.latest = completedIncrementalAttempt(
				"run-1",
				"completed-attempt",
				started,
				&state.TimestampWatermark{Column: "updated_at", Value: upper},
			)
			store.latestFound = true
			request := incrementalTestRequest(store, plan, started.Add(2*time.Hour))
			request.AttemptID = requestedAttempt
			request.SampleUpperFence = func(
				context.Context,
				IncrementalTable,
				IncrementalColumn,
			) (*time.Time, error) {
				t.Fatal("identity mismatch sampled a new fence")
				return nil, nil
			}
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("identity mismatch falsely replayed or completed")
				return nil
			}
			result, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || result.Completed ||
				ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), "verify the target and repair state") {
				t.Fatalf(
					"identity mismatch result=%#v err=%v class=%q",
					result,
					err,
					ClassifyTransferError(err),
				)
			}
			if store.commit.AttemptID != "" {
				t.Fatalf("identity mismatch committed %#v", store.commit)
			}
		})
	}
}

func TestExecuteIncrementalRejectsRegressedFenceChangedColumnAndMutatedPlan(t *testing.T) {
	started := time.Date(2026, 7, 30, 18, 0, 0, 0, time.UTC)
	lower := started.Add(time.Hour)
	tests := []struct {
		name      string
		configure func(*incrementalFakeState, *IncrementalExecutionRequest)
		want      string
	}{
		{
			name: "regressed upper fence",
			configure: func(store *incrementalFakeState, request *IncrementalExecutionRequest) {
				store.latest = completedIncrementalAttempt(
					"prior",
					"prior-attempt",
					started,
					&state.TimestampWatermark{Column: "updated_at", Value: lower},
				)
				store.latestFound = true
				older := lower.Add(-time.Nanosecond)
				request.SampleUpperFence = fixedIncrementalFence(older)
			},
			want: "regresses below durable lower watermark",
		},
		{
			name: "changed watermark column",
			configure: func(store *incrementalFakeState, request *IncrementalExecutionRequest) {
				store.latest = completedIncrementalAttempt(
					"prior",
					"prior-attempt",
					started,
					&state.TimestampWatermark{Column: "modified_at", Value: lower},
				)
				store.latestFound = true
				request.SampleUpperFence = func(
					context.Context,
					IncrementalTable,
					IncrementalColumn,
				) (*time.Time, error) {
					t.Fatal("column drift sampled before failing")
					return nil, nil
				}
			},
			want: "uses column modified_at",
		},
		{
			name: "mutated plan",
			configure: func(_ *incrementalFakeState, request *IncrementalExecutionRequest) {
				request.Plan.Ordering[0].Column = "tampered"
				request.SampleUpperFence = fixedIncrementalFence(lower)
			},
			want: "mutated after planning",
		},
		{
			name: "zero sampled fence",
			configure: func(_ *incrementalFakeState, request *IncrementalExecutionRequest) {
				request.SampleUpperFence = fixedIncrementalFence(time.Time{})
			},
			want: "upper fence is zero",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := incrementalTestPlan(t)
			store := newIncrementalFakeState()
			request := incrementalTestRequest(store, plan, started.Add(2*time.Hour))
			test.configure(store, &request)
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("unsafe request reached transfer")
				return nil
			}
			_, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || ClassifyTransferError(err) != ErrorClassPolicy ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, class=%q", err, ClassifyTransferError(err))
			}
			if store.hasActive() || store.commit.AttemptID != "" {
				t.Fatalf("unsafe request mutated state: active=%#v commit=%#v", store.active, store.commit)
			}
		})
	}
}

func TestExecuteIncrementalResumeRejectsUnexpectedAttemptOrEvidence(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 19, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		active state.IncrementalAttempt
		want   string
	}{
		{
			name: "different active attempt ID",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "stored",
				Mode: state.IncrementalBaseline, Status: state.IncrementalRunning,
				StartedAt: started,
			},
			want: "not requested attempt",
		},
		{
			name: "different upper-fence column",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode: state.IncrementalWindow, Status: state.IncrementalRunning,
				StartedAt: started,
				UpperFence: &state.TimestampWatermark{
					Column: "modified_at", Value: started.Add(time.Hour),
				},
			},
			want: "uses column modified_at",
		},
		{
			name: "unknown status",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode: state.IncrementalBaseline, Status: "paused", StartedAt: started,
			},
			want: "unknown status",
		},
		{
			name: "blank loaded watermark column",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode: state.IncrementalWindow, Status: state.IncrementalRunning,
				StartedAt: started,
				LowerWatermark: &state.TimestampWatermark{
					Column: " \t ", Value: started,
				},
			},
			want: "has a blank column",
		},
		{
			name: "zero loaded watermark value",
			active: state.IncrementalAttempt{
				RunID: "run-1", Task: incrementalTask(), AttemptID: "attempt-1",
				Mode: state.IncrementalWindow, Status: state.IncrementalRunning,
				StartedAt: started,
				LowerWatermark: &state.TimestampWatermark{
					Column: "updated_at", Value: time.Time{},
				},
			},
			want: "has a zero value",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			store.active = test.active
			store.activeFound = true
			request := incrementalTestRequest(store, plan, started)
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("unsafe active evidence reached transfer")
				return nil
			}
			_, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v", err)
			}
			if ClassifyTransferError(err) != ErrorClassState &&
				ClassifyTransferError(err) != ErrorClassPolicy {
				t.Fatalf("error class = %q", ClassifyTransferError(err))
			}
			if store.commit.AttemptID != "" {
				t.Fatalf("corrupted evidence reached state commit %#v", store.commit)
			}
			if got, want := store.eventsSnapshot(), []string{"load_active"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("corrupted evidence events = %#v, want %#v", got, want)
			}
		})
	}
}

func TestExecuteIncrementalRejectsCorruptedHistoricalWatermarkBeforeSampling(t *testing.T) {
	plan := incrementalTestPlan(t)
	started := time.Date(2026, 7, 30, 20, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		watermark state.TimestampWatermark
		want      string
	}{
		{
			name: "blank column",
			watermark: state.TimestampWatermark{
				Column: " ", Value: started,
			},
			want: "has a blank column",
		},
		{
			name: "zero value",
			watermark: state.TimestampWatermark{
				Column: "updated_at", Value: time.Time{},
			},
			want: "has a zero value",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newIncrementalFakeState()
			store.latest = state.IncrementalAttempt{
				RunID: "prior-run", Task: incrementalTask(),
				AttemptID: "prior-attempt", Mode: state.IncrementalBaseline,
				UpperFence:         cloneTimestampWatermark(&test.watermark),
				Status:             state.IncrementalCompleted,
				CommittedWatermark: cloneTimestampWatermark(&test.watermark),
				TableSucceeded:     true,
				StartedAt:          started,
				CompletedAt:        started.Add(time.Hour),
			}
			store.latestFound = true
			request := incrementalTestRequest(store, plan, started.Add(2*time.Hour))
			request.SampleUpperFence = func(
				context.Context,
				IncrementalTable,
				IncrementalColumn,
			) (*time.Time, error) {
				t.Fatal("corrupted historical evidence reached sampler")
				return nil, nil
			}
			request.Transfer = func(context.Context, IncrementalReadPlan) error {
				t.Fatal("corrupted historical evidence reached transfer")
				return nil
			}
			_, err := ExecuteIncrementalTable(context.Background(), request)
			if err == nil || ClassifyTransferError(err) != ErrorClassState ||
				!strings.Contains(err.Error(), test.want) {
				t.Fatalf("error=%v class=%q", err, ClassifyTransferError(err))
			}
			if store.hasActive() || store.commit.AttemptID != "" {
				t.Fatalf(
					"corrupted historical evidence mutated state: %#v %#v",
					store.active,
					store.commit,
				)
			}
			if got, want := store.eventsSnapshot(), []string{
				"load_active",
				"load_latest",
			}; !reflect.DeepEqual(got, want) {
				t.Fatalf("events=%#v want=%#v", got, want)
			}
		})
	}
}

func incrementalTestTable() IncrementalTable {
	return IncrementalTable{
		Schema: "public",
		Name:   "events",
		Columns: []IncrementalColumn{
			{
				Name: "tenant_id", PrimaryKeyPosition: 1,
				OrderAdmission: IncrementalOrderExact,
			},
			{
				Name: "id", PrimaryKeyPosition: 2,
				OrderAdmission: IncrementalOrderExact,
			},
			{
				Name: "updated_at", TemporalKind: IncrementalTemporalTimestamp,
				Nullable: true, OrderAdmission: IncrementalOrderExact,
			},
			{
				Name: "created_at", TemporalKind: IncrementalTemporalDate,
				OrderAdmission: IncrementalOrderExact,
			},
			{Name: "payload", OrderAdmission: IncrementalOrderExact},
		},
	}
}

func incrementalTestPlan(t *testing.T) IncrementalTablePlan {
	t.Helper()
	plan, err := BuildIncrementalTablePlan(
		incrementalTestTable(),
		[]string{"updated_at"},
	)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func incrementalTask() state.TaskKey {
	return state.TaskKey{
		Type: "table-copy", Schema: "public", Table: "events",
	}
}

func incrementalTestRequest(
	store IncrementalState,
	plan IncrementalTablePlan,
	started time.Time,
) IncrementalExecutionRequest {
	return IncrementalExecutionRequest{
		State:        store,
		RunID:        "run-1",
		Task:         incrementalTask(),
		AttemptID:    "attempt-1",
		TopologyHash: "incremental-topology",
		StartedAt:    started,
		Plan:         plan,
		VerifyDurableBinding: func(
			context.Context,
			state.IncrementalAttempt,
			string,
			string,
		) error {
			return nil
		},
		CompletionTime: func() time.Time {
			return started.Add(2 * time.Hour)
		},
	}
}

func fixedIncrementalFence(value time.Time) IncrementalFenceSampler {
	return func(
		context.Context,
		IncrementalTable,
		IncrementalColumn,
	) (*time.Time, error) {
		return &value, nil
	}
}

func completedIncrementalAttempt(
	runID string,
	attemptID string,
	started time.Time,
	watermark *state.TimestampWatermark,
) state.IncrementalAttempt {
	return state.IncrementalAttempt{
		RunID:              runID,
		Task:               incrementalTask(),
		AttemptID:          attemptID,
		Mode:               state.IncrementalBaseline,
		UpperFence:         cloneTimestampWatermark(watermark),
		Status:             state.IncrementalCompleted,
		CommittedWatermark: cloneTimestampWatermark(watermark),
		TableSucceeded:     true,
		StartedAt:          started,
		CompletedAt:        started.Add(time.Hour),
	}
}

func incrementalColumnNames(columns []IncrementalColumn) []string {
	names := make([]string, len(columns))
	for index, column := range columns {
		names[index] = column.Name
	}
	return names
}

func incrementalOrderNames(terms []IncrementalOrderTerm) []string {
	names := make([]string, len(terms))
	for index, term := range terms {
		names[index] = term.Column + ":" + string(term.Role) + ":" + string(term.Nulls)
	}
	return names
}

func incrementalDecisionActions(
	decisions []IncrementalCandidateDecision,
) []IncrementalCandidateAction {
	actions := make([]IncrementalCandidateAction, len(decisions))
	for index, decision := range decisions {
		actions[index] = decision.Action
	}
	return actions
}

func gotPKOrder(terms []IncrementalOrderTerm) string {
	names := make([]string, 0, len(terms))
	for _, term := range terms {
		if term.Role == IncrementalOrderPrimaryKey {
			names = append(names, term.Column)
		}
	}
	return strings.Join(names, ",")
}

type incrementalFakeState struct {
	mu sync.Mutex

	active      state.IncrementalAttempt
	activeFound bool
	latest      state.IncrementalAttempt
	latestFound bool
	commit      state.IncrementalCommit
	events      []string

	loadActiveErr error
	loadLatestErr error
	beginErr      error
	commitErr     error
}

func newIncrementalFakeState() *incrementalFakeState {
	return &incrementalFakeState{}
}

func (store *incrementalFakeState) record(event string) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.events = append(store.events, event)
}

func (store *incrementalFakeState) eventsSnapshot() []string {
	store.mu.Lock()
	defer store.mu.Unlock()
	return append([]string(nil), store.events...)
}

func (store *incrementalFakeState) hasActive() bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.activeFound
}

func (store *incrementalFakeState) LoadActiveIncrementalAttempt(
	_ string,
	_ state.TaskKey,
) (state.IncrementalAttempt, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.events = append(store.events, "load_active")
	return cloneIncrementalAttempt(store.active), store.activeFound, store.loadActiveErr
}

func (store *incrementalFakeState) LoadLatestCommittedIncrementalAttempt(
	_ string,
	_ state.TaskKey,
) (state.IncrementalAttempt, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.events = append(store.events, "load_latest")
	return cloneIncrementalAttempt(store.latest), store.latestFound, store.loadLatestErr
}

func (store *incrementalFakeState) BeginIncrementalAttempt(
	attempt state.IncrementalAttempt,
) (state.IncrementalAttempt, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.events = append(store.events, "begin")
	if store.beginErr != nil {
		return state.IncrementalAttempt{}, false, store.beginErr
	}
	if store.activeFound {
		return cloneIncrementalAttempt(store.active), false, nil
	}
	attempt.Status = state.IncrementalRunning
	store.active = cloneIncrementalAttempt(attempt)
	store.activeFound = true
	return cloneIncrementalAttempt(attempt), true, nil
}

func (store *incrementalFakeState) CommitIncrementalAttempt(
	commit state.IncrementalCommit,
) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.events = append(store.events, "commit")
	if store.commitErr != nil {
		return store.commitErr
	}
	store.commit = commit
	store.active.Status = state.IncrementalCompleted
	store.active.CommittedWatermark = cloneTimestampWatermark(commit.Watermark)
	store.active.TableSucceeded = true
	store.active.CompletedAt = commit.CompletedAt
	store.activeFound = false
	return nil
}
