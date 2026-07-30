package migrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestPlanMySQLSourceValueChecksCoversTemporalAndJSONColumns(
	t *testing.T,
) {
	checks, err := planMySQLSourceValueChecks(
		"app",
		[]schema.Table{{
			Schema: "app",
			Name:   "events",
			Columns: []schema.Column{
				{Name: "id", Type: "bigint"},
				{Name: "occurred_at", Type: "datetime"},
				{Name: "observed_on", Type: "date"},
				{Name: "local_time", Type: "time"},
				{Name: "document", Type: "json"},
			},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(checks) != 4 {
		t.Fatalf("checks = %#v", checks)
	}
	for _, expected := range []struct {
		column string
		kind   string
		parts  []string
	}{
		{
			column: "occurred_at",
			kind:   "temporal",
			parts: []string{
				"`app`.`events`",
				"YEAR(`occurred_at`) = 0",
				"LAST_DAY(`occurred_at`) IS NULL",
			},
		},
		{
			column: "observed_on",
			kind:   "temporal",
			parts: []string{
				"`app`.`events`",
				"DAYOFMONTH(`observed_on`) = 0",
			},
		},
		{
			column: "local_time",
			kind:   "TIME clock",
			parts: []string{
				"`app`.`events`",
				"TIME_TO_SEC(`local_time`) < 0",
				"TIME_TO_SEC(`local_time`) >= 86400",
			},
		},
		{
			column: "document",
			kind:   "JSON",
			parts: []string{
				"`app`.`events`",
				"JSON_VALID(`document`)",
			},
		},
	} {
		var got *mySQLSourceValueCheck
		for index := range checks {
			if checks[index].column == expected.column {
				got = &checks[index]
				break
			}
		}
		if got == nil {
			t.Fatalf("missing check for %s: %#v", expected.column, checks)
		}
		if got.table != "events" || got.kind != expected.kind {
			t.Fatalf("check for %s = %#v", expected.column, got)
		}
		for _, part := range expected.parts {
			if !strings.Contains(got.query, part) {
				t.Fatalf(
					"check for %s omits %q: %s",
					expected.column,
					part,
					got.query,
				)
			}
		}
	}
}

func TestPlanMySQLSourceValueChecksFailsClosedOnNamespaceMismatch(
	t *testing.T,
) {
	for name, namespace := range map[string]string{
		"empty namespace":     "",
		"different namespace": "other",
	} {
		t.Run(name, func(t *testing.T) {
			tableNamespace := "app"
			if namespace == "other" {
				tableNamespace = "different"
			}
			_, err := planMySQLSourceValueChecks(
				namespace,
				[]schema.Table{{
					Schema: tableNamespace,
					Name:   "events",
				}},
			)
			if err == nil {
				t.Fatal("expected namespace policy failure")
			}
		})
	}
}

type rejectingSourceRowPreflightAdapter struct {
	*recordingAdapterSource
	err error
}

func (source *rejectingSourceRowPreflightAdapter) PreflightRows(
	context.Context,
	[]schema.Table,
) error {
	*source.events = append(*source.events, "source_row_preflight")
	return source.err
}

func TestAdapterRunnerInvokesSourceRowPreflightBeforeTargetPlanning(
	t *testing.T,
) {
	events := make([]string, 0)
	forced := errors.New("legacy source value rejected")
	source := &rejectingSourceRowPreflightAdapter{
		recordingAdapterSource: &recordingAdapterSource{
			events: &events,
			table: schema.Table{
				Schema: "public",
				Name:   "items",
				Columns: []schema.Column{
					{Name: "id", PrimaryKey: true},
					{Name: "payload"},
				},
			},
		},
		err: forced,
	}
	target := &recordingAdapterTarget{events: &events}
	result, err := migrateWithAdapters(
		context.Background(),
		config.Config{},
		recordingTableObserver{events: &events},
		source,
		target,
	)
	if !errors.Is(err, forced) ||
		!strings.Contains(err.Error(), "preflight source rows") {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
	if len(target.planned) != 0 ||
		len(target.prepared) != 0 ||
		len(target.written) != 0 {
		t.Fatalf(
			"target activity after source-row rejection: plan=%v prepare=%v write=%v",
			target.planned,
			target.prepared,
			target.written,
		)
	}
	if got := fmt.Sprint(events); got != fmt.Sprint([]string{
		"source_list",
		"source_inspect",
		"source_row_preflight",
	}) {
		t.Fatalf("events = %s", got)
	}
}
