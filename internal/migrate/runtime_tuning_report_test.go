package migrate

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestRuntimeTuningReportJSONUsesExplicitPublicDTO(t *testing.T) {
	t.Parallel()

	plan := runtimeTuningTestPlan()
	values := RuntimeTuningValues{
		ChunkRows: RuntimeTuningValue{
			Value:             32,
			IntentValue:       64,
			IntentProvenance:  config.ProvenanceDerived,
			LiveProvenance:    RuntimeTuningSafetyReduction,
			PerformancePinned: false,
		},
		Writers: RuntimeTuningValue{
			Value:             1,
			IntentValue:       2,
			IntentProvenance:  config.ProvenanceDerived,
			LiveProvenance:    RuntimeTuningSafetyReduction,
			PerformancePinned: false,
		},
		BufferDepth: RuntimeTuningValue{
			Value:             2,
			IntentValue:       2,
			IntentProvenance:  config.ProvenanceDerived,
			LiveProvenance:    RuntimeTuningInitial,
			PerformancePinned: false,
		},
	}
	boundary := RuntimeTuningBoundary{
		Ordinal:       2,
		TableSchema:   "public",
		TableName:     "items",
		RangeIndex:    0,
		ChunkSequence: 1,
		Attempt:       1,
	}
	report := RuntimeTuningReport{
		Enabled: true,
		Tables: []RuntimeTuningTableReport{
			runtimeTuningTableReport(
				"public",
				"items",
				RuntimeTuningSnapshot{
					Intent:                plan,
					Effective:             values,
					InitializationReasons: []RuntimeTuningReason{RuntimeReasonMemoryCeiling},
					Interval:              5 * time.Second,
					HasBoundary:           true,
					LastBoundary:          boundary,
					AppliedBoundaries:     2,
					TotalDecisions:        2,
					RetainedDecisions:     2,
					HealthyBoundaries:     1,
					TrustedRowWidthBytes:  128,
				},
				[]RuntimeTuningDecision{{
					Boundary: boundary,
					Before:   values,
					After:    values,
					Reasons:  []RuntimeTuningReason{RuntimeReasonProtocolWriteError},
				}},
			),
		},
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "\"Intent\"") ||
		strings.Contains(string(encoded), "5000000000") {
		t.Fatalf("runtime tuning JSON leaked internal fields/duration: %s", encoded)
	}
	var document map[string]any
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	tables, ok := document["tables"].([]any)
	if !ok || len(tables) != 1 {
		t.Fatalf("tables JSON = %#v", document["tables"])
	}
	table, ok := tables[0].(map[string]any)
	if !ok {
		t.Fatalf("table JSON = %#v", tables[0])
	}
	snapshot, ok := table["snapshot"].(map[string]any)
	if !ok || snapshot["interval"] != "5s" {
		t.Fatalf("snapshot JSON = %#v", snapshot)
	}
	if _, exists := snapshot["Interval"]; exists {
		t.Fatalf("snapshot retained internal field names: %#v", snapshot)
	}
	intent, ok := snapshot["intent"].(map[string]any)
	if !ok || intent["target_mode"] != "upsert" {
		t.Fatalf("intent JSON = %#v", intent)
	}
	effective, ok := snapshot["effective"].(map[string]any)
	if !ok {
		t.Fatalf("effective JSON = %#v", snapshot["effective"])
	}
	chunkRows, ok := effective["chunk_rows"].(map[string]any)
	if !ok || chunkRows["live_provenance"] != "safety_reduction" {
		t.Fatalf("chunk rows JSON = %#v", chunkRows)
	}
	adjustments, ok := table["adjustments"].([]any)
	if !ok || len(adjustments) != 1 {
		t.Fatalf("adjustments JSON = %#v", table["adjustments"])
	}
}
