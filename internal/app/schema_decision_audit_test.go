package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/audit"
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/schema"
)

func TestTableCheckpointObserverPublishesStableCompleteSchemaDecisionAudit(
	t *testing.T,
) {
	configPath := filepath.Join(t.TempDir(), "migration.yaml")
	digest := strings.Repeat("a", 64)
	reports := []migrate.Stage4SchemaDecisionReport{
		{
			RunID:                  "fresh-schema-decisions",
			Resume:                 false,
			Baseline:               false,
			SourceEngine:           "postgres",
			TargetEngine:           "mysql",
			TargetMode:             "upsert",
			GateTopologyHash:       digest,
			PreviousSchemaDigest:   digest,
			CurrentSchemaDigest:    strings.Repeat("b", 64),
			SuccessfulSchemaDigest: strings.Repeat("c", 64),
			Decisions: []migrate.SchemaContractDecision{{
				Entity:     schema.SchemaContractDataType,
				Mode:       config.SchemaContractReport,
				ChangeKind: schema.SchemaDriftDataTypeChanged,
				Object: schema.SchemaDriftObject{
					Kind:   schema.SchemaDriftObjectDataType,
					Schema: "public",
					Table:  "items",
					Column: "payload",
				},
				Previous: json.RawMessage(`{"type":"text"}`),
				Current:  json.RawMessage(`{"type":"varchar","length":80}`),
				Action:   migrate.SchemaContractReport,
				Reason:   "report preserves the current transfer value",
			}},
		},
		{
			RunID:                  "resume-schema-decisions",
			Resume:                 true,
			Baseline:               false,
			SourceEngine:           "postgres",
			TargetEngine:           "mysql",
			TargetMode:             "upsert",
			GateTopologyHash:       digest,
			PreviousSchemaDigest:   digest,
			CurrentSchemaDigest:    strings.Repeat("d", 64),
			SuccessfulSchemaDigest: strings.Repeat("e", 64),
			Decisions: []migrate.SchemaContractDecision{{
				Entity:     schema.SchemaContractColumns,
				Mode:       config.SchemaContractDiscardValue,
				ChangeKind: schema.SchemaDriftColumnAdded,
				Object: schema.SchemaDriftObject{
					Kind:   schema.SchemaDriftObjectColumn,
					Schema: "public",
					Table:  "items",
					Column: "transient",
				},
				Previous: json.RawMessage(`null`),
				Current:  json.RawMessage(`{"name":"transient","type":"text"}`),
				Action:   migrate.SchemaContractDiscardValue,
				Reason:   "discard_value omits the eligible added column",
			}},
		},
	}

	for _, report := range reports {
		observer := tableCheckpointObserver{
			runID:      report.RunID,
			resume:     report.Resume,
			configPath: configPath,
		}
		for repetition := 0; repetition < 2; repetition++ {
			if err := observer.ObserveStage4SchemaDecisions(
				context.Background(),
				report,
			); err != nil {
				t.Fatalf(
					"publish %s repetition %d: %v",
					report.RunID,
					repetition,
					err,
				)
			}
		}
		if found, err := audit.HasEvent(
			configPath+".audit.ndjson",
			report.RunID,
			stage4SchemaDecisionsAuditEvent,
		); err != nil || !found {
			t.Fatalf(
				"audit event for %s found=%v err=%v",
				report.RunID,
				found,
				err,
			)
		}
	}

	data, err := os.ReadFile(configPath + ".audit.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("password")) ||
		bytes.Contains(data, []byte("credential")) {
		t.Fatalf("schema decision audit contains a secret field: %s", data)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	if len(lines) != 4 {
		t.Fatalf("audit event count = %d, want 4", len(lines))
	}
	for index, line := range lines {
		var event audit.Event
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		wantReport := reports[index/2]
		wantPayload, err := json.Marshal(wantReport)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(event.Payload, wantPayload) {
			t.Fatalf(
				"payload %d is not stable and complete:\n got %s\nwant %s",
				index,
				event.Payload,
				wantPayload,
			)
		}
	}
}

func TestTableCheckpointObserverRejectsMismatchedSchemaDecisionRun(
	t *testing.T,
) {
	observer := tableCheckpointObserver{
		runID:      "owned-run",
		configPath: filepath.Join(t.TempDir(), "migration.yaml"),
	}
	err := observer.ObserveStage4SchemaDecisions(
		context.Background(),
		migrate.Stage4SchemaDecisionReport{RunID: "other-run"},
	)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("run mismatch error = %v", err)
	}
}
