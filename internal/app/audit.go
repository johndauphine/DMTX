package app

import (
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
)

const stage4SchemaDecisionsAuditEvent = "stage4_schema_decisions"

func appendAudit(configPath, runID, eventType string) error {
	if err := audit.Append(configPath+".audit.ndjson", runID, eventType, time.Now().UTC()); err != nil {
		return fmt.Errorf("record audit event: %w", err)
	}
	return nil
}

func appendAuditPayload(
	configPath string,
	runID string,
	eventType string,
	payload any,
) error {
	if err := audit.AppendPayload(
		configPath+".audit.ndjson",
		runID,
		eventType,
		payload,
		time.Now().UTC(),
	); err != nil {
		return fmt.Errorf("record audit event: %w", err)
	}
	return nil
}
