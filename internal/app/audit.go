package app

import (
	"fmt"
	"time"

	"github.com/johndauphine/dmtx/internal/audit"
)

func appendAudit(configPath, runID, eventType string) error {
	if err := audit.Append(configPath+".audit.ndjson", runID, eventType, time.Now().UTC()); err != nil {
		return fmt.Errorf("record audit event: %w", err)
	}
	return nil
}
