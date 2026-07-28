package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// ResumeCompatibilityHash fingerprints configuration that can change which
// rows are observed or how existing target rows are interpreted. Runtime
// resource controls are deliberately excluded: their concrete pagination
// topology is checked independently against durable range state.
func ResumeCompatibilityHash(value Config) (string, error) {
	normalizeDefaults(&value)
	projection := struct {
		Source    resumeEndpoint  `json:"source"`
		Target    resumeEndpoint  `json:"target"`
		Migration resumeMigration `json:"migration"`
	}{
		Source: resumeEndpointFrom(value.Source),
		Target: resumeEndpointFrom(value.Target),
		Migration: resumeMigration{
			TargetMode:             value.Migration.TargetMode,
			IncludeTables:          append([]string(nil), value.Migration.IncludeTables...),
			ExcludeTables:          append([]string(nil), value.Migration.ExcludeTables...),
			LargeTableThreshold:    value.Migration.LargeTableThreshold,
			StrictConsistency:      value.Migration.StrictConsistency,
			StrictConsistencyScope: value.Migration.StrictConsistencyScope,
		},
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("encode resume compatibility hash: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

type resumeEndpoint struct {
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Schema   string `json:"schema"`
}

func resumeEndpointFrom(endpoint Endpoint) resumeEndpoint {
	return resumeEndpoint{
		Type:     endpoint.Type,
		Host:     endpoint.Host,
		Port:     endpoint.Port,
		Database: endpoint.Database,
		User:     endpoint.User,
		Schema:   endpoint.Schema,
	}
}

type resumeMigration struct {
	TargetMode             string   `json:"target_mode"`
	IncludeTables          []string `json:"include_tables"`
	ExcludeTables          []string `json:"exclude_tables"`
	LargeTableThreshold    int64    `json:"large_table_threshold"`
	StrictConsistency      bool     `json:"strict_consistency"`
	StrictConsistencyScope string   `json:"strict_consistency_scope"`
}
