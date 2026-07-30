package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const configurationHashVersion = 2

// Hash returns a stable fingerprint of data-plane configuration without secrets.
func Hash(value Config) (string, error) {
	intent := canonicalMigrationIntent(value.Migration)
	normalizeDefaults(&value)
	source, err := canonicalEndpointForHash(value.Source)
	if err != nil {
		return "", fmt.Errorf("canonicalize source configuration: %w", err)
	}
	target, err := canonicalEndpointForHash(value.Target)
	if err != nil {
		return "", fmt.Errorf("canonicalize target configuration: %w", err)
	}
	value.Source = source
	value.Target = target
	sanitized := Sanitize(value)
	projection := struct {
		Version                  int      `json:"version"`
		Config                   Config   `json:"config"`
		RequestedMigrationFields []string `json:"requested_migration_fields"`
	}{
		Version:                  configurationHashVersion,
		Config:                   sanitized,
		RequestedMigrationFields: intent,
	}
	encoded, err := json.Marshal(projection)
	if err != nil {
		return "", fmt.Errorf("encode configuration hash: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func canonicalEndpointForHash(endpoint Endpoint) (Endpoint, error) {
	engine, err := CanonicalEngine(endpoint.Type)
	if err != nil {
		return Endpoint{}, err
	}
	endpoint.Type = engine
	if engine == "sqlite" {
		database, err := canonicalSQLiteHashIdentity(endpoint.Database)
		if err != nil {
			return Endpoint{}, err
		}
		return Endpoint{
			Type:     engine,
			Database: database,
		}, nil
	}
	endpoint.Host = canonicalNetworkHost(endpoint.Host)
	endpoint.Port = effectivePort(endpoint)
	endpoint.SSLMode = strings.ToLower(strings.TrimSpace(endpoint.SSLMode))
	endpoint.Password = ""
	return endpoint, nil
}

func canonicalNetworkHost(host string) string {
	return strings.TrimRight(
		strings.ToLower(strings.TrimSpace(host)),
		".",
	)
}

func normalizeDefaults(value *Config) {
	if value.Source.Type == "" {
		value.Source.Type = "mssql"
	}
	if value.Target.Type == "" {
		value.Target.Type = "postgres"
	}
	if value.Migration.TargetMode == "" {
		value.Migration.TargetMode = "drop_recreate"
	}
	if value.Migration.SchemaContract != nil {
		contract := *value.Migration.SchemaContract
		value.Migration.SchemaContract = &contract
	} else if value.Migration.SchemaEvolution != nil {
		legacy := *value.Migration.SchemaEvolution
		value.Migration.SchemaContract = &legacy
	}
	applyTransferDefaults(&value.Migration)
	applyProductionSemanticsDefaults(&value.Migration)
}
