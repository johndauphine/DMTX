package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const configurationHashVersion = 2

type sqliteHashIdentityMode uint8

const (
	sqliteHashIdentityCanonical sqliteHashIdentityMode = iota
	sqliteHashIdentityPath
	sqliteHashIdentityFile
)

// Hash returns a stable fingerprint of data-plane configuration without secrets.
func Hash(value Config) (string, error) {
	return hashWithSQLiteIdentityModes(
		value,
		sqliteHashIdentityCanonical,
		sqliteHashIdentityCanonical,
	)
}

// LegacySQLitePathHash reproduces the configuration hash written before a
// single-link SQLite file later gained a hardlink. It exists only to bridge
// durable resume evidence across that safe identity transition; new evidence
// must continue to use Hash.
func LegacySQLitePathHash(value Config) (string, error) {
	return hashWithSQLiteIdentityModes(
		value,
		sqliteHashIdentityPath,
		sqliteHashIdentityPath,
	)
}

// SQLiteFileIdentityHash reproduces evidence written while a SQLite file had
// multiple hardlinks, even if aliases were later removed. Resume may use this
// only after independently proving that the current endpoint is the persisted
// workload.
func SQLiteFileIdentityHash(value Config) (string, error) {
	return hashWithSQLiteIdentityModes(
		value,
		sqliteHashIdentityFile,
		sqliteHashIdentityFile,
	)
}

// SQLiteIdentityHashCandidates returns every independently canonical, path,
// or file-based SQLite endpoint identity combination. The canonical Hash is
// first. Resume may compare these candidates only after separately proving
// both persisted workload endpoints.
func SQLiteIdentityHashCandidates(value Config) ([]string, error) {
	modes := []sqliteHashIdentityMode{
		sqliteHashIdentityCanonical,
		sqliteHashIdentityPath,
		sqliteHashIdentityFile,
	}
	candidates := make([]string, 0, len(modes)*len(modes))
	seen := make(map[string]struct{}, cap(candidates))
	for _, sourceMode := range modes {
		for _, targetMode := range modes {
			candidate, err := hashWithSQLiteIdentityModes(
				value,
				sourceMode,
				targetMode,
			)
			if err != nil {
				return nil, err
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			candidates = append(candidates, candidate)
		}
	}
	return candidates, nil
}

func hashWithSQLiteIdentityModes(
	value Config,
	sourceIdentityMode sqliteHashIdentityMode,
	targetIdentityMode sqliteHashIdentityMode,
) (string, error) {
	intent := canonicalMigrationIntent(value.Migration)
	normalizeDefaults(&value)
	source, err := canonicalEndpointForHash(
		value.Source,
		sourceIdentityMode,
	)
	if err != nil {
		return "", fmt.Errorf("canonicalize source configuration: %w", err)
	}
	target, err := canonicalEndpointForHash(
		value.Target,
		targetIdentityMode,
	)
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

func canonicalEndpointForHash(
	endpoint Endpoint,
	sqliteIdentityMode sqliteHashIdentityMode,
) (Endpoint, error) {
	engine, err := CanonicalEngine(endpoint.Type)
	if err != nil {
		return Endpoint{}, err
	}
	endpoint.Type = engine
	if engine == "sqlite" {
		var database string
		switch sqliteIdentityMode {
		case sqliteHashIdentityPath:
			database, err = canonicalSQLitePathHashIdentity(endpoint.Database)
		case sqliteHashIdentityFile:
			database, err = canonicalSQLiteFileHashIdentity(endpoint.Database)
		default:
			database, err = canonicalSQLiteHashIdentity(endpoint.Database)
		}
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
