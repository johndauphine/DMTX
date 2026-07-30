package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ResumeCompatibilityHash fingerprints configuration that can change which
// rows are observed or how existing target rows are interpreted. Runtime
// resource controls are deliberately excluded: their concrete pagination
// topology is checked independently against durable range state.
func ResumeCompatibilityHash(value Config) (string, error) {
	return resumeCompatibilityHashWithSQLiteIdentityModes(
		value,
		sqliteHashIdentityCanonical,
		sqliteHashIdentityCanonical,
	)
}

// LegacySQLitePathResumeCompatibilityHash reproduces compatibility evidence
// written while a SQLite endpoint still had a single filesystem link. It is a
// narrow resume bridge; new evidence must use ResumeCompatibilityHash.
func LegacySQLitePathResumeCompatibilityHash(value Config) (string, error) {
	return resumeCompatibilityHashWithSQLiteIdentityModes(
		value,
		sqliteHashIdentityPath,
		sqliteHashIdentityPath,
	)
}

// SQLiteFileIdentityResumeCompatibilityHash reproduces compatibility evidence
// written while a SQLite file had multiple hardlinks. Resume may use it only
// after endpoint ownership has been proven independently.
func SQLiteFileIdentityResumeCompatibilityHash(value Config) (string, error) {
	return resumeCompatibilityHashWithSQLiteIdentityModes(
		value,
		sqliteHashIdentityFile,
		sqliteHashIdentityFile,
	)
}

// SQLiteIdentityResumeCompatibilityHashCandidates returns every independent
// source/target SQLite identity combination, with the canonical compatibility
// hash first. Resume may use them only after proving both workload endpoints.
func SQLiteIdentityResumeCompatibilityHashCandidates(
	value Config,
) ([]string, error) {
	modes := []sqliteHashIdentityMode{
		sqliteHashIdentityCanonical,
		sqliteHashIdentityPath,
		sqliteHashIdentityFile,
	}
	candidates := make([]string, 0, len(modes)*len(modes))
	seen := make(map[string]struct{}, cap(candidates))
	for _, sourceMode := range modes {
		for _, targetMode := range modes {
			candidate, err :=
				resumeCompatibilityHashWithSQLiteIdentityModes(
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

func resumeCompatibilityHashWithSQLiteIdentityModes(
	value Config,
	sourceIdentityMode sqliteHashIdentityMode,
	targetIdentityMode sqliteHashIdentityMode,
) (string, error) {
	normalizeDefaults(&value)
	source, err := resumeEndpointFrom(
		value.Source,
		sourceIdentityMode,
	)
	if err != nil {
		return "", fmt.Errorf("canonicalize source resume identity: %w", err)
	}
	target, err := resumeEndpointFrom(
		value.Target,
		targetIdentityMode,
	)
	if err != nil {
		return "", fmt.Errorf("canonicalize target resume identity: %w", err)
	}
	projection := struct {
		Source    resumeEndpoint  `json:"source"`
		Target    resumeEndpoint  `json:"target"`
		Migration resumeMigration `json:"migration"`
	}{
		Source: source,
		Target: target,
		Migration: resumeMigration{
			TargetMode:             value.Migration.TargetMode,
			IncludeTables:          append([]string(nil), value.Migration.IncludeTables...),
			ExcludeTables:          append([]string(nil), value.Migration.ExcludeTables...),
			DateUpdatedColumns:     append([]string(nil), value.Migration.DateUpdatedColumns...),
			LargeTableThreshold:    value.Migration.LargeTableThreshold,
			StrictConsistency:      value.Migration.StrictConsistency,
			StrictConsistencyScope: value.Migration.StrictConsistencyScope,
			SchemaContract:         cloneSchemaContract(value.Migration.SchemaContract),
			Deletes:                resumeDeletePolicyFrom(value.Migration.Deletes),
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
	Type      string `json:"type"`
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Database  string `json:"database"`
	User      string `json:"user"`
	Schema    string `json:"schema"`
	SSLMode   string `json:"ssl_mode"`
	TLSCAFile string `json:"tls_ca_file"`
}

func resumeEndpointFrom(
	endpoint Endpoint,
	sqliteIdentityMode sqliteHashIdentityMode,
) (resumeEndpoint, error) {
	engine, err := CanonicalEngine(endpoint.Type)
	if err != nil {
		return resumeEndpoint{}, err
	}
	endpoint.Type = engine
	database := endpoint.Database
	if engine == "sqlite" && strings.TrimSpace(database) != "" {
		switch sqliteIdentityMode {
		case sqliteHashIdentityPath:
			database, err = canonicalSQLitePathHashIdentity(database)
		case sqliteHashIdentityFile:
			database, err = canonicalSQLiteFileHashIdentity(database)
		default:
			database, err = canonicalSQLiteHashIdentity(database)
		}
		if err != nil {
			return resumeEndpoint{}, err
		}
		return resumeEndpoint{
			Type:     engine,
			Database: database,
		}, nil
	}
	return resumeEndpoint{
		Type:      engine,
		Host:      canonicalNetworkHost(endpoint.Host),
		Port:      effectivePort(endpoint),
		Database:  database,
		User:      endpoint.User,
		Schema:    endpoint.Schema,
		SSLMode:   strings.ToLower(strings.TrimSpace(endpoint.SSLMode)),
		TLSCAFile: endpoint.TLSCAFile,
	}, nil
}

type resumeMigration struct {
	TargetMode             string                 `json:"target_mode"`
	IncludeTables          []string               `json:"include_tables"`
	ExcludeTables          []string               `json:"exclude_tables"`
	DateUpdatedColumns     []string               `json:"date_updated_columns"`
	LargeTableThreshold    int64                  `json:"large_table_threshold"`
	StrictConsistency      bool                   `json:"strict_consistency"`
	StrictConsistencyScope StrictConsistencyScope `json:"strict_consistency_scope"`
	SchemaContract         *SchemaContract        `json:"schema_contract"`
	Deletes                resumeDeletePolicy     `json:"deletes"`
}

type resumeDeletePolicy struct {
	Mode               DeleteMode           `json:"mode"`
	TargetBehavior     DeleteTargetBehavior `json:"target_behavior"`
	Schedule           DeleteSchedule       `json:"schedule"`
	IntervalNanosecond int64                `json:"interval_nanoseconds"`
	RequirePrimaryKey  bool                 `json:"require_primary_key"`
}

func resumeDeletePolicyFrom(policy DeletePolicy) resumeDeletePolicy {
	return resumeDeletePolicy{
		Mode:               policy.Mode,
		TargetBehavior:     policy.TargetBehavior,
		Schedule:           policy.Reconcile.Schedule,
		IntervalNanosecond: int64(policy.Reconcile.Interval),
		RequirePrimaryKey:  policy.Reconcile.RequirePrimaryKey,
	}
}

func cloneSchemaContract(contract *SchemaContract) *SchemaContract {
	if contract == nil {
		return nil
	}
	cloned := *contract
	return &cloned
}
