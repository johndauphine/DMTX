package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Hash returns a stable fingerprint of data-plane configuration without secrets.
func Hash(value Config) (string, error) {
	normalizeDefaults(&value)
	sanitized := Sanitize(value)
	encoded, err := json.Marshal(sanitized)
	if err != nil {
		return "", fmt.Errorf("encode configuration hash: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
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
}
