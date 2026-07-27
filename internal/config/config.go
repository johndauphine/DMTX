// Package config loads DMTX configuration without exposing resolved secrets.
package config

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

type Endpoint struct {
	Type     string `yaml:"type"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Schema   string `yaml:"schema"`
}

type Migration struct {
	TargetMode    string   `yaml:"target_mode"`
	IncludeTables []string `yaml:"include_tables"`
	ExcludeTables []string `yaml:"exclude_tables"`
}
type Config struct {
	Source    Endpoint  `yaml:"source"`
	Target    Endpoint  `yaml:"target"`
	Migration Migration `yaml:"migration"`
}

func Parse(data []byte) (Config, error) {
	var value Config
	if err := yaml.Unmarshal(data, &value); err != nil {
		return Config{}, fmt.Errorf("parse configuration: %w", err)
	}
	if value.Source.Type == "" {
		value.Source.Type = "mssql"
	}
	if value.Target.Type == "" {
		value.Target.Type = "postgres"
	}
	if value.Migration.TargetMode == "" {
		value.Migration.TargetMode = "drop_recreate"
	}
	if value.Migration.TargetMode != "drop_recreate" && value.Migration.TargetMode != "upsert" {
		return Config{}, fmt.Errorf("invalid target_mode %q", value.Migration.TargetMode)
	}
	if err := validatePatterns("include_tables", value.Migration.IncludeTables); err != nil {
		return Config{}, err
	}
	if err := validatePatterns("exclude_tables", value.Migration.ExcludeTables); err != nil {
		return Config{}, err
	}
	return value, nil
}

// SelectTables applies path-style glob patterns in the source's existing,
// deterministic order. An empty include list selects every table; exclusions
// always take precedence over inclusions.
func SelectTables(names, include, exclude []string) ([]string, error) {
	if err := validatePatterns("include_tables", include); err != nil {
		return nil, err
	}
	if err := validatePatterns("exclude_tables", exclude); err != nil {
		return nil, err
	}

	selected := make([]string, 0, len(names))
	for _, name := range names {
		included, err := matchesAny(name, include)
		if err != nil {
			return nil, err
		}
		if len(include) > 0 && !included {
			continue
		}
		excluded, err := matchesAny(name, exclude)
		if err != nil {
			return nil, err
		}
		if !excluded {
			selected = append(selected, name)
		}
	}
	return selected, nil
}

func validatePatterns(field string, patterns []string) error {
	for _, pattern := range patterns {
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid %s glob %q: %w", field, pattern, err)
		}
	}
	return nil
}

func matchesAny(name string, patterns []string) (bool, error) {
	for _, pattern := range patterns {
		matched, err := path.Match(pattern, name)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

var template = regexp.MustCompile(`^\$\{(env:|file:)?([^}]+)\}$`)

func ExpandSecret(value string) (string, error) {
	matches := template.FindStringSubmatch(value)
	if matches == nil {
		return value, nil
	}
	switch matches[1] {
	case "file:":
		content, err := os.ReadFile(matches[2])
		if err != nil {
			return "", fmt.Errorf("read secret file: %w", err)
		}
		return strings.TrimSuffix(string(content), "\n"), nil
	default:
		return os.Getenv(matches[2]), nil
	}
}

func Sanitize(value Config) Config {
	value.Source.Password = redact(value.Source.Password)
	value.Target.Password = redact(value.Target.Password)
	return value
}
func redact(value string) string {
	if value == "" {
		return ""
	}
	return "[REDACTED]"
}
