// Package config loads DMTX configuration without exposing resolved secrets.
package config

import (
	"fmt"
	"os"
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
	TargetMode string `yaml:"target_mode"`
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
	return value, nil
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
