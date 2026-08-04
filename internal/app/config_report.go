package app

import (
	"fmt"
	"os"

	"github.com/johndauphine/dmtx/internal/config"
)

// ConfigReport is what `dmtx config` shows: a configuration as dmtx understood
// it, with nothing in it that must not be shown.
//
// Secrets are absent by construction rather than by redaction. DMT writes
// "Password: [REDACTED]" at each place a password would appear, which works
// until someone adds a second credential field and does not remember the
// second place. Here there is no field to put a secret in, so a new secret in
// config.Endpoint is invisible here until somebody deliberately adds it - and
// TestEveryEndpointFieldIsDecided fails until they say which it is. The
// default is silence, and silence is the safe direction.
type ConfigReport struct {
	Path      string                    `json:"path"`
	Source    EndpointReport            `json:"source"`
	Target    EndpointReport            `json:"target"`
	Migration MigrationReport           `json:"migration"`
	Notes     []config.ConfigDiagnostic `json:"notes,omitempty"`
}

// EndpointReport is the shown subset of an endpoint.
//
// Every field here is one somebody decided may be shown. Password is not
// omitted by an annotation or a filter that could be edited away; it is not
// here at all.
type EndpointReport struct {
	Type      string `json:"type"`
	Host      string `json:"host,omitempty"`
	Port      int    `json:"port,omitempty"`
	Database  string `json:"database"`
	Schema    string `json:"schema,omitempty"`
	User      string `json:"user,omitempty"`
	SSLMode   string `json:"ssl_mode,omitempty"`
	TLSCAFile string `json:"tls_ca_file,omitempty"`
}

// MigrationReport is the shown subset of the migration settings.
type MigrationReport struct {
	TargetMode      string   `json:"target_mode,omitempty"`
	IncludeTables   []string `json:"include_tables,omitempty"`
	ExcludeTables   []string `json:"exclude_tables,omitempty"`
	Workers         int      `json:"workers,omitempty"`
	ConnectionLimit int      `json:"connection_limit,omitempty"`
}

// executeConfig reports a configuration without running anything.
//
// Read-only on purpose: it opens no database and takes no lease, so an
// operator can look at what dmtx thinks their configuration says before
// committing to a command that touches data.
func executeConfig(request Request) Outcome {
	out := newOutcome(request.Command)
	if request.ConfigPath == "" {
		return out.failWith(
			ConfigurationError,
			"usage: dmtx config --config migration.yaml",
		)
	}
	data, err := os.ReadFile(request.ConfigPath)
	if err != nil {
		return out.failWith(FileError, "read configuration: "+err.Error())
	}
	cfg, err := config.Parse(data)
	if err != nil {
		return out.failWith(ConfigurationError, "configuration: "+err.Error())
	}

	report := describeConfig(request.ConfigPath, cfg)
	for _, line := range report.lines() {
		out.out(line)
	}
	if err := out.setPayload(PayloadConfig, report); err != nil {
		return out.failWith(FileError, "write configuration report: "+err.Error())
	}
	return out.done(Success)
}

// describeConfig copies across only the fields a report may carry.
func describeConfig(path string, cfg config.Config) ConfigReport {
	return ConfigReport{
		Path:      path,
		Source:    describeEndpoint(cfg.Source),
		Target:    describeEndpoint(cfg.Target),
		Migration: describeMigration(cfg.Migration),
		Notes:     cfg.Diagnostics(),
	}
}

func describeEndpoint(endpoint config.Endpoint) EndpointReport {
	return EndpointReport{
		Type:      endpoint.Type,
		Host:      endpoint.Host,
		Port:      endpoint.Port,
		Database:  endpoint.Database,
		Schema:    endpoint.Schema,
		User:      endpoint.User,
		SSLMode:   endpoint.SSLMode,
		TLSCAFile: endpoint.TLSCAFile,
	}
}

func describeMigration(migration config.Migration) MigrationReport {
	return MigrationReport{
		TargetMode:      migration.TargetMode,
		IncludeTables:   migration.IncludeTables,
		ExcludeTables:   migration.ExcludeTables,
		Workers:         migration.Workers,
		ConnectionLimit: migration.ConnectionLimit,
	}
}

// lines renders the report for a terminal. The console renders the payload
// instead, so this is the command line's view rather than the only one.
func (report ConfigReport) lines() []string {
	lines := []string{"configuration: " + report.Path}
	lines = append(lines, report.Source.lines("source")...)
	lines = append(lines, report.Target.lines("target")...)

	if report.Migration.TargetMode != "" {
		lines = append(lines, "migration:")
		lines = append(lines, "  target mode: "+report.Migration.TargetMode)
	}
	if report.Migration.Workers > 0 {
		lines = append(lines, fmt.Sprintf("  workers: %d", report.Migration.Workers))
	}
	if report.Migration.ConnectionLimit > 0 {
		lines = append(lines, fmt.Sprintf("  connection limit: %d", report.Migration.ConnectionLimit))
	}
	if count := len(report.Migration.IncludeTables); count > 0 {
		lines = append(lines, fmt.Sprintf("  included tables: %d", count))
	}
	if count := len(report.Migration.ExcludeTables); count > 0 {
		lines = append(lines, fmt.Sprintf("  excluded tables: %d", count))
	}
	for _, note := range report.Notes {
		lines = append(lines, fmt.Sprintf("%s: %s", note.Severity, note.Message))
	}
	return lines
}

func (endpoint EndpointReport) lines(role string) []string {
	lines := []string{role + ":"}
	lines = append(lines, "  type: "+endpoint.Type)
	if endpoint.Host != "" {
		if endpoint.Port > 0 {
			lines = append(lines, fmt.Sprintf("  host: %s:%d", endpoint.Host, endpoint.Port))
		} else {
			lines = append(lines, "  host: "+endpoint.Host)
		}
	}
	lines = append(lines, "  database: "+endpoint.Database)
	if endpoint.Schema != "" {
		lines = append(lines, "  schema: "+endpoint.Schema)
	}
	if endpoint.User != "" {
		lines = append(lines, "  user: "+endpoint.User)
	}
	if endpoint.SSLMode != "" {
		lines = append(lines, "  ssl mode: "+endpoint.SSLMode)
	}
	if endpoint.TLSCAFile != "" {
		lines = append(lines, "  tls ca file: "+endpoint.TLSCAFile)
	}
	return lines
}
