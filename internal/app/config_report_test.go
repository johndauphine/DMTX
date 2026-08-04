package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

// withheldEndpointFields names every config.Endpoint field a report must not
// carry, and why.
//
// Adding a field to config.Endpoint and not deciding about it fails
// TestEveryEndpointFieldIsDecided. That is the point: a new credential should
// break a test rather than quietly appear in a console, an API response, and
// whatever an operator pastes into a support ticket.
var withheldEndpointFields = map[string]string{
	"Password": "a credential; §21.2 forbids it in any surface output",
}

// TestEveryEndpointFieldIsDecided is the guarantee that redaction cannot be
// forgotten.
//
// DMT redacts by writing "[REDACTED]" at each place a password is printed,
// which holds only as long as everyone remembers every place. Here a field is
// either in the report or in the withheld list, and a field that is in neither
// stops the build until somebody says which.
func TestEveryEndpointFieldIsDecided(t *testing.T) {
	shown := map[string]bool{}
	reportType := reflect.TypeOf(EndpointReport{})
	for index := 0; index < reportType.NumField(); index++ {
		shown[reportType.Field(index).Name] = true
	}

	endpointType := reflect.TypeOf(config.Endpoint{})
	if endpointType.NumField() == 0 {
		t.Fatal("config.Endpoint has no fields; the reflection is broken, not the code")
	}
	for index := 0; index < endpointType.NumField(); index++ {
		name := endpointType.Field(index).Name
		_, withheld := withheldEndpointFields[name]
		switch {
		case shown[name] && withheld:
			t.Errorf(
				"config.Endpoint.%s is both reported and listed as withheld; "+
					"one of the two is wrong",
				name,
			)
		case !shown[name] && !withheld:
			t.Errorf(
				"config.Endpoint.%s is new and nobody has decided about it. "+
					"Add it to EndpointReport if it may be shown, or to "+
					"withheldEndpointFields with the reason if it may not.",
				name,
			)
		}
	}
}

// TestAReportNeverCarriesAPassword is the same guarantee checked the blunt way,
// against a real configuration rather than against the type.
//
// The structural test above would pass if EndpointReport gained a Password
// field and somebody moved it out of the withheld list. This would not.
func TestAReportNeverCarriesAPassword(t *testing.T) {
	const secret = "hunter2-do-not-print-me"
	cfg := config.Config{
		Source: config.Endpoint{
			Type: "postgres", Host: "db", Port: 5432,
			Database: "shop", User: "reader", Password: secret,
		},
		Target: config.Endpoint{
			Type: "sqlite", Database: "out.db", Password: secret,
		},
	}

	report := describeConfig("migration.yaml", cfg)
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), secret) {
		t.Errorf("the report payload carries the password: %s", encoded)
	}
	if joined := strings.Join(report.lines(), "\n"); strings.Contains(joined, secret) {
		t.Errorf("the rendered lines carry the password:\n%s", joined)
	}
}

// TestConfigReportsWhatItWasGiven pins that the command is useful, not merely
// safe: a report that showed nothing would pass the tests above.
func TestConfigReportsWhatItWasGiven(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "migration.yaml")
	if err := os.WriteFile(path, []byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  workers: 4
`), 0o600); err != nil {
		t.Fatal(err)
	}

	outcome := executeConfig(Request{Command: "config", ConfigPath: path})
	if outcome.ExitCode != Success {
		t.Fatalf("config failed: %+v", outcome.Messages)
	}
	said := saidBy(outcome)
	for _, want := range []string{"source.db", "target.db", "sqlite", "workers: 4"} {
		if !strings.Contains(said, want) {
			t.Errorf("the report does not mention %q:\n%s", want, said)
		}
	}
	if outcome.Payload == nil || outcome.Payload.Kind != PayloadConfig {
		t.Fatalf("config produced no config payload: %+v", outcome.Payload)
	}
	var decoded ConfigReport
	if err := json.Unmarshal(outcome.Payload.Data, &decoded); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	// Resolved, not as written: config.Parse turns a relative SQLite path into
	// an absolute one, and showing the resolved form is the useful behaviour -
	// it tells an operator which file will actually be read rather than
	// repeating what they already typed.
	if !strings.HasSuffix(decoded.Source.Database, "source.db") {
		t.Errorf("the payload lost the source database: %q", decoded.Source.Database)
	}
	if !filepath.IsAbs(decoded.Source.Database) {
		t.Errorf("the report shows an unresolved path: %q", decoded.Source.Database)
	}
	if decoded.Migration.Workers != 4 {
		t.Errorf("the payload lost the worker count: %+v", decoded.Migration)
	}
}

// TestConfigPayloadWireShapeExcludesSecrets pins the JSON a console will read,
// and is the shape TestEveryPayloadKindIsPinned points at for PayloadConfig.
//
// The absent paths matter as much as the present ones: a field appearing here
// that is not in this list is a field somebody added without deciding whether
// an operator's screen, a support ticket, or a bug report may carry it.
func TestConfigPayloadWireShapeExcludesSecrets(t *testing.T) {
	report := describeConfig("migration.yaml", config.Config{
		Source: config.Endpoint{
			Type: "postgres", Host: "db", Port: 5432, Database: "shop",
			Schema: "public", User: "reader", Password: "secret",
			SSLMode: "require", TLSCAFile: "/etc/ssl/ca.pem",
		},
		Target: config.Endpoint{Type: "sqlite", Database: "out.db", Password: "secret"},
		Migration: config.Migration{
			TargetMode: "drop_recreate", Workers: 4, ConnectionLimit: 8,
			IncludeTables: []string{"orders"}, ExcludeTables: []string{"audit"},
		},
	})
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	present := map[string]bool{}
	collectPaths("", decoded, present)

	for _, path := range []string{
		"path",
		"source.type", "source.host", "source.port", "source.database",
		"source.schema", "source.user", "source.ssl_mode", "source.tls_ca_file",
		"target.type", "target.database",
		"migration.target_mode", "migration.workers", "migration.connection_limit",
		"migration.include_tables", "migration.exclude_tables",
	} {
		if !present[path] {
			t.Errorf("the wire shape lost %s", path)
		}
	}
	for _, forbidden := range []string{"source.password", "target.password"} {
		if present[forbidden] {
			t.Errorf("the wire shape carries %s", forbidden)
		}
	}
	if strings.Contains(string(encoded), "secret") {
		t.Errorf("the encoded payload contains the password: %s", encoded)
	}
}

// collectPaths flattens a decoded object into dotted field paths.
func collectPaths(prefix string, value map[string]any, into map[string]bool) {
	for key, nested := range value {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		into[path] = true
		if child, ok := nested.(map[string]any); ok {
			collectPaths(path, child, into)
		}
	}
}

// TestConfigRefusesWithoutAPath pins the usage message, and that the command
// does not invent a default file to read.
func TestConfigRefusesWithoutAPath(t *testing.T) {
	outcome := executeConfig(Request{Command: "config"})
	if outcome.ExitCode == Success {
		t.Fatal("config succeeded with no configuration to report")
	}
	if !strings.Contains(saidBy(outcome), "usage: dmtx config --config") {
		t.Errorf("config did not say how to call it: %q", saidBy(outcome))
	}
}

// TestConfigReadsNoDatabase pins that reporting a configuration is offline. An
// operator looks at a config precisely when they are not yet ready to touch
// the databases it names.
func TestConfigReadsNoDatabase(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "migration.yaml")
	// Endpoints that cannot possibly be reached.
	if err := os.WriteFile(path, []byte(`
source:
  type: postgres
  host: 203.0.113.1
  port: 5432
  database: nowhere
  user: nobody
  password: nothing
target:
  type: sqlite
  database: target.db
`), 0o600); err != nil {
		t.Fatal(err)
	}

	outcome := executeConfig(Request{Command: "config", ConfigPath: path})
	if outcome.ExitCode != Success {
		t.Fatalf(
			"config failed on unreachable endpoints, so it is connecting: %+v",
			outcome.Messages,
		)
	}
}
