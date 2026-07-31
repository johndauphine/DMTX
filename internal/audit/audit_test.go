package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAppendCreatesHashLinkedAuditEvents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "audit.ndjson")
	at := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	if err := Append(path, "run-1", "run_started", at); err != nil {
		t.Fatal(err)
	}
	if err := Append(path, "run-1", "run_succeeded", at.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(contents)), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], `"previous_hash"`) {
		t.Fatalf("audit stream = %q", contents)
	}
	if found, err := HasEvent(path, "run-1", "run_succeeded"); err != nil || !found {
		t.Fatalf("terminal event found = %v, error = %v", found, err)
	}
	if found, err := HasEvent(path, "run-1", "resume_succeeded"); err != nil || found {
		t.Fatalf("absent event found = %v, error = %v", found, err)
	}
}

func TestAppendRejectsTamperedAuditStream(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	if err := Append(path, "run-1", "run_started", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(`{"hash":"tampered"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Append(path, "run-1", "run_succeeded", time.Now()); err == nil {
		t.Fatal("expected tampered audit stream to be rejected")
	}
	if _, err := HasEvent(path, "run-1", "run_started"); err == nil {
		t.Fatal("expected verified event lookup to reject tampered audit stream")
	}
}

func TestAppendPayloadExtendsLegacyChainAndDetectsPayloadTampering(
	t *testing.T,
) {
	path := filepath.Join(t.TempDir(), "audit.ndjson")
	at := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)

	legacy := Event{
		At:    at,
		RunID: "legacy-run",
		Type:  "run_started",
	}
	legacyMaterial := at.Format(time.RFC3339Nano) +
		"\x00legacy-run\x00run_started\x00"
	legacyDigest := sha256.Sum256([]byte(legacyMaterial))
	legacy.Hash = hex.EncodeToString(legacyDigest[:])
	legacyJSON, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		path,
		append(legacyJSON, '\n'),
		0o600,
	); err != nil {
		t.Fatal(err)
	}

	payload := struct {
		Action string `json:"action"`
		Count  int    `json:"count"`
	}{
		Action: "report",
		Count:  2,
	}
	if err := AppendPayload(
		path,
		"legacy-run",
		"stage4_schema_decisions",
		payload,
		at.Add(time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := Append(
		path,
		"legacy-run",
		"run_succeeded",
		at.Add(2*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if err := AppendPayload(
		path,
		"legacy-run",
		"post_legacy_payload",
		payload,
		at.Add(3*time.Second),
	); err != nil {
		t.Fatal(err)
	}
	if found, err := HasEvent(
		path,
		"legacy-run",
		"stage4_schema_decisions",
	); err != nil || !found {
		t.Fatalf("payload event found=%v err=%v", found, err)
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(contents)), "\n")
	if len(lines) != 4 {
		t.Fatalf("audit lines = %d, want 4", len(lines))
	}
	var event Event
	if err := json.Unmarshal([]byte(lines[1]), &event); err != nil {
		t.Fatal(err)
	}
	if string(event.Payload) != `{"action":"report","count":2}` {
		t.Fatalf("payload = %s", event.Payload)
	}
	event.Payload = json.RawMessage(
		`{"action":"discard_value","count":2}`,
	)
	tampered, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	lines[1] = string(tampered)
	if err := os.WriteFile(
		path,
		[]byte(strings.Join(lines, "\n")+"\n"),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := HasEvent(
		path,
		"legacy-run",
		"stage4_schema_decisions",
	); err == nil ||
		!strings.Contains(err.Error(), "integrity") {
		t.Fatalf("payload tamper error = %v", err)
	}
}
