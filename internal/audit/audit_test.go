package audit

import (
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
}
