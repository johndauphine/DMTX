package app

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/state"
)

const redactionSecretToken = "lease-owner-secret-token"

func seedRedactionRun(t *testing.T, statePath string) state.Backend {
	t.Helper()
	store, err := state.NewBackend(statePath)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	if err := store.InitializeRun(state.Run{
		ID:              "redaction-run",
		Source:          "source.db",
		Target:          "target.db",
		SourceEngine:    "postgres",
		SourceIdentity:  "postgres:source.example:5432/app",
		TargetIdentity:  "postgres:target.example:5432/app",
		LeaseTarget:     "sqlite:/tmp/target.db",
		LeaseOwnerToken: redactionSecretToken,
		LeaseGeneration: 7,
		Outcome:         state.Running,
		Resumable:       true,
		Reason:          "migration in progress",
		StartedAt:       started,
	}, "config-hash"); err != nil {
		t.Fatal(err)
	}
	return store
}

// TestStage4RunRecordRoundTripAndRedaction proves two things that must not
// drift apart: the durable run record keeps every identity and lease field
// across a backend reopen, and the operator-facing surfaces never print the
// lease owner token.
//
// The token is the secret half of target ownership. A run record that
// round-trips it correctly is exactly the record that would leak it if status
// or history forgot to redact, so both halves belong in one fixture.
func TestStage4RunRecordRoundTripAndRedaction(t *testing.T) {
	for _, stateName := range []string{"state.db", "state.yaml"} {
		t.Run(stateName, func(t *testing.T) {
			directory := t.TempDir()
			statePath := filepath.Join(directory, stateName)
			seedRedactionRun(t, statePath)

			// Round-trip: reopen the backend so the assertion reads decoded
			// durable bytes rather than an in-memory value.
			reopened, err := state.NewBackend(statePath)
			if err != nil {
				t.Fatal(err)
			}
			stored, found, err := reopened.Latest()
			if err != nil || !found {
				t.Fatalf("latest found=%v err=%v", found, err)
			}
			if stored.ID != "redaction-run" ||
				stored.SourceEngine != "postgres" ||
				stored.SourceIdentity != "postgres:source.example:5432/app" ||
				stored.TargetIdentity != "postgres:target.example:5432/app" ||
				stored.LeaseTarget != "sqlite:/tmp/target.db" ||
				stored.LeaseOwnerToken != redactionSecretToken ||
				stored.LeaseGeneration != 7 ||
				stored.Outcome != state.Running ||
				!stored.Resumable ||
				stored.Reason != "migration in progress" {
				t.Fatalf("durable run record = %#v", stored)
			}

			// Redaction: the token must be absent from both operator surfaces,
			// while the non-secret identity fields remain present so the
			// redaction cannot be satisfied by emitting nothing.
			for _, command := range []string{"status", "history"} {
				var stdout, stderr bytes.Buffer
				if code := Run(
					[]string{command, "--state", statePath},
					&stdout,
					&stderr,
				); code != Success {
					t.Fatalf(
						"%s exit=%d stderr=%s",
						command,
						code,
						stderr.String(),
					)
				}
				output := stdout.String()
				if strings.Contains(output, redactionSecretToken) {
					t.Fatalf("%s leaked the lease owner token: %s", command, output)
				}
				if !strings.Contains(output, "redaction-run") ||
					!strings.Contains(output, "postgres:target.example:5432/app") {
					t.Fatalf("%s omitted non-secret identity: %s", command, output)
				}
			}
		})
	}
}

// TestStage4PublicRunKeepsEveryNonSecretField pins the redaction to exactly one
// field. A future field added to Run must be considered deliberately: dropping
// more than the token would silently degrade status output, and dropping less
// would leak.
func TestStage4PublicRunKeepsEveryNonSecretField(t *testing.T) {
	started := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	full := state.Run{
		ID: "run", Source: "s", Target: "t",
		SourceEngine: "postgres", SourceIdentity: "si", TargetIdentity: "ti",
		LeaseTarget: "lt", LeaseOwnerToken: redactionSecretToken,
		LeaseGeneration: 3,
		Outcome:         state.Running, Resumable: true, Reason: "why",
		StartedAt: started,
	}
	public := publicRun(full)
	if public.LeaseOwnerToken != "" {
		t.Fatalf("public run retained the token: %#v", public)
	}
	expected := full
	expected.LeaseOwnerToken = ""
	if public != expected {
		t.Fatalf("public run dropped more than the token: %#v", public)
	}
	// The encoded form must not carry an empty key that hints at the field's
	// existence beyond what the struct tag already implies.
	encoded, err := json.Marshal(public)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte(redactionSecretToken)) {
		t.Fatalf("encoded public run leaked the token: %s", encoded)
	}
}
