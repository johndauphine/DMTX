package state

import (
	"path/filepath"
	"testing"
	"time"
)

func TestResumeCompatibilityEvidenceConformance(t *testing.T) {
	t.Parallel()

	for _, extension := range []string{".db", ".yaml"} {
		extension := extension
		t.Run(extension, func(t *testing.T) {
			t.Parallel()
			backend, err := NewBackend(filepath.Join(t.TempDir(), "state"+extension))
			if err != nil {
				t.Fatal(err)
			}
			if err := backend.InitializeRun(Run{
				ID:        "run",
				Source:    "source",
				Target:    "target",
				Outcome:   Running,
				Resumable: true,
				Reason:    "running",
				StartedAt: time.Now().UTC(),
			}, "config-a"); err != nil {
				t.Fatalf("initialize run: %v", err)
			}
			if err := backend.SaveResumeCompatibilityHash("run", "compatible"); err != nil {
				t.Fatalf("save compatibility hash: %v", err)
			}
			if err := backend.SaveResumeCompatibilityHash("run", "duplicate"); err == nil {
				t.Fatal("duplicate compatibility hash succeeded")
			}
			hash, found, err := backend.ResumeCompatibilityHash("run")
			if err != nil || !found || hash != "compatible" {
				t.Fatalf("compatibility hash = %q found=%t err=%v", hash, found, err)
			}
			if err := backend.AcknowledgeConfigOverride(
				"run", "config-b", "compatible",
			); err != nil {
				t.Fatalf("acknowledge override: %v", err)
			}
			configHash, found, err := backend.ConfigHash("run")
			if err != nil || !found || configHash != "config-b" {
				t.Fatalf("config hash = %q found=%t err=%v", configHash, found, err)
			}
		})
	}
}
