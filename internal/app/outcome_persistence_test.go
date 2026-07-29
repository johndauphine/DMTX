package app

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

func TestPersistAttemptDispositionConformance(t *testing.T) {
	t.Parallel()

	for _, extension := range []string{".db", ".yaml"} {
		extension := extension
		t.Run(extension, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "state"+extension)
			backend, err := state.NewBackend(path)
			if err != nil {
				t.Fatalf("new backend: %v", err)
			}
			started := time.Now().UTC()
			if err := backend.InitializeRun(state.Run{
				ID:        "attempt",
				Source:    "source.db",
				Target:    "target.db",
				Outcome:   state.Running,
				Resumable: true,
				Reason:    "migration in progress",
				StartedAt: started,
			}, "hash"); err != nil {
				t.Fatalf("initialize run: %v", err)
			}

			disposition := migrationAttemptDisposition(
				migrate.Result{Rows: 7},
				context.DeadlineExceeded,
				config.Migration{AllowPartial: true},
			)
			if err := persistAttemptDisposition(
				backend,
				"attempt",
				disposition,
				context.DeadlineExceeded.Error(),
				started.Add(time.Second),
			); err != nil {
				t.Fatalf("persist cancelled outcome: %v", err)
			}
			latest, found, err := backend.Latest()
			if err != nil || !found {
				t.Fatalf("latest cancelled run: found=%t err=%v", found, err)
			}
			if latest.Outcome != state.Cancelled || !latest.Resumable {
				t.Fatalf("cancelled run = %#v, want resumable cancelled", latest)
			}

			if err := backend.UpdateRecoverableOutcome(
				"attempt",
				state.Partial,
				"writer failed",
				started.Add(2*time.Second),
			); err != nil {
				t.Fatalf("prepare partial outcome: %v", err)
			}
			accepted := migrationAttemptDisposition(
				migrate.Result{Tables: 1},
				assertionError("writer failed"),
				config.Migration{AllowPartial: true},
			)
			if err := persistAttemptDisposition(
				backend,
				"attempt",
				accepted,
				"writer failed",
				started.Add(3*time.Second),
			); err != nil {
				t.Fatalf("persist accepted partial: %v", err)
			}
			latest, found, err = backend.Latest()
			if err != nil || !found {
				t.Fatalf("latest accepted partial: found=%t err=%v", found, err)
			}
			if latest.Outcome != state.Partial || latest.Resumable ||
				!strings.Contains(latest.Reason, "accepted partial outcome") {
				t.Fatalf("accepted partial run = %#v", latest)
			}
		})
	}
}

type assertionError string

func (err assertionError) Error() string { return string(err) }
