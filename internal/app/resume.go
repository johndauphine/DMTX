package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/johndauphine/DMTX/internal/config"
	"github.com/johndauphine/DMTX/internal/migrate"
	"github.com/johndauphine/DMTX/internal/state"
)

func resume(args []string, stdout, stderr io.Writer) int {
	if len(args) != 2 || args[0] != "--config" {
		fmt.Fprintln(stderr, "usage: dmt resume --config migration.yaml")
		return ConfigurationError
	}
	data, err := os.ReadFile(args[1])
	if err != nil {
		fmt.Fprintf(stderr, "read configuration: %v\n", err)
		return FileError
	}
	cfg, err := config.Parse(data)
	if err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	store := state.SQLiteStore{Path: args[1] + ".state.db"}
	run, found, err := store.LatestResumableForTarget(cfg.Target.Database)
	if err != nil {
		fmt.Fprintf(stderr, "read resumable run: %v\n", err)
		return StateError
	}
	if !found {
		fmt.Fprintln(stderr, "no resumable run exists for this target")
		return StateError
	}
	if run.Source != cfg.Source.Database {
		fmt.Fprintln(stderr, "resumable run source does not match the supplied configuration")
		return ConfigurationError
	}
	leaseStore, lease, err := acquireSQLiteTargetLease(cfg.Target.Database, run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease: %v\n", err)
		return StateError
	}
	defer leaseStore.ReleaseLease(lease)

	tasks, err := store.ListTasks(run.ID)
	if err != nil {
		fmt.Fprintf(stderr, "read table checkpoints: %v\n", err)
		return StateError
	}
	completed, existing := make(map[string]bool), make(map[string]bool)
	for _, task := range tasks {
		existing[task.Table] = true
		if task.Status == "completed" {
			completed[task.Table] = true
		}
	}
	observer := resumeCheckpointObserver{tableCheckpointObserver: tableCheckpointObserver{store: store, runID: run.ID}, existing: existing}
	result, err := migrate.SQLiteToSQLiteResume(context.Background(), cfg, completed, observer)
	if err != nil {
		if stateErr := store.UpdateFailure(run.ID, err.Error(), time.Now().UTC()); stateErr != nil {
			fmt.Fprintf(stderr, "record failed resume state: %v\n", stateErr)
			return StateError
		}
		fmt.Fprintf(stderr, "resume: %v\n", err)
		return TransferError
	}
	if err := store.Append(state.Run{ID: run.ID, Source: run.Source, Target: run.Target, Outcome: state.Success, Resumable: false, Reason: "migration resumed and completed", StartedAt: run.StartedAt, EndedAt: time.Now().UTC()}); err != nil {
		fmt.Fprintf(stderr, "record resumed migration state: %v\n", err)
		return StateError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
}

type resumeCheckpointObserver struct {
	tableCheckpointObserver
	existing map[string]bool
}

func (observer resumeCheckpointObserver) BeforeTable(ctx context.Context, table string) error {
	if observer.existing[table] {
		return nil
	}
	return observer.tableCheckpointObserver.BeforeTable(ctx, table)
}
