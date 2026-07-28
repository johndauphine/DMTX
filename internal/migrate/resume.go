package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

// CompletedTableCheckpoint is the durable row count recorded when a table was
// marked complete. Resume accepts the checkpoint only when this count still
// agrees exactly with both endpoints.
type CompletedTableCheckpoint struct {
	Rows int
}

// CompletedTableCheckpoints identifies tables that durable state marked
// complete. Presence, rather than a boolean value, carries completion.
type CompletedTableCheckpoints map[string]CompletedTableCheckpoint

// SQLiteToSQLiteResume reuses only validated, completed table checkpoints.
func SQLiteToSQLiteResume(ctx context.Context, cfg config.Config, completed CompletedTableCheckpoints, observer TableObserver) (Result, error) {
	return SQLiteToSQLiteResumeWithProgress(ctx, cfg, completed, nil, observer)
}

// SQLiteToSQLiteResumeWithProgress resumes target-acknowledged pages in upsert
// mode. Rebuild mode restarts incomplete tables intentionally.
func SQLiteToSQLiteResumeWithProgress(ctx context.Context, cfg config.Config, completed CompletedTableCheckpoints, progress map[string]TableProgress, observer TableObserver) (Result, error) {
	return runSQLiteToSQLite(ctx, cfg, completed, progress, observer, true)
}

func sqliteToSQLiteLegacyResumeWithProgress(
	ctx context.Context,
	cfg config.Config,
	completed CompletedTableCheckpoints,
	progress map[string]TableProgress,
	observer TableObserver,
) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("SQLite first pass requires source.type and target.type to be sqlite")
	}
	if cfg.Source.Database == "" || cfg.Target.Database == "" || config.SameEndpoint(cfg.Source, cfg.Target) {
		return Result{}, fmt.Errorf("resume requires distinct source and target SQLite database paths")
	}
	source, err := sql.Open("sqlite", cfg.Source.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open source: %w", err)
	}
	defer source.Close()
	target, err := sql.Open("sqlite", cfg.Target.Database)
	if err != nil {
		return Result{}, fmt.Errorf("open target: %w", err)
	}
	defer target.Close()

	names, err := userTables(ctx, source)
	if err != nil {
		return Result{}, err
	}
	names, err = selectedTables(names, cfg)
	if err != nil {
		return Result{}, err
	}
	if err := requireSQLiteDestructiveAcknowledgement(ctx, target, names, cfg.Migration); err != nil {
		return Result{}, err
	}
	if err := validateSQLiteSchemaBeforeMutation(ctx, source, target, names, cfg.Migration.TargetMode); err != nil {
		return Result{}, err
	}
	validatedCompleted, err := validateCompletedSQLiteTableCheckpoints(
		ctx, source, target, names, completed, true,
	)
	if err != nil {
		return Result{}, err
	}
	if setObserver, ok := observer.(TableSetObserver); ok {
		tables := append([]string(nil), names...)
		if err := setObserver.BeforeTables(ctx, tables); err != nil {
			return Result{}, fmt.Errorf("checkpoint table set: %w", err)
		}
		if err := notifySQLiteWriteBoundary(ctx, observer, SQLiteBoundaryTableSetCheckpoint, ""); err != nil {
			return Result{}, err
		}
	}
	result := Result{Validated: true}
	for _, name := range names {
		if rows, complete := validatedCompleted[name]; complete {
			result.Tables++
			result.Rows += rows
			continue
		}
		if observer != nil {
			if err := observer.BeforeTable(ctx, name); err != nil {
				return Result{}, fmt.Errorf("checkpoint before %s: %w", name, err)
			}
		}
		tableProgress := progress[name]
		if cfg.Migration.TargetMode != "upsert" {
			tableProgress = TableProgress{}
		}
		copied, err := copyTable(ctx, source, target, name, cfg.Migration.TargetMode, observer, tableProgress, true)
		if err != nil {
			return Result{}, err
		}
		if err := validateCount(ctx, source, target, name, cfg.Migration.TargetMode); err != nil {
			return Result{}, err
		}
		if observer != nil {
			if err := observer.AfterTable(ctx, name, tableProgress.RowsDone+copied); err != nil {
				return Result{}, fmt.Errorf("checkpoint after %s: %w", name, err)
			}
		}
		result.Tables++
		result.Rows += tableProgress.RowsDone + copied
	}
	return result, nil
}

func validateCompletedSQLiteTableCheckpoints(
	ctx context.Context,
	source, target *sql.DB,
	names []string,
	completed CompletedTableCheckpoints,
	resume bool,
) (map[string]int, error) {
	validated := make(map[string]int)
	if !resume {
		return validated, nil
	}
	for _, name := range names {
		checkpoint, complete := completed[name]
		if !complete {
			continue
		}
		if checkpoint.Rows < 0 {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"completed checkpoint for %s has invalid row count %d",
				name, checkpoint.Rows,
			))
		}
		sourceRows, err := countRows(ctx, source, name)
		if err != nil {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"validate completed checkpoint for %s against source: %w", name, err,
			))
		}
		targetRows, err := countRows(ctx, target, name)
		if err != nil {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"validate completed checkpoint for %s against target: %w", name, err,
			))
		}
		if checkpoint.Rows != sourceRows || checkpoint.Rows != targetRows {
			return nil, NewTransferError(ErrorClassState, fmt.Errorf(
				"completed checkpoint for %s is not reusable: checkpoint has %d rows, source has %d rows, target has %d rows",
				name, checkpoint.Rows, sourceRows, targetRows,
			))
		}
		validated[name] = checkpoint.Rows
	}
	return validated, nil
}
