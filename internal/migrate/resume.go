package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/dmtx/internal/config"
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
