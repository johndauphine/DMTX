package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

// SQLiteToSQLiteResume reuses only validated, completed table checkpoints.
func SQLiteToSQLiteResume(ctx context.Context, cfg config.Config, completed map[string]bool, observer TableObserver) (Result, error) {
	return SQLiteToSQLiteResumeWithProgress(ctx, cfg, completed, nil, observer)
}

// SQLiteToSQLiteResumeWithProgress resumes target-acknowledged pages in upsert
// mode. Rebuild mode restarts incomplete tables intentionally.
func SQLiteToSQLiteResumeWithProgress(ctx context.Context, cfg config.Config, completed map[string]bool, progress map[string]TableProgress, observer TableObserver) (Result, error) {
	if cfg.Source.Type != "sqlite" || cfg.Target.Type != "sqlite" {
		return Result{}, fmt.Errorf("SQLite first pass requires source.type and target.type to be sqlite")
	}
	if cfg.Source.Database == "" || cfg.Target.Database == "" || cfg.Source.Database == cfg.Target.Database {
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
	result := Result{Validated: true}
	for _, name := range names {
		if completed[name] {
			rows, err := countRows(ctx, source, name)
			if err != nil {
				return Result{}, err
			}
			if err := validateCount(ctx, source, target, name); err != nil {
				return Result{}, fmt.Errorf("completed checkpoint for %s is not reusable: %w", name, err)
			}
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
		copied, err := copyTable(ctx, source, target, name, cfg.Migration.TargetMode, observer, tableProgress)
		if err != nil {
			return Result{}, err
		}
		if err := validateCount(ctx, source, target, name); err != nil {
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
