package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

func TestSQLiteCompletedCheckpointRequiresExactEndpointAgreement(t *testing.T) {
	tests := []struct {
		name             string
		addSourceRow     bool
		addTargetRow     bool
		wantCountMessage string
	}{
		{
			name:             "source and target both changed after checkpoint",
			addSourceRow:     true,
			addTargetRow:     true,
			wantCountMessage: "checkpoint has 1 rows, source has 2 rows, target has 2 rows",
		},
		{
			name:             "target differs from checkpoint",
			addTargetRow:     true,
			wantCountMessage: "checkpoint has 1 rows, source has 1 rows, target has 2 rows",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			sourcePath := filepath.Join(directory, "source.db")
			targetPath := filepath.Join(directory, "target.db")
			source := openSQLitePipelineDatabase(t, sourcePath)
			target := openSQLitePipelineDatabase(t, targetPath)
			for _, database := range []*sql.DB{source, target} {
				if _, err := database.Exec(`
					CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
					INSERT INTO items VALUES (1, 'checkpointed');
				`); err != nil {
					t.Fatal(err)
				}
			}
			if test.addSourceRow {
				if _, err := source.Exec(`INSERT INTO items VALUES (2, 'source changed')`); err != nil {
					t.Fatal(err)
				}
			}
			if test.addTargetRow {
				if _, err := target.Exec(`INSERT INTO items VALUES (2, 'target changed')`); err != nil {
					t.Fatal(err)
				}
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
			if err := target.Close(); err != nil {
				t.Fatal(err)
			}

			cfg := sqliteCompletedCheckpointConfig(sourcePath, targetPath)
			result, err := SQLiteToSQLiteResume(
				context.Background(),
				cfg,
				CompletedTableCheckpoints{
					"items": {Rows: 1},
				},
				nil,
			)
			if err == nil {
				t.Fatalf("result = %+v, expected stale checkpoint rejection", result)
			}
			if result != (Result{}) {
				t.Fatalf("result before mutation = %+v", result)
			}
			if got := ClassifyTransferError(err); got != ErrorClassState {
				t.Fatalf("error class = %s, want %s: %v", got, ErrorClassState, err)
			}
			if !strings.Contains(err.Error(), test.wantCountMessage) {
				t.Fatalf("error = %q, want count evidence %q", err, test.wantCountMessage)
			}
			target = openSQLitePipelineDatabase(t, targetPath)
			defer target.Close()
			var targetRows int
			if err := target.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&targetRows); err != nil {
				t.Fatal(err)
			}
			wantTargetRows := 1
			if test.addTargetRow {
				wantTargetRows = 2
			}
			if targetRows != wantTargetRows {
				t.Fatalf("target rows after rejection = %d, want %d", targetRows, wantTargetRows)
			}
		})
	}
}

func TestSQLiteCompletedCheckpointSkipsOnlyAfterExactAgreement(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openSQLitePipelineDatabase(t, sourcePath)
	target := openSQLitePipelineDatabase(t, targetPath)
	for _, database := range []*sql.DB{source, target} {
		if _, err := database.Exec(`
			CREATE TABLE items (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);
			INSERT INTO items VALUES (1, 'checkpointed');
		`); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	observer := &sqlitePipelineTestObserver{}
	result, err := SQLiteToSQLiteResume(
		context.Background(),
		sqliteCompletedCheckpointConfig(sourcePath, targetPath),
		CompletedTableCheckpoints{
			"items": {Rows: 1},
		},
		observer,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != 1 || !result.Validated {
		t.Fatalf("result = %+v", result)
	}
	if plans := observer.snapshotPlans(); len(plans) != 0 {
		t.Fatalf("completed table emitted transfer plans: %+v", plans)
	}
	if len(observer.tableRows) != 0 {
		t.Fatalf("completed table emitted completion callback: %+v", observer.tableRows)
	}
}

func sqliteCompletedCheckpointConfig(sourcePath, targetPath string) config.Config {
	cfg := sqlitePipelineTestConfig(sourcePath, targetPath)
	cfg.Migration.TargetMode = "upsert"
	return cfg
}
