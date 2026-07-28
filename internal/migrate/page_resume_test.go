package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

func TestSQLiteUpsertResumeReusesIntegerKeysetFrontier(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openPageResumeDatabase(t, sourcePath)
	target := openPageResumeDatabase(t, targetPath)
	for _, database := range []*sql.DB{source, target} {
		if _, err := database.Exec(`CREATE TABLE items (id INTEGER PRIMARY KEY, value TEXT)`); err != nil {
			t.Fatal(err)
		}
	}
	for id := 1; id <= sqliteWriteBatchSize+1; id++ {
		if _, err := source.Exec(`INSERT INTO items VALUES (?, ?)`, id, "source"); err != nil {
			t.Fatal(err)
		}
		if id <= sqliteWriteBatchSize {
			if _, err := target.Exec(`INSERT INTO items VALUES (?, ?)`, id, "source"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	watermark := int64(sqliteWriteBatchSize)
	result, err := SQLiteToSQLiteResumeWithProgress(context.Background(), config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "upsert"},
	}, nil, map[string]TableProgress{
		"items": {RowsDone: sqliteWriteBatchSize, IntegerWatermark: &watermark},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != sqliteWriteBatchSize+1 {
		t.Fatalf("result = %+v", result)
	}
	target = openPageResumeDatabase(t, targetPath)
	defer target.Close()
	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != sqliteWriteBatchSize+1 {
		t.Fatalf("target row count = %d", count)
	}
}

func TestSQLiteUpsertResumeReusesRowNumberFrontier(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openPageResumeDatabase(t, sourcePath)
	target := openPageResumeDatabase(t, targetPath)
	for _, database := range []*sql.DB{source, target} {
		if _, err := database.Exec(`CREATE TABLE items (id REAL PRIMARY KEY NOT NULL, value TEXT)`); err != nil {
			t.Fatal(err)
		}
	}
	for id := 0; id <= sqliteWriteBatchSize; id++ {
		key := fmt.Sprintf("key-%04d", id)
		if _, err := source.Exec(`INSERT INTO items VALUES (?, ?)`, key, "source"); err != nil {
			t.Fatal(err)
		}
		if id < sqliteWriteBatchSize {
			if _, err := target.Exec(`INSERT INTO items VALUES (?, ?)`, key, "source"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}

	watermark := int64(sqliteWriteBatchSize)
	result, err := SQLiteToSQLiteResumeWithProgress(context.Background(), config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "upsert"},
	}, nil, map[string]TableProgress{
		"items": {RowsDone: sqliteWriteBatchSize, RowNumberWatermark: &watermark},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Tables != 1 || result.Rows != sqliteWriteBatchSize+1 {
		t.Fatalf("result = %+v", result)
	}
	target = openPageResumeDatabase(t, targetPath)
	defer target.Close()
	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != sqliteWriteBatchSize+1 {
		t.Fatalf("target row count = %d", count)
	}
}

func TestSQLiteUpsertReplaysAfterRowNumberCheckpointFailure(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source := openPageResumeDatabase(t, sourcePath)
	if _, err := source.Exec(`CREATE TABLE items (id REAL PRIMARY KEY NOT NULL, value TEXT)`); err != nil {
		t.Fatal(err)
	}
	for id := 0; id <= sqliteWriteBatchSize; id++ {
		if _, err := source.Exec(`INSERT INTO items VALUES (?, ?)`, fmt.Sprintf("key-%04d", id), "source"); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := config.Config{
		Source:    config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target:    config.Endpoint{Type: "sqlite", Database: targetPath},
		Migration: config.Migration{TargetMode: "upsert"},
	}
	if _, err := SQLiteToSQLiteWithObserver(context.Background(), cfg, failRowNumberCheckpoint{}); err == nil {
		t.Fatal("expected a checkpoint failure after the first target write")
	}
	result, err := SQLiteToSQLiteResumeWithProgress(context.Background(), cfg, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != sqliteWriteBatchSize+1 {
		t.Fatalf("resumed rows = %d", result.Rows)
	}
	target := openPageResumeDatabase(t, targetPath)
	defer target.Close()
	var count int
	if err := target.QueryRow(`SELECT COUNT(*) FROM items`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != sqliteWriteBatchSize+1 {
		t.Fatalf("target row count = %d", count)
	}
}

type failRowNumberCheckpoint struct{}

func (failRowNumberCheckpoint) BeforeTable(context.Context, string) error { return nil }

func (failRowNumberCheckpoint) AfterTable(context.Context, string, int) error { return nil }

func (failRowNumberCheckpoint) AfterIntegerKeysetPage(context.Context, string, int, int64) error {
	return nil
}

func (failRowNumberCheckpoint) AfterRowNumberPage(context.Context, string, int, int64) error {
	return errors.New("injected checkpoint failure")
}

func openPageResumeDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	return database
}
