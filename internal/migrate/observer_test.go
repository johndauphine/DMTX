package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

type recordingObserver struct {
	events []string
}

func (observer *recordingObserver) BeforeTable(_ context.Context, table string) error {
	observer.events = append(observer.events, "before:"+table)
	return nil
}

func (observer *recordingObserver) AfterTable(_ context.Context, table string, _ int) error {
	observer.events = append(observer.events, "after:"+table)
	return nil
}

func TestSQLiteToSQLiteNotifiesTableCheckpointBoundaries(t *testing.T) {
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	targetPath := filepath.Join(directory, "target.db")
	source, err := sql.Open("sqlite", sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT); INSERT INTO users (name) VALUES ('Ada')`); err != nil {
		t.Fatal(err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	observer := &recordingObserver{}
	_, err = SQLiteToSQLiteWithObserver(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite", Database: sourcePath},
		Target: config.Endpoint{Type: "sqlite", Database: targetPath},
	}, observer)
	if err != nil {
		t.Fatal(err)
	}
	if len(observer.events) != 2 || observer.events[0] != "before:users" || observer.events[1] != "after:users" {
		t.Fatalf("events = %#v", observer.events)
	}
}
