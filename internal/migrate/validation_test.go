package migrate

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
	_ "modernc.org/sqlite"
)

func TestValidateSQLiteReturnsStableMismatchFinding(t *testing.T) {
	directory := t.TempDir()
	sourcePath, targetPath := filepath.Join(directory, "source.db"), filepath.Join(directory, "target.db")
	for _, fixture := range []struct{ path, statement string }{{sourcePath, `CREATE TABLE users (id INTEGER); INSERT INTO users VALUES (1), (2)`}, {targetPath, `CREATE TABLE users (id INTEGER); INSERT INTO users VALUES (1)`}} {
		database, err := sql.Open("sqlite", fixture.path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := database.Exec(fixture.statement); err != nil {
			t.Fatal(err)
		}
		database.Close()
	}
	result, err := ValidateSQLite(context.Background(), config.Config{Source: config.Endpoint{Type: "sqlite", Database: sourcePath}, Target: config.Endpoint{Type: "sqlite", Database: targetPath}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Passed || len(result.Tables) != 1 || result.Tables[0].Match || result.Tables[0].SourceRows != 2 || result.Tables[0].TargetRows != 1 {
		t.Fatalf("result = %#v", result)
	}
}
