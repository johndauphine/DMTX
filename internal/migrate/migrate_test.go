package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestExecuteRejectsUnsupportedEnginePair(t *testing.T) {
	_, err := Execute(context.Background(), config.Config{
		Source: config.Endpoint{Type: "clickhouse"}, Target: config.Endpoint{Type: "postgres"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "clickhouse-to-postgres") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateMigrationRejectsUnsupportedTargetModeBeforePairSelection(t *testing.T) {
	err := ValidateMigration(config.Config{
		Source:    config.Endpoint{Type: "postgres"},
		Target:    config.Endpoint{Type: "clickhouse"},
		Migration: config.Migration{TargetMode: "upsert"},
	})
	if err == nil || !strings.Contains(err.Error(), "does not support upsert") {
		t.Fatalf("error = %v", err)
	}
}

func TestValidateMigrationRejectsSameEndpoint(t *testing.T) {
	err := ValidateMigration(config.Config{
		Source: config.Endpoint{
			Type: "postgres", Host: "db.example.test", Database: "dmtx",
		},
		Target: config.Endpoint{
			Type: "postgres", Host: "DB.EXAMPLE.TEST", Port: 5432, Database: "dmtx",
		},
	})
	if err == nil || !strings.Contains(err.Error(), "same endpoint") {
		t.Fatalf("error = %v", err)
	}
}

func TestComposedPairEntrypointsValidateTargetBeforeOpeningSource(t *testing.T) {
	tests := []struct {
		name   string
		source string
		run    migrationRunner
	}{
		{
			name:   "PostgreSQL",
			source: "postgres",
			run:    PostgresToSQLiteWithObserver,
		},
		{
			name:   "MySQL",
			source: "mysql",
			run:    MySQLToSQLiteWithObserver,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.run(context.Background(), config.Config{
				Source: config.Endpoint{Type: test.source},
				Target: config.Endpoint{Type: "sqlite"},
			}, nil)
			if err == nil ||
				!strings.Contains(err.Error(), "SQLite target database path is required") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestSQLServerToSQLiteRemainsDeferred(t *testing.T) {
	_, err := SQLServerToSQLiteWithObserver(
		context.Background(),
		config.Config{
			Source: config.Endpoint{Type: "mssql"},
			Target: config.Endpoint{Type: "sqlite"},
		},
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "mssql-to-sqlite") {
		t.Fatalf("error = %v", err)
	}
}
