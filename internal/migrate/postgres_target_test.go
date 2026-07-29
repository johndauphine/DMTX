package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLiteToPostgresWithObserverRejectsOtherPairs(t *testing.T) {
	_, err := SQLiteToPostgresWithObserver(
		context.Background(),
		config.Config{
			Source: config.Endpoint{Type: "sqlite"},
			Target: config.Endpoint{Type: "sqlite"},
		},
		nil,
	)
	if err == nil ||
		!strings.Contains(
			err.Error(),
			"requires source.type sqlite and target.type postgres",
		) {
		t.Fatalf("wrong-pair error = %v", err)
	}
}
