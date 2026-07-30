package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLiteToMySQLWithObserverRejectsOtherPairs(t *testing.T) {
	_, err := SQLiteToMySQLWithObserver(
		context.Background(),
		config.Config{
			Source: config.Endpoint{Type: "postgres"},
			Target: config.Endpoint{Type: "mysql"},
		},
		nil,
	)
	if err == nil ||
		!strings.Contains(err.Error(), "requires source.type sqlite") {
		t.Fatalf("route error = %v", err)
	}
}
