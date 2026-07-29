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
