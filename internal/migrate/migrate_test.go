package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

func TestExecuteRejectsUnsupportedEnginePair(t *testing.T) {
	_, err := Execute(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "clickhouse"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "sqlite-to-clickhouse") {
		t.Fatalf("error = %v", err)
	}
}
