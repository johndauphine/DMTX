package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

func TestExecuteRejectsUnsupportedEnginePair(t *testing.T) {
	_, err := Execute(context.Background(), config.Config{
		Source: config.Endpoint{Type: "postgres"}, Target: config.Endpoint{Type: "sqlite"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "postgres-to-sqlite") {
		t.Fatalf("error = %v", err)
	}
}
