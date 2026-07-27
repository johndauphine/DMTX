package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/DMTX/internal/config"
)

func TestExecuteRejectsUnsupportedEnginePair(t *testing.T) {
	_, err := Execute(context.Background(), config.Config{
		Source: config.Endpoint{Type: "sqlite"}, Target: config.Endpoint{Type: "mysql"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "sqlite-to-mysql") {
		t.Fatalf("error = %v", err)
	}
}
