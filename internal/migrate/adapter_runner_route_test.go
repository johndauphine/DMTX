package migrate

import (
	"context"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestExecuteBuiltInComposedRouteRejectsResolvedPairMismatch(
	t *testing.T,
) {
	result, err := executeBuiltInComposedRoute(
		context.Background(),
		config.Config{
			Source: config.Endpoint{
				Type:     "postgres",
				Host:     "source.example.test",
				Database: "source",
				User:     "reader",
			},
			Target: config.Endpoint{
				Type:     "sqlite",
				Database: "target.db",
			},
		},
		nil,
		adapterPair{
			source: "mysql",
			target: "sqlite",
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"resolved migration pair postgres-to-sqlite does not match requested pair mysql-to-sqlite",
	) {
		t.Fatalf("result = %#v, error = %v", result, err)
	}
	if result != (Result{}) {
		t.Fatalf("partial result = %#v", result)
	}
}
