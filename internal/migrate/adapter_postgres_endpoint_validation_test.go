package migrate

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestPostgresEndpointTLSValidationIsRegisteredForBothRoles(
	t *testing.T,
) {
	t.Parallel()

	postgres := config.Endpoint{
		Type:     "postgres",
		Host:     "database.example.test",
		Database: "dmtx",
		User:     "migrator",
		Password: "${file:/secret/that/must/not/be/resolved}",
	}
	sqlite := config.Endpoint{
		Type:     "sqlite",
		Database: filepath.Join(t.TempDir(), "target.db"),
	}

	sourceConfig := config.Config{
		Source: postgres,
		Target: sqlite,
	}
	if err := ValidateMigration(sourceConfig); err != nil {
		t.Fatalf("secure-default PostgreSQL source rejected: %v", err)
	}
	sourceConfig.Source.SSLMode = "disable"
	if err := ValidateMigration(sourceConfig); err == nil ||
		!strings.Contains(err.Error(), "ssl_mode is unsupported") {
		t.Fatalf("unsafe PostgreSQL source TLS error = %v", err)
	}

	targetConfig := config.Config{
		Source: config.Endpoint{
			Type:     "sqlite",
			Database: filepath.Join(t.TempDir(), "source.db"),
		},
		Target: postgres,
	}
	if err := ValidateMigration(targetConfig); err != nil {
		t.Fatalf("secure-default PostgreSQL target rejected: %v", err)
	}
	targetConfig.Target.SSLMode = "verify-full"
	if err := ValidateMigration(targetConfig); err == nil ||
		!strings.Contains(err.Error(), "requires tls_ca_file") {
		t.Fatalf("unverifiable PostgreSQL target TLS error = %v", err)
	}
}

func TestPostgresSourceTLSValidationPrecedesAdapterConstruction(
	t *testing.T,
) {
	t.Parallel()

	sourceOpened := false
	targetOpened := false
	registry, err := newAdapterRegistry(
		[]sourceRole{{
			engine:   "postgres",
			validate: validatePostgresSourceEndpoint,
			open: func(
				context.Context,
				config.Endpoint,
			) (sourceAdapter, error) {
				sourceOpened = true
				return nil, errors.New("source must not open")
			},
		}},
		[]targetRole{{
			engine:     "sqlite",
			capability: testTargetCapability,
			open: func(
				context.Context,
				config.Endpoint,
			) (targetAdapter, error) {
				targetOpened = true
				return nil, errors.New("target must not open")
			},
		}},
		[]adapterPair{{source: "postgres", target: "sqlite"}},
		nil,
	)
	if err != nil {
		t.Fatalf("build endpoint-validation registry: %v", err)
	}
	_, err = executeWithRegistry(
		context.Background(),
		config.Config{
			Source: config.Endpoint{
				Type:     "postgres",
				Host:     "database.example.test",
				Database: "source",
				User:     "reader",
				SSLMode:  "disable",
			},
			Target: config.Endpoint{
				Type:     "sqlite",
				Database: filepath.Join(t.TempDir(), "target.db"),
			},
		},
		nil,
		registry,
	)
	if err == nil || !strings.Contains(err.Error(), "ssl_mode is unsupported") {
		t.Fatalf("unsafe PostgreSQL source TLS error = %v", err)
	}
	if sourceOpened || targetOpened {
		t.Fatalf(
			"adapter opened before source TLS rejection: source=%t target=%t",
			sourceOpened,
			targetOpened,
		)
	}
}

func TestPostgresEndpointValidationRejectsFalseCAVerification(
	t *testing.T,
) {
	t.Parallel()

	endpoint := config.Endpoint{
		Host:      "database.example.test",
		Database:  "dmtx",
		User:      "migrator",
		SSLMode:   "require",
		TLSCAFile: filepath.Join(t.TempDir(), "unused-ca.pem"),
	}
	for _, validate := range []struct {
		name string
		run  endpointValidator
	}{
		{name: "source", run: validatePostgresSourceEndpoint},
		{name: "target", run: validatePostgresTargetEndpoint},
	} {
		t.Run(validate.name, func(t *testing.T) {
			t.Parallel()
			err := validate.run(endpoint)
			if err == nil ||
				!strings.Contains(err.Error(), "requires ssl_mode") {
				t.Fatalf("false CA verification error = %v", err)
			}
		})
	}
}
