package migrate

import (
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/engine"
)

// validatePostgresSourceEndpoint performs the same structured identity and
// TLS admission as the connection opener without opening state or a database
// connection. Password templates remain opaque because PostgresDSN only
// encodes them.
func validatePostgresSourceEndpoint(endpoint config.Endpoint) error {
	_, err := engine.PostgresDSN(endpoint)
	return err
}
