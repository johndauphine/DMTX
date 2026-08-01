package config

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"strings"
)

// NetworkEndpointWorkloadIdentity returns the credential-free network endpoint
// identity that the application persists on a run. Target-owned durable
// protocols use this value when their state receipt must bind exactly to the
// run, while retaining any native server-incarnation facts separately.
func NetworkEndpointWorkloadIdentity(endpoint Endpoint) (string, error) {
	engine, err := CanonicalEngine(endpoint.Type)
	if err != nil {
		return "", err
	}
	if engine == "sqlite" {
		return "", fmt.Errorf("network endpoint workload identity cannot represent SQLite")
	}
	if strings.TrimSpace(endpoint.Host) == "" || strings.TrimSpace(endpoint.Database) == "" {
		return "", fmt.Errorf("network endpoint host and database are required")
	}
	port := effectivePort(endpoint)
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("network endpoint port %d is outside 1..65535", port)
	}
	identity := struct {
		Version  int    `json:"version"`
		Engine   string `json:"engine"`
		Host     string `json:"host"`
		Port     int    `json:"port"`
		Database string `json:"database"`
		Schema   string `json:"schema"`
	}{
		Version:  1,
		Engine:   engine,
		Host:     canonicalNetworkWorkloadHost(endpoint.Host),
		Port:     port,
		Database: endpoint.Database,
		Schema:   endpoint.Schema,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode network endpoint workload identity: %w", err)
	}
	return string(encoded), nil
}

func canonicalNetworkWorkloadHost(host string) string {
	host = canonicalNetworkHost(host)
	unbracketed := host
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		unbracketed = host[1 : len(host)-1]
	}
	if address, err := netip.ParseAddr(unbracketed); err == nil {
		return address.Unmap().String()
	}
	return host
}
