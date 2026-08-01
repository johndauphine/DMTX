package config

import (
	"encoding/json"
	"testing"
)

func TestNetworkEndpointWorkloadIdentityCanonicalizesAliasDefaultPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		alias  string
		engine string
		port   int
	}{
		{name: "PostgreSQL", alias: "postgresql", engine: "postgres", port: 5432},
		{name: "SQLServer", alias: "sql-server", engine: "mssql", port: 1433},
		{name: "MariaDB", alias: "mariadb", engine: "mysql", port: 3306},
		{name: "ClickHouse", alias: "ch", engine: "clickhouse", port: 9440},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			endpoint := Endpoint{
				Type:     test.alias,
				Host:     "DB.EXAMPLE.",
				Database: "warehouse",
				Schema:   "public",
			}
			got, err := NetworkEndpointWorkloadIdentity(endpoint)
			if err != nil {
				t.Fatal(err)
			}

			canonical := endpoint
			canonical.Type = test.engine
			want, err := NetworkEndpointWorkloadIdentity(canonical)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("alias identity = %s, want canonical identity %s", got, want)
			}

			var identity struct {
				Engine string `json:"engine"`
				Port   int    `json:"port"`
			}
			if err := json.Unmarshal([]byte(got), &identity); err != nil {
				t.Fatal(err)
			}
			if identity.Engine != test.engine || identity.Port != test.port {
				t.Fatalf(
					"identity = %+v, want engine=%q port=%d",
					identity,
					test.engine,
					test.port,
				)
			}
		})
	}
}

func TestNetworkEndpointWorkloadIdentityPreservesExplicitAliasPort(t *testing.T) {
	t.Parallel()

	alias, err := NetworkEndpointWorkloadIdentity(Endpoint{
		Type: "postgresql", Host: "db.example", Port: 5544,
		Database: "warehouse", Schema: "public",
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := NetworkEndpointWorkloadIdentity(Endpoint{
		Type: "postgres", Host: "db.example", Port: 5544,
		Database: "warehouse", Schema: "public",
	})
	if err != nil {
		t.Fatal(err)
	}
	if alias != canonical {
		t.Fatalf("alias identity = %s, want canonical identity %s", alias, canonical)
	}
}
