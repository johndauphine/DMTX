package app

import (
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestNetworkLeaseIdentityNormalizesHostAndDefaultPort(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	identity, path, err := targetLeaseLocation(config.Endpoint{Type: "postgres", Host: "DB.EXAMPLE", Database: "warehouse", Schema: "public"})
	if err != nil {
		t.Fatal(err)
	}
	aliasIdentity, aliasPath, err := targetLeaseLocation(config.Endpoint{
		Type: "pg", Host: "db.example.", Port: 5432,
		Database: "warehouse", Schema: "public",
	})
	if err != nil {
		t.Fatal(err)
	}
	if identity != aliasIdentity || path != aliasPath {
		t.Fatalf(
			"equivalent lease endpoints differ:\n(%s, %s)\n(%s, %s)",
			identity,
			path,
			aliasIdentity,
			aliasPath,
		)
	}
	if !strings.HasSuffix(path, ".db") || !strings.Contains(path, "dmtx") {
		t.Fatalf("unexpected lease path %q", path)
	}
}

func TestNetworkLeaseIdentityCollapsesLoopbackAliases(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	var identity, path string
	for index, host := range []string{
		"localhost",
		"127.0.0.1",
		"127.000.000.001",
		"::1",
		"[::1]",
		"::ffff:127.0.0.1",
	} {
		currentIdentity, currentPath, err := targetLeaseLocation(config.Endpoint{
			Type: "mysql", Host: host, Database: "warehouse",
		})
		if host == "127.000.000.001" {
			// netip intentionally rejects non-canonical dotted-decimal text.
			// It remains a DNS spelling rather than being guessed as an IP.
			if err != nil {
				t.Fatal(err)
			}
			if currentIdentity == identity {
				t.Fatalf("ambiguous dotted decimal %q was guessed as loopback", host)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if index == 0 {
			identity, path = currentIdentity, currentPath
			continue
		}
		if currentIdentity != identity || currentPath != path {
			t.Fatalf(
				"loopback alias %q = (%q, %q), want (%q, %q)",
				host,
				currentIdentity,
				currentPath,
				identity,
				path,
			)
		}
	}
}

func TestNetworkLeaseIdentityIsCollisionSafe(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())

	first, _, err := targetLeaseLocation(config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "a:b", Schema: "c",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := targetLeaseLocation(config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "a", Schema: "b:c",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("delimiter-bearing database/schema names collided")
	}
}

func TestNetworkLeaseRejectsRelativeXDGCacheHome(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", "relative-cache")
	_, _, err := targetLeaseLocation(config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "warehouse",
	})
	if err == nil || !strings.Contains(err.Error(), "must be absolute") {
		t.Fatalf("relative XDG cache error = %v", err)
	}
}

func TestSQLiteLeaseUsesCanonicalCacheLocation(t *testing.T) {
	t.Setenv("XDG_CACHE_HOME", t.TempDir())
	identity, path, err := targetLeaseLocation(config.Endpoint{Type: "sqlite", Database: "target.db"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(identity, "sqlite:") ||
		!strings.HasSuffix(path, ".db") ||
		!strings.Contains(path, "dmtx") {
		t.Fatalf("identity = %q, path = %q", identity, path)
	}
}

func TestNetworkWorkloadIdentityIsStableCollisionSafeAndCredentialFree(
	t *testing.T,
) {
	t.Parallel()

	first, err := endpointWorkloadIdentity(config.Endpoint{
		Type: "postgresql", Host: "DB.EXAMPLE.", Database: "warehouse",
		Schema: "public", User: "reader", Password: "FIRST",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := endpointWorkloadIdentity(config.Endpoint{
		Type: "pg", Host: "db.example", Port: 5432, Database: "warehouse",
		Schema: "public", User: "other", Password: "SECOND",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("equivalent identities differ:\n%s\n%s", first, second)
	}
	for _, forbidden := range []string{"FIRST", "SECOND", "reader", "other"} {
		if strings.Contains(first, forbidden) || strings.Contains(second, forbidden) {
			t.Fatalf("workload identity leaked credential/user token %q", forbidden)
		}
	}

	changed, err := endpointWorkloadIdentity(config.Endpoint{
		Type: "postgres", Host: "db.example", Database: "warehouse",
		Schema: "reporting",
	})
	if err != nil {
		t.Fatal(err)
	}
	if changed == first {
		t.Fatal("different endpoint schema reused a workload identity")
	}
}
