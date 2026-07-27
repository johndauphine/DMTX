package config

import "testing"

func TestParseAppliesCompatibilityDefaults(t *testing.T) {
	got, err := Parse([]byte("source: {}\ntarget: {}\nmigration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Source.Type != "mssql" || got.Target.Type != "postgres" || got.Migration.TargetMode != "drop_recreate" {
		t.Fatalf("unexpected defaults: %#v", got)
	}
}

func TestSanitizeNeverReturnsPassword(t *testing.T) {
	got := Sanitize(Config{Source: Endpoint{Password: "source-secret"}, Target: Endpoint{Password: "target-secret"}})
	if got.Source.Password == "source-secret" || got.Target.Password == "target-secret" {
		t.Fatal("password leaked")
	}
}

func TestParseCanonicalizesEngineAliases(t *testing.T) {
	got, err := Parse([]byte("source:\n  type: pg\ntarget:\n  type: sqlite3\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Source.Type != "postgres" || got.Target.Type != "sqlite" {
		t.Fatalf("canonical engines = %q, %q", got.Source.Type, got.Target.Type)
	}
}

func TestParseRejectsUnknownEngine(t *testing.T) {
	if _, err := Parse([]byte("source:\n  type: oracle\ntarget:\n  type: sqlite\n")); err == nil {
		t.Fatal("expected unsupported engine error")
	}
}

func TestSameEndpointNormalizesDefaultNetworkPorts(t *testing.T) {
	if !SameEndpoint(Endpoint{Type: "postgres", Host: "DB.EXAMPLE.TEST", Database: "dmtx"}, Endpoint{Type: "postgres", Host: "db.example.test", Port: 5432, Database: "dmtx"}) {
		t.Fatal("expected equivalent PostgreSQL endpoints")
	}
	if SameEndpoint(Endpoint{Type: "postgres", Host: "db.example.test", Database: "dmtx"}, Endpoint{Type: "postgres", Host: "db.example.test", Database: "other"}) {
		t.Fatal("different databases must not compare equal")
	}
}
