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
