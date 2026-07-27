package config

import "testing"

func TestParsePreservesSecretTemplates(t *testing.T) {
	config, err := Parse([]byte("source:\n  password: ${env:SOURCE_PASSWORD}\ntarget:\n  password: ${file:/run/secrets/target}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if config.Source.Password != "${env:SOURCE_PASSWORD}" {
		t.Fatalf("source template changed: %q", config.Source.Password)
	}
	if config.Target.Password != "${file:/run/secrets/target}" {
		t.Fatalf("target template changed: %q", config.Target.Password)
	}
}
