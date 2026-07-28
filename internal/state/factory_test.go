package state

import "testing"

func TestNewBackendSelectsByStatePath(t *testing.T) {
	tests := []struct {
		path string
		kind string
	}{
		{path: "state.yaml", kind: "yaml"},
		{path: "state.YML", kind: "yaml"},
		{path: "state.db", kind: "sqlite"},
		{path: "state", kind: "sqlite"},
	}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			backend, err := NewBackend(test.path)
			if err != nil {
				t.Fatal(err)
			}
			switch test.kind {
			case "yaml":
				if _, ok := backend.(YAMLStore); !ok {
					t.Fatalf("backend = %T", backend)
				}
			case "sqlite":
				if _, ok := backend.(SQLiteStore); !ok {
					t.Fatalf("backend = %T", backend)
				}
			}
		})
	}
}

func TestNewBackendRejectsEmptyPath(t *testing.T) {
	if _, err := NewBackend(" \t"); err == nil {
		t.Fatal("expected an empty state path to fail")
	}
}
