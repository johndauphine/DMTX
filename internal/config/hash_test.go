package config

import "testing"

func TestHashExcludesPasswordButIncludesDataPlaneSettings(t *testing.T) {
	base := Config{Source: Endpoint{Type: "sqlite", Database: "source.db", Password: "first"}, Target: Endpoint{Type: "sqlite", Database: "target.db"}}
	changedSecret := base
	changedSecret.Source.Password = "second"
	first, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Hash(changedSecret)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("secret changed configuration hash: %s != %s", first, second)
	}
	changedTarget := base
	changedTarget.Target.Database = "other.db"
	third, err := Hash(changedTarget)
	if err != nil {
		t.Fatal(err)
	}
	if first == third {
		t.Fatal("target change did not affect configuration hash")
	}
}
