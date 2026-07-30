package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

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

func TestHashOmitsNetworkPasswordPresence(t *testing.T) {
	base := Config{
		Source: Endpoint{
			Type: "postgres", Host: "source.example", Database: "warehouse",
			User: "reader",
		},
		Target: Endpoint{
			Type: "mssql", Host: "target.example", Database: "mirror",
			User: "writer",
		},
	}
	withPasswords := base
	withPasswords.Source.Password = "source-secret"
	withPasswords.Target.Password = "target-secret"
	first, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Hash(withPasswords)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf(
			"password presence changed configuration hash: %s != %s",
			first,
			second,
		)
	}
}

func TestHashesDoNotMutateProgrammaticSchemaContract(t *testing.T) {
	contract := &SchemaContract{Tables: SchemaContractFreeze}
	value := Config{Migration: Migration{SchemaContract: contract}}

	if _, err := Hash(value); err != nil {
		t.Fatal(err)
	}
	if _, err := ResumeCompatibilityHash(value); err != nil {
		t.Fatal(err)
	}
	if contract.Columns != "" || contract.DataType != "" {
		t.Fatalf("hashing mutated caller schema contract: %#v", contract)
	}
}

func TestHashDistinguishesPinnedFromDerivedEquivalentValue(t *testing.T) {
	derived, err := Parse([]byte("migration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := Parse([]byte("migration:\n  workers: 4\n"))
	if err != nil {
		t.Fatal(err)
	}
	if derived.Migration.Workers != pinned.Migration.Workers {
		t.Fatalf(
			"fixture values differ: derived=%d pinned=%d",
			derived.Migration.Workers,
			pinned.Migration.Workers,
		)
	}
	derivedHash, err := Hash(derived)
	if err != nil {
		t.Fatal(err)
	}
	pinnedHash, err := Hash(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if derivedHash == pinnedHash {
		t.Fatal("full configuration hash discarded pinned-vs-derived intent")
	}
	derivedResume, err := ResumeCompatibilityHash(derived)
	if err != nil {
		t.Fatal(err)
	}
	pinnedResume, err := ResumeCompatibilityHash(pinned)
	if err != nil {
		t.Fatal(err)
	}
	if derivedResume != pinnedResume {
		t.Fatal("runtime-only pinning altered structural resume compatibility")
	}
}

func TestHashPreservesProgrammaticChangesToParsedDerivedValues(t *testing.T) {
	value, err := Parse([]byte("migration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := Hash(value)
	if err != nil {
		t.Fatal(err)
	}

	value.Migration.ChunkSize = 321
	value.Migration.Validation.FailOnMismatch = false
	if provenance, found := value.Migration.SettingProvenance(
		"chunk_size",
	); !found || provenance != ProvenanceRequested {
		t.Fatalf(
			"mutated chunk size provenance = %q found=%t",
			provenance,
			found,
		)
	}
	changed, err := Hash(value)
	if err != nil {
		t.Fatal(err)
	}
	if changed == baseline {
		t.Fatal("programmatic changes to parsed defaults did not alter hash")
	}

	encoded, err := yaml.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, token := range []string{
		"chunk_size: 321",
		"fail_on_mismatch: false",
	} {
		if !strings.Contains(text, token) {
			t.Fatalf("canonical YAML omitted %q:\n%s", token, text)
		}
	}
	roundTrip, err := Parse(encoded)
	if err != nil {
		t.Fatal(err)
	}
	roundTripHash, err := Hash(roundTrip)
	if err != nil {
		t.Fatal(err)
	}
	if roundTripHash != changed {
		t.Fatalf(
			"programmatic intent changed across YAML: %s != %s",
			roundTripHash,
			changed,
		)
	}
}

func TestHashCanonicalizesSQLitePathAndHardlinkAliases(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "target.db")
	if err := os.WriteFile(database, []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "target-hardlink.db")
	if err := os.Link(database, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	first := Config{
		Source: Endpoint{
			Type: "sqlite", Database: filepath.Join(directory, "source.db"),
		},
		Target: Endpoint{Type: "sqlite", Database: database},
	}
	second := first
	second.Source.Database = filepath.Join(
		directory,
		"missing",
		"..",
		"source.db",
	)
	second.Target.Database = hardlink
	second.Target.Host = "irrelevant"
	second.Target.User = "irrelevant"
	second.Target.SSLMode = "irrelevant"

	firstHash, err := Hash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := Hash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf(
			"equivalent SQLite endpoints differ: %s != %s",
			firstHash,
			secondHash,
		)
	}
}
