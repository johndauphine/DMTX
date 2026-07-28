package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseAppliesCompatibilityDefaults(t *testing.T) {
	got, err := Parse([]byte("source: {}\ntarget: {}\nmigration: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Source.Type != "mssql" || got.Target.Type != "postgres" || got.Migration.TargetMode != "drop_recreate" {
		t.Fatalf("unexpected defaults: %#v", got)
	}
	if got.Migration.ChunkSize != DefaultChunkSize ||
		got.Migration.Partitions != DefaultPartitions ||
		got.Migration.MaxRetries != DefaultMaxRetries ||
		got.Migration.MemoryCeilingBytes != DefaultMemoryCeilingBytes {
		t.Fatalf("unexpected transfer defaults: %#v", got.Migration)
	}
}

func TestParseAcceptsExplicitZeroRetriesAndCheckpointFrequency(t *testing.T) {
	got, err := Parse([]byte("source: {}\ntarget: {}\nmigration:\n  max_retries: 0\n  checkpoint_frequency: 0\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got.Migration.MaxRetries != 0 || got.Migration.CheckpointFrequency != 0 {
		t.Fatalf("explicit zero settings were defaulted: %#v", got.Migration)
	}
}

func TestParseRejectsUnsafeTransferSettings(t *testing.T) {
	cases := []string{
		"chunk_size: -1",
		"partitions: -1",
		"memory_ceiling_bytes: -1",
		"max_retries: -1",
		"strict_consistency_scope: process",
		"connection_limit: 2\n  reader_parallelism: 2\n  writer_parallelism: 2",
	}
	for _, settings := range cases {
		data := []byte("source: {}\ntarget: {}\nmigration:\n  " + settings + "\n")
		if _, err := Parse(data); err == nil {
			t.Fatalf("expected invalid transfer settings for %q", settings)
		}
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

func TestSameEndpointResolvesSQLitePathAliases(t *testing.T) {
	directory := t.TempDir()
	database := filepath.Join(directory, "source.db")
	if err := os.WriteFile(database, []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	lexicalAlias := directory + string(filepath.Separator) + "." + string(filepath.Separator) + "source.db"
	if !SameEndpoint(Endpoint{Type: "sqlite", Database: database}, Endpoint{Type: "sqlite", Database: lexicalAlias}) {
		t.Fatal("lexical aliases of one SQLite file must compare equal")
	}
	hardlink := filepath.Join(directory, "source-hardlink.db")
	if err := os.Link(database, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	if !SameEndpoint(Endpoint{Type: "sqlite", Database: database}, Endpoint{Type: "sqlite", Database: hardlink}) {
		t.Fatal("hardlinks to one SQLite file must compare equal")
	}
	if SameEndpoint(Endpoint{Type: "sqlite", Database: database}, Endpoint{Type: "sqlite", Database: filepath.Join(directory, "other.db")}) {
		t.Fatal("different SQLite files must not compare equal")
	}
}
