package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestResumeCompatibilityHashSeparatesSafeRuntimeAndStructuralChanges(t *testing.T) {
	t.Parallel()

	base := Config{
		Source: Endpoint{
			Type: "postgres", Host: "source.example", Database: "source",
			User: "reader", Password: "first",
		},
		Target: Endpoint{Type: "sqlite", Database: "target.db"},
		Migration: Migration{
			TargetMode:             "drop_recreate",
			IncludeTables:          []string{"orders*"},
			LargeTableThreshold:    100,
			ConnectionLimit:        4,
			Workers:                4,
			ChunkSize:              500,
			Partitions:             1,
			ReaderParallelism:      2,
			WriterParallelism:      2,
			ReadAhead:              2,
			UpsertMergeSize:        500,
			MemoryCeilingBytes:     64 << 20,
			CheckpointFrequency:    10,
			MaxRetries:             3,
			StrictConsistencyScope: "table",
		},
	}
	baseline, err := ResumeCompatibilityHash(base)
	if err != nil {
		t.Fatal(err)
	}

	safe := base
	safe.Source.Password = "rotated"
	safe.Migration.ConnectionLimit = 8
	safe.Migration.Workers = 8
	safe.Migration.ChunkSize = 111
	safe.Migration.Partitions = 3
	safe.Migration.ReaderParallelism = 6
	safe.Migration.ReadAhead = 5
	safe.Migration.UpsertMergeSize = 111
	safe.Migration.MemoryCeilingBytes = 128 << 20
	safe.Migration.CheckpointFrequency = 1
	safe.Migration.MaxRetries = 0
	safe.Migration.AllowPartial = true
	compatible, err := ResumeCompatibilityHash(safe)
	if err != nil {
		t.Fatal(err)
	}
	if compatible != baseline {
		t.Fatalf("safe runtime changes altered compatibility hash: %s != %s", compatible, baseline)
	}

	structural := []struct {
		name   string
		change func(*Config)
	}{
		{"source", func(value *Config) { value.Source.Database = "other.db" }},
		{"source user", func(value *Config) { value.Source.User = "other" }},
		{"source TLS mode", func(value *Config) { value.Source.SSLMode = "verify-full" }},
		{"source TLS CA", func(value *Config) { value.Source.TLSCAFile = "/etc/dmtx/source-ca.pem" }},
		{"target mode", func(value *Config) { value.Migration.TargetMode = "upsert" }},
		{"include", func(value *Config) { value.Migration.IncludeTables = []string{"customers"} }},
		{"threshold", func(value *Config) { value.Migration.LargeTableThreshold++ }},
		{"strict", func(value *Config) { value.Migration.StrictConsistency = true }},
	}
	for _, test := range structural {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := base
			test.change(&changed)
			hash, err := ResumeCompatibilityHash(changed)
			if err != nil {
				t.Fatal(err)
			}
			if hash == baseline {
				t.Fatalf("%s change did not alter compatibility hash", test.name)
			}
		})
	}
}

func TestResumeCompatibilityHashCoversProductionDataSemantics(t *testing.T) {
	t.Parallel()

	base, err := Parse([]byte(`
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  target_mode: upsert
  strict_consistency: true
  strict_consistency_scope: table
  date_updated_columns: [updated_at, modified_at]
  schema_contract: evolve
  validation:
    mode: count_only
  deletes:
    mode: reconcile
    reconcile:
      interval: 168h
`))
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := ResumeCompatibilityHash(base)
	if err != nil {
		t.Fatal(err)
	}

	structural := []struct {
		name   string
		change func(*Config)
	}{
		{
			name: "watermark candidates",
			change: func(value *Config) {
				value.Migration.DateUpdatedColumns =
					[]string{"modified_at", "updated_at"}
			},
		},
		{
			name: "schema contract",
			change: func(value *Config) {
				value.Migration.SchemaContract = &SchemaContract{
					Tables:   SchemaContractFreeze,
					Columns:  SchemaContractFreeze,
					DataType: SchemaContractFreeze,
				}
			},
		},
		{
			name: "delete cadence",
			change: func(value *Config) {
				value.Migration.Deletes.Reconcile.Interval = 24 * time.Hour
			},
		},
		{
			name: "strict scope",
			change: func(value *Config) {
				value.Migration.StrictConsistencyScope = "migration"
			},
		},
	}
	for _, test := range structural {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			changed := base
			test.change(&changed)
			hash, err := ResumeCompatibilityHash(changed)
			if err != nil {
				t.Fatal(err)
			}
			if hash == baseline {
				t.Fatalf("%s change did not alter compatibility hash", test.name)
			}
		})
	}

	policyOnly := base
	policyOnly.Migration.Validation.FailOnMismatch = false
	policyOnly.Migration.Validation.FailOnTimeout = false
	policyOnly.Migration.Validation.FailOnEstimateMismatch = false
	policyOnly.Migration.Validation.Mode = ValidationSample
	policyOnly.Migration.FailOnSchemaDrift = true
	policyOnly.Migration.HistoryRetentionDays = 1
	policyOnly.Migration.Tuning = TuningOff
	policyOnly.Migration.RuntimeTuning = false
	policyOnly.Migration.RuntimeTuningInterval = time.Second
	policyOnly.Migration.Deletes.Reconcile.BatchSize = 17
	policyHash, err := ResumeCompatibilityHash(policyOnly)
	if err != nil {
		t.Fatal(err)
	}
	if policyHash != baseline {
		t.Fatalf(
			"policy/tuning-only changes altered resume hash: %s != %s",
			policyHash,
			baseline,
		)
	}
	baseHash, err := Hash(base)
	if err != nil {
		t.Fatal(err)
	}
	changedPolicyHash, err := Hash(policyOnly)
	if err != nil {
		t.Fatal(err)
	}
	if changedPolicyHash == baseHash {
		t.Fatal("full config hash ignored explicit resume policy changes")
	}
}

func TestSchemaEvolutionRenamePreservesHashWireShape(t *testing.T) {
	t.Parallel()

	preferred, err := Parse([]byte("migration:\n  schema_contract: report\n"))
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := Parse([]byte("migration:\n  schema_evolution: report\n"))
	if err != nil {
		t.Fatal(err)
	}

	preferredResume, err := ResumeCompatibilityHash(preferred)
	if err != nil {
		t.Fatal(err)
	}
	legacyResume, err := ResumeCompatibilityHash(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if preferredResume != legacyResume {
		t.Fatalf(
			"schema rename changed resume hash: %s != %s",
			preferredResume,
			legacyResume,
		)
	}

	preferredHash, err := Hash(preferred)
	if err != nil {
		t.Fatal(err)
	}
	legacyHash, err := Hash(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if preferredHash != legacyHash {
		t.Fatalf(
			"schema rename changed config hash: %s != %s",
			preferredHash,
			legacyHash,
		)
	}
}

func TestResumeCompatibilityHashCanonicalizesNetworkEndpointIdentity(
	t *testing.T,
) {
	first := Config{
		Source: Endpoint{
			Type: "PG", Host: "DB.EXAMPLE", Database: "warehouse",
			User: "reader", SSLMode: "REQUIRE",
		},
		Target: Endpoint{
			Type: "sql-server", Host: "TARGET.EXAMPLE",
			Database: "mirror", User: "writer",
		},
	}
	second := first
	second.Source.Type = "postgres"
	second.Source.Host = "db.example"
	second.Source.Port = 5432
	second.Source.SSLMode = "require"
	second.Target.Type = "mssql"
	second.Target.Host = "target.example"
	second.Target.Port = 1433

	firstHash, err := ResumeCompatibilityHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := ResumeCompatibilityHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf(
			"equivalent network identities differ: %s != %s",
			firstHash,
			secondHash,
		)
	}
}

func TestResumeCompatibilityHashNormalizesNetworkHostSpelling(
	t *testing.T,
) {
	first := Config{
		Source: Endpoint{
			Type: "postgres", Host: " DB.EXAMPLE. ", Database: "warehouse",
			User: "reader",
		},
		Target: Endpoint{
			Type: "mssql", Host: " TARGET.EXAMPLE. ", Database: "mirror",
			User: "writer",
		},
	}
	second := first
	second.Source.Host = "db.example"
	second.Target.Host = "target.example"
	firstHash, err := ResumeCompatibilityHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := ResumeCompatibilityHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf(
			"equivalent host spellings differ: %s != %s",
			firstHash,
			secondHash,
		)
	}
}

func TestResumeCompatibilityHashCanonicalizesProgrammaticSQLitePath(
	t *testing.T,
) {
	directory := t.TempDir()
	canonical := filepath.Join(directory, "source.db")
	alias := directory + string(filepath.Separator) + "missing" +
		string(filepath.Separator) + ".." +
		string(filepath.Separator) + "source.db"
	first := Config{
		Source: Endpoint{Type: "sqlite3", Database: alias},
		Target: Endpoint{
			Type: "sqlite", Database: filepath.Join(directory, "target.db"),
		},
	}
	second := first
	second.Source.Type = "sqlite"
	second.Source.Database = canonical

	firstHash, err := ResumeCompatibilityHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := ResumeCompatibilityHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf(
			"equivalent SQLite identities differ: %s != %s",
			firstHash,
			secondHash,
		)
	}
}

func TestResumeCompatibilityHashUsesOnlySQLiteEndpointIdentity(
	t *testing.T,
) {
	directory := t.TempDir()
	database := filepath.Join(directory, "source.db")
	if err := os.WriteFile(database, []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "source-hardlink.db")
	if err := os.Link(database, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	first := Config{
		Source: Endpoint{Type: "sqlite", Database: database},
		Target: Endpoint{
			Type: "sqlite", Database: filepath.Join(directory, "target.db"),
		},
	}
	second := first
	second.Source.Database = hardlink
	second.Source.Host = "irrelevant"
	second.Source.Port = 9999
	second.Source.User = "irrelevant"
	second.Source.Schema = "irrelevant"
	second.Source.SSLMode = "verify-full"
	second.Source.TLSCAFile = "/irrelevant"

	firstHash, err := ResumeCompatibilityHash(first)
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := ResumeCompatibilityHash(second)
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf(
			"equivalent SQLite identities differ: %s != %s",
			firstHash,
			secondHash,
		)
	}
}
