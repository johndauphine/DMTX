package config

import "testing"

func TestResumeCompatibilityHashSeparatesSafeRuntimeAndStructuralChanges(t *testing.T) {
	t.Parallel()

	base := Config{
		Source: Endpoint{
			Type: "sqlite", Database: "source.db", User: "reader",
			Password: "first",
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
