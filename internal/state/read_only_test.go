package state

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/johndauphine/dmtx/internal/schema"
	"gopkg.in/yaml.v3"
	_ "modernc.org/sqlite"
)

func readOnlyScopedRun(id, source, target string, startedAt time.Time) Run {
	return Run{
		ID: id, SourceIdentity: source, TargetIdentity: target,
		Outcome: Success, StartedAt: startedAt,
	}
}

func readOnlyScopedDeleteRecord(
	runID string,
	task TaskKey,
	attempt string,
	completedAt time.Time,
) DeleteReconciliation {
	record := readOnlyTestDeleteRecord(attempt, completedAt)
	record.RunID = runID
	record.Task = task
	return record
}

func readOnlyTestDeleteRecord(attempt string, completedAt time.Time) DeleteReconciliation {
	return DeleteReconciliation{
		RunID:       "run-1",
		Task:        TaskKey{Type: "table-copy", Schema: "public", Table: "items"},
		AttemptID:   attempt,
		Due:         true,
		Status:      DeleteReconciliationCompleted,
		StartedAt:   completedAt.Add(-time.Minute),
		CompletedAt: completedAt,
	}
}

func readOnlyTestSchemaTask() TaskKey {
	return TaskKey{Type: "schema-contract", Table: "aggregate-source-schema"}
}

func readOnlyTestSchemaSnapshot(
	t *testing.T,
	runID string,
	task TaskKey,
	table string,
	capturedAt time.Time,
) SchemaSnapshot {
	t.Helper()
	snapshot, err := schema.NewSchemaSnapshot([]schema.Table{{
		Name: table,
		Columns: []schema.Column{{
			Name: "id", Type: "integer",
			DeclaredType: &schema.DeclaredType{Base: "integer"},
			PrimaryKey:   true, PrimaryKeyPosition: 1,
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := snapshot.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := snapshot.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return SchemaSnapshot{
		RunID: runID, Task: task, CanonicalJSON: string(canonical),
		Digest: digest, CapturedAt: capturedAt,
	}
}

func TestReadOnlyDeleteEvidenceDoesNotCreateMissingStateArtifacts(t *testing.T) {
	directory := t.TempDir()
	for _, name := range []string{"missing.state.yaml", "nested/missing.state.db"} {
		path := filepath.Join(directory, name)
		if _, found, err := ReadOnlyLatestSuccessfulDeleteReconciliation(path); err != nil || found {
			t.Fatalf("read missing state %s = found %t, err %v", path, found, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read-only state created %s: %v", path, err)
		}
		if filepath.Base(filepath.Dir(path)) == "nested" {
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("read-only state created parent %s: %v", filepath.Dir(path), err)
			}
		}
	}
}

func TestReadOnlySchemaSnapshotDoesNotCreateMissingStateArtifacts(t *testing.T) {
	directory := t.TempDir()
	scope := SchemaSnapshotReadScope{
		SourceIdentity: "source-a", TargetIdentity: "target-a",
		Task: readOnlyTestSchemaTask(),
	}
	for _, name := range []string{"missing.state.yaml", "nested/missing.state.db"} {
		path := filepath.Join(directory, name)
		if _, found, err := ReadOnlyLatestSuccessfulSchemaSnapshot(path, scope); err != nil || found {
			t.Fatalf("read missing state %s = found %t, err %v", path, found, err)
		}
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read-only state created %s: %v", path, err)
		}
		if filepath.Base(filepath.Dir(path)) == "nested" {
			if _, err := os.Stat(filepath.Dir(path)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("read-only state created parent %s: %v", filepath.Dir(path), err)
			}
		}
	}
}

func TestReadOnlySchemaSnapshotScopesLatestYAMLAndSQLite(t *testing.T) {
	for name, write := range map[string]func(*testing.T, string, []Run, []SchemaSnapshot){
		"yaml":   writeReadOnlySchemaYAML,
		"sqlite": writeReadOnlySchemaSQLite,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state."+name)
			task := readOnlyTestSchemaTask()
			first := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			runs := []Run{
				readOnlyScopedRun("matching-old", "source-a", "target-a", first),
				readOnlyScopedRun("matching-new", "source-a", "target-a", first.Add(time.Hour)),
				readOnlyScopedRun("unrelated-newer", "source-b", "target-b", first.Add(2*time.Hour)),
			}
			snapshots := []SchemaSnapshot{
				readOnlyTestSchemaSnapshot(t, "matching-old", task, "old_items", first),
				readOnlyTestSchemaSnapshot(t, "matching-new", task, "new_items", first.Add(time.Hour)),
				// This malformed payload is newer but belongs to another workload.
				// The scoped reader must not decode it or let it poison selection.
				{RunID: "unrelated-newer", Task: task, CanonicalJSON: "{", Digest: "bad", CapturedAt: first.Add(2 * time.Hour)},
				// Another task is likewise irrelevant even when malformed.
				{RunID: "matching-new", Task: TaskKey{Type: "table-copy", Table: "other"}, CanonicalJSON: "{", Digest: "bad", CapturedAt: first.Add(3 * time.Hour)},
			}
			write(t, path, runs, snapshots)
			result, found, err := ReadOnlyLatestSuccessfulSchemaSnapshot(path, SchemaSnapshotReadScope{
				SourceIdentity: "source-a", TargetIdentity: "target-a", Task: task,
			})
			if err != nil || !found {
				t.Fatalf("read scoped schema snapshot = %#v, found %t, err %v", result, found, err)
			}
			want := snapshots[1]
			if result.RunID != want.RunID || result.Digest != want.Digest || !result.CapturedAt.Equal(want.CapturedAt) {
				t.Fatalf("selected schema snapshot = %#v, want %#v", result, want)
			}
		})
	}
}

func TestReadOnlySchemaSnapshotFailsClosedForApplicableCorruption(t *testing.T) {
	for name, write := range map[string]func(*testing.T, string, []Run, []SchemaSnapshot){
		"yaml":   writeReadOnlySchemaYAML,
		"sqlite": writeReadOnlySchemaSQLite,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state."+name)
			task := readOnlyTestSchemaTask()
			write(t, path, []Run{readOnlyScopedRun("matching", "source-a", "target-a", time.Now().UTC())}, []SchemaSnapshot{{
				RunID: "matching", Task: task, CanonicalJSON: "{", Digest: "bad", CapturedAt: time.Now().UTC(),
			}})
			if _, _, err := ReadOnlyLatestSuccessfulSchemaSnapshot(path, SchemaSnapshotReadScope{
				SourceIdentity: "source-a", TargetIdentity: "target-a", Task: task,
			}); err == nil {
				t.Fatal("applicable malformed schema evidence was accepted")
			}
		})
	}
}

func TestReadOnlySchemaSnapshotReadsSQLiteWALWithoutArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	task := readOnlyTestSchemaTask()
	run := readOnlyScopedRun("matching", "source-a", "target-a", time.Now().UTC())
	snapshot := readOnlyTestSchemaSnapshot(t, run.ID, task, "items", time.Now().UTC())
	writeReadOnlySchemaSQLite(t, path, []Run{run}, []SchemaSnapshot{snapshot})
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO stage4_records (kind, run_id, task_key, record_id, payload) VALUES ('unrelated', 'other', 'other', 'other', '{}')`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	before := readOnlySQLiteArtifactBytes(t, path)
	if _, found, err := ReadOnlyLatestSuccessfulSchemaSnapshot(path, SchemaSnapshotReadScope{
		SourceIdentity: "source-a", TargetIdentity: "target-a", Task: task,
	}); err != nil || !found {
		database.Close()
		t.Fatalf("read WAL schema snapshot found=%t err=%v", found, err)
	}
	after := readOnlySQLiteArtifactBytes(t, path)
	if !equalReadOnlySQLiteArtifactBytes(before, after) {
		database.Close()
		t.Fatalf("read-only schema snapshot changed SQLite artifacts: before=%#v after=%#v", before, after)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReadOnlyDeleteEvidenceReadsLatestYAMLWithoutLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	first := readOnlyTestDeleteRecord("attempt-1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	second := readOnlyTestDeleteRecord("attempt-2", first.CompletedAt.Add(time.Hour))
	data, err := yaml.Marshal(yamlStateDocument{
		Version:               yamlStateVersion,
		DeleteReconciliations: []DeleteReconciliation{first, second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	record, found, err := ReadOnlyLatestSuccessfulDeleteReconciliation(path)
	if err != nil || !found {
		t.Fatalf("read YAML evidence = %#v, found %t, err %v", record, found, err)
	}
	if record.AttemptID != second.AttemptID {
		t.Fatalf("latest attempt = %q, want %q", record.AttemptID, second.AttemptID)
	}
	if _, err := os.Stat(path + ".lock"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read-only YAML created lock: %v", err)
	}
}

func TestReadOnlyDeleteEvidenceReadsLatestSQLiteWithoutSchemaMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE stage4_records (
			kind TEXT NOT NULL,
			run_id TEXT NOT NULL,
			task_key TEXT NOT NULL,
			record_id TEXT NOT NULL,
			payload TEXT NOT NULL
		)`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	first := readOnlyTestDeleteRecord("attempt-1", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	second := readOnlyTestDeleteRecord("attempt-2", first.CompletedAt.Add(time.Hour))
	for _, record := range []DeleteReconciliation{first, second} {
		payload, marshalErr := json.Marshal(record)
		if marshalErr != nil {
			database.Close()
			t.Fatal(marshalErr)
		}
		if _, err := database.Exec(
			`INSERT INTO stage4_records(kind, run_id, task_key, record_id, payload) VALUES (?, ?, ?, ?, ?)`,
			stage4DeleteRecord,
			record.RunID,
			record.Task.Table,
			record.AttemptID,
			string(payload),
		); err != nil {
			database.Close()
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	record, found, err := ReadOnlyLatestSuccessfulDeleteReconciliation(path)
	if err != nil || !found {
		t.Fatalf("read SQLite evidence = %#v, found %t, err %v", record, found, err)
	}
	if record.AttemptID != second.AttemptID {
		t.Fatalf("latest attempt = %q, want %q", record.AttemptID, second.AttemptID)
	}
}

func TestReadOnlyScopedDeleteEvidenceIgnoresUnrelatedHistory(t *testing.T) {
	started := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	source, target := "source-a", "target-a"
	items := TaskKey{Type: "network-table-copy", Schema: "public", Table: "items"}
	orders := TaskKey{Type: "network-table-copy", Schema: "public", Table: "orders"}
	scope := DeleteReconciliationReadScope{SourceIdentity: source, TargetIdentity: target, Tasks: []TaskKey{items, orders}}
	records := []DeleteReconciliation{
		readOnlyScopedDeleteRecord("matching-old", items, "items-old", started.Add(time.Hour)),
		readOnlyScopedDeleteRecord("matching-new", items, "items-new", started.Add(2*time.Hour)),
		readOnlyScopedDeleteRecord("matching-old", orders, "orders-only", started.Add(90*time.Minute)),
		// This is newer and deliberately malformed, but belongs to another
		// source/target workload. It must be ignored before evidence validation.
		{RunID: "other", Task: items, Status: DeleteReconciliationCompleted, AttemptID: "", CompletedAt: started.Add(3 * time.Hour)},
		// A malformed record for another task is likewise outside the scope.
		{RunID: "other", Task: TaskKey{}, Status: DeleteReconciliationCompleted},
	}
	runs := []Run{
		readOnlyScopedRun("matching-old", source, target, started),
		readOnlyScopedRun("matching-new", source, target, started.Add(time.Hour)),
		func() Run {
			run := readOnlyScopedRun("other", "source-other", "target-other", started.Add(2*time.Hour))
			run.SourceEngine = "not-an-engine"
			return run
		}(),
		{ID: "unrelated-malformed-run", SourceEngine: "not-an-engine"},
	}
	for name, write := range map[string]func(*testing.T, string, []Run, []DeleteReconciliation){
		"yaml":   writeReadOnlyScopedYAML,
		"sqlite": writeReadOnlyScopedSQLite,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state."+name)
			write(t, path, runs, records)
			result, err := ReadOnlyLatestSuccessfulDeleteReconciliations(path, scope)
			if err != nil {
				t.Fatalf("scoped read: %v", err)
			}
			if len(result) != 2 || !result[0].Found || !result[1].Found ||
				result[0].Record.AttemptID != "items-new" ||
				result[1].Record.AttemptID != "orders-only" {
				t.Fatalf("scoped evidence = %#v", result)
			}
		})
	}
}

func TestReadOnlyScopedDeleteEvidenceDoesNotCreateSQLiteWALArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal-state.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var mode string
	if err := database.QueryRow(`PRAGMA journal_mode=WAL`).Scan(&mode); err != nil || mode != "wal" {
		database.Close()
		t.Fatalf("enable WAL mode = %q, %v", mode, err)
	}
	if _, err := database.Exec(`CREATE TABLE harmless (id INTEGER PRIMARY KEY)`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	before := readOnlySQLiteArtifacts(t, path)
	scope := DeleteReconciliationReadScope{SourceIdentity: "source", TargetIdentity: "target", Tasks: []TaskKey{{Type: "network-table-copy", Table: "items"}}}
	result, err := ReadOnlyLatestSuccessfulDeleteReconciliations(path, scope)
	if err != nil || len(result) != 1 || result[0].Found {
		t.Fatalf("read WAL-mode SQLite state = %#v, %v", result, err)
	}
	if after := readOnlySQLiteArtifacts(t, path); !equalReadOnlySQLiteArtifacts(before, after) {
		t.Fatalf("read-only SQLite artifacts changed: before=%#v after=%#v", before, after)
	}
}

func TestReadOnlyScopedDeleteEvidenceReadsCommittedWALWithoutArtifacts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal-state.db")
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE runs (id TEXT, source TEXT, target TEXT, source_engine TEXT, source_identity TEXT, target_identity TEXT, lease_target TEXT, lease_owner_token TEXT, lease_generation INTEGER, outcome TEXT, resumable BOOLEAN, reason TEXT, started_at TIMESTAMP, ended_at TIMESTAMP); CREATE TABLE stage4_records (kind TEXT, run_id TEXT, task_key TEXT, record_id TEXT, payload TEXT)`); err != nil {
		t.Fatal(err)
	}
	task := TaskKey{Type: "network-table-copy", Table: "items"}
	run := readOnlyScopedRun("wal-run", "source", "target", time.Now().UTC())
	if _, err := database.Exec(`INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, run.ID, run.Source, run.Target, run.SourceEngine, run.SourceIdentity, run.TargetIdentity, run.LeaseTarget, run.LeaseOwnerToken, run.LeaseGeneration, run.Outcome, run.Resumable, run.Reason, run.StartedAt, nil); err != nil {
		t.Fatal(err)
	}
	record := readOnlyScopedDeleteRecord(run.ID, task, "wal-attempt", time.Now().UTC())
	payload, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	key, err := task.canonical()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`INSERT INTO stage4_records VALUES (?, ?, ?, ?, ?)`, stage4DeleteRecord, run.ID, key, record.AttemptID, string(payload)); err != nil {
		t.Fatal(err)
	}
	before := readOnlySQLiteArtifacts(t, path)
	if !before["-wal"] || !before["-shm"] {
		t.Fatalf("test setup did not retain WAL artifacts: %#v", before)
	}
	beforeBytes := readOnlySQLiteArtifactBytes(t, path)
	scope := DeleteReconciliationReadScope{SourceIdentity: "source", TargetIdentity: "target", Tasks: []TaskKey{task}}
	result, err := ReadOnlyLatestSuccessfulDeleteReconciliations(path, scope)
	if err != nil || len(result) != 1 || !result[0].Found || result[0].Record.AttemptID != record.AttemptID {
		t.Fatalf("read-only state did not see committed WAL evidence: %#v, %v", result, err)
	}
	if after := readOnlySQLiteArtifacts(t, path); !equalReadOnlySQLiteArtifacts(before, after) {
		t.Fatalf("WAL state read changed SQLite artifacts: before=%#v after=%#v", before, after)
	}
	if afterBytes := readOnlySQLiteArtifactBytes(t, path); !equalReadOnlySQLiteArtifactBytes(beforeBytes, afterBytes) {
		t.Fatalf("WAL state read changed SQLite artifact bytes")
	}
}

func TestSQLiteReadOnlyArtifactSnapshotRejectsEqualMetadataRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	if err := os.WriteFile(path, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := snapshotSQLiteReadOnlyArtifacts(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("changed!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, time.Unix(0, before[""].modified), time.Unix(0, before[""].modified)); err != nil {
		t.Fatal(err)
	}
	after, err := snapshotSQLiteReadOnlyArtifacts(path)
	if err != nil {
		t.Fatal(err)
	}
	if before[""].size != after[""].size || before[""].modified != after[""].modified {
		t.Fatalf("test did not preserve metadata: before=%#v after=%#v", before[""], after[""])
	}
	if err := verifySQLiteReadOnlyArtifacts(before, after); err == nil {
		t.Fatal("artifact snapshot accepted equal-metadata rewrite")
	}
}

func writeReadOnlyScopedYAML(t *testing.T, path string, runs []Run, records []DeleteReconciliation) {
	t.Helper()
	data, err := yaml.Marshal(yamlStateDocument{Version: yamlStateVersion, Runs: runs, DeleteReconciliations: records})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeReadOnlyScopedSQLite(t *testing.T, path string, runs []Run, records []DeleteReconciliation) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE runs (id TEXT, source TEXT, target TEXT, source_engine TEXT, source_identity TEXT, target_identity TEXT, lease_target TEXT, lease_owner_token TEXT, lease_generation INTEGER, outcome TEXT, resumable BOOLEAN, reason TEXT, started_at TIMESTAMP, ended_at TIMESTAMP); CREATE TABLE stage4_records (kind TEXT, run_id TEXT, task_key TEXT, record_id TEXT, payload TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, run := range runs {
		if _, err := database.Exec(`INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, run.ID, run.Source, run.Target, run.SourceEngine, run.SourceIdentity, run.TargetIdentity, run.LeaseTarget, run.LeaseOwnerToken, run.LeaseGeneration, run.Outcome, run.Resumable, run.Reason, run.StartedAt, nil); err != nil {
			t.Fatal(err)
		}
	}
	for index, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		key, err := record.Task.canonical()
		if err != nil {
			// Malformed unrelated records use a key outside the requested scope.
			key = "malformed-" + string(rune('a'+index))
		}
		if _, err := database.Exec(`INSERT INTO stage4_records VALUES (?, ?, ?, ?, ?)`, stage4DeleteRecord, record.RunID, key, record.AttemptID, string(payload)); err != nil {
			t.Fatal(err)
		}
	}
}

func writeReadOnlySchemaYAML(t *testing.T, path string, runs []Run, snapshots []SchemaSnapshot) {
	t.Helper()
	data, err := yaml.Marshal(yamlStateDocument{
		Version: yamlStateVersion, Runs: runs, SchemaSnapshots: snapshots,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeReadOnlySchemaSQLite(t *testing.T, path string, runs []Run, snapshots []SchemaSnapshot) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := database.Exec(`CREATE TABLE runs (id TEXT, source TEXT, target TEXT, source_engine TEXT, source_identity TEXT, target_identity TEXT, lease_target TEXT, lease_owner_token TEXT, lease_generation INTEGER, outcome TEXT, resumable BOOLEAN, reason TEXT, started_at TIMESTAMP, ended_at TIMESTAMP); CREATE TABLE stage4_records (kind TEXT, run_id TEXT, task_key TEXT, record_id TEXT, payload TEXT)`); err != nil {
		t.Fatal(err)
	}
	for _, run := range runs {
		if _, err := database.Exec(`INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, run.ID, run.Source, run.Target, run.SourceEngine, run.SourceIdentity, run.TargetIdentity, run.LeaseTarget, run.LeaseOwnerToken, run.LeaseGeneration, run.Outcome, run.Resumable, run.Reason, run.StartedAt, nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, snapshot := range snapshots {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			t.Fatal(err)
		}
		key, err := snapshot.Task.canonical()
		if err != nil {
			key = "malformed-schema-snapshot"
		}
		if _, err := database.Exec(`INSERT INTO stage4_records VALUES (?, ?, ?, ?, ?)`, stage4SchemaRecord, snapshot.RunID, key, snapshot.Digest, string(payload)); err != nil {
			t.Fatal(err)
		}
	}
}

func readOnlySQLiteArtifacts(t *testing.T, path string) map[string]bool {
	t.Helper()
	result := make(map[string]bool)
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		_, err := os.Stat(path + suffix)
		if err == nil {
			result[suffix] = true
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("inspect SQLite artifact %s: %v", suffix, err)
		}
	}
	return result
}

func equalReadOnlySQLiteArtifacts(left, right map[string]bool) bool {
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if left[suffix] != right[suffix] {
			return false
		}
	}
	return true
}

func readOnlySQLiteArtifactBytes(t *testing.T, path string) map[string][sha256.Size]byte {
	t.Helper()
	result := make(map[string][sha256.Size]byte)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		data, err := os.ReadFile(path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatalf("read SQLite artifact %s: %v", suffix, err)
		}
		result[suffix] = sha256.Sum256(data)
	}
	return result
}

func equalReadOnlySQLiteArtifactBytes(left, right map[string][sha256.Size]byte) bool {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		if left[suffix] != right[suffix] {
			return false
		}
	}
	return true
}
