package state

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

func TestYAMLStoreWritesPrivateCompleteDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "state.yaml")
	store := YAMLStore{Path: path}
	started := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	if err := store.InitializeRun(Run{
		ID:        "run-1",
		Source:    "source.db",
		Target:    "target.db",
		Outcome:   Running,
		Resumable: true,
		StartedAt: started,
	}, "hash-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateTasks([]Task{{RunID: "run-1", Table: "users", StartedAt: started}}); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceIntegerKeysetTask("run-1", "users", 25, 101); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	versionPrefix := []byte(fmt.Sprintf("version: %d\n", yamlStateVersion))
	if !bytes.HasPrefix(data, versionPrefix) || bytes.HasPrefix(bytes.TrimSpace(data), []byte("{")) {
		t.Fatalf("state is not a YAML document:\n%s", data)
	}
	var document yamlStateDocument
	if err := yaml.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	if document.Version != yamlStateVersion || len(document.Runs) != 1 || len(document.Tasks) != 1 ||
		document.ConfigHashes["run-1"] != "hash-1" {
		t.Fatalf("document = %#v", document)
	}
	if document.Tasks[0].IntegerWatermark == nil || *document.Tasks[0].IntegerWatermark != 101 {
		t.Fatalf("task = %#v", document.Tasks[0])
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if permission := info.Mode().Perm(); permission != 0o600 {
			t.Fatalf("state permissions = %o", permission)
		}
	}
	temporary, err := filepath.Glob(filepath.Join(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("temporary files remain: %v", temporary)
	}
}

func TestYAMLStoreDoesNotReplaceMalformedState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.yaml")
	original := []byte("version: [not valid\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	store := YAMLStore{Path: path}
	if err := store.Append(Run{ID: "run-1", Outcome: Running, StartedAt: time.Now()}); err == nil {
		t.Fatal("expected malformed YAML to reject the update")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, original) {
		t.Fatalf("malformed state was replaced:\n%s", after)
	}
}

func TestYAMLStoreSerializesConcurrentProcesses(t *testing.T) {
	const (
		writerCount   = 4
		runsPerWriter = 20
	)
	path := filepath.Join(t.TempDir(), "state.yaml")
	type childProcess struct {
		command *exec.Cmd
		output  bytes.Buffer
	}
	children := make([]childProcess, writerCount)
	for writer := range writerCount {
		command := exec.Command(os.Args[0], "-test.run=^TestYAMLStoreWriterProcess$")
		command.Env = append(os.Environ(),
			"DMTX_YAML_WRITER_PATH="+path,
			"DMTX_YAML_WRITER_ID="+strconv.Itoa(writer),
			"DMTX_YAML_WRITER_COUNT="+strconv.Itoa(runsPerWriter),
		)
		children[writer].command = command
		command.Stdout = &children[writer].output
		command.Stderr = &children[writer].output
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
	}
	for writer := range children {
		if err := children[writer].command.Wait(); err != nil {
			t.Errorf("writer %d: %v\n%s", writer, err, children[writer].output.String())
		}
	}
	if t.Failed() {
		return
	}

	runs, err := (YAMLStore{Path: path}).List()
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != writerCount*runsPerWriter {
		t.Fatalf("runs = %d, want %d", len(runs), writerCount*runsPerWriter)
	}
	ids := make(map[string]struct{}, len(runs))
	for _, run := range runs {
		if _, duplicate := ids[run.ID]; duplicate {
			t.Fatalf("duplicate run %q", run.ID)
		}
		ids[run.ID] = struct{}{}
	}
}

func TestYAMLStoreWriterProcess(t *testing.T) {
	path := os.Getenv("DMTX_YAML_WRITER_PATH")
	if path == "" {
		t.Skip("subprocess helper")
	}
	writer, err := strconv.Atoi(os.Getenv("DMTX_YAML_WRITER_ID"))
	if err != nil {
		t.Fatal(err)
	}
	count, err := strconv.Atoi(os.Getenv("DMTX_YAML_WRITER_COUNT"))
	if err != nil {
		t.Fatal(err)
	}
	store := YAMLStore{Path: path}
	for index := range count {
		id := fmt.Sprintf("writer-%d-run-%d", writer, index)
		if err := store.Append(Run{
			ID:        id,
			Source:    "source.db",
			Target:    "target.db",
			Outcome:   Running,
			Resumable: true,
			StartedAt: time.Unix(int64(writer*count+index), 0).UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
}
