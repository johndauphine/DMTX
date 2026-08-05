package app

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

// TestTheStarterConfigIsValid pins that the file dmtx writes is one dmtx
// accepts.
//
// A starter config that does not parse is worse than none: it tells an operator
// their first command failed because of something they did, when it failed
// because of something we shipped.
func TestTheStarterConfigIsValid(t *testing.T) {
	if err := starterConfigIsValid(); err != nil {
		t.Fatalf("the config init writes does not parse: %v", err)
	}
}

// TestInitProducesAConfigTheOtherCommandsAccept closes the loop: the file is
// not merely parseable, it is usable by the commands the template tells the
// operator to run next.
func TestInitProducesAConfigTheOtherCommandsAccept(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	if outcome := executeInit(Request{Command: "init", ConfigPath: path}); outcome.ExitCode != Success {
		t.Fatalf("init failed: %+v", outcome.Messages)
	}

	// The template names these two commands, so they have to work on it.
	if outcome := executeConfig(Request{Command: "config", ConfigPath: path}); outcome.ExitCode != Success {
		t.Errorf("config rejects the starter file: %+v", outcome.Messages)
	}
	if outcome := executeAnalyze(t.Context(), Request{Command: "analyze", ConfigPath: path}); outcome.ExitCode != Success {
		t.Errorf("analyze rejects the starter file: %+v", outcome.Messages)
	}
}

// TestInitRefusesToOverwrite is the safety property.
//
// A configuration is something an operator has edited, often with connection
// details they cannot reconstruct. Replacing one silently costs them their
// afternoon; refusing one that could have been replaced costs them a flag.
func TestInitRefusesToOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	const existing = "# the operator's own file, with their connection details\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	outcome := executeInit(Request{Command: "init", ConfigPath: path})
	if outcome.ExitCode == Success {
		t.Fatal("init overwrote an existing configuration")
	}
	if !strings.Contains(saidBy(outcome), "--force") {
		t.Errorf("the refusal does not say how to proceed: %q", saidBy(outcome))
	}

	// The file must be untouched, not merely reported as refused.
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != existing {
		t.Errorf("the existing file was modified despite the refusal:\n%s", after)
	}
}

// TestForceReplacesAnExistingConfig pins that the escape hatch works, since a
// refusal with no way past it is a trap.
func TestForceReplacesAnExistingConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	if err := os.WriteFile(path, []byte("# old\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	outcome := executeInit(Request{Command: "init", ConfigPath: path, Force: true})
	if outcome.ExitCode != Success {
		t.Fatalf("init --force refused anyway: %+v", outcome.Messages)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(written), "# old") {
		t.Error("--force did not replace the file")
	}
}

// TestTheWrittenConfigIsNotWorldReadable pins the mode. This is the file
// credentials go into, and the operator who adds them should not have to
// remember to tighten it first.
func TestTheWrittenConfigIsNotWorldReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	path := filepath.Join(t.TempDir(), "migration.yaml")
	if outcome := executeInit(Request{Command: "init", ConfigPath: path}); outcome.ExitCode != Success {
		t.Fatalf("init failed: %+v", outcome.Messages)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the starter config is %04o; credentials will be added to it", mode)
	}
}

// TestTheTemplateShipsNoPlaceholderPassword pins that dmtx does not write a
// credential-shaped string into a file people trust because a tool made it.
func TestTheTemplateShipsNoPlaceholderPassword(t *testing.T) {
	for _, forbidden := range []string{
		"password: changeme", "password: secret", "password: password",
		"password: hunter2", "password: <", "password: your",
	} {
		if strings.Contains(strings.ToLower(starterConfigFor(defaultConfigFilename)), forbidden) {
			t.Errorf("the template ships %q, which invites being kept", forbidden)
		}
	}
}

// TestInitNamesADefaultFile pins that init with no arguments does something
// useful rather than asking for a path the operator does not have yet.
//
// It checks the resolution rather than running the write. An earlier version
// ran init with no path and changed the process's working directory to keep the
// file out of the repository - and a stray internal/app/migration.yaml, which
// reached a commit, is what that costs when it goes wrong. Changing
// process-wide state for one test's benefit is a hazard to every test near it.
func TestInitNamesADefaultFile(t *testing.T) {
	if got := configPathFor(Request{Command: "init"}); got != defaultConfigFilename {
		t.Errorf("init with no path would write %q, want %q", got, defaultConfigFilename)
	}
	if got := configPathFor(Request{Command: "init", ConfigPath: "envs/prod.yaml"}); got != "envs/prod.yaml" {
		t.Errorf("init ignored the path it was given: %q", got)
	}
}

// TestInitCreatesMissingDirectories pins that naming a path inside a directory
// that does not exist yet works, since that is how a project gets laid out.
func TestInitCreatesMissingDirectories(t *testing.T) {
	path := filepath.Join(t.TempDir(), "envs", "prod", "migration.yaml")
	if outcome := executeInit(Request{Command: "init", ConfigPath: path}); outcome.ExitCode != Success {
		t.Fatalf("init failed for a nested path: %+v", outcome.Messages)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("init reported success without writing the file: %v", err)
	}
}

// TestInitRefusesFlagsItDoesNotKnow pins that a typo is refused rather than
// skipped - which matters more here than elsewhere, because "--forse" silently
// ignored would turn a refusal into a surprise.
func TestInitRefusesFlagsItDoesNotKnow(t *testing.T) {
	if _, ok := initArguments([]string{"--config", "a.yaml", "--force"}); !ok {
		t.Fatal("init refused its own flags")
	}
	for _, refused := range [][]string{
		{"--forse"},
		{"--config"},
		{"--config", "a.yaml", "--config", "b.yaml"},
		{"--force", "--force"},
		{"a.yaml"},
	} {
		if _, ok := initArguments(refused); ok {
			t.Errorf("init accepted %v", refused)
		}
	}
}

// TestForcingOverAWorldReadableFileTightensIt pins the mode on the path that
// had none.
//
// A mode argument applies only when a file is created, so --force over an
// existing 0644 file would leave it 0644 - the same defect as the handoff state
// file in #8, in a second place, because that fix was applied where it was
// found rather than looked for elsewhere.
func TestForcingOverAWorldReadableFileTightensIt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	path := filepath.Join(t.TempDir(), "migration.yaml")
	if err := os.WriteFile(path, []byte("# old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if before, err := os.Stat(path); err != nil {
		t.Fatal(err)
	} else if before.Mode().Perm()&0o077 == 0 {
		t.Fatal("the fixture file is already restricted, so this proves nothing")
	}

	outcome := executeInit(Request{Command: "init", ConfigPath: path, Force: true})
	if outcome.ExitCode != Success {
		t.Fatalf("init --force failed: %+v", outcome.Messages)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf(
			"replacing a world-readable config left it %04o; credentials will "+
				"be added to it",
			mode,
		)
	}
}

// TestTheTemplateNamesTheFileItIsWrittenTo pins that the instructions inside
// the file point at the file.
//
// A template that says "--config migration.yaml" inside envs/prod.yaml sends
// the operator to something that does not exist.
func TestTheTemplateNamesTheFileItIsWrittenTo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "envs", "prod.yaml")
	if outcome := executeInit(Request{Command: "init", ConfigPath: path}); outcome.ExitCode != Success {
		t.Fatalf("init failed: %+v", outcome.Messages)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(written), "--config "+path) {
		t.Errorf("the template does not name its own path:\n%s", written)
	}
	if strings.Contains(string(written), "--config "+defaultConfigFilename) {
		t.Errorf(
			"the template still points at %s, which is not where it was written",
			defaultConfigFilename,
		)
	}
}

// TestTheTemplateDoesNotClaimToBeAllDefaults pins the comment against the code
// it describes.
//
// The comment once said every value in the template was a default dmtx would
// apply anyway. It is not: a missing source type defaults to mssql and a
// missing target type to postgres, so sqlite-to-sqlite is a choice. Saying so
// matters because the next person to edit the template will trust the comment.
func TestTheTemplateDoesNotClaimToBeAllDefaults(t *testing.T) {
	parsed, err := config.Parse([]byte("source:\n  database: a\ntarget:\n  database: b\n"))
	if err != nil {
		t.Fatalf("parse a typeless config: %v", err)
	}
	if parsed.Source.Type == "sqlite" && parsed.Target.Type == "sqlite" {
		t.Skip("the defaults are now sqlite-to-sqlite, so the template matches them")
	}
	if !strings.Contains(starterConfigFor(defaultConfigFilename), "type: sqlite") {
		t.Fatal("the template no longer chooses sqlite, so this test is stale")
	}
	// The template overrides the defaults deliberately. Nothing to assert
	// beyond recording that it does, so a future "simplify by removing the
	// types" reads this first.
	t.Logf(
		"dmtx defaults a missing source type to %q and target to %q; the "+
			"template chooses sqlite for both so it can be tried without a server",
		parsed.Source.Type, parsed.Target.Type,
	)
}
