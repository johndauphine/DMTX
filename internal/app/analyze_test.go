package app

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/migrate"
)

// analyzableConfig pins the memory ceiling so the plan is deterministic.
//
// Without it the budget comes from memory available at the moment it is read,
// and workers derive from that budget through boundedMemorySlots - so on a
// machine where memory is the binding constraint rather than the worker cap,
// two readings seconds apart can resolve to different worker counts. That is a
// test which passes on a laptop and fails on a small CI runner, occasionally,
// for reasons unrelated to the code.
const analyzableConfig = `
source:
  type: sqlite
  database: source.db
target:
  type: sqlite
  database: target.db
migration:
  workers: 3
  memory_ceiling_bytes: 268435456
`

func analyzableConfigPath(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "migration.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func analysisOf(t *testing.T, path string) (Outcome, Analysis) {
	t.Helper()
	outcome := executeAnalyze(context.Background(), Request{Command: "analyze", ConfigPath: path})
	if outcome.Payload == nil {
		return outcome, Analysis{}
	}
	var analysis Analysis
	if err := json.Unmarshal(outcome.Payload.Data, &analysis); err != nil {
		t.Fatalf("decode analysis: %v", err)
	}
	return outcome, analysis
}

// TestAnalyzeReportsEverySettingWithItsProvenance is the feature: the answer to
// "why four workers?" is the provenance, not the number.
func TestAnalyzeReportsEverySettingWithItsProvenance(t *testing.T) {
	outcome, analysis := analysisOf(t, analyzableConfigPath(t, analyzableConfig))
	if outcome.ExitCode != Success {
		t.Fatalf("analyze failed: %+v", outcome.Messages)
	}
	if analysis.Tuning == nil {
		t.Fatal("analyze produced no tuning")
	}

	for label, setting := range map[string]struct {
		value      int64
		provenance string
	}{
		"workers":          {analysis.Tuning.Workers.Value, analysis.Tuning.Workers.Provenance},
		"readers":          {analysis.Tuning.Readers.Value, analysis.Tuning.Readers.Provenance},
		"writers":          {analysis.Tuning.Writers.Value, analysis.Tuning.Writers.Provenance},
		"queue depth":      {analysis.Tuning.QueueDepth.Value, analysis.Tuning.QueueDepth.Provenance},
		"chunk rows":       {analysis.Tuning.ChunkRows.Value, analysis.Tuning.ChunkRows.Provenance},
		"connection limit": {analysis.Tuning.ConnectionLimit.Value, analysis.Tuning.ConnectionLimit.Provenance},
		"memory budget":    {analysis.Tuning.MemoryBudget.Value, analysis.Tuning.MemoryBudget.Provenance},
	} {
		if setting.value <= 0 {
			t.Errorf("%s is %d; the plan should resolve to a usable value", label, setting.value)
		}
		if setting.provenance == "" {
			t.Errorf(
				"%s has no provenance, so the report says what was chosen and "+
					"not why - which is the question analyze exists to answer",
				label,
			)
		}
	}
}

// TestAnalyzeCreditsARequestedSettingToTheOperator pins that a value the
// operator wrote down is reported as theirs rather than as something dmtx
// decided. Told "derived" for a number they chose, they go looking for the
// logic that chose it.
func TestAnalyzeCreditsARequestedSettingToTheOperator(t *testing.T) {
	_, analysis := analysisOf(t, analyzableConfigPath(t, analyzableConfig))
	if analysis.Tuning.Workers.Value != 3 {
		t.Fatalf("workers resolved to %d, want the requested 3", analysis.Tuning.Workers.Value)
	}
	if analysis.Tuning.Workers.Provenance == "derived" {
		t.Errorf(
			"a requested worker count is credited as %q, which sends an "+
				"operator looking for logic that did not run",
			analysis.Tuning.Workers.Provenance,
		)
	}
}

// TestAnalyzeAgreesWithTheDryRunDisclosure pins the property that made sharing
// one code path worth it.
//
// Two ways of reporting "the effective plan" would be two chances to disagree,
// and an operator comparing them would have no way to tell which was right.
func TestAnalyzeAgreesWithTheDryRunDisclosure(t *testing.T) {
	path := analyzableConfigPath(t, analyzableConfig)
	_, analysis := analysisOf(t, path)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	disclosed, err := migrate.DiscloseTuning(context.Background(), cfg)
	if err != nil {
		t.Fatalf("disclose: %v", err)
	}

	// Compared in full, including the memory budget. An earlier version of this
	// test excused the budget because two readings differed by 229KB - which
	// was true, and was the wrong fix: the fixture pins memory_ceiling_bytes,
	// so the budget is clamped rather than measured and every value below it is
	// deterministic. Excusing the field would also have excused the values
	// derived from it, which is what made this flaky rather than merely loose.
	if analysis.Tuning.MemoryBudget.Provenance != "user_memory_ceiling" {
		t.Fatalf(
			"the fixture is not pinning memory: budget provenance is %q, so "+
				"this comparison is against a live reading and will flake",
			analysis.Tuning.MemoryBudget.Provenance,
		)
	}
	fromAnalysis, err := json.Marshal(analysis.Tuning)
	if err != nil {
		t.Fatal(err)
	}
	fromDisclosure, err := json.Marshal(disclosed)
	if err != nil {
		t.Fatal(err)
	}
	if string(fromAnalysis) != string(fromDisclosure) {
		t.Errorf(
			"analyze and the dry-run disclosure describe the plan differently:\n"+
				"  analyze:  %s\n  dry run:  %s",
			fromAnalysis, fromDisclosure,
		)
	}
}

// TestAnalyzeReadsNoDatabase pins that the report is offline. The question it
// answers is often asked precisely when a database is unreachable.
func TestAnalyzeReadsNoDatabase(t *testing.T) {
	path := analyzableConfigPath(t, `
source:
  type: postgres
  host: 203.0.113.1
  port: 5432
  database: nowhere
  user: nobody
  password: nothing
target:
  type: sqlite
  database: target.db
`)
	outcome, _ := analysisOf(t, path)
	if outcome.ExitCode != Success {
		t.Fatalf(
			"analyze failed against unreachable endpoints, so it is connecting: %+v",
			outcome.Messages,
		)
	}
}

// TestAnalyzeRendersProvenanceForAPerson pins that the terminal output carries
// the reasons too, not only the payload.
func TestAnalyzeRendersProvenanceForAPerson(t *testing.T) {
	outcome, analysis := analysisOf(t, analyzableConfigPath(t, analyzableConfig))
	said := saidBy(outcome)
	for _, label := range []string{
		"workers", "readers", "writers", "queue depth",
		"chunk rows", "connection limit", "memory budget",
	} {
		if !strings.Contains(said, label) {
			t.Errorf("the rendered report omits %q:\n%s", label, said)
		}
	}
	if !strings.Contains(said, analysis.Tuning.Workers.Provenance) {
		t.Errorf(
			"the rendered report omits the provenance %q, so a person reading "+
				"the terminal sees the number without the reason:\n%s",
			analysis.Tuning.Workers.Provenance, said,
		)
	}
}

// TestAnalyzeRefusesWithoutAConfig pins the usage message.
func TestAnalyzeRefusesWithoutAConfig(t *testing.T) {
	outcome := executeAnalyze(context.Background(), Request{Command: "analyze"})
	if outcome.ExitCode == Success {
		t.Fatal("analyze succeeded with no configuration to analyze")
	}
	if !strings.Contains(saidBy(outcome), "usage: dmtx analyze --config") {
		t.Errorf("analyze did not say how to call it: %q", saidBy(outcome))
	}
}

// TestAnalysisPayloadWireShape pins the JSON a console will read, and is the
// shape TestEveryPayloadKindIsPinned points at for PayloadAnalysis.
func TestAnalysisPayloadWireShape(t *testing.T) {
	outcome, _ := analysisOf(t, analyzableConfigPath(t, analyzableConfig))
	var decoded map[string]any
	if err := json.Unmarshal(outcome.Payload.Data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	present := map[string]bool{}
	collectPaths("", decoded, present)

	for _, path := range []string{
		"path",
		"tuning.workers", "tuning.workers.value", "tuning.workers.provenance",
		"tuning.readers", "tuning.writers", "tuning.queue_depth",
		"tuning.chunk_rows", "tuning.connection_limit",
		"tuning.memory_budget_bytes",
	} {
		if !present[path] {
			t.Errorf("the wire shape lost %s", path)
		}
	}
}

// TestAnalyzeRefusesAConfigurationRunWouldReject pins that analyze does not
// describe a plan for a migration that will never start.
//
// run validates the adapter route before doing anything. An analyze that
// skipped the check would answer "here is your effective plan" for a
// configuration dmtx cannot execute - a confident answer to a question the
// operator did not ask.
//
// The route matters. An invented engine type is rejected by config.Parse, so a
// fixture using one never reaches the validation this test exists for: the
// first version did exactly that, and passed with the check removed.
// clickhouse-to-sqlite parses cleanly and is an uncertified pair, so it fails
// where it should.
func TestAnalyzeRefusesAConfigurationRunWouldReject(t *testing.T) {
	path := analyzableConfigPath(t, `
source:
  type: clickhouse
  host: source.internal
  database: source
  user: reader
  password: secret
target:
  type: sqlite
  database: target.db
`)
	outcome, _ := analysisOf(t, path)
	if outcome.ExitCode == Success {
		t.Fatal("analyze reported a plan for a migration dmtx cannot run")
	}
	said := saidBy(outcome)
	if !strings.Contains(said, "not implemented") {
		t.Errorf("the refusal does not say the route is unsupported: %q", said)
	}

	// And the same configuration must be refused by run, or analyze is
	// refusing something that would in fact have worked.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Parse(data)
	if err != nil {
		t.Fatalf("the fixture no longer parses, so it tests the wrong thing: %v", err)
	}
	if err := migrate.ValidateMigration(cfg); err == nil {
		t.Fatal("the fixture route is valid, so this test proves nothing")
	}
}
