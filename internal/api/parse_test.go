package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/johndauphine/dmtx/internal/app"
)

// TestSplitLineHandlesWhatAShellWouldHaveHandled covers the tokenising the
// command line never has to do, because a shell did it before dmtx started.
func TestSplitLineHandlesWhatAShellWouldHaveHandled(t *testing.T) {
	for _, testCase := range []struct {
		name string
		line string
		want []string
	}{
		{"empty", "", nil},
		{"only spaces", "   \t ", nil},
		{"bare words", "run --dry-run", []string{"run", "--dry-run"}},
		{"runs of spaces", "  run   --dry-run  ", []string{"run", "--dry-run"}},
		{"tabs separate", "run\t--dry-run", []string{"run", "--dry-run"}},
		{
			"double-quoted path with spaces",
			`run --config "My Documents/migration.yaml"`,
			[]string{"run", "--config", "My Documents/migration.yaml"},
		},
		{
			"single-quoted path with spaces",
			`run --config 'My Documents/migration.yaml'`,
			[]string{"run", "--config", "My Documents/migration.yaml"},
		},
		{
			"single quotes are literal inside double",
			`resume --abandon-reason "it's stuck"`,
			[]string{"resume", "--abandon-reason", "it's stuck"},
		},
		{
			"double quotes are literal inside single",
			`resume --abandon-reason 'the "source" vanished'`,
			[]string{"resume", "--abandon-reason", `the "source" vanished`},
		},
		{
			"backslash escapes inside double quotes",
			`run --config "a\"b.yaml"`,
			[]string{"run", "--config", `a"b.yaml`},
		},
		{
			"backslash is literal outside quotes",
			`run --config C:\migrations\m.yaml`,
			[]string{"run", "--config", `C:\migrations\m.yaml`},
		},
		{
			"backslash is literal inside single quotes",
			`run --config 'C:\m.yaml'`,
			[]string{"run", "--config", `C:\m.yaml`},
		},
		{
			"an empty quoted span is still an argument",
			`resume --abandon --abandon-reason ""`,
			[]string{"resume", "--abandon", "--abandon-reason", ""},
		},
		{
			"quotes may open mid-word",
			`run --config=a" b".yaml`,
			[]string{"run", `--config=a b.yaml`},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := splitLine(testCase.line)
			if err != nil {
				t.Fatalf("splitLine(%q) = %v", testCase.line, err)
			}
			if len(got) != len(testCase.want) {
				t.Fatalf("splitLine(%q) = %q, want %q", testCase.line, got, testCase.want)
			}
			for index := range got {
				if got[index] != testCase.want[index] {
					t.Fatalf("splitLine(%q) = %q, want %q", testCase.line, got, testCase.want)
				}
			}
		})
	}
}

// TestSplitLineRefusesAnUnfinishedLine pins the refusal rather than a guess.
//
// Closing the quote at end of line would run a command the operator had not
// finished typing - and the argument list it produced would be a different
// command from the one they were partway through writing.
func TestSplitLineRefusesAnUnfinishedLine(t *testing.T) {
	for _, line := range []string{
		`run --config "unfinished`,
		`run --config 'unfinished`,
		`run --config "trailing escape\`,
	} {
		if _, err := splitLine(line); err == nil {
			t.Errorf("splitLine(%q) was accepted", line)
		}
	}
}

// TestSplitLineIsNotAShell states what the console deliberately cannot express.
//
// Every one of these is a character a shell would act on. Here they are text,
// because a console input line is a command and its flags - nothing that
// reaches back out into the machine. A future edit that adds expansion has to
// change this test, which is the point of writing it.
func TestSplitLineIsNotAShell(t *testing.T) {
	for _, testCase := range []struct {
		line string
		want string
	}{
		{`run --config $HOME/m.yaml`, "$HOME/m.yaml"},
		{`run --config *.yaml`, "*.yaml"},
		{"run --config `whoami`.yaml", "`whoami`.yaml"},
		{`run --config a;rm.yaml`, "a;rm.yaml"},
		{`run --config a|b.yaml`, "a|b.yaml"},
		{`run --config ~/m.yaml`, "~/m.yaml"},
	} {
		got, err := splitLine(testCase.line)
		if err != nil {
			t.Fatalf("splitLine(%q) = %v", testCase.line, err)
		}
		if len(got) != 3 || got[2] != testCase.want {
			t.Errorf("splitLine(%q) = %q, want the path left as %q", testCase.line, got, testCase.want)
		}
	}
}

// TestParseAgreesWithTheCommandLine is the §21.1 criterion tested at the point
// it now actually holds.
//
// Before ParseRequest was exported there were two ways to reach a Request and
// only one of them had flag rules. This drives real lines through the route and
// compares against app.ParseRequest on the same words - which is not comparing
// a function against itself, because the route also tokenises, applies session
// defaults, and serialises through JSON, and any of those could lose something.
func TestParseAgreesWithTheCommandLine(t *testing.T) {
	server := newTestServer(t)

	for _, line := range []string{
		"run --config m.yaml",
		"run --config m.yaml --dry-run",
		"run --config m.yaml --state other.state.db --acknowledge-destructive",
		"status --state m.yaml.state.db",
		"history --state m.yaml.state.db",
		"validate --config m.yaml",
		"diagnose --state m.yaml.state.db --run abc",
		// Quoted, because the reason is two words - which is the whole
		// argument for tokenising in Go rather than splitting on spaces in
		// JavaScript. Unquoted, "up" is a stray positional and resume is
		// correctly refused.
		`resume --config m.yaml --abandon --abandon-reason "gave up"`,
		"preflight --config m.yaml",
		"health-check --config m.yaml",
		"init --config m.yaml --force",
	} {
		t.Run(line, func(t *testing.T) {
			body := parseLine(t, server, line)
			if !body.Dispatched {
				t.Fatalf("%q was not dispatched: %v", line, body.Outcome)
			}
			words, err := splitLine(line)
			if err != nil {
				t.Fatal(err)
			}
			want, _, dispatched := app.ParseRequest(words)
			if !dispatched {
				t.Fatalf("app.ParseRequest(%q) did not dispatch", words)
			}
			if body.Request != want {
				t.Fatalf("route parsed %+v, command line parsed %+v", body.Request, want)
			}
		})
	}
}

// TestParseAnswersTheLinesThatAnswerThemselves covers dispatched=false.
//
// version, help and an unknown command have no orchestration to perform. They
// come back as an Outcome rather than as an HTTP error, because the line was
// understood - a successful parse whose answer happens not to be a job.
func TestParseAnswersTheLinesThatAnswerThemselves(t *testing.T) {
	server := newTestServer(t)

	for _, testCase := range []struct {
		line     string
		contains string
	}{
		{"--version", app.Version},
		{"version", app.Version},
		{"help", "dmtx"},
		{"--help", "dmtx"},
		{"", "DMTX terminal UI is planned"},
		{"nonsense-command", "nonsense-command"},
	} {
		t.Run(testCase.line, func(t *testing.T) {
			body := parseLine(t, server, testCase.line)
			if body.Dispatched {
				t.Fatalf("%q dispatched to %+v", testCase.line, body.Request)
			}
			var joined strings.Builder
			for _, message := range body.Outcome.Messages {
				joined.WriteString(message.Text)
				joined.WriteString("\n")
			}
			if !strings.Contains(joined.String(), testCase.contains) {
				t.Fatalf("%q answered %q, wanted it to mention %q",
					testCase.line, joined.String(), testCase.contains)
			}
		})
	}
}

// TestParseRefusesFlagMistakesTheCommandLineRefuses is why this route exists.
//
// Each of these is a rule that lives in one of app's argv parsers. A console
// that built Requests in JavaScript would have to carry every one of them, and
// nothing in Go could tell when it stopped.
func TestParseRefusesFlagMistakesTheCommandLineRefuses(t *testing.T) {
	server := newTestServer(t)

	for _, testCase := range []struct {
		name string
		line string
	}{
		{"run repeats a flag", "run --config a.yaml --config b.yaml"},
		{"run has an unknown flag", "run --config a.yaml --wat"},
		{"run has no config", "run"},
		{"diagnose has a misspelled flag", "diagnose --state s.db --ruun abc"},
		{"diagnose repeats a flag", "diagnose --state a.db --state b.db"},
		{"resume abandons with no reason", "resume --config m.yaml --abandon"},
		{"resume gives a reason without abandoning", "resume --config m.yaml --abandon-reason why"},
		{"resume abandons and forces at once", "resume --config m.yaml --abandon --abandon-reason x --force-resume"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			body := parseLine(t, server, testCase.line)
			if body.Dispatched {
				t.Fatalf("%q was accepted as %+v", testCase.line, body.Request)
			}
			if body.Outcome.ExitCode == 0 {
				t.Fatalf("%q was refused with exit code 0", testCase.line)
			}
		})
	}
}

// TestParseAppliesSessionDefaults keeps the route honest about what would run.
//
// Returning the typed Request rather than the resolved one would be the
// status-bar mistake from the design note: a display that is right until a
// default makes it wrong, on the surface an operator uses to check what a
// destructive command is about to touch.
func TestParseAppliesSessionDefaults(t *testing.T) {
	server := newTestServer(t)
	if err := server.defaults.set(SessionConfig, "from-defaults.yaml"); err != nil {
		t.Fatal(err)
	}

	body := parseLine(t, server, "validate")
	if !body.Dispatched {
		t.Fatalf("not dispatched: %v", body.Outcome)
	}
	if body.Request.ConfigPath != "from-defaults.yaml" {
		t.Fatalf("config path = %q, want the session default", body.Request.ConfigPath)
	}

	// An explicit path still wins.
	body = parseLine(t, server, "validate --config typed.yaml")
	if body.Request.ConfigPath != "typed.yaml" {
		t.Fatalf("config path = %q, want the typed path", body.Request.ConfigPath)
	}
}

// TestParseRefusesAnUnfinishedLine checks the tokenising failure reaches the
// caller as a refusal rather than as a differently-parsed command.
func TestParseRefusesAnUnfinishedLine(t *testing.T) {
	server := newTestServer(t)

	recorder := postParse(t, server, `{"line":"run --config \"unfinished"}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body)
	}
}

// TestParseRefusesAnUnknownField pins that the body is closed like every other.
func TestParseRefusesAnUnknownField(t *testing.T) {
	server := newTestServer(t)

	recorder := postParse(t, server, `{"line":"status","argv":["status"]}`)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body)
	}
}

// parsedLine is what POST /api/v1/parse answers.
type parsedLine struct {
	Dispatched bool        `json:"dispatched"`
	Request    app.Request `json:"request"`
	Outcome    app.Outcome `json:"outcome"`
}

func parseLine(t *testing.T, server *Server, line string) parsedLine {
	t.Helper()
	recorder := postParse(t, server, map[string]string{"line": line})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d for %q: %s", recorder.Code, line, recorder.Body)
	}
	var body parsedLine
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}

// postParse sends one body to the parse route and returns the raw response.
func postParse(t *testing.T, server *Server, body any) *httptest.ResponseRecorder {
	t.Helper()
	var encoded []byte
	switch typed := body.(type) {
	case string:
		encoded = []byte(typed)
	default:
		marshalled, err := json.Marshal(typed)
		if err != nil {
			t.Fatal(err)
		}
		encoded = marshalled
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/parse",
		bytes.NewReader(encoded),
	)
	request.Header.Set("Authorization", "Bearer "+server.auth.session)
	recorder := httptest.NewRecorder()
	server.routes().ServeHTTP(recorder, request)
	return recorder
}
