package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/johndauphine/dmtx/internal/app"
)

// Turning a typed line into a Request.
//
// The console has words where the command line has argv. Everything after the
// splitting is app.ParseRequest, so the flag rules exist once; the splitting
// itself is here because the command line never does it - a shell did that
// before dmtx was started.

// errUnterminatedQuote is returned for a line whose quote never closes.
var errUnterminatedQuote = errors.New("unterminated quote")

// errMultipleLines is returned for input holding more than one line.
var errMultipleLines = errors.New("input holds more than one line")

// splitLine breaks a typed line into arguments.
//
// This is deliberately not a shell. Spaces and tabs separate, single and double
// quotes group, and a backslash inside a double-quoted span escapes the next
// character. There is no variable expansion, no globbing, no command
// substitution, no operators - a line is a command and its flags, nothing that
// reaches back out into the machine.
//
// Only spaces and tabs, and only ASCII ones. A non-breaking space arrives by
// pasting from a web page rather than by being typed, and splitting on it would
// break a path that an operator can see is one path. Quoting protects it either
// way.
//
// Newlines are the other half of that, and they are refused rather than treated
// as separators. Two pasted lines joined into one would silently produce a
// different command than either: "status" and "--state m.db" pasted together
// tokenise into a perfectly valid status the operator never typed. The same
// reasoning as decodeRequest refusing a body with two JSON documents - a caller
// must not be able to believe it asked for something that never happened. A
// trailing newline is trimmed rather than refused, because that is what a paste
// or a script's ReadString leaves behind and is not part of what was typed.
//
// Quoting exists at all because config paths contain spaces. Without it an
// operator with a config under "My Documents" would find the console unable to
// express something the command line handles without being asked.
//
// An unterminated quote is refused rather than closed at end of line. Closing
// it would run a command the operator did not finish typing, and the one thing
// worse than refusing a destructive command is starting a different one.
func splitLine(line string) ([]string, error) {
	line = strings.Trim(line, " \t\r\n\v\f")
	if strings.ContainsAny(line, "\r\n") {
		return nil, errMultipleLines
	}
	var (
		args    []string
		current strings.Builder
		quote   rune
		started bool
		escaped bool
	)
	for _, character := range line {
		switch {
		case escaped:
			current.WriteRune(character)
			escaped = false
		case quote == '"' && character == '\\':
			escaped = true
		case quote != 0:
			if character == quote {
				quote = 0
				break
			}
			current.WriteRune(character)
		case character == '\'' || character == '"':
			quote = character
			// An empty quoted span is still an argument: --abandon-reason ""
			// has to be expressible, even though the pairing rule then refuses
			// it, because refusing it is the answer the operator should get.
			started = true
		case character == ' ' || character == '\t':
			if started {
				args = append(args, current.String())
				current.Reset()
				started = false
			}
		default:
			current.WriteRune(character)
			started = true
		}
	}
	if quote != 0 || escaped {
		return nil, errUnterminatedQuote
	}
	if started {
		args = append(args, current.String())
	}
	return args, nil
}

// parse answers what a typed line means, without running it.
//
// Separate from starting a job on purpose. The console shows the operator what
// their line resolved to before anything runs, and the lines that answer
// themselves - version, help, an unknown command - are answered here without a
// job ever existing. It also means POST /api/v1/jobs keeps taking a Request and
// nothing else, so a script driving the API directly is unaffected by the
// console having a different input shape.
//
// Session defaults are applied to the parsed Request, so what comes back is
// what would run rather than what was typed. Showing the operator anything else
// would be the status-bar mistake again: a display that is right until a
// default makes it wrong.
//
// POST /api/v1/jobs applies them again to whatever it is sent, which costs
// nothing: applyTo only fills fields that are empty, so a Request that already
// has them is unchanged. Applying here as well is what lets a console show the
// resolved paths before it starts anything, rather than after.
func (server *Server) parse(writer http.ResponseWriter, request *http.Request) {
	var asked struct {
		Line string `json:"line"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&asked); err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": "malformed request: " + err.Error(),
		})
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": "request body holds more than one JSON document",
		})
		return
	}

	args, err := splitLine(asked.Line)
	if err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
		return
	}

	parsed, outcome, dispatched := app.ParseRequest(args)
	if !dispatched {
		// Not an HTTP error. The line was understood and answered; "dmtx
		// --version" is a successful parse whose answer happens to be a
		// version. Mapping it onto a 4xx would make the console re-decide what
		// the seam already decided, which is the one thing Stage 5 must not do.
		writeJSON(writer, http.StatusOK, map[string]any{
			"dispatched": false,
			"outcome":    outcome,
		})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"dispatched": true,
		"request":    server.defaults.applyTo(parsed),
	})
}
