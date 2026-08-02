package app

import (
	"fmt"
	"io"
)

// RenderText writes an Outcome the way the command line always has: each
// message to the stream it was tagged with, then the structured payload, if
// any, as a single JSON line on stdout.
//
// The payload bytes are written verbatim rather than re-marshalled. That is
// what makes this refactor provably behaviour-preserving: the command produced
// exactly these bytes before the seam existed, and the existing CLI tests
// compare them.
func RenderText(stdout, stderr io.Writer, outcome Outcome) error {
	for _, message := range outcome.Messages {
		target := stdout
		if message.Stream == StreamStderr {
			target = stderr
		}
		if _, err := fmt.Fprintln(target, message.Text); err != nil {
			return err
		}
	}
	if outcome.Payload == nil {
		return nil
	}
	// encoding/json terminates Encode with a newline; the payload was captured
	// from Marshal, which does not. Adding it here keeps the byte stream
	// identical to what Encode produced.
	if _, err := fmt.Fprintf(stdout, "%s\n", outcome.Payload.Data); err != nil {
		return err
	}
	return nil
}

// RenderJSON writes the whole Outcome, including its messages and exit code,
// as one JSON document.
//
// This is the shape a non-terminal surface consumes. It deliberately includes
// the human-readable messages rather than discarding them: an API client that
// wants to show an operator what happened should show the same words the CLI
// would, not a paraphrase reconstructed from the exit code.
func RenderJSON(stdout io.Writer, outcome Outcome) error {
	data, err := marshalOutcome(outcome)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "%s\n", data)
	return err
}
