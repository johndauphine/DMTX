package app

import "encoding/json"

// Request is what a surface asks the application to do, independent of how it
// was expressed. The CLI builds one from argv; a WebUI will deserialize one
// from a JSON body.
//
// Every field is a plain, serialisable value on purpose. No interfaces, no
// io.Writer, no unexported state: a Request that cannot survive a round trip
// through JSON is a Request the HTTP surface would have to translate for, and
// that translation layer is exactly what makes two front ends drift apart.
type Request struct {
	// Command is the registry name, not the raw argv token. Aliases are
	// resolved before a Request exists, so downstream code never has to know
	// that health-check and preflight are the same thing.
	Command string `json:"command"`

	ConfigPath string `json:"config_path,omitempty"`
	StatePath  string `json:"state_path,omitempty"`

	DryRun                 bool `json:"dry_run,omitempty"`
	AcknowledgeDestructive bool `json:"acknowledge_destructive,omitempty"`

	// Latest distinguishes status from history, which share an implementation.
	Latest bool `json:"latest,omitempty"`

	// Force replaces a file init would otherwise refuse to overwrite.
	Force bool `json:"force,omitempty"`

	// RunID names a particular run for diagnose. Empty asks for the most
	// recent run that did not succeed, which is what an operator means when
	// they type diagnose after something went wrong.
	RunID string `json:"run_id,omitempty"`

	// Resume-only. Abandon and AbandonReason travel together: abandoning
	// without a reason, or giving a reason without abandoning, is refused,
	// because an abandoned run with no recorded why is not a decision anyone
	// can audit later.
	ForceResume   bool   `json:"force_resume,omitempty"`
	Abandon       bool   `json:"abandon,omitempty"`
	AbandonReason string `json:"abandon_reason,omitempty"`
}

// Stream names where a message belongs. They exist because the CLI contract
// includes *which* stream a line went to, and a renderer cannot reconstruct
// that from the text alone.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// Message is one human-readable line an operation produced, tagged with the
// stream it belongs on.
//
// Holding these as data rather than writing them as they occur is the whole
// point of the seam: two surfaces can be compared for equality, and the same
// operation can be rendered as text for a terminal or as JSON for an API
// without running it twice or scraping its output.
type Message struct {
	Stream string `json:"stream"`
	Text   string `json:"text"`
}

// Outcome is everything that happened, in a form any surface can render.
//
// ExitCode is retained rather than derived because the exit-code taxonomy is
// part of the public contract: callers script against it, and recomputing it
// per surface would let the CLI and an API disagree about whether the same
// operation failed.
type Outcome struct {
	Command  string    `json:"command"`
	ExitCode int       `json:"exit_code"`
	Messages []Message `json:"messages,omitempty"`
	Payload  *Payload  `json:"payload,omitempty"`
}

// Payload carries the structured facts an operation produced, already
// marshalled.
//
// Holding raw JSON rather than mirrored Go structs is deliberate. The commands
// encode eight distinct shapes; redeclaring each one here would create a second
// set of types that must be kept in step with the engine's, and Stage 4 spent
// considerable effort establishing that a surface which re-derives facts
// eventually disagrees with the engine that decided them.
//
// It also makes the guarantee this refactor needs cheap to keep: a renderer
// writes Data verbatim, so CLI output is byte-identical to what the command
// produced before the seam existed. And it gives the parity test the strongest
// form it can take - comparing the bytes two surfaces would actually emit,
// rather than two Go values that might marshal differently.
type Payload struct {
	// Kind names the shape so a consumer knows what it is holding without
	// having to infer it from the command.
	Kind string          `json:"kind"`
	Data json.RawMessage `json:"data"`
}

// Payload kinds, one per structured shape a command can produce.
const (
	PayloadPlan            = "plan"
	PayloadResult          = "result"
	PayloadPartialResult   = "partial_result"
	PayloadRun             = "run"
	PayloadRuns            = "runs"
	PayloadPreflightReport = "preflight_report"
	PayloadResumeResponse  = "resume_response"
	PayloadConfig          = "config"
	PayloadDiagnosis       = "diagnosis"
	PayloadAnalysis        = "analysis"
)

// outcomeBuilder accumulates messages while an operation runs.
//
// It exists so converting the existing procedural commands is mechanical:
// every `fmt.Fprintf(stderr, ...)` followed by `return CODE` becomes one
// builder call, with the text and stream preserved exactly. Preserving them
// exactly is what lets the existing CLI tests keep working as the check that
// this refactor changed no behaviour.
type outcomeBuilder struct {
	command  string
	messages []Message
	payload  *Payload
}

func newOutcome(command string) *outcomeBuilder {
	return &outcomeBuilder{command: command}
}

// out records a line the CLI would have written to stdout.
func (builder *outcomeBuilder) out(text string) {
	builder.messages = append(
		builder.messages,
		Message{Stream: StreamStdout, Text: text},
	)
}

// fail records a line the CLI would have written to stderr.
func (builder *outcomeBuilder) fail(text string) {
	builder.messages = append(
		builder.messages,
		Message{Stream: StreamStderr, Text: text},
	)
}

// done finishes with an exit code, keeping whatever messages and payload were
// accumulated.
func (builder *outcomeBuilder) done(code int) Outcome {
	return Outcome{
		Command:  builder.command,
		ExitCode: code,
		Messages: builder.messages,
		Payload:  builder.payload,
	}
}

// setPayload marshals a structured value once, so every renderer writes the
// same bytes. A marshalling failure is returned rather than swallowed: it means
// the command produced something it cannot describe, which is a defect, not a
// presentation detail.
func (builder *outcomeBuilder) setPayload(kind string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	builder.payload = &Payload{Kind: kind, Data: data}
	return nil
}

// failWith is the common shape: one stderr line, then a non-zero exit.
func (builder *outcomeBuilder) failWith(code int, text string) Outcome {
	builder.fail(text)
	return builder.done(code)
}

// marshalOutcome is the single place an Outcome becomes bytes, so every
// surface that emits one emits the same shape.
func marshalOutcome(outcome Outcome) ([]byte, error) {
	return json.Marshal(outcome)
}
