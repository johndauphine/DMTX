package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestWriteJSONDoesNotCommitSuccessItCannotDeliver pins that a response is
// encoded before its status is written.
//
// Streaming an encoder straight into the ResponseWriter sends 200, then fails
// partway, and the client reads a truncated body as a successful answer.
// Outcome.Payload is a json.RawMessage, so bytes that fail to marshal are a
// real shape this function can be handed rather than a contrived one.
func TestWriteJSONDoesNotCommitSuccessItCannotDeliver(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusOK, json.RawMessage("this is not json"))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf(
			"an unencodable value returned %d; the client is told the request "+
				"succeeded and handed a body that is not the answer",
			recorder.Code,
		)
	}
	if !json.Valid(recorder.Body.Bytes()) {
		t.Errorf("the failure response is not itself JSON: %s", recorder.Body)
	}
}

// TestWriteJSONSetsTheHeadersThatKeepABodyFromBeingRenderedAsMarkup pins
// nosniff, which is what stops a reflected error string being treated as HTML.
func TestWriteJSONSetsTheHeadersThatKeepABodyFromBeingRenderedAsMarkup(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeJSON(recorder, http.StatusOK, map[string]string{"ok": "yes"})

	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type is %q, want application/json", got)
	}
	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Errorf("X-Content-Type-Options is %q, want nosniff", got)
	}
}
