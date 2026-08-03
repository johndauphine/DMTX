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
// Writing the status first commits a 200 that cannot be withdrawn, so a value
// that fails to marshal leaves the client holding an empty body it must read as
// success.
//
// No caller can reach this today: Outcome.Payload is filled by
// outcomeBuilder.setPayload, which marshals through json.Marshal and returns
// the error, so payload bytes are valid by construction. That is a property of
// the callers rather than of writeJSON, and it is one refactor away from
// changing - a payload assembled from a template, a cache, or another process
// would not carry the same guarantee. This pins writeJSON's own contract so
// that change stays a bug in one place instead of a silent empty 200.
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
