package api

import (
	"fmt"
	"net/http"
	"strconv"
)

// startJob accepts a command and returns immediately with its id.
func (server *Server) startJob(writer http.ResponseWriter, request *http.Request) {
	decoded, ok := server.decodeRequest(writer, request)
	if !ok {
		return
	}
	running, err := server.jobs.start(decoded)
	if err != nil {
		writeJSON(writer, http.StatusInternalServerError, map[string]string{
			"error": "could not start the command",
		})
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]string{
		"id":      running.ID(),
		"command": running.command,
	})
}

// jobStatus reports where a job has got to, and its outcome once it has one.
//
// This is what makes a reconnecting client whole without replaying a stream: it
// can ask the state directly and only subscribe if the answer is "still going".
func (server *Server) jobStatus(writer http.ResponseWriter, request *http.Request) {
	running, ok := server.jobs.find(request.PathValue("id"))
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "no such job"})
		return
	}
	body := map[string]any{
		"id":      running.ID(),
		"command": running.command,
		"state":   "running",
	}
	if outcome, finished := running.result(); finished {
		body["state"] = "finished"
		body["outcome"] = outcome
	}
	writeJSON(writer, http.StatusOK, body)
}

// cancelJob asks a running command to stop.
func (server *Server) cancelJob(writer http.ResponseWriter, request *http.Request) {
	if err := server.jobs.cancel(request.PathValue("id")); err != nil {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "no such job"})
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]string{"state": "cancelling"})
}

// jobEvents streams a job's events until it ends or the client goes away.
//
// The client going away ends the stream, not the job. That distinction is the
// whole reason jobs exist.
func (server *Server) jobEvents(writer http.ResponseWriter, request *http.Request) {
	running, ok := server.jobs.find(request.PathValue("id"))
	if !ok {
		writeJSON(writer, http.StatusNotFound, map[string]string{"error": "no such job"})
		return
	}
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeJSON(writer, http.StatusInternalServerError, map[string]string{
			"error": "this connection cannot stream",
		})
		return
	}

	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(http.StatusOK)
	flusher.Flush()

	from := resumeFrom(request)
	for {
		// Events and the wake-up channel together, from one lock. See job.next:
		// reading and subscribing as two steps leaves a gap an event can fall
		// into, and this loop would then wait forever for a job that had
		// already finished.
		pending, ended, changed := running.next(from)
		for _, event := range pending {
			if _, err := fmt.Fprintf(
				writer,
				"id: %d\nevent: %s\ndata: %s\n\n",
				event.Sequence, event.Kind, eventData(event),
			); err != nil {
				return
			}
			from = event.Sequence
		}
		flusher.Flush()
		if ended {
			return
		}

		select {
		case <-changed:
		case <-request.Context().Done():
			return
		}
	}
}

// eventData is an event's payload, or an empty object when it has none, so
// every frame carries JSON a client can parse without a special case.
func eventData(event jobEvent) string {
	if len(event.Data) == 0 {
		return "{}"
	}
	return string(event.Data)
}

// resumeFrom reads the sequence a client already has.
//
// Last-Event-ID is what a browser's EventSource resends by itself after a drop,
// so reconnection needs no cooperation from the page. The query parameter is
// for everything else - curl, scripts, the parity tests.
func resumeFrom(request *http.Request) int {
	if header := request.Header.Get("Last-Event-ID"); header != "" {
		if sequence, err := strconv.Atoi(header); err == nil && sequence > 0 {
			return sequence
		}
	}
	if query := request.URL.Query().Get("from"); query != "" {
		if sequence, err := strconv.Atoi(query); err == nil && sequence > 0 {
			return sequence
		}
	}
	return 0
}
