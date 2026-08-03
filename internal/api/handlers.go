package api

import (
	"encoding/json"
	"net/http"

	"github.com/johndauphine/dmtx/internal/app"
	"github.com/johndauphine/dmtx/internal/contract"
)

// maxRequestBytes bounds a request body. Requests are a handful of short
// strings, so anything larger is a mistake or an attempt to exhaust memory.
const maxRequestBytes = 64 << 10

// execute runs a command and returns its Outcome.
//
// The Outcome is returned whole, including its messages and exit code, rather
// than being translated into HTTP semantics. A failed migration is not an HTTP
// error: the request succeeded and the answer is "it did not work, here is
// why". Mapping exit codes onto status codes would force this surface to
// re-decide what the engine already decided, which is the one thing Stage 5
// must not do.
func (server *Server) execute(writer http.ResponseWriter, request *http.Request) {
	var decoded app.Request
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBytes))
	// Unknown fields are refused rather than ignored. A client sending
	// force_resume to a server that does not know the field would otherwise
	// believe it had asked for something it had not.
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&decoded); err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": "malformed request: " + err.Error(),
		})
		return
	}
	if decoded.Command == "" {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": "request has no command",
		})
		return
	}
	outcome := app.Execute(request.Context(), decoded)
	writeJSON(writer, http.StatusOK, outcome)
}

// commands reports the command registry, so a front end can build its own
// command list from the same source the CLI uses rather than hard-coding one
// that drifts.
func (server *Server) commands(writer http.ResponseWriter, request *http.Request) {
	type command struct {
		Name    string   `json:"name"`
		Aliases []string `json:"aliases,omitempty"`
		WebUI   string   `json:"webui"`
	}
	listed := make([]command, 0, len(contract.Commands))
	for _, registered := range contract.Commands {
		listed = append(listed, command{
			Name:    registered.Name,
			Aliases: registered.Aliases,
			WebUI:   string(registered.WebUI),
		})
	}
	writeJSON(writer, http.StatusOK, listed)
}

// placeholder stands in for the console until it exists. It is authenticated
// like everything else so the auth path is exercised from the first request an
// operator makes.
func (server *Server) placeholder(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(writer, request)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = writer.Write([]byte(
		"dmtx is serving.\n\n" +
			"The console is not built yet. The API is at POST /api/v1/execute.\n",
	))
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	// Browsers must not sniff a JSON body as HTML; without this a reflected
	// error string could be rendered as markup.
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
