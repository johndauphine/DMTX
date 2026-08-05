package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/johndauphine/dmtx/internal/app"
)

// Session keys. A closed set, not a key-value store.
//
// DMT refuses an unknown key rather than storing it, and that is worth
// carrying: a console free to write anything would accumulate keys nothing
// reads, and an operator who mistyped one would see it accepted and then
// ignored.
const (
	SessionConfig = "config"
	SessionState  = "state"
)

// errUnknownSessionKey is returned for a key dmtx does not have.
var errUnknownSessionKey = errors.New("unknown session key")

// sessionKeys describes what each key defaults, for a console building its own
// settings list rather than hard-coding one that drifts.
var sessionKeys = map[string]string{
	SessionConfig: "configuration file used when a command does not name one",
	SessionState:  "state database used when a command does not name one",
}

// sessionDefaults remembers what an operator would otherwise retype.
//
// Held on disk rather than in memory, which is where DMT holds them. The reason
// is particular to dmtx: this server stops itself when idle, so an in-memory
// default would evaporate over a lunch break, and a second invocation handing
// off to a running instance would not carry them either. Neither would say so.
type sessionDefaults struct {
	mutex  sync.Mutex
	path   string
	values map[string]string
}

// newSessionDefaults loads what was set before, if anything.
//
// A file that cannot be read is treated as no defaults rather than as an error:
// these are a convenience, and refusing to serve a console because a
// convenience file is damaged would be the wrong trade.
func newSessionDefaults(path string) *sessionDefaults {
	defaults := &sessionDefaults{path: path, values: map[string]string{}}
	raw, err := os.ReadFile(path)
	if err != nil {
		return defaults
	}
	var stored map[string]string
	if err := json.Unmarshal(raw, &stored); err != nil {
		return defaults
	}
	for key, value := range stored {
		// Keys are re-checked on load. A file written by a later dmtx, or
		// edited by hand, must not reintroduce a key this one does not honour -
		// it would sit there looking effective.
		if _, known := sessionKeys[key]; known && value != "" {
			defaults.values[key] = value
		}
	}
	return defaults
}

// get returns a default, or empty.
func (defaults *sessionDefaults) get(key string) string {
	defaults.mutex.Lock()
	defer defaults.mutex.Unlock()
	return defaults.values[key]
}

// all returns every default that is set.
func (defaults *sessionDefaults) all() map[string]string {
	defaults.mutex.Lock()
	defer defaults.mutex.Unlock()
	copied := make(map[string]string, len(defaults.values))
	for key, value := range defaults.values {
		copied[key] = value
	}
	return copied
}

// set records a default, refusing a key dmtx does not have.
func (defaults *sessionDefaults) set(key, value string) error {
	if _, known := sessionKeys[key]; !known {
		return errUnknownSessionKey
	}
	defaults.mutex.Lock()
	defer defaults.mutex.Unlock()
	if value == "" {
		delete(defaults.values, key)
	} else {
		defaults.values[key] = value
	}
	return defaults.persistLocked()
}

// clear forgets a default.
func (defaults *sessionDefaults) clear(key string) error {
	if _, known := sessionKeys[key]; !known {
		return errUnknownSessionKey
	}
	defaults.mutex.Lock()
	defer defaults.mutex.Unlock()
	delete(defaults.values, key)
	return defaults.persistLocked()
}

// persistLocked writes the defaults. The caller holds the lock.
//
// Written to a new file and renamed. That is what guarantees the mode: a mode
// argument applies only when a file is created, so writing over an existing one
// would keep whatever mode it had, while a file that cannot already exist has
// nothing to inherit. The rename also means a reader sees one set of defaults
// or the other, never half of a write.
//
// The Chmod below is belt and braces rather than the guarantee - os.CreateTemp
// already creates at 0600 - and it is kept because that is a promise of a
// standard library call rather than of this code. Nothing tests it, because
// nothing can: removing it changes no observable behaviour.
func (defaults *sessionDefaults) persistLocked() error {
	if defaults.path == "" {
		// In memory only, which is what a caller that is not the serve command
		// gets. Nothing to write, and no error either: the Options field says
		// empty means memory, so failing here would make the comment a lie.
		return nil
	}
	if len(defaults.values) == 0 {
		if err := os.Remove(defaults.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(defaults.path), 0o700); err != nil {
		return err
	}
	encoded, err := json.Marshal(defaults.values)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(defaults.path), "session-*.json")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temporary.Name()) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(temporary.Name(), defaults.path)
}

// applyTo fills in what a request did not say.
//
// Explicit wins, then the session default, then whatever the command's own
// default is - the precedence DMT's resolveOrigin uses. Applied here in the API
// and nowhere else: the command line deliberately does not consult these, so a
// destructive command always says what it will act on.
//
// This is request construction, not execution, which is what keeps the parity
// criterion intact. The command line builds a Request from argv and the console
// builds one from its input and these defaults; both then hand an identical
// Request to an identical Execute. Parity is a claim about the seam, and filling
// a field before reaching it is the same kind of act as parseRequest resolving
// an alias or supplying Latest.
func (defaults *sessionDefaults) applyTo(request app.Request) app.Request {
	if request.ConfigPath == "" {
		request.ConfigPath = defaults.get(SessionConfig)
	}
	if request.StatePath == "" {
		request.StatePath = defaults.get(SessionState)
	}
	return request
}

// session reports the defaults and the keys that may be set.
func (server *Server) session(writer http.ResponseWriter, request *http.Request) {
	type describedKey struct {
		Key         string `json:"key"`
		Description string `json:"description"`
		Value       string `json:"value,omitempty"`
	}
	set := server.defaults.all()
	names := make([]string, 0, len(sessionKeys))
	for key := range sessionKeys {
		names = append(names, key)
	}
	sort.Strings(names)

	described := make([]describedKey, 0, len(names))
	for _, key := range names {
		described = append(described, describedKey{
			Key:         key,
			Description: sessionKeys[key],
			Value:       set[key],
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{"defaults": described})
}

// setSession records one default.
func (server *Server) setSession(writer http.ResponseWriter, request *http.Request) {
	var asked struct {
		Key   string `json:"key"`
		Value string `json:"value"`
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
	if err := server.defaults.set(asked.Key, asked.Value); err != nil {
		server.refuseSessionKey(writer, asked.Key, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"key": asked.Key})
}

// clearSession forgets one default.
func (server *Server) clearSession(writer http.ResponseWriter, request *http.Request) {
	key := request.PathValue("key")
	if err := server.defaults.clear(key); err != nil {
		server.refuseSessionKey(writer, key, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"key": key})
}

// refuseSessionKey answers a key dmtx does not have, naming the ones it does.
//
// Naming them matters: a console or a script that guessed a key gets told what
// to guess instead, rather than a bare refusal it has to go and read source to
// resolve.
func (server *Server) refuseSessionKey(writer http.ResponseWriter, key string, err error) {
	if !errors.Is(err, errUnknownSessionKey) {
		writeJSON(writer, http.StatusInternalServerError, map[string]string{
			"error": "could not record the default: " + err.Error(),
		})
		return
	}
	known := make([]string, 0, len(sessionKeys))
	for name := range sessionKeys {
		known = append(known, name)
	}
	sort.Strings(known)
	writeJSON(writer, http.StatusNotFound, map[string]any{
		"error": "unknown session key " + key,
		"known": known,
	})
}
