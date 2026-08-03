package api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// instanceState is what a running server records so a second invocation can
// find it instead of starting a rival.
//
// The secret is not a bearer token and is never sent anywhere; see proof.
type instanceState struct {
	Port   int    `json:"port"`
	Secret string `json:"secret"`
}

// Labels keep the two directions of the handshake from being interchangeable.
// Without them a reply could be replayed as a request, and each side would
// accept its own challenge back as proof the other side had answered it.
const (
	clientProofLabel = "dmtx-handoff-client:"
	serverProofLabel = "dmtx-handoff-server:"
)

// handoffTimeout bounds the probe. A running instance on loopback answers
// immediately; anything slower is something else holding the port, and waiting
// on it would just delay starting the server the operator asked for.
const handoffTimeout = 3 * time.Second

// proof is one side's evidence that it holds the secret.
//
// The secret itself never crosses the connection. That matters because the
// port recorded in the state file may not be ours by the time we probe it: if
// this instance died, anyone - including another user on the machine - can bind
// the port it used. Sending a bearer token would hand a console credential to
// whoever answered. Instead both sides demonstrate knowledge of the secret over
// a nonce, so an impostor learns nothing and cannot prove anything.
func proof(secret, label, nonce string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(label + nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

// handoffRequest and handoffReply are the two messages of the handshake.
type handoffRequest struct {
	Nonce string `json:"nonce"`
	Proof string `json:"proof"`
}

type handoffReply struct {
	Proof string `json:"proof"`
	Token string `json:"token"`
}

// statePath is a variable so tests can point the handoff machinery somewhere
// temporary instead of writing to the operator's real config directory. Tests
// that replace it must not run in parallel with each other.
var statePath = defaultStatePath

// defaultStatePath is where a running instance records itself.
func defaultStatePath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("locate config directory: %w", err)
	}
	return filepath.Join(base, "dmtx", "serve.json"), nil
}

// writeInstanceState records this server so a second invocation can find it.
//
// The file ends up 0600. The directory is created 0700 when it is missing; an
// existing one is left as the operator has it, because tightening a directory
// this tool did not create is not its decision to make. Neither is what
// protects the secret - the handshake is - but a credential file readable by
// other accounts is a bad habit regardless.
func writeInstanceState(path string, state instanceState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode instance state: %w", err)
	}

	// Written to a new file and renamed into place, for two reasons.
	//
	// A mode argument applies only when a file is created, so writing straight
	// over an existing serve.json would keep whatever mode that file already
	// had - 0600 would be a hope, not a guarantee. A file that cannot already
	// exist cannot inherit anything.
	//
	// The rename is also atomic, so a second invocation reading at the same
	// moment sees the old record or the new one, never half of one.
	temporary, err := os.CreateTemp(filepath.Dir(path), "serve-*.json")
	if err != nil {
		return fmt.Errorf("create instance state: %w", err)
	}
	defer func() { _ = os.Remove(temporary.Name()) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("restrict instance state: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write instance state: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("write instance state: %w", err)
	}
	if err := os.Rename(temporary.Name(), path); err != nil {
		return fmt.Errorf("install instance state: %w", err)
	}
	return nil
}

// recordInstance writes this server's location and handoff secret.
func (server *Server) recordInstance(path string) error {
	address, ok := server.listener.Addr().(*net.TCPAddr)
	if !ok {
		return errors.New("listener is not a TCP address")
	}
	return writeInstanceState(path, instanceState{
		Port:   address.Port,
		Secret: server.handoffSecret,
	})
}

// forgetInstance removes this server's record, and only this server's.
//
// The check is not pedantry. A second server started with --new-instance, or
// two starting at once, can leave a record written by someone else; removing it
// on the way out would strand a server that is still running and still serving,
// so the next invocation would start a third rather than find the first.
func (server *Server) forgetInstance(path string) {
	recorded, found := readInstanceState(path)
	if !found {
		return
	}
	address, ok := server.listener.Addr().(*net.TCPAddr)
	if !ok || recorded.Port != address.Port ||
		!constantTimeEqual(recorded.Secret, server.handoffSecret) {
		return
	}
	_ = os.Remove(path)
}

// readInstanceState reports a recorded instance, if the file names a usable
// one. A missing, unreadable, or malformed file simply means there is nobody to
// hand off to, which is not an error worth reporting to an operator who only
// asked to start a server.
func readInstanceState(path string) (instanceState, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return instanceState{}, false
	}
	var state instanceState
	if err := json.Unmarshal(raw, &state); err != nil {
		return instanceState{}, false
	}
	if state.Port < 1 || state.Port > 65535 || state.Secret == "" {
		return instanceState{}, false
	}
	return state, true
}

// handOff asks a recorded instance for a fresh launch URL.
//
// It returns false whenever there is no usable instance, which is the ordinary
// case: no file, a stale one, or something on the port that cannot prove it is
// dmtx. Every one of those means "start a server", so none of them is reported
// as a failure.
//
// wantPort is the port the operator asked for, or zero if they did not care. An
// explicit port that disagrees with the running instance is a request for a
// different server, not a request to be sent to this one.
func handOff(path string, wantPort int) (string, bool) {
	state, found := readInstanceState(path)
	if !found {
		return "", false
	}
	if wantPort != 0 && wantPort != state.Port {
		return "", false
	}

	nonce, err := newToken()
	if err != nil {
		return "", false
	}
	body, err := json.Marshal(handoffRequest{
		Nonce: nonce,
		Proof: proof(state.Secret, clientProofLabel, nonce),
	})
	if err != nil {
		return "", false
	}

	client := &http.Client{Timeout: handoffTimeout}
	response, err := client.Post(
		fmt.Sprintf("http://127.0.0.1:%d/api/v1/handoff", state.Port),
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		// Nothing is listening. The record is stale, so clear it rather than
		// probing a dead port on every future start.
		_ = os.Remove(path)
		return "", false
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", false
	}

	var reply handoffReply
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxRequestBytes))
	if err := decoder.Decode(&reply); err != nil {
		return "", false
	}
	// Checked before the operator is sent anywhere. Whatever is on that port
	// has to prove it holds the secret, or it does not get a browser opened at
	// it - an impostor could otherwise serve a convincing console and collect
	// whatever was typed into it.
	if !constantTimeEqual(reply.Proof, proof(state.Secret, serverProofLabel, nonce)) {
		return "", false
	}
	if reply.Token == "" {
		return "", false
	}
	return loginURL(state.Port, reply.Token), true
}

// handoff answers a second invocation with a fresh launch token.
//
// The route is deliberately outside the session check: a separate process has
// no cookie and no bearer token, and the proof it carries is what authenticates
// it. It is the only unauthenticated write on the server, which is why it does
// nothing except mint a launch token for a caller that already demonstrated it
// holds the secret.
func (server *Server) handoff(writer http.ResponseWriter, request *http.Request) {
	var asked handoffRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&asked); err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": "malformed handoff request",
		})
		return
	}
	// A short nonce would let a caller steer the handshake onto a value it had
	// seen answered before.
	if len(asked.Nonce) < 32 {
		writeJSON(writer, http.StatusBadRequest, map[string]string{
			"error": "handoff nonce is too short",
		})
		return
	}
	if !constantTimeEqual(
		asked.Proof,
		proof(server.handoffSecret, clientProofLabel, asked.Nonce),
	) {
		writeJSON(writer, http.StatusUnauthorized, map[string]string{
			"error": "handoff proof rejected",
		})
		return
	}

	token, err := server.auth.remint()
	if err != nil {
		writeJSON(writer, http.StatusInternalServerError, map[string]string{
			"error": "could not mint a launch token",
		})
		return
	}
	writeJSON(writer, http.StatusOK, handoffReply{
		Proof: proof(server.handoffSecret, serverProofLabel, asked.Nonce),
		Token: token,
	})
}
