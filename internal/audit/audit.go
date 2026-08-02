// Package audit writes an append-only, hash-linked migration audit stream.
package audit

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Event is one durable operator-visible migration fact.
type Event struct {
	At       time.Time       `json:"at"`
	RunID    string          `json:"run_id"`
	Type     string          `json:"type"`
	Previous string          `json:"previous_hash,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Hash     string          `json:"hash"`
}

// Append records an event after linking it to the stream's previous event.
func Append(path, runID, eventType string, at time.Time) error {
	return appendEvent(path, runID, eventType, nil, at)
}

// AppendPayload records a typed JSON payload as part of the hash-linked event.
// Payload-free Append events retain the original wire format and hash formula.
func AppendPayload(
	path string,
	runID string,
	eventType string,
	payload any,
	at time.Time,
) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode audit event payload: %w", err)
	}
	return appendEvent(
		path,
		runID,
		eventType,
		json.RawMessage(encoded),
		at,
	)
}

func appendEvent(
	path string,
	runID string,
	eventType string,
	payload json.RawMessage,
	at time.Time,
) error {
	if path == "" {
		return errors.New("audit path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create audit directory: %w", err)
	}
	previous, err := lastHash(path)
	if err != nil {
		return err
	}
	event := Event{
		At:       at.UTC(),
		RunID:    runID,
		Type:     eventType,
		Previous: previous,
		Payload:  payload,
	}
	event.Hash = eventHash(event)
	encoded, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode audit event: %w", err)
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open audit stream: %w", err)
	}
	defer file.Close()
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write audit event: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync audit event: %w", err)
	}
	return nil
}

// HasEvent reports whether a verified audit stream contains an event for the
// supplied run. A malformed or broken stream is an error rather than evidence
// that terminal audit is absent.
func HasEvent(path, runID, eventType string) (bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return false, fmt.Errorf("read audit stream: %w", err)
	}
	defer file.Close()

	found := false
	previous := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return false, fmt.Errorf("decode audit stream: %w", err)
		}
		if event.Previous != previous {
			return false, errors.New("audit stream chain is broken")
		}
		if event.Hash != eventHash(event) {
			return false, errors.New("audit stream integrity check failed")
		}
		if event.RunID == runID && event.Type == eventType {
			found = true
		}
		previous = event.Hash
	}
	if err := scanner.Err(); err != nil {
		return false, fmt.Errorf("scan audit stream: %w", err)
	}
	return found, nil
}

func lastHash(path string) (string, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read audit stream: %w", err)
	}
	defer file.Close()
	previous := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event Event
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return "", fmt.Errorf("decode audit stream: %w", err)
		}
		if event.Previous != previous {
			return "", errors.New("audit stream chain is broken")
		}
		if event.Hash != eventHash(event) {
			return "", errors.New("audit stream integrity check failed")
		}
		previous = event.Hash
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan audit stream: %w", err)
	}
	return previous, nil
}

func eventHash(event Event) string {
	payload := event.At.UTC().Format(time.RFC3339Nano) + "\x00" + event.RunID + "\x00" + event.Type + "\x00" + event.Previous
	if event.Payload != nil {
		payload += "\x00payload-v1\x00" + string(event.Payload)
	}
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}
