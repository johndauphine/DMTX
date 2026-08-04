//go:build windows

package api

import "errors"

// makeFIFO has no Windows equivalent worth building for a test; the caller
// skips when this fails.
func makeFIFO(string) error {
	return errors.New("no mkfifo on windows")
}
