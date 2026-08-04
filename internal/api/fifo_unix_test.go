//go:build !windows

package api

import "syscall"

// makeFIFO creates a named pipe, which completion must never offer: reading one
// blocks until something else writes to it.
func makeFIFO(path string) error {
	return syscall.Mkfifo(path, 0o600)
}
