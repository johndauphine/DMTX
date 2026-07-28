//go:build !windows

package state

import (
	"os"

	"golang.org/x/sys/unix"
)

func lockStateFile(file *os.File, exclusive bool) error {
	operation := unix.LOCK_SH
	if exclusive {
		operation = unix.LOCK_EX
	}
	return unix.Flock(int(file.Fd()), operation)
}

func unlockStateFile(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_UN)
}
