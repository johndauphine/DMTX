//go:build windows

package state

import (
	"os"

	"golang.org/x/sys/windows"
)

func lockStateFile(file *os.File, exclusive bool) error {
	var flags uint32
	if exclusive {
		flags = windows.LOCKFILE_EXCLUSIVE_LOCK
	}
	return windows.LockFileEx(windows.Handle(file.Fd()), flags, 0, 1, 0, new(windows.Overlapped))
}

func unlockStateFile(file *os.File) error {
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, new(windows.Overlapped))
}
