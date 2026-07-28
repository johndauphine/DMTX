//go:build windows

package state

import "golang.org/x/sys/windows"

func replaceStateFile(temporaryPath, destinationPath string) error {
	temporary, err := windows.UTF16PtrFromString(temporaryPath)
	if err != nil {
		return err
	}
	destination, err := windows.UTF16PtrFromString(destinationPath)
	if err != nil {
		return err
	}
	return windows.MoveFileEx(
		temporary,
		destination,
		windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH,
	)
}

func syncStateDirectory(string) error {
	// MoveFileEx with MOVEFILE_WRITE_THROUGH flushes the replacement before
	// returning. Windows does not support opening directories with os.Open for
	// a portable directory fsync equivalent.
	return nil
}
