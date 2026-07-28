//go:build !windows

package state

import "os"

func replaceStateFile(temporaryPath, destinationPath string) error {
	return os.Rename(temporaryPath, destinationPath)
}

func syncStateDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
