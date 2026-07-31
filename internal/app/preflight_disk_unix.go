//go:build !windows

package app

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func sqliteTargetFreeBytes(path string) (uint64, bool) {
	location := path
	if _, err := os.Stat(location); os.IsNotExist(err) {
		location = filepath.Dir(location)
	}
	var statistics unix.Statfs_t
	if err := unix.Statfs(location, &statistics); err != nil {
		return 0, false
	}
	return uint64(statistics.Bavail) * uint64(statistics.Bsize), true
}

func sqliteTargetParentWriteAccess(path string) (bool, bool) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsPermission(err) || os.IsNotExist(err) {
			return false, true
		}
		return false, false
	}
	if !info.IsDir() {
		return false, true
	}
	return unix.Access(path, unix.W_OK|unix.X_OK) == nil, true
}
