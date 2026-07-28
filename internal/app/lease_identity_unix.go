//go:build !windows

package app

import (
	"fmt"
	"os"
	"syscall"
)

func sqliteLeaseFileIdentity(path string) (identity string, multipleLinks bool, err error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return "path:" + path, false, nil
	}
	if err != nil {
		return "", false, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", false, fmt.Errorf("read SQLite file identity")
	}
	return fmt.Sprintf("file:%d:%d", stat.Dev, stat.Ino), stat.Nlink > 1, nil
}
