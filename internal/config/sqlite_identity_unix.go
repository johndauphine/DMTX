//go:build !windows

package config

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

func canonicalSQLiteHashIdentity(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	canonical, err := CanonicalSQLitePath(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if os.IsNotExist(err) {
		return "path:" + canonical, nil
	}
	if err != nil {
		return "", err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return "", fmt.Errorf("read SQLite file identity")
	}
	if stat.Nlink > 1 {
		return fmt.Sprintf("file:%d:%d", stat.Dev, stat.Ino), nil
	}
	return "path:" + canonical, nil
}
