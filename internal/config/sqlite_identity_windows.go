//go:build windows

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
	file, err := os.Open(canonical)
	if os.IsNotExist(err) {
		return "path:" + strings.ToLower(canonical), nil
	}
	if err != nil {
		return "", err
	}
	defer file.Close()
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(
		syscall.Handle(file.Fd()),
		&info,
	); err != nil {
		return "", err
	}
	if info.NumberOfLinks > 1 {
		return fmt.Sprintf(
			"file:%08x:%08x%08x",
			info.VolumeSerialNumber,
			info.FileIndexHigh,
			info.FileIndexLow,
		), nil
	}
	return "path:" + strings.ToLower(canonical), nil
}
