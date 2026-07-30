//go:build windows

package app

import (
	"fmt"
	"os"
	"strings"
	"syscall"
)

func sqliteLeaseFileIdentity(path string) (identity string, multipleLinks bool, err error) {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return "path:" + path, false, nil
	}
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(syscall.Handle(file.Fd()), &info); err != nil {
		return "", false, err
	}
	return fmt.Sprintf("file:%08x:%08x%08x", info.VolumeSerialNumber, info.FileIndexHigh, info.FileIndexLow), info.NumberOfLinks > 1, nil
}

func sqliteLeaseFoldedPath(path string) string {
	return strings.ToLower(path)
}
