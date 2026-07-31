//go:build windows

package app

func sqliteTargetFreeBytes(string) (uint64, bool) {
	return 0, false
}

func sqliteTargetParentWriteAccess(string) (bool, bool) {
	// Mode bits are not authoritative on Windows, and creating a probe file
	// would violate preflight's non-mutation contract.
	return false, false
}
