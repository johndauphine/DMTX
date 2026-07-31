//go:build !windows

package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteTargetParentWriteAccessRequiresSearchPermission(
	t *testing.T,
) {
	parent := filepath.Join(t.TempDir(), "no-search")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o200); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(parent, 0o700); err != nil {
			t.Errorf("restore target parent permissions: %v", err)
		}
	})

	writable, known := sqliteTargetParentWriteAccess(parent)
	if !known || writable {
		t.Fatalf(
			"write-only parent evidence = writable:%t known:%t",
			writable,
			known,
		)
	}
}
