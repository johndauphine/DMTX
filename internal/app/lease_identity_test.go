package app

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/johndauphine/dmtx/internal/config"
)

func TestSQLiteLeaseIdentityCanonicalizesAliasesAndHardlinks(t *testing.T) {
	directory := t.TempDir()
	cache := filepath.Join(directory, "cache")
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("LOCALAPPDATA", cache)

	target := filepath.Join(directory, "target.db")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "target-hardlink.db")
	if err := os.Link(target, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}

	identity, storePath, err := targetLeaseLocation(config.Endpoint{Type: "sqlite", Database: target})
	if err != nil {
		t.Fatal(err)
	}
	for name, alias := range map[string]string{
		"lexical":  filepath.Join(directory, ".", "target.db"),
		"hardlink": hardlink,
	} {
		t.Run(name, func(t *testing.T) {
			aliasIdentity, aliasStorePath, err := targetLeaseLocation(config.Endpoint{Type: "sqlite", Database: alias})
			if err != nil {
				t.Fatal(err)
			}
			if aliasIdentity != identity || aliasStorePath != storePath {
				t.Fatalf("identity/path = (%q, %q), want (%q, %q)", aliasIdentity, aliasStorePath, identity, storePath)
			}
		})
	}

	symlink := filepath.Join(directory, "target-symlink.db")
	if err := os.Symlink(target, symlink); err == nil {
		aliasIdentity, aliasStorePath, err := targetLeaseLocation(config.Endpoint{Type: "sqlite", Database: symlink})
		if err != nil {
			t.Fatal(err)
		}
		if aliasIdentity != identity || aliasStorePath != storePath {
			t.Fatalf("symlink identity/path = (%q, %q), want (%q, %q)", aliasIdentity, aliasStorePath, identity, storePath)
		}
	}
}
