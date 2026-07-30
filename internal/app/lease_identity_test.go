package app

import (
	"os"
	"path/filepath"
	"runtime"
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

func TestSQLiteWorkloadIdentityCanonicalizesAliasesAndHardlinks(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "target.db")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "target-hardlink.db")
	if err := os.Link(target, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}

	identity, err := endpointWorkloadIdentity(
		config.Endpoint{Type: "sqlite", Database: target},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{
		filepath.Join(directory, ".", "target.db"),
		hardlink,
	} {
		aliasIdentity, err := endpointWorkloadIdentity(
			config.Endpoint{Type: "sqlite3", Database: alias},
		)
		if err != nil {
			t.Fatal(err)
		}
		if aliasIdentity != identity {
			t.Fatalf("identity = %q, want %q", aliasIdentity, identity)
		}
	}
}

func TestSQLiteLeaseIdentityRecognizesHardlinkAfterSingleLinkPath(t *testing.T) {
	directory := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", filepath.Join(directory, "cache"))
	target := filepath.Join(directory, "target.db")
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}

	beforeIdentity, beforePath, err := targetLeaseLocation(
		config.Endpoint{Type: "sqlite", Database: target},
	)
	if err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "target-hardlink.db")
	if err := os.Link(target, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	for _, alias := range []string{target, hardlink} {
		afterIdentity, afterPath, err := targetLeaseLocation(
			config.Endpoint{Type: "sqlite", Database: alias},
		)
		if err != nil {
			t.Fatal(err)
		}
		equivalent, err := equivalentSQLiteLeaseIdentity(
			beforeIdentity,
			afterIdentity,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !equivalent || afterPath != beforePath {
			t.Fatalf(
				"hardlink identity is not safely related: (%q, %q), want lease path %q equivalent to %q",
				afterIdentity,
				afterPath,
				beforePath,
				beforeIdentity,
			)
		}
	}
}

func TestMissingSQLiteLeaseIdentityCaseFoldsOnDarwin(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin case-alias safety")
	}
	directory := t.TempDir()
	lower := filepath.Join(directory, "target.db")
	upper := filepath.Join(directory, "TARGET.DB")

	lowerIdentity, lowerPath, err := targetLeaseLocation(
		config.Endpoint{Type: "sqlite", Database: lower},
	)
	if err != nil {
		t.Fatal(err)
	}
	upperIdentity, upperPath, err := targetLeaseLocation(
		config.Endpoint{Type: "sqlite", Database: upper},
	)
	if err != nil {
		t.Fatal(err)
	}
	equivalent, err := equivalentSQLiteLeaseIdentity(
		lowerIdentity,
		upperIdentity,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !equivalent || lowerPath != upperPath {
		t.Fatalf(
			"missing target case aliases are not safely related: (%q, %q), (%q, %q)",
			lowerIdentity,
			lowerPath,
			upperIdentity,
			upperPath,
		)
	}
}

func TestSQLiteLeaseRejectsHardlinkAfterMissingTargetBecomesAFile(
	t *testing.T,
) {
	directory := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", filepath.Join(directory, "cache"))
	target := filepath.Join(directory, "target.db")

	store, lease, err := acquireTargetLease(
		config.Endpoint{Type: "sqlite", Database: target},
		"first",
	)
	if err != nil {
		t.Fatal(err)
	}
	defer store.ReleaseLease(lease)
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("lease acquisition mutated target: %v", err)
	}
	if err := os.WriteFile(target, []byte("target"), 0o600); err != nil {
		t.Fatal(err)
	}
	hardlink := filepath.Join(directory, "target-hardlink.db")
	if err := os.Link(target, hardlink); err != nil {
		t.Skipf("hardlinks unavailable: %v", err)
	}
	if _, _, err := acquireTargetLease(
		config.Endpoint{Type: "sqlite", Database: hardlink},
		"second",
	); err == nil {
		t.Fatal("hardlink alias acquired a second live target lease")
	}
}
