package migrate

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/johndauphine/dmtx/internal/config"
)

// dryRunSQLiteArtifact records every artifact a SQLite connection can affect
// while it establishes a read snapshot. A mode=ro connection may still update
// a shared-memory WAL index with some drivers, so dry-run never relies on the
// URI alone to uphold its no-artifact guarantee.
type dryRunSQLiteArtifact struct {
	exists   bool
	size     int64
	modified int64
	identity os.FileInfo
	digest   [sha256.Size]byte
}

// snapshotDryRunSQLiteArtifacts captures both the main database and every
// sidecar whose existence or bytes are part of the endpoint's authority. The
// caller uses a second capture after closing the private snapshot to prove the
// real endpoint stayed untouched.
func snapshotDryRunSQLiteArtifacts(
	path string,
) (map[string]dryRunSQLiteArtifact, error) {
	artifacts := make(map[string]dryRunSQLiteArtifact, 5)
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		artifactPath := path + suffix
		info, err := os.Stat(artifactPath)
		if errors.Is(err, os.ErrNotExist) {
			artifacts[suffix] = dryRunSQLiteArtifact{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf(
				"inspect SQLite dry-run %s artifact: %w",
				suffix,
				err,
			)
		}
		digest, err := digestDryRunSQLiteArtifact(artifactPath, info)
		if err != nil {
			return nil, err
		}
		artifacts[suffix] = dryRunSQLiteArtifact{
			exists:   true,
			size:     info.Size(),
			modified: info.ModTime().UnixNano(),
			identity: info,
			digest:   digest,
		}
	}
	return artifacts, nil
}

func verifyDryRunSQLiteArtifacts(
	before, after map[string]dryRunSQLiteArtifact,
) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal", ".lock"} {
		left, right := before[suffix], after[suffix]
		if left.exists != right.exists || left.size != right.size ||
			left.modified != right.modified || left.digest != right.digest ||
			(left.exists && !os.SameFile(left.identity, right.identity)) {
			return fmt.Errorf("SQLite dry-run %s artifact changed", suffix)
		}
	}
	return nil
}

func digestDryRunSQLiteArtifact(
	path string,
	expected os.FileInfo,
) ([sha256.Size]byte, error) {
	input, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf(
			"open SQLite dry-run artifact: %w",
			err,
		)
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf(
			"inspect opened SQLite dry-run artifact: %w",
			err,
		)
	}
	if !os.SameFile(expected, opened) || expected.Size() != opened.Size() ||
		expected.ModTime() != opened.ModTime() {
		return [sha256.Size]byte{}, errors.New(
			"SQLite dry-run artifact changed while snapshotting",
		)
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, input); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf(
			"digest SQLite dry-run artifact: %w",
			err,
		)
	}
	current, err := os.Stat(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf(
			"reinspect SQLite dry-run artifact: %w",
			err,
		)
	}
	if !os.SameFile(opened, current) || opened.Size() != current.Size() ||
		opened.ModTime() != current.ModTime() {
		return [sha256.Size]byte{}, errors.New(
			"SQLite dry-run artifact changed while snapshotting",
		)
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result, nil
}

// dryRunSQLiteEndpointSnapshots returns an equivalent configuration whose
// selected SQLite endpoints point at private snapshots. Call the returned
// cleanup only after all adapters using the cloned paths are closed. Cleanup
// verifies the original database, WAL, shared-memory index, rollback journal,
// and lock artifact before deleting the private copies; a concurrent writer or
// an accidental original-path open therefore fails closed.
func dryRunSQLiteEndpointSnapshots(
	cfg config.Config,
	snapshotSource bool,
	snapshotTarget bool,
) (config.Config, func() error, error) {
	result := cfg
	cleanups := make([]func() error, 0, 2)

	capture := func(endpoint *config.Endpoint, role string) error {
		if !strings.EqualFold(strings.TrimSpace(endpoint.Type), "sqlite") {
			return nil
		}
		path, err := config.CanonicalSQLitePath(endpoint.Database)
		if err != nil {
			return fmt.Errorf("resolve SQLite dry-run %s path: %w", role, err)
		}
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("inspect SQLite dry-run %s: %w", role, err)
		}
		if info.IsDir() {
			return fmt.Errorf("SQLite dry-run %s path is a directory", role)
		}
		before, err := snapshotDryRunSQLiteArtifacts(path)
		if err != nil {
			return fmt.Errorf("snapshot SQLite dry-run %s artifacts: %w", role, err)
		}
		snapshot, remove, err := cloneDryRunSQLiteSnapshot(path, before, role)
		if err != nil {
			return err
		}
		endpoint.Database = snapshot
		cleanups = append(cleanups, func() error {
			after, artifactErr := snapshotDryRunSQLiteArtifacts(path)
			if artifactErr == nil {
				artifactErr = verifyDryRunSQLiteArtifacts(before, after)
			}
			if artifactErr != nil {
				artifactErr = fmt.Errorf(
					"verify SQLite dry-run %s artifacts: %w",
					role,
					artifactErr,
				)
			}
			return errors.Join(artifactErr, remove())
		})
		return nil
	}

	if snapshotSource {
		if err := capture(&result.Source, "source"); err != nil {
			return cfg, func() error { return nil },
				errors.Join(err, cleanDryRunSQLiteEndpointSnapshots(cleanups))
		}
	}
	if snapshotTarget {
		if err := capture(&result.Target, "target"); err != nil {
			return cfg, func() error { return nil },
				errors.Join(err, cleanDryRunSQLiteEndpointSnapshots(cleanups))
		}
	}
	return result, func() error {
		return cleanDryRunSQLiteEndpointSnapshots(cleanups)
	}, nil
}

func cleanDryRunSQLiteEndpointSnapshots(cleanups []func() error) error {
	var result error
	for index := len(cleanups) - 1; index >= 0; index-- {
		result = errors.Join(result, cleanups[index]())
	}
	return result
}

func cloneDryRunSQLiteSnapshot(
	path string,
	before map[string]dryRunSQLiteArtifact,
	role string,
) (string, func() error, error) {
	return cloneDryRunSQLiteSnapshotWithRemove(
		path,
		before,
		role,
		os.RemoveAll,
	)
}

func cloneDryRunSQLiteSnapshotWithRemove(
	path string,
	before map[string]dryRunSQLiteArtifact,
	role string,
	removeDirectory func(string) error,
) (string, func() error, error) {
	directory, err := os.MkdirTemp("", "dmtx-dryrun-sqlite-")
	if err != nil {
		return "", nil, fmt.Errorf(
			"create SQLite dry-run %s snapshot: %w",
			role,
			err,
		)
	}
	cleanup := func() error {
		if err := removeDirectory(directory); err != nil {
			return fmt.Errorf(
				"remove SQLite dry-run %s snapshot: %w",
				role,
				err,
			)
		}
		return nil
	}
	snapshot := filepath.Join(directory, role+".db")
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		artifact := before[suffix]
		if !artifact.exists {
			continue
		}
		if err := copyDryRunSQLiteArtifact(
			path+suffix,
			snapshot+suffix,
			artifact,
		); err != nil {
			return "", nil, errors.Join(err, cleanup())
		}
	}
	after, err := snapshotDryRunSQLiteArtifacts(path)
	if err != nil {
		return "", nil, errors.Join(err, cleanup())
	}
	if err := verifyDryRunSQLiteArtifacts(before, after); err != nil {
		return "", nil, errors.Join(
			fmt.Errorf(
				"SQLite dry-run %s changed while capturing read-only snapshot: %w",
				role,
				err,
			),
			cleanup(),
		)
	}
	return snapshot, cleanup, nil
}

func copyDryRunSQLiteArtifact(
	source string,
	target string,
	expected dryRunSQLiteArtifact,
) error {
	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("read SQLite dry-run snapshot artifact: %w", err)
	}
	defer input.Close()
	opened, err := input.Stat()
	if err != nil {
		return fmt.Errorf("inspect SQLite dry-run snapshot artifact: %w", err)
	}
	if !os.SameFile(expected.identity, opened) || expected.size != opened.Size() ||
		expected.modified != opened.ModTime().UnixNano() {
		return errors.New("SQLite dry-run artifact changed while copying snapshot")
	}
	output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create SQLite dry-run snapshot artifact: %w", err)
	}
	if _, err := io.Copy(output, input); err != nil {
		copyErr := fmt.Errorf("copy SQLite dry-run snapshot artifact: %w", err)
		if closeErr := output.Close(); closeErr != nil {
			return errors.Join(
				copyErr,
				fmt.Errorf("close SQLite dry-run snapshot artifact: %w", closeErr),
			)
		}
		return copyErr
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("close SQLite dry-run snapshot artifact: %w", err)
	}
	return nil
}
