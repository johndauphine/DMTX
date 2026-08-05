// Package secrets owns the file dmtx keeps material in that must not sit in a
// migration configuration.
//
// It is at ~/.secrets/dmtx/config.yaml. The ~/.secrets convention comes from
// DMT, so an operator moving between the tools finds it where they expect and
// their backup exclusions carry over; the per-tool subdirectory does not,
// because that convention predates several tools sharing the directory.
//
// Partitioning is what makes the protections enforceable. dmtx owns
// ~/.secrets/dmtx and can tighten it; it cannot tighten ~/.secrets without
// changing permissions on other tools' files. The rule throughout is: enforce
// what dmtx owns, report what it does not.
//
// It does mean dmtx keeps its own files in two places, since the serve state
// file lives in the platform config directory; the familiarity was judged worth
// the split.
//
// What this protects, and what it does not: mode 0600 and a refusal to load a
// looser file keep other accounts on the machine out. They do not protect
// against somebody holding the disk - that is full-disk encryption's job. The
// distinction matters because a store that implied more would invite putting
// things in it that deserve better.
package secrets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"gopkg.in/yaml.v3"
)

const (
	// sharedDirectory is where several tools keep their secrets. dmtx creates
	// it when it is missing but does not otherwise touch it: an operator with
	// other tools' files in there has made a choice dmtx is not party to.
	sharedDirectory = ".secrets"

	// appDirectory is dmtx's own, inside the shared one. Partitioning by tool
	// is what lets dmtx enforce anything at all here - a directory it owns can
	// be tightened, where the shared parent cannot be tightened on somebody
	// else's behalf.
	appDirectory = "dmtx"

	// fileName needs no prefix now that the directory names the tool.
	fileName = "config.yaml"

	fileMode      = 0o600
	directoryMode = 0o700
)

// ErrInsecurePermissions means the file is readable beyond its owner.
var ErrInsecurePermissions = errors.New("secret file permissions are too open")

// ErrAlreadyExists means Create found a file and would not replace it.
//
// Distinguishable because the caller answers it quite differently from a
// failure: one is a normal second run, the other is something wrong. Reporting
// an I/O error as "already exists" would send an operator looking at a file
// that may not be there.
var ErrAlreadyExists = errors.New("secret file already exists")

// ErrInsecureDirectory means the directory holding the file is listable by
// other accounts.
var ErrInsecureDirectory = errors.New("secrets directory permissions are too open")

// Config is what the file holds.
//
// Only Encryption is read by anything today, and only once profiles exist. The
// remaining sections are described in the template as not yet read, because a
// file that lists capabilities dmtx does not have is a file that promises them.
type Config struct {
	// Encryption holds the key profiles are sealed with. Losing it makes every
	// stored profile unrecoverable, which is why nothing here ever rewrites the
	// file wholesale.
	Encryption Encryption `yaml:"encryption"`
}

// Encryption is the profile-sealing key material.
type Encryption struct {
	MasterKey string `yaml:"master_key,omitempty"`
}

// Path is where the file lives: ~/.secrets/dmtx/config.yaml.
func Path() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home directory: %w", err)
	}
	return filepath.Join(home, sharedDirectory, appDirectory, fileName), nil
}

// Load reads the file, refusing one that other accounts can read.
//
// Refusing rather than warning: a warning about a credential file is a line an
// operator scrolls past, and the whole point of the file is that its contents
// are worth more than the inconvenience of a chmod.
func Load(path string) (Config, error) {
	if err := ValidatePermissions(path); err != nil {
		return Config{}, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read secrets: %w", err)
	}
	var value Config
	if err := yaml.Unmarshal(data, &value); err != nil {
		return Config{}, fmt.Errorf("parse secrets: %w", err)
	}
	return value, nil
}

// ValidateDirectoryPermissions reports whether dmtx's own secrets directory can
// be listed by other accounts.
//
// This is the directory dmtx owns, so a loose one is a fault rather than a
// choice - Create tightens it. The check exists for a directory changed
// afterwards.
func ValidateDirectoryPermissions(path string) error {
	return checkDirectory(filepath.Dir(path))
}

// ValidateSharedDirectoryPermissions reports whether the directory holding
// every tool's secrets can be listed by other accounts.
//
// Reported and never corrected, because dmtx does not own it. A listable
// ~/.secrets does not disclose any file's contents, but it does disclose which
// tools an operator has configured - worth telling them once, and not worth
// changing on their behalf when other tools' files are in there too.
func ValidateSharedDirectoryPermissions(path string) error {
	return checkDirectory(filepath.Dir(filepath.Dir(path)))
}

func checkDirectory(directory string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(directory)
	if err != nil {
		return fmt.Errorf("check secrets directory permissions: %w", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf(
			"%w: %s is %04o; run: chmod %03o %s",
			ErrInsecureDirectory, directory, mode, directoryMode, directory,
		)
	}
	return nil
}

// ValidatePermissions reports whether the file is readable beyond its owner.
//
// os.Stat rather than os.Lstat, so a symlink is judged by what it points at: a
// world-readable file reached through a private link is still world-readable.
func ValidatePermissions(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("check secret file permissions: %w", err)
	}
	if runtime.GOOS == "windows" {
		// POSIX mode bits do not represent Windows ACLs, so a check here would
		// report a reassuring answer it has not established.
		return nil
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf(
			"%w: %s is %04o; run: chmod %03o %s",
			ErrInsecurePermissions, path, mode, fileMode, path,
		)
	}
	return nil
}

// Create writes the starter file, and will not overwrite one.
//
// Overwriting matters more here than for a migration configuration: this file
// is the only copy of key material, and replacing it makes everything sealed
// with the old key unreadable. The caller passes force explicitly so that
// choice is never a default.
func Create(path string, force bool) error {
	switch _, err := os.Stat(path); {
	case err == nil:
		if !force {
			return fmt.Errorf("%w: %s", ErrAlreadyExists, path)
		}
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("check %s: %w", path, err)
	}

	// Two directories, answered differently, which is the whole reason for
	// partitioning by tool.
	//
	// The shared parent is created when missing and otherwise left exactly as
	// it is - an operator keeping several tools' secrets there has made a
	// choice dmtx is not party to, and tightening it would change permissions
	// on somebody else's data.
	//
	// dmtx's own directory inside it is enforced, because it is dmtx's. MkdirAll
	// applies its mode only when it creates, so an existing one is chmodded:
	// that is the third place in this codebase where a mode argument alone was
	// not enough, and here it can simply be fixed rather than reported.
	own := filepath.Dir(path)
	if err := os.MkdirAll(own, directoryMode); err != nil {
		return fmt.Errorf("create secrets directory: %w", err)
	}
	if err := os.Chmod(own, directoryMode); err != nil {
		return fmt.Errorf("restrict secrets directory: %w", err)
	}
	// A mode argument applies only when a file is created, so an existing file
	// replaced with --force would keep whatever mode it had. Chmod covers that;
	// the same defect has now been found twice elsewhere in this codebase.
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return fmt.Errorf("create secrets: %w", err)
	}
	if err := file.Chmod(fileMode); err != nil {
		_ = file.Close()
		return fmt.Errorf("restrict secrets: %w", err)
	}
	if _, err := file.WriteString(Template); err != nil {
		_ = file.Close()
		return fmt.Errorf("write secrets: %w", err)
	}
	return file.Close()
}

// Template is the file Create writes.
//
// Every section says plainly that nothing reads it yet. That sentence is the
// condition on shipping this before its consumers exist: a file listing
// capabilities dmtx does not have would promise them, and an operator who put
// an API key here expecting it to be used would be wrong in a way that is their
// time to discover.
const Template = `# dmtx secrets.
#
# Keep this file out of version control and out of your migration
# configuration. It is created 0600 and dmtx refuses to read it if that
# changes, which keeps other accounts on this machine out.
#
# It does not protect the file from somebody holding the disk. For that you
# want full-disk encryption; these permissions are not a substitute for it.
#
# NOTHING IN DMTX READS ANY OF THIS YET. The sections below are here so the
# file and its protections exist before anything depends on them. Each says
# what will read it, and when.

# Read by: profile storage, once profiles exist.
# Sealing key for stored connection profiles. Losing it makes every stored
# profile unrecoverable, so back it up somewhere you would back up a password.
# Leave it empty and dmtx will generate one the first time it seals a profile.
encryption:
  master_key: ""

# Read by: nothing yet. AI advisories are not built.
# ai:
#   api_key: ""

# Read by: nothing yet. Notifications are not built.
# notifications:
#   slack:
#     webhook_url: ""
`
