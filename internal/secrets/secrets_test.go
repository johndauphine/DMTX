package secrets

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// TestTheTemplateSaysNothingReadsItYet is the condition on shipping this store
// before its consumers exist.
//
// A file listing capabilities dmtx does not have promises them. An operator who
// put an API key in an "ai" section expecting it to be used would be wrong in a
// way that costs them their time to discover, so each section says what will
// read it and when.
func TestTheTemplateSaysNothingReadsItYet(t *testing.T) {
	if !strings.Contains(Template, "NOTHING IN DMTX READS ANY OF THIS YET") {
		t.Error("the template does not say that nothing reads it")
	}
	// Each section is checked against the line above it, not against the
	// template as a whole. Looking for "Read by: nothing yet" anywhere passed
	// while only one section carried it - a single note vouching for every
	// section, which is not what it says.
	lines := strings.Split(Template, "\n")
	for _, section := range []string{"ai:", "notifications:"} {
		found := false
		for index, line := range lines {
			if !strings.Contains(line, section) {
				continue
			}
			found = true
			if index == 0 || !strings.Contains(lines[index-1], "Read by:") {
				above := ""
				if index > 0 {
					above = lines[index-1]
				}
				t.Errorf(
					"the section %q is not preceded by a line saying what reads "+
						"it; the line above is %q",
					section, above,
				)
			}
		}
		if !found {
			t.Errorf("the template no longer mentions %q; this test is stale", section)
		}
	}

	// Every unbuilt section stays commented, so a parse cannot silently accept
	// a key nothing will use. Checked by walking the lines rather than naming
	// the sections twice, because the list I named by hand omitted
	// "notifications:" and would not have noticed it going live.
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") || trimmed == "" {
			continue
		}
		if !strings.HasPrefix(trimmed, "encryption:") &&
			!strings.HasPrefix(trimmed, "master_key:") {
			t.Errorf(
				"an unread section is uncommented and looks live: %q\n"+
					"only encryption is real; everything else must stay commented "+
					"until something reads it",
				line,
			)
		}
	}
}

// TestTheTemplateIsHonestAboutWhatItProtects pins that the file does not imply
// more safety than 0600 buys.
func TestTheTemplateIsHonestAboutWhatItProtects(t *testing.T) {
	if !strings.Contains(Template, "full-disk encryption") {
		t.Error(
			"the template does not say that permissions are not protection " +
				"against somebody holding the disk",
		)
	}
}

// TestCreateWritesAnOwnerOnlyFile pins the mode, which is the whole protection.
func TestCreateWritesAnOwnerOnlyFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	path := filepath.Join(t.TempDir(), ".secrets", fileName)
	if err := Create(path, false); err != nil {
		t.Fatalf("create: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the secrets file is %04o", mode)
	}
	directory, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	if mode := directory.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf(
			"the secrets directory is %04o; a listing discloses that the file exists",
			mode,
		)
	}
}

// TestCreateRefusesToDiscardKeyMaterial is the safety property, and it matters
// more here than for a configuration: this file is the only copy of a key, and
// replacing it makes everything sealed with the old one unreadable.
func TestCreateRefusesToDiscardKeyMaterial(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".secrets", fileName)
	if err := Create(path, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	const existing = "encryption:\n  master_key: \"the-only-copy\"\n"
	if err := os.WriteFile(path, []byte(existing), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := Create(path, false); err == nil {
		t.Fatal("create replaced a secrets file that held a key")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != existing {
		t.Errorf("the key material was modified despite the refusal:\n%s", after)
	}
}

// TestForceReplacesAndTightens pins the escape hatch, and that using it does
// not leave a loose mode behind.
func TestForceReplacesAndTightens(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not how Windows restricts a file")
	}
	directory := filepath.Join(t.TempDir(), ".secrets")
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, fileName)
	if err := os.WriteFile(path, []byte("old\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := Create(path, true); err != nil {
		t.Fatalf("create --force: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("replacing a world-readable secrets file left it %04o", mode)
	}
}

// TestLoadRefusesAFileOtherAccountsCanRead is the reason the mode is worth
// anything. Written 0600 and later loosened, the file must stop being readable
// by dmtx rather than being read with a warning nobody sees.
func TestLoadRefusesAFileOtherAccountsCanRead(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits do not represent Windows ACLs")
	}
	path := filepath.Join(t.TempDir(), fileName)
	if err := os.WriteFile(path, []byte("encryption:\n  master_key: \"k\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatalf("a correctly protected file did not load: %v", err)
	}

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Fatal("a world-readable secrets file was loaded")
	}
	if !errors.Is(err, ErrInsecurePermissions) {
		t.Errorf("the refusal is not identifiable as a permissions problem: %v", err)
	}
	if !strings.Contains(err.Error(), "chmod") {
		t.Errorf("the refusal does not say how to fix it: %v", err)
	}
}

// TestPermissionsAreJudgedByTheTargetOfASymlink pins that a private link to a
// world-readable file does not pass. What matters is who can read the bytes.
func TestPermissionsAreJudgedByTheTargetOfASymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits do not represent Windows ACLs")
	}
	directory := t.TempDir()
	target := filepath.Join(directory, "exposed.yaml")
	if err := os.WriteFile(target, []byte("encryption:\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(directory, fileName)
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("cannot create a symlink here: %v", err)
	}

	if err := ValidatePermissions(link); !errors.Is(err, ErrInsecurePermissions) {
		t.Errorf(
			"a link to a world-readable file passed the check: %v\n"+
				"the link's own mode is not what decides who can read the bytes",
			err,
		)
	}

	// The other direction, which is what actually distinguishes Stat from
	// Lstat here. A symlink's own mode is 0777, so Lstat rejects every link
	// whatever it points at - and the case above passes under both. A private
	// file reached through an ordinary link must load, or the check is
	// rejecting links rather than judging exposure.
	private := filepath.Join(directory, "private.yaml")
	if err := os.WriteFile(private, []byte("encryption:\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	toPrivate := filepath.Join(directory, "link-to-private.yaml")
	if err := os.Symlink(private, toPrivate); err != nil {
		t.Skipf("cannot create a symlink here: %v", err)
	}
	if err := ValidatePermissions(toPrivate); err != nil {
		t.Errorf(
			"a link to an owner-only file was refused: %v\n"+
				"the check is judging the link's own mode, not the target's",
			err,
		)
	}
}

// TestLoadReadsWhatCreateWrote closes the loop: the file dmtx writes is one
// dmtx can read.
func TestLoadReadsWhatCreateWrote(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".secrets", fileName)
	if err := Create(path, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("the file Create wrote does not load: %v", err)
	}
	// Empty, and that is the point: the template ships no key, and one is
	// generated when a profile is first sealed.
	if loaded.Encryption.MasterKey != "" {
		t.Errorf("the template ships a master key: %q", loaded.Encryption.MasterKey)
	}
}

// TestCreateDistinguishesAnExistingFileFromAFailure pins that a second run and
// something going wrong are told apart.
//
// The caller answers them quite differently, and reporting an I/O failure as
// "already exists" sends an operator to look at a file that may not be there.
func TestCreateDistinguishesAnExistingFileFromAFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".secrets", fileName)
	if err := Create(path, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := Create(path, false)
	if !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("a second create is not identifiable as already-exists: %v", err)
	}

	// A path that cannot be created at all must not look like a second run.
	// A file standing where the directory should be does it.
	blocked := filepath.Join(t.TempDir(), "in-the-way")
	if err := os.WriteFile(blocked, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err = Create(filepath.Join(blocked, fileName), false)
	if err == nil {
		t.Fatal("create succeeded through a file standing where a directory should be")
	}
	if errors.Is(err, ErrAlreadyExists) {
		t.Errorf("a creation failure is reported as already-exists: %v", err)
	}
}

// TestALooseDirectoryIsReportedNotCorrected pins the decision not to change
// permissions on a directory dmtx did not create.
//
// ~/.secrets is shared - DMT's own config lives beside this one - so tightening
// it would change permissions on somebody else's data to fix ours.
func TestALooseDirectoryIsReportedNotCorrected(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits do not represent Windows ACLs")
	}
	directory := filepath.Join(t.TempDir(), ".secrets")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, fileName)
	if err := Create(path, false); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The file is still owner-only whatever the directory is.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the file is %04o inside a loose directory", mode)
	}
	// The directory is left alone...
	before, err := os.Stat(directory)
	if err != nil {
		t.Fatal(err)
	}
	if before.Mode().Perm() != 0o755 {
		t.Errorf(
			"create changed the directory to %04o; it is shared with other "+
				"tools and is not dmtx's to tighten",
			before.Mode().Perm(),
		)
	}
	// ...and reported.
	if err := ValidateDirectoryPermissions(path); !errors.Is(err, ErrInsecureDirectory) {
		t.Errorf("a listable secrets directory was not reported: %v", err)
	}
}

// TestATightDirectoryIsNotReported pins that the warning is about exposure
// rather than about directories, so it does not cry wolf on a correct setup.
func TestATightDirectoryIsNotReported(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX mode bits do not represent Windows ACLs")
	}
	path := filepath.Join(t.TempDir(), ".secrets", fileName)
	if err := Create(path, false); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ValidateDirectoryPermissions(path); err != nil {
		t.Errorf("a directory dmtx created 0700 was reported as loose: %v", err)
	}
}
