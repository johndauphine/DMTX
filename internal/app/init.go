package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/johndauphine/dmtx/internal/config"
)

// defaultConfigFilename is what init writes when the operator names nothing.
const defaultConfigFilename = "migration.yaml"

// starterConfigFor is the file init writes, addressed to where it is written.
//
// The path is threaded through rather than hard-coded, because a template that
// says "dmtx validate --config migration.yaml" inside a file called
// envs/prod.yaml sends the operator to a file that does not exist.
//
// Commented rather than minimal, because the operator reading it is by
// definition someone who has not written one before.
//
// Two deliberate choices, neither of them a default. The endpoints are
// sqlite-to-sqlite because that is the one pair somebody can try without
// provisioning a server first; dmtx's own defaults for a missing type are
// mssql and postgres, which are no use to someone finding out what this does.
// The tuning settings are left commented out because dmtx derives them, and a
// starter file that pinned them would override the very thing analyze exists
// to explain.
//
// The password is left empty deliberately. Writing a placeholder invites it
// being kept, and a file created by a tool is a file people trust.
func starterConfigFor(path string) string {
	return `# dmtx migration configuration.
#
# Fill in the endpoints below, then:
#
#   dmtx validate --config ` + path + `    check it
#   dmtx analyze  --config ` + path + `    see the plan and why
#   dmtx run --config ` + path + ` --dry-run   rehearse it

source:
  type: sqlite            # sqlite, postgres, mysql, sqlserver, clickhouse
  database: source.db     # a file path for sqlite; a database name otherwise
  # host: source.internal
  # port: 5432
  # user: reader
  # password: ""          # leave empty and supply it another way
  # schema: public
  # ssl_mode: require

target:
  type: sqlite
  database: target.db
  # host: target.internal
  # port: 5432
  # user: writer
  # password: ""

migration:
  # drop_recreate replaces the target tables; upsert merges into them.
  target_mode: drop_recreate

  # Left unset, dmtx derives these from the machine it runs on. Set one and it
  # is honoured; dmtx analyze reports which values came from where.
  # workers: 4
  # connection_limit: 8

  # include_tables: [orders, customers]
  # exclude_tables: [audit_log]
`
}

// executeInit writes a starter configuration.
//
// It refuses to overwrite. A configuration is something an operator has edited,
// often with connection details they cannot reconstruct, and the cost of
// refusing a file that could have been replaced is one flag - while the cost of
// replacing one that should not have been is their afternoon.
func executeInit(request Request) Outcome {
	out := newOutcome(request.Command)
	path := configPathFor(request)

	switch _, err := os.Stat(path); {
	case err == nil:
		if !request.Force {
			return out.failWith(
				FileError,
				fmt.Sprintf(
					"%s already exists; move it aside or pass --force to replace it",
					path,
				),
			)
		}
	case !errors.Is(err, os.ErrNotExist):
		// Something is there that cannot be examined. Writing over it blind is
		// exactly what the check above exists to prevent.
		return out.failWith(FileError, "check "+path+": "+err.Error())
	}

	if directory := filepath.Dir(path); directory != "." {
		if err := os.MkdirAll(directory, 0o755); err != nil {
			return out.failWith(FileError, "create directory: "+err.Error())
		}
	}
	// 0600 rather than 0644: this file is where credentials will go, and the
	// operator who adds them should not have to remember to tighten it first.
	//
	// Chmod as well as the mode argument, because a mode argument applies only
	// when a file is created. With --force the file already exists, so it would
	// otherwise keep whatever mode it had - which is the same defect found in
	// the handoff state file in #8, in a second place, because I fixed it there
	// and did not go looking for the pattern.
	if err := writeRestricted(path, starterConfigFor(path)); err != nil {
		return out.failWith(FileError, "write configuration: "+err.Error())
	}

	out.out("wrote " + path)
	// "then" rather than "next": the template points at databases that do not
	// exist, so an operator who validates before editing sees a failure and
	// wonders what they did wrong. They did nothing wrong; they have not
	// finished yet.
	out.out("edit it, then: dmtx validate --config " + path)
	return out.done(Success)
}

// configPathFor is the file init will write.
//
// Separate from the writing so a test can check the default without running a
// command that creates a file relative to the working directory. The test that
// did that changed the process's directory to keep the file out of the
// repository, and a stray migration.yaml in internal/app is what happens when
// that goes wrong - process-wide state changed for one test's benefit is a
// hazard to every test that runs near it.
func configPathFor(request Request) string {
	if request.ConfigPath != "" {
		return request.ConfigPath
	}
	return defaultConfigFilename
}

// writeRestricted writes a file and leaves it readable only by its owner,
// whether or not it already existed.
func writeRestricted(path, contents string) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.WriteString(contents); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// starterConfigIsValid reports whether the template dmtx ships actually parses.
// Used by a test; kept here so the template and its check stay together.
func starterConfigIsValid() error {
	_, err := config.Parse([]byte(starterConfigFor(defaultConfigFilename)))
	return err
}
