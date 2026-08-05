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

// starterConfig is the file init writes.
//
// Commented rather than minimal, because the operator reading it is by
// definition someone who has not written one before. Every value here is a real
// default dmtx would apply anyway, so the file describes the tool's behaviour
// rather than overriding it - a starter config full of settings that override
// defaults teaches the wrong lesson and ages badly.
//
// The password is left empty deliberately. Writing a placeholder invites it
// being kept, and a file created by a tool is a file people trust.
const starterConfig = `# dmtx migration configuration.
#
# Fill in the endpoints below, then:
#
#   dmtx validate --config ` + defaultConfigFilename + `    check it
#   dmtx analyze  --config ` + defaultConfigFilename + `    see the plan and why
#   dmtx run --config ` + defaultConfigFilename + ` --dry-run   rehearse it

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

// executeInit writes a starter configuration.
//
// It refuses to overwrite. A configuration is something an operator has edited,
// often with connection details they cannot reconstruct, and the cost of
// refusing a file that could have been replaced is one flag - while the cost of
// replacing one that should not have been is their afternoon.
func executeInit(request Request) Outcome {
	out := newOutcome(request.Command)
	path := request.ConfigPath
	if path == "" {
		path = defaultConfigFilename
	}

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
	if err := os.WriteFile(path, []byte(starterConfig), 0o600); err != nil {
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

// starterConfigIsValid reports whether the template dmtx ships actually parses.
// Used by a test; kept here so the template and its check stay together.
func starterConfigIsValid() error {
	_, err := config.Parse([]byte(starterConfig))
	return err
}
