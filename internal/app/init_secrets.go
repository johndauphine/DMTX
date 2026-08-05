package app

import (
	"errors"

	"github.com/johndauphine/dmtx/internal/secrets"
)

// executeInitSecrets creates the secrets file, or reports on the one that is
// already there.
//
// Reporting rather than only refusing: an operator running this a second time
// is usually asking where the file is or whether it is safe, and answering that
// is more useful than repeating that it exists. It is also the one place today
// that checks the permissions, since nothing reads the file yet.
func executeInitSecrets(request Request) Outcome {
	out := newOutcome(request.Command)
	path, err := secrets.Path()
	if err != nil {
		return out.failWith(FileError, err.Error())
	}

	if err := secrets.Create(path, request.Force); err != nil {
		// Only an existing file is an ordinary second run. Anything else is
		// something wrong, and reporting it as "already exists" would send the
		// operator to look at a file that may not be there.
		if !errors.Is(err, secrets.ErrAlreadyExists) {
			return out.failWith(FileError, err.Error())
		}
		out.out(path + " already exists")
		if permissionErr := secrets.ValidatePermissions(path); permissionErr != nil {
			out.fail(permissionErr.Error())
			return out.done(FileError)
		}
		out.out("permissions are correct")
		reportDirectory(out, path)
		out.out("to replace it: dmtx init-secrets --force")
		out.out("replacing it discards any key sealed profiles were written with")
		return out.done(Success)
	}

	out.out("wrote " + path)
	reportDirectory(out, path)
	out.out("nothing reads it yet; it is here so its protections exist first")
	return out.done(Success)
}

// reportDirectory mentions a listable secrets directory without changing it.
//
// A warning rather than a failure: the file itself is still owner-only, and
// ~/.secrets is shared with whatever else keeps files there, so this is the
// operator's call to make on their own directory.
func reportDirectory(out *outcomeBuilder, path string) {
	if err := secrets.ValidateDirectoryPermissions(path); err != nil {
		out.out("warning: " + err.Error())
	}
}
