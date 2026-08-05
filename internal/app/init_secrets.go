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
		if request.Force {
			return out.failWith(FileError, err.Error())
		}
		// Already there. Say where it is and whether it is safe, then say how
		// to replace it - with the warning that matters, because replacing this
		// file is not like replacing a configuration.
		out.out(path + " already exists")
		switch permissionErr := secrets.ValidatePermissions(path); {
		case permissionErr == nil:
			out.out("permissions are correct")
		case errors.Is(permissionErr, secrets.ErrInsecurePermissions):
			out.fail(permissionErr.Error())
			return out.done(FileError)
		default:
			out.fail(permissionErr.Error())
			return out.done(FileError)
		}
		out.out("to replace it: dmtx init-secrets --force")
		out.out("replacing it discards any key sealed profiles were written with")
		return out.done(Success)
	}

	out.out("wrote " + path)
	out.out("nothing reads it yet; it is here so its protections exist first")
	return out.done(Success)
}
