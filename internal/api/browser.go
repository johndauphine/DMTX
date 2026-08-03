package api

import (
	"os/exec"
	"runtime"
)

// launchBrowser opens the operator's default browser at the authenticated URL.
//
// This is what makes local access one step: the operator runs a command, a
// browser opens, and they are in an authenticated session having never seen the
// token. Without it the token would be something to copy and paste, and the
// friction would be an argument for having no token at all.
//
// Failure is deliberately silent. The URL has already been printed, so a
// headless machine or an unusual desktop environment leaves the operator with a
// link to click rather than an error about a browser they may not have wanted
// opened.
func launchBrowser(target string) {
	var command *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		command = exec.Command("open", target)
	case "windows":
		// rundll32 avoids cmd's parsing of & in the query string, which would
		// otherwise truncate the URL at the token.
		command = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		command = exec.Command("xdg-open", target)
	}
	_ = command.Start()
}
