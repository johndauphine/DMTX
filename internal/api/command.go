package api

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// Exit codes mirror internal/app's taxonomy for the cases serve can produce.
// They are duplicated rather than imported so this package stays a leaf that
// depends on app for execution only.
const (
	success            = 0
	configurationError = 1
	stateError         = 6
)

// RunCommand is the command-line entry point for serving the browser front end.
//
// It lives here rather than behind app.Execute because serve is not a migration
// command that several surfaces share - it is the thing that creates a surface.
// Routing it through the seam would also invert the dependency: app is the
// facade this package consumes, so app importing it back is a cycle the
// compiler correctly refuses.
func RunCommand(args []string, stdout, stderr io.Writer) int {
	options, ok := parseArguments(args)
	if !ok {
		fmt.Fprintln(stderr,
			"usage: dmtx serve [--port N] [--no-browser] [--idle-timeout D]")
		return configurationError
	}
	server, err := New(options)
	if err != nil {
		fmt.Fprintf(stderr, "start server: %v\n", err)
		return stateError
	}
	// Printed before Serve blocks: an operator watching a terminal needs the
	// URL now, and it is the fallback for anyone who passed --no-browser or
	// whose browser did not open.
	fmt.Fprintf(stdout, "dmtx is serving at %s\n", server.URL())
	fmt.Fprintln(stdout, "That link carries a one-time token, exchanged for a session on first use.")
	if options.IdleTimeout > 0 {
		fmt.Fprintf(stdout, "It will stop on its own after %s unused.\n", options.IdleTimeout)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx); err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return stateError
	}
	// Said plainly, because otherwise an operator returning to the terminal
	// finds a prompt and no explanation for where the server went.
	if server.ExitedIdle() {
		fmt.Fprintf(stdout, "dmtx stopped after %s without a request.\n", options.IdleTimeout)
	}
	return success
}

// defaultIdleTimeout is how long an unused server waits before stopping.
//
// Long enough that reading a plan, thinking, and coming back does not kill the
// session; short enough that a console able to start destructive migrations is
// not still listening on a laptop the next morning. It is a default rather than
// a rule: --idle-timeout 0 turns it off for a host meant to keep serving.
const defaultIdleTimeout = 30 * time.Minute

// parseArguments reads the serve flags. There is deliberately no bind address;
// see Options.
func parseArguments(args []string) (Options, bool) {
	// Opening a browser is the default: one command landing the operator in an
	// authenticated session is the point. --no-browser turns it off for
	// headless hosts and for anyone driving the API directly.
	options := Options{OpenBrowser: true, IdleTimeout: defaultIdleTimeout}
	idleGiven := false
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--idle-timeout":
			if index+1 >= len(args) || idleGiven {
				return Options{}, false
			}
			timeout, err := time.ParseDuration(args[index+1])
			// Negative is refused rather than read as "off": an operator who
			// typed it meant something, and guessing which is worse than
			// saying the flag was wrong.
			if err != nil || timeout < 0 {
				return Options{}, false
			}
			options.IdleTimeout = timeout
			idleGiven = true
			index++
		case "--port":
			if index+1 >= len(args) || options.Port != 0 {
				return Options{}, false
			}
			port, err := strconv.Atoi(args[index+1])
			if err != nil || port < 1 || port > 65535 {
				return Options{}, false
			}
			options.Port = port
			index++
		case "--no-browser":
			if !options.OpenBrowser {
				// Already given: a repeated flag is a mistake worth reporting
				// rather than silently accepting.
				return Options{}, false
			}
			options.OpenBrowser = false
		default:
			return Options{}, false
		}
	}
	return options, true
}
