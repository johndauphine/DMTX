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
		fmt.Fprintln(stderr, usage)
		return configurationError
	}
	path, pathErr := statePath()

	// A server is already running unless proven otherwise. Starting a second
	// one would leave the operator with two consoles onto the same databases
	// and no indication which is which.
	if !options.NewInstance && pathErr == nil {
		if target, handedOff := handOff(path, options.Port); handedOff {
			// The message has to match what actually happens. Saying "opening"
			// under --no-browser would leave an operator waiting for a window
			// that was never going to appear.
			if options.OpenBrowser {
				fmt.Fprintf(stdout, "dmtx is already serving; opening %s\n", target)
				launchBrowser(target)
			} else {
				fmt.Fprintf(stdout, "dmtx is already serving at %s\n", target)
				fmt.Fprintln(stdout, "That link carries a one-time token, exchanged for a session on first use.")
			}
			return success
		}
	}

	server, err := New(options)
	if err != nil {
		fmt.Fprintf(stderr, "start server: %v\n", err)
		return stateError
	}

	// Recorded here rather than inside Serve because failing to record is not a
	// reason to refuse to serve. The operator asked for a server; handoff is a
	// convenience layered on top, and losing it costs them a second console
	// later, not the thing they came for. Said out loud so that later surprise
	// is explained in advance.
	//
	// A --new-instance server records nothing. It was asked for alongside an
	// existing one, so claiming the record would point every later invocation
	// at this server and strand the one already running.
	switch {
	case options.NewInstance:
	case pathErr != nil:
		fmt.Fprintf(stderr,
			"warning: cannot locate a state directory, so a second dmtx serve "+
				"will start its own server: %v\n", pathErr)
	default:
		if err := server.recordInstance(path); err != nil {
			fmt.Fprintf(stderr,
				"warning: cannot record this instance, so a second dmtx serve "+
					"will start its own server: %v\n", err)
		} else {
			defer server.forgetInstance(path)
		}
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
	//
	// Not claimed when a signal arrived: ctx is the signal context, so a
	// non-nil error means the operator stopped this, whatever the watchdog
	// concluded a moment earlier.
	if server.ExitedIdle() && ctx.Err() == nil {
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

// usage is one string so the flags an operator is offered cannot drift from the
// flags parseArguments accepts without both changing in the same edit.
const usage = "usage: dmtx serve [--port N] [--no-browser] " +
	"[--idle-timeout D] [--new-instance]"

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
		case "--new-instance":
			if options.NewInstance {
				return Options{}, false
			}
			options.NewInstance = true
		default:
			return Options{}, false
		}
	}
	return options, true
}
