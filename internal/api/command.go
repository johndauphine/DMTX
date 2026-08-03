package api

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
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
		fmt.Fprintln(stderr, "usage: dmtx serve [--port N] [--no-browser]")
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx); err != nil {
		fmt.Fprintf(stderr, "serve: %v\n", err)
		return stateError
	}
	return success
}

// parseArguments reads the serve flags. There is deliberately no bind address;
// see Options.
func parseArguments(args []string) (Options, bool) {
	// Opening a browser is the default: one command landing the operator in an
	// authenticated session is the point. --no-browser turns it off for
	// headless hosts and for anyone driving the API directly.
	options := Options{OpenBrowser: true}
	for index := 0; index < len(args); index++ {
		switch args[index] {
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
