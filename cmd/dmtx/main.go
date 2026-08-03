package main

import (
	"os"

	"github.com/johndauphine/dmtx/internal/api"
	"github.com/johndauphine/dmtx/internal/app"
)

func main() {
	// serve is intercepted here rather than routed through app.Execute. It is
	// not a migration command that several surfaces share - it is what creates
	// a surface - and internal/api consumes internal/app, so routing it through
	// the seam would make app import its own surface.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve", "webui", "gui":
			os.Exit(api.RunCommand(os.Args[2:], os.Stdout, os.Stderr))
		}
	}
	os.Exit(app.Run(os.Args[1:], os.Stdout, os.Stderr))
}
