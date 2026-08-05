package contract

// FrontendDisposition makes unsupported and planned interactive capabilities explicit.
type FrontendDisposition string

const (
	Supported FrontendDisposition = "supported"
	Planned   FrontendDisposition = "planned"
	Omitted   FrontendDisposition = "omitted"
)

type Command struct {
	Name    string
	Aliases []string
	TUI     FrontendDisposition
	WebUI   FrontendDisposition

	// Note explains an Omitted disposition to whoever asks for the command.
	//
	// Omitted covers two different situations that must not be answered with
	// the same sentence: a command that exists but is not something a front end
	// should run, and a command that does not apply to dmtx at all. An operator
	// told the wrong one goes looking for a flag that will never exist.
	Note string
}

var Commands = []Command{
	{Name: "run", TUI: Planned, WebUI: Planned},
	{Name: "resume", TUI: Planned, WebUI: Planned},
	{Name: "status", TUI: Planned, WebUI: Planned},
	{Name: "history", TUI: Planned, WebUI: Planned},
	{Name: "validate", TUI: Planned, WebUI: Planned},
	{Name: "diagnose", TUI: Planned, WebUI: Planned},
	{Name: "preflight", Aliases: []string{"health-check"}, TUI: Planned, WebUI: Planned},
	{Name: "analyze", TUI: Planned, WebUI: Planned},
	{Name: "profile", TUI: Planned, WebUI: Planned},
	{Name: "ai", TUI: Planned, WebUI: Planned},
	{Name: "init", TUI: Planned, WebUI: Planned},
	{Name: "init-secrets", TUI: Planned, WebUI: Planned},
	{Name: "setup", TUI: Planned, WebUI: Planned},
	// cache is Omitted rather than Planned because there is nothing to clear.
	// DMT's cache command clears a type-mapping cache; dmtx has none, and the
	// only thing it keeps under the user cache directory is lease coordination
	// state, which is durable and would be actively harmful to clear while a
	// migration is running. A command that clears nothing is worse than an
	// absent one, because it tells an operator something happened. Reinstate
	// this the day there is a cache worth clearing.
	{
		Name: "cache", TUI: Omitted, WebUI: Omitted,
		Note: "dmtx keeps no cache to clear",
	},
	// config was in DMT's slash-command surface but missing here; without it a
	// front end built from this registry would silently lack it.
	{Name: "config", TUI: Planned, WebUI: Planned},
	// serve starts the browser front end. It is Omitted for both interactive
	// surfaces because it is the thing that launches them: a command to start
	// the WebUI, offered inside the WebUI, is nonsense rather than a gap.
	{
		Name: "serve", Aliases: []string{"webui", "gui"},
		TUI: Omitted, WebUI: Omitted,
		Note: "serve starts a front end, so it is not something a front end runs",
	},
}

func Valid() bool {
	for _, command := range Commands {
		if command.Name == "" || command.TUI == "" || command.WebUI == "" {
			return false
		}
	}
	return true
}
