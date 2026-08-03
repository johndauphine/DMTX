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
}

var Commands = []Command{
	{"run", nil, Planned, Planned}, {"resume", nil, Planned, Planned},
	{"status", nil, Planned, Planned}, {"history", nil, Planned, Planned},
	{"validate", nil, Planned, Planned}, {"diagnose", nil, Planned, Planned},
	{"preflight", []string{"health-check"}, Planned, Planned}, {"analyze", nil, Planned, Planned},
	{"profile", nil, Planned, Planned}, {"ai", nil, Planned, Planned},
	{"init", nil, Planned, Planned}, {"init-secrets", nil, Planned, Planned},
	{"setup", nil, Planned, Planned}, {"cache", nil, Planned, Planned},
	// config was in DMT's slash-command surface but missing here; without it a
	// front end built from this registry would silently lack it.
	{"config", nil, Planned, Planned},
	// serve starts the browser front end. It is Omitted for both interactive
	// surfaces because it is the thing that launches them: a command to start
	// the WebUI, offered inside the WebUI, is nonsense rather than a gap.
	{"serve", []string{"webui", "gui"}, Omitted, Omitted},
}

func Valid() bool {
	for _, command := range Commands {
		if command.Name == "" || command.TUI == "" || command.WebUI == "" {
			return false
		}
	}
	return true
}
