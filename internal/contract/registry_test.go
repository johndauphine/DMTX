package contract

import "testing"

func TestEveryCommandHasFrontendDisposition(t *testing.T) {
	if !Valid() {
		t.Fatal("every registered command must declare TUI and WebUI disposition")
	}
}

func TestHealthCheckAliasIsRegistered(t *testing.T) {
	for _, command := range Commands {
		if command.Name != "preflight" {
			continue
		}
		for _, alias := range command.Aliases {
			if alias == "health-check" {
				return
			}
		}
	}
	t.Fatal("preflight must retain the health-check alias")
}

// TestValidRejectsEachBrokenInvariant feeds the validator registries that break
// one rule each.
//
// Checking Valid() against the shipped registry proves the data is good and
// nothing about whether the check works: removing a rule and the field it
// guards leaves such a test passing, which is what happened before valid took
// its input as an argument.
func TestValidRejectsEachBrokenInvariant(t *testing.T) {
	sound := []Command{
		{Name: "run", TUI: Planned, WebUI: Planned},
		{Name: "preflight", Aliases: []string{"health-check"}, TUI: Planned, WebUI: Planned},
		{Name: "serve", TUI: Omitted, WebUI: Omitted, Note: "starts a front end"},
	}
	if !valid(sound) {
		t.Fatal("a sound registry was rejected, so every case below proves nothing")
	}

	for name, broken := range map[string][]Command{
		"no name":              {{Name: "", TUI: Planned, WebUI: Planned}},
		"no TUI disposition":   {{Name: "run", WebUI: Planned}},
		"no WebUI disposition": {{Name: "run", TUI: Planned}},
		"omitted with no reason": {
			{Name: "cache", TUI: Omitted, WebUI: Omitted},
		},
		"two commands share a name": {
			{Name: "run", TUI: Planned, WebUI: Planned},
			{Name: "run", TUI: Planned, WebUI: Planned},
		},
		"an alias collides with a name": {
			{Name: "status", TUI: Planned, WebUI: Planned},
			{Name: "analyze", Aliases: []string{"status"}, TUI: Planned, WebUI: Planned},
		},
		"two commands share an alias": {
			{Name: "run", Aliases: []string{"go"}, TUI: Planned, WebUI: Planned},
			{Name: "resume", Aliases: []string{"go"}, TUI: Planned, WebUI: Planned},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if valid(broken) {
				t.Errorf("the validator accepted a registry with %s", name)
			}
		})
	}
}

// TestResolveFindsCommandsByNameAndAlias pins the lookup both surfaces depend
// on.
func TestResolveFindsCommandsByNameAndAlias(t *testing.T) {
	for spelling, want := range map[string]string{
		"preflight":    "preflight",
		"health-check": "preflight",
		"serve":        "serve",
		"webui":        "serve",
		"gui":          "serve",
	} {
		resolved, ok := Resolve(spelling)
		if !ok {
			t.Errorf("Resolve(%q) found nothing", spelling)
			continue
		}
		if resolved.Name != want {
			t.Errorf("Resolve(%q) gave %q, want %q", spelling, resolved.Name, want)
		}
	}
	if _, ok := Resolve("teleport"); ok {
		t.Error("Resolve found a command that does not exist")
	}
}
