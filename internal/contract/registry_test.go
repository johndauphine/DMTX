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
