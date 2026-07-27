package app

import (
	"bytes"
	"testing"
)

func TestVersion(t *testing.T) {
	var output, errors bytes.Buffer
	if code := Run([]string{"--version"}, &output, &errors); code != Success {
		t.Fatalf("exit code = %d", code)
	}
	if output.String() != Version+"\n" {
		t.Fatalf("version output = %q", output.String())
	}
}

func TestUnknownCommandHasConfigurationExitCode(t *testing.T) {
	var output, errors bytes.Buffer
	if code := Run([]string{"unknown"}, &output, &errors); code != ConfigurationError {
		t.Fatalf("exit code = %d", code)
	}
}
