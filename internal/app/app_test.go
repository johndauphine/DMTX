package app

import (
	"bytes"
	"context"
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

func TestMigrationExitCodeClassifiesCancellation(t *testing.T) {
	if got := migrationExitCode(context.Canceled); got != Cancelled {
		t.Fatalf("cancelled exit code = %d", got)
	}
	if got := migrationExitCode(context.DeadlineExceeded); got != TransferError {
		t.Fatalf("ordinary transfer exit code = %d", got)
	}
}
