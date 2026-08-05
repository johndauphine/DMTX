package app

// Choosing what a run acts on: options from a Request or from argv, and the
// rules both must obey.
//
// This mirrors resume_selection.go deliberately. Resume has had a shared
// validResumeOptions since the seam was introduced; run did not, and the
// difference was a live §21.1 violation rather than a stylistic one. The
// derivation below lived only in runArguments - an argv parser - so
// `dmtx run --config m.yaml` recorded into m.yaml.state.db while the same
// command through the API recorded into nothing at all.

// runUsage and resumeUsage are each one string, so the flags an operator is
// offered cannot drift from the flags the parser accepts without both changing
// in the same edit. Both were duplicated across two files before this - the
// parser's copy and the executor's - which is two places to update and one to
// forget.
//
// The --state example is a .yaml deliberately, though the derived default is
// .state.db. state.NewBackend selects the YAML store for .yaml and .yml and
// SQLite for everything else, so the example shows the thing --state is for:
// naming a state file the default would not have produced. An example matching
// the default would demonstrate only what happens without the flag.
const runUsage = "usage: dmtx run --config migration.yaml " +
	"[--state migration.state.yaml] [--dry-run] [--acknowledge-destructive]"

const resumeUsage = "usage: dmtx resume --config migration.yaml " +
	"[--state migration.state.yaml] [--acknowledge-destructive] " +
	"[--force-resume] [--abandon --abandon-reason TEXT]"

// runOptions is what a run acts on, however it was asked for.
type runOptions struct {
	configPath              string
	statePath               string
	dryRun                  bool
	destructiveAcknowledged bool
}

// runOptionsFrom builds the options from a Request rather than argv, so a
// surface with no command line gets the same defaults the command line gets.
func runOptionsFrom(request Request) (runOptions, bool) {
	return validRunOptions(runOptions{
		configPath:              request.ConfigPath,
		statePath:               request.StatePath,
		dryRun:                  request.DryRun,
		destructiveAcknowledged: request.AcknowledgeDestructive,
	})
}

// validRunOptions holds the rules both entry points must obey.
//
// The state path is derived here rather than in either caller. That is the
// whole point: a rule applied in a parser is a rule the surfaces without a
// parser do not get, and "both remember to derive it" is not a design - it is
// two chances to forget.
func validRunOptions(options runOptions) (runOptions, bool) {
	if options.configPath == "" {
		return runOptions{}, false
	}
	if options.statePath == "" {
		options.statePath = options.configPath + ".state.db"
	}
	return options, true
}
