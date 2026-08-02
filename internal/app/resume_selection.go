package app

import (
	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/state"
)

// Choosing which run to resume: options from a Request or from argv, candidate
// selection, and the identity checks that decide whether a stored run is the
// same run the caller is asking about.

func resumeOptionsFrom(request Request) (resumeOptions, bool) {
	options := resumeOptions{
		configPath:              request.ConfigPath,
		statePath:               request.StatePath,
		destructiveAcknowledged: request.AcknowledgeDestructive,
		forceResume:             request.ForceResume,
		abandon:                 request.Abandon,
		abandonReason:           request.AbandonReason,
	}
	return validResumeOptions(options)
}

// validResumeOptions holds the rules both entry points must obey, so a surface
// with no command line cannot request a combination the CLI refuses.
func validResumeOptions(options resumeOptions) (resumeOptions, bool) {
	if options.configPath == "" || options.abandon != (options.abandonReason != "") ||
		options.abandon && (options.forceResume || options.destructiveAcknowledged) {
		return resumeOptions{}, false
	}
	if options.statePath == "" {
		options.statePath = options.configPath + ".state.db"
	}
	return options, true
}

func resumeArguments(args []string) (resumeOptions, bool) {
	var options resumeOptions
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--config":
			if index+1 >= len(args) || options.configPath != "" {
				return resumeOptions{}, false
			}
			options.configPath = args[index+1]
			index++
		case "--state":
			if index+1 >= len(args) || options.statePath != "" {
				return resumeOptions{}, false
			}
			options.statePath = args[index+1]
			index++
		case "--acknowledge-destructive":
			if options.destructiveAcknowledged {
				return resumeOptions{}, false
			}
			options.destructiveAcknowledged = true
		case "--force-resume":
			if options.forceResume {
				return resumeOptions{}, false
			}
			options.forceResume = true
		case "--abandon":
			if options.abandon {
				return resumeOptions{}, false
			}
			options.abandon = true
		case "--abandon-reason":
			if index+1 >= len(args) || options.abandonReason != "" {
				return resumeOptions{}, false
			}
			options.abandonReason = args[index+1]
			index++
		default:
			return resumeOptions{}, false
		}
	}
	return validResumeOptions(options)
}

func latestRunForTarget(
	store state.Backend,
	target config.Endpoint,
) (state.Run, bool, error) {
	targetIdentity, err := endpointWorkloadIdentity(target)
	if err != nil {
		return state.Run{}, false, err
	}
	runs, err := store.List()
	if err != nil {
		return state.Run{}, false, err
	}
	var selected state.Run
	var found bool
	for _, run := range runs {
		matches, err := runEndpointIdentityMatches(
			run.TargetIdentity,
			config.Endpoint{
				Type:     target.Type,
				Database: run.Target,
			},
			target,
			targetIdentity,
		)
		if err != nil {
			return state.Run{}, false, err
		}
		if !matches {
			continue
		}
		if run.Outcome == state.Success {
			// A later success supersedes every older resumable attempt. Keep
			// it selectable so resume can finish any missing terminal audit
			// or release bookkeeping without touching the data plane.
			selected, found = run, true
			continue
		}
		// A terminal non-resumable result supersedes only an older revision
		// of the same run. In particular, a SQL Server migration-snapshot
		// run that closed gracefully has released its physical snapshot and
		// must not fall back to that run's earlier running row.
		if !run.Resumable {
			if found && selected.ID == run.ID {
				selected, found = state.Run{}, false
			}
			continue
		}
		if run.Resumable && resumeEligibleOutcome(run.Outcome) {
			selected, found = run, true
		}
	}
	return selected, found, nil
}

func resumeEligibleOutcome(outcome state.Outcome) bool {
	switch outcome {
	case state.Running, state.Failed, state.Cancelled, state.Partial:
		return true
	default:
		return false
	}
}

func runSourceMatchesEndpoint(
	run state.Run,
	source config.Endpoint,
) (bool, error) {
	engine, err := config.CanonicalEngine(source.Type)
	if err != nil {
		return false, err
	}
	if run.SourceEngine != "" && run.SourceEngine != engine {
		return false, nil
	}
	identity, err := endpointWorkloadIdentity(source)
	if err != nil {
		return false, err
	}
	return runEndpointIdentityMatches(
		run.SourceIdentity,
		config.Endpoint{Type: engine, Database: run.Source},
		source,
		identity,
	)
}

func runEndpointIdentityMatches(
	storedIdentity string,
	legacy config.Endpoint,
	current config.Endpoint,
	currentIdentity string,
) (bool, error) {
	if storedIdentity != "" {
		if storedIdentity == currentIdentity {
			return true, nil
		}
		engine, err := config.CanonicalEngine(current.Type)
		if err != nil {
			return false, err
		}
		if engine != "sqlite" {
			return false, nil
		}
		return equivalentSQLiteLeaseIdentity(
			storedIdentity,
			currentIdentity,
		)
	}
	engine, err := config.CanonicalEngine(current.Type)
	if err != nil {
		return false, err
	}
	if engine != "sqlite" {
		// A legacy network run has no canonical host/port/schema evidence and
		// cannot safely be attached to the supplied endpoint.
		return false, nil
	}
	legacy.Type = engine
	return config.SameEndpoint(legacy, current), nil
}

func sameResumeCandidate(left, right state.Run) bool {
	return left.ID == right.ID &&
		left.Source == right.Source &&
		left.Target == right.Target &&
		left.SourceEngine == right.SourceEngine &&
		left.SourceIdentity == right.SourceIdentity &&
		left.TargetIdentity == right.TargetIdentity &&
		left.LeaseTarget == right.LeaseTarget &&
		left.LeaseOwnerToken == right.LeaseOwnerToken &&
		left.LeaseGeneration == right.LeaseGeneration &&
		left.Outcome == right.Outcome &&
		left.Resumable == right.Resumable &&
		left.Reason == right.Reason &&
		left.StartedAt.Equal(right.StartedAt) &&
		left.EndedAt.Equal(right.EndedAt)
}
