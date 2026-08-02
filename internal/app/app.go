// Package app owns the public command-line contract.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/johndauphine/dmtx/internal/config"
	"github.com/johndauphine/dmtx/internal/contract"
	"github.com/johndauphine/dmtx/internal/migrate"
	"github.com/johndauphine/dmtx/internal/state"
)

const Version = "0.3.0-dev"

const (
	Success = iota
	ConfigurationError
	ConnectionError
	TransferError
	ValidationError
	Cancelled
	StateError
	FileError
)

// Run is the command-line surface: parse argv, execute, render. It is
// deliberately thin. Everything it does that is not parsing or rendering
// belongs in Execute, so a second surface can reach the same behaviour without
// synthesising an argv or scraping bytes back out of a stream.
func Run(args []string, stdout, stderr io.Writer) int {
	// run and resume are not yet behind the seam. They remain on their
	// original path so this refactor changes no behaviour, and are routed here
	// rather than through Execute so nothing has to pretend they produce a
	// structured Outcome. Converting them is the remaining work.
	if len(args) > 0 {
		switch args[0] {
		case "run":
			return run(args[1:], stdout, stderr)
		case "resume":
			return resume(args[1:], stdout, stderr)
		}
	}
	request, outcome, dispatched := parseRequest(args)
	if !dispatched {
		_ = RenderText(stdout, stderr, outcome)
		return outcome.ExitCode
	}
	outcome = Execute(context.Background(), request)
	if err := RenderText(stdout, stderr, outcome); err != nil {
		fmt.Fprintf(stderr, "write output: %v\n", err)
		return FileError
	}
	return outcome.ExitCode
}

// parseRequest turns argv into a Request. It returns dispatched=false for the
// cases that are answered by argv alone - version, help, an unknown command -
// because those have no orchestration to perform and should not pretend to.
func parseRequest(args []string) (Request, Outcome, bool) {
	out := newOutcome("")
	if !contract.Valid() {
		return Request{}, out.failWith(
			StateError,
			"internal command registry is invalid",
		), false
	}
	if len(args) == 0 {
		out.out("DMTX terminal UI is planned; use --help for automation commands.")
		return Request{}, out.done(Success), false
	}
	switch args[0] {
	case "--version", "version":
		out.out(Version)
		return Request{}, out.done(Success), false
	case "--help", "help":
		for _, line := range helpLines() {
			out.out(line)
		}
		return Request{}, out.done(Success), false
	case "resume":
		// Not yet converted: resume still writes as it works. Handled below by
		// the legacy path rather than pretending to produce a structured
		// Outcome it cannot yet build.
		return Request{Command: "resume"}, Outcome{}, true
	case "status", "history":
		request := Request{Command: args[0], Latest: args[0] == "status"}
		if len(args) == 3 && args[1] == "--state" {
			request.StatePath = args[2]
		}
		return request, Outcome{}, true
	case "validate":
		request := Request{Command: "validate"}
		if len(args) == 3 && args[1] == "--config" {
			request.ConfigPath = args[2]
		}
		return request, Outcome{}, true
	case "preflight", "health-check":
		// The alias is resolved here so nothing downstream has to know it
		// exists.
		request := Request{Command: "preflight"}
		if len(args) == 3 && args[1] == "--config" {
			request.ConfigPath = args[2]
		}
		return request, Outcome{}, true
	default:
		for _, command := range contract.Commands {
			if command.Name == args[0] {
				out.out(command.Name + " is planned in this stage.")
				return Request{}, out.done(Success), false
			}
		}
		return Request{}, out.failWith(
			ConfigurationError,
			fmt.Sprintf("unknown command %q; use --help", args[0]),
		), false
	}
}

// Execute performs a request and reports what happened. This is the seam every
// surface shares: the CLI renders the Outcome as text, an API renders it as
// JSON, and a parity test compares two Outcomes rather than two transcripts.
// Commands not listed here are not yet behind the seam; Execute refuses them
// explicitly rather than returning a plausible-looking empty Outcome.
func Execute(ctx context.Context, request Request) Outcome {
	switch request.Command {
	case "validate":
		return executeValidate(ctx, request)
	case "preflight":
		return executePreflight(ctx, request)
	case "status", "history":
		return executeShowState(request)
	case "run", "resume":
		// Valid commands that are simply not behind the seam yet. Reporting
		// them as unknown would send a surface author looking for a typo
		// instead of telling them the truth.
		out := newOutcome(request.Command)
		return out.failWith(
			ConfigurationError,
			fmt.Sprintf(
				"%s is not yet available through Execute; it still writes as it works and is handled by the command line directly",
				request.Command,
			),
		)
	default:
		out := newOutcome(request.Command)
		return out.failWith(
			ConfigurationError,
			fmt.Sprintf("unknown command %q; use --help", request.Command),
		)
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	configPath, statePath, dryRun, destructiveAcknowledged, ok := runArguments(args)
	if !ok {
		fmt.Fprintln(stderr, "usage: dmtx run --config migration.yaml [--state migration.state.yaml] [--dry-run] [--acknowledge-destructive]")
		return ConfigurationError
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Fprintf(stderr, "read configuration: %v\n", err)
		return FileError
	}
	cfg, err := config.Parse(data)
	if err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	if err := migrate.ValidateMigration(cfg); err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	cfg.Migration.DestructiveAcknowledged = destructiveAcknowledged
	if dryRun {
		plan, err := migrate.DryRun(context.Background(), cfg)
		if err != nil {
			fmt.Fprintf(stderr, "dry run: %v\n", err)
			return ConfigurationError
		}
		if plan.Admission != nil && plan.Admission.Supported {
			applyDryRunSchemaDriftState(cfg, statePath, &plan)
		}
		if plan.Deletes != nil &&
			cfg.Migration.Deletes.Mode == config.DeleteModeReconcile {
			if stateErr := applyDryRunDeleteDueState(
				cfg,
				statePath,
				&plan,
				time.Now().UTC(),
			); stateErr != nil {
				plan.Proceed = false
				plan.Deletes.StateError =
					"durable delete due-state could not be inspected read-only"
			}
			migrate.ApplyDryRunDeleteCandidateImpact(
				context.Background(),
				cfg,
				&plan,
			)
		}
		if err := json.NewEncoder(stdout).Encode(plan); err != nil {
			fmt.Fprintf(stderr, "write dry run: %v\n", err)
			return FileError
		}
		if !plan.Proceed {
			return ConfigurationError
		}
		return Success
	}
	if err := config.ValidateBoundedStage4Settings(cfg.Migration); err != nil {
		fmt.Fprintf(stderr, "configuration: %v\n", err)
		return ConfigurationError
	}
	configHash, err := config.Hash(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "configuration hash: %v\n", err)
		return StateError
	}
	resumeCompatibilityHash, err := config.ResumeCompatibilityHash(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "resume compatibility hash: %v\n", err)
		return StateError
	}
	migrationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	store, err := state.NewBackend(statePath)
	if err != nil {
		fmt.Fprintf(stderr, "state backend: %v\n", err)
		return StateError
	}
	started := time.Now().UTC()
	runID := started.Format("20060102T150405.000000000Z")
	leaseStore, lease, err := acquireTargetLease(cfg.Target, runID)
	if err != nil {
		fmt.Fprintf(stderr, "acquire target lease: %v\n", err)
		return StateError
	}
	guard := state.NewLeaseGuard(leaseStore, lease)
	leaseReleased := false
	defer func() {
		if !leaseReleased {
			_ = guard.Release()
		}
	}()
	store, err = newStage4FencedStateBackend(store, guard)
	if err != nil {
		fmt.Fprintf(stderr, "fence Stage 4 state backend: %v\n", err)
		return StateError
	}
	sourceIdentity, err := endpointWorkloadIdentity(cfg.Source)
	if err != nil {
		fmt.Fprintf(stderr, "source workload identity: %v\n", err)
		return StateError
	}
	targetIdentity, err := endpointWorkloadIdentity(cfg.Target)
	if err != nil {
		fmt.Fprintf(stderr, "target workload identity: %v\n", err)
		return StateError
	}
	if err := store.InitializeRun(state.Run{
		ID:             runID,
		Source:         cfg.Source.Database,
		Target:         cfg.Target.Database,
		SourceEngine:   cfg.Source.Type,
		SourceIdentity: sourceIdentity,
		TargetIdentity: targetIdentity,
		Outcome:        state.Running,
		Resumable:      true,
		Reason:         "migration in progress",
		StartedAt:      started,
	}, configHash); err != nil {
		fmt.Fprintf(stderr, "record migration state: %v\n", err)
		return StateError
	}
	if err := store.SaveResumeCompatibilityHash(runID, resumeCompatibilityHash); err != nil {
		fmt.Fprintf(stderr, "record resume compatibility: %v\n", err)
		return StateError
	}
	spoolDirectory, err := stage4SpoolDirectory(statePath, runID)
	if err != nil {
		if stateErr := persistStage4SpoolPreparationFailure(
			store,
			runID,
			err,
		); stateErr != nil {
			fmt.Fprintf(stderr, "record Stage 4 spool preparation failure: %v\n", stateErr)
			return StateError
		}
		fmt.Fprintf(stderr, "Stage 4 spool directory: %v\n", err)
		return StateError
	}
	if err := appendAudit(configPath, runID, "run_started"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := appLifecycleBoundary("run_initialized"); err != nil {
		fmt.Fprintf(stderr, "run lifecycle: %v\n", err)
		return StateError
	}
	migrationContext, heartbeat := startLeaseHeartbeat(migrationContext, guard, 30*time.Second)
	observer := tableCheckpointObserver{
		store:          store,
		runID:          runID,
		guard:          guard,
		resume:         false,
		spoolDirectory: spoolDirectory,
		configPath:     configPath,
	}
	result, err := migrate.Execute(migrationContext, cfg, observer)
	if heartbeatErr := heartbeat.Stop(); heartbeatErr != nil {
		err = fmt.Errorf("%w: renew target lease: %v", state.ErrState, heartbeatErr)
	}
	if err == nil {
		if ownershipErr := guard.Renew(); ownershipErr != nil {
			err = fmt.Errorf("%w: verify final target lease: %v", state.ErrState, ownershipErr)
		}
	}
	if err != nil {
		disposition := migrationAttemptDisposition(result, err, cfg.Migration)
		endedAt := time.Now().UTC()
		if stateErr := persistAttemptDisposition(
			store, runID, disposition, err.Error(), endedAt,
		); stateErr != nil {
			fmt.Fprintf(stderr, "record migration outcome: %v\n", stateErr)
			return StateError
		}
		if auditErr := appendAttemptTerminalAudit(
			configPath,
			runID,
			"run",
			result,
			disposition,
			err,
		); auditErr != nil {
			fmt.Fprintf(stderr, "%v\n", auditErr)
			return StateError
		}
		if releaseErr := guard.Release(); releaseErr != nil {
			fmt.Fprintf(stderr, "release target lease: %v\n", releaseErr)
			return StateError
		}
		leaseReleased = true
		if disposition.acceptedPartial {
			result.Validated = false
			if encodeErr := json.NewEncoder(stdout).Encode(acceptedPartialResult{
				Result: result, Outcome: state.Partial, Resumable: false,
			}); encodeErr != nil {
				fmt.Fprintf(stderr, "write partial result: %v\n", encodeErr)
				return FileError
			}
			return Success
		}
		fmt.Fprintf(stderr, "migration: %v\n", err)
		return disposition.exitCode
	}
	if err := appendAudit(configPath, runID, "validation_completed"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	published, err := publishStage4RunSuccess(observer, runSuccessReason)
	if err != nil {
		fmt.Fprintf(stderr, "publish completed migration state: %v\n", err)
		return StateError
	}
	if !published {
		if err := store.Append(state.Run{
			ID:             runID,
			Source:         cfg.Source.Database,
			Target:         cfg.Target.Database,
			SourceEngine:   cfg.Source.Type,
			SourceIdentity: sourceIdentity,
			TargetIdentity: targetIdentity,
			Outcome:        state.Success,
			Resumable:      false,
			Reason:         runSuccessReason,
			StartedAt:      started,
			EndedAt:        time.Now().UTC(),
		}); err != nil {
			fmt.Fprintf(stderr, "record completed migration state: %v\n", err)
			return StateError
		}
	}
	if err := appLifecycleBoundary("run_success_persisted"); err != nil {
		fmt.Fprintf(stderr, "run lifecycle: %v\n", err)
		return StateError
	}
	if err := guard.Release(); err != nil {
		fmt.Fprintf(stderr, "release target lease: %v\n", err)
		return StateError
	}
	leaseReleased = true
	if err := appendAudit(configPath, runID, "run_succeeded"); err != nil {
		fmt.Fprintf(stderr, "%v\n", err)
		return StateError
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		fmt.Fprintf(stderr, "write result: %v\n", err)
		return FileError
	}
	return Success
}

// applyDryRunSchemaDriftState reads only the latest successful aggregate
// source-schema sentinel for this exact workload. Dry-run must not create a
// run merely to use stateful selection, and a state-read uncertainty must be a
// structured non-proceed plan rather than a silently absent baseline.
func applyDryRunSchemaDriftState(
	cfg config.Config,
	statePath string,
	plan *migrate.Plan,
) {
	baseline := migrate.DryRunSchemaBaseline{}
	sourceIdentity, sourceErr := endpointWorkloadIdentity(cfg.Source)
	targetIdentity, targetErr := endpointWorkloadIdentity(cfg.Target)
	if sourceErr != nil || targetErr != nil {
		baseline.Error = "durable schema baseline scope could not be resolved"
		migrate.ApplyDryRunSchemaDrift(plan, cfg, baseline)
		return
	}
	record, found, err := state.ReadOnlyLatestSuccessfulSchemaSnapshot(
		statePath,
		state.SchemaSnapshotReadScope{
			SourceIdentity: sourceIdentity,
			TargetIdentity: targetIdentity,
			Task: state.TaskKey{
				Type:  "schema-contract",
				Table: "aggregate-source-schema",
			},
		},
	)
	if err != nil {
		baseline.Error =
			"durable schema baseline could not be inspected read-only"
		migrate.ApplyDryRunSchemaDrift(plan, cfg, baseline)
		return
	}
	baseline = migrate.DryRunSchemaBaseline{
		Found:         found,
		CanonicalJSON: record.CanonicalJSON,
		Digest:        record.Digest,
	}
	migrate.ApplyDryRunSchemaDrift(plan, cfg, baseline)
}

// applyDryRunDeleteDueState exposes due state only after matching every record
// to the exact canonical source, target, and Stage 4 table task. A state file
// often contains unrelated migration histories; its newest completed record
// must never influence another workload's delete schedule.
func applyDryRunDeleteDueState(
	cfg config.Config,
	statePath string,
	plan *migrate.Plan,
	now time.Time,
) error {
	if plan == nil || plan.Deletes == nil {
		return errors.New("dry-run delete plan is unavailable")
	}
	sourceIdentity, err := endpointWorkloadIdentity(cfg.Source)
	if err != nil {
		return fmt.Errorf("source workload identity: %w", err)
	}
	targetIdentity, err := endpointWorkloadIdentity(cfg.Target)
	if err != nil {
		return fmt.Errorf("target workload identity: %w", err)
	}
	tasks, err := dryRunDeleteTasks(cfg, plan.Tables)
	if err != nil {
		return err
	}
	plan.Deletes.Tables = make([]migrate.PlannedDeleteTable, len(tasks))
	for index, task := range tasks {
		plan.Deletes.Tables[index] = migrate.PlannedDeleteTable{
			Schema: task.Schema,
			Table:  task.Table,
		}
	}
	if len(tasks) == 0 {
		plan.Deletes.DueStateKnown = true
		plan.Deletes.Due = false
		plan.Deletes.DueReason = "no selected tables require reconciliation"
		plan.Deletes.DueStateScope =
			"per selected source/target/network-table-copy workload"
		return nil
	}
	evidence, err := state.ReadOnlyLatestSuccessfulDeleteReconciliations(
		statePath,
		state.DeleteReconciliationReadScope{
			SourceIdentity: sourceIdentity,
			TargetIdentity: targetIdentity,
			Tasks:          tasks,
		},
	)
	if err != nil {
		return err
	}
	if len(evidence) != len(tasks) {
		return errors.New("read-only delete due-state returned an incomplete task scope")
	}
	deleteTables := make([]migrate.PlannedDeleteTable, len(evidence))
	anyDue := false
	for index, item := range evidence {
		facts, dueErr := migrate.EvaluateDeleteReconciliationDue(
			now,
			cfg.Migration.Deletes.Reconcile.Interval,
			item.Record,
			item.Found,
		)
		if dueErr != nil {
			return fmt.Errorf(
				"durable delete due-state for %s.%s: %w",
				item.Task.Schema,
				item.Task.Table,
				dueErr,
			)
		}
		deleteTables[index] = migrate.PlannedDeleteTable{
			Schema:           item.Task.Schema,
			Table:            item.Task.Table,
			DueStateKnown:    true,
			Due:              facts.Due,
			LastSuccessfulAt: facts.LastSuccessfulAt,
			NextDueAt:        facts.NextDueAt,
			DueReason:        facts.Reason,
		}
		anyDue = anyDue || facts.Due
	}
	plan.Deletes.Tables = deleteTables
	plan.Deletes.DueStateKnown = true
	plan.Deletes.Due = anyDue
	plan.Deletes.DueStateScope =
		"per selected source/target/network-table-copy workload"
	if len(deleteTables) == 1 {
		plan.Deletes.LastSuccessfulAt = deleteTables[0].LastSuccessfulAt
		plan.Deletes.NextDueAt = deleteTables[0].NextDueAt
		plan.Deletes.DueReason = deleteTables[0].DueReason
	} else if anyDue {
		plan.Deletes.DueReason =
			"one or more selected table reconciliations are due"
	} else {
		plan.Deletes.DueReason =
			"all selected table reconciliations are not due"
	}
	return nil
}

func dryRunDeleteTasks(
	cfg config.Config,
	tables []migrate.PlannedTable,
) ([]state.TaskKey, error) {
	engine, err := config.CanonicalEngine(cfg.Source.Type)
	if err != nil {
		return nil, err
	}
	namespace := cfg.Source.Schema
	switch engine {
	case "postgres":
		if namespace == "" {
			namespace = "public"
		}
	case "mssql":
		if namespace == "" {
			namespace = "dbo"
		}
	case "mysql", "mariadb":
		if namespace == "" {
			namespace = cfg.Source.Database
		}
	case "sqlite":
		namespace = ""
	default:
		return nil, fmt.Errorf(
			"source engine %q has no documented delete-reconciliation task scope",
			engine,
		)
	}
	tasks := make([]state.TaskKey, len(tables))
	for index, table := range tables {
		tasks[index] = state.TaskKey{
			Type:   "network-table-copy",
			Schema: namespace,
			Table:  table.Name,
		}
		if err := tasks[index].Validate(); err != nil {
			return nil, fmt.Errorf(
				"build delete-reconciliation task for %s: %w",
				table.Name,
				err,
			)
		}
	}
	return tasks, nil
}

func migrationExitCode(err error) int {
	if isStateOrLeaseFailure(err) {
		return StateError
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Cancelled
	}
	if errors.Is(err, migrate.ErrDestructiveAcknowledgement) {
		return ConfigurationError
	}
	return TransferError
}

func runArguments(args []string) (configPath, statePath string, dryRun, destructiveAcknowledged, ok bool) {
	for index := 0; index < len(args); index++ {
		switch args[index] {
		case "--config":
			if index+1 >= len(args) || configPath != "" {
				return "", "", false, false, false
			}
			configPath = args[index+1]
			index++
		case "--state":
			if index+1 >= len(args) || statePath != "" {
				return "", "", false, false, false
			}
			statePath = args[index+1]
			index++
		case "--dry-run":
			if dryRun {
				return "", "", false, false, false
			}
			dryRun = true
		case "--acknowledge-destructive":
			if destructiveAcknowledged {
				return "", "", false, false, false
			}
			destructiveAcknowledged = true
		default:
			return "", "", false, false, false
		}
	}
	if configPath == "" {
		return "", "", false, false, false
	}
	if statePath == "" {
		statePath = configPath + ".state.db"
	}
	return configPath, statePath, dryRun, destructiveAcknowledged, true
}

// executeShowState serves both status and history; Latest distinguishes them.
//
// The original wrote its errors to stdout rather than stderr. That is preserved
// exactly - it is the observable contract, and correcting it here would make
// this refactor a behaviour change wearing a refactor's clothes.
func executeShowState(request Request) Outcome {
	out := newOutcome(request.Command)
	if request.StatePath == "" {
		out.out("usage: dmtx status --state migration.yaml.state.db")
		return out.done(ConfigurationError)
	}
	store, err := state.NewBackend(request.StatePath)
	if err != nil {
		out.out(err.Error())
		return out.done(StateError)
	}
	if request.Latest {
		run, found, err := store.Latest()
		if err != nil {
			out.out(err.Error())
			return out.done(StateError)
		}
		if !found {
			out.out("no runs recorded")
			return out.done(Success)
		}
		if err := out.setPayload(PayloadRun, publicRun(run)); err != nil {
			out.out(err.Error())
			return out.done(FileError)
		}
		return out.done(Success)
	}
	runs, err := store.List()
	if err != nil {
		out.out(err.Error())
		return out.done(StateError)
	}
	publicRuns := make([]state.Run, len(runs))
	for index, run := range runs {
		publicRuns[index] = publicRun(run)
	}
	if err := out.setPayload(PayloadRuns, publicRuns); err != nil {
		out.out(err.Error())
		return out.done(FileError)
	}
	return out.done(Success)
}

func publicRun(run state.Run) state.Run {
	run.LeaseOwnerToken = ""
	return run
}

// helpLines is the help text as data. A surface that is not a terminal needs
// the same content without a writer to push it into.
func helpLines() []string {
	lines := []string{
		"dmtx - deterministic database migration tool",
		"SQLite first pass: dmtx run --config migration.yaml",
		"Commands:",
	}
	for _, command := range contract.Commands {
		lines = append(lines, "  "+command.Name)
	}
	return lines
}
