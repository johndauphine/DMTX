# Stage 5 design note: operator surfaces

Written 2026-08-01, before implementation. This records decisions and their
reasoning so they are not silently re-litigated or rediscovered. It is a design
note, not a specification; `docs/RECREATE_DMT.md` §21 remains normative, and
where this note contradicts it, §21 must be amended deliberately rather than
drifting.

Statements are marked **[decided]** where John has settled them and
**[proposed]** where they are a recommendation awaiting confirmation.

## Deployment model

**[decided]** DMTX runs in two places, both first-class:

1. On a laptop, as a convenience tool pointed at remote databases.
2. On a server sitting next to the database — a server room or jump box.

Every surface decision below follows from having to serve both.

## The TUI is omitted; the WebUI is the interactive surface

**[decided]** There will be no terminal UI. The WebUI must be rich enough that
nothing is lost by its absence.

**[proposed]** Record this honestly rather than by omission:

- set `TUI: Omitted` for every entry in `internal/contract`, which already
  admits `Supported` / `Planned` / `Omitted` and enforces that none is blank
  via `TestEveryCommandHasFrontendDisposition`;
- amend §21.1, which currently requires TUI launch and three-way CLI/TUI/WebUI
  parity, to two-way CLI/WebUI parity.

### Why omitting it is defensible

A TUI's real job is not "nicer than the CLI" — it is *working on a host where
you will not bind a reachable port*. That case is answered without a TUI:

```sh
ssh -L 8484:localhost:8484 dbhost     # then open the UI locally
```

The server binds only to localhost; the operator reaches it through the SSH
channel they already have. This is both the standard pattern for tools of this
shape and **more** secure than exposing a port would be. DMT's
`internal/webui/security.go` already treats non-localhost binds conservatively;
carry that default forward, and make any override loud.

**[proposed]** Documenting the port-forward pattern is a Stage 5 deliverable,
not an afterthought. It is what makes "nothing is lost" true rather than
aspirational.

The effort not spent on a TUI should go into CLI progress output, which serves
the "tailing a long migration over SSH" case better than a TUI would.

## Getting to the console

**[decided]** A laptop operator should reach an authenticated console in one
command, and the process should not outlive their attention.

- **One-click launch.** `dmtx serve` generates a random launch token, prints the
  URL, and opens a browser at it. The token is exchanged for a session cookie
  and the response redirects to `/`, so it does not linger in the address bar,
  history, or a shared screenshot. `--no-browser` opts out.
- **The launch token is single-use.** It authenticates exactly one redemption
  and is cleared. The session secret is a separate value, so the URL is never a
  bearer credential — a URL that stayed valid would be a long-lived secret
  wherever it came to rest.
- **Loopback only, with no bind flag.** The remote path is an SSH forward. There
  is deliberately no way to ask for a reachable listener, so exposing a console
  that starts destructive migrations cannot be a mistyped flag. Anyone who truly
  needs it puts a reverse proxy in front — a decision they make and audit.
- **Exit when idle.** `--idle-timeout` (default 30m, `0` disables) stops an
  unused server. A command in flight is never idle, so this cannot end a running
  migration, and the clock restarts when work *finishes* rather than when it
  started — otherwise a run longer than the timeout would be followed by an
  immediate shutdown while the operator is still reading the result.

- **Single-instance handoff.** A second `dmtx serve` finds the running instance
  and opens a browser at it rather than starting a rival console onto the same
  databases. `--new-instance` overrides it, and a `--port` that disagrees with
  the running instance is read as a request for a different server.

- **Chromeless window.** **[decided]** Delivered by the PWA shell, not by a
  launcher flag. Both routes end at a console with its own window, own icon,
  and no browser chrome, but an `--app-window` flag would have to name a
  Chromium binary and launch *that* — so an operator whose default browser is
  Firefox or Safari would find their console opening in a browser they did not
  choose. Installing the PWA reaches the same place through the browser they
  already use. Tracked with the console frontend rather than here.

### The handoff handshake

A running instance records its port and a secret in `serve.json`, mode `0600`.
The secret is **never sent**. Both sides prove they hold it, over a nonce the
client picks, with distinct labels for each direction so a reply cannot be
replayed as a request.

The reason is that the recorded port belongs to this instance only while it is
alive. Once it dies, anything on the machine — including another account — can
bind that port. A handoff carrying a bearer token would hand a console
credential to whoever answered; a handoff that trusted the reply would open the
operator's browser at an impostor's page, on loopback, looking exactly like the
real console. Proving instead of telling closes both.

The launch token a handoff returns is freshly minted and single-use, like the
one printed at startup. A `--new-instance` server records nothing, and a server
removes only a record it wrote itself — otherwise the second server would
strand the first by taking over its record and then deleting it on exit.

### Threat model

Other processes running as the **same user** are trusted. They can already read
this process's memory and replace its binary, so defending against them is not
achievable here and pretending otherwise would buy complexity without safety.

What *is* defended:

- **The browser.** Any page the operator visits can issue requests to
  `127.0.0.1`, so loopback is not treated as an authorization boundary and every
  route requires a secret.
- **Other accounts on the same machine.** They cannot read `0600` state, and the
  handshake means that even if they could, or if they seized a released port,
  they learn nothing and cannot impersonate a server.

## What the WebUI must preserve

**[decided]** Base it on DMT's TUI, which is a **console REPL**, not a form
wizard. That distinction matters: the target is a command console in the
browser, which reaches parity far more directly than an admin panel would.

Source material in `~/repos/dmt`:

| Capability | Where |
| --- | --- |
| 24 slash commands and dispatch | `internal/tui/commands.go` |
| Autocomplete list with descriptions | `internal/tui/model.go` (`availableCommands`) |
| `@path` file references | `internal/tui/args.go` |
| Command history and completion state | `internal/tui/model.go` |
| Output kinds: plain, boxed, progress | `internal/tui/model.go` message types |
| Parity enforcement | `internal/tui/parity_surface_test.go` |
| Remote-bind safety | `internal/webui/security.go` |
| PWA shell | `internal/webui/static/` |

`@` is optional sugar in DMT: `args.go` strips the prefix, so `@cfg.yaml` and
`cfg.yaml` both work. Preserve that — it is forgiving in the right direction.

### The WebUI is not only the TUI in a browser

**[decided]** DMT's web UI has capability its TUI does not, and reaching parity
means recreating that too. Basing the console on the TUI was the right call for
its *shape*; it is not the whole obligation.

Auditing DMT's `internal/webui` routes against DMTX found that most of the gap
is **not front-end work**. Nine capabilities are registered in
`internal/contract` and unimplemented in `internal/app`:

| DMT web UI route | DMTX |
| --- | --- |
| `/api/run`, `/api/resume`, `/api/status`, `/api/history`, `/api/validate`, `/api/preflight` | implemented |
| `/api/diagnose` | registered, unimplemented |
| `/api/analyze` | registered, unimplemented |
| `/api/profiles` (list, save, delete, export) | registered, unimplemented |
| `/api/ai/config-review` | registered, unimplemented |
| `/api/init-secrets` | registered, unimplemented |
| `/api/setup/{prompt,start,input}` | registered, unimplemented |
| `/api/cache/clear` | **[decided]** Omitted — see below |
| `/api/configs`, `/api/config/check` | registered, unimplemented |
| no distinct route; part of `/api/setup/*` | `init`: registered, unimplemented |
| `/api/session` (get, set, delete defaults) | **absent from the registry** |
| `/api/health` | absent |

The count is of commands, not rows above it: `init` is registered in DMTX and
unimplemented, but DMT reaches it through the setup flow rather than a route of
its own, which is why it has no left-hand entry.

**[decided]** All of it is in scope, including the four that carry real design
weight — session defaults, the setup wizard, profiles, and AI config review.
Commands are built before the console, so the console arrives able to render a
whole surface rather than mostly reporting "planned".

### Two commands scoped differently from DMT

**[decided]** `cache` is **Omitted**, not planned. DMT's clears a type-mapping
cache; dmtx has none, and the only thing it keeps under the user cache directory
is lease coordination state — durable, and harmful to clear during a run. A
command that clears nothing is worse than an absent one, because it tells an
operator something happened. Reinstate it the day there is a cache worth
clearing.

**[decided]** `analyze` reports the **effective transfer plan and why each
setting is what it is**, offline, using the same resolver the migration uses.
Source-derived *suggestions* — DMT's `--apply`-able recommendations — are a
later, separate piece. Inventing a suggestion policy now would mean dmtx
asserting tuning advice it cannot justify from measurement. The offline report
is distinct from `run --dry-run`'s disclosure despite the overlap: dry run
connects to the source, and this answers "why four workers?" with no database at
all.

Both are recorded because they are reductions against DMT, and a reduction
nobody wrote down is rediscovered as a gap.

### Credentials

**[decided]** dmtx has no secrets store today — passwords sit in
`migration.yaml`, which is the file people share. `init-secrets` creates one at
**`~/.secrets/dmtx/config.yaml`**.

The `~/.secrets` convention comes from DMT, so an operator moving between the
tools finds it where they expect and their backup exclusions carry over. The
per-tool subdirectory does not: that convention predates several tools sharing
the directory, and on this machine `~/.secrets` already holds DMT's config and
thirty-five files belonging to a third tool.

**Partitioning is what makes the protections enforceable.** dmtx owns
`~/.secrets/dmtx` and can tighten it; it cannot tighten `~/.secrets` without
changing permissions on other tools' files. The rule throughout is: **enforce
what dmtx owns, report what it does not.**

`os.UserHomeDir` works on Windows, so this is portable even though the
convention is unix-flavoured. It does mean dmtx keeps its own files in two
places: `serve.json` stays in `os.UserConfigDir`.

**[decided]** Protection is **`0600`, and a refusal to load a file with group or
world bits set** — naming the `chmod` to run. This is DMT's model
(`internal/secrets/permissions.go`). No key to manage and nothing to lose.

Being honest about what that buys: it keeps *other accounts on the machine* out.
It does not protect against someone holding the disk — that is what full-disk
encryption is for, and the comments should say so rather than implying more.

Rejected, with reasons, so they are not re-proposed:

- **Passphrase encryption.** Prompting on every command that touches credentials
  breaks unattended runs, and a passphrase cached to avoid that is a file on
  disk again.
- **OS keychain.** Strongest, and what a desktop tool's users expect — but three
  implementations, awkward on the server-next-to-the-database deployment, and
  hard to test without mocking the platform.

DMT stores an AES-GCM master key *inside* that `0600` file and uses it to
encrypt **profiles**. So encryption there protects profiles that move, not the
file itself. Carry that split into the profiles work rather than inventing a
different one.

**A writer with no reader is the `cache` problem in reverse.** `init-secrets`
has to land with the loader and the wiring into endpoint resolution, so a
password absent from `migration.yaml` is taken from the store. Otherwise it
creates a file nothing consumes.

### Profiles

**[decided]** A profile is a whole configuration saved under a name, referenced
as `--profile NAME` where a config path would go. That is DMT's model and it
carries over unchanged.

**Storage: SQLite at `~/.secrets/dmtx/profiles.db`.** Matching DMT's shape means
its behaviour and edge cases carry over rather than being rediscovered.
Replacing several profiles *can* be made atomic with a transaction. It sits in
the directory dmtx owns and enforces at `0700`, not in the per-migration state
database, which is the wrong lifetime — profiles outlive any one migration.

**This is not one file.** In WAL mode SQLite keeps `profiles.db-wal` and
`profiles.db-shm` beside the database, and §21.2's requirement lands on the
profile *files*, not on the directory holding them. So `0600` is enforced on the
database **and its companions**, with the directory mode as defence in depth
rather than the protection itself. Verify at implementation time what mode
`modernc.org/sqlite` gives the companions: SQLite normally matches the main
database, and "normally" is not a guarantee to rest a credential store on.

**Sealing: AES-GCM under a master key**, with a versioned prefix on the
serialised payload — a *format* version rather than a cipher version, so the
encoding can change without anything having to guess what an older file
contains. The key comes from the environment or
`~/.secrets/dmtx/config.yaml`, which is why that file's `encryption:` section
exists. An absent key is generated on first seal and written back — and the
write must preserve the rest of the file, because losing the key makes every
stored profile unrecoverable. DMT guards this explicitly; carry the guard.

**Export re-encrypts under a passphrase supplied at export time.** The exported
file is portable to another machine without the master key ever leaving this
one: the recipient needs only the passphrase. Plaintext export was rejected —
§21.2 exists to stop credentials being written to an arbitrary path in the
clear, and "export" is exactly where an operator would expect that to be fine.

**[decided]** The three sub-decisions, settled 2026-08-05:

- **Argon2id** derives the export key from the passphrase. It won the Password
  Hashing Competition and is what OWASP recommends; being memory-hard, an
  attacker with GPUs gains far less against it than against an iteration-only
  function. `golang.org/x/crypto` is **already in the dependency graph
  indirectly**, pulled in by the database drivers, so promoting it to direct
  adds no module — which removed the only reason to consider a weaker function.
  The parameters live in the file header so they can be raised later without
  breaking exports already written.
- **`--passphrase-file`**, and not a flag, a prompt, or an environment variable.
  It matches an idiom dmtx already has, since config passwords support
  `${file:...}`. The file can be `0600`, never enters shell history, and never
  appears in a process listing. One path serves a person and automation alike,
  so there is one thing to test rather than two — and it needs no new module,
  where a hidden prompt would pull in `golang.org/x/term`.
- **Import ships with export.** Portability was the whole reason for choosing a
  passphrase; without import it is theoretical and the file is a backup rather
  than a transfer. Building both also exercises the format from each end, which
  is how a format avoids quietly becoming unreadable.

### Design needed before implementation

Three do not fit the `Request`/`Outcome` seam as it stands:

- **Session defaults** are not a command. DMT's `resolveOrigin` gives explicit
  argument, then session default, then built-in — state the console needs so an
  operator is not retyping a config path into every command.
- **The setup wizard** is a stateful conversation (`prompt`, `start`, `input`),
  closer to the job model than to a command that answers once.
- **Profile export** writes credentials somewhere, so redaction and encryption
  are decisions to take before it is built, not after.

### Domain commands versus shell commands

**[proposed]** DMT's 24 commands are two different things, and only one is a
parity obligation:

- **Domain** — `/run`, `/resume`, `/validate`, `/diagnose`, `/status`,
  `/history`, `/preflight`, `/analyze`, `/ai`, `/profile`, `/cache`, `/setup`,
  `/init-secrets`. These must reach parity.
- **Shell** — `/clear`, `/quit`, `/about`, `/help`, `/logs`, `/verbosity`,
  `/explore`, `/session`, `/wizard`. These change meaning in a browser
  (`/quit` closes a tab, `/clear` clears a pane) or become UI chrome. Keep them
  as slash commands for muscle memory, but they are not parity obligations.

DMTX's `internal/contract` registers 14 commands. Check that split against
DMT's 24 before implementing, so no domain-level capability is missing from the
registry itself.

## The security decision that has no precedent in the TUI

**[proposed]** `@` completion is the one feature that genuinely changes when it
moves to a browser, and it is the easiest thing here to implement unsafely.

In the TUI, `@` completes against the local filesystem as the invoking user. In
a remote WebUI the files live on the **server**, so completion means exposing a
path-enumeration endpoint. That is an attack surface the TUI never had.

**[decided]** Built as `GET /api/v1/complete`, before the console, so the
console is written against a confined API rather than retrofitted onto a
permissive one. DMT has no precedent to copy here: its WebUI never accepts a
client-supplied path at all — `handlers_configs.go` scans a fixed set of
directories precisely so that "there is no directory-traversal surface".

- **Root-confined.** The root is `--root`, else the directory of `--config`,
  else the working directory. It is resolved once at startup with symlinks
  followed. Containment is checked with `filepath.Rel`, not a string prefix:
  `/base/root-evil` starts with `/base/root` and is not inside it.
- **Symlinks resolved before the check, not after.** A link inside the root
  looks contained until it is followed. Entries whose target escapes are not
  offered either, or completion becomes a way to enumerate the disk by planting
  one link.
- **Absolute prefixes are read relative to the root**, the way a path inside a
  chroot is. There is therefore no spelling of an absolute path that reaches
  out. An entry's returned path is the real absolute one, so nothing is
  mis-sent as a result.
- **Regular files and directories only.** A FIFO offered here could be opened by
  whatever the operator does next, and reading one blocks until something else
  writes.
- **Authenticated**, like every other route.
- **Non-leaking.** One status and one sentence for every failure. Telling
  "outside the root" apart from "does not exist" would make the endpoint an
  oracle for mapping the filesystem above the root without reading a byte of it.
- **Fails closed.** A root that will not resolve leaves completion off rather
  than widening to the working directory.

## Parity enforcement

**[proposed]** Carry DMT's mechanism over rather than inventing one: a registry
plus a surface test per front end, asserting that every registry path a surface
claims to support is actually discoverable in that surface — in autocomplete and
in `/help`, not merely routable.

`TestTUICommandSurface` is the model. DMTX needs its WebUI-side equivalent. With
the TUI omitted, that test is what converts "nothing is lost" from an intention
into an enforced property.

## Wails

**[decided]** Under consideration for a native desktop shell.

**[proposed]** Defer it, and do not let it shape the architecture.

- **Wails does not serve remotely.** It is a local desktop binary with an
  embedded webview. It cannot replace an HTTP-served UI, so it is additive, not
  a substitute — the server-room deployment still needs the served UI.
- **It covers one of the two deployment modes.** The served PWA covers both.
- **It breaks the single-binary story.** Wails needs cgo and platform webview
  toolchains (WebKit on Linux, WebView2 on Windows), which ends the trivial
  `GOOS=windows go build` cross-compilation CI does today and collides with
  §21.1's "one self-contained `dmtx` executable starts on every release
  platform". It also adds macOS signing and notarization.
- **A PWA already delivers most of the benefit.** DMT ships
  `manifest.webmanifest`, a service worker, and maskable icons — installable,
  own window, own icon, no browser chrome.

If Wails is adopted later, build it as a **separate binary** over the same
frontend assets. Revisit only if the laptop experience concretely needs native
menus, file dialogs, or a system tray.

## Long-running commands

**[decided]** Commands run as **jobs**, not inside the HTTP handler, and the
console watches them over **server-sent events** rather than a WebSocket.

The defect this fixes was live: `execute` passed the request's context into
`app.Execute`, so closing the browser tab cancelled the migration. Hours of work
could be discarded by a lid closing. A job's context comes from the process, not
the request, so losing the client ends the response and nothing else. Stopping
is something an operator asks for — `POST /api/v1/jobs/{id}/cancel` — not
something their network does to them.

SSE over WebSocket because the traffic is one-directional: the server reports,
the client watches, and commands are ordinary authenticated POSTs. SSE also
reconnects by itself — a browser's `EventSource` resends `Last-Event-ID` with no
help from the page, so a closed lid resumes where it left off, which is exactly
the case that matters. A WebSocket would add a second protocol, its own auth
story, and hand-written reconnection to buy bidirectionality nothing needs.

`POST /api/v1/execute` is kept and now waits on a job internally, so the
synchronous and streaming surfaces cannot drift into deciding things
differently, and the CLI/WebUI parity test keeps meaning what it says.

Two consequences worth stating:

- **A running job counts as activity.** The idle watchdog's guard assumed
  commands ran inside handlers; once they do not, a migration nobody is
  watching produces no requests, and the server would have stopped itself in
  the middle of one.
- **Progress is reported per table.** A job emits `started`, then a `progress`
  event as each table is planned, begins, and completes, then `finished`.

### Reporting progress

**[decided]** `internal/migrate` needed no change. `app`'s checkpoint observer
already receives `BeforeTables`, `BeforeTable` and `AfterTable(table, rows)`
from `migrate.Execute`; what was missing was turning those calls into reports.

`app.ExecuteWithProgress(ctx, request, sink)` sits beside `Execute`, which
delegates with a nil sink. Separate rather than an extra parameter, because
every command-line invocation has nobody to report to, and because `Request` is
a transport type pinned by golden wire tests — a function does not survive
serialisation, so it belongs beside the request rather than in it.

**A watcher cannot stop a migration.** This is the property that makes the
feature safe to have. Reports are raised from inside observer hooks, and those
hooks run at durable checkpoint boundaries where a returned error aborts the
run. So the sink returns no error, and a sink that panics — a closed channel, a
nil map, a bug in a front end — is contained rather than unwound into the
engine. Reporting also happens *after* the checkpoint each hook exists to write,
so a watcher sees what has durably happened rather than what is about to be
attempted.

Every *per-table* report carries the running tally, so a client that missed
events can still render a fraction correctly from one recent event. That is what
makes the job event buffer safe to trim: a large migration would otherwise
retain two events per table for an hour after finishing.

It is not true of every event, and the earlier wording said it was. The planned
table set is announced once and never restated, so trimming it away left a
reconnecting client the tail of a run with no idea which tables were in it —
about a thousand tables in, since each emits two events. Events like that are
held back from trimming rather than the claim being narrowed: see
`announcedOnce` in `internal/api/job.go`. The corollary is that sequence numbers
have holes after a trim, and nothing may assume they are contiguous.

## Suggested build order

**[proposed]**

1. Surface-agnostic command layer and JSON API — the parity seam.
2. Root-confined, authenticated path-completion endpoint.
3. Console component: input line, slash autocomplete from the registry, history,
   `@` completion against the endpoint from step 2.
4. Output rendering for the three kinds: plain, boxed, progress.
5. WebUI surface parity test.
6. PWA shell, localhost-bound by default, with the port-forward pattern
   documented.
7. Metrics and tracing — read-only, low risk.
8. Notifications, encrypted profiles, AI advisories — last; all three move
   internal state somewhere external and are the highest redaction risk.

## Carried-over obligations

- `history_retention_days` is parsed and validated but has no consumer. Stage 4
  deliberately deferred it here. See block F2 of
  `docs/STAGE4_REQUIREMENTS_TESTS.md`.
- §21.2 requires secrets absent from logs, JSON, state, audit, notifications,
  WebUI responses, and AI payloads. Stage 4 has redaction tests, but **every new
  surface is a new leak path**, and Stage 5 adds five or six at once. Treat
  redaction as a cross-cutting test every surface must pass, not a checklist
  item at the end.
- Stage 4's constraint still binds: Stage 5 **presents** structured facts and
  must not re-decide correctness. `Plan`, `Result`, `PlannedTarget`,
  `PlannedSchemaDrift`, validation findings, and audit records already exist and
  are tested. The failure mode to design against is a surface that recomputes
  something and quietly disagrees with the engine.

## Open questions

- Does a forwarded-port session still require authentication? **[proposed]** yes.
- Should the WebUI ever be permitted to bind beyond localhost, and if so, behind
  what explicit acknowledgement?
- What is the allowed root for `@` completion — the config file's directory, an
  explicit setting, or the working directory?
- Do the 14 registered commands in `internal/contract` cover every domain
  capability DMT's 24 slash commands expose?
