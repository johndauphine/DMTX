# Console design note

Written 2026-08-05, revised after review before any code was written. Settled
parts fold into `docs/STAGE5_DESIGN.md`; this is the working draft.

Three review rounds. Each rejected part of the one before: round 1 killed three
of eight decisions, round 2 killed round 1's replacement for one of them, and
round 3 killed round 2's replacement for *that*. Between them they found three
defects in already-merged code, all tracked separately. Where a decision was
wrong and then replaced with something also wrong, both are recorded — the
sequence is the useful part.

## The goal that sets the scope

**An operator never opens a YAML file, and never needs a terminal after
`dmtx serve`.**

This is the requirement, and it is larger than "recreate DMT's web UI." Two
consequences fall straight out of it, and both change what is being built:

- **The console is not only a REPL.** A REPL cannot edit a config. Meeting the
  goal means structured editing of the migration config *and* of the secret
  store, which is a form, which is a panel. The console is a REPL **plus**
  panels that write files — with the REPL as the surface an operator who knows
  the commands can always fall back to.
- **Nothing writes YAML today.** There is no `yaml.Marshal` anywhere in the tree
  (`init` renders a fixed starter template, `init.go:120`), and `config.Config`
  has 44 settable fields. Writing a config back is new construction, and it has
  a hard sub-problem: a regenerated file loses an operator's comments and
  ordering. `yaml.v3`'s `yaml.Node` round-trips both and is already a direct
  dependency — that is the path, and it needs its own design and its own PR.

`dmtx serve` itself stays a command. It is the bootstrap, and a browser cannot
start a server that is not running.

**This note does not yet cover the editing surface.** It designs the REPL, which
is the half that is nearly buildable. The config editor, the secret-store
editor, and the setup wizard that stitches them into a first-run path are a
second note. Recording that split now, because everything below was written
against the smaller goal and parts of it read as complete when they are not.

## What it is

A **command console in a browser** — an input line, a scrolling transcript, a
status bar, and (per the goal above) panels for the things a line of text cannot
edit.

DMT's *TUI* is a REPL; DMT's *web UI* is a dashboard of forms. DMTX takes the
TUI's shape and the web UI's **capabilities**, which is what the nine-command
audit established. An operator who knows the command line should not have to
learn a second model — but an operator who does not know it must still be able
to do everything, which is what the panels are for.

## Constraints already fixed

Vanilla JS and CSS served by `go:embed` — no build step, no npm in CI, one
self-contained binary that still cross-compiles. Authenticated by the session
cookie `/login` sets. Loopback only.

## The console needs API changes first

They are the difference between a console whose correctness lives in Go and one
whose correctness lives in untestable JavaScript. They should land as their own
PR before any frontend work.

The count in earlier drafts was "four", and it was wrong — later decisions add
server work without adding to the list. The full set is change 0 through 4
below, plus: the JSON 401 body (decision 10), `job.started_at` and the
`forgetFinished` fix (change 2), the render function and its renderers
(decision 5), and the registry's `WebUI` dispositions, which are `Planned` for
**every** command today (`registry.go:28`) and become false for ten of them the
day the console ships. Decision 1 renders those dispositions, so shipping
without updating them puts a wrong label on the console's own help.

0. **Something turns a typed line into a `Request`.** *Built.* This is the
   change the first three drafts missed entirely, and it was the largest.

   Every decision below assumes flags — decision 7 has `/session config PATH`,
   decision 8 has an operator retyping `run --acknowledge-destructive`. Nothing
   in the API accepts a command line. `POST /api/v1/execute` decodes a JSON
   `app.Request` with `DisallowUnknownFields` (`handlers.go:59`), and the only
   argv parser, `parseRequest` (`app.go:54`), is unexported behind `app.Run`,
   which returns an `int`.

   So as written, the console must reimplement in JavaScript: `runArguments`'
   duplicate-flag rejection and `--state` derivation (`app.go:739`),
   `diagnoseArguments`' unknown- and repeated-flag refusals (`app.go:199`), the
   `--abandon`/`--abandon-reason` pairing rule (`request.go:36`), and the
   positional `--state` of `status` and `history` (`app.go:107`). That is the
   single largest piece of correctness in the console, invisible to every Go
   test — precisely what this section exists to prevent.

   **Shipped as** `app.ParseRequest([]string) (Request, Outcome, bool)`, with
   `parseRequest` kept as its unexported body, and `POST /api/v1/parse` taking
   `{"line": "..."}` and returning either the resolved `Request` or the same
   `Outcome` the CLI would print. The console gets flags for free, the flag
   rules stay in one place, and the parity criterion gains teeth it did not
   have: both surfaces parse the same bytes with the same function.

   Two things settled in the building that the design did not anticipate:

   - **Tokenising lives in `internal/api`, not `internal/app`.** The command
     line never tokenises — a shell did that before dmtx started — so putting it
     in `app` would have added a concern the CLI does not have to the package
     that defines the CLI's contract.
   - **A line is one line.** An *embedded* newline is refused rather than
     treated as whitespace, because `status` and `--state m.db` pasted together
     tokenise into a perfectly valid `status` the operator never typed. Same
     reasoning as `decodeRequest` refusing a body with two JSON documents.
     Newlines at either edge are trimmed rather than refused — that is what a
     paste leaves around what was typed, and there is nothing on the far side
     of them to join.

   This also decided the shape of the input line: the console offers flags. The
   alternative — no flags, `Request` fields from panels only — was coherent but
   would have required rewriting decisions 7 and 8.

1. **`GET /api/v1/commands` returns what the console should show**: filtered to
   non-`Omitted` and alias-expanded.

   The justification given for this in the second draft was backwards — it
   claimed today's endpoint makes a `/help` "strictly worse than the CLI's
   `helpLines()`", but `helpLines()` is three header lines and a bare name per
   command, with no aliases and no dispositions. Today's endpoint already
   carries more.

   The real case is simpler and stronger: **`help` is not in
   `contract.Commands`**, so it cannot reach the seam at all. `parseRequest`
   answers `--help` before dispatch, `Execute(Request{Command:"help"})` returns
   `unknown command "help"`, and `helpLines()` is unexported. The console has
   nothing behind `/help` today.

   Descriptions are deferred. `contract.Command` has no such field, so adding
   one is a registry change across sixteen entries needing its own invariant in
   `valid()` — otherwise it ships empty, which is the drift the registry's
   existing invariants exist to prevent. `Note` is populated only for the two
   `Omitted` commands, so it is *absent* from a filtered list by construction;
   shipping it alongside a filter that removes every entry carrying it was
   incoherent.

2. **`GET /api/v1/jobs`** — list them. There is no way to find a running job
   today except an id the client kept, which is why decision 3 below changed.

   This needs more than a handler. `jobs.byID` is a map and `job` has no start
   time, so **there is nothing to order the list by** and a reload reshuffles
   the transcript; `job` needs a `started_at`. And `forgetFinished` runs only
   when a job starts, so on a quiet server a job finished five hours ago is
   still listed despite the one-hour retention — the list *over*-reports, which
   is the opposite of the caveat the second draft wrote down.

3. **No server change. The console calls `close()` when it sees `finished`.**

   Two server-side fixes were proposed and both were wrong. The second draft's
   `204` fails the connection, which fires `error` with no status — normal
   completion then looks like a `404`, a `401`, or a dropped socket. The third
   draft's `retry: 86400000` was adopted on the claim that it "stops the loop
   without firing `error`". **It does not.** On a clean EOF the UA runs
   *reestablish the connection*, whose first queued task sets `readyState` to
   `CONNECTING` and fires `error`; `retry:` changes only the delay before the
   next fetch. The observable state at `error` time is `readyState`, and it
   makes an exhausted stream **identical to a dropped socket** (both
   `CONNECTING`), while `404`/`401` fail the connection and give `CLOSED`. So
   one proposal conflated completion with 404/401 and its replacement conflated
   completion with a socket drop. Neither is self-distinguishing.

   The discriminator was already in hand: **the console has received the
   `finished` frame.** `jobEvents` returns as soon as `ended` (`job_handlers.go:101`)
   and `finished` is always the highest sequence, so a stream that ends has
   delivered it. Closing on `finished` means the reconnect loop never starts,
   which is what decision 11 needed, and it costs nothing on the server.

   If a backstop is still wanted for a client that never closes, a `retry:` line
   is fine — but justified as bounding a reconnect storm, not as suppressing
   `error`. The console's own rule is "`readyState` **plus** have I seen
   `finished`", and it must be written that way or it will be written wrong.

   Two further things the drafts missed, both already in the code:
   **`EventSource` cannot set headers**, so `resumeFrom`'s `Last-Event-ID` path
   serves only the browser's own auto-reconnect. A console reopening a stream
   itself must pass `?from=N`, or it replays the whole buffer — including
   `started` and `finished`. And trimming keeps `events[0]` plus the tail, so
   **sequence numbers have a hole**; nothing may assume contiguity.

4. **The `started` event carries the fully resolved `app.Request`.** Today it
   carries `{id, command}`, so nothing tells an operator which config a command
   actually ran against after session defaults filled it in.

## Decisions

### 1. Commands come from the registry, and the console never judges one

The console renders whatever `GET /api/v1/commands` returns and **sends every
command it is given, including ones it does not recognise.** A console that
refused an unknown command locally would answer `/cache` with "unknown command"
while the seam answers "cache is not available: dmtx keeps no cache to clear" —
the exact disagreement `TestNoSurfaceCallsARegisteredCommandUnknown` was written
after.

The original justification for this decision — that it makes the parity test
possible — was wrong and is worth recording as wrong. Building the list from the
API *removes* the drift the parity test would look for; a Go test comparing the
endpoint to `contract.Commands` compares the handler against itself. What
remains untestable from Go is the JS that renders the list, and no amount of
dynamic construction fixes that. The honest position: the drift is designed out,
and the rendering is held by review rather than by a test.

### 2. Everything runs as a job, and a start is never retried

The console POSTs `/api/v1/jobs` and subscribes to
`/api/v1/jobs/{id}/events` — even for `status`. One path means the fast route
cannot behave differently from the slow one.

**A failed or lost `POST /api/v1/jobs` is never retried automatically.** If the
response is lost the console cannot know whether a `run` started, and a retry
starts a second one. The target lease makes that mostly survivable; "mostly" is
not a design. The operator is shown what happened and decides.

### 3. The server is the only record of a job; the console stores nothing

Rejected from the first draft: keeping job ids in `sessionStorage`.

The argument given was that a job id is a capability that should not outlive its
tab. It is not one. Every job route sits behind `auth.require`; an id without
the session grants nothing, and with the session grants nothing the session did
not already grant, since the session can start and cancel jobs anyway. The
argument had the shape of a security argument attached to something that was not
a security property.

What it cost was real: close the tab during a three-hour run and the migration
becomes unwatchable and **uncancellable from the console**. It also contradicted
`docs/STAGE5_DESIGN.md`, which delivers the chromeless window through an
installed PWA — a window whose whole purpose is being closed and reopened, and
which clears `sessionStorage` when it is.

So: every tab renders from `GET /api/v1/jobs` on load and after each transition,
and stores nothing about jobs. This also answers the second-tab question —
tabs are views, never owners.

Two caveats, and one claim withdrawn.

Retention is best-effort in the direction of keeping too much, not too little —
`forgetFinished` is called only from `jobs.start` (`job.go:231`), so on a quiet
server a finished job outlives its hour indefinitely rather than for an hour.
The console must not present a listed job as current without checking its state.

The list is **server-global**: every `POST /api/v1/execute`, including the parity
tests and any script, creates a job in it. Whether the console shows jobs it did
not start is unresolved, and it is the load-bearing question of this decision:
if it does, "tabs are views" is real; if it does not, the reload story does not
work.

Withdrawn: "makes a server restart show an honest empty list rather than a
phantom." A restarted server mints a fresh `auth.session` (`server.go:112`), so
the old tab's cookie fails `auth.require` and never sees a list at all — it sees
decision 10's server-gone state. The benefit was imagined.

### 4. No HTML is ever constructed from data

Not "output is escaped" — the stronger property: no `innerHTML`,
`insertAdjacentHTML`, `document.write`, `eval` or `new Function` anywhere in the
shipped assets. Everything is `textContent` and `createElement`.

The risk is not confined to messages: the `plan` and `preflight_report` tables
build DOM from payload keys *and* values, the progress line carries table names,
and every one of those ultimately comes from a database dmtx was pointed at. DMT's `app.js` does not
hold this property — it is `innerHTML` throughout with a hand-rolled `esc()` —
so there is prior art to avoid here rather than copy.

Two things this needs alongside it:

- **A Content-Security-Policy header** on the console page:

  ```
  default-src 'self'; script-src 'self'; object-src 'none';
  base-uri 'none'; frame-ancestors 'none'; form-action 'none'
  ```

  `nosniff` protects JSON responses and does nothing for the page.
  `frame-ancestors 'none'` matters for a page that starts migrations, and
  `form-action 'none'` for one that should not be able to submit anywhere.
  `default-src 'self'` blocks inline styles, so the stylesheet must be a
  separate same-origin file. A `Referrer-Policy` belongs here too — though not for
  the reason an earlier draft gave. `/login` is a 302 with no body
  (`auth.go:59`), so no document ever loads at the `?token=` URL to send a
  referrer from, and `default-src 'self'` blocks off-origin requests anyway. It
  is defence in depth, and should be labelled as such.
- **`white-space: pre-wrap`**, or `textContent` collapses the newlines in
  multi-line command output and aligned CLI output stops aligning.

### 5. Output is rendered in Go, by a function that does not exist yet

Rejected twice. The first draft proposed a JS renderer per payload kind with a
raw-JSON fallback: that cited wire-shape tests, which pin the **Go** side, as
coverage for the **JS** side, and the fallback was a redaction hole no test
could close.

The second draft proposed routing everything through `app.RenderText` and
claimed the console would then "match the command line by construction, on a
path that is already tested". **That claim was true and useless.** `RenderText`
writes the messages and then dumps `Payload.Data` as one raw JSON line, and
`TestRenderTextReproducesTheOriginalByteStream` pins exactly that. Measured:

```
$ dmtx status --state s.db      messages: 0
{"id":"r1","source":"","target":"","outcome":"success",...}
```

`run`, `status`, `history` and `validate` produce **no messages at all**. So the
proposal would have shown an operator a JSON blob for the most-typed command in
the console, while the sentence justifying it was literally accurate. Three
other commands — `config`, `analyze`, `diagnose` — would have printed their
content as prose and then again as JSON.

It also could not have worked mechanically. `RenderText` takes two writers, and
`request.go` states the reason as a property of the seam: "the CLI contract
includes *which* stream a line went to, and a renderer cannot reconstruct that
from the text alone." One `<pre>` means either losing the stream tag, so stderr
cannot be styled, or losing the interleaving order.

**What is actually needed** is a third function beside `RenderText` and
`RenderJSON` that returns tagged lines and never writes `Payload.Data`, plus
renderers in Go for the payloads that carry content. The kinds whose commands
already produce prose — `config`, `analysis`, `diagnosis` — suppress the payload
line rather than rendering it twice.

Two things the third draft got wrong about *how* those renderers are keyed:

- **`Payload.Kind` does not discriminate.** `PayloadResult` is set from two
  incompatible Go types: `migrate.Result` (`app.go:529`, `resume.go:353`) where
  `tables` is an `int`, and `migrate.ValidationResult` (`validate.go:37`) where
  `tables` is an array of findings. A renderer keyed on `result` alone cannot
  unmarshal both — one of them *errors*. So renderers are keyed on
  **`(command, kind)`**, or `result` is split into two kinds. Note also that
  `request.go:93`'s comment — the kind "names the shape so a consumer knows what
  it is holding without having to infer it from the command" — is **untrue
  today**, and the fix should either make it true or correct it.
- **The enumerated list was incomplete.** `PayloadResumeResponse`
  (`resume_terminal.go:69`) appeared in neither the render list nor the suppress
  list, which would leave `resume --abandon` — the recovery path after a failed
  migration — showing "this dmtx cannot display it". The list is not written by
  hand: **a table test asserts every `Payload*` constant has a renderer or an
  explicit suppression**, and fails when a new constant is added. That is the
  same mechanism API change 1 argues for in `valid()`, applied here instead of
  a runtime fallback message — a missing kind is a missing `case` in a binary
  that ships both halves, not a runtime condition.

And one claim to strike rather than repair: the third draft said per-kind
renderers "are covered by the wire-shape tests that genuinely do cover Go." They
are not. `wire_shape_test.go` marshals a value and compares the set of JSON
field *paths* against a golden list; it never calls a renderer. What it gives a
renderer is an early warning when a field it reads is renamed. **That is exactly
the coverage fallacy round 1 killed, committed again two paragraphs after
condemning it** — the tell being a sentence that names a real test suite and
lets its reputation stand in for a claim it does not make. Renderers need golden
tests of their own.

One tradeoff to state rather than let a reader discover: this deliberately makes
the console's output **differ** from the terminal's, for `run`, `status`,
`history` and `validate` — the terminal prints one JSON line, the console prints
a rendered block. "An operator who knows the command line should not have to
learn a second model" survives at the level of commands and flags, not bytes.
Also: `config_report.go:119` currently says "the console renders the payload
instead, so this is the command line's view rather than the only one." This
decision reverses that, and the comment goes on the list to correct.

### 6. `@` completion is confined; execution is not

The completion endpoint is well built: it resolves symlinks before checking
containment, uses `filepath.Rel` rather than a string prefix, refuses non-regular
files, and answers every failure identically. The console sends the entry's
absolute `path`, so what runs is what was completed.

**Confinement is a property of enumeration only.** `app.Execute` accepts any
`ConfigPath` and reads it; there is no root check in the command layer. That is
defensible under the recorded threat model — same-user processes are trusted —
but it must be written down here, because `STAGE5_DESIGN.md` says "there is no
spelling of an absolute path that reaches out" about *completion*, and a later
reader will carry that sentence to execution, where it is false.

### 7. What ran is recorded, not predicted

Rejected from the first draft: a status bar showing the session default as the
mitigation for defaults being invisible.

Showing a default does not establish that the shown default is the one used.
`decodeRequest` resolves it server-side and the `Outcome` never echoes the
resolved path, so the status bar shows the console's cached belief — which
another tab, a script, or a default persisted from a previous server can make
stale. A destructive `run` would then report success against a config the
operator never saw.

The `started` event carries the resolved `Request` (API change 4), so the
transcript is a record of what actually ran. The status bar still shows the
current default, but as a convenience rather than as the guarantee.

`/session config PATH` sets it — **not** `/config`, which is a registered domain
command with its own payload. `/session` also matches DMT and the API's own key
names.

### 8. The console does not invent a destructive-command policy

Rejected from the first draft: the console confirming `run` "against a non-empty
target".

The console cannot know the target is non-empty — that is discovered by
connecting to the database during preflight or the run itself. A confirmation
the console cannot ground is a false alarm, and a false alarm trains exactly the
click-through it was meant to prevent.

An earlier draft supported this with "the engine's gate only fires for
`target_mode: drop_recreate`, so the console would confirm on most runs that
would never have been gated." That is backwards: `drop_recreate` is the
**default** and one of only two modes (`config.go:255`), so most runs *are*
gate-eligible. The conclusion stands on the first sentence alone.

Instead the console sends `acknowledge_destructive: false` and lets the engine
refuse. That refusal — which names the specific table, containing rows on most
adapters and merely *existing* on ClickHouse
(`adapter_target_clickhouse.go:582`) — *is* the prompt. It cannot appear for a run the engine would not gate, cannot be
skipped for one it would, and adds no second policy that could disagree with the
engine.

The re-run gesture is **typing, not clicking**: `/run --acknowledge-destructive`
retyped. Not an OK button. Typing is the cost that makes the gesture mean
something, and it is the same cost the command line imposes.

An earlier draft said in the same breath that the engine's message "names a flag
an operator in a browser cannot type", which contradicts the sentence above it —
they can type it, that is the whole design. The real point is narrower: the
engine's wording says *rerun*, which describes a shell, and the console should
present it as the flag to add rather than a command to re-enter.

### 9. Cancel is part of the console

Absent from the first draft entirely, against a `STAGE5_DESIGN.md` decision that
stopping is something an operator asks for. The command line has Ctrl-C; a
console that starts destructive migrations and cannot stop them is both a parity
gap and a safety gap.

One trap: `POST /api/v1/jobs/{id}/cancel` answers `202 {"state":"cancelling"}`
for any job that exists, including one that already finished, and `jobStatus`
never reports a cancelled state — it reports `running` until the outcome lands
with exit code `Cancelled`. **The console renders nothing on the `202` and waits
for the `finished` event**, or it announces a cancellation that did not happen.

### 10. States the console must have

The first draft had none of these.

- **The server is gone** — and this is *two* states, which earlier drafts ran
  together under "every request 401s".

  When the idle watchdog fires the **process exits**: `watchIdle` calls `stop()`
  (`idle.go:85`), `Serve` shuts down, `RunCommand` returns. The listener is
  closed, so `fetch` **rejects with a network error** — no status, no body,
  nothing to read — and an `EventSource` retries against a dead port forever
  without ever producing one. There is no 401 anywhere in the case the console
  will hit most.

  A 401 arises only when a *different* dmtx is now on that port with a fresh
  `auth.session` (`server.go:112`). Then the cookie authenticates against a
  secret that no longer exists, and since `GET /` is itself behind auth, a
  reload gives a bare page reading "authentication required".

  The console needs both paths: a rejected-`fetch` path and a 401 path, ending
  in the same plain statement that the server it was talking to has stopped and
  `dmtx serve` will start a new one with a new link. **The 401 body should still
  become JSON** — `auth.go:136` is `http.Error`, so a fetch wrapper expecting
  `{error}` throws something unrelated — but that fix does not address the
  headline case, and the earlier draft introduced it as though it did.
- **Nothing configured yet.** `dmtx serve` in a fresh directory discovers no
  configs and has no default, so the first command fails on an empty path. The
  trigger is **zero discovered configs and no session default**, not a
  particular error string: `run` and `resume` produce `read configuration:
  open : no such file or directory` (`app.go:319`, `resume.go:42`), while
  `validate`, `config`, `analyze` and `preflight` answer with a usage line and
  `status`/`history`/`diagnose` with theirs. Keying the empty state on the
  message would catch two commands out of nine. Under the goal at the top of
  this note this state does not point at `init` — it opens the setup panel.
- **Started, nothing to report.** A run acquires a lease, fences state and
  computes identities before the first progress event, which on a slow source is
  minutes. `--dry-run` emits no progress events at all. The progress line needs
  a state for this that is not a stalled `0/0`.
- **The outcome arrives twice** — once in the `finished` frame, once from
  `GET /api/v1/jobs/{id}` on reconnect. Render one.

### 11. The idle timeout has to be chosen, not inherited

`activity.track` wraps the whole handler, and `jobEvents` blocks in it until the
job ends — so **an open stream is a request in flight**, and `idleFor` returns
zero for as long as one exists. That is right while a job runs. It is the
reconnect afterwards that bites: the server closes the stream itself on `ended`,
the browser reopens it three seconds later, gets an immediate close, and repeats
— each cycle a fresh request, so a finished job's stream keeps the server alive
forever and `--idle-timeout` never fires. API change 3 stops that loop. The
question it exposes is real and unanswered:

An operator reading a 200-table plan for 31 minutes makes no requests, so the
server stops and takes their session with it. `STAGE5_DESIGN.md` justified
restarting the idle clock on completion with "otherwise a run longer than the
timeout would be followed by an immediate shutdown while the operator is still
reading the result" — but reading the result is precisely the state that
generates no requests.

Either the console heartbeats, and the idle timeout means nothing while a tab is
open; or it does not, and reading is punished.

**Recommendation: no heartbeat.** A heartbeat does not merely weaken the timeout,
it deletes it — a forgotten tab is exactly the case the timeout exists for, and a
tab that pings makes "unused" unobservable. That would leave a console able to
start destructive migrations listening on a laptop overnight, which is the
sentence `defaultIdleTimeout` is documented with.

The reading problem is then solved where it actually is, which is the *warning*,
not the clock:

- The console warns at **T-2min** and offers one click that both makes a request
  and resets the clock. An operator present sees it; an operator gone does not,
  and the server stops.

  The third draft got the mechanism backwards. It said the console "must not
  keep its own countdown" and should read a deadline from
  `GET /api/v1/health` — a route that **does not exist** (`server.go:219`
  registers no such thing), and, worse, a design that requires polling. Polling
  *is* a heartbeat: `activity.track` wraps the whole mux (`server.go:264`) and
  `end()` sets `last = time.Now()` unconditionally (`idle.go:29`), so a `GET` of
  the deadline resets the very clock it reports. It would have re-introduced the
  thing this decision exists to refuse, two paragraphs after refusing it.

  The design that works is a **client-side one-shot timer re-anchored on every
  response**. Every response carries the deadline; each one resets the timer to
  `deadline - 2min`. It cannot drift, because every request that moves the
  deadline also delivers the new value — a client countdown that is correct
  precisely because it is anchored server-side, and it costs no extra requests.

  Two encodings it needs: while a job runs `inFlight > 0` and `idleFor` returns
  0 (`idle.go:42`) while `last` stays frozen, so `last + idleTimeout` is a
  timestamp in the past — "not idle" needs its own value rather than a stale
  one. And `--idle-timeout 0` disables the watchdog (`command.go:184`), so the
  field must be able to say there is no deadline at all.
- The server-gone state is **non-destructive**: the transcript stays on screen
  and readable, the input goes dead with the message from decision 10. Losing a
  plan an operator was mid-way through reading is the actual harm; the session
  ending is not.

One hazard this removes rather than manages: a long run is already safe without
any of it. `jobs.start` calls `activity.begin()` and the goroutine defers
`end()`, so **a running job is never idle whatever it emits** — a `--dry-run`
that produces no progress events at all is covered on the same path. The console
needs no keep-alive during a run; it needs one only while an operator reads.

### 12. The input model

Absent from both earlier drafts, and it is what makes or breaks a REPL.

- **History.** `STAGE5_DESIGN.md` lists it as a capability to preserve. It is
  also the one thing that legitimately wants client storage, so decision 3's
  "the console stores nothing" is scoped to **job state** — history is not job
  state.

  But client storage is keyed by origin *including port*, and `Options.Port`
  defaults to zero meaning "ask the OS for a free one" (`server.go:20`,
  `command.go:172`). So without `--port` the origin changes on every start and
  history is lost each time — the capability does not survive its own default —
  and with `--port` the store is shared with every other local service that has
  ever bound that number, in both directions, while command history holds config
  and state paths. Neither is acceptable as-is. **History belongs server-side**,
  beside `session.json`, which the console already has a persistence story for
  and which no other origin can read.
- **Arrow keys** move the completion popup's active descendant when it is open
  and walk history when it is not. One key, two subsystems, and the precedence
  has to be written down rather than discovered.
- **Ctrl-C is cancel.** Decision 9 made cancel a console feature and bound no
  key; on a REPL Ctrl-C *is* the gesture, and in a browser it is Copy. The rule:
  with a selection, copy; with none, cancel **the job the input line is
  addressing** — not "the running job", which the concurrency bullet below makes
  ambiguous. Silently cancelling the wrong `run` is worse than binding no key,
  so with more than one job live the key asks which.
- **Typing survives a streaming job.** The input is never disabled during a run
  and never re-rendered on a progress event. Autoscroll only when already
  pinned to the bottom — appending and scrolling while an operator reads
  scrollback yanks them away from what they were reading. Focus never moves.
- **Concurrent jobs from one tab are permitted**, because a live input line plus
  "everything is a job" already implies it and refusing would need a rule the
  engine does not have. Two `status` calls are harmless; two `run`s against one
  target are what the target lease exists for. The transcript groups output by
  job so two streams cannot interleave into nonsense.

  This is load-bearing on two things written elsewhere as singular, and both
  have to become per-job: decision 13's "progress mutates one node" means one
  node **per live job**, and the accessibility section's single `role="status"`
  region cannot serve two — with `aria-atomic="true"` the second job overwrites
  the first mid-sentence. Either the live region announces only the addressed
  job, or concurrency is refused. It cannot be both.

### 13. The transcript is bounded

Also absent from both drafts, and the outputs are not small.

- Progress **mutates one node per live job**; it never appends. A 200-table
  migration emits 401 events, and a node each would grow the DOM without limit.
  This is the DOM half of the rule whose `aria-live` half is below.
- The transcript caps its entries, dropping oldest, and truncates a single
  oversized block with a visible marker. `history` returns every run ever
  recorded as one payload, and `history_retention_days` still has no consumer —
  a multi-megabyte text node is a hang, not a scroll.
- One reconnect can deliver up to `maxRetainedEvents` frames at once, so the
  cap has to survive a burst rather than only steady growth.

## Accessibility

- One `role="status" aria-live="polite" aria-atomic="true"` region updated on
  `tables_planned` and on `finished` — **not per table**. A live region that
  updates per progress event is unusable with a screen reader. (See decision 12
  on what "one region" means once two jobs can run at once.)
- **`tables_planned` may never arrive.** Trimming keeps `events[0]` plus the
  tail, so a reconnect to a long run replays without it — the tracked defect —
  and the region and the progress denominator both need a stated fallback for
  "the planned set never arrived" rather than rendering `0` or `NaN`. "Nothing
  may assume contiguity" was too weak: a *specific* event can be missing.
- The transcript is `aria-live="polite"` for completed output only.
- The `/` completion popup is a listbox-over-textbox widget needing
  `role="combobox"`, `aria-expanded`, `aria-controls`, `aria-activedescendant`,
  and arrow keys that move the active descendant without moving focus. This is
  more work than the progress line and is where screen-reader users actually get
  stuck — which is half the reason `@` is deferred rather than built alongside
  it. One such widget, done properly, beats two done at the same cost.

## What is testable, and what is not

Worth writing, cheap, high value — all Go, all against the shipped bytes:

- **No HTML construction**: scan the embedded assets for `innerHTML`,
  `outerHTML`, `insertAdjacentHTML`, `document.write`, `eval`, `new Function`.
  Worth writing — but the earlier claim that it "converts decision 4 from a
  convention into an enforced property" was an overclaim. It enforces a set of
  *spellings*: `el["inner"+"HTML"]`, `createContextualFragment`, `srcdoc`, and
  `<template>` cloning all pass a substring grep. Real enforcement is
  `require-trusted-types-for 'script'` in the CSP, which is testable as a header
  — a test this section already plans.
- **Every `/api/v1/...` string in the JS is a route the mux registers.**
  Otherwise a typo'd endpoint is a runtime-only failure. A literal scan cannot
  do this: the JS holds `` `/api/v1/jobs/${id}/events` ``, so a scan finds
  `/api/v1/jobs/` and `/events`, neither a registered pattern — and the three
  templated routes are the ones most likely to be typo'd. `http.ServeMux` also
  cannot enumerate its patterns. The workable form is `mux.Handler(req)` against
  synthesised URLs, which requires **the JS to declare its route table as
  data**. Written any other way this test ships checking only the four static
  routes while looking like it checks all thirteen.
- **Every served asset carries `Content-Type`, `nosniff` and the CSP**, and the
  HTML references no off-origin URL.
- **The `retry:` line on an exhausted stream**, as bytes — `job_test.go`
  already asserts frame text, so this costs one case.
- **A trimmed buffer's sequence hole**, so nothing later assumes contiguity.
- The `started` event echoing the resolved request, once it exists.

Not testable from Go, and this note says so rather than implying otherwise: the
rendered command list and `/help`, keyboard handling, completion popup
behaviour, and `aria-live`. The reconnect lifecycle and 401 handling belong here
only as *browser* halves — their server halves are as testable as anything else,
and an earlier draft filed them as untestable wholesale while filing the stream's
closing bytes as testable, which cannot both be true. If a surface
parity test is written anyway, **its comment must say it asserts the API
surface and not the rendered one** — or it becomes the next test that compares a
function against itself.

## What this note does not decide

Named here so the gap is visible rather than discovered during implementation:

- **The config editor and the secret-store editor**, which the goal at the top
  requires and which need YAML round-tripping through `yaml.Node`. Their own
  note.
- **The setup wizard** that makes a first run possible with no terminal, which
  is where the empty state above leads.
- **Where profiles fit**, once a config can be edited from the panel that would
  also save one.

## Still open

- **Whether the console shows jobs it did not start** (decision 3). This is the
  one genuinely open question left, and it decides whether the reload story
  works.
- **`@` completion is deferred, not cut.** `Request` has exactly two path
  fields, so there is no "other file an operator references" in the registered
  surface yet — the justification for `@` was a claim about a future that is not
  registered. Discovery filters by content and needs no keystroke protocol, and
  deferring `@` removes one of the two combobox widgets, which is where the
  accessibility cost actually sits. The endpoint is already built and tested;
  not shipping a UI over it removes nothing. There is **no precondition**, contrary to an
  earlier draft: `/session state PATH` already names a state file with no
  completion at all — `sessionKeys` holds both `config` and `state`
  (`session.go:22`) and `applyTo` fills `Request.StatePath` from it
  (`session.go:191`). What deferring `@` costs is convenience for an operator
  who does not know the path, which is what discovery and the panels are for.
  Gating this PR on an unrelated tracked defect was wrong. (Decision 7 should
  name the `state` key too; it mentions only `config`, and `session.go:31`
  exists precisely so a console does not hard-code that list.)
- **The service worker must cache the shell only and never `/api/v1/*`.** A
  cached `GET /api/v1/jobs/{id}` would show a finished migration as running.
  This is a correctness requirement rather than a performance note, and it
  belongs in the PWA work.
