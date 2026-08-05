# Console design note

Written 2026-08-05, revised after review before any code was written. Settled
parts fold into `docs/STAGE5_DESIGN.md`; this is the working draft.

The review rejected three of the eight original decisions outright and found two
defects in already-merged code. Both defects are tracked separately; this note
records only what the console needs.

## What it is

A **command console in a browser** — an input line, a scrolling transcript, a
status bar. Not a dashboard.

DMT's *TUI* is a REPL; DMT's *web UI* is a dashboard of forms. DMTX takes the
TUI's shape and the web UI's **capabilities**, which is what the nine-command
audit established. An operator who knows the command line should not have to
learn a second model.

## Constraints already fixed

Vanilla JS and CSS served by `go:embed` — no build step, no npm in CI, one
self-contained binary that still cross-compiles. Authenticated by the session
cookie `/login` sets. Loopback only.

## The console needs API changes first

Four, and they are the difference between a console whose correctness lives in
Go and one whose correctness lives in untestable JavaScript. They should land
as their own PR before any frontend work.

1. **`GET /api/v1/commands` returns what the console should show**: already
   filtered to non-`Omitted`, alias-expanded, and carrying `contract.Command.Note`
   and a description. Today it returns names, aliases and disposition only, so a
   `/help` built from it is strictly worse than the CLI's `helpLines()`. Moving
   the decision server-side makes "which commands does the console offer" a Go
   function with a Go test, and leaves the JS a `for` loop.

2. **`GET /api/v1/jobs`** — list them. There is no way to find a running job
   today except an id the client kept, which is why decision 3 below changed.

3. **The SSE stream answers `204` to a reconnect that has already seen
   everything.** A `200` with an empty body leaves `EventSource` reconnecting
   every three seconds forever; the HTML spec makes any non-`200` fatal to it.
   This removes the hazard rather than asking the console to remember
   `close()` — and it is Go-testable, where "the console remembers" is not.

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
and stores nothing. This also answers the second-tab question — tabs are views,
never owners — and makes a server restart show an honest empty list rather than
a phantom.

One caveat to design around: `forgetFinished` runs only when a job starts, so
retention is best-effort. **The console must never read absence as "it did not
happen."**

### 4. No HTML is ever constructed from data

Not "output is escaped" — the stronger property: no `innerHTML`,
`insertAdjacentHTML`, `document.write`, `eval` or `new Function` anywhere in the
shipped assets. Everything is `textContent` and `createElement`.

The risk is not confined to messages: the boxed renderer builds DOM from payload
keys *and* values, the progress line carries table names, and every one of those
ultimately comes from a database dmtx was pointed at. DMT's `app.js` does not
hold this property — it is `innerHTML` throughout with a hand-rolled `esc()` —
so there is prior art to avoid here rather than copy.

Two things this needs alongside it:

- **A Content-Security-Policy header** on the console page:
  `default-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none';
  frame-ancestors 'none'`. `nosniff` protects JSON responses and does nothing
  for the page. `frame-ancestors 'none'` matters for a page that starts
  migrations.
- **`white-space: pre-wrap`**, or `textContent` collapses the newlines in
  multi-line command output and aligned CLI output stops aligning.

### 5. Most output is rendered by the server, not by the console

Rejected from the first draft: a JS renderer per payload kind, with a raw-JSON
fallback for unknown kinds.

Two things were wrong with it. The claim that "the payload kinds are already
pinned by wire-shape tests, so the renderer keys off `Payload.Kind`" cited a
test on the **Go** side as coverage for the **JS** side; the wire-shape tests
establish nothing about whether a renderer exists or reads the right fields. And
the raw-JSON fallback is a redaction hole that no test can close, since its
input set is by definition the shapes nobody enumerated — against a §21.2
obligation that names WebUI responses explicitly.

Instead: `app.RenderText` already produces byte-identical CLI output from an
`Outcome`, and `TestRenderTextReproducesTheOriginalByteStream` already holds it.
Most output goes through that and lands in a `<pre>`. The console matches the
command line **by construction**, on a path that is already tested.

Two kinds get real tables, because an operator reads them while deciding rather
than afterwards: `plan` and `preflight_report`. An unrecognised kind prints its
name and a line saying this dmtx cannot display it — never its contents.

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
connecting to the database during preflight or the run itself. And the engine's
gate only fires for `target_mode: drop_recreate`, so a console implementing the
stated rule would confirm on most runs that would never have been gated. A
confirmation that is usually a false alarm trains exactly the click-through it
was meant to prevent.

Instead the console sends `acknowledge_destructive: false` and lets the engine
refuse. That refusal — which already names the specific table containing rows —
*is* the prompt. It cannot appear for a run the engine would not gate, cannot be
skipped for one it would, and adds no second policy that could disagree with the
engine.

The re-run gesture is **typing, not clicking**: `/run --acknowledge-destructive`
retyped. Not an OK button.

Related fix: the engine's refusal says "rerun with `--acknowledge-destructive`",
naming a flag an operator in a browser cannot type.

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

- **The server is gone.** `auth.session` lives in memory, the launch token was
  consumed at first load, and there is no login form. When the idle watchdog
  fires — 30 minutes, by design — or the server restarts, the tab's cookie
  authenticates against a secret that no longer exists. Every request 401s, and
  `GET /` is itself behind auth, so a reload gives a bare page reading
  "authentication required". The console must recognise this and say plainly
  that the server it was talking to has stopped and `dmtx serve` will start a
  new one with a new link. **The 401 body must become JSON** like every other
  route; today it is `text/plain` from `http.Error`, so a fetch wrapper
  expecting `{error}` throws something unrelated.
- **Nothing configured yet.** `dmtx serve` in a fresh directory discovers no
  configs and has no default, so the first command produces `read configuration:
  open : no such file or directory` — an error naming an empty path, as the
  first thing a new operator sees. Zero discovered configs is an empty state
  that points at `init`.
- **Started, nothing to report.** A run acquires a lease, fences state and
  computes identities before the first progress event, which on a slow source is
  minutes. `--dry-run` emits no progress events at all. The progress line needs
  a state for this that is not a stalled `0/0`.
- **The outcome arrives twice** — once in the `finished` frame, once from
  `GET /api/v1/jobs/{id}` on reconnect. Render one.

### 11. The idle timeout has to be chosen, not inherited

An `EventSource` left open reconnects every three seconds, and every reconnect
is a request that `activity.track` counts — so a console with a stale stream
keeps the server alive forever and `--idle-timeout` never fires. API change 3
removes that particular loop, but the question it exposes is real and unanswered:

An operator reading a 200-table plan for 31 minutes makes no requests, so the
server stops and takes their session with it. `STAGE5_DESIGN.md` justified
restarting the idle clock on completion with "otherwise a run longer than the
timeout would be followed by an immediate shutdown while the operator is still
reading the result" — but reading the result is precisely the state that
generates no requests.

Either the console heartbeats, and the idle timeout means nothing while a tab is
open; or it does not, and reading is punished. **This must be decided
deliberately and written down**, not left to whichever the implementation
happens to do.

## Accessibility

- One `role="status" aria-live="polite" aria-atomic="true"` region updated on
  `tables_planned` and on `finished` — **not per table**. A live region that
  updates per progress event is unusable with a screen reader.
- The transcript is `aria-live="polite"` for completed output only.
- The completion popups — both `/` and `@` — are listbox-over-textbox widgets
  needing `role="combobox"`, `aria-expanded`, `aria-controls`,
  `aria-activedescendant`, and arrow keys that move the active descendant
  without moving focus. This is more work than the progress line and is where
  screen-reader users actually get stuck.

## What is testable, and what is not

Worth writing, cheap, high value — all Go, all against the shipped bytes:

- **No HTML construction**: scan the embedded assets for `innerHTML`,
  `outerHTML`, `insertAdjacentHTML`, `document.write`, `eval`, `new Function`.
  This converts decision 4 from a convention into an enforced property, and is
  the single best test available here.
- **Every `/api/v1/...` string in the JS is a route the mux registers.**
  Otherwise a typo'd endpoint is a runtime-only failure.
- **Every served asset carries `Content-Type`, `nosniff` and the CSP**, and the
  HTML references no off-origin URL.
- The `204`-on-exhausted-reconnect behaviour, and the `started` event echoing
  the resolved request, once both exist.

Not testable from Go, and this note says so rather than implying otherwise: the
rendered command list and `/help`, keyboard handling, completion popup
behaviour, `aria-live`, the reconnect lifecycle, and 401 handling. If a surface
parity test is written anyway, **its comment must say it asserts the API
surface and not the rendered one** — or it becomes the next test that compares a
function against itself.

## Still open

- **The idle-timeout question in decision 11.** It needs an answer before the
  console ships.
- **Whether `@` completion and config discovery are both needed.** Discovery
  plus a session default covers the empty and steady states; `@` earns its place
  for the *other* files an operator references, but if it only ever carries
  config paths it duplicates the picker.
- **The service worker must cache the shell only and never `/api/v1/*`.** A
  cached `GET /api/v1/jobs/{id}` would show a finished migration as running.
  This is a correctness requirement rather than a performance note, and it
  belongs in the PWA work.
