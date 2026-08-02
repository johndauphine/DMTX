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

Requirements for that endpoint:

- **Root-confined.** An explicit allowed root, with `..` traversal and absolute
  paths outside it refused. Resolve symlinks before checking containment.
- **Authenticated.** SSH access to the host is not authorization to run a
  destructive migration, and a forwarded port must not be treated as implicit
  trust. DMT already has `internal/webui/session.go` and a trusted-proxy module;
  carry those decisions over deliberately.
- **Non-leaking.** Errors must not disclose whether a path outside the root
  exists.

Implement this endpoint before the console component, so the console is built
against the confined API rather than retrofitted onto a permissive one.

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

## Suggested build order

**[proposed]**

1. Surface-agnostic command layer and JSON/WebSocket API — the parity seam.
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
