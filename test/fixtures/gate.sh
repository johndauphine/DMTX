#!/usr/bin/env bash
# Run the same gate .github/workflows/verify.yml runs, on this machine.
#
# Written because GitHub Actions went into a major outage - throttled to about
# fifteen percent of webhooks - and waiting a day for it is not a plan. The
# containers this needs are already the ones the armed gate uses, so the only
# thing missing was something that runs the whole sequence in the right order
# and refuses to be optimistic about the parts it skipped.
#
#   ./test/fixtures/gate.sh            offline checks, then armed live tests
#   ./test/fixtures/gate.sh --offline  offline only, no containers needed
#   ./test/fixtures/gate.sh --race     also the armed race run (slow; nightly in CI)
#
# It is not a replacement for CI and says so at the end. What it cannot do is
# prove the workflow file's own triggers work, which is exactly the thing that
# went unverified today - so a green run here is evidence about the code, not
# about the pipeline.

set -euo pipefail

fixtures_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
repo_root="$(cd "${fixtures_dir}/../.." && pwd)"
cd "${repo_root}"

offline_only=0
with_race=0
for argument in "$@"; do
  case "${argument}" in
    --offline) offline_only=1 ;;
    --race) with_race=1 ;;
    *) echo "unknown option ${argument}" >&2; exit 2 ;;
  esac
done

failures=()
step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# Each check records its own failure and keeps going. A gate that stops at the
# first problem makes you run it once per problem, and the whole point of
# running it locally is a short loop.
check() {
  local name="$1"; shift
  step "${name}"
  if "$@"; then
    printf '\033[32mPASS\033[0m %s\n' "${name}"
  else
    printf '\033[31mFAIL\033[0m %s\n' "${name}"
    failures+=("${name}")
  fi
}

# ---------- offline: no endpoints, live tests skip themselves ----------

check "build"                go build ./...
check "vet"                  go vet ./...
check "offline tests"        go test ./... -count=1
check "offline race"         go test -race ./... -count=1
check "unused code"          golangci-lint run
check "linux build"          go build -o /tmp/dmtx-gate ./cmd/dmtx
check "windows cross-build"  env GOOS=windows GOARCH=amd64 go build -o /tmp/dmtx-gate.exe ./cmd/dmtx

# The workflow files themselves. CI cannot check these while it is down, and
# today proved an unverified workflow change is worse than a wasteful one.
if command -v actionlint >/dev/null 2>&1; then
  check "workflow lint" actionlint
fi

if [ "${offline_only}" -eq 1 ]; then
  printf '\n\033[1m-- offline only --\033[0m\n'
  printf 'Live endpoints were not touched, so every live test skipped.\n'
  printf 'That is most of what this repository proves. Run without --offline.\n'
  [ "${#failures[@]}" -eq 0 ] || { printf '\n\033[31m%d failed\033[0m\n' "${#failures[@]}"; exit 1; }
  exit 0
fi

# ---------- armed: the part that actually proves the product ----------

step "fixtures"
healthy=$(docker ps --filter 'name=dmtx-' --filter 'health=healthy' --quiet | wc -l | tr -d ' ')
if [ "${healthy}" -ne 5 ]; then
  echo "only ${healthy} of 5 fixtures are healthy; starting them"
  (cd "${fixtures_dir}" && docker compose up -d)
  for _ in $(seq 1 120); do
    healthy=$(docker ps --filter 'name=dmtx-' --filter 'health=healthy' --quiet | wc -l | tr -d ' ')
    [ "${healthy}" -eq 5 ] && break
    sleep 5
  done
fi
if [ "${healthy}" -ne 5 ]; then
  docker ps -a --filter 'name=dmtx-' --format '{{.Names}}\t{{.Status}}'
  echo "fixtures did not become healthy" >&2
  exit 1
fi
echo "all five fixtures healthy"

check "provision" bash "${fixtures_dir}/provision.sh"

# shellcheck source=/dev/null
source "${fixtures_dir}/env.sh"

# DMTX_STAGE4_LIVE_REQUIRED=1 is the point. Without it a missing endpoint skips
# and the run still reports success, which is the failure mode the gate exists
# to prevent.
check "armed live tests" env DMTX_STAGE4_LIVE_REQUIRED=1 \
  go test ./... -count=1 -timeout 30m

if [ "${with_race}" -eq 1 ]; then
  check "armed race tests" env DMTX_STAGE4_LIVE_REQUIRED=1 \
    go test -race ./... -count=1 -timeout 30m
fi

# ---------- what this run did not cover ----------
#
# The same habit the workflow has: say what was skipped rather than let a green
# result imply more than it earned.

printf '\n\033[1m== what this run did not cover ==\033[0m\n'
if [ "${with_race}" -eq 0 ]; then
  printf '  - the armed race run (pass --race; it is the slow half)\n'
fi
printf '  - whether the workflow triggers fire, which only GitHub can answer\n'
printf '  - a clean checkout: this ran against the working tree, so anything\n'
printf '    uncommitted or untracked was part of the result\n'

if [ "${#failures[@]}" -ne 0 ]; then
  printf '\n\033[31m%d check(s) failed:\033[0m\n' "${#failures[@]}"
  printf '  %s\n' "${failures[@]}"
  exit 1
fi
printf '\n\033[32mall checks passed\033[0m\n'
