#!/usr/bin/env bash
# Prove the provenance path rejects bad evidence. Does not leave the worktree dirty.
# Requires a clean checkout. Does not build an image.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
# shellcheck source=provenance-preflight.sh
source "$root/scripts/provenance-preflight.sh"

if [[ -n "$(git status --porcelain)" ]]; then
  echo "negative checks need a clean worktree" >&2
  exit 1
fi

expect_reject() {
  local needle=$1
  shift
  local out code
  set +e
  out=$("$@" 2>&1)
  code=$?
  set -e
  if [[ "$code" -eq 0 ]]; then
    echo "expected rejection (${needle}) but the check succeeded" >&2
    exit 1
  fi
  if [[ "$out" != *"$needle"* ]]; then
    echo "rejection did not report: ${needle}" >&2
    printf '%s\n' "$out" >&2
    exit 1
  fi
}

expect_reject "not a full commit SHA" \
  env ARCADE_EXPECTED_REVISION=abc bash -c 'source "$1"; provenance_preflight' _ "$root/scripts/provenance-preflight.sh"

expect_reject "does not match ARCADE_EXPECTED_REVISION" \
  env ARCADE_EXPECTED_REVISION=0000000000000000000000000000000000000000 bash -c 'source "$1"; provenance_preflight' _ "$root/scripts/provenance-preflight.sh"

marker=$root/.provenance-negative-dirty
cleanup() { rm -f "$marker"; }
trap cleanup EXIT
printf 'temporary\n' >"$marker"
expect_reject "working tree is dirty" \
  bash -c 'source "$1"; provenance_preflight' _ "$root/scripts/provenance-preflight.sh"
cleanup
trap - EXIT

if [[ -n "$(git status --porcelain)" ]]; then
  echo "negative checks left the worktree dirty" >&2
  git status --porcelain >&2
  exit 1
fi

echo "negative fail-closed checks passed"
