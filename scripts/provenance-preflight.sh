#!/usr/bin/env bash
# Fail-closed source checks for a provenance-qualified build.
# Source this file and call provenance_preflight. On success it sets
# PROVENANCE_REVISION and PROVENANCE_SOURCE. It does not build or push.

provenance_preflight() {
  local rev dirty remote source_url
  rev=$(git rev-parse --verify HEAD)
  if [[ ! "$rev" =~ ^[0-9a-f]{40}$ ]]; then
    echo "refusing provenance build: revision ${rev} is not a full commit SHA" >&2
    return 1
  fi
  dirty=$(git status --porcelain)
  if [[ -n "$dirty" ]]; then
    echo "refusing provenance build: working tree is dirty" >&2
    printf '%s\n' "$dirty" >&2
    return 1
  fi
  if [[ -n "${ARCADE_EXPECTED_REVISION:-}" ]]; then
    if [[ ! "${ARCADE_EXPECTED_REVISION}" =~ ^[0-9a-f]{40}$ ]]; then
      echo "refusing provenance build: ARCADE_EXPECTED_REVISION is not a full commit SHA" >&2
      return 1
    fi
    if [[ "${ARCADE_EXPECTED_REVISION}" != "$rev" ]]; then
      echo "refusing provenance build: HEAD ${rev} does not match ARCADE_EXPECTED_REVISION ${ARCADE_EXPECTED_REVISION}" >&2
      return 1
    fi
  fi

  source_url=${ARCADE_IMAGE_SOURCE:-}
  if [[ -z "$source_url" ]]; then
    remote=$(git remote get-url origin 2>/dev/null || true)
    remote=${remote%.git}
    if [[ "$remote" =~ ^git@([^:]+):(.+)$ ]]; then
      source_url="https://${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    elif [[ "$remote" =~ ^ssh://git@([^/]+)/(.+)$ ]]; then
      source_url="https://${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
    elif [[ "$remote" =~ ^https:// ]]; then
      source_url=$remote
    fi
  fi
  if [[ ! "$source_url" =~ ^https://[^[:space:]]+$ ]]; then
    echo "refusing provenance build: set ARCADE_IMAGE_SOURCE to the https source repository URL" >&2
    return 1
  fi

  PROVENANCE_REVISION=$rev
  PROVENANCE_SOURCE=$source_url
}
