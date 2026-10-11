#!/usr/bin/env bash
# Probe the isolated Stage-2 stack from inside its private network.
# Prints status codes and topology checks only. Does not print bearers.
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose=(docker compose -f "$root/compose/stage2-isolated/docker-compose.yml")
network=votari-stage2-isolated
curl_image=curlimages/curl:8.12.1
callback_path=/api/v1/merkle-service/callback
body='{"type":"SEEN_ON_NETWORK","txids":["0000000000000000000000000000000000000000000000000000000000000001"]}'

token=$(awk '/^callback_token:/ { print $2; exit }' "$root/compose/stage2-isolated/arcade-config.yaml")
if [[ -z "$token" || "$token" != synthetic-stage2-* ]]; then
  echo "callback fixture is missing or is not a synthetic-stage2 value" >&2
  exit 1
fi

net_curl() {
  docker run --rm --network "$network" "$curl_image" "$@"
}

wait_http() {
  local url=$1
  local accept=$2
  local code=""
  for _ in $(seq 1 60); do
    code=$(net_curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$url" || true)
    case " $accept " in
      *" $code "*) printf '%s' "$code"; return 0 ;;
    esac
    sleep 2
  done
  echo "timed out waiting for $url (last status ${code:-none})" >&2
  return 1
}

if published=$("${compose[@]}" port votari-arcade 8080 2>/dev/null); then
  echo "callback port is published on the host: $published" >&2
  exit 1
fi
if published=$("${compose[@]}" port votari-stage2-merkle 8080 2>/dev/null); then
  echo "merkle port is published on the host: $published" >&2
  exit 1
fi
echo "PRIVATE_CALLBACK_HOST_PORT=unpublished"
echo "MERKLE_HOST_PORT=unpublished"

arcade_health=$(wait_http "http://votari-arcade:8080/health" "200")
echo "ARCADE_HEALTH=$arcade_health"
merkle_health=$(wait_http "http://votari-stage2-merkle:8080/health" "200 503")
echo "MERKLE_HEALTH=$merkle_health"

post_callback() {
  local auth_args=()
  if [[ $# -gt 0 ]]; then
    auth_args=(-H "Authorization: Bearer $1")
  fi
  net_curl -s -o /dev/null -w '%{http_code}' --max-time 10 \
    -X POST "http://votari-arcade:8080${callback_path}" \
    -H 'Content-Type: application/json' \
    "${auth_args[@]}" \
    -d "$body"
}

missing=$(post_callback)
wrong=$(post_callback "synthetic-stage2-wrong-bearer")
correct=$(post_callback "$token")
echo "CALLBACK_MISSING_BEARER=$missing"
echo "CALLBACK_WRONG_BEARER=$wrong"
echo "CALLBACK_CORRECT_BEARER=$correct"

if [[ "$missing" != "401" || "$wrong" != "401" || "$correct" != "200" ]]; then
  echo "callback authentication did not match 401/401/200" >&2
  exit 1
fi

logs=$("${compose[@]}" logs --no-color votari-arcade votari-stage2-merkle || true)
if grep -F -q -- "$token" <<<"$logs"; then
  echo "callback bearer appeared in container logs" >&2
  exit 1
fi
if grep -F -q -- "synthetic-stage2-merkle-auth-bearer" <<<"$logs"; then
  echo "merkle auth bearer appeared in container logs" >&2
  exit 1
fi
echo "SECRET_IN_LOGS=absent"

if grep -E -q 'bsvb\.tech|dnsaddr/' <<<"$logs"; then
  echo "external bootstrap hostname appeared in container logs" >&2
  exit 1
fi
echo "EXTERNAL_BOOTSTRAP_CONNECTIONS=0"

"${compose[@]}" restart votari-arcade votari-stage2-merkle >/dev/null
restart_health=$(wait_http "http://votari-arcade:8080/health" "200")
restart_correct=$(post_callback "$token")
echo "RESTART_HEALTH=$restart_health"
echo "RESTART_CORRECT_BEARER=$restart_correct"
if [[ "$restart_correct" != "200" ]]; then
  echo "callback after restart was not accepted" >&2
  exit 1
fi
