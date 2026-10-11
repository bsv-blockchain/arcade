#!/usr/bin/env bash
# Provenance-qualified Arcade image build.
#
# Fails closed when the source revision is missing or not a full commit, the
# working tree is dirty, ARCADE_EXPECTED_REVISION does not match HEAD, or the
# binary/image metadata cannot be embedded. Ordinary developer builds stay on
# `make build` and `make docker-build`.
#
# Does not push the image anywhere.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"
# shellcheck source=provenance-preflight.sh
source "$root/scripts/provenance-preflight.sh"

if [[ "$(go env GOOS)" != "linux" ]]; then
  echo "provenance-build requires a Linux host: CGO + gobdk cannot cross-compile a linux binary from $(go env GOOS)." >&2
  exit 1
fi

provenance_preflight
rev=$PROVENANCE_REVISION
source_url=$PROVENANCE_SOURCE

version=${ARCADE_VERSION:-$(git describe --tags --always)}
if [[ -n "${SOURCE_DATE_EPOCH:-}" ]]; then
  created=$(date -u -d "@${SOURCE_DATE_EPOCH}" +%Y-%m-%dT%H:%M:%SZ)
else
  created=$(date -u +%Y-%m-%dT%H:%M:%SZ)
fi

arch=$(go env GOARCH)
mkdir -p "dist/linux-${arch}" dist/provenance
bin="dist/linux-${arch}/arcade"

CGO_ENABLED=1 GOOS=linux GOARCH="$arch" go build \
  -trimpath \
  -buildvcs=true \
  -ldflags="-s -w -X github.com/bsv-blockchain/arcade/version.Version=${version}" \
  -o "$bin" \
  ./cmd/arcade

info=$(go version -m "$bin")
embedded_rev=$(printf '%s\n' "$info" | awk -F= '/[[:space:]]vcs\.revision=/{print $2; exit}')
embedded_mod=$(printf '%s\n' "$info" | awk -F= '/[[:space:]]vcs\.modified=/{print $2; exit}')
if [[ "$embedded_rev" != "$rev" || "$embedded_mod" != "false" ]]; then
  echo "refusing provenance build: embedded VCS revision=${embedded_rev:-<missing>} modified=${embedded_mod:-<missing>}, want ${rev} modified=false" >&2
  exit 1
fi

docker build \
  --platform="linux/${arch}" \
  --build-arg "ARCADE_IMAGE_SOURCE=${source_url}" \
  --build-arg "ARCADE_REVISION=${rev}" \
  --build-arg "ARCADE_CREATED=${created}" \
  --build-arg "ARCADE_VERSION=${version}" \
  -t arcade:provenance \
  .

digest=$(docker image inspect --format '{{.Id}}' arcade:provenance)
source_label=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.source"}}' arcade:provenance)
revision_label=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' arcade:provenance)
created_label=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.created"}}' arcade:provenance)
version_label=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' arcade:provenance)
if [[ "$source_label" != "$source_url" || "$revision_label" != "$rev" || "$created_label" != "$created" || "$version_label" != "$version" ]]; then
  echo "refusing provenance build: image labels do not match the source stamp" >&2
  printf 'source=%q revision=%q created=%q version=%q\n' "$source_label" "$revision_label" "$created_label" "$version_label" >&2
  exit 1
fi

syft=$(bash scripts/install-syft.sh)
"$syft" arcade:provenance -o spdx-json=dist/provenance/sbom.spdx.json
sbom_hash=$(sha256sum dist/provenance/sbom.spdx.json | awk '{print $1}')
if [[ ! "$sbom_hash" =~ ^[0-9a-f]{64}$ ]]; then
  echo "refusing provenance build: SBOM hash is missing" >&2
  exit 1
fi

python3 - "$rev" "$source_url" "$version" "$created" "$digest" "$embedded_rev" "$sbom_hash" <<'PY'
import json, sys
rev, source, version, created, local_id, go_rev, sbom_hash = sys.argv[1:]
doc = {
    "source_repository": source,
    "source_revision": rev,
    "source_clean": True,
    "oci_source": source,
    "oci_revision": rev,
    "oci_created": created,
    "oci_version": version,
    "go_vcs_revision": go_rev,
    "go_vcs_modified": False,
    "application_version": version,
    "image_reference": "arcade:provenance",
    "local_image_id": local_id,
    "registry_manifest_digest": None,
    "sbom": "sbom.spdx.json",
    "sbom_sha256": sbom_hash,
    "build_timestamp": created,
}
with open("dist/provenance/release-manifest.json", "w", encoding="utf-8") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
PY

echo "provenance image arcade:provenance"
echo "revision ${rev}"
echo "local_image_id ${digest}"
echo "registry_manifest_digest none"
echo "manifest dist/provenance/release-manifest.json"
echo "sbom dist/provenance/sbom.spdx.json"
