#!/usr/bin/env bash
# Verify a locally built provenance image. Does not contact a registry.
#
# Usage: scripts/verify-image-provenance.sh [image]
# Default image is arcade:provenance. The release manifest and SBOM written
# by scripts/provenance-build.sh are read from dist/provenance/.
#
# local_image_id is the daemon's image ID. registry_manifest_digest stays
# empty unless an image was pushed; this script does not treat them as the
# same value.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

image=${1:-arcade:provenance}
manifest=dist/provenance/release-manifest.json
sbom=dist/provenance/sbom.spdx.json

fail() {
  echo "provenance verification failed: $*" >&2
  exit 1
}

[[ -s "$manifest" ]] || fail "release manifest $manifest is missing"
[[ -s "$sbom" ]] || fail "SBOM $sbom is missing"
grep -q '"spdxVersion"' "$sbom" || fail "SBOM is not SPDX JSON"
grep -q 'arcade' "$sbom" || fail "SBOM does not identify arcade"

eval "$(python3 - "$manifest" <<'PY'
import json, shlex, sys
doc = json.load(open(sys.argv[1], encoding="utf-8"))
registry = doc.get("registry_manifest_digest")
fields = {
    "MANIFEST_REVISION": doc.get("source_revision", ""),
    "MANIFEST_SOURCE": doc.get("source_repository", ""),
    "MANIFEST_CLEAN": "true" if doc.get("source_clean") is True else "false",
    "MANIFEST_OCI_REVISION": doc.get("oci_revision", ""),
    "MANIFEST_OCI_CREATED": doc.get("oci_created", ""),
    "MANIFEST_OCI_VERSION": doc.get("oci_version", ""),
    "MANIFEST_GO_REVISION": doc.get("go_vcs_revision", ""),
    "MANIFEST_GO_MODIFIED": "true" if doc.get("go_vcs_modified") is True else "false",
    "MANIFEST_APP_VERSION": doc.get("application_version", ""),
    "MANIFEST_IMAGE": doc.get("image_reference", ""),
    "MANIFEST_LOCAL_ID": doc.get("local_image_id", ""),
    "MANIFEST_REGISTRY": "" if registry in (None, "") else str(registry),
    "MANIFEST_SBOM": doc.get("sbom", ""),
    "MANIFEST_SBOM_SHA": doc.get("sbom_sha256", ""),
    "MANIFEST_CREATED": doc.get("build_timestamp", ""),
}
for key, value in fields.items():
    print(f"{key}={shlex.quote(str(value))}")
PY
)"

local_id=$(docker image inspect --format '{{.Id}}' "$image")
oci_source=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.source"}}' "$image")
oci_revision=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image")
oci_created=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.created"}}' "$image")
oci_version=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.version"}}' "$image")
sbom_hash=$(sha256sum "$sbom" | awk '{print $1}')

[[ "$MANIFEST_CLEAN" == "true" ]] || fail "manifest source_clean is not true"
[[ "$MANIFEST_REGISTRY" == "" ]] || fail "manifest registry digest is set, but this check does not push"
[[ "$local_id" == "$MANIFEST_LOCAL_ID" && "$local_id" == sha256:* ]] || fail "local image id $local_id does not match manifest"
[[ "$oci_source" == "$MANIFEST_SOURCE" && "$oci_source" == https://* ]] || fail "OCI source label is $oci_source"
[[ "$oci_revision" == "$MANIFEST_REVISION" && "$oci_revision" == "$MANIFEST_OCI_REVISION" ]] || fail "OCI revision $oci_revision does not match source $MANIFEST_REVISION"
[[ "$oci_revision" =~ ^[0-9a-f]{40}$ ]] || fail "OCI revision is not a full commit SHA"
[[ -n "$oci_created" && "$oci_created" == "$MANIFEST_OCI_CREATED" && "$oci_created" == "$MANIFEST_CREATED" ]] || fail "OCI created label does not match the build timestamp"
[[ -n "$oci_version" && "$oci_version" == "$MANIFEST_OCI_VERSION" && "$oci_version" == "$MANIFEST_APP_VERSION" ]] || fail "OCI version label does not match the release version"
[[ "$MANIFEST_SBOM" == "sbom.spdx.json" ]] || fail "manifest SBOM name is $MANIFEST_SBOM"
[[ "$sbom_hash" == "$MANIFEST_SBOM_SHA" && "$sbom_hash" =~ ^[0-9a-f]{64}$ ]] || fail "SBOM sha256 does not match the manifest"
[[ "$MANIFEST_IMAGE" == "$image" || "$MANIFEST_IMAGE" == "arcade:provenance" ]] || fail "image reference $MANIFEST_IMAGE"

app_out=$(docker run --rm "$image" version)
app_version=$(printf '%s\n' "$app_out" | awk -F= '/^version=/{print $2; exit}')
app_revision=$(printf '%s\n' "$app_out" | awk -F= '/^revision=/{print $2; exit}')
app_modified=$(printf '%s\n' "$app_out" | awk -F= '/^modified=/{print $2; exit}')
[[ "$app_version" == "$MANIFEST_APP_VERSION" ]] || fail "arcade version $app_version does not match release version $MANIFEST_APP_VERSION"
[[ "$app_revision" == "$oci_revision" ]] || fail "arcade revision $app_revision does not match OCI revision $oci_revision"
[[ "$app_modified" == "false" ]] || fail "arcade version reports modified=$app_modified"

workdir=$(mktemp -d)
trap 'rm -rf "$workdir"' EXIT
cid=$(docker create "$image")
docker cp "$cid:/usr/local/bin/arcade" "$workdir/arcade"
docker rm "$cid" >/dev/null
info=$(go version -m "$workdir/arcade")
go_rev=$(printf '%s\n' "$info" | awk -F= '/[[:space:]]vcs\.revision=/{print $2; exit}')
go_mod=$(printf '%s\n' "$info" | awk -F= '/[[:space:]]vcs\.modified=/{print $2; exit}')

[[ "$go_rev" == "$oci_revision" && "$go_rev" == "$MANIFEST_GO_REVISION" ]] || fail "Go vcs.revision $go_rev does not match OCI revision $oci_revision"
[[ "$go_mod" == "false" && "$MANIFEST_GO_MODIFIED" == "false" ]] || fail "Go vcs.modified is ${go_mod:-<missing>}"

printf 'SOURCE_REVISION=%s\n' "$oci_revision"
printf 'SOURCE_CLEAN_STATE=clean\n'
printf 'OCI_SOURCE=%s\n' "$oci_source"
printf 'OCI_REVISION=%s\n' "$oci_revision"
printf 'OCI_CREATED=%s\n' "$oci_created"
printf 'OCI_VERSION=%s\n' "$oci_version"
printf 'GO_VCS_REVISION=%s\n' "$go_rev"
printf 'GO_VCS_MODIFIED=%s\n' "$go_mod"
printf 'APPLICATION_VERSION=%s\n' "$app_version"
printf 'APPLICATION_REVISION=%s\n' "$app_revision"
printf 'LOCAL_IMAGE_ID=%s\n' "$local_id"
printf 'REGISTRY_MANIFEST_DIGEST=none\n'
printf 'SBOM_PRESENT=true\n'
printf 'SBOM_SHA256=%s\n' "$sbom_hash"
