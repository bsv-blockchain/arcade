# Container build provenance

This is the generic Arcade source-to-image contract. It identifies a build
from a clean commit. It does not sign the image, approve a release, or pin a
deployment.

## Threat model

A mutable tag such as `latest` does not name the source that was compiled.
A dirty working tree can ship uncommitted changes while still advertising a
commit. An image label can name a commit the binary was not built from.
Provenance-qualified builds fail closed on those cases. A developer build
does not.

What this contract does not do: key management, image signatures,
attestations, registry promotion, or production digest pinning. Those stay
with the organisation that publishes and runs the image.

## Source-to-image chain

```
clean committed source
  → full git commit
  → go build -trimpath -buildvcs=true
  → binary buildinfo (vcs.revision, vcs.modified)
  → OCI image labels
  → image digest
  → SPDX SBOM
  → release manifest linking commit, digest, and SBOM
```

`version.Version` remains the human version stamp (`dev`, or the release tag
passed with `-ldflags`). The commit is not copied into that variable. `arcade
version` prints both the stamp and the buildinfo revision.

## Build paths

`make build` and `make docker-build` are developer builds. They are allowed
to be dirty. `make docker-build` still requires a Linux host, because the
validator links gobdk with cgo and that archive cannot be cross-compiled.

`make provenance-build` is the provenance-qualified build. It refuses to
continue when any of these are true:

- the host is not Linux
- `HEAD` is not a 40-character lowercase hex commit
- `git status --porcelain` is non-empty
- `ARCADE_EXPECTED_REVISION` is set and is not `HEAD`
- `ARCADE_IMAGE_SOURCE` is missing and `origin` is not an https or git SSH URL
- the binary's `vcs.revision` is not `HEAD`, or `vcs.modified` is not `false`
- the image labels do not match that stamp
- syft cannot write an SPDX SBOM

The script tags the local image `arcade:provenance`. It does not push.

Optional inputs:

- `ARCADE_IMAGE_SOURCE` — https URL of the source repository. Derived from
  `origin` when unset. Do not bake a deployment-specific URL into the
  Dockerfile.
- `ARCADE_EXPECTED_REVISION` — commit the caller intended to build.
- `ARCADE_VERSION` — value stored in `version.Version` and the version label.
  Defaults to `git describe --tags --always`.
- `SOURCE_DATE_EPOCH` — unix time used for `org.opencontainers.image.created`.

## OCI labels

The runtime Dockerfile sets:

| Label | Value |
| --- | --- |
| `org.opencontainers.image.source` | https source repository |
| `org.opencontainers.image.revision` | full git commit used to compile |
| `org.opencontainers.image.created` | UTC RFC3339 build time |
| `org.opencontainers.image.version` | version stamp |

A developer `docker build` that omits the build args leaves those labels
empty. That image is not provenance-qualified.

The GitHub image workflow passes the same args from the checked-out commit
and `github.repository`. It also refuses a dirty checkout and a binary whose
embedded revision does not match that commit.

## Go VCS evidence

`go build -buildvcs=true` stamps `vcs.revision` and `vcs.modified` into the
binary. `-s -w` does not remove that buildinfo. Read it with:

```sh
go version -m path/to/arcade
arcade version
```

`version.ParseSettings` rejects a revision that is not 40 lowercase hex
characters, a modified flag other than `true` or `false`, and a modified flag
with no revision. A binary with no VCS keys is a developer build: revision
empty, not an error.

## Dirty-tree policy

A provenance-qualified build and the image-publishing workflow both stop when
the worktree is dirty. The embedded `vcs.modified=true` bit is a second
check, so a tree that became dirty between the status check and the compile
is still rejected. Developer builds may be dirty; `arcade version` then
reports `modified=true`.

## SBOM contract

The SBOM is SPDX JSON produced by syft 1.46.0 (or a syft already on `PATH`).
`scripts/install-syft.sh` downloads the pinned Linux release and checks its
SHA-256 when syft is not already installed. The SBOM is written to
`dist/provenance/sbom.spdx.json`. `dist/` is gitignored. Do not commit
generated SBOMs.

The publishing workflow writes the same pair of files for each architecture
after the image is pushed by digest, and uploads them as a workflow artifact.
That artifact is build evidence, not a deployment record.

## Immutable digest

The identity recorded by a provenance-qualified local build is the daemon
image ID, not a mutable tag and not a registry manifest digest.
`make provenance-build` stores `docker image inspect --format '{{.Id}}'` as
`local_image_id`. `registry_manifest_digest` stays null because this build
does not push. The image-publishing workflow is separate: it pushes each
architecture by digest, then also attaches a commit tag and a deployment
tag. That deployment tag, including `latest`, moves and is not provenance
evidence. A registry manifest digest remains a deployment-system gate.

`dist/provenance/release-manifest.json` links the clean commit to the local
image. `local_image_id` is `docker image inspect` image ID (`sha256:…`).
`registry_manifest_digest` is null until an image is pushed; the local ID is
not that digest. The manifest also records:

- `source_repository`, `source_revision`, `source_clean`
- `oci_revision`, `oci_source`, `oci_created`, `oci_version`
- `go_vcs_revision`, `go_vcs_modified`
- `application_version`
- `image_reference` (`arcade:provenance`)
- `sbom` filename and `sbom_sha256`
- `build_timestamp`

`.github/workflows/provenance-qualification.yml` runs this path on a Linux
runner: clean checkout, fail-closed rejection of a bad revision and a dirty
tree, local image build, verification, and upload of the SBOM, manifest, and
verification log. It does not log in to a registry and does not push.

## Generic Arcade and deployment governance

Generic Arcade owns source revision, dirty-tree detection, OCI labels, Go
buildinfo, SBOM generation, and the local release manifest.

Votari infrastructure owns the rest of release governance: the registry and
image name used in production, secret storage, host rollout, release
approval, pinning a running deployment to a digest, and where that
deployment's release manifest is stored. None of that is configured in this
repository.

Signing and attestation are not implemented here. They stay with the
publishing foundation that holds the signing keys.

## Verification

On a Linux host, after `make provenance-build`:

```sh
make provenance-verify
```

That runs `scripts/verify-image-provenance.sh arcade:provenance`. It does not
need a registry. It checks the local image, copies the binary out, and reads
`go version -m`. Success prints:

```
SOURCE_REVISION
SOURCE_CLEAN_STATE
OCI_SOURCE
OCI_REVISION
GO_VCS_REVISION
GO_VCS_MODIFIED
IMAGE_DIGEST
SBOM_PRESENT
```

`GO_VCS_REVISION` must equal `OCI_REVISION`, `GO_VCS_MODIFIED` must be
`false`, and the digest must equal the release manifest. The script exits
non-zero otherwise.
