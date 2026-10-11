#!/usr/bin/env bash
# Install or reuse syft 1.46.0 and print the binary path.
# A syft already on PATH is reused. Otherwise the pinned GitHub release
# tarball is downloaded and checked against the checksums below.
set -euo pipefail

version=1.46.0

if command -v syft >/dev/null 2>&1; then
  printf '%s\n' "$(command -v syft)"
  exit 0
fi

os=$(uname -s | tr '[:upper:]' '[:lower:]')
machine=$(uname -m)
case "$machine" in
  x86_64) arch=amd64 ;;
  aarch64|arm64) arch=arm64 ;;
  *)
    echo "unsupported architecture for syft: $machine" >&2
    exit 1
    ;;
esac

case "${os}_${arch}" in
  linux_amd64) sum=d654f678b709eb53c393d38519d5ed7d2e57205529404018614cfefa0fb2b5ca ;;
  linux_arm64) sum=9fafef4db4f032ce81008d3a1529985d41ceb6ccdf2b388c9ce2f1ed7d32082e ;;
  *)
    echo "no pinned syft checksum for ${os}_${arch}; install syft ${version} on PATH" >&2
    exit 1
    ;;
esac

name="syft_${version}_${os}_${arch}.tar.gz"
url="https://github.com/anchore/syft/releases/download/v${version}/${name}"
dest=${SYFT_INSTALL_DIR:-${TMPDIR:-/tmp}/syft-${version}}
mkdir -p "$dest"
archive="$dest/$name"

curl -fsSL "$url" -o "$archive"
echo "${sum}  ${archive}" | sha256sum -c --status -

tar -xzf "$archive" -C "$dest" syft
chmod +x "$dest/syft"
printf '%s\n' "$dest/syft"
