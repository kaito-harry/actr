#!/usr/bin/env bash
set -euo pipefail

# Package ActrFFI.xcframework for distribution:
# - Zips the xcframework into dist/ActrFFI.xcframework.zip
# - Computes the SwiftPM checksum
# - Prints the URL/checksum pair for Package.swift and Release asset upload
#
# Usage:
#   ./scripts/package-binary.sh v0.1.0
#     - Uses the provided tag for the Release download URL.
#   ACTR_BINARY_TAG=v0.1.0 ./scripts/package-binary.sh
#     - Or set via environment variable.
#
# Prerequisites:
# - Run ./build-xcframework.sh first to generate ActrFFI.xcframework
# - swift (for `swift package compute-checksum`)
# - python3 (for deterministic ZIP creation)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
resolve_root_path() {
  local path="$1"
  if [[ "${path}" = /* ]]; then
    printf '%s\n' "${path}"
  else
    printf '%s\n' "${ROOT_DIR}/${path}"
  fi
}

DIST_DIR="$(resolve_root_path "${ACTR_DIST_DIR:-dist}")"
FRAMEWORK_DIR="$(resolve_root_path "${ACTR_BINARY_PATH:-ActrFFI.xcframework}")"
ZIP_PATH="${DIST_DIR}/ActrFFI.xcframework.zip"
RELEASE_REPOSITORY="${ACTR_RELEASE_REPOSITORY:-Actrium/actr-swift-package-sync}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "error: missing required command: $1" >&2
    exit 1
  fi
}

require_cmd python3
require_cmd swift

RELEASE_TAG="${1:-${ACTR_BINARY_TAG:-v0.1.0}}"

if [[ ! -d "${FRAMEWORK_DIR}" ]]; then
  echo "error: missing ${FRAMEWORK_DIR}; run ./build-xcframework.sh first" >&2
  exit 1
fi

mkdir -p "${DIST_DIR}"
rm -f "${ZIP_PATH}" "${DIST_DIR}/release.txt"

echo "[1/3] Zipping XCFramework -> ${ZIP_PATH}"
python3 - "${FRAMEWORK_DIR}" "${ZIP_PATH}" <<'PY'
from __future__ import annotations

import os
import stat
import sys
import zipfile
from pathlib import Path


source = Path(sys.argv[1]).resolve()
destination = Path(sys.argv[2]).resolve()
parent = source.parent
entries = [source]

for directory, directory_names, file_names in os.walk(source, followlinks=False):
    directory_names.sort()
    file_names.sort()
    current = Path(directory)
    entries.extend(current / name for name in directory_names)
    entries.extend(current / name for name in file_names)

entries = sorted(set(entries), key=lambda path: path.relative_to(parent).as_posix())
with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_STORED) as archive:
    for path in entries:
        mode = path.lstat().st_mode
        is_directory = stat.S_ISDIR(mode) and not path.is_symlink()
        archive_name = path.relative_to(parent).as_posix()
        if is_directory:
            archive_name += "/"

        info = zipfile.ZipInfo(archive_name)
        info.date_time = (1980, 1, 1, 0, 0, 0)
        info.create_system = 3
        info.compress_type = zipfile.ZIP_STORED
        info.external_attr = (mode & 0xFFFF) << 16
        if is_directory:
            info.external_attr |= 0x10
            payload = b""
        elif stat.S_ISLNK(mode):
            payload = os.readlink(path).encode()
        else:
            payload = path.read_bytes()
        archive.writestr(info, payload)
PY

echo "[2/3] Computing SwiftPM checksum"
CHECKSUM="$(cd "${ROOT_DIR}" && swift package compute-checksum "${ZIP_PATH}")"

DOWNLOAD_URL="https://github.com/${RELEASE_REPOSITORY}/releases/download/${RELEASE_TAG}/ActrFFI.xcframework.zip"

echo "[3/3] Release info"
cat > "${DIST_DIR}/release.txt" <<EOF
Release tag:     ${RELEASE_TAG}
Download URL:    ${DOWNLOAD_URL}
SHA256 checksum: ${CHECKSUM}

Update Package.swift (default release tag/checksum):
  - ACTR_BINARY_TAG=${RELEASE_TAG}
  - ACTR_BINARY_CHECKSUM=${CHECKSUM}

Upload asset to GitHub Release:
  gh release create ${RELEASE_TAG} --notes "ActrFFI ${RELEASE_TAG}" ${ZIP_PATH}
  # or:
  gh release upload ${RELEASE_TAG} ${ZIP_PATH} --clobber
EOF

echo ""
echo "✅ Packaged ${ZIP_PATH}"
echo "🔑 Checksum: ${CHECKSUM}"
echo ""
echo "Next steps:"
echo "  1) Upload ${ZIP_PATH} to GitHub Release: ${RELEASE_TAG}"
echo "  2) Set ACTR_BINARY_CHECKSUM=${CHECKSUM} and ACTR_BINARY_TAG=${RELEASE_TAG} in Package.swift (or export as env when resolving)"
echo "  3) Push tag ${RELEASE_TAG} to https://github.com/${RELEASE_REPOSITORY}"
echo ""
