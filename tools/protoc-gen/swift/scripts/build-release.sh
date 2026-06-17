#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PRODUCT_NAME="protoc-gen-actrframework-swift"
ARCH="arm64"
CONFIGURATION="release"

DIST_DIR="${DIST_DIR:-$ROOT_DIR/dist}"
BUILD_DIR="${BUILD_DIR:-$ROOT_DIR/.build}"

swift build \
  --package-path "$ROOT_DIR" \
  -c "$CONFIGURATION" \
  --product "$PRODUCT_NAME" \
  --arch "$ARCH"

PRIMARY_BIN_PATH="$BUILD_DIR/$ARCH-apple-macosx/$CONFIGURATION/$PRODUCT_NAME"
FALLBACK_BIN_PATH="$BUILD_DIR/$CONFIGURATION/$PRODUCT_NAME"

if [[ -f "$PRIMARY_BIN_PATH" ]]; then
  BIN_PATH="$PRIMARY_BIN_PATH"
elif [[ -f "$FALLBACK_BIN_PATH" ]]; then
  BIN_PATH="$FALLBACK_BIN_PATH"
else
  echo "Release binary not found under $BUILD_DIR." >&2
  exit 1
fi

mkdir -p "$DIST_DIR"
cp "$BIN_PATH" "$DIST_DIR/$PRODUCT_NAME"
chmod +x "$DIST_DIR/$PRODUCT_NAME"

ZIP_NAME="${PRODUCT_NAME}-macos-arm64.zip"
ZIP_PATH="$DIST_DIR/$ZIP_NAME"
rm -f "$ZIP_PATH"

(cd "$DIST_DIR" && zip -q "$ZIP_NAME" "$PRODUCT_NAME")

CHECKSUM_PATH="${ZIP_PATH}.sha256"
( cd "$DIST_DIR" && shasum -a 256 "$ZIP_NAME" ) > "$CHECKSUM_PATH"

echo "Release artifact: $ZIP_PATH"
echo "SHA256 file: $CHECKSUM_PATH"
