#!/usr/bin/env bash
set -euo pipefail

: "${CLI_RELEASE_API:=https://api.github.com}"
readonly CLI_RELEASE_API

log_info() { printf '[INFO] %s\n' "$*"; }
fail() { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage:
  scripts/release-actr-cli.sh build --target <target> --version <version> --output <dir> --state-file <file> [--dry-run]
  scripts/release-actr-cli.sh publish --tag <tag> --assets <dir> --state-file <file> [--replace] [--create-release] [--dry-run]
EOF
}

append_state() {
  local name=$1 status=$2 reason=$3 url=$4
  mkdir -p "$(dirname "$STATE_FILE")"
  printf '%s\tcli-binary\tcli_binary\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" "$VERSION" "$status" "$reason" "$url" "${RELEASE_SHA:-}" >>"$STATE_FILE"
}

archive_ext() {
  [[ "$1" == *windows-msvc* ]] && printf zip || printf tar.gz
}

archive_name() {
  printf 'actr-v%s-%s.%s' "$VERSION" "$TARGET" "$(archive_ext "$TARGET")"
}

binary_name() {
  [[ "$TARGET" == *windows-msvc* ]] && printf actr.exe || printf actr
}

write_sha256() {
  local file_path=$1 output_path=$2 name hash
  name=$(basename "$file_path")

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$(dirname "$file_path")" && sha256sum "$name") |
      awk -v name="$name" '{print $1 "  " name}' >"$output_path"
  elif command -v shasum >/dev/null 2>&1; then
    (cd "$(dirname "$file_path")" && shasum -a 256 "$name") |
      awk -v name="$name" '{print $1 "  " name}' >"$output_path"
  elif command -v certutil >/dev/null 2>&1; then
    hash=$(certutil -hashfile "$file_path" SHA256 | awk 'NR==2 {gsub(/[[:space:]]/, ""); print tolower($0)}')
    [[ -n "$hash" ]] || fail "Failed to compute SHA256 for $name with certutil"
    printf '%s  %s\n' "$hash" "$name" >"$output_path"
  else
    fail "No SHA256 tool found (need sha256sum, shasum, or certutil)"
  fi

  [[ -s "$output_path" ]] || fail "Failed to write SHA256 for $name"
}

repository_name() {
  if [[ -n "${GITHUB_REPOSITORY:-}" ]]; then
    printf '%s' "$GITHUB_REPOSITORY"
  elif [[ -n "${PACKAGE_SYNC_OWNER:-}" ]]; then
    printf '%s/%s' "$PACKAGE_SYNC_OWNER" "${GITHUB_REPOSITORY_NAME:-actr}"
  else
    fail "Cannot resolve GitHub repository for CLI release"
  fi
}

release_url() {
  printf 'https://github.com/%s/releases/tag/%s' "$(repository_name)" "$(release_tag)"
}

release_tag() {
  if [[ -n "${TAG:-}" ]]; then
    printf '%s' "$TAG"
  elif [[ -n "${ACTR_RELEASE_TAG:-}" ]]; then
    printf '%s' "$ACTR_RELEASE_TAG"
  else
    printf 'v%s' "$VERSION"
  fi
}

version_from_release_tag() {
  local tag=$1
  local version version_without_build
  case "$tag" in
    validation-v*) version=${tag#validation-v} ;;
    v*) version=${tag#v} ;;
    *) fail "Unsupported release tag: $tag" ;;
  esac

  is_strict_semver "$version" || fail "Unsupported release tag: $tag"
  version_without_build=${version%%+*}
  if [[ "$tag" == validation-v* && "$version_without_build" != *-* ]]; then
    fail "Unsupported release tag: $tag"
  fi

  printf '%s' "$version"
}

is_strict_semver() {
  local version=$1 version_without_build core prerelease build identifier
  local -a prerelease_identifiers
  local numeric_identifier='(0|[1-9][0-9]*)'
  local core_pattern="^${numeric_identifier}\\.${numeric_identifier}\\.${numeric_identifier}$"
  local identifiers_pattern='^[0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*$'

  version_without_build=${version%%+*}
  if [[ "$version" == *+* ]]; then
    build=${version#*+}
    [[ "$build" =~ $identifiers_pattern ]] || return 1
  fi

  core=${version_without_build%%-*}
  [[ "$core" =~ $core_pattern ]] || return 1

  if [[ "$version_without_build" == *-* ]]; then
    prerelease=${version_without_build#*-}
    [[ "$prerelease" =~ $identifiers_pattern ]] || return 1
    IFS='.' read -r -a prerelease_identifiers <<<"$prerelease"
    for identifier in "${prerelease_identifiers[@]}"; do
      if [[ "$identifier" =~ ^[0-9]+$ && ! "$identifier" =~ ^$numeric_identifier$ ]]; then
        return 1
      fi
    done
  fi

  return 0
}

use_zigbuild() {
  case "$TARGET" in
    x86_64-unknown-linux-musl|aarch64-unknown-linux-gnu|aarch64-unknown-linux-musl) return 0 ;;
    *) return 1 ;;
  esac
}

package_binary() {
  local binary_path=$1 output_dir=$2 archive archive_path source_dir filename
  archive=$(archive_name)
  archive_path="$output_dir/$archive"
  source_dir=$(dirname "$binary_path")
  filename=$(basename "$binary_path")
  mkdir -p "$output_dir"

  if [[ "$(archive_ext "$TARGET")" == zip ]]; then
    if command -v zip >/dev/null 2>&1; then
      (cd "$source_dir" && zip -j -X "$archive_path" "$filename") >/dev/null
    else
      (cd "$source_dir" && powershell -NoProfile -Command \
        "Compress-Archive -Force -Path '$filename' -DestinationPath '$archive_path'") >/dev/null
    fi
  else
    tar -C "$source_dir" -czf "$archive_path" "$filename"
  fi

  [[ -f "$archive_path" ]] || fail "Failed to package actr CLI at $archive_path"
  write_sha256 "$archive_path" "$archive_path.sha256"
}

command_build() {
  TARGET="" VERSION="" OUTPUT_DIR="" STATE_FILE="" DRY_RUN=false
  while (($#)); do
    case "$1" in
      --target) TARGET=${2:-}; shift 2 ;;
      --version) VERSION=${2:-}; shift 2 ;;
      --output) OUTPUT_DIR=${2:-}; shift 2 ;;
      --state-file) STATE_FILE=${2:-}; shift 2 ;;
      --dry-run) DRY_RUN=true; shift ;;
      *) fail "Unknown build argument: $1" ;;
    esac
  done
  [[ -n "$TARGET" && -n "$VERSION" && -n "$OUTPUT_DIR" && -n "$STATE_FILE" ]] ||
    fail "build requires --target, --version, --output, and --state-file"

  local url
  url=$(release_url)
  if [[ "$DRY_RUN" == true ]]; then
    append_state "actr-cli-$TARGET" success dry_run_validated "$url"
    return
  fi

  local repo_root cargo_args filename binary_path
  repo_root=${ACTR_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
  if [[ "${ACTR_PREBUILT_ASSETS:-}" != "1" ]]; then
    log_info "Syncing CLI web runtime assets for $TARGET"
    (cd "$repo_root" && bash bindings/web/scripts/sync-cli-assets.sh --build)
  else
    log_info "Skipping asset sync (ACTR_PREBUILT_ASSETS=1)"
  fi
  cargo_args=(--release -p actr-cli --locked --features wasm-engine)
  filename=$(binary_name)

  if use_zigbuild; then
    log_info "Cross-compiling actr-cli for $TARGET via cargo-zigbuild"
    if ! (cd "$repo_root" && RUSTFLAGS="${RUSTFLAGS:-} -C strip=symbols" \
      cargo zigbuild "${cargo_args[@]}" --target "$TARGET"); then
      append_state "actr-cli-$TARGET" failure build_failed "$url"
      fail "cargo zigbuild failed for actr-cli ($TARGET)"
    fi
  else
    log_info "Building actr-cli for $TARGET"
    if ! (cd "$repo_root" && RUSTFLAGS="${RUSTFLAGS:-} -C strip=symbols" \
      cargo build "${cargo_args[@]}" --target "$TARGET"); then
      append_state "actr-cli-$TARGET" failure build_failed "$url"
      fail "cargo build failed for actr-cli ($TARGET)"
    fi
  fi

  binary_path="$repo_root/target/$TARGET/release/$filename"
  [[ -f "$binary_path" ]] || {
    append_state "actr-cli-$TARGET" failure binary_missing "$url"
    fail "Built actr-cli binary not found at $binary_path"
  }
  package_binary "$binary_path" "$OUTPUT_DIR"
  append_state "actr-cli-$TARGET" success packaged "$url"
}

upload_asset_once() {
  local upload_url=$1 asset_path=$2 token=$3
  curl -sS -o /dev/null -w '%{http_code}' -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/octet-stream" \
    -H "User-Agent: actr-release-cli" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$upload_url?name=$(basename "$asset_path")" --data-binary "@$asset_path"
}

resolve_release() {
  local repository=$1 tag=$2 token=$3 create_release=$4 response
  response=$(curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer $token" \
    -H "User-Agent: actr-release-cli" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$CLI_RELEASE_API/repos/$repository/releases/tags/$tag" 2>/dev/null || true)
  if [[ -z "$response" && "$create_release" == true ]]; then
    response=$(curl -fsSL -X POST \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer $token" \
      -H "User-Agent: actr-release-cli" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "$CLI_RELEASE_API/repos/$repository/releases" \
      -d "$(python3 -c 'import json,sys; print(json.dumps({"tag_name":sys.argv[1],"name":"actr "+sys.argv[1],"generate_release_notes":True,"prerelease":sys.argv[2]=="true"}))' "$tag" "${PRE_RELEASE:-false}")")
  fi
  [[ -n "$response" ]] || fail "GitHub Release $repository@$tag does not exist"
  printf '%s' "$response"
}

delete_existing_asset() {
  local repository=$1 release_id=$2 asset_name=$3 token=$4 assets asset_id
  assets=$(curl -fsSL \
    -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$CLI_RELEASE_API/repos/$repository/releases/$release_id/assets?per_page=100")
  asset_id=$(python3 -c 'import json,sys; n=sys.argv[1]; print(next((str(a["id"]) for a in json.load(sys.stdin) if a.get("name")==n),""))' "$asset_name" <<<"$assets")
  [[ -n "$asset_id" ]] || fail "Duplicate asset $asset_name was not found for replacement"
  curl -fsSL -X DELETE -H "Authorization: Bearer $token" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "$CLI_RELEASE_API/repos/$repository/releases/assets/$asset_id" >/dev/null
}

command_publish() {
  TAG="" ASSETS_DIR="" STATE_FILE="" REPLACE=false CREATE_RELEASE=false DRY_RUN=false
  while (($#)); do
    case "$1" in
      --tag) TAG=${2:-}; shift 2 ;;
      --assets) ASSETS_DIR=${2:-}; shift 2 ;;
      --state-file) STATE_FILE=${2:-}; shift 2 ;;
      --replace) REPLACE=true; shift ;;
      --create-release) CREATE_RELEASE=true; shift ;;
      --dry-run) DRY_RUN=true; shift ;;
      *) fail "Unknown publish argument: $1" ;;
    esac
  done
  [[ -n "$TAG" && -n "$ASSETS_DIR" && -n "$STATE_FILE" ]] ||
    fail "publish requires --tag, --assets, and --state-file"
  VERSION=$(version_from_release_tag "$TAG")
  local url
  url=$(release_url)
  if [[ "$DRY_RUN" == true ]]; then
    append_state actr-cli-release success dry_run_validated "$url"
    return
  fi

  local token=${GITHUB_TOKEN:-${RELEASE_GITHUB_TOKEN:-}} repository response release_id upload_url
  [[ -n "$token" ]] || fail "GITHUB_TOKEN must be set for CLI release upload"
  repository=$(repository_name)
  response=$(resolve_release "$repository" "$TAG" "$token" "$CREATE_RELEASE")
  release_id=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])' <<<"$response")
  upload_url=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["upload_url"].split("{",1)[0])' <<<"$response")

  local asset_count=0 asset status
  while IFS= read -r asset; do
    [[ -n "$asset" ]] || continue
    status=$(upload_asset_once "$upload_url" "$asset" "$token")
    if [[ "$status" == 422 && "$REPLACE" == true ]]; then
      delete_existing_asset "$repository" "$release_id" "$(basename "$asset")" "$token"
      status=$(upload_asset_once "$upload_url" "$asset" "$token")
    fi
    [[ "$status" == 200 || "$status" == 201 ]] ||
      fail "GitHub release upload failed for $(basename "$asset") (HTTP $status)"
    asset_count=$((asset_count + 1))
  done < <(find "$ASSETS_DIR" -maxdepth 1 -type f \( \
    -name "actr-v${VERSION}-*.tar.gz" -o -name "actr-v${VERSION}-*.tar.gz.sha256" \
    -o -name "actr-v${VERSION}-*.zip" -o -name "actr-v${VERSION}-*.zip.sha256" \) | sort)

  ((asset_count > 0)) || fail "No actr CLI assets found in $ASSETS_DIR"
  append_state actr-cli-release success "uploaded_${asset_count}_assets" "$url"
}

main() {
  local command=${1:-}
  [[ -n "$command" ]] || { usage >&2; exit 2; }
  shift
  case "$command" in
    build) command_build "$@" ;;
    publish) command_publish "$@" ;;
    *) usage >&2; fail "Unknown command: $command" ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
