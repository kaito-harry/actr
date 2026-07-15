#!/usr/bin/env bash
set -euo pipefail

# Basic release train for the monorepo-managed foundations, tools, SDKs, and CLI.
# Supports staged execution via --stage for parallel CI jobs.

readonly FINAL_TAG_PREFIX="v"
readonly LEGACY_FINAL_TAG_PREFIX="release-train-v"
readonly PYTHON_PACKAGE_NAME="framework_codegen_python"
readonly CRATES_IO_API="https://crates.io/api/v1/crates"
readonly PYPI_API="https://pypi.org/pypi"
readonly TEST_PYPI_API="https://test.pypi.org/pypi"

readonly FOUNDATION_CRATES=(
  "actr-protocol"
  "actr-service-compat"
  "actr-config"
  "actr-web-abi"
  "actr-framework"
  "actr-runtime-mailbox"
  "actr-runtime"
  "actr-platform-traits"
  "actr-pack"
  "actr-mock-actrix"
  "actr-hyper"
  "actr-platform-native"
)

readonly PROTOC_CRATES=(
  "actr-framework-protoc-codegen"
  "actr-web-protoc-codegen"
)

readonly SDK_CRATES=(
  "actr"
)

readonly CLI_CRATES=(
  "actr-cli"
)

# GitHub Release binary matrix for the `actr` CLI.
# Format per entry: rust-target|runner|use_zigbuild (true=zig cross-compile, false=native cargo build)
readonly CLI_BINARY_TARGETS=(
  "x86_64-unknown-linux-gnu|ubuntu-latest|false"
  "x86_64-unknown-linux-musl|ubuntu-latest|true"
  "aarch64-unknown-linux-gnu|ubuntu-latest|true"
  "aarch64-unknown-linux-musl|ubuntu-latest|true"
  "aarch64-apple-darwin|macos-14|false"
  "x86_64-pc-windows-msvc|windows-latest|false"
)

readonly VALID_STAGES=(
  "create-tag"
  "publish-rust"
  "publish-python"
  "publish-swift"
  "publish-kotlin"
  "publish-web"
  "build-typescript-native"
  "publish-typescript-workload"
  "publish-typescript"
  "build-cli-binaries"
  "publish-cli-binaries"
  "report"
  "notify-wechat"
)

readonly OPTIONAL_SKIPPED_COMPONENTS=(
)
readonly PACKAGE_SYNC_GITHUB_API="https://api.github.com"
readonly SWIFT_PACKAGE_SYNC_REPO="actr-swift-package-sync"
readonly KOTLIN_PACKAGE_SYNC_REPO="actr-kotlin-package-sync"
readonly RELEASE_BRANCH_PREFIX="release-prepare/v"

ORIGINAL_REPO_ROOT=""
WORK_REPO_ROOT=""
WORKTREE_PATH=""
REPORT_DIR=""
STATE_FILE=""
REPORT_MARKDOWN=""
REPORT_JSON=""
RELEASE_PYTHON_BIN="python3"
RELEASE_PYTHON_ENV=""

VERSION=""
DRY_RUN=false
PREPARE_ONLY=false
SKIP_PYTHON=false
PRE_RELEASE=false
SKIP_WEB=false
AUTO_VERSION=false
RUN_MODE="publish"
OVERALL_STATUS="success"
FAILURE_REASON=""
RELEASE_SHA=""
FINAL_TAG=""
TAG_ALREADY_EXISTS=false
STAGE="all"
PACKAGE_SYNC_OWNER="${PACKAGE_SYNC_OWNER:-}"
RELEASE_BRANCH="${RELEASE_BRANCH:-main}"
MAINTENANCE_RELEASE=false
RELEASE_LINE=""
NPM_DIST_TAG="latest"
PREVIOUS_TAG=""

usage() {
  cat <<'EOF'
Usage:
  scripts/release-train.sh [--version <X.Y.Z>] [--dry-run] [--prepare-only] [--skip-python] [--branch <branch>] [--stage <name>]

Options:
  --version <X.Y.Z>  Stable semver used by the monorepo-managed release train (optional in CI).
  --dry-run          Validate the full flow in a disposable worktree without publishing.
  --prepare-only     Update release versions, validate, and commit locally for a release PR.
  --skip-python      Skip Python package validation, version update, and publishing.
  --pre-release      Mark this release as a pre-release (e.g. 0.2.2-pre.1).
                     Uses npm tag "pre" and allows pre-release semver versions.
  --branch <branch>  Target release branch (default: main). Maintenance branches
                     must use release-X.Y and can only publish X.Y patch releases.
  --stage <name>     Run a single stage instead of the full pipeline.
                     Stages: create-tag, publish-rust, publish-python,
                     publish-swift, publish-kotlin, publish-web,
                     build-typescript-native, publish-typescript-workload,
                     publish-typescript, build-cli-binaries, publish-cli-binaries,
                     report, notify-wechat.
                     Default: all (runs full pipeline sequentially).
  --help             Show this help message.
EOF
}

log_info() {
  printf '[INFO] %s\n' "$*"
}

log_warn() {
  printf '[WARN] %s\n' "$*" >&2
}

log_error() {
  printf '[ERROR] %s\n' "$*" >&2
}

fail() {
  FAILURE_REASON="$*"
  log_error "$*"
  exit 1
}

cleanup() {
  if [[ -n "$WORKTREE_PATH" ]] && [[ -d "$WORKTREE_PATH" ]]; then
    git -C "$ORIGINAL_REPO_ROOT" worktree remove --force "$WORKTREE_PATH" >/dev/null 2>&1 || true
  fi
  if [[ -n "$RELEASE_PYTHON_ENV" ]] && [[ -d "$RELEASE_PYTHON_ENV" ]]; then
    rm -rf "$RELEASE_PYTHON_ENV"
  fi
}

generate_report() {
  mkdir -p "$REPORT_DIR"

  # When running a specific stage, merge all per-stage state files first.
  if [[ "$STAGE" != "all" ]] && [[ "$STAGE" != "report" ]]; then
    # Per-stage: only generate state file, skip full report.
    return
  fi

  if [[ "$STAGE" == "report" ]]; then
    # Merge per-stage state files into the main state file.
    : >"$STATE_FILE"
    local stage_state
    for stage_name in "${VALID_STAGES[@]}"; do
      stage_state="$REPORT_DIR/release-train-v${VERSION}.${stage_name}.state.tsv"
      if [[ -f "$stage_state" ]]; then
        cat "$stage_state" >>"$STATE_FILE"
      fi
    done
  fi

  python3 - "$STATE_FILE" "$REPORT_MARKDOWN" "$REPORT_JSON" "$VERSION" "$RUN_MODE" "$OVERALL_STATUS" "$FAILURE_REASON" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

state_path = Path(sys.argv[1])
markdown_path = Path(sys.argv[2])
json_path = Path(sys.argv[3])
version = sys.argv[4]
run_mode = sys.argv[5]
overall_status = sys.argv[6]
failure_reason = sys.argv[7]

rows = []
if state_path.exists():
    for line in state_path.read_text().splitlines():
        if not line.strip():
            continue
        name, stage, kind, component_version, status, mode, registry_url, git_sha = line.split("\t")
        rows.append(
            {
                "name": name,
                "stage": stage,
                "kind": kind,
                "version": component_version,
                "status": status,
                "mode": mode,
                "registry_url": registry_url,
                "git_sha": git_sha,
            }
        )

payload = {
    "version": version,
    "run_mode": run_mode,
    "overall_status": overall_status,
    "failure_reason": failure_reason,
    "components": rows,
}
json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n")

lines = [
    f"# Basic Release Train Report: {version}",
    "",
    f"- Run mode: `{run_mode}`",
    f"- Overall status: `{overall_status}`",
]

if failure_reason:
    lines.append(f"- Failure reason: {failure_reason}")

lines.extend(
    [
        "",
        "| Component | Stage | Kind | Version | Status | Mode | Registry | Git SHA |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
)

if rows:
    for row in rows:
        lines.append(
            f"| {row['name']} | {row['stage']} | {row['kind']} | {row['version']} | {row['status']} | "
            f"{row['mode']} | {row['registry_url']} | {row['git_sha']} |"
        )
else:
    lines.append("| _(none)_ | - | - | - | - | - | - | - |")

markdown_path.write_text("\n".join(lines) + "\n")
PY
}

on_exit() {
  local exit_code=$1
  if [[ $exit_code -ne 0 ]]; then
    OVERALL_STATUS="failure"
  fi

  if [[ -n "$STATE_FILE" ]] && [[ -n "$REPORT_MARKDOWN" ]] && [[ -n "$REPORT_JSON" ]]; then
    generate_report
  fi

  cleanup
  exit "$exit_code"
}

trap 'on_exit $?' EXIT

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "Required command not found: $1"
}

current_workspace_version() {
  python3 - "$ORIGINAL_REPO_ROOT" <<'PY'
from __future__ import annotations
import tomllib, sys
from pathlib import Path

repo = Path(sys.argv[1])
cargo = repo / "Cargo.toml"
with cargo.open("rb") as fh:
    data = tomllib.load(fh)
print(data["workspace"]["package"]["version"])
PY
}

detect_conventional_bump() {
  local last_tag
  last_tag=$(latest_release_tag)

  if [[ -z "$last_tag" ]]; then
    log_info "No prior release tag found; defaulting to minor bump" >&2
    echo "minor"
    return
  fi

  log_info "Analyzing conventional commits since ${last_tag}" >&2
  local highest="none"

  while IFS= read -r commit_msg; do
    if echo "$commit_msg" | grep -qE '^[a-z]+\([^)]+\)?!:|BREAKING CHANGE'; then
      highest="major"
      break
    fi
    if [[ "$highest" != "major" ]] && echo "$commit_msg" | grep -qE '^feat(\([^)]+\))?:'; then
      highest="minor"
    fi
    if [[ "$highest" == "none" ]] && echo "$commit_msg" | grep -qE '^fix(\([^)]+\))?:'; then
      highest="patch"
    fi
  done < <(git -C "$ORIGINAL_REPO_ROOT" log "${last_tag}..HEAD" --pretty=format:"%s%n")

  echo "$highest"
}

release_prepare_should_skip_current_head() {
  local message
  message=$(git -C "$ORIGINAL_REPO_ROOT" log -1 --pretty=%B)
  grep -qE '(^|[[:space:]])chore\(release\):' <<<"$message"
}

latest_release_tag() {
  git -C "$ORIGINAL_REPO_ROOT" describe \
    --tags \
    --match "${FINAL_TAG_PREFIX}[0-9]*" \
    --match "${LEGACY_FINAL_TAG_PREFIX}[0-9]*" \
    --abbrev=0 2>/dev/null || echo ""
}

previous_release_tag() {
  git -C "$ORIGINAL_REPO_ROOT" describe \
    --tags \
    --match "${FINAL_TAG_PREFIX}[0-9]*" \
    --match "${LEGACY_FINAL_TAG_PREFIX}[0-9]*" \
    --abbrev=0 HEAD^ 2>/dev/null || echo ""
}

calculate_next_version() {
  local current=$1
  local bump=$2

  IFS='.' read -r major minor patch <<< "$current"

  case "$bump" in
    major) echo "$((major + 1)).0.0" ;;
    minor) echo "${major}.$((minor + 1)).0" ;;
    patch) echo "${major}.${minor}.$((patch + 1))" ;;
    *) echo "$current" ;;
  esac
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version)
        VERSION="${2:-}"
        shift 2
        ;;
      --dry-run)
        DRY_RUN=true
        shift
        ;;
      --prepare-only)
        PREPARE_ONLY=true
        shift
        ;;
      --skip-python)
        SKIP_PYTHON=true
        shift
        ;;
      --pre-release)
        PRE_RELEASE=true
        shift
        ;;
      --branch)
        RELEASE_BRANCH="${2:-main}"
        shift 2
        ;;
      --stage)
        STAGE="${2:-}"
        shift 2
        ;;
      --help)
        usage
        exit 0
        ;;
      *)
        usage
        fail "Unknown argument: $1"
        ;;
    esac
  done

  if [[ -z "$VERSION" ]]; then
    AUTO_VERSION=true
  fi

  if [[ "$DRY_RUN" == true && "$PREPARE_ONLY" == true ]]; then
    fail "--dry-run and --prepare-only cannot be used together"
  fi

  if [[ "$DRY_RUN" == true ]]; then
    RUN_MODE="dry-run"
  elif [[ "$PREPARE_ONLY" == true ]]; then
    RUN_MODE="prepare"
  fi

  if [[ "$STAGE" != "all" ]]; then
    local found=false
    local s
    for s in "${VALID_STAGES[@]}"; do
      if [[ "$s" == "$STAGE" ]]; then
        found=true
        break
      fi
    done
    if [[ "$found" == false ]]; then
      fail "Unknown stage: ${STAGE}. Valid stages: ${VALID_STAGES[*]}"
    fi
  fi
}

validate_version() {
  if [[ "$PRE_RELEASE" == true ]]; then
    if ! is_strict_semver "$VERSION" || [[ "${VERSION%%+*}" != *-* ]]; then
      fail "Pre-release version must follow strict SemVer X.Y.Z-<id> format"
      return 1
    fi
    if [[ "$SKIP_PYTHON" == false ]] && ! is_pep440_compatible_pre_release "$VERSION"; then
      fail "Pre-release version must be PEP 440-compatible while Python publishing is enabled (for example X.Y.Z-rc.1); use --skip-python for other SemVer identifiers"
      return 1
    fi
  else
    if ! is_strict_semver "$VERSION" || [[ "$VERSION" == *[-+]* ]]; then
      fail "Version must be a stable SemVer in X.Y.Z format"
      return 1
    fi
  fi
}

configure_release_channel() {
  local version_core=${VERSION%%-*}

  if [[ "$RELEASE_BRANCH" == "main" ]]; then
    RELEASE_LINE="${version_core%.*}"
    if [[ "$PRE_RELEASE" == true ]]; then
      NPM_DIST_TAG="pre"
    else
      NPM_DIST_TAG="latest"
    fi
    return
  fi

  if [[ "$RELEASE_BRANCH" =~ ^release-([0-9]+)\.([0-9]+)$ ]]; then
    MAINTENANCE_RELEASE=true
    RELEASE_LINE="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
    NPM_DIST_TAG="legacy-${RELEASE_LINE}"
    if [[ "$PRE_RELEASE" != false ]]; then
      fail "Maintenance branch ${RELEASE_BRANCH} only supports stable patch releases"
      return 1
    fi
    if [[ ! "$version_core" =~ ^${RELEASE_LINE}\.[0-9]+$ ]]; then
      fail "Maintenance branch ${RELEASE_BRANCH} can only publish ${RELEASE_LINE}.x, got ${VERSION}"
      return 1
    fi
    return
  fi

  fail "Unsupported release branch ${RELEASE_BRANCH}; expected main or release-X.Y"
}

validate_maintenance_release_policy() {
  [[ "$MAINTENANCE_RELEASE" == true ]] || return 0

  local current current_patch target_patch last_tag last_version tagged_patch history
  current=$(current_workspace_version)
  if [[ ! "$current" =~ ^${RELEASE_LINE}\.([0-9]+)$ ]]; then
    fail "Workspace version ${current} does not belong to maintenance line ${RELEASE_LINE}.x"
    return 1
  fi
  current_patch=${BASH_REMATCH[1]}
  target_patch=${VERSION##*.}

  if [[ "$VERSION" != "$current" ]] && (( target_patch != current_patch + 1 )); then
    fail "Maintenance releases must increment exactly one patch: ${current} -> ${RELEASE_LINE}.$((current_patch + 1))"
    return 1
  fi

  last_tag=${PREVIOUS_TAG:-$(previous_release_tag)}
  if [[ -n "$last_tag" ]]; then
    last_version=${last_tag#${LEGACY_FINAL_TAG_PREFIX}}
    last_version=${last_version#${FINAL_TAG_PREFIX}}
    if [[ "$last_version" =~ ^${RELEASE_LINE}\.([0-9]+)$ ]]; then
      tagged_patch=${BASH_REMATCH[1]}
      if (( target_patch != tagged_patch && target_patch != tagged_patch + 1 )); then
        fail "Maintenance releases must publish the current or next patch after ${last_tag}, got ${VERSION}"
        return 1
      fi
    fi
    history=$(git -C "$ORIGINAL_REPO_ROOT" log "${last_tag}..HEAD" --format='%s%n%b')
    if grep -qE '^[a-z]+(\([^)]+\))?!:|^BREAKING CHANGE:' <<<"$history"; then
      fail "Maintenance branch ${RELEASE_BRANCH} contains a breaking change since ${last_tag}"
      return 1
    fi
    if grep -qE '^feat(\([^)]+\))?:' <<<"$history"; then
      fail "Maintenance branch ${RELEASE_BRANCH} contains a feature commit since ${last_tag}; only fixes are allowed"
      return 1
    fi
  fi
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

is_pep440_compatible_pre_release() {
  local version=$1 version_without_build prerelease
  local pep440_pre_pattern='^(a|alpha|b|beta|c|rc|pre|preview|dev)([0-9]+|[-.](0|[1-9][0-9]*))?$'

  # Release metadata accepted by SemVer is not necessarily valid PEP 440
  # local-version metadata, so keep Python-enabled releases unambiguous.
  [[ "$version" != *+* ]] || return 1
  version_without_build=${version%%+*}
  prerelease=${version_without_build#*-}
  [[ "$prerelease" =~ $pep440_pre_pattern ]]
}

ensure_clean_worktree() {
  if [[ "$DRY_RUN" == true ]] || [[ "$PREPARE_ONLY" == true ]]; then
    return
  fi
  if [[ -n "$(git -C "$ORIGINAL_REPO_ROOT" status --porcelain)" ]]; then
    fail "Working tree must be clean before running the release train"
  fi
}

# Path helpers: per-stage state file for parallel-safe writes.
stage_state_file() {
  printf '%s/release-train-v%s.%s.state.tsv' "$REPORT_DIR" "$VERSION" "$1"
}

context_file() {
  printf '%s/release-train-v%s.context.json' "$REPORT_DIR" "$VERSION"
}

write_context() {
  mkdir -p "$REPORT_DIR"
  python3 - "$VERSION" "$RELEASE_SHA" "$DRY_RUN" "$PRE_RELEASE" "$SKIP_PYTHON" \
    "$FINAL_TAG" "$RELEASE_BRANCH" "$RELEASE_LINE" "$MAINTENANCE_RELEASE" \
    "$NPM_DIST_TAG" "$PREVIOUS_TAG" "$(context_file)" <<'PY'
from __future__ import annotations
import json, sys
(
    version,
    sha,
    dry_run,
    pre_release,
    skip_python,
    tag,
    release_branch,
    release_line,
    maintenance_release,
    npm_dist_tag,
    previous_tag,
    path,
) = sys.argv[1:13]
json.dump({
    "version": version,
    "release_sha": sha,
    "dry_run": dry_run == "true",
    "pre_release": pre_release == "true",
    "skip_python": skip_python == "true",
    "final_tag": tag,
    "release_branch": release_branch,
    "release_line": release_line,
    "maintenance_release": maintenance_release == "true",
    "npm_dist_tag": npm_dist_tag,
    "previous_tag": previous_tag,
}, open(path, "w"), indent=2)
print(path)
PY
}

read_context() {
  local ctx
  ctx="$(context_file)"
  if [[ ! -f "$ctx" ]]; then
    fail "Context file not found: ${ctx}. Run --stage create-tag first."
  fi
  eval "$(python3 - "$ctx" <<'PY'
from __future__ import annotations
import json, sys
ctx = json.load(open(sys.argv[1]))
for k, v in ctx.items():
    if isinstance(v, bool):
        print(f'{k.upper()}={"true" if v else "false"}')
    else:
        print(f'{k.upper()}={v}')
PY
)"

  if [[ "$STAGE" == "report" ]]; then
    return
  fi

  local current_sha
  current_sha=$(git rev-parse HEAD)
  [[ "$current_sha" == "$RELEASE_SHA" ]] ||
    fail "Release context SHA ${RELEASE_SHA} does not match current HEAD ${current_sha}"
}

prepare_paths() {
  ORIGINAL_REPO_ROOT=$(git rev-parse --show-toplevel)
  WORK_REPO_ROOT="$ORIGINAL_REPO_ROOT"
  REPORT_DIR="$ORIGINAL_REPO_ROOT/release/reports"

  if [[ "$STAGE" == "all" || "$STAGE" == "report" ]]; then
    STATE_FILE="$REPORT_DIR/release-train-v${VERSION}.state.tsv"
    REPORT_MARKDOWN="$REPORT_DIR/release-train-v${VERSION}.md"
    REPORT_JSON="$REPORT_DIR/release-train-v${VERSION}.json"
  else
    # Per-stage: write to stage-specific state file.
    STATE_FILE="$(stage_state_file "$STAGE")"
    REPORT_MARKDOWN=""
    REPORT_JSON=""
  fi

  mkdir -p "$REPORT_DIR"
  : >"$STATE_FILE"
}

prepare_worktree() {
  git -C "$ORIGINAL_REPO_ROOT" fetch origin --tags >/dev/null

  if [[ "$DRY_RUN" == true ]]; then
    local current_head
    current_head=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse HEAD)
    WORKTREE_PATH=$(mktemp -d "${TMPDIR:-/tmp}/actr-release-train.XXXXXX")
    git -C "$ORIGINAL_REPO_ROOT" worktree add --detach "$WORKTREE_PATH" "$current_head" >/dev/null
    WORK_REPO_ROOT="$WORKTREE_PATH"
  elif [[ "$PREPARE_ONLY" == true ]]; then
    :
  else
    local current_branch current_head origin_target
    current_branch=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse --abbrev-ref HEAD)
    [[ "$current_branch" == "$RELEASE_BRANCH" ]] || fail "Publish execution must start from the local ${RELEASE_BRANCH} branch"

    current_head=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse HEAD)
    origin_target=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse "origin/${RELEASE_BRANCH}")
    [[ "$current_head" == "$origin_target" ]] || fail "Local ${RELEASE_BRANCH} must match origin/${RELEASE_BRANCH} before publishing"
  fi

  cd "$WORK_REPO_ROOT"
}

append_state() {
  local name=$1
  local stage=$2
  local kind=$3
  local status=$4
  local mode=$5
  local registry_url=$6
  local git_sha=$7

  name=${name//$'\t'/ }
  stage=${stage//$'\t'/ }
  kind=${kind//$'\t'/ }
  status=${status//$'\t'/ }
  mode=${mode//$'\t'/ }
  registry_url=${registry_url//$'\t'/ }
  git_sha=${git_sha//$'\t'/ }

  name=${name//$'\n'/ }
  stage=${stage//$'\n'/ }
  kind=${kind//$'\n'/ }
  status=${status//$'\n'/ }
  mode=${mode//$'\n'/ }
  registry_url=${registry_url//$'\n'/ }
  git_sha=${git_sha//$'\n'/ }

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$name" \
    "$stage" \
    "$kind" \
    "$VERSION" \
    "$status" \
    "$mode" \
    "$registry_url" \
    "$git_sha" >>"$STATE_FILE"
}

set_release_sha() {
  RELEASE_SHA=$(git rev-parse HEAD)
}

configure_git_identity() {
  if [[ -z "$(git config --get user.name || true)" ]]; then
    git config user.name "github-actions[bot]"
  fi

  if [[ -z "$(git config --get user.email || true)" ]]; then
    git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
  fi
}

resolve_package_sync_owner() {
  if [[ -n "${PACKAGE_SYNC_OWNER:-}" ]]; then
    return
  fi

  PACKAGE_SYNC_OWNER=$(
    python3 - "$(git config --get remote.origin.url)" <<'PY'
from __future__ import annotations

import re
import sys

url = sys.argv[1]
match = re.search(r'github\.com[:/]([^/]+)/', url)
if not match:
    raise SystemExit("failed to resolve GitHub owner from origin URL")
print(match.group(1))
PY
  )
}

package_sync_repo_url() {
  local repo=$1
  printf 'https://github.com/%s/%s' "$PACKAGE_SYNC_OWNER" "$repo"
}

package_sync_release_url() {
  local repo=$1
  printf '%s/releases/tag/v%s' "$(package_sync_repo_url "$repo")" "$VERSION"
}

ensure_release_tag_available() {
  FINAL_TAG="${FINAL_TAG_PREFIX}${VERSION}"
  TAG_ALREADY_EXISTS=false

  if [[ "$DRY_RUN" == true ]]; then
    log_info "Skipping final tag existence check in dry-run mode (tag: ${FINAL_TAG})"
    return
  fi

  local current_sha
  current_sha=$(git rev-parse HEAD 2>/dev/null || true)

  # Check local tag first.
  if git rev-parse -q --verify "refs/tags/${FINAL_TAG}" >/dev/null 2>&1; then
    local tag_sha
    tag_sha=$(git rev-parse "refs/tags/${FINAL_TAG}")
    if [[ -n "$current_sha" && "$tag_sha" == "$current_sha" ]]; then
      log_info "Final release tag ${FINAL_TAG} already exists on current HEAD; skipping tag creation"
      TAG_ALREADY_EXISTS=true
      return
    elif [[ -n "$current_sha" ]]; then
      fail "Final release tag ${FINAL_TAG} exists locally but points to a different commit (tag: ${tag_sha}, HEAD: ${current_sha})"
    fi
    # current_sha is empty (e.g. fresh repo with no commits); this is unexpected but continuing.
  fi

  # Check remote tag.
  if git ls-remote --exit-code --tags origin "refs/tags/${FINAL_TAG}" >/dev/null 2>&1; then
    local remote_tag_sha
    remote_tag_sha=$(git ls-remote --tags origin "refs/tags/${FINAL_TAG}" | awk '{print $1}')
    if [[ -n "$current_sha" && "$remote_tag_sha" == "$current_sha" ]]; then
      log_info "Final release tag ${FINAL_TAG} already exists on origin and points to current HEAD; skipping tag creation"
      TAG_ALREADY_EXISTS=true
      return
    elif [[ -n "$current_sha" ]]; then
      fail "Final release tag ${FINAL_TAG} exists on origin but points to a different commit (tag: ${remote_tag_sha}, HEAD: ${current_sha})"
    fi
    fail "Final release tag ${FINAL_TAG} exists on origin (tag: ${remote_tag_sha}) but HEAD is not a commit yet"
  fi
}

stage_requires_tag_availability_check() {
  case "$STAGE" in
    all|create-tag)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

install_python_release_tools() {
  RELEASE_PYTHON_ENV=$(mktemp -d "${TMPDIR:-/tmp}/actr-release-python.XXXXXX")
  python3 -m venv "$RELEASE_PYTHON_ENV"
  RELEASE_PYTHON_BIN="${RELEASE_PYTHON_ENV}/bin/python"
  "$RELEASE_PYTHON_BIN" -m pip install --quiet --upgrade pip build twine
}

stage_requires_python_release_tools() {
  [[ "$SKIP_PYTHON" == false ]] || return 1
  [[ "$PREPARE_ONLY" == true || "$STAGE" == "all" || "$STAGE" == "publish-python" ]]
}

build_python_distribution() {
  rm -rf tools/protoc-gen/python/dist tools/protoc-gen/python/build tools/protoc-gen/python/*.egg-info
  (
    cd tools/protoc-gen/python
    "$RELEASE_PYTHON_BIN" -m build >/dev/null
  )
}

update_versions() {
  python3 - "$WORK_REPO_ROOT" "$VERSION" "$SKIP_PYTHON" <<'PY'
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

repo = Path(sys.argv[1])
version = sys.argv[2]
skip_python = sys.argv[3] == "true"

package_files = [
    repo / "Cargo.toml",
    repo / "bindings/web/Cargo.toml",
    repo / "core/protocol/Cargo.toml",
    repo / "core/service-compat/Cargo.toml",
    repo / "core/config/Cargo.toml",
    repo / "core/framework/Cargo.toml",
    repo / "core/runtime-mailbox/Cargo.toml",
    repo / "core/runtime/Cargo.toml",
    repo / "core/platform-traits/Cargo.toml",
    repo / "core/pack/Cargo.toml",
    repo / "core/hyper/Cargo.toml",
    repo / "core/platform-native/Cargo.toml",
    repo / "testing/mock-actrix/Cargo.toml",
    repo / "tools/protoc-gen/rust/Cargo.toml",
    repo / "tools/protoc-gen/web/Cargo.toml",
    repo / "cli/Cargo.toml",
    repo / "bindings/typescript/Cargo.toml",
    repo / "bindings/web/crates/actr-web-abi/Cargo.toml",
    repo / "bindings/web/crates/common/Cargo.toml",
    repo / "bindings/web/crates/sw-host/Cargo.toml",
    repo / "bindings/web/crates/dom-bridge/Cargo.toml",
    repo / "bindings/web/crates/mailbox-web/Cargo.toml",
    repo / "bindings/web/crates/platform-web/Cargo.toml",
    repo / "bindings/web/crates/framework-web-entry-smoke/Cargo.toml",
]

cli_dependency_names = {
    "actr",
    "actr-runtime-mailbox",
    "actr-hyper",
    "actr-pack",
    "actr-platform-native",
    "actr-mock-actrix",
    "actr-config",
    "actr-protocol",
    "actr-service-compat",
    "actr-framework-protoc-codegen",
    "actr-web-protoc-codegen",
}

dependency_version_names = {
    "actr-web-abi",
    "actr-pack",
}

workspace_dependency_names = {
    "actr-protocol",
    "actr-service-compat",
    "actr-config",
    "actr-framework",
    "actr-framework-protoc-codegen",
    "actr-runtime",
    "actr-runtime-mailbox",
    "actr-platform-traits",
    "actr-pack",
    "actr-hyper",
    "actr-platform-native",
    "actr-mock-actrix",
}

def replace_first_version(lines: list[str]) -> list[str]:
    for index, line in enumerate(lines):
        if line.startswith("version = "):
            lines[index] = f'version = "{version}"'
            return lines
    raise RuntimeError("package version line not found")

for path in package_files:
    lines = path.read_text().splitlines()
    if path.name != "Cargo.toml" or path.parent == repo / "cli":
        lines = replace_first_version(lines)
    else:
        current_section = None
        package_done = False
        workspace_package_done = False
        for index, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                current_section = stripped
                continue

            if current_section == "[package]" and stripped.startswith("version = ") and not package_done:
                lines[index] = f'version = "{version}"'
                package_done = True
                continue

            if current_section == "[workspace.package]" and stripped.startswith("version = ") and not workspace_package_done:
                lines[index] = f'version = "{version}"'
                workspace_package_done = True
                continue

            if current_section == "[workspace.dependencies]":
                name = stripped.split("=", 1)[0].strip()
                if name in workspace_dependency_names:
                    lines[index] = re.sub(r'version = "[^"]+"', f'version = "{version}"', line)

    if path.parent == repo / "cli":
        for index, line in enumerate(lines):
            stripped = line.strip()
            name = stripped.split("=", 1)[0].strip()
            if name in cli_dependency_names and "version = " in line:
                lines[index] = re.sub(r'version = "[^"]+"', f'version = "{version}"', line)

    for index, line in enumerate(lines):
        stripped = line.strip()
        name = stripped.split("=", 1)[0].strip()
        if name in dependency_version_names and "version = " in line:
            lines[index] = re.sub(r'version = "[^"]+"', f'version = "{version}"', line)

    path.write_text("\n".join(lines) + "\n")

if not skip_python:
    pyproject = repo / "tools/protoc-gen/python/pyproject.toml"
    py_lines = pyproject.read_text().splitlines()
    current_section = None
    for index, line in enumerate(py_lines):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            current_section = stripped
            continue
        if current_section == "[project]" and stripped.startswith("version = "):
            py_lines[index] = f'version = "{version}"'
            break
    else:
        raise RuntimeError("project version not found in pyproject.toml")

    pyproject.write_text("\n".join(py_lines) + "\n")

# Bump web and typescript package versions to match the release train version.
web_packages = [
    repo / "bindings/web/packages/actr-dom/package.json",
    repo / "bindings/web/packages/web-sdk/package.json",
    repo / "bindings/web/packages/web-react/package.json",
    repo / "bindings/typescript/package.json",
    repo / "bindings/typescript/actr-workload/package.json",
]
for wp in web_packages:
    pkg = json.loads(wp.read_text())
    pkg["version"] = version
    # Sync optionalDependencies for @actrium/actr-* platform packages.
    for dep_name, dep_val in list(pkg.get("optionalDependencies", {}).items()):
        if dep_name.startswith("@actrium/actr-"):
            pkg["optionalDependencies"][dep_name] = version
    wp.write_text(json.dumps(pkg, indent=2) + "\n")

typescript_plugin_package = repo / "tools/protoc-gen/typescript/package.json"
typescript_plugin_package_data = json.loads(typescript_plugin_package.read_text())
typescript_plugin_package_data["version"] = version
typescript_plugin_package.write_text(
    json.dumps(typescript_plugin_package_data, indent=2) + "\n"
)

typescript_plugin_lock = repo / "tools/protoc-gen/typescript/package-lock.json"
typescript_plugin_lock_data = json.loads(typescript_plugin_lock.read_text())
typescript_plugin_lock_data["version"] = version
typescript_plugin_lock_data["packages"][""]["version"] = version
typescript_plugin_lock.write_text(
    json.dumps(typescript_plugin_lock_data, indent=2) + "\n"
)

embedded_versions = [
    (
        repo / "tools/protoc-gen/swift/Sources/framework-codegen-swift/main.swift",
        r'(static let version = ")[^"]+(")',
        rf'\g<1>{version}\g<2>',
        1,
    ),
    (
        repo / "tools/protoc-gen/typescript/src/main.ts",
        r'(const VERSION = ")[^"]+(";)',
        rf'\g<1>{version}\g<2>',
        1,
    ),
    (
        repo / "tools/protoc-gen/kotlin/build.gradle.kts",
        r'(?m)^(version = ")[^"]+(")$',
        rf'\g<1>{version}\g<2>',
        1,
    ),
    (
        repo / "tools/protoc-gen/kotlin/src/main/kotlin/io/actrium/codegen/Main.kt",
        r'(protoc-gen-actrframework-kotlin )[0-9]+\.[0-9]+\.[0-9]+',
        rf'\g<1>{version}',
        1,
    ),
    (
        repo / "tools/protoc-gen/kotlin/src/main/kotlin/io/actrium/codegen/Main.kt",
        r'(println\("    )[0-9]+\.[0-9]+\.[0-9]+("\))',
        rf'\g<1>{version}\g<2>',
        1,
    ),
]

for path, pattern, replacement, expected_count in embedded_versions:
    updated, count = re.subn(pattern, replacement, path.read_text())
    if count != expected_count:
        raise RuntimeError(
            f"expected {expected_count} version replacement(s) in {path}, got {count}"
        )
    path.write_text(updated)
PY
}

all_publishable_crates() {
  printf '%s\n' \
    "${FOUNDATION_CRATES[@]}" \
    "${PROTOC_CRATES[@]}" \
    "${SDK_CRATES[@]}" \
    "${CLI_CRATES[@]}"
}

package_workspace_dir() {
  case "$1" in
    actr-web-abi)
      printf '%s/bindings/web' "$WORK_REPO_ROOT"
      ;;
    *)
      printf '%s' "$WORK_REPO_ROOT"
      ;;
  esac
}

run_validation_suite() {
  log_info "Running formatter and compile checks"
  log_info "Generating CLI web runtime assets"
  bash bindings/web/scripts/sync-cli-assets.sh --build

  cargo fmt --all
  cargo check

  local package
  while IFS= read -r package; do
    [[ -n "$package" ]] || continue
    log_info "Checking package contents for ${package}"
    (
      cd "$(package_workspace_dir "$package")"
      cargo package -p "$package" --locked --allow-dirty --list >/dev/null
    )
  done < <(all_publishable_crates)

  if [[ "$SKIP_PYTHON" == false ]]; then
    log_info "Building Python package for validation"
    build_python_distribution
  else
    log_info "Skipping Python package validation"
  fi
}

prepare_cli_publish_assets() {
  log_info "Generating CLI web runtime assets for actr-cli publish"
  (cd "$WORK_REPO_ROOT" && bash bindings/web/scripts/sync-cli-assets.sh --build)
}

append_skipped_components() {
  if (( ${#OPTIONAL_SKIPPED_COMPONENTS[@]} == 0 )); then
    return
  fi

  local descriptor name stage reason
  for descriptor in "${OPTIONAL_SKIPPED_COMPONENTS[@]}"; do
    IFS='|' read -r name stage reason <<<"$descriptor"
    append_state "$name" "$stage" "external" "skipped" "$reason" "-" "$RELEASE_SHA"
  done
}

dispatch_package_sync_workflow() {
  local repo=$1
  local workflow=$2
  local dispatched_at payload

  dispatched_at=$(python3 - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)
  payload=$(python3 - "$VERSION" "$RELEASE_SHA" "$FINAL_TAG" "$MAINTENANCE_RELEASE" "$RELEASE_BRANCH" <<'PY'
from __future__ import annotations

import json
import sys

version, source_sha, source_tag, maintenance_release, release_branch = sys.argv[1:6]
inputs = {
    "version": version,
    "source_sha": source_sha,
    "source_tag": source_tag,
}
if maintenance_release == "true":
    inputs["target_branch"] = release_branch
print(json.dumps({"ref": "main", "inputs": inputs}))
PY
)
  curl -fsSL \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${PACKAGE_SYNC_GITHUB_TOKEN}" \
    -H "User-Agent: actr-release-train" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "${PACKAGE_SYNC_GITHUB_API}/repos/${PACKAGE_SYNC_OWNER}/${repo}/actions/workflows/${workflow}/dispatches" \
    -d "$payload" >/dev/null

  printf '%s\n' "${dispatched_at}"
}

find_package_sync_run_id() {
  local repo=$1
  local workflow=$2
  local dispatched_at=$3
  local response

  response=$(curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${PACKAGE_SYNC_GITHUB_TOKEN}" \
    -H "User-Agent: actr-release-train" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "${PACKAGE_SYNC_GITHUB_API}/repos/${PACKAGE_SYNC_OWNER}/${repo}/actions/workflows/${workflow}/runs?event=workflow_dispatch&branch=main&per_page=10") || return

  python3 -c '
from __future__ import annotations

import json
import sys

payload = json.load(sys.stdin)
dispatched_at = sys.argv[1]

for run in payload.get("workflow_runs", []):
    if run.get("created_at", "") >= dispatched_at:
        print(run["id"])
        break
' "$dispatched_at" <<<"$response"
}

wait_for_package_sync_workflow() {
  local repo=$1
  local workflow=$2
  local dispatched_at=$3
  local run_id=""
  local attempt
  local query_failed=false

  for attempt in $(seq 1 30); do
    if ! run_id=$(find_package_sync_run_id "$repo" "$workflow" "$dispatched_at"); then
      run_id=""
      if [[ "$query_failed" == false ]]; then
        log_warn "Unable to query ${repo} workflow runs; verify PACKAGE_SYNC_GITHUB_TOKEN has Actions read access for ${PACKAGE_SYNC_OWNER}/${repo}"
      fi
      query_failed=true
    fi
    if [[ -n "${run_id}" ]]; then
      break
    fi
    log_info "Waiting for ${repo} workflow run creation (${attempt}/30)" >&2
    sleep 10
  done

  if [[ -z "${run_id}" && "$query_failed" == true ]]; then
    fail "Failed to locate workflow run for ${repo} after ${dispatched_at}; package-sync workflow run queries failed"
  fi

  [[ -n "${run_id}" ]] || fail "Failed to locate workflow run for ${repo} after ${dispatched_at}"

  for attempt in $(seq 1 120); do
    local response status conclusion html_url
    response=$(curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${PACKAGE_SYNC_GITHUB_TOKEN}" \
      -H "User-Agent: actr-release-train" \
      -H "X-GitHub-Api-Version: 2022-11-28" \
      "${PACKAGE_SYNC_GITHUB_API}/repos/${PACKAGE_SYNC_OWNER}/${repo}/actions/runs/${run_id}")
    status=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"$response")
    conclusion=$(python3 -c 'import json,sys; print(json.load(sys.stdin).get("conclusion",""))' <<<"$response")
    html_url=$(python3 -c 'import json,sys; print(json.load(sys.stdin)["html_url"])' <<<"$response")

    if [[ "${status}" == "completed" ]]; then
      if [[ "${conclusion}" == "success" ]]; then
        printf '%s\n' "${html_url}"
        return 0
      fi
      log_error "${repo} workflow failed: ${html_url}"
      return 1
    fi

    log_info "Waiting for ${repo} workflow completion (${attempt}/120)" >&2
    sleep 15
  done

  log_error "Timed out waiting for ${repo} workflow completion"
  return 1
}

publish_web_packages() {
  local web_root="$WORK_REPO_ROOT/bindings/web"
  local publish_script="$web_root/scripts/publish.sh"
  local web_version publish_args

  if [[ ! -f "$publish_script" ]]; then
    log_warn "Web publish script not found; skipping web packages"
    append_state "actr-web" "sdk" "npm" "skipped" "publish_script_missing" "-" "$RELEASE_SHA"
    return
  fi

  if ! command -v node >/dev/null 2>&1 || ! command -v pnpm >/dev/null 2>&1; then
    log_warn "Node.js or pnpm not found; skipping web packages"
    append_state "actr-web" "sdk" "npm" "skipped" "toolchain_missing" "-" "$RELEASE_SHA"
    return
  fi

  log_info "Installing web dependencies (skipping Puppeteer browser download)"
  (
    cd "$web_root"
    PUPPETEER_SKIP_DOWNLOAD=true \
    PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    pnpm install --frozen-lockfile
  )

  publish_args=()

  if [[ "$DRY_RUN" == true ]]; then
    log_info "Web package dry-run validation"
    (
      cd "$web_root"
      # Validate metadata without contacting npm
      # (npm publish --dry-run rejects already-published versions,
      #  and pnpm pack --dry-run is only available in pnpm >= 10)
      node <<'EOF'
const fs = require("node:fs");
const packages = [
  ["packages/actr-dom/package.json", "@actrium/actr-dom"],
  ["packages/web-sdk/package.json", "@actrium/actr-web"],
  ["packages/web-react/package.json", "@actrium/actr-web-react"],
];
for (const [path, expectedName] of packages) {
  const pkg = JSON.parse(fs.readFileSync(path, "utf8"));
  if (pkg.name !== expectedName) throw new Error(path + ": expected " + expectedName + ", got " + pkg.name);
  if (!pkg.version) throw new Error(path + ": missing version");
  if (pkg.publishConfig?.access !== "public") throw new Error(path + ": publishConfig.access must be public");
  console.log("  OK " + pkg.name + "@" + pkg.version);
}
console.log("  All web package metadata valid");
EOF
    )
    append_state "actr-web" "sdk" "npm" "success" "dry_run_validated" "https://www.npmjs.com/package/@actrium/actr-web" "$RELEASE_SHA"
    return
  fi

  log_info "Publishing web packages to npm"

  # Prepare for npm Trusted Publishing (OIDC).
  # Clear any lingering token-based auth so OIDC takes precedence.
  rm -f "${NPM_CONFIG_USERCONFIG:-}"
  unset NPM_CONFIG_USERCONFIG NODE_AUTH_TOKEN
  npm config set registry https://registry.npmjs.org/

  if [[ "$NPM_DIST_TAG" != "latest" ]]; then
    publish_args+=(--tag "$NPM_DIST_TAG")
  fi

  # Read the actual version from the first web package (updated by update_versions).
  web_version=$(node -p "require('${web_root}/packages/actr-dom/package.json').version")
  publish_args+=(--expected-version "$web_version")

  (
    cd "$web_root"
    bash scripts/publish.sh "${publish_args[@]}"
  )

  append_state "actr-web" "sdk" "npm" "success" "published" "https://www.npmjs.com/package/@actrium/actr-web" "$RELEASE_SHA"
}

publish_typescript_workload_package() {
  local ts_workload_root="$WORK_REPO_ROOT/bindings/typescript/actr-workload"
  local ts_version

  if [[ ! -d "$ts_workload_root" ]]; then
    log_warn "TypeScript workload package directory not found; skipping"
    append_state "actr-ts-workload" "sdk" "npm" "skipped" "directory_missing" "-" "$RELEASE_SHA"
    return
  fi

  if ! command -v npm >/dev/null 2>&1; then
    log_warn "npm not found; skipping TypeScript workload package"
    append_state "actr-ts-workload" "sdk" "npm" "skipped" "toolchain_missing" "-" "$RELEASE_SHA"
    return
  fi

  log_info "Installing TypeScript workload dependencies"
  (cd "$ts_workload_root" && npm ci)

  log_info "Building TypeScript workload package"
  (cd "$ts_workload_root" && npm run build)
  (
    cd "$ts_workload_root"
    test -f dist/index.js
    test -f dist/index.d.ts
    test -f dist/cli.js
  )

  ts_version=$(node -p "require('${ts_workload_root}/package.json').version")

  if [[ "$DRY_RUN" == true ]]; then
    log_info "TypeScript workload package dry-run validation"
    local pkg_name
    pkg_name=$(node -p "require('${ts_workload_root}/package.json').name")
    if [[ "$pkg_name" != "@actrium/actr-workload" ]]; then
      fail "Expected package name @actrium/actr-workload, got ${pkg_name}"
    fi
    log_info "  OK ${pkg_name}@${ts_version}"
    append_state "actr-ts-workload" "sdk" "npm" "success" "dry_run_validated" "https://www.npmjs.com/package/@actrium/actr-workload" "$RELEASE_SHA"
    return
  fi

  log_info "Publishing TypeScript workload package to npm"

  # Prepare for npm Trusted Publishing (OIDC).
  rm -f "${NPM_CONFIG_USERCONFIG:-}"
  unset NPM_CONFIG_USERCONFIG NODE_AUTH_TOKEN
  npm config set registry https://registry.npmjs.org/

  local npm_tag="$NPM_DIST_TAG"

  # Check if already published.
  if npm view "@actrium/actr-workload@${ts_version}" version >/dev/null 2>&1; then
    log_info "@actrium/actr-workload@${ts_version} already exists; skipping"
    append_state "actr-ts-workload" "sdk" "npm" "success" "already_published" "https://www.npmjs.com/package/@actrium/actr-workload" "$RELEASE_SHA"
    return
  fi

  (cd "$ts_workload_root" && npm publish --access public --tag "$npm_tag")

  # Visibility verification.
  local attempt
  for attempt in $(seq 1 20); do
    if npm view "@actrium/actr-workload@${ts_version}" version >/dev/null 2>&1; then
      log_info "@actrium/actr-workload@${ts_version} is visible on npm"
      break
    fi
    if [[ "$attempt" -eq 20 ]]; then
      append_state "actr-ts-workload" "sdk" "npm" "failure" "visibility_timeout" "https://www.npmjs.com/package/@actrium/actr-workload" "$RELEASE_SHA"
      fail "Timed out waiting for @actrium/actr-workload@${ts_version} to become visible on npm"
    fi
    log_info "Waiting for @actrium/actr-workload@${ts_version} visibility (${attempt}/20)"
    sleep 15
  done

  append_state "actr-ts-workload" "sdk" "npm" "success" "published" "https://www.npmjs.com/package/@actrium/actr-workload" "$RELEASE_SHA"
}

publish_typescript_package() {
  local ts_root="$WORK_REPO_ROOT/bindings/typescript"
  local ts_version main_package cargo_version npm_tag
  local native_packages=(
    "@actrium/actr-darwin-x64|darwin-x64|actr.darwin-x64.node"
    "@actrium/actr-darwin-arm64|darwin-arm64|actr.darwin-arm64.node"
    "@actrium/actr-linux-x64-gnu|linux-x64-gnu|actr.linux-x64-gnu.node"
    "@actrium/actr-linux-x64-musl|linux-x64-musl|actr.linux-x64-musl.node"
    "@actrium/actr-linux-arm64-gnu|linux-arm64-gnu|actr.linux-arm64-gnu.node"
    "@actrium/actr-linux-arm64-musl|linux-arm64-musl|actr.linux-arm64-musl.node"
    "@actrium/actr-win32-x64-msvc|win32-x64-msvc|actr.win32-x64-msvc.node"
  )

  if [[ ! -d "$ts_root" ]]; then
    log_warn "TypeScript package directory not found; skipping"
    append_state "@actrium/actr" "sdk" "npm" "skipped" "directory_missing" "-" "$RELEASE_SHA"
    return
  fi

  if ! command -v npm >/dev/null 2>&1; then
    log_warn "npm not found; skipping TypeScript package"
    append_state "@actrium/actr" "sdk" "npm" "skipped" "toolchain_missing" "-" "$RELEASE_SHA"
    return
  fi

  log_info "Installing TypeScript dependencies"
  (cd "$ts_root" && npm install)

  ts_version=$(node -p "require('${ts_root}/package.json').version")
  main_package=$(node -p "require('${ts_root}/package.json').name")
  if [[ "$main_package" != "@actrium/actr" ]]; then
    fail "Expected package name @actrium/actr, got ${main_package}"
  fi

  cargo_version=$(python3 - "$ts_root/Cargo.toml" <<'PY'
from __future__ import annotations
import re
import sys

content = open(sys.argv[1], encoding="utf-8").read()
match = re.search(r'^version = "([^"]+)"$', content, re.M)
if not match:
    raise SystemExit("failed to read Cargo.toml version")
print(match.group(1))
PY
)
  if [[ "$ts_version" != "$cargo_version" ]]; then
    fail "Version mismatch: package.json=${ts_version}, Cargo.toml=${cargo_version}"
  fi

  if [[ "$DRY_RUN" != true && "$ts_version" != "$VERSION" ]]; then
    fail "Expected TypeScript version ${VERSION}, but repository version is ${ts_version}"
  fi

  log_info "Preparing TypeScript native package layout"
  (
    cd "$ts_root"
    npm run compile:ts
    npx napi create-npm-dirs
    if [[ -d artifacts ]]; then
      npm run artifacts -- --output-dir artifacts
    elif [[ "$DRY_RUN" == true ]]; then
      log_info "No native artifacts directory; validating TypeScript package metadata only in dry-run"
    else
      fail "TypeScript native artifacts directory is required for publish"
    fi
  )

  local descriptor package dir artifact
  local missing_native_artifacts=false
  for descriptor in "${native_packages[@]}"; do
    IFS='|' read -r package dir artifact <<<"$descriptor"
    if [[ ! -f "$ts_root/npm/$dir/$artifact" ]]; then
      missing_native_artifacts=true
      if [[ "$DRY_RUN" != true ]]; then
        fail "Missing TypeScript native artifact: npm/${dir}/${artifact}"
      fi
    fi
  done

  if [[ "$DRY_RUN" == true ]]; then
    log_info "TypeScript package dry-run validation"
    for descriptor in "${native_packages[@]}"; do
      IFS='|' read -r package dir artifact <<<"$descriptor"
      if [[ "$missing_native_artifacts" == true ]]; then
        node - "$ts_root/npm/$dir/package.json" "$package" "$ts_version" <<'NODE'
const fs = require("node:fs");
const [packageJsonPath, expectedName, expectedVersion] = process.argv.slice(2);
const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8"));
if (packageJson.name !== expectedName) {
  throw new Error(`${packageJsonPath}: expected ${expectedName}, got ${packageJson.name}`);
}
if (packageJson.version !== expectedVersion) {
  throw new Error(`${packageJsonPath}: expected ${expectedVersion}, got ${packageJson.version}`);
}
NODE
      else
        (cd "$ts_root" && npm pack "./npm/$dir" --dry-run >/dev/null)
      fi
      append_state "$package" "sdk" "npm" "success" "dry_run_validated" "$(npm_registry_url "$package")" "$RELEASE_SHA"
    done
    # Validate the package locally without contacting npm. `npm publish --dry-run`
    # rejects versions that are already published, which makes PR verification
    # fail for the current workspace version even though no publish would occur.
    (cd "$ts_root" && npm pack --dry-run --ignore-scripts >/dev/null)
    append_state "@actrium/actr" "sdk" "npm" "success" "dry_run_validated" "$(npm_registry_url "@actrium/actr")" "$RELEASE_SHA"
    return
  fi

  log_info "Publishing TypeScript packages to npm"

  # Prepare for npm Trusted Publishing (OIDC).
  rm -f "${NPM_CONFIG_USERCONFIG:-}"
  unset NPM_CONFIG_USERCONFIG NODE_AUTH_TOKEN
  npm config set registry https://registry.npmjs.org/

  npm_tag="$NPM_DIST_TAG"

  # Publish native platform packages first, then the main package.
  log_info "Publishing native platform packages"
  local publish_results=()
  for descriptor in "${native_packages[@]}"; do
    IFS='|' read -r package dir artifact <<<"$descriptor"
    if npm view "${package}@${ts_version}" version >/dev/null 2>&1; then
      log_info "${package}@${ts_version} already exists; skipping"
      publish_results+=("$package|already_published")
      continue
    fi
    (cd "$ts_root" && npm publish "./npm/$dir" --access public --tag "$npm_tag")
    publish_results+=("$package|published")
  done

  # Publish the main @actrium/actr package.
  log_info "Publishing @actrium/actr@${ts_version} (tag: ${npm_tag})"

  if npm view "@actrium/actr@${ts_version}" version >/dev/null 2>&1; then
    log_info "@actrium/actr@${ts_version} already exists; skipping"
    publish_results+=("@actrium/actr|already_published")
  else
    (cd "$ts_root" && npm publish --access public --tag "$npm_tag" --ignore-scripts)
    publish_results+=("@actrium/actr|published")
  fi

  # Visibility verification for all 8 packages.
  local result pkg mode attempt
  for result in "${publish_results[@]}"; do
    IFS='|' read -r pkg mode <<<"$result"
    for attempt in $(seq 1 20); do
      if npm view "${pkg}@${ts_version}" version >/dev/null 2>&1; then
        log_info "${pkg}@${ts_version} is visible on npm"
        append_state "$pkg" "sdk" "npm" "success" "$mode" "$(npm_registry_url "$pkg")" "$RELEASE_SHA"
        break
      fi
      if [[ "$attempt" -eq 20 ]]; then
        append_state "$pkg" "sdk" "npm" "failure" "visibility_timeout" "$(npm_registry_url "$pkg")" "$RELEASE_SHA"
        fail "Timed out waiting for ${pkg}@${ts_version} to become visible on npm"
      fi
      log_info "Waiting for ${pkg}@${ts_version} visibility (${attempt}/20)"
      sleep 15
    done
  done
}

publish_package_sync_repo() {
  local _language=$1
  local repo=$2
  local workflow=$3
  local release_url dispatched_at run_url
  release_url=$(package_sync_release_url "$repo")

  if [[ "$DRY_RUN" == true ]]; then
    append_state "$repo" "sdk" "package_sync" "success" "dry_run_validated" "$release_url" "$RELEASE_SHA"
    return
  fi

  dispatched_at=$(dispatch_package_sync_workflow "$repo" "$workflow")
  if ! run_url=$(wait_for_package_sync_workflow "$repo" "$workflow" "$dispatched_at"); then
    append_state "$repo" "sdk" "package_sync" "failure" "workflow_failed" "$release_url" "$RELEASE_SHA"
    fail "Package sync workflow failed for ${repo}"
  fi

  append_state "$repo" "sdk" "package_sync" "success" "$run_url" "$release_url" "$RELEASE_SHA"
}

stage_release_version_files() {
  git add Cargo.toml Cargo.lock \
    bindings/web/Cargo.toml \
    core/protocol/Cargo.toml \
    core/service-compat/Cargo.toml \
    core/config/Cargo.toml \
    core/framework/Cargo.toml \
    core/runtime-mailbox/Cargo.toml \
    core/runtime/Cargo.toml \
    core/platform-traits/Cargo.toml \
    core/pack/Cargo.toml \
    core/hyper/Cargo.toml \
    core/platform-native/Cargo.toml \
    testing/mock-actrix/Cargo.toml \
    tools/protoc-gen/rust/Cargo.toml \
    tools/protoc-gen/web/Cargo.toml \
    tools/protoc-gen/swift/Sources/framework-codegen-swift/main.swift \
    tools/protoc-gen/typescript/src/main.ts \
    tools/protoc-gen/typescript/package.json \
    tools/protoc-gen/typescript/package-lock.json \
    tools/protoc-gen/kotlin/build.gradle.kts \
    tools/protoc-gen/kotlin/src/main/kotlin/io/actrium/codegen/Main.kt \
    cli/Cargo.toml \
    tools/protoc-gen/python/pyproject.toml \
    bindings/web/packages/actr-dom/package.json \
    bindings/web/packages/web-sdk/package.json \
    bindings/web/packages/web-react/package.json \
    bindings/typescript/Cargo.toml \
    bindings/typescript/package.json \
    bindings/typescript/actr-workload/package.json \
    bindings/web/crates/actr-web-abi/Cargo.toml \
    bindings/web/crates/common/Cargo.toml \
    bindings/web/crates/sw-host/Cargo.toml \
    bindings/web/crates/dom-bridge/Cargo.toml \
    bindings/web/crates/mailbox-web/Cargo.toml \
    bindings/web/crates/platform-web/Cargo.toml \
    bindings/web/crates/framework-web-entry-smoke/Cargo.toml \
    bindings/web/Cargo.lock \
    bindings/typescript/Cargo.lock
}

commit_release_prepare() {
  if git diff --quiet; then
    log_info "Version files already match ${VERSION}; skipping release prepare commit"
    set_release_sha
    return
  fi

  configure_git_identity
  stage_release_version_files
  git commit -m "chore(release): basic train v${VERSION}"
  set_release_sha
}

ensure_versions_prepared() {
  local check_path previous_work_repo_root diff_files
  check_path=$(mktemp -d "${TMPDIR:-/tmp}/actr-release-version-check.XXXXXX")
  git -C "$ORIGINAL_REPO_ROOT" worktree add --detach "$check_path" HEAD >/dev/null

  previous_work_repo_root="$WORK_REPO_ROOT"
  WORK_REPO_ROOT="$check_path"
  update_versions

  diff_files=$(git -C "$check_path" diff --name-only)

  git -C "$ORIGINAL_REPO_ROOT" worktree remove --force "$check_path" >/dev/null
  WORK_REPO_ROOT="$previous_work_repo_root"

  if [[ -n "$diff_files" ]]; then
    printf '%s\n' "$diff_files" >&2
    fail "Release version files do not match ${VERSION}; run scripts/release-train.sh --branch ${RELEASE_BRANCH} --version ${VERSION} --prepare-only on a PR branch and merge it before publishing"
  fi
}

ensure_publish_worktree_clean() {
  local dirty_files
  dirty_files=$(
    git status --porcelain --untracked-files=normal -- . \
      ":(exclude)release/reports/release-train-v${VERSION}.state.tsv" \
      ":(exclude)release/reports/release-train-v${VERSION}.md" \
      ":(exclude)release/reports/release-train-v${VERSION}.json" \
      ":(exclude)release/reports/release-train-v${VERSION}.*.state.tsv" \
      ":(exclude)release/reports/release-train-v${VERSION}.context.json" \
      ":(exclude)cli/assets/web-runtime/"
  )
  if [[ -n "$dirty_files" ]]; then
    printf '%s\n' "$dirty_files" >&2
    fail "Release validation modified files; include these changes in the release prepare PR before publishing"
  fi
}

crate_registry_url() {
  printf 'https://crates.io/crates/%s/%s' "$1" "$VERSION"
}

python_registry_url() {
  if [[ "$PRE_RELEASE" == true ]]; then
    printf 'https://test.pypi.org/project/%s/%s/' "$PYTHON_PACKAGE_NAME" "$VERSION"
  else
    printf 'https://pypi.org/project/%s/%s/' "$PYTHON_PACKAGE_NAME" "$VERSION"
  fi
}

npm_registry_url() {
  printf 'https://www.npmjs.com/package/%s' "$1"
}

registry_user_agent() {
  printf 'actr-release-train/%s (https://github.com/Actrium/actr)' "${VERSION:-unknown}"
}

crate_version_visible() {
  curl -A "$(registry_user_agent)" -fsSLo /dev/null "${CRATES_IO_API}/$1/${VERSION}"
}

python_version_visible() {
  local api_url="$PYPI_API"
  if [[ "$PRE_RELEASE" == true ]]; then
    api_url="$TEST_PYPI_API"
  fi

  curl -A "$(registry_user_agent)" -fsSLo /dev/null "${api_url}/${PYTHON_PACKAGE_NAME}/${VERSION}/json"
}

wait_for_visibility() {
  local component=$1
  local kind=$2
  local attempt=1
  local max_attempts=30

  while (( attempt <= max_attempts )); do
    if [[ "$kind" == "crate" ]]; then
      if crate_version_visible "$component"; then
        return 0
      fi
    else
      if python_version_visible; then
        return 0
      fi
    fi

    log_info "Waiting for ${component} ${VERSION} visibility (${attempt}/${max_attempts})"
    sleep 10
    attempt=$(( attempt + 1 ))
  done

  return 1
}

publish_rust_package() {
  local package=$1
  local stage=$2
  local registry_url
  registry_url=$(crate_registry_url "$package")

  if crate_version_visible "$package"; then
    log_info "Skipping ${package}; version already visible"
    append_state "$package" "$stage" "crate" "success" "already_published" "$registry_url" "$RELEASE_SHA"
    return
  fi

  if [[ "$DRY_RUN" == true ]]; then
    append_state "$package" "$stage" "crate" "success" "dry_run_validated" "$registry_url" "$RELEASE_SHA"
    return
  fi

  if [[ "$package" == "actr-cli" ]]; then
    prepare_cli_publish_assets
  fi

  local publish_log
  publish_log=$(mktemp)
  if ! (
    cd "$(package_workspace_dir "$package")"
    local publish_args=(publish -p "$package" --locked)
    if [[ "$package" == "actr-cli" ]]; then
      # The CLI package embeds web runtime assets generated during validation.
      publish_args+=(--allow-dirty)
    fi
    cargo "${publish_args[@]}"
  ) 2>&1 | tee "$publish_log"; then
    if grep -qi "already exists" "$publish_log"; then
      append_state "$package" "$stage" "crate" "success" "already_published" "$registry_url" "$RELEASE_SHA"
      rm -f "$publish_log"
      return
    fi

    if grep -qiE "Uploaded[[:space:]]+${package} v${VERSION}" "$publish_log"; then
      log_warn "cargo publish uploaded ${package} ${VERSION} but returned non-zero while waiting for registry visibility"
    else
      rm -f "$publish_log"
      append_state "$package" "$stage" "crate" "failure" "publish_failed" "$registry_url" "$RELEASE_SHA"
      fail "cargo publish failed for ${package}"
    fi
  fi

  rm -f "$publish_log"

  if ! wait_for_visibility "$package" "crate"; then
    append_state "$package" "$stage" "crate" "failure" "visibility_timeout" "$registry_url" "$RELEASE_SHA"
    fail "Timed out waiting for ${package} ${VERSION} to become visible on crates.io"
  fi

  append_state "$package" "$stage" "crate" "success" "published" "$registry_url" "$RELEASE_SHA"
}

publish_python_package() {
  local registry_url
  registry_url=$(python_registry_url)

  if python_version_visible; then
    log_info "Skipping ${PYTHON_PACKAGE_NAME}; version already visible"
    append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "success" "already_published" "$registry_url" "$RELEASE_SHA"
    return
  fi

  if [[ "$DRY_RUN" == true ]]; then
    append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "success" "dry_run_validated" "$registry_url" "$RELEASE_SHA"
    return
  fi

  local twine_password="${PYPI_API_TOKEN:-}"
  if [[ "$PRE_RELEASE" == true ]]; then
    twine_password="${TEST_PYPI_API_TOKEN:-}"
  fi

  if [[ -z "$twine_password" ]]; then
    local token_name="PYPI_API_TOKEN"
    if [[ "$PRE_RELEASE" == true ]]; then
      token_name="TEST_PYPI_API_TOKEN"
    fi
    log_warn "Skipping ${PYTHON_PACKAGE_NAME}; ${token_name} not set"
    append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "skipped" "pypi_token_missing" "$registry_url" "$RELEASE_SHA"
    return
  fi

  log_info "Building Python package for publishing"
  build_python_distribution

  local upload_log
  upload_log=$(mktemp)
  (
    cd tools/protoc-gen/python
    if [[ "$PRE_RELEASE" == true ]]; then
      TWINE_USERNAME="__token__" \
      TWINE_PASSWORD="$twine_password" \
      "$RELEASE_PYTHON_BIN" -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
    else
      TWINE_USERNAME="__token__" \
      TWINE_PASSWORD="$twine_password" \
      "$RELEASE_PYTHON_BIN" -m twine upload dist/*
    fi
  ) 2>&1 | tee "$upload_log"
  local twine_status=${PIPESTATUS[0]}

  if [[ $twine_status -ne 0 ]]; then
    if grep -qi "already exist" "$upload_log"; then
      append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "success" "already_published" "$registry_url" "$RELEASE_SHA"
      rm -f "$upload_log"
      return
    fi

    rm -f "$upload_log"
    append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "failure" "publish_failed" "$registry_url" "$RELEASE_SHA"
    fail "twine upload failed for ${PYTHON_PACKAGE_NAME}"
  fi

  rm -f "$upload_log"

  if ! wait_for_visibility "$PYTHON_PACKAGE_NAME" "python"; then
    append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "failure" "visibility_timeout" "$registry_url" "$RELEASE_SHA"
    fail "Timed out waiting for ${PYTHON_PACKAGE_NAME} ${VERSION} to become visible on PyPI"
  fi

  append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "success" "published" "$registry_url" "$RELEASE_SHA"
}

skip_python_package() {
  append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "skipped" "skip_python" "$(python_registry_url)" "$RELEASE_SHA"
}

create_final_tag() {
  if [[ "$DRY_RUN" == true ]]; then
    log_info "Dry-run: skipping tag creation (tag: ${FINAL_TAG})"
    return
  fi

  if [[ "${TAG_ALREADY_EXISTS:-false}" == "true" ]]; then
    log_info "Tag ${FINAL_TAG} already exists on target commit; skipping creation"
    return
  fi

  git tag "$FINAL_TAG"
  git push origin "$FINAL_TAG"
}

# ---------------------------------------------------------------------------
# Staged execution
# ---------------------------------------------------------------------------


stage_create_tag() {
  # Staged CI must enforce the same version preparation invariant as the
  # sequential release train before it creates an externally visible tag.
  ensure_versions_prepared
  set_release_sha
  write_context
  read_context
  FINAL_TAG="${FINAL_TAG_PREFIX}${VERSION}"

  create_final_tag

  log_info "Stage create-tag complete (tag: ${FINAL_TAG})"
}

stage_publish_rust() {
  read_context

  local package
  for package in "${FOUNDATION_CRATES[@]}"; do
    publish_rust_package "$package" "foundation"
  done

  for package in "${PROTOC_CRATES[@]}"; do
    publish_rust_package "$package" "protoc-gen"
  done

  for package in "${SDK_CRATES[@]}"; do
    publish_rust_package "$package" "sdk"
  done

  for package in "${CLI_CRATES[@]}"; do
    publish_rust_package "$package" "cli"
  done

  log_info "Stage publish-rust complete"
}

# ---------------------------------------------------------------------------
# CLI binary GitHub Release stages
# ---------------------------------------------------------------------------

stage_build_cli_binaries() {
  read_context

  local staging="$WORK_REPO_ROOT/release/reports/cli-binaries"
  local descriptors=() descriptor target
  if [[ -n "${ACTR_CLI_TARGET_DESCRIPTOR:-}" ]]; then
    descriptors+=("$ACTR_CLI_TARGET_DESCRIPTOR")
  else
    descriptors+=("${CLI_BINARY_TARGETS[@]}")
  fi

  for descriptor in "${descriptors[@]}"; do
    target=$(printf "%s" "$descriptor" | cut -d"|" -f1)
    local args=(build --target "$target" --version "$VERSION" --output "$staging" --state-file "$STATE_FILE")
    [[ "$DRY_RUN" == true ]] && args+=(--dry-run)
    RELEASE_SHA="$RELEASE_SHA" PACKAGE_SYNC_OWNER="$PACKAGE_SYNC_OWNER" \
      scripts/release-actr-cli.sh "${args[@]}"
  done

  log_info "Stage build-cli-binaries complete"
}

stage_publish_cli_binaries() {
  read_context

  local staging="$WORK_REPO_ROOT/release/reports/cli-binaries"
  local args=(publish --tag "$FINAL_TAG" --assets "$staging" --state-file "$STATE_FILE" --replace --create-release)
  [[ "$DRY_RUN" == true ]] && args+=(--dry-run)
  RELEASE_SHA="$RELEASE_SHA" PRE_RELEASE="$PRE_RELEASE" PACKAGE_SYNC_OWNER="$PACKAGE_SYNC_OWNER" \
    scripts/release-actr-cli.sh "${args[@]}"

  log_info "Stage publish-cli-binaries complete"
}

stage_publish_python() {
  read_context

  if [[ "$SKIP_PYTHON" == false ]]; then
    publish_python_package
  else
    skip_python_package
  fi

  log_info "Stage publish-python complete"
}

stage_publish_swift() {
  read_context

  publish_package_sync_repo "swift" "$SWIFT_PACKAGE_SYNC_REPO" "release.yml"

  log_info "Stage publish-swift complete"
}

stage_publish_kotlin() {
  read_context

  publish_package_sync_repo "kotlin" "$KOTLIN_PACKAGE_SYNC_REPO" "release.yml"

  log_info "Stage publish-kotlin complete"
}

stage_publish_web() {
  read_context

  if [[ "$SKIP_WEB" != true ]]; then
    publish_web_packages
  fi

  log_info "Stage publish-web complete"
}

stage_build_typescript_native() {
  read_context

  # Build step: compile TypeScript and prepare NAPI artifacts directory.
  local ts_root="$WORK_REPO_ROOT/bindings/typescript"
  if [[ -d "$ts_root" ]]; then
    log_info "Compiling TypeScript"
    (cd "$ts_root" && npm install && npm run compile:ts)
    log_info "Stage build-typescript-native complete"
  else
    log_warn "TypeScript directory not found; skipping build"
  fi
}

stage_publish_typescript_workload() {
  read_context

  if [[ "$SKIP_WEB" != true ]]; then
    publish_typescript_workload_package
  fi

  log_info "Stage publish-typescript-workload complete"
}

stage_publish_typescript() {
  read_context

  if [[ "$SKIP_WEB" != true ]]; then
    publish_typescript_package
  fi

  log_info "Stage publish-typescript complete"
}

stage_report() {
  read_context

  # Report stage merges all per-stage state files.
  # generate_report (called via on_exit trap) will handle the merge.
  log_info "Stage report: collecting all stage results"

  # List all stage state files found.
  local stage_name
  for stage_name in "${VALID_STAGES[@]}"; do
    local sf
    sf="$(stage_state_file "$stage_name")"
    if [[ -f "$sf" ]]; then
      log_info "  Found: $(basename "$sf")"
    fi
  done

  log_info "Stage report complete"
}

stage_notify_wechat() {
  # This stage reads the consolidated report JSON directly instead of
  # requiring the release context file (which may not be available in
  # post-report CI jobs). All necessary flags come from CLI args.

  local report_json="$REPORT_DIR/release-train-v${VERSION}.json"

  if [[ ! -f "$report_json" ]]; then
    log_warn "Report JSON not found at ${report_json}; skipping WeChat notification"
    return
  fi

  local webhook_url="${RELEASE_WEBHOOK_URL:?RELEASE_WEBHOOK_URL must be set for WeChat notification}"

  python3 - "$report_json" "$webhook_url" "$VERSION" "$DRY_RUN" "$PRE_RELEASE" <<'PY'
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
webhook_url = sys.argv[2]
version = sys.argv[3]
is_dry_run = sys.argv[4] == "true"
is_pre_release = sys.argv[5] == "true"

with report_path.open() as f:
    report = json.load(f)

components = report.get("components", [])
overall_status = report.get("overall_status", "unknown")

# Filter successful and failed components.
successful = [c for c in components if c["status"] == "success"]
failed = [c for c in components if c["status"] == "failure"]
skipped = [c for c in components if c["status"] == "skipped"]

# Build WeChat Work markdown message.
if is_dry_run:
    mode_label = '<font color="comment">Dry Run</font>'
elif is_pre_release:
    mode_label = '<font color="warning">Pre-release</font>'
else:
    mode_label = '<font color="info">Release</font>'

if overall_status == "success":
    status_icon = "✅"
    status_color = "info"
else:
    status_icon = "❌"
    status_color = "warning"

total = len(components)
success_count = len(successful)

lines = [
    f'{status_icon} Actr Release Train <font color="{status_color}">{overall_status}</font>',
    f"> Version: <font color=\"info\">v{version}</font>",
    f"> Mode: {mode_label}",
    f"> Published: <font color=\"info\">{success_count}</font> / {total}",
]

if failed:
    lines.append(f"> Failed: <font color=\"warning\">{len(failed)}</font>")
if skipped:
    lines.append(f"> Skipped: <font color=\"comment\">{len(skipped)}</font>")

lines.append("")

# Group successful components by kind for a readable summary.
kind_order = [
    ("crate", "Rust Crates"),
    ("python", "Python"),
    ("npm", "npm Packages"),
    ("package_sync", "Package Sync"),
    ("cli_binary", "CLI Binaries (GitHub Release)"),
]

by_kind = {}
for c in successful:
    kind = c["kind"]
    by_kind.setdefault(kind, []).append(c)

for kind_key, kind_label in kind_order:
    items = by_kind.get(kind_key)
    if not items:
        continue
    lines.append(f"**{kind_label}**")
    for item in items:
        url = item.get("registry_url", "")
        if url and url != "-":
            lines.append(f"> [{item['name']}]({url}) `v{version}`")
        else:
            lines.append(f"> {item['name']} `v{version}`")
    lines.append("")

# List failures if any.
if failed:
    lines.append("**Failed**")
    for item in failed:
        reason = item.get("mode", "unknown")
        lines.append(f"> {item['name']}: <font color=\"warning\">{reason}</font>")
    lines.append("")

content = "\n".join(lines)

payload = {
    "msgtype": "markdown",
    "markdown": {
        "content": content,
    },
}

result = subprocess.run(
    [
        "curl",
        "-s",
        "-X", "POST",
        webhook_url,
        "-H", "Content-Type: application/json",
        "-d", json.dumps(payload, ensure_ascii=False),
    ],
    capture_output=True,
    text=True,
)

print(f"WeChat webhook response: {result.stdout}")
if result.returncode != 0:
    print(f"WeChat webhook error: {result.stderr}", file=sys.stderr)
    sys.exit(1)
PY

  log_info "Stage notify-wechat complete"
}

# ---------------------------------------------------------------------------
# Main entry: full pipeline or single stage
# ---------------------------------------------------------------------------

run_release_train() {
  if [[ "$PREPARE_ONLY" == true ]]; then
    update_versions
    cargo update --workspace
    cargo update --workspace --manifest-path bindings/web/Cargo.toml
    cargo update --manifest-path bindings/typescript/Cargo.toml -p actr-protocol -p actr-framework -p actr-config -p actr-hyper
    run_validation_suite
    commit_release_prepare
    return
  fi

  # Staged execution: run a single stage.
  if [[ "$STAGE" != "all" ]]; then
    "stage_${STAGE//-/_}"
    return
  fi

  # Full sequential pipeline (--stage all or no --stage).
  if [[ "$DRY_RUN" == true ]]; then
    update_versions
  else
    ensure_versions_prepared
  fi

  if [[ "$DRY_RUN" == false ]]; then
    ensure_publish_worktree_clean
  fi
  set_release_sha
  # Persist the release context so stage functions (e.g.
  # stage_publish_cli_binaries) invoked later in the full pipeline can
  # read_context without requiring a separate --stage create-tag run.
  write_context
  append_skipped_components

  local package
  for package in "${FOUNDATION_CRATES[@]}"; do
    publish_rust_package "$package" "foundation"
  done

  for package in "${PROTOC_CRATES[@]}"; do
    publish_rust_package "$package" "protoc-gen"
  done

  if [[ "$SKIP_PYTHON" == false ]]; then
    publish_python_package
  else
    skip_python_package
  fi

  for package in "${SDK_CRATES[@]}"; do
    publish_rust_package "$package" "sdk"
  done

  for package in "${CLI_CRATES[@]}"; do
    publish_rust_package "$package" "cli"
  done

  create_final_tag
  publish_package_sync_repo "swift" "$SWIFT_PACKAGE_SYNC_REPO" "release.yml"
  publish_package_sync_repo "kotlin" "$KOTLIN_PACKAGE_SYNC_REPO" "release.yml"
  if [[ "$SKIP_WEB" != true ]]; then
    publish_web_packages
  fi

  if [[ "$SKIP_WEB" != true ]]; then
    publish_typescript_workload_package
    publish_typescript_package
  fi

  # Build and attach CLI binaries to the GitHub Release created for v${VERSION}.
  stage_build_cli_binaries
  stage_publish_cli_binaries
}

main() {
  require_command git
  require_command cargo
  require_command curl
  require_command python3

  parse_args "$@"

  ORIGINAL_REPO_ROOT=$(git rev-parse --show-toplevel)

  if [[ "$AUTO_VERSION" == true && "$PREPARE_ONLY" == true ]] && release_prepare_should_skip_current_head; then
    log_info "Current HEAD is a release commit; skipping automatic release prepare"
    exit 0
  fi

  if [[ "$AUTO_VERSION" == true ]]; then
    if [[ "$PREPARE_ONLY" == true ]]; then
      local current_ver bump
      current_ver=$(current_workspace_version)
      bump=$(detect_conventional_bump)
      if [[ "$bump" == "none" ]]; then
        log_info "No publishable conventional-commit changes since last release; nothing to prepare"
        exit 0
      fi
      VERSION=$(calculate_next_version "$current_ver" "$bump")
      log_info "Auto-detected version: ${VERSION} (bump: ${bump}, current: ${current_ver})"
    else
      VERSION=$(current_workspace_version)
      log_info "Using workspace version: ${VERSION}"
    fi
  fi

  validate_version
  configure_release_channel
  ensure_clean_worktree
  prepare_paths
  prepare_worktree
  PREVIOUS_TAG=$(previous_release_tag)
  validate_maintenance_release_policy
  if stage_requires_tag_availability_check; then
    ensure_release_tag_available
  else
    FINAL_TAG="${FINAL_TAG_PREFIX}${VERSION}"
  fi
  resolve_package_sync_owner

  # Stage-specific secret requirements.
  case "$STAGE" in
    all)
      if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]] && [[ "$DRY_RUN" == false && "$PREPARE_ONLY" == false ]]; then
        fail "CARGO_REGISTRY_TOKEN must be set for publishing"
      fi
      if [[ -z "${PACKAGE_SYNC_GITHUB_TOKEN:-}" ]] && [[ "$DRY_RUN" == false && "$PREPARE_ONLY" == false ]]; then
        fail "PACKAGE_SYNC_GITHUB_TOKEN must be set for package-sync publishing"
      fi
      ;;
    publish-rust)
      if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]] && [[ "$DRY_RUN" == false ]]; then
        fail "CARGO_REGISTRY_TOKEN must be set for publish-rust stage"
      fi
      ;;
    publish-swift|publish-kotlin)
      if [[ -z "${PACKAGE_SYNC_GITHUB_TOKEN:-}" ]] && [[ "$DRY_RUN" == false ]]; then
        fail "PACKAGE_SYNC_GITHUB_TOKEN must be set for ${STAGE} stage"
      fi
      ;;
    publish-python)
      # PYPI_API_TOKEN is optional: when unset, the Python package publish is skipped.
      ;;
    publish-cli-binaries)
      if [[ -z "${GITHUB_TOKEN:-${RELEASE_GITHUB_TOKEN:-}}" ]] && [[ "$DRY_RUN" == false ]]; then
        fail "GITHUB_TOKEN must be set for publish-cli-binaries stage"
      fi
      ;;
  esac

  if stage_requires_python_release_tools; then
    install_python_release_tools
  else
    log_info "Skipping Python release tool installation"
  fi
  run_release_train
}

main "$@"
