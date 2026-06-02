#!/usr/bin/env bash
set -euo pipefail

# Basic release train for the monorepo-managed foundations, tools, SDKs, and CLI.

readonly FINAL_TAG_PREFIX="release-train-v"
readonly PYTHON_PACKAGE_NAME="framework_codegen_python"
readonly CRATES_IO_API="https://crates.io/api/v1/crates"
readonly PYPI_API="https://pypi.org/pypi"

readonly FOUNDATION_CRATES=(
  "actr-protocol"
  "actr-service-compat"
  "actr-config"
  "actr-web-abi"
  "actr-framework"
  "actr-runtime-mailbox"
  "actr-runtime"
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

readonly OPTIONAL_SKIPPED_COMPONENTS=(
  "actr-ts|sdk|external_repo_not_managed_in_monorepo"
)
readonly PACKAGE_SYNC_GITHUB_API="https://api.github.com"
readonly SWIFT_PACKAGE_SYNC_REPO="actr-swift-package-sync"
readonly KOTLIN_PACKAGE_SYNC_REPO="actr-kotlin-package-sync"

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
SKIP_PYTHON=false
RUN_MODE="publish"
OVERALL_STATUS="success"
FAILURE_REASON=""
RELEASE_SHA=""
FINAL_TAG=""
PACKAGE_SYNC_OWNER="${PACKAGE_SYNC_OWNER:-}"

usage() {
  cat <<'EOF'
Usage:
  scripts/release-train-cli-protoc.sh --version <X.Y.Z> [--dry-run] [--skip-python]

Options:
  --version <X.Y.Z>  Stable semver used by the monorepo-managed release train.
  --dry-run          Validate the full flow in a disposable worktree without publishing.
  --skip-python      Skip Python package validation, version update, and publishing.
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
      --skip-python)
        SKIP_PYTHON=true
        shift
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
    usage
    fail "Missing required --version"
  fi

  if [[ "$DRY_RUN" == true ]]; then
    RUN_MODE="dry-run"
  fi
}

validate_version() {
  [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "Version must be a stable semver in X.Y.Z format"
}

ensure_clean_worktree() {
  if [[ -n "$(git -C "$ORIGINAL_REPO_ROOT" status --porcelain)" ]]; then
    fail "Working tree must be clean before running the release train"
  fi
}

prepare_paths() {
  ORIGINAL_REPO_ROOT=$(git rev-parse --show-toplevel)
  WORK_REPO_ROOT="$ORIGINAL_REPO_ROOT"
  REPORT_DIR="$ORIGINAL_REPO_ROOT/release/reports"
  STATE_FILE="$REPORT_DIR/release-train-v${VERSION}.state.tsv"
  REPORT_MARKDOWN="$REPORT_DIR/release-train-v${VERSION}.md"
  REPORT_JSON="$REPORT_DIR/release-train-v${VERSION}.json"

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
  else
    local current_branch current_head origin_main
    current_branch=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse --abbrev-ref HEAD)
    [[ "$current_branch" == "main" ]] || fail "Non-dry-run execution must start from the local main branch"

    current_head=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse HEAD)
    origin_main=$(git -C "$ORIGINAL_REPO_ROOT" rev-parse origin/main)
    [[ "$current_head" == "$origin_main" ]] || fail "Local main must match origin/main before releasing"
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

ensure_release_tag_absent() {
  FINAL_TAG="${FINAL_TAG_PREFIX}${VERSION}"
  if git rev-parse -q --verify "refs/tags/${FINAL_TAG}" >/dev/null 2>&1; then
    fail "Final release tag already exists locally: ${FINAL_TAG}"
  fi

  if git ls-remote --exit-code --tags origin "refs/tags/${FINAL_TAG}" >/dev/null 2>&1; then
    fail "Final release tag already exists on origin: ${FINAL_TAG}"
  fi
}

install_python_release_tools() {
  RELEASE_PYTHON_ENV=$(mktemp -d "${TMPDIR:-/tmp}/actr-release-python.XXXXXX")
  python3 -m venv "$RELEASE_PYTHON_ENV"
  RELEASE_PYTHON_BIN="${RELEASE_PYTHON_ENV}/bin/python"
  "$RELEASE_PYTHON_BIN" -m pip install --quiet --upgrade pip build twine
}

update_versions() {
  python3 - "$WORK_REPO_ROOT" "$VERSION" "$SKIP_PYTHON" <<'PY'
from __future__ import annotations

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
    repo / "tools/protoc-gen/rust/Cargo.toml",
    repo / "tools/protoc-gen/web/Cargo.toml",
    repo / "cli/Cargo.toml",
]

cli_dependency_names = {
    "actr",
    "actr-runtime-mailbox",
    "actr-config",
    "actr-protocol",
    "actr-service-compat",
    "actr-framework-protoc-codegen",
    "actr-web-protoc-codegen",
}

dependency_version_names = {
    "actr-web-abi",
}

workspace_dependency_names = {
    "actr-protocol",
    "actr-service-compat",
    "actr-config",
    "actr-framework",
    "actr-framework-protoc-codegen",
    "actr-runtime",
    "actr-runtime-mailbox",
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
    rm -rf tools/protoc-gen/python/dist tools/protoc-gen/python/build tools/protoc-gen/python/*.egg-info
    (
      cd tools/protoc-gen/python
      "$RELEASE_PYTHON_BIN" -m build >/dev/null
    )
  else
    log_info "Skipping Python package validation"
  fi
}

append_skipped_components() {
  local descriptor name stage reason
  for descriptor in "${OPTIONAL_SKIPPED_COMPONENTS[@]}"; do
    IFS='|' read -r name stage reason <<<"$descriptor"
    append_state "$name" "$stage" "external" "skipped" "$reason" "-" "$RELEASE_SHA"
  done
}

dispatch_package_sync_workflow() {
  local repo=$1
  local workflow=$2
  local dispatched_at

  dispatched_at=$(python3 - <<'PY'
from datetime import datetime, timezone
print(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)
  curl -fsSL \
    -X POST \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${PACKAGE_SYNC_GITHUB_TOKEN}" \
    "${PACKAGE_SYNC_GITHUB_API}/repos/${PACKAGE_SYNC_OWNER}/${repo}/actions/workflows/${workflow}/dispatches" \
    -d @- >/dev/null <<EOF
{
  "ref": "main",
  "inputs": {
    "version": "${VERSION}",
    "source_sha": "${RELEASE_SHA}",
    "source_tag": "${FINAL_TAG}"
  }
}
EOF

  printf '%s\n' "${dispatched_at}"
}

find_package_sync_run_id() {
  local repo=$1
  local workflow=$2
  local dispatched_at=$3

  curl -fsSL \
    -H "Accept: application/vnd.github+json" \
    -H "Authorization: Bearer ${PACKAGE_SYNC_GITHUB_TOKEN}" \
    "${PACKAGE_SYNC_GITHUB_API}/repos/${PACKAGE_SYNC_OWNER}/${repo}/actions/workflows/${workflow}/runs?event=workflow_dispatch&branch=main&per_page=10" \
    | python3 - "$dispatched_at" <<'PY'
from __future__ import annotations

import json
import sys

payload = json.load(sys.stdin)
dispatched_at = sys.argv[1]

for run in payload.get("workflow_runs", []):
    if run.get("created_at", "") >= dispatched_at:
        print(run["id"])
        break
PY
}

wait_for_package_sync_workflow() {
  local repo=$1
  local workflow=$2
  local dispatched_at=$3
  local run_id=""
  local attempt

  for attempt in $(seq 1 30); do
    run_id=$(find_package_sync_run_id "$repo" "$workflow" "$dispatched_at" || true)
    if [[ -n "${run_id}" ]]; then
      break
    fi
    log_info "Waiting for ${repo} workflow run creation (${attempt}/30)"
    sleep 10
  done

  [[ -n "${run_id}" ]] || fail "Failed to locate workflow run for ${repo} after ${dispatched_at}"

  for attempt in $(seq 1 120); do
    local response status conclusion html_url
    response=$(curl -fsSL \
      -H "Accept: application/vnd.github+json" \
      -H "Authorization: Bearer ${PACKAGE_SYNC_GITHUB_TOKEN}" \
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

    log_info "Waiting for ${repo} workflow completion (${attempt}/120)"
    sleep 15
  done

  log_error "Timed out waiting for ${repo} workflow completion"
  return 1
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

commit_and_push_version_bump() {
  if [[ "$DRY_RUN" == true ]]; then
    set_release_sha
    return
  fi

  if git diff --quiet; then
    log_info "Version files already match ${VERSION}; skipping commit"
    set_release_sha
    return
  fi

  configure_git_identity

  git add Cargo.toml Cargo.lock \
    core/protocol/Cargo.toml \
    core/service-compat/Cargo.toml \
    core/config/Cargo.toml \
    core/framework/Cargo.toml \
    core/runtime-mailbox/Cargo.toml \
    core/runtime/Cargo.toml \
    tools/protoc-gen/rust/Cargo.toml \
    tools/protoc-gen/web/Cargo.toml \
    cli/Cargo.toml \
    tools/protoc-gen/python/pyproject.toml
  git commit -m "chore(release): basic train v${VERSION}"
  git push origin main
  set_release_sha
}

crate_registry_url() {
  printf 'https://crates.io/crates/%s/%s' "$1" "$VERSION"
}

python_registry_url() {
  printf 'https://pypi.org/project/%s/%s/' "$PYTHON_PACKAGE_NAME" "$VERSION"
}

registry_user_agent() {
  printf 'actr-release-train/%s (https://github.com/Actrium/actr)' "${VERSION:-unknown}"
}

crate_version_visible() {
  curl -A "$(registry_user_agent)" -fsSLo /dev/null "${CRATES_IO_API}/$1/${VERSION}"
}

python_version_visible() {
  curl -A "$(registry_user_agent)" -fsSLo /dev/null "${PYPI_API}/${PYTHON_PACKAGE_NAME}/${VERSION}/json"
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
    attempt=$((attempt + 1))
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

  local publish_log
  publish_log=$(mktemp)
  if ! (
    cd "$(package_workspace_dir "$package")"
    cargo publish -p "$package" --locked
  ) 2>&1 | tee "$publish_log"; then
    if grep -qi "already exists" "$publish_log"; then
      append_state "$package" "$stage" "crate" "success" "already_published" "$registry_url" "$RELEASE_SHA"
      rm -f "$publish_log"
      return
    fi

    rm -f "$publish_log"
    append_state "$package" "$stage" "crate" "failure" "publish_failed" "$registry_url" "$RELEASE_SHA"
    fail "cargo publish failed for ${package}"
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

  if [[ -z "${PYPI_API_TOKEN:-}" ]]; then
    log_warn "Skipping ${PYTHON_PACKAGE_NAME}; PYPI_API_TOKEN not set"
    append_state "$PYTHON_PACKAGE_NAME" "protoc-gen" "python" "skipped" "pypi_token_missing" "$registry_url" "$RELEASE_SHA"
    return
  fi

  local upload_log
  upload_log=$(mktemp)
  (
    cd tools/protoc-gen/python
    TWINE_USERNAME="__token__" \
    TWINE_PASSWORD="${PYPI_API_TOKEN:-}" \
    "$RELEASE_PYTHON_BIN" -m twine upload dist/*
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
    return
  fi

  git tag "$FINAL_TAG"
  git push origin "$FINAL_TAG"
}

main() {
  require_command git
  require_command cargo
  require_command curl
  require_command python3

  parse_args "$@"
  validate_version

  ORIGINAL_REPO_ROOT=$(git rev-parse --show-toplevel)
  ensure_clean_worktree
  prepare_paths
  prepare_worktree
  ensure_release_tag_absent
  resolve_package_sync_owner

  if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]] && [[ "$DRY_RUN" == false ]]; then
    fail "CARGO_REGISTRY_TOKEN must be set for publishing"
  fi

  # PYPI_API_TOKEN is optional: when unset, the Python package publish is
  # skipped (see publish_python_package) and the train continues.

  if [[ -z "${PACKAGE_SYNC_GITHUB_TOKEN:-}" ]] && [[ "$DRY_RUN" == false ]]; then
    fail "PACKAGE_SYNC_GITHUB_TOKEN must be set for package-sync publishing"
  fi

  if [[ "$SKIP_PYTHON" == false ]]; then
    install_python_release_tools
  else
    log_info "Skipping Python release tool installation"
  fi
  update_versions
  run_validation_suite
  commit_and_push_version_bump
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
}

main "$@"
