#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"

script_under_test=$(mktemp)
sed '/^main "\$@"$/d' scripts/release-train.sh >"$script_under_test"
# shellcheck source=/dev/null
source "$script_under_test"
rm -f "$script_under_test"

# Override fail to throw a catchable error instead of exit.
# The original fail calls exit which triggers the EXIT trap and kills the test runner.
fail() {
  FAILURE_REASON="$*"
  log_error "$*" >&2
  return 1
}

# Save original function definitions so tests can restore after stubbing.
_original_update_versions=$(declare -f update_versions)
_original_run_validation_suite=$(declare -f run_validation_suite)
_original_ensure_clean_worktree=$(declare -f ensure_clean_worktree)
_original_prepare_paths=$(declare -f prepare_paths)
_original_prepare_worktree=$(declare -f prepare_worktree)
_original_ensure_release_tag_available=$(declare -f ensure_release_tag_available)
_original_resolve_package_sync_owner=$(declare -f resolve_package_sync_owner)
_original_install_python_release_tools=$(declare -f install_python_release_tools)
_original_build_python_distribution=$(declare -f build_python_distribution)
_original_prepare_cli_publish_assets=$(declare -f prepare_cli_publish_assets)
_original_ensure_versions_prepared=$(declare -f ensure_versions_prepared)
_original_ensure_publish_worktree_clean=$(declare -f ensure_publish_worktree_clean)
_original_commit_release_prepare=$(declare -f commit_release_prepare)
_original_append_skipped_components=$(declare -f append_skipped_components)
_original_append_state=$(declare -f append_state)
_original_set_release_sha=$(declare -f set_release_sha)
_original_publish_rust_package=$(declare -f publish_rust_package)
_original_publish_python_package=$(declare -f publish_python_package)
_original_python_version_visible=$(declare -f python_version_visible)
_original_wait_for_visibility=$(declare -f wait_for_visibility)
_original_skip_python_package=$(declare -f skip_python_package)
_original_create_final_tag=$(declare -f create_final_tag)
_original_publish_package_sync_repo=$(declare -f publish_package_sync_repo)
_original_publish_web_packages=$(declare -f publish_web_packages)
_original_publish_typescript_workload_package=$(declare -f publish_typescript_workload_package)
_original_publish_typescript_package=$(declare -f publish_typescript_package)
_original_stage_create_tag=$(declare -f stage_create_tag)
_original_stage_publish_rust=$(declare -f stage_publish_rust)
_original_write_context=$(declare -f write_context)
_original_read_context=$(declare -f read_context)
_original_stage_build_cli_binaries=$(declare -f stage_build_cli_binaries)
_original_stage_publish_cli_binaries=$(declare -f stage_publish_cli_binaries)

restore_all_functions() {
  eval "$_original_update_versions"
  eval "$_original_run_validation_suite"
  eval "$_original_ensure_clean_worktree"
  eval "$_original_prepare_paths"
  eval "$_original_prepare_worktree"
  eval "$_original_ensure_release_tag_available"
  eval "$_original_resolve_package_sync_owner"
  eval "$_original_install_python_release_tools"
  eval "$_original_build_python_distribution"
  eval "$_original_prepare_cli_publish_assets"
  eval "$_original_ensure_versions_prepared"
  eval "$_original_ensure_publish_worktree_clean"
  eval "$_original_commit_release_prepare"
  eval "$_original_append_skipped_components"
  eval "$_original_append_state"
  eval "$_original_set_release_sha"
  eval "$_original_publish_rust_package"
  eval "$_original_publish_python_package"
  eval "$_original_python_version_visible"
  eval "$_original_wait_for_visibility"
  eval "$_original_skip_python_package"
  eval "$_original_create_final_tag"
  eval "$_original_publish_package_sync_repo"
  eval "$_original_publish_web_packages"
  eval "$_original_publish_typescript_workload_package"
  eval "$_original_publish_typescript_package"
  eval "$_original_stage_create_tag"
  eval "$_original_stage_publish_rust"
  eval "$_original_write_context"
  eval "$_original_read_context"
  eval "$_original_stage_build_cli_binaries"
  eval "$_original_stage_publish_cli_binaries"
}

reset_release_train_state() {
  VERSION=""
  DRY_RUN=false
  PREPARE_ONLY=false
  SKIP_PYTHON=false
  PRE_RELEASE=false
  SKIP_WEB=false
  RUN_MODE="publish"
  RELEASE_SHA=""
  RELEASE_BRANCH="main"
  STAGE="all"
  REPORT_DIR=""
  STATE_FILE=""
  REPORT_MARKDOWN=""
  REPORT_JSON=""
  OVERALL_STATUS="success"
  FAILURE_REASON=""
  FINAL_TAG=""
  TAG_ALREADY_EXISTS=false
  ORIGINAL_REPO_ROOT="$repo_root"
  restore_all_functions
}

assert_eq() {
  local expected=$1
  local actual=$2
  local label=$3
  if [[ "$expected" != "$actual" ]]; then
    printf '%s: expected %s, got %s\n' "$label" "$expected" "$actual" >&2
    exit 1
  fi
}

test_detect_conventional_bump_reads_commit_without_trailing_newline() {
  reset_release_train_state

  local temp_repo previous_root
  temp_repo=$(mktemp -d)
  previous_root=$ORIGINAL_REPO_ROOT

  git -C "$temp_repo" init -q
  printf 'tracked
' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "init"
  git -C "$temp_repo" tag v0.1.0
  printf 'fix
' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "fix(runtime): repair mailbox state"

  ORIGINAL_REPO_ROOT=$temp_repo

  local bump
  bump=$(detect_conventional_bump)

  ORIGINAL_REPO_ROOT=$previous_root
  rm -rf "$temp_repo"

  assert_eq "patch" "$bump" "fix commit after tag must return patch"
}

test_parse_prepare_only_mode() {
  reset_release_train_state

  parse_args --version 1.2.3 --prepare-only

  assert_eq "1.2.3" "$VERSION" "VERSION"
  assert_eq "true" "$PREPARE_ONLY" "PREPARE_ONLY"
  assert_eq "prepare" "$RUN_MODE" "RUN_MODE"
}

test_parse_stage_argument() {
  reset_release_train_state

  parse_args --version 1.2.3 --stage create-tag

  assert_eq "create-tag" "$STAGE" "STAGE"
  assert_eq "1.2.3" "$VERSION" "VERSION"
}

test_parse_stage_publish_rust() {
  reset_release_train_state

  parse_args --version 1.2.3 --stage publish-rust

  assert_eq "publish-rust" "$STAGE" "STAGE"
}

test_parse_stage_all_is_default() {
  reset_release_train_state

  parse_args --version 1.2.3

  assert_eq "all" "$STAGE" "STAGE default"
}

test_parse_stage_rejects_unknown() {
  reset_release_train_state

  if ! parse_args --version 1.2.3 --stage nonexistent 2>/dev/null; then
    : # expected: parse_args returns non-zero
  else
    printf 'parse_args must reject unknown stage\n' >&2
    exit 1
  fi
}

test_validate_version_requires_strict_semver() {
  reset_release_train_state
  VERSION="01.2.3-rc.1"
  PRE_RELEASE=true
  SKIP_PYTHON=true
  if validate_version 2>/dev/null; then
    printf 'pre-release core identifiers must not contain leading zeroes\n' >&2
    exit 1
  fi

  VERSION="1.2.3-rc.01"
  if validate_version 2>/dev/null; then
    printf 'numeric pre-release identifiers must not contain leading zeroes\n' >&2
    exit 1
  fi
}

test_validate_version_requires_pep440_when_python_is_enabled() {
  reset_release_train_state
  VERSION="1.2.3-rc.1"
  PRE_RELEASE=true
  validate_version

  VERSION="1.2.3-pre.1"
  validate_version

  VERSION="1.2.3-feature-x"
  if validate_version 2>/dev/null; then
    printf 'Python-enabled pre-releases must reject non-PEP-440 identifiers\n' >&2
    exit 1
  fi

  SKIP_PYTHON=true
  VERSION="1.2.3-feature-x.7+build.01"
  validate_version
}

test_append_skipped_components_allows_empty_list() {
  reset_release_train_state

  local calls=()
  append_state() { calls+=("$1"); }

  append_skipped_components

  assert_eq "0" "${#calls[@]}" "empty skipped components"
}

test_publish_clean_check_rejects_untracked_files() {
  reset_release_train_state

  local temp_repo
  temp_repo=$(mktemp -d)
  git -C "$temp_repo" init -q
  printf 'tracked\n' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "init"
  printf 'generated\n' >"$temp_repo/generated.txt"

  if (cd "$temp_repo" && ensure_publish_worktree_clean >/dev/null 2>&1); then
    printf 'publish clean check must reject untracked generated files\n' >&2
    rm -rf "$temp_repo"
    exit 1
  fi

  rm -rf "$temp_repo"
}

test_publish_clean_check_allows_current_report_artifacts() {
  reset_release_train_state

  local temp_repo
  temp_repo=$(mktemp -d)
  git -C "$temp_repo" init -q
  printf 'tracked\n' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "init"
  mkdir -p "$temp_repo/release/reports"
  VERSION="1.2.3"
  printf 'state\n' >"$temp_repo/release/reports/release-train-v1.2.3.state.tsv"
  printf 'markdown\n' >"$temp_repo/release/reports/release-train-v1.2.3.md"
  printf '{}\n' >"$temp_repo/release/reports/release-train-v1.2.3.json"
  printf 'stage-state\n' >"$temp_repo/release/reports/release-train-v1.2.3.publish-rust.state.tsv"
  printf '{}\n' >"$temp_repo/release/reports/release-train-v1.2.3.context.json"

  if ! (cd "$temp_repo" && ensure_publish_worktree_clean >/dev/null 2>&1); then
    printf 'publish clean check must allow current release report and stage artifacts\n' >&2
    rm -rf "$temp_repo"
    exit 1
  fi

  rm -rf "$temp_repo"
}

test_final_tag_uses_conventional_v_prefix() {
  reset_release_train_state

  local temp_repo previous_pwd
  temp_repo=$(mktemp -d)
  previous_pwd=$PWD
  git -C "$temp_repo" init -q

  cd "$temp_repo"
  VERSION="1.2.3"
  DRY_RUN=false

  ensure_release_tag_available

  cd "$previous_pwd"
  rm -rf "$temp_repo"

  assert_eq "v1.2.3" "$FINAL_TAG" "FINAL_TAG"
}

test_latest_release_tag_accepts_legacy_release_train_prefix() {
  reset_release_train_state

  local temp_repo previous_root
  temp_repo=$(mktemp -d)
  previous_root=$ORIGINAL_REPO_ROOT

  git -C "$temp_repo" init -q
  printf 'tracked\n' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "init"
  git -C "$temp_repo" tag release-train-v0.3.1

  ORIGINAL_REPO_ROOT=$temp_repo

  assert_eq "release-train-v0.3.1" "$(latest_release_tag)" "latest legacy release tag"

  ORIGINAL_REPO_ROOT=$previous_root
  rm -rf "$temp_repo"
}

test_release_prepare_skips_release_commit_head() {
  reset_release_train_state

  local temp_repo previous_root
  temp_repo=$(mktemp -d)
  previous_root=$ORIGINAL_REPO_ROOT

  git -C "$temp_repo" init -q
  printf 'tracked\n' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "init"
  printf 'release\n' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "chore(release): basic train v1.2.3"

  ORIGINAL_REPO_ROOT=$temp_repo

  if ! release_prepare_should_skip_current_head; then
    printf 'auto release prepare must skip release commits\n' >&2
    ORIGINAL_REPO_ROOT=$previous_root
    rm -rf "$temp_repo"
    exit 1
  fi

  printf 'fix\n' >"$temp_repo/tracked.txt"
  git -C "$temp_repo" add tracked.txt
  git -C "$temp_repo" -c user.name="Release Test" -c user.email="release-test@example.com" commit -q -m "fix(runtime): repair mailbox state"

  if release_prepare_should_skip_current_head; then
    printf 'auto release prepare must not skip normal conventional commits\n' >&2
    ORIGINAL_REPO_ROOT=$previous_root
    rm -rf "$temp_repo"
    exit 1
  fi

  ORIGINAL_REPO_ROOT=$previous_root
  rm -rf "$temp_repo"
}

test_publish_mode_uses_prepared_versions_without_mutating() {
  reset_release_train_state

  local calls=()
  update_versions() { calls+=("update_versions"); }
  run_validation_suite() { calls+=("run_validation_suite"); }
  ensure_versions_prepared() { calls+=("ensure_versions_prepared"); }
  ensure_publish_worktree_clean() { calls+=("ensure_publish_worktree_clean"); }
  commit_release_prepare() { calls+=("commit_release_prepare"); }
  append_skipped_components() { calls+=("append_skipped_components"); }
  set_release_sha() { calls+=("set_release_sha"); RELEASE_SHA="test-sha"; }
  write_context() { calls+=("write_context"); }
  publish_rust_package() { calls+=("publish_rust_package:$1:$2"); }
  publish_python_package() { calls+=("publish_python_package"); }
  skip_python_package() { calls+=("skip_python_package"); }
  create_final_tag() { calls+=("create_final_tag"); }
  publish_package_sync_repo() { calls+=("publish_package_sync_repo:$2"); }
  publish_web_packages() { calls+=("publish_web_packages"); }
  publish_typescript_workload_package() { calls+=("publish_typescript_workload_package"); }
  publish_typescript_package() { calls+=("publish_typescript_package"); }
  stage_build_cli_binaries() { calls+=("stage_build_cli_binaries"); }
  stage_publish_cli_binaries() { calls+=("stage_publish_cli_binaries"); }

  VERSION="1.2.3"
  DRY_RUN=false
  PREPARE_ONLY=false
  SKIP_PYTHON=true
  SKIP_WEB=false
  STAGE="all"

  run_release_train

  local joined
  joined=$(printf '%s\n' "${calls[@]}")

  if grep -qx "update_versions" <<<"$joined"; then
    printf 'publish mode must not mutate version files with update_versions\n' >&2
    exit 1
  fi

  if grep -qx "commit_release_prepare" <<<"$joined"; then
    printf 'publish mode must not create release prepare commits\n' >&2
    exit 1
  fi

  assert_eq "ensure_versions_prepared" "${calls[0]}" "first publish step"
  # run_validation_suite removed from publish path (now covered by CI gate test job)
  assert_eq "ensure_publish_worktree_clean" "${calls[1]}" "second publish step"
  assert_eq "set_release_sha" "${calls[2]}" "third publish step"
}

test_prepare_only_updates_validates_and_commits_without_publishing() {
  reset_release_train_state

  local calls=()
  update_versions() { calls+=("update_versions"); }
  run_validation_suite() { calls+=("run_validation_suite"); }
  commit_release_prepare() { calls+=("commit_release_prepare"); }
  ensure_versions_prepared() { calls+=("ensure_versions_prepared"); }
  publish_rust_package() { calls+=("publish_rust_package"); }
  create_final_tag() { calls+=("create_final_tag"); }

  VERSION="1.2.3"
  DRY_RUN=false
  PREPARE_ONLY=true

  run_release_train

  assert_eq "update_versions" "${calls[0]}" "first prepare step"
  assert_eq "run_validation_suite" "${calls[1]}" "second prepare step"
  assert_eq "commit_release_prepare" "${calls[2]}" "third prepare step"

  local joined
  joined=$(printf '%s\n' "${calls[@]}")
  if grep -Eq "publish_rust_package|create_final_tag|ensure_versions_prepared" <<<"$joined"; then
    printf 'prepare-only mode must stop before publish-only steps\n' >&2
    exit 1
  fi
}

test_create_tag_dry_run_does_not_push() {
  reset_release_train_state

  local calls=()
  read_context() {
    VERSION="1.2.3"
    RELEASE_SHA="abc123"
    DRY_RUN=true
    FINAL_TAG="v1.2.3"
  }
  create_final_tag() { calls+=("create_final_tag"); }

  VERSION="1.2.3"
  DRY_RUN=true
  STAGE="create-tag"

  REPORT_DIR="/tmp/test-release-reports"
  mkdir -p "$REPORT_DIR"
  run_release_train

  # In dry-run mode, create_final_tag is called but returns early.
  # Verify it was called (the function checks DRY_RUN internally).
  if [[ "${#calls[@]}" -ne 1 ]]; then
    printf 'create-tag stage in dry-run must still call create_final_tag\n' >&2
    exit 1
  fi
}

test_main_publish_stage_skips_tag_availability_check() {
  reset_release_train_state

  local calls=()
  ensure_clean_worktree() { calls+=("ensure_clean_worktree"); }
  prepare_paths() { calls+=("prepare_paths"); }
  prepare_worktree() { calls+=("prepare_worktree"); }
  ensure_release_tag_available() { calls+=("ensure_release_tag_available"); }
  resolve_package_sync_owner() { calls+=("resolve_package_sync_owner"); }
  install_python_release_tools() { calls+=("install_python_release_tools"); }
  stage_publish_rust() { calls+=("stage_publish_rust"); }

  main --version 1.2.3 --stage publish-rust --dry-run --skip-python

  local joined
  joined=$(printf '%s\n' "${calls[@]}")

  if grep -qx "ensure_release_tag_available" <<<"$joined"; then
    printf 'publish-rust stage must not check that the final tag is absent\n' >&2
    exit 1
  fi

  if ! grep -qx "stage_publish_rust" <<<"$joined"; then
    printf 'publish-rust stage must still run through main\n' >&2
    exit 1
  fi

  assert_eq "v1.2.3" "$FINAL_TAG" "publish stage FINAL_TAG"
}

test_main_validate_and_create_tag_check_absent_tag() {
  local stage expected_stage_call
  for stage in create-tag; do
    reset_release_train_state
    REPORT_DIR="/tmp/test-release-reports"
    mkdir -p "$REPORT_DIR"

    local calls=()
    ensure_clean_worktree() { calls+=("ensure_clean_worktree"); }
    prepare_paths() { calls+=("prepare_paths"); }
    prepare_worktree() { calls+=("prepare_worktree"); }
    ensure_release_tag_available() { calls+=("ensure_release_tag_available"); FINAL_TAG="${FINAL_TAG_PREFIX}${VERSION}"; }
    resolve_package_sync_owner() { calls+=("resolve_package_sync_owner"); }
    install_python_release_tools() { calls+=("install_python_release_tools"); }
    stage_create_tag() { calls+=("stage_create_tag"); }

    main --version 1.2.3 --stage "$stage" --dry-run --skip-python

    local joined
    joined=$(printf '%s\n' "${calls[@]}")
    if ! grep -qx "ensure_release_tag_available" <<<"$joined"; then
      printf '%s stage must check that the final tag is absent\n' "$stage" >&2
      exit 1
    fi

    expected_stage_call="stage_${stage//-/_}"
    if ! grep -qx "$expected_stage_call" <<<"$joined"; then
      printf '%s must still run through main\n' "$stage" >&2
      exit 1
    fi
  done
}

test_publish_python_package_builds_distribution_before_upload() {
  reset_release_train_state

  local calls=()
  python_version_visible() { calls+=("python_version_visible"); return 1; }
  build_python_distribution() { calls+=("build_python_distribution"); }
  wait_for_visibility() { calls+=("wait_for_visibility:$1:$2"); return 0; }
  append_state() { calls+=("append_state:$1:$5"); }

  VERSION="1.2.3"
  DRY_RUN=false
  PYPI_API_TOKEN="test-token"
  RELEASE_PYTHON_BIN=$(command -v true)
  RELEASE_SHA="abc123"

  publish_python_package

  local joined
  joined=$(printf '%s\n' "${calls[@]}")
  if ! grep -qx "build_python_distribution" <<<"$joined"; then
    printf 'publish_python_package must rebuild dist before upload\n' >&2
    exit 1
  fi
}

test_publish_python_package_uses_testpypi_for_pre_release() {
  reset_release_train_state

  local temp_dir call_log dist_file
  temp_dir=$(mktemp -d)
  call_log="$temp_dir/python.log"
  dist_file="tools/protoc-gen/python/dist/framework_codegen_python-1.2.3-pre.1.tar.gz"

  cat >"$temp_dir/python" <<'EOF'
#!/usr/bin/env bash
{
  printf 'args:%s\n' "$*"
  printf 'password:%s\n' "${TWINE_PASSWORD:-}"
} >>"$RELEASE_TEST_CALL_LOG"
exit 0
EOF
  chmod +x "$temp_dir/python"

  python_version_visible() { return 1; }
  build_python_distribution() {
    mkdir -p "$(dirname "$dist_file")"
    : >"$dist_file"
  }
  wait_for_visibility() { return 0; }
  append_state() { :; }

  VERSION="1.2.3-pre.1"
  PRE_RELEASE=true
  DRY_RUN=false
  PYPI_API_TOKEN="pypi-token"
  TEST_PYPI_API_TOKEN="test-pypi-token"
  RELEASE_PYTHON_BIN="$temp_dir/python"
  export RELEASE_TEST_CALL_LOG="$call_log"
  RELEASE_SHA="abc123"

  publish_python_package
  rm -f "$dist_file"

  if ! grep -q -- '--repository-url https://test.pypi.org/legacy/' "$call_log"; then
    printf 'pre-release Python publish must target TestPyPI\n' >&2
    exit 1
  fi

  if grep -q -- 'dist/\*' "$call_log"; then
    printf 'pre-release Python publish must expand dist artifacts before calling twine\n' >&2
    exit 1
  fi

  if ! grep -qx 'password:test-pypi-token' "$call_log"; then
    printf 'pre-release Python publish must use TEST_PYPI_API_TOKEN\n' >&2
    exit 1
  fi
}

test_publish_rust_package_prepares_cli_web_assets_before_publish() {
  reset_release_train_state

  local temp_dir original_path call_log
  temp_dir=$(mktemp -d)
  original_path=$PATH
  call_log="$temp_dir/calls.log"
  mkdir -p "$temp_dir/bin"

  cat >"$temp_dir/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf 'cargo:%s\n' "$*" >>"$RELEASE_TEST_CALL_LOG"
exit 0
EOF
  chmod +x "$temp_dir/bin/cargo"

  prepare_cli_publish_assets() {
    printf 'prepare_cli_publish_assets\n' >>"$RELEASE_TEST_CALL_LOG"
  }
  crate_version_visible() { return 1; }
  wait_for_visibility() { return 0; }
  append_state() { :; }

  export RELEASE_TEST_CALL_LOG="$call_log"
  PATH="$temp_dir/bin:$PATH"
  WORK_REPO_ROOT="$temp_dir"
  VERSION="1.2.3"
  DRY_RUN=false
  RELEASE_SHA="abc123"

  publish_rust_package actr-cli cli

  PATH=$original_path

  local expected
  expected=$'prepare_cli_publish_assets\ncargo:publish -p actr-cli --locked --allow-dirty'
  if [[ "$(cat "$call_log")" != "$expected" ]]; then
    printf 'actr-cli publish must prepare web runtime assets before cargo publish\n' >&2
    cat "$call_log" >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  : >"$call_log"
  PATH="$temp_dir/bin:$PATH"

  publish_rust_package actr-protocol foundation

  PATH=$original_path

  if grep -q "prepare_cli_publish_assets" "$call_log"; then
    printf 'non-cli rust package publish must not prepare CLI web runtime assets\n' >&2
    cat "$call_log" >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  rm -rf "$temp_dir"
}

test_publish_typescript_workload_builds_before_publish() {
  reset_release_train_state

  local temp_dir original_path
  temp_dir=$(mktemp -d)
  original_path=$PATH
  mkdir -p "$temp_dir/bin" "$temp_dir/bindings/typescript/actr-workload"

  cat >"$temp_dir/bin/npm" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$NPM_CALL_LOG"
if [[ "$1" == "run" && "$2" == "build" ]]; then
  mkdir -p dist
  printf 'console.log("ok");\n' >dist/index.js
  printf 'export {};\n' >dist/index.d.ts
  printf '#!/usr/bin/env node\n' >dist/cli.js
fi
exit 0
EOF
  chmod +x "$temp_dir/bin/npm"

  cat >"$temp_dir/bindings/typescript/actr-workload/package.json" <<'EOF'
{
  "name": "@actrium/actr-workload",
  "version": "1.2.3",
  "scripts": {
    "build": "echo build"
  }
}
EOF

  export NPM_CALL_LOG="$temp_dir/npm-calls.log"
  PATH="$temp_dir/bin:$PATH"
  WORK_REPO_ROOT="$temp_dir"
  DRY_RUN=true
  VERSION="1.2.3"
  RELEASE_SHA="abc123"
  append_state() { :; }

  publish_typescript_workload_package

  PATH=$original_path

  if ! grep -qx "run build" "$NPM_CALL_LOG"; then
    printf 'publish_typescript_workload_package must run npm run build\n' >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  rm -rf "$temp_dir"
}

test_publish_typescript_package_writes_native_and_main_state() {
  reset_release_train_state

  local temp_dir original_path
  temp_dir=$(mktemp -d)
  original_path=$PATH
  mkdir -p "$temp_dir/bin" "$temp_dir/bindings/typescript"

  cat >"$temp_dir/bin/npm" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$NPM_CALL_LOG"
exit 0
EOF
  chmod +x "$temp_dir/bin/npm"

  cat >"$temp_dir/bin/npx" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$NPX_CALL_LOG"
if [[ "$1" == "napi" && "$2" == "create-npm-dirs" ]]; then
  while IFS='|' read -r package dir artifact; do
    mkdir -p "npm/$dir"
    printf '{"name":"%s","version":"1.2.3"}\n' "$package" >"npm/$dir/package.json"
  done <<'PACKAGES'
@actrium/actr-darwin-x64|darwin-x64|actr.darwin-x64.node
@actrium/actr-darwin-arm64|darwin-arm64|actr.darwin-arm64.node
@actrium/actr-linux-x64-gnu|linux-x64-gnu|actr.linux-x64-gnu.node
@actrium/actr-linux-x64-musl|linux-x64-musl|actr.linux-x64-musl.node
@actrium/actr-linux-arm64-gnu|linux-arm64-gnu|actr.linux-arm64-gnu.node
@actrium/actr-linux-arm64-musl|linux-arm64-musl|actr.linux-arm64-musl.node
@actrium/actr-win32-x64-msvc|win32-x64-msvc|actr.win32-x64-msvc.node
PACKAGES
fi
exit 0
EOF
  chmod +x "$temp_dir/bin/npx"

  cat >"$temp_dir/bindings/typescript/package.json" <<'EOF'
{
  "name": "@actrium/actr",
  "version": "1.2.3",
  "scripts": {
    "compile:ts": "echo compile",
    "artifacts": "echo artifacts"
  }
}
EOF
  printf '[package]\nversion = "1.2.3"\n' >"$temp_dir/bindings/typescript/Cargo.toml"

  export NPM_CALL_LOG="$temp_dir/npm-calls.log"
  export NPX_CALL_LOG="$temp_dir/npx-calls.log"
  PATH="$temp_dir/bin:$PATH"
  WORK_REPO_ROOT="$temp_dir"
  DRY_RUN=true
  VERSION="1.2.3"
  RELEASE_SHA="abc123"
  STATE_FILE="$temp_dir/state.tsv"
  : >"$STATE_FILE"

  publish_typescript_package

  PATH=$original_path

  if grep -qx "run artifacts -- --output-dir artifacts" "$NPM_CALL_LOG"; then
    printf 'TypeScript dry-run without native artifacts must not run npm run artifacts\n' >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  local line_count
  line_count=$(wc -l <"$STATE_FILE" | tr -d ' ')
  assert_eq "8" "$line_count" "TypeScript package state rows"

  for package in \
    @actrium/actr \
    @actrium/actr-darwin-x64 \
    @actrium/actr-darwin-arm64 \
    @actrium/actr-linux-x64-gnu \
    @actrium/actr-linux-x64-musl \
    @actrium/actr-linux-arm64-gnu \
    @actrium/actr-linux-arm64-musl \
    @actrium/actr-win32-x64-msvc; do
    if ! grep -Fq "${package}"$'\t'"sdk"$'\t'"npm"$'\t'"1.2.3"$'\t'"success"$'\t'"dry_run_validated" "$STATE_FILE"; then
      printf 'missing TypeScript package state row for %s\n' "$package" >&2
      rm -rf "$temp_dir"
      exit 1
    fi
  done

  rm -rf "$temp_dir"
}

test_release_train_workflow_publish_typescript_uses_script_stage() {
  reset_release_train_state

  if ! grep -q 'args=(--stage publish-typescript --branch main --version' .github/workflows/release-train.yml; then
    printf 'publish-typescript workflow job must call scripts/release-train.sh --stage publish-typescript\n' >&2
    exit 1
  fi

  if grep -q 'Publish native + main packages\|Dry run native + main package publish' .github/workflows/release-train.yml; then
    printf 'publish-typescript workflow job must not inline npm publish logic\n' >&2
    exit 1
  fi
}

test_release_train_workflow_downloads_only_typescript_native_artifacts() {
  reset_release_train_state

  if ! grep -q 'pattern: actr.*.node' .github/workflows/release-train.yml; then
    printf 'publish-typescript workflow must download only native .node artifacts\n' >&2
    exit 1
  fi
}

test_release_prepare_workflow_skips_release_commits() {
  reset_release_train_state

  if ! grep -q "contains(github.event.head_commit.message, 'chore(release):')" .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must skip chore(release) commits\n' >&2
    exit 1
  fi
}

test_report_stage_merges_state_files() {
  reset_release_train_state

  local temp_dir
  temp_dir=$(mktemp -d)
  mkdir -p "$temp_dir/release/reports"

  VERSION="1.2.3"
  REPORT_DIR="$temp_dir/release/reports"
  STATE_FILE="$REPORT_DIR/release-train-v1.2.3.state.tsv"
  REPORT_MARKDOWN="$REPORT_DIR/release-train-v1.2.3.md"
  REPORT_JSON="$REPORT_DIR/release-train-v1.2.3.json"
  OVERALL_STATUS="success"
  FAILURE_REASON=""
  STAGE="report"
  RELEASE_SHA="abc123"
  DRY_RUN=false
  PRE_RELEASE=false
  SKIP_PYTHON=false

  # Create per-stage state files.
  printf 'actr-protocol\tfoundation\tcrate\t1.2.3\tpublished\tpublished\t-\t-\n' >"$REPORT_DIR/release-train-v1.2.3.publish-rust.state.tsv"
  printf 'framework_codegen_python\tprotoc-gen\tpython\t1.2.3\tpublished\tpublished\t-\t-\n' >"$REPORT_DIR/release-train-v1.2.3.publish-python.state.tsv"

  # Create a context file.
  cat >"$REPORT_DIR/release-train-v1.2.3.context.json" <<EOF
{"version": "1.2.3", "release_sha": "abc123", "dry_run": false, "pre_release": false, "skip_python": false, "final_tag": "v1.2.3"}
EOF

  # Run report stage.
  stage_report

  # generate_report is normally called via on_exit trap.
  # In test context we call it explicitly.
  generate_report

  # Verify merged state file.
  if [[ ! -f "$STATE_FILE" ]]; then
    printf 'report stage must create merged state file\n' >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  local line_count
  line_count=$(wc -l < "$STATE_FILE" | tr -d ' ')
  if [[ "$line_count" -ne 2 ]]; then
    printf 'merged state file must contain 2 rows, got %s\n' "$line_count" >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  rm -rf "$temp_dir"
}

test_update_versions_syncs_optional_dependencies() {
  reset_release_train_state

  local temp_dir
  temp_dir=$(mktemp -d)
  mkdir -p "$temp_dir/bindings/typescript"

  # Create a minimal package.json with optionalDependencies.
  cat >"$temp_dir/bindings/typescript/package.json" <<'EOF'
{
  "name": "@actrium/actr",
  "version": "0.2.0",
  "optionalDependencies": {
    "@actrium/actr-darwin-x64": "0.2.0",
    "@actrium/actr-linux-x64-gnu": "0.2.0",
    "@other/package": "1.0.0"
  }
}
EOF

  WORK_REPO_ROOT="$temp_dir"
  VERSION="0.3.0"
  SKIP_PYTHON=true

  # Stub out all Cargo.toml paths.
  for f in \
    Cargo.toml \
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
    cli/Cargo.toml \
    bindings/typescript/Cargo.toml \
    bindings/web/crates/actr-web-abi/Cargo.toml \
    bindings/web/crates/common/Cargo.toml \
    bindings/web/crates/sw-host/Cargo.toml \
    bindings/web/crates/dom-bridge/Cargo.toml \
    bindings/web/crates/mailbox-web/Cargo.toml \
    bindings/web/crates/platform-web/Cargo.toml \
    bindings/web/crates/framework-web-entry-smoke/Cargo.toml; do
    mkdir -p "$(dirname "$temp_dir/$f")"
    printf '[package]\nversion = "0.2.0"\n' >"$temp_dir/$f"
  done

  # Create web packages.
  for wp in actr-dom web-sdk web-react; do
    mkdir -p "$temp_dir/bindings/web/packages/$wp"
    printf '{"name":"@actrium/actr-dom","version":"0.2.0"}' >"$temp_dir/bindings/web/packages/$wp/package.json"
  done

  # Create workload package.
  mkdir -p "$temp_dir/bindings/typescript/actr-workload"
  printf '{"name":"@actrium/actr-workload","version":"0.2.0"}' >"$temp_dir/bindings/typescript/actr-workload/package.json"

  # Create protoc plugin version sources.
  mkdir -p \
    "$temp_dir/tools/protoc-gen/swift/Sources/framework-codegen-swift" \
    "$temp_dir/tools/protoc-gen/typescript/src" \
    "$temp_dir/tools/protoc-gen/kotlin/src/main/kotlin/io/actrium/codegen"
  printf 'struct Generator { static let version = "0.2.0" }\n' \
    >"$temp_dir/tools/protoc-gen/swift/Sources/framework-codegen-swift/main.swift"
  printf 'const VERSION = "0.2.0";\n' \
    >"$temp_dir/tools/protoc-gen/typescript/src/main.ts"
  printf '{"name":"framework-codegen-typescript","version":"0.2.0"}\n' \
    >"$temp_dir/tools/protoc-gen/typescript/package.json"
  printf '{"name":"framework-codegen-typescript","version":"0.2.0","lockfileVersion":3,"packages":{"":{"version":"0.2.0"}}}\n' \
    >"$temp_dir/tools/protoc-gen/typescript/package-lock.json"
  printf 'version = "0.2.0"\n' \
    >"$temp_dir/tools/protoc-gen/kotlin/build.gradle.kts"
  cat >"$temp_dir/tools/protoc-gen/kotlin/src/main/kotlin/io/actrium/codegen/Main.kt" <<'EOF'
println("protoc-gen-actrframework-kotlin 0.2.0")
println("    0.2.0")
EOF

  update_versions

  # Verify optionalDependencies were synced.
  local darwin_ver
  darwin_ver=$(python3 -c "import json; d=json.load(open('$temp_dir/bindings/typescript/package.json')); print(d['optionalDependencies']['@actrium/actr-darwin-x64'])")
  assert_eq "0.3.0" "$darwin_ver" "optionalDependencies sync"

  # Verify non-actrium deps are untouched.
  local other_ver
  other_ver=$(python3 -c "import json; d=json.load(open('$temp_dir/bindings/typescript/package.json')); print(d['optionalDependencies']['@other/package'])")
  assert_eq "1.0.0" "$other_ver" "non-actrium deps untouched"

  # Verify actr-workload version was updated.
  local workload_ver
  workload_ver=$(python3 -c "import json; d=json.load(open('$temp_dir/bindings/typescript/actr-workload/package.json')); print(d['version'])")
  assert_eq "0.3.0" "$workload_ver" "actr-workload version"

  assert_eq \
    'struct Generator { static let version = "0.3.0" }' \
    "$(cat "$temp_dir/tools/protoc-gen/swift/Sources/framework-codegen-swift/main.swift")" \
    "Swift protoc plugin version"
  assert_eq \
    'const VERSION = "0.3.0";' \
    "$(cat "$temp_dir/tools/protoc-gen/typescript/src/main.ts")" \
    "TypeScript protoc plugin source version"
  assert_eq \
    "0.3.0" \
    "$(python3 -c "import json; print(json.load(open('$temp_dir/tools/protoc-gen/typescript/package-lock.json'))['packages']['']['version'])")" \
    "TypeScript protoc plugin lock version"
  assert_eq \
    'version = "0.3.0"' \
    "$(cat "$temp_dir/tools/protoc-gen/kotlin/build.gradle.kts")" \
    "Kotlin protoc plugin Gradle version"
  if grep -q '0\.2\.0' "$temp_dir/tools/protoc-gen/kotlin/src/main/kotlin/io/actrium/codegen/Main.kt"; then
    printf 'Kotlin protoc plugin output versions were not synchronized\n' >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  rm -rf "$temp_dir"
}

test_publish_web_packages_skips_puppeteer_download() {
  reset_release_train_state

  local temp_dir pnpm_env_log
  temp_dir=$(mktemp -d)
  pnpm_env_log="$temp_dir/pnpm-env.log"
  mkdir -p "$temp_dir/bin" "$temp_dir/bindings/web/scripts"

  # Fake pnpm that records its environment.
  cat >"$temp_dir/bin/pnpm" <<'EOF'
#!/usr/bin/env bash
printf 'PUPPETEER_SKIP_DOWNLOAD=%s\n' "${PUPPETEER_SKIP_DOWNLOAD:-unset}" >>"${PNPM_ENV_LOG}"
printf 'PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=%s\n' "${PUPPETEER_SKIP_CHROMIUM_DOWNLOAD:-unset}" >>"${PNPM_ENV_LOG}"
printf 'ARGS=%s\n' "$*" >>"${PNPM_ENV_LOG}"
exit 0
EOF
  chmod +x "$temp_dir/bin/pnpm"

  # Fake publish.sh so it succeeds.
  cat >"$temp_dir/bindings/web/scripts/publish.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$temp_dir/bindings/web/scripts/publish.sh"

  # Fake node for version read.
  mkdir -p "$temp_dir/bindings/web/packages/actr-dom" \
    "$temp_dir/bindings/web/packages/web-sdk" \
    "$temp_dir/bindings/web/packages/web-react"
  printf '{"name":"@actrium/actr-dom","version":"1.2.3","publishConfig":{"access":"public"}}\n' >"$temp_dir/bindings/web/packages/actr-dom/package.json"
  printf '{"name":"@actrium/actr-web","version":"1.2.3","publishConfig":{"access":"public"}}\n' >"$temp_dir/bindings/web/packages/web-sdk/package.json"
  printf '{"name":"@actrium/actr-web-react","version":"1.2.3","publishConfig":{"access":"public"}}\n' >"$temp_dir/bindings/web/packages/web-react/package.json"

  local original_path=$PATH
  PATH="$temp_dir/bin:$PATH"
  export PNPM_ENV_LOG="$pnpm_env_log"
  WORK_REPO_ROOT="$temp_dir"
  VERSION="1.2.3"
  DRY_RUN=true
  RELEASE_SHA="abc123"
  append_state() { :; }
  log_info() { :; }
  log_warn() { :; }

  publish_web_packages

  PATH=$original_path

  # Assert both Puppeteer skip env vars are set to "true".
  local skip_download skip_chromium
  skip_download=$(grep '^PUPPETEER_SKIP_DOWNLOAD=' "$pnpm_env_log" | head -1 | cut -d= -f2)
  skip_chromium=$(grep '^PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=' "$pnpm_env_log" | head -1 | cut -d= -f2)

  assert_eq "true" "$skip_download" "PUPPETEER_SKIP_DOWNLOAD"
  assert_eq "true" "$skip_chromium" "PUPPETEER_SKIP_CHROMIUM_DOWNLOAD"

  # Assert --frozen-lockfile is used.
  if ! grep -q 'install --frozen-lockfile' "$pnpm_env_log"; then
    printf 'publish_web_packages must use pnpm install --frozen-lockfile\n' >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  rm -rf "$temp_dir"
}

test_cli_zigbuild_uses_zigbuild_arguments_without_build_subcommand() {
  reset_release_train_state
  # shellcheck source=/dev/null
  source scripts/release-actr-cli.sh

  local temp_dir original_path cargo_args_log
  temp_dir=$(mktemp -d)
  cargo_args_log="$temp_dir/cargo-args.log"
  mkdir -p "$temp_dir/bin" "$temp_dir/bindings/web/scripts"

  cat >"$temp_dir/bindings/web/scripts/sync-cli-assets.sh" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
  chmod +x "$temp_dir/bindings/web/scripts/sync-cli-assets.sh"

  cat >"$temp_dir/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >"${CARGO_ARGS_LOG}"
target=""
previous=""
for arg in "$@"; do
  if [[ "$previous" == "--target" ]]; then
    target="$arg"
    break
  fi
  previous="$arg"
done
mkdir -p "${ACTR_REPO_ROOT}/target/${target}/release"
printf 'actr test binary\n' >"${ACTR_REPO_ROOT}/target/${target}/release/actr"
exit 0
EOF
  chmod +x "$temp_dir/bin/cargo"

  original_path=$PATH
  PATH="$temp_dir/bin:$PATH"
  export CARGO_ARGS_LOG="$cargo_args_log"
  export ACTR_REPO_ROOT="$temp_dir"
  export GITHUB_REPOSITORY="actrium/actr"
  export RELEASE_SHA="abc123"

  command_build \
    --target aarch64-unknown-linux-musl \
    --version 1.2.3 \
    --output "$temp_dir/staging" \
    --state-file "$temp_dir/state.tsv"

  PATH=$original_path
  unset CARGO_ARGS_LOG ACTR_REPO_ROOT GITHUB_REPOSITORY RELEASE_SHA

  if grep -q '^zigbuild build ' "$cargo_args_log"; then
    printf 'cargo zigbuild must not receive cargo build subcommand arguments\n' >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  if ! grep -q '^zigbuild --release -p actr-cli --locked --features wasm-engine --target aarch64-unknown-linux-musl$' "$cargo_args_log"; then
    printf 'cargo zigbuild arguments did not match expected release CLI build\n' >&2
    cat "$cargo_args_log" >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  rm -rf "$temp_dir"
}

test_cli_release_state_rows_match_report_schema() {
  reset_release_train_state
  # shellcheck source=/dev/null
  source scripts/release-actr-cli.sh

  local temp_dir field_count version status mode
  temp_dir=$(mktemp -d)

  export GITHUB_REPOSITORY="actrium/actr"
  export RELEASE_SHA="abc123"

  command_build \
    --target x86_64-unknown-linux-gnu \
    --version 1.2.3 \
    --output "$temp_dir/staging" \
    --state-file "$temp_dir/state.tsv" \
    --dry-run

  unset GITHUB_REPOSITORY RELEASE_SHA

  field_count=$(awk -F '\t' '{print NF}' "$temp_dir/state.tsv")
  version=$(cut -f4 "$temp_dir/state.tsv")
  status=$(cut -f5 "$temp_dir/state.tsv")
  mode=$(cut -f6 "$temp_dir/state.tsv")

  assert_eq "8" "$field_count" "CLI release state field count"
  assert_eq "1.2.3" "$version" "CLI release state version"
  assert_eq "success" "$status" "CLI release state status"
  assert_eq "dry_run_validated" "$mode" "CLI release state mode"

  rm -rf "$temp_dir"
}

test_cli_sha256_uses_sha256sum_without_shasum() {
  reset_release_train_state
  # shellcheck source=/dev/null
  source scripts/release-actr-cli.sh

  local temp_dir original_path expected_line actual_line
  temp_dir=$(mktemp -d)
  mkdir -p "$temp_dir/bin"
  printf 'archive\n' >"$temp_dir/actr-v1.2.3-x86_64-pc-windows-msvc.zip"

  ln -s "$(command -v awk)" "$temp_dir/bin/awk"
  ln -s "$(command -v basename)" "$temp_dir/bin/basename"
  ln -s "$(command -v dirname)" "$temp_dir/bin/dirname"
  cat >"$temp_dir/bin/sha256sum" <<'EOF'
#!/bin/sh
printf 'abc123  %s\n' "$1"
EOF
  chmod +x "$temp_dir/bin/sha256sum"

  original_path=$PATH
  PATH="$temp_dir/bin"

  write_sha256 \
    "$temp_dir/actr-v1.2.3-x86_64-pc-windows-msvc.zip" \
    "$temp_dir/actr-v1.2.3-x86_64-pc-windows-msvc.zip.sha256"

  PATH=$original_path

  expected_line="abc123  actr-v1.2.3-x86_64-pc-windows-msvc.zip"
  actual_line=$(cat "$temp_dir/actr-v1.2.3-x86_64-pc-windows-msvc.zip.sha256")
  assert_eq "$expected_line" "$actual_line" "CLI release sha256sum fallback"

  rm -rf "$temp_dir"
}

test_cli_release_upload_asset_deletes_existing_asset_before_retry() {
  reset_release_train_state
  # shellcheck source=/dev/null
  source scripts/release-actr-cli.sh

  local temp_dir asset_path curl_log upload_attempts
  temp_dir=$(mktemp -d)
  asset_path="$temp_dir/actr-v1.2.3-x86_64-unknown-linux-gnu.tar.gz"
  curl_log="$temp_dir/curl.log"
  upload_attempts="$temp_dir/upload-attempts"
  printf 'archive\n' >"$asset_path"
  printf 'stale archive\n' >"$temp_dir/actr-v9.9.9-x86_64-unknown-linux-gnu.tar.gz"
  printf '0\n' >"$upload_attempts"

  curl() {
    local method="GET"
    local url=""
    local previous=""
    local arg
    for arg in "$@"; do
      if [[ "$previous" == "-X" ]]; then
        method="$arg"
        previous=""
        continue
      fi
      if [[ "$arg" == "-X" ]]; then
        previous="-X"
        continue
      fi
      if [[ "$arg" == http://* || "$arg" == https://* ]]; then
        url="$arg"
      fi
    done
    printf '%s %s\n' "$method" "$url" >>"$curl_log"

    if [[ "$url" == */repos/actrium/actr/releases/tags/v1.2.3 ]]; then
      printf '{"id":123,"upload_url":"https://uploads.github.test/repos/actrium/actr/releases/123/assets{?name,label}"}'
      return 0
    fi

    if [[ "$url" == https://uploads.github.test/* ]]; then
      local count
      count=$(cat "$upload_attempts")
      count=$((count + 1))
      printf '%s\n' "$count" >"$upload_attempts"
      if [[ "$count" -eq 1 ]]; then
        printf '422'
      else
        printf '201'
      fi
      return 0
    fi

    if [[ "$url" == */repos/actrium/actr/releases/123/assets?per_page=100 ]]; then
      printf '[{"id":456,"name":"actr-v1.2.3-x86_64-unknown-linux-gnu.tar.gz"}]'
      return 0
    fi

    if [[ "$method" == "DELETE" && "$url" == */repos/actrium/actr/releases/assets/456 ]]; then
      return 0
    fi

    printf 'unexpected curl call: %s %s\n' "$method" "$url" >&2
    return 1
  }

  export GITHUB_TOKEN="test-token"
  export GITHUB_REPOSITORY="actrium/actr"
  export RELEASE_SHA="abc123"

  command_publish \
    --tag v1.2.3 \
    --assets "$temp_dir" \
    --state-file "$temp_dir/state.tsv" \
    --replace

  unset -f curl
  unset GITHUB_TOKEN GITHUB_REPOSITORY RELEASE_SHA

  if ! grep -q '^DELETE .*/repos/actrium/actr/releases/assets/456$' "$curl_log"; then
    printf 'existing GitHub Release asset must be deleted before retry\n' >&2
    cat "$curl_log" >&2
    rm -rf "$temp_dir"
    exit 1
  fi

  local upload_count
  upload_count=$(grep -c '^POST https://uploads.github.test/' "$curl_log")
  assert_eq "2" "$upload_count" "upload retry count"

  rm -rf "$temp_dir"
}

test_cli_validation_tag_requires_strict_semver() {
  reset_release_train_state
  # shellcheck source=/dev/null
  source scripts/release-actr-cli.sh

  local version
  version=$(version_from_release_tag validation-v1.2.3-rc.1)
  assert_eq "1.2.3-rc.1" "$version" "validation tag version"
  version=$(version_from_release_tag validation-v1.2.3-rc.1+build.x)
  assert_eq "1.2.3-rc.1+build.x" "$version" "validation tag version with build metadata"

  local invalid_tag
  for invalid_tag in \
    validation-v1.2.3 \
    validation-v1.2.3+build-x \
    validation-v01.2.3-rc.1 \
    validation-v1.2.3-rc.01 \
    validation-v1.2.3-feature..1; do
    if (version_from_release_tag "$invalid_tag" >/dev/null 2>&1); then
      printf 'validation tag must reject invalid SemVer: %s\n' "$invalid_tag" >&2
      exit 1
    fi
  done
}

test_publish_web_workflow_has_timeout() {
  reset_release_train_state

  if ! grep -q 'timeout-minutes: 20' .github/workflows/release-train.yml; then
    printf 'publish-web job must have timeout-minutes: 20\n' >&2
    exit 1
  fi
}

test_release_prepare_workflow_rejects_stale_runs() {
  reset_release_train_state

  if ! grep -q 'cancel-in-progress: true' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must cancel superseded runs\n' >&2
    exit 1
  fi

  if ! grep -q 'ref: \${{ github.sha }}' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must checkout the triggering commit\n' >&2
    exit 1
  fi

  if ! grep -q 'git rev-parse origin/main' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must compare against the latest origin/main\n' >&2
    exit 1
  fi

  if ! grep -q 'steps.freshness.outputs.stale' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must block stale branch pushes\n' >&2
    exit 1
  fi
}

test_release_prepare_workflow_closes_superseded_prs() {
  reset_release_train_state

  if ! grep -q 'startswith(\\"release-prepare/\\")' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must identify open release prepare PRs\n' >&2
    exit 1
  fi

  if ! grep -q 'Superseded by release PR' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must explain why older release PRs are closed\n' >&2
    exit 1
  fi

  if ! grep -q 'gh pr close' .github/workflows/release-prepare.yml; then
    printf 'release prepare workflow must close superseded release PRs\n' >&2
    exit 1
  fi
}

test_typescript_dry_run_uses_pack_without_registry_publish() {
  reset_release_train_state

  if ! grep -q 'npm pack --dry-run --ignore-scripts' scripts/release-train.sh; then
    printf 'TypeScript dry-run must validate the package with npm pack\n' >&2
    exit 1
  fi

  if grep -q 'npm publish --access public --dry-run --ignore-scripts' scripts/release-train.sh; then
    printf 'TypeScript dry-run must not contact the registry through npm publish\n' >&2
    exit 1
  fi
}

test_cli_has_no_unused_git2_dependency() {
  if grep -q '^git2[[:space:]]*=' cli/Cargo.toml; then
    printf 'actr-cli must not depend on unused git2\n' >&2
    exit 1
  fi

  if grep -q 'git2::Error' cli/src/error.rs; then
    printf 'actr-cli must not retain unused git2 conversion\n' >&2
    exit 1
  fi
}

test_release_train_delegates_cli_release_operations() {
  if ! grep -q 'scripts/release-actr-cli.sh.*"${args\[@\]}"' scripts/release-train.sh; then
    printf 'release train must delegate CLI release operations\n' >&2
    exit 1
  fi

  if grep -q '^build_cli_binary_target()' scripts/release-train.sh; then
    printf 'release train must not retain the CLI build implementation\n' >&2
    exit 1
  fi
}

test_python_release_tools_are_stage_scoped() {
  reset_release_train_state

  STAGE="build-cli-binaries"
  if stage_requires_python_release_tools; then
    printf 'CLI build stage must not install Python release tools\n' >&2
    exit 1
  fi

  STAGE="publish-python"
  if ! stage_requires_python_release_tools; then
    printf 'Python publish stage must install Python release tools\n' >&2
    exit 1
  fi

  SKIP_PYTHON=true
  if stage_requires_python_release_tools; then
    printf 'skip-python must disable Python release tool installation\n' >&2
    exit 1
  fi
}

test_cli_release_workflows_share_one_matrix() {
  local reusable=.github/workflows/_actr-cli-release.yml
  local manual=.github/workflows/release-actr-cli.yml
  [[ -f "$reusable" ]] || { printf 'reusable CLI release workflow is missing\n' >&2; exit 1; }
  [[ -f "$manual" ]] || { printf 'manual CLI release workflow is missing\n' >&2; exit 1; }

  grep -q '^  workflow_call:' "$reusable" || {
    printf 'reusable CLI release workflow must declare workflow_call\n' >&2
    exit 1
  }
  grep -q '^  workflow_dispatch:' "$manual" || {
    printf 'manual CLI release workflow must declare workflow_dispatch\n' >&2
    exit 1
  }
  grep -q 'uses: ./\.github/workflows/_actr-cli-release.yml' "$manual" || {
    printf 'manual CLI workflow must call the reusable workflow\n' >&2
    exit 1
  }
  if grep -q 'matrix:' "$manual"; then
    printf 'manual CLI workflow must not duplicate the target matrix\n' >&2
    exit 1
  fi

  local target
  for target in \
    x86_64-unknown-linux-gnu \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-gnu \
    aarch64-unknown-linux-musl \
    aarch64-apple-darwin \
    x86_64-pc-windows-msvc; do
    assert_eq "1" "$(grep -c "target: ${target}" "$reusable")" "${target} matrix count"
  done
}

test_release_train_reuses_cli_workflow() {
  local workflow=.github/workflows/release-train.yml
  grep -q 'uses: ./\.github/workflows/_actr-cli-release.yml' "$workflow" || {
    printf 'release train must call the reusable CLI workflow\n' >&2
    exit 1
  }
  if grep -q '^  build-cli-binaries:' "$workflow" || grep -q '^  publish-cli-binaries:' "$workflow"; then
    printf 'release train must not retain embedded CLI workflow jobs\n' >&2
    exit 1
  fi
  if grep -q '^      cli_only:' "$workflow"; then
    printf 'release train must not retain cli_only dispatch mode\n' >&2
    exit 1
  fi
  if grep -q '^  publish-cli-only:' "$workflow"; then
    printf 'release train must not retain publish-cli-only bridge job\n' >&2
    exit 1
  fi
  local manual=.github/workflows/release-actr-cli.yml
  grep -q 'uses: ./\.github/workflows/_actr-cli-release.yml' "$manual" || {
    printf 'manual CLI release workflow must call the reusable CLI workflow\n' >&2
    exit 1
  }
}

test_protoc_plugin_release_workflow_is_reusable() {
  local reusable=.github/workflows/_protoc-plugins-release.yml
  local manual=.github/workflows/release-protoc-plugins.yml
  [[ -f "$reusable" ]] || { printf 'reusable protoc plugins release workflow is missing\n' >&2; exit 1; }
  [[ -f "$manual" ]] || { printf 'manual protoc plugins release workflow is missing\n' >&2; exit 1; }

  grep -q '^  workflow_call:' "$reusable" || {
    printf 'reusable protoc plugins release workflow must declare workflow_call\n' >&2
    exit 1
  }
  grep -q '^  workflow_dispatch:' "$manual" || {
    printf 'manual protoc plugins release workflow must declare workflow_dispatch\n' >&2
    exit 1
  }
  grep -q 'uses: ./\.github/workflows/_protoc-plugins-release.yml' "$manual" || {
    printf 'manual protoc plugins workflow must call the reusable workflow\n' >&2
    exit 1
  }
  if grep -q '^  build-rust:' "$manual" || grep -q '^  publish:' "$manual"; then
    printf 'manual protoc plugins workflow must not duplicate build or publish jobs\n' >&2
    exit 1
  fi

  local language
  for language in Rust Swift Kotlin TypeScript; do
    grep -q "${language} plugin reports" "$reusable" || {
      printf 'reusable protoc plugins workflow must verify the %s plugin version\n' "$language" >&2
      exit 1
    }
  done
}

test_release_train_publishes_release_assets_in_parallel() {
  local workflow=.github/workflows/release-train.yml
  grep -q '^  publish-cli:' "$workflow" || {
    printf 'release train must publish CLI assets\n' >&2
    exit 1
  }
  grep -q '^  publish-actrix:' "$workflow" || {
    printf 'release train must publish actrix assets\n' >&2
    exit 1
  }
  grep -q '^  publish-protoc-plugins:' "$workflow" || {
    printf 'release train must publish protoc plugin assets\n' >&2
    exit 1
  }
  grep -q 'uses: ./\.github/workflows/_protoc-plugins-release.yml' "$workflow" || {
    printf 'release train must call the reusable protoc plugins workflow\n' >&2
    exit 1
  }
  if ! sed -n '/^  publish-protoc-plugins:/,/^    secrets: inherit/p' "$workflow" | grep -q 'create-release'; then
    printf 'protoc plugin release assets must wait for create-release\n' >&2
    exit 1
  fi
  if ! sed -n '/^  collect-report:/,/^    runs-on:/p' "$workflow" | grep -q 'publish-protoc-plugins'; then
    printf 'release report must wait for protoc plugin release assets\n' >&2
    exit 1
  fi
  if ! sed -n '/^  publish-cli:/,/^    secrets: inherit/p' "$workflow" | grep -Fq "source_ref: \${{ fromJSON(needs.context.outputs.dry_run) && needs.context.outputs.release_sha || format('v{0}', needs.context.outputs.version) }}"; then
    printf 'CLI assets must build from release_sha during release train dry-run\n' >&2
    exit 1
  fi
  if ! sed -n '/^  publish-actrix:/,/^    secrets: inherit/p' "$workflow" | grep -Fq "source_ref: \${{ fromJSON(needs.context.outputs.dry_run) && needs.context.outputs.release_sha || format('v{0}', needs.context.outputs.version) }}"; then
    printf 'actrix assets must build from release_sha during release train dry-run\n' >&2
    exit 1
  fi
  if ! sed -n '/^  publish-protoc-plugins:/,/^    secrets: inherit/p' "$workflow" | grep -Fq "source_ref: \${{ fromJSON(needs.context.outputs.dry_run) && needs.context.outputs.release_sha || format('v{0}', needs.context.outputs.version) }}"; then
    printf 'protoc plugin assets must build from release_sha during release train dry-run\n' >&2
    exit 1
  fi
  local reusable
  for reusable in \
    .github/workflows/_actr-cli-release.yml \
    .github/workflows/_actrix-release.yml \
    .github/workflows/_protoc-plugins-release.yml; do
    if ! grep -B3 -A3 'path: tag-source' "$reusable" | grep -Fq 'if: inputs.publish'; then
      printf '%s must skip tag checkout when publish=false\n' "$reusable" >&2
      exit 1
    fi
    if ! grep -A3 'path: tag-source' "$reusable" | grep -Fq 'fetch-depth: 0'; then
      printf '%s must keep the release tag checkout scoped and explicit\n' "$reusable" >&2
      exit 1
    fi
  done
}

test_actrix_release_stages_binaries_with_bash() {
  local workflow=.github/workflows/_actrix-release.yml
  if ! sed -n '/name: Stage binary/,/name: Upload artifacts/p' "$workflow" | grep -q 'shell: bash'; then
    printf 'actrix release Stage binary step must use bash for Windows runner compatibility\n' >&2
    exit 1
  fi
  if grep -Fq 'name: actrix-${{ matrix.target.asset }}' "$workflow"; then
    printf 'actrix release workflow must not double-prefix e2e artifact names\n' >&2
    exit 1
  fi
  if ! grep -Fq 'name: ${{ matrix.target.asset }}' "$workflow"; then
    printf 'actrix release workflow must publish artifact names consumed by e2e\n' >&2
    exit 1
  fi
}

test_detect_conventional_bump_reads_commit_without_trailing_newline
test_parse_prepare_only_mode
test_parse_stage_argument
test_parse_stage_publish_rust
test_parse_stage_all_is_default
test_parse_stage_rejects_unknown
test_validate_version_requires_strict_semver
test_validate_version_requires_pep440_when_python_is_enabled
test_append_skipped_components_allows_empty_list
test_publish_clean_check_rejects_untracked_files
test_publish_clean_check_allows_current_report_artifacts
test_final_tag_uses_conventional_v_prefix
test_latest_release_tag_accepts_legacy_release_train_prefix
test_release_prepare_skips_release_commit_head
test_publish_mode_uses_prepared_versions_without_mutating
test_prepare_only_updates_validates_and_commits_without_publishing
test_create_tag_dry_run_does_not_push
test_main_publish_stage_skips_tag_availability_check
test_main_validate_and_create_tag_check_absent_tag
test_publish_python_package_builds_distribution_before_upload
test_publish_python_package_uses_testpypi_for_pre_release
test_publish_rust_package_prepares_cli_web_assets_before_publish
test_publish_typescript_workload_builds_before_publish
test_publish_typescript_package_writes_native_and_main_state
test_release_train_workflow_publish_typescript_uses_script_stage
test_release_train_workflow_downloads_only_typescript_native_artifacts
test_release_prepare_workflow_skips_release_commits
test_report_stage_merges_state_files
test_update_versions_syncs_optional_dependencies
test_publish_web_packages_skips_puppeteer_download
test_cli_zigbuild_uses_zigbuild_arguments_without_build_subcommand
test_cli_release_state_rows_match_report_schema
test_cli_sha256_uses_sha256sum_without_shasum
test_cli_release_upload_asset_deletes_existing_asset_before_retry
test_cli_validation_tag_requires_strict_semver
test_protoc_plugin_release_workflow_is_reusable
test_release_train_publishes_release_assets_in_parallel
test_actrix_release_stages_binaries_with_bash
test_publish_web_workflow_has_timeout
test_release_prepare_workflow_rejects_stale_runs
test_release_prepare_workflow_closes_superseded_prs
test_typescript_dry_run_uses_pack_without_registry_publish
test_cli_has_no_unused_git2_dependency
test_release_train_delegates_cli_release_operations
test_python_release_tools_are_stage_scoped
test_cli_release_workflows_share_one_matrix
test_release_train_reuses_cli_workflow
