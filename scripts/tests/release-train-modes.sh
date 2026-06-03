#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"

script_under_test=$(mktemp)
sed '/^main "\$@"$/d' scripts/release-train.sh >"$script_under_test"
# shellcheck source=/dev/null
source "$script_under_test"
rm -f "$script_under_test"

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

test_parse_prepare_only_mode() {
  reset_release_train_state

  parse_args --version 1.2.3 --prepare-only

  assert_eq "1.2.3" "$VERSION" "VERSION"
  assert_eq "true" "$PREPARE_ONLY" "PREPARE_ONLY"
  assert_eq "prepare" "$RUN_MODE" "RUN_MODE"
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

  if ! (cd "$temp_repo" && ensure_publish_worktree_clean >/dev/null 2>&1); then
    printf 'publish clean check must allow current release report artifacts\n' >&2
    rm -rf "$temp_repo"
    exit 1
  fi

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
  publish_rust_package() { calls+=("publish_rust_package:$1:$2"); }
  publish_python_package() { calls+=("publish_python_package"); }
  skip_python_package() { calls+=("skip_python_package"); }
  create_final_tag() { calls+=("create_final_tag"); }
  publish_package_sync_repo() { calls+=("publish_package_sync_repo:$2"); }
  publish_web_packages() { calls+=("publish_web_packages"); }
  publish_typescript_package() { calls+=("publish_typescript_package"); }

  VERSION="1.2.3"
  DRY_RUN=false
  PREPARE_ONLY=false
  SKIP_PYTHON=true
  SKIP_WEB=false

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
  assert_eq "run_validation_suite" "${calls[1]}" "second publish step"
  assert_eq "ensure_publish_worktree_clean" "${calls[2]}" "third publish step"
  assert_eq "set_release_sha" "${calls[3]}" "fourth publish step"
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

test_parse_prepare_only_mode
test_append_skipped_components_allows_empty_list
test_publish_clean_check_rejects_untracked_files
test_publish_clean_check_allows_current_report_artifacts
test_publish_mode_uses_prepared_versions_without_mutating
test_prepare_only_updates_validates_and_commits_without_publishing
