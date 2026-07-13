#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
cd "$repo_root"

python3 - <<'PY'
from __future__ import annotations

import re
from pathlib import Path


workflow_path = Path(".github/workflows/release-asset-validation.yml")
protoc_path = Path(".github/workflows/_protoc-plugins-release.yml")
workflow = workflow_path.read_text()
protoc = protoc_path.read_text()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def job_block(name: str) -> str:
    match = re.search(
        rf"(?ms)^  {re.escape(name)}:\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        workflow,
    )
    require(match is not None, f"missing workflow job: {name}")
    return match.group(0)


def step_block(job: str, name: str) -> str:
    match = re.search(
        rf"(?ms)^      - name: {re.escape(name)}\n.*?(?=^      - name: |\Z)",
        job,
    )
    require(match is not None, f"missing workflow step: {name}")
    return match.group(0)


native = job_block("typescript-native-assets")
expected_targets = {
    "x86_64-apple-darwin": "actr.darwin-x64.node",
    "aarch64-apple-darwin": "actr.darwin-arm64.node",
    "x86_64-unknown-linux-gnu": "actr.linux-x64-gnu.node",
    "x86_64-unknown-linux-musl": "actr.linux-x64-musl.node",
    "aarch64-unknown-linux-gnu": "actr.linux-arm64-gnu.node",
    "aarch64-unknown-linux-musl": "actr.linux-arm64-musl.node",
    "x86_64-pc-windows-msvc": "actr.win32-x64-msvc.node",
}
for target, artifact in expected_targets.items():
    require(native.count(f"target: {target}") == 1, f"native matrix must contain {target} once")
    require(native.count(f"artifact: {artifact}") == 1, f"native matrix must contain {artifact} once")

npm = job_block("npm-package-assets")
require("      - typescript-native-assets\n" in npm, "npm packaging must depend on native builds")
require("pattern: actr.*.node" in npm, "npm packaging must download every native artifact")
require("merge-multiple: true" in npm, "native artifacts must be merged")
require("npx napi create-npm-dirs" in npm, "npm platform directories must be generated")
require("npm run artifacts -- --output-dir artifacts" in npm, "native binaries must populate npm packages")
for package_dir in (
    "darwin-x64",
    "darwin-arm64",
    "linux-x64-gnu",
    "linux-x64-musl",
    "linux-arm64-gnu",
    "linux-arm64-musl",
    "win32-x64-msvc",
):
    require(f"npm/{package_dir}" in npm, f"npm package {package_dir} must be packed")

require("  python-package-assets:\n" not in workflow, "Python distributions must not have a duplicate standalone producer")
require("python -m build tools/protoc-gen/python" not in workflow, "validation workflow must delegate Python builds to protoc workflow")
require(protoc.count("python -m build") == 1, "protoc workflow must have exactly one Python build")
require("python -m twine check" in protoc, "Python distributions must pass twine validation")
require("pattern: protoc-gen-*" in protoc, "protoc publisher must only download its own artifacts")

asset_jobs = {
    "rust-package-assets",
    "typescript-native-assets",
    "npm-package-assets",
    "cli-assets",
    "actrix-assets",
    "protoc-plugin-assets",
    "swift-package",
    "kotlin-package",
}
for sync_job_name in (
    "prepare-swift-package-sync",
    "publish-swift-package-sync",
    "publish-kotlin-package-sync",
):
    sync_job = job_block(sync_job_name)
    for dependency in asset_jobs:
        require(
            f"      - {dependency}\n" in sync_job,
            f"{sync_job_name} must wait for {dependency}",
        )

for sync_job_name, language in (
    ("prepare-swift-package-sync", "Swift"),
    ("publish-kotlin-package-sync", "Kotlin"),
):
    sync_job = job_block(sync_job_name)
    probe = step_block(sync_job, f"Probe {language} package-sync publishing")
    require(
        "GH_TOKEN: ${{ secrets.PACKAGE_SYNC_GITHUB_TOKEN }}" in probe,
        f"{language} probe must authenticate with the package-sync token",
    )
    require("github.token" not in probe, f"{language} probe must not use the source repository token")
    require("persist-credentials: false" in sync_job, f"{language} sync checkouts must not persist the PAT")

for sync_job_name in ("publish-swift-package-sync", "publish-kotlin-package-sync"):
    sync_job = job_block(sync_job_name)
    require(
        '[[ "$REPLACE_ASSETS" == "true" ]] && upload_args+=(--clobber)' in sync_job,
        f"{sync_job_name} must honor replace_assets",
    )

swift_prepare_job = job_block("prepare-swift-package-sync")
swift_prepare = step_block(swift_prepare_job, "Prepare Swift package-sync tag")
require("swift build" in swift_prepare, "Swift package-sync preparation must validate the package")
require("GH_TOKEN:" not in swift_prepare, "Swift manifest execution must not receive GH_TOKEN")
swift_publish_job = job_block("publish-swift-package-sync")
require("swift build" not in swift_publish_job, "secret-bearing Swift publisher must not execute the manifest")
require(
    "needs.prepare-swift-package-sync.outputs.publish == 'true'" in swift_publish_job,
    "Swift publishing must require successful isolated preparation",
)
require(
    "Reusing immutable Swift package-sync asset" in swift_publish_job,
    "existing Swift tags must not clobber immutable release assets",
)

inputs = workflow.split("concurrency:", 1)[0]
require("package_sync_owner:" not in inputs, "package-sync owner must not be dispatcher-controlled")
require(
    'package_sync_owner="${GITHUB_REPOSITORY_OWNER}"' in workflow,
    "package-sync owner must be fixed to the current repository owner",
)
require(
    workflow.count('"coordinate": f"io.actrium:actr:{os.environ[\'PRE_VERSION\']}"') == 2,
    "Kotlin package-sync metadata must use the documented Maven coordinate",
)
gradle = Path("bindings/kotlin/actr-kotlin/build.gradle.kts").read_text()
require('artifactId = "actr"' in gradle, "Kotlin publication must pin the documented artifact ID")

print("release asset validation workflow checks passed")
PY
