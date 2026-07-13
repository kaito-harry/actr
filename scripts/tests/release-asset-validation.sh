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
swift_packaging_path = Path("bindings/swift/scripts/package-binary.sh")
verify_path = Path(".github/workflows/release-train-verify.yml")
reusable_paths = (
    Path(".github/workflows/_actr-cli-release.yml"),
    Path(".github/workflows/_actrix-release.yml"),
    protoc_path,
)
workflow = workflow_path.read_text()
protoc = protoc_path.read_text()
swift_packaging = swift_packaging_path.read_text()
verify_workflow = verify_path.read_text()
reusable_workflows = {path.name: path.read_text() for path in reusable_paths}


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


def permissions(block: str, indent: int) -> dict[str, str]:
    prefix = " " * indent
    match = re.search(
        rf"(?m)^{prefix}permissions:\n((?:^{prefix}  [A-Za-z0-9_-]+: [A-Za-z]+\n)+)",
        block,
    )
    require(match is not None, f"missing permissions block at indentation {indent}")
    entries: dict[str, str] = {}
    for line in match.group(1).splitlines():
        key, value = line.strip().split(": ", 1)
        entries[key] = value
    return entries


preamble, _ = workflow.split("\njobs:\n", 1)
default_permissions = permissions(preamble, 0)
require(
    default_permissions.get("contents") == "read",
    "workflow contents permission must default to read-only",
)
require(
    default_permissions == {"actions": "read", "contents": "read"},
    "workflow default permissions must contain only required read grants",
)

package_sync_input = re.search(
    r"(?ms)^      publish_package_sync:\n.*?(?=^      [A-Za-z0-9_-]+:\n|^concurrency:)",
    preamble,
)
require(package_sync_input is not None, "missing publish_package_sync input")
require(
    "        default: never\n" in package_sync_input.group(0),
    "package-sync publishing must be opt-in by default",
)

def checkout_steps(document: str) -> list[str]:
    result: list[str] = []
    for checkout_use in re.finditer(r"actions/checkout@[^\s]+", document):
        step_start = document.rfind("\n      - ", 0, checkout_use.start())
        require(step_start >= 0, "checkout action must be inside a workflow step")
        step_start += 1
        next_step = document.find("\n      - ", checkout_use.end())
        next_job_match = re.search(
            r"(?m)^  [A-Za-z0-9_-]+:\n", document[checkout_use.end() :]
        )
        next_job = (
            checkout_use.end() + next_job_match.start()
            if next_job_match is not None
            else len(document)
        )
        step_end = min(position for position in (next_step, next_job) if position >= 0)
        result.append(document[step_start:step_end])
    return result


for workflow_name, document in {
    workflow_path.name: workflow,
    **reusable_workflows,
}.items():
    checkouts = checkout_steps(document)
    require(
        len(checkouts) == document.count("uses: actions/checkout@"),
        f"every checkout in {workflow_name} must be inspected",
    )
    for checkout in checkouts:
        name = re.search(r"(?m)^      - name: (.+)$", checkout)
        label = name.group(1) if name else "unnamed checkout"
        require(
            "persist-credentials: false" in checkout,
            f"{workflow_name}: {label} must not persist repository credentials",
        )

write_jobs = {
    "publish-validation-tag",
    "create-validation-release",
    "rust-package-assets",
    "npm-package-assets",
    "cli-assets",
    "actrix-assets",
    "protoc-plugin-assets",
    "swift-package",
    "kotlin-package",
}
for match in re.finditer(r"(?m)^  ([A-Za-z0-9_-]+):\n", workflow):
    name = match.group(1)
    job = job_block(name)
    require(
        re.search(r"(?m)^    permissions:[ \t]+\S+", job) is None,
        f"job {name} must not use scalar permissions",
    )
    permission_match = re.search(
        r"(?m)^    permissions:\n((?:^      [A-Za-z0-9_-]+: [A-Za-z]+\n)+)",
        job,
    )
    require(
        "\n    permissions:\n" not in job or permission_match is not None,
        f"job {name} permissions must use a validated mapping",
    )
    job_permissions: dict[str, str] = {}
    if permission_match:
        for line in permission_match.group(1).splitlines():
            key, value = line.strip().split(": ", 1)
            job_permissions[key] = value
    has_write = any(value == "write" for value in job_permissions.values())
    require(
        not has_write or name in write_jobs,
        f"non-publishing job {name} must not receive write permission",
    )

for name in write_jobs:
    require(
        permissions(job_block(name), 4) == {"contents": "write"},
        f"publishing job {name} must request only contents: write explicitly",
    )

build_publish_pairs = (
    ("build-rust-package-assets", "rust-package-assets", "rust-package-assets"),
    ("build-npm-package-assets", "npm-package-assets", "npm-package-assets"),
    ("build-swift-package", "swift-package", "swift-validation-assets"),
    ("build-kotlin-package", "kotlin-package", "kotlin-validation-assets"),
)
for build_name, publish_name, artifact_name in build_publish_pairs:
    build_job = job_block(build_name)
    publish_job = job_block(publish_name)
    require(
        f"name: {artifact_name}" in build_job,
        f"{build_name} must stage {artifact_name}",
    )
    require(
        f"      - {build_name}\n" in publish_job,
        f"{publish_name} must depend on {build_name}",
    )
    require(
        f"name: {artifact_name}" in publish_job,
        f"{publish_name} must download {artifact_name}",
    )
    require(
        publish_job.count("uses:") == 1
        and publish_job.count("uses: actions/download-artifact@") == 1,
        f"pure publisher {publish_name} may only use download-artifact",
    )
    for forbidden in (
        "actions/checkout@",
        "./gradlew",
        "source/",
        "working-directory:",
    ):
        require(
            forbidden not in publish_job,
            f"pure publisher {publish_name} must not execute {forbidden}",
        )
    require(
        re.search(
            r"(?m)^ {10,}(?:cargo|npm|pnpm|swift|bash|python(?:3)?|node)\s",
            publish_job,
        )
        is None,
        f"pure publisher {publish_name} must not execute downloaded build scripts",
    )

prepare_source = job_block("prepare-validation-source")
require("git push" not in prepare_source, "source preparation must not push with read permission")
require(
    "git bundle create validation-source.bundle" in prepare_source
    and '"^${SOURCE_SHA}"' in prepare_source
    and "name: validation-source-tag" in prepare_source,
    "source preparation must pass the isolated tag through a thin bundle",
)
publish_tag = job_block("publish-validation-tag")
for marker in (
    "validation-source.bundle",
    'tag_sha=$(git rev-parse "refs/tags/${TAG}^{commit}")',
    'tag_parent=$(git rev-parse "${tag_sha}^")',
    '[[ "$tag_sha" == "$VALIDATION_SHA" && "$tag_parent" == "$SOURCE_SHA" ]]',
    "::add-mask::",
    "extraheader=AUTHORIZATION: basic",
):
    require(marker in publish_tag, f"validation tag publisher must contain {marker}")
publish_tag_download = step_block(publish_tag, "Download prepared validation tag")
require(
    "if: needs.prepare-validation-source.outputs.push_tag == 'true'"
    in publish_tag_download,
    "validation tag bundle must only be downloaded when a new tag is prepared",
)
validation_release = job_block("create-validation-release")
require(
    "      - publish-validation-tag\n" in validation_release
    and "--verify-tag" in validation_release,
    "validation release must wait for and verify the isolated tag publisher",
)

for name, document in reusable_workflows.items():
    reusable_preamble, reusable_jobs = document.split("\njobs:\n", 1)
    require(
        permissions(reusable_preamble, 0) == {"contents": "read"},
        f"{name} must default to contents: read",
    )
    publish_match = re.search(
        r"(?ms)^  publish:\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)", document
    )
    require(publish_match is not None, f"{name} must define a publish job")
    require(
        permissions(publish_match.group(0), 4) == {"contents": "write"},
        f"{name} publish job must request only contents: write",
    )
    require(
        document.count("      contents: write") == 1,
        f"{name} may grant contents: write only once",
    )
    for job_match in re.finditer(
        r"(?ms)^  ([A-Za-z0-9_-]+):\n.*?(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        "jobs:\n" + reusable_jobs,
    ):
        reusable_job_name = job_match.group(1)
        reusable_job = job_match.group(0)
        require(
            re.search(r"(?m)^    permissions:[ \t]+\S+", reusable_job) is None,
            f"{name}:{reusable_job_name} must not use scalar permissions",
        )
        mapping = re.search(
            r"(?m)^    permissions:\n((?:^      [A-Za-z0-9_-]+: [A-Za-z]+\n)+)",
            reusable_job,
        )
        require(
            "\n    permissions:\n" not in reusable_job or mapping is not None,
            f"{name}:{reusable_job_name} permissions must use a validated mapping",
        )
        if mapping is not None:
            grants = {
                line.strip().split(": ", 1)[0]: line.strip().split(": ", 1)[1]
                for line in mapping.group(1).splitlines()
            }
            require(
                reusable_job_name == "publish" or "write" not in grants.values(),
                f"{name}:{reusable_job_name} must not receive write permission",
            )

for caller, reusable_name in (
    ("cli-assets", "_actr-cli-release.yml"),
    ("actrix-assets", "_actrix-release.yml"),
    ("protoc-plugin-assets", "_protoc-plugins-release.yml"),
):
    caller_job = job_block(caller)
    require(
        f"uses: ./.github/workflows/{reusable_name}" in caller_job,
        f"{caller} must call {reusable_name}",
    )
    require("secrets: inherit" not in caller_job, f"{caller} must not inherit secrets")


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

npm = job_block("build-npm-package-assets")
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
        "--json isDraft" in sync_job
        and "--draft=false" in sync_job
        and "needs_clobber" in sync_job,
        f"{sync_job_name} must repair incomplete draft releases before publishing",
    )
    require(
        "REPLACE_ASSETS" not in sync_job,
        f"{sync_job_name} assets must remain hash-locked after publication",
    )

swift_validation_upload = step_block(
    job_block("swift-package"), "Upload Swift validation assets"
)
require(
    'ActrFFI.xcframework.${checksum}.zip' in swift_validation_upload,
    "Swift validation releases must retain a content-addressed recovery asset",
)
require(
    "gh release download" in swift_validation_upload
    and '[[ "$existing_checksum" == "$checksum" ]]' in swift_validation_upload,
    "an existing Swift recovery asset must be checksum-verified before reuse",
)

swift_prepare_job = job_block("prepare-swift-package-sync")
swift_prepare = step_block(swift_prepare_job, "Prepare Swift package-sync tag")
require("swift build" in swift_prepare, "Swift package-sync preparation must validate the package")
require("GH_TOKEN:" not in swift_prepare, "Swift manifest execution must not receive GH_TOKEN")
require(
    "re.subn(" in swift_prepare and "count != 1" in swift_prepare,
    "Swift manifest updates must require each expected substitution exactly once",
)
require(
    "re.sub(" not in swift_prepare,
    "Swift manifest preparation must not use unchecked regular-expression substitutions",
)
require(
    "swift-sync/ActrFFI.xcframework/" in swift_prepare
    and "ACTR_BINARY_PATH=ActrFFI.xcframework" in swift_prepare
    and "rm -rf swift-sync/ActrFFI.xcframework" in swift_prepare,
    "Swift preparation must build against a temporary package-root XCFramework",
)
for existing_assignment in (
    '"release tag"',
    '"remote binary URL"',
    '"remote binary checksum"',
    "expected exactly one existing",
):
    require(
        existing_assignment in swift_prepare,
        f"existing Swift tags must validate {existing_assignment}",
    )
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
swift_publish = step_block(swift_publish_job, "Publish Swift package-sync tag and release")
require(
    'recovery_name="ActrFFI.xcframework.${TAG_CHECKSUM}.zip"' in swift_publish
    and 'gh release download "$SOURCE_TAG"' in swift_publish
    and '--repo "$SOURCE_REPO"' in swift_publish,
    "Swift retries must recover tag-addressed bytes from the validation release",
)
require(
    "local_checksum=" in swift_publish
    and re.search(r'\[\[ "\$local_checksum" == "\$TAG_CHECKSUM" \]\]', swift_publish),
    "Swift publishing must verify the local asset against the immutable tag checksum",
)
require(
    "gh release download" in swift_publish and "remote_checksum=" in swift_publish,
    "existing Swift release assets must be downloaded before reuse",
)
require(
    re.search(r'\[\[ "\$remote_checksum" != "\$TAG_CHECKSUM" \]\]', swift_publish)
    is not None
    and '[[ "$release_is_draft" == "true" ]]' in swift_publish,
    "existing Swift release assets must match the immutable tag checksum",
)
for deterministic_zip_markers in (
    ("zipfile.ZipInfo", "ZipInfo("),
    ("date_time = (1980, 1, 1, 0, 0, 0)",),
    ("zipfile.ZIP_STORED", "ZIP_STORED"),
    ("sorted(",),
    ("lstat()",),
    ("S_ISLNK",),
    ("external_attr",),
):
    require(
        any(marker in swift_packaging for marker in deterministic_zip_markers),
        "Swift archive creation must use deterministic setting: "
        + " or ".join(deterministic_zip_markers),
    )

kotlin_build_job = job_block("build-kotlin-package")
require(
    "publishMavenPublicationToValidationStagingRepository" in kotlin_build_job
    and "actrValidationStagingUrl" in kotlin_build_job,
    "read-only Kotlin builder must stage the Maven publication",
)
require(
    "PACKAGE_SYNC_GITHUB_TOKEN" not in kotlin_build_job,
    "Kotlin source Gradle execution must not receive the package-sync PAT",
)

kotlin_publish_job = job_block("publish-kotlin-package-sync")
require(
    "group: release-asset-validation-kotlin-maven-${{ github.repository_owner }}"
    in kotlin_publish_job
    and "cancel-in-progress: false" in kotlin_publish_job,
    "Kotlin Maven metadata updates must be serialized across validation versions",
)
kotlin_validation = step_block(kotlin_publish_job, "Validate Kotlin package-sync inputs")
kotlin_publish = step_block(kotlin_publish_job, "Publish Kotlin Maven package, tag, and release")
for validation_marker in (
    "staged_module=",
    "unexpected staged Maven files",
    "component_mismatches",
    "invalid {algorithm} checksum",
    "cmp -s \"$staged_aar\" kotlin-inputs/actr-kotlin-release.aar",
):
    require(
        validation_marker in kotlin_validation,
        f"Kotlin staged inputs must validate {validation_marker}",
    )
for forbidden in (
    "./gradlew",
    "package-binary.sh",
    "source/bindings/kotlin",
    "actions/setup-java",
):
    require(
        forbidden not in kotlin_publish_job,
        f"secret-bearing Kotlin publisher must not execute or read {forbidden}",
    )
require(
    'maven_repo="${SYNC_REPO,,}"' in kotlin_publish,
    "Kotlin Maven owner and repository path must be lowercased",
)
require(
    "--retry-all-errors" in kotlin_publish and kotlin_publish.count("--retry 3") >= 3,
    "Kotlin Maven network operations must retry transient failures",
)
require(
    "pom_status" in kotlin_publish
    and "aar_status" in kotlin_publish
    and "partial:${pom_status}:${aar_status}" in kotlin_publish
    and "inaccessible:${pom_status}:${aar_status}" in kotlin_publish,
    "Kotlin Maven probing must distinguish complete, partial, and inaccessible states",
)
require(
    "reconcile_maven_primary" in kotlin_publish
    and "reconcile_maven_checksums" in kotlin_publish
    and "reconcile_gradle_module" in kotlin_publish
    and "canonicalize_gradle_module" in kotlin_publish,
    "Kotlin Maven retries must reconcile canonical files, module metadata, and checksums",
)
require(
    "Gradle module artifact metadata mismatch" in kotlin_publish
    and "artifact.update(expected)" in kotlin_publish
    and 'f"actr-{version}-sources.jar"' in kotlin_publish
    and 'actr-${PRE_VERSION}-sources.jar"' in kotlin_publish
    and 'module.get("formatVersion") != "1.1"' in kotlin_publish
    and "compare_gradle_modules" in kotlin_publish
    and "published Gradle module semantics differ" in kotlin_publish,
    "Kotlin retries must validate canonical module format, graph, and artifact hashes",
)
require(
    'upload_maven_file "$checksum_file" "$checksum_path"' in kotlin_publish
    and "did not become consistent" in kotlin_publish,
    "derived Maven checksums must be repairable and verified after upload",
)
require(
    'reconcile_maven_checksums "$remote_metadata" "$relative_path" "md5 sha1"'
    in kotlin_publish,
    "repository metadata must use the checksums supported by GitHub Packages",
)
require(
    "ensure_maven_metadata" in kotlin_publish
    and "existing_versions" in kotlin_publish
    and "Published Maven metadata does not contain" in kotlin_publish,
    "Kotlin Maven publication must merge and verify repository version metadata",
)
require(
    'upload_maven_file "$staged_metadata"' not in kotlin_publish,
    "isolated staging metadata must never overwrite repository version history",
)
for pom_property in (
    "actr.source.repository",
    "actr.source.tag",
    "actr.source.sha",
):
    require(
        pom_property in kotlin_publish,
        f"Kotlin Maven reuse must validate provenance property {pom_property}",
    )
aar_reconcile_position = kotlin_publish.find(
    'reconcile_maven_primary "kotlin-maven/io/actrium/actr/${PRE_VERSION}/actr-${PRE_VERSION}.aar"'
)
pom_reconcile_position = kotlin_publish.find(
    'reconcile_maven_primary "kotlin-maven/io/actrium/actr/${PRE_VERSION}/actr-${PRE_VERSION}.pom"'
)
module_reconcile_position = kotlin_publish.find("reconcile_gradle_module\n")
metadata_position = kotlin_publish.find("ensure_maven_metadata\n")
tag_push_position = kotlin_publish.find('push origin "refs/tags/${sync_tag}"')
require(
    0
    <= aar_reconcile_position
    < module_reconcile_position
    < pom_reconcile_position
    < metadata_position
    < tag_push_position,
    "Kotlin Maven AAR, module, POM, and metadata must be complete before pushing the tag",
)
for immutable_marker in (
    'canonical_aar="$probe_dir/actr.aar"',
    '"maven_aar_sha256"',
    '"native_zip_sha256"',
    'recovery_name="actr-kotlin-native.${native_checksum}.zip"',
    'aar_asset="$canonical_dir/actr-kotlin-release.aar"',
    "--verify-tag",
):
    require(
        immutable_marker in kotlin_publish,
        f"Kotlin retry state must retain {immutable_marker}",
    )
kotlin_gradle = Path("bindings/kotlin/actr-kotlin/build.gradle.kts").read_text()
for pom_property in (
    "actr.source.repository",
    "actr.source.tag",
    "actr.source.sha",
):
    require(
        pom_property in kotlin_gradle,
        f"Kotlin Maven POM must record provenance property {pom_property}",
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
require('artifactId = "actr"' in kotlin_gradle, "Kotlin publication must pin the documented artifact ID")
require(
    "https://maven.pkg.github.com/actrium/actr-kotlin-package-sync" in kotlin_gradle,
    "Kotlin Maven default repository owner must be lowercase",
)
require(
    "actrValidationStagingUrl" in kotlin_gradle
    and 'name = "validationStaging"' in kotlin_gradle,
    "Kotlin Gradle publication must support credential-free local staging",
)
for filtered_path in (
    '"bindings/swift/scripts/package-binary.sh"',
    '"bindings/kotlin/actr-kotlin/build.gradle.kts"',
    '".github/workflows/_actr-cli-release.yml"',
    '".github/workflows/_actrix-release.yml"',
):
    require(
        verify_workflow.count(filtered_path) == 2,
        f"release verification push and PR filters must include {filtered_path}",
    )
require(
    "published package-sync assets remain hash-locked" in preamble,
    "replace_assets input must document immutable package-sync assets",
)

print("release asset validation workflow checks passed")
PY
