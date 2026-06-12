#!/usr/bin/env python3

import sqlite3
import subprocess
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CI_GATE_WORKFLOW = ROOT / ".github/workflows/ci-gate.yml"
CI_E2E_WORKFLOW = ROOT / ".github/workflows/ci-e2e.yml"
RELEASE_TRAIN_WORKFLOW = ROOT / ".github/workflows/release-train.yml"
RELEASE_TRAIN_SCRIPT = ROOT / "scripts/release-train.sh"
SWIFT_E2E_READINESS = ROOT / "e2e/swift-echo-app/lib/readiness.sh"


def _job(workflow: str, name: str, next_name: str) -> str:
    job_start = workflow.index(f"  {name}:\n")
    next_job_start = workflow.index(f"\n  {next_name}:\n", job_start)
    return workflow[job_start:next_job_start]


def test_rust_gate_avoids_slow_workspace_tests_and_unused_prewarm() -> None:
    workflow = CI_GATE_WORKFLOW.read_text(encoding="utf-8")
    rust_job = _job(workflow, "rust", "test")

    assert "- name: Run tests" not in rust_job
    assert "cargo test --workspace" not in rust_job
    assert "- name: Prepare Rust codegen plugins" not in rust_job
    assert "cargo install protoc-gen-prost" not in rust_job
    assert "cargo build --quiet -p actr-framework-protoc-codegen" not in rust_job
    assert "- name: Build workspace" not in rust_job
    assert "cargo build --verbose --all-features" not in rust_job
    assert "- name: Build release" not in rust_job
    assert "cargo build --release --verbose --all-features" not in rust_job

    # The separate test job (not inside rust) runs cargo test on push to main
    assert "  test:" in workflow
    test_job = _job(workflow, "test", "typescript")
    assert "- name: Run tests" in test_job
    assert "cargo test --workspace" in test_job


def test_pr_gate_excludes_heavy_root_e2e_jobs() -> None:
    workflow = CI_GATE_WORKFLOW.read_text(encoding="utf-8")

    assert "  ts_stream_e2e:\n" not in workflow
    assert "  web_browser_e2e:\n" not in workflow
    assert "ts_stream_e2e" not in workflow
    assert "web_browser_e2e" not in workflow


def test_scheduled_e2e_runs_root_level_browser_and_stream_e2e() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")

    assert "typescript-e2e:" in workflow
    assert "web-browser-e2e:" in workflow
    assert "bash e2e/typescript-stream/run.sh" in workflow
    assert "bash e2e/web-browser/run.sh" in workflow


def test_pr_gate_swift_uses_macos_only_xcframework() -> None:
    workflow = CI_GATE_WORKFLOW.read_text(encoding="utf-8")
    swift_job = _job(workflow, "swift", "kotlin")

    assert "ACTR_XCFRAMEWORK_TARGETS: macos" in swift_job
    assert "targets: aarch64-apple-darwin" in swift_job
    assert "targets: aarch64-apple-ios,aarch64-apple-ios-sim,aarch64-apple-darwin" not in swift_job


def test_release_train_has_valid_publish_steps() -> None:
    raw_workflow = RELEASE_TRAIN_WORKFLOW.read_bytes()
    assert all(byte >= 0x20 or byte in b"\n\r\t" for byte in raw_workflow)

    workflow = raw_workflow.decode("utf-8")
    for stage in (
        "publish-rust",
        "publish-python",
        "publish-swift",
        "publish-kotlin",
        "publish-web",
        "publish-typescript-workload",
        "publish-typescript",
    ):
        assert f"- name: Run {stage} stage" in workflow


def test_release_train_verifies_ci_gate_triggered() -> None:
    workflow = RELEASE_TRAIN_WORKFLOW.read_text(encoding="utf-8")
    gate_job = _job(workflow, "gate", "context")

    assert "actions: read" in workflow
    assert "- name: Verify CI Gate is triggered" in gate_job
    assert "actions/workflows/ci-gate.yml/runs" in gate_job
    assert "head_sha=${RELEASE_SHA}" in gate_job


def test_release_train_forwards_release_context() -> None:
    workflow = RELEASE_TRAIN_WORKFLOW.read_text(encoding="utf-8")
    release_script = RELEASE_TRAIN_SCRIPT.read_text(encoding="utf-8")
    create_tag_job = _job(workflow, "create-tag", "publish-rust")

    assert "EXPECTED_RELEASE_SHA" in create_tag_job
    assert 'needs.context.outputs.release_sha }}' in create_tag_job

    stage_jobs = (
        ("create-tag", "publish-rust"),
        ("publish-rust", "publish-python"),
        ("publish-python", "publish-swift"),
        ("publish-swift", "publish-kotlin"),
        ("publish-kotlin", "publish-web"),
        ("publish-web", "build-typescript-native"),
        ("publish-typescript-workload", "publish-typescript"),
        ("publish-typescript", "collect-report"),
    )
    for job, next_job in stage_jobs:
        job_workflow = _job(workflow, job, next_job)
        assert 'needs.context.outputs.skip_python }}" == "true"' in job_workflow
        assert "args+=(--skip-python)" in job_workflow
        assert 'needs.context.outputs.pre_release }}" == "true"' in job_workflow
        assert "args+=(--pre-release)" in job_workflow

    report_job = workflow[workflow.index("  collect-report:\n") :]
    assert 'needs.context.outputs.skip_python }}" == "true"' in report_job
    assert "args+=(--skip-python)" in report_job
    assert 'needs.context.outputs.pre_release }}" == "true"' in report_job
    assert "args+=(--pre-release)" in report_job

    assert '[[ "$STAGE" == "report" ]]' in release_script
    assert 'current_sha=$(git rev-parse HEAD)' in release_script
    assert 'Release context SHA ${RELEASE_SHA} does not match current HEAD ${current_sha}' in release_script


def test_swift_echoapp_e2e_job_present() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    swift_job = _job(workflow, "swift-echo-app-e2e", "python-web-e2e")

    assert "runs-on: macos-latest" in swift_job
    assert "bash e2e/swift-echo-app/run.sh" in swift_job


def test_e2e_actrix_artifact_download_uses_shared_script() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    pkg_job = _job(workflow, "package-runtime-echo-e2e", "typescript-e2e")
    swift_job = _job(workflow, "swift-echo-app-e2e", "python-web-e2e")

    # Both jobs call the shared script with arch-appropriate artifact names
    assert "bash .github/scripts/download-actrix-artifact.sh actrix-linux-x86_64" in pkg_job
    assert "bash .github/scripts/download-actrix-artifact.sh actrix-macos-arm64" in swift_job

    # Linux runner gets x86_64, macOS runner gets arm64
    assert "runs-on: ubuntu-latest" in pkg_job
    assert "actrix-linux-x86_64" in pkg_job
    assert "runs-on: macos-latest" in swift_job
    assert "actrix-macos-arm64" in swift_job


def test_e2e_no_private_actrix_checkout() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")

    # No private Actrix checkout git config in any job
    assert "insteadOf" not in workflow
    assert "x-access-token" not in workflow
    assert "Configure git for private Actrix checkout" not in workflow


def test_e2e_no_inline_archive_download_url() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")

    # Inline archive_download_url + curl download replaced by shared script
    assert "archive_download_url" not in workflow


def test_e2e_linux_deps_includes_unzip() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    pkg_job = _job(workflow, "package-runtime-echo-e2e", "typescript-e2e")

    # unzip is required by the download script on Linux
    assert "unzip" in pkg_job


def test_e2e_linux_job_has_no_ios_rust_targets() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    pkg_job = _job(workflow, "package-runtime-echo-e2e", "typescript-e2e")

    assert "aarch64-apple-ios" not in pkg_job
    assert "aarch64-apple-ios-sim" not in pkg_job


def test_swift_e2e_upload_artifact_on_failure() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    swift_job = _job(workflow, "swift-echo-app-e2e", "python-web-e2e")

    # Upload step runs even on failure
    assert "if: always()" in swift_job
    assert "actions/upload-artifact@v4" in swift_job
    assert "retention-days: 7" in swift_job
    # Upload path must match the fixed location used by cleanup trap
    assert "e2e/swift-echo-app/.tmp/sanitized-logs/" in swift_job

    run_sh = (ROOT / "e2e/swift-echo-app/run.sh").read_text(encoding="utf-8")
    # Cleanup must output sanitized logs to the same fixed path the workflow uploads
    assert '.tmp/sanitized-logs' in run_sh


def test_e2e_no_call_remote_in_ffi() -> None:
    ffi_runtime = (ROOT / "bindings/ffi/src/runtime.rs").read_text(encoding="utf-8")
    swift_actr_ref = (ROOT / "bindings/swift/Sources/Actr/ActrRef.swift").read_text(encoding="utf-8")

    # callRemote was an exploratory API, removed to keep the established
    # local-workload-forwarding path
    assert "call_remote" not in ffi_runtime
    assert "callRemote" not in swift_actr_ref


def test_download_script_explicit_empty_runs_check() -> None:
    script = (ROOT / ".github/scripts/download-actrix-artifact.sh").read_text(encoding="utf-8")

    # P2 fix: must not use jq -er on .workflow_runs[0] without an explicit
    # empty-list guard. The script must check for empty RUN_ID and call fail
    # with a clear message, not let jq exit silently.
    assert 'No successful actrix CI run found' in script
    assert 'jq -r \'.workflow_runs[0].id // ""\'' in script
    # Must NOT use the old pattern that exits silently on empty list
    assert 'jq -er \'.workflow_runs[0].id\'' not in script


def test_run_sh_uses_correct_signaling_cache_table() -> None:
    run_sh = (ROOT / "e2e/swift-echo-app/run.sh").read_text(encoding="utf-8")

    # Must query the actual table name (service_registry, not service_registrations)
    assert "service_registry" in run_sh
    assert "service_registrations" not in run_sh
    assert "actor_registry" not in run_sh


def _create_service_registry(db_path: Path) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE service_registry (
                actor_realm_id INTEGER NOT NULL,
                actor_manufacturer TEXT NOT NULL,
                actor_device_name TEXT NOT NULL,
                service_name TEXT NOT NULL,
                status TEXT NOT NULL,
                last_heartbeat_at INTEGER NOT NULL
            )
            """
        )


def _run_service_readiness(db_path: Path, timeout: str = "0") -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "bash",
            "-c",
            'source "$1"; wait_for_service_registration "$2" 1001 actrium EchoService "$3"',
            "bash",
            str(SWIFT_E2E_READINESS),
            str(db_path),
            timeout,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def test_service_readiness_rejects_missing_or_unrelated_registration() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "signaling_cache.db"
        _create_service_registry(db_path)

        missing = _run_service_readiness(db_path)
        assert missing.returncode != 0

        with sqlite3.connect(db_path) as connection:
            connection.execute(
                """
                INSERT INTO service_registry
                    (actor_realm_id, actor_manufacturer, actor_device_name,
                     service_name, status, last_heartbeat_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (1001, "actrium", "OtherService", "actrium:OtherService", "Available", 1),
            )

        unrelated = _run_service_readiness(db_path)
        assert unrelated.returncode != 0


def test_service_readiness_waits_for_exact_registration() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "signaling_cache.db"
        _create_service_registry(db_path)

        script = """
            source "$1"
            (
                sleep 0.2
                sqlite3 "$2" \
                    "INSERT INTO service_registry VALUES \
                    (1001, 'actrium', 'EchoService', 'actrium:EchoService', 'Available', 1);"
            ) &
            SERVICE_READY_POLL_INTERVAL_SECONDS=0.1 \
                wait_for_service_registration "$2" 1001 actrium EchoService 2
        """
        result = subprocess.run(
            ["bash", "-c", script, "bash", str(SWIFT_E2E_READINESS), str(db_path)],
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr


if __name__ == "__main__":
    test_rust_gate_avoids_slow_workspace_tests_and_unused_prewarm()
    test_pr_gate_excludes_heavy_root_e2e_jobs()
    test_scheduled_e2e_runs_root_level_browser_and_stream_e2e()
    test_pr_gate_swift_uses_macos_only_xcframework()
    test_release_train_has_valid_publish_steps()
    test_release_train_verifies_ci_gate_triggered()
    test_release_train_forwards_release_context()
    test_swift_echoapp_e2e_job_present()
    test_e2e_actrix_artifact_download_uses_shared_script()
    test_e2e_no_private_actrix_checkout()
    test_e2e_no_inline_archive_download_url()
    test_e2e_linux_deps_includes_unzip()
    test_e2e_linux_job_has_no_ios_rust_targets()
    test_swift_e2e_upload_artifact_on_failure()
    test_e2e_no_call_remote_in_ffi()
    test_download_script_explicit_empty_runs_check()
    test_run_sh_uses_correct_signaling_cache_table()
    test_service_readiness_rejects_missing_or_unrelated_registration()
    test_service_readiness_waits_for_exact_registration()
