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
CLI_TEST_SUPPORT = ROOT / "cli/src/test_support.rs"
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

    # The separate test job (not inside rust) runs cargo test for Rust changes.
    assert "  test:" in workflow
    test_job = _job(workflow, "test", "typescript")
    assert "- name: Run tests" in test_job
    assert "cargo test --workspace" in test_job


def test_rust_test_gate_restores_cache_before_installing_tools() -> None:
    workflow = CI_GATE_WORKFLOW.read_text(encoding="utf-8")
    test_job = _job(workflow, "test", "typescript")

    cache_step = "- uses: Swatinem/rust-cache@v2"
    install_step = "- name: Install prebuilt WebAssembly tools"
    assert test_job.index(cache_step) < test_job.index(install_step)
    assert "cache-targets: false" in test_job


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
    assert "we do NOT block on" in gate_job


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


def test_release_train_supports_generic_maintenance_branches() -> None:
    workflow = RELEASE_TRAIN_WORKFLOW.read_text(encoding="utf-8")
    release_script = RELEASE_TRAIN_SCRIPT.read_text(encoding="utf-8")

    assert '- "release-*"' in workflow
    assert "target_branch: ${{ steps.check.outputs.target_branch }}" in workflow
    assert "ref: main" not in workflow
    assert "--branch main" not in workflow
    assert "--latest=false" in workflow
    assert "--notes-start-tag" in workflow
    assert "needs.context.result == 'success'" in workflow

    assert "^release-([0-9]+)\\.([0-9]+)$" in release_script
    assert 'NPM_DIST_TAG="legacy-${RELEASE_LINE}"' in release_script
    assert 'ensure_versions_prepared\n  set_release_sha' in release_script


def test_swift_echoapp_e2e_job_present() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    swift_job = _job(workflow, "swift-echo-app-e2e", "python-web-e2e")

    assert "runs-on: macos-latest" in swift_job
    assert "bash e2e/swift-echo-app/run.sh" in swift_job


def test_e2e_actrix_uses_in_tree_install_instead_of_artifact_download() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    pkg_job = _job(workflow, "package-runtime-echo-e2e", "typescript-e2e")
    swift_job = _job(workflow, "swift-echo-app-e2e", "python-web-e2e")

    assert "download-actrix-artifact.sh" not in workflow
    assert "ACTR_E2E_ACTRIX_ARTIFACT" not in workflow
    assert "ACTRIX_READ_TOKEN" not in workflow
    assert "Actrium/actrix" not in workflow
    assert "actions: read" not in workflow

    assert "runs-on: ubuntu-latest" in pkg_job
    assert "runs-on: macos-latest" in swift_job


def test_e2e_no_private_actrix_checkout() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    common = (ROOT / "e2e/package-runtime-echo/lib/common.sh").read_text(encoding="utf-8")
    cli_support = CLI_TEST_SUPPORT.read_text(encoding="utf-8")

    # No private Actrix checkout git config in any job
    assert "insteadOf" not in workflow
    assert "x-access-token" not in workflow
    assert "Configure git for private Actrix checkout" not in workflow
    assert "Actrium/actrix" not in common
    assert "git clone" not in common
    assert 'actrix_repo_dir="$repo_root/actrix"' in common
    assert "Actrium/actrix" not in cli_support
    assert "DEFAULT_ACTRIX_REPO" not in cli_support
    assert "ACTR_E2E_ACTRIX_REPO" not in cli_support
    assert "ACTR_E2E_ACTRIX_REV" not in cli_support
    assert "ACTR_E2E_ACTRIX_BIN" not in cli_support
    assert "git clone actrix" not in cli_support
    assert "ACTR_E2E_ACTRIX_ARTIFACT" not in cli_support
    assert "DEFAULT_ACTRIX_ARTIFACT" not in cli_support
    assert "latest_successful_actrix_run" not in cli_support
    assert "try_ensure_actrix_artifact_binary" not in cli_support
    assert 'std::env::var("ACTRIX_BIN")' in cli_support
    assert 'workspace_root().join("actrix")' in cli_support


def test_e2e_no_inline_archive_download_url() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")

    # Inline archive_download_url + curl download replaced by shared script
    assert "archive_download_url" not in workflow


def test_e2e_linux_deps_do_not_include_unused_unzip() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    pkg_job = _job(workflow, "package-runtime-echo-e2e", "typescript-e2e")

    # actrix is installed from the in-tree source path; no artifact unzip is needed.
    assert "unzip" not in pkg_job


def test_e2e_linux_job_has_no_ios_rust_targets() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    pkg_job = _job(workflow, "package-runtime-echo-e2e", "typescript-e2e")

    assert "aarch64-apple-ios" not in pkg_job
    assert "aarch64-apple-ios-sim" not in pkg_job


def test_e2e_workspace_prebuilds_use_locked_cargo_resolution() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    ts_stream = (ROOT / "e2e/typescript-stream/run.sh").read_text(encoding="utf-8")

    assert "cargo build --release -p actr-cli --bin actr --features wasm-engine" not in workflow
    assert "cargo build --release -p actr-cli --bin actr --features wasm-engine" not in ts_stream
    assert workflow.count(
        "cargo build --locked --release -p actr-cli --bin actr --features wasm-engine"
    ) == 3
    assert workflow.count(
        "cargo build --locked --release -p actr-mock-actrix --bin mock-actrix"
    ) == 2
    assert "cargo build --locked -p actr-framework-protoc-codegen" in ts_stream
    assert 'cp "$REPO_ROOT/Cargo.lock" "$project/Cargo.lock"' in ts_stream
    assert "cargo generate-lockfile --offline" in ts_stream
    assert "cargo run --locked --features host --bin client-app" in ts_stream


def test_swift_e2e_upload_artifact_on_failure() -> None:
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    swift_jobs = {
        "swift-echo-app-e2e": "e2e/swift-echo-app/.tmp/sanitized-logs/",
        "swift-datastream-app-e2e": "e2e/swift-datastream-app/.tmp/sanitized-logs/",
        "swift-ts-workload-e2e": "e2e/swift-ts-workload/.tmp/sanitized-logs/",
    }

    for job_name, upload_path in swift_jobs.items():
        swift_job = _job(workflow, job_name, "python-web-e2e")
        # Upload step runs even on failure.
        assert "if: always()" in swift_job
        assert "actions/upload-artifact@v4" in swift_job
        assert "retention-days: 7" in swift_job
        # Upload path must match the fixed location used by cleanup trap.
        assert upload_path in swift_job

    for script_path in (
        "e2e/swift-echo-app/run.sh",
        "e2e/swift-datastream-app/run.sh",
        "e2e/swift-ts-workload/run.sh",
    ):
        run_sh = (ROOT / script_path).read_text(encoding="utf-8")
        # Cleanup must output sanitized logs to the same fixed path the workflow uploads.
        assert ".tmp/sanitized-logs" in run_sh


def test_swift_e2e_scripts_capture_app_diagnostics() -> None:
    swift_scripts = {
        "e2e/swift-echo-app/run.sh": "io.actrium.EchoApp",
        "e2e/swift-datastream-app/run.sh": "io.actrium.DataStreamApp",
        "e2e/swift-ts-workload/run.sh": "io.actrium.SwiftTsWorkloadApp",
    }

    for script_path, bundle_id in swift_scripts.items():
        run_sh = (ROOT / script_path).read_text(encoding="utf-8")

        assert f'APP_BUNDLE_ID="{bundle_id}"' in run_sh
        assert 'APP_PID=""' in run_sh
        assert "record_app_pid_from_launch_log" in run_sh
        assert 'echo "APP_PID=${APP_PID:-none}"' in run_sh
        assert 'ps -p "$APP_PID" -o pid,ppid,stat,etime,command' in run_sh
        assert 'sample "$APP_PID" 5 1 -file "$diag_dir/app.sample.txt"' in run_sh
        assert 'xcrun simctl spawn "$DEVICE_UDID" log show --last 10m --style compact' in run_sh
        assert (
            'printf \'\\n\' | xcrun simctl diagnose -b --timeout=60 --output="$diagnose_dir" --no-archive'
            in run_sh
        )
        assert '--udid="$DEVICE_UDID"' in run_sh
        assert "Library/Logs/DiagnosticReports" in run_sh
        assert 'get_app_container "$DEVICE_UDID" "$APP_BUNDLE_ID" data' in run_sh
        assert run_sh.count("fail_if_app_exited_before_result") >= 2
        assert 'find "$src_dir" -type f' in run_sh


def test_swift_e2e_uploads_debug_symbols_separately() -> None:
    debug_symbol_settings = (
        "configs:\n"
        "    Debug:\n"
        "      DEBUG_INFORMATION_FORMAT: dwarf-with-dsym\n"
        "      GCC_GENERATE_DEBUGGING_SYMBOLS: YES\n"
        "      COPY_PHASE_STRIP: NO\n"
        "      STRIP_INSTALLED_PRODUCT: NO"
    )

    for project_path in (
        "e2e/swift-echo-app/project.yml",
        "e2e/swift-datastream-app/project.yml",
    ):
        project_yml = (ROOT / project_path).read_text(encoding="utf-8")
        assert debug_symbol_settings in project_yml

    for script_path in (
        "e2e/swift-echo-app/run.sh",
        "e2e/swift-datastream-app/run.sh",
        "e2e/swift-ts-workload/run.sh",
    ):
        run_sh = (ROOT / script_path).read_text(encoding="utf-8")
        assert 'SYMBOL_DIR="$RUN_DIR/symbols"' in run_sh
        assert 'APP_DSYM="${APP_PATH}.dSYM"' in run_sh
        assert "DEBUG_INFORMATION_FORMAT=dwarf-with-dsym" in run_sh
        assert "GCC_GENERATE_DEBUGGING_SYMBOLS=YES" in run_sh
        assert "COPY_PHASE_STRIP=NO" in run_sh
        assert "STRIP_INSTALLED_PRODUCT=NO" in run_sh
        assert 'xcrun dwarfdump --uuid "$APP_DSYM" >"$SYMBOL_DIR/uuids.txt"' in run_sh
        assert 'cp -R "$APP_DSYM" "$SYMBOL_DIR/"' in run_sh
        assert 'find "$products_dir" -type d -name "*.dSYM"' in run_sh
        assert 'for app_binary in "$APP_BINARY" "$APP_PATH"/*.debug.dylib' in run_sh
        assert (
            'cp "$app_binary" "$SYMBOL_DIR/${APP_PROCESS_NAME}.app/"'
            in run_sh
        )
        assert 'local symbol_upload_dir="$SCRIPT_DIR/.tmp/symbols"' in run_sh
        assert 'mv "$SYMBOL_DIR" "$symbol_upload_dir"' in run_sh
        assert 'sanitize_logs_for_upload "$SYMBOL_DIR"' not in run_sh
        assert 'collect_app_symbols "$derived_data/Build/Products"' in run_sh
        assert 'CAPTURE_CRASH_BACKTRACE:-0' in run_sh
        assert 'launch_args+=(--wait-for-debugger)' in run_sh
        assert "start_lldb_crash_capture" in run_sh
        assert '--attach-pid "$APP_PID"' in run_sh
        assert "settings set target.debug-file-search-paths" in run_sh
        assert "target symbols add" not in run_sh
        assert '-k "thread backtrace all"' in run_sh
        assert '-k "register read"' in run_sh
        assert '-k "image list -o -f"' in run_sh
        assert '"$LOG_DIR/app.lldb.log"' in run_sh
        assert "wait_for_lldb_capture" in run_sh

    for script_path in (
        "e2e/swift-datastream-app/run.sh",
        "e2e/swift-ts-workload/run.sh",
    ):
        run_sh = (ROOT / script_path).read_text(encoding="utf-8")
        assert debug_symbol_settings in run_sh

    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    swift_jobs = (
        (
            "swift-echo-app-e2e",
            "swift-datastream-app-e2e",
            "e2e/swift-echo-app/.tmp/symbols/",
        ),
        (
            "swift-datastream-app-e2e",
            "swift-ts-workload-e2e",
            "e2e/swift-datastream-app/.tmp/symbols/",
        ),
        (
            "swift-ts-workload-e2e",
            "python-web-e2e",
            "e2e/swift-ts-workload/.tmp/symbols/",
        ),
    )
    for job_name, next_job_name, symbol_path in swift_jobs:
        swift_job = _job(workflow, job_name, next_job_name)
        assert symbol_path in swift_job
        assert "-symbols-${{ github.run_id }}-${{ github.run_attempt }}" in swift_job
        assert swift_job.count("actions/upload-artifact@v4") == 2
        assert "DerivedData" not in "\n".join(
            line
            for line in swift_job.splitlines()
            if "path:" in line and ".tmp/" in line
        )


def test_e2e_no_call_remote_in_ffi() -> None:
    ffi_runtime = (ROOT / "bindings/ffi/src/runtime.rs").read_text(encoding="utf-8")
    swift_actr_ref = (ROOT / "bindings/swift/Sources/Actr/ActrRef.swift").read_text(encoding="utf-8")

    # callRemote was an exploratory API, removed to keep the established
    # local-workload-forwarding path
    assert "call_remote" not in ffi_runtime
    assert "callRemote" not in swift_actr_ref


def test_no_actrix_release_train_artifact_download_script() -> None:
    assert not (ROOT / ".github/scripts/download-actrix-artifact.sh").exists()


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


def test_nightly_e2e_includes_swift_datastream() -> None:
    """Verify ci-e2e.yml contains the swift-datastream-app-e2e job."""
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    assert "swift-datastream-app-e2e:" in workflow


def test_swift_datastream_job_runs_on_macos() -> None:
    """Verify the datastream job uses runs-on: macos-latest."""
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    ds_job = _job(workflow, "swift-datastream-app-e2e", "python-web-e2e")
    assert "runs-on: macos-latest" in ds_job


def test_swift_datastream_job_does_not_download_actrix_artifact() -> None:
    """Verify the datastream job builds actrix from the in-tree source path."""
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    ds_job = _job(workflow, "swift-datastream-app-e2e", "python-web-e2e")
    assert "download-actrix-artifact.sh" not in ds_job


def test_swift_datastream_job_builds_xcframework() -> None:
    """Verify the datastream job builds the Swift XCFramework."""
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    ds_job = _job(workflow, "swift-datastream-app-e2e", "python-web-e2e")
    assert "build-xcframework.sh" in ds_job


def test_swift_datastream_timeout_not_less_than_240() -> None:
    """Verify the datastream job has timeout-minutes >= 240."""
    workflow = CI_E2E_WORKFLOW.read_text(encoding="utf-8")
    ds_job = _job(workflow, "swift-datastream-app-e2e", "python-web-e2e")
    import re

    match = re.search(r"timeout-minutes:\s*(\d+)", ds_job)
    assert match is not None, "timeout-minutes not found in swift-datastream-app-e2e job"
    timeout = int(match.group(1))
    assert timeout >= 240, f"timeout-minutes is {timeout}, expected >= 240"


def test_swift_datastream_job_not_in_pr_gate() -> None:
    """Verify ci-gate.yml does NOT contain swift-datastream-app-e2e."""
    workflow = CI_GATE_WORKFLOW.read_text(encoding="utf-8")
    assert "swift-datastream-app-e2e" not in workflow


def test_swift_datastream_cleanup_only_removes_owned_simulator() -> None:
    """Verify local cleanup cannot shut down unrelated iOS Simulators."""
    run_sh = (ROOT / "e2e/swift-datastream-app/run.sh").read_text(encoding="utf-8")

    assert "xcrun simctl shutdown all" not in run_sh
    assert 'DEVICE_CREATED="0"' in run_sh
    assert 'DEVICE_CREATED="1"' in run_sh
    assert 'if [ "$DEVICE_CREATED" = "1" ]' in run_sh
    assert 'xcrun simctl shutdown "$DEVICE_UDID"' in run_sh
    assert 'xcrun simctl delete "$DEVICE_UDID"' in run_sh


def test_datastream_cli_templates_use_empty_scaffolds() -> None:
    """Verify Rust/Swift DataStream init starts from empty templates only."""
    rust_mod = (ROOT / "cli/src/templates/rust/mod.rs").read_text(encoding="utf-8")
    swift_mod = (ROOT / "cli/src/templates/swift/mod.rs").read_text(encoding="utf-8")

    assert "pub mod data_stream;" not in rust_mod
    assert "pub mod data_stream;" not in swift_mod
    assert "ProjectTemplateName::Empty" in rust_mod
    assert "ProjectTemplateName::Empty" in swift_mod
    assert "ProjectTemplateName::DataStream" in rust_mod
    assert "ProjectTemplateName::DataStream" in swift_mod
    assert rust_mod.count("empty::load(&mut files)?;") >= 2
    assert swift_mod.count("empty::load(&mut files)?;") >= 2
    assert not (ROOT / "cli/src/templates/rust/data_stream.rs").exists()
    assert not (ROOT / "cli/src/templates/swift/data_stream.rs").exists()
    assert not any((ROOT / "cli/fixtures/rust/data-stream").glob("*"))
    assert not any((ROOT / "cli/fixtures/swift/data-stream").glob("*"))


def test_swift_datastream_e2e_builds_custom_proto_flow_from_empty() -> None:
    """Verify DataStreamApp E2E creates custom proto/service after empty init."""
    run_sh = (ROOT / "e2e/swift-datastream-app/run.sh").read_text(encoding="utf-8")
    actr_toml = (ROOT / "e2e/swift-datastream-app/actr.toml.tpl").read_text(
        encoding="utf-8"
    )
    actr_service = (
        ROOT / "e2e/swift-datastream-app/DataStreamApp/Services/ActrService.swift"
    ).read_text(encoding="utf-8")

    assert "--template empty" in run_sh
    assert 'rm -f "$TMP_SERVICE_DIR/protos/local/local.proto"' in run_sh
    assert 'rm -f "$TMP_APP_DIR/protos/local/local.proto"' in run_sh
    assert 'write_duplex_stream_proto "$TMP_SERVICE_DIR/protos/local/duplex_stream.proto"' in run_sh
    assert 'write_probe_proto "$TMP_APP_DIR/protos/local/probe.proto"' in run_sh
    assert 'write_duplex_stream_proto "$TMP_APP_DIR/protos/remote' not in run_sh
    assert "use crate::generated::local::{" in run_sh
    assert "use crate::generated::duplex_stream::{" not in run_sh
    assert "run_actr gen -l rust" in run_sh
    assert "run_actr gen -l swift" in run_sh
    assert "rm -f DataStreamApp/ActrService.swift" in run_sh
    assert "SIMCTL_CHILD_ACTR_DATASTREAMAPP_AUTO_STREAM_COUNT=3" in run_sh
    assert "ACTR_E2E_RESULT:3/3" in run_sh
    main_flow = run_sh.split("# ──── Main ────", 1)[1]
    assert main_flow.index("run_server_host") < main_flow.index("build_datastream_app")
    assert main_flow.index("check_service_ready") < main_flow.index("build_datastream_app")
    assert "__MANUFACTURER__:DuplexStreamService:1.0.0" in actr_toml
    assert 'ActrType(manufacturer: manufacturer, name: "DataStreamApp", version: "0.1.0")' in actr_service
    assert 'let expectedLines = (1...count).map { "received: echo: hello \\($0)" }' in actr_service


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
    test_rust_test_gate_restores_cache_before_installing_tools()
    test_pr_gate_excludes_heavy_root_e2e_jobs()
    test_scheduled_e2e_runs_root_level_browser_and_stream_e2e()
    test_pr_gate_swift_uses_macos_only_xcframework()
    test_release_train_has_valid_publish_steps()
    test_release_train_verifies_ci_gate_triggered()
    test_release_train_forwards_release_context()
    test_release_train_supports_generic_maintenance_branches()
    test_swift_echoapp_e2e_job_present()
    test_e2e_actrix_uses_in_tree_install_instead_of_artifact_download()
    test_e2e_no_private_actrix_checkout()
    test_e2e_no_inline_archive_download_url()
    test_e2e_linux_deps_do_not_include_unused_unzip()
    test_e2e_linux_job_has_no_ios_rust_targets()
    test_e2e_workspace_prebuilds_use_locked_cargo_resolution()
    test_swift_e2e_upload_artifact_on_failure()
    test_swift_e2e_scripts_capture_app_diagnostics()
    test_swift_e2e_uploads_debug_symbols_separately()
    test_e2e_no_call_remote_in_ffi()
    test_no_actrix_release_train_artifact_download_script()
    test_run_sh_uses_correct_signaling_cache_table()
    test_service_readiness_rejects_missing_or_unrelated_registration()
    test_service_readiness_waits_for_exact_registration()
    test_nightly_e2e_includes_swift_datastream()
    test_swift_datastream_job_runs_on_macos()
    test_swift_datastream_job_does_not_download_actrix_artifact()
    test_swift_datastream_job_builds_xcframework()
    test_swift_datastream_timeout_not_less_than_240()
    test_swift_datastream_job_not_in_pr_gate()
    test_swift_datastream_cleanup_only_removes_owned_simulator()
    test_datastream_cli_templates_use_empty_scaffolds()
    test_swift_datastream_e2e_builds_custom_proto_flow_from_empty()
