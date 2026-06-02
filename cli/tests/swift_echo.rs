//! Integration tests for `actr init -l swift --template echo`
//!
//! These tests invoke the real `actr` binary and verify that the generated
//! Swift echo project contains the expected files and correct content.
//!
//! Run with: `cargo test --test swift_echo`

use std::path::PathBuf;
use std::process::{Command, Output};
use tempfile::TempDir;

fn actr_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_actr"))
}

fn run_actr(args: &[&str], cwd: &std::path::Path) -> Output {
    Command::new(actr_bin())
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("failed to run actr binary")
}

/// Initialize a Swift echo project under `parent` and return the project directory.
fn init_swift_echo(parent: &std::path::Path, project_name: &str) -> std::path::PathBuf {
    let out = run_actr(
        &[
            "init",
            "-l",
            "swift",
            "--template",
            "echo",
            "--signaling",
            "wss://actrix1.develenv.com",
            project_name,
        ],
        parent,
    );

    assert!(
        out.status.success(),
        "`actr init` failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    parent.join(project_name)
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

#[test]
#[cfg_attr(not(target_os = "macos"), ignore = "Requires macOS and xcodegen")]
fn swift_echo_init_creates_expected_files() {
    let tmp = TempDir::new().unwrap();
    let project_dir = init_swift_echo(tmp.path(), "SwiftClient");
    let app_dir = project_dir.join("SwiftClient");

    // Core project files
    assert!(
        project_dir.join("project.yml").exists(),
        "project.yml should exist"
    );
    assert!(
        project_dir.join("manifest.toml").exists(),
        "manifest.toml should exist"
    );
    assert!(
        project_dir.join("manifest.lock.toml").exists(),
        "manifest.lock.toml should exist (for xcodegen)"
    );
    assert!(
        project_dir.join("README.md").exists(),
        "README.md should exist"
    );
    assert!(
        project_dir.join(".protoc-plugin.toml").exists(),
        ".protoc-plugin.toml should exist"
    );
    assert!(
        project_dir.join("protos/local/local.proto").exists(),
        "protos/local/local.proto should exist"
    );

    // App core swift files
    assert!(
        app_dir.join("SwiftClient.swift").exists(),
        "App entry point should exist"
    );
    assert!(
        app_dir.join("ContentView.swift").exists(),
        "ContentView.swift should exist"
    );
    assert!(
        app_dir.join("ActrService.swift").exists(),
        "ActrService.swift should exist"
    );
    assert!(
        app_dir.join("Info.plist").exists(),
        "Info.plist should exist"
    );
    assert!(
        !app_dir.join("Generated").exists(),
        "Generated/ should not exist before `actr gen -l swift`"
    );

    let actr_service =
        std::fs::read_to_string(app_dir.join("ActrService.swift")).expect("read ActrService.swift");
    assert!(
        actr_service.contains("ACTR: mutable scaffold"),
        "ActrService.swift should contain the mutable scaffold marker"
    );
}

#[test]
#[cfg_attr(not(target_os = "macos"), ignore = "Requires macOS and xcodegen")]
fn swift_echo_manifest_has_signaling_url() {
    let tmp = TempDir::new().unwrap();
    let project_dir = init_swift_echo(tmp.path(), "SignalingApp");

    let actr_toml =
        std::fs::read_to_string(project_dir.join("manifest.toml")).expect("read manifest.toml");

    // The template always appends /signaling/ws; input is normalized.
    assert!(
        actr_toml.contains("wss://actrix1.develenv.com/signaling/ws"),
        "manifest.toml should contain the full signaling URL with /signaling/ws path, got:\n{actr_toml}"
    );
}

#[test]
#[cfg_attr(not(target_os = "macos"), ignore = "Requires macOS and xcodegen")]
fn swift_echo_init_fails_if_directory_exists() {
    let tmp = TempDir::new().unwrap();

    // First init succeeds
    init_swift_echo(tmp.path(), "DuplicateApp");

    // Second init into same directory should fail
    let out = run_actr(
        &[
            "init",
            "-l",
            "swift",
            "--template",
            "echo",
            "--signaling",
            "wss://actrix1.develenv.com",
            "DuplicateApp",
        ],
        tmp.path(),
    );

    assert!(
        !out.status.success(),
        "second `actr init` into existing directory should fail"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("already exists") || stderr.contains("exist"),
        "error message should mention existing directory, got:\n{stderr}"
    );
}

#[test]
#[ignore = "TODO: re-enable when AIS is implemented; also requires macOS and xcodegen"]
fn swift_echo_full_workflow_init_install_gen() {
    let tmp = TempDir::new().unwrap();
    let project_name = "EchoWorkflowApp";

    // Step 1: actr init
    let project_dir = init_swift_echo(tmp.path(), project_name);

    // Step 2: actr deps install (downloads remote proto files)
    let out = run_actr(&["deps", "install"], &project_dir);
    assert!(
        out.status.success(),
        "actr deps install failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify remote proto is downloaded
    assert!(
        project_dir
            .join("protos/remote/echo-echo-server/echo.proto")
            .exists(),
        "echo.proto should be downloaded by actr deps install"
    );

    // Step 3: actr gen -l swift
    let out = run_actr(&["gen", "-l", "swift"], &project_dir);

    // Some swift environment setup might be missing on CI/locally without xcode
    // so we don't strictly assert success here if protoc fails for swift,
    // but we will print errors. Assuming it succeeds on standard mac environment.
    assert!(
        out.status.success(),
        "actr gen failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Verify generated files
    // The swift template sets generated files in $PROJECT_NAME/Generated
    let gen_dir = project_dir.join(project_name).join("Generated");
    assert!(
        gen_dir.join("local.actor.swift").exists(),
        "actr gen should produce local.actor.swift"
    );
    assert!(
        gen_dir.join("echo.pb.swift").exists(),
        "actr gen should produce echo.pb.swift"
    );
    assert!(
        gen_dir.join("echo.client.swift").exists(),
        "actr gen should produce echo.client.swift"
    );
    assert!(
        !project_dir
            .join(project_name)
            .join("echo.pb.swift")
            .exists(),
        "generated protobuf files should not remain in the app root"
    );
    assert!(
        !project_dir
            .join(project_name)
            .join("echo.client.swift")
            .exists(),
        "generated client files should not remain in the app root"
    );
    assert!(
        project_dir
            .join(project_name)
            .join("ActrService.swift")
            .exists(),
        "ActrService.swift should remain in the app root"
    );
}
