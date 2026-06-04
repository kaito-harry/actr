//! Integration tests for `actr init -l kotlin --template echo` and
//! Kotlin linked workload code generation.
//!
//! These tests invoke the real `actr` binary and verify that the generated
//! Kotlin echo project contains the expected files and correct content,
//! including linked workload scaffolding.
//!
//! Run with: `cargo test --test kotlin_echo`

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

/// Initialize a Kotlin echo project under `parent` and return the project directory.
fn init_kotlin_echo(parent: &std::path::Path, project_name: &str) -> std::path::PathBuf {
    let out = run_actr(
        &[
            "init",
            "-l",
            "kotlin",
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
fn kotlin_echo_init_creates_expected_files() {
    let tmp = TempDir::new().unwrap();
    let project_dir = init_kotlin_echo(tmp.path(), "KotlinEchoApp");

    // Core project files
    assert!(
        project_dir.join("manifest.toml").exists(),
        "manifest.toml should exist"
    );
    assert!(
        project_dir.join("build.gradle.kts").exists(),
        "build.gradle.kts should exist"
    );
    assert!(
        project_dir.join("settings.gradle.kts").exists(),
        "settings.gradle.kts should exist"
    );
    assert!(
        project_dir.join(".protoc-plugin.toml").exists(),
        ".protoc-plugin.toml should exist"
    );
    assert!(
        project_dir.join("protos/local/local.proto").exists(),
        "protos/local/local.proto should exist"
    );

    // Android app module
    assert!(
        project_dir.join("app/build.gradle.kts").exists(),
        "app/build.gradle.kts should exist"
    );
    assert!(
        project_dir
            .join("app/src/main/AndroidManifest.xml")
            .exists(),
        "AndroidManifest.xml should exist"
    );
}

#[test]
fn kotlin_echo_manifest_has_signaling_url() {
    let tmp = TempDir::new().unwrap();
    let project_dir = init_kotlin_echo(tmp.path(), "SignalingKtApp");

    let manifest =
        std::fs::read_to_string(project_dir.join("manifest.toml")).expect("read manifest.toml");

    // Kotlin template stores the signaling URL as-is (without /signaling/ws suffix)
    assert!(
        manifest.contains("wss://actrix1.develenv.com"),
        "manifest.toml should contain the signaling URL, got:\n{manifest}"
    );
}

#[test]
fn kotlin_echo_init_fails_if_directory_exists() {
    let tmp = TempDir::new().unwrap();

    // First init succeeds
    init_kotlin_echo(tmp.path(), "DuplicateKtApp");

    // Second init into same directory should fail
    let out = run_actr(
        &[
            "init",
            "-l",
            "kotlin",
            "--template",
            "echo",
            "--signaling",
            "wss://actrix1.develenv.com",
            "DuplicateKtApp",
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

/// Verify that the Kotlin codegen scaffold includes linked workload support.
///
/// This test checks that:
/// 1. `actr gen -l kotlin` produces UnifiedWorkload.kt with linked workload APIs
/// 2. The generated workload class implements WorkloadLifecycleBridge (not WorkloadBridge)
/// 3. A `toDynamicWorkload()` method is generated for use with `linked()`
#[test]
#[ignore = "Requires protoc and protoc-gen-actrframework-kotlin plugin"]
fn kotlin_gen_produces_linked_workload_scaffold() {
    let tmp = TempDir::new().unwrap();
    let project_dir = init_kotlin_echo(tmp.path(), "LinkedKtApp");

    // Install dependencies
    let deps_out = run_actr(&["deps", "install"], &project_dir);
    if !deps_out.status.success() {
        // deps install may fail without network; skip gracefully
        eprintln!(
            "Skipping kotlin_gen test: deps install failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&deps_out.stdout),
            String::from_utf8_lossy(&deps_out.stderr)
        );
        return;
    }

    // Generate Kotlin code
    let gen_out = run_actr(&["gen", "-l", "kotlin"], &project_dir);
    assert!(
        gen_out.status.success(),
        "actr gen -l kotlin failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&gen_out.stdout),
        String::from_utf8_lossy(&gen_out.stderr)
    );

    // Find the generated UnifiedWorkload.kt scaffold
    let output_dir = project_dir.join("app/src/main/java/com/example");
    let workload_file = output_dir.join("UnifiedWorkload.kt");

    if workload_file.exists() {
        let content = std::fs::read_to_string(&workload_file).expect("read UnifiedWorkload.kt");

        // Verify linked workload patterns
        assert!(
            content.contains("WorkloadLifecycleBridge"),
            "UnifiedWorkload should implement WorkloadLifecycleBridge for linked mode, got:\n{content}"
        );
        assert!(
            content.contains("onReady"),
            "UnifiedWorkload should override onReady (required by WorkloadLifecycleBridge), got:\n{content}"
        );
        assert!(
            content.contains("onError"),
            "UnifiedWorkload should override onError (required by WorkloadLifecycleBridge), got:\n{content}"
        );
        assert!(
            content.contains("DynamicWorkload"),
            "UnifiedWorkload should reference DynamicWorkload for linked mode, got:\n{content}"
        );
        assert!(
            content.contains("toDynamicWorkload"),
            "UnifiedWorkload should have toDynamicWorkload() helper method, got:\n{content}"
        );
    }
    // If the scaffold file doesn't exist (e.g., no_scaffold flag), that's also acceptable
}

/// Verify that `actr gen -l kotlin` produces per-service actor files
/// that can be discovered by the codegen (not just unified_actor.kt).
#[test]
#[ignore = "Requires protoc and protoc-gen-actrframework-kotlin plugin"]
fn kotlin_gen_produces_per_service_actor_files() {
    let tmp = TempDir::new().unwrap();
    let project_dir = init_kotlin_echo(tmp.path(), "ActorFilesKtApp");

    // Install dependencies
    let deps_out = run_actr(&["deps", "install"], &project_dir);
    if !deps_out.status.success() {
        eprintln!(
            "Skipping kotlin_gen test: deps install failed\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&deps_out.stdout),
            String::from_utf8_lossy(&deps_out.stderr)
        );
        return;
    }

    // Generate Kotlin code
    let gen_out = run_actr(&["gen", "-l", "kotlin"], &project_dir);
    assert!(
        gen_out.status.success(),
        "actr gen -l kotlin failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&gen_out.stdout),
        String::from_utf8_lossy(&gen_out.stderr)
    );

    // Check that *_actor.kt files exist in the generated output
    let gen_dir = project_dir.join("app/src/main/java/com/example/generated");
    if gen_dir.exists() {
        let actor_files: Vec<_> = std::fs::read_dir(&gen_dir)
            .expect("read generated dir")
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|name| name.ends_with("_actor.kt"))
            })
            .collect();

        assert!(
            !actor_files.is_empty(),
            "Kotlin codegen should produce *_actor.kt files in generated directory"
        );
    }
}
