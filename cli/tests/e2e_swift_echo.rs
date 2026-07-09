//! End-to-end tests for Swift echo template.
//!
//! These tests run against a local Actrix instance and local Swift projects only.
//! Run with: `cargo test --test e2e_swift_echo -- --ignored --test-threads=1`

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use actr_cli::test_support::{
    LocalRustEchoService, MockSignaling, align_project_with_local_actrix, assert_success,
    ensure_local_swift_xcframework, pin_echo_service_dependency_version, run_actr,
};
use tempfile::TempDir;
use toml::Value;

fn append_cli_target(project_dir: &std::path::Path, project_name: &str, target_name: &str) {
    let project_yml = project_dir.join("project.yml");
    let mut yml = std::fs::read_to_string(&project_yml).expect("read project.yml");
    let cli_target = format!(
        r#"
  {target_name}:
    type: tool
    platform: macOS
    deploymentTarget: "13.0"
    sources:
      - path: {target_name}
      - path: {project_name}/Generated
    settings:
      SWIFT_VERSION: "6.0"
    dependencies:
      - package: actr-swift
        product: Actr
      - package: swift-protobuf
        product: SwiftProtobuf
"#
    );
    yml.push_str(&cli_target);
    std::fs::write(&project_yml, yml).expect("write project.yml");
    std::fs::create_dir_all(project_dir.join(target_name)).expect("create CLI target dir");
}

fn write_service_cli(project_dir: &std::path::Path) {
    let source = r#"import Actr
import Foundation
import SwiftProtobuf

public final class EchoServiceHandlerImpl: EchoServiceHandler {
    public init() {}

    public func echo(req: Echo_EchoRequest, ctx _: any ActrContext) async throws(ActrError) -> Echo_EchoResponse {
        print("Received echo request: \(req.message)")
        var response = Echo_EchoResponse()
        response.reply = req.message
        return response
    }
}

private final class EchoServiceLifecycleAdapter: Workload, @unchecked Sendable {
    private let workload = EchoServiceWorkload(handler: EchoServiceHandlerImpl())

    func onStart(ctx _: Context) async throws {}

    func onReady(ctx _: Context) async throws {}

    func onStop(ctx _: Context) async throws {}

    func onError(ctx _: Context, event _: ErrorEvent) async throws {}

    func dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data {
        return try await workload.__dispatch(ctx: ctx, envelope: envelope)
    }
}

@main
struct EchoServiceCLI {
    static func main() async throws {
        let cwd = FileManager.default.currentDirectoryPath
        let configPath = (cwd as NSString).appendingPathComponent("actr.e2e.toml")
        let actorType = ActrType(
            manufacturer: "acme",
            name: "EchoService",
            version: "1.0.0"
        )
        let workload = dynamicWorkload(lifecycle: EchoServiceLifecycleAdapter())

        let system = try await ActrNode.linked(
            config: configPath,
            type: actorType,
            workload: workload
        )
        let _ = try await system.start()
        print("EchoService registered")

        while true {
            try await Task.sleep(nanoseconds: 1_000_000_000)
        }
    }
}
"#;
    std::fs::write(
        project_dir.join("EchoServiceCLI/EchoServiceCLI.swift"),
        source,
    )
    .expect("write EchoServiceCLI.swift");
}

fn write_app_cli(project_dir: &std::path::Path) {
    let source = r#"import Actr
import Foundation
import SwiftProtobuf

private final class EchoAppLifecycleAdapter: Workload, @unchecked Sendable {
    private let workload = EchoAppWorkload()

    func onStart(ctx _: Context) async throws {}

    func onReady(ctx _: Context) async throws {}

    func onStop(ctx _: Context) async throws {}

    func onError(ctx _: Context, event: ErrorEvent) async throws {
        print("EchoAppLifecycleAdapter error: \(event)")
    }

    func dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data {
        return try await workload.__dispatch(ctx: ctx, envelope: envelope)
    }
}

@main
struct EchoAppCLI {
    static func main() async throws {
        let cwd = FileManager.default.currentDirectoryPath
        let configPath = (cwd as NSString).appendingPathComponent("actr.e2e.toml")
        let actorType = ActrType(
            manufacturer: "acme",
            name: "EchoApp",
            version: "1.0.0"
        )
        let workload = dynamicWorkload(lifecycle: EchoAppLifecycleAdapter())
        let system = try await ActrNode.linked(config: configPath, type: actorType, workload: workload)
        let actr = try await system.start()

        var request = Echo_EchoRequest()
        request.message = "hello"

        let response: Echo_EchoResponse = try await actr.call(request)
        print("Echo reply: \(response.reply)")

        await actr.stop()
    }
}
"#;
    std::fs::write(project_dir.join("EchoAppCLI/EchoAppCLI.swift"), source)
        .expect("write EchoAppCLI.swift");
}

fn write_linked_runtime_config(project_dir: &Path) {
    let manifest_path = project_dir.join("manifest.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("read manifest.toml");
    let parsed: Value = toml::from_str(&manifest).expect("parse manifest.toml");

    let signaling_url = parsed["system"]["signaling"]["url"]
        .as_str()
        .expect("manifest system.signaling.url")
        .to_string();
    let ais_url = parsed["system"]["ais_endpoint"]["url"]
        .as_str()
        .expect("manifest system.ais_endpoint.url")
        .to_string();
    let realm_id = parsed["system"]["deployment"]["realm_id"]
        .as_integer()
        .expect("manifest system.deployment.realm_id");
    let runtime_root = project_dir.join(".actr-e2e-runtime");
    let data_dir = runtime_root.join("hyper");
    fs::create_dir_all(&data_dir).expect("create linked runtime data dir");

    let config = format!(
        "edition = 1\n\n[signaling]\nurl = \"{signaling_url}\"\n\n[ais_endpoint]\nurl = \"{ais_url}\"\n\n[deployment]\nrealm_id = {realm_id}\n\n[hyper]\ndata_dir = \"{}\"\n\n[hyper.trust]\nkind = \"dev_only\"\n",
        data_dir.display(),
    );

    fs::write(project_dir.join("actr.e2e.toml"), config).expect("write actr.e2e.toml");
}

fn rewrite_package_name(project_dir: &Path, package_name: &str) {
    let manifest_path = project_dir.join("manifest.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("read manifest.toml");
    let rewritten = manifest.replacen(
        "name = \"echo-client-app\"",
        &format!("name = \"{package_name}\""),
        1,
    );
    fs::write(&manifest_path, rewritten).expect("write manifest.toml");
}

fn pin_echo_service_dependency_to_registry_version(project_dir: &Path) {
    let manifest_path = project_dir.join("manifest.toml");
    let manifest = fs::read_to_string(&manifest_path).expect("read manifest.toml");
    let rewritten = manifest.replace(
        r#"EchoService = { actr_type = "acme:EchoService:1.0.0" }"#,
        r#"EchoService = { actr_type = "acme:EchoService:0.1.0" }"#,
    );
    fs::write(&manifest_path, rewritten).expect("write manifest.toml");
}

fn signaling_base_url(signaling_ws_url: &str) -> String {
    signaling_ws_url
        .trim_end_matches("/signaling/ws")
        .trim_end_matches('/')
        .to_string()
}

fn copy_remote_echo_proto(service_dir: &Path, app_dir: &Path) {
    let service_proto = service_dir.join("protos/local/echo.proto");
    let app_remote_dir = app_dir.join("protos/remote/echo-service");
    fs::create_dir_all(&app_remote_dir).expect("create app remote proto dir");
    fs::copy(&service_proto, app_remote_dir.join("echo.proto")).expect("copy remote echo proto");
}

fn generate_xcode_project(project_dir: &std::path::Path) {
    let out = Command::new("xcodegen")
        .args(["generate"])
        .current_dir(project_dir)
        .output()
        .expect("xcodegen not found");
    assert_success(&out, "xcodegen generate");
}

fn build_cli_binary(
    project_dir: &std::path::Path,
    project_name: &str,
    scheme: &str,
    swift_assets: &actr_cli::test_support::LocalSwiftPackageAssets,
) -> std::path::PathBuf {
    let out = Command::new("xcodebuild")
        .env("ACTR_BINARY_PATH", &swift_assets.xcframework_path)
        .env("ACTR_BINDINGS_PATH", &swift_assets.bindings_path)
        .args([
            "build",
            "-project",
            &format!("{project_name}.xcodeproj"),
            "-scheme",
            scheme,
            "-configuration",
            "Debug",
            "-derivedDataPath",
            "build",
            "ONLY_ACTIVE_ARCH=YES",
            "CODE_SIGNING_ALLOWED=NO",
        ])
        .current_dir(project_dir)
        .output()
        .expect("xcodebuild not found");
    assert_success(&out, &format!("xcodebuild build ({scheme})"));

    let binary = project_dir.join(format!("build/Build/Products/Debug/{scheme}"));
    assert!(
        binary.exists(),
        "{scheme} binary should exist at {}",
        binary.display()
    );
    binary
}

#[test]
#[ignore] // Requires macOS, Xcode, xcodegen, and local Swift bindings
fn swift_echo_e2e_service_and_app() {
    let swift_assets =
        ensure_local_swift_xcframework().expect("failed to prepare local swift xcframework");
    let signaling = MockSignaling::start().expect("failed to start mock actrix");
    let signaling_base = signaling_base_url(&signaling.signaling_ws_url);
    let tmp = TempDir::new().unwrap();

    let init_out = run_actr(
        &[
            "init",
            "-l",
            "swift",
            "--template",
            "echo",
            "--role",
            "both",
            "--signaling",
            &signaling_base,
            "--manufacturer",
            "swift-e2e",
            "e2e-swift",
        ],
        tmp.path(),
    );
    assert_success(&init_out, "actr init -l swift --role both");

    let svc_dir = tmp.path().join("e2e-swift/echo-service");
    let app_dir = tmp.path().join("e2e-swift/echo-app");
    assert!(svc_dir.exists(), "echo-service dir should exist");
    assert!(app_dir.exists(), "echo-app dir should exist");
    align_project_with_local_actrix(&svc_dir).expect("failed to set local realm for service");
    align_project_with_local_actrix(&app_dir).expect("failed to set local realm for app");
    pin_echo_service_dependency_version(&app_dir, "acme")
        .expect("failed to pin app echo dependency version");
    pin_echo_service_dependency_to_registry_version(&app_dir);
    rewrite_package_name(&app_dir, "EchoApp");
    write_linked_runtime_config(&svc_dir);
    write_linked_runtime_config(&app_dir);

    assert_success(
        &run_actr(&["gen", "-l", "swift"], &svc_dir),
        "actr gen -l swift (svc)",
    );
    append_cli_target(&svc_dir, "EchoService", "EchoServiceCLI");
    write_service_cli(&svc_dir);
    generate_xcode_project(&svc_dir);
    let _svc_binary = build_cli_binary(&svc_dir, "EchoService", "EchoServiceCLI", &swift_assets);
    let rust_service = LocalRustEchoService::start(&signaling.signaling_ws_url)
        .expect("failed to start rust echo service");

    copy_remote_echo_proto(&svc_dir, &app_dir);
    assert_success(
        &run_actr(&["gen", "-l", "swift"], &app_dir),
        "actr gen -l swift (app)",
    );
    append_cli_target(&app_dir, "EchoApp", "EchoAppCLI");
    write_app_cli(&app_dir);
    generate_xcode_project(&app_dir);
    let app_binary = build_cli_binary(&app_dir, "EchoApp", "EchoAppCLI", &swift_assets);

    let mut app = Command::new(&app_binary)
        .current_dir(&app_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start app");

    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match app.try_wait().unwrap() {
            Some(_) => break,
            None if Instant::now() > deadline => {
                app.kill().ok();
                let app_out = app.wait_with_output().unwrap();
                panic!(
                    "app did not exit within 120s:\nstdout: {}\nstderr: {}\nservice:\n{}",
                    String::from_utf8_lossy(&app_out.stdout),
                    String::from_utf8_lossy(&app_out.stderr),
                    rust_service.logs()
                );
            }
            None => std::thread::sleep(Duration::from_millis(500)),
        }
    }

    let app_out = app.wait_with_output().unwrap();
    let app_stdout = String::from_utf8_lossy(&app_out.stdout);
    let app_stderr = String::from_utf8_lossy(&app_out.stderr);
    assert!(
        app_out.status.success(),
        "app failed:\nstdout: {app_stdout}\nstderr: {app_stderr}\nservice logs:\n{}",
        rust_service.logs(),
    );
    assert!(
        app_stdout.contains("Echo reply: Echo: hello"),
        "missing echo reply in app output:\nstdout: {app_stdout}\nstderr: {app_stderr}"
    );
    assert!(
        rust_service.logs().contains("✅ ActrNode started"),
        "service missing request log:\n{}",
        rust_service.logs()
    );
}
