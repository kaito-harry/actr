//! Integration tests for TypeScript code generation scenarios.
//!
//! These tests verify the file structure and content of generated TypeScript code
//! for three main scenarios:
//! 1. Local service only
//! 2. Remote service only
//! 3. Both local and remote services
//!
//! Run with:
//! `cargo test --test typescript_gen_scenarios -- --test-threads=1`

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::OnceLock;
use tempfile::TempDir;

fn actr_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_actr"))
}

fn framework_codegen_typescript_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("actr-cli should live under the workspace root")
        .join("tools/protoc-gen/typescript")
}

fn prepare_typescript_codegen_tools() -> &'static Path {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = framework_codegen_typescript_dir();

        // Ensure we are in the right directory
        assert!(
            dir.exists(),
            "tools/protoc-gen/typescript dir not found at {}",
            dir.display()
        );

        println!("Running npm install in {}...", dir.display());
        let npm_install = Command::new("npm")
            .args(["install"])
            .current_dir(&dir)
            .output()
            .expect("failed to run npm install");
        if !npm_install.status.success() {
            panic!(
                "npm install failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&npm_install.stdout),
                String::from_utf8_lossy(&npm_install.stderr)
            );
        }

        println!("Running npm run bundle in {}...", dir.display());
        let npm_bundle = Command::new("npm")
            .args(["run", "bundle"])
            .current_dir(&dir)
            .output()
            .expect("failed to run npm run bundle");
        if !npm_bundle.status.success() {
            panic!(
                "npm bundle failed:\nstdout: {}\nstderr: {}",
                String::from_utf8_lossy(&npm_bundle.stdout),
                String::from_utf8_lossy(&npm_bundle.stderr)
            );
        }

        dir
    })
}

fn run_actr(args: &[&str], cwd: &Path) -> Output {
    let tool_dir = prepare_typescript_codegen_tools();

    // Construct PATH to include the local plugin and its dependencies
    let mut path_entries = vec![tool_dir.join("scripts"), tool_dir.join("node_modules/.bin")];
    if let Some(existing) = std::env::var_os("PATH") {
        path_entries.extend(std::env::split_paths(&existing));
    }
    let path = std::env::join_paths(path_entries).expect("failed to construct PATH");

    Command::new(actr_bin())
        .args(args)
        .current_dir(cwd)
        .env("PATH", path)
        .output()
        .expect("failed to run actr binary")
}

fn assert_success(out: &Output, context: &str) {
    if !out.status.success() {
        panic!(
            "{context} failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

// Project construction helpers

fn write_ts_project_files(root: &Path) {
    fs::write(
        root.join("package.json"),
        r#"{
  "name": "test-gen",
  "dependencies": {
    "@actrium/actr": "*"
  }
}"#,
    )
    .unwrap();

    fs::write(
        root.join("tsconfig.json"),
        r#"{
  "compilerOptions": {
    "target": "ESNext",
    "module": "ESNext",
    "moduleResolution": "node",
    "strict": true,
    "skipLibCheck": true
  }
}"#,
    )
    .unwrap();
}

fn write_actr_toml_local_only(root: &Path) {
    fs::write(
        root.join("manifest.toml"),
        r#"edition = 1
[package]
name = "LocalService"
manufacturer = "acme"
version="0.0.1"
[dependencies]

[system.signaling]
url = "wss://localhost:8080"

[system.ais_endpoint]
url = "https://localhost:8080/ais"

[system.deployment]
realm_id = 1
"#,
    )
    .unwrap();
}

fn write_actr_toml_with_remote(root: &Path) {
    fs::write(
        root.join("manifest.toml"),
        r#"edition = 1
[package]
name = "RemoteApp"
manufacturer = "acme"
version="0.0.1"
[dependencies]
echo-service = { actr_type = "acme:EchoService:0.0.1" }

[system.signaling]
url = "wss://localhost:8080"

[system.ais_endpoint]
url = "https://localhost:8080/ais"

[system.deployment]
realm_id = 1
"#,
    )
    .unwrap();
}

fn write_actr_toml_both(root: &Path) {
    fs::write(
        root.join("manifest.toml"),
        r#"edition = 1
[package]
name = "BothService"
manufacturer = "acme"
version = "0.0.1"
[dependencies]
echo-service = { actr_type = "acme:EchoService:0.0.1" }

[system.signaling]
url = "wss://localhost:8080"

[system.ais_endpoint]
url = "https://localhost:8080/ais"

[system.deployment]
realm_id = 1
"#,
    )
    .unwrap();
}

fn write_actr_toml_both_with_two_remotes(root: &Path) {
    fs::write(
        root.join("manifest.toml"),
        r#"edition = 1
[package]
name = "BothTwoRemotesService"
manufacturer = "acme"
version="0.0.1"
[dependencies]
echo-service = { actr_type = "acme:EchoService:0.0.1" }
profile-service = { actr_type = "acme:ProfileService:0.0.1" }

[system.signaling]
url = "wss://localhost:8080"

[system.ais_endpoint]
url = "https://localhost:8080/ais"

[system.deployment]
realm_id = 1
"#,
    )
    .unwrap();
}

fn write_lock_file_empty(root: &Path) {
    fs::write(
        root.join("manifest.lock.toml"),
        r#"[metadata]
version = 1
generated_at = "2026-03-03T00:00:00Z"
"#,
    )
    .unwrap();
}

fn write_lock_file_with_echo(root: &Path) {
    fs::write(
        root.join("manifest.lock.toml"),
        r#"[metadata]
version = 1
generated_at = "2026-03-03T00:00:00Z"

[[dependency]]
name = "echo-service"
actr_type = "acme:EchoService:1.0.0"
fingerprint = "service_semantic:123"
cached_at = "2026-03-03T00:00:00Z"
files = [
    { path = "echo-service/echo.proto", fingerprint = "semantic:456" }
]
"#,
    )
    .unwrap();
}

fn write_lock_file_with_echo_and_profile(root: &Path) {
    fs::write(
        root.join("manifest.lock.toml"),
        r#"[metadata]
version = 1
generated_at = "2026-03-03T00:00:00Z"

[[dependency]]
name = "echo-service"
actr_type = "acme:EchoService:1.0.0"
fingerprint = "service_semantic:123"
cached_at = "2026-03-03T00:00:00Z"
files = [
    { path = "echo-service/echo.proto", fingerprint = "semantic:456" }
]

[[dependency]]
name = "profile-service"
actr_type = "acme:ProfileService:1.0.0"
fingerprint = "service_semantic:789"
cached_at = "2026-03-03T00:00:00Z"
files = [
    { path = "profile-service/profile.proto", fingerprint = "semantic:999" }
]
"#,
    )
    .unwrap();
}

fn write_local_greeter_proto(root: &Path) {
    let proto_dir = root.join("protos");
    fs::create_dir_all(&proto_dir).unwrap();
    fs::write(
        proto_dir.join("greeter.proto"),
        r#"syntax = "proto3";
package greeter;

message HelloRequest {
  string name = 1;
}

message HelloResponse {
  string message = 1;
}

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloResponse);
}
"#,
    )
    .unwrap();
}

fn write_remote_echo_proto(root: &Path) {
    let remote_proto_dir = root.join("protos/remote/echo-service");
    fs::create_dir_all(&remote_proto_dir).unwrap();
    fs::write(
        remote_proto_dir.join("echo.proto"),
        r#"syntax = "proto3";
package echo;

message EchoRequest {
  string message = 1;
}

message EchoResponse {
  string message = 1;
}

service EchoService {
  rpc Echo(EchoRequest) returns (EchoResponse);
}
"#,
    )
    .unwrap();
}

fn write_remote_profile_proto(root: &Path) {
    let remote_proto_dir = root.join("protos/remote/profile-service");
    fs::create_dir_all(&remote_proto_dir).unwrap();
    fs::write(
        remote_proto_dir.join("profile.proto"),
        r#"syntax = "proto3";
package profile;

message GetProfileRequest {
  string user_id = 1;
}

message GetProfileResponse {
  string nickname = 1;
}

service ProfileService {
  rpc GetProfile(GetProfileRequest) returns (GetProfileResponse);
}
"#,
    )
    .unwrap();
}

#[test]
fn test_local_service_only() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();

    write_ts_project_files(root);
    write_actr_toml_local_only(root);
    write_lock_file_empty(root);
    write_local_greeter_proto(root);

    let out = run_actr(&["gen", "-l", "typescript"], root);
    assert_success(&out, "actr gen -l typescript (local only)");

    let gen_dir = root.join("src/generated");
    assert!(gen_dir.join("greeter_pb.ts").exists());
    assert!(gen_dir.join("greeter_workload.ts").exists());
    assert!(!gen_dir.join("greeter_client.ts").exists());
    assert!(
        !gen_dir.join("local_actor.ts").exists(),
        "TypeScript local_actor.ts dispatcher should not be generated"
    );

    assert!(
        !gen_dir.join("local").exists(),
        "local/ directory should have been flattened"
    );
    assert!(
        !gen_dir.join("remote").exists(),
        "remote/ directory should not exist"
    );

    let workload_content = fs::read_to_string(gen_dir.join("greeter_workload.ts")).unwrap();
    assert!(
        workload_content.contains("import type { RpcEnvelope } from '@actrium/actr-workload';")
    );
    assert!(
        workload_content.contains("import { fromBinary, toBinary } from '@bufbuild/protobuf';")
    );
    assert!(
        workload_content
            .contains("export const GREETER_SAY_HELLO_ROUTE = \"greeter.Greeter.SayHello\";")
    );
    assert!(workload_content.contains("export interface GreeterHandler"));
    assert!(
        workload_content
            .contains("sayHello(req: HelloRequest): HelloResponse | Promise<HelloResponse>;")
    );
    assert!(workload_content.contains("export class GreeterDispatcher"));
    assert!(workload_content.contains("if (envelope.method === GREETER_SAY_HELLO_ROUTE)"));
    assert!(
        workload_content
            .contains("fromBinary(HelloRequestSchema, envelope.payload ?? new Uint8Array())")
    );
    assert!(workload_content.contains("return toBinary(HelloResponseSchema, response);"));
    assert!(workload_content.contains("throw new Error(`Unknown route: ${envelope.method}`);"));

    let actr_service_content = fs::read_to_string(root.join("src/actr_service.ts")).unwrap();
    assert!(
        actr_service_content.contains("import { defineWorkload } from '@actrium/actr-workload';")
    );
    assert!(
        actr_service_content
            .contains("import type { GreeterHandler } from './generated/greeter_workload.js';")
    );
    assert!(
        actr_service_content
            .contains("import { GreeterDispatcher } from './generated/greeter_workload.js';")
    );
    assert!(actr_service_content.contains("class GreeterHandlerImpl implements GreeterHandler"));
    assert!(
        actr_service_content
            .contains("const dispatcher = new GreeterDispatcher(new GreeterHandlerImpl());")
    );
    assert!(actr_service_content.contains("export default defineWorkload({"));
    assert!(actr_service_content.contains("return dispatcher.dispatch(envelope);"));
    assert!(!actr_service_content.contains("Local RPC methods:', 1"));
    assert!(actr_service_content.contains("Remote RPC methods:', 0"));
    assert!(!actr_service_content.contains("Received workload RPC:', envelope.method"));
    assert!(
        !actr_service_content.contains("// - Greeter.SayHello (HelloRequest -> HelloResponse)")
    );
    assert!(!actr_service_content.contains("from './generated/local_actor'"));
    assert!(!actr_service_content.contains("dispatchLocalActor"));
}

#[test]
fn test_remote_service_only() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();

    write_ts_project_files(root);
    write_actr_toml_with_remote(root);
    write_lock_file_with_echo(root);
    write_remote_echo_proto(root);

    let out = run_actr(&["gen", "-l", "typescript"], root);
    assert_success(&out, "actr gen -l typescript (remote only)");

    let gen_dir = root.join("src/generated");
    // Remote files are lifted: remote/echo-service/echo.proto -> src/generated/echo-service/echo_pb.ts
    assert!(gen_dir.join("echo-service/echo_pb.ts").exists());
    assert!(gen_dir.join("echo-service/echo_client.ts").exists());
    assert!(!gen_dir.join("echo_workload.ts").exists());
    assert!(
        !gen_dir.join("local_actor.ts").exists(),
        "TypeScript remote-only generation should emit client stubs only"
    );

    // Verify directory lifting
    assert!(
        !gen_dir.join("remote").exists(),
        "remote/ directory should have been lifted"
    );

    let client_content = fs::read_to_string(gen_dir.join("echo-service/echo_client.ts")).unwrap();
    assert!(client_content.contains("export const EchoRequest = {"));
    assert!(client_content.contains("routeKey: \"echo.EchoService.Echo\""));
    assert!(client_content.contains("response: {"));

    let actr_service_content = fs::read_to_string(root.join("src/actr_service.ts")).unwrap();
    assert!(
        actr_service_content.contains("import { defineWorkload } from '@actrium/actr-workload';")
    );
    assert!(actr_service_content.contains("Remote RPC methods:', 1"));
    assert!(actr_service_content.contains("Remote RPC quick-start examples"));
    assert!(actr_service_content.contains("EchoRequest.routeKey"));
    assert!(actr_service_content.contains("EchoRequest.encode"));
    assert!(actr_service_content.contains("EchoRequest.response.decode"));
    assert!(!actr_service_content.contains("HelloRequest.decode"));
}

#[test]
fn test_local_and_remote_services() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();

    write_ts_project_files(root);
    write_actr_toml_both(root);
    write_lock_file_with_echo(root);
    write_local_greeter_proto(root);
    write_remote_echo_proto(root);

    let out = run_actr(&["gen", "-l", "typescript"], root);
    assert_success(&out, "actr gen -l typescript (both)");

    let gen_dir = root.join("src/generated");

    // Local files
    assert!(gen_dir.join("greeter_pb.ts").exists());
    assert!(gen_dir.join("greeter_workload.ts").exists());
    assert!(!gen_dir.join("greeter_client.ts").exists());

    // Remote files
    assert!(gen_dir.join("echo-service/echo_pb.ts").exists());
    assert!(gen_dir.join("echo-service/echo_client.ts").exists());

    assert!(
        !gen_dir.join("local_actor.ts").exists(),
        "TypeScript generation should not emit a local dispatcher"
    );

    let client_content = fs::read_to_string(gen_dir.join("echo-service/echo_client.ts")).unwrap();
    assert!(client_content.contains("export const EchoRequest = {"));
    assert!(client_content.contains("routeKey: \"echo.EchoService.Echo\""));

    let actr_service_content = fs::read_to_string(root.join("src/actr_service.ts")).unwrap();
    assert!(actr_service_content.contains("export default defineWorkload({"));
    assert!(
        actr_service_content
            .contains("import { GreeterDispatcher } from './generated/greeter_workload.js';")
    );
    assert!(actr_service_content.contains("return dispatcher.dispatch(envelope);"));
    assert!(!actr_service_content.contains("Local RPC methods:', 1"));
    assert!(
        !actr_service_content.contains("// - Greeter.SayHello (HelloRequest -> HelloResponse)")
    );
    assert!(actr_service_content.contains("from './generated/echo-service/echo_client';"));
    assert!(actr_service_content.contains("EchoRequest.routeKey"));
    assert!(!actr_service_content.contains("from './generated/local_actor'"));
}

#[test]
fn test_remote_clients_are_generated_without_local_dispatcher() {
    let tmp = TempDir::new().unwrap();
    let root = tmp.path();

    write_ts_project_files(root);
    write_actr_toml_both_with_two_remotes(root);
    write_lock_file_with_echo_and_profile(root);
    write_local_greeter_proto(root);
    write_remote_echo_proto(root);
    write_remote_profile_proto(root);

    let out = run_actr(&["gen", "-l", "typescript"], root);
    assert_success(&out, "actr gen -l typescript (local + two remotes)");

    let gen_dir = root.join("src/generated");
    assert!(gen_dir.join("greeter_workload.ts").exists());
    assert!(
        !gen_dir.join("local_actor.ts").exists(),
        "TypeScript remote client generation should not emit local_actor.ts"
    );

    let echo_client = fs::read_to_string(gen_dir.join("echo-service/echo_client.ts")).unwrap();
    let profile_client =
        fs::read_to_string(gen_dir.join("profile-service/profile_client.ts")).unwrap();
    assert!(echo_client.contains("routeKey: \"echo.EchoService.Echo\""));
    assert!(profile_client.contains("routeKey: \"profile.ProfileService.GetProfile\""));

    let actr_service_content = fs::read_to_string(root.join("src/actr_service.ts")).unwrap();
    assert!(actr_service_content.contains("Remote RPC methods:', 2"));
    assert!(actr_service_content.contains("EchoRequest.routeKey"));
    assert!(actr_service_content.contains("GetProfileRequest.routeKey"));
}
