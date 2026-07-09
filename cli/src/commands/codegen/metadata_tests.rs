use super::*;
use tempfile::TempDir;

fn svc_model(name: &str, side: super::super::ProtoSide) -> ServiceModel {
    ServiceModel {
        name: name.into(),
        package: format!("pkg.{name}"),
        proto_file: PathBuf::from(format!("protos/{name}.proto")),
        relative_path: PathBuf::from(format!("protos/{name}.proto")),
        side,
        methods: vec![MethodModel {
            name: "Echo".into(),
            snake_name: "echo".into(),
            input_type: "EchoRequest".into(),
            output_type: "EchoResponse".into(),
            route_key: "echo.EchoService.Echo".into(),
        }],
        actr_type: Some("acme:Echo:1.0.0".into()),
    }
}

#[test]
fn from_proto_model_populates_local_and_remote() {
    let model = ProtoModel {
        files: vec![],
        local_services: vec![svc_model("EchoService", super::super::ProtoSide::Local)],
        remote_services: vec![],
    };
    let meta = ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &model).unwrap();
    assert_eq!(meta.local_services.len(), 1);
    assert_eq!(meta.local_services[0].name, "EchoService");
    assert_eq!(meta.language, "rust");
    assert!(meta.remote_services.is_empty());
}

#[test]
fn metadata_path_joins_output_dir_with_filename() {
    let p = metadata_path(std::path::Path::new("out"));
    assert_eq!(p, std::path::Path::new("out").join(ACTR_GEN_META_FILE));
}

#[test]
fn load_metadata_returns_none_when_file_absent() {
    let dir = TempDir::new().unwrap();
    assert!(load_metadata(dir.path()).unwrap().is_none());
}

#[test]
fn write_and_load_metadata_roundtrip() {
    let dir = TempDir::new().unwrap();
    let meta = ActrGenMetadata {
        plugin_version: "actr-cli".into(),
        language: "rust".into(),
        local_services: vec![],
        remote_services: vec![],
    };
    let path = write_metadata(dir.path(), &meta).unwrap();
    assert!(path.exists());
    let loaded = load_metadata(dir.path()).unwrap().unwrap();
    assert_eq!(loaded.plugin_version, "actr-cli");
}

const ASK_PROTO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/protos/data-stream-app/remote/ask-service/ask.proto"
));
const DATA_STREAM_APP_PROTO: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/protos/data-stream-app/local/data_stream_app.proto"
));

fn write_data_stream_app_fixture(root: &std::path::Path) -> std::path::PathBuf {
    let proto_root = root.join("protos");
    let remote_dir = proto_root.join("remote/ask-service");
    let local_dir = proto_root.join("local");
    std::fs::create_dir_all(&remote_dir).unwrap();
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::write(remote_dir.join("ask.proto"), ASK_PROTO).unwrap();
    let local_proto = local_dir.join("data_stream_app.proto");
    std::fs::write(&local_proto, DATA_STREAM_APP_PROTO).unwrap();
    proto_root
}

fn minimal_manifest(root: &std::path::Path) -> actr_config::ManifestConfig {
    use actr_config::ConfigParser;
    let config_path = root.join("manifest.toml");
    std::fs::write(
        &config_path,
        r#"edition = 1
exports = []

[package]
name = "DataStreamApp"
manufacturer = "acme"
version = "0.1.0"

[system.signaling]
url = "ws://127.0.0.1:8080"

[system.ais_endpoint]
url = "http://127.0.0.1:8080/ais"

[system.deployment]
realm_id = 1001
"#,
    )
    .unwrap();
    ConfigParser::from_manifest_file(&config_path).unwrap()
}

#[test]
fn from_proto_model_resolves_imported_rpc_type_owner() {
    let tmp = TempDir::new().unwrap();
    let proto_root = write_data_stream_app_fixture(tmp.path());
    let config = minimal_manifest(tmp.path());

    let local_proto = proto_root.join("local/data_stream_app.proto");
    let remote_proto = proto_root.join("remote/ask-service/ask.proto");
    let proto_files = vec![local_proto.clone(), remote_proto.clone()];
    let proto_model = ProtoModel::parse(&proto_files, &proto_root, &config).unwrap();

    let meta = ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &proto_model).unwrap();
    assert_eq!(meta.local_services.len(), 1);
    let service = &meta.local_services[0];
    assert_eq!(service.name, "DataStreamAppService");
    assert_eq!(service.methods.len(), 1);
    let method = &service.methods[0];

    // Bare type names stay language-agnostic.
    assert_eq!(
        method.input_ref.type_name,
        "ContinuePromptResultStreamsRequest"
    );
    assert_eq!(
        method.output_ref.type_name,
        "ContinuePromptResultStreamsResponse"
    );

    // Owner refs point at the declaring `ask` proto, not the local
    // `data_stream_app` service package.
    assert_eq!(method.input_ref.proto_package, "ask");
    assert_eq!(method.input_ref.proto_file, "remote/ask-service/ask.proto");
    assert_eq!(
        method.input_ref.type_name,
        "ContinuePromptResultStreamsRequest"
    );
    assert_eq!(
        method.input_ref.proto_type,
        "ask.ContinuePromptResultStreamsRequest"
    );
    assert_eq!(method.output_ref.proto_package, "ask");
    assert_eq!(method.output_ref.proto_file, "remote/ask-service/ask.proto");
}

#[test]
fn type_owner_index_prefers_current_file_for_unqualified_types() {
    let tmp = TempDir::new().unwrap();
    let proto_root = write_data_stream_app_fixture(tmp.path());
    let config = minimal_manifest(tmp.path());
    let local_proto = proto_root.join("local/data_stream_app.proto");
    let remote_proto = proto_root.join("remote/ask-service/ask.proto");
    let proto_files = vec![local_proto, remote_proto];
    let proto_model = ProtoModel::parse(&proto_files, &proto_root, &config).unwrap();

    let index = crate::commands::codegen::TypeOwnerIndex::from_files(&proto_model.files);
    let local_file = proto_model
        .files
        .iter()
        .find(|f| f.package == "data_stream_app")
        .expect("local data_stream_app file should be parsed");

    // Qualified imported type resolves to the declaring `ask` file.
    let resolved = index
        .resolve("ask.ContinuePromptResultStreamsRequest", local_file)
        .expect("qualified type should resolve unambiguously")
        .expect("owner should be known");
    assert_eq!(resolved.proto_package, "ask");
    assert_eq!(resolved.proto_file, "remote/ask-service/ask.proto");

    // An unresolvable type (e.g. a well-known type) falls back gracefully.
    let unresolvable = index
        .resolve("google.protobuf.Empty", local_file)
        .expect("unresolvable type should fall back, not error");
    assert!(unresolvable.is_none());
}

#[test]
fn type_owner_index_resolves_unqualified_unique_non_current_type() {
    // A local service references an unqualified `SharedRequest` declared in
    // exactly one remote file. It should resolve to that file (the
    // `bare` single-candidate branch), not fall back to the current file.
    let tmp = TempDir::new().unwrap();
    let proto_root = tmp.path().join("protos");
    let local_dir = proto_root.join("local");
    let remote_dir = proto_root.join("remote/pkg-a");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&remote_dir).unwrap();
    std::fs::write(
        local_dir.join("client.proto"),
        "syntax = \"proto3\";\npackage client;\nimport \"remote/pkg-a/a.proto\";\nservice Client {\n  rpc Foo(SharedRequest) returns (SharedResponse);\n}\n",
    )
    .unwrap();
    std::fs::write(
        remote_dir.join("a.proto"),
        "syntax = \"proto3\";\npackage a;\nmessage SharedRequest {}\nmessage SharedResponse {}\n",
    )
    .unwrap();

    let config = minimal_manifest(tmp.path());
    let local_proto = proto_root.join("local/client.proto");
    let remote_proto = proto_root.join("remote/pkg-a/a.proto");
    let proto_files = vec![local_proto, remote_proto];
    let proto_model = ProtoModel::parse(&proto_files, &proto_root, &config).unwrap();

    let meta = ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &proto_model).unwrap();
    let method = &meta.local_services[0].methods[0];
    assert_eq!(method.input_ref.proto_package, "a");
    assert_eq!(method.input_ref.proto_file, "remote/pkg-a/a.proto");
    assert_eq!(method.input_ref.type_name, "SharedRequest");
}

#[test]
fn from_proto_model_errors_on_ambiguous_unqualified_type() {
    // An unqualified `SharedRequest` declared in TWO remote files is
    // ambiguous and must surface as a config error rather than silently
    // picking one owner.
    let tmp = TempDir::new().unwrap();
    let proto_root = tmp.path().join("protos");
    let local_dir = proto_root.join("local");
    let remote_a = proto_root.join("remote/pkg-a");
    let remote_b = proto_root.join("remote/pkg-b");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&remote_a).unwrap();
    std::fs::create_dir_all(&remote_b).unwrap();
    std::fs::write(
        local_dir.join("client.proto"),
        "syntax = \"proto3\";\npackage client;\nimport \"remote/pkg-a/a.proto\";\nimport \"remote/pkg-b/b.proto\";\nservice Client {\n  rpc Foo(SharedRequest) returns (SharedResponse);\n}\n",
    )
    .unwrap();
    std::fs::write(
        remote_a.join("a.proto"),
        "syntax = \"proto3\";\npackage a;\nmessage SharedRequest {}\nmessage SharedResponse {}\n",
    )
    .unwrap();
    std::fs::write(
        remote_b.join("b.proto"),
        "syntax = \"proto3\";\npackage b;\nmessage SharedRequest {}\nmessage SharedResponse {}\n",
    )
    .unwrap();

    let config = minimal_manifest(tmp.path());
    let local_proto = proto_root.join("local/client.proto");
    let proto_files = vec![
        local_proto,
        remote_a.join("a.proto"),
        remote_b.join("b.proto"),
    ];
    let proto_model = ProtoModel::parse(&proto_files, &proto_root, &config).unwrap();

    let result = ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &proto_model);
    let err = result.expect_err("ambiguous type should error");
    let msg = err.to_string();
    assert!(
        msg.contains("Cannot uniquely resolve") && msg.contains("SharedRequest"),
        "unexpected error message: {msg}"
    );
}

#[test]
fn from_proto_model_errors_on_unresolved_qualified_type() {
    let tmp = TempDir::new().unwrap();
    let proto_root = tmp.path().join("protos");
    let local_dir = proto_root.join("local");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::write(
        local_dir.join("client.proto"),
        "syntax = \"proto3\";\npackage client;\nimport \"google/protobuf/empty.proto\";\nservice Client {\n  rpc Ping(google.protobuf.Empty) returns (google.protobuf.Empty);\n}\n",
    )
    .unwrap();

    let config = minimal_manifest(tmp.path());
    let local_proto = proto_root.join("local/client.proto");
    let proto_model = ProtoModel::parse(&[local_proto], &proto_root, &config).unwrap();

    let err = ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &proto_model)
        .expect_err("unresolved qualified RPC type should be a config error");
    let message = err.to_string();
    assert!(
        message.contains("Cannot resolve input type `google.protobuf.Empty`"),
        "unexpected error message: {message}"
    );
}

#[test]
fn from_proto_model_resolves_nested_qualified_type_owner() {
    let tmp = TempDir::new().unwrap();
    let proto_root = tmp.path().join("protos");
    let local_dir = proto_root.join("local");
    let remote_dir = proto_root.join("remote/ask");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&remote_dir).unwrap();
    std::fs::write(
        local_dir.join("client.proto"),
        "syntax = \"proto3\";\npackage client;\nimport \"remote/ask/ask.proto\";\nservice Client {\n  rpc Foo(ask.Outer.InnerRequest) returns (ask.Outer.InnerResponse);\n}\n",
    )
    .unwrap();
    std::fs::write(
        remote_dir.join("ask.proto"),
        "syntax = \"proto3\";\npackage ask;\nmessage Outer {\n  message InnerRequest {}\n  message InnerResponse {}\n}\n",
    )
    .unwrap();

    let config = minimal_manifest(tmp.path());
    let local_proto = proto_root.join("local/client.proto");
    let remote_proto = remote_dir.join("ask.proto");
    let proto_model =
        ProtoModel::parse(&[local_proto, remote_proto], &proto_root, &config).unwrap();

    let meta = ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &proto_model).unwrap();
    let method = &meta.local_services[0].methods[0];
    assert_eq!(method.input_ref.proto_package, "ask");
    assert_eq!(method.input_ref.proto_file, "remote/ask/ask.proto");
    assert_eq!(method.input_ref.type_name, "Outer.InnerRequest");
    assert_eq!(method.output_ref.type_name, "Outer.InnerResponse");
}

#[test]
fn load_metadata_rejects_missing_type_refs() {
    let dir = TempDir::new().unwrap();
    let json = r#"{
        "plugin_version": "actr-cli",
        "language": "rust",
        "local_services": [{
            "name": "EchoService",
            "package": "echo",
            "proto_file": "local/echo.proto",
            "handler_interface": "EchoServiceHandler",
            "workload_type": "EchoServiceWorkload",
            "dispatcher_type": "EchoServiceDispatcher",
            "methods": [{
                "name": "Echo",
                "snake_name": "echo",
                "input_type": "EchoRequest",
                "output_type": "EchoResponse",
                "route_key": "echo.EchoService.Echo"
            }]
        }],
        "remote_services": []
    }"#;
    let path = dir.path().join("actr-gen-meta.json");
    std::fs::write(&path, json).unwrap();
    let err = load_metadata(dir.path()).expect_err("missing type refs should fail");
    let message = err.to_string();
    assert!(
        message.contains("missing field `input_ref`"),
        "unexpected error message: {message}"
    );
}
