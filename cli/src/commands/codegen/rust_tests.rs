use super::RustGenerator;
use crate::commands::codegen::scaffold::{ScaffoldCatalog, ScaffoldMethod, ScaffoldService};
use crate::commands::codegen::{
    ActrGenMetadata, GenContext, LanguageGenerator, ProtoModel, SupportedLanguage, TypeRef,
    write_metadata,
};
use actr_config::ConfigParser;
use std::path::PathBuf;
use tempfile::TempDir;

fn scaffold_service() -> ScaffoldService {
    ScaffoldService {
        name: "EmptyShell".to_string(),
        package: "demo.shell".to_string(),
        proto_file: PathBuf::from("bridge.proto"),
        handler_interface: Some("EmptyShellHandler".to_string()),
        workload_type: Some("EmptyShellWorkload".to_string()),
        dispatcher_type: Some("EmptyShellDispatcher".to_string()),
        client_type: None,
        actr_type: None,
        methods: vec![ScaffoldMethod {
            name: "Ping".to_string(),
            snake_name: "ping".to_string(),
            input_type: "PingRequest".to_string(),
            output_type: "PingResponse".to_string(),
            route_key: "demo.shell.EmptyShell/Ping".to_string(),
            input_ref: TypeRef {
                proto_type: "demo.shell.PingRequest".to_string(),
                type_name: "PingRequest".to_string(),
                proto_package: "demo.shell".to_string(),
                proto_file: "bridge.proto".to_string(),
                generated_type: None,
            },
            output_ref: TypeRef {
                proto_type: "demo.shell.PingResponse".to_string(),
                type_name: "PingResponse".to_string(),
                proto_package: "demo.shell".to_string(),
                proto_file: "bridge.proto".to_string(),
                generated_type: None,
            },
        }],
    }
}

#[test]
fn modified_generated_handler_scaffold_is_preserved() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("empty_shell.rs");
    let generator = RustGenerator;
    let scaffold = generator.generate_scaffold_content(&scaffold_service());
    let modified = format!("{scaffold}\n// User customization.\n");
    std::fs::write(&path, modified).unwrap();

    assert!(
        !generator
            .should_overwrite_handler_scaffold(&path, &scaffold)
            .unwrap()
    );
}

#[test]
fn modified_generated_entry_scaffold_is_preserved() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("lib.rs");
    let generator = RustGenerator;
    let scaffold = generator.generate_entry_scaffold_content(&scaffold_service());
    let modified = format!("{scaffold}\n// User customization.\n");
    std::fs::write(&path, modified).unwrap();

    assert!(
        !generator
            .should_overwrite_entry_scaffold(&path, &scaffold)
            .unwrap()
    );
}

#[test]
fn scaffold_empty_service_uses_service_metadata_and_writes_entry() {
    let tmp = TempDir::new().unwrap();
    let src_dir = tmp.path().join("src");
    let proto_root = tmp.path().join("protos");
    std::fs::create_dir_all(src_dir.join("generated")).unwrap();
    std::fs::create_dir_all(&proto_root).unwrap();

    let proto_file = proto_root.join("bridge.proto");
    std::fs::write(
        &proto_file,
        "syntax = \"proto3\";\npackage demo.shell;\nservice EmptyShell {}\n",
    )
    .unwrap();

    let config_path = tmp.path().join("manifest.toml");
    std::fs::write(
        &config_path,
        r#"edition = 1
exports = []

[package]
name = "Demo"
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

    let config = ConfigParser::from_manifest_file(&config_path).unwrap();
    let proto_files = vec![proto_file];
    let proto_model = ProtoModel::parse(&proto_files, &proto_root, &config).unwrap();
    let context = GenContext {
        proto_files,
        proto_model,
        input_path: proto_root,
        output: src_dir.join("generated"),
        config_path,
        config,
        no_scaffold: false,
        overwrite_user_code: false,
        no_format: false,
        debug: false,
        skip_validation: false,
    };
    let metadata =
        ActrGenMetadata::from_proto_model(SupportedLanguage::Rust, &context.proto_model).unwrap();
    write_metadata(&context.output, &metadata).unwrap();
    let catalog = ScaffoldCatalog::from_metadata(&metadata);

    tokio_test::block_on(RustGenerator.generate_scaffold(&context, &catalog)).unwrap();

    let handler_path = src_dir.join("empty_shell.rs");
    assert!(
        handler_path.exists(),
        "handler file should be named from service metadata, not proto stem"
    );
    assert!(
        !src_dir.join("bridge_service.rs").exists(),
        "proto file stem should not drive the scaffold handler path"
    );

    let handler = std::fs::read_to_string(&handler_path).unwrap();
    assert!(handler.contains("use crate::generated::bridge_actor::EmptyShellHandler;"));
    assert!(handler.contains("pub struct EmptyShellImpl;"));
    assert!(handler.contains("impl EmptyShellHandler for EmptyShellImpl"));

    let lib = std::fs::read_to_string(src_dir.join("lib.rs")).unwrap();
    assert!(lib.contains("pub mod generated;"));
    assert!(lib.contains("pub mod empty_shell;"));
    assert!(lib.contains("use generated::bridge_actor::EmptyShellWorkload;"));
    assert!(lib.contains("pub use crate::empty_shell::EmptyShellImpl;"));
    assert!(lib.contains("entry!("));
    assert!(lib.contains("EmptyShellWorkload<EmptyShellImpl>"));

    std::fs::write(&handler_path, "pub struct UserImplemented;\n").unwrap();
    tokio_test::block_on(RustGenerator.generate_scaffold(&context, &catalog)).unwrap();
    assert_eq!(
        std::fs::read_to_string(&handler_path).unwrap(),
        "pub struct UserImplemented;\n",
        "implemented handler files must be preserved without overwrite_user_code"
    );
}

#[test]
fn build_remote_file_actr_types_uses_shared_proto_model() {
    let tmp = TempDir::new().unwrap();
    let proto_root = tmp.path().join("protos");
    let local_dir = proto_root.join("local");
    let remote_dir = proto_root.join("remote/echo");
    std::fs::create_dir_all(&local_dir).unwrap();
    std::fs::create_dir_all(&remote_dir).unwrap();

    let local_proto = local_dir.join("local.proto");
    let remote_proto = remote_dir.join("echo.proto");

    std::fs::write(
        &local_proto,
        "syntax = \"proto3\";\npackage demo;\nservice EmptyBridge {}\n",
    )
    .unwrap();
    std::fs::write(
        &remote_proto,
        "syntax = \"proto3\";\npackage demo;\nservice EchoService {}\n",
    )
    .unwrap();

    let config_path = tmp.path().join("manifest.toml");
    std::fs::write(
        &config_path,
        r#"edition = 1
exports = []

[package]
name = "Demo"
manufacturer = "acme"
version = "0.1.0"

[dependencies]
echo = { actr_type = "remote:EchoService:0.1.0" }

[system.signaling]
url = "ws://127.0.0.1:8080"

[system.ais_endpoint]
url = "http://127.0.0.1:8080/ais"

[system.deployment]
realm_id = 1001
"#,
    )
    .unwrap();

    let config = ConfigParser::from_manifest_file(&config_path).unwrap();
    let proto_files = vec![local_proto, remote_proto];
    let proto_model = ProtoModel::parse(&proto_files, &proto_root, &config).unwrap();

    let context = GenContext {
        proto_files,
        proto_model,
        input_path: proto_root,
        output: tmp.path().join("src/generated"),
        config_path,
        config,
        no_scaffold: false,
        overwrite_user_code: false,
        no_format: false,
        debug: false,
        skip_validation: false,
    };

    let mappings = RustGenerator
        .build_remote_file_actr_types(&context)
        .unwrap();
    assert!(mappings.contains("echo.proto="));
    assert!(mappings.contains("EchoService"));
}

#[test]
fn pure_helpers_produce_correct_output() {
    let svc = scaffold_service();
    assert_eq!(super::handler_module_name(&svc), "empty_shell");
    assert_eq!(super::handler_impl_type(&svc), "EmptyShellImpl");
    assert_eq!(
        super::service_type_or_default(&svc, None, "Workload"),
        "EmptyShellWorkload"
    );
    assert!(!super::message_imports(&svc).is_empty());
    assert!(!super::handler_method_impls(&svc).is_empty());
    // is_default_cargo_lib_rs returns false for non-default lib.rs.
    assert!(!super::is_default_cargo_lib_rs("custom content"));
}

#[test]
fn message_imports_use_nested_parent_modules() {
    let mut svc = scaffold_service();
    svc.methods[0].input_ref = TypeRef {
        proto_type: "ask.Outer.InnerRequest".to_string(),
        type_name: "InnerRequest".to_string(),
        proto_package: "ask".to_string(),
        proto_file: "remote/ask/ask.proto".to_string(),
        generated_type: None,
    };
    svc.methods[0].output_ref = TypeRef {
        proto_type: "ask.Outer.InnerResponse".to_string(),
        type_name: "InnerResponse".to_string(),
        proto_package: "ask".to_string(),
        proto_file: "remote/ask/ask.proto".to_string(),
        generated_type: None,
    };

    let imports = super::message_imports(&svc);

    assert!(
        imports.contains("use crate::generated::ask::{outer::InnerRequest, outer::InnerResponse};")
            || imports.contains(
                "use crate::generated::ask::{outer::InnerResponse, outer::InnerRequest};"
            ),
        "expected nested parent modules in imports, got:\n{imports}"
    );
}
