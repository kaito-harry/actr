use super::*;
use crate::commands::codegen::ProtoModel;
use crate::commands::codegen::write_metadata;
use actr_config::ConfigParser;
use tempfile::TempDir;

#[test]
fn from_metadata_maps_local_and_remote_to_scaffold_services() {
    let metadata = ActrGenMetadata {
        plugin_version: "actr-cli".into(),
        language: "rust".into(),
        local_services: vec![crate::commands::codegen::metadata::LocalServiceMetadata {
            name: "EchoService".into(),
            package: "echo".into(),
            proto_file: "echo.proto".into(),
            handler_interface: "EchoServiceHandler".into(),
            workload_type: "EchoServiceWorkload".into(),
            dispatcher_type: "EchoServiceDispatcher".into(),
            methods: vec![crate::commands::codegen::metadata::MethodMetadata {
                name: "Echo".into(),
                snake_name: "echo".into(),
                route_key: "echo.Echo".into(),
                input_ref: TypeRef {
                    proto_type: "echo.EchoRequest".into(),
                    type_name: "EchoRequest".into(),
                    proto_package: "echo".into(),
                    proto_file: "echo.proto".into(),
                    generated_type: None,
                },
                output_ref: TypeRef {
                    proto_type: "echo.EchoResponse".into(),
                    type_name: "EchoResponse".into(),
                    proto_package: "echo".into(),
                    proto_file: "echo.proto".into(),
                    generated_type: None,
                },
            }],
        }],
        remote_services: vec![],
    };
    let catalog = ScaffoldCatalog::from_metadata(&metadata);
    assert_eq!(catalog.local_services.len(), 1);
    assert_eq!(catalog.local_services[0].name, "EchoService");
    assert!(catalog.local_services[0].handler_interface.is_some());
    assert!(catalog.local_services[0].client_type.is_none());
    assert!(catalog.remote_services.is_empty());
    assert!(catalog.has_any_methods());

    assert!(
        !ScaffoldCatalog {
            local_services: vec![],
            remote_services: vec![]
        }
        .has_any_methods()
    );
}

fn empty_context(tmp: &TempDir) -> GenContext {
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

    GenContext {
        proto_files: vec![],
        proto_model: ProtoModel {
            files: vec![],
            local_services: vec![],
            remote_services: vec![],
        },
        input_path: tmp.path().join("protos"),
        output: tmp.path().join("generated"),
        config_path,
        config,
        no_scaffold: false,
        overwrite_user_code: false,
        no_format: true,
        debug: false,
        skip_validation: true,
    }
}

#[test]
fn load_requires_plugin_metadata() {
    let tmp = TempDir::new().unwrap();
    let context = empty_context(&tmp);

    let err = ScaffoldCatalog::load(&context, SupportedLanguage::Rust)
        .expect_err("scaffold catalog must not fall back to ProtoModel");

    assert!(
        err.to_string().contains("actr-gen-meta.json"),
        "expected missing metadata error, got: {err}"
    );
}

#[test]
fn load_rejects_metadata_for_different_language() {
    let tmp = TempDir::new().unwrap();
    let context = empty_context(&tmp);
    write_metadata(
        &context.output,
        &ActrGenMetadata {
            plugin_version: "test-plugin".into(),
            language: "swift".into(),
            local_services: vec![],
            remote_services: vec![],
        },
    )
    .unwrap();

    let err = ScaffoldCatalog::load(&context, SupportedLanguage::Rust)
        .expect_err("scaffold catalog must reject metadata from a different language");

    assert!(
        err.to_string().contains("language"),
        "expected language mismatch error, got: {err}"
    );
}
