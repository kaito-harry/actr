use std::collections::HashMap;

use crate::commands::codegen::{
    ActrGenMetadata, GenContext, MethodModel, ProtoFileModel, ProtoModel, ProtoSide,
    ScaffoldCatalog, ServiceModel, SupportedLanguage, write_metadata,
};

#[test]
fn test_remote_path_extraction() {
    // Test the logic for extracting remote path after "/remote/"
    let test_cases = vec![
        (
            "protos/remote/server/service.proto",
            Some("server/service.proto"),
        ),
        // "remote/test.proto" will NOT match because split produces ["", "test.proto"]
        // which is only 2 parts, but the first part is empty, not what we want
        ("protos/remote/test.proto", Some("test.proto")),
        ("protos/local.proto", None),
        ("no_remote_here.proto", None),
    ];

    for (input, expected) in test_cases {
        let parts: Vec<&str> = input.split("/remote/").collect();
        let result = if parts.len() == 2 && !parts[0].is_empty() {
            Some(parts[1])
        } else {
            None
        };

        assert_eq!(
            result, expected,
            "Failed for input: {}, expected: {:?}, got: {:?}",
            input, expected, result
        );
    }
}

#[test]
fn test_remote_services_map_construction() {
    // Create a simple mock lock file structure
    let mut remote_services_map: HashMap<String, String> = HashMap::new();

    // Simulate adding entries from lock file
    remote_services_map.insert(
        "server/service.proto".to_string(),
        "acme:TestServer".to_string(),
    );
    remote_services_map.insert(
        "api/v1/api.proto".to_string(),
        "custom:ApiService".to_string(),
    );

    // Verify the mapping
    assert_eq!(remote_services_map.len(), 2);
    assert_eq!(
        remote_services_map.get("server/service.proto"),
        Some(&"acme:TestServer".to_string())
    );
    assert_eq!(
        remote_services_map.get("api/v1/api.proto"),
        Some(&"custom:ApiService".to_string())
    );
}

#[test]
fn test_options_string_building() {
    let remote_file_mappings = [
        "remote/s1.proto=testco:S1".to_string(),
        "remote/s2.proto=other:S2".to_string(),
    ];
    let local_paths = ["local.proto".to_string()];

    let mut options = String::new();

    if !remote_file_mappings.is_empty() {
        options.push_str(&format!(
            "RemoteFileMapping={}",
            remote_file_mappings.join(";")
        ));
    }

    if !local_paths.is_empty() {
        if !options.is_empty() {
            options.push(',');
        }
        options.push_str(&format!("LocalFiles={}", local_paths.join(":")));
    }

    assert!(
        options.contains("RemoteFileMapping=remote/s1.proto=testco:S1;remote/s2.proto=other:S2")
    );
    assert!(options.contains("LocalFiles=local.proto"));
}

#[test]
fn test_actr_type_extraction_logic() {
    let remote_services_map: HashMap<String, String> = [
        (
            "service1/api.proto".to_string(),
            "mfg1:Service1".to_string(),
        ),
        (
            "service2/api.proto".to_string(),
            "mfg2:Service2".to_string(),
        ),
    ]
    .iter()
    .cloned()
    .collect();

    // Test matched path
    let path1 = "service1/api.proto";
    assert_eq!(
        remote_services_map.get(path1),
        Some(&"mfg1:Service1".to_string())
    );

    // Test unmatched path (should return None)
    let path2 = "unknown/api.proto";
    assert_eq!(remote_services_map.get(path2), None);

    // Test that we can handle None gracefully with empty string
    let actr_type = remote_services_map.get(path2).cloned().unwrap_or_default();
    assert_eq!(actr_type, "");
}

#[test]
fn test_empty_lock_file_scenario() {
    // When lock file doesn't exist or has no dependencies
    let remote_services_map: HashMap<String, String> = HashMap::new();

    // Should handle gracefully
    assert_eq!(remote_services_map.len(), 0);
    assert_eq!(remote_services_map.get("any/path.proto"), None);

    // Simulating the warning path
    let _path_str = "remote/service/api.proto";
    let is_in_map = remote_services_map.contains_key("service/api.proto");
    assert!(!is_in_map);
    // In actual code, this triggers warn! and pushes empty string
}

#[test]
fn pb2_alias_and_import_resolves_imported_type_owner() {
    // An imported `ask.*` type declared in `remote/ask-service/ask.proto`
    // resolves to an alias based on its owner proto path, not the local
    // service's package.
    let (alias, import) = super::pb2_alias_and_import("ask", "remote/ask-service/ask.proto");
    assert_eq!(alias, "remote_ask_service_ask_pb2");
    assert_eq!(
        import,
        "from generated.remote.ask_service import ask_pb2 as remote_ask_service_ask_pb2"
    );

    // A locally-declared type keeps its own package alias + module path.
    let (local_alias, local_import) =
        super::pb2_alias_and_import("data_stream_app", "local/data_stream_app.proto");
    assert_eq!(local_alias, "local_data_stream_app_pb2");
    assert_eq!(
        local_import,
        "from generated.local import data_stream_app_pb2 as local_data_stream_app_pb2"
    );
}

#[test]
fn pb2_alias_and_import_distinguishes_same_package_owner_files() {
    let (request_alias, request_import) =
        super::pb2_alias_and_import("shared", "remote/shared/request.proto");
    let (response_alias, response_import) =
        super::pb2_alias_and_import("shared", "remote/shared/response.proto");

    assert_eq!(request_alias, "remote_shared_request_pb2");
    assert_eq!(response_alias, "remote_shared_response_pb2");
    assert_ne!(request_alias, response_alias);
    assert_eq!(
        request_import,
        "from generated.remote.shared import request_pb2 as remote_shared_request_pb2"
    );
    assert_eq!(
        response_import,
        "from generated.remote.shared import response_pb2 as remote_shared_response_pb2"
    );
}

#[test]
fn generated_scaffold_uses_imported_rpc_owner_pb2_alias() {
    let tmp = tempfile::TempDir::new().unwrap();
    let config_path = tmp.path().join("manifest.toml");
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
    let config = actr_config::ConfigParser::from_manifest_file(&config_path).unwrap();
    let local_file = ProtoFileModel {
        proto_file: "local/data_stream_app.proto".into(),
        relative_path: "local/data_stream_app.proto".into(),
        package: "data_stream_app".to_string(),
        side: ProtoSide::Local,
        declared_type_names: vec![],
        services: vec![ServiceModel {
            name: "DataStreamAppService".to_string(),
            package: "data_stream_app".to_string(),
            proto_file: "local/data_stream_app.proto".into(),
            relative_path: "local/data_stream_app.proto".into(),
            side: ProtoSide::Local,
            methods: vec![MethodModel {
                name: "ContinuePromptResultStreams".to_string(),
                snake_name: "continue_prompt_result_streams".to_string(),
                input_type: "ask.ContinuePromptResultStreamsRequest".to_string(),
                output_type: "ask.ContinuePromptResultStreamsResponse".to_string(),
                route_key: "data_stream_app.DataStreamAppService.ContinuePromptResultStreams"
                    .to_string(),
            }],
            actr_type: None,
        }],
    };
    let remote_file = ProtoFileModel {
        proto_file: "remote/ask-service/ask.proto".into(),
        relative_path: "remote/ask-service/ask.proto".into(),
        package: "ask".to_string(),
        side: ProtoSide::Remote,
        declared_type_names: vec![
            "ContinuePromptResultStreamsRequest".to_string(),
            "ContinuePromptResultStreamsResponse".to_string(),
        ],
        services: vec![],
    };
    let context = GenContext {
        proto_files: vec![],
        proto_model: ProtoModel {
            files: vec![local_file.clone(), remote_file],
            local_services: local_file.services,
            remote_services: vec![],
        },
        input_path: tmp.path().join("protos"),
        output: tmp.path().join("generated"),
        config_path,
        config,
        no_scaffold: false,
        overwrite_user_code: false,
        no_format: false,
        debug: false,
        skip_validation: false,
    };
    let metadata =
        ActrGenMetadata::from_proto_model(SupportedLanguage::Python, &context.proto_model).unwrap();
    write_metadata(&context.output, &metadata).unwrap();
    let catalog = ScaffoldCatalog::from_metadata(&metadata);

    let generator = super::PythonGenerator;
    let services = generator
        .parse_local_services(&catalog)
        .expect("parse local services");
    let scaffold = generator
        .generate_scaffold_content(&context, "ActrService", "Workload", &services)
        .expect("render scaffold");

    assert!(scaffold.contains(
        "from generated.remote.ask_service import ask_pb2 as remote_ask_service_ask_pb2"
    ));
    assert!(scaffold.contains(
        "def continue_prompt_result_streams(self, req: remote_ask_service_ask_pb2.ContinuePromptResultStreamsRequest) -> remote_ask_service_ask_pb2.ContinuePromptResultStreamsResponse:"
    ));
    assert!(
        scaffold
            .contains("return remote_ask_service_ask_pb2.ContinuePromptResultStreamsResponse()")
    );
    assert!(!scaffold.contains("local_data_stream_app_pb2.ContinuePromptResultStreamsRequest"));
    assert!(!scaffold.contains("data_stream_app_pb2.ContinuePromptResultStreamsRequest"));
}

#[test]
fn parse_local_services_preserves_nested_python_type_names() {
    let tmp = tempfile::TempDir::new().unwrap();
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
    let config = actr_config::ConfigParser::from_manifest_file(&config_path).unwrap();
    let local_file = ProtoFileModel {
        proto_file: "local/client.proto".into(),
        relative_path: "local/client.proto".into(),
        package: "client".to_string(),
        side: ProtoSide::Local,
        declared_type_names: vec![],
        services: vec![ServiceModel {
            name: "Client".to_string(),
            package: "client".to_string(),
            proto_file: "local/client.proto".into(),
            relative_path: "local/client.proto".into(),
            side: ProtoSide::Local,
            methods: vec![MethodModel {
                name: "Foo".to_string(),
                snake_name: "foo".to_string(),
                input_type: "ask.Outer.InnerRequest".to_string(),
                output_type: "ask.Outer.InnerResponse".to_string(),
                route_key: "client.Client.Foo".to_string(),
            }],
            actr_type: None,
        }],
    };
    let remote_file = ProtoFileModel {
        proto_file: "remote/ask/ask.proto".into(),
        relative_path: "remote/ask/ask.proto".into(),
        package: "ask".to_string(),
        side: ProtoSide::Remote,
        declared_type_names: vec![
            "Outer".to_string(),
            "Outer.InnerRequest".to_string(),
            "Outer.InnerResponse".to_string(),
        ],
        services: vec![],
    };
    let context = GenContext {
        proto_files: vec![],
        proto_model: ProtoModel {
            files: vec![local_file.clone(), remote_file],
            local_services: local_file.services,
            remote_services: vec![],
        },
        input_path: tmp.path().join("protos"),
        output: tmp.path().join("generated"),
        config_path,
        config,
        no_scaffold: false,
        overwrite_user_code: false,
        no_format: false,
        debug: false,
        skip_validation: false,
    };
    let metadata =
        ActrGenMetadata::from_proto_model(SupportedLanguage::Python, &context.proto_model).unwrap();
    write_metadata(&context.output, &metadata).unwrap();
    let catalog = ScaffoldCatalog::from_metadata(&metadata);

    let services = super::PythonGenerator
        .parse_local_services(&catalog)
        .expect("parse local services");
    let method = &services[0].methods[0];

    assert_eq!(method.input_type, "Outer.InnerRequest");
    assert_eq!(method.output_type, "Outer.InnerResponse");
}
