use super::*;
use crate::commands::codegen::{
    GenContext, MethodModel, ProtoFileModel, ProtoModel, ProtoSide, ServiceModel,
};

fn remote_echo_service() -> ServiceInfo {
    ServiceInfo {
        service_name: "EchoService".to_string(),
        proto_package: "echo".to_string(),
        is_local: false,
        remote_target_type: Some("acme:EchoService:1.0.0".to_string()),
        methods: vec![MethodInfo {
            name: "Echo".to_string(),
            request_type: "EchoRequest".to_string(),
            response_type: "EchoResponse".to_string(),
            request_import: "echo.Echo".to_string(),
            response_import: "echo.Echo".to_string(),
        }],
    }
}

fn local_client_service() -> ServiceInfo {
    ServiceInfo {
        service_name: "ClientService".to_string(),
        proto_package: "client".to_string(),
        is_local: true,
        remote_target_type: None,
        methods: vec![MethodInfo {
            name: "Send".to_string(),
            request_type: "SendRequest".to_string(),
            response_type: "SendResponse".to_string(),
            request_import: "client.Client".to_string(),
            response_import: "client.Client".to_string(),
        }],
    }
}

#[test]
fn remote_service_registry_uses_manifest_dependency_aliases() {
    let generator = KotlinGenerator;
    let service = remote_echo_service();
    let mut aliases = HashMap::new();
    aliases.insert(
        "acme:EchoService:1.0.0".to_string(),
        "echo-service".to_string(),
    );

    let content = generator
        .generate_remote_service_registry(&[&service], &aliases)
        .expect("render remote registry");

    assert!(content.contains("val remoteRouteAliases: Map<String, String>"));
    assert!(content.contains("\"echo.Echo\" to \"echo-service\""));
    assert!(content.contains("resolveManifestDependency(manifestPath, alias)"));
    assert!(content.contains(".toSet()"));
    assert!(content.contains(".associateWith { alias ->"));
    assert!(content.contains("targetsByAlias.getValue(alias)"));
    assert!(
        content.contains("getActorType(routeKey: String, remoteTargets: Map<String, ActrType>)")
    );
    assert!(!content.contains("ActrType(manufacturer"));
    assert!(!content.contains("remoteRoutes: Map<String, ActrType>"));
}

#[test]
fn unified_workload_requires_pre_resolved_remote_targets() {
    let services = vec![local_client_service(), remote_echo_service()];
    let content = generate_unified_workload_scaffold(&services, "com.example.generated");

    assert!(content.contains("class UnifiedWorkload("));
    assert!(content.contains("private val remoteTargets: Map<String, ActrType>,"));
    assert!(content.contains("suspend fun onStart(ctx: ActrContext)"));
    assert!(content.contains("suspend fun onReady(ctx: ActrContext)"));
    assert!(content.contains("suspend fun onStop(ctx: ActrContext)"));
    assert!(content.contains("suspend fun onError(ctx: ActrContext, event: ErrorEvent)"));
    assert!(
        content
            .contains("suspend fun dispatch(ctx: ActrContext, envelope: RpcEnvelope): ByteArray")
    );
    assert!(content.contains("UnifiedDispatcher.discoverRemoteServices(ctx, remoteTargets)"));
    assert!(content.contains("UnifiedDispatcher.dispatch(handler, ctx, remoteTargets, envelope)"));

    assert!(!content.contains("WorkloadLifecycleBridge"));
    assert!(content.contains("val lifecycle = UnifiedLifecycleAdapter(workload)"));
    assert!(content.contains("val dynamicWorkload = lifecycle.toDynamicWorkload()"));
    assert!(!content.contains("import io.actrium.actr.DynamicWorkload"));
    assert!(!content.contains("fun toDynamicWorkload(): DynamicWorkload"));
    assert!(!content.contains("private val realmId"));
    assert!(!content.contains("ActrId("));
    assert!(!content.contains("Realm("));
    assert!(!content.contains("manifestPath: String?"));
    assert!(!content.contains("remoteTargets: Map<String, ActrType> = emptyMap()"));
    assert!(!content.contains("manifestPath?.let"));
    assert!(!content.contains("\n\\\n"));
}

#[test]
fn local_only_workload_does_not_require_remote_targets() {
    let content =
        generate_unified_workload_scaffold(&[local_client_service()], "com.example.generated");

    assert!(!content.contains("private val remoteTargets"));
    assert!(!content.contains("resolveRemoteTargets"));
    assert!(content.contains("val workload = UnifiedWorkload(handler)"));
}

#[test]
fn collect_services_preserves_nested_kotlin_type_names() {
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
    std::fs::create_dir_all(&context.output).unwrap();
    std::fs::write(
        context.output.join("actr-gen-meta.json"),
        r#"{
  "plugin_version": "test",
  "language": "kotlin",
  "local_services": [{
    "name": "Client",
    "package": "client",
    "proto_file": "local/client.proto",
    "handler_interface": "ClientHandler",
    "workload_type": "ClientWorkload",
    "dispatcher_type": "ClientDispatcher",
    "methods": [{
      "name": "Foo",
      "snake_name": "foo",
      "route_key": "client.Client.Foo",
      "input_ref": {
        "proto_type": "ask.Outer.InnerRequest",
        "type_name": "Outer.InnerRequest",
        "proto_package": "ask",
        "proto_file": "remote/ask/ask.proto",
        "generated_type": "ask.Ask.Outer.InnerRequest"
      },
      "output_ref": {
        "proto_type": "ask.Outer.InnerResponse",
        "type_name": "Outer.InnerResponse",
        "proto_package": "ask",
        "proto_file": "remote/ask/ask.proto",
        "generated_type": "ask.Ask.Outer.InnerResponse"
      }
    }]
  }],
  "remote_services": []
}"#,
    )
    .unwrap();

    let catalog = ScaffoldCatalog::load(&context, SupportedLanguage::Kotlin).unwrap();
    let services = KotlinGenerator
        .collect_services(&catalog)
        .expect("collect services");
    let method = &services[0].methods[0];

    assert_eq!(method.request_type, "ask.Ask.Outer.InnerRequest");
    assert_eq!(method.response_type, "ask.Ask.Outer.InnerResponse");
}

#[test]
fn collect_services_uses_descriptor_generated_kotlin_types() {
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
    let output = tmp.path().join("generated");
    std::fs::create_dir_all(&output).unwrap();
    std::fs::write(
        output.join("actr-gen-meta.json"),
        r#"{
  "plugin_version": "test",
  "language": "kotlin",
  "local_services": [{
    "name": "Client",
    "package": "client",
    "proto_file": "local/client.proto",
    "handler_interface": "ClientHandler",
    "workload_type": "ClientWorkload",
    "dispatcher_type": "ClientDispatcher",
    "methods": [{
      "name": "Call",
      "snake_name": "call",
      "route_key": "client.Client.Call",
      "input_ref": {
        "proto_type": "types.Request",
        "type_name": "Request",
        "proto_package": "types",
        "proto_file": "types.proto",
        "generated_type": "com.example.types.UserTypesProto.Request"
      },
      "output_ref": {
        "proto_type": "types.Response",
        "type_name": "Response",
        "proto_package": "types",
        "proto_file": "types.proto",
        "generated_type": "com.example.types.Response"
      }
    }]
  }],
  "remote_services": []
}"#,
    )
    .unwrap();

    let local_file = ProtoFileModel {
        proto_file: "local/client.proto".into(),
        relative_path: "local/client.proto".into(),
        package: "client".to_string(),
        side: ProtoSide::Local,
        declared_type_names: vec!["WrongRequest".to_string(), "WrongResponse".to_string()],
        services: vec![ServiceModel {
            name: "Client".to_string(),
            package: "client".to_string(),
            proto_file: "local/client.proto".into(),
            relative_path: "local/client.proto".into(),
            side: ProtoSide::Local,
            methods: vec![MethodModel {
                name: "Call".to_string(),
                snake_name: "call".to_string(),
                input_type: "client.WrongRequest".to_string(),
                output_type: "client.WrongResponse".to_string(),
                route_key: "client.Client.Call".to_string(),
            }],
            actr_type: None,
        }],
    };
    let context = GenContext {
        proto_files: vec![],
        proto_model: ProtoModel {
            files: vec![local_file.clone()],
            local_services: local_file.services,
            remote_services: vec![],
        },
        input_path: tmp.path().join("protos"),
        output,
        config_path,
        config,
        no_scaffold: false,
        overwrite_user_code: false,
        no_format: false,
        debug: false,
        skip_validation: false,
    };

    let catalog = ScaffoldCatalog::load(&context, SupportedLanguage::Kotlin).unwrap();
    let services = KotlinGenerator
        .collect_services(&catalog)
        .expect("collect services");
    let method = &services[0].methods[0];

    assert_eq!(
        method.request_type,
        "com.example.types.UserTypesProto.Request"
    );
    assert_eq!(method.response_type, "com.example.types.Response");
    assert!(method.request_import.is_empty());
    assert!(method.response_import.is_empty());
}

#[test]
fn unified_lifecycle_adapter_wraps_unified_workload() {
    let content = generate_unified_lifecycle_adapter_scaffold("com.example.generated");

    assert!(content.contains("package com.example"));
    assert!(content.contains("class UnifiedLifecycleAdapter("));
    assert!(content.contains("private val workload: UnifiedWorkload"));
    assert!(content.contains(") : Workload"));
    assert!(content.contains("import io.actrium.actr.dsl.ActrContext"));
    assert!(content.contains("import io.actrium.actr.dsl.DynamicWorkload"));
    assert!(content.contains("import io.actrium.actr.dsl.ErrorEvent"));
    assert!(content.contains("import io.actrium.actr.dsl.RpcEnvelope"));
    assert!(content.contains("import io.actrium.actr.dsl.Workload"));
    assert!(content.contains("import io.actrium.actr.dsl.dynamicWorkload"));
    assert!(content.contains("override suspend fun onStart(ctx: ActrContext)"));
    assert!(content.contains("workload.onStart(ctx)"));
    assert!(content.contains("override suspend fun onReady(ctx: ActrContext)"));
    assert!(content.contains("workload.onReady(ctx)"));
    assert!(content.contains("override suspend fun onStop(ctx: ActrContext)"));
    assert!(content.contains("workload.onStop(ctx)"));
    assert!(content.contains("override suspend fun onError(ctx: ActrContext, event: ErrorEvent)"));
    assert!(content.contains("workload.onError(ctx, event)"));
    assert!(content.contains(
        "override suspend fun dispatch(ctx: ActrContext, envelope: RpcEnvelope): ByteArray"
    ));
    assert!(content.contains("return workload.dispatch(ctx, envelope)"));
    assert!(content.contains("fun toDynamicWorkload(): DynamicWorkload"));
    assert!(content.contains("return dynamicWorkload("));
    assert!(content.contains("lifecycle = this"));
}

#[test]
fn kotlin_bootstrap_fixtures_inject_manifest_resolved_targets() {
    for fixture in [
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/kotlin/echo/MainActivity.kt"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/kotlin/echo/EchoIntegrationTest.kt"
        )),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/kotlin/data-stream/DataStreamIntegrationTest.kt"
        )),
    ] {
        assert!(fixture.contains("RemoteServiceRegistry.resolveRemoteTargets("));
        assert!(fixture.contains("remoteTargets = remoteTargets"));
        assert!(fixture.contains("UnifiedLifecycleAdapter("));
        assert!(fixture.contains("toDynamicWorkload()"));
        assert!(!fixture.contains(concat!("attach", "(clientWorkload)")));
    }
}

#[test]
fn unified_handler_scaffold_imports_and_resolves_imported_rpc_types() {
    // A local `data_stream_app` service references `ask.*` message types.
    // The scaffold must import the `ask.Ask` outer class and reference the
    // bare message name (resolving via that import) instead of pinning the
    // type to the local service's package.
    let service = ServiceInfo {
        service_name: "DataStreamAppService".to_string(),
        proto_package: "data_stream_app".to_string(),
        is_local: true,
        remote_target_type: None,
        methods: vec![MethodInfo {
            name: "continue_prompt_result_streams".to_string(),
            request_type: "ContinuePromptResultStreamsRequest".to_string(),
            response_type: "ContinuePromptResultStreamsResponse".to_string(),
            request_import: "ask.Ask".to_string(),
            response_import: "ask.Ask".to_string(),
        }],
    };

    let imports = super::kotlin_type_imports(std::iter::once(&service));
    assert!(
        imports.contains("import ask.Ask.*"),
        "expected `ask.Ask` import, got:\n{imports}"
    );
    let scaffold = generate_unified_handler_scaffold(&[service], "com.example.generated");
    // Signature uses the bare message name, resolved via the `ask.Ask` import.
    assert!(scaffold.contains("request: ContinuePromptResultStreamsRequest"));
    assert!(scaffold.contains(": ContinuePromptResultStreamsResponse"));
    // The unqualified `ask.` proto-package prefix must NOT leak into Kotlin.
    assert!(!scaffold.contains("request: ask."));
}
