use super::*;
use tempfile::TempDir;

fn sample_echo_service() -> ProtoService {
    ProtoService {
        name: "EchoService".to_string(),
        package: "echo".to_string(),
        swift_package_prefix: "Echo_".to_string(),
        workload_name: "EchoServiceWorkload".to_string(),
        methods: vec![ProtoMethod {
            name: "Echo".to_string(),
            swift_name: "echo".to_string(),
            input_type: "Echo_EchoRequest".to_string(),
            output_type: "Echo_EchoResponse".to_string(),
        }],
    }
}

#[test]
fn generated_handler_impl_contains_rpc_method_stubs() {
    let generator = SwiftGenerator;
    let handler = generator
        .generate_handler_impl_content(&sample_echo_service())
        .expect("render handler");

    assert!(handler.contains("public final class EchoServiceHandlerImpl: EchoServiceHandler"));
    assert!(handler.contains("private let targetType: ActrType?"));
    assert!(handler.contains("public init(targetType: ActrType? = nil)"));
    assert!(handler.contains("Use targetType with ctx.discover(targetType:)"));
    assert!(handler.contains("public func echo("));
    assert!(handler.contains("req: Echo_EchoRequest"));
    assert!(handler.contains("ctx: any ActrContext"));
    assert!(handler.contains("async throws(ActrError) -> Echo_EchoResponse"));
    assert!(handler.contains("ActrError.NotImplemented"));
}

#[test]
fn swift_type_name_uses_declaring_package_for_imported_types() {
    let generator = SwiftGenerator;

    // An imported type declared in package `ask` resolves to the `Ask_`
    // prefix, not the current `data_stream_app` service's prefix.
    let imported = TypeRef {
        proto_type: "ask.ContinuePromptResultStreamsRequest".to_string(),
        type_name: "ContinuePromptResultStreamsRequest".to_string(),
        proto_package: "ask".to_string(),
        proto_file: "remote/ask-service/ask.proto".to_string(),
        generated_type: None,
    };
    assert_eq!(
        generator.swift_type_name(&imported),
        "Ask_ContinuePromptResultStreamsRequest"
    );

    // A local type declared in the service's own package keeps that prefix.
    let local = TypeRef {
        proto_type: "echo.EchoRequest".to_string(),
        type_name: "EchoRequest".to_string(),
        proto_package: "echo".to_string(),
        proto_file: "local/echo.proto".to_string(),
        generated_type: None,
    };
    assert_eq!(generator.swift_type_name(&local), "Echo_EchoRequest");
}

#[test]
fn swift_type_name_preserves_nested_parent_scope() {
    let generator = SwiftGenerator;
    let nested = TypeRef {
        proto_type: "ask.Outer.InnerRequest".to_string(),
        type_name: "InnerRequest".to_string(),
        proto_package: "ask".to_string(),
        proto_file: "remote/ask/ask.proto".to_string(),
        generated_type: None,
    };

    assert_eq!(generator.swift_type_name(&nested), "Ask_Outer.InnerRequest");
}

#[test]
fn generated_scaffold_uses_linked_runtime() {
    let generator = SwiftGenerator;
    let service = sample_echo_service();
    let remote_targets = vec![RemoteTarget {
        alias: "echo-service".to_string(),
        variable_name: "echoServiceTargetType".to_string(),
        routes: vec![RemoteTargetRoute {
            route_key: "echo.EchoService.Echo".to_string(),
            variable_name: "echoServiceTargetType".to_string(),
        }],
    }];
    let scaffold = generator
        .generate_scaffold_content(
            "demo",
            "EchoService",
            "EchoServiceWorkload",
            std::slice::from_ref(&service),
            &remote_targets,
        )
        .expect("render scaffold");

    assert!(scaffold.contains("ACTR: mutable scaffold"));
    assert!(scaffold.contains("public final class ActrService: ObservableObject"));
    assert!(scaffold.contains("public init() {}"));
    assert!(scaffold.contains("public private(set) var connectionStatus"));
    assert!(scaffold.contains("public func initialize("));
    assert!(scaffold.contains("manifestPath: String,"));
    assert!(scaffold.contains("configPath: String"));
    assert!(scaffold.contains("public func initializeFromBundle() async throws"));
    assert!(scaffold.contains("public func shutdown() async"));
    assert!(scaffold.contains("public enum ConnectionStatus"));
    assert!(scaffold.contains("resolveManifestPackageActrType"));
    assert!(!scaffold.contains("import ActrBindings"));
    assert!(!scaffold.contains("ActrBindings.resolveManifest"));
    assert!(scaffold.contains("resolveManifestDependency("));
    assert!(scaffold.contains("dependencyAlias: \"echo-service\""));
    assert!(scaffold.contains("let remoteTargets: [String: ActrType] = ["));
    assert!(scaffold.contains("\"echo.EchoService.Echo\": echoServiceTargetType"));
    assert!(scaffold.contains("EchoServiceHandlerImpl(targetType: targetType)"));
    assert!(scaffold.contains("EchoServiceLifecycleAdapter("));
    assert!(scaffold.contains("remoteTargets: remoteTargets"));
    assert!(scaffold.contains("dynamicWorkload(lifecycle: lifecycle)"));
    assert!(scaffold.contains("ActrNode.linked("));
    assert!(!scaffold.contains("ActrNode.from(packageConfig:"));
}

#[test]
fn generated_scaffold_supports_empty_local_workload() {
    let generator = SwiftGenerator;
    let remote_targets = vec![RemoteTarget {
        alias: "echo-service".to_string(),
        variable_name: "echoServiceTargetType".to_string(),
        routes: vec![RemoteTargetRoute {
            route_key: "echo.EchoService.Echo".to_string(),
            variable_name: "echoServiceTargetType".to_string(),
        }],
    }];
    let scaffold = generator
        .generate_scaffold_content("demo", "Client", "ClientWorkload", &[], &remote_targets)
        .expect("render scaffold");

    assert!(scaffold.contains("let remoteTargets: [String: ActrType] = ["));
    assert!(scaffold.contains("\"echo.EchoService.Echo\": echoServiceTargetType"));
    assert!(scaffold.contains("ClientLifecycleAdapter(remoteTargets: remoteTargets)"));
    assert!(scaffold.contains("dynamicWorkload(lifecycle: lifecycle)"));
    assert!(!scaffold.contains("HandlerImpl"));
    assert!(!scaffold.contains("targetType ="));
}

#[test]
fn swift_runtime_config_template_has_valid_defaults() {
    let template = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/swift/actr.toml.hbs"
    ));

    assert!(
        !template.contains("\n[acl]\n"),
        "empty [acl] tables fail runtime config parsing"
    );
    assert!(
        template.contains("[hyper.trust]") || template.contains("[[trust]]"),
        "linked Swift runtimes must declare an explicit trust policy"
    );
}

#[test]
fn remote_target_variable_names_are_swift_safe_and_unique() {
    let mut used = std::collections::HashMap::new();
    let names = ["foo-bar", "foo_bar", "foo.bar", "1service", "!!!"]
        .into_iter()
        .map(|alias| remote_target_variable_name(alias, &mut used))
        .collect::<Vec<_>>();

    assert_eq!(
        names,
        vec![
            "fooBarTargetType",
            "fooBar2TargetType",
            "fooBar3TargetType",
            "target1serviceTargetType",
            "targetTargetType",
        ]
    );
}

#[test]
fn echo_content_view_uses_generated_actr_service_api() {
    let content_view = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/swift/echo/ContentView.swift.hbs"
    ));

    assert!(content_view.contains("initializeFromBundle()"));
    assert!(!content_view.contains("actrService.initialize()"));
    assert!(!content_view.contains("sendEcho("));
}

#[test]
fn generated_lifecycle_adapter_contains_workload_conformance() {
    let generator = SwiftGenerator;
    let adapter = generator
        .generate_lifecycle_adapter_content("EchoService", "EchoServiceWorkload", true)
        .expect("render lifecycle adapter");

    assert!(adapter.contains("ACTR: mutable scaffold"));
    assert!(adapter.contains("import Foundation"));
    assert!(adapter.lines().any(|line| {
            line == "public final class EchoServiceLifecycleAdapter<Handler: EchoServiceHandler>: Workload, @unchecked Sendable {"
        }));
    assert!(!adapter.contains("private final class EchoServiceLifecycleAdapter"));
    assert!(adapter.contains("EchoServiceWorkload<Handler>"));
    assert!(!adapter.contains("EchoServiceHandlerImpl"));
    assert!(adapter.contains("public func onStart(ctx: Context) async throws"));
    assert!(adapter.contains("public func onReady(ctx: Context) async throws"));
    assert!(adapter.contains("public func onStop(ctx: Context) async throws"));
    assert!(adapter.contains("public func onError(ctx: Context, event: ErrorEvent) async throws"));
    assert!(adapter.contains(
        "public func dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data"
    ));
    assert!(!adapter.contains("ctx: ContextBridge"));
    assert!(!adapter.contains("event: ErrorEventBridge"));
    assert!(!adapter.contains("envelope: RpcEnvelopeBridge"));
    assert!(adapter.contains("workload.__dispatch(ctx: ctx, envelope: envelope)"));
}

#[test]
fn generated_lifecycle_adapter_supports_empty_workload() {
    let generator = SwiftGenerator;
    let adapter = generator
        .generate_lifecycle_adapter_content("Client", "ClientWorkload", false)
        .expect("render lifecycle adapter");

    assert!(
        adapter.lines().any(|line| line
            == "public final class ClientLifecycleAdapter: Workload, @unchecked Sendable {")
    );
    assert!(adapter.contains("private let workload: ClientWorkload"));
    assert!(adapter.contains("public init(workload: ClientWorkload)"));
    assert!(adapter.contains("public convenience init(remoteTargets: [String: ActrType] = [:])"));
    assert!(adapter.contains("ClientWorkload(remoteTargets: remoteTargets)"));
    assert!(adapter.contains("workload.__dispatch(ctx: ctx, envelope: envelope)"));
    assert!(!adapter.contains("<Handler"));
    assert!(!adapter.contains("HandlerImpl"));
}

#[test]
fn xcodegen_root_is_optional_for_swiftpm_layouts() {
    let temp_dir = TempDir::new().unwrap();
    let sources = temp_dir.path().join("Sources/ActrBridge/Generated");
    std::fs::create_dir_all(&sources).unwrap();

    assert_eq!(find_project_yml_ancestor([sources]), None);
}

#[test]
fn xcodegen_root_finds_project_yml_ancestor() {
    let temp_dir = TempDir::new().unwrap();
    let sources = temp_dir.path().join("App/Generated");
    std::fs::create_dir_all(&sources).unwrap();
    std::fs::write(temp_dir.path().join("project.yml"), "name: App\n").unwrap();

    assert_eq!(
        find_project_yml_ancestor([sources]),
        Some(temp_dir.path().to_path_buf())
    );
}

#[test]
fn detects_standard_swift_template_layout() {
    let temp_dir = TempDir::new().unwrap();
    let project_root = temp_dir.path();
    std::fs::write(project_root.join("project.yml"), "actr-swift\n").unwrap();
    std::fs::write(
        project_root.join("manifest.toml"),
        "[package]\nname = \"echo-app\"\n",
    )
    .unwrap();
    std::fs::write(project_root.join("manifest.lock.toml"), "").unwrap();
    std::fs::create_dir_all(project_root.join("EchoApp")).unwrap();

    let layout = SwiftTemplateProjectLayout::detect(project_root, "echo-app")
        .expect("expected standard Swift template layout");

    assert_eq!(layout.app_root, project_root.join("EchoApp"));
    assert_eq!(
        layout.generated_root,
        project_root.join("EchoApp/Generated")
    );
    assert_eq!(
        layout.mutable_scaffold,
        project_root.join("EchoApp/ActrService.swift")
    );
}

#[test]
fn converges_legacy_generated_files_into_generated_directory() {
    let temp_dir = TempDir::new().unwrap();
    let project_root = temp_dir.path();
    let app_root = project_root.join("EchoApp");
    let generated_root = app_root.join("Generated");
    std::fs::create_dir_all(&generated_root).unwrap();
    std::fs::write(project_root.join("project.yml"), "actr-swift\n").unwrap();
    std::fs::write(
        project_root.join("manifest.toml"),
        "[package]\nname = \"echo-app\"\n",
    )
    .unwrap();
    std::fs::write(project_root.join("manifest.lock.toml"), "").unwrap();

    let legacy_file = app_root.join("echo.pb.swift");
    let generated_file = generated_root.join("echo.pb.swift");
    let generated_content =
        "// Generated by the Swift generator plugin for the protocol buffer compiler.\n";
    std::fs::write(&legacy_file, generated_content).unwrap();
    std::fs::write(&generated_file, generated_content).unwrap();

    let layout = SwiftTemplateProjectLayout::detect(project_root, "echo-app")
        .expect("expected standard Swift template layout");
    layout
        .converge_generated_outputs(std::slice::from_ref(&generated_file))
        .expect("converge generated outputs");

    assert!(
        !legacy_file.exists(),
        "legacy generated file should be removed"
    );
    assert!(
        generated_file.exists(),
        "generated file should remain in Generated/"
    );
}

#[test]
fn extracts_service_workload_name_from_service_specific_actor_file() {
    let tmp = TempDir::new().unwrap();
    let output_dir = tmp.path();
    std::fs::write(
        output_dir.join("local_echo.actor.swift"),
        "public actor LocalEchoServiceWorkload<T: LocalEchoServiceHandler> {\n",
    )
    .unwrap();

    let generator = SwiftGenerator;
    let workload = generator.extract_workload_name_for_service(output_dir, "LocalEchoService");

    assert_eq!(workload.as_deref(), Some("LocalEchoServiceWorkload"));
}

#[test]
fn preserves_modified_legacy_generated_files() {
    let temp_dir = TempDir::new().unwrap();
    let project_root = temp_dir.path();
    let app_root = project_root.join("EchoApp");
    let generated_root = app_root.join("Generated");
    std::fs::create_dir_all(&generated_root).unwrap();
    std::fs::write(project_root.join("project.yml"), "actr-swift\n").unwrap();
    std::fs::write(
        project_root.join("manifest.toml"),
        "[package]\nname = \"echo-app\"\n",
    )
    .unwrap();
    std::fs::write(project_root.join("manifest.lock.toml"), "").unwrap();

    let legacy_file = app_root.join("echo.pb.swift");
    let generated_file = generated_root.join("echo.pb.swift");
    std::fs::write(
            &legacy_file,
            "// Generated by the Swift generator plugin for the protocol buffer compiler.\n// user edit\n",
        )
        .unwrap();
    std::fs::write(
        &generated_file,
        "// Generated by the Swift generator plugin for the protocol buffer compiler.\n",
    )
    .unwrap();

    let layout = SwiftTemplateProjectLayout::detect(project_root, "echo-app")
        .expect("expected standard Swift template layout");
    layout
        .converge_generated_outputs(std::slice::from_ref(&generated_file))
        .expect("converge generated outputs");

    assert!(
        legacy_file.exists(),
        "modified legacy file should be preserved"
    );
    assert!(
        generated_file.exists(),
        "generated file should remain in Generated/"
    );
}
