use super::*;
use crate::commands::codegen::ProtoSide;
use tempfile::TempDir;

#[test]
fn parses_proto_content_from_real_service_definitions() {
    let generator = TypeScriptGenerator;
    let (package, services) = generator.parse_proto_content(
        r#"
            syntax = "proto3";

            package demo.echo;

            service EchoService {
              rpc Echo(EchoRequest) returns (EchoResponse);
              rpc Ping(demo.echo.PingRequest) returns (demo.echo.PingResponse);
            }
            "#,
    );

    assert_eq!(package, "demo.echo");
    assert_eq!(services.len(), 1);
    assert_eq!(services[0].name, "EchoService");
    assert_eq!(services[0].methods.len(), 2);
    assert_eq!(services[0].methods[0].name, "Echo");
    assert_eq!(services[0].methods[0].input_type_short, "EchoRequest");
    assert_eq!(services[0].methods[1].output_type_short, "PingResponse");
}

#[test]
fn inspects_generated_client_exports() {
    let generator = TypeScriptGenerator;
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("echo_client.ts");
    std::fs::write(
        &path,
        r#"
            export const EchoRequest = {
                routeKey: "demo.echo.Echo",
            } as const;
            "#,
    )
    .unwrap();

    let api = generator.inspect_generated_client_api(&path).unwrap();
    assert!(api.exported_consts.contains("EchoRequest"));
}

#[test]
fn implemented_marker_prevents_overwrite() {
    let generator = TypeScriptGenerator;
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("actr_service.ts");
    std::fs::write(
            &path,
            format!(
                "{IMPLEMENTED_MARKER}\n{UNIMPLEMENTED_MARKER}\nexport default defineWorkload({{ dispatch() {{ throw new Error('custom'); }} }});\n"
            ),
        )
        .unwrap();
    assert!(!generator.should_overwrite_scaffold(&path).unwrap());
}

#[test]
fn recognizes_minimal_unimplemented_scaffold() {
    let generator = TypeScriptGenerator;
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("actr_service.ts");
    std::fs::write(
            &path,
            format!(
                "{UNIMPLEMENTED_MARKER}\n{SCAFFOLD_HINT}\nexport default defineWorkload({{ dispatch() {{ throw new Error('TODO'); }} }});\n"
            ),
        )
        .unwrap();
    assert!(generator.should_overwrite_scaffold(&path).unwrap());
}

#[test]
fn unimplemented_marker_with_scaffold_hint_is_overwritten() {
    let generator = TypeScriptGenerator;
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("actr_service.ts");
    std::fs::write(
        &path,
        format!("{UNIMPLEMENTED_MARKER}\n{SCAFFOLD_HINT}\nconsole.log('custom quick-start');\n"),
    )
    .unwrap();
    assert!(generator.should_overwrite_scaffold(&path).unwrap());

    std::fs::write(&path, "console.log('user code');\n").unwrap();
    assert!(!generator.should_overwrite_scaffold(&path).unwrap());
}

#[test]
fn does_not_treat_echo_templates_as_generated_scaffold() {
    let generator = TypeScriptGenerator;
    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("actr_service.ts");

    std::fs::write(
            &path,
            format!(
                "{IMPLEMENTED_MARKER}\nexport default defineWorkload({{ dispatch() {{ throw new Error('template'); }} }});\n"
            ),
        )
        .unwrap();
    assert!(!generator.should_overwrite_scaffold(&path).unwrap());

    std::fs::write(
            &path,
            format!(
                "{IMPLEMENTED_MARKER}\nimport {{ ActrNode }} from '@actrium/actr';\nconsole.log('template');\n"
            ),
        )
        .unwrap();
    assert!(!generator.should_overwrite_scaffold(&path).unwrap());
}

#[test]
fn generates_scaffold_with_local_and_remote_sections() {
    let generator = TypeScriptGenerator;
    let scaffold = generator.generate_scaffold_content(&[
        BoundMethodInfo {
            generated_client_import: "./generated/echo_client".to_string(),
            generated_workload_import: "./generated/echo_workload.js".to_string(),
            service_name: "EchoService".to_string(),
            handler_interface: "EchoServiceHandler".to_string(),
            dispatcher_type: "EchoServiceDispatcher".to_string(),
            method_name: "Echo".to_string(),
            handler_method_name: "echo".to_string(),
            input_type: "EchoRequest".to_string(),
            output_type: "EchoResponse".to_string(),
            input_type_short: "EchoRequest".to_string(),
            output_type_short: "EchoResponse".to_string(),
            input_pb_import: "./generated/echo_pb.js".to_string(),
            output_pb_import: "./generated/echo_pb.js".to_string(),
            request_companion: Some("EchoRequest".to_string()),
            is_local: true,
        },
        BoundMethodInfo {
            generated_client_import: "./generated/demo/remote_client".to_string(),
            generated_workload_import: String::new(),
            service_name: "RemoteService".to_string(),
            handler_interface: String::new(),
            dispatcher_type: String::new(),
            method_name: "Ping".to_string(),
            handler_method_name: "ping".to_string(),
            input_type: "PingRequest".to_string(),
            output_type: "PingResponse".to_string(),
            input_type_short: "PingRequest".to_string(),
            output_type_short: "PingResponse".to_string(),
            input_pb_import: "./generated/demo/remote_pb.js".to_string(),
            output_pb_import: "./generated/demo/remote_pb.js".to_string(),
            request_companion: Some("PingRequest".to_string()),
            is_local: false,
        },
    ]);

    assert!(scaffold.contains("import { defineWorkload } from '@actrium/actr-workload';"));
    assert!(scaffold.contains("export default defineWorkload({"));
    assert!(
        scaffold.contains("import { EchoServiceDispatcher } from './generated/echo_workload.js';")
    );
    assert!(scaffold.contains("class EchoServiceHandlerImpl implements EchoServiceHandler"));
    assert!(scaffold.contains("return dispatcher.dispatch(envelope);"));
    assert!(!scaffold.contains("Implement this workload with @actrium/actr-workload"));
    assert!(!scaffold.contains("// - EchoService.Echo (EchoRequest -> EchoResponse)"));
    assert!(scaffold.contains("Remote RPC quick-start examples"));
    assert!(scaffold.contains("PingRequest.encode"));
    assert!(scaffold.contains("PingRequest.routeKey"));
    assert!(scaffold.contains("PingRequest.response.decode"));
    assert!(scaffold.contains(UNIMPLEMENTED_MARKER));
}

#[test]
fn local_workload_imports_imported_types_from_their_owner_module() {
    // A local `data_stream_app` service references `ask.*` message types
    // declared in `remote/ask-service/ask.proto`. The workload dispatcher must
    // import those types from the owner module (`./ask-service/ask_pb.js`),
    // not from the local service's proto stem.
    let module = LocalWorkloadModule {
        name: "data_stream_app_workload".to_string(),
        services: vec![LocalWorkloadService {
            name: "DataStreamAppService".to_string(),
            handler_interface: "DataStreamAppServiceHandler".to_string(),
            dispatcher_type: "DataStreamAppServiceDispatcher".to_string(),
            methods: vec![LocalWorkloadMethod {
                name: "ContinuePromptResultStreams".to_string(),
                handler_method_name: "continuePromptResultStreams".to_string(),
                input_type_short: "ContinuePromptResultStreamsRequest".to_string(),
                output_type_short: "ContinuePromptResultStreamsResponse".to_string(),
                route_key: "data_stream_app.DataStreamAppService.ContinuePromptResultStreams"
                    .to_string(),
                input_pb_import: "./ask-service/ask_pb.js".to_string(),
                output_pb_import: "./ask-service/ask_pb.js".to_string(),
            }],
        }],
    };

    let content = generate_local_workload_content(&module);

    assert!(
        content.contains("from './ask-service/ask_pb.js'"),
        "expected owner module import, got:\n{content}"
    );
    assert!(
        !content.contains("from './data_stream_app_pb.js'"),
        "must not pin imported types to the local service proto stem:\n{content}"
    );
    assert!(content.contains("ContinuePromptResultStreamsRequest"));
    assert!(content.contains("ContinuePromptResultStreamsResponseSchema"));
}

#[test]
fn user_scaffold_imports_imported_types_from_owner_module() {
    // The user-facing `actr_service.ts` scaffold must import an imported RPC
    // type from its declaring `_pb.js` module (owner-resolved), not from the
    // local service's proto module.
    let generator = TypeScriptGenerator;
    let scaffold = generator.generate_scaffold_content(&[BoundMethodInfo {
        generated_client_import: String::new(),
        generated_workload_import: "./generated/data_stream_app_workload.js".to_string(),
        service_name: "DataStreamAppService".to_string(),
        handler_interface: "DataStreamAppServiceHandler".to_string(),
        dispatcher_type: "DataStreamAppServiceDispatcher".to_string(),
        method_name: "ContinuePromptResultStreams".to_string(),
        handler_method_name: "continuePromptResultStreams".to_string(),
        input_type: "ContinuePromptResultStreamsRequest".to_string(),
        output_type: "ContinuePromptResultStreamsResponse".to_string(),
        input_type_short: "ContinuePromptResultStreamsRequest".to_string(),
        output_type_short: "ContinuePromptResultStreamsResponse".to_string(),
        input_pb_import: "./generated/ask-service/ask_pb.js".to_string(),
        output_pb_import: "./generated/ask-service/ask_pb.js".to_string(),
        request_companion: None,
        is_local: true,
    }]);

    assert!(
        scaffold.contains("from './generated/ask-service/ask_pb.js'"),
        "expected owner module import in user scaffold, got:\n{scaffold}"
    );
    assert!(
        !scaffold.contains("from './generated/data_stream_app_pb.js'"),
        "user scaffold must not pin imported types to the local service module:\n{scaffold}"
    );
}

#[test]
fn user_scaffold_imports_nested_local_types_from_owner_module() {
    assert_eq!(
        scaffold_proto_import_for("local/foo/bar/payload.proto"),
        "./generated/foo/bar/payload_pb.js"
    );
    assert_eq!(
        scaffold_proto_import_for("local/payload.proto"),
        "./generated/payload_pb.js"
    );

    let generator = TypeScriptGenerator;
    let scaffold = generator.generate_scaffold_content(&[BoundMethodInfo {
        generated_client_import: String::new(),
        generated_workload_import: "./generated/app_workload.js".to_string(),
        service_name: "AppService".to_string(),
        handler_interface: "AppServiceHandler".to_string(),
        dispatcher_type: "AppServiceDispatcher".to_string(),
        method_name: "UsePayload".to_string(),
        handler_method_name: "usePayload".to_string(),
        input_type: "PayloadRequest".to_string(),
        output_type: "PayloadResponse".to_string(),
        input_type_short: "PayloadRequest".to_string(),
        output_type_short: "PayloadResponse".to_string(),
        input_pb_import: scaffold_proto_import_for("local/foo/bar/payload.proto"),
        output_pb_import: scaffold_proto_import_for("local/foo/bar/payload.proto"),
        request_companion: None,
        is_local: true,
    }]);

    assert!(
        scaffold.contains("from './generated/foo/bar/payload_pb.js'"),
        "expected nested owner module import in user scaffold, got:\n{scaffold}"
    );
    assert!(
        !scaffold.contains("from './generated/payload_pb.js'"),
        "user scaffold must not flatten nested local owner modules:\n{scaffold}"
    );
}

#[test]
fn workload_import_path_only_strips_exact_source_marker() {
    assert_eq!(workload_pb_import_path("local/foo.proto"), "./foo_pb.js");
    assert_eq!(
        workload_pb_import_path("remote/ask-service/ask.proto"),
        "./ask-service/ask_pb.js"
    );
    assert_eq!(
        workload_pb_import_path("localization/foo.proto"),
        "./localization/foo_pb.js"
    );
    assert_eq!(
        workload_pb_import_path("remote-control/foo.proto"),
        "./remote-control/foo_pb.js"
    );
}

#[test]
fn workload_import_errors_on_unresolved_qualified_type() {
    let current_file = ProtoFileModel {
        proto_file: "local/client.proto".into(),
        relative_path: "local/client.proto".into(),
        package: "client".to_string(),
        side: ProtoSide::Local,
        declared_type_names: vec!["Request".to_string()],
        services: vec![],
    };
    let owner_index = TypeOwnerIndex::from_files(std::slice::from_ref(&current_file));

    let err = resolve_workload_pb_import("google.protobuf.Empty", &current_file, &owner_index)
        .expect_err("unresolved qualified type should not fall back to current pb module");

    assert!(
        err.to_string()
            .contains("Cannot resolve RPC type `google.protobuf.Empty`"),
        "unexpected error message: {err}"
    );
}

#[test]
fn typescript_plugin_extract_command_uses_destination_flag() {
    // Regression guard: `unzip` selects its destination directory with `-d`.
    // `-C` controls case-insensitive member matching, not the destination; a
    // `-C` here makes the extract path a member pattern and breaks extraction.
    let archive = Path::new("/tmp/actr-plugin.zip");
    let extract_dir = Path::new("/tmp/actr-extract");
    let args = unzip_extract_args(archive, extract_dir);

    let arg_strs: Vec<&str> = args.iter().filter_map(|a| a.to_str()).collect();

    assert!(
        arg_strs.contains(&"-d"),
        "unzip must select the destination with -d, got: {arg_strs:?}"
    );
    assert!(
        !arg_strs.contains(&"-C"),
        "unzip -C controls case-insensitive matching, not the destination, got: {arg_strs:?}"
    );
    assert!(arg_strs.contains(&"/tmp/actr-plugin.zip"));
    assert!(arg_strs.contains(&"/tmp/actr-extract"));
}
