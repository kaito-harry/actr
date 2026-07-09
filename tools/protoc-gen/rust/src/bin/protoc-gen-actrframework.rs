use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use heck::ToSnakeCase;
use prost::Message;
use prost_types::{
    DescriptorProto, FileDescriptorProto, ServiceDescriptorProto,
    compiler::{CodeGeneratorRequest, CodeGeneratorResponse, code_generator_response::File},
};
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, Read, Write};

use actr_framework_protoc_codegen::{GeneratorRole, ModernGenerator, RemoteServiceInfo};
use actr_protocol::{ActrType, PackageName, ServiceName};

/// Proto source type enum — simplified design for compile-time routing
#[derive(Debug, Clone, PartialEq)]
pub enum ProtoSource {
    /// Local service (from proto/ directory)
    Local,
    /// Remote service (from actr.toml [dependencies])
    Remote,
}

#[derive(Debug, Clone)]
struct TypeOwner {
    proto_package: String,
    proto_file: String,
    type_name: String,
}

impl ProtoSource {
    /// Infer source type from proto file
    ///
    /// The proper way is to use the `LocalFiles` and `RemoteFiles` parameters passed
    /// by the CLI. If not present or ambiguous, fallback to checking whether it has services.
    ///
    /// A file must belong to exactly one side: either LocalFiles or RemoteFiles, never both.
    pub fn from_proto_file(
        file: &FileDescriptorProto,
        params: &HashMap<String, String>,
    ) -> Result<Self> {
        let file_name = normalize_proto_path(file.name());
        let path_ends_with = |path: &str, suffix: &str| {
            path == suffix
                || path
                    .strip_suffix(suffix)
                    .is_some_and(|prefix| prefix.ends_with('/'))
        };

        let matches = |list_str: &str| {
            list_str.split(':').filter(|p| !p.is_empty()).any(|p| {
                let candidate = normalize_proto_path(p);
                path_ends_with(&file_name, &candidate) || path_ends_with(&candidate, &file_name)
            })
        };

        let in_remote = params.get("RemoteFiles").is_some_and(|s| matches(s));
        let in_local = params.get("LocalFiles").is_some_and(|s| matches(s));

        if in_remote && in_local {
            return Err(anyhow!(
                "{}: appears in both RemoteFiles and LocalFiles; a file must belong to exactly one side.",
                file_name
            ));
        }

        if in_remote {
            return Ok(Self::Remote);
        }
        if in_local {
            return Ok(Self::Local);
        }

        // Fallback: Assume Local if it has services, otherwise Remote
        let has_services = !file.service.is_empty();
        Ok(if has_services {
            Self::Local
        } else {
            Self::Remote
        })
    }
}

#[derive(Serialize)]
struct ActrGenMetadata {
    plugin_version: String,
    language: &'static str,
    local_services: Vec<LocalServiceMetadata>,
    remote_services: Vec<RemoteServiceMetadata>,
}

#[derive(Serialize)]
struct LocalServiceMetadata {
    name: String,
    package: String,
    proto_file: String,
    handler_interface: String,
    workload_type: String,
    dispatcher_type: String,
    methods: Vec<MethodMetadata>,
}

#[derive(Serialize)]
struct RemoteServiceMetadata {
    name: String,
    package: String,
    proto_file: String,
    actr_type: String,
    client_type: String,
    methods: Vec<MethodMetadata>,
}

#[derive(Serialize)]
struct MethodMetadata {
    name: String,
    snake_name: String,
    route_key: String,
    input_ref: TypeRef,
    output_ref: TypeRef,
}

#[derive(Serialize)]
struct TypeRef {
    proto_type: String,
    type_name: String,
    proto_package: String,
    proto_file: String,
}

fn main() -> Result<()> {
    // Support --version and --help arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() > 1 {
        match args[1].as_str() {
            "--version" | "-V" => {
                println!("protoc-gen-actrframework {}", env!("CARGO_PKG_VERSION"));
                println!(
                    "actr-framework-protoc-codegen library version: {}",
                    env!("CARGO_PKG_VERSION")
                );
                return Ok(());
            }
            "--help" | "-h" => {
                println!("protoc-gen-actrframework - Protobuf plugin for Actrium ACTR framework");
                println!();
                println!("USAGE:");
                println!(
                    "    As protoc plugin: protoc --plugin=protoc-gen-actrframework=PATH --actrframework_out=OUT_DIR input.proto"
                );
                println!("    Version info:     protoc-gen-actrframework --version");
                println!();
                println!("VERSION:");
                println!("    {}", env!("CARGO_PKG_VERSION"));
                return Ok(());
            }
            _ => {}
        }
    }

    // Read CodeGeneratorRequest from stdin
    let mut stdin = io::stdin();
    let mut buf = Vec::new();
    stdin
        .read_to_end(&mut buf)
        .context("Failed to read from stdin")?;

    let request = CodeGeneratorRequest::decode(Bytes::from(buf))
        .context("Failed to decode CodeGeneratorRequest")?;

    // Generate code
    let response = generate_code(request)?;

    // Write CodeGeneratorResponse to stdout
    let mut out_buf = Vec::new();
    response
        .encode(&mut out_buf)
        .context("Failed to encode CodeGeneratorResponse")?;

    io::stdout()
        .write_all(&out_buf)
        .context("Failed to write to stdout")?;

    Ok(())
}

/// Parse parameters from protoc --actrframework_opt
/// Format: key1=value1,key2=value2
fn parse_parameters(param_str: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for pair in param_str.split(',') {
        if let Some((key, value)) = pair.split_once('=') {
            params.insert(key.trim().to_string(), value.trim().to_string());
        }
    }
    params
}

fn generate_code(request: CodeGeneratorRequest) -> Result<CodeGeneratorResponse> {
    // Set supported features if available (FEATURE_PROTO3_OPTIONAL = 1)
    let mut response = CodeGeneratorResponse {
        supported_features: Some(1u64),
        ..Default::default()
    };

    // Parse parameters from --actrframework_opt
    let params = parse_parameters(request.parameter.as_deref().unwrap_or(""));

    // Parse RemoteFileActrTypes parameter: file1=actr_type1;file2=actr_type2
    let mut remote_file_to_actr_type: HashMap<String, String> = HashMap::new();
    if let Some(remote_file_actr_types) = params.get("RemoteFileActrTypes") {
        for mapping in remote_file_actr_types.split(';') {
            if let Some((file, actr_type)) = mapping.split_once('=') {
                remote_file_to_actr_type.insert(
                    normalize_proto_path(file.trim()),
                    actr_type.trim().to_string(),
                );
            }
        }
    }

    // Build fully-qualified message name -> declaring proto package map so
    // imported RPC message types resolve to their declaring module instead of
    // the current service's package.
    let mut type_owners: HashMap<String, TypeOwner> = HashMap::new();
    for file in &request.proto_file {
        collect_type_owners(file, &mut type_owners, file.package());
    }

    // Collect all remote services information
    let mut remote_services = Vec::new();
    let mut metadata = ActrGenMetadata {
        plugin_version: env!("CARGO_PKG_VERSION").to_string(),
        language: "rust",
        local_services: Vec::new(),
        remote_services: Vec::new(),
    };
    for file in &request.proto_file {
        let proto_source = ProtoSource::from_proto_file(file, &params)?;
        for service in &file.service {
            let package_name = file.package().to_string();
            let service_name = service.name().to_string();
            let actr_type = remote_file_to_actr_type
                .get(&normalize_proto_path(file.name()))
                .cloned()
                .unwrap_or_else(|| {
                    let manufacturer = params
                        .get("manufacturer")
                        .map(|s| s.as_str())
                        .unwrap_or(&package_name);
                    ActrType {
                        manufacturer: manufacturer.to_string(),
                        name: service_name.clone(),
                        version: "1.0.0".to_string(),
                    }
                    .to_string_repr()
                });

            if proto_source == ProtoSource::Remote {
                let methods: Vec<String> = service
                    .method
                    .iter()
                    .map(|m| m.name().to_string())
                    .collect();
                remote_services.push(RemoteServiceInfo {
                    package_name: package_name.clone(),
                    service_name: service_name.clone(),
                    methods,
                    actr_type: actr_type.clone(),
                });
                metadata.remote_services.push(build_remote_service_metadata(
                    file,
                    service,
                    actr_type,
                    &type_owners,
                )?);
            } else {
                metadata.local_services.push(build_local_service_metadata(
                    file,
                    service,
                    &type_owners,
                )?);
            }
        }
    }

    // Process services for each file to generate
    for file_name in &request.file_to_generate {
        if let Some(file) = request.proto_file.iter().find(|f| f.name() == file_name) {
            if file.service.len() > 1 {
                return Err(anyhow!(
                    "{}: defines {} services, but only one service per .proto file is supported. \
                     Split each service into its own .proto file.",
                    file_name,
                    file.service.len()
                ));
            }
            for service in &file.service {
                let generated_file =
                    generate_service_code(file, service, &type_owners, &params, &remote_services)?;
                response.file.push(generated_file);
            }
        }
    }

    response.file.push(File {
        name: Some("actr-gen-meta.json".to_string()),
        content: Some(serde_json::to_string_pretty(&metadata)?),
        ..Default::default()
    });

    Ok(response)
}

fn collect_type_owners(
    file: &FileDescriptorProto,
    owners: &mut HashMap<String, TypeOwner>,
    package: &str,
) {
    for message in &file.message_type {
        collect_message_type_owner(message, owners, package, file.name(), "");
    }
}

fn collect_message_type_owner(
    message: &DescriptorProto,
    owners: &mut HashMap<String, TypeOwner>,
    package: &str,
    proto_file: &str,
    parent_type: &str,
) {
    let type_name = if parent_type.is_empty() {
        message.name().to_string()
    } else {
        format!("{}.{}", parent_type, message.name())
    };
    insert_type_owner(owners, package, proto_file, &type_name);

    for nested in &message.nested_type {
        collect_message_type_owner(nested, owners, package, proto_file, &type_name);
    }
}

fn insert_type_owner(
    owners: &mut HashMap<String, TypeOwner>,
    package: &str,
    proto_file: &str,
    type_name: &str,
) {
    let full_name = if package.is_empty() {
        type_name.to_string()
    } else {
        format!("{package}.{type_name}")
    };
    owners.insert(
        full_name,
        TypeOwner {
            proto_package: package.to_string(),
            proto_file: normalize_proto_path(proto_file),
            type_name: type_name.to_string(),
        },
    );
}

fn generate_service_code(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    type_owners: &HashMap<String, TypeOwner>,
    params: &HashMap<String, String>,
    remote_services: &[RemoteServiceInfo],
) -> Result<File> {
    let service_name = service.name();
    let package_name = file.package();

    // Validate proto package name early to surface clear errors
    PackageName::new(package_name.to_string())
        .map_err(|e| anyhow!("Invalid proto package name '{}': {}", package_name, e))?;
    // Validate proto service name early
    ServiceName::new(service_name.to_string())
        .map_err(|e| anyhow!("Invalid proto service name '{}': {}", service_name, e))?;

    // Determine proto source based on proto file characteristics and passed params
    let proto_source = ProtoSource::from_proto_file(file, params)?;

    // Extract the proto file stem (e.g., "echo.proto" -> "echo")
    let file_stem = std::path::Path::new(file.name())
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(service_name)
        .to_snake_case();

    // Use the modern code generator
    let role = match proto_source {
        ProtoSource::Local => GeneratorRole::ServerSide,
        ProtoSource::Remote => GeneratorRole::ClientSide,
    };

    let generator_type_owners = type_owners
        .iter()
        .map(|(full_name, owner)| (full_name.clone(), owner.proto_package.clone()))
        .collect();
    let generator = ModernGenerator::new(package_name, service_name, role, generator_type_owners);

    // Pass remote_services only for ServerSide generation
    let final_code = if role == GeneratorRole::ServerSide {
        generator.generate_with_remotes(&service.method, remote_services)?
    } else {
        generator.generate(&service.method)?
    };

    // Generate different file suffixes based on role
    let file_suffix = match role {
        GeneratorRole::ServerSide => "_actor",
        GeneratorRole::ClientSide => "_client",
    };

    Ok(File {
        name: Some(format!("{}{}.rs", file_stem, file_suffix)),
        content: Some(final_code),
        insertion_point: None,
        generated_code_info: None,
    })
}

fn build_local_service_metadata(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    type_owners: &HashMap<String, TypeOwner>,
) -> Result<LocalServiceMetadata> {
    Ok(LocalServiceMetadata {
        name: service.name().to_string(),
        package: file.package().to_string(),
        proto_file: normalize_proto_path(file.name()),
        handler_interface: format!("{}Handler", service.name()),
        workload_type: format!("{}Workload", service.name()),
        dispatcher_type: format!("{}Dispatcher", service.name()),
        methods: service
            .method
            .iter()
            .map(|method| build_method_metadata(file, service, method, type_owners))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn build_remote_service_metadata(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    actr_type: String,
    type_owners: &HashMap<String, TypeOwner>,
) -> Result<RemoteServiceMetadata> {
    Ok(RemoteServiceMetadata {
        name: service.name().to_string(),
        package: file.package().to_string(),
        proto_file: normalize_proto_path(file.name()),
        actr_type,
        client_type: format!("{}Client", service.name()),
        methods: service
            .method
            .iter()
            .map(|method| build_method_metadata(file, service, method, type_owners))
            .collect::<Result<Vec<_>>>()?,
    })
}

fn build_method_metadata(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    method: &prost_types::MethodDescriptorProto,
    type_owners: &HashMap<String, TypeOwner>,
) -> Result<MethodMetadata> {
    let package = file.package();
    let route_key = if package.is_empty() {
        format!("{}.{}", service.name(), method.name())
    } else {
        format!("{}.{}.{}", package, service.name(), method.name())
    };
    let input_ref = build_type_ref(
        file,
        service,
        method,
        "input",
        method.input_type(),
        type_owners,
    )?;
    let output_ref = build_type_ref(
        file,
        service,
        method,
        "output",
        method.output_type(),
        type_owners,
    )?;

    Ok(MethodMetadata {
        name: method.name().to_string(),
        snake_name: method.name().to_snake_case(),
        route_key,
        input_ref,
        output_ref,
    })
}

fn build_type_ref(
    file: &FileDescriptorProto,
    service: &ServiceDescriptorProto,
    method: &prost_types::MethodDescriptorProto,
    kind: &str,
    raw: &str,
    type_owners: &HashMap<String, TypeOwner>,
) -> Result<TypeRef> {
    let proto_type = normalize_type_name(raw);
    if let Some(owner) = type_owners.get(&proto_type) {
        return Ok(TypeRef {
            proto_type,
            type_name: owner.type_name.clone(),
            proto_package: owner.proto_package.clone(),
            proto_file: owner.proto_file.clone(),
        });
    }

    Err(anyhow!(
        "Cannot resolve {} type `{}` for {}.{} in {}: RPC types must be declared in one of the parsed proto files",
        kind,
        proto_type,
        service.name(),
        method.name(),
        normalize_proto_path(file.name())
    ))
}

fn normalize_type_name(raw: &str) -> String {
    raw.trim_start_matches('.').to_string()
}

fn normalize_proto_path(raw: &str) -> String {
    let normalized = raw.replace('\\', "/");
    let mut path = normalized.as_str();
    while let Some(stripped) = path.strip_prefix("./") {
        path = stripped;
    }
    if path.ends_with(".proto") {
        path.to_string()
    } else {
        format!("{path}.proto")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_types::MethodDescriptorProto;

    #[test]
    fn generate_code_uses_remote_file_actr_types_for_local_bridge() {
        let request = CodeGeneratorRequest {
            file_to_generate: vec!["local.proto".to_string(), "remote/echo.proto".to_string()],
            parameter: Some(
                "manufacturer=acme,LocalFiles=local.proto,RemoteFiles=remote/echo.proto,RemoteFileActrTypes=remote/echo.proto=custom:EchoAlias:1.0.0"
                    .to_string(),
            ),
            proto_file: vec![
                FileDescriptorProto {
                    name: Some("local.proto".to_string()),
                    package: Some("demo".to_string()),
                    service: vec![ServiceDescriptorProto {
                        name: Some("DemoClientApp".to_string()),
                        method: vec![],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                FileDescriptorProto {
                    name: Some("remote/echo.proto".to_string()),
                    package: Some("echo".to_string()),
                    message_type: vec![
                        DescriptorProto {
                            name: Some("EchoRequest".to_string()),
                            ..Default::default()
                        },
                        DescriptorProto {
                            name: Some("EchoResponse".to_string()),
                            ..Default::default()
                        },
                    ],
                    service: vec![ServiceDescriptorProto {
                        name: Some("EchoService".to_string()),
                        method: vec![MethodDescriptorProto {
                            name: Some("Echo".to_string()),
                            input_type: Some(".echo.EchoRequest".to_string()),
                            output_type: Some(".echo.EchoResponse".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let response = generate_code(request).unwrap();
        let local_actor = response
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("local_actor.rs"))
            .and_then(|file| file.content.as_ref())
            .expect("local_actor.rs should be generated");

        assert!(local_actor.contains("pub trait DemoClientAppHandler"));
        assert!(local_actor.contains("\"echo.EchoService.Echo\""));
        assert!(local_actor.contains("manufacturer"));
        assert!(local_actor.contains("\"custom\""));
        assert!(local_actor.contains("name"));
        assert!(local_actor.contains("\"EchoAlias\""));
    }

    #[test]
    fn generate_code_metadata_preserves_imported_rpc_type_owner() {
        let request = CodeGeneratorRequest {
            file_to_generate: vec!["local/data_stream_app.proto".to_string()],
            parameter: Some(
                "manufacturer=acme,LocalFiles=local/data_stream_app.proto,RemoteFiles=remote/ask-service/ask.proto"
                    .to_string(),
            ),
            proto_file: vec![
                FileDescriptorProto {
                    name: Some("local/data_stream_app.proto".to_string()),
                    package: Some("data_stream_app".to_string()),
                    service: vec![ServiceDescriptorProto {
                        name: Some("DataStreamAppService".to_string()),
                        method: vec![MethodDescriptorProto {
                            name: Some("ContinuePromptResultStreams".to_string()),
                            input_type: Some(
                                ".ask.ContinuePromptResultStreamsRequest".to_string(),
                            ),
                            output_type: Some(
                                ".ask.ContinuePromptResultStreamsResponse".to_string(),
                            ),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                FileDescriptorProto {
                    name: Some("remote/ask-service/ask.proto".to_string()),
                    package: Some("ask".to_string()),
                    message_type: vec![
                        DescriptorProto {
                            name: Some("ContinuePromptResultStreamsRequest".to_string()),
                            ..Default::default()
                        },
                        DescriptorProto {
                            name: Some("ContinuePromptResultStreamsResponse".to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let response = generate_code(request).unwrap();
        let metadata = response
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("actr-gen-meta.json"))
            .and_then(|file| file.content.as_ref())
            .expect("actr-gen-meta.json should be generated");
        let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
        let method = &metadata["local_services"][0]["methods"][0];

        assert_eq!(
            method["input_ref"]["type_name"],
            "ContinuePromptResultStreamsRequest"
        );
        assert_eq!(
            method["input_ref"]["proto_type"],
            "ask.ContinuePromptResultStreamsRequest"
        );
        assert_eq!(method["input_ref"]["proto_package"], "ask");
        assert_eq!(
            method["input_ref"]["proto_file"],
            "remote/ask-service/ask.proto"
        );
        assert_eq!(
            method["output_ref"]["proto_type"],
            "ask.ContinuePromptResultStreamsResponse"
        );
        assert_eq!(method["output_ref"]["proto_package"], "ask");
    }

    #[test]
    fn generate_code_metadata_preserves_nested_imported_rpc_type_owner() {
        let request = CodeGeneratorRequest {
            file_to_generate: vec!["local/data_stream_app.proto".to_string()],
            parameter: Some(
                "manufacturer=acme,LocalFiles=local/data_stream_app.proto,RemoteFiles=remote/ask-service/ask.proto"
                    .to_string(),
            ),
            proto_file: vec![
                FileDescriptorProto {
                    name: Some("local/data_stream_app.proto".to_string()),
                    package: Some("data_stream_app".to_string()),
                    service: vec![ServiceDescriptorProto {
                        name: Some("DataStreamAppService".to_string()),
                        method: vec![MethodDescriptorProto {
                            name: Some("ContinuePromptResultStreams".to_string()),
                            input_type: Some(".ask.Outer.InnerRequest".to_string()),
                            output_type: Some(".ask.Outer.InnerResponse".to_string()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                FileDescriptorProto {
                    name: Some("remote/ask-service/ask.proto".to_string()),
                    package: Some("ask".to_string()),
                    message_type: vec![DescriptorProto {
                        name: Some("Outer".to_string()),
                        nested_type: vec![
                            DescriptorProto {
                                name: Some("InnerRequest".to_string()),
                                ..Default::default()
                            },
                            DescriptorProto {
                                name: Some("InnerResponse".to_string()),
                                ..Default::default()
                            },
                        ],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let response = generate_code(request).unwrap();
        let metadata = response
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("actr-gen-meta.json"))
            .and_then(|file| file.content.as_ref())
            .expect("actr-gen-meta.json should be generated");
        let metadata: serde_json::Value = serde_json::from_str(metadata).unwrap();
        let method = &metadata["local_services"][0]["methods"][0];

        assert_eq!(method["input_ref"]["proto_type"], "ask.Outer.InnerRequest");
        assert_eq!(method["input_ref"]["type_name"], "Outer.InnerRequest");
        assert_eq!(method["input_ref"]["proto_package"], "ask");
        assert_eq!(
            method["input_ref"]["proto_file"],
            "remote/ask-service/ask.proto"
        );
        assert_eq!(
            method["output_ref"]["proto_type"],
            "ask.Outer.InnerResponse"
        );
        assert_eq!(method["output_ref"]["type_name"], "Outer.InnerResponse");
    }

    #[test]
    fn generate_code_metadata_errors_on_unresolved_qualified_rpc_type() {
        let request = CodeGeneratorRequest {
            file_to_generate: vec!["local/client.proto".to_string()],
            parameter: Some("manufacturer=acme,LocalFiles=local/client.proto".to_string()),
            proto_file: vec![FileDescriptorProto {
                name: Some("local/client.proto".to_string()),
                package: Some("client".to_string()),
                service: vec![ServiceDescriptorProto {
                    name: Some("Client".to_string()),
                    method: vec![MethodDescriptorProto {
                        name: Some("Ping".to_string()),
                        input_type: Some(".google.protobuf.Empty".to_string()),
                        output_type: Some(".google.protobuf.Empty".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = generate_code(request).expect_err(
            "unresolved qualified RPC type should not fall back to current metadata owner",
        );
        let message = err.to_string();
        assert!(
            message.contains("Cannot resolve input type `google.protobuf.Empty`"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn generate_code_metadata_errors_on_unresolved_unqualified_rpc_type() {
        let request = CodeGeneratorRequest {
            file_to_generate: vec!["local/client.proto".to_string()],
            parameter: Some("manufacturer=acme,LocalFiles=local/client.proto".to_string()),
            proto_file: vec![FileDescriptorProto {
                name: Some("local/client.proto".to_string()),
                package: Some("client".to_string()),
                service: vec![ServiceDescriptorProto {
                    name: Some("Client".to_string()),
                    method: vec![MethodDescriptorProto {
                        name: Some("Ping".to_string()),
                        input_type: Some("MissingRequest".to_string()),
                        output_type: Some("MissingResponse".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let err = generate_code(request).expect_err(
            "unresolved unqualified RPC type should not fall back to current metadata owner",
        );
        let message = err.to_string();
        assert!(
            message.contains("Cannot resolve input type `MissingRequest`"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn collect_type_owners_only_indexes_rpc_message_types() {
        let file = FileDescriptorProto {
            name: Some("ask.proto".to_string()),
            package: Some("ask".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("Request".to_string()),
                enum_type: vec![prost_types::EnumDescriptorProto {
                    name: Some("State".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            enum_type: vec![prost_types::EnumDescriptorProto {
                name: Some("Status".to_string()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut owners = HashMap::new();
        collect_type_owners(&file, &mut owners, "ask");
        assert!(owners.contains_key("ask.Request"));
        assert!(!owners.contains_key("ask.Status"));
        assert!(!owners.contains_key("ask.Request.State"));
    }

    #[test]
    fn normalize_proto_path_is_cross_platform_and_stable() {
        assert_eq!(normalize_proto_path(r".\remote\ask"), "remote/ask.proto");
        assert_eq!(
            normalize_proto_path("./remote/ask.proto"),
            "remote/ask.proto"
        );
    }
}
