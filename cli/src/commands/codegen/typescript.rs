use crate::commands::codegen::proto_model::{ProtoFileModel, TypeOwnerIndex};
use crate::commands::codegen::scaffold::{ScaffoldCatalog, ScaffoldService};
use crate::commands::codegen::traits::{GenContext, LanguageGenerator};
use crate::error::{ActrCliError, Result};
use crate::utils::command_exists;
use actr_config::LockFile;
use actr_protocol::ActrType;
use async_trait::async_trait;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

const PROTOC: &str = "protoc";
const NODE: &str = "node";
const NPX: &str = "npx";
const PROTOC_GEN_ES: &str = "protoc-gen-es";
const EXPECTED_PROTOC_GEN_ES_VERSION: &str = "2.11.0";
const PLUGIN_NAME: &str = "protoc-gen-actrframework-typescript";
const GITHUB_RELEASE_URL_TEMPLATE: &str =
    "https://github.com/Actrium/actr/releases/download/v{}/protoc-gen-actrframework-typescript.zip";
const IMPLEMENTED_MARKER: &str =
    "// ActrService is Implemented: This file contains a complete implementation.";
const UNIMPLEMENTED_MARKER: &str =
    "// ActrService is not implemented: Generated quick-start scaffold.";
const SCAFFOLD_HINT: &str = "// Replace scaffold sections with your business logic";

#[derive(Debug, Clone)]
struct ProtoModuleInfo {
    proto_stem: String,
    is_local: bool,
    generated_client_path: PathBuf,
    generated_client_import: String,
    services: Vec<ProtoServiceInfo>,
}

#[derive(Debug, Clone)]
struct ProtoServiceInfo {
    name: String,
    handler_interface: String,
    dispatcher_type: String,
    generated_workload_import: String,
    methods: Vec<ProtoMethodInfo>,
}

#[derive(Debug, Clone)]
struct ProtoMethodInfo {
    name: String,
    handler_method_name: String,
    input_type: String,
    output_type: String,
    input_type_short: String,
    output_type_short: String,
    /// Scaffold-relative `_pb.js` import path for the request type's declaring
    /// proto file, so imported types resolve to their real owner module.
    input_pb_import: String,
    output_pb_import: String,
}

#[derive(Debug, Clone, Default)]
struct GeneratedClientApi {
    exported_consts: HashSet<String>,
}

#[derive(Debug, Clone)]
struct BoundMethodInfo {
    generated_client_import: String,
    generated_workload_import: String,
    service_name: String,
    handler_interface: String,
    dispatcher_type: String,
    method_name: String,
    handler_method_name: String,
    input_type: String,
    output_type: String,
    input_type_short: String,
    output_type_short: String,
    input_pb_import: String,
    output_pb_import: String,
    request_companion: Option<String>,
    is_local: bool,
}

#[derive(Debug, Clone)]
struct LocalWorkloadModule {
    name: String,
    services: Vec<LocalWorkloadService>,
}

#[derive(Debug, Clone)]
struct LocalWorkloadService {
    name: String,
    handler_interface: String,
    dispatcher_type: String,
    methods: Vec<LocalWorkloadMethod>,
}

#[derive(Debug, Clone)]
struct LocalWorkloadMethod {
    name: String,
    handler_method_name: String,
    input_type_short: String,
    output_type_short: String,
    route_key: String,
    /// Workload-relative import path for the request type's generated `_pb.js`
    /// module (e.g. `./data_stream_app_pb.js` or `./ask-service/ask_pb.js`),
    /// resolved from the declaring proto file so imported types are imported
    /// from their real owner module.
    input_pb_import: String,
    /// Workload-relative import path for the response type's `_pb.js` module.
    output_pb_import: String,
}

type ServiceImportKey<'a> = (&'a str, &'a str);
type ServiceImportGroups = (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>);

pub struct TypeScriptGenerator;

#[async_trait]
impl LanguageGenerator for TypeScriptGenerator {
    async fn generate_infrastructure(&self, context: &GenContext) -> Result<Vec<PathBuf>> {
        info!("🔧 Generating TypeScript infrastructure code...");
        self.ensure_required_tools()?;

        let es_plugin_path = self.ensure_protoc_gen_es(context)?;

        std::fs::create_dir_all(&context.output).map_err(|e| {
            ActrCliError::config_error(format!("Failed to create output directory: {e}"))
        })?;

        let proto_root = if context.input_path.is_file() {
            context
                .input_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf()
        } else {
            context.input_path.clone()
        };

        let mut local_files = Vec::new();
        let mut remote_files = Vec::new();

        for proto_file in &context.proto_files {
            let rel = proto_file.strip_prefix(&proto_root).unwrap_or(proto_file);
            let rel_norm = normalize_proto_path(rel);
            if rel_norm.starts_with("remote/") {
                remote_files.push(rel_norm);
            } else {
                local_files.push(rel_norm);
            }
        }

        let remote_mapping = self.build_remote_mapping(context, &remote_files)?;

        let mut proto_inputs = Vec::new();
        for proto_file in &context.proto_files {
            let rel = proto_file.strip_prefix(&proto_root).unwrap_or(proto_file);
            proto_inputs.push(normalize_proto_path(rel));
        }

        let mut es_cmd = StdCommand::new(PROTOC);
        es_cmd
            .arg(format!("--proto_path={}", proto_root.display()))
            .arg(format!(
                "--plugin=protoc-gen-es={}",
                es_plugin_path.display()
            ))
            .arg("--es_opt=target=ts")
            .arg(format!("--es_out={}", context.output.display()));

        for proto_input in &proto_inputs {
            es_cmd.arg(proto_input);
        }

        debug!("Executing protoc (es): {:?}", es_cmd);
        let output = es_cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to execute protoc (es): {e}"))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "protoc (es) execution failed: {stderr}"
            )));
        }

        let mut options = Vec::new();
        options.push("target=ts".to_string());
        if !local_files.is_empty() {
            options.push(format!("LocalFiles={}", local_files.join(":")));
        }
        if !remote_files.is_empty() {
            options.push(format!("RemoteFiles={}", remote_files.join(":")));
        }
        if !remote_mapping.is_empty() {
            let mut sorted_mapping: Vec<String> = remote_mapping
                .iter()
                .map(|(path, actr_type)| format!("{path}={actr_type}"))
                .collect();
            sorted_mapping.sort();
            options.push(format!("RemoteFileMapping={}", sorted_mapping.join(";")));
        }
        let option_str = options.join(",");

        if !proto_inputs.is_empty() {
            let plugin_path = self.ensure_typescript_plugin()?;
            let mut cmd = StdCommand::new(PROTOC);
            cmd.arg(format!("--proto_path={}", proto_root.display()))
                .arg(format!(
                    "--plugin=protoc-gen-actrframework-typescript={}",
                    plugin_path.display()
                ))
                .arg(format!("--actrframework-typescript_opt={option_str}"))
                .arg(format!(
                    "--actrframework-typescript_out={}",
                    context.output.display()
                ));

            for proto_input in &proto_inputs {
                cmd.arg(proto_input);
            }

            debug!("Executing protoc (actrframework-typescript): {:?}", cmd);
            let output = cmd.output().map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to execute protoc (actrframework-typescript): {e}"
                ))
            })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(ActrCliError::command_error(format!(
                    "protoc (actrframework-typescript) execution failed: {stderr}"
                )));
            }
        }

        debug!(
            "TypeScript protoc inputs: {}",
            if proto_inputs.is_empty() {
                "<none>".to_string()
            } else {
                proto_inputs.join(", ")
            }
        );

        flatten_local_and_lift_remote(&context.output)?;
        self.generate_local_workload_files(context)?;

        let generated_files = collect_ts_files(&context.output);
        info!("✅ Generated {} TypeScript files", generated_files.len());
        Ok(generated_files)
    }

    async fn generate_scaffold(
        &self,
        context: &GenContext,
        catalog: &ScaffoldCatalog,
    ) -> Result<Vec<PathBuf>> {
        info!("📝 Generating TypeScript user code scaffold...");

        let mut scaffold_files = Vec::new();
        let target_path = context
            .output
            .parent()
            .unwrap_or(&context.output)
            .join("actr_service.ts");

        if target_path.exists() {
            let is_scaffold = self.should_overwrite_scaffold(&target_path)?;
            if is_scaffold {
                info!("🔄 Overwriting scaffold file: {}", target_path.display());
            } else if !context.overwrite_user_code {
                info!(
                    "⏭️  Skipping existing user code file: {}",
                    target_path.display()
                );
                info!("   Use --overwrite-user-code to regenerate the scaffold.");
                return Ok(scaffold_files);
            } else {
                info!(
                    "🔄 Overwriting existing file (--overwrite-user-code): {}",
                    target_path.display()
                );
            }
        }

        let modules = self.collect_proto_modules(context, catalog)?;
        let bound_methods = self.bind_methods(&modules)?;
        let scaffold_content = self.generate_scaffold_content(&bound_methods);

        std::fs::write(&target_path, scaffold_content).map_err(|e| {
            ActrCliError::config_error(format!("Failed to write TypeScript scaffold: {e}"))
        })?;

        info!("📄 Generated user code scaffold: {}", target_path.display());
        scaffold_files.push(target_path);
        info!("✅ User code scaffold generation completed");
        Ok(scaffold_files)
    }

    async fn format_code(&self, _context: &GenContext, files: &[PathBuf]) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        if !command_exists("prettier") {
            info!("💡 prettier not found, skipping TypeScript formatting");
            return Ok(());
        }

        for file in files {
            let output = StdCommand::new("prettier")
                .arg("--write")
                .arg(file)
                .output();
            if let Err(e) = output {
                warn!("Failed to run prettier for {}: {}", file.display(), e);
            }
        }

        info!("✅ TypeScript formatting completed");
        Ok(())
    }

    async fn validate_code(&self, context: &GenContext) -> Result<()> {
        let ts_files = collect_ts_files(&context.output);
        if ts_files.is_empty() {
            return Err(ActrCliError::config_error(
                "No TypeScript files were generated",
            ));
        }

        info!(
            "🔍 TypeScript validation: generated {} files",
            ts_files.len()
        );
        Ok(())
    }

    fn print_next_steps(&self, context: &GenContext) {
        println!("\n🎉 TypeScript code generation completed!");
        println!(
            "1. 📖 Review generated files in {}",
            context.output.display()
        );
        println!("2. 📦 Ensure `actr deps install` has been executed in project root");
        println!("3. ▶️  Run your app with `npm run dev`");
    }
}

impl TypeScriptGenerator {
    fn collect_proto_modules(
        &self,
        context: &GenContext,
        catalog: &ScaffoldCatalog,
    ) -> Result<Vec<ProtoModuleInfo>> {
        let mut services_by_file: HashMap<String, Vec<ProtoServiceInfo>> = HashMap::new();

        for service in catalog
            .local_services
            .iter()
            .cloned()
            .chain(catalog.remote_services.iter().cloned())
        {
            services_by_file
                .entry(normalize_proto_lookup_key(&service.proto_file))
                .or_default()
                .push(self.to_proto_service_info(service));
        }

        let mut modules = Vec::new();
        for file in &context.proto_model.files {
            let relative_norm = normalize_proto_lookup_key(&file.relative_path);
            let relative_parts = file
                .relative_path
                .components()
                .filter_map(|component| component.as_os_str().to_str())
                .map(str::to_string)
                .collect::<Vec<_>>();
            let is_local = !relative_norm.starts_with("remote/");
            let proto_stem = file
                .proto_file
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("proto")
                .to_string();
            let (generated_client_path, generated_client_import) = self
                .derive_generated_client_location(
                    &context.output,
                    &relative_parts,
                    &proto_stem,
                    is_local,
                );

            modules.push(ProtoModuleInfo {
                proto_stem,
                is_local,
                generated_client_path,
                generated_client_import,
                services: services_by_file.remove(&relative_norm).unwrap_or_default(),
            });
        }

        Ok(modules)
    }

    fn to_proto_service_info(&self, service: ScaffoldService) -> ProtoServiceInfo {
        let generated_workload_import = format!(
            "./generated/{}.js",
            workload_module_name(&service.package, &service.name)
        );
        ProtoServiceInfo {
            handler_interface: service
                .handler_interface
                .clone()
                .unwrap_or_else(|| format!("{}Handler", service.name)),
            dispatcher_type: service
                .dispatcher_type
                .clone()
                .unwrap_or_else(|| format!("{}Dispatcher", service.name)),
            generated_workload_import,
            name: service.name,
            methods: service
                .methods
                .into_iter()
                .map(|method| ProtoMethodInfo {
                    handler_method_name: snake_to_camel_case(&method.snake_name),
                    name: method.name,
                    input_type_short: short_proto_type(&method.input_type),
                    output_type_short: short_proto_type(&method.output_type),
                    input_type: method.input_type,
                    output_type: method.output_type,
                    input_pb_import: scaffold_proto_import_for(&method.input_ref.proto_file),
                    output_pb_import: scaffold_proto_import_for(&method.output_ref.proto_file),
                })
                .collect(),
        }
    }

    #[allow(dead_code)]
    fn parse_proto_module(
        &self,
        proto_path: &Path,
        relative_path: &Path,
        output: &Path,
    ) -> Result<ProtoModuleInfo> {
        let content = std::fs::read_to_string(proto_path).map_err(|e| {
            ActrCliError::config_error(format!(
                "Failed to read proto file {}: {e}",
                proto_path.display()
            ))
        })?;
        let relative_parts = relative_path
            .components()
            .filter_map(|component| component.as_os_str().to_str())
            .map(str::to_string)
            .collect::<Vec<_>>();
        let is_local = relative_parts.first().is_none_or(|part| part != "remote");
        let proto_stem = proto_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("proto")
            .to_string();
        let (_package, services) = self.parse_proto_content(&content);
        let (generated_client_path, generated_client_import) =
            self.derive_generated_client_location(output, &relative_parts, &proto_stem, is_local);

        Ok(ProtoModuleInfo {
            proto_stem,
            is_local,
            generated_client_path,
            generated_client_import,
            services,
        })
    }

    #[allow(dead_code)]
    fn parse_proto_content(&self, content: &str) -> (String, Vec<ProtoServiceInfo>) {
        let mut current_package = String::new();
        let mut current_service: Option<ProtoServiceInfo> = None;
        let mut services = Vec::new();

        for raw_line in content.lines() {
            let line = raw_line.trim();
            if line.is_empty() || line.starts_with("//") {
                continue;
            }

            if let Some(rest) = line.strip_prefix("package ") {
                current_package = rest
                    .trim_end_matches(';')
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_string();
                continue;
            }

            if let Some(rest) = line.strip_prefix("service ") {
                let name = rest
                    .split(|ch: char| ch.is_whitespace() || ch == '{')
                    .find(|segment| !segment.is_empty())
                    .unwrap_or_default()
                    .to_string();
                if !name.is_empty() {
                    current_service = Some(ProtoServiceInfo {
                        name,
                        handler_interface: String::new(),
                        dispatcher_type: String::new(),
                        generated_workload_import: String::new(),
                        methods: Vec::new(),
                    });
                }
                continue;
            }

            if let Some(rest) = line.strip_prefix("rpc ")
                && let Some(service) = current_service.as_mut()
                && let Some(input_start) = rest.find('(')
            {
                let method_name = rest[..input_start]
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_string();
                if method_name.is_empty() {
                    continue;
                }

                let after_input_start = &rest[input_start + 1..];
                let Some(input_end) = after_input_start.find(')') else {
                    continue;
                };
                let input_type = normalize_proto_type(&after_input_start[..input_end]);

                let Some(returns_pos) = after_input_start.find("returns") else {
                    continue;
                };
                let after_returns = &after_input_start[returns_pos + "returns".len()..];
                let Some(output_start) = after_returns.find('(') else {
                    continue;
                };
                let Some(output_end) = after_returns[output_start + 1..].find(')') else {
                    continue;
                };
                let output_type = normalize_proto_type(
                    &after_returns[output_start + 1..output_start + 1 + output_end],
                );

                service.methods.push(ProtoMethodInfo {
                    handler_method_name: snake_to_camel_case(&to_snake_case(&method_name)),
                    name: method_name,
                    input_type_short: short_proto_type(&input_type),
                    output_type_short: short_proto_type(&output_type),
                    input_type,
                    output_type,
                    // parse_proto_content has no owner metadata; the live path
                    // (to_proto_service_info) fills these from TypeRef.
                    input_pb_import: String::new(),
                    output_pb_import: String::new(),
                });
            }

            if line.starts_with('}')
                && let Some(service) = current_service.take()
            {
                services.push(service);
            }
        }

        if let Some(service) = current_service.take() {
            services.push(service);
        }

        (current_package, services)
    }

    fn generate_local_workload_files(&self, context: &GenContext) -> Result<Vec<PathBuf>> {
        let mut modules: BTreeMap<String, LocalWorkloadModule> = BTreeMap::new();
        let owner_index = TypeOwnerIndex::from_files(&context.proto_model.files);

        for file in &context.proto_model.files {
            if !file.services.iter().any(|service| {
                context
                    .proto_model
                    .local_services
                    .iter()
                    .any(|local| local.relative_path == service.relative_path)
            }) {
                continue;
            }

            for service in &file.services {
                if !context.proto_model.local_services.iter().any(|local| {
                    local.name == service.name && local.relative_path == service.relative_path
                }) {
                    continue;
                }

                let module_name = workload_module_name(&service.package, &service.name);
                let module =
                    modules
                        .entry(module_name.clone())
                        .or_insert_with(|| LocalWorkloadModule {
                            name: module_name,
                            services: Vec::new(),
                        });
                module.services.push(LocalWorkloadService {
                    name: service.name.clone(),
                    handler_interface: format!("{}Handler", service.name),
                    dispatcher_type: format!("{}Dispatcher", service.name),
                    methods: service
                        .methods
                        .iter()
                        .map(|method| {
                            let input_pb_import =
                                resolve_workload_pb_import(&method.input_type, file, &owner_index)?;
                            let output_pb_import = resolve_workload_pb_import(
                                &method.output_type,
                                file,
                                &owner_index,
                            )?;
                            Ok(LocalWorkloadMethod {
                                name: method.name.clone(),
                                handler_method_name: snake_to_camel_case(&method.snake_name),
                                input_type_short: short_proto_type(&method.input_type),
                                output_type_short: short_proto_type(&method.output_type),
                                route_key: method.route_key.clone(),
                                input_pb_import,
                                output_pb_import,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                });
            }
        }

        let mut generated = Vec::new();
        for module in modules.values() {
            let path = context.output.join(format!("{}.ts", module.name));
            let content = generate_local_workload_content(module);
            std::fs::write(&path, content).map_err(|e| {
                ActrCliError::config_error(format!(
                    "Failed to write TypeScript workload dispatcher {}: {e}",
                    path.display()
                ))
            })?;
            generated.push(path);
        }

        Ok(generated)
    }

    fn derive_generated_client_location(
        &self,
        output: &Path,
        relative_parts: &[String],
        proto_stem: &str,
        is_local: bool,
    ) -> (PathBuf, String) {
        if is_local {
            return (
                output.join(format!("{proto_stem}_client.ts")),
                format!("./generated/{proto_stem}_client"),
            );
        }

        let mut generated_parts = relative_parts.to_vec();
        if generated_parts
            .first()
            .is_some_and(|part| matches!(part.as_str(), "local" | "remote"))
        {
            generated_parts.remove(0);
        }

        if generated_parts.is_empty() {
            generated_parts.push(format!("{proto_stem}.proto"));
        }

        if let Some(last) = generated_parts.last_mut() {
            *last = format!("{proto_stem}_client.ts");
        }

        let generated_path = generated_parts
            .iter()
            .fold(output.to_path_buf(), |acc, part| acc.join(part));

        let mut import_parts = generated_parts;
        if let Some(last) = import_parts.last_mut() {
            *last = last.trim_end_matches(".ts").to_string();
        }
        let generated_import = format!("./generated/{}", import_parts.join("/"));

        (generated_path, generated_import)
    }

    fn inspect_generated_client_api(&self, path: &Path) -> Result<GeneratedClientApi> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            ActrCliError::config_error(format!(
                "Failed to read generated client helper {}: {e}",
                path.display()
            ))
        })?;

        let mut api = GeneratedClientApi::default();
        for raw_line in content.lines() {
            let line = raw_line.trim();
            if let Some(name) = extract_exported_name(line, "export const ") {
                api.exported_consts.insert(name);
            }
        }

        Ok(api)
    }

    fn bind_methods(&self, modules: &[ProtoModuleInfo]) -> Result<Vec<BoundMethodInfo>> {
        let mut bound_methods = Vec::new();

        for module in modules {
            let api = if module.is_local {
                GeneratedClientApi::default()
            } else if module.generated_client_path.exists() {
                self.inspect_generated_client_api(&module.generated_client_path)?
            } else {
                warn!(
                    "Generated client helper not found for {}: {}",
                    module.proto_stem,
                    module.generated_client_path.display()
                );
                GeneratedClientApi::default()
            };

            for service in &module.services {
                for method in &service.methods {
                    let request_companion_name = method.input_type_short.clone();

                    bound_methods.push(BoundMethodInfo {
                        generated_client_import: module.generated_client_import.clone(),
                        generated_workload_import: service.generated_workload_import.clone(),
                        service_name: service.name.clone(),
                        handler_interface: service.handler_interface.clone(),
                        dispatcher_type: service.dispatcher_type.clone(),
                        method_name: method.name.clone(),
                        handler_method_name: method.handler_method_name.clone(),
                        input_type: method.input_type.clone(),
                        output_type: method.output_type.clone(),
                        input_type_short: method.input_type_short.clone(),
                        output_type_short: method.output_type_short.clone(),
                        input_pb_import: method.input_pb_import.clone(),
                        output_pb_import: method.output_pb_import.clone(),
                        request_companion: api
                            .exported_consts
                            .contains(&request_companion_name)
                            .then_some(request_companion_name),
                        is_local: module.is_local,
                    });
                }
            }
        }

        Ok(bound_methods)
    }

    fn generate_scaffold_content(&self, bound_methods: &[BoundMethodInfo]) -> String {
        let local_methods = bound_methods
            .iter()
            .filter(|method| method.is_local)
            .collect::<Vec<_>>();
        let remote_methods = bound_methods
            .iter()
            .filter(|method| !method.is_local)
            .collect::<Vec<_>>();
        let local_service_count = local_methods
            .iter()
            .map(|method| method.service_name.as_str())
            .collect::<BTreeSet<_>>()
            .len();

        let mut output = String::new();
        output.push_str(UNIMPLEMENTED_MARKER);
        output.push('\n');
        output.push_str(SCAFFOLD_HINT);
        output.push_str("\n\n");
        if !local_methods.is_empty() {
            output.push_str("import { create } from '@bufbuild/protobuf';\n");
        }
        output.push_str("import { defineWorkload } from '@actrium/actr-workload';\n");

        if !local_methods.is_empty() {
            let mut service_imports: BTreeMap<ServiceImportKey<'_>, ServiceImportGroups> =
                BTreeMap::new();
            let mut proto_type_imports: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
            let mut proto_schema_imports: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();

            for method in &local_methods {
                let entry = service_imports
                    .entry((
                        method.generated_workload_import.as_str(),
                        method.service_name.as_str(),
                    ))
                    .or_default();
                entry.0.insert(method.handler_interface.clone());
                entry.1.insert(method.dispatcher_type.clone());
                if local_service_count > 1 {
                    entry.2.insert(route_constant_name(
                        &method.service_name,
                        &method.method_name,
                    ));
                }

                // Group proto type/schema imports by each method type's
                // declaring proto module (owner-resolved), so imported types
                // are imported from their real owner `_pb.js` instead of the
                // local service's proto module.
                proto_type_imports
                    .entry(&method.input_pb_import)
                    .or_default()
                    .insert(&method.input_type_short);
                proto_type_imports
                    .entry(&method.output_pb_import)
                    .or_default()
                    .insert(&method.output_type_short);
                proto_schema_imports
                    .entry(&method.output_pb_import)
                    .or_default()
                    .insert(format!("{}Schema", method.output_type_short));
            }

            for ((module_import, _service_name), (handler_types, dispatchers, route_constants)) in
                service_imports
            {
                output.push_str("import type { ");
                output.push_str(&handler_types.into_iter().collect::<Vec<_>>().join(", "));
                output.push_str(" } from '");
                output.push_str(module_import);
                output.push_str("';\n");

                let values = dispatchers
                    .into_iter()
                    .chain(route_constants)
                    .collect::<Vec<_>>();
                output.push_str("import { ");
                output.push_str(&values.join(", "));
                output.push_str(" } from '");
                output.push_str(module_import);
                output.push_str("';\n");
            }

            for (proto_import, types) in proto_type_imports {
                output.push_str("import type { ");
                output.push_str(&types.into_iter().collect::<Vec<_>>().join(", "));
                output.push_str(" } from '");
                output.push_str(proto_import);
                output.push_str("';\n");
            }

            for (proto_import, schemas) in proto_schema_imports {
                output.push_str("import { ");
                output.push_str(&schemas.into_iter().collect::<Vec<_>>().join(", "));
                output.push_str(" } from '");
                output.push_str(proto_import);
                output.push_str("';\n");
            }
        }

        if !local_methods.is_empty() {
            let mut services: BTreeMap<&str, Vec<&&BoundMethodInfo>> = BTreeMap::new();
            for method in &local_methods {
                services
                    .entry(method.service_name.as_str())
                    .or_default()
                    .push(method);
            }

            for (service_name, methods) in &services {
                let first = methods[0];
                output.push_str("\nclass ");
                output.push_str(service_name);
                output.push_str("HandlerImpl implements ");
                output.push_str(&first.handler_interface);
                output.push_str(" {\n");
                for method in methods {
                    output.push_str("  ");
                    output.push_str(&method.handler_method_name);
                    output.push_str("(_req: ");
                    output.push_str(&method.input_type_short);
                    output.push_str("): ");
                    output.push_str(&method.output_type_short);
                    output.push_str(" {\n");
                    output.push_str("    return create(");
                    output.push_str(&method.output_type_short);
                    output.push_str("Schema, {});\n");
                    output.push_str("  }\n\n");
                }
                output.push_str("}\n");
                output.push_str("\nconst ");
                output.push_str(&scaffold_dispatcher_variable_name(
                    service_name,
                    local_service_count,
                ));
                output.push_str(" = new ");
                output.push_str(&first.dispatcher_type);
                output.push_str("(new ");
                output.push_str(service_name);
                output.push_str("HandlerImpl());\n");
            }
        }

        output.push_str("\nexport default defineWorkload({\n  async onStart(): Promise<void> {\n    console.log('ACTR TypeScript workload started');\n");
        output.push_str("    console.log('Remote RPC methods:', ");
        output.push_str(&remote_methods.len().to_string());
        output.push_str(");\n");
        if !remote_methods.is_empty() {
            output.push_str(
                "    console.log('Remote call examples are listed below. Uncomment and adapt them when you are ready.');\n",
            );
        }
        output.push_str("  },\n\n  async onStop(): Promise<void> {\n    console.log('ACTR TypeScript workload stopped');\n  },\n\n  async dispatch(envelope): Promise<Uint8Array> {\n");
        let local_services = local_methods
            .iter()
            .map(|method| method.service_name.as_str())
            .collect::<BTreeSet<_>>();
        if local_services.len() == 1 {
            let service_name = local_services.iter().next().expect("service exists");
            output.push_str("    return ");
            output.push_str(&scaffold_dispatcher_variable_name(
                service_name,
                local_services.len(),
            ));
            output.push_str(".dispatch(envelope);\n");
        } else if !local_services.is_empty() {
            output.push_str("    switch (envelope.method) {\n");
            for service_name in &local_services {
                for method in local_methods
                    .iter()
                    .filter(|method| method.service_name == **service_name)
                {
                    output.push_str("      case ");
                    output.push_str(&route_constant_name(
                        &method.service_name,
                        &method.method_name,
                    ));
                    output.push_str(":\n");
                }
                output.push_str("        return ");
                output.push_str(&scaffold_dispatcher_variable_name(
                    service_name,
                    local_services.len(),
                ));
                output.push_str(".dispatch(envelope);\n");
            }
            output.push_str("      default:\n");
            output.push_str("        throw new Error(`Unknown route: ${envelope.method}`);\n");
            output.push_str("    }\n");
        } else {
            output.push_str(
                "    throw new Error('No local RPC methods were inferred for this workload.');\n",
            );
        }
        output.push_str("  },\n});\n");

        if !remote_methods.is_empty() {
            output.push_str("\n// Remote RPC quick-start examples:\n");
            for method in &remote_methods {
                output.push_str("//\n// ");
                output.push_str(&method.service_name);
                output.push('.');
                output.push_str(&method.method_name);
                output.push_str(" (");
                output.push_str(&method.input_type);
                output.push_str(" -> ");
                output.push_str(&method.output_type);
                output.push_str(")\n");
                output.push_str("// Generated client: ");
                output.push_str(&method.generated_client_import);
                output.push('\n');
                output.push_str("// import {\n");
                if let Some(request_companion) = &method.request_companion {
                    output.push_str("//   ");
                    output.push_str(request_companion);
                    output.push_str(",\n");
                } else {
                    output.push_str("//   // TODO: infer the request companion,\n");
                }
                output.push_str("// } from '");
                output.push_str(&method.generated_client_import);
                output.push_str("';\n");
                output.push_str("// const payload = ");
                if let Some(request_companion) = &method.request_companion {
                    output.push_str(request_companion);
                    output.push_str(".encode({\n");
                } else {
                    output.push_str("/* TODO: request encoder */({\n");
                }
                output.push_str("//   // TODO: fill ");
                output.push_str(&method.input_type_short);
                output.push_str(" fields\n// });\n");
                if let Some(request_companion) = &method.request_companion {
                    output.push_str("// const responseBytes = await actorRef.call(");
                    output.push_str(request_companion);
                    output.push_str(".routeKey, 0, payload, 15000);\n");
                    output.push_str("// const response = ");
                    output.push_str(request_companion);
                    output.push_str(".response.decode(responseBytes);\n");
                } else {
                    output.push_str(
                        "// const responseBytes = await actorRef.call(/* TODO: route key */, 0, payload, 15000);\n",
                    );
                    output.push_str(
                        "// const response = /* TODO: response decoder */(responseBytes);\n",
                    );
                }
                output.push_str("// console.log('Response (");
                output.push_str(&method.output_type_short);
                output.push_str("):', response);\n");
            }
        }

        output
    }

    fn should_overwrite_scaffold(&self, path: &Path) -> Result<bool> {
        let content = match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(_) => return Ok(false),
        };

        if content.contains(IMPLEMENTED_MARKER) {
            return Ok(false);
        }

        if !content.contains(UNIMPLEMENTED_MARKER) {
            return Ok(false);
        }

        Ok(content.contains(SCAFFOLD_HINT))
    }

    fn ensure_required_tools(&self) -> Result<()> {
        if !command_exists(PROTOC) {
            return Err(ActrCliError::command_error(
                "protoc not found. Please install protobuf compiler.".to_string(),
            ));
        }
        if !command_exists(NODE) {
            return Err(ActrCliError::command_error(
                "node not found. Please install Node.js.".to_string(),
            ));
        }
        Ok(())
    }

    fn ensure_protoc_gen_es(&self, context: &GenContext) -> Result<PathBuf> {
        if let Some(local_path) = self.locate_project_protoc_gen_es(context) {
            info!("✅ Using local {PROTOC_GEN_ES} at {}", local_path.display());
            return Ok(local_path);
        }

        if let Ok(path) = self.locate_binary_in_path(PROTOC_GEN_ES) {
            info!("✅ Using installed {PROTOC_GEN_ES} at {}", path.display());
            return Ok(path);
        }

        self.ensure_npx_available()?;
        let wrapper_path = self.ensure_protoc_gen_es_wrapper()?;
        info!(
            "✅ Using cached npx wrapper for {PROTOC_GEN_ES} at {}",
            wrapper_path.display()
        );
        Ok(wrapper_path)
    }

    fn ensure_typescript_plugin(&self) -> Result<PathBuf> {
        let required_version = env!("CARGO_PKG_VERSION");

        // Check system installation (Homebrew)
        if let Some(version) = self.check_installed_plugin_version()? {
            if version == required_version {
                info!("✅ Using installed {PLUGIN_NAME} v{version}");
                return self.locate_installed_plugin();
            }

            return Err(ActrCliError::command_error(format!(
                "Installed {PLUGIN_NAME} v{version} does not match actr v{required_version}. \
                 Install the plugin asset from the matching ACTR release."
            )));
        }

        // Download from GitHub Release
        info!("📦 {PLUGIN_NAME} not found in PATH, downloading from GitHub Release...");
        let plugin_path = self.download_plugin_from_release(required_version)?;

        // Verify version
        self.ensure_required_plugin_version(&plugin_path, required_version)?;

        Ok(plugin_path)
    }

    fn check_installed_plugin_version(&self) -> Result<Option<String>> {
        let path = match self.locate_installed_plugin() {
            Ok(path) => path,
            Err(error) => {
                debug!("{PLUGIN_NAME} not available: {}", error);
                return Ok(None);
            }
        };

        let output = StdCommand::new(&path).arg("--version").output();
        let Ok(output) = output else {
            debug!("Failed to execute {PLUGIN_NAME} at {}", path.display());
            return Ok(None);
        };
        if !output.status.success() {
            warn!("{PLUGIN_NAME} --version returned non-zero; treating as unavailable");
            return Ok(None);
        }

        Ok(self.parse_plugin_version(&output.stdout))
    }

    fn parse_plugin_version(&self, stdout: &[u8]) -> Option<String> {
        String::from_utf8_lossy(stdout)
            .split_whitespace()
            .find(|part| part.chars().next().is_some_and(|c| c.is_ascii_digit()))
            .map(str::to_string)
    }

    fn ensure_required_plugin_version(
        &self,
        plugin_path: &Path,
        required_version: &str,
    ) -> Result<()> {
        let output = StdCommand::new(plugin_path)
            .arg("--version")
            .output()
            .map_err(|e| {
                ActrCliError::command_error(format!("Failed to run {PLUGIN_NAME} --version: {e}"))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "{PLUGIN_NAME} --version failed: {stderr}"
            )));
        }

        let Some(version) = self.parse_plugin_version(&output.stdout) else {
            return Err(ActrCliError::command_error(format!(
                "Failed to determine {PLUGIN_NAME} version"
            )));
        };

        if version == required_version {
            info!("✅ {PLUGIN_NAME} version {version} matches actr version {required_version}");
            return Ok(());
        }

        Err(ActrCliError::command_error(format!(
            "{PLUGIN_NAME} version {version} does not match actr version {required_version}"
        )))
    }

    fn locate_installed_plugin(&self) -> Result<PathBuf> {
        self.locate_binary_in_path(PLUGIN_NAME)
    }

    fn locate_binary_in_path(&self, binary: &str) -> Result<PathBuf> {
        let output = StdCommand::new("which").arg(binary).output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to locate {binary} in PATH: {e}"))
        })?;

        if !output.status.success() {
            return Err(ActrCliError::command_error(format!(
                "{binary} is not available in PATH"
            )));
        }

        let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if path.is_empty() {
            return Err(ActrCliError::command_error(format!(
                "{binary} resolved to an empty path"
            )));
        }

        Ok(PathBuf::from(path))
    }

    fn locate_project_protoc_gen_es(&self, context: &GenContext) -> Option<PathBuf> {
        let config_dir = context
            .config_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
        let local_path = config_dir
            .join("node_modules")
            .join(".bin")
            .join(PROTOC_GEN_ES);
        local_path.exists().then_some(local_path)
    }

    fn ensure_npx_available(&self) -> Result<()> {
        if command_exists(NPX) {
            return Ok(());
        }
        Err(ActrCliError::command_error(
            "npx not found. Please install npm or add protoc-gen-es to PATH.".to_string(),
        ))
    }

    fn ensure_protoc_gen_es_wrapper(&self) -> Result<PathBuf> {
        let wrapper_dir = std::env::temp_dir()
            .join("actr-cli")
            .join("protoc-gen-es")
            .join(format!("v{EXPECTED_PROTOC_GEN_ES_VERSION}"));
        std::fs::create_dir_all(&wrapper_dir).map_err(|e| {
            ActrCliError::command_error(format!(
                "Failed to create protoc-gen-es cache directory {}: {}",
                wrapper_dir.display(),
                e
            ))
        })?;

        let wrapper_path = wrapper_dir.join(PROTOC_GEN_ES);
        let script = format!(
            "#!/usr/bin/env bash\nset -euo pipefail\nexec npx --yes -p @bufbuild/protoc-gen-es@{EXPECTED_PROTOC_GEN_ES_VERSION} protoc-gen-es \"$@\"\n"
        );

        let should_write = match std::fs::read_to_string(&wrapper_path) {
            Ok(existing) => existing != script,
            Err(_) => true,
        };

        if should_write {
            std::fs::write(&wrapper_path, script).map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to write protoc-gen-es wrapper {}: {}",
                    wrapper_path.display(),
                    e
                ))
            })?;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&wrapper_path)
                .map_err(|e| {
                    ActrCliError::command_error(format!(
                        "Failed to read protoc-gen-es wrapper metadata {}: {}",
                        wrapper_path.display(),
                        e
                    ))
                })?
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&wrapper_path, perms).map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to set protoc-gen-es wrapper permissions {}: {}",
                    wrapper_path.display(),
                    e
                ))
            })?;
        }

        Ok(wrapper_path)
    }

    fn download_plugin_from_release(&self, version: &str) -> Result<PathBuf> {
        let cache_root = std::env::temp_dir()
            .join("actr-cli")
            .join("framework-codegen-typescript");

        let version_tag = format!("v{}", version);
        let cache_dir = cache_root.join(&version_tag);
        let plugin_path = cache_dir.join("scripts").join(PLUGIN_NAME);
        let bundle_path = cache_dir.join("dist").join("bundle.js");

        // Check cache
        if plugin_path.exists() && bundle_path.exists() {
            info!("✅ Using cached TypeScript plugin (v{})", version);
            return Ok(plugin_path);
        }

        // Clean incomplete cache
        if cache_dir.exists() {
            std::fs::remove_dir_all(&cache_dir).map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to clean cache directory {}: {}",
                    cache_dir.display(),
                    e
                ))
            })?;
        }
        std::fs::create_dir_all(&cache_dir).map_err(|e| {
            ActrCliError::command_error(format!(
                "Failed to create cache directory {}: {}",
                cache_dir.display(),
                e
            ))
        })?;

        // Build download URL
        let release_url = GITHUB_RELEASE_URL_TEMPLATE.replace("{}", version);

        info!("📦 Downloading from: {}", release_url);

        // Check if curl is available
        if !command_exists("curl") {
            return Err(ActrCliError::command_error(
                "curl not found. Please install curl to download the plugin.".to_string(),
            ));
        }
        if !command_exists("unzip") {
            return Err(ActrCliError::command_error(
                "unzip not found. Please install unzip to extract the plugin.".to_string(),
            ));
        }

        // Download
        let archive_path = cache_dir.join("plugin.zip");

        let output = StdCommand::new("curl")
            .arg("-L") // Follow redirects
            .arg("-f") // Return error code on failure
            .arg("--progress-bar")
            .arg("-o")
            .arg(&archive_path)
            .arg(&release_url)
            .output()
            .map_err(|e| ActrCliError::command_error(format!("Failed to execute curl: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to download TypeScript plugin from GitHub Release\n\
                 URL: {}\n\
                 Error: {}",
                release_url, stderr
            )));
        }

        // Extract
        info!("📦 Extracting plugin...");

        let extract_dir = cache_dir.join("extracted");
        std::fs::create_dir_all(&extract_dir).map_err(|e| {
            ActrCliError::command_error(format!("Failed to create extract directory: {}", e))
        })?;

        let output = StdCommand::new("unzip")
            .arg("-q")
            .arg(&archive_path)
            .arg("-C")
            .arg(&extract_dir)
            .output()
            .map_err(|e| ActrCliError::command_error(format!("Failed to execute unzip: {}", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to extract plugin: {}",
                stderr
            )));
        }

        // Adjust directory structure
        // Release package structure:
        //   extracted/
        //   ├── dist/bundle.js
        //   ├── scripts/protoc-gen-actrframework-typescript
        //   └── package.json
        //
        // Required structure (plugin script expects PROJECT_DIR/../dist/bundle.js):
        //   cache_dir/
        //   ├── dist/
        //   │   └── bundle.js
        //   └── scripts/
        //       └── protoc-gen-actrframework-typescript

        let dist_dir = cache_dir.join("dist");
        std::fs::create_dir_all(&dist_dir).map_err(|e| {
            ActrCliError::command_error(format!("Failed to create dist directory: {}", e))
        })?;

        let scripts_dir = cache_dir.join("scripts");
        std::fs::create_dir_all(&scripts_dir).map_err(|e| {
            ActrCliError::command_error(format!("Failed to create scripts directory: {}", e))
        })?;

        // Move bundle.js
        let src_bundle = extract_dir.join("dist").join("bundle.js");
        if !src_bundle.exists() {
            return Err(ActrCliError::command_error(
                "bundle.js not found in downloaded package".to_string(),
            ));
        }
        std::fs::rename(&src_bundle, &bundle_path)
            .map_err(|e| ActrCliError::command_error(format!("Failed to move bundle.js: {}", e)))?;

        // Move executable script to scripts/ directory
        let src_plugin = extract_dir.join("scripts").join(PLUGIN_NAME);
        if !src_plugin.exists() {
            return Err(ActrCliError::command_error(format!(
                "{} not found in downloaded package",
                PLUGIN_NAME
            )));
        }
        std::fs::rename(&src_plugin, &plugin_path).map_err(|e| {
            ActrCliError::command_error(format!("Failed to move plugin script: {}", e))
        })?;

        // Clean up temporary files
        std::fs::remove_file(&archive_path).ok();
        std::fs::remove_dir_all(&extract_dir).ok();

        // Set executable permissions (Unix)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&plugin_path)
                .map_err(|e| {
                    ActrCliError::command_error(format!("Failed to read plugin metadata: {}", e))
                })?
                .permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&plugin_path, perms).map_err(|e| {
                ActrCliError::command_error(format!("Failed to set plugin permissions: {}", e))
            })?;
        }

        // Final verification
        if !plugin_path.exists() || !bundle_path.exists() {
            return Err(ActrCliError::command_error(
                "Plugin installation incomplete: required files not found".to_string(),
            ));
        }

        info!("✅ Successfully installed TypeScript plugin (v{})", version);

        Ok(plugin_path)
    }

    fn build_remote_mapping(
        &self,
        context: &GenContext,
        remote_files: &[String],
    ) -> Result<HashMap<String, String>> {
        if remote_files.is_empty() {
            return Ok(HashMap::new());
        }

        let config_dir = context
            .config_path
            .parent()
            .unwrap_or_else(|| Path::new("."));
        let lock_path = config_dir.join("manifest.lock.toml");
        if !lock_path.exists() {
            return Err(ActrCliError::config_error(format!(
                "manifest.lock.toml not found at {}. Please run `actr deps install` first.",
                lock_path.display()
            )));
        }

        let lock_file = LockFile::from_file(&lock_path).map_err(|e| {
            ActrCliError::config_error(format!("Failed to parse {}: {e}", lock_path.display()))
        })?;

        let mut lock_mapping = HashMap::new();
        for dep in lock_file.dependencies {
            for file in dep.files {
                lock_mapping.insert(file.path, dep.actr_type.clone());
            }
        }

        let mut result = HashMap::new();
        for remote_file in remote_files {
            let lock_key = remote_file.trim_start_matches("remote/").to_string();
            let actr_type = lock_mapping.get(&lock_key).ok_or_else(|| {
                ActrCliError::config_error(format!(
                    "Remote proto '{}' missing in manifest.lock.toml.\n\
                     Please run `actr deps install` and retry.",
                    lock_key
                ))
            })?;
            result.insert(
                remote_file.clone(),
                normalize_actr_type_for_typescript_plugin(actr_type)?,
            );
        }

        Ok(result)
    }
}

fn normalize_actr_type_for_typescript_plugin(raw: &str) -> Result<String> {
    let parsed = ActrType::from_string_repr(raw).map_err(|e| {
        ActrCliError::config_error(format!(
            "Invalid actr_type '{raw}' in manifest.lock.toml: {e}"
        ))
    })?;
    Ok(parsed.to_string_repr())
}

fn generate_local_workload_content(module: &LocalWorkloadModule) -> String {
    // Group referenced message types by their declaring proto module's
    // workload-relative import path, so imported types are imported from their
    // real owner `_pb.js` instead of the local service's proto stem.
    let mut type_imports: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut schema_imports: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();

    for service in &module.services {
        for method in &service.methods {
            type_imports
                .entry(&method.input_pb_import)
                .or_default()
                .insert(&method.input_type_short);
            type_imports
                .entry(&method.output_pb_import)
                .or_default()
                .insert(&method.output_type_short);
            schema_imports
                .entry(&method.input_pb_import)
                .or_default()
                .insert(format!("{}Schema", method.input_type_short));
            schema_imports
                .entry(&method.output_pb_import)
                .or_default()
                .insert(format!("{}Schema", method.output_type_short));
        }
    }

    let mut output = String::new();
    output.push_str("// DO NOT EDIT.\n");
    output.push_str("// Generated by actr gen -l typescript.\n\n");
    output.push_str("import type { RpcEnvelope } from '@actrium/actr-workload';\n");
    output.push_str("import { fromBinary, toBinary } from '@bufbuild/protobuf';\n");

    for (import_path, types) in type_imports {
        output.push_str("import type { ");
        output.push_str(&types.into_iter().collect::<Vec<_>>().join(", "));
        output.push_str(" } from '");
        output.push_str(import_path);
        output.push_str("';\n");
    }

    for (import_path, schemas) in schema_imports {
        output.push_str("import { ");
        output.push_str(&schemas.into_iter().collect::<Vec<_>>().join(", "));
        output.push_str(" } from '");
        output.push_str(import_path);
        output.push_str("';\n");
    }

    for service in &module.services {
        output.push('\n');
        for method in &service.methods {
            output.push_str("export const ");
            output.push_str(&route_constant_name(&service.name, &method.name));
            output.push_str(" = ");
            output.push_str(&typescript_string_literal(&method.route_key));
            output.push_str(";\n");
        }

        output.push_str("\nexport interface ");
        output.push_str(&service.handler_interface);
        output.push_str(" {\n");
        for method in &service.methods {
            output.push_str("  ");
            output.push_str(&method.handler_method_name);
            output.push_str("(req: ");
            output.push_str(&method.input_type_short);
            output.push_str("): ");
            output.push_str(&method.output_type_short);
            output.push_str(" | Promise<");
            output.push_str(&method.output_type_short);
            output.push_str(">;\n");
        }
        output.push_str("}\n");

        output.push_str("\nexport class ");
        output.push_str(&service.dispatcher_type);
        output.push_str(" {\n");
        output.push_str("  constructor(private readonly handler: ");
        output.push_str(&service.handler_interface);
        output.push_str(") {}\n\n");
        output.push_str("  async dispatch(envelope: RpcEnvelope): Promise<Uint8Array> {\n");
        for method in &service.methods {
            output.push_str("    if (envelope.method === ");
            output.push_str(&route_constant_name(&service.name, &method.name));
            output.push_str(") {\n");
            output.push_str("      const request = fromBinary(");
            output.push_str(&method.input_type_short);
            output.push_str("Schema, envelope.payload ?? new Uint8Array());\n");
            output.push_str("      const response = await this.handler.");
            output.push_str(&method.handler_method_name);
            output.push_str("(request);\n");
            output.push_str("      return toBinary(");
            output.push_str(&method.output_type_short);
            output.push_str("Schema, response);\n");
            output.push_str("    }\n\n");
        }
        output.push_str("    throw new Error(`Unknown route: ${envelope.method}`);\n");
        output.push_str("  }\n");
        output.push_str("}\n");
    }

    output
}

/// Post-process generated directory:
/// 1. Flatten `local/` files into the output root
/// 2. Lift `remote/xxx/` to `xxx/` (remove the `remote` layer)
/// 3. Rewrite import paths in generated files
fn flatten_local_and_lift_remote(output: &Path) -> Result<()> {
    // 1. Flatten local/
    let local_dir = output.join("local");
    if local_dir.exists() {
        for entry in std::fs::read_dir(&local_dir)
            .map_err(|e| ActrCliError::command_error(format!("Failed to read local dir: {e}")))?
        {
            let entry = entry.map_err(|e| {
                ActrCliError::command_error(format!("Failed to read local dir entry: {e}"))
            })?;
            let dest = output.join(entry.file_name());
            std::fs::rename(entry.path(), &dest).map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to move {} to {}: {e}",
                    entry.path().display(),
                    dest.display()
                ))
            })?;
        }
        std::fs::remove_dir(&local_dir)
            .map_err(|e| ActrCliError::command_error(format!("Failed to remove local dir: {e}")))?;
    }

    // 2. Lift remote/xxx/ → xxx/
    let remote_dir = output.join("remote");
    if remote_dir.exists() {
        for entry in std::fs::read_dir(&remote_dir)
            .map_err(|e| ActrCliError::command_error(format!("Failed to read remote dir: {e}")))?
        {
            let entry = entry.map_err(|e| {
                ActrCliError::command_error(format!("Failed to read remote dir entry: {e}"))
            })?;
            let dest = output.join(entry.file_name());
            std::fs::rename(entry.path(), &dest).map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to move {} to {}: {e}",
                    entry.path().display(),
                    dest.display()
                ))
            })?;
        }
        std::fs::remove_dir(&remote_dir).map_err(|e| {
            ActrCliError::command_error(format!("Failed to remove remote dir: {e}"))
        })?;
    }

    // 3. Rewrite import paths
    rewrite_imports(output)?;

    debug!("Post-processed generated directory: flattened local/ and lifted remote/");
    Ok(())
}

/// Rewrite import paths in generated .js/.ts/.d.ts files:
/// - `from "./remote/` → `from "./`
/// - `from "./local/` → `from "./`
fn rewrite_imports(output: &Path) -> Result<()> {
    for entry in WalkDir::new(output).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        if !matches!(ext, "ts" | "js") && !path.to_string_lossy().ends_with(".d.ts") {
            continue;
        }
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        let updated = content
            .replace("from \"./remote/", "from \"./")
            .replace("from './remote/", "from './")
            .replace("from \"./local/", "from \"./")
            .replace("from './local/", "from './");
        if updated != content {
            std::fs::write(path, updated).map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to rewrite imports in {}: {e}",
                    path.display()
                ))
            })?;
        }
    }
    Ok(())
}

fn normalize_proto_path(path: &Path) -> String {
    path.components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect::<Vec<_>>()
        .join("/")
}

fn normalize_proto_lookup_key(path: &Path) -> String {
    let normalized = normalize_proto_path(path);
    normalized
        .strip_suffix(".proto")
        .unwrap_or(&normalized)
        .to_string()
}

fn collect_ts_files(root: &Path) -> Vec<PathBuf> {
    WalkDir::new(root)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path().to_path_buf())
        .filter(|path| path.extension().is_some_and(|ext| ext == "ts"))
        .collect()
}

fn normalize_proto_type(raw: &str) -> String {
    raw.trim().trim_start_matches('.').to_string()
}

fn short_proto_type(raw: &str) -> String {
    normalize_proto_type(raw)
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_string()
}

/// Scaffold-relative (`./generated/...`) import path for a proto file's
/// generated `_pb.js` module, as referenced from the user-facing
/// `actr_service.ts` (which lives outside `generated/`). Generated protobuf
/// files are emitted under `generated/` with only the leading `local/` or
/// `remote/` source marker removed.
fn scaffold_proto_import_for(relative_path: &str) -> String {
    let path = Path::new(relative_path);
    let proto_stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("proto")
        .to_string();
    let relative_parts: Vec<String> = path
        .components()
        .filter_map(|component| component.as_os_str().to_str())
        .map(str::to_string)
        .collect();
    let mut import_parts = relative_parts;
    if import_parts
        .first()
        .is_some_and(|part| matches!(part.as_str(), "local" | "remote"))
    {
        import_parts.remove(0);
    }
    if import_parts.is_empty() {
        import_parts.push(format!("{proto_stem}.proto"));
    }
    if let Some(last) = import_parts.last_mut() {
        *last = format!("{proto_stem}_pb.js");
    }
    format!("./generated/{}", import_parts.join("/"))
}

/// Workload-relative import path for a generated `_pb.js` module, derived from
/// a proto file's path relative to the proto root. Mirrors the
/// `flatten_local_and_lift_remote` post-processing: `local/X.proto` flattens
/// to `./X_pb.js`, `remote/<alias>/X.proto` lifts to `./<alias>/X_pb.js`.
fn workload_pb_import_path(relative_path: &str) -> String {
    let path = Path::new(relative_path);
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("proto");
    let mut import_parts = path
        .parent()
        .map(|parent| {
            parent
                .components()
                .filter_map(|component| component.as_os_str().to_str())
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if import_parts
        .first()
        .is_some_and(|part| matches!(part.as_str(), "local" | "remote"))
    {
        import_parts.remove(0);
    }
    if import_parts.is_empty() {
        format!("./{stem}_pb.js")
    } else {
        format!("./{}/{stem}_pb.js", import_parts.join("/"))
    }
}

/// Resolve the workload-relative `_pb.js` import path for a referenced RPC
/// message type by looking up its declaring proto file. Falls back to the
/// current service's file for types absent from the local proto set, and
/// surfaces ambiguous unqualified types as a config error instead of silently
/// picking the wrong owner.
fn resolve_workload_pb_import(
    referenced: &str,
    current_file: &ProtoFileModel,
    owner_index: &TypeOwnerIndex,
) -> Result<String> {
    let normalized = referenced.trim().trim_start_matches('.');
    match owner_index.resolve(referenced, current_file) {
        Ok(Some(owner)) => Ok(workload_pb_import_path(&owner.proto_file)),
        Ok(None) if normalized.contains('.') => Err(ActrCliError::config_error(format!(
            "Cannot resolve RPC type `{normalized}` for TypeScript workload: qualified RPC types must be declared in one of the parsed proto files"
        ))),
        Ok(None) => Ok(workload_pb_import_path(
            &current_file.relative_path.to_string_lossy(),
        )),
        Err(candidates) => {
            let declared_files = candidates
                .iter()
                .map(|owner| owner.proto_file.clone())
                .collect::<Vec<_>>()
                .join(", ");
            Err(ActrCliError::config_error(format!(
                "Cannot uniquely resolve RPC type `{}` for TypeScript workload: declared in multiple proto files [{}]",
                referenced.trim_start_matches('.'),
                declared_files
            )))
        }
    }
}

fn workload_module_name(package: &str, service_name: &str) -> String {
    let base = if package.is_empty() {
        to_snake_case(service_name)
    } else {
        package.replace(['.', '-'], "_").to_ascii_lowercase()
    };
    format!("{base}_workload")
}

fn route_constant_name(service_name: &str, method_name: &str) -> String {
    format!(
        "{}_{}_ROUTE",
        to_screaming_snake(service_name),
        to_screaming_snake(method_name)
    )
}

fn dispatcher_variable_name(service_name: &str) -> String {
    format!("{}Dispatcher", lower_camel_case(service_name))
}

fn scaffold_dispatcher_variable_name(service_name: &str, local_service_count: usize) -> String {
    if local_service_count == 1 {
        "dispatcher".to_string()
    } else {
        dispatcher_variable_name(service_name)
    }
}

fn snake_to_camel_case(raw: &str) -> String {
    let mut parts = raw.split('_').filter(|part| !part.is_empty());
    let Some(first) = parts.next() else {
        return String::new();
    };
    let mut output = first.to_ascii_lowercase();
    for part in parts {
        output.push_str(&upper_camel_case(part));
    }
    output
}

fn lower_camel_case(raw: &str) -> String {
    let upper = upper_camel_case(raw);
    let mut chars = upper.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    format!(
        "{}{}",
        first.to_ascii_lowercase(),
        chars.collect::<String>()
    )
}

fn upper_camel_case(raw: &str) -> String {
    raw.split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            let Some(first) = chars.next() else {
                return String::new();
            };
            format!("{}{}", first.to_ascii_uppercase(), chars.as_str())
        })
        .collect::<String>()
}

fn to_snake_case(raw: &str) -> String {
    let mut output = String::new();
    let mut previous_was_separator = false;
    let mut previous_was_lower_or_digit = false;

    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && previous_was_lower_or_digit && !output.ends_with('_') {
                output.push('_');
            }
            output.push(ch.to_ascii_lowercase());
            previous_was_separator = false;
            previous_was_lower_or_digit = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        } else if !previous_was_separator && !output.is_empty() {
            output.push('_');
            previous_was_separator = true;
            previous_was_lower_or_digit = false;
        }
    }

    output.trim_matches('_').to_string()
}

fn to_screaming_snake(raw: &str) -> String {
    to_snake_case(raw).to_ascii_uppercase()
}

fn typescript_string_literal(raw: &str) -> String {
    format!("{raw:?}")
}

fn extract_exported_name(line: &str, prefix: &str) -> Option<String> {
    let rest = line.strip_prefix(prefix)?.trim_start();
    let name = rest
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>();
    (!name.is_empty()).then_some(name)
}

#[cfg(test)]
#[path = "typescript_tests.rs"]
mod tests;
