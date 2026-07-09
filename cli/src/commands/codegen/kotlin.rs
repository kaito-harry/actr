use crate::commands::SupportedLanguage;
use crate::commands::codegen::scaffold::{ScaffoldCatalog, ScaffoldService};
use crate::commands::codegen::traits::{GenContext, LanguageGenerator};
use crate::error::{ActrCliError, Result};
use crate::utils::{command_exists, to_snake_case};
use actr_config::LockFile;
use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use tracing::{debug, info, warn};
use walkdir::WalkDir;

const PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN: &str = "protoc-gen-actrframework-kotlin";

pub struct KotlinGenerator;

/// Information about a proto service
#[derive(Debug, Clone)]
struct ServiceInfo {
    /// Service name (e.g., "EchoService", "FileTransferService")
    service_name: String,
    /// Proto package (e.g., "echo", "file_transfer")
    proto_package: String,
    /// Whether this is a local service (vs remote)
    is_local: bool,
    /// Remote target actor type (only for remote services)
    remote_target_type: Option<String>,
    /// List of RPC methods in this service
    methods: Vec<MethodInfo>,
}

/// Information about an RPC method
#[derive(Debug, Clone, Default)]
struct MethodInfo {
    /// Method name (e.g., "send_file")
    name: String,
    /// Descriptor-derived generated JVM request type.
    request_type: String,
    /// Descriptor-derived generated JVM response type.
    response_type: String,
    /// Kotlin import path (`package.OuterClass`) for the request type's
    /// declaring proto file, so imported types resolve to their real owner.
    request_import: String,
    /// Kotlin import path for the response type's declaring proto file.
    response_import: String,
}

impl KotlinGenerator {
    /// Ensure required tools are available and return the plugin path.
    ///
    /// Tries to build the workspace-local Kotlin protoc plugin first,
    /// then falls back to system PATH lookup.
    fn ensure_required_tools(&self) -> Result<PathBuf> {
        // 1. Try building from workspace-local tools/protoc-gen/kotlin/
        if let Some(local_plugin) = self.try_build_workspace_kotlin_plugin()? {
            return Ok(local_plugin);
        }

        // 2. Try environment variable
        if let Ok(plugin_path) = std::env::var("ACTR_KOTLIN_PLUGIN_PATH") {
            let path = PathBuf::from(&plugin_path);
            if path.exists() {
                self.ensure_plugin_version(&path)?;
                debug!("Using Kotlin plugin from env: {:?}", path);
                return Ok(path);
            }
        }

        // 3. Try system PATH
        let output = StdCommand::new("which")
            .arg(PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN)
            .output();

        if let Ok(output) = output
            && output.status.success()
        {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                let path = PathBuf::from(path);
                self.ensure_plugin_version(&path)?;
                return Ok(path);
            }
        }

        Err(ActrCliError::config_error(
            "Could not find protoc-gen-actrframework-kotlin plugin.\n\n\
             Installation options:\n\n\
             1. Build from workspace (automatic):\n\
                The CLI will attempt to build from tools/protoc-gen/kotlin/ if it exists.\n\
                Requires: Java 17+, Gradle wrapper in that directory.\n\n\
             2. Build from source:\n\
                cd tools/protoc-gen/kotlin && ./gradlew protocPluginJar\n\n\
             3. Set environment variable:\n\
                export ACTR_KOTLIN_PLUGIN_PATH=/path/to/protoc-gen-actrframework-kotlin\n\n\
             For more information, visit: https://github.com/Actrium/actr/tree/main/tools/protoc-gen/kotlin",
        ))
    }

    /// Try to build the workspace-local Kotlin protoc plugin from
    /// `tools/protoc-gen/kotlin/` and return its path on success.
    fn try_build_workspace_kotlin_plugin(&self) -> Result<Option<PathBuf>> {
        let plugin_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(|path| path.join("tools/protoc-gen/kotlin"))
            .unwrap_or_else(|| PathBuf::from("tools/protoc-gen/kotlin"));

        let build_gradle = plugin_root.join("build.gradle.kts");
        if !build_gradle.is_file() {
            return Ok(None);
        }

        let gradlew = if plugin_root.join("gradlew").is_file() {
            "./gradlew"
        } else if command_exists("gradle") {
            "gradle"
        } else {
            debug!(
                "No gradlew or gradle found in {:?}, skipping workspace-local build",
                plugin_root
            );
            return Ok(None);
        };

        info!(
            "🔨 Building workspace-local {}...",
            PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN
        );
        let output = StdCommand::new(gradlew)
            .args(["protocPluginJar"])
            .current_dir(&plugin_root)
            .output()
            .map_err(|e| {
                ActrCliError::command_error(format!(
                    "Failed to build workspace-local {PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN}: {e}"
                ))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "workspace-local {PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} build failed: {stderr}"
            )));
        }

        // The wrapper script in the plugin root calls the built JAR
        let wrapper_script = plugin_root.join(PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN);
        if wrapper_script.is_file() {
            info!(
                "✅ Using workspace-local {} at {}",
                PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN,
                wrapper_script.display()
            );
            return Ok(Some(wrapper_script));
        }

        // Also check the built JAR directly
        let jar_path = plugin_root.join("build/libs/protoc-gen-actrframework-kotlin.jar");
        if jar_path.is_file() {
            info!(
                "✅ Built workspace-local {} JAR at {}",
                PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN,
                jar_path.display()
            );
        }

        Err(ActrCliError::command_error(format!(
            "workspace-local {PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} build completed but plugin not found under {}",
            plugin_root.display()
        )))
    }

    fn ensure_plugin_version(&self, plugin_path: &Path) -> Result<()> {
        let required_version = env!("CARGO_PKG_VERSION");
        let output = StdCommand::new(plugin_path)
            .arg("--version")
            .output()
            .map_err(|error| {
                ActrCliError::command_error(format!(
                    "Failed to run {PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} --version: {error}"
                ))
            })?;

        if !output.status.success() {
            return Err(ActrCliError::command_error(format!(
                "{PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} --version failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let version = stdout
            .split_whitespace()
            .find(|part| part.chars().next().is_some_and(|ch| ch.is_ascii_digit()))
            .ok_or_else(|| {
                ActrCliError::command_error(format!(
                    "Could not determine {PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} version"
                ))
            })?;

        if version == required_version {
            return Ok(());
        }

        Err(ActrCliError::command_error(format!(
            "{PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} version {version} does not match actr version {required_version}. Install the plugin asset from the matching ACTR release."
        )))
    }

    /// Collect generated `*_actor.kt` file paths from the output directory.
    #[allow(dead_code)] // Used by future workload-name discovery (aligned with Swift codegen)
    fn generated_actor_files(&self, output_dir: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = WalkDir::new(output_dir)
            .min_depth(1)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.into_path())
            .filter(|path| {
                path.is_file()
                    && path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.ends_with("_actor.kt"))
            })
            .collect();
        paths.sort();
        paths
    }

    /// Get Kotlin package name - infer from output path or use default
    fn get_kotlin_package(&self, context: &GenContext) -> String {
        // Try to infer package from output path
        // e.g., ".../java/io/actrium/testkotlinecho/generated" -> "io.actrium.testkotlinecho.generated"
        let output_str = context.output.to_string_lossy();
        debug!("get_kotlin_package: output_str = {}", output_str);

        // Look for common Java/Kotlin source roots
        for marker in &["/java/", "/kotlin/"] {
            if let Some(pos) = output_str.find(marker) {
                let after_marker = &output_str[pos + marker.len()..];
                // Convert path to package name (replace / with .)
                let package = after_marker.replace(['/', '\\'], ".");
                debug!(
                    "get_kotlin_package: found marker {}, package = {}",
                    marker, package
                );
                if !package.is_empty() {
                    return package;
                }
            }
        }

        // Fallback to default
        debug!("get_kotlin_package: using fallback com.example.generated");
        "com.example.generated".to_string()
    }

    /// Analyze proto file to determine if it's local or remote
    /// Convention: files under "local/" are local, files under "remote/" are remote
    ///
    /// Now reads actr_type from manifest.lock.toml instead of inferring from directory names.
    /// Returns None if the proto file has no service definitions (skip it).
    #[allow(dead_code)]
    fn analyze_proto_file(
        &self,
        proto_path: &PathBuf,
        actr_type_map: &HashMap<String, String>,
    ) -> Option<ServiceInfo> {
        let path_str = proto_path.to_string_lossy();
        let is_local = path_str.contains("/local/");

        // Get directory name for remote services to look up in lock file
        let dir_name = proto_path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());

        // Get actr_type from lock file mapping (for remote services)
        let remote_target_type = if !is_local {
            if let Some(ref dir) = dir_name {
                actr_type_map.get(dir).cloned()
            } else {
                None
            }
        } else {
            None
        };

        // Read service name from proto file directly
        let proto_content = std::fs::read_to_string(proto_path).unwrap_or_default();

        // Extract service name from proto file
        // Look for "service ServiceName {"
        // If no service definition found, return None (skip this proto file)
        let service_name = proto_content
            .lines()
            .find(|l| l.trim().starts_with("service ") && l.contains("{"))
            .and_then(|l| {
                let trimmed = l.trim();
                let after_service = trimmed.strip_prefix("service ")?;
                let name_end = after_service.find([' ', '{'])?;
                Some(after_service[..name_end].trim().to_string())
            });

        // If no service definition found, skip this proto file
        let service_name = match service_name {
            Some(name) => name,
            None => {
                debug!(
                    "analyze_proto_file: {} has no service definition, skipping",
                    proto_path.display()
                );
                return None;
            }
        };

        // Read proto package from the proto file
        let proto_package = proto_content
            .lines()
            .find(|l| l.starts_with("package "))
            .and_then(|l| l.strip_prefix("package "))
            .and_then(|l| l.strip_suffix(";"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| {
                let file_stem = proto_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown");
                file_stem.to_lowercase().replace('-', "_")
            });

        // Extract RPC methods from proto file
        let mut methods = Vec::new();
        for line in proto_content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("rpc ") {
                // Parse: rpc method_name(request_type) returns (response_type);
                if let Some(rpc_content) = trimmed.strip_prefix("rpc ")
                    && let Some(semicolon_pos) = rpc_content.find(';')
                {
                    let rpc_def = &rpc_content[..semicolon_pos];
                    // Split by " returns "
                    if let Some((method_and_req, resp_part)) = rpc_def.split_once(" returns ") {
                        // Parse method name and request type
                        if let Some((method_name, req_part)) = method_and_req.split_once('(') {
                            let method_name = to_snake_case(method_name.trim());
                            if let Some(req_type) = req_part.strip_suffix(')') {
                                let request_type = req_type.trim().to_string();
                                // Parse response type
                                if let Some(resp_type) = resp_part
                                    .strip_prefix('(')
                                    .and_then(|s| s.strip_suffix(')'))
                                {
                                    let response_type = resp_type.trim().to_string();
                                    methods.push(MethodInfo {
                                        name: method_name,
                                        request_type,
                                        response_type,
                                        ..Default::default()
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        // Determine if the outer class needs "OuterClass" suffix
        // protobuf-java adds this suffix when the file name (in PascalCase) conflicts
        // with a message, service, or enum name defined in the proto file.
        //
        // Example: stream_client.proto -> StreamClient (PascalCase)
        //          If there's "message StreamClient" or "service StreamClient" -> needs suffix
        //
        // Example: echo.proto -> Echo (PascalCase)
        //          If there's "service EchoService" (different) -> no suffix needed

        // Convert file name to PascalCase (what protobuf would use as outer class name)
        let file_stem = proto_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown");
        let outer_class_base_name = to_pascal_case(file_stem);

        // Extract all message, service, and enum names from proto
        let mut declared_names: Vec<String> = Vec::new();

        for line in proto_content.lines() {
            let trimmed = line.trim();

            // Check for message declarations
            if trimmed.starts_with("message ")
                && let Some(name) = trimmed
                    .strip_prefix("message ")
                    .and_then(|s| s.split_whitespace().next())
                    .map(|s| s.trim_end_matches('{'))
            {
                declared_names.push(name.to_string());
            }

            // Check for service declarations
            if trimmed.starts_with("service ")
                && let Some(name) = trimmed
                    .strip_prefix("service ")
                    .and_then(|s| s.split_whitespace().next())
                    .map(|s| s.trim_end_matches('{'))
            {
                declared_names.push(name.to_string());
            }

            // Check for enum declarations
            if trimmed.starts_with("enum ")
                && let Some(name) = trimmed
                    .strip_prefix("enum ")
                    .and_then(|s| s.split_whitespace().next())
                    .map(|s| s.trim_end_matches('{'))
            {
                declared_names.push(name.to_string());
            }
        }

        let needs_outer_class_suffix = declared_names.contains(&outer_class_base_name);

        debug!(
            "analyze_proto_file: {} -> service={}, package={}, is_local={}, remote_target_type={:?}, methods={}, outer_class_base={}, declared_names={:?}, needs_suffix={}",
            proto_path.display(),
            service_name,
            proto_package,
            is_local,
            remote_target_type,
            methods.len(),
            outer_class_base_name,
            declared_names,
            needs_outer_class_suffix
        );

        Some(ServiceInfo {
            service_name,
            proto_package,
            is_local,
            remote_target_type,
            methods,
        })
    }

    /// Load manifest.lock.toml and build a mapping from dependency name to canonical actr_type.
    /// Returns a HashMap where key is the dependency name (e.g., "echo-real-server")
    /// and value is the actr_type (e.g., "acme:EchoService")
    #[allow(dead_code)]
    fn load_actr_type_map(&self, context: &GenContext) -> Result<HashMap<String, String>> {
        // Find project root by looking for manifest.lock.toml relative to input path
        // The input path is typically "protos" or a similar directory
        let project_root = context.input_path.parent().unwrap_or(&context.input_path);
        let lock_file_path = project_root.join("manifest.lock.toml");

        debug!(
            "load_actr_type_map: looking for lock file at {:?}",
            lock_file_path
        );

        if !lock_file_path.exists() {
            return Err(ActrCliError::config_error(format!(
                "manifest.lock.toml not found at {:?}.\n\
                 Please run 'actr deps install' first to generate the lock file.",
                lock_file_path
            )));
        }

        let lock_file = LockFile::from_file(&lock_file_path).map_err(|e| {
            ActrCliError::config_error(format!(
                "Failed to parse manifest.lock.toml: {}\n\
                 Please run 'actr deps install' to regenerate the lock file.",
                e
            ))
        })?;

        // Build the mapping: dependency name -> canonical actr_type
        let mut map = HashMap::new();
        for dep in &lock_file.dependencies {
            debug!("load_actr_type_map: {} -> {}", dep.name, dep.actr_type);
            map.insert(dep.name.clone(), dep.actr_type.clone());
        }

        info!(
            "📦 Loaded {} dependencies from manifest.lock.toml",
            map.len()
        );
        Ok(map)
    }

    /// Collect all service information from proto files
    /// Skips proto files that have no service definitions
    fn collect_services(&self, catalog: &ScaffoldCatalog) -> Result<Vec<ServiceInfo>> {
        catalog
            .local_services
            .iter()
            .cloned()
            .map(|service| kotlin_service_info(service, true))
            .chain(
                catalog
                    .remote_services
                    .iter()
                    .cloned()
                    .map(|service| kotlin_service_info(service, false)),
            )
            .collect()
    }

    /// Generate unified infrastructure code
    fn generate_unified_infrastructure(
        &self,
        services: &[ServiceInfo],
        kotlin_package: &str,
        context: &GenContext,
    ) -> Result<String> {
        let local_services: Vec<_> = services.iter().filter(|s| s.is_local).collect();
        let remote_services: Vec<_> = services.iter().filter(|s| !s.is_local).collect();
        let manifest_resolver_import = if remote_services.is_empty() {
            ""
        } else {
            "import io.actrium.actr.resolveManifestDependency\n"
        };

        let mut code = String::new();

        // Header
        code.push_str(&format!(
            r#"/**
 * Auto-generated Unified Actor Code - DO NOT EDIT
 *
 * Generated by actr gen command
 *
 * This file contains:
 * - UnifiedHandler interface combining all local service handlers
 * - UnifiedDispatcher for routing requests to local handlers or remote services
 *
 * Local services: {local_count}
 * Remote services: {remote_count}
 */
package {kotlin_package}

import io.actrium.actr.ActrId
import io.actrium.actr.ActrType
import io.actrium.actr.PayloadType
import io.actrium.actr.dsl.ActrContext
import io.actrium.actr.dsl.RpcEnvelope
{manifest_resolver_import}

"#,
            local_count = local_services.len(),
            remote_count = remote_services.len(),
            kotlin_package = kotlin_package,
            manifest_resolver_import = manifest_resolver_import,
        ));

        // Import protobuf message types for all services.
        // Protobuf Java Lite generates one outer class per proto file (PascalCase
        // of the file stem, +`OuterClass` on name collisions); import every
        // declaring file referenced by the services' RPC methods so imported
        // types resolve to their real owner instead of the local service's
        // package.
        code.push_str("// Import protobuf message types\n");
        let type_imports = kotlin_type_imports(services);
        if !type_imports.is_empty() {
            code.push_str(&type_imports);
            code.push('\n');
        }
        code.push('\n');

        // Import individual service handlers and dispatchers
        for service in &local_services {
            code.push_str(&format!(
                "// Local service\nimport {}.{}Handler\nimport {}.{}Dispatcher\n",
                kotlin_package, service.service_name, kotlin_package, service.service_name
            ));
        }
        code.push('\n');

        // Generate UnifiedHandler interface (only for local services)
        if !local_services.is_empty() {
            code.push_str(&self.generate_unified_handler(&local_services));
            code.push('\n');
        }

        // Generate RemoteServiceRegistry for remote service discovery
        if !remote_services.is_empty() {
            let dependency_aliases = self.remote_dependency_aliases_by_type(context);
            code.push_str(
                &self.generate_remote_service_registry(&remote_services, &dependency_aliases)?,
            );
            code.push('\n');
        }

        // Generate UnifiedDispatcher
        code.push_str(&self.generate_unified_dispatcher(&local_services, &remote_services));

        Ok(code)
    }

    /// Generate UnifiedHandler interface
    fn generate_unified_handler(&self, local_services: &[&ServiceInfo]) -> String {
        let handler_extends: Vec<_> = local_services
            .iter()
            .map(|s| format!("{}Handler", s.service_name))
            .collect();

        format!(
            r#"/**
 * Unified Handler interface combining all local service handlers
 *
 * Implement this interface to provide your business logic for all local services.
 */
interface UnifiedHandler : {} {{
    // All methods are inherited from individual service handlers
}}
"#,
            handler_extends.join(", ")
        )
    }

    /// Generate RemoteServiceRegistry for managing remote service discovery
    fn remote_dependency_aliases_by_type(&self, context: &GenContext) -> HashMap<String, String> {
        context
            .config
            .dependencies
            .iter()
            .filter_map(|dependency| {
                dependency
                    .actr_type
                    .as_ref()
                    .map(|actr_type| (actr_type.to_string_repr(), dependency.alias.clone()))
            })
            .collect()
    }

    fn generate_remote_service_registry(
        &self,
        remote_services: &[&ServiceInfo],
        dependency_aliases_by_type: &HashMap<String, String>,
    ) -> Result<String> {
        let mut code = String::new();

        code.push_str(
            r#"/**
 * Remote Service Route prefixes and their manifest dependency aliases
 *
 * Used by UnifiedDispatcher to route requests to remote services.
 */
object RemoteServiceRegistry {
    /**
     * Map of route key prefix to manifest dependency alias for remote services.
     */
    val remoteRouteAliases: Map<String, String> = mapOf(
"#,
        );

        for service in remote_services {
            let actor_type_raw = service.remote_target_type.as_ref().ok_or_else(|| {
                ActrCliError::config_error(format!(
                    "Missing actr_type for remote service '{}'",
                    service.service_name
                ))
            })?;
            let dependency_alias = dependency_aliases_by_type.get(actor_type_raw).ok_or_else(|| {
                ActrCliError::config_error(format!(
                    "Missing manifest dependency alias for remote service '{}' with actr_type '{}'",
                    service.service_name, actor_type_raw
                ))
            })?;
            // Extract service base name without "Service" suffix for route key
            let service_base = service.service_name.replace("Service", "");
            code.push_str(&format!(
                "        \"{}.{}\" to \"{}\",\n",
                service.proto_package, service_base, dependency_alias
            ));
        }

        code.push_str(
            r#"    )

    /**
     * Check if a route key belongs to a remote service
     */
    fun isRemoteRoute(routeKey: String): Boolean {
        return remoteRouteAliases.keys.any { routeKey.startsWith(it) }
    }

    /**
     * Resolve remote route targets from a runtime manifest.
     */
    fun resolveRemoteTargets(manifestPath: String): Map<String, ActrType> {
        val targetsByAlias = remoteRouteAliases.values
            .toSet()
            .associateWith { alias ->
                resolveManifestDependency(manifestPath, alias)
            }
        return remoteRouteAliases.mapValues { (_, alias) ->
            targetsByAlias.getValue(alias)
        }
    }

    /**
     * Get the actor type for a remote route from runtime-resolved targets.
     */
    fun getActorType(routeKey: String, remoteTargets: Map<String, ActrType>): ActrType? {
        val routePrefix = remoteRouteAliases.keys.find { routeKey.startsWith(it) }
        return routePrefix?.let { remoteTargets[it] }
    }
}
"#,
        );

        Ok(code)
    }

    /// Generate UnifiedDispatcher
    fn generate_unified_dispatcher(
        &self,
        local_services: &[&ServiceInfo],
        remote_services: &[&ServiceInfo],
    ) -> String {
        let mut local_dispatch_cases = String::new();
        for service in local_services {
            let service_base = service.service_name.replace("Service", "");
            local_dispatch_cases.push_str(&format!(
                r#"            // Local: {service_name}
            routeKey.startsWith("{proto_package}.{service_base}") -> {{
                {service_name}Dispatcher.dispatch(handler, ctx, envelope)
            }}
"#,
                service_name = service.service_name,
                proto_package = service.proto_package,
                service_base = service_base,
            ));
        }

        let has_remote = !remote_services.is_empty();
        let has_local = !local_services.is_empty();

        let handler_param = if has_local {
            "handler: UnifiedHandler,\n        "
        } else {
            ""
        };

        let remote_dispatch = if has_remote {
            r#"
            // Check if this is a remote service call
            RemoteServiceRegistry.isRemoteRoute(routeKey) -> {
                // Get target actor type and discover it
                val actrType = RemoteServiceRegistry.getActorType(routeKey, remoteTargets)
                    ?: throw IllegalArgumentException("Unknown remote route: $routeKey")

                val targetId = resolveRemoteActor(ctx, actrType)

                try {
                    ctx.callRaw(
                        targetId,
                        routeKey,
                        PayloadType.RPC_RELIABLE,
                        envelope.payload,
                        30000L
                    )
                } catch (original: Exception) {
                    invalidateRemoteActor(actrType)
                    val freshTargetId = resolveRemoteActor(ctx, actrType)
                    try {
                        ctx.callRaw(
                            freshTargetId,
                            routeKey,
                            PayloadType.RPC_RELIABLE,
                            envelope.payload,
                            30000L
                        )
                    } catch (retry: Exception) {
                        throw IllegalStateException(
                            "Remote route $routeKey failed after rediscovery: ${retry.message}",
                            retry
                        )
                    }
                }
            }
"#
        } else {
            ""
        };

        let discovered_actors_field = if has_remote {
            r#"
    // Cache for discovered remote actors
    private val discoveredActors = mutableMapOf<ActrType, ActrId>()

    private suspend fun resolveRemoteActor(ctx: ActrContext, actrType: ActrType): ActrId {
        return discoveredActors[actrType] ?: ctx.discover(actrType).also { discoveredActors[actrType] = it }
    }

    private fun invalidateRemoteActor(actrType: ActrType) {
        discoveredActors.remove(actrType)
    }

    /**
     * Discover all remote services
     *
     * Call this in your Workload's onStart method to pre-discover remote actors.
     */
    suspend fun discoverRemoteServices(ctx: ActrContext, remoteTargets: Map<String, ActrType>) {
        for ((_, actrType) in remoteTargets) {
            if (!discoveredActors.containsKey(actrType)) {
                val actorId = ctx.discover(actrType)
                discoveredActors[actrType] = actorId
            }
        }
    }

    /**
     * Clear discovered actors cache
     */
    fun clearDiscoveredActors() {
        discoveredActors.clear()
    }
"#
        } else {
            ""
        };
        let remote_targets_param = if has_remote {
            "remoteTargets: Map<String, ActrType>,\n        "
        } else {
            ""
        };

        format!(
            r#"/**
 * Unified Dispatcher for routing requests
 *
 * Routes requests to:
 * - Local service handlers for local routes
 * - Remote actors via RPC for remote routes
 */
object UnifiedDispatcher {{
{discovered_actors_field}
    /**
     * Dispatch an RPC envelope to the appropriate handler or remote service
     *
     * @param handler The unified handler implementation (for local services)
     * @param ctx The actor context for making remote calls
     * @param envelope The RPC envelope containing the request
     * @return The serialized response bytes
     */
    suspend fun dispatch(
        {handler_param}ctx: ActrContext,
        {remote_targets_param}
        envelope: RpcEnvelope
    ): ByteArray {{
        val routeKey = envelope.routeKey

        return when {{
{local_dispatch_cases}{remote_dispatch}
            else -> throw IllegalArgumentException("Unknown route key: $routeKey")
        }}
    }}
}}
"#,
            discovered_actors_field = discovered_actors_field,
            handler_param = handler_param,
            remote_targets_param = remote_targets_param,
            local_dispatch_cases = local_dispatch_cases,
            remote_dispatch = remote_dispatch,
        )
    }
}

#[async_trait]
impl LanguageGenerator for KotlinGenerator {
    async fn generate_infrastructure(&self, context: &GenContext) -> Result<Vec<PathBuf>> {
        info!("🔧 Generating Kotlin Actor infrastructure code...");

        // Find or build the Kotlin plugin
        let plugin_path = self.ensure_required_tools()?;
        info!("✅ Using Kotlin plugin: {:?}", plugin_path);

        let kotlin_package = self.get_kotlin_package(context);
        let mut generated_files = Vec::new();
        let proto_root = if context.input_path.is_file() {
            context
                .input_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf()
        } else {
            context.input_path.clone()
        };
        let (local_files, remote_files, remote_file_mapping) =
            kotlin_plugin_file_options(context, &proto_root);

        let mut options = vec![format!("kotlin_package={}", kotlin_package)];
        if !local_files.is_empty() {
            options.push(format!("LocalFiles={}", local_files.join(":")));
        }
        if !remote_files.is_empty() {
            options.push(format!("RemoteFiles={}", remote_files.join(":")));
        }
        if !remote_file_mapping.is_empty() {
            options.push(format!(
                "RemoteFileMapping={}",
                remote_file_mapping.join(";")
            ));
        }

        // Invoke the ACTR framework plugin once with the full descriptor graph.
        let mut cmd = StdCommand::new("protoc");
        cmd.arg(format!("--proto_path={}", proto_root.display()))
            .arg(format!(
                "--plugin=protoc-gen-actrframework-kotlin={}",
                plugin_path.display()
            ))
            .arg(format!("--actrframework-kotlin_opt={}", options.join(",")))
            .arg(format!(
                "--actrframework-kotlin_out={}",
                context.output.display()
            ));

        for proto_file in &context.proto_files {
            cmd.arg(proto_file);
        }

        debug!("Executing protoc (actrframework-kotlin): {:?}", cmd);
        let output = cmd
            .output()
            .map_err(|e| ActrCliError::command_error(format!("Failed to execute protoc: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "protoc (actrframework-kotlin) execution failed: {stderr}"
            )));
        }

        for proto_file in &context.proto_files {
            let service_name = proto_file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            let generated_file = context.output.join(format!("{}_actor.kt", service_name));
            if generated_file.exists() {
                generated_files.push(generated_file);
            }
        }

        // NOW collect service info (after per-service files are generated)
        let catalog = ScaffoldCatalog::load(context, SupportedLanguage::Kotlin)?;
        let services = self.collect_services(&catalog)?;
        info!(
            "📊 Found {} services ({} local, {} remote)",
            services.len(),
            services.iter().filter(|s| s.is_local).count(),
            services.iter().filter(|s| !s.is_local).count()
        );

        // Generate unified infrastructure file
        let unified_code =
            self.generate_unified_infrastructure(&services, &kotlin_package, context)?;
        let unified_file = context.output.join("unified_actor.kt");
        std::fs::write(&unified_file, &unified_code).map_err(|e| {
            ActrCliError::config_error(format!("Failed to write unified_actor.kt: {e}"))
        })?;
        generated_files.push(unified_file);
        info!("📄 Generated unified_actor.kt");

        info!(
            "✅ Generated {} Kotlin infrastructure files",
            generated_files.len()
        );
        Ok(generated_files)
    }

    async fn generate_scaffold(
        &self,
        context: &GenContext,
        catalog: &ScaffoldCatalog,
    ) -> Result<Vec<PathBuf>> {
        info!("📝 Generating Kotlin user code scaffold...");

        let mut generated_files = Vec::new();
        let kotlin_package = self.get_kotlin_package(context);
        let services = self.collect_services(catalog)?;

        let output_dir = context.output.parent().unwrap_or(&context.output);

        // Generate unified workload
        let unified_workload_file = output_dir.join("UnifiedWorkload.kt");
        if !unified_workload_file.exists() || context.overwrite_user_code {
            let unified_workload_content =
                generate_unified_workload_scaffold(&services, &kotlin_package);
            std::fs::write(&unified_workload_file, &unified_workload_content).map_err(|e| {
                ActrCliError::config_error(format!("Failed to write UnifiedWorkload.kt: {e}"))
            })?;
            info!("📄 Generated UnifiedWorkload.kt");
            generated_files.push(unified_workload_file);
        } else {
            info!("⏭️  Skipping existing UnifiedWorkload.kt");
        }

        // Generate lifecycle adapter
        let lifecycle_adapter_file = output_dir.join("UnifiedLifecycleAdapter.kt");
        if !lifecycle_adapter_file.exists() || context.overwrite_user_code {
            let lifecycle_adapter_content =
                generate_unified_lifecycle_adapter_scaffold(&kotlin_package);
            std::fs::write(&lifecycle_adapter_file, &lifecycle_adapter_content).map_err(|e| {
                ActrCliError::config_error(format!(
                    "Failed to write UnifiedLifecycleAdapter.kt: {e}"
                ))
            })?;
            info!("📄 Generated UnifiedLifecycleAdapter.kt");
            generated_files.push(lifecycle_adapter_file);
        } else {
            info!("⏭️  Skipping existing UnifiedLifecycleAdapter.kt");
        }

        // Generate unified handler implementation
        let unified_handler_file = output_dir.join("MyUnifiedHandler.kt");
        if !unified_handler_file.exists() || context.overwrite_user_code {
            let unified_handler_content =
                generate_unified_handler_scaffold(&services, &kotlin_package);
            std::fs::write(&unified_handler_file, &unified_handler_content).map_err(|e| {
                ActrCliError::config_error(format!("Failed to write MyUnifiedHandler.kt: {e}"))
            })?;
            info!("📄 Generated MyUnifiedHandler.kt");
            generated_files.push(unified_handler_file);
        } else {
            info!("⏭️  Skipping existing MyUnifiedHandler.kt");
        }

        Ok(generated_files)
    }

    async fn format_code(&self, _context: &GenContext, files: &[PathBuf]) -> Result<()> {
        info!("🎨 Formatting Kotlin code...");

        // Try to use ktlint if available
        let ktlint_check = StdCommand::new("which").arg("ktlint").output();

        if let Ok(output) = ktlint_check {
            if output.status.success() {
                for file in files {
                    let mut cmd = StdCommand::new("ktlint");
                    cmd.arg("-F").arg(file);

                    let output = cmd.output();
                    if let Err(e) = output {
                        warn!("ktlint formatting failed for {:?}: {}", file, e);
                    }
                }
                info!("✅ Kotlin code formatted with ktlint");
            } else {
                info!("💡 ktlint not found, skipping formatting");
            }
        }

        Ok(())
    }

    async fn validate_code(&self, context: &GenContext) -> Result<()> {
        info!("🔍 Validating Kotlin code...");

        // Check if generated files exist
        let generated_dir = &context.output;
        if !generated_dir.exists() {
            return Err(ActrCliError::config_error(
                "Generated output directory does not exist",
            ));
        }

        let kt_files: Vec<_> = std::fs::read_dir(generated_dir)
            .map_err(|e| {
                ActrCliError::config_error(format!("Failed to read output directory: {e}"))
            })?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|ext| ext == "kt").unwrap_or(false))
            .collect();

        if kt_files.is_empty() {
            warn!("No Kotlin files found in output directory");
        } else {
            info!("✅ Found {} Kotlin files", kt_files.len());
        }

        // Note: Full compilation validation would require a Kotlin compiler setup
        info!("💡 For full validation, compile the Kotlin project with gradle/kotlinc");

        Ok(())
    }

    fn print_next_steps(&self, context: &GenContext) {
        println!("\n🎉 Kotlin code generation completed!");
        println!("\n📋 Next steps:");
        println!("1. 📖 View generated code: {:?}", context.output);
        println!("2. 📦 Ensure protobuf gradle plugin is configured for message classes");
        println!("3. ✏️  Implement MyUnifiedHandler with your business logic");
        println!(
            "4. 🚀 Wrap UnifiedWorkload with UnifiedLifecycleAdapter and pass adapter.toDynamicWorkload() to ActrNode.linked"
        );
        println!("5. 🏗️  Build project: ./gradlew build");
        println!("6. 🧪 Run tests: ./gradlew connectedAndroidTest");
        println!(
            "\n💡 Tip: The UnifiedDispatcher routes local requests to your handler and remote requests via RPC"
        );
    }
}

fn kotlin_service_info(service: ScaffoldService, is_local: bool) -> Result<ServiceInfo> {
    let methods = service
        .methods
        .into_iter()
        .map(|method| {
            let request_type = method.input_ref.generated_type.ok_or_else(|| {
                ActrCliError::config_error(format!(
                    "Kotlin plugin metadata is missing generated_type for {}.{} input",
                    service.name, method.name
                ))
            })?;
            let response_type = method.output_ref.generated_type.ok_or_else(|| {
                ActrCliError::config_error(format!(
                    "Kotlin plugin metadata is missing generated_type for {}.{} output",
                    service.name, method.name
                ))
            })?;
            Ok(MethodInfo {
                name: method.snake_name,
                request_type,
                response_type,
                request_import: String::new(),
                response_import: String::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(ServiceInfo {
        service_name: service.name,
        proto_package: service.package,
        is_local,
        remote_target_type: service.actr_type,
        methods,
    })
}

/// Convert a string to PascalCase
fn to_pascal_case(s: &str) -> String {
    s.split(['_', '-'])
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

/// Distinct Kotlin imports requested by service metadata.
fn kotlin_type_imports<'a>(services: impl IntoIterator<Item = &'a ServiceInfo>) -> String {
    let mut imports: BTreeSet<String> = BTreeSet::new();
    for service in services {
        for method in &service.methods {
            if !method.request_import.is_empty() {
                imports.insert(method.request_import.clone());
            }
            if !method.response_import.is_empty() {
                imports.insert(method.response_import.clone());
            }
        }
    }
    imports
        .into_iter()
        .map(|path| format!("import {}.*", path))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generate unified workload scaffold
fn generate_unified_workload_scaffold(services: &[ServiceInfo], kotlin_package: &str) -> String {
    let base_package = kotlin_package
        .strip_suffix(".generated")
        .unwrap_or(kotlin_package);

    let has_local = services.iter().any(|s| s.is_local);
    let has_remote = services.iter().any(|s| !s.is_local);

    let handler_field = if has_local {
        "private val handler: UnifiedHandler,"
    } else {
        ""
    };

    let handler_import = if has_local {
        format!("\nimport {}.UnifiedHandler", kotlin_package)
    } else {
        String::new()
    };

    let discover_call = if has_remote {
        r#"
        // Discover all remote services
        Log.i(TAG, "📡 Discovering remote services...")
        UnifiedDispatcher.discoverRemoteServices(ctx, remoteTargets)
        Log.i(TAG, "✅ Remote services discovered")"#
    } else {
        ""
    };

    let remote_constructor_params = if has_remote {
        "private val remoteTargets: Map<String, ActrType>,\n    "
    } else {
        ""
    };

    let usage_example = match (has_local, has_remote) {
        (true, true) => format!(
            r#" * val handler = MyUnifiedHandler()
 * val remoteTargets =
 *     {kotlin_package}.RemoteServiceRegistry.resolveRemoteTargets(manifestPath)
 * val workload = UnifiedWorkload(
 *     handler = handler,
 *     remoteTargets = remoteTargets,
 * )
"#
        ),
        (false, true) => format!(
            r#" * val remoteTargets =
 *     {kotlin_package}.RemoteServiceRegistry.resolveRemoteTargets(manifestPath)
 * val workload = UnifiedWorkload(remoteTargets = remoteTargets)
"#
        ),
        (true, false) => {
            " * val handler = MyUnifiedHandler()\n * val workload = UnifiedWorkload(handler)\n"
                .to_string()
        }
        (false, false) => " * val workload = UnifiedWorkload()\n".to_string(),
    };

    let dispatch_handler = if has_local { "handler, " } else { "" };
    let dispatch_remote_targets = if has_remote { "remoteTargets, " } else { "" };

    format!(
        r#"/**
 * Unified Workload for all services
 *
 * This Workload handles both local and remote service requests using the UnifiedDispatcher.
 * Local requests are routed to your UnifiedHandler implementation.
 * Remote requests are forwarded to discovered remote actors.
 */
package {base_package}

import android.util.Log
import {kotlin_package}.UnifiedDispatcher{handler_import}
import io.actrium.actr.ActrType
import io.actrium.actr.dsl.ActrContext
import io.actrium.actr.dsl.ErrorEvent
import io.actrium.actr.dsl.RpcEnvelope

/**
 * Unified Workload lifecycle scaffold
 *
 * This can handle dispatch and lifecycle-like callbacks.
 * UnifiedLifecycleAdapter wraps it for the SDK-facing lifecycle bridge.
 *
 * Usage:
 * ```kotlin
{usage_example}
 * val lifecycle = UnifiedLifecycleAdapter(workload)
 * val dynamicWorkload = lifecycle.toDynamicWorkload()
 * ```
 */
class UnifiedWorkload(
    {handler_field}
    {remote_constructor_params}
) {{

    companion object {{
        private const val TAG = "UnifiedWorkload"
    }}

    suspend fun onStart(ctx: ActrContext) {{
        Log.i(TAG, "UnifiedWorkload.onStart"){discover_call}
    }}

    suspend fun onReady(ctx: ActrContext) {{
        Log.i(TAG, "UnifiedWorkload.onReady")
    }}

    suspend fun onStop(ctx: ActrContext) {{
        Log.i(TAG, "UnifiedWorkload.onStop")
    }}

    suspend fun onError(ctx: ActrContext, event: ErrorEvent) {{
        Log.e(TAG, "UnifiedWorkload.onError: $event")
    }}

    /**
     * Dispatch RPC requests
     *
     * Uses the UnifiedDispatcher to route requests to:
     * - Local handler methods for local service routes
     * - Remote actors for remote service routes
     */
    suspend fun dispatch(ctx: ActrContext, envelope: RpcEnvelope): ByteArray {{
        Log.i(TAG, "🔀 dispatch() called")
        Log.i(TAG, "   route_key: ${{envelope.routeKey}}")
        Log.i(TAG, "   request_id: ${{envelope.requestId}}")
        Log.i(TAG, "   payload size: ${{envelope.payload.size}} bytes")

        return UnifiedDispatcher.dispatch({dispatch_handler}ctx, {dispatch_remote_targets}envelope)
    }}
}}
"#,
        base_package = base_package,
        kotlin_package = kotlin_package,
        handler_import = handler_import,
        handler_field = handler_field,
        remote_constructor_params = remote_constructor_params,
        usage_example = usage_example,
        discover_call = discover_call,
        dispatch_handler = dispatch_handler,
        dispatch_remote_targets = dispatch_remote_targets,
    )
}

/// Generate lifecycle adapter scaffold
fn generate_unified_lifecycle_adapter_scaffold(kotlin_package: &str) -> String {
    let base_package = kotlin_package
        .strip_suffix(".generated")
        .unwrap_or(kotlin_package);

    format!(
        r#"/**
 * Lifecycle adapter for UnifiedWorkload
 *
 * This adapter is the SDK-facing lifecycle bridge. Keep business logic in
 * [UnifiedWorkload] and keep generated dispatch glue under the generated package.
 */
package {base_package}

import io.actrium.actr.dsl.ActrContext
import io.actrium.actr.dsl.DynamicWorkload
import io.actrium.actr.dsl.ErrorEvent
import io.actrium.actr.dsl.RpcEnvelope
import io.actrium.actr.dsl.Workload
import io.actrium.actr.dsl.dynamicWorkload

class UnifiedLifecycleAdapter(
    private val workload: UnifiedWorkload
) : Workload {{

    override suspend fun onStart(ctx: ActrContext) {{
        workload.onStart(ctx)
    }}

    override suspend fun onReady(ctx: ActrContext) {{
        workload.onReady(ctx)
    }}

    override suspend fun onStop(ctx: ActrContext) {{
        workload.onStop(ctx)
    }}

    override suspend fun onError(ctx: ActrContext, event: ErrorEvent) {{
        workload.onError(ctx, event)
    }}

    override suspend fun dispatch(ctx: ActrContext, envelope: RpcEnvelope): ByteArray {{
        return workload.dispatch(ctx, envelope)
    }}

    fun toDynamicWorkload(): DynamicWorkload {{
        return dynamicWorkload(
            lifecycle = this,
            signaling = null,
            websocket = null,
            webrtc = null,
            credential = null,
            mailbox = null
        )
    }}
}}
"#,
        base_package = base_package,
    )
}

/// Generate unified handler implementation scaffold
fn generate_unified_handler_scaffold(services: &[ServiceInfo], kotlin_package: &str) -> String {
    let base_package = kotlin_package
        .strip_suffix(".generated")
        .unwrap_or(kotlin_package);

    let local_services: Vec<_> = services.iter().filter(|s| s.is_local).collect();

    if local_services.is_empty() {
        return format!(
            r#"/**
 * No local services - this file is a placeholder
 *
 * All services are remote and will be handled by the UnifiedDispatcher.
 */
package {base_package}

// No local handler needed - all services are remote
"#,
            base_package = base_package,
        );
    }

    let mut imports = String::new();
    let mut method_impls = String::new();

    // Import every declaring proto file referenced by the local services' RPC
    // methods so imported types resolve to their real owner outer class.
    let type_imports = kotlin_type_imports(local_services.iter().copied());
    if !type_imports.is_empty() {
        imports.push_str(&type_imports);
        imports.push('\n');
    }

    for service in &local_services {
        // Generate method implementations for each RPC method
        for method in &service.methods {
            method_impls.push_str(&format!(
                r#"
    /**
     * Handle {} request for {} service
     *
     * @param request The {} request message
     * @param ctx Actor context for actor operations
     * @return {} response message
     */
    override suspend fun {}(request: {}, ctx: ActrContext): {} {{
        TODO("Not yet implemented")
    }}
"#,
                method.name,
                service.service_name,
                method.request_type,
                method.response_type,
                method.name,
                method.request_type,
                method.response_type
            ));
        }

        // Add a separator comment between services
        if !service.methods.is_empty() {
            method_impls.push_str(&format!(
                r#"
    // ===== End of {} methods =====
"#,
                service.service_name
            ));
        }
    }

    format!(
        r#"/**
 * Unified Handler Implementation
 *
 * This file provides the implementation for all local service handlers.
 * Implement your business logic in this class.
 */
package {base_package}

import android.util.Log
import {kotlin_package}.UnifiedHandler
import io.actrium.actr.dsl.ActrContext
{imports}

/**
 * Implementation of UnifiedHandler
 *
 * This class handles all local service requests.
 * Remote service requests are automatically forwarded by the UnifiedDispatcher.
 */
class MyUnifiedHandler : UnifiedHandler {{

    companion object {{
        private const val TAG = "MyUnifiedHandler"
    }}
{method_impls}
}}
"#,
        base_package = base_package,
        kotlin_package = kotlin_package,
        imports = imports,
        method_impls = method_impls,
    )
}

fn kotlin_plugin_file_options(
    context: &GenContext,
    proto_root: &Path,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut local_files = BTreeSet::new();
    let mut remote_files = BTreeSet::new();

    for proto_file in &context.proto_files {
        let relative = proto_file.strip_prefix(proto_root).unwrap_or(proto_file);
        let normalized = normalize_proto_path(relative);
        if normalized.starts_with("remote/") {
            remote_files.insert(normalized);
        } else {
            local_files.insert(normalized);
        }
    }

    let mut remote_file_mapping = BTreeSet::new();
    for service in &context.proto_model.remote_services {
        if let Some(actr_type) = &service.actr_type {
            remote_file_mapping.insert(format!(
                "{}={}",
                normalize_proto_path(&service.relative_path),
                actr_type
            ));
        }
    }

    (
        local_files.into_iter().collect(),
        remote_files.into_iter().collect(),
        remote_file_mapping.into_iter().collect(),
    )
}

fn normalize_proto_path(path: &Path) -> String {
    path.components()
        .filter_map(|component| component.as_os_str().to_str())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
#[path = "kotlin_tests.rs"]
mod tests;
