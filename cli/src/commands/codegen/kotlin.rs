use crate::commands::codegen::traits::{GenContext, LanguageGenerator};
use crate::error::{ActrCliError, Result};
use crate::utils::{command_exists, to_snake_case};
use actr_config::LockFile;
use actr_protocol::ActrType;
use async_trait::async_trait;
use std::collections::HashMap;
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
    /// Proto file name (e.g., "echo.proto")
    proto_file_name: String,
    /// Whether this is a local service (vs remote)
    is_local: bool,
    /// Remote target actor type (only for remote services)
    remote_target_type: Option<String>,
    /// List of RPC methods in this service
    methods: Vec<MethodInfo>,
    /// Whether the proto file outer class needs "OuterClass" suffix
    /// (true when file name PascalCase conflicts with a message/service/enum name)
    needs_outer_class_suffix: bool,
}

/// Information about an RPC method
#[derive(Debug, Clone)]
struct MethodInfo {
    /// Method name (e.g., "send_file")
    name: String,
    /// Request type (e.g., "SendFileRequest")
    request_type: String,
    /// Response type (e.g., "SendFileResponse")
    response_type: String,
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
                return Ok(PathBuf::from(path));
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
             For more information, visit: https://github.com/actor-rtc/framework-codegen-kotlin",
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
        let jar_candidates = [
            plugin_root.join("build/libs/protoc-gen-actrframework-kotlin-0.1.0.jar"),
            plugin_root.join("build/libs/protoc-gen-actrframework-kotlin.jar"),
        ];
        for candidate in jar_candidates {
            if candidate.is_file() {
                info!(
                    "✅ Using workspace-local {} JAR at {}",
                    PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN,
                    candidate.display()
                );
                // Return the wrapper script path (it knows how to invoke the JAR)
                if wrapper_script.is_file() {
                    return Ok(Some(wrapper_script));
                }
                // If no wrapper script, we can't use it directly via protoc --plugin
                break;
            }
        }

        Err(ActrCliError::command_error(format!(
            "workspace-local {PROTOC_GEN_ACTR_FRAMEWORK_KOTLIN} build completed but plugin not found under {}",
            plugin_root.display()
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
        // e.g., ".../java/io/actr/testkotlinecho/generated" -> "io.actr.testkotlinecho.generated"
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

        let proto_file_name = proto_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown.proto")
            .to_string();

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
            proto_file_name,
            is_local,
            remote_target_type,
            methods,
            needs_outer_class_suffix,
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
    fn collect_services(&self, context: &GenContext) -> Result<Vec<ServiceInfo>> {
        Ok(context
            .proto_model
            .files
            .iter()
            .flat_map(|file| {
                let outer_class_base_name = file
                    .proto_file
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(to_pascal_case)
                    .unwrap_or_else(|| "Unknown".to_string());
                let needs_outer_class_suffix =
                    file.declared_type_names.contains(&outer_class_base_name);

                file.services.iter().map(move |service| ServiceInfo {
                    service_name: service.name.clone(),
                    proto_package: service.package.clone(),
                    proto_file_name: file
                        .proto_file
                        .file_name()
                        .and_then(|name| name.to_str())
                        .unwrap_or("unknown.proto")
                        .to_string(),
                    is_local: service.side == crate::commands::codegen::ProtoSide::Local,
                    remote_target_type: service.actr_type.clone(),
                    methods: service
                        .methods
                        .iter()
                        .map(|method| MethodInfo {
                            name: method.snake_name.clone(),
                            request_type: method.input_type.clone(),
                            response_type: method.output_type.clone(),
                        })
                        .collect(),
                    needs_outer_class_suffix,
                })
            })
            .collect())
    }

    /// Generate unified infrastructure code
    fn generate_unified_infrastructure(
        &self,
        services: &[ServiceInfo],
        kotlin_package: &str,
    ) -> Result<String> {
        let local_services: Vec<_> = services.iter().filter(|s| s.is_local).collect();
        let remote_services: Vec<_> = services.iter().filter(|s| !s.is_local).collect();

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

import io.actor_rtc.actr.ActrId
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.ContextBridge
import io.actor_rtc.actr.PayloadType
import io.actor_rtc.actr.RpcEnvelopeBridge

"#,
            local_count = local_services.len(),
            remote_count = remote_services.len(),
            kotlin_package = kotlin_package,
        ));

        // Import protobuf message types for all services
        // Protobuf Java Lite generates outer class with PascalCase file name
        // The outer class name is file_name in PascalCase (e.g., echo.proto -> Echo, stream_client.proto -> StreamClient)
        // If the PascalCase name conflicts with a message/service/enum name, protobuf adds "OuterClass" suffix
        code.push_str("// Import protobuf message types\n");
        for service in services {
            let file_stem = service.proto_file_name.replace(".proto", "");
            let outer_class = if service.needs_outer_class_suffix {
                format!("{}OuterClass", to_pascal_case(&file_stem))
            } else {
                to_pascal_case(&file_stem)
            };
            code.push_str(&format!(
                "import {}.{}.*\n",
                service.proto_package, outer_class
            ));
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
            code.push_str(&self.generate_remote_service_registry(&remote_services)?);
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
    fn generate_remote_service_registry(&self, remote_services: &[&ServiceInfo]) -> Result<String> {
        let mut code = String::new();

        code.push_str(
            r#"/**
 * Remote Service Route prefixes and their corresponding actor types
 *
 * Used by UnifiedDispatcher to route requests to remote services.
 */
object RemoteServiceRegistry {
    /**
     * Map of route key prefix to actor type for remote services
     */
    val remoteRoutes: Map<String, ActrType> = mapOf(
"#,
        );

        for service in remote_services {
            let actor_type_raw = service.remote_target_type.as_ref().ok_or_else(|| {
                ActrCliError::config_error(format!(
                    "Missing actr_type for remote service '{}'",
                    service.service_name
                ))
            })?;
            let actor_type = ActrType::from_string_repr(actor_type_raw).map_err(|e| {
                ActrCliError::config_error(format!(
                    "Invalid remote actr_type '{}' for service '{}': {}",
                    actor_type_raw, service.service_name, e
                ))
            })?;
            // Extract service base name without "Service" suffix for route key
            let service_base = service.service_name.replace("Service", "");
            code.push_str(&format!(
                "        \"{}.{}\" to ActrType(manufacturer = \"{}\", name = \"{}\", version = \"{}\"),\n",
                service.proto_package,
                service_base,
                actor_type.manufacturer,
                actor_type.name,
                actor_type.version
            ));
        }

        code.push_str(
            r#"    )

    /**
     * Check if a route key belongs to a remote service
     */
    fun isRemoteRoute(routeKey: String): Boolean {
        return remoteRoutes.keys.any { routeKey.startsWith(it) }
    }

    /**
     * Get the actor type for a remote route
     */
    fun getActorType(routeKey: String): ActrType? {
        return remoteRoutes.entries.find { routeKey.startsWith(it.key) }?.value
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
                val actrType = RemoteServiceRegistry.getActorType(routeKey)
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

    private suspend fun resolveRemoteActor(ctx: ContextBridge, actrType: ActrType): ActrId {
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
    suspend fun discoverRemoteServices(ctx: ContextBridge) {
        for ((_, actrType) in RemoteServiceRegistry.remoteRoutes) {
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
     * @param ctx The context bridge for making remote calls
     * @param envelope The RPC envelope containing the request
     * @return The serialized response bytes
     */
    suspend fun dispatch(
        {handler_param}ctx: ContextBridge,
        envelope: RpcEnvelopeBridge
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

        // Generate per-service Handler and Dispatcher files FIRST
        for proto_file in &context.proto_files {
            debug!("Processing proto file: {:?}", proto_file);

            // Get the proto directory for include path
            let proto_dir = proto_file
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));

            // Use protoc with the Kotlin plugin
            let mut cmd = StdCommand::new("protoc");
            // Add the main input path (protos directory) as include path for imports
            cmd.arg(format!("--proto_path={}", context.input_path.display()))
                .arg(format!("--proto_path={}", proto_dir.display()))
                .arg(format!(
                    "--plugin=protoc-gen-actrframework-kotlin={}",
                    plugin_path.display()
                ))
                .arg(format!(
                    "--actrframework-kotlin_opt=kotlin_package={}",
                    kotlin_package
                ))
                .arg(format!(
                    "--actrframework-kotlin_out={}",
                    context.output.display()
                ))
                .arg(proto_file);

            debug!("Executing protoc: {:?}", cmd);
            let output = cmd.output().map_err(|e| {
                ActrCliError::command_error(format!("Failed to execute protoc: {e}"))
            })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(ActrCliError::command_error(format!(
                    "protoc (actrframework-kotlin) execution failed: {stderr}"
                )));
            }

            // Track generated files
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
        let services = self.collect_services(context)?;
        info!(
            "📊 Found {} services ({} local, {} remote)",
            services.len(),
            services.iter().filter(|s| s.is_local).count(),
            services.iter().filter(|s| !s.is_local).count()
        );

        // Generate unified infrastructure file
        let unified_code = self.generate_unified_infrastructure(&services, &kotlin_package)?;
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

    async fn generate_scaffold(&self, context: &GenContext) -> Result<Vec<PathBuf>> {
        if context.no_scaffold {
            return Ok(vec![]);
        }

        info!("📝 Generating Kotlin user code scaffold...");

        let mut generated_files = Vec::new();
        let kotlin_package = self.get_kotlin_package(context);
        let services = self.collect_services(context)?;

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
        println!("4. 🚀 Use UnifiedWorkload in your app");
        println!("5. 🏗️  Build project: ./gradlew build");
        println!("6. 🧪 Run tests: ./gradlew connectedAndroidTest");
        println!(
            "\n💡 Tip: The UnifiedDispatcher routes local requests to your handler and remote requests via RPC"
        );
    }
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
        UnifiedDispatcher.discoverRemoteServices(ctx)
        Log.i(TAG, "✅ Remote services discovered")"#
    } else {
        ""
    };

    let dispatch_handler = if has_local { "handler, " } else { "" };

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
import io.actor_rtc.actr.ActrId
import io.actor_rtc.actr.ActrType
import io.actor_rtc.actr.ContextBridge
import io.actor_rtc.actr.DynamicWorkload
import io.actor_rtc.actr.Realm
import io.actor_rtc.actr.RpcEnvelopeBridge
import io.actor_rtc.actr.WorkloadLifecycleBridge

/**
 * Unified Workload lifecycle scaffold
 *
 * This can already handle dispatch and lifecycle callbacks.
 * The generated [DynamicWorkload] wrapper is ready to use once
 * Kotlin bindings expose a linked-runtime node constructor.
 *
 * Usage:
 * ```kotlin
 * val handler = MyUnifiedHandler()
 * val workload = UnifiedWorkload(handler)
 * val dynamicWorkload = workload.toDynamicWorkload()
 * ```
 */
class UnifiedWorkload(
    {handler_field}
    private val realmId: UInt = 2368266035u
) : WorkloadLifecycleBridge {{

    companion object {{
        private const val TAG = "UnifiedWorkload"
    }}

    private val selfId = ActrId(
        realm = Realm(realmId = realmId),
        serialNumber = System.currentTimeMillis().toULong(),
        type = ActrType(manufacturer = "acme", name = "UnifiedActor", version = "1.0.0")
    )

    override suspend fun onStart(ctx: ContextBridge) {{
        Log.i(TAG, "UnifiedWorkload.onStart"){discover_call}
    }}

    override suspend fun onReady(ctx: ContextBridge) {{
        Log.i(TAG, "UnifiedWorkload.onReady")
    }}

    override suspend fun onStop(ctx: ContextBridge) {{
        Log.i(TAG, "UnifiedWorkload.onStop")
    }}

    override suspend fun onError(ctx: ContextBridge, event: io.actor_rtc.actr.ErrorEventBridge) {{
        Log.e(TAG, "UnifiedWorkload.onError: $event")
    }}

    /**
     * Dispatch RPC requests
     *
     * Uses the UnifiedDispatcher to route requests to:
     * - Local handler methods for local service routes
     * - Remote actors for remote service routes
     */
    override suspend fun dispatch(ctx: ContextBridge, envelope: RpcEnvelopeBridge): ByteArray {{
        Log.i(TAG, "🔀 dispatch() called")
        Log.i(TAG, "   route_key: ${{envelope.routeKey}}")
        Log.i(TAG, "   request_id: ${{envelope.requestId}}")
        Log.i(TAG, "   payload size: ${{envelope.payload.size}} bytes")

        return UnifiedDispatcher.dispatch({dispatch_handler}ctx, envelope)
    }}

    /**
     * Create a DynamicWorkload suitable for the linked() factory function.
     *
     * This wraps this lifecycle implementation into a DynamicWorkload
     * that can be passed to `linked(configPath, actorType, workload)`.
     */
    fun toDynamicWorkload(): DynamicWorkload {{
        return DynamicWorkload(
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
        kotlin_package = kotlin_package,
        handler_import = handler_import,
        handler_field = handler_field,
        discover_call = discover_call,
        dispatch_handler = dispatch_handler,
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

    for service in &local_services {
        let outer_class = if service.needs_outer_class_suffix {
            format!(
                "{}OuterClass",
                to_pascal_case(&service.proto_file_name.replace(".proto", ""))
            )
        } else {
            to_pascal_case(&service.proto_file_name.replace(".proto", ""))
        };
        imports.push_str(&format!(
            "import {}.{}.*\n",
            service.proto_package, outer_class
        ));

        // Generate method implementations for each RPC method
        for method in &service.methods {
            method_impls.push_str(&format!(
                r#"
    /**
     * Handle {} request for {} service
     *
     * @param request The {} request message
     * @param ctx Context bridge for actor operations
     * @return {} response message
     */
    override suspend fun {}(request: {}, ctx: ContextBridge): {} {{
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
import io.actor_rtc.actr.ContextBridge
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
