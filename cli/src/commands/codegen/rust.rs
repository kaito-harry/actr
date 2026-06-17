use crate::commands::SupportedLanguage;
use crate::commands::codegen::scaffold::{ScaffoldCatalog, ScaffoldService};
use crate::commands::codegen::traits::{GenContext, LanguageGenerator};
use crate::error::{ActrCliError, Result};
use crate::plugin_config::{load_protoc_plugin_config, version_is_at_least};
use crate::utils::to_snake_case;
use async_trait::async_trait;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use tracing::{debug, info, warn};

pub struct RustGenerator;

const HANDLER_SCAFFOLD_MARKER: &str = "// ACTR: generated Rust handler scaffold";
const ENTRY_SCAFFOLD_MARKER: &str = "// ACTR: generated Rust workload entry scaffold";

#[async_trait]
impl LanguageGenerator for RustGenerator {
    async fn generate_infrastructure(&self, context: &GenContext) -> Result<Vec<PathBuf>> {
        info!("🔧 Generating infrastructure code...");

        let prost_plugin_path = self.ensure_prost_plugin(&context.config_path)?;
        let plugin_path = self.ensure_protoc_plugin(&context.config_path)?;

        let manufacturer = context.config.package.actr_type.manufacturer.clone();
        debug!("Using manufacturer from manifest.toml: {}", manufacturer);

        let output = &context.output;

        // Ensure output directory is clean before regeneration
        if output.exists() {
            make_writable_recursive(output)?;
            std::fs::remove_dir_all(output).map_err(|e| {
                ActrCliError::command_error(format!("Failed to clean output directory: {e}"))
            })?;
        }
        std::fs::create_dir_all(output).map_err(|e| {
            ActrCliError::command_error(format!("Failed to create output directory: {e}"))
        })?;

        self.run_protoc_passes(
            context,
            output,
            &prost_plugin_path,
            &plugin_path,
            &manufacturer,
        )?;

        self.generate_mod_rs(output).await?;

        info!("✅ Infrastructure code generation completed");
        // Return empty Vec — Rust's format/validate scan the directory themselves
        Ok(vec![])
    }

    async fn generate_scaffold(&self, context: &GenContext) -> Result<Vec<PathBuf>> {
        info!("📝 Generating user code scaffold...");

        let catalog = ScaffoldCatalog::load(context, SupportedLanguage::Rust)?;

        for service in &catalog.local_services {
            self.generate_service_scaffold(service, &context.output, context.overwrite_user_code)
                .await?;
        }

        if let Some(service) = catalog.local_services.first() {
            self.generate_entry_scaffold(service, &context.output, context.overwrite_user_code)
                .await?;
        }

        info!("✅ User code scaffold generation completed");
        Ok(vec![])
    }

    async fn format_code(&self, context: &GenContext, _files: &[PathBuf]) -> Result<()> {
        info!("🎨 Formatting generated code...");

        let mut cmd = StdCommand::new("rustfmt");
        cmd.arg("--edition")
            .arg("2024")
            .arg("--config")
            .arg("max_width=100");

        for entry in std::fs::read_dir(&context.output).map_err(|e| {
            ActrCliError::config_error(format!("Failed to read output directory: {e}"))
        })? {
            let entry = entry.map_err(|e| ActrCliError::config_error(e.to_string()))?;
            let path = entry.path();
            if path.extension().unwrap_or_default() == "rs" {
                cmd.arg(&path);
            }
        }

        let output = cmd
            .output()
            .map_err(|e| ActrCliError::command_error(format!("Failed to execute rustfmt: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!("rustfmt execution warning: {}", stderr);
        } else {
            info!("✅ Code formatting completed");
        }

        Ok(())
    }

    async fn validate_code(&self, context: &GenContext) -> Result<()> {
        if context.skip_validation {
            info!("⏭️  Skipped code validation (--skip-validation)");
            return Ok(());
        }

        info!("🔍 Validating generated code...");

        let project_root = self.find_project_root()?;

        let mut cmd = StdCommand::new("cargo");
        cmd.arg("check").arg("--quiet").current_dir(&project_root);

        let output = cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to execute cargo check: {e}"))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            warn!(
                "Generated code has compilation warnings or errors:\n{}",
                stderr
            );
            info!("💡 This is usually normal because the user code scaffold contains TODO markers");
        } else {
            info!("✅ Code validation passed");
        }

        Ok(())
    }

    async fn finalize_generation(&self, context: &GenContext) -> Result<()> {
        self.set_generated_files_readonly(&context.output)
    }

    fn print_next_steps(&self, context: &GenContext) {
        let has_local_services = context
            .proto_model
            .local_services
            .iter()
            .any(|service| !service.methods.is_empty());

        println!("\n🎉 Code generation completed!");
        println!("\n📋 Next steps:");
        println!("1. 📖 View generated code: {:?}", context.output);
        if has_local_services {
            if !context.no_scaffold {
                println!(
                    "2. ✏️  Implement business logic: in the *_service.rs files in the src/ directory"
                );
                println!("3. 📦 Build package: actr build");
                println!("4. 🚀 Host the resulting .actr package from your runtime config");
            } else {
                println!("2. 📦 Build package: actr build");
                println!("3. 🚀 Host the resulting .actr package from your runtime config");
            }
        } else if !context.no_scaffold {
            println!(
                "2. ✏️  Implement business logic: in the *_service.rs files in the src/ directory"
            );
            println!("3. 🔧 Add dependencies: add required packages in Cargo.toml");
            println!("4. 🏗️  Build project: cargo build");
            println!("5. 🧪 Run tests: cargo test");
            println!("6. 🚀 Start service: cargo run");
        } else {
            println!("2. 🏗️  Build project: cargo build");
            println!("3. 🧪 Run tests: cargo test");
            println!("4. 🚀 Start service: cargo run");
        }
        println!("\n💡 Tip: Check the detailed user guide in the generated user code files");
    }
}

impl RustGenerator {
    fn run_protoc_passes(
        &self,
        context: &GenContext,
        output: &Path,
        prost_plugin_path: &Path,
        plugin_path: &Path,
        manufacturer: &str,
    ) -> Result<()> {
        let mut local_paths = Vec::new();
        let mut remote_paths = Vec::new();
        for proto_file in &context.proto_files {
            let path_str = proto_file.to_string_lossy().to_string();
            if path_str.contains("/remote/") {
                remote_paths.push(path_str);
            } else {
                local_paths.push(path_str);
            }
        }

        let mut opt_str = format!("manufacturer={}", manufacturer);
        if !local_paths.is_empty() {
            opt_str.push_str(&format!(",LocalFiles={}", local_paths.join(":")));
        }
        if !remote_paths.is_empty() {
            opt_str.push_str(&format!(",RemoteFiles={}", remote_paths.join(":")));
        }

        // Build RemoteFileActrTypes mapping: file1=actr_type1;file2=actr_type2
        if !remote_paths.is_empty() {
            let remote_file_actr_types = self.build_remote_file_actr_types(context)?;
            if !remote_file_actr_types.is_empty() {
                opt_str.push_str(&format!(",RemoteFileActrTypes={}", remote_file_actr_types));
            }
        }

        for proto_file in &context.proto_files {
            debug!("Processing proto file: {:?}", proto_file);

            // Step 1: prost for protobuf message types
            let mut cmd = StdCommand::new("protoc");
            cmd.arg(format!("--proto_path={}", context.input_path.display()))
                .arg(format!(
                    "--plugin=protoc-gen-prost={}",
                    prost_plugin_path.display()
                ))
                .arg("--prost_opt=flat_output_dir")
                .arg(format!("--prost_out={}", output.display()))
                .arg(proto_file);

            debug!("Executing protoc (prost): {:?}", cmd);
            let output_cmd = cmd.output().map_err(|e| {
                ActrCliError::command_error(format!("Failed to execute protoc (prost): {e}"))
            })?;

            if !output_cmd.status.success() {
                let stderr = String::from_utf8_lossy(&output_cmd.stderr);
                return Err(ActrCliError::command_error(format!(
                    "protoc (prost) execution failed: {stderr}"
                )));
            }
        }

        // Step 2: actrframework plugin for Actor framework code.
        // Run once across the full proto set so local generators can see remote services.
        let mut cmd = StdCommand::new("protoc");
        cmd.arg(format!("--proto_path={}", context.input_path.display()))
            .arg(format!(
                "--plugin=protoc-gen-actrframework={}",
                plugin_path.display()
            ))
            .arg(format!("--actrframework_opt={}", opt_str))
            .arg(format!("--actrframework_out={}", output.display()));

        for proto_file in &context.proto_files {
            cmd.arg(proto_file);
        }

        debug!("Executing protoc (actrframework): {:?}", cmd);
        let output_cmd = cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to execute protoc (actrframework): {e}"))
        })?;

        if !output_cmd.status.success() {
            let stderr = String::from_utf8_lossy(&output_cmd.stderr);
            return Err(ActrCliError::command_error(format!(
                "protoc (actrframework) execution failed: {stderr}"
            )));
        }

        let stdout = String::from_utf8_lossy(&output_cmd.stdout);
        if !stdout.is_empty() {
            debug!("protoc output: {}", stdout);
        }

        Ok(())
    }

    fn set_generated_files_readonly(&self, output: &Path) -> Result<()> {
        use std::fs;

        for entry in fs::read_dir(output).map_err(|e| {
            ActrCliError::config_error(format!("Failed to read output directory: {e}"))
        })? {
            let entry = entry.map_err(|e| ActrCliError::config_error(e.to_string()))?;
            let path = entry.path();

            if path.is_file() && path.extension().unwrap_or_default() == "rs" {
                let metadata = fs::metadata(&path).map_err(|e| {
                    ActrCliError::config_error(format!("Failed to get file metadata: {e}"))
                })?;
                let mut permissions = metadata.permissions();

                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let mode = permissions.mode();
                    permissions.set_mode(mode & !0o222);
                }

                #[cfg(not(unix))]
                {
                    permissions.set_readonly(true);
                }

                fs::set_permissions(&path, permissions).map_err(|e| {
                    ActrCliError::config_error(format!("Failed to set file permissions: {e}"))
                })?;

                debug!("Set read-only attribute: {:?}", path);
            }
        }

        Ok(())
    }

    async fn generate_mod_rs(&self, output: &Path) -> Result<()> {
        let mod_path = output.join("mod.rs");

        let mut proto_modules = Vec::new();
        let mut service_modules = Vec::new();

        use std::fs;
        for entry in fs::read_dir(output).map_err(|e| {
            ActrCliError::config_error(format!("Failed to read output directory: {e}"))
        })? {
            let entry = entry.map_err(|e| ActrCliError::config_error(e.to_string()))?;
            let path = entry.path();

            if path.is_file()
                && path.extension().unwrap_or_default() == "rs"
                && let Some(file_name) = path.file_stem().and_then(|s| s.to_str())
            {
                if file_name == "mod" {
                    continue;
                }

                if file_name.ends_with("_actor") || file_name.ends_with("_client") {
                    service_modules.push(format!("pub mod {file_name};"));
                } else {
                    proto_modules.push(format!("pub mod {file_name};"));
                }
            }
        }

        proto_modules.sort();
        service_modules.sort();

        let mod_content = format!(
            "//! Automatically generated code module\n\
             //!\n\
             //! This module is automatically generated by the `actr gen` command, including:\n\
             //! - protobuf message type definitions\n\
             //! - Actor framework code (router, traits)\n\
             //!\n\
             //! ⚠️ Do not manually modify files in this directory\n\
             \n\
             // Protobuf message types (generated by prost)\n\
             {}\n\
             \n\
             // Actor framework code (generated by protoc-gen-actrframework)\n\
             {}\n\
             \n\
             // Common types are defined in their respective modules, please import as needed\n",
            proto_modules.join("\n"),
            service_modules.join("\n"),
        );

        std::fs::write(&mod_path, mod_content)
            .map_err(|e| ActrCliError::config_error(format!("Failed to write mod.rs: {e}")))?;

        debug!("Generated mod.rs: {:?}", mod_path);
        Ok(())
    }

    /// Build RemoteFileActrTypes parameter for protoc plugin
    /// Format: file1=actr_type1;file2=actr_type2
    fn build_remote_file_actr_types(&self, context: &GenContext) -> Result<String> {
        let mut mappings = Vec::new();

        for file in &context.proto_model.files {
            if let Some(service) = file.services.first()
                && let Some(actr_type) = &service.actr_type
            {
                mappings.push(format!(
                    "{}={}",
                    file.proto_file.to_string_lossy(),
                    actr_type
                ));
            }
        }

        mappings.sort();
        Ok(mappings.join(";"))
    }

    async fn generate_service_scaffold(
        &self,
        service: &ScaffoldService,
        output: &Path,
        overwrite_user_code: bool,
    ) -> Result<()> {
        let src_dir = src_dir_from_output(output);
        std::fs::create_dir_all(&src_dir).map_err(|e| {
            ActrCliError::config_error(format!("Failed to create src directory: {e}"))
        })?;
        let user_file_path = src_dir.join(format!("{}.rs", handler_module_name(service)));
        let scaffold_content = self.generate_scaffold_content(service);

        if user_file_path.exists() && !overwrite_user_code {
            if self.should_overwrite_handler_scaffold(&user_file_path, &scaffold_content)? {
                info!(
                    "🔄 Overwriting generated Rust handler scaffold: {:?}",
                    user_file_path
                );
            } else {
                info!("⏭️  Skipping existing user code file: {:?}", user_file_path);
                return Ok(());
            }
        }

        if user_file_path.exists() {
            make_writable_recursive(&user_file_path)?;
        }

        std::fs::write(&user_file_path, scaffold_content).map_err(|e| {
            ActrCliError::config_error(format!("Failed to write user code scaffold: {e}"))
        })?;

        info!("📄 Generated user code scaffold: {:?}", user_file_path);
        Ok(())
    }

    async fn generate_entry_scaffold(
        &self,
        service: &ScaffoldService,
        output: &Path,
        overwrite_user_code: bool,
    ) -> Result<()> {
        let src_dir = src_dir_from_output(output);
        std::fs::create_dir_all(&src_dir).map_err(|e| {
            ActrCliError::config_error(format!("Failed to create src directory: {e}"))
        })?;
        let lib_path = src_dir.join("lib.rs");
        let scaffold_content = self.generate_entry_scaffold_content(service);

        if lib_path.exists() && !overwrite_user_code {
            if self.should_overwrite_entry_scaffold(&lib_path, &scaffold_content)? {
                info!(
                    "🔄 Overwriting generated Rust entry scaffold: {:?}",
                    lib_path
                );
            } else {
                info!("⏭️  Skipping existing Rust entry file: {:?}", lib_path);
                return Ok(());
            }
        }

        if lib_path.exists() {
            make_writable_recursive(&lib_path)?;
        }

        std::fs::write(&lib_path, scaffold_content).map_err(|e| {
            ActrCliError::config_error(format!("Failed to write Rust entry scaffold: {e}"))
        })?;

        info!("📄 Generated Rust entry scaffold: {:?}", lib_path);
        Ok(())
    }

    fn should_overwrite_handler_scaffold(
        &self,
        path: &Path,
        expected_scaffold: &str,
    ) -> Result<bool> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            ActrCliError::config_error(format!("Failed to read {}: {e}", path.display()))
        })?;

        Ok(content == expected_scaffold)
    }

    fn should_overwrite_entry_scaffold(
        &self,
        path: &Path,
        expected_scaffold: &str,
    ) -> Result<bool> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            ActrCliError::config_error(format!("Failed to read {}: {e}", path.display()))
        })?;

        if content == expected_scaffold {
            return Ok(true);
        }

        Ok(is_default_cargo_lib_rs(&content))
    }

    fn generate_entry_scaffold_content(&self, service: &ScaffoldService) -> String {
        let actor_module = actor_module_name(service);
        let handler_module = handler_module_name(service);
        let handler_impl = handler_impl_type(service);
        let workload_type =
            service_type_or_default(service, service.workload_type.as_deref(), "Workload");

        format!(
            r#"{ENTRY_SCAFFOLD_MARKER}
//! Entry point for the generated {service_name} workload.

pub mod generated;
pub mod {handler_module};

use actr_framework::entry;
use generated::{actor_module}::{workload_type};

pub use crate::{handler_module}::{handler_impl};

entry!(
    {workload_type}<{handler_impl}>,
    {workload_type}::new({handler_impl}::new())
);
"#,
            service_name = service.name,
        )
    }

    fn generate_scaffold_content(&self, service: &ScaffoldService) -> String {
        let actor_module = actor_module_name(service);
        let proto_module = proto_module_name(service);
        let handler_interface =
            service_type_or_default(service, service.handler_interface.as_deref(), "Handler");
        let handler_impl = handler_impl_type(service);
        let message_imports = message_imports(service);
        let method_impls = handler_method_impls(service);
        let method_imports = if message_imports.is_empty() {
            String::new()
        } else {
            format!("use crate::generated::{proto_module}::{{{message_imports}}};\n")
        };
        let framework_imports = if service.methods.is_empty() {
            String::new()
        } else {
            "use actr_framework::Context;\nuse actr_protocol::ActorResult;\n".to_string()
        };

        format!(
            r#"{HANDLER_SCAFFOLD_MARKER}
//! # {service_name} user business logic implementation
//!
//! This file is a user code scaffold automatically generated by the `actr gen` command.
//! Please implement your specific business logic here.

use crate::generated::{actor_module}::{handler_interface};
{method_imports}{framework_imports}

/// Specific implementation of the {service_name} service.
pub struct {handler_impl};

impl {handler_impl} {{
    /// Create a new service instance
    pub fn new(/* TODO: Add necessary dependencies */) -> Self {{
        Self
    }}
}}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl {handler_interface} for {handler_impl} {{
{method_impls}
}}
"#,
            service_name = service.name,
        )
    }

    fn find_project_root(&self) -> Result<PathBuf> {
        let mut current = std::env::current_dir().map_err(ActrCliError::Io)?;

        loop {
            if current.join("Cargo.toml").exists() {
                return Ok(current);
            }

            match current.parent() {
                Some(parent) => current = parent.to_path_buf(),
                None => break,
            }
        }

        std::env::current_dir().map_err(ActrCliError::Io)
    }

    fn ensure_protoc_plugin(&self, config_path: &Path) -> Result<PathBuf> {
        const EXPECTED_VERSION: &str = "0.2.0";
        const PLUGIN_NAME: &str = "protoc-gen-actrframework";

        if let Some(plugin_path) = self.try_use_local_workspace_plugin()? {
            return Ok(plugin_path);
        }

        let min_version = self.resolve_plugin_min_version(PLUGIN_NAME, config_path)?;
        let require_exact = min_version.is_none();
        let required_version = min_version.unwrap_or_else(|| EXPECTED_VERSION.to_string());

        let installed_version = self.check_installed_plugin_version()?;

        match installed_version {
            Some(version) if self.version_satisfies(&version, &required_version, require_exact) => {
                info!("✅ Using installed protoc-gen-actrframework v{}", version);
                self.locate_installed_plugin(PLUGIN_NAME)
            }
            Some(version) => {
                if require_exact {
                    info!(
                        "🔄 Version mismatch: installed v{}, need v{}",
                        version, required_version
                    );
                } else {
                    info!(
                        "🔄 Version below minimum: installed v{}, need >= v{}",
                        version, required_version
                    );
                }
                info!("🔨 Upgrading plugin...");
                let path = self.install_or_upgrade_plugin(&required_version)?;
                self.ensure_required_plugin_version(&required_version, require_exact)?;
                Ok(path)
            }
            None => {
                info!("📦 protoc-gen-actrframework not found, installing...");
                let path = self.install_or_upgrade_plugin(&required_version)?;
                self.ensure_required_plugin_version(&required_version, require_exact)?;
                Ok(path)
            }
        }
    }

    fn try_use_local_workspace_plugin(&self) -> Result<Option<PathBuf>> {
        if !cfg!(debug_assertions) {
            return Ok(None);
        }

        let Some(workspace_root) = self.find_development_actr_workspace_root()? else {
            return Ok(None);
        };

        let plugin_path = self.local_workspace_plugin_path(&workspace_root);

        info!(
            "🧪 Building local workspace plugin in debug build: {}",
            workspace_root.display()
        );
        let mut build_cmd = StdCommand::new("cargo");
        build_cmd
            .arg("build")
            .arg("--quiet")
            .arg("-p")
            .arg("actr-framework-protoc-codegen")
            .arg("--bin")
            .arg("protoc-gen-actrframework")
            .current_dir(&workspace_root);

        let output = build_cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to build local protoc plugin: {e}"))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to build local protoc plugin:\n{stderr}"
            )));
        }

        if plugin_path.exists() {
            info!("✅ Built local workspace plugin: {}", plugin_path.display());
            Ok(Some(plugin_path))
        } else {
            Err(ActrCliError::command_error(format!(
                "Local plugin build succeeded but binary was not found at {}",
                plugin_path.display()
            )))
        }
    }

    fn check_installed_plugin_version(&self) -> Result<Option<String>> {
        let output = StdCommand::new("protoc-gen-actrframework")
            .arg("--version")
            .output();

        match output {
            Ok(output) if output.status.success() => {
                let version_info = String::from_utf8_lossy(&output.stdout);
                let version = version_info
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .map(|v| v.to_string());

                debug!("Detected installed version: {:?}", version);
                Ok(version)
            }
            _ => {
                debug!("Plugin not found in PATH");
                Ok(None)
            }
        }
    }

    fn ensure_prost_plugin(&self, _config_path: &Path) -> Result<PathBuf> {
        const PLUGIN_NAME: &str = "protoc-gen-prost";

        if self.find_plugin_in_path(PLUGIN_NAME)?.is_some() {
            return self.locate_installed_plugin(PLUGIN_NAME);
        }

        info!("📦 protoc-gen-prost not found, installing from crates.io...");
        self.install_prost_plugin_from_registry()
    }

    fn install_or_upgrade_plugin(&self, required_version: &str) -> Result<PathBuf> {
        if self.is_ci_environment() {
            info!("🔧 CI detected, installing protoc-gen-actrframework from GitHub source...");
            return self.install_plugin_from_github_source();
        }

        if let Some(workspace_root) = self.find_actr_workspace_root()? {
            info!("🔍 Found actr workspace at: {}", workspace_root.display());

            match self.install_plugin_from_local_path(&workspace_root) {
                Ok(path) => return Ok(path),
                Err(error) => {
                    warn!(
                        "Local plugin installation failed, falling back to crates.io: {}",
                        error
                    );
                }
            }
        } else {
            info!("🔍 No local actr workspace found, falling back to crates.io install...");
        }

        self.install_plugin_from_registry(required_version)
    }

    fn is_ci_environment(&self) -> bool {
        std::env::var_os("CI").is_some()
    }

    fn find_actr_workspace_root(&self) -> Result<Option<PathBuf>> {
        let current_dir = std::env::current_dir()?;
        let workspace_root = current_dir.ancestors().find(|p| {
            let is_workspace =
                p.join("Cargo.toml").exists() && p.join("tools/protoc-gen/rust").exists();
            if is_workspace {
                debug!("Found workspace root: {:?}", p);
            }
            is_workspace
        });

        Ok(workspace_root.map(Path::to_path_buf))
    }

    fn find_development_actr_workspace_root(&self) -> Result<Option<PathBuf>> {
        if let Some(workspace_root) = self.find_actr_workspace_root()? {
            return Ok(Some(workspace_root));
        }

        let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
        if let Some(parent_workspace) = manifest_dir.parent()
            && parent_workspace.join("Cargo.toml").exists()
            && parent_workspace.join("tools/protoc-gen/rust").exists()
        {
            return Ok(Some(parent_workspace.to_path_buf()));
        }

        let sibling_workspace = manifest_dir.join("../actr");
        if sibling_workspace.join("Cargo.toml").exists()
            && sibling_workspace.join("tools/protoc-gen/rust").exists()
        {
            return Ok(Some(sibling_workspace));
        }

        Ok(None)
    }

    fn local_workspace_plugin_path(&self, workspace_root: &Path) -> PathBuf {
        let target_dir = std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .map(|path| {
                if path.is_absolute() {
                    path
                } else {
                    workspace_root.join(path)
                }
            })
            .unwrap_or_else(|| workspace_root.join("target"));

        target_dir.join("debug").join(format!(
            "protoc-gen-actrframework{}",
            std::env::consts::EXE_SUFFIX
        ))
    }

    fn install_plugin_from_local_path(&self, workspace_root: &Path) -> Result<PathBuf> {
        info!("Installing protoc-gen-actrframework from local path...");
        let mut install_cmd = StdCommand::new("cargo");
        install_cmd
            .arg("install")
            .arg("--path")
            .arg(workspace_root.join("tools/protoc-gen/rust"))
            .arg("--bin")
            .arg("protoc-gen-actrframework")
            .arg("--force");

        debug!("Running: {:?}", install_cmd);
        let output = install_cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to run local plugin installation: {e}"))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to install plugin from local path:\n{stderr}"
            )));
        }

        info!("✅ Plugin installed successfully from local path");
        self.locate_installed_plugin("protoc-gen-actrframework")
    }

    fn install_plugin_from_registry(&self, required_version: &str) -> Result<PathBuf> {
        const PACKAGE_NAME: &str = "actr-framework-protoc-codegen";
        const PLUGIN_NAME: &str = "protoc-gen-actrframework";

        info!(
            "Installing {} v{} from crates.io...",
            PLUGIN_NAME, required_version
        );

        let mut install_cmd = StdCommand::new("cargo");
        install_cmd
            .arg("install")
            .arg(PACKAGE_NAME)
            .arg("--version")
            .arg(required_version)
            .arg("--bin")
            .arg(PLUGIN_NAME)
            .arg("--force");

        debug!("Running: {:?}", install_cmd);
        let output = install_cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to run crates.io plugin installation: {e}"))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to install plugin from crates.io:\n{stderr}"
            )));
        }

        info!("✅ Plugin installed successfully from crates.io");
        self.locate_installed_plugin(PLUGIN_NAME)
    }

    fn install_plugin_from_github_source(&self) -> Result<PathBuf> {
        const PACKAGE_NAME: &str = "actr-framework-protoc-codegen";
        const PLUGIN_NAME: &str = "protoc-gen-actrframework";
        const REPOSITORY_URL: &str = "https://github.com/Actrium/actr.git";

        let mut install_cmd = StdCommand::new("cargo");
        install_cmd
            .arg("install")
            .arg("--git")
            .arg(REPOSITORY_URL)
            .arg("--branch")
            .arg("main")
            .arg(PACKAGE_NAME)
            .arg("--bin")
            .arg(PLUGIN_NAME)
            .arg("--force");

        debug!("Running: {:?}", install_cmd);
        let output = install_cmd.output().map_err(|e| {
            ActrCliError::command_error(format!(
                "Failed to run GitHub source plugin installation: {e}"
            ))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to install plugin from GitHub source:\n{stderr}"
            )));
        }

        info!("✅ Plugin installed successfully from GitHub source");
        self.locate_installed_plugin(PLUGIN_NAME)
    }

    fn install_prost_plugin_from_registry(&self) -> Result<PathBuf> {
        const PACKAGE_NAME: &str = "protoc-gen-prost";
        const PLUGIN_NAME: &str = "protoc-gen-prost";

        let mut install_cmd = StdCommand::new("cargo");
        install_cmd.arg("install").arg(PACKAGE_NAME).arg("--locked");

        debug!("Running: {:?}", install_cmd);
        let output = install_cmd.output().map_err(|e| {
            ActrCliError::command_error(format!("Failed to run protoc-gen-prost install: {e}"))
        })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(ActrCliError::command_error(format!(
                "Failed to install protoc-gen-prost from crates.io:\n{stderr}"
            )));
        }

        info!("✅ protoc-gen-prost installed successfully from crates.io");
        self.locate_installed_plugin(PLUGIN_NAME)
    }

    fn find_plugin_in_path(&self, plugin_name: &str) -> Result<Option<PathBuf>> {
        let which_output = StdCommand::new("which")
            .arg(plugin_name)
            .output()
            .map_err(|e| {
                ActrCliError::command_error(format!("Failed to locate plugin in PATH: {e}"))
            })?;

        if !which_output.status.success() {
            return Ok(None);
        }

        let path = String::from_utf8_lossy(&which_output.stdout)
            .trim()
            .to_string();
        if path.is_empty() {
            return Ok(None);
        }

        Ok(Some(PathBuf::from(path)))
    }

    fn locate_installed_plugin(&self, plugin_name: &str) -> Result<PathBuf> {
        self.find_plugin_in_path(plugin_name)?.ok_or_else(|| {
            ActrCliError::command_error(format!(
                "Failed to locate installed plugin: {} is not in PATH",
                plugin_name
            ))
        })
    }

    fn resolve_plugin_min_version(
        &self,
        plugin_name: &str,
        config_path: &Path,
    ) -> Result<Option<String>> {
        let config = load_protoc_plugin_config(config_path)?;
        if let Some(config) = config
            && let Some(min_version) = config.min_version(plugin_name)
        {
            info!(
                "🔧 Using minimum version for {} from {}",
                plugin_name,
                config.path().display()
            );
            return Ok(Some(min_version.to_string()));
        }
        Ok(None)
    }

    fn version_satisfies(&self, installed: &str, required: &str, strict_equal: bool) -> bool {
        if strict_equal {
            installed == required
        } else {
            version_is_at_least(installed, required)
        }
    }

    fn ensure_required_plugin_version(
        &self,
        required_version: &str,
        strict_equal: bool,
    ) -> Result<()> {
        let installed_version = self.check_installed_plugin_version()?;
        let Some(installed_version) = installed_version else {
            return Err(ActrCliError::command_error(
                "Failed to determine installed protoc-gen-actrframework version after install"
                    .to_string(),
            ));
        };

        if self.version_satisfies(&installed_version, required_version, strict_equal) {
            return Ok(());
        }

        if strict_equal {
            Err(ActrCliError::command_error(format!(
                "protoc-gen-actrframework version {} does not match required version {}",
                installed_version, required_version
            )))
        } else {
            Err(ActrCliError::command_error(format!(
                "protoc-gen-actrframework version {} is lower than minimum version {}",
                installed_version, required_version
            )))
        }
    }
}

fn src_dir_from_output(output: &Path) -> PathBuf {
    output
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("src"))
}

fn handler_module_name(service: &ScaffoldService) -> String {
    to_snake_case(&service.name)
}

fn handler_impl_type(service: &ScaffoldService) -> String {
    format!("{}Impl", service.name)
}

fn service_type_or_default(
    service: &ScaffoldService,
    metadata_type: Option<&str>,
    suffix: &str,
) -> String {
    metadata_type
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("{}{}", service.name, suffix))
}

fn actor_module_name(service: &ScaffoldService) -> String {
    service
        .proto_file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(|stem| format!("{}_actor", to_snake_case(stem)))
        .unwrap_or_else(|| format!("{}_actor", to_snake_case(&service.name)))
}

fn proto_module_name(service: &ScaffoldService) -> String {
    if !service.package.is_empty() {
        return service.package.replace('.', "_");
    }

    service
        .proto_file
        .file_stem()
        .and_then(|stem| stem.to_str())
        .map(to_snake_case)
        .unwrap_or_else(|| to_snake_case(&service.name))
}

fn message_imports(service: &ScaffoldService) -> String {
    let mut imports = BTreeSet::new();
    for method in &service.methods {
        imports.insert(method.input_type.clone());
        imports.insert(method.output_type.clone());
    }
    imports.into_iter().collect::<Vec<_>>().join(", ")
}

fn handler_method_impls(service: &ScaffoldService) -> String {
    if service.methods.is_empty() {
        return "    // This service does not declare RPC methods yet.\n".to_string();
    }

    service
        .methods
        .iter()
        .map(|method| {
            format!(
                r#"    async fn {method_name}<C: Context>(
        &self,
        _req: {input_type},
        _ctx: &C,
    ) -> ActorResult<{output_type}> {{
        todo!("Implement {service_name}.{rpc_name}")
    }}
"#,
                method_name = method.snake_name,
                input_type = method.input_type,
                output_type = method.output_type,
                service_name = service.name,
                rpc_name = method.name,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_default_cargo_lib_rs(content: &str) -> bool {
    content.contains("pub fn add(left: u64, right: u64) -> u64")
        && content.contains("fn it_works()")
}

/// Recursively make all files in a directory (or a file) writable.
fn make_writable_recursive(path: &Path) -> Result<()> {
    use std::fs;

    if path.is_file() {
        let metadata = fs::metadata(path).map_err(|e| {
            ActrCliError::config_error(format!("Failed to read file metadata: {e}"))
        })?;
        let mut permissions = metadata.permissions();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = permissions.mode();
            permissions.set_mode(mode | 0o222);
        }

        #[cfg(not(unix))]
        {
            permissions.set_readonly(false);
        }

        fs::set_permissions(path, permissions).map_err(|e| {
            ActrCliError::config_error(format!("Failed to reset file permissions: {e}"))
        })?;
    } else if path.is_dir() {
        for entry in fs::read_dir(path)
            .map_err(|e| ActrCliError::config_error(format!("Failed to read directory: {e}")))?
        {
            let entry = entry.map_err(|e| ActrCliError::config_error(e.to_string()))?;
            make_writable_recursive(&entry.path())?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::RustGenerator;
    use crate::commands::codegen::scaffold::{ScaffoldMethod, ScaffoldService};
    use crate::commands::codegen::{GenContext, LanguageGenerator, ProtoModel};
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

        tokio_test::block_on(RustGenerator.generate_scaffold(&context)).unwrap();

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
        tokio_test::block_on(RustGenerator.generate_scaffold(&context)).unwrap();
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
}
