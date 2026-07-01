//! Operation pipeline definitions
//!
//! Defines three core operation pipelines for cross-command logic reuse

use actr_config::{LockFile, LockedDependency, ProtoFileMeta, ServiceSpecMeta};
use anyhow::Result;
use std::sync::Arc;

use super::components::*;

// ============================================================================
// Pipeline result types
// ============================================================================

/// Install result
#[derive(Debug, Clone)]
pub struct InstallResult {
    pub installed_dependencies: Vec<ResolvedDependency>,
    pub updated_config: bool,
    pub updated_lock_file: bool,
    pub cache_updates: usize,
    pub warnings: Vec<String>,
}

impl InstallResult {
    pub fn success() -> Self {
        Self {
            installed_dependencies: Vec::new(),
            updated_config: false,
            updated_lock_file: false,
            cache_updates: 0,
            warnings: Vec::new(),
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "Installed {} dependencies, updated {} cache entries",
            self.installed_dependencies.len(),
            self.cache_updates
        )
    }
}

/// Install plan
#[derive(Debug, Clone)]
pub struct InstallPlan {
    pub dependencies_to_install: Vec<DependencySpec>,
    pub resolved_dependencies: Vec<ResolvedDependency>,
    pub estimated_cache_size: u64,
    pub required_permissions: Vec<String>,
}

/// Generation options
#[derive(Debug, Clone)]
pub struct GenerationOptions {
    pub input_path: std::path::PathBuf,
    pub output_path: std::path::PathBuf,
    pub clean_before_generate: bool,
    pub generate_scaffold: bool,
    pub format_code: bool,
    pub run_checks: bool,
}

// ============================================================================
// 1. Validation Pipeline
// ============================================================================

/// Core validation pipeline - reused by multiple commands
#[derive(Clone)]
pub struct ValidationPipeline {
    config_manager: Arc<dyn ConfigManager>,
    dependency_resolver: Arc<dyn DependencyResolver>,
    service_discovery: Arc<dyn ServiceDiscovery>,
    network_validator: Arc<dyn NetworkValidator>,
    fingerprint_validator: Arc<dyn FingerprintValidator>,
}

impl ValidationPipeline {
    pub fn new(
        config_manager: Arc<dyn ConfigManager>,
        dependency_resolver: Arc<dyn DependencyResolver>,
        service_discovery: Arc<dyn ServiceDiscovery>,
        network_validator: Arc<dyn NetworkValidator>,
        fingerprint_validator: Arc<dyn FingerprintValidator>,
    ) -> Self {
        Self {
            config_manager,
            dependency_resolver,
            service_discovery,
            network_validator,
            fingerprint_validator,
        }
    }

    /// Get service discovery component
    pub fn service_discovery(&self) -> &Arc<dyn ServiceDiscovery> {
        &self.service_discovery
    }

    /// Get network validator component
    pub fn network_validator(&self) -> &Arc<dyn NetworkValidator> {
        &self.network_validator
    }

    /// Get config manager component
    pub fn config_manager(&self) -> &Arc<dyn ConfigManager> {
        &self.config_manager
    }

    /// Get dependency resolver component
    pub fn dependency_resolver(&self) -> &Arc<dyn DependencyResolver> {
        &self.dependency_resolver
    }

    fn dependency_lookup_key(spec: &DependencySpec) -> String {
        spec.actr_type
            .as_ref()
            .map(|actr_type| actr_type.to_string_repr())
            .unwrap_or_else(|| spec.name.clone())
    }

    /// Full project validation flow
    pub async fn validate_project(&self) -> Result<ValidationReport> {
        // 1. Config file validation
        let config_validation = self.config_manager.validate_config().await?;

        // If config file has issues, return immediately
        if !config_validation.is_valid {
            return Ok(ValidationReport {
                is_valid: false,
                config_validation,
                dependency_validation: vec![],
                network_validation: vec![],
                fingerprint_validation: vec![],
                conflicts: vec![],
            });
        }

        // 2. Dependency resolution and validation
        let config = self
            .config_manager
            .load_config(
                self.config_manager
                    .get_project_root()
                    .join("manifest.toml")
                    .as_path(),
            )
            .await?;
        let dependency_specs = self.dependency_resolver.resolve_spec(&config).await?;

        let mut service_details = Vec::new();
        for spec in &dependency_specs {
            let lookup_key = Self::dependency_lookup_key(spec);
            match self
                .service_discovery
                .get_service_details(&lookup_key)
                .await
            {
                Ok(details) => service_details.push(details),
                Err(_) => {
                    // Service might not be available, continue without details
                }
            }
        }

        let resolved_dependencies = self
            .dependency_resolver
            .resolve_dependencies(&dependency_specs, &service_details)
            .await?;

        // 3. Conflict check
        let conflicts = self
            .dependency_resolver
            .check_conflicts(&resolved_dependencies)
            .await?;

        let dependency_validation = self.validate_dependencies(&dependency_specs).await?;
        let network_validation = self
            .validate_network_connectivity(&resolved_dependencies, &NetworkCheckOptions::default())
            .await?;
        let fingerprint_validation = self.validate_fingerprints(&resolved_dependencies).await?;

        let is_valid = config_validation.is_valid
            && dependency_validation.iter().all(|d| d.is_available)
            && network_validation
                .iter()
                .all(|n| !n.is_applicable || n.is_reachable)
            && fingerprint_validation.iter().all(|f| f.is_valid)
            && conflicts.is_empty();

        Ok(ValidationReport {
            is_valid,
            config_validation,
            dependency_validation,
            network_validation,
            fingerprint_validation,
            conflicts,
        })
    }

    /// Validate a specific list of dependencies
    /// Note: Multiple aliases pointing to the same service name will be deduplicated
    pub async fn validate_dependencies(
        &self,
        specs: &[DependencySpec],
    ) -> Result<Vec<DependencyValidation>> {
        use std::collections::HashMap;

        let mut results = Vec::new();
        // Cache validation results by service name to avoid duplicate checks
        let mut validation_cache: HashMap<String, (bool, Option<String>)> = HashMap::new();

        for spec in specs {
            let lookup_key = Self::dependency_lookup_key(spec);
            // Check cache first - if we already validated this service name, reuse the result
            let (is_available, error) = if let Some(cached) = validation_cache.get(&lookup_key) {
                cached.clone()
            } else {
                // Perform validation
                let (available, err) = match self
                    .service_discovery
                    .check_service_availability(&lookup_key)
                    .await
                {
                    Ok(status) => {
                        if status.is_available {
                            (true, None)
                        } else {
                            // Provide meaningful error when service is not found
                            (
                                false,
                                Some(format!("Service '{}' not found in registry", lookup_key)),
                            )
                        }
                    }
                    Err(e) => (false, Some(e.to_string())),
                };

                // Cache the result for this service name
                validation_cache.insert(lookup_key, (available, err.clone()));
                (available, err)
            };

            results.push(DependencyValidation {
                dependency: spec.alias.clone(),
                is_available,
                error,
            });
        }

        Ok(results)
    }

    /// Network connectivity validation
    pub async fn validate_network_connectivity(
        &self,
        deps: &[ResolvedDependency],
        options: &NetworkCheckOptions,
    ) -> Result<Vec<NetworkValidation>> {
        let names = deps.iter().map(|d| d.spec.name.clone()).collect::<Vec<_>>();
        let network_results = self.network_validator.batch_check(&names, options).await?;

        Ok(network_results
            .into_iter()
            .map(|result| {
                let mut is_applicable = true;
                let mut error = result.connectivity.error;
                let mut health = result.health;
                let mut latency_ms = result.connectivity.response_time_ms;

                if let Some(ref message) = error
                    && message.starts_with("Address resolution failed: Invalid address format")
                {
                    is_applicable = false;
                    error =
                        Some("Network check skipped: no endpoint address available".to_string());
                    health = HealthStatus::Unknown;
                    latency_ms = None;
                }

                NetworkValidation {
                    is_reachable: result.connectivity.is_reachable,
                    health,
                    latency_ms,
                    error,
                    is_applicable,
                }
            })
            .collect())
    }

    /// Fingerprint validation
    pub async fn validate_fingerprints(
        &self,
        deps: &[ResolvedDependency],
    ) -> Result<Vec<FingerprintValidation>> {
        let mut results = Vec::new();

        for dep in deps {
            let expected_val = dep.spec.fingerprint.clone().unwrap_or_default();
            let expected = Fingerprint {
                algorithm: "sha256".to_string(),
                value: expected_val,
            };

            // Compute actual fingerprint (if resolved_dependencies has none, fetch from remote)
            let actual_fp = if dep.fingerprint.is_empty() {
                let lookup_key = Self::dependency_lookup_key(&dep.spec);
                match self
                    .service_discovery
                    .get_service_details(&lookup_key)
                    .await
                {
                    Ok(details) => {
                        let computed = self
                            .fingerprint_validator
                            .compute_service_fingerprint(&details.info)
                            .await?;
                        Some(computed)
                    }
                    Err(e) => {
                        results.push(FingerprintValidation {
                            dependency: dep.spec.alias.clone(),
                            expected,
                            actual: None,
                            is_valid: false,
                            error: Some(e.to_string()),
                        });
                        continue;
                    }
                }
            } else {
                // Already has fingerprint, no need to recompute
                None
            };

            let is_valid = if expected.value.is_empty() {
                true
            } else if let Some(ref computed) = actual_fp {
                self.fingerprint_validator
                    .verify_fingerprint(&expected, computed)
                    .await
                    .unwrap_or(false)
            } else {
                // Fingerprint already matched (from resolve_dependencies)
                true
            };

            results.push(FingerprintValidation {
                dependency: dep.spec.alias.clone(),
                expected,
                actual: actual_fp,
                is_valid,
                error: None,
            });
        }

        Ok(results)
    }
}

// ============================================================================
// 2. Install Pipeline
// ============================================================================

/// Install pipeline - built on top of ValidationPipeline
pub struct InstallPipeline {
    validation_pipeline: ValidationPipeline,
    config_manager: Arc<dyn ConfigManager>,
    cache_manager: Arc<dyn CacheManager>,
    #[allow(dead_code)]
    proto_processor: Arc<dyn ProtoProcessor>,
}

impl InstallPipeline {
    pub fn new(
        validation_pipeline: ValidationPipeline,
        config_manager: Arc<dyn ConfigManager>,
        cache_manager: Arc<dyn CacheManager>,
        proto_processor: Arc<dyn ProtoProcessor>,
    ) -> Self {
        Self {
            validation_pipeline,
            config_manager,
            cache_manager,
            proto_processor,
        }
    }

    /// Get validation pipeline reference
    pub fn validation_pipeline(&self) -> &ValidationPipeline {
        &self.validation_pipeline
    }

    /// Get config manager reference
    pub fn config_manager(&self) -> &Arc<dyn ConfigManager> {
        &self.config_manager
    }

    /// Check-first install flow
    pub async fn install_dependencies(&self, specs: &[DependencySpec]) -> Result<InstallResult> {
        // Phase 1: Full validation (reuse ValidationPipeline)
        let validation_report = self
            .validation_pipeline
            .validate_dependencies(specs)
            .await?;

        // Check validation results
        let failed_validations: Vec<_> = validation_report
            .iter()
            .filter(|v| !v.is_available)
            .collect();

        if !failed_validations.is_empty() {
            return Err(anyhow::anyhow!(
                "Dependency validation failed: {}",
                failed_validations
                    .iter()
                    .map(|v| format!(
                        "{}: {}",
                        v.dependency,
                        v.error.as_deref().unwrap_or("unknown error")
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        // Phase 2: Atomic install
        let backup = self.config_manager.backup_config().await?;

        match self.execute_atomic_install(specs).await {
            Ok(result) => {
                // Install succeeded, clean up backup
                self.config_manager.remove_backup(backup).await?;
                Ok(result)
            }
            Err(e) => {
                // Install failed, restore backup
                self.config_manager.restore_backup(backup).await?;
                Err(e)
            }
        }
    }

    /// Atomic install execution
    /// Note: Multiple aliases pointing to the same service will be deduplicated -
    /// only one entry per unique service name will be installed and recorded in lock file
    async fn execute_atomic_install(&self, specs: &[DependencySpec]) -> Result<InstallResult> {
        use std::collections::HashSet;

        let mut result = InstallResult::success();
        let mut installed_services: HashSet<String> = HashSet::new();

        for spec in specs {
            let lookup_key = ValidationPipeline::dependency_lookup_key(spec);
            // Skip if we already installed this service (by name)
            if installed_services.contains(&lookup_key) {
                tracing::debug!(
                    "Skipping duplicate service '{}' (alias: '{}')",
                    lookup_key,
                    spec.alias
                );
                continue;
            }

            // 1. Get service details (before updating config, ensure we have the full actr_type)
            let service_details = self
                .validation_pipeline
                .service_discovery
                .get_service_details(&lookup_key)
                .await?;

            // 2. Build resolved_spec using the canonical actr_type from discovery
            let mut resolved_spec = spec.clone();
            resolved_spec.actr_type = Some(service_details.info.actr_type.clone());

            // 3. Update config file (using resolved_spec with actr_type)
            self.config_manager
                .update_dependency(&resolved_spec)
                .await?;
            result.updated_config = true;

            // 4. Cache proto files
            self.cache_manager
                .cache_proto(&spec.name, &service_details.proto_files)
                .await?;

            result.cache_updates += 1;

            // 5. Record installed dependency

            let resolved_dep = ResolvedDependency {
                spec: resolved_spec,
                fingerprint: service_details.info.fingerprint,
                proto_files: service_details.proto_files,
            };
            result.installed_dependencies.push(resolved_dep);

            // Mark this service as installed
            installed_services.insert(lookup_key);
        }

        // 4. Update lock file (lock file also deduplicates by name)
        self.update_lock_file(&result.installed_dependencies)
            .await?;
        result.updated_lock_file = true;

        Ok(result)
    }

    /// Update lock file with new format (no embedded proto content)
    async fn update_lock_file(&self, dependencies: &[ResolvedDependency]) -> Result<()> {
        let project_root = self.config_manager.get_project_root();
        let lock_file_path = project_root.join("manifest.lock.toml");

        // Load existing lock file or create new one
        let mut lock_file = if lock_file_path.exists() {
            LockFile::from_file(&lock_file_path).unwrap_or_else(|_| LockFile::new())
        } else {
            LockFile::new()
        };

        // Update dependencies
        for dep in dependencies {
            let service_name = dep.spec.name.clone();

            // Create protobuf entries with relative path (no content)
            let protobufs: Vec<ProtoFileMeta> = dep
                .proto_files
                .iter()
                .map(|pf| {
                    let file_name = if pf.name.ends_with(".proto") {
                        pf.name.clone()
                    } else {
                        format!("{}.proto", pf.name)
                    };
                    // Path relative to proto/remote/ (e.g., "service_name/file.proto")
                    let path = format!("{}/{}", service_name, file_name);

                    ProtoFileMeta {
                        path,
                        fingerprint: String::new(), // TODO: compute semantic fingerprint
                    }
                })
                .collect();

            // Create service spec metadata
            let spec = ServiceSpecMeta {
                name: dep.spec.name.clone(),
                description: None,
                fingerprint: dep.fingerprint.clone(),
                protobufs,
                published_at: None,
                tags: Vec::new(),
            };

            // Create locked dependency
            let actr_type = dep.spec.actr_type.clone().ok_or_else(|| {
                anyhow::anyhow!("Actr type is required for dependency: {}", service_name)
            })?;
            let locked_dep = LockedDependency::new(actr_type.to_string_repr(), spec);
            lock_file.add_dependency(locked_dep);
        }

        // Update timestamp and save
        lock_file.update_timestamp();
        lock_file.save_to_file(&lock_file_path)?;

        tracing::info!("Updated lock file: {} dependencies", dependencies.len());
        Ok(())
    }
}

// ============================================================================
// 3. Generation Pipeline
// ============================================================================

/// Code generation pipeline
pub struct GenerationPipeline {
    #[allow(dead_code)]
    config_manager: Arc<dyn ConfigManager>,
    proto_processor: Arc<dyn ProtoProcessor>,
    #[allow(dead_code)]
    cache_manager: Arc<dyn CacheManager>,
}

impl GenerationPipeline {
    pub fn new(
        config_manager: Arc<dyn ConfigManager>,
        proto_processor: Arc<dyn ProtoProcessor>,
        cache_manager: Arc<dyn CacheManager>,
    ) -> Self {
        Self {
            config_manager,
            proto_processor,
            cache_manager,
        }
    }

    /// Execute code generation
    pub async fn generate_code(&self, options: &GenerationOptions) -> Result<GenerationResult> {
        // 1. Clean output directory (if needed)
        if options.clean_before_generate {
            self.clean_output_directory(&options.output_path).await?;
        }

        // 2. Discover local proto files
        let local_protos = self
            .proto_processor
            .discover_proto_files(&options.input_path)
            .await?;

        // 3. Load dependency proto files
        let dependency_protos = self.load_dependency_protos().await?;

        // 4. Validate proto syntax
        let all_protos = [local_protos, dependency_protos].concat();
        let validation = self
            .proto_processor
            .validate_proto_syntax(&all_protos)
            .await?;

        if !validation.is_valid {
            return Err(anyhow::anyhow!("Proto file syntax validation failed"));
        }

        // 5. Execute code generation
        let mut generation_result = self
            .proto_processor
            .generate_code(&options.input_path, &options.output_path)
            .await?;

        // 6. Post-processing: format and check
        if options.format_code {
            self.format_generated_code(&generation_result.generated_files)
                .await?;
        }

        if options.run_checks {
            let check_result = self
                .run_code_checks(&generation_result.generated_files)
                .await?;
            generation_result.warnings.extend(check_result.warnings);
            generation_result.errors.extend(check_result.errors);
        }

        Ok(generation_result)
    }

    /// Clean the output directory
    async fn clean_output_directory(&self, output_path: &std::path::Path) -> Result<()> {
        if output_path.exists() {
            std::fs::remove_dir_all(output_path)?;
        }
        std::fs::create_dir_all(output_path)?;
        Ok(())
    }

    /// Load dependency proto files
    async fn load_dependency_protos(&self) -> Result<Vec<ProtoFile>> {
        // TODO: Load dependency proto files from cache
        Ok(Vec::new())
    }

    /// Format generated code
    async fn format_generated_code(&self, files: &[std::path::PathBuf]) -> Result<()> {
        for file in files {
            if file.extension().and_then(|s| s.to_str()) == Some("rs") {
                // Run rustfmt
                let output = std::process::Command::new("rustfmt").arg(file).output()?;

                if !output.status.success() {
                    eprintln!(
                        "rustfmt warning: {}",
                        String::from_utf8_lossy(&output.stderr)
                    );
                }
            }
        }
        Ok(())
    }

    /// Run code checks
    async fn run_code_checks(&self, files: &[std::path::PathBuf]) -> Result<GenerationResult> {
        // TODO: Run cargo check or other code validation tools
        Ok(GenerationResult {
            generated_files: files.to_vec(),
            warnings: vec![],
            errors: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DependencySpec;

    #[test]
    fn dependency_lookup_key_prefers_actr_type_over_name() {
        let spec = DependencySpec {
            alias: "echo".into(),
            name: "echo-service".into(),
            actr_type: Some(actr_protocol::ActrType::from_string_repr("acme:Echo:1.0.0").unwrap()),
            fingerprint: None,
        };
        assert_eq!(
            ValidationPipeline::dependency_lookup_key(&spec),
            "acme:Echo:1.0.0"
        );

        let spec = DependencySpec {
            alias: "echo".into(),
            name: "echo-service".into(),
            actr_type: None,
            fingerprint: None,
        };
        assert_eq!(
            ValidationPipeline::dependency_lookup_key(&spec),
            "echo-service"
        );
    }

    #[test]
    fn install_result_success_is_empty() {
        let result = InstallResult::success();
        assert!(result.installed_dependencies.is_empty());
        assert!(!result.updated_config);
        assert!(!result.updated_lock_file);
        assert_eq!(result.cache_updates, 0);
    }

    #[test]
    fn install_result_summary_counts_deps_and_cache() {
        let result = InstallResult {
            installed_dependencies: vec![],
            updated_config: true,
            updated_lock_file: true,
            cache_updates: 5,
            warnings: vec![],
        };
        let s = result.summary();
        assert!(s.contains("Installed 0 dependencies"));
        assert!(s.contains("updated 5 cache entries"));
    }

    // ── mock trait implementations for pipeline testing ─────────────────

    use crate::core::components::{
        ConfigManager, DependencyResolver, FingerprintValidator, NetworkValidator, ServiceDiscovery,
    };
    use crate::core::{ConfigValidation, NetworkCheckOptions};
    use actr_config::ManifestConfig;
    use async_trait::async_trait;
    use std::path::Path;
    use std::sync::Arc;

    struct MockConfig {
        is_valid: bool,
    }

    #[async_trait]
    impl ConfigManager for MockConfig {
        async fn load_config(&self, _path: &Path) -> anyhow::Result<ManifestConfig> {
            unreachable!()
        }
        async fn save_config(&self, _config: &ManifestConfig, _path: &Path) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn update_dependency(&self, _spec: &DependencySpec) -> anyhow::Result<()> {
            Ok(())
        }
        async fn validate_config(&self) -> anyhow::Result<ConfigValidation> {
            Ok(ConfigValidation {
                is_valid: self.is_valid,
                errors: if self.is_valid {
                    vec![]
                } else {
                    vec!["invalid".into()]
                },
                warnings: vec![],
            })
        }
        fn get_project_root(&self) -> &Path {
            Path::new(".")
        }
        async fn backup_config(&self) -> anyhow::Result<crate::core::ConfigBackup> {
            Ok(crate::core::ConfigBackup {
                original_path: Path::new("manifest.toml").into(),
                backup_path: Path::new("manifest.toml.bak").into(),
                timestamp: std::time::SystemTime::now(),
            })
        }
        async fn restore_backup(&self, _backup: crate::core::ConfigBackup) -> anyhow::Result<()> {
            Ok(())
        }
        async fn remove_backup(&self, _backup: crate::core::ConfigBackup) -> anyhow::Result<()> {
            Ok(())
        }
    }

    struct MockDepResolver;
    #[async_trait]
    impl DependencyResolver for MockDepResolver {
        async fn resolve_spec(
            &self,
            _config: &ManifestConfig,
        ) -> anyhow::Result<Vec<DependencySpec>> {
            Ok(vec![])
        }
        async fn resolve_dependencies(
            &self,
            _specs: &[DependencySpec],
            _service_details: &[crate::core::ServiceDetails],
        ) -> anyhow::Result<Vec<ResolvedDependency>> {
            Ok(vec![])
        }
        async fn check_conflicts(
            &self,
            _deps: &[ResolvedDependency],
        ) -> anyhow::Result<Vec<crate::core::ConflictReport>> {
            Ok(vec![])
        }
        async fn build_dependency_graph(
            &self,
            _deps: &[ResolvedDependency],
        ) -> anyhow::Result<crate::core::DependencyGraph> {
            unreachable!()
        }
    }

    struct MockServiceDiscovery {
        service_available: bool,
    }
    #[async_trait]
    impl ServiceDiscovery for MockServiceDiscovery {
        async fn discover_services(
            &self,
            _filter: Option<&crate::core::ServiceFilter>,
        ) -> anyhow::Result<Vec<crate::core::ServiceInfo>> {
            unreachable!()
        }
        async fn get_service_details(
            &self,
            _name: &str,
        ) -> anyhow::Result<crate::core::ServiceDetails> {
            if self.service_available {
                Ok(crate::core::ServiceDetails {
                    info: crate::core::ServiceInfo {
                        name: "echo".into(),
                        tags: vec![],
                        fingerprint: "fp-echo".into(),
                        actr_type: actr_protocol::ActrType::from_string_repr("acme:Echo:1.0.0")
                            .unwrap(),
                        published_at: None,
                        description: None,
                        methods: vec![],
                    },
                    proto_files: vec![],
                    dependencies: vec![],
                })
            } else {
                anyhow::bail!("service not found")
            }
        }
        async fn check_service_availability(
            &self,
            _name: &str,
        ) -> anyhow::Result<crate::core::AvailabilityStatus> {
            Ok(crate::core::AvailabilityStatus {
                is_available: self.service_available,
                last_seen: None,
                health: crate::core::HealthStatus::Healthy,
            })
        }
        async fn get_service_proto(
            &self,
            _name: &str,
        ) -> anyhow::Result<Vec<crate::core::ProtoFile>> {
            unreachable!()
        }
    }

    struct MockNetworkValidator {
        reachable: bool,
    }
    #[async_trait]
    impl NetworkValidator for MockNetworkValidator {
        async fn check_connectivity(
            &self,
            _name: &str,
            _opts: &crate::core::NetworkCheckOptions,
        ) -> anyhow::Result<crate::core::ConnectivityStatus> {
            Ok(crate::core::ConnectivityStatus {
                is_reachable: self.reachable,
                response_time_ms: if self.reachable { Some(5) } else { None },
                error: if self.reachable {
                    None
                } else {
                    Some("unreachable".into())
                },
            })
        }
        async fn verify_service_health(
            &self,
            _name: &str,
            _opts: &crate::core::NetworkCheckOptions,
        ) -> anyhow::Result<crate::core::HealthStatus> {
            unreachable!()
        }
        async fn test_latency(
            &self,
            _name: &str,
            _opts: &crate::core::NetworkCheckOptions,
        ) -> anyhow::Result<crate::core::LatencyInfo> {
            unreachable!()
        }
        async fn batch_check(
            &self,
            names: &[String],
            _opts: &crate::core::NetworkCheckOptions,
        ) -> anyhow::Result<Vec<crate::core::NetworkCheckResult>> {
            Ok(names
                .iter()
                .map(|_| crate::core::NetworkCheckResult {
                    connectivity: crate::core::ConnectivityStatus {
                        is_reachable: self.reachable,
                        response_time_ms: if self.reachable { Some(5) } else { None },
                        error: if self.reachable {
                            None
                        } else {
                            Some("unreachable".into())
                        },
                    },
                    health: if self.reachable {
                        crate::core::HealthStatus::Healthy
                    } else {
                        crate::core::HealthStatus::Unhealthy
                    },
                    latency: None,
                })
                .collect())
        }
    }

    struct MockFingerprintValidator;
    #[async_trait]
    impl FingerprintValidator for MockFingerprintValidator {
        async fn compute_service_fingerprint(
            &self,
            svc: &crate::core::ServiceInfo,
        ) -> anyhow::Result<crate::core::Fingerprint> {
            Ok(crate::core::Fingerprint {
                algorithm: "sha256".into(),
                value: svc.fingerprint.clone(),
            })
        }
        async fn verify_fingerprint(
            &self,
            expected: &crate::core::Fingerprint,
            actual: &crate::core::Fingerprint,
        ) -> anyhow::Result<bool> {
            Ok(expected.algorithm == actual.algorithm && expected.value == actual.value)
        }
        async fn compute_project_fingerprint(
            &self,
            _path: &Path,
        ) -> anyhow::Result<crate::core::Fingerprint> {
            unreachable!()
        }
        async fn generate_lock_fingerprint(
            &self,
            _deps: &[ResolvedDependency],
        ) -> anyhow::Result<crate::core::Fingerprint> {
            unreachable!()
        }
    }

    struct MockCacheManager;
    #[async_trait]
    impl crate::core::CacheManager for MockCacheManager {
        async fn get_cached_proto(
            &self,
            _name: &str,
        ) -> anyhow::Result<Option<crate::core::CachedProto>> {
            unreachable!()
        }
        async fn cache_proto(
            &self,
            _name: &str,
            _protos: &[crate::core::ProtoFile],
        ) -> anyhow::Result<()> {
            Ok(())
        }
        async fn invalidate_cache(&self, _name: &str) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn clear_cache(&self) -> anyhow::Result<()> {
            unreachable!()
        }
        async fn get_cache_stats(&self) -> anyhow::Result<crate::core::CacheStats> {
            unreachable!()
        }
    }

    struct MockProtoProcessor {
        proto_is_valid: bool,
    }
    #[async_trait]
    impl crate::core::ProtoProcessor for MockProtoProcessor {
        async fn discover_proto_files(
            &self,
            _path: &Path,
        ) -> anyhow::Result<Vec<crate::core::ProtoFile>> {
            Ok(vec![])
        }
        async fn parse_proto_services(
            &self,
            _files: &[crate::core::ProtoFile],
        ) -> anyhow::Result<Vec<crate::core::ServiceDefinition>> {
            unreachable!()
        }
        async fn generate_code(
            &self,
            _input: &Path,
            output: &Path,
        ) -> anyhow::Result<crate::core::GenerationResult> {
            Ok(crate::core::GenerationResult {
                generated_files: vec![output.to_path_buf()],
                warnings: vec![],
                errors: vec![],
            })
        }
        async fn validate_proto_syntax(
            &self,
            _files: &[crate::core::ProtoFile],
        ) -> anyhow::Result<crate::core::ValidationReport> {
            Ok(crate::core::ValidationReport {
                is_valid: self.proto_is_valid,
                config_validation: crate::core::ConfigValidation {
                    is_valid: true,
                    errors: vec![],
                    warnings: vec![],
                },
                dependency_validation: vec![],
                network_validation: vec![],
                fingerprint_validation: vec![],
                conflicts: vec![],
            })
        }
    }

    #[test]
    fn validation_pipeline_constructs_and_exposes_getters() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);

        let pipeline = ValidationPipeline::new(
            config.clone(),
            dep.clone(),
            sd.clone(),
            net.clone(),
            fp.clone(),
        );
        // Getters return the stored Arcs.
        let _ = pipeline.config_manager();
        let _ = pipeline.dependency_resolver();
        let _ = pipeline.service_discovery();
        let _ = pipeline.network_validator();
    }

    #[tokio::test]
    async fn validate_project_returns_early_on_invalid_config() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: false });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);

        let pipeline = ValidationPipeline::new(config, dep, sd, net, fp);
        let report = pipeline.validate_project().await.unwrap();
        assert!(!report.is_valid);
        assert!(!report.config_validation.is_valid);
        assert!(
            report
                .config_validation
                .errors
                .contains(&"invalid".to_string())
        );
    }

    #[tokio::test]
    async fn validate_dependencies_reports_service_availability() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);
        let pipeline = ValidationPipeline::new(config, dep, sd, net, fp);

        let specs = vec![
            DependencySpec {
                alias: "echo".into(),
                name: "echo-service".into(),
                actr_type: None,
                fingerprint: None,
            },
            DependencySpec {
                alias: "other".into(),
                name: "other-service".into(),
                actr_type: None,
                fingerprint: None,
            },
        ];
        let results = pipeline.validate_dependencies(&specs).await.unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_available);
        assert!(results[1].is_available);
    }

    #[tokio::test]
    async fn validate_network_connectivity_maps_reachable_and_unreachable() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);

        let deps = vec![ResolvedDependency {
            spec: DependencySpec {
                alias: "echo".into(),
                name: "echo".into(),
                actr_type: None,
                fingerprint: None,
            },
            fingerprint: "".into(),
            proto_files: vec![],
        }];

        // Reachable.
        let net_r: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let pipeline =
            ValidationPipeline::new(config.clone(), dep.clone(), sd.clone(), net_r, fp.clone());
        let results = pipeline
            .validate_network_connectivity(&deps, &NetworkCheckOptions::default())
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].is_reachable);

        // Unreachable.
        let net_u: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: false });
        let pipeline2 = ValidationPipeline::new(config, dep, sd, net_u, fp);
        let results2 = pipeline2
            .validate_network_connectivity(&deps, &NetworkCheckOptions::default())
            .await
            .unwrap();
        assert!(!results2[0].is_reachable);
    }

    #[tokio::test]
    async fn validate_fingerprints_covers_present_and_missing_fingerprints() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);

        let deps = vec![
            // Dep with matching (pre-computed) fingerprint → passes without recompute.
            ResolvedDependency {
                spec: DependencySpec {
                    alias: "a".into(),
                    name: "a".into(),
                    actr_type: None,
                    fingerprint: Some("fp-a".into()),
                },
                fingerprint: "fp-a".into(),
                proto_files: vec![],
            },
            // Dep with empty computed fingerprint → fetches from service discovery,
            // then verifies against spec fingerprint (mismatch → invalid).
            ResolvedDependency {
                spec: DependencySpec {
                    alias: "b".into(),
                    name: "b".into(),
                    actr_type: None,
                    fingerprint: Some("fp-b".into()),
                },
                fingerprint: "".into(),
                proto_files: vec![],
            },
            // No spec fingerprint → empty expected → is_valid = true.
            ResolvedDependency {
                spec: DependencySpec {
                    alias: "c".into(),
                    name: "c".into(),
                    actr_type: None,
                    fingerprint: None,
                },
                fingerprint: "fp-c".into(),
                proto_files: vec![],
            },
        ];

        let pipeline = ValidationPipeline::new(config, dep, sd, net, fp);
        let results = pipeline.validate_fingerprints(&deps).await.unwrap();
        assert_eq!(results.len(), 3);
        // a: pre-computed matches → valid.
        assert!(results[0].is_valid);
        // b: fetched from service discovery (fingerprint="fp-echo"), spec expects "fp-b" → mismatch.
        assert!(!results[1].is_valid);
        // c: expected empty → valid regardless.
        assert!(results[2].is_valid);
    }

    #[test]
    fn install_and_generation_pipelines_construct() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);
        let cache: Arc<dyn crate::core::CacheManager> = Arc::new(MockCacheManager);
        let proto: Arc<dyn crate::core::ProtoProcessor> = Arc::new(MockProtoProcessor {
            proto_is_valid: true,
        });

        let vp = ValidationPipeline::new(
            config.clone(),
            dep.clone(),
            sd.clone(),
            net.clone(),
            fp.clone(),
        );
        let ip = InstallPipeline::new(vp, config.clone(), cache.clone(), proto.clone());
        let _ = ip.validation_pipeline();
        let _ = ip.config_manager();

        std::mem::drop(GenerationPipeline::new(config, proto, cache));
    }

    #[tokio::test]
    async fn install_dependencies_reports_failed_validation() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        // Service unavailable → validate_dependencies returns is_available=false.
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: false,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);
        let cache: Arc<dyn crate::core::CacheManager> = Arc::new(MockCacheManager);
        let proto: Arc<dyn crate::core::ProtoProcessor> = Arc::new(MockProtoProcessor {
            proto_is_valid: true,
        });

        let vp = ValidationPipeline::new(
            config.clone(),
            dep.clone(),
            sd.clone(),
            net.clone(),
            fp.clone(),
        );
        let ip = InstallPipeline::new(vp, config, cache, proto);

        let specs = vec![DependencySpec {
            alias: "echo".into(),
            name: "echo".into(),
            actr_type: None,
            fingerprint: None,
        }];
        let err = ip.install_dependencies(&specs).await.unwrap_err();
        assert!(format!("{err}").contains("Dependency validation failed"));
    }

    #[tokio::test]
    async fn install_dependencies_succeeds_with_empty_specs() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let dep: Arc<dyn DependencyResolver> = Arc::new(MockDepResolver);
        let sd: Arc<dyn ServiceDiscovery> = Arc::new(MockServiceDiscovery {
            service_available: true,
        });
        let net: Arc<dyn NetworkValidator> = Arc::new(MockNetworkValidator { reachable: true });
        let fp: Arc<dyn FingerprintValidator> = Arc::new(MockFingerprintValidator);
        let cache: Arc<dyn crate::core::CacheManager> = Arc::new(MockCacheManager);
        let proto: Arc<dyn crate::core::ProtoProcessor> = Arc::new(MockProtoProcessor {
            proto_is_valid: true,
        });

        let vp = ValidationPipeline::new(
            config.clone(),
            dep.clone(),
            sd.clone(),
            net.clone(),
            fp.clone(),
        );
        let ip = InstallPipeline::new(vp, config, cache, proto);
        let result = ip.install_dependencies(&[]).await.unwrap();
        assert!(result.installed_dependencies.is_empty());
    }

    #[tokio::test]
    async fn generation_pipeline_rejects_invalid_proto_syntax() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let proto_invalid: Arc<dyn crate::core::ProtoProcessor> = Arc::new(MockProtoProcessor {
            proto_is_valid: false,
        });
        let cache: Arc<dyn crate::core::CacheManager> = Arc::new(MockCacheManager);
        let gp = GenerationPipeline::new(config, proto_invalid, cache);
        let options = crate::core::pipelines::GenerationOptions {
            input_path: Path::new("protos").to_path_buf(),
            output_path: Path::new("out").to_path_buf(),
            clean_before_generate: false,
            generate_scaffold: false,
            format_code: false,
            run_checks: false,
        };
        let err = gp.generate_code(&options).await.unwrap_err();
        assert!(format!("{err}").contains("Proto file syntax validation failed"));
    }

    #[tokio::test]
    async fn generation_pipeline_succeeds_with_valid_proto() {
        let config: Arc<dyn ConfigManager> = Arc::new(MockConfig { is_valid: true });
        let proto_valid: Arc<dyn crate::core::ProtoProcessor> = Arc::new(MockProtoProcessor {
            proto_is_valid: true,
        });
        let cache: Arc<dyn crate::core::CacheManager> = Arc::new(MockCacheManager);
        let gp = GenerationPipeline::new(config, proto_valid, cache);
        let options = crate::core::pipelines::GenerationOptions {
            input_path: Path::new("protos").to_path_buf(),
            output_path: Path::new("out").to_path_buf(),
            clean_before_generate: true,
            generate_scaffold: false,
            format_code: false,
            run_checks: false,
        };
        let result = gp.generate_code(&options).await.unwrap();
        assert_eq!(result.generated_files, vec![Path::new("out").to_path_buf()]);
    }
}
