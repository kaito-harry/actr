use actr_config::ManifestConfig;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs;
use toml_edit::{DocumentMut, InlineTable, Item, Table, Value};

use crate::core::{ConfigBackup, ConfigManager, ConfigValidation, DependencySpec};
use actr_config::ConfigParser;

pub struct TomlConfigManager {
    config_path: PathBuf,
    project_root: PathBuf,
}

impl TomlConfigManager {
    pub fn new<P: Into<PathBuf>>(config_path: P) -> Self {
        let config_path = config_path.into();
        let project_root = resolve_project_root(&config_path);
        Self {
            config_path,
            project_root,
        }
    }

    async fn read_config_string(&self, path: &Path) -> Result<String> {
        fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read config file: {}", path.display()))
    }

    async fn write_config_string(&self, path: &Path, contents: &str) -> Result<()> {
        fs::write(path, contents)
            .await
            .with_context(|| format!("Failed to write config file: {}", path.display()))
    }

    fn build_backup_path(&self) -> Result<PathBuf> {
        let file_name = self
            .config_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("Config path is missing file name"))?
            .to_string_lossy();
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let backup_name = format!("{file_name}.bak.{timestamp}");
        let parent = self
            .config_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        Ok(parent.join(backup_name))
    }
}

#[async_trait]
impl ConfigManager for TomlConfigManager {
    async fn load_config(&self, path: &Path) -> Result<ManifestConfig> {
        ConfigParser::from_manifest_file(path)
            .with_context(|| format!("Failed to parse config: {}", path.display()))
    }

    async fn save_config(&self, _config: &ManifestConfig, _path: &Path) -> Result<()> {
        Err(anyhow::anyhow!(
            "Saving parsed ManifestConfig is not supported; update manifest.toml directly"
        ))
    }

    async fn update_dependency(&self, spec: &DependencySpec) -> Result<()> {
        let contents = self.read_config_string(&self.config_path).await?;
        let mut doc = contents
            .parse::<DocumentMut>()
            .with_context(|| format!("Failed to parse config: {}", self.config_path.display()))?;

        if !doc.contains_key("dependencies") {
            doc["dependencies"] = Item::Table(Table::new());
        }

        // Preserve existing dependency entry if it exists
        let existing_dep = doc["dependencies"]
            .get(&spec.alias)
            .and_then(|item| item.as_inline_table());

        let mut dep_table = InlineTable::new();

        // Add name attribute if it differs from alias
        if spec.name != spec.alias {
            dep_table.insert("name", Value::from(spec.name.clone()));
        }

        // Add actr_type attribute - preserve existing if new one is not provided
        if let Some(actr_type) = &spec.actr_type {
            let actr_type_repr = actr_type.to_string_repr();
            if actr_type_repr.is_empty() {
                return Err(anyhow::anyhow!(
                    "Actr type is required for dependency: {}",
                    spec.alias
                ));
            }
            dep_table.insert("actr_type", Value::from(actr_type_repr));
        } else if let Some(existing) = existing_dep {
            // Preserve existing actr_type if new spec doesn't have one
            if let Some(existing_actr_type) = existing.get("actr_type") {
                dep_table.insert("actr_type", existing_actr_type.clone());
            }
        }

        // Add fingerprint - preserve existing if new one is not provided
        if let Some(fingerprint) = &spec.fingerprint {
            dep_table.insert("fingerprint", Value::from(fingerprint.as_str()));
        } else if let Some(existing) = existing_dep {
            // Preserve existing fingerprint if new spec doesn't have one
            if let Some(existing_fp) = existing.get("fingerprint") {
                dep_table.insert("fingerprint", existing_fp.clone());
            }
        }

        doc["dependencies"][&spec.alias] = Item::Value(Value::InlineTable(dep_table));

        self.write_config_string(&self.config_path, &doc.to_string())
            .await
    }

    async fn validate_config(&self) -> Result<ConfigValidation> {
        let mut errors = Vec::new();
        let warnings = Vec::new();

        let config = match ConfigParser::from_manifest_file(&self.config_path) {
            Ok(config) => config,
            Err(e) => {
                errors.push(format!("Failed to parse config: {e}"));
                return Ok(ConfigValidation {
                    is_valid: false,
                    errors,
                    warnings,
                });
            }
        };

        if config.package.name.trim().is_empty() {
            errors.push("package.name is required".to_string());
        }

        for dependency in &config.dependencies {
            if dependency.alias.trim().is_empty() {
                errors.push("dependency alias is required".to_string());
            }
            if let Some(actr_type) = &dependency.actr_type
                && actr_type.name.trim().is_empty()
            {
                errors.push(format!(
                    "dependency {} has an empty actr_type name",
                    dependency.alias
                ));
            }
        }

        Ok(ConfigValidation {
            is_valid: errors.is_empty(),
            errors,
            warnings,
        })
    }

    fn get_project_root(&self) -> &Path {
        &self.project_root
    }

    async fn backup_config(&self) -> Result<ConfigBackup> {
        if !self.config_path.exists() {
            return Err(anyhow::anyhow!(
                "Config file not found: {}",
                self.config_path.display()
            ));
        }

        let backup_path = self.build_backup_path()?;
        fs::copy(&self.config_path, &backup_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to backup config from {} to {}",
                    self.config_path.display(),
                    backup_path.display()
                )
            })?;

        Ok(ConfigBackup {
            original_path: self.config_path.clone(),
            backup_path,
            timestamp: SystemTime::now(),
        })
    }

    async fn restore_backup(&self, backup: ConfigBackup) -> Result<()> {
        fs::copy(&backup.backup_path, &backup.original_path)
            .await
            .with_context(|| {
                format!(
                    "Failed to restore config from {} to {}",
                    backup.backup_path.display(),
                    backup.original_path.display()
                )
            })?;
        Ok(())
    }

    async fn remove_backup(&self, backup: ConfigBackup) -> Result<()> {
        if backup.backup_path.exists() {
            fs::remove_file(&backup.backup_path)
                .await
                .with_context(|| {
                    format!(
                        "Failed to remove backup file: {}",
                        backup.backup_path.display()
                    )
                })?;
        }
        Ok(())
    }
}

fn resolve_project_root(config_path: &Path) -> PathBuf {
    let canonical_path =
        std::fs::canonicalize(config_path).expect("Failed to canonicalize config path");
    canonical_path
        .parent()
        .expect("Config path must have a parent directory")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::DependencySpec;
    use tempfile::TempDir;

    const VALID_MANIFEST: &str = r#"edition = 1

[package]
name = "echo-service"
manufacturer = "acme"
version = "0.1.0"
description = "Echo service"

[dependencies]
"#;

    fn manager_with(dir: &Path, body: &str) -> (TomlConfigManager, PathBuf) {
        let manifest = dir.join("manifest.toml");
        std::fs::write(&manifest, body).unwrap();
        (TomlConfigManager::new(&manifest), manifest)
    }

    #[tokio::test]
    async fn load_config_parses_valid_manifest() {
        let dir = TempDir::new().unwrap();
        let (mgr, manifest) = manager_with(dir.path(), VALID_MANIFEST);
        let config = mgr.load_config(&manifest).await.unwrap();
        assert_eq!(config.package.name, "echo-service");
    }

    #[tokio::test]
    async fn load_config_errors_on_missing_file() {
        let dir = TempDir::new().unwrap();
        let (mgr, _) = manager_with(dir.path(), VALID_MANIFEST);
        assert!(
            mgr.load_config(&dir.path().join("nope.toml"))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn save_config_is_unsupported() {
        let dir = TempDir::new().unwrap();
        let (mgr, manifest) = manager_with(dir.path(), VALID_MANIFEST);
        let config = mgr.load_config(&manifest).await.unwrap();
        let err = mgr.save_config(&config, &manifest).await.unwrap_err();
        assert!(format!("{err}").contains("not supported"));
    }

    #[tokio::test]
    async fn validate_config_reports_valid_and_parse_errors() {
        let dir = TempDir::new().unwrap();
        let (mgr, _) = manager_with(dir.path(), VALID_MANIFEST);
        let result = mgr.validate_config().await.unwrap();
        assert!(result.is_valid);
        assert!(result.errors.is_empty());

        // Malformed TOML → parse-error branch.
        std::fs::write(dir.path().join("manifest.toml"), "bad = {{{").unwrap();
        let result = mgr.validate_config().await.unwrap();
        assert!(!result.is_valid);
        assert!(result.errors[0].contains("Failed to parse config"));
    }

    #[tokio::test]
    async fn validate_config_flags_empty_package_name() {
        let dir = TempDir::new().unwrap();
        let body = VALID_MANIFEST.replace("name = \"echo-service\"", "name = \"\"");
        let (mgr, _) = manager_with(dir.path(), &body);
        let result = mgr.validate_config().await.unwrap();
        // Either the parser rejects the empty name, or the validator flags it;
        // either way the config is invalid with a reported error.
        assert!(!result.is_valid);
        assert!(!result.errors.is_empty());
    }

    #[tokio::test]
    async fn update_dependency_adds_new_entry_with_actr_type() {
        let dir = TempDir::new().unwrap();
        let (mgr, _) = manager_with(dir.path(), VALID_MANIFEST);
        let spec = DependencySpec {
            alias: "echo".into(),
            name: "echo-service".into(),
            actr_type: Some(actr_protocol::ActrType::from_string_repr("acme:Echo:1.0.0").unwrap()),
            fingerprint: Some("fp1".into()),
        };
        mgr.update_dependency(&spec).await.unwrap();
        let content = std::fs::read_to_string(dir.path().join("manifest.toml")).unwrap();
        assert!(content.contains("echo"), "added dep key: {content}");
        assert!(
            content.contains("acme:Echo:1.0.0"),
            "actr_type written: {content}"
        );
        assert!(content.contains("fp1"), "fingerprint written: {content}");
        // name differs from alias → name field emitted.
        assert!(
            content.contains("name = \"echo-service\""),
            "name written: {content}"
        );
    }

    #[tokio::test]
    async fn update_dependency_preserves_existing_actr_type_and_fingerprint() {
        let dir = TempDir::new().unwrap();
        let body = r#"edition = 1
[package]
name = "svc"
manufacturer = "acme"
version = "0.1.0"

[dependencies]
echo = { actr_type = "acme:Echo:1.0.0", fingerprint = "keep-fp" }
"#;
        let (mgr, _) = manager_with(dir.path(), body);
        // New spec omits actr_type/fingerprint → existing must be preserved.
        let spec = DependencySpec {
            alias: "echo".into(),
            name: "echo".into(),
            actr_type: None,
            fingerprint: None,
        };
        mgr.update_dependency(&spec).await.unwrap();
        let content = std::fs::read_to_string(dir.path().join("manifest.toml")).unwrap();
        assert!(
            content.contains("acme:Echo:1.0.0"),
            "actr_type preserved: {content}"
        );
        assert!(
            content.contains("keep-fp"),
            "fingerprint preserved: {content}"
        );
    }

    #[tokio::test]
    async fn backup_restore_remove_roundtrip() {
        let dir = TempDir::new().unwrap();
        let (mgr, manifest) = manager_with(dir.path(), VALID_MANIFEST);

        let backup = mgr.backup_config().await.unwrap();
        assert!(backup.backup_path.exists());

        // Corrupt the original, then restore from backup.
        std::fs::write(&manifest, "corrupted").unwrap();
        mgr.restore_backup(backup.clone()).await.unwrap();
        assert_eq!(std::fs::read_to_string(&manifest).unwrap(), VALID_MANIFEST);

        // Remove the backup file.
        mgr.remove_backup(backup).await.unwrap();
        // Removing again is a no-op (file gone).
        let gone = ConfigBackup {
            original_path: manifest.clone(),
            backup_path: dir.path().join("absent.bak"),
            timestamp: SystemTime::now(),
        };
        mgr.remove_backup(gone).await.unwrap();
    }

    #[tokio::test]
    async fn backup_config_errors_when_file_missing() {
        let dir = TempDir::new().unwrap();
        let (mgr, manifest) = manager_with(dir.path(), VALID_MANIFEST);
        std::fs::remove_file(&manifest).unwrap();
        let err = mgr.backup_config().await.unwrap_err();
        assert!(format!("{err}").contains("Config file not found"));
    }

    #[test]
    fn get_project_root_returns_parent() {
        let dir = TempDir::new().unwrap();
        let (mgr, manifest) = manager_with(dir.path(), VALID_MANIFEST);
        let expected = std::fs::canonicalize(&manifest)
            .unwrap()
            .parent()
            .unwrap()
            .to_path_buf();
        assert_eq!(mgr.get_project_root(), expected);
    }
}
