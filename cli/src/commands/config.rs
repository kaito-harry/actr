//! Config command implementation - manage CLI configuration layers.
//!
//! Supported locations:
//! - Global: `~/.actr/config.toml`
//! - Local override: `.actr/config.toml`

use crate::config::loader::{global_config_path, load_cli_config, local_config_path};
use crate::config::resolver::resolve_effective_cli_config;
use crate::config::schema::CliConfig;
use crate::core::{Command, CommandContext, CommandResult, ComponentType};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use clap::{Args, Subcommand};
use owo_colors::OwoColorize;
use std::path::{Path, PathBuf};
use toml::Value;

/// All known schema field paths — used to validate `set` keys.
const KNOWN_KEYS: &[&str] = &[
    "mfr.manufacturer",
    "mfr.keychain",
    "codegen.language",
    "codegen.output",
    "codegen.clean_before_generate",
    "cache.dir",
    "cache.auto_lock",
    "cache.prefer_cache",
    "ui.format",
    "ui.verbose",
    "ui.color",
    "ui.non_interactive",
    "network.signaling_url",
    "network.ais_endpoint",
    "network.realm_id",
    "network.realm_secret",
    "storage.hyper_data_dir",
];

fn parse_toml_document_value(content: &str, path: impl std::fmt::Display) -> Result<Value> {
    let table = toml::from_str::<toml::Table>(content)
        .with_context(|| format!("Failed to parse {path}"))?;
    Ok(Value::Table(table))
}

#[derive(Args, Clone)]
pub struct ConfigCommand {
    /// Read or write the global CLI config (~/.actr/config.toml)
    #[arg(long, conflicts_with = "local")]
    pub global: bool,

    /// Read or write the project-local CLI config (.actr/config.toml)
    #[arg(long, conflicts_with = "global")]
    pub local: bool,

    #[command(subcommand)]
    pub command: ConfigSubcommand,
}

#[derive(Subcommand, Clone)]
pub enum ConfigSubcommand {
    /// Set a configuration key to a value
    Set {
        /// Configuration key (e.g., mfr.manufacturer)
        key: String,
        /// Value to assign
        value: String,
    },
    /// Get the current value of a configuration key
    Get {
        /// Configuration key (e.g., mfr.manufacturer)
        key: String,
    },
    /// List all known schema fields with current effective values
    List,
    /// Show the raw TOML of the active scope
    Show {
        #[arg(long, default_value = "toml")]
        format: OutputFormat,
    },
    /// Remove a configuration key
    Unset {
        /// Configuration key to remove (e.g., mfr.manufacturer)
        key: String,
    },
    /// Validate syntax and schema of all config files
    Test,
}

#[derive(Debug, Clone, clap::ValueEnum, Default)]
pub enum OutputFormat {
    #[default]
    Toml,
    Json,
    Yaml,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConfigScope {
    Global,
    Local,
    Merged,
}

#[async_trait]
impl Command for ConfigCommand {
    async fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        match &self.command {
            ConfigSubcommand::Set { key, value } => self.set_config(key, value).await,
            ConfigSubcommand::Get { key } => self.get_config(key).await,
            ConfigSubcommand::List => self.list_config().await,
            ConfigSubcommand::Show { format } => self.show_config(format).await,
            ConfigSubcommand::Unset { key } => self.unset_config(key).await,
            ConfigSubcommand::Test => self.test_config().await,
        }
    }

    fn required_components(&self) -> Vec<ComponentType> {
        vec![]
    }

    fn name(&self) -> &str {
        "config"
    }

    fn description(&self) -> &str {
        "Manage layered CLI configuration (~/.actr/config.toml and .actr/config.toml)"
    }
}

impl ConfigCommand {
    fn read_scope(&self) -> ConfigScope {
        if self.global {
            ConfigScope::Global
        } else if self.local {
            ConfigScope::Local
        } else {
            ConfigScope::Merged
        }
    }

    fn write_scope(&self) -> ConfigScope {
        if self.global {
            ConfigScope::Global
        } else if self.local || Path::new("manifest.toml").exists() || Path::new(".actr").exists() {
            ConfigScope::Local
        } else {
            ConfigScope::Global
        }
    }

    fn scope_label(scope: ConfigScope) -> &'static str {
        match scope {
            ConfigScope::Global => "global",
            ConfigScope::Local => "local",
            ConfigScope::Merged => "merged",
        }
    }

    fn scope_path(scope: ConfigScope) -> Result<PathBuf> {
        match scope {
            ConfigScope::Global => Ok(global_config_path()?),
            ConfigScope::Local => Ok(local_config_path()),
            ConfigScope::Merged => bail!("Merged scope does not map to a single file"),
        }
    }

    /// Load the raw TOML Value for a specific scope.
    fn load_scope_value(&self, scope: ConfigScope) -> Result<Value> {
        match scope {
            ConfigScope::Global => {
                let path = global_config_path()?;
                if !path.exists() {
                    return Ok(Value::Table(toml::map::Map::new()));
                }
                let content = std::fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?;
                parse_toml_document_value(&content, path.display())
            }
            ConfigScope::Local => {
                let path = local_config_path();
                if !path.exists() {
                    return Ok(Value::Table(toml::map::Map::new()));
                }
                let content = std::fs::read_to_string(&path)
                    .with_context(|| format!("Failed to read {}", path.display()))?;
                parse_toml_document_value(&content, path.display())
            }
            ConfigScope::Merged => self.load_merged_value(),
        }
    }

    fn load_merged_value(&self) -> Result<Value> {
        let global_path = global_config_path()?;
        let mut merged = if global_path.exists() {
            let content = std::fs::read_to_string(&global_path)
                .with_context(|| format!("Failed to read {}", global_path.display()))?;
            parse_toml_document_value(&content, global_path.display())?
        } else {
            Value::Table(toml::map::Map::new())
        };

        let local_path = local_config_path();
        if local_path.exists() {
            let content = std::fs::read_to_string(&local_path)
                .with_context(|| format!("Failed to read {}", local_path.display()))?;
            let local_value = parse_toml_document_value(&content, local_path.display())?;
            Self::merge_values(&mut merged, local_value);
        }
        Ok(merged)
    }

    fn merge_values(base: &mut Value, overlay: Value) {
        match (base, overlay) {
            (Value::Table(base_table), Value::Table(overlay_table)) => {
                for (key, overlay_value) in overlay_table {
                    if let Some(base_value) = base_table.get_mut(&key) {
                        Self::merge_values(base_value, overlay_value);
                    } else {
                        base_table.insert(key, overlay_value);
                    }
                }
            }
            (base_slot, overlay_value) => *base_slot = overlay_value,
        }
    }

    fn get_nested_value<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
        let mut current = value;
        for part in key.split('.') {
            current = match current {
                Value::Table(table) => table.get(part)?,
                _ => return None,
            };
        }
        Some(current)
    }

    fn write_scope_file(scope: ConfigScope, config: &CliConfig) -> Result<PathBuf> {
        let path = Self::scope_path(scope)?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create {}", parent.display()))?;
        }
        let content = toml::to_string_pretty(config)
            .with_context(|| format!("Failed to serialize config for {}", path.display()))?;
        std::fs::write(&path, content)
            .with_context(|| format!("Failed to write {}", path.display()))?;
        Ok(path)
    }

    /// Apply a key=value setting to a `CliConfig` struct.
    fn apply_key_to_config(config: &mut CliConfig, key: &str, raw_value: &str) -> Result<()> {
        // Parse the value as TOML so we handle booleans/numbers correctly
        let parsed_value: Value = raw_value
            .parse::<Value>()
            .unwrap_or_else(|_| Value::String(raw_value.to_string()));

        match key {
            "mfr.manufacturer" => {
                config.mfr.manufacturer = Some(value_to_string(&parsed_value)?);
            }
            "mfr.keychain" => {
                config.mfr.keychain = Some(value_to_string(&parsed_value)?);
            }
            "codegen.language" => {
                config.codegen.language = Some(value_to_string(&parsed_value)?);
            }
            "codegen.output" => {
                config.codegen.output = Some(value_to_string(&parsed_value)?);
            }
            "codegen.clean_before_generate" => {
                config.codegen.clean_before_generate = Some(value_to_bool(&parsed_value, key)?);
            }
            "cache.dir" => {
                config.cache.dir = Some(value_to_string(&parsed_value)?);
            }
            "cache.auto_lock" => {
                config.cache.auto_lock = Some(value_to_bool(&parsed_value, key)?);
            }
            "cache.prefer_cache" => {
                config.cache.prefer_cache = Some(value_to_bool(&parsed_value, key)?);
            }
            "network.signaling_url" => {
                config.network.signaling_url = Some(value_to_string(&parsed_value)?);
            }
            "network.ais_endpoint" => {
                config.network.ais_endpoint = Some(value_to_string(&parsed_value)?);
            }
            "network.realm_id" => {
                config.network.realm_id = Some(value_to_u32(&parsed_value, key)?);
            }
            "network.realm_secret" => {
                config.network.realm_secret = Some(value_to_string(&parsed_value)?);
            }
            "storage.hyper_data_dir" => {
                config.storage.hyper_data_dir = Some(value_to_string(&parsed_value)?);
            }
            "ui.format" => {
                config.ui.format = Some(value_to_string(&parsed_value)?);
            }
            "ui.verbose" => {
                config.ui.verbose = Some(value_to_bool(&parsed_value, key)?);
            }
            "ui.color" => {
                config.ui.color = Some(value_to_string(&parsed_value)?);
            }
            "ui.non_interactive" => {
                config.ui.non_interactive = Some(value_to_bool(&parsed_value, key)?);
            }
            other => {
                bail!(
                    "Unknown configuration key '{}'. Known keys:\n{}",
                    other,
                    KNOWN_KEYS.join("\n")
                );
            }
        }
        Ok(())
    }

    /// Remove a key from a `CliConfig` struct.
    fn unset_key_from_config(config: &mut CliConfig, key: &str) -> Result<bool> {
        let was_set = match key {
            "mfr.manufacturer" => {
                let had = config.mfr.manufacturer.is_some();
                config.mfr.manufacturer = None;
                had
            }
            "mfr.keychain" => {
                let had = config.mfr.keychain.is_some();
                config.mfr.keychain = None;
                had
            }
            "codegen.language" => {
                let had = config.codegen.language.is_some();
                config.codegen.language = None;
                had
            }
            "codegen.output" => {
                let had = config.codegen.output.is_some();
                config.codegen.output = None;
                had
            }
            "codegen.clean_before_generate" => {
                let had = config.codegen.clean_before_generate.is_some();
                config.codegen.clean_before_generate = None;
                had
            }
            "cache.dir" => {
                let had = config.cache.dir.is_some();
                config.cache.dir = None;
                had
            }
            "cache.auto_lock" => {
                let had = config.cache.auto_lock.is_some();
                config.cache.auto_lock = None;
                had
            }
            "cache.prefer_cache" => {
                let had = config.cache.prefer_cache.is_some();
                config.cache.prefer_cache = None;
                had
            }
            "network.signaling_url" => {
                let had = config.network.signaling_url.is_some();
                config.network.signaling_url = None;
                had
            }
            "network.ais_endpoint" => {
                let had = config.network.ais_endpoint.is_some();
                config.network.ais_endpoint = None;
                had
            }
            "network.realm_id" => {
                let had = config.network.realm_id.is_some();
                config.network.realm_id = None;
                had
            }
            "network.realm_secret" => {
                let had = config.network.realm_secret.is_some();
                config.network.realm_secret = None;
                had
            }
            "storage.hyper_data_dir" => {
                let had = config.storage.hyper_data_dir.is_some();
                config.storage.hyper_data_dir = None;
                had
            }
            "ui.format" => {
                let had = config.ui.format.is_some();
                config.ui.format = None;
                had
            }
            "ui.verbose" => {
                let had = config.ui.verbose.is_some();
                config.ui.verbose = None;
                had
            }
            "ui.color" => {
                let had = config.ui.color.is_some();
                config.ui.color = None;
                had
            }
            "ui.non_interactive" => {
                let had = config.ui.non_interactive.is_some();
                config.ui.non_interactive = None;
                had
            }
            other => {
                bail!(
                    "Unknown configuration key '{}'. Known keys:\n{}",
                    other,
                    KNOWN_KEYS.join("\n")
                );
            }
        };
        Ok(was_set)
    }

    async fn set_config(&self, key: &str, raw_value: &str) -> Result<CommandResult> {
        let scope = self.write_scope();

        // Load existing config (or default)
        let path = Self::scope_path(scope)?;
        let mut config = load_cli_config(&path)?.unwrap_or_default();

        // Apply the setting
        Self::apply_key_to_config(&mut config, key, raw_value)?;

        // Validate after change
        config.validate().map_err(|e| anyhow::anyhow!("{}", e))?;

        // Write back
        let path = Self::write_scope_file(scope, &config)?;

        Ok(CommandResult::Success(format!(
            "{} Updated {} config: {} = {}\n{}",
            "✅".green(),
            Self::scope_label(scope).cyan(),
            key.yellow(),
            raw_value.green(),
            path.display()
        )))
    }

    async fn get_config(&self, key: &str) -> Result<CommandResult> {
        let scope = self.read_scope();
        let value = self.load_scope_value(scope)?;
        let nested = Self::get_nested_value(&value, key).ok_or_else(|| {
            anyhow::anyhow!(
                "Configuration key '{}' not found in {} scope",
                key,
                Self::scope_label(scope)
            )
        })?;

        let output = if matches!(nested, Value::Table(_) | Value::Array(_)) {
            toml::to_string_pretty(nested)?
        } else {
            nested.to_string()
        };

        Ok(CommandResult::Success(output.trim().to_string()))
    }

    async fn list_config(&self) -> Result<CommandResult> {
        // Resolve effective config to show all fields with current values
        let effective = resolve_effective_cli_config()?;
        let lines: Vec<String> = vec![
            format!("mfr.manufacturer = {}", effective.mfr.manufacturer),
            format!(
                "mfr.keychain = {}",
                effective.mfr.keychain.as_deref().unwrap_or("<not set>")
            ),
            format!("codegen.language = {}", effective.codegen.language),
            format!("codegen.output = {}", effective.codegen.output),
            format!(
                "codegen.clean_before_generate = {}",
                effective.codegen.clean_before_generate
            ),
            format!("cache.dir = {}", effective.cache.dir),
            format!("cache.auto_lock = {}", effective.cache.auto_lock),
            format!("cache.prefer_cache = {}", effective.cache.prefer_cache),
            format!("ui.format = {}", effective.ui.format),
            format!("ui.verbose = {}", effective.ui.verbose),
            format!("ui.color = {}", effective.ui.color),
            format!("ui.non_interactive = {}", effective.ui.non_interactive),
            format!(
                "network.signaling_url = {}",
                effective.network.signaling_url
            ),
            format!("network.ais_endpoint = {}", effective.network.ais_endpoint),
            format!(
                "network.realm_id = {}",
                effective
                    .network
                    .realm_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "<not set>".to_string())
            ),
            format!(
                "network.realm_secret = {}",
                effective
                    .network
                    .realm_secret
                    .as_deref()
                    .unwrap_or("<not set>")
            ),
            format!(
                "storage.hyper_data_dir = {}",
                effective.storage.hyper_data_dir.display()
            ),
        ];
        Ok(CommandResult::Success(lines.join("\n")))
    }

    async fn show_config(&self, format: &OutputFormat) -> Result<CommandResult> {
        let scope = self.read_scope();
        let value = self.load_scope_value(scope)?;
        let output = match format {
            OutputFormat::Toml => toml::to_string_pretty(&value)?,
            OutputFormat::Json => serde_json::to_string_pretty(&value)?,
            OutputFormat::Yaml => serde_yaml::to_string(&value)?,
        };
        Ok(CommandResult::Success(output))
    }

    async fn unset_config(&self, key: &str) -> Result<CommandResult> {
        let scope = self.write_scope();
        let path = Self::scope_path(scope)?;
        let mut config = load_cli_config(&path)?.unwrap_or_default();

        let was_set = Self::unset_key_from_config(&mut config, key)?;
        if !was_set {
            bail!(
                "Configuration key '{}' not found in {} scope",
                key,
                Self::scope_label(scope)
            );
        }
        let path = Self::write_scope_file(scope, &config)?;
        Ok(CommandResult::Success(format!(
            "{} Removed {} from {} config\n{}",
            "✅".green(),
            key.cyan(),
            Self::scope_label(scope),
            path.display()
        )))
    }

    async fn test_config(&self) -> Result<CommandResult> {
        let scope = self.read_scope();
        let mut lines = Vec::new();
        match scope {
            ConfigScope::Global => {
                let path = global_config_path()?;
                if let Some(config) = load_cli_config(&path)? {
                    config.validate().map_err(|e| anyhow::anyhow!("{}", e))?;
                }
                lines.push(format!(
                    "{} Global config syntax and schema are valid",
                    "✅".green()
                ));
                lines.push(path.display().to_string());
            }
            ConfigScope::Local => {
                let path = local_config_path();
                if let Some(config) = load_cli_config(&path)? {
                    config.validate().map_err(|e| anyhow::anyhow!("{}", e))?;
                }
                lines.push(format!(
                    "{} Local config syntax and schema are valid",
                    "✅".green()
                ));
                lines.push(path.display().to_string());
            }
            ConfigScope::Merged => {
                let global_path = global_config_path()?;
                let local_path = local_config_path();

                if let Some(config) = load_cli_config(&global_path)? {
                    config.validate().map_err(|e| anyhow::anyhow!("{}", e))?;
                    lines.push(format!(
                        "{} Global config parsed and validated",
                        "✅".green()
                    ));
                } else {
                    lines.push(format!(
                        "{} Global config not found (using defaults)",
                        "ℹ️".cyan()
                    ));
                }

                if let Some(config) = load_cli_config(&local_path)? {
                    config.validate().map_err(|e| anyhow::anyhow!("{}", e))?;
                    lines.push(format!(
                        "{} Local config parsed and validated",
                        "✅".green()
                    ));
                } else {
                    lines.push(format!(
                        "{} Local config not found (using defaults)",
                        "ℹ️".cyan()
                    ));
                }

                // Validate the merged result
                resolve_effective_cli_config()?;
                lines.push(format!("{} Merged view is valid", "✅".green()));
            }
        }
        Ok(CommandResult::Success(lines.join("\n")))
    }
}

/// Convert a TOML Value to a String, extracting the inner string value.
fn value_to_string(v: &Value) -> Result<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        other => Ok(other.to_string()),
    }
}

/// Convert a TOML Value to a bool.
fn value_to_bool(v: &Value, key: &str) -> Result<bool> {
    match v {
        Value::Boolean(b) => Ok(*b),
        Value::String(s) => match s.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            other => bail!(
                "Key '{}' expects a boolean (true/false), got '{}'",
                key,
                other
            ),
        },
        other => bail!("Key '{}' expects a boolean, got {:?}", key, other),
    }
}

fn value_to_u32(v: &Value, key: &str) -> Result<u32> {
    // Accept both numbers and strings (e.g., `1001` or `"1001"`).
    let s = value_to_string(v)?;
    s.parse::<u32>()
        .map_err(|_| anyhow::anyhow!("Key '{}' expects a positive integer, got '{}'", key, s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_toml_document_as_value_table() {
        let value = parse_toml_document_value(
            r#"
[mfr]
manufacturer = "demo1"

[network]
realm_id = 2368266035
"#,
            ".actr/config.toml",
        )
        .expect("config TOML should parse");

        assert_eq!(
            ConfigCommand::get_nested_value(&value, "mfr.manufacturer"),
            Some(&Value::String("demo1".to_string()))
        );
    }

    #[test]
    fn parse_toml_document_value_errors_on_invalid_input() {
        assert!(parse_toml_document_value("invalid = {{{", "bad.toml").is_err());
    }

    #[test]
    fn get_nested_value_returns_none_for_missing_or_non_table_path() {
        let value = parse_toml_document_value("[mfr]\nmanufacturer = \"x\"\n", "f").unwrap();
        assert!(ConfigCommand::get_nested_value(&value, "mfr.missing").is_none());
        assert!(ConfigCommand::get_nested_value(&value, "nonexistent.key").is_none());
        // Non-table intermediate stops traversal.
        assert!(ConfigCommand::get_nested_value(&value, "mfr.manufacturer.nested").is_none());
    }

    #[test]
    fn apply_key_to_config_sets_every_known_string_field() {
        let mut config = CliConfig::default();
        for (key, val) in [
            ("mfr.manufacturer", "acme"),
            ("mfr.keychain", "/keys/k.json"),
            ("codegen.language", "rust"),
            ("codegen.output", "src/gen"),
            ("cache.dir", "/tmp/cache"),
            ("network.signaling_url", "ws://localhost"),
            ("network.ais_endpoint", "http://ais"),
            ("network.realm_secret", "secret"),
            ("storage.hyper_data_dir", "/hyper"),
            ("ui.format", "json"),
            ("ui.color", "auto"),
        ] {
            ConfigCommand::apply_key_to_config(&mut config, key, val)
                .unwrap_or_else(|e| panic!("apply {key} failed: {e}"));
        }
        assert_eq!(config.mfr.manufacturer.as_deref(), Some("acme"));
        assert_eq!(
            config.network.signaling_url.as_deref(),
            Some("ws://localhost")
        );
        assert_eq!(config.storage.hyper_data_dir.as_deref(), Some("/hyper"));
        assert_eq!(config.ui.format.as_deref(), Some("json"));
    }

    #[test]
    fn apply_key_to_config_parses_bool_and_integer_fields() {
        let mut config = CliConfig::default();
        for key in [
            "codegen.clean_before_generate",
            "cache.auto_lock",
            "cache.prefer_cache",
            "ui.verbose",
            "ui.non_interactive",
        ] {
            ConfigCommand::apply_key_to_config(&mut config, key, "true")
                .unwrap_or_else(|e| panic!("apply {key}=true failed: {e}"));
            assert_eq!(
                ConfigCommand::apply_key_to_config(&mut CliConfig::default(), key, "false")
                    .unwrap(),
                ()
            );
        }
        assert_eq!(config.cache.auto_lock, Some(true));
        assert_eq!(config.ui.non_interactive, Some(true));

        // realm_id accepts both bare number and quoted string.
        ConfigCommand::apply_key_to_config(&mut config, "network.realm_id", "4242").unwrap();
        assert_eq!(config.network.realm_id, Some(4242));
        ConfigCommand::apply_key_to_config(&mut config, "network.realm_id", "\"9999\"").unwrap();
        assert_eq!(config.network.realm_id, Some(9999));
    }

    #[test]
    fn apply_key_to_config_rejects_unknown_key_and_bad_values() {
        let mut config = CliConfig::default();
        let err = ConfigCommand::apply_key_to_config(&mut config, "nope.nope", "x").unwrap_err();
        assert!(format!("{err}").contains("Unknown configuration key 'nope.nope'"));

        let err = ConfigCommand::apply_key_to_config(&mut config, "cache.auto_lock", "maybe")
            .unwrap_err();
        assert!(format!("{err}").contains("expects a boolean"));

        let err =
            ConfigCommand::apply_key_to_config(&mut config, "network.realm_id", "abc").unwrap_err();
        assert!(format!("{err}").contains("expects a positive integer"));
    }

    #[test]
    fn value_to_bool_accepts_native_and_string_forms() {
        assert!(value_to_bool(&Value::Boolean(true), "k").unwrap());
        assert!(!value_to_bool(&Value::String("false".into()), "k").unwrap());
        assert!(value_to_bool(&Value::Integer(1), "k").is_err());
    }

    #[test]
    fn value_to_string_extracts_inner_or_displays() {
        assert_eq!(value_to_string(&Value::String("x".into())).unwrap(), "x");
        assert_eq!(value_to_string(&Value::Integer(7)).unwrap(), "7");
    }

    #[test]
    fn unset_key_from_config_clears_and_reports_presence() {
        let mut config = CliConfig::default();
        ConfigCommand::apply_key_to_config(&mut config, "mfr.manufacturer", "acme").unwrap();
        assert!(ConfigCommand::unset_key_from_config(&mut config, "mfr.manufacturer").unwrap());
        assert!(config.mfr.manufacturer.is_none());
        // Already unset → reports not set.
        assert!(!ConfigCommand::unset_key_from_config(&mut config, "mfr.manufacturer").unwrap());

        let err = ConfigCommand::unset_key_from_config(&mut CliConfig::default(), "bogus.key")
            .unwrap_err();
        assert!(format!("{err}").contains("Unknown configuration key 'bogus.key'"));
    }

    #[test]
    fn merge_values_overlays_tables_and_scalars() {
        let mut base = parse_toml_document_value(
            "[mfr]\nmanufacturer = \"old\"\n[network]\nrealm_id = 1\n",
            "base",
        )
        .unwrap();
        let overlay = parse_toml_document_value(
            "[mfr]\nkeychain = \"/k\"\nmanufacturer = \"new\"\n",
            "overlay",
        )
        .unwrap();
        ConfigCommand::merge_values(&mut base, overlay);
        // Overlay scalar wins, new sub-key inserted.
        assert_eq!(
            ConfigCommand::get_nested_value(&base, "mfr.manufacturer"),
            Some(&Value::String("new".into()))
        );
        assert_eq!(
            ConfigCommand::get_nested_value(&base, "mfr.keychain"),
            Some(&Value::String("/k".into()))
        );
        // Untouched table preserved.
        assert_eq!(
            ConfigCommand::get_nested_value(&base, "network.realm_id"),
            Some(&Value::Integer(1))
        );
    }
}
