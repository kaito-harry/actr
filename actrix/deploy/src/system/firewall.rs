//! Firewall planning and application for deploy service flow.

use anyhow::Result;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

const ENABLE_STUN: u8 = 0b00010;
const ENABLE_TURN: u8 = 0b00100;

const DEFAULT_HTTP_PORT: u16 = 8080;
const DEFAULT_HTTPS_PORT: u16 = 8443;
const DEFAULT_ICE_PORT: u16 = 3478;
const DEFAULT_TURN_RELAY_PORT_RANGE: &str = "49152-65535";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FirewallManagerKind {
    Ufw,
    Firewalld,
    Unsupported,
}

impl FirewallManagerKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ufw => "ufw",
            Self::Firewalld => "firewalld",
            Self::Unsupported => "unsupported",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FirewallManager {
    kind: FirewallManagerKind,
    active: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Protocol {
    Tcp,
    Udp,
}

impl Protocol {
    fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Udp => "udp",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PortSpec {
    Single(u16),
    Range(u16, u16),
}

impl PortSpec {
    fn display(self) -> String {
        match self {
            Self::Single(port) => port.to_string(),
            Self::Range(start, end) => format!("{start}-{end}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FirewallRule {
    protocol: Protocol,
    port: PortSpec,
    reason: String,
}

#[derive(Debug, Deserialize, Default)]
struct RuntimeConfig {
    #[serde(default)]
    enable: u8,
    #[serde(default)]
    bind: BindConfig,
    #[serde(default)]
    turn: TurnConfig,
}

#[derive(Debug, Deserialize, Default)]
struct BindConfig {
    #[serde(default)]
    http: Option<ListenerConfig>,
    #[serde(default)]
    https: Option<ListenerConfig>,
    #[serde(default)]
    ice: Option<ListenerConfig>,
}

#[derive(Debug, Deserialize, Default)]
struct ListenerConfig {
    #[serde(default)]
    ip: Option<String>,
    #[serde(default)]
    port: Option<u16>,
}

#[derive(Debug, Deserialize, Default)]
struct TurnConfig {
    #[serde(default)]
    relay_port_range: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FirewallPlanPreview {
    pub manager_name: String,
    pub manager_active: bool,
    pub supported: bool,
    pub rules: Vec<String>,
    pub commands: Vec<String>,
}

pub fn plan_firewall(config_path: &Path) -> Result<Option<FirewallPlanPreview>> {
    let runtime_config = load_runtime_config(config_path)?;
    let rules = collect_rules(&runtime_config);
    if rules.is_empty() {
        return Ok(None);
    }

    let manager = detect_firewall_manager();
    let commands = render_commands(manager.kind, &rules);
    let rule_lines = rules
        .iter()
        .map(|rule| {
            format!(
                "{} {}/{} ({})",
                rule.reason,
                rule.port.display(),
                rule.protocol.as_str(),
                rule.protocol.as_str().to_uppercase()
            )
        })
        .collect();

    Ok(Some(FirewallPlanPreview {
        manager_name: manager.kind.as_str().to_string(),
        manager_active: manager.active,
        supported: manager.kind != FirewallManagerKind::Unsupported,
        rules: rule_lines,
        commands,
    }))
}

pub fn apply_firewall(config_path: &Path) -> Result<()> {
    let runtime_config = load_runtime_config(config_path)?;
    let rules = collect_rules(&runtime_config);
    if rules.is_empty() {
        return Ok(());
    }

    let manager = detect_firewall_manager();
    if manager.kind == FirewallManagerKind::Unsupported {
        anyhow::bail!("No supported firewall manager found (supported: ufw, firewalld)");
    }

    apply_rules(manager.kind, &rules)
}

fn load_runtime_config(config_path: &Path) -> Result<RuntimeConfig> {
    let config_text = std::fs::read_to_string(config_path)?;
    let runtime_config = toml::from_str::<RuntimeConfig>(&config_text)?;
    Ok(runtime_config)
}

fn collect_rules(config: &RuntimeConfig) -> Vec<FirewallRule> {
    let mut raw_rules = Vec::new();
    let has_ice_surface = config.enable & (ENABLE_STUN | ENABLE_TURN) != 0;

    // Control plane is always available under /admin and reuses main HTTP/HTTPS listeners.
    // Therefore HTTP/HTTPS firewall planning is based on bind listeners, not enable bitmask.
    if let Some(http) = &config.bind.http {
        let port = http.port.unwrap_or(DEFAULT_HTTP_PORT);
        if should_open_for_bind(http.ip.as_deref()) {
            raw_rules.push(FirewallRule {
                protocol: Protocol::Tcp,
                port: PortSpec::Single(port),
                reason: "HTTP endpoints".to_string(),
            });
        }
    }

    if let Some(https) = &config.bind.https {
        let port = https.port.unwrap_or(DEFAULT_HTTPS_PORT);
        if should_open_for_bind(https.ip.as_deref()) {
            raw_rules.push(FirewallRule {
                protocol: Protocol::Tcp,
                port: PortSpec::Single(port),
                reason: "HTTPS endpoints".to_string(),
            });
        }
    }

    if has_ice_surface && let Some(ice) = &config.bind.ice {
        let port = ice.port.unwrap_or(DEFAULT_ICE_PORT);
        if should_open_for_bind(ice.ip.as_deref()) {
            raw_rules.push(FirewallRule {
                protocol: Protocol::Udp,
                port: PortSpec::Single(port),
                reason: "STUN/TURN bind port".to_string(),
            });
        }
    }

    if config.enable & ENABLE_TURN != 0 {
        let relay_range = config
            .turn
            .relay_port_range
            .as_deref()
            .unwrap_or(DEFAULT_TURN_RELAY_PORT_RANGE);
        if let Some((start, end)) = parse_relay_port_range(relay_range) {
            raw_rules.push(FirewallRule {
                protocol: Protocol::Udp,
                port: PortSpec::Range(start, end),
                reason: "TURN relay range".to_string(),
            });
        }
    }

    deduplicate_rules(raw_rules)
}

fn deduplicate_rules(rules: Vec<FirewallRule>) -> Vec<FirewallRule> {
    let mut merged: BTreeMap<(Protocol, PortSpec), Vec<String>> = BTreeMap::new();

    for rule in rules {
        let key = (rule.protocol, rule.port);
        merged.entry(key).or_default().push(rule.reason);
    }

    merged
        .into_iter()
        .map(|((protocol, port), reasons)| FirewallRule {
            protocol,
            port,
            reason: reasons.join(" + "),
        })
        .collect()
}

fn should_open_for_bind(bind_ip: Option<&str>) -> bool {
    !bind_ip.is_some_and(is_loopback_bind)
}

fn is_loopback_bind(raw: &str) -> bool {
    let value = raw.trim().to_ascii_lowercase();
    value == "localhost"
        || value == "::1"
        || value == "127.0.0.1"
        || value.starts_with("127.")
}

fn parse_relay_port_range(raw: &str) -> Option<(u16, u16)> {
    let (start, end) = raw.split_once('-')?;
    let start = start.trim().parse::<u16>().ok()?;
    let end = end.trim().parse::<u16>().ok()?;

    if start == 0 || end == 0 || start > end {
        return None;
    }

    Some((start, end))
}

fn detect_firewall_manager() -> FirewallManager {
    let ufw_exists = command_exists("ufw");
    let firewalld_exists = command_exists("firewall-cmd");
    let ufw_active = ufw_exists && is_ufw_active();
    let firewalld_active = firewalld_exists && is_firewalld_active();

    if ufw_active {
        return FirewallManager {
            kind: FirewallManagerKind::Ufw,
            active: true,
        };
    }

    if firewalld_active {
        return FirewallManager {
            kind: FirewallManagerKind::Firewalld,
            active: true,
        };
    }

    if ufw_exists {
        return FirewallManager {
            kind: FirewallManagerKind::Ufw,
            active: false,
        };
    }

    if firewalld_exists {
        return FirewallManager {
            kind: FirewallManagerKind::Firewalld,
            active: false,
        };
    }

    FirewallManager {
        kind: FirewallManagerKind::Unsupported,
        active: false,
    }
}

fn render_commands(manager: FirewallManagerKind, rules: &[FirewallRule]) -> Vec<String> {
    match manager {
        FirewallManagerKind::Ufw => rules
            .iter()
            .map(|rule| {
                let target = match rule.port {
                    PortSpec::Single(port) => format!("{port}/{}", rule.protocol.as_str()),
                    PortSpec::Range(start, end) => {
                        format!("{start}:{end}/{}", rule.protocol.as_str())
                    }
                };
                format!("sudo ufw allow {target}")
            })
            .collect(),
        FirewallManagerKind::Firewalld => {
            let mut commands: Vec<String> = rules
                .iter()
                .map(|rule| {
                    let target = match rule.port {
                        PortSpec::Single(port) => port.to_string(),
                        PortSpec::Range(start, end) => format!("{start}-{end}"),
                    };
                    format!(
                        "sudo firewall-cmd --permanent --add-port={target}/{}",
                        rule.protocol.as_str()
                    )
                })
                .collect();
            commands.push("sudo firewall-cmd --reload".to_string());
            commands
        }
        FirewallManagerKind::Unsupported => Vec::new(),
    }
}

fn apply_rules(manager: FirewallManagerKind, rules: &[FirewallRule]) -> Result<()> {
    match manager {
        FirewallManagerKind::Ufw => apply_ufw_rules(rules),
        FirewallManagerKind::Firewalld => apply_firewalld_rules(rules),
        FirewallManagerKind::Unsupported => anyhow::bail!(
            "No supported firewall manager found (supported: ufw, firewalld)"
        ),
    }
}

fn apply_ufw_rules(rules: &[FirewallRule]) -> Result<()> {
    for rule in rules {
        let target = match rule.port {
            PortSpec::Single(port) => format!("{port}/{}", rule.protocol.as_str()),
            PortSpec::Range(start, end) => format!("{start}:{end}/{}", rule.protocol.as_str()),
        };

        let output = Command::new("sudo")
            .args(["ufw", "allow", &target])
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to apply ufw rule '{}': {}", target, stderr.trim());
        }
    }
    Ok(())
}

fn apply_firewalld_rules(rules: &[FirewallRule]) -> Result<()> {
    for rule in rules {
        let target = match rule.port {
            PortSpec::Single(port) => port.to_string(),
            PortSpec::Range(start, end) => format!("{start}-{end}"),
        };
        let arg = format!("--add-port={target}/{}", rule.protocol.as_str());

        let output = Command::new("sudo")
            .args(["firewall-cmd", "--permanent", &arg])
            .output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!("Failed to apply firewalld rule '{}': {}", arg, stderr.trim());
        }
    }

    let reload_output = Command::new("sudo")
        .args(["firewall-cmd", "--reload"])
        .output()?;
    if !reload_output.status.success() {
        let stderr = String::from_utf8_lossy(&reload_output.stderr);
        anyhow::bail!("Failed to reload firewalld: {}", stderr.trim());
    }

    Ok(())
}

fn is_ufw_active() -> bool {
    Command::new("ufw")
        .arg("status")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
            stdout.contains("status: active")
        })
        .unwrap_or(false)
}

fn is_firewalld_active() -> bool {
    Command::new("firewall-cmd")
        .arg("--state")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            let stdout = String::from_utf8_lossy(&output.stdout).to_ascii_lowercase();
            stdout.contains("running")
        })
        .unwrap_or(false)
}

fn command_exists(command: &str) -> bool {
    Command::new("which")
        .arg(command)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_runtime_config() -> RuntimeConfig {
        RuntimeConfig {
            enable: ENABLE_STUN | ENABLE_TURN,
            bind: BindConfig {
                http: Some(ListenerConfig {
                    ip: Some("0.0.0.0".to_string()),
                    port: Some(8080),
                }),
                https: Some(ListenerConfig {
                    ip: Some("0.0.0.0".to_string()),
                    port: Some(8443),
                }),
                ice: Some(ListenerConfig {
                    ip: Some("0.0.0.0".to_string()),
                    port: Some(3478),
                }),
            },
            turn: TurnConfig {
                relay_port_range: Some("50000-50010".to_string()),
            },
        }
    }

    #[test]
    fn collect_rules_should_include_http_ice_and_turn() {
        let rules = collect_rules(&sample_runtime_config());
        let mut signatures: Vec<String> = rules
            .iter()
            .map(|rule| format!("{}/{}", rule.port.display(), rule.protocol.as_str()))
            .collect();
        signatures.sort();

        assert!(signatures.contains(&"8080/tcp".to_string()));
        assert!(signatures.contains(&"8443/tcp".to_string()));
        assert!(signatures.contains(&"3478/udp".to_string()));
        assert!(signatures.contains(&"50000-50010/udp".to_string()));
    }

    #[test]
    fn collect_rules_should_include_http_even_if_only_ice_is_enabled() {
        let mut cfg = sample_runtime_config();
        cfg.enable = ENABLE_TURN;

        let signatures: Vec<String> = collect_rules(&cfg)
            .iter()
            .map(|rule| format!("{}/{}", rule.port.display(), rule.protocol.as_str()))
            .collect();

        assert!(signatures.contains(&"8080/tcp".to_string()));
        assert!(signatures.contains(&"8443/tcp".to_string()));
    }

    #[test]
    fn should_skip_rules_for_loopback_bindings() {
        let mut cfg = sample_runtime_config();
        cfg.bind.http = Some(ListenerConfig {
            ip: Some("127.0.0.1".to_string()),
            port: Some(8080),
        });
        cfg.bind.https = Some(ListenerConfig {
            ip: Some("localhost".to_string()),
            port: Some(8443),
        });

        let rules = collect_rules(&cfg);
        let signatures: Vec<String> = rules
            .iter()
            .map(|rule| format!("{}/{}", rule.port.display(), rule.protocol.as_str()))
            .collect();

        assert!(!signatures.contains(&"8080/tcp".to_string()));
        assert!(!signatures.contains(&"8443/tcp".to_string()));
    }

    #[test]
    fn relay_port_range_parser_handles_invalid_values() {
        assert_eq!(parse_relay_port_range("49152-65535"), Some((49152, 65535)));
        assert_eq!(parse_relay_port_range("65535-49152"), None);
        assert_eq!(parse_relay_port_range("not-a-range"), None);
        assert_eq!(parse_relay_port_range("0-100"), None);
    }
}
