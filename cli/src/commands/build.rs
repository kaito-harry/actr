//! `actr build` - build source artifacts and package signed `.actr` workloads.

use std::path::{Path, PathBuf};
use std::process::{Command as StdCommand, Stdio};

use actr_config::{BuildArtifact, BuildConfig, BuildProfile, ConfigParser, ManifestConfig};
use anyhow::{Context, Result};
use async_trait::async_trait;
use cargo_metadata::MetadataCommand;
use clap::Args;

use crate::commands::codegen::metadata_path;
use crate::commands::package_build::{
    PackageBuildInput, build_package, default_dist_output_path, print_build_summary,
    resolve_key_path,
};
use crate::core::{Command, CommandContext, CommandResult, ComponentType};
use crate::project_language::DetectedProjectLanguage;

#[derive(Args, Debug)]
#[command(
    about = "Build source artifact and package a signed .actr workload",
    long_about = "Build source artifact and package a signed .actr workload from manifest.toml"
)]
pub struct BuildCommand {
    /// manifest.toml path
    #[arg(
        long = "manifest-path",
        short = 'm',
        default_value = "manifest.toml",
        value_name = "FILE"
    )]
    pub manifest_path: PathBuf,

    /// Override target triple
    #[arg(long, short = 't', value_name = "TARGET")]
    pub target: Option<String>,

    /// Output .actr file path
    #[arg(long, short = 'o', value_name = "FILE")]
    pub output: Option<PathBuf>,

    /// Signing key file (overrides config mfr.keychain)
    #[arg(long, short = 'k', value_name = "FILE")]
    pub key: Option<PathBuf>,

    /// Skip compilation and only package the declared binary artifact
    #[arg(long)]
    pub no_compile: bool,
}

#[async_trait]
impl Command for BuildCommand {
    async fn execute(&self, _ctx: &CommandContext) -> Result<CommandResult> {
        execute_build(self).await?;
        Ok(CommandResult::Success(String::new()))
    }

    fn required_components(&self) -> Vec<ComponentType> {
        vec![]
    }

    fn name(&self) -> &str {
        "build"
    }

    fn description(&self) -> &str {
        "Build source artifact and package a signed .actr workload"
    }
}

async fn execute_build(args: &BuildCommand) -> Result<()> {
    let manifest_path = resolve_manifest_path(&args.manifest_path)?;
    let config = ConfigParser::from_manifest_file(&manifest_path).with_context(|| {
        format!(
            "Failed to load manifest configuration from {}",
            manifest_path.display()
        )
    })?;

    let binary = config.binary.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "manifest.toml is missing [binary].\nDeclare the final packaged artifact path before running `actr build`."
        )
    })?;

    let effective_target = resolve_effective_target(args, &config)?;
    let output_path = resolve_output_path(&manifest_path, &effective_target, args.output.as_ref())?;

    if !args.no_compile {
        let build = config.build.as_ref().ok_or_else(|| {
            anyhow::anyhow!(
                "manifest.toml is missing [build].\nAdd [build] or rerun with `--no-compile` to package an existing artifact."
            )
        })?;
        ensure_rust_codegen_ready(build)?;
        compile_project(
            &manifest_path,
            &output_path,
            &binary.path,
            &effective_target,
            build,
        )?;
    }

    if !binary.path.exists() {
        anyhow::bail!(
            "Configured binary artifact not found: {}\nCheck [binary].path or your post_build steps.",
            binary.path.display()
        );
    }

    let cli_config = crate::config::resolver::resolve_effective_cli_config()?;
    let key_path = resolve_key_path(args.key.as_deref(), cli_config.mfr.keychain.as_deref())?;

    let summary = build_package(PackageBuildInput {
        binary_path: binary.path.clone(),
        config_path: manifest_path,
        key_path,
        output_path,
        target: effective_target,
        resources: vec![],
    })?;

    print_build_summary(&summary);
    Ok(())
}

fn ensure_rust_codegen_ready(build: &BuildConfig) -> Result<()> {
    let project_root = build
        .manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    if DetectedProjectLanguage::detect(&project_root) != DetectedProjectLanguage::Rust {
        return Ok(());
    }

    let generated_dir = project_root.join("src/generated");
    let generated_meta = metadata_path(&generated_dir);
    if generated_dir.exists() && generated_meta.exists() {
        return Ok(());
    }

    anyhow::bail!(
        "Rust generated sources are missing or stale for {}.\nRun `actr gen -l rust` before `actr build`.",
        project_root.display()
    );
}

fn resolve_manifest_path(path: &Path) -> Result<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };

    if !candidate.exists() {
        anyhow::bail!(
            "manifest.toml not found: {}\nBy default `actr build` looks for ./manifest.toml. Use `-m, --manifest-path` to specify a different path.",
            candidate.display()
        );
    }

    Ok(candidate)
}

fn resolve_effective_target(args: &BuildCommand, config: &ManifestConfig) -> Result<String> {
    if let Some(target) = &args.target {
        return Ok(target.clone());
    }

    if let Some(target) = config
        .binary
        .as_ref()
        .and_then(|binary| binary.target.clone())
    {
        return Ok(target);
    }

    if let Some(target) = config.build.as_ref().and_then(|build| build.target.clone()) {
        return Ok(target);
    }

    resolve_host_target()
}

fn resolve_output_path(
    manifest_path: &Path,
    effective_target: &str,
    output: Option<&PathBuf>,
) -> Result<PathBuf> {
    let manifest_dir = manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    match output {
        Some(path) if path.is_absolute() => Ok(path.clone()),
        Some(path) => Ok(manifest_dir.join(path)),
        None => default_dist_output_path(manifest_path, effective_target),
    }
}

fn compile_project(
    manifest_path: &Path,
    output_path: &Path,
    binary_path: &Path,
    effective_target: &str,
    build: &BuildConfig,
) -> Result<()> {
    if !build.manifest_path.exists() {
        anyhow::bail!(
            "Cargo manifest not found: {}",
            build.manifest_path.display()
        );
    }

    let cargo_target_dir = resolve_cargo_target_dir(&build.manifest_path)?;

    ensure_target_installed(effective_target)?;
    run_cargo_build(build, effective_target)?;
    run_post_build_steps(
        manifest_path,
        output_path,
        binary_path,
        effective_target,
        &cargo_target_dir,
        build,
    )?;

    if !binary_path.exists() {
        anyhow::bail!(
            "Binary artifact was not produced after build/post_build: {}",
            binary_path.display()
        );
    }

    Ok(())
}

fn ensure_target_installed(target: &str) -> Result<()> {
    let host_target = resolve_host_target()?;
    if target == host_target {
        return Ok(());
    }

    let status = StdCommand::new("rustup")
        .arg("target")
        .arg("add")
        .arg(target)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("Failed to run `rustup target add {target}`"))?;

    if !status.success() {
        anyhow::bail!("`rustup target add {target}` failed with status {status}");
    }

    Ok(())
}

fn run_cargo_build(build: &BuildConfig, effective_target: &str) -> Result<()> {
    let mut command = StdCommand::new("cargo");
    command.arg("build");
    command.arg("--manifest-path").arg(&build.manifest_path);

    match build.artifact {
        BuildArtifact::Lib => {
            command.arg("--lib");
        }
        BuildArtifact::Bin => {
            command
                .arg("--bin")
                .arg(resolve_cargo_bin_name(&build.manifest_path)?);
        }
    }

    if build.profile == BuildProfile::Release {
        command.arg("--release");
    }

    command.arg("--target").arg(effective_target);

    if !build.features.is_empty() {
        command.arg("--features").arg(build.features.join(","));
    }

    if build.no_default_features {
        command.arg("--no-default-features");
    }

    // Component Model guests (`wasm32-wasip2`) must be linked by
    // `wasm-component-ld` so the emitted artifact is a Component rather
    // than a core module. Rust 1.91 ships `wasm-component-ld 0.5.17`,
    // which rejects the async custom sections wit-bindgen 0.57 emits.
    // Use Cargo's target-specific linker variable so host build scripts
    // still link with the native linker.
    if effective_target == "wasm32-wasip2" {
        let linker = resolve_wasm_component_linker()?;
        command.env("CARGO_TARGET_WASM32_WASIP2_LINKER", linker);
    }

    let status = command
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| {
            format!(
                "Failed to run cargo build for manifest {}",
                build.manifest_path.display()
            )
        })?;

    if !status.success() {
        anyhow::bail!("cargo build failed with status {status}");
    }

    Ok(())
}

/// Locate a `wasm-component-ld` binary suitable for linking Component
/// Model guests and validate its version.
///
/// Lookup order:
/// 1. `WASM_COMPONENT_LD` environment variable (explicit override)
/// 2. `wasm-component-ld` on `PATH`
/// 3. `~/.cargo/bin/wasm-component-ld`
///
/// Returns an actionable `cargo install` hint when none are found.
fn resolve_wasm_component_linker() -> Result<PathBuf> {
    const REQUIRED: &str = "0.5.22";

    let candidate = if let Some(p) = std::env::var_os("WASM_COMPONENT_LD") {
        PathBuf::from(p)
    } else if let Some(p) = find_on_path("wasm-component-ld") {
        p
    } else if let Some(home) = std::env::var_os("HOME") {
        let p = PathBuf::from(home).join(".cargo/bin/wasm-component-ld");
        if p.is_file() {
            p
        } else {
            anyhow::bail!(
                "`wasm-component-ld` (>= {REQUIRED}) is required to link wasm32-wasip2 Components.\n\
                 Install it with: cargo install wasm-component-ld --version {REQUIRED}\n\
                 Or set WASM_COMPONENT_LD to an existing binary."
            );
        }
    } else {
        anyhow::bail!(
            "`wasm-component-ld` (>= {REQUIRED}) is required to link wasm32-wasip2 Components.\n\
             Install it with: cargo install wasm-component-ld --version {REQUIRED}\n\
             Or set WASM_COMPONENT_LD to an existing binary."
        );
    };

    if !candidate.is_file() {
        anyhow::bail!(
            "wasm-component-ld path `{}` is not a file.\n\
             Install it with: cargo install wasm-component-ld --version {REQUIRED}",
            candidate.display()
        );
    }

    validate_wasm_component_linker_version(&candidate, REQUIRED)?;

    Ok(candidate)
}

fn validate_wasm_component_linker_version(linker: &Path, required: &str) -> Result<()> {
    let output = StdCommand::new(linker)
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .with_context(|| {
            format!(
                "Failed to run `{}` --version for wasm-component-ld validation",
                linker.display()
            )
        })?;

    if !output.status.success() {
        anyhow::bail!(
            "`{}` --version failed with status {}.\n\
             Install it with: cargo install wasm-component-ld --version {required}",
            linker.display(),
            output.status
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let version_text = if stdout.trim().is_empty() {
        stderr.trim()
    } else {
        stdout.trim()
    };

    let actual = extract_semver(version_text).with_context(|| {
        format!(
            "Failed to parse wasm-component-ld version from `{version_text}`.\n\
             Install it with: cargo install wasm-component-ld --version {required}"
        )
    })?;
    let required = parse_semver(required).expect("REQUIRED wasm-component-ld version is valid");

    if actual < required {
        anyhow::bail!(
            "`{}` reports version {}, but wasm32-wasip2 Component linking requires >= {}.\n\
             Install it with: cargo install wasm-component-ld --version {}",
            linker.display(),
            format_semver(actual),
            format_semver(required),
            format_semver(required)
        );
    }

    Ok(())
}

fn extract_semver(text: &str) -> Option<(u64, u64, u64)> {
    text.split_whitespace().find_map(parse_semver)
}

fn parse_semver(text: &str) -> Option<(u64, u64, u64)> {
    let mut parts = text.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch_text = parts.next()?;
    let patch_len = patch_text
        .bytes()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if patch_len == 0 {
        return None;
    }
    let patch = patch_text[..patch_len].parse().ok()?;
    Some((major, minor, patch))
}

fn format_semver(version: (u64, u64, u64)) -> String {
    format!("{}.{}.{}", version.0, version.1, version.2)
}

/// Walk `PATH` looking for `binary`. Returns the first hit that exists
/// as a file. Mirrors the shell `which` semantics closely enough for
/// the CLI's purposes — no PATHEXT handling because `wasm-component-ld`
/// is the only target today and Windows builds are not on the Phase 1
/// migration path.
fn find_on_path(binary: &str) -> Option<PathBuf> {
    let path_var = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(binary);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn run_post_build_steps(
    manifest_path: &Path,
    output_path: &Path,
    binary_path: &Path,
    effective_target: &str,
    cargo_target_dir: &Path,
    build: &BuildConfig,
) -> Result<()> {
    if build.post_build.is_empty() {
        return Ok(());
    }

    let manifest_dir = manifest_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    for command_text in &build.post_build {
        let output = StdCommand::new("sh")
            .arg("-c")
            .arg(command_text)
            .current_dir(&manifest_dir)
            .env("ACTR_BUILD_MANIFEST_PATH", manifest_path)
            .env("ACTR_BUILD_PROJECT_DIR", &manifest_dir)
            .env("ACTR_BUILD_BINARY_PATH", binary_path)
            .env("ACTR_BUILD_TARGET", effective_target)
            .env("ACTR_BUILD_PROFILE", build.profile.as_str())
            .env("ACTR_BUILD_OUTPUT_PATH", output_path)
            .env("ACTR_BUILD_CARGO_TARGET_DIR", cargo_target_dir)
            .env("CARGO_TARGET_DIR", cargo_target_dir)
            .output()
            .with_context(|| format!("Failed to run post_build command: {command_text}"))?;

        if !output.stdout.is_empty() {
            print!("{}", String::from_utf8_lossy(&output.stdout));
        }
        if !output.stderr.is_empty() {
            eprint!("{}", String::from_utf8_lossy(&output.stderr));
        }

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            anyhow::bail!(
                "post_build command failed: {command_text}\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
                output.status,
                stdout,
                stderr,
            );
        }
    }

    Ok(())
}

fn resolve_cargo_bin_name(manifest_path: &Path) -> Result<String> {
    let metadata = MetadataCommand::new()
        .manifest_path(manifest_path)
        .no_deps()
        .exec()
        .with_context(|| {
            format!(
                "Failed to read Cargo metadata from {}",
                manifest_path.display()
            )
        })?;

    let manifest_path =
        std::fs::canonicalize(manifest_path).unwrap_or_else(|_| manifest_path.to_path_buf());

    let package = metadata
        .packages
        .iter()
        .find(|package| {
            std::fs::canonicalize(package.manifest_path.as_std_path())
                .map(|path| path == manifest_path)
                .unwrap_or(false)
        })
        .or_else(|| metadata.root_package())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Unable to resolve Cargo package for {}",
                manifest_path.display()
            )
        })?;

    Ok(package.name.clone())
}

fn resolve_cargo_target_dir(manifest_path: &Path) -> Result<PathBuf> {
    let metadata = MetadataCommand::new()
        .manifest_path(manifest_path)
        .no_deps()
        .exec()
        .with_context(|| {
            format!(
                "Failed to read Cargo metadata from {}",
                manifest_path.display()
            )
        })?;

    Ok(metadata.target_directory.into_std_path_buf())
}

fn resolve_host_target() -> Result<String> {
    let output = StdCommand::new("rustc")
        .arg("-vV")
        .output()
        .context("Failed to run `rustc -vV` to resolve host target")?;

    if !output.status.success() {
        anyhow::bail!("`rustc -vV` failed with status {}", output.status);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let host = stdout
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| anyhow::anyhow!("Unable to resolve host target from `rustc -vV`"))?;

    Ok(host.trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_semver_extracts_three_component_versions() {
        assert_eq!(parse_semver("1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_semver("0.5.22"), Some((0, 5, 22)));
        assert_eq!(parse_semver("1.0"), None);
        assert_eq!(parse_semver("abc"), None);
        assert_eq!(parse_semver(""), None);
        assert_eq!(parse_semver("1.2.3-beta"), Some((1, 2, 3)));
    }

    #[test]
    fn extract_semver_finds_first_semver_in_text() {
        assert_eq!(extract_semver("wasm-component-ld 0.5.22"), Some((0, 5, 22)));
        assert_eq!(extract_semver("version 1.0.0 (abc 2.3.4)"), Some((1, 0, 0)));
        assert_eq!(extract_semver("no version here"), None);
    }

    #[test]
    fn format_semver_joins_with_dots() {
        assert_eq!(format_semver((1, 2, 3)), "1.2.3");
        assert_eq!(format_semver((0, 5, 22)), "0.5.22");
    }

    #[test]
    fn resolve_manifest_path_errors_when_file_absent() {
        let dir = tempfile::TempDir::new().unwrap();
        let p = dir.path().join("manifest.toml");
        assert!(resolve_manifest_path(&p).is_err());
    }

    #[test]
    fn resolve_output_path_handles_absolute_relative_and_default() {
        let mf = std::path::Path::new("/proj/manifest.toml");
        assert_eq!(
            resolve_output_path(
                mf,
                "x86_64-linux",
                Some(&std::path::PathBuf::from("/abs/pkg.actr"))
            )
            .unwrap(),
            std::path::PathBuf::from("/abs/pkg.actr")
        );
        assert_eq!(
            resolve_output_path(
                mf,
                "x86_64-linux",
                Some(&std::path::PathBuf::from("rel/pkg.actr"))
            )
            .unwrap(),
            std::path::PathBuf::from("/proj/rel/pkg.actr")
        );
        // Without explicit output, defaults to dist/<name>-<target>.actr
        let err = resolve_output_path(mf, "x86_64-linux", None).unwrap_err();
        assert!(format!("{err}").contains("manifest"));
    }
}
