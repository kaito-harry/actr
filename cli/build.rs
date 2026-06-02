use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

const GENERATED_WEB_ASSETS: &[&str] = &["actr_sw_host_bg.wasm", "actr_sw_host.js"];

fn main() {
    // Only run if we are in a git repository
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output();

    let git_hash = match output {
        Ok(output) if output.status.success() => {
            String::from_utf8(output.stdout).unwrap().trim().to_string()
        }
        _ => "unknown".to_string(),
    };

    // Get git commit date
    let output_date = Command::new("git")
        .args(["log", "-1", "--format=%cs"])
        .output();

    let git_date = match output_date {
        Ok(output) if output.status.success() => {
            String::from_utf8(output.stdout).unwrap().trim().to_string()
        }
        _ => "unknown".to_string(),
    };

    println!("cargo:rustc-env=ACTR_GIT_HASH={}", git_hash);
    println!("cargo:rustc-env=ACTR_GIT_DATE={}", git_date);
    // Re-run build script if git head changes (monorepo: .git is in parent)
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/refs");
    // Re-run if web runtime assets change
    println!("cargo:rerun-if-changed=assets/web-runtime");

    copy_generated_web_assets();
}

fn copy_generated_web_assets() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let asset_dir = manifest_dir.join("assets/web-runtime");
    let generated_dir = manifest_dir
        .parent()
        .expect("cli crate should live under the workspace root")
        .join("bindings/web/dist/sw");
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("web-runtime");

    fs::create_dir_all(&out_dir).unwrap();

    for name in GENERATED_WEB_ASSETS {
        let src = resolve_generated_asset_source(&asset_dir, &generated_dir, name);
        if !src.is_file() {
            fail_missing_web_asset(&src);
        }

        fs::copy(&src, out_dir.join(name)).unwrap_or_else(|err| {
            panic!(
                "failed to copy generated web runtime asset {}: {err}",
                src.display()
            )
        });
    }
}

fn resolve_generated_asset_source(asset_dir: &Path, generated_dir: &Path, name: &str) -> PathBuf {
    let cli_asset = asset_dir.join(name);
    if cli_asset.is_file() {
        return cli_asset;
    }

    generated_dir.join(name)
}

fn fail_missing_web_asset(path: &Path) -> ! {
    panic!(
        "missing generated web runtime asset: {}\n\
         run `bash bindings/web/scripts/sync-cli-assets.sh --build` from the workspace root \
         before building actr-cli",
        path.display()
    );
}
