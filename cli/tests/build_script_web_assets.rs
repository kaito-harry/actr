use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn missing_generated_web_assets_copy_from_canonical_sources_without_mutating_cli_assets() {
    let temp = TempRepo::new("actr-cli-build-script");
    let repo_root = temp.path();
    let cli_dir = repo_root.join("cli");
    let asset_dir = cli_dir.join("assets/web-runtime");
    fs::create_dir_all(&asset_dir).expect("create cli asset dir");
    let actor_asset = asset_dir.join("actor.sw.js");
    fs::write(&actor_asset, "// actor service worker\n").expect("write stable actor asset");
    fs::write(asset_dir.join("actr-host.html"), "<!doctype html>\n")
        .expect("write stable host asset");

    let generated_dir = repo_root.join("bindings/web/dist/sw");
    fs::create_dir_all(&generated_dir).expect("create fake generated asset dir");
    fs::write(generated_dir.join("actr_sw_host_bg.wasm"), "fake wasm")
        .expect("write canonical wasm asset");
    fs::write(generated_dir.join("actr_sw_host.js"), "fake js").expect("write canonical js asset");

    let script_dir = repo_root.join("bindings/web/scripts");
    fs::create_dir_all(&script_dir).expect("create fake script dir");
    fs::write(
        script_dir.join("sync-cli-assets.sh"),
        r#"#!/bin/sh
set -eu
printf 'called\n' > sync-called.txt
printf 'mutated\n' > cli/assets/web-runtime/actor.sw.js
"#,
    )
    .expect("write fake sync script");

    let out_dir = repo_root.join("out");
    fs::create_dir_all(&out_dir).expect("create OUT_DIR");
    let build_script = compile_build_script(repo_root);

    let output = Command::new(&build_script)
        .current_dir(repo_root)
        .env("CARGO_MANIFEST_DIR", &cli_dir)
        .env("OUT_DIR", &out_dir)
        .output()
        .expect("run compiled build script");

    assert!(
        output.status.success(),
        "build script should recover missing generated assets from canonical sources\nstatus: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !repo_root.join("sync-called.txt").exists(),
        "build script should not invoke sync-cli-assets.sh when only OUT_DIR needs generated assets"
    );
    assert!(
        out_dir.join("web-runtime/actr_sw_host_bg.wasm").is_file(),
        "wasm asset should be copied into OUT_DIR"
    );
    assert!(
        out_dir.join("web-runtime/actr_sw_host.js").is_file(),
        "js asset should be copied into OUT_DIR"
    );
    assert_eq!(
        fs::read_to_string(&actor_asset).expect("read actor asset"),
        "// actor service worker\n",
        "build script must not rewrite tracked cli assets during a normal build"
    );
}

fn compile_build_script(repo_root: &Path) -> PathBuf {
    let source = std::env::var_os("ACTR_BUILD_RS_SOURCE")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("cli/build.rs"));
    let output = repo_root.join("build-script");

    let status = Command::new("rustc")
        .arg("--edition=2024")
        .arg(&source)
        .arg("-o")
        .arg(&output)
        .status()
        .expect("compile build script");
    assert!(status.success(), "rustc should compile cli/build.rs");

    output
}

fn workspace_root() -> PathBuf {
    if let Some(manifest_dir) = option_env!("CARGO_MANIFEST_DIR") {
        return PathBuf::from(manifest_dir)
            .parent()
            .expect("cli crate should live under the workspace root")
            .to_path_buf();
    }

    Path::new(file!())
        .canonicalize()
        .expect("canonicalize test file path")
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .expect("derive workspace root from test path")
        .to_path_buf()
}

struct TempRepo {
    path: PathBuf,
}

impl TempRepo {
    fn new(prefix: &str) -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{nanos}", std::process::id()));
        fs::create_dir_all(&path).expect("create temp repo");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempRepo {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
