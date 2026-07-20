#[cfg(feature = "admin-ui")]
use std::{
    env, fs, io,
    path::{Path, PathBuf},
    process::Command,
};

fn main() {
    println!("cargo:rerun-if-changed=admin/web/src");
    println!("cargo:rerun-if-changed=admin/web/public");
    println!("cargo:rerun-if-changed=admin/web/index.html");
    println!("cargo:rerun-if-changed=admin/web/package.json");
    println!("cargo:rerun-if-changed=admin/web/package-lock.json");
    println!("cargo:rerun-if-changed=admin/web/pnpm-lock.yaml");
    println!("cargo:rerun-if-changed=admin/web/tsconfig.json");
    println!("cargo:rerun-if-changed=admin/web/vite.config.ts");
    println!("cargo:rerun-if-changed=admin/web/dist");

    #[cfg(feature = "admin-ui")]
    prepare_admin_ui();
}

#[cfg(feature = "admin-ui")]
fn prepare_admin_ui() {
    let source_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("admin/web");
    let output_dir =
        PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is not set")).join("admin-web");
    let output_dist = output_dir.join("dist");

    if output_dir.exists() {
        fs::remove_dir_all(&output_dir).expect("failed to clear Admin UI build directory");
    }

    let source_dist = source_dir.join("dist");
    if source_dist.exists() {
        copy_tree(&source_dist, &output_dist).expect("failed to copy prebuilt Admin UI");
        return;
    }

    if !try_build_admin_ui(&source_dir, &output_dir) {
        println!(
            "cargo:warning=Admin UI not built (no JS toolchain or build failed); \
             embedding empty dist. Run `pnpm build` in admin/web for a UI-enabled binary."
        );
        fs::create_dir_all(output_dist).expect("failed to create empty Admin UI dist");
    }
}

/// Attempt to build the Admin UI in Cargo's output directory.
///
/// Build scripts must not modify the packaged source tree. Copying the web
/// sources first also keeps dependency installation and generated assets out of
/// the checkout during `cargo package` verification.
#[cfg(feature = "admin-ui")]
fn try_build_admin_ui(source_dir: &Path, output_dir: &Path) -> bool {
    fn available(command: &str) -> bool {
        Command::new(command)
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    let package_manager = if available("pnpm") {
        "pnpm"
    } else if available("npm") {
        "npm"
    } else {
        return false;
    };

    let build_dir = output_dir.join("build");
    if let Err(error) = copy_tree(source_dir, &build_dir) {
        println!("cargo:warning=Failed to copy Admin UI sources: {error}");
        return false;
    }

    println!("cargo:warning=Admin UI dist not found, building with {package_manager}...");
    println!("cargo:warning=Installing Admin UI dependencies...");

    let install_args: &[&str] = if package_manager == "pnpm" {
        &["install", "--frozen-lockfile"]
    } else {
        &["ci"]
    };
    let installed = Command::new(package_manager)
        .args(install_args)
        .current_dir(&build_dir)
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    if !installed {
        let _ = fs::remove_dir_all(build_dir);
        return false;
    }

    println!("cargo:warning=Building Admin UI...");
    let built = Command::new(package_manager)
        .args(["run", "build"])
        .current_dir(&build_dir)
        .status()
        .map(|status| status.success())
        .unwrap_or(false);

    let build_dist = build_dir.join("dist");
    let build_succeeded = built && build_dist.exists();
    if build_succeeded && let Err(error) = copy_tree(&build_dist, &output_dir.join("dist")) {
        println!("cargo:warning=Failed to copy built Admin UI: {error}");
        let _ = fs::remove_dir_all(build_dir);
        return false;
    }

    if let Err(error) = fs::remove_dir_all(build_dir) {
        println!("cargo:warning=Failed to clean Admin UI build directory: {error}");
    }

    if build_succeeded {
        println!("cargo:warning=Admin UI built successfully");
        true
    } else {
        false
    }
}

#[cfg(feature = "admin-ui")]
fn copy_tree(source: &Path, destination: &Path) -> io::Result<()> {
    fs::create_dir_all(destination)?;

    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let file_name = entry.file_name();
        let source_path = entry.path();
        let destination_path = destination.join(&file_name);

        if file_name == "node_modules"
            || file_name == "dist"
            || file_name == ".vite"
            || source_path
                .extension()
                .is_some_and(|ext| ext == "tsbuildinfo")
        {
            continue;
        }

        if file_type.is_dir() {
            copy_tree(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(source_path, destination_path)?;
        }
    }

    Ok(())
}
