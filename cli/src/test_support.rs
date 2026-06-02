use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use serde_json::Value;
use tempfile::TempDir;

pub const DEFAULT_ACTRIX_REPO: &str = "https://github.com/Actrium/actrix.git";
pub const DEFAULT_ACTRIX_ARTIFACT_REPO: &str = "Actrium/actrix";
pub const DEFAULT_ACTRIX_ARTIFACT_WORKFLOW: &str = "243491296";
pub const DEFAULT_ACTRIX_ARTIFACT_BRANCH: &str = "main";
pub const LOCAL_E2E_REALM_ID: u32 = 1001;
const KS_GRPC_PORT: u16 = 50052;

pub fn actr_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_actr") {
        return PathBuf::from(path);
    }

    let candidate = workspace_root()
        .join("target/debug")
        .join(format!("actr{}", std::env::consts::EXE_SUFFIX));
    if candidate.is_file() {
        return candidate;
    }

    panic!(
        "failed to locate actr binary: CARGO_BIN_EXE_actr is unset and fallback missing at {}",
        candidate.display()
    );
}

pub fn rust_e2e_target_dir() -> PathBuf {
    workspace_root().join("target/e2e-cache/rust-target")
}

fn local_swift_package_dir() -> PathBuf {
    workspace_root().join("bindings/swift")
}

fn local_swift_e2e_output_dir() -> PathBuf {
    local_swift_package_dir().join(".build/e2e")
}

pub fn run_actr(args: &[&str], cwd: &Path) -> Output {
    let mut cmd = Command::new(actr_bin());
    cmd.args(args).current_dir(cwd);
    cmd.env("CARGO_TARGET_DIR", rust_e2e_target_dir());

    // Make Swift template/e2e use the workspace-local Swift package directly.
    let local_swift = local_swift_package_dir();
    if local_swift.join("Package.swift").is_file() {
        cmd.env("ACTR_SWIFT_LOCAL_PATH", local_swift);
    }

    cmd.output().expect("failed to run actr binary")
}

pub fn assert_success(out: &Output, context: &str) {
    assert!(
        out.status.success(),
        "{context} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

pub fn ensure_success(out: &Output, context: &str) -> Result<()> {
    if out.status.success() {
        return Ok(());
    }
    bail!(
        "{context} failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

pub fn cargo_build(dir: &Path) {
    let out = Command::new("cargo")
        .args(["build"])
        .current_dir(dir)
        .env("CARGO_TARGET_DIR", rust_e2e_target_dir())
        .output()
        .expect("cargo build failed");
    assert_success(&out, &format!("cargo build in {}", dir.display()));
}

pub fn random_manufacturer() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!("test{nanos:08x}")
}

pub fn align_project_with_local_actrix(project_dir: &Path) -> Result<()> {
    rewrite_project_realm_id(project_dir, LOCAL_E2E_REALM_ID)
}

pub fn pin_echo_service_dependency_version(project_dir: &Path, manufacturer: &str) -> Result<()> {
    let actr_toml_path = project_dir.join("manifest.toml");
    let content = fs::read_to_string(&actr_toml_path)
        .with_context(|| format!("failed to read {}", actr_toml_path.display()))?;

    // Newer Rust echo template renders the dependency with manufacturer
    // already inlined as `EchoService = { actr_type = "<mfr>:EchoService:1.0.0" }`.
    // Detect that case and skip — no pinning needed.
    let already_rendered = format!("EchoService = {{ actr_type = \"{manufacturer}:");
    if content.contains(&already_rendered) {
        return Ok(());
    }

    // Two placeholder shapes appear across templates:
    // - Rust legacy: `echo-service = {}`
    // - TS / Swift current: `EchoService = {}`
    let placeholders: &[(&str, String)] = &[
        (
            "EchoService = {}",
            format!("EchoService = {{ actr_type = \"{manufacturer}:EchoService:1.0.0\" }}"),
        ),
        (
            "echo-service = {}",
            format!("echo-service = {{ actr_type = \"{manufacturer}:EchoService:1.0.0\" }}"),
        ),
    ];

    for (placeholder, replacement) in placeholders {
        if content.contains(placeholder) {
            let rewritten = content.replacen(placeholder, replacement, 1);
            fs::write(&actr_toml_path, rewritten)
                .with_context(|| format!("failed to write {}", actr_toml_path.display()))?;
            return Ok(());
        }
    }

    bail!(
        "failed to pin echo dependency version in {}: neither 'EchoService = {{}}' nor \
         'echo-service = {{}}' placeholders nor a pre-rendered EchoService entry were found",
        actr_toml_path.display()
    );
}

pub fn align_rust_project_with_workspace(project_dir: &Path) -> Result<()> {
    let cargo_toml_path = project_dir.join("Cargo.toml");
    let mut content = fs::read_to_string(&cargo_toml_path)
        .with_context(|| format!("failed to read {}", cargo_toml_path.display()))?;

    if content.contains("[patch.crates-io]") && content.contains("actr = { path =") {
        return Ok(());
    }

    let workspace = workspace_root();
    let mut patch = String::from("\n[patch.crates-io]\n");
    let crates = [
        ("actr", workspace.clone()),
        ("actr-protocol", workspace.join("core/protocol")),
        ("actr-framework", workspace.join("core/framework")),
        ("actr-hyper", workspace.join("core/hyper")),
        ("actr-runtime", workspace.join("core/runtime")),
        ("actr-config", workspace.join("core/config")),
        ("actr-service-compat", workspace.join("core/service-compat")),
        (
            "actr-runtime-mailbox",
            workspace.join("core/runtime-mailbox"),
        ),
    ];

    for (name, path) in crates {
        patch.push_str(&format!(
            "{name} = {{ path = \"{}\" }}\n",
            normalize_path_for_toml(&path)
        ));
    }

    content.push_str(&patch);
    fs::write(&cargo_toml_path, content)
        .with_context(|| format!("failed to write {}", cargo_toml_path.display()))?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct LocalSwiftPackageAssets {
    pub package_dir: PathBuf,
    pub bindings_path: String,
    pub xcframework_path: String,
}

pub fn ensure_local_swift_xcframework() -> Result<LocalSwiftPackageAssets> {
    static SWIFT_ASSETS: OnceLock<LocalSwiftPackageAssets> = OnceLock::new();
    if let Some(assets) = SWIFT_ASSETS.get() {
        return Ok(assets.clone());
    }

    let workspace = workspace_root();
    let package_dir = local_swift_package_dir();
    let output_dir = local_swift_e2e_output_dir();
    let bindings_path = ".build/e2e/ActrBindings".to_string();
    let xcframework_path = ".build/e2e/ActrFFI.xcframework".to_string();
    let output_path = package_dir.join(&xcframework_path);
    let bindings_dir = package_dir.join(&bindings_path);

    let cache_root = workspace.join("target/e2e-cache");
    fs::create_dir_all(&cache_root).context("failed to create e2e cache root")?;
    let _lock = DirLock::acquire(
        &cache_root.join("swift-xcframework-build.lock"),
        Duration::from_secs(900),
    )?;

    if output_dir.exists() {
        fs::remove_dir_all(&output_dir)
            .with_context(|| format!("failed to remove {}", output_dir.display()))?;
    }
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create {}", output_dir.display()))?;
    if output_path.exists() {
        fs::remove_dir_all(&output_path)
            .with_context(|| format!("failed to remove {}", output_path.display()))?;
    }
    let headers_dir = bindings_dir.join("include");
    fs::create_dir_all(&headers_dir)
        .with_context(|| format!("failed to create {}", headers_dir.display()))?;
    run_checked(
        {
            let mut cmd = Command::new("./build-xcframework.sh");
            cmd.current_dir(&package_dir)
                .env("ACTR_BINDINGS_PATH", &bindings_path)
                .env("ACTR_BINARY_PATH", &xcframework_path);
            cmd
        },
        "build local swift ffi xcframework",
    )?;

    if !bindings_dir.join("Actr.swift").is_file() {
        bail!(
            "local swift bindings not found at {}",
            bindings_dir.join("Actr.swift").display()
        );
    }
    if !output_path.exists() {
        bail!(
            "local swift ffi xcframework not found at {}",
            output_path.display()
        );
    }

    let assets = LocalSwiftPackageAssets {
        package_dir,
        bindings_path,
        xcframework_path,
    };
    let _ = SWIFT_ASSETS.set(assets.clone());
    Ok(assets)
}

pub struct LoggedProcess {
    child: Child,
    logs: Arc<Mutex<Vec<String>>>,
}

impl LoggedProcess {
    pub fn spawn(mut cmd: Command, name: &str) -> Result<Self> {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn process '{name}'"))?;
        let logs = Arc::new(Mutex::new(Vec::new()));
        drain_stream(child.stdout.take(), Arc::clone(&logs), name, "stdout");
        drain_stream(child.stderr.take(), Arc::clone(&logs), name, "stderr");
        Ok(Self { child, logs })
    }

    pub fn wait_for_log(&mut self, needle: &str, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self
                .logs
                .lock()
                .unwrap()
                .iter()
                .any(|line| line.contains(needle))
            {
                return true;
            }

            if matches!(self.child.try_wait(), Ok(Some(_))) {
                return false;
            }
            if Instant::now() > deadline {
                return false;
            }
            thread::sleep(Duration::from_millis(200));
        }
    }

    pub fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
        self.child.try_wait()
    }

    pub fn logs(&self) -> String {
        self.logs.lock().unwrap().join("\n")
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for LoggedProcess {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

/// Mock signaling server for e2e tests (replaces LocalActrix).
///
/// Runs `MockSignalingServer` on a background tokio runtime and exposes
/// a synchronous API compatible with the test harness.
pub struct MockSignaling {
    pub signaling_ws_url: String,
    _runtime: tokio::runtime::Runtime,
    _server: std::sync::Arc<tokio::sync::Mutex<actr_mock_actrix::MockActrixServer>>,
}

impl MockSignaling {
    pub fn start() -> Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("failed to build tokio runtime for mock signaling")?;

        let server = rt
            .block_on(actr_mock_actrix::MockActrixServer::start())
            .context("failed to start mock signaling server")?;

        let ws_url = server.url();
        let server = std::sync::Arc::new(tokio::sync::Mutex::new(server));

        Ok(Self {
            signaling_ws_url: ws_url,
            _runtime: rt,
            _server: server,
        })
    }
}

pub struct LocalActrix {
    pub state_dir: TempDir,
    process: LoggedProcess,
    actrix_bin: PathBuf,
    config_path: PathBuf,
    http_port: u16,
    pub http_base_url: String,
    pub signaling_ws_url: String,
}

impl LocalActrix {
    pub fn start() -> Result<Self> {
        ensure_ks_port_available()?;
        let actrix_bin = ensure_actrix_binary()?;

        let state_dir = TempDir::new().context("failed to create actrix state dir")?;
        let http_port = free_port().context("failed to allocate HTTP port")?;
        let ice_port = free_port().context("failed to allocate ICE port")?;
        let config_path = state_dir.path().join("actrix-e2e.toml");
        write_actrix_config(&config_path, state_dir.path(), http_port, ice_port)?;

        let mut cmd = Command::new(&actrix_bin);
        cmd.arg("--config")
            .arg(&config_path)
            .current_dir(state_dir.path());
        let mut process = LoggedProcess::spawn(cmd, "actrix")?;

        let health_path = "/signaling/health";
        if !wait_for_http_ok(
            &mut process,
            http_port,
            health_path,
            Duration::from_secs(120),
        ) {
            let logs = process.logs();
            bail!(
                "actrix did not become healthy within timeout (http://127.0.0.1:{http_port}{health_path})\n{logs}"
            );
        }

        ensure_realm_exists(&state_dir.path().join("sqlite"), LOCAL_E2E_REALM_ID)?;

        Ok(Self {
            state_dir,
            process,
            actrix_bin,
            config_path,
            http_port,
            http_base_url: format!("http://127.0.0.1:{http_port}"),
            signaling_ws_url: format!("ws://127.0.0.1:{http_port}/signaling/ws"),
        })
    }

    /// Kill the actrix process without cleaning up state (SQLite remains).
    pub fn kill(&mut self) {
        self.process.kill();
    }

    /// Restart actrix using the same state directory and ports.
    /// The SQLite database is preserved so service registry can be restored.
    pub fn restart(&mut self) -> Result<()> {
        self.kill();
        // Brief pause to let the OS release the port
        thread::sleep(Duration::from_millis(500));

        let mut cmd = Command::new(&self.actrix_bin);
        cmd.arg("--config")
            .arg(&self.config_path)
            .current_dir(self.state_dir.path());
        let mut process = LoggedProcess::spawn(cmd, "actrix")?;

        let health_path = "/signaling/health";
        if !wait_for_http_ok(
            &mut process,
            self.http_port,
            health_path,
            Duration::from_secs(120),
        ) {
            let logs = process.logs();
            bail!("actrix did not become healthy after restart\n{logs}");
        }

        self.process = process;
        Ok(())
    }

    pub fn logs(&self) -> String {
        self.process.logs()
    }

    pub fn wait_for_log(&mut self, needle: &str, timeout: Duration) -> bool {
        self.process.wait_for_log(needle, timeout)
    }
}

pub struct LocalRustEchoService {
    _workspace: TempDir,
    process: LoggedProcess,
}

impl LocalRustEchoService {
    pub fn start(signaling_ws_url: &str) -> Result<Self> {
        let workspace = TempDir::new().context("failed to create rust echo e2e workspace")?;
        let project_name = "registry-echo-service";

        let init_out = run_actr(
            &[
                "init",
                "-l",
                "rust",
                "--template",
                "echo",
                "--role",
                "service",
                "--manufacturer",
                "acme",
                "--signaling",
                signaling_ws_url,
                project_name,
            ],
            workspace.path(),
        );
        ensure_success(&init_out, "actr init rust service")?;

        let service_dir = workspace.path().join(project_name);
        align_project_with_local_actrix(&service_dir)?;
        rewrite_manifest_version(&service_dir, "0.1.0")?;
        align_rust_project_with_workspace(&service_dir)?;
        let install_out = run_actr(&["deps", "install"], &service_dir);
        ensure_success(&install_out, "actr deps install rust service")?;
        let gen_out = run_actr(&["gen", "-l", "rust"], &service_dir);
        ensure_success(&gen_out, "actr gen rust service")?;
        let build_out = run_actr(&["build"], &service_dir);
        ensure_success(&build_out, "actr build rust service")?;
        let package_path = find_built_package(&service_dir)?;
        publish_local_package(&package_path, signaling_ws_url)?;

        let runtime_config_path = write_package_runtime_config(&service_dir, signaling_ws_url)?;

        let mut cmd = Command::new(actr_bin());
        cmd.args(["run", "-c", runtime_config_path.to_string_lossy().as_ref()])
            .current_dir(&service_dir)
            .env("CARGO_TARGET_DIR", rust_e2e_target_dir());
        let mut process = LoggedProcess::spawn(cmd, "rust-echo-service")?;
        if !process.wait_for_log("✅ ActrNode started", Duration::from_secs(180)) {
            let logs = process.logs();
            bail!("rust echo service did not register in time\n{logs}");
        }

        Ok(Self {
            _workspace: workspace,
            process,
        })
    }

    pub fn logs(&self) -> String {
        self.process.logs()
    }
}

fn rewrite_project_realm_id(project_dir: &Path, realm_id: u32) -> Result<()> {
    // realm_id placement varies by template:
    // - Rust echo (split layout): runtime config goes to `actr.toml`; the
    //   service subproject has no deployment file at all (services are
    //   realm-neutral packages) so we skip silently.
    // - TS / Swift / Kotlin echo (single-file layout): realm_id still lives
    //   inline in `manifest.toml`.
    // Visit every candidate, rewrite where the line exists, no-op otherwise.
    let candidates = ["actr.toml", "manifest.toml"];
    let mut visited_any = false;
    let mut rewrote_any = false;
    for filename in candidates {
        let path = project_dir.join(filename);
        if !path.exists() {
            continue;
        }
        visited_any = true;
        if rewrite_realm_id_in_file(&path, realm_id)? {
            rewrote_any = true;
        }
    }
    if !visited_any {
        bail!(
            "no actr.toml or manifest.toml under {}",
            project_dir.display()
        );
    }
    let _ = rewrote_any;
    Ok(())
}

/// Returns `true` when a `realm_id =` line was found and rewritten;
/// `false` when the file simply has no realm_id (e.g. a service-only
/// `manifest.toml` whose deployment lives elsewhere).
fn rewrite_realm_id_in_file(path: &Path, realm_id: u32) -> Result<bool> {
    let content =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;

    let mut replaced = false;
    let mut rewritten = String::with_capacity(content.len() + 32);
    for line in content.lines() {
        if line.trim_start().starts_with("realm_id =") {
            let prefix = line.split("realm_id").next().unwrap_or_default();
            rewritten.push_str(prefix);
            rewritten.push_str("realm_id = ");
            rewritten.push_str(&realm_id.to_string());
            rewritten.push('\n');
            replaced = true;
        } else {
            rewritten.push_str(line);
            rewritten.push('\n');
        }
    }

    if !replaced {
        return Ok(false);
    }

    if !content.ends_with('\n') {
        rewritten.pop();
    }

    fs::write(path, rewritten).with_context(|| format!("failed to write {}", path.display()))?;
    Ok(true)
}

fn ensure_realm_exists(sqlite_dir: &Path, realm_id: u32) -> Result<()> {
    let db_path = sqlite_dir.join("actrix.db");
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        match Connection::open(&db_path) {
            Ok(conn) => {
                conn.busy_timeout(Duration::from_secs(3))
                    .context("failed to set sqlite busy timeout")?;
                conn.execute_batch(
                    "CREATE TABLE IF NOT EXISTS realm (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'Active',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        expires_at INTEGER,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER,
                        secret_current TEXT NOT NULL DEFAULT '',
                        secret_previous_hash TEXT,
                        secret_previous_valid_until INTEGER
                    );
                     INSERT OR IGNORE INTO sqlite_sequence(name, seq) VALUES('realm', 33554431);",
                )?;
                conn.execute(
                    "INSERT OR IGNORE INTO realm (id, name, status, enabled, created_at, secret_current)
                     VALUES (?1, 'e2e-realm', 'Active', 1, strftime('%s','now'), ?2)",
                    rusqlite::params![realm_id, ""],
                )
                .context("failed to ensure local e2e realm exists")?;
                return Ok(());
            }
            Err(err) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(200));
                let _ = err;
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to open actrix db {}", db_path.display()));
            }
        }
    }
}

fn write_package_runtime_config(project_dir: &Path, signaling_ws_url: &str) -> Result<PathBuf> {
    let cli_config = crate::config::resolver::resolve_effective_cli_config()?;
    let key_path =
        crate::commands::package_build::resolve_key_path(None, cli_config.mfr.keychain.as_deref())?;
    let package_path = find_built_package(project_dir)?;

    let runtime_root = project_dir.join(".actr-e2e-runtime");
    let data_dir = runtime_root.join("hyper");
    fs::create_dir_all(&data_dir)
        .with_context(|| format!("failed to create {}", data_dir.display()))?;
    let ais_url = signaling_ws_url
        .trim_end_matches("/signaling/ws")
        .trim_end_matches("/signaling")
        .trim_end_matches("/ws");
    let ais_url = if let Some(rest) = ais_url.strip_prefix("ws://") {
        format!("http://{rest}/ais")
    } else if let Some(rest) = ais_url.strip_prefix("wss://") {
        format!("https://{rest}/ais")
    } else {
        format!("{ais_url}/ais")
    };

    let runtime_config = format!(
        "edition = 1\n\n[signaling]\nurl = \"{signaling_ws_url}\"\n\n[ais_endpoint]\nurl = \"{ais_url}\"\n\n[deployment]\nrealm_id = {LOCAL_E2E_REALM_ID}\n\n[package]\npath = \"{}\"\n\n[hyper]\ndata_dir = \"{}\"\n\n[[trust]]\nkind = \"static\"\npubkey_file = \"{}\"\n",
        normalize_path_for_toml(&package_path),
        normalize_path_for_toml(&data_dir),
        normalize_path_for_toml(&key_path),
    );
    let runtime_config_path = project_dir.join("actr.e2e.toml");
    fs::write(&runtime_config_path, runtime_config)
        .with_context(|| format!("failed to write {}", runtime_config_path.display()))?;
    Ok(runtime_config_path)
}

fn rewrite_manifest_version(project_dir: &Path, version: &str) -> Result<()> {
    let manifest_path = project_dir.join("manifest.toml");
    let content = fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    let rewritten = content.replacen(
        "version = \"1.0.0\"",
        &format!("version = \"{version}\""),
        1,
    );
    fs::write(&manifest_path, rewritten)
        .with_context(|| format!("failed to write {}", manifest_path.display()))?;
    Ok(())
}

fn find_built_package(project_dir: &Path) -> Result<PathBuf> {
    fs::read_dir(project_dir.join("dist"))
        .with_context(|| format!("failed to read {}", project_dir.join("dist").display()))?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .find(|path| path.extension().and_then(|ext| ext.to_str()) == Some("actr"))
        .context("built rust service package missing .actr artifact under dist/")
}

fn publish_local_package(package_path: &Path, signaling_ws_url: &str) -> Result<()> {
    let cli_config = crate::config::resolver::resolve_effective_cli_config()?;
    let key_path =
        crate::commands::package_build::resolve_key_path(None, cli_config.mfr.keychain.as_deref())?;
    let ais_url = signaling_ws_url
        .trim_end_matches("/signaling/ws")
        .trim_end_matches("/signaling")
        .trim_end_matches("/ws");
    let ais_url = if let Some(rest) = ais_url.strip_prefix("ws://") {
        format!("http://{rest}/ais")
    } else if let Some(rest) = ais_url.strip_prefix("wss://") {
        format!("https://{rest}/ais")
    } else {
        format!("{ais_url}/ais")
    };

    let out = Command::new(actr_bin())
        .args([
            "registry",
            "publish",
            "--package",
            package_path.to_string_lossy().as_ref(),
            "--keychain",
            key_path.to_string_lossy().as_ref(),
            "--endpoint",
            &ais_url,
        ])
        .output()
        .context("failed to invoke actr registry publish")?;
    ensure_success(&out, "actr registry publish")?;
    Ok(())
}

fn drain_stream(
    stream: Option<impl Read + Send + 'static>,
    logs: Arc<Mutex<Vec<String>>>,
    name: &str,
    stream_name: &str,
) {
    if let Some(stream) = stream {
        let tag = format!("[{name}:{stream_name}]");
        thread::spawn(move || {
            for line in BufReader::new(stream).lines().map_while(Result::ok) {
                logs.lock().unwrap().push(format!("{tag} {line}"));
            }
        });
    }
}

fn ensure_ks_port_available() -> Result<()> {
    let probe = TcpListener::bind(("127.0.0.1", KS_GRPC_PORT))
        .with_context(|| format!("port {KS_GRPC_PORT} is already in use"))?;
    drop(probe);
    Ok(())
}

fn wait_for_http_ok(process: &mut LoggedProcess, port: u16, path: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if http_get_ok(port, path) {
            return true;
        }
        if matches!(process.try_wait(), Ok(Some(_))) {
            return false;
        }
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(250));
    }
}

fn http_get_ok(port: u16, path: &str) -> bool {
    let addr = format!("127.0.0.1:{port}");
    let timeout = Duration::from_secs(1);
    let Ok(mut stream) =
        TcpStream::connect_timeout(&addr.parse().expect("valid socket addr"), timeout)
    else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(timeout));
    let _ = stream.set_write_timeout(Some(timeout));

    let request = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    if stream.write_all(request.as_bytes()).is_err() {
        return false;
    }

    let mut response = String::new();
    if stream.read_to_string(&mut response).is_err() {
        return false;
    }

    let Some(status_line) = response.lines().next() else {
        return false;
    };
    status_line.contains(" 200 ")
}

fn free_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").context("failed to bind ephemeral port")?;
    let port = listener
        .local_addr()
        .context("failed to read local address")?
        .port();
    drop(listener);
    Ok(port)
}

fn ensure_actrix_binary() -> Result<PathBuf> {
    static ACTRIX_BIN: OnceLock<PathBuf> = OnceLock::new();
    if let Some(path) = ACTRIX_BIN.get() {
        return Ok(path.clone());
    }

    if let Ok(path) = std::env::var("ACTR_E2E_ACTRIX_BIN") {
        let binary_path = PathBuf::from(path);
        if binary_path.is_file() {
            let _ = ACTRIX_BIN.set(binary_path.clone());
            return Ok(binary_path);
        }
        bail!(
            "ACTR_E2E_ACTRIX_BIN points to a missing file: {}",
            binary_path.display()
        );
    }

    let repo =
        std::env::var("ACTR_E2E_ACTRIX_REPO").unwrap_or_else(|_| DEFAULT_ACTRIX_REPO.to_string());
    let cache_root = workspace_root().join("target/e2e-cache");
    fs::create_dir_all(&cache_root).context("failed to create e2e cache root")?;
    let _lock = DirLock::acquire(
        &cache_root.join("actrix-build.lock"),
        Duration::from_secs(600),
    )?;

    let latest_run = if std::env::var("ACTR_E2E_ACTRIX_REV").is_err() && artifact_download_enabled()
    {
        Some(latest_successful_actrix_run()?)
    } else {
        None
    };

    if artifact_download_enabled() {
        if let Some(binary_path) =
            try_ensure_actrix_artifact_binary(&cache_root, latest_run.as_ref())?
        {
            let _ = ACTRIX_BIN.set(binary_path.clone());
            return Ok(binary_path);
        }
    }

    let rev = resolve_actrix_source_rev(&repo, latest_run.as_ref())?;
    let checkout_dir = cache_root.join("actrix-checkout");
    ensure_actrix_checkout(&checkout_dir, &repo, &rev)?;

    let target_dir = cache_root.join("actrix-target");
    let mut build_cmd = Command::new("cargo");
    build_cmd
        .arg("build")
        .arg("--release")
        .arg("--bin")
        .arg("actrix")
        .current_dir(&checkout_dir)
        .env("CARGO_TARGET_DIR", &target_dir);
    run_checked(build_cmd, "cargo build --release --bin actrix")?;

    let binary_path = target_dir.join("release/actrix");
    if !binary_path.exists() {
        bail!("actrix binary not found at {}", binary_path.display());
    }

    let _ = ACTRIX_BIN.set(binary_path.clone());
    Ok(binary_path)
}

#[derive(Clone, Debug)]
struct ActrixRunInfo {
    run_id: String,
    head_sha: String,
}

fn artifact_download_enabled() -> bool {
    std::env::var("ACTR_E2E_ACTRIX_ARTIFACT")
        .map(|value| value != "0" && !value.eq_ignore_ascii_case("false"))
        .unwrap_or(true)
}

fn try_ensure_actrix_artifact_binary(
    cache_root: &Path,
    latest_run: Option<&ActrixRunInfo>,
) -> Result<Option<PathBuf>> {
    let Some(artifact_name) = current_actrix_artifact_name() else {
        return Ok(None);
    };
    let Some(latest_run) = latest_run else {
        return Ok(None);
    };

    let artifact_repo = std::env::var("ACTR_E2E_ACTRIX_ARTIFACT_REPO")
        .unwrap_or_else(|_| DEFAULT_ACTRIX_ARTIFACT_REPO.to_string());

    let artifact_dir = cache_root
        .join("actrix-artifacts")
        .join(&latest_run.run_id)
        .join(artifact_name);
    let binary_path = artifact_dir.join("actrix");
    if binary_path.is_file() {
        ensure_executable(&binary_path)?;
        return Ok(Some(binary_path));
    }

    fs::create_dir_all(&artifact_dir)
        .with_context(|| format!("failed to create {}", artifact_dir.display()))?;

    let download_output = Command::new("gh")
        .args([
            "run",
            "download",
            &latest_run.run_id,
            "-R",
            &artifact_repo,
            "-n",
            artifact_name,
        ])
        .current_dir(&artifact_dir)
        .output()
        .context("failed to invoke gh run download for actrix artifact")?;

    if !download_output.status.success() {
        eprintln!(
            "actrix artifact download failed, falling back to source build:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&download_output.stdout),
            String::from_utf8_lossy(&download_output.stderr)
        );
        return Ok(None);
    }

    if !binary_path.is_file() {
        eprintln!(
            "actrix artifact downloaded but binary missing at {}, falling back to source build",
            binary_path.display()
        );
        return Ok(None);
    }

    ensure_executable(&binary_path)?;
    Ok(Some(binary_path))
}

fn current_actrix_artifact_name() -> Option<&'static str> {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => Some("actrix-linux-x86_64"),
        ("macos", "aarch64") => Some("actrix-macos-arm64"),
        _ => None,
    }
}

fn latest_successful_actrix_run() -> Result<ActrixRunInfo> {
    let artifact_repo = std::env::var("ACTR_E2E_ACTRIX_ARTIFACT_REPO")
        .unwrap_or_else(|_| DEFAULT_ACTRIX_ARTIFACT_REPO.to_string());
    let workflow = std::env::var("ACTR_E2E_ACTRIX_ARTIFACT_WORKFLOW")
        .unwrap_or_else(|_| DEFAULT_ACTRIX_ARTIFACT_WORKFLOW.to_string());
    let branch = std::env::var("ACTR_E2E_ACTRIX_ARTIFACT_BRANCH")
        .unwrap_or_else(|_| DEFAULT_ACTRIX_ARTIFACT_BRANCH.to_string());
    let route = format!(
        "repos/{artifact_repo}/actions/workflows/{workflow}/runs?branch={branch}&status=success&per_page=1"
    );
    let output = Command::new("gh")
        .args(["api", &route])
        .output()
        .context("failed to invoke gh api for latest actrix workflow run")?;

    if !output.status.success() {
        bail!(
            "failed to resolve latest actrix workflow run from GitHub:\nstdout: {}\nstderr: {}\nset ACTR_E2E_ACTRIX_REV or ACTR_E2E_ACTRIX_BIN to override",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let payload: Value = serde_json::from_slice(&output.stdout)
        .context("failed to parse latest actrix workflow run payload")?;
    let run = payload
        .get("workflow_runs")
        .and_then(Value::as_array)
        .and_then(|runs| runs.first())
        .context("latest actrix workflow run payload did not include a successful run")?;
    let run_id = run
        .get("id")
        .and_then(Value::as_u64)
        .context("latest actrix workflow run payload did not include a successful run id")?;
    let head_sha = run
        .get("head_sha")
        .and_then(Value::as_str)
        .context("latest actrix workflow run payload did not include head_sha")?;

    Ok(ActrixRunInfo {
        run_id: run_id.to_string(),
        head_sha: head_sha.to_string(),
    })
}

fn resolve_actrix_source_rev(repo: &str, latest_run: Option<&ActrixRunInfo>) -> Result<String> {
    if let Ok(rev) = std::env::var("ACTR_E2E_ACTRIX_REV") {
        return Ok(rev);
    }

    if let Some(latest_run) = latest_run {
        return Ok(latest_run.head_sha.clone());
    }

    let route = format!(
        "refs/heads/{}",
        std::env::var("ACTR_E2E_ACTRIX_ARTIFACT_BRANCH")
            .unwrap_or_else(|_| DEFAULT_ACTRIX_ARTIFACT_BRANCH.to_string())
    );
    let output = Command::new("git")
        .args(["ls-remote", repo, &route])
        .output()
        .context("failed to invoke git ls-remote for actrix revision")?;

    if !output.status.success() {
        bail!(
            "failed to resolve latest actrix revision:\nstdout: {}\nstderr: {}\nset ACTR_E2E_ACTRIX_REV or ACTR_E2E_ACTRIX_BIN to override",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let rev = stdout
        .split_whitespace()
        .next()
        .context("git ls-remote did not return a revision for actrix")?;

    Ok(rev.to_string())
}

fn ensure_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = fs::metadata(path)
            .with_context(|| format!("failed to read metadata for {}", path.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .with_context(|| format!("failed to mark {} executable", path.display()))?;
    }

    Ok(())
}

fn ensure_actrix_checkout(checkout_dir: &Path, repo: &str, rev: &str) -> Result<()> {
    if !checkout_dir.join(".git").exists() {
        clone_actrix_repo(checkout_dir, repo)?;
    } else {
        let current_remote = Command::new("git")
            .args(["config", "--get", "remote.origin.url"])
            .current_dir(checkout_dir)
            .output()
            .ok()
            .and_then(|out| {
                if out.status.success() {
                    Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
                } else {
                    None
                }
            });
        if current_remote.as_deref() != Some(repo) {
            fs::remove_dir_all(checkout_dir).with_context(|| {
                format!(
                    "failed to remove stale actrix checkout {}",
                    checkout_dir.display()
                )
            })?;
            clone_actrix_repo(checkout_dir, repo)?;
        }
    }

    run_checked(
        {
            let mut cmd = Command::new("git");
            cmd.arg("fetch")
                .arg("--depth")
                .arg("1")
                .arg("origin")
                .arg(rev)
                .current_dir(checkout_dir);
            cmd
        },
        "git fetch actrix revision",
    )?;
    run_checked(
        {
            let mut cmd = Command::new("git");
            cmd.arg("checkout")
                .arg("--detach")
                .arg(rev)
                .current_dir(checkout_dir);
            cmd
        },
        "git checkout actrix revision",
    )?;
    Ok(())
}

fn clone_actrix_repo(checkout_dir: &Path, repo: &str) -> Result<()> {
    if let Some(parent) = checkout_dir.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create parent dir for {}", checkout_dir.display())
        })?;
    }
    run_checked(
        {
            let mut cmd = Command::new("git");
            cmd.arg("clone")
                .arg("--filter=blob:none")
                .arg(repo)
                .arg(checkout_dir)
                .current_dir(
                    checkout_dir
                        .parent()
                        .expect("actrix checkout dir should have parent"),
                );
            cmd
        },
        "git clone actrix",
    )?;
    Ok(())
}

fn run_checked(mut cmd: Command, context_name: &str) -> Result<Output> {
    let output = cmd
        .output()
        .with_context(|| format!("{context_name}: failed to execute"))?;
    if output.status.success() {
        return Ok(output);
    }

    bail!(
        "{} failed:\nstdout: {}\nstderr: {}",
        context_name,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn write_actrix_config(
    config_path: &Path,
    state_dir: &Path,
    http_port: u16,
    ice_port: u16,
) -> Result<()> {
    let sqlite_dir = state_dir.join("sqlite");
    let log_dir = state_dir.join("logs");
    fs::create_dir_all(&sqlite_dir).context("failed to create sqlite dir")?;
    fs::create_dir_all(&log_dir).context("failed to create log dir")?;

    let sqlite_path = normalize_path_for_toml(&sqlite_dir);

    let config = format!(
        r#"enable = 25
name = "actrix-e2e"
env = "dev"
sqlite_path = "{sqlite_path}"
location_tag = "local,e2e,default"
actrix_shared_key = "actrix-e2e-shared-key-0123456789abcdef"

[control]
head = "admin_ui"

[control.admin_ui]
password = "e2e-test-password"

[bind.http]
domain_name = "127.0.0.1"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {http_port}

[bind.ice]
domain_name = "127.0.0.1"
ip = "127.0.0.1"
port = {ice_port}
advertised_ip = "127.0.0.1"
advertised_port = {ice_port}

[turn]
advertised_ip = "127.0.0.1"
advertised_port = {ice_port}
relay_port_range = "49152-49200"
realm = "local.actrix"

[services.ks]

[services.signer]

[services.ais]

[services.signaling]

[services.signaling.server]
ws_path = "/signaling"
"#
    );

    fs::write(config_path, config)
        .with_context(|| format!("failed to write actrix config to {}", config_path.display()))?;
    Ok(())
}

fn normalize_path_for_toml(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("actr-cli should live under workspace root")
        .to_path_buf()
}

struct DirLock {
    path: PathBuf,
}

impl DirLock {
    fn acquire(path: &Path, timeout: Duration) -> Result<Self> {
        let deadline = Instant::now() + timeout;
        loop {
            match fs::create_dir(path) {
                Ok(()) => {
                    return Ok(Self {
                        path: path.to_path_buf(),
                    });
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if Instant::now() > deadline {
                        bail!("timed out waiting for lock directory {}", path.display());
                    }
                    thread::sleep(Duration::from_millis(250));
                }
                Err(err) => return Err(err).context("failed to acquire lock directory"),
            }
        }
    }
}

impl Drop for DirLock {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
