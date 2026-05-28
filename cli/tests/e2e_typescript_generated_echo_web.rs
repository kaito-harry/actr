//! End-to-end test for actr-web calling a generated TypeScript EchoService workload.
//!
//! Run with:
//! `cargo test -p actr-cli --test e2e_typescript_generated_echo_web -- --ignored --test-threads=1`

use std::ffi::{OsStr, OsString};
use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use actr_cli::test_support::{LOCAL_E2E_REALM_ID, LoggedProcess, MockSignaling, assert_success};
use tempfile::TempDir;

const SERVICE_PACKAGE: &str = "acme-EchoService-0.1.0-wasm32-wasip2.actr";
const CLIENT_PACKAGE: &str = "acme-echo-client-app-0.1.0-wasm32-wasip2.actr";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("actr-cli should live under the workspace root")
        .to_path_buf()
}

fn typescript_echo_example_dir() -> PathBuf {
    workspace_root().join("examples/typescript/echo-workload")
}

fn web_echo_dir() -> PathBuf {
    workspace_root().join("bindings/web/examples/echo")
}

fn actr_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("ACTR_E2E_ACTR_BIN") {
        return PathBuf::from(path);
    }
    actr_cli::test_support::actr_bin()
}

fn e2e_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn run<I, S>(program: &str, args: I, cwd: &Path) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(program)
        .args(args)
        .current_dir(cwd)
        .output()
        .unwrap_or_else(|error| panic!("failed to run {program} in {}: {error}", cwd.display()))
}

fn run_with_env<I, S>(program: &str, args: I, cwd: &Path, envs: &[(&str, OsString)]) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut command = Command::new(program);
    command.args(args).current_dir(cwd);
    for (key, value) in envs {
        command.env(key, value);
    }
    command
        .output()
        .unwrap_or_else(|error| panic!("failed to run {program} in {}: {error}", cwd.display()))
}

fn run_actr<I, S>(args: I, cwd: &Path) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    Command::new(actr_bin())
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("failed to run actr binary")
}

fn e2e_rust_log() -> OsString {
    std::env::var_os("ACTR_E2E_RUST_LOG")
        .or_else(|| std::env::var_os("RUST_LOG"))
        .unwrap_or_else(|| OsString::from("info"))
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("failed to bind ephemeral port")
        .local_addr()
        .expect("failed to read ephemeral port")
        .port()
}

fn mock_http_url(mock: &MockSignaling) -> String {
    mock.signaling_ws_url
        .replace("ws://", "http://")
        .replace("wss://", "https://")
        .trim_end_matches("/signaling/ws")
        .trim_end_matches('/')
        .to_string()
}

fn read_public_key(key_path: &Path) -> String {
    let raw = fs::read_to_string(key_path).expect("failed to read signing key");
    let value: serde_json::Value = serde_json::from_str(&raw).expect("failed to parse key JSON");
    value
        .get("public_key")
        .and_then(serde_json::Value::as_str)
        .expect("key JSON missing public_key")
        .to_string()
}

fn ensure_typescript_workload_package_built() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let dir = workspace_root().join("bindings/typescript/actr-workload");
        assert_success(
            &run("npm", ["install"], &dir),
            "npm install (actr-workload)",
        );
        assert_success(
            &run("npm", ["run", "build"], &dir),
            "npm run build (actr-workload)",
        );
    });
}

fn build_generated_typescript_service(release_dir: &Path, key_path: &Path) -> PathBuf {
    ensure_typescript_workload_package_built();
    let example_dir = typescript_echo_example_dir();
    assert!(
        example_dir.exists(),
        "generated TypeScript EchoService workload example is missing at {}",
        example_dir.display()
    );

    assert_success(
        &run("npm", ["install"], &example_dir),
        "npm install (generated TS workload)",
    );
    assert_success(
        &run("npm", ["run", "build"], &example_dir),
        "npm run build (generated TS workload)",
    );
    assert_success(
        &run("npm", ["run", "componentize"], &example_dir),
        "npm run componentize (generated TS workload)",
    );

    let output = release_dir.join(SERVICE_PACKAGE);
    assert_success(
        &run_actr(
            [
                "build",
                "--no-compile",
                "--manifest-path",
                "manifest.toml",
                "--key",
                key_path.to_str().expect("key path should be UTF-8"),
                "--output",
                output.to_str().expect("output path should be UTF-8"),
            ],
            &example_dir,
        ),
        "actr build generated TS workload package",
    );
    output
}

fn build_web_client_package(release_dir: &Path, key_path: &Path) -> PathBuf {
    let client_guest = web_echo_dir().join("client-guest");
    let component_ld = which("wasm-component-ld")
        .unwrap_or_else(|| panic!("wasm-component-ld is required for this e2e"));

    let rustflags = OsString::from(format!("-Clinker={}", component_ld.display()));
    assert_success(
        &run_with_env(
            "cargo",
            ["build", "--target", "wasm32-wasip2", "--release"],
            &client_guest,
            &[("RUSTFLAGS", rustflags)],
        ),
        "cargo build web client guest component",
    );

    assert_success(
        &run_with_env(
            "wasm-pack",
            [
                "build",
                "--target",
                "no-modules",
                "--release",
                "--out-dir",
                "pkg",
                "--",
                "--no-default-features",
                "--features",
                "web",
            ],
            &client_guest,
            &[
                ("RUSTFLAGS", OsString::new()),
                ("CARGO_ENCODED_RUSTFLAGS", OsString::new()),
            ],
        ),
        "wasm-pack build web client guest",
    );

    let output = release_dir.join(CLIENT_PACKAGE);
    assert_success(
        &run_actr(
            [
                "build",
                "--no-compile",
                "--manifest-path",
                "manifest.toml",
                "--key",
                key_path.to_str().expect("key path should be UTF-8"),
                "--output",
                output.to_str().expect("output path should be UTF-8"),
            ],
            &client_guest,
        ),
        "actr build web client package",
    );

    let wbg_dir = release_dir.join("acme-echo-client-app-0.1.0-wasm32-wasip2.wbg");
    if wbg_dir.exists() {
        fs::remove_dir_all(&wbg_dir).expect("failed to clear old WBG dir");
    }
    fs::create_dir_all(&wbg_dir).expect("failed to create WBG dir");
    fs::copy(
        client_guest.join("pkg/echo_client_guest_web.js"),
        wbg_dir.join("guest.js"),
    )
    .expect("failed to copy WBG JS");
    fs::copy(
        client_guest.join("pkg/echo_client_guest_web_bg.wasm"),
        wbg_dir.join("guest_bg.wasm"),
    )
    .expect("failed to copy WBG wasm");

    output
}

fn which(binary: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for entry in std::env::split_paths(&path) {
        let candidate = entry.join(binary);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

fn write_runtime_config(
    path: &Path,
    package_path: &Path,
    mock: &MockSignaling,
    public_key: &str,
    acl_type: &str,
    web_port: Option<u16>,
) {
    let web = web_port
        .map(|port| {
            format!(
                r#"
[web]
port = {port}
host = "127.0.0.1"
"#
            )
        })
        .unwrap_or_default();

    let content = format!(
        r#"edition = 1

[package]
path = "{}"

[signaling]
url = "{}"

[ais_endpoint]
url = "{}/ais"

[deployment]
realm_id = {}

[discovery]
visible = true

[observability]
filter_level = "info"
tracing_enabled = false

[webrtc]
force_relay = false
stun_urls = ["stun:localhost:3478"]
turn_urls = ["turn:localhost:3478"]

[acl]

[[acl.rules]]
permission = "allow"
type = "{acl_type}"

[[trust]]
kind = "static"
pubkey_b64 = "{public_key}"
{web}
"#,
        package_path.display(),
        mock.signaling_ws_url,
        mock_http_url(mock),
        LOCAL_E2E_REALM_ID,
    );
    fs::write(path, content).expect("failed to write runtime config");
}

fn write_puppeteer_assertion(path: &Path, client_url: &str) {
    let script = format!(
        r#"
const puppeteer = require('puppeteer');
const fs = require('fs');
const {{ execSync }} = require('child_process');

function findChrome() {{
  const candidates = [
    process.env.PUPPETEER_EXECUTABLE_PATH,
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
    '/Applications/Chromium.app/Contents/MacOS/Chromium',
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
  ].filter(Boolean);
  for (const candidate of candidates) {{
    if (fs.existsSync(candidate)) return candidate;
  }}
  for (const command of ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser']) {{
    try {{
      const resolved = execSync(`command -v ${{command}}`, {{ encoding: 'utf8' }}).trim();
      if (resolved) return resolved;
    }} catch (_) {{}}
  }}
  return undefined;
}}

(async () => {{
  const logs = [];
  const executablePath = findChrome();
  const browser = await puppeteer.launch({{
    headless: 'new',
    executablePath,
    protocolTimeout: 300000,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--allow-insecure-localhost',
      '--ignore-certificate-errors',
      '--disable-web-security',
      '--disable-features=IsolateOrigins,site-per-process',
    ],
  }});
  try {{
    const page = await browser.newPage();
    page.on('console', (msg) => logs.push(msg.text()));
    page.on('pageerror', (err) => logs.push('[PAGE_ERROR] ' + err.message));
    await page.goto('{client_url}', {{ waitUntil: 'networkidle2', timeout: 60000 }});
    await page.waitForFunction(
      () => document.getElementById('status')?.textContent?.includes('✅'),
      {{ timeout: 60000 }},
    );
    await page.waitForFunction(
      () => document.getElementById('log')?.textContent?.includes('Guest workload registered'),
      {{ timeout: 60000 }},
    );
    await new Promise((resolve) => setTimeout(resolve, 1000));
    await page.evaluate(() => {{
      const input = document.getElementById('msgInput');
      input.value = 'generated ts workload';
      input.dispatchEvent(new Event('input', {{ bubbles: true }}));
    }});
    await page.evaluate(() => document.getElementById('sendBtn').click());
    await page.waitForFunction(
      () => document.getElementById('result')?.textContent?.includes('Reply: "generated ts workload"'),
      {{ timeout: 90000 }},
    );
  }} catch (error) {{
    console.error(error);
    try {{
      const pages = await browser.pages();
      const page = pages[pages.length - 1];
      if (page) {{
        const snapshot = await page.evaluate(() => ({{
          status: document.getElementById('status')?.textContent ?? '',
          result: document.getElementById('result')?.textContent ?? '',
          log: document.getElementById('log')?.textContent ?? '',
        }}));
        console.error('Page snapshot:\n' + JSON.stringify(snapshot, null, 2));
      }}
    }} catch (snapshotError) {{
      console.error('Failed to collect page snapshot:', snapshotError);
    }}
    console.error('Browser logs:\n' + logs.join('\n'));
    process.exitCode = 1;
  }} finally {{
    await browser.close();
  }}
}})();
"#
    );
    fs::write(path, script).expect("failed to write Puppeteer assertion");
}

fn puppeteer_node_path() -> OsString {
    let candidates = [
        workspace_root().join("bindings/web/node_modules"),
        workspace_root().join("node_modules"),
    ];

    for candidate in &candidates {
        let check = Command::new("node")
            .arg("-e")
            .arg("require('puppeteer')")
            .env("NODE_PATH", candidate)
            .output()
            .expect("failed to probe puppeteer");
        if check.status.success() {
            return candidate.clone().into_os_string();
        }
    }

    let web_dir = workspace_root().join("bindings/web");
    if which("pnpm").is_none() {
        assert!(
            which("corepack").is_some(),
            "neither pnpm nor corepack is available to install Puppeteer"
        );
        assert_success(
            &run(
                "corepack",
                ["prepare", "pnpm@9.15.9", "--activate"],
                &web_dir,
            ),
            "corepack prepare pnpm",
        );
    }
    assert_success(
        &run_with_env(
            "pnpm",
            ["install", "--frozen-lockfile"],
            &web_dir,
            &[
                ("PUPPETEER_SKIP_DOWNLOAD", OsString::from("true")),
                ("PUPPETEER_SKIP_CHROMIUM_DOWNLOAD", OsString::from("true")),
            ],
        ),
        "pnpm install (bindings/web)",
    );
    web_dir.join("node_modules").into_os_string()
}

#[test]
#[ignore] // Slow browser/native e2e, run explicitly in CI.
fn actr_web_calls_generated_typescript_echo_workload() {
    let _guard = e2e_lock()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let mock = MockSignaling::start().expect("failed to start mock-actrix");
    let tmp = TempDir::new().expect("failed to create temp dir");
    let release_dir = tmp.path().join("release");
    fs::create_dir_all(&release_dir).expect("failed to create release dir");

    let key_path = release_dir.join("dev-key.json");
    assert_success(
        &run_actr(
            [
                "pkg",
                "keygen",
                "--output",
                key_path.to_str().expect("key path should be UTF-8"),
                "--force",
            ],
            tmp.path(),
        ),
        "actr pkg keygen",
    );
    let public_key = read_public_key(&key_path);

    let service_package = build_generated_typescript_service(&release_dir, &key_path);
    let client_package = build_web_client_package(&release_dir, &key_path);

    let service_config = tmp.path().join("service-actr.toml");
    write_runtime_config(
        &service_config,
        &service_package,
        &mock,
        &public_key,
        "acme:echo-client-app:0.1.0",
        None,
    );

    let mut service_cmd = Command::new(actr_bin());
    service_cmd
        .args(["run", "--config"])
        .arg(&service_config)
        .args(["--hyper-dir"])
        .arg(tmp.path().join("service-hyper"))
        .env("RUST_LOG", e2e_rust_log());
    let mut service = LoggedProcess::spawn(service_cmd, "generated-ts-service")
        .expect("failed to start generated TS service");
    assert!(
        service.wait_for_log("ActrNode started", Duration::from_secs(240)),
        "generated TS service did not start:\n{}",
        service.logs()
    );

    let client_port = free_port();
    let client_config = tmp.path().join("client-actr.toml");
    write_runtime_config(
        &client_config,
        &client_package,
        &mock,
        &public_key,
        "acme:EchoService:0.1.0",
        Some(client_port),
    );

    let mut web_cmd = Command::new(actr_bin());
    web_cmd
        .args(["run", "--web", "--config"])
        .arg(&client_config)
        .env("RUST_LOG", e2e_rust_log());
    let mut web =
        LoggedProcess::spawn(web_cmd, "actr-web-client").expect("failed to start web client");
    assert!(
        web.wait_for_log("Web server started", Duration::from_secs(30)),
        "web client did not start:\n{}",
        web.logs()
    );

    let assertion = tmp.path().join("assert-generated-ts-web-e2e.cjs");
    write_puppeteer_assertion(&assertion, &format!("http://127.0.0.1:{client_port}"));
    let puppeteer_output = run_with_env(
        "node",
        [assertion.as_os_str()],
        tmp.path(),
        &[("NODE_PATH", puppeteer_node_path())],
    );
    if !puppeteer_output.status.success() {
        panic!(
            "Puppeteer generated TS workload browser assertion failed\n\
             status: {}\n\
             stdout:\n{}\n\
             stderr:\n{}\n\
             generated TS service logs:\n{}\n\
             actr-web client logs:\n{}",
            puppeteer_output.status,
            String::from_utf8_lossy(&puppeteer_output.stdout),
            String::from_utf8_lossy(&puppeteer_output.stderr),
            service.logs(),
            web.logs()
        );
    }

    assert!(
        service
            .logs()
            .contains("Received Echo request: generated ts workload"),
        "generated TS service did not log the Echo request:\n{}",
        service.logs()
    );
}
