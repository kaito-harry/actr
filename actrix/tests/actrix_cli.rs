use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

fn write_valid_config(dir: &Path, file_name: &str) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");

    let config_path = dir.join(file_name);
    fs::write(
        &config_path,
        format!(
            r#"
name = "actrix-cli-test"
enable = 16
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,cli"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 39999

[bind.ice]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]

[observability.log]
output = "console"
level = "info"
"#,
            sqlite = data_dir.display(),
            pid = dir.join("actrix.pid").display()
        ),
    )
    .expect("write valid config");

    config_path
}

fn write_warning_only_config(dir: &Path, file_name: &str) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");

    let config_path = dir.join(file_name);
    fs::write(
        &config_path,
        format!(
            r#"
name = "actrix-cli-warning-test"
enable = 16
env = "prod"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,cli-warning"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 39998

[bind.https]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 44443
cert = "/tmp/fake.crt"
key = "/tmp/fake.key"

[bind.ice]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]

[observability.log]
output = "console"
rotate = false
level = "info"
"#,
            sqlite = data_dir.display(),
            pid = dir.join("actrix.pid").display()
        ),
    )
    .expect("write warning-only config");

    config_path
}

fn write_validation_error_config(dir: &Path, file_name: &str) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");

    let config_path = dir.join(file_name);
    fs::write(
        &config_path,
        format!(
            r#"
name = "actrix-cli-validation-error-test"
enable = 16
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,cli-validation-error"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 39997

[bind.ice]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

# Intentionally no [services.ks] to trigger validation error for ENABLE_KS.

[observability.log]
output = "console"
level = "info"
"#,
            sqlite = data_dir.display(),
            pid = dir.join("actrix.pid").display()
        ),
    )
    .expect("write validation-error config");

    config_path
}

fn run_actrix(args: &[&str], current_dir: Option<&Path>) -> Output {
    let mut cmd = Command::new(PathBuf::from(env!("CARGO_BIN_EXE_actrix")));
    cmd.args(args);
    if let Some(dir) = current_dir {
        cmd.current_dir(dir);
    }
    cmd.output().expect("run actrix command")
}

#[test]
fn actrix_test_command_accepts_explicit_valid_config() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_path = write_valid_config(temp.path(), "valid.toml");
    let output = run_actrix(&["test", config_path.to_str().expect("utf8 path")], None);

    assert!(
        output.status.success(),
        "command should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn actrix_test_command_finds_default_config_in_current_directory() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_valid_config(temp.path(), "config.toml");
    let output = run_actrix(&["test"], Some(temp.path()));

    assert!(
        output.status.success(),
        "command should succeed, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn actrix_test_command_fails_for_missing_custom_config_path() {
    let temp = tempfile::tempdir().expect("temp dir");
    let missing_path = temp.path().join("missing.toml");
    let output = run_actrix(&["test", missing_path.to_str().expect("utf8 path")], None);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "command should fail");
    assert!(
        stderr.contains("Config file not found")
            || stderr.contains("configuration file")
            || stderr.contains("No configuration file found"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_test_command_fails_when_no_default_config_exists() {
    let temp = tempfile::tempdir().expect("temp dir");
    let output = run_actrix(&["test"], Some(temp.path()));
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "command should fail");
    assert!(
        stderr.contains("No configuration file found")
            || stderr.contains("Config file not found")
            || stderr.contains("configuration file"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_test_command_fails_for_invalid_config_content() {
    let temp = tempfile::tempdir().expect("temp dir");
    let bad_path = temp.path().join("bad.toml");
    fs::write(&bad_path, "name = \"broken\"\nenable = [\n").expect("write invalid toml");

    let output = run_actrix(&["test", bad_path.to_str().expect("utf8 path")], None);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "command should fail");
    assert!(
        stderr.contains("配置解析失败") || stderr.contains("parse") || stderr.contains("invalid"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_test_command_fails_for_validation_errors() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_path = write_validation_error_config(temp.path(), "validation-error.toml");
    let output = run_actrix(&["test", config_path.to_str().expect("utf8 path")], None);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "command should fail");
    assert!(
        stderr.contains("配置验证失败")
            || stderr.contains("validation")
            || stderr.contains("services.ks"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_test_command_succeeds_with_warning_only_config() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_path = write_warning_only_config(temp.path(), "warning.toml");
    let output = run_actrix(&["test", config_path.to_str().expect("utf8 path")], None);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "warning-only config should succeed, stderr: {stderr}"
    );
}

#[test]
fn actrix_run_mode_fails_when_no_default_config_exists() {
    let temp = tempfile::tempdir().expect("temp dir");
    let output = run_actrix(&[], Some(temp.path()));
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "run mode should fail");
    assert!(
        stderr.contains("No configuration file found")
            || stderr.contains("Config file not found")
            || stderr.contains("configuration file"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_run_mode_fails_for_missing_custom_config_flag() {
    let temp = tempfile::tempdir().expect("temp dir");
    let missing_path = temp.path().join("missing-run.toml");
    let output = run_actrix(
        &["--config", missing_path.to_str().expect("utf8 path")],
        None,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "run mode should fail");
    assert!(
        stderr.contains("Config file not found")
            || stderr.contains("No configuration file found")
            || stderr.contains("configuration file"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_run_mode_fails_for_invalid_config_content() {
    let temp = tempfile::tempdir().expect("temp dir");
    let bad_path = temp.path().join("bad-run.toml");
    fs::write(&bad_path, "name = \"broken\"\nenable = [\n").expect("write invalid toml");
    let output = run_actrix(&["--config", bad_path.to_str().expect("utf8 path")], None);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "run mode should fail");
    assert!(
        stderr.contains("配置加载失败")
            || stderr.contains("配置解析失败")
            || stderr.contains("parse")
            || stderr.contains("invalid"),
        "unexpected stderr: {stderr}"
    );
}

#[test]
fn actrix_run_mode_fails_for_validation_errors() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_path = write_validation_error_config(temp.path(), "run-validation-error.toml");
    let output = run_actrix(
        &["--config", config_path.to_str().expect("utf8 path")],
        None,
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success(), "run mode should fail");
    assert!(
        stderr.contains("配置验证失败")
            || stderr.contains("services.ks")
            || stderr.contains("validation"),
        "unexpected stderr: {stderr}"
    );
}
