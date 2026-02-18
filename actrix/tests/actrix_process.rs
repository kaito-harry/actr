use actrix_common::storage::db::Database;
use ks::{GrpcClient, GrpcClientConfig};
use nonce_auth::CredentialBuilder;
use serde_json::Value;
use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use supervit::{GetNodeInfoRequest, ShutdownRequest, SupervisedServiceClient};
use tonic::Code;

const START_TIMEOUT: Duration = Duration::from_secs(15);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const TEST_ACTRIX_SHARED_KEY: &str = "0123456789abcdef0123456789abcdef";
const TEST_VALID_KEK_HEX: &str = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
const TEST_SUPERVISOR_SHARED_SECRET: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const TEST_SUPERVISOR_NODE_ID: &str = "actrix-process-node";
const TEST_TLS_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIIDCTCCAfGgAwIBAgIUEtoFL1fivjqWKHCAUVysBEESK2YwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDIxODA2MjU1MFoXDTI3MDIx
ODA2MjU1MFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEA6S4he2gvt9HC2RBHz9hU/+pGAOdCxFI2PWCK8Wx4J/99
CyxTpvXZsdweYUvdW18BJFrTCTbTo/q6QJYg6VaNhwXJ0+XOrLI1XG/XizfepIrQ
TvRua2Z2RlM1gTQb9TrlOZhgHyb/q9UHjhZy70AwgN2cwlyr+JlypeMnRBEra3us
1iOjrHb8FaKF06vCFLyIrnLn23Ejhtb/i1w/NI7mZphTYCf+7cRu1eAIH2wCZTNV
MPteILO/W3s6ysH22EBjYNYJckjXH73iv/8g22cRpMa1x3jvqqhd/mX5AT/Xsbk0
7QpM3rgoTeE2QX0yTaSWd0s7k1DP7MwrWvFOmFLTdwIDAQABo1MwUTAdBgNVHQ4E
FgQUPtDBOQVdj9EsWGUMOpoJ6Q83LwcwHwYDVR0jBBgwFoAUPtDBOQVdj9EsWGUM
OpoJ6Q83LwcwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAeMue
PZrKuA0qKnR9vvKaavnmsijmUsU5+cJPUnjTbCaWEVptJu5/qdqxwbCQQJ1bG84D
S2krvn0ovNXk77qKJb5k10c6yKPn12PcW/ktM0ghZwszZWWScmAELI3ZFviRIg6+
budljLIxtJXhJun+HGMGCPJd3TJUmnodgMZoLkamBiNRNkF3vPNcWSIVSSpkmyiU
yKdRuoV0noaIZzjZH2ScFcRQ24idVTy7cOFXY6O72KjOyLOdiSrHnkITUmBZOY+j
oR/uCoHUxHMlhe/lHLvOrVOs9cMJg4m1dkBxAJufYE4zFGlvq5qNhZt/SIxabdWm
nNCuuhuQMNMXZbHhyA==
-----END CERTIFICATE-----
"#;
const TEST_TLS_KEY_PEM: &str = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDpLiF7aC+30cLZ
EEfP2FT/6kYA50LEUjY9YIrxbHgn/30LLFOm9dmx3B5hS91bXwEkWtMJNtOj+rpA
liDpVo2HBcnT5c6ssjVcb9eLN96kitBO9G5rZnZGUzWBNBv1OuU5mGAfJv+r1QeO
FnLvQDCA3ZzCXKv4mXKl4ydEEStre6zWI6OsdvwVooXTq8IUvIiucufbcSOG1v+L
XD80juZmmFNgJ/7txG7V4AgfbAJlM1Uw+14gs79bezrKwfbYQGNg1glySNcfveK/
/yDbZxGkxrXHeO+qqF3+ZfkBP9exuTTtCkzeuChN4TZBfTJNpJZ3SzuTUM/szCta
8U6YUtN3AgMBAAECggEACs0lpRpjZq48/citeukR4htPcm7dmEuynxwGWgtk2X14
cZJBFPFu5YVFYJgFQSlHQhB1YogVCUbtzI+0jWDo0QHDCQf5KRjFzgPERhR1dMVi
Kg+6jORD9rTK1eXv2AE9aXIvtntPY2QZcH4e66RBwVM14rvGOSk3UIxGklzO2EpD
4zZD3RusHuXJ1ick3xm8mS1jJub4q0y/yFzk9GdFOWn0ZvmgyrOCX9bzaTZe17Ok
Za67JettYKwQVZ/NutPa3w/kQdXLCDYHiJ+yVfGN68rxMaWyj+/44NX3Aa0mbJcT
6If44/JEzkvCUQXz9/0s89CS5I2CHPM682hv4NPSEQKBgQD1Tgjzh/t5JCeiJvWK
cNYMh2LFTvzIQNg6urbgBEyxHrT3us4EXj5xnptZCCMadSCFC8RP9eDF2hswdESW
2n1xJ2yfklUs0DBni9Fc1yFedGGlMspKw6wf5rfz73HIfzPml6PKdbflMcoIM4RV
Sq7xeQK15j8YNOw31Qub5uzo/wKBgQDzWMRdUZpSeYHX7KZyNcS+7hN2ocu0c6Gl
39oiPtyowErCKOYqs1Hz9goJ5bxcqDyQP6qDaUuwYq03RtgHyFVLvGiTva1Us5wk
V62KmnweghGQtUrM1i0RC1j2x/R4ZFUhTV2emV/b+xIvyw+qgSsHSSNgH3vp3v2O
846hq5rdiQKBgH5vSTfUh+4pj3AJapd/jyQICAWwr6O7oHes0yNls+266QWiuBsS
RFclq+ZYxlcVtbw9k2KvVbpEr6zq0It8dBmFe3xH3TTq3XgRXcjfbWiUzdtq8U9V
yXrr3TaS3O+9eI/K6vYodK9iWUKe4v9fLgpyF86PrUeZx4MDgSdLACMbAoGBAMfY
BWNRybejm9N0wHiY2ZunLwrE8uKd94menah0EYjwajSrm+JDY7FDRJk+NwOtEhew
gVrsVUFkuDXmEzHI/ut0rjlukvM1kaxy6M0j83ymesBpciVoWphdxlDcg1N/qj3w
KEtAT+37ccMYMyRmcazJDqk5Ee1NuNP2BxOUN1lpAoGATsMJlsmD/YgdCtfN7xfL
RJx5Jl5w8mX4ttUK+bQBQKWFS9w1puXlMGJNlVx3m2iaKjouNtNNUzNHVKLK56zL
PL0kD6UHn3bvio6PJQ1tCI/ocvy6fZPxrmLtp1KpxPnNXz6eJ2tp3vmMYSYjECmZ
MnKe1uXgE8houbMEhvDdjtQ=
-----END PRIVATE KEY-----
"#;

#[cfg(test)]
use serial_test::serial;

fn choose_port() -> u16 {
    if let Some(p) = std::env::var("ACTRIX_TEST_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return p;
    }
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read bound local addr")
        .port()
}

fn write_minimal_config(dir: &PathBuf, port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,default"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[observability.log]
output = "console"
level = "info"

"#,
        sqlite = data_dir.display(),
        port = port,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn write_minimal_config_with_ks_kek(dir: &PathBuf, port: u16, kek: &str) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-ks-kek"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,ks-kek"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
kek = "{kek}"

[observability.log]
output = "console"
level = "info"
"#,
        sqlite = data_dir.display(),
        port = port,
        pid = dir.join("actrix.pid").display(),
        kek = kek
    )
    .expect("write config");
    config_path
}

fn write_minimal_config_with_custom_pid_path(dir: &PathBuf, port: u16, pid_path: &str) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-custom-pid"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,custom-pid"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[observability.log]
output = "console"
level = "info"
"#,
        sqlite = data_dir.display(),
        port = port,
        pid = pid_path
    )
    .expect("write config");
    config_path
}

fn write_minimal_config_without_pid(dir: &PathBuf, port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-no-pid"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,no-pid"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[observability.log]
output = "console"
level = "info"
"#,
        sqlite = data_dir.display(),
        port = port
    )
    .expect("write config");
    config_path
}

fn write_minimal_config_with_file_logging(
    dir: &PathBuf,
    port: u16,
    rotate: bool,
) -> (PathBuf, PathBuf) {
    let data_dir = dir.join("data");
    let log_dir = dir.join("logs");
    fs::create_dir_all(&data_dir).expect("create data dir");
    fs::create_dir_all(&log_dir).expect("create log dir");

    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,default"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[observability.log]
output = "file"
path = "{log_path}"
rotate = {rotate}
level = "info"

"#,
        sqlite = data_dir.display(),
        port = port,
        pid = dir.join("actrix.pid").display(),
        log_path = log_dir.display(),
        rotate = rotate
    )
    .expect("write config");

    (config_path, log_dir)
}

fn write_minimal_config_with_user_group(
    dir: &PathBuf,
    port: u16,
    user: &str,
    group: &str,
) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");

    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,default"
pid = "{pid}"
user = "{user}"
group = "{group}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[observability.log]
output = "console"
level = "info"

"#,
        sqlite = data_dir.display(),
        port = port,
        pid = dir.join("actrix.pid").display(),
        user = user,
        group = group
    )
    .expect("write config");

    config_path
}

fn write_config_with_sqlite_path(dir: &PathBuf, port: u16, sqlite_path: &str) -> PathBuf {
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-invalid-db"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,default"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[observability.log]
output = "console"
level = "info"

"#,
        sqlite = sqlite_path,
        port = port,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn write_config_with_http_bind_ip(dir: &PathBuf, port: u16, bind_ip: &str) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-http-bind-ip"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,http-bind-ip"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "{bind_ip}"
port = {port}

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
# defaults

[observability.log]
output = "console"
level = "info"

"#,
        sqlite = data_dir.display(),
        port = port,
        pid = dir.join("actrix.pid").display(),
        bind_ip = bind_ip
    )
    .expect("write config");
    config_path
}

fn write_config_with_supervisor(
    dir: &PathBuf,
    port: u16,
    supervisor_bind_ip: &str,
    supervisor_port: u16,
    supervisor_endpoint: &str,
) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-supervisor"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,supervisor"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

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
# defaults

[supervisor]
connect_timeout_secs = 1
status_report_interval_secs = 5
health_check_interval_secs = 5
enable_tls = false
max_clock_skew_secs = 300

[supervisor.supervisord]
node_name = "actrix-test-node"
ip = "{supervisor_bind_ip}"
port = {supervisor_port}
advertised_ip = "127.0.0.1"

[supervisor.client]
node_id = "{node_id}"
shared_secret = "{shared_secret}"
endpoint = "{supervisor_endpoint}"

[observability.log]
output = "console"
level = "info"

"#,
        sqlite = data_dir.display(),
        port = port,
        supervisor_bind_ip = supervisor_bind_ip,
        supervisor_port = supervisor_port,
        node_id = TEST_SUPERVISOR_NODE_ID,
        shared_secret = TEST_SUPERVISOR_SHARED_SECRET,
        supervisor_endpoint = supervisor_endpoint,
        pid = dir.join("actrix.pid").display()
    )
    .expect("write config");
    config_path
}

fn write_ice_only_config(dir: &PathBuf, enable: u8, ice_port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-ice-only"
enable = {enable}
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,ice"
pid = "{pid}"

[bind]
[bind.ice]
domain_name = "127.0.0.1"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {ice_port}

[turn]
advertised_ip = "127.0.0.1"
advertised_port = {ice_port}
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[observability.log]
output = "console"
level = "info"
"#,
        enable = enable,
        sqlite = data_dir.display(),
        pid = dir.join("actrix.pid").display(),
        ice_port = ice_port
    )
    .expect("write config");
    config_path
}

fn write_ks_without_http_or_https_bind_config(dir: &PathBuf, ice_port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-ks-no-http-bind"
enable = 16  # ENABLE_KS
env = "dev"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,ks-no-http-bind"
pid = "{pid}"

[bind]
[bind.ice]
domain_name = "127.0.0.1"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {ice_port}

[turn]
advertised_ip = "127.0.0.1"
advertised_port = {ice_port}
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
# defaults

[observability.log]
output = "console"
level = "info"
"#,
        sqlite = data_dir.display(),
        pid = dir.join("actrix.pid").display(),
        ice_port = ice_port
    )
    .expect("write config");
    config_path
}

fn write_prod_ks_without_https_bind_config(dir: &PathBuf, port: u16) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-prod-no-https"
enable = 16  # ENABLE_KS
env = "prod"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,prod-no-https"
pid = "{pid}"

[bind]
[bind.http]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}

[bind.ice]
domain_name = "127.0.0.1"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
# defaults

[observability.log]
output = "console"
level = "info"
"#,
        sqlite = data_dir.display(),
        pid = dir.join("actrix.pid").display(),
        port = port
    )
    .expect("write config");
    config_path
}

fn write_ks_https_only_with_missing_tls_files_config(
    dir: &PathBuf,
    port: u16,
    env: &str,
) -> PathBuf {
    let cert_path = dir.join("missing-cert.pem");
    let key_path = dir.join("missing-key.pem");
    write_ks_https_only_config(dir, port, env, &cert_path, &key_path)
}

fn write_ks_https_only_config(
    dir: &PathBuf,
    port: u16,
    env: &str,
    cert_path: &PathBuf,
    key_path: &PathBuf,
) -> PathBuf {
    let data_dir = dir.join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");
    let config_path = dir.join("config.toml");
    let mut f = fs::File::create(&config_path).expect("create config file");
    writeln!(
        f,
        r#"
name = "actrix-test-https-missing-tls"
enable = 16  # ENABLE_KS
env = "{env}"
sqlite_path = "{sqlite}"
actrix_shared_key = "0123456789abcdef0123456789abcdef"
location_tag = "local,test,https-missing-tls"
pid = "{pid}"

[bind]
[bind.https]
domain_name = "localhost"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = {port}
cert = "{cert}"
key = "{key}"

[bind.ice]
domain_name = "127.0.0.1"
advertised_ip = "127.0.0.1"
ip = "127.0.0.1"
port = 0

[turn]
advertised_ip = "127.0.0.1"
advertised_port = 3478
relay_port_range = "49152-65535"
realm = "actor-rtc.local"

[services.ks]
# defaults

[observability.log]
output = "console"
level = "info"
"#,
        env = env,
        sqlite = data_dir.display(),
        pid = dir.join("actrix.pid").display(),
        port = port,
        cert = cert_path.display(),
        key = key_path.display()
    )
    .expect("write config");
    config_path
}

fn spawn_actrix(config: &PathBuf, log_path: &PathBuf) -> Child {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_actrix"));
    let log_file = fs::File::create(log_path).expect("create log file");
    Command::new(bin)
        .arg("--config")
        .arg(config)
        .stdout(Stdio::from(log_file.try_clone().expect("dup log")))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn actrix")
}

fn spawn_actrix_from_workdir(workdir: &PathBuf, log_path: &PathBuf) -> Child {
    let bin = PathBuf::from(env!("CARGO_BIN_EXE_actrix"));
    let log_file = fs::File::create(log_path).expect("create log file");
    Command::new(bin)
        .current_dir(workdir)
        .stdout(Stdio::from(log_file.try_clone().expect("dup log")))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn actrix")
}

async fn wait_for_health(url: &str, child: &mut Child, log_path: &PathBuf) {
    let client = reqwest::Client::new();
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        if let Ok(resp) = client.get(url).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("health check not ready at {}\nlogs:\n{}", url, log);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_health_https_insecure(url: &str, child: &mut Child, log_path: &PathBuf) {
    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("create insecure https client");
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        if let Ok(resp) = client.get(url).send().await {
            if resp.status().is_success() {
                return;
            }
        }
        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("health check not ready at {}\nlogs:\n{}", url, log);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn write_test_tls_cert_pair(dir: &PathBuf) -> (PathBuf, PathBuf) {
    let cert_path = dir.join("tls-cert.pem");
    let key_path = dir.join("tls-key.pem");
    fs::write(&cert_path, TEST_TLS_CERT_PEM).expect("write test tls cert");
    fs::write(&key_path, TEST_TLS_KEY_PEM).expect("write test tls key");
    (cert_path, key_path)
}

async fn wait_for_supervisord(
    endpoint: &str,
    child: &mut Child,
    log_path: &PathBuf,
) -> SupervisedServiceClient<tonic::transport::Channel> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        if let Ok(client) = SupervisedServiceClient::connect(endpoint.to_string()).await {
            return client;
        }

        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("supervisord grpc not ready at {endpoint}\nlogs:\n{log}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_ks_grpc_client(
    shared_key: &str,
    child: &mut Child,
    log_path: &PathBuf,
) -> GrpcClient {
    let start = Instant::now();
    let endpoint = "http://127.0.0.1:50052".to_string();
    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        let config = GrpcClientConfig {
            endpoint: endpoint.clone(),
            actrix_shared_key: shared_key.to_string(),
            timeout_seconds: 2,
            enable_tls: false,
            tls_domain: None,
            ca_cert: None,
            client_cert: None,
            client_key: None,
        };

        if let Ok(client) = GrpcClient::new(&config).await {
            return client;
        }

        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("ks grpc not ready at {endpoint}\nlogs:\n{log}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn build_node_info_credential(shared_secret: &[u8]) -> supervit::NonceCredential {
    let payload = format!("node_info:{TEST_SUPERVISOR_NODE_ID}");
    let credential = CredentialBuilder::new(shared_secret)
        .sign(payload.as_bytes())
        .expect("build node info credential");
    supervit::nonce_auth::to_proto_credential(credential)
}

fn build_node_info_credential_with_timestamp(
    shared_secret: &[u8],
    timestamp: u64,
) -> supervit::NonceCredential {
    let payload = format!("node_info:{TEST_SUPERVISOR_NODE_ID}");
    let credential = CredentialBuilder::new(shared_secret)
        .with_time_provider(move || Ok(timestamp))
        .sign(payload.as_bytes())
        .expect("build node info credential with timestamp");
    supervit::nonce_auth::to_proto_credential(credential)
}

fn build_shutdown_credential(shared_secret: &[u8]) -> supervit::NonceCredential {
    let payload = format!("shutdown:{TEST_SUPERVISOR_NODE_ID}");
    let credential = CredentialBuilder::new(shared_secret)
        .sign(payload.as_bytes())
        .expect("build shutdown credential");
    supervit::nonce_auth::to_proto_credential(credential)
}

fn build_stun_binding_request(txid: [u8; 12]) -> [u8; 20] {
    let mut req = [0u8; 20];
    req[1] = 0x01; // Binding Request
    req[4] = 0x21;
    req[5] = 0x12;
    req[6] = 0xA4;
    req[7] = 0x42;
    req[8..20].copy_from_slice(&txid);
    req
}

fn is_stun_binding_success(packet: &[u8], txid: [u8; 12]) -> bool {
    packet.len() >= 20
        && packet[0] == 0x01
        && packet[1] == 0x01
        && packet[4] == 0x21
        && packet[5] == 0x12
        && packet[6] == 0xA4
        && packet[7] == 0x42
        && packet[8..20] == txid
}

async fn wait_for_stun_binding_success(target: &str, child: &mut Child, log_path: &PathBuf) {
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind udp probe socket");
    let txid = [1, 35, 69, 103, 137, 171, 205, 239, 16, 50, 84, 118];
    let request = build_stun_binding_request(txid);
    let mut buf = [0u8; 2048];
    let start = Instant::now();

    loop {
        if let Some(status) = child.try_wait().unwrap_or(None) {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("actrix exited early: status={status:?}\nlogs:\n{log}");
        }

        socket
            .send_to(&request, target)
            .await
            .expect("send stun probe");
        if let Ok(Ok((len, _addr))) =
            tokio::time::timeout(Duration::from_millis(300), socket.recv_from(&mut buf)).await
            && is_stun_binding_success(&buf[..len], txid)
        {
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(log_path).unwrap_or_default();
            panic!("stun binding response not ready at {target}\nlogs:\n{log}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn graceful_shutdown(mut child: Child) {
    // Try SIGINT first (Unix only); fallback to kill.
    #[cfg(unix)]
    unsafe {
        libc::kill(child.id() as i32, libc::SIGINT);
    }
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_status)) => return,
            Ok(None) => {
                if start.elapsed() > SHUTDOWN_TIMEOUT {
                    let _ = child.kill();
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
            Err(_) => return,
        }
    }
}

#[tokio::test]
#[serial]
async fn actrix_starts_serves_health_and_shuts_down() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    let resp = reqwest::get(&health_url).await.expect("health request");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("health json");
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["service"], "ks");

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_ks_grpc_health_and_key_lifecycle() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-ks-grpc.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let mut grpc_client =
        wait_for_ks_grpc_client(TEST_ACTRIX_SHARED_KEY, &mut child, &log_path).await;
    let grpc_health = grpc_client.health_check().await.expect("grpc health check");
    assert_eq!(grpc_health, "healthy");

    let (key_id, _public_key, expires_at, tolerance_seconds) =
        grpc_client.generate_key().await.expect("grpc generate key");
    assert!(key_id > 0, "generated key_id should be positive");
    assert!(expires_at > 0, "generated key should have expiration");
    assert!(tolerance_seconds > 0, "tolerance should be positive");

    let (_secret_key, secret_expires_at, secret_tolerance_seconds) = grpc_client
        .fetch_secret_key(key_id)
        .await
        .expect("grpc fetch secret key");
    assert_eq!(secret_expires_at, expires_at);
    assert_eq!(secret_tolerance_seconds, tolerance_seconds);

    let missing_key_err = grpc_client
        .fetch_secret_key(key_id.saturating_add(1_000_000))
        .await
        .expect_err("fetching non-existent key should fail");
    let missing_key_text = missing_key_err.to_string();
    assert!(
        missing_key_text.contains("NotFound")
            || missing_key_text.to_lowercase().contains("not found"),
        "expected not-found grpc error, got: {missing_key_text}"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_ks_grpc_rejects_invalid_shared_secret() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-ks-grpc-bad-secret.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;
    let _ready_client =
        wait_for_ks_grpc_client(TEST_ACTRIX_SHARED_KEY, &mut child, &log_path).await;

    let bad_config = GrpcClientConfig {
        endpoint: "http://127.0.0.1:50052".to_string(),
        actrix_shared_key: "wrong-shared-secret".to_string(),
        timeout_seconds: 2,
        enable_tls: false,
        tls_domain: None,
        ca_cert: None,
        client_cert: None,
        client_key: None,
    };
    let mut bad_client = GrpcClient::new(&bad_config)
        .await
        .expect("grpc client should connect before auth checks");
    let err = bad_client
        .generate_key()
        .await
        .expect_err("invalid shared secret should be rejected");
    let err_text = err.to_string();
    assert!(
        err_text.contains("Unauthenticated") || err_text.to_lowercase().contains("authentication"),
        "expected authentication failure from grpc generate_key, got: {err_text}"
    );

    let http_health = reqwest::get(&health_url).await.expect("ks health request");
    assert!(
        http_health.status().is_success(),
        "process should keep running after grpc auth rejection"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_ks_grpc_health_and_key_lifecycle_with_valid_kek() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_minimal_config_with_ks_kek(&tmp.path().to_path_buf(), port, TEST_VALID_KEK_HEX);
    let log_path = tmp.path().join("actrix-ks-grpc-kek.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let mut grpc_client =
        wait_for_ks_grpc_client(TEST_ACTRIX_SHARED_KEY, &mut child, &log_path).await;
    let (key_id, _public_key, expires_at, tolerance_seconds) = grpc_client
        .generate_key()
        .await
        .expect("grpc generate key with kek");
    assert!(key_id > 0);
    assert!(expires_at > 0);
    assert!(tolerance_seconds > 0);

    let (_secret_key, fetched_expires_at, fetched_tolerance) = grpc_client
        .fetch_secret_key(key_id)
        .await
        .expect("grpc fetch secret key with kek");
    assert_eq!(fetched_expires_at, expires_at);
    assert_eq!(fetched_tolerance, tolerance_seconds);

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_ks_kek_is_invalid() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_minimal_config_with_ks_kek(&tmp.path().to_path_buf(), port, "invalid-kek");
    let log_path = tmp.path().join("actrix-invalid-kek.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail fast when KS KEK is invalid"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("KEK")
                    || log.contains("key encryptor")
                    || log.contains("Invalid")
                    || log.contains("初始化失败"),
                "expected invalid kek startup error in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when KS KEK is invalid");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_starts_with_default_config_in_working_directory() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-default-cwd.log");
    let mut child = spawn_actrix_from_workdir(&tmp.path().to_path_buf(), &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_starts_without_pid_file_configured() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config_without_pid(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-no-pid.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    assert!(
        !tmp.path().join("actrix.pid").exists(),
        "pid file should not be created when pid is not configured"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_writes_pid_file_and_cleans_on_shutdown() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-pid.log");
    let pid_path = tmp.path().join("actrix.pid");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    let pid_content = fs::read_to_string(&pid_path).expect("pid file should exist while running");
    let pid_from_file: u32 = pid_content
        .trim()
        .parse()
        .expect("pid file should contain numeric pid");
    assert_eq!(pid_from_file, child.id(), "pid file should match child pid");

    graceful_shutdown(child);

    let start = Instant::now();
    while pid_path.exists() {
        if start.elapsed() > SHUTDOWN_TIMEOUT {
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            panic!("pid file should be removed after shutdown\nlogs:\n{log}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_shutdown_tolerates_missing_pid_file() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-missing-pid.log");
    let pid_path = tmp.path().join("actrix.pid");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    fs::remove_file(&pid_path).expect("remove pid file before shutdown");
    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_writes_pid_file_in_nested_directory_and_cleans_on_shutdown() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let pid_path = tmp.path().join("run").join("state").join("actrix.pid");
    let config_path = write_minimal_config_with_custom_pid_path(
        &tmp.path().to_path_buf(),
        port,
        pid_path.to_str().expect("pid path to string"),
    );
    let log_path = tmp.path().join("actrix-nested-pid.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    assert!(
        pid_path.parent().expect("pid parent").exists(),
        "nested pid parent directory should be created automatically"
    );
    assert!(
        pid_path.exists(),
        "pid file should be created inside nested directory"
    );

    graceful_shutdown(child);

    let start = Instant::now();
    while pid_path.exists() {
        if start.elapsed() > SHUTDOWN_TIMEOUT {
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            panic!("nested pid file should be removed after shutdown\nlogs:\n{log}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exposes_prometheus_metrics_endpoint() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-metrics.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{}/ks/health", port);
    wait_for_health(&health_url, &mut child, &log_path).await;

    let metrics_url = format!("http://127.0.0.1:{port}/metrics");
    let resp = reqwest::get(&metrics_url).await.expect("metrics request");
    assert!(resp.status().is_success());
    let body = resp.text().await.expect("metrics text");
    assert!(
        body.contains("actrix_websocket_connections"),
        "expected websocket gauge in /metrics output"
    );
    assert!(
        body.contains("actrix_turn_active_sessions"),
        "expected turn session gauge in /metrics output"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_writes_logs_to_file_when_configured() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let (config_path, log_dir) =
        write_minimal_config_with_file_logging(&tmp.path().to_path_buf(), port, false);
    let expected_log_file = log_dir.join("actrix.log");
    let bootstrap_log_path = tmp.path().join("actrix-bootstrap.log");
    let mut child = spawn_actrix(&config_path, &bootstrap_log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &bootstrap_log_path).await;

    graceful_shutdown(child);

    let start = Instant::now();
    loop {
        if expected_log_file.exists() {
            let content = fs::read_to_string(&expected_log_file).unwrap_or_default();
            if !content.trim().is_empty() {
                return;
            }
        }

        if start.elapsed() > SHUTDOWN_TIMEOUT {
            let bootstrap_log = fs::read_to_string(&bootstrap_log_path).unwrap_or_default();
            panic!(
                "expected file log output at {}\nbootstrap logs:\n{}",
                expected_log_file.display(),
                bootstrap_log
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_writes_rotated_logs_when_configured() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let (config_path, log_dir) =
        write_minimal_config_with_file_logging(&tmp.path().to_path_buf(), port, true);
    let bootstrap_log_path = tmp.path().join("actrix-rotate-bootstrap.log");
    let mut child = spawn_actrix(&config_path, &bootstrap_log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &bootstrap_log_path).await;

    graceful_shutdown(child);

    let start = Instant::now();
    loop {
        let has_log_file = fs::read_dir(&log_dir)
            .ok()
            .into_iter()
            .flat_map(|entries| entries.filter_map(Result::ok))
            .any(|entry| entry.file_name().to_string_lossy().contains("actrix.log"));

        if has_log_file {
            return;
        }

        if start.elapsed() > SHUTDOWN_TIMEOUT {
            let bootstrap_log = fs::read_to_string(&bootstrap_log_path).unwrap_or_default();
            panic!(
                "expected rotated log files under {}\nbootstrap logs:\n{}",
                log_dir.display(),
                bootstrap_log
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_starts_with_user_and_group_set_on_non_root() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_minimal_config_with_user_group(&tmp.path().to_path_buf(), port, "nobody", "nogroup");
    let log_path = tmp.path().join("actrix-user-group.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    graceful_shutdown(child);

    #[cfg(unix)]
    {
        if unsafe { libc::geteuid() } != 0 {
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("cannot switch user/group"),
                "expected non-root privilege-drop warning in logs, got: {log}"
            );
        }
    }
}

#[tokio::test]
#[serial]
async fn actrix_stun_only_serves_binding_success() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let ice_port = choose_port();
    let config_path = write_ice_only_config(&tmp.path().to_path_buf(), 2, ice_port);
    let log_path = tmp.path().join("actrix-stun-only.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    wait_for_stun_binding_success(&format!("127.0.0.1:{ice_port}"), &mut child, &log_path).await;

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_turn_only_serves_binding_success() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let ice_port = choose_port();
    let config_path = write_ice_only_config(&tmp.path().to_path_buf(), 4, ice_port);
    let log_path = tmp.path().join("actrix-turn-only.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    wait_for_stun_binding_success(&format!("127.0.0.1:{ice_port}"), &mut child, &log_path).await;

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_database_path_is_unavailable() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_config_with_sqlite_path(&tmp.path().to_path_buf(), port, "/proc/actrix-db-denied");
    let log_path = tmp.path().join("actrix-invalid-db.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should exit with non-zero status when db init fails"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            let log_lower = log.to_lowercase();
            assert!(
                log_lower.contains("database")
                    || log.contains("数据库")
                    || log_lower.contains("sqlite"),
                "expected database failure hint in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when database path is unavailable");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_http_bind_ip_is_invalid() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_config_with_http_bind_ip(&tmp.path().to_path_buf(), port, "invalid-bind-ip");
    let log_path = tmp.path().join("actrix-invalid-http-bind-ip.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail when HTTP bind IP is invalid"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("Invalid bind address")
                    || log.contains("Failed to bind")
                    || log.to_lowercase().contains("bind address"),
                "expected invalid http bind address error in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when HTTP bind IP is invalid");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_http_port_is_already_in_use() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let occupied = std::net::TcpListener::bind(("127.0.0.1", port))
        .expect("occupy port before starting actrix");
    let config_path = write_minimal_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-http-port-conflict.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail when HTTP port is occupied"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("Failed to bind to address")
                    || log.contains("Address already in use")
                    || log.to_lowercase().contains("in use"),
                "expected port bind conflict in logs, got: {log}"
            );
            drop(occupied);
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            drop(occupied);
            graceful_shutdown(child);
            panic!("actrix should fail fast when HTTP port is already in use");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_pid_parent_path_is_not_directory() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let blocked_parent = tmp.path().join("pid-parent-file");
    fs::write(&blocked_parent, "not-a-directory").expect("create blocker file");
    let bad_pid = blocked_parent.join("actrix.pid");
    let config_path = write_minimal_config_with_custom_pid_path(
        &tmp.path().to_path_buf(),
        port,
        bad_pid
            .to_str()
            .expect("convert blocked pid path to string"),
    );
    let log_path = tmp.path().join("actrix-invalid-pid-path.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should exit with non-zero status when pid path parent is invalid"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("Failed to create PID file directory")
                    || log.contains("pid")
                    || log.contains("PID"),
                "expected pid file directory failure in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when pid path parent is not a directory");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_on_incompatible_existing_schema() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let data_dir = tmp.path().join("data");
    fs::create_dir_all(&data_dir).expect("create data dir");

    // Pre-create a deliberately incompatible schema:
    // `realm` exists but misses required `realm_id`, so index creation must fail on startup.
    let db = Database::new(&data_dir)
        .await
        .expect("precreate db with default schema");
    db.execute("DROP TABLE IF EXISTS realm")
        .await
        .expect("drop realm table");
    db.execute(
        "CREATE TABLE realm (
            rowid INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL
        )",
    )
    .await
    .expect("create incompatible realm table");

    let config_path = write_config_with_sqlite_path(
        &tmp.path().to_path_buf(),
        port,
        &data_dir.display().to_string(),
    );
    let log_path = tmp.path().join("actrix-incompatible-schema.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should exit with non-zero status for incompatible schema"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            let log_lower = log.to_lowercase();
            assert!(
                log_lower.contains("realm_id")
                    || log_lower.contains("idx_realm_realm_id")
                    || log_lower.contains("database")
                    || log.contains("数据库"),
                "expected schema/index failure hint in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast for incompatible existing schema");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_supervisord_bind_address_is_invalid() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let supervisor_port = choose_port();
    let config_path = write_config_with_supervisor(
        &tmp.path().to_path_buf(),
        port,
        "invalid-bind-ip",
        supervisor_port,
        "http://127.0.0.1:50051",
    );
    let log_path = tmp.path().join("actrix-invalid-supervisord-bind.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should exit with non-zero status when supervisord bind address is invalid"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("Failed to parse supervisord bind address")
                    || log.to_lowercase().contains("supervisord bind address"),
                "expected supervisord bind parse failure in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast on invalid supervisord bind address");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_http_service_has_no_http_or_https_bind_config() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let ice_port = choose_port();
    let config_path =
        write_ks_without_http_or_https_bind_config(&tmp.path().to_path_buf(), ice_port);
    let log_path = tmp.path().join("actrix-no-http-bind.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail when HTTP service has no HTTP/HTTPS bind config"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("No HTTP or HTTPS binding configuration found")
                    || log.to_lowercase().contains("http") && log.to_lowercase().contains("https"),
                "expected missing HTTP/HTTPS bind error in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when HTTP service has no bind configuration");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_in_production_without_https_bind_config() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path = write_prod_ks_without_https_bind_config(&tmp.path().to_path_buf(), port);
    let log_path = tmp.path().join("actrix-prod-no-https.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail when prod config lacks HTTPS bind"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            assert!(
                log.contains("HTTPS binding configuration is required for production environment")
                    || (log.to_lowercase().contains("https")
                        && log.to_lowercase().contains("production")),
                "expected production HTTPS requirement in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast in prod when https bind config is missing");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_dev_https_fallback_tls_files_are_missing() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_ks_https_only_with_missing_tls_files_config(&tmp.path().to_path_buf(), port, "dev");
    let log_path = tmp.path().join("actrix-dev-https-missing-tls.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail when dev HTTPS fallback TLS files are missing"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            let log_lower = log.to_lowercase();
            assert!(
                log_lower.contains("no such file")
                    || log_lower.contains("failed to")
                    || log_lower.contains("pem")
                    || log_lower.contains("missing-cert"),
                "expected TLS file error in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when dev HTTPS fallback TLS files are missing");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_exits_when_prod_https_tls_files_are_missing() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let config_path =
        write_ks_https_only_with_missing_tls_files_config(&tmp.path().to_path_buf(), port, "prod");
    let log_path = tmp.path().join("actrix-prod-https-missing-tls.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                !status.success(),
                "process should fail when prod HTTPS TLS files are missing"
            );
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            let log_lower = log.to_lowercase();
            assert!(
                log_lower.contains("https")
                    && (log_lower.contains("no such file")
                        || log_lower.contains("failed to")
                        || log_lower.contains("pem")),
                "expected prod TLS file error in logs, got: {log}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            graceful_shutdown(child);
            panic!("actrix should fail fast when prod HTTPS TLS files are missing");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_serves_https_health_in_dev_when_only_https_bind_is_configured() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let (cert_path, key_path) = write_test_tls_cert_pair(&tmp.path().to_path_buf());
    let config_path = write_ks_https_only_config(
        &tmp.path().to_path_buf(),
        port,
        "dev",
        &cert_path,
        &key_path,
    );
    let log_path = tmp.path().join("actrix-dev-https.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("https://127.0.0.1:{port}/ks/health");
    wait_for_health_https_insecure(&health_url, &mut child, &log_path).await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("create insecure client");
    let resp = client
        .get(&health_url)
        .send()
        .await
        .expect("health request");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("health json");
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["service"], "ks");

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_serves_https_health_in_prod_with_valid_tls_config() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let (cert_path, key_path) = write_test_tls_cert_pair(&tmp.path().to_path_buf());
    let config_path = write_ks_https_only_config(
        &tmp.path().to_path_buf(),
        port,
        "prod",
        &cert_path,
        &key_path,
    );
    let log_path = tmp.path().join("actrix-prod-https.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("https://127.0.0.1:{port}/ks/health");
    wait_for_health_https_insecure(&health_url, &mut child, &log_path).await;

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("create insecure client");
    let resp = client
        .get(&health_url)
        .send()
        .await
        .expect("health request");
    assert!(resp.status().is_success());
    let body: Value = resp.json().await.expect("health json");
    assert_eq!(body["status"], "healthy");
    assert_eq!(body["service"], "ks");

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_supervisord_serves_node_info_and_rejects_bad_signature() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let supervisor_port = choose_port();
    let config_path = write_config_with_supervisor(
        &tmp.path().to_path_buf(),
        port,
        "127.0.0.1",
        supervisor_port,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix-supervisord-auth.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let endpoint = format!("http://127.0.0.1:{supervisor_port}");
    let mut client = wait_for_supervisord(&endpoint, &mut child, &log_path).await;
    let shared_secret = hex::decode(TEST_SUPERVISOR_SHARED_SECRET).expect("decode shared secret");
    let valid_credential = build_node_info_credential(&shared_secret);

    let ok = client
        .get_node_info(GetNodeInfoRequest {
            credential: valid_credential.clone(),
        })
        .await
        .expect("get node info with valid credential")
        .into_inner();
    assert!(ok.success);
    assert_eq!(ok.node_id, TEST_SUPERVISOR_NODE_ID);
    assert_eq!(ok.location_tag, "local,test,supervisor");

    let mut bad_credential = valid_credential;
    bad_credential.signature.push('x');
    let err = client
        .get_node_info(GetNodeInfoRequest {
            credential: bad_credential,
        })
        .await
        .expect_err("bad signature should be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_supervisord_shutdown_rpc_stops_process() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let supervisor_port = choose_port();
    let config_path = write_config_with_supervisor(
        &tmp.path().to_path_buf(),
        port,
        "127.0.0.1",
        supervisor_port,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix-supervisord-shutdown.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let endpoint = format!("http://127.0.0.1:{supervisor_port}");
    let mut client = wait_for_supervisord(&endpoint, &mut child, &log_path).await;
    let shared_secret = hex::decode(TEST_SUPERVISOR_SHARED_SECRET).expect("decode shared secret");

    let resp = client
        .shutdown(ShutdownRequest {
            graceful: true,
            timeout_secs: Some(2),
            reason: Some("integration requested shutdown".to_string()),
            credential: build_shutdown_credential(&shared_secret),
        })
        .await
        .expect("shutdown rpc should succeed")
        .into_inner();
    assert!(resp.accepted, "shutdown rpc should be accepted");

    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("check child status") {
            assert!(
                status.success(),
                "process should exit successfully after shutdown rpc, got {status:?}"
            );
            return;
        }

        if start.elapsed() > START_TIMEOUT {
            let log = fs::read_to_string(&log_path).unwrap_or_default();
            graceful_shutdown(child);
            panic!("actrix should exit after supervisor shutdown rpc within timeout\nlogs:\n{log}");
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
#[serial]
async fn actrix_supervisord_rejects_bad_shutdown_signature_and_keeps_running() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let supervisor_port = choose_port();
    let config_path = write_config_with_supervisor(
        &tmp.path().to_path_buf(),
        port,
        "127.0.0.1",
        supervisor_port,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix-supervisord-bad-shutdown.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let endpoint = format!("http://127.0.0.1:{supervisor_port}");
    let mut client = wait_for_supervisord(&endpoint, &mut child, &log_path).await;
    let shared_secret = hex::decode(TEST_SUPERVISOR_SHARED_SECRET).expect("decode shared secret");
    let mut bad_credential = build_shutdown_credential(&shared_secret);
    bad_credential.signature.push('x');

    let err = client
        .shutdown(ShutdownRequest {
            graceful: true,
            timeout_secs: Some(1),
            reason: Some("bad signature".to_string()),
            credential: bad_credential,
        })
        .await
        .expect_err("bad shutdown signature should be rejected");
    assert_eq!(err.code(), Code::Unauthenticated);

    let health_resp = reqwest::get(&health_url).await.expect("health request");
    assert!(
        health_resp.status().is_success(),
        "process should keep running after rejected shutdown rpc"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_supervisord_rejects_duplicate_nonce_on_node_info_and_keeps_running() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let supervisor_port = choose_port();
    let config_path = write_config_with_supervisor(
        &tmp.path().to_path_buf(),
        port,
        "127.0.0.1",
        supervisor_port,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix-supervisord-replay.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let endpoint = format!("http://127.0.0.1:{supervisor_port}");
    let mut client = wait_for_supervisord(&endpoint, &mut child, &log_path).await;
    let shared_secret = hex::decode(TEST_SUPERVISOR_SHARED_SECRET).expect("decode shared secret");
    let replay_credential = build_node_info_credential(&shared_secret);

    let first = client
        .get_node_info(GetNodeInfoRequest {
            credential: replay_credential.clone(),
        })
        .await
        .expect("first node info request should pass")
        .into_inner();
    assert!(first.success);

    let replay_err = client
        .get_node_info(GetNodeInfoRequest {
            credential: replay_credential,
        })
        .await
        .expect_err("replayed nonce should be rejected");
    assert_eq!(replay_err.code(), Code::Unauthenticated);

    let health_resp = reqwest::get(&health_url).await.expect("health request");
    assert!(
        health_resp.status().is_success(),
        "process should keep running after replay rejection"
    );

    graceful_shutdown(child);
}

#[tokio::test]
#[serial]
async fn actrix_supervisord_rejects_stale_node_info_timestamp_and_keeps_running() {
    let tmp = tempfile::tempdir().expect("temp dir");
    let port = choose_port();
    let supervisor_port = choose_port();
    let config_path = write_config_with_supervisor(
        &tmp.path().to_path_buf(),
        port,
        "127.0.0.1",
        supervisor_port,
        "http://127.0.0.1:1",
    );
    let log_path = tmp.path().join("actrix-supervisord-stale-ts.log");
    let mut child = spawn_actrix(&config_path, &log_path);

    let health_url = format!("http://127.0.0.1:{port}/ks/health");
    wait_for_health(&health_url, &mut child, &log_path).await;

    let endpoint = format!("http://127.0.0.1:{supervisor_port}");
    let mut client = wait_for_supervisord(&endpoint, &mut child, &log_path).await;
    let shared_secret = hex::decode(TEST_SUPERVISOR_SHARED_SECRET).expect("decode shared secret");
    let stale_ts = (chrono::Utc::now().timestamp() as u64).saturating_sub(301);
    let stale_credential = build_node_info_credential_with_timestamp(&shared_secret, stale_ts);

    let stale_err = client
        .get_node_info(GetNodeInfoRequest {
            credential: stale_credential,
        })
        .await
        .expect_err("stale timestamp should be rejected");
    assert_eq!(stale_err.code(), Code::Unauthenticated);

    let health_resp = reqwest::get(&health_url).await.expect("health request");
    assert!(
        health_resp.status().is_success(),
        "process should keep running after stale timestamp rejection"
    );

    graceful_shutdown(child);
}
