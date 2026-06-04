//! Standalone `mock-actrix` binary — starts a mock actrix server on a given
//! port and blocks until Ctrl+C. Used by `bindings/web/examples/echo`'s
//! `start-mock.sh` to run integration e2e tests without a real actrix.

use actr_mock_actrix::MockActrixServer;
use clap::Parser;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    author,
    version,
    about = "Mock actrix server for Actor-RTC integration tests"
)]
struct Args {
    /// TCP port to bind. Defaults to 8081 to match the echo e2e config.
    #[arg(long, default_value_t = 8081)]
    port: u16,

    /// Bind address (default 0.0.0.0 for accessibility from emulators).
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// Log filter (defaults to `info` for mock-actrix, `warn` elsewhere).
    #[arg(long, default_value = "actr_mock_actrix=info,info")]
    log: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&args.log))
        .with_target(true)
        .init();

    let listener = TcpListener::bind(format!("{}:{}", args.bind, args.port)).await?;
    let server = MockActrixServer::start_with_listener(listener).await?;

    // This log line is the readiness marker scripts grep for.
    println!("mock-actrix listening on 127.0.0.1:{}", server.port());
    println!("  http: {}", server.http_url());
    println!("  ws:   {}", server.ws_url());

    tokio::signal::ctrl_c().await?;
    println!("mock-actrix shutting down");
    Ok(())
}
