//! 命令行界面定义
//!
//! 定义了主程序的命令行参数和选项
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "aux-servers", version)]
#[command(
    about = "Collection of WebRTC auxiliary servers including Signaling, STUN and TURN services"
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Commands>,

    /// Configuration file path (defaults to searching standard locations)
    #[arg(short, long, default_value = "config.toml")]
    pub(crate) config: PathBuf,
}

#[derive(Subcommand, Debug)]
pub(crate) enum Commands {
    /// Test configuration file
    Test {
        /// Configuration file path (optional, defaults to config.toml)
        #[arg(index = 1)]
        config_file: Option<PathBuf>,
    },
}
