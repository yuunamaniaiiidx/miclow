use anyhow::Result;
use clap::Parser;
use miclow::config::SystemConfig;
use miclow::miclow::MiclowSystem;
use std::process::exit;

#[derive(Parser)]
#[command(name = "miclow")]
#[command(about = "Miclow - A lightweight orchestration system for asynchronous task execution")]
pub struct Cli {
    #[arg(value_name = "CONFIG", help = "Configuration file path")]
    pub config: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = SystemConfig::from_file(cli.config)?;

    let miclow_system = MiclowSystem::new(config);
    miclow_system.start_system().await?;

    exit(0);
}