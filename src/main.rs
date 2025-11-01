mod task;
mod buffer;
mod logging;

use anyhow::Result;
use task::{
    SystemConfig, MiclowSystem
};
use clap::Parser;
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
    
    run_miclow(cli.config).await?;

    Ok(())
}

async fn run_miclow(config_file: String) -> Result<()> {
    let config = SystemConfig::from_file(config_file)?;
    
    let miclow_system = MiclowSystem::new(config);
    miclow_system.start_system_with_interactive().await?;

    exit(0);
    Ok(())
}
