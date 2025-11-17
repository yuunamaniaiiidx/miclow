mod backend;
mod background_worker_registry;
mod channels;
mod config;
mod logging;
mod message_id;
mod messages;
mod miclow;
mod system_control;
mod task_id;
mod task_runtime;
mod topic_broker;

use crate::config::SystemConfig;
use anyhow::Result;
use clap::Parser;
use miclow::MiclowSystem;
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
    miclow_system.start_system().await?;

    exit(0);
    Ok(())
}
