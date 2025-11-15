mod task_id;
mod message_id;
mod executor_event_channel;
mod input_channel;
mod system_response_channel;
mod shutdown_channel;
mod user_log_sender;
mod system_control_command;
mod backend;
mod protocol;
mod running_task;
mod start_context;
mod system_control_manager;
mod topic_manager;
mod background_task_manager;
mod config;
mod variable_expansion;
mod miclow;
mod logging;

use anyhow::Result;
use crate::config::SystemConfig;
use miclow::MiclowSystem;
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
    miclow_system.start_system().await?;

    exit(0);
    Ok(())
}
