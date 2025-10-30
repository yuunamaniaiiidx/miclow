mod task;
mod buffer;
mod logging;

use anyhow::Result;
use task::{
    ServerConfig, MiclowServer
};
use clap::Parser;
use tokio::sync::mpsc;

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
    let config = ServerConfig::from_file(config_file)?;
    
    let miclow_server = MiclowServer::new(config);
    miclow_server.start_server().await?;

    Ok(())
}
