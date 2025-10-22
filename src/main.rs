mod task;
mod buffer;

use anyhow::Result;
use task::{
    ServerConfig, MiclowServer
};
use clap::Parser;

#[derive(Parser)]
#[command(name = "miclow")]
#[command(about = "Miclow - A lightweight orchestration system for asynchronous task execution")]
pub struct Cli {
    #[arg(long, default_value = "config.toml")]
    pub config_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let cli = Cli::parse();
    
    run_miclow(cli.config_file).await?;

    Ok(())
}

async fn run_miclow(config_file: String) -> Result<()> {
    let config = ServerConfig::from_file(config_file)?;
    
    let miclow_server = MiclowServer::new(config);
    miclow_server.start_server().await?;

    Ok(())
}
