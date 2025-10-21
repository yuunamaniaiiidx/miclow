mod task;
mod buffer;

use anyhow::Result;
use task::{
    ServerConfig, CotoServer
};
use clap::Parser;

#[derive(Parser)]
#[command(name = "coto")]
#[command(about = "Coto - Interactive coding tool")]
pub struct Cli {
    #[arg(long, default_value = "config.toml")]
    pub config_file: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    
    let cli = Cli::parse();
    
    run_coto(cli.config_file).await?;

    Ok(())
}

async fn run_coto(config_file: String) -> Result<()> {
    let config = ServerConfig::from_file(config_file)?;
    
    let coto_server = CotoServer::new(config);
    coto_server.start_server().await?;

    Ok(())
}
