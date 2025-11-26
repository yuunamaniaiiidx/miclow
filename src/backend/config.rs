use crate::backend::interactive::{try_interactive_from_expanded_config, InteractiveConfig};
use crate::backend::mcp::{try_mcp_from_expanded_config, MCPConfig};
use crate::backend::miclowstdio::{try_miclow_stdio_from_expanded_config, MiclowStdIOConfig};
use crate::backend::ProtocolBackend;
use crate::config::ExpandedTaskConfig;
use anyhow::{anyhow, Result};

pub trait BackendConfigMeta {
    fn protocol_name() -> &'static str;

    fn default_view_stdout() -> bool;

    fn default_view_stderr() -> bool;
}

pub fn get_default_view_stdout(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_view_stdout()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_view_stdout()),
        "MCP" => Ok(MCPConfig::default_view_stdout()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_view_stdout. Supported sections: [[Interactive]], [[MiclowStdIO]], [[MCP]]",
            section_name
        )),
    }
}

pub fn get_default_view_stderr(section_name: &str) -> Result<bool, String> {
    match section_name {
        "Interactive" => Ok(InteractiveConfig::default_view_stderr()),
        "MiclowStdIO" => Ok(MiclowStdIOConfig::default_view_stderr()),
        "MCP" => Ok(MCPConfig::default_view_stderr()),
        _ => Err(format!(
            "Unknown section name '{}' for get_default_view_stderr. Supported sections: [[Interactive]], [[MiclowStdIO]], [[MCP]]",
            section_name
        )),
    }
}

pub fn create_protocol_backend(
    protocol: &str,
    expanded: &ExpandedTaskConfig,
) -> Result<ProtocolBackend> {
    match protocol.trim() {
        "MiclowStdIO" => {
            let backend_config = try_miclow_stdio_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::MiclowStdIO(backend_config))
        }
        "Interactive" => {
            let backend_config = try_interactive_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::Interactive(backend_config))
        }
        "MCP" => {
            let backend_config = try_mcp_from_expanded_config(expanded)?;
            Ok(ProtocolBackend::MCP(backend_config))
        }
        _ => Err(anyhow!(
            "Unknown protocol '{}' for task '{}'. Supported protocols: MiclowStdIO, Interactive, MCP",
            protocol,
            expanded.name
        )),
    }
}
