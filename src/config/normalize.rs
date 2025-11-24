use crate::backend::{get_default_view_stderr, get_default_view_stdout};
use crate::config::RawTaskConfig;
use anyhow::Result;
use toml::Value as TomlValue;

impl RawTaskConfig {
    pub(crate) fn normalize_defaults(&mut self, backend_name: &str) -> Result<()> {
        if self.view_stdout.is_none() {
            let default_value =
                get_default_view_stdout(backend_name).map_err(|e| anyhow::anyhow!("{}", e))?;
            self.view_stdout = Some(TomlValue::Boolean(default_value));
        }
        if self.view_stderr.is_none() {
            let default_value =
                get_default_view_stderr(backend_name).map_err(|e| anyhow::anyhow!("{}", e))?;
            self.view_stderr = Some(TomlValue::Boolean(default_value));
        }

        Ok(())
    }
}
