use crate::backend::create_protocol_backend;
use crate::config::expansion::{ExpandContext, Expandable};
use crate::config::value_conversion::FromTomlValue;
use crate::config::{
    expand_toml_value, ExpandedTaskConfig, LifecycleConfig, RawTaskConfig, TaskConfig,
};
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use toml::Value as TomlValue;

impl RawTaskConfig {
    pub fn expand(self, context: &ExpandContext) -> Result<ExpandedTaskConfig> {
        let expanded_name = expand_toml_value(&self.name, context)?;
        let name_str = String::from_toml_value(&expanded_name)
            .ok_or_else(|| anyhow::anyhow!("task_name must be a string"))?;
        let name = Arc::from(name_str);

        let view_stdout = expand_bool_field(self.view_stdout, context, "view_stdout")?
            .expect("view_stdout should be set by normalize_defaults()");

        let view_stderr = expand_bool_field(self.view_stderr, context, "view_stderr")?
            .expect("view_stderr should be set by normalize_defaults()");

        let lifecycle = if let Some(raw_lifecycle) = self.lifecycle {
            raw_lifecycle.expand(context)?
        } else {
            LifecycleConfig::default()
        };

        let mut protocol_config = HashMap::new();
        for (key, value) in self.protocol_config {
            let expanded_key = key.expand(context)?;
            let expanded_value = expand_toml_value(&value, context)?;
            protocol_config.insert(expanded_key, expanded_value);
        }

        Ok(ExpandedTaskConfig {
            name,
            view_stdout,
            view_stderr,
            lifecycle,
            protocol_config,
        })
    }

    pub fn expand_and_create_backend(self, context: &ExpandContext) -> Result<TaskConfig> {
        let expanded_protocol = expand_toml_value(&self.protocol, context)?;
        let protocol = String::from_toml_value(&expanded_protocol)
            .ok_or_else(|| anyhow::anyhow!("protocol must be a string"))?;

        let expanded = self.expand(context)?;

        let protocol_backend = create_protocol_backend(&protocol, &expanded)?;

        Ok(TaskConfig {
            name: expanded.name,
            view_stdout: expanded.view_stdout,
            view_stderr: expanded.view_stderr,
            lifecycle: expanded.lifecycle,
            protocol_config: expanded.protocol_config,
            protocol_backend,
        })
    }
}

fn expand_bool_field(
    raw_value: Option<TomlValue>,
    context: &ExpandContext,
    field_name: &str,
) -> Result<Option<bool>> {
    raw_value
        .map(|raw_value| {
            let expanded_value = expand_toml_value(&raw_value, context)?;
            bool::from_toml_value(&expanded_value)
                .ok_or_else(|| anyhow::anyhow!("{} must be a boolean", field_name))
        })
        .transpose()
}
