use anyhow::Result;

use super::expand_toml_value;
use super::expansion::ExpandContext;
use super::value_conversion::FromTomlValue;
use toml::Value as TomlValue;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleMode {
    RoundRobin,
}

impl LifecycleMode {
    pub fn from_str(value: &str) -> Result<Self> {
        match value.to_lowercase().as_str() {
            "round_robin" => Ok(Self::RoundRobin),
            other => Err(anyhow::anyhow!(
                "Unknown lifecycle.mode '{}'. Supported modes: round_robin",
                other
            )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LifecycleConfig {
    pub desired_instances: u32,
    pub mode: LifecycleMode,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            desired_instances: 1,
            mode: LifecycleMode::RoundRobin,
        }
    }
}

impl LifecycleConfig {
    pub fn with_desired_instances(self, value: u32) -> Self {
        Self {
            desired_instances: value,
            ..self
        }
    }

    pub fn with_mode(self, mode: LifecycleMode) -> Self {
        Self { mode, ..self }
    }
}

#[derive(Debug, Clone)]
pub struct RawLifecycleConfig {
    pub desired_instances: Option<TomlValue>,
    pub mode: Option<TomlValue>,
}

impl RawLifecycleConfig {
    pub fn from_entry(entry: LifecycleEntry) -> Result<Self> {
        let mut desired_instances = None;
        let mut mode = None;

        for (key, value) in entry.fields {
            match key.as_str() {
                "desired_instances" => {
                    if desired_instances.is_some() {
                        return Err(anyhow::anyhow!(
                            "Duplicate lifecycle field 'desired_instances'"
                        ));
                    }
                    desired_instances = Some(value);
                }
                "mode" => {
                    if mode.is_some() {
                        return Err(anyhow::anyhow!("Duplicate lifecycle field 'mode'"));
                    }
                    mode = Some(value);
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "Unknown lifecycle field '{}'. Supported fields: desired_instances, mode",
                        other
                    ))
                }
            }
        }

        Ok(Self {
            desired_instances,
            mode,
        })
    }

    pub fn expand(self, context: &ExpandContext) -> Result<LifecycleConfig> {
        let mut lifecycle = LifecycleConfig::default();

        if let Some(raw_desired) = self.desired_instances {
            let expanded_value = expand_toml_value(&raw_desired, context)?;
            let desired = u32::from_toml_value(&expanded_value).ok_or_else(|| {
                anyhow::anyhow!("lifecycle.desired_instances must be an integer (>= 0)")
            })?;
            if desired == 0 {
                return Err(anyhow::anyhow!(
                    "lifecycle.desired_instances must be greater than 0"
                ));
            }
            lifecycle = lifecycle.with_desired_instances(desired);
        }

        if let Some(raw_mode) = self.mode {
            let expanded_value = expand_toml_value(&raw_mode, context)?;
            let mode_str = String::from_toml_value(&expanded_value)
                .ok_or_else(|| anyhow::anyhow!("lifecycle.mode must be a string"))?;
            let mode = LifecycleMode::from_str(&mode_str)?;
            lifecycle = lifecycle.with_mode(mode);
        }

        Ok(lifecycle)
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct LifecycleEntry {
    #[serde(flatten)]
    pub fields: std::collections::HashMap<String, TomlValue>,
}
