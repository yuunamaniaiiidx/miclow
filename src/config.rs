use anyhow::Result;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TaskConfig {
    #[serde(rename = "task_name")]
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
    pub subscribe_topics: Option<Vec<String>>,
    pub stdout_topic: Option<String>,
    pub stderr_topic: Option<String>,
    #[serde(default)]
    pub view_stdout: bool,
    #[serde(default)]
    pub view_stderr: bool,
}

impl TaskConfig {
    pub fn get_stdout_topic(&self) -> String {
        self.stdout_topic.clone()
            .unwrap_or_else(|| format!("{}.stdout", self.name))
    }

    pub fn get_stderr_topic(&self) -> String {
        self.stderr_topic.clone()
            .unwrap_or_else(|| format!("{}.stderr", self.name))
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SystemConfig {
    #[serde(skip)]
    pub config_file: Option<String>,
    pub tasks: Vec<TaskConfig>,
    #[serde(default)]
    pub functions: Vec<TaskConfig>,
    #[serde(default)]
    pub include_paths: Vec<String>,
}

impl SystemConfig {
    pub fn from_toml(toml_content: &str) -> Result<Self> {
        let mut config: SystemConfig = toml::from_str(toml_content)?;
        config.config_file = None;
        config.normalize_defaults();
        config.validate()?;
        Ok(config)
    }
    
    pub fn from_file(config_file: String) -> Result<Self> {
        let config_content: String = std::fs::read_to_string(&config_file)?;
        let mut config = Self::from_toml(&config_content)?;
        config.config_file = Some(config_file.clone());
        
        config.load_includes(&config_file)?;
        config.validate()?;
        
        Ok(config)
    }
    
    pub fn get_all_tasks(&self) -> Vec<&TaskConfig> {
        self.tasks.iter().chain(self.functions.iter()).collect()
    }
    
    pub fn validate(&self) -> Result<()> {
        if self.tasks.is_empty() {
            return Err(anyhow::anyhow!("No tasks configured"));
        }
        
        for (index, task) in self.tasks.iter().enumerate() {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Task {} has empty name", index));
            }
            
            if task.command.is_empty() {
                return Err(anyhow::anyhow!("Task '{}' has empty command", task.name));
            }
            
            if let Some(working_dir) = &task.working_directory {
                if !std::path::Path::new(working_dir).exists() {
                    return Err(anyhow::anyhow!("Task '{}' working directory '{}' does not exist", task.name, working_dir));
                }
            }
            
            if let Some(subscribe_topics) = &task.subscribe_topics {
                for (topic_index, topic) in subscribe_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!("Task '{}' has empty initial topic at index {}", task.name, topic_index));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!("Task '{}' initial topic '{}' contains spaces (not allowed)", task.name, topic));
                    }
                }
            }

            if let Some(stdout_topic) = &task.stdout_topic {
                if stdout_topic.is_empty() {
                    return Err(anyhow::anyhow!("Task '{}' stdout_topic is empty", task.name));
                }
                if stdout_topic.contains(' ') {
                    return Err(anyhow::anyhow!("Task '{}' stdout_topic '{}' contains spaces (not allowed)", task.name, stdout_topic));
                }
            }
            if let Some(stderr_topic) = &task.stderr_topic {
                if stderr_topic.is_empty() {
                    return Err(anyhow::anyhow!("Task '{}' stderr_topic is empty", task.name));
                }
                if stderr_topic.contains(' ') {
                    return Err(anyhow::anyhow!("Task '{}' stderr_topic '{}' contains spaces (not allowed)", task.name, stderr_topic));
                }
            }
        }
        
        Ok(())
    }
    
    fn load_includes(&mut self, base_config_path: &str) -> Result<()> {
        let mut loaded_files = HashSet::new();
        let base_dir = std::path::Path::new(base_config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        
        let include_paths = self.include_paths.clone();
        self.load_includes_recursive(&mut loaded_files, base_dir, &include_paths)?;
        Ok(())
    }
    
    fn load_includes_recursive(
        &mut self,
        loaded_files: &mut HashSet<String>,
        base_dir: &std::path::Path,
        include_paths: &[String],
    ) -> Result<()> {
        for include_path in include_paths {
            let full_path = if std::path::Path::new(include_path).is_absolute() {
                include_path.clone()
            } else {
                base_dir.join(include_path).to_string_lossy().to_string()
            };
            
            if loaded_files.contains(&full_path) {
                log::warn!("Circular include detected for file: {}", full_path);
                continue;
            }
            
            if !std::path::Path::new(&full_path).exists() {
                log::warn!("Include file not found: {}", full_path);
                continue;
            }
            
            match std::fs::read_to_string(&full_path) {
                Ok(content) => {
                    loaded_files.insert(full_path.clone());
                    log::info!("Loading include file: {}", full_path);
                    
                    match Self::from_toml(&content) {
                        Ok(included_config) => {
                            self.tasks.extend(included_config.tasks);
                            if !included_config.include_paths.is_empty() {
                                let include_dir = std::path::Path::new(&full_path)
                                    .parent()
                                    .unwrap_or(std::path::Path::new("."));
                                
                                self.load_includes_recursive(
                                    loaded_files,
                                    include_dir,
                                    &included_config.include_paths,
                                )?;
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to parse include file {}: {}", full_path, e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to read include file {}: {}", full_path, e);
                }
            }
        }
        
        Ok(())
    }

    fn normalize_defaults(&mut self) {
        for task in self.tasks.iter_mut().chain(self.functions.iter_mut()) {
            if task.working_directory.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.working_directory = Some("./".to_string());
            }
            if task.view_stdout != true {
                task.view_stdout = false;
            }
            if task.view_stderr != true {
                task.view_stderr = false;
            }
        }
    }
}

