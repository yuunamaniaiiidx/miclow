use anyhow::Result;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TaskConfig {
    #[serde(rename = "task_name")]
    pub name: String,
    pub protocol: String,
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
    // Internal flags (not exposed in config file)
    #[serde(skip)]
    pub allow_duplicate: bool,
    #[serde(skip)]
    pub auto_start: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SystemConfig {
    #[serde(skip)]
    pub config_file: Option<String>,
    #[serde(default)]
    pub tasks: Vec<TaskConfig>,
    #[serde(default)]
    pub functions: Vec<TaskConfig>,
    #[serde(default)]
    pub include_paths: Vec<String>,
}

impl SystemConfig {
    pub fn from_toml(toml_content: &str) -> Result<Self> {
        Self::from_toml_internal(toml_content, true)
    }
    
    fn from_toml_internal(toml_content: &str, validate: bool) -> Result<Self> {
        let mut config: SystemConfig = toml::from_str(toml_content)?;
        config.config_file = None;
        config.normalize_defaults();
        if validate {
            config.validate().unwrap_or_else(|e| {
                eprintln!("Config validation failed: {}", e);
                eprintln!("\nError details:");
                for (i, cause) in e.chain().enumerate() {
                    eprintln!("  {}: {}", i, cause);
                }
                std::process::exit(1);
            });
        }
        Ok(config)
    }
    
    pub fn from_file(config_file: String) -> Result<Self> {
        let config_content: String = std::fs::read_to_string(&config_file)?;
        let mut config = Self::from_toml(&config_content)?;
        config.config_file = Some(config_file.clone());
        
        log::debug!("Before load_includes: {} tasks, {} functions", 
            config.tasks.len(), config.functions.len());
        config.load_includes(&config_file)?;
        log::debug!("After load_includes: {} tasks, {} functions", 
            config.tasks.len(), config.functions.len());
        config.validate().unwrap_or_else(|e| {
            eprintln!("Config validation failed: {}", e);
            eprintln!("\nError details:");
            for (i, cause) in e.chain().enumerate() {
                eprintln!("  {}: {}", i, cause);
            }
            std::process::exit(1);
        });
        
        Ok(config)
    }
    
    pub fn get_autostart_tasks(&self) -> Vec<&TaskConfig> {
        self.tasks.iter().chain(self.functions.iter())
            .filter(|task| task.auto_start)
            .collect()
    }
    
    pub fn validate(&self) -> Result<()> {
        let autostart_count = self.tasks.iter().filter(|t| t.auto_start).count()
            + self.functions.iter().filter(|t| t.auto_start).count();
        if autostart_count == 0 {
            return Err(anyhow::anyhow!("No autostart tasks configured"));
        }
        
        // Validate tasks
        for (index, task) in self.tasks.iter().enumerate() {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Task {} has empty name", index));
            }
            
            if task.name.starts_with("system") {
                return Err(anyhow::anyhow!("Task '{}' cannot start with 'system' (reserved for system tasks)", task.name));
            }
            
            if task.protocol.is_empty() {
                return Err(anyhow::anyhow!("Task '{}' has empty protocol", task.name));
            }
            
            // Protocol-specific validation is done in ProtocolBackend::from()
            // Here we only check basic requirements
            
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
        
        // Validate functions
        for (index, task) in self.functions.iter().enumerate() {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Function {} has empty name", index));
            }
            
            if task.protocol.is_empty() {
                return Err(anyhow::anyhow!("Function '{}' has empty protocol", task.name));
            }
            
            // Protocol-specific validation is done in ProtocolBackend::from()
            // Here we only check basic requirements
            
            if let Some(working_dir) = &task.working_directory {
                if !std::path::Path::new(working_dir).exists() {
                    return Err(anyhow::anyhow!("Function '{}' working directory '{}' does not exist", task.name, working_dir));
                }
            }
            
            if let Some(subscribe_topics) = &task.subscribe_topics {
                for (topic_index, topic) in subscribe_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!("Function '{}' has empty initial topic at index {}", task.name, topic_index));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!("Function '{}' initial topic '{}' contains spaces (not allowed)", task.name, topic));
                    }
                }
            }

            if let Some(stdout_topic) = &task.stdout_topic {
                if stdout_topic.is_empty() {
                    return Err(anyhow::anyhow!("Function '{}' stdout_topic is empty", task.name));
                }
                if stdout_topic.contains(' ') {
                    return Err(anyhow::anyhow!("Function '{}' stdout_topic '{}' contains spaces (not allowed)", task.name, stdout_topic));
                }
            }
            if let Some(stderr_topic) = &task.stderr_topic {
                if stderr_topic.is_empty() {
                    return Err(anyhow::anyhow!("Function '{}' stderr_topic is empty", task.name));
                }
                if stderr_topic.contains(' ') {
                    return Err(anyhow::anyhow!("Function '{}' stderr_topic '{}' contains spaces (not allowed)", task.name, stderr_topic));
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
                // Standard approach: resolve relative to the config file's directory
                // This is the most common pattern used by Docker Compose, Nginx, etc.
                base_dir.join(include_path).to_string_lossy().to_string()
            };
            
            if loaded_files.contains(&full_path) {
                log::warn!("Circular include detected for file: {}", full_path);
                continue;
            }
            
            if !std::path::Path::new(&full_path).exists() {
                log::warn!("Include file not found: {} (base_dir: {}, include_path: {})", 
                    full_path, base_dir.display(), include_path);
                continue;
            }
            
            match std::fs::read_to_string(&full_path) {
                Ok(content) => {
                    loaded_files.insert(full_path.clone());
                    log::info!("Loading include file: {}", full_path);
                    
                    match Self::from_toml_internal(&content, false) {
                        Ok(included_config) => {
                            // from_toml_internal already calls normalize_defaults()
                            log::info!("Loaded {} tasks and {} functions from {}", 
                                included_config.tasks.len(), 
                                included_config.functions.len(),
                                full_path);
                            for func in &included_config.functions {
                                log::info!("  Function: {}", func.name);
                            }
                            self.tasks.extend(included_config.tasks);
                            self.functions.extend(included_config.functions);
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
        // Tasks: allow_duplicate = false, auto_start = true
        for task in self.tasks.iter_mut() {
            if task.working_directory.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.working_directory = Some("./".to_string());
            }
            if task.view_stdout != true {
                task.view_stdout = false;
            }
            if task.view_stderr != true {
                task.view_stderr = false;
            }
            task.allow_duplicate = false;
            task.auto_start = true;
        }
        
        // Functions: allow_duplicate = true, auto_start = false
        for task in self.functions.iter_mut() {
            if task.working_directory.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.working_directory = Some("./".to_string());
            }
            if task.view_stdout != true {
                task.view_stdout = false;
            }
            if task.view_stderr != true {
                task.view_stderr = false;
            }
            task.allow_duplicate = true;
            task.auto_start = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_load_functions_from_include() {
        let temp_dir = TempDir::new().unwrap();
        let base_dir = temp_dir.path();
        
        // Create include file with functions
        let include_file = base_dir.join("include.toml");
        fs::write(&include_file, r#"
[[functions]]
task_name = "test_function"
command = "echo"
args = ["test"]
"#).unwrap();
        
        // Create main config file
        let main_file = base_dir.join("main.toml");
        fs::write(&main_file, format!(r#"
include_paths = ["include.toml"]

[[tasks]]
task_name = "main_task"
command = "echo"
args = ["main"]
"#)).unwrap();
        
        let config = SystemConfig::from_file(main_file.to_string_lossy().to_string()).unwrap();
        
        assert_eq!(config.functions.len(), 1, "Should have 1 function from include file");
        assert_eq!(config.functions[0].name, "test_function");
        assert_eq!(config.tasks.len(), 1, "Should have 1 task from main file");
        assert_eq!(config.tasks[0].name, "main_task");
    }
}
