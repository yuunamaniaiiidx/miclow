use crate::config::SystemConfig;
use anyhow::Result;

impl SystemConfig {
    /// タスクのプロトコル非依存バリデーション
    pub(crate) fn validate_tasks(&self) -> Result<()> {
        for (name, task) in &self.tasks {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Task '{}' has empty name", name));
            }

            if task.name.starts_with("system") {
                return Err(anyhow::anyhow!(
                    "Task '{}' cannot start with 'system' (reserved for system tasks)",
                    task.name
                ));
            }
        }
        Ok(())
    }
}
