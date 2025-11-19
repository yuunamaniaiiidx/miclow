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

            if let Some(subscribe_topics) = &task.subscribe_topics {
                for (topic_index, topic) in subscribe_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!(
                            "Task '{}' has empty initial topic at index {}",
                            task.name,
                            topic_index
                        ));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!(
                            "Task '{}' initial topic '{}' contains spaces (not allowed)",
                            task.name,
                            topic
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}
