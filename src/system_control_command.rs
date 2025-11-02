#[derive(Debug, Clone)]
pub enum SystemControlCommand {
    SubscribeTopic { topic: String },
    UnsubscribeTopic { topic: String },
    StartTask { task_name: String },
    StopTask { task_name: String },
    AddTaskFromToml { toml_data: String },
    Status,
    CallFunction { task_name: String, initial_input: Option<String> },
    Unknown { command: String, data: String },
}
