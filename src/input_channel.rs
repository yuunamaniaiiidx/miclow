use tokio::sync::mpsc;
use anyhow::Result;

pub trait StdinProtocol: Send + Sync {
    fn to_input_lines(&self) -> Vec<String> {
        let lines = self.to_input_lines_raw();
        
        if lines.len() < 2 {
            panic!("StdinProtocol validation failed: must have at least 2 lines, got {}", lines.len());
        }
        
        let line_count: usize = lines[1].parse()
            .unwrap_or_else(|_| {
                panic!("StdinProtocol validation failed: line 2 must be a number, got '{}'", lines[1]);
            });
        
        let data_line_count = lines.len() - 2;
        if data_line_count != line_count {
            panic!(
                "StdinProtocol validation failed: expected {} data lines (from line 2), but got {} (total lines: {})",
                line_count, data_line_count, lines.len()
            );
        }
        
        lines
    }
    
    fn to_input_lines_raw(&self) -> Vec<String>;
}

#[derive(Clone, Debug)]
pub struct TopicMessage {
    pub topic: String,
    pub data: String,
}

impl StdinProtocol for TopicMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let mut lines = vec![self.topic.clone()];
        let data_lines: Vec<&str> = self.data.lines().collect();
        lines.push(data_lines.len().to_string());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

#[derive(Clone, Debug)]
pub struct SystemResponseMessage {
    pub topic: String,
    pub status: String,
    pub data: String,
}

impl StdinProtocol for SystemResponseMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let mut lines = vec![self.topic.clone()];
        let data_lines: Vec<&str> = self.data.lines().collect();
        lines.push((data_lines.len() + 1).to_string());
        lines.push(self.status.clone());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

#[derive(Clone, Debug)]
pub struct ReturnMessage {
    pub data: String,
}

impl StdinProtocol for ReturnMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let data_lines: Vec<&str> = self.data.lines().collect();
        let mut lines = vec!["system.return".to_string(), data_lines.len().to_string()];
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

#[derive(Clone, Debug)]
pub struct FunctionMessage {
    pub task_name: String,
    pub data: String,
}

impl StdinProtocol for FunctionMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let data_lines: Vec<&str> = self.data.lines().collect();
        let mut lines = vec!["system.function".to_string(), (data_lines.len() + 1).to_string()];
        lines.push(self.task_name.clone());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

#[derive(Clone, Debug)]
pub enum InputDataMessage {
    Topic(TopicMessage),
    SystemResponse(SystemResponseMessage),
    Return(ReturnMessage),
    Function(FunctionMessage),
}

impl StdinProtocol for InputDataMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        match self {
            InputDataMessage::Topic(msg) => msg.to_input_lines_raw(),
            InputDataMessage::SystemResponse(msg) => msg.to_input_lines_raw(),
            InputDataMessage::Return(msg) => msg.to_input_lines_raw(),
            InputDataMessage::Function(msg) => msg.to_input_lines_raw(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InputSender {
    sender: mpsc::UnboundedSender<InputDataMessage>,
}

impl InputSender {
    pub fn new(sender: mpsc::UnboundedSender<InputDataMessage>) -> Self {
        Self { sender }
    }

    pub fn send(&self, input: InputDataMessage) -> Result<(), mpsc::error::SendError<InputDataMessage>> {
        self.sender.send(input)
    }
}

pub struct InputReceiver {
    receiver: mpsc::UnboundedReceiver<InputDataMessage>,
}

impl InputReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<InputDataMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<InputDataMessage> {
        self.receiver.recv().await
    }
}

pub struct InputChannel {
    pub sender: InputSender,
    pub receiver: InputReceiver,
}

impl InputChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<InputDataMessage>();
        Self {
            sender: InputSender::new(tx),
            receiver: InputReceiver::new(receiver),
        }
    }
}
