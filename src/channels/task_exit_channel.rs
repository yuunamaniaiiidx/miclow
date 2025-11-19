use tokio::sync::mpsc;

/// タスク終了通知の送信側（タスク名を送信）
#[derive(Clone, Debug)]
pub struct TaskExitSender {
    sender: mpsc::UnboundedSender<String>,
}

impl TaskExitSender {
    pub fn new(sender: mpsc::UnboundedSender<String>) -> Self {
        Self { sender }
    }

    pub fn send(&self, task_name: String) -> Result<(), mpsc::error::SendError<String>> {
        self.sender.send(task_name)
    }
}

/// タスク終了通知の受信側
pub struct TaskExitReceiver {
    receiver: mpsc::UnboundedReceiver<String>,
}

impl TaskExitReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<String>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<String> {
        self.receiver.recv().await
    }
}

/// タスク終了通知チャネル
pub struct TaskExitChannel {
    pub sender: TaskExitSender,
    pub receiver: TaskExitReceiver,
}

impl TaskExitChannel {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        Self {
            sender: TaskExitSender::new(tx),
            receiver: TaskExitReceiver::new(rx),
        }
    }
}

