use tokio::sync::mpsc;

use crate::pod::PodId;

/// タスク終了通知の送信側（PodIdを送信）
#[derive(Clone, Debug)]
pub struct TaskExitSender {
    sender: mpsc::UnboundedSender<PodId>,
}

impl TaskExitSender {
    pub fn new(sender: mpsc::UnboundedSender<PodId>) -> Self {
        Self { sender }
    }

    pub fn send(&self, pod_id: PodId) -> Result<(), mpsc::error::SendError<PodId>> {
        self.sender.send(pod_id)
    }
}

/// タスク終了通知の受信側
pub struct TaskExitReceiver {
    receiver: mpsc::UnboundedReceiver<PodId>,
}

impl TaskExitReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<PodId>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<PodId> {
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
        let (tx, rx) = mpsc::unbounded_channel::<PodId>();
        Self {
            sender: TaskExitSender::new(tx),
            receiver: TaskExitReceiver::new(rx),
        }
    }
}
