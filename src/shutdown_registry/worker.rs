use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait BackgroundWorker: Send + 'static {
    fn name(&self) -> &str;
    fn readiness(&self) -> WorkerReadiness {
        WorkerReadiness::Immediate
    }
    async fn run(self, ctx: BackgroundWorkerContext);
}

pub enum WorkerReadiness {
    Immediate,
    NeedsSignal,
}

pub struct ReadyHandle {
    sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl ReadyHandle {
    pub fn new(sender: tokio::sync::oneshot::Sender<()>) -> Self {
        Self {
            sender: Some(sender),
        }
    }
    pub fn noop() -> Self {
        Self { sender: None }
    }
    pub fn notify(&mut self) {
        if let Some(tx) = self.sender.take() {
            let _ = tx.send(());
        }
    }
}

pub struct BackgroundWorkerContext {
    pub shutdown: CancellationToken,
    pub ready: ReadyHandle,
}
