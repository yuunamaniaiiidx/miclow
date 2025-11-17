use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Backend 系など独自にライフサイクルを管理するコンポーネントは登録対象外とし、
/// ここでは Miclow 本体が管理するバックグラウンドワーカーのみ扱う。
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

pub struct BackgroundWorkerRegistry {
    handles: Vec<(String, JoinHandle<()>)>,
    shutdown_token: CancellationToken,
}

impl BackgroundWorkerRegistry {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            handles: Vec::new(),
            shutdown_token,
        }
    }

    pub async fn register_worker<W>(&mut self, worker: W)
    where
        W: BackgroundWorker,
    {
        let name = worker.name().to_string();
        let shutdown = self.shutdown_token.clone();
        let (ready_handle, mut ready_rx) = match worker.readiness() {
            WorkerReadiness::Immediate => (ReadyHandle::noop(), None),
            WorkerReadiness::NeedsSignal => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                (ReadyHandle::new(tx), Some(rx))
            }
        };
        let handle = tokio::spawn(async move {
            let ctx = BackgroundWorkerContext {
                shutdown,
                ready: ready_handle,
            };
            worker.run(ctx).await;
        });
        self.handles.push((name, handle));
        if let Some(ref mut ready_rx) = ready_rx {
            let _ = ready_rx.await;
        }
    }

    pub async fn shutdown_all(&mut self, timeout: std::time::Duration) {
        self.shutdown_token.cancel();
        let mut handles = std::mem::take(&mut self.handles);
        for (idx, (name, mut h)) in handles.drain(..).enumerate() {
            log::info!(
                "Waiting background worker {} ({}) up to {:?}",
                idx,
                name,
                timeout
            );
            let finished_in_time = tokio::time::timeout(timeout, &mut h).await.is_ok();
            if !finished_in_time {
                log::warn!(
                    "Background worker {} ({}) did not finish in {:?}, aborting",
                    idx,
                    name,
                    timeout
                );
                h.abort();
                let _ = h.await;
            }
        }
    }

    pub async fn abort_all(&mut self) {
        self.shutdown_token.cancel();
        let mut handles = std::mem::take(&mut self.handles);
        for (_name, h) in handles.drain(..) {
            h.abort();
            let _ = h.await;
        }
    }
}
