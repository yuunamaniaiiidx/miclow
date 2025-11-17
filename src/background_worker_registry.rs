use async_trait::async_trait;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Backend 系など独自にライフサイクルを管理するコンポーネントは登録対象外とし、
/// ここでは Miclow 本体が管理するバックグラウンドワーカーのみ扱う。
#[async_trait]
pub trait BackgroundWorker: Send + 'static {
    fn name(&self) -> &str;
    async fn run(self, shutdown: CancellationToken);
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

    pub async fn register_worker<W>(
        &mut self,
        worker: W,
        mut ready: Option<tokio::sync::oneshot::Receiver<()>>,
    ) where
        W: BackgroundWorker,
    {
        let name = worker.name().to_string();
        let shutdown = self.shutdown_token.clone();
        let handle = tokio::spawn(async move {
            worker.run(shutdown).await;
        });
        self.handles.push((name, handle));
        if let Some(ref mut ready_rx) = ready {
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
