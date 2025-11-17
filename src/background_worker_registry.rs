use tokio::task::JoinHandle;

#[derive(Default)]
pub struct BackgroundWorkerRegistry {
    handles: Vec<(String, JoinHandle<()>)>,
}

impl BackgroundWorkerRegistry {
    pub fn new() -> Self {
        Self {
            handles: Vec::new(),
        }
    }

    pub fn register(&mut self, name: &str, handle: JoinHandle<()>) {
        self.handles.push((name.to_string(), handle));
    }

    pub async fn shutdown_all(&mut self, timeout: std::time::Duration) {
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
        let mut handles = std::mem::take(&mut self.handles);
        for (_name, h) in handles.drain(..) {
            h.abort();
            let _ = h.await;
        }
    }
}
