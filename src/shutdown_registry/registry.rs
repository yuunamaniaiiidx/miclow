use crate::shutdown_registry::worker::{
    BackgroundWorker, BackgroundWorkerContext, ReadyHandle, WorkerReadiness,
};
use crate::shutdown_registry::task_handle::{TaskHandle, TaskInfo};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use std::sync::Arc;
use tokio::sync::Mutex;

/// すべてのタスクを統括管理するレジストリ
pub struct ShutdownRegistry {
    tasks: Arc<Mutex<Vec<TaskInfo>>>,
    handle_sender: mpsc::UnboundedSender<TaskInfo>,
    manager_handle: Option<JoinHandle<()>>,
    shutdown_token: CancellationToken,
}

impl ShutdownRegistry {
    pub fn new() -> Self {
        Self::with_shutdown_token(CancellationToken::new())
    }

    pub fn with_shutdown_token(shutdown_token: CancellationToken) -> Self {
        let (handle_sender, mut handle_receiver) = mpsc::unbounded_channel();
        let tasks = Arc::new(Mutex::new(Vec::new()));
        
        // 統括タスクを開始
        let tasks_clone = tasks.clone();
        let manager_handle = tokio::spawn(async move {
            while let Some(task_info) = handle_receiver.recv().await {
                tasks_clone.lock().await.push(task_info);
            }
        });

        Self {
            tasks,
            handle_sender,
            manager_handle: Some(manager_handle),
            shutdown_token,
        }
    }

    /// グローバルなshutdown_tokenを取得
    pub fn shutdown_token(&self) -> &CancellationToken {
        &self.shutdown_token
    }

    /// 新しいTaskHandleを作成
    /// このTaskHandleを使ってタスクをspawnできる
    pub fn create_task_handle(&self) -> TaskHandle {
        TaskHandle::new(self.handle_sender.clone())
    }

    /// BackgroundWorkerを登録（既存のAPIとの互換性のため）
    pub async fn register_worker<W>(&mut self, worker: W)
    where
        W: BackgroundWorker,
    {
        let task_handle = self.create_task_handle();
        let name = worker.name().to_string();
        let cancel_token = task_handle.cancel_token().clone();
        
        let (ready_handle, mut ready_rx) = match worker.readiness() {
            WorkerReadiness::Immediate => (ReadyHandle::noop(), None),
            WorkerReadiness::NeedsSignal => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                (ReadyHandle::new(tx), Some(rx))
            }
        };

        let ctx = BackgroundWorkerContext {
            shutdown: cancel_token.clone(),
            ready: ready_handle,
        };

        let _handle = task_handle.run(name.clone(), async move {
            worker.run(ctx).await;
        }).expect("Failed to send task info");

        // NeedsSignalの場合は準備完了を待つ
        if let Some(ref mut ready_rx) = ready_rx {
            let _ = ready_rx.await;
        }
    }

    /// すべてのタスクをgraceful shutdown
    pub async fn shutdown_all(&mut self, timeout: std::time::Duration) {
        // グローバルなshutdown_tokenをキャンセル
        self.shutdown_token.cancel();
        
        // 統括タスクの受信を停止（senderをdrop）
        drop(self.handle_sender.clone());
        
        // すべてのタスク情報を取得
        let mut tasks = self.tasks.lock().await;
        
        // すべてのCancellationTokenをキャンセル
        for task_info in tasks.iter() {
            task_info.cancel_token.cancel();
        }

        // すべてのタスクが完了するまで待機
        let handles: Vec<(String, Arc<JoinHandle<()>>)> = tasks
            .drain(..)
            .map(|info| (info.name, info.handle))
            .collect();
        drop(tasks);

        for (idx, (name, h)) in handles.into_iter().enumerate() {
            log::info!(
                "Waiting task {} ({}) up to {:?}",
                idx,
                name,
                timeout
            );
            // Arcから参照を取得してawait
            let finished_in_time = tokio::time::timeout(timeout, async {
            }).await.is_ok();
            if !finished_in_time {
                log::warn!(
                    "Task {} ({}) did not finish in {:?}, aborting",
                    idx,
                    name,
                    timeout
                );
                h.abort();
            }
        }

        // 統括タスクも終了
        if let Some(handle) = self.manager_handle.take() {
            handle.abort();
        }
    }

    /// すべてのタスクを即座にabort
    pub async fn abort_all(&mut self) {
        // グローバルなshutdown_tokenをキャンセル
        self.shutdown_token.cancel();
        
        drop(self.handle_sender.clone());
        
        let mut tasks = self.tasks.lock().await;
        
        for task_info in tasks.iter() {
            task_info.cancel_token.cancel();
        }
        
        let handles: Vec<Arc<JoinHandle<()>>> = tasks
            .drain(..)
            .map(|info| info.handle)
            .collect();
        drop(tasks);
        
        for h in handles {
            h.abort();
        }

        if let Some(handle) = self.manager_handle.take() {
            handle.abort();
        }
    }
}

