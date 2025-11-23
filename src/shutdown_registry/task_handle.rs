use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tokio::sync::mpsc;
use std::sync::Arc;

/// タスク情報を統括タスクに送るためのメッセージ
#[derive(Debug)]
pub struct TaskInfo {
    pub name: String,
    pub handle: Arc<JoinHandle<()>>,
    pub cancel_token: CancellationToken,
}

/// タスクをspawnするためのハンドル
/// handle_senderを通じて統括タスクに登録される
pub struct TaskHandle {
    handle_sender: mpsc::UnboundedSender<TaskInfo>,
    cancel_token: CancellationToken,
}

impl TaskHandle {
    pub fn new(handle_sender: mpsc::UnboundedSender<TaskInfo>) -> Self {
        Self {
            handle_sender,
            cancel_token: CancellationToken::new(),
        }
    }

    /// タスクをspawnし、JoinHandleとCancellationTokenを統括タスクに送る
    /// JoinHandleを返す（呼び出し側で管理する場合）
    pub fn run<F>(&self, name: impl Into<String>, task: F) -> Result<Arc<JoinHandle<()>>, mpsc::error::SendError<TaskInfo>>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let name = name.into();
        let cancel_token = self.cancel_token.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    // キャンセルされた場合は終了
                },
                _ = task => {
                    // タスクが完了した場合は終了
                },
            }
        });

        let handle_arc = Arc::new(handle);
        self.handle_sender.send(TaskInfo {
            name,
            handle: handle_arc.clone(),
            cancel_token: self.cancel_token.clone(),
        })?;
        Ok(handle_arc)
    }

    /// このタスク用のCancellationTokenを取得
    pub fn cancel_token(&self) -> &CancellationToken {
        &self.cancel_token
    }
}

