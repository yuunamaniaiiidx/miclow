use crate::channels::{ExecutorInputEventSender, ShutdownSender};

pub struct SpawnBackendResult {
    pub worker_handle: tokio::task::JoinHandle<()>,
    pub input_sender: ExecutorInputEventSender,
    pub shutdown_sender: ShutdownSender,
}
