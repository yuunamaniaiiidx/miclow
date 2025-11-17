use crate::channels::{InputSender, ShutdownSender};

pub struct SpawnBackendResult {
    pub worker_handle: tokio::task::JoinHandle<()>,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}
