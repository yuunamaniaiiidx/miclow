use crate::chunnel::InputSender;
use crate::chunnel::ShutdownSender;

pub struct SpawnBackendResult {
    pub worker_handle: tokio::task::JoinHandle<()>,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}

