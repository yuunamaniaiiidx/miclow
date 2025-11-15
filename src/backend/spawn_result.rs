use crate::input_channel::InputSender;
use crate::shutdown_channel::ShutdownSender;

pub struct SpawnBackendResult {
    pub task_handle: tokio::task::JoinHandle<()>,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}

