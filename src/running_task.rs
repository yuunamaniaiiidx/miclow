use crate::task_id::TaskId;
use crate::shutdown_channel::ShutdownSender;
use crate::input_channel::InputSender;

#[derive(Debug)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub shutdown_sender: ShutdownSender,
    pub input_sender: InputSender,
    pub task_handle: tokio::task::JoinHandle<()>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

