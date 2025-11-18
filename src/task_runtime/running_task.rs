use crate::channels::{ExecutorInputEventSender, ShutdownSender};
use crate::task_id::TaskId;

#[derive(Debug)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub shutdown_sender: ShutdownSender,
    pub input_sender: ExecutorInputEventSender,
    pub task_handle: tokio::task::JoinHandle<()>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}
