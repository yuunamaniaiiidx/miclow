use crate::message_id::MessageId;
use crate::task_id::TaskId;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        task_id: TaskId,
        topic: String,
        data: String,
    },
    SystemResponse {
        message_id: MessageId,
        task_id: TaskId,
        topic: String,
        status: String,
        data: String,
    },
    Function {
        message_id: MessageId,
        task_id: TaskId,
        caller_task_id: TaskId,
        data: String,
    },
    FunctionResponse {
        message_id: MessageId,
        task_id: TaskId,
        function_name: String,
        data: String,
    },
}
