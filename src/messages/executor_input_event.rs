use crate::message_id::MessageId;

#[derive(Clone, Debug)]
pub enum ExecutorInputEvent {
    Topic {
        message_id: MessageId,
        topic: String,
        data: String,
    },
    SystemResponse {
        message_id: MessageId,
        topic: String,
        status: String,
        data: String,
    },
    Function {
        message_id: MessageId,
        data: String,
    },
    FunctionResponse {
        message_id: MessageId,
        function_name: String,
        data: String,
    },
}
