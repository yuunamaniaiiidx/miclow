use crate::message_id::MessageId;

#[derive(Clone, Debug)]
pub struct TopicMessage {
    pub message_id: MessageId,
    pub topic: String,
    pub data: String,
}

#[derive(Clone, Debug)]
pub struct SystemResponseMessage {
    pub message_id: MessageId,
    pub topic: String,
    pub status: String,
    pub data: String,
}

#[derive(Clone, Debug)]
pub struct ReturnMessage {
    pub message_id: MessageId,
    pub data: String,
}

#[derive(Clone, Debug)]
pub struct FunctionMessage {
    pub message_id: MessageId,
    pub task_name: String,
    pub data: String,
}

#[derive(Clone, Debug)]
pub enum InputDataMessage {
    Topic(TopicMessage),
    SystemResponse(SystemResponseMessage),
    Return(ReturnMessage),
    Function(FunctionMessage),
}

