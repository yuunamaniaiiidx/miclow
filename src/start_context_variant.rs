use crate::executor_event_channel::ExecutorEventSender;

#[derive(Clone)]
pub enum StartContextVariant {
    Tasks,
    Functions {
        return_message_sender: ExecutorEventSender,
        initial_input: Option<String>,
    },
}

