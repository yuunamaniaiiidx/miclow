use crate::message_id::MessageId;
use crate::messages::ExecutorOutputEvent;
use crate::pod::PodId;
use crate::topic::Topic;

/// トピックとデータから適切な`ExecutorOutputEvent`を生成する。
/// すべてのトピックメッセージを`Topic`として扱う（`.result`で終わるトピックも含む）。
pub fn create_topic_event(
    message_id: MessageId,
    pod_id: PodId,
    topic: impl Into<Topic>,
    data: String,
) -> ExecutorOutputEvent {
    // すべてのトピックメッセージをTopicとして扱う
    ExecutorOutputEvent::new_message(message_id, pod_id, topic, data)
}
