use crate::message_id::MessageId;
use crate::messages::{ExecutorOutputEvent, TopicResponseStatus, RESULT_TOPIC_SUFFIX};
use crate::task_id::TaskId;

/// トピックとデータから適切な`ExecutorOutputEvent`を生成する。
/// `.result`で終わるトピックの場合は`TopicResponse`に変換し、それ以外は通常の`Topic`として扱う。
pub fn create_topic_event(
    message_id: MessageId,
    task_id: TaskId,
    topic: String,
    data: String,
) -> ExecutorOutputEvent {
    if topic.ends_with(RESULT_TOPIC_SUFFIX) {
        // .resultで終わるトピックはTopicResponseに変換
        let original_topic = topic
            .strip_suffix(RESULT_TOPIC_SUFFIX)
            .expect("topic should end with RESULT_TOPIC_SUFFIX");

        ExecutorOutputEvent::TopicResponse {
            message_id,
            task_id,
            to_task_id: None,
            status: TopicResponseStatus::Unknown,
            topic: original_topic.to_string(),
            return_topic: topic,
            data,
        }
    } else {
        // 通常のトピックメッセージ
        ExecutorOutputEvent::new_message(message_id, task_id, topic, data)
    }
}
