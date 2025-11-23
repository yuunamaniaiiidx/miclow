use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct MessageLog<K, M> 
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static + std::fmt::Debug,
    M: Clone + Send + Sync + 'static + std::fmt::Debug,
{
    log: Arc<RwLock<VecDeque<M>>>,
    cursors: Arc<RwLock<HashMap<K, usize>>>,
    max_size: Option<usize>,
}

impl<K, M> MessageLog<K, M>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static + std::fmt::Debug,
    M: Clone + Send + Sync + 'static + std::fmt::Debug,
{
    pub fn new(max_size: Option<usize>) -> Self {
        Self {
            log: Arc::new(RwLock::new(VecDeque::new())),
            cursors: Arc::new(RwLock::new(HashMap::new())),
            max_size,
        }
    }

    pub async fn push(&self, event: M) {
        let mut log = self.log.write().await;
        log.push_back(event);

        if let Some(max_size) = self.max_size {
            let mut popped_count = 0;
            while log.len() > max_size {
                log.pop_front();
                popped_count += 1;
            }
            // pop_front()が実際に呼ばれた場合のみカーソルを調整
            if popped_count > 0 {
                self.adjust_cursors_after_pop_front(popped_count).await;
            }
        }
    }

    pub async fn pull(&self, key: K) -> Option<M> {
        // cursorsを先に取得してからlogを読む（ロック順序の最適化）
        let mut cursors = self.cursors.write().await;
        let log = self.log.read().await;

        let cursor_index = cursors.get(&key).copied().unwrap_or(0);

        if cursor_index >= log.len() {
            return None;
        }

        if let Some(event) = log.get(cursor_index) {
            let event = event.clone();
            let new_cursor = cursor_index + 1;
            cursors.insert(key.clone(), new_cursor);
            Some(event)
        } else {
            None
        }
    }

    pub async fn get_cursor(&self, key: &K) -> Option<usize> {
        let cursors = self.cursors.read().await;
        cursors.get(key).copied()
    }

    pub async fn set_cursor(&self, key: K, index: usize) {
        let mut cursors = self.cursors.write().await;
        cursors.insert(key, index);
    }

    pub async fn cleanup(&self) {
        let mut log = self.log.write().await;
        let mut cursors = self.cursors.write().await;

        if log.is_empty() || cursors.is_empty() {
            return;
        }

        let min_cursor = cursors.values().min().copied().unwrap_or(0);

        if min_cursor == 0 {
            return;
        }

        for _ in 0..min_cursor {
            log.pop_front();
        }

        for cursor in cursors.values_mut() {
            *cursor -= min_cursor;
        }
    }

    pub async fn len(&self) -> usize {
        let log = self.log.read().await;
        log.len()
    }

    pub async fn is_empty(&self) -> bool {
        let log = self.log.read().await;
        log.is_empty()
    }

    pub async fn cursor_count(&self) -> usize {
        let cursors = self.cursors.read().await;
        cursors.len()
    }

    async fn adjust_cursors_after_pop_front(&self, count: usize) {
        let mut cursors = self.cursors.write().await;
        for cursor in cursors.values_mut() {
            if *cursor > count {
                *cursor -= count;
            } else {
                *cursor = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerId;
    use crate::messages::MessageId;
    use crate::messages::ExecutorOutputEvent;
    use crate::subscription::SubscriptionId;
    use crate::topic::Topic;

    fn create_topic_event(index: usize) -> ExecutorOutputEvent {
        ExecutorOutputEvent::Topic {
            message_id: MessageId::new(),
            consumer_id: ConsumerId::new(),
            from_subscription_id: SubscriptionId::new(),
            to_subscription_id: None,
            topic: Topic::from(format!("test/topic{}", index)),
            data: format!("data{}", index).into(),
        }
    }

    #[tokio::test]
    async fn test_push_and_pull() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;
        log.push(create_topic_event(2)).await;

        assert_eq!(log.len().await, 3);

        let msg1 = log.pull("cursor1".to_string()).await;
        assert!(msg1.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        let msg2 = log.pull("cursor1".to_string()).await;
        assert!(msg2.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(2));

        let msg3 = log.pull("cursor1".to_string()).await;
        assert!(msg3.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(3));

        let msg4 = log.pull("cursor1".to_string()).await;
        assert!(msg4.is_none());
    }

    #[tokio::test]
    async fn test_multiple_cursors() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        for i in 0..5 {
            log.push(create_topic_event(i)).await;
        }

        let _msg1 = log.pull("cursor1".to_string()).await;
        let _msg2 = log.pull("cursor1".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(2));

        let _msg3 = log.pull("cursor2".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(1));

        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(2));
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(1));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        for i in 0..10 {
            log.push(create_topic_event(i)).await;
        }

        let _msg1 = log.pull("cursor1".to_string()).await;
        let _msg2 = log.pull("cursor1".to_string()).await;
        let _msg3 = log.pull("cursor1".to_string()).await;

        let _msg4 = log.pull("cursor2".to_string()).await;
        let _msg5 = log.pull("cursor2".to_string()).await;

        log.cleanup().await;

        assert_eq!(log.len().await, 8);

        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(0)); // 2 - 2 = 0
    }

    #[tokio::test]
    async fn test_max_size() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(3));
        
        for i in 0..5 {
            log.push(create_topic_event(i)).await;
        }

        assert_eq!(log.len().await, 3);
    }

    #[tokio::test]
    async fn test_max_size_with_cursor() {
        // max_sizeが設定されている場合でもカーソルが正しく動作することを確認
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(3));
        
        // 3つpush
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;
        log.push(create_topic_event(2)).await;

        // カーソル1でpull（index 0を取得）
        let msg1 = log.pull("cursor1".to_string()).await;
        assert!(msg1.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        // max_sizeを超えるようにpush（古い要素が削除される）
        // 4つ目をpushすると、len=4 > max_size=3 なので、1つpop_frontされる
        log.push(create_topic_event(3)).await;
        // カーソル1は調整されて0になる（1 - 1 = 0）
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));
        assert_eq!(log.len().await, 3);

        // 5つ目をpushすると、また1つpop_frontされる
        log.push(create_topic_event(4)).await;
        // カーソル0は調整されて0のまま（max(0, 0-1) = 0）
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));
        assert_eq!(log.len().await, 3);
        
        // 次のpullで正しい要素が取得できることを確認
        let msg2 = log.pull("cursor1".to_string()).await;
        assert!(msg2.is_some());
        // カーソル0なので、index 0の要素（event(2)）が取得される
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));
    }

    #[tokio::test]
    async fn test_cursor_reset_when_out_of_range() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        log.set_cursor("cursor1".to_string(), 10).await;

        let msg = log.pull("cursor1".to_string()).await;
        assert!(msg.is_none());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(10));
    }

    #[tokio::test]
    async fn test_empty_log() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        assert!(log.is_empty().await);
        assert_eq!(log.len().await, 0);

        let msg = log.pull("cursor1".to_string()).await;
        assert!(msg.is_none());
    }

    #[tokio::test]
    async fn test_subscription_key_type() {
        use crate::subscription::SubscriptionId;
        use crate::topic::Topic;

        type SubscriptionKey = (SubscriptionId, Topic);
        let log: MessageLog<SubscriptionKey, ExecutorOutputEvent> = MessageLog::new(None);
        
        let key = (SubscriptionId::new(), Topic::from("test/topic"));
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        let msg = log.pull(key.clone()).await;
        assert!(msg.is_some());
        assert_eq!(log.get_cursor(&key).await, Some(1));
    }

    #[tokio::test]
    async fn test_consumer_id_key_type() {
        use crate::consumer::ConsumerId;

        let log: MessageLog<ConsumerId, ExecutorOutputEvent> = MessageLog::new(None);
        
        let consumer_id = ConsumerId::new();
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        let msg = log.pull(consumer_id.clone()).await;
        assert!(msg.is_some());
        assert_eq!(log.get_cursor(&consumer_id).await, Some(1));
    }

    #[tokio::test]
    async fn test_generic_message_type() {
        let log: MessageLog<String, String> = MessageLog::new(None);
        
        log.push("message1".to_string()).await;
        log.push("message2".to_string()).await;
        log.push("message3".to_string()).await;

        let msg1 = log.pull("cursor1".to_string()).await;
        assert_eq!(msg1, Some("message1".to_string()));

        let msg2 = log.pull("cursor1".to_string()).await;
        assert_eq!(msg2, Some("message2".to_string()));

        let msg3 = log.pull("cursor2".to_string()).await;
        assert_eq!(msg3, Some("message1".to_string()));
    }

    #[tokio::test]
    async fn test_generic_with_integer() {
        let log: MessageLog<usize, i32> = MessageLog::new(None);
        
        log.push(100).await;
        log.push(200).await;
        log.push(300).await;

        let msg1 = log.pull(0).await;
        assert_eq!(msg1, Some(100));

        let msg2 = log.pull(0).await;
        assert_eq!(msg2, Some(200));

        let msg3 = log.pull(1).await;
        assert_eq!(msg3, Some(100));
    }

    #[tokio::test]
    async fn test_max_size_multiple_pop_front() {
        // 一度に複数要素がpop_frontされる場合のカーソル調整を確認
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(3));
        
        // 3つpush
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;
        log.push(create_topic_event(2)).await;

        // カーソル1でpull（index 0を取得）
        let _msg1 = log.pull("cursor1".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        // カーソル2でpull（index 1を取得）、カーソルは2になる
        // ただし、カーソル2は新規なので、index 0を取得してカーソルは1になる
        let _msg2 = log.pull("cursor2".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(1));

        // 一度に3つpushして、3つpop_frontされる（len=3 -> 6 -> 3）
        log.push(create_topic_event(3)).await;
        log.push(create_topic_event(4)).await;
        log.push(create_topic_event(5)).await;

        // カーソル1は 1 - 3 = -2 -> 0 になる
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));
        // カーソル2は 1 - 3 = -2 -> 0 になる
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(0));
        assert_eq!(log.len().await, 3);
    }

    #[tokio::test]
    async fn test_cursor_zero_after_pop_then_push() {
        // カーソルが0になった後、再度pushされた場合の動作を確認
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(2));
        
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        // カーソル1でpull
        let msg1 = log.pull("cursor1".to_string()).await;
        assert!(msg1.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        // pushしてpop_frontが発生、カーソルが0になる
        log.push(create_topic_event(2)).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));

        // カーソル0の状態で再度pull（index 0の要素が取得される）
        let msg2 = log.pull("cursor1".to_string()).await;
        assert!(msg2.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        // さらにpushしてpop_frontが発生、カーソルが0になる
        log.push(create_topic_event(3)).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));
        
        // pullでカーソルが1になる
        let msg3 = log.pull("cursor1".to_string()).await;
        assert!(msg3.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));
    }

    #[tokio::test]
    async fn test_cursor_out_of_range_then_push() {
        // カーソルが範囲外になった後、pushで範囲内に戻る場合
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        // カーソルを範囲外に設定
        log.set_cursor("cursor1".to_string(), 10).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(10));

        // pullはNoneを返す
        let msg = log.pull("cursor1".to_string()).await;
        assert!(msg.is_none());

        // pushしてもカーソルは範囲外のまま
        log.push(create_topic_event(2)).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(10));

        // 手動でカーソルをリセットしてからpull
        log.set_cursor("cursor1".to_string(), 0).await;
        let msg2 = log.pull("cursor1".to_string()).await;
        assert!(msg2.is_some());
    }

    #[tokio::test]
    async fn test_max_size_with_multiple_cursors() {
        // max_size制限下で複数カーソルが存在する場合の動作
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(3));
        
        // 3つpush
        for i in 0..3 {
            log.push(create_topic_event(i)).await;
        }

        // 複数カーソルでpull（それぞれindex 0, 1, 2を取得）
        let _msg1 = log.pull("cursor1".to_string()).await; // index 0取得、カーソル1
        let _msg2 = log.pull("cursor2".to_string()).await; // index 0取得、カーソル1（新規カーソル）
        // cursor2は新規なので、index 0を取得してカーソルは1になる
        // 次にpullするとindex 1を取得してカーソルは2になる
        let _msg2_2 = log.pull("cursor2".to_string()).await; // index 1取得、カーソル2
        let _msg3 = log.pull("cursor3".to_string()).await; // index 0取得、カーソル1（新規カーソル）

        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(2));
        assert_eq!(log.get_cursor(&"cursor3".to_string()).await, Some(1));

        // max_sizeを超えるようにpush（1つpop_frontされる）
        log.push(create_topic_event(3)).await;

        // すべてのカーソルが1減らされる
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(1));
        assert_eq!(log.get_cursor(&"cursor3".to_string()).await, Some(0));
        
        // カーソル2で再度pull（index 1の要素が取得される）
        let msg4 = log.pull("cursor2".to_string()).await;
        assert!(msg4.is_some());
        // pull後、カーソルは2になる（1 + 1）
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(2));
        assert_eq!(log.len().await, 3);
    }

    #[tokio::test]
    async fn test_adjust_cursors_with_zero_cursor() {
        // カーソルが0の状態でpop_frontが発生した場合
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(2));
        
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        // カーソル1でpullしてカーソルを1にする
        let _msg1 = log.pull("cursor1".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        // pushしてpop_front、カーソルが0になる
        log.push(create_topic_event(2)).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));

        // さらにpushしてpop_front、カーソル0は0のまま
        log.push(create_topic_event(3)).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(0));
        assert_eq!(log.len().await, 2);
    }
}

