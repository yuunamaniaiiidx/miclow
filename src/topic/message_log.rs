use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

/// メッセージログとカーソルを統合管理する構造体
/// 
/// タイムスタンプではなくインデックスベースでカーソルを管理し、
/// 効率的なメッセージ取得とクリーンアップを提供します。
/// 
/// # 型パラメータ
/// * `K` - カーソルを識別するキーの型
/// * `M` - メッセージの型
#[derive(Clone)]
pub struct MessageLog<K, M> 
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    log: Arc<RwLock<VecDeque<M>>>,
    cursors: Arc<RwLock<HashMap<K, usize>>>,
    max_size: Option<usize>,
}

impl<K, M> MessageLog<K, M>
where
    K: std::hash::Hash + Eq + Clone + Send + Sync + 'static,
    M: Clone + Send + Sync + 'static,
{
    /// 新しいMessageLogを作成
    /// 
    /// # Arguments
    /// * `max_size` - ログの最大サイズ。Noneの場合は無制限
    pub fn new(max_size: Option<usize>) -> Self {
        Self {
            log: Arc::new(RwLock::new(VecDeque::new())),
            cursors: Arc::new(RwLock::new(HashMap::new())),
            max_size,
        }
    }

    /// メッセージをログに追加
    pub async fn push(&self, event: M) {
        let mut log = self.log.write().await;
        log.push_back(event);

        // 最大サイズ制限がある場合、古いメッセージを削除
        if let Some(max_size) = self.max_size {
            while log.len() > max_size {
                log.pop_front();
            }
            // カーソルを調整（削除されたメッセージ数だけ減らす）
            self.adjust_cursors_after_pop_front().await;
        }
    }

    /// カーソル位置から次のメッセージを取得し、カーソルを更新
    /// 
    /// # Arguments
    /// * `key` - カーソルを識別するキー
    /// 
    /// # Returns
    /// カーソル位置の次のメッセージ。存在しない場合はNone
    pub async fn pull(&self, key: K) -> Option<M> {
        let log = self.log.read().await;
        let mut cursors = self.cursors.write().await;

        // カーソル位置を取得（存在しない場合は0から開始）
        let cursor_index = cursors.get(&key).copied().unwrap_or(0);

        // カーソル位置がログの範囲外の場合はNoneを返す
        if cursor_index >= log.len() {
            return None;
        }

        // カーソル位置から次のメッセージを取得
        if let Some(event) = log.get(cursor_index) {
            let event = event.clone();
            // カーソルを次の位置に更新
            cursors.insert(key, cursor_index + 1);
            Some(event)
        } else {
            None
        }
    }

    /// カーソル位置を取得
    /// 
    /// # Arguments
    /// * `key` - カーソルを識別するキー
    /// 
    /// # Returns
    /// カーソル位置（インデックス）。存在しない場合はNone
    pub async fn get_cursor(&self, key: &K) -> Option<usize> {
        let cursors = self.cursors.read().await;
        cursors.get(key).copied()
    }

    /// カーソル位置を設定
    /// 
    /// # Arguments
    /// * `key` - カーソルを識別するキー
    /// * `index` - 設定するインデックス位置
    pub async fn set_cursor(&self, key: K, index: usize) {
        let mut cursors = self.cursors.write().await;
        cursors.insert(key, index);
    }

    /// すべてのカーソルより前のメッセージを削除
    /// 
    /// すべてのカーソルが参照している位置より前のメッセージを削除し、
    /// カーソル位置を調整します。
    pub async fn cleanup(&self) {
        let mut log = self.log.write().await;
        let mut cursors = self.cursors.write().await;

        if log.is_empty() || cursors.is_empty() {
            return;
        }

        // すべてのカーソル位置の最小値を取得
        let min_cursor = cursors.values().min().copied().unwrap_or(0);

        if min_cursor == 0 {
            // 最小カーソルが0の場合は削除しない
            return;
        }

        // 最小カーソル位置より前のメッセージを削除
        for _ in 0..min_cursor {
            log.pop_front();
        }

        // すべてのカーソル位置を調整
        for cursor in cursors.values_mut() {
            *cursor -= min_cursor;
        }
    }

    /// ログの現在のサイズを取得
    pub async fn len(&self) -> usize {
        let log = self.log.read().await;
        log.len()
    }

    /// ログが空かどうかを確認
    pub async fn is_empty(&self) -> bool {
        let log = self.log.read().await;
        log.is_empty()
    }

    /// カーソル数を取得
    pub async fn cursor_count(&self) -> usize {
        let cursors = self.cursors.read().await;
        cursors.len()
    }

    /// 最大サイズ制限でpop_frontが発生した後にカーソルを調整
    async fn adjust_cursors_after_pop_front(&self) {
        let mut cursors = self.cursors.write().await;
        // カーソルが0より大きい場合は1減らす
        for cursor in cursors.values_mut() {
            if *cursor > 0 {
                *cursor -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::ConsumerId;
    use crate::message_id::MessageId;
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
        
        // メッセージを追加
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;
        log.push(create_topic_event(2)).await;

        assert_eq!(log.len().await, 3);

        // カーソル1でメッセージを取得
        let msg1 = log.pull("cursor1".to_string()).await;
        assert!(msg1.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1));

        // 次のメッセージを取得
        let msg2 = log.pull("cursor1".to_string()).await;
        assert!(msg2.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(2));

        // さらに次のメッセージを取得
        let msg3 = log.pull("cursor1".to_string()).await;
        assert!(msg3.is_some());
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(3));

        // これ以上メッセージがない
        let msg4 = log.pull("cursor1".to_string()).await;
        assert!(msg4.is_none());
    }

    #[tokio::test]
    async fn test_multiple_cursors() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        // メッセージを追加
        for i in 0..5 {
            log.push(create_topic_event(i)).await;
        }

        // カーソル1で2つ取得
        let _msg1 = log.pull("cursor1".to_string()).await;
        let _msg2 = log.pull("cursor1".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(2));

        // カーソル2で1つ取得
        let _msg3 = log.pull("cursor2".to_string()).await;
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(1));

        // カーソル1は2、カーソル2は1の位置にある
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(2));
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(1));
    }

    #[tokio::test]
    async fn test_cleanup() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        // メッセージを追加
        for i in 0..10 {
            log.push(create_topic_event(i)).await;
        }

        // カーソル1で3つ取得（カーソル位置: 3）
        let _msg1 = log.pull("cursor1".to_string()).await;
        let _msg2 = log.pull("cursor1".to_string()).await;
        let _msg3 = log.pull("cursor1".to_string()).await;

        // カーソル2で2つ取得（カーソル位置: 2）
        let _msg4 = log.pull("cursor2".to_string()).await;
        let _msg5 = log.pull("cursor2".to_string()).await;

        // クリーンアップ（最小カーソルは2なので、最初の2つを削除）
        log.cleanup().await;

        // ログサイズが8になっているはず（10 - 2 = 8）
        assert_eq!(log.len().await, 8);

        // カーソル位置が調整されているはず
        assert_eq!(log.get_cursor(&"cursor1".to_string()).await, Some(1)); // 3 - 2 = 1
        assert_eq!(log.get_cursor(&"cursor2".to_string()).await, Some(0)); // 2 - 2 = 0
    }

    #[tokio::test]
    async fn test_max_size() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(Some(3));
        
        // 最大サイズを超えるメッセージを追加
        for i in 0..5 {
            log.push(create_topic_event(i)).await;
        }

        // 最大サイズに制限されているはず
        assert_eq!(log.len().await, 3);

        // カーソルが調整されているはず
        // 最初の2つが削除されたので、カーソルも調整される
    }

    #[tokio::test]
    async fn test_cursor_reset_when_out_of_range() {
        let log: MessageLog<String, ExecutorOutputEvent> = MessageLog::new(None);
        
        // メッセージを追加
        log.push(create_topic_event(0)).await;
        log.push(create_topic_event(1)).await;

        // カーソルを範囲外に設定
        log.set_cursor("cursor1".to_string(), 10).await;

        // pullすると、範囲外なのでNoneが返される
        let msg = log.pull("cursor1".to_string()).await;
        assert!(msg.is_none());
        // カーソル位置は変更されない
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
        // String型のメッセージでも使用可能
        let log: MessageLog<String, String> = MessageLog::new(None);
        
        log.push("message1".to_string()).await;
        log.push("message2".to_string()).await;
        log.push("message3".to_string()).await;

        let msg1 = log.pull("cursor1".to_string()).await;
        assert_eq!(msg1, Some("message1".to_string()));

        let msg2 = log.pull("cursor1".to_string()).await;
        assert_eq!(msg2, Some("message2".to_string()));

        // 別のカーソルで独立に読み取り可能
        let msg3 = log.pull("cursor2".to_string()).await;
        assert_eq!(msg3, Some("message1".to_string()));
    }

    #[tokio::test]
    async fn test_generic_with_integer() {
        // i32型のメッセージでも使用可能
        let log: MessageLog<usize, i32> = MessageLog::new(None);
        
        log.push(100).await;
        log.push(200).await;
        log.push(300).await;

        let msg1 = log.pull(0).await;
        assert_eq!(msg1, Some(100));

        let msg2 = log.pull(0).await;
        assert_eq!(msg2, Some(200));

        // 別のカーソルで独立に読み取り可能
        let msg3 = log.pull(1).await;
        assert_eq!(msg3, Some(100));
    }
}

