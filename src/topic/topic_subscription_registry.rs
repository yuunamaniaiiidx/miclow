use crate::channels::ExecutorOutputEventSender;
use crate::messages::ExecutorOutputEvent;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    topic_senders: Arc<RwLock<HashMap<String, ExecutorOutputEventSender>>>,
    latest_messages: Arc<RwLock<HashMap<String, ExecutorOutputEvent>>>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            topic_senders: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_subscriber(&self, topic: String, sender: ExecutorOutputEventSender) {
        let replicaset_label = sender
            .replicaset_id()
            .map(|id| id.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let mut topics = self.topic_senders.write().await;
        topics.insert(topic.clone(), sender);

        log::info!(
            "Registered subscriber for topic '{}' (ReplicaSet: {})",
            topic,
            replicaset_label
        );
    }

    pub async fn broadcast_message(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic = match event.topic() {
            Some(topic) => topic,
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };
        let topic_owned = topic.clone();

        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let mut latest_messages = self.latest_messages.write().await;
            latest_messages.insert(topic_owned.clone(), event.clone());
        }

        let sender = {
            let topics = self.topic_senders.read().await;
            topics.get(&topic_owned).cloned()
        };

        let Some(sender) = sender else {
            log::info!(
                "No subscriber found for topic '{}', skipping broadcast",
                topic_owned
            );
            return Ok(());
        };

        sender.send(event.clone()).map_err(|e| {
            format!(
                "Failed to send message on topic '{}' (ReplicaSet unknown): {}",
                topic_owned, e
            )
        })?;

        log::info!("Broadcasted message on topic '{}'", topic_owned);
        Ok(())
    }

    pub async fn get_latest_message(&self, topic: &str) -> Option<ExecutorOutputEvent> {
        let latest_messages = self.latest_messages.read().await;
        latest_messages.get(topic).cloned()
    }
}
