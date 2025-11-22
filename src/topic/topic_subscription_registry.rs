use crate::channels::ExecutorOutputEventSender;
use crate::messages::ExecutorOutputEvent;
use crate::topic::Topic;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct TopicSubscriptionRegistry {
    topic_senders: Arc<RwLock<HashMap<Topic, Vec<ExecutorOutputEventSender>>>>,
    latest_messages: Arc<RwLock<HashMap<Topic, ExecutorOutputEvent>>>,
}

impl TopicSubscriptionRegistry {
    pub fn new() -> Self {
        Self {
            topic_senders: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_subscriber(
        &self,
        topic: impl Into<Topic>,
        sender: ExecutorOutputEventSender,
    ) {
        let topic = topic.into();
        let mut topics = self.topic_senders.write().await;
        
        topics.entry(topic.clone()).or_insert_with(Vec::new).push(sender);

        let subscriber_count = topics.get(&topic).map(|v| v.len()).unwrap_or(0);
        log::info!(
            "Registered subscriber for topic '{}' (total subscribers: {})",
            topic,
            subscriber_count
        );
    }

    pub async fn broadcast_message(&self, event: ExecutorOutputEvent) -> Result<(), String> {
        let topic_owned = match event.topic() {
            Some(topic) => topic.clone(),
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };

        if matches!(event, ExecutorOutputEvent::Topic { .. }) {
            let mut latest_messages = self.latest_messages.write().await;
            latest_messages.insert(topic_owned.clone(), event.clone());
        }

        let senders = {
            let topics = self.topic_senders.read().await;
            topics.get(&topic_owned).cloned()
        };

        let Some(senders) = senders else {
            log::info!(
                "No subscriber found for topic '{}', skipping broadcast",
                topic_owned
            );
            return Ok(());
        };

        let mut success_count = 0;
        let mut error_count = 0;
        for sender in senders.iter() {
            match sender.send(event.clone()) {
                Ok(_) => {
                    success_count += 1;
                }
                Err(e) => {
                    error_count += 1;
                    log::warn!(
                        "Failed to send message on topic '{}': {}",
                        topic_owned,
                        e
                    );
                }
            }
        }

        if error_count > 0 {
            log::warn!(
                "Broadcasted message on topic '{}' to {} subscribers ({} failed)",
                topic_owned,
                success_count,
                error_count
            );
        } else {
            log::info!(
                "Broadcasted message on topic '{}' to {} subscribers",
                topic_owned,
                success_count
            );
        }

        Ok(())
    }

    pub async fn get_latest_message(&self, topic: impl Into<Topic>) -> Option<ExecutorOutputEvent> {
        let topic = topic.into();
        let latest_messages = self.latest_messages.read().await;
        latest_messages.get(&topic).cloned()
    }
}
