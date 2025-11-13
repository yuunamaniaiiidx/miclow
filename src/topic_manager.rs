use anyhow::Result;
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::task_id::TaskId;
use crate::executor_event_channel::{ExecutorEvent, ExecutorEventSender};

#[derive(Clone)]
pub struct TopicManager {
    subscribers: Arc<RwLock<HashMap<String, Arc<Vec<Arc<ExecutorEventSender>>>>>>,
    task_subscriptions: Arc<RwLock<HashMap<(String, TaskId), Weak<ExecutorEventSender>>>>,
    latest_messages: Arc<RwLock<HashMap<String, ExecutorEvent>>>,
}

impl TopicManager {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            task_subscriptions: Arc::new(RwLock::new(HashMap::new())),
            latest_messages: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn add_subscriber(&self, topic: String, task_id: TaskId, subscriber: ExecutorEventSender) {
        let subscriber_arc = Arc::new(subscriber);
        let mut subscribers = self.subscribers.write().await;
        
        if let Some(existing_subscribers) = subscribers.get_mut(&topic) {
            let new_subscribers = Arc::new({
                let mut vec = existing_subscribers.as_ref().clone();
                vec.push(subscriber_arc.clone());
                vec
            });
            *existing_subscribers = new_subscribers;
            log::info!("Added subscriber to existing topic '{}' (total subscribers: {})", topic, existing_subscribers.len());
        } else {
            let new_subscribers = Arc::new(vec![subscriber_arc.clone()]);
            subscribers.insert(topic.clone(), new_subscribers);
            log::info!("Added new topic '{}' with subscriber", topic);
        }
        let mut task_subs = self.task_subscriptions.write().await;
        task_subs.insert((topic.clone(), task_id.clone()), Arc::downgrade(&subscriber_arc));
        log::info!("Recorded task {} subscription to topic '{}'", task_id, topic);
    }

    pub async fn remove_failed_subscribers(&self, topic: &str, failed_indices: Vec<usize>) {
        let mut subscribers = self.subscribers.write().await;
        
        if let Some(topic_subscribers) = subscribers.get_mut(topic) {
            let mut new_subscribers = topic_subscribers.as_ref().clone();
            for &index in failed_indices.iter().rev() {
                if index < new_subscribers.len() {
                    new_subscribers.remove(index);
                }
            }
            
            if new_subscribers.is_empty() {
                subscribers.remove(topic);
            } else {
                *topic_subscribers = Arc::new(new_subscribers);
            }
        }
    }

    pub async fn remove_subscriber_by_task(&self, topic: String, task_id: TaskId) -> bool {
        let mut task_subs = self.task_subscriptions.write().await;
        let weak_sender = task_subs.remove(&(topic.clone(), task_id.clone()));
        drop(task_subs);
        
        if weak_sender.is_none() {
            log::warn!("No mapping found for task {} and topic '{}'", task_id, topic);
            return false;
        }
        let mut subscribers = self.subscribers.write().await;
        if let Some(topic_subscribers) = subscribers.get_mut(&topic) {
            let mut new_subscribers = topic_subscribers.as_ref().clone();
            new_subscribers.retain(|sender| {
                if let Some(weak_ref) = weak_sender.as_ref() {
                    if let Some(strong_ref) = weak_ref.upgrade() {
                        !Arc::ptr_eq(sender, &strong_ref)
                    } else {
                        true
                    }
                } else {
                    true
                }
            });
            
            if new_subscribers.is_empty() {
                subscribers.remove(&topic);
                log::info!("Removed empty topic '{}'", topic);
            } else {
                *topic_subscribers = Arc::new(new_subscribers);
            }
            
            log::info!("Removed subscriber for task {} from topic '{}'", task_id, topic);
            return true;
        } else {
            log::warn!("Topic '{}' not found for removal", topic);
            return false;
        }
    }

    pub async fn remove_all_subscriptions_by_task(&self, task_id: TaskId) -> Vec<String> {
        let mut removed_topics = Vec::new();
        let mut task_subs = self.task_subscriptions.write().await;
        let task_entries: Vec<(String, Weak<ExecutorEventSender>)> = task_subs.iter()
            .filter(|((_, stored_task_id), _)| *stored_task_id == task_id)
            .map(|((topic, _), weak_sender)| (topic.clone(), weak_sender.clone()))
            .collect();
        task_subs.retain(|(_, stored_task_id), _| *stored_task_id != task_id);
        drop(task_subs);
        let mut subscribers = self.subscribers.write().await;
        for (topic, weak_sender) in task_entries {
            if let Some(topic_subscribers) = subscribers.get_mut(&topic) {
                let mut new_subscribers = topic_subscribers.as_ref().clone();
                if let Some(strong_ref) = weak_sender.upgrade() {
                    new_subscribers.retain(|sender| !Arc::ptr_eq(sender, &strong_ref));
                }
                
                if new_subscribers.is_empty() {
                    subscribers.remove(&topic);
                    log::info!("Removed empty topic '{}'", topic);
                    removed_topics.push(topic.clone());
                } else {
                    *topic_subscribers = Arc::new(new_subscribers);
                }
            }
        }
        
        log::info!("Removed all subscriptions for task {} ({} topics affected)", task_id, removed_topics.len());
        removed_topics
    }

    pub async fn get_subscribers(&self, topic: &str) -> Option<Vec<Arc<ExecutorEventSender>>> {
        let subscribers = self.subscribers.read().await;
        subscribers.get(topic).map(|arc_vec| arc_vec.as_ref().clone())
    }

    pub async fn get_topics_info(&self) -> Vec<(String, usize)> {
        let subscribers = self.subscribers.read().await;
        subscribers.iter()
            .map(|(topic, subscriber_list)| (topic.clone(), subscriber_list.len()))
            .collect()
    }

    pub async fn broadcast_message(&self, event: ExecutorEvent) -> Result<usize, String> {
        let topic = match event.topic() {
            Some(topic) => topic,
            None => {
                return Err("Event does not contain a topic".to_string());
            }
        };
        let topic_owned = topic.clone();

        if matches!(event, ExecutorEvent::Message { .. }) {
            let mut latest_messages = self.latest_messages.write().await;
            latest_messages.insert(topic_owned.clone(), event.clone());
        }
        
        let subscribers = self.get_subscribers(&topic_owned).await;
        
        if let Some(subscriber_list) = subscribers {
            if subscriber_list.is_empty() {
                return Ok(0);
            }
            let send_tasks: Vec<_> = subscriber_list.into_iter()
                .enumerate()
                .map(|(index, sender)| {
                    let event_clone = event.clone();
                    tokio::spawn(async move {
                        match sender.send(event_clone) {
                            Ok(_) => Ok(index),
                            Err(e) => Err((index, e)),
                        }
                    })
                })
                .collect();
            let results = futures::future::join_all(send_tasks).await;
            
            let mut success_count = 0;
            let mut failed_indices = Vec::new();
            
            for result in results {
                match result {
                    Ok(Ok(_)) => success_count += 1,
                    Ok(Err((index, _))) => failed_indices.push(index),
                    Err(_) => {
                        failed_indices.push(0);
                    }
                }
            }
            if !failed_indices.is_empty() {
                self.remove_failed_subscribers(topic, failed_indices).await;
            }
            
            log::info!("Broadcasted message to {} subscribers on topic '{}'", success_count, topic);
            Ok(success_count)
        } else {
            log::info!("No subscribers found for topic '{}'", topic);
            Ok(0)
        }
    }

    pub async fn get_latest_message(&self, topic: &str) -> Option<ExecutorEvent> {
        let latest_messages = self.latest_messages.read().await;
        latest_messages.get(topic).cloned()
    }
}

