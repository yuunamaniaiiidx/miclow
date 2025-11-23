use std::collections::HashMap;
use crate::channels::SubscriptionTopicSender;
use crate::consumer::{ConsumerId, ConsumerSpawnHandler, ConsumerState};
use crate::topic::Topic;

pub struct ManagedConsumer {
    pub handler: ConsumerSpawnHandler,
    pub topic_sender: SubscriptionTopicSender,
    pub state: ConsumerState,
}

#[derive(Default)]
struct ConsumerRouter {
    order: Vec<ConsumerId>,
    next_index: usize,
}

impl ConsumerRouter {
    fn add(&mut self, consumer_id: ConsumerId) {
        self.order.push(consumer_id);
    }

    fn remove(&mut self, consumer_id: &ConsumerId) {
        if let Some(pos) = self.order.iter().position(|id| id == consumer_id) {
            self.order.remove(pos);
            if self.order.is_empty() {
                self.next_index = 0;
                return;
            }
            if pos < self.next_index {
                self.next_index = self.next_index.saturating_sub(1);
            } else if self.next_index >= self.order.len() {
                self.next_index = 0;
            }
        }
    }

    fn next_consumer(&mut self) -> Option<ConsumerId> {
        if self.order.is_empty() {
            return None;
        }
        let consumer_id = self.order[self.next_index].clone();
        self.next_index = (self.next_index + 1) % self.order.len();
        Some(consumer_id)
    }

    fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}

pub struct ConsumerRegistry {
    consumers: HashMap<ConsumerId, ManagedConsumer>,
    router: ConsumerRouter,
    idle_count: usize,
}

impl ConsumerRegistry {
    pub fn new() -> Self {
        Self {
            consumers: HashMap::new(),
            router: ConsumerRouter::default(),
            idle_count: 0,
        }
    }

    pub fn add_consumer(&mut self, consumer: ManagedConsumer) -> ConsumerId {
        let consumer_id = consumer.handler.consumer_id.clone();
        self.consumers.insert(consumer_id.clone(), consumer);
        self.router.add(consumer_id.clone());
        self.idle_count += 1;
        consumer_id
    }

    pub fn remove_consumer(&mut self, consumer_id: &ConsumerId) -> Option<ManagedConsumer> {
        if let Some(consumer) = self.consumers.remove(consumer_id) {
            if matches!(consumer.state, ConsumerState::Requesting { .. }) {
                self.idle_count = self.idle_count.saturating_sub(1);
            }
            self.router.remove(consumer_id);
            Some(consumer)
        } else {
            None
        }
    }

    pub fn set_consumer_processing(&mut self, consumer_id: &ConsumerId) {
        if let Some(consumer) = self.consumers.get_mut(consumer_id) {
            if matches!(consumer.state, ConsumerState::Requesting { .. }) {
                consumer.state = ConsumerState::Processing { from_consumer_id: None };
                self.idle_count = self.idle_count.saturating_sub(1);
            }
        }
    }

    pub fn set_consumer_processing_with_from_consumer_id(
        &mut self,
        consumer_id: &ConsumerId,
        from_consumer_id: Option<ConsumerId>,
    ) {
        if let Some(consumer) = self.consumers.get_mut(consumer_id) {
            if matches!(consumer.state, ConsumerState::Requesting { .. }) {
                consumer.state = ConsumerState::Processing { from_consumer_id };
                self.idle_count = self.idle_count.saturating_sub(1);
            }
        }
    }

    pub fn set_consumer_requesting(&mut self, consumer_id: &ConsumerId, topic: Topic) {
        if let Some(consumer) = self.consumers.get_mut(consumer_id) {
            if !matches!(consumer.state, ConsumerState::Requesting { .. }) {
                consumer.state = ConsumerState::Requesting { topic };
                self.idle_count += 1;
            } else {
                consumer.state = ConsumerState::Requesting { topic };
            }
        }
    }

    pub fn idle_count(&self) -> usize {
        self.idle_count
    }

    pub fn get_consumer_mut(&mut self, consumer_id: &ConsumerId) -> Option<&mut ManagedConsumer> {
        self.consumers.get_mut(consumer_id)
    }

    pub fn get_consumer(&self, consumer_id: &ConsumerId) -> Option<&ManagedConsumer> {
        self.consumers.get(consumer_id)
    }

    pub fn next_consumer_id(&mut self) -> Option<ConsumerId> {
        self.router.next_consumer()
    }

    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty() || self.router.is_empty()
    }

    pub fn len(&self) -> usize {
        self.consumers.len()
    }

    pub fn drain(&mut self) -> HashMap<ConsumerId, ManagedConsumer> {
        self.router = ConsumerRouter::default();
        self.idle_count = 0;
        std::mem::take(&mut self.consumers)
    }
}

