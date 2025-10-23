use anyhow::{Result, Error};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::process::Command as TokioCommand;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};
use log::info;
use tokio::task;
use chrono;
use uuid::Uuid;
use tokio::signal::unix::{signal, SignalKind};
use tokio_util::sync::CancellationToken;
use async_trait::async_trait;
use std::process::Stdio;
use crate::buffer::{TopicInputBuffer, consume_stream_line, StreamOutcome};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn uuid(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MessageId(Uuid);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn uuid(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub enum TopicRequestType {
    Subscribe,
    Unsubscribe,
}

#[derive(Debug, Clone)]
pub struct TopicRequestMessage {
    pub message_id: MessageId,
    pub topic: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub task_id: TaskId,
    pub request_type: TopicRequestType,
    pub response_channel: ExecutorEventSender,
}

impl TopicRequestMessage {
    pub fn new_subscribe(topic: String, task_id: TaskId, response_channel: ExecutorEventSender) -> Self {
        Self {
            message_id: MessageId::new(),
            topic,
            timestamp: chrono::Utc::now(),
            task_id,
            request_type: TopicRequestType::Subscribe,
            response_channel,
        }
    }

    pub fn new_unsubscribe(topic: String, task_id: TaskId, response_channel: ExecutorEventSender) -> Self {
        Self {
            message_id: MessageId::new(),
            topic,
            timestamp: chrono::Utc::now(),
            task_id,
            request_type: TopicRequestType::Unsubscribe,
            response_channel,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SystemCommandMessage {
    pub command: SystemCommand,
    pub task_id: TaskId,
    pub request_id: MessageId,
    pub response_channel: ExecutorEventSender,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl SystemCommandMessage {
    pub fn new(command: SystemCommand, task_id: TaskId, response_channel: ExecutorEventSender) -> Self {
        Self {
            command,
            task_id,
            request_id: MessageId::new(),
            response_channel,
            timestamp: chrono::Utc::now(),
        }
    }
}

#[derive(Clone)]
pub struct TopicRequestSender {
    sender: Arc<mpsc::UnboundedSender<TopicRequestMessage>>,
}

impl TopicRequestSender {
    pub fn new(sender: Arc<mpsc::UnboundedSender<TopicRequestMessage>>) -> Self {
        Self { sender }
    }

    pub fn send(&self, message: TopicRequestMessage) -> Result<(), mpsc::error::SendError<TopicRequestMessage>> {
        self.sender.send(message)
    }

    pub fn send_subscribe_request(&self, topic: String, task_id: TaskId, response_channel: ExecutorEventSender) -> Result<(), mpsc::error::SendError<TopicRequestMessage>> {
        self.send(TopicRequestMessage::new_subscribe(topic, task_id, response_channel))
    }

    pub fn send_unsubscribe_request(&self, topic: String, task_id: TaskId, response_channel: ExecutorEventSender) -> Result<(), mpsc::error::SendError<TopicRequestMessage>> {
        self.send(TopicRequestMessage::new_unsubscribe(topic, task_id, response_channel))
    }
}

pub struct TopicRequestReceiver {
    receiver: mpsc::UnboundedReceiver<TopicRequestMessage>,
}

impl TopicRequestReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<TopicRequestMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<TopicRequestMessage> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<TopicRequestMessage, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub struct TopicChannel {
    pub sender: TopicRequestSender,
    pub receiver: TopicRequestReceiver,
}

impl TopicChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<TopicRequestMessage>();
        Self {
            sender: TopicRequestSender::new(Arc::new(tx)),
            receiver: TopicRequestReceiver::new(receiver),
        }
    }
}

#[derive(Clone)]
pub struct TopicDataSender {
    sender: mpsc::UnboundedSender<ExecutorEvent>,
}

impl TopicDataSender {
    pub fn new(sender: mpsc::UnboundedSender<ExecutorEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: ExecutorEvent) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.sender.send(event)
    }
}

pub struct TopicDataReceiver {
    receiver: mpsc::UnboundedReceiver<ExecutorEvent>,
}

impl TopicDataReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ExecutorEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ExecutorEvent> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<ExecutorEvent, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub struct TopicDataChannel {
    pub sender: TopicDataSender,
    pub receiver: TopicDataReceiver,
}

impl TopicDataChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<ExecutorEvent>();
        Self {
            sender: TopicDataSender::new(tx),
            receiver: TopicDataReceiver::new(receiver),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutorEventSender {
    sender: mpsc::UnboundedSender<ExecutorEvent>,
}

impl ExecutorEventSender {
    pub fn new(sender: mpsc::UnboundedSender<ExecutorEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: ExecutorEvent) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.sender.send(event)
    }

    pub fn send_message(&self, key: String, data: String, task_id: TaskId) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_message(key, data, task_id))
    }

    pub fn send_error(&self, error: String, task_id: TaskId) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_error(error, task_id))
    }

    pub fn send_system_command(&self, command: SystemCommand, task_id: TaskId) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_systemcommand(command, task_id))
    }

    pub fn send_exit(&self, code: i32, task_id: TaskId) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_exit(code, task_id))
    }
}

pub struct ExecutorEventReceiver {
    receiver: mpsc::UnboundedReceiver<ExecutorEvent>,
}

impl ExecutorEventReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<ExecutorEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<ExecutorEvent> {
        self.receiver.recv().await
    }
}

pub struct ExecutorEventChannel {
    pub sender: ExecutorEventSender,
    pub receiver: ExecutorEventReceiver,
}

impl ExecutorEventChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<ExecutorEvent>();
        Self {
            sender: ExecutorEventSender::new(tx),
            receiver: ExecutorEventReceiver::new(receiver),
        }
    }
}

#[derive(Clone)]
pub struct InputSender {
    sender: mpsc::UnboundedSender<String>,
}

impl InputSender {
    pub fn new(sender: mpsc::UnboundedSender<String>) -> Self {
        Self { sender }
    }

    pub fn send(&self, input: String) -> Result<(), mpsc::error::SendError<String>> {
        self.sender.send(input)
    }
}

pub struct InputReceiver {
    receiver: mpsc::UnboundedReceiver<String>,
}

impl InputReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<String>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<String> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<String, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub struct InputChannel {
    pub sender: InputSender,
    pub receiver: InputReceiver,
}

impl InputChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<String>();
        Self {
            sender: InputSender::new(tx),
            receiver: InputReceiver::new(receiver),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ShutdownSender {
    sender: mpsc::UnboundedSender<()>,
}

impl ShutdownSender {
    pub fn new(sender: mpsc::UnboundedSender<()>) -> Self {
        Self { sender }
    }

    pub fn shutdown(&self) -> Result<(), mpsc::error::SendError<()>> {
        self.sender.send(())
    }

    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

pub struct ShutdownChannel {
    pub sender: ShutdownSender,
    pub receiver: mpsc::UnboundedReceiver<()>,
}

impl ShutdownChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<()>();
        Self {
            sender: ShutdownSender::new(tx),
            receiver,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SystemCommandSender {
    sender: mpsc::UnboundedSender<SystemCommandMessage>,
}

impl SystemCommandSender {
    pub fn new(sender: mpsc::UnboundedSender<SystemCommandMessage>) -> Self {
        Self { sender }
    }

    pub fn send(&self, message: SystemCommandMessage) -> Result<(), mpsc::error::SendError<SystemCommandMessage>> {
        self.sender.send(message)
    }

    pub fn send_system_command(&self, command: SystemCommand, task_id: TaskId, response_channel: ExecutorEventSender) -> Result<(), mpsc::error::SendError<SystemCommandMessage>> {
        self.send(SystemCommandMessage::new(command, task_id, response_channel))
    }
}

pub struct SystemCommandReceiver {
    receiver: mpsc::UnboundedReceiver<SystemCommandMessage>,
}

impl SystemCommandReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<SystemCommandMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<SystemCommandMessage> {
        self.receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<SystemCommandMessage, mpsc::error::TryRecvError> {
        self.receiver.try_recv()
    }
}

pub struct SystemCommandChannel {
    pub sender: SystemCommandSender,
    pub receiver: SystemCommandReceiver,
}

impl SystemCommandChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<SystemCommandMessage>();
        Self {
            sender: SystemCommandSender::new(tx),
            receiver: SystemCommandReceiver::new(receiver),
        }
    }
}

#[derive(Clone)]
pub struct TopicSubscriber {
    subscribers: Arc<RwLock<HashMap<String, Arc<Vec<Arc<ExecutorEventSender>>>>>>,
    task_subscriptions: Arc<RwLock<HashMap<(String, TaskId), Weak<ExecutorEventSender>>>>,
}

impl TopicSubscriber {
    pub fn new() -> Self {
        Self {
            subscribers: Arc::new(RwLock::new(HashMap::new())),
            task_subscriptions: Arc::new(RwLock::new(HashMap::new())),
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
                        true // 弱参照が無効な場合は保持
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

    pub async fn broadcast_message(&self, topic: &str, event: ExecutorEvent) -> Result<usize, String> {
        let subscribers = self.get_subscribers(topic).await;
        
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
}

pub struct SystemCommandWorker {
    pub system_command_channel: SystemCommandChannel,
    pub topic_request_sender: TopicRequestSender,
    pub topic_subscriber: TopicSubscriber,
    pub task_registry: TaskRegistry,
    pub config: ServerConfig,
}

impl SystemCommandWorker {
    pub fn new(system_command_channel: SystemCommandChannel, topic_request_sender: TopicRequestSender, topic_subscriber: TopicSubscriber, task_registry: TaskRegistry, config: ServerConfig) -> Self {
        Self {
            system_command_channel,
            topic_request_sender,
            topic_subscriber,
            task_registry,
            config,
        }
    }

    pub fn start_worker(
        mut system_command_receiver: SystemCommandReceiver,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        task_registry: TaskRegistry,
        config: ServerConfig,
        system_command_sender: SystemCommandSender,
        shutdown_token: CancellationToken,
    ) {
        task::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    
                    _ = shutdown_token.cancelled() => {
                        log::info!("SystemCommand worker received shutdown signal");
                        break;
                    }
                    
                    maybe = async { system_command_receiver.recv().await } => {
                        match maybe {
                            Some(message) => {
                                let task_id: TaskId = message.task_id.clone();
                                let command: SystemCommand = message.command.clone();
                                
                                log::info!("Processing SystemCommand for task {}: {:?}", task_id, command);
                                
                                let command_clone = command.clone();
                                match command {
                                    SystemCommand::SubscribeTopic { topic } => {
                                        log::info!("Processing SubscribeTopic command for task {}: '{}'", task_id, topic);
                                        
                                        if let Err(e) = topic_request_sender.send_subscribe_request(
                                            topic.clone(),
                                            task_id.clone(), 
                                            message.response_channel.clone()
                                        ) {
                                            log::warn!("Failed to send topic request (task {}): {}", task_id, e);
                                        } else {
                                            log::info!("Sent topic request for '{}' from task {}", topic, task_id);
                                        }
                                    },
                                    SystemCommand::UnsubscribeTopic { topic } => {
                                        log::info!("Processing UnsubscribeTopic command for task {}: '{}'", task_id, topic);
                                        
                                        if let Err(e) = topic_request_sender.send_unsubscribe_request(
                                            topic.clone(),
                                            task_id.clone(), 
                                            message.response_channel.clone()
                                        ) {
                                            log::warn!("Failed to send unsubscribe request (task {}): {}", task_id, e);
                                        } else {
                                            log::info!("Sent unsubscribe request for '{}' from task {}", topic, task_id);
                                        }
                                    },
                                    SystemCommand::Stdout { data } => {
                                        println!("{}", data);
                                    },
                                    SystemCommand::Stderr { data } => {
                                        eprintln!("{}", data);
                                    },
                                    SystemCommand::StartTask { task_name } => {
                                        log::info!("Processing StartTask command for task {}: '{}'", task_id, task_name);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        match task_registry.start_single_task(
                                            &task_name,
                                            &config,
                                            topic_request_sender.clone(),
                                            topic_subscriber.clone(),
                                            system_command_sender.clone(),
                                            shutdown_token.clone(),
                                        ).await {
                                            Ok(_) => {
                                                let success_msg = format!("Successfully started task '{}'", task_name);
                                                log::info!("{}", success_msg);
                                                
                                                let success_event = ExecutorEvent::new_system_response(
                                                    message.request_id.clone(),
                                                    topic_name,
                                                    success_msg,
                                                    task_id.clone()
                                                );
                                                let _ = message.response_channel.send(success_event);
                                            },
                                            Err(e) => {
                                                let error_msg = format!("Failed to start task '{}': {}", task_name, e);
                                                log::error!("{}", error_msg);
                                                
                                                let error_event = ExecutorEvent::new_system_error(
                                                    message.request_id.clone(),
                                                    error_msg,
                                                    task_id.clone()
                                                );
                                                let _ = message.response_channel.send(error_event);
                                            }
                                        }
                                    },
                                    SystemCommand::StopTask { task_name } => {
                                        log::info!("Processing StopTask command for task {}: '{}'", task_id, task_name);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        match task_registry.stop_task(&task_name).await {
                                            Ok(_) => {
                                                let success_msg = format!("Successfully stopped task '{}'", task_name);
                                                log::info!("{}", success_msg);
                                                
                                                let success_event = ExecutorEvent::new_system_response(
                                                    message.request_id.clone(),
                                                    topic_name,
                                                    success_msg,
                                                    task_id.clone()
                                                );
                                                let _ = message.response_channel.send(success_event);
                                            },
                                            Err(e) => {
                                                let error_msg = format!("Failed to stop task '{}': {}", task_name, e);
                                                log::error!("{}", error_msg);
                                                
                                                let error_event = ExecutorEvent::new_system_error(
                                                    message.request_id.clone(),
                                                    error_msg,
                                                    task_id.clone()
                                                );
                                                let _ = message.response_channel.send(error_event);
                                            }
                                        }
                                    },
                                    SystemCommand::KillServer => {
                                        log::info!("Processing KillServer command for task {} - initiating graceful server shutdown", task_id);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        let shutdown_msg = "Server graceful shutdown initiated";
                                        let shutdown_event = ExecutorEvent::new_system_response(
                                            message.request_id.clone(),
                                            topic_name,
                                            shutdown_msg.to_string(),
                                            task_id.clone()
                                        );
                                        let _ = message.response_channel.send(shutdown_event);
                                        
                                        log::info!("Starting graceful shutdown process...");
                                        
                                        let running_tasks = task_registry.running_tasks.read().await;
                                        let task_names: Vec<String> = running_tasks.keys().cloned().collect();
                                        drop(running_tasks);
                                        
                                        for task_name in task_names {
                                            if let Err(e) = task_registry.stop_task(&task_name).await {
                                                log::warn!("Failed to stop task '{}': {}", task_name, e);
                                            }
                                        }
                                        
                                        log::info!("Cancelling shutdown token to stop all workers");
                                        shutdown_token.cancel();
                                        
                                        log::info!("Graceful shutdown process completed, exiting worker");
                                        
                                        log::info!("Exiting server process with code 0");
                                        std::process::exit(0);
                                    },
                                    SystemCommand::AddTaskFromToml { toml_data } => {
                                        log::info!("Processing AddTaskFromToml command for task {} with TOML data", task_id);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        match task_registry.add_task_from_toml(
                                            &toml_data,
                                            topic_request_sender.clone(),
                                            topic_subscriber.clone(),
                                            system_command_sender.clone(),
                                            shutdown_token.clone(),
                                        ).await {
                                            Ok(_) => {
                                                let success_msg = format!("Successfully added task to registry from TOML");
                                                log::info!("{}", success_msg);
                                                
                                                let success_event = ExecutorEvent::new_system_response(
                                                    message.request_id.clone(),
                                                    topic_name,
                                                    success_msg,
                                                    task_id.clone()
                                                );
                                                let _ = message.response_channel.send(success_event);
                                            },
                                            Err(e) => {
                                                let error_msg = format!("Failed to add task from TOML: {}", e);
                                                log::error!("{}", error_msg);
                                                
                                                let error_event = ExecutorEvent::new_system_error(
                                                    message.request_id.clone(),
                                                    error_msg,
                                                    task_id.clone()
                                                );
                                                let _ = message.response_channel.send(error_event);
                                            }
                                        }
                                    },
                                    SystemCommand::Status => {
                                        log::info!("Processing Status command for task {}", task_id);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        let tasks_info = task_registry.get_running_tasks_info().await;
                                        let topics_info = topic_subscriber.get_topics_info().await;
                                        
                                        let mut json_response = String::from("{\n");
                                        json_response.push_str("  \"tasks\": [\n");
                                        
                                        for (i, (task_name, task_id)) in tasks_info.iter().enumerate() {
                                            if i > 0 {
                                                json_response.push_str(",\n");
                                            }
                                            json_response.push_str(&format!("    {{\"name\": \"{}\", \"id\": \"{}\"}}", task_name, task_id));
                                        }
                                        
                                        json_response.push_str("\n  ],\n");
                                        json_response.push_str("  \"topics\": [\n");
                                        
                                        for (i, (topic_name, subscriber_count)) in topics_info.iter().enumerate() {
                                            if i > 0 {
                                                json_response.push_str(",\n");
                                            }
                                            json_response.push_str(&format!("    {{\"name\": \"{}\", \"subscribers\": {}}}", topic_name, subscriber_count));
                                        }
                                        
                                        json_response.push_str("\n  ]\n");
                                        json_response.push_str("}");
                                        
                                        let status_event = ExecutorEvent::new_system_response(
                                            message.request_id.clone(),
                                            topic_name,
                                            json_response,
                                            task_id.clone()
                                        );
                                        
                                        if let Err(e) = message.response_channel.send(status_event) {
                                            log::warn!("Failed to send status response to task {}: {}", task_id, e);
                                        } else {
                                            log::info!("Sent status response to task {}", task_id);
                                        }
                                    },
                                    SystemCommand::Unknown { command, data } => {
                                        log::warn!("Unknown system command '{}' with data '{}' from task {}", command, data, task_id);
                                    }
                                }
                            }
                            None => {
                                log::info!("SystemCommand receiver closed");
                                break;
                            }
                        }
                    }
                }
            }
            log::info!("SystemCommand worker stopped");
        });
    }
}

pub struct TopicRequestWorker {
    pub topic_channel: TopicChannel,
    pub topic_subscriber: TopicSubscriber,
}

impl TopicRequestWorker {
    pub fn new(topic_channel: TopicChannel, topic_subscriber: TopicSubscriber) -> Self {
        Self {
            topic_channel,
            topic_subscriber,
        }
    }

    pub fn start_worker(
        mut topic_request_receiver: TopicRequestReceiver,
        subscriber_manager: TopicSubscriber,
        shutdown_token: CancellationToken,
    ) {
        task::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    
                    _ = shutdown_token.cancelled() => {
                        log::info!("Topic request worker received shutdown signal");
                        break;
                    }
                    
                    maybe = async { topic_request_receiver.recv().await } => {
                        match maybe {
                            Some(request) => {
                                let task_id: TaskId = request.task_id.clone();
                                let topic: String = request.topic.clone();
                                
                                match request.request_type {
                                    TopicRequestType::Subscribe => {
                                        log::info!("New subscribe request for '{}' from task {}", topic, task_id);
                                        subscriber_manager.add_subscriber(topic.clone(), task_id.clone(), request.response_channel).await;
                                    },
                                    TopicRequestType::Unsubscribe => {
                                        log::info!("New unsubscribe request for '{}' from task {}", topic, task_id);
                                        let removed: bool = subscriber_manager.remove_subscriber_by_task(topic.clone(), task_id.clone()).await;
                                        if removed {
                                            log::info!("Successfully removed subscriber for task {} from topic '{}'", task_id, topic);
                                        } else {
                                            log::warn!("Failed to remove subscriber for task {} from topic '{}'", task_id, topic);
                                        }
                                    }
                                }
                            }
                            None => {
                                log::info!("Topic request receiver closed");
                                break;
                            }
                        }
                    }
                }
            }
            log::info!("Topic request worker stopped");
        });
    }
}

pub type TaskMessageData = String;

#[derive(Debug, Clone)]
pub enum ExecutorEventKind {
    Message {
        message_id: MessageId,
        topic: String,
        data: TaskMessageData,
    },
    SystemResponse {
        request_id: MessageId,
        topic: String,
        data: TaskMessageData,
    },
    SystemError {
        request_id: MessageId,
        error: String,
    },
    SystemCommand {
        command: SystemCommand,
    },
    Error {
        error: String,
    },
    Exit {
        exit_code: i32,
    },
}

#[derive(Debug, Clone)]
pub struct ExecutorEvent {
    pub kind: ExecutorEventKind,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub task_id: TaskId,
}

impl ExecutorEvent {
    pub fn new_message(topic: String, data: TaskMessageData, task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::Message { 
                message_id: MessageId::new(),
                topic, 
                data 
            },
            timestamp: chrono::Utc::now(),
            task_id,
        }
    }


    pub fn new_error(error: String, task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::Error { error },
            timestamp: chrono::Utc::now(),
            task_id,
        }
    }

    pub fn new_exit(exit_code: i32, task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::Exit { exit_code },
            timestamp: chrono::Utc::now(),
            task_id,
        }
    }


    pub fn new_systemcommand(command: SystemCommand, task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::SystemCommand { command },
            timestamp: chrono::Utc::now(),
            task_id,
        }
    }

    pub fn new_system_response(request_id: MessageId, topic: String, data: TaskMessageData, task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::SystemResponse { 
                request_id, 
                topic,
                data 
            },
            timestamp: chrono::Utc::now(),
            task_id,
        }
    }

    pub fn new_system_error(request_id: MessageId, error: String, task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::SystemError { 
                request_id, 
                error 
            },
            timestamp: chrono::Utc::now(),
            task_id,
        }
    }


    pub fn data(&self) -> Option<&TaskMessageData> {
        match &self.kind {
            ExecutorEventKind::Message { data, .. } => Some(data),
            ExecutorEventKind::SystemResponse { data, .. } => Some(data),
            _ => None,
        }
    }

    pub fn message_id(&self) -> Option<&MessageId> {
        match &self.kind {
            ExecutorEventKind::Message { message_id, .. } => Some(message_id),
            ExecutorEventKind::SystemResponse { request_id, .. } => Some(request_id),
            ExecutorEventKind::SystemError { request_id, .. } => Some(request_id),
            _ => None,
        }
    }

    pub fn topic(&self) -> Option<&String> {
        match &self.kind {
            ExecutorEventKind::Message { topic, .. } => Some(topic),
            ExecutorEventKind::SystemResponse { topic, .. } => Some(topic),
            ExecutorEventKind::SystemCommand { command } => {
                match command {
                    SystemCommand::SubscribeTopic { topic } | 
                    SystemCommand::UnsubscribeTopic { topic } => Some(topic),
                    SystemCommand::Stdout { .. } | 
                    SystemCommand::Stderr { .. } => None,
                    _ => None,
                }
            },
            _ => None,
        }
    }

    pub fn timestamp(&self) -> &chrono::DateTime<chrono::Utc> {
        &self.timestamp
    }

    pub fn task_id(&self) -> &TaskId {
        &self.task_id
    }
}

pub struct TaskSpawner {
    pub task_id: TaskId,
    pub topic_request_sender: TopicRequestSender,
    pub topic_subscriber: TopicSubscriber,
    pub system_command_sender: SystemCommandSender,
    pub task_registry: TaskRegistry,
    pub task_name: String,
}

impl TaskSpawner {
    pub fn new(
        task_id: TaskId,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        system_command_sender: SystemCommandSender,
        task_registry: TaskRegistry,
        task_name: String,
    ) -> Self {
        Self {
            task_id,
            topic_request_sender,
            topic_subscriber,
            system_command_sender,
            task_registry,
            task_name,
        }
    }

    pub async fn spawn_executor<E>(
        self,
        executor: E,
        config: E::Config,
        shutdown_token: CancellationToken,
        initial_topics: Option<Vec<String>>,
    ) -> tokio::task::JoinHandle<()>
    where
        E: Executor + 'static,
        E::Event: 'static + Into<ExecutorEvent>,
        E::Config: 'static,
    {
        let task_id: TaskId = self.task_id.clone();
        let topic_request_sender: TopicRequestSender = self.topic_request_sender;
        let topic_subscriber: TopicSubscriber = self.topic_subscriber;
        let system_command_sender: SystemCommandSender = self.system_command_sender;
        let task_registry: TaskRegistry = self.task_registry;
        let task_name: String = self.task_name;

        tokio::task::spawn(async move {
            let topic_data_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let mut topic_data_receiver = topic_data_channel.receiver;
            
            let mut executor_handle = match executor.spawn(config, task_id.clone()).await {
                Ok(handle) => handle,
                Err(e) => {
                    log::error!("Failed to spawn executor for task {}: {}", task_id, e);
                    return;
                }
            };

            if let Some(topics) = initial_topics {
                log::info!("Processing initial topic subscriptions for task {}: {:?}", task_id, topics);
                for topic in topics {
                    if let Err(e) = topic_request_sender.send_subscribe_request(
                        topic.clone(),
                        task_id.clone(),
                        topic_data_channel.sender.clone()
                    ) {
                        log::warn!("Failed to send initial topic request for '{}' (task {}): {}", topic, task_id, e);
                    } else {
                        log::info!("Sent initial topic request for '{}' from task {}", topic, task_id);
                    }
                }
            }


            loop {
                tokio::select! {
                    biased;
                    
                    _ = shutdown_token.cancelled() => {
                        log::info!("Task {} received shutdown signal", task_id);
                        let _ = executor_handle.shutdown_sender.shutdown();
                        break;
                    },
                    
                    event = executor_handle.event_receiver.recv() => {
                        match event {
                            Some(executor_event) => {
                                let event: ExecutorEvent = executor_event;
                                
                                match &event.kind {
                                    ExecutorEventKind::Message { message_id, topic, data } => {
                                        log::info!("Message event {} for task {} on topic '{}': '{}'", message_id, task_id, topic, data);
                                        match topic_subscriber.broadcast_message(&topic, event.clone()).await {
                                            Ok(success_count) => {
                                                log::info!("Broadcasted message from task {} to {} subscribers on topic '{}'", task_id, success_count, topic);
                                            },
                                            Err(e) => {
                                                log::error!("Failed to broadcast message from task {} on topic '{}': {}", task_id, topic, e);
                                            }
                                        }
                                    },
                                    ExecutorEventKind::SystemResponse { request_id, data, topic: _ } => {
                                        log::info!("SystemResponse event {} for task {}: '{}'", request_id, task_id, data);
                                    },
                                    ExecutorEventKind::SystemError { request_id, error } => {
                                        log::error!("SystemError event {} for task {}: '{}'", request_id, task_id, error);
                                    },
                                    ExecutorEventKind::SystemCommand { command } => {
                                        log::info!("SystemCommand event for task {}: {:?}", task_id, command);
                                        
                                        if let Err(e) = system_command_sender.send_system_command(
                                            command.clone(),
                                            task_id.clone(),
                                            topic_data_channel.sender.clone()
                                        ) {
                                            log::warn!("Failed to send system command to worker (task {}): {}", task_id, e);
                                        } else {
                                            log::info!("Sent system command to worker for task {}", task_id);
                                        }
                                    },
                                    ExecutorEventKind::Error { error } => {
                                        log::error!("Error event for task {}: '{}'", task_id, error);
                                    },
                                    ExecutorEventKind::Exit { exit_code } => {
                                        log::info!("Exit event for task {} with exit code: {}", task_id, exit_code);
                                        
                                        let removed_topics: Vec<String> = topic_subscriber.remove_all_subscriptions_by_task(task_id.clone()).await;
                                        log::info!("Task {} exited, removed {} topic subscriptions", task_id, removed_topics.len());
                                        
                                        if let Some(_removed_task) = task_registry.unregister_task(&task_name).await {
                                            log::info!("Removed task '{}' from registry", task_name);
                                        } else {
                                            log::warn!("Task '{}' not found in registry during cleanup", task_name);
                                        }
                                    }
                                }
                            },
                            None => {
                                log::info!("Executor event receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },
                    
                    topic_data = topic_data_receiver.recv() => {
                        
                        match topic_data {
                            Some(topic_data) => {
                                if let Some(data) = topic_data.data() {
                                    let lines: Vec<&str> = data.lines().collect();
                                    
                                    // トピック名は必須
                                    if let Some(topic_name) = topic_data.topic() {
                                        if let Err(e) = executor_handle.input_channel.sender.send(topic_name.clone()) {
                                            log::warn!("Failed to send topic name to executor for task {}: {}", task_id, e);
                                            continue;
                                        }
                                    } else {
                                        log::warn!("Topic data received without topic name for task {}, skipping", task_id);
                                        continue;
                                    }
                                    
                                    if let Err(e) = executor_handle.input_channel.sender.send(lines.len().to_string()) {
                                        log::warn!("Failed to send line count to executor for task {}: {}", task_id, e);
                                        continue;
                                    }
                                    
                                    for line in lines {
                                        if let Err(e) = executor_handle.input_channel.sender.send(line.to_string()) {
                                            log::warn!("Failed to send line to executor for task {}: {}", task_id, e);
                                            break;
                                        }
                                    }
                                }
                            },
                            None => {
                                log::info!("Topic data receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    }
                }
            }
            
            log::info!("Executor task {} completed", task_id);
        })
    }
}

pub struct ExecutorHandle {
    pub event_receiver: ExecutorEventReceiver,
    pub input_channel: InputChannel,
    pub shutdown_sender: ShutdownSender,
}

#[async_trait]
pub trait Executor: Send + Sync {
    type Event: Send + Clone;
    type Config: Send + Clone;
    
    async fn spawn(&self, config: Self::Config, task_id: TaskId) -> Result<ExecutorHandle, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemCommand {
    SubscribeTopic { topic: String },
    UnsubscribeTopic { topic: String },
    Stdout { data: String },
    Stderr { data: String },
    StartTask { task_name: String },
    StopTask { task_name: String },
    KillServer,
    AddTaskFromToml { toml_data: String },
    Status,
    Unknown { command: String, data: String },
}

impl SystemCommand {
    fn get_topic_name(&self) -> String {
        match self {
            SystemCommand::SubscribeTopic { topic } => format!("system.subscribe-topic.{}", topic),
            SystemCommand::UnsubscribeTopic { topic } => format!("system.unsubscribe-topic.{}", topic),
            SystemCommand::Stdout { .. } => "system.stdout".to_string(),
            SystemCommand::Stderr { .. } => "system.stderr".to_string(),
            SystemCommand::StartTask { task_name } => format!("system.start-task.{}", task_name),
            SystemCommand::StopTask { task_name } => format!("system.stop-task.{}", task_name),
            SystemCommand::KillServer => "system.killserver".to_string(),
            SystemCommand::AddTaskFromToml { .. } => "system.add-task-from-toml".to_string(),
            SystemCommand::Status => "system.status".to_string(),
            SystemCommand::Unknown { command, .. } => command.clone(),
        }
    }

    fn parse_add_task_from_toml(data: &str) -> Option<Self> {
        if data.trim().is_empty() {
            return None;
        }

        Some(SystemCommand::AddTaskFromToml { 
            toml_data: data.trim().to_string() 
        })
    }

    pub fn parse_from_plaintext(key: &str, data: &str) -> Option<Self> {
        let key_lower: String = key.to_lowercase();
        let data_trimmed: &str = data.trim();
        
        match key_lower.as_str() {
            "system.subscribe-topic" => {
                if !data_trimmed.is_empty() {
                    Some(SystemCommand::SubscribeTopic { 
                        topic: data_trimmed.to_lowercase() 
                    })
                } else {
                    None
                }
            },
            "system.unsubscribe-topic" => {
                if !data_trimmed.is_empty() {
                    Some(SystemCommand::UnsubscribeTopic { 
                        topic: data_trimmed.to_lowercase() 
                    })
                } else {
                    None
                }
            },
            "system.stdout" => {
                Some(SystemCommand::Stdout { 
                    data: data_trimmed.to_string() 
                })
            },
            "system.stderr" => {
                Some(SystemCommand::Stderr { 
                    data: data_trimmed.to_string() 
                })
            },
            "system.start-task" => {
                if !data_trimmed.is_empty() {
                    Some(SystemCommand::StartTask { 
                        task_name: data_trimmed.to_string() 
                    })
                } else {
                    None
                }
            },
            "system.stop-task" => {
                if !data_trimmed.is_empty() {
                    Some(SystemCommand::StopTask { 
                        task_name: data_trimmed.to_string() 
                    })
                } else {
                    None
                }
            },
            "system.killserver" => {
                Some(SystemCommand::KillServer)
            },
            "system.add-task-from-toml" => {
                Self::parse_add_task_from_toml(data)
            },
            "system.status" => {
                Some(SystemCommand::Status)
            },
            _ if key_lower.starts_with("system.") => {
                Some(SystemCommand::Unknown { 
                    command: key_lower, 
                    data: data_trimmed.to_string() 
                })
            },
            _ => None,
        }
    }
    
    pub fn send_system_command(&self, event_tx: &ExecutorEventSender, task_id: TaskId) {
        match self {
            SystemCommand::SubscribeTopic { topic } => {
                log::info!("Executing system.subscribe-topic with topic: '{}'", topic);
                let _ = event_tx.send_system_command(SystemCommand::SubscribeTopic { topic: topic.clone() }, task_id.clone());
            },
            SystemCommand::UnsubscribeTopic { topic } => {
                log::info!("Executing system.unsubscribe-topic with topic: '{}'", topic);
                let _ = event_tx.send_system_command(SystemCommand::UnsubscribeTopic { topic: topic.clone() }, task_id.clone());
            },
            SystemCommand::Stdout { data } => {
                log::info!("Executing system.stdout with data: '{}'", data);
                let _ = event_tx.send_system_command(SystemCommand::Stdout { data: data.clone() }, task_id.clone());
            },
            SystemCommand::Stderr { data } => {
                log::info!("Executing system.stderr with data: '{}'", data);
                let _ = event_tx.send_system_command(SystemCommand::Stderr { data: data.clone() }, task_id.clone());
            },
            SystemCommand::StartTask { task_name } => {
                log::info!("Executing system.start-task with task: '{}'", task_name);
                let _ = event_tx.send_system_command(SystemCommand::StartTask { task_name: task_name.clone() }, task_id.clone());
            },
            SystemCommand::StopTask { task_name } => {
                log::info!("Executing system.stop-task with task: '{}'", task_name);
                let _ = event_tx.send_system_command(SystemCommand::StopTask { task_name: task_name.clone() }, task_id.clone());
            },
            SystemCommand::KillServer => {
                log::info!("Executing system.killserver - initiating server shutdown");
                let _ = event_tx.send_system_command(SystemCommand::KillServer, task_id.clone());
            },
            SystemCommand::AddTaskFromToml { toml_data } => {
                log::info!("Executing system.add-task-from-toml with TOML data");
                let _ = event_tx.send_system_command(SystemCommand::AddTaskFromToml {
                    toml_data: toml_data.clone(),
                }, task_id.clone());
            },
            SystemCommand::Status => {
                log::info!("Executing system.status");
                let _ = event_tx.send_system_command(SystemCommand::Status, task_id.clone());
            },
            SystemCommand::Unknown { command, data } => {
                log::warn!("Unknown system command: '{}' with data: '{}'", command, data);
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct CommandConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
}

impl CommandConfig {
    pub fn new(command: String, args: Vec<String>) -> Self {
        Self {
            command,
            args,
            working_directory: None,
            environment_vars: None,
        }
    }

}

pub struct CommandExecutor;

#[async_trait]
impl Executor for CommandExecutor {
    type Event = ExecutorEvent;
    type Config = CommandConfig;
    
    async fn spawn(&self, config: CommandConfig, task_id: TaskId) -> Result<ExecutorHandle, Error> {
        let event_channel: ExecutorEventChannel = ExecutorEventChannel::new();
        let input_channel: InputChannel = InputChannel::new();
        let mut shutdown_channel = ShutdownChannel::new();
        
        let event_tx_clone: ExecutorEventSender = event_channel.sender.clone();
        let input_sender: InputSender = input_channel.sender;
        let input_receiver_task = input_channel.receiver;

        let _input_sender_clone: InputSender = input_sender.clone();
        let mut input_receiver_task_clone = input_receiver_task;

        task::spawn(async move {
            let mut command_builder = TokioCommand::new(&config.command);
            
            for arg in &config.args {
                command_builder.arg(arg);
            }
            
            if let Some(working_dir) = &config.working_directory {
                command_builder.current_dir(working_dir);
            }

            if let Some(env_vars) = &config.environment_vars {
                for (key, value) in env_vars {
                    command_builder.env(key, value);
                }
            }
            
            let mut child = match command_builder
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .stdin(Stdio::piped())
                .spawn()
            {
                Ok(child) => child,
                Err(e) => {
                    let _ = event_tx_clone.send(ExecutorEvent::new_error(
                        format!("Failed to start process '{}': {}", config.command, e),
                        task_id.clone(),
                    ));
                    return;
                }
            };

            let stdout: tokio::process::ChildStdout = child.stdout.take().unwrap();
            let stderr: tokio::process::ChildStderr = child.stderr.take().unwrap();
            let mut stdin_writer = child.stdin.take().unwrap();


            let cancel_token: CancellationToken = CancellationToken::new();

            let stdout_handle = spawn_stream_reader(
                TokioBufReader::new(stdout),
                "stdout",
                event_tx_clone.clone(),
                cancel_token.clone(),
                task_id.clone(),
            );

            let stderr_handle = spawn_stream_reader(
                TokioBufReader::new(stderr),
                "stderr",
                event_tx_clone.clone(),
                cancel_token.clone(),
                task_id.clone(),
            );

            
            let cancel_input: CancellationToken = cancel_token.clone();
            let event_tx_input: ExecutorEventSender = event_tx_clone.clone();
            let task_id_input: TaskId = task_id.clone();
            let input_handle = task::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_input.cancelled() => { break; }
                        input_data = input_receiver_task_clone.recv() => {
                            match input_data {
                                Some(input_data) => {
                                    let bytes: Vec<u8> = if input_data.ends_with('\n') { input_data.into_bytes() } else { format!("{}\n", input_data).into_bytes() };
                                    if let Err(e) = stdin_writer.write_all(&bytes).await {
                                        let _ = event_tx_input.send_error(format!("Failed to write to stdin: {}", e), task_id_input.clone());
                                        break;
                                    }
                                    if let Err(e) = stdin_writer.flush().await {
                                        let _ = event_tx_input.send_error(format!("Failed to flush stdin: {}", e), task_id_input.clone());
                                        break;
                                    }
                                },
                                None => break,
                            }
                        }
                    }
                }
            });

            let event_tx_status: ExecutorEventSender = event_tx_clone.clone();
            let status_cancel: CancellationToken = cancel_token.clone();
            let task_id_status: TaskId = task_id.clone();
            let status_handle = task::spawn(async move {
                tokio::select! {
                    _ = shutdown_channel.receiver.recv() => {
                        let _ = child.kill().await;
                        match child.wait().await {
                            Ok(exit_status) => {
                                let code: i32 = exit_status.code().unwrap_or(-1);
                                let _ = event_tx_status.send_exit(code, task_id_status.clone());
                            },
                            Err(e) => {
                                log::error!("Error waiting for process: {}", e);
                                let _ = event_tx_status.send_error(format!("Error waiting for process: {}", e), task_id_status.clone());
                            }
                        }
                        status_cancel.cancel();
                    }
                    status = child.wait() => {
                        match status {
                            Ok(exit_status) => {
                                let code: i32 = exit_status.code().unwrap_or(-1);
                                let _ = event_tx_status.send_exit(code, task_id_status.clone());
                            },
                            Err(e) => {
                                log::error!("Error waiting for process: {}", e);
                                let _ = event_tx_status.send_error(format!("Error waiting for process: {}", e), task_id_status.clone());
                            }
                        }
                    }
                }
            });

            let _ = status_handle.await;
            let _ = stdout_handle.await;
            let _ = stderr_handle.await;
            cancel_token.cancel();
            let _ = input_handle.await;
        });
        
        Ok(ExecutorHandle {
            event_receiver: event_channel.receiver,
            input_channel: InputChannel {
                sender: input_sender,
                receiver: InputReceiver::new(mpsc::unbounded_channel::<String>().1),
            },
            shutdown_sender: shutdown_channel.sender,
        })
    }
}

fn spawn_stream_reader<R>(
    mut reader: R,
    label: &'static str,
    event_tx: ExecutorEventSender,
    cancel_token: CancellationToken,
    task_id: TaskId,
) -> task::JoinHandle<()>
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static,
{
    task::spawn(async move {
        let mut buffer = TopicInputBuffer::new();
        let mut line = String::new();
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => { break; }
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => break,
                        Ok(_) => {
                            match consume_stream_line(&mut buffer, &line) {
                                Ok(StreamOutcome::Emit { key, data }) => {
                                    if let Some(system_cmd) = SystemCommand::parse_from_plaintext(&key, &data) {
                                        let _ = event_tx.send_system_command(system_cmd, task_id.clone());
                                    } else {
                                        let _ = event_tx.send_message(key, data, task_id.clone());
                                    }
                                }
                                Ok(StreamOutcome::Plain(output)) => {
                                    let _ = event_tx.send_message(label.to_string(), output, task_id.clone());
                                }
                                Ok(StreamOutcome::None) => {}
                                Err(e) => {
                                    let _ = event_tx.send_error(e, task_id.clone());
                                    let output = crate::buffer::strip_crlf(&line).to_string();
                                    let _ = event_tx.send_message(label.to_string(), output, task_id.clone());
                                }
                            }
                            line.clear();
                        },
                        Err(e) => {
                            log::error!("Error reading from {}: {}", label, e);
                            let _ = event_tx.send_error(format!("Error reading from {}: {}", label, e), task_id.clone());
                            break;
                        }
                    }
                }
            }
        }
    })
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct TaskConfig {
    #[serde(rename = "task_name")]
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
    pub auto_start: Option<bool>,
    pub initial_topics: Option<Vec<String>>,
}

impl TaskConfig {
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        toml::from_str(toml_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse TOML: {}", e))
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ServerConfig {
    #[serde(skip)]
    pub config_file: Option<String>,
    pub tasks: Vec<TaskConfig>,
}

impl ServerConfig {
    pub fn from_toml(toml_content: &str) -> Result<Self> {
        let mut config: ServerConfig = toml::from_str(toml_content)?;
        config.config_file = None; // ファイルパスなしで初期化
        config.validate()?;
        Ok(config)
    }
    
    pub fn from_file(config_file: String) -> Result<Self> {
        let config_content: String = std::fs::read_to_string(&config_file)?;
        let mut config = Self::from_toml(&config_content)?;
        config.config_file = Some(config_file);
        Ok(config)
    }
    
    pub fn get_auto_start_tasks(&self) -> Vec<&TaskConfig> {
        self.tasks.iter()
            .filter(|task| task.auto_start.unwrap_or(true))
            .collect()
    }
    
    pub fn validate(&self) -> Result<()> {
        if self.tasks.is_empty() {
            return Err(anyhow::anyhow!("No tasks configured"));
        }
        
        for (index, task) in self.tasks.iter().enumerate() {
            if task.name.is_empty() {
                return Err(anyhow::anyhow!("Task {} has empty name", index));
            }
            
            if task.command.is_empty() {
                return Err(anyhow::anyhow!("Task '{}' has empty command", task.name));
            }
            
            if let Some(working_dir) = &task.working_directory {
                if !std::path::Path::new(working_dir).exists() {
                    return Err(anyhow::anyhow!("Task '{}' working directory '{}' does not exist", task.name, working_dir));
                }
            }
            
            if let Some(initial_topics) = &task.initial_topics {
                for (topic_index, topic) in initial_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!("Task '{}' has empty initial topic at index {}", task.name, topic_index));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!("Task '{}' initial topic '{}' contains spaces (not allowed)", task.name, topic));
                    }
                }
            }
        }
        
        Ok(())
    }
}

#[derive(Debug)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub task_name: String,
    pub shutdown_sender: ShutdownSender,
    pub task_handle: tokio::task::JoinHandle<()>,
}

#[derive(Clone)]
pub struct TaskRegistry {
    running_tasks: Arc<RwLock<HashMap<String, RunningTask>>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_task(&self, task_name: String, task: RunningTask) {
        let mut running_tasks = self.running_tasks.write().await;
        running_tasks.insert(task_name, task);
    }


    pub async fn try_register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        let mut running_tasks = self.running_tasks.write().await;
        let is_running = running_tasks.contains_key(&task_name);
        log::info!("Task '{}' is_running={}", task_name, is_running);
        
        if is_running {
            log::warn!("Task '{}' is already running - cancelling new task start", task_name);
            Err(format!("Task '{}' is already running", task_name))
        } else {
            running_tasks.insert(task_name.clone(), task);
            log::info!("Registered new task '{}'", task_name);
            Ok(())
        }
    }

    pub async fn unregister_task(&self, task_name: &str) -> Option<RunningTask> {
        let mut running_tasks = self.running_tasks.write().await;
        running_tasks.remove(task_name)
    }

    pub async fn stop_task(&self, task_name: &str) -> Result<()> {
        let running_task = {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.remove(task_name)
        };
        
        match running_task {
            Some(task) => {
                log::info!("Attempting to stop task '{}' (ID: {})", task_name, task.task_id);
                
                if task.task_handle.is_finished() {
                    log::info!("Task '{}' already finished, skipping shutdown signal", task_name);
                } else {
                    if let Err(e) = task.shutdown_sender.shutdown() {
                        log::debug!("Task '{}' already terminated, shutdown signal not needed: {}", task_name, e);
                    }
                }
                
                task.task_handle.abort();
                
                info!("Successfully stopped task '{}' (ID: {})", task_name, task.task_id);
                Ok(())
            },
            None => {
                log::warn!("Attempted to stop non-running task '{}'", task_name);
                Err(anyhow::anyhow!("Task '{}' is not running", task_name))
            }
        }
    }


    pub async fn cleanup_finished_tasks(&self) -> Vec<String> {
        let mut finished_tasks = Vec::new();
        let mut running_tasks = self.running_tasks.write().await;
        
        running_tasks.retain(|name, task| {
            if task.shutdown_sender.is_closed() {
                finished_tasks.push(name.clone());
                false
            } else {
                true
            }
        });
        
        if !finished_tasks.is_empty() {
            log::info!("Cleaned up {} finished tasks: {:?}", finished_tasks.len(), finished_tasks);
        }
        
        finished_tasks
    }

    pub async fn get_task_count(&self) -> usize {
        let running_tasks = self.running_tasks.read().await;
        running_tasks.len()
    }

    pub async fn get_running_tasks_info(&self) -> Vec<(String, TaskId)> {
        let running_tasks = self.running_tasks.read().await;
        running_tasks.iter()
            .map(|(name, task)| (name.clone(), task.task_id.clone()))
            .collect()
    }

    pub async fn start_task_from_config(
        &self,
        task_config: &TaskConfig,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        system_command_sender: SystemCommandSender,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        log::info!("Starting task '{}'", task_config.name);
        
        
        if !std::path::Path::new(&task_config.command).exists() && !which::which(&task_config.command).is_ok() {
            return Err(anyhow::anyhow!("Command '{}' not found in PATH or file system", task_config.command));
        }

        if let Some(working_dir) = &task_config.working_directory {
            if !std::path::Path::new(working_dir).exists() {
                return Err(anyhow::anyhow!("Working directory '{}' does not exist", working_dir));
            }
        }
        
        let task_id_new = TaskId::new();
        let shutdown_channel = ShutdownChannel::new();
        
        let command_config = CommandConfig {
            command: task_config.command.clone(),
            args: task_config.args.clone(),
            working_directory: task_config.working_directory.clone(),
            environment_vars: task_config.environment_vars.clone(),
        };
        
        let executor = CommandExecutor;
        let initial_topics = task_config.initial_topics.clone();
        
        let task_spawner = TaskSpawner::new(
            task_id_new.clone(),
            topic_request_sender,
            topic_subscriber,
            system_command_sender,
            self.clone(),
            task_config.name.clone(),
        );
        
        let task_handle = task_spawner.spawn_executor(
            executor,
            command_config,
            shutdown_token,
            initial_topics,
        ).await;
        
        let running_task = RunningTask {
            task_id: task_id_new.clone(),
            task_name: task_config.name.clone(),
            shutdown_sender: shutdown_channel.sender,
            task_handle,
        };
        
        if let Err(e) = self.try_register_task(task_config.name.clone(), running_task).await {
            return Err(anyhow::anyhow!("Failed to register task '{}': {}", task_config.name, e));
        }
        
        log::info!("Successfully started task '{}' (ID: {})", task_config.name, task_id_new);
        Ok(())
    }

    pub async fn start_single_task(
        &self,
        task_name: &str,
        config: &ServerConfig,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        system_command_sender: SystemCommandSender,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        let task_config = config.tasks.iter().find(|t| t.name == task_name);
        match task_config {
            Some(task_config) => {
                self.start_task_from_config(
                    task_config,
                    topic_request_sender,
                    topic_subscriber,
                    system_command_sender,
                    shutdown_token,
                ).await
            },
            None => Err(anyhow::anyhow!("Task '{}' not found in configuration", task_name))
        }
    }

    pub async fn add_task_from_toml(
        &self,
        toml_data: &str,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        system_command_sender: SystemCommandSender,
        shutdown_token: CancellationToken,
    ) -> Result<()> {
        let task_config = TaskConfig::from_toml(toml_data)?;
        
        if task_config.auto_start.unwrap_or(false) {
            log::info!("Auto-starting task '{}' from TOML", task_config.name);
            self.start_task_from_config(
                &task_config,
                topic_request_sender,
                topic_subscriber,
                system_command_sender,
                shutdown_token,
            ).await
        } else {
            log::info!("Task '{}' added to registry from TOML (auto_start=false, waiting for start command)", task_config.name);
            Ok(())
        }
    }

}

pub struct MiclowServer {
    pub config: ServerConfig,
    topic_channel: TopicChannel,
    topic_subscriber: TopicSubscriber,
    system_command_channel: SystemCommandChannel,
    task_registry: TaskRegistry,
    shutdown_token: CancellationToken,
}

impl MiclowServer {
    pub fn new(config: ServerConfig) -> Self {
        let topic_channel: TopicChannel = TopicChannel::new();
        let topic_subscriber: TopicSubscriber = TopicSubscriber::new();
        let system_command_channel: SystemCommandChannel = SystemCommandChannel::new();
        let shutdown_token: CancellationToken = CancellationToken::new();
        let task_registry: TaskRegistry = TaskRegistry::new();
        
        Self {
            config,
            topic_channel,
            topic_subscriber,
            system_command_channel,
            task_registry,
            shutdown_token,
        }
    }

    fn start_topic_request_worker(
        topic_request_receiver: TopicRequestReceiver,
        subscriber_manager: TopicSubscriber,
        shutdown_token: CancellationToken,
    ) {
        log::info!("Starting topic request worker");
        TopicRequestWorker::start_worker(topic_request_receiver, subscriber_manager, shutdown_token);
    }

    fn start_system_command_worker(
        system_command_receiver: SystemCommandReceiver,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        task_registry: TaskRegistry,
        config: ServerConfig,
        system_command_sender: SystemCommandSender,
        shutdown_token: CancellationToken,
    ) {
        log::info!("Starting system command worker");
        SystemCommandWorker::start_worker(system_command_receiver, topic_request_sender, topic_subscriber, task_registry, config, system_command_sender, shutdown_token);
    }


    async fn start_user_tasks(
        config: &ServerConfig,
        task_registry: &TaskRegistry,
        topic_request_sender: TopicRequestSender,
        topic_subscriber: TopicSubscriber,
        system_command_sender: SystemCommandSender,
        shutdown_token: CancellationToken,
    ) {
        let auto_start_tasks: Vec<&TaskConfig> = config.get_auto_start_tasks();
        
        for task_config in auto_start_tasks.iter() {
            let task_name: String = task_config.name.clone();
            
            match task_registry.start_single_task(
                &task_name,
                config,
                topic_request_sender.clone(),
                topic_subscriber.clone(),
                system_command_sender.clone(),
                shutdown_token.clone(),
            ).await {
                Ok(_) => {
                    info!("Started user task {} with command: {} {}", 
                          task_name, 
                          task_config.command, 
                          task_config.args.join(" "));
                },
                Err(e) => {
                    log::error!("Failed to start auto-start task '{}': {}", task_name, e);
                    continue;
                }
            }
        }
        
        if auto_start_tasks.is_empty() {
            info!("No auto-start tasks configured");
        } else {
            info!("Started {} user tasks from configuration", auto_start_tasks.len());
        }
    }

    pub async fn start_server(
        self,
    ) -> Result<()> {
        let topic_req_receiver: TopicRequestReceiver = self.topic_channel.receiver;
        let topic_sender: TopicRequestSender = self.topic_channel.sender;
        let topic_subscriber: TopicSubscriber = self.topic_subscriber.clone();
        let system_command_receiver: SystemCommandReceiver = self.system_command_channel.receiver;
        let system_command_sender: SystemCommandSender = self.system_command_channel.sender;

        Self::start_topic_request_worker(topic_req_receiver, topic_subscriber.clone(), self.shutdown_token.clone());
        Self::start_system_command_worker(system_command_receiver, topic_sender.clone(), topic_subscriber.clone(), self.task_registry.clone(), self.config.clone(), system_command_sender.clone(), self.shutdown_token.clone());

        Self::start_user_tasks(
            &self.config,
            &self.task_registry,
            topic_sender.clone(),
            topic_subscriber,
            system_command_sender,
            self.shutdown_token.clone(),
        ).await;

        info!("All workers and user tasks started successfully");
        
        let topic_request_sender: TopicRequestSender = topic_sender;
        
        log::info!("Server running. Press Ctrl+C to stop.");
        println!("Server running. Press Ctrl+C to stop.");
        
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to create SIGINT signal handler");
        sigint.recv().await;
        
        log::info!("Received shutdown signal, stopping all workers...");
        
        Self::shutdown_workers(
            topic_request_sender,
            self.task_registry,
            self.shutdown_token,
        ).await;
        
        log::info!("Server shutdown completed");
        println!("Graceful shutdown completed");
        
        Ok(())
    }

    pub async fn trigger_graceful_shutdown(
        topic_request_sender: TopicRequestSender,
        task_registry: TaskRegistry,
        shutdown_token: CancellationToken,
    ) {
        Self::shutdown_workers(topic_request_sender, task_registry, shutdown_token).await;
    }

    async fn shutdown_workers(
        topic_request_sender: TopicRequestSender,
        task_registry: TaskRegistry,
        shutdown_token: CancellationToken,
    ) {
        log::info!("Starting graceful shutdown...");
        
        let running_tasks = task_registry.running_tasks.read().await;
        let task_names: Vec<String> = running_tasks.keys().cloned().collect();
        drop(running_tasks);
        
        for task_name in task_names {
            if let Err(e) = task_registry.stop_task(&task_name).await {
                log::warn!("Failed to stop task '{}': {}", task_name, e);
            }
        }
        
        log::info!("Cancelling all tasks...");
        shutdown_token.cancel();
        
        log::info!("Closing all communication channels...");
        
        drop(topic_request_sender);
        
        log::info!("All channels closed, waiting for workers to finish...");
        
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        
        log::info!("Graceful shutdown completed");
    }
}