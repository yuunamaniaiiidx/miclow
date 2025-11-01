use anyhow::{Result, Error};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::process::Command as TokioCommand;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, stdin};
use log::info;
use tokio::task;
use uuid::Uuid;
use tokio_util::sync::CancellationToken;
use async_trait::async_trait;
use std::process::Stdio;
use crate::buffer::{InputBufferManager, StreamOutcome};
use crate::logging::{UserLogEvent, UserLogKind, spawn_user_log_aggregator, LogEvent, spawn_log_aggregator, set_channel_logger, level_from_env};
use tokio::task::JoinHandle;
use tokio::sync::mpsc::UnboundedSender as TokioUnboundedSender;
 

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(Uuid);

impl TaskId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}


#[derive(Debug, Clone)]
pub struct SystemCommandMessage {
    pub command: SystemCommand,
    pub task_id: TaskId,
    pub response_channel: ExecutorEventSender,
}

impl SystemCommandMessage {
    pub fn new(command: SystemCommand, task_id: TaskId, response_channel: ExecutorEventSender) -> Self {
        Self {
            command,
            task_id,
            response_channel,
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
pub struct SystemCommandManager {
    commands: Arc<RwLock<Vec<SystemCommandMessage>>>,
    shutdown_token: CancellationToken,
}

impl SystemCommandManager {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            commands: Arc::new(RwLock::new(Vec::new())),
            shutdown_token,
        }
    }

    pub async fn add_command(&self, message: SystemCommandMessage) -> Result<(), String> {
        let mut commands = self.commands.write().await;
        commands.push(message);
        Ok(())
    }

    pub async fn send_system_command(&self, command: SystemCommand, task_id: TaskId, response_channel: ExecutorEventSender) -> Result<(), String> {
        let message = SystemCommandMessage::new(command, task_id, response_channel);
        self.add_command(message).await
    }

    pub async fn recv_command(&self) -> Option<SystemCommandMessage> {
        loop {
            if self.shutdown_token.is_cancelled() {
                return None;
            }

            {
                let mut commands = self.commands.write().await;
                if let Some(message) = commands.pop() {
                    return Some(message);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

}


#[derive(Clone)]
pub struct TopicManager {
    subscribers: Arc<RwLock<HashMap<String, Arc<Vec<Arc<ExecutorEventSender>>>>>>,
    task_subscriptions: Arc<RwLock<HashMap<(String, TaskId), Weak<ExecutorEventSender>>>>,
}

impl TopicManager {
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

pub fn start_system_command_worker(
    system_command_manager: SystemCommandManager,
    topic_manager: TopicManager,
    task_registry: TaskRegistry,
    config: ServerConfig,
    shutdown_token: CancellationToken,
    userlog_sender: TokioUnboundedSender<UserLogEvent>,
) -> tokio::task::JoinHandle<()> {
    task::spawn(async move {
        loop {
            tokio::select! {
                biased;
                
                _ = shutdown_token.cancelled() => {
                    log::info!("SystemCommand worker received shutdown signal");
                    break;
                }
                
                maybe = async { system_command_manager.recv_command().await } => {
                        match maybe {
                            Some(message) => {
                                let task_id: TaskId = message.task_id.clone();
                                let command: SystemCommand = message.command.clone();
                                
                                log::info!("Processing SystemCommand for task {}: {:?}", task_id, command);
                                
                                let command_clone = command.clone();
                                match command {
                                    SystemCommand::SubscribeTopic { topic } => {
                                        log::info!("Processing SubscribeTopic command for task {}: '{}'", task_id, topic);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        topic_manager.add_subscriber(
                                            topic.clone(),
                                            task_id.clone(), 
                                            message.response_channel.clone()
                                        ).await;
                                        
                                        log::info!("Successfully subscribed to topic '{}'", topic);
                                        
                                        let success_msg = format!("Successfully subscribed to topic '{}'", topic);
                                        let success_event = ExecutorEvent::new_system_response(
                                            topic_name,
                                            success_msg,
                                            task_id.clone()
                                        );
                                        let _ = message.response_channel.send(success_event);
                                    },
                                    SystemCommand::UnsubscribeTopic { topic } => {
                                        log::info!("Processing UnsubscribeTopic command for task {}: '{}'", task_id, topic);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        let removed = topic_manager.remove_subscriber_by_task(
                                            topic.clone(),
                                            task_id.clone()
                                        ).await;
                                        
                                        if removed {
                                            log::info!("Successfully unsubscribed from topic '{}'", topic);
                                            
                                            let success_msg = format!("Successfully unsubscribed from topic '{}'", topic);
                                            let success_event = ExecutorEvent::new_system_response(
                                                topic_name,
                                                success_msg,
                                                task_id.clone()
                                            );
                                            let _ = message.response_channel.send(success_event);
                                        } else {
                                            log::warn!("Failed to unsubscribe from topic '{}'", topic);
                                            
                                            let error_msg = format!("Failed to unsubscribe from topic '{}'", topic);
                                            let error_event = ExecutorEvent::new_system_error(
                                                error_msg,
                                                task_id.clone()
                                            );
                                            let _ = message.response_channel.send(error_event);
                                        }
                                    },
                                    
                                    SystemCommand::StartTask { task_name } => {
                                        log::info!("Processing StartTask command for task {}: '{}'", task_id, task_name);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        match task_registry.start_single_task(
                                            &task_name,
                                            &config,
                                            topic_manager.clone(),
                                            system_command_manager.clone(),
                                            shutdown_token.clone(),
                                            userlog_sender.clone(),
                                        ).await {
                                            Ok(_) => {
                                                let success_msg = format!("Successfully started task '{}'", task_name);
                                                log::info!("{}", success_msg);
                                                
                                                let success_event = ExecutorEvent::new_system_response(
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
                                        match task_registry.stop_task_by_name(&task_name).await {
                                            Ok(_) => {
                                                let success_msg = format!("Successfully stopped task '{}'", task_name);
                                                log::info!("{}", success_msg);
                                                
                                                let success_event = ExecutorEvent::new_system_response(
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
                                            topic_name,
                                            shutdown_msg.to_string(),
                                            task_id.clone()
                                        );
                                        let _ = message.response_channel.send(shutdown_event);
                                        
                                        log::info!("Starting graceful shutdown process...");
                                        
                                        let name_to_id = task_registry.name_to_id.read().await;
                                        let task_names: Vec<String> = name_to_id.keys().cloned().collect();
                                        drop(name_to_id);
                                        
                                        for task_name in task_names {
                                            if let Err(e) = task_registry.stop_task_by_name(&task_name).await {
                                                log::warn!("Failed to stop task '{}': {}", task_name, e);
                                            }
                                        }
                                        
                                        log::info!("Cancelling shutdown token to stop all workers");
                                        shutdown_token.cancel();

                                        // Wait until tasks finish gracefully
                                        log::info!("Waiting for workers to finish before exit...");
                                        MiclowServer::shutdown_workers(
                                            task_registry.clone(),
                                            shutdown_token.clone(),
                                        ).await;

                                        log::info!("Graceful shutdown process completed");
                                    },
                                    SystemCommand::AddTaskFromToml { toml_data } => {
                                        log::info!("Processing AddTaskFromToml command for task {} with TOML data", task_id);
                                        
                                        let topic_name = command_clone.get_topic_name();
                                        match task_registry.add_task_from_toml(
                                            &toml_data,
                                            topic_manager.clone(),
                                            system_command_manager.clone(),
                                            shutdown_token.clone(),
                                            userlog_sender.clone(),
                                        ).await {
                                            Ok(_) => {
                                                let success_msg = format!("Successfully added task to registry from TOML");
                                                log::info!("{}", success_msg);
                                                
                                                let success_event = ExecutorEvent::new_system_response(
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
                                        let topics_info = topic_manager.get_topics_info().await;
                                        
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
    })
}


pub type TaskMessageData = String;

#[derive(Debug, Clone)]
pub enum ExecutorEventKind {
    Message {
        topic: String,
        data: TaskMessageData,
    },
    TaskStdout {
        data: TaskMessageData,
    },
    TaskStderr {
        data: TaskMessageData,
    },
    SystemResponse {
        topic: String,
        data: TaskMessageData,
    },
    SystemError {
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
}

impl ExecutorEvent {
    pub fn new_message(topic: String, data: TaskMessageData, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::Message { 
                topic, 
                data 
            },
        }
    }


    pub fn new_error(error: String, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::Error { error },
        }
    }

    pub fn new_exit(exit_code: i32, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::Exit { exit_code },
        }
    }


    pub fn new_systemcommand(command: SystemCommand, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::SystemCommand { command },
        }
    }

    pub fn new_task_stdout(data: TaskMessageData, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::TaskStdout { data },
        }
    }

    pub fn new_task_stderr(data: TaskMessageData, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::TaskStderr { data },
        }
    }

    pub fn new_system_response(topic: String, data: TaskMessageData, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::SystemResponse { 
                topic,
                data 
            },
        }
    }

    pub fn new_system_error(error: String, _task_id: TaskId) -> Self {
        Self {
            kind: ExecutorEventKind::SystemError { 
                error 
            },
        }
    }


    pub fn data(&self) -> Option<&TaskMessageData> {
        match &self.kind {
            ExecutorEventKind::Message { data, .. } => Some(data),
            ExecutorEventKind::TaskStdout { data } => Some(data),
            ExecutorEventKind::TaskStderr { data } => Some(data),
            ExecutorEventKind::SystemResponse { data, .. } => Some(data),
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
                    _ => None,
                }
            },
            _ => None,
        }
    }
}

pub struct TaskSpawner {
    pub task_id: TaskId,
    pub topic_manager: TopicManager,
    pub system_command_manager: SystemCommandManager,
    pub task_registry: TaskRegistry,
    pub task_name: String,
    pub userlog_sender: TokioUnboundedSender<UserLogEvent>,
}

impl TaskSpawner {
    pub fn new(
        task_id: TaskId,
        topic_manager: TopicManager,
        system_command_manager: SystemCommandManager,
        task_registry: TaskRegistry,
        task_name: String,
        userlog_sender: TokioUnboundedSender<UserLogEvent>,
    ) -> Self {
        Self {
            task_id,
            topic_manager,
            system_command_manager,
            task_registry,
            task_name,
            userlog_sender,
        }
    }

    pub async fn spawn_executor<E>(
        self,
        executor: E,
        config: E::Config,
        shutdown_token: CancellationToken,
        subscribe_topics: Option<Vec<String>>,
    ) -> tokio::task::JoinHandle<()>
    where
        E: Executor + 'static,
        E::Event: 'static + Into<ExecutorEvent>,
        E::Config: 'static,
    {
        let task_id: TaskId = self.task_id.clone();
        let task_name: String = self.task_name.clone();
        let topic_manager: TopicManager = self.topic_manager;
        let system_command_manager: SystemCommandManager = self.system_command_manager;
        let task_registry: TaskRegistry = self.task_registry;
        let userlog_sender = self.userlog_sender.clone();

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

            if let Some(topics) = subscribe_topics {
                log::info!("Processing initial topic subscriptions for task {}: {:?}", task_id, topics);
                for topic in topics {
                    topic_manager.add_subscriber(
                        topic.clone(),
                        task_id.clone(),
                        topic_data_channel.sender.clone()
                    ).await;
                    log::info!("Added initial topic subscription for '{}' from task {}", topic, task_id);
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
                                    ExecutorEventKind::Message { topic, data } => {
                                        log::info!("Message event for task {} on topic '{}': '{}'", task_id, topic, data);
                                        match topic_manager.broadcast_message(event.clone()).await {
                                            Ok(success_count) => {
                                                log::info!("Broadcasted message from task {} to {} subscribers on topic '{}'", task_id, success_count, topic);
                                            },
                                            Err(e) => {
                                                log::error!("Failed to broadcast message from task {} on topic '{}': {}", task_id, topic, e);
                                            }
                                        }
                                    },
                                    ExecutorEventKind::SystemResponse { data, topic: _ } => {
                                        log::info!("SystemResponse event for task {}: '{}'", task_id, data);
                                    },
                                    ExecutorEventKind::SystemError { error } => {
                                        log::error!("SystemError event for task {}: '{}'", task_id, error);
                                    },
                                    ExecutorEventKind::TaskStdout { data } => {
                                        let flags = task_registry.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((view_stdout, _)) = flags {
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorEventKind::TaskStderr { data } => {
                                        let flags = task_registry.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((_, view_stderr)) = flags {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorEventKind::SystemCommand { command } => {
                                        log::info!("SystemCommand event for task {}: {:?}", task_id, command);
                                        
                                        if let Err(e) = system_command_manager.send_system_command(
                                            command.clone(),
                                            task_id.clone(),
                                            topic_data_channel.sender.clone()
                                        ).await {
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
                                        
                                        let removed_topics: Vec<String> = topic_manager.remove_all_subscriptions_by_task(task_id.clone()).await;
                                        log::info!("Task {} exited, removed {} topic subscriptions", task_id, removed_topics.len());
                                        
                                        if let Some(_removed_task) = task_registry.unregister_task_by_task_id(&task_id).await {
                                            log::info!("Removed task with TaskId={} (Human name index updated)", task_id);
                                        } else {
                                            log::warn!("Task with TaskId={} not found in registry during cleanup", task_id);
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
                                    
                                    if let Some(topic_name) = topic_data.topic() {
                                        if let Err(e) = executor_handle.input_sender.send(topic_name.clone()) {
                                            log::warn!("Failed to send topic name to executor for task {}: {}", task_id, e);
                                            continue;
                                        }
                                    } else {
                                        log::warn!("Topic data received without topic name for task {}, skipping", task_id);
                                        continue;
                                    }
                                    
                                    if let Err(e) = executor_handle.input_sender.send(lines.len().to_string()) {
                                        log::warn!("Failed to send line count to executor for task {}: {}", task_id, e);
                                        continue;
                                    }
                                    
                                    for line in lines {
                                        if let Err(e) = executor_handle.input_sender.send(line.to_string()) {
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
    pub input_sender: InputSender,
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
    StartTask { task_name: String },
    StopTask { task_name: String },
    KillServer,
    AddTaskFromToml { toml_data: String },
    Status,
    Unknown { command: String, data: String },
}

impl SystemCommand {
    
    pub fn get_topic_name(&self) -> String {
        match self {
            SystemCommand::SubscribeTopic { topic } => format!("system.subscribe-topic.{}", topic),
            SystemCommand::UnsubscribeTopic { topic } => format!("system.unsubscribe-topic.{}", topic),
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
        let key_lower = key.to_lowercase();
        let data_trimmed = data.trim();
        
        if data_trimmed.is_empty() && !matches!(key_lower.as_str(), "system.killserver" | "system.status") {
            return None;
        }
        
        match key_lower.as_str() {
            "system.subscribe-topic" => {
                Some(SystemCommand::SubscribeTopic { 
                    topic: data_trimmed.to_lowercase() 
                })
            },
            "system.unsubscribe-topic" => {
                Some(SystemCommand::UnsubscribeTopic { 
                    topic: data_trimmed.to_lowercase() 
                })
            },
            "system.start-task" => {
                Some(SystemCommand::StartTask { 
                    task_name: data_trimmed.to_string() 
                })
            },
            "system.stop-task" => {
                Some(SystemCommand::StopTask { 
                    task_name: data_trimmed.to_string() 
                })
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
    
}

#[derive(Debug, Clone)]
pub struct CommandConfig {
    pub command: String,
    pub args: Vec<String>,
    pub working_directory: Option<String>,
    pub environment_vars: Option<HashMap<String, String>>,
    pub stdout_topic: String,
    pub stderr_topic: String,
    pub view_stdout: bool,
    pub view_stderr: bool,
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
        let mut input_receiver = input_channel.receiver;

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

            command_builder.env("MICLOW_TASK_ID", task_id.to_string());
            
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
                config.stdout_topic.clone(),
                event_tx_clone.clone(),
                cancel_token.clone(),
                task_id.clone(),
                if config.view_stdout { Some(ExecutorEvent::new_task_stdout as fn(String, TaskId) -> ExecutorEvent) } else { None },
            );

            let stderr_handle = spawn_stream_reader(
                TokioBufReader::new(stderr),
                config.stderr_topic.clone(),
                event_tx_clone.clone(),
                cancel_token.clone(),
                task_id.clone(),
                if config.view_stderr { Some(ExecutorEvent::new_task_stderr as fn(String, TaskId) -> ExecutorEvent) } else { None },
            );

            
            let cancel_input: CancellationToken = cancel_token.clone();
            let event_tx_input: ExecutorEventSender = event_tx_clone.clone();
            let task_id_input: TaskId = task_id.clone();
            let input_handle = task::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_input.cancelled() => { break; }
                        input_data = input_receiver.recv() => {
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
                let notify = |res: std::io::Result<std::process::ExitStatus>| {
                    match res {
                        Ok(exit_status) => {
                            let code: i32 = exit_status.code().unwrap_or(-1);
                            let _ = event_tx_status.send_exit(code, task_id_status.clone());
                        }
                        Err(e) => {
                            log::error!("Error waiting for process: {}", e);
                            let _ = event_tx_status
                                .send_error(format!("Error waiting for process: {}", e), task_id_status.clone());
                        }
                    }
                };
                tokio::select! {
                    _ = shutdown_channel.receiver.recv() => {
                        let _ = child.kill().await;
                        notify(child.wait().await);
                        status_cancel.cancel();
                    }
                    status = child.wait() => {
                        notify(status);
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
            input_sender: input_channel.sender,
            shutdown_sender: shutdown_channel.sender,
        })
    }
}

fn spawn_stream_reader<R>(
    mut reader: R,
    topic_name: String,
    event_tx: ExecutorEventSender,
    cancel_token: CancellationToken,
    task_id: TaskId,
    emit_func: Option<fn(String, TaskId) -> ExecutorEvent>,
) -> task::JoinHandle<()> 
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static
{
    task::spawn(async move {
        let mut buffer_manager = InputBufferManager::new();
        let mut line = String::new();
        let task_id_str = task_id.to_string();
        
        loop {
            let mut flush_unfinished = || {
                let unfinished = buffer_manager.flush_all_unfinished();
                for (_, key, data) in unfinished {
                    if let Some(system_cmd) = SystemCommand::parse_from_plaintext(&key, &data) {
                        let _ = event_tx.send_system_command(system_cmd, task_id.clone());
                    } else {
                        let _ = event_tx.send_message(key, data, task_id.clone());
                    }
                }
            };
            tokio::select! {
                _ = cancel_token.cancelled() => { flush_unfinished(); break; }
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => { flush_unfinished(); break; },
                        Ok(_) => {
                            match buffer_manager.consume_stream_line(&task_id_str, &line) {
                                Ok(StreamOutcome::Emit { key, data }) => {
                                    if let Some(system_cmd) = SystemCommand::parse_from_plaintext(&key, &data) {
                                        let _ = event_tx.send_system_command(system_cmd, task_id.clone());
                                    } else {
                                        let _ = event_tx.send_message(key, data, task_id.clone());
                                    }
                                }
                                Ok(StreamOutcome::Plain(output)) => {
                                    let _ = event_tx.send_message(topic_name.clone(), output.clone(), task_id.clone());
                                    if let Some(emit) = emit_func {
                                        let _ = event_tx.send(emit(output, task_id.clone()));
                                    }
                                }
                                Ok(StreamOutcome::None) => {}
                                Err(e) => {
                                    let _ = event_tx.send_error(e.clone(), task_id.clone());
                                    let output = crate::buffer::strip_crlf(&line).to_string();
                                    let _ = event_tx.send_message(topic_name.clone(), output.clone(), task_id.clone());
                                    if let Some(emit) = emit_func {
                                        let _ = event_tx.send(emit(output, task_id.clone()));
                                    }
                                }
                            }
                            line.clear();
                        },
                        Err(e) => {
                            log::error!("Error reading from {}: {}", topic_name, e);
                            let _ = event_tx.send_error(format!("Error reading from {}: {}", topic_name, e), task_id.clone());
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
    pub subscribe_topics: Option<Vec<String>>,
    pub stdout_topic: Option<String>,
    pub stderr_topic: Option<String>,
    #[serde(default)]
    pub view_stdout: bool,
    #[serde(default)]
    pub view_stderr: bool,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ServerConfig {
    #[serde(skip)]
    pub config_file: Option<String>,
    pub tasks: Vec<TaskConfig>,
    #[serde(default)]
    pub include_paths: Vec<String>,
}

impl ServerConfig {
    pub fn from_toml(toml_content: &str) -> Result<Self> {
        let mut config: ServerConfig = toml::from_str(toml_content)?;
        config.config_file = None;
        config.normalize_defaults();
        config.validate()?;
        Ok(config)
    }
    
    pub fn from_file(config_file: String) -> Result<Self> {
        let config_content: String = std::fs::read_to_string(&config_file)?;
        let mut config = Self::from_toml(&config_content)?;
        config.config_file = Some(config_file.clone());
        
        config.load_includes(&config_file)?;
        config.validate()?;
        
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
            
            if let Some(subscribe_topics) = &task.subscribe_topics {
                for (topic_index, topic) in subscribe_topics.iter().enumerate() {
                    if topic.is_empty() {
                        return Err(anyhow::anyhow!("Task '{}' has empty initial topic at index {}", task.name, topic_index));
                    }
                    if topic.contains(' ') {
                        return Err(anyhow::anyhow!("Task '{}' initial topic '{}' contains spaces (not allowed)", task.name, topic));
                    }
                }
            }

            if let Some(stdout_topic) = &task.stdout_topic {
                if stdout_topic.is_empty() {
                    return Err(anyhow::anyhow!("Task '{}' stdout_topic is empty", task.name));
                }
                if stdout_topic.contains(' ') {
                    return Err(anyhow::anyhow!("Task '{}' stdout_topic '{}' contains spaces (not allowed)", task.name, stdout_topic));
                }
            }
            if let Some(stderr_topic) = &task.stderr_topic {
                if stderr_topic.is_empty() {
                    return Err(anyhow::anyhow!("Task '{}' stderr_topic is empty", task.name));
                }
                if stderr_topic.contains(' ') {
                    return Err(anyhow::anyhow!("Task '{}' stderr_topic '{}' contains spaces (not allowed)", task.name, stderr_topic));
                }
            }
        }
        
        Ok(())
    }
    
    fn load_includes(&mut self, base_config_path: &str) -> Result<()> {
        let mut loaded_files = std::collections::HashSet::new();
        let base_dir = std::path::Path::new(base_config_path)
            .parent()
            .unwrap_or(std::path::Path::new("."));
        
        let include_paths = self.include_paths.clone();
        self.load_includes_recursive(&mut loaded_files, base_dir, &include_paths)?;
        Ok(())
    }
    
    fn load_includes_recursive(
        &mut self,
        loaded_files: &mut std::collections::HashSet<String>,
        base_dir: &std::path::Path,
        include_paths: &[String],
    ) -> Result<()> {
        for include_path in include_paths {
            let full_path = if std::path::Path::new(include_path).is_absolute() {
                include_path.clone()
            } else {
                base_dir.join(include_path).to_string_lossy().to_string()
            };
            
            if loaded_files.contains(&full_path) {
                log::warn!("Circular include detected for file: {}", full_path);
                continue;
            }
            
            if !std::path::Path::new(&full_path).exists() {
                log::warn!("Include file not found: {}", full_path);
                continue;
            }
            
            match std::fs::read_to_string(&full_path) {
                Ok(content) => {
                    loaded_files.insert(full_path.clone());
                    log::info!("Loading include file: {}", full_path);
                    
                    match Self::from_toml(&content) {
                        Ok(included_config) => {
                            self.tasks.extend(included_config.tasks);
                            if !included_config.include_paths.is_empty() {
                                let include_dir = std::path::Path::new(&full_path)
                                    .parent()
                                    .unwrap_or(std::path::Path::new("."));
                                
                                self.load_includes_recursive(
                                    loaded_files,
                                    include_dir,
                                    &included_config.include_paths,
                                )?;
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to parse include file {}: {}", full_path, e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to read include file {}: {}", full_path, e);
                }
            }
        }
        
        Ok(())
    }

    fn normalize_defaults(&mut self) {
        for task in self.tasks.iter_mut() {
            if task.stdout_topic.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.stdout_topic = Some("stdout".to_string());
            }
            if task.stderr_topic.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.stderr_topic = Some("stderr".to_string());
            }
            if task.working_directory.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.working_directory = Some("./".to_string());
            }
            if task.view_stdout != true {
                task.view_stdout = false;
            }
            if task.view_stderr != true {
                task.view_stderr = false;
            }
        }
    }
}

#[derive(Debug)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub shutdown_sender: ShutdownSender,
    pub task_handle: tokio::task::JoinHandle<()>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

#[derive(Clone)]
pub struct TaskRegistry {
    running_tasks: Arc<RwLock<HashMap<TaskId, RunningTask>>>,
    name_to_id: Arc<RwLock<HashMap<String, TaskId>>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            name_to_id: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn try_register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        let mut running_tasks = self.running_tasks.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let task_id = task.task_id.clone();
        if name_to_id.contains_key(&task_name) {
            log::warn!("Task '{}' is already running - cancelling new task start", task_name);
            return Err(format!("Task '{}' is already running", task_name));
        }
        running_tasks.insert(task_id.clone(), task);
        name_to_id.insert(task_name, task_id);
        Ok(())
    }

    pub async fn unregister_task_by_task_id(&self, task_id: &TaskId) -> Option<RunningTask> {
        let mut running_tasks = self.running_tasks.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        if let Some(task) = running_tasks.remove(task_id) {
            name_to_id.retain(|_name, id| id != task_id);
            Some(task)
        } else {
            None
        }
    }
    pub async fn unregister_task_by_name(&self, task_name: &str) -> Option<RunningTask> {
        let id = {
            let name_to_id = self.name_to_id.read().await;
            name_to_id.get(task_name).cloned()
        };
        if let Some(tid) = id {
            self.unregister_task_by_task_id(&tid).await
        } else {
            None
        }
    }

    pub async fn stop_task_by_task_id(&self, task_id: &TaskId) -> Result<()> {
        let running_task = {
            let mut running_tasks = self.running_tasks.write().await;
            running_tasks.remove(task_id)
        };
        match running_task {
            Some(task) => {
                log::info!("Stopped task with TaskId={} (Human name index updated)", task_id);
                let mut name_to_id = self.name_to_id.write().await;
                name_to_id.retain(|_name, id| id != task_id);
                task.task_handle.abort();
                Ok(())
            },
            None => {
                log::warn!("Attempted to stop non-running task TaskId={}", task_id);
                Err(anyhow::anyhow!("TaskId '{}' is not running", task_id))
            }
        }
    }

    pub async fn stop_task_by_name(&self, task_name: &str) -> Result<()> {
        let id = {
            let name_to_id = self.name_to_id.read().await;
            name_to_id.get(task_name).cloned()
        };
        if let Some(tid) = id {
            self.stop_task_by_task_id(&tid).await
        } else {
            Err(anyhow::anyhow!("Task name '{}' not found", task_name))
        }
    }

    pub async fn get_running_tasks_info(&self) -> Vec<(String, TaskId)> {
        let name_to_id = self.name_to_id.read().await;
        name_to_id.iter().map(|(name, task_id)| (name.clone(), task_id.clone())).collect()
    }

    pub async fn get_view_flags_by_task_id(&self, task_id: &TaskId) -> Option<(bool, bool)> {
        let running_tasks_guard = self.running_tasks.read().await;
        running_tasks_guard.get(task_id).map(|t| (t.view_stdout, t.view_stderr))
    }

    pub async fn graceful_shutdown_all(&self, timeout: std::time::Duration) {
        // Drain all running tasks to take ownership of their JoinHandles
        let tasks: Vec<(TaskId, RunningTask)> = {
            let mut running_tasks = self.running_tasks.write().await;
            let drained: Vec<(TaskId, RunningTask)> = running_tasks.drain().collect();
            drained
        };
        {
            let mut name_to_id = self.name_to_id.write().await;
            name_to_id.clear();
        }

        // First, signal shutdown to each task
        for (_tid, task) in &tasks {
            let _ = task.shutdown_sender.shutdown();
        }

        // Then, wait for each task handle to complete with a timeout
        for (_tid, task) in tasks {
            let _ = tokio::time::timeout(timeout, task.task_handle).await;
        }
    }

    pub async fn start_task_from_config(
        &self,
        task_config: &TaskConfig,
        topic_manager: TopicManager,
        system_command_manager: SystemCommandManager,
        shutdown_token: CancellationToken,
        userlog_sender: TokioUnboundedSender<UserLogEvent>,
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
            stdout_topic: task_config.stdout_topic.clone().unwrap(),
            stderr_topic: task_config.stderr_topic.clone().unwrap(),
            view_stdout: task_config.view_stdout,
            view_stderr: task_config.view_stderr,
        };
        
        let executor = CommandExecutor;
        let subscribe_topics = task_config.subscribe_topics.clone();
        
        let task_spawner = TaskSpawner::new(
            task_id_new.clone(),
            topic_manager,
            system_command_manager,
            self.clone(),
            task_config.name.clone(),
            userlog_sender,
        );
        
        let task_handle = task_spawner.spawn_executor(
            executor,
            command_config,
            shutdown_token,
            subscribe_topics,
        ).await;
        
        let running_task = RunningTask {
            task_id: task_id_new.clone(),
            shutdown_sender: shutdown_channel.sender,
            task_handle,
            view_stdout: task_config.view_stdout,
            view_stderr: task_config.view_stderr,
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
        topic_manager: TopicManager,
        system_command_manager: SystemCommandManager,
        shutdown_token: CancellationToken,
        userlog_sender: TokioUnboundedSender<UserLogEvent>,
    ) -> Result<()> {
        let task_config = config.tasks.iter().find(|t| t.name == task_name);
        match task_config {
            Some(task_config) => {
                self.start_task_from_config(
                    task_config,
                    topic_manager,
                    system_command_manager,
                    shutdown_token,
                    userlog_sender,
                ).await
            },
            None => Err(anyhow::anyhow!("Task '{}' not found in configuration", task_name))
        }
    }

    pub async fn add_task_from_toml(
        &self,
        toml_data: &str,
        topic_manager: TopicManager,
        system_command_manager: SystemCommandManager,
        shutdown_token: CancellationToken,
        userlog_sender: TokioUnboundedSender<UserLogEvent>,
    ) -> Result<()> {
        let server_config = ServerConfig::from_toml(toml_data)?;
        
        if server_config.tasks.is_empty() {
            return Err(anyhow::anyhow!("No tasks found in TOML data"));
        }
        
        for task_config in &server_config.tasks {
            if task_config.auto_start.unwrap_or(false) {
                log::info!("Auto-starting task '{}' from TOML", task_config.name);
                if let Err(e) = self.start_task_from_config(
                    task_config,
                    topic_manager.clone(),
                    system_command_manager.clone(),
                    shutdown_token.clone(),
                    userlog_sender.clone(),
                ).await {
                    log::error!("Failed to start task '{}': {}", task_config.name, e);
                }
            } else {
                log::info!("Task '{}' added to registry from TOML (auto_start=false, waiting for start command)", task_config.name);
            }
        }
        
        Ok(())
    }

}

pub struct MiclowServer {
    pub config: ServerConfig,
    topic_manager: TopicManager,
    system_command_manager: SystemCommandManager,
    task_registry: TaskRegistry,
    shutdown_token: CancellationToken,
    background_tasks: BackgroundTaskManager,
}

impl MiclowServer {
    pub fn new(config: ServerConfig) -> Self {
        let topic_manager: TopicManager = TopicManager::new();
        let shutdown_token: CancellationToken = CancellationToken::new();
        let system_command_manager: SystemCommandManager = SystemCommandManager::new(shutdown_token.clone());
        let task_registry: TaskRegistry = TaskRegistry::new();
        Self {
            config,
            topic_manager,
            system_command_manager,
            task_registry,
            shutdown_token,
            background_tasks: BackgroundTaskManager::new(),
        }
    }




    async fn start_user_tasks(
        config: &ServerConfig,
        task_registry: &TaskRegistry,
        topic_manager: TopicManager,
        system_command_manager: SystemCommandManager,
        shutdown_token: CancellationToken,
        userlog_sender: TokioUnboundedSender<UserLogEvent>,
    ) {
        let auto_start_tasks: Vec<&TaskConfig> = config.get_auto_start_tasks();
        
        for task_config in auto_start_tasks.iter() {
            let task_name: String = task_config.name.clone();
            
            match task_registry.start_single_task(
                &task_name,
                config,
                topic_manager.clone(),
                system_command_manager.clone(),
                shutdown_token.clone(),
                userlog_sender.clone(),
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

    pub async fn start_server_with_interactive(
        mut self,
    ) -> Result<()> {
        let topic_manager: TopicManager = self.topic_manager.clone();

        // Initialize ChannelLogger and aggregator for log:: messages
        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        // ログ集約は独立トークンで最後に止める
        let logging_shutdown = CancellationToken::new();
        let h_log = spawn_log_aggregator(log_rx, logging_shutdown.clone());
        self.background_tasks.register("log_aggregator", h_log);

        let (userlog_tx, userlog_rx) = tokio::sync::mpsc::unbounded_channel::<UserLogEvent>();
        let h_userlog = spawn_user_log_aggregator(userlog_rx, logging_shutdown.clone());
        self.background_tasks.register("user_log_aggregator", h_userlog);

        let h_sys = start_system_command_worker(
            self.system_command_manager.clone(),
            topic_manager.clone(),
            self.task_registry.clone(),
            self.config.clone(),
            self.shutdown_token.clone(),
            userlog_tx.clone(),
        );
        self.background_tasks.register("system_command_worker", h_sys);

        Self::start_user_tasks(
            &self.config,
            &self.task_registry,
            topic_manager.clone(),
            self.system_command_manager.clone(),
            self.shutdown_token.clone(),
            userlog_tx,
        ).await;
        
        log::info!("Server running. Press Ctrl+C to stop.");
        println!("Server running. Press Ctrl+C to stop.");
        
        // ★ここでCtrl+Cの監視タスクを追加
        let shutdown_token = self.shutdown_token.clone();
        // インタラクティブ入力は専用タスクとして管理
        let mut interactive_handle = tokio::spawn(Self::start_interactive_mode(
            self.system_command_manager.clone(),
            topic_manager.clone(),
            self.shutdown_token.clone()
        ));
        let ctrlc_fut = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Ctrl+C signal error: {}", e);
            } else {
                log::info!("Received Ctrl+C. Requesting graceful shutdown...");
                shutdown_token.cancel();
            }
        };
        tokio::select! {
            _ = &mut interactive_handle => {},
            _ = ctrlc_fut => {},
        }

        log::info!("Received shutdown signal, stopping all workers...");
        
        Self::shutdown_workers(
            self.task_registry,
            self.shutdown_token,
        ).await;

        // ログを吐き切る猶予を与えた後、ログ集約を停止
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        logging_shutdown.cancel();
        // 念のためインタラクティブタスクも収束させる
        if !interactive_handle.is_finished() {
            interactive_handle.abort();
            let _ = interactive_handle.await;
        }
        // 背景タスクは即時abortで収束させる（ログ集約のブロック回避）
        self.background_tasks.abort_all().await;
        // ロガーのフラッシュを呼び出す（ChannelLoggerのflushは軽量だが呼ぶ）
        log::logger().flush();

        log::info!("Graceful shutdown completed");
        println!("Graceful shutdown completed");
        return Ok(());
    }


    async fn start_interactive_mode(
        system_command_manager: SystemCommandManager,
        topic_manager: TopicManager,
        shutdown_token: CancellationToken
    ) {
        let stdin = stdin();
        let reader = TokioBufReader::new(stdin);
        let mut lines = reader.lines();
        
        let interactive_task_id = TaskId::new();
        
        let interactive_event_channel = ExecutorEventChannel::new();
        let interactive_event_sender = interactive_event_channel.sender;
        
        let system_input_topic = "system";
        
        loop {
            if shutdown_token.is_cancelled() {
                log::info!("Interactive mode received shutdown signal");
                break;
            }
            
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    log::info!("Interactive mode received shutdown signal");
                    break;
                }
                line_result = lines.next_line() => {
                    if let Some(line) = line_result.unwrap_or_default() {
                        let trimmed = line.trim();
                        if trimmed.is_empty() {
                            continue;
                        }
                        
                        let system_input = trimmed;
                        
                        if let Some(system_cmd) = SystemCommand::parse_from_plaintext("", system_input) {
                            log::info!("Sending system command from interactive mode: {:?}", system_cmd);
                            
                            if let Err(e) = system_command_manager.send_system_command(
                                system_cmd,
                                interactive_task_id.clone(),
                                interactive_event_sender.clone()
                            ).await {
                                log::error!("Failed to send system command: {}", e);
                                eprintln!("Failed to send system command: {}", e);
                            } else {
                                log::info!("Successfully sent system command from interactive mode");
                            }
                        } else {
                            log::info!("Sending message topic:'{}'", system_input);
                            
                            let executor_event = ExecutorEvent::new_message(
                                system_input_topic.to_string(),
                                system_input.to_string(),
                                interactive_task_id.clone()
                            );
                            
                            match topic_manager.broadcast_message(executor_event).await {
                                Ok(success_count) => {
                                    log::info!("Successfully broadcasted message to {} subscribers on stdout topic", success_count);
                                },
                                Err(e) => {
                                    log::error!("Failed to broadcast message to stdout topic: {}", e);
                                    eprintln!("Failed to broadcast message to stdout topic: {}", e);
                                }
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }


    async fn shutdown_workers(
        task_registry: TaskRegistry,
        shutdown_token: CancellationToken,
    ) {
        log::info!("Starting graceful shutdown...");

        // Cancel the shared shutdown token to notify all executors
        log::info!("Cancelling shutdown token");
        shutdown_token.cancel();

        // Ask tasks to shutdown and wait for them to finish
        log::info!("Waiting for running tasks to finish...");
        task_registry.graceful_shutdown_all(std::time::Duration::from_secs(5)).await;

        log::info!("All user tasks stopped");
    }
}

#[derive(Default)]
pub struct BackgroundTaskManager {
    handles: Vec<(String, JoinHandle<()>)>,
}

impl BackgroundTaskManager {
    pub fn new() -> Self { Self { handles: Vec::new() } }
    pub fn register(&mut self, name: &str, handle: JoinHandle<()>) {
        self.handles.push((name.to_string(), handle));
    }
    pub async fn shutdown_all(&mut self, timeout: std::time::Duration) {
        let mut handles = std::mem::take(&mut self.handles);
        for (idx, (name, mut h)) in handles.drain(..).enumerate() {
            log::info!("Waiting background task {} ({}) up to {:?}", idx, name, timeout);
            let finished_in_time = tokio::time::timeout(timeout, &mut h).await.is_ok();
            if !finished_in_time {
                log::warn!("Background task {} ({}) did not finish in {:?}, aborting", idx, name, timeout);
                h.abort();
                // await once to observe cancellation and free resources
                let _ = h.await;
            }
        }
    }

    pub async fn abort_all(&mut self) {
        let mut handles = std::mem::take(&mut self.handles);
        for (_name, mut h) in handles.drain(..) {
            h.abort();
            let _ = h.await;
        }
    }
}
