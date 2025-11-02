use anyhow::{Result, Error};
use std::sync::{Arc, Weak};
use tokio::sync::RwLock;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::process::Command as TokioCommand;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader, stdin};
use tokio::task;
use uuid::Uuid;
use tokio_util::sync::CancellationToken;
use async_trait::async_trait;
use std::process::Stdio;
use crate::buffer::{InputBufferManager, StreamOutcome};
use crate::logging::{UserLogEvent, UserLogKind, spawn_user_log_aggregator, LogEvent, spawn_log_aggregator, set_channel_logger, level_from_env};
use tokio::task::JoinHandle;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::unistd::Pid;
 

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


pub struct SystemControlMessage {
    pub command: Box<dyn SystemControlHandler>,
    pub task_id: TaskId,
    pub response_channel: SystemResponseSender,
    pub task_event_sender: ExecutorEventSender,
    pub return_message_sender: ExecutorEventSender,
}

impl std::fmt::Debug for SystemControlMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemControlMessage")
            .field("command", &"SystemControl")
            .field("task_id", &self.task_id)
            .field("response_channel", &"SystemResponseSender")
            .field("task_event_sender", &"ExecutorEventSender")
            .finish()
    }
}

impl SystemControlMessage {
    pub fn new(command: Box<dyn SystemControlHandler>, task_id: TaskId, response_channel: SystemResponseSender, task_event_sender: ExecutorEventSender, return_message_sender: ExecutorEventSender) -> Self {
        Self {
            command,
            task_id,
            response_channel,
            task_event_sender,
            return_message_sender,
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

    pub fn send_message(&self, key: String, data: String) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_message(key, data))
    }

    pub fn send_error(&self, error: String) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_error(error))
    }


    pub fn send_exit(&self, code: i32) -> Result<(), mpsc::error::SendError<ExecutorEvent>> {
        self.send(ExecutorEvent::new_exit(code))
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

// StdinProtocolトレイト：stdinに入力可能なデータを表す
pub trait StdinProtocol: Send + Sync {
    /// stdinに入力する形式の文字列のベクターを返す
    /// 各行がstdinに送信される順序で返される
    /// 
    /// デフォルト実装では以下のバリデーションを行います：
    /// - 1行目がStringであること（常に満たされる）
    /// - 2行目が数値であること
    /// - 3行目以降の行数が2行目の数値と一致すること
    fn to_input_lines(&self) -> Vec<String> {
        let lines = self.to_input_lines_raw();
        
        // バリデーション: 少なくとも2行あることを確認
        if lines.len() < 2 {
            panic!("StdinProtocol validation failed: must have at least 2 lines, got {}", lines.len());
        }
        
        // バリデーション: 2行目が数値であることを確認
        let line_count: usize = lines[1].parse()
            .unwrap_or_else(|_| {
                panic!("StdinProtocol validation failed: line 2 must be a number, got '{}'", lines[1]);
            });
        
        // バリデーション: 3行目以降の行数が2行目の数値と一致することを確認
        let data_line_count = lines.len() - 2;
        if data_line_count != line_count {
            panic!(
                "StdinProtocol validation failed: expected {} data lines (from line 2), but got {} (total lines: {})",
                line_count, data_line_count, lines.len()
            );
        }
        
        lines
    }
    
    /// 内部実装メソッド: バリデーションなしで行のベクターを返す
    /// 各実装ではこのメソッドを実装する
    fn to_input_lines_raw(&self) -> Vec<String>;
}

// TopicMessage: トピック経由のメッセージ
#[derive(Clone, Debug)]
pub struct TopicMessage {
    pub topic: String,
    pub data: String,
}

impl StdinProtocol for TopicMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let mut lines = vec![self.topic.clone()];
        let data_lines: Vec<&str> = self.data.lines().collect();
        lines.push(data_lines.len().to_string());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

// SystemResponseMessage: SystemResponseイベントからのメッセージ
#[derive(Clone, Debug)]
pub struct SystemResponseMessage {
    pub topic: String,
    pub status: String,
    pub data: String,
}

impl StdinProtocol for SystemResponseMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let mut lines = vec![self.topic.clone()];
        let data_lines: Vec<&str> = self.data.lines().collect();
        lines.push((data_lines.len() + 1).to_string()); // +1 for status line
        lines.push(self.status.clone());
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

// ReturnMessage: system.returnで返されるメッセージ
#[derive(Clone, Debug)]
pub struct ReturnMessage {
    pub data: String,
}

impl StdinProtocol for ReturnMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        let data_lines: Vec<&str> = self.data.lines().collect();
        let mut lines = vec!["system.return".to_string(), data_lines.len().to_string()];
        lines.extend(data_lines.iter().map(|s| s.to_string()));
        lines
    }
}

// InputDataMessage: 各種メッセージ型をまとめるenum
#[derive(Clone, Debug)]
pub enum InputDataMessage {
    Topic(TopicMessage),
    SystemResponse(SystemResponseMessage),
    Return(ReturnMessage),
}

impl StdinProtocol for InputDataMessage {
    fn to_input_lines_raw(&self) -> Vec<String> {
        match self {
            InputDataMessage::Topic(msg) => msg.to_input_lines_raw(),
            InputDataMessage::SystemResponse(msg) => msg.to_input_lines_raw(),
            InputDataMessage::Return(msg) => msg.to_input_lines_raw(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InputSender {
    sender: mpsc::UnboundedSender<InputDataMessage>,
}

impl InputSender {
    pub fn new(sender: mpsc::UnboundedSender<InputDataMessage>) -> Self {
        Self { sender }
    }

    pub fn send(&self, input: InputDataMessage) -> Result<(), mpsc::error::SendError<InputDataMessage>> {
        self.sender.send(input)
    }
}

pub struct InputReceiver {
    receiver: mpsc::UnboundedReceiver<InputDataMessage>,
}

impl InputReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<InputDataMessage>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<InputDataMessage> {
        self.receiver.recv().await
    }
}

pub struct InputChannel {
    pub sender: InputSender,
    pub receiver: InputReceiver,
}

impl InputChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<InputDataMessage>();
        Self {
            sender: InputSender::new(tx),
            receiver: InputReceiver::new(receiver),
        }
    }
}

// SystemControl コマンドの応答ステータス
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemResponseStatus {
    Success,
    Error,
}

impl std::fmt::Display for SystemResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SystemResponseStatus::Success => write!(f, "success"),
            SystemResponseStatus::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum SystemResponseEvent {
    SystemResponse {
        topic: String,
        status: String,
        data: String,
    },
    SystemError {
        topic: String,
        status: String,
        error: String,
    },
}

impl SystemResponseEvent {
    pub fn new_system_response(topic: String, status: String, data: String) -> Self {
        Self::SystemResponse { topic, status, data }
    }

    pub fn new_system_error(topic: String, status: String, error: String) -> Self {
        Self::SystemError { topic, status, error }
    }

    pub fn topic(&self) -> &str {
        match self {
            Self::SystemResponse { topic, .. } => topic,
            Self::SystemError { topic, .. } => topic,
        }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::SystemResponse { data, .. } => Some(data),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SystemResponseSender {
    sender: mpsc::UnboundedSender<SystemResponseEvent>,
}

impl SystemResponseSender {
    pub fn new(sender: mpsc::UnboundedSender<SystemResponseEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: SystemResponseEvent) -> Result<(), mpsc::error::SendError<SystemResponseEvent>> {
        self.sender.send(event)
    }
}

pub struct SystemResponseReceiver {
    receiver: mpsc::UnboundedReceiver<SystemResponseEvent>,
}

impl SystemResponseReceiver {
    pub fn new(receiver: mpsc::UnboundedReceiver<SystemResponseEvent>) -> Self {
        Self { receiver }
    }

    pub async fn recv(&mut self) -> Option<SystemResponseEvent> {
        self.receiver.recv().await
    }
}

pub struct SystemResponseChannel {
    pub sender: SystemResponseSender,
    pub receiver: SystemResponseReceiver,
}

impl SystemResponseChannel {
    pub fn new() -> Self {
        let (tx, receiver) = mpsc::unbounded_channel::<SystemResponseEvent>();
        Self {
            sender: SystemResponseSender::new(tx),
            receiver: SystemResponseReceiver::new(receiver),
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

#[derive(Clone, Debug)]
pub struct UserLogSender {
    sender: mpsc::UnboundedSender<UserLogEvent>,
}

impl UserLogSender {
    pub fn new(sender: mpsc::UnboundedSender<UserLogEvent>) -> Self {
        Self { sender }
    }

    pub fn send(&self, event: UserLogEvent) -> Result<(), mpsc::error::SendError<UserLogEvent>> {
        self.sender.send(event)
    }
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
pub struct SystemControlManager {
    commands: Arc<RwLock<Vec<SystemControlMessage>>>,
    shutdown_token: CancellationToken,
}

impl SystemControlManager {
    pub fn new(shutdown_token: CancellationToken) -> Self {
        Self {
            commands: Arc::new(RwLock::new(Vec::new())),
            shutdown_token,
        }
    }

    pub async fn add_command(&self, message: SystemControlMessage) -> Result<(), String> {
        let mut commands = self.commands.write().await;
        commands.push(message);
        Ok(())
    }

    pub async fn send_system_control_command(&self, command: Box<dyn SystemControlHandler>, task_id: TaskId, response_channel: SystemResponseSender, task_event_sender: ExecutorEventSender, return_message_sender: ExecutorEventSender) -> Result<(), String> {
        let message = SystemControlMessage::new(command, task_id, response_channel, task_event_sender, return_message_sender);
        self.add_command(message).await
    }

    pub async fn recv_command(&self) -> Option<SystemControlMessage> {
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

pub fn start_system_control_worker(
    system_control_manager: SystemControlManager,
    topic_manager: TopicManager,
    task_executor: TaskExecutor,
    config: SystemConfig,
    shutdown_token: CancellationToken,
    userlog_sender: UserLogSender,
) -> tokio::task::JoinHandle<()> {
        task::spawn(async move {
        let running_commands: Arc<RwLock<HashMap<TaskId, JoinHandle<()>>>> = Arc::new(RwLock::new(HashMap::new()));
        
        loop {
            tokio::select! {
                biased;
                
                _ = shutdown_token.cancelled() => {
                    log::info!("SystemControl worker received shutdown signal, aborting all running commands");
                    
                    let mut commands = running_commands.write().await;
                    for (task_id, handle) in commands.drain() {
                        log::info!("Aborting SystemControl execution for task {}", task_id);
                        handle.abort();
                    }
                    
                    break;
                }
                
                maybe = async { system_control_manager.recv_command().await } => {
                        match maybe {
                            Some(message) => {
                                let task_id: TaskId = message.task_id.clone();
                                
                                log::info!("Spawning SystemControl for task {} (parallel execution)", task_id);
                                
                                let context = SystemControlContext {
                                    topic_manager: topic_manager.clone(),
                                    task_executor: task_executor.clone(),
                                    config: config.clone(),
                                    shutdown_token: shutdown_token.clone(),
                                    userlog_sender: userlog_sender.clone(),
                                    system_control_manager: system_control_manager.clone(),
                                    task_id: task_id.clone(),
                                    response_channel: message.response_channel.clone(),
                                    task_event_sender: message.task_event_sender.clone(),
                                    return_message_sender: message.return_message_sender.clone(),
                                };
                                
                                let command_shutdown_token = shutdown_token.clone();
                                let running_commands_clone = running_commands.clone();
                                let task_id_clone = task_id.clone();
                                
                                let handle = tokio::spawn(async move {
                                    log::info!("Executing SystemControl for task {}", task_id_clone);
                                    
                                    // contextをクローンして、キャンセル分岐でも使用できるようにする
                                    let context_for_cancel = context.clone();
                                    
                                    // executeをspawnして別タスクで実行することで、
                                    // ブロッキングしてもキャンセルが機能するようにする
                                    let execute_handle = tokio::spawn(async move {
                                        message.command.execute(&context).await
                                    });
                                    
                                    // abort_handleを取得して、tokio::select!の両方の分岐で使用できるようにする
                                    let execute_abort_handle = execute_handle.abort_handle();
                                    
                                    tokio::select! {
                                        _ = command_shutdown_token.cancelled() => {
                                            log::info!("SystemControl for task {} cancelled due to shutdown", task_id_clone);
                                            // executeタスクを中断
                                            execute_abort_handle.abort();
                                            
                                            let status = SystemResponseStatus::Error;
                                            let cancel_error = SystemResponseEvent::new_system_error(
                                                "system.error".to_string(),
                                                status.to_string(),
                                                "cancelled".to_string(),
                                            );
                                            let _ = context_for_cancel.response_channel.send(cancel_error);
                                        }
                                        result = execute_handle => {
                                            match result {
                                                Ok(Ok(_)) => {
                                                    log::info!("Successfully executed SystemControl for task {}", task_id_clone);
                                                }
                                                Ok(Err(e)) => {
                                                    log::error!("Failed to execute SystemControl for task {}: {}", task_id_clone, e);
                                                    // エラー時にもレスポンスを送信（execute内で送信していない場合に備える）
                                                    let status = SystemResponseStatus::Error;
                                                    let error_event = SystemResponseEvent::new_system_error(
                                                        "system.error".to_string(),
                                                        status.to_string(),
                                                        e.clone(),
                                                    );
                                                    let _ = context_for_cancel.response_channel.send(error_event);
                                                }
                                                Err(e) => {
                                                    // executeタスクがパニックした場合、またはabortされた場合
                                                    if e.is_cancelled() {
                                                        log::info!("SystemControl execution was cancelled for task {}", task_id_clone);
                                                    } else {
                                                        log::error!("SystemControl execution task panicked for task {}: {:?}", task_id_clone, e);
                                                        let status = SystemResponseStatus::Error;
                                                        let error_event = SystemResponseEvent::new_system_error(
                                                            "system.error".to_string(),
                                                            status.to_string(),
                                                            format!("Task panicked: {:?}", e),
                                                        );
                                                        let _ = context_for_cancel.response_channel.send(error_event);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    
                                    let mut commands = running_commands_clone.write().await;
                                    commands.remove(&task_id_clone);
                                });
                                
                                let mut commands = running_commands.write().await;
                                commands.insert(task_id, handle);
                            }
                            None => {
                                log::info!("SystemControl receiver closed");
                                break;
                            }
                        }
                    }
                }
            }
            
            log::info!("SystemControl worker stopped");
    })
}


#[derive(Debug, Clone)]
pub enum ExecutorEvent {
    Message {
        topic: String,
        data: String,
    },
    TaskStdout {
        data: String,
    },
    TaskStderr {
        data: String,
    },
    SystemControl {
        key: String,
        data: String,
    },
    ReturnMessage {
        data: String,
    },
    Error {
        error: String,
    },
    Exit {
        exit_code: i32,
    },
}

impl ExecutorEvent {
    pub fn new_message(topic: String, data: String) -> Self {
        Self::Message { 
            topic, 
            data 
        }
    }

    pub fn new_error(error: String) -> Self {
        Self::Error { error }
    }

    pub fn new_exit(exit_code: i32) -> Self {
        Self::Exit { exit_code }
    }

    pub fn new_task_stdout(data: String) -> Self {
        Self::TaskStdout { data }
    }

    pub fn new_task_stderr(data: String) -> Self {
        Self::TaskStderr { data }
    }

    pub fn new_system_control(key: String, data: String) -> Self {
        Self::SystemControl { 
            key, 
            data 
        }
    }

    pub fn new_return_message(data: String) -> Self {
        Self::ReturnMessage { data }
    }

    pub fn data(&self) -> Option<&String> {
        match self {
            Self::Message { data, .. } => Some(data),
            Self::TaskStdout { data } => Some(data),
            Self::TaskStderr { data } => Some(data),
            Self::ReturnMessage { data } => Some(data),
            _ => None,
        }
    }

    pub fn topic(&self) -> Option<&String> {
        match self {
            Self::Message { topic, .. } => Some(topic),
            _ => None,
        }
    }
}

pub struct TaskSpawner {
    pub task_id: TaskId,
    pub topic_manager: TopicManager,
    pub system_control_manager: SystemControlManager,
    pub task_executor: TaskExecutor,
    pub task_name: String,
    pub userlog_sender: UserLogSender,
}

impl TaskSpawner {
    pub fn new(
        task_id: TaskId,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
        task_executor: TaskExecutor,
        task_name: String,
        userlog_sender: UserLogSender,
    ) -> Self {
        Self {
            task_id,
            topic_manager,
            system_control_manager,
            task_executor,
            task_name,
            userlog_sender,
        }
    }

    pub async fn spawn_backend(
        self,
        backend: Box<dyn TaskBackend>,
        shutdown_token: CancellationToken,
        subscribe_topics: Option<Vec<String>>,
        other_return_message_sender: Option<ExecutorEventSender>,
    ) -> SpawnBackendResult {
        let task_id: TaskId = self.task_id.clone();
        let task_name: String = self.task_name.clone();
        let topic_manager: TopicManager = self.topic_manager;
        let system_control_manager: SystemControlManager = self.system_control_manager;
        let task_executor: TaskExecutor = self.task_executor;
        let userlog_sender = self.userlog_sender.clone();

        let mut backend_handle = match backend.spawn(task_id.clone()).await {
            Ok(handle) => handle,
            Err(e) => {
                log::error!("Failed to spawn task backend for task {}: {}", task_id, e);
                // エラー時はダミーのチャネルを作成して返す
                let input_channel: InputChannel = InputChannel::new();
                let shutdown_channel = ShutdownChannel::new();
                return SpawnBackendResult {
                    task_handle: tokio::task::spawn(async {}),
                    input_sender: input_channel.sender,
                    shutdown_sender: shutdown_channel.sender,
                };
            }
        };

        // Executor側でsystem_response_senderを設定
        let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
        let mut system_response_receiver = system_response_channel.receiver;
        backend_handle.system_response_sender = system_response_channel.sender;

        // 外部に公開するためのinput_senderとshutdown_senderをクローン
        let input_sender_for_external = backend_handle.input_sender.clone();
        let shutdown_sender_for_external = backend_handle.shutdown_sender.clone();

        let task_handle = tokio::task::spawn(async move {
            let topic_data_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let mut topic_data_receiver = topic_data_channel.receiver;

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


            let return_message_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let mut return_message_receiver = return_message_channel.receiver;
            
            loop {
                tokio::select! {
                    biased;
                    
                    _ = shutdown_token.cancelled() => {
                        log::info!("Task {} received shutdown signal", task_id);
                        let _ = backend_handle.shutdown_sender.shutdown();
                        break;
                    },
                    
                    event = backend_handle.event_receiver.recv() => {
                        match event {
                            Some(event) => {
                                let event: ExecutorEvent = event;
                                
                                match &event {
                                    ExecutorEvent::Message { topic, data } => {
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
                                    ExecutorEvent::SystemControl { .. } => {
                                        if let Some(system_control_cmd) = system_control_command_to_handler(&event) {
                                            log::info!("SystemControl detected from task {}", task_id);
                                            if let Err(e) = system_control_manager.send_system_control_command(
                                                system_control_cmd,
                                                task_id.clone(),
                                                backend_handle.system_response_sender.clone(),
                                                topic_data_channel.sender.clone(),
                                                return_message_channel.sender.clone()
                                            ).await {
                                                log::warn!("Failed to send system control command to worker (task {}): {}", task_id, e);
                                            } else {
                                                log::info!("Sent system control command to worker for task {}", task_id);
                                            }
                                        } else {
                                            log::warn!("Failed to convert SystemControl event to handler for task {}", task_id);
                                        }
                                    },
                                    ExecutorEvent::ReturnMessage { data } => {
                                        log::info!("ReturnMessage received from task {}: '{}'", task_id, data);
                                        // 外部から注入されたother_return_message_senderに送信
                                        if let Some(ref sender) = other_return_message_sender {
                                            if let Err(e) = sender.send(ExecutorEvent::new_return_message(data.clone())) {
                                                log::warn!("Failed to send return message to other_return_message_sender for task {}: {}", task_id, e);
                                            }
                                        } else {
                                            log::warn!("ReturnMessage received but other_return_message_sender is not available for task {}", task_id);
                                        }
                                    },
                                    ExecutorEvent::TaskStdout { data } => {
                                        let flags = task_executor.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((view_stdout, _)) = flags {
                                            if view_stdout {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stdout, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorEvent::TaskStderr { data } => {
                                        let flags = task_executor.get_view_flags_by_task_id(&task_id).await;
                                        if let Some((_, view_stderr)) = flags {
                                            if view_stderr {
                                                let _ = userlog_sender.send(UserLogEvent { task_id: task_id.to_string(), task_name: task_name.clone(), kind: UserLogKind::Stderr, msg: data.clone() });
                                            }
                                        }
                                    },
                                    ExecutorEvent::Error { error } => {
                                        log::error!("Error event for task {}: '{}'", task_id, error);
                                    },
                                    ExecutorEvent::Exit { exit_code } => {
                                        log::info!("Exit event for task {} with exit code: {}", task_id, exit_code);
                                        
                                        let removed_topics: Vec<String> = topic_manager.remove_all_subscriptions_by_task(task_id.clone()).await;
                                        log::info!("Task {} exited, removed {} topic subscriptions", task_id, removed_topics.len());
                                        
                                        if let Some(_removed_task) = task_executor.unregister_task_by_task_id(&task_id).await {
                                            log::info!("Removed task with TaskId={} (Human name index updated)", task_id);
                                        } else {
                                            log::warn!("Task with TaskId={} not found in executor during cleanup", task_id);
                                        }
                                    }
                                }
                            },
                            None => {
                                log::info!("Task backend event receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },
                    
                    topic_data = topic_data_receiver.recv() => {
                        match topic_data {
                            Some(topic_data) => {
                                if let Some(data) = topic_data.data() {
                                    if let Some(topic_name) = topic_data.topic() {
                                        let topic_msg = TopicMessage {
                                            topic: topic_name.clone(),
                                            data: data.clone(),
                                        };
                                        if let Err(e) = backend_handle.input_sender.send(InputDataMessage::Topic(topic_msg)) {
                                            log::warn!("Failed to send topic message to task backend for task {}: {}", task_id, e);
                                        }
                                    } else {
                                        log::warn!("Topic data received without topic name for task {}, skipping", task_id);
                                        continue;
                                    }
                                }
                            },
                            None => {
                                log::info!("Topic data receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },
                    
                    return_message = return_message_receiver.recv() => {
                        match return_message {
                            Some(message) => {
                                log::info!("Return message received for task {}: {:?}", task_id, message);
                                if let Some(data) = message.data() {
                                    let return_msg = ReturnMessage {
                                        data: data.clone(),
                                    };
                                    if let Err(e) = backend_handle.input_sender.send(InputDataMessage::Return(return_msg)) {
                                        log::warn!("Failed to send return message to task backend for task {}: {}", task_id, e);
                                    }
                                }
                            },
                            None => {
                                log::info!("Return message receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    },
                    
                    system_response = system_response_receiver.recv() => {
                        match system_response {
                            Some(SystemResponseEvent::SystemResponse { topic, status, data }) => {
                                log::info!("SystemResponse event for task {}: topic='{}', status='{}', data='{}'", task_id, topic, status, data);
                                let system_response_msg = SystemResponseMessage {
                                    topic,
                                    status,
                                    data,
                                };
                                if let Err(e) = backend_handle.input_sender.send(InputDataMessage::SystemResponse(system_response_msg)) {
                                    log::warn!("Failed to send system response message to task backend for task {}: {}", task_id, e);
                                }
                            },
                            Some(SystemResponseEvent::SystemError { topic, status, error }) => {
                                log::error!("SystemError event for task {}: topic='{}', status='{}', error='{}'", task_id, topic, status, error);
                                let system_response_msg = SystemResponseMessage {
                                    topic,
                                    status,
                                    data: error,
                                };
                                if let Err(e) = backend_handle.input_sender.send(InputDataMessage::SystemResponse(system_response_msg)) {
                                    log::warn!("Failed to send system error message to task backend for task {}: {}", task_id, e);
                                }
                            },
                            None => {
                                log::info!("SystemResponse receiver closed for task {}", task_id);
                                break;
                            }
                        }
                    }
                }
            }
            
            log::info!("Task {} completed", task_id);
        });

        SpawnBackendResult {
            task_handle,
            input_sender: input_sender_for_external,
            shutdown_sender: shutdown_sender_for_external,
        }
    }
}

#[derive(Clone)]
pub struct SystemControlContext {
    pub topic_manager: TopicManager,
    pub task_executor: TaskExecutor,
    pub config: SystemConfig,
    pub shutdown_token: CancellationToken,
    pub userlog_sender: UserLogSender,
    pub system_control_manager: SystemControlManager,
    pub task_id: TaskId,
    pub response_channel: SystemResponseSender,
    pub task_event_sender: ExecutorEventSender,
    pub return_message_sender: ExecutorEventSender,
}

#[async_trait]
pub trait SystemControlHandler: Send + Sync {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeTopicSystemControl {
    pub topic: String,
}

#[async_trait]
impl SystemControlHandler for SubscribeTopicSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing SubscribeTopic command for task {}: '{}'", context.task_id, self.topic);
        
        context.topic_manager.add_subscriber(
            self.topic.clone(),
            context.task_id.clone(), 
            context.task_event_sender.clone()
        ).await;
        
        log::info!("Successfully subscribed to topic '{}'", self.topic);
        
        let status = SystemResponseStatus::Success;
        let response_topic = "system.subscribe-topic".to_string();
        let success_event = SystemResponseEvent::new_system_response(
            response_topic,
            status.to_string(),
            self.topic.clone(),
        );
        let _ = context.response_channel.send(success_event);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeTopicSystemControl {
    pub topic: String,
}

#[async_trait]
impl SystemControlHandler for UnsubscribeTopicSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing UnsubscribeTopic command for task {}: '{}'", context.task_id, self.topic);
        
        let removed = context.topic_manager.remove_subscriber_by_task(
            self.topic.clone(),
            context.task_id.clone()
        ).await;
        
        if removed {
            log::info!("Successfully unsubscribed from topic '{}'", self.topic);
            
            let status = SystemResponseStatus::Success;
            let response_topic = "system.unsubscribe-topic".to_string();
            let success_event = SystemResponseEvent::new_system_response(
                response_topic,
                status.to_string(),
                self.topic.clone(),
            );
            let _ = context.response_channel.send(success_event);
        } else {
            log::warn!("Failed to unsubscribe from topic '{}'", self.topic);
            
            let status = SystemResponseStatus::Error;
            let response_topic = "system.unsubscribe-topic".to_string();
            let error_event = SystemResponseEvent::new_system_error(
                response_topic,
                status.to_string(),
                self.topic.clone(),
            );
            let _ = context.response_channel.send(error_event);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartTaskSystemControl {
    pub task_name: String,
}

#[async_trait]
impl SystemControlHandler for StartTaskSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing StartTask command for task {}: '{}'", context.task_id, self.task_name);
        
            match context.task_executor.start_single_task(
                &self.task_name,
                &context.config,
                context.topic_manager.clone(),
                context.system_control_manager.clone(),
                context.shutdown_token.clone(),
                context.userlog_sender.clone(),
                None, // system.start-taskではreturn_message_senderは不要
                None, // system.start-taskではinitial_inputは不要
            ).await {
            Ok(_) => {
                log::info!("Successfully started task '{}'", self.task_name);
                
                let status = SystemResponseStatus::Success;
                let response_topic = "system.start-task".to_string();
                let success_event = SystemResponseEvent::new_system_response(
                    response_topic,
                    status.to_string(),
                    self.task_name.clone(),
                );
                let _ = context.response_channel.send(success_event);
                Ok(())
            },
            Err(e) => {
                log::error!("Failed to start task '{}': {}", self.task_name, e);
                
                let status = SystemResponseStatus::Error;
                let response_topic = "system.start-task".to_string();
                let error_event = SystemResponseEvent::new_system_error(
                    response_topic,
                    status.to_string(),
                    self.task_name.clone(),
                );
                let _ = context.response_channel.send(error_event);
                Err(format!("Failed to start task '{}': {}", self.task_name, e))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StopTaskSystemControl {
    pub task_name: String,
}

#[async_trait]
impl SystemControlHandler for StopTaskSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing StopTask command for task {}: '{}'", context.task_id, self.task_name);
        
        match context.task_executor.stop_task_by_name(&self.task_name).await {
            Ok(_) => {
                log::info!("Successfully stopped task '{}'", self.task_name);
                
                let status = SystemResponseStatus::Success;
                let response_topic = "system.stop-task".to_string();
                let success_event = SystemResponseEvent::new_system_response(
                    response_topic,
                    status.to_string(),
                    self.task_name.clone(),
                );
                let _ = context.response_channel.send(success_event);
                Ok(())
            },
            Err(e) => {
                log::error!("Failed to stop task '{}': {}", self.task_name, e);
                
                let status = SystemResponseStatus::Error;
                let response_topic = "system.stop-task".to_string();
                let error_event = SystemResponseEvent::new_system_error(
                    response_topic,
                    status.to_string(),
                    self.task_name.clone(),
                );
                let _ = context.response_channel.send(error_event);
                Err(format!("Failed to stop task '{}': {}", self.task_name, e))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AddTaskFromTomlSystemControl {
    pub toml_data: String,
}

#[async_trait]
impl SystemControlHandler for AddTaskFromTomlSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing AddTaskFromToml command for task {} with TOML data", context.task_id);
        
        match context.task_executor.add_task_from_toml(
            &self.toml_data,
            context.topic_manager.clone(),
            context.system_control_manager.clone(),
            context.shutdown_token.clone(),
            context.userlog_sender.clone(),
        ).await {
            Ok(_) => {
                log::info!("Successfully added task to executor from TOML");
                
                let status = SystemResponseStatus::Success;
                let success_event = SystemResponseEvent::new_system_response(
                    "system.add-task-from-toml".to_string(),
                    status.to_string(),
                    String::new(),
                );
                let _ = context.response_channel.send(success_event);
                Ok(())
            },
            Err(e) => {
                log::error!("Failed to add task from TOML: {}", e);
                
                let status = SystemResponseStatus::Error;
                let error_event = SystemResponseEvent::new_system_error(
                    "system.add-task-from-toml".to_string(),
                    status.to_string(),
                    e.to_string(),
                );
                let _ = context.response_channel.send(error_event);
                Err(format!("Failed to add task from TOML: {}", e))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallFunctionSystemControl {
    pub task_name: String,
    pub initial_input: Option<String>,
}

#[async_trait]
impl SystemControlHandler for CallFunctionSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing CallFunction command for task {}: '{}'", context.task_id, self.task_name);
        
        match context.task_executor.start_single_task(
            &self.task_name,
            &context.config,
            context.topic_manager.clone(),
            context.system_control_manager.clone(),
            context.shutdown_token.clone(),
            context.userlog_sender.clone(),
            Some(context.return_message_sender.clone()), // function呼び出し時はreturn_message_senderを渡す
            self.initial_input.clone(), // 初期インプットを渡す
        ).await {
            Ok(_) => {
                log::info!("Successfully called function '{}'", self.task_name);
                
                let status = SystemResponseStatus::Success;
                let response_topic = format!("system.function.{}", self.task_name);
                let success_event = SystemResponseEvent::new_system_response(
                    response_topic,
                    status.to_string(),
                    self.task_name.clone(),
                );
                let _ = context.response_channel.send(success_event);
                Ok(())
            },
            Err(e) => {
                log::error!("Failed to call function '{}': {}", self.task_name, e);
                let status = SystemResponseStatus::Error;
                let response_topic = format!("system.function.{}", self.task_name);
                let error_event = SystemResponseEvent::new_system_error(
                    response_topic,
                    status.to_string(),
                    e.to_string(),
                );
                let _ = context.response_channel.send(error_event);
                Err(e.to_string())
            }
        }
    }
}

pub struct StatusSystemControl;

#[async_trait]
impl SystemControlHandler for StatusSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::info!("Processing Status command for task {}", context.task_id);
        
        let tasks_info = context.task_executor.get_running_tasks_info().await;
        let topics_info = context.topic_manager.get_topics_info().await;
        
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
        
        let status = SystemResponseStatus::Success;
        let status_event = SystemResponseEvent::new_system_response(
            "system.status".to_string(),
            status.to_string(),
            json_response,
        );
        
        if let Err(e) = context.response_channel.send(status_event) {
            log::warn!("Failed to send status response to task {}: {}", context.task_id, e);
            Err(format!("Failed to send status response: {}", e))
        } else {
            log::info!("Sent status response to task {}", context.task_id);
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownSystemControl {
    pub command: String,
    pub data: String,
}

#[async_trait]
impl SystemControlHandler for UnknownSystemControl {
    async fn execute(&self, context: &SystemControlContext) -> Result<(), String> {
        log::warn!("Unknown system control command '{}' with data '{}' from task {}", self.command, self.data, context.task_id);
        Ok(())
    }
}


pub fn system_control_command_to_handler(event: &ExecutorEvent) -> Option<Box<dyn SystemControlHandler>> {
    let ExecutorEvent::SystemControl { key, data } = event else {
        return None;
    };
    
    let key_lower = key.to_lowercase();
    let data_trimmed = data.trim();
    
    if data_trimmed.is_empty() && key_lower.as_str() != "system.status" {
        return None;
    }
    
    let cmd: Box<dyn SystemControlHandler> = match key_lower.as_str() {
        "system.subscribe-topic" => {
            Box::new(SubscribeTopicSystemControl { 
                topic: data_trimmed.to_string() 
            })
        },
        "system.unsubscribe-topic" => {
            Box::new(UnsubscribeTopicSystemControl { 
                topic: data_trimmed.to_string() 
            })
        },
        "system.start-task" => {
            Box::new(StartTaskSystemControl { 
                task_name: data_trimmed.to_string() 
            })
        },
        "system.stop-task" => {
            Box::new(StopTaskSystemControl { 
                task_name: data_trimmed.to_string() 
            })
        },
        "system.add-task-from-toml" => {
            if data_trimmed.is_empty() {
                return None;
            }
            Box::new(AddTaskFromTomlSystemControl { 
                toml_data: data_trimmed.to_string() 
            })
        },
        "system.status" => {
            Box::new(StatusSystemControl)
        },
        _ if key_lower.starts_with("system.function.") => {
            // system.function.{task_name}の形式をパース
            let function_name = key_lower.strip_prefix("system.function.").unwrap_or("");
            if function_name.is_empty() {
                return None;
            }
            // dataが空でない場合、それを初期インプットとして使用
            let initial_input = if data_trimmed.is_empty() {
                None
            } else {
                Some(data_trimmed.to_string())
            };
            Box::new(CallFunctionSystemControl { 
                task_name: function_name.to_string(),
                initial_input,
            })
        },
        _ if key_lower.starts_with("system.") => {
            Box::new(UnknownSystemControl { 
                command: key_lower.clone(), 
                data: data_trimmed.to_string() 
            })
        },
        _ => return None,
    };
    
    Some(cmd)
}

pub struct TaskBackendHandle {
    pub event_receiver: ExecutorEventReceiver,
    pub event_sender: ExecutorEventSender,
    pub system_response_sender: SystemResponseSender,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}

/// spawn_backendの戻り値。JoinHandleと外部からアクセス可能なハンドルを含む
pub struct SpawnBackendResult {
    pub task_handle: tokio::task::JoinHandle<()>,
    pub input_sender: InputSender,
    pub shutdown_sender: ShutdownSender,
}

#[async_trait]
pub trait TaskBackend: Send + Sync {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error>;
}


#[derive(Clone)]
pub struct InteractiveBackend {
    system_input_topic: String,
}

impl InteractiveBackend {
    pub fn new(system_input_topic: String) -> Self {
        Self {
            system_input_topic,
        }
    }
}

#[async_trait]
impl TaskBackend for InteractiveBackend {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error> {
        let system_input_topic = self.system_input_topic.clone();
        
        let event_channel: ExecutorEventChannel = ExecutorEventChannel::new();
        let input_channel: InputChannel = InputChannel::new();
        let shutdown_channel = ShutdownChannel::new();
        let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
        
        let event_tx_clone: ExecutorEventSender = event_channel.sender.clone();
        let cancel_token: CancellationToken = CancellationToken::new();
        let mut shutdown_receiver = shutdown_channel.receiver;
        
        task::spawn(async move {
            let stdin = stdin();
            let reader = TokioBufReader::new(stdin);
            let mut lines = reader.lines();
            
            loop {
                tokio::select! {
                    _ = cancel_token.cancelled() => {
                        log::info!("Interactive mode received shutdown signal for task {}", task_id);
                        break;
                    }
                    _ = shutdown_receiver.recv() => {
                        log::info!("Interactive mode received shutdown signal via channel for task {}", task_id);
                        cancel_token.cancel();
                        break;
                    }
                    line_result = lines.next_line() => {
                        match line_result {
                            Ok(Some(line)) => {
                                let trimmed = line.trim();
                                if trimmed.is_empty() {
                                    continue;
                                }
                                
                                log::info!("Sending message topic:'{}' data:'{}'", system_input_topic, trimmed);
                                
                                let event = ExecutorEvent::new_message(
                                    system_input_topic.clone(),
                                    trimmed.to_string(),
                                );
                                
                                if let Err(e) = event_tx_clone.send(event) {
                                    log::error!("Failed to send event from interactive backend: {}", e);
                                    break;
                                }
                            }
                            Ok(None) => {
                                log::info!("Interactive mode stdin closed for task {}", task_id);
                                break;
                            }
                            Err(e) => {
                                log::error!("Error reading from stdin for task {}: {}", task_id, e);
                                let _ = event_tx_clone.send_error(format!("Error reading from stdin: {}", e));
                                break;
                            }
                        }
                    }
                }
            }
            
            log::info!("Interactive mode task {} completed", task_id);
        });
        
        Ok(TaskBackendHandle {
            event_receiver: event_channel.receiver,
            event_sender: event_channel.sender,
            system_response_sender: system_response_channel.sender,
            input_sender: input_channel.sender,
            shutdown_sender: shutdown_channel.sender,
        })
    }
}

#[derive(Clone)]
pub struct CommandBackend {
    command: String,
    args: Vec<String>,
    working_directory: Option<String>,
    environment_vars: Option<HashMap<String, String>>,
    stdout_topic: String,
    stderr_topic: String,
    view_stdout: bool,
    view_stderr: bool,
}

impl CommandBackend {
    pub fn new(
        command: String,
        args: Vec<String>,
        working_directory: Option<String>,
        environment_vars: Option<HashMap<String, String>>,
        stdout_topic: String,
        stderr_topic: String,
        view_stdout: bool,
        view_stderr: bool,
    ) -> Self {
        Self {
            command,
            args,
            working_directory,
            environment_vars,
            stdout_topic,
            stderr_topic,
            view_stdout,
            view_stderr,
        }
    }

    pub fn parse_system_control_command_from_outcome(topic: &str, data: &str) -> Option<ExecutorEvent> {
        let topic_lower = topic.to_lowercase();
        let data_trimmed = data.trim();
        
        if data_trimmed.is_empty() && topic_lower.as_str() != "system.status" {
            return None;
        }
        
        let is_system_command = topic_lower.starts_with("system.") || (topic_lower.is_empty() && data_trimmed.starts_with("system."));
        
        if is_system_command {
            let actual_topic = if topic_lower.is_empty() {
                let parts: Vec<&str> = data_trimmed.splitn(2, ' ').collect();
                if parts.len() == 2 {
                    parts[0].to_string()
                } else {
                    data_trimmed.to_string()
                }
            } else {
                topic_lower.clone()
            };
            
            let actual_data = if topic_lower.is_empty() && actual_topic != data_trimmed {
                let parts: Vec<&str> = data_trimmed.splitn(2, ' ').collect();
                if parts.len() == 2 {
                    parts[1].to_string()
                } else {
                    String::new()
                }
            } else {
                data_trimmed.to_string()
            };
            
            return Some(ExecutorEvent::new_system_control(actual_topic, actual_data));
        }
        
        None
    }

    pub fn parse_return_message_from_outcome(topic: &str, data: &str) -> Option<ExecutorEvent> {
        let topic_lower = topic.to_lowercase();
        let data_trimmed = data.trim();
        
        // topicが"system.return"の場合、ReturnMessageとして処理
        if topic_lower == "system.return" {
            if !data_trimmed.is_empty() {
                return Some(ExecutorEvent::new_return_message(data_trimmed.to_string()));
            }
        }
        
        None
    }
}

#[async_trait]
impl TaskBackend for CommandBackend {
    async fn spawn(&self, task_id: TaskId) -> Result<TaskBackendHandle, Error> {
        let command = self.command.clone();
        let args = self.args.clone();
        let working_directory = self.working_directory.clone();
        let environment_vars = self.environment_vars.clone();
        let stdout_topic = self.stdout_topic.clone();
        let stderr_topic = self.stderr_topic.clone();
        let view_stdout = self.view_stdout;
        let view_stderr = self.view_stderr;

            let event_channel: ExecutorEventChannel = ExecutorEventChannel::new();
            let input_channel: InputChannel = InputChannel::new();
            let mut shutdown_channel = ShutdownChannel::new();
            // system_response_channelはExecutor側で作成されるため、ここでは作成しない
            // Executor側から渡されたsenderを使用する（TaskBackendHandleに含まれる）
            // Backend側ではsystem_response_channelを作成するが、receiverは使用しない
            let system_response_channel: SystemResponseChannel = SystemResponseChannel::new();
            
            let event_tx_clone: ExecutorEventSender = event_channel.sender.clone();
            let mut input_receiver: InputReceiver = input_channel.receiver;

        task::spawn(async move {
            let mut command_builder = TokioCommand::new(&command);
            
            for arg in &args {
                command_builder.arg(arg);
            }
            
            if let Some(working_dir) = &working_directory {
                command_builder.current_dir(working_dir);
            }

            if let Some(env_vars) = &environment_vars {
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
                        format!("Failed to start process '{}': {}", command, e),
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
                stdout_topic.clone(),
                event_tx_clone.clone(),
                cancel_token.clone(),
                task_id.clone(),
                if view_stdout { Some(ExecutorEvent::new_task_stdout as fn(String) -> ExecutorEvent) } else { None },
            );

            let stderr_handle = spawn_stream_reader(
                TokioBufReader::new(stderr),
                stderr_topic.clone(),
                event_tx_clone.clone(),
                cancel_token.clone(),
                task_id.clone(),
                if view_stderr { Some(ExecutorEvent::new_task_stderr as fn(String) -> ExecutorEvent) } else { None },
            );

            
            let cancel_input: CancellationToken = cancel_token.clone();
            let event_tx_input: ExecutorEventSender = event_tx_clone.clone();
            let input_handle = task::spawn(async move {
                loop {
                    tokio::select! {
                        _ = cancel_input.cancelled() => { break; }
                        input_data = input_receiver.recv() => {
                            match input_data {
                                Some(input_data_msg) => {
                                    let lines = input_data_msg.to_input_lines();
                                    for line in lines {
                                        let bytes: Vec<u8> = if line.ends_with('\n') { 
                                            line.into_bytes() 
                                        } else { 
                                            format!("{}\n", line).into_bytes() 
                                        };
                                        if let Err(e) = stdin_writer.write_all(&bytes).await {
                                            let _ = event_tx_input.send_error(format!("Failed to write to stdin: {}", e));
                                            break;
                                        }
                                    }
                                    if let Err(e) = stdin_writer.flush().await {
                                        let _ = event_tx_input.send_error(format!("Failed to flush stdin: {}", e));
                                        break;
                                    }
                                },
                                None => break,
                            }
                        }
                    }
                }
            });

            // SystemResponseの処理はExecutor側で行うため、Backend側では不要
            // system_response_channel.receiverは使用しない（Executor側で処理される）

            let event_tx_status: ExecutorEventSender = event_tx_clone.clone();
            let status_cancel: CancellationToken = cancel_token.clone();
            let task_id_status: TaskId = task_id.clone();
            let status_handle = task::spawn(async move {
                let notify = |res: Result<std::process::ExitStatus, anyhow::Error>| {
                    match res {
                        Ok(exit_status) => {
                            let code: i32 = exit_status.code().unwrap_or(-1);
                            let _ = event_tx_status.send_exit(code);
                        }
                        Err(e) => {
                            log::error!("Error waiting for process: {}", e);
                            let _ = event_tx_status
                                .send_error(format!("Error waiting for process: {}", e));
                        }
                    }
                };
                tokio::select! {
                    _ = shutdown_channel.receiver.recv() => {
                        log::info!("Shutdown signal received for task {}, attempting graceful termination", task_id_status);
                        
                        #[cfg(unix)]
                        let pid_opt = child.id().map(|id| Pid::from_raw(id as i32));
                        #[cfg(not(unix))]
                        let pid_opt: Option<()> = None;
                        
                        #[cfg(unix)]
                        let graceful_shutdown_attempted = if let Some(pid) = pid_opt {
                            match kill(pid, Signal::SIGTERM) {
                                Ok(_) => {
                                    log::info!("Sent SIGTERM to child process {} (task {}), waiting for graceful shutdown", pid, task_id_status);
                                    true
                                }
                                Err(e) => {
                                    log::warn!("Failed to send SIGTERM to child process {} (task {}): {}, will use SIGKILL", pid, task_id_status, e);
                                    false
                                }
                            }
                        } else {
                            false
                        };
                        #[cfg(not(unix))]
                        let graceful_shutdown_attempted = false;
                        
                        if graceful_shutdown_attempted {
                            let graceful_timeout = tokio::time::Duration::from_secs(3);
                            
                            let mut wait_handle = tokio::spawn(async move {
                                child.wait().await.map_err(anyhow::Error::from)
                            });
                            
                            tokio::select! {
                                result = &mut wait_handle => {
                                    match result {
                                        Ok(Ok(exit_status)) => {
                                            #[cfg(unix)]
                                            if let Some(pid) = pid_opt {
                                                log::info!("Child process {} (task {}) exited gracefully after SIGTERM", pid, task_id_status);
                                            }
                                            notify(Ok(exit_status));
                                            status_cancel.cancel();
                                            return;
                                        }
                                        Ok(Err(e)) => {
                                            notify(Err(e));
                                            status_cancel.cancel();
                                            return;
                                        }
                                        Err(e) => {
                                            log::warn!("Error waiting for child process (task {}): {}", task_id_status, e);
                                            notify(Err(anyhow::Error::from(e)));
                                            status_cancel.cancel();
                                            return;
                                        }
                                    }
                                }
                                _ = tokio::time::sleep(graceful_timeout) => {
                                    #[cfg(unix)]
                                    if let Some(pid) = pid_opt {
                                        log::warn!("Child process {} (task {}) did not exit within timeout, sending SIGKILL", pid, task_id_status);
                                        let _ = kill(pid, Signal::SIGKILL);
                                    }
                                    match wait_handle.await {
                                        Ok(Ok(exit_status)) => {
                                            notify(Ok(exit_status));
                                        }
                                        Ok(Err(e)) => {
                                            notify(Err(e));
                                        }
                                        Err(e) => {
                                            notify(Err(anyhow::anyhow!("Child process terminated by SIGKILL: {}", e)));
                                        }
                                    }
                                    status_cancel.cancel();
                                    return;
                                }
                            }
                        } else {
                            log::info!("Forcing termination of child process for task {}", task_id_status);
                            let _ = child.kill().await;
                            notify(child.wait().await.map_err(anyhow::Error::from));
                            status_cancel.cancel();
                        }
                    }
                    status = child.wait() => {
                        notify(status.map_err(anyhow::Error::from));
                    }
                }
            });

            let _ = status_handle.await;
            let _ = stdout_handle.await;
            let _ = stderr_handle.await;
            cancel_token.cancel();
            let _ = input_handle.await;
        });
        
        Ok(TaskBackendHandle {
            event_receiver: event_channel.receiver,
            event_sender: event_channel.sender,
            system_response_sender: system_response_channel.sender,
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
    emit_func: Option<fn(String) -> ExecutorEvent>,
) -> task::JoinHandle<()> 
where
    R: tokio::io::AsyncBufRead + Unpin + Send + 'static
{
    task::spawn(async move {
        let mut buffer_manager = InputBufferManager::new();
        let mut line = String::new();
        let task_id_str = task_id.to_string();
        
        let process_stream_outcome = |outcome: Result<StreamOutcome, String>, line_content: &str| {
            match outcome {
                Ok(StreamOutcome::Emit { topic, data }) => {
                    // system.returnのチェック（ReturnMessage）
                    if let Some(return_message_event) = CommandBackend::parse_return_message_from_outcome(&topic, &data) {
                        let _ = event_tx.send(return_message_event);
                    } else if let Some(system_control_cmd_event) = CommandBackend::parse_system_control_command_from_outcome(&topic, &data) {
                        // SystemControlコマンドのチェック
                        let _ = event_tx.send(system_control_cmd_event);
                    } else {
                        // 通常のメッセージとして送信
                        let _ = event_tx.send_message(topic, data);
                    }
                }
                Ok(StreamOutcome::Plain(output)) => {
                    // Plain形式は通常のメッセージとして送信
                    let _ = event_tx.send_message(topic_name.clone(), output.clone());
                    if let Some(emit) = emit_func {
                        let _ = event_tx.send(emit(output));
                    }
                }
                Ok(StreamOutcome::None) => {}
                Err(e) => {
                    let _ = event_tx.send_error(e.clone());
                    let output = crate::buffer::strip_crlf(line_content).to_string();
                    let _ = event_tx.send_message(topic_name.clone(), output.clone());
                    if let Some(emit) = emit_func {
                        let _ = event_tx.send(emit(output));
                    }
                }
            }
        };

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    let unfinished = buffer_manager.flush_all_unfinished();
                    for (_, topic, data) in unfinished {
                        process_stream_outcome(Ok(StreamOutcome::Emit { topic, data }), "");
                    }
                    break;
                }
                result = reader.read_line(&mut line) => {
                    match result {
                        Ok(0) => {
                            let unfinished = buffer_manager.flush_all_unfinished();
                            for (_, topic, data) in unfinished {
                                process_stream_outcome(Ok(StreamOutcome::Emit { topic, data }), "");
                            }
                            break;
                        }
                        Ok(_) => {
                            process_stream_outcome(buffer_manager.consume_stream_line(&task_id_str, &line), &line);
                            line.clear();
                        },
                        Err(e) => {
                            log::error!("Error reading from {}: {}", topic_name, e);
                            let _ = event_tx.send_error(format!("Error reading from {}: {}", topic_name, e));
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
    pub subscribe_topics: Option<Vec<String>>,
    pub stdout_topic: Option<String>,
    pub stderr_topic: Option<String>,
    #[serde(default)]
    pub view_stdout: bool,
    #[serde(default)]
    pub view_stderr: bool,
}

impl TaskConfig {
    /// stdout_topicの値を取得（Noneの場合はタスク名ベースのデフォルト値を返す）
    pub fn get_stdout_topic(&self) -> String {
        self.stdout_topic.clone()
            .unwrap_or_else(|| format!("{}.stdout", self.name))
    }

    /// stderr_topicの値を取得（Noneの場合はタスク名ベースのデフォルト値を返す）
    pub fn get_stderr_topic(&self) -> String {
        self.stderr_topic.clone()
            .unwrap_or_else(|| format!("{}.stderr", self.name))
    }
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SystemConfig {
    #[serde(skip)]
    pub config_file: Option<String>,
    pub tasks: Vec<TaskConfig>,
    #[serde(default)]
    pub functions: Vec<TaskConfig>,
    #[serde(default)]
    pub include_paths: Vec<String>,
}

impl SystemConfig {
    pub fn from_toml(toml_content: &str) -> Result<Self> {
        let mut config: SystemConfig = toml::from_str(toml_content)?;
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
    
    pub fn get_all_tasks(&self) -> Vec<&TaskConfig> {
        // tasksとfunctionsを統一的に扱う
        self.tasks.iter().chain(self.functions.iter()).collect()
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
        // tasksとfunctionsの両方を処理
        for task in self.tasks.iter_mut().chain(self.functions.iter_mut()) {
            if task.working_directory.as_ref().map(|s| s.is_empty()).unwrap_or(true) {
                task.working_directory = Some("./".to_string());
            }
            if task.view_stdout != true {
                task.view_stdout = false;
            }
            if task.view_stderr != true {
                task.view_stderr = false;
            }
            // stdout_topicとstderr_topicは、使用時にタスク名ベースのデフォルト値を設定する
            // （TaskConfig::get_stdout_topic()とTaskConfig::get_stderr_topic()を使用）
        }
    }
}

#[derive(Debug)]
pub struct RunningTask {
    pub task_id: TaskId,
    pub shutdown_sender: ShutdownSender,
    pub input_sender: InputSender,
    pub task_handle: tokio::task::JoinHandle<()>,
    pub view_stdout: bool,
    pub view_stderr: bool,
}

#[derive(Clone)]
pub struct TaskExecutor {
    running_tasks: Arc<RwLock<HashMap<TaskId, RunningTask>>>,
    name_to_id: Arc<RwLock<HashMap<String, TaskId>>>,
}

impl TaskExecutor {
    pub fn new() -> Self {
        Self {
            running_tasks: Arc::new(RwLock::new(HashMap::new())),
            name_to_id: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        let mut running_tasks = self.running_tasks.write().await;
        let mut name_to_id = self.name_to_id.write().await;
        let task_id = task.task_id.clone();
        
        running_tasks.insert(task_id.clone(), task);
        name_to_id.insert(task_name, task_id);
        Ok(())
    }

    pub async fn try_register_task(&self, task_name: String, task: RunningTask) -> Result<(), String> {
        // 重複チェックを行い、重複していない場合のみ登録
        let name_to_id = self.name_to_id.read().await;
        if name_to_id.contains_key(&task_name) {
            log::warn!("Task '{}' is already running - cancelling new task start", task_name);
            return Err(format!("Task '{}' is already running", task_name));
        }
        drop(name_to_id); // ロックを解放
        
        // 重複していないので登録
        self.register_task(task_name, task).await
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
        let tasks: Vec<(TaskId, RunningTask)> = {
            let mut running_tasks = self.running_tasks.write().await;
            let drained: Vec<(TaskId, RunningTask)> = running_tasks.drain().collect();
            drained
        };
        {
            let mut name_to_id = self.name_to_id.write().await;
            name_to_id.clear();
        }

        for (_tid, task) in &tasks {
            let _ = task.shutdown_sender.shutdown();
        }

        for (_tid, task) in tasks {
            let _ = tokio::time::timeout(timeout, task.task_handle).await;
        }
    }

    pub async fn start_task_from_config(
        &self,
        task_config: &TaskConfig,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
        return_message_sender: Option<ExecutorEventSender>,
        is_function: bool,
        initial_input: Option<String>,
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
        
        let backend: Box<dyn TaskBackend> = Box::new(CommandBackend::new(
            task_config.command.clone(),
            task_config.args.clone(),
            task_config.working_directory.clone(),
            task_config.environment_vars.clone(),
            task_config.get_stdout_topic(),
            task_config.get_stderr_topic(),
            task_config.view_stdout,
            task_config.view_stderr,
        ));
        let subscribe_topics = task_config.subscribe_topics.clone();
        
        let task_spawner = TaskSpawner::new(
            task_id_new.clone(),
            topic_manager,
            system_control_manager,
            self.clone(),
            task_config.name.clone(),
            userlog_sender,
        );
        
        let spawn_result = task_spawner.spawn_backend(
            backend,
            shutdown_token,
            subscribe_topics,
            return_message_sender, // other_return_message_sender
        ).await;
        
        let running_task = RunningTask {
            task_id: task_id_new.clone(),
            shutdown_sender: spawn_result.shutdown_sender.clone(),
            input_sender: spawn_result.input_sender.clone(),
            task_handle: spawn_result.task_handle,
            view_stdout: task_config.view_stdout,
            view_stderr: task_config.view_stderr,
        };

        // 初期インプットを送信（is_functionの場合のみ、プロセス起動後に短い待機を入れる）
        if is_function {
            if let Some(initial_input) = initial_input {
                let input_sender_for_initial = spawn_result.input_sender.clone();
                let initial_input_lines: Vec<String> = initial_input.lines().map(|s| s.to_string()).collect();
                let initial_input_for_log = initial_input.clone();
                let function_name = task_config.name.clone();
                let task_id_for_log = task_id_new.clone();
                tokio::task::spawn(async move {
                    // プロセス起動を待つ（短い待機）
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                    
                    // 初期インプットの各行を個別に送信
                    // TopicMessageを使う場合、topicはfunction名にして、各行をdataとして送信
                    for line in initial_input_lines {
                        let topic_msg = TopicMessage {
                            topic: function_name.clone(),
                            data: line.clone(),
                        };
                        
                        if let Err(e) = input_sender_for_initial.send(InputDataMessage::Topic(topic_msg)) {
                            log::warn!("Failed to send initial input line to task {}: {}", task_id_for_log, e);
                            break;
                        }
                    }
                    
                    log::info!("Sent initial input to function {}: '{}'", task_id_for_log, initial_input_for_log);
                });
            }
        }
        
        // functionsの場合は重複許可（register_taskを直接呼ぶ）、tasksの場合は重複チェック（try_register_taskを呼ぶ）
        if is_function {
            if let Err(e) = self.register_task(task_config.name.clone(), running_task).await {
                return Err(anyhow::anyhow!("Failed to register task '{}': {}", task_config.name, e));
            }
            log::debug!("Registered function '{}' (ID: {}) - duplicate instances allowed", task_config.name, task_id_new);
        } else {
            if let Err(e) = self.try_register_task(task_config.name.clone(), running_task).await {
                return Err(anyhow::anyhow!("Failed to register task '{}': {}", task_config.name, e));
            }
        }
        
        log::info!("Successfully started task '{}' (ID: {})", task_config.name, task_id_new);
        Ok(())
    }

    pub async fn start_single_task(
        &self,
        task_name: &str,
        config: &SystemConfig,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
        return_message_sender: Option<ExecutorEventSender>,
        initial_input: Option<String>,
    ) -> Result<()> {
        // tasksから検索（優先）
        if let Some(task_config) = config.tasks.iter().find(|t| t.name == task_name) {
            return self.start_task_from_config(
                task_config,
                topic_manager,
                system_control_manager,
                shutdown_token,
                userlog_sender,
                return_message_sender,
                false, // is_function = false
                initial_input,
            ).await;
        }

        // functionsから検索
        if let Some(task_config) = config.functions.iter().find(|t| t.name == task_name) {
            return self.start_task_from_config(
                task_config,
                topic_manager,
                system_control_manager,
                shutdown_token,
                userlog_sender,
                return_message_sender,
                true, // is_function = true
                initial_input,
            ).await;
        }

        // どちらにも見つからなかった場合
        Err(anyhow::anyhow!("Task '{}' not found in configuration", task_name))
    }

    pub async fn add_task_from_toml(
        &self,
        toml_data: &str,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) -> Result<()> {
        let system_config = SystemConfig::from_toml(toml_data)?;
        
        if system_config.tasks.is_empty() {
            return Err(anyhow::anyhow!("No tasks found in TOML data"));
        }
        
        for task_config in &system_config.tasks {
            log::info!("Starting task '{}' from TOML", task_config.name);
            if let Err(e) = self.start_task_from_config(
                task_config,
                topic_manager.clone(),
                system_control_manager.clone(),
                shutdown_token.clone(),
                userlog_sender.clone(),
                None, // add_task_from_tomlではreturn_message_senderは不要
                false, // add_task_from_tomlはtasksとして扱う
                None, // add_task_from_tomlではinitial_inputは不要
            ).await {
                log::error!("Failed to start task '{}': {}", task_config.name, e);
            }
        }
        
        Ok(())
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
                let _ = h.await;
            }
        }
    }

    pub async fn abort_all(&mut self) {
        let mut handles = std::mem::take(&mut self.handles);
        for (_name, h) in handles.drain(..) {
            h.abort();
            let _ = h.await;
        }
    }
}

pub struct MiclowSystem {
    pub config: SystemConfig,
    topic_manager: TopicManager,
    system_control_manager: SystemControlManager,
    task_executor: TaskExecutor,
    shutdown_token: CancellationToken,
    background_tasks: BackgroundTaskManager,
}

impl MiclowSystem {
    pub fn new(config: SystemConfig) -> Self {
        let topic_manager: TopicManager = TopicManager::new();
        let shutdown_token: CancellationToken = CancellationToken::new();
        let system_control_manager: SystemControlManager = SystemControlManager::new(shutdown_token.clone());
        let task_executor: TaskExecutor = TaskExecutor::new();
        Self {
            config,
            topic_manager,
            system_control_manager,
            task_executor,
            shutdown_token,
            background_tasks: BackgroundTaskManager::new(),
        }
    }

    async fn start_user_tasks(
        config: &SystemConfig,
        task_executor: &TaskExecutor,
        topic_manager: TopicManager,
        system_control_manager: SystemControlManager,
        shutdown_token: CancellationToken,
        userlog_sender: UserLogSender,
    ) {
        let tasks: Vec<&TaskConfig> = config.get_all_tasks();

        for task_config in tasks.iter() {
            let task_name: String = task_config.name.clone();

            match task_executor.start_single_task(
                &task_name,
                config,
                topic_manager.clone(),
                system_control_manager.clone(),
                shutdown_token.clone(),
                userlog_sender.clone(),
                None, // 起動時はreturn_message_senderは不要
                None, // 起動時はinitial_inputは不要
            ).await {
                Ok(_) => {
                    log::info!("Started user task {} with command: {} {}",
                          task_name,
                          task_config.command,
                          task_config.args.join(" "));
                },
                Err(e) => {
                    log::error!("Failed to start task '{}': {}", task_name, e);
                    continue;
                }
            }
        }

        if tasks.is_empty() {
            log::info!("No tasks configured");
        } else {
            log::info!("Started {} user tasks from configuration", tasks.len());
        }
    }

    pub async fn start_system_with_interactive(
        mut self,
    ) -> Result<()> {
        let topic_manager: TopicManager = self.topic_manager.clone();

        let (log_tx, log_rx) = tokio::sync::mpsc::unbounded_channel::<LogEvent>();
        let _ = set_channel_logger(log_tx, level_from_env());
        let logging_shutdown = CancellationToken::new();
        let h_log = spawn_log_aggregator(log_rx, logging_shutdown.clone());
        self.background_tasks.register("log_aggregator", h_log);

        let (userlog_tx, userlog_rx) = mpsc::unbounded_channel::<UserLogEvent>();
        let userlog_sender = UserLogSender::new(userlog_tx);
        let h_userlog = spawn_user_log_aggregator(userlog_rx, logging_shutdown.clone());
        self.background_tasks.register("user_log_aggregator", h_userlog);

        let h_sys = start_system_control_worker(
            self.system_control_manager.clone(),
            topic_manager.clone(),
            self.task_executor.clone(),
            self.config.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        );
        self.background_tasks.register("system_control_worker", h_sys);

        Self::start_user_tasks(
            &self.config,
            &self.task_executor,
            topic_manager.clone(),
            self.system_control_manager.clone(),
            self.shutdown_token.clone(),
            userlog_sender.clone(),
        ).await;
        
        log::info!("System running. Press Ctrl+C to stop.");
        println!("System running. Press Ctrl+C to stop.");
        
        let shutdown_token = self.shutdown_token.clone();
        let interactive_task_id = TaskId::new();
        let interactive_backend: Box<dyn TaskBackend> = Box::new(InteractiveBackend::new("system".to_string()));
        let interactive_task_spawner = TaskSpawner::new(
            interactive_task_id.clone(),
            topic_manager.clone(),
            self.system_control_manager.clone(),
            self.task_executor.clone(),
            "interactive".to_string(),
            userlog_sender.clone(),
        );
        let interactive_result = interactive_task_spawner.spawn_backend(
            interactive_backend,
            self.shutdown_token.clone(),
            None,
            None, // other_return_message_sender
        ).await;
        
        let mut interactive_handle = interactive_result.task_handle;
        
        let ctrlc_fut = async {
            if let Err(e) = tokio::signal::ctrl_c().await {
                log::error!("Ctrl+C signal error: {}", e);
            } else {
                log::info!("Received Ctrl+C. Requesting graceful shutdown...");
                shutdown_token.cancel();
            }
        };
        tokio::select! {
            _ = &mut interactive_handle => {
                log::info!("Interactive mode terminated");
            },
            _ = ctrlc_fut => {
                log::info!("Ctrl+C signal received, proceeding with shutdown");
            },
        }

        log::info!("Received shutdown signal, stopping all workers...");
        
        Self::shutdown_workers(
            self.task_executor,
            self.shutdown_token,
        ).await;

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        logging_shutdown.cancel();
        if !interactive_handle.is_finished() {
            interactive_handle.abort();
            let _ = interactive_handle.await;
        }
        self.background_tasks.abort_all().await;
        log::logger().flush();

        log::info!("Graceful shutdown completed");
        println!("Graceful shutdown completed");
        return Ok(());
    }

    async fn shutdown_workers(
        task_executor: TaskExecutor,
        shutdown_token: CancellationToken,
    ) {
        log::info!("Starting graceful shutdown...");

        log::info!("Cancelling shutdown token");
        shutdown_token.cancel();

        log::info!("Waiting for running tasks to finish...");
        task_executor.graceful_shutdown_all(std::time::Duration::from_secs(5)).await;

        log::info!("All user tasks stopped");
    }
}

// TODO function機能を追加のリファクタが必要、かなり難しそう