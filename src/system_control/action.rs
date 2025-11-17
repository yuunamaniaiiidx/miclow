use crate::channels::ExecutorOutputEventSender;
use crate::channels::SystemResponseSender;
use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::messages::{ExecutorOutputEvent, SystemResponseEvent, SystemResponseStatus};
use crate::system_control::queue::SystemControlQueue;
use crate::task_id::TaskId;
use crate::task_runtime::{ParentInvocationContext, StartContext, TaskExecutor};
use crate::topic_broker::TopicBroker;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

#[derive(Serialize)]
struct StatusResponse {
    tasks: Vec<StatusTaskInfo>,
    topics: Vec<StatusTopicInfo>,
    functions: Vec<StatusFunctionInfo>,
}

#[derive(Serialize)]
struct StatusTaskInfo {
    name: String,
    id: String,
}

#[derive(Serialize)]
struct StatusTopicInfo {
    name: String,
    subscribers: usize,
}

#[derive(Serialize)]
struct StatusFunctionInfo {
    name: String,
    protocol: String,
    allow_duplicate: bool,
    auto_start: bool,
}

#[derive(Debug, Clone)]
pub enum SystemControlAction {
    SubscribeTopic {
        topic: String,
    },
    UnsubscribeTopic {
        topic: String,
    },
    Status,
    GetLatestMessage {
        topic: String,
    },
    CallFunction {
        task_name: String,
        initial_input: Option<String>,
    },
    Unknown {
        command: String,
        data: String,
    },
}

impl SystemControlAction {
    pub async fn execute(
        &self,
        topic_manager: &TopicBroker,
        task_executor: &TaskExecutor,
        system_config: &SystemConfig,
        shutdown_token: &CancellationToken,
        userlog_sender: &UserLogSender,
        system_control_manager: &SystemControlQueue,
        task_id: &TaskId,
        response_channel: &SystemResponseSender,
        task_event_sender: &ExecutorOutputEventSender,
        return_message_sender: &ExecutorOutputEventSender,
    ) -> Result<(), String> {
        match self {
            SystemControlAction::SubscribeTopic { topic } => {
                log::info!(
                    "Processing SubscribeTopic action for task {}: '{}'",
                    task_id,
                    topic
                );

                topic_manager
                    .add_subscriber(topic.clone(), task_id.clone(), task_event_sender.clone())
                    .await;

                log::info!("Successfully subscribed to topic '{}'", topic);

                let status = SystemResponseStatus::Success;
                let response_topic = "system.subscribe-topic".to_string();
                let success_event = SystemResponseEvent::new_system_response(
                    response_topic,
                    status.to_string(),
                    topic.clone(),
                );
                let _ = response_channel.send(success_event);
                Ok(())
            }
            SystemControlAction::UnsubscribeTopic { topic } => {
                log::info!(
                    "Processing UnsubscribeTopic action for task {}: '{}'",
                    task_id,
                    topic
                );

                let removed = topic_manager
                    .remove_subscriber_by_task(topic.clone(), task_id.clone())
                    .await;

                if removed {
                    log::info!("Successfully unsubscribed from topic '{}'", topic);

                    let status = SystemResponseStatus::Success;
                    let response_topic = "system.unsubscribe-topic".to_string();
                    let success_event = SystemResponseEvent::new_system_response(
                        response_topic,
                        status.to_string(),
                        topic.clone(),
                    );
                    let _ = response_channel.send(success_event);
                } else {
                    log::warn!("Failed to unsubscribe from topic '{}'", topic);

                    let status = SystemResponseStatus::Error;
                    let response_topic = "system.unsubscribe-topic".to_string();
                    let error_event = SystemResponseEvent::new_system_error(
                        response_topic,
                        status.to_string(),
                        topic.clone(),
                    );
                    let _ = response_channel.send(error_event);
                }
                Ok(())
            }
            SystemControlAction::Status => {
                log::info!("Processing Status action for task {}", task_id);

                let tasks_info = task_executor.get_running_tasks_info().await;
                let topics_info = topic_manager.get_topics_info().await;
                let mut functions_info: Vec<StatusFunctionInfo> = system_config
                    .functions
                    .iter()
                    .map(|(name, config)| StatusFunctionInfo {
                        name: name.clone(),
                        protocol: config.protocol.clone(),
                        allow_duplicate: config.allow_duplicate,
                        auto_start: config.auto_start,
                    })
                    .collect();

                functions_info.sort_by(|a, b| a.name.cmp(&b.name));

                let status_payload = StatusResponse {
                    tasks: tasks_info
                        .into_iter()
                        .map(|(task_name, task_id)| StatusTaskInfo {
                            name: task_name,
                            id: task_id.to_string(),
                        })
                        .collect(),
                    topics: topics_info
                        .into_iter()
                        .map(|(topic_name, subscriber_count)| StatusTopicInfo {
                            name: topic_name,
                            subscribers: subscriber_count,
                        })
                        .collect(),
                    functions: functions_info,
                };

                let json_response = match serde_json::to_string_pretty(&status_payload)
                    .map_err(|e| e.to_string())
                {
                    Ok(json) => json,
                    Err(err) => {
                        log::error!("Failed to serialize status response: {}", err);
                        let status = SystemResponseStatus::Error;
                        let error_event = SystemResponseEvent::new_system_error(
                            "system.status".to_string(),
                            status.to_string(),
                            format!("Failed to serialize status response: {}", err),
                        );
                        let _ = response_channel.send(error_event);
                        return Err(err);
                    }
                };

                let status = SystemResponseStatus::Success;
                let status_event = SystemResponseEvent::new_system_response(
                    "system.status".to_string(),
                    status.to_string(),
                    json_response,
                );

                if let Err(e) = response_channel.send(status_event) {
                    log::warn!("Failed to send status response to task {}: {}", task_id, e);
                    Err(format!("Failed to send status response: {}", e))
                } else {
                    log::info!("Sent status response to task {}", task_id);
                    Ok(())
                }
            }
            SystemControlAction::GetLatestMessage { topic } => {
                log::info!(
                    "Processing GetLatestMessage action for task {}: '{}'",
                    task_id,
                    topic
                );

                match topic_manager.get_latest_message(topic).await {
                    Some(ExecutorOutputEvent::Message { data, .. }) => {
                        let status = SystemResponseStatus::Success;
                        let response_topic = "system.get-latest-message".to_string();
                        let success_event = SystemResponseEvent::new_system_response(
                            response_topic,
                            status.to_string(),
                            data.clone(),
                        );
                        let _ = response_channel.send(success_event);
                        Ok(())
                    }
                    Some(_) => {
                        log::warn!(
                            "Latest event for topic '{}' is not a Message; task {}",
                            topic,
                            task_id
                        );
                        let status = SystemResponseStatus::Error;
                        let response_topic = "system.get-latest-message".to_string();
                        let error_event = SystemResponseEvent::new_system_error(
                            response_topic,
                            status.to_string(),
                            format!("Latest event for '{}' is not a message", topic),
                        );
                        let _ = response_channel.send(error_event);
                        Err("Latest event is not a Message variant".to_string())
                    }
                    None => {
                        log::info!(
                            "No latest message found for topic '{}' (task {})",
                            topic,
                            task_id
                        );
                        let status = SystemResponseStatus::Error;
                        let response_topic = "system.get-latest-message".to_string();
                        let error_event = SystemResponseEvent::new_system_error(
                            response_topic,
                            status.to_string(),
                            format!("No latest message for topic '{}'", topic),
                        );
                        let _ = response_channel.send(error_event);
                        Err(format!("No latest message for topic '{}'", topic))
                    }
                }
            }
            SystemControlAction::CallFunction {
                task_name,
                initial_input,
            } => {
                log::info!(
                    "Processing CallFunction action for task {}: '{}'",
                    task_id,
                    task_name
                );

                let parent_invocation = ParentInvocationContext {
                    return_channel: return_message_sender.clone(),
                    initial_input: initial_input.clone(),
                };

                let ready_context = match StartContext::from_task_name(
                    task_name.clone(),
                    system_config,
                    topic_manager.clone(),
                    system_control_manager.clone(),
                    shutdown_token.clone(),
                    userlog_sender.clone(),
                    Some(parent_invocation),
                ) {
                    Ok(ctx) => ctx,
                    Err(e) => {
                        log::error!("Failed to find task '{}': {}", task_name, e);
                        let status = SystemResponseStatus::Error;
                        let response_topic = format!("system.function.{}", task_name);
                        let error_event = SystemResponseEvent::new_system_error(
                            response_topic,
                            status.to_string(),
                            e.to_string(),
                        );
                        let _ = response_channel.send(error_event);
                        return Err(format!("Task '{}' not found", task_name));
                    }
                };

                match task_executor.start_task_from_config(ready_context).await {
                    Ok(_) => {
                        log::info!("Successfully called function '{}'", task_name);

                        let status = SystemResponseStatus::Success;
                        let response_topic = format!("system.function.{}", task_name);
                        let success_event = SystemResponseEvent::new_system_response(
                            response_topic,
                            status.to_string(),
                            task_name.clone(),
                        );
                        let _ = response_channel.send(success_event);
                        Ok(())
                    }
                    Err(e) => {
                        log::error!("Failed to call function '{}': {}", task_name, e);
                        let status = SystemResponseStatus::Error;
                        let response_topic = format!("system.function.{}", task_name);
                        let error_event = SystemResponseEvent::new_system_error(
                            response_topic,
                            status.to_string(),
                            e.to_string(),
                        );
                        let _ = response_channel.send(error_event);
                        Err(e.to_string())
                    }
                }
            }
            SystemControlAction::Unknown { command, data } => {
                log::warn!(
                    "Unknown system control action '{}' with data '{}' from task {}",
                    command,
                    data,
                    task_id
                );
                Ok(())
            }
        }
    }
}
