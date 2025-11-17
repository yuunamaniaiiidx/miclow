use crate::channels::ExecutorOutputEventSender;
use crate::channels::SystemResponseSender;
use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::messages::ExecutorOutputEvent;
use crate::messages::{SystemResponseEvent, SystemResponseStatus};
use crate::system_control::queue::SystemControlQueue;
use crate::task_id::TaskId;
use crate::task_runtime::{ParentInvocationContext, StartContext, TaskExecutor};
use crate::topic_broker::TopicBroker;
use tokio_util::sync::CancellationToken;

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

                let mut json_response = String::from("{\n");
                json_response.push_str("  \"tasks\": [\n");

                for (i, (task_name, task_id)) in tasks_info.iter().enumerate() {
                    if i > 0 {
                        json_response.push_str(",\n");
                    }
                    json_response.push_str(&format!(
                        "    {{\"name\": \"{}\", \"id\": \"{}\"}}",
                        task_name, task_id
                    ));
                }

                json_response.push_str("\n  ],\n");
                json_response.push_str("  \"topics\": [\n");

                for (i, (topic_name, subscriber_count)) in topics_info.iter().enumerate() {
                    if i > 0 {
                        json_response.push_str(",\n");
                    }
                    json_response.push_str(&format!(
                        "    {{\"name\": \"{}\", \"subscribers\": {}}}",
                        topic_name, subscriber_count
                    ));
                }

                json_response.push_str("\n  ]\n");
                json_response.push_str("}");

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

pub fn system_control_action_from_event(
    event: &ExecutorOutputEvent,
) -> Option<SystemControlAction> {
    let ExecutorOutputEvent::SystemControl { key, data } = event else {
        return None;
    };

    let key_lower = key.to_lowercase();
    let data_trimmed = data.trim();

    let action = match key_lower.as_str() {
        "system.subscribe-topic" => SystemControlAction::SubscribeTopic {
            topic: data_trimmed.to_string(),
        },
        "system.unsubscribe-topic" => SystemControlAction::UnsubscribeTopic {
            topic: data_trimmed.to_string(),
        },
        "system.status" => SystemControlAction::Status,
        "system.get-latest-message" => {
            if data_trimmed.is_empty() {
                return None;
            }
            SystemControlAction::GetLatestMessage {
                topic: data_trimmed.to_string(),
            }
        }
        _ if key_lower.starts_with("system.function.") => {
            let function_name = key_lower.strip_prefix("system.function.").unwrap_or("");
            if function_name.is_empty() {
                return None;
            }
            let initial_input = if data_trimmed.is_empty() {
                None
            } else {
                Some(data_trimmed.to_string())
            };
            SystemControlAction::CallFunction {
                task_name: function_name.to_string(),
                initial_input,
            }
        }
        _ if key_lower.starts_with("system.") => SystemControlAction::Unknown {
            command: key_lower.clone(),
            data: data_trimmed.to_string(),
        },
        _ => return None,
    };

    Some(action)
}
