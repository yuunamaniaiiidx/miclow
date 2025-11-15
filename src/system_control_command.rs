use tokio_util::sync::CancellationToken;
use crate::task_id::TaskId;
use crate::topic_manager::TopicManager;
use crate::miclow::TaskExecutor;
use crate::config::SystemConfig;
use crate::chunnel::UserLogSender;
use crate::system_control_manager::SystemControlManager;
use crate::chunnel::{SystemResponseSender, SystemResponseEvent, SystemResponseStatus};
use crate::chunnel::{ExecutorEvent, ExecutorEventSender};
use crate::start_context::StartContext;

#[derive(Debug, Clone)]
pub enum SystemControlCommand {
    SubscribeTopic { topic: String },
    UnsubscribeTopic { topic: String },
    Status,
    GetLatestMessage { topic: String },
    CallFunction { task_name: String, initial_input: Option<String> },
    Unknown { command: String, data: String },
}

impl SystemControlCommand {
    pub async fn execute(
        &self,
        topic_manager: &TopicManager,
        task_executor: &TaskExecutor,
        system_config: &SystemConfig,
        shutdown_token: &CancellationToken,
        userlog_sender: &UserLogSender,
        system_control_manager: &SystemControlManager,
        task_id: &TaskId,
        response_channel: &SystemResponseSender,
        task_event_sender: &ExecutorEventSender,
        return_message_sender: &ExecutorEventSender,
    ) -> Result<(), String> {
        match self {
            SystemControlCommand::SubscribeTopic { topic } => {
                log::info!("Processing SubscribeTopic command for task {}: '{}'", task_id, topic);
                
                topic_manager.add_subscriber(
                    topic.clone(),
                    task_id.clone(), 
                    task_event_sender.clone()
                ).await;
                
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
            },
            SystemControlCommand::UnsubscribeTopic { topic } => {
                log::info!("Processing UnsubscribeTopic command for task {}: '{}'", task_id, topic);
                
                let removed = topic_manager.remove_subscriber_by_task(
                    topic.clone(),
                    task_id.clone()
                ).await;
                
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
            },
            SystemControlCommand::Status => {
                log::info!("Processing Status command for task {}", task_id);
                
                let tasks_info = task_executor.get_running_tasks_info().await;
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
            },
            SystemControlCommand::GetLatestMessage { topic } => {
                log::info!("Processing GetLatestMessage command for task {}: '{}'", task_id, topic);

                match topic_manager.get_latest_message(topic).await {
                    Some(ExecutorEvent::Message { data, .. }) => {
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
                        log::warn!("Latest event for topic '{}' is not a Message; task {}", topic, task_id);
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
                        log::info!("No latest message found for topic '{}' (task {})", topic, task_id);
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
            },
            SystemControlCommand::CallFunction { task_name, initial_input } => {
                log::info!("Processing CallFunction command for task {}: '{}'", task_id, task_name);
                
                let caller_task_name = task_executor.get_task_name_by_id(task_id).await;
                
                let ready_context = match StartContext::from_task_name(
                    task_name.clone(),
                    system_config,
                    topic_manager.clone(),
                    system_control_manager.clone(),
                    shutdown_token.clone(),
                    userlog_sender.clone(),
                    Some(return_message_sender.clone()),
                    initial_input.clone(),
                    caller_task_name.clone(),
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
                    },
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
            },
            SystemControlCommand::Unknown { command, data } => {
                log::warn!("Unknown system control command '{}' with data '{}' from task {}", command, data, task_id);
                Ok(())
            },
        }
    }
}

pub fn system_control_command_to_handler(event: &ExecutorEvent) -> Option<SystemControlCommand> {
    let ExecutorEvent::SystemControl { key, data } = event else {
        return None;
    };
    
    let key_lower = key.to_lowercase();
    let data_trimmed = data.trim();

    let cmd = match key_lower.as_str() {
        "system.subscribe-topic" => {
            SystemControlCommand::SubscribeTopic { 
                topic: data_trimmed.to_string() 
            }
        },
        "system.unsubscribe-topic" => {
            SystemControlCommand::UnsubscribeTopic { 
                topic: data_trimmed.to_string() 
            }
        },
        "system.status" => {
            SystemControlCommand::Status
        },
        "system.get-latest-message" => {
            if data_trimmed.is_empty() {
                return None;
            }
            SystemControlCommand::GetLatestMessage {
                topic: data_trimmed.to_string(),
            }
        },
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
            SystemControlCommand::CallFunction { 
                task_name: function_name.to_string(),
                initial_input,
            }
        },
        _ if key_lower.starts_with("system.") => {
            SystemControlCommand::Unknown { 
                command: key_lower.clone(), 
                data: data_trimmed.to_string() 
            }
        },
        _ => return None,
    };
    
    Some(cmd)
}
