use crate::channels::UserLogSender;
use crate::config::SystemConfig;
use crate::messages::{SystemResponseEvent, SystemResponseStatus};
use crate::system_control::queue::SystemControlQueue;
use crate::task_id::TaskId;
use crate::task_runtime::TaskExecutor;
use crate::topic_broker::TopicBroker;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub fn start_system_control_worker(
    system_control_manager: SystemControlQueue,
    topic_manager: TopicBroker,
    task_executor: TaskExecutor,
    system_config: SystemConfig,
    shutdown_token: CancellationToken,
    userlog_sender: UserLogSender,
) -> tokio::task::JoinHandle<()> {
    task::spawn(async move {
        let running_commands: Arc<RwLock<HashMap<TaskId, JoinHandle<()>>>> =
            Arc::new(RwLock::new(HashMap::new()));

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

                            let command_shutdown_token = shutdown_token.clone();
                            let running_commands_clone = running_commands.clone();
                            let task_id_clone = task_id.clone();
                            let topic_manager_clone = topic_manager.clone();
                            let task_executor_clone = task_executor.clone();
                            let system_config_clone = system_config.clone();
                            let shutdown_token_clone = shutdown_token.clone();
                            let userlog_sender_clone = userlog_sender.clone();
                            let system_control_manager_clone = system_control_manager.clone();
                            let response_channel_clone = message.response_channel.clone();
                            let task_event_sender_clone = message.task_event_sender.clone();
                            let return_message_sender_clone = message.return_message_sender.clone();
                            let response_channel_for_cancel = message.response_channel.clone();
                            let action_clone = message.action.clone();

                            let worker = tokio::spawn(async move {
                                let task_id_for_log = task_id_clone.clone();
                                log::info!("Executing SystemControl for task {}", task_id_for_log);

                                let execute_worker = tokio::spawn(async move {
                                    action_clone.execute(
                                        &topic_manager_clone,
                                        &task_executor_clone,
                                        &system_config_clone,
                                        &shutdown_token_clone,
                                        &userlog_sender_clone,
                                        &system_control_manager_clone,
                                        &task_id_clone,
                                        &response_channel_clone,
                                        &task_event_sender_clone,
                                        &return_message_sender_clone,
                                    ).await
                                });

                                let execute_abort_handle = execute_worker.abort_handle();
                                let task_id_for_select = task_id_for_log.clone();

                                tokio::select! {
                                    _ = command_shutdown_token.cancelled() => {
                                        log::info!("SystemControl for task {} cancelled due to shutdown", task_id_for_select);
                                        execute_abort_handle.abort();

                                        let status = SystemResponseStatus::Error;
                                        let cancel_error = SystemResponseEvent::new_system_error(
                                            "system.error".to_string(),
                                            status.to_string(),
                                            "cancelled".to_string(),
                                        );
                                        let _ = response_channel_for_cancel.send(cancel_error);
                                    }
                                    result = execute_worker => {
                                        match result {
                                            Ok(Ok(_)) => {
                                                log::info!("Successfully executed SystemControl for task {}", task_id_for_select);
                                            }
                                            Ok(Err(e)) => {
                                                log::error!("Failed to execute SystemControl for task {}: {}", task_id_for_select, e);
                                                let status = SystemResponseStatus::Error;
                                                let error_event = SystemResponseEvent::new_system_error(
                                                    "system.error".to_string(),
                                                    status.to_string(),
                                                    e.clone(),
                                                );
                                                let _ = response_channel_for_cancel.send(error_event);
                                            }
                                            Err(e) => {
                                                if e.is_cancelled() {
                                                    log::info!("SystemControl execution was cancelled for task {}", task_id_for_select);
                                                } else {
                                                    log::error!("SystemControl execution task panicked for task {}: {:?}", task_id_for_select, e);
                                                    let status = SystemResponseStatus::Error;
                                                    let error_event = SystemResponseEvent::new_system_error(
                                                        "system.error".to_string(),
                                                        status.to_string(),
                                                        format!("Task panicked: {:?}", e),
                                                    );
                                                    let _ = response_channel_for_cancel.send(error_event);
                                                }
                                            }
                                        }
                                    }
                                }

                                let mut commands = running_commands_clone.write().await;
                                commands.remove(&task_id_for_select);
                            });

                            let mut commands = running_commands.write().await;
                            commands.insert(task_id, worker);
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
